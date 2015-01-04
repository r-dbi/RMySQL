#' @include mysql.R result.R
NULL

## the following code was kindly provided ny J. T. Lindgren.
#' @useDynLib RMySQL rmysql_escape_strings
mysqlEscapeStrings <- function(con, strings) {
  checkValid(con)

  out <- .Call(rmysql_escape_strings, con@Id, as.character(strings))
  names(out) <- names(strings)
  out
}

#' Escape SQL-special characters in strings.
#'
#' @param con a connection object (see \code{\link[DBI]{dbConnect}}).
#' @param strings a character vector.
#' @param ... any additional arguments to be passed to the dispatched method.
#' @return A character vector with SQL special characters properly escaped.
#' @export
#' @examples
#' if (mysqlHasDefault()) {
#' con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#'
#' tmp <- sprintf("SELECT * FROM emp WHERE lname = %s", "O'Reilly")
#' dbEscapeStrings(con, tmp)
#'
#' dbDisconnect(con)
#' }
setGeneric("dbEscapeStrings", function(con, strings, ...) {
  standardGeneric("dbEscapeStrings")
})

#' @rdname dbEscapeStrings
#' @export
setMethod("dbEscapeStrings",
  sig = signature(con = "MySQLConnection", strings = "character"),
  def = mysqlEscapeStrings,
  valueClass = "character"
)

#' @rdname dbEscapeStrings
#' @export
setMethod("dbEscapeStrings",
  sig = signature(con = "MySQLResult", strings = "character"),
  def = function(con, strings, ...)
    mysqlEscapeStrings(as(con, "MySQLConnection"), strings),
  valueClass = "character"
)

#' Apply R/S-Plus functions to remote groups of DBMS rows (experimental)
#'
#' Applies R/S-Plus functions to groups of remote DBMS rows without bringing an
#' entire result set all at once.  The result set is expected to be sorted by
#' the grouping field.
#'
#' This function is meant to handle somewhat gracefully(?) large
#' amounts of data from the DBMS by bringing into R manageable chunks (about
#' \code{batchSize} records at a time, but not more than \code{maxBatch}); the
#' idea is that the data from individual groups can be handled by R, but not
#' all the groups at the same time.
#'
#' @export
setGeneric("dbApply", function(res, ...) {
  standardGeneric("dbApply")
})

#' The MySQL implementation allows us to register R
#' functions that get invoked when certain fetching events occur. These include
#' the ``begin'' event (no records have been yet fetched), ``begin.group'' (the
#' record just fetched belongs to a new group), ``new record'' (every fetched
#' record generates this event), ``group.end'' (the record just fetched was the
#' last row of the current group), ``end'' (the very last record from the
#' result set). Awk and perl programmers will find this paradigm very familiar
#' (although SAP's ABAP language is closer to what we're doing).
#'
#' @param res a result set (see \code{\link[DBI]{dbSendQuery}}).
#' @param INDEX a character or integer specifying the field name or field
#'   number that defines the various groups.
#' @param FUN a function to be invoked upon identifying the last row from every
#'   group. This function will be passed a data frame holding the records of the
#'   current group, a character string with the group label, plus any other
#'   arguments passed to \code{dbApply} as \code{"..."}.
#' @param begin a function of no arguments to be invoked just prior to retrieve
#'   the first row from the result set.
#' @param end a function of no arguments to be invoked just after retrieving
#'   the last row from the result set.
#' @param group.begin a function of one argument (the group label) to be
#'   invoked upon identifying a row from a new group
#' @param new.record a function to be invoked as each individual record is
#'   fetched.  The first argument to this function is a one-row data.frame
#'   holding the new record.
#' @param batchSize the default number of rows to bring from the remote result
#'   set. If needed, this is automatically extended to hold groups bigger than
#'   \code{batchSize}.
#' @param maxBatch the absolute maximum of rows per group that may be extracted
#'   from the result set.
#' @param ... any additional arguments to be passed to \code{FUN}.
#' @param simplify Not yet implemented
#' @return A list with as many elements as there were groups in the result set.
#' @export
#' @rdname dbApply
#' @examples
#' if (mysqlHasDefault()) {
#' con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#'
#' dbWriteTable(con, "mtcars", mtcars, overwrite = TRUE)
#' res <- dbSendQuery(con, "SELECT * FROM mtcars ORDER BY cyl")
#' dbApply(res, "cyl", function(x, grp) quantile(x$mpg, names=FALSE))
#'
#' dbClearResult(res)
#' dbRemoveTable(con, "mtcars")
#' dbDisconnect(con)
#' }
#' @useDynLib RMySQL RS_MySQL_dbApply
setMethod("dbApply", "MySQLResult",
  function(res, INDEX, FUN = stop("must specify FUN"),
    begin = NULL,
    group.begin =  NULL,
    new.record = NULL,
    end = NULL,
    batchSize = 100, maxBatch = 1e6,
    ..., simplify = TRUE)
  ## The "begin", "begin.group", etc., specify R functions to be
  ## invoked upon the corresponding events.  (For compatibility
  ## with other apply functions the arg FUN is used to specify the
  ## most common case where we only specify the "group.end" event.)
  ##
  ## The following describes the exact order and form of invocation for the
  ## various callbacks in the underlying  C code.  All callback functions
  ## (except FUN) are optional.
  ##  begin()
  ##    group.begin(group.name)
  ##    new.record(df.record)
  ##    FUN(df.group, group.name, ...)   (aka group.end)
  ##  end()
  ##
  ## TODO: (1) add argument output=F/T to suppress the creation of
  ##           an expensive(?) output list.
  ##       (2) allow INDEX to be a list as in tapply()
  ##       (3) add a "counter" event, to callback every k rows
  ##       (3) should we implement a simplify argument, as in sapply()?
  ##       (4) should it report (instead of just warning) when we're forced
  ##           to handle partial groups (groups larger than maxBatch).
  ##       (5) extend to the case where even individual groups are too
  ##           big for R (as in incremental quantiles).
  ##       (6) Highly R-dependent, not sure yet how to port it to S-plus.
{
    if(dbHasCompleted(res))
      stop("result set has completed")
    if(is.character(INDEX)){
      flds <- tolower(as.character(dbColumnInfo(res)$name))
      INDEX <- match(tolower(INDEX[1]), flds, 0)
    }
    if(INDEX<1)
      stop(paste("INDEX field", INDEX, "not in result set"))

    "null.or.fun" <- function(fun) # get fun obj, but a NULL is ok
    {
      if(is.null(fun))
        fun
      else
        match.fun(fun)
    }
    begin <- null.or.fun(begin)
    group.begin <- null.or.fun(group.begin)
    group.end <- null.or.fun(FUN)     ## probably this is the most important
    end <- null.or.fun(end)
    new.record <- null.or.fun(new.record)
    con <- as(res, "MySQLConnection")
    on.exit({
      rc <- dbGetException(con)
      if(!is.null(rc$errorNum) && rc$errorNum!=0)
        cat("dbApply aborted with MySQL error ", rc$errorNum,
          " (", rc$errorMsg, ")\n", sep = "")

    })
    ## BEGIN event handler (re-entrant, only prior to reading first row)
    if(!is.null(begin) && dbGetRowCount(res)==0)
      begin()
    rho <- environment()
    funs <- list(begin = begin, end = end,
      group.begin = group.begin,
      group.end = group.end, new.record = new.record)
    out <- .Call(RS_MySQL_dbApply,
      rs = res@Id,
      INDEX = as.integer(INDEX-1),
      funs, rho, as.integer(batchSize), as.integer(maxBatch))
    if(!is.null(end) && dbHasCompleted(res))
      end()
    out
  }
)

#' Fetch next result set from an SQL script or stored procedure (experimental)
#'
#' SQL scripts (i.e., multiple SQL statements separated by ';') and stored
#' procedures oftentimes generate multiple result sets.  These generic
#' functions provide a means to process them sequentially. \code{dbNextResult}
#' fetches the next result from the sequence of pending results sets;
#' \code{dbMoreResults} returns a logical to indicate whether there are
#' additional results to process.
#'
#' @param con a connection object (see \code{\link[DBI]{dbConnect}}).
#' @param ... any additional arguments to be passed to the dispatched method
#' @return
#'   \code{dbNextResult} returns a result set or \code{NULL}.
#'
#'   \code{dbMoreResults} returns a logical specifying whether or not there are
#'   additional result sets to process in the connection.
#' @export
#' @examples
#' if (mysqlHasDefault()) {
#' con <- dbConnect(RMySQL::MySQL(), dbname = "test", client.flag = CLIENT_MULTI_STATEMENTS)
#' dbWriteTable(con, "mtcars", datasets::mtcars, overwrite = TRUE)
#'
#' sql <- "SELECT cyl FROM mtcars LIMIT 5; SELECT vs FROM mtcars LIMIT 5"
#' rs1 <- dbSendQuery(con, sql)
#' dbFetch(rs1, n = -1)
#'
#' if (dbMoreResults(con)) {
#'    rs2 <- dbNextResult(con)
#'    dbFetch(rs2, n = -1)
#' }
#'
#' dbClearResult(rs1)
#' dbClearResult(rs2)
#' dbRemoveTable(con, "mtcars")
#' dbDisconnect(con)
#' }
setGeneric("dbNextResult", function(con, ...) {
  standardGeneric("dbNextResult")
})

#' @export
#' @rdname dbNextResult
#' @useDynLib RMySQL RS_MySQL_nextResultSet
setMethod("dbNextResult", "MySQLConnection", function(con, ...) {
  for(rs in dbListResults(con)){
    dbClearResult(rs)
  }

  id = .Call(RS_MySQL_nextResultSet, con@Id)
  new("MySQLResult", Id = id)
}
)

#' @export
#' @rdname dbNextResult
setGeneric("dbMoreResults", function(con, ...) {
  standardGeneric("dbMoreResults")
})

#' @export
#' @rdname dbNextResult
#' @useDynLib RMySQL RS_MySQL_moreResultSets
setMethod("dbMoreResults", "MySQLConnection", function(con, ...) {
  .Call(RS_MySQL_moreResultSets, con@Id)
})

#' Build the SQL CREATE TABLE definition as a string
#'
#' The output SQL statement is a simple \code{CREATE TABLE} with suitable for
#' \code{dbGetQuery}
#'
#' @param dbObj any DBI object (used only to dispatch according to the engine
#' (e.g., MySQL, Oracle, PostgreSQL, SQLite)
#' @param name name of the new SQL table
#' @param obj an R object coerceable to data.frame for which we want to create
#' a table
#' @param field.types optional named list of the types for each field in
#' \code{obj}
#' @param row.names logical, should row.name of \code{value} be exported as a
#' \code{row\_names} field? Default is TRUE
#' @param \dots reserved for future use
#' @return An SQL string
#' @export
#' @keywords internal
mysqlBuildTableDefinition <- function(dbObj, name, obj, field.types = NULL,
                                   row.names = TRUE, ...) {
  if (!is.data.frame(obj)) {
    obj <- as.data.frame(obj)
  }
  value <- explict_rownames(obj, row.names)

  if (is.null(field.types)) {
    field.types <- vapply(value, dbDataType, dbObj = dbObj,
      FUN.VALUE = character(1))
  }
  # Escape field names
  names(field.types) <- dbQuoteIdentifier(dbObj, names(field.types))

  flds <- paste(names(field.types), field.types)
  paste("CREATE TABLE", name, "\n(", paste(flds, collapse = ",\n\t"), "\n)")
}

## Escape problematic characters in the data frame.
## These are: - tab, as this is the field separator
##            - newline, as this is the record separator
##            - backslash, the escaping character
## Obviously, not all data types can contain these, e.g. numeric types
## can not. So we only substitute character and factor types.
## (FIXME: is there anything else?)
escape <- function(table) {
  table <- as.data.frame(table)
  repcols <- which(sapply(table, is.character) | sapply(table, is.factor))
  for (rc in repcols) {
    table[,rc] <- gsub("\\\\", "\\\\\\\\", table[,rc])
    table[,rc] <- gsub("\\n", "\\\\n", table[,rc])
    table[,rc] <- gsub("\\t", "\\\\t", table[,rc])
  }
  table
}


## safe.write makes sure write.table doesn't exceed available memory by batching
## at most batch rows (but it is still slowww)
safe.write <- function(value, file, batch, ...) {
  N <- nrow(value)
  if(N<1){
    warning("no rows in data.frame")
    return(NULL)
  }
  digits <- options(digits = 17)
  on.exit(options(digits))
  if(missing(batch) || is.null(batch))
    batch <- 10000
  else if(batch<=0)
    batch <- N
  from <- 1
  to <- min(batch, N)
  conb <- file(file,open="wb")
  while(from<=N){
    write.table(escape(value[from:to,, drop=FALSE]), file = conb,
      append = TRUE, quote = FALSE, sep="\t", na = "\\N",
      row.names=FALSE, col.names=FALSE, eol = '\n', ...)
    from <- to+1
    to <- min(to+batch, N)
  }
  close(conb)
  invisible(NULL)
}

#' MySQL Check for Compiled Versus Loaded Client Library Versions
#'
#' This function prints out the compiled and loaded client library versions.
#'
#' @return A named integer vector of length two, the first element representing
#' the compiled library version and the second element representint the loaded
#' client library version.
#' @export
#' @examples
#' mysqlClientLibraryVersions()
#' @useDynLib RMySQL rmysql_version
mysqlClientLibraryVersions <- function() {
  .Call(rmysql_version)
}

#' Quote method for MySQL identifiers
#'
#' In MySQL, identifiers are enclosed in backticks, e.g. \code{`x`}.
#'
#' @export
#' @keywords internal
setMethod("dbQuoteIdentifier", c("MySQLConnection", "character"),
  function(conn, x, ...) {
    x <- gsub('`', '``', x, fixed = TRUE)
    SQL(paste('`', x, '`', sep = ""))
  }
)

## the following code was kindly provided ny J. T. Lindgren.
mysqlEscapeStrings <-
  function(con, strings)
  {
    ## Escapes the given strings
    if(!isIdCurrent(con))
      stop(paste("expired", class(con)))
    strings <- as(strings, "character")
    conId <- as(con, "integer");
    out <- .Call("RS_MySQL_escapeStrings", conId, strings,
      PACKAGE = .MySQLPkgName)
    names(out) <- names(strings)
    out
  }

#' Escape SQL-special characters in strings
#'
#' @param con a connection object (see \code{\link[DBI]{dbConnect}}).
#' @param strings a character vector.
#' @param ... any additional arguments to be passed to the dispatched method.
#' @return A character vector with SQL special characters properly escaped.
#' @seealso \code{\link{MySQL}} \code{\link[DBI]{dbSendQuery}}
#' \code{\link[DBI]{fetch}}
#' @keywords programming interface database
#' @export
#' @examples
#' \dontrun{
#' tmp <- sprintf("select * from emp where lname = %s", "O'Reilly")
#' sql <- dbEscapeString(con, tmp)
#' dbGetQuery(con, sql)
#' }
#'
setGeneric("dbEscapeStrings",
  def = function(con, strings, ...) standardGeneric("dbEscapeStrings"))

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
#' @export
#' @section Methods: \describe{ \item{res}{a MySQL result set (see
#' \code{\link[DBI]{dbSendQuery}}).} \item{...}{any additional arguments to be
#' passed to \code{FUN}.} }
#' @seealso \code{\link{MySQL}} \code{\link{mysqlDBApply}}
#' \code{\link[DBI]{dbSendQuery}} \code{\link[DBI]{fetch}}
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords programming interface database
#' @examples
#' \dontrun{
#' ## compute quanitiles for each network agent
#' con <- dbConnect(MySQL(), group="vitalAnalysis")
#' rs <- dbSendQuery(con,
#'              "select Agent, ip_addr, DATA from pseudo_data order by Agent")
#' out <- dbApply(rs, INDEX = "Agent",
#'         FUN = function(x, grp) quantile(x$DATA, names=FALSE))
#' }
setGeneric("dbApply", def = function(res, ...) standardGeneric("dbApply"))

#' @export
#' @rdname dbApply
setMethod("dbApply", "MySQLResult",
  def = function(res, ...)  mysqlDBApply(res, ...),
)

#' Fetch next result set from an SQL script or stored procedure (experimental)
#'
#' Fetches the next result set from the output of a multi-statement SQL script
#' or stored procedure; checkes whether there are additonal result sets to
#' process.
#'
#'
#' SQL scripts (i.e., multiple SQL statements separated by ';') and stored
#' procedures oftentimes generate multiple result sets.  These DBI generic
#' functions provide a means to process them sequentially.
#'
#' \code{dbNextResult} fetches the next result from the sequence of pending
#' results sets; \code{dbMoreResults} returns a logical to indicate whether
#' there are additional results to process.
#'
#' @aliases dbNextResult dbMoreResults
#' @param con a connection object (see \code{\link[DBI]{dbConnect}}).
#' @param ... any additional arguments to be passed to the dispatched method
#' @return \code{dbNextResult} returns a result set or \code{NULL}.
#'
#' \code{dbMoreResults} returns a logical specifying whether or not there are
#' additional result sets to process in the connection.
#' @section Note: Currently only the \code{\link{MySQL}} driver implements
#' these methods.  See 'methods?dbNextMethod'.
#' @seealso \code{\link{MySQL}} \code{\link[DBI]{dbConnect}}
#' \code{\link[DBI]{dbSendQuery}} \code{\link[DBI]{fetch}}
#' @keywords programming interface database
#' @export
#' @examples
#' \dontrun{
#' con <- dbConnect(MySQL(),
#'           dbname = "rs-dbi",
#'           client.flag=CLIENT_MULTI_STATEMENTS)
#' sql.script <- paste(
#'    "select * from abc",
#'    "select * def",
#'    collapse = ";")
#'
#' rs1 <- dbSendQuery(con, sql.script)
#' data1 <- fetch(rs1, n = -1)
#'
#' if(dbMoreResults(con)){
#'    rs2 <- dbNextResult(con)
#'    ## you could use dbHasCompleted(rs2) to determine whether
#'    ## rs2 is a select-like that generates output or not.
#'    data2 <- fetch(rs2, n = -1)
#'    }
#' }
#'
setGeneric("dbNextResult",
  def = function(con, ...) standardGeneric("dbNextResult")
  #valueClass = "DBIResult" or NULL
)

#' @export
#' @rdname dbNextResult
setMethod("dbNextResult",
  signature(con = "MySQLConnection"),
  def = function(con, ...){
    for(rs in dbListResults(con)){
      dbClearResult(rs)
    }
    id = .Call("RS_MySQL_nextResultSet", as(con, "integer"),
      PACKAGE=.MySQLPkgName)
    new("MySQLResult", Id = id)
  }
)

#' @export
#' @rdname dbNextResult
setGeneric("dbMoreResults",
  def = function(con, ...) standardGeneric("dbMoreResults"),
  valueClass = "logical"
)

#' @export
#' @rdname dbNextResult
setMethod("dbMoreResults",
  signature(con = "MySQLConnection"),
  def = function(con, ...)
    .Call("RS_MySQL_moreResultSets", as(con, "integer"),
      PACKAGE=.MySQLPkgName)
)


#' Apply R/S-Plus functions to remote groups of DBMS rows (experimental)
#'
#' Applies R/S-Plus functions to groups of remote DBMS rows without bringing an
#' entire result set all at once.  The result set is expected to be sorted by
#' the grouping field.
#'
#' \code{dbApply} This function is meant to handle somewhat gracefully(?) large
#' amounts of data from the DBMS by bringing into R manageable chunks (about
#' \code{batchSize} records at a time, but not more than \code{maxBatch}); the
#' idea is that the data from individual groups can be handled by R, but not
#' all the groups at the same time.
#'
#' The MySQL implementation \code{mysqlDBApply} allows us to register R
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
#' number that defines the various groups.
#' @param FUN a function to be invoked upon identifying the last row from every
#' group. This function will be passed a data frame holding the records of the
#' current group, a character string with the group label, plus any other
#' arguments passed to \code{dbApply} as \code{"..."}.
#' @param begin a function of no arguments to be invoked just prior to retrieve
#' the first row from the result set.
#' @param end a function of no arguments to be invoked just after retrieving
#' the last row from the result set.
#' @param group.begin a function of one argument (the group label) to be
#' invoked upon identifying a row from a new group
#' @param new.record a function to be invoked as each individual record is
#' fetched.  The first argument to this function is a one-row data.frame
#' holding the new record.
#' @param batchSize the default number of rows to bring from the remote result
#' set. If needed, this is automatically extended to hold groups bigger than
#' \code{batchSize}.
#' @param maxBatch the absolute maximum of rows per group that may be extracted
#' from the result set.
#' @param ... any additional arguments to be passed to \code{FUN}.
#' @param simplify Not yet implemented
#' @return A list with as many elements as there were groups in the result set.
#' @note This is an experimental version implemented only in R (there are
#' plans, time permitting, to implement it in S-Plus).
#'
#' The terminology that we're using is closer to SQL than R.  In R what we're
#' referring to ``groups'' are the individual levels of a factor (grouping
#' field in our terminology).
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{dbSendQuery}},
#' \code{\link[DBI]{fetch}}.
#' @keywords programming interface database
#' @examples
#' \dontrun{
#' ## compute quanitiles for each network agent
#' con <- dbConnect(MySQL(), group="vitalAnalysis")
#' res <- dbSendQuery(con,
#'              "select Agent, ip_addr, DATA from pseudo_data order by Agent")
#' out <- dbApply(res, INDEX = "Agent",
#'         FUN = function(x, grp) quantile(x$DATA, names=FALSE))
#' }
#'
mysqlDBApply <-
  function(res, INDEX, FUN = stop("must specify FUN"),
    begin = NULL,
    group.begin =  NULL,
    new.record = NULL,
    end = NULL,
    batchSize = 100, maxBatch = 1e6,
    ..., simplify = TRUE)
    ## (Experimental)
    ## This function is meant to handle somewhat gracefully(?) large amounts
    ## of data from the DBMS by bringing into R manageable chunks (about
    ## batchSize records at a time, but not more than maxBatch); the idea
    ## is that the data from individual groups can be handled by R, but
    ## not all the groups at the same time.
    ##
    ## dbApply apply functions to groups of rows coming from a remote
    ## database resultSet upon the following fetching events:
    ##   begin         (prior to fetching the first record)
    ##   group.begin   (the record just fetched begins a new group)
##   new_record    (a new record just fetched)
##   group.end     (the record just fetched ends the current group)
##   end           (the record just fetched is the very last record)
##
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
    rsId <- as(res, "integer")
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
    out <- .Call("RS_MySQL_dbApply",
      rs = rsId,
      INDEX = as.integer(INDEX-1),
      funs, rho, as.integer(batchSize), as.integer(maxBatch),
      PACKAGE = .MySQLPkgName)
    if(!is.null(end) && dbHasCompleted(res))
      end()
    out
  }



## Note that originally we had only resultSet both for SELECTs
## and INSERTS, ...  Later on we created a base class dbResult
## for non-Select SQL and a derived class resultSet for SELECTS.







#' Build the SQL CREATE TABLE definition as a string
#'
#' Build the SQL CREATE TABLE definition as a string for the input data.frame
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
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{dbConnect}},
#' \code{\link[DBI]{dbSendQuery}}, \code{\link[DBI]{dbGetQuery}},
#' \code{\link[DBI]{fetch}}, \code{\link[DBI]{dbCommit}},
#' \code{\link[DBI]{dbGetInfo}}, \code{\link[DBI]{dbReadTable}}.
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
dbBuildTableDefinition <-
  function(dbObj, name, obj, field.types = NULL, row.names = TRUE, ...)
  {
    if(!is.data.frame(obj))
      obj <- as.data.frame(obj)
    if(!is.null(row.names) && row.names){
      obj  <- cbind(row.names(obj), obj)  ## can't use row.names= here
      names(obj)[1] <- "row.names"
    }
    if(is.null(field.types)){
      ## the following mapping should be coming from some kind of table
      ## also, need to use converter functions (for dates, etc.)
      field.types <- lapply(obj, dbDataType, dbObj = dbObj)
    }
    i <- match("row.names", names(field.types), nomatch=0)
    if(i>0) ## did we add a row.names value?  If so, it's a text field.
      field.types[i] <- dbDataType(dbObj, field.types$row.names)
    names(field.types) <-
      make.db.names(dbObj, names(field.types), allow.keywords = FALSE)

    ## need to create a new (empty) table
    flds <- paste(names(field.types), field.types)
    paste("CREATE TABLE", name, "\n(", paste(flds, collapse=",\n\t"), "\n)")
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

## the following is almost exactly from the ROracle driver


#' Write a data.frame avoiding exceeding memory limits
#'
#' This function batches calls to \code{write.table} to avoid exceeding memory
#' limits for very large data.frames.
#'
#' The function has a while loop invoking \code{\link{write.table}} for subsets
#' of \code{batch} rows of \code{value}.  Since this is a helper function for
#' \code{mysqlWriteTable}, it has hardcoded other arguments to
#' \code{write.table}.
#'
#' @param value a data.frame;
#' @param file a file object (connection, file name, etc).
#' @param batch maximum number of rows to write at a time.
#' @param \dots any other arguments are passed to \code{write.table}.
#' @return \code{NULL}, invisibly.
#' @note No error checking whatsoever is done.
#' @seealso \code{\link{write.table}}
#' @keywords internal
#' @examples
#' \dontrun{
#'    ctr.file <- file("dump.sqloader", "w")
#'    safe.write(big.data, file = ctr.file, batch = 25000)
#' }
#'
safe.write <-
  function(value, file, batch, ...)
    ## safe.write makes sure write.table doesn't exceed available memory by batching
    ## at most batch rows (but it is still slowww)
  {
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
        append = TRUE, quote = FALSE, sep="\t", na = .MySQL.NA.string,
        row.names=FALSE, col.names=FALSE, eol = '\n', ...)
      from <- to+1
      to <- min(to+batch, N)
    }
    close(conb)
    invisible(NULL)
  }

mysqlDataType <-
  function(obj, ...)
    ## find a suitable SQL data type for the R/S object obj
    ## TODO: Lots and lots!! (this is a very rough first draft)
    ## need to register converters, abstract out MySQL and generalize
    ## to Oracle, Informix, etc.  Perhaps this should be table-driven.
    ## NOTE: MySQL data types differ from the SQL92 (e.g., varchar truncate
    ## trailing spaces).  MySQL enum() maps rather nicely to factors (with
    ## up to 65535 levels)
  {
    rs.class <- data.class(obj)    ## this differs in R 1.4 from older vers
    rs.mode <- storage.mode(obj)
    if(rs.class=="numeric" || rs.class == "integer"){
      sql.type <- if(rs.mode=="integer") "bigint" else  "double"
    }
    else {
      sql.type <- switch(rs.class,
        character = "text",
        logical = "tinyint",  ## but we need to coerce to int!!
        factor = "text",      ## up to 65535 characters
        ordered = "text",
        "text")
    }
    sql.type
  }


## For testing compiled against loaded mysql client library versions


#' MySQL Check for Compiled Versus Loaded Client Library Versions
#'
#' This function prints out the compiled and loaded client library versions.
#'
#'
#' @return A named integer vector of length two, the first element representing
#' the compiled library version and the second element representint the loaded
#' client library version.
#' @keywords database
mysqlClientLibraryVersions <-
  function()
  {
    .Call("RS_MySQL_clientLibraryVersions",PACKAGE=.MySQLPkgName)
  }

## the following reserved words were taken from Section 6.1.7
## of the MySQL Manual, Version 4.1.1-alpha, html format.

.MySQLKeywords <-
  c("ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE",
    "AUTO_INCREMENT", "BDB", "BEFORE", "BERKELEYDB", "BETWEEN", "BIGINT",
    "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE",
    "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN", "COLUMNS",
    "CONDITION", "CONNECTION", "CONSTRAINT", "CONTINUE", "CREATE",
    "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
    "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND",
    "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT",
    "DELAYED", "DELETE", "DESC", "DESCRIBE", "DISTINCT", "DISTINCTROW",
    "DIV", "DOUBLE", "DROP", "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED",
    "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FIELDS", "FLOAT",
    "FOR", "FORCE", "FOREIGN", "FOUND", "FROM", "FULLTEXT", "GRANT",
    "GROUP", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE",
    "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER",
    "INNODB", "INOUT", "INSENSITIVE", "INSERT", "INT", "INTEGER",
    "INTERVAL", "INTO", "IO_THREAD", "IS", "ITERATE", "JOIN", "KEY",
    "KEYS", "KILL", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT",
    "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG",
    "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY", "MASTER_SERVER_ID",
    "MATCH", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT",
    "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "NATURAL", "NOT",
    "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE", "OPTION",
    "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "PRECISION",
    "PRIMARY", "PRIVILEGES", "PROCEDURE", "PURGE", "READ", "REAL",
    "REFERENCES", "REGEXP", "RENAME", "REPEAT", "REPLACE", "REQUIRE",
    "RESTRICT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "RLIKE",
    "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET",
    "SHOW", "SMALLINT", "SOME", "SONAME", "SPATIAL", "SPECIFIC",
    "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT",
    "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING",
    "STRAIGHT_JOIN", "STRIPED", "TABLE", "TABLES", "TERMINATED",
    "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING",
    "TRUE", "TYPES", "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED",
    "UPDATE", "USAGE", "USE", "USER_RESOURCES", "USING", "UTC_DATE",
    "UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR",
    "VARCHARACTER", "VARYING", "WHEN", "WHERE", "WHILE", "WITH",
    "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL"
  )

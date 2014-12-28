#' Class MySQLResult
#'
#' MySQL's query results class.  This classes encapsulates the result of an SQL
#' statement (either \code{select} or not).
#'
#' @export
#' @keywords internal
setClass("MySQLResult", representation("DBIResult", "MySQLObject"))

setAs("MySQLResult", "MySQLDriver", function(from) {
  new("MySQLDriver", Id = as(from, "integer")[1:2])
})
setAs("MySQLResult", "MySQLConnection", function(from) {
  new("MySQLConnection", Id = as(from, "integer")[1:3])
})

#' Execute a SQL statement on a database connection.
#'
#' To retrieve results a chunk at a time, use \code{dbSendQuery},
#' \code{dbFetch}, then \code{dbClearResult}. Alternatively, if you want all the
#' results (and they'll fit in memory) use \code{dbGetQuery} which sends,
#' fetches and clears for you.
#'
#' @param conn an \code{\linkS4class{SQLiteConnection}} object.
#' @param statement a character vector of length one specifying the SQL
#'   statement that should be executed.  Only a single SQL statment should be
#'   provided.
#' @param ... Unused. Needed for compatibility with generic.
#' @export
#' @examples
#' \dontrun{
#' con <- dbConnect(RMySQL::MySQL())
#' dbWriteTable(con, "arrests", datasets::USArrests)
#'
#' # Run query to get results as dataframe
#' dbGetQuery(con, "SELECT * FROM arrests limit 3")
#'
#' # Send query to pull requests in batches
#' res <- dbSendQuery(con, "SELECT * FROM arrests")
#' data <- fetch(res, n = 2)
#' data
#' dbHasCompleted(res)
#'
#' dbListResults(con)
#' dbClearResult(res)
#'
#' dbDisconnect(con)
#' }
#' @name query
setMethod("fetch", signature(res="MySQLResult", n="numeric"),
  def = function(res, n, ...){
    n <- as(n, "integer")
    rsId <- as(res, "integer")
    rel <- .Call("RS_MySQL_fetch", rsId, nrec = n, PACKAGE = .MySQLPkgName)
    if(length(rel)==0 || length(rel[[1]])==0)
      return(data.frame())
    ## create running row index as of previous fetch (if any)
    cnt <- dbGetRowCount(res)
    nrec <- length(rel[[1]])
    indx <- seq(from = cnt - nrec + 1, length = nrec)
    attr(rel, "row.names") <- as.integer(indx)
    class(rel) <- "data.frame"
  }
)

#' @param n maximum number of records to retrieve per fetch. Use \code{-1} to
#'    retrieve all pending records; use \code{0} for to fetch the default
#'    number of rows as defined in \code{\link{SQLite}}
#' @rdname query
#' @export
setMethod("fetch", c("MySQLResult", "missing"), function(res, n, ...) {
  fetch(res, n = 0, ...)
})

#' @rdname query
#' @export
setMethod("dbSendQuery", c("MySQLConnection", "character"),
  function(conn, statement) {
    if(!isIdCurrent(conn))
      stop(paste("expired", class(conn)))
    conId <- as(conn, "integer")
    statement <- as(statement, "character")
    rsId <- .Call("RS_MySQL_exec", conId, statement, PACKAGE = .MySQLPkgName)
    new("MySQLResult", Id = rsId)
  }
)

#' @rdname query
#' @export
setMethod("dbGetQuery", c("MySQLConnection", "character"),
  function(conn, statement) {
    if(!isIdCurrent(conn)) stop(paste("expired", class(conn)))

    ## are there resultSets pending on con?
    .clearResultSets(conn)

    rs <- dbSendQuery(conn, statement)
    if(dbHasCompleted(rs)){
      dbClearResult(rs)            ## no records to fetch, we're done
      invisible()
      return(NULL)
    }
    res <- fetch(rs, n = -1)
    if(dbHasCompleted(rs))
      dbClearResult(rs)
    else
      warning("pending rows")
    res
  }
)

#' @rdname query
#' @export
setMethod("dbClearResult", "MySQLResult", function(res, ...) {
  if(!isIdCurrent(res))
    return(TRUE)
  rsId <- as(res, "integer")
  .Call("RS_MySQL_closeResultSet", rsId, PACKAGE = .MySQLPkgName)
})


#' @rdname query
#' @export
setMethod("dbGetInfo", "MySQLResult", function(dbObj, what = "", ...) {
  if(!isIdCurrent(dbObj))
    stop(paste("expired", class(dbObj), deparse(substitute(dbObj))))
  id <- as(dbObj, "integer")
  info <- .Call("RS_MySQL_resultSetInfo", id, PACKAGE = .MySQLPkgName)
  if(!missing(what))
    info[what]
  else
    info
})

#' @rdname query
#' @export
setMethod("dbGetStatement", "MySQLResult",
  def = function(res, ...){
    st <- dbGetInfo(res, "statement")[[1]]
    if(is.null(st))
      st <- character()
    st
  },
  valueClass = "character"
)

#' @rdname query
#' @export
setMethod("dbListFields",
  signature(conn="MySQLResult", name="missing"),
  def = function(conn, name, ...){
    flds <- dbGetInfo(conn, "fields")$fields$name
    if(is.null(flds))
      flds <- character()
    flds
  },
  valueClass = "character"
)

#' Database interface meta-data.
#'
#' See documentation of generics for more details.
#'
#' @param res An object of class \code{\linkS4class{SQLiteResult}}
#' @param ... Ignored. Needed for compatibility with generic
#' @examples
#' \dontrun{
#' con <- dbConnect(RMySQL::MySQL())
#' dbWriteTable(con, "t1", datasets::USArrests)
#'
#' rs <- dbSendQuery(con, "SELECT * FROM t1 WHERE UrbanPop >= 80")
#' dbGetStatement(rs)
#' dbHasCompleted(rs)
#'
#' info <- dbGetInfo(rs)
#' names(info)
#' info$fields
#'
#' dbClearResult(rs)
#' dbDisconnect(con)
#' }
#' @name sqlite-meta
NULL

#' @export
#' @rdname sqlite-meta
setMethod("dbColumnInfo", "MySQLResult", function(res, ...) {
  flds <- dbGetInfo(res, "fieldDescription")[[1]][[1]]
  if(!is.null(flds)){
    flds$Sclass <- .Call("RS_DBI_SclassNames", flds$Sclass,
      PACKAGE = .MySQLPkgName)
    flds$type <- .Call("RS_MySQL_typeNames", as.integer(flds$type),
      PACKAGE = .MySQLPkgName)
    ## no factors
    structure(flds, row.names = paste(seq(along=flds$type)),
      class = "data.frame")
  }
  else data.frame(flds)
}
)

#' @export
#' @rdname sqlite-meta
setMethod("dbGetRowsAffected", "MySQLResult", function(res, ...) {
  dbGetInfo(res, "rowsAffected")[[1]]
})

#' @export
#' @rdname sqlite-meta
setMethod("dbGetRowCount", "MySQLResult", function(res, ...) {
  dbGetInfo(res, "rowCount")[[1]]
})

#' @export
#' @rdname sqlite-meta
setMethod("dbHasCompleted", "MySQLResult", function(res, ...) {
  dbGetInfo(res, "completed")[[1]] == 1
})

#' @export
#' @rdname sqlite-meta
setMethod("dbGetException", "MySQLResult", function(conn, ...) {
  id <- as(conn, "integer")[1:2]
  .Call("RS_MySQL_getException", id, PACKAGE = .MySQLPkgName)
})

#' @export
setMethod("summary", "MySQLResult", function(object, verbose = FALSE, ...) {
  if(!isIdCurrent(object)){
    print(object)
    invisible(return(NULL))
  }
  print(object)
  cat("  Statement:", dbGetStatement(object), "\n")
  cat("  Has completed?", if(dbHasCompleted(object)) "yes" else "no", "\n")
  cat("  Affected rows:", dbGetRowsAffected(object), "\n")
  cat("  Rows fetched:", dbGetRowCount(object), "\n")
  flds <- dbColumnInfo(object)
  if(verbose && !is.null(flds)){
    cat("  Fields:\n")
    out <- print(dbColumnInfo(object))
  }
  invisible(NULL)
})

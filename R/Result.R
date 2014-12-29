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
#' @param conn an \code{\linkS4class{MySQLConnection}} object.
#' @param res,dbObj A  \code{\linkS4class{MySQLResult}} object.
#' @param statement a character vector of length one specifying the SQL
#'   statement that should be executed.  Only a single SQL statment should be
#'   provided.
#' @param ... Unused. Needed for compatibility with generic.
#' @export
#' @examples
#' if (mysqlHasDefault()) {
#' con <- dbConnect(RMySQL::MySQL())
#' dbWriteTable(con, "arrests", datasets::USArrests, overwrite = TRUE)
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
#' dbRemoveTable(con, "arrests")
#' dbDisconnect(con)
#' }
#' @rdname query
#' @useDynLib RMySQL RS_MySQL_fetch
setMethod("fetch", signature(res="MySQLResult", n="numeric"),
  def = function(res, n, ...){
    n <- as(n, "integer")
    rsId <- as(res, "integer")
    rel <- .Call(RS_MySQL_fetch, rsId, nrec = n)
    if(length(rel)==0 || length(rel[[1]])==0)
      return(data.frame())
    ## create running row index as of previous fetch (if any)
    cnt <- dbGetRowCount(res)
    nrec <- length(rel[[1]])
    indx <- seq(from = cnt - nrec + 1, length = nrec)
    attr(rel, "row.names") <- as.integer(indx)
    class(rel) <- "data.frame"
    rel
  }
)

#' @param n maximum number of records to retrieve per fetch. Use \code{-1} to
#'    retrieve all pending records; use \code{0} for to fetch the default
#'    number of rows as defined in \code{\link{MySQL}}
#' @rdname query
#' @export
setMethod("fetch", c("MySQLResult", "missing"), function(res, n, ...) {
  fetch(res, n = 0, ...)
})

#' @rdname query
#' @export
#' @useDynLib RMySQL RS_MySQL_exec
setMethod("dbSendQuery", c("MySQLConnection", "character"),
  function(conn, statement) {
    if(!isIdCurrent(conn))
      stop(paste("expired", class(conn)))
    conId <- as(conn, "integer")
    statement <- as(statement, "character")
    rsId <- .Call(RS_MySQL_exec, conId, statement)
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
#' @useDynLib RMySQL RS_MySQL_closeResultSet
setMethod("dbClearResult", "MySQLResult", function(res, ...) {
  if(!isIdCurrent(res))
    return(TRUE)
  rsId <- as(res, "integer")
  .Call(RS_MySQL_closeResultSet, rsId)
})


#' @rdname query
#' @param what optional
#' @export
#' @useDynLib RMySQL RS_MySQL_resultSetInfo
setMethod("dbGetInfo", "MySQLResult", function(dbObj, what = "", ...) {
  if(!isIdCurrent(dbObj))
    stop(paste("expired", class(dbObj), deparse(substitute(dbObj))))
  id <- as(dbObj, "integer")
  info <- .Call(RS_MySQL_resultSetInfo, id)
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

#' @param name Table name.
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
#' @param res,conn,object An object of class \code{\linkS4class{MySQLResult}}
#' @param ... Ignored. Needed for compatibility with generic
#' @examples
#' if (mysqlHasDefault()) {
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
#' dbRemoveTable(con, "t1")
#' dbDisconnect(con)
#' }
#' @name result-meta
NULL

#' @export
#' @rdname result-meta
#' @useDynLib RMySQL RS_DBI_SclassNames RS_MySQL_typeNames
setMethod("dbColumnInfo", "MySQLResult", function(res, ...) {
  flds <- dbGetInfo(res, "fieldDescription")[[1]][[1]]
  if(!is.null(flds)){
    flds$Sclass <- .Call(RS_DBI_SclassNames, flds$Sclass)
    flds$type <- .Call(RS_MySQL_typeNames, as.integer(flds$type))
    ## no factors
    structure(flds, row.names = paste(seq(along=flds$type)),
      class = "data.frame")
  }
  else data.frame(flds)
}
)

#' @export
#' @rdname result-meta
setMethod("dbGetRowsAffected", "MySQLResult", function(res, ...) {
  dbGetInfo(res, "rowsAffected")[[1]]
})

#' @export
#' @rdname result-meta
setMethod("dbGetRowCount", "MySQLResult", function(res, ...) {
  dbGetInfo(res, "rowCount")[[1]]
})

#' @export
#' @rdname result-meta
setMethod("dbHasCompleted", "MySQLResult", function(res, ...) {
  dbGetInfo(res, "completed")[[1]] == 1
})

#' @export
#' @rdname result-meta
#' @useDynLib RMySQL RS_MySQL_getException
setMethod("dbGetException", "MySQLResult", function(conn, ...) {
  id <- as(conn, "integer")[1:2]
  .Call(RS_MySQL_getException, id)
})

#' @export
#' @param verbose If \code{TRUE}, print extra information.
#' @rdname result-meta
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

.clearResultSets <- function(con){
  ## are there resultSets pending on con?
  rsList <- dbListResults(con)
  if(length(rsList)>0){
    warning("There are pending result sets. Removing.",call.=FALSE)
    lapply(rsList,dbClearResult) ## clear all pending results
  }
  NULL
}

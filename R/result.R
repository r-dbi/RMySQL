#' Class MySQLResult
#'
#' MySQL's query results class.  This classes encapsulates the result of an SQL
#' statement (either \code{select} or not).
#'
#' @export
#' @keywords internal
setClass("MySQLResult",
  contains = "DBIResult",
  slots = list(Id = "integer")
)

setAs("MySQLResult", "MySQLConnection", function(from) {
  new("MySQLConnection", Id = from@Id[1:2])
})

mysqlFetch <- function(res, n, ...) {
  rel <- .Call(RS_MySQL_fetch, res@Id, nrec = as.integer(n))
  if (is.null(rel))
    return(data.frame())

  if (length(rel) > 0) {
    n <- length(rel[[1]])
  } else {
    n <- 0
  }

  attr(rel, "row.names") <- .set_row_names(n)
  class(rel) <- "data.frame"
  rel
}

#' Execute a SQL statement on a database connection.
#'
#' To retrieve results a chunk at a time, use \code{dbSendQuery},
#' \code{dbFetch}, then \code{dbClearResult}. Alternatively, if you want all the
#' results (and they'll fit in memory) use \code{dbGetQuery} which sends,
#' fetches and clears for you.
#'
#' \code{fetch()} will be deprecated in the near future; please use
#' \code{dbFetch()} instead.
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
#' con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#' dbWriteTable(con, "arrests", datasets::USArrests, overwrite = TRUE)
#'
#' # Run query to get results as dataframe
#' dbGetQuery(con, "SELECT * FROM arrests limit 3")
#'
#' # Send query to pull requests in batches
#' res <- dbSendQuery(con, "SELECT * FROM arrests")
#' data <- dbFetch(res, n = 2)
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
setMethod("dbFetch", c("MySQLResult", "numeric"), mysqlFetch)

#' @export
#' @rdname query
setMethod("fetch", c("MySQLResult", "numeric"), mysqlFetch)

#' @param n maximum number of records to retrieve per fetch. Use \code{-1} to
#'    retrieve all pending records; use \code{0} for to fetch the default
#'    number of rows as defined in \code{\link{MySQL}}
#' @rdname query
#' @export
setMethod("dbFetch", c("MySQLResult", "missing"), function(res, n, ...) {
  mysqlFetch(res, n = 0, ...)
})

#' @rdname query
#' @export
setMethod("fetch", c("MySQLResult", "missing"), function(res, n, ...) {
  mysqlFetch(res, n = 0, ...)
})

#' @rdname query
#' @export
#' @useDynLib RMySQL RS_MySQL_exec
setMethod("dbSendQuery", c("MySQLConnection", "character"),
  function(conn, statement) {
    checkValid(conn)

    rsId <- .Call(RS_MySQL_exec, conn@Id, as.character(statement))
    new("MySQLResult", Id = rsId)
  }
)

#' @rdname query
#' @export
#' @useDynLib RMySQL RS_MySQL_closeResultSet
setMethod("dbClearResult", "MySQLResult", function(res, ...) {
  if (!dbIsValid(res)) return(TRUE)

  .Call(RS_MySQL_closeResultSet, res@Id)
})


#' @rdname query
#' @param what optional
#' @export
#' @useDynLib RMySQL RS_MySQL_resultSetInfo
setMethod("dbGetInfo", "MySQLResult", function(dbObj, what = "", ...) {
  checkValid(dbObj)

  info <- .Call(RS_MySQL_resultSetInfo, dbObj@Id)

  if (!missing(what)) {
    info[what]
  } else {
    info
  }
})

#' @rdname query
#' @export
setMethod("dbGetStatement", "MySQLResult", function(res, ...) {
  dbGetInfo(res)$statement
})

#' @param name Table name.
#' @rdname query
#' @export
#' @useDynLib RMySQL rmysql_fields_info
setMethod("dbListFields", c("MySQLResult", "missing"), function(conn, name, ...) {
  .Call(rmysql_fields_info, conn@Id)$name
})

#' Database interface meta-data.
#'
#' See documentation of generics for more details.
#'
#' @param res,conn,object An object of class \code{\linkS4class{MySQLResult}}
#' @param ... Ignored. Needed for compatibility with generic
#' @examples
#' if (mysqlHasDefault()) {
#' con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#' dbWriteTable(con, "t1", datasets::USArrests, overwrite = TRUE)
#'
#' rs <- dbSendQuery(con, "SELECT * FROM t1 WHERE UrbanPop >= 80")
#' dbGetStatement(rs)
#' dbHasCompleted(rs)
#'
#' dbGetInfo(rs)
#' dbColumnInfo(rs)
#'
#' dbClearResult(rs)
#' dbRemoveTable(con, "t1")
#' dbDisconnect(con)
#' }
#' @name result-meta
NULL

#' @export
#' @rdname result-meta
setMethod("dbColumnInfo", "MySQLResult", function(res, ...) {
  as.data.frame(.Call(rmysql_fields_info, res@Id))
})

#' @export
#' @rdname result-meta
setMethod("dbGetRowsAffected", "MySQLResult", function(res, ...) {
  dbGetInfo(res)$rowsAffected
})

#' @export
#' @rdname result-meta
setMethod("dbGetRowCount", "MySQLResult", function(res, ...) {
  dbGetInfo(res)$rowCount
})

#' @export
#' @rdname result-meta
setMethod("dbHasCompleted", "MySQLResult", function(res, ...) {
  dbGetInfo(res)$completed == 1
})

#' @export
#' @rdname result-meta
#' @useDynLib RMySQL rmysql_exception_info
setMethod("dbGetException", "MySQLResult", function(conn, ...) {
  .Call(rmysql_exception_info, conn@Id[1:2])
})

#' @export
#' @param verbose If \code{TRUE}, print extra information.
#' @rdname result-meta
setMethod("summary", "MySQLResult", function(object, verbose = FALSE, ...) {
  checkValid(object)

  print(object)
  cat("  Statement:", dbGetStatement(object), "\n")
  cat("  Has completed?", if(dbHasCompleted(object)) "yes" else "no", "\n")
  cat("  Affected rows:", dbGetRowsAffected(object), "\n")
  cat("  Rows fetched:", dbGetRowCount(object), "\n")

  flds <- dbColumnInfo(object)
  if (verbose && !is.null(flds)) {
    cat("  Fields:\n")
    print(dbColumnInfo(object))
  }
  invisible(NULL)
})

#' @export
#' @rdname result-meta
setMethod("show", "MySQLResult", function(object) {
  expired <- if (dbIsValid(object)) "" else "Expired "
  cat("<", expired, "MySQLResult:", paste(object@Id, collapse = ","), ">\n",
    sep = "")
  invisible(NULL)
})

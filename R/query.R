#' @include MySQLConnection.R
#' @include MySQLResult.R
NULL

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
#' @inheritParams SQL::rownamesToColumn
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
setMethod("dbFetch", c("MySQLResult", "numeric"),
  function(res, n = -1, ..., row.names = NA) {
    SQL::columnToRownames(result_fetch(res@ptr, n), row.names)
  }
)

#' @rdname query
#' @export
setMethod("dbSendQuery", c("MySQLConnection", "character"),
  function(conn, statement) {
    new("MySQLResult",
      ptr = result_create(conn@ptr, statement),
      sql = statement)
  }
)

#' @export
#' @rdname query
setMethod("dbGetQuery", signature("MySQLConnection", "character"),
  function(conn, statement, ..., params = NULL, row.names = NA) {
    rs <- dbSendQuery(conn, statement, ...) # params = params
    on.exit(dbClearResult(rs))

    dbFetch(rs, n = -1, ..., row.names = row.names)
  }
)

#' @rdname query
#' @export
#' @useDynLib RMySQL RS_MySQL_closeResultSet
setMethod("dbClearResult", "MySQLResult", function(res, ...) {
  result_release(res@ptr)
  TRUE
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
  res@sql
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
  result_column_info(res@ptr)
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

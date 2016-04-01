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
#' @param res A  \code{\linkS4class{MySQLResult}} object.
#' @inheritParams DBI::sqlRownamesToColumn
#' @param n Number of rows to retrieve. Use -1 to retrieve all rows.
#' @param params A list of query parameters to be substituted into
#'   a parameterised query.
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
#' dbClearResult(res)
#' dbRemoveTable(con, "arrests")
#' dbDisconnect(con)
#' }
#' @rdname query
setMethod("dbFetch", c("MySQLResult", "numeric"),
  function(res, n = -1, ..., row.names = NA) {
    sqlColumnToRownames(result_fetch(res@ptr, n), row.names)
  }
)

#' @rdname query
#' @export
setMethod("dbSendQuery", c("MySQLConnection", "character"),
  function(conn, statement, params = NULL, ...) {
    statement <- enc2utf8(statement)

    rs <- new("MySQLResult",
      sql = statement,
      ptr = result_create(conn@ptr, statement)
    )

    if (!is.null(params)) {
      dbBind(rs, params)
    }

    rs
  }
)

#' @rdname query
#' @export
setMethod("dbBind", "MySQLResult", function(res, params, ...) {
  result_bind(res@ptr, params)
  TRUE
})

#' @rdname query
#' @export
setMethod("dbClearResult", "MySQLResult", function(res, ...) {
  result_release(res@ptr)
  TRUE
})

#' @rdname query
#' @export
setMethod("dbGetStatement", "MySQLResult", function(res, ...) {
  res@sql
})

#' Database interface meta-data.
#'
#' See documentation of generics for more details.
#'
#' @param res An object of class \code{\linkS4class{MySQLResult}}
#' @param ... Ignored. Needed for compatibility with generic
#' @examples
#' if (mysqlHasDefault()) {
#' con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#' dbWriteTable(con, "t1", datasets::USArrests, overwrite = TRUE)
#'
#' rs <- dbSendQuery(con, "SELECT * FROM t1 WHERE UrbanPop >= 80")
#' rs
#'
#' dbGetStatement(rs)
#' dbHasCompleted(rs)
#' dbColumnInfo(rs)
#'
#' dbFetch(rs)
#' rs
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
  result_rows_affected(res@ptr)
})

#' @export
#' @rdname result-meta
setMethod("dbGetRowCount", "MySQLResult", function(res, ...) {
  result_rows_fetched(res@ptr)
})

#' @export
#' @rdname result-meta
setMethod("dbHasCompleted", "MySQLResult", function(res, ...) {
  result_complete(res@ptr)
})


#' Execute a query on the server (no binding).
#'
#' MySQL has two APIs for submitting queries, one for parameterised queries
#' and one for unparameterised. In most cases, you can use the parameterised
#' query interface even if you have zero parameter. However, some queries
#' (e.g. transaction modification) can not be executed through the
#' paramaterised interface. This function allows you to submit those queries
#' directly.
#'
#' @param con A MySQL connection.
#' @param sql A sql string to execute
#' @export
mysqlExecQuery <- function(con, sql) {
  connection_exec(con@ptr, sql)
}

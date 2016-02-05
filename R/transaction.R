#' @include MySQLConnection.R
NULL

#' DBMS Transaction Management
#'
#' Commits or roll backs the current transaction in an MySQL connection.
#' Note that in MySQL DDL statements (e.g. \code{CREATE TABLE}) can not
#' be rolled back.
#'
#' @param conn a \code{MySQLConnection} object, as produced by
#'  \code{\link{dbConnect}}.
#' @param ... Unused.
#' @examples
#' if (mysqlHasDefault()) {
#' library(DBI)
#' con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#' df <- data.frame(id = 1:5)
#'
#' dbWriteTable(con, "df", df)
#' dbBegin(con)
#' dbGetQuery(con, "UPDATE df SET id = id * 10")
#' dbGetQuery(con, "SELECT id FROM df")
#' dbRollback(con)
#'
#' dbGetQuery(con, "SELECT id FROM df")
#'
#' dbRemoveTable(con, "df")
#' dbDisconnect(con)
#' }
#' @name transactions
NULL

#' @export
#' @rdname transactions
setMethod("dbCommit", "MySQLConnection", function(conn, ...) {
  mysqlExecQuery(conn, "COMMIT")
})

#' @export
#' @rdname transactions
setMethod("dbBegin", "MySQLConnection", function(conn, ...) {
  mysqlExecQuery(conn, "START TRANSACTION")
})

#' @export
#' @rdname transactions
setMethod("dbRollback", "MySQLConnection", function(conn, ...) {
  mysqlExecQuery(conn, "ROLLBACK")
})

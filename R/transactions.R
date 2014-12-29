#' @include Connection.R
NULL

#' DBMS Transaction Management
#'
#' Commits or roll backs the current transaction in an MySQL connection.
#'
#' @param conn a \code{MySQLConnection} object, as produced by
#'  \code{\link{dbConnect}}.
#' @param ... Unused.
setMethod("dbCommit", "MySQLConnection",
  def = function(conn, ...) .NotYetImplemented()
)

#' @export
#' @rdname dbCommit-MySQLConnection-method
setMethod("dbRollback", "MySQLConnection",
  def = function(conn, ...) .NotYetImplemented()
)

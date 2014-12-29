#' @include Connection.R
NULL

#' DBMS Transaction Management
#'
#' Commits or roll backs the current transaction in an MySQL connection.
#'
#' @param conn a \code{MySQLConnection} object, as produced by
#'  \code{\link{dbConnect}}.
#' @param ... Unused.
#' @examples
#' \dontrun{
#' con <- dbConnect(RMySQL::MySQL(), group = "group")
#' rs <- dbSendQuery(con,
#'       "delete * from PURGE as p where p.wavelength<0.03")
#' if(dbGetInfo(rs, what = "rowsAffected") > 250){
#'   warning("dubious deletion -- rolling back transaction")
#'   dbRollback(con)
#' }
#' }
setMethod("dbCommit", "MySQLConnection",
  def = function(conn, ...) .NotYetImplemented()
)

#' @export
#' @rdname dbCommit-MySQLConnection-method
setMethod("dbRollback", "MySQLConnection",
  def = function(conn, ...) .NotYetImplemented()
)

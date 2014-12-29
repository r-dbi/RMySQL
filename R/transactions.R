
#' DBMS Transaction Management
#'
#' Commits or roll backs the current transaction in an MySQL connection
#'
#' @section Methods: \describe{ \item{conn}{a \code{MySQLConnection} object, as
#' produced by the function \code{dbConnect}.} \item{list()}{currently
#' unused.}\item{ }{currently unused.} }
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{dbConnect}},
#' \code{\link[DBI]{dbSendQuery}}, \code{\link[DBI]{dbGetQuery}},
#' \code{\link[DBI]{fetch}}, \code{\link[DBI]{dbCommit}},
#' \code{\link[DBI]{dbGetInfo}}, \code{\link[DBI]{dbReadTable}}.
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
#' @examples
#' \dontrun{
#' drv <- dbDriver("MySQL")
#' con <- dbConnect(drv, group = "group")
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
#' @rdname dbCommit
setMethod("dbRollback", "MySQLConnection",
  def = function(conn, ...) .NotYetImplemented()
)

#' @export
setMethod("dbCallProc", "MySQLConnection",
  def = function(conn, ...) .NotYetImplemented()
)


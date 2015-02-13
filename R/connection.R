#' @include driver.R mysql.R
NULL

#' Class MySQLConnection.
#'
#' \code{MySQLConnection.} objects are usually created by
#' \code{\link[DBI]{dbConnect}}
#'
#' @export
#' @keywords internal
setClass("MySQLConnection",
  contains = "DBIConnection",
  slots = list(
    ptr = "externalptr",
    host = "character",
    db = "character"
  )
)

#' @export
#' @rdname dbConnect-MySQLDriver-method
setMethod("dbDisconnect", "MySQLConnection", function(conn, ...) {
  connection_release(conn@ptr)
  TRUE
})

#' @export
#' @rdname dbConnect-MySQLDriver-method
setMethod("dbGetInfo", "MySQLConnection", function(dbObj, what="", ...) {
  connection_info(dbObj@ptr)
})

#' @export
#' @rdname dbConnect-MySQLDriver-method
setMethod("show", "MySQLConnection", function(object) {
  info <- dbGetInfo(object)
  cat("<MySQLConnection>\n")
  if (dbIsValid(object)) {
    cat("  Host:   ", info$host, "\n", sep = "")
    cat("  Server: ", info$server, "\n", sep = "")
    cat("  Client: ", info$client, "\n", sep = "")
  } else {
    cat("  DISCONNECTED\n")
  }
})


#' Connect/disconnect to a MySQL DBMS
#'
#' These methods are straight-forward implementations of the corresponding
#' generic functions.
#'
#' @param drv an object of class \code{MySQLDriver}, or the character string
#'   "MySQL" or an \code{MySQLConnection}.
#' @param conn an \code{MySQLConnection} object as produced by \code{dbConnect}.
#' @param username,password Username and password. If username omitted,
#'   defaults to the current user. If password is ommitted, only users
#'   without a password can log in.
#' @param dbname string with the database name or NULL. If not NULL, the
#'   connection sets the default daabase to this value.
#' @param host string identifying the host machine running the MySQL server or
#'   NULL. If NULL or the string \code{"localhost"}, a connection to the local
#'   host is assumed.
#' @param unix.socket (optional) string of the unix socket or named pipe.
#' @param port (optional) integer of the TCP/IP default port.
#' @param client.flag (optional) integer setting various MySQL client flags. See
#'   the MySQL manual for details.
#' @param groups string identifying a section in the \code{default.file} to use
#'   for setting authentication parameters (see \code{\link{MySQL}}).
#' @param default.file string of the filename with MySQL client options.
#'   Defaults to \code{\$HOME/.my.cnf}
#' @param ... Unused, needed for compatibility with generic.
#' @export
#' @examples
#' \dontrun{
#' # Connect to a MySQL database running locally
#' con <- dbConnect(RMySQL::MySQL(), dbname = "mydb")
#' # Connect to a remote database with username and password
#' con <- dbConnect(RMySQL::MySQL(), host = "mydb.mycompany.com",
#'   user = "abc", password = "def")
#' # But instead of supplying the username and password in code, it's usually
#' # better to set up a group in your .my.cnf (usually located in your home
#' directory). Then it's less likely you'll inadvertently share them.
#' con <- dbConnect(RMySQL::MySQL(), group = "test")
#'
#' # Always cleanup by disconnecting the database
#' dbDisconnect(con)
#' }
#'
#' # All examples use the rs-dbi group by default.
#' if (mysqlHasDefault()) {
#'   con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#'   con
#'   dbDisconnect(con)
#' }
#' @export
#' @useDynLib RMySQL RS_MySQL_newConnection
setMethod("dbConnect", "MySQLDriver",
  function(drv, dbname = "", username = "", password = "", host = "",
           unix.socket = "", port = 0, client.flag = 0,
           groups = "rs-dbi", default.file = "", ...) {

  ptr <- connection_create(host, username, password, dbname, port, unix.socket,
    client.flag, groups, default.file)

  new("MySQLConnection",
    ptr = ptr,
    host = host,
    db = dbname
  )
})

#' @export
#' @rdname dbConnect-MySQLDriver-method
#' @useDynLib RMySQL RS_MySQL_cloneConnection
setMethod("dbConnect", "MySQLConnection", function(drv, ...) {
  checkValid(drv)

  newId <- .Call(RS_MySQL_cloneConnection, drv@Id)
  new("MySQLConnection", Id = newId)
})

#' @include MySQLConnection.R
NULL

#' Connect/disconnect to a MySQL DBMS
#'
#' These methods are straight-forward implementations of the corresponding
#' generic functions.
#'
#' @param drv an object of class \code{MySQLDriver}, or the character string
#'   "MySQL" or an \code{MySQLConnection}.
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
setMethod("dbConnect", "MySQLDriver",
  function(drv, dbname = NULL, username = NULL, password = NULL, host = NULL,
    unix.socket = NULL, port = 0, client.flag = 0,
    groups = "rs-dbi", default.file = NULL, ...) {

    ptr <- connection_create(host, username, password, dbname, port, unix.socket,
      client.flag, groups, default.file)

    con <- new("MySQLConnection",
      ptr = ptr,
      host = if(is.null(host)) NA_character_ else host,
      db = if(is.null(dbname)) NA_character_ else dbname
    )

    dbGetQuery(con, 'SET time_zone = "+00:00"')
    dbGetQuery(con, 'SET character set utf8')
    con
  }
)

#' @param max.con DEPRECATED
#' @param fetch.default.rec DEPRECATED
#' @export
#' @import methods DBI
#' @importFrom Rcpp sourceCpp
#' @useDynLib RMySQL
#' @rdname dbConnect-MySQLDriver-method
#' @examples
#' if (mysqlHasDefault()) {
#' # connect to a database and load some data
#' con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#' dbWriteTable(con, "USArrests", datasets::USArrests, overwrite = TRUE)
#'
#' # query
#' rs <- dbSendQuery(con, "SELECT * FROM USArrests")
#' d1 <- dbFetch(rs, n = 10)      # extract data in chunks of 10 rows
#' dbHasCompleted(rs)
#' d2 <- dbFetch(rs, n = -1)      # extract all remaining data
#' dbHasCompleted(rs)
#' dbClearResult(rs)
#' dbListTables(con)
#'
#' # clean up
#' dbRemoveTable(con, "USArrests")
#' dbDisconnect(con)
#' }
MySQL <- function(max.con=16, fetch.default.rec = 500) {
  if (!missing(max.con)) {
    warning("`max.con` argument is ignored", call. = FALSE)
  }
  if (!fetch.default.rec) {
    warning("`fetch.default.rec` argument is ignored", call. = FALSE)
  }

  new("MySQLDriver")
}

#' Constants
#'
#' @aliases .MySQLPkgName .MySQLPkgVersion .MySQLPkgRCS
#' .MySQLSQLKeywords CLIENT_LONG_PASSWORD CLIENT_FOUND_ROWS CLIENT_LONG_FLAG
#' CLIENT_CONNECT_WITH_DB CLIENT_NO_SCHEMA CLIENT_COMPRESS CLIENT_ODBC
#' CLIENT_LOCAL_FILES CLIENT_IGNORE_SPACE CLIENT_PROTOCOL_41 CLIENT_INTERACTIVE
#' CLIENT_SSL CLIENT_IGNORE_SIGPIPE CLIENT_TRANSACTIONS CLIENT_RESERVED
#' CLIENT_SECURE_CONNECTION CLIENT_MULTI_STATEMENTS CLIENT_MULTI_RESULTS
#' @section Constants: \code{.MySQLPkgName} (currently \code{"RMySQL"}),
#' \code{.MySQLPkgVersion} (the R package version), \code{.MySQLPkgRCS} (the
#' RCS revision), \code{.MySQLSQLKeywords} (a lot!)
#' @name constants
NULL

## The following client flags were copied from mysql_com.h (version 4.1.13)
## but it may not make sense to set some of this from RMySQL.

#' @export
CLIENT_LONG_PASSWORD <-   1    # new more secure passwords
#' @export
CLIENT_FOUND_ROWS    <-   2    # Found instead of affected rows
#' @export
CLIENT_LONG_FLAG     <-   4    # Get all column flags
#' @export
CLIENT_CONNECT_WITH_DB <- 8    # One can specify db on connect
#' @export
CLIENT_NO_SCHEMA     <-  16    # Don't allow database.table.column
#' @export
CLIENT_COMPRESS      <-  32    # Can use compression protocol
#' @export
CLIENT_ODBC          <-  64    # Odbc client
#' @export
CLIENT_LOCAL_FILES   <- 128    # Can use LOAD DATA LOCAL
#' @export
CLIENT_IGNORE_SPACE  <- 256    # Ignore spaces before '('
#' @export
CLIENT_PROTOCOL_41   <- 512    # New 4.1 protocol
#' @export
CLIENT_INTERACTIVE   <- 1024   # This is an interactive client
#' @export
CLIENT_SSL           <- 2048   # Switch to SSL after handshake
#' @export
CLIENT_IGNORE_SIGPIPE <- 4096  # IGNORE sigpipes
#' @export
CLIENT_TRANSACTIONS <- 8192    # Client knows about transactions
#' @export
CLIENT_RESERVED     <- 16384   # Old flag for 4.1 protocol
#' @export
CLIENT_SECURE_CONNECTION <- 32768 # New 4.1 authentication
#' @export
CLIENT_MULTI_STATEMENTS  <- 65536 # Enable/disable multi-stmt support
#' @export
CLIENT_MULTI_RESULTS     <- 131072 # Enable/disable multi-results

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
  slots = list(Id = "integer")
)

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
#'   summary(con)
#'   dbDisconnect(con)
#' }
#' @export
#' @useDynLib RMySQL RS_MySQL_newConnection
setMethod("dbConnect", "MySQLDriver", function(drv, dbname=NULL, username=NULL,
          password=NULL, host=NULL,
          unix.socket=NULL, port = 0, client.flag = 0,
          groups = 'rs-dbi', default.file = NULL, ...) {
    checkValid(drv)

    if (!is.null(dbname) && !is.character(dbname))
      stop("Argument dbname must be a string or NULL")
    if (!is.null(username) && !is.character(username))
      stop("Argument username must be a string or NULL")
    if (!is.null(password) && !is.character(password))
      stop("Argument password must be a string or NULL")
    if (!is.null(host) && !is.character(host))
      stop("Argument host must be a string or NULL")
    if (!is.null(unix.socket) && !is.character(unix.socket))
      stop("Argument unix.socket must be a string or NULL")

    if (is.null(port) || !is.numeric(port))
      stop("Argument port must be an integer value")
    if (is.null(client.flag) || !is.numeric(client.flag))
      stop("Argument client.flag must be an integer value")

    if (!is.null(groups) && !is.character(groups))
      stop("Argument groups must be a string or NULL")

    if(!is.null(default.file) && !is.character(default.file))
      stop("Argument default.file must be a string")

    if(!is.null(default.file) && !file.exists(default.file[1]))
      stop(sprintf("mysql default file %s does not exist", default.file))

    conId <- .Call(RS_MySQL_newConnection, drv@Id,
      dbname, username, password, host, unix.socket,
      as.integer(port), as.integer(client.flag),
      groups, default.file[1])

    new("MySQLConnection", Id = conId)
  }
)

#' @export
#' @rdname dbConnect-MySQLDriver-method
#' @useDynLib RMySQL RS_MySQL_cloneConnection
setMethod("dbConnect", "MySQLConnection", function(drv, ...) {
  checkValid(drv)

  newId <- .Call(RS_MySQL_cloneConnection, drv@Id)
  new("MySQLConnection", Id = newId)
})

#' @export
#' @rdname dbConnect-MySQLDriver-method
#' @useDynLib RMySQL RS_MySQL_closeConnection
setMethod("dbDisconnect", "MySQLConnection", function(conn, ...) {
  if (!dbIsValid(conn)) return(TRUE)

  rs <- dbListResults(conn)
  if (length(rs) > 0) {
    warning("Closing open result sets", call. = FALSE)
    lapply(rs, dbClearResult)
  }

  .Call(RS_MySQL_closeConnection, conn@Id)
})

#' Database interface meta-data
#'
#' @name db-meta
#' @param conn,dbObj,object MySQLConnection object.
#' @param ... Other arguments for compatibility with generic.
#' @examples
#' if (mysqlHasDefault()) {
#'   con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#'
#'   summary(con)
#'
#'   dbGetInfo(con)
#'   dbListResults(con)
#'   dbListTables(con)
#'
#'   dbDisconnect(con)
#' }
NULL

#' @rdname db-meta
#' @param what optional
#' @export
#' @useDynLib RMySQL RS_MySQL_connectionInfo
setMethod("dbGetInfo", "MySQLConnection", function(dbObj, what="", ...) {
  checkValid(dbObj)

  info <- .Call(RS_MySQL_connectionInfo, dbObj@Id)
  info$rsId <- lapply(info$rsId, function(id) {
    new("MySQLResult", Id = c(dbObj@Id, id))
  })

  if (!missing(what)) {
    info[what]
  } else {
    info
  }
})

#' @rdname db-meta
#' @export
setMethod("dbListResults", "MySQLConnection",
  def = function(conn, ...) dbGetInfo(conn)$rsId
)

#' @rdname db-meta
#' @param verbose If \code{TRUE}, add extra info.
#' @export
setMethod("summary", "MySQLConnection",
  function(object, verbose = FALSE, ...) {
    print(object)

    info <- dbGetInfo(object)
    cat("  User:  ", info$user, "\n")
    cat("  Host:  ", info$host, "\n")
    cat("  Dbname:", info$dbname, "\n")
    cat("  Connection type:", info$conType, "\n")
    if(verbose){
      cat("  MySQL server version:  ", info$serverVersion, "\n")
      cat("  MySQL client version:  ", dbGetInfo(MySQL())$clientVersion, "\n")
      cat("  MySQL protocol version:", info$protocolVersion, "\n")
      cat("  MySQL server thread id:", info$threadId, "\n")
    }

    cat("\nResults:\n")
    lapply(info$rsId, function(x) print(summary(x)))

    invisible(NULL)
  }
)


#' @rdname db-meta
#' @export
#' @useDynLib RMySQL rmysql_exception_info
setMethod("dbGetException", "MySQLConnection",
  def = function(conn, ...) {
    checkValid(conn)

    .Call(rmysql_exception_info, conn@Id)
  }
)

#' @rdname db-meta
#' @export
setMethod("show", "MySQLConnection", function(object) {
  expired <- if(dbIsValid(object)) "" else "Expired "
  cat("<", expired, "MySQLConnection:", paste(object@Id, collapse = ","), ">\n",
    sep = "")
  invisible(NULL)
})

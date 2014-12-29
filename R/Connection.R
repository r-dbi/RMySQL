#' @include Driver.R MySQL.R
NULL

#' Class MySQLConnection.
#'
#' \code{MySQLConnection.} objects are usually created by
#' \code{\link[DBI]{dbConnect}}
#'
#' @export
#' @keywords internal
setClass("MySQLConnection", representation("DBIConnection", "MySQLObject"))

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
#'   con <- dbConnect(RMySQL::MySQL())
#'   summary(con)
#'   dbDisconnect(con)
#' }
#' @export
#' @useDynLib RMySQL RS_MySQL_newConnection
setMethod("dbConnect", "MySQLDriver", function(drv, dbname=NULL, username=NULL,
          password=NULL, host=NULL,
          unix.socket=NULL, port = 0, client.flag = 0,
          groups = 'rs-dbi', default.file = NULL, ...) {
    if(!isIdCurrent(drv))
      stop("expired manager")

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

    drvId <- as(drv, "integer")
    conId <- .Call(RS_MySQL_newConnection, drvId,
      dbname, username, password, host, unix.socket,
      as.integer(port), as.integer(client.flag),
      groups, default.file[1])

    new("MySQLConnection", Id = conId)
  }
)

#' @export
#' @rdname dbConnect-MySQLDriver-method
#' @useDynLib RMySQL RS_MySQL_cloneConnection
setMethod("dbConnect", "MySQLConnection",
  function(drv, ...) {
    if (!isIdCurrent(drv)) stop(paste("expired", class(drv)))
    conId <- as(drv, "integer")
    newId <- .Call(RS_MySQL_cloneConnection, conId)
    new("MySQLConnection", Id = newId)
  }
)

#' @export
#' @rdname dbConnect-MySQLDriver-method
#' @useDynLib RMySQL RS_MySQL_closeConnection
setMethod("dbDisconnect", "MySQLConnection",
  function(conn, ...) {
    if(!isIdCurrent(conn))
      return(TRUE)
    rs <- dbListResults(conn)
    if(length(rs)>0){
      if(dbHasCompleted(rs[[1]]))
        dbClearResult(rs[[1]])
      else
        stop("connection has pending rows (close open results set first)")
    }
    conId <- as(conn, "integer")
    .Call(RS_MySQL_closeConnection, conId)
  }
)

#' Database interface meta-data
#'
#' @name db-meta
#' @param conn,dbObj,object MySQLConnection object.
#' @param ... Other arguments for compatibility with generic.
#' @examples
#' if (mysqlHasDefault()) {
#'   con <- dbConnect(RMySQL::MySQL())
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
  if(!isIdCurrent(dbObj))
    stop(paste("expired", class(dbObj), deparse(substitute(dbObj))))
  id <- as(dbObj, "integer")
  info <- .Call(RS_MySQL_connectionInfo, id)
  rsId <- vector("list", length = length(info$rsId))
  for(i in seq(along = info$rsId))
    rsId[[i]] <- new("MySQLResult", Id = c(id, info$rsId[i]))
  info$rsId <- rsId

  if(!missing(what))
    info[what]
  else
    info
})

#' @rdname db-meta
#' @export
setMethod("dbListResults", "MySQLConnection",
  def = function(conn, ...) dbGetInfo(conn, "rsId")[[1]]
)

#' @rdname db-meta
#' @param verbose If \code{TRUE}, add extra info.
#' @export
setMethod("summary", "MySQLConnection",
  function(object, verbose = FALSE, ...) {
    info <- dbGetInfo(object)
    print(object)
    cat("  User:", info$user, "\n")
    cat("  Host:", info$host, "\n")
    cat("  Dbname:", info$dbname, "\n")
    cat("  Connection type:", info$conType, "\n")
    if(verbose){
      cat("  MySQL server version: ", info$serverVersion, "\n")
      cat("  MySQL client version: ",
        dbGetInfo(as(obj, "MySQLDriver"), what="clientVersion")[[1]], "\n")
      cat("  MySQL protocol version: ", info$protocolVersion, "\n")
      cat("  MySQL server thread id: ", info$threadId, "\n")
    }
    if(length(info$rsId)>0){
      for(i in seq(along = info$rsId)){
        cat("   ", i, " ")
        print(info$rsId[[i]])
      }
    } else
      cat("  No resultSet available\n")
    invisible(NULL)
  }
)


#' @rdname db-meta
#' @export
#' @useDynLib RMySQL RS_MySQL_getException
setMethod("dbGetException", "MySQLConnection",
  def = function(conn, ...) {
    if(!isIdCurrent(conn))
      stop(paste("expired", class(conn)))
    .Call(RS_MySQL_getException, as(conn, "integer"))
  }
)

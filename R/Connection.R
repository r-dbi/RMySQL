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
#' @param username string of the MySQL login name or NULL. If NULL or the empty
#'   string \code{""}, the current user is assumed.
#' @param password string with the MySQL password or NULL. If NULL, only entries
#'   in the user table for the users that have a blank (empty) password field
#'   are hecked for a match.
#' @param dbname string with the database name or NULL. If NOT NULL, the
#'   connection sets the default da abaseto this value.
#' @param host string identifying the host machine running the MySQL server or
#'   NULL. If NULL or the string \code{"localhost"}, a connection to the local
#'   host s assumed.
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
#' # create an MySQL instance and create one connection.
#' drv <- dbDriver(RMySQL::MySQL())
#'
#' # open the connection using user, passsword, etc., as
#' con <- dbConnect(drv, group = "rs-dbi")
#'
#' # Run an SQL statement by creating first a resultSet object
#' rs <- dbSendQuery(con, statement = paste(
#'                       "SELECT w.laser_id, w.wavelength, p.cut_off",
#'                       "FROM WL w, PURGE P",
#'                       "WHERE w.laser_id = p.laser_id",
#'                       "SORT BY w.laser_id")
#' # we now fetch records from the resultSet into a data.frame
#' data <- fetch(rs, n = -1)   # extract all rows
#' dim(data)
#' }
#' @export
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
    conId <- .Call("RS_MySQL_newConnection", drvId,
      dbname, username, password, host, unix.socket,
      as.integer(port), as.integer(client.flag),
      groups, default.file[1], PACKAGE = .MySQLPkgName)

    new("MySQLConnection", Id = conId)
  }
)

#' @export
#' @rdname dbConnect-MySQLDriver-method
setMethod("dbConnect", "MySQLConnection",
  function(drv, ...) {
    if (!isIdCurrent(drv)) stop(paste("expired", class(drv)))
    conId <- as(drv, "integer")
    newId <- .Call("RS_MySQL_cloneConnection", conId, PACKAGE = .MySQLPkgName)
    new("MySQLConnection", Id = newId)
  }
)

#' @export
#' @rdname dbConnect-MySQLDriver-method
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
    .Call("RS_MySQL_closeConnection", conId, PACKAGE = .MySQLPkgName)
  }
)

#' Database interface meta-data
#'
#' @name db-meta
#' @param conn,dbObj,object MySQLConnection object.
#' @param ... Other arguments for compatibility with generic.
#' @examples
#' \dontrun{
#' con <- dbConnect(RMySQL::MySQL(), group = "wireless")
#' dbGetInfo(con)
#' dbListResults(con)
#' dbListTables(con)
#' }
NULL

#' @rdname db-meta
#' @param what optional
#' @export
setMethod("dbGetInfo", "MySQLConnection", function(dbObj, what="", ...) {
  if(!isIdCurrent(dbObj))
    stop(paste("expired", class(dbObj), deparse(substitute(dbObj))))
  id <- as(dbObj, "integer")
  info <- .Call("RS_MySQL_connectionInfo", id, PACKAGE = .MySQLPkgName)
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
setMethod("dbGetException", "MySQLConnection",
  def = function(conn, ...) {
    if(!isIdCurrent(conn))
      stop(paste("expired", class(conn)))
    .Call("RS_MySQL_getException", as(conn, "integer"),
      PACKAGE = .MySQLPkgName)
  }
)

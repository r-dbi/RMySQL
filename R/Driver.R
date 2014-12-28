#' MySQL implementation of the Database Interface (DBI) classes and drivers
#'
#' MySQL driver initialization and closing
#'
#' @docType methods
#' @section Methods: \describe{ \item{drvName}{ character name of the driver to
#' instantiate.  } \item{drv}{ an object that inherits from \code{MySQLDriver}
#' as created by \code{dbDriver}. } \item{max.con}{optional integer requesting
#' the maximum number of simultanous connections (may be up to 100)}.
#' \item{fetch.default.rec}{default number of records to retrieve per fetch.
#' Default is 500.  This may be overridden in calls to \code{\link[DBI]{fetch}}
#' with the \code{n=} argument.} \item{force.reload}{optional logical used to
#' force re-loading or recomputing the size of the connection table. Default is
#' \code{FALSE}.} \item{...}{currently unused.} }
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{dbConnect}},
#' \code{\link[DBI]{dbSendQuery}}, \code{\link[DBI]{dbGetQuery}},
#' \code{\link[DBI]{fetch}}, \code{\link[DBI]{dbCommit}},
#' \code{\link[DBI]{dbGetInfo}}, \code{\link[DBI]{dbListTables}},
#' \code{\link[DBI]{dbReadTable}}.
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
#' @examples
#' \dontrun{
#' # create an MySQL instance and set 10000 of rows per fetch.
#' m <- dbDriver("MySQL", fetch.default.records=10000)
#'
#' con <- dbConnect(m, username="usr", password = "pwd",
#'            dbname = "iptraffic")
#' rs <- dbSubmitQuery(con,
#'          "select * from HTTP_ACCESS where IP_ADDRESS = '127.0.0.1'")
#' df <- fetch(rs, n = 50)
#' df2 <- fetch(rs, n = -1)
#' dbClearResult(rs)
#'
#' pcon <- dbConnect(p, group = "wireless")
#' dbListTables(pcon)
#' }
setClass("MySQLDriver", representation("DBIDriver", "MySQLObject"))


## create a MySQL database connection manager.  By default we allow
## up to "max.con" connections and single fetches of up to "fetch.default.rec"
## records.  These settings may be changed by re-loading the driver
## using the "force.reload" = T flag (note that this will close all
## currently open connections).
## Returns an object of class "MySQLManger".
## Note: This class is a singleton.


#' Instantiate a MySQL client from the current R session
#'
#' This function creates and initializes a MySQL client. It returns an driver
#' object that allows you to connect to one or several MySQL servers.
#'
#' This object is a singleton, that is, on subsequent invocations it returns
#' the same initialized object.
#'
#' This implementation allows you to connect to multiple host servers and run
#' multiple connections on each server simultaneously.
#'
#' @param max.con maximum number of connections that are intended to have open
#' at one time.  There's no intrinic limit, since strictly speaking this limit
#' applies to MySQL \emph{servers}, but clients can have (at least in theory)
#' more than this.  Typically there are at most a handful of open connections,
#' thus the internal \code{RMySQL} code uses a very simple linear search
#' algorithm to manage its connection table.
#' @param fetch.default.rec number of records to fetch at one time from the
#' database.  (The \code{\link[DBI]{fetch}} method uses this number as a
#' default.)
#' @param force.reload should the client code be reloaded (reinitialize)?
#' Setting this to \code{TRUE} allows you to change default settings.  Notice
#' that all connections should be closed before re-loading.
#' @return An object \code{MySQLDriver} that extends \code{dbDriver}. This
#' object is required to create connections to one or several MySQL database
#' engines.
#' @note Use the option \code{database} in place of\code{dbname} in
#' configuration files.
#' @section Side Effects: The R client part of the database communication is
#' initialized, but note that connecting to the database engine needs to be
#' done through calls to \code{\link[DBI]{dbConnect}}.
#' @author David A. James
#' @seealso On database managers:
#'
#' \code{\link[DBI]{dbDriver}} \code{\link[DBI]{dbUnloadDriver}}
#'
#' On connections, SQL statements and resultSets:
#'
#' \code{\link[DBI]{dbConnect}} \code{\link[DBI]{dbDisconnect}}
#' \code{\link[DBI]{dbSendQuery}} \code{\link[DBI]{dbGetQuery}}
#' \code{\link[DBI]{fetch}} \code{\link[DBI]{dbClearResult}}
#'
#' On transaction management:
#'
#' \code{\link[DBI]{dbCommit}} \code{\link[DBI]{dbRollback}}
#'
#' On meta-data:
#'
#' \code{\link{summary}} \code{\link[DBI]{dbGetInfo}}
#' \code{\link[DBI]{dbGetDBIVersion}} \code{\link[DBI]{dbListTables}}
#' \code{\link[DBI]{dbListConnections}} \code{\link[DBI]{dbListResults}}
#' \code{\link[DBI]{dbColumnInfo}} \code{\link[DBI]{dbGetException}}
#' \code{\link[DBI]{dbGetStatement}} \code{\link[DBI]{dbHasCompleted}}
#' \code{\link[DBI]{dbGetRowCount}}
#' @keywords interface database
#' @examples
#' \dontrun{
#' # create a MySQL instance and create one connection.
#' > m <- dbDriver("MySQL")
#' <MySQLDriver:(4378)>
#'
#' # open the connection using user, passsword, etc., as
#' # specified in the "[iptraffic]" section of the
#' # configuration file \file{\$HOME/.my.cnf}
#' > con <- dbConnect(m, group = "iptraffic")
#' > rs <- dbSendQuery(con, "select * from HTTP_ACCESS where IP_ADDRESS = '127.0.0.1'")
#' > df <- fetch(rs, n = 50)
#' > dbHasCompleted(rs)
#' [1] FALSE
#' > df2 <- fetch(rs, n = -1)
#' > dbHasCompleted(rs)
#' [1] TRUE
#' > dbClearResult(rs)
#' > dim(dbGetQuery(con, "show tables"))
#' [1] 74   1
#' > dbListTables(con)
#' }
#'
MySQL <- function(max.con=16, fetch.default.rec = 500, force.reload=FALSE) {
  if(fetch.default.rec<=0)
    stop("default num of records per fetch must be positive")
  config.params <- as(c(max.con, fetch.default.rec), "integer")
  force <- as.logical(force.reload)
  drvId <- .Call("RS_MySQL_init", config.params, force,
    PACKAGE = .MySQLPkgName)
  new("MySQLDriver", Id = drvId)
}

## coerce (extract) any MySQLObject into a MySQLDriver
setAs("MySQLObject", "MySQLDriver",
  def = function(from) new("MySQLDriver", Id = as(from, "integer")[1:2])
)

setMethod("dbUnloadDriver", "MySQLDriver",
  function(drv, ...) {
    if(!isIdCurrent(drv))
      return(TRUE)
    drvId <- as(drv, "integer")
    .Call("RS_MySQL_closeManager", drvId, PACKAGE = .MySQLPkgName)
  }
)


setMethod("dbGetInfo", "MySQLDriver",
  function(dbObj, what="", ...)
  {
    if(!isIdCurrent(dbObj))
      stop(paste("expired", class(dbObj)))
    drvId <- as(dbObj)
    info <- .Call("RS_MySQL_managerInfo", drvId, PACKAGE = .MySQLPkgName)
    ## replace drv/connection id w. actual drv/connection objects
    conObjs <- vector("list", length = info$"num_con")
    ids <- info$connectionIds
    for(i in seq(along = ids))
      conObjs[[i]] <- new("MySQLConnection", Id = c(drvId, ids[i]))
    info$connectionIds <- conObjs
    info$managerId <- new("MySQLDriver", Id = drvId)
    if(!missing(what))
      info[what]
    else
      info
  }
)

setMethod("dbListConnections", "MySQLDriver",
  function(drv, ...) dbGetInfo(drv, "connectionIds")[[1]]
)

setMethod("summary", "MySQLDriver",
  function(object, verbose = FALSE, ...) {
    info <- dbGetInfo(object)
    print(object)
    cat("  Driver name: ", info$drvName, "\n")
    cat("  Max  connections:", info$length, "\n")
    cat("  Conn. processed:", info$counter, "\n")
    cat("  Default records per fetch:", info$"fetch_default_rec", "\n")
    if(verbose){
      cat("  DBI API version: ", dbGetDBIVersion(), "\n")
      cat("  MySQL client version: ", info$clientVersion, "\n")
    }
    cat("  Open connections:", info$"num_con", "\n")
    if(verbose && !is.null(info$connectionIds)){
      for(i in seq(along = info$connectionIds)){
        cat("   ", i, " ")
        print(info$connectionIds[[i]])
      }
    }
    invisible(NULL)
  }
)

#' @include MySQL.R
NULL

#' Class MySQLDriver with constructor MySQL.
#'
#' An MySQL driver implementing the R database (DBI) API.
#' This class should always be initialized with the \code{MySQL()} function.
#' It returns a singleton that allows you to connect to MySQL.
#'
#' @export
#' @aliases RMySQL-package
setClass("MySQLDriver", representation("DBIDriver", "MySQLObject"))

#' @param max.con maximum number of connections that can be open
#'   at one time. There's no intrinic limit, since strictly speaking this limit
#'   applies to MySQL \emph{servers}, but clients can have (at least in theory)
#'   more than this.  Typically there are at most a handful of open connections,
#'   thus the internal \code{RMySQL} code uses a very simple linear search
#'   algorithm to manage its connection table.
#' @param fetch.default.rec number of records to fetch at one time from the
#'   database. (The \code{\link[DBI]{fetch}} method uses this number as a
#'   default.)
#' @param force.reload should the client code be reloaded (reinitialized)?
#'   Setting this to \code{TRUE} allows you to change default settings.
#'   All connections should be closed before re-loading.
#' @export
#' @import methods DBI
#' @useDynLib RMySQL
#' @rdname MySQLDriver-class
#' @examples
#' \dontrun{
#' # connect to a database and load some data
#' con <- dbConnect(RMySQL::MySQL())
#' data(USArrests)
#' dbWriteTable(con, "USArrests", USArrests)
#'
#' # query
#' rs <- dbSendQuery(con, "SELECT * FROM USArrests")
#' d1 <- fetch(rs, n = 10)      # extract data in chunks of 10 rows
#' dbHasCompleted(rs)
#' d2 <- fetch(rs, n = -1)      # extract all remaining data
#' dbHasCompleted(rs)
#' dbClearResult(rs)
#' dbListTables(con)
#'
#' # clean up
#' dbDisconnect(con)
#' }
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

#' Unload MySQL driver.
#'
#' @param drv Object created by \code{\link{MySQL}}.
#' @param ... Ignored. Needed for compatibility with generic.
#' @return A logical indicating whether the operation succeeded or not.
#' @export
#' @examples
#' db <- RMySQL::MySQL()
#' dbUnloadDriver(db)
setMethod("dbUnloadDriver", "MySQLDriver", function(drv, ...) {
  if(!isIdCurrent(drv))
    return(TRUE)
  drvId <- as(drv, "integer")
  .Call("RS_MySQL_closeManager", drvId, PACKAGE = .MySQLPkgName)
})


#' Get information about a MySQL driver.
#'
#' @param drv Object created by \code{\link{MySQL}}.
#' @param what Optional
#' @param ... Ignored. Needed for compatibility with generic.
#' @export
#' @examples
#' db <- RMySQL::MySQL()
#'
#' db
#' dbGetInfo(db)
#' dbListConnections(db)
#' summary(db)
setMethod("dbGetInfo", "MySQLDriver", function(dbObj, what="", ...) {
  if(!isIdCurrent(dbObj))
    stop(paste("expired", class(dbObj)))
  drvId <- as(dbObj, "integer")
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
})

#' @rdname dbGetInfo-MySQLDriver-method
#' @export
setMethod("dbListConnections", "MySQLDriver", function(drv, ...) {
  dbGetInfo(drv, "connectionIds")[[1]]
})

#' @rdname dbGetInfo-MySQLDriver-method
#' @export
setMethod("summary", "MySQLDriver", function(object, verbose = FALSE, ...) {
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
})

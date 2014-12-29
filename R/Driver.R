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
#' if (mysqlHasDefault()) {
#' # connect to a database and load some data
#' con <- dbConnect(RMySQL::MySQL())
#' dbWriteTable(con, "USArrests", datasets::USArrests)
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
#' dbRemoveTable(con, "USArrests")
#' dbDisconnect(con)
#' }
#' @useDynLib RMySQL RS_MySQL_init
MySQL <- function(max.con=16, fetch.default.rec = 500, force.reload=FALSE) {
  if (fetch.default.rec <= 0) {
    stop("default num of records per fetch must be positive")
  }

  config.params <- as.integer(c(max.con, fetch.default.rec))
  force <- as.logical(force.reload)

  drvId <- .Call(RS_MySQL_init, config.params, force)
  new("MySQLDriver", Id = drvId)
}

## coerce (extract) any MySQLObject into a MySQLDriver
## HW: I'm pretty sure this is incorrect, since a Driver only needs a singe ID
setAs("MySQLObject", "MySQLDriver", function(from) {
  new("MySQLDriver", Id = from@Id[1:2])
})

#' Unload MySQL driver.
#'
#' @param drv Object created by \code{\link{MySQL}}.
#' @param ... Ignored. Needed for compatibility with generic.
#' @return A logical indicating whether the operation succeeded or not.
#' @export
#' @useDynLib RMySQL RS_MySQL_closeManager
setMethod("dbUnloadDriver", "MySQLDriver", function(drv, ...) {
  if(!isIdCurrent(drv)) return(TRUE)

  .Call(RS_MySQL_closeManager, drv@Id)
})


#' Get information about a MySQL driver.
#'
#' @param dbObj,object,drv Object created by \code{\link{MySQL}}.
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
#' @useDynLib RMySQL RS_MySQL_managerInfo
setMethod("dbGetInfo", "MySQLDriver", function(dbObj, what="", ...) {
  checkValid(dbObj)

  info <- .Call(RS_MySQL_managerInfo, dbObj@Id)
  info$connectionIds <- lapply(info$connectionIds, function(conId) {
    new("MySQLConnection", Id = c(dbObj@Id, conId))
  })

  # Don't need to insert self into info
  info$managerId <- NULL

  if (!missing(what)) {
    info[what]
  } else {
    info
  }
})

#' @rdname dbGetInfo-MySQLDriver-method
#' @export
setMethod("dbListConnections", "MySQLDriver", function(drv, ...) {
  dbGetInfo(drv, "connectionIds")[[1]]
})

#' @rdname dbGetInfo-MySQLDriver-method
#' @param verbose If \code{TRUE}, print extra info.
#' @export
setMethod("summary", "MySQLDriver", function(object, verbose = FALSE, ...) {
  info <- dbGetInfo(object)

  print(object)
  cat("  Max connections:  ", info$length, "\n")
  cat("  Cur connections:  ", info$`num_con`, "\n")
  cat("  Total connections:", info$counter, "\n")
  cat("  Default records per fetch:", info$`fetch_default_rec`, "\n")
  if (verbose) {
    cat("  DBI API version:      ", as.character(packageVersion("DBI")), "\n")
    cat("  MySQL client version: ", info$clientVersion, "\n")

    cat("\nConnections:\n")
    lapply(info$connectionIds, function(x) print(summary(x)))
  }

  invisible(NULL)
})

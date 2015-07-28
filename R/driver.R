#' @include mysql.R
NULL

#' Class MySQLDriver with constructor MySQL.
#'
#' An MySQL driver implementing the R database (DBI) API.
#' This class should always be initialized with the \code{MySQL()} function.
#' It returns a singleton that allows you to connect to MySQL.
#'
#' @export
#' @aliases RMySQL-package RMySQL
setClass("MySQLDriver",
  contains = "DBIDriver",
  slots = list(Id = "integer")
)

#' @param max.con maximum number of connections that can be open
#'   at one time. There's no intrinic limit, since strictly speaking this limit
#'   applies to MySQL \emph{servers}, but clients can have (at least in theory)
#'   more than this.  Typically there are at most a handful of open connections,
#'   thus the internal \code{RMySQL} code uses a very simple linear search
#'   algorithm to manage its connection table.
#' @param fetch.default.rec number of records to fetch at one time from the
#'   database. (The \code{\link[DBI]{fetch}} method uses this number as a
#'   default.)
#' @export
#' @import methods DBI
#' @importFrom utils packageVersion read.table write.table
#' @useDynLib RMySQL
#' @rdname MySQLDriver-class
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
#' @useDynLib RMySQL rmysql_driver_init
MySQL <- function(max.con=16, fetch.default.rec = 500) {
  if (fetch.default.rec <= 0) {
    stop("default num of records per fetch must be positive")
  }

  drvId <- .Call(rmysql_driver_init, max.con, fetch.default.rec)
  new("MySQLDriver", Id = drvId)
}

#' Unload MySQL driver.
#'
#' @param drv Object created by \code{\link{MySQL}}.
#' @param ... Ignored. Needed for compatibility with generic.
#' @return A logical indicating whether the operation succeeded or not.
#' @export
#' @useDynLib RMySQL rmysql_driver_close
setMethod("dbUnloadDriver", "MySQLDriver", function(drv, ...) {
  if(!dbIsValid(drv)) return(TRUE)

  .Call(rmysql_driver_close)
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
#' @useDynLib RMySQL rmysql_driver_info
setMethod("dbGetInfo", "MySQLDriver", function(dbObj, what="", ...) {
  checkValid(dbObj)

  info <- .Call(rmysql_driver_info)
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

#' @rdname dbGetInfo-MySQLDriver-method
#' @export
setMethod("show", "MySQLDriver", function(object) {
  expired <- if(dbIsValid(object)) "" else "Expired "
  cat("<", expired, "MySQLDriver>\n",
    sep = "")
  invisible(NULL)
})

#' @include mysql.R
NULL

#' Class MySQLDriver with constructor MySQL.
#'
#' An MySQL driver implementing the R database (DBI) API.
#' This class should always be initialized with the \code{MySQL()} function.
#' It returns a singleton that allows you to connect to MySQL.
#'
#' @export
#' @keywords internal
setClass("MySQLDriver",
  contains = "DBIDriver",
)

#' @rdname MySQLDriver-class
setMethod("dbUnloadDriver", "MySQLDriver", function(drv, ...) {
  TRUE
})

#' @rdname MySQLDriver-class
#' @export
setMethod("show", "MySQLDriver", function(object) {
  cat("<MySQLDriver>\n")
})

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
#' @useDynLib RMySQL rmysql_driver_init
MySQL <- function(max.con=16, fetch.default.rec = 500) {
  if (!missing(max.con)) {
    warning("`max.con` argument is ignored", call. = FALSE)
  }
  if (!fetch.default.rec) {
    warning("`fetch.default.rec` argument is ignored", call. = FALSE)
  }

  new("MySQLDriver")
}

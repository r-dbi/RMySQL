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

#' MySQL Check for Compiled Versus Loaded Client Library Versions
#'
#' This function prints out the compiled and loaded client library versions.
#'
#' @return A named integer vector of length two, the first element
#'   representing the compiled library version and the second element
#'   representing the loaded client library version.
#' @export
#' @examples
#' mysqlClientLibraryVersions()
mysqlClientLibraryVersions <- function() {
  version()
}

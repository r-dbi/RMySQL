#' @include driver.R connection.R result.R
NULL

#' Check if a database object is valid.
#'
#' Support function that verifies that an object holding a reference to a
#' foreign object is still valid for communicating with the RDBMS.
#' \code{isIdCurrent} will be deprecated in the near future; please use
#' the \code{\link[DBI]{dbIsValid}()} generic instead.
#'
#' \code{dbObjects} are R/S-Plus remote references to foreign objects. This
#' introduces differences to the object's semantics such as persistence (e.g.,
#' connections may be closed unexpectedly), thus this function provides a
#' minimal verification to ensure that the foreign object being referenced can
#' be contacted.
#'
#' @param dbObj,obj A \code{MysqlDriver}, \code{MysqlConnection},
#'  \code{MysqlResult}.
#' @return a logical scalar.
#' @export
#' @examples
#' dbIsValid(MySQL())
isIdCurrent <- function(obj)  {
  dbIsValid(obj)
}

checkValid <- function(obj) {
  if (dbIsValid(obj)) return(TRUE)

  stop("Expired ", class(obj), call. = FALSE)
}

#' @export
#' @rdname isIdCurrent
#' @useDynLib RMySQL rmysql_driver_valid
setMethod("dbIsValid", "MySQLDriver", function(dbObj) {
  .Call(rmysql_driver_valid)
})

#' @export
#' @rdname isIdCurrent
#' @useDynLib RMySQL rmysql_connection_valid
setMethod("dbIsValid", "MySQLConnection", function(dbObj) {
  .Call(rmysql_connection_valid, dbObj@Id)
})

#' @export
#' @rdname isIdCurrent
#' @useDynLib RMySQL rmysql_result_valid
setMethod("dbIsValid", "MySQLResult", function(dbObj) {
  .Call(rmysql_result_valid, dbObj@Id)
})

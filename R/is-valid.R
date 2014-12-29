#' @include Driver.R Connection.R Result.R
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
#' @param obj any \code{dbObject} (e.g., \code{dbDriver}, \code{dbConnection},
#' \code{dbResult}).
#' @return a logical scalar.
#' @export
#' @examples
#' isIdCurrent(MySQL())
#' @useDynLib RMySQL RS_DBI_validHandle
isIdCurrent <- function(obj)  {
  .Call(RS_DBI_validHandle, obj@Id)
}

checkValid <- function(obj) {
  if (dbIsValid(obj)) return(TRUE)

  stop("Expired ", class(obj), call = FALSE)
}

#' @export
#' @rdname isIdCurrent
setMethod("dbIsValid", "MySQLDriver", function(dbObj) {
  .Call(RS_DBI_validHandle, dbObj@Id)
})

#' @export
#' @rdname isIdCurrent
setMethod("dbIsValid", "MySQLConnection", function(dbObj) {
  .Call(RS_DBI_validHandle, dbObj@Id)
})

#' @export
#' @rdname isIdCurrent
setMethod("dbIsValid", "MySQLResult", function(dbObj) {
  .Call(RS_DBI_validHandle, dbObj@Id)
})

#' Class MySQLResult
#'
#' MySQL's query results class.  This classes encapsulates the result of an SQL
#' statement (either \code{select} or not).
#'
#' @export
#' @keywords internal
setClass("MySQLResult",
  contains = "DBIResult",
  slots = list(
    ptr = "externalptr",
    sql = "character"
  )
)

#' @rdname MySQLResult-class
#' @export
setMethod("dbIsValid", "MySQLResult", function(dbObj) {
  result_active(dbObj@ptr)
})


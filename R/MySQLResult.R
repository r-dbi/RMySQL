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
setMethod("show", "MySQLResult", function(object) {
  cat("<MySQLResult>\n")
  if(!dbIsValid(object)){
    cat("EXPIRED\n")
  } else {
    cat("  SQL  ", dbGetStatement(object), "\n", sep = "")

    #     done <- if (dbHasCompleted(object)) "complete" else "incomplete"
    #     cat("  ROWS Fetched: ", dbGetRowCount(object), " [", done, "]\n", sep = "")
    #     cat("       Changed: ", dbGetRowsAffected(object), "\n", sep = "")
  }
  invisible(NULL)
})

#' @rdname MySQLResult-class
#' @export
setMethod("dbIsValid", "MySQLResult", function(dbObj) {
  TRUE
})


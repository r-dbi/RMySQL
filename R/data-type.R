#' Determine the SQL Data Type of an S object
#'
#' This method is a straight-forward implementation of the corresponding
#' generic function.
#'
#' @param dbObj A \code{MySQLDriver} or \code{MySQLConnection}.
#' @param obj R/S-Plus object whose SQL type we want to determine.
#' @param \dots any other parameters that individual methods may need.
#' @export
#' @examples
#' dbDataType(RMySQL::MySQL(), "a")
#' dbDataType(RMySQL::MySQL(), 1:3)
#' dbDataType(RMySQL::MySQL(), 2.5)
setMethod("dbDataType", c("MySQLDriver", "ANY"), function(dbObj, obj) {
  mysqlDataType(obj)
})

#' @export
#' @rdname dbDataType-MySQLDriver-method
setMethod("dbDataType", c("MySQLConnection", "ANY"), function(dbObj, obj) {
  mysqlDataType(obj)
})

mysqlDataType <- function(obj) {
  rs.class <- data.class(obj)    ## this differs in R 1.4 from older vers
  rs.mode <- storage.mode(obj)
  if (rs.class == "numeric" || rs.class == "integer") {
    if (rs.mode == "integer") {
      "bigint"
    } else {
      "double"
    }
  } else {
    switch(rs.class,
      character = "text",
      logical = "tinyint",  ## but we need to coerce to int!!
      factor = "text",      ## up to 65535 characters
      ordered = "text",
      "text"
    )
  }
}

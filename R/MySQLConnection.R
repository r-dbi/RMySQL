#' Class MySQLConnection.
#'
#' \code{MySQLConnection.} objects are usually created by
#' \code{\link[DBI]{dbConnect}}
#'
#' @export
#' @keywords internal
setClass("MySQLConnection",
  contains = "DBIConnection",
  slots = list(
    ptr = "externalptr",
    host = "character",
    db = "character"
  )
)

#' @export
#' @rdname dbConnect-MySQLDriver-method
setMethod("dbDisconnect", "MySQLConnection", function(conn, ...) {
  connection_release(conn@ptr)
  TRUE
})

#' @export
#' @rdname dbConnect-MySQLDriver-method
setMethod("dbGetInfo", "MySQLConnection", function(dbObj, what="", ...) {
  connection_info(dbObj@ptr)
})

#' @export
#' @rdname dbConnect-MySQLDriver-method
setMethod("show", "MySQLConnection", function(object) {
  info <- dbGetInfo(object)
  cat("<MySQLConnection>\n")
  if (dbIsValid(object)) {
    cat("  Host:   ", info$host, "\n", sep = "")
    cat("  Server: ", info$server, "\n", sep = "")
    cat("  Client: ", info$client, "\n", sep = "")
  } else {
    cat("  DISCONNECTED\n")
  }
})

#' @export
#' @rdname dbConnect-MySQLDriver-method
setMethod("dbIsValid", "MySQLConnection", function(dbObj) {
  connection_valid(dbObj@ptr)
})

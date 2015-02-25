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
#' @rdname MySQLConnection-class
setMethod("dbDisconnect", "MySQLConnection", function(conn, ...) {
  connection_release(conn@ptr)
  TRUE
})

#' @export
#' @rdname MySQLConnection-class
setMethod("dbGetInfo", "MySQLConnection", function(dbObj, what="", ...) {
  connection_info(dbObj@ptr)
})

#' @export
#' @rdname MySQLConnection-class
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
#' @rdname MySQLConnection-class
setMethod("dbIsValid", "MySQLConnection", function(dbObj) {
  connection_valid(dbObj@ptr)
})

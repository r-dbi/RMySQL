#' Quote MySQL strings and identifiers.
#'
#' In MySQL, identifiers are enclosed in backticks, e.g. \code{`x`}.
#'
#' @keywords internal
#' @name mysql-quoting
#' @examples
#' if (mysqlHasDefault()) {
#'   con <- dbConnect(RMySQL::MySQL())
#'   dbQuoteIdentifier(con, c("a b", "a`b"))
#'   dbQuoteString(con, c("a b", "a'b"))
#'   dbDisconnect(con)
#' }
NULL

#' @rdname dbQuoteIdentifer
#' @export
setMethod("dbQuoteIdentifier", c("MySQLConnection", "character"),
  function(conn, x, ...) {
    x <- gsub('`', '``', x, fixed = TRUE)
    SQL(paste('`', x, '`', sep = ""))
  }
)

#' @rdname dbQuoteIdentifer
#' @export
setMethod("dbQuoteString", c("MySQLConnection", "character"),
  function(conn, x, ...) {
    SQL(paste0("'", connection_quote_string(conn@ptr, x), "'"));
  }
)

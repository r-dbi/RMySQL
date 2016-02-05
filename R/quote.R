#' @include MySQLConnection.R
NULL

#' Quote MySQL strings and identifiers.
#'
#' In MySQL, identifiers are enclosed in backticks, e.g. \code{`x`}.
#'
#' @keywords internal
#' @name mysql-quoting
#' @examples
#' if (mysqlHasDefault()) {
#'   library(DBI)
#'   con <- dbConnect(RMySQL::MySQL())
#'   dbQuoteIdentifier(con, c("a b", "a`b"))
#'   dbQuoteString(con, c("a b", "a'b"))
#'   dbDisconnect(con)
#' }
NULL

#' @rdname mysql-quoting
#' @export
setMethod("dbQuoteIdentifier", c("MySQLConnection", "character"),
  function(conn, x, ...) {
    x <- gsub('`', '``', x, fixed = TRUE)
    SQL(paste('`', x, '`', sep = ""))
  }
)

#' @rdname mysql-quoting
#' @export
setMethod("dbQuoteString", c("MySQLConnection", "character"),
  function(conn, x, ...) {
    SQL(connection_quote_string(conn@ptr, enc2utf8(x)));
  }
)

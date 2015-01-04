#' Check if default database is available.
#'
#' RMySQL examples and tests connect to a database defined by the
#' \code{rs-dbi} group in \code{~/.my.cnf}. This function checks if that
#' database is available, and if not, displays an informative message.
#'
#' @export
#' @examples
#' if (mysqlHasDefault()) {
#'   db <- dbConnect(RMySQL::MySQL(), dbname = "test")
#'   dbListTables(db)
#'   dbDisconnect(db)
#' }
mysqlHasDefault <- function() {
  tryCatch({
    dbConnect(MySQL(), dbname = "test")
    TRUE
  }, error = function(...) {
    message(
      "Could not initialise default MySQL database. If MySQL is running\n",
      "check that you have a ~/.my.cnf file that contains a [rs-dbi] section\n",
      "describing how to connect to a test database."
    )
    FALSE
  })
}

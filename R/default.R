#' Connect to a default database.
#'
#' Connect to a database configured by environment variables. The default
#' database name is "test", override with env var \code{MYSQL_DATABASE}.
#' Specify default username and password in \code{MYSQL_USER} and
#' \code{MYSQL_PASSWORD} envvars; or alternatively configured in
#' \code{my.ini} or \code{my.cnf}.
#' @export
#' @examples
#' if (mysqlHasDefault()) {
#'   db <- mysqlDefault()
#'   dbListTables(db)
#'   dbDisconnect(db)
#' }
mysqlDefault <- function() {
  dbname <- Sys.getenv("MYSQL_DATABASE", unset = "test")
  user <- Sys.getenv("MYSQL_USER", unset = NA)
  password <- Sys.getenv("MYSQL_PASSWORD", unset = '')

  if (is.na(user)) {
    # in this leg user and password should be set in my.ini or my.cnf files
    dbConnect(MySQL(), dbname = dbname)
  } else {
    dbConnect(MySQL(), user = user, password = password, dbname = dbname)
  }
}

#' @export
#' @rdname mysqlDefault
mysqlHasDefault <- function() {
  tryCatch({
    mysqlDefault()
    TRUE
  }, error = function(...) FALSE)
}

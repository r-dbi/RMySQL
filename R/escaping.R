#' @include Connection.R
NULL

#' Make R/S-Plus identifiers into legal SQL identifiers
#'
#' These methods are straight-forward implementations of the corresponding
#' generic functions.
#'
#' @param dbObj any MySQL object (e.g., \code{MySQLDriver}).
#' @param snames a character vector of R/S-Plus
#'   identifiers (symbols) from which we need to make SQL identifiers.
#' @param name a character vector of SQL identifiers we want to check against
#'   keywords from the DBMS.
#' @param unique logical describing whether the resulting set of SQL names
#'   should be unique.  Its default is \code{TRUE}. Following the SQL 92
#'   standard, uniqueness of SQL identifiers is determined regardless of whether
#'   letters are upper or lower case.
#' @param allow.keywords logical describing whether SQL keywords should be
#'   allowed in the resulting set of SQL names.  Its default is \code{TRUE}
#' @param keywords a character vector with SQL keywords, by default it is
#'   \code{.MySQLKeywords} define in \code{RMySQL}. This may be overriden by
#'   users.
#' @param case a character string specifying whether to make the
#'   comparison as lower case, upper case, or any of the two.  it defaults to
#'   \code{any}.
#' @param ... Unused, needed for compatibility with generic.
#' @export
setMethod("make.db.names", c("MySQLConnection", "character"),
  function(dbObj, snames, keywords, unique, allow.keywords, ...) {
    make.db.names.default(snames, .MySQLKeywords, unique, allow.keywords)
  }
)

#' @export
#' @rdname make.db.names-MySQLConnection-character-method
setMethod("SQLKeywords", "MySQLConnection", def = function(dbObj, ...) {
  .MySQLKeywords
})

#' @export
#' @rdname make.db.names-MySQLConnection-character-method
setMethod("isSQLKeyword", c("MySQLConnection", "character"),
  function(dbObj, name, keywords = .MySQLKeywords, case, ...) {
    isSQLKeyword.default(name, keywords = .MySQLKeywords, case = case)
  }
)

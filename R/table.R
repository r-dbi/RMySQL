#' @include MySQLConnection.R
NULL

#' Read and write MySQL tables.
#'
#' These functions mimic their R counterpart \code{get}, \code{assign},
#' \code{exists}, \code{remove}, and \code{ls}.
#'
#' @return A data.frame in the case of \code{dbReadTable}; otherwise a logical
#' indicating whether the operation was successful.
#' @note Note that the data.frame returned by \code{dbReadTable} only has
#' primitive data, e.g., it does not coerce character data to factors.
#'
#' @param conn a \code{\linkS4class{MySQLConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param name a character string specifying a table name.
#' @param check.names If \code{TRUE}, the default, column names will be
#'   converted to valid R identifiers.
#' @inheritParams SQL::rownamesToColumn
#' @param ... Unused, needed for compatiblity with generic.
#' @examples
#' if (mysqlHasDefault()) {
#' con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#'
#' # By default, row names are written in a column to row_names, and
#' # automatically read back into the row.names()
#' dbWriteTable(con, "mtcars", mtcars[1:5, ], overwrite = TRUE)
#' dbReadTable(con, "mtcars")
#' dbReadTable(con, "mtcars", row.names = NULL)
#' }
#' @name mysql-tables
NULL

#' @export
#' @rdname mysql-tables
setMethod("dbReadTable", c("MySQLConnection", "character"),
  function(conn, name, row.names, check.names = TRUE, ...) {
    name <- dbQuoteIdentifier(conn, name)
    out <- dbGetQuery(conn, paste("SELECT * FROM ", name),
      row.names = row.names)

    if (check.names) {
      names(out) <- make.names(names(out), unique = TRUE)
    }

    out
  }
)

#' @inheritParams SQL::rownamesToColumn
#' @inheritParams SQL::sqlTableCreate
#' @param overwrite a logical specifying whether to overwrite an existing table
#'   or not. Its default is \code{FALSE}. (See the BUGS section below)
#' @param append a logical specifying whether to append to an existing table
#'   in the DBMS.  Its default is \code{FALSE}.
#' @param allow.keywords DEPRECATED.
#' @export
#' @rdname mysql-tables
setMethod("dbWriteTable", c("MySQLConnection", "character", "data.frame"),
  function(conn, name, value, field.types = NULL, row.names = TRUE,
    overwrite = FALSE, append = FALSE, ..., allow.keywords = FALSE)     {

    if (!missing(allow.keywords)) {
      warning("allow.keywords is deprecated.")
    }

    if (overwrite && append)
      stop("overwrite and append cannot both be TRUE", call. = FALSE)

    dbBegin(conn)
    on.exit(dbRollback(conn))

    found <- dbExistsTable(conn, name)
    if (found && !overwrite && !append) {
      stop("Table ", name, " exists in database, and both overwrite and",
        " append are FALSE", call. = FALSE)
    }
    if (found && overwrite) {
      dbRemoveTable(conn, name)
    }

    if (!found || overwrite) {
      sql <- SQL::sqlTableCreate(conn, name, value, row.names = row.names)
      dbGetQuery(conn, sql)
    }

    if (nrow(value) > 0) {
      sql <- SQL::sqlTableInsertInto(conn, name, value)
      rs <- dbSendQuery(conn, sql)
    }

    on.exit(NULL)
    dbCommit(conn)

    TRUE
  }
)

#' @export
#' @rdname mysql-tables
#' @param sep field separator character
#' @param eol End-of-line separator
#' @param skip number of lines to skip before reading data in the input file.
#' @param quote the quote character used in the input file (defaults to
#'    \code{\"}.)
#' @param header logical, does the input file have a header line? Default is the
#'    same heuristic used by \code{read.table}, i.e., \code{TRUE} if the first
#'    line has one fewer column that the second line.
#' @param nrows number of lines to rows to import using \code{read.table} from
#'   the input file to create the proper table definition. Default is 50.
setMethod("dbWriteTable", c("MySQLConnection", "character", "character"),
  function(conn, name, value, field.types = NULL, overwrite = FALSE,
    append = FALSE, header = TRUE, row.names = FALSE, nrows = 50, sep = ",",
    eol="\n", skip = 0, quote = '"', ...)   {

    if (overwrite && append)
      stop("overwrite and append cannot both be TRUE", call. = FALSE)

    found <- dbExistsTable(conn, name)
    if (found && !overwrite && !append) {
      stop("Table ", name, " exists in database, and both overwrite and",
        " append are FALSE", call. = FALSE)
    }
    if (found && overwrite) {
      dbRemoveTable(conn, name)
    }

    if (!found || overwrite) {
      # Initialise table with first `nrows` lines
      d <- read.table(value, sep = sep, header = header, skip = skip, nrows = nrows,
        na.strings = "\\N", comment.char = "", stringsAsFactors = FALSE)
      sql <- mysqlBuildTableDefinition(conn, name, d, field.types = field.types,
        row.names = row.names)
      dbGetQuery(conn, sql)
    }

    path <- normalizePath(value, winslash = "/", mustWork = TRUE)
    sql <- paste0(
      "LOAD DATA LOCAL INFILE ", dbQuoteString(conn, path), "\n",
      "INTO TABLE ", dbQuoteIdentifier(conn, name), "\n",
      "FIELDS TERMINATED BY ", dbQuoteString(conn, encodeString(sep)), "\n",
      "OPTIONALLY ENCLOSED BY ", dbQuoteString(conn, quote), "\n",
      "LINES TERMINATED BY ", dbQuoteString(conn, encodeString(eol)), "\n",
      "IGNORE ", skip + as.integer(header), " LINES")
    dbSendQuery(conn, sql)

    TRUE
  }
)

#' @export
#' @rdname mysql-tables
setMethod("dbListTables", "MySQLConnection", function(conn, ...) {
  dbGetQuery(conn, "SHOW TABLES")[[1]]
})

#' @export
#' @rdname mysql-tables
setMethod("dbExistsTable", c("MySQLConnection", "character"),
  function(conn, name, ...) {
    name %in% dbListTables(conn)
  }
)

#' @export
#' @rdname mysql-tables
setMethod("dbRemoveTable", c("MySQLConnection", "character"),
  function(conn, name, ...){
    if (!dbExistsTable(conn, name)) return(FALSE)

    dbGetQuery(conn, paste("DROP TABLE", name))
    TRUE
  }
)

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
#' @rdname dbDataType-MySQLDriver-ANY-method
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

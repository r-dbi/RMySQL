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
#' @inheritParams DBI::sqlRownamesToColumn
#' @param ... Unused, needed for compatiblity with generic.
#' @examples
#' if (mysqlHasDefault()) {
#' con <- dbConnect(RMySQL::MySQL(), dbname = "test")
#'
#' # By default, row names are written in a column to row_names, and
#' # automatically read back into the row.names()
#' dbWriteTable(con, "mtcars", mtcars[1:5, ], temporary = TRUE)
#' dbReadTable(con, "mtcars")
#' dbReadTable(con, "mtcars", row.names = NULL)
#' }
#' @name mysql-tables
NULL

#' @export
#' @rdname mysql-tables
setMethod("dbReadTable", c("MySQLConnection", "character"),
  function(conn, name, row.names = NA, check.names = TRUE, ...) {
    name <- dbQuoteIdentifier(conn, name)
    out <- dbGetQuery(conn, paste("SELECT * FROM ", name),
      row.names = row.names)

    if (check.names) {
      names(out) <- make.names(names(out), unique = TRUE)
    }

    out
  }
)

#' @inheritParams DBI::sqlRownamesToColumn
#' @param overwrite a logical specifying whether to overwrite an existing table
#'   or not. Its default is \code{FALSE}. (See the BUGS section below)
#' @param append a logical specifying whether to append to an existing table
#'   in the DBMS.  If appending, then the table (or temporary table)
#'   must exist, otherwise an error is reported. Its default is \code{FALSE}.
#' @param value A data frame.
#' @param field.types Optional, overrides default choices of field types,
#'   derived from the classes of the columns in the data frame.
#' @param temporary If \code{TRUE}, creates a temporary table that expires
#'   when the connection is closed.
#' @param allow.keywords DEPRECATED.
#' @export
#' @rdname mysql-tables
setMethod("dbWriteTable", c("MySQLConnection", "character", "data.frame"),
  function(conn, name, value, field.types = NULL, row.names = NA,
           overwrite = FALSE, append = FALSE, ..., allow.keywords = FALSE,
           temporary = FALSE) {

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
    if (!found && append) {
      stop("Table ", name, " does not exists when appending")
    }

    if (!found || overwrite) {
      sql <- sqlCreateTable(conn, name, value, row.names = row.names,
        temporary = temporary)
      dbGetQuery(conn, sql)
    }

    if (nrow(value) > 0) {
      values <- sqlData(conn, value[, , drop = FALSE], row.names)

      name <- dbQuoteIdentifier(conn, name)
      fields <- dbQuoteIdentifier(conn, names(values))
      params <- rep("?", length(fields))

      sql <- paste0(
        "INSERT INTO ", name, " (", paste0(fields, collapse = ", "), ")\n",
        "VALUES (", paste0(params, collapse = ", "), ")"
      )
      rs <- dbSendQuery(conn, sql)
      tryCatch(
        result_bind_rows(rs@ptr, values),
        finally = dbClearResult(rs)
      )
    }

    on.exit(NULL)
    dbCommit(conn)

    TRUE
  }
)

setMethod("sqlData", "MySQLConnection", function(con, value, row.names = NA, ...) {
  value <- sqlRownamesToColumn(value, row.names)

  # Convert factors to strings
  is_factor <- vapply(value, is.factor, logical(1))
  value[is_factor] <- lapply(value[is_factor], as.character)

  # Ensure all in utf-8
  is_char <- vapply(value, is.character, logical(1))
  value[is_char] <- lapply(value[is_char], enc2utf8)

  value
})

#' @export
#' @rdname mysql-tables
#' @importFrom utils read.table
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
           append = FALSE, header = TRUE, row.names = FALSE, nrows = 50,
           sep = ",", eol = "\n", skip = 0, quote = '"', temporary = FALSE,
           ...) {

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
    if (!found && append) {
      stop("Table ", name, " does not exists when appending")
    }

    if (!found || overwrite) {
      if (is.null(field.types)) {
        # Initialise table with first `nrows` lines
        d <- read.table(value, sep = sep, header = header, skip = skip,
          nrows = nrows, na.strings = "\\N", comment.char = "",
          stringsAsFactors = FALSE)
        field.types <- vapply(d, dbDataType, dbObj = conn,
          FUN.VALUE = character(1))
      }

      sql <- sqlCreateTable(conn, name, field.types,
        row.names = row.names, temporary = temporary)
      dbGetQuery(conn, sql)
    }

    path <- normalizePath(value, winslash = "/", mustWork = TRUE)
    sql <- paste0(
      "LOAD DATA LOCAL INFILE ", dbQuoteString(conn, path), "\n",
      "INTO TABLE ", dbQuoteIdentifier(conn, name), "\n",
      "FIELDS TERMINATED BY ", dbQuoteString(conn, sep), "\n",
      "OPTIONALLY ENCLOSED BY ", dbQuoteString(conn, quote), "\n",
      "LINES TERMINATED BY ", dbQuoteString(conn, eol), "\n",
      "IGNORE ", skip + as.integer(header), " LINES")

    mysqlExecQuery(conn, sql)

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
    tryCatch({
      dbGetQuery(conn, paste0(
        "SELECT NULL FROM ", dbQuoteIdentifier(conn, name), " WHERE FALSE"
      ))
      TRUE
    }, error = function(...) {
      FALSE
    })
  }
)

#' @export
#' @rdname mysql-tables
setMethod("dbRemoveTable", c("MySQLConnection", "character"),
  function(conn, name, ...){
    name <- dbQuoteIdentifier(conn, name)
    dbGetQuery(conn, paste0("DROP TABLE ", name))
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
#' @rdname dbDataType
#' @examples
#' dbDataType(RMySQL::MySQL(), "a")
#' dbDataType(RMySQL::MySQL(), 1:3)
#' dbDataType(RMySQL::MySQL(), 2.5)
setMethod("dbDataType", "MySQLConnection", function(dbObj, obj, ...) {
  dbDataType(MySQL(), obj, ...)
})

#' @export
#' @rdname dbDataType
setMethod("dbDataType", "MySQLDriver", function(dbObj, obj, ...) {
  if (is.factor(obj)) return("TEXT")
  if (inherits(obj, "POSIXct")) return("DATETIME")
  if (inherits(obj, "Date")) return("DATE")

  switch(typeof(obj),
    logical = "TINYINT",
    integer = "INTEGER",
    double = "DOUBLE",
    character = "TEXT",
    list = "BLOB",
    stop("Unsupported type", call. = FALSE)
  )
})

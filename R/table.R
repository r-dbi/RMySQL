#' @include connection.R
NULL

#' Convenience functions for importing/exporting DBMS tables
#'
#' These functions mimic their R/S-Plus counterpart \code{get}, \code{assign},
#' \code{exists}, \code{remove}, and \code{objects}, except that they generate
#' code that gets remotely executed in a database engine.
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
#' @param row.names A string or an index specifying the column in the DBMS table
#'   to use as \code{row.names} in the output data.frame. Defaults to using the
#'   \code{row_names} column if present. Set to \code{NULL} to never use
#'   row names.
#' @param ... Unused, needed for compatiblity with generic.
#' @export
#' @rdname dbReadTable
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
setMethod("dbReadTable", c("MySQLConnection", "character"),
  function(conn, name, row.names, check.names = TRUE, ...) {
    out <- dbGetQuery(conn, paste("SELECT * FROM", name))

    if (check.names) {
      names(out) <- make.names(names(out), unique = TRUE)
    }
    row.names <- rownames_column(out, row.names)
    if (is.null(row.names)) return(out)

    rnms <- as.character(out[[row.names]])
    if (anyDuplicated(rnms)) {
      warning("row.names not set (duplicate elements in field)", call. = FALSE)
    } else {
      out <- out[, -row.names, drop = F]
      row.names(out) <- rnms
    }
    out
  }
)

## Create table "name" (must be an SQL identifier) and populate
## it with the values of the data.frame "value"
## TODO: This function should execute its sql as a single transaction,
##       and allow converter functions.
## TODO: In the unlikely event that value has a field called "row_names"
##       we could inadvertently overwrite it (here the user should set
##       row.names=F)  I'm (very) reluctantly adding the code re: row.names,
##       because I'm not 100% comfortable using data.frames as the basic
##       data for relations.
#' Write a local data frame or file to the database.
#'
#' @export
#' @rdname dbWriteTable
#' @param conn a \code{\linkS4class{MySQLConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param name a character string specifying a table name.
#' @param value a data.frame (or coercible to data.frame) object or a
#'   file name (character).  In the first case, the data.frame is
#'   written to a temporary file and then imported to SQLite; when \code{value}
#'   is a character, it is interpreted as a file name and its contents imported
#'   to SQLite.
#' @param row.names A logical specifying whether the \code{row.names} should be
#'   output to the output DBMS table; if \code{TRUE}, an extra field whose name
#'   will be whatever the R identifier \code{"row.names"} maps to the DBMS (see
#'   \code{\link[DBI]{make.db.names}}). If \code{NA} will add rows names if
#'   they are characters, otherwise will ignore.
#' @param overwrite a logical specifying whether to overwrite an existing table
#'   or not. Its default is \code{FALSE}. (See the BUGS section below)
#' @param append a logical specifying whether to append to an existing table
#'   in the DBMS.  Its default is \code{FALSE}.
#' @param field.types character vector of named  SQL field types where
#'   the names are the names of new table's columns. If missing, types inferred
#'   with \code{\link[DBI]{dbDataType}}).
#' @param allow.keywords logical indicating whether column names that happen to
#'  be MySQL keywords be used as column names in the resulting relation (table)
#'  being written. Defaults to FALSE, forcing mysqlWriteTable to modify column
#'  names to make them legal MySQL identifiers.
#' @param header logical, does the input file have a header line? Default is the
#'    same heuristic used by \code{read.table}, i.e., \code{TRUE} if the first
#'    line has one fewer column that the second line.
#' @param nrows number of lines to rows to import using \code{read.table} from
#'   the input file to create the proper table definition. Default is 50.
#' @param sep field separator character
#' @param eol End-of-line separator
#' @param skip number of lines to skip before reading data in the input file.
#' @param quote the quote character used in the input file (defaults to
#'    \code{\"}.)
#' @param ... Unused, needs for compatibility with generic.
#' @export
setMethod("dbWriteTable", c("MySQLConnection", "character", "data.frame"),
  function(conn, name, value, field.types = NULL, row.names = TRUE,
    overwrite = FALSE, append = FALSE, ..., allow.keywords = FALSE)     {

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

    value <- explict_rownames(value, row.names)

    if (!found || overwrite) {
      sql <- mysqlBuildTableDefinition(conn, name, value,
        field.types = field.types, row.names = FALSE)
      dbGetQuery(conn, sql)
    }

    if (nrow(value) == 0) return(TRUE)

    ## Save file to disk, then use LOAD DATA command
    fn <- normalizePath(tempfile("rsdbi"), winslash = "/", mustWork = FALSE)
    safe.write(value, file = fn)
    on.exit(unlink(fn), add = TRUE)

    sql <- paste0(
      "LOAD DATA LOCAL INFILE ", dbQuoteString(conn, fn),
      "  INTO TABLE ", dbQuoteIdentifier(conn, name),
      "  FIELDS TERMINATED BY '\t' ",
      "  LINES TERMINATED BY '\n' ",
      "  (", paste(dbQuoteIdentifier(conn, names(value)), collapse=", "), ");"
    )
    dbGetQuery(conn, sql)

    TRUE
  }
)

#' @export
#' @rdname dbWriteTable
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
      "FIELDS TERMINATED BY ", dbQuoteString(conn, sep), "\n",
      "OPTIONALLY ENCLOSED BY ", dbQuoteString(conn, quote), "\n",
      "LINES TERMINATED BY ", dbQuoteString(conn, eol), "\n",
      "IGNORE ", skip + as.integer(header), " LINES")
    dbSendQuery(conn, sql)

    TRUE
  }
)

#' @export
#' @rdname dbReadTable
setMethod("dbListTables", "MySQLConnection", function(conn, ...) {
  dbGetQuery(conn, "SHOW TABLES")[[1]]
})

#' @export
#' @rdname dbReadTable
setMethod("dbExistsTable", c("MySQLConnection", "character"),
  function(conn, name, ...) {
    name %in% dbListTables(conn)
  }
)

#' @export
#' @rdname dbReadTable
setMethod("dbRemoveTable", c("MySQLConnection", "character"),
  function(conn, name, ...){
    if (!dbExistsTable(conn, name)) return(FALSE)

    dbGetQuery(conn, paste("DROP TABLE", name))
    TRUE
  }
)

#' @export
#' @rdname dbReadTable
setMethod("dbListFields", c("MySQLConnection", "character"),
  function(conn, name, ...){
    dbGetQuery(conn, paste("DESCRIBE", name))[[1]]
  }
)

#' Experimental dbColumnInfo method for a connection
#'
#' @export
#' @keywords internal
setMethod("dbColumnInfo", "MySQLConnection", function(res, name, ...) {
  rs <- dbSendQuery(res, paste0("SELECT * from ", dbQuoteIdentifier(res, name)))
  on.exit(dbClearResult(rs))

  dbColumnInfo(rs)
})


# Row name handling ------------------------------------------------------------

explict_rownames <- function(df, row.names = NA) {
  if (is.na(row.names)) {
    row.names <- is.character(attr(df, "row.names"))
  }
  if (!row.names) return(df)

  rn <- data.frame(row_names = row.names(df))
  cbind(rn, df)
}

# Figure out which column to
rownames_column <- function(df, row.names) {
  if (missing(row.names)) {
    if (!"row_names" %in% names(df)) {
      return(NULL)
    }

    row.names <- "row_names"
  }

  if (is.null(row.names) || identical(row.names, FALSE)) {
    NULL
  } else if (is.character(row.names)) {
    if (!(row.names %in% names(df))) {
      stop("Column ", row.names, " not present in output", call. = FALSE)
    }
    match(row.names, names(df))
  } else if (is.numeric(row.names)) {
    if (row.names == 0) return(NULL)
    if (row.names < 0 || row.names > ncol(df)) {
      stop("Column ", row.names, " not present in output", call. = FALSE)
    }
    row.names
  } else {
    stop("Unknown specification for row.names")
  }
}

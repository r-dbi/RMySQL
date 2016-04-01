#' @include MySQLConnection.R
NULL

#' Fetch rows (deprecated)
#'
#' Please use \code{dbFetch} instead
#'
#' @export
#' @keywords internal
setMethod("fetch", c("MySQLResult", "numeric"), function(res, n) {
  result_fetch(res@ptr, n)
})


#' Make R/S-Plus identifiers into legal SQL identifiers
#'
#' DEPRECATED: please use \code{dbQuoteIdentifier} instead.
#'
#' @keywords internal
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

## the following reserved words were taken from Section 6.1.7
## of the MySQL Manual, Version 4.1.1-alpha, html format.
.MySQLKeywords <- c("ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC",
  "ASENSITIVE", "AUTO_INCREMENT", "BDB", "BEFORE", "BERKELEYDB", "BETWEEN",
  "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE",
  "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN", "COLUMNS",
  "CONDITION", "CONNECTION", "CONSTRAINT", "CONTINUE", "CREATE",
  "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
  "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND",
  "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT",
  "DELAYED", "DELETE", "DESC", "DESCRIBE", "DISTINCT", "DISTINCTROW",
  "DIV", "DOUBLE", "DROP", "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED",
  "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FIELDS", "FLOAT",
  "FOR", "FORCE", "FOREIGN", "FOUND", "FROM", "FULLTEXT", "GRANT",
  "GROUP", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE",
  "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER",
  "INNODB", "INOUT", "INSENSITIVE", "INSERT", "INT", "INTEGER",
  "INTERVAL", "INTO", "IO_THREAD", "IS", "ITERATE", "JOIN", "KEY",
  "KEYS", "KILL", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT",
  "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG",
  "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY", "MASTER_SERVER_ID",
  "MATCH", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT",
  "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "NATURAL", "NOT",
  "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE", "OPTION",
  "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER", "OUTFILE", "PRECISION",
  "PRIMARY", "PRIVILEGES", "PROCEDURE", "PURGE", "READ", "REAL",
  "REFERENCES", "REGEXP", "RENAME", "REPEAT", "REPLACE", "REQUIRE",
  "RESTRICT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "RLIKE",
  "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET",
  "SHOW", "SMALLINT", "SOME", "SONAME", "SPATIAL", "SPECIFIC",
  "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT",
  "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING",
  "STRAIGHT_JOIN", "STRIPED", "TABLE", "TABLES", "TERMINATED",
  "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING",
  "TRUE", "TYPES", "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED",
  "UPDATE", "USAGE", "USE", "USER_RESOURCES", "USING", "UTC_DATE",
  "UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR",
  "VARCHARACTER", "VARYING", "WHEN", "WHERE", "WHILE", "WITH",
  "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL"
)

#' Check if a database object is valid.
#'
#' DEPRECATED
#'
#' @keywords internal
#' @return a logical scalar.
#' @export
#' @examples
#' dbIsValid(MySQL())
isIdCurrent <- function(obj)  {
  .Deprecated("dbIsValid")
  dbIsValid(obj)
}


#' Escape SQL-special characters in strings.
#'
#' DEPRECATED: Please use \code{dbQuoteString} instead.
#'
#' @export
#' @keywords internal
setGeneric("dbEscapeStrings", function(con, strings, ...) {
  .Deprecated("dbQuoteString")
  dbQuoteString(con, strings, ..)
})

#' Build the SQL CREATE TABLE definition as a string
#'
#' DEPRECATED
#'
#' @keywords internal
#' @export
mysqlBuildTableDefinition <- function(dbObj, name, obj, field.types = NULL,
  row.names = TRUE, ...) {

  warning("Deprecated. Please use DBI::sqlCreateTable instead.")

  if (!is.data.frame(obj)) {
    obj <- as.data.frame(obj)
  }
  value <- sqlColumnToRownames(obj, row.names)

  if (is.null(field.types)) {
    field.types <- vapply(value, dbDataType, dbObj = dbObj,
      FUN.VALUE = character(1))
  }
  # Escape field names
  names(field.types) <- dbQuoteIdentifier(dbObj, names(field.types))

  flds <- paste(names(field.types), field.types)
  paste("CREATE TABLE", name, "\n(", paste(flds, collapse = ",\n\t"), "\n)")
}

## Escape problematic characters in the data frame.
## These are: - tab, as this is the field separator
##            - newline, as this is the record separator
##            - backslash, the escaping character
## Obviously, not all data types can contain these, e.g. numeric types
## can not. So we only substitute character and factor types.
## (FIXME: is there anything else?)
escape <- function(table) {
  table <- as.data.frame(table)
  repcols <- which(sapply(table, is.character) | sapply(table, is.factor))
  for (rc in repcols) {
    table[,rc] <- gsub("\\\\", "\\\\\\\\", table[,rc])
    table[,rc] <- gsub("\\n", "\\\\n", table[,rc])
    table[,rc] <- gsub("\\t", "\\\\t", table[,rc])
  }
  table
}


## safe.write makes sure write.table doesn't exceed available memory by batching
## at most batch rows (but it is still slowww)
safe.write <- function(value, file, batch, ...) {
  N <- nrow(value)
  if(N<1){
    warning("no rows in data.frame")
    return(NULL)
  }
  digits <- options(digits = 17)
  on.exit(options(digits))
  if(missing(batch) || is.null(batch))
    batch <- 10000
  else if(batch<=0)
    batch <- N
  from <- 1
  to <- min(batch, N)
  conb <- file(file,open="wb")
  while(from<=N){
    write.table(escape(value[from:to,, drop=FALSE]), file = conb,
      append = TRUE, quote = FALSE, sep="\t", na = "\\N",
      row.names=FALSE, col.names=FALSE, eol = '\n', ...)
    from <- to+1
    to <- min(to+batch, N)
  }
  close(conb)
  invisible(NULL)
}

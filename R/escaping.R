#' @include connection.R
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

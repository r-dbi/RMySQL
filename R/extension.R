#' @include mysql.R result.R
NULL

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
#' The output SQL statement is a simple \code{CREATE TABLE} with suitable for
#' \code{dbGetQuery}
#'
#' @param dbObj any DBI object (used only to dispatch according to the engine
#' (e.g., MySQL, Oracle, PostgreSQL, SQLite)
#' @param name name of the new SQL table
#' @param obj an R object coerceable to data.frame for which we want to create
#' a table
#' @param field.types optional named list of the types for each field in
#' \code{obj}
#' @param row.names logical, should row.name of \code{value} be exported as a
#' \code{row\_names} field? Default is TRUE
#' @param \dots reserved for future use
#' @return An SQL string
#' @export
#' @keywords internal
mysqlBuildTableDefinition <- function(dbObj, name, obj, field.types = NULL,
                                   row.names = TRUE, ...) {
  if (!is.data.frame(obj)) {
    obj <- as.data.frame(obj)
  }
  value <- explict_rownames(obj, row.names)

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

#' MySQL Check for Compiled Versus Loaded Client Library Versions
#'
#' This function prints out the compiled and loaded client library versions.
#'
#' @return A named integer vector of length two, the first element representing
#' the compiled library version and the second element representint the loaded
#' client library version.
#' @export
#' @examples
#' mysqlClientLibraryVersions()
#' @useDynLib RMySQL rmysql_version
mysqlClientLibraryVersions <- function() {
  .Call(rmysql_version)
}

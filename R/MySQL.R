## Copyright (C) 1999 The Omega Project for Statistical Computing.
##
## This library is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License
## as published by the Free Software Foundation; either
## version 2 of the License, or (at your option) any later version.
##
## This library is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public
## License along with this library; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA

#' Constants
#'
#' @aliases .MySQLPkgName .MySQLPkgVersion .MySQLPkgRCS .MySQL.NA.string
#' .MySQLSQLKeywords CLIENT_LONG_PASSWORD CLIENT_FOUND_ROWS CLIENT_LONG_FLAG
#' CLIENT_CONNECT_WITH_DB CLIENT_NO_SCHEMA CLIENT_COMPRESS CLIENT_ODBC
#' CLIENT_LOCAL_FILES CLIENT_IGNORE_SPACE CLIENT_PROTOCOL_41 CLIENT_INTERACTIVE
#' CLIENT_SSL CLIENT_IGNORE_SIGPIPE CLIENT_TRANSACTIONS CLIENT_RESERVED
#' CLIENT_SECURE_CONNECTION CLIENT_MULTI_STATEMENTS CLIENT_MULTI_RESULTS
#' @section Constants: \code{.MySQLPkgName} (currently \code{"RMySQL"}),
#' \code{.MySQLPkgVersion} (the R package version), \code{.MySQLPkgRCS} (the
#' RCS revision), \code{.MySQL.NA.string} (character that MySQL uses to denote
#' \code{NULL} on input), \code{.MySQLSQLKeywords} (a lot!)
#' @name constants
NULL

.MySQLPkgName <- "RMySQL"      ## should we set thru package.description()?
.MySQLVersion <- "0.5-12"      ##package.description(.MySQLPkgName, fields = "Version")
.MySQL.NA.string <- "\\N"      ## on input, MySQL interprets \N as NULL (NA)

## The following client flags were copied from mysql_com.h (version 4.1.13)
## but it may not make sense to set some of this from RMySQL.

#' @export
CLIENT_LONG_PASSWORD <-   1    # new more secure passwords
#' @export
CLIENT_FOUND_ROWS    <-   2    # Found instead of affected rows
#' @export
CLIENT_LONG_FLAG     <-   4    # Get all column flags
#' @export
CLIENT_CONNECT_WITH_DB <- 8    # One can specify db on connect
#' @export
CLIENT_NO_SCHEMA     <-  16    # Don't allow database.table.column
#' @export
CLIENT_COMPRESS      <-  32    # Can use compression protocol
#' @export
CLIENT_ODBC          <-  64    # Odbc client
#' @export
CLIENT_LOCAL_FILES   <- 128    # Can use LOAD DATA LOCAL
#' @export
CLIENT_IGNORE_SPACE  <- 256    # Ignore spaces before '('
#' @export
CLIENT_PROTOCOL_41   <- 512    # New 4.1 protocol
#' @export
CLIENT_INTERACTIVE   <- 1024   # This is an interactive client
#' @export
CLIENT_SSL           <- 2048   # Switch to SSL after handshake
#' @export
CLIENT_IGNORE_SIGPIPE <- 4096  # IGNORE sigpipes
#' @export
CLIENT_TRANSACTIONS <- 8192    # Client knows about transactions
#' @export
CLIENT_RESERVED     <- 16384   # Old flag for 4.1 protocol
#' @export
CLIENT_SECURE_CONNECTION <- 32768 # New 4.1 authentication
#' @export
CLIENT_MULTI_STATEMENTS  <- 65536 # Enable/disable multi-stmt support
#' @export
CLIENT_MULTI_RESULTS     <- 131072 # Enable/disable multi-results

setOldClass("data.frame")      ## to appease setMethod's signature warnings...

#' Class MySQLObject
#'
#' Base class for all MySQL-specific DBI classes
#'
#' @export
setClass("MySQLObject",
  contains = c("DBIObject", "VIRTUAL"),
  slots = list(Id = "integer")
)

## coercion methods
setAs("MySQLObject", "integer",
  def = function(from) as(slot(from,"Id"), "integer")
)
setAs("MySQLObject", "numeric",
  def = function(from) as(slot(from, "Id"), "integer")
)
setAs("MySQLObject", "character",
  def = function(from) as(slot(from, "Id"), "character")
)

#### Temporary compatibility fix for TSMySQL
setClass("dbObjectId")
setAs("dbObjectId", "integer",
  def = function(from) as(slot(from,"Id"), "integer")
)

setGeneric("summary")
setGeneric("format")

## formating, showing, printing,...
setMethod("format", "MySQLObject", function(x, ...) {
  paste("(", paste(as(x, "integer"), collapse=","), ")", sep="")
})

setMethod("show", "MySQLObject", function(object) {
  expired <- if(isIdCurrent(object)) "" else "Expired "
  str <- paste("<", expired, class(object), ":", format(object), ">", sep="")
  cat(str, "\n")
  invisible(NULL)
})

#' Check whether a database handle object is valid or not
#'
#' Support function that verifies that an object holding a reference to a
#' foreign object is still valid for communicating with the RDBMS
#'
#' \code{dbObjects} are R/S-Plus remote references to foreign objects. This
#' introduces differences to the object's semantics such as persistence (e.g.,
#' connections may be closed unexpectedly), thus this function provides a
#' minimal verification to ensure that the foreign object being referenced can
#' be contacted.
#'
#' @param obj any \code{dbObject} (e.g., \code{dbDriver}, \code{dbConnection},
#' \code{dbResult}).
#' @return a logical scalar.
#' @export
#' @examples
#' isIdCurrent(MySQL())
isIdCurrent <- function(obj)  {
  obj <- as(obj, "integer")
  .Call("RS_DBI_validHandle", obj, PACKAGE = .MySQLPkgName)
}

#' Determine the SQL Data Type of an S object
#'
#' This method is a straight-forward implementation of the corresponding
#' generic function.
#'
#' @param dbObj any \code{MySQLObject} object, e.g., \code{MySQLDriver},
#' \code{MySQLConnection}, \code{MySQLResult}.
#' @param obj R/S-Plus object whose SQL type we want to determine.
#' @param \dots any other parameters that individual methods may need.
#' @export
#' @examples
#' dbDataType(RMySQL::MySQL(), "a")
#' dbDataType(RMySQL::MySQL(), 1:3)
#' dbDataType(RMySQL::MySQL(), 2.5)
setMethod("dbDataType", c("MySQLObject", "ANY"), function(dbObj, obj) {
  rs.class <- data.class(obj)    ## this differs in R 1.4 from older vers
  rs.mode <- storage.mode(obj)
  if(rs.class == "numeric" || rs.class == "integer") {
    sql.type <- if(rs.mode=="integer") "bigint" else  "double"
  } else {
    sql.type <- switch(rs.class,
      character = "text",
      logical = "tinyint",  ## but we need to coerce to int!!
      factor = "text",      ## up to 65535 characters
      ordered = "text",
      "text")
  }
  sql.type
})

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
setMethod("make.db.names",
   signature(dbObj="MySQLObject", snames = "character"),
   def = function(dbObj, snames, keywords = .MySQLKeywords,
       unique, allow.keywords, ...){
       #      make.db.names.default(snames, keywords = .MySQLKeywords, unique = unique,
       #                            allow.keywords = allow.keywords)
       "makeUnique" <- function(x, sep = "_") {
	   if (length(x) == 0)
	       return(x)
	   out <- x
	   lc <- make.names(tolower(x), unique = FALSE)
	   i <- duplicated(lc)
	   lc <- make.names(lc, unique = TRUE)
	   out[i] <- paste(out[i], substring(lc[i], first = nchar(out[i]) +
		   1), sep = sep)
	   out
       }
       fc <- substring(snames, 1, 1)
       lc <- substring(snames, nchar(snames))
       i <- match(fc, c("'", "\"","`"), 0) > 0 & match(lc, c("'", "\"","`"),
	   0) > 0
       snames[!i] <- make.names(snames[!i], unique = FALSE)
       if (unique)
	   snames[!i] <- makeUnique(snames[!i])
       if (!allow.keywords) {
	   kwi <- match(keywords, toupper(snames), nomatch = 0L)

	   # We could check to see if the database we are connected to is
	   # running in ANSI mode. That would allow double quoted strings
	   # as database identifiers. Until then, the backtick needs to be used.
	   snames[kwi] <- paste("`", snames[kwi], "`", sep = "")
       }
       gsub("\\.", "_", snames)
   },
   valueClass = "character"
)

#' @export
#' @rdname make.db.names-MySQLObject-character-method
setMethod("SQLKeywords", "MySQLObject",
   def = function(dbObj, ...) .MySQLKeywords,
   valueClass = "character"
)

#' @export
#' @rdname make.db.names-MySQLObject-character-method
setMethod("isSQLKeyword",
   signature(dbObj="MySQLObject", name="character"),
   def = function(dbObj, name, keywords = .MySQLKeywords, case, ...){
        isSQLKeyword.default(name, keywords = .MySQLKeywords, case = case)
   },
   valueClass = "character"
)


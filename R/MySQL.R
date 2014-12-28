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

##
## Constants
##

.MySQLPkgName <- "RMySQL"      ## should we set thru package.description()?
.MySQLVersion <- "0.5-12"      ##package.description(.MySQLPkgName, fields = "Version")
.MySQL.NA.string <- "\\N"      ## on input, MySQL interprets \N as NULL (NA)

## The following client flags were copied from mysql_com.h (version 4.1.13)
## but it may not make sense to set some of this from RMySQL.

CLIENT_LONG_PASSWORD <-   1    # new more secure passwords
CLIENT_FOUND_ROWS    <-   2    # Found instead of affected rows
CLIENT_LONG_FLAG     <-   4    # Get all column flags
CLIENT_CONNECT_WITH_DB <- 8    # One can specify db on connect
CLIENT_NO_SCHEMA     <-  16    # Don't allow database.table.column
CLIENT_COMPRESS      <-  32    # Can use compression protocol
CLIENT_ODBC          <-  64    # Odbc client
CLIENT_LOCAL_FILES   <- 128    # Can use LOAD DATA LOCAL
CLIENT_IGNORE_SPACE  <- 256    # Ignore spaces before '('
CLIENT_PROTOCOL_41   <- 512    # New 4.1 protocol
CLIENT_INTERACTIVE   <- 1024   # This is an interactive client
CLIENT_SSL           <- 2048   # Switch to SSL after handshake
CLIENT_IGNORE_SIGPIPE <- 4096  # IGNORE sigpipes
CLIENT_TRANSACTIONS <- 8192    # Client knows about transactions
CLIENT_RESERVED     <- 16384   # Old flag for 4.1 protocol
CLIENT_SECURE_CONNECTION <- 32768 # New 4.1 authentication
CLIENT_MULTI_STATEMENTS  <- 65536 # Enable/disable multi-stmt support
CLIENT_MULTI_RESULTS     <- 131072 # Enable/disable multi-results

setOldClass("data.frame")      ## to appease setMethod's signature warnings...

##
## Class: DBIObject
##
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

## formating, showing, printing,...
setMethod("format", "MySQLObject",
  def = function(x, ...) {
    paste("(", paste(as(x, "integer"), collapse=","), ")", sep="")
  },
  valueClass = "character"
)

setMethod("show", "MySQLObject", def = function(object) {
  expired <- if(isIdCurrent(object)) "" else "Expired "
  str <- paste("<", expired, class(object), ":", format(object), ">", sep="")
  cat(str, "\n")
  invisible(NULL)
})

## verify that obj refers to a currently open/loaded database
isIdCurrent <- function(obj)  {
  obj <- as(obj, "integer")
  .Call("RS_DBI_validHandle", obj, PACKAGE = .MySQLPkgName)
}

setMethod("dbDataType",
   signature(dbObj = "MySQLObject", obj = "ANY"),
   def = function(dbObj, obj, ...) mysqlDataType(obj, ...),
   valueClass = "character"
)

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

setMethod("SQLKeywords", "MySQLObject",
   def = function(dbObj, ...) .MySQLKeywords,
   valueClass = "character"
)

setMethod("isSQLKeyword",
   signature(dbObj="MySQLObject", name="character"),
   def = function(dbObj, name, keywords = .MySQLKeywords, case, ...){
        isSQLKeyword.default(name, keywords = .MySQLKeywords, case = case)
   },
   valueClass = "character"
)

## extension to the DBI 0.1-4


#### Temporary compatibility fix for TSMySQL
setClass("dbObjectId")
setAs("dbObjectId", "integer",
  def = function(from) as(slot(from,"Id"), "integer")
)
####

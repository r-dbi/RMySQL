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



#' R interface to the MySQL database
#'
#' The functions in this package allow you interact with one or more MySQL
#' databases from R.
#'
#'
#' @name RMySQL-package
#' @aliases RMySQL-package RMySQL
#' @docType package
#' @section Overview: A typical usage of the R-MySQL interface is: \enumerate{
#' \item Connect and authenticate to one or more MySQL databases:
#' \preformatted{ con <- dbConnect(MySQL(), group = "lasers") con2 <-
#' dbConnect(MySQL(), user="opto", password="pure-light", dbname="lasers",
#' host="merced") } \item List tables and fields in a table: \preformatted{
#' dbListTables(con) dbListFields(con, "table\_name") } \item Import and export
#' data.frames: \preformatted{ d <- dbReadTable(con, "WL") dbWriteTable(con,
#' "WL2", a.data.frame) ## table from a data.frame dbWriteTable(con, "test2",
#' "~/data/test2.csv") ## table from a file } \item Run an arbitrary SQL
#' statement and extract all its output (returns a data.frame): \preformatted{
#' dbGetQuery(con, "select count(*) from a\_table") dbGetQuery(con, "select *
#' from a\_table") } \item Run an SQL statement and extract its output in
#' pieces (returns a result set): \preformatted{ rs <- dbSendQuery(con, "select
#' * from WL where width\_nm between 0.5 and 1") d1 <- fetch(rs, n = 10000) d2
#' <- fetch(rs, n = -1 } \item Run multiple SQL statements and process the
#' various result sets (note the \code{client.flag} value in the
#' \code{dbConnect} call): \preformatted{ con <- dbConnection(MySQL(), dbname =
#' "rs-dbi", client.flag = CLIENT\_MULTI\_STATEMENTS) script <- paste("select *
#' from WL where width\_nm between 0.5 and 1" "select * from lasers\_id where
#' id LIKE 'AL100%' sep = ";") rs1 <- dbSendQuery(con, script) d1 <- fetch(rs1,
#' n = -1) if(dbMoreResults(con)){ rs2 <- dbNextResult(con) d2 <- fetch(rs2,
#' n=-1) } } \item Get meta-information on a connection (thread-id, etc.):
#' \preformatted{ summary(MySQL(), verbose = TRUE) summary(con, verbose = TRUE)
#' summary(rs, verbose = TRUE) dbListConnections(MySQL()) dbListResultSets(con)
#' dbHasCompleted(rs) } \item Close connections: \preformatted{
#' dbDisconnect(con) dbDisconnect(con2) } }
#' @author David A. James <dj@@bell-labs.com> Saikat DebRoy
#' <saikat@@stat.wisc.edu>
#' @seealso On database managers:
#'
#' \code{\link[DBI]{dbDriver}} \code{\link[DBI]{dbUnloadDriver}}
#'
#' On connections, SQL statements and resultSets:
#'
#' \code{\link[DBI]{dbConnect}} \code{\link[DBI]{dbDisconnect}}
#' \code{\link[DBI]{dbSendQuery}} \code{\link[DBI]{dbGetQuery}}
#' \code{\link[DBI]{fetch}} \code{\link[DBI]{dbClearResult}}
#'
#' On transaction management:
#'
#' \code{\link[DBI]{dbCommit}} \code{\link[DBI]{dbRollback}}
#'
#' On meta-data:
#'
#' \code{\link{summary}} \code{\link[DBI]{dbGetInfo}}
#' \code{\link[DBI]{dbGetDBIVersion}} \code{\link[DBI]{dbListTables}}
#' \code{\link[DBI]{dbListConnections}} \code{\link[DBI]{dbListResults}}
#' \code{\link[DBI]{dbColumnInfo}} \code{\link[DBI]{dbGetException}}
#' \code{\link[DBI]{dbGetStatement}} \code{\link[DBI]{dbHasCompleted}}
#' \code{\link[DBI]{dbGetRowCount}}
#' @keywords package interface database
#' @examples
#' \dontrun{
#' # create a MySQL instance and create one connection.
#' > m <- dbDriver("MySQL")  ## or MySQL()
#' <MySQLDriver:(4378)>
#'
#' # open the connection using user, passsword, etc., as
#' # specified in the "[iptraffic]" section of the
#' # configuration file \file{\$HOME/.my.cnf}
#' > con <- dbConnect(m, group = "iptraffic")
#' > rs <- dbSendQuery(con, "select * from HTTP_ACCESS where IP_ADDRESS = '127.0.0.1'")
#' > df <- fetch(rs, n = 50)
#' > dbHasCompleted(rs)
#' [1] FALSE
#' > df2 <- fetch(rs, n = -1)
#' > dbHasCompleted(rs)
#' [1] TRUE
#' > dbClearResult(rs)
#' > dim(dbGetQuery(con, "show tables"))
#' [1] 74   1
#' > dbListTables(con)
#' }
#'
NULL


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
#' \code{NULL} on input), \code{.MySQLSQLKeywords} (a lot!) %\non_function{}
#' @keywords datasets interface database
#' @name constants
NULL

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

#' Class MySQLObject
#'
#' Base class for all MySQL-specific DBI classes
#'
#'
#' @name MySQLObject-class
#' @aliases MySQLObject-class dbObjectId-class
#' @docType class
#' @section Objects from the Class: A virtual Class: No objects may be created
#' from it.
#' @seealso DBI base classes:
#'
#' \code{\link[DBI]{DBIObject-class}} \code{\link[DBI]{DBIDriver-class}}
#' \code{\link[DBI]{DBIConnection-class}} \code{\link[DBI]{DBIResult-class}}
#'
#' MySQL classes:
#'
#' \code{\link{MySQLObject-class}} \code{\link{MySQLDriver-class}}
#' \code{\link{MySQLConnection-class}} \code{\link{MySQLResult-class}}
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://developer.r-project.org/db}.
#' @keywords database interface classes
#' @examples
#' \dontrun{
#' drv <- dbDriver("MySQL")
#' con <- dbConnect(drv, dbname = "rsdbi.db")
#' }
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
#' @seealso \code{\link[DBI]{dbDriver}} \code{\link[DBI]{dbConnect}}
#' \code{\link[DBI]{dbSendQuery}} \code{\link[DBI]{fetch}}
#' @keywords interface database
#' @examples
#' \dontrun{
#' cursor <- dbSendQuery(con, sql.statement)
#' isIdCurrent(cursor)
#' }
#'
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
#' @seealso \code{\link[DBI]{isSQLKeyword}} \code{\link[DBI]{make.db.names}}
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
#' @export
#' @examples
#' \dontrun{
#' data(quakes)
#' drv <- dbDriver("MySQL")
#' sql.type <- dbDataType(drv, quakes)
#' }
setMethod("dbDataType",
   signature(dbObj = "MySQLObject", obj = "ANY"),
   def = function(dbObj, obj, ...) mysqlDataType(obj, ...),
   valueClass = "character"
)

#' Make R/S-Plus identifiers into legal SQL identifiers
#'
#' These methods are straight-forward implementations of the corresponding
#' generic functions.
#'
#'
#' @name make.db.names-methods
#' @aliases SQLKeywords-methods isSQLKeyword-methods
#' make.db.names,MySQLObject,character-method SQLKeywords,MySQLObject-method
#' SQLKeywords,missing-method isSQLKeyword,MySQLObject,character-method
#' @docType methods
#' @section Methods: \describe{ \item{dbObj}{ any MySQL object (e.g.,
#' \code{MySQLDriver}).  } \item{snames}{ a character vector of R/S-Plus
#' identifiers (symbols) from which we need to make SQL identifiers.  }
#' \item{name}{ a character vector of SQL identifiers we want to check against
#' keywords from the DBMS. } \item{unique}{ logical describing whether the
#' resulting set of SQL names should be unique.  Its default is \code{TRUE}.
#' Following the SQL 92 standard, uniqueness of SQL identifiers is determined
#' regardless of whether letters are upper or lower case.  }
#' \item{allow.keywords }{ logical describing whether SQL keywords should be
#' allowed in the resulting set of SQL names.  Its default is \code{TRUE} }
#' \item{keywords}{ a character vector with SQL keywords, by default it is
#' \code{.MySQLKeywords} define in \code{RMySQL}. This may be overriden by
#' users.  } \item{case}{ a character string specifying whether to make the
#' comparison as lower case, upper case, or any of the two.  it defaults to
#' \code{any}.  } \item{list()}{currently not used.} }
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{dbReadTable}},
#' \code{\link[DBI]{dbWriteTable}}, \code{\link[DBI]{dbExistsTable}},
#' \code{\link[DBI]{dbRemoveTable}}, \code{\link[DBI]{dbListTables}}.
#' @references The set of SQL keywords is stored in the character vector
#' \code{.SQL92Keywords} and reflects the SQL ANSI/ISO standard as documented
#' in "X/Open SQL and RDA", 1994, ISBN 1-872630-68-8.  Users can easily
#' override or update this vector.
#'
#' MySQL does add some keywords to the SQL 92 standard, they are listed in the
#' \code{.MySQLKeywords} object.
#'
#' See the Database Interface definition document \code{DBI.pdf} in the base
#' directory of this package or \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
#' @examples
#' \dontrun{
#' # This example shows how we could export a bunch of data.frames
#' # into tables on a remote database.
#'
#' con <- dbConnect("MySQL", "user", "password")
#'
#' export <- c("trantime.email", "trantime.print", "round.trip.time.email")
#' tabs <- make.db.names(export, unique = T, allow.keywords = T)
#'
#' for(i in seq(along = export) )
#'    dbWriteTable(con, name = tabs[i],  get(export[i]))
#' }
#'
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

#' Summarize an MySQL object
#'
#' These methods are straight-forward implementations of the corresponding
#' generic functions.
#'
#'
#' @name summary-methods
#' @aliases coerce-methods summary-methods format-methods show-methods
#' coerce,dbObjectId,integer-method coerce,MySQLObject,integer-method
#' coerce,MySQLObject,numeric-method coerce,MySQLObject,character-method
#' coerce,MySQLObject,MySQLDriver-method
#' coerce,MySQLConnection,MySQLResult-method
#' coerce,MySQLConnection,MySQLDriver-method
#' coerce,MySQLResult,MySQLConnection-method
#' coerce,MySQLResult,MySQLDriver-method format,MySQLObject-method
#' print,MySQLObject-method show,MySQLObject-method summary,MySQLObject-method
#' summary,MySQLDriver-method summary,MySQLConnection-method
#' summary,MySQLResult-method
#' @docType methods
#' @section Methods: \describe{
#'
#' \item{object = "DBIObject"}{ Provides relevant metadata information on
#' \code{object}, for instance, the MySQL server file, the SQL statement
#' associated with a result set, etc.  } \item{from}{object to be coerced}
#' \item{to}{coercion class} \item{x}{object to \code{format} or \code{print}
#' or \code{show}} }
#' @keywords methods database interface
NULL


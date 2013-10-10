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

setMethod("show", "MySQLObject", def = function(object) print(object))

setMethod("print", "MySQLObject",
  def = function(x, ...){
    expired <- if(isIdCurrent(x)) "" else "Expired "
    str <- paste("<", expired, class(x), ":", format(x), ">", sep="")
    cat(str, "\n")
    invisible(NULL)
  }
)

## verify that obj refers to a currently open/loaded database
isIdCurrent <- function(obj)  { 
  obj <- as(obj, "integer")
  .Call("RS_DBI_validHandle", obj, PACKAGE = .MySQLPkgName)
}


##
## Class: dbDriver
##

"MySQL" <- 
function(max.con=16, fetch.default.rec = 500, force.reload=FALSE)
{
   mysqlInitDriver(max.con = max.con, fetch.default.rec = fetch.default.rec,
      force.reload = force.reload)
}

##
## Class: DBIDriver
##
setClass("MySQLDriver", representation("DBIDriver", "MySQLObject"))

## coerce (extract) any MySQLObject into a MySQLDriver
setAs("MySQLObject", "MySQLDriver", 
   def = function(from) new("MySQLDriver", Id = as(from, "integer")[1:2])
)

setMethod("dbUnloadDriver", "MySQLDriver",
   def = function(drv, ...) mysqlCloseDriver(drv, ...),
   valueClass = "logical"
)

setMethod("dbGetInfo", "MySQLDriver", 
   def = function(dbObj, ...) mysqlDriverInfo(dbObj, ...)
)

setMethod("dbListConnections", "MySQLDriver",
   def = function(drv, ...) dbGetInfo(drv, "connectionIds")[[1]]
)

setMethod("summary", "MySQLDriver", 
   def = function(object, ...) mysqlDescribeDriver(object, ...)
)

##
## Class: DBIConnection
##
setClass("MySQLConnection", representation("DBIConnection", "MySQLObject"))

setMethod("dbConnect", "MySQLDriver",
   def = function(drv, ...) mysqlNewConnection(drv, ...),
   valueClass = "MySQLConnection"
)

setMethod("dbConnect", "character",
   def = function(drv, ...) mysqlNewConnection(dbDriver(drv), ...),
   valueClass = "MySQLConnection"
)

## clone a connection
setMethod("dbConnect", "MySQLConnection",
   def = function(drv, ...) mysqlCloneConnection(drv, ...),
   valueClass = "MySQLConnection"
)

setMethod("dbDisconnect", "MySQLConnection",
   def = function(conn, ...) mysqlCloseConnection(conn, ...),
   valueClass = "logical"
)

setMethod("dbSendQuery", 
   signature(conn = "MySQLConnection", statement = "character"),
   def = function(conn, statement,...) mysqlExecStatement(conn, statement,...),
   valueClass = "MySQLResult"
)

setMethod("dbGetQuery", 
   signature(conn = "MySQLConnection", statement = "character"),
   def = function(conn, statement, ...) mysqlQuickSQL(conn, statement, ...)
)

setMethod("dbGetException", "MySQLConnection",
   def = function(conn, ...){
      if(!isIdCurrent(conn))
         stop(paste("expired", class(conn)))
      .Call("RS_MySQL_getException", as(conn, "integer"), 
            PACKAGE = .MySQLPkgName)
   },
   valueClass = "list"
)

setMethod("dbGetInfo", "MySQLConnection",
   def = function(dbObj, ...) mysqlConnectionInfo(dbObj, ...)
)

setMethod("dbListResults", "MySQLConnection",
   def = function(conn, ...) dbGetInfo(conn, "rsId")[[1]]
)

setMethod("summary", "MySQLConnection",
   def = function(object, ...) mysqlDescribeConnection(object, ...)
)

## convenience methods 
setMethod("dbListTables", "MySQLConnection",
   def = function(conn, ...){
      tbls <- dbGetQuery(conn, "show tables")
      if(length(tbls)>0) 
         tbls <- tbls[,1]
      else
         tbls <- character()
      tbls
   },
   valueClass = "character"
)

setMethod("dbReadTable", signature(conn="MySQLConnection", name="character"),
   def = function(conn, name, ...) mysqlReadTable(conn, name, ...),
   valueClass = "data.frame"
)

setMethod("dbWriteTable", 
   signature(conn="MySQLConnection", name="character", value="data.frame"),
   def = function(conn, name, value, ...){
      mysqlWriteTable(conn, name, value, ...)
   },
   valueClass = "logical"
)

## write table from filename (TODO: connections)
setMethod("dbWriteTable", 
   signature(conn="MySQLConnection", name="character", value="character"),
   def = function(conn, name, value, ...){
      mysqlImportFile(conn, name, value, ...)
   },
   valueClass = "logical"
)

setMethod("dbExistsTable", 
   signature(conn="MySQLConnection", name="character"),
   def = function(conn, name, ...){
      ## TODO: find out the appropriate query to the MySQL metadata
      avail <- dbListTables(conn)
      if(length(avail)==0) avail <- ""
      match(tolower(name), tolower(avail), nomatch=0)>0
   },
   valueClass = "logical"
)

setMethod("dbRemoveTable", 
   signature(conn="MySQLConnection", name="character"),
   def = function(conn, name, ...){
      if(dbExistsTable(conn, name)){
         rc <- try(dbGetQuery(conn, paste("DROP TABLE", name)))
         !inherits(rc, ErrorClass)
      } 
      else FALSE
   },
   valueClass = "logical"
)

## return field names (no metadata)
setMethod("dbListFields", 
   signature(conn="MySQLConnection", name="character"),
   def = function(conn, name, ...){
      flds <- dbGetQuery(conn, paste("describe", name))[,1]
      if(length(flds)==0)
         flds <- character()
      flds
   },
  valueClass = "character"
)

setMethod("dbCommit", "MySQLConnection",
   def = function(conn, ...) .NotYetImplemented()
)

setMethod("dbRollback", "MySQLConnection",
   def = function(conn, ...) .NotYetImplemented()
)

setMethod("dbCallProc", "MySQLConnection",
   def = function(conn, ...) .NotYetImplemented()
)

##
## Class: DBIResult
##
setClass("MySQLResult", representation("DBIResult", "MySQLObject"))

setAs("MySQLResult", "MySQLConnection",
   def = function(from) new("MySQLConnection", Id = as(from, "integer")[1:3])
)
setAs("MySQLResult", "MySQLDriver",
   def = function(from) new("MySQLDriver", Id = as(from, "integer")[1:2])
)

setMethod("dbClearResult", "MySQLResult", 
   def = function(res, ...) mysqlCloseResult(res, ...), 
   valueClass = "logical"
)

setMethod("fetch", signature(res="MySQLResult", n="numeric"),
   def = function(res, n, ...){ 
      out <- mysqlFetch(res, n, ...)
      if(is.null(out))
         out <- data.frame(out)
      out
   },
   valueClass = "data.frame"
)

setMethod("fetch", 
   signature(res="MySQLResult", n="missing"),
   def = function(res, n, ...){
      out <-  mysqlFetch(res, n=0, ...)
      if(is.null(out))
         out <- data.frame(out)
      out
   },
   valueClass = "data.frame"
)

setMethod("dbGetInfo", "MySQLResult",
   def = function(dbObj, ...) mysqlResultInfo(dbObj, ...),
   valueClass = "list"
)

setMethod("dbGetStatement", "MySQLResult",
   def = function(res, ...){
      st <-  dbGetInfo(res, "statement")[[1]]
      if(is.null(st))
         st <- character()
      st
   },
   valueClass = "character"
)

setMethod("dbListFields", 
   signature(conn="MySQLResult", name="missing"),
   def = function(conn, name, ...){
       flds <- dbGetInfo(conn, "fields")$fields$name
       if(is.null(flds))
          flds <- character()
       flds
   },
   valueClass = "character"
)

setMethod("dbColumnInfo", "MySQLResult", 
   def = function(res, ...) mysqlDescribeFields(res, ...),
   valueClass = "data.frame"
)

## NOTE: The following is experimental (as suggested by Greg Warnes)
setMethod("dbColumnInfo", "MySQLConnection",
   def = function(res, ...){
      dots <- list(...) 
      if(length(dots) == 0)
         stop("must specify one MySQL object (table) name")
      if(length(dots) > 1)
         warning("dbColumnInfo: only one MySQL object name (table) may be specified", call.=FALSE)
      dbGetQuery(res, paste("describe", dots[[1]]))
   },
   valueClass = "data.frame"
)
setMethod("dbGetRowsAffected", "MySQLResult",
   def = function(res, ...) dbGetInfo(res, "rowsAffected")[[1]],
   valueClass = "numeric"
)

setMethod("dbGetRowCount", "MySQLResult",
   def = function(res, ...) dbGetInfo(res, "rowCount")[[1]],
   valueClass = "numeric"
)

setMethod("dbHasCompleted", "MySQLResult",
   def = function(res, ...) dbGetInfo(res, "completed")[[1]] == 1,
   valueClass = "logical"
)

setMethod("dbGetException", "MySQLResult",
   def = function(conn, ...){
      id <- as(conn, "integer")[1:2]
      .Call("RS_MySQL_getException", id, PACKAGE = .MySQLPkgName)
   },
   valueClass = "list"    ## TODO: should be a DBIException?
)

setMethod("summary", "MySQLResult", 
   def = function(object, ...) mysqlDescribeResult(object, ...)
)

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

setGeneric("dbEscapeStrings", 
   def = function(con, strings, ...) standardGeneric("dbEscapeStrings"))
setMethod("dbEscapeStrings",
   sig = signature(con = "MySQLConnection", strings = "character"),
   def = mysqlEscapeStrings,
   valueClass = "character"
)
setMethod("dbEscapeStrings",
   sig = signature(con = "MySQLResult", strings = "character"),
   def = function(con, strings, ...) 
       mysqlEscapeStrings(as(con, "MySQLConnection"), strings),
   valueClass = "character"
)
  
setGeneric("dbApply", def = function(res, ...) standardGeneric("dbApply"))
setMethod("dbApply", "MySQLResult",
   def = function(res, ...)  mysqlDBApply(res, ...),
)

setGeneric("dbMoreResults",
   def = function(con, ...) standardGeneric("dbMoreResults"),
   valueClass = "logical"
)

setMethod("dbMoreResults", 
   signature(con = "MySQLConnection"),
   def = function(con, ...) 
      .Call("RS_MySQL_moreResultSets", as(con, "integer"), 
            PACKAGE=.MySQLPkgName)
)

setGeneric("dbNextResult",
   def = function(con, ...) standardGeneric("dbNextResult")
   #valueClass = "DBIResult" or NULL
)

setMethod("dbNextResult", 
   signature(con = "MySQLConnection"),
   def = function(con, ...){
       for(rs in dbListResults(con)){
           dbClearResult(rs)
       }
      id = .Call("RS_MySQL_nextResultSet", as(con, "integer"),
                 PACKAGE=.MySQLPkgName)
      new("MySQLResult", Id = id)
   }
)


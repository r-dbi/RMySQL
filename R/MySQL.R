## $Id$
##
## Copyright (C) 1999 The Omega Project for Statistical Computing.
##
## This library is free software; you can redistribute it and/or
## modify it under the terms of the GNU Lesser General Public
## License as published by the Free Software Foundation; either
## version 2 of the License, or (at your option) any later version.
##
## This library is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## Lesser General Public License for more details.
##
## You should have received a copy of the GNU Lesser General Public
## License along with this library; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA

## Class: dbManager

"NEW.MySQLObject" <- 
function(Id)
{
   out <- as.integer(Id)
   class(out) <- c("MySQLObject", "dbObjectId")
   out
}

"NEW.MySQLManager" <- 
function(Id)
{
   out <- list()
   attr(out, "Id") <- Id
   class(out) <- c("MySQLManager", "dbManger", "MySQLObject", "dbObjectId")
   out
}

"NEW.MySQLConnection" <- 
function(Id)
{
   out <- list()
   attr(out, "Id") <- Id
   class(out) <- c("MySQLConnection", "dbConnection", 
                    "MySQLObject", "dbObjectId")
   out
}

"NEW.MySQLResultSet" <- 
function(Id)
{
   out <- list()
   attr(out, "Id") <- Id
   class(out) <- c("MySQLResultSet", "dbResultSet", 
                    "MySQLObject", "dbObjectId")
   out
}

"as.integer.dbObjectId" <- 
function(x, ...)
{
   x <- attr(x, "Id")
   NextMethod("as.integer")
}

"as.MySQLManager" <- 
function(object)
{
   NEW("MySQLManger", Id = AS(object, "integer")[1])
}

"MySQL" <- 
"MySQLManager" <- 
function(max.con=10, fetch.default.rec = 500, force.reload=F)
## create a MySQL database connection manager.  By default we allow
## up to "max.con" connections and single fetches of up to "fetch.default.rec"
## records.  These settings may be changed by re-loading the driver
## using the "force.reload" = T flag (note that this will close all 
## currently open connections).
## Returns an object of class "MySQLManger".  
## Note: This class is a singleton.
{
   if(fetch.default.rec<=0)
      stop("default num of records per fetch must be positive")
   id <- load.MySQLManager(max.con, fetch.default.rec, force.reload)
   NEW("MySQLManager", Id = id)
}

"loadManager.MySQLManager" <- 
function(mgr, ...)
{
   load.MySQLManager(...)
}

## Class: dbConnection

"dbConnect.MySQLManager" <- 
function(mgr, ...)
{
   id <- newConnection.MySQLManager(mgr, ...)
   NEW("MySQLConnection",Id = id)
}

"dbConnect.MySQLConnection" <- 
function(mgr, ...)
{
   if(!isIdCurrent(mgr))
      stop("expired connection")
   con.id <- AS(mgr, "integer")
   con <- .Call("RS_MySQL_cloneConnection", con.id)
   NEW("MySQLConnection", Id = con)
}

"dbConnect.default" <- 
function(mgr, ...)
{
## dummy default (it only works for MySQL)  See the S4 for the
## real default method
   if(!is.character(mgr)) 
      stop("mgr must be a string with the driver name")
   id <- do.call(mgr, list())
   dbConnect(id, ...)
}

"getConnections.MySQLManager" <- 
function(mgr, ...)
{
   getInfo(mgr, what = "connectionIds")
}

"getManager.MySQLConnection" <- "getManager.MySQLResultSet" <-
function(obj, ...)
{
   AS(obj, "MySQLManager")
}

"quickSQL" <- 
function(con, statement, ...)
{
   UseMethod("quickSQL")
}

"getException.MySQLConnection" <- 
function(object)
{
   id <- as.integer(object)
   .Call("RS_MySQL_getException", id)
}

"dbExec.MySQLConnection" <- 
"dbExecStatement.MySQLConnection" <- 
function(con, statement, ...)
{
   if(!is.character(statement))
      stop("non character statement")
   execStatement.MySQLConnection(con, statement, ...)
}

## Class: resultSet

"getConnection.MySQLConnection" <-
"getConnection.MySQLResultSet" <- 
function(object)
{
   NEW("MySQLConnection", Id=AS(object, "integer")[1:2])
}

"getStatement.MySQLResultSet" <- 
function(object)
{
   getInfo.MySQLResultSet(object, "statement")
}

## ???

"getFields.MySQLResultSet" <- 
function(res)
{
   flds <- getInfo.MySQLResultSet(res, "fieldDescription")[[1]]
   flds$Sclass <- .Call("RS_DBI_SclassNames", flds$Sclass)
   flds$type <- .Call("RS_MySQL_typeNames", flds$type)
   structure(flds, row.names = paste(seq(along=flds$type)), class="data.frame")
}

"getRowsAffected.MySQLResultSet" <- 
function(object)
   getInfo.MySQLResultSet(object, "rowsAffected")

"getRowCount.MySQLResultSet" <- 
function(object)
{
  getInfo.MySQLResultSet(object, "rowCount")
}

"hasCompleted.MySQLResultSet" <- 
function(object)
{
   getInfo.MySQLResultSet(object, "completed")
}

"getException.MySQLResultSet" <- 
function(object)
{
   id <- as.integer(object)
   .Call("RS_MySQL_getException", id[1:2])
}

"getCurrentDatabase.MySQLConnection" <- 
function(object, ...)
{
   quickSQL(object, "select DATABASE()")
}

"getDatabases.MySQLConnection" <- 
function(obj, ...)
{
   quickSQL(obj, "show databases")
}

"getTables.MySQLConnection" <- 
function(object, dbname, ...)
{
   if (missing(dbname))
      quickSQL(object, "show tables")[,1]
   else quickSQL(object, paste("show tables from", dbname))[,1]
}

"getTableFields.MySQLResultSet" <- 
function(object, table, dbname, ...)
{
   getFields(object)
}

"getTableFields.MySQLConnection" <- 
function(object, table, dbname, ...)
{
   if(missing(dbname))
      cmd <- paste("show columns from", table)
   else 
      cmd <- paste("show columns from", table, "from", dbname)
   quickSQL(object, cmd)
}

"getTableIndices.MySQLConnection" <- 
function(obj, table, dbname, ...)
{
   if(missing(dbname))
      cmd <- paste("show index from", table)
   else 
      cmd <- paste("show index from", table, "from", dbname)
   quickSQL(obj, cmd)
}

"assignTable.MySQLConnection" <-
function(con, name, value, field.types, row.names = T, 
   overwrite=F, append=F, ...)
## TODO: This function should execute its sql as a single transaction,
## and allow converter functions.
## Create table "name" (must be an SQL identifier) and populate
## it with the values of the data.frame "value"
## BUG: In the unlikely event that value has a field called "row.names"
## we could inadvertently overwrite it (here the user should set row.names=F)
## (I'm reluctantly adding the code re: row.names -- I'm not 100% comfortable
## using data.frames as the basic data for relations.)
{
   if(overwrite && append)
      stop("overwrite and append cannot both be TRUE")
   if(!is.data.frame(value))
      value <- as.data.frame(value)
   if(row.names){
      value <- cbind(row.names(value), value)  ## can't use row.names= here
      names(value)[1] <- "row.names"
   }
   if(missing(field.types) || is.null(field.types)){
      ## the following mapping should be coming from some kind of table
      ## also, need to use converter functions (for dates, etc.)
      field.types <- sapply(value, SQLDataType, mgr = con)
   } 
   i <- match("row.names", names(field.types), nomatch=0)
   if(i>0) ## did we add a row.names value?  If so, it's a text field.
      field.types[i] <- SQLDataType(mgr=con, field.types$row.names)
   names(field.types) <- make.SQL.names(names(field.types), 
                             keywords = .MySQLKeywords,
                             allow.keywords=F)

   ## Do we need to clone the connection (ie., if it is in use)?
   if(length(getResultSets(con))!=0){ 
      NEW.con <- dbConnect(con)              ## there's pending work, so clone
      on.exit(close(NEW.con))
   } 
   else {
      NEW.con <- con
   }

   if(existsTable(con,name)){
      if(overwrite){
         if(!removeTable(con, name)){
         warning(paste("table", name, "couldn't be overwritten"))
         return(F)
         }
      }
      else if(!append){
         warning(paste("table",name,"exists in database: aborting assignTable"))
         return(F)
      }
   } 
   if(!existsTable(con,name)){      ## need to re-test table for existance 
      ## need to create a new (empty) table
      sql1 <- paste("create table ", name, "\n(\n\t", sep="")
      sql2 <- paste(paste(names(field.types), field.types), collapse=",\n\t",
                          sep="")
      sql3 <- "\n)\n"
      sql <- paste(sql1, sql2, sql3, sep="")
      rs <- try(dbExecStatement(NEW.con, sql))
      if(inherits(rs, ErrorClass)){
         warning("could not create table: aborting assignTable")
         return(F)
      } 
      else 
         close(rs)
   }

   ## TODO: here, we should query the MySQL to find out if it supports
   ## LOAD DATA thru pipes; if so, should open the pipe instead of a file.

   fn <- tempfile("rsdbi")
   if(usingR())
      write.table(value, file = fn, quote = F, sep="\t", 
                  na = .MySQL.NA.string, row.names=F, col.names=F, eol = '\n')
   else
      write.table(value, file = fn, quote.string = F, sep="\t", 
                  na = .MySQL.NA.string, dimnames.write=F, end.of.row = '\n')
   on.exit(unlink(fn), add = T)
   sql4 <- paste("LOAD DATA LOCAL INFILE '", fn, "'",
                  " INTO TABLE ", name, 
                  " LINES TERMINATED BY '\n' ", sep="")
   rs <- try(dbExecStatement(NEW.con, sql4))
   if(inherits(rs, ErrorClass)){
      warning("could not load data into table")
      return(F)
   } 
   else 
      close(rs)
   TRUE
}

"SQLDataType.MySQLConnection" <-
"SQLDataType.MySQLManager" <- 
function(obj, ...)
## find a suitable SQL data type for the R/S object obj
## TODO: Lots and lots!! (this is a very rough first draft)
## need to register converters, abstract out MySQL and generalize 
## to Oracle, Informix, etc.  Perhaps this should be table-driven.
## NOTE: MySQL data types differ from the SQL92 (e.g., varchar truncate
## trailing spaces).  MySQL enum() maps rather nicely to factors (with
## up to 65535 levels)
{
   rs.class <- data.class(obj)    ## this differs in R 1.4 from older vers
   rs.mode <- storage.mode(obj)
   if(rs.class=="numeric" || rs.class == "integer"){
      sql.type <- if(rs.mode=="integer") "bigint" else  "double"
   } 
   else {
      sql.type <- switch(rs.class,
                     character = "text",
                     logical = "tinyint",
                     factor = "text",      ## up to 65535 characters
                     ordered = "text",
                     "text")
   }
   sql.type
}

## Additional MySQL keywords that are not part of the SQL92 standard
## TODO: we're introducing some SQL92 keywords that  are *not* keywords 
## in MySQL strictly speaking. Need to delete those.

".MySQLKeywords" <- 
sort(c(.SQL92Keywords,
   "ACTION", "AFTER", "AGGREGATE", "AUTO_INCREMENT", "AVG_ROW_LENGTH",
   "BIGINT", "BINARY", "BLOB", "BOOL", "BOTH", 
   "CHANGE", "CHECKSUM", "COLUMNS", "COMMENT", "CROSS", 
   "DATA", "DATABASE", "DATABASES", "DATETIME", "DAY_HOUR", "DAY_MINUTE",
   "DAY_SECOND", "DAYOFMONTH", "DAYOFWEEK", "DAYOFYEAR", "DALAY_KEY_WRITE",
   "ENCLOSED", "ENUM", "ESCAPED", "EXPLAIN",
   "FIELDS", "FILE", "FLOAT4", "FLOAT8", "FLUSH", "FUNCTION",
   "GRANT", "GRANTS", "GROUP",
   "HEAP", "HIGH_PRIORITY", "HOSTS", "HOUR_MINUTE", "HOUR_SECOND", 
   "IDENDIFIED", "IF", "IGNORE", "INFILE", "INSERT_ID", "INT1", "INT2", 
   "INT3", "INT4", "INT8", "ISAM",
   "KEYS", "KILL",
   "LEADING", "LEFT", "LENGTH", "LIMIT", "LINES", "LOAD", "LOCK", "LOGS",
   "LONG", "LONGBLOB", "LONGTEXT", "LOW_PRIORITY", 
   "MAX_ROWS", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", 
   "MIN_ROWS", "MINUTE_SECOND", "MODIFY", "MONTHNAME", "MYISAM",
   "NATURAL", "NO",
   "ON", "OPTIMIZE", "OPIONALLY", "OUTFILE",
   "PACK_KEYS", "PASSWORD", "PROCESS", "PROCESSLIST",
   "REGEXP", "RELOAD", "RENAME", "REPLACE", "RESTRICT", "RETURNS", 
   "RLIKE", "ROW",
   "SHUTDOWN", "SONAME", "SQL_BIG_RESULT", "SQL_BIG_SELECTS",
   "SQL_BIG_TABLES", "SQL_LOG_OFF", "SQL_LOG_UPDATE", 
   "SQL_LOW_PRIORITY_UPDATES", "SQL_SELECT_LIMIT", "SQL_SMALL_RESULT", 
   "SQL_WARNINGS", "STARTING", "STATUS", "STRAIGHT_JOIN", "STRING", 
   "SQL_SMALL_RESULT",
   "TABLES", "TERMINATES", "TEXT", "TINYINT", "TINYTEXT", "TRAILING", "TYPE",
   "UNLOCK", "UNSIGNED", "USAGE", "USE", 
   "VARBINARY", "VARIABLES", 
   "ZEROFILL")
)


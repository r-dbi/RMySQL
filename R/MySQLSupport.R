##
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
## Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
##

"mysqlInitDriver" <- 
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
   config.params <- as.integer(c(max.con, fetch.default.rec))
   force <- as.logical(force.reload)
   drvId <- .Call("RS_MySQL_init", config.params, force, 
                  PACKAGE = .MySQLPkgName)
   new("MySQLDriver", Id = drvId)
}

"mysqlCloseDriver"<- 
function(drv, ...)
{
   if(!isIdCurrent(drv))
      return(TRUE)
   drvId <- as(drv, "integer")
   .Call("RS_MySQL_closeManager", drvId, PACKAGE = .MySQLPkgName)
}

"mysqlDescribeDriver" <-
function(obj, verbose = F, ...)
## Print out nicely a brief description of the connection Driver
{
   info <- dbGetInfo(obj)
   print(obj)
   cat("  Driver name: ", info$drvName, "\n")
   cat("  Max  connections:", info$length, "\n")
   cat("  Conn. processed:", info$counter, "\n")
   cat("  Default records per fetch:", info$"fetch_default_rec", "\n")
   if(verbose){
      cat("  DBI API version: ", dbGetDBIVersion(), "\n")
      cat("  MySQL client version: ", info$clientVersion, "\n")
   }
   cat("  Open connections:", info$"num_con", "\n")
   if(verbose && !is.null(info$connectionIds)){
      for(i in seq(along = info$connectionIds)){
         cat("   ", i, " ")
         print(info$connectionIds[[i]])
      }
   }
   invisible(NULL)
}

"mysqlDriverInfo" <-
function(obj, what="", ...)
{
   mgrId <- as(obj, "integer")[1]
   info <- .Call("RS_MySQL_managerInfo", mgrId, PACKAGE = .MySQLPkgName)  
   mgrId <- info$managerId
   ## replace mgr/connection id w. actual mgr/connection objects
   conObjs <- vector("list", length = info$"num_con")
   ids <- info$connectionIds
   for(i in seq(along = ids))
      conObjs[[i]] <- new("MySQLConnection", Id = c(mgrId, ids[i]))
   info$connectionIds <- conObjs
   info$managerId <- new("MySQLDriver", Id = mgrId)
   if(!missing(what))
      info[what]
   else
      info
}

"mysqlNewConnection" <-
## note that dbname may be a database name, an empty string "", or NULL.
## The distinction between "" and NULL is that "" is interpreted by 
## the MySQL API as the default database (MySQL config specific)
## while NULL means "no database".
function(mgr, dbname = "", username="",
   password="", host="",
   unix.socket = "", port = 0, client.flag = 0, 
   groups = NULL)
{
   if(!isIdCurrent(mgr))
      stop("expired manager")
   con.params <- as.character(c(username, password, host, 
                                dbname, unix.socket, port, 
                                client.flag))
   groups <- as.character(groups)
   mgrId <- as(mgr, "integer")
   conId <- .Call("RS_MySQL_newConnection", mgrId, con.params, groups, 
               PACKAGE = .MySQLPkgName)
   new("MySQLConnection", Id = conId)
}

"mysqlCloneConnection" <-
function(con, ...)
{
   if(!isIdCurrent(con))
      stop(paste("expired", class(con)))
   conId <- as(con, "integer")[1:2]
   newId <- .Call("RS_MySQL_cloneConnection", conId, PACKAGE = .MySQLPkgName)
   new("MySQLConnection", Id = newId)
}

"mysqlDescribeConnection" <-
function(obj, verbose = F, ...)
{
   info <- dbGetInfo(obj)
   print(obj)
   cat("  User:", info$user, "\n")
   cat("  Host:", info$host, "\n")
   cat("  Dbname:", info$dbname, "\n")
   cat("  Connection type:", info$conType, "\n")
   if(verbose){
      cat("  MySQL server version: ", info$serverVersion, "\n")
      cat("  MySQL client version: ", 
         dbGetInfo(as(obj, "MySQLDriver"), what="clientVersion")[[1]], "\n")
      cat("  MySQL protocol version: ", info$protocolVersion, "\n")
      cat("  MySQL server thread id: ", info$threadId, "\n")
   }
   if(length(info$rsId)>0){
      for(i in seq(along = info$rsId)){
         cat("   ", i, " ")
         print(info$rsId[[i]])
      }
   } else 
      cat("  No resultSet available\n")
   invisible(NULL)
}

"mysqlCloseConnection" <-
function(con, ...)
{
   if(!isIdCurrent(con))
      return(TRUE)
   rs <- dbListResults(con)
   if(length(rs)>0){
      if(dbHasCompleted(rs[[1]]))
         dbClearResult(rs[[1]])
      else
         stop("connection has pending rows (close open results set first)")
   }
   conId <- as(con, "integer")
   .Call("RS_MySQL_closeConnection", conId, PACKAGE = .MySQLPkgName)
}

"mysqlConnectionInfo" <-
function(obj, what="", ...)
{
   if(!isIdCurrent(obj))
      stop(paste("expired", class(obj), deparse(substitute(obj))))
   id <- as(obj, "integer")
   info <- .Call("RS_MySQL_connectionInfo", id, PACKAGE = .MySQLPkgName)
   rsId <- vector("list", length = length(info$rsId))
   for(i in seq(along = info$rsId))
       rsId[[i]] <- new("MySQLResult", Id = c(id, info$rsId[i]))
   info$rsId <- rsId
   if(!missing(what))
      info[what]
   else
      info
}
       
"mysqlExecStatement" <-
function(con, statement)
## submits the sql statement to MySQL and creates a
## dbResult object if the SQL operation does not produce
## output, otherwise it produces a resultSet that can
## be used for fetching rows.
{
   if(!isIdCurrent(con))
      stop(paste("expired", class(con)))
   conId <- as(con, "integer")
   statement <- as(statement, "character")
   rsId <- .Call("RS_MySQL_exec", conId, statement, PACKAGE = .MySQLPkgName)
   new("MySQLResult", Id = rsId)
}

## helper function: it exec's *and* retrieves a statement. It should
## be named somehting else.
"mysqlQuickSQL" <-
function(con, statement)
{
   if(!isIdCurrent(con))
      stop(paste("expired", class(con)))
   nr <- length(dbListResults(con))
   if(nr>0){                     ## are there resultSets pending on con?
      new.con <- dbConnect(con)   ## yep, create a clone connection
      on.exit(dbDisconnect(new.con))
      rs <- dbSendQuery(new.con, statement)
   } else rs <- dbSendQuery(con, statement)
   if(dbHasCompleted(rs)){
      dbClearResult(rs)            ## no records to fetch, we're done
      invisible()
      return(NULL)
   }
   res <- fetch(rs, n = -1)
   if(dbHasCompleted(rs))
      dbClearResult(rs)
   else 
      warning("pending rows")
   res
}

"mysqlDescribeFields" <-
function(res, ...)
{
   flds <- dbGetInfo(res, "fieldDescription")[[1]][[1]]
   if(!is.null(flds)){
      flds$Sclass <- .Call("RS_DBI_SclassNames", flds$Sclass, 
                        PACKAGE = .MySQLPkgName)
      flds$type <- .Call("RS_MySQL_typeNames", as.integer(flds$type), 
                        PACKAGE = .MySQLPkgName)
      ## no factors
      structure(flds, row.names = paste(seq(along=flds$type)),
                            class = "data.frame")
   }
   else data.frame(flds)
}

## Experimental dbApply (should it be seqApply?)
setGeneric("dbApply", def = function(rs, ...) standardGeneric("dbApply"))
setMethod("dbApply", "MySQLResult",
   def = function(rs, ...)  mysqlDBApply(rs, ...),
)

"mysqlDBApply" <-
function(rs, INDEX, FUN = stop("must specify FUN"), 
         begin = NULL, 
         group.begin =  NULL, 
         new.record = NULL, 
         end = NULL, 
         batchSize = 100, maxBatch = 1e6, 
         ..., simplify = T)
## (Experimental)
## This function is meant to handle somewhat gracefully(?) large amounts 
## of data from the DBMS by bringing into R manageable chunks (about 
## batchSize records at a time, but not more than maxBatch); the idea
## is that the data from individual groups can be handled by R, but
## not all the groups at the same time.  
##
## dbApply apply functions to groups of rows coming from a remote
## database resultSet upon the following fetching events: 
##   begin         (prior to fetching the first record)
##   group.begin   (the record just fetched begins a new group)
##   new_record    (a new record just fetched)
##   group.end     (the record just fetched ends the current group)
##   end           (the record just fetched is the very last record)
##
## The "begin", "begin.group", etc., specify R functions to be
## invoked upon the corresponding events.  (For compatibility 
## with other apply functions the arg FUN is used to specify the
## most common case where we only specify the "group.end" event.)
## 
## The following describes the exact order and form of invocation for the
## various callbacks in the underlying  C code.  All callback function 
## (except FUN) are optional.
##  begin()
##    group.begin(group.name)   
##    new.record(df.record)
##    FUN(df.group, group.name)   (aka group.end)
##  end()
##
## TODO: (1) add argument output=F/T to suppress the creation of
##           an expensive(?) output list.
##       (2) allow INDEX to be a list as in tapply()
##       (3) should we implement a simplify argument, as in sapply()?
##       (4) should report (instead of just warning) when we're forced
##           to handle partial groups (groups larger than maxBatch).
##       (5) extend to the case where even individual groups are too
##           big for R (as in incrementatl quantiles).
##       (6) Highly R-dependent, not sure yet how to port it to S-plus.
{
   if(dbHasCompleted(rs))
      stop("result set has completed")
   if(is.character(INDEX)){
      flds <- tolower(as.character(dbColumnInfo(rs)$name))
      INDEX <- match(tolower(INDEX[1]), flds, 0)
   }
   if(INDEX<1)
      stop(paste("INDEX field", INDEX, "not in result set"))

   "null.or.fun" <- function(fun) # get fun obj, but a NULL is ok 
   {
      if(is.null(fun)) 
         fun 
      else 
         match.fun(fun)
   }
   begin <- null.or.fun(begin)
   group.begin <- null.or.fun(group.begin)
   group.end <- null.or.fun(FUN)     ## probably this is the most important
   end <- null.or.fun(end)
   new.record <- null.or.fun(new.record)
   rsId <- as(rs, "integer")
   con <- dbGetConnection(rs)
   on.exit({
      rc <- dbGetException(con)
      if(!is.null(rc$errorNum) && rc$errorNum!=0)
         cat("dbApply aborted with MySQL error ", rc$errorNum,
             " (", rc$errorMsg, ")\n", sep = "")

      })
   ## BEGIN event handler (re-entrant, only prior to reading first row)
   if(!is.null(begin) && dbGetRowCount(rs)==0) 
      begin()
   rho <- environment()
   funs <- list(begin = begin, end = end,
                group.begin = group.begin,
                group.end = group.end, new.record = new.record)
   out <- .Call("RS_MySQL_dbApply",
	        rs = rsId,
		INDEX = as.integer(INDEX-1),
		funs, rho, as.integer(batchSize), as.integer(maxBatch),
                PACKAGE = .MySQLPkgName)
   if(!is.null(end) && dbHasCompleted(rs))
      end()
   out
}

"mysqlFetch" <-
function(res, n=0, ...)
## Fetch at most n records from the opened resultSet (n = -1 means
## all records, n=0 means extract as many as "default_fetch_rec",
## as defined by MySQLDriver (see describe(mgr, T)).
## The returned object is a data.frame. 
## Note: The method dbHasCompleted() on the resultSet tells you whether
## or not there are pending records to be fetched. 
## 
## TODO: Make sure we don't exhaust all the memory, or generate
## an object whose size exceeds option("object.size").  Also,
## are we sure we want to return a data.frame?
{    
   n <- as(n, "integer")
   rsId <- as(res, "integer")
   rel <- .Call("RS_MySQL_fetch", rsId, nrec = n, PACKAGE = .MySQLPkgName)
   if(length(rel)==0 || length(rel[[1]])==0) 
      return(NULL)
   ## create running row index as of previous fetch (if any)
   cnt <- dbGetRowCount(res)
   nrec <- length(rel[[1]])
   indx <- seq(from = cnt - nrec + 1, length = nrec)
   attr(rel, "row.names") <- as.character(indx)
   if(usingR())
      class(rel) <- "data.frame"
   else
      oldClass(rel) <- "data.frame"
   rel
}

## Note that originally we had only resultSet both for SELECTs
## and INSERTS, ...  Later on we created a base class dbResult
## for non-Select SQL and a derived class resultSet for SELECTS.

"mysqlResultInfo" <-
function(obj, what = "", ...)
{
   if(!isIdCurrent(obj))
      stop(paste("expired", class(obj), deparse(substitute(obj))))
   id <- as(obj, "integer")
   info <- .Call("RS_MySQL_resultSetInfo", id, PACKAGE = .MySQLPkgName)
   if(!missing(what))
      info[what]
   else
      info
}

"mysqlDescribeResult" <-
function(obj, verbose = F, ...)
{

   if(!isIdCurrent(obj)){
      print(obj)
      invisible(return(NULL))
   }
   print(obj)
   cat("  Statement:", dbGetStatement(obj), "\n")
   cat("  Has completed?", if(dbHasCompleted(obj)) "yes" else "no", "\n")
   cat("  Affected rows:", dbGetRowsAffected(obj), "\n")
   cat("  Rows fetched:", dbGetRowCount(obj), "\n")
   flds <- dbColumnInfo(obj)
   if(verbose && !is.null(flds)){
      cat("  Fields:\n")  
      out <- print(dbColumnInfo(obj))
   }
   invisible(NULL)
}

"mysqlCloseResult" <-
function(res, ...)
{
   if(!isIdCurrent(res))
      return(TRUE)
   rsId <- as(res, "integer")
   .Call("RS_MySQL_closeResultSet", rsId, PACKAGE = .MySQLPkgName)
}

"mysqlReadTable" <- 
function(con, name, row.names = "row.names", check.names = T, ...)
## Use NULL, "", or 0 as row.names to prevent using any field as row.names.
{
   out <- dbGetQuery(con, paste("SELECT * from", name))
   if(check.names)
       names(out) <- make.names(names(out), unique = T)
   ## should we set the row.names of the output data.frame?
   nms <- names(out)
   j <- switch(mode(row.names),
           "character" = if(row.names=="") 0 else
               match(tolower(row.names), tolower(nms), 
                     nomatch = if(missing(row.names)) 0 else -1),
           "numeric" = row.names,
           "NULL" = 0,
           0)
   if(j==0) 
      return(out)
   if(j<0 || j>ncol(out)){
      warning("row.names not set on output data.frame (non-existing field)")
      return(out)
   }
   rnms <- as.character(out[,j])
   if(all(!duplicated(rnms))){
      out <- out[,-j, drop = F]
      row.names(out) <- rnms
   } else warning("row.names not set on output (duplicate elements in field)")
   out
} 

"mysqlWriteTable" <-
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
   if(length(dbListResults(con))!=0){ 
      new.con <- dbConnect(con)              ## there's pending work, so clone
      on.exit(dbDisconnect(new.con))
   } 
   else {
      new.con <- con
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
      rs <- try(dbSendQuery(new.con, sql))
      if(inherits(rs, ErrorClass)){
         warning("could not create table: aborting assignTable")
         return(F)
      } 
      else 
         dbClearResult(rs)
   }

   ## TODO: here, we should query the MySQL to find out if it supports
   ## LOAD DATA thru pipes; if so, should open the pipe instead of a file.

   fn <- tempfile("rsdbi")
   fn <- gsub("\\\\", "/", fn)  # Since MySQL on Windows wants \ double (BDR)
   safe.write(value, file = fn)
   on.exit(unlink(fn), add = T)
   sql4 <- paste("LOAD DATA LOCAL INFILE '", fn, "'",
                  " INTO TABLE ", name, 
                  " LINES TERMINATED BY '\n' ", sep="")
   rs <- try(dbSendQuery(new.con, sql4))
   if(inherits(rs, ErrorClass)){
      warning("could not load data into table")
      return(F)
   } 
   else 
      dbClearResult(rs)
   TRUE
}

## the following is almost exactly from the ROracle driver 
"safe.write" <- 
function(value, file, batch, ...)
## safe.write makes sure write.table don't exceed available memory by batching
## at most batch rows (but it is still slowww)
{  
   N <- nrow(value)
   if(N<1){
      warning("no rows in data.frame")
      return(NULL)
   }
   if(missing(batch) || is.null(batch))
      batch <- 10000
   else if(batch<=0) 
      batch <- N
   from <- 1 
   to <- min(batch, N)
   while(from<=N){
      if(usingR())
         write.table(value[from:to, drop=FALSE], file = file, append = TRUE, 
               quote = FALSE, sep="\t", na = .MySQL.NA.string,
               row.names=FALSE, col.names=FALSE, eol = '\n', ...)
      else
         write.table(value[from:to, drop=FALSE], file = file, append = TRUE, 
               quote.string = FALSE, sep="\t", na = .MySQL.NA.string,
               dimnames.write=FALSE, end.of.row = '\n', ...)
      from <- to+1
      to <- min(to+batch, N)
   }
   invisible(NULL)
}

"mysqlDataType" <-
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


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

## RS-MySQL Support functions.  These functions are named
## following S3/R convention "<method>.<class>" (e.g., close.MySQLconnection)
## to allow easy porting to R and S3(?).  Also, we tried to minimize
## S4 specific construct as much as possible (there're still a few
## S4 idioms left, but few).
##

.MySQL.NA.string <- "\\N"  ## on input, MySQL interprets \N as NULL (NA)

if(!usingV4()){

   "format.MySQLManager" <- 
   "format.MySQLConnection" <-
   "format.MySQLResultSet" <- 
   function(x, ...)
   {
      format.dbObjectId(x)
   }

   "print.MySQLManager" <- 
   "print.MySQLConnection" <-
   "print.MySQLResultSet" <- 
   function(x, ...)
   {
      print.dbObjectId(x, ...)
   }

   "as.integer.MySQLManager" <- 
   "as.integer.MySQLConnection" <-
   "as.integer.MySQLResultSet" <- 
   function(x, ...)
   {
      as.integer(attr(x,"Id"))
   }

   "as.MySQLManager" <- 
   function(obj)
   {
      new("MySQLManager", Id =as(obj,"integer")[1])
   }

   "as.MySQLConnection" <- 
   function(obj)
   {
      new("MySQLConnection", Id = as(obj, "integer")[1:2])
   } 

   "as.MySQLResultSet" <- 
   function(obj)
   {
      new("MySQLResultSet", Id = as(obj, "integer")[1:3])
   }

   ## these, again, are needed only because virtual classes (dbObject)
   ## are not available in R prior to 1.4

   "getTable.MySQLConnection" <- 
   function(con, name, ...)
   {
      getTable.dbConnection(con, name, ...)
   }

   "existsTable.MySQLConnection" <- 
   function(con, name, ...)
   {
      existsTable.dbConnection(con, name, ...)
   }

   "removeTable.MySQLConnection" <- 
   function(con, name, ...)
   {
      removeTable.dbConnection(con, name, ...)
   }
}

"load.MySQLManager" <- 
function(max.con = 16, fetch.default.rec = 500, force.reload=F)
## return a manager id
{
   config.params <- as.integer(c(max.con, fetch.default.rec))
   force <- as.logical(force.reload)
   .Call("RS_MySQL_init", config.params, force, PACKAGE = "RMySQL")
}

"describe.MySQLManager" <-
function(obj, verbose = F, ...)
## Print out nicely a brief description of the connection Manager
{
   info <- getInfo.MySQLManager(obj)
   show(obj)
   cat("  Driver name: ", info$drvName, "\n")
   cat("  Max  connections:", info$length, "\n")
   cat("  Conn. processed:", info$counter, "\n")
   cat("  Default records per fetch:", info$"fetch_default_rec", "\n")
   if(verbose){
      cat("  MySQL client version: ", info$clientVersion, "\n")
      cat("  RS-DBI version: ", "0.2", "\n")
   }
   cat("  Open connections:", info$"num_con", "\n")
   if(verbose && !is.null(info$connectionIds)){
      for(i in seq(along = info$connectionIds)){
         cat("   ", i, " ")
         show(info$connectionIds[[i]])
      }
   }
   invisible(NULL)
}

"unload.MySQLManager"<- 
function(mgr, ...)
{
   if(!isIdCurrent(mgr))
      return(TRUE)
   mgrId <- as(mgr, "integer")
   .Call("RS_MySQL_closeManager", mgrId, PACKAGE = "RMySQL")
}

"getInfo.MySQLManager" <- 
function(obj, what="", ...)
{
   mgrId <- as(obj, "integer")[1]
   info <- .Call("RS_MySQL_managerInfo", mgrId, PACKAGE = "RMySQL")  
   mgrId <- info$managerId
   ## replace mgr/connection id w. actual mgr/connection objects
   conObjs <- vector("list", length = info$"num_con")
   ids <- info$connectionIds
   for(i in seq(along = ids))
      conObjs[[i]] <- new("MySQLConnection", Id = c(mgrId, ids[i]))
   info$connectionIds <- conObjs
   info$managerId <- new("MySQLManager", Id = mgrId)
   if(length(what)==1 && what=="")
      return(info)
   info <- info[what]
   if(length(info)==1)
      info[[1]]
   else
      info
}

"getVersion.MySQLManager" <- 
function(mgr)
{
   ## TODO: the DBI version number should be coming from the manager obj
   ## TODO: should also report the R/S MySQL driver version 
   list("DBI" = "0.2",
        "MySQL (client) library" = getInfo(mgr, what="clientVersion"))
}

"getVersion.MySQLConnection" <-
function(con)
{
   getInfo(con, what = c("serverVersion", "protocolVersion"))
}

## note that dbname may be a database name, an empty string "", or NULL.
## The distinction between "" and NULL is that "" is interpreted by 
## the MySQL API as the default database (MySQL config specific)
## while NULL means "no database".

"newConnection.MySQLManager"<- 
function(mgr, dbname = "", username="",
   password="", host="",
   unix.socket = "", port = 0, client.flag = 0, 
   groups = NULL)
{
   con.params <- as.character(c(username, password, host, 
                                dbname, unix.socket, port, 
                                client.flag))
   groups <- as.character(groups)
   mgrId <- as(mgr, "integer")
   .Call("RS_MySQL_newConnection", mgrId, con.params, groups, 
         PACKAGE = "RMySQL")
}

## functions/methods not implementable

"commit.MySQLConnection" <- 
"rollback.MySQLConnection" <- 
function(con) 
{
   warning("MySQL does not support transactions")
}

"describe.MySQLConnection" <- 
function(obj, verbose = F, ...)
{
   info <- getInfo(obj)
   show(obj)
   cat("  User:", info$user, "\n")
   cat("  Host:", info$host, "\n")
   cat("  Dbname:", info$dbname, "\n")
   cat("  Connection type:", info$conType, "\n")
   if(verbose){
      cat("  MySQL server version: ", info$serverVersion, "\n")
      cat("  MySQL client version: ", 
         getInfo(as(obj, "MySQLManager"),what="clientVersion"), "\n")
      cat("  MySQL protocol version: ", info$protocolVersion, "\n")
      cat("  MySQL server thread id: ", info$threadId, "\n")
   }
   if(length(info$rsId)>0){
      for(i in seq(along = info$rsId)){
         cat("   ", i, " ")
         show(info$rsId[[i]])
      }
   } else 
      cat("  No resultSet available\n")
   invisible(NULL)
}

"close.MySQLConnection" <- 
function(con, ...)
{
   if(!isIdCurrent(con))
      return(TRUE)
   conId <- as(con, "integer")
   .Call("RS_MySQL_closeConnection", conId, PACKAGE = "RMySQL")
}

"getInfo.MySQLConnection" <-
function(obj, what="", ...)
{
   if(!isIdCurrent(obj))
      stop(paste("expired", class(obj), deparse(substitute(obj))))
   id <- as(obj, "integer")
   info <- .Call("RS_MySQL_connectionInfo", id, PACKAGE = "RMySQL")
   if(length(info$rsId)){
      rsId <- vector("list", length = length(info$rsId))
      for(i in seq(along = info$rsId))
         rsId[[i]] <- new("MySQLResultSet", Id = c(id, info$rsId[i]))
      info$rsId <- rsId
   }
   else
      info$rsId <- NULL
   if(length(what)==1 && what=="")
      return(info)
   info <- info[what]
   if(length(info)==1)
      info[[1]]
   else
      info
}
       
"execStatement.MySQLConnection" <- 
function(con, statement)
## submits the sql statement to MySQL and creates a
## dbResult object if the SQL operation does not produce
## output, otherwise it produces a resultSet that can
## be used for fetching rows.
{
   conId <- as(con, "integer")
   statement <- as(statement, "character")
   rsId <- .Call("RS_MySQL_exec", conId, statement, PACKAGE = "RMySQL")
   #out <- new("MySQLdbResult", Id = rsId)
   #if(getInfo(out, what="isSelect")
   #   out <- new("MySQLResultSet", Id = rsId)
   #out
   out <- new("MySQLResultSet", Id = rsId)
   out
}

"getResultSets.MySQLConnection" <-
function(con)
{
   getInfo(con, what = "rsId")
}
## helper function: it exec's *and* retrieves a statement. It should
## be named somehting else.
quickSQL.MySQLConnection <- function(con, statement)
{
   nr <- length(getResultSets(con))
   if(nr>0){                     ## are there resultSets pending on con?
      new.con <- dbConnect(con)   ## yep, create a clone connection
      on.exit(close(new.con))
      rs <- dbExecStatement(new.con, statement)
   } else rs <- dbExecStatement(con, statement)
   if(hasCompleted(rs)){
      close(rs)            ## no records to fetch, we're done
      invisible()
      return(NULL)
   }
   res <- fetch(rs, n = -1)
   if(hasCompleted(rs))
      close(rs)
   else 
      warning("pending rows")
   res
}

## Experimental dbApply (should it be seqApply?)
dbApply <- function(rs, ...)
{
   UseMethod("dbApply")
}

"dbApply.MySQLResultSet" <- 
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
   if(hasCompleted(rs))
      stop("result set has completed")
   if(is.character(INDEX)){
      flds <- tolower(as.character(getFields(rs)$name))
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
   con <- getConnection(rs)
   on.exit({
      rc <- getException(con)
      if(!is.null(rc$errorNum) && rc$errorNum!=0)
         cat("dbApply aborted with MySQL error ", rc$errorNum,
             " (", rc$errorMsg, ")\n", sep = "")

      })
   ## BEGIN event handler (re-entrant, only prior to reading first row)
   if(!is.null(begin) && getRowCount(rs)==0) 
      begin()
   rho <- environment()
   funs <- list(begin = begin, end = end,
                group.begin = group.begin,
                group.end = group.end, new.record = new.record)
   out <- .Call("RS_MySQL_dbApply",
	        rs = rsId,
		INDEX = as.integer(INDEX-1),
		funs, rho, as.integer(batchSize), as.integer(maxBatch),
                PACKAGE = "RMySQL")
   if(!is.null(end) && hasCompleted(rs))
      end()
   out
}

"fetch.MySQLResultSet" <- 
function(res, n=0)   
## Fetch at most n records from the opened resultSet (n = -1 means
## all records, n=0 means extract as many as "default_fetch_rec",
## as defined by MySQLManager (see describe(mgr, T)).
## The returned object is a data.frame. 
## Note: The method hasCompleted() on the resultSet tells you whether
## or not there are pending records to be fetched. See also the methods
## describe(), getFieldDescrition(), getRowCount(), getAffectedRows(),
## getDBConnection(), getException().
## 
## TODO: Make sure we don't exhaust all the memory, or generate
## an object whose size exceeds option("object.size").  Also,
## are we sure we want to return a data.frame?
{    
   n <- as(n, "integer")
   rsId <- as(res, "integer")
   rel <- .Call("RS_MySQL_fetch", rsId, nrec = n, PACKAGE = "RMySQL")
   if(length(rel)==0 || length(rel[[1]])==0) 
      return(NULL)
   ## create running row index as of previous fetch (if any)
   cnt <- getRowCount(res)
   nrec <- length(rel[[1]])
   indx <- seq(from = cnt - nrec + 1, length = nrec)
   attr(rel, "row.names") <- as.character(indx)
   oldClass(rel) <- "data.frame"
   rel
}

## Note that originally we had only resultSet both for SELECTs
## and INSERTS, ...  Later on we created a base class dbResult
## for non-Select SQL and a derived class resultSet for SELECTS.

"getInfo.MySQLResultSet" <- 
#"getInfo.MySQLdbResult" <- 
function(obj, what = "", ...)
{
   if(!isIdCurrent(obj))
      stop(paste("expired", class(obj), deparse(substitute(obj))))
   id <- as(obj, "integer")
   info <- .Call("RS_MySQL_resultSetInfo", id, PACKAGE = "RMySQL")
   if(length(what)==1 && what=="")
      return(info)
   info <- info[what]
   if(length(info)==1)
      info[[1]]
   else
      info
}

if(FALSE){
   ##"describe.MySQLResultSet" <- 
   "describe.MySQLdbResult" <- 
   function(obj, verbose = F, ...)
   {
      if(!isIdCurrent(obj)){
         show(obj)
         invisible(return(NULL))
      }
      show(obj)
      cat("  Statement:", getStatement(obj), "\n")
      cat("  Has completed?", if(hasCompleted(obj)) "yes" else "no", "\n")
      cat("  Affected rows:", getRowsAffected(obj), "\n")
      invisible(NULL)
   }
}

"describe.MySQLResultSet" <- 
function(obj, verbose = F, ...)
{

   if(!isIdCurrent(obj)){
      show(obj)
      invisible(return(NULL))
   }
   show(obj)
   cat("  Statement:", getStatement(obj), "\n")
   cat("  Has completed?", if(hasCompleted(obj)) "yes" else "no", "\n")
   cat("  Affected rows:", getRowsAffected(obj), "\n")
   cat("  Rows fetched:", getRowCount(obj), "\n")
   flds <- getFields(obj)
   if(verbose && !is.null(flds)){
      cat("  Fields:\n")  
      out <- print(getFields(obj))
   }
   invisible(NULL)
}

"close.MySQLResultSet" <- 
function(con, ...)
{
   if(!isIdCurrent(con))
      return(TRUE)
   rsId <- as(con, "integer")
   .Call("RS_MySQL_closeResultSet", rsId, PACKAGE = "RMySQL")
}


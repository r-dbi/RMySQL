## Class: dbObjectId
##
## This helper class is *not* part of the database interface definition,
## but it is extended by the Oracle, MySQL, and SQLite implementations to
## MySQLObject and OracleObject to allow us to conviniently implement 
## all database foreign objects methods (i.e., methods for show(), 
## print() format() the dbManger, dbConnection, dbResultSet, etc.) 
## A dbObjectId is an  identifier into an actual remote database objects.  
## This class and its derived classes <driver-manager>Object need to 
## be VIRTUAL to avoid coercion (green book, p.293) during method dispatching.

NEW.dbObjectId <- function(Id, ...)
{
   NEW("dbObjectId", Id = Id)
}

## Coercion: the trick as(dbObject, "integer") is very useful
"as.integer.dbObjectId" <- 
function(x, ...) 
{
   as.integer(attr(x,"Id")) 
}

"as.numeric.dbObjectId" <- 
function(x, ...) 
{
   as.numeric(attr(x,"Id")) 
}

"as.character.dbObjectId" <- 
function(x, ...) 
{
   as.character(attr(x,"Id")) 
}

"isIdCurrent" <- 
function(obj)
## verify that obj refers to a currently open/loaded database
{ 
   obj <- as.integer(obj)
   .Call("RS_DBI_validHandle", obj)
}

"format.dbObjectId" <- 
function(x, ...)
{
   id <- as.integer(x)
   paste("(", paste(id, collapse=","), ")", sep="")
}

"print.dbObjectId" <- 
function(x, ...)
{
   if(isIdCurrent(x))
      str <- paste("<", class(x)[1], ":", format(x), ">", sep="")
   else 
      str <- paste("<Expired ",class(x)[1], ":", format(x), ">", sep="")
   cat(str, "\n")
   invisible(NULL)
}

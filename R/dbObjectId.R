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

if(usingV4()){
   setClass("dbObjectId", representation(Id = "integer", "VIRTUAL"))
   ## coercion methods 
   setAs("dbObjectId", "integer", 
      def = function(from) as(slot(from,"Id"), "integer")
   )
   setAs("dbObjectId", "numeric",
      def = function(from) as(slot(from, "Id"), "integer")
   )
   setAs("dbObjectId", "character",
      def = function(from) as(slot(from, "Id"), "character")
   )   
   ## formating, showing, printing,...
   setMethod("format", "dbObjectId", 
      def = function(x, ...) format.dbObjectId(x, ...),
      valueClass = "character"
   )
   setMethod("print", "dbObjectId",
      def = function(x, ...) print.dbObjectId(x, ...)
   )
   setMethod("show", "dbObjectId",
      def = function(object) print.dbObjectId(object)
   )
} else {
   new.dbObjectId <- function(Id, ...)
   {
      new("dbObjectId", Id = Id)
   }
   ## Coercion: the trick as(dbObject, "integer") is very useful
   "as.integer.dbObjectId" <- 
   function(x, ...) as.integer(attr(x,"Id")) 
   "as.numeric.dbObjectId" <- 
   function(x, ...) as.numeric(attr(x,"Id")) 
   "as.character.dbObjectId" <- 
   function(x, ...) as.character(attr(x,"Id")) 
}

"isIdCurrent" <- 
function(obj)
## verify that obj refers to a currently open/loaded database
{ 
   obj <- as(obj, "integer")
   .Call("RS_DBI_validHandle", obj)
}

"format.dbObjectId" <- 
function(x, ...)
{
   id <- as(x, "integer")
   paste("(", paste(id, collapse=","), ")", sep="")
}

"print.dbObjectId" <- 
function(x, ...)
{
   if(isIdCurrent(x))
      str <- paste("<", class(x), ":", format(x), ">", sep="")
   else 
      str <- paste("<Expired ",class(x), ":", format(x), ">", sep="")
   cat(str, "\n")
   invisible(NULL)
}

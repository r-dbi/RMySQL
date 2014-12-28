
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

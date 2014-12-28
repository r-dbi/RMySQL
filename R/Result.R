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

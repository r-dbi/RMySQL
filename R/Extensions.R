
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

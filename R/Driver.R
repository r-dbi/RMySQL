setClass("MySQLDriver", representation("DBIDriver", "MySQLObject"))


"MySQL" <- function(max.con=16, fetch.default.rec = 500, force.reload=FALSE) {
  mysqlInitDriver(max.con = max.con, fetch.default.rec = fetch.default.rec,
    force.reload = force.reload)
}

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

setClass("MySQLDriver", representation("DBIDriver", "MySQLObject"))

## create a MySQL database connection manager.  By default we allow
## up to "max.con" connections and single fetches of up to "fetch.default.rec"
## records.  These settings may be changed by re-loading the driver
## using the "force.reload" = T flag (note that this will close all
## currently open connections).
## Returns an object of class "MySQLManger".
## Note: This class is a singleton.
MySQL <- function(max.con=16, fetch.default.rec = 500, force.reload=FALSE) {
  if(fetch.default.rec<=0)
    stop("default num of records per fetch must be positive")
  config.params <- as(c(max.con, fetch.default.rec), "integer")
  force <- as.logical(force.reload)
  drvId <- .Call("RS_MySQL_init", config.params, force,
    PACKAGE = .MySQLPkgName)
  new("MySQLDriver", Id = drvId)
}

## coerce (extract) any MySQLObject into a MySQLDriver
setAs("MySQLObject", "MySQLDriver",
  def = function(from) new("MySQLDriver", Id = as(from, "integer")[1:2])
)

setMethod("dbUnloadDriver", "MySQLDriver",
  function(drv, ...) {
    if(!isIdCurrent(drv))
      return(TRUE)
    drvId <- as(drv, "integer")
    .Call("RS_MySQL_closeManager", drvId, PACKAGE = .MySQLPkgName)
  }
)


setMethod("dbGetInfo", "MySQLDriver",
  function(dbObj, what="", ...)
  {
    if(!isIdCurrent(dbObj))
      stop(paste("expired", class(dbObj)))
    drvId <- as(dbObj)
    info <- .Call("RS_MySQL_managerInfo", drvId, PACKAGE = .MySQLPkgName)
    ## replace drv/connection id w. actual drv/connection objects
    conObjs <- vector("list", length = info$"num_con")
    ids <- info$connectionIds
    for(i in seq(along = ids))
      conObjs[[i]] <- new("MySQLConnection", Id = c(drvId, ids[i]))
    info$connectionIds <- conObjs
    info$managerId <- new("MySQLDriver", Id = drvId)
    if(!missing(what))
      info[what]
    else
      info
  }
)

setMethod("dbListConnections", "MySQLDriver",
  function(drv, ...) dbGetInfo(drv, "connectionIds")[[1]]
)

setMethod("summary", "MySQLDriver",
  function(object, verbose = FALSE, ...) {
    info <- dbGetInfo(object)
    print(object)
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
)

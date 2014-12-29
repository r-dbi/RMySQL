setClass("MySQLResult", representation("DBIResult", "MySQLObject"))

setAs("MySQLResult", "MySQLConnection",
  def = function(from) new("MySQLConnection", Id = as(from, "integer")[1:3])
)
setAs("MySQLResult", "MySQLDriver",
  def = function(from) new("MySQLDriver", Id = as(from, "integer")[1:2])
)

setMethod("dbClearResult", "MySQLResult",
  function(res, ...) {
    if(!isIdCurrent(res))
      return(TRUE)
    rsId <- as(res, "integer")
    .Call("RS_MySQL_closeResultSet", rsId, PACKAGE = .MySQLPkgName)
  }
)

setMethod("fetch", signature(res="MySQLResult", n="numeric"),
  def = function(res, n, ...){
    n <- as(n, "integer")
    rsId <- as(res, "integer")
    rel <- .Call("RS_MySQL_fetch", rsId, nrec = n, PACKAGE = .MySQLPkgName)
    if(length(rel)==0 || length(rel[[1]])==0)
      return(data.frame())
    ## create running row index as of previous fetch (if any)
    cnt <- dbGetRowCount(res)
    nrec <- length(rel[[1]])
    indx <- seq(from = cnt - nrec + 1, length = nrec)
    attr(rel, "row.names") <- as.integer(indx)
    class(rel) <- "data.frame"
  }
)

setMethod("fetch",
  signature(res="MySQLResult", n="missing"),
  function(res, n, ...) fetch(res, n = 0, ...)
)

setMethod("dbGetInfo", "MySQLResult",
  function(dbObj, what = "", ...) {
    if(!isIdCurrent(dbObj))
      stop(paste("expired", class(dbObj), deparse(substitute(dbObj))))
    id <- as(dbObj, "integer")
    info <- .Call("RS_MySQL_resultSetInfo", id, PACKAGE = .MySQLPkgName)
    if(!missing(what))
      info[what]
    else
      info
  }
)

setMethod("dbGetStatement", "MySQLResult",
  def = function(res, ...){
    st <- dbGetInfo(res, "statement")[[1]]
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
  function(res, ...) {
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
  function(object, verbose = FALSE, ...) {

    if(!isIdCurrent(object)){
      print(object)
      invisible(return(NULL))
    }
    print(object)
    cat("  Statement:", dbGetStatement(object), "\n")
    cat("  Has completed?", if(dbHasCompleted(object)) "yes" else "no", "\n")
    cat("  Affected rows:", dbGetRowsAffected(object), "\n")
    cat("  Rows fetched:", dbGetRowCount(object), "\n")
    flds <- dbColumnInfo(object)
    if(verbose && !is.null(flds)){
      cat("  Fields:\n")
      out <- print(dbColumnInfo(object))
    }
    invisible(NULL)
  }
)

.clearResultSets <- function(con){
  ## are there resultSets pending on con?
  rsList <- dbListResults(con)
  if(length(rsList)>0){
    warning("There are pending result sets. Removing.",call.=FALSE)
    lapply(rsList,dbClearResult) ## clear all pending results
  }
  NULL
}

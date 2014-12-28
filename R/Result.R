#' Class MySQLResult
#'
#' MySQL's query results class.  This classes encapsulates the result of an SQL
#' statement (either \code{select} or not).
#'
#'
#' @name MySQLResult-class
#' @docType class
#' @section Generators: The main generator is \code{\link[DBI]{dbSendQuery}}.
#' @seealso DBI base classes:
#'
#' \code{\link[DBI]{DBIObject-class}} \code{\link[DBI]{DBIDriver-class}}
#' \code{\link[DBI]{DBIConnection-class}} \code{\link[DBI]{DBIResult-class}}
#'
#' MySQL classes:
#'
#' \code{\link{MySQLObject-class}} \code{\link{MySQLDriver-class}}
#' \code{\link{MySQLConnection-class}} \code{\link{MySQLResult-class}}
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://developer.r-project.org/db}.
#' @keywords database interface classes
#' @examples
#' \dontrun{
#' drv <- dbDriver("MySQL")
#' con <- dbConnect(drv, dbname = "rsdbi.db")
#' }
#'
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


#' Fetch records from a previously executed query
#'
#' This method is a straight-forward implementation of the corresponding
#' generic function.
#'
#' The \code{RMySQL} implementations retrieves only \code{n} records, and if
#' \code{n} is missing it only returns up to \code{fetch.default.rec} as
#' specified in the call to \code{\link{MySQL}} (500 by default).
#'
#' @name fetch-methods
#' @aliases fetch-methods fetch,MySQLResult,numeric-method
#' fetch,MySQLResult,missing-method
#' @docType methods
#' @section Methods: \describe{
#'
#' \item{res}{ an \code{MySQLResult} object.  } \item{n}{ maximum number of
#' records to retrieve per fetch.  Use \code{n = -1} to retrieve all pending
#' records; use a value of \code{n = 0} for fetching the default number of rows
#' \code{fetch.default.rec} defined in the \code{\link{MySQL}} initialization
#' invocation.  } \item{list()}{currently not used.}\item{ }{currently not
#' used.} }
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{dbConnect}},
#' \code{\link[DBI]{dbSendQuery}}, \code{\link[DBI]{dbGetQuery}},
#' \code{\link[DBI]{dbClearResult}}, \code{\link[DBI]{dbCommit}},
#' \code{\link[DBI]{dbGetInfo}}, \code{\link[DBI]{dbReadTable}}.
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
#' @examples
#' \dontrun{
#' drv <- dbDriver("MySQL")
#' con <- dbConnect(drv, user = "opto", password="pure-light",
#'                  host = "localhost", dbname="lasers")
#' res <- dbSendQuery(con, statement = paste(
#'                       "SELECT w.laser_id, w.wavelength, p.cut_off",
#'                       "FROM WL w, PURGE P",
#'                       "WHERE w.laser_id = p.laser_id",
#'                       "ORDER BY w.laser_id"))
#' # we now fetch the first 100 records from the resultSet into a data.frame
#' data1 <- fetch(res, n = 100)
#' dim(data1)
#'
#' dbHasCompleted(res)
#'
#' # let's get all remaining records
#' data2 <- fetch(res, n = -1)
#' }
#'
NULL
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

#' Class MySQLConnection
#'
#' MySQLConnection class.
#'
#'
#' @name MySQLConnection-class
#' @docType class
#' @section Generators: The method \code{\link[DBI]{dbConnect}} is the main
#' generator.
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
#' drv <- dbDriver("MySQL)
#' con <- dbConnect(drv, dbname = "rsdbi.db")
#' }
setClass("MySQLConnection", representation("DBIConnection", "MySQLObject"))

#' Create a connection object to an MySQL DBMS
#'
#' These methods are straight-forward implementations of the corresponding
#' generic functions.
#'
#' @section Methods: \describe{ \item{drv}{ an object of class
#' \code{MySQLDriver}, or the character string "MySQL" or an
#' \code{MySQLConnection}.  } \item{conn}{ an \code{MySQLConnection} object as
#' produced by \code{dbConnect}.  } \item{username}{string of the MySQL login
#' name or NULL. If NULL or the empty string \code{""}, the current user is
#' assumed.} \item{password}{string with the MySQL password or NULL. If NULL,
#' only entries in the user table for the users that have a blank (empty)
#' password field are checked for a match.} \item{dbname}{string with the
#' database name or NULL. If NOT NULL, the connection sets the default database
#' to this value.} \item{host}{string identifying the host machine running the
#' MySQL server or NULL. If NULL or the string \code{"localhost"}, a connection
#' to the local host is assumed.} \item{unix.socket}{(optional) string of the
#' unix socket or named pipe.} \item{port}{(optional) integer of the TCP/IP
#' default port.} \item{client.flag}{(optional) integer setting various MySQL
#' client flags. See the MySQL manual for details.} \item{group}{string
#' identifying a section in the \code{default.file} to use for setting
#' authentication parameters (see \code{\link{MySQL}}.)}
#' \item{default.file}{string of the filename with MySQL client options.
#' Defaults to \code{\$HOME/.my.cnf}} \item{list()}{Currently unused.}\item{
#' }{Currently unused.} }
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{dbConnect}},
#' \code{\link[DBI]{dbSendQuery}}, \code{\link[DBI]{dbGetQuery}},
#' \code{\link[DBI]{fetch}}, \code{\link[DBI]{dbCommit}},
#' \code{\link[DBI]{dbGetInfo}}, \code{\link[DBI]{dbReadTable}}.
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
#' @examples
#' \dontrun{
#' # create an MySQL instance and create one connection.
#' drv <- dbDriver("MySQL")
#'
#' # open the connection using user, passsword, etc., as
#' con <- dbConnect(drv, group = "rs-dbi")
#'
#' # Run an SQL statement by creating first a resultSet object
#' rs <- dbSendQuery(con, statement = paste(
#'                       "SELECT w.laser_id, w.wavelength, p.cut_off",
#'                       "FROM WL w, PURGE P",
#'                       "WHERE w.laser_id = p.laser_id",
#'                       "SORT BY w.laser_id")
#' # we now fetch records from the resultSet into a data.frame
#' data <- fetch(rs, n = -1)   # extract all rows
#' dim(data)
#' }
#'
#' @export
setMethod("dbConnect", "MySQLDriver",
  function(drv, dbname=NULL, username=NULL,
    password=NULL, host=NULL,
    unix.socket=NULL, port = 0, client.flag = 0,
    groups = 'rs-dbi', default.file = NULL, ...)
  {
    if(!isIdCurrent(drv))
      stop("expired manager")

    if (!is.null(dbname) && !is.character(dbname))
      stop("Argument dbname must be a string or NULL")
    if (!is.null(username) && !is.character(username))
      stop("Argument username must be a string or NULL")
    if (!is.null(password) && !is.character(password))
      stop("Argument password must be a string or NULL")
    if (!is.null(host) && !is.character(host))
      stop("Argument host must be a string or NULL")
    if (!is.null(unix.socket) && !is.character(unix.socket))
      stop("Argument unix.socket must be a string or NULL")

    if (is.null(port) || !is.numeric(port))
      stop("Argument port must be an integer value")
    if (is.null(client.flag) || !is.numeric(client.flag))
      stop("Argument client.flag must be an integer value")

    if (!is.null(groups) && !is.character(groups))
      stop("Argument groups must be a string or NULL")

    if(!is.null(default.file) && !is.character(default.file))
      stop("Argument default.file must be a string")

    if(!is.null(default.file) && !file.exists(default.file[1]))
      stop(sprintf("mysql default file %s does not exist", default.file))

    drvId <- as(drv, "integer")
    conId <- .Call("RS_MySQL_newConnection", drvId,
      dbname, username, password, host, unix.socket,
      as.integer(port), as.integer(client.flag),
      groups, default.file[1], PACKAGE = .MySQLPkgName)

    new("MySQLConnection", Id = conId)
  }
)

#' @export
#' @rdname dbConnect-MySQLDriver
setMethod("dbConnect", "MySQLConnection",
  function(drv, ...) {
    if(!isIdCurrent(drv)) stop(paste("expired", class(drv)))
    conId <- as(drv, "integer")
    newId <- .Call("RS_MySQL_cloneConnection", conId, PACKAGE = .MySQLPkgName)
    new("MySQLConnection", Id = newId)
  }
)

#' @export
#' @rdname dbConnect-MySQLDriver
setMethod("dbDisconnect", "MySQLConnection",
  function(conn, ...) {
    if(!isIdCurrent(conn))
      return(TRUE)
    rs <- dbListResults(conn)
    if(length(rs)>0){
      if(dbHasCompleted(rs[[1]]))
        dbClearResult(rs[[1]])
      else
        stop("connection has pending rows (close open results set first)")
    }
    conId <- as(conn, "integer")
    .Call("RS_MySQL_closeConnection", conId, PACKAGE = .MySQLPkgName)
  }
)

## submits the sql statement to MySQL and creates a
## dbResult object if the SQL operation does not produce
## output, otherwise it produces a resultSet that can
## be used for fetching rows.
#' Execute a statement on a given database connection
#'
#' These methods are straight-forward implementations of the corresponding
#' generic functions.
#'
#'
#' @name dbSendQuery-methods
#' @aliases dbSendQuery-methods dbGetQuery-methods dbClearResult-methods
#' dbGetException-methods dbSendQuery,MySQLConnection,character-method
#' dbGetQuery,MySQLConnection,character-method dbClearResult,MySQLResult-method
#' dbGetException,MySQLConnection-method dbGetException,MySQLResult-method
#' @docType methods
#' @section Methods: \describe{ \item{conn}{ an \code{MySQLConnection} object.
#' } \item{statement}{a character vector of length 1 with the SQL statement.}
#' \item{res}{an \code{MySQLResult} object.} \item{list()}{additional
#' parameters.}\item{ }{additional parameters.} }
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{dbDriver}},
#' \code{\link[DBI]{dbConnect}}, \code{\link[DBI]{fetch}},
#' \code{\link[DBI]{dbCommit}}, \code{\link[DBI]{dbGetInfo}},
#' \code{\link[DBI]{dbReadTable}}.
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
#' @examples
#' \dontrun{
#' drv <- dbDriver("MySQL")
#' con <- dbConnect(drv, "usr", "password", "dbname")
#' res <- dbSendQuery(con, "SELECT * from liv25")
#' data <- fetch(res, n = -1)
#' }
#'
setMethod("dbSendQuery", c("MySQLConnection", "character"),
  function(conn, statement) {
    if(!isIdCurrent(conn))
      stop(paste("expired", class(conn)))
    conId <- as(conn, "integer")
    statement <- as(statement, "character")
    rsId <- .Call("RS_MySQL_exec", conId, statement, PACKAGE = .MySQLPkgName)
    new("MySQLResult", Id = rsId)
  }
)

setMethod("dbGetQuery", c("MySQLConnection", "character"),
  function(conn, statement) {
    if(!isIdCurrent(conn)) stop(paste("expired", class(conn)))

    ## are there resultSets pending on con?
    .clearResultSets(conn)

    rs <- dbSendQuery(conn, statement)
    if(dbHasCompleted(rs)){
      dbClearResult(rs)            ## no records to fetch, we're done
      invisible()
      return(NULL)
    }
    res <- fetch(rs, n = -1)
    if(dbHasCompleted(rs))
      dbClearResult(rs)
    else
      warning("pending rows")
    res
  }
)

setMethod("dbGetException", "MySQLConnection",
  def = function(conn, ...) {
    if(!isIdCurrent(conn))
      stop(paste("expired", class(conn)))
    .Call("RS_MySQL_getException", as(conn, "integer"),
      PACKAGE = .MySQLPkgName)
  }
)

#' Database interface meta-data
#'
#' These methods are straight-forward implementations of the corresponding
#' generic functions.
#'
#' @name dbGetInfo-methods
#' @aliases dbGetInfo dbGetDBIVersion-methods dbGetStatement-methods
#' dbGetRowCount-methods dbGetRowsAffected-methods dbColumnInfo-methods
#' dbHasCompleted-methods dbGetInfo,MySQLObject-method
#' dbGetInfo,MySQLDriver-method dbGetInfo,MySQLConnection-method
#' dbGetInfo,MySQLResult-method dbGetStatement,MySQLResult-method
#' dbGetRowCount,MySQLResult-method dbGetRowsAffected,MySQLResult-method
#' dbColumnInfo,MySQLResult-method dbColumnInfo,MySQLConnection-method
#' dbHasCompleted,MySQLResult-method
#' @docType methods
#' @section Methods: \describe{ \item{dbObj}{ any object that implements some
#' functionality in the R/S-Plus interface to databases (a driver, a connection
#' or a result set).  } \item{res}{ an \code{MySQLResult}.}
#' \item{list()}{currently not being used.} }
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{dbDriver}},
#' \code{\link[DBI]{dbConnect}}, \code{\link[DBI]{dbSendQuery}},
#' \code{\link[DBI]{dbGetQuery}}, \code{\link[DBI]{fetch}},
#' \code{\link[DBI]{dbCommit}}, \code{\link[DBI]{dbGetInfo}},
#' \code{\link[DBI]{dbListTables}}, \code{\link[DBI]{dbReadTable}}.
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
#' @examples
#' \dontrun{
#' drv <- dbDriver("MySQL")
#' con <- dbConnect(drv, group = "wireless")
#'
#' dbListTables(con)
#'
#' rs <- dbSendQuery(con, query.sql)
#' dbGetStatement(rs)
#' dbHasCompleted(rs)
#'
#' info <- dbGetInfo(rs)
#' names(dbGetInfo(drv))
#'
#' # DBIConnection info
#' names(dbGetInfo(con))
#'
#' # DBIResult info
#' names(dbGetInfo(rs))
#' }
#'
setMethod("dbGetInfo", "MySQLConnection",
  function(dbObj, what="", ...)    {
    if(!isIdCurrent(dbObj))
      stop(paste("expired", class(dbObj), deparse(substitute(dbObj))))
    id <- as(dbObj, "integer")
    info <- .Call("RS_MySQL_connectionInfo", id, PACKAGE = .MySQLPkgName)
    rsId <- vector("list", length = length(info$rsId))
    for(i in seq(along = info$rsId))
      rsId[[i]] <- new("MySQLResult", Id = c(id, info$rsId[i]))
    info$rsId <- rsId

    if(!missing(what))
      info[what]
    else
      info
  }
)

setMethod("dbListResults", "MySQLConnection",
  def = function(conn, ...) dbGetInfo(conn, "rsId")[[1]]
)

setMethod("summary", "MySQLConnection",
  function(object, verbose = FALSE, ...) {
    info <- dbGetInfo(object)
    print(object)
    cat("  User:", info$user, "\n")
    cat("  Host:", info$host, "\n")
    cat("  Dbname:", info$dbname, "\n")
    cat("  Connection type:", info$conType, "\n")
    if(verbose){
      cat("  MySQL server version: ", info$serverVersion, "\n")
      cat("  MySQL client version: ",
        dbGetInfo(as(obj, "MySQLDriver"), what="clientVersion")[[1]], "\n")
      cat("  MySQL protocol version: ", info$protocolVersion, "\n")
      cat("  MySQL server thread id: ", info$threadId, "\n")
    }
    if(length(info$rsId)>0){
      for(i in seq(along = info$rsId)){
        cat("   ", i, " ")
        print(info$rsId[[i]])
      }
    } else
      cat("  No resultSet available\n")
    invisible(NULL)
  }
)

## convenience methods
setMethod("dbListTables", "MySQLConnection",
  function(conn, ...) {
    tbls <- dbGetQuery(conn, "show tables")
    if(length(tbls)>0)
      tbls <- tbls[,1]
    else
      tbls <- character()
    tbls
  }
)

#' Convenience functions for Importing/Exporting DBMS tables
#'
#' These functions mimic their R/S-Plus counterpart \code{get}, \code{assign},
#' \code{exists}, \code{remove}, and \code{objects}, except that they generate
#' code that gets remotely executed in a database engine.
#'
#'
#' @name dbReadTable-methods
#' @aliases dbReadTable-methods dbWriteTable-methods dbExistsTable-methods
#' dbRemoveTable-methods dbReadTable,MySQLConnection,character-method
#' dbWriteTable,MySQLConnection,character,character-method
#' dbWriteTable,MySQLConnection,character,data.frame-method
#' dbExistsTable,MySQLConnection,character-method
#' dbRemoveTable,MySQLConnection,character-method
#' @docType methods
#' @return A \code{data.frame} in the case of \code{dbReadTable}; otherwise a
#' logical indicating whether the operation was successful.
#' @note Note that data.frames are only approximately analogous to tables
#' (relations) in relational DBMS, and thus you should not expect complete
#' agreement in their semantics.  Tables in RDBMS are best thought of as
#' \emph{relations} with a number of constraints imposed by the relational
#' database model, and data.frames, with their roots in statistical modeling,
#' as self-contained "sequence of observations on some chosen variables"
#' (Chambers and Hastie (1992), p.46).  In particular the \code{data.frame}
#' returned by \code{dbReadTable} only has primitive data, e.g., it does not
#' coerce character data to factors.  Also, column names in a data.frame are
#' \emph{not} guaranteed to be equal to the column names in a MySQL
#' exported/imported table (e.g., by default MySQL reserved identifiers may not
#' be used as column names --- and with 218 keywords like \code{"BEFORE"},
#' \code{"DESC"}, and \code{"FROM"} the likelihood of name conflicts is not
#' small.) Use \code{isSQLKeyword(con, names(value))} to check whether the
#' data.frame names in \code{value} coincide with MySQL reserver words.
#'
#' MySQL table names are \emph{not} case sensitive, e.g., table names
#' \code{ABC} and \code{abc} are considered equal.
#' @section Methods: \describe{ \item{conn}{ an \code{MySQLConnection} database
#' connection object.  } \item{name}{ a character string specifying a table
#' name.  } \item{value}{ a data.frame (or coercible to data.frame).  }
#' \item{row.names}{ in the case of \code{dbReadTable}, this argument can be a
#' string or an index specifying the column in the DBMS table to be used as
#' \code{row.names} in the output data.frame (a \code{NULL}, \code{""}, or 0
#' specifies that no column should be used as \code{row.names} in the output).
#'
#' In the case of \code{dbWriteTable}, this argument should be a logical
#' specifying whether the \code{row.names} should be output to the output DBMS
#' table; if \code{TRUE}, an extra field whose name will be whatever the
#' R/S-Plus identifier \code{"row.names"} maps to the DBMS (see
#' \code{\link[DBI]{make.db.names}}).  } \item{overwrite}{ a logical specifying
#' whether to overwrite an existing table or not.  Its default is \code{FALSE}.
#' } \item{append}{ a logical specifying whether to append to an existing table
#' in the DBMS.  Its default is \code{FALSE}.  } \item{allow.keywords}{
#' \code{dbWriteTable} accepts a logical \code{allow.keywords} to allow or
#' prevent MySQL reserved identifiers to be used as column names. By default it
#' is \code{FALSE}.  } \item{dots}{ optional arguments.
#'
#' When \code{dbWriteTable} is used to import data from a file, you may
#' optionally specify \code{header=}, \code{row.names=}, \code{col.names=},
#' \code{sep=}, \code{eol=}, \code{field.types=}, \code{skip=}, and
#' \code{quote=}.
#'
#' \code{header} is a logical indicating whether the first data line (but see
#' \code{skip}) has a header or not.  If missing, it value is determined
#' following \code{\link{read.table}} convention, namely, it is set to TRUE if
#' and only if the first row has one fewer field that the number of columns.
#'
#' \code{row.names} is a logical to specify whether the first column is a set
#' of row names.  If missing its default follows the \code{\link{read.table}}
#' convention.
#'
#' \code{col.names} a character vector with column names (these names will be
#' filtered with \code{\link[DBI]{make.db.names}} to ensure valid SQL
#' identifiers. (See also \code{field.types} below.)
#'
#' \code{sep=} specifies the field separator, and its default is \code{','}.
#'
#' \code{eol=} specifies the end-of-line delimiter, and its default is
#' \code{'\n'}.
#'
#' \code{skip} specifies number of lines to skip before reading the data, and
#' it defaults to 0.
#'
#' \code{field.types} is a list of named field SQL types where
#' \code{names(field.types)} provide the new table's column names (if missing,
#' field types are inferred using \code{\link[DBI]{dbDataType}}).  }
#'
#' }
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{isSQLKeyword}},
#' \code{\link[DBI]{dbDriver}}, \code{\link[DBI]{dbConnect}},
#' \code{\link[DBI]{dbSendQuery}}, \code{\link[DBI]{dbGetQuery}},
#' \code{\link[DBI]{fetch}}, \code{\link[DBI]{dbCommit}},
#' \code{\link[DBI]{dbGetInfo}}, \code{\link[DBI]{dbListTables}},
#' \code{\link[DBI]{dbReadTable}}.
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
#' @examples
#' \dontrun{
#' conn <- dbConnect("MySQL", group = "wireless")
#' if(dbExistsTable(con, "fuel_frame")){
#'    dbRemoveTable(conn, "fuel_frame")
#'    dbWriteTable(conn, "fuel_frame", fuel.frame)
#' }
#' if(dbExistsTable(conn, "RESULTS")){
#'    dbWriteTable(conn, "RESULTS", results2000, append = T)
#' else
#'    dbWriteTable(conn, "RESULTS", results2000)
#' }
#' }
#'
setMethod("dbReadTable", signature(conn="MySQLConnection", name="character"),
  function(conn, name, row.names = "row_names", check.names = TRUE, ...) {
    out <- dbGetQuery(conn, paste("SELECT * from", name))
    if(check.names)
      names(out) <- make.names(names(out), unique = TRUE)
    ## should we set the row.names of the output data.frame?
    nms <- names(out)
    j <- switch(mode(row.names),
      "character" = if(row.names=="") 0 else
        match(tolower(row.names), tolower(nms),
          nomatch = if(missing(row.names)) 0 else -1),
      "numeric" = row.names,
      "NULL" = 0,
      0)
    if(j==0)
      return(out)
    if(j<0 || j>ncol(out)){
      warning("row.names not set on output data.frame (non-existing field)")
      return(out)
    }
    rnms <- as.character(out[,j])
    if(all(!duplicated(rnms))){
      out <- out[,-j, drop = FALSE]
      row.names(out) <- rnms
    } else warning("row.names not set on output (duplicate elements in field)")
    out
  }
)

## Create table "name" (must be an SQL identifier) and populate
## it with the values of the data.frame "value"
## TODO: This function should execute its sql as a single transaction,
##       and allow converter functions.
## TODO: In the unlikely event that value has a field called "row_names"
##       we could inadvertently overwrite it (here the user should set
##       row.names=F)  I'm (very) reluctantly adding the code re: row.names,
##       because I'm not 100% comfortable using data.frames as the basic
##       data for relations.
setMethod("dbWriteTable",
  signature(conn="MySQLConnection", name="character", value="data.frame"),
  function(conn, name, value, field.types, row.names = TRUE,
           overwrite = FALSE, append = FALSE, ..., allow.keywords = FALSE)     {
    if(overwrite && append)
      stop("overwrite and append cannot both be TRUE")
    if(!is.data.frame(value))
      value <- as.data.frame(value)
    if(row.names){
      value <- cbind(row.names(value), value)  ## can't use row.names= here
      names(value)[1] <- "row.names"
    }
    if(missing(field.types) || is.null(field.types)){
      ## the following mapping should be coming from some kind of table
      ## also, need to use converter functions (for dates, etc.)
      field.types <- lapply(value, dbDataType, dbObj = conn)
    }

    ## Do we need to coerce any field prior to write it out?
    ## TODO: MySQL 4.1 introduces the boolean data type.
    for(i in seq(along = value)){
      if(is(value[[i]], "logical"))
        value[[i]] <- as(value[[i]], "integer")
    }
    i <- match("row.names", names(field.types), nomatch=0)
    if(i>0) ## did we add a row.names value?  If so, it's a text field.
      field.types[i] <- dbDataType(dbObj=conn, field.types$row.names)
    names(field.types) <- make.db.names(conn, names(field.types),
      allow.keywords = allow.keywords)

    .clearResultSets(conn)

    if(dbExistsTable(conn,name)){
      if(overwrite){
        if(!dbRemoveTable(conn, name)){
          warning(paste("table", name, "couldn't be overwritten"))
          return(FALSE)
        }
      }
      else if(!append){
        warning(paste("table",name,"exists in database: aborting mysqlWriteTable"))
        return(FALSE)
      }
    }
    if(!dbExistsTable(conn,name)){      ## need to re-test table for existence
      ## need to create a new (empty) table
      sql1 <- paste("create table ", name, "\n(\n\t", sep="")
      sql2 <- paste(paste(names(field.types), field.types), collapse=",\n\t",
        sep="")
      sql3 <- "\n)\n"
      sql <- paste(sql1, sql2, sql3, sep="")
      rs <- try(dbSendQuery(conn, sql))
      if(inherits(rs, "try-error")){
        warning("could not create table: aborting mysqlWriteTable")
        return(FALSE)
      }
      else
        dbClearResult(rs)
    }

    ## TODO: here, we should query the MySQL to find out if it supports
    ## LOAD DATA thru pipes; if so, should open the pipe instead of a file.

    fn <- tempfile("rsdbi")
    fn <- gsub("\\\\", "/", fn)  # Since MySQL on Windows wants \ double (BDR)
    safe.write(value, file = fn)
    on.exit(unlink(fn), add = TRUE)
    sql4 <- paste("LOAD DATA LOCAL INFILE '", fn, "'",
      " INTO TABLE ", name,
      " LINES TERMINATED BY '\n' ",
      "( ", paste(names(field.types), collapse=", "), ");",
      sep="")
    rs <- try(dbSendQuery(conn, sql4))
    if(inherits(rs, "try-error")){
      warning("could not load data into table")
      return(FALSE)
    }
    else
      dbClearResult(rs)
    TRUE
  }
)

## write table from filename (TODO: connections)
setMethod("dbWriteTable",
  signature(conn="MySQLConnection", name="character", value="character"),
  function(conn, name, value, field.types = NULL, overwrite = FALSE,
           append = FALSE, header, row.names, nrows = 50, sep = ",",
           eol="\n", skip = 0, quote = '"', ...)   {
    if(overwrite && append)
      stop("overwrite and append cannot both be TRUE")

    .clearResultSets(conn)

    if(dbExistsTable(conn,name)){
      if(overwrite){
        if(!dbRemoveTable(conn, name)){
          warning(paste("table", name, "couldn't be overwritten"))
          return(FALSE)
        }
      }
      else if(!append){
        warning(paste("table", name, "exists in database: aborting dbWriteTable"))
        return(FALSE)
      }
    }

    ## compute full path name (have R expand ~, etc)
    fn <- file.path(dirname(value), basename(value))
    if(missing(header) || missing(row.names)){
      f <- file(fn, open="r")
      if(skip>0)
        readLines(f, n=skip)
      txtcon <- textConnection(readLines(f, n=2))
      flds <- count.fields(txtcon, sep)
      close(txtcon)
      close(f)
      nf <- length(unique(flds))
    }
    if(missing(header)){
      header <- nf==2
    }
    if(missing(row.names)){
      if(header)
        row.names <- if(nf==2) TRUE else FALSE
      else
        row.names <- FALSE
    }

    new.table <- !dbExistsTable(conn, name)
    if(new.table){
      ## need to init table, say, with the first nrows lines
      d <- read.table(fn, sep=sep, header=header, skip=skip, nrows=nrows, ...)
      sql <-
        dbBuildTableDefinition(conn, name, obj=d, field.types = field.types,
          row.names = row.names)
      rs <- try(dbSendQuery(conn, sql))
      if(inherits(rs, "try-error")){
        warning("could not create table: aborting mysqlImportFile")
        return(FALSE)
      }
      else
        dbClearResult(rs)
    }
    else if(!append){
      warning(sprintf("table %s already exists -- use append=TRUE?", name))
    }

    fmt <-
      paste("LOAD DATA LOCAL INFILE '%s' INTO TABLE  %s ",
        "FIELDS TERMINATED BY '%s' ",
        if(!is.null(quote)) "OPTIONALLY ENCLOSED BY '%s' " else "",
        "LINES TERMINATED BY '%s' ",
        "IGNORE %d LINES ", sep="")
    if(is.null(quote))
      sql <- sprintf(fmt, fn, name, sep, eol, skip + as.integer(header))
    else
      sql <- sprintf(fmt, fn, name, sep, quote, eol, skip + as.integer(header))

    rs <- try(dbSendQuery(conn, sql))
    if(inherits(rs, "try-error")){
      warning("could not load data into table")
      return(FALSE)
    }
    dbClearResult(rs)
    TRUE
  }

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
      !inherits(rc, "try-error")
    }
    else FALSE
  },
  valueClass = "logical"
)

#' List items from an MySQL DBMS and from objects
#'
#' These methods are straight-forward implementations of the corresponding
#' generic functions.
#'
#'
#' @name dbListTables-methods
#' @aliases dbListTables-methods dbListFields-methods dbListConnections-methods
#' dbListResults-methods dbListTables,MySQLConnection-method
#' dbListFields,MySQLConnection,character-method
#' dbListFields,MySQLResult,missing-method dbListConnections,MySQLDriver-method
#' dbListResults,MySQLConnection-method
#' @docType methods
#' @section Methods: \describe{ \item{drv}{an \code{MySQLDriver}.}
#' \item{conn}{an \code{MySQLConnection}.} \item{name}{a character string with
#' the table name.} \item{list()}{currently not used.} }
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{dbGetInfo}},
#' \code{\link[DBI]{dbColumnInfo}}, \code{\link[DBI]{dbDriver}},
#' \code{\link[DBI]{dbConnect}}, \code{\link[DBI]{dbSendQuery}}
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
#' @examples
#' \dontrun{
#' drv <- dbDriver("MySQL")
#' # after working awhile...
#' for(con in dbListConnections(drv)){
#'    dbGetStatement(dbListResults(con))
#' }
#' }
#'
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

#' DBMS Transaction Management
#'
#' Commits or roll backs the current transaction in an MySQL connection
#'
#' @section Methods: \describe{ \item{conn}{a \code{MySQLConnection} object, as
#' produced by the function \code{dbConnect}.} \item{list()}{currently
#' unused.}\item{ }{currently unused.} }
#' @seealso \code{\link{MySQL}}, \code{\link[DBI]{dbConnect}},
#' \code{\link[DBI]{dbSendQuery}}, \code{\link[DBI]{dbGetQuery}},
#' \code{\link[DBI]{fetch}}, \code{\link[DBI]{dbCommit}},
#' \code{\link[DBI]{dbGetInfo}}, \code{\link[DBI]{dbReadTable}}.
#' @references See the Database Interface definition document \code{DBI.pdf} in
#' the base directory of this package or
#' \url{http://stat.bell-labs.com/RS-DBI}.
#' @keywords methods interface database
#' @examples
#' \dontrun{
#' drv <- dbDriver("MySQL")
#' con <- dbConnect(drv, group = "group")
#' rs <- dbSendQuery(con,
#'       "delete * from PURGE as p where p.wavelength<0.03")
#' if(dbGetInfo(rs, what = "rowsAffected") > 250){
#'   warning("dubious deletion -- rolling back transaction")
#'   dbRollback(con)
#' }
#' }
setMethod("dbCommit", "MySQLConnection",
  def = function(conn, ...) .NotYetImplemented()
)

#' @export
#' @rdname dbCommit
setMethod("dbRollback", "MySQLConnection",
  def = function(conn, ...) .NotYetImplemented()
)

#' @export
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
  }
)

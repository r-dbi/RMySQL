
##
## Class: DBIConnection
##
setClass("MySQLConnection", representation("DBIConnection", "MySQLObject"))

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

## clone a connection
setMethod("dbConnect", "MySQLConnection",
  function(drv, ...) {
    if(!isIdCurrent(drv)) stop(paste("expired", class(drv)))
    conId <- as(drv, "integer")
    newId <- .Call("RS_MySQL_cloneConnection", conId, PACKAGE = .MySQLPkgName)
    new("MySQLConnection", Id = newId)
  }
)

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

## Use NULL, "", or 0 as row.names to prevent using any field as row.names.
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
  }
)

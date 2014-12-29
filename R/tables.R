#' Convenience functions for importing/exporting DBMS tables
#'
#' These functions mimic their R/S-Plus counterpart \code{get}, \code{assign},
#' \code{exists}, \code{remove}, and \code{objects}, except that they generate
#' code that gets remotely executed in a database engine.
#'
#' @return A data.frame in the case of \code{dbReadTable}; otherwise a logical
#' indicating whether the operation was successful.
#' @note Note that the data.frame returned by \code{dbReadTable} only has
#' primitive data, e.g., it does not coerce character data to factors.
#'
#' @param conn a \code{\linkS4class{SQLiteConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param name a character string specifying a table name. SQLite table names
#'   are \emph{not} case sensitive, e.g., table names \code{ABC} and \code{abc}
#'   are considered equal.
#' @param check.names If \code{TRUE}, the default, column names will be
#'   converted to valid R identifiers.
#' @param row.names A string or an index specifying the column in the DBMS table
#'   to use as \code{row.names} in the output data.frame. Defaults to using the
#'   \code{row_names} column if present. Set to \code{NULL} to never use
#'   row names.
#' @name table
NULL

#' @export
#' @rdname table
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
#' @export
#' @rdname table
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
#' @export
#' @rdname table
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

#' @export
#' @rdname table
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

#' @export
#' @rdname table
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

#' @export
#' @rdname table
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

#' @export
#' @rdname table
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

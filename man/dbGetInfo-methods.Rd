% $Id$
\name{dbGetInfo-methods}
\docType{methods}
\alias{dbGetInfo}
\alias{dbGetDBIVersion-methods}
\alias{dbGetStatement-methods}
\alias{dbGetRowCount-methods}
\alias{dbGetRowsAffected-methods}
\alias{dbColumnInfo-methods}
\alias{dbHasCompleted-methods}
\alias{dbGetInfo,MySQLObject-method}
\alias{dbGetInfo,MySQLDriver-method}        % BUG: this is not needed
\alias{dbGetInfo,MySQLConnection-method}        % BUG: this is not needed
\alias{dbGetInfo,MySQLResult-method}        % BUG: this is not needed
\alias{dbGetStatement,MySQLResult-method}
\alias{dbGetRowCount,MySQLResult-method}
\alias{dbGetRowsAffected,MySQLResult-method}
\alias{dbColumnInfo,MySQLResult-method}
\alias{dbColumnInfo,MySQLConnection-method}
\alias{dbHasCompleted,MySQLResult-method}
\title{
  Database interface meta-data
}
\description{
   These methods are straight-forward implementations of the corresponding
   generic functions.
}
\section{Methods}{\describe{
\item{dbObj}{
  any object that implements some functionality in the R/S-Plus
  interface to databases (a driver, a connection or a result set).
  }
\item{res}{ an \code{MySQLResult}.}
\item{\dots}{currently not being used.}
}
}
\references{
  See the Database Interface definition document
  \code{DBI.pdf} in the base directory of this package
  or \url{http://stat.bell-labs.com/RS-DBI}.
}
\seealso{
  \code{\link{MySQL}},
  \code{\link[DBI]{dbDriver}},
  \code{\link[DBI]{dbConnect}},
  \code{\link[DBI]{dbSendQuery}},
  \code{\link[DBI]{dbGetQuery}},
  \code{\link[DBI]{fetch}},
  \code{\link[DBI]{dbCommit}},
  \code{\link[DBI]{dbGetInfo}},
  \code{\link[DBI]{dbListTables}},
  \code{\link[DBI]{dbReadTable}}.
}
\examples{\dontrun{
drv <- dbDriver("MySQL")
con <- dbConnect(drv, group = "wireless")

dbListTables(con)

rs <- dbSendQuery(con, query.sql)
dbGetStatement(rs)
dbHasCompleted(rs)

info <- dbGetInfo(rs)
names(dbGetInfo(drv))  

# DBIConnection info
names(dbGetInfo(con))

# DBIResult info
names(dbGetInfo(rs)) 
}
}
\keyword{methods}
\keyword{interface}
\keyword{database}
% vim: syntax=tex

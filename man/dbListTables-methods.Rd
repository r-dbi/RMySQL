% $Id$
\name{dbListTables-methods}
\docType{methods}
\alias{dbListTables-methods}
\alias{dbListFields-methods}
\alias{dbListConnections-methods}
\alias{dbListResults-methods}
\alias{dbListTables,MySQLConnection-method}
\alias{dbListFields,MySQLConnection,character-method}
\alias{dbListFields,MySQLResult,missing-method}
\alias{dbListConnections,MySQLDriver-method}
\alias{dbListResults,MySQLConnection-method}
\title{
  List items from an MySQL DBMS and from objects 
}
\description{
   These methods are straight-forward implementations of the corresponding
   generic functions.
}
\section{Methods}{\describe{
  \item{drv}{an \code{MySQLDriver}.}
  \item{conn}{an \code{MySQLConnection}.}
  \item{name}{a character string with the table name.}
  \item{\dots}{currently not used.}
}
}
\references{
  See the Database Interface definition document
  \code{DBI.pdf} in the base directory of this package
  or
  \url{http://stat.bell-labs.com/RS-DBI}.
}
\seealso{
  \code{\link{MySQL}},
  \code{\link[DBI]{dbGetInfo}},
  \code{\link[DBI]{dbColumnInfo}},
  \code{\link[DBI]{dbDriver}},
  \code{\link[DBI]{dbConnect}},
  \code{\link[DBI]{dbSendQuery}}
}

\examples{\dontrun{
drv <- dbDriver("MySQL")
# after working awhile...
for(con in dbListConnections(drv)){
   dbGetStatement(dbListResults(con))
}
}
}
\keywords{methods}
\keyword{interface}
\keyword{database}
% vim: syntax=tex

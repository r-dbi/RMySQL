\name{dbNextResult-methods}
\docType{methods}
\alias{dbNextResult-methods}
\alias{dbNextResult,MySQLConnection-method}
\alias{dbMoreResults-methods}
\alias{dbMoreResults,MySQLConnection-method}
\title{Fetch Next Result Set from Multiple Statements or Stored Procedures}
\description{
  \code{dbMoreResults} checks whether there are additional result sets for
  processing. \code{dbNextResult} fetches the next result set.  
}
\section{Methods}{
\describe{
 These MySQL methods provide functionality to sequentially extract multiple
 results produced by SQL scripts or stored procedures.

 \item{con = "MySQLConnection"}{a MySQL connection object.}
 }
}
\section{Note}{
  MySQL supports SQL scripts (multiple statements terminated by ';') and 
  stored procedures from version 4.1.1 onwards.

  To process SQL scripts or stored procedures on a MySQL connection, 
  the connection must be created using the \code{CLIENT\_MULTI\_STATEMENTS} or 
  \code{CLIENT\_MULTI\_RESULTS} client flags (see the example below).

  More precisely, to execute multiple statements the connection needs 
  \code{CLIENT\_MULTI\_STATEMENTS}, but this does not allow 
  \emph{fetching} of multiple output results -- for that you need
  the connection to support \code{CLIENT\_MULTI\_RESULTS}; 
  this last flag does provide functionality for both executing multiple 
  statements \bold{and} fetching multiple results, thus it is simpler 
  to just use the latter.

  See the MySQL documentation in \url{www.mysql.com} for more details.
}
\keyword{methods}
\seealso{
  \code{\link{MySQL}},
  \code{\link[DBI]{dbConnect}},
  \code{\link[DBI]{dbSendQuery}},
  \code{\link[DBI]{dbGetQuery}},
  \code{\link[DBI]{fetch}},
  \code{\link[DBI]{dbCommit}},
  \code{\link[DBI]{dbGetInfo}},
  \code{\link[DBI]{dbReadTable}}.
}
\examples{\dontrun{
con <- dbConnect(MySQL(), 
          dbname = "rs-dbi", 
          client.flag=CLIENT\_MULTI\_RESULTS)
sql.script <- paste(
   "select * from abc",
   "select * def", 
   collapse = ";")

rs1 <- dbSendQuery(con, sql.script)
data1 <- fetch(rs1, n = -1)

if(dbMoreResults(con)){
   rs2 <- dbNextResult(con)
   data2 <- fetch(rs2, n = -1)   
   }
}
}
\keyword{methods}
\keyword{interface}
\keyword{database}
% vim: syntax=tex

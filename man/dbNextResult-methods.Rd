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

 In the case of stored procedures invoked with \code{CALL}, the first 
 result set indicates the call status, and output data (if any) are
 return as additional result sets.
 
 \item{con = "MySQLConnection"}{a MySQL connection object.}
 }
}
\section{Note}{
  MySQL supports SQL scripts (a single string with multiple statements 
  terminated by ';') from version 4.1.1 onwards and stored procedures 
  from version 5.0.

  To process SQL scripts on a MySQL connection, 
  the connection must be created using the \code{CLIENT\_MULTI\_STATEMENTS}.
  In addition, to process stored procedures that return one or more
  result sets, the connection must be created using the 
  \code{CLIENT\_MULTI\_RESULTS} client flag.

  For simplicity, use \code{CLIENT\_MULTI\_STATEMENTS} for working
  with either SQL scripts or stored procedures.  For more details, read on.

  More precisely, to execute multiple statements the connection needs 
  \code{CLIENT\_MULTI\_STATEMENTS}; this in turn automatically enables 
  \code{CLIENT\_MULTI\_RESULTS} for \emph{fetching} of multiple output 
  results. 
  On the other hand, the client flag \code{CLIENT\_MULTI\_RESULTS} by 
  itself enables stored procedures to return one or more results.
  See the MySQL documentation in \url{www.mysql.com} for full details.
}
\keyword{methods}
\seealso{
  \code{\link{MySQL}},
  \code{\link[DBI]{dbConnect}},
  \code{\link[DBI]{dbSendQuery}},
  \code{\link[DBI]{dbHasCompleted}},
  \code{\link[DBI]{fetch}},
  \code{\link[DBI]{dbCommit}},
  \code{\link[DBI]{dbGetInfo}},
  \code{\link[DBI]{dbReadTable}}.
}
\examples{\dontrun{
con <- dbConnect(MySQL(), 
          dbname = "rs-dbi", 
          client.flag=CLIENT\_MULTI\_STATEMENTS)
sql.script <- paste(
   "select * from abc",
   "select * def", 
   collapse = ";")

rs1 <- dbSendQuery(con, sql.script)
data1 <- fetch(rs1, n = -1)

if(dbMoreResults(con)){
   rs2 <- dbNextResult(con)
   ## you could use dbHasCompleted(rs2) to determine whether
   ## rs2 is a select-like that generates output or not.
   data2 <- fetch(rs2, n = -1)   
   }
}
}
\keyword{methods}
\keyword{interface}
\keyword{database}
% vim: syntax=tex

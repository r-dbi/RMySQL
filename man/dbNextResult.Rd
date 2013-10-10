\name{dbNextResult}
\alias{dbNextResult}
\alias{dbMoreResults}
\title{Fetch next result set from an SQL script or stored procedure (experimental)}
\description{
  Fetches the next result set from the output of a multi-statement SQL 
  script or stored procedure; checkes whether there are additonal
  result sets to process.
}
\usage{
dbNextResult(con, ...)
dbMoreResults(con, ...)
}
\arguments{
  \item{con}{a connection object (see \code{\link[DBI]{dbConnect}}).}
  \item{...}{any additional arguments to be passed to the dispatched method}
}
\details{

  SQL scripts (i.e., multiple SQL statements separated by ';')
  and stored procedures oftentimes generate multiple result sets.  These
  DBI generic functions provide a means to process them sequentially.

  \code{dbNextResult} fetches the next result from the sequence of
  pending results sets;  \code{dbMoreResults} returns a logical to 
  indicate whether there are additional results to process.
}
\value{
  \code{dbNextResult} returns a result set or \code{NULL}.

  \code{dbMoreResults} returns a logical specifying whether or not there
  are additional result sets to process in the connection.
}
\section{Note}{
  Currently only the \code{\link{MySQL}} driver implements these methods.
  See 'methods?dbNextMethod'.
}
\seealso{
   \code{\link{MySQL}}
   \code{\link[DBI]{dbConnect}}
   \code{\link[DBI]{dbSendQuery}}
   \code{\link[DBI]{fetch}}
}

\examples{\dontrun{
rs1 <- dbSendQuery(con, 
         paste(
             "select Agent, ip\_addr, DATA from pseudo\_data order by Agent",
             "select * from Agent\_name",
              sep = ";")
         )
x1 <- fetch(rs1, n = -1)
if(dbMoreResults(con)){
   rs2 <- dbNextResult(con)
   x2 <- fetch(rs2, n = -1)
}
}
}
\keyword{programming}% at least one, from doc/KEYWORDS
\keyword{interface}% __ONLY ONE__ keyword per line
\keyword{database}
% vim: syntax=tex

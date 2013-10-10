\name{fetch-methods}
\docType{methods}
\alias{fetch-methods}
\alias{fetch,MySQLResult,numeric-method}
\alias{fetch,MySQLResult,missing-method}
%\alias{fetch,MySQLResult-method}
\title{
  Fetch records from a previously executed query
}
\description{
   This method is a straight-forward implementation of the corresponding
   generic function.
}
\section{Methods}{\describe{

\item{res}{
  an \code{MySQLResult} object.
  }
\item{n}{
  maximum number of records to retrieve per fetch.
  Use \code{n = -1} to retrieve all pending records;
  use a value of \code{n = 0} for fetching the default number 
  of rows \code{fetch.default.rec} defined in the 
  \code{\link{MySQL}} initialization invocation.
  }
\item{\dots }{currently not used.}
}
}
\details{
  The \code{RMySQL} implementations retrieves only \code{n} records,
  and if \code{n} is missing it only returns up to \code{fetch.default.rec}
  as specified in the call to \code{\link{MySQL}} (500 by default).
}
\references{
  See the Database Interface definition document
  \code{DBI.pdf} in the base directory of this package
  or \url{http://stat.bell-labs.com/RS-DBI}.
}
\seealso{
  \code{\link{MySQL}},
  \code{\link[DBI]{dbConnect}},
  \code{\link[DBI]{dbSendQuery}},
  \code{\link[DBI]{dbGetQuery}},
  \code{\link[DBI]{dbClearResult}},
  \code{\link[DBI]{dbCommit}},
  \code{\link[DBI]{dbGetInfo}},
  \code{\link[DBI]{dbReadTable}}.
}
\examples{\dontrun{
drv <- dbDriver("MySQL")
con <- dbConnect(drv, user = "opto", password="pure-light", 
                 host = "localhost", dbname="lasers")
res <- dbSendQuery(con, statement = paste(
                      "SELECT w.laser_id, w.wavelength, p.cut_off",
                      "FROM WL w, PURGE P", 
                      "WHERE w.laser_id = p.laser_id",
                      "ORDER BY w.laser_id"))
# we now fetch the first 100 records from the resultSet into a data.frame
data1 <- fetch(res, n = 100)   
dim(data1)

dbHasCompleted(res)

# let's get all remaining records
data2 <- fetch(res, n = -1)
}
}
\keyword{methods}
\keyword{interface}
\keyword{database}
% vim: syntax=tex

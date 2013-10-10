\name{dbSendQuery-methods}
\docType{methods}
\alias{dbSendQuery-methods}
\alias{dbGetQuery-methods}
\alias{dbClearResult-methods}
\alias{dbGetException-methods}
\alias{dbSendQuery,MySQLConnection,character-method}
\alias{dbGetQuery,MySQLConnection,character-method}
\alias{dbClearResult,MySQLResult-method}
\alias{dbGetException,MySQLConnection-method}
\alias{dbGetException,MySQLResult-method}
\title{
  Execute a statement on a given database connection
}
\description{
   These methods are straight-forward implementations of the corresponding
   generic functions.
}
\section{Methods}{\describe{
\item{conn}{
  an \code{MySQLConnection} object.
  }
\item{statement}{a character vector of length 1 with the SQL statement.}
\item{res}{an \code{MySQLResult} object.}
\item{\dots }{additional parameters.}
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
  \code{\link[DBI]{fetch}},
  \code{\link[DBI]{dbCommit}},
  \code{\link[DBI]{dbGetInfo}},
  \code{\link[DBI]{dbReadTable}}.
}
\examples{\dontrun{
drv <- dbDriver("MySQL")
con <- dbConnect(drv, "usr", "password", "dbname")
res <- dbSendQuery(con, "SELECT * from liv25")
data <- fetch(res, n = -1)
}
}
\keyword{methods}
\keyword{interface}
\keyword{database}
% vim: syntax=tex

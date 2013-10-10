\name{MySQLDriver-class}
\docType{class}
\alias{MySQLDriver-class}
\title{Class MySQLDriver}
\description{
   An MySQL driver implementing the R/S-Plus database (DBI) API.
}
\section{Generators}{
   The main generators are \code{\link[DBI]{dbDriver}} and 
   \code{\link{MySQL}}.
}
\section{Extends}{
Class \code{"DBIDriver"}, directly.
Class \code{"MySQLObject"}, directly.
Class \code{"DBIObject"}, by class "DBIDriver".
}
\section{Methods}{
  \describe{
    \item{\link{coerce}}{\code{signature(from = "MySQLObject", to = "MySQLDriver")}: ... }
    \item{\link[DBI]{dbConnect}}{\code{signature(drv = "MySQLDriver")}: ... }
    \item{\link[DBI]{dbGetInfo}}{\code{signature(dbObj = "MySQLDriver")}: ... }
    \item{\link[DBI]{dbListConnections}}{\code{signature(drv = "MySQLDriver")}: ... }
    \item{\link[DBI]{dbUnloadDriver}}{\code{signature(drv = "MySQLDriver")}: ... }
    \item{\link{summary}}{\code{signature(object = "MySQLDriver")}: ... }
  }
}
\references{
  See the Database Interface definition document
  \code{DBI.pdf} in the base directory of this package
  or \url{http://developer.r-project.org/db}.
}

\seealso{
 DBI base classes:

 \code{\link[DBI]{DBIObject-class}}
 \code{\link[DBI]{DBIDriver-class}}
 \code{\link[DBI]{DBIConnection-class}}
 \code{\link[DBI]{DBIResult-class}}

 MySQL classes:

 \code{\link{MySQLObject-class}}
 \code{\link{MySQLDriver-class}}
 \code{\link{MySQLConnection-class}}
 \code{\link{MySQLResult-class}}
}

\examples{\dontrun{
drv <- dbDriver("MySQL")
con <- dbConnect(drv, "user/password@dbname")
}
}
\keyword{database}
\keyword{interface}
\keyword{classes}
% vim: syntax=tex

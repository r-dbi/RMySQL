\name{MySQLObject-class}
\docType{class}
\alias{MySQLObject-class}
\title{Class MySQLObject}
\description{
   Base class for all MySQL-specific DBI classes
}
\section{Objects from the Class}{
   A virtual Class: No objects may be created from it.
}
\section{Extends}{
Class \code{"DBIObject"}, directly.
Class \code{"dbObjectId"}, directly.
}
\section{Methods}{
  \describe{
    \item{\link{coerce}}{\code{signature(from = "MySQLObject", to = "MySQLriver")}: ... }
    \item{\link[DBI]{dbDataType}}{\code{signature(dbObj = "MySQLObject")}: ... }
    \item{\link[DBI]{isSQLKeyword}}{\code{signature(dbObj = "MySQLObject", name = "character")}: ... }
    \item{\link[DBI]{make.db.names}}{\code{signature(dbObj = "MySQLObject", snames = "character")}: ... }
    \item{\link[DBI]{SQLKeywords}}{\code{signature(dbObj = "MySQLObject")}: ... }
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
con <- dbConnect(drv, dbname = "rsdbi.db")
}
}
\keyword{database}
\keyword{interface}
\keyword{classes}
% vim: syntax=tex

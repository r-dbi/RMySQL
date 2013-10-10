\name{MySQLResult-class}
\docType{class}
\alias{MySQLResult-class}
\title{Class MySQLResult}
\description{
   MySQL's query results class.  This classes encapsulates the
   result of an SQL statement (either \code{select} or not).
}
\section{Generators}{
   The main generator is \code{\link[DBI]{dbSendQuery}}.
}
\section{Extends}{
Class \code{"DBIResult"}, directly.
Class \code{"MySQLObject"}, directly.
Class \code{"DBIObject"}, by class "DBIResult".
Class \code{"dbObjectId"}, by class "MySQLObject".
}
\section{Methods}{
  \describe{
    \item{\link{coerce}}{\code{signature(from = "MySQLConnection", to = "MySQLResult")}: ... }
    \item{\link[DBI]{dbClearResult}}{\code{signature(res = "MySQLResult")}: ... }
    \item{\link[DBI]{dbColumnInfo}}{\code{signature(res = "MySQLResult")}: ... }
    \item{\link[DBI]{dbGetException}}{\code{signature(conn = "MySQLResult")}: ... }
    \item{\link[DBI]{dbGetInfo}}{\code{signature(dbObj = "MySQLResult")}: ... }
    \item{\link[DBI]{dbGetRowCount}}{\code{signature(res = "MySQLResult")}: ... }
    \item{\link[DBI]{dbGetRowsAffected}}{\code{signature(res = "MySQLResult")}: ... }
    \item{\link[DBI]{dbGetStatement}}{\code{signature(res = "MySQLResult")}: ... }
    \item{\link[DBI]{dbHasCompleted}}{\code{signature(res = "MySQLResult")}: ... }
    \item{\link[DBI]{dbListFields}}{\code{signature(conn = "MySQLResult", name = "missing")}: ... }
    \item{\link[DBI]{fetch}}{\code{signature(res = "MySQLResult", n = "numeric")}: ... }
    \item{\link[DBI]{fetch}}{\code{signature(res = "MySQLResult", n = "missing")}: ... }
    \item{\link{summary}}{\code{signature(object = "MySQLResult")}: ... }
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

% $Id$
\name{MySQLConnection-class}
\docType{class}
\alias{MySQLConnection-class}
\title{Class MySQLConnection}
\description{MySQLConnection class.}
\section{Generators}
    The method \code{\link[DBI]{dbConnect}} is the main generator.
}
\section{Extends}{
Class \code{"DBIConnection"}, directly.
Class \code{"MySQLObject"}, directly.
Class \code{"DBIObject"}, by class "DBIConnection".
Class \code{"dbObjectId"}, by class "MySQLObject".
}
\section{Methods}{
  \describe{
    \item{\link{coerce}}{\code{signature(from = "MySQLConnection", to = "MySQLResult")}: ... }
    \item{\link[DBI]{dbCallProc}}{\code{signature(conn = "MySQLConnection")}: ... }
    \item{\link[DBI]{dbCommit}}{\code{signature(conn = "MySQLConnection")}: ... }
    \item{\link[DBI]{dbConnect}}{\code{signature(drv = "MySQLConnection")}: ... }
    \item{\link[DBI]{dbDisconnect}}{\code{signature(conn = "MySQLConnection")}: ... }
    \item{\link[DBI]{dbExistsTable}}{\code{signature(conn = "MySQLConnection", name = "character")}: ... }
    \item{\link[DBI]{dbGetException}}{\code{signature(conn = "MySQLConnection")}: ... }
    \item{\link[DBI]{dbGetInfo}}{\code{signature(dbObj = "MySQLConnection")}: ... }
    \item{\link[DBI]{dbGetQuery}}{\code{signature(conn = "MySQLConnection", statement = "character")}: ... }
    \item{\link[DBI]{dbListFields}}{\code{signature(conn = "MySQLConnection", name = "character")}: ... }
    \item{\link[DBI]{dbListResults}}{\code{signature(conn = "MySQLConnection")}: ... }
    \item{\link[DBI]{dbListTables}}{\code{signature(conn = "MySQLConnection")}: ... }
    \item{\link[DBI]{dbReadTable}}{\code{signature(conn = "MySQLConnection", name = "character")}: ... }
    \item{\link[DBI]{dbRemoveTable}}{\code{signature(conn = "MySQLConnection", name = "character")}: ... }
    \item{\link[DBI]{dbRollback}}{\code{signature(conn = "MySQLConnection")}: ... }
    \item{\link[DBI]{dbSendQuery}}{\code{signature(conn = "MySQLConnection", statement = "character")}: ... }
    \item{\link[DBI]{dbWriteTable}}{\code{signature(conn = "MySQLConnection", name = "character", value = "data.frame")}: ... }
    \item{summary}{\code{signature(object = "MySQLConnection")}: ... }
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
drv <- dbDriver("MySQL)
con <- dbConnect(drv, dbname = "rsdbi.db")
}
}
\keyword{database}
\keyword{interface}
\keyword{classes}
% vim: syntax=tex

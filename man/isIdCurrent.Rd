% $Id$
\name{isIdCurrent}
\alias{isIdCurrent}
\title{
  Check whether a database handle object is valid or not
}
\description{
Support function that verifies that an object holding a reference
to a foreign object is still valid for communicating with the RDBMS
}
\usage{
isIdCurrent(obj)
}
\arguments{
\item{obj}{
any \code{dbObject} (e.g., \code{dbDriver},
\code{dbConnection}, \code{dbResult}).
}
}
\value{
a logical scalar.
}
\details{
\code{dbObjects} are R/S-Plus remote references to foreign objects.
This introduces differences to the object's semantics such as
persistence (e.g., connections may be closed unexpectedly),
thus this function provides a minimal verification to ensure 
that the foreign object being referenced can be contacted.
}
\seealso{
\code{\link[DBI]{dbDriver}}
\code{\link[DBI]{dbConnect}}
\code{\link[DBI]{dbSendQuery}}
\code{\link[DBI]{fetch}}
}
\examples{\dontrun{
cursor <- dbSendQuery(con, sql.statement)
isIdCurrent(cursor)
}
}
\keyword{interface}
\keyword{database}
% docclass is function
% vim: syntax=tex

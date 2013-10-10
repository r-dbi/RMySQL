\name{dbEscapeStrings}
\alias{dbEscapeStrings}
\title{Escape SQL-special characters in strings}
\description{
  is expected to be sorted by the grouping field.
}
\usage{
dbEscapeStrings(con, strings, ...)
}
\arguments{
  \item{con}{a connection object (see \code{\link[DBI]{dbConnect}}).}
  \item{strings}{a character vector.}
  \item{...}{any additional arguments to be passed to the dispatched method.}
}
\details{
  \code{dbEscapeStrings} 

  Currently, only the \code{\link{MySQL}} driver implements this method.
}
\value{
  A character vector with SQL special characters properly escaped.
}

\seealso{
   \code{\link{MySQL}}
   \code{\link[DBI]{dbSendQuery}}
   \code{\link[DBI]{fetch}}
}

\examples{\dontrun{
tmp <- sprintf("select * from emp where lname = \%s", "O'Reilly")
sql <- dbEscapeString(con, tmp)
dbGetQuery(con, sql)
}
}
\keyword{programming}% at least one, from doc/KEYWORDS
\keyword{interface}% __ONLY ONE__ keyword per line
\keyword{database}
% vim: syntax=tex

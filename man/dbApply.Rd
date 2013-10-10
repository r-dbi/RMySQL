\name{dbApply}
\alias{dbApply}
\title{Apply R functions to remote groups of DBMS rows (experimental)}
\description{
  Applies R functions to groups of remote DBMS rows without
  bringing an entire result set all at once.  The result set
  is expected to be sorted by the grouping field.
}
\usage{
dbApply(res, ...)
}
\arguments{
  \item{res}{a result set (see \code{\link[DBI]{dbSendQuery}}).}
  \item{...}{any additional arguments to be passed to \code{FUN}.}
}
\details{
  \code{dbApply} 
  This generic is meant to handle somewhat gracefully(?) large amounts 
  of data from the DBMS by bringing into R manageable chunks;
  the idea is that the data from individual groups can be handled by R, but
  not all the groups at the same time.  

  Currently, only the \code{\link{MySQL}} driver implements a method 
  (see the helper function \code{\link{mysqlDBApply}}) for this 
  generic function.
}
\value{
  A list with as many elements as there were groups in the
  result set.
}

\seealso{
   \code{\link{MySQL}}
   \code{\link{mysqlDBApply}}
   \code{\link[DBI]{dbSendQuery}}
   \code{\link[DBI]{fetch}}
}

\examples{\dontrun{
## compute quanitiles for each network agent
con <- dbConnect(MySQL(), group="vitalAnalysis")
rs <- dbSendQuery(con, 
             "select Agent, ip_addr, DATA from pseudo_data order by Agent")
out <- dbApply(rs, INDEX = "Agent", 
        FUN = function(x, grp) quantile(x$DATA, names=FALSE))
}
}
\keyword{programming}% at least one, from doc/KEYWORDS
\keyword{interface}% __ONLY ONE__ keyword per line
\keyword{database}
% vim: syntax=tex

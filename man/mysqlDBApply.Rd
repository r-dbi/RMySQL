% $Id$
\name{mysqlDBApply}
\alias{mysqlDBApply}
\title{Apply R/S-Plus functions to remote groups of DBMS rows (experimental)}
\description{
  Applies R/S-Plus functions to groups of remote DBMS rows without
  bringing an entire result set all at once.  The result set
  is expected to be sorted by the grouping field.
}
\usage{
mysqlDBApply(res, INDEX, FUN = stop("must specify FUN"), 
         begin = NULL, 
         group.begin =  NULL, 
         new.record = NULL, 
         end = NULL, 
         batchSize = 100, maxBatch = 1e6, 
         ..., simplify = TRUE)
}
\arguments{
  \item{res}{a result set (see \code{\link[DBI]{dbSendQuery}}).}
  \item{INDEX}{a character or integer specifying the field name or
               field number that defines the various groups.}
  \item{FUN}{a function to be invoked upon identifying the last
               row from every group. This function will be passed
               a data frame holding the records of the current group, 
               a character string with the group label, plus any
               other arguments passed to \code{dbApply} as \code{"..."}.}
  \item{begin}{a function of no arguments to be invoked just prior to 
               retrieve the first row from the result set.}
  \item{end}{a function of no arguments to be invoked just after retrieving 
               the last row from the result set.}
  \item{group.begin}{a function of one argument (the group label) to be 
               invoked upon identifying a row from a new group}.
  \item{new.record}{a function to be invoked as each individual record
               is fetched.  The first argument to this function is a
               one-row data.frame holding the new record.}
  \item{batchSize}{the default number of rows to bring from the remote 
                result set. If needed, this is automatically extended
                to hold groups bigger than \code{batchSize}.}
  \item{maxBatch}{the absolute maximum of rows per group that may
                be extracted from the result set.}
  \item{...}{any additional arguments to be passed to \code{FUN}.}
  \item{simplify}{Not yet implemented}
}
\details{
  \code{dbApply} 
  This function is meant to handle somewhat gracefully(?) large amounts 
  of data from the DBMS by bringing into R manageable chunks (about 
  \code{batchSize} records at a time, but not more than \code{maxBatch}); 
  the idea is that the data from individual groups can be handled by R, but
  not all the groups at the same time.  
  
  The MySQL implementation \code{mysqlDBApply} allows us to register R 
  functions that get invoked
  when certain fetching events occur. These include the ``begin'' event
  (no records have been yet fetched), ``begin.group'' (the record just 
  fetched belongs to a new group), ``new record'' (every fetched record
  generates this event), ``group.end'' (the record just fetched was the
  last row of the current group), ``end'' (the very last record from the
  result set). Awk and perl programmers will find this paradigm very
  familiar (although SAP's ABAP language is closer to what we're doing).
}
\value{
  A list with as many elements as there were groups in the
  result set.
}
\note{This is an experimental version implemented only in R (there are
plans, time permitting, to implement it in S-Plus).

The terminology that we're using is closer to SQL than R.  In R
what we're referring to ``groups'' are the individual levels of
a factor (grouping field in our terminology).
}

\seealso{\code{\link{MySQL}}, \code{\link[DBI]{dbSendQuery}}, \code{\link[DBI]{fetch}}.}

\examples{\dontrun{
## compute quanitiles for each network agent
con <- dbConnect(MySQL(), group="vitalAnalysis")
res <- dbSendQuery(con, 
             "select Agent, ip_addr, DATA from pseudo_data order by Agent")
out <- dbApply(res, INDEX = "Agent", 
        FUN = function(x, grp) quantile(x$DATA, names=FALSE))
}
}
\keyword{programming}% at least one, from doc/KEYWORDS
\keyword{interface}% __ONLY ONE__ keyword per line
\keyword{database}
% vim: syntax=tex

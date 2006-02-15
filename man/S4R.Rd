% $Id$
\name{S4R}
\alias{ErrorClass}
\alias{usingR}

\title{R compatibility with S version 4/Splus5+ support functions}
\description{
  These objects ease the task of porting functions into R
  from S Version 4 and S-Plus 5.0 and later.  See the documentation
  there. May be obsolete in the future.
}
\usage{
usingR(major, minor)
}
\examples{\dontrun{
rc <- try(fetch(res, n = -1))
if(inherit(rc, ErrorClass))
   stop("could not fetch the data")
}
}
\keyword{internal}
% vim:syntax=tex

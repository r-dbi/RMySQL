\name{summary-methods}
\docType{methods}
\alias{coerce-methods}
\alias{summary-methods}
\alias{format-methods}
\alias{show-methods}
\alias{coerce,dbObjectId,integer-method}
\alias{coerce,MySQLObject,integer-method}
\alias{coerce,MySQLObject,numeric-method}
\alias{coerce,MySQLObject,character-method}
\alias{coerce,MySQLObject,MySQLDriver-method}
\alias{coerce,MySQLConnection,MySQLResult-method}
\alias{coerce,MySQLConnection,MySQLDriver-method}
\alias{coerce,MySQLResult,MySQLConnection-method}
\alias{coerce,MySQLResult,MySQLDriver-method}
\alias{format,MySQLObject-method}
\alias{print,MySQLObject-method}
\alias{show,MySQLObject-method}
\alias{summary,MySQLObject-method}
\alias{summary,MySQLDriver-method}
\alias{summary,MySQLConnection-method}
\alias{summary,MySQLResult-method}
\title{Summarize an MySQL object}
\description{
   These methods are straight-forward implementations of the corresponding
   generic functions.
}
\section{Methods}{\describe{

\item{object = "DBIObject"}{
   Provides relevant metadata information on \code{object}, 
   for instance, the MySQL server file, the
   SQL statement associated with a result set, etc.  
   }
\item{from}{object to be coerced}
\item{to}{coercion class}
\item{x}{object to \code{format} or \code{print} or \code{show}}
}
}
\keyword{methods}
\keyword{database}
\keyword{interface}
% vim: syntax=tex

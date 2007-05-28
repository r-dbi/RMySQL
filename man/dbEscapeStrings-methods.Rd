\name{dbEscapeStrings-methods}
\docType{methods}
\alias{dbEscapeStrings-methods}
\alias{dbEscapeStrings,MySQLConnection,character-method}
\alias{dbEscapeStrings,MySQLResult,character-method}
\title{Escape a Character Vector According to SQL rules}
\description{
  Escape SQL-special characters in a character vector according to MySQL rules.
}
\section{Methods}{
\describe{

\item{con = "MySQLConnection", strings = "character"}{ 
  This method encodes the \code{strings} character vector according
  to the MySQL escape rules and taking into consideration the character
  set used by the connection (each MySQL connection may be set to
  use different character sets).  Note that the \code{RMySQL} package
  currently does not deal with character set conversions -- it uses
  whatever character encoding the R session is using, but the MySQL
  runtime library handles this transparently.
}
\item{...}{currently unused.}
}}
\keyword{methods}
\seealso{
  \code{\link{MySQL}},
  \code{\link[DBI]{dbConnect}},
  \code{\link[DBI]{dbSendQuery}},
  \code{\link[DBI]{dbGetQuery}},
  \code{\link[DBI]{fetch}},
  \code{\link[DBI]{dbNextResult}},
  \code{\link[DBI]{dbCommit}},
  \code{\link[DBI]{dbGetInfo}},
  \code{\link[DBI]{dbReadTable}}.
}
\examples{\dontrun{
con <- dbConnect(MySQL(), 
          dbname = "rs-dbi", 
          client.flag=CLIENT\_MULTI\_STATEMENTS)
tmp <- sprintf("select * from emp where lname = %s", "O'Reilly")
sql <- dbEscapeString(con, tmp)
dbGetQuery(con, sql)
}
}
\keyword{methods}
\keyword{interface}
\keyword{database}
% vim: syntax=tex

\name{MySQL}
\alias{MySQL}
\title{
  Instantiate a MySQL client from the current R session
}
\description{
This function creates and initializes a MySQL client.
It returns an driver object that allows you to connect
to one or several MySQL servers.
}
\usage{
MySQL(max.con = 16, fetch.default.rec = 500, force.reload = FALSE)
}
\arguments{
\item{max.con }{
  maximum number of connections that are intended to have open at one time.
  There's no intrinic limit, since strictly speaking this limit applies 
  to MySQL \emph{servers}, but clients can have (at least in theory) 
  more than this.  Typically there are at most a handful of open connections,
  thus the internal \code{RMySQL} code uses a very simple linear search 
  algorithm to manage its connection table.
  }
\item{fetch.default.rec}{
  number of records to fetch at one time from the database.
  (The \code{\link[DBI]{fetch}} method uses this number as a default.)
  }
\item{force.reload}{
  should the client code be reloaded (reinitialize)?
  Setting this to \code{TRUE} allows you to change
  default settings.  Notice that all connections should be closed
  before re-loading.
  }
}
\value{
An object \code{MySQLDriver} that extends 
\code{dbDriver} and 
\code{dbObjectId}.
This object is required to create connections
to one or several MySQL database engines.
}
\section{Side Effects}{
The R client part of the database communication is initialized,
but note that connecting to the database engine needs to be done through
calls to \code{\link[DBI]{dbConnect}}.
}
\details{
This object is a singleton, that is, on subsequent invocations
it returns the same initialized object. 

This implementation allows you to connect
to multiple host servers and run multiple connections on each server
simultaneously.
}
\section{User authentication}{
The preferred method to pass authentication parameters to the
server (e.g., user, password, host) is through the MySQL personal
configuration file \file{\$HOME/.my.cnf} (or \file{c:/my.cnf} under Windows).
Since specifying passwords on calls to 
\code{dbConnect}
is a very bad idea (and so is specifying passwords through
shell variables),
the client code parses the configuration file \file{\$HOME/.my.cnf};
this file consists of zero or more sections, each starting with a 
line of the form \code{[section-name]}, for instance

\$ cat \$HOME/.my.cnf\cr
\# this is a comment\cr
[client]\cr
user = dj\cr
host = localhost\cr
\cr
[rs-dbi]\cr
database = s-data\cr
\cr
[lasers]\cr
user = opto\cr
database = opto\cr
password = pure-light\cr
host = merced\cr
...\cr
[iptraffic]\cr
host = data\cr
database = iptraffic\cr
\cr
This file should be readeable only by you. 
Inside each section, MySQL parameters may be specified one per line
(e.g., \code{user = opto}).  
MySQL always considers default options from  the \code{[client]} group
for connecting to a server. To override or add additional options, R MySQL
combines default options from the \code{[rs-dbi]} group, but you may specifiy you own
group in the \code{dbConnect} call to tailor your environment. Note that 
to override options, you must place your group after the \code{[client]} group
in configuration file.

For instance, if you define a group, say, \code{[iptraffic]},
then instead of including all these parameters in the
call to \code{dbConnect}, you simply supply the
name of the \code{group},
e.g., \code{dbConnect(mgr, group = "iptraffic")}.

The most important parameters are \code{user}, 
\code{password}, \code{host}, 
and \code{dbname}.
}

\note{
Use the option \code{database} in place of\code{dbname} in configuration files.
}
\author{David A. James}
\section{References}{
See \url{stat.bell-labs.com/RS-DBI} 
for more details on the R/S-Plus database interface.

See the documentation at the MySQL Web site
\url{http://www.mysql.com} for details.
}
\seealso{
On database managers:

\code{\link[DBI]{dbDriver}}
\code{\link[DBI]{dbUnloadDriver}}

On connections, SQL statements and resultSets:

\code{\link[DBI]{dbConnect}}
\code{\link[DBI]{dbDisconnect}}
\code{\link[DBI]{dbSendQuery}}
\code{\link[DBI]{dbGetQuery}}
\code{\link[DBI]{fetch}}
\code{\link[DBI]{dbClearResult}}

On transaction management:

\code{\link[DBI]{dbCommit}}
\code{\link[DBI]{dbRollback}}

On meta-data:

\code{\link{summary}}
\code{\link[DBI]{dbGetInfo}}
\code{\link[DBI]{dbGetDBIVersion}}
\code{\link[DBI]{dbListTables}}
\code{\link[DBI]{dbListConnections}}
\code{\link[DBI]{dbListResults}}
\code{\link[DBI]{dbColumnInfo}}
\code{\link[DBI]{dbGetException}}
\code{\link[DBI]{dbGetStatement}}
\code{\link[DBI]{dbHasCompleted}}
\code{\link[DBI]{dbGetRowCount}}
}
\examples{\dontrun{
# create a MySQL instance and create one connection.
> m <- dbDriver("MySQL")
<MySQLDriver:(4378)>

# open the connection using user, passsword, etc., as
# specified in the "[iptraffic]" section of the 
# configuration file \file{\$HOME/.my.cnf}
> con <- dbConnect(m, group = "iptraffic")
> rs <- dbSendQuery(con, "select * from HTTP_ACCESS where IP_ADDRESS = '127.0.0.1'")
> df <- fetch(rs, n = 50)
> dbHasCompleted(rs)
[1] FALSE
> df2 <- fetch(rs, n = -1)
> dbHasCompleted(rs)
[1] TRUE
> dbClearResult(rs)
> dim(dbGetQuery(con, "show tables"))
[1] 74   1
> dbListTables(con)    
}
}
\keyword{interface}
\keyword{database}
% vim: syntax=tex

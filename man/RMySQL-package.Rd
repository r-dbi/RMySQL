\name{RMySQL-package}
\alias{RMySQL-package}
\alias{RMySQL}
\docType{package}
\title{
R interface to the MySQL database
}
\description{
The functions in this package allow you interact with
one or more MySQL databases from R.
}

\section{Overview}{
A typical usage of the R-MySQL interface is:
\enumerate{
\item Connect and authenticate to one or more MySQL databases:
   \preformatted{
   con <- dbConnect(MySQL(), group = "lasers")
   con2 <- dbConnect(MySQL(), user="opto", password="pure-light", 
                     dbname="lasers", host="merced")
   }
\item List tables and fields in a table:
   \preformatted{
   dbListTables(con)
   dbListFields(con, "table\_name")
   }
\item Import and export data.frames:
   \preformatted{
   d <- dbReadTable(con, "WL")
   dbWriteTable(con, "WL2", a.data.frame)         ## table from a data.frame
   dbWriteTable(con, "test2", "~/data/test2.csv") ## table from a file
   }
\item Run an arbitrary SQL statement and extract all its output (returns
      a data.frame):
   \preformatted{
   dbGetQuery(con, "select count(*) from a\_table")
   dbGetQuery(con, "select * from a\_table")
   }
\item Run an SQL statement and extract its output in pieces (returns a
      result set):
   \preformatted{
   rs <- dbSendQuery(con, "select * from WL where width\_nm between 0.5 and 1")
   d1 <- fetch(rs, n = 10000)
   d2 <- fetch(rs, n = -1
   }
\item Get meta-information on a connection (thread-id, etc.):
   \preformatted{
   summary(MySQL(), verbose = TRUE)
   summary(con, verbose = TRUE)
   summary(rs, verbose = TRUE)
   dbListConnections(MySQL())
   dbListResultSets(con)
   dbHasCompleted(rs)
   }
\item Close connections:
   \preformatted{
   dbDisconnect(con)
   dbDisconnect(con2)
   }
}
}

\section{Data mappings between MySQL and R}{
MySQL tables are read into R as data.frames, but without coercing
character or logical data into factors.  Similarly while exporting 
data.frames, factors are exported as character vectors.

Integer columns are usually imported as R integer vectors, except
for cases such as \code{BIGINT} or \code{UNSIGNED INTEGER}
which are coerced to R's \code{double} precision vectors to avoid 
truncation (currently R's integers are signed 32-bit quantities).

Time variables are imported/exported as character data, so you need 
to convert these to your favorite date/time representation.

Currently there are no facilities to import/export \code{BLOBs}.
}

\section{RDBMS tables, data.frames, and data types}{
Tables in a relational database are only superficially similar 
to R's data.frames (e.g., tables as unordered sets of rows compared
to data.frames as ordered sets, tables having referential constraints, 
indexes, and so on.)  
}

\section{User authentication}{
Although you can specify user authentication parameters 
(user, password, database, and host) in
the call to \code{dbConnect}, the preferred method to pass 
these parameters to the server is through a MySQL 
\code{default.file}, e.g., \file{\$HOME/.my.cnf} (or \file{c:/my.cnf} 
under Windows).
The MySQL \code{dbConnect} method parses the 
\code{default.file=\$HOME/.my.cnf} to initialize connections to
MySQL databases.
This file consists of zero or more named sections
each starting with a line of the form \code{[section-name]};
each section includes zero or more MySQL variable declaration per line, 
such as, \code{user=}, \code{password=}, \code{host=}, etc.
For instance,

\preformatted{
$ cat $HOME/.my.cnf
# this is a comment
; this is also a comment
[client]
user = dj
host = localhost

[rs-dbi]
database = s-data

[lasers]
user = opto
database = opto
password = pure-light
host = merced
...
[iptraffic]
host = data
database = iptraffic
}

This file should be readable only by you.  \code{RMySQL}
always initializes connection values from the \code{[client]} and
\code{[rs-dbi]} sections, but you may define you own project-specific
sections (as in the example above) to tailor its environment; 
if the same parameter appears in multiple sections (e.g., in \code{client} 
and \code{rs-dbi}), the last (closer to the bottom) occurrence is used.

If you define a section, for instance, \code{[iptraffic]},
then instead of including all these parameters in the
call to \code{dbConnect}, you simply supply the
name of the \code{group},
e.g., \code{dbConnect(MySQL(), group = "iptraffic")}.

In addition to \code{user}, \code{password}, \code{host}, and 
\code{dbname}, you may specify any other connection parameters,
e.g., \code{port}, \code{socket}.  See the MySQL documentation
for details.

Lastly, you may specify an alternate \code{default.file}, e.g.,
\code{dbConnect(MySQL(), group="iptraffic", default.file="router\_shield")}.
}

\author{
David A. James <dj@bell-labs.com> 
Saikat DebRoy <saikat@stat.wisc.edu>
}

\section{References}{
See \url{stat.bell-labs.com/RS-DBI} 
for more details on the R/S-Plus database interface.

See the documentation at the MySQL Web site
\url{http://www.mysql.com} for details.
}
\seealso{
On database managers:

\code{\link[DBI:DBI-package]{DBI}} 
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
\code{\link[DBI]{dbGetAffectedRows}}
}
\examples{\dontrun{
# create a MySQL instance and create one connection.
> m <- dbDriver("MySQL")  ## or MySQL()
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
\keyword{package}
\keyword{interface}
\keyword{database}
% vim: syntax=tex

% $Id$
\name{dbReadTable-methods}
\docType{methods}
\alias{dbReadTable-methods}
\alias{dbWriteTable-methods}
\alias{dbExistsTable-methods}
\alias{dbRemoveTable-methods}
\alias{dbReadTable,MySQLConnection,character-method}
\alias{dbWriteTable,MySQLConnection,character,data.frame-method}
\alias{dbExistsTable,MySQLConnection,character-method}
\alias{dbRemoveTable,MySQLConnection,character-method}
\title{
  Convenience functions for Importing/Exporting DBMS tables
}
\description{
  These functions mimic their R/S-Plus counterpart 
  \code{get},
  \code{assign},
  \code{exists}, 
  \code{remove}, and
  \code{objects},
  except that they generate code that gets remotely executed
  in a database engine.  
}
\section{Methods}{\describe{
\item{conn}{
  an \code{MySQLConnection} database connection object.
  }
\item{name}{
  a character string specifying a table name.
  }
\item{value}{
  a data.frame (or coercible to data.frame).
  }
\item{row.names}{
  in the case of \code{dbReadTable}, this argument can be a string or
  an index specifying the column in the DBMS table to be used as 
  \code{row.names} in the output data.frame (a \code{NULL}, \code{""}, or 0 
  specifies that no column should be used as \code{row.names} in the output).

  In the case of \code{dbWriteTable}, this argument should be a logical
  specifying whether the \code{row.names} should be output to the
  output DBMS table; if \code{TRUE}, an extra field whose name will be 
  whatever the R/S-Plus identifier \code{"row.names"} maps to the DBMS
  (see \code{\link[DBI]{make.db.names}}).
  }
\item{overwrite}{
  a logical specifying whether to overwrite an existing table
  or not.  Its default is \code{FALSE}.
  }
\item{append}{
  a logical specifying whether to append to an existing table
  in the DBMS.  Its default is \code{FALSE}.
  }
\item{allow.keywords}{
   \code{dbWriteTable} accepts a logical \code{allow.keywords} 
    to allow or prevent MySQL reserved identifiers to be used as 
    column names. By default it is \code{FALSE}.
   }
}
}

\value{
   A \code{data.frame} in the case of \code{dbReadTable}; otherwise a logical 
   indicating whether the operation was successful.  
}
\note{
   Note that data.frames are only approximately analogous to tables
   (relations) in relational DBMS, and thus you should not expect
   complete agreement in their semantics.  Tables in RDBMS are
   best thought of as \emph{relations} with a number of constraints
   imposed by the relational database model, and data.frames, with
   their roots in statistical modeling, as self-contained "sequence
   of observations on some chosen variables" (Chambers and Hastie
   (1992), p.46). 
   In particular the \code{data.frame} returned by \code{dbReadTable}
   only has primitive data, e.g., it does not coerce character data
   to factors.
   Also, column names in a data.frame are \emph{not} guaranteed to be 
   equal to the column names in a MySQL exported/imported table
   (e.g., by default MySQL reserved identifiers may not be used
   as column names --- and with 218 keywords like \code{"BEFORE"},
   \code{"DESC"}, and \code{"FROM"} the likelihood of name conflicts
   is not small.) Use \code{isSQLKeyword(con, names(value))} to check
   whether the data.frame names in \code{value} coincide with 
   MySQL reserver words.

   MySQL table names are \emph{not} case sensitive, e.g., table
   names \code{ABC} and \code{abc} are considered equal.  
}
\references{
   See the Database Interface definition document
   \code{DBI.pdf} in the base directory of this package
   or \url{http://stat.bell-labs.com/RS-DBI}.
}
\seealso{
  \code{\link{MySQL}},
  \code{\link[DBI]{isSQLKeyword}},
  \code{\link[DBI]{dbDriver}},
  \code{\link[DBI]{dbConnect}},
  \code{\link[DBI]{dbSendQuery}},
  \code{\link[DBI]{dbGetQuery}},
  \code{\link[DBI]{fetch}},
  \code{\link[DBI]{dbCommit}},
  \code{\link[DBI]{dbGetInfo}},
  \code{\link[DBI]{dbListTables}},
  \code{\link[DBI]{dbReadTable}}.
}
\examples{\dontrun{
conn <- dbConnect("MySQL", group = "wireless")
if(dbExistsTable(con, "fuel_frame")){
   dbRemoveTable(conn, "fuel_frame")
   dbWriteTable(conn, "fuel_frame", fuel.frame)
}
if(dbExistsTable(conn, "RESULTS")){
   dbWriteTable(conn, "RESULTS", results2000, append = T)
else
   dbWriteTable(conn, "RESULTS", results2000)
}
}
}
\keywords{methods}
\keyword{interface}
\keyword{database}
% vim: syntax=tex

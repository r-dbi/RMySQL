% $Id$
\name{mysqlSupport}
\alias{mysqlInitDriver}            % driver-related function
\alias{mysqlDriverInfo}
\alias{mysqlDescribeDriver}
\alias{mysqlCloseDriver}           % connection-related 
\alias{mysqlNewConnection}
\alias{mysqlCloneConnection}
\alias{mysqlConnectionInfo}
\alias{mysqlDescribeConnection}
\alias{mysqlCloseConnection}
\alias{mysqlExecStatement}         % result-related
\alias{mysqlFetch}
\alias{mysqlQuickSQL}
\alias{mysqlResultInfo}
\alias{mysqlDescribeResult}
\alias{mysqlCloseResult}
\alias{mysqlDescribeFields}
\alias{mysqlReadTable}
\alias{mysqlWriteTable}            % convenience functions
\alias{mysqlDataType}
\alias{.MySQLPkgName}              % constants
\alias{.MySQLPkgVersion}
\alias{.MySQLPkgRCS}
\alias{.MySQL.NA.string}
\alias{.MySQLSQLKeywords}
\alias{.conflicts.OK}
%\non_function{}
\title{Support Functions}
\description{
   These functions are the workhorse behind the RMySQL package, but
   users need not invoke these directly.  For details see \code{\link{MySQL}}.
}
\usage{
   ## MySQLDriver-related
   mysqlInitDriver(max.con=16, fetch.default.rec = 500, force.reload=FALSE)
   mysqlDriverInfo(obj, what, ...)
   mysqlDescribeDriver(obj, verbose = FALSE, ...)
   mysqlCloseDriver(drv, ...)

   ## MySQLConnection-related
   mysqlNewConnection(drv, dbname, username, password, host, unix.socket,
         port, client.flag, groups)
   mysqlCloneConnection(con, ...)
   mysqlConnectionInfo(obj, what, ...)
   mysqlDescribeConnection(obj, verbose = FALSE, ...)
   mysqlCloseConnection(con, ...)

   ## MySQLResult-related
   mysqlExecStatement(con, statement)
   mysqlFetch(res, n=0, ...)
   mysqlQuickSQL(con, statement)
   mysqlResultInfo(obj, what, ...)
   mysqlDescribeResult(obj, verbose = FALSE, ...)
   mysqlCloseResult(res, ...)
   mysqlDescribeFields(res, ...)

   ## data mappings, convenience functions, and extensions
   mysqlDataType(obj, ...)
   mysqlReadTable(con, name, row.names = "row.names", check.names = TRUE, ...)
   mysqlWriteTable(con, name, value, field.types, row.names = TRUE, 
      overwrite=FALSE, append=FALSE, ..., allow.keywords = FALSE)
}
\arguments{
\item{max.con}{
   positive integer specifying maximum number of open connections.
   The current default of 10 is hardcoded in the C code.
}
\item{fetch.default.rec}{
   default number of rows to fetch (move to R/S-Plus).  This default is
   used in \code{mysqlFetch}. 
   The default is 500.
}
\item{force.reload}{
   logical indicating whether to re-initialize the driver.  This may be
   useful if you want to change the defaults (e.g., \code{fetch.default.rec}).
   Note that the driver is a singleton (subsequent inits just returned the
   previously initialized driver, thus this argument).
}
\item{obj}{
   any of the MySQL DBI objects (e.g., \code{MySQLConnection},
   \code{MySQLResult}).
}
\item{what}{
   character vector of metadata to extract, e.g., "version", "statement",
   "isSelect".
}
\item{verbose}{
   logical controlling how much information to display.
   Defaults to \code{FALSE}.
}
\item{drv}{
   an \code{MySQLDriver} object as produced by \code{mysqlInitDriver}.
}
\item{con}{
   an \code{MySQLConnection} object as produced by \code{mysqlNewConnection}
   and \code{mysqlCloneConnection}.
}
\item{res}{
   an \code{MySQLResult} object as produced by by \code{mysqlExecStatement}.
}
\item{username}{
   a character string with the MySQL's user name.  
}
\item{password}{
   character string with the MySQL's password.
}
\item{groups}{
   character vector with one or more MySQL group names.  For details
   see \code{\link{MySQL}}.
}
\item{dbname}{
   character string with the MySQL database name.
}
\item{host}{
   character string with the name (or IP address) of the machine
   hosting the database. Default is \code{""}, which is
   interpreted as \code{localhost} by the MySQL's API.
}
\item{unix.socket}{
   (optional) character string with a filename for the socket
   file name.  Consult the MySQL documentation for details.
}
\item{port}{
   (optional) positive integer specifying the TCP port number that
   the MySQL server is listening to.  Consult the MySQL documentation 
   for details.
}
\item{client.flag}{
   (optional) integer setting flags for the client.  Consult the
   MySQL documentation for details.
}
\item{force}{
   logical indicating whether to close a connection that has open
   result sets.  The default is \code{FALSE}.
}
\item{statement}{
   character string holding one (and only one) SQL statement.
}
\item{n}{
   number of rows to fetch from the given result set. A value of -1
   indicates to retrieve all the rows.  The default of 0 specifies
   to extract whatever the \code{fetch.default.rec} was specified
   during driver initialization \code{mysqlInit}.
}
\item{name}{
   character vector of names (table names, fields, keywords).
}
\item{value}{
   a data.frame.
}
\item{field.types}{
   a list specifying the mapping from R/S-Plus fields in the data.frame
   \code{value} to SQL data types.  The default is
   \code{sapply(value,SQLDataType)}, see \code{MySQLSQLType}.
}
\item{row.names}{
   a logical specifying whether to prepend the \code{value} data.frame 
   row names or not.  The default is \code{TRUE}.
}
\item{check.names}{
   a logical specifying whether to convert DBMS field names into
   legal S names. Default is \code{TRUE}.
}
\item{overwrite}{
   logical indicating whether to replace the table \code{name} with
   the contents of the data.frame \code{value}.
   The defauls is \code{FALSE}.
}
\item{append}{
   logical indicating whether to append \code{value} to the existing
   table \code{name}.
}
\item{allow.keywords}{
   logical indicating whether column names that happen to be MySQL
   keywords be used as column names in the resulting relation (table)
   being written.  Defaults to \code{FALSE}, forcing \code{mysqlWriteTable}
   to modify column names to make them legal MySQL identifiers.
   }
\item{\dots}{
   placeholder for future use.
}
}
\value{
   \code{mysqlInitDriver} returns an \code{MySQLDriver} object.

   \code{mysqlDriverInfo} returns a list of name-value metadata pairs.

   \code{mysqlDescribeDriver} returns \code{NULL} (displays the object's 
   metadata).

   \code{mysqlCloseDriver} returns a logical indicating whether the 
   operation succeeded or not.

   \code{mysqlNewConnection} returns an \code{MySQLConnection} object.

   \code{mysqlCloneConnection} returns an \code{MySQLConnection} object.

   \code{mysqlConnectionInfo}returns a list of name-value metadata pairs.

   \code{mysqlDescribeConnection} returns \code{NULL} (displays the 
   object's metadata).

   \code{mysqlCloseConnection} returns a logical indicating whether the 
   operation succeeded or not.

   \code{mysqlExecStatement} returns an \code{MySQLResult} object.

   \code{mysqlFetch} returns a data.frame.

   \code{mysqlQuickSQL} returns either a data.frame if the \code{statement} is
   a \code{select}-like or NULL otherwise.

   \code{mysqlDescribeResult} returns \code{NULL} (displays the object's 
   metadata).
   
   \code{mysqlCloseResult} returns a logical indicating whether the 
   operation succeeded or not.
   
   \code{mysqlDescribeFields} returns a data.frame with one row per field
   with columns \code{name}, \code{Sclass}, \code{type}, \code{len}, 
   \code{precision}, \code{scale}, and \code{nullOK} which fully describe
   each field in a result set.  Except for \code{Sclass} (which shows the
   mapping of the field type into an R/S-Plus class) all the information
   pertains to MySQL's data storage attributes.

   \code{mysqlReadTable}  returns a data.frame with the contents of the
   DBMS table.
   
   \code{mysqlWriteTable}  returns a logical indicating whether the 
   operation succeeded or not.
   
   \code{mysqlDataType} retuns a character string with the closest
   
   \code{mysqlResultInfo} returns a list of name-value metadata pairs.
}
\section{Constants}{
\code{.MySQLPkgName} (currently \code{"RMySQL"}),
\code{.MySQLPkgVersion} (the R package version),
\code{.MySQLPkgRCS} (the RCS revision),
\code{.MySQL.NA.string} (character that MySQL uses to 
denote \code{NULL} on input),
\code{.MySQLSQLKeywords} (a lot!)
\code{.conflicts.OK}.
%\non_function{}
}
\keyword{datasets}
\keyword{interface}
\keyword{database}
%\keyword{internal}
% vim:syntax=tex

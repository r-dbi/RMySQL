\name{mysqlSupport}
\alias{.MySQLPkgName}              % constants
\alias{.MySQLPkgVersion}
\alias{.MySQLPkgRCS}
\alias{.MySQL.NA.string}
\alias{.MySQLSQLKeywords}
\alias{CLIENT_LONG_PASSWORD}     % new more secure passwords
\alias{CLIENT_FOUND_ROWS}        % Found instead of affected rows
\alias{CLIENT_LONG_FLAG}         % Get all column flags
\alias{CLIENT_CONNECT_WITH_DB}   % One can specify db on connect
\alias{CLIENT_NO_SCHEMA}         % Don't allow database.table.column
\alias{CLIENT_COMPRESS}          % Can use compression protocol
\alias{CLIENT_ODBC}              % Odbc client
\alias{CLIENT_LOCAL_FILES}       % Can use LOAD DATA LOCAL
\alias{CLIENT_IGNORE_SPACE}      % Ignore spaces before '('
\alias{CLIENT_PROTOCOL_41}       % New 4.1 protocol
\alias{CLIENT_INTERACTIVE}       % This is an interactive client
\alias{CLIENT_SSL}               % Switch to SSL after handshake
\alias{CLIENT_IGNORE_SIGPIPE}    % IGNORE sigpipes
\alias{CLIENT_TRANSACTIONS}      % Client knows about transactions
\alias{CLIENT_RESERVED}          % Old flag for 4.1 protocol
\alias{CLIENT_SECURE_CONNECTION} % New 4.1 authentication
\alias{CLIENT_MULTI_STATEMENTS}  % Enable/disable multi-stmt support
\alias{CLIENT_MULTI_RESULTS}     % Enable/disable multi-results


%\non_function{}
\title{Support Functions}
\description{
   These functions are the workhorse behind the RMySQL package, but
   users need not invoke these directly.  For details see \code{\link{MySQL}}.
}
\section{Constants}{
\code{.MySQLPkgName} (currently \code{"RMySQL"}),
\code{.MySQLPkgVersion} (the R package version),
\code{.MySQLPkgRCS} (the RCS revision),
\code{.MySQL.NA.string} (character that MySQL uses to
denote \code{NULL} on input),
\code{.MySQLSQLKeywords} (a lot!)
%\non_function{}
}
\keyword{datasets}
\keyword{interface}
\keyword{database}
%\keyword{internal}
% vim:syntax=tex

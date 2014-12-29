## Copyright (C) 1999 The Omega Project for Statistical Computing.
##
## This library is free software; you can redistribute it and/or
## modify it under the terms of the GNU General Public License
## as published by the Free Software Foundation; either
## version 2 of the License, or (at your option) any later version.
##
## This library is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
## General Public License for more details.
##
## You should have received a copy of the GNU General Public
## License along with this library; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA

#' Constants
#'
#' @aliases .MySQLPkgName .MySQLPkgVersion .MySQLPkgRCS
#' .MySQLSQLKeywords CLIENT_LONG_PASSWORD CLIENT_FOUND_ROWS CLIENT_LONG_FLAG
#' CLIENT_CONNECT_WITH_DB CLIENT_NO_SCHEMA CLIENT_COMPRESS CLIENT_ODBC
#' CLIENT_LOCAL_FILES CLIENT_IGNORE_SPACE CLIENT_PROTOCOL_41 CLIENT_INTERACTIVE
#' CLIENT_SSL CLIENT_IGNORE_SIGPIPE CLIENT_TRANSACTIONS CLIENT_RESERVED
#' CLIENT_SECURE_CONNECTION CLIENT_MULTI_STATEMENTS CLIENT_MULTI_RESULTS
#' @section Constants: \code{.MySQLPkgName} (currently \code{"RMySQL"}),
#' \code{.MySQLPkgVersion} (the R package version), \code{.MySQLPkgRCS} (the
#' RCS revision), \code{.MySQLSQLKeywords} (a lot!)
#' @name constants
NULL

## The following client flags were copied from mysql_com.h (version 4.1.13)
## but it may not make sense to set some of this from RMySQL.

#' @export
CLIENT_LONG_PASSWORD <-   1    # new more secure passwords
#' @export
CLIENT_FOUND_ROWS    <-   2    # Found instead of affected rows
#' @export
CLIENT_LONG_FLAG     <-   4    # Get all column flags
#' @export
CLIENT_CONNECT_WITH_DB <- 8    # One can specify db on connect
#' @export
CLIENT_NO_SCHEMA     <-  16    # Don't allow database.table.column
#' @export
CLIENT_COMPRESS      <-  32    # Can use compression protocol
#' @export
CLIENT_ODBC          <-  64    # Odbc client
#' @export
CLIENT_LOCAL_FILES   <- 128    # Can use LOAD DATA LOCAL
#' @export
CLIENT_IGNORE_SPACE  <- 256    # Ignore spaces before '('
#' @export
CLIENT_PROTOCOL_41   <- 512    # New 4.1 protocol
#' @export
CLIENT_INTERACTIVE   <- 1024   # This is an interactive client
#' @export
CLIENT_SSL           <- 2048   # Switch to SSL after handshake
#' @export
CLIENT_IGNORE_SIGPIPE <- 4096  # IGNORE sigpipes
#' @export
CLIENT_TRANSACTIONS <- 8192    # Client knows about transactions
#' @export
CLIENT_RESERVED     <- 16384   # Old flag for 4.1 protocol
#' @export
CLIENT_SECURE_CONNECTION <- 32768 # New 4.1 authentication
#' @export
CLIENT_MULTI_STATEMENTS  <- 65536 # Enable/disable multi-stmt support
#' @export
CLIENT_MULTI_RESULTS     <- 131072 # Enable/disable multi-results

setOldClass("data.frame")      ## to appease setMethod's signature warnings...

setGeneric("summary")
setGeneric("format")

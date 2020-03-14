# Version 0.10.20

 * Fixed improper escaping with dbQuoteString and dbQuoteLiteral, which can lead to SQL injection (Found and patched by Andrew Kane, CVE-2020-10380).

# Version 0.10.19

 * Fix new warning for CRAN
 * Small tweaks for configure script

# Version 0.10.18

 * MacOS: CRAN binaries have been updated to mariadb-connector-c to 3.1.6

# Version 0.10.17

 * Fix rcheck problems requested by CRAN

# Version 0.10.16

 * Windows: Update mariadb-connector-c to v2.3.7 + OpenSSL 1.1.1
 * MacOS: Update mariadb-connector-c to 3.0.8
 * Fix for MySQL 8 (issue #223)

# Version 0.10.15

 * Windows: Update mariadbclient to v 2.3.5

# Version 0.10.14

 * Windows: Update mariadbclient to v 2.3.4
 * Fix headers and linker flags for mariadb-connector-c 3.0 compatibility
 * Remove some redundant include headers

# Version 0.10.13

 * Add cleanup script (requested by CRAN)
 * Fix PROTECT() bugs from rchk

# Version 0.10.12

 * Windows: update libmariadbclient to v2.3.3 (fixes a timeout bug)
 
# Version 0.10.11

 * Fix for OSX Mavericks
 
# Version 0.10.10
 
 * Window: update libmariadbclient to v2.3.2
 
 * OSX: extract autobrew script

# Version 0.10.9

 * OSX binary packages for OSX now use mariadb-connector-c instead of mysql-connector-c
 
 * CMD check fix for DBI 0.4

# Version 0.10.8

 * Replace #include S.h with R.h as required by BDR 

# Version 0.10.6

 * Fix configure script for OSX

# Version 0.10.5

 * Refactor configure script
 
 * Fix for setInternet2 in R-devel on Windows

# Version 0.10.4

 *  Fix dbWriteTable bug with MySQL-based Infobright database
 
 *  Use mariadb_config if available
 
 *  Properly import used utils functions

# Version 0.10.3

 *  Fix the RPostgreSQL conflict
 
 *  Remove the workaround for the bug in TSMySQL
 
 *  Update URL to MariaDB client in DESCRIPTION

# Version 0.10.2
 
 *  Backport fixes from Rcpp branch.
 
 *  Configure script now uses `mysql_config` if available.

 *  MariaDB windows libraries have been updated to 2.1.0

 *  Add SSL support to MariaDB windows libraries.

# Version 0.10.1

 *  Fix configure script for OSX 10.6 Snow Leopard
 
 *  Issue in `dbWriteTable()` with temporary files on Windows fixed.

# Version 0.10

 *  New maintainer: Jeroen Ooms

 *  Internal changes to support static linking on Windows; this means that
    windows a binary is now available on CRAN.

 *  The following internal functions are no longer exported: please
    use the corresponding DBI generic instead:

    `mysqlInitDriver`, `mysqlCloseDriver`, `mysqlDescribeDriver`,
    `mysqlDriverInfo`, `mysqlNewConnection`, `mysqlCloneConnection`,
    `mysqlDescribeConnection`, `mysqlConnectionInfo`, `mysqlCloseConnection`,
    `mysqlExecStatement`, `mysqlQuickSQL`, `mysqlDBApply`, `mysqlFetch`,
    `mysqlResultInfo`, `mysqlDescribeResult`, `mysqlDescribeFields`,
    `mysqlCloseResult`, `mysqlImportFile`, `mysqlReadTable`, `mysqlWriteTable`,
    `mysqlEscapeStrings`, `mysqlDataType`, `safe.write`.

 *  RMySQL gains transaction support with `dbBegin()`, `dbCommit()`,
    and `dbRollback()`, but note that MySQL does not allow data definition
    language statements to be rolled back.

 *  The MySQLObject base class has been removed - there is no real shared
    behaviour for MySQLDriver, MySQLConnection and MySQLResult so this
    simplifies the code

 *  Implemented methods for `dbIsValid()`; please use these instead of
    `isIdCurrent()`.

 *  Implement `dbFetch()` method; please use this in preference to `fetch()`.
    `dbFetch()` now returns a 0-row data frame (instead of an 0-col data frame)
    if there are no results.

 *  Methods no longer automatically close open result sets. This was implemented 
    inconsistently in a handful of places.

 *  `dbBuildTableDefinition()` has been renamed to `mysqlBuildTableDefinition()`.
 
 *  `dbWriteTable()` has been rewritten:

    * It quotes field names using `dbQuoteIdentifier()`, rather
      than use a flawed black-list based approach with name munging.

    * It now throws errors on failure, rather than returning FALSE. 
    
    * It will automatically add row names only if they are character, not integer.
    
    * When loading a file from disk, `dbWriteTable()` will no longer
      attempt to guess the correct values for `row.names` and `header` - instead
      supply them explicitly if the defaults are incorrect. 
    
    * When given a zero-row data frame it will just creates the table 
      definition. 

 *  Assorted fixes accumulated since last release 3 years ago.

 * `MySQL()` no longer has `force.reload` argument - it's not obvious that
    this ever worked.

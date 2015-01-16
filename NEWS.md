# Version 0.11

 *  Add SSL support on Windows 64bit.

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

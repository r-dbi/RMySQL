# Version 0.10

 * New maintainer: Jereon Ooms

 *  Internal changes to support static linking on Windows.

 *  The following internal functions are no longer exported: please
    use the corresponding DBI generic instead:

    `isIdCurrent`, `mysqlInitDriver`, `mysqlCloseDriver`, `mysqlDescribeDriver`,
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

 *  Implement methods for `dbIsValid()`; please use these instead of
    `isIdCurrent()`.

 *  Assorted fixes accumulated since last release 3 years ago.

 *  `fetch()` now returns a 0-row data frame (instead of an 0-col data frame)
    if there are no results.

 *  Methods no longer automatically close open result sets. This was implemented 
    inconsistently in a handful of places.

## RMySQL 0.11-4 (2016-12-29)

- Adapt to `DBItest` changes.
- Fix compiler warnings.
- Improve compatibility with different versions of `libmysql`.


# RMySQL 0.11-3 (2016-06-08)

- Fix failing compilation on Linux if a  function is declared elsewhere.
- More robust check for numeric `NA` values.
- New SSL-related arguments to `dbConnect()`: `ssl.key`, `ssl.cert`, `ssl.ca`, `ssl.capath`, `ssl.cipher` (#131, #148, @adamchainz).
- Add `TAGS` file to .gitignore (@sambrightman, #78).
- Can build with MariaDB libraries on Ubuntu (#145).
- Use new `sqlRownamesToColumn()` and `sqlColumnToRownames()` (rstats-db/DBI#91).
- Use `const&` for `Rcpp::Nullable` (@peternowee, #129).
- Use container-based builds on Travis (#143).


# RMySQL 0.11-2 (2016-03-29)

- Use the `DBItest` package for testing (#100).


# RMySQL 0.11-1 (2016-03-24)

 *  RMySQL fully supports DATE and DATETIME columns. On output, DATE columns
    will be converted to vectors of `Date`s and DATETIME will be converted
    to `POSIXct`. To faciliate correct computation of time zone, RMySQL
    always sets the session timezone to UTC.

 *  RMySQL has been rewritten (essentially from scratch) in C++ with
    Rcpp. This has considerably reduced the amount of code, and allow us to
    take advantage of the more sophisticated memory management tools available in
    Rcpp. This rewrite should yield some minor performance improvements, but 
    most importantly protect against memory leaks and crashes. It also provides
    a better base for future development.

 *  Support for prepared queries: create prepared query with `dbSendQuery()` 
    and bind values with `dbBind()`. `dbSendQuery()` and `dbGetQuery()` also 
    support inline parameterised queries, like 
    `dbGetQuery(mysqlDefault(), "SELECT * FROM mtcars WHERE cyl = :cyl", 
    params = list(cyl = 4))`. This has no performance benefits but protects you 
    from SQL injection attacks.

 * `dbListFields()` has been removed. Please use `dbColumnInfo()` instead.

 * `dbGetInfo()` has been removed. Please use the individual metadata 
    functions.

 *  Information formerly contain in `summary()` methods has now been integrated
    into `show()` methods.

 *  `make.db.names()` has been deprecated. Use `dbQuoteIdentifier()` instead.
 
 *  `isIdCurrent()` has been deprecated. Use `dbIsValid()` instead.

 *  `dbApply()`, `dbMoreResults()` and `dbNextResults()` have been removed.
    These were always flagged as experimental, and now the experiment is over.

 *  `dbEscapeStrings()` has been deprecated. Please use `dbQuoteStrings()`
    instead.

 *  dbObjectId compatibility shim removed

 *  Add SSL support on Windows.

 *  Fix repetition of strings in subsequent rows (@peternowee, #125).

 *  Always set connection character set to utf-8

 *  Backport build system improvements from stable branch

 *  Reenable Travis-CI, switch to R Travis, collect coverage


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

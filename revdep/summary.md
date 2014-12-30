# Setup

## Platform

|setting  |value                        |
|:--------|:----------------------------|
|version  |R version 3.1.2 (2014-10-31) |
|system   |x86_64, darwin13.4.0         |
|ui       |RStudio (0.99.104)           |
|language |(EN)                         |
|collate  |en_US.UTF-8                  |
|tz       |Pacific/Auckland             |

## Packages

|package  |*  |version |date       |source         |
|:--------|:--|:-------|:----------|:--------------|
|DBI      |   |0.3.1   |2014-09-24 |CRAN (R 3.1.1) |
|testthat |   |0.9.1   |2014-10-01 |CRAN (R 3.1.1) |

# Check results
10 checked out of 10 dependencies 

## Causata (4.2-0)
Maintainer: Justin Hemann <justinh@causata.com>

```
checking top-level files ... NOTE
Non-standard file/directory found at top level:
  ‘integration_tests’
```
```
checking R code for possible problems ... NOTE
GetMetadata.Connect: no visible global function definition for
  ‘dbGetQuery’
GetRawData.Connect: no visible global function definition for
  ‘dbGetQuery’
```
```
checking Rd cross-references ... WARNING
Missing link or links in documentation object 'Connect.Rd':
  ‘RMySQL’

See the information in section 'Cross-references' of the 'Writing R
Extensions' manual.

```
```
checking files in ‘vignettes’ ... NOTE
The following files look like leftovers/mistakes:
  ‘Causata-vignette.log’
Please remove them from your package.
```

## compendiumdb (0.1.0)
Maintainer: Umesh Nandal <u.k.nandal@amc.uva.nl>

```
checking package dependencies ... NOTE
Packages suggested but not available for checking:
  ‘inSilicoDb’ ‘hgu133a.db’ ‘GSVA’ ‘GSVAdata’
```

## db.r (0.1.3)
Maintainer: Greg Lamp <greg@yhathq.com>  
Bug reports: https://github.com/yhat/db.r/issues

```
checking package dependencies ... NOTE
Package suggested but not available for checking: ‘RPostgreSQL’
```

## dbConnect (1.0)
Maintainer: Dason Kurkiewicz <dasonk@iastate.edu>

```
checking package dependencies ... ERROR
Package required but not available: ‘gWidgets’

Package suggested but not available for checking: ‘gWidgetsRGtk2’

See the information on DESCRIPTION files in the chapter ‘Creating R
packages’ of the ‘Writing R Extensions’ manual.
```

## dplyr (0.3.0.2)
Maintainer: Hadley Wickham <hadley@rstudio.com>  
Bug reports: https://github.com/hadley/dplyr/issues

```
checking package dependencies ... NOTE
Packages suggested but not available for checking:
  ‘RSQLite.extfuns’ ‘RPostgreSQL’
```
```
checking dependencies in R code ... NOTE
Namespace in Imports field not imported from: ‘R6’
  All declared Imports should be used.
See the information on DESCRIPTION files in the chapter ‘Creating R
packages’ of the ‘Writing R Extensions’ manual.
```
```
checking examples ... ERROR
Running examples in ‘dplyr-Ex.R’ failed
The error most likely occurred in:

> base::assign(".ptime", proc.time(), pos = "CheckExEnv")
> ### Name: src_mysql
> ### Title: Connect to mysql/mariadb.
> ### Aliases: src_mysql tbl.src_mysql
> 
> ### ** Examples
> 
> ## Not run: 
> ##D # Connection basics ---------------------------------------------------------
> ##D # To connect to a database first create a src:
> ##D my_db <- src_mysql(host = "blah.com", user = "hadley",
> ##D   password = "pass")
> ##D # Then reference a tbl within that src
> ##D my_tbl <- tbl(my_db, "my_table")
> ## End(Not run)
> 
> # Here we'll use the Lahman database: to create your own local copy,
> # create a local database called "lahman", or tell lahman_mysql() how to
> # a database that you can write to
> 
> if (!has_lahman("postgres") && has_lahman("mysql")) {
+ # Methods -------------------------------------------------------------------
+ batting <- tbl(lahman_mysql(), "Batting")
+ dim(batting)
+ colnames(batting)
+ head(batting)
+ 
+ # Data manipulation verbs ---------------------------------------------------
+ filter(batting, yearID > 2005, G > 130)
+ select(batting, playerID:lgID)
+ arrange(batting, playerID, desc(yearID))
+ summarise(batting, G = mean(G), n = n())
+ mutate(batting, rbi2 = 1.0 * R / AB)
+ 
+ # note that all operations are lazy: they don't do anything until you
+ # request the data, either by `print()`ing it (which shows the first ten
+ # rows), by looking at the `head()`, or `collect()` the results locally.
+ 
+ system.time(recent <- filter(batting, yearID > 2010))
+ system.time(collect(recent))
+ 
+ # Group by operations -------------------------------------------------------
+ # To perform operations by group, create a grouped object with group_by
+ players <- group_by(batting, playerID)
+ group_size(players)
+ 
+ # MySQL doesn't support windowed functions, which means that only
+ # grouped summaries are really useful:
+ summarise(players, mean_g = mean(G), best_ab = max(AB))
+ 
+ # When you group by multiple level, each summarise peels off one level
+ per_year <- group_by(batting, playerID, yearID)
+ stints <- summarise(per_year, stints = max(stint))
+ filter(ungroup(stints), stints > 3)
+ summarise(stints, max(stints))
+ 
+ # Joins ---------------------------------------------------------------------
+ player_info <- select(tbl(lahman_mysql(), "Master"), playerID,
+   birthYear)
+ hof <- select(filter(tbl(lahman_mysql(), "HallOfFame"), inducted == "Y"),
+  playerID, votedBy, category)
+ 
+ # Match players and their hall of fame data
+ inner_join(player_info, hof)
+ # Keep all players, match hof data where available
+ left_join(player_info, hof)
+ # Find only players in hof
+ semi_join(player_info, hof)
+ # Find players not in hof
+ anti_join(player_info, hof)
+ 
+ # Arbitrary SQL -------------------------------------------------------------
+ # You can also provide sql as is, using the sql function:
+ batting2008 <- tbl(lahman_mysql(),
+   sql("SELECT * FROM Batting WHERE YearID = 2008"))
+ batting2008
+ }
Loading required package: RPostgreSQL
Warning in library(package, lib.loc = lib.loc, character.only = TRUE, logical.return = TRUE,  :
  there is no package called ‘RPostgreSQL’
Loading required package: RMySQL
Loading required package: DBI
Error: select is not a character vector
Execution halted
```

## ProjectTemplate (0.6)
Maintainer: Kirill Mueller <krlmlr+r@mailbox.org>  
Bug reports: https://github.com/johnmyleswhite/ProjectTemplate/issues

```
checking package dependencies ... NOTE
Packages suggested but not available for checking: ‘RODBC’ ‘RPostgreSQL’
```
```
checking dependencies in R code ... NOTE
No Java runtime present, requesting install.
See the information on DESCRIPTION files in the chapter ‘Creating R
packages’ of the ‘Writing R Extensions’ manual.
```

## quantmod (0.4-3)
Maintainer: Joshua M. Ulrich <josh.m.ulrich@gmail.com>

```
checking R code for possible problems ... NOTE
Found the following calls to attach():
File ‘quantmod/R/attachSymbols.R’:
  attach(NULL, pos = pos, name = DB$name)
  attach(NULL, pos = pos, name = DB$name)
See section ‘Good practice’ in ‘?attach’.
```

## sqldf (0.4-10)
Maintainer: G. Grothendieck <ggrothendieck@gmail.com>  
Bug reports: http://groups.google.com/group/sqldf

```
checking package dependencies ... NOTE
Package suggested but not available for checking: ‘RPostgreSQL’
```

## TSMySQL (2013.9-1)
Maintainer: Paul Gilbert <pgilbert.ttv9z@ncf.ca>

```
checking package dependencies ... ERROR
Package required but not available: ‘TSsql’

See the information on DESCRIPTION files in the chapter ‘Creating R
packages’ of the ‘Writing R Extensions’ manual.
```

## TSsql (2014.4-1)
Maintainer: Paul Gilbert <pgilbert.ttv9z@ncf.ca>

```
checking R code for possible problems ... NOTE
TSgetSQL: no visible global function definition for ‘as.zoo’
TSgetSQL: no visible global function definition for ‘zoo’
TSgetSQL: no visible global function definition for
  ‘changeTSrepresentation’
TSquery: no visible global function definition for ‘zoo’
```


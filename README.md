RMySQL
======

[![Build Status](https://travis-ci.org/rstats-db/RMySQL.svg?branch=master)](https://travis-ci.org/rstats-db/RMySQL) [![Coverage Status](https://img.shields.io/codecov/c/github/rstats-db/RMySQL/master.svg)](https://codecov.io/github/rstats-db/RMySQL?branch=master)

RMySQL is a database interface and MySQL driver for R. This version complies with the database interface definition as implemented in the package DBI 0.2-2. 

## Installation

The released version from CRAN:

```R
install.packages("RMySQL")
```

The development version from github:

```R
# install.packages("devtools")
devtools::install_github("RcppCore/Rcpp")
devtools::install_github("rstats-db/DBI")
devtools::install_github("rstats-db/RMySQL")
```

The package has been setup to statically link against [libmysqlclient](http://dev.mysql.com/downloads/connector/c/) on Windows and OSX. No run-time dependencies are required.

## Basic usage

```R
library(DBI)
# Connect to my-db as defined in ~/.my.cnf
con <- dbConnect(RMySQL::MySQL(), group = "my-db")

dbListTables(con)
dbWriteTable(con, "mtcars", mtcars)
dbListTables(con)

dbListFields(con, "mtcars")
dbReadTable(con, "mtcars")

# You can fetch all results:
res <- dbSendQuery(con, "SELECT * FROM mtcars WHERE cyl = 4")
dbFetch(res)
dbClearResult(res)

# Or a chunk at a time
res <- dbSendQuery(con, "SELECT * FROM mtcars WHERE cyl = 4")
while(!dbHasCompleted(res)){
  chunk <- dbFetch(res, n = 5)
  print(nrow(chunk))
}
# Clear the result
dbClearResult(res)

# Disconnect from the database
dbDisconnect(con)
```

## MySQL configuration file

Instead of specifying a username and password in calls to `dbConnect()`, it's better to set up a MySQL configuration file that names the databases that you connect to most commonly. This file should live in `~/.my.cnf` and look like:

```
[database_name]
option1=value1
option2=value2
```

If you want to run the examples, you'll need to set the proper options in the `[rs-dbi]` group of any MySQL option file, such as /etc/my.cnf or the .my.cnf file in your home directory. For a default single user install of MySQL, the following code should work:

```
[rs-dbi]
database=test
user=root
password=
```

## Acknowledgements

Many thanks to Christoph M. Friedrich, John Heuer, Kurt Hornik, Torsten Hothorn, Saikat Debroy, Matthew Kelly, Brian D. Ripley, Mikhail Kondrin, Jake Luciani, Jens Nieschulze, Deepayan Sarkar, Louis Springer, Duncan Temple Lang, Luis Torgo, Arend P. van der Veen, Felix Weninger, J. T. Lindgren, Crespin Miller, and Michal Okonlewski, Seth Falcon and Paul Gilbert for comments, suggestions, bug reports, and patches.


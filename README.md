RMySQL
======

NOTE: this package is being phased out in favor of the new [RMariaDB](https://github.com/rstats-db/RMariaDB) package.

> Database Interface and MySQL Driver for R

[![Build Status](https://travis-ci.org/rstats-db/RMySQL.svg?branch=stable)](https://travis-ci.org/rstats-db/RMySQL/branches)
[![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/github/rstats-db/RMySQL?branch=stable&svg=true)](https://ci.appveyor.com/project/jeroen/RMySQL?branch=stable)
[![CRAN_Status_Badge](http://www.r-pkg.org/badges/version/RMySQL)](https://cran.r-project.org/package=RMySQL)
[![CRAN RStudio mirror downloads](http://cranlogs.r-pkg.org/badges/RMySQL)](https://cran.r-project.org/package=RMySQL)
[![Coverage Status](https://codecov.io/github/rstats-db/RMySQL/coverage.svg?branch=stable)](https://codecov.io/github/rstats-db/RMySQL?branch=stable)

RMySQL is a database interface and MySQL driver for R. This version complies with the database interface definition as implemented in the package DBI 0.2-2. 

## Hello World

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

## Installation

Binary packages for __OS-X__ or __Windows__ can be installed directly from CRAN:

```r
install.packages("RMySQL")
```

The development version from github:

```R
# install.packages("devtools")
devtools::install_github("rstats-db/DBI")
devtools::install_github("rstats-db/RMySQL")
```

Installation from source on Linux or OSX requires [`MariaDB Connector/C`](https://downloads.mariadb.org/connector-c/). On some older platforms you can also link against Oracle's [libmysqlclient](https://packages.debian.org/testing/libmysqlclient-dev) driver but the mariadb implementation is much better.

On recent __Debian or Ubuntu__ install [libmariadb-client-lgpl-dev](https://packages.debian.org/testing/libmariadb-client-lgpl-dev). In Ubuntu 14.04 this was called [libmariadbclient-dev](http://packages.ubuntu.com/trusty/libmariadbclient-dev).

```
sudo apt-get install -y libmariadb-client-lgpl-dev
```

On __Fedora__,  __CentOS or RHEL__ we need [mariadb-devel](https://apps.fedoraproject.org/packages/mariadb-devel):

```
sudo yum install mariadb-devel
````

On __OS-X__ use [mariadb-connector-c](https://github.com/Homebrew/homebrew-core/blob/master/Formula/mariadb-connector-c.rb) from Homebrew:

```
brew install mariadb-connector-c
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


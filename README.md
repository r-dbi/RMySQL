RMySQL
======

[![Build Status](https://travis-ci.org/rstats-db/RMySQL.svg?branch=master)](https://travis-ci.org/rstats-db/RMySQL)

RMySQL is a database interface and MySQL driver for R. This version complies with the database interface definition as implemented in the package DBI 0.2-2. 

## Installation

The released version from CRAN:

```R
install.packages("RMySQL")
```

The development version from github:

```R
# install.packages("devtools")
devtools::install_github("rstats-db/RMySQL")
```

## MySQL configuration file

Instead of specifying a username and password in calls to `dbConnect()`, it's better to set up a MySQL configuration file that names the databases that you connect to most commonly. This file should live in `~/.my.cnf` and look like:

```
[database_name]
option1=value1
option2=value2
```

If you want to be able to run the examples, you'll need to set up a database called `rs-dbi`. For a default single user install of MySQL, the following code should work:

```
[rs-dbi]
database=test
username=root
password=
```

## Acknowledgements

Many thanks to Christoph M. Friedrich, John Heuer, Kurt Hornik, Torsten Hothorn, Saikat Debroy, Matthew Kelly, Brian D. Ripley, Mikhail Kondrin, Jake Luciani, Jens Nieschulze, Deepayan Sarkar, Louis Springer, Duncan Temple Lang, Luis Torgo, Arend P. van der Veen, Felix Weninger, J. T. Lindgren, Crespin Miller, and Michal Okonlewski, Seth Falcon and Paul Gilbert for comments, suggestions, bug reports, and patches.


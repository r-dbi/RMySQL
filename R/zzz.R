## 
## $Id$
##

".conflicts.OK" <- TRUE
## need DBI and methods *prior* to having library.dynam() invoked!
library(methods)
library(DBI, quietly = TRUE, warn.conflicts = FALSE)

".First.lib" <- 
function(lib, pkg) 
{
   library(methods)
   library(DBI, warn.conflicts = FALSE)
   library.dynam("RMySQL", pkg, lib)
}

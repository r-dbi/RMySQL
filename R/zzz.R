## 
## $Id$
##

".conflicts.OK" <- TRUE
## need DBI and methods *prior* to having library.dynam() invoked!
require(DBI, quietly = TRUE, warn.conflicts = FALSE)

".First.lib" <- 
function(lib, pkg) 
{
   library(methods)
   require(DBI, quietly = TRUE, warn.conflicts = FALSE)
   library.dynam("RMySQL", pkg, lib)
}

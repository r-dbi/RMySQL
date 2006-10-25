## 
## $Id$
##

".conflicts.OK" <- TRUE

".First.lib" <- 
function(libname, pkgname) 
{
   library.dynam("RMySQL", pkgname, libname)
}

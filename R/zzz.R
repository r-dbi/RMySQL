".conflicts.OK" <- TRUE

".First.lib" <- 
function(lib, pkg) 
{
   library.dynam("RMySQL", pkg, lib)
}

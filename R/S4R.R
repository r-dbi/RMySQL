## This file defines some functions that mimic S4 functionality,
## for R versions prior to 1.4.
## Functions: oldClass, "oldClass<-", as, new, and show

"usingR" <- 
function(major=0, minor=0)
## are we using at least R version major, minor?
{
   if(is.null(version$language))
      return(FALSE)
   if(version$language!="R")
      return(FALSE)
   version$major>=major && version$minor>=minor
}

"usingSplus" <- 
function(major, minor)
## are we using at least Splus version major, minor?
{
   if(!is.null(version$language) && version$language=="R")
      return(FALSE)
   version$major>=major && version$minor>=minor
}

"usingV4" <- 
function()
## are we using V4-style classes and methods? (this 
## applies to either R or Splus)
{
   exists("setClass", mode = "function")
}

## constant holding the appropriate error class returned by try() 
if(usingR()){
  ErrorClass <- "try-error"
} else {
  ErrorClass <- "Error"      ## Splus
}

if(!exists("existsFunction", mode = "function")){
    existsFunction <- function(f) exists(f, mode = "function")
}

if(!existsFunction("oldClass")){
   "oldClass" <- class
   "oldClass<-" <- 
   function(x, value) 
   {
      class(x) <- value
      x
   }
}

if(TRUE){

   "as" <- 
   function(object, classname)
   {
      get(paste("as", as.character(classname), sep = "."))(object)
   }

   "new" <- 
   function(classname, ...) 
   {
      if(!is.character(classname))
         stop("classname must be a character string")
      class(classname) <- classname
      UseMethod("new")
   }

   "new.default" <- 
   function(classname, ...) 
   {
      #structure(list(...), class = unclass(classname))
      x <- list()
      attributes(x) <- list(...)
      class(x) <- unclass(classname)
      x
   }

   "show" <- 
   function(object, ...) 
   {
      UseMethod("show")
   }

   "show.default" <- 
   function(object) 
   {
      print(object)
      invisible(NULL)
   }
}

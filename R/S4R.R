## 
## $Id$
##
## This file defines some functions that mimic S4 functionality,
## namely:  new, as, show.

usingR <- function(major=0, minor=0){
  if(is.null(version$language))
    return(FALSE)
  if(version$language!="R")
    return(FALSE)
  version$major>=major && version$minor>=minor
}

## constant holding the appropriate error class returned by try() 
if(usingR()){
  ErrorClass <- "try-error"
} else {
  ErrorClass <- "Error"  
}

if(TRUE){
   ## When we move to version 4 style classes, we should replace 
   ## the calls to the following by their lower-case counerpart, 
   ## as defined in library(methods)
   AS <- function(object, classname)
   {
     get(paste("as", as.character(classname), sep = "."))(object)
   }

   NEW <- function(classname, ...)
   {
     if(!is.character(classname))
       stop("classname must be a character string")
     do.call(paste("NEW", classname[1], sep="."), list(...))
   }

   NEW.default <- function(classname, ...)
   {
     structure(list(...), class = unclass(classname))
   }

}

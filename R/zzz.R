## 
## $Id$
##

.conflicts.OK <- TRUE

.onLoad <-
if(.Platform$OS.type == "windows") {
    # It looks in for MySQL in the following directory trees:
    #   MYSQL_HOME, registry, %ProgramFiles%\MySQL, C:\MySQL, ..., 
    #   and C:\Apps\MySQL, ...  (where ... means check other disks too)
    #
    # Thanks to Gabor Grothendieck ggrothendieck@gmail.com
    #
    function(libname, pkgname)
    {
	verbose <- TRUE
	dir.exists <- function(x) {
	    !is.null(x) && file.exists(x) && file.info(x)$isdir
	}

	# check MYSQL_HOME environment variable
	mysql <- Sys.getenv("MYSQL_HOME")
	if (verbose && dir.exists(mysql)) cat("MYSQL_HOME defined as", mysql, "\n")

	# check registry
	if (!dir.exists(mysql)) {
	    reg <- utils::readRegistry("SOFTWARE\\MySQL AB", hive="HLM", maxdepth=2)
	    for (i in reg){
		mysql <- i$Location
		if (dir.exists(mysql)) {
		    if (verbose) cat(mysql, "found in registry\n")
		    break
		}
	    }
	}

	# check %ProgramFiles%:\MySQL and these 
	# C:\MySQL, ..., C:\Apps\MySQL, ...

	if (!dir.exists(mysql)) {

	    ProgramFilesMySQL <- file.path(Sys.getenv("ProgramFiles"), "MySQL")
	    default.disks <- c("C:", "D:", "E:", "F:", "G:")
	    default.dirs <- file.path(c("", "/xampp", "/Apps"), "MySQL")
	    g <- with(expand.grid(disk = default.disks, dir = default.dirs), 
		paste(disk, dir, sep = ""))
	    lookup.paths <- c(ProgramFilesMySQL, g)

	    if (verbose) cat("Looking in", toString(lookup.paths), "\n")
	    mysql <- Find(dir.exists, lookup.paths)
        if (dir.exists(mysql) && verbose) cat("Found", mysql, "\n")

	    # if still not found find other disks and look in them
	    # we save this until the end since running wmic takes a bit longer
	    if (!dir.exists(mysql)) {
		# wmic should exist on Vista and Win7
		wmic.out <- if (nzchar(Sys.which("wmic"))) {
		    shell("wmic logicaldisk get name", intern = TRUE)
		} else character(0)
		all.disks <- grep(":", gsub("[^[:graph:]]", "", wmic.out), 
		    value = TRUE)
		other.disks <- setdiff(all.disks, default.disks)
		more <- with(expand.grid(disk = default.disks, dir = default.dirs), 
		    paste(disk, dir, sep = ""))
		if (verbose) cat("Looking in", toString(more), "\n")
		mysql <- Find(dir.exists, more)
		if (verbose && dir.exists(mysql)) cat("Found", mysql, "\n")
	    }
	}

	if (dir.exists(mysql)) {
		if (Sys.getenv("MYSQL_HOME")=="") {
			bin <- dir(path = mysql, pattern = "^bin$", recursive = TRUE, 
				full = TRUE, ignore.case = TRUE)
			cwd <- getwd()
			setwd(bin)
			setwd("..")
			Sys.setenv(MYSQL_HOME=getwd())
			setwd(cwd)
		}
	    lib <- dir(path = mysql, pattern = "^libmysql.lib$",
		recursive = TRUE, full = TRUE, ignore.case = TRUE)
	    dll <- dir(path = mysql, pattern = "^libmysql.dll$",
		recursive = TRUE, full = TRUE, ignore.case = TRUE)
		dll <- dirname(dll)
	    include <- dir(path = mysql, pattern = "^include$", include.dirs = TRUE,
		recursive = TRUE, full = TRUE, ignore.case = TRUE)
	    library.dynam("RMySQL", pkgname, libname, DLLpath = dll)
	    #c(home = mysql, lib = lib, dll = dll, include = include)
	} else {
	    stop("Cannot find a suitable MySQL install")
	}
    }
} else {
    function(libname, pkgname) library.dynam("RMySQL", pkgname, libname)
}

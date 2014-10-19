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
	    !is.null(x) & file.exists(x) & file.info(x)$isdir
	}

	# check MYSQL_HOME environment variable
	mysql <- Sys.getenv("MYSQL_HOME")
	if (verbose && dir.exists(mysql)) cat("MYSQL_HOME defined as", mysql, "\n")

	# check registry
	if (!dir.exists(mysql)) {
	    mysql = character(0)
	    reg <- utils::readRegistry("SOFTWARE\\MySQL AB", hive="HLM", maxdepth=2, view="32-bit")
	    reg <- Filter(is.list, reg)
	    reg <- Filter(function(i) "Version" %in% names(i) & "Location" %in% names(i), reg)
	    reg <- reg[order(grepl("Server", names(reg)), sapply(reg, function(i) i$Version), decreasing=TRUE)]
	    mysql <- sapply(reg, function(i) i$Location)
	    mysql <- sub("[\\/]$", "", mysql)
	    mysql <- Filter(dir.exists, mysql)
	    if (verbose) for (i in mysql) cat(i, "found in registry\n")
	}

	# check %ProgramFiles%:\MySQL and these 
	# C:\MySQL, ..., C:\Apps\MySQL, ...

	if (length(mysql) == 0) {

	    ProgramFilesMySQL <- file.path(Sys.getenv("ProgramFiles"), "MySQL")
	    default.disks <- c("C:", "D:", "E:", "F:", "G:")
	    default.dirs <- file.path(c("", "/xampp", "/Apps"), "MySQL")
	    g <- with(expand.grid(disk = default.disks, dir = default.dirs), 
		paste(disk, dir, sep = ""))
	    lookup.paths <- c(ProgramFilesMySQL, g)

	    if (verbose) cat("Looking in", toString(lookup.paths), "\n")
	    mysql <- Filter(dir.exists, lookup.paths)
	    if (verbose) for (i in mysql) cat("Found", i, "by guessing\n")

	    # if still not found find other disks and look in them
	    # we save this until the end since running wmic takes a bit longer
	    if (length(mysql) == 0) {
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
		mysql <- Filter(dir.exists, more)
		if (verbose) for (i in mysql) cat("Found", i, "by searching\n")
	    }
	}

	if (length(mysql) == 0)
	    stop("Cannot find a suitable MySQL install")

	for (i in mysql) {
	    if (verbose) cat("Searching for libmysql.dll in", i, "\n")
	    dll <- dir(path = i, pattern = "^libmysql\\.dll$",
		       recursive = TRUE, full = TRUE, ignore.case = TRUE)
	    if (length(dll) != 0) {
		if (verbose) cat(dll[1], "found\n")
		if (Sys.getenv("MYSQL_HOME")=="") {
		    bin <- dir(path = i, pattern = "^bin$", recursive = TRUE,
			       full = TRUE, ignore.case = TRUE)
		    if (length(bin) == 1) {
			cwd <- getwd()
			setwd(bin)
			setwd("..")
			Sys.setenv(MYSQL_HOME=getwd())
			setwd(cwd)
		    }
		    else {
			Sys.setenv(MYSQL_HOME=i)
		    }
		}
		dll <- dirname(dll[1])
		library.dynam("RMySQL", pkgname, libname, DLLpath = dll)
		break
	    }
	}
    }
} else {
    function(libname, pkgname) library.dynam("RMySQL", pkgname, libname)
}

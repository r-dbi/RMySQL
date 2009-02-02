## 
## $Id$
##

.conflicts.OK <- TRUE

.onLoad <-
	if(.Platform$OS.type == "windows") {
		function(libname, pkgname)
		{

			## We try to suppress trailing slash pain

			MySQLhome <- Sys.getenv("MYSQL_HOME")
			if(nzchar(MySQLhome)) {
                MySQLhome <- file.path(MySQLhome,".")
				if (!utils::file_test("-d", MySQLhome))
					stop("MYSQL_HOME was set but does not point to a directory",
						call. = FALSE)
			} else {
				reg <- NULL
                reg <- utils::readRegistry("SOFTWARE\\MySQL AB", hive="HLM", maxdepth=2)
                if(is.null(reg))
                    stop("MySQL is not installed according to a Registry search")

				for (i in reg){
					MySQLhome <- file.path(i$Location,".")
					if (utils::file_test("-d",MySQLhome))
						break
				}

				## One more time for loop fall-through. 
				if (!utils::file_test("-d",MySQLhome))
                    stop("A MySQL Registry key was found (",i,"), but the Location key was empty. Please fix your registry.")

        	}

			## Users may only install the libraries and not the full distribution
			## so try 'bin' and 'lib/opt'
			MySQLdllPath <- file.path(MySQLhome,"bin")
			if (!utils::file_test("-d",MySQLdllPath))
				MySQLdllPath <- file.path(MySQLhome,"lib/opt")
			if (!utils::file_test("-d",MySQLdllPath))
                    stop("A MySQL Registry key was found but the folder ",MySQLhome," doesn't contain a bin or lib/opt folder. That's where we need to find libmySQL.dll. ")

            library.dynam("RMySQL", pkgname, libname, DLLpath = MySQLdllPath)
		}
    } else {
        function(libname, pkgname) library.dynam("RMySQL", pkgname, libname)
    }

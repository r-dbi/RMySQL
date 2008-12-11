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
                reg <- utils::readRegistry("SOFTWARE\\MySQL AB", hive="HLM", maxdepth=2)
                if(is.null(reg))
                    stop("MySQL is not installed according to a Registry search")


				# Find first existing Mysql home with MYSQL_VERSION taking precedence
				MySQLversion <- Sys.getenv("MYSQL_VERSION")
				for (i in reg){
					MySQLhome <- file.path(i$Location,".")
					if (nzchar(MySQLversion)){
						if (i$Version==MySQLversion){
							if (utils::file_test("-d",MySQLhome)){
								break
							} else {
								stop(sprintf(
									"MYSQL_VERSION was set but it's directory '%s' does not exist",
									i$Location),
									call. = FALSE)
							}
						}
					} else if (utils::file_test("-d",MySQLhome))
						break
				}

				## One more time for corner cases.
				if (!utils::file_test("-d",MySQLhome))
                    stop("MySQL is not installed according to a Registry search")

        	}

			## Users may only install the libraries and not the full distribution
			## so try 'bin' and 'lib/opt'
			MySQLdllPath <- file.path(MySQLhome,"bin")
			if (!utils::file_test("-d",MySQLdllPath))
				MySQLdllPath <- file.path(MySQLhome,"lib/opt")
			if (!utils::file_test("-d",MySQLdllPath))
                    stop("MySQL is not installed according to a Registry search")

            library.dynam("RMySQL", pkgname, libname, DLLpath = MySQLdllPath)
		}
    } else {
        function(libname, pkgname) library.dynam("RMySQL", pkgname, libname)
    }

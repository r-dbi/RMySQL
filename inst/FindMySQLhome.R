require(utils,quietly=TRUE)
# Find first existing Mysql home 
reg <- readRegistry("SOFTWARE\\MySQL AB", hive="HLM", maxdepth=2, view="32-bit")
if (!is.null(reg)){
	cat("Try setting MYSQL_HOME to one of the following (you may have to use the non-8dot3 file name):\r\n\r\n")
	invisible(lapply(reg,function(i){
		MySQLhome <- file.path(i$Location,".")
		if (utils::file_test("-d",MySQLhome) &&
			utils::file_test("-d",file.path(MySQLhome,"include")) &&
			utils::file_test("-d",file.path(MySQLhome,"lib/opt"))){
			cat(MySQLhome,"\r\n",sep='')
		}
	}
	))
}
q(save="no")

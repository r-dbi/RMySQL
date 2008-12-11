require(utils,quietly=TRUE)
# Find first existing Mysql home 
reg <- readRegistry("SOFTWARE\\MySQL AB", hive="HLM", maxdepth=2)
if (!is.null(reg)){
	cat("Try setting MYSQL_HOME to one of the following:\r\n\r\n")
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

# Link against mariadb native client static libraries
# Info and source code: https://downloads.mariadb.org/client-native/
# Source copy: http://rstats-db.github.io/RMySQL/mariadb_client-2.0.0-src.tar.gz
if(!file.exists("../windows/libmariadbclient-2.1.0/include/mysql.h")){
  if(getRversion() < "3.3.0") setInternet2()
  download.file("https://github.com/rwinlib/libmariadbclient/archive/v2.1.0.zip", "lib.zip", quiet = TRUE)
  dir.create("../windows", showWarnings = FALSE)
  unzip("lib.zip", exdir = "../windows")
  unlink("lib.zip")
}

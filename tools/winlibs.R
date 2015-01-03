# Link against mariadb native client static libraries
# Info and source code: https://downloads.mariadb.org/client-native/
# Source copy: http://rstats-db.github.io/RMySQL/mariadb_client-2.0.0-src.tar.gz
if(!file.exists("../windows/mariadb-client-2.0/include/mysql.h")){
  setInternet2()
  download.file("http://rstats-db.github.io/RMySQL/mariadb-client-2.0.zip", "lib.zip", quiet = TRUE)
  dir.create("../windows", showWarnings = FALSE)
  unzip("lib.zip", exdir = "../windows")
  unlink("lib.zip")
}

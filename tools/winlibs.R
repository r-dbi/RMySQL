# Link against mariadb native client static libraries
# Info and source code: https://downloads.mariadb.org/client-native/
# Copy of source (as per license requirements): http://rstats-db.github.io/RMySQL/mariadb_client-2.0.0-src.tar.gz
if(!file.exists("../windows/mariadb-5.5/include/mysql.h")){
  setInternet2()
  download.file("http://rstats-db.github.io/RMySQL/mariadb-5.5.zip", "lib.zip", quiet = TRUE)
  dir.create("../windows", showWarnings = FALSE)
  unzip("lib.zip", exdir = "../windows")
  unlink("lib.zip")
}

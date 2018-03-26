# Link against mariadb native client static libraries
# Info and source code: https://downloads.mariadb.org/client-native/
if(!file.exists("../windows/libmariadbclient-2.3.5/include/mariadb/mysql.h")){
  if(getRversion() < "3.3.0") setInternet2()
  curl::curl_download("https://github.com/rwinlib/libmariadbclient/archive/v2.3.5.zip", "lib.zip")
  dir.create("../windows", showWarnings = FALSE)
  unzip("lib.zip", exdir = "../windows")
  unlink("lib.zip")
}

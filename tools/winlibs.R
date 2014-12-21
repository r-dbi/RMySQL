# Build against static libraries from curl website.
if(!file.exists("../windows/mariadb-5.5/include/mysql.h")){
  setInternet2()
  download.file("http://jeroenooms.github.io/RMySQL/mariadb-5.5.zip", "lib.zip", quiet = TRUE)
  dir.create("../windows", showWarnings = FALSE)
  unzip("lib.zip", exdir = "../windows")
  unlink("lib.zip")
}

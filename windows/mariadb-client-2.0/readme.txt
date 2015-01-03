MariaDB Client libraries for Windows
====================================

Windows builds by Jeroen Ooms, 2014.

Download from: https://downloads.mariadb.org/client-native
Copy of source: http://rstats-db.github.io/RMySQL/mariadb_client-2.0.0-src.tar.gz
Adapted from: https://gist.github.com/ant32/ad40af3b5fec652f335b

Instructions
------------

Download standard CMAKE windows executables and copy to into the MSYS dir.
Current Rtools gcc is too old; I used mingw-w64 version 4.9.2 on both i386 and x64.

Build
-----

cmake -G "MSYS Makefiles" . -DCMAKE_BUILD_TYPE=Release
make

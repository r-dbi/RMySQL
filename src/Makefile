# vim: set syntax=make
RMySQL.so: RS-DBI.o RS-MySQL.o
	R CMD SHLIB -o RMySQL.so RS-DBI.o RS-MySQL.o

RS-DBI.o: S4R.h RS-DBI.h RS-DBI.c
	R CMD COMPILE RS-DBI.c

RS-MySQL.o: S4R.h RS-MySQL.h RS-MySQL.c
	R CMD COMPILE RS-MySQL.c

clean:
	rm -rf RS-DBI.o RS-MySQL.o RMySQL.so

library(RMySQL)
drv <- dbDriver("MySQL")


## 0 dbConnect()
user <- Sys.getenv("MYSQL_USER", unset = NA)
if(is.na(user)) q()
## a MySQL installation would usually have an open 'tests' db
dbname <- Sys.getenv("MYSQL_DATABASE")
if(!nzchar(dbname)) dbname <- "test"

conn <- dbConnect(drv, user=user, password=Sys.getenv("MYSQL_PASSWD"),
                  dbname=dbname)
dbListTables(conn)

## 1 Ensure that dbWriteTable doesn't add trailing \r
dbRemoveTable(conn, "myDF")         # precaution
## RMySQL is not very clever: it does not convert to factor,
## and converts row names to character.
myDF <- data.frame(x = paste("x", 1:5, sep = ""),
                   y = paste("y", 1:5, sep = ""),
                   row.names = letters[1:5],
                   stringsAsFactors = FALSE)
dbWriteTable(conn, name= "myDF", value = myDF)
myDF2 <- dbReadTable(conn, "myDF")
stopifnot(identical(myDF, myDF2))
dbRemoveTable(conn, "myDF")
dbDisconnect(conn)

## 2 Exercise fetch.default.rec and dbGetRowCount... along with 
##   dbSendQuery() and fetch()
drv <- MySQL(fetch.default.rec=2)
conn <- dbConnect(drv, user=user, password=Sys.getenv("MYSQL_PASSWD"),
                  dbname=dbname)

dbRemoveTable(conn, "iris")         # precaution
dbWriteTable(conn,name='iris',value=iris,row.names=FALSE)
rso <- dbSendQuery(conn,"select * from iris where Species='versicolor'")
x <- fetch(rso,n=2)
rowCount <- nrow(x)
stopifnot(rowCount==2)
stopifnot(rowCount==dbGetRowCount(rso))
while(nrow(x)) {
	x <- fetch(rso)
	rowCount <- rowCount + nrow(x)
	stopifnot(rowCount==dbGetRowCount(rso))
}

## 2  Exercise dbRowsAffected()
nrows <- nrow(iris[iris$Species=='versicolor',])
rso <- dbSendQuery(conn,"delete from iris where Species='versicolor'")
stopifnot(nrows==dbGetRowsAffected(rso))

## 3 Exercise dbGetQuery()
stopifnot(
	identical(
		iris[iris$Species!='versicolor','Sepal.Length'],
		dbGetQuery(conn,'select * from iris')$Sepal_Length
	)
)

dbDisconnect(conn)
dbUnloadDriver(drv)

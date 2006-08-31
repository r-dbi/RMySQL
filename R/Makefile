S_SRC = S4R.R zzz.R dbObjectId.R MySQL.R MySQLSupport.R 

RMySQL.R: $(S_SRC)
	cat $(S_SRC) > RMySQL.R

tags: $(S_SRC)
	rtags.py $(S_SRC) > tags

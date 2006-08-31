if(F){
   con <- dbConnect(MySQL(), 
      user = "celnet",
      password = "celnet-db",
      host = "celnet-db",
      dbname = "celnet_devel")

}
if(F){
   rs <- dbSendQuery(con, 
         "SELECT BTS, FLM0_T1, FLM1_T1 from throughput ORDER BY BTS")
}
if(T){

out <- dbApply(rs, INDEX = "BTS", FUN = function(x) mean(x$FLM0_T1))

}

#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

#include <mysql.h>

void R_init_RMySQL(DllInfo *info){
  mysql_library_init(0,NULL,NULL);
  R_registerRoutines(info, NULL, NULL, NULL, NULL);
  R_useDynamicSymbols(info, TRUE);

  /* Test release vs. compiled client library, warning
   * when the major or minor revision number differ. The integer format is XYYZZ
   * where X is the major revision, YY is the minor revision, and ZZ is the revision
   * within the minor revision.

   * Jeroen 2014: this is incorrect. It merely compares the VERSION and VERSION.server
   * files contained within the connector library. It has nothing to do with build vs
   * compile. Disabling this.

   int compiled=MYSQL_VERSION_ID;
   int loaded = (int)mysql_get_client_version();
   if ( (compiled-(compiled%100)) != (loaded-(loaded%100)) ){
   warning("\n\n   RMySQL was compiled with MySQL %s but loading MySQL %s instead!\n   This may cause problems with your database connections.\n\n   Please install MySQL %s.\n\n   If you have already done so, you may need to set your environment\n   variable MYSQL_HOME to the proper install directory.",MYSQL_SERVER_VERSION,mysql_get_client_info(),MYSQL_SERVER_VERSION);
   }

   */
}

void R_unload_RMySQL(DllInfo *info){
  mysql_library_end();
}

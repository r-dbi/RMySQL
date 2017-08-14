#include "RS-MySQL.h"

static MySQLDriver* dbManager = NULL;
MySQLDriver* rmysql_driver() {
  if (!dbManager) error("Corrupt MySQL handle");
  return dbManager;
}

SEXP rmysql_driver_valid() {
  if(!dbManager || !dbManager->connections) {
    return ScalarLogical(FALSE);
  } else {
    return ScalarLogical(TRUE);
  }
}

SEXP rmysql_driver_init(SEXP max_con_, SEXP fetch_default_rec_) {
  SEXP mgrHandle = ScalarInteger(0);
  if (dbManager) return mgrHandle;
  PROTECT(mgrHandle);

  int max_con = asInteger(max_con_),
      fetch_default_rec = asInteger(fetch_default_rec_);

  int counter = 0;
  MySQLDriver* mgr = (MySQLDriver*) malloc(sizeof(MySQLDriver));
  if (!mgr)
    error("Could not allocate memory for the MySQL driver");

  /* Ok, we're here to expand number of connections, etc.*/
  mgr->managerId = 0;
  mgr->connections = calloc(max_con, sizeof(RS_DBI_connection));
  if (!mgr->connections) {
    free(mgr);
    error("Could not allocate memory for connections");
  }

  mgr->connectionIds = calloc(max_con, sizeof(int));
  if (!mgr->connectionIds){
    free(mgr->connections);
    free(mgr);
    error("Could not allocation memory for connection Ids");
  }
  mgr->counter = counter;
  mgr->length = max_con;
  mgr->num_con = (int) 0;
  mgr->fetch_default_rec = fetch_default_rec;

  for(int i = 0; i < max_con; i++){
    mgr->connectionIds[i] = -1;
    mgr->connections[i] = (RS_DBI_connection *) NULL;
  }

  dbManager = mgr;
  UNPROTECT(1);
  return mgrHandle;
}

SEXP rmysql_driver_close() {
  MySQLDriver *mgr = rmysql_driver();

  if(mgr->num_con)
    error("Open connections -- close them first");

  if(mgr->connections) {
    free(mgr->connections);
    mgr->connections = (RS_DBI_connection **) NULL;
  }

  if(mgr->connectionIds) {
    free(mgr->connectionIds);
    mgr->connectionIds = (int *) NULL;
  }

  return ScalarLogical(TRUE);
}

SEXP rmysql_driver_info() {
  MySQLDriver *mgr = rmysql_driver();

  // Allocate output
  SEXP output = PROTECT(allocVector(VECSXP, 6));
  SEXP output_nms = PROTECT(allocVector(STRSXP, 6));
  SET_NAMES(output, output_nms);
  UNPROTECT(1);

  SET_CHR_EL(output_nms, 0, mkChar("connectionIds"));
  SEXP cons = PROTECT(allocVector(INTSXP, mgr->num_con));
  RS_DBI_listEntries(mgr->connectionIds, mgr->num_con, INTEGER(cons));
  SET_VECTOR_ELT(output, 0, cons);
  UNPROTECT(1);

  SET_CHR_EL(output_nms, 1, mkChar("fetch_default_rec"));
  SET_VECTOR_ELT(output, 1, ScalarInteger(mgr->fetch_default_rec));

  SET_CHR_EL(output_nms, 2, mkChar("length"));
  SET_VECTOR_ELT(output, 2, ScalarInteger(mgr->length));

  SET_CHR_EL(output_nms, 3, mkChar("num_con"));
  SET_VECTOR_ELT(output, 3, ScalarInteger(mgr->num_con));

  SET_CHR_EL(output_nms, 4, mkChar("counter"));
  SET_VECTOR_ELT(output, 4, ScalarInteger(mgr->counter));

  SET_CHR_EL(output_nms, 5, mkChar("clientVersion"));
  SET_VECTOR_ELT(output, 5, mkString(mysql_get_client_info()));

  UNPROTECT(1);
  return output;
}

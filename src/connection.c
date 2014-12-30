#include "RS-MySQL.h"


SEXP
  RS_DBI_allocConnection(SEXP mgrHandle, int max_res)
  {
    RS_DBI_manager    *mgr;
    RS_DBI_connection *con;
    SEXP conHandle;
    int  i, indx, con_id;

    mgr = RS_DBI_getManager(mgrHandle);
    indx = RS_DBI_newEntry(mgr->connectionIds, mgr->length);
    if(indx < 0){
      char buf[128], msg[128];
      (void) strcat(msg, "cannot allocate a new connection -- maximum of ");
      (void) strcat(msg, "%d connections already opened");
      (void) sprintf(buf, msg, (int) mgr->length);
      RS_DBI_errorMessage(buf, RS_DBI_ERROR);
    }
    con = (RS_DBI_connection *) malloc(sizeof(RS_DBI_connection));
    if(!con){
      char *errMsg = "could not malloc dbConnection";
      RS_DBI_freeEntry(mgr->connectionIds, indx);
      RS_DBI_errorMessage(errMsg, RS_DBI_ERROR);
    }
    con->managerId = MGR_ID(mgrHandle);
    con_id = mgr->counter;
    con->connectionId = con_id;
    con->drvConnection = (void *) NULL;
    con->drvData = (void *) NULL;    /* to be used by the driver in any way*/
    con->conParams = (void *) NULL;
    con->counter = (int) 0;
    con->length = max_res;           /* length of resultSet vector */

    /* result sets for this connection */
    con->resultSets = (RS_DBI_resultSet **)
    calloc((size_t) max_res, sizeof(RS_DBI_resultSet));
    if(!con->resultSets){
      char  *errMsg = "could not calloc resultSets for the dbConnection";
      RS_DBI_freeEntry(mgr->connectionIds, indx);
      free(con);
      RS_DBI_errorMessage(errMsg, RS_DBI_ERROR);
    }
    con->num_res = (int) 0;
    con->resultSetIds = (int *) calloc((size_t) max_res, sizeof(int));
    if(!con->resultSetIds) {
      char *errMsg = "could not calloc vector of resultSet Ids";
      free(con->resultSets);
      free(con);
      RS_DBI_freeEntry(mgr->connectionIds, indx);
      RS_DBI_errorMessage(errMsg, RS_DBI_ERROR);
    }
    for(i=0; i<max_res; i++){
      con->resultSets[i] = (RS_DBI_resultSet *) NULL;
      con->resultSetIds[i] = -1;
    }

    /* Finally, update connection table in mgr */
    mgr->num_con += (int) 1;
    mgr->counter += (int) 1;
    mgr->connections[indx] = con;
    mgr->connectionIds[indx] = con_id;
    conHandle = RS_DBI_asConHandle(MGR_ID(mgrHandle), con_id);
    return conHandle;
  }

/* the invoking (freeing) function must provide a function for
* freeing the conParams, and by setting the (*free_drvConParams)(void *)
* pointer.
*/

void
  RS_DBI_freeConnection(SEXP conHandle)
  {
    RS_DBI_connection *con;
    RS_DBI_manager    *mgr;
    int indx;

    con = RS_DBI_getConnection(conHandle);
    mgr = RS_DBI_getManager(conHandle);

    /* Are there open resultSets? If so, free them first */
    if(con->num_res > 0) {
      char *errMsg = "opened resultSet(s) forcebly closed";
      int  i;
      SEXP rsHandle;

      for(i=0; i < con->num_res; i++){
        rsHandle = RS_DBI_asResHandle(con->managerId,
          con->connectionId,
          (int) con->resultSetIds[i]);
        RS_DBI_freeResultSet(rsHandle);
      }
      RS_DBI_errorMessage(errMsg, RS_DBI_WARNING);
    }
    if(con->drvConnection) {
      char *errMsg =
        "internal error in RS_DBI_freeConnection: driver might have left open its connection on the server";
      RS_DBI_errorMessage(errMsg, RS_DBI_WARNING);
    }
    if(con->conParams){
      char *errMsg =
        "internal error in RS_DBI_freeConnection: non-freed con->conParams (tiny memory leaked)";
      RS_DBI_errorMessage(errMsg, RS_DBI_WARNING);
    }
    if(con->drvData){
      char *errMsg =
        "internal error in RS_DBI_freeConnection: non-freed con->drvData (some memory leaked)";
      RS_DBI_errorMessage(errMsg, RS_DBI_WARNING);
    }
    /* delete this connection from manager's connection table */
    if(con->resultSets) free(con->resultSets);
    if(con->resultSetIds) free(con->resultSetIds);

    /* update the manager's connection table */
    indx = RS_DBI_lookup(mgr->connectionIds, mgr->length, con->connectionId);
    RS_DBI_freeEntry(mgr->connectionIds, indx);
    mgr->connections[indx] = (RS_DBI_connection *) NULL;
    mgr->num_con -= (int) 1;

    free(con);
    con = (RS_DBI_connection *) NULL;

    return;
  }

SEXP RS_DBI_asConHandle(int mgrId, int conId)
{
  SEXP conHandle;

  PROTECT(conHandle = NEW_INTEGER((int) 2));
  MGR_ID(conHandle) = mgrId;
  CON_ID(conHandle) = conId;
  UNPROTECT(1);
  return conHandle;
}

RS_DBI_connection* RS_DBI_getConnection(SEXP conHandle) {
  RS_DBI_manager  *mgr;
  int indx;

  mgr = RS_DBI_getManager(conHandle);
  indx = RS_DBI_lookup(mgr->connectionIds, mgr->length, CON_ID(conHandle));
  if(indx < 0)
    RS_DBI_errorMessage(
      "internal error in RS_DBI_getConnection: corrupt connection handle",
      RS_DBI_ERROR);
  if(!mgr->connections[indx])
    RS_DBI_errorMessage(
      "internal error in RS_DBI_getConnection: corrupt connection  object",
      RS_DBI_ERROR);
  return mgr->connections[indx];
}


SEXP         /* return a named list */
    RS_DBI_connectionInfo(SEXP conHandle)
    {

      RS_DBI_connection  *con;
      SEXP output;
      int     i;
      int  n = (int) 8;
      char *conDesc[] = {"host", "user", "dbname", "conType",
        "serverVersion", "protocolVersion",
        "threadId", "rsHandle"};
      SEXPTYPE conType[] = {STRSXP, STRSXP, STRSXP,
        STRSXP, STRSXP, INTSXP,
        INTSXP, INTSXP};
      int  conLen[]  = {1, 1, 1, 1, 1, 1, 1, -1};

      con = RS_DBI_getConnection(conHandle);
      conLen[7] = con->num_res;   /* number of resultSets opened */

    output = RS_DBI_createNamedList(conDesc, conType, conLen, n);

    /* dummy */
    SET_LST_CHR_EL(output,0,0,C_S_CPY("NA"));        /* host */
    SET_LST_CHR_EL(output,1,0,C_S_CPY("NA"));        /* dbname */
    SET_LST_CHR_EL(output,2,0,C_S_CPY("NA"));        /* user */
    SET_LST_CHR_EL(output,3,0,C_S_CPY("NA"));        /* conType */
    SET_LST_CHR_EL(output,4,0,C_S_CPY("NA"));        /* serverVersion */

    LST_INT_EL(output,5,0) = (int) -1;            /* protocolVersion */
    LST_INT_EL(output,6,0) = (int) -1;            /* threadId */

    for(i=0; i < con->num_res; i++)
      LST_INT_EL(output,7,(int) i) = con->resultSetIds[i];

    return output;
    }

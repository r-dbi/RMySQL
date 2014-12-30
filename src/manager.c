#include "RS-MySQL.h"

static RS_DBI_manager *dbManager = NULL;

SEXP RS_DBI_allocManager(const char *drvName, int max_con,
  int fetch_default_rec, int force_realloc) {
  /* Currently, the dbManager is a singleton (therefore we don't
  * completly free all the space).  Here we alloc space
  * for the dbManager and return its mgrHandle.  force_realloc
  * means to re-allocate number of connections, etc. (in this case
  * we require to have all connections closed).  (Note that if we
  * re-allocate, we don't re-set the counter, and thus we make sure
  * we don't recycle connection Ids in a giver S/R session).
  */
  SEXP mgrHandle;
  RS_DBI_manager* mgr;
  int counter;
  int mgr_id = 0;
  int i;

  mgrHandle = RS_DBI_asMgrHandle(mgr_id);

  if(!dbManager){                      /* alloc for the first time */
  counter = 0;                       /* connections handled so far */
  mgr = (RS_DBI_manager*) malloc(sizeof(RS_DBI_manager));
  }
  else {                               /* we're re-entering */
  if(dbManager->connections){        /* and mgr is valid */
  if(!force_realloc)
    return mgrHandle;
  else
    RS_DBI_freeManager(mgrHandle);  /* i.e., free connection arrays*/
  }
  counter = dbManager->counter;
  mgr = dbManager;
  }
  /* Ok, we're here to expand number of connections, etc.*/
  if(!mgr)
    RS_DBI_errorMessage("could not malloc the dbManger", RS_DBI_ERROR);
  mgr->drvName = RS_DBI_copyString(drvName);
  mgr->drvData = (void *) NULL;
  mgr->managerId = mgr_id;
  mgr->connections =  (RS_DBI_connection **)
    calloc((size_t) max_con, sizeof(RS_DBI_connection));
  if(!mgr->connections){
    free(mgr);
    RS_DBI_errorMessage("could not calloc RS_DBI_connections", RS_DBI_ERROR);
  }
  mgr->connectionIds = (int *) calloc((size_t)max_con, sizeof(int));
  if(!mgr->connectionIds){
    free(mgr->connections);
    free(mgr);
    RS_DBI_errorMessage("could not calloc vector of connection Ids",
      RS_DBI_ERROR);
  }
  mgr->counter = counter;
  mgr->length = max_con;
  mgr->num_con = (int) 0;
  mgr->fetch_default_rec = fetch_default_rec;
  for(i=0; i < max_con; i++){
    mgr->connectionIds[i] = -1;
    mgr->connections[i] = (RS_DBI_connection *) NULL;
  }

  dbManager = mgr;

  return mgrHandle;
}

/* We don't want to completely free the dbManager, but rather we
* re-initialize all the fields except for mgr->counter to ensure we don't
* re-cycle connection ids across R/S DBI sessions in the the same pid
* (S/R session).
*/
void
  RS_DBI_freeManager(SEXP mgrHandle)
  {
    RS_DBI_manager *mgr;

    mgr = RS_DBI_getManager(mgrHandle);
    if(mgr->num_con > 0){
      char *errMsg = "all opened connections were forcebly closed";
      RS_DBI_errorMessage(errMsg, RS_DBI_WARNING);
    }
    if(mgr->drvData){
      char *errMsg = "mgr->drvData was not freed (some memory leaked)";
      RS_DBI_errorMessage(errMsg, RS_DBI_WARNING);
    }
    if(mgr->drvName){
      free(mgr->drvName);
      mgr->drvName = (char *) NULL;
    }
    if(mgr->connections) {
      free(mgr->connections);
      mgr->connections = (RS_DBI_connection **) NULL;
    }
    if(mgr->connectionIds) {
      free(mgr->connectionIds);
      mgr->connectionIds = (int *) NULL;
    }
    return;
  }


SEXP RS_DBI_asMgrHandle(int mgrId)
{
  SEXP mgrHandle;

  PROTECT(mgrHandle = NEW_INTEGER((int) 1));
  MGR_ID(mgrHandle) = mgrId;
  UNPROTECT(1);
  return mgrHandle;
}

RS_DBI_manager* RS_DBI_getManager(SEXP  handle) {
  RS_DBI_manager *mgr;

  if(!is_validHandle(handle, MGR_HANDLE_TYPE))
    RS_DBI_errorMessage("invalid dbManager handle", RS_DBI_ERROR);
  mgr = dbManager;
  if(!mgr)
    RS_DBI_errorMessage(
      "internal error in RS_DBI_getManager: corrupt dbManager handle",
      RS_DBI_ERROR);
  return mgr;
}


SEXP         /* named list */
RS_DBI_managerInfo(SEXP mgrHandle)
{

  RS_DBI_manager *mgr;
  SEXP output;
  int  i, num_con;
  int n = (int) 7;
  char *mgrDesc[] = {"connectionIds", "fetch_default_rec","managerId",
    "length", "num_con", "counter", "clientVersion"};
  SEXPTYPE mgrType[] = {INTSXP, INTSXP, INTSXP,
    INTSXP, INTSXP, INTSXP,
    STRSXP};
  int  mgrLen[]  = {1, 1, 1, 1, 1, 1, 1};

  mgr = RS_DBI_getManager(mgrHandle);
  num_con = (int) mgr->num_con;
  mgrLen[0] = num_con;

  output = RS_DBI_createNamedList(mgrDesc, mgrType, mgrLen, n);

  for(i = 0; i < num_con; i++)
    LST_INT_EL(output,0,i) = (int) mgr->connectionIds[i];

  LST_INT_EL(output,1,0) = (int) mgr->fetch_default_rec;
  LST_INT_EL(output,2,0) = (int) mgr->managerId;
  LST_INT_EL(output,3,0) = (int) mgr->length;
  LST_INT_EL(output,4,0) = (int) mgr->num_con;
  LST_INT_EL(output,5,0) = (int) mgr->counter;
  SET_LST_CHR_EL(output,6,0,C_S_CPY("NA"));   /* client versions? */

return output;
}

SEXP RS_DBI_validHandle(SEXP handle) {
  int  handleType = 0;

  switch( (int) GET_LENGTH(handle)){
  case MGR_HANDLE_TYPE:
    handleType = MGR_HANDLE_TYPE;
    break;
  case CON_HANDLE_TYPE:
    handleType = CON_HANDLE_TYPE;
    break;
  case RES_HANDLE_TYPE:
    handleType = RES_HANDLE_TYPE;
    break;
  }

  return ScalarLogical(is_validHandle(handle, handleType));
}

int is_validHandle(SEXP handle, HANDLE_TYPE handleType) {
  int  len, indx;
  RS_DBI_manager    *mgr;
  RS_DBI_connection *con;

  if(IS_INTEGER(handle))
    handle = AS_INTEGER(handle);
  else
    return 0;       /* non handle object */

  len = (int) GET_LENGTH(handle);
  if(len<handleType || handleType<1 || handleType>3)
    return 0;

  /* at least we have a potential valid dbManager */
  mgr = dbManager;
  if(!mgr || !mgr->connections)  return 0;   /* expired manager*/
  if(handleType == MGR_HANDLE_TYPE) return 1;     /* valid manager id */

  /* ... on to connections */
  indx = RS_DBI_lookup(mgr->connectionIds, mgr->length, CON_ID(handle));
  if(indx<0) return 0;
  con = mgr->connections[indx];
  if(!con) return 0;
  if(!con->resultSets) return 0;       /* un-initialized (invalid) */
  if(handleType==CON_HANDLE_TYPE) return 1; /* valid connection id */

  /* .. on to resultSets */
  indx = RS_DBI_lookup(con->resultSetIds, con->length, RES_ID(handle));
  if(indx < 0) return 0;
  if(!con->resultSets[indx]) return 0;

  return 1;
}





SEXP
  RS_MySQL_init(SEXP config_params, SEXP reload)
  {
    /* Currently we can specify the defaults for 2 parameters, max num of
    * connections, and max of records per fetch (this can be over-ridden
    * explicitly in the S call to fetch).
    */
    SEXP mgrHandle;
    int  fetch_default_rec, force_reload, max_con;
    const char *drvName = "MySQL";

    max_con = INTEGER(config_params)[0];
    fetch_default_rec = INTEGER(config_params)[1];
    force_reload = LOGICAL(reload)[0];

    mgrHandle = RS_DBI_allocManager(drvName, max_con, fetch_default_rec,
      force_reload);
    return mgrHandle;
  }

SEXP
  RS_MySQL_closeManager(SEXP mgrHandle)
  {
    RS_DBI_manager *mgr;

    mgr = RS_DBI_getManager(mgrHandle);
    if(mgr->num_con)
      RS_DBI_errorMessage(
        "there are opened connections -- close them first",
        RS_DBI_ERROR);

    RS_DBI_freeManager(mgrHandle);

    return ScalarLogical(TRUE);
  }


SEXP
  RS_MySQL_managerInfo(SEXP mgrHandle)
  {
    RS_DBI_manager *mgr;
    SEXP output;
    int i, num_con, max_con, *cons, ncon;
    int j, n = 8;
    char *mgrDesc[] = {"drvName",   "connectionIds", "fetch_default_rec",
      "managerId", "length",        "num_con",
      "counter",   "clientVersion"};
    SEXPTYPE mgrType[] = {STRSXP, INTSXP, INTSXP,
      INTSXP,   INTSXP, INTSXP,
      INTSXP,   STRSXP};
    int  mgrLen[]  = {1, 1, 1, 1, 1, 1, 1, 1};

    mgr = RS_DBI_getManager(mgrHandle);
    if(!mgr)
      RS_DBI_errorMessage("driver not loaded yet", RS_DBI_ERROR);
    num_con = (int) mgr->num_con;
    max_con = (int) mgr->length;
    mgrLen[1] = num_con;

    output = RS_DBI_createNamedList(mgrDesc, mgrType, mgrLen, n);

    j = (int) 0;
    if(mgr->drvName)
      SET_LST_CHR_EL(output,j++,0,C_S_CPY(mgr->drvName));
    else
      SET_LST_CHR_EL(output,j++,0,C_S_CPY(""));

    cons = (int *) S_alloc((long)max_con, (int)sizeof(int));
    ncon = RS_DBI_listEntries(mgr->connectionIds, mgr->length, cons);
    if(ncon != num_con){
      RS_DBI_errorMessage(
        "internal error: corrupt RS_DBI connection table",
        RS_DBI_ERROR);
    }

    for(i = 0; i < num_con; i++)
      LST_INT_EL(output, j, i) = cons[i];
    j++;
    LST_INT_EL(output,j++,0) = mgr->fetch_default_rec;
    LST_INT_EL(output,j++,0) = mgr->managerId;
    LST_INT_EL(output,j++,0) = mgr->length;
    LST_INT_EL(output,j++,0) = mgr->num_con;
    LST_INT_EL(output,j++,0) = mgr->counter;
    SET_LST_CHR_EL(output,j++,0,C_S_CPY(mysql_get_client_info()));

    return output;
  }

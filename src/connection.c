#include "RS-MySQL.h"

/* RS_MySQL_createConnection - internal function
 *
 * Used by both RS_MySQL_newConnection and RS_MySQL_cloneConnection.
 * It is responsible for the memory associated with conParams.
 */
SEXP RS_MySQL_createConnection(SEXP mgrHandle, RS_MySQL_conParams *conParams) {
  RS_DBI_connection  *con;
  SEXP conHandle;
  MYSQL     *my_connection;

  if(!is_validHandle(mgrHandle, MGR_HANDLE_TYPE))
    RS_DBI_errorMessage("invalid MySQLManager", RS_DBI_ERROR);

  /* Initialize MySQL connection */
  my_connection = mysql_init(NULL);
  // Always enable INFILE option, since needed for dbWriteTable
  mysql_options(my_connection, MYSQL_OPT_LOCAL_INFILE, 0);

  /* Load MySQL default connection values from a group.
  *
  * MySQL will combine the options found in the '[client]' group and one more group
  * specified by MYSQL_READ_DEFAULT_GROUP. Typically, this will
  * be '[rs-dbi]' but the user can override with another group. Note that
  * while our interface will allow a user to pass in a vector of groups,
  * only the first group in the vector will be combined with '[client]'.
  *
  * Should we make this an error in a later release?)
  */
  if (conParams->groups)
    mysql_options(my_connection, MYSQL_READ_DEFAULT_GROUP, conParams->groups);

  /* MySQL reads defaults from my.cnf or equivalent, but the user can supply
  * an alternative.
  */
  if(conParams->default_file)
    mysql_options(my_connection, MYSQL_READ_DEFAULT_FILE, conParams->default_file);

  if(!mysql_real_connect(my_connection,
    conParams->host, conParams->username, conParams->password, conParams->dbname,
    conParams->port, conParams->unix_socket, conParams->client_flag)){

    RS_MySQL_freeConParams(conParams);

    char buf[2048];
    sprintf(buf, "Failed to connect to database: Error: %s\n", mysql_error(my_connection));
    RS_DBI_errorMessage(buf, RS_DBI_ERROR);
  }

  /* MySQL connections can only have 1 result set open at a time */
  conHandle = RS_DBI_allocConnection(mgrHandle, (int) 1);
  con = RS_DBI_getConnection(conHandle);
  if(!con){
    mysql_close(my_connection);
    RS_MySQL_freeConParams(conParams);
    RS_DBI_errorMessage("could not alloc space for connection object",
      RS_DBI_ERROR);
  }

  con->conParams = (void *) conParams;
  con->drvConnection = (void *) my_connection;

  return conHandle;
}

SEXP RS_DBI_allocConnection(SEXP mgrHandle, int max_res) {
  MySQLDriver* mgr = rmysql_driver();

  int indx = RS_DBI_newEntry(mgr->connectionIds, mgr->length);
  if (indx < 0) {
    error(
      "Cannot allocate a new connection: %d connections already opened",
      mgr->length
    );
  }

  RS_DBI_connection* con = malloc(sizeof(RS_DBI_connection));
  if (!con){
    error("Could not allocate memory for connection");
  }

  int con_id = mgr->counter;
  con->connectionId = con_id;
  con->drvConnection = (void *) NULL;
  con->conParams = (void *) NULL;
  con->counter = (int) 0;
  con->length = max_res; /* length of resultSet vector */

  /* result sets for this connection */
  con->resultSets = calloc(max_res, sizeof(RS_DBI_resultSet));
  if (!con->resultSets) {
    error("Could not allocate memory for result sets");
  }

  con->num_res = (int) 0;
  con->resultSetIds = (int *) calloc((size_t) max_res, sizeof(int));
  if (!con->resultSetIds) {
    error("Could not allocate memory for result set ids");
  }
  for(int i = 0; i < max_res; i++){
    con->resultSets[i] = (RS_DBI_resultSet *) NULL;
    con->resultSetIds[i] = -1;
  }

  /* Finally, update connection table in mgr */
  mgr->num_con += 1;
  mgr->counter += 1;
  mgr->connections[indx] = con;
  mgr->connectionIds[indx] = con_id;
  SEXP conHandle = RS_DBI_asConHandle(MGR_ID(mgrHandle), con_id);
  return conHandle;
}

/* the invoking (freeing) function must provide a function for
* freeing the conParams, and by setting the (*free_drvConParams)(void *)
* pointer.
*/
void RS_DBI_freeConnection(SEXP conHandle) {
  int indx;

  RS_DBI_connection* con = RS_DBI_getConnection(conHandle);
  MySQLDriver* mgr = rmysql_driver();

  /* Are there open resultSets? If so, free them first */
  if (con->num_res > 0) {
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

SEXP RS_DBI_asConHandle(int mgrId, int conId) {
  SEXP conHandle;

  PROTECT(conHandle = NEW_INTEGER((int) 2));
  MGR_ID(conHandle) = mgrId;
  CON_ID(conHandle) = conId;
  UNPROTECT(1);
  return conHandle;
}

RS_DBI_connection* RS_DBI_getConnection(SEXP conHandle) {
  MySQLDriver  *mgr;
  int indx;

  mgr = rmysql_driver();
  indx = RS_DBI_lookup(mgr->connectionIds, mgr->length, CON_ID(conHandle));
  if(indx < 0)
    error("internal error in RS_DBI_getConnection: corrupt connection handle");
  if(!mgr->connections[indx])
    error("internal error in RS_DBI_getConnection: corrupt connection  object");
  return mgr->connections[indx];
}


SEXP RS_DBI_connectionInfo(SEXP conHandle) {

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
  SET_LST_CHR_EL(output,0,0,mkChar("NA"));        /* host */
  SET_LST_CHR_EL(output,1,0,mkChar("NA"));        /* dbname */
  SET_LST_CHR_EL(output,2,0,mkChar("NA"));        /* user */
  SET_LST_CHR_EL(output,3,0,mkChar("NA"));        /* conType */
  SET_LST_CHR_EL(output,4,0,mkChar("NA"));        /* serverVersion */

  LST_INT_EL(output,5,0) = (int) -1;            /* protocolVersion */
  LST_INT_EL(output,6,0) = (int) -1;            /* threadId */

  for(i=0; i < con->num_res; i++)
    LST_INT_EL(output,7,(int) i) = con->resultSetIds[i];

  return output;
}


/* Are there more results on this connection (as in multi results or
 * SQL scripts
 */

SEXP RS_MySQL_moreResultSets(SEXP conHandle) {
  RS_DBI_connection *con;
  MYSQL             *my_connection;
  my_bool           tmp;

  con = RS_DBI_getConnection(conHandle);
  my_connection = (MYSQL *) con->drvConnection;

  tmp = mysql_more_results(my_connection);
  return ScalarLogical(tmp);
}


/* open a connection with the same parameters used for in conHandle */
SEXP RS_MySQL_cloneConnection(SEXP conHandle) {

  return RS_MySQL_createConnection(
    ScalarInteger(0),
    RS_MySQL_cloneConParams(RS_DBI_getConnection(conHandle)->conParams));
}


RS_MySQL_conParams* RS_MySQL_allocConParams(void) {
  RS_MySQL_conParams *conParams;

  conParams = (RS_MySQL_conParams *) malloc(sizeof(RS_MySQL_conParams));
  if(!conParams){
    RS_DBI_errorMessage("could not malloc space for connection params",
      RS_DBI_ERROR);
  }
  conParams->dbname = NULL;
  conParams->username = NULL;
  conParams->password = NULL;
  conParams->host = NULL;
  conParams->unix_socket = NULL;
  conParams->port = 0;
  conParams->client_flag = 0;
  conParams->groups = NULL;
  conParams->default_file = NULL;
  return conParams;
}

RS_MySQL_conParams* RS_MySQL_cloneConParams(RS_MySQL_conParams *cp) {
  RS_MySQL_conParams *new = RS_MySQL_allocConParams();

  if (cp->dbname) new->dbname = RS_DBI_copyString(cp->dbname);
  if (cp->username) new->username = RS_DBI_copyString(cp->username);
  if (cp->password) new->password = RS_DBI_copyString(cp->password);
  if (cp->host) new->host = RS_DBI_copyString(cp->host);
  if (cp->unix_socket) new->unix_socket = RS_DBI_copyString(cp->unix_socket);
  new->port = cp->port;
  new->client_flag = cp->client_flag;
  if (cp->groups) new->groups = RS_DBI_copyString(cp->groups);
  if (cp->default_file) new->default_file = RS_DBI_copyString(cp->default_file);

  return new;
}

void RS_MySQL_freeConParams(RS_MySQL_conParams *conParams) {
  if(conParams->dbname) free(conParams->dbname);
  if(conParams->username) free(conParams->username);
  if(conParams->password) free(conParams->password);
  if(conParams->host) free(conParams->host);
  if(conParams->unix_socket) free(conParams->unix_socket);
  /* port and client_flag are unsigned ints */
  if(conParams->groups) free(conParams->groups);
  if(conParams->default_file) free(conParams->default_file);
  free(conParams);
  return;
}

SEXP RS_MySQL_newConnection(SEXP mgrHandle, SEXP s_dbname, SEXP s_username,
  SEXP s_password, SEXP s_myhost, SEXP s_unix_socket,
  SEXP s_port, SEXP s_client_flag, SEXP s_groups,
  SEXP s_default_file) {

  RS_MySQL_conParams *conParams;

  if(!is_validHandle(mgrHandle, MGR_HANDLE_TYPE))
    RS_DBI_errorMessage("invalid MySQLManager", RS_DBI_ERROR);

  /* Create connection parameters structure and initialize */
  conParams = RS_MySQL_allocConParams();

  /* Arguments override defaults in config file */
  if(s_dbname != R_NilValue)
    conParams->dbname = RS_DBI_copyString(CHAR(asChar(s_dbname)));
  if(s_username != R_NilValue)
    conParams->username = RS_DBI_copyString(CHAR(asChar(s_username)));
  if(s_password != R_NilValue)
    conParams->password = RS_DBI_copyString(CHAR(asChar(s_password)));
  if(s_myhost != R_NilValue)
    conParams->host = RS_DBI_copyString(CHAR(asChar(s_myhost)));
  if(s_unix_socket != R_NilValue)
    conParams->unix_socket = RS_DBI_copyString(CHAR(asChar(s_unix_socket)));
  if (s_port != R_NilValue)
    conParams->port = asInteger(s_port);
  if (s_client_flag != R_NilValue)
    conParams->client_flag = asInteger(s_client_flag);
  if(s_groups != R_NilValue)
    conParams->groups = RS_DBI_copyString(CHAR(asChar(s_groups)));
  if(s_default_file != R_NilValue)
    conParams->default_file = RS_DBI_copyString(CHAR(asChar(s_default_file)));

  return RS_MySQL_createConnection(mgrHandle, conParams);
}

SEXP RS_MySQL_closeConnection(SEXP conHandle) {
  RS_DBI_connection *con;
  MYSQL *my_connection;

  con = RS_DBI_getConnection(conHandle);
  if(con->num_res>0){
    RS_DBI_errorMessage(
      "close the pending result sets before closing this connection",
      RS_DBI_ERROR);
  }
  /* make sure we first free the conParams and mysql connection from
   * the RS-RBI connection object.
   */
  if(con->conParams){
    RS_MySQL_freeConParams(con->conParams);
    con->conParams = (RS_MySQL_conParams *) NULL;
  }
  my_connection = (MYSQL *) con->drvConnection;
  mysql_close(my_connection);
  con->drvConnection = (void *) NULL;

  RS_DBI_freeConnection(conHandle);

  return ScalarLogical(TRUE);
}

SEXP RS_MySQL_connectionInfo(SEXP conHandle) {
  MYSQL   *my_con;
  RS_MySQL_conParams *conParams;
  RS_DBI_connection  *con;
  SEXP output;
  int       i, n = 8, *res, nres;
  char *conDesc[] = {"host", "user", "dbname", "conType",
    "serverVersion", "protocolVersion",
    "threadId", "rsId"};
  SEXPTYPE conType[] = {STRSXP, STRSXP, STRSXP,
    STRSXP, STRSXP, INTSXP,
    INTSXP, INTSXP};
  int  conLen[]  = {1, 1, 1, 1, 1, 1, 1, 1};
  char *tmp;

  con = RS_DBI_getConnection(conHandle);
  conLen[7] = con->num_res;         /* num of open resultSets */
  my_con = (MYSQL *) con->drvConnection;
  output = RS_DBI_createNamedList(conDesc, conType, conLen, n);

  conParams = (RS_MySQL_conParams *) con->conParams;

  PROTECT(output);

  tmp = conParams->host? conParams->host : (my_con->host?my_con->host:"");
  SET_LST_CHR_EL(output,0,0,mkChar(tmp));
  tmp = conParams->username? conParams->username : (my_con->user?my_con->user:"");
  SET_LST_CHR_EL(output,1,0,mkChar(tmp));
  tmp = conParams->dbname? conParams->dbname : (my_con->db?my_con->db:"");
  SET_LST_CHR_EL(output,2,0,mkChar(tmp));
  SET_LST_CHR_EL(output,3,0,mkChar(mysql_get_host_info(my_con)));
  SET_LST_CHR_EL(output,4,0,mkChar(mysql_get_server_info(my_con)));

  LST_INT_EL(output,5,0) = (int) mysql_get_proto_info(my_con);
  LST_INT_EL(output,6,0) = (int) mysql_thread_id(my_con);

  res = (int *) S_alloc( (long) con->length, (int) sizeof(int));
  nres = RS_DBI_listEntries(con->resultSetIds, con->length, res);
  if(nres != con->num_res){
    UNPROTECT(1);
    RS_DBI_errorMessage(
      "internal error: corrupt RS_DBI resultSet table",
      RS_DBI_ERROR);
  }
  for( i = 0; i < con->num_res; i++){
    LST_INT_EL(output,7,i) = (int) res[i];
  }
  UNPROTECT(1);

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
  int  mgr_id, len, indx;
  MySQLDriver    *mgr = rmysql_driver();
  RS_DBI_connection *con;

  if(IS_INTEGER(handle))
    handle = AS_INTEGER(handle);
  else
    return 0;       /* non handle object */

    len = (int) GET_LENGTH(handle);
    if(len<handleType || handleType<1 || handleType>3)
      return 0;
    mgr_id = MGR_ID(handle);

    /* at least we have a potential valid dbManager */
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

/* 
 * $Id$
 *
 *
 * Copyright (C) 1999-2002 The Omega Project for Statistical Computing.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

#include "RS-MySQL.h"

/* as of version 4.0 mysql_get_client_version() only returns a string */
static char *compiled_mysql_client_version = MYSQL_SERVER_VERSION;   /* sic.*/

#ifndef USING_R
#  error("the function RS_DBI_invokeBeginGroup() has not been implemented in S")
#  error("the function RS_DBI_invokeEndGroup()   has not been implemented in S")
#  error("the function RS_DBI_invokeNewRecord()  has not been implemented in S")
#endif

/* R and S DataBase Interface to MySQL
 * 
 *
 * C Function library which can be used to run SQL queries from
 * inside of S4, Splus5.x, or R.
 * This driver hooks R/S and MySQL and implements the proposed S-DBI
 * generic R/S-database interface 0.2.
 * 
 * For details see
 *  On S, Appendix A in "Programming with Data" by John M. Chambers. 
 *  On R, "The .Call and .External Interfaces"  in the R manual.
 *  On MySQL, 
       The "MySQL Reference Manual (version 3.23.7 alpha,
 *         02 Dec 1999" and the O'Reilly book "MySQL & mSQL" by Yarger, 
 *         Reese, and King.
 *     Also, "MySQL" by Paul Dubois (2000) New Riders Publishing.
 *
 * TODO:
 *    1. Make sure the code is thread-safe, in particular,
 *       we need to remove the PROBLEM ... ERROR macros
 *       in RS_DBI_errorMessage() because it's definetely not 
 *       thread-safe.  But see RS_DBI_setException().
 *    2. Apparently, MySQL treats TEXT as BLOB's (actually, it seems
 *       that MySQL makes no clear distinction between BLOB's and
 *       CLOB's).  Need to resolve this once and for all.
 */


Mgr_Handle *
RS_MySQL_init(s_object *config_params, s_object *reload)
{
  S_EVALUATOR

  /* Currently we can specify the defaults for 2 parameters, max num of
   * connections, and max of records per fetch (this can be over-ridden
   * explicitly in the S call to fetch).
   */
  Mgr_Handle *mgrHandle;
  Sint  fetch_default_rec, force_reload, max_con;
  const char *drvName = "MySQL";
  const char *clientVersion = mysql_get_client_info();

  /* make sure the versions of the runtime and compile-time of the 
   * MySQL client library are reasonably close, e.g., first major
   * and minor id, e.g., 41 for 4.1.*, 42 for 4.2.*, etc.
   * TODO: Should we using PROTOCOL_VERSION here?
   */
  if(strncmp(clientVersion, compiled_mysql_client_version, (size_t) 2)){
     char  buf[256];
     (void) sprintf(buf, 
                    "%s mismatch between compiled version %s and runtime version %s",
                    drvName, compiled_mysql_client_version, clientVersion);
     RS_DBI_errorMessage(buf, RS_DBI_WARNING);
  }
  max_con = INT_EL(config_params,0); 
  fetch_default_rec = INT_EL(config_params,1);
  force_reload = LGL_EL(reload,0);

  mgrHandle = RS_DBI_allocManager(drvName, max_con, fetch_default_rec, 
			     force_reload);
  return mgrHandle;
} 

s_object *
RS_MySQL_closeManager(Mgr_Handle *mgrHandle)
{
  S_EVALUATOR

  RS_DBI_manager *mgr;
  s_object *status;

  mgr = RS_DBI_getManager(mgrHandle);
  if(mgr->num_con)
    RS_DBI_errorMessage("there are opened connections -- close them first",
			RS_DBI_ERROR);

  RS_DBI_freeManager(mgrHandle);

  MEM_PROTECT(status = NEW_LOGICAL((Sint) 1));
  LGL_EL(status,0) = TRUE;
  MEM_UNPROTECT(1);
  return status;
}

/* open a connection with the same parameters used for in conHandle */
Con_Handle *
RS_MySQL_cloneConnection(Con_Handle *conHandle)
{
  S_EVALUATOR

  Mgr_Handle  *mgrHandle;
  RS_DBI_connection  *con;
  RS_MySQL_conParams *conParams;
  s_object    *con_params, *MySQLgroups, *s_mysql_default_file;
  char   buf1[256], buf2[256];

  /* get connection params used to open existing connection */
  con = RS_DBI_getConnection(conHandle);
  conParams = con->conParams;

  /* will not used the "group" MySQL config file info, nor the
   * default_file (use dummy ones) 
   */
  MEM_PROTECT(MySQLgroups = NEW_CHARACTER((Sint) 1)) ;
  SET_CHR_EL(MySQLgroups,0,C_S_CPY(""));

  MEM_PROTECT(s_mysql_default_file = NEW_CHARACTER((Sint) 1)) ;
  SET_CHR_EL(s_mysql_default_file,0,C_S_CPY(""));

  mgrHandle = RS_DBI_asMgrHandle(MGR_ID(conHandle));
  
  /* Connection parameters need to be put into a 7-element character
   * vector to be passed to the RS_MySQL_newConnection() function.
   */
  MEM_PROTECT(con_params = NEW_CHARACTER((Sint) 7));
  SET_CHR_EL(con_params,0,C_S_CPY(conParams->user));
  SET_CHR_EL(con_params,1,C_S_CPY(conParams->passwd));
  SET_CHR_EL(con_params,2,C_S_CPY(conParams->host));
  SET_CHR_EL(con_params,3,C_S_CPY(conParams->dbname));
  SET_CHR_EL(con_params,4,C_S_CPY(conParams->unix_socket));
  sprintf(buf1, "%d", conParams->port);
  sprintf(buf2, "%d", conParams->client_flags);
  SET_CHR_EL(con_params,5,C_S_CPY(buf1));
  SET_CHR_EL(con_params,6,C_S_CPY(buf2));
  
  MEM_UNPROTECT(3); 

  return RS_MySQL_newConnection(mgrHandle, con_params, MySQLgroups,
             s_mysql_default_file);
}

RS_MySQL_conParams *
RS_mysql_allocConParams(void)
{
  RS_MySQL_conParams *conParams;

  conParams = (RS_MySQL_conParams *)
     malloc(sizeof(RS_MySQL_conParams));
  if(!conParams){
    RS_DBI_errorMessage("could not malloc space for connection params",
                       RS_DBI_ERROR);
  }
  return conParams;
}

void
RS_MySQL_freeConParams(RS_MySQL_conParams *conParams)
{
  if(conParams->host) free(conParams->host);
  if(conParams->dbname) free(conParams->dbname);
  if(conParams->user) free(conParams->user);
  if(conParams->passwd) free(conParams->passwd);
  if(conParams->unix_socket) free(conParams->unix_socket);
  /* port and client_flags are unsigned ints */
  free(conParams);
  return;
}

Con_Handle *
RS_MySQL_newConnection(Mgr_Handle *mgrHandle, s_object *con_params, 
		       s_object *MySQLgroups, s_object *s_mysql_default_file)
{
  S_EVALUATOR

  RS_DBI_connection  *con;
  RS_MySQL_conParams *conParams;
  Con_Handle  *conHandle;
  MYSQL     *my_connection;
  unsigned int  p, port, client_flags;
  char      *user = NULL, *passwd = NULL, *host = NULL, *dbname = NULL;
  char      *unix_socket = NULL;
  int       i; 
  Sint      ngroups;
  char      **groups, *mysql_default_file;
#if HAVE_GETOPT_LONG
  int   argc, option_index;    /* TODO: What about Mac OS, OS X??     */
  char  **argv;                /* we do have MySQL's load_defaults() */
#else
  char  *tmp;  /* we'll have to peek into the MYSQL connection obj, darn!*/
#endif

  if(!is_validHandle(mgrHandle, MGR_HANDLE_TYPE))
    RS_DBI_errorMessage("invalid MySQLManager", RS_DBI_ERROR);

  my_connection = mysql_init(NULL);

#if defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID > 32348
  /* (BDR's fix)
   * Starting w.  MySQL 3.23.39, LOAD DATA INFILE may be disabled (although
   * the default is enabled);  since assignTable() depends on it,
   * we unconditionally enable it.
   */
  mysql_options(my_connection, MYSQL_OPT_LOCAL_INFILE, 0);
#endif

  /* Load MySQL default connection values from the [client] and
   * [rs-dbi] sections of the MySQL configuration files.  We
   * recognize options in the [client] and [rs-dbi] sections, plus any
   * other passed from S/R in the MySQLgroups character vector.  
   * Note that we're faking the argc and argv -- it's just simpler (and the
   * recommended way from MySQL perspective).  Re-initialize the
   * getopt_long buffers by setting optind = 0 (defined in getopt.h),
   * this is needed to avoid getopt_long "remembering" options from
   * one invocation to the next.  
   * TODO: This can't be thread-safe, can it?
   *
   */
  if(GET_LENGTH(s_mysql_default_file)==1){
      mysql_default_file = RS_DBI_copyString(CHR_EL(s_mysql_default_file,0));
      mysql_options(my_connection, MYSQL_READ_DEFAULT_FILE, mysql_default_file);
  }
  ngroups = GET_LENGTH(MySQLgroups);
  groups = (char **) S_alloc((long) ngroups+3, (int) sizeof(char **));
  groups[0] = RS_DBI_copyString("client");  
  groups[1] = RS_DBI_copyString("rs-dbi");
  groups[ngroups+2] = NULL;       /* required sentinel */

  /* the MySQL doc asserts that the [client] group is always processed,
   * so we skip it...
  */
  mysql_options(my_connection, MYSQL_READ_DEFAULT_GROUP,  groups[1]);
  for(i=0; i<ngroups; i++){
    groups[i+2] = RS_DBI_copyString(CHR_EL(MySQLgroups,i));
    mysql_options(my_connection, MYSQL_READ_DEFAULT_GROUP,  groups[i+2]);
  }
#if HAVE_GETOPT_LONG
  argc = 1;
  argv = (char **) S_alloc((long) 1, (int) sizeof(char **));
  argv[0] = (char *) RS_DBI_copyString("dummy"); 

  load_defaults("my",(const char **) groups, &argc, &argv);
  option_index = optind = 0;
  while(1){
    int nc;
    char c;
    struct option long_options[] = {
      {"host",     required_argument, NULL, 'h'},
      {"user",     required_argument, NULL, 'u'},
      {"password", required_argument, NULL, 'p'},
      {"port",     required_argument, NULL, 'P'},
      {"socket",   required_argument, NULL, 's'},
      {"database", required_argument, NULL, 'd'},
      {0, 0, 0, 0}
    };
    nc = getopt_long(argc, argv, "h:u:p:d:P:s:", long_options, &option_index);
    if(nc == -1)
      break;
    c = (char) nc;
    switch(c){
    case 'h': host = optarg;   break;
    case 'u': user = optarg;   break;
    case 'p': passwd = optarg; break;
    case 'd': dbname = optarg; break;
    case 'P': port = (unsigned int) atoi(optarg); break;
    case 's': unix_socket = optarg;  break;
    }
  }
#endif  /* HAVE_GETOPT_LONG */
  
#define IS_EMPTY(s1)   !strcmp((s1), "")

  /* R/S arguments take precedence over configuration file defaults. */
  if(!IS_EMPTY(CHR_EL(con_params,0)))
    user =  CHR_EL(con_params,0);
  if(!IS_EMPTY(CHR_EL(con_params,1)))
    passwd= CHR_EL(con_params,1);
  if(!IS_EMPTY(CHR_EL(con_params,2)))
    host = CHR_EL(con_params,2);
  if(!IS_EMPTY(CHR_EL(con_params,3)))
    dbname = CHR_EL(con_params,3);
  if(!IS_EMPTY(CHR_EL(con_params,4)))
    unix_socket = CHR_EL(con_params,4);
  p = (unsigned int) atol(CHR_EL(con_params,5));
  port = (p > 0) ? p : 0;
  client_flags = (unsigned int) atol(CHR_EL(con_params,6));

  my_connection = 
    mysql_real_connect(my_connection, host, user, passwd, dbname, 
		       port, unix_socket, client_flags);
  if(!my_connection){
    char buf[512];
    sprintf(buf, "could not connect %s@%s on dbname \"%s\"\n",
	    user, host, dbname);
    RS_DBI_errorMessage(buf, RS_DBI_ERROR);
  }

  conParams = RS_mysql_allocConParams();

#if ! HAVE_GETOPT_LONG
  /* On Windows, MySQL (3.23+?) doesn't have the load_defaults() function
   * included in the shared libMySQL.dll lib (only in the static, threaded
   * mysqlclient.lib.) Thus we need to extract the host/dbname/user/passwd
   * from the (non-API) internals of the MYSQL connection object (we're
   * on thin ice here...) to put it in our internal conParams object.
   * This is needed in the RS_DBI_connection object in order to clone 
   * connections (for instance, in quickSQL, getInfo).
   */
   host = my_connection->host;
   dbname = my_connection->db;
   unix_socket = my_connection->unix_socket;   /* may be null */
   passwd = my_connection->passwd;
   tmp = my_connection->user;        /* possibly as 'user@machinename' */
   user = (char *) S_alloc((long)strlen(tmp)+1, (int)sizeof(char));
   for(i=0; tmp[i]; i++)
      if(tmp[i]!='@') 
         user[i] = tmp[i];
      else {
         user[i] = '\0';
         break;
      }
#endif    /* ! HAVE_GETOPT_LONG */
  /* save actual connection parameters */
  if(!user) 
     user = ""; 
  conParams->user = RS_DBI_copyString(user);
  if(!passwd) 
     passwd = "";
  conParams->passwd = RS_DBI_copyString(passwd);
  if(!host) 
     host = "";
  conParams->host = RS_DBI_copyString(host);
  if(!dbname) 
     dbname = "";
  conParams->dbname = RS_DBI_copyString(dbname);
  if(!unix_socket) 
     unix_socket = "";
  conParams->unix_socket = RS_DBI_copyString(unix_socket);
  conParams->port = port;
  conParams->client_flags = client_flags;

  /* MySQL connections can only have 1 result set open at a time */  
  conHandle = RS_DBI_allocConnection(mgrHandle, (Sint) 1); 
  con = RS_DBI_getConnection(conHandle);
  if(!con){
    mysql_close(my_connection);
    RS_MySQL_freeConParams(conParams);
    conParams = (RS_MySQL_conParams *) NULL;
    RS_DBI_errorMessage("could not alloc space for connection object",
                       RS_DBI_ERROR);
  }
  con->drvConnection = (void *) my_connection;
  con->conParams = (void *) conParams;

  return conHandle;
}

s_object *
RS_MySQL_closeConnection(Con_Handle *conHandle)
{
  S_EVALUATOR

  RS_DBI_connection *con;
  MYSQL *my_connection;
  s_object *status;

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
  
  MEM_PROTECT(status = NEW_LOGICAL((Sint) 1));
  LGL_EL(status, 0) = TRUE;
  MEM_UNPROTECT(1);

  return status;
}  
  
/* Execute (currently) one sql statement (INSERT, DELETE, SELECT, etc.),
 * set coercion type mappings between the server internal data types and 
 * S classes.   Returns  an S handle to a resultSet object.
 */
Res_Handle *
RS_MySQL_exec(Con_Handle *conHandle, s_object *statement)
{
  S_EVALUATOR

  RS_DBI_connection *con;
  Res_Handle        *rsHandle;
  RS_DBI_resultSet  *result;
  MYSQL             *my_connection;
  MYSQL_RES         *my_result;
  int      num_fields, state;
  Sint     res_id, is_select;
  char     *dyn_statement;

  con = RS_DBI_getConnection(conHandle);
  my_connection = (MYSQL *) con->drvConnection;
  dyn_statement = RS_DBI_copyString(CHR_EL(statement,0));

  /* Do we have a pending resultSet in the current connection?  
   * MySQL only allows  one resultSet per connection.
   */
  if(con->num_res>0){
    res_id = (Sint) con->resultSetIds[0]; /* recall, MySQL has only 1 res */
    rsHandle = RS_DBI_asResHandle(MGR_ID(conHandle), 
	                          CON_ID(conHandle), res_id);
    result = RS_DBI_getResultSet(rsHandle);
    if(result->completed == 0){
      free(dyn_statement);
      RS_DBI_errorMessage( 
       "connection with pending rows, close resultSet before continuing",
       RS_DBI_ERROR);
    }
    else 
      RS_MySQL_closeResultSet(rsHandle);
  }

  /* Here is where we actually run the query */
  state = mysql_query(my_connection, dyn_statement);
  if(state) { 
    char errMsg[256];
    free(dyn_statement);
    (void) sprintf(errMsg, "could not run statement: %s",
		   mysql_error(my_connection));
    RS_DBI_errorMessage(errMsg, RS_DBI_ERROR);
  }
  /* Do we need output column/field descriptors?  Only for SELECT-like
   * statements. The MySQL reference manual suggests invoking
   * mysql_use_result() and if it succeed the statement is SELECT-like
   * that can use a resultSet.  Otherwise call mysql_field_count()
   * and if it returns zero, the sql was not a SELECT-like statement.
   * Finally a non-zero means a failed SELECT-like statement.
   */
  my_result = mysql_use_result(my_connection);
  if(!my_result)
    my_result = (MYSQL_RES *) NULL;
    
  num_fields = (int) mysql_field_count(my_connection);
  is_select = (Sint) TRUE;
  if(!my_result){
    if(num_fields>0){
      free(dyn_statement);
      RS_DBI_errorMessage("error in select/select-like", RS_DBI_ERROR);
    }
    else 
      is_select = FALSE;
  }

  /* we now create the wrapper and copy values */
  rsHandle = RS_DBI_allocResultSet(conHandle);
  result = RS_DBI_getResultSet(rsHandle);
  result->statement = RS_DBI_copyString(dyn_statement);
  result->drvResultSet = (void *) my_result;
  result->rowCount = (Sint) 0;
  result->isSelect = is_select;
  if(!is_select){
    result->rowsAffected = (Sint) mysql_affected_rows(my_connection);
    result->completed = 1;
   }
  else {
    result->rowsAffected = (Sint) -1;
    result->completed = 0;
  }
  
  if(is_select)
    result->fields = RS_MySQL_createDataMappings(rsHandle);

  free(dyn_statement);
  return rsHandle;
}

RS_DBI_fields *
RS_MySQL_createDataMappings(Res_Handle *rsHandle)
{
  MYSQL_RES     *my_result;
  MYSQL_FIELD   *select_dp;
  RS_DBI_connection  *con;
  RS_DBI_resultSet   *result; 
  RS_DBI_fields      *flds;
  int     j, num_fields, internal_type;
  char    errMsg[128];

  result = RS_DBI_getResultSet(rsHandle);
  my_result = (MYSQL_RES *) result->drvResultSet;

  /* here we fetch from the MySQL server the field descriptions */
  select_dp = mysql_fetch_fields(my_result);

  con = RS_DBI_getConnection(rsHandle);
  num_fields = (int) mysql_field_count((MYSQL *) con->drvConnection);

  flds = RS_DBI_allocFields(num_fields);

  /* WARNING: TEXT fields are represented as BLOBS (sic),
   * not VARCHAR or some kind of string type. More troublesome is the
   * fact that a TEXT fields can be BINARY to indicate case-sensitivity.
   * The bottom line is that MySQL has a serious deficiency re: text
   * types (IMHO).  A binary object (in SQL92, X/SQL at least) can
   * store all kinds of non-ASCII, non-printable stuff that can
   * potentially screw up S and R CHARACTER_TYPE.  We are on thin ice.
   * 
   * I'm aware that I'm introducing a potential bug here by following
   * the MySQL convention of treating BLOB's as TEXT (I'm symplifying
   * in order to properly handle commonly-found TEXT fields, at the 
   * risk of core dumping when bona fide Binary objects are being
   * retrieved.
   * 
   * Possible workaround: if strlen() of the field equals the
   *    MYSQL_FIELD->length for all rows, then we are probably(?) safe
   * in considering TEXT a character type (non-binary).
   */
  for (j = 0; j < num_fields; j++){

    /* First, save the name, MySQL internal field name, type, length, etc. */
    
    flds->name[j] = RS_DBI_copyString(select_dp[j].name);
    flds->type[j] = select_dp[j].type;  /* recall that these are enum*/
    flds->length[j] = select_dp[j].length;
    flds->precision[j] = select_dp[j].length; 
    flds->scale[j] = select_dp[j].decimals; 
    flds->nullOk[j] = (!IS_NOT_NULL(select_dp[j].flags));

    internal_type = select_dp[j].type;
    switch(internal_type) {
    case FIELD_TYPE_VAR_STRING:
    case FIELD_TYPE_STRING:
      flds->Sclass[j] = CHARACTER_TYPE;
      flds->isVarLength[j] = (Sint) 1;
      break;
    case FIELD_TYPE_TINY:            /* 1-byte TINYINT   */
    case FIELD_TYPE_SHORT:           /* 2-byte SMALLINT  */
    case FIELD_TYPE_INT24:           /* 3-byte MEDIUMINT */
    case FIELD_TYPE_LONG:            /* 4-byte INTEGER   */
    /* if unsigned, turn into numeric (may be too large for ints/long)*/
      if(select_dp[j].flags & UNSIGNED_FLAG)
        flds->Sclass[j] = NUMERIC_TYPE;
      else
        flds->Sclass[j] = INTEGER_TYPE;
      break;
    case FIELD_TYPE_LONGLONG:        /* TODO: can we fit these in R/S ints? */
      if(sizeof(Sint)>=8)            /* Arg! this ain't pretty:-( */
	flds->Sclass[j] = INTEGER_TYPE;
      else 
	flds->Sclass[j] = NUMERIC_TYPE;
    case FIELD_TYPE_DECIMAL:
      if(flds->scale[j] > 0 || flds->precision[j] > 10)
	flds->Sclass[j] = NUMERIC_TYPE;
      else 
	flds->Sclass[j] = INTEGER_TYPE;
      break;
    case FIELD_TYPE_FLOAT:
    case FIELD_TYPE_DOUBLE:
      flds->Sclass[j] = NUMERIC_TYPE;
      break;
    case FIELD_TYPE_BLOB:         /* TODO: how should we bring large ones*/
    case FIELD_TYPE_TINY_BLOB:
    case FIELD_TYPE_MEDIUM_BLOB:
    case FIELD_TYPE_LONG_BLOB:
      flds->Sclass[j] = CHARACTER_TYPE;   /* Grr! Hate this! */
      flds->isVarLength[j] = (Sint) 1;
      break;
    case FIELD_TYPE_DATE:
    case FIELD_TYPE_TIME:
    case FIELD_TYPE_DATETIME:
    case FIELD_TYPE_YEAR:
    case FIELD_TYPE_NEWDATE:
      flds->Sclass[j] = CHARACTER_TYPE;
      flds->isVarLength[j] = (Sint) 1;
      break;
    case FIELD_TYPE_ENUM:
      flds->Sclass[j] = CHARACTER_TYPE;   /* see the MySQL ref. manual */
      flds->isVarLength[j] = (Sint) 1;
      break;
    case FIELD_TYPE_SET:
      flds->Sclass[j] = CHARACTER_TYPE;
      flds->isVarLength[j] = (Sint) 0;
      break;
    default:
      flds->Sclass[j] = CHARACTER_TYPE;
      flds->isVarLength[j] = (Sint) 1;
      (void) sprintf(errMsg, 
		     "unrecognized MySQL field type %d in column %d", 
		     internal_type, j);
      RS_DBI_errorMessage(errMsg, RS_DBI_WARNING);
      break;
    }
  }  
  return flds;
}

s_object *    /* output is a named list */
RS_MySQL_fetch(s_object *rsHandle, s_object *max_rec)
{
  S_EVALUATOR

  RS_DBI_manager   *mgr;
  RS_DBI_resultSet *result;
  RS_DBI_fields    *flds;
  MYSQL_RES *my_result;
  MYSQL_ROW  row;
  s_object  *output, *s_tmp;
#ifndef USING_R
  s_object  *raw_obj, *raw_container;
#endif
  unsigned long  *lens;
  int    i, j, null_item, expand;
  Sint   *fld_nullOk, completed;
  Stype  *fld_Sclass;
  Sint   num_rec; 
  int    num_fields;

  result = RS_DBI_getResultSet(rsHandle);
  flds = result->fields;
  if(!flds)
    RS_DBI_errorMessage("corrupt resultSet, missing fieldDescription",
		       RS_DBI_ERROR);
  num_rec = INT_EL(max_rec,0);
  expand = (num_rec < 0);   /* dyn expand output to accommodate all rows*/
  if(expand || num_rec == 0){
    mgr = RS_DBI_getManager(rsHandle);
    num_rec = mgr->fetch_default_rec;
  }
  num_fields = flds->num_fields;
  MEM_PROTECT(output = NEW_LIST((Sint) num_fields));
  RS_DBI_allocOutput(output, flds, num_rec, 0);
#ifndef USING_R
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc output list",
			RS_DBI_ERROR);
#endif
  fld_Sclass = flds->Sclass;
  fld_nullOk = flds->nullOk;
  
  /* actual fetching....*/
  my_result = (MYSQL_RES *) result->drvResultSet;
  completed = (Sint) 0;
  for(i = 0; ; i++){ 
    if(i==num_rec){  /* exhausted the allocated space */
      if(expand){    /* do we extend or return the records fetched so far*/
	num_rec = 2 * num_rec;
	RS_DBI_allocOutput(output, flds, num_rec, expand);
#ifndef USING_R
	if(IS_LIST(output))
	  output = AS_LIST(output);
	else
	  RS_DBI_errorMessage("internal error: could not alloc output list",
			      RS_DBI_ERROR);
#endif
      }
      else
	break;       /* okay, no more fetching for now */
    }
    row = mysql_fetch_row(my_result);
    if(row==NULL){    /* either we finish or we encounter an error */
      unsigned int  err_no;
      RS_DBI_connection   *con;
      con = RS_DBI_getConnection(rsHandle);
      err_no = mysql_errno((MYSQL *) con->drvConnection);
      completed = (Sint) (err_no ? -1 : 1);
      break;
    }
    lens = mysql_fetch_lengths(my_result); 
    for(j = 0; j < num_fields; j++){

      null_item = (row[j] == NULL);
      switch((int)fld_Sclass[j]){
      case INTEGER_TYPE:
	if(null_item)
	  NA_SET(&(LST_INT_EL(output,j,i)), INTEGER_TYPE);
	else
	  LST_INT_EL(output,j,i) = (Sint) atol(row[j]);
	break;
      case CHARACTER_TYPE:
	/* BUG: I need to verify that a TEXT field (which is stored as
	 * a BLOB by MySQL!) is indeed char and not a true
	 * Binary obj (MySQL does not truly distinguish them). This
	 * test is very gross.
	 */
	if(null_item)
#ifdef USING_R
	  SET_LST_CHR_EL(output,j,i,NA_STRING);
#else
	  NA_CHR_SET(LST_CHR_EL(output,j,i));
#endif
	else {
	  if((size_t) lens[j] != strlen(row[j])){
	    char warn[128];
	    (void) sprintf(warn, 
			   "internal error: row %ld field %ld truncated",
			   (long) i, (long) j);
	    RS_DBI_errorMessage(warn, RS_DBI_WARNING);
	  }
	  SET_LST_CHR_EL(output,j,i,C_S_CPY(row[j]));
	}
	break;
      case NUMERIC_TYPE:
	if(null_item)
	  NA_SET(&(LST_NUM_EL(output,j,i)), NUMERIC_TYPE);
	else
	  LST_NUM_EL(output,j,i) = (double) atof(row[j]);
	break;
#ifndef USING_R
      case SINGLE_TYPE:
	if(null_item)
	  NA_SET(&(LST_FLT_EL(output,j,i)), SINGLE_TYPE);
	else
	  LST_FLT_EL(output,j,i) = (float) atof(row[j]);
	break;
      case RAW_TYPE:           /* these are blob's */
	raw_obj = NEW_RAW((Sint) lens[j]);
	memcpy(RAW_DATA(raw_obj), row[j], lens[j]);
	raw_container = LST_EL(output,j);    /* get list of raw objects*/
	SET_ELEMENT(raw_container, (Sint) i, raw_obj); 
	SET_ELEMENT(output, (Sint) j, raw_container);
  	break;
#endif
      default:  /* error, but we'll try the field as character (!)*/
	if(null_item)
#ifdef USING_R
	  SET_LST_CHR_EL(output,j,i, NA_STRING);
#else
	  NA_CHR_SET(LST_CHR_EL(output,j,i));
#endif
	else {
	    char warn[64];
	    (void) sprintf(warn, 
			   "unrecognized field type %d in column %d",
			   (int) fld_Sclass[j], (int) j);
	    RS_DBI_errorMessage(warn, RS_DBI_WARNING);
	    SET_LST_CHR_EL(output,j,i,C_S_CPY(row[j]));
	  }
	  break;
      } 
    } 
  }
  
  /* actual number of records fetched */
  if(i < num_rec){
    num_rec = i;
    /* adjust the length of each of the members in the output_list */
    for(j = 0; j<num_fields; j++){
      s_tmp = LST_EL(output,j);
      MEM_PROTECT(SET_LENGTH(s_tmp, num_rec));
      SET_ELEMENT(output, j, s_tmp);
      MEM_UNPROTECT(1);
    }
  }
  if(completed < 0)
    RS_DBI_errorMessage("error while fetching rows", RS_DBI_WARNING);

  result->rowCount += num_rec;
  result->completed = (int) completed;

  MEM_UNPROTECT(1);
  return output;
}

/* return a 2-elem list with the last exception number and
 * exception message on a given connection.
 */
s_object *
RS_MySQL_getException(s_object *conHandle)
{
  S_EVALUATOR

  MYSQL *my_connection;
  s_object  *output;
  RS_DBI_connection   *con;
  Sint  n = 2;
  char *exDesc[] = {"errorNum", "errorMsg"};
  Stype exType[] = {INTEGER_TYPE, CHARACTER_TYPE};
  Sint  exLen[]  = {1, 1};

  con = RS_DBI_getConnection(conHandle);
  if(!con->drvConnection)
    RS_DBI_errorMessage("internal error: corrupt connection handle",
			RS_DBI_ERROR);
  output = RS_DBI_createNamedList(exDesc, exType, exLen, n);
#ifndef USING_R
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not allocate named list",
			RS_DBI_ERROR);
#endif
  my_connection = (MYSQL *) con->drvConnection;
  LST_INT_EL(output,0,0) = (Sint) mysql_errno(my_connection);
  SET_LST_CHR_EL(output,1,0,C_S_CPY(mysql_error(my_connection)));

  return output;
}

s_object *
RS_MySQL_closeResultSet(s_object *resHandle)
{
  S_EVALUATOR 

  RS_DBI_resultSet *result;
  MYSQL_RES        *my_result;
  s_object *status;

  result = RS_DBI_getResultSet(resHandle);

  my_result = (MYSQL_RES *) result->drvResultSet;
  if(my_result){
    /* we need to flush any possibly remaining rows (see Manual Ch 20 p358) */
    MYSQL_ROW row;
    while((row = mysql_fetch_row(result->drvResultSet)))
      ;
  }
  mysql_free_result(my_result);

  /* need to NULL drvResultSet, otherwise can't free the rsHandle */
  result->drvResultSet = (void *) NULL;
  RS_DBI_freeResultSet(resHandle);

  MEM_PROTECT(status = NEW_LOGICAL((Sint) 1));
  LGL_EL(status, 0) = TRUE;
  MEM_UNPROTECT(1);

  return status;
}

s_object *
RS_MySQL_managerInfo(Mgr_Handle *mgrHandle)
{
  S_EVALUATOR

  RS_DBI_manager *mgr;
  s_object *output;
  Sint i, num_con, max_con, *cons, ncon;
  Sint j, n = 8;
  char *mgrDesc[] = {"drvName",   "connectionIds", "fetch_default_rec",
                     "managerId", "length",        "num_con", 
                     "counter",   "clientVersion"};
  Stype mgrType[] = {CHARACTER_TYPE, INTEGER_TYPE, INTEGER_TYPE, 
                     INTEGER_TYPE,   INTEGER_TYPE, INTEGER_TYPE, 
                     INTEGER_TYPE,   CHARACTER_TYPE};
  Sint  mgrLen[]  = {1, 1, 1, 1, 1, 1, 1, 1};
  
  mgr = RS_DBI_getManager(mgrHandle);
  if(!mgr)
    RS_DBI_errorMessage("driver not loaded yet", RS_DBI_ERROR);
  num_con = (Sint) mgr->num_con;
  max_con = (Sint) mgr->length;
  mgrLen[1] = num_con;

  output = RS_DBI_createNamedList(mgrDesc, mgrType, mgrLen, n);
#ifndef USING_R
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc named list", 
			RS_DBI_ERROR);
#endif
  j = (Sint) 0;
  if(mgr->drvName)
    SET_LST_CHR_EL(output,j++,0,C_S_CPY(mgr->drvName));
  else 
    SET_LST_CHR_EL(output,j++,0,C_S_CPY(""));

  cons = (Sint *) S_alloc((long)max_con, (int)sizeof(Sint));
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

s_object *
RS_MySQL_connectionInfo(Con_Handle *conHandle)
{
  S_EVALUATOR
  
  MYSQL   *my_con;
  RS_MySQL_conParams *conParams;
  RS_DBI_connection  *con;
  s_object   *output;
  Sint       i, n = 8, *res, nres;
  char *conDesc[] = {"host", "user", "dbname", "conType",
		     "serverVersion", "protocolVersion",
		     "threadId", "rsId"};
  Stype conType[] = {CHARACTER_TYPE, CHARACTER_TYPE, CHARACTER_TYPE,
		      CHARACTER_TYPE, CHARACTER_TYPE, INTEGER_TYPE,
		      INTEGER_TYPE, INTEGER_TYPE};
  Sint  conLen[]  = {1, 1, 1, 1, 1, 1, 1, 1};

  con = RS_DBI_getConnection(conHandle);
  conLen[7] = con->num_res;         /* num of open resultSets */
  my_con = (MYSQL *) con->drvConnection;
  output = RS_DBI_createNamedList(conDesc, conType, conLen, n);
#ifndef USING_R
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc named list",
			RS_DBI_ERROR);
#endif
  conParams = (RS_MySQL_conParams *) con->conParams;
  
  SET_LST_CHR_EL(output,0,0,C_S_CPY(conParams->host));
  SET_LST_CHR_EL(output,1,0,C_S_CPY(conParams->user));
  SET_LST_CHR_EL(output,2,0,C_S_CPY(conParams->dbname));
  SET_LST_CHR_EL(output,3,0,C_S_CPY(mysql_get_host_info(my_con)));
  SET_LST_CHR_EL(output,4,0,C_S_CPY(mysql_get_server_info(my_con)));

  LST_INT_EL(output,5,0) = (Sint) mysql_get_proto_info(my_con);
  LST_INT_EL(output,6,0) = (Sint) mysql_thread_id(my_con);

  res = (Sint *) S_alloc( (long) con->length, (int) sizeof(Sint));
  nres = RS_DBI_listEntries(con->resultSetIds, con->length, res);
  if(nres != con->num_res){
    RS_DBI_errorMessage(
	  "internal error: corrupt RS_DBI resultSet table",
	  RS_DBI_ERROR);
  }
  for( i = 0; i < con->num_res; i++){
    LST_INT_EL(output,7,i) = (Sint) res[i];
  }

  return output;

}
s_object *
RS_MySQL_resultSetInfo(Res_Handle *rsHandle)
{
  S_EVALUATOR

  RS_DBI_resultSet   *result;
  s_object  *output, *flds;
  Sint  n = 6;
  char  *rsDesc[] = {"statement", "isSelect", "rowsAffected",
		     "rowCount", "completed", "fieldDescription"};
  Stype rsType[]  = {CHARACTER_TYPE, INTEGER_TYPE, INTEGER_TYPE,
		     INTEGER_TYPE,   INTEGER_TYPE, LIST_TYPE};
  Sint  rsLen[]   = {1, 1, 1, 1, 1, 1};

  result = RS_DBI_getResultSet(rsHandle);
  if(result->fields)
    flds = RS_DBI_getFieldDescriptions(result->fields);
  else
    flds = S_NULL_ENTRY;

  output = RS_DBI_createNamedList(rsDesc, rsType, rsLen, n);
#ifndef USING_R
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc named list",
			RS_DBI_ERROR);
#endif
  SET_LST_CHR_EL(output,0,0,C_S_CPY(result->statement));
  LST_INT_EL(output,1,0) = result->isSelect;
  LST_INT_EL(output,2,0) = result->rowsAffected;
  LST_INT_EL(output,3,0) = result->rowCount;
  LST_INT_EL(output,4,0) = result->completed;
  if(flds != S_NULL_ENTRY)
     SET_ELEMENT(LST_EL(output, 5), (Sint) 0, flds);

  return output;
}

s_object *
RS_MySQL_typeNames(s_object *type)
{
  s_object *typeNames;
  Sint n, *typeCodes;
  int i;
  
  n = LENGTH(type);
  typeCodes = INTEGER_DATA(type);
  MEM_PROTECT(typeNames = NEW_CHARACTER(n));
  for(i = 0; i < n; i++) {
    SET_CHR_EL(typeNames, i,
          C_S_CPY(RS_DBI_getTypeName(typeCodes[i], RS_MySQL_dataTypes)));
  }
  MEM_UNPROTECT(1);
  return typeNames;
}

/*
 * RS_MySQL_dbApply. 
 *
 * R/S: dbApply(rs, INDEX, FUN, group.begin, group.end, end, ...)
 *
 * This first implementation of R's dbApply()
 * extracts rows from an open result set rs and applies functions
 * to those rows of each group.  This is how it works: it keeps tracks of
 * the values of the field pointed by "group" and it identifies events:
 * BEGIN_GROUP (just read the first row of a different group),
 * NEW_RECORD (every record fetched generates this event),
 * and END_GROUP (just finished with the current group). At these points
 * we invoke the R functions group.end() and group.begin() in the
 * environment() of dbApply
 * [should it be the environment where dbApply was called from (i.e., 
 * dbApply's parent's * frame)?]  
 * Except for the very first group, the order of invocation is 
 * end.group() followed by begin.group()
 *
 * NOTE: We're thinking of groups as commonly defined in awk scripts
 * (but also in SAP's ABAP/4) were rows are assumed to be sorted by
 * the "group" fields and we detect a different (new) group when any of
 * the "group" fields changes.  Our implementation does not require
 * the result set to be sorted by group, but for performance-sake, 
 * it better be.
 *
 * TODO: 1. Notify the reason for exiting (normal, exhausted maxBatches, etc.)
 *       2. Allow INDEX to be a list, as in tapply().
 *       3. Handle NA's (SQL NULL's) in the INDEX and/or data fields.  
 *          Currently they are ignored, thus effectively causing a
 *          new BEGIN_GROUP event.
 *       4. Re-write fetch() in terms of events (END_OF_DATA, 
 *          EXHAUST_DATAFRAME, DB_ERROR, etc.)
 *       5. Create a table of R callback functions indexed by events,
 *          then a handle_event() could conveniently handle all the events.
 */

s_object    *expand_list(s_object *old, Sint new_len);
void         add_group(s_object *group_names, s_object *data, 
		             Stype *fld_Sclass, Sint group, 
			     Sint ngroup, Sint i);
unsigned int check_groupEvents(s_object *data, Stype fld_Sclass[], 
                          Sint row, Sint col);

/* The following are the masks for the events/states we recognize as we
 * bring rows from the result set/cursor
 */
#define NEVER           0
#define BEGIN           1  /* prior to reading 1st row from the resultset */
#define END             2  /* after reading last row from the result set  */
#define BEGIN_GROUP     4  /* just read in 1'st row for a different group */
#define END_GROUP       8  /* just read the last row of the current group */
#define NEW_RECORD     16  /* uninteresting ... */
#define PARTIAL_GROUP  32  /* too much data (>max_rex) partial buffer     */

/* the following are non-grouping events (e.g., db errors, memory) */
#define EXHAUSTED_DF   64  /* exhausted the allocated data.frame  */
#define EXHAUSTED_OUT 128  /* exhausted the allocated output list */
#define END_OF_DATA   256  /* end of data from the result set     */
#define DBMS_ERROR    512  /* error in remote dbms                */

/* beginGroupFun takes only one arg: the name of the current group */
s_object *
RS_DBI_invokeBeginGroup(s_object *callObj,      /* should be initialized */
                        const char *group_name, /* one string */
                        s_object *rho)
{
   S_EVALUATOR

   s_object *s_group_name, *val;

   /* make a copy of the argument */
   MEM_PROTECT(s_group_name = NEW_CHARACTER((Sint) 1));
   SET_CHR_EL(s_group_name, 0, C_S_CPY(group_name));

   /* and stick into call object */
   SETCADR(callObj, s_group_name);
   val = EVAL_IN_FRAME(callObj, rho);
   MEM_UNPROTECT(1);

   return S_NULL_ENTRY;
}

s_object *
RS_DBI_invokeNewRecord(s_object *callObj,   /* should be initialized already */
                       s_object *new_record,/* a 1-row data.frame */
                       s_object *rho)
{
   S_EVALUATOR

   s_object *df, *val;

   /* make a copy of the argument */
   MEM_PROTECT(df = COPY_ALL(new_record));

   /* and stick it into the call object */
   SETCADR(callObj, df);
   val = EVAL_IN_FRAME(callObj, rho);
   MEM_UNPROTECT(1);

   return S_NULL_ENTRY;
}

/* endGroupFun takes two args: a data.frame and the group name */
s_object *
RS_DBI_invokeEndGroup(s_object *callObj, s_object *data,
                      const char *group_name, s_object *rho)
{
   S_EVALUATOR

   s_object *s_x, *s_group_name, *val;

   /* make copies of the arguments */
   MEM_PROTECT(callObj = duplicate(callObj));
   MEM_PROTECT(s_x = COPY_ALL(data));
   MEM_PROTECT(s_group_name = NEW_CHARACTER((Sint) 1));
   SET_CHR_EL(s_group_name, 0, C_S_CPY(group_name));

   /* stick copies of args into the call object */
   SETCADR(callObj, s_x);
   SETCADDR(callObj, s_group_name);
   SETCADDDR(callObj, R_DotsSymbol);

   val = EVAL_IN_FRAME(callObj, rho);

   MEM_UNPROTECT(3);
   return val;
}

s_object *                               /* output is a named list */
RS_MySQL_dbApply(s_object *rsHandle,     /* resultset handle */
                 s_object *s_group_field,/* this is a 0-based field number */
                 s_object *s_funs,       /* a 5-elem list with handler funs */
                 s_object *rho,          /* the env where to run funs */
                 s_object *s_batch_size, /* alloc these many rows */
                 s_object *s_max_rec)    /* max rows per group */
{
   S_EVALUATOR

   RS_DBI_resultSet *result;
   RS_DBI_fields    *flds;

   MYSQL_RES *my_result;
   MYSQL_ROW  row;

   s_object  *data, *cur_rec, *out_list, *group_names, *val;
#ifndef USING_R
   s_object  *raw_obj, *raw_container;
#endif
   unsigned long  *lens = (unsigned long *)0;
   Stype  *fld_Sclass;
   Sint   i, j, null_item, expand, *fld_nullOk, completed;
   Sint   num_rec, num_groups;
   int    num_fields;
   Sint   max_rec = INT_EL(s_max_rec,0);     /* max rec per group */
   Sint   ngroup = 0, group_field = INT_EL(s_group_field,0);
   long   total_records;
   Sint   pushed_back = FALSE;

   unsigned int event = NEVER; 
   int    np = 0;        /* keeps track of MEM_PROTECT()'s */
   s_object    *beginGroupCall, *beginGroupFun = LST_EL(s_funs, 2);
   s_object    *endGroupCall,   *endGroupFun   = LST_EL(s_funs, 3);
   s_object    *newRecordCall,   *newRecordFun  = LST_EL(s_funs, 4);
   int        invoke_beginGroup = (GET_LENGTH(beginGroupFun)>0);
   int        invoke_endGroup   = (GET_LENGTH(endGroupFun)>0);
   int        invoke_newRecord  = (GET_LENGTH(newRecordFun)>0);

   row = NULL;
   beginGroupCall = R_NilValue;    /* -Wall */
   if(invoke_beginGroup){
      MEM_PROTECT(beginGroupCall=lang2(beginGroupFun, R_NilValue));
      ++np;
   }
   endGroupCall = R_NilValue;    /* -Wall */
   if(invoke_endGroup){
      /* TODO: append list(...) to the call object */
      MEM_PROTECT(endGroupCall = lang4(endGroupFun, R_NilValue, 
	          R_NilValue, R_NilValue));
      ++np;
   }
   newRecordCall = R_NilValue;    /* -Wall */
   if(invoke_newRecord){
      MEM_PROTECT(newRecordCall = lang2(newRecordFun, R_NilValue));
      ++np;
   }

   result = RS_DBI_getResultSet(rsHandle);
   flds = result->fields;
   if(!flds)
     RS_DBI_errorMessage("corrupt resultSet, missing fieldDescription",
		       RS_DBI_ERROR);
   num_fields = flds->num_fields;
   fld_Sclass = flds->Sclass;
   fld_nullOk = flds->nullOk;
   MEM_PROTECT(data = NEW_LIST((Sint) num_fields));     /* buffer records */
   MEM_PROTECT(cur_rec = NEW_LIST((Sint) num_fields));  /* current record */
   np += 2;
   RS_DBI_allocOutput(cur_rec, flds, (Sint) 1, 1);
   RS_DBI_makeDataFrame(cur_rec);

   num_rec = INT_EL(s_batch_size, 0);     /* this is num of rec per group! */
   max_rec = INT_EL(s_max_rec,0);         /* max rec **per group**         */
   num_groups = num_rec;
   MEM_PROTECT(out_list = NEW_LIST(num_groups));
   MEM_PROTECT(group_names = NEW_CHARACTER(num_groups));
   np += 2;

   /* set conversion for group names */

   if(result->rowCount==0){
      event = BEGIN;
      /* here we could invoke the begin function*/
   }

   /* actual fetching.... */

   my_result = (MYSQL_RES *) result->drvResultSet;
   completed = (Sint) 0;

   total_records = 0;
   expand = 0;                  /* expand or init each data vector? */
   i = 0;                       /* index into row number **within** groups */
   while(1){

     if(i==0 || i==num_rec){         /* BEGIN, EXTEND_DATA, BEGIN_GROUP */

        /* reset num_rec upon a new group, double it if needs to expand */
        num_rec = (i==0) ? INT_EL(s_batch_size, 0) : 2*num_rec;
        if(i<max_rec)
	   RS_DBI_allocOutput(data, flds, num_rec, expand++);
        else
	   break;       /* ok, no more fetching for now (pending group?) */
     }

     if(!pushed_back)
        row = mysql_fetch_row(my_result);

     if(row==NULL){    /* either we finish or we encounter an error */
        unsigned int  err_no;     /* TODO: move block to check_groupEvents */
        RS_DBI_connection   *con;
        con = RS_DBI_getConnection(rsHandle);
        err_no = mysql_errno((MYSQL *) con->drvConnection);
        completed = (Sint) (err_no ? -1 : 1);
        break;
     }

     if(!pushed_back){                      /* recompute fields lengths? */
       lens = mysql_fetch_lengths(my_result);  /* lengths for each field */
       ++total_records;
     }
     else
        pushed_back = FALSE;

     /* coerce each entry row[j] to an R/S type according to its Sclass.
      * TODO:  converter functions are badly needed.
      */
     for(j = 0; j < num_fields; j++){

        null_item = (row[j] == NULL);
        switch((int)fld_Sclass[j]){

	   case INTEGER_TYPE: 
	      if(null_item)
		 NA_SET(&(LST_INT_EL(data,j,i)), INTEGER_TYPE);
	      else
	      LST_INT_EL(data,j,i) = atol(row[j]);
	      LST_INT_EL(cur_rec,j,0) = LST_INT_EL(data,j,i);
	      break;

	   case CHARACTER_TYPE: 
	      /* BUG: I need to verify that a TEXT field (which is stored as
	       * a BLOB by MySQL!) is indeed char and not a true
	       * Binary obj (MySQL does not truly distinguish them). This
	       * test is very gross.
	       */
	      if(null_item)
#ifdef USING_R
	          SET_LST_CHR_EL(data,j,i,NA_STRING);
#else
	          NA_CHR_SET(LST_CHR_EL(data,j,i));
#endif
	      else {
	         if((size_t) lens[j] != strlen(row[j])){
	            char warn[128];
	            (void) sprintf(warn, 
			   "internal error: row %ld field %ld truncated",
			   (long) i, (long) j);
	            RS_DBI_errorMessage(warn, RS_DBI_WARNING);
	         }
	         SET_LST_CHR_EL(data,j,i,C_S_CPY(row[j]));
	      }
	      SET_LST_CHR_EL(cur_rec, j, 0, C_S_CPY(LST_CHR_EL(data,j,i)));
	      break;

           case NUMERIC_TYPE:
	      if(null_item)
	         NA_SET(&(LST_NUM_EL(data,j,i)), NUMERIC_TYPE);
	      else
	         LST_NUM_EL(data,j,i) = (double) atof(row[j]);
	      LST_NUM_EL(cur_rec,j,0) = LST_NUM_EL(data,j,i);
	      break;

#ifndef USING_R
           case SINGLE_TYPE:
	      if(null_item)
	         NA_SET(&(LST_FLT_EL(data,j,i)), SINGLE_TYPE);
	      else
	         LST_FLT_EL(data,j,i) = (float) atof(row[j]);
	      LST_FLT_EL(cur_rec,j,0) = LST_FLT_EL(data,j,i);
	      break;

           case RAW_TYPE:           /* these are blob's */
	      raw_obj = NEW_RAW((Sint) lens[j]);
	      memcpy(RAW_DATA(raw_obj), row[j], lens[j]);
	      raw_container = LST_EL(data,j);    /* get list of raw objects*/
	      SET_ELEMENT(raw_container, (Sint) i, raw_obj); 
	      SET_ELEMENT(data, (Sint) j, raw_container);
    	      break;
#endif

           default:  /* error, but we'll try the field as character (!)*/
	      if(null_item)
#ifdef USING_R
		 SET_LST_CHR_EL(data,j,i, NA_STRING);
#else
	         NA_CHR_SET(LST_CHR_EL(data,j,i));
#endif
	      else {
	         char warn[64];
	         (void) sprintf(warn, 
			   "unrecognized field type %d in column %d",
			   (int) fld_Sclass[j], (int) j);
	         RS_DBI_errorMessage(warn, RS_DBI_WARNING);
	         SET_LST_CHR_EL(data,j,i,C_S_CPY(row[j]));
	      }
	      SET_LST_CHR_EL(cur_rec,j,0, C_S_CPY(LST_CHR_EL(data,j,i)));
	      break;
        } 
     }

     /* We just finished processing the new record, now we check 
      * for some events (in addition to NEW_RECORD, of course).
      */
     event = check_groupEvents(data, fld_Sclass, i, group_field);

     if(BEGIN_GROUP & event){
        if(ngroup==num_groups){                  /* exhausted output list? */
           num_groups = 2 * num_groups;
           MEM_PROTECT(SET_LENGTH(out_list, num_groups)); 
           MEM_PROTECT(SET_LENGTH(group_names, num_groups)); 
	   np += 2;
        }
	if(invoke_beginGroup)
           RS_DBI_invokeBeginGroup(
		beginGroupCall, CHR_EL(group_names, ngroup), rho);
     }

     if(invoke_newRecord){
	RS_DBI_invokeNewRecord(newRecordCall, cur_rec, rho);
     }

     if(END_GROUP & event){                     
     
	add_group(group_names, data, fld_Sclass, group_field, ngroup, i-1);

	RS_DBI_allocOutput(data, flds, i, expand++);
        RS_DBI_makeDataFrame(data); 
     
	val = RS_DBI_invokeEndGroup(endGroupCall, data,
		                    CHR_EL(group_names, ngroup), rho);
        SET_ELEMENT(out_list, ngroup, val);

        /* set length of data to zero to force initialization 
	 * for next group 
	 */
	RS_DBI_allocOutput(data, flds, (Sint) 0, (Sint) 1);
        i = 0;                                  /* flush */
	++ngroup;   
        pushed_back = TRUE;
        continue;
     }

     i++;
   }    
  
   /* we fetched all the rows we needed/could; compute actual number of 
    * records fetched.
    * TODO: What should we return in the case of partial groups???
    */
   if(completed < 0)
       RS_DBI_errorMessage("error while fetching rows", RS_DBI_WARNING);
   else if(completed)
       event = (END_GROUP|END);
   else
       event = PARTIAL_GROUP;

   /* wrap up last group */
   if((END_GROUP & event) || (PARTIAL_GROUP & event)){ 

      add_group(group_names, data, fld_Sclass, group_field, ngroup, i-i);

      if(i<num_rec){
	 RS_DBI_allocOutput(data, flds, i, expand++);
	 RS_DBI_makeDataFrame(data); 
      }
      if(invoke_endGroup){
	 val = RS_DBI_invokeEndGroup(endGroupCall, data, 
	                             CHR_EL(group_names, ngroup), rho);
         SET_ELEMENT(out_list, ngroup++, val);
      }
      if(PARTIAL_GROUP & event){
         char buf[512];
	 (void) strcpy(buf, "exhausted the pre-allocated storage. The last ");
	 (void) strcat(buf, "output group was computed with partial data. ");
	 (void) strcat(buf, "The remaining data were left un-read in the ");
	 (void) strcat(buf, "result set.");
	 RS_DBI_errorMessage(buf, RS_DBI_WARNING);
      }
   }

   /* set the correct length of output list */
   if(GET_LENGTH(out_list) != ngroup){
      MEM_PROTECT(SET_LENGTH(out_list, ngroup));
      MEM_PROTECT(SET_LENGTH(group_names, ngroup));
      np += 2;
   }

   result->rowCount += total_records;  
   result->completed = (int) completed;
 
   SET_NAMES(out_list, group_names);       /* do I need to PROTECT? */
#ifndef USING_R
   out_list = AS_LIST(out_list);           /* for S4/Splus[56]'s sake */
#endif

   MEM_UNPROTECT(np);
   return out_list;
}

unsigned int
check_groupEvents(s_object *data, Stype fld_Sclass[], Sint irow, Sint jcol)
{
   if(irow==0) /* Begin */
       return (BEGIN|BEGIN_GROUP);

   switch(fld_Sclass[jcol]){

      case LOGICAL_TYPE:
	 if(LST_LGL_EL(data,jcol,irow)!=LST_LGL_EL(data,jcol,irow-1))
	    return (END_GROUP|BEGIN_GROUP);
	 break;

      case INTEGER_TYPE:
	 if(LST_INT_EL(data,jcol,irow)!=LST_INT_EL(data,jcol,irow-1))
	    return (END_GROUP|BEGIN_GROUP);
	 break;

      case NUMERIC_TYPE:
	 if(LST_NUM_EL(data,jcol,irow)!=LST_NUM_EL(data,jcol,irow-1))
	    return (END_GROUP|BEGIN_GROUP);
	 break;
#ifndef USING_R

      case SINGLE_TYPE:
	 if(LST_FLT_EL(data,jcol,irow)!=LST_FLT_EL(data,jcol,irow-1))
	    return (END_GROUP|BEGIN_GROUP);
	 break;
#endif

      case CHARACTER_TYPE:
	 if(strcmp(LST_CHR_EL(data,jcol,irow), LST_CHR_EL(data,jcol,irow-1)))
	    return (END_GROUP|BEGIN_GROUP);
	 break;

      default:
	 PROBLEM
	   "un-regongnized R/S data type %d", fld_Sclass[jcol]
	 ERROR;
         break;
   }      

   return NEW_RECORD;
}

/* append current group (as character) to the vector of group names */
void
add_group(s_object *group_names, s_object *data, 
		Stype *fld_Sclass, Sint group_field, Sint ngroup, Sint i)
{
   char  buff[1024];   

   switch((int) fld_Sclass[group_field]){

      case LOGICAL_TYPE:
	 (void) sprintf(buff, "%ld", (long) LST_LGL_EL(data,group_field,i));
	 break;
      case INTEGER_TYPE:
	 (void) sprintf(buff, "%ld", (long) LST_INT_EL(data,group_field,i));
	 break;
#ifndef USING_R
      case SINGLE_TYPE:
	 (void) sprintf(buff, "%f", (double) LST_FLT_EL(data,group_field,i));
	 break;
#endif
      case NUMERIC_TYPE:
	 (void) sprintf(buff, "%f", (double) LST_NUM_EL(data,group_field,i));
	 break;
      case CHARACTER_TYPE:
	 strcpy(buff, LST_CHR_EL(data,group_field,i));
	 break;
      default:
	 RS_DBI_errorMessage("unrecognized R/S type for group", RS_DBI_ERROR);
	 break;
   }
   SET_CHR_EL(group_names, ngroup, C_S_CPY(buff));
   return;
}

/* the following function was kindly provided by Mikhail Kondrin 
 * it returns the last inserted index. 
 * TODO: It returns an int, but it can potentially be inadequate
 *       if the index is anunsigned integer.  Should we return
 *       a numeric instead?
 */
s_object *
RS_MySQL_insertid(Con_Handle *conHandle)
{
  S_EVALUATOR
 
  MYSQL   *my_con;
  RS_DBI_connection  *con;
  s_object   *output;
  char *conDesc[] = {"iid"};
  Stype conType[] = {INTEGER_TYPE};    /* dj: are we sure an int will do? */
  Sint  conLen[]  = {1};

  con = RS_DBI_getConnection(conHandle);
  my_con = (MYSQL *) con->drvConnection;
  output = RS_DBI_createNamedList(conDesc, conType, conLen, 1);
#ifndef USING_R
  if(IS_LIST(output))
    output = AS_LIST(output);
  else
    RS_DBI_errorMessage("internal error: could not alloc named list",
                        RS_DBI_ERROR);
#endif
  LST_INT_EL(output,0,0) = (Sint) mysql_insert_id(my_con);

  return output;

}

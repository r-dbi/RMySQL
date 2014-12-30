/*
 * $Id: RS-MySQL.c 360 2009-04-07 16:41:24Z j.horner $
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
#include "R_ext/Rdynload.h"

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
 *  On R, "The .Call and .External Interfaces"  in the "Writing R Extensions"
 *  manual.
 *  On MySQL, The "MySQL Reference Manual" available at http://www.mysql.com
 *     Also, "MySQL" Third Ed. by Paul Dubois (2005) New Riders Publishing.
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
    int  fetch_default_rec, force_reload, max_con;
    const char *drvName = "MySQL";

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
        RS_DBI_errorMessage(
            "there are opened connections -- close them first",
            RS_DBI_ERROR);

    RS_DBI_freeManager(mgrHandle);

    PROTECT(status = NEW_LOGICAL((int) 1));
    LGL_EL(status,0) = TRUE;
    UNPROTECT(1);
    return status;
}

/* Are there more results on this connection (as in multi results or
 * SQL scripts
 */

s_object * /* boolean */
RS_MySQL_moreResultSets(Con_Handle *conHandle)
{
    S_EVALUATOR

    RS_DBI_connection *con;
    MYSQL             *my_connection;
    my_bool           tmp;
    s_object          *status;            /* boolean */

    con = RS_DBI_getConnection(conHandle);
    my_connection = (MYSQL *) con->drvConnection;

    tmp = mysql_more_results(my_connection);
    PROTECT(status = NEW_LOGICAL((int) 1));
    if(tmp)
       LGL_EL(status, 0) = TRUE;
    else
       LGL_EL(status, 0) = FALSE;

    UNPROTECT(1);

    return status;
}

Res_Handle *
RS_MySQL_nextResultSet(Con_Handle *conHandle)
{
    S_EVALUATOR

    RS_DBI_connection *con;
    RS_DBI_resultSet  *result;
    Res_Handle        *rsHandle;
    MYSQL_RES         *my_result;
    MYSQL             *my_connection;
    int              rc, num_fields, is_select;

    con = RS_DBI_getConnection(conHandle);
    my_connection = (MYSQL *) con->drvConnection;

    rc = (int) mysql_next_result(my_connection);

    if(rc<0){
        RS_DBI_errorMessage("no more result sets", RS_DBI_ERROR);
    }
    else if(rc>0){
        RS_DBI_errorMessage("error in getting next result set", RS_DBI_ERROR);
    }

    /* the following comes verbatim from RS_MySQL_exec() */
    my_result = mysql_use_result(my_connection);
    if(!my_result)
        my_result = (MYSQL_RES *) NULL;

    num_fields = (int) mysql_field_count(my_connection);
    is_select = (int) TRUE;
    if(!my_result){
        if(num_fields>0){
            RS_DBI_errorMessage("error in getting next result set", RS_DBI_ERROR);
        }
        else
            is_select = FALSE;
    }

    /* we now create the wrapper and copy values */
    rsHandle = RS_DBI_allocResultSet(conHandle);
    result = RS_DBI_getResultSet(rsHandle);
    result->statement = RS_DBI_copyString("<UNKNOWN>");
    result->drvResultSet = (void *) my_result;
    result->rowCount = (int) 0;
    result->isSelect = is_select;
    if(!is_select){
        result->rowsAffected = (int) mysql_affected_rows(my_connection);
        result->completed = 1;
    }
    else {
        result->rowsAffected = (int) -1;
        result->completed = 0;
    }

    if(is_select)
      result->fields = RS_MySQL_createDataMappings(rsHandle);

    return rsHandle;
}

/* open a connection with the same parameters used for in conHandle */
Con_Handle *
RS_MySQL_cloneConnection(Con_Handle *conHandle)
{
    S_EVALUATOR

    return RS_MySQL_createConnection(
			RS_DBI_asMgrHandle(MGR_ID(conHandle)),
			RS_MySQL_cloneConParams(RS_DBI_getConnection(conHandle)->conParams));
}

RS_MySQL_conParams *
RS_MySQL_allocConParams(void)
{
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

RS_MySQL_conParams *
RS_MySQL_cloneConParams(RS_MySQL_conParams *cp)
{
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

void
RS_MySQL_freeConParams(RS_MySQL_conParams *conParams)
{
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

Con_Handle *
RS_MySQL_newConnection(Mgr_Handle *mgrHandle,
    s_object *s_dbname,
    s_object *s_username,
    s_object *s_password,
    s_object *s_myhost,
    s_object *s_unix_socket,
    s_object *s_port,
    s_object *s_client_flag,
    s_object *s_groups,
    s_object *s_default_file)
{
    S_EVALUATOR

	RS_MySQL_conParams *conParams;

    if(!is_validHandle(mgrHandle, MGR_HANDLE_TYPE))
        RS_DBI_errorMessage("invalid MySQLManager", RS_DBI_ERROR);

    /* Create connection parameters structure and initialize */
    conParams = RS_MySQL_allocConParams();

	/* Arguments override defaults in config file */
    if(s_dbname != R_NilValue && IS_CHARACTER(s_dbname))
        conParams->dbname = RS_DBI_copyString((char *) CHR_EL(s_dbname,0));
    if(s_username != R_NilValue && IS_CHARACTER(s_username))
        conParams->username = RS_DBI_copyString((char *) CHR_EL(s_username,0));
    if(s_password != R_NilValue && IS_CHARACTER(s_password))
        conParams->password = RS_DBI_copyString((char *) CHR_EL(s_password,0));
    if(s_myhost != R_NilValue && IS_CHARACTER(s_myhost))
        conParams->host = RS_DBI_copyString((char *) CHR_EL(s_myhost,0));
    if(s_unix_socket != R_NilValue && IS_CHARACTER(s_unix_socket))
        conParams->unix_socket = RS_DBI_copyString((char *) CHR_EL(s_unix_socket,0));
	if (s_port != R_NilValue && IS_INTEGER(s_port) && INT_EL(s_port,0) > 0)
		conParams->port = (unsigned int) INT_EL(s_port,0);
	if (s_client_flag != R_NilValue && IS_INTEGER(s_client_flag))
		conParams->client_flag = (unsigned int) INT_EL(s_client_flag,0);
    if(s_groups != R_NilValue && IS_CHARACTER(s_groups))
        conParams->groups = RS_DBI_copyString((char *) CHR_EL(s_groups,0));
    if(s_default_file != R_NilValue && IS_CHARACTER(s_default_file))
        conParams->default_file = RS_DBI_copyString((char *) CHR_EL(s_default_file,0));

    return RS_MySQL_createConnection(mgrHandle,conParams);
}

/* RS_MySQL_createConnection - internal function
 *
 * Used by both RS_MySQL_newConnection and RS_MySQL_cloneConnection.
 * It is responsible for the memory associated with conParams.
 */
Con_Handle *
RS_MySQL_createConnection(Mgr_Handle *mgrHandle, RS_MySQL_conParams *conParams)
{
    S_EVALUATOR

    RS_DBI_connection  *con;
    Con_Handle  *conHandle;
    MYSQL     *my_connection;

    if(!is_validHandle(mgrHandle, MGR_HANDLE_TYPE))
        RS_DBI_errorMessage("invalid MySQLManager", RS_DBI_ERROR);

	/* Initialize MySQL connection */
    my_connection = mysql_init(NULL);

#if defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID > 32348
    /* (BDR's fix)
     * Starting w.  MySQL 3.23.39, LOAD DATA INFILE may be disabled (although
     * the default is enabled);  since assignTable() depends on it,
     * we unconditionally enable it.
     */
    mysql_options(my_connection, MYSQL_OPT_LOCAL_INFILE, 0);
#endif

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

    PROTECT(status = NEW_LOGICAL((int) 1));
    LGL_EL(status, 0) = TRUE;
    UNPROTECT(1);

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
    int     res_id, is_select;
    char     *dyn_statement;

    con = RS_DBI_getConnection(conHandle);
    my_connection = (MYSQL *) con->drvConnection;
    dyn_statement = RS_DBI_copyString(CHR_EL(statement,0));

    /* Do we have a pending resultSet in the current connection?
    * MySQL only allows  one resultSet per connection.
    */
    if(con->num_res>0){
        res_id = (int) con->resultSetIds[0]; /* recall, MySQL has only 1 res */
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
    is_select = (int) TRUE;
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
    result->rowCount = (int) 0;
    result->isSelect = is_select;
    if(!is_select){
        result->rowsAffected = (int) mysql_affected_rows(my_connection);
        result->completed = 1;
    }
    else {
        result->rowsAffected = (int) -1;
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
    char    errMsg[2048];

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
            flds->Sclass[j] = STRSXP;
            flds->isVarLength[j] = (int) 1;
            break;
        case FIELD_TYPE_TINY:            /* 1-byte TINYINT   */
        case FIELD_TYPE_SHORT:           /* 2-byte SMALLINT  */
        case FIELD_TYPE_INT24:           /* 3-byte MEDIUMINT */
        case FIELD_TYPE_LONG:            /* 4-byte INTEGER   */
            /* if unsigned, turn into numeric (may be too large for ints/long)*/
            if(select_dp[j].flags & UNSIGNED_FLAG)
                flds->Sclass[j] = REALSXP;
            else
                flds->Sclass[j] = INTSXP;
            break;
#if defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 50003 /* 5.0.3 */
        case FIELD_TYPE_BIT:
            if(flds->precision[j] <= sizeof(int))   /* can R int hold the bytes? */
                flds->Sclass[j] = INTSXP;
            else {
                flds->Sclass[j] = STRSXP;
                (void) sprintf(errMsg,
                    "BIT field in column %d too long (%d bits) for an R integer (imported as character)",
                    j+1, flds->precision[j]);
            }
#endif
        case FIELD_TYPE_LONGLONG:        /* TODO: can we fit these in R/S ints? */
            if(sizeof(int)>=8)            /* Arg! this ain't pretty:-( */
                flds->Sclass[j] = INTSXP;
            else
                flds->Sclass[j] = REALSXP;
        case FIELD_TYPE_DECIMAL:
#if defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 50003 /* 5.0.3 */
        case FIELD_TYPE_NEWDECIMAL:
#endif
            if(flds->scale[j] > 0 || flds->precision[j] > 10)
                flds->Sclass[j] = REALSXP;
            else
                flds->Sclass[j] = INTSXP;
            break;
        case FIELD_TYPE_FLOAT:
        case FIELD_TYPE_DOUBLE:
            flds->Sclass[j] = REALSXP;
            break;
        case FIELD_TYPE_BLOB:         /* TODO: how should we bring large ones*/
        case FIELD_TYPE_TINY_BLOB:
        case FIELD_TYPE_MEDIUM_BLOB:
        case FIELD_TYPE_LONG_BLOB:
            flds->Sclass[j] = STRSXP;   /* Grr! Hate this! */
            flds->isVarLength[j] = (int) 1;
            break;
        case FIELD_TYPE_DATE:
        case FIELD_TYPE_TIME:
        case FIELD_TYPE_DATETIME:
        case FIELD_TYPE_YEAR:
        case FIELD_TYPE_NEWDATE:
            flds->Sclass[j] = STRSXP;
            flds->isVarLength[j] = (int) 1;
            break;
        case FIELD_TYPE_ENUM:
            flds->Sclass[j] = STRSXP;   /* see the MySQL ref. manual */
            flds->isVarLength[j] = (int) 1;
            break;
        case FIELD_TYPE_SET:
            flds->Sclass[j] = STRSXP;
            flds->isVarLength[j] = (int) 0;
            break;
        default:
            flds->Sclass[j] = STRSXP;
            flds->isVarLength[j] = (int) 1;
            (void) sprintf(errMsg,
                "unrecognized MySQL field type %d in column %d imported as character",
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

    unsigned long  *lens;
    int    i, j, null_item, expand;
    int   *fld_nullOk, completed;
    SEXPTYPE  *fld_Sclass;
    int   num_rec;
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
    PROTECT(output = NEW_LIST((int) num_fields));
    RS_DBI_allocOutput(output, flds, num_rec, 0);
    fld_Sclass = flds->Sclass;
    fld_nullOk = flds->nullOk;

    /* actual fetching....*/
    my_result = (MYSQL_RES *) result->drvResultSet;
    completed = (int) 0;

    for(i = 0; ; i++){

        if(i==num_rec){  /* exhausted the allocated space */

            if(expand){    /* do we extend or return the records fetched so far*/
                num_rec = 2 * num_rec;
                RS_DBI_allocOutput(output, flds, num_rec, expand);
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
            completed = (int) (err_no ? -1 : 1);
            break;
        }
        lens = mysql_fetch_lengths(my_result);

        for(j = 0; j < num_fields; j++){

            null_item = (row[j] == NULL);

            switch((int)fld_Sclass[j]){

            case INTSXP:
                if(null_item)
                    NA_SET(&(LST_INT_EL(output,j,i)), INTSXP);
                else
                    LST_INT_EL(output,j,i) = (int) atol(row[j]);
                break;

            case STRSXP:
                /* BUG: I need to verify that a TEXT field (which is stored as
                * a BLOB by MySQL!) is indeed char and not a true
                * Binary obj (MySQL does not truly distinguish them). This
                * test is very gross.
                */
                if(null_item)
                    SET_LST_CHR_EL(output,j,i,NA_STRING);
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

            case REALSXP:
                if(null_item)
                    NA_SET(&(LST_NUM_EL(output,j,i)), REALSXP);
                else
                    LST_NUM_EL(output,j,i) = (double) atof(row[j]);
                break;

            default:  /* error, but we'll try the field as character (!)*/
                if(null_item)
                    SET_LST_CHR_EL(output,j,i, NA_STRING);
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
        PROTECT(SET_LENGTH(s_tmp, num_rec));
        SET_ELEMENT(output, j, s_tmp);
        UNPROTECT(1);
        }
    }
    if(completed < 0)
        RS_DBI_errorMessage("error while fetching rows", RS_DBI_WARNING);

    result->rowCount += num_rec;
    result->completed = (int) completed;

    UNPROTECT(1);
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
    int  n = 2;
    char *exDesc[] = {"errorNum", "errorMsg"};
    SEXPTYPE exType[] = {INTSXP, STRSXP};
    int  exLen[]  = {1, 1};

    con = RS_DBI_getConnection(conHandle);
    if(!con->drvConnection)
        RS_DBI_errorMessage("internal error: corrupt connection handle",
            RS_DBI_ERROR);
    output = RS_DBI_createNamedList(exDesc, exType, exLen, n);

    my_connection = (MYSQL *) con->drvConnection;
    LST_INT_EL(output,0,0) = (int) mysql_errno(my_connection);
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

    PROTECT(status = NEW_LOGICAL((int) 1));
    LGL_EL(status, 0) = TRUE;
    UNPROTECT(1);

    return status;
}

s_object *
RS_MySQL_managerInfo(Mgr_Handle *mgrHandle)
{
    S_EVALUATOR

    RS_DBI_manager *mgr;
    s_object *output;
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

s_object *
RS_MySQL_connectionInfo(Con_Handle *conHandle)
{
    S_EVALUATOR

    MYSQL   *my_con;
    RS_MySQL_conParams *conParams;
    RS_DBI_connection  *con;
    s_object   *output;
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
    SET_LST_CHR_EL(output,0,0,C_S_CPY(tmp));
	tmp = conParams->username? conParams->username : (my_con->user?my_con->user:"");
    SET_LST_CHR_EL(output,1,0,C_S_CPY(tmp));
	tmp = conParams->dbname? conParams->dbname : (my_con->db?my_con->db:"");
    SET_LST_CHR_EL(output,2,0,C_S_CPY(tmp));
    SET_LST_CHR_EL(output,3,0,C_S_CPY(mysql_get_host_info(my_con)));
    SET_LST_CHR_EL(output,4,0,C_S_CPY(mysql_get_server_info(my_con)));

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

s_object *
RS_MySQL_resultSetInfo(Res_Handle *rsHandle)
{
    S_EVALUATOR

    RS_DBI_resultSet   *result;
    s_object  *output, *flds;
    int  n = 6;
    char  *rsDesc[] = {"statement", "isSelect", "rowsAffected",
                        "rowCount", "completed", "fieldDescription"};
    SEXPTYPE rsType[]  = {STRSXP, INTSXP, INTSXP,
      INTSXP,   INTSXP, VECSXP};
    int  rsLen[]   = {1, 1, 1, 1, 1, 1};

    result = RS_DBI_getResultSet(rsHandle);
    if(result->fields)
        flds = RS_DBI_getFieldDescriptions(result->fields);
    else
        flds = R_NilValue;

    output = RS_DBI_createNamedList(rsDesc, rsType, rsLen, n);

    SET_LST_CHR_EL(output,0,0,C_S_CPY(result->statement));
    LST_INT_EL(output,1,0) = result->isSelect;
    LST_INT_EL(output,2,0) = result->rowsAffected;
    LST_INT_EL(output,3,0) = result->rowCount;
    LST_INT_EL(output,4,0) = result->completed;
    if(flds != R_NilValue)
        SET_ELEMENT(LST_EL(output, 5), (int) 0, flds);

    return output;
}

s_object *
RS_MySQL_typeNames(s_object *type)
{
    s_object *typeNames;
    int n, *typeCodes;
    int i;
	char *tname;

    n = LENGTH(type);
    typeCodes = INTEGER(type);
    PROTECT(typeNames = NEW_CHARACTER(n));
    for(i = 0; i < n; i++) {
		tname = RS_DBI_getTypeName(typeCodes[i], RS_MySQL_dataTypes);
		if (!tname) tname = "";
		SET_CHR_EL(typeNames, i, C_S_CPY(tname));
    }
    UNPROTECT(1);
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

s_object    *expand_list(s_object *old, int new_len);
void         add_group(s_object *group_names, s_object *data,
             SEXPTYPE *fld_Sclass, int group,
           int ngroup, int i);
unsigned int check_groupEvents(s_object *data, SEXPTYPE fld_Sclass[],
                          int row, int col);

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
    PROTECT(s_group_name = NEW_CHARACTER((int) 1));
    SET_CHR_EL(s_group_name, 0, C_S_CPY(group_name));

    /* and stick into call object */
    SETCADR(callObj, s_group_name);
    val = eval(callObj, rho);
    UNPROTECT(1);

    return R_NilValue;
}

s_object *
RS_DBI_invokeNewRecord(s_object *callObj,   /* should be initialized already */
                       s_object *new_record,/* a 1-row data.frame */
                       s_object *rho)
{
    S_EVALUATOR

    s_object *df, *val;

    /* make a copy of the argument */
    PROTECT(df = duplicate(new_record));

    /* and stick it into the call object */
    SETCADR(callObj, df);
    val = eval(callObj, rho);
    UNPROTECT(1);

    return R_NilValue;
}

/* endGroupFun takes two args: a data.frame and the group name */
s_object *
RS_DBI_invokeEndGroup(s_object *callObj, s_object *data,
                      const char *group_name, s_object *rho)
{
    S_EVALUATOR

    s_object *s_x, *s_group_name, *val;

    /* make copies of the arguments */
    PROTECT(callObj = duplicate(callObj));
    PROTECT(s_x = duplicate(data));
    PROTECT(s_group_name = NEW_CHARACTER((int) 1));
    SET_CHR_EL(s_group_name, 0, C_S_CPY(group_name));

    /* stick copies of args into the call object */
    SETCADR(callObj, s_x);
    SETCADDR(callObj, s_group_name);
    SETCADDDR(callObj, R_DotsSymbol);

    val = eval(callObj, rho);

    UNPROTECT(3);
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

    unsigned long  *lens = (unsigned long *)0;
    SEXPTYPE  *fld_Sclass;
    int   i, j, null_item, expand, *fld_nullOk, completed;
    int   num_rec, num_groups;
    int    num_fields;
    int   max_rec = INT_EL(s_max_rec,0);     /* max rec per group */
    int   ngroup = 0, group_field = INT_EL(s_group_field,0);
    long   total_records;
    int   pushed_back = FALSE;

    unsigned int event = NEVER;
    int    np = 0;        /* keeps track of PROTECT()'s */
    s_object    *beginGroupCall, *beginGroupFun = LST_EL(s_funs, 2);
    s_object    *endGroupCall,   *endGroupFun   = LST_EL(s_funs, 3);
    s_object    *newRecordCall,   *newRecordFun  = LST_EL(s_funs, 4);
    int        invoke_beginGroup = (GET_LENGTH(beginGroupFun)>0);
    int        invoke_endGroup   = (GET_LENGTH(endGroupFun)>0);
    int        invoke_newRecord  = (GET_LENGTH(newRecordFun)>0);

    row = NULL;
    beginGroupCall = R_NilValue;    /* -Wall */
    if(invoke_beginGroup){
        PROTECT(beginGroupCall=lang2(beginGroupFun, R_NilValue));
        ++np;
    }
    endGroupCall = R_NilValue;    /* -Wall */
    if(invoke_endGroup){
        /* TODO: append list(...) to the call object */
        PROTECT(endGroupCall = lang4(endGroupFun, R_NilValue,
            R_NilValue, R_NilValue));
        ++np;
    }
    newRecordCall = R_NilValue;    /* -Wall */
    if(invoke_newRecord){
        PROTECT(newRecordCall = lang2(newRecordFun, R_NilValue));
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
    PROTECT(data = NEW_LIST((int) num_fields));     /* buffer records */
    PROTECT(cur_rec = NEW_LIST((int) num_fields));  /* current record */
    np += 2;

    RS_DBI_allocOutput(cur_rec, flds, (int) 1, 0);
    RS_DBI_makeDataFrame(cur_rec);

    num_rec = INT_EL(s_batch_size, 0);     /* this is num of rec per group! */
    max_rec = INT_EL(s_max_rec,0);         /* max rec **per group**         */
    num_groups = num_rec;
    PROTECT(out_list = NEW_LIST(num_groups));
    PROTECT(group_names = NEW_CHARACTER(num_groups));
    np += 2;

    /* set conversion for group names */

    if(result->rowCount==0){
        event = BEGIN;
        /* here we could invoke the begin function*/
    }

    /* actual fetching.... */

    my_result = (MYSQL_RES *) result->drvResultSet;
    completed = (int) 0;

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
            completed = (int) (err_no ? -1 : 1);
            break;
        }

        if(!pushed_back){                      /* recompute fields lengths? */
            lens = mysql_fetch_lengths(my_result);  /* lengths for each field */
            ++total_records;
        }

        /* coerce each entry row[j] to an R/S type according to its Sclass.
         * TODO:  converter functions are badly needed.
         */
        for(j = 0; j < num_fields; j++){

            null_item = (row[j] == NULL);

            switch((int)fld_Sclass[j]){

            case INTSXP:
                if(null_item)
                    NA_SET(&(LST_INT_EL(data,j,i)), INTSXP);
                else
                    LST_INT_EL(data,j,i) = atol(row[j]);
                LST_INT_EL(cur_rec,j,0) = LST_INT_EL(data,j,i);
                break;

            case STRSXP:
                /* BUG: I need to verify that a TEXT field (which is stored as
                * a BLOB by MySQL!) is indeed char and not a true
                * Binary obj (MySQL does not truly distinguish them). This
                * test is very gross.
                */
                if(null_item)
                    SET_LST_CHR_EL(data,j,i,NA_STRING);
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

            case REALSXP:
                if(null_item)
                    NA_SET(&(LST_NUM_EL(data,j,i)), REALSXP);
                else
                    LST_NUM_EL(data,j,i) = (double) atof(row[j]);
                LST_NUM_EL(cur_rec,j,0) = LST_NUM_EL(data,j,i);
                break;

            default:  /* error, but we'll try the field as character (!)*/
                if(null_item)
                    SET_LST_CHR_EL(data,j,i, NA_STRING);
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

        if(!pushed_back){
            if(invoke_newRecord)
                RS_DBI_invokeNewRecord(newRecordCall, cur_rec, rho);
        }
        else {
            pushed_back = FALSE;
        }

        /* We just finished processing the new record, now we check
         * for some events (in addition to NEW_RECORD, of course).
         */
        event = check_groupEvents(data, fld_Sclass, i, group_field);

        if(BEGIN_GROUP & event){

            if(ngroup==num_groups){                  /* exhausted output list? */
                num_groups = 2 * num_groups;
                PROTECT(SET_LENGTH(out_list, num_groups));
                PROTECT(SET_LENGTH(group_names, num_groups));
                np += 2;
            }

            if(invoke_beginGroup)
                RS_DBI_invokeBeginGroup(
            beginGroupCall, CHR_EL(group_names, ngroup), rho);
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
            RS_DBI_allocOutput(data, flds, (int) 0, (int) 1);
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
        PROTECT(SET_LENGTH(out_list, ngroup));
        PROTECT(SET_LENGTH(group_names, ngroup));
        np += 2;
    }

    result->rowCount += total_records;
    result->completed = (int) completed;

    SET_NAMES(out_list, group_names);       /* do I need to PROTECT? */

    UNPROTECT(np);
    return out_list;
}

unsigned int
check_groupEvents(s_object *data, SEXPTYPE fld_Sclass[], int irow, int jcol)
{
    if(irow==0) /* Begin */
        return (BEGIN|BEGIN_GROUP);

    switch(fld_Sclass[jcol]){

    case LGLSXP:
        if(LST_LGL_EL(data,jcol,irow)!=LST_LGL_EL(data,jcol,irow-1))
            return (END_GROUP|BEGIN_GROUP);
        break;

    case INTSXP:
        if(LST_INT_EL(data,jcol,irow)!=LST_INT_EL(data,jcol,irow-1))
            return (END_GROUP|BEGIN_GROUP);
        break;

    case REALSXP:
        if(LST_NUM_EL(data,jcol,irow)!=LST_NUM_EL(data,jcol,irow-1))
            return (END_GROUP|BEGIN_GROUP);
        break;

    case STRSXP:
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
          SEXPTYPE *fld_Sclass, int group_field, int ngroup, int i)
{
    char  buff[1024];

    switch((int) fld_Sclass[group_field]){

    case LGLSXP:
        (void) sprintf(buff, "%ld", (long) LST_LGL_EL(data,group_field,i));
        break;
    case INTSXP:
        (void) sprintf(buff, "%ld", (long) LST_INT_EL(data,group_field,i));
        break;
    case REALSXP:
        (void) sprintf(buff, "%f", (double) LST_NUM_EL(data,group_field,i));
        break;
    case STRSXP:
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
 *       if the index is an�unsigned integer.  Should we return
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
    SEXPTYPE conType[] = {INTSXP};    /* dj: are we sure an int will do? */
    int  conLen[]  = {1};

    con = RS_DBI_getConnection(conHandle);
    my_con = (MYSQL *) con->drvConnection;
    output = RS_DBI_createNamedList(conDesc, conType, conLen, 1);

    LST_INT_EL(output,0,0) = (int) mysql_insert_id(my_con);

    return output;

}

/* The single string version of this function was kindly provided by
 * J. T. Lindgren (any bugs are probably dj's)
 *
 * NOTE/BUG?: This function could potentially grab a huge amount of memory
 *   if given (not inappropriately) very large binary objects. How should
 *   we protect against potentially deadly requests?
 */

s_object *
RS_MySQL_escapeStrings(Con_Handle *conHandle, s_object *strings)
{
    RS_DBI_connection *con;
    MYSQL             *my_connection;
    long len, old_len;
    int i, nStrings;
    char *str;
    char *escapedString;
    s_object  *output;

    con = RS_DBI_getConnection(conHandle);
    my_connection = (MYSQL *) con->drvConnection;

    nStrings = GET_LENGTH(strings);
    PROTECT(output = NEW_CHARACTER(nStrings));

    old_len = (long) 1;
    escapedString = (char *) S_alloc(old_len, (int) sizeof(char));
    if(!escapedString){
        RS_DBI_errorMessage(
           "(RS_MySQL_escapeStrings) could not allocate memory",
        RS_DBI_ERROR);
    }

    for(i=0; i<nStrings; i++){
        str = RS_DBI_copyString(CHR_EL(strings,i));
        len = (long) strlen(str);
        escapedString = (char *) S_realloc(escapedString,
               (long) 2*len+1, old_len, (int)sizeof(char));
        if(!escapedString){
            RS_DBI_errorMessage(
               "(RS_MySQL_escapeStrings) could not (re)allocate memory",
            RS_DBI_ERROR);
        }

        mysql_real_escape_string(my_connection, escapedString, str, len);

        SET_CHR_EL(output, i, C_S_CPY(escapedString));
    }

    UNPROTECT(1);
    return output;
}

s_object *
RS_MySQL_clientLibraryVersions(void)
{
	s_object *ret, *name;

	PROTECT(name=NEW_CHARACTER(2));
	SET_STRING_ELT(name, 0, COPY_TO_USER_STRING(MYSQL_SERVER_VERSION));
	SET_STRING_ELT(name, 1, COPY_TO_USER_STRING(mysql_get_client_info()));
	PROTECT(ret=NEW_INTEGER(2));
	INTEGER(ret)[0] = (int)MYSQL_VERSION_ID;
	INTEGER(ret)[1] = (int)mysql_get_client_version();
	SET_NAMES(ret,name);
	UNPROTECT(2);
	return ret;
}

void R_init_RMySQL(DllInfo *info){
  mysql_library_init(0,NULL,NULL);

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

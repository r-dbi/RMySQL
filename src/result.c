#include "RS-MySQL.h"


SEXP
  RS_DBI_allocResultSet(SEXP conHandle)
  {
    RS_DBI_connection *con = NULL;
    RS_DBI_resultSet  *result = NULL;
    SEXP rsHandle;
    int indx, res_id;

    con = RS_DBI_getConnection(conHandle);
    indx = RS_DBI_newEntry(con->resultSetIds, con->length);
    if(indx < 0){
      char msg[128], fmt[128];
      (void) strcpy(fmt, "cannot allocate a new resultSet -- ");
      (void) strcat(fmt, "maximum of %d resultSets already reached");
      (void) sprintf(msg, fmt, con->length);
      RS_DBI_errorMessage(msg, RS_DBI_ERROR);
    }

    result = (RS_DBI_resultSet *) malloc(sizeof(RS_DBI_resultSet));
    if(!result){
      char *errMsg = "could not malloc dbResultSet";
      RS_DBI_freeEntry(con->resultSetIds, indx);
      RS_DBI_errorMessage(errMsg, RS_DBI_ERROR);
    }
    result->drvResultSet = (void *) NULL; /* driver's own resultSet (cursor)*/
    result->statement = (char *) NULL;
    result->managerId = MGR_ID(conHandle);
    result->connectionId = CON_ID(conHandle);
    result->resultSetId = con->counter;
    result->isSelect = (int) -1;
    result->rowsAffected = (int) -1;
    result->rowCount = (int) 0;
    result->completed = (int) -1;
    result->fields = (RS_DBI_fields *) NULL;

    /* update connection's resultSet table */
    res_id = con->counter;
    con->num_res += (int) 1;
    con->counter += (int) 1;
    con->resultSets[indx] = result;
    con->resultSetIds[indx] = res_id;

    rsHandle = RS_DBI_asResHandle(MGR_ID(conHandle),CON_ID(conHandle),res_id);
    return rsHandle;
  }

void
  RS_DBI_freeResultSet(SEXP rsHandle)
  {
    RS_DBI_resultSet  *result;
    RS_DBI_connection *con;
    int indx;

    con = RS_DBI_getConnection(rsHandle);
    result = RS_DBI_getResultSet(rsHandle);

    if(result->drvResultSet) {
      char *errMsg =
        "internal error in RS_DBI_freeResultSet: non-freed result->drvResultSet (some memory leaked)";
      RS_DBI_errorMessage(errMsg, RS_DBI_ERROR);
    }

    if(result->statement)
      free(result->statement);
    if(result->fields)
      RS_DBI_freeFields(result->fields);
    free(result);
    result = (RS_DBI_resultSet *) NULL;

    /* update connection's resultSet table */
    indx = RS_DBI_lookup(con->resultSetIds, con->length, RES_ID(rsHandle));
    RS_DBI_freeEntry(con->resultSetIds, indx);
    con->resultSets[indx] = (RS_DBI_resultSet *) NULL;
    con->num_res -= (int) 1;

    return;
  }

SEXP RS_DBI_asResHandle(int mgrId, int conId, int resId)
{
  SEXP resHandle;

  PROTECT(resHandle = NEW_INTEGER((int) 3));
  MGR_ID(resHandle) = mgrId;
  CON_ID(resHandle) = conId;
  RES_ID(resHandle) = resId;
  UNPROTECT(1);
  return resHandle;
}



RS_DBI_resultSet* RS_DBI_getResultSet(SEXP rsHandle) {
  RS_DBI_connection *con;
  int indx;

  con = RS_DBI_getConnection(rsHandle);
  indx = RS_DBI_lookup(con->resultSetIds, con->length, RES_ID(rsHandle));
  if(indx<0)
    RS_DBI_errorMessage(
      "internal error in RS_DBI_getResultSet: could not find resultSet in connection",
      RS_DBI_ERROR);
  if(!con->resultSets[indx])
    RS_DBI_errorMessage(
      "internal error in RS_DBI_getResultSet: missing resultSet",
      RS_DBI_ERROR);
  return con->resultSets[indx];
}


SEXP        /* return a named list */
    RS_DBI_resultSetInfo(SEXP rsHandle)
    {
      RS_DBI_resultSet       *result;
      SEXP output, flds;
      int  n = (int) 6;
      char  *rsDesc[] = {"statement", "isSelect", "rowsAffected",
        "rowCount", "completed", "fields"};
      SEXPTYPE rsType[]  = {STRSXP, INTSXP, INTSXP,
        INTSXP,   INTSXP, VECSXP};
      int  rsLen[]   = {1, 1, 1, 1, 1, 1};

      result = RS_DBI_getResultSet(rsHandle);
      if(result->fields)
        flds = RS_DBI_copyfields(result->fields);
      else
        flds = R_NilValue;

      output = RS_DBI_createNamedList(rsDesc, rsType, rsLen, n);

      SET_LST_CHR_EL(output,0,0,C_S_CPY(result->statement));
      LST_INT_EL(output,1,0) = result->isSelect;
      LST_INT_EL(output,2,0) = result->rowsAffected;
      LST_INT_EL(output,3,0) = result->rowCount;
      LST_INT_EL(output,4,0) = result->completed;
      SET_ELEMENT(LST_EL(output, 5), (int) 0, flds);

      return output;
    }


SEXP
  RS_MySQL_nextResultSet(SEXP conHandle)
  {
    RS_DBI_connection *con;
    RS_DBI_resultSet  *result;
    SEXP rsHandle;
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


/* Execute (currently) one sql statement (INSERT, DELETE, SELECT, etc.),
* set coercion type mappings between the server internal data types and
* S classes.   Returns  an S handle to a resultSet object.
*/
SEXP
  RS_MySQL_exec(SEXP conHandle, SEXP statement)
  {
    RS_DBI_connection *con;
    SEXP rsHandle;
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


SEXP     /* output is a named list */
    RS_MySQL_fetch(SEXP rsHandle, SEXP max_rec)
    {
      MySQLDriver   *mgr;
      RS_DBI_resultSet *result;
      RS_DBI_fields    *flds;
      MYSQL_RES *my_result;
      MYSQL_ROW  row;
      SEXP output, s_tmp;

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
      num_rec = asInteger(max_rec);
      expand = (num_rec < 0);   /* dyn expand output to accommodate all rows*/
    if(expand || num_rec == 0){
      mgr = rmysql_driver();
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


SEXP
  RS_MySQL_closeResultSet(SEXP resHandle)
  {
    RS_DBI_resultSet *result;
    MYSQL_RES        *my_result;

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

    return ScalarLogical(TRUE);
  }


SEXP
  RS_MySQL_resultSetInfo(SEXP rsHandle)
  {
    RS_DBI_resultSet   *result;
    SEXP output, flds;
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

SEXP
  RS_MySQL_typeNames(SEXP type)
  {
    SEXP typeNames;
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

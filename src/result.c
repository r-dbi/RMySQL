#include "RS-MySQL.h"

SEXP RS_DBI_allocResultSet(SEXP conHandle) {
  RS_DBI_connection *con = RS_DBI_getConnection(conHandle);

  int indx = RS_DBI_newEntry(con->resultSetIds, con->length);
  if (indx < 0) {
    error(
      "cannot allocate a new resultSet -- maximum of %d resultSets already reached",
      con->length
    );
  }
  RS_DBI_resultSet* result = malloc(sizeof(RS_DBI_resultSet));
  if (!result) {
    RS_DBI_freeEntry(con->resultSetIds, indx);
    error("could not malloc dbResultSet");
  }
  result->drvResultSet = (void *) NULL; /* driver's own resultSet (cursor)*/
  result->statement = (char *) NULL;
  result->connectionId = CON_ID(conHandle);
  result->resultSetId = con->counter;
  result->isSelect = (int) -1;
  result->rowsAffected = (int) -1;
  result->rowCount = (int) 0;
  result->completed = (int) -1;
  result->fields = NULL;

  /* update connection's resultSet table */
  int res_id = con->counter;
  con->num_res += (int) 1;
  con->counter += (int) 1;
  con->resultSets[indx] = result;
  con->resultSetIds[indx] = res_id;

  return RS_DBI_asResHandle(MGR_ID(conHandle), CON_ID(conHandle), res_id);
}

void RS_DBI_freeResultSet(SEXP rsHandle) {
  RS_DBI_connection* con = RS_DBI_getConnection(rsHandle);
  RS_DBI_resultSet* result = RS_DBI_getResultSet(rsHandle);

  if(result->drvResultSet) {
    error("internal error in RS_DBI_freeResultSet: non-freed result->drvResultSet (some memory leaked)");
  }

  if (result->statement)
    free(result->statement);
  if (result->fields)
    rmysql_fields_free(result->fields);
  free(result);
  result = NULL;

  /* update connection's resultSet table */
  int indx = RS_DBI_lookup(con->resultSetIds, con->length, RES_ID(rsHandle));
  RS_DBI_freeEntry(con->resultSetIds, indx);
  con->resultSets[indx] = NULL;
  con->num_res -= 1;
}

SEXP RS_DBI_asResHandle(int mgrId, int conId, int resId) {
  SEXP resHandle = PROTECT(allocVector(INTSXP, 3));
  CON_ID(resHandle) = conId;
  RES_ID(resHandle) = resId;
  UNPROTECT(1);

  return resHandle;
}

RS_DBI_resultSet* RS_DBI_getResultSet(SEXP rsHandle) {
  RS_DBI_connection* con = RS_DBI_getConnection(rsHandle);
  int indx = RS_DBI_lookup(con->resultSetIds, con->length, RES_ID(rsHandle));
  if (indx < 0)
    error("internal error in RS_DBI_getResultSet: could not find resultSet in connection");
  if (!con->resultSets[indx])
    error("internal error in RS_DBI_getResultSet: missing resultSet");

  return con->resultSets[indx];
}

SEXP RS_DBI_resultSetInfo(SEXP rsHandle) {
  RS_DBI_resultSet       *result;
  SEXP output, flds;
  int  n = (int) 6;
  char  *rsDesc[] = {"statement", "isSelect", "rowsAffected",
    "rowCount", "completed", "fields"};
  SEXPTYPE rsType[]  = {STRSXP, INTSXP, INTSXP,
    INTSXP,   INTSXP, VECSXP};
  int  rsLen[]   = {1, 1, 1, 1, 1, 1};

  result = RS_DBI_getResultSet(rsHandle);
  flds = R_NilValue;

  output = RS_DBI_createNamedList(rsDesc, rsType, rsLen, n);

  SET_LST_CHR_EL(output,0,0,mkChar(result->statement));
  LST_INT_EL(output,1,0) = result->isSelect;
  LST_INT_EL(output,2,0) = result->rowsAffected;
  LST_INT_EL(output,3,0) = result->rowCount;
  LST_INT_EL(output,4,0) = result->completed;
  SET_ELEMENT(LST_EL(output, 5), (int) 0, flds);

  return output;
}

SEXP RS_MySQL_nextResultSet(SEXP conHandle) {
  RS_DBI_resultSet  *result;
  SEXP rsHandle;
  int num_fields, is_select;

  RS_DBI_connection* con = RS_DBI_getConnection(conHandle);
  MYSQL* my_connection = con->drvConnection;

  int rc = mysql_next_result(my_connection);
  if (rc < 0) {
    error("no more result sets");
  } else if (rc > 0){
    error("error in getting next result set");
  }

  /* the following comes verbatim from RS_MySQL_exec() */
  MYSQL_RES* my_result = mysql_use_result(my_connection);
  if (!my_result)
    my_result = NULL;

  num_fields = mysql_field_count(my_connection);
  is_select = TRUE;
  if (!my_result) {
    if (num_fields > 0) {
      error("error in getting next result set");
    } else {
      is_select = FALSE;
    }
  }

  /* we now create the wrapper and copy values */
  rsHandle = RS_DBI_allocResultSet(conHandle);
  result = RS_DBI_getResultSet(rsHandle);
  result->statement = RS_DBI_copyString("<UNKNOWN>");
  result->drvResultSet = (void *) my_result;
  result->rowCount = (int) 0;
  result->isSelect = is_select;
  if (!is_select){
    result->rowsAffected = (int) mysql_affected_rows(my_connection);
    result->completed = 1;
  } else {
    result->rowsAffected = (int) -1;
    result->completed = 0;
  }

  if (is_select)
    result->fields = RS_MySQL_createDataMappings(rsHandle);

  return rsHandle;
}


/* Execute (currently) one sql statement (INSERT, DELETE, SELECT, etc.),
* set coercion type mappings between the server internal data types and
* S classes.   Returns  an S handle to a resultSet object.
*/
SEXP RS_MySQL_exec(SEXP conHandle, SEXP statement) {
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
    error("connection with pending rows, close resultSet before continuing");
  }
  else
    RS_MySQL_closeResultSet(rsHandle);
  }

  /* Here is where we actually run the query */
  state = mysql_query(my_connection, dyn_statement);
  if(state) {
    error("could not run statement: %s", mysql_error(my_connection));
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
      error("error in select/select-like");
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


// output is a named list
SEXP RS_MySQL_fetch(SEXP rsHandle, SEXP max_rec) {
  MySQLDriver   *mgr;
  RS_DBI_resultSet *result;
  RMySQLFields* flds;
  MYSQL_RES *my_result;
  MYSQL_ROW  row;
  SEXP output, s_tmp;

  unsigned long  *lens;
  int    i, j, null_item, expand;
  int   completed;
  SEXPTYPE  *fld_Sclass;
  int   num_rec;
  int    num_fields;

  result = RS_DBI_getResultSet(rsHandle);
  flds = result->fields;
  if(!flds)
    error("corrupt resultSet, missing fieldDescription");
  num_rec = asInteger(max_rec);
  expand = (num_rec < 0);   // dyn expand output to accommodate all rows
  if(expand || num_rec == 0){
    mgr = rmysql_driver();
    num_rec = mgr->fetch_default_rec;
  }
  num_fields = flds->num_fields;
  PROTECT(output = NEW_LIST((int) num_fields));
  RS_DBI_allocOutput(output, flds, num_rec, 0);
  fld_Sclass = flds->Sclass;

  // actual fetching....
  my_result = (MYSQL_RES *) result->drvResultSet;
  completed = (int) 0;

  for(i = 0; ; i++){
    if(i==num_rec){  // exhausted the allocated space

      if(expand){    // do we extend or return the records fetched so far
        num_rec = 2 * num_rec;
        RS_DBI_allocOutput(output, flds, num_rec, expand);
      }
      else
        break;       // okay, no more fetching for now
    }
    row = mysql_fetch_row(my_result);
    if(row==NULL){    // either we finish or we encounter an error
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
        // BUG: I need to verify that a TEXT field (which is stored as
        // a BLOB by MySQL!) is indeed char and not a true
        // Binary obj (MySQL does not truly distinguish them). This
        // test is very gross.
        if(null_item)
          SET_LST_CHR_EL(output,j,i,NA_STRING);
        else {
          if((size_t) lens[j] != strlen(row[j])){
            warning("internal error: row %d field %d truncated", i, j);
          }
          SET_LST_CHR_EL(output,j,i,mkChar(row[j]));
        }
        break;

      case REALSXP:
        if(null_item)
          NA_SET(&(LST_NUM_EL(output,j,i)), REALSXP);
        else
          LST_NUM_EL(output,j,i) = (double) atof(row[j]);
        break;

      default:  // error, but we'll try the field as character (!)
        if(null_item)
          SET_LST_CHR_EL(output,j,i, NA_STRING);
        else {
          warning("unrecognized field type %d in column %d", fld_Sclass[j], j);
          SET_LST_CHR_EL(output,j,i,mkChar(row[j]));
        }
        break;
      }
    }
  }

  // actual number of records fetched
  if(i < num_rec){
    num_rec = i;
    // adjust the length of each of the members in the output_list
    for(j = 0; j<num_fields; j++){
      s_tmp = LST_EL(output,j);
      PROTECT(SET_LENGTH(s_tmp, num_rec));
      SET_ELEMENT(output, j, s_tmp);
      UNPROTECT(1);
    }
  }
  if(completed < 0)
    warning("error while fetching rows");

  result->rowCount += num_rec;
  result->completed = (int) completed;

  UNPROTECT(1);
  return output;
}


SEXP RS_MySQL_closeResultSet(SEXP resHandle) {
  RS_DBI_resultSet *result;
  MYSQL_RES        *my_result;

  result = RS_DBI_getResultSet(resHandle);

  my_result = (MYSQL_RES *) result->drvResultSet;
  if(my_result){
    // we need to flush any possibly remaining rows (see Manual Ch 20 p358)
    MYSQL_ROW row;
    while((row = mysql_fetch_row(result->drvResultSet)))
      ;
  }
  mysql_free_result(my_result);

  // need to NULL drvResultSet, otherwise can't free the rsHandle
  result->drvResultSet = (void *) NULL;
  RS_DBI_freeResultSet(resHandle);

  return ScalarLogical(TRUE);
}


SEXP RS_MySQL_resultSetInfo(SEXP rsHandle) {
  RS_DBI_resultSet   *result;
  SEXP output, flds;
  int  n = 6;
  char  *rsDesc[] = {"statement", "isSelect", "rowsAffected",
    "rowCount", "completed", "fieldDescription"};
  SEXPTYPE rsType[]  = {STRSXP, INTSXP, INTSXP,
    INTSXP,   INTSXP, VECSXP};
  int  rsLen[]   = {1, 1, 1, 1, 1, 1};

  result = RS_DBI_getResultSet(rsHandle);
  flds = R_NilValue;

  output = RS_DBI_createNamedList(rsDesc, rsType, rsLen, n);

  SET_LST_CHR_EL(output,0,0,mkChar(result->statement));
  LST_INT_EL(output,1,0) = result->isSelect;
  LST_INT_EL(output,2,0) = result->rowsAffected;
  LST_INT_EL(output,3,0) = result->rowCount;
  LST_INT_EL(output,4,0) = result->completed;
  if(flds != R_NilValue)
    SET_ELEMENT(LST_EL(output, 5), (int) 0, flds);

  return output;
}

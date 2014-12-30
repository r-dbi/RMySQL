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
    result->drvData = (void *) NULL;   /* this can be used by driver*/
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
    if(result->drvData){
      char *errMsg =
        "internal error in RS_DBI_freeResultSet: non-freed result->drvData (some memory leaked)";
      RS_DBI_errorMessage(errMsg, RS_DBI_WARNING);
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

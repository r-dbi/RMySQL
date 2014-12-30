#include "RS-MySQL.h"


void
  RS_DBI_setException(SEXP handle, DBI_EXCEPTION exceptionType,
    int errorNum, const char *errorMsg)
  {
    HANDLE_TYPE handleType;

    handleType = (int) GET_LENGTH(handle);
    if(handleType == MGR_HANDLE_TYPE){
      RS_DBI_manager *obj;
      obj =  RS_DBI_getManager(handle);
      obj->exception->exceptionType = exceptionType;
      obj->exception->errorNum = errorNum;
      obj->exception->errorMsg = RS_DBI_copyString(errorMsg);
    }
    else if(handleType==CON_HANDLE_TYPE){
      RS_DBI_connection *obj;
      obj = RS_DBI_getConnection(handle);
      obj->exception->exceptionType = exceptionType;
      obj->exception->errorNum = errorNum;
      obj->exception->errorMsg = RS_DBI_copyString(errorMsg);
    }
    else {
      RS_DBI_errorMessage(
        "internal error in RS_DBI_setException: could not setException",
        RS_DBI_ERROR);
    }
    return;
  }

void
  RS_DBI_errorMessage(char *msg, DBI_EXCEPTION exception_type)
  {
    char *driver = "RS-DBI";   /* TODO: use the actual driver name */

    switch(exception_type) {
    case RS_DBI_MESSAGE:
      warning("%s driver message: (%s)", driver, msg);
      break;
    case RS_DBI_WARNING:
      warning("%s driver warning: (%s)", driver, msg);
      break;
    case RS_DBI_ERROR:
      error("%s driver: (%s)", driver, msg);
      break;
    case RS_DBI_TERMINATE:
      error("%s driver fatal: (%s)", driver, msg); /* was TERMINATE */
      break;
    }
    return;
  }


/* return a 2-elem list with the last exception number and
* exception message on a given connection.
*/
SEXP
  RS_MySQL_getException(SEXP conHandle)
  {
    MYSQL *my_connection;
    SEXP output;
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

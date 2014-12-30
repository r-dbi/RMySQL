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

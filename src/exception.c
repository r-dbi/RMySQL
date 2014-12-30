#include "RS-MySQL.h"

SEXP rmysql_exception_info(SEXP conHandle) {
  RS_DBI_connection* con = RS_DBI_getConnection(conHandle);
  if (!con->drvConnection)
    error("RMySQL error: corrupt connection handle");

  MYSQL* my_connection = con->drvConnection;

  // Allocate output
  SEXP output = PROTECT(allocVector(VECSXP, 2));
  SEXP output_nms = PROTECT(allocVector(STRSXP, 2));
  SET_NAMES(output, output_nms);
  UNPROTECT(1);

  SET_CHR_EL(output_nms, 0, mkChar("errorNum"));
  SET_VECTOR_ELT(output, 0, ScalarInteger(mysql_errno(my_connection)));

  SET_CHR_EL(output_nms, 1, mkChar("errorMsg"));
  SET_VECTOR_ELT(output, 1, mkString(mysql_error(my_connection)));

  UNPROTECT(1);
  return output;
}

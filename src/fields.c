#include "RS-MySQL.h"

void rmysql_fields_free(RMySQLFields* flds) {
  int i;
  if (flds->name) {
    for(i = 0; i < flds->num_fields; i++) {
      if (flds->name[i])
        free(flds->name[i]);
    }
    free(flds->name);
  }
  if(flds->type) free(flds->type);
  if(flds->length) free(flds->length);
  if(flds->precision) free(flds->precision);
  if(flds->scale) free(flds->scale);
  if(flds->nullOk) free(flds->nullOk);
  if(flds->isVarLength) free(flds->isVarLength);
  if(flds->Sclass) free(flds->Sclass);
  free(flds);
  flds = NULL;
  return;
}

RMySQLFields* RS_MySQL_createDataMappings(SEXP rsHandle) {
  // Fetch MySQL field descriptions
  RS_DBI_resultSet* result = RS_DBI_getResultSet(rsHandle);
  MYSQL_RES* my_result = result->drvResultSet;
  MYSQL_FIELD* select_dp = mysql_fetch_fields(my_result);
  int num_fields = mysql_num_fields(my_result);

  // Allocate memory for output object
  RMySQLFields* flds = malloc(sizeof(RMySQLFields));
  if (!flds) {
    error("Could not allocate memory for database fields");
  }

  flds->num_fields =  num_fields;
  flds->name =        calloc(num_fields, sizeof(char *));
  flds->type =        calloc(num_fields, sizeof(int));
  flds->length =      calloc(num_fields, sizeof(int));
  flds->precision =   calloc(num_fields, sizeof(int));
  flds->scale =       calloc(num_fields, sizeof(int));
  flds->nullOk =      calloc(num_fields, sizeof(int));
  flds->isVarLength = calloc(num_fields, sizeof(int));
  flds->Sclass =      calloc(num_fields, sizeof(SEXPTYPE));

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
  for (int j = 0; j < num_fields; j++){
    /* First, save the name, MySQL internal field name, type, length, etc. */
    flds->name[j] = RS_DBI_copyString(select_dp[j].name);
    flds->type[j] = select_dp[j].type;  /* recall that these are enum*/
    flds->length[j] = select_dp[j].length;
    flds->precision[j] = select_dp[j].length;
    flds->scale[j] = select_dp[j].decimals;
    flds->nullOk[j] = (!IS_NOT_NULL(select_dp[j].flags));

    int internal_type = select_dp[j].type;
    switch(internal_type) {
      case FIELD_TYPE_VAR_STRING:
      case FIELD_TYPE_STRING:
        flds->Sclass[j] = STRSXP;
        flds->isVarLength[j] = (int) 1;
        break;
      case FIELD_TYPE_TINY:            /* 1-byte TINYINT   */
      case FIELD_TYPE_SHORT:           /* 2-byte SMALLINT  */
      case FIELD_TYPE_INT24:           /* 3-byte MEDIUMINT */
        flds->Sclass[j] = INTSXP;
      case FIELD_TYPE_LONG:            /* 4-byte INTEGER   */
        /* if unsigned, turn into numeric (may be too large for ints/long)*/
        if(select_dp[j].flags & UNSIGNED_FLAG) {
          warning("Unsigned INTEGER in col %d imported as numeric", j);
          flds->Sclass[j] = REALSXP;
        } else {
          flds->Sclass[j] = INTSXP;
        }
        break;
      case FIELD_TYPE_LONGLONG:       /* 8-byte BIGINT   */
        flds->Sclass[j] = REALSXP;
        break;

  #if defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 50003 /* 5.0.3 */
      case FIELD_TYPE_BIT:
        if(flds->precision[j] <= sizeof(int)) {
          /* can R int hold the bytes? */
          flds->Sclass[j] = INTSXP;
        } else {
          flds->Sclass[j] = STRSXP;
          warning(
            "BIT field in column %d too long (%d bits) for an R integer (imported as character)",
            j+1, flds->precision[j]
          );
        }
        break;
  #endif
        flds->Sclass[j] = REALSXP;
        break;
      case FIELD_TYPE_DECIMAL:
  #if defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 50003 /* 5.0.3 */
      case FIELD_TYPE_NEWDECIMAL:
  #endif
        warning("Decimal MySQL column %d imported as numeric", j);
        flds->Sclass[j] = REALSXP;
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
        warning("unrecognized MySQL field type %d in column %d imported as character",
          internal_type, j);
        break;
    }
  }
  return flds;
}

struct data_types {
  char *typeName;
  int typeId;
};
struct data_types rmysql_types[] = {
  { "DECIMAL",    FIELD_TYPE_DECIMAL},
  { "TINYINT",       FIELD_TYPE_TINY},
  { "SMALLINT",      FIELD_TYPE_SHORT},
  { "INTEGER",       FIELD_TYPE_LONG},
  { "MEDIUMINT",      FIELD_TYPE_INT24},
  { "BIGINT",   FIELD_TYPE_LONGLONG},
  { "FLOAT",      FIELD_TYPE_FLOAT},
  { "DOUBLE",     FIELD_TYPE_DOUBLE},
  { "NULL",       FIELD_TYPE_NULL},
  { "TIMESTAMP",  FIELD_TYPE_TIMESTAMP},
  { "DATE",       FIELD_TYPE_DATE},
  { "TIME",       FIELD_TYPE_TIME},
  { "DATETIME",   FIELD_TYPE_DATETIME},
  { "YEAR",       FIELD_TYPE_YEAR},
  { "ENUM",       FIELD_TYPE_ENUM},
  { "SET",        FIELD_TYPE_SET},
  { "BLOB/TEXT",       FIELD_TYPE_BLOB},
  { "VAR_STRING", FIELD_TYPE_VAR_STRING},
  { "STRING",     FIELD_TYPE_STRING},
  { NULL, -1 }
};

char* rmysql_type(int type) {
  for (int i = 0; rmysql_types[i].typeName != NULL; i++) {
    if (rmysql_types[i].typeId == type)
      return rmysql_types[i].typeName;
  }
  return "<unknown>";
}

SEXP rmysql_fields_info(SEXP rsHandle) {
  RS_DBI_resultSet* result = RS_DBI_getResultSet(rsHandle);
  RMySQLFields* flds = result->fields;
  int n = flds->num_fields;

  // Allocate output
  SEXP output = PROTECT(allocVector(VECSXP, 4));
  SEXP output_nms = PROTECT(allocVector(STRSXP, 4));
  SET_NAMES(output, output_nms);
  UNPROTECT(1);

  SET_STRING_ELT(output_nms, 0, mkChar("name"));
  SEXP names = PROTECT(allocVector(STRSXP, n));
  for (int j = 0; j < n; j++) {
    SET_STRING_ELT(names, j, mkChar(flds->name[j]));
  }
  SET_VECTOR_ELT(output, 0, names);
  UNPROTECT(1);

  SET_STRING_ELT(output_nms, 1, mkChar("Sclass"));
  SEXP sclass = PROTECT(allocVector(STRSXP, n));
  for (int j = 0; j < n; j++) {
    const char* type = type2char(flds->Sclass[j]);
    SET_STRING_ELT(sclass, j, mkChar(type));
  }
  SET_VECTOR_ELT(output, 1, sclass);
  UNPROTECT(1);

  SET_STRING_ELT(output_nms, 2, mkChar("type"));
  SEXP types = PROTECT(allocVector(STRSXP, n));
  for (int j = 0; j < n; j++) {
    char* type = rmysql_type(flds->type[j]);
    SET_STRING_ELT(types, j, mkChar(type));
  }
  SET_VECTOR_ELT(output, 2, types);
  UNPROTECT(1);

  SET_STRING_ELT(output_nms, 3, mkChar("length"));
  SEXP lens = PROTECT(allocVector(INTSXP, n));
  for (int j = 0; j < n; j++) {
    INTEGER(lens)[j] = flds->length[j];
  }
  SET_VECTOR_ELT(output, 3, lens);
  UNPROTECT(1);

  UNPROTECT(1);
  return output;
}

#include "RS-MySQL.h"


RS_DBI_fields *
  RS_DBI_allocFields(int num_fields)
  {
    RS_DBI_fields *flds;
    size_t n;

    flds = (RS_DBI_fields *)malloc(sizeof(RS_DBI_fields));
    if(!flds){
      char *errMsg = "could not malloc RS_DBI_fields";
      RS_DBI_errorMessage(errMsg, RS_DBI_ERROR);
    }
    n = (size_t) num_fields;
    flds->num_fields = num_fields;
    flds->name =     (char **) calloc(n, sizeof(char *));
    flds->type =     (int *) calloc(n, sizeof(int));
    flds->length =   (int *) calloc(n, sizeof(int));
    flds->precision= (int *) calloc(n, sizeof(int));
    flds->scale =    (int *) calloc(n, sizeof(int));
    flds->nullOk =   (int *) calloc(n, sizeof(int));
    flds->isVarLength = (int *) calloc(n, sizeof(int));
    flds->Sclass =   (SEXPTYPE *) calloc(n, sizeof(SEXPTYPE));

    return flds;
  }

void
  RS_DBI_freeFields(RS_DBI_fields *flds)
  {
    int i;
    if(flds->name) {       /* (as per Jeff Horner's patch) */
      for(i = 0; i < flds->num_fields; i++)
        if(flds->name[i]) free(flds->name[i]);
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
    flds = (RS_DBI_fields *) NULL;
    return;
  }


SEXP RS_DBI_copyfields(RS_DBI_fields *flds){

  SEXP S_fields;
  int  n = (int) 8;
  char  *desc[]={"name", "Sclass", "type", "len", "precision",
    "scale","isVarLength", "nullOK"};
  SEXPTYPE types[] = {STRSXP, INTSXP, INTSXP,
    INTSXP, INTSXP, INTSXP,
    LGLSXP, LGLSXP};
  int  lengths[8];
  int   i, j, num_fields;

  num_fields = flds->num_fields;
  for(j = 0; j < n; j++)
    lengths[j] = (int) num_fields;
  S_fields =  RS_DBI_createNamedList(desc, types, lengths, n);

  /* copy contentes from flds into an R/S list */
  for(i = 0; i < num_fields; i++){
    SET_LST_CHR_EL(S_fields,0,i, C_S_CPY(flds->name[i]));
    LST_INT_EL(S_fields,1,i) = (int) flds->Sclass[i];
    LST_INT_EL(S_fields,2,i) = (int) flds->type[i];
    LST_INT_EL(S_fields,3,i) = (int) flds->length[i];
    LST_INT_EL(S_fields,4,i) = (int) flds->precision[i];
    LST_INT_EL(S_fields,5,i) = (int) flds->scale[i];
    LST_INT_EL(S_fields,6,i) = (int) flds->isVarLength[i];
    LST_INT_EL(S_fields,7,i) = (int) flds->nullOk[i];
  }

  return S_fields;
}


SEXP RS_DBI_getFieldDescriptions(RS_DBI_fields *flds){

  SEXP S_fields;
  int  n = (int) 7;
  int  lengths[7];
  char  *desc[]={"name", "Sclass", "type", "len", "precision",
    "scale","nullOK"};
  SEXPTYPE types[] = {STRSXP, INTSXP, INTSXP,
    INTSXP, INTSXP, INTSXP, LGLSXP};
  int   i, j;
  int    num_fields;

  num_fields = flds->num_fields;
  for(j = 0; j < n; j++)
    lengths[j] = (int) num_fields;
  PROTECT(S_fields =  RS_DBI_createNamedList(desc, types, lengths, n));

  /* copy contentes from flds into an R/S list */
  for(i = 0; i < (int) num_fields; i++){
    SET_LST_CHR_EL(S_fields,0,i,C_S_CPY(flds->name[i]));
    LST_INT_EL(S_fields,1,i) = (int) flds->Sclass[i];
    LST_INT_EL(S_fields,2,i) = (int) flds->type[i];
    LST_INT_EL(S_fields,3,i) = (int) flds->length[i];
    LST_INT_EL(S_fields,4,i) = (int) flds->precision[i];
    LST_INT_EL(S_fields,5,i) = (int) flds->scale[i];
    LST_INT_EL(S_fields,6,i) = (int) flds->nullOk[i];
  }
  UNPROTECT(1);
  return(S_fields);
}


/* given a type id return its human-readable name.
 * We define an RS_DBI_dataTypeTable */
char* RS_DBI_getTypeName(int t, const struct data_types table[]) {
  int i;
  char buf[128];

  for (i = 0; table[i].typeName != (char *) 0; i++) {
    if (table[i].typeId == t)
      return table[i].typeName;
  }
  sprintf(buf, "unknown type (%ld)", (long) t);
  RS_DBI_errorMessage(buf, RS_DBI_WARNING);
  return (char *) 0; /* for -Wall */
}


RS_DBI_fields *
  RS_MySQL_createDataMappings(SEXP rsHandle)
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

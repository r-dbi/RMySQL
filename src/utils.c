#include "RS-MySQL.h"

// Turn a list in to a data frame, in place
void make_data_frame(SEXP data) {
  int n = length(VECTOR_ELT(data, 0));

  SEXP rownames = PROTECT(allocVector(REALSXP, 2));
  REAL(rownames)[0] = NA_REAL;
  REAL(rownames)[1] = -n;

  setAttrib(data, R_RowNamesSymbol, rownames);
  setAttrib(data, R_ClassSymbol, mkString("data.frame"));

  UNPROTECT(1);
  return;
}

void RS_DBI_allocOutput(SEXP output, RMySQLFields* flds, int num_rec, int  expand) {
  SEXP names, s_tmp;
  int   j;
  int    num_fields;
  SEXPTYPE  *fld_Sclass;

  PROTECT(output);

  num_fields = flds->num_fields;
  if(expand){
    for(j = 0; j < (int) num_fields; j++){
      /* Note that in R-1.2.3 (at least) we need to protect SET_LENGTH */
      s_tmp = LST_EL(output,j);
      PROTECT(SET_LENGTH(s_tmp, num_rec));
      SET_ELEMENT(output, j, s_tmp);
      UNPROTECT(1);
    }
    UNPROTECT(1);
    return;
  }

  fld_Sclass = flds->Sclass;
  for(j = 0; j < (int) num_fields; j++){
    switch((int)fld_Sclass[j]){
    case LGLSXP:
      SET_ELEMENT(output, j, NEW_LOGICAL(num_rec));
      break;
    case STRSXP:
      SET_ELEMENT(output, j, NEW_CHARACTER(num_rec));
      break;
    case INTSXP:
      SET_ELEMENT(output, j, NEW_INTEGER(num_rec));
      break;
    case REALSXP:
      SET_ELEMENT(output, j, NEW_NUMERIC(num_rec));
      break;
    case VECSXP:
      SET_ELEMENT(output, j, NEW_LIST(num_rec));
      break;
    default:
      error("unsupported data type");
    }
  }

  PROTECT(names = NEW_CHARACTER((int) num_fields));
  for(j = 0; j< (int) num_fields; j++){
    SET_CHR_EL(names,j, mkChar(flds->name[j]));
  }
  SET_NAMES(output, names);

  UNPROTECT(2);

  return;
}

/* wrapper to strcpy */
char* RS_DBI_copyString(const char *str) {
  char *buffer;

  buffer = (char *) malloc((size_t) strlen(str)+1);
  if(!buffer)
    error("internal error in RS_DBI_copyString: could not alloc string space");
  return strcpy(buffer, str);
}

/* wrapper to strncpy, plus (optionally) deleting trailing spaces */

SEXP RS_DBI_createNamedList(char **names, SEXPTYPE *types, int *lengths, int  n) {
  SEXP output, output_names, obj = R_NilValue;
  int  num_elem;
  int   j;

  PROTECT(output = NEW_LIST(n));
  PROTECT(output_names = NEW_CHARACTER(n));
  for(j = 0; j < n; j++){
    num_elem = lengths[j];
    switch((int)types[j]){
    case LGLSXP:
      PROTECT(obj = NEW_LOGICAL(num_elem));
      break;
    case INTSXP:
      PROTECT(obj = NEW_INTEGER(num_elem));
      break;
    case REALSXP:
      PROTECT(obj = NEW_NUMERIC(num_elem));
      break;
    case STRSXP:
      PROTECT(obj = NEW_CHARACTER(num_elem));
      break;
    case VECSXP:
      PROTECT(obj = NEW_LIST(num_elem));
      break;
    default:
      error("unsupported data type");
    }
    SET_ELEMENT(output, (int)j, obj);
    SET_CHR_EL(output_names, j, mkChar(names[j]));
  }
  SET_NAMES(output, output_names);
  UNPROTECT(n+2);
  return(output);
}

/* Very simple objectId (mapping) table. newEntry() returns an index
 * to an empty cell in table, and lookup() returns the position in the
 * table of obj_id.  Notice that we decided not to touch the entries
 * themselves to give total control to the invoking functions (this
 * simplify error management in the invoking routines.)
 */
int RS_DBI_newEntry(int *table, int length)   {
  int i, indx, empty_val;

  indx = empty_val = (int) -1;
  for(i = 0; i < length; i++)
    if(table[i] == empty_val){
      indx = i;
      break;
    }
    return indx;
}
int RS_DBI_lookup(int *table, int length, int obj_id) {
  int i, indx;

  indx = (int) -1;
  for(i = 0; i < length; ++i){
    if(table[i]==obj_id){
      indx = i;
      break;
    }
  }
  return indx;
}

/* return a list of entries pointed by *entries (we allocate the space,
 * but the caller should free() it).  The function returns the number
 * of entries.
 */
int RS_DBI_listEntries(int *table, int length, int *entries)   {
  int i,n;

  for(i=n=0; i<length; i++){
    if(table[i]<0) continue;
    entries[n++] = table[i];
  }
  return n;
}
void RS_DBI_freeEntry(int *table, int indx) { /* no error checking!!! */
  int empty_val = (int) -1;
  table[indx] = empty_val;
  return;
}


/*  These 2 R-specific functions are used by the C macros IS_NA(p,t)
*  and NA_SET(p,t) (in this way one simply use macros to test and set
*  NA's regardless whether we're using R or S.
*/
void
  RS_na_set(void *ptr, SEXPTYPE type)
  {
    double *d;
    int   *i;
    switch(type){
    case INTSXP:
      i = (int *) ptr;
      *i = NA_INTEGER;
      break;
    case LGLSXP:
      i = (int *) ptr;
      *i = NA_LOGICAL;
      break;
    case REALSXP:
      d = (double *) ptr;
      *d = NA_REAL;
      break;
    }
  }
int
  RS_is_na(void *ptr, SEXPTYPE type)
  {
    int *i, out = -2;
    char *c;
    double *d;

    switch(type){
    case INTSXP:
    case LGLSXP:
      i = (int *) ptr;
      out = (int) ((*i) == NA_INTEGER);
      break;
    case REALSXP:
      d = (double *) ptr;
      out = ISNA(*d);
      break;
    case STRSXP:
      c = (char *) ptr;
      out = (int) (strcmp(c, CHR_EL(NA_STRING, 0))==0);
      break;
    }
    return out;
  }


/* The single string version of this function was kindly provided by
* J. T. Lindgren (any bugs are probably dj's)
*
* NOTE/BUG?: This function could potentially grab a huge amount of memory
*   if given (not inappropriately) very large binary objects. How should
*   we protect against potentially deadly requests?
*/

SEXP rmysql_escape_strings(SEXP conHandle, SEXP strings) {
  MYSQL* con = RS_DBI_getConnection(conHandle)->drvConnection;

  int n = length(strings);
  SEXP output = PROTECT(allocVector(STRSXP, n));

  char* escaped = NULL;
  for(int i = 0; i < n; i++){
    const char* string = CHAR(STRING_ELT(strings, i));

    size_t len = strlen(string);
    escaped = realloc(escaped, (2 * len + 1) * sizeof(char));
    if (!escaped) {
      error("Could not allocate memory to escape string");
    }

    mysql_real_escape_string(con, escaped, string, len);
    SET_STRING_ELT(output, i, mkChar(escaped));
  }
  free(escaped);

  UNPROTECT(1);
  return output;
}

SEXP rmysql_version() {
  SEXP output = PROTECT(allocVector(INTSXP, 2));
  SEXP output_nms = PROTECT(allocVector(STRSXP, 2));
  SET_NAMES(output, output_nms);
  UNPROTECT(1);

  SET_STRING_ELT(output_nms, 0, mkChar(MYSQL_SERVER_VERSION));
  INTEGER(output)[0] = MYSQL_VERSION_ID;

  SET_STRING_ELT(output_nms, 1, mkChar(mysql_get_client_info()));
  INTEGER(output)[1] = mysql_get_client_version();

  UNPROTECT(1);
  return output;
}


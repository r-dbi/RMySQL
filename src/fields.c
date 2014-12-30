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

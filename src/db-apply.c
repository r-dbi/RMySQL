#include "RS-MySQL.h"

/*
 * RS_MySQL_dbApply.
 *
 * R/S: dbApply(rs, INDEX, FUN, group.begin, group.end, end, ...)
 *
 * This first implementation of R's dbApply()
 * extracts rows from an open result set rs and applies functions
 * to those rows of each group.  This is how it works: it keeps tracks of
 * the values of the field pointed by "group" and it identifies events:
 * BEGIN_GROUP (just read the first row of a different group),
 * NEW_RECORD (every record fetched generates this event),
 * and END_GROUP (just finished with the current group). At these points
 * we invoke the R functions group.end() and group.begin() in the
 * environment() of dbApply
 * [should it be the environment where dbApply was called from (i.e.,
   * dbApply's parent's * frame)?]
   * Except for the very first group, the order of invocation is
   * end.group() followed by begin.group()
   *
   * NOTE: We're thinking of groups as commonly defined in awk scripts
   * (but also in SAP's ABAP/4) were rows are assumed to be sorted by
   * the "group" fields and we detect a different (new) group when any of
   * the "group" fields changes.  Our implementation does not require
   * the result set to be sorted by group, but for performance-sake,
   * it better be.
   *
   * TODO: 1. Notify the reason for exiting (normal, exhausted maxBatches, etc.)
   *       2. Allow INDEX to be a list, as in tapply().
   *       3. Handle NA's (SQL NULL's) in the INDEX and/or data fields.
   *          Currently they are ignored, thus effectively causing a
   *          new BEGIN_GROUP event.
   *       4. Re-write fetch() in terms of events (END_OF_DATA,
     *          EXHAUST_DATAFRAME, DB_ERROR, etc.)
     *       5. Create a table of R callback functions indexed by events,
     *          then a handle_event() could conveniently handle all the events.
     */

SEXP     expand_list(SEXP old, int new_len);
void         add_group(SEXP group_names, SEXP data,
  SEXPTYPE *fld_Sclass, int group,
  int ngroup, int i);
unsigned int check_groupEvents(SEXP data, SEXPTYPE fld_Sclass[],
  int row, int col);

/* The following are the masks for the events/states we recognize as we
* bring rows from the result set/cursor
*/
#define NEVER           0
#define BEGIN           1  /* prior to reading 1st row from the resultset */
#define END             2  /* after reading last row from the result set  */
#define BEGIN_GROUP     4  /* just read in 1'st row for a different group */
#define END_GROUP       8  /* just read the last row of the current group */
#define NEW_RECORD     16  /* uninteresting ... */
#define PARTIAL_GROUP  32  /* too much data (>max_rex) partial buffer     */

/* the following are non-grouping events (e.g., db errors, memory) */
#define EXHAUSTED_DF   64  /* exhausted the allocated data.frame  */
#define EXHAUSTED_OUT 128  /* exhausted the allocated output list */
#define END_OF_DATA   256  /* end of data from the result set     */
#define DBMS_ERROR    512  /* error in remote dbms                */

/* beginGroupFun takes only one arg: the name of the current group */
SEXP
  RS_DBI_invokeBeginGroup(SEXP callObj,      /* should be initialized */
const char *group_name, /* one string */
SEXP rho)
  {
    SEXP s_group_name;

    /* make a copy of the argument */
    PROTECT(s_group_name = NEW_CHARACTER((int) 1));
    SET_CHR_EL(s_group_name, 0, mkChar(group_name));

    /* and stick into call object */
    SETCADR(callObj, s_group_name);
    eval(callObj, rho);
    UNPROTECT(1);

    return R_NilValue;
  }

SEXP
  RS_DBI_invokeNewRecord(SEXP callObj,   /* should be initialized already */
    SEXP new_record,/* a 1-row data.frame */
    SEXP rho)
  {
    SEXP df;

    /* make a copy of the argument */
    PROTECT(df = duplicate(new_record));

    /* and stick it into the call object */
    SETCADR(callObj, df);
    eval(callObj, rho);
    UNPROTECT(1);

    return R_NilValue;
  }

/* endGroupFun takes two args: a data.frame and the group name */
SEXP
  RS_DBI_invokeEndGroup(SEXP callObj, SEXP data,
    const char *group_name, SEXP rho)
  {
    SEXP s_x, s_group_name, val;

    /* make copies of the arguments */
    PROTECT(callObj = duplicate(callObj));
    PROTECT(s_x = duplicate(data));
    PROTECT(s_group_name = NEW_CHARACTER((int) 1));
    SET_CHR_EL(s_group_name, 0, mkChar(group_name));

    /* stick copies of args into the call object */
    SETCADR(callObj, s_x);
    SETCADDR(callObj, s_group_name);
    SETCADDDR(callObj, R_DotsSymbol);

    val = eval(callObj, rho);

    UNPROTECT(3);
    return val;
  }

SEXP                                /* output is a named list */
    RS_MySQL_dbApply(SEXP rsHandle,     /* resultset handle */
    SEXP s_group_field,/* this is a 0-based field number */
    SEXP s_funs,       /* a 5-elem list with handler funs */
    SEXP rho,          /* the env where to run funs */
    SEXP s_batch_size, /* alloc these many rows */
    SEXP s_max_rec)    /* max rows per group */
    {
      RS_DBI_resultSet *result;
      RMySQLFields* flds;

      MYSQL_RES *my_result;
      MYSQL_ROW  row;

      SEXP data, cur_rec, out_list, group_names, val;

      unsigned long  *lens = (unsigned long *)0;
      SEXPTYPE  *fld_Sclass;
      int   i, j, null_item, expand, completed;
      int   num_rec, num_groups;
      int    num_fields;
      int   max_rec = INT_EL(s_max_rec,0);     /* max rec per group */
    int   ngroup = 0, group_field = INT_EL(s_group_field,0);
    long   total_records;
    int   pushed_back = FALSE;

    unsigned int event = NEVER;
    int    np = 0;        /* keeps track of PROTECT()'s */
    SEXP beginGroupCall, beginGroupFun = LST_EL(s_funs, 2);
    SEXP endGroupCall,   endGroupFun   = LST_EL(s_funs, 3);
    SEXP newRecordCall,  newRecordFun  = LST_EL(s_funs, 4);
    int        invoke_beginGroup = (GET_LENGTH(beginGroupFun)>0);
    int        invoke_endGroup   = (GET_LENGTH(endGroupFun)>0);
    int        invoke_newRecord  = (GET_LENGTH(newRecordFun)>0);

    row = NULL;
    beginGroupCall = R_NilValue;    /* -Wall */
    if(invoke_beginGroup){
      PROTECT(beginGroupCall=lang2(beginGroupFun, R_NilValue));
      ++np;
    }
    endGroupCall = R_NilValue;    /* -Wall */
    if(invoke_endGroup){
      /* TODO: append list(...) to the call object */
      PROTECT(endGroupCall = lang4(endGroupFun, R_NilValue,
        R_NilValue, R_NilValue));
      ++np;
    }
    newRecordCall = R_NilValue;    /* -Wall */
      if(invoke_newRecord){
        PROTECT(newRecordCall = lang2(newRecordFun, R_NilValue));
        ++np;
      }

      result = RS_DBI_getResultSet(rsHandle);
      flds = result->fields;
      if(!flds)
        error("corrupt resultSet, missing fieldDescription");
      num_fields = flds->num_fields;
      fld_Sclass = flds->Sclass;
      PROTECT(data = NEW_LIST((int) num_fields));     /* buffer records */
      PROTECT(cur_rec = NEW_LIST((int) num_fields));  /* current record */
      np += 2;

      RS_DBI_allocOutput(cur_rec, flds, (int) 1, 0);
      make_data_frame(cur_rec);

      num_rec = INT_EL(s_batch_size, 0);     /* this is num of rec per group! */
      max_rec = INT_EL(s_max_rec,0);         /* max rec **per group**         */
      num_groups = num_rec;
      PROTECT(out_list = NEW_LIST(num_groups));
      PROTECT(group_names = NEW_CHARACTER(num_groups));
      np += 2;

      /* set conversion for group names */

      if(result->rowCount==0){
        event = BEGIN;
        /* here we could invoke the begin function*/
      }

      /* actual fetching.... */

      my_result = (MYSQL_RES *) result->drvResultSet;
      completed = (int) 0;

      total_records = 0;
      expand = 0;                  /* expand or init each data vector? */
      i = 0;                       /* index into row number **within** groups */
      while(1){

        if(i==0 || i==num_rec){         /* BEGIN, EXTEND_DATA, BEGIN_GROUP */

      /* reset num_rec upon a new group, double it if needs to expand */
      num_rec = (i==0) ? INT_EL(s_batch_size, 0) : 2*num_rec;
          if(i<max_rec)
            RS_DBI_allocOutput(data, flds, num_rec, expand++);
          else
            break;       /* ok, no more fetching for now (pending group?) */
        }

        if(!pushed_back)
          row = mysql_fetch_row(my_result);

        if(row==NULL){    /* either we finish or we encounter an error */
      unsigned int  err_no;     /* TODO: move block to check_groupEvents */
      RS_DBI_connection   *con;
      con = RS_DBI_getConnection(rsHandle);
      err_no = mysql_errno((MYSQL *) con->drvConnection);
      completed = (int) (err_no ? -1 : 1);
      break;
        }

        if(!pushed_back){                      /* recompute fields lengths? */
      lens = mysql_fetch_lengths(my_result);  /* lengths for each field */
      ++total_records;
        }

        /* coerce each entry row[j] to an R/S type according to its Sclass.
        * TODO:  converter functions are badly needed.
        */
        for(j = 0; j < num_fields; j++){

          null_item = (row[j] == NULL);

          switch((int)fld_Sclass[j]){

          case INTSXP:
            if(null_item)
              NA_SET(&(LST_INT_EL(data,j,i)), INTSXP);
            else
              LST_INT_EL(data,j,i) = atol(row[j]);
            LST_INT_EL(cur_rec,j,0) = LST_INT_EL(data,j,i);
            break;

          case STRSXP:
            /* BUG: I need to verify that a TEXT field (which is stored as
            * a BLOB by MySQL!) is indeed char and not a true
            * Binary obj (MySQL does not truly distinguish them). This
            * test is very gross.
            */
            if(null_item)
              SET_LST_CHR_EL(data,j,i,NA_STRING);
            else {
              if((size_t) lens[j] != strlen(row[j])){
                warning("internal error: row %ld field %ld truncated", i, j);
              }
              SET_LST_CHR_EL(data,j,i,mkChar(row[j]));
            }
            SET_LST_CHR_EL(cur_rec, j, 0, mkChar(LST_CHR_EL(data,j,i)));
            break;

          case REALSXP:
            if(null_item)
              NA_SET(&(LST_NUM_EL(data,j,i)), REALSXP);
            else
              LST_NUM_EL(data,j,i) = (double) atof(row[j]);
            LST_NUM_EL(cur_rec,j,0) = LST_NUM_EL(data,j,i);
            break;

          default:  /* error, but we'll try the field as character (!)*/
            if(null_item)
              SET_LST_CHR_EL(data,j,i, NA_STRING);
            else {
              warning("unrecognized field type %d in column %d", fld_Sclass[j], j);
              SET_LST_CHR_EL(data,j,i,mkChar(row[j]));
            }
            SET_LST_CHR_EL(cur_rec,j,0, mkChar(LST_CHR_EL(data,j,i)));
            break;
          }
        }

        if(!pushed_back){
          if(invoke_newRecord)
            RS_DBI_invokeNewRecord(newRecordCall, cur_rec, rho);
        }
        else {
          pushed_back = FALSE;
        }

        /* We just finished processing the new record, now we check
        * for some events (in addition to NEW_RECORD, of course).
        */
        event = check_groupEvents(data, fld_Sclass, i, group_field);

        if(BEGIN_GROUP & event){

          if(ngroup==num_groups){                  /* exhausted output list? */
        num_groups = 2 * num_groups;
            PROTECT(SET_LENGTH(out_list, num_groups));
            PROTECT(SET_LENGTH(group_names, num_groups));
            np += 2;
          }

          if(invoke_beginGroup)
            RS_DBI_invokeBeginGroup(
              beginGroupCall, CHR_EL(group_names, ngroup), rho);
        }

        if(END_GROUP & event){

          add_group(group_names, data, fld_Sclass, group_field, ngroup, i-1);

          RS_DBI_allocOutput(data, flds, i, expand++);
          make_data_frame(data);

          val = RS_DBI_invokeEndGroup(endGroupCall, data,
            CHR_EL(group_names, ngroup), rho);
          SET_ELEMENT(out_list, ngroup, val);

          /* set length of data to zero to force initialization
          * for next group
          */
          RS_DBI_allocOutput(data, flds, (int) 0, (int) 1);
          i = 0;                                  /* flush */
          ++ngroup;
          pushed_back = TRUE;
          continue;
        }

        i++;
      }

      /* we fetched all the rows we needed/could; compute actual number of
      * records fetched.
      * TODO: What should we return in the case of partial groups???
      */
      if(completed < 0)
        warning("error while fetching rows");
      else if(completed)
        event = (END_GROUP|END);
      else
        event = PARTIAL_GROUP;

      /* wrap up last group */
      if((END_GROUP & event) || (PARTIAL_GROUP & event)){

        add_group(group_names, data, fld_Sclass, group_field, ngroup, i-i);

        if(i<num_rec){
          RS_DBI_allocOutput(data, flds, i, expand++);
          make_data_frame(data);
        }
        if(invoke_endGroup){
          val = RS_DBI_invokeEndGroup(endGroupCall, data,
            CHR_EL(group_names, ngroup), rho);
          SET_ELEMENT(out_list, ngroup++, val);
        }
        if(PARTIAL_GROUP & event){
          char buf[512];
          (void) strcpy(buf, "exhausted the pre-allocated storage. The last ");
          (void) strcat(buf, "output group was computed with partial data. ");
          (void) strcat(buf, "The remaining data were left un-read in the ");
          (void) strcat(buf, "result set.");
          warning(buf);
        }
      }

      /* set the correct length of output list */
      if(GET_LENGTH(out_list) != ngroup){
        PROTECT(SET_LENGTH(out_list, ngroup));
        PROTECT(SET_LENGTH(group_names, ngroup));
        np += 2;
      }

      result->rowCount += total_records;
      result->completed = (int) completed;

      SET_NAMES(out_list, group_names);       /* do I need to PROTECT? */

      UNPROTECT(np);
      return out_list;
    }

unsigned int
  check_groupEvents(SEXP data, SEXPTYPE fld_Sclass[], int irow, int jcol)
  {
    if(irow==0) /* Begin */
      return (BEGIN|BEGIN_GROUP);

    SEXP col = VECTOR_ELT(data, jcol);

    switch(fld_Sclass[jcol]) {
    case LGLSXP:
      if (LOGICAL(col)[irow] == LOGICAL(col)[irow - 1])
        return (END_GROUP|BEGIN_GROUP);
      break;
    case INTSXP:
      if (INTEGER(col)[irow] == INTEGER(col)[irow - 1])
        return (END_GROUP|BEGIN_GROUP);
      break;
    case REALSXP:
      if (REAL(col)[irow] == REAL(col)[irow - 1])
        return (END_GROUP|BEGIN_GROUP);
      break;
    case STRSXP:
      if (STRING_ELT(col, irow) == STRING_ELT(col, irow - 1))
        return (END_GROUP|BEGIN_GROUP);
      break;
    default:
      error("un-regongnized R/S data type %d", fld_Sclass[jcol]);
    break;
    }

    return NEW_RECORD;
  }

/* append current group (as character) to the vector of group names */
void
  add_group(SEXP group_names, SEXP data,
    SEXPTYPE *fld_Sclass, int group_field, int ngroup, int i)
  {
    char  buff[1024];

    SEXP col = VECTOR_ELT(data, group_field);

    switch((int) fld_Sclass[group_field]){

    case LGLSXP:
      (void) snprintf(buff, 1024, "%ld", (long) LOGICAL(col)[i]);
      break;
    case INTSXP:
      (void) snprintf(buff, 1024, "%ld", (long) INTEGER(col)[i]);
      break;
    case REALSXP:
      (void) snprintf(buff, 1024, "%f", (double) REAL(col)[i]);
      break;
    case STRSXP:
      strcpy(buff, CHAR(STRING_ELT(col, i)));
      break;
    default:
      error("unrecognized R/S type for group");
    break;
    }

    SET_CHR_EL(group_names, ngroup, mkChar(buff));
    return;
  }

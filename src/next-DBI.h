#ifndef _RS_DBI_H
#define _RS_DBI_H 1
/*  
 *  $Id: RS-DBI.h,v 1.6 2003/11/04 15:57:14 dj Exp dj $
 *
 * Copyright (C) 1999-2003 The Omega Project for Statistical Computing.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

/* The following include file defines a number of C macros that hide
 * differences between R and S (e.g., the macro for type "Sint" expand
 * to "long" in the case of S and to int in the case of R, etc.)
 */

#ifdef __cplusplus
extern "C" {
#endif

#include "S4R.h"

/* Microsoft Visual C++ uses int _getpid()  */
#ifdef MSVC
#include <process.h>
#define getpid _getpid
#define pid_t int
#else           
#include <unistd.h>
#endif

pid_t getpid(); 

/* We now define the following structures:
 * RS_DBI_drvManager -- driver manager (e.g., ROracle, RMySQL, ...)
 * RS_DBI_manager    -- connection manger (handles RS-DBI connections)
 * RS_DBI_connection -- RS-DBI connection (handles result sets)
 * RS_DBI_resultSet  -- result sets
 * RS_DBI_fields     -- metadata about individual fields
 * corresponding to dbManager, dbConnection,
 * dbResultSet, and list of field descriptions.
 *
 * In R/S a dbObject is a foreign reference consisting of a vector
 * of pid plus 1, 2 or 3 integers.  In the C implementation we use these 
 * R/S vectors as handles (we could have use pointers).
 */
typedef enum enum_dbi_exception {
  RS_DBI_MESSAGE,
  RS_DBI_WARNING,
  RS_DBI_ERROR,
  RS_DBI_TERMINATE
} DBI_EXCEPTION;

/* dbObject handles are simple S/R integer vectors of 1, 2, or 3 integers
 * the *_ID macros extract the appropriate scalar.
 */

#define Mgr_Handle s_object
#define Con_Handle s_object
#define Res_Handle s_object
#define Db_Handle  s_object       /* refers to any one of the above */

#define GET_HANDLE_TYPE(dbHandle) ((int) GET_LENGTH(dbHandle)-1)

typedef enum enum_handle_type {
  MGR_HANDLE_TYPE = 1,     /* dbManager handle */
  CON_HANDLE_TYPE = 2,     /* dbConnection handle */
  RES_HANDLE_TYPE = 3      /* dbResult handle */
} HANDLE_TYPE; 

#define PID(handle)    INT_EL((handle),0)  /* process id */
#define MGR_ID(handle) INT_EL((handle),1)  /* the actual scalar mgr id */
#define CON_ID(handle) INT_EL((handle),2)  
#define RES_ID(handle) INT_EL((handle),3)

/* First, the following fully describes the field output by a select
 * (or select-like) statement, and the mappings from the internal
 * database types to S classes.  This structure contains the info we need
 * to build the R/S list (or data.frame) that will receive the SQL
 * output.  It still needs some work to handle arbitrty BLOB's (namely
 * methods to map BLOBs into user-defined S objects).
 * Each element is an array of num_fields, this flds->Sclass[3] stores
 * the S class for the 4th output fields.
 */
typedef struct st_sdbi_fields {
  int num_fields;
  char  **name;         /* DBMS field names */
  Sint  *type;          /* DBMS internal types */
  Sint  *length;        /* DBMS lengths in bytes */
  Sint  *precision;     /* DBMS num of digits for numeric types */
  Sint  *scale;         /* DBMS num of decimals for numeric types */
  Sint  *nullOk;        /* DBMS indicator for DBMS'  NULL type */
  Sint  *isVarLength;   /* DBMS variable-length char type */
  Stype *Sclass;        /* R/S class (type) -- may be overriden */
  /* TODO: Need a table of fun pointers to converters */
} RS_DBI_fields;

typedef struct st_sdbi_exception {
  DBI_EXCEPTION  exceptionType; /* one of RS_DBI_WARN, RS_RBI_ERROR, etc */
  int  errorNum;            /* SQL error number (possibly driver-dependent*/
  char *errorMsg;           /* SQL error message */
} RS_DBI_exception;

/* The RS-DBI resultSet consists of a pointer to the actual DBMS 
 * resultSet (e.g., MySQL, Oracle) possibly NULL,  plus the fields 
 * defined by the RS-DBI implementation. 
 */
typedef struct st_sdbi_resultset {
  RS_DBI_connection *con;
  void  *drvResultSet;   /* the actual (driver's) cursor/result set */
  void  *drvData;        /* a pointer to driver-specific data */
  Sint  isSelect;        /* boolean for testing SELECTs */
  char  *statement;      /* SQL statement */
  Sint  rowsAffected;    /* used by non-SELECT statements */
  Sint  rowCount;        /* rows fetched so far (SELECT-types)*/
  Sint  completed;       /* have we fetched all rows? */
  RS_DBI_fields *fields;
} RS_DBI_resultSet;

/* A dbConnection consists of a pointer to the actual implementation
 * (MySQL, Oracle, etc.) connection plus a resultSet and other
 * goodies used by the RS-DBI implementation.
 * The connection parameters (user, password, database name, etc.) are
 * defined by the actual driver -- we just set aside a void pointer.
 */
typedef struct st_sdbi_connection {
  RS_DBI_manger *mgr;
  void  *conParams;      /* pointer to connection params (host, user, etc)*/
  void  *drvConnection;  /* pointer to the actual DBMS connection struct*/
  void  *drvData;        /* to be used at will by individual drivers */
  RS_DBI_resultSet  **resultSets;    /* vector to result set ptrs  */
  Sint   length;                     /* max num of concurrent resultSets */
  Sint   counter;                    /* total number of queries */
  RS_DBI_exception *exception;
} RS_DBI_connection;

/* connection manager */
typedef struct st_sdbi_manager {
  RS_DBI_drvManager  *drvManager;
  char *drvName;                    /* what driver are we implementing?*/
  void *drvData;                    /* to be used by the drv implementation*/
  RS_DBI_connection **connections;  /* list of dbConnections */
  Sint length;                      /* max num of concurrent connections */
  Sint counter;                     /* num of connections handled so far*/
  Sint fetch_default_rec;           /* default num of records per fetch */
  RS_DBI_exception *exception;    
} RS_DBI_manager;

/* driver manager */
typedef struct st_sdbi_drivers {
   RS_DBI_manager **managers;
   Sint    length;                  /* max num of concurrent drivers */
   Sint    counter;                 /* num of drivers handled so far */
} RS_DBI_drvManager;

/* All RS_DBI functions and their signatures */

/* Note: the following alloc functions allocate the space for the
 * corresponding manager, connection, resultSet; they all 
 * return handles.  All DBI functions (free/get/etc) use the handle 
 * to work with the various dbObjects.
 */
Mgr_Handle     *RS_DBI_allocManager(const char *drvName, Sint max_con, 
				    Sint fetch_default_rec, 
				    Sint force_realloc);
void            RS_DBI_freeManager(Mgr_Handle *mgrHandle);
RS_DBI_manager *RS_DBI_getManager(Db_Handle *handle);
Mgr_Handle     *RS_DBI_asMgrHandle(Sint pid, Sint mgrId);
s_object       *RS_DBI_managerInfo(Mgr_Handle *mgrHandle);

/* dbConnection */
Con_Handle        *RS_DBI_allocConnection(Mgr_Handle *mgrHandle, 
					  Sint max_res);
void               RS_DBI_freeConnection(Con_Handle *conHandle);
RS_DBI_connection *RS_DBI_getConnection(Db_Handle *handle);
Con_Handle        *RS_DBI_asConHandle(Sint pid, Sint mgrId, Sint conId);
s_object          *RS_DBI_connectionInfo(Con_Handle *con_Handle);

/* dbResultSet */
Res_Handle        *RS_DBI_allocResultSet(Con_Handle *conHandle);
void               RS_DBI_freeResultSet(Res_Handle *rsHandle);
RS_DBI_resultSet  *RS_DBI_getResultSet(Res_Handle *rsHandle);
Res_Handle        *RS_DBI_asResHandle(Sint pid, Sint mgrId, Sint conId, Sint resId);
s_object          *RS_DBI_resultSetInfo(Res_Handle *rsHandle);

/* utility funs */
s_object *RS_DBI_validHandle(Db_Handle *handle); /* callable from S/R */
int       is_validHandle(Db_Handle *handle, HANDLE_TYPE handleType);

/* a simple object database (mapping table) -- it uses simple linear 
 * search (we don't expect to have more than a handful of simultaneous 
 * connections and/or resultSets. If this is not the case, we could
 * use a hash table, but I doubt it's worth it (famous last words!).
 * These are used for storing/retrieving object ids, such as
 * connection ids from the manager object, and resultSet ids from a 
 * connection object;  of course, this is transparent to the various
 * drivers -- they should deal with handles exclusively.
 */
Sint  RS_DBI_newEntry(Sint *table, Sint length);
Sint  RS_DBI_lookupEntry(Sint *table, Sint length, Sint obj_id);
Sint  RS_DBI_listEntries(Sint *table, Sint length, Sint *entries);
void  RS_DBI_freeEntry(Sint *table, Sint indx);

/* description of the fields in a result set */
RS_DBI_fields *RS_DBI_allocFields(int num_fields);
s_object      *RS_DBI_getFieldDescriptions(RS_DBI_fields *flds);
void           RS_DBI_freeFields(RS_DBI_fields *flds);

/* we (re)allocate the actual output list in here (with the help of
 * RS_DBI_fields).  This should be some kind of R/S "relation"
 * table, not a dataframe nor a list.
 */
void  RS_DBI_allocOutput(s_object *output, 
			RS_DBI_fields *flds,
			Sint num_rec,
			Sint expand);
void RS_DBI_makeDataFrame(s_object *data);

/* TODO: We need to elevate RS_DBI_errorMessage to either
 * dbManager and/or dbConnection methods.  I still need to 
 * go back and re-code the error-handling throughout, darn!
 */
void  RS_DBI_errorMessage(char *msg, DBI_EXCEPTION exceptionType);
void  RS_DBI_setException(Db_Handle *handle, 
			  DBI_EXCEPTION exceptionType,
			  int errorNum, 
			  const char *errorMsg);
/* utility funs (copy strings, convert from R/S types to string, etc.*/
char     *RS_DBI_copyString(const char *str);
char     *RS_DBI_nCopyString(const char *str, size_t len, int del_blanks);

/* We now define a generic data type name-Id mapping struct
 * and initialize the RS_dataTypeTable[].  Each driver could
 * define similar table for generating friendly type names
 */
struct data_types {
    char *typeName;
    Sint typeId;
};

/* return the primitive type name for a primitive type id */
char     *RS_DBI_getTypeName(Sint typeCode, const struct data_types table[]);
/* same, but callable from S/R and vectorized */
s_object *RS_DBI_SclassNames(s_object *types);  

s_object *RS_DBI_createNamedList(char  **names, 
				 Stype *types,
				 Sint  *lengths,
				 Sint  n);
s_object *RS_DBI_copyFields(RS_DBI_fields *flds);

void RS_na_set(void *ptr, Stype type);
int  RS_is_na(void *ptr, Stype type);
extern const struct data_types RS_dataTypeTable[];

/* we now creates table of drivers, and map driver names 
 * to TCP port number.
*/
#define RS_DBI_MGR_DB2         0001  /* those < 10 are unknown */
#define RS_DBI_MGR_INFORMIX    0002
#define RS_DBI_MGR_INGRES      0003
#define RS_DBI_MGR_MSQL        1114
#define RS_DBI_MGR_MSSQLSERVER 1433
#define RS_DBI_MGR_MYSQL       3306
#define RS_DBI_MGR_ODBC        1000   /* doesn't use any, use 1000  */
#define RS_DBI_MGR_ORACLE      1521
#define RS_DBI_MGR_POSTGRESQL  5432
#define RS_DBI_MGR_SAPDB       0005
#define RS_DBI_MGR_SQLITE      80
#define RS_DBI_MGR_SYBASE      0006

struct drv_table {
    char *drvName;         /* arbitrary name */
    Sint  drvId;           /* unique positive id, say, TCP port number */
    RS_DBI_manager *mgr;   /* the actual manager */
};

/* Add to this table new drivers.
 * TODO: allow drivers to register at runtime, allow users to query
 *       list of available drivers.
*/
static struct drv_table RS_DBI_driverTable[] = {
  {"Oracle",  RS_DBI_MGR_ORACLE, (RS_DBI_manager *) NULL},
  {"MySQL",   RS_DBI_MGR_MYSQL,  (RS_DBI_manager *) NULL},
  {"SQLite",  RS_DBI_MGR_SQLITE, (RS_DBI_manager *) NULL},
  {(char *) 0, -1, 0}
};

#ifdef __cplusplus 
}
#endif
#endif   /* _RS_DBI_H */

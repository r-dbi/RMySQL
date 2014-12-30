#ifndef _RS_MYSQL_H
#define _RS_MYSQL_H 1
/*
 * Copyright (C) 1999-2002 The Omega Project for Statistical Computing.
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

#ifdef _cplusplus
extern  "C" {
#endif

#include "S4R.h"

#if defined WIN32
# include <windows.h>
# undef ERROR
#endif

#include <mysql.h>
#include <mysql_version.h>
#include <mysql_com.h>
#include <string.h>

#include <ctype.h> /* for isalpha */

/* We now define 4 important data structures:
* MySQLDriver, RS_DBI_connection, RS_DBI_resultSet, and
* RS_DBI_fields, corresponding to dbManager, dbConnection,
* dbResultSet, and list of field descriptions.
*/
/* In R/S a dbObject is a foreign reference consisting of a vector
* of 1, 2 or 3 integers.  In the C implementation we use these
* R/S vectors as handles (we could have used pointers).
*/
typedef enum enum_dbi_exception {
  RS_DBI_MESSAGE,
  RS_DBI_WARNING,
  RS_DBI_ERROR,
  RS_DBI_TERMINATE
} DBI_EXCEPTION;

/* The integer value for the following enum's needs to equal
* GET_LENGTH(handle) for the various handles.
*/
typedef enum enum_handle_type {
  MGR_HANDLE_TYPE = 1,     /* dbManager handle */
CON_HANDLE_TYPE = 2,     /* dbConnection handle */
RES_HANDLE_TYPE = 3      /* dbResult handle */
} HANDLE_TYPE;

#define MGR_ID(handle) INTEGER(handle)[0]  /* the actual scalar mgr id */
#define CON_ID(handle) INTEGER(handle)[1]
#define RES_ID(handle) INTEGER(handle)[2]

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
int  *type;          /* DBMS internal types */
int  *length;        /* DBMS lengths in bytes */
int  *precision;     /* DBMS num of digits for numeric types */
int  *scale;         /* DBMS num of decimals for numeric types */
int  *nullOk;        /* DBMS indicator for DBMS'  NULL type */
int  *isVarLength;   /* DBMS variable-length char type */
SEXPTYPE *Sclass;        /* R/S class (type) -- may be overriden */
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
  void  *drvResultSet;   /* the actual (driver's) cursor/result set */
int  managerId;       /* the 3 *Id's are used for   */
int  connectionId;    /* validating stuff coming from S */
int  resultSetId;
int  isSelect;        /* boolean for testing SELECTs */
char  *statement;      /* SQL statement */
int  rowsAffected;    /* used by non-SELECT statements */
int  rowCount;        /* rows fetched so far (SELECT-types)*/
int  completed;       /* have we fetched all rows? */
RS_DBI_fields *fields;
} RS_DBI_resultSet;

/* A dbConnection consists of a pointer to the actual implementation
* (MySQL, Oracle, etc.) connection plus a resultSet and other
* goodies used by the RS-DBI implementation.
* The connection parameters (user, password, database name, etc.) are
* defined by the actual driver -- we just set aside a void pointer.
*/

typedef struct st_sdbi_connection {
  void  *conParams;      /* pointer to connection params (host, user, etc)*/
void  *drvConnection;  /* pointer to the actual DBMS connection struct*/
RS_DBI_resultSet  **resultSets;    /* vector to result set ptrs  */
int   *resultSetIds;
int   length;                     /* max num of concurrent resultSets */
int   num_res;                    /* num of open resultSets */
int   counter;                    /* total number of queries */
int   managerId;
int   connectionId;
RS_DBI_exception *exception;
} RS_DBI_connection;

/* dbManager */
typedef struct MySQLDriver {
RS_DBI_connection **connections;  /* list of dbConnections */
int *connectionIds;              /* array of connectionIds */
int length;                      /* max num of concurrent connections */
int num_con;                     /* num of opened connections */
int counter;                     /* num of connections handled so far*/
int fetch_default_rec;           /* default num of records per fetch */
int managerId;                   /* typically, process id */
RS_DBI_exception *exception;
} MySQLDriver;

/* All RS_DBI functions and their signatures */

/* Note: the following alloc functions allocate the space for the
* corresponding manager, connection, resultSet; they all
* return handles.  All DBI functions (free/get/etc) use the handle
* to work with the various dbObjects.
*/
void            RS_DBI_freeManager(SEXP mgrHandle);
SEXP RS_DBI_asMgrHandle(int pid);
SEXP RS_DBI_managerInfo(SEXP mgrHandle);

/* dbConnection */
SEXP RS_DBI_allocConnection(SEXP mgrHandle,
  int max_res);
void               RS_DBI_freeConnection(SEXP conHandle);
RS_DBI_connection *RS_DBI_getConnection(SEXP handle);
SEXP RS_DBI_asConHandle(int mgrId, int conId);
SEXP RS_DBI_connectionInfo(SEXP con_Handle);

/* dbResultSet */
SEXP RS_DBI_allocResultSet(SEXP conHandle);
void               RS_DBI_freeResultSet(SEXP rsHandle);
RS_DBI_resultSet  *RS_DBI_getResultSet(SEXP rsHandle);
SEXP RS_DBI_asResHandle(int pid, int conId, int resId);
SEXP RS_DBI_resultSetInfo(SEXP rsHandle);

/* utility funs */
SEXP RS_DBI_validHandle(SEXP handle); /* callable from S/R */
int       is_validHandle(SEXP handle, HANDLE_TYPE handleType);

/* a simple object database (mapping table) -- it uses simple linear
* search (we don't expect to have more than a handful of simultaneous
* connections and/or resultSets. If this is not the case, we could
* use a hash table, but I doubt it's worth it (famous last words!).
* These are used for storing/retrieving object ids, such as
* connection ids from the manager object, and resultSet ids from a
* connection object;  of course, this is transparent to the various
* drivers -- they should deal with handles exclusively.
*/
int  RS_DBI_newEntry(int *table, int length);
int  RS_DBI_lookup(int *table, int length, int obj_id);
int  RS_DBI_listEntries(int *table, int length, int *entries);
void  RS_DBI_freeEntry(int *table, int indx);

/* description of the fields in a result set */
RS_DBI_fields *RS_DBI_allocFields(int num_fields);
SEXP RS_DBI_getFieldDescriptions(RS_DBI_fields *flds);
void           RS_DBI_freeFields(RS_DBI_fields *flds);

/* we (re)allocate the actual output list in here (with the help of
* RS_DBI_fields).  This should be some kind of R/S "relation"
* table, not a dataframe nor a list.
*/
void  RS_DBI_allocOutput(SEXP output,
  RS_DBI_fields *flds,
  int num_rec,
  int expand);
void RS_DBI_makeDataFrame(SEXP data);

void  RS_DBI_errorMessage(char *msg, DBI_EXCEPTION exceptionType);
/* utility funs (copy strings, convert from R/S types to string, etc.*/
char     *RS_DBI_copyString(const char *str);
char     *RS_DBI_nCopyString(const char *str, size_t len, int del_blanks);

/* We now define a generic data type name-Id mapping struct
* and initialize the RS_dataTypeTable[].  Each driver could
* define similar table for generating friendly type names
*/
struct data_types {
  char *typeName;
  int typeId;
};

/* return the primitive type name for a primitive type id */
char     *RS_DBI_getTypeName(int typeCode, const struct data_types table[]);
/* same, but callable from S/R and vectorized */
SEXP RS_DBI_SclassNames(SEXP types);

SEXP RS_DBI_createNamedList(char  **names,
  SEXPTYPE *types,
  int  *lengths,
  int  n);
SEXP RS_DBI_copyFields(RS_DBI_fields *flds);

void RS_na_set(void *ptr, SEXPTYPE type);
int  RS_is_na(void *ptr, SEXPTYPE type);
extern const struct data_types RS_dataTypeTable[];

/* Note that MySQL client/server buffers are limited to 16MB and 1MB,
 * respectively (MySQL 4.1.1-alpha manual).  So plan accordingly!
 */

/* MySQL connection parameters struct, allocating and freeing
 * methods. See mysql_real_connect() for details on the params
 * themselves.
 */
typedef struct st_sdbi_conParams {
    char *dbname;
    char *username;
    char *password;
    char *host;
    char *unix_socket;
    unsigned int  port;
    unsigned int  client_flag;
	char *groups;
	char *default_file;
} RS_MySQL_conParams;

RS_MySQL_conParams *RS_MySQL_allocConParams(void);
RS_MySQL_conParams *RS_MySQL_cloneConParams(RS_MySQL_conParams *conParams);
void                RS_MySQL_freeConParams(RS_MySQL_conParams *conParams);

/* The following functions are the S/R entry points into the C implementation
 * (i.e., these are the only ones visible from R/S) we use the prefix
 * "RS_MySQL" in function names to denote this.
 * These functions are  built on top of the underlying RS_DBI manager,
 * connection, and resultsets structures and functions (see RS-DBI.h).
 */

/* dbManager */
MySQLDriver* rmysql_driver();
SEXP rmysql_driver_init(SEXP max_con_, SEXP fetch_default_rec_);

/* dbConnection */
SEXP RS_MySQL_newConnection(SEXP mgrHandle,
  SEXP s_dbname,
  SEXP s_username,
  SEXP s_password,
  SEXP s_myhost,
  SEXP s_unix_socket,
  SEXP s_port,
  SEXP s_client_flag,
  SEXP s_groups,
  SEXP s_default_file);
SEXP RS_MySQL_createConnection(SEXP mgrHandle, RS_MySQL_conParams *conParams);
SEXP RS_MySQL_cloneConnection(SEXP conHandle);
SEXP RS_MySQL_closeConnection(SEXP conHandle);
SEXP rmysql_exception_info(SEXP conHandle);

/* dbResultSet */
SEXP RS_MySQL_exec(SEXP conHandle, SEXP statement);
SEXP RS_MySQL_fetch(SEXP rsHandle, SEXP max_rec);
SEXP RS_MySQL_closeResultSet(SEXP rsHandle);

/* Multiple result set function (as of MySQL Version 4.1) */
SEXP RS_MySQL_nextResultSet(SEXP conHandle);
SEXP RS_MySQL_moreResultSets(SEXP conHandle);  /* boolean */

SEXP RS_MySQL_validHandle(SEXP handle);      /* boolean */

RS_DBI_fields *RS_MySQL_createDataMappings(SEXP resHandle);
/* the following funs return named lists with meta-data for
 * the manager, connections, and  result sets, respectively.
 */
SEXP RS_MySQL_managerInfo(SEXP mgrHandle);
SEXP RS_MySQL_connectionInfo(SEXP conHandle);
SEXP RS_MySQL_resultSetInfo(SEXP rsHandle);

SEXP RS_MySQL_escapeStrings(SEXP conHandle, SEXP statement);

SEXP RS_MySQL_versionId(void);

/* the following type names are from "mysql_com.h" */
static struct data_types RS_MySQL_dataTypes[] = {
    { "FIELD_TYPE_DECIMAL",    FIELD_TYPE_DECIMAL},
    { "FIELD_TYPE_TINY",       FIELD_TYPE_TINY},
    { "FIELD_TYPE_SHORT",      FIELD_TYPE_SHORT},
    { "FIELD_TYPE_LONG",       FIELD_TYPE_LONG},
    { "FIELD_TYPE_FLOAT",      FIELD_TYPE_FLOAT},
    { "FIELD_TYPE_DOUBLE",     FIELD_TYPE_DOUBLE},
    { "FIELD_TYPE_NULL",       FIELD_TYPE_NULL},
    { "FIELD_TYPE_TIMESTAMP",  FIELD_TYPE_TIMESTAMP},
    { "FIELD_TYPE_LONGLONG",   FIELD_TYPE_LONGLONG},
    { "FIELD_TYPE_INT24",      FIELD_TYPE_INT24},
    { "FIELD_TYPE_DATE",       FIELD_TYPE_DATE},
    { "FIELD_TYPE_TIME",       FIELD_TYPE_TIME},
    { "FIELD_TYPE_DATETIME",   FIELD_TYPE_DATETIME},
    { "FIELD_TYPE_YEAR",       FIELD_TYPE_YEAR},
    { "FIELD_TYPE_NEWDATE",    FIELD_TYPE_NEWDATE},
    { "FIELD_TYPE_ENUM",       FIELD_TYPE_ENUM},
    { "FIELD_TYPE_SET",        FIELD_TYPE_SET},
    { "FIELD_TYPE_TINY_BLOB",  FIELD_TYPE_TINY_BLOB},
    { "FIELD_TYPE_MEDIUM_BLOB",FIELD_TYPE_MEDIUM_BLOB},
    { "FIELD_TYPE_LONG_BLOB",  FIELD_TYPE_LONG_BLOB},
    { "FIELD_TYPE_BLOB",       FIELD_TYPE_BLOB},
    { "FIELD_TYPE_VAR_STRING", FIELD_TYPE_VAR_STRING},
    { "FIELD_TYPE_STRING",     FIELD_TYPE_STRING},
    { (char *) 0, -1 }
};

SEXP RS_DBI_copyfields(RS_DBI_fields *flds);

SEXP RS_MySQL_typeNames(SEXP typeIds);
extern const struct data_types RS_dataTypeTable[];

#ifdef _cplusplus
}
#endif

#endif   /* _RS_MYSQL_H */

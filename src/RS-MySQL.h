#ifndef _RS_MYSQL_H
#define _RS_MYSQL_H 1
// Copyright (C) 1999-2002 The Omega Project for Statistical Computing.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307

#ifdef _cplusplus
extern  "C" {
#endif

#if defined WIN32
# include <windows.h>
# undef ERROR
#endif

#include "S4R.h"

#include <mysql.h>
#include <mysql_version.h>
#include <mysql_com.h>
#include <string.h>

// Objects =====================================================================

typedef struct RMySQLFields {
  int num_fields;
  char  **name;         // DBMS field names
  int  *type;           // DBMS internal types
  int  *length;         // DBMS lengths in bytes
  int  *precision;      // DBMS num of digits for numeric types
  int  *scale;          // DBMS num of decimals for numeric types
  int  *nullOk;         // DBMS indicator for DBMS'  NULL type
  int  *isVarLength;    // DBMS variable-length char type
  SEXPTYPE *Sclass;     // R/S class (type) -- may be overriden
} RMySQLFields;

typedef struct st_sdbi_resultset {
  void  *drvResultSet;   // the actual (driver's) cursor/result set
  int  managerId;        // the 3 *Id's are used for
  int  connectionId;     // validating stuff coming from S
  int  resultSetId;
  int  isSelect;         // boolean for testing SELECTs
  char  *statement;      // SQL statement
  int  rowsAffected;     // used by non-SELECT statements
  int  rowCount;         // rows fetched so far (SELECT-types)
  int  completed;        // have we fetched all rows?
  RMySQLFields* fields;
} RS_DBI_resultSet;

typedef struct st_sdbi_connection {
  void  *conParams;                 // pointer to connection params (host, user, etc)
  void  *drvConnection;             // pointer to the actual DBMS connection struct
  RS_DBI_resultSet  **resultSets;   // vector to result set ptrs
  int   *resultSetIds;
  int   length;                     // max num of concurrent resultSets
  int   num_res;                    // num of open resultSets
  int   counter;                    // total number of queries
  int   managerId;
  int   connectionId;
} RS_DBI_connection;

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


// dbManager
typedef struct MySQLDriver {
  RS_DBI_connection **connections; // list of dbConnections
  int *connectionIds;              // array of connectionIds
  int length;                      // max num of concurrent connections
  int num_con;                     // num of opened connections
  int counter;                     // num of connections handled so far
  int fetch_default_rec;           // default num of records per fetch
  int managerId;                   // typically, process id
} MySQLDriver;

// Functions ===================================================================

#define MGR_ID(handle) INTEGER(handle)[0]
#define CON_ID(handle) INTEGER(handle)[1]
#define RES_ID(handle) INTEGER(handle)[2]

// Driver ----------------------------------------------------------------------

MySQLDriver* rmysql_driver();
SEXP rmysql_driver_init(SEXP max_con_, SEXP fetch_default_rec_);
SEXP rmysql_driver_info();

SEXP rmysql_exception_info(SEXP conHandle);

// Connection ------------------------------------------------------------------

SEXP RS_DBI_allocConnection(SEXP mgrHandle, int max_res);
void RS_DBI_freeConnection(SEXP conHandle);
RS_DBI_connection *RS_DBI_getConnection(SEXP handle);
SEXP RS_DBI_asConHandle(int mgrId, int conId);
SEXP RS_DBI_connectionInfo(SEXP con_Handle);
SEXP RS_MySQL_newConnection(SEXP mgrHandle, SEXP s_dbname, SEXP s_username, SEXP s_password, SEXP s_myhost, SEXP s_unix_socket, SEXP s_port, SEXP s_client_flag, SEXP s_groups, SEXP s_default_file);
SEXP RS_MySQL_createConnection(SEXP mgrHandle, RS_MySQL_conParams *conParams);
SEXP RS_MySQL_cloneConnection(SEXP conHandle);
SEXP RS_MySQL_closeConnection(SEXP conHandle);
SEXP RS_MySQL_connectionInfo(SEXP conHandle);

RS_MySQL_conParams* RS_MySQL_allocConParams(void);
RS_MySQL_conParams* RS_MySQL_cloneConParams(RS_MySQL_conParams *conParams);
void RS_MySQL_freeConParams(RS_MySQL_conParams *conParams);

// Result set ------------------------------------------------------------------
SEXP RS_DBI_allocResultSet(SEXP conHandle);
void RS_DBI_freeResultSet(SEXP rsHandle);
RS_DBI_resultSet* RS_DBI_getResultSet(SEXP rsHandle);
SEXP RS_DBI_asResHandle(int pid, int conId, int resId);
SEXP RS_DBI_resultSetInfo(SEXP rsHandle);
SEXP RS_MySQL_exec(SEXP conHandle, SEXP statement);
SEXP RS_MySQL_fetch(SEXP rsHandle, SEXP max_rec);
SEXP RS_MySQL_closeResultSet(SEXP rsHandle);
SEXP RS_MySQL_nextResultSet(SEXP conHandle);
SEXP RS_MySQL_moreResultSets(SEXP conHandle);
SEXP RS_MySQL_resultSetInfo(SEXP rsHandle);

// Fields ----------------------------------------------------------------------
void rmysql_fields_free(RMySQLFields* flds);
void RS_DBI_allocOutput(SEXP output, RMySQLFields* flds, int num_rec, int expand);
void make_data_frame(SEXP data);
SEXP RS_DBI_copyFields(RMySQLFields* flds);
RMySQLFields* RS_MySQL_createDataMappings(SEXP resHandle);

// Utilities -------------------------------------------------------------------
char *RS_DBI_copyString(const char* str);
SEXP RS_DBI_createNamedList(char** names, SEXPTYPE* types, int* lengths, int n);
void RS_na_set(void* ptr, SEXPTYPE type);
int RS_is_na(void* ptr, SEXPTYPE type);
SEXP rmysql_escape_strings(SEXP conHandle, SEXP statement);

// Object database -------------------------------------------------------------
// Simple object database used for storing all connections for a driver,
// and all result sets for a connection.
int RS_DBI_newEntry(int* table, int length);
int RS_DBI_lookup(int* table, int length, int obj_id);
int RS_DBI_listEntries(int* table, int length, int* entries);
void RS_DBI_freeEntry(int* table, int indx);

#ifdef _cplusplus
}
#endif

#endif   // _RS_MYSQL_H

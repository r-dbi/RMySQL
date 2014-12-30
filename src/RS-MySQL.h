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

#if defined WIN32
# include <windows.h>
# undef ERROR
#endif

#include <mysql.h>
#include <mysql_version.h>
#include <mysql_com.h>
#include <string.h>

#include "RS-DBI.h"

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
SEXP RS_MySQL_init(SEXP config_params, SEXP reload);
SEXP    RS_MySQL_close(SEXP mgrHandle);

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
SEXP RS_MySQL_getException(SEXP conHandle);    /* err No, Msg */

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

SEXP RS_MySQL_typeNames(SEXP typeIds);
extern const struct data_types RS_dataTypeTable[];

#ifdef _cplusplus
}
#endif

#endif   /* _RS_MYSQL_H */

#ifndef _RS_MYSQL_H
#define _RS_MYSQL_H 1
/*  
 *  $Id$
 *
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
 * 
 * Note: A handle is just an R/S object (see RS-DBI.h for details), i.e.,
 *       Mgr_Handle, Con_Handle, Res_Handle, Db_Handle are s_object 
 *       (integer vectors, to be precise).
 */
  
/* dbManager */
Mgr_Handle *RS_MySQL_init(s_object *config_params, s_object *reload);
s_object   *RS_MySQL_close(Mgr_Handle *mgrHandle); 

/* dbConnection */
Con_Handle *RS_MySQL_newConnection(Mgr_Handle *mgrHandle, 
    s_object *s_dbname,
    s_object *s_username,
    s_object *s_password,
    s_object *s_myhost,
    s_object *s_unix_socket,
    s_object *s_port,
    s_object *s_client_flag,
    s_object *s_groups,
    s_object *s_default_file);
Con_Handle *RS_MySQL_createConnection(Mgr_Handle *mgrHandle, RS_MySQL_conParams *conParams);
Con_Handle *RS_MySQL_cloneConnection(Con_Handle *conHandle);
s_object   *RS_MySQL_closeConnection(Con_Handle *conHandle);
s_object   *RS_MySQL_getException(Con_Handle *conHandle);    /* err No, Msg */

/* dbResultSet */
Res_Handle *RS_MySQL_exec(Con_Handle *conHandle, s_object *statement);
s_object   *RS_MySQL_fetch(Res_Handle *rsHandle, s_object *max_rec);
s_object   *RS_MySQL_closeResultSet(Res_Handle *rsHandle); 

/* Multiple result set function (as of MySQL Version 4.1) */
Res_Handle *RS_MySQL_nextResultSet(Con_Handle *conHandle);
s_object   *RS_MySQL_moreResultSets(Con_Handle *conHandle);  /* boolean */

s_object   *RS_MySQL_validHandle(Db_Handle *handle);      /* boolean */

RS_DBI_fields *RS_MySQL_createDataMappings(Res_Handle *resHandle);
/* the following funs return named lists with meta-data for 
 * the manager, connections, and  result sets, respectively.
 */
s_object *RS_MySQL_managerInfo(Mgr_Handle *mgrHandle);
s_object *RS_MySQL_connectionInfo(Con_Handle *conHandle);
s_object *RS_MySQL_resultSetInfo(Res_Handle *rsHandle);

s_object *RS_MySQL_escapeStrings(Con_Handle *conHandle, s_object *statement);

s_object *RS_MySQL_versionId(void);

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

s_object *RS_MySQL_typeNames(s_object *typeIds);
extern const struct data_types RS_dataTypeTable[];

#ifdef _cplusplus
}
#endif

#endif   /* _RS_MYSQL_H */

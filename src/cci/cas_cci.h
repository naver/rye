/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors
 *   may be used to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

/*
 * cas_cci.h -
 */

#ifndef	_CAS_CCI_H_
#define	_CAS_CCI_H_

#ifdef __cplusplus
extern "C"
{
#endif

/************************************************************************
 * IMPORTED SYSTEM HEADER FILES						*
 ************************************************************************/
#include <stdlib.h>
#include <sys/types.h>

/************************************************************************
 * IMPORTED OTHER HEADER FILES						*
 ************************************************************************/
#include "cas_error.h"

/************************************************************************
 * EXPORTED DEFINITIONS							*
 ************************************************************************/

#define CCI_GET_RESULT_INFO_TYPE(RES_INFO, INDEX)	\
		(((T_CCI_COL_INFO*) (RES_INFO))[(INDEX) - 1].type)

#define CCI_GET_RESULT_INFO_SCALE(RES_INFO, INDEX)	\
		(((T_CCI_COL_INFO*) (RES_INFO))[(INDEX) - 1].scale)

#define CCI_GET_RESULT_INFO_PRECISION(RES_INFO, INDEX)	\
		(((T_CCI_COL_INFO*) (RES_INFO))[(INDEX) - 1].precision)

#define CCI_GET_RESULT_INFO_NAME(RES_INFO, INDEX)	\
		(((T_CCI_COL_INFO*) (RES_INFO))[(INDEX) - 1].col_name)

#define CCI_GET_RESULT_INFO_ATTR_NAME(RES_INFO, INDEX)	\
		(((T_CCI_COL_INFO*) (RES_INFO))[(INDEX) - 1].real_attr)

#define CCI_GET_RESULT_INFO_CLASS_NAME(RES_INFO, INDEX)	\
		(((T_CCI_COL_INFO*) (RES_INFO))[(INDEX) - 1].class_name)

#define CCI_GET_RESULT_INFO_IS_NON_NULL(RES_INFO, INDEX)	\
		(((T_CCI_COL_INFO*) (RES_INFO))[(INDEX) - 1].is_non_null)

#define CCI_GET_RESULT_INFO_DEFAULT_VALUE(RES_INFO, INDEX)	\
		(((T_CCI_COL_INFO*) (RES_INFO))[(INDEX) - 1].default_value)

#define CCI_GET_RESULT_INFO_IS_UNIQUE_KEY(RES_INFO, INDEX)	\
		(((T_CCI_COL_INFO*) (RES_INFO))[(INDEX) - 1].is_unique_key)

#define CCI_GET_RESULT_INFO_IS_PRIMARY_KEY(RES_INFO, INDEX)	\
		(((T_CCI_COL_INFO*) (RES_INFO))[(INDEX) - 1].is_primary_key)

#define CCI_QUERY_RESULT_RESULT(QR, INDEX)	\
	(((T_CCI_QUERY_RESULT*) (QR))[(INDEX) - 1].result_count)

#define CCI_QUERY_RESULT_ERR_NO(QR, INDEX)	\
	(((T_CCI_QUERY_RESULT*) (QR))[(INDEX) - 1].err_no)

#define CCI_QUERY_RESULT_ERR_MSG(QR, INDEX)	\
	((((T_CCI_QUERY_RESULT*) (QR))[(INDEX) - 1].err_msg) == NULL ? "" : (((T_CCI_QUERY_RESULT*) (QR))[(INDEX) - 1].err_msg))

#define CCI_QUERY_RESULT_STMT_TYPE(QR, INDEX)	\
	(((T_CCI_QUERY_RESULT*) (QR))[(INDEX) - 1].stmt_type)

#define CCI_BIND_PTR			1

/* cci_end_tran type */
#define CCI_TRAN_COMMIT			1
#define CCI_TRAN_ROLLBACK		2

/* cci_preapre flag */
#define CCI_PREPARE_HOLDABLE		0x01
#define CCI_PREPARE_FROM_MIGRATOR       0x02

#define CCI_LOCK_TIMEOUT_INFINITE	-1
#define CCI_LOCK_TIMEOUT_DEFAULT	-2

#define CCI_LOGIN_TIMEOUT_INFINITE      (0)
#define CCI_LOGIN_TIMEOUT_DEFAULT       (30000)

#define SSIZEOF(val) ((ssize_t) sizeof(val))

/* for cci auto_comit mode support */
  typedef enum
  {
    CCI_AUTOCOMMIT_FALSE = 0,
    CCI_AUTOCOMMIT_TRUE
  } CCI_AUTOCOMMIT_MODE;

/************************************************************************
 * EXPORTED TYPE DEFINITIONS						*
 ************************************************************************/

  typedef struct
  {
    int err_code;
    char err_msg[1024];
  } T_CCI_ERROR;

  typedef struct
  {
    int size;
    char *buf;
  } T_CCI_VARBIT;

  typedef struct
  {
    short yr;
    short mon;
    short day;
    short hh;
    short mm;
    short ss;
    short ms;
  } T_CCI_DATETIME;

  typedef struct
  {
    int result_count;
    int stmt_type;
    int err_no;
    char *err_msg;
  } T_CCI_QUERY_RESULT;

  typedef enum
  {
    CCI_TYPE_FIRST = 0,
    CCI_TYPE_UNKNOWN = 0,
    CCI_TYPE_NULL = 0,

    CCI_TYPE_VARCHAR = 1,
    CCI_TYPE_VARBIT = 2,
    CCI_TYPE_NUMERIC = 3,
    CCI_TYPE_INT = 4,
    CCI_TYPE_DOUBLE = 5,
    CCI_TYPE_DATE = 6,
    CCI_TYPE_TIME = 7,
    CCI_TYPE_BIGINT = 8,
    CCI_TYPE_DATETIME = 9,

    CCI_TYPE_LAST = CCI_TYPE_DATETIME
  } T_CCI_TYPE;

  enum
  {
    UNMEASURED_LENGTH = -1
  };

  typedef enum
  {
    CCI_PARAM_FIRST = 1,
    CCI_PARAM_MAX_STRING_LENGTH = 1,
  } T_CCI_DB_PARAM;

  typedef enum
  {
    CCI_SCH_FIRST = 1,
    CCI_SCH_TABLE = 1,
    CCI_SCH_QUERY_SPEC = 2,
    CCI_SCH_COLUMN = 3,
    CCI_SCH_INDEX_INFO = 4,
    CCI_SCH_TABLE_PRIVILEGE = 5,
    CCI_SCH_COLUMN_PRIVILEGE = 6,
    CCI_SCH_PRIMARY_KEY = 7,
    CCI_SCH_LAST = CCI_SCH_PRIMARY_KEY
  } T_CCI_SCH_TYPE;

  typedef enum
  {
    CCI_ER_NO_ERROR = 0,
    CCI_ER_DBMS = -20001,
    CCI_ER_CON_HANDLE = -20002,
    CCI_ER_NO_MORE_MEMORY = -20003,
    CCI_ER_COMMUNICATION = -20004,
    CCI_ER_NO_MORE_DATA = -20005,
    CCI_ER_TRAN_TYPE = -20006,
    CCI_ER_STRING_PARAM = -20007,
    CCI_ER_TYPE_CONVERSION = -20008,
    CCI_ER_BIND_INDEX = -20009,
    CCI_ER_ATYPE = -20010,
    CCI_ER_NOT_BIND = -20011,
    CCI_ER_PARAM_NAME = -20012,
    CCI_ER_COLUMN_INDEX = -20013,
    CCI_ER_SCHEMA_TYPE = -20014,
    CCI_ER_FILE = -20015,
    CCI_ER_CONNECT = -20016,

    CCI_ER_ALLOC_CON_HANDLE = -20017,
    CCI_ER_REQ_HANDLE = -20018,
    CCI_ER_INVALID_CURSOR_POS = -20019,
    CCI_ER_CAS = -20021,
    CCI_ER_HOSTNAME = -20022,

    CCI_ER_BIND_ARRAY_SIZE = -20024,
    CCI_ER_ISOLATION_LEVEL = -20025,

    CCI_ER_SAVEPOINT_CMD = -20028,
    CCI_ER_INVALID_URL = -20030,
    CCI_ER_INVALID_LOB_READ_POS = -20031,

#if 0				/* unused */
    CCI_ER_INVALID_LOB_HANDLE = -20032,
#endif

    CCI_ER_NO_PROPERTY = -20033,

    CCI_ER_PROPERTY_TYPE = -20034,
    CCI_ER_INVALID_PROPERTY_VALUE = CCI_ER_PROPERTY_TYPE,

    CCI_ER_LOGIN_TIMEOUT = -20038,
    CCI_ER_QUERY_TIMEOUT = -20039,

    CCI_ER_RESULT_SET_CLOSED = -20040,

    CCI_ER_INVALID_HOLDABILITY = -20041,

    CCI_ER_INVALID_ARGS = -20043,
    CCI_ER_USED_CONNECTION = -20044,

    CCI_ER_NOT_IMPLEMENTED = -20099,
    CCI_ER_VALUE_OUT_OF_RANGE = -20100,

    CCI_ER_NOT_SHARDING_CONNECTION = -20101,
    CCI_ER_SHARD_NOT_ALLOWED_USER = -20102,

    CCI_ER_ASYNC_LAUNCH_FAIL = -20103,
    CCI_ER_SERVER_RESTARTED = -20104,

    CCI_ER_END = -20200
  } T_CCI_ERROR_CODE;

#if !defined(CAS)
#ifdef DBDEF_HEADER_
  typedef int T_CCI_STMT_TYPE;
#else
  typedef enum
  {
    CCI_STMT_ALTER_CLASS,
    CCI_STMT_ALTER_SERIAL,
    CCI_STMT_COMMIT_WORK,
    CCI_STMT_REGISTER_DATABASE,
    CCI_STMT_CREATE_CLASS,
    CCI_STMT_CREATE_INDEX,
    CCI_STMT_CREATE_TRIGGER,	/* do not delete me; unused */
    CCI_STMT_CREATE_SERIAL,
    CCI_STMT_DROP_DATABASE,
    CCI_STMT_DROP_CLASS,
    CCI_STMT_DROP_INDEX,
    CCI_STMT_DROP_LABEL,
    CCI_STMT_DROP_TRIGGER,	/* do not delete me; unused */
    CCI_STMT_DROP_SERIAL,
    CCI_STMT_EVALUATE,
    CCI_STMT_RENAME_CLASS,
    CCI_STMT_ROLLBACK_WORK,
    CCI_STMT_GRANT,
    CCI_STMT_REVOKE,
    CCI_STMT_STATISTICS,
    CCI_STMT_INSERT,
    CCI_STMT_SELECT,
    CCI_STMT_UPDATE,
    CCI_STMT_DELETE,
    CCI_STMT_CALL,
    CCI_STMT_GET_ISO_LVL,
    CCI_STMT_GET_TIMEOUT,
    CCI_STMT_GET_OPT_LVL,
    CCI_STMT_SET_OPT_LVL,
    CCI_STMT_SCOPE,
    CCI_STMT_GET_TRIGGER,	/* do not delete me; unused */
    CCI_STMT_SET_TRIGGER,	/* do not delete me; unused */
    CCI_STMT_SAVEPOINT,
    CCI_STMT_PREPARE,
    CCI_STMT_ATTACH,
    CCI_STMT_USE,
    CCI_STMT_REMOVE_TRIGGER,	/* do not delete me; unused */
    CCI_STMT_RENAME_TRIGGER,	/* do not delete me; unused */
    CCI_STMT_ON_LDB,
    CCI_STMT_GET_LDB,
    CCI_STMT_SET_LDB,
    CCI_STMT_GET_STATS,
    CCI_STMT_CREATE_USER,
    CCI_STMT_DROP_USER,
    CCI_STMT_ALTER_USER,
    CCI_STMT_SET_SYS_PARAMS,
    CCI_STMT_ALTER_INDEX,

    CCI_STMT_TRUNCATE,		/* do not delete me; unused */
    CCI_STMT_DO,		/* do not delete me; unused */
    CCI_STMT_SELECT_UPDATE,
    CCI_STMT_SET_SESSION_VARIABLES,
    CCI_STMT_DROP_SESSION_VARIABLES,
    CCI_STMT_MERGE,		/* do not delete me; unused */
    CCI_STMT_SET_NAMES,
    CCI_STMT_ALTER_STORED_PROCEDURE_OWNER,

    CCI_MAX_STMT_TYPE
  } T_CCI_STMT_TYPE;

  typedef int T_CCI_CONN;
  typedef int T_CCI_REQ;
  typedef struct PROPERTIES_T T_CCI_PROPERTIES;

#endif
#endif
#define CCI_STMT_UNKNOWN	0x7f

/* for backward compatibility */
#define T_CCI_SQLX_CMD T_CCI_STMT_TYPE

  typedef enum
  {
    CCI_CURSOR_FIRST = 0,
    CCI_CURSOR_CURRENT = 1,
    CCI_CURSOR_LAST = 2
  } T_CCI_CURSOR_POS;

  typedef struct
  {
    T_CCI_TYPE type;
    char is_non_null;
    short scale;
    int precision;
    char *col_name;
    char *real_attr;
    char *class_name;
    char *default_value;
    char is_auto_increment;
    char is_unique_key;
    char is_primary_key;
  } T_CCI_COL_INFO;

#if !defined(CAS)
#ifdef DBDEF_HEADER_
  typedef int T_CCI_TRAN_ISOLATION;
#else
  typedef enum
  {
    CCI_TRAN_UNKNOWN_ISOLATION = 0,
    CCI_TRAN_ISOLATION_MIN = 1,

#if 0				/* unused */
    TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE = 1,
    TRAN_COMMIT_CLASS_COMMIT_INSTANCE = 2,
#endif
    CCI_TRAN_REP_CLASS_UNCOMMIT_INSTANCE = 3,
#if 0				/* unused */
    TRAN_REP_CLASS_COMMIT_INSTANCE = 4,
    TRAN_REP_CLASS_REP_INSTANCE = 5,
    TRAN_SERIALIZABLE = 6,
#endif

    CCI_TRAN_DEFAULT_ISOLATION = CCI_TRAN_REP_CLASS_UNCOMMIT_INSTANCE,

    CCITRAN_ISOLATION_MAX = 6
  } T_CCI_TRAN_ISOLATION;
#endif
#endif

  /* delete or update action type for foreign key */
  typedef enum
  {
    CCI_FOREIGN_KEY_CASCADE = 0,
    CCI_FOREIGN_KEY_RESTRICT = 1,
    CCI_FOREIGN_KEY_NO_ACTION = 2,
    CCI_FOREIGN_KEY_SET_NULL = 3
  } T_CCI_FOREIGN_KEY_ACTION;

  /* memory allocators */
  typedef void *(*CCI_MALLOC_FUNCTION) (size_t);
  typedef void *(*CCI_CALLOC_FUNCTION) (size_t, size_t);
  typedef void *(*CCI_REALLOC_FUNCTION) (void *, size_t);
  typedef void (*CCI_FREE_FUNCTION) (void *);

  typedef struct
  {
    int id;
    int64_t id_seq;
  } CCI_HANDLE_ID;

  typedef struct
  {
    CCI_HANDLE_ID conn_handle_id;
    T_CCI_ERROR err_buf;
  } CCI_CONN;

  typedef struct
  {
    CCI_HANDLE_ID conn_handle_id;
    CCI_HANDLE_ID stmt_handle_id;
    T_CCI_ERROR err_buf;
  } CCI_STMT;

  typedef short T_CCI_NODEID;

  typedef struct
  {
    int64_t groupid_version;
    int groupid_count;
    T_CCI_NODEID nodeid_table[1];
  } CCI_SHARD_GROUPID_INFO;

  typedef struct
  {
    const char *dbname;
    const char *hostname;
    T_CCI_NODEID nodeid;
    int port;
  } CCI_NODE_INFO;

  typedef struct
  {
    int64_t node_version;
    int node_count;
    CCI_NODE_INFO node_info[1];
  } CCI_SHARD_NODE_INFO;

/************************************************************************
 * EXPORTED FUNCTION PROTOTYPES						*
 ************************************************************************/

#if !defined(CAS)
  extern int cci_get_version_string (char *str, size_t len);
  extern int cci_get_version (int *major, int *minor, int *patch);
  extern int cci_connect (CCI_CONN * conn, char *url, const char *user,
			  const char *pass);
  extern int cci_disconnect (CCI_CONN * conn);
  extern int cci_end_tran (CCI_CONN * conn, char type);
  extern int cci_prepare (CCI_CONN * conn, CCI_STMT * stmt,
			  const char *sql_stmt, char flag);
  extern int cci_get_bind_num (CCI_STMT * stmt);
  extern T_CCI_COL_INFO *cci_get_result_info (CCI_STMT * stmt,
					      T_CCI_STMT_TYPE * cmd_type,
					      int *num);
  extern int cci_bind_param (CCI_STMT * stmt, int index, T_CCI_TYPE a_type,
			     const void *value, char flag);
  extern int cci_bind_param_ex (CCI_STMT * stmt, int index,
				T_CCI_TYPE a_type, const void *value,
				int length, char flag);
  extern int cci_execute (CCI_STMT * stmt, char flag, int max_col_size);

  extern int cci_get_db_parameter (CCI_CONN * conn, T_CCI_DB_PARAM param_name,
				   void *value);
  extern long cci_escape_string (CCI_CONN * conn, char *to, const char *from,
				 unsigned long length);
  extern int cci_close_query_result (CCI_STMT * stmt);
  extern int cci_close_req_handle (CCI_STMT * stmt);
  extern int cci_fetch_size (CCI_STMT * stmt, int fetch_size);
  extern int cci_fetch_next (CCI_STMT * stmt);
  extern int cci_fetch_first (CCI_STMT * stmt);
  extern int cci_fetch_last (CCI_STMT * stmt);
  extern int cci_get_int (CCI_STMT * stmt, int col_idx, int *indicator);
  extern int64_t cci_get_bigint (CCI_STMT * stmt, int col_idx,
				 int *indicator);
  extern double cci_get_double (CCI_STMT * stmt, int col_idx, int *indicator);
  extern char *cci_get_string (CCI_STMT * stmt, int col_idx, int *indicator);
  extern T_CCI_DATETIME cci_get_datetime (CCI_STMT * stmt, int col_idx,
					  int *indicator);
  extern T_CCI_VARBIT cci_get_bit (CCI_STMT * stmt, int col_idx,
				   int *indicator);
  extern int cci_schema_info (CCI_CONN * conn, CCI_STMT * stmt,
			      T_CCI_SCH_TYPE type, char *arg1, char *arg2,
			      int flag);
  extern int cci_get_db_version (CCI_CONN * conn, char *out_buf,
				 int buf_size);
  extern CCI_AUTOCOMMIT_MODE cci_get_autocommit (CCI_CONN * conn);
  extern int cci_set_autocommit (CCI_CONN * conn,
				 CCI_AUTOCOMMIT_MODE autocommit_mode);
  extern int cci_set_holdability (CCI_CONN * conn, int holdable);
  extern int cci_get_holdability (CCI_CONN * conn);
  extern int cci_set_login_timeout (CCI_CONN * conn, int timeout);
  extern int cci_get_login_timeout (CCI_CONN * conn, int *timeout);

  extern int cci_is_holdable (CCI_STMT * stmt);
  extern int cci_bind_param_array_size (CCI_STMT * stmt, int array_size);
  extern int cci_bind_param_array (CCI_STMT * stmt, int index,
				   T_CCI_TYPE a_type, const void *value,
				   int *null_ind);
  extern int cci_execute_batch (CCI_STMT * stmt, T_CCI_QUERY_RESULT ** qr);
  extern int cci_query_result_free (T_CCI_QUERY_RESULT * qr, int num_q);
  extern int cci_execute_result (CCI_STMT * stmt, T_CCI_QUERY_RESULT ** qr);

  extern int cci_get_query_plan (CCI_CONN * conn, const char *sql,
				 char **out_buf);
  extern int cci_query_info_free (char *out_buf);
  extern int cci_set_max_row (CCI_STMT * stmt, int max_row);

  extern int cci_cancel (CCI_CONN * conn);
  extern int cci_get_error_msg (int err_code, T_CCI_ERROR * err_buf,
				char *out_buf, int out_buf_size);
  extern int cci_get_err_msg (int err_code, char *buf, int bufsize);

  extern T_CCI_PROPERTIES *cci_property_create (void);
  extern void cci_property_destroy (T_CCI_PROPERTIES * properties);
  extern int cci_property_set (T_CCI_PROPERTIES * properties, char *key,
			       char *value);
  extern char *cci_property_get (T_CCI_PROPERTIES * properties,
				 const char *key);
  extern int cci_set_query_timeout (CCI_STMT * stmt, int timeout);
  extern int cci_get_query_timeout (CCI_STMT * stmt);

  extern int cci_set_allocators (CCI_MALLOC_FUNCTION malloc_func,
				 CCI_FREE_FUNCTION free_func,
				 CCI_REALLOC_FUNCTION realloc_func,
				 CCI_CALLOC_FUNCTION calloc_func);

  extern int cci_get_cas_info (CCI_CONN * conn, char *info_buf,
			       int buf_length);
#endif

/************************************************************************
 * EXPORTED VARIABLES							*
 ************************************************************************/

#ifdef __cplusplus
}
#endif

#endif				/* _CAS_CCI_H_ */

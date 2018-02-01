/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 */


/*
 * db.h - Stubs for the SQL interface layer.
 */

#ifndef _DB_H_
#define _DB_H_

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include "error_manager.h"
#include "dbi.h"
#include "dbtype.h"
#include "dbdef.h"
#include "intl_support.h"
#include "db_date.h"
#include "db_query.h"
#include "object_representation.h"
#include "object_domain.h"
#if !defined(SERVER_MODE)
#include "authenticate.h"
#endif
#include "log_comm.h"
#include "parser.h"

/* GLOBAL STATE */
#define DB_CONNECTION_STATUS_NOT_CONNECTED      0
#define DB_CONNECTION_STATUS_CONNECTED          1
#define DB_CONNECTION_STATUS_RESET              -1
extern int db_Connect_status;

extern SESSION_ID db_Session_id;

extern int db_Row_count;

#if !defined(_DB_IS_ALLOWED_MODIFICATION_)
#define _DB_IS_ALLOWED_MODIFICATION_
extern bool db_is_Allowed_Modification;
#endif /* _DB_IS_ALLOWED_MODIFICATION_ */

#if !defined(SERVER_MODE)
extern int db_Client_type;

extern char db_Database_name[];
extern char db_Program_name[];
#endif /* !SERVER_MODE */

/* MACROS FOR ERROR CHECKING */
/* These should be used at the start of every db_ function so we can check
   various validations before executing. */

/* CHECK CONNECT */
#define CHECK_CONNECT_VOID()                                            \
  do {                                                                  \
    if (db_Connect_status != DB_CONNECTION_STATUS_CONNECTED)            \
    {                                                                   \
      assert (false);							\
      er_set(ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_NO_CONNECT, 0);   \
      return;                                                           \
    }                                                                   \
  } while (0)

#define CHECK_CONNECT_AND_RETURN_EXPR(return_expr_)                     \
  do {                                                                  \
    if (db_Connect_status != DB_CONNECTION_STATUS_CONNECTED)            \
    {                                                                   \
      er_set(ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_NO_CONNECT, 0);   \
      return (return_expr_);                                            \
    }                                                                   \
  } while (0)

#if 0

#define CHECK_CONNECT_ERROR()     \
  CHECK_CONNECT_AND_RETURN_EXPR((DB_TYPE) ER_OBJ_NO_CONNECT)

#define CHECK_CONNECT_NULL()      \
  CHECK_CONNECT_AND_RETURN_EXPR(NULL)

#define CHECK_CONNECT_ZERO()      \
  CHECK_CONNECT_AND_RETURN_EXPR(0)

#define CHECK_CONNECT_ZERO_TYPE(TYPE)      \
  CHECK_CONNECT_AND_RETURN_EXPR((TYPE)0)

#define CHECK_CONNECT_MINUSONE()  \
  CHECK_CONNECT_AND_RETURN_EXPR(-1)

#define CHECK_CONNECT_FALSE()     \
  CHECK_CONNECT_AND_RETURN_EXPR(false)

#else

#define CHECK_CONNECT_ERROR()
#define CHECK_CONNECT_NULL()
#define CHECK_CONNECT_ZERO()
#define CHECK_CONNECT_ZERO_TYPE(TYPE)
#define CHECK_CONNECT_MINUSONE()
#define CHECK_CONNECT_FALSE()

#endif

/* CHECK MODIFICATION */
#define CHECK_MODIFICATION_AND_RETURN_EXPR(return_expr_)                     \
  if (db_is_Allowed_Modification == false) {                                 \
    er_set(ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_DB_NO_MODIFICATIONS, 0);     \
    return (return_expr_);                                                   \
  }

#define CHECK_MODIFICATION_ERROR()   \
  CHECK_MODIFICATION_AND_RETURN_EXPR(ER_DB_NO_MODIFICATIONS)

#define CHECK_MODIFICATION_NULL()   \
  CHECK_MODIFICATION_AND_RETURN_EXPR(NULL)

/* Argument checking macros */
#define CHECK_1ARG_RETURN_EXPR(obj, expr)                                      \
  do {                                                                         \
    if((obj) == NULL) {                                                        \
      assert (false);							       \
      er_set(ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0); \
      return (expr);                                                           \
    }                                                                          \
  } while (0)

#define CHECK_2ARGS_RETURN_EXPR(obj1, obj2, expr)                              \
  do {                                                                         \
    if((obj1) == NULL || (obj2) == NULL) {                                     \
      assert (false);							       \
      er_set(ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0); \
      return (expr);                                                           \
    }                                                                          \
  } while (0)

#define CHECK_3ARGS_RETURN_EXPR(obj1, obj2, obj3, expr)                        \
  do {                                                                         \
    if((obj1) == NULL || (obj2) == NULL || (obj3) == NULL) {                   \
      assert (false);							       \
      er_set(ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0); \
      return (expr);                                                           \
    }                                                                          \
  } while (0)

#define CHECK_1ARG_NULL(obj)        \
  CHECK_1ARG_RETURN_EXPR(obj, NULL)

#define CHECK_2ARGS_NULL(obj1, obj2)    \
  CHECK_2ARGS_RETURN_EXPR(obj1,obj2,NULL)

#define CHECK_3ARGS_NULL(obj1, obj2, obj3) \
  CHECK_3ARGS_RETURN_EXPR(obj1,obj2,obj3,NULL)

#define CHECK_1ARG_FALSE(obj)  \
  CHECK_1ARG_RETURN_EXPR(obj,false)

#define CHECK_1ARG_TRUE(obj)   \
  CHECK_1ARG_RETURN_EXPR(obj, true)

#define CHECK_1ARG_ERROR(obj)  \
  CHECK_1ARG_RETURN_EXPR(obj,ER_OBJ_INVALID_ARGUMENTS)

#define CHECK_1ARG_ERROR_WITH_TYPE(obj, TYPE)  \
  CHECK_1ARG_RETURN_EXPR(obj,(TYPE)ER_OBJ_INVALID_ARGUMENTS)

#define CHECK_1ARG_MINUSONE(obj) \
  CHECK_1ARG_RETURN_EXPR(obj,-1)

#define CHECK_2ARGS_ERROR(obj1, obj2)   \
  CHECK_2ARGS_RETURN_EXPR(obj1, obj2, ER_OBJ_INVALID_ARGUMENTS)

#define CHECK_3ARGS_ERROR(obj1, obj2, obj3) \
  CHECK_3ARGS_RETURN_EXPR(obj1, obj2, obj3, ER_OBJ_INVALID_ARGUMENTS)

#define CHECK_1ARG_ZERO(obj)     \
  CHECK_1ARG_RETURN_EXPR(obj, 0)

#define CHECK_1ARG_ZERO_WITH_TYPE(obj1, RETURN_TYPE)     \
  CHECK_1ARG_RETURN_EXPR(obj1, (RETURN_TYPE) 0)

#define CHECK_2ARGS_ZERO(obj1, obj2)    \
  CHECK_2ARGS_RETURN_EXPR(obj1,obj2, 0)

#define DB_MAKE_OID(value, oid)						\
  do {									\
    if ((db_value_domain_init(value, DB_TYPE_OID, 0, 0)) == NO_ERROR)	\
	(void)db_make_oid(value, oid);				        \
  } while (0)

#define DB_GET_OID(value)		(db_get_oid(value))

#define db_locate_numeric(value) \
  ((const DB_C_NUMERIC) ((value)->data.num.d.buf))

extern int db_init (const char *program, int print_version,
		    const char *dbname, const char *host_name,
		    const bool overwrite,
		    const char *addmore_vols_file,
		    int npages, int desired_pagesize,
		    int log_npages, int desired_log_page_size);

#if defined (ENABLE_UNUSED_FUNCTION)
#ifdef __cplusplus
extern "C"
{
#endif
  extern int parse_one_statement (int state);
#ifdef __cplusplus
}
#endif

extern int db_get_parser_line_col (DB_SESSION * session, int *line, int *col);
extern int db_get_line_col_of_1st_error (DB_SESSION * session,
					 DB_QUERY_ERROR * linecol);
extern DB_VALUE *db_get_hostvars (DB_SESSION * session);
extern char **db_get_lock_classes (DB_SESSION * session);
extern void db_drop_all_statements (DB_SESSION * session);
extern PARSER_CONTEXT *db_get_parser (DB_SESSION * session);
extern DB_SESSION *db_make_session_for_one_statement_execution (FILE * file);
#endif
extern int db_abort_to_savepoint_internal (const char *savepoint_name);

#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_error_code_test (void);
extern const char *db_error_string_test (int level);
#endif

extern int db_make_oid (DB_VALUE * value, const OID * oid);
extern OID *db_get_oid (const DB_VALUE * value);
extern int db_value_alter_type (DB_VALUE * value, DB_TYPE type);

#if !defined(_DBTYPE_H_)
extern int db_value_put_encoded_time (DB_VALUE * value, const DB_TIME * time);
extern int db_value_put_encoded_date (DB_VALUE * value, const DB_DATE * date);
#endif
#if defined (ENABLE_UNUSED_FUNCTION)
extern void *db_value_eh_key (DB_VALUE * value);
extern int db_value_put_db_data (DB_VALUE * value, const DB_DATA * data);
extern DB_DATA *db_value_get_db_data (DB_VALUE * value);
#endif
extern int db_make_db_char (DB_VALUE * value, const int collation_id,
			    char *str, const int size);

extern DB_OBJECT *db_create_internal (DB_OBJECT * obj);
extern int db_put_internal (DB_OBJECT * obj, const char *name,
			    DB_VALUE * value);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int dbt_put_internal (DB_OTMPL * def, const char *name,
			     DB_VALUE * value);
extern int dbt_dput_internal (DB_OTMPL * def,
			      DB_ATTDESC * attribute, DB_VALUE * value);

extern int db_add_attribute_internal (MOP class_, const char *name,
				      const char *domain,
				      DB_VALUE * default_value);
extern int db_rename_internal (DB_OBJECT * classobj,
			       const char *name, const char *newname);
extern int db_drop_attribute_internal (DB_OBJECT * classobj,
				       const char *name);
#endif
extern DB_SESSION *db_open_buffer_local (const char *buffer);
extern int db_compile_statement_local (DB_SESSION * session);
extern int db_execute_statement_local (DB_SESSION * session,
				       DB_QUERY_RESULT ** result);
extern void db_close_session_local (DB_SESSION * session);
extern int db_savepoint_transaction_internal (const char *savepoint_name);
#if defined (ENABLE_UNUSED_FUNCTION)
extern BTID *db_constraint_index (DB_CONSTRAINT * constraint, BTID * index);
#endif

#endif /* _DB_H_ */

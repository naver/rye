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
 * dbi.h - Definitions and function prototypes for the Rye
 *         Application Program Interface (API).
 */

#ifndef _DBI_H_
#define _DBI_H_

#ident "$Id$"

#include <stdio.h>
#include <time.h>
#include "error_code.h"
#include "dbtype.h"
#include "dbdef.h"
#include "db_date.h"
#include "db_query.h"
#include "ini_parser.h"

/* reason for a client to reconnect */
#define DB_RC_MISMATCHED_RW_MODE        0x00000002
#define DB_RC_NON_PREFERRED_HOSTS       0x00000004
#define DB_RC_HA_REPL_DELAY             0x00000008

#define DB_MAX_SHARDKEY_INFO_COUNT	100

enum
{ DB_NO_OIDS = 0, DB_ROW_OIDS };

extern int db_initialize (void);
extern int db_finalize (void);

/* Memory reclamation functions */
extern void db_objlist_free (DB_OBJLIST * list);
extern void db_string_free (char *string);

extern int db_login (const char *name, const char *password);
extern int db_restart (const char *program,
		       int print_version, const char *volume);
extern int db_restart_ex (const char *program, const char *db_name,
			  const char *db_user, const char *db_password,
			  int client_type);
extern int db_get_server_start_time (void);
extern void db_set_server_session_key (const char *key);
extern char *db_get_server_session_key (void);
extern SESSION_ID db_get_session_id (void);
extern void db_set_session_id (const SESSION_ID session_id);
extern int db_end_session (void);
extern int db_find_or_create_session (const char *db_user,
				      const char *program_name);
extern int db_shutdown (void);
extern int db_ping_server (int client_val, int *server_val);
extern int db_disable_modification (void);
extern int db_enable_modification (void);
extern bool db_is_modification_disabled (void);
extern int db_commit_transaction (void);
extern int db_abort_transaction (void);
extern int db_commit_is_needed (void);
extern int db_savepoint_transaction (const char *savepoint_name);
extern int db_abort_to_savepoint (const char *savepoint_name);
extern void db_set_interrupt (int set);
extern int db_set_suppress_repl_on_transaction (int set);
extern void db_checkpoint (void);
extern char *db_vol_label (int volid, char *vol_fullname);
extern int db_add_volume (DBDEF_VOL_EXT_INFO * ext_info);
extern int db_num_volumes (void);
extern int db_num_bestspace_entries (void);
extern void db_set_client_ro_tran (bool mode);
extern bool db_is_server_in_tran (void);
extern short db_server_shard_nodeid (void);
extern int db_update_group_id (int migrator_id, int group_id, int target,
			       int on_off);
extern int db_block_globl_dml (int start_or_end);
extern const char *db_error_string (int level);
extern int db_error_code (void);

typedef void (*db_error_log_handler_t) (unsigned int);
extern db_error_log_handler_t
db_register_error_log_handler (db_error_log_handler_t f);

#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_set_lock_timeout (int seconds);
extern int db_set_isolation (DB_TRAN_ISOLATION isolation);
#endif
extern void db_get_tran_settings (int *lock_wait,
				  DB_TRAN_ISOLATION * tran_isolation);

/* Authorization */
extern DB_OBJECT *db_get_user (void);
extern DB_OBJECT *db_get_owner (DB_OBJECT * classobj);
extern char *db_get_user_name (void);
extern char *db_get_user_and_host_name (void);
extern DB_OBJECT *db_find_user (const char *name);
extern DB_OBJECT *db_add_user (const char *name, int *exists);
extern int db_drop_user (DB_OBJECT * user);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_drop_member (DB_OBJECT * user, DB_OBJECT * member);
#endif
extern int db_grant (DB_OBJECT * user, DB_OBJECT * classobj,
		     DB_AUTH auth, int grant_option);
extern int db_revoke (DB_OBJECT * user, DB_OBJECT * classobj, DB_AUTH auth);
extern int db_check_authorization (DB_OBJECT * op, DB_AUTH auth);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_get_class_privilege (DB_OBJECT * op, unsigned int *auth);
#endif

extern void db_value_fprint (FILE * fp, const DB_VALUE * value);

extern void db_idxkey_fprint (FILE * fp, const DB_IDXKEY * key);

/* Instance manipulation */
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_get (DB_OBJECT * object, const char *attpath, DB_VALUE * value);
#if 0				/* unused */
extern int db_put (DB_OBJECT * obj, const char *name, DB_VALUE * value);
#endif
extern int db_drop (DB_OBJECT * obj);
#endif
extern void db_set_execution_plan (char *plan, int length);

extern int db_fetch_set (DB_COLLECTION * set, int quit_on_error);

/* Collection functions */
#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_COLLECTION *db_col_create (DB_TYPE type, int size,
				     DB_DOMAIN * domain);
extern int db_col_size (DB_COLLECTION * col);
extern DB_TYPE db_col_type (DB_COLLECTION * col);
extern int db_col_get (DB_COLLECTION * col,
		       int element_index, DB_VALUE * value);
#endif

/* Sequence functions.
   These are now obsolete. Please use the generic collection functions
   "db_col*" instead */
extern DB_COLLECTION *db_seq_create (DB_OBJECT * classobj,
				     const char *name, int size);
extern int db_set_free (DB_COLLECTION * set);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_set_add (DB_COLLECTION * set, DB_VALUE * value);
#endif
extern int db_set_get (DB_COLLECTION * set,
		       int element_index, DB_VALUE * value);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_set_drop (DB_COLLECTION * set, DB_VALUE * value);
#endif
extern int db_set_size (DB_COLLECTION * set);

#if defined (RYE_DEBUG)
extern int db_set_print (DB_COLLECTION * set);
#endif
#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_TYPE db_set_type (DB_COLLECTION * set);
#endif
extern DB_COLLECTION *db_set_copy (DB_COLLECTION * set);
extern int db_seq_get (DB_COLLECTION * set,
		       int element_index, DB_VALUE * value);
extern int db_seq_put (DB_COLLECTION * set,
		       int element_index, DB_VALUE * value);
extern int db_seq_size (DB_COLLECTION * set);

/* Class definition */
extern DB_OBJECT *db_create_class (const char *name);
#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_OBJECT *db_create_vclass (const char *name);
#endif
extern int db_drop_class (DB_OBJECT * classobj);
extern int db_rename_class (DB_OBJECT * classobj, const char *new_name);

#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_add_index (DB_OBJECT * classobj, const char *attname);
extern int db_drop_index (DB_OBJECT * classobj, const char *attname);

extern int db_rename (DB_OBJECT * classobj, const char *name,
		      int class_namespace, const char *newname);

extern int db_add_attribute (DB_OBJECT * obj, const char *name,
			     const char *domain, DB_VALUE * default_value);
#endif
#if 0				/* unused */
extern int db_drop_attribute (DB_OBJECT * classobj, const char *name);
#endif
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_change_default (DB_OBJECT * classobj,
			      const char *name, DB_VALUE * value);

extern int db_constrain_non_null (DB_OBJECT * classobj,
				  const char *name, int on_or_off);
#endif
#if 0				/* unused */
extern int db_constrain_unique (DB_OBJECT * classobj,
				const char *name, int on_or_off);
#endif
extern int db_add_constraint (MOP classmop,
			      DB_CONSTRAINT_TYPE
			      constraint_type,
			      const char *constraint_name,
			      const char **att_names);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_drop_constraint (MOP classmop,
			       DB_CONSTRAINT_TYPE
			       constraint_type,
			       const char *constraint_name,
			       const char **att_names);
#endif

/* Browsing functions */
extern char *db_get_database_name (void);
extern void db_set_client_type (int client_type);
extern void db_set_preferred_hosts (const PRM_NODE_LIST * hosts);
extern void db_set_connect_order_random (bool connect_order_random);
#if !defined(SERVER_MODE)
extern int db_get_client_type (void);
#endif
extern const char *db_get_type_name (DB_TYPE type_id);

extern void db_set_reconnect_reason (int reason);
extern void db_unset_reconnect_reason (int reason);
extern void db_clear_reconnect_reason (void);
extern bool db_get_need_reconnect (void);
extern void db_set_ignore_repl_delay (void);
extern void db_clear_ignore_repl_delay (void);
extern bool db_get_ignore_repl_delay (void);
extern int db_get_delayed_hosts_count (void);
extern void db_clear_delayed_hosts_count (void);
extern void db_set_max_num_delayed_hosts_lookup (int
						 max_num_delayed_hosts_lookup);
extern int db_get_max_num_delayed_hosts_lookup (void);

extern int db_is_class (DB_OBJECT * obj);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_is_any_class (DB_OBJECT * obj);
#endif
extern int db_is_system_table (DB_OBJECT * op);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_is_deleted (DB_OBJECT * obj);
#endif

extern DB_ATTRIBUTE *db_get_attribute (DB_OBJECT * obj, const char *name);
extern DB_ATTRIBUTE *db_get_attribute_by_name (const char *class_name,
					       const char *atrribute_name);
extern DB_ATTRIBUTE *db_get_attributes (DB_OBJECT * obj);

#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_TYPE db_attribute_type (DB_ATTRIBUTE * attribute);
#endif
extern DB_ATTRIBUTE *db_attribute_next (DB_ATTRIBUTE * attribute);
extern const char *db_attribute_name (DB_ATTRIBUTE * attribute);
extern int db_attribute_order (DB_ATTRIBUTE * attribute);
extern DB_DOMAIN *db_attribute_domain (DB_ATTRIBUTE * attribute);
extern DB_OBJECT *db_attribute_class (DB_ATTRIBUTE * attribute);
extern DB_VALUE *db_attribute_default (DB_ATTRIBUTE * attribute);
extern int db_attribute_is_unique (DB_ATTRIBUTE * attribute);
extern int db_attribute_is_primary_key (DB_ATTRIBUTE * attribute);
extern int db_attribute_is_non_null (DB_ATTRIBUTE * attribute);
extern int db_attribute_is_indexed (DB_ATTRIBUTE * attribute);
extern DB_DOMAIN *db_type_to_db_domain (DB_TYPE type);

extern DB_DOMAIN *db_domain_next (const DB_DOMAIN * domain);
extern DB_OBJECT *db_domain_class (const DB_DOMAIN * domain);
extern DB_DOMAIN *db_domain_set (const DB_DOMAIN * domain);
extern int db_domain_precision (const DB_DOMAIN * domain);
extern int db_domain_scale (const DB_DOMAIN * domain);
extern int db_domain_collation_id (const DB_DOMAIN * domain);

extern int db_get_class_num_objs_and_pages (DB_OBJECT *
					    classmop,
					    int approximation,
					    int64_t * nobjs, int *npages);

/* Constraint Functions */
extern DB_CONSTRAINT *db_get_constraints (DB_OBJECT * obj);
extern DB_CONSTRAINT *db_constraint_next (DB_CONSTRAINT * constraint);
extern DB_CONSTRAINT_TYPE db_constraint_type (const DB_CONSTRAINT *
					      constraint);
extern const char *db_constraint_name (DB_CONSTRAINT * constraint);
extern DB_ATTRIBUTE **db_constraint_attributes (DB_CONSTRAINT * constraint);
extern const int *db_constraint_asc_desc (DB_CONSTRAINT * constraint);

/* Schema template functions */
extern DB_CTMPL *dbt_create_class (const char *name);
extern DB_CTMPL *dbt_create_vclass (const char *name);
extern DB_CTMPL *dbt_edit_class (DB_OBJECT * classobj);
extern DB_CTMPL *dbt_copy_class (const char *new_name,
				 const char *existing_name,
				 SM_CLASS ** class_);
extern DB_OBJECT *dbt_finish_class (DB_CTMPL * def);
extern void dbt_abort_class (DB_CTMPL * def);

extern int dbt_constrain_non_null (DB_CTMPL * def,
				   const char *name, int on_or_off);
extern int dbt_add_constraint (DB_CTMPL * def,
			       DB_CONSTRAINT_TYPE
			       constraint_type,
			       const char *constraint_name,
			       const char **attnames);
extern int dbt_drop_constraint (DB_CTMPL * def,
				DB_CONSTRAINT_TYPE
				constraint_type,
				const char *constraint_name,
				const char **attnames);
extern int dbt_change_primary_key (DB_CTMPL * def, const char *index_name);
extern int dbt_drop_attribute (DB_CTMPL * def, const char *name);
extern int dbt_rename (DB_CTMPL * def, const char *name,
		       int class_namespace, const char *newname);

extern int dbt_add_query_spec (DB_CTMPL * def, const char *query);

#if defined (ENABLE_UNUSED_FUNCTION)
/* Object template functions */
extern DB_OTMPL *dbt_edit_object (DB_OBJECT * object);
extern DB_OBJECT *dbt_finish_object (DB_OTMPL * def);
extern void dbt_abort_object (DB_OTMPL * def);

/* Descriptor functions.
 * The descriptor interface offers an alternative to attribute
 * names that can be substantially faster for repetitive operations.
 */
extern int db_get_attribute_descriptor (DB_OBJECT * obj,
					const char *attname,
					int for_update,
					DB_ATTDESC ** descriptor);
extern void db_free_attribute_descriptor (DB_ATTDESC * descriptor);

extern int db_add_query_spec (DB_OBJECT * vclass, const char *query);
#endif

extern int db_is_vclass (DB_OBJECT * op);

extern DB_QUERY_SPEC *db_get_query_specs (DB_OBJECT * obj);
extern DB_QUERY_SPEC *db_query_spec_next (DB_QUERY_SPEC * query_spec);
extern const char *db_query_spec_string (DB_QUERY_SPEC * query_spec);

/* query pre-processing functions */
extern DB_QUERY_TYPE *db_query_format_next (DB_QUERY_TYPE * query_type);
extern char *db_query_format_name (DB_QUERY_TYPE * query_type);
extern DB_TYPE db_query_format_type (DB_QUERY_TYPE * query_type);
extern void db_query_format_free (DB_QUERY_TYPE * query_type);
extern DB_DOMAIN *db_query_format_domain (DB_QUERY_TYPE * query_type);
#if defined (ENABLE_UNUSED_FUNCTION)
extern char *db_query_format_attr_name (DB_QUERY_TYPE * query_type);
#endif
extern const char *db_query_format_class_name (DB_QUERY_TYPE * query_type);
extern int db_query_format_is_non_null (DB_QUERY_TYPE * query_type);

/* query processing functions */
extern int db_query_next_tuple (DB_QUERY_RESULT * result);
extern int db_query_prev_tuple (DB_QUERY_RESULT * result);
extern int db_query_first_tuple (DB_QUERY_RESULT * result);
extern int db_query_last_tuple (DB_QUERY_RESULT * result);
extern int db_query_get_tuple_value (DB_QUERY_RESULT * result,
				     int tuple_index, DB_VALUE * value);

extern int db_query_get_tuple_valuelist (DB_QUERY_RESULT *
					 result, int size,
					 DB_VALUE * value_list);

extern int db_query_tuple_count (DB_QUERY_RESULT * result);

extern int db_query_column_count (DB_QUERY_RESULT * result);

/* query post-processing functions */
extern int db_query_plan_dump_file (char *filename);

/* sql query routines */
extern DB_SESSION *db_open_buffer (const char *buffer);

extern int db_statement_count (DB_SESSION * session);

extern int db_compile_statement (DB_SESSION * session);

extern DB_SESSION_ERROR *db_get_errors (DB_SESSION * session);

extern DB_SESSION_ERROR *db_get_next_error (DB_SESSION_ERROR *
					    errors,
					    int *linenumber,
					    int *columnnumber);
#if 0				/* unused */
extern DB_SESSION_ERROR *db_get_warnings (DB_SESSION * session);
extern DB_SESSION_ERROR *db_get_next_warning (DB_SESSION_WARNING *
					      errors,
					      int *linenumber,
					      int *columnnumber);
#endif
extern void db_session_set_holdable (DB_SESSION * session, bool holdable);
extern void db_session_set_autocommit_mode (DB_SESSION * session,
					    bool autocommit_mode);


extern DB_QUERY_TYPE *db_get_query_type_list (DB_SESSION * session);

#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_MARKER *db_get_input_markers (DB_SESSION * session);
extern DB_MARKER *db_marker_next (DB_MARKER * marker);
extern DB_DOMAIN *db_marker_domain (DB_MARKER * marker);
#endif

extern RYE_STMT_TYPE db_get_statement_type (DB_SESSION * session);

extern int db_push_values (DB_SESSION * session, int count,
			   DB_VALUE * in_values);
extern int db_get_host_var_count (DB_SESSION * session);
extern bool db_is_shard_table_query (DB_SESSION * session);
extern int db_get_shard_key_values (DB_SESSION * session,
				    int *num_shard_values, int *num_shard_pos,
				    char **value_out_buf,
				    int value_out_buf_size, int *pos_out_buf,
				    int pos_out_buf_size);
extern bool db_is_select_for_update (DB_SESSION * session);

extern int db_execute (const char *RSQL_query,
		       DB_QUERY_RESULT ** result,
		       DB_QUERY_ERROR * query_error);

extern int db_execute_statement (DB_SESSION * session,
				 DB_QUERY_RESULT ** result);

extern int db_execute_and_keep_statement (DB_SESSION * session,
					  DB_QUERY_RESULT ** result);

extern int db_query_get_info (DB_QUERY_RESULT * result,
			      int *done, int *count,
			      int *error, char **err_string);

#if defined (ENABLE_UNUSED_FUNCTION)
extern int db_query_sync (DB_QUERY_RESULT * result, int wait);
#endif

extern int db_query_set_copy_tplvalue (DB_QUERY_RESULT * result, int copy);

extern void db_close_session (DB_SESSION * session);
extern void db_drop_statement (DB_SESSION * session);

extern bool db_get_cacheinfo (DB_SESSION * session, bool * use_plan_cache);

/* These are used by rsql but weren't in the 2.0 dbi.h file, added
   it for the PC.  If we don't want them here, they should go somewhere
   else so rsql.c doesn't have to have an explicit declaration.
*/
extern void db_free_query (DB_SESSION * session);
extern DB_QUERY_TYPE *db_get_query_type_ptr (DB_QUERY_RESULT * result);

/* INTERNAL FUNCTIONS
 * These are part of the interface but are intended only for
 * internal use by Rye.  Applications should not use these
 * functions.
 */
#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_IDENTIFIER *db_identifier (DB_OBJECT * obj);
extern DB_OBJECT *db_object (DB_IDENTIFIER * oid);
#endif

extern int db_update_persist_conf_file (const char *proc_name,
					const char *sect_name,
					const char *key, const char *value);
extern int db_delete_proc_persist_conf_file (const char *proc_name);
extern int db_delete_sect_persist_conf_file (const char *proc_name,
					     const char *sect_name);
extern int db_delete_key_persist_conf_file (const char *proc_name,
					    const char *sect_name,
					    const char *key);
extern int db_read_server_persist_conf_file (const char *sect_name,
					     const bool reload);
extern int db_read_broker_persist_conf_file (INI_TABLE * ini);
extern int db_dump_persist_conf_file (FILE * fp, const char *proc_name,
				      const char *sect_name);

extern int db_set_system_parameters (char *prm_names, int len,
				     const char *data, const bool persist);
extern int db_get_system_parameters (char *data, int len);
extern int db_get_system_parameter_value (char *value, int max_len,
					  const char *param_name);

extern void db_clear_host_connected (void);
extern char *db_get_database_version (void);
extern char *db_get_execution_plan (void);
extern void db_session_set_groupid (DB_SESSION * session, int groupid);
extern void db_session_set_from_migrator (DB_SESSION * session,
					  bool from_migrator);
extern int db_get_connect_status (void);
extern void db_set_connect_status (int status);

extern int db_get_server_state (void);
extern unsigned int db_get_server_addr (void);

#endif /* _DBI_H_ */

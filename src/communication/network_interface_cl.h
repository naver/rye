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
 * network_interface_cl.h - Definitions for client network support
 */

#ifndef _NETWORK_INTERFACE_CL_H_
#define _NETWORK_INTERFACE_CL_H_

#ident "$Id$"

#include <stdio.h>

#include "dbdef.h"
#include "repl_log.h"
#include "server_interface.h"
#include "perf_monitor.h"
#include "storage_common.h"
#include "object_domain.h"
#include "query_list.h"
#include "statistics.h"
#include "connection_defs.h"
#include "language_support.h"
#include "log_comm.h"
#include "query_executor.h"
#include "repl_writer.h"
#include "rbl_sync_log.h"
#include "tcp.h"
#include "backup.h"
#include "rye_server_shm.h"

typedef struct server_info SERVER_INFO;
struct server_info
{
  int info_bits;
  DB_VALUE *value[SI_CNT];
};

/* killtran supporting structures and functions */
typedef struct one_tran_info ONE_TRAN_INFO;
struct one_tran_info
{
  int tran_index;
  int state;
  int process_id;
  char *db_user;
  char *program_name;
  char *login_name;
  char *host_name;
  TRAN_QUERY_EXEC_INFO query_exec_info;
};

typedef struct trans_info TRANS_INFO;
struct trans_info
{
  int num_trans;
  bool include_query_exec_info;
  ONE_TRAN_INFO tran[1];	/* really [num_trans] */
};

extern void db_free_execution_plan (void);
extern int locator_fetch (OID * oidp, LOCK lock, OID * class_oid,
			  int prefetch, LC_COPYAREA ** fetch_copyarea);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int locator_get_class (OID * class_oid, const OID * oid,
			      LOCK lock, int prefetching,
			      LC_COPYAREA ** fetch_copyarea);
#endif
extern int locator_fetch_all (const HFID * hfid, LOCK * lock,
			      OID * class_oidp, INT64 * nobjects,
			      INT64 * nfetched, OID * last_oidp,
			      LC_COPYAREA ** fetch_copyarea);
extern int locator_force (LC_COPYAREA * copy_area);
extern int locator_repl_force (LC_COPYAREA * copy_area,
			       LC_COPYAREA ** reply_copy_area);
extern int locator_fetch_lockset (LC_LOCKSET * lockset,
				  LC_COPYAREA ** fetch_copyarea);
extern LC_FIND_CLASSNAME locator_find_class_oid (const char *class_name,
						 OID * class_oid, LOCK lock);
extern LC_FIND_CLASSNAME locator_reserve_class_names (const int num_classes,
						      const char
						      **class_names,
						      OID * class_oids);
extern LC_FIND_CLASSNAME locator_delete_class_name (const char *class_name);
extern LC_FIND_CLASSNAME locator_rename_class_name (const char *old_name,
						    const char *new_name,
						    OID * class_oid);
extern int locator_assign_oid (const HFID * hfid, OID * perm_oid,
			       int expected_length, OID * class_oid,
			       const char *class_name);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int locator_assign_oid_batch (LC_OIDSET * oidset);
#endif
extern LC_FIND_CLASSNAME
locator_find_lockhint_class_oids (int num_classes,
				  const char **many_classnames,
				  LOCK * many_locks,
				  OID * guessed_class_oids,
				  int quit_on_errors,
				  LC_LOCKHINT ** lockhint,
				  LC_COPYAREA ** fetch_copyarea);
extern int locator_fetch_lockhint_classes (LC_LOCKHINT * lockhint,
					   LC_COPYAREA ** fetch_area);
extern int locator_lock_system_ddl_lock (void);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int lock_change_class_xlock_to_ulock (OID * class_oid);
#endif
extern int heap_create (HFID * hfid, const OID * class_oid);
extern int heap_destroy (const HFID * hfid);
extern VOLID
disk_get_purpose_and_space_info (VOLID volid,
				 DISK_VOLPURPOSE * vol_purpose,
				 VOL_SPACE_INFO * space_info);
extern char *disk_get_fullname (const char *database_name, VOLID volid,
				char *vol_fullname);
extern int log_reset_wait_msecs (int wait_msecs);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int log_reset_isolation (TRAN_ISOLATION isolation);
#endif
extern void log_set_interrupt (int set);
extern void log_checkpoint (void);
extern void log_dump_stat (FILE * outfp);
extern int log_set_suppress_repl_on_transaction (int set);

extern TRAN_STATE tran_server_commit (void);
extern TRAN_STATE tran_server_abort (void);
#if defined(ENABLE_UNUSED_FUNCTION)
extern bool tran_is_blocked (int tran_index);
#endif
extern int tran_server_has_updated (void);
extern int tran_server_is_active_and_has_updated (void);
extern int tran_wait_server_active_trans (void);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int tran_server_start_topop (LOG_LSA * topop_lsa);
extern TRAN_STATE tran_server_end_topop (LOG_RESULT_TOPOP result,
					 LOG_LSA * topop_lsa);
#endif
extern int tran_server_savepoint (const char *savept_name,
				  LOG_LSA * savept_lsa);
extern TRAN_STATE tran_server_partial_abort (const char *savept_name,
					     LOG_LSA * savept_lsa);
extern const char *tran_get_tranlist_state_name (TRAN_STATE state);
extern void lock_dump (FILE * outfp);
extern int acl_reload (void);
extern void acl_dump (FILE * outfp);

int boot_initialize_server (const BOOT_CLIENT_CREDENTIAL * client_credential,
			    BOOT_DB_PATH_INFO * db_path_info,
			    bool db_overwrite, const char *file_addmore_vols,
			    DKNPAGES db_npages, PGLENGTH db_desired_pagesize,
			    DKNPAGES log_npages,
			    PGLENGTH db_desired_log_page_size,
			    OID * rootclass_oid, HFID * rootclass_hfid,
			    int client_lock_wait);
int boot_register_client (BOOT_CLIENT_CREDENTIAL * client_credential,
			  int client_lock_wait,
			  TRAN_STATE * tran_state,
			  BOOT_SERVER_CREDENTIAL * server_credential);
extern int boot_unregister_client (int tran_index);
extern VOLID boot_add_volume_extension (DBDEF_VOL_EXT_INFO * ext_info);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int boot_check_db_consistency (int check_flag, OID * oids,
				      int num_oids);
#endif
extern int boot_find_number_permanent_volumes (void);
extern int boot_find_number_temp_volumes (void);
extern int boot_find_last_temp (void);
extern int boot_find_number_bestspace_entries (void);
extern int boot_delete (const char *db_name, bool force_delete);
extern int boot_restart_from_backup (int print_restart, const char *db_name,
				     BO_RESTART_ARG * r_args);
extern bool boot_shutdown_server (void);
extern HA_STATE boot_get_server_state (void);
extern int boot_notify_ha_apply_state (const char *host_ip,
				       HA_APPLY_STATE state);
extern char *stats_get_statistics_from_server (OID * classoid,
					       unsigned int timestamp,
					       int *length_ptr);
extern int stats_update_statistics (OID * classoid, int update_stats,
				    int with_fullscan);
extern int stats_update_all_statistics (int update_stats, int with_fullscan);

extern int btree_add_index (BTID * btid, int num_atts, DB_TYPE * att_type,
			    OID * class_oid, int attr_id);
extern int btree_load_data (BTID * btid, OID * class_oid, HFID * hfid);
extern int btree_delete_index (BTID * btid);
extern BTREE_SEARCH btree_find_unique (OID * class_oid, BTID * btid,
				       DB_IDXKEY * key, OID * oid);
extern int qfile_get_list_file_page (QUERY_ID query_id, VOLID volid,
				     PAGEID pageid, char *buffer,
				     int *buffer_size);
extern XASL_ID *qmgr_prepare_query (COMPILE_CONTEXT * context,
				    XASL_STREAM * stream,
				    const OID * user_oid);

extern QFILE_LIST_ID *qmgr_execute_query (const XASL_ID * xasl_id,
					  QUERY_ID * query_idp, int dbval_cnt,
					  const DB_VALUE * dbvals,
					  QUERY_FLAG flag,
					  int query_timeout,
					  int shard_groupid,
					  DB_VALUE * shard_key,
					  QUERY_EXECUTE_STATUS_FLAG *
					  qe_status_flag);
extern int qmgr_end_query (QUERY_ID query_id);
extern int qmgr_drop_query_plan (const char *qstmt, const OID * user_oid,
				 const XASL_ID * xasl_id);
extern int qmgr_drop_all_query_plans (void);
extern void qmgr_dump_query_plans (FILE * outfp);
extern int qmgr_get_query_info (DB_QUERY_RESULT * query_result, int *done,
				int *count, int *error, char **error_string);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int qmgr_sync_query (DB_QUERY_RESULT * query_result, int wait);
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
extern int mnt_server_start_stats (bool for_all_trans);
extern int mnt_server_stop_stats (void);
#endif
extern int mnt_server_copy_stats (MNT_SERVER_EXEC_STATS * to_stats);
extern int mnt_server_copy_global_stats (MNT_SERVER_EXEC_STATS * to_stats);
extern int catalog_is_acceptable_new_representation (OID * class_id,
						     HFID * hfid,
						     int *can_accept);
extern int thread_kill_tran_index (int kill_tran_index, char *kill_user,
				   char *kill_host, int kill_pid);
extern void thread_dump_cs_stat (FILE * outfp);
extern void thread_dump_server_stat (FILE * outfp);

extern int logtb_get_pack_tran_table (char **buffer_p, int *size_p,
				      bool include_query_exec_info);
extern void logtb_free_trans_info (TRANS_INFO * info);
extern TRANS_INFO *logtb_get_trans_info (bool include_query_exec_info);
extern void logtb_dump_trantable (FILE * outfp);

extern int heap_get_class_num_objects_pages (HFID * hfid, int approximation,
					     DB_BIGINT * nobjs, int *npages);

extern int qp_get_server_info (SERVER_INFO * server_info);

extern int repl_log_get_eof_lsa (LOG_LSA * lsa);
extern int repl_set_info (REPL_INFO * repl_info);

extern int rbl_get_log_pages (RBL_SYNC_CONTEXT * ctx_ptr);

extern bool histo_is_supported (void);
extern int histo_start (bool for_all_trans);
extern int histo_stop (void);
extern void histo_print (FILE * stream);
extern void histo_print_global_stats (FILE * stream, bool cumulative,
				      const char *substr,
				      const char *db_name);
extern void histo_clear (void);

extern int net_histo_start (bool for_all_trans);
extern int net_histo_stop (void);
extern void net_histo_print (FILE * stream);
extern void net_histo_print_global_stats (FILE * stream, bool cumulative,
					  const char *substr,
					  const char *db_name);
extern void net_histo_clear (void);

extern int net_client_request_send_msg (unsigned int *eid, int request,
					int num_buffers, ...);
extern int net_client_request_send_msg_v (unsigned int *eid, int request,
					  int num_buffers, va_list args);
extern int net_client_data_send_msg (unsigned int eid, int num_buffers, ...);
extern int net_client_data_send_msg_v (unsigned int eid, int num_buffers,
				       va_list args);

extern int net_client_request_recv_msg (CSS_NET_PACKET ** recv_packet,
					unsigned int eid,
					int timeout, int num_buffers, ...);
extern int net_client_request_recv_msg_v (CSS_NET_PACKET ** recv_packet,
					  unsigned int eid, int timeout,
					  int num_buffers, va_list args);
extern int net_client_request (int request, CSS_NET_PACKET ** recv_packet,
			       int num_send_buffers, int num_recv_buffers,
			       ...);
extern int net_client_request_with_callback (int request,
					     char *argbuf, int argsize,
					     char *replybuf, int replysize,
					     CSS_NET_PACKET ** recv_packet);
extern int net_client_get_log_header (LOGWR_CONTEXT * ctx_ptr, char *argbuf,
				      int argsize, char *replybuf,
				      int replysize, char **logpg_area_buf);
extern int net_client_request_with_rbl_context (RBL_SYNC_CONTEXT * ctx_ptr,
						char *argbuf, int argsize,
						char *replybuf,
						int replysize);
extern int net_client_logwr_send_end_msg (unsigned int eid, int error);
extern int net_client_request_recv_stream (int request, char *argbuf,
					   int argsize, char *replybuf,
					   int replybuf_size, char *databuf,
					   int datasize, FILE * outfp);
extern int net_client_ping_server (int client_val, int *server_val,
				   int timeout);
extern int net_client_ping_server_with_handshake (int client_type,
						  bool check_capabilities,
						  int opt_cap,
						  RYE_VERSION * svr_version);

extern int net_client_init (const char *dbname, const char *hostname);
extern int net_client_final (void);

extern int net_client_send_data (char *host, unsigned int rc, char *databuf,
				 int datasize);

extern in_addr_t net_client_get_server_addr (void);

extern bool net_client_is_server_in_transaction (void);
extern short net_client_server_shard_nodeid (void);
extern bool net_client_reset_on_commit (void);
extern void net_client_set_ro_tran (bool ro_tran);

extern int boot_get_server_locales (LANG_COLL_COMPAT ** server_collations,
				    LANG_LOCALE_COMPAT ** server_locales,
				    int *server_coll_cnt,
				    int *server_locales_cnt);

/* session state API */
extern int csession_find_or_create_session (SESSION_ID * session_id,
					    char *server_session_key,
					    const char *db_user,
					    const char *host,
					    const char *program_name);
extern int csession_end_session (SESSION_ID session_id);

#if defined (ENABLE_UNUSED_FUNCTION)
extern int clogin_user (const char *username);
#endif
extern int logtb_update_group_id (int migrator_id, int group_id, int target,
				  int on_off);
extern int logtb_block_globl_dml (int start_or_end);
extern int bk_prepare_backup (int num_threads, int do_compress,
			      int sleep_msecs,
			      int make_slave, BK_BACKUP_SESSION * session);
extern int bk_backup_volume (BK_BACKUP_SESSION * session);
extern int bk_backup_log_volume (BK_BACKUP_SESSION * session,
				 int delete_unneeded_logarchives);

#endif /* _NETWORK_INTERFACE_CL_H_ */

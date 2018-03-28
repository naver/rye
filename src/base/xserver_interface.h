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
 * xserver_interface.h - Server interface functions
 *
 * Note: This file defines the interface to the server. The client modules that
 * calls any function in the server should include this module instead of the
 * header file of the desired function.
 */

#ifndef _XSERVER_INTERFACE_H_
#define _XSERVER_INTERFACE_H_

#ident "$Id$"

#include "config.h"

#include "error_manager.h"
#include "storage_common.h"
#include "boot.h"
#include "locator.h"
#include "log_comm.h"
#include "perf_monitor.h"
#include "query_list.h"
#include "file_io.h"
#include "thread.h"
#include "repl_log.h"
#include "query_manager.h"
extern int
xboot_initialize_server (THREAD_ENTRY * thread_p,
                         const BOOT_CLIENT_CREDENTIAL * client_credential,
                         BOOT_DB_PATH_INFO * db_path_info,
                         bool db_overwrite, const char *file_addmore_vols,
                         volatile DKNPAGES db_npages,
                         PGLENGTH db_desired_pagesize,
                         volatile DKNPAGES xlog_npages,
                         PGLENGTH db_desired_log_page_size,
                         OID * rootclass_oid, HFID * rootclass_hfid, int client_lock_wait);
extern const char *xboot_get_server_session_key (void);
extern int xboot_register_client (THREAD_ENTRY * thread_p,
                                  BOOT_CLIENT_CREDENTIAL * client_credential,
                                  int client_lock_wait,
                                  TRAN_STATE * tran_state, BOOT_SERVER_CREDENTIAL * server_credential);
extern int xboot_unregister_client (THREAD_ENTRY * thread_p, int tran_index);
extern VOLID xboot_add_volume_extension (THREAD_ENTRY * thread_p, DBDEF_VOL_EXT_INFO * ext_info);
extern int xboot_find_number_permanent_volumes (THREAD_ENTRY * thread_p);
extern int xboot_find_number_temp_volumes (THREAD_ENTRY * thread_p);
extern VOLID xboot_find_last_temp (THREAD_ENTRY * thread_p);
extern int xboot_find_number_bestspace_entries (THREAD_ENTRY * thread_p);

extern LC_FIND_CLASSNAME xlocator_reserve_class_names (THREAD_ENTRY *
                                                       thread_p,
                                                       const int num_classes,
                                                       const char **classname, const OID * class_oid);
extern LC_FIND_CLASSNAME xlocator_delete_class_name (THREAD_ENTRY * thread_p, const char *classname);
extern LC_FIND_CLASSNAME xlocator_rename_class_name (THREAD_ENTRY * thread_p,
                                                     const char *oldname, const char *newname, const OID * class_oid);
extern LC_FIND_CLASSNAME xlocator_find_class_oid (THREAD_ENTRY * thread_p,
                                                  const char *classname, OID * class_oid, LOCK lock);
extern int xlocator_assign_oid (THREAD_ENTRY * thread_p, const HFID * hfid,
                                OID * perm_oid, int expected_length, OID * class_oid, const char *classname);
extern int xlocator_fetch (THREAD_ENTRY * thrd, OID * oid, LOCK lock,
                           OID * class_oid, int prefetching, LC_COPYAREA ** fetch_area);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int xlocator_get_class (THREAD_ENTRY * thread_p, OID * class_oid,
                               const OID * oid, LOCK lock, int prefetching, LC_COPYAREA ** fetch_area);
#endif
extern int xlocator_fetch_all (THREAD_ENTRY * thread_p, const HFID * hfid,
                               LOCK * lock, OID * class_oid, INT64 * nobjects,
                               INT64 * nfetched, OID * last_oid, LC_COPYAREA ** fetch_area);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int xlocator_lock_and_fetch_all (THREAD_ENTRY * thread_p,
                                        const HFID * hfid,
                                        LOCK * instance_lock,
                                        int *instance_lock_timeout,
                                        OID * class_oid, LOCK * class_lock,
                                        int *nobjects, int *nfetched,
                                        int *nfailed_instance_locks, OID * last_oid, LC_COPYAREA ** fetch_area);
#endif
extern int xlocator_fetch_lockset (THREAD_ENTRY * thread_p, LC_LOCKSET * lockset, LC_COPYAREA ** fetch_area);
extern LC_FIND_CLASSNAME xlocator_find_lockhint_class_oids (THREAD_ENTRY *
                                                            thread_p,
                                                            int num_classes,
                                                            const char
                                                            **many_classnames,
                                                            LOCK * many_locks,
                                                            OID *
                                                            guessed_class_oids,
                                                            int
                                                            quit_on_errors,
                                                            LC_LOCKHINT ** hlock, LC_COPYAREA ** fetch_area);
extern int xlocator_fetch_lockhint_classes (THREAD_ENTRY * thread_p, LC_LOCKHINT * lockhint, LC_COPYAREA ** fetch_area);
extern int xlocator_force (THREAD_ENTRY * thread_p, LC_COPYAREA * copy_area);
extern int xlocator_repl_force (THREAD_ENTRY * thread_p, LC_COPYAREA * copy_area, LC_COPYAREA ** reply_area);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int xlocator_assign_oid_batch (THREAD_ENTRY * thread_p, LC_OIDSET * oidset);
#endif

extern void xrepl_log_get_eof_lsa (THREAD_ENTRY * thread_p, LOG_LSA * lsa);
extern int xrepl_set_info (THREAD_ENTRY * thread_p, REPL_INFO * repl_info);

extern int xheap_create (THREAD_ENTRY * thread_p, HFID * hfid, const OID * class_oid);
extern int xheap_destroy (THREAD_ENTRY * thread_p, const HFID * hfid);

extern TRAN_STATE xtran_server_commit (THREAD_ENTRY * thrd);
extern TRAN_STATE xtran_server_abort (THREAD_ENTRY * thrd);
extern int xtran_server_start_topop (THREAD_ENTRY * thread_p, LOG_LSA * topop_lsa);
extern TRAN_STATE xtran_server_end_topop (THREAD_ENTRY * thread_p, LOG_RESULT_TOPOP result, LOG_LSA * topop_lsa);
extern int xtran_server_savepoint (THREAD_ENTRY * thread_p, const char *savept_name, LOG_LSA * savept_lsa);
extern TRAN_STATE xtran_server_partial_abort (THREAD_ENTRY * thread_p, const char *savept_name, LOG_LSA * savept_lsa);
#if defined (ENABLE_UNUSED_FUNCTION)
extern bool xtran_is_blocked (THREAD_ENTRY * thread_p, int tran_index);
#endif
extern bool xtran_server_has_updated (THREAD_ENTRY * thread_p);
extern int xtran_server_is_active_and_has_updated (THREAD_ENTRY * thread_p);
extern int xtran_wait_server_active_trans (THREAD_ENTRY * thrd);

extern void xlogtb_set_interrupt (THREAD_ENTRY * thread_p, int set);
extern void xlogtb_set_suppress_repl_on_transaction (THREAD_ENTRY * thread_p, int set);

extern int xlogtb_reset_wait_msecs (THREAD_ENTRY * thread_p, int wait_msecs);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int xlogtb_reset_isolation (THREAD_ENTRY * thread_p, TRAN_ISOLATION isolation);

#if defined(SERVER_MODE) || defined(SA_MODE)
extern float log_get_db_compatibility (void);
#endif /* SERVER_MODE || SA_MODE */
#endif
extern bool logtb_has_updated (THREAD_ENTRY * thread_p);


extern BTID *xbtree_add_index (THREAD_ENTRY * thread_p, BTID * btid,
                               int num_atts, DB_TYPE * att_type, OID * class_oid, int attr_id);
extern int xbtree_load_data (THREAD_ENTRY * thread_p, BTID * btid, OID * class_oid, HFID * hfid);
extern int xbtree_delete_index (THREAD_ENTRY * thread_p, BTID * btid);
extern BTREE_SEARCH xbtree_find_unique (THREAD_ENTRY * thread_p,
                                        OID * class_oid, BTID * btid, DB_IDXKEY * key, OID * oid);
extern EHID *xehash_create (THREAD_ENTRY * thread_p, EHID * ehid,
                            DB_TYPE key_type, int exp_num_entries, OID * class_oid, int attr_id, bool is_tmp);
extern int xehash_destroy (THREAD_ENTRY * thread_p, EHID * ehid);

extern char *xstats_get_statistics_from_server (THREAD_ENTRY * thread_p,
                                                OID * class_id, unsigned int timestamp, int *length);
extern int xstats_update_statistics (THREAD_ENTRY * thread_p, OID * classoid, bool update_stats, bool with_fullscan);
extern int xstats_update_all_statistics (THREAD_ENTRY * thread_p, bool update_stats, bool with_fullscan);

extern DKNPAGES xdisk_get_total_numpages (THREAD_ENTRY * thread_p, VOLID volid);
extern char *xdisk_get_fullname (THREAD_ENTRY * thread_p, const char *database_name, VOLID volid, char *vol_fullname);
extern DISK_VOLPURPOSE xdisk_get_purpose (THREAD_ENTRY * thread_p, VOLID volid);
extern VOLID xdisk_get_purpose_and_space_info (THREAD_ENTRY * thread_p,
                                               VOLID volid, DISK_VOLPURPOSE * vol_purpose, VOL_SPACE_INFO * space_info);

extern int xqfile_get_list_file_page (THREAD_ENTRY * thread_p,
                                      QUERY_ID query_id, VOLID volid, PAGEID pageid, char *page_bufp, int *page_sizep);

/* new query interface */
extern XASL_ID *xqmgr_prepare_query (THREAD_ENTRY * thrd,
                                     COMPILE_CONTEXT * ctx, XASL_STREAM * stream, const OID * user_oid);

extern QFILE_LIST_ID *xqmgr_execute_query (THREAD_ENTRY * thrd,
                                           const XASL_ID * xasl_id,
                                           QUERY_ID * query_idp,
                                           int dbval_cnt,
                                           void *data,
                                           QUERY_FLAG * flagp,
                                           int query_timeout,
                                           int shard_groupid,
                                           DB_VALUE * shard_key, XASL_CACHE_ENTRY ** ret_cache_entry_p);
extern int xqmgr_end_query (THREAD_ENTRY * thrd, QUERY_ID query_id);
extern int xqmgr_drop_query_plan (THREAD_ENTRY * thread_p,
                                  const char *qstmt, const OID * user_oid, const XASL_ID * xasl_id);
extern int xqmgr_drop_all_query_plans (THREAD_ENTRY * thread_p);
extern void xqmgr_dump_query_plans (THREAD_ENTRY * thread_p, FILE * outfp);

extern int xqmgr_get_query_info (THREAD_ENTRY * thread_p, QUERY_ID query_id);

/* catalog manager interface */

extern int xcatalog_is_acceptable_new_representation (THREAD_ENTRY * thread_p,
                                                      OID * class_id, HFID * hfid, int *can_accept);

extern int xacl_reload (THREAD_ENTRY * thread_p);
extern void xacl_dump (THREAD_ENTRY * thread_p, FILE * outfp);
extern void xlock_dump (THREAD_ENTRY * thread_p, FILE * outfp);

extern int xlogtb_get_pack_tran_table (THREAD_ENTRY * thread_p,
                                       char **buffer_p, int *size_p, int include_query_exec_info);

extern int xsession_create_new (THREAD_ENTRY * thread_p, SESSION_KEY * key);
extern int xsession_check_session (THREAD_ENTRY * thread_p, const SESSION_KEY * key);
extern int xsession_set_session_key (THREAD_ENTRY * thread_p, const SESSION_KEY * key);

extern int xsession_end_session (THREAD_ENTRY * thread, const SESSION_KEY * key);

#if defined (ENABLE_UNUSED_FUNCTION)
extern int xlogin_user (THREAD_ENTRY * thread_p, const char *username);
#endif

extern void xsession_store_query_entry_info (THREAD_ENTRY * thread_p, QMGR_QUERY_ENTRY * qentry_p);
extern int xsession_load_query_entry_info (THREAD_ENTRY * thread_p, QMGR_QUERY_ENTRY * qentry_p);
extern int xsession_remove_query_entry_info (THREAD_ENTRY * thread_p, const QUERY_ID query_id);
extern int xsession_clear_query_entry_info (THREAD_ENTRY * thread_p, const QUERY_ID query_id);
extern bool xlogtb_exist_working_tran (UNUSED_ARG THREAD_ENTRY * thread_p, int group_id);
extern int xlogtb_update_group_id (THREAD_ENTRY * thread_p, int migrator_id, int group_id, int target, int on_off);
extern int xlogtb_block_global_dml (THREAD_ENTRY * thread_p, int start_or_end);

#endif /* _XSERVER_INTERFACE_H_ */

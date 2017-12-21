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
 *      network_interface_sr.h -Definitions for server network support.
 */

#ifndef _NETWORK_INTERFACE_SR_H_
#define _NETWORK_INTERFACE_SR_H_

#ident "$Id$"

#include "query_opfunc.h"	/* for VACOMM stuff */
#include "thread.h"

extern void return_error_to_client (THREAD_ENTRY * thread_p,
				    unsigned int rid);
extern int server_ping_with_handshake (THREAD_ENTRY * thread_p,
				       unsigned int rid, char *request,
				       int reqlen);
extern void server_ping (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, int reqlen);
extern void slocator_fetch (THREAD_ENTRY * thrd, unsigned int rid,
			    char *request, int reqlen);
#if defined(ENABLE_UNUSED_FUNCTION)
extern void slocator_get_class (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);
#endif

extern void slocator_fetch_all (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);
extern void slocator_force (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, int reqlen);
extern void slocator_repl_force (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void slocator_fetch_lockset (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void slocator_find_class_oid (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void slocator_reserve_classnames (THREAD_ENTRY * thread_p,
					 unsigned int rid, char *request,
					 int reqlen);
extern void slocator_delete_class_name (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void slocator_rename_class_name (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void slocator_assign_oid (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void sqst_server_get_statistics (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void slog_checkpoint (THREAD_ENTRY * thread_p, unsigned int rid,
			     char *request, int reqlen);
#if defined(ENABLE_UNUSED_FUNCTION)
extern void slogtb_has_updated (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);
#endif
extern void slogtb_set_interrupt (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void slogtb_set_suppress_repl_on_transaction (THREAD_ENTRY * thread_p,
						     unsigned int rid,
						     char *request,
						     int reqlen);
extern void slogtb_reset_wait_msecs (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void slogtb_reset_isolation (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
#endif
extern void slogpb_dump_stat (THREAD_ENTRY * thread_p, unsigned int rid,
			      char *request, int reqlen);
extern void sacl_reload (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, int reqlen);
extern void sacl_dump (THREAD_ENTRY * thread_p, unsigned int rid,
		       char *request, int reqlen);
extern void slock_dump (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, int reqlen);
extern void sheap_create (THREAD_ENTRY * thread_p, unsigned int rid,
			  char *request, int reqlen);
extern void sheap_destroy (THREAD_ENTRY * thread_p, unsigned int rid,
			   char *request, int reqlen);
extern void stran_server_commit (THREAD_ENTRY * thrd, unsigned int rid,
				 char *request, int reqlen);
extern void stran_server_abort (THREAD_ENTRY * thrd, unsigned int rid,
				char *request, int reqlen);
extern void stran_server_has_updated (THREAD_ENTRY * thread_p,
				      unsigned int rid, char *request,
				      int reqlen);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void stran_server_start_topop (THREAD_ENTRY * thread_p,
				      unsigned int rid, char *request,
				      int reqlen);
extern void stran_server_end_topop (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
#endif
extern void stran_server_savepoint (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void stran_server_partial_abort (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void stran_server_is_active_and_has_updated (THREAD_ENTRY * thread_p,
						    unsigned int rid,
						    char *request,
						    int reqlen);
extern void stran_wait_server_active_trans (THREAD_ENTRY * thread_p,
					    unsigned int rid, char *request,
					    int reqlen);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void stran_is_blocked (THREAD_ENTRY * thread_p, unsigned int rid,
			      char *request, int reqlen);
#endif
extern void sboot_initialize_server (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void sboot_register_client (THREAD_ENTRY * thread_p, unsigned int rid,
				   char *request, int reqlen);
extern void sboot_notify_unregister_client (THREAD_ENTRY * thread_p,
					    unsigned int rid, char *request,
					    int reqlen);
extern void sboot_add_volume_extension (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void sboot_find_number_permanent_volumes (THREAD_ENTRY * thread_p,
						 unsigned int rid,
						 char *request, int reqlen);
extern void sboot_find_number_temp_volumes (THREAD_ENTRY * thread_p,
					    unsigned int rid, char *request,
					    int reqlen);
extern void sboot_find_last_temp (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void sboot_find_number_bestspace_entries (THREAD_ENTRY * thread_p,
						 unsigned int rid,
						 char *request, int reqlen);
extern void sboot_get_server_state (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, UNUSED_ARG int reqlen);
extern void sboot_notify_ha_apply_state (THREAD_ENTRY * thread_p,
					 unsigned int rid, char *request,
					 int reqlen);
extern void sqst_update_statistics (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void sqst_update_all_statistics (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void sbtree_add_index (THREAD_ENTRY * thread_p, unsigned int rid,
			      char *request, int reqlen);
extern void sbtree_load_data (THREAD_ENTRY * thread_p, unsigned int rid,
			      char *request, int reqlen);
extern void sbtree_delete_index (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void sbtree_find_unique (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);
extern void sdisk_get_purpose_and_space_info (THREAD_ENTRY * thread_p,
					      unsigned int rid, char *request,
					      int reqlen);
extern void sdk_vlabel (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, int reqlen);
extern void sqfile_get_list_file_page (THREAD_ENTRY * thread_p,
				       unsigned int rid, char *request,
				       int reqlen);
extern void sqmgr_prepare_query (THREAD_ENTRY * thrd, unsigned int rid,
				 char *request, int reqlen);
extern void sqmgr_execute_query (THREAD_ENTRY * thrd, unsigned int rid,
				 char *request, int reqlen);
extern void sqmgr_end_query (THREAD_ENTRY * thrd, unsigned int rid,
			     char *request, int reqlen);
extern void sqmgr_drop_query_plan (THREAD_ENTRY * thread_p, unsigned int rid,
				   char *request, int reqlen);
extern void sqmgr_drop_all_query_plans (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void sqmgr_dump_query_plans (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void sqmgr_get_query_info (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void sct_can_accept_new_repr (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern int xs_receive_data_from_client (THREAD_ENTRY * thread_p,
					char **area, int *datasize,
					int timeout);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void slocator_assign_oid_batch (THREAD_ENTRY * thread_p,
				       unsigned int rid, char *request,
				       int reqlen);
#endif
extern void slocator_find_lockhint_class_oids (THREAD_ENTRY * thread_p,
					       unsigned int rid,
					       char *request, int reqlen);
extern void slocator_fetch_lockhint_classes (THREAD_ENTRY * thread_p,
					     unsigned int rid, char *request,
					     int reqlen);
extern void tm_restart_event_handler (unsigned int, char *, int);
extern void sthread_kill_tran_index (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void sthread_dump_cs_stat (THREAD_ENTRY * thread_p,
				  unsigned int rid, char *request,
				  int reqlen);
extern void sserver_stats_dump (THREAD_ENTRY * thread_p,
				unsigned int rid, char *request, int reqlen);
extern void slogtb_get_pack_tran_table (THREAD_ENTRY * thread_p,
					unsigned int rid, char *request,
					int reqlen);
extern void slogtb_dump_trantable (THREAD_ENTRY * thread_p, unsigned int rid,
				   char *request, int reqlen);

extern int xlog_send_log_pages_to_client (THREAD_ENTRY * thread_p,
					  char *logpb_area, int area_size,
					  INT64 first_pageid, int num_page,
					  int file_status);
extern int xlog_send_log_pages_to_migrator (THREAD_ENTRY * thread_p,
					    char *logpg_area, int area_size);
extern int xlog_get_page_request_with_reply (THREAD_ENTRY * thread_p,
					     LOG_PAGEID * fpageid_ptr);
extern void sheap_get_class_num_objs_and_pages (THREAD_ENTRY * thread_p,
						unsigned int rid,
						char *request, int reqlen);
extern void sqp_get_server_info (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, int reqlen);
extern void sprm_server_change_parameters (THREAD_ENTRY * thread_p,
					   unsigned int rid, char *request,
					   int reqlen);
extern void sprm_server_obtain_parameters (THREAD_ENTRY * thread_p,
					   unsigned int rid, char *request,
					   int reqlen);
extern void sprm_server_get_force_parameters (THREAD_ENTRY * thread_p,
					      unsigned int rid, char *request,
					      int reqlen);
extern void sprm_server_dump_parameters (THREAD_ENTRY * thread_p,
					 unsigned int rid, char *request,
					 int reqlen);
extern void srepl_set_info (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, int reqlen);
extern void srepl_log_get_eof_lsa (THREAD_ENTRY * thread_p,
				   unsigned int rid, char *request,
				   int reqlen);
extern void slogwr_get_log_pages (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);

extern void ssession_find_or_create_session (THREAD_ENTRY * thread_p,
					     unsigned int rid,
					     char *request, int reqlen);
extern void ssession_end_session (THREAD_ENTRY * thread_p, unsigned int rid,
				  char *request, int reqlen);
extern void ssession_create_prepared_statement (THREAD_ENTRY * thread_p,
						unsigned int rid,
						char *request, int reqlen);
extern void ssession_get_prepared_statement (THREAD_ENTRY * thread_p,
					     unsigned int rid, char *request,
					     int reqlen);
extern void ssession_delete_prepared_statement (THREAD_ENTRY * thread_p,
						unsigned int rid,
						char *request, int reqlen);
extern void sboot_get_locales_info (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void slocator_lock_system_ddl_lock (THREAD_ENTRY * thread_p,
					   unsigned int rid, char *request,
					   int reqlen);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void slock_change_class_xlock_to_ulock (THREAD_ENTRY * thread_p,
					       unsigned int rid,
					       char *request, int reqlen);
#endif
extern void smigrator_get_log_pages (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void slogtb_update_group_id (THREAD_ENTRY * thread_p, unsigned int rid,
				    char *request, int reqlen);
extern void slogtb_block_global_dml (THREAD_ENTRY * thread_p,
				     unsigned int rid, char *request,
				     int reqlen);
extern void sbk_prepare_backup (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, int reqlen);
extern void sbk_backup_volume (THREAD_ENTRY * thread_p, unsigned int rid,
			       char *request, int reqlen);
extern void sbk_backup_log_volume (THREAD_ENTRY * thread_p, unsigned int rid,
				   char *request, int reqlen);
#endif /* _NETWORK_INTERFACE_SR_H_ */

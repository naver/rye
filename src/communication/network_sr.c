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
 * network_sr.c - server side support functions.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>

#if defined(CS_MODE)
#include "server_interface.h"
#else
#include "xserver_interface.h"
#endif
#include "memory_alloc.h"
#include "system_parameter.h"
#include "network.h"
#include "boot_sr.h"
#include "network_interface_sr.h"
#include "query_list.h"
#include "thread.h"
#include "critical_section.h"
#include "release_string.h"
#include "server_support.h"
#include "connection_sr.h"
#include "connection_error.h"
#include "message_catalog.h"
#include "log_impl.h"
#include "event_log.h"
#include "util_func.h"

#include "rye_server_shm.h"

#include "fault_injection.h"

enum net_req_act
{
  CHECK_DB_MODIFICATION = 0x0001,
  CHECK_AUTHORIZATION = 0x0002,
  IN_TRANSACTION = 0x0008,
  OUT_TRANSACTION = 0x0010,
};
typedef void (*net_server_func) (THREAD_ENTRY * thrd, unsigned int rid,
				 char *request, int reqlen);
struct net_request
{
  int action_attribute;
  net_server_func processing_function;
  const char *name;
  int request_count;
  int total_size_sent;
  int total_size_received;
  int elapsed_time;
};

static struct net_request net_Requests[NET_SERVER_REQUEST_END];

static int net_Histo_call_count = 0;

#if defined(RYE_DEBUG)
static void net_server_histo_print (void);
static void net_server_histo_add_entry (int request, int data_sent);
#endif /* RYE_DEBUG */

static void net_server_init (void);

/*
 * net_server_init () -
 *   return:
 */
static void
net_server_init (void)
{
  struct net_request *req_p;
  unsigned int i;

  net_Histo_call_count = 0;

  for (i = 0; i < DIM (net_Requests); i++)
    {
      net_Requests[i].action_attribute = 0;
      net_Requests[i].processing_function = NULL;
      net_Requests[i].name = "";
      net_Requests[i].request_count = 0;
      net_Requests[i].total_size_sent = 0;
      net_Requests[i].total_size_received = 0;
      net_Requests[i].elapsed_time = 0;
    }

  /* ping */
  req_p = &net_Requests[NET_SERVER_PING];
  req_p->processing_function = server_ping;
  req_p->name = "NET_SERVER_PING";

  /* boot */
  req_p = &net_Requests[NET_SERVER_BO_INIT_SERVER];
  req_p->processing_function = sboot_initialize_server;
  req_p->name = "NET_SERVER_BO_INIT_SERVER";

  req_p = &net_Requests[NET_SERVER_BO_REGISTER_CLIENT];
  req_p->processing_function = sboot_register_client;
  req_p->name = "NET_SERVER_BO_REGISTER_CLIENT";

  req_p = &net_Requests[NET_SERVER_BO_UNREGISTER_CLIENT];
  req_p->processing_function = sboot_notify_unregister_client;
  req_p->name = "NET_SERVER_BO_UNREGISTER_CLIENT";

  req_p = &net_Requests[NET_SERVER_BO_ADD_VOLEXT];
  req_p->action_attribute = (CHECK_AUTHORIZATION | IN_TRANSACTION);
  req_p->processing_function = sboot_add_volume_extension;
  req_p->name = "NET_SERVER_BO_ADD_VOLEXT";

  req_p = &net_Requests[NET_SERVER_BO_FIND_NPERM_VOLS];
  req_p->processing_function = sboot_find_number_permanent_volumes;
  req_p->name = "NET_SERVER_BO_FIND_NPERM_VOLS";

  req_p = &net_Requests[NET_SERVER_BO_FIND_NTEMP_VOLS];
  req_p->processing_function = sboot_find_number_temp_volumes;
  req_p->name = "NET_SERVER_BO_FIND_NTEMP_VOLS";

  req_p = &net_Requests[NET_SERVER_BO_FIND_LAST_TEMP];
  req_p->processing_function = sboot_find_last_temp;
  req_p->name = "NET_SERVER_BO_FIND_LAST_TEMP";

  req_p = &net_Requests[NET_SERVER_BO_FIND_NBEST_ENTRIES];
  req_p->processing_function = sboot_find_number_bestspace_entries;
  req_p->name = "NET_SERVER_BO_FIND_NBEST_ENTRIES";

  req_p = &net_Requests[NET_SERVER_BO_GET_SERVER_STATE];
  req_p->action_attribute = (CHECK_AUTHORIZATION);
  req_p->processing_function = sboot_get_server_state;
  req_p->name = "NET_SERVER_BO_GET_SERVER_STATE";

  req_p = &net_Requests[NET_SERVER_BO_NOTIFY_HA_APPLY_STATE];
  req_p->action_attribute = (CHECK_AUTHORIZATION | IN_TRANSACTION);
  req_p->processing_function = sboot_notify_ha_apply_state;
  req_p->name = "NET_SERVER_BO_NOTIFY_HA_APPLY_STATE";

  req_p = &net_Requests[NET_SERVER_BO_GET_LOCALES_INFO];
  req_p->processing_function = sboot_get_locales_info;
  req_p->name = "NET_SERVER_BO_GET_LOCALES_INFO";

  /* transaction */
  req_p = &net_Requests[NET_SERVER_TM_SERVER_COMMIT];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | OUT_TRANSACTION);
  req_p->processing_function = stran_server_commit;
  req_p->name = "NET_SERVER_TM_SERVER_COMMIT";

  req_p = &net_Requests[NET_SERVER_TM_SERVER_ABORT];
  req_p->action_attribute = (OUT_TRANSACTION);
  req_p->processing_function = stran_server_abort;
  req_p->name = "NET_SERVER_TM_SERVER_ABORT";

#if defined (ENABLE_UNUSED_FUNCTION)
  req_p = &net_Requests[NET_SERVER_TM_SERVER_START_TOPOP];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = stran_server_start_topop;
  req_p->name = "NET_SERVER_TM_SERVER_START_TOPOP";

  req_p = &net_Requests[NET_SERVER_TM_SERVER_END_TOPOP];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = stran_server_end_topop;
  req_p->name = "NET_SERVER_TM_SERVER_END_TOPOP";
#endif

  req_p = &net_Requests[NET_SERVER_TM_SERVER_SAVEPOINT];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = stran_server_savepoint;
  req_p->name = "NET_SERVER_TM_SERVER_SAVEPOINT";

  req_p = &net_Requests[NET_SERVER_TM_SERVER_PARTIAL_ABORT];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = stran_server_partial_abort;
  req_p->name = "NET_SERVER_TM_SERVER_PARTIAL_ABORT";

  req_p = &net_Requests[NET_SERVER_TM_SERVER_HAS_UPDATED];
  req_p->processing_function = stran_server_has_updated;
  req_p->name = "NET_SERVER_TM_SERVER_HAS_UPDATED";

  req_p = &net_Requests[NET_SERVER_TM_SERVER_ISACTIVE_AND_HAS_UPDATED];
  req_p->processing_function = stran_server_is_active_and_has_updated;
  req_p->name = "NET_SERVER_TM_SERVER_ISACTIVE_AND_HAS_UPDATED";

#if defined (ENABLE_UNUSED_FUNCTION)
  req_p = &net_Requests[NET_SERVER_TM_ISBLOCKED];
  req_p->processing_function = stran_is_blocked;
  req_p->name = "NET_SERVER_TM_ISBLOCKED";
#endif

  req_p = &net_Requests[NET_SERVER_TM_WAIT_SERVER_ACTIVE_TRANS];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = stran_wait_server_active_trans;
  req_p->name = "NET_SERVER_TM_WAIT_SERVER_ACTIVE_TRANS";

  /* locator */
  req_p = &net_Requests[NET_SERVER_LC_FETCH];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = slocator_fetch;
  req_p->name = "NET_SERVER_LC_FETCH";

  req_p = &net_Requests[NET_SERVER_LC_FETCHALL];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = slocator_fetch_all;
  req_p->name = "NET_SERVER_LC_FETCHALL";

  req_p = &net_Requests[NET_SERVER_LC_FETCH_LOCKSET];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = slocator_fetch_lockset;
  req_p->name = "NET_SERVER_LC_FETCH_LOCKSET";

#if defined (ENABLE_UNUSED_FUNCTION)
  req_p = &net_Requests[NET_SERVER_LC_GET_CLASS];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = slocator_get_class;
  req_p->name = "NET_SERVER_LC_GET_CLASS";
#endif

  req_p = &net_Requests[NET_SERVER_LC_FIND_CLASSOID];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = slocator_find_class_oid;
  req_p->name = "NET_SERVER_LC_FIND_CLASSOID";

  req_p = &net_Requests[NET_SERVER_LC_FORCE];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = slocator_force;
  req_p->name = "NET_SERVER_LC_FORCE";

  req_p = &net_Requests[NET_SERVER_LC_RESERVE_CLASSNAME];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = slocator_reserve_classnames;
  req_p->name = "NET_SERVER_LC_RESERVE_CLASSNAME";

  req_p = &net_Requests[NET_SERVER_LC_DELETE_CLASSNAME];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = slocator_delete_class_name;
  req_p->name = "NET_SERVER_LC_DELETE_CLASSNAME";

  req_p = &net_Requests[NET_SERVER_LC_RENAME_CLASSNAME];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = slocator_rename_class_name;
  req_p->name = "NET_SERVER_LC_RENAME_CLASSNAME";

  req_p = &net_Requests[NET_SERVER_LC_ASSIGN_OID];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = slocator_assign_oid;
  req_p->name = "NET_SERVER_LC_ASSIGN_OID";

  req_p = &net_Requests[NET_SERVER_LC_FIND_LOCKHINT_CLASSOIDS];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = slocator_find_lockhint_class_oids;
  req_p->name = "NET_SERVER_LC_FIND_LOCKHINT_CLASSOIDS";

  req_p = &net_Requests[NET_SERVER_LC_FETCH_LOCKHINT_CLASSES];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = slocator_fetch_lockhint_classes;
  req_p->name = "NET_SERVER_LC_FETCH_LOCKHINT_CLASSES";

#if defined (ENABLE_UNUSED_FUNCTION)
  req_p = &net_Requests[NET_SERVER_LC_ASSIGN_OID_BATCH];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = slocator_assign_oid_batch;
  req_p->name = "NET_SERVER_LC_ASSIGN_OID_BATCH";
#endif

  /* heap */
  req_p = &net_Requests[NET_SERVER_HEAP_CREATE];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = sheap_create;
  req_p->name = "NET_SERVER_HEAP_CREATE";

  req_p = &net_Requests[NET_SERVER_HEAP_DESTROY];
  req_p->action_attribute = CHECK_DB_MODIFICATION | IN_TRANSACTION;
  req_p->processing_function = sheap_destroy;
  req_p->name = "NET_SERVER_HEAP_DESTROY";

  req_p = &net_Requests[NET_SERVER_HEAP_GET_CLASS_NOBJS_AND_NPAGES];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = sheap_get_class_num_objs_and_pages;
  req_p->name = "NET_SERVER_HEAP_GET_CLASS_NOBJS_AND_NPAGES";

  /* log */
  req_p = &net_Requests[NET_SERVER_LOG_RESET_WAIT_MSECS];
  req_p->processing_function = slogtb_reset_wait_msecs;
  req_p->name = "NET_SERVER_LOG_RESET_WAIT_MSECS";

#if defined (ENABLE_UNUSED_FUNCTION)
  req_p = &net_Requests[NET_SERVER_LOG_RESET_ISOLATION];
  req_p->processing_function = slogtb_reset_isolation;
  req_p->name = "NET_SERVER_LOG_RESET_ISOLATION";
#endif

  req_p = &net_Requests[NET_SERVER_LOG_SET_INTERRUPT];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = slogtb_set_interrupt;
  req_p->name = "NET_SERVER_LOG_SET_INTERRUPT";

  req_p = &net_Requests[NET_SERVER_LOG_DUMP_STAT];
  req_p->processing_function = slogpb_dump_stat;
  req_p->name = "NET_SERVER_LOG_DUMP_STAT";

  req_p = &net_Requests[NET_SERVER_LOG_GETPACK_TRANTB];
  req_p->processing_function = slogtb_get_pack_tran_table;
  req_p->name = "NET_SERVER_LOG_GETPACK_TRANTB";

  req_p = &net_Requests[NET_SERVER_LOG_DUMP_TRANTB];
  req_p->processing_function = slogtb_dump_trantable;
  req_p->name = "NET_SERVER_LOG_DUMP_TRANTB";

  req_p = &net_Requests[NET_SERVER_LOG_CHECKPOINT];
  req_p->processing_function = slog_checkpoint;
  req_p->name = "NET_SERVER_LOG_CHECKPOINT";

  req_p = &net_Requests[NET_SERVER_LOG_SET_SUPPRESS_REPL_ON_TRANSACTION];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = slogtb_set_suppress_repl_on_transaction;
  req_p->name = "NET_SERVER_LOG_SET_SUPPRESS_REPL_ON_TRANSACTION";

  /* lock */
  req_p = &net_Requests[NET_SERVER_LK_DUMP];
  req_p->processing_function = slock_dump;
  req_p->name = "NET_SERVER_LK_DUMP";

  /* b-tree */
  req_p = &net_Requests[NET_SERVER_BTREE_ADD_INDEX];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = sbtree_add_index;
  req_p->name = "NET_SERVER_BTREE_ADD_INDEX";

  req_p = &net_Requests[NET_SERVER_BTREE_DEL_INDEX];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = sbtree_delete_index;
  req_p->name = "NET_SERVER_BTREE_DEL_INDEX";

  req_p = &net_Requests[NET_SERVER_BTREE_LOAD_DATA];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = sbtree_load_data;
  req_p->name = "NET_SERVER_BTREE_LOAD_DATA";

  req_p = &net_Requests[NET_SERVER_BTREE_FIND_UNIQUE];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = sbtree_find_unique;
  req_p->name = "NET_SERVER_BTREE_FIND_UNIQUE";

  /* disk */
  req_p = &net_Requests[NET_SERVER_DISK_GET_PURPOSE_AND_SPACE_INFO];
  req_p->processing_function = sdisk_get_purpose_and_space_info;
  req_p->name = "NET_SERVER_DISK_GET_PURPOSE_AND_SPACE_INFO";

  req_p = &net_Requests[NET_SERVER_DISK_VLABEL];
  req_p->processing_function = sdk_vlabel;
  req_p->name = "NET_SERVER_DISK_VLABEL";

  /* statistics */
  req_p = &net_Requests[NET_SERVER_QST_GET_STATISTICS];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = sqst_server_get_statistics;
  req_p->name = "NET_SERVER_QST_GET_STATISTICS";

  req_p = &net_Requests[NET_SERVER_QST_UPDATE_STATISTICS];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = sqst_update_statistics;
  req_p->name = "NET_SERVER_QST_UPDATE_STATISTICS";

  req_p = &net_Requests[NET_SERVER_QST_UPDATE_ALL_STATISTICS];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = sqst_update_all_statistics;
  req_p->name = "NET_SERVER_QST_UPDATE_ALL_STATISTICS";

  /* query manager */
  req_p = &net_Requests[NET_SERVER_QM_QUERY_PREPARE];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = sqmgr_prepare_query;
  req_p->name = "NET_SERVER_QM_QUERY_PREPARE";

  req_p = &net_Requests[NET_SERVER_QM_QUERY_EXECUTE];
  req_p->action_attribute = (IN_TRANSACTION);
  req_p->processing_function = sqmgr_execute_query;
  req_p->name = "NET_SERVER_QM_QUERY_EXECUTE";

  req_p = &net_Requests[NET_SERVER_QM_QUERY_END];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = sqmgr_end_query;
  req_p->name = "NET_SERVER_QM_QUERY_END";

  req_p = &net_Requests[NET_SERVER_QM_QUERY_DROP_PLAN];
  req_p->processing_function = sqmgr_drop_query_plan;
  req_p->name = "NET_SERVER_QM_QUERY_DROP_PLAN";

  req_p = &net_Requests[NET_SERVER_QM_QUERY_DROP_ALL_PLANS];
  req_p->processing_function = sqmgr_drop_all_query_plans;
  req_p->name = "NET_SERVER_QM_QUERY_DROP_ALL_PLANS";

  req_p = &net_Requests[NET_SERVER_QM_GET_QUERY_INFO];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = sqmgr_get_query_info;
  req_p->name = "NET_SERVER_QM_GET_QUERY_INFO";

  req_p = &net_Requests[NET_SERVER_QM_QUERY_DUMP_PLANS];
  req_p->processing_function = sqmgr_dump_query_plans;
  req_p->name = "NET_SERVER_QM_QUERY_DUMP_PLANS";

  /* query file */
  req_p = &net_Requests[NET_SERVER_LS_GET_LIST_FILE_PAGE];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = sqfile_get_list_file_page;
  req_p->name = "NET_SERVER_LS_GET_LIST_FILE_PAGE";

  /* catalog */
  req_p = &net_Requests[NET_SERVER_CT_CAN_ACCEPT_NEW_REPR];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = sct_can_accept_new_repr;
  req_p->name = "NET_SERVER_CT_CAN_ACCEPT_NEW_REPR";

  /* thread */
  req_p = &net_Requests[NET_SERVER_CSS_KILL_TRANSACTION];
  req_p->action_attribute = CHECK_AUTHORIZATION;
  req_p->processing_function = sthread_kill_tran_index;
  req_p->name = "NET_SERVER_CSS_KILL_TRANSACTION";

  req_p = &net_Requests[NET_SERVER_CSS_DUMP_CS_STAT];
  req_p->action_attribute = CHECK_AUTHORIZATION;
  req_p->processing_function = sthread_dump_cs_stat;
  req_p->name = "NET_SERVER_CSS_DUMP_CS_STAT";

  req_p = &net_Requests[NET_SERVER_CSS_DUMP_SERVER_STAT];
  req_p->action_attribute = CHECK_AUTHORIZATION;
  req_p->processing_function = sserver_stats_dump;
  req_p->name = "NET_SERVER_CSS_DUMP_SERVER_STAT";

  /* query processing */
  req_p = &net_Requests[NET_SERVER_QPROC_GET_SERVER_INFO];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = sqp_get_server_info;
  req_p->name = "NET_SERVER_QPROC_GET_SERVER_INFO";

  /* parameter */
  req_p = &net_Requests[NET_SERVER_PRM_SET_PARAMETERS];
  req_p->processing_function = sprm_server_change_parameters;
  req_p->name = "NET_SERVER_PRM_SET_PARAMETERS";

  req_p = &net_Requests[NET_SERVER_PRM_GET_PARAMETERS];
  req_p->processing_function = sprm_server_obtain_parameters;
  req_p->name = "NET_SERVER_PRM_GET_PARAMETERS";

  req_p = &net_Requests[NET_SERVER_PRM_GET_FORCE_PARAMETERS];
  req_p->processing_function = sprm_server_get_force_parameters;
  req_p->name = "NET_SERVER_PRM_GET_FORCE_PARAMETERS";

  req_p = &net_Requests[NET_SERVER_PRM_DUMP_PARAMETERS];
  req_p->processing_function = sprm_server_dump_parameters;
  req_p->name = "NET_SERVER_PRM_DUMP_PARAMETERS";

  /* replication */
  req_p = &net_Requests[NET_SERVER_REPL_INFO];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = srepl_set_info;
  req_p->name = "NET_SERVER_REPL_INFO";

  req_p = &net_Requests[NET_SERVER_REPL_LOG_GET_EOF_LSA];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = srepl_log_get_eof_lsa;
  req_p->name = "NET_SERVER_REPL_LOG_GET_EOF_LSA";

  /* log writer */
  req_p = &net_Requests[NET_SERVER_LOGWR_GET_LOG_PAGES];
  req_p->processing_function = slogwr_get_log_pages;
  req_p->name = "NET_SERVER_LOGWR_GET_LOG_PAGES";

  /* shutdown */
  req_p = &net_Requests[NET_SERVER_SHUTDOWN];
  req_p->action_attribute = CHECK_AUTHORIZATION;
  req_p->name = "NET_SERVER_SHUTDOWN";

  /* session state */
  req_p = &net_Requests[NET_SERVER_SES_CHECK_SESSION];
  req_p->processing_function = ssession_find_or_create_session;
  req_p->name = "NET_SERVER_SES_CHECK_SESSION";

  req_p = &net_Requests[NET_SERVER_SES_END_SESSION];
  req_p->processing_function = ssession_end_session;
  req_p->name = "NET_SERVER_END_SESSION";

  /* ip control */
  req_p = &net_Requests[NET_SERVER_ACL_DUMP];
  req_p->processing_function = sacl_dump;
  req_p->name = "NET_SERVER_ACL_DUMP";

  req_p = &net_Requests[NET_SERVER_ACL_RELOAD];
  req_p->processing_function = sacl_reload;
  req_p->name = "NET_SERVER_ACL_RELOAD";

  req_p = &net_Requests[NET_SERVER_LC_REPL_FORCE];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = slocator_repl_force;
  req_p->name = "NET_SERVER_LC_REPL_FORCE";

  req_p = &net_Requests[NET_SERVER_LC_LOCK_SYSTEM_DDL_LOCK];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = slocator_lock_system_ddl_lock;
  req_p->name = "NET_SERVER_LC_LOCK_SYSTEM_DDL_LOCK";

#if defined (ENABLE_UNUSED_FUNCTION)
  req_p = &net_Requests[NET_SERVER_LC_LOCK_CHANGE_CLASS_XLOCK_TO_ULOCK];
  req_p->action_attribute = (CHECK_DB_MODIFICATION | IN_TRANSACTION);
  req_p->processing_function = slock_change_class_xlock_to_ulock;
  req_p->name = "NET_SERVER_LC_LOCK_CHANGE_CLASS_XLOCK_TO_ULOCK";
#endif

  /* group migrator */
  req_p = &net_Requests[NET_SERVER_MIGRATOR_GET_LOG_PAGES];
  req_p->processing_function = smigrator_get_log_pages;
  req_p->name = "NET_SERVER_MIGRATOR_GET_LOG_PAGES";

  req_p = &net_Requests[NET_SERVER_UPDATE_GROUP_ID];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = slogtb_update_group_id;
  req_p->name = "NET_SERVER_UPDATE_GROUP_ID";

  req_p = &net_Requests[NET_SERVER_BLOCK_GLOBAL_DML];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = slogtb_block_global_dml;
  req_p->name = "NET_SERVER_BLOCK_GLOBAL_DML";

  req_p = &net_Requests[NET_SERVER_BK_PREPARE_BACKUP];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = sbk_prepare_backup;
  req_p->name = "NET_SERVER_BK_PREPARE_BACKUP";

  req_p = &net_Requests[NET_SERVER_BK_BACKUP_VOLUME];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = sbk_backup_volume;
  req_p->name = "NET_SERVER_BK_BACKUP_VOLUME";

  req_p = &net_Requests[NET_SERVER_BK_BACKUP_LOG_VOLUME];
  req_p->action_attribute = IN_TRANSACTION;
  req_p->processing_function = sbk_backup_log_volume;
  req_p->name = "NET_SERVER_BK_BACKUP_LOG_VOLUME";
}

#if defined(RYE_DEBUG)
/*
 * net_server_histo_print () -
 *   return:
 */
static void
net_server_histo_print (void)
{
  unsigned int i, found = 0, total_requests = 0, total_size_sent = 0;
  int total_size_received = 0;
  float server_time, total_server_time = 0;
  float avg_response_time, avg_client_time;

  fprintf (stdout, "\nHistogram of client requests:\n");
  fprintf (stdout, "%-31s %6s  %10s %10s , %10s \n",
	   "Name", "Rcount", "Sent size", "Recv size", "Server time");

  for (i = 0; i < DIM (net_Requests); i++)
    {
      if (net_Requests[i].request_count)
	{
	  found = 1;
	  server_time = ((float) net_Requests[i].elapsed_time / 1000000 /
			 (float) (net_Requests[i].request_count));
	  fprintf (stdout, "%-29s %6d X %10d+%10d b, %10.6f s\n",
		   net_Requests[i].name, net_Requests[i].request_count,
		   net_Requests[i].total_size_sent,
		   net_Requests[i].total_size_received, server_time);
	  total_requests += net_Requests[i].request_count;
	  total_size_sent += net_Requests[i].total_size_sent;
	  total_size_received += net_Requests[i].total_size_received;
	  total_server_time += (server_time * net_Requests[i].request_count);
	}
    }

  if (!found)
    {
      fprintf (stdout, " No server requests made\n");
    }
  else
    {
      fprintf (stdout,
	       "-------------------------------------------------------------"
	       "--------------\n");
      fprintf (stdout,
	       "Totals:                       %6d X %10d+%10d b  "
	       "%10.6f s\n", total_requests, total_size_sent,
	       total_size_received, total_server_time);
      avg_response_time = total_server_time / total_requests;
      avg_client_time = 0.0;
      fprintf (stdout, "\n Average server response time = %6.6f secs \n"
	       " Average time between client requests = %6.6f secs \n",
	       avg_response_time, avg_client_time);
    }
}

/*
 * net_server_histo_add_entry () -
 *   return:
 *   request(in):
 *   data_sent(in):
 */
static void
net_server_histo_add_entry (int request, int data_sent)
{
  net_Requests[request].request_count++;
  net_Requests[request].total_size_sent += data_sent;

  net_Histo_call_count++;
}
#endif /* RYE_DEBUG */

/*
 * net_server_request () - The main server request dispatch handler
 *   return: error status
 *   thrd(in): this thread handle
 *   rid(in): CSS request id
 *   request(in): request constant
 *   size(in): size of argument buffer
 *   buffer(in): argument buffer
 */
int
net_server_request (THREAD_ENTRY * thread_p, unsigned int rid, int request,
		    int size, char *buffer)
{
  net_server_func func;
  int status = CSS_NO_ERRORS;
  int error_code;
  CSS_CONN_ENTRY *conn;
  int popped = NO_ERROR;

  popped = NO_ERROR;		/* init */
  while (popped == NO_ERROR)
    {
      thread_mnt_track_pop (thread_p, &popped);
      assert (popped == ER_FAILED);	/* mnt_track stack must be empty */
    }

  if (buffer == NULL && size > 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_CANT_ALLOC_BUFFER, 0);
      return_error_to_client (thread_p, rid);
      css_send_abort_to_client (thread_p->conn_entry, rid);
      status = CSS_UNPLANNED_SHUTDOWN;
      goto end;
    }

  /* handle some special requests */
  if (request == NET_SERVER_PING_WITH_HANDSHAKE)
    {
      status = server_ping_with_handshake (thread_p, rid, buffer, size);
      if (status != CSS_NO_ERRORS)
	{
	  css_send_abort_to_client (thread_p->conn_entry, rid);
	}
      goto end;
    }
  else if (request == NET_SERVER_SHUTDOWN)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_NET_SERVER_SHUTDOWN, 0);
      /* When this actually does a shutdown, change to CSS_PLANNED_SHUTDOWN */
      status = CSS_UNPLANNED_SHUTDOWN;
      goto end;
    }

  if (request <= NET_SERVER_REQUEST_START
      || request >= NET_SERVER_REQUEST_END)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_NET_UNKNOWN_SERVER_REQ,
	      0);
      return_error_to_client (thread_p, rid);
      css_send_abort_to_client (thread_p->conn_entry, rid);
      goto end;
    }
#if defined(RYE_DEBUG)
  net_server_histo_add_entry (request, size);
#endif /* RYE_DEBUG */
  conn = thread_p->conn_entry;
  assert (conn != NULL);
  /* check if the conn is valid */
  if (IS_INVALID_SOCKET (conn->fd) || conn->status != CONN_OPEN)
    {
      /* have nothing to do because the client has gone */
      goto end;
    }

  if ((svr_shm_get_server_state () == HA_STATE_MASTER)
      && (logtb_find_current_client_type (thread_p)
	  == BOOT_CLIENT_SLAVE_ONLY_BROKER))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      "active && slave only broker: reset connection\n");
      return_error_to_client (thread_p, rid);
      css_send_abort_to_client (thread_p->conn_entry, rid);

      goto end;
    }

  /* check the defined action attribute */
  if (net_Requests[request].action_attribute & CHECK_DB_MODIFICATION)
    {
      bool check = true;

      if (request == NET_SERVER_TM_SERVER_COMMIT
	  && !logtb_has_updated (thread_p))
	{
	  check = false;
	}

      /* check if DB modification is allowed */
      if (check == true)
	{
	  CHECK_MODIFICATION_NO_RETURN (thread_p, error_code);
	  if (error_code != NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "net_server_request(): CHECK_DB_MODIFICATION error"
			    " request %s\n", net_Requests[request].name);
	      return_error_to_client (thread_p, rid);
	      css_send_abort_to_client (conn, rid);
	      goto end;
	    }
	}
    }
  if (net_Requests[request].action_attribute & CHECK_AUTHORIZATION)
    {
      if (!logtb_am_i_dba_client (thread_p))
	{
	  er_log_debug (ARG_FILE_LINE,
			"net_server_request(): CHECK_AUTHORIZATION error"
			" request %s\n", net_Requests[request].name);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_DBA_ONLY, 1, "");
	  return_error_to_client (thread_p, rid);
	  css_send_abort_to_client (conn, rid);
	  goto end;
	}
    }
  if (net_Requests[request].action_attribute & IN_TRANSACTION)
    {
      conn->is_server_in_tran = true;
    }

  /* call a request processing function */
  if (thread_p->tran_index > 0)
    {
      mnt_stats_counter (thread_p, MNT_STATS_NETWORK_REQUESTS, 1);
    }
  func = net_Requests[request].processing_function;
  assert (func != NULL);
  if (func)
    {
      if ((net_Requests[request].action_attribute & IN_TRANSACTION)
	  || (net_Requests[request].action_attribute & OUT_TRANSACTION))
	{
	  logtb_start_transaction_if_needed (thread_p);
	}

      FI_INIT_THREAD (thread_p);

      (*func) (thread_p, rid, buffer, size);

      /* defense code: let other threads continue.
       */
      pgbuf_unfix_all (thread_p);
    }

end:

  popped = NO_ERROR;		/* init */
  while (popped == NO_ERROR)
    {
      thread_mnt_track_pop (thread_p, &popped);
      assert (popped == ER_FAILED);	/* mnt_track stack must be empty */
    }

  db_reset_private_heap (thread_p);

  return (status);
}

/*
 * net_server_conn_down () - CSS callback function used when a connection to a
 *                       particular client went down
 *   return: 0
 *   arg(in): transaction id
 */
int
net_server_conn_down (THREAD_ENTRY * thread_p, CSS_THREAD_ARG arg,
		      bool interrupt_only)
{
  int tran_index;
  CSS_CONN_ENTRY *conn_p;
  int prev_thrd_cnt, thrd_cnt;
  bool continue_check;
  int client_id;
//  int local_tran_index;
  THREAD_ENTRY *suspended_p;

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
      if (thread_p == NULL)
	{
	  return 0;
	}
    }

//  local_tran_index = thread_p->tran_index;

  conn_p = (CSS_CONN_ENTRY *) arg;
  tran_index = conn_p->tran_index;
  client_id = conn_p->client_id;

  thread_set_info (thread_p, client_id, 0, tran_index);

  /* avoid infinite waiting with xtran_wait_server_active_trans() */
  thread_p->status = TS_CHECK;

  if (tran_index == NULL_TRAN_INDEX)
    {
      goto conn_down_end1;
    }

loop:
  prev_thrd_cnt = thread_has_threads (thread_p, tran_index, client_id);
  if (prev_thrd_cnt > 0)
    {
      if (tran_index == NULL_TRAN_INDEX)
	{
	  /* the connected client does not yet finished boot_client_register */
	  thread_sleep (50);	/* 50 msec */
	  tran_index = conn_p->tran_index;
	}
      if (!logtb_is_interrupted_tran (thread_p, false, &continue_check,
				      tran_index))
	{
	  logtb_set_tran_index_interrupt (thread_p, tran_index, true);
	}

      /* never try to wake non TRAN_ACTIVE state trans.
       * note that non-TRAN_ACTIVE trans will not be interrupted.
       */
      if (logtb_is_interrupted_tran (thread_p, false, &continue_check,
				     tran_index))
	{
	  suspended_p =
	    thread_find_entry_by_tran_index_except_me (tran_index);
	  if (suspended_p != NULL)
	    {
	      int r;
	      bool wakeup_now;

	      r = thread_lock_entry (suspended_p);
	      if (r != NO_ERROR)
		{
		  return r;
		}

	      switch (suspended_p->resume_status)
		{
		case THREAD_CSECT_READER_SUSPENDED:
		case THREAD_CSECT_WRITER_SUSPENDED:
		case THREAD_CSECT_PROMOTER_SUSPENDED:
		case THREAD_LOCK_SUSPENDED:
		case THREAD_PGBUF_SUSPENDED:
		case THREAD_JOB_QUEUE_SUSPENDED:
		  /* never try to wake thread up while the thread is waiting
		   * for a critical section or a lock.
		   */
		  wakeup_now = false;
		  break;
		case THREAD_CSS_QUEUE_SUSPENDED:
		case THREAD_QMGR_ACTIVE_QRY_SUSPENDED:
		case THREAD_QMGR_MEMBUF_PAGE_SUSPENDED:
		case THREAD_HEAP_CLSREPR_SUSPENDED:
		case THREAD_LOGWR_SUSPENDED:
		  wakeup_now = true;
		  break;

		case THREAD_RESUME_NONE:
		case THREAD_RESUME_DUE_TO_INTERRUPT:
		case THREAD_RESUME_DUE_TO_SHUTDOWN:
		case THREAD_PGBUF_RESUMED:
		case THREAD_JOB_QUEUE_RESUMED:
		case THREAD_CSECT_READER_RESUMED:
		case THREAD_CSECT_WRITER_RESUMED:
		case THREAD_CSECT_PROMOTER_RESUMED:
		case THREAD_CSS_QUEUE_RESUMED:
		case THREAD_QMGR_ACTIVE_QRY_RESUMED:
		case THREAD_QMGR_MEMBUF_PAGE_RESUMED:
		case THREAD_HEAP_CLSREPR_RESUMED:
		case THREAD_LOCK_RESUMED:
		case THREAD_LOGWR_RESUMED:
		  /* thread is in resumed status, we don't need to wake up */
		  wakeup_now = false;
		  break;
		default:
		  assert (false);
		  wakeup_now = false;
		  break;
		}

	      if (wakeup_now == true)
		{
		  r =
		    thread_wakeup_already_had_mutex (suspended_p,
						     THREAD_RESUME_DUE_TO_INTERRUPT);
		}
	      r = thread_unlock_entry (suspended_p);

	      if (r != NO_ERROR)
		{
		  return r;
		}
	    }
	}
    }

  while ((thrd_cnt = thread_has_threads (thread_p, tran_index, client_id))
	 >= prev_thrd_cnt && thrd_cnt > 0)
    {
      /* Some threads may wait for data from the m-driver.
       * It's possible from the fact that css_server_thread() is responsible
       * for receiving every data from which is sent by a client and all
       * m-drivers. We must have chance to receive data from them.
       */
      thread_sleep (50);	/* 50 msec */
    }

  if (thrd_cnt > 0)
    {
      goto loop;
    }

conn_down_end1:

  logtb_set_tran_index_interrupt (thread_p, tran_index, false);

  if (interrupt_only)
    {
      return NO_ERROR;
    }

  if (tran_index != NULL_TRAN_INDEX)
    {
      (void) xboot_unregister_client (thread_p, tran_index);
    }

  while (conn_p->con_close_handler_activated)
    {
      thread_sleep (10);
    }

  css_free_conn (conn_p);

  return NO_ERROR;
}

/*
 * net_server_start () - Starts the operation of a Rye server
 *   return: error status
 *   server_name(in): name of server
 */
int
net_server_start (const char *server_name)
{
  int error = NO_ERROR;
  int r, status = 0;

  /* open the system message catalog, before prm_ ?  */
  if (msgcat_init () != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("Failed to initialize message catalog\n");
      status = -1;
      goto end;
    }
  sysprm_load_and_init (NULL);
  sysprm_set_er_log_file (server_name);

  FI_INIT ();

  if (thread_initialize_manager () != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("Failed to initialize thread manager\n");
      status = -1;
      goto end;
    }
  if (csect_initialize () != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("Failed to initialize critical section\n");
      status = -1;
      goto end;
    }
  if (er_init (NULL, ER_EXIT_DEFAULT) != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("Failed to initialize error manager\n");
      status = -1;
      goto end;
    }

  if (lang_init () != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("Failed to initialize lang manager\n");
      status = -1;
      goto end;
    }

  event_log_init (server_name);

  net_server_init ();

  if (boot_restart_server (NULL, true, server_name, false, NULL) != NO_ERROR)
    {
      error = er_errid ();
    }
  else
    {
      r = css_init_job_queue ();

      if (r == NO_ERROR)
	{
	  r = css_init (server_name);
	}

      if (r < 0)
	{
	  error = er_errid ();

	  if (error == NO_ERROR)
	    {
	      error = ER_NET_NO_MASTER;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	    }
	}

      (void) xboot_shutdown_server (NULL);

#if defined(RYE_DEBUG)
      net_server_histo_print ();
#endif /* RYE_DEBUG */

      thread_kill_all_workers ();
      css_final_job_queue ();
      css_final_conn_list ();
      css_free_user_access_status ();
    }

  if (error != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", er_msg ());
      fflush (stderr);
      status = 2;
    }

  csect_finalize ();
  thread_final_manager ();

end:

  return status;
}

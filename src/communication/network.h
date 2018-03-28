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
 * network.h -Definitions for client/server network support.
 */

#ifndef _NETWORK_H_
#define _NETWORK_H_

#ident "$Id$"

#include "query_opfunc.h"
#include "perf_monitor.h"
#include "locator.h"
#include "log_comm.h"
#include "thread.h"


/* Server statistics structure size, used to make sure the pack/unpack
   routines follow the current structure definition.
   This must be the byte size of the structure
   as returned by sizeof().  Note that MEMORY_STAT_SIZE and PACKED_STAT_SIZE
   are not necesarily the same although they will be in most cases.
*/
#define STAT_SIZE_PACKED \
        (OR_INT64_SIZE * MNT_SIZE_OF_SERVER_EXEC_STATS)
#define STAT_SIZE_MEMORY (STAT_SIZE_PACKED)

/* These define the requests that the server will respond to */
typedef enum
{
  NET_SERVER_REQUEST_START = 0,

  NET_SERVER_PING,

  NET_SERVER_BO_INIT_SERVER,
  NET_SERVER_BO_REGISTER_CLIENT,
  NET_SERVER_BO_UNREGISTER_CLIENT,
  NET_SERVER_BO_ADD_VOLEXT,
  NET_SERVER_BO_FIND_NPERM_VOLS,
  NET_SERVER_BO_FIND_NTEMP_VOLS,
  NET_SERVER_BO_FIND_LAST_TEMP,
  NET_SERVER_BO_FIND_NBEST_ENTRIES,
  NET_SERVER_BO_GET_SERVER_STATE,
  NET_SERVER_BO_NOTIFY_HA_APPLY_STATE,
  NET_SERVER_BO_GET_LOCALES_INFO,

  NET_SERVER_TM_SERVER_COMMIT,
  NET_SERVER_TM_SERVER_ABORT,
#if defined (ENABLE_UNUSED_FUNCTION)
  NET_SERVER_TM_SERVER_START_TOPOP,
  NET_SERVER_TM_SERVER_END_TOPOP,
#endif
  NET_SERVER_TM_SERVER_SAVEPOINT,
  NET_SERVER_TM_SERVER_PARTIAL_ABORT,
  NET_SERVER_TM_SERVER_HAS_UPDATED,
  NET_SERVER_TM_SERVER_ISACTIVE_AND_HAS_UPDATED,
#if defined (ENABLE_UNUSED_FUNCTION)
  NET_SERVER_TM_ISBLOCKED,
#endif
  NET_SERVER_TM_WAIT_SERVER_ACTIVE_TRANS,

  NET_SERVER_LC_FETCH,
  NET_SERVER_LC_FETCHALL,
  NET_SERVER_LC_FETCH_LOCKSET,
#if defined (ENABLE_UNUSED_FUNCTION)
  NET_SERVER_LC_GET_CLASS,
#endif
  NET_SERVER_LC_FIND_CLASSOID,
  NET_SERVER_LC_FORCE,
  NET_SERVER_LC_RESERVE_CLASSNAME,
  NET_SERVER_LC_DELETE_CLASSNAME,
  NET_SERVER_LC_RENAME_CLASSNAME,
  NET_SERVER_LC_ASSIGN_OID,
  NET_SERVER_LC_FIND_LOCKHINT_CLASSOIDS,
  NET_SERVER_LC_FETCH_LOCKHINT_CLASSES,
#if defined (ENABLE_UNUSED_FUNCTION)
  NET_SERVER_LC_ASSIGN_OID_BATCH,
#endif

  NET_SERVER_HEAP_CREATE,
  NET_SERVER_HEAP_DESTROY,
  NET_SERVER_HEAP_GET_CLASS_NOBJS_AND_NPAGES,

  NET_SERVER_LOG_RESET_WAIT_MSECS,
#if defined (ENABLE_UNUSED_FUNCTION)
  NET_SERVER_LOG_RESET_ISOLATION,
#endif
  NET_SERVER_LOG_SET_INTERRUPT,
  NET_SERVER_LOG_DUMP_STAT,
  NET_SERVER_LOG_GETPACK_TRANTB,
  NET_SERVER_LOG_DUMP_TRANTB,

  NET_SERVER_LK_DUMP,

  NET_SERVER_BTREE_ADD_INDEX,
  NET_SERVER_BTREE_DEL_INDEX,
  NET_SERVER_BTREE_LOAD_DATA,
  NET_SERVER_BTREE_FIND_UNIQUE,

  NET_SERVER_DISK_GET_PURPOSE_AND_SPACE_INFO,
  NET_SERVER_DISK_VLABEL,

  NET_SERVER_QST_GET_STATISTICS,
  NET_SERVER_QST_UPDATE_STATISTICS,
  NET_SERVER_QST_UPDATE_ALL_STATISTICS,

  NET_SERVER_QM_QUERY_PREPARE,
  NET_SERVER_QM_QUERY_EXECUTE,
  NET_SERVER_QM_QUERY_END,
  NET_SERVER_QM_QUERY_DROP_PLAN,
  NET_SERVER_QM_QUERY_DROP_ALL_PLANS,
  NET_SERVER_QM_GET_QUERY_INFO,
  NET_SERVER_QM_QUERY_DUMP_PLANS,

  NET_SERVER_LS_GET_LIST_FILE_PAGE,

  NET_SERVER_CT_CAN_ACCEPT_NEW_REPR,

  NET_SERVER_CSS_KILL_TRANSACTION,

  NET_SERVER_QPROC_GET_SERVER_INFO,

  NET_SERVER_PRM_SET_PARAMETERS,
  NET_SERVER_PRM_GET_PARAMETERS,
  NET_SERVER_PRM_GET_FORCE_PARAMETERS,
  NET_SERVER_PRM_DUMP_PARAMETERS,

  NET_SERVER_REPL_INFO,
  NET_SERVER_REPL_LOG_GET_EOF_LSA,

  NET_SERVER_LOGWR_GET_LOG_PAGES,

  NET_SERVER_SHUTDOWN,

  /* Followings are not grouped because they are appended after the above.
     It is necessary to rearrange with changing network compatibility. */
  NET_SERVER_CSS_DUMP_CS_STAT,

  NET_SERVER_LOG_CHECKPOINT,

  NET_SERVER_LOG_SET_SUPPRESS_REPL_ON_TRANSACTION,

  /* Session state requests */
  NET_SERVER_SES_CHECK_SESSION,
  NET_SERVER_SES_END_SESSION,

  NET_SERVER_ACL_DUMP,
  NET_SERVER_ACL_RELOAD,

  NET_SERVER_CSS_DUMP_SERVER_STAT,

  NET_SERVER_LC_REPL_FORCE,
  NET_SERVER_LC_LOCK_SYSTEM_DDL_LOCK,
#if defined (ENABLE_UNUSED_FUNCTION)
  NET_SERVER_LC_LOCK_CHANGE_CLASS_XLOCK_TO_ULOCK,
#endif

  NET_SERVER_MIGRATOR_GET_LOG_PAGES,
  NET_SERVER_UPDATE_GROUP_ID,
  NET_SERVER_BLOCK_GLOBAL_DML,

  NET_SERVER_BK_PREPARE_BACKUP,
  NET_SERVER_BK_BACKUP_VOLUME,
  NET_SERVER_BK_BACKUP_LOG_VOLUME,

  /*
   * This is the last entry. It is also used for the end of an
   * array of statistics information on client/server communication.
   */
  NET_SERVER_REQUEST_END,
  /*
   * This request number must be preserved.
   */
  NET_SERVER_PING_WITH_HANDSHAKE = 999,
} NET_SERVER_REQUEST;

/* Server/client capabilities */
#define NET_CAP_INTERRUPT_ENABLED       0x00800000
#define NET_CAP_UPDATE_DISABLED         0x00008000
#define NET_CAP_HA_REPL_DELAY           0x00000008
#define NET_CAP_HA_REPLICA              0x00000004
#define NET_CAP_HA_IGNORE_REPL_DELAY	0x00000002

/* Server startup */
extern int net_server_start (const char *name);

#if defined(SERVER_MODE)
extern int net_server_conn_down (THREAD_ENTRY * thread_p, CSS_THREAD_ARG arg, bool force);
extern int net_server_request (THREAD_ENTRY * thread_p, unsigned int rid, int request, int size, char *buffer);

#endif

#endif /* _NETWORK_H_ */

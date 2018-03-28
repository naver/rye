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
 * transaction_cl.c -
 */

#ident "$Id$"

#include "config.h"

#include <unistd.h>
#include <stdio.h>
#include <sys/param.h>

#include "dbi.h"
#include "misc_string.h"
#include "transaction_cl.h"
#include "memory_alloc.h"
#include "locator_cl.h"
#include "work_space.h"
#include "server_interface.h"
#include "log_comm.h"
#include "db_query.h"
#include "boot_cl.h"
#include "schema_manager.h"
#include "system_parameter.h"
#include "dbdef.h"
#include "db.h"                 /* for db_Connect_status */
#include "porting.h"
#include "network_interface_cl.h"

int tm_Tran_index = NULL_TRAN_INDEX;
int tm_Tran_wait_msecs = TRAN_LOCK_INFINITE_WAIT;
int tm_Tran_ID = -1;

/* Timeout(milli seconds) for queries.
 *
 * 0 means "unlimited", and negative value means "do not calculate timeout".
 */
static UINT64 tm_Query_begin = 0;
static int tm_Query_timeout = 0;

/* this is a local list of user-defined savepoints.  It may be updated upon
 * the following calls:
 *    tran_savepoint()		-> tm_add_savepoint()
 *    tran_commit()		-> tran_free_savepoint_list()
 *    tran_abort()		-> tran_free_savepoint_list()
 *    tran_abort_upto_savepoint() -> tm_free_list_upto_savepoint()
 */
static DB_NAMELIST *user_savepoint_list = NULL;

static int tran_add_savepoint (const char *savept_name);
static void tran_free_list_upto_savepoint (const char *savept_name);

/*
 * tran_cache_tran_settings - Cache transaction settings
 *
 * return:
 *
 *   tran_index(in): Transaction index assigned to client
 *   lock_timeout(in): Transaction lock wait assigned to client transaction
 *
 * Note: Transaction settings are cached for future retieval.
 *       If tm_Tran_index is NULL then we can safely assume that the
 *       database connect flag can be turned off. i.e., db_Connect_status=0
 */
void
tran_cache_tran_settings (int tran_index, int lock_timeout)
{
  tm_Tran_index = tran_index;
  tm_Tran_wait_msecs = lock_timeout;

  /* This is a dirty, but quick, method by which we can flag that
   * the database connection has been terminated. This flag is used by
   * the C API calls to determine if a database connection exists.
   */
  if (tm_Tran_index == NULL_TRAN_INDEX)
    {
      db_Connect_status = DB_CONNECTION_STATUS_NOT_CONNECTED;
    }
}

/*
 * tran_get_tran_settings - Get transaction settings
 *
 * return: nothing
 *
 *   lock_wait(in/out): Transaction lock wait assigned to client transaction
 *   tran_isolation(in/out): Transaction isolation assigned to client
 *                     transactions
 *
 * Note: Retrieve transaction settings.
 */
void
tran_get_tran_settings (int *lock_wait_in_msecs, TRAN_ISOLATION * tran_isolation)
{
  *lock_wait_in_msecs = TM_TRAN_WAIT_MSECS ();
  /* lock timeout in milliseconds */ ;
  *tran_isolation = TRAN_DEFAULT_ISOLATION;
}

/*
 * tran_reset_wait_times - Reset future waiting times for client transactions
 *
 * return: The old wait_msecs.
 *
 *   wait_in_msecs(in): Wait for at least this number of milliseconds to acquire a lock
 *               before the transaction is timed out.
 *               A negative value (e.g., -1) means wait forever until a lock
 *               is granted or transaction is selected as a victim of a
 *               deadlock.
 *               A value of zero means do not wait at all, timeout immediately
 *
 * NOTE: Reset the default waiting time for the client transactions.
 */
int
tran_reset_wait_times (int wait_in_msecs)
{
  tm_Tran_wait_msecs = wait_in_msecs;

  return log_reset_wait_msecs (tm_Tran_wait_msecs);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * tran_reset_isolation - Reset isolation level of client session (transaction
 *                     index)
 *
 * return:  NO_ERROR if all OK, ER_ status otherwise
 *
 *   isolation(in): New Isolation level. One of the following:
 *                         TRAN_REP_CLASS_REP_INSTANCE
 *                         TRAN_REP_CLASS_COMMIT_INSTANCE
 *                         TRAN_DEFAULT_ISOLATION
 *                         TRAN_COMMIT_CLASS_COMMIT_INSTANCE
 *                         TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE
 *
 * NOTE: Reset the default isolation level for the current transaction
 *              index (client). It is recommended that the isolation level of
 *              a client session get reseted at the beginning of a transaction
 *              (i.e., just after client restart, abort or commit). If this is
 *              not done some of the current acquired locks of the transaction
 *              may be released according to the new isolation level.
 */
int
tran_reset_isolation (TRAN_ISOLATION isolation)
{
  int error_code = NO_ERROR;

  if (isolation != TRAN_DEFAULT_ISOLATION)
    {
      error_code = ER_LOG_INVALID_ISOLATION_LEVEL;
      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 2, TRAN_DEFAULT_ISOLATION, TRAN_DEFAULT_ISOLATION);
      return error_code;
    }

  assert (isolation == TRAN_DEFAULT_ISOLATION);

#if 0                           /* unused */
  error_code = log_reset_isolation (isolation);
#endif

  return error_code;
}
#endif

/*
 * tran_commit - COMMIT THE CURRENT TRANSACTION
 *
 * return:
 *
 * NOTE: commit the current transaction. All objects that have been
 *              updated by the transaction and are still dirty in the
 *              workspace are flushed to the page buffer pool (server). Then,
 *              the commit statement is forwarded to the transaction manager
 *              in the server. The transaction manager in the server will do a
 *              few things and the notify the recovery manager of the commit.
 *              The recovery manager commits the transaction and may notify of
 *              some loose end actions that need to be executed in the client
 *              as part of the commit (after commit actions). As a result of
 *              the commit all changes made by the transaction are made
 *              permanent and all acquired locks are released. Any locks
 *              cached in the workspace are cleared.
 */
int
tran_commit ()
{
  TRAN_STATE state;
  int error_code = NO_ERROR;

#if defined(ENABLE_UNUSED_FUNCTION)
  /* tell the schema manager to flush any transaction caches */
  sm_transaction_boundary ();
#endif

  if (ws_need_flush ())
    {
#if defined (ENABLE_UNUSED_FUNCTION)
      (void) locator_assign_all_permanent_oids ();
#endif

      /* Flush all dirty objects */
      /* Flush virtual objects first so that locator_all_flush doesn't see any */
      error_code = locator_all_flush ();
      if (error_code != NO_ERROR)
        {
          return error_code;
        }
    }

  /* Clear all the queries */
  db_clear_client_query_result (true, false);

  /* if the commit fails or not, we should clear the clients savepoint list */
  tran_free_savepoint_list ();

  /* Forward the commit the transaction manager in the server */
  state = tran_server_commit ();

  switch (state)
    {
    case TRAN_UNACTIVE_COMMITTED:
      /* Successful commit */
      error_code = NO_ERROR;
      break;

    case TRAN_UNACTIVE_ABORTED:
    case TRAN_UNACTIVE_UNILATERALLY_ABORTED:
      /* The commit failed */
      error_code = er_errid ();
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE, "tran_commit: Unable to commit. Transaction was aborted\n");
#endif /* RYE_DEBUG */
      break;

    case TRAN_UNACTIVE_UNKNOWN:
      if (!BOOT_IS_CLIENT_RESTARTED ())
        {
          error_code = er_errid ();
          break;
        }
      /* Fall Thru */
    case TRAN_RECOVERY:
    case TRAN_ACTIVE:
    case TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE:
    case TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE:
    default:
      error_code = er_errid ();
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE, "tran_commit: Unknown commit state = %s at client\n", log_state_string (state));
#endif /* RYE_DEBUG */
      break;
    }

  /* clear workspace information and any open query cursors */
  if (error_code == NO_ERROR || BOOT_IS_CLIENT_RESTARTED ())
    {
      ws_clear_all_hints ();
      er_stack_clearall ();
    }

  return error_code;
}

/*
 * tran_abort - ABORT THE CURRENT TRANSACTION
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 * NOTE:Abort the current transaction. All objects updated by the
 *              current transaction that are still dirty are removed from the
 *              workspace and then the abort is forwarded to the transaction
 *              manager in the server. In the server all updates made to the
 *              database and the page buffer pool are rolled back, and the
 *              transaction is declared as aborted. The server may notify the
 *              the transaction manager in the client of any loose end undoes
 *              that need to be executed in the client as part of the abort.
 *              As a result of the abort all changes made by the transaction
 *              are rolled back and acquired locks are released. Any locks
 *              cached in the workspace are cleared.
 */
int
tran_abort (void)
{
  TRAN_STATE state;
  int error_cod = NO_ERROR;

#if defined(ENABLE_UNUSED_FUNCTION)
  /* tell the schema manager to flush any transaction caches */
  sm_transaction_boundary ();
#endif

  /* Remove any dirty objects and remove any hints */
  ws_abort_mops (false);
  ws_filter_dirty ();

  /* free the local list of savepoint names */
  tran_free_savepoint_list ();

  /* Clear any query cursor */
  db_clear_client_query_result (true, true);

  /* Forward the abort the transaction manager in the server */
  state = tran_server_abort ();

  switch (state)
    {
      /* Successful abort */
    case TRAN_UNACTIVE_ABORTED:
      break;

    case TRAN_UNACTIVE_UNKNOWN:
      if (!BOOT_IS_CLIENT_RESTARTED ())
        {
          error_cod = er_errid ();
          break;
        }
      /* Fall Thru */
    case TRAN_RECOVERY:
    case TRAN_ACTIVE:
    case TRAN_UNACTIVE_COMMITTED:
    case TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE:
    case TRAN_UNACTIVE_UNILATERALLY_ABORTED:
    case TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE:
    default:
      error_cod = er_errid ();
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE, "tran_abort: Unknown abort state = %s\n", log_state_string (state));
#endif /* RYE_DEBUG */
      break;
    }

  er_stack_clearall ();

  return error_cod;
}

/*
 * tran_unilaterally_abort - Unilaterally abort the current transaction
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 * NOTE:The current transaction is unilaterally aborted by a client
 *              module of the system.
 *              Execute tran_abort & set an error message
 */
int
tran_unilaterally_abort (void)
{
  int error_code = NO_ERROR;
  char user_name[L_cuserid + 1];
  char host[MAXHOSTNAMELEN];
  int pid;

#if 1                           /* TODO - trace */
  assert (false);
#endif

  /* Get the user name, host, and process identifier */
  if (getuserid (user_name, L_cuserid) == NULL)
    {
      strcpy (user_name, "(unknown)");
    }
  if (GETHOSTNAME (host, MAXHOSTNAMELEN) != 0)
    {
      /* unknown error */
      strcpy (host, "(unknown)");
    }
  pid = getpid ();

  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_UNILATERALLY_ABORTED, 4, tm_Tran_index, user_name, host, pid);

  error_code = tran_abort ();

  return error_code;
}

/*
 * tran_abort_only_client - Abort the current transaction only at the client
 *                       level. (the server aborted the transaction)
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   is_server_down(in):Was the transaction aborted because of the server crash?
 *
 * NOTE: The current transaction is aborted only at the client level,
 *              since the transaction has already been aborted at the server.
 *              All dirty objects in the workspace are removed and cached
 *              locks are cleared.
 *       This function is called when the transaction component (e.g.,
 *              transaction object locator) finds that the transaction was
 *              unilaterally aborted.
 */
int
tran_abort_only_client (bool is_server_down)
{
  if (!BOOT_IS_CLIENT_RESTARTED ())
    {
      if (is_server_down)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED, 0);
          return ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED;
        }

      return NO_ERROR;
    }

  /* Remove any dirty objects and close all open query cursors */
  ws_abort_mops (true);
  ws_filter_dirty ();
  db_clear_client_query_result (false, true);

  if (is_server_down == false)
    {
      /* Do we need to execute any loose ends ? */
      return NO_ERROR;
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED, 0);
      return ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED;
    }

  return NO_ERROR;
}

/*
 * tran_has_updated - HAS TRANSACTION UPDATED THE DATABASE ?
 *
 * return:
 *
 * NOTE:Find if the transaction has dirtied the database.
 */
bool
tran_has_updated (void)
{
  bool found = false;

  if (tran_server_has_updated ())
    {
      found = true;
    }

  assert (found || !ws_has_updated ());

  return found;
}

/*
 * tran_is_active_and_has_updated - Find if transaction is active and
 *				    has updated the database ?
 *
 * return:
 *
 * NOTE:Find if the transaction is active and has updated/dirtied the
 *              database.
 */
bool
tran_is_active_and_has_updated (void)
{
  bool found = false;

  if (tran_server_is_active_and_has_updated ())
    {
      found = true;
    }

  assert (found || !ws_has_updated ());

  return found;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * tran_start_topop - Start a macro nested top operation
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 */
int
tran_start_topop (void)
{
  LOG_LSA topop_lsa;
  int error_code = NO_ERROR;

  /* Flush all dirty objects */
  if (ws_need_flush ())
    {
      error_code = locator_all_flush ();
      if (error_code != NO_ERROR)
        {
#if defined(RYE_DEBUG)
          er_log_debug (ARG_FILE_LINE,
                        "tran_start_topop: Unable to start a top operation. \n %s",
                        " Flush failed.\nerrmsg = %s\n", er_msg ());
#endif /* RYE_DEBUG */
          goto end;
        }
    }

  if (tran_server_start_topop (&topop_lsa) != NO_ERROR)
    {
      error_code = ER_FAILED;
    }

end:
  return error_code;
}

static int
tran_end_topop_commit (void)
{
  int error_code = NO_ERROR;
  TRAN_STATE state;
  LOG_LSA topop_lsa;

  /* Flush all dirty objects */
  if (ws_need_flush ())
    {
      error_code = locator_all_flush ();
      if (error_code != NO_ERROR)
        {
#if defined(RYE_DEBUG)
          er_log_debug (ARG_FILE_LINE,
                        "tm_end_topop_commit: Unable to finish a nested top oper. Flush failed.\nerrmsg = %s\n",
                        er_msg ());
#endif /* RYE_DEBUG */
          goto end;
        }
    }

  state = tran_server_end_topop (LOG_RESULT_TOPOP_COMMIT, &topop_lsa);
  if (state != TRAN_UNACTIVE_COMMITTED)
    {
      error_code = er_errid ();
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE,
                    "tran_end_topop_commit: oper failed with state = %s at client.\n", log_state_string (state));
#endif /* RYE_DEBUG */
    }

end:
  return error_code;
}

static int
tran_end_topop_abort (void)
{
  int error_code = NO_ERROR;
  TRAN_STATE state;
  LOG_LSA topop_lsa;

  /* Remove any dirty objects */
  ws_abort_mops (false);
  ws_filter_dirty ();

  state = tran_server_end_topop (LOG_RESULT_TOPOP_ABORT, &topop_lsa);
  if (state != TRAN_UNACTIVE_ABORTED)
    {
      error_code = er_errid ();
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE,
                    "tran_end_topop_abort: oper failed with state = %s at client.\n", log_state_string (state));
#endif /* RYE_DEBUG */
    }

  return error_code;
}

/*
 * tran_end_topop - END A MACRO NESTED TOP OPERATION
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   result(in): Result of the nested top action
 *
 * NOTE: Finish the latest nested top macro operation by either
 *              committing, aborting, or attaching to outter parent. Note that
 *              a top operation is not associated with the current
 *              transaction, thus, it can be committed and/or aborted
 *              independently of the transaction.
 */
int
tran_end_topop (LOG_RESULT_TOPOP result)
{
  int error_code = NO_ERROR;
  LOG_LSA topop_lsa;
  TRAN_STATE state;

  switch (result)
    {
    case LOG_RESULT_TOPOP_COMMIT:
      error_code = tran_end_topop_commit ();
      break;

    case LOG_RESULT_TOPOP_ABORT:
      error_code = tran_end_topop_abort ();
      break;

    case LOG_RESULT_TOPOP_ATTACH_TO_OUTER:
    default:
      state = tran_server_end_topop (result, &topop_lsa);
      if (state != TRAN_ACTIVE)
        {
          error_code = er_errid ();
#if defined(RYE_DEBUG)
          er_log_debug (ARG_FILE_LINE,
                        "tran_end_topop: oper failed with state = %s at client.\n", log_state_string (state));
#endif /* RYE_DEBUG */
        }
      break;
    }

  return error_code;

}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * tran_add_savepoint -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   savept_name(in):
 *
 * NOTE:insert a savepoint name into the front of the list.  This way, the list
 * is sorted in reverse chronological order
 */
static int
tran_add_savepoint (const char *savept_name)
{
  DB_NAMELIST *sp;

  sp = (DB_NAMELIST *) db_ws_alloc (sizeof (DB_NAMELIST));
  if (sp == NULL)
    {
      return er_errid ();
    }

  sp->name = ws_copy_string (savept_name);
  if (sp->name == NULL)
    {
      db_ws_free (sp);
      return er_errid ();
    }
  sp->next = user_savepoint_list;
  user_savepoint_list = sp;

  return NO_ERROR;
}

/*
 * tran_free_savepoint_list -
 *
 * return:
 *
 * NOTE:free the entire user savepoint list.  Called during abort, commit, or
 * restart
 */
void
tran_free_savepoint_list (void)
{
  nlist_free (user_savepoint_list);
  user_savepoint_list = NULL;
}

/*
 * tran_free_list_upto_savepoint -
 *
 * return:
 *
 *   savept_name(in):
 *
 * NOTE:frees the latest savepoints from the list up to, but not including, the
 * given savepoint.  Called during rollback to savepoint command.
 */
static void
tran_free_list_upto_savepoint (const char *savept_name)
{
  DB_NAMELIST *sp, *temp;
  bool found = false;

  /* first, check to see if it's in the list */
  for (sp = user_savepoint_list; sp && !found; sp = sp->next)
    {
      if (intl_mbs_casecmp (sp->name, savept_name) == 0)
        {
          found = true;
        }
    }

  /* not 'found' is not necessarily an error.  We may be rolling back to a
   * system-defined savepoint rather than a user-defined savepoint.  In that
   * case, the name would not appear on the user savepoint list and the list
   * should be preserved.  We should be able to guarantee that any rollback
   * to a system-defined savepoint will affect only the latest atomic command
   * and not overlap any user-defined savepoint.  That is, system invoked
   * partial rollbacks should never rollback farther than the last
   * user-defined savepoint.
   */
  if (found == true)
    {
      for (sp = user_savepoint_list; sp;)
        {
          if (intl_mbs_casecmp (sp->name, savept_name) == 0)
            {
              break;
            }

          temp = sp;
          sp = sp->next;
          db_ws_free ((char *) temp->name);
          db_ws_free (temp);
        }
      user_savepoint_list = sp;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * tran_get_savepoints - Get list of current savepoint names.  The latest
 *                    savepoint is listed first. (Reverse chronological
 *                    order)
 *
 * return: NO_ERROR
 *
 *   savepoint_list(in): savepoint list pointer
 *
 * NOTE: A list of user-defined savepoint names is maintained locally.
 *              The list has a lifespan of the current transaction and is
 *              freed upon commit or abort.  It is partially freed upon a
 *              partial rollback (to savepoint).  The savepoint list returned
 *              in this function must be later freed by nlist_free().
 */
int
tran_get_savepoints (DB_NAMELIST ** savepoint_list)
{
  *savepoint_list = nlist_copy (user_savepoint_list);
  return NO_ERROR;
}

/*
 * tran_system_savepoint -
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   savept_name(in): Name of the savepoint
 *
 */
int
tran_system_savepoint (const char *savept_name)
{
  return tran_savepoint_internal (savept_name, SYSTEM_SAVEPOINT);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * tran_savepoint_internal - Declare a user savepoint
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   savept_name(in): Name of the savepoint
 *   savepoint_type(in):
 *
 * NOTE: A savepoint is established for the current transaction, so
 *              that future transaction actions can be rolled back to this
 *              established savepoint. We call this operation a partial abort
 *              (rollback). That is, all database actions affected by the
 *              transaction after the savepoint are "undone", and all effects
 *              of the transaction preceding the savepoint remain. The
 *              transaction can then continue executing other database
 *              statement. It is permissible to abort to the same savepoint
 *              repeatedly within the same transaction.
 *              If the same savepoint name is used in multiple savepoint
 *              declarations within the same transaction, then only the latest
 *              savepoint with that name is available for aborts and the
 *              others are forgotten.
 *              There are no limits on the number of savepoints that a
 *              transaction can have.
 */
int
tran_savepoint_internal (const char *savept_name, SAVEPOINT_TYPE savepoint_type)
{
  LOG_LSA savept_lsa;
  int error_code = NO_ERROR;

  assert (savepoint_type == USER_SAVEPOINT);

  /* Flush all dirty objects */
  if (ws_need_flush ())
    {
      error_code = locator_all_flush ();
      if (error_code != NO_ERROR)
        {
#if defined(RYE_DEBUG)
          er_log_debug (ARG_FILE_LINE,
                        "tran_savepoint_internal: Unable to start a top operation\n Flush failed.\nerrmsg = %s",
                        er_msg ());
#endif /* RYE_DEBUG */
          return error_code;
        }
    }

  if (tran_server_savepoint (savept_name, &savept_lsa) != NO_ERROR)
    {
      error_code = er_errid ();
      return error_code;
    }

  /* add savepoint to local list */
  if (savepoint_type == USER_SAVEPOINT)
    {
      error_code = tran_add_savepoint (savept_name);
      if (error_code != NO_ERROR)
        {
          return error_code;
        }
    }

  return error_code;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * tran_abort_upto_system_savepoint -
 *
 * return: state of partial aborted operation (i.e., notify if
 *              there are client actions that need to be undone).
 *
 *   savepoint_name(in): Name of the savepoint
 */
int
tran_abort_upto_system_savepoint (const char *savepoint_name)
{
  return tran_internal_abort_upto_savepoint (savepoint_name, SYSTEM_SAVEPOINT);
}
#endif

/*
 * tran_abort_upto_user_savepoint -
 *
 * return: state of partial aborted operation (i.e., notify if
 *              there are client actions that need to be undone).
 *   savepoint_name(in): Name of the savepoint
 */
int
tran_abort_upto_user_savepoint (const char *savepoint_name)
{
  /* delete client's local copy of savepoint names back to here */
  tran_free_list_upto_savepoint (savepoint_name);

  return tran_internal_abort_upto_savepoint (savepoint_name, USER_SAVEPOINT);
}

/*
 * tran_internal_abort_upto_savepoint - Abort operations of a transaction
 *    upto a savepoint
 *
 * return: state of partial aborted operation (i.e., notify if
 *              there are client actions that need to be undone).
 *
 *   savepoint_name(in): Name of the savepoint
 *   savepoint_type(in):
 *
 * NOTE: All the effects of the current transaction after the
 *              given savepoint are undone, and all effects of the transaction
 *              preceding the given savepoint remain. After the partial abort
 *              the transaction can continue its normal execution as if
 *              the statements that were undone were never executed.
 *              All objects updated by the current transaction that are still
 *              dirty are removed from the workspace and then the partial
 *              abort is forwarded to the transaction manager in the server.
 *              In the server all updates made to the database and the page
 *              buffer pool after the given savepoint are rolled back. The
 *              server may notify the transaction manager in the client of
 *              any client loose_end undoes that need to be executed at the
 *              client as part of the partial abort.
 *              The locks in the workspace will need to be cleared since we do
 *              not know in the client what objects were rolled back. This is
 *              needed since the client does not request the objects from the
 *              server if the desired lock has been already acquired (cached
 *              in the workspace). Therefore, from the point of view of the
 *              workspace, the transaction will need to validate the objects
 *              that need to be accessed in the future.
 */
int
tran_internal_abort_upto_savepoint (const char *savepoint_name, SAVEPOINT_TYPE savepoint_type)
{
  int error_code = NO_ERROR;
  LOG_LSA savept_lsa;
  TRAN_STATE state;

  assert (savepoint_type == USER_SAVEPOINT);

#if defined(ENABLE_UNUSED_FUNCTION)
  /* tell the schema manager to flush any transaction caches */
  sm_transaction_boundary ();
#endif

  /*
   * We need to start all over since we do not know what set of objects are
   * going to be rolled back.. Thus, we need to remove any kind of hints
   * cached in the workspace.
   */

  /* Remove any dirty objects and remove any hints */
  ws_abort_mops (false);
  ws_filter_dirty ();

  state = tran_server_partial_abort (savepoint_name, &savept_lsa);
  if (state != TRAN_UNACTIVE_ABORTED)
    {
      error_code = er_errid ();
      if (savepoint_type == SYSTEM_SAVEPOINT && state == TRAN_UNACTIVE_UNKNOWN
          && error_code != NO_ERROR && !tran_has_updated ())
        {
          assert (false);       /* is impossible */

          /*
           * maybe transaction has been unilaterally aborted by the system
           * and ER_LK_UNILATERALLY_ABORTED was overwritten by a consecutive error.
           */
          (void) tran_unilaterally_abort ();
        }

#if defined(RYE_DEBUG)
      if (error_code != ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED && error_code != ER_NET_SERVER_CRASHED)
        {
          er_log_debug (ARG_FILE_LINE,
                        "tran_abort_upto_savepoint: oper failed with state = %s %s",
                        log_state_string (state), " at client.\n");
        }
#endif /* RYE_DEBUG */
    }

  return error_code;
}

static UINT64
tran_current_timemillis (void)
{
  struct timeval tv;
  UINT64 msecs;

  gettimeofday (&tv, NULL);
  msecs = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);

  return msecs;
}

/*
 * tran_set_query_timeout() -
 *   return: void
 *   query_timeout(in): timeout in milliseconds to be shipped to "query_execute_query"
 */
void
tran_set_query_timeout (int query_timeout)
{
  tm_Query_begin = tran_current_timemillis ();
  tm_Query_timeout = query_timeout;
}

/*
 * tran_get_query_timeout() -
 *   return: timeout (milliseconds)
 */
int
tran_get_query_timeout (void)
{
  UINT64 elapsed;
  int timeout;

  if (tm_Query_timeout <= 0)
    {
      return 0;
    }

  elapsed = tran_current_timemillis () - tm_Query_begin;
  timeout = tm_Query_timeout - elapsed;
  if (timeout <= 0)
    {
      /* already expired */
      timeout = -2;
    }

  return timeout;
}

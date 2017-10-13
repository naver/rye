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
 * transaction_sr.c -
 */

#ident "$Id$"

#include "config.h"

#include <assert.h>

#include "db.h"
#include "transaction_sr.h"
#include "locator_sr.h"
#include "log_manager.h"
#include "log_impl.h"
#include "wait_for_graph.h"
#include "thread.h"
#if defined(SERVER_MODE)
#include "server_support.h"
#endif
#include "xserver_interface.h"

/*
 * xtran_server_commit - Commit the current transaction
 *
 * return: state of operation
 *
 *   thrd(in): this thread handle
 *
 * NOTE: Commit the current transaction. All transient class name
 *              entries are removed, and the commit is forwarded to the log
 *              and recovery manager. The log manager declares all changes of
 *              the transaction as permanent and releases all locks acquired
 *              by the transaction. The return value may indicate that the
 *              transaction has not been committed completely when there are
 *              some loose_end postpone actions to be done in the client
 *              machine. In this case the client transaction manager must
 *              obtain and execute these actions.
 *
 *       This function should be called after all objects that have
 *              been updated by the transaction are flushed from the workspace
 *              (client) to the page buffer pool (server).
 */
TRAN_STATE
xtran_server_commit (THREAD_ENTRY * thread_p)
{
  TRAN_STATE state;
  int tran_index;

  /*
   * Execute some few remaining actions before the log manager is notified of
   * the commit
   */

  tran_index = logtb_get_current_tran_index (thread_p);

  state = log_commit (thread_p, tran_index);

  (void) locator_drop_transient_class_name_entries (thread_p, tran_index,
						    NULL);
  return state;
}

/*
 * xtran_server_abort - Abort the current transaction
 *
 * return: state of operation
 *
 *   thrd(in): this thread handle
 *
 * NOTE: Abort the current transaction. All transient class name
 *              entries are removed, and the abort operation is forwarded to
 *              to the log/recovery manager. The log manager undoes any
 *              changes made by the transaction and releases all lock acquired
 *              by the transaction. The return value may indicate that the
 *              transaction has not been aborted completely when there are
 *              some loose_end undo actions  to be executed in the client
 *              machine. In this case the client transaction manager must
 *              obtain and execute these actions.
 *       This function should be called after all updated objects in
 *              the workspace are removed.
 */
TRAN_STATE
xtran_server_abort (THREAD_ENTRY * thread_p)
{
  TRAN_STATE state;
  int tran_index;

  /*
   * Execute some few remaining actions before the log manager is notified of
   * the commit
   */

  tran_index = logtb_get_current_tran_index (thread_p);

  state = log_abort (thread_p, tran_index);

  (void) locator_drop_transient_class_name_entries (thread_p, tran_index,
						    NULL);

  return state;
}

#if defined(SERVER_MODE)
/*
 * tran_server_unilaterally_abort_tran -
 *
 * return:
 *
 * NOTE:this function is used when pgbuf_fix() results in deadlock.
 * It is used by request handler functions to rollback gracefully,
 */
void
tran_server_unilaterally_abort_tran (THREAD_ENTRY * thread_p)
{
  TRAN_STATE state;
  int tran_index;

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  tran_index = logtb_get_current_tran_index (thread_p);
  state = xtran_server_abort (thread_p);
}
#endif /* SERVER_MODE */

/*
 * xtran_server_start_topop - Start a server macro nested top operation
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   topop_lsa(in/out): Address of top operation for rollback purposes
 *
 */
int
xtran_server_start_topop (THREAD_ENTRY * thread_p, LOG_LSA * topop_lsa)
{
  int error_code = NO_ERROR;

  /*
   * Execute some few remaining actions before the start top nested action is
   * started by the log manager.
   */

  if (log_start_system_op (thread_p) == NULL)
    {
      LSA_SET_NULL (topop_lsa);
      error_code = ER_FAILED;
    }

  return error_code;
}

/*
 * xtran_server_end_topop - End a macro nested top operation
 *
 * return: states of transactions
 *
 *   result(in): Result of the nested top action
 *   topop_lsa(in): Address where the top operation for rollback purposes
 *                  started.
 *
 * NOTE:Finish the latest nested top macro operation by either
 *              committing, aborting, or attaching to outter parent.
 *
 *      Note that a top operation is not associated with the current
 *              transaction, thus, it can be committed and/or aborted
 *              independently of the transaction.
 */
TRAN_STATE
xtran_server_end_topop (THREAD_ENTRY * thread_p, LOG_RESULT_TOPOP result,
			LOG_LSA * topop_lsa)
{
  int tran_index;
  TRAN_STATE state;

  /*
   * Execute some few remaining actions before the start top nested action is
   * started by the log manager.
   */

  switch (result)
    {
    case LOG_RESULT_TOPOP_COMMIT:
    case LOG_RESULT_TOPOP_ABORT:
      if (log_get_parent_lsa_system_op (thread_p, topop_lsa) == topop_lsa)
	{
	  tran_index = logtb_get_current_tran_index (thread_p);
	  (void) locator_drop_transient_class_name_entries (thread_p,
							    tran_index,
							    topop_lsa);
	}
      state = log_end_system_op (thread_p, result);
      break;

    case LOG_RESULT_TOPOP_ATTACH_TO_OUTER:
    default:
      state = log_end_system_op (thread_p, result);
      break;
    }
  return state;
}

/*
 * xtran_server_savepoint - Declare a user savepoint
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   savept_name(in): Name of the savepoint
 *   savept_lsa(in): Address of save point operation.
 *
 * NOTE: A savepoint is established for the current transaction so
 *              that future transaction actions can be rolled back to this
 *              established savepoint. We call this operation a partial abort
 *              (rollback). That is, all database actions affected by the
 *              transaction after the savepoint are "undone", and all effects
 *              of the transaction preceding the savepoint remain. The
 *              transaction can then continue executing other database
 *              statements. It is permissible to abort to the same savepoint
 *              repeatedly within the same transaction.
 *              If the same savepoint name is used in multiple savepoint
 *              declarations within the same transaction, then only the latest
 *              savepoint with that name is available for aborts and the
 *              others are forgotten.
 *              There are no limits on the number of savepoints that a
 *              transaction can have.
 */
int
xtran_server_savepoint (THREAD_ENTRY * thread_p, const char *savept_name,
			LOG_LSA * savept_lsa)
{
  int error_code = NO_ERROR;

  /*
   * Execute some few remaining actions before the start top nested action is
   * started by the log manager.
   */

  if (log_append_savepoint (thread_p, savept_name) == NULL)
    {
      LSA_SET_NULL (savept_lsa);
      error_code = ER_FAILED;
    }

  return error_code;
}

/*
 * xtran_server_partial_abort -Abort operations of a transaction up to a savepoint
 *
 * return: state of partial aborted operation (i.e., notify if
 *              there are client actions that need to be undone).
 *
 *   savept_name(in): Name of the savepoint
 *   savept_lsa(in/out): Address of save point operation.
 *
 * Note: All the effects of the current transaction after the
 *              given savepoint are undone and all the effects of the transaction
 *              preceding the given savepoint remain. After the partial abort
 *              the transaction can continue its normal execution as if
 *              the statements that were undone, were never executed.
 *              The return value may indicate that there are some client
 *              loose_end undo actions to be performed at the client machine.
 *              In this case the transaction manager must obtain and execute
 *              these actions at the client.
 */
TRAN_STATE
xtran_server_partial_abort (THREAD_ENTRY * thread_p, const char *savept_name,
			    LOG_LSA * savept_lsa)
{
  int tran_index;
  TRAN_STATE state;

  tran_index = logtb_get_current_tran_index (thread_p);
  state = log_abort_partial (thread_p, savept_name, savept_lsa);

  if (!LSA_ISNULL (savept_lsa))
    {
      (void) locator_drop_transient_class_name_entries (thread_p, tran_index,
							savept_lsa);
    }

  return state;
}

/*
 * xtran_server_has_updated -  Has transaction updated the database ?
 *
 * return:
 *
 * NOTE: Find if the transaction has dirtied the database. We say that
 *              a transaction has updated the database, if it has log
 *              something and it has a write lock on an object, or if there
 *              has been an update to a remote database.
 */
bool
xtran_server_has_updated (THREAD_ENTRY * thread_p)
{
  return ((logtb_has_updated (thread_p) && lock_has_xlock (thread_p)));
}

/*
 * xtran_wait_server_active_trans -
 *
 * return:
 *
 * NOTE: wait for server threads with current tran index to finish
 */
int
xtran_wait_server_active_trans (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if defined(SERVER_MODE)
  int prev_thrd_cnt, thrd_cnt;
  CSS_CONN_ENTRY *p;
  int tran_index, client_id;
  bool continue_check;

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  p = thread_p->conn_entry;
  if (p == NULL)
    {
      return 0;
    }

  tran_index = thread_p->tran_index;
  client_id = p->client_id;

loop:
  prev_thrd_cnt = thread_has_threads (thread_p, tran_index, client_id);
  if (prev_thrd_cnt > 0)
    {
      if (!logtb_is_interrupted_tran (thread_p, false, &continue_check,
				      tran_index))
	{
	  logtb_set_tran_index_interrupt (thread_p, tran_index, true);
	}
    }

  while ((thrd_cnt = thread_has_threads (thread_p, tran_index, client_id))
	 >= prev_thrd_cnt && thrd_cnt > 0)
    {
      /* Some threads may wait for data from the m-driver.
       * It's possible from the fact that css_server_thread() is responsible
       * for
       * receiving every data from which is sent by a client and all
       * m-drivers.
       * We must have chance to receive data from them.
       */
      thread_sleep (10);	/* 10 msec */
    }

  if (thrd_cnt > 0)
    {
      goto loop;
    }

  logtb_set_tran_index_interrupt (thread_p, tran_index, false);

#endif /* SERVER_MODE */
  return 0;
}

/*
 * xtran_server_is_active_and_has_updated - Find if transaction is active and
 *					    has updated the database ?
 * return:
 *
 * NOTE: Find if the transaction is active and has dirtied the
 *              database. We say that a transaction has updated the database,
 *              if it has log something and it has a write lock on an object.
 */
int
xtran_server_is_active_and_has_updated (THREAD_ENTRY * thread_p)
{
  return (logtb_is_current_active (thread_p)
	  && xtran_server_has_updated (thread_p));
}

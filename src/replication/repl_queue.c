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
 * repl_queue.c -
 */

#ident "$Id$"

#include <sys/types.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/shm.h>

#include "repl_queue.h"
#include "error_manager.h"
#include "system_parameter.h"
#include "db_date.h"
#include "db.h"
#include "repl_common.h"

static CIRP_Q_ITEM *cirp_tran_q_push (CIRP_TRAN_Q * q,
				      TRANID trid,
				      LOG_LSA * tran_start_lsa,
				      LOG_LSA * commited_lsa,
				      LOG_LSA * repl_start_lsa);
static CIRP_Q_ITEM *cirp_tran_q_pop (CIRP_TRAN_Q * q);
static bool cirp_tran_q_is_empty (CIRP_TRAN_Q * q);
static bool cirp_tran_q_is_committed_and_empty (CIRP_TRAN_Q * q);
static void cirp_tran_q_clear_committed_item (CIRP_TRAN_Q * q);

/*
 * cirp_tran_q_is_full () -
 *    return: true or false
 *
 *    q(in):
 */
static bool
cirp_tran_q_is_full (CIRP_TRAN_Q * q)
{
  return (((q->rear + 1) % CIRP_TRAN_Q_SIZE) == q->boundary);
}

/*
 * cirp_tran_q_is_empty () -
 *    return: true or false
 *
 *    q(in):
 */
static bool
cirp_tran_q_is_empty (CIRP_TRAN_Q * q)
{
  return (q->rear == q->front);
}

/*
 * cirp_tran_q_is_committed_and_empty () -
 *    return: true or false
 *
 *    q(in):
 */
static bool
cirp_tran_q_is_committed_and_empty (CIRP_TRAN_Q * q)
{
  return (q->rear == q->boundary);
}

/*
 * cirp_tran_q_clear_committed_item () -
 *    q(in/out):
 */
static void
cirp_tran_q_clear_committed_item (CIRP_TRAN_Q * q)
{
  q->boundary = q->front;
}

/*
 * cirp_tran_q_push -
 *
 * return:
 * Note:
 */
static CIRP_Q_ITEM *
cirp_tran_q_push (CIRP_TRAN_Q * q, TRANID trid,
		  LOG_LSA * tran_start_lsa, LOG_LSA * commited_lsa,
		  LOG_LSA * repl_start_lsa)
{
  CIRP_Q_ITEM *p;

  if (((q->rear + 1) % CIRP_TRAN_Q_SIZE) == q->boundary)
    {
      /* queue full */
      return NULL;
    }

  p = &q->log_item[q->rear];

  q->rear = (q->rear + 1) % CIRP_TRAN_Q_SIZE;

  p->trid = trid;
  LSA_COPY (&p->tran_start_lsa, tran_start_lsa);
  LSA_COPY (&p->committed_lsa, commited_lsa);
  LSA_COPY (&p->repl_start_lsa, repl_start_lsa);

  q->num_item++;
  assert (q->num_item >= 0 && q->num_item <= CIRP_TRAN_Q_SIZE);

  return p;
}

/*
 * cirp_tran_q_pop -
 *
 * return:
 * Note:
 */
static CIRP_Q_ITEM *
cirp_tran_q_pop (CIRP_TRAN_Q * q)
{
  CIRP_Q_ITEM *p;

  if (q->front == q->rear)
    {
      /* queue empty */
      return NULL;
    }

  p = &q->log_item[q->front];

  q->front = (q->front + 1) % CIRP_TRAN_Q_SIZE;

  q->num_item--;
  assert (q->num_item >= 0 && q->num_item <= CIRP_TRAN_Q_SIZE);

  return p;
}

/*
 * cirp_analyzer_item_push -
 *
 * return:
 * Note:
 */
int
cirp_analyzer_item_push (int la_index, TRANID trid,
			 LOG_LSA * tran_start_lsa, LOG_LSA * committed_lsa,
			 LOG_LSA * repl_start_lsa)
{
  CIRP_APPLIER_INFO *applier;
  CIRP_Q_ITEM *item;
  int error = NO_ERROR;

  assert (la_index >= 0 && la_index < Repl_Info->num_applier);

  applier = &Repl_Info->applier_info[la_index];

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  item = cirp_tran_q_push (&applier->logq, trid,
			   tran_start_lsa, committed_lsa, repl_start_lsa);
  if (item == NULL)
    {
      /* queue is full */
      pthread_mutex_unlock (&applier->lock);

      error = ER_HA_JOB_QUEUE_FULL;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "jab queue full");
      return error;
    }

  if (applier->status == CIRP_AGENT_WAIT)
    {
      pthread_cond_signal (&applier->cond);
    }

  pthread_mutex_unlock (&applier->lock);

  assert (error == NO_ERROR);
  return error;
}

/*
 * cirp_applier_item_pop -
 *    return: error code
 *
 *    shm_applier(in/out):
 */
int
cirp_applier_item_pop (CIRP_APPLIER_INFO * applier, CIRP_Q_ITEM ** item)
{
  CIRP_Q_ITEM *log_item;
  int error = NO_ERROR;

  if (applier == NULL || item == NULL)
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid arguments");
      return error;
    }

  *item = NULL;

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  log_item = cirp_tran_q_pop (&applier->logq);
  if (applier->analyzer_status == CIRP_AGENT_WAIT)
    {
      pthread_cond_signal (&applier->cond);
    }

  pthread_mutex_unlock (&applier->lock);

  *item = log_item;

  return NO_ERROR;
}

/*
 * cirp_applier_clear_committed_item ()
 *    return: error code
 *
 *    (shm_applier(in/out):
 */
int
cirp_applier_clear_committed_item (CIRP_APPLIER_INFO * applier)
{
  int error = NO_ERROR;

  if (applier == NULL)
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      "Invalid arguments");
      return error;
    }

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  cirp_tran_q_clear_committed_item (&applier->logq);

  pthread_mutex_unlock (&applier->lock);

  return NO_ERROR;
}

/*
 * cirp_analyzer_is_applier_busy()-
 *    return: true or false
 *
 *    la_index(in):
 */
bool
cirp_analyzer_is_applier_busy (int la_index)
{
  CIRP_APPLIER_INFO *applier;
  int error = NO_ERROR;
  bool is_busy = true;

  assert (la_index >= 0 && la_index < Repl_Info->num_applier);

  applier = &Repl_Info->applier_info[la_index];

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  if (cirp_tran_q_is_committed_and_empty (&applier->logq))
    {
      /* all committed and empty */
      is_busy = false;
    }

  pthread_mutex_unlock (&applier->lock);

  return is_busy;
}

/*
 * cirp_pthread_cond_timedwait ()-
 *   return: error code
 *
 *   pcond(in):
 *   plock(in):
 *   wakeup_interval(in): msecs
 */
int
cirp_pthread_cond_timedwait (pthread_cond_t * pcond, pthread_mutex_t * plock,
			     int wakeup_interval)
{
  struct timespec wakeup_time = {
    0, 0
  };
  int error = NO_ERROR;
  int rv;

  clock_gettime (CLOCK_REALTIME, &wakeup_time);

  wakeup_time = timespec_add_msec (&wakeup_time, wakeup_interval);

  rv = pthread_cond_timedwait (pcond, plock, &wakeup_time);

  if (rp_need_restart () == true)
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "need_restart");

      return error;
    }

  if (rv != 0)
    {
      if (rv == ETIMEDOUT)
	{
	  return NO_ERROR;
	}
      error = ER_GENERIC_ERROR;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
			   "os error:");

      return error;
    }

  return NO_ERROR;
}

/*
 * cirp_analyzer_wait_for_queue()-
 *    return: error code
 *
 *    la_index(in):
 */
int
cirp_analyzer_wait_for_queue (int la_index)
{
  CIRP_APPLIER_INFO *applier;
  int wakeup_interval = 1;
  int error = NO_ERROR;

  assert (la_index >= 0 && la_index < Repl_Info->num_applier);

  applier = &Repl_Info->applier_info[la_index];

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  while (cirp_tran_q_is_full (&applier->logq))
    {
      applier->analyzer_status = CIRP_AGENT_WAIT;

      error = cirp_pthread_cond_timedwait (&applier->cond,
					   &applier->lock, wakeup_interval);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  applier->analyzer_status = CIRP_AGENT_BUSY;

  pthread_mutex_unlock (&applier->lock);

  return NO_ERROR;

exit_on_error:
  applier->analyzer_status = CIRP_AGENT_DEAD;

  pthread_mutex_unlock (&applier->lock);

  return error;
}

/*
 * cirp_analyzer_wait_tran_commit ()
 *   return: error code
 *
 *   la_index(in):
 *   lsa(in):
 */
int
cirp_analyzer_wait_tran_commit (int la_index, LOG_LSA * lsa)
{
  CIRP_APPLIER_INFO *applier;
  int error = NO_ERROR;
  int wakeup_interval = 10;

  assert (lsa != NULL);
  assert (la_index == DDL_APPLIER_INDEX || la_index == GLOBAL_APPLIER_INDEX);

  applier = &Repl_Info->applier_info[la_index];

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  while (LSA_LE (&applier->ct.committed_lsa, lsa))
    {
      applier->analyzer_status = CIRP_AGENT_WAIT;

      error = cirp_pthread_cond_timedwait (&applier->cond,
					   &applier->lock, wakeup_interval);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  applier->analyzer_status = CIRP_AGENT_BUSY;

  pthread_mutex_unlock (&applier->lock);

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  applier->analyzer_status = CIRP_AGENT_DEAD;

  pthread_mutex_unlock (&applier->lock);

  return error;
}

/*
 * cirp_applier_wait_for_queue ()
 *    return: error code
 *
 *    shm_applier(in/out):
 *    repl_shm(in):
 */
int
cirp_applier_wait_for_queue (CIRP_APPLIER_INFO * applier)
{
  int error = NO_ERROR;
  int wakeup_interval = 100;

  if (applier == NULL)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid arguments");
      return error;
    }

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  while (cirp_tran_q_is_empty (&applier->logq))
    {
      applier->status = CIRP_AGENT_WAIT;

      error = cirp_pthread_cond_timedwait (&applier->cond,
					   &applier->lock, wakeup_interval);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  applier->status = CIRP_AGENT_BUSY;

  pthread_mutex_unlock (&applier->lock);

  return NO_ERROR;

exit_on_error:
  pthread_mutex_unlock (&applier->lock);

  return error;
}

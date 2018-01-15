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
 * log_tran_table.c -
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>
#include <limits.h>
#include <sys/stat.h>
#include <assert.h>

#include "porting.h"
#include "xserver_interface.h"
#include "log_impl.h"
#include "log_manager.h"
#include "log_comm.h"
#include "recovery.h"
#include "system_parameter.h"
#include "release_string.h"
#include "memory_alloc.h"
#include "error_manager.h"
#include "storage_common.h"
#include "file_io.h"
#include "disk_manager.h"
#include "page_buffer.h"
#include "lock_manager.h"
#include "wait_for_graph.h"
#include "file_manager.h"
#include "critical_section.h"
#include "query_manager.h"
#include "perf_monitor.h"
#include "object_representation.h"
#include "connection_defs.h"
#include "connection_support.h"
#if defined(SERVER_MODE)
#include "server_support.h"
#include "connection_sr.h"
#include "thread.h"
#endif /* SERVER_MODE */
#include "rye_server_shm.h"

#if defined(SERVER_MODE) || defined(SA_MODE)
#include "repl_log.h"
#endif

#define NUM_TOTAL_TRAN_INDICES log_Gl.trantable.num_total_indices

#if defined (SERVER_MODE)
typedef struct tran_table_mutex TRAN_TABLE_MUTEX;
struct tran_table_mutex
{
  THREAD_ENTRY *holder;
  int lock_cnt;
  pthread_mutex_t lock;
};

static TRAN_TABLE_MUTEX tb_Mutex = { NULL, 0, PTHREAD_MUTEX_INITIALIZER };
#endif /* SERVER_MODE */

static const int LOG_MAX_NUM_CONTIGUOUS_TDES = INT_MAX / sizeof (LOG_TDES);
static const float LOG_EXPAND_TRANTABLE_RATIO = 1.25;	/* Increase table by 25% */
static const int LOG_TOPOPS_STACK_INCREMENT = 3;	/* No more than 3 nested
							 * top system operations
							 */
static const char *log_Client_id_unknown_string = "(unknown)";
static BOOT_CLIENT_CREDENTIAL log_Client_credential = {
  BOOT_CLIENT_SYSTEM_INTERNAL,	/* client_type */
  NULL,				/* client_info */
  NULL,				/* db_name */
  NULL,				/* db_user */
  NULL,				/* db_password */
  (char *) "(system)",		/* program_name */
  NULL,				/* login_name */
  NULL,				/* host_name */
  NULL,				/* preferred_hosts */
  0,				/* connect_order */
  -1				/* process_id */
};

static int block_Global_dml = false;

static int logtb_expand_trantable (THREAD_ENTRY * thread_p,
				   int num_new_indices);
static int logtb_allocate_tran_index (THREAD_ENTRY * thread_p, TRANID trid,
				      TRAN_STATE state,
				      const BOOT_CLIENT_CREDENTIAL *
				      client_credential,
				      TRAN_STATE * current_state,
				      int wait_msecs);
static void logtb_initialize_tdes (LOG_TDES * tdes, int tran_index);
static LOG_ADDR_TDESAREA *logtb_allocate_tdes_area (int num_indices);
static void logtb_initialize_trantable (TRANTABLE * trantable_p);
static int logtb_initialize_system_tdes (THREAD_ENTRY * thread_p);
static void logtb_set_number_of_assigned_tran_indices (int num_trans);
static void logtb_increment_number_of_assigned_tran_indices ();
static void logtb_decrement_number_of_assigned_tran_indices ();
static void logtb_set_number_of_total_tran_indices (int num_total_trans);
static bool logtb_is_interrupted_tdes (THREAD_ENTRY * thread_p,
				       LOG_TDES * tdes, bool clear,
				       bool * continue_checking);
static void logtb_dump_top_operations (FILE * out_fp,
				       LOG_TOPOPS_STACK * topops_p);
static void logtb_dump_tdes (FILE * out_fp, LOG_TDES * tdes);
static void logtb_set_tdes (THREAD_ENTRY * thread_p, LOG_TDES * tdes,
			    const BOOT_CLIENT_CREDENTIAL * client_credential,
			    int wait_msecs);
static void logtb_init_working_tran (WORKING_TRAN * working_tran);
static int logtb_append_working_tran (int tran_index);
static int logtb_remove_working_tran (int tran_index);
static int logtb_find_client_type (int tran_index);
static char *logtb_find_client_name (int tran_index);
static char *logtb_find_client_hostname (int tran_index);
static int logtb_find_shard_groupid (int tran_index);
static char *logtb_find_shard_lockname (int tran_index);

/*
 * logtb_realloc_topops_stack - realloc stack of top system operations
 *
 * return: stack or NULL
 *
 *   tdes(in): State structure of transaction to realloc stack
 *   num_elms(in):
 *
 * Note: Realloc the current transaction top system operation stack by
 *              the given number of entries.
 */
void *
logtb_realloc_topops_stack (LOG_TDES * tdes, int num_elms)
{
  size_t size;
  void *newptr;

  if (num_elms < LOG_TOPOPS_STACK_INCREMENT)
    {
      num_elms = LOG_TOPOPS_STACK_INCREMENT;
    }

  size = tdes->topops.max + num_elms;
  size = size * sizeof (*tdes->topops.stack);

  newptr = (struct log_topops_addresses *) realloc (tdes->topops.stack, size);
  if (newptr != NULL)
    {
      tdes->topops.stack = (struct log_topops_addresses *) newptr;
      if (tdes->topops.max == 0)
	{
	  tdes->topops.last = -1;
	}
      tdes->topops.max += num_elms;
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, size);
      return NULL;
    }
  return tdes->topops.stack;
}

/*
 * logtb_allocate_tdes_area -
 *
 * return:
 *
 *   num_indices(in):
 *
 * Note:
 */
static LOG_ADDR_TDESAREA *
logtb_allocate_tdes_area (int num_indices)
{
  LOG_ADDR_TDESAREA *area;	/* Contiguous area for new
				 * transaction indices
				 */
  LOG_TDES *tdes;		/* Transaction descriptor */
  int i, tran_index;
  size_t area_size;

  /*
   * Allocate an area for the transaction descriptors, set the address of
   * each transaction descriptor, and keep the address of the area for
   * deallocation purposes at shutdown time.
   */
  area_size = num_indices * sizeof (LOG_TDES) + sizeof (LOG_ADDR_TDESAREA);
  area = (LOG_ADDR_TDESAREA *) malloc (area_size);
  if (area == NULL)
    {
      return NULL;
    }

  area->tdesarea = ((LOG_TDES *) ((char *) area
				  + sizeof (LOG_ADDR_TDESAREA)));
  area->next = log_Gl.trantable.area;

  /*
   * Initialize every newly created transaction descriptor index
   */
  for (i = 0, tran_index = NUM_TOTAL_TRAN_INDICES;
       i < num_indices; tran_index++, i++)
    {
      tdes = log_Gl.trantable.all_tdes[tran_index] = &area->tdesarea[i];
      logtb_initialize_tdes (tdes, i);
    }

  return area;
}

/*
 * logtb_expand_trantable - expand the transaction table
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   num_new_indices(in): Number of indices of expansion.
 *                       (i.e., threads/clients of execution)
 *
 * Note: Expand the transaction table with the number of given indices.
 */
static int
logtb_expand_trantable (THREAD_ENTRY * thread_p, int num_new_indices)
{
  LOG_ADDR_TDESAREA *area;	/* Contiguous area for new transaction indices */
  int total_indices;		/* Total number of transaction indices */
#if 0
  int groupid_size, bitmap_size;
  char *groupid_bitmap;
#endif
  int i;
  int error_code = NO_ERROR;

#if defined(SERVER_MODE)
  /*
   * When second time this function invoked during normal processing,
   * just return.
   */
  total_indices = MAX_NTRANS;
  if (log_Gl.rcv_phase == LOG_RESTARTED
      && total_indices <= NUM_TOTAL_TRAN_INDICES)
    {
      return NO_ERROR;
    }
#endif /* SERVER_MODE */

  while (num_new_indices > LOG_MAX_NUM_CONTIGUOUS_TDES)
    {
      error_code =
	logtb_expand_trantable (thread_p, LOG_MAX_NUM_CONTIGUOUS_TDES);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
      num_new_indices -= LOG_MAX_NUM_CONTIGUOUS_TDES;
    }

  if (num_new_indices <= 0)
    {
      return NO_ERROR;
    }

#if defined(SERVER_MODE)
  if (log_Gl.rcv_phase != LOG_RESTARTED)
    {
      total_indices = NUM_TOTAL_TRAN_INDICES + num_new_indices;
    }
#else /* SERVER_MODE */
  total_indices = NUM_TOTAL_TRAN_INDICES + num_new_indices;
#endif

  /*
   * NOTE that this realloc is OK since we are in a critical section.
   * Nobody should have pointer to transaction table
   */
  i = total_indices * sizeof (*log_Gl.trantable.all_tdes);
  log_Gl.trantable.all_tdes =
    (LOG_TDES **) realloc (log_Gl.trantable.all_tdes, i);
  if (log_Gl.trantable.all_tdes == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, i);
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error;
    }

  area = logtb_allocate_tdes_area (num_new_indices);
  if (area == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error;
    }

  /*
   * Notify other modules of new number of transaction indices
   */
#if defined(ENABLE_UNUSED_FUNCTION)
  error_code = wfg_alloc_nodes (thread_p, total_indices);
  if (error_code != NO_ERROR)
    {
      free_and_init (area);
      goto error;
    }
#endif

  if (qmgr_allocate_tran_entries (thread_p, total_indices) != NO_ERROR)
    {
      free_and_init (area);
      error_code = ER_FAILED;
      goto error;
    }

  log_Gl.trantable.area = area;
  log_Gl.trantable.hint_free_index = NUM_TOTAL_TRAN_INDICES;
  logtb_set_number_of_total_tran_indices (total_indices);

  return error_code;

  /* **** */
error:
  return error_code;
}

/*
 * logtb_define_trantable -  define the transaction table
 *
 * return: nothing
 *
 *   num_expected_tran_indices(in): Number of expected concurrent transactions
 *                                 (i.e., threads/clients of execution)
 *   num_expected_locks(in): Number of expected locks
 *
 * Note: Define the transaction table which is used to support the
 *              number of expected transactions.
 */
void
logtb_define_trantable (THREAD_ENTRY * thread_p,
			int num_expected_tran_indices,
			UNUSED_ARG int num_expected_locks)
{
  logtb_set_to_system_tran_index (thread_p);

  LOG_CS_ENTER (thread_p);
  TR_TABLE_LOCK (thread_p);

  if (logpb_is_initialize_pool ())
    {
      logpb_finalize_pool ();
    }

  (void) logtb_define_trantable_log_latch (thread_p,
					   num_expected_tran_indices);

  logtb_set_to_system_tran_index (thread_p);

  TR_TABLE_UNLOCK (thread_p);
  LOG_CS_EXIT ();
}

/*
 * logtb_define_trantable_log_latch - define the transaction table
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   num_expected_tran_indices(in): Number of expected concurrent transactions
 *                                 (i.e., threads/clients of execution)
 *   num_expected_locks(in): Number of expected locks
 *
 * Note: This function is only called by the log manager when the log
 *              latch has already been acquired. (See logtb_define_trantable for
 *              other uses).
 */
int
logtb_define_trantable_log_latch (THREAD_ENTRY * thread_p,
				  int num_expected_tran_indices)
{
  int error_code = NO_ERROR;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  /*
   * for XA support: there is prepared transaction after recovery.
   *                 so, can not recreate transaction description
   *                 table after recovery.
   *
   * Total number of transaction descriptor is set to the value of
   * MAX_NTRANS
   */
  num_expected_tran_indices = MAX (num_expected_tran_indices, MAX_NTRANS);

  num_expected_tran_indices = MAX (num_expected_tran_indices,
				   LOG_SYSTEM_TRAN_INDEX + 1);

  /* If there is an already defined table, free such a table */
  if (log_Gl.trantable.area != NULL)
    {
      logtb_undefine_trantable (thread_p);
    }
  else
    {
      /* Initialize the transaction table as empty */
      logtb_initialize_trantable (&log_Gl.trantable);
    }

  /*
   * Create an area to keep the number of desired transaction descriptors
   */

  error_code = logtb_expand_trantable (thread_p, num_expected_tran_indices);
  if (error_code != NO_ERROR)
    {
      /*
       * Unable to create transaction table to hold the desired number
       * of indices. Probabely, a lot of indices were requested.
       * try again with defaults.
       */
      if (log_Gl.trantable.area != NULL)
	{
	  logtb_undefine_trantable (thread_p);
	}

#if defined(SERVER_MODE)
      if (num_expected_tran_indices <= LOG_ESTIMATE_NACTIVE_TRANS
	  || log_Gl.rcv_phase == LOG_RESTARTED)
#else /* SERVER_MODE */
      if (num_expected_tran_indices <= LOG_ESTIMATE_NACTIVE_TRANS)
#endif /* SERVER_MODE */
	{
	  /* Out of memory */
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_def_trantable");
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      else
	{
	  error_code =
	    logtb_define_trantable_log_latch (thread_p,
					      LOG_ESTIMATE_NACTIVE_TRANS);
	  return error_code;
	}
    }

  logtb_set_number_of_assigned_tran_indices (1);	/* sys tran */

  /*
   * Assign the first entry for the system transaction. System transaction
   * has an infinite timeout
   */
  error_code = logtb_initialize_system_tdes (thread_p);
  if (error_code != NO_ERROR)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_UNKNOWN_TRANINDEX, 1, LOG_SYSTEM_TRAN_INDEX);
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "log_def_trantable");
      return error_code;
    }

  logtb_set_to_system_tran_index (thread_p);

  /* Initialize the lock manager and the page buffer pool */
  error_code = lock_initialize ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = pgbuf_initialize ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = file_manager_initialize (thread_p);
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  return error_code;

error:
  logtb_undefine_trantable (thread_p);
  logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "log_def_trantable");

  return error_code;
}

/*
 * logtb_initialize_trantable -
 *
 * return: nothing
 *
 *   trantable_p(in/out):
 *
 * Note: .
 */
static void
logtb_initialize_trantable (TRANTABLE * trantable_p)
{
  trantable_p->num_total_indices = 0;
  trantable_p->num_assigned_indices = 1;
  trantable_p->hint_free_index = 0;
  trantable_p->num_interrupts = 0;
  trantable_p->working_tran_list_head = NULL;
  trantable_p->working_tran_list_tail = NULL;
  trantable_p->area = NULL;
  trantable_p->all_tdes = NULL;
}

/*
 * logtb_initialize_system_tdes -
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 * Note: .
 */
static int
logtb_initialize_system_tdes (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;

  tdes = LOG_FIND_TDES (LOG_SYSTEM_TRAN_INDEX);
  if (tdes == NULL)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_UNKNOWN_TRANINDEX, 1, LOG_SYSTEM_TRAN_INDEX);
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_initialize_system_tdes");
      return ER_LOG_UNKNOWN_TRANINDEX;
    }

  logtb_clear_tdes (thread_p, tdes);
  tdes->state = TRAN_ACTIVE;
  tdes->trid = LOG_SYSTEM_TRANID;
  tdes->wait_msecs = TRAN_LOCK_INFINITE_WAIT;
  tdes->client_id = NULL_CLIENT_ID;
  logtb_set_client_ids_all (&tdes->client, -1, NULL, NULL, NULL, NULL, NULL,
			    -1);
  tdes->query_timeout = 0;
  tdes->tran_abort_reason = TRAN_NORMAL;

  return NO_ERROR;
}

/*
 * logtb_undefine_trantable - undefine the transaction table
 *
 * return: nothing
 *
 * Note: Undefine and free the transaction table space.
 */
void
logtb_undefine_trantable (THREAD_ENTRY * thread_p)
{
  struct log_addr_tdesarea *area;
  LOG_TDES *tdes;		/* Transaction descriptor */
  int i;

  lock_finalize ();
  pgbuf_finalize ();
  (void) file_manager_finalize (thread_p);

  if (log_Gl.trantable.area != NULL)
    {
      /*
       * If any one of the transaction indices has coordinator info,
       * free this area
       */
      for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
	{
	  /*
	   * If there is any memory allocated in the transaction descriptor,
	   * release it
	   */
	  tdes = LOG_FIND_TDES (i);
	  if (tdes != NULL)
	    {
	      logtb_clear_tdes (thread_p, tdes);
	      csect_finalize_critical_section (&tdes->cs_topop);
	      if (tdes->topops.max != 0)
		{
		  free_and_init (tdes->topops.stack);
		  tdes->topops.max = 0;
		  tdes->topops.last = -1;
		}
	    }
	}

#if defined(ENABLE_UNUSED_FUNCTION)
      wfg_free_nodes (thread_p);
#endif

      if (log_Gl.trantable.all_tdes != NULL)
	{
	  free_and_init (log_Gl.trantable.all_tdes);
	}

      area = log_Gl.trantable.area;
      while (area != NULL)
	{
	  log_Gl.trantable.area = area->next;
	  free_and_init (area);
	}
    }

  logtb_initialize_trantable (&log_Gl.trantable);
}

/*
 * logtb_get_number_assigned_tran_indices - find number of transaction indices
 *
 * return: number of transaction indices
 *
 */
int
logtb_get_number_assigned_tran_indices (void)
{
  /* Do not use TR_TABLE_LOCK()/TR_TABLE_UNLOCK(),
   * Estimated value is sufficient for the caller
   */
  return log_Gl.trantable.num_assigned_indices;
}

/*
 * logtb_set_number_of_assigned_tran_indices - set the number of tran indices
 *
 * return: nothing
 *      num_trans(in): the number of assigned tran indices
 *
 * Note: Callers have to call this function in the 'TR_TABLE' critical section.
 */
static void
logtb_set_number_of_assigned_tran_indices (int num_trans)
{
  log_Gl.trantable.num_assigned_indices = num_trans;
}

/*
 * logtb_increment_number_of_assigned_tran_indices -
 *      increment the number of tran indices
 *
 * return: nothing
 *
 * Note: Callers have to call this function in the 'TR_TABLE' critical section.
 */
static void
logtb_increment_number_of_assigned_tran_indices (void)
{
  log_Gl.trantable.num_assigned_indices++;
}

/*
 * logtb_decrement_number_of_assigned_tran_indices -
 *      decrement the number of tran indices
 *
 * return: nothing
 *
 * Note: Callers have to call this function in the 'TR_TABLE' critical section.
 */
static void
logtb_decrement_number_of_assigned_tran_indices (void)
{
  log_Gl.trantable.num_assigned_indices--;
}

/*
 * logtb_get_number_of_total_tran_indices - find number of total transaction
 *                                          indices
 *
 * return: nothing
 *
 * Note: Find number of total transaction indices in the transaction
 *              table. Note that some of this indices may have not been
 *              assigned. See logtb_get_number_assigned_tran_indices.
 */
int
logtb_get_number_of_total_tran_indices (void)
{
  return log_Gl.trantable.num_total_indices;
}

/*
 * logtb_set_number_of_total_tran_indices - set the number of total tran indices
 *
 * return: nothing
 *      num_trans(in): the number of total tran indices
 *
 * Note: Callers have to call this function in the 'TR_TABLE' critical section.
 */
static void
logtb_set_number_of_total_tran_indices (int num_total_trans)
{
  log_Gl.trantable.num_total_indices = num_total_trans;
}

bool
logtb_am_i_dba_client (THREAD_ENTRY * thread_p)
{
  const char *db_user;

  db_user = logtb_find_current_client_name (thread_p);
  return (db_user != NULL && !strcasecmp (db_user, "DBA"));
}

/*
 * logtb_assign_tran_index - assign a transaction index for a sequence of
 *                        transactions (thread of execution.. a client)
 *
 * return: transaction index
 *
 *   client_prog_name(in): Name of the client program or NULL
 *   client_user_name(in): Name of the client user or NULL
 *   client_host_name(in): Name of the client host or NULL
 *   client_process_id(in): Identifier of the process of the host where the
 *                      client transaction runs.
 *   current_state(in/out): Set as a side effect to state of transaction, when
 *                      a valid pointer is given.
 *   wait_msecs(in): Wait for at least this number of milliseconds to acquire a
 *                      lock. Negative value is infinite
 *
 * Note:Assign a transaction index for a sequence of transactions
 *              (i.e., a client) and initialize the state structure for the
 *              first transaction in the sequence. If trid is equal to
 *              NULL_TRANID, a transaction is assigned to the assigned
 *              structure and the transaction is declared active; otherwise,
 *              the given transaction with the given state is assigned to the
 *              index.
 *
 *       This function must be called when a client is restarted.
 */
int
logtb_assign_tran_index (THREAD_ENTRY * thread_p,
			 const BOOT_CLIENT_CREDENTIAL * client_credential,
			 TRAN_STATE * current_state, int wait_msecs)
{
  int tran_index;		/* The allocated transaction index */

#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }
#endif /* SERVER_MODE */

  logtb_set_to_system_tran_index (thread_p);

  TR_TABLE_LOCK (thread_p);
  tran_index =
    logtb_allocate_tran_index (thread_p, LOG_SYSTEM_TRANID, TRAN_ACTIVE,
			       client_credential, current_state, wait_msecs);
  TR_TABLE_UNLOCK (thread_p);

  if (tran_index != NULL_TRAN_INDEX)
    {
      logtb_set_current_tran_index (thread_p, tran_index);
    }
  else
    {
      logtb_set_to_system_tran_index (thread_p);
    }

  return tran_index;
}

/*
 * logtb_set_tdes -
 *
 * return:
 *
 *   tdes(in/out): Transaction descriptor
 *   client_prog_name(in): the name of the client program
 *   client_host_name(in): the name of the client host
 *   client_user_name(in): the name of the client user
 *   client_process_id(in): the process id of the client
 *   wait_msecs(in): Wait for at least this number of milliseconds to acquire a lock.
 */
static void
logtb_set_tdes (UNUSED_ARG THREAD_ENTRY * thread_p, LOG_TDES * tdes,
		const BOOT_CLIENT_CREDENTIAL * client_credential,
		int wait_msecs)
{
#if defined(SERVER_MODE)
  CSS_CONN_ENTRY *conn;
#endif /* SERVER_MODE */

  if (client_credential == NULL)
    {
      client_credential = &log_Client_credential;
    }
  logtb_set_client_ids_all (&tdes->client, client_credential->client_type,
			    client_credential->client_info,
			    client_credential->db_user,
			    client_credential->program_name,
			    client_credential->login_name,
			    client_credential->host_name,
			    client_credential->process_id);
#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  conn = thread_p->conn_entry;
  if (conn != NULL)
    {
      tdes->client_id = conn->client_id;
    }
  else
    {
      tdes->client_id = NULL_CLIENT_ID;
    }
#else /* SERVER_MODE */
  tdes->client_id = NULL_CLIENT_ID;
#endif /* SERVER_MODE */
  tdes->wait_msecs = wait_msecs;
  tdes->interrupt = false;
  tdes->topops.stack = NULL;
  tdes->topops.max = 0;
  tdes->topops.last = -1;
  tdes->modified_class_list = NULL;
  tdes->num_transient_classnames = 0;
  tdes->first_save_entry = NULL;
  tdes->num_new_files = 0;
  tdes->num_new_tmp_files = 0;
}

/*
 * logtb_allocate_tran_index - allocate a transaction index for a sequence of
 *                       transactions (thread of execution.. a client)
 *
 * return: tran_index or NULL_TRAN_INDEX
 *
 *   trid(in): Transaction identifier or NULL_TRANID
 *   state(in): Transaction state (Usually active)
 *   client_prog_name(in): Name of the client program or NULL
 *   client_user_name(in): Name of the client user or NULL
 *   client_host_name(in): Name of the client host or NULL
 *   client_process_id(in): Identifier of the process of the host where the
 *                      client transaction runs.
 *   current_state(in/out): Set as a side effect to state of transaction, when
 *                      a valid pointer is given.
 *   wait_msecs(in): Wait for at least this number of milliseconds to acquire a
 *                      lock. That is, wait this much before the transaction
 *                      is timed out. Negative value is infinite.
 *
 * Note:Allocate a transaction index for a sequence of transactions
 *              (i.e., a client) and initialize the state structure for the
 *              first transaction in the sequence. If trid is equal to
 *              NULL_TRANID, a transaction is assigned to the assigned
 *              structure and the transaction is declared active; otherwise,
 *              the given transaction with the given state is assigned to the
 *              index. If the given client user has a dangling entry due to
 *              client loose ends, this index is attached to it.
 *
 *       This function is only called by the log manager when the log
 *              latch has already been acquired. (See logtb_assign_tran_index)
 */
static int
logtb_allocate_tran_index (THREAD_ENTRY * thread_p, TRANID trid,
			   TRAN_STATE state,
			   const BOOT_CLIENT_CREDENTIAL * client_credential,
			   TRAN_STATE * current_state, int wait_msecs)
{
  int i;
  int visited_loop_start_pos;
  LOG_TDES *tdes;		/* Transaction descriptor */
  int tran_index;		/* The assigned index */
  UNUSED_VAR int save_tran_index;		/* Save as a good index to assign */
  int assigned_tran_indics, total_tran_indices;

#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }
#endif /* SERVER_MODE */

  assert (trid != NULL_TRANID);

  save_tran_index = tran_index = NULL_TRAN_INDEX;

  /* Is there any free index ? */
  assigned_tran_indics = logtb_get_number_assigned_tran_indices ();
  total_tran_indices = logtb_get_number_of_total_tran_indices ();
  if (assigned_tran_indics >= total_tran_indices)
    {
#if defined(SERVER_MODE)
      /* When normal processing, we never expand trantable */
      if (log_Gl.rcv_phase == LOG_RESTARTED && total_tran_indices > 0)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_TM_TOO_MANY_CLIENTS, 1, total_tran_indices - 1);
	  return NULL_TRAN_INDEX;
	}
#endif /* SERVER_MODE */

      i = (int) (((float) total_tran_indices * LOG_EXPAND_TRANTABLE_RATIO)
		 + 0.5);
      if (logtb_expand_trantable (thread_p, i) != NO_ERROR)
	{
	  /* Out of memory or something like that */
	  return NULL_TRAN_INDEX;
	}
    }

  /*
   * Note that we could have found the entry already and it may be stored in
   * tran_index.
   */
  for (i = log_Gl.trantable.hint_free_index, visited_loop_start_pos = 0;
       tran_index == NULL_TRAN_INDEX && visited_loop_start_pos < 2;
       i = (i + 1) % NUM_TOTAL_TRAN_INDICES)
    {
      tdes = LOG_FIND_TDES (i);
      if (tdes != NULL && tdes->client_id == NULL_CLIENT_ID
	  && tdes->trid == NULL_TRANID)
	{
	  tran_index = i;
	}
      if (i == log_Gl.trantable.hint_free_index)
	{
	  visited_loop_start_pos++;
	}
    }

  if (tran_index != NULL_TRAN_INDEX)
    {
      log_Gl.trantable.hint_free_index =
	(tran_index + 1) % NUM_TOTAL_TRAN_INDICES;

      logtb_increment_number_of_assigned_tran_indices ();

      tdes = LOG_FIND_TDES (tran_index);
      if (tdes == NULL)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_UNKNOWN_TRANINDEX, 1, tran_index);
	  return NULL_TRAN_INDEX;
	}

      logtb_clear_tdes (thread_p, tdes);
      logtb_set_tdes (thread_p, tdes, client_credential, wait_msecs);

      tdes->trid = trid;
      tdes->state = state;

      if (current_state)
	{
	  *current_state = state;
	}

      logtb_set_current_tran_index (thread_p, tran_index);

      tdes->tran_abort_reason = TRAN_NORMAL;
    }

  return tran_index;
}

int
logtb_is_tran_modification_disabled (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);

  if (tdes == NULL)
    {
      return db_Disable_modifications;
    }

  return tdes->disable_modifications;
}

/*
 * logtb_rv_find_allocate_tran_index - find/alloc a transaction during the recovery
 *                         analysis process
 *
 * return: The transaction descriptor
 *
 *   trid(in): The desired transaction identifier
 *   log_lsa(in): Log address where the transaction was seen in the log
 *
 * Note: Find or allocate the transaction descriptor for the given
 *              transaction identifier. If the descriptor was allocated, it is
 *              assumed that this is the first time the transaction is seen.
 *              Thus, the head of the transaction in the log is located at the
 *              givel location (i.e., log_lsa.pageid, log_offset).
 *       This function should be called only by the recovery process
 *              (the analysis phase).
 */
LOG_TDES *
logtb_rv_find_allocate_tran_index (THREAD_ENTRY * thread_p, TRANID trid,
				   const LOG_LSA * log_lsa)
{
  LOG_TDES *tdes;		/* Transaction descriptor */
  int tran_index;

  /*
   * If this is the first time, the transaction is seen. Assign a new
   * index to describe it and assume that the transaction was active
   * at the time of the crash, and thus it will be unilateraly aborted
   */
  tran_index = logtb_find_tran_index (thread_p, trid);
  if (tran_index == NULL_TRAN_INDEX)
    {
      /* Define the index */
      tran_index = logtb_allocate_tran_index (thread_p, trid,
					      TRAN_UNACTIVE_UNILATERALLY_ABORTED,
					      NULL, NULL,
					      TRAN_LOCK_INFINITE_WAIT);
      tdes = LOG_FIND_TDES (tran_index);
      if (tran_index == NULL_TRAN_INDEX || tdes == NULL)
	{
	  /*
	   * Unable to assign a transaction index. The recovery process
	   * cannot continue
	   */
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_recovery_find_or_alloc");
	  return NULL;
	}
      else
	{
	  LSA_COPY (&tdes->begin_lsa, log_lsa);
	}
    }
  else
    {
      tdes = LOG_FIND_TDES (tran_index);
    }

  return tdes;
}

/*
 * logtb_release_tran_index - return an assigned transaction index
 *
 * return: nothing
 *
 *   tran_index(in): Transaction index
 *
 * Note: Return a transaction index which was used for a sequence of
 *              transactions (i.e., a client).
 *
 *       This function must be called when a client is shutdown (i.e.,
 *              unregistered).
 */
void
logtb_release_tran_index (THREAD_ENTRY * thread_p, int tran_index)
{
  LOG_TDES *tdes;		/* Transaction descriptor */

  qmgr_clear_trans_wakeup (thread_p, tran_index, true, false);

  tdes = LOG_FIND_TDES (tran_index);
  if (tran_index != LOG_SYSTEM_TRAN_INDEX && tdes != NULL)
    {

      TR_TABLE_LOCK (thread_p);

      /*
       * Free the top system operation stack since the transaction entry may
       * not be freed (i.e., left as loose end distributed transaction)
       */
      if (tdes->topops.max != 0)
	{
	  free_and_init (tdes->topops.stack);
	  tdes->topops.max = 0;
	  tdes->topops.last = -1;
	}

      logtb_free_tran_index (thread_p, tran_index);

      TR_TABLE_UNLOCK (thread_p);
    }
}

/*
 * logtb_free_tran_index - free a transaction index
 *
 * return: nothing
 *
 *   tran_index(in): Transaction index
 *
 * Note: Free a transaction index which was used for a sequence of
 *              transactions (i.e., a client).
 *
 *       This function is only called by the log manager when the log
 *              latch has already been acquired. (See logtb_release_tran_index
 *              for other cases).
 */
void
logtb_free_tran_index (THREAD_ENTRY * thread_p, int tran_index)
{
  LOG_TDES *tdes;		/* Transaction descriptor */
  int log_tran_index;

#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }
#endif /* SERVER_MODE */

  log_tran_index = logtb_get_current_tran_index (thread_p);

  tdes = LOG_FIND_TDES (tran_index);
  if (tran_index > NUM_TOTAL_TRAN_INDICES || tdes == NULL)
    {
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE, "log_free_tran_index: Unknown index = %d."
		    " Operation is ignored", tran_index);
#endif /* RYE_DEBUG */
      return;
    }

  logtb_clear_tdes (thread_p, tdes);
  if (tdes->repl_records)
    {
      free_and_init (tdes->repl_records);
    }
  tdes->num_repl_records = 0;
  if (tdes->topops.max != 0)
    {
      free_and_init (tdes->topops.stack);
      tdes->topops.max = 0;
      tdes->topops.last = -1;
    }

  if (tran_index != LOG_SYSTEM_TRAN_INDEX)
    {
      tdes->trid = NULL_TRANID;
      tdes->client_id = NULL_CLIENT_ID;

      TR_TABLE_LOCK (thread_p);
      logtb_decrement_number_of_assigned_tran_indices ();
      if (log_Gl.trantable.hint_free_index > tran_index)
	{
	  log_Gl.trantable.hint_free_index = tran_index;
	}
      TR_TABLE_UNLOCK (thread_p);

      if (log_tran_index == tran_index)
	{
	  if (!LOG_ISRESTARTED ())
	    {
	      log_tran_index = LOG_SYSTEM_TRAN_INDEX;
	    }
	  else
	    {
	      log_tran_index = NULL_TRAN_INDEX;
	    }

	  logtb_set_current_tran_index (thread_p, log_tran_index);
	}
    }
}

/*
 * logtb_free_tran_index_with_undo_lsa - free tranindex with lsa
 *
 * return: nothing
 *
 *   undo_lsa(in): Undo log sequence address
 *
 * Note: Remove the transaction index associated with the undo LSA.
 *              This function execute a sequential search on the transaction
 *              table to find out the transaction with such lsa. This
 *              sequential scan is OK since this function is only called when
 *              a system error happens, which in principle never happen.
 */
void
logtb_free_tran_index_with_undo_lsa (THREAD_ENTRY * thread_p,
				     const LOG_LSA * undo_lsa)
{
  int i;
  LOG_TDES *tdes;		/* Transaction descriptor */

  TR_TABLE_LOCK (thread_p);

  if (undo_lsa != NULL && !LSA_ISNULL (undo_lsa))
    {
      for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
	{
	  if (i != LOG_SYSTEM_TRAN_INDEX)
	    {
	      tdes = LOG_FIND_TDES (i);
	      if (tdes != NULL
		  && tdes->trid != NULL_TRANID
		  && tdes->state == TRAN_UNACTIVE_UNILATERALLY_ABORTED
		  && LSA_EQ (undo_lsa, &tdes->undo_nxlsa))
		{
		  logtb_free_tran_index (thread_p, i);
		}
	    }
	}
    }

  TR_TABLE_UNLOCK (thread_p);
}

/*
 * logtb_dump_tdes -
 *
 * return: nothing
 *
 *   tdes(in):
 *
 * Note:
 */
static void
logtb_dump_tdes (FILE * out_fp, LOG_TDES * tdes)
{
  fprintf (out_fp, "Tran_index = %2d, Trid = %d,\n"
	   "    State = %s,\n"
	   "    Isolation = %s,\n"
	   "    Wait_msecs = %d\n"
	   "    Head_lsa = %lld|%d, Tail_lsa = %lld|%d,"
	   " Postpone_lsa = %lld|%d,\n"
	   "    SaveLSA = %lld|%d, UndoNextLSA = %lld|%d,\n"
	   "    Client_User: (Type = %d, User = %s, Program = %s, "
	   "Login = %s, Host = %s, Pid = %d)\n"
	   "\n",
	   tdes->tran_index, tdes->trid,
	   log_state_string (tdes->state),
	   log_isolation_string (TRAN_DEFAULT_ISOLATION),
	   tdes->wait_msecs,
	   (long long int) tdes->begin_lsa.pageid, tdes->begin_lsa.offset,
	   (long long int) tdes->last_lsa.pageid, tdes->last_lsa.offset,
	   (long long int) tdes->posp_nxlsa.pageid, tdes->posp_nxlsa.offset,
	   (long long int) tdes->savept_lsa.pageid, tdes->savept_lsa.offset,
	   (long long int) tdes->undo_nxlsa.pageid, tdes->undo_nxlsa.offset,
	   tdes->client.client_type, tdes->client.db_user,
	   tdes->client.program_name, tdes->client.login_name,
	   tdes->client.host_name, tdes->client.process_id);

  if (tdes->topops.max != 0 && tdes->topops.last >= 0)
    {
      logtb_dump_top_operations (out_fp, &tdes->topops);
    }
}

/*
 * logtb_dump_top_operations -
 *
 * return: nothing
 *
 *   tdes(in):
 *
 * Note:
 */
static void
logtb_dump_top_operations (FILE * out_fp, LOG_TOPOPS_STACK * topops_p)
{
  int i;

  fprintf (out_fp, "    Active top system operations for tran:\n");
  for (i = topops_p->last; i >= 0; i--)
    {
      fprintf (out_fp, " Head = %lld|%d, Posp_Head = %lld|%d\n",
	       (long long int) topops_p->stack[i].lastparent_lsa.pageid,
	       topops_p->stack[i].lastparent_lsa.offset,
	       (long long int) topops_p->stack[i].posp_lsa.pageid,
	       topops_p->stack[i].posp_lsa.offset);
    }
}

/*
 * xlogtb_dump_trantable - dump the transaction table
 *
 * return: nothing
 *
 * Note: Dump the transaction state table.
 *              This function is used for debugging purposes.
 */
void
xlogtb_dump_trantable (UNUSED_ARG THREAD_ENTRY * thread_p, FILE * out_fp)
{
  int i;
  LOG_TDES *tdes;		/* Transaction descriptor */

  fprintf (out_fp, "\n ** DUMPING TABLE OF ACTIVE TRANSACTIONS **\n");

  TR_TABLE_LOCK (thread_p);

  for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
    {
      tdes = LOG_FIND_TDES (i);
      if (tdes == NULL || tdes->trid == NULL_TRANID)
	{
	  fprintf (out_fp, "Tran_index = %2d... Free transaction index\n", i);
	}
      else
	{
	  logtb_dump_tdes (out_fp, tdes);
	}
    }

  TR_TABLE_UNLOCK (thread_p);

  fprintf (out_fp, "\n");
}

/*
 * logtb_commit_lsa
 *   return: error code
 */
int
logtb_commit_lsa (THREAD_ENTRY * thread_p)
{
  LOG_LSA min_lsa, commit_lsa;
  bool exists_ddl_tran = false;

  logtb_find_smallest_begin_lsa_without_ddl_tran (thread_p, &min_lsa,
						  &exists_ddl_tran);
  if (exists_ddl_tran == false)
    {
      LSA_SET_NULL (&min_lsa);
      logtb_set_commit_lsa (&min_lsa);
    }
  else
    {
      if (LSA_ISNULL (&min_lsa))
	{
	  prior_lsa_get_current_lsa (thread_p, &min_lsa);
	}

#if !defined(NDEBUG)
      logtb_get_commit_lsa (&commit_lsa);

      assert (!LSA_ISNULL (&min_lsa));
      assert (LSA_GE (&min_lsa, &commit_lsa));
#endif

      logtb_set_commit_lsa (&min_lsa);
    }

  return NO_ERROR;
}

/*
 * logtb_clear_tdes - clear the transaction descriptor
 *
 * return: nothing..
 *
 *   tdes(in/out): Transaction descriptor
 */
void
logtb_clear_tdes (THREAD_ENTRY * thread_p, LOG_TDES * tdes)
{
  int i, j;
  DB_VALUE *dbval;
  LOG_LSA current_begin_lsa, commit_lsa;
  bool need_commit_lsa = false;
  int error = NO_ERROR;

  TR_TABLE_LOCK (thread_p);

  if (tdes->tran_index == LOG_SYSTEM_TRAN_INDEX)
    {
      tdes->trid = LOG_SYSTEM_TRANID;
    }
  else
    {
      tdes->trid = NULL_TRANID;
    }

  if (tdes->type == TRAN_TYPE_DDL || tdes->type == TRAN_TYPE_DML)
    {
      error = logtb_remove_working_tran (tdes->tran_index);
      assert (error == NO_ERROR);

      if (tdes->type == TRAN_TYPE_DML)
	{
	  need_commit_lsa = true;
	}
    }

  tdes->type = TRAN_TYPE_UNKNOWN;
  tdes->state = TRAN_UNACTIVE_UNKNOWN;

  LSA_COPY (&current_begin_lsa, &tdes->begin_lsa);
  LSA_SET_NULL (&tdes->begin_lsa);

  LSA_SET_NULL (&tdes->last_lsa);
  LSA_SET_NULL (&tdes->undo_nxlsa);
  LSA_SET_NULL (&tdes->posp_nxlsa);
  LSA_SET_NULL (&tdes->savept_lsa);
  LSA_SET_NULL (&tdes->topop_lsa);
  LSA_SET_NULL (&tdes->tail_topresult_lsa);
  tdes->topops.last = -1;

  logtb_get_commit_lsa (&commit_lsa);
  if (!LSA_ISNULL (&commit_lsa) && need_commit_lsa == true)
    {
      assert (LSA_LE (&commit_lsa, &current_begin_lsa));

      logtb_commit_lsa (thread_p);
    }

  if (tdes->interrupt == true)
    {
      tdes->interrupt = false;
      log_Gl.trantable.num_interrupts--;
    }
  tdes->modified_class_list = NULL;

  for (i = 0; i < tdes->cur_repl_record; i++)
    {
      if (tdes->repl_records[i].repl_data)
	{
	  free_and_init (tdes->repl_records[i].repl_data);
	}
    }

  for (i = 0; i < tdes->num_exec_queries && i < MAX_NUM_EXEC_QUERY_HISTORY;
       i++)
    {
      if (tdes->bind_history[i].vals == NULL)
	{
	  continue;
	}

      dbval = tdes->bind_history[i].vals;
      for (j = 0; j < tdes->bind_history[i].size; j++)
	{
	  db_value_clear (dbval);
	  dbval++;
	}

      free_and_init (tdes->bind_history[i].vals);
      tdes->bind_history[i].size = 0;
    }

  pr_clear_value (&tdes->tran_shard_key);
  DB_MAKE_NULL (&tdes->tran_shard_key);

  tdes->cur_repl_record = 0;
  tdes->append_repl_recidx = -1;
  LSA_SET_NULL (&tdes->repl_insert_lsa);
  LSA_SET_NULL (&tdes->repl_update_lsa);
  tdes->first_save_entry = NULL;
  tdes->num_new_files = 0;
  tdes->num_new_tmp_files = 0;
  tdes->query_timeout = 0;
  tdes->query_start_time = 0;
  tdes->tran_start_time = 0;
  XASL_ID_SET_NULL (&tdes->xasl_id);
  tdes->waiting_for_res = NULL;
  tdes->tran_abort_reason = TRAN_NORMAL;
  tdes->num_exec_queries = 0;
  tdes->suppress_replication = 0;

  tdes->disable_modifications = 0;

  tdes->tran_group_id = NULL_GROUPID;	/* init */

  if (tdes->bk_session != NULL)
    {
#if defined(SERVER_MODE)
      LOG_CS_ENTER (thread_p);
      if (tdes->bk_session->saved_run_nxchkpt_atpageid != NULL_PAGEID)
	{
	  log_Gl.run_nxchkpt_atpageid =
	    tdes->bk_session->saved_run_nxchkpt_atpageid;
	}
      log_Gl.backup_in_progress = false;
      LOG_CS_EXIT ();
#endif /* SERVER_MODE */
      free_and_init (tdes->bk_session);
    }

  TR_TABLE_UNLOCK (thread_p);
}

/*
 * logtb_get_commit_lsa -
 *
 */
void
logtb_get_commit_lsa (LOG_LSA * commit_lsa_p)
{
  volatile INT64 tmp_int64;

  tmp_int64 = ATOMIC_INC_64 ((INT64 *) (&log_Gl.commit_lsa), 0);
  memcpy (commit_lsa_p, (LOG_LSA *) (&tmp_int64), sizeof (LOG_LSA));
}

/*
 * logtb_set_commit_lsa -
 */
void
logtb_set_commit_lsa (LOG_LSA * lsa)
{
  UINT64 tmp_int64;

  tmp_int64 = *((INT64 *) (lsa));
  ATOMIC_TAS_64 ((INT64 *) (&log_Gl.commit_lsa), tmp_int64);
}

/*
 * logtb_initialize_tdes - initialize the transaction descriptor
 *
 * return: nothing..
 *
 *   tdes(in/out): Transaction descriptor
 *   tran_index(in): Transaction index
 */
static void
logtb_initialize_tdes (LOG_TDES * tdes, int tran_index)
{
  int i;

  tdes->tran_index = tran_index;
  tdes->type = TRAN_TYPE_UNKNOWN;
  tdes->trid = NULL_TRANID;
  tdes->state = TRAN_UNACTIVE_UNKNOWN;
  tdes->client_id = NULL_CLIENT_ID;
  tdes->client.client_type = BOOT_CLIENT_UNKNOWN;
  tdes->interrupt = false;
  tdes->wait_msecs = TRAN_LOCK_INFINITE_WAIT;
  LSA_SET_NULL (&tdes->begin_lsa);
  LSA_SET_NULL (&tdes->last_lsa);
  LSA_SET_NULL (&tdes->undo_nxlsa);
  LSA_SET_NULL (&tdes->posp_nxlsa);
  LSA_SET_NULL (&tdes->savept_lsa);
  LSA_SET_NULL (&tdes->topop_lsa);
  LSA_SET_NULL (&tdes->tail_topresult_lsa);

  tdes->cs_topop.cs_index = CSECT_UNKNOWN; /* leave me as is */
  tdes->cs_topop.name = NULL; /* leave me as is */
  csect_initialize_critical_section (&tdes->cs_topop);

  tdes->topops.stack = NULL;
  tdes->topops.last = -1;
  tdes->topops.max = 0;
  tdes->num_transient_classnames = 0;
  tdes->num_repl_records = 0;
  tdes->cur_repl_record = 0;
  tdes->append_repl_recidx = -1;
  tdes->repl_records = NULL;
  LSA_SET_NULL (&tdes->repl_insert_lsa);
  LSA_SET_NULL (&tdes->repl_update_lsa);
  tdes->first_save_entry = NULL;
  tdes->num_new_files = 0;
  tdes->num_new_tmp_files = 0;
  tdes->suppress_replication = 0;
  tdes->query_timeout = 0;
  tdes->query_start_time = 0;
  tdes->tran_start_time = 0;
  XASL_ID_SET_NULL (&tdes->xasl_id);
  tdes->waiting_for_res = NULL;
  tdes->disable_modifications = db_Disable_modifications;
  tdes->tran_abort_reason = TRAN_NORMAL;
  tdes->num_exec_queries = 0;

  for (i = 0; i < MAX_NUM_EXEC_QUERY_HISTORY; i++)
    {
      tdes->bind_history[i].size = 0;
      tdes->bind_history[i].vals = NULL;
    }

  DB_MAKE_NULL (&tdes->tran_shard_key);
  tdes->tran_group_id = NULL_GROUPID;	/* init */
  tdes->bk_session = NULL;
}

/*
 * logtb_start_transaction_if_needed -
 *
 *   return: transaction ID
 *
 */
int
logtb_start_transaction_if_needed (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;
  int trid = NULL_TRANID;

  tdes = logtb_get_current_tdes (thread_p);
  if (tdes == NULL)
    {
      goto exit_on_error;
    }

  if (LSA_ISNULL (&tdes->begin_lsa))
    {
      trid = logtb_get_new_tran_id (thread_p, tdes);
      if (BOOT_WRITE_ON_STANDY_CLIENT_TYPE (tdes->client.client_type))
	{
	  tdes->disable_modifications = 0;
	}
      else
	{
	  tdes->disable_modifications = db_Disable_modifications;
	}
    }
  else
    {
      trid = tdes->trid;
    }

  assert (trid != NULL_TRANID);

  return trid;

exit_on_error:

  return NULL_TRANID;
}

/*
 * logtb_init_working_tran() -
 *    return:
 *
 *    working_tran(out):
 */
static void
logtb_init_working_tran (WORKING_TRAN * working_tran)
{
  working_tran->tran_index = NULL_TRAN_INDEX;
  working_tran->next = NULL;
}

/*
 * logtb_append_working_tran() -
 *    return: NO_ERROR or error code
 *
 *    tran_index(in):
 */
static int
logtb_append_working_tran (int tran_index)
{
  int error = NO_ERROR;
  WORKING_TRAN *working_tran = NULL;

  working_tran = (WORKING_TRAN *) malloc (sizeof (WORKING_TRAN));
  if (working_tran == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, sizeof (WORKING_TRAN));

      GOTO_EXIT_ON_ERROR;
    }
  logtb_init_working_tran (working_tran);

  working_tran->tran_index = tran_index;
  if (log_Gl.trantable.working_tran_list_tail == NULL)
    {
      /* append first node */
      assert (log_Gl.trantable.working_tran_list_head == NULL);

      log_Gl.trantable.working_tran_list_head = working_tran;
    }
  else
    {
      log_Gl.trantable.working_tran_list_tail->next = working_tran;
#if !defined(NDEBUG)
      {
	LOG_TDES *tail_tdes, *current_tdes;

	tail_tdes =
	  LOG_FIND_TDES (log_Gl.trantable.working_tran_list_tail->tran_index);
	current_tdes = LOG_FIND_TDES (working_tran->tran_index);

	assert (tail_tdes != NULL && current_tdes != NULL
		&& tail_tdes->trid != NULL_TRANID
		&& current_tdes->trid != NULL_TRANID
		&& LSA_LE (&tail_tdes->begin_lsa, &current_tdes->begin_lsa));
      }
#endif
    }

  log_Gl.trantable.working_tran_list_tail = working_tran;


  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (working_tran != NULL)
    {
      free_and_init (working_tran);
    }

  return error;
}

/*
 * logtb_remove_working_tran() -
 *    return: NO_ERROR or error code
 *
 *    tran_index(in):
 */
static int
logtb_remove_working_tran (int tran_index)
{
  WORKING_TRAN *current, *prev;

  for (prev = NULL, current = log_Gl.trantable.working_tran_list_head;
       current != NULL; current = current->next)
    {
#if !defined(NDEBUG)
      if (prev != NULL)
	{
	  LOG_TDES *prev_tdes, *current_tdes;

	  prev_tdes = LOG_FIND_TDES (prev->tran_index);
	  current_tdes = LOG_FIND_TDES (current->tran_index);

	  assert (prev_tdes != NULL);
	  assert (current_tdes != NULL);
	  assert (LSA_LE (&prev_tdes->begin_lsa, &current_tdes->begin_lsa));
	}
#endif
      if (current->tran_index == tran_index)
	{
	  break;
	}

      prev = current;
    }

  if (current == NULL)
    {
      /* not found */
      return NO_ERROR;
    }

  if (prev == NULL)
    {
      /* remove first node */
      assert (current == log_Gl.trantable.working_tran_list_head);

      log_Gl.trantable.working_tran_list_head = current->next;
      current->next = NULL;

      if (current == log_Gl.trantable.working_tran_list_tail)
	{
	  log_Gl.trantable.working_tran_list_tail = NULL;
	  assert (log_Gl.trantable.working_tran_list_head == NULL);
	}
    }
  else
    {
      assert (current != log_Gl.trantable.working_tran_list_head);

      prev->next = current->next;
      current->next = NULL;

      if (current == log_Gl.trantable.working_tran_list_tail)
	{
	  log_Gl.trantable.working_tran_list_tail = prev;
	}
    }

  free_and_init (current);

  return NO_ERROR;
}

/*
 * logtb_get_new_tran_id - assign a new transaction identifier
 *
 * return: tranid
 *
 *   tdes(in/out): Transaction descriptor
 */
int
logtb_get_new_tran_id (THREAD_ENTRY * thread_p, LOG_TDES * tdes)
{
  int client_type;
  LOG_LSA current_lsa;

  assert (logtb_get_current_tdes (thread_p) == tdes);

  client_type = logtb_find_current_client_type (thread_p);

  TR_TABLE_LOCK (thread_p);

  assert (LSA_ISNULL (&tdes->begin_lsa));

  (void) prior_lsa_get_current_lsa (thread_p, &current_lsa);

  LSA_COPY (&tdes->begin_lsa, &current_lsa);

  tdes->trid = log_Gl.hdr.next_trid++;
  /* check overflow */
  if (tdes->trid < 0)
    {
      tdes->trid = LOG_SYSTEM_TRANID + 1;
      log_Gl.hdr.next_trid = tdes->trid + 1;
    }
  tdes->state = TRAN_ACTIVE;

  if (BOOT_READ_ONLY_CLIENT_TYPE (client_type)
      || css_is_client_ro_tran (thread_p) == true)
    {
      tdes->type = TRAN_TYPE_READ_ONLY;
    }
  else
    {
      tdes->type = TRAN_TYPE_DML;
      if (logtb_append_working_tran (tdes->tran_index) != NO_ERROR)
	{
	  tdes->trid = NULL_TRANID;
	  tdes->state = TRAN_UNACTIVE_UNKNOWN;
	}
    }

  TR_TABLE_UNLOCK (thread_p);

  return tdes->trid;
}

/*
 * logtb_find_tran_index - find index of transaction
 *
 * return: tran index
 *
 *   trid(in): Transaction identifier
 *
 * Note: Find the index of a transaction. This function execute a
 *              sequential search in the transaction table to find out the
 *              transaction index. The function bypasses the search if the
 *              trid belongs to the current transaction.
 *
 *       The assumption of this function is that the transaction table
 *              is not very big and that most of the time the search is
 *              avoided. if this assumption becomes false, we may need to
 *              define a hash table from trid to tdes to speed up the
 *              search.
 */
int
logtb_find_tran_index (THREAD_ENTRY * thread_p, TRANID trid)
{
  int i;
  int tran_index = NULL_TRAN_INDEX;	/* The transaction index */
  LOG_TDES *tdes;		/* Transaction descriptor */

  /* Avoid searching as much as possible */
  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes == NULL || tdes->trid != trid)
    {
      tran_index = NULL_TRAN_INDEX;

      TR_TABLE_LOCK (thread_p);
      /* Search the transaction table for such transaction */
      for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
	{
	  tdes = LOG_FIND_TDES (i);
	  if (tdes != NULL && tdes->trid != NULL_TRANID && tdes->trid == trid)
	    {
	      tran_index = i;
	      break;
	    }
	}
      TR_TABLE_UNLOCK (thread_p);
    }

  return tran_index;
}

/*
 * logtb_find_tranid - find TRANID of transaction index
 *
 * return: TRANID
 *
 *   tran_index(in): Index of transaction
 */
TRANID
logtb_find_tranid (int tran_index)
{
  LOG_TDES *tdes;		/* Transaction descriptor */
  TRANID trid = NULL_TRANID;	/* Transaction index */

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL)
    {
      trid = tdes->trid;
    }
  return trid;
}

/*
 * logtb_find_current_tranid - find current transaction identifier
 *
 * return: TRANID
 */
TRANID
logtb_find_current_tranid (THREAD_ENTRY * thread_p)
{
  return logtb_find_tranid (logtb_get_current_tran_index (thread_p));
}

/*
 * logtb_find_current_ha_status -
 *
 * return: ha status string
 */
const char *
logtb_find_current_ha_status (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  const char *status = NULL;

#if defined(SERVER_MODE)
//  csect_enter (thread_p, CSECT_HA_SERVER_STATE, INF_WAIT);

  status = HA_STATE_NAME (svr_shm_get_server_state ());

//  csect_exit (CSECT_HA_SERVER_STATE);
#else
  status = HA_STATE_NAME (HA_STATE_NA);
#endif

  return status;
}

/*
 * logtb_find_shard_groupid - find shard group id of transaction index
 *
 * return: shard_group ID
 *
 *   tran_index(in): Index of transaction
 */
static int
logtb_find_shard_groupid (int tran_index)
{
  LOG_TDES *tdes;		/* Transaction descriptor */
  int groupid = NULL_GROUPID;

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL)
    {
      groupid = tdes->tran_group_id;
    }

  return groupid;
}

/*
 * logtb_find_current_shard_groupid -
 *
 * return: shard_group ID
 */
int
logtb_find_current_shard_groupid (THREAD_ENTRY * thread_p)
{
  return logtb_find_shard_groupid (logtb_get_current_tran_index (thread_p));
}

/*
 * logtb_find_shard_lockname - find shard key of transaction index
 *
 * return: shard_lock name string
 *
 *   tran_index(in): Index of transaction
 */
static char *
logtb_find_shard_lockname (int tran_index)
{
  LOG_TDES *tdes;		/* Transaction descriptor */
  char *lockname = NULL;	/* shard key lock name */

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL)
    {
      lockname = DB_GET_STRING (&(tdes->tran_shard_key));
    }

  return lockname;
}

/*
 * logtb_find_current_shard_lockname -
 *
 * return: shard_lock name string
 */
char *
logtb_find_current_shard_lockname (THREAD_ENTRY * thread_p)
{
  return logtb_find_shard_lockname (logtb_get_current_tran_index (thread_p));
}

/*
 * logtb_set_client_ids_all - Set client identifications
 *
 * return: nothing..
 *
 *   client(in/out): The client block
 *   client_type(in):
 *   client_info(in):
 *   db_user(in):
 *   program_name(in):
 *   login_name(in):
 *   host_name(in):
 *   process_id(in):
 *
 * NOTE: Set client identifications.
 */
void
logtb_set_client_ids_all (LOG_CLIENTIDS * client, int client_type,
			  const char *client_info, const char *db_user,
			  const char *program_name, const char *login_name,
			  const char *host_name, int process_id)
{
  client->client_type = client_type;
  strncpy (client->client_info,
	   (client_info) ? client_info : "", DB_MAX_IDENTIFIER_LENGTH);
  client->client_info[DB_MAX_IDENTIFIER_LENGTH] = '\0';
  strncpy (client->db_user,
	   (db_user) ? db_user : log_Client_id_unknown_string,
	   LOG_USERNAME_MAX - 1);
  client->db_user[LOG_USERNAME_MAX - 1] = '\0';
  if (program_name == NULL
      || basename_r (program_name, client->program_name, PATH_MAX) < 0)
    {
      strncpy (client->program_name, log_Client_id_unknown_string, PATH_MAX);
    }
  client->program_name[PATH_MAX] = '\0';
  strncpy (client->login_name,
	   (login_name) ? login_name : log_Client_id_unknown_string,
	   L_cuserid);
  client->login_name[L_cuserid] = '\0';
  strncpy (client->host_name,
	   (host_name) ? host_name : log_Client_id_unknown_string,
	   MAXHOSTNAMELEN);
  client->host_name[MAXHOSTNAMELEN] = '\0';
  client->process_id = process_id;
}

/*
 * logtb_shutdown_write_normal_clients
 *   return:
 *
 */
void
logtb_shutdown_write_normal_clients (UNUSED_ARG THREAD_ENTRY * thread_p,
				     bool force)
{
  LOG_TDES *tdes;
  int i;

  TR_TABLE_LOCK (thread_p);

  for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
    {
      tdes = LOG_FIND_TDES (i);
      if (tdes != NULL && tdes->client_id != NULL_CLIENT_ID)
	{
	  if (BOOT_WRITE_NORMAL_CLIENT_TYPE (tdes->client.client_type)
	      && (force == true || tdes->trid == -1))
	    {
#if defined(SERVER_MODE)
	      css_shutdown_conn_by_tran_index (i);
#endif
	    }
	}
    }

  TR_TABLE_UNLOCK (thread_p);
}

/*
 * logtb_count_active_write_clients -
 *   return: number of clients
 */
int
logtb_count_active_write_clients (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;
  int i, count;

  TR_TABLE_LOCK (thread_p);

  count = 0;
  for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
    {
      tdes = LOG_FIND_TDES (i);
      if (tdes != NULL && tdes->client_id != NULL_CLIENT_ID)
	{
	  if (BOOT_WRITE_NORMAL_CLIENT_TYPE (tdes->client.client_type)
	      && tdes->trid != -1)
	    {
	      count++;
	    }
	}
    }
  TR_TABLE_UNLOCK (thread_p);

  return count;
}

/*
  * logtb_find_client_type - find client type of transaction index
   *
   * return: client type
   *
   *   tran_index(in): Index of transaction
   */
static int
logtb_find_client_type (int tran_index)
{
  LOG_TDES *tdes;

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL && tdes->client_id != NULL_CLIENT_ID)
    {
      return tdes->client.client_type;	/* BOOT_CLIENT_TYPE */
    }

  return -1;
}

/*
 * logtb_find_client_name - find client name of transaction index
 *
 * return: client name
 *
 *   tran_index(in): Index of transaction
 */
static char *
logtb_find_client_name (int tran_index)
{
  LOG_TDES *tdes;

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL && tdes->client_id != NULL_CLIENT_ID)
    {
      return tdes->client.db_user;
    }
  return NULL;
}

/*
 * logtb_find_client_hostname - find client hostname of transaction index
 *
 * return: client hostname
 *
 *   tran_index(in): Index of transaction
 */
static char *
logtb_find_client_hostname (int tran_index)
{
  LOG_TDES *tdes;

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL && tdes->client_id != NULL_CLIENT_ID)
    {
      return tdes->client.host_name;
    }

  return NULL;
}

/*
 * logtb_find_client_name_host_pid - find client identifiers(user_name,
 *                                host_name, host_pid) OF TRANSACTION INDEX
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   tran_index(in): Index of transaction
 *   client_prog_name(in/out): Name of the client program
 *   client_user_name(in/out): Name of the client user
 *   client_host_name(in/out): Name of the client host
 *   client_pid(in/out): Identifier of the process of the host where the client
 *                      client transaction runs.
 *
 * Note: Find the client user name, host name, and process identifier
 *              associated with the given transaction index.
 *
 *       The above pointers are valid until the client is unregister.
 */
int
logtb_find_client_name_host_pid (int tran_index, char **client_prog_name,
				 char **client_user_name,
				 char **client_host_name, int *client_pid)
{
  LOG_TDES *tdes;		/* Transaction descriptor */

  tdes = LOG_FIND_TDES (tran_index);

  if (tdes == NULL || tdes->trid == NULL_TRANID)
    {
      *client_prog_name = (char *) log_Client_id_unknown_string;
      *client_user_name = (char *) log_Client_id_unknown_string;
      *client_host_name = (char *) log_Client_id_unknown_string;
      *client_pid = -1;
      return ER_FAILED;
    }

  *client_prog_name = tdes->client.program_name;
  *client_user_name = tdes->client.db_user;
  *client_host_name = tdes->client.host_name;
  *client_pid = tdes->client.process_id;

  return NO_ERROR;
}

/*
 * logtb_get_current_client_ids -
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   client_info(out):
 */
int
logtb_get_current_client_ids (THREAD_ENTRY * thread_p,
			      LOG_CLIENTIDS * client_info)
{
  LOG_TDES *tdes;		/* Transaction descriptor */

  tdes = logtb_get_current_tdes (thread_p);
  if (tdes == NULL || tdes->trid == NULL_TRANID)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");

      return ER_GENERIC_ERROR;
    }

  *client_info = tdes->client;

  return NO_ERROR;
}

/*
 * xlogtb_get_pack_tran_table - return transaction info stored on transaction table
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   buffer_p(in/out): returned buffer poitner
 *   size_p(in/out): returned buffer size
 *
 * Note: This is a support function which is used mainly for the
 *              killtran utility. It returns a variety of client information
 *              of transactions.  This will be displayed by killtran so that
 *              the user can select which transaction id needs to be aborted.
 *
 *       The buffer is allocated using malloc and must be freed by the
 *       caller.
 */
int
xlogtb_get_pack_tran_table (UNUSED_ARG THREAD_ENTRY * thread_p,
			    char **buffer_p, int *size_p,
			    int include_query_exec_info)
{
  int error_code = NO_ERROR;
  int num_clients = 0;
  int i;
  int size;
  char *buffer, *ptr;
  LOG_TDES *tdes;		/* Transaction descriptor */
#if defined(SERVER_MODE)
  UINT64 current_msec = 0;
  TRAN_QUERY_EXEC_INFO *query_exec_info = NULL;
  XASL_CACHE_ENTRY *ent;
#endif

#if defined(SERVER_MODE)
  if (include_query_exec_info)
    {
      query_exec_info =
	(TRAN_QUERY_EXEC_INFO *) calloc (NUM_TOTAL_TRAN_INDICES,
					 sizeof (TRAN_QUERY_EXEC_INFO));

      if (query_exec_info == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, NUM_TOTAL_TRAN_INDICES * sizeof (TRAN_QUERY_EXEC_INFO));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      current_msec = thread_get_log_clock_msec ();
    }
#endif

  /* Note, we'll be in a critical section while we gather the data but
   * the section ends as soon as we return the data.  This means that the
   * transaction table can change after the information is used.
   */

  TR_TABLE_LOCK (thread_p);

  size = OR_INT_SIZE;		/* Number of client transactions */

  /* Find size of needed buffer */
  for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
    {
      tdes = LOG_FIND_TDES (i);
      if (!LOG_IS_CLIENT_ACTIVE (tdes))
	{
	  /* The index is not assigned or is system transaction (no-client) */
	  continue;
	}

      size += 3 * OR_INT_SIZE	/* tran index + tran state + process id */
	+ or_packed_string_length (tdes->client.db_user, NULL)
	+ or_packed_string_length (tdes->client.program_name, NULL)
	+ or_packed_string_length (tdes->client.login_name, NULL)
	+ or_packed_string_length (tdes->client.host_name, NULL);

#if defined(SERVER_MODE)
      if (include_query_exec_info)
	{
	  if (tdes->query_start_time > 0)
	    {
	      query_exec_info[i].query_time =
		(float) (current_msec - tdes->query_start_time) / 1000.0;
	    }

	  if (tdes->tran_start_time > 0)
	    {
	      query_exec_info[i].tran_time =
		(float) (current_msec - tdes->tran_start_time) / 1000.0;
	    }

	  lock_get_lock_holder_tran_index (thread_p,
					   &query_exec_info[i].
					   wait_for_tran_index_string,
					   tdes->tran_index,
					   tdes->waiting_for_res);

	  if (!XASL_ID_IS_NULL (&tdes->xasl_id))
	    {
	      /* retrieve query statement in the xasl_cache entry */
	      ent =
		qexec_check_xasl_cache_ent_by_xasl (thread_p, &tdes->xasl_id,
						    -1, NULL, false);

	      /* entry can be NULL, if xasl cache entry is deleted */
	      if (ent != NULL)
		{
		  if (ent->sql_info.sql_hash_text != NULL)
		    {
		      char *sql = ent->sql_info.sql_hash_text;

		      if (qmgr_get_sql_id (thread_p,
					   &query_exec_info[i].sql_id,
					   sql, strlen (sql)) != NO_ERROR)
			{
			  goto error;
			}

		      if (ent->sql_info.sql_user_text != NULL)
			{
			  sql = ent->sql_info.sql_user_text;
			}

		      /* copy query string */
		      query_exec_info[i].query_stmt = strdup (sql);
		      if (query_exec_info[i].query_stmt == NULL)
			{
			  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
			  goto error;
			}
		    }
		  (void) qexec_remove_my_tran_id_in_xasl_entry (thread_p,
								ent, true);
		}

	      /* structure copy */
	      XASL_ID_COPY (&query_exec_info[i].xasl_id, &tdes->xasl_id);
	    }
	  else
	    {
	      XASL_ID_SET_NULL (&query_exec_info[i].xasl_id);
	    }

	  size += 2 * OR_FLOAT_SIZE	/* query time + tran time */
	    + or_packed_string_length (query_exec_info[i].
				       wait_for_tran_index_string, NULL)
	    + or_packed_string_length (query_exec_info[i].query_stmt, NULL)
	    + or_packed_string_length (query_exec_info[i].sql_id, NULL)
	    + OR_XASL_ID_SIZE;
	}
#endif
      num_clients++;
    }

  /* Now allocate the area and pack the information */
  buffer = (char *) malloc (size);
  if (buffer == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, size);
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error;
    }

  ptr = buffer;
  ptr = or_pack_int (ptr, num_clients);

  /* Find size of needed buffer */
  for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
    {
      tdes = LOG_FIND_TDES (i);
      if (!LOG_IS_CLIENT_ACTIVE (tdes))
	{
	  /* The index is not assigned or is system transaction (no-client) */
	  continue;
	}

      ptr = or_pack_int (ptr, tdes->tran_index);
      ptr = or_pack_int (ptr, tdes->state);
      ptr = or_pack_int (ptr, tdes->client.process_id);
      ptr = or_pack_string (ptr, tdes->client.db_user);
      ptr = or_pack_string (ptr, tdes->client.program_name);
      ptr = or_pack_string (ptr, tdes->client.login_name);
      ptr = or_pack_string (ptr, tdes->client.host_name);

#if defined(SERVER_MODE)
      if (include_query_exec_info)
	{
	  ptr = or_pack_float (ptr, query_exec_info[i].query_time);
	  ptr = or_pack_float (ptr, query_exec_info[i].tran_time);
	  ptr =
	    or_pack_string (ptr,
			    query_exec_info[i].wait_for_tran_index_string);
	  ptr = or_pack_string (ptr, query_exec_info[i].query_stmt);
	  ptr = or_pack_string (ptr, query_exec_info[i].sql_id);
	  OR_PACK_XASL_ID (ptr, &query_exec_info[i].xasl_id);
	}
#endif
    }

  *buffer_p = buffer;
  *size_p = size;

error:

#if defined(SERVER_MODE)
  if (query_exec_info != NULL)
    {
      for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
	{
	  if (query_exec_info[i].wait_for_tran_index_string)
	    {
	      free_and_init (query_exec_info[i].wait_for_tran_index_string);
	    }
	  if (query_exec_info[i].query_stmt)
	    {
	      free_and_init (query_exec_info[i].query_stmt);
	    }
	  if (query_exec_info[i].sql_id)
	    {
	      free_and_init (query_exec_info[i].sql_id);
	    }
	}
      free_and_init (query_exec_info);
    }
#endif

  TR_TABLE_UNLOCK (thread_p);
  return error_code;
}

/*
 * logtb_find_current_client_type - find client type of current transaction
 *
 * return: client type
 */
int
logtb_find_current_client_type (THREAD_ENTRY * thread_p)
{
  return logtb_find_client_type (logtb_get_current_tran_index (thread_p));
}

/*
 * logtb_find_current_client_name - find client name of current transaction
 *
 * return: client name
 */
char *
logtb_find_current_client_name (THREAD_ENTRY * thread_p)
{
  return logtb_find_client_name (logtb_get_current_tran_index (thread_p));
}

/*
 * logtb_find_current_client_hostname - find client hostname of current transaction
 *
 * return: client hostname
 */
char *
logtb_find_current_client_hostname (THREAD_ENTRY * thread_p)
{
  return logtb_find_client_hostname (logtb_get_current_tran_index (thread_p));
}

#if defined(RYE_DEBUG)
/*
 * logtb_find_current_tran_lsa - find current transaction log sequence address
 *
 * return:  LOG_LSA *
 */
LOG_LSA *
logtb_find_current_tran_lsa (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;		/* Transaction descriptor */

  tdes = logtb_get_current_tdes (thread_p);
  return ((tdes != NULL) ? &tdes->last_lsa : NULL);
}
#endif /* RYE_DEBUG */

/*
 * logtb_find_state - find the state of the transaction
 *
 * return: TRAN_STATE
 *
 *   tran_index(in): transaction index
 */
TRAN_STATE
logtb_find_state (int tran_index)
{
  LOG_TDES *tdes;		/* Transaction descriptor */

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL)
    {
      return tdes->state;
    }
  else
    {
      return TRAN_UNACTIVE_UNKNOWN;
    }
}

/*
 * xlogtb_reset_wait_msecs - reset future waiting times
 *
 * return: The old wait_msecs.
 *
 *   wait_msecs(in): Wait for at least this number of milliseconds to acquire a lock
 *               before the transaction is timed out.
 *               A negative value (e.g., -1) means wait forever until a lock
 *               is granted or transaction is selected as a victim of a
 *               deadlock.
 *               A value of zero means do not wait at all, timeout immediately
 *               (in milliseconds)
 *
 * Note:Reset the default waiting time for the current transaction index(client).
 */
int
xlogtb_reset_wait_msecs (THREAD_ENTRY * thread_p, int wait_msecs)
{
  LOG_TDES *tdes;		/* Transaction descriptor */
  int old_wait_msecs;		/* The old waiting time to be returned */

  tdes = logtb_get_current_tdes (thread_p);
  if (tdes == NULL)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_UNKNOWN_TRANINDEX, 1,
	      logtb_get_current_tran_index (thread_p));
      return -1;
    }

  old_wait_msecs = tdes->wait_msecs;
  tdes->wait_msecs = wait_msecs;

  return old_wait_msecs;
}

/*
 * logtb_find_wait_msecs -  find waiting times for given transaction
 *
 * return: wait_msecs...
 *
 *   tran_index(in): Index of transaction
 */
int
logtb_find_wait_msecs (int tran_index)
{
  LOG_TDES *tdes;		/* Transaction descriptor */

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL)
    {
      return tdes->wait_msecs;
    }
  else
    {
      assert (false);
      return 0;
    }
}

/*
 * logtb_find_current_wait_msecs - find waiting times for current transaction
 *
 * return : wait_msecs...
 *
 * Note: Find the waiting time for the current transaction.
 */
int
logtb_find_current_wait_msecs (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;		/* Transaction descriptor */
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL)
    {
      return tdes->wait_msecs;
    }
  else
    {
      return 0;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * xlogtb_reset_isolation - reset consistency of transaction
 *
 * return: error code.
 *
 *   isolation(in): New Isolation level. One of the following:
 *                         TRAN_SERIALIZABLE
 *                         TRAN_REP_CLASS_REP_INSTANCE
 *                         TRAN_REP_CLASS_COMMIT_INSTANCE
 *                         TRAN_DEFAULT_ISOLATION
 *                         TRAN_COMMIT_CLASS_COMMIT_INSTANCE
 *                         TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE
 *
 * Note:Reset the default isolation level for the current transaction
 *              index (client).
 *
 * Note/Warning: This function must be called when the current transaction has
 *               not been done any work (i.e, just after restart, commit, or
 *               abort), otherwise, its isolation behaviour will be undefined.
 */
int
xlogtb_reset_isolation (THREAD_ENTRY * thread_p, TRAN_ISOLATION isolation)
{
  int error_code = NO_ERROR;
  LOG_TDES *tdes;		/* Transaction descriptor */
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (isolation == TRAN_DEFAULT_ISOLATION && tdes != NULL)
    {
      assert (error_code == NO_ERROR);	/* OK - nop */
    }
  else
    {
      error_code = ER_LOG_INVALID_ISOLATION_LEVEL;
      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE,
	      error_code, 2, TRAN_DEFAULT_ISOLATION, TRAN_DEFAULT_ISOLATION);
    }

  return error_code;
}
#endif

/*
 * logtb_find_isolation - find the isolation level for given trans
 *
 * return: isolation
 *
 *   tran_index(in):Index of transaction
 */
TRAN_ISOLATION
logtb_find_isolation (UNUSED_ARG int tran_index)
{
#if 1
  return TRAN_DEFAULT_ISOLATION;	/* == TRAN_REP_CLASS_UNCOMMIT_INSTANCE */
#else
  LOG_TDES *tdes;		/* Transaction descriptor */

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL)
    {
      return tdes->isolation;
    }
  else
    {
      return TRAN_UNKNOWN_ISOLATION;
    }
#endif
}

/*
 * logtb_find_current_isolation - find the isolation level for current
 *                             transaction
 *
 * return: isolation...
 *
 * Note: Find the isolation level for the current transaction
 */
TRAN_ISOLATION
logtb_find_current_isolation (THREAD_ENTRY * thread_p)
{
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);

  return logtb_find_isolation (tran_index);
}

/*
 * xlogtb_set_interrupt - indicate interrupt to a future caller of current
 *                     transaction
 *
 * return: nothing
 *
 *   set(in): true for set and false for clear
 *
 * Note: Set the interrupt falg for the current execution, so that the
 *              next caller obtains an interrupt.
 */
void
xlogtb_set_interrupt (THREAD_ENTRY * thread_p, int set)
{
  logtb_set_tran_index_interrupt (thread_p,
				  logtb_get_current_tran_index (thread_p),
				  set);
}

/*
 * logtb_set_tran_index_interrupt - indicate interrupt to a future caller for an
 *                               specific transaction index
 *
 * return: false is returned when the tran_index is not associated
 *              with a transaction
 *
 *   tran_index(in): Transaction index
 *   set(in): true for set and false for clear
 *
 * Note:Set the interrupt flag for the execution of the given transaction,
 *              so that the next caller obtains an interrupt.
 */
bool
logtb_set_tran_index_interrupt (THREAD_ENTRY * thread_p, int tran_index,
				int set)
{
  LOG_TDES *tdes;		/* Transaction descriptor */

  if (log_Gl.trantable.area != NULL)
    {
      tdes = LOG_FIND_TDES (tran_index);
      if (tdes != NULL && tdes->trid != NULL_TRANID)
	{
	  if (tdes->interrupt != set)
	    {
	      TR_TABLE_LOCK (thread_p);

	      tdes->interrupt = set;
	      if (set == true)
		{
		  log_Gl.trantable.num_interrupts++;
		}
	      else
		{
		  log_Gl.trantable.num_interrupts--;
		}

	      TR_TABLE_UNLOCK (thread_p);
	    }

	  if (set == true)
	    {
	      pgbuf_force_to_check_for_interrupts ();
	      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
		      ER_INTERRUPTING, 1, tran_index);
	      mnt_stats_counter (thread_p, MNT_STATS_TRAN_INTERRUPTS, 1);
	    }

	  return true;
	}
    }

  return false;
}

/*
 * logtb_is_interrupted_tdes -
 *
 * return:
 *
 *   tdes(in/out):
 *   clear(in/out):
 *   continue_checking(out):
 *
 * Note:
 */
static bool
logtb_is_interrupted_tdes (UNUSED_ARG THREAD_ENTRY * thread_p,
			   LOG_TDES * tdes, bool clear,
			   bool * continue_checking)
{
  int interrupt;
  INT64 now;
#if !defined(SERVER_MODE)
  struct timeval tv;
#endif /* !SERVER_MODE */

  interrupt = tdes->interrupt;
  if (!LOG_ISTRAN_ACTIVE (tdes))
    {
      interrupt = false;

      if (log_Gl.trantable.num_interrupts > 0)
	{
	  *continue_checking = true;
	}
      else
	{
	  *continue_checking = false;
	}
    }
  else if (interrupt == true && clear == true)
    {
      tdes->interrupt = false;

      TR_TABLE_LOCK (thread_p);
      log_Gl.trantable.num_interrupts--;
      if (log_Gl.trantable.num_interrupts > 0)
	{
	  *continue_checking = true;
	}
      else
	{
	  *continue_checking = false;
	}
      TR_TABLE_UNLOCK (thread_p);
    }
  else if (interrupt == false && tdes->query_timeout > 0)
    {
      /* In order to prevent performance degradation, we use log_Clock_msec
       * set by thread_log_clock_thread instead of calling gettimeofday here
       * if the system supports atomic built-ins.
       */
#if defined(SERVER_MODE)
      now = thread_get_log_clock_msec ();
#else /* SERVER_MODE */
      gettimeofday (&tv, NULL);
      now = (tv.tv_sec * 1000LL) + (tv.tv_usec / 1000LL);
#endif /* !SERVER_MODE */
      if (tdes->query_timeout < now)
	{
	  er_log_debug (ARG_FILE_LINE,
			"logtb_is_interrupted_tdes: timeout %lld milliseconds "
			"delayed (expected=%lld, now=%lld)",
			now - tdes->query_timeout, tdes->query_timeout, now);
	  interrupt = true;
	}
    }
  return interrupt;
}

/*
 * logtb_is_interrupted - find if execution must be stopped due to
 *			  an interrupt (^C)
 *
 * return:
 *
 *   clear(in): true if the interrupt should be cleared.
 *   continue_checking(in): Set as a side effect to true if there are more
 *                        interrupts to check or to false if there are not
 *                        more interrupts.
 *
 * Note: Find if the current execution must be stopped due to an
 *              interrupt (^C). If clear is true, the interruption flag is
 *              cleared; This is the expected case, once someone is notified,
 *              we do not have to keep the flag on.
 *
 *       If the transaction is not active, false is returned. For
 *              example, in the middle of an undo action, the transaction will
 *              not be interrupted. The recovery manager will interrupt the
 *              transaction at the end of the undo action...int this case the
 *              transaction will be partially aborted.
 */
bool
logtb_is_interrupted (THREAD_ENTRY * thread_p, bool clear,
		      bool * continue_checking)
{
  LOG_TDES *tdes;		/* Transaction descriptor */
  int tran_index;

  if (log_Gl.trantable.area == NULL)
    {
      return false;
    }
  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes == NULL)
    {
      return false;
    }

  return logtb_is_interrupted_tdes (thread_p, tdes, clear, continue_checking);
}

/*
 * logtb_is_interrupted_tran - find if the execution of the given transaction
 *			       must be stopped due to an interrupt (^C)
 *
 * return:
 *
 *   clear(in): true if the interrupt should be cleared.
 *   continue_checking(in): Set as a side effect to true if there are more
 *                        interrupts to check or to false if there are not
 *                        more interrupts.
 *   tran_index(in):
 *
 * Note: Find if the execution o fthe given transaction must be stopped
 *              due to an interrupt (^C). If clear is true, the
 *              interruption flag is cleared; This is the expected case, once
 *              someone is notified, we do not have to keep the flag on.
 *       This function is called to see if a transaction that is
 *              waiting (e.g., suspended wiating on a lock) on an event must
 *              be interrupted.
 */
bool
logtb_is_interrupted_tran (THREAD_ENTRY * thread_p, bool clear,
			   bool * continue_checking, int tran_index)
{
  LOG_TDES *tdes;		/* Transaction descriptor */

  tdes = LOG_FIND_TDES (tran_index);
  if (log_Gl.trantable.area == NULL || tdes == NULL)
    {
      return false;
    }

  return logtb_is_interrupted_tdes (thread_p, tdes, clear, continue_checking);
}

/*
 * xlogtb_set_suppress_repl_on_transaction - set or unset suppress_replication flag
 *                                           on transaction descriptor
 *
 * return: nothing
 *
 *   set(in): non-zero to set, zero to unset
 */
void
xlogtb_set_suppress_repl_on_transaction (THREAD_ENTRY * thread_p, int set)
{
  logtb_set_suppress_repl_on_transaction (thread_p,
					  logtb_get_current_tran_index
					  (thread_p), set);
}

/*
 * logtb_set_suppress_repl_on_transaction - set or unset suppress_replication flag
 *                                          on transaction descriptor
 *
 * return: false is returned when the tran_index is not associated
 *              with a transaction
 *
 *   tran_index(in): Transaction index
 *   set(in): non-zero to set, zero to unset
 */
bool
logtb_set_suppress_repl_on_transaction (UNUSED_ARG THREAD_ENTRY * thread_p,
					int tran_index, int set)
{
  LOG_TDES *tdes;		/* Transaction descriptor */

  if (log_Gl.trantable.area != NULL)
    {
      tdes = LOG_FIND_TDES (tran_index);
      if (tdes != NULL && tdes->trid != NULL_TRANID)
	{
	  if (tdes->suppress_replication != set)
	    {
	      TR_TABLE_LOCK (thread_p);

	      tdes->suppress_replication = set;

	      TR_TABLE_UNLOCK (thread_p);
	    }
	  return true;
	}
    }
  return false;
}


/*
 * logtb_is_active - is transaction active ?
 *
 * return:
 *
 *   trid(in): Transaction identifier
 *
 * Note: Find if given transaction is an active one. This function
 *              execute a sequential search in the transaction table to find
 *              out the given transaction. The function bypasses the search if
 *              the given trid is current transaction.
 *
 * Note:        The assumption of this function is that the transaction table
 *              is not very big and that most of the time the search is
 *              avoided. if this assumption becomes false, we may need to
 *              define a hash table from trid to tdes to speed up the search.
 */
bool
logtb_is_active (THREAD_ENTRY * thread_p, TRANID trid)
{
  int i;
  LOG_TDES *tdes;		/* Transaction descriptor */
  bool active = false;
  int tran_index;

  if (!LOG_ISRESTARTED ())
    {
      return false;
    }

  /* Avoid searching as much as possible */
  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL && tdes->trid == trid)
    {
      active = LOG_ISTRAN_ACTIVE (tdes);
    }
  else
    {
      TR_TABLE_LOCK (thread_p);
      /* Search the transaction table for such transaction */
      for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
	{
	  tdes = LOG_FIND_TDES (i);
	  if (tdes != NULL && tdes->trid != NULL_TRANID && tdes->trid == trid)
	    {
	      active = LOG_ISTRAN_ACTIVE (tdes);
	      break;
	    }
	}
      TR_TABLE_UNLOCK (thread_p);
    }

  return active;
}

/*
 * logtb_is_current_active - is current transaction active ?
 *
 * return:
 *
 * Note: Find if the current transaction is an active one.
 */
bool
logtb_is_current_active (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;		/* Transaction descriptor */
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);

  if (tdes != NULL && LOG_ISTRAN_ACTIVE (tdes))
    {
      return true;
    }
  else
    {
      return false;
    }
}

/*
 * logtb_is_current_system_tran - is current system transaction ?
 *
 * return:
 */
bool
logtb_is_current_system_tran (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;		/* Transaction descriptor */
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  if (tran_index == NULL_TRAN_INDEX)
    {
      assert (false);
      return false;
    }

  if (tran_index == LOG_SYSTEM_TRAN_INDEX)
    {
      return true;
    }

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL && tdes->trid == LOG_SYSTEM_TRANID)
    {
      return true;
    }
  else
    {
      return false;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * logtb_istran_finished - is transaction finished?
 *
 * return:
 *
 *   trid(in): Transaction identifier
 *
 * Note: Find if given transaction exists. That is, find if the
 *              transaction has completely finished its execution.
 *              Note that this function differs from log_istran_active in
 *              that a transaction in commit or abort state is not active but
 *              it is still alive (i.e., has not done completely).
 *              This function execute a sequential search in the transaction
 *              table to find out the given transaction. The function bypasses
 *              the search if the given trid is current transaction.
 *
 *       The assumption of this function is that the transaction table
 *              is not very big and that most of the time the search is
 *              avoided. if this assumption becomes false, we may need to
 *              define a hash table from trid to tdes to speed up the search.
 */
bool
logtb_istran_finished (THREAD_ENTRY * thread_p, TRANID trid)
{
  int i;
  LOG_TDES *tdes;		/* Transaction descriptor */
  bool active = true;
  int tran_index;

  /* Avoid searching as much as possible */
  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL && tdes->trid == trid)
    {
      active = false;
    }
  else
    {
      TR_TABLE_LOCK (thread_p);
      /* Search the transaction table for such transaction */
      for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
	{
	  tdes = LOG_FIND_TDES (i);
	  if (tdes != NULL && tdes->trid != NULL_TRANID && tdes->trid == trid)
	    {
	      active = false;
	      break;
	    }
	}
      TR_TABLE_UNLOCK (thread_p);
    }

  return active;
}
#endif

/*
 * logtb_has_updated - has transaction updated the database ?
 *
 * return:
 *
 */
bool
logtb_has_updated (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;		/* Transaction descriptor */
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL && !LSA_ISNULL (&tdes->last_lsa))
    {
      return true;
    }
  else
    {
      return false;
    }
}

/*
 * logtb_disable_update -
 *   return: none
 */
void
logtb_disable_update (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  db_Disable_modifications = 1;
  er_log_debug (ARG_FILE_LINE,
		"logtb_disable_update: db_Disable_modifications = %d\n",
		db_Disable_modifications);
}

/*
 * logtb_enable_update -
 *   return: none
 */
void
logtb_enable_update (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  db_Disable_modifications = 0;
  er_log_debug (ARG_FILE_LINE,
		"logtb_enable_update: db_Disable_modifications = %d\n",
		db_Disable_modifications);
}

/*
 * logtb_set_to_system_tran_index - set to tran index system
 *
 * return: nothing
 *
 * Note: The current log_tran_index is set to the system transaction index.
 */
void
logtb_set_to_system_tran_index (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;

  logtb_set_current_tran_index (thread_p, LOG_SYSTEM_TRAN_INDEX);

  tdes = LOG_FIND_TDES (LOG_SYSTEM_TRAN_INDEX);
  if (tdes != NULL)
    {
      assert (tdes->trid != NULL_TRANID);
      tdes->state = TRAN_ACTIVE;
    }
}

/*
 * logtb_set_current_tran_index -
 *
 * return: nothing
 *
 * Note:
 */
void
logtb_set_current_tran_index (UNUSED_ARG THREAD_ENTRY * thread_p,
			      int tran_index)
{
#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  assert (thread_p != NULL);

  thread_p->tran_index = tran_index;
#else
  log_Tran_index = tran_index;
#endif
}

/*
 * logtb_get_current_tran_index -
 *
 * return: tran_index
 *
 * Note: get transaction index if current thread
 */
int
logtb_get_current_tran_index (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  if (thread_p == NULL)
    {
      assert (false);
      return NULL_TRAN_INDEX;
    }

  return thread_p->tran_index;
#else
  return log_Tran_index;
#endif
}

/*
 * logtb_find_current_tdes -
 *
 * return: tran_index
 *
 * Note: get transaction index if current thread
 */
LOG_TDES *
logtb_get_current_tdes (THREAD_ENTRY * thread_p)
{
  return LOG_FIND_TDES (logtb_get_current_tran_index (thread_p));
}

/*
 * logtb_find_unilaterally_largest_undo_lsa - find maximum lsa address to undo
 *
 * return:
 *
 * Note: Find the maximum log sequence address to undo during the undo
 *              crash recovery phase.
 */
LOG_LSA *
logtb_find_unilaterally_largest_undo_lsa (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  int i;
  LOG_TDES *tdes;		/* Transaction descriptor */
  LOG_LSA *max = NULL;		/* The maximum LSA value  */

  TR_TABLE_LOCK (thread_p);
  for (i = 0; i < NUM_TOTAL_TRAN_INDICES; i++)
    {
      if (i != LOG_SYSTEM_TRAN_INDEX)
	{
	  tdes = LOG_FIND_TDES (i);
	  if (tdes != NULL
	      && tdes->trid != NULL_TRANID
	      && (tdes->state == TRAN_UNACTIVE_UNILATERALLY_ABORTED
		  || tdes->state == TRAN_UNACTIVE_ABORTED)
	      && !LSA_ISNULL (&tdes->undo_nxlsa)
	      && (max == NULL || LSA_LT (max, &tdes->undo_nxlsa)))
	    {
	      max = &tdes->undo_nxlsa;
	    }
	}
    }
  TR_TABLE_UNLOCK (thread_p);

  return max;
}

/*
 * logtb_find_smallest_begin_lsa_without_ddl_tran -
 *
 * return: NO_ERROR
 *
 *   min_lsa(out):
 *   exists_ddl_tran(out):
 */
int
logtb_find_smallest_begin_lsa_without_ddl_tran (UNUSED_ARG THREAD_ENTRY *
						thread_p, LOG_LSA * min_lsa,
						bool * exists_ddl_tran)
{
  LOG_TDES *tdes;
  WORKING_TRAN *current;

  assert (exists_ddl_tran != NULL);
  assert (min_lsa != NULL);

  *exists_ddl_tran = false;
  LSA_SET_NULL (min_lsa);
  for (current = log_Gl.trantable.working_tran_list_head;
       current != NULL; current = current->next)
    {
      if (current->tran_index == LOG_SYSTEM_TRAN_INDEX)
	{
	  continue;
	}

      tdes = LOG_FIND_TDES (current->tran_index);
#if defined(SERVER_MODE)
      assert (tdes != NULL);
      assert (tdes->trid != NULL_TRANID);
      assert (!LSA_ISNULL (&tdes->begin_lsa));
#endif

      if (tdes != NULL
	  && tdes->trid != NULL_TRANID && !LSA_ISNULL (&tdes->begin_lsa))
	{
	  if (tdes->type == TRAN_TYPE_DDL)
	    {
	      *exists_ddl_tran = true;
	    }
	  else
	    {
	      if (LSA_ISNULL (min_lsa))
		{
		  LSA_COPY (min_lsa, &tdes->begin_lsa);
		}
	    }
	}

      if (*exists_ddl_tran == true && !LSA_ISNULL (min_lsa))
	{
	  break;
	}
    }

  return NO_ERROR;
}

#if defined (SERVER_MODE)
int
logtb_lock_tran_table (THREAD_ENTRY * thread_p)
{
  int r;

  assert (tb_Mutex.lock_cnt >= 0);

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  assert (thread_p != NULL);

  if (tb_Mutex.holder != NULL && tb_Mutex.holder == thread_p)
    {
      /* assert (false); *//* to trace nesting */
      assert (tb_Mutex.lock_cnt > 0);
      tb_Mutex.lock_cnt++;
      return NO_ERROR;
    }

  r = pthread_mutex_lock (&tb_Mutex.lock);
  if (r != 0)
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_MUTEX_LOCK, 0);
      return ER_FAILED;
    }

  assert (thread_p != NULL);
  assert (tb_Mutex.holder == NULL);
  assert (tb_Mutex.lock_cnt == 0);

  tb_Mutex.holder = thread_p;
  tb_Mutex.lock_cnt = 1;

  return NO_ERROR;
}

int
logtb_unlock_tran_table (THREAD_ENTRY * thread_p)
{
  int r;

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  assert (thread_p != NULL);
  assert (tb_Mutex.holder != NULL);
  assert (tb_Mutex.holder == thread_p);
  assert (tb_Mutex.lock_cnt > 0);

  tb_Mutex.lock_cnt--;

  if (tb_Mutex.lock_cnt > 0)
    {
      /* assert (false); *//* to trace nesting */
      return NO_ERROR;
    }

  assert (tb_Mutex.holder != NULL);
  assert (tb_Mutex.holder == thread_p);
  assert (tb_Mutex.lock_cnt == 0);

  tb_Mutex.holder = NULL;
  tb_Mutex.lock_cnt = 0;

  r = pthread_mutex_unlock (&tb_Mutex.lock);
  if (r != 0)
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_MUTEX_UNLOCK, 0);
      return ER_FAILED;
    }

  return NO_ERROR;
}
#endif /* SERVER_MODE */

/*
 * UNDER CONSTRUCTION
 */
bool
logtb_shard_group_check_own (UNUSED_ARG THREAD_ENTRY * thread_p, int gid)
{
#if defined (SERVER_MODE)

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }
  assert (thread_p != NULL);

  /* check iff is invalid groupid
   */
  if (gid == NULL_GROUPID || gid < GLOBAL_GROUPID)
    {
      assert (false);
      return false;
    }

  /* check iff is non-shard table
   */
  if (gid == GLOBAL_GROUPID)
    {
      return true;		/* OK */
    }

  /* check iff client-type need to skip out group id filtering
   */
  if (thread_get_check_groupid (thread_p) == false)
    {
      return true;
    }

  return svr_shm_is_group_own (gid);
#else
  /* check iff is invalid groupid
   */
  if (gid == NULL_GROUPID || gid < GLOBAL_GROUPID)
    {
      assert (false);
      return false;
    }

  return true;
#endif
}

#if defined (SERVER_MODE)
bool
xlogtb_exist_working_tran (UNUSED_ARG THREAD_ENTRY * thread_p, int group_id)
{
  LOG_TDES *tdes;
  WORKING_TRAN *current;

  assert (group_id != GLOBAL_GROUPID);

  for (current = log_Gl.trantable.working_tran_list_head;
       current != NULL; current = current->next)
    {
      if (current->tran_index == LOG_SYSTEM_TRAN_INDEX)
	{
	  continue;
	}

      tdes = LOG_FIND_TDES (current->tran_index);
      assert (tdes != NULL);
      assert (tdes->trid != NULL_TRANID);
      assert (!LSA_ISNULL (&tdes->begin_lsa));

      if (tdes != NULL
	  && tdes->trid != NULL_TRANID
	  && !LSA_ISNULL (&tdes->begin_lsa)
	  && (NULL_GROUPID == group_id || tdes->tran_group_id == group_id))
	{
	  return true;
	}
    }

  return false;
}
#endif

int
xlogtb_update_group_id (UNUSED_ARG THREAD_ENTRY * thread_p,
			UNUSED_ARG int migrator_id, UNUSED_ARG int group_id,
			UNUSED_ARG int target, UNUSED_ARG int on_off)
{
#if defined (SERVER_MODE)
  bool continue_check;
  int error = NO_ERROR;

  if (on_off == true)
    {
      error = svr_shm_enable_group_bit (group_id);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }
  else
    {
      while (true)
	{
	  TR_TABLE_LOCK (thread_p);

	  if (group_id == GLOBAL_GROUPID
	      /* migrator already locks the global shard lock */
	      || xlogtb_exist_working_tran (thread_p, group_id) == false)
	    {
	      error = svr_shm_clear_group_bit (group_id);
	      TR_TABLE_UNLOCK (thread_p);

	      if (error != NO_ERROR)
		{
		  return error;
		}
	      break;
	    }

	  TR_TABLE_UNLOCK (thread_p);
	  thread_sleep (10);

	  /* interrupt check */
	  if (thread_get_check_interrupt (thread_p) == true)
	    {
	      if (logtb_is_interrupted (thread_p, true, &continue_check) ==
		  true)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INTERRUPTED,
			  0);
		  return ER_FAILED;
		}
	    }
	}
    }

  log_append_gid_bitmap_update_record (thread_p, migrator_id, group_id,
				       target, on_off);
#endif

  return NO_ERROR;
}

int
xlogtb_block_global_dml (UNUSED_ARG THREAD_ENTRY * thread_p, int start_or_end)
{
  block_Global_dml = start_or_end;
  return NO_ERROR;
}

int
logtb_is_blocked_global_dml (void)
{
  return block_Global_dml;
}

void
logtb_set_backup_session (THREAD_ENTRY * thread_p,
			  BK_BACKUP_SESSION * session)
{
  LOG_TDES *tdes;
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL)
    {
      tdes->bk_session = session;
    }
}

BK_BACKUP_SESSION *
logtb_get_backup_session (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL)
    {
      return tdes->bk_session;
    }

  return NULL;
}

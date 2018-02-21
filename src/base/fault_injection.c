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
 * fault_injection.c :
 *
 */

#ident "$Id$"

#include <assert.h>

#include "porting.h"
#include "fault_injection.h"

#include "ds_string_array.h"

#include "system_parameter.h"
#include "log_impl.h"

#if !defined(NDEBUG)

static int fi_handler_random_exit (THREAD_ENTRY * thread_p, void *arg);
static int fi_handler_random_fail (THREAD_ENTRY * thread_p, void *arg);
static int fi_handler_random_sleep (THREAD_ENTRY * thread_p, void *arg);

static FI_TEST_ITEM *fi_code_item (THREAD_ENTRY * thread_p,
				   FI_TEST_CODE code);

/******************************************************************************
 *
 * FI test scenario array
 *
 * Register new scenario in here with new FI_TEST_CODE & handler function
 *
 *******************************************************************************/
static FI_TEST_ITEM fi_Test_items[FI_TEST_END];
static bool fi_hasalready_Initiated = false;


static void
fi_init_test_item (FI_TEST_CODE test_code, const char *name,
		   FI_HANDLER_FUNC func)
{
  FI_TEST_ITEM *fi_item;

  fi_item = &fi_Test_items[test_code];
  fi_item->name = name;
  fi_item->func = func;
  fi_item->state = FI_INIT_STATE;
}

/*
 * fi_init -
 *
 * return: NO_ERROR or ER_FAILED
 *
 */
void
fi_init (void)
{
  fi_init_test_item (FI_TEST_FILE_MANAGER_FILE_DESTROY_ERROR1,
		     "fi_file_destroy_error_1", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_OVF_FILE_UPDATE_ERROR1,
		     "fi_ovf_file_update_error_1", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_HEAP_CREATE_ERROR1,
		     "fi_heap_create_error_1", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_BTREE_MANAGER_RANDOM_EXIT,
		     "fi_btree_random_exit", fi_handler_random_exit);

  fi_init_test_item (FI_TEST_LOG_MANAGER_RANDOM_EXIT_AT_RUN_POSTPONE,
		     "fi_run_postpone_random_exit", fi_handler_random_exit);

  fi_init_test_item (FI_TEST_LOG_MANAGER_DOESNT_FIT_EXIT,
		     "fi_doesnt_fit_exit", fi_handler_random_exit);

  fi_init_test_item (FI_TEST_BTREE_MANAGER_SPLIT_ERROR1,
		     "fi_btree_split_error_1", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_BTREE_MANAGER_ROOT_SPLIT_ERROR1,
		     "fi_btree_root_split_error_1", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_BTREE_MANAGER_ADD_INDEX_ERROR1,
		     "fi_btree_add_index_error_1", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_BTREE_MANAGER_ADD_INDEX_ERROR2,
		     "fi_btree_add_index_error_2", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR1,
		     "fi_btree_alloc_page_error_1", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR2,
		     "fi_btree_alloc_page_error_2", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR3,
		     "fi_btree_alloc_page_error_3", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_BTREE_MANAGER_MERGE_ERROR1,
		     "fi_btree_merge_error_1", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_BTREE_MANAGER_DELETE_INDEX_ERROR1,
		     "fi_btree_delete_index_error_1", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_BTREE_MANAGER_DELETE_INDEX_ERROR2,
		     "fi_btree_delete_index_error_2", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_HB_SLOW_HEARTBEAT_MESSAGE,
		     "fi_hb_slow_heartbeat_message", fi_handler_random_sleep);

  fi_init_test_item (FI_TEST_HB_DISK_FAIL,
		     "fi_hb_disk_fail", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_HB_PING_HOST_FAIL,
		     "fi_hb_ping_host_fail", fi_handler_random_fail);

  fi_init_test_item (FI_TEST_REPL_RANDOM_EXIT,
		     "fi_repl_random_exit", fi_handler_random_exit);

  fi_init_test_item (FI_TEST_REPL_RANDOM_FAIL,
		     "fi_repl_random_fail", fi_handler_random_fail);


  fi_hasalready_Initiated = true;
}

/*
 * fi_thread_init -
 *
 * return: NO_ERROR or ER_FAILED
 *
 *   code(in):
 *   state(in):
 */
int
fi_thread_init (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  FI_TEST_ITEM *fi_test_array = NULL;
  unsigned int i;

  if (fi_hasalready_Initiated == false)
    {
      fi_init ();
    }

#if defined (SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }
  if (thread_p == NULL)
    {
      assert (thread_p != NULL);

      return ER_FAILED;
    }

  if (thread_p->fi_test_array == NULL)
    {
      thread_p->fi_test_array =
	(FI_TEST_ITEM *) malloc (sizeof (fi_Test_items));
      if (thread_p->fi_test_array == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (fi_Test_items));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      memcpy (thread_p->fi_test_array, fi_Test_items, sizeof (fi_Test_items));
    }

  fi_test_array = thread_p->fi_test_array;
#else
  fi_test_array = fi_Test_items;
#endif

  for (i = 0; i < DIM (fi_Test_items); i++)
    {
      fi_test_array[i].state = FI_INIT_STATE;
    }

  return NO_ERROR;
}

/*
 * fi_set -
 *
 * return: NO_ERROR or ER_FAILED
 *
 *   code(in):
 *   state(in):
 */
static FI_TEST_ITEM *
fi_code_item (UNUSED_ARG THREAD_ENTRY * thread_p, FI_TEST_CODE code)
{
  FI_TEST_ITEM *fi_test_array;
  FI_TEST_ITEM *item;

#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }
  if (thread_p == NULL)
    {
      assert (thread_p != NULL);

      return NULL;
    }
  if (thread_p->fi_test_array == NULL)
    {
      fi_thread_init (thread_p);
    }

  fi_test_array = thread_p->fi_test_array;
#else
  if (fi_hasalready_Initiated == false)
    {
      fi_init ();
    }

  fi_test_array = fi_Test_items;
#endif

  item = &fi_test_array[code];

  assert (item != NULL);
  return item;
}

/*
 * fi_set -
 *
 * return: NO_ERROR or ER_FAILED
 *
 *   code(in):
 *   state(in):
 */
int
fi_set (THREAD_ENTRY * thread_p, FI_TEST_CODE code, int state)
{
  FI_TEST_ITEM *item = NULL;

  if (fi_test_on (thread_p, code) == false)
    {
      return NO_ERROR;
    }

  item = fi_code_item (thread_p, code);
  if (item == NULL)
    {
      assert (item != NULL);
      return ER_FAILED;
    }

  if (item->state == state - 1)
    {
      item->state = state;
    }

  return NO_ERROR;
}

/*
 * fi_set_force -
 *
 * return: NO_ERROR or error code
 *
 *   code(in):
 *   state(in):
 */
int
fi_set_force (THREAD_ENTRY * thread_p, FI_TEST_CODE code, int state)
{
  FI_TEST_ITEM *item = NULL;

  if (fi_test_on (thread_p, code) == false)
    {
      return NO_ERROR;
    }

  item = fi_code_item (thread_p, code);
  if (item == NULL)
    {
      assert (item != NULL);
      return ER_FAILED;
    }

  item->state = state;

  return NO_ERROR;
}

/*
 * fi_reset -
 *
 * return:
 *
 *   code(in):
 */
void
fi_reset (THREAD_ENTRY * thread_p, FI_TEST_CODE code)
{
  FI_TEST_ITEM *item = NULL;

  item = fi_code_item (thread_p, code);
  item->state = FI_INIT_STATE;
}

/*
 * fi_test_arg_int -
 *
 * return: NO_ERROR or error code
 *
 *   code(in):
 *   arg(in):
 *   state(in):
 */
int
fi_test_arg_int (THREAD_ENTRY * thread_p, FI_TEST_CODE code, int arg,
		 int state)
{
  return fi_test (thread_p, code, &arg, state);
}

/*
 * fi_test -
 *
 * return: NO_ERROR or error code
 *
 *   code(in):
 *   arg(in):
 *   state(in):
 */
int
fi_test (THREAD_ENTRY * thread_p, FI_TEST_CODE code, void *arg, int state)
{
  FI_TEST_ITEM *item = NULL;

  if (fi_test_on (thread_p, code) == false)
    {
      return NO_ERROR;
    }

  item = fi_code_item (thread_p, code);
  if (item == NULL)
    {
      assert (item != NULL);
      return ER_FAILED;
    }

  if (item->state == state)
    {
      item->state = FI_INIT_STATE;
      return (*item->func) (thread_p, arg);
    }

  return NO_ERROR;
}

/* fi_test_array -
 *    return: error code
 *
 *    num_entries(in):
 *    entries(in):
 */
int
fi_test_array (THREAD_ENTRY * thread_p, int num_entries,
	       FI_TEST_ENTRY * entries)
{
  int error = NO_ERROR;
  int i;

  for (i = 0; i < num_entries; i++)
    {
      error = fi_test (thread_p, entries[i].test_code, NULL,
		       entries[i].state);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  return error;
}

/* fi_test_array_arg_int -
 *    return: error code
 *
 *    num_entries(in):
 *    entries(in):
 */
int
fi_test_array_arg_int (THREAD_ENTRY * thread_p, int num_entries,
		       FI_TEST_ENTRY_ARG_INT * entries)
{
  int error = NO_ERROR;
  int i;

  for (i = 0; i < num_entries; i++)
    {
      error = fi_test (thread_p, entries[i].test_code, &entries[i].arg,
		       entries[i].state);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  return error;
}

/*
 * fi_state -
 *
 * return:
 *
 *   code(in):
 */
int
fi_state (THREAD_ENTRY * thread_p, FI_TEST_CODE code)
{
  FI_TEST_ITEM *item = NULL;

  if (fi_test_on (thread_p, code) == false)
    {
      return FI_INIT_STATE;
    }

  item = fi_code_item (thread_p, code);
  assert (item != NULL);

  return item->state;
}

/*
 * fi_test_on -
 *
 * return: true or false
 *
 *   code(in):
 */
bool
fi_test_on (THREAD_ENTRY * thread_p, FI_TEST_CODE code)
{
  FI_TEST_ITEM *item = NULL;
  char *fi_name = NULL;
  char **fi_name_array = NULL;
  int found;

  item = fi_code_item (thread_p, code);
  if (item == NULL || item->name == NULL)
    {
      assert (false);
      return false;
    }

  fi_name = prm_get_string_value (PRM_ID_FAULT_INJECTION);
  if (fi_name == NULL)
    {
      return false;
    }

  fi_name_array = Rye_split_string (fi_name, ",:");
  if (fi_name_array == NULL)
    {
      return false;
    }

  found = Rye_str_array_find (fi_name_array, item->name);

  Rye_str_array_free (fi_name_array);
  fi_name_array = NULL;

  if (found != -1)
    {
      return true;
    }
  else
    {
      return false;
    }
}

static int
fi_handler_random_exit (UNUSED_ARG THREAD_ENTRY * thread_p, void *arg)
{
  static bool init = false;
  int r;
  int mod_factor;

  if (arg == NULL)
    {
      mod_factor = 20000;
    }
  else
    {
      mod_factor = *((int *) arg);
    }

  if (init == false)
    {
      srand (time (NULL));
      init = true;
    }
  r = rand ();

#if !defined(CS_MODE)
  if ((r % 10) == 0)
    {
      LOG_CS_ENTER (thread_p);
      logpb_flush_pages_direct (thread_p);
      LOG_CS_EXIT ();
    }
#endif
  if ((r % mod_factor) == 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_FAULT_INJECTION, 1, "fault injection: random exit");

      _exit (0);
    }

  return NO_ERROR;
}

static int
fi_handler_random_fail (UNUSED_ARG THREAD_ENTRY * thread_p, void *arg)
{
  static bool init = false;
  int r;
  int mod_factor;

  if (arg == NULL)
    {
      mod_factor = 20000;
    }
  else
    {
      mod_factor = *((int *) arg);
    }

  if (init == false)
    {
      srand (time (NULL));
      init = true;
    }
  r = rand ();

  if (mod_factor == 0 || ((r % mod_factor) == 0))
    {
      char err_msg[ER_MSG_SIZE];

      snprintf (err_msg, sizeof (err_msg),
		"fault injection: random fail(mod_factor(%d)", mod_factor);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_FAULT_INJECTION, 1, err_msg);

      return ER_FAULT_INJECTION;
    }

  return NO_ERROR;
}

static int
fi_handler_random_sleep (UNUSED_ARG THREAD_ENTRY * thread_p, void *arg)
{
  static bool init = false;
  int r;
  FI_ARG_SLEEP arg_sleep;

  if (arg == NULL)
    {
      arg_sleep.mod_factor = 20000;
      arg_sleep.sleep_time = 10 * ONE_SEC;
    }
  else
    {
      arg_sleep = *((FI_ARG_SLEEP *) arg);
    }

  if (init == false)
    {
      srand (time (NULL));
      init = true;
    }

  r = rand () % arg_sleep.mod_factor;
  if (r == 0)
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_FAULT_INJECTION, 1, "fault injection: random sleep");
      THREAD_SLEEP (arg_sleep.sleep_time);
    }

  return NO_ERROR;
}

#endif /* NDEBUG */

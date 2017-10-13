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
 * fault_injection.h :
 *
 */

#ifndef _FAULT_INJECTION_H_
#define _FAULT_INJECTION_H_

#ident "$Id$"

#include "error_manager.h"

#include "thread.h"

#if !defined(NDEBUG)
#define FI_INIT()                       fi_init()
#define FI_INIT_THREAD(th)              fi_thread_init(th)
#define FI_INSERTED(code) 		fi_test_on(code)
#define FI_SET(th, code, state)		fi_set(th, code, state)
#define FI_RESET(th, code)		fi_reset(th, code)
#define FI_TEST(th, code, state) 	fi_test(th, code, NULL, state)
#define FI_TEST_ARRAY(th, num_entries, entries) fi_test_array(th, num_entries, entries)
#define FI_TEST_ARG(th, code, arg, state) fi_test(th, code, arg, state)
#define FI_TEST_ARG_INT(th, code, arg, state) fi_test_arg_int(th, code, arg, state)
#define FI_TEST_ARRAY_ARG_INT(th, num_entries, entries) fi_test_array_arg_int(th, num_entries, entries)
#else
#define FI_INIT()
#define FI_INIT_THREAD(th)
#define FI_INSERTED(code) 0
#define FI_SET(th, code, state)
#define FI_RESET(th, code)
#define FI_TEST(th, code, state) (NO_ERROR)
#define FI_TEST_ARRAY(th, num_entries, entries) (NO_ERROR)
#define FI_TEST_ARG(th, code, arg, state) (NO_ERROR)
#define FI_TEST_ARG_INT(th, code, arg, state) (NO_ERROR)
#define FI_TEST_ARRAY_ARG_INT(th, num_entries, entries) (NO_ERROR)
#endif


typedef enum
{
  FI_TEST_NONE = 0,

  /* common */

  /* DISK MANAGER */

  /* FILE MANAGER */
  FI_TEST_FILE_MANAGER_FILE_DESTROY_ERROR1,

  /* OVERFLOW MANAGER */
  FI_TEST_OVF_FILE_UPDATE_ERROR1,

  /* HEAP MANAGER */
  FI_TEST_HEAP_CREATE_ERROR1,

  /* BTREE MANAGER */
  FI_TEST_BTREE_MANAGER_RANDOM_EXIT,

  FI_TEST_BTREE_MANAGER_SPLIT_ERROR1,
  FI_TEST_BTREE_MANAGER_ROOT_SPLIT_ERROR1,
  FI_TEST_BTREE_MANAGER_ADD_INDEX_ERROR1,
  FI_TEST_BTREE_MANAGER_ADD_INDEX_ERROR2,
  FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR1,
  FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR2,
  FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR3,

  FI_TEST_BTREE_MANAGER_MERGE_ERROR1,
  FI_TEST_BTREE_MANAGER_DELETE_INDEX_ERROR1,
  FI_TEST_BTREE_MANAGER_DELETE_INDEX_ERROR2,

  /* QUERY MANAGER */

  /* LOG MANAGER */
  FI_TEST_LOG_MANAGER_RANDOM_EXIT_AT_RUN_POSTPONE,

  /* HEARTBEAT */
  FI_TEST_HB_SLOW_HEARTBEAT_MESSAGE,
  FI_TEST_HB_SLOW_DISK,
  FI_TEST_HB_SLOW_PING_HOST,

  /* etc .... */

  FI_TEST_END
} FI_TEST_CODE;

#define FI_INIT_STATE 0


typedef int (*FI_HANDLER_FUNC) (THREAD_ENTRY * thread_p, void *arg);

typedef struct fi_test_item FI_TEST_ITEM;
struct fi_test_item
{
  const char *name;
  FI_HANDLER_FUNC func;
  int state;
};

typedef struct fi_test_entry FI_TEST_ENTRY;
struct fi_test_entry
{
  FI_TEST_CODE test_code;
  int state;
};

typedef struct fi_test_entry_arg_int FI_TEST_ENTRY_ARG_INT;
struct fi_test_entry_arg_int
{
  FI_TEST_CODE test_code;
  int arg;
  int state;
};

extern void fi_init (void);
extern int fi_thread_init (THREAD_ENTRY * thread_p);
extern int fi_set (THREAD_ENTRY * thread_p, FI_TEST_CODE code, int state);
extern int fi_set_force (THREAD_ENTRY * thread_p, FI_TEST_CODE code,
			 int state);
extern void fi_reset (THREAD_ENTRY * thread_p, FI_TEST_CODE code);
extern int fi_test (THREAD_ENTRY * thread_p, FI_TEST_CODE code, void *arg,
		    int state);
extern int fi_test_array (THREAD_ENTRY * thread_p, int num_entries,
			  FI_TEST_ENTRY * entries);
extern int fi_test_array_arg_int (THREAD_ENTRY * thread_p, int num_entries,
				  FI_TEST_ENTRY_ARG_INT * entries);
extern int fi_test_arg_int (THREAD_ENTRY * thread_p, FI_TEST_CODE code,
			    int arg, int state);
extern int fi_state (THREAD_ENTRY * thread_p, FI_TEST_CODE code);
extern bool fi_test_on (THREAD_ENTRY * thread_p, FI_TEST_CODE code);

#endif /* _FAULT_INJECTION_H_ */

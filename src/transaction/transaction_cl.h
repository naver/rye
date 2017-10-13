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
 * transaction_cl.h - transaction manager (at client)
 *
 */

#ifndef _TRANSACTION_CL_H_
#define _TRANSACTION_CL_H_

#ident "$Id$"

#include "config.h"

#include "error_manager.h"
#include "storage_common.h"
#include "log_comm.h"
#include "dbdef.h"


#define TM_TRAN_INDEX()      (tm_Tran_index)
#define TM_TRAN_WAIT_MSECS() (tm_Tran_wait_msecs)
#define TM_TRAN_ID()         (tm_Tran_ID)

typedef enum savepoint_type
{
  USER_SAVEPOINT = 1,
  SYSTEM_SAVEPOINT = 2
} SAVEPOINT_TYPE;

extern int tm_Tran_index;
extern TRAN_ISOLATION tm_Tran_isolation;
extern int tm_Tran_wait_msecs;
extern int tm_Tran_ID;

extern void tran_cache_tran_settings (int tran_index, int lock_timeout);
extern void tran_get_tran_settings (int *lock_timeout_in_msecs,
				    TRAN_ISOLATION * tran_isolation);
extern int tran_reset_wait_times (int wait_in_msecs);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int tran_reset_isolation (TRAN_ISOLATION isolation);
#endif
extern int tran_commit (void);
extern int tran_abort (void);
extern int tran_unilaterally_abort (void);
extern int tran_abort_only_client (bool is_server_down);
extern bool tran_has_updated (void);
extern bool tran_is_active_and_has_updated (void);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int tran_start_topop (void);
extern int tran_end_topop (LOG_RESULT_TOPOP result);
extern int tran_get_savepoints (DB_NAMELIST ** savepoint_list);
#endif
extern void tran_free_savepoint_list (void);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int tran_system_savepoint (const char *savept_name);
#endif
extern int tran_savepoint_internal (const char *savept_name,
				    SAVEPOINT_TYPE savepoint_type);
extern int tran_abort_upto_user_savepoint (const char *savepoint_name);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int tran_abort_upto_system_savepoint (const char *savepoint_name);
#endif
extern int tran_internal_abort_upto_savepoint (const char *savepoint_name,
					       SAVEPOINT_TYPE savepoint_type);
extern void tran_set_query_timeout (int query_timeout);
extern int tran_get_query_timeout (void);
#endif /* _TRANSACTION_CL_H_ */

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
 * repl_queue.h -
 */

#ifndef _REPL_QUEUE_H_
#define _REPL_QUEUE_H_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <pthread.h>

#include "repl.h"

#define SEQ_NUMBER 			(1)
#define MAGIC_NUMBER            	(MAJOR_VERSION * 1000000 + MINOR_VERSION * 10000 + SEQ_NUMBER)

#define DDL_APPLIER_INDEX                    0
#define GLOBAL_APPLIER_INDEX                 1

extern int cirp_analyzer_item_push (int la_index,
                                    TRANID trid, LOG_LSA * tran_start_lsa,
                                    LOG_LSA * committed_lsa, LOG_LSA * repl_start_lsa);
extern int cirp_applier_item_pop (CIRP_APPLIER_INFO * applier, CIRP_Q_ITEM ** item);
extern int cirp_applier_clear_committed_item (CIRP_APPLIER_INFO * applier);

extern bool cirp_analyzer_is_applier_busy (int la_index);

extern int cirp_pthread_cond_timedwait (pthread_cond_t * pcond, pthread_mutex_t * plock, int wakeup_interval);
extern int cirp_applier_wait_for_queue (CIRP_APPLIER_INFO * shm_applier);
extern int cirp_analyzer_wait_for_queue (int la_index);
extern int cirp_analyzer_wait_tran_commit (int la_index, LOG_LSA * lsa);

#endif /* _REPL_QUEUE_H_ */

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
 * ds_queue.h
 * Queues
 *
 */


#ifndef _DS_QUEUE_H_
#define _DS_QUEUE_H_

#include "ds_list.h"

typedef struct _rye_queue RQueue;
struct _rye_queue
{
  RSList list;
};

extern RQueue *Rye_queue_new (void);
#if defined(ENABLE_UNUSED_FUNCTION)
extern void Rye_queue_free (RQueue * queue);
#endif
extern void Rye_queue_free_full (RQueue * queue, Rye_func free_func);
extern void *Rye_queue_get_first (RQueue * queue);
extern RSNode *Rye_queue_enqueue (RQueue * queue, void *data);
extern void *Rye_queue_dequeue (RQueue * queue);

#endif /* _DS_QUEUE_H_ */

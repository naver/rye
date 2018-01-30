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
 * ds_queue.c
 * Double-ended Queues
 *
 */

#include <assert.h>

#include "ds_queue.h"

/*
 * Rye_queue_new -
 *   return: queue
 */
RQueue *
Rye_queue_new (void)
{
  RQueue *new_queue;

  new_queue = malloc (sizeof (RQueue));
  if (new_queue == NULL)
    {
      return NULL;
    }

  Rye_slist_clear (&new_queue->list);

  return new_queue;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * Rye_queue_free -
 *   return:
 *
 *   queue(in/out):
 */
void
Rye_queue_free (RQueue * queue)
{
  if (queue == NULL)
    {
      assert (false);
      return;
    }

  Rye_slist_free (&queue->list);
}
#endif

/*
 * Rye_queue_free_full -
 */
void
Rye_queue_free_full (RQueue * queue, Rye_func free_func)
{
  if (queue == NULL)
    {
      assert (false);
      return;
    }

  Rye_slist_free_full (&queue->list, free_func);
}

/*
 * Rye_queue_get_first -
 *   return: user data
 *
 *   queue(in/out):
 */
void *
Rye_queue_get_first (RQueue * queue)
{
  if (queue == NULL)
    {
      assert (false);
      return NULL;
    }

  return Rye_slist_get_head (&queue->list);
}

/*
 * Rye_queue_enqueue -
 *   return: new node
 *
 *   queue(in/out):
 *   data(in):
 */
RSNode *
Rye_queue_enqueue (RQueue * queue, void *data)
{
  RSNode *node;

  if (queue == NULL)
    {
      assert (false);
      return NULL;
    }

  /* Adds a new element on to the tail of the list. */
  node = Rye_slist_append (&queue->list, data);
  if (node == NULL)
    {
      return NULL;
    }

  return node;
}

/*
 * Rye_queue_dequeue -
 *   return: user data
 *
 *   queue(in/out):
 */
void *
Rye_queue_dequeue (RQueue * queue)
{
  if (queue == NULL)
    {
      assert (false);
      return NULL;
    }

  return Rye_slist_remove_head (&queue->list);
}

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
 * ds_list.c
 * singly linked lists
 *
 */

#include <assert.h>

#include "ds_list.h"

/*
 * Rye_slist_alloc -
 *   return: RSList node
 */
RSList *
Rye_slist_alloc (void)
{
  RSList *list;

  list = (RSList *) malloc (sizeof (RSList));
  if (list == NULL)
    {
      return NULL;
    }

  Rye_slist_clear (list);

  return list;
}

/*
 * Rye_slist_alloc -
 *   return: RSList node
 */
void
Rye_slist_clear (RSList * list)
{
  list->head = NULL;
  list->tail = NULL;

  list->count = 0;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * Rye_slist_free -
 *   return:
 *
 *   list(in/out):
 */
void
Rye_slist_free (RSList * list)
{
  RSNode *current, *next;

  current = list->head;
  while (current != NULL)
    {
      next = current->next;
      free (current);
      current = next;
    }

  Rye_slist_clear (list);
}
#endif

/*
 * Rye_slist_free_full -
 *   return:
 *
 *   list(in/out):
 *   free_func(in):
 */
void
Rye_slist_free_full (RSList * list, Rye_func free_func)
{
  RSNode *current, *next;

  current = list->head;
  while (current != NULL)
    {
      next = current->next;
      if (current->data != NULL && free_func != NULL)
	{
	  free_func (current->data, NULL);
	}
      free (current);
      current = next;
    }

  Rye_slist_clear (list);
}

/*
 * Rye_slist_get_head -
 *   return: user data
 *
 *   list(in):
 */
void *
Rye_slist_get_head (RSList * list)
{
  if (list == NULL)
    {
      assert (false);
      return NULL;
    }

  if (list->head == NULL)
    {
      /* empty */
      return NULL;
    }

  return list->head->data;
}

/*
 * Rye_slist_get_tail -
 *   return: user data
 *
 *   list(in):
 */
void *
Rye_slist_get_tail (RSList * list)
{
  if (list == NULL)
    {
      assert (false);
      return NULL;
    }

  if (list->tail == NULL)
    {
      /* empty */
      return NULL;
    }

  return list->tail->data;
}

/*
 * Rye_slist_prepend -
 *   return: new node
 *
 *   list(in/out):
 *   data(in):
 */
RSNode *
Rye_slist_prepend (RSList * list, void *data)
{
  RSNode *new_node;

  if (list == NULL)
    {
      assert (false);
      return NULL;
    }

  new_node = (RSNode *) malloc (sizeof (RSNode));
  if (new_node == NULL)
    {
      return NULL;
    }
  new_node->data = data;
  new_node->next = list->head;

  list->head = new_node;

  if (list->tail == NULL)
    {
      assert (list->count == 0);
      assert (new_node->next == NULL);

      list->tail = list->head;
    }

  list->count++;

  return new_node;
}

/*
 * Rye_slist_append -
 *   return: new node
 *
 *   list(in/out):
 *   data(in):
 */
RSNode *
Rye_slist_append (RSList * list, void *data)
{
  RSNode *new_node;

  if (list == NULL)
    {
      assert (false);
      return NULL;
    }

  new_node = (RSNode *) malloc (sizeof (RSNode));
  if (new_node == NULL)
    {
      return NULL;
    }
  new_node->data = data;
  new_node->next = NULL;

  if (list->head == NULL)
    {
      assert (list->tail == NULL);
      assert (list->count == 0);

      list->head = new_node;
      list->tail = new_node;
    }
  else
    {
      assert (list->tail != NULL);
      assert (list->count > 0);

      list->tail->next = new_node;
      list->tail = new_node;
    }

  list->count++;

  return new_node;
}

/*
 * Rye_slist_remove_head -
 *   return: user data
 *
 *   list(in/out):
 */
void *
Rye_slist_remove_head (RSList * list)
{
  RSNode *node = NULL;
  void *data = NULL;

  if (list == NULL)
    {
      assert (false);
      return NULL;
    }

  if (list->head == NULL)
    {
      /* empty */
      return NULL;
    }

  node = list->head;
  list->head = list->head->next;
  if (list->head == NULL)
    {
      assert (list->count == 1);

      list->tail = NULL;
    }

  list->count--;
  data = node->data;
  node->next = NULL;

  free (node);

  return data;
}

/*
 * Rye_slist_foreach -
 *   return: error code
 *
 *   list(in):
 *   func(in):
 *   user_data(in):
 */
int
Rye_slist_foreach (RSList * list, Rye_func func, void *user_data)
{
  RSNode *current;
  int ret;

  current = list->head;
  while (current != NULL)
    {
      ret = (*func) (current->data, user_data);
      if (ret < 0)
	{
	  return ret;
	}
      current = current->next;
    }

  return 0;
}

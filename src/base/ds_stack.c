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
 * ds_stack.c
 * stack
 *
 */

#include <assert.h>

#include "ds_stack.h"


/*
 * Rye_stack_new -
 *   return: stack
 */
RStack *
Rye_stack_new (void)
{
  RStack *new_stack;

  new_stack = malloc (sizeof (RStack));
  if (new_stack == NULL)
    {
      return NULL;
    }

  Rye_slist_clear (&new_stack->list);

  return new_stack;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * Rye_stack_free -
 *   return:
 *
 *   stack(in/out):
 */
void
Rye_stack_free (RStack * stack)
{
  if (stack == NULL)
    {
      assert (false);
      return;
    }

  Rye_slist_free (&stack->list);
}
#endif

/*
 * Rye_stack_free_full -
 */
void
Rye_stack_free_full (RStack * stack, Rye_func free_func)
{
  if (stack == NULL)
    {
      assert (false);
      return;
    }

  Rye_slist_free_full (&stack->list, free_func);
}

/*
 * Rye_stack_push -
 *   return: new node
 *
 *   stack(in/out):
 *   data(in):
 */
RSNode *
Rye_stack_push (RStack * stack, void *data)
{
  RSNode *node;

  if (stack == NULL)
    {
      assert (false);
      return NULL;
    }

  /* Adds a new element on to the head of the list. */
  node = Rye_slist_prepend (&stack->list, data);
  if (node == NULL)
    {
      return NULL;
    }

  return node;
}

/*
 * Rye_stack_destack -
 *   return: user data
 *
 *   stack(in/out):
 */
void *
Rye_stack_pop (RStack * stack)
{
  if (stack == NULL)
    {
      assert (false);
      return NULL;
    }

  return Rye_slist_remove_head (&stack->list);
}

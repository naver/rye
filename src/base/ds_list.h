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
 * ds_list.h
 * singly linked lists
 */

#ifndef DS_LIST_H_
#define DS_LIST_H_

#include "porting.h"

typedef int (*Rye_func) (void *, void *);

typedef struct _rye_single_node RSNode;
struct _rye_single_node
{
  void *data;
  RSNode *next;
};

typedef struct _rye_singly_linked_lists RSList;
struct _rye_singly_linked_lists
{
  RSNode *head;
  RSNode *tail;

  int count;
};

extern RSList *Rye_slist_alloc (void);
extern void Rye_slist_clear (RSList * list);
extern void Rye_slist_free (RSList * list);
extern RSNode *Rye_slist_prepend (RSList * list, void *data);
extern RSNode *Rye_slist_append (RSList * list, void *data);
extern void *Rye_slist_remove_first (RSList * list);
extern int Rye_slist_foreach (RSList * list, Rye_func func, void *user_data);

#endif /* DS_LIST_H_ */

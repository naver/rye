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
 * ds_stack.h
 * stack
 */

#ifndef _DS_STACK_H_
#define _DS_STACK_H_

#include "ds_list.h"

typedef struct _rye_stack RStack;
struct _rye_stack
{
  RSList list;
};

extern RStack *Rye_stack_new (void);
extern void Rye_stack_free (RStack * stack);
extern void Rye_stack_free_full (RStack * stack, Rye_func free_func);
extern RSNode *Rye_stack_push (RStack * stack, void *data);
extern void *Rye_stack_pop (RStack * stack);

#endif /* _DS_STACK_H_ */

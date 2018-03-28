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
 * quick_fit.c: Implementation of the workspace heap
 */

#ident "$Id$"

#include <assert.h>

#include "config.h"
#include "customheaps.h"
#include "memory_alloc.h"
#include "quick_fit.h"
#include "memory_alloc.h"

#if defined (ENABLE_UNUSED_FUNCTION)
HL_HEAPID ws_heap_id = 0;

/*
 * db_create_workspace_heap () - create a lea heap
 *   return: memory heap identifier
 *   req_size(in): a paramter to get chunk size
 *   recs_per_chunk(in): a parameter to get chunk size
 */
HL_HEAPID
db_create_workspace_heap (void)
{
  ws_heap_id = hl_register_lea_heap ();
  return ws_heap_id;
}

/*
 * db_destroy_workspace_heap () - destroy a lea heap
 *   return:
 *   heap_id(in): memory heap identifier to destroy
 */
void
db_destroy_workspace_heap (void)
{
  hl_unregister_lea_heap (ws_heap_id);
}
#endif

/*
 * db_ws_alloc () - call allocation function for the lea heap
 *   return: allocated memory pointer
 *   size(in): size to allocate
 */
void *
db_ws_alloc (size_t size)
{
  assert (size > 0);

  return malloc (size);
}

/*
 * db_ws_realloc () - call re-allocation function for the lea heap
 *   return: allocated memory pointer
 *   size(in): size to allocate
 */
void *
db_ws_realloc (void *ptr, size_t size)
{
  return realloc (ptr, size);
}

/*
 * db_ws_free () - call free function for the lea heap
 *   return:
 *   ptr(in): memory pointer to free
 */
void
db_ws_free (void *ptr)
{
#if 0                           /* TODO:[happy] */
  assert (ptr != NULL);
#endif

  if (ptr)
    {
      free (ptr);
    }
}

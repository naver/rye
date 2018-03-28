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
 * memory_alloc.c - Memory allocation module
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>

#include "misc_string.h"
#include "dbtype.h"
#include "memory_alloc.h"
#include "util_func.h"
#include "error_manager.h"
#include "intl_support.h"
#include "thread.h"
#include "customheaps.h"
#if !defined (SERVER_MODE)
#include "quick_fit.h"
#endif /* SERVER_MODE */

#define DEFAULT_OBSTACK_CHUNK_SIZE      32768   /* 1024 x 32 */

#if !defined (SERVER_MODE)
extern unsigned int db_on_server;
HL_HEAPID private_heap_id = 0;
#endif /* SERVER_MODE */

/*
 * ansisql_strcmp - String comparison according to ANSI SQL
 *   return: an integer value which is less than zero
 *           if s is lexicographically less than t,
 *           equal to zero if s is equal to t,
 *           and greater than zero if s is greater than zero.
 *   s(in): first string to be compared
 *   t(in): second string to be compared
 *
 * Note: The contents of the null-terminated string s are compared with
 *       the contents of the null-terminated string t, using the ANSI
 *       SQL semantics. That is, if the lengths of the strings are not
 *       the same, the shorter string is considered to be extended
 *       with the blanks on the right, so that both strings have the
 *       same length.
 */
int
ansisql_strcmp (const char *s, const char *t)
{
  for (; *s == *t; s++, t++)
    {
      if (*s == '\0')
        {
          return 0;
        }
    }

  if (*s == '\0')
    {
      while (*t != '\0')
        {
          if (*t++ != ' ')
            {
              return -1;
            }
        }
      return 0;
    }
  else if (*t == '\0')
    {
      while (*s != '\0')
        {
          if (*s++ != ' ')
            {
              return 1;
            }
        }
      return 0;
    }
  else
    {
      return (*(unsigned const char *) s < *(unsigned const char *) t) ? -1 : 1;
    }
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * ansisql_strcasecmp - Case-insensitive string comparison according to ANSI SQL
 *   return: an integer value which is less than zero
 *           if s is lexicographically less than t,
 *           equal to zero if s is equal to t,
 *           and greater than zero if s is greater than zero.
 *   s(in): first string to be compared
 *   t(in): second string to be compared
 *
 * Note: The contents of the null-terminated string s are compared with
 *       the contents of the null-terminated string t, using the ANSI
 *       SQL semantics. That is, if the lengths of the strings are not
 *       the same, the shorter string is considered to be extended
 *       with the blanks on the right, so that both strings have the
 *       same length.
 */
int
ansisql_strcasecmp (const char *s, const char *t)
{
  size_t s_length, t_length, min_length;
  int cmp_val;

  s_length = strlen (s);
  t_length = strlen (t);

  min_length = s_length < t_length ? s_length : t_length;

  cmp_val = intl_identifier_casecmp (s, t);

  /* If not equal for shorter length, return */
  if (cmp_val)
    {
      return cmp_val;
    }

  /* If equal and same size, return */
  if (s_length == t_length)
    {
      return 0;
    }

  /* If equal for shorter length and not same size, look for trailing blanks */
  s += min_length;
  t += min_length;

  if (*s == '\0')
    {
      while (*t != '\0')
        {
          if (*t++ != ' ')
            {
              return -1;
            }
        }
      return 0;
    }
  else
    {
      while (*s != '\0')
        {
          if (*s++ != ' ')
            {
              return 1;
            }
        }
      return 0;
    }
}
#endif

/*
 * db_alignment () -
 *   return:
 *   n(in):
 */
int
db_alignment (int n)
{
  return (n >= (int) sizeof (double)) ? (int) sizeof (double) :
    (n >= (int) sizeof (void *))? (int) sizeof (void *) :
    (n >= (int) sizeof (int)) ? (int) sizeof (int) : (n >= (int) sizeof (short)) ? (int) sizeof (short) : 1;
}

/*
 * db_align_to () - Return the least multiple of 'alignment' that is greater
 * than or equal to 'n'.
 *   return:
 *   n(in):
 *   alignment(in):
 */
int
db_align_to (int n, int alignment)
{
  /*
   * Return the least multiple of 'alignment' that is greater than or
   * equal to 'n'.  'alignment' must be a power of 2.
   */
  return (n + alignment - 1) & ~(alignment - 1);
}

/*
 * db_create_ostk_heap () - create an obstack heap
 *   return: memory heap identifier
 *   chunk_size(in):
 */
HL_HEAPID
db_create_ostk_heap (int chunk_size)
{
  return hl_register_ostk_heap (chunk_size);
}

/*
 * db_destroy_ostk_heap () - destroy an obstack heap
 *   return:
 *   heap_id(in): memory heap identifier to destroy
 */
void
db_destroy_ostk_heap (HL_HEAPID heap_id)
{
  hl_unregister_ostk_heap (heap_id);
}

/*
 * db_ostk_alloc () - call allocation function for the obstack heap
 *   return: allocated memory pointer
 *   heap_id(in): memory heap identifier
 *   size(in): size to allocate
 */
void *
db_ostk_alloc (HL_HEAPID heap_id, size_t size)
{
  void *ptr = NULL;
  if (heap_id && size > 0)
    {
      ptr = hl_ostk_alloc (heap_id, size);
    }
  return ptr;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_ostk_free () - call free function for the ostk heap
 *   return:
 *   heap_id(in): memory heap identifier
 *   ptr(in): memory pointer to free
 */
void
db_ostk_free (HL_HEAPID heap_id, void *ptr)
{
  if (heap_id && ptr)
    {
      hl_ostk_free (heap_id, ptr);
    }
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * db_create_private_heap () - create a thread specific heap
 *   return: memory heap identifier
 */
HL_HEAPID
db_create_private_heap (void)
{
  HL_HEAPID heap_id = 0;
#if defined (SERVER_MODE)
  heap_id = hl_register_lea_heap ();
#else /* SERVER_MODE */
  if (db_on_server)
    {
      heap_id = hl_register_lea_heap ();
    }
#endif /* SERVER_MODE */
  return heap_id;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * db_clear_private_heap () - clear a thread specific heap
 *   return:
 *   heap_id(in): memory heap identifier to clear
 */
void
db_clear_private_heap (UNUSED_ARG THREAD_ENTRY * thread_p, HL_HEAPID heap_id)
{
  if (heap_id == 0)
    {
#if defined (SERVER_MODE)
      heap_id = css_get_private_heap (thread_p);
#else /* SERVER_MODE */
      heap_id = private_heap_id;
#endif /* SERVER_MODE */
    }

  if (heap_id)
    {
      hl_clear_lea_heap (heap_id);
    }
}
#endif

/*
 * db_destroy_private_heap () - destroy a thread specific heap
 *   return:
 *   heap_id(in): memory heap identifier to destroy
 */
void
db_destroy_private_heap (HL_HEAPID heap_id)
{
  if (heap_id)
    {
      hl_clear_lea_heap (heap_id);
      hl_unregister_lea_heap (heap_id);
    }
}

/*
 * db_reset_private_heap () - reset a thread specific heap id
 *   return:
 *   heap_id(in): memory heap identifier to destroy
 */
void
db_reset_private_heap (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if defined (SERVER_MODE)
  thread_p->private_heap_id = 0;
#endif
}

/*
 * db_private_alloc () - call allocation function for current private heap
 *   return: allocated memory pointer
 *   thrd(in): thread context if it is available, otherwise NULL; if called
 *             on the server and this parameter is NULL, the function will
 *             determine the appropriate thread context
 *   size(in): size to allocate
 */
void *
db_private_alloc (UNUSED_ARG void *thrd, size_t size)
{
#if defined (CS_MODE)
  return db_ws_alloc (size);
#elif defined (SERVER_MODE)
  void *ptr = NULL;
  HL_HEAPID heap_id;

  if (size <= 0)
    {
      return NULL;
    }

  heap_id = (thrd ? ((THREAD_ENTRY *) thrd)->private_heap_id : css_get_private_heap (NULL));

  if (heap_id)
    {
      ptr = hl_lea_alloc (heap_id, size);
    }
  else
    {
      ptr = malloc (size);
      if (ptr == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
        }
    }
  return ptr;
#else /* SA_MODE */
  void *ptr = NULL;

  if (!db_on_server)
    {
      return db_ws_alloc (size);
    }
  else
    {
      if (size <= 0)
        {
          return NULL;
        }

      if (private_heap_id)
        {
          PRIVATE_MALLOC_HEADER *h = NULL;
          size_t req_sz;

          req_sz = private_request_size (size);
          h = hl_lea_alloc (private_heap_id, req_sz);

          if (h != NULL)
            {
              h->magic = PRIVATE_MALLOC_HEADER_MAGIC;
              h->alloc_type = PRIVATE_ALLOC_TYPE_LEA;
              return private_hl2user_ptr (h);
            }
          else
            {
              return NULL;
            }
        }
      else
        {
          ptr = malloc (size);
          if (ptr == NULL)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
            }
          return ptr;
        }
    }
#endif /* SA_MODE */
}


/*
 * db_private_realloc () - call re-allocation function for current private heap
 *   return: allocated memory pointer
 *   thrd(in): thread conext if it is server, otherwise NULL
 *   ptr(in): memory pointer to reallocate
 *   size(in): size to allocate
 */
void *
db_private_realloc (UNUSED_ARG void *thrd, void *ptr, size_t size)
{
#if defined (CS_MODE)
  return db_ws_realloc (ptr, size);
#elif defined (SERVER_MODE)
  void *new_ptr = NULL;
  HL_HEAPID heap_id;

  if (size <= 0)
    {
      return NULL;
    }

  heap_id = (thrd ? ((THREAD_ENTRY *) thrd)->private_heap_id : css_get_private_heap (NULL));

  if (heap_id)
    {
      new_ptr = hl_lea_realloc (heap_id, ptr, size);
    }
  else
    {
      new_ptr = realloc (ptr, size);
      if (new_ptr == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
        }
    }
  return new_ptr;
#else /* SA_MODE */
  void *new_ptr = NULL;

  if (ptr == NULL)
    {
      return db_private_alloc (thrd, size);
    }

  if (!db_on_server)
    {
      return db_ws_realloc (ptr, size);
    }
  else
    {
      if (private_heap_id)
        {
          PRIVATE_MALLOC_HEADER *h;

          h = private_user2hl_ptr (ptr);
          if (h->magic != PRIVATE_MALLOC_HEADER_MAGIC)
            {
              return NULL;
            }

          if (h->alloc_type == PRIVATE_ALLOC_TYPE_LEA)
            {
              PRIVATE_MALLOC_HEADER *new_h;
              size_t req_sz;

              req_sz = private_request_size (size);
              new_h = hl_lea_realloc (private_heap_id, h, req_sz);
              if (new_h == NULL)
                {
                  return NULL;
                }
              return private_hl2user_ptr (new_h);
            }
          else if (h->alloc_type == PRIVATE_ALLOC_TYPE_WS)
            {
              return db_ws_realloc (ptr, size);
            }
          else
            {
              return NULL;
            }
        }
      else
        {
          new_ptr = realloc (ptr, size);
          if (new_ptr == NULL)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
            }
          return new_ptr;
        }
    }
#endif /* SA_MODE */
}

/*
 * db_private_free () - call free function for current private heap
 *   return:
 *   thrd(in): thread conext if it is server, otherwise NULL
 *   ptr(in): memory pointer to free
 */
void
db_private_free (UNUSED_ARG void *thrd, void *ptr)
{
#if defined (SERVER_MODE)
  HL_HEAPID heap_id;
#endif

  if (ptr == NULL)
    {
      return;
    }

#if defined (CS_MODE)
  db_ws_free (ptr);
#elif defined (SERVER_MODE)
  heap_id = (thrd ? ((THREAD_ENTRY *) thrd)->private_heap_id : css_get_private_heap (NULL));

  if (heap_id)
    {
      hl_lea_free (heap_id, ptr);
    }
  else
    {
      free_and_init (ptr);
    }
#else /* SA_MODE */

  if (!db_on_server)
    {
      db_ws_free (ptr);
      return;
    }

  if (private_heap_id == 0)
    {
      free (ptr);
    }
  else
    {
      PRIVATE_MALLOC_HEADER *h;

      h = private_user2hl_ptr (ptr);
      if (h->magic != PRIVATE_MALLOC_HEADER_MAGIC)
        {
          /* assertion point */
          return;
        }

      if (h->alloc_type == PRIVATE_ALLOC_TYPE_LEA)
        {
          hl_lea_free (private_heap_id, h);
        }
      else if (h->alloc_type == PRIVATE_ALLOC_TYPE_WS)
        {
          db_ws_free (ptr);     /* not h */
        }
      else
        {
          return;
        }
    }
#endif /* SA_MODE */
}

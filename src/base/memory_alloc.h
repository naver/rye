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
 * memory_alloc.h - Memory allocation module
 */

#ifndef _MEMORY_ALLOC_H_
#define _MEMORY_ALLOC_H_

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <sys/types.h>
#include <stddef.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

#include "thread.h"
#include "dbtype.h"

/* Ceiling of positive division */
#define CEIL_PTVDIV(dividend, divisor) \
        (((dividend) == 0) ? 0 : ((dividend) - 1) / (divisor) + 1)

/* Make sure that sizeof returns and integer, so I can use in the operations */
#define DB_SIZEOF(val)          (sizeof(val))

/*
 * Macros related to alignments
 */
#define CHAR_ALIGNMENT          sizeof(char)
#define SHORT_ALIGNMENT         sizeof(short)
#define INT_ALIGNMENT           sizeof(int)
#define LONG_ALIGNMENT          sizeof(long)
#define FLOAT_ALIGNMENT         sizeof(float)
#define DOUBLE_ALIGNMENT        sizeof(double)
#define MAX_ALIGNMENT           DOUBLE_ALIGNMENT

#if defined(NDEBUG)
#define PTR_ALIGN(addr, boundary) \
        ((char *)((((UINTPTR)(addr) + ((UINTPTR)((boundary)-1)))) \
                  & ~((UINTPTR)((boundary)-1))))
#else
#define PTR_ALIGN(addr, boundary) \
        (memset((void*)(addr), 0,\
	   DB_WASTED_ALIGN((UINTPTR)(addr), (UINTPTR)(boundary))),\
        (char *)((((UINTPTR)(addr) + ((UINTPTR)((boundary)-1)))) \
                  & ~((UINTPTR)((boundary)-1))))
#endif

#define DB_ALIGN(offset, align) \
        (((offset) + (align) - 1) & ~((align) - 1))

#define DB_ALIGN_BELOW(offset, align) \
        ((offset) & ~((align) - 1))

#define DB_WASTED_ALIGN(offset, align) \
        (DB_ALIGN((offset), (align)) - (offset))

#define DB_ATT_ALIGN(offset) \
        (((offset) + (INT_ALIGNMENT) - 1) & ~((INT_ALIGNMENT) - 1))

/*
 * Macros related to memory allocation
 */

#define MEM_REGION_INIT_MARK       '\0' /* Set this to allocated areas */
#define MEM_REGION_SCRAMBLE_MARK         '\01'  /* Set this to allocated areas */
#define MEM_REGION_GUARD_MARK            '\02'  /* Set this as a memory guard to detect
                                                 * over/under runs  */

#if defined (RYE_DEBUG)
extern void db_scramble (void *region, int size);
#define MEM_REGION_INIT(region, size) \
        memset((region), MEM_REGION_SCRAMBLE_MARK, (size))
#define MEM_REGION_SCRAMBLE(region, size) \
        memset (region, MEM_REGION_SCRAMBLE_MARK, size)
#else /* RYE_DEBUG */
#define MEM_REGION_INIT(region, size) \
        memset((region), MEM_REGION_INIT_MARK, (size))
#define MEM_REGION_SCRAMBLE(region, size)
#endif /* RYE_DEBUG */

#define db_private_free_and_init(thrd, ptr) \
        do { \
          if ((ptr)) { \
            db_private_free ((thrd), (ptr)); \
            (ptr) = NULL; \
          } \
        } while (0)

#define free_and_init(ptr) \
        do { \
          if ((ptr)) { \
            free ((ptr)); \
            (ptr) = NULL; \
          } \
        } while (0)

#define db_ws_free_and_init(ptr) \
        do { \
          db_ws_free ((ptr)); \
          (ptr) = NULL; \
        } while (0)

extern int ansisql_strcmp (const char *s, const char *t);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int ansisql_strcasecmp (const char *s, const char *t);
#endif

#if !defined (SERVER_MODE)
extern HL_HEAPID private_heap_id;
#endif /* SERVER_MODE */

/*
 * Return the assumed minimum alignment requirement for the requested
 * size.  Multiples of sizeof(double) are assumed to need double
 * alignment, etc.
 */
extern int db_alignment (int);

/*
 * Return the value of "n" to the next "alignment" boundary.  "alignment"
 * must be a power of 2.
 */
extern int db_align_to (int n, int alignment);

extern HL_HEAPID db_create_ostk_heap (int chunk_size);
extern void db_destroy_ostk_heap (HL_HEAPID heap_id);

extern void *db_ostk_alloc (HL_HEAPID heap_id, size_t size);
#if defined(ENABLE_UNUSED_FUNCTION)
extern void db_ostk_free (HL_HEAPID heap_id, void *ptr);
#endif

extern HL_HEAPID db_create_private_heap (void);
#if defined(ENABLE_UNUSED_FUNCTION)
extern void db_clear_private_heap (THREAD_ENTRY * thread_p, HL_HEAPID heap_id);
#endif
extern void db_destroy_private_heap (HL_HEAPID heap_id);
extern void db_reset_private_heap (THREAD_ENTRY * thread_p);
extern void *db_private_alloc (void *thrd, size_t size);
extern void *db_private_realloc (void *thrd, void *ptr, size_t size);
extern void db_private_free (void *thrd, void *ptr);

extern HL_HEAPID db_create_fixed_heap (int req_size, int recs_per_chunk);
extern void db_destroy_fixed_heap (HL_HEAPID heap_id);
extern void *db_fixed_alloc (HL_HEAPID heap_id, size_t size);
#if defined(ENABLE_UNUSED_FUNCTION)
extern void db_fixed_free (HL_HEAPID heap_id, void *ptr);
#endif

#if defined(SA_MODE)
typedef struct private_malloc_header_s PRIVATE_MALLOC_HEADER;
struct private_malloc_header_s
{
  unsigned int magic;
  int alloc_type;
};

#define PRIVATE_MALLOC_HEADER_MAGIC 0xafdaafdaU

enum
{
  PRIVATE_ALLOC_TYPE_LEA = 1,
  PRIVATE_ALLOC_TYPE_WS = 2
};

#define PRIVATE_MALLOC_HEADER_ALIGNED_SIZE \
  ((sizeof(PRIVATE_MALLOC_HEADER) + 7) & ~7)

#define private_request_size(s) \
  (PRIVATE_MALLOC_HEADER_ALIGNED_SIZE + (s))

#define private_hl2user_ptr(ptr) \
  (void *)((char *)(ptr) + PRIVATE_MALLOC_HEADER_ALIGNED_SIZE)

#define private_user2hl_ptr(ptr) \
  (PRIVATE_MALLOC_HEADER *)((char *)(ptr) - PRIVATE_MALLOC_HEADER_ALIGNED_SIZE)
#endif /* SA_MODE */

#endif /* _MEMORY_ALLOC_H_ */

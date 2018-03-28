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
 * set_object.c - controls the allocation and access of set objects which
 *               can be used as attribute values in database objects.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>

#include "set_object.h"
#include "memory_alloc.h"
#include "error_manager.h"
#include "dbtype.h"
#include "dbdef.h"
#include "object_representation.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "object_print.h"
#include "parse_tree.h"
#include "db.h"
#include "environment_variable.h"
#include "system_parameter.h"
#include "oid.h"
#include "server_interface.h"

#if !defined(SERVER_MODE)
#include "work_space.h"
#include "authenticate.h"
#include "locator_cl.h"
#include "class_object.h"
#include "object_accessor.h"
#else /* !SERVER_MODE */
#include "thread.h"
#include "connection_error.h"
#endif

/* this must be the last header file included!!! */
#include "dbval.h"


/* If this is the server stub out ws_pin.
 * The other client-side functions will be completely commented out but unfortunately,
 * ws_pin appears in lots of places and its easier to define a stub.
 */

#if !defined(SERVER_MODE)
#define pthread_mutex_init(a, b)
#define pthread_mutex_destroy(a)
#define pthread_mutex_lock(b) 0
#define pthread_mutex_unlock(a)
#endif

#if !defined(SERVER_MODE)
extern unsigned int db_on_server;
#endif /* !SERVER_MODE */

/*
 * COL_ARRAY_SIZE
 *      return: size of the value indirection array
 *  size(in) : desired collection size
 *
 *  Note :
 *
 *    This converts a collection size into the number of indirection
 *    array elements necessary to hold the value blocks for this collection.
 *    Its a basic ceiling divide operation based on the COL_BLOCK_SIZE
 *    value.
 *
 */

#define COL_ARRAY_SIZE(size) ((size + (COL_BLOCK_SIZE - 1)) / COL_BLOCK_SIZE)

#if defined(SERVER_MODE)
#define SET_AREA_COUNT (32)
#else
#define SET_AREA_COUNT (4096)
#endif

typedef struct collect_block
{
  struct collect_block *next;
  long count;
  DB_VALUE val[1];
} COL_BLOCK;

typedef int (*SORT_CMP_FUNC) (const void *, const void *);
typedef int (*SETOBJ_OP) (COL * set1, COL * set2, COL * result);

static long col_init = 0;
#if defined(RYE_DEBUG)
static int debug_level = 0;
#endif

static long collection_quick_offset = 0;        /* inited by col_initialize */

#if defined(ENABLE_UNUSED_FUNCTION)
static void col_debug (COL * col);
#endif
static void col_initialize (void);

static DB_VALUE *new_block (long n);
static DB_VALUE *realloc_block (DB_VALUE * in_block, long n);

static int col_expand_array (COL * col, long blockindex);
static void col_null_values (COL * col, long bottomvalue, long topvalue);
static int col_expand_blocks (COL * col, long blockindex, long blockoffset);

#if defined(ENABLE_UNUSED_FUNCTION)
static long non_null_index (COL * col, long lower, long upper);
static int col_is_all_null (COL * col);
#endif
static void free_set_reference (DB_COLLECTION * ref);

#if !defined(SERVER_MODE)
static void merge_set_references (COL * set, DB_COLLECTION * ref);
#endif

#if defined(ENABLE_UNUSED_FUNCTION)
static int set_op (DB_COLLECTION * collection1, DB_COLLECTION * collection2,
                   DB_COLLECTION ** result, DB_DOMAIN * domain, SETOBJ_OP op);

static SET_ITERATOR *make_iterator (void);
static void free_iterator (SET_ITERATOR * it);
#endif

static int assign_set_value (COL * set, DB_VALUE * src, DB_VALUE * dest, bool implicit_coercion);
#if defined(ENABLE_UNUSED_FUNCTION)
static int check_set_object (DB_VALUE * var, int *removed_ptr);
#endif

/*
 * set_area_init() - Initialize the areas used for set storage
 *      return: none
 *
 */

void
set_area_init (void)
{
  /* we need to add safe guard to prevent any client from calling */
  /* this initialize function several times during the client's life time */

  col_initialize ();
}

/* VALUE BLOCK RESOURCE */

/* SET STRUCTURE ALLOCATION/DEALLOCATION */

/*
 * new_block() - This allocates a db_value block, using a block from a free list
 * 	         if its available
 *      return: DB_VALUE *
 *  n(in) : The top indexable offset (total size less one)
 *
 */


static DB_VALUE *
new_block (long n)
{
  COL_BLOCK *block;

  block = (COL_BLOCK *) malloc (COLBLOCKSIZE (n));
  if (block)
    {
      block->count = n;
      block->next = NULL;
      return &(block->val[0]);
    }
  return NULL;
}

/*
 * realloc_block() - This re-allocates a db_value block
 *      return: DB_VALUE *
 *  in_block(in) : The block being reallocated
 *  n(in) : The top indexable offset (total size less one)
 *
 */

static DB_VALUE *
realloc_block (DB_VALUE * in_block, long n)
{
  COL_BLOCK *block, *tmp_new;

  if (in_block == NULL)
    {
      return new_block (n);
    }

  assert (in_block != NULL);

  block = BLOCK_START (in_block);
  assert (block != NULL);
  tmp_new = (COL_BLOCK *) realloc (block, COLBLOCKSIZE (n));
  if (tmp_new == NULL)
    {
      assert (false);           /* TODO - realloc failure */
      return NULL;
    }

  assert (tmp_new != NULL);

  block = tmp_new;
  block->count = n;
  block->next = NULL;

  return &(block->val[0]);
}

/*
 * set_free_block() - This frees a db_value block, maintaining a free list
 * 	          for future use
 *      return: none
 *  in_block(in) : db_value pointer to beginning of block
 *
 */

void
set_free_block (DB_VALUE * in_block)
{
  struct collect_block *freeblock;

  if (in_block)
    {
      /* back up to beginning of block */
      freeblock = BLOCK_START (in_block);
      free_and_init (freeblock);
    }
}

/*
 * set_final() -
 *      return: none
 *
 */

void
set_final (void)
{
  col_init = 0;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * col_value_compare() - total order compare of two collection
 * 	                 values (for qsort)
 *      return:
 *  a(in) : first value
 *  b(in) : second value
 *
 */

int
col_value_compare (DB_VALUE * a, DB_VALUE * b)
{
  /* note that the coerce flag is OFF */
  return tp_value_compare (a, b, 0, 1, NULL);
}
#endif

/*
 * col_expand_array() -
 *      return: int
 *  col(in) :
 *  blockindex(in) :
 *
 *  Note :
 *      expands db_value block indirection array to include the given
 *      maximum array index.
 *
 */

static int
col_expand_array (COL * col, long blockindex)
{
  long i;

  if (blockindex <= col->arraytop)
    {
      return NO_ERROR;
    }

  if (col->array)
    {
      col->array = (DB_VALUE **) realloc (col->array, EXPAND (blockindex) * sizeof (DB_VALUE *));
    }
  else
    {
      col->array = (DB_VALUE **) malloc (EXPAND (blockindex) * sizeof (DB_VALUE *));
    }
  if (!col->array)
    {
      return ER_GENERIC_ERROR;  /* error set by memory system */
    }
  for (i = col->topblock + 1; i < EXPAND (blockindex); i++)
    {
      col->array[i] = NULL;
    }
  col->arraytop = EXPAND (blockindex) - 1;
  return NO_ERROR;
}

/*
 * col_null_values()
 *      return: none
 *  col(in) :
 *  bottomvalue(in) :
 *  topvalue(in) :
 *
 */

static void
col_null_values (COL * col, long bottomvalue, long topvalue)
{
  if (col)
    {
      for (; bottomvalue <= topvalue; bottomvalue++)
        {
          DB_MAKE_NULL (INDEX (col, bottomvalue));
        }
    }
  return;
}

/*
 * col_expand_blocks() - populates the db_value blocks in
 *                       the collection structure up to maximum block index
 *      return: int
 *  col(in) :
 *  blockindex(in) :
 *  blockoffset(in) :
 *
 */

static int
col_expand_blocks (COL * col, long blockindex, long blockoffset)
{
  DB_VALUE *block;
  int err;
  long topfullblock;

  err = col_expand_array (col, blockindex);
  if (err < 0)
    {
      return err;
    }

  if (blockindex > col->topblock)
    {
      /* If the old top block was less than a full block,
       * allocate a full block for it. Note its unfilled
       * db_values will still be initialized below.
       */
      if (col->topblockcount < BLOCKING_LESS1 && col->topblock >= 0)
        {
          block = realloc_block (col->array[col->topblock], BLOCKING_LESS1);
          if (!block)
            {
              return er_errid ();
            }
          col->array[col->topblock] = block;
          /* The next statment is just maintaining the invariant that
           * topblockcount is the size of the top block. Immediately below,
           * we will reset both topblock and topblockcount.
           */
          col->topblockcount = BLOCKING_LESS1;
        }

      if (blockoffset > collection_quick_offset)
        {
          topfullblock = blockindex;
          col->topblockcount = BLOCKING_LESS1;
        }
      else
        {
          /* want to allocate a new short block to conserve space */
          topfullblock = blockindex - 1;
          col->topblockcount = blockoffset;
          block = realloc_block (col->array[blockindex], blockoffset);
          if (!block)
            {
              return er_errid ();
            }
          col->array[blockindex] = block;
        }
      for (; col->topblock < topfullblock; col->topblock++)
        {
          block = new_block (BLOCKING_LESS1);
          if (!block)
            {
              return er_errid ();
            }
          col->array[col->topblock + 1] = block;
        }
      col->topblock = blockindex;
    }
  else if (blockindex == col->topblock && (blockoffset > col->topblockcount))
    {
      /* want to re-allocate a short block to conserve space */
      topfullblock = blockindex - 1;
      col->topblockcount = blockoffset;
      block = realloc_block (col->array[blockindex], blockoffset);
      if (!block)
        {
          return er_errid ();
        }
      col->array[blockindex] = block;
    }

  col_null_values (col, col->size, VALUETOP (col));
  return NO_ERROR;
}

/*
 * col_expand() - expands the collection size to at the given logical
 * 	          maximum collection index. New values added are set to NULL.
 *      return: int
 *  col(in) :
 *  i(in) :
 *
 */

int
col_expand (COL * col, long i)
{
  int err = NO_ERROR;

  if (col)
    {
      if (i > VALUETOP (col))
        {
          err = col_expand_blocks (col, BLOCK (i), OFFSET (i));
        }
      if (!(err < 0) && i >= col->size)
        {
          col->size = i + 1;
        }
    }
  else
    {
      /* err = bad args */
    }
  return err;
}

/*
 * col_new() - new initialized collection header structure
 *      return: returns a new initialized collection header structure
 *  size(in) : number of items in the collection
 *  settype(in) : set, multiset or sequence
 *
 */

COL *
col_new (long size, int settype)
{
  COL *col = NULL;
  int err;

  set_area_init ();

  col = (COL *) malloc (sizeof (COL));
  if (col)
    {
      /* maintain original structure members */
      col->domain = NULL;
      col->references = NULL;

      /* newer structure members */
      col->coltype = (DB_TYPE) settype;
      col->arraytop = -1;
      col->topblock = -1;
      col->topblockcount = -1;
      col->size = 0;
      col->lastinsert = 0;
      col->array = NULL;
      col->may_have_temporary_oids = 0;

      /* Pre-allocate arrays when size is available, this is particularly
       * important for sequences.  When pre-allocating sets & multisets, must
       * set the size back down as the elements do not logically exist yet
       */
      err = col_expand (col, size - 1);
      if (err)
        {
          setobj_free (col);
          return NULL;
        }

      col->size = 0;

      /* initialize the domain with one of the built in domain structures */
      if (col)
        {
          switch (settype)
            {
            case DB_TYPE_SEQUENCE:
              col->domain = &tp_Sequence_domain;
              break;
            default:
              assert (false);
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SET_INVALID_DOMAIN, 1, pr_type_name ((DB_TYPE) settype));
              setobj_free (col);
              col = NULL;
              break;
            }
        }
    }

#if !defined(NDEBUG)
  if (col != NULL && col->domain != NULL)
    {
      assert (col->domain->next == NULL);
      assert (col->domain->setdomain == NULL);
    }
#endif

  return col;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * non_null_index() - search for the greatest index between a lower and
 *                    upper bound which has a non NULL db_value.
 *      return: long
 *  col(in) :
 *  lower(in) :
 *  upper(in) :
 *
 */

static long
non_null_index (COL * col, long lower, long upper)
{
  long lowblock, highblock, midblock, offset;

  if (!col)
    {
      return lower - 1;         /* guard against NULL */
    }

  /* optimize for this most likely case */
  if (!DB_IS_NULL (INDEX (col, upper)))
    {
      return upper;
    }

  /* handle case where all collection values NULL */
  if (DB_IS_NULL (INDEX (col, lower)))
    {
      return lower - 1;
    }

  lowblock = BLOCK (lower);
  highblock = BLOCK (upper);
  while (lowblock < highblock)
    {
      midblock = (lowblock + highblock) / 2;
      if (DB_IS_NULL (&col->array[midblock][0]))
        {
          /* lowest entry in midbloack is NULL. Look to the low side */
          highblock = midblock - 1;
        }
      else if (!DB_IS_NULL (&col->array[midblock][BLOCKING_LESS1]))
        {
          /* highest entry in mid is non-NULL,  look to the high side */
          lowblock = midblock + 1;
        }
      else
        {
          /* the non-NULL to NULL transition is on this block */
          lowblock = highblock = midblock;
        }
    }
  /* here lowblock should point to a block containing one of the
   * end points of the non-NULL to NULL transitions. If the first
   * value is NULL, the non-null index is one less than the index
   * of that value. (could be -1).
   */
  offset = 0;
  while (offset < COL_BLOCK_SIZE && !DB_IS_NULL (&col->array[lowblock][offset]))
    {
      offset++;
    }
  /* offset is first NULL value. return one less */

  return (lowblock * COL_BLOCK_SIZE) + offset - 1;
}
#endif

/*
 * col_has_null() - It returns a boolean indicating whether or not
 *                  the collection has a null in it
 *      return: long
 *  col(in) :
 *
 */

int
col_has_null (COL * col)
{
  long i;

  if (!col || col->size == 0)
    {
      return 0;
    }

  /* unordered collections, must be exhaustively scanned */
  for (i = 0; i < col->size; i++)
    {
      if (DB_IS_NULL (INDEX (col, i)))
        {
          return 1;
        }
    }

  return 0;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * col_is_all_null() -
 *      return:
 *  col(in) :
 *
 *  Note :
 *      It returns a boolean indicating whether or not the collection contains
 *      all nulls.  This is being written so that multi-column index keys
 *      can be tested for NULL's but the function is general enough
 *      to support other collection types as well.
 */

static int
col_is_all_null (COL * col)
{
  long i;

  /*
   *  The collection pointer should be verified before this function is
   *  called.  But, just in case it isn't well say that a NULL collection
   *  or a collection of size o is all NULL's.
   */
  if (!col || col->size == 0)
    {
      return 1;
    }

  /* unordered collections, must be exhaustively scanned */
  for (i = 0; i < col->size; i++)
    {
      if (!DB_IS_NULL (INDEX (col, i)))
        {
          return 0;
        }
    }

  return 1;
}
#endif

/*
 * col_find() -
 *      return: long
 *  col(in) :
 *  found(in) :
 *  val(in) :
 *  do_coerce(in) :
 *
 *  Note:
 *      function serves dual purpose.
 *
 *      It returns a boolean indicating whether or not
 *      the collection has an equal value in it.
 *
 *      Whether or not its found, it returns the index the
 *      new value would be inserted at.
 */

long
col_find (COL * col, long *found, DB_VALUE * val, int do_coerce)
{
  long insertindex;

  if (col && found)
    {
      *found = 0;
      if (DB_IS_NULL (val) || col->size == 0)
        {
          /* append to end */
          /* ANSI puts NULLs at end of set collections for comparison */
          /*
           * since NULL is not equal to NULL, the insertion index
           * returned for sequences might as well be at the end where
           * its checp to insert.
           */
          insertindex = col->size;
        }
      else
        {
          insertindex = 0;
          /* sequence of unordered values. Must do sequential search */
          while (insertindex < col->size)
            {
              if (tp_value_compare (val, INDEX (col, insertindex), do_coerce, 1, NULL) == 0)
                {
                  *found = 1;
                  break;
                }
              insertindex++;
            }
        }
    }
  else
    {
      /* bad args */
      insertindex = ER_GENERIC_ERROR;
    }

  return insertindex;
}

/*
 * col_put() - set the ith value of col to val overwriting the old ith value
 *      return: int
 *  col(in) :
 *  colindex(in) :
 *  val(in) :
 *
 */

int
col_put (COL * col, long colindex, DB_VALUE * val)
{
  long offset, blockindex;
  int error;

  if (!col || colindex < 0 || !val)
    {
      /* invalid args */
      error = ER_GENERIC_ERROR;
      return error;
    }

  error = col_expand (col, colindex);

  if (!(error < 0))
    {
      if (!DB_IS_NULL (val))
        {
          col->lastinsert = colindex;
        }
      blockindex = BLOCK (colindex);
      offset = OFFSET (colindex);

      /* If this should be cloned, the caller should do it.
       * This primitive just allows the assignment to the
       * right location in the collection
       */
      col->array[blockindex][offset] = *val;
    }

  return error;
}

/*
 * col_initialize() -
 *      return: none
 *
 */

static void
col_initialize (void)
{
  if (col_init)
    {
      return;
    }

#if defined(RYE_DEBUG)
  debug_level = 1;
#endif

  /* Calculate the largest collect_block offset that will fit in
   * the workspace "quick" size. */
#define WS_MAX_QUICK_SIZE       1024

  collection_quick_offset = 1 + (WS_MAX_QUICK_SIZE - (sizeof (struct collect_block))) / sizeof (DB_VALUE);

  /* make sure that collection quick offset is smaller than
   * COL_BLOCK_SIZE. Otherwise, it will disable the more
   * efficient block handling.
   */
  if (collection_quick_offset >= COL_BLOCK_SIZE)
    {
      collection_quick_offset = COL_BLOCK_SIZE / 2;
    }

  col_init = 1;
}

/*
 * col_delete() - delete the ith value of the collection
 *                moving the old ith+1 thru nth values to i to n-1
 *      return: int
 *  col(in) :
 *  colindex(in) :
 *
 */

int
col_delete (COL * col, long colindex)
{
  long offset, blockindex, topblock, topblockcount, topoffset, fillblock;
  int error = NO_ERROR;

  if (!col_init)
    {
      col_initialize ();
    }

  if (!col || colindex < 0)
    {
      error = ER_GENERIC_ERROR;
      return error;
    }

  blockindex = BLOCK (colindex);
  offset = OFFSET (colindex);
  (void) pr_clear_value (&col->array[blockindex][offset]);

  topblock = col->topblock;
  fillblock = BLOCK (col->size - 1);

  if (blockindex < topblock)
    {
      topoffset = BLOCKING_LESS1;
    }
  else
    {
      topoffset = OFFSET (col->size - 1);
    }
  while (offset < topoffset)
    {
      col->array[blockindex][offset] = col->array[blockindex][offset + 1];
      offset++;
    }

  if (fillblock > blockindex)
    {
      /* for all the blocks greater than the insertion block,
       * we must move all the db_values DOWN one space.
       */
      while (blockindex < fillblock)
        {
          col->array[blockindex][BLOCKING_LESS1] = col->array[blockindex + 1][0];
          blockindex++;
          if (blockindex < topblock)
            {
              topblockcount = BLOCKING_LESS1;
            }
          else
            {
              topblockcount = col->topblockcount;
            }
          memmove (&col->array[blockindex][0], &col->array[blockindex][1], topblockcount * sizeof (DB_VALUE));
        }
    }

  /* Now that we're finished shifting the DB_VALUES down by one, we need
     to NULL the un-used DB_VALUE at the end so that we know that it's
     available for use later (i.e. we don't want to run pr_clear_value()
     on it later because it may contain pointers to data which was copied
     to other DB_VALUEs during the shift operations).

     Also, just in case the DB_VALUE pointed to an object, set the
     object pointer to NULL so that the GC doesn't get confused. - JB */
  PRIM_SET_NULL (INDEX (col, (col->size - 1)));
  INDEX (col, (col->size - 1))->data.op = NULL;

  col->size--;
  if (col->lastinsert >= col->size)
    {
      col->lastinsert = col->size - 1;
    }

  if (BLOCK (col->size) < topblock)
    {
      set_free_block (col->array[topblock]);
      col->array[topblock] = NULL;
      col->topblock--;
      if (col->topblock >= 0)
        {
          col->topblockcount = BLOCKING_LESS1;
        }
      else
        {
          col->topblockcount = -1;
        }
    }

  return error;
}

/*
 * col_add() - add val to col
 *             if its a set - use set union.
 *             if its a multiset - use multiset union.
 *             if its a sequence - append.
 *      return: int
 *  col(in) :
 *  val(in) :
 *
 *  Note :
 *     CALLER is responsible for cloning, if necessary.
 *     That makes this function eligible to cheaply move stack
 *     variables into a collection.
 */

int
col_add (COL * col, DB_VALUE * val)
{
  int error = NO_ERROR;

  if (!col)
    {
      error = ER_GENERIC_ERROR;
      return error;
    }

  switch (col->coltype)
    {
    case DB_TYPE_SEQUENCE:
      error = col_put (col, col->size, val);
      break;
    default:
      /* bad args */
      assert (false);
      error = ER_GENERIC_ERROR;
      break;
    }

  return error;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * col_drop() - drop val to col
 *      return: int
 *  col(in) :
 *  val(in) :
 *
 *  Note :
 *      if its a set - use set difference.
 *      if its a multiset - use multiset difference.
 *      if its a sequence - find one and set it to NULL
 */

int
col_drop (COL * col, DB_VALUE * val)
{
  int error = NO_ERROR;
  long i, found;

  if (!col || !val)
    {
      error = ER_GENERIC_ERROR; /* bad args */
      return error;
    }

  i = col_find (col, &found, val, 0);
  if (i < 0)
    {
      error = i;
      return error;
    }
  if (found)
    {
      PRIM_SET_NULL (INDEX (col, i));
    }

  return error;
}

/*
 * col_drop_nulls() - drop all nulls calues in col
 *      return: int
 *  col(in) :
 *
 */

int
col_drop_nulls (COL * col)
{
  int error = NO_ERROR;
  long i;

  if (!col)
    {
      error = ER_GENERIC_ERROR; /* bad args */
      return error;
    }

  /*  Unsorted collections must be scanned entirely.
   */
  for (i = col->size - 1; i >= 0; i--)
    {
      if (DB_IS_NULL (INDEX (col, i)))
        {
          error = col_delete (col, i);
        }
    }

  return error;
}

/*
 * col_permanent_oids() - assign all oid's in a collection permamenent values
 *      return: int
 *  col(in) :
 *
 */

int
col_permanent_oids (COL * col)
{
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int tcount, i;
  LC_OIDSET *oidset;
  LC_OIDMAP *oidmap;
  DB_VALUE *val;
  DB_OBJECT *obj;

  if (col->may_have_temporary_oids)
    {
      oidset = locator_make_oid_set ();
      if (oidset == NULL)
        {
          error = er_errid ();
        }
      else
        {
          /* Whip through the elements building a lockset for any temporary
           * objects. If none are found, there's nothing to do.
           */
          tcount = 0;

          for (i = 0; i < col->size && !error; i++)
            {
              val = INDEX (col, i);
              if (DB_IS_NULL (val))
                {
                  continue;
                }

              if (DB_VALUE_DOMAIN_TYPE (val) == DB_TYPE_OBJECT)
                {
                  obj = DB_GET_OBJECT (val);
                  if (obj != NULL && OBJECT_HAS_TEMP_OID (obj))
                    {
                      tcount++;
                      oidmap = locator_add_oidset_object (oidset, obj);
                      if (oidmap == NULL)
                        {
                          error = er_errid ();
                        }
                    }
                }
            }

          /* tcount has the number of unqiue OIDs in the oidset */
          if (!error && tcount)
            {
              error = locator_assign_oidset (oidset, NULL);
            }

          locator_free_oid_set (NULL, oidset);
        }

      /* we can now turn this off */
      if (!error)
        {
          col->may_have_temporary_oids = 0;
        }
    }
#endif

  return error;
}
#endif

/* DEBUGGING FUNCTIONS */

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * col_debug() - Print a description of the set
 *      return: none
 *  col(in) :
 *
 */

static void
col_debug (COL * col)
{
  setobj_print (stdout, col);
}
#endif

/* SET REFERENCE MAINTENANCE */

/*
 * set_make_reference() - set reference structure
 *      return:
 *
 * Note :
 *    Builds a set reference structure for the application layer.
 *    These must not be allocated in the workspace because the reference
 *    owner field must serve as a root for the garbage collector.
 */

DB_COLLECTION *
set_make_reference (void)
{
  DB_COLLECTION *ref;

  set_area_init ();

  ref = (DB_COLLECTION *) malloc (sizeof (DB_COLLECTION));
  if (ref != NULL)
    {
      ref->owner = NULL;
      ref->attribute = -1;
      ref->ref_link = ref;      /* circular list of one */
      ref->set = NULL;
      ref->ref_count = 1;       /* when this goes to zero, we can free */
      ref->disk_set = NULL;
      ref->disk_size = 0;
      ref->need_clear = false;
      ref->disk_domain = NULL;
    }

  return (ref);
}

/*
 * free_set_reference() - Frees storage for a reference structure
 *      return: none
 *  ref(in) : set reference structure
 *
 *  Note :
 *      Make sure the MOP fields are NULL before freeing so they
 *      don't serve as GC roots.
 *      Removes the reference from the chain it there is more than one.
 *      If this is the only reference in the chain, it will free the
 *      associated set object as well.
 *      If there are other references in the chain, and the referenced set
 *      points to this reference, adjust the set so that it points to one
 *      of the other references in the chain.
 */

static void
free_set_reference (DB_COLLECTION * ref)
{
  DB_COLLECTION *r;

  /* if refcount has become so large that the int goes negative,
   * don't bother to free it, there is probably something wrong
   * in the application anyway
   */
  if (ref == NULL)
    {
      return;
    }

  if (ref->ref_count <= 0)
    {
      return;
    }

  ref->ref_count--;

  if (ref->ref_count != 0)
    {
      return;
    }
  /* search for the previous reference in the chain */
  for (r = ref; r != NULL && r->ref_link != ref; r = r->ref_link);

  if (r == ref)
    {
      /* we're the only one in the list, and this is an unconnected
       * set, free the set object too
       */
      if (ref->set)
        {
          /* always NULL the reference cache in the set */
          ref->set->references = NULL;
          /* if the set is unconnected, free it */
          if (ref->owner == NULL)
            {
              setobj_free (ref->set);
              ref->set = NULL;
            }
        }
    }
  else
    {
      /* make sure the set object points to a real reference */
      if (ref->set != NULL && ref->set->references == ref)
        {
          ref->set->references = r;
        }

      /* take it out of the chain */
      if (r != NULL)
        {
          r->ref_link = ref->ref_link;
        }
    }

  /* free any disk set */
  if (ref->disk_set && ref->need_clear)
    {
      free_and_init (ref->disk_set);
    }
  ref->disk_set = NULL;
  ref->need_clear = false;
  ref->disk_size = 0;
  ref->disk_domain = NULL;

  /* NULL this field so it doesn't serve as a GC root */
  ref->owner = NULL;

  free_and_init (ref);
}

#if !defined(SERVER_MODE)

/*
 * merge_set_references() - Merges two set reference chains
 *      return: none
 *  set(in) : set object
 *  ref(in) : new reference
 *
 *  Note :
 *    This is necessary in cases where there is a lot of swapping and
 *    applications keep set references cached in their memory for long
 *    periods of time.
 *    In these cases it is possible for a set to be swapped out making
 *    the reference unbound.  If the set is swapped in later, it does
 *    not know that there were previous references to it until those
 *    references are used.  If after the set is swapped in another request
 *    is made, a new reference is created because the system doesn't know
 *    about the old one.  Later when the old reference is used, the system
 *    detects that there is already a reference chain for the set and it
 *    must merge them together.
 *    This could be avoided if set references were interned like MOPs,
 *    the search key would be the owning object, attribute id pair.
 *    This would however add overhead to the creation of set references
 *    and/or the fetching of objects.
 */

static void
merge_set_references (COL * set, DB_COLLECTION * ref)
{
  DB_COLLECTION *r;

  if (set != NULL && ref != NULL)
    {
      /* make all new references point to the set */
      r = ref;
      do
        {
          r->set = set;
          r = r->ref_link;
        }
      while (r != ref);

      /* merge the lists */
      if (set->references == NULL)
        {
          set->references = ref;
        }
      else
        {
          r = set->references->ref_link;
          set->references->ref_link = ref->ref_link;
          ref->ref_link = r;
        }
    }
}
#endif

/*
 * set_tform_disk_set() - This gets the set object associated with a set reference
 *      return: COL *
 *  ref(in) : set refernce
 *  setptr(in) :
 *
 *  Note :
 *      It may need to transform the disk_set image to the memory image.
 *      The rule is this: if the reference has a non-NULL disk_set field,
 *      then the set must be transformed.  After tranformation the disk_set
 *      must be freed and the disk_set, disk_size cleared.
 */

int
set_tform_disk_set (DB_COLLECTION * ref, COL ** setptr)
{
  OR_BUF buf;

  if (ref->disk_set)
    {
      or_init (&buf, ref->disk_set, 0);
      ref->set = or_get_set (&buf, ref->disk_domain);
      if (ref->set == NULL)
        {
          return er_errid ();
        }
      *setptr = ref->set;

      /* free/clear the disk_set */
      if (ref->need_clear)
        {
          free_and_init (ref->disk_set);
        }
      ref->disk_set = NULL;
      ref->disk_size = 0;
      ref->need_clear = false;
      ref->disk_domain = NULL;
    }
  else
    {
      *setptr = ref->set;       /* already been unpacked */
    }

  return NO_ERROR;

}                               /* set_tform_disk_set */

/*
 * set_get_setobj() - This gets the set object associated with a set reference
 *      return: error code
 *  ref(in) : set refernce
 *  setptr(out) : returned pointer to set object
 *  for_write(in) : indicates intention
 *
 *  Note :
 *      If the set is swapped out, it will be brought back in from
 *      disk.  The for_write flag is used so that if the object needs
 *      to be fetched, the appropriate lock will be aquired early.
 *      This is a dangerous function, the object had better be pinned
 *      immediately upon return from this function.  Absolutely do not
 *      call any function that allocates storage before pinning the owning
 *      object.
 */

int
set_get_setobj (DB_COLLECTION * ref, COL ** setptr, UNUSED_ARG int for_write)
{
  int error = NO_ERROR;
  COL *set = NULL;

  if (ref != NULL)
    {
      if (set_tform_disk_set (ref, &set) != NO_ERROR)
        {
          /* an error (like "out of memory") should have already been set */
          *setptr = NULL;
          return er_errid ();
        }
#if !defined(SERVER_MODE)
      {
        char *mem;
        if (set == NULL && ref->owner != NULL)
          {
            error = obj_locate_attribute (ref->owner, ref->attribute, for_write, &mem, NULL);
            if (error == NO_ERROR && mem != NULL)
              {
                /* this should be a PRIM level accessor */
                set = *(COL **) mem;
                merge_set_references (set, ref);
              }
          }
      }
#endif
    }
  *setptr = set;
  return (error);
}

/*
 * set_connect() - This establishes ownership of a set
 *      return: error
 *  ref(in) : set reference
 *  owner(in) : pointer to the owning object
 *  attid(in) : attribute id
 *  domain(in) :  attribute domain
 *
 *  Note :
 *      It is called when a set is assigned as the value of an attribute.
 *      The classobj and attid information is placed in the set reference
 *      structure so the set can be retrieved if it is swapped out.
 *      The domain information is cached in the set structure itself so that
 *      each set doesn't have to carry around an independent domain which
 *      wastes space.  The domain structure comes from the class and will
 *      not be swapped out before all objects (including their sets) have
 *      been swapped.  Its ok therefore to treat this as if it were a static
 *      domain.
 */

int
set_connect (DB_COLLECTION * ref, MOP owner, int attid, TP_DOMAIN * domain)
{
  DB_COLLECTION *r;

  assert (domain != NULL);
  assert (domain->is_cached);

  if (ref == NULL)
    {
      return NO_ERROR;
    }
  /* must make sure ALL reference structures are properly tagged */
  r = ref;
  do
    {
      r->owner = owner;
      r->attribute = attid;
      r = r->ref_link;
    }
  while (r != ref);

  /* if this is NULL, is it an error ? */
  if (ref->set != NULL)
    {
#if !defined (NDEBUG)
      if (domain != NULL)
        {
          assert (domain->is_cached);
          assert (domain->next == NULL);
        }
#endif
      ref->set->domain = domain;
    }

  return (NO_ERROR);
}

/*
 * set_disconnect() - This is used to remove ownership from a set
 *      return: error
 *  ref(in) : set reference
 *
 *  Note :
 *      The owner fields in the reference structure are cleared
 *
 */

int
set_disconnect (DB_COLLECTION * ref)
{
  int error = NO_ERROR;
  DB_COLLECTION *r;

  if (ref == NULL)
    {
      return error;
    }
  /* disconnect the references */
  r = ref;

  do
    {
      r->owner = NULL;
      r->attribute = -1;
      r = r->ref_link;
    }
  while (r != ref);
  return (error);
}

/*
 * set_change_owner() -
 *      return: set reference (NULL if error)
 *  ref(in) : set reference
 *  owner(in) : new owner
 *  attid(in) : attribute id
 *  domain(in) : set domain
 *
 *  Note :
 *      This is simiar to set_connect except that it handles the case
 *      where the set is already owned by another object and must
 *      be copied.
 *      If NULL is returned, it indicates some kind of error.
 *      The domain is provided for connection only, semantic checking
 *      of domain validity is assumed to have already been done.
 */

DB_COLLECTION *
set_change_owner (UNUSED_ARG DB_COLLECTION * ref, UNUSED_ARG MOP owner,
                  UNUSED_ARG int attid, UNUSED_ARG TP_DOMAIN * domain)
{
  DB_COLLECTION *new_ = NULL;
#if !defined(SERVER_MODE)
  COL *current, *newset;
  int pin;

  /* fetch set of interest */
  if (set_get_setobj (ref, &current, 0) != NO_ERROR)
    {
      return NULL;
    }

  if (ref == NULL || current == NULL)
    {
      /* this indicates an unbound set reference with no owner,
         shouldn't happen, probably should have better error here */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }
  else
    {
      pin = ws_pin (ref->owner, 1);

      if (ref->owner == NULL || (ref->owner == owner && ref->attribute == attid))
        {
          new_ = ref;
        }
      else
        {
          /* must make a copy */
          newset = setobj_copy (current);
          if (newset != NULL)
            {
              new_ = setobj_get_reference (newset);
              if (new_ == NULL)
                {
                  setobj_free (newset);
                }
            }
        }
      if (new_ != NULL)
        {
          set_connect (new_, owner, attid, domain);
        }

      (void) ws_pin (ref->owner, pin);
    }
#endif
  return (new_);
}

/* SET REFERENCE SHELLS */
/* These are shell functions that resolve a set reference through a
   DB_COLLECTION structure and make a corresponding call to a COL structure
   with the referenced set.
*/
/*
 * set_create() -
 *      return: DB_COLLECTION *
 *  type(in) :
 *  initial_size(in) :
 *
 */

DB_COLLECTION *
set_create (DB_TYPE type, int initial_size)
{
  DB_COLLECTION *col;
  COL *setobj;

  col = NULL;
  setobj = setobj_create (type, initial_size);

  if (setobj == NULL)
    {
      return col;
    }

  col = set_make_reference ();

  if (col == NULL)
    {
      setobj_free (setobj);
    }
  else
    {
      col->set = setobj;
      setobj->references = col;
    }
  return col;
}

/*
 * set_create_sequence()
 *      return: DB_COLLECTION *
 *  size(in) :
 *
 */

DB_COLLECTION *
set_create_sequence (int size)
{
  return set_create (DB_TYPE_SEQUENCE, size);
}

#if defined (ENABLE_UNUSED_FUNCTION)
DB_COLLECTION *
set_create_with_domain (TP_DOMAIN * domain, int initial_size)
{
  DB_COLLECTION *col;
  COL *setobj;

  col = NULL;
  setobj = setobj_create_with_domain (domain, initial_size);
  if (setobj == NULL)
    {
      return (col);
    }

  col = set_make_reference ();

  if (col == NULL)
    {
      setobj_free (setobj);
    }
  else
    {
      col->set = setobj;
      setobj->references = col;
    }
  return (col);
}
#endif

/*
 * set_copy() -
 *      return: DB_COLLECTION *
 *  set(in) :
 *
 */


DB_COLLECTION *
set_copy (DB_COLLECTION * set)
{
  COL *srcobj, *newobj;
  DB_COLLECTION *new_;
#if !defined(SERVER_MODE)
  int pin;
#endif

  new_ = NULL;
  if (set_get_setobj (set, &srcobj, 0) != NO_ERROR)
    {
      return new_;
    }

  if (set != NULL && srcobj != NULL)
    {
#if !defined(SERVER_MODE)
      pin = ws_pin (set->owner, 1);
#endif
      newobj = setobj_copy (srcobj);
      if (newobj != NULL)
        {
          new_ = set_make_reference ();
          if (new_ != NULL)
            {
              new_->set = newobj;
              newobj->references = new_;
            }
          else
            {
              setobj_free (newobj);
            }
        }
#if !defined(SERVER_MODE)
      (void) ws_pin (set->owner, pin);
#endif
    }
  return (new_);
}

/*
 * set_clear() -
 *      return: int
 *  set(in) :
 *
 */

#if defined (ENABLE_UNUSED_FUNCTION)
int
set_clear (DB_COLLECTION * set)
{
  int error;
  COL *obj;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 1);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (set == NULL || obj == NULL)
    {
      return error;
    }

#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  setobj_clear (obj);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (error);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * set_free() -
 *      return: none
 *  set(in) :
 *
 */

void
set_free (DB_COLLECTION * set)
{
  free_set_reference (set);
}

/*
 * set_get_element() -
 *      return: int
 *  set(in) :
 *  index(in) :
 *  value(in) :
 *
 */

int
set_get_element (DB_COLLECTION * set, int index, DB_VALUE * value)
{
  int error;
  COL *obj;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (set == NULL || obj == NULL)
    {
      return error;
    }
#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  error = setobj_get_element (obj, index, value);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (error);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * set_get_element_nocopy() -
 *      return: error code
 *  set(in) : set to examine
 *  index(in) : element index
 *  value(out) : value to put result in
 *
 *  Note :
 *    Hack to optimize iteration over the property list sets used for
 *    the storage of index and constraint information.
 */

int
set_get_element_nocopy (DB_COLLECTION * set, int index, DB_VALUE * value)
{
  int error;
  DB_VALUE *valptr;

  /* set contents can't be swapped out here */
  error = setobj_get_element_ptr (set->set, index, &valptr);

  if (error == NO_ERROR)
    {
      *value = *valptr;
    }

  return error;
}
#endif

/*
 * set_add_element() -
 *      return: error code
 *  set(in) :
 *  value(out) :
 *
 */

int
set_add_element (DB_COLLECTION * set, DB_VALUE * value)
{
  int error;
  COL *obj;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 1);
  if (error == NO_ERROR)
    {
      if (set != NULL && obj != NULL)
        {
#if !defined(SERVER_MODE)
          pin = ws_pin (set->owner, 1);
#endif
          if (set->owner == NULL)
            {
              error = setobj_add_element (obj, value);
            }

#if !defined(SERVER_MODE)
          /* get write lock on owner and mark as dirty */
          else
            {
#if !defined (NDEBUG)
              MOP class_mop = NULL;

              class_mop = ws_class_mop (set->owner);
#endif
              /* the caller should have holden a lock already
               * we need write lock here
               */
              error = obj_lock (set->owner, 1);
              if (error == NO_ERROR)
                {
                  error = setobj_add_element (obj, value);
                }
            }
#endif

#if !defined(SERVER_MODE)
          (void) ws_pin (set->owner, pin);
#endif
        }
    }
  return (error);
}

/* call this when you know the elements are ok */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * set_add_element_quick() -
 *      return: error code
 *  set(in) :
 *  value(out) :
 *
 */

int
set_add_element_quick (DB_COLLECTION * set, DB_VALUE * value)
{
  return set_add_element (set, value);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * set_add_element_quick() -
 *      return: error code
 *  set(in) :
 *  index(in) :
 *  value(out) :
 *
 */

int
set_put_element (DB_COLLECTION * set, int index, DB_VALUE * value)
{
  int error;
  COL *obj;
#if !defined(SERVER_MODE)
  int pin;
#endif
  error = set_get_setobj (set, &obj, 1);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (set != NULL && obj != NULL)
    {
#if !defined(SERVER_MODE)
      pin = ws_pin (set->owner, 1);
#endif
      if (set->owner == NULL)
        {
          error = setobj_put_element (obj, index, value);
        }

#if !defined(SERVER_MODE)
      /* get write lock on owner and mark as dirty */
      else
        {
#if !defined (NDEBUG)
          MOP class_mop = NULL;

          class_mop = ws_class_mop (set->owner);
#endif
          /* the caller should have holden a lock already
           * we need write lock here
           */
          error = obj_lock (set->owner, 1);
          if (error == NO_ERROR)
            {
              error = setobj_put_element (obj, index, value);
            }
        }
#endif

#if !defined(SERVER_MODE)
      (void) ws_pin (set->owner, pin);
#endif
    }

  return (error);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * set_drop_element() -
 *      return:
 *  set(in) :
 *  value(out) :
 *  match_nulls(in) :
 *
 */

int
set_drop_element (DB_COLLECTION * set, DB_VALUE * value, bool match_nulls)
{
  int error;
  COL *obj;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 1);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (set != NULL && obj != NULL)
    {
#if !defined(SERVER_MODE)
      pin = ws_pin (set->owner, 1);
#endif
      if (set->owner == NULL)
        {
          error = setobj_drop_element (obj, value, match_nulls);
        }

#if !defined(SERVER_MODE)
      else
        {
#if !defined (NDEBUG)
          MOP class_mop = NULL;

          class_mop = ws_class_mop (set->owner);
#endif
          /* the caller should have holden a lock already
           * we need write lock here
           */
          error = obj_lock (set->owner, 1);
          if (error == NO_ERROR)
            {
              error = setobj_drop_element (obj, value, match_nulls);
            }
        }
#endif

#if !defined(SERVER_MODE)
      (void) ws_pin (set->owner, pin);
#endif
    }
  return (error);
}
#endif

/*
 * set_drop_seq_element() -
 *      return:
 *  set(out) :
 *  index(in) :
 *
 */


int
set_drop_seq_element (DB_COLLECTION * set, int index)
{
  int error = NO_ERROR;
  COL *obj;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 1);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (set != NULL && obj != NULL)
    {
#if !defined(SERVER_MODE)
      pin = ws_pin (set->owner, 1);
#endif
      if (set->owner == NULL)
        {
          error = setobj_drop_seq_element (obj, index);
        }

#if !defined(SERVER_MODE)
      else
        {
#if !defined (NDEBUG)
          MOP class_mop = NULL;

          class_mop = ws_class_mop (set->owner);
#endif
          /* the caller should have holden a lock already
           * we need write lock here
           */
          error = obj_lock (set->owner, 1);
          if (error == NO_ERROR)
            {
              error = setobj_drop_seq_element (obj, index);
            }
        }
#endif

#if !defined(SERVER_MODE)
      (void) ws_pin (set->owner, pin);
#endif
    }
  return (error);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * set_find_seq_element() -
 *      return: int
 *  set(in) :
 *  value(in) :
 *  index(in) :
 *
 */

int
set_find_seq_element (DB_COLLECTION * set, DB_VALUE * value, int index)
{
  COL *obj;
  int psn = -1;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 1);
  if (error != NO_ERROR)
    {
      return (psn);
    }

  if (set != NULL && obj != NULL)
    {
#if !defined(SERVER_MODE)
      pin = ws_pin (set->owner, 1);
#endif
      psn = setobj_find_seq_element (obj, value, index);
#if !defined(SERVER_MODE)
      (void) ws_pin (set->owner, pin);
#endif
    }

  return (psn);
}

/*
 * set_cardinality() -
 *      return: int
 *  set(in) :
 *
 */

int
set_cardinality (DB_COLLECTION * set)
{
  COL *obj;
  int length;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin;
#endif
  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return -1;
    }

  if (set == NULL || obj == NULL)
    {
      return 0;
    }

#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  length = setobj_cardinality (obj);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (length);
}
#endif

/*
 * set_size() -
 *      return:
 *  set(in) : int
 *
 */

int
set_size (DB_COLLECTION * set)
{
  COL *obj;
  int length;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return -1;
    }

  if (set == NULL || obj == NULL)
    {
      return 0;
    }

#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  length = setobj_size (obj);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (length);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * set_isempty() -
 *      return: bool
 *  set(in) :
 *
 */

bool
set_isempty (DB_COLLECTION * set)
{
  COL *obj;
  bool isempty = true;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return isempty;
    }

  if (set == NULL || obj == NULL)
    {
      return isempty;
    }
#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  isempty = setobj_isempty (obj);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif

  return (isempty);
}

/*
 * set_is_all_null() -
 *      return: Boolean value.
 *              true if the sequence contains all NULL's and
 *              false otherwise.
 *  set(in) : set pointer
 *
 *  Note :
 *      Check the contents of the collection and return true if the
 *      contents contain nothing but NULL values.  Return false otherwise.
 *      This function is used by seq_key_is_null() to look for NULL
 *      multi-column index keys.
 */

bool
set_is_all_null (DB_COLLECTION * set)
{
  COL *obj;
  bool isallnull = true;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return isallnull;
    }

  if (set == NULL || obj == NULL)
    {
      return isallnull;
    }

#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  isallnull = col_is_all_null (obj);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif

  return (isallnull);
}

/*
 * set_has_null() -
 *      return: bool
 *  set(in) :
 *
 */

bool
set_has_null (DB_COLLECTION * set)
{
  COL *obj;
  bool hasnull = true;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return hasnull;
    }

  if (set == NULL || obj == NULL)
    {
      return hasnull;
    }

#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  hasnull = col_has_null (obj);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (hasnull);
}

/*
 * set_ismember() -
 *      return: bool
 *  set(in) :
 *  value(out) :
 *
 */

bool
set_ismember (DB_COLLECTION * set, DB_VALUE * value)
{
  COL *obj;
  bool ismember = false;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return ismember;
    }

  if (set == NULL || obj == NULL)
    {
      return ismember;
    }

#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  ismember = setobj_ismember (obj, value, 0);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (ismember);
}

/*
 * set_issome() -
 *      return: int
 *  value(in) :
 *  set(in) :
 *  op(in) :
 *  do_coercion(in) :
 *
 */

int
set_issome (DB_VALUE * value, DB_COLLECTION * set, PT_OP_TYPE op, int do_coercion)
{
  COL *obj;
  int issome = -1;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin;
#endif

  assert (op == PT_IS_IN);

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return issome;
    }

  if (set == NULL || obj == NULL)
    {
      return issome;
    }

#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  issome = setobj_issome (value, obj, op, do_coercion);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (issome);
}

/*
 * set_convert_oids_to_objects() -
 *      return: error code
 *  set(in) :
 *
 */

int
set_convert_oids_to_objects (DB_COLLECTION * set)
{
  int error = NO_ERROR;
  COL *obj;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 1);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (set != NULL && obj != NULL)
    {
#if !defined(SERVER_MODE)
      pin = ws_pin (set->owner, 1);
#endif
      if (set->owner == NULL)
        {
          error = setobj_convert_oids_to_objects (obj);
        }
#if !defined(SERVER_MODE)
      else if ((error = obj_lock (set->owner, 1)) == NO_ERROR)
        {
          error = setobj_convert_oids_to_objects (obj);
        }
#endif

#if !defined(SERVER_MODE)
      (void) ws_pin (set->owner, pin);
#endif
    }

  return (error);
}
#endif

/*
 * set_get_domain() -
 *      return: TP_DOMAIN *
 *  set(in) :
 *
 */

TP_DOMAIN *
set_get_domain (DB_COLLECTION * set)
{
  COL *obj;
  TP_DOMAIN *domain = NULL;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return domain;
    }

  if (set == NULL || obj == NULL)
    {
      return domain;
    }

#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  domain = setobj_get_domain (obj);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (domain);
}

/*
 * set_seq_compare() -
 *      return: int
 *  set1(in) :
 *  set2(in) :
 *  do_coercion(in) :
 *  total_order(in) :
 *
 */

int
set_seq_compare (DB_COLLECTION * set1, DB_COLLECTION * set2, int do_coercion, int total_order)
{
  COL *obj1, *obj2;
  int status = DB_UNK;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin1, pin2;
#endif

  error = set_get_setobj (set1, &obj1, 0);

  if (error != NO_ERROR)
    {
      return status;
    }
  if (set1 == NULL || obj1 == NULL)
    {
      return status;
    }
#if !defined(SERVER_MODE)
  pin1 = ws_pin (set1->owner, 1);
#endif
  if (set_get_setobj (set2, &obj2, 0) == NO_ERROR)
    {
      if (set2 != NULL && obj2 != NULL)
        {
#if !defined(SERVER_MODE)
          pin2 = ws_pin (set2->owner, 1);
#endif
          status = setobj_compare_order (obj1, obj2, do_coercion, total_order);

#if !defined(SERVER_MODE)
          (void) ws_pin (set2->owner, pin2);
#endif
        }
    }
#if !defined(SERVER_MODE)
  (void) ws_pin (set1->owner, pin1);
#endif
  return (status);
}

/*
 * set_get_type() -
 *      return: DB_TYPE
 *  set(in) :
 *
 */

DB_TYPE
set_get_type (DB_COLLECTION * set)
{
  COL *obj;
  DB_TYPE stype = DB_TYPE_NULL;
  int error = NO_ERROR;

#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return stype;
    }
  if (set == NULL || obj == NULL)
    {
      return stype;
    }
#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  stype = obj->coltype;
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (stype);
}

/*
 * set_check_domain() -
 *      return: TP_DOMAIN_STATUS
 *  set(in) :
 *  domain(in) :
 *
 */

TP_DOMAIN_STATUS
set_check_domain (DB_COLLECTION * set, TP_DOMAIN * domain)
{
  TP_DOMAIN_STATUS status;
  COL *obj;
  int error = NO_ERROR;

#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      status = DOMAIN_ERROR;
      return status;
    }
  if (set == NULL || obj == NULL)
    {
      status = DOMAIN_INCOMPATIBLE;
      return status;
    }
#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  status = setobj_check_domain (obj, domain);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (status);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * set_coerce() -
 *      return: DB_COLLECTION *
 *  set(in) :
 *  domain(in) :
 *  implicit_coercion(in) :
 *
 */

DB_COLLECTION *
set_coerce (DB_COLLECTION * set, TP_DOMAIN * domain, bool implicit_coercion)
{
  COL *srcobj, *newobj;
  DB_COLLECTION *new_;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin;
#endif

  new_ = NULL;
  error = set_get_setobj (set, &srcobj, 0);
  if (error != NO_ERROR)
    {
      return new_;
    }
  if (set == NULL || srcobj == NULL)
    {
      return new_;
    }
#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  newobj = setobj_coerce (srcobj, domain, implicit_coercion);
  if (newobj != NULL)
    {
      new_ = set_make_reference ();
      if (new_ != NULL)
        {
          new_->set = newobj;
          newobj->references = new_;
        }
      else
        {
          setobj_free (newobj);
        }
    }
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (new_);
}
#endif

/*
 * set_fprint() -
 *      return: none
 *  fp(in) :
 *  set(in) :
 *
 */

void
set_fprint (FILE * fp, DB_COLLECTION * set)
{
  COL *obj;
  int error = NO_ERROR;

#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return;
    }
  if (set == NULL || obj == NULL)
    {
      return;
    }
#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  setobj_print (fp, obj);
#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
}

/*
 * set_print() -
 *      return: none
 *  set(in) :
 *
 */

void
set_print (DB_COLLECTION * set)
{
  set_fprint (stdout, set);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * set_filter() -
 *      return: error code
 *  set(in) :
 *
 */

int
set_filter (DB_COLLECTION * set)
{
  int error;
  COL *obj;
#if !defined(SERVER_MODE)
  int pin;
#endif

  error = set_get_setobj (set, &obj, 0);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (set == NULL || obj == NULL)
    {
      return error;
    }
#if !defined(SERVER_MODE)
  pin = ws_pin (set->owner, 1);
#endif
  error = setobj_filter (obj, 1, NULL);

#if !defined(SERVER_MODE)
  (void) ws_pin (set->owner, pin);
#endif
  return (error);
}

/*
 * set_op() -
 *      return:
 *  collection1(in) : input collections
 *  collection2(in) : input collections
 *  result(in) : output set or multiset
 *  domain(in) : desired result domain
 *  op(in) :
 *
 *  Note :
 *      takes the set difference of two collections
 *      If either argument is a sequence, it is
 *      first corced to the result domain, which must be
 *      a multiset or set.
 *
 */

static int
set_op (DB_COLLECTION * collection1, DB_COLLECTION * collection2,
        DB_COLLECTION ** result, DB_DOMAIN * domain, SETOBJ_OP op)
{
  COL *col1, *col2;
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  int pin1, pin2;
#endif

  CHECKNULL (collection1);
  CHECKNULL (collection2);
  CHECKNULL (result);
  CHECKNULL (domain);
  CHECKNULL (domain->type);

  error = set_get_setobj (collection1, &col1, 0);
  if (error != NO_ERROR || col1 == NULL)
    {
      return ER_FAILED;
    }
  error = set_get_setobj (collection2, &col2, 0);
  if (error != NO_ERROR || col2 == NULL)
    {
      return ER_FAILED;
    }

#if !defined(SERVER_MODE)
  pin1 = ws_pin (collection1->owner, 1);
  pin2 = ws_pin (collection2->owner, 1);
#endif

  /* build result in the correct domain */
  *result = set_create_with_domain (domain, 0);
  if (!*result)
    {
      error = er_errid ();
      goto error_exit;
    }

  /* At this point, we know we have two sorted collections.
   * and a result of the correct domain.
   */

  error = (*op) (col1, col2, (*result)->set);

error_exit:
#if !defined(SERVER_MODE)
  (void) ws_pin (collection1->owner, pin1);
  (void) ws_pin (collection2->owner, pin2);
#endif

  /* check to see if we had to coerce (copy) the input argumanets.
   * If so, free them.
   */
  if (col1 != collection1->set)
    {
      setobj_free (col1);
    }
  if (col2 != collection2->set)
    {
      setobj_free (col2);
    }
  if (error < 0 && *result)
    {
      set_free (*result);
      *result = NULL;
    }
  return error;
}

/*
 * set_difference() -
 *      return: error code
 *  collection1(in) : input collections
 *  collection2(in) : input collections
 *  result(in) : output set or multiset
 *  domain(in) : desired result domain
 *  op(in) :
 *
 *  Note :
 *      takes the set difference of two collections
 *      If either argument is a sequence, it is
 *      first corced to the result domain, which must be
 *      a multiset or set.
 *
 */

int
set_difference (DB_COLLECTION * collection1, DB_COLLECTION * collection2, DB_COLLECTION ** result, DB_DOMAIN * domain)
{
  return set_op (collection1, collection2, result, domain, setobj_difference);
}

/*
 * set_union() -
 *      return: error code
 *  collection1(in) : input collections
 *  collection2(in) : input collections
 *  result(in) : output set or multiset
 *  domain(in) : desired result domain
 *  op(in) :
 *
 *  Note :
 *      takes the set difference of two collections
 *      If either argument is a sequence, it is
 *      first corced to the result domain, which must be
 *      a multiset or set.
 *
 */

int
set_union (DB_COLLECTION * collection1, DB_COLLECTION * collection2, DB_COLLECTION ** result, DB_DOMAIN * domain)
{
  return set_op (collection1, collection2, result, domain, setobj_union);
}

/*
 * set_intersection() -
 *      return: error code
 *  collection1(in) : input collections
 *  collection2(in) : input collections
 *  result(in) : output set or multiset
 *  domain(in) : desired result domain
 *  op(in) :
 *
 *  Note :
 *      takes the set difference of two collections
 *      If either argument is a sequence, it is
 *      first corced to the result domain, which must be
 *      a multiset or set.
 *
 */

int
set_intersection (DB_COLLECTION * collection1, DB_COLLECTION * collection2, DB_COLLECTION ** result, DB_DOMAIN * domain)
{
  return set_op (collection1, collection2, result, domain, setobj_intersection);
}
#endif

/*
 * set_make_collection() -
 *      return: none
 *  value(in) :
 *  col(in) :
 *
 */

void
set_make_collection (DB_VALUE * value, DB_COLLECTION * col)
{
  if (value && col)
    {
      value->domain.general_info.type = setobj_type (col->set);
      value->data.set = col;
      value->domain.general_info.is_null = 0;
    }
}

#if defined(ENABLE_UNUSED_FUNCTION)

/* SET ITERATORS */
/*
 * This is something new, added initially only to support some of the
 * migration utilities but can be generalized to be used in more places.
 * I've been wanting to do something like this for some time.
 * This allows you to iterate over the elements of a set, quickly, without
 * worring about the physical representation of the set.
 * This should be the preferred way to perform iterations, internally at
 * least and possibly through the API as well.
 *
 * This isn't general enough yet, in particular it needs to be able to
 * understand multiple iterator state on the set and allow the
 * iteration state to become invalid if the set changes.
 *
 */

/*
 * make_iterator() -
 *      return: SET_ITERATOR *
 *
 */

static SET_ITERATOR *
make_iterator (void)
{
  SET_ITERATOR *it;

  it = (SET_ITERATOR *) malloc (sizeof (SET_ITERATOR));
  if (it != NULL)
    {
      it->next = NULL;
      it->set = NULL;
      it->position = 0;
      it->value = NULL;
    }
  return it;
}

/*
 * free_iterator() -
 *      return: none
 *  it(in) :
 *
 */

static void
free_iterator (SET_ITERATOR * it)
{
  free_and_init (it);
}

/*
 * set_iterate() -
 *      return: SET_ITERATOR *
 *  set(in) :
 *
 */

SET_ITERATOR *
set_iterate (DB_COLLECTION * set)
{
  SET_ITERATOR *it;
  int error = NO_ERROR;

  /* pain in the ass, can't iterate on attached objects yet until we figure
     out how to pin them reliably, and more importantly, how to release
     the pin !
   */
  if (set == NULL || set->owner != NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      return NULL;
    }

  it = make_iterator ();
  if (it == NULL)
    {
      return it;
    }
  it->ref = set;
  error = set_get_setobj (set, &it->set, 1);
  if (error != NO_ERROR || it->set == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      free_iterator (it);
      return NULL;
    }

  if (it->set->size != 0)
    {
      it->value = INDEX (it->set, 0);
    }
  return it;
}

/*
 * set_iterator_free() -
 *      return: none
 *  it(in) :
 *
 */

void
set_iterator_free (SET_ITERATOR * it)
{
  free_iterator (it);
}

/*
 * set_iterator_value() -
 *      return: DB_VALUE *
 *  it(in) :
 *
 */

DB_VALUE *
set_iterator_value (SET_ITERATOR * it)
{
  if (it != NULL)
    {
      return it->value;
    }
  else
    {
      return NULL;
    }
}

/*
 * set_iterator_finished() -
 *      return: int
 *  it(in) :
 *
 */

int
set_iterator_finished (SET_ITERATOR * it)
{
  return (it == NULL) || (it->value == NULL);
}

/*
 * set_iterator_next() -
 *      return: int
 *  it(in) :
 *
 */

int
set_iterator_next (SET_ITERATOR * it)
{
  if (it == NULL || it->value == NULL)
    {
      return 0;
    }

  it->position++;
  if (it->position < setobj_size (it->set))
    {
      it->value = INDEX (it->set, it->position);
    }
  else
    {
      it->value = NULL;
    }

  return (it->value != NULL);
}

/*
 * setobj_create_with_domain() - This creates a set of the appropriate type
 *                               and assigns it an initial domain
 *      return: new set, multiset, or sequence
 *  domain(in) : desired domain
 *  initial_size(in) : starting size for sequences, ignored otherwise
 *
 * Note :
 *      If NULL is returned, an error condition will have been set
 */

COL *
setobj_create_with_domain (TP_DOMAIN * domain, int initial_size)
{
  COL *new_ = NULL;

  assert (domain != NULL);
  assert (domain->is_cached);

  new_ = setobj_create (TP_DOMAIN_TYPE (domain), initial_size);
  if (new_ != NULL)
    {
      new_->domain = domain;
    }

  return (new_);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * setobj_create() - create a collectio object, or sets an error
 *      return: COL *
 *  collection_type(in) : what kind of collection
 *  size(in) : initial size of the sequence
 *
 */

COL *
setobj_create (DB_TYPE collection_type, int size)
{
  if (!TP_IS_SET_TYPE (collection_type))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return NULL;
    }
  return col_new (size, collection_type);
}

/*
 * setobj_clear() - Remove all elements from a set
 *      return: none
 *  col(in) : collection object
 *
 */

void
setobj_clear (COL * col)
{
  int i;

  if (col == NULL)
    {
      return;
    }

  for (i = 0; i < col->size; i++)
    {
      (void) pr_clear_value (INDEX (col, i));
    }
  for (i = 0; i <= col->topblock; i++)
    {
      set_free_block (col->array[i]);
      col->array[i] = NULL;
    }
  col->topblock = -1;
  col->topblockcount = -1;
  col->size = 0;
}

/*
 * setobj_free() - Free a set object and all of its elements
 *      return: none
 *  col(in) : collection object
 *
 */

void
setobj_free (COL * col)
{
  register DB_COLLECTION *start, *r;

  if (col == NULL)
    {
      return;
    }
  setobj_clear (col);

  if (col->array != NULL)
    {
      free_and_init (col->array);
      col->array = NULL;
    }

  start = r = col->references;
  if (start)
    {
      do
        {
          r->set = NULL;
          r = r->ref_link;
        }
      while (r != start);
    }

  free_and_init (col);
}

/*
 * setobj_copy() - Copy a set object and all of its elements
 *      return: new set
 *  col(in) : collection object
 *
 */

COL *
setobj_copy (COL * col)
{
  COL *new_;
  int i;
  int error;

  if (col == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return (NULL);
    }

  new_ = col_new (col->size, col->coltype);
  if (new_ == NULL)
    {
      return (NULL);
    }

  /* must retain this ! */
  new_->may_have_temporary_oids = col->may_have_temporary_oids;

  if (new_ != NULL)
    {
      error = col_expand (new_, col->size - 1);
      if (error < 0)
        {
          setobj_free (new_);
          new_ = NULL;
        }
    }

  if (new_ == NULL)
    {
      return new_;
    }

  for (i = 0; i < col->size && new_; i++)
    {
      error = pr_clone_value (INDEX (col, i), INDEX (new_, i));
      if (error < 0)
        {
          setobj_free (new_);
          new_ = NULL;
        }
    }

#if !defined (NDEBUG)
#if 0                           /* TODO - */
  if (new_ != NULL && new_->domain != NULL)
    {
      assert (new_->domain->next == NULL);
    }
#endif
#endif

  return (new_);
}


#if !defined(SERVER_MODE)
#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * setobj_find_temporary_oids() -
 *      return: error
 *  col(in) : collection
 *  oidset(in) : oidset to populate
 *
 *  Note :
 *    This maps over the collection looking for temporary OIDs and when
 *    found, adds them to the oidset.  This is a recursive operation that
 *    will handle nested collections.
 *    It is intended to be used by tf_find_temporary_oids and perhaps others,
 *    it is by definition a client only function.
 */

int
setobj_find_temporary_oids (SETOBJ * col, LC_OIDSET * oidset)
{
  int error;
  DB_VALUE *val;
  DB_TYPE type;
  DB_OBJECT *obj;
  SETREF *ref;
  int i, tempoids;

  error = NO_ERROR;

  /* Can this collection have any temporary OIDs in it ? */
  if (col && col->may_have_temporary_oids)
    {

      tempoids = 0;
      for (i = 0; i < col->size && !error; i++)
        {
          val = INDEX (col, i);
          type = DB_VALUE_TYPE (val);
          if (type == DB_TYPE_OBJECT)
            {
              obj = DB_GET_OBJECT (val);
              if (obj != NULL && OBJECT_HAS_TEMP_OID (obj))
                {
                  tempoids++;
                  if (locator_add_oidset_object (oidset, obj) == NULL)
                    {
                      error = er_errid ();
                    }
                }
            }
          else if (TP_IS_SET_TYPE (type))
            {
              /* its a nested set, recurse, since we must already be pinned
               * don't have to worry about pinning the nested set.
               */
              ref = DB_GET_SET (val);
              if (ref && ref->set != NULL)
                {
                  error = setobj_find_temporary_oids (ref->set, oidset);
                  if (ref->set->may_have_temporary_oids)
                    {
                      tempoids++;
                    }
                }
            }
        }

      /* if we made it through a traversal and didn't encounter any temporary
       * oids, then we can turn this flag off.
       */
      if (tempoids == 0)
        {
          col->may_have_temporary_oids = 0;
        }
    }

  return error;
}
#endif /* ENABLE_UNUSED_FUNCTION */
#endif /* !SERVER_MODE */

/* SET DOMAIN MAINTENANCE */
/*
 * setobj_check_domain() -
 *      return: error code
 *  col(in) : collection object
 *  domain(in) : domain structure
 *  Note:
 *    This is used to see if the contents of a set conforms to a particular
 *    domain specification.
 *    It calls tp_domain_check to do the work, basically this only
 *    provides the infrastructure to map over the set elements.
 *
 *    Note that we use "exact" matches on the set elements here rather than
 *    tolerance matches.  This is because the "setval" function used to
 *    assign the element values does not have enough information to perform
 *    deferred coercion (for the CHAR) types and we will have to perform
 *    coercion to make sure that the set gets re-formatted exactly like
 *    its domain wants.  This isn't especially efficient but optimizing
 *    set copying is going to require a rather long look at the life of
 *    set literals.
 *
 *    This is only called from tp_value_select which is used to check
 *    to see if coercion is necessary.  Think about cleaning up the
 *    various coercion checking logic we have scattered about.
 *
 */

TP_DOMAIN_STATUS
setobj_check_domain (COL * col, TP_DOMAIN * domain)
{
  DB_VALUE *val = NULL;
  TP_DOMAIN_STATUS status;
  int i;

  status = DOMAIN_COMPATIBLE;

  /* if any of these conditions are true, the domains are compatible */
  /* -bk, I don't understand the above comment. Presumably "false" is
   * meant, since the code will return compatible if any of the tests
   * is false. However, that appears to be broken as well, since
   * a NULL domain by no means guarantees compatibility.
   * Perhaps this should start with setobj_get_domain?
   * Anyway, I'm not so sure of this to change the logic now,
   * and am leaving the test as I found it.
   */
  if (col->domain != NULL && col->domain != domain)
    {
      /* couldn't do it simply, have to look at each element */
      for (i = 0; i < col->size && status == DOMAIN_COMPATIBLE; i++)
        {
          val = INDEX (col, i);
          assert (val != NULL);
#if 1                           /* remove nested set */
          assert (!TP_IS_SET_TYPE (DB_VALUE_TYPE (val)));
#endif
          status = tp_domain_check (domain->setdomain, val, TP_EXACT_MATCH);
        }
    }

  return status;
}

/*
 * setobj_get_domain() - This is used to get a description of a set's domain
 *      return: copy of the set's domain
 *  set(in) : collection object
 *  Note :
 *    If NULL is returned, it indicates a memory allocation failure
 *    or an access failure on the owning object.
 */

TP_DOMAIN *
setobj_get_domain (COL * set)
{
  if (set->domain != NULL)
    {
      assert (set->domain->is_cached);
      assert (set->domain->next == NULL);

      return set->domain;
    }
  /* Set without an owner and without a domain, this isn't
   * supposed to happen any more.  Slam in one of the built-in domains.
   */
  switch (set->coltype)
    {
    case DB_TYPE_SEQUENCE:
      set->domain = &tp_Sequence_domain;
      break;
    default:
      /* what is it? must be a structure error */
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SET_INVALID_DOMAIN, 1, "NULL set domain");
      break;
    }

#if !defined (NDEBUG)
  if (set->domain != NULL)
    {
      assert (set->domain->is_cached);
      assert (set->domain->next == NULL);
    }
#endif

  return set->domain;
}

/*
 * swizzle_value() - This converts a value containing an OID into one
 *                   containing a DB_OBJECT*
 *      return: none
 *  val(in) : value to convert
 *  input(in) :
 *  Note :
 *    This process is commonly known in the literature as "swizzling" a
 *    database pointer.
 *    Note that this does not change the "domain" of a value.  DB_TYPE_OBJECT
 *    and DB_TYPE_OID are used to tag two different physical representations
 *    of the same domain.  DB_TYPE_OBJECT is a domain type, DB_TYPE_OID is
 *    not, though it carries a lot of the same type handler baggage.
 *
 *    With the set_ module, this is called as objects are put into the set,
 *    and as objects are extracted from the set.
 *    Note that the lowest level set builders bypass swizzling so sets
 *    can always have DB_TYPE_OID elements in them if they haven't
 *    been touched by set_get_element() or some other function that
 *    maps over the set elements.
 */

void
swizzle_value (DB_VALUE * val, UNUSED_ARG int input)
{
  OID *oid;


  if (DB_VALUE_TYPE (val) != DB_TYPE_OID)
    {
      return;
    }

  /* We always convert incoming "NULL oid" references into a NULL value.
   * This makes set elements consistent with OIDs assigned to attribute
   * values and is very important for proper comparison of VOBJ sets.
   */
  oid = db_get_oid (val);
  if (oid == NULL)
    {
      return;
    }

  if ((oid)->pageid == NULL_PAGEID)
    {
      db_value_put_null (val);
    }
  else
    {
#if !defined(SERVER_MODE)
      /* If we're on the client, and this is a value being extracted,
       * convert it to a DB_OBJECT* reference.
       * We could do this on input too in order to get better type
       * checking.  Since this only happens in the internal functions
       * for building VOBJ sequences and those sequences have the wildcard
       * domain, we don't really need to swizzle the OIDs or do type checking.
       */
      if (!db_on_server && !input)
        {
          DB_OBJECT *mop;
          mop = ws_mop (oid, NULL);
          db_value_domain_init (val, DB_TYPE_OBJECT, DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
          DB_MAKE_OBJECT (val, mop);
        }
#endif /* !SERVER_MODE */
    }
}

/*
 * assign_set_value() -
 *      return: error code
 *  set(in) : set containing element
 *  src(in) : source value
 *  dest(in) : destination value
 *  implicit_coercion(in) :
 *
 *  Note :
 *    This is the main function the handles the assignment of values
 *    into sets.  All element assignment function will end up
 *    here.  This encapsulates the domain checking and coercion rules.
 *    Note that for set assignment, setobj_ismember has already performed
 *    the coercion yet we do it again here.  It might be nice
 *    to restructure this a bit to avoid the extra coercion.
 */

static int
assign_set_value (COL * set, DB_VALUE * src, DB_VALUE * dest, bool implicit_coercion)
{
  int error = NO_ERROR;
  TP_DOMAIN_STATUS status;
  TP_DOMAIN *domain;
  DB_VALUE temp;

  assert (src != NULL);
  assert (dest != NULL);
  assert (dest->need_clear == false);

  /* On the client, always swizzle incoming OID's so we can properly check
   * their domains.  On the server, we'll have to trust it until
   * tp_domain_check understands how to do domain verification using
   * just OIDs.
   * Do this into a temp buffer so we can leave the input value constant.
   * This will only change for OID-OBJECT conversion which
   * don't have to be freed.
   */
  temp = *src;
  swizzle_value (&temp, 1);

  domain = setobj_get_domain (set);

  /* quickly handle the wildcard cases */
  if (domain == NULL || domain->setdomain == NULL)
    {
      error = pr_clone_value (&temp, dest);
    }
  else
    {
      /* Request an exact match so we know if there is any coercion to be
       * performed.
       * This is especially important for set elements since we use "setval" to
       * assign the value and this doesn't have enough information to perform
       * deferred coercion if that's what we need.
       */
      status = tp_domain_check (domain->setdomain, &temp, TP_EXACT_MATCH);
      if (status == DOMAIN_COMPATIBLE)
        {
          error = pr_clone_value (&temp, dest);
        }
      else
        {
          /* try coercion */
          if (implicit_coercion)
            {
              status = tp_value_coerce (&temp, dest, domain->setdomain);
            }
          else
            {
              status = tp_value_cast (&temp, dest, domain->setdomain);
            }

          if (status != DOMAIN_COMPATIBLE)
            {
              /* handle all non-compatible states as a domain failure */
              error = ER_SET_DOMAIN_CONFLICT;
              er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
            }
        }
    }
  return (error);
}

/* INTERNAL SET UTILITIES */

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * check_set_object() -
 *      return: non-zero if the element was deleted
 *  var(in) : value container to check
 *  removed_ptr(in) :
 *  Note :
 *    This checks a set element value to see if it contains a reference
 *    to an object that has been deleted.  If the object has been deleted,
 *    the set element is changed to NULL.
 *    This is rather ugly and expensive, not sure it should always be the
 *    responsibility of sets to detect these references.
 *
 */

static int
check_set_object (DB_VALUE * var, int *removed_ptr)
{
  int error = NO_ERROR;
  int removed = 0;
#if !defined(SERVER_MODE)
  MOP mop;
  int status;

  if (db_on_server)
    {
      goto end;
    }
  swizzle_value (var, 0);

  if (DB_VALUE_TYPE (var) != DB_TYPE_OBJECT)
    {
      goto end;
    }
  mop = DB_GET_OBJECT (var);
  if (mop == NULL)
    {
      goto end;
    }

  if (!mop->deleted)
    {
      goto end;
    }

  removed = 1;
  DB_MAKE_NULL (var);

end:
#endif /* !SERVER_MODE */

  if (error == NO_ERROR && removed_ptr != NULL)
    {
      *removed_ptr = removed;
    }

  return (error);
}

/*
 * setobj_filter()
 *      return: error code
 *  col(in) : set to filter
 *  filter(in) : non-zero if elements are to be filtered
 *  cardptr(out) : cardinality variable pointer (returned)
 *
 *  Note :
 *    This function can be used for two purposes.
 *    If the filter flag is zero, it calculates the cardinality of the
 *    set by counting the number of non-NULL elements.
 *    If the filter flag is non-zero it calculates the cardinality AND
 *    filters out pointers to deleted objects.
 */

int
setobj_filter (COL * col, int filter, int *cardptr)
{
  int error = NO_ERROR;
  DB_VALUE *var;
  int i, card, removed = 0;

  card = 0;

  /*  We need to loop backwards here since we may be deleting values
   *  and everything after that value will be shift down one index
   */
  for (i = col->size - 1; i >= 0; i--)
    {
      var = INDEX (col, i);
      if (filter)
        {
          swizzle_value (var, 0);
        }
      if (filter && DB_VALUE_TYPE (var) == DB_TYPE_OBJECT)
        {
          error = check_set_object (var, &removed);
          if (error < 0)
            {
              break;
            }
          if (!removed)
            {
              card++;
            }
        }
      else if (!db_value_is_null (var))
        {
          card++;
        }
    }
  if (cardptr == NULL)
    {
      return error;
    }

  if (error < 0)
    {
      *cardptr = 0;
    }
  else
    {
      *cardptr = card;
    }
  return (error);
}
#endif

/* SET INFORMATION FUNCTIONS */

/*
 * setobj_size() - This returns the number of elements in a set
 *      return: number of elements in the set
 *  col(in) : collection object
 *  Note :
 *      For sets and multisets, this is the length of the element list and
 *      does not include checking for NULL elements or filtering out
 *      deleted references.
 *      For sequences, this is the length of the used portion of the array.
 *      This should not be used as the iteration count for sets and multi
 *      sets.  This should be used for the iteration counts of sequences since
 *      elements can legally be NULL.
 */

int
setobj_size (COL * col)
{
  if (col == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return (ER_OBJ_INVALID_ARGUMENTS);
    }

  return col->size;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * setobj_cardinality() -  This returns the number of non-null elements in a set
 *      return: number of non-null elements
 *  col(in) : collection object
 *
 *  Note :
 *    This should be used as the length for iterating over sets and multi sets.
 *    This should NOT be used for iterating over sequences.
 */

int
setobj_cardinality (COL * col)
{
  int error;
  int card;

  if (col == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return (ER_OBJ_INVALID_ARGUMENTS);
    }

  error = setobj_filter (col, 0, &card);
  /* should be passing the error back up ! */

  return (card);
}

/*
 * setobj_isempty() - Returns the number of elements, including null elements,
 *                    in the collection
 *      return: non-zero if the set is empty
 *  col(in) : collection object
 *
 */

bool
setobj_isempty (COL * col)
{
  if (setobj_size (col) == 0)
    {
      return true;
    }
  else
    {
      return false;
    }
}

/*
 * setobj_ismember() - Check to see if a value is in a set
 *      return: non-zero of the value was found in the set
 *  col(in) : set object
 *  proposed_value(in) : value to search for
 *  check_null(in) : non-zero if we want to perform true comparisons on NULL
 *
 *  Note :
 *      Checks up front to see if the value is within the domain of the set
 *      before searching.
 *
 */

bool
setobj_ismember (COL * col, DB_VALUE * proposed_value, int check_null)
{
  DB_VALUE coerced_value, *value;
  DB_DOMAIN *domain;
  TP_DOMAIN_STATUS status;
  long found, coerce;

  /* guard against bad arguments. No error return possible */
  if (!col || !proposed_value)
    {
      return false;
    }

  if (DB_IS_NULL (proposed_value))
    {
      /* handle NULL quickly */
      if (!check_null)
        {
          return false;
        }
      else
        {
          return col_has_null (col);
        }
    }

  found = false;
  value = proposed_value;
  domain = setobj_get_domain (col);

  /* validate the value against the domain of the col, coerce if necessary */
  if (domain && domain->setdomain != NULL)
    {
#if 1                           /* remove nested set */
      assert (!TP_IS_SET_TYPE (DB_VALUE_TYPE (value)));
#endif
      status = tp_domain_check (domain->setdomain, value, TP_EXACT_MATCH);
      if (status != DOMAIN_COMPATIBLE)
        {
          value = &coerced_value;
          status = tp_value_coerce (proposed_value, value, domain->setdomain);
          if (status != DOMAIN_COMPATIBLE)
            {
              return false;
            }
        }
    }


  if (domain && domain->setdomain && !domain->setdomain->next)
    {
      /* exactly one domain, no need to coerce */
      coerce = 0;
    }
  else
    {
      /* unknown or multiple domains */
      coerce = 1;
    }

  (void) col_find (col, &found, value, coerce);

  if (found > 0)
    {
      return true;
    }

  return false;
}
#endif

/*
 * setobj_compare_order() - Compare the values of the sets to deteremine
 *                          their order. If total order is asked for,
 *                          DB_UNK will not be returned
 *      return: DB_EQ, DB_LT, DB_GT, DB_UNK
 *  set1(in) : set object
 *  set2(in) : set object
 *  do_coercion(in) :
 *  total_order(in) :
 *
 */

int
setobj_compare_order (COL * set1, COL * set2, int do_coercion, int total_order)
{
  int i, rc;

  if (set1->size < set2->size)
    {
      return DB_LT;
    }
  else if (set1->size > set2->size)
    {
      return DB_GT;
    }

  if (set1 == set2)
    {
      /* optimize comparison to self */
      if (total_order)
        {
          return DB_EQ;
        }
      if (col_has_null (set1))
        {
          return DB_UNK;
        }
      return DB_EQ;
    }

  /* same size, compare elements until order is determined */
  for (i = 0; i < set1->size; i++)
    {
      rc = tp_value_compare (INDEX (set1, i), INDEX (set2, i), do_coercion, total_order, NULL);
      if (rc != DB_EQ)
        {
          return rc;
        }
    }

  return DB_EQ;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * setobj_difference() -
 *      return: result which is the set difference (set1-set2)
 *  set1(in) : set object
 *  set2(in) : set objeccollection objectsult(in) : result set object
 *
 */

int
setobj_difference (COL * set1, COL * set2, COL * result)
{
  int rc;
  int error = NO_ERROR;
  int index1, index2;
  DB_VALUE *val1, *val2;

  /* compare elements in ascending order */
  index1 = 0;
  index2 = 0;
  while (index1 < set1->size && index2 < set2->size && !(error < NO_ERROR))
    {
      val1 = INDEX (set1, index1);
      val2 = INDEX (set2, index2);
      if (DB_IS_NULL (val1) || DB_IS_NULL (val2))
        {
          error = setobj_add_element (result, val1);
          index1++;
        }
      else
        {
          rc = tp_value_compare (val1, val2, 1, 0, NULL);

          switch (rc)
            {
            case DB_EQ:        /* element appears in both sets */
              index1++;         /* should NOT be in result */
              index2++;
              break;
            case DB_GT:        /* element of second set not in first */
              index2++;         /* should NOT be in result */
              break;
            case DB_LT:        /* element of first set not in second */
              index1++;         /* SHOULD be in result */
              error = setobj_add_element (result, val1);
              break;
            case DB_UNK:       /* a NULL encountered
                                 * (also unexpected results) */
            default:           /* NULL's are not equal to nulls.
                                 * so {NULL} - {NULL} is = {NULL}
                                 * ie, the first value result SHOULD
                                 * be added to the result.
                                 */
              /* At least one of these must be a collection with an
               * embedded NULL, we need to increment to the next
               * pair of values, but must check again to see
               * which index to increase for total ordering. */
              rc = tp_value_compare (val1, val2, 1, 1, NULL);
              if (rc == DB_GT)
                {
                  index2++;
                }
              else
                {
                  index1++;
                  error = setobj_add_element (result, val1);
                }
            }
        }
    }
  /* we have exhausted set1 or set2. Append the remains of set1 if any */
  while (index1 < set1->size && !(error < NO_ERROR))
    {
      val1 = INDEX (set1, index1);
      error = setobj_add_element (result, val1);
      index1++;
    }
  return error;
}

/*
 * setobj_union()
 *      return: result which is the set union (set1 + set2)
 *  set1(in) : set object
 *  set2(in) : set object
 *  result(in) : result set object
 *
 *  Note :
 *      Work through each set in ascending order. This
 *      allows the result set to be built in ascending order,
 *      without causing any insertions, which are more expensive
 *      than appending.
 */

int
setobj_union (COL * set1, COL * set2, COL * result)
{
  int error = NO_ERROR;
  int index1, index2;
  DB_VALUE *val1, *val2;

  assert (result->coltype == DB_TYPE_SEQUENCE);

  /* append sequences */
  for (index1 = 0; index1 < set1->size && !(error < NO_ERROR); index1++)
    {
      val1 = INDEX (set1, index1);
      error = setobj_add_element (result, val1);
    }
  for (index2 = 0; index2 < set2->size && !(error < NO_ERROR); index2++)
    {
      val2 = INDEX (set2, index2);
      error = setobj_add_element (result, val2);
    }

  return error;
}

/*
 * setobj_intersection() -
 *      return: result which is the set intersection (set1*set2)
 *  set1(in) : set object
 *  set2(in) : set object
 *  result(in) : result set object
 *
 */
int
setobj_intersection (COL * set1, COL * set2, COL * result)
{
  int rc;
  int error = NO_ERROR;
  int index1, index2;
  DB_VALUE *val1, *val2;

  /* compare elements in ascending order */
  index1 = 0;
  index2 = 0;
  while (index1 < set1->size && index2 < set2->size && !(error < NO_ERROR))
    {
      val1 = INDEX (set1, index1);
      val2 = INDEX (set2, index2);
      if (DB_IS_NULL (val1) || DB_IS_NULL (val2))
        {
          /* NULL never equals NULL, so {NULL} * {NULL} = {} */
          /* NULLs are at the end, so once we have hit one
           * from either collection, we will never get another
           * equality match, and can bail out.
           */
          break;
        }
      else
        {
          rc = tp_value_compare (val1, val2, 1, 0, NULL);

          switch (rc)
            {
            case DB_EQ:        /* element appears in both sets */
              index1++;         /* SHOULD be in result */
              index2++;
              error = setobj_add_element (result, val1);
              break;
            case DB_GT:        /* element of second set not in first */
              index2++;         /* should NOT be in result */
              break;
            case DB_LT:        /* element of first set not in second */
              index1++;         /* should NOT be in result */
              break;
            case DB_UNK:       /* a NULL encountered
                                 * (also unexpected results) */
            default:           /* NULL's are not equal to nulls.
                                 * so {NULL} * {NULL} is = {}
                                 * ie, the value should NOT
                                 * be added to the result.
                                 */
              /* At least one of these must be a collection with an
               * embedded NULL, we need to increment to the next
               * pair of values, but must check again to see
               * which side to increase for total ordering. */
              rc = tp_value_compare (val1, val2, 1, 1, NULL);
              if (rc == DB_GT)
                {
                  index2++;
                }
              else
                {
                  index1++;
                }
              break;
            }
        }
    }
  return error;
}

/*
 * setobj_issome()
 *      return: 1 if value compares successfully using op to some element
 *              in set, -1 if unknown, othersize 0
 *  value(in) : a value
 *  set(in) : set object
 *  op(in) : op code
 *  do_coercion(in) :
 *
 *  Note :
 *      Compares value to the members of set using op.
 *      If any member compares favorably, returns 1
 */

int
setobj_issome (DB_VALUE * value, COL * set, PT_OP_TYPE op, int do_coercion)
{
  int status;
  int i;
  int has_null = 0;

  assert (op == PT_IS_IN);

  if (DB_IS_NULL (value))
    {
      return -1;
    }

  for (i = 0; i < set->size; i++)
    {
      status = tp_value_compare (value, INDEX (set, i), do_coercion, 0, NULL);
      if (status == DB_UNK)
        {
          has_null = 1;
          continue;
        }

      switch (op)
        {
        case PT_IS_IN:
          if (status == DB_EQ)
            {
              return 1;
            }
          break;
        default:
          assert (false);
          break;
        }
    }

  if (col_has_null (set))
    {
      return -1;
    }
  else
    {
      return 0;
    }
}

/*
 * setobj_convert_oids_to_objects() - This will convert all OID and VOBJ
 *                                    elements to OBJECT elements
 *      return: error code
 *  col(in) : collection object
 *
 */

int
setobj_convert_oids_to_objects (UNUSED_ARG COL * col)
{
  int error = NO_ERROR;
#if !defined(SERVER_MODE)
  DB_VALUE *var;
  int i;
  DB_TYPE typ;

  if (col == NULL)
    {
      return NO_ERROR;
    }

  for (i = 0; i < col->size && !(error < NO_ERROR); i++)
    {
      var = INDEX (col, i);
      typ = DB_VALUE_DOMAIN_TYPE (var);
      switch (typ)
        {
        case DB_TYPE_OID:
          swizzle_value (var, 0);
          break;
        case DB_TYPE_SEQUENCE:
          error = set_convert_oids_to_objects (DB_GET_SET (var));
          break;
        default:
          break;
        }
    }
#endif

  return (error);
}
#endif

/* SET ELEMENT ACCESS FUNCTIONS */

/*
 * setobj_get_element_ptr() - This is used in controlled conditions to
 *                            get a direct pointer to a set element value.
 *      return: error code
 *  col(in) : collection object
 *  index(in) : element index
 *  result(in) : pointer to pointer to value (returned)
 *
 */

int
setobj_get_element_ptr (COL * col, int index, DB_VALUE ** result)
{
  int error;

  CHECKNULL (col);
  CHECKNULL (result);

  if (index >= 0 && index < col->size)
    {
      *result = INDEX (col, index);
      error = NO_ERROR;
    }
  else
    {
      *result = NULL;
      error = ER_SET_INVALID_INDEX;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, index);
    }

  return error;
}

/*
 * setobj_get_element() - Get the value of a set element.
 *                        An error code is returned if the index is beyond
 *                        the length of the set
 *      return:  error code
 *  set(in) : set object
 *  index(in) : element index
 *  value(in) : return value container
 *
 */

int
setobj_get_element (COL * set, int index, DB_VALUE * value)
{
  int error = NO_ERROR;
  DB_VALUE *element;

  /* should this be pr_clear_value instead? */
  DB_MAKE_NULL (value);

  error = setobj_get_element_ptr (set, index, &element);

  if (element)
    {
      swizzle_value (element, 0);
      error = pr_clone_value (element, value);
      /* kludge, should be part of pr_ level */
      SET_FIX_VALUE (value);
    }

  return error;
}

/*
 * setobj_add_element() - Add an element to a set
 *      return: error code
 *  col(in) : collection object
 *  value(in) : value to add
 *
 *  Note :
 *      For basic sets, the set is first checked to see if the element
 *      already exists.  For basic & multi sets, the element is added
 *      to the head of the list.  For sequences, the element is appended
 *      to the end of the sequence.
 */

int
setobj_add_element (COL * col, DB_VALUE * value)
{
  DB_VALUE temp;
  int error = NO_ERROR;

  DB_MAKE_NULL (&temp);
  CHECKNULL (col);
  CHECKNULL (value);

#if 1                           /* remove nested set */
  assert (!TP_IS_SET_TYPE (DB_VALUE_TYPE (value)));
#endif

  error = assign_set_value (col, value, &temp, IMPLICIT);
  if (error != NO_ERROR)
    {
      return error;
    }
  /* assign_set_value has done the necessary cloning */
  error = col_add (col, &temp);

  if (error != NO_ERROR)
    {
      (void) pr_clear_value (&temp);
    }

  return error;
}

/*
 * setobj_put_element() - This is used to overwrite an element of a sequence
 *                        with another value
 *      return:  error code
 *  col(in) : collection object
 *  index(in) : element index
 *  value(in) : value to put in sequence
 *
 */

int
setobj_put_element (COL * col, int index, DB_VALUE * value)
{
  int error = NO_ERROR;
  DB_VALUE temp;

  DB_MAKE_NULL (&temp);
  CHECKNULL (col);
  CHECKNULL (value);

  if (index < 0)
    {
      error = ER_SET_INVALID_INDEX;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_SET_INVALID_INDEX, 1, index);
      return error;
    }

  error = assign_set_value (col, value, &temp, IMPLICIT);
  if (error != NO_ERROR)
    {
      (void) pr_clear_value (&temp);

      return error;
    }

  if (index < col->size)
    {
      /* clear existing value */
      (void) pr_clear_value (INDEX (col, index));
    }

  /* temp contains the new value, just blast it in */
  col_put (col, index, &temp);

  return (error);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * setobj_drop_element() - This will remove values from a set or sequence
 *      return: error code
 *  col(in) : collection object
 *  value(in) : value to drop
 *  match_nulls(in) : drop NULL values?
 *
 *  Note :
 *    For basic & multisets, the matching elements are removed from the list.
 *    For sequences the elements are made NULL.
 *    If match_nulls is true and we have a NULL db_value, then we'll
 *    drop all of the NULL values from the set, multi-set, or sequence.
 *    To be more consistent with the SQL/X language, this has been
 *    changed so that it will only drop the FIRST occurrence of
 *    the value.  This is mostly an issue for multisets.
 *
 */

int
setobj_drop_element (COL * col, DB_VALUE * value, bool match_nulls)
{
  int error = NO_ERROR;

  CHECKNULL (col);
  CHECKNULL (value);

  /*  Minor enhancement: if the value is NULL and we aren't matching NULLs,
   *  col_drop() won't drop anything, so don't call it.
   */

  if (DB_IS_NULL (value))
    {
      if (match_nulls)
        {
          error = col_drop_nulls (col);
        }
    }
  else
    {
      error = col_drop (col, value);
    }
  return (error);
}
#endif

/*
 * setobj_drop_seq_element() - This will completely remove an element
 *                             from a sequence and shift the subsequent
 *                             elements up to fill in the empty space
 *      return: error code
 *  col(in) : collection object
 *  index(in) : element index
 *
 */

int
setobj_drop_seq_element (COL * col, int index)
{
  int error = NO_ERROR;

  CHECKNULL (col);

  if (index < 0 || index >= col->size)
    {
      error = ER_SET_INVALID_INDEX;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, index);
    }
  else
    {
      error = col_delete (col, index);
    }
  return (error);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * setobj_find_seq_element() -
 *      return: the index of the element or error code if not found
 *  col(in) : sequence descriptor
 *  value(in) : value to search for
 *  index(in) : starting index (zero if starting from the beginning)
 *
 *  Note :
 *    This can be used to sequentially search for elements in a
 *    sequence matching a particular value.  Do search for duplicate elements
 *    call this function multiple times and set the "index" parameter to
 *    1+ the number returned by the previous call to this function.
 *    Errors are defined to be negative integers to you can check to see
 *    if this function failed to find a value by testing for a negative
 *    return value.  The specific error to test for is
 *    ER_SET_ELEMENT_NOT_FOUND.  A error code other than this one may indicate
 *    an authorization failure.
 */

int
setobj_find_seq_element (COL * col, DB_VALUE * value, int index)
{
  int result;
  DB_VALUE *valp;
  long found;

  if (index >= col->size || index < 0)
    {
      result = ER_SEQ_OUT_OF_BOUNDS;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_SEQ_OUT_OF_BOUNDS, 0);
    }
  else
    {
      for (found = -1; index < col->size && found == -1; index++)
        {
          valp = INDEX (col, index);
          if (tp_value_equal (valp, value, 1))
            {
              found = index;
            }
        }
      if (found == -1)
        {
          result = ER_SEQ_ELEMENT_NOT_FOUND;
          er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_SEQ_ELEMENT_NOT_FOUND, 0);
        }
      else
        {
          result = found;
        }
    }

  return result;
}

/*
 * setobj_coerce() - This coerces a set from one domain into another if possible
 *      return: coerced set
 *  col(in) : source set
 *  domain(in) : desired domain
 *  implicit_coercion(in) :
 *
 *  Note :
 *    If all of the elements can be coerced into a compatible
 *    domain, a new set is returned.  Otherwise NULL is returned.
 *    This could be made faster if necessary but not worth the trouble
 *    right now.
 */

COL *
setobj_coerce (COL * col, TP_DOMAIN * domain, bool implicit_coercion)
{
  COL *new_;
  DB_VALUE *val, temp;
  int error;
  int i;

  new_ = setobj_create_with_domain (domain, col->size);
  if (new_ == NULL)
    {
      return NULL;
    }

  /* first copy the values over in the order they appear,
   * coercing domains as we go.
   */
  for (i = 0; i < col->size && new_ != NULL; i++)
    {
      val = INDEX (col, i);

      DB_MAKE_NULL (&temp);
      error = assign_set_value (new_, val, &temp, implicit_coercion);
      if (error == NO_ERROR)
        {
          /* assign_set_value has done the necessary cloning */
          error = col_put (new_, new_->size, &temp);
        }
      if (error != NO_ERROR)
        {
          setobj_free (new_);
          new_ = NULL;
        }
    }

  return (new_);
}
#endif

/* DEBUGGING FUNCTIONS */

/*
 * setobj_print() - Print a description of the set.  For debugging only
 *      return: none
 *  fp(in) :
 *  col(in) :
 *
 */

void
setobj_print (FILE * fp, COL * col)
{
  int i;

  if (col == NULL)
    {
      return;
    }

  fprintf (fp, "{");
  for (i = 0; i < col->size; i++)
    {
      help_fprint_value (fp, INDEX (col, i));
      if (i < col->size - 1)
        {
          fprintf (fp, ", ");
        }
    }
  fprintf (fp, "}\n");
}

/* SET REFERENCE MAINTENANCE */

/*
 * setobj_type() - Returns the type of setobj is passed in
 *      return: DB_TYPE
 *  set(in) : set object
 *
 */

DB_TYPE
setobj_type (COL * set)
{
  if (set)
    {
      return set->coltype;
    }
  return DB_TYPE_NULL;
}

/*
 * setobj_domain() - Returns domain of setobj is passed in
 *      return: TP_DOMAIN
 *  set(in) : set object
 *
 */

TP_DOMAIN *
setobj_domain (COL * set)
{
  if (set)
    {
#if !defined (NDEBUG)
      if (set->domain != NULL)
        {
          assert (set->domain->is_cached);
#if 0                           /* TODO - */
          assert (set->domain->next == NULL);
#endif
        }
#endif

      return set->domain;
    }

  return NULL;
}

/*
 * setobj_put_domain() - sets domain in setobj structure
 *      return: none
 *  set(in) : set object
 *  domain(in) :
 *
 */

void
setobj_put_domain (COL * set, TP_DOMAIN * domain)
{
  assert (domain != NULL);
  assert (domain->is_cached);

  if (set)
    {
#if NDEBUG
      if (domain != NULL)
        {
          assert (domain->is_cached);
#if 0                           /* TODO - */
          assert (domain->next == NULL);
#endif
        }
#endif

      set->domain = domain;
    }
}

/*
 * setobj_put_value() -
 *      return: error code
 *  col(in) : collection
 *  index(in) :
 *  value(in) :
 *
 */

int
setobj_put_value (COL * col, int index, DB_VALUE * value)
{
  int error;

  error = col_put (col, index, value);

  /* if the value was added successfully,
   * make sure caller does not inadvertantly clear or use this
   * value container
   */
  if (error == NO_ERROR)
    {
      PRIM_SET_NULL (value);
    }

  return error;
}

/*
 * setobj_get_reference() - This constructs a set reference for a set object
 *      return: set reference structure
 *  set(in) : set object
 *
 *  Note :
 *    If references already exist, one is used, otherwise a new one
 *    is created.
 *
 *    The reference count IS INCREMENTED.  If this is a brand new reference
 *    structure, the reference count will be initialized to 1.
 */

DB_COLLECTION *
setobj_get_reference (COL * set)
{
  DB_COLLECTION *ref = NULL;

  if (set == NULL)
    {
      return NULL;
    }

  if (set->references != NULL)
    {
      ref = set->references;    /* use the first one on the list */
      ref->ref_count++;
    }
  else
    {
      ref = set_make_reference ();
      if (ref != NULL)
        {
          ref->set = set;
          set->references = ref;
        }
    }
  return (ref);
}

/*
 * setobj_release() -
 *      return: error code
 *  set(in) : set object
 *
 *  Note :
 *    This is used to disconnect a set object directly.  It should
 *    be called only by the low leve instance memory handler since this
 *    is the only thing that deals with set pointers directly.
 *    It functions basically the same as set_disconnect except that if there
 *    are no references to the set, it is ok to free all the storage
 *    for the set.
 *
 */

int
setobj_release (COL * set)
{
  int error = NO_ERROR;

  if (set->references == NULL)
    {
      setobj_free (set);        /* no references, free it */
    }
  else
    {
      error = set_disconnect (set->references); /* disconnect references */
    }

  return (error);
}

/*
 * setobj_build_domain_from_col() - builds a set domain (a chained list of
 *				    domains) from a collection object
 *  return: new domain constructed from the domains in the collection or NULL
 *	    if the domain couldn't be build
 *  col(in): input collection
 *  set_domain(in/out): set domain to be build, requires an already created
 *			domain
 */
int
setobj_build_domain_from_col (COL * col, TP_DOMAIN ** set_domain)
{
  int i;
  DB_VALUE *curr_value = NULL;
  TP_DOMAIN *curr_domain = NULL;
  int error_status = NO_ERROR;

  assert (set_domain != NULL);
  assert (*set_domain == NULL);

  if (col->domain != NULL && set_domain != NULL)
    {
      assert (col->domain != NULL);
      assert (col->domain->is_cached);

      for (i = 0; i < col->size; i++)
        {
          curr_value = INDEX (col, i);

          /* force copy of component set domains: domains are chained by next
           * in the set domain, so cached/primary domain should not be
           * included in this chain; component domain is not cached - it
           * should be freed when collection domain is freed */
          curr_domain = tp_domain_resolve_value (curr_value);
          if (curr_domain == NULL)
            {
              assert (false);
              return ER_FAILED;
            }
          assert (curr_domain->is_cached);

          /* !!! should copy component set domains !!! */
          curr_domain = tp_domain_copy (curr_domain);
          if (curr_domain == NULL)
            {
              assert (false);
              return ER_FAILED;
            }
          assert (curr_domain->next == NULL);
          assert (curr_domain->is_cached == 0);

          error_status = tp_domain_add (set_domain, curr_domain);
          if (error_status != NO_ERROR)
            {
              return error_status;
            }
        }
    }

  return error_status;
}

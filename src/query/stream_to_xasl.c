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
 * stream_to_xasl.c - XASL tree restorer
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include "error_manager.h"
#include "query_executor.h"
#include "server_interface.h"
#include "class_object.h"
#include "object_primitive.h"
#include "work_space.h"
#include "memory_alloc.h"
#include "heap_file.h"
#include "release_string.h"
#if defined(SERVER_MODE)
#include "thread.h"
#endif /* SERVER_MODE */

/* memory alignment unit - to align stored XASL tree nodes */
#define	ALIGN_UNIT	sizeof(double)
#define	ALIGN_MASK	(ALIGN_UNIT - 1)
#define MAKE_ALIGN(x)	(((x) & ~ALIGN_MASK) + \
                         (((x) & ALIGN_MASK) ? ALIGN_UNIT : 0))

/* to limit size of XASL trees */
#define	OFFSETS_PER_BLOCK	256
#define	START_PTR_PER_BLOCK	15
#define MAX_PTR_BLOCKS		256

#define PTR_BLOCK(ptr)  (((UINTPTR) ptr) / sizeof(UINTPTR)) % MAX_PTR_BLOCKS

/*
 * the linear byte stream for store the given XASL tree is allocated
 * and expanded dynamically on demand by the following amount of bytes
 */
#define	STREAM_EXPANSION_UNIT	(OFFSETS_PER_BLOCK * sizeof(int))
#define BUFFER_EXPANSION 4

#define BOUND_VAL (1 << ((OR_INT_SIZE * 8) - 2))

/* structure of a visited pointer constant */
typedef struct visited_ptr VISITED_PTR;
struct visited_ptr
{
  const void *ptr;              /* a pointer constant */
  void *str;                    /* where the struct pointed by 'ptr'
                                   is stored */
};

/* structure to hold information needed during packing */
typedef struct xasl_unpack_info XASL_UNPACK_INFO;
struct xasl_unpack_info
{
  char *packed_xasl;            /* ptr to packed xasl tree */
#if defined (SERVER_MODE)
  THREAD_ENTRY *thrd;           /* used for private allocation */
#endif                          /* SERVER_MODE */

  /* blocks of visited pointer constants */
  VISITED_PTR *ptr_blocks[MAX_PTR_BLOCKS];

  char *alloc_buf;              /* alloced buf */

  int packed_size;              /* packed xasl tree size */

  /* low-water-mark of visited pointers */
  int ptr_lwm[MAX_PTR_BLOCKS];

  /* max number of visited pointers */
  int ptr_max[MAX_PTR_BLOCKS];

  int alloc_size;               /* alloced buf size */
};

#if !defined(SERVER_MODE)
static XASL_UNPACK_INFO *xasl_unpack_info;
static int stx_Xasl_errcode = NO_ERROR;
#endif /* !SERVER_MODE */

static int stx_get_xasl_errcode (THREAD_ENTRY * thread_p);
static void stx_set_xasl_errcode (THREAD_ENTRY * thread_p, int errcode);
static XASL_UNPACK_INFO *stx_get_xasl_unpack_info_ptr (THREAD_ENTRY * thread_p);
#if defined(SERVER_MODE)
static void stx_set_xasl_unpack_info_ptr (THREAD_ENTRY * thread_p, XASL_UNPACK_INFO * ptr);
#endif /* SERVER_MODE */

static ACCESS_SPEC_TYPE *stx_restore_access_spec_type (THREAD_ENTRY * thread_p, char **ptr, void *arg);
static AGGREGATE_TYPE *stx_restore_aggregate_type (THREAD_ENTRY * thread_p, char *ptr);
static FUNCTION_TYPE *stx_restore_function_type (THREAD_ENTRY * thread_p, char *ptr);
static QFILE_SORTED_LIST_ID *stx_restore_srlist_id (THREAD_ENTRY * thread_p, char *ptr);
static QFILE_LIST_ID *stx_restore_list_id (THREAD_ENTRY * thread_p, char *ptr);
static ARITH_TYPE *stx_restore_arith_type (THREAD_ENTRY * thread_p, char *ptr);
static INDX_INFO *stx_restore_indx_info (THREAD_ENTRY * thread_p, char *ptr);
static OUTPTR_LIST *stx_restore_outptr_list (THREAD_ENTRY * thread_p, char *ptr);
static UPDDEL_CLASS_INFO *stx_restore_upddel_class_info (THREAD_ENTRY * thread_p, char *ptr);
static UPDATE_ASSIGNMENT *stx_restore_update_assignment_array (THREAD_ENTRY * thread_p, char *ptr, int no_assigns);
static ODKU_INFO *stx_restore_odku_info (THREAD_ENTRY * thread_p, char *ptr);
static PRED_EXPR *stx_restore_pred_expr (THREAD_ENTRY * thread_p, char *ptr);
static REGU_VARIABLE *stx_restore_regu_variable (THREAD_ENTRY * thread_p, char *ptr);
static REGU_VARIABLE_LIST stx_restore_regu_variable_list (THREAD_ENTRY * thread_p, char *ptr);
static SORT_LIST *stx_restore_sort_list (THREAD_ENTRY * thread_p, char *ptr);
#if defined (ENABLE_UNUSED_FUNCTION)
static char *stx_restore_string (THREAD_ENTRY * thread_p, char *ptr);
#endif
static VAL_LIST *stx_restore_val_list (THREAD_ENTRY * thread_p, char *ptr);
static DB_VALUE *stx_restore_db_value (THREAD_ENTRY * thread_p, char *ptr);
static XASL_NODE *stx_restore_xasl_node (THREAD_ENTRY * thread_p, char *ptr);
static HEAP_CACHE_ATTRINFO *stx_restore_cache_attrinfo (THREAD_ENTRY * thread_p, char *ptr);
static int *stx_restore_int_array (THREAD_ENTRY * thread_p, char *ptr, int size);
#if defined (ENABLE_UNUSED_FUNCTION)
static HFID *stx_restore_hfid_array (THREAD_ENTRY * thread_p, char *ptr, int nelements);
static OID *stx_restore_OID_array (THREAD_ENTRY * thread_p, char *ptr, int size);
#endif
static KEY_RANGE *stx_restore_key_range_array (THREAD_ENTRY * thread_p, char *ptr, int size);

static char *stx_build_xasl_node (THREAD_ENTRY * thread_p, char *tmp, XASL_NODE * ptr);
static char *stx_build_xasl_header (THREAD_ENTRY * thread_p, char *ptr, XASL_NODE_HEADER * xasl_header);
static char *stx_build_cache_attrinfo (char *tmp);
static char *stx_build_union_proc (THREAD_ENTRY * thread_p, char *tmp, UNION_PROC_NODE * ptr);
static char *stx_build_buildlist_proc (THREAD_ENTRY * thread_p, char *tmp, BUILDLIST_PROC_NODE * ptr);
static char *stx_build_buildvalue_proc (THREAD_ENTRY * thread_p, char *tmp, BUILDVALUE_PROC_NODE * ptr);
static char *stx_build_update_assignment (THREAD_ENTRY * thread_p, char *tmp, UPDATE_ASSIGNMENT * ptr);
static char *stx_build_update_proc (THREAD_ENTRY * thread_p, char *tmp, UPDATE_PROC_NODE * ptr);
static char *stx_build_delete_proc (THREAD_ENTRY * thread_p, char *tmp, DELETE_PROC_NODE * ptr);
static char *stx_build_insert_proc (THREAD_ENTRY * thread_p, char *tmp, INSERT_PROC_NODE * ptr);
static char *stx_build_outptr_list (THREAD_ENTRY * thread_p, char *tmp, OUTPTR_LIST * ptr);
static char *stx_build_pred_expr (THREAD_ENTRY * thread_p, char *tmp, PRED_EXPR * ptr);
static char *stx_build_pred (THREAD_ENTRY * thread_p, char *tmp, PRED * ptr);
static char *stx_build_eval_term (THREAD_ENTRY * thread_p, char *tmp, EVAL_TERM * ptr);
static char *stx_build_comp_eval_term (THREAD_ENTRY * thread_p, char *tmp, COMP_EVAL_TERM * ptr);
static char *stx_build_alsm_eval_term (THREAD_ENTRY * thread_p, char *tmp, ALSM_EVAL_TERM * ptr);
static char *stx_build_like_eval_term (THREAD_ENTRY * thread_p, char *tmp, LIKE_EVAL_TERM * ptr);
static char *stx_build_rlike_eval_term (THREAD_ENTRY * thread_p, char *tmp, RLIKE_EVAL_TERM * ptr);
static char *stx_build_access_spec_type (THREAD_ENTRY * thread_p, char *tmp, ACCESS_SPEC_TYPE * ptr, void *arg);
static char *stx_build_indx_info (THREAD_ENTRY * thread_p, char *tmp, INDX_INFO * ptr);
static char *stx_build_indx_id (THREAD_ENTRY * thread_p, char *tmp, INDX_ID * ptr);
static char *stx_build_key_info (THREAD_ENTRY * thread_p, char *tmp, KEY_INFO * ptr);
static char *stx_build_cls_spec_type (THREAD_ENTRY * thread_p, char *tmp, CLS_SPEC_TYPE * ptr);
static char *stx_build_list_spec_type (THREAD_ENTRY * thread_p, char *tmp, LIST_SPEC_TYPE * ptr);
static char *stx_build_val_list (THREAD_ENTRY * thread_p, char *tmp, VAL_LIST * ptr);
static char *stx_build_regu_variable (THREAD_ENTRY * thread_p, char *tmp, REGU_VARIABLE * ptr);
static char *stx_unpack_regu_variable_value (THREAD_ENTRY * thread_p, char *tmp, REGU_VARIABLE * ptr);
static char *stx_build_attr_descr (THREAD_ENTRY * thread_p, char *tmp, ATTR_DESCR * ptr);
static char *stx_build_pos_descr (char *tmp, QFILE_TUPLE_VALUE_POSITION * ptr);
static char *stx_build_db_value (THREAD_ENTRY * thread_p, char *tmp, DB_VALUE * ptr);
static char *stx_build_arith_type (THREAD_ENTRY * thread_p, char *tmp, ARITH_TYPE * ptr);
static char *stx_build_aggregate_type (THREAD_ENTRY * thread_p, char *tmp, AGGREGATE_TYPE * ptr);
static char *stx_build_function_type (THREAD_ENTRY * thread_p, char *tmp, FUNCTION_TYPE * ptr);
static char *stx_build_srlist_id (THREAD_ENTRY * thread_p, char *tmp, QFILE_SORTED_LIST_ID * ptr);
static char *stx_build_sort_list (THREAD_ENTRY * thread_p, char *tmp, SORT_LIST * ptr);

#if defined(ENABLE_UNUSED_FUNCTION)
static void stx_init_regu_variable (REGU_VARIABLE * regu);
#endif

static int stx_mark_struct_visited (THREAD_ENTRY * thread_p, const void *ptr, void *str);
static void *stx_get_struct_visited_ptr (THREAD_ENTRY * thread_p, const void *ptr);
static void stx_free_visited_ptrs (THREAD_ENTRY * thread_p);
static char *stx_alloc_struct (THREAD_ENTRY * thread_p, int size);
static int stx_init_xasl_unpack_info (THREAD_ENTRY * thread_p, char *xasl_stream, int xasl_stream_size);

#if defined(ENABLE_UNUSED_FUNCTION)
static char *stx_unpack_char (char *tmp, char *ptr);
static char *stx_unpack_long (char *tmp, long *ptr);
#endif

/*
 * stx_map_stream_to_xasl_node_header () - Obtain xasl node header from xasl
 *					   stream.
 *
 * return	       : error code.
 * thread_p (in)       : thread entry.
 * xasl_header_p (out) : pointer to xasl node header.
 * xasl_stream (in)    : xasl stream.
 */
int
stx_map_stream_to_xasl_node_header (UNUSED_ARG THREAD_ENTRY * thread_p,
                                    XASL_NODE_HEADER * xasl_header_p, char *xasl_stream)
{
  int xasl_stream_header_size = 0, offset = 0;
  char *ptr = NULL;

  if (xasl_stream == NULL || xasl_header_p == NULL)
    {
      assert (0);
      return ER_FAILED;
    }
  (void) or_unpack_int (xasl_stream, &xasl_stream_header_size);
  offset = OR_INT_SIZE +        /* xasl stream header size */
    xasl_stream_header_size +   /* xasl stream header data */
    OR_INT_SIZE;                /* xasl stream body size */
  offset = MAKE_ALIGN (offset);
  ptr = xasl_stream + offset;
  OR_UNPACK_XASL_NODE_HEADER (ptr, xasl_header_p);
  return NO_ERROR;
}

/*
 * stx_map_stream_to_xasl () -
 *   return: if successful, return 0, otherwise non-zero error code
 *   xasl_tree(in)      : pointer to where to return the
 *                        root of the unpacked XASL tree
 *   xasl_stream(in)    : pointer to xasl stream
 *   xasl_stream_size(in)       : # of bytes in xasl_stream
 *   xasl_unpack_info_ptr(in)   : pointer to where to return the pack info
 *
 * Note: map the linear byte stream in disk representation to an XASL tree.
 *
 * Note: the caller is responsible for freeing the memory of
 * xasl_unpack_info_ptr. The free function is stx_free_xasl_unpack_info().
 */
int
stx_map_stream_to_xasl (THREAD_ENTRY * thread_p, XASL_NODE ** xasl_tree, char *xasl_stream, int xasl_stream_size)
{
  XASL_NODE *xasl;
  char *p;
  int header_size;
  int offset;
  int i;
#if defined(SERVER_MODE)
  HL_HEAPID pri_heap_id = 0;
#endif
  int errcode = NO_ERROR;

  if (!xasl_tree || !xasl_stream || xasl_stream_size <= 0)
    {
      return ER_QPROC_INVALID_XASLNODE;
    }

#if defined(SERVER_MODE)
  (void) css_set_private_heap (thread_p, db_create_private_heap ());
  pri_heap_id = css_get_private_heap (thread_p);
  if (pri_heap_id == 0)
    {
      return ER_CSS_ALLOC;
    }
#endif

  stx_set_xasl_errcode (thread_p, NO_ERROR);
  stx_init_xasl_unpack_info (thread_p, xasl_stream, xasl_stream_size);

  /* calculate offset to XASL tree in the stream buffer */
  p = or_unpack_int (xasl_stream, &header_size);
  offset = sizeof (int)         /* [size of header data] */
    + header_size               /* [header data] */
    + sizeof (int);             /* [size of body data] */
  assert (p != NULL);
  offset = MAKE_ALIGN (offset);

  /* restore XASL tree from body data of the stream buffer */
  xasl = stx_restore_xasl_node (thread_p, xasl_stream + offset);
  if (xasl == NULL)
    {
      assert (false);
#if defined(SERVER_MODE)
      stx_free_xasl_unpack_info (stx_get_xasl_unpack_info_ptr (thread_p));
#endif /* SERVER_MODE */
      goto end;
    }

  /* set result */
  *xasl_tree = xasl;

#if defined(SERVER_MODE)
  xasl->private_heap_id = pri_heap_id;
  assert (xasl->private_heap_id != 0);
#endif /* SERVER_MODE */

  /* restore header data of new XASL format */
  p = or_unpack_int (p, &xasl->dbval_cnt);
  assert (p != NULL);
  p = or_unpack_oid (p, &xasl->creator_oid);
  assert (p != NULL);
  p = or_unpack_int (p, &xasl->n_oid_list);
  assert (p != NULL);

  if (xasl->n_oid_list > 0)
    {
      xasl->class_oid_list = (OID *) stx_alloc_struct (thread_p, xasl->n_oid_list * sizeof (OID));
      if (xasl->class_oid_list == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          goto end;
        }
      xasl->tcard_list = (int *) stx_alloc_struct (thread_p, xasl->n_oid_list * sizeof (int));
      if (xasl->class_oid_list == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          goto end;
        }
    }
  for (i = 0; i < xasl->n_oid_list; i++)
    {
      p = or_unpack_oid (p, &xasl->class_oid_list[i]);
      assert (p != NULL);
    }
  for (i = 0; i < xasl->n_oid_list; i++)
    {
      p = or_unpack_int (p, &xasl->tcard_list[i]);
      assert (p != NULL);
    }

  /* initialize the query in progress flag to FALSE.  Note that this flag
     is not packed/unpacked.  It is strictly a server side flag. */
  xasl->query_in_progress = false;

end:
  stx_free_visited_ptrs (thread_p);
#if defined(SERVER_MODE)
  stx_set_xasl_unpack_info_ptr (thread_p, NULL);
#endif /* SERVER_MODE */

  errcode = stx_get_xasl_errcode (thread_p);
#if defined (SERVER_MODE)
  if (errcode != NO_ERROR)
    {
      assert (pri_heap_id != 0);
      db_destroy_private_heap (pri_heap_id);
    }
#endif /* SERVER_MODE */

  return errcode;
}

#if defined (SERVER_MODE)
/*
 * stx_free_xasl_unpack_info () -
 *   return:
 *   xasl_unpack_info(in): unpack info returned by stx_map_stream_to_xasl ()
 *
 * Note: free the memory used for unpacking the xasl tree.
 */
void
stx_free_xasl_unpack_info (void *xasl_unpack_info)
{
#if defined (SERVER_MODE)
  if (xasl_unpack_info)
    {
      ((XASL_UNPACK_INFO *) xasl_unpack_info)->thrd = NULL;
    }
#endif /* SERVER_MODE */
}
#endif /* SERVER_MODE */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * stx_free_additional_buff () - free additional buffers allocated during
 *				 XASL unpacking
 * return : void
 * xasl_unpack_info (in) : XASL unpack info
 */
void
stx_free_additional_buff (THREAD_ENTRY * thread_p, void *xasl_unpack_info)
{
#if defined (SERVER_MODE)
  if (xasl_unpack_info)
    {
      UNPACK_EXTRA_BUF *add_buff = ((XASL_UNPACK_INFO *) xasl_unpack_info)->additional_buffers;
      UNPACK_EXTRA_BUF *temp = NULL;
      while (add_buff != NULL)
        {
          temp = add_buff->next;
          db_private_free_and_init (thread_p, add_buff->buff);
          db_private_free_and_init (thread_p, add_buff);
          add_buff = temp;
        }
    }
#endif /* SERVER_MODE */
}
#endif

/*
 * stx_restore_func_postfix () -
 *   return: if successful, return the offset of position
 *           in disk object where the node is stored, otherwise
 *           return ER_FAILED and error code is set to xasl_errcode.
 *   ptr(in): pointer to an XASL tree node whose type is return type
 *
 * Note: store the XASL tree node pointed by 'ptr' into disk
 * object with the help of stx_build_func_postfix to process
 * the members of the node.
 */

static AGGREGATE_TYPE *
stx_restore_aggregate_type (THREAD_ENTRY * thread_p, char *ptr)
{
  AGGREGATE_TYPE *aggregate;

  if (ptr == NULL)
    {
      return NULL;
    }

  aggregate = (AGGREGATE_TYPE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (aggregate != NULL)
    {
      return aggregate;
    }

  aggregate = (AGGREGATE_TYPE *) stx_alloc_struct (thread_p, sizeof (*aggregate));
  if (aggregate == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, aggregate) == ER_FAILED
      || stx_build_aggregate_type (thread_p, ptr, aggregate) == NULL)
    {
      return NULL;
    }

  return aggregate;
}

static FUNCTION_TYPE *
stx_restore_function_type (THREAD_ENTRY * thread_p, char *ptr)
{
  FUNCTION_TYPE *function;

  if (ptr == NULL)
    {
      return NULL;
    }

  function = (FUNCTION_TYPE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (function != NULL)
    {
      return function;
    }

  function = (FUNCTION_TYPE *) stx_alloc_struct (thread_p, sizeof (*function));
  if (function == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, function) == ER_FAILED
      || stx_build_function_type (thread_p, ptr, function) == NULL)
    {
      return NULL;
    }

  return function;
}

static QFILE_SORTED_LIST_ID *
stx_restore_srlist_id (THREAD_ENTRY * thread_p, char *ptr)
{
  QFILE_SORTED_LIST_ID *sort_list_id;

  if (ptr == NULL)
    {
      return NULL;
    }

  sort_list_id = (QFILE_SORTED_LIST_ID *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (sort_list_id != NULL)
    {
      return sort_list_id;
    }

  sort_list_id = (QFILE_SORTED_LIST_ID *) stx_alloc_struct (thread_p, sizeof (*sort_list_id));
  if (sort_list_id == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, sort_list_id) == ER_FAILED
      || stx_build_srlist_id (thread_p, ptr, sort_list_id) == NULL)
    {
      return NULL;
    }

  return sort_list_id;
}

static ARITH_TYPE *
stx_restore_arith_type (THREAD_ENTRY * thread_p, char *ptr)
{
  ARITH_TYPE *arithmetic;

  if (ptr == NULL)
    {
      return NULL;
    }

  arithmetic = (ARITH_TYPE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (arithmetic != NULL)
    {
      return arithmetic;
    }

  arithmetic = (ARITH_TYPE *) stx_alloc_struct (thread_p, sizeof (*arithmetic));
  if (arithmetic == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, arithmetic) == ER_FAILED
      || stx_build_arith_type (thread_p, ptr, arithmetic) == NULL)
    {
      return NULL;
    }

  return arithmetic;
}

static INDX_INFO *
stx_restore_indx_info (THREAD_ENTRY * thread_p, char *ptr)
{
  INDX_INFO *indx_info;

  if (ptr == NULL)
    {
      return NULL;
    }

  indx_info = (INDX_INFO *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (indx_info != NULL)
    {
      return indx_info;
    }

  indx_info = (INDX_INFO *) stx_alloc_struct (thread_p, sizeof (*indx_info));
  if (indx_info == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, indx_info) == ER_FAILED
      || stx_build_indx_info (thread_p, ptr, indx_info) == NULL)
    {
      return NULL;
    }

  return indx_info;
}

static OUTPTR_LIST *
stx_restore_outptr_list (THREAD_ENTRY * thread_p, char *ptr)
{
  OUTPTR_LIST *outptr_list;

  if (ptr == NULL)
    {
      return NULL;
    }

  outptr_list = (OUTPTR_LIST *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (outptr_list != NULL)
    {
      return outptr_list;
    }

  outptr_list = (OUTPTR_LIST *) stx_alloc_struct (thread_p, sizeof (*outptr_list));
  if (outptr_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, outptr_list) == ER_FAILED
      || stx_build_outptr_list (thread_p, ptr, outptr_list) == NULL)
    {
      return NULL;
    }

  return outptr_list;
}

static PRED_EXPR *
stx_restore_pred_expr (THREAD_ENTRY * thread_p, char *ptr)
{
  PRED_EXPR *pred_expr;

  if (ptr == NULL)
    {
      return NULL;
    }

  pred_expr = (PRED_EXPR *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (pred_expr != NULL)
    {
      return pred_expr;
    }

  pred_expr = (PRED_EXPR *) stx_alloc_struct (thread_p, sizeof (*pred_expr));
  if (pred_expr == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, pred_expr) == ER_FAILED
      || stx_build_pred_expr (thread_p, ptr, pred_expr) == NULL)
    {
      return NULL;
    }

  return pred_expr;
}

static REGU_VARIABLE *
stx_restore_regu_variable (THREAD_ENTRY * thread_p, char *ptr)
{
  REGU_VARIABLE *regu_var;

  if (ptr == NULL)
    {
      return NULL;
    }

  regu_var = (REGU_VARIABLE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (regu_var != NULL)
    {
      return regu_var;
    }

  regu_var = (REGU_VARIABLE *) stx_alloc_struct (thread_p, sizeof (*regu_var));
  if (regu_var == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, regu_var) == ER_FAILED
      || stx_build_regu_variable (thread_p, ptr, regu_var) == NULL)
    {
      return NULL;
    }

  return regu_var;
}

static SORT_LIST *
stx_restore_sort_list (THREAD_ENTRY * thread_p, char *ptr)
{
  SORT_LIST *sort_list;

  if (ptr == NULL)
    {
      return NULL;
    }

  sort_list = (SORT_LIST *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (sort_list != NULL)
    {
      return sort_list;
    }

  sort_list = (SORT_LIST *) stx_alloc_struct (thread_p, sizeof (*sort_list));
  if (sort_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, sort_list) == ER_FAILED
      || stx_build_sort_list (thread_p, ptr, sort_list) == NULL)
    {
      return NULL;
    }

  return sort_list;
}

static VAL_LIST *
stx_restore_val_list (THREAD_ENTRY * thread_p, char *ptr)
{
  VAL_LIST *val_list;

  if (ptr == NULL)
    {
      return NULL;
    }

  val_list = (VAL_LIST *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (val_list != NULL)
    {
      return val_list;
    }

  val_list = (VAL_LIST *) stx_alloc_struct (thread_p, sizeof (*val_list));
  if (val_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, val_list) == ER_FAILED
      || stx_build_val_list (thread_p, ptr, val_list) == NULL)
    {
      return NULL;
    }

  return val_list;
}

static DB_VALUE *
stx_restore_db_value (THREAD_ENTRY * thread_p, char *ptr)
{
  DB_VALUE *value;

  if (ptr == NULL)
    {
      return NULL;
    }

  value = (DB_VALUE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (value != NULL)
    {
      return value;
    }

  value = (DB_VALUE *) stx_alloc_struct (thread_p, sizeof (*value));
  if (value == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, value) == ER_FAILED || stx_build_db_value (thread_p, ptr, value) == NULL)
    {
      return NULL;
    }

  return value;
}

static XASL_NODE *
stx_restore_xasl_node (THREAD_ENTRY * thread_p, char *ptr)
{
  XASL_NODE *xasl;

  if (ptr == NULL)
    {
      return NULL;
    }

  xasl = (XASL_NODE *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (xasl != NULL)
    {
      return xasl;
    }

  xasl = (XASL_NODE *) stx_alloc_struct (thread_p, sizeof (*xasl));
  if (xasl == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, xasl) == ER_FAILED || stx_build_xasl_node (thread_p, ptr, xasl) == NULL)
    {
      return NULL;
    }

  return xasl;
}

static HEAP_CACHE_ATTRINFO *
stx_restore_cache_attrinfo (THREAD_ENTRY * thread_p, char *ptr)
{
  HEAP_CACHE_ATTRINFO *cache_attrinfo;

  if (ptr == NULL)
    {
      return NULL;
    }

  cache_attrinfo = (HEAP_CACHE_ATTRINFO *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (cache_attrinfo != NULL)
    {
      return cache_attrinfo;
    }

  cache_attrinfo = (HEAP_CACHE_ATTRINFO *) stx_alloc_struct (thread_p, sizeof (*cache_attrinfo));
  if (cache_attrinfo == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  if (stx_mark_struct_visited (thread_p, ptr, cache_attrinfo) == ER_FAILED || stx_build_cache_attrinfo (ptr) == NULL)
    {
      return NULL;
    }

  return cache_attrinfo;
}

static QFILE_LIST_ID *
stx_restore_list_id (THREAD_ENTRY * thread_p, char *ptr)
{
  QFILE_LIST_ID *list_id;

  if (ptr == NULL)
    {
      return NULL;
    }

  list_id = (QFILE_LIST_ID *) stx_get_struct_visited_ptr (thread_p, ptr);
  if (list_id != NULL)
    {
      return list_id;
    }

  or_unpack_listid (ptr, (void **) &list_id);
  if (list_id == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  if (stx_mark_struct_visited (thread_p, ptr, list_id) == ER_FAILED)
    {
      return NULL;
    }

  return list_id;
}

#if defined (ENABLE_UNUSED_FUNCTION)
static char *
stx_restore_string (THREAD_ENTRY * thread_p, char *ptr)
{
  char *string;

  or_unpack_string (ptr, &string);
  if (string == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  return string;
}
#endif

static int *
stx_restore_int_array (THREAD_ENTRY * thread_p, char *ptr, int nelements)
{
  int *int_array;
  int i;

  int_array = (int *) stx_alloc_struct (thread_p, sizeof (int) * nelements);
  if (int_array == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  for (i = 0; i < nelements; ++i)
    {
      ptr = or_unpack_int (ptr, &int_array[i]);
    }

  return int_array;
}

#if defined(ENABLE_UNUSED_FUNCTION)
static HFID *
stx_restore_hfid_array (THREAD_ENTRY * thread_p, char *ptr, int nelements)
{
  HFID *hfid_array;
  int i;

  hfid_array = (HFID *) stx_alloc_struct (thread_p, sizeof (HFID) * nelements);
  if (hfid_array == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  for (i = 0; i < nelements; ++i)
    {
      ptr = or_unpack_hfid (ptr, &hfid_array[i]);
    }

  return hfid_array;
}

static OID *
stx_restore_OID_array (THREAD_ENTRY * thread_p, char *ptr, int nelements)
{
  OID *oid_array;
  int i;

  oid_array = (OID *) stx_alloc_struct (thread_p, sizeof (OID) * nelements);
  if (oid_array == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  for (i = 0; i < nelements; i++)
    {
      ptr = or_unpack_oid (ptr, &oid_array[i]);
    }

  return oid_array;
}

static char *
stx_restore_input_vals (THREAD_ENTRY * thread_p, char *ptr, int nelements)
{
  char *input_vals;

  input_vals = stx_alloc_struct (thread_p, nelements);
  if (input_vals == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  memmove (input_vals, ptr, nelements);

  return input_vals;
}
#endif

/*
 * Restore the regu_variable_list as an array to avoid recursion in the server.
 * The array size is restored first, then the array.
 */
static REGU_VARIABLE_LIST
stx_restore_regu_variable_list (THREAD_ENTRY * thread_p, char *ptr)
{
  REGU_VARIABLE_LIST regu_var_list;
  int offset;
  int total;
  int i;
  char *ptr2;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  regu_var_list = (REGU_VARIABLE_LIST) stx_get_struct_visited_ptr (thread_p, ptr);
  if (regu_var_list != NULL)
    {
      return regu_var_list;
    }

  ptr2 = or_unpack_int (ptr, &total);
  regu_var_list = (REGU_VARIABLE_LIST) stx_alloc_struct (thread_p, sizeof (struct regu_variable_list_node) * total);
  if (regu_var_list == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }
  if (stx_mark_struct_visited (thread_p, ptr, regu_var_list) == ER_FAILED)
    {
      return NULL;
    }

  ptr = ptr2;
  for (i = 0; i < total; i++)
    {
      ptr = or_unpack_int (ptr, &offset);
      if (stx_build_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset], &regu_var_list[i].value) == NULL)
        {
          return NULL;
        }

      if (i < total - 1)
        {
          regu_var_list[i].next = (struct regu_variable_list_node *) &regu_var_list[i + 1];
        }
      else
        {
          regu_var_list[i].next = NULL;
        }
    }

  return regu_var_list;
}

static KEY_RANGE *
stx_restore_key_range_array (THREAD_ENTRY * thread_p, char *ptr, int nelements)
{
  KEY_RANGE *key_range_array;
  int *ints;
  int i, j, offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  key_range_array = (KEY_RANGE *) stx_alloc_struct (thread_p, sizeof (KEY_RANGE) * nelements);
  if (key_range_array == NULL)
    {
      goto error;
    }

  ints = stx_restore_int_array (thread_p, ptr, 3 * nelements);
  if (ints == NULL)
    {
      return NULL;
    }

  for (i = 0, j = 0; i < nelements; i++, j++)
    {
      key_range_array[i].range = (RANGE) ints[j];

      offset = ints[++j];
      if (offset)
        {
          key_range_array[i].key1 = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
          if (key_range_array[i].key1 == NULL)
            {
              goto error;
            }
        }
      else
        {
          key_range_array[i].key1 = NULL;
        }

      offset = ints[++j];
      if (offset)
        {
          key_range_array[i].key2 = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
          if (key_range_array[i].key2 == NULL)
            {
              goto error;
            }
        }
      else
        {
          key_range_array[i].key2 = NULL;
        }
    }

  return key_range_array;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

/*
 * Restore access spec type list as an array to avoid recursion in the server.
 * The array size is restored first, then the array.
 */
static ACCESS_SPEC_TYPE *
stx_restore_access_spec_type (THREAD_ENTRY * thread_p, char **ptr, void *arg)
{
  ACCESS_SPEC_TYPE *access_spec_type = NULL;
  int total, i;

  *ptr = or_unpack_int (*ptr, &total);
  if (total > 0)
    {
      access_spec_type = (ACCESS_SPEC_TYPE *) stx_alloc_struct (thread_p, sizeof (ACCESS_SPEC_TYPE) * total);
      if (access_spec_type == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  for (i = 0; i < total; i++)
    {
      *ptr = stx_build_access_spec_type (thread_p, *ptr, &access_spec_type[i], arg);
      if (*ptr == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
      if (i < total - 1)
        {
          access_spec_type[i].next = (ACCESS_SPEC_TYPE *) (&access_spec_type[i + 1]);
        }
      else
        {
          access_spec_type[i].next = NULL;
        }
    }

  return access_spec_type;
}

/*
 * stx_build_xasl_header () - Unpack XASL node header from buffer.
 *
 * return	     : buffer pointer after the unpacked XASL node header
 * thread_p (in)     : thread entry
 * ptr (in)	     : buffer pointer where XASL node header is packed
 * xasl_header (out) : Unpacked XASL node header
 */
static char *
stx_build_xasl_header (UNUSED_ARG THREAD_ENTRY * thread_p, char *ptr, XASL_NODE_HEADER * xasl_header)
{
  if (ptr == NULL)
    {
      return NULL;
    }
  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  OR_UNPACK_XASL_NODE_HEADER (ptr, xasl_header);
  return ptr;
}

static char *
stx_build_xasl_node (THREAD_ENTRY * thread_p, char *ptr, XASL_NODE * xasl)
{
  int offset;
  int tmp;

  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  /* initialize query_in_progress flag */
  xasl->query_in_progress = false;

  /* XASL node header is packed first */
  ptr = stx_build_xasl_header (thread_p, ptr, &xasl->header);

  ptr = or_unpack_int (ptr, &tmp);
  xasl->type = (PROC_TYPE) tmp;

  ptr = or_unpack_int (ptr, &xasl->flag);

  /* initialize xasl status */
  xasl->status = XASL_INITIALIZED;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->list_id = NULL;
    }
  else
    {
      xasl->list_id = stx_restore_list_id (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->list_id == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->after_iscan_list = NULL;
    }
  else
    {
      xasl->after_iscan_list = stx_restore_sort_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->after_iscan_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->orderby_list = NULL;
    }
  else
    {
      xasl->orderby_list = stx_restore_sort_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->orderby_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->ordbynum_pred = NULL;
    }
  else
    {
      xasl->ordbynum_pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->ordbynum_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->ordbynum_val = NULL;
    }
  else
    {
      xasl->ordbynum_val = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->ordbynum_val == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->orderby_limit = NULL;
    }
  else
    {
      xasl->orderby_limit = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->orderby_limit == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, (int *) &xasl->ordbynum_flag);

  xasl->topn_items = NULL;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->limit_row_count = NULL;
    }
  else
    {
      xasl->limit_row_count = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->limit_row_count == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->single_tuple = NULL;
    }
  else
    {
      xasl->single_tuple = stx_restore_val_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->single_tuple == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &xasl->is_single_tuple);

  ptr = or_unpack_int (ptr, &tmp);
  xasl->option = (QUERY_OPTIONS) tmp;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->outptr_list = NULL;
    }
  else
    {
      xasl->outptr_list = stx_restore_outptr_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->outptr_list == NULL)
        {
          goto error;
        }
    }

  xasl->spec_list = stx_restore_access_spec_type (thread_p, &ptr, (void *) xasl->outptr_list);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->val_list = NULL;
    }
  else
    {
      xasl->val_list = stx_restore_val_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->val_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->aptr_list = NULL;
    }
  else
    {
      xasl->aptr_list = stx_restore_xasl_node (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->aptr_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->dptr_list = NULL;
    }
  else
    {
      xasl->dptr_list = stx_restore_xasl_node (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->dptr_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->after_join_pred = NULL;
    }
  else
    {
      xasl->after_join_pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->after_join_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->if_pred = NULL;
    }
  else
    {
      xasl->if_pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->if_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->instnum_pred = NULL;
    }
  else
    {
      xasl->instnum_pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->instnum_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->instnum_val = NULL;
    }
  else
    {
      xasl->instnum_val = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->instnum_val == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->save_instnum_val = NULL;
    }
  else
    {
      xasl->save_instnum_val = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->save_instnum_val == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, (int *) &xasl->instnum_flag);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->scan_ptr = NULL;
    }
  else
    {
      xasl->scan_ptr = stx_restore_xasl_node (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->scan_ptr == NULL)
        {
          goto error;
        }
    }

  xasl->curr_spec = stx_restore_access_spec_type (thread_p, &ptr, NULL);

  ptr = or_unpack_int (ptr, &xasl->next_scan_on);

  ptr = or_unpack_int (ptr, &xasl->next_scan_block_on);

  ptr = or_unpack_int (ptr, &xasl->upd_del_class_cnt);

  switch (xasl->type)
    {
    case BUILDLIST_PROC:
      ptr = stx_build_buildlist_proc (thread_p, ptr, &xasl->proc.buildlist);
      break;

    case BUILDVALUE_PROC:
      ptr = stx_build_buildvalue_proc (thread_p, ptr, &xasl->proc.buildvalue);
      break;

    case UPDATE_PROC:
      ptr = stx_build_update_proc (thread_p, ptr, &xasl->proc.update);
      break;

    case DELETE_PROC:
      ptr = stx_build_delete_proc (thread_p, ptr, &xasl->proc.delete_);
      break;

    case INSERT_PROC:
      ptr = stx_build_insert_proc (thread_p, ptr, &xasl->proc.insert);
      break;

    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:
      ptr = stx_build_union_proc (thread_p, ptr, &xasl->proc.union_);
      break;

    case SCAN_PROC:
      break;

    default:
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_unpack_int (ptr, (int *) &xasl->projected_size);
  ptr = or_unpack_double (ptr, (double *) &xasl->cardinality);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      xasl->next = NULL;
    }
  else
    {
      xasl->next = stx_restore_xasl_node (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (xasl->next == NULL)
        {
          goto error;
        }
    }

  memset (&xasl->orderby_stats, 0, sizeof (xasl->orderby_stats));
  memset (&xasl->groupby_stats, 0, sizeof (xasl->groupby_stats));
  memset (&xasl->xasl_stats, 0, sizeof (xasl->xasl_stats));
  xasl->private_heap_id = 0;

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_cache_attrinfo (char *ptr)
{
  int dummy;

  /* unpack the zero int that is sent mainly as a placeholder */
  ptr = or_unpack_int (ptr, &dummy);

  return ptr;
}

static char *
stx_build_union_proc (THREAD_ENTRY * thread_p, char *ptr, UNION_PROC_NODE * union_proc)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      union_proc->left = NULL;
    }
  else
    {
      union_proc->left = stx_restore_xasl_node (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (union_proc->left == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      union_proc->right = NULL;
    }
  else
    {
      union_proc->right = stx_restore_xasl_node (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (union_proc->right == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  return ptr;
}

static char *
stx_build_buildlist_proc (THREAD_ENTRY * thread_p, char *ptr, BUILDLIST_PROC_NODE * stx_build_list_proc)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  stx_build_list_proc->output_columns = (DB_VALUE **) 0;
  EHID_SET_NULL (&(stx_build_list_proc->upd_del_ehid));

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->eptr_list = NULL;
    }
  else
    {
      stx_build_list_proc->eptr_list = stx_restore_xasl_node (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->eptr_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->groupby_list = NULL;
    }
  else
    {
      stx_build_list_proc->groupby_list = stx_restore_sort_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->groupby_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->after_groupby_list = NULL;
    }
  else
    {
      stx_build_list_proc->after_groupby_list =
        stx_restore_sort_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->after_groupby_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_outptr_list = NULL;
    }
  else
    {
      stx_build_list_proc->g_outptr_list = stx_restore_outptr_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_outptr_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_regu_list = NULL;
    }
  else
    {
      stx_build_list_proc->g_regu_list =
        stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_regu_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_val_list = NULL;
    }
  else
    {
      stx_build_list_proc->g_val_list = stx_restore_val_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_val_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_having_pred = NULL;
    }
  else
    {
      stx_build_list_proc->g_having_pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_having_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_grbynum_pred = NULL;
    }
  else
    {
      stx_build_list_proc->g_grbynum_pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_grbynum_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_grbynum_val = NULL;
    }
  else
    {
      stx_build_list_proc->g_grbynum_val = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_grbynum_val == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, (int *) &stx_build_list_proc->g_grbynum_flag);
  ptr = or_unpack_int (ptr, (int *) &stx_build_list_proc->g_with_rollup);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_list_proc->g_agg_list = NULL;
    }
  else
    {
      stx_build_list_proc->g_agg_list = stx_restore_aggregate_type (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_list_proc->g_agg_list == NULL)
        {
          goto error;
        }
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_buildvalue_proc (THREAD_ENTRY * thread_p, char *ptr, BUILDVALUE_PROC_NODE * stx_build_value_proc)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_value_proc->having_pred = NULL;
    }
  else
    {
      stx_build_value_proc->having_pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_value_proc->having_pred == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_value_proc->grbynum_val = NULL;
    }
  else
    {
      stx_build_value_proc->grbynum_val = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_value_proc->grbynum_val == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_value_proc->agg_list = NULL;
    }
  else
    {
      stx_build_value_proc->agg_list = stx_restore_aggregate_type (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_value_proc->agg_list == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_build_value_proc->outarith_list = NULL;
    }
  else
    {
      stx_build_value_proc->outarith_list = stx_restore_arith_type (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (stx_build_value_proc->outarith_list == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  return ptr;
}

static char *
stx_build_update_class_info (THREAD_ENTRY * thread_p, char *ptr, UPDDEL_CLASS_INFO * upd_cls)
{
  int offset = 0;

  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  /* class_oid */
  ptr = or_unpack_oid (ptr, &(upd_cls->class_oid));
  if (OID_ISNULL (&(upd_cls->class_oid)))
    {
      return NULL;
    }

  /* class_hfid */
  ptr = or_unpack_hfid (ptr, &(upd_cls->class_hfid));
  if (HFID_IS_NULL (&(upd_cls->class_hfid)))
    {
      return NULL;
    }

  /* no_attrs & att_id */
  ptr = or_unpack_int (ptr, &upd_cls->no_attrs);
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || upd_cls->no_attrs == 0)
    {
      upd_cls->att_id = NULL;
    }
  else
    {
      upd_cls->att_id = stx_restore_int_array (thread_p, &xasl_unpack_info->packed_xasl[offset], upd_cls->no_attrs);
      if (upd_cls->att_id == NULL)
        {
          return NULL;
        }
    }

  /* has_uniques */
  ptr = or_unpack_int (ptr, &upd_cls->has_uniques);

  return ptr;
}

static UPDDEL_CLASS_INFO *
stx_restore_upddel_class_info (THREAD_ENTRY * thread_p, char *ptr)
{
  UPDDEL_CLASS_INFO *upd_cls = NULL;

  upd_cls = (UPDDEL_CLASS_INFO *) stx_alloc_struct (thread_p, sizeof (UPDDEL_CLASS_INFO));
  if (upd_cls == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  ptr = stx_build_update_class_info (thread_p, ptr, upd_cls);
  if (ptr == NULL)
    {
      return NULL;
    }

  return upd_cls;
}

static char *
stx_build_update_assignment (THREAD_ENTRY * thread_p, char *ptr, UPDATE_ASSIGNMENT * assign)
{
  int offset = 0;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  /* att_idx */
  ptr = or_unpack_int (ptr, &assign->att_idx);

  /* regu_var */
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      assign->regu_var = NULL;
    }
  else
    {
      assign->regu_var = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (assign->regu_var == NULL)
        {
          return NULL;
        }
    }

  return ptr;
}

static UPDATE_ASSIGNMENT *
stx_restore_update_assignment_array (THREAD_ENTRY * thread_p, char *ptr, int no_assigns)
{
  int idx;
  UPDATE_ASSIGNMENT *assigns = NULL;

  assigns = (UPDATE_ASSIGNMENT *) stx_alloc_struct (thread_p, sizeof (*assigns) * no_assigns);
  if (assigns == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  for (idx = 0; idx < no_assigns; idx++)
    {
      ptr = stx_build_update_assignment (thread_p, ptr, &assigns[idx]);
      if (ptr == NULL)
        {
          return NULL;
        }
    }

  return assigns;
}

static ODKU_INFO *
stx_restore_odku_info (THREAD_ENTRY * thread_p, char *ptr)
{
  ODKU_INFO *odku_info = NULL;
  int offset;

  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  odku_info = (ODKU_INFO *) stx_alloc_struct (thread_p, sizeof (ODKU_INFO));
  if (odku_info == NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return NULL;
    }

  /* no_assigns */
  ptr = or_unpack_int (ptr, &odku_info->no_assigns);

  /* attr_ids */
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      odku_info->attr_ids = NULL;
    }
  else
    {
      odku_info->attr_ids =
        stx_restore_int_array (thread_p, &xasl_unpack_info->packed_xasl[offset], odku_info->no_assigns);
      if (odku_info->attr_ids == NULL)
        {
          return NULL;
        }
    }

  /* assignments */
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      odku_info->assignments = NULL;
    }
  else
    {
      odku_info->assignments =
        stx_restore_update_assignment_array (thread_p, &xasl_unpack_info->packed_xasl[offset], odku_info->no_assigns);
      if (odku_info->assignments == NULL)
        {
          return NULL;
        }
    }

  /* constraint predicate */
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      odku_info->cons_pred = NULL;
    }
  else
    {
      odku_info->cons_pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (odku_info->cons_pred == NULL)
        {
          return NULL;
        }
    }

  /* cache attr info */
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      odku_info->attr_info = NULL;
    }
  else
    {
      odku_info->attr_info = stx_restore_cache_attrinfo (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (odku_info->attr_info == NULL)
        {
          return NULL;
        }
    }

  return odku_info;
}

static char *
stx_build_update_proc (THREAD_ENTRY * thread_p, char *ptr, UPDATE_PROC_NODE * update_info)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      stx_set_xasl_errcode (thread_p, ER_GENERIC_ERROR);
      return NULL;
    }
  else
    {
      update_info->class_info = stx_restore_upddel_class_info (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (update_info->class_info == NULL)
        {
          goto error;
        }
    }

  /* assigns */
  ptr = or_unpack_int (ptr, &update_info->no_assigns);
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      update_info->assigns = NULL;
    }
  else
    {
      update_info->assigns =
        stx_restore_update_assignment_array (thread_p, &xasl_unpack_info->packed_xasl[offset], update_info->no_assigns);
      if (update_info->assigns == NULL)
        {
          goto error;
        }
    }

  /* cons_pred */
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      update_info->cons_pred = NULL;
    }
  else
    {
      update_info->cons_pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (update_info->cons_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &(update_info->no_orderby_keys));

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_delete_proc (THREAD_ENTRY * thread_p, char *ptr, DELETE_PROC_NODE * delete_info)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      delete_info->class_info = NULL;
    }
  else
    {
      delete_info->class_info = stx_restore_upddel_class_info (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (delete_info->class_info == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  return ptr;
}

static char *
stx_build_insert_proc (THREAD_ENTRY * thread_p, char *ptr, INSERT_PROC_NODE * insert_info)
{
  int offset;
  int i;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_oid (ptr, &insert_info->class_oid);

  ptr = or_unpack_hfid (ptr, &insert_info->class_hfid);

  ptr = or_unpack_int (ptr, &insert_info->no_vals);

  ptr = or_unpack_int (ptr, &insert_info->no_default_expr);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || insert_info->no_vals == 0)
    {
      insert_info->att_id = NULL;
    }
  else
    {
      insert_info->att_id =
        stx_restore_int_array (thread_p, &xasl_unpack_info->packed_xasl[offset], insert_info->no_vals);
      if (insert_info->att_id == NULL)
        {
          goto error;
        }
    }

  /* Make space for the subquery values. */
  insert_info->vals = (DB_VALUE **) stx_alloc_struct (thread_p, sizeof (DB_VALUE *) * insert_info->no_vals);
  if (insert_info->no_vals)
    {
      if (insert_info->vals == NULL)
        {
          goto error;
        }
      for (i = 0; i < insert_info->no_vals; ++i)
        {
          insert_info->vals[i] = (DB_VALUE *) 0;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      insert_info->cons_pred = NULL;
    }
  else
    {
      insert_info->cons_pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (insert_info->cons_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &insert_info->has_uniques);
  ptr = or_unpack_int (ptr, &insert_info->do_replace);
  ptr = or_unpack_int (ptr, &insert_info->force_page_allocation);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      insert_info->odku = NULL;
    }
  else
    {
      insert_info->odku = stx_restore_odku_info (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (insert_info->odku == NULL)
        {
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &insert_info->no_val_lists);
  if (insert_info->no_val_lists == 0)
    {
      insert_info->valptr_lists = NULL;
    }
  else
    {
      assert (insert_info->no_val_lists > 0);

      insert_info->valptr_lists =
        (OUTPTR_LIST **) stx_alloc_struct (thread_p, sizeof (OUTPTR_LIST *) * insert_info->no_val_lists);
      if (insert_info->valptr_lists == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
      for (i = 0; i < insert_info->no_val_lists; i++)
        {
          ptr = or_unpack_int (ptr, &offset);
          if (ptr == 0)
            {
              assert (0);
              return NULL;
            }
          else
            {
              insert_info->valptr_lists[i] = stx_restore_outptr_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
              if (insert_info->valptr_lists[i] == NULL)
                {
                  return NULL;
                }
            }
        }
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_outptr_list (THREAD_ENTRY * thread_p, char *ptr, OUTPTR_LIST * outptr_list)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &outptr_list->valptr_cnt);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      outptr_list->valptrp = NULL;
    }
  else
    {
      outptr_list->valptrp = stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (outptr_list->valptrp == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  return ptr;
}

static char *
stx_build_pred_expr (THREAD_ENTRY * thread_p, char *ptr, PRED_EXPR * pred_expr)
{
  int tmp, offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &tmp);
  pred_expr->type = (TYPE_PRED_EXPR) tmp;

  switch (pred_expr->type)
    {
    case T_PRED:
      ptr = stx_build_pred (thread_p, ptr, &pred_expr->pe.pred);
      break;

    case T_EVAL_TERM:
      ptr = stx_build_eval_term (thread_p, ptr, &pred_expr->pe.eval_term);
      break;

    case T_NOT_TERM:
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
        {
          pred_expr->pe.not_term = NULL;
        }
      else
        {
          pred_expr->pe.not_term = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
          if (pred_expr->pe.not_term == NULL)
            {
              stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
              return NULL;
            }
        }
      break;

    default:
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  return ptr;
}

static char *
stx_build_pred (THREAD_ENTRY * thread_p, char *ptr, PRED * pred)
{
  int tmp, offset;
  int rhs_type;
  PRED_EXPR *rhs;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  /* lhs */
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      pred->lhs = NULL;
    }
  else
    {
      pred->lhs = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (pred->lhs == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &tmp);
  pred->bool_op = (BOOL_OP) tmp;

  ptr = or_unpack_int (ptr, &rhs_type); /* rhs-type */

  /* Traverse right-linear chains of AND/OR terms */
  while (rhs_type == T_PRED)
    {
      pred->rhs = (PRED_EXPR *) stx_alloc_struct (thread_p, sizeof (PRED_EXPR));
      if (pred->rhs == NULL)
        {
          goto error;
        }

      rhs = pred->rhs;

      rhs->type = T_PRED;

      pred = &rhs->pe.pred;

      /* lhs */
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
        {
          pred->lhs = NULL;
        }
      else
        {
          pred->lhs = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
          if (pred->lhs == NULL)
            {
              goto error;
            }
        }

      ptr = or_unpack_int (ptr, &tmp);
      pred->bool_op = (BOOL_OP) tmp;    /* bool_op */

      ptr = or_unpack_int (ptr, &rhs_type);     /* rhs-type */
    }

  /* rhs */
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      pred->rhs = NULL;
    }
  else
    {
      pred->rhs = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (pred->rhs == NULL)
        {
          goto error;
        }
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_eval_term (THREAD_ENTRY * thread_p, char *ptr, EVAL_TERM * eval_term)
{
  int tmp;

  ptr = or_unpack_int (ptr, &tmp);
  eval_term->et_type = (TYPE_EVAL_TERM) tmp;

  switch (eval_term->et_type)
    {
    case T_COMP_EVAL_TERM:
      ptr = stx_build_comp_eval_term (thread_p, ptr, &eval_term->et.et_comp);
      break;

    case T_ALSM_EVAL_TERM:
      ptr = stx_build_alsm_eval_term (thread_p, ptr, &eval_term->et.et_alsm);
      break;

    case T_LIKE_EVAL_TERM:
      ptr = stx_build_like_eval_term (thread_p, ptr, &eval_term->et.et_like);
      break;

    case T_RLIKE_EVAL_TERM:
      ptr = stx_build_rlike_eval_term (thread_p, ptr, &eval_term->et.et_rlike);
      break;

    default:
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  return ptr;
}

static char *
stx_build_comp_eval_term (THREAD_ENTRY * thread_p, char *ptr, COMP_EVAL_TERM * comp_eval_term)
{
  int tmp, offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      comp_eval_term->comp_lhs = NULL;
    }
  else
    {
      comp_eval_term->comp_lhs = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (comp_eval_term->comp_lhs == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      comp_eval_term->comp_rhs = NULL;
    }
  else
    {
      comp_eval_term->comp_rhs = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (comp_eval_term->comp_rhs == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &tmp);
  comp_eval_term->comp_rel_op = (REL_OP) tmp;

  return ptr;
}

static char *
stx_build_alsm_eval_term (THREAD_ENTRY * thread_p, char *ptr, ALSM_EVAL_TERM * alsm_eval_term)
{
  int tmp, offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      alsm_eval_term->elem = NULL;
    }
  else
    {
      alsm_eval_term->elem = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (alsm_eval_term->elem == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      alsm_eval_term->elemset = NULL;
    }
  else
    {
      alsm_eval_term->elemset = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (alsm_eval_term->elemset == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &tmp);
  alsm_eval_term->eq_flag = (QL_FLAG) tmp;

  ptr = or_unpack_int (ptr, &tmp);
  alsm_eval_term->alsm_rel_op = (REL_OP) tmp;

  return ptr;
}

static char *
stx_build_like_eval_term (THREAD_ENTRY * thread_p, char *ptr, LIKE_EVAL_TERM * like_eval_term)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      like_eval_term->src = NULL;
    }
  else
    {
      like_eval_term->src = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (like_eval_term->src == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      like_eval_term->pattern = NULL;
    }
  else
    {
      like_eval_term->pattern = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (like_eval_term->pattern == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      like_eval_term->esc_char = NULL;
    }
  else
    {
      like_eval_term->esc_char = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (like_eval_term->esc_char == NULL)
        {
          goto error;
        }
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_rlike_eval_term (THREAD_ENTRY * thread_p, char *ptr, RLIKE_EVAL_TERM * rlike_eval_term)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      rlike_eval_term->src = NULL;
    }
  else
    {
      rlike_eval_term->src = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (rlike_eval_term->src == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      rlike_eval_term->pattern = NULL;
    }
  else
    {
      rlike_eval_term->pattern = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (rlike_eval_term->pattern == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      rlike_eval_term->case_sensitive = NULL;
    }
  else
    {
      rlike_eval_term->case_sensitive = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (rlike_eval_term->case_sensitive == NULL)
        {
          goto error;
        }
    }

  /* initialize regex object pointer */
  rlike_eval_term->compiled_regex = NULL;
  rlike_eval_term->compiled_pattern = NULL;

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}


static char *
stx_build_access_spec_type (THREAD_ENTRY * thread_p, char *ptr, ACCESS_SPEC_TYPE * access_spec, UNUSED_ARG void *arg)
{
  int tmp, offset;
  int val;

  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &tmp);
  access_spec->type = (TARGET_TYPE) tmp;

  ptr = or_unpack_int (ptr, &tmp);
  access_spec->access = (ACCESS_METHOD) tmp;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      access_spec->indexptr = NULL;
    }
  else
    {
      access_spec->indexptr = stx_restore_indx_info (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (access_spec->indexptr == NULL)
        {
          goto error;
        }
      /* backup index id */
      access_spec->indx_id = access_spec->indexptr->indx_id;
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      access_spec->where_key = NULL;
    }
  else
    {
      access_spec->where_key = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (access_spec->where_key == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      access_spec->where_pred = NULL;
    }
  else
    {
      access_spec->where_pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (access_spec->where_pred == NULL)
        {
          goto error;
        }
    }

  switch (access_spec->type)
    {
    case TARGET_CLASS:
      ptr = stx_build_cls_spec_type (thread_p, ptr, &ACCESS_SPEC_CLS_SPEC (access_spec));
      break;

    case TARGET_LIST:
      ptr = stx_build_list_spec_type (thread_p, ptr, &ACCESS_SPEC_LIST_SPEC (access_spec));
      break;

    default:
      assert (false);
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  if (ptr == NULL)
    {
      return NULL;
    }

  /* access_spec_type->s_id is not sent to server */
  memset (&access_spec->s_id, '\0', sizeof (SCAN_ID));

  access_spec->s_id.status = S_CLOSED;

  ptr = or_unpack_int (ptr, &access_spec->fixed_scan);
  ptr = or_unpack_int (ptr, &access_spec->qualified_block);

  ptr = or_unpack_int (ptr, &tmp);
  access_spec->fetch_type = (QPROC_FETCH_TYPE) tmp;

  ptr = or_unpack_int (ptr, &val);
  access_spec->flags = val;

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_indx_info (THREAD_ENTRY * thread_p, char *ptr, INDX_INFO * indx_info)
{
  int tmp;

  ptr = stx_build_indx_id (thread_p, ptr, &indx_info->indx_id);
  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_unpack_int (ptr, &indx_info->coverage);

  ptr = or_unpack_int (ptr, &tmp);
  indx_info->range_type = (RANGE_TYPE) tmp;

  ptr = stx_build_key_info (thread_p, ptr, &indx_info->key_info);
  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_unpack_int (ptr, &indx_info->orderby_desc);

  ptr = or_unpack_int (ptr, &indx_info->groupby_desc);

  ptr = or_unpack_int (ptr, &indx_info->use_desc_index);

  ptr = or_unpack_int (ptr, &indx_info->orderby_skip);

  ptr = or_unpack_int (ptr, &indx_info->groupby_skip);

  return ptr;
}

static char *
stx_build_indx_id (THREAD_ENTRY * thread_p, char *ptr, INDX_ID * indx_id)
{
  int tmp;

  ptr = or_unpack_int (ptr, &tmp);
  indx_id->type = (INDX_ID_TYPE) tmp;
  if (ptr == NULL)
    {
      return NULL;
    }

  switch (indx_id->type)
    {
    case T_BTID:
      ptr = or_unpack_btid (ptr, &indx_id->i.btid);
      break;

    default:
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  return ptr;
}

static char *
stx_build_key_info (THREAD_ENTRY * thread_p, char *ptr, KEY_INFO * key_info)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &key_info->key_cnt);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || key_info->key_cnt == 0)
    {
      key_info->key_ranges = NULL;
    }
  else
    {
      key_info->key_ranges =
        stx_restore_key_range_array (thread_p, &xasl_unpack_info->packed_xasl[offset], key_info->key_cnt);
      if (key_info->key_ranges == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &key_info->is_constant);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      key_info->key_limit_l = NULL;
    }
  else
    {
      key_info->key_limit_l = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (key_info->key_limit_l == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      key_info->key_limit_u = NULL;
    }
  else
    {
      key_info->key_limit_u = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (key_info->key_limit_u == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &key_info->key_limit_reset);

  return ptr;
}

static char *
stx_build_cls_spec_type (THREAD_ENTRY * thread_p, char *ptr, CLS_SPEC_TYPE * cls_spec)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_hfid (ptr, &cls_spec->hfid);
  ptr = or_unpack_oid (ptr, &cls_spec->cls_oid);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cls_regu_list_key = NULL;
    }
  else
    {
      cls_spec->cls_regu_list_key = stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cls_regu_list_key == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cls_regu_list_pred = NULL;
    }
  else
    {
      cls_spec->cls_regu_list_pred = stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cls_regu_list_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cls_regu_list_rest = NULL;
    }
  else
    {
      cls_spec->cls_regu_list_rest = stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cls_regu_list_rest == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cls_regu_list_pk_next = NULL;
    }
  else
    {
      cls_spec->cls_regu_list_pk_next =
        stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cls_regu_list_pk_next == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cls_output_val_list = NULL;
    }
  else
    {
      cls_spec->cls_output_val_list = stx_restore_outptr_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cls_output_val_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cls_regu_val_list = NULL;
    }
  else
    {
      cls_spec->cls_regu_val_list = stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cls_regu_val_list == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &cls_spec->num_attrs_key);
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || cls_spec->num_attrs_key == 0)
    {
      cls_spec->attrids_key = NULL;
    }
  else
    {
      cls_spec->attrids_key =
        stx_restore_int_array (thread_p, &xasl_unpack_info->packed_xasl[offset], cls_spec->num_attrs_key);
      if (cls_spec->attrids_key == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cache_key = NULL;
    }
  else
    {
      cls_spec->cache_key = stx_restore_cache_attrinfo (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cache_key == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &cls_spec->num_attrs_pred);
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || cls_spec->num_attrs_pred == 0)
    {
      cls_spec->attrids_pred = NULL;
    }
  else
    {
      cls_spec->attrids_pred =
        stx_restore_int_array (thread_p, &xasl_unpack_info->packed_xasl[offset], cls_spec->num_attrs_pred);
      if (cls_spec->attrids_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cache_pred = NULL;
    }
  else
    {
      cls_spec->cache_pred = stx_restore_cache_attrinfo (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cache_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &cls_spec->num_attrs_rest);
  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0 || cls_spec->num_attrs_rest == 0)
    {
      cls_spec->attrids_rest = NULL;
    }
  else
    {
      cls_spec->attrids_rest =
        stx_restore_int_array (thread_p, &xasl_unpack_info->packed_xasl[offset], cls_spec->num_attrs_rest);
      if (cls_spec->attrids_rest == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      cls_spec->cache_rest = NULL;
    }
  else
    {
      cls_spec->cache_rest = stx_restore_cache_attrinfo (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (cls_spec->cache_rest == NULL)
        {
          goto error;
        }
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_list_spec_type (THREAD_ENTRY * thread_p, char *ptr, LIST_SPEC_TYPE * list_spec_type)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      list_spec_type->xasl_node = NULL;
    }
  else
    {
      list_spec_type->xasl_node = stx_restore_xasl_node (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (list_spec_type->xasl_node == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      list_spec_type->list_regu_list_pred = NULL;
    }
  else
    {
      list_spec_type->list_regu_list_pred =
        stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (list_spec_type->list_regu_list_pred == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      list_spec_type->list_regu_list_rest = NULL;
    }
  else
    {
      list_spec_type->list_regu_list_rest =
        stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (list_spec_type->list_regu_list_rest == NULL)
        {
          goto error;
        }
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_val_list (THREAD_ENTRY * thread_p, char *ptr, VAL_LIST * val_list)
{
  int offset, i;
  QPROC_DB_VALUE_LIST value_list = NULL;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &val_list->val_cnt);

  value_list = (QPROC_DB_VALUE_LIST)
    stx_alloc_struct (thread_p, sizeof (struct qproc_db_value_list) * val_list->val_cnt);
  if (val_list->val_cnt)
    {
      if (value_list == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  for (i = 0; i < val_list->val_cnt; i++)
    {
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
        {
          value_list[i].val = NULL;
        }
      else
        {
          value_list[i].val = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
          if (value_list[i].val == NULL)
            {
              stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
              return NULL;
            }
        }

      if (i < val_list->val_cnt - 1)
        {
          value_list[i].next = (QPROC_DB_VALUE_LIST) & value_list[i + 1];
        }
      else
        {
          value_list[i].next = NULL;
        }
    }

  val_list->valp = value_list;

  return ptr;
}

static char *
stx_build_regu_variable (THREAD_ENTRY * thread_p, char *ptr, REGU_VARIABLE * regu_var)
{
  int tmp, offset;

  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &tmp);
  regu_var->type = (REGU_DATATYPE) tmp;

  ptr = or_unpack_int (ptr, &regu_var->flags);
  assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));
  assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST));

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      regu_var->vfetch_to = NULL;
    }
  else
    {
      regu_var->vfetch_to = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (regu_var->vfetch_to == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      regu_var->xasl = NULL;
    }
  else
    {
      regu_var->xasl = stx_restore_xasl_node (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (regu_var->xasl == NULL)
        {
          goto error;
        }
    }

  ptr = stx_unpack_regu_variable_value (thread_p, ptr, regu_var);

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_unpack_regu_variable_value (THREAD_ENTRY * thread_p, char *ptr, REGU_VARIABLE * regu_var)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  assert (ptr != NULL && regu_var != NULL);

  switch (regu_var->type)
    {
    case TYPE_DBVAL:
      ptr = stx_build_db_value (thread_p, ptr, &regu_var->value.dbval);
      break;

    case TYPE_CONSTANT:
    case TYPE_ORDERBY_NUM:
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
        {
          regu_var->value.dbvalptr = NULL;
        }
      else
        {
          regu_var->value.dbvalptr = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
          if (regu_var->value.dbvalptr == NULL)
            {
              goto error;
            }
        }
      break;

    case TYPE_INARITH:
    case TYPE_OUTARITH:
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
        {
          regu_var->value.arithptr = NULL;
        }
      else
        {
          regu_var->value.arithptr = stx_restore_arith_type (thread_p, &xasl_unpack_info->packed_xasl[offset]);
          if (regu_var->value.arithptr == NULL)
            {
              goto error;
            }
        }
      break;

    case TYPE_FUNC:
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
        {
          regu_var->value.funcp = NULL;
        }
      else
        {
          regu_var->value.funcp = stx_restore_function_type (thread_p, &xasl_unpack_info->packed_xasl[offset]);
          if (regu_var->value.funcp == NULL)
            {
              goto error;
            }
        }
      break;

    case TYPE_ATTR_ID:
      ptr = stx_build_attr_descr (thread_p, ptr, &regu_var->value.attr_descr);
      break;

    case TYPE_LIST_ID:
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
        {
          regu_var->value.srlist_id = NULL;
        }
      else
        {
          regu_var->value.srlist_id = stx_restore_srlist_id (thread_p, &xasl_unpack_info->packed_xasl[offset]);
          if (regu_var->value.srlist_id == NULL)
            {
              goto error;
            }
        }
      break;

    case TYPE_POSITION:
      ptr = stx_build_pos_descr (ptr, &regu_var->value.pos_descr);
      break;

    case TYPE_POS_VALUE:
      ptr = or_unpack_int (ptr, &regu_var->value.val_pos);
      break;

    case TYPE_OID:
      break;

    default:
      stx_set_xasl_errcode (thread_p, ER_QPROC_INVALID_XASLNODE);
      return NULL;
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_attr_descr (THREAD_ENTRY * thread_p, char *ptr, ATTR_DESCR * attr_descr)
{
  int tmp, offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &tmp);
  attr_descr->id = (CL_ATTR_ID) tmp;

  ptr = or_unpack_int (ptr, &tmp);
  attr_descr->type = (DB_TYPE) tmp;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      attr_descr->cache_attrinfo = NULL;
    }
  else
    {
      attr_descr->cache_attrinfo = stx_restore_cache_attrinfo (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (attr_descr->cache_attrinfo == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  attr_descr->cache_dbvalp = NULL;

  return ptr;
}

static char *
stx_build_pos_descr (char *ptr, QFILE_TUPLE_VALUE_POSITION * position_descr)
{
  ptr = or_unpack_int (ptr, &position_descr->pos_no);

  return ptr;
}

static char *
stx_build_db_value (UNUSED_ARG THREAD_ENTRY * thread_p, char *ptr, DB_VALUE * value)
{
  ptr = or_unpack_db_value (ptr, value);

  return ptr;
}

static char *
stx_build_arith_type (THREAD_ENTRY * thread_p, char *ptr, ARITH_TYPE * arith_type)
{
  int tmp, offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      arith_type->value = NULL;
    }
  else
    {
      arith_type->value = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (arith_type->value == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &tmp);
  arith_type->opcode = (OPERATOR_TYPE) tmp;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      arith_type->leftptr = NULL;
    }
  else
    {
      arith_type->leftptr = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (arith_type->leftptr == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      arith_type->rightptr = NULL;
    }
  else
    {
      arith_type->rightptr = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (arith_type->rightptr == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      arith_type->thirdptr = NULL;
    }
  else
    {
      arith_type->thirdptr = stx_restore_regu_variable (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (arith_type->thirdptr == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &tmp);
  arith_type->misc_operand = (MISC_OPERAND) tmp;

  if (arith_type->opcode == T_CASE || arith_type->opcode == T_DECODE
      || arith_type->opcode == T_PREDICATE || arith_type->opcode == T_IF)
    {
      ptr = or_unpack_int (ptr, &offset);
      if (offset == 0)
        {
          arith_type->pred = NULL;
        }
      else
        {
          arith_type->pred = stx_restore_pred_expr (thread_p, &xasl_unpack_info->packed_xasl[offset]);
          if (arith_type->pred == NULL)
            {
              goto error;
            }
        }
    }
  else
    {
      arith_type->pred = NULL;
    }

  /* This member is only used on server internally. */
  arith_type->rand_seed = NULL;

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_aggregate_type (THREAD_ENTRY * thread_p, char *ptr, AGGREGATE_TYPE * aggregate)
{
  int offset;
  int tmp;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      aggregate->accumulator.value = NULL;
    }
  else
    {
      aggregate->accumulator.value = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (aggregate->accumulator.value == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      aggregate->accumulator.value2 = NULL;
    }
  else
    {
      aggregate->accumulator.value2 = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (aggregate->accumulator.value2 == NULL)
        {
          goto error;
        }
    }

  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_unpack_int64 (ptr, &aggregate->accumulator.curr_cnt);

  ptr = stx_build_regu_variable (thread_p, ptr, &aggregate->group_concat_sep);
  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      aggregate->next = NULL;
    }
  else
    {
      aggregate->next = stx_restore_aggregate_type (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (aggregate->next == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, &tmp);
  aggregate->function = (FUNC_TYPE) tmp;

  ptr = or_unpack_int (ptr, &tmp);
  aggregate->option = (QUERY_OPTIONS) tmp;

  ptr = stx_build_regu_variable (thread_p, ptr, &aggregate->operand);
  if (ptr == NULL)
    {
      return NULL;
    }

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      aggregate->list_id = NULL;
    }
  else
    {
      aggregate->list_id = stx_restore_list_id (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (aggregate->list_id == NULL)
        {
          goto error;
        }
    }

  ptr = or_unpack_int (ptr, (int *) &aggregate->flag_agg_optimize);
  ptr = or_unpack_btid (ptr, &aggregate->btid);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      aggregate->sort_list = NULL;
    }
  else
    {
      aggregate->sort_list = stx_restore_sort_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (aggregate->sort_list == NULL)
        {
          goto error;
        }
    }

  return ptr;

error:
  stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
  return NULL;
}

static char *
stx_build_function_type (THREAD_ENTRY * thread_p, char *ptr, FUNCTION_TYPE * function)
{
  int tmp, offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      function->value = NULL;
    }
  else
    {
      function->value = stx_restore_db_value (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (function->value == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = or_unpack_int (ptr, &tmp);
  function->ftype = (FUNC_TYPE) tmp;

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      function->operand = NULL;
    }
  else
    {
      function->operand = stx_restore_regu_variable_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (function->operand == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  return ptr;
}

static char *
stx_build_srlist_id (THREAD_ENTRY * thread_p, char *ptr, QFILE_SORTED_LIST_ID * sort_list_id)
{
  int offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &sort_list_id->sorted);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      sort_list_id->list_id = NULL;
    }
  else
    {
      sort_list_id->list_id = stx_restore_list_id (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (sort_list_id->list_id == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  return ptr;
}

static char *
stx_build_sort_list (THREAD_ENTRY * thread_p, char *ptr, SORT_LIST * sort_list)
{
  int tmp, offset;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  ptr = or_unpack_int (ptr, &offset);
  if (offset == 0)
    {
      sort_list->next = NULL;
    }
  else
    {
      sort_list->next = stx_restore_sort_list (thread_p, &xasl_unpack_info->packed_xasl[offset]);
      if (sort_list->next == NULL)
        {
          stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
          return NULL;
        }
    }

  ptr = stx_build_pos_descr (ptr, &sort_list->pos_descr);
  if (ptr == NULL)
    {
      return NULL;
    }
  ptr = or_unpack_int (ptr, &tmp);
  sort_list->s_order = (SORT_ORDER) tmp;

  ptr = or_unpack_int (ptr, &tmp);
  sort_list->s_nulls = (SORT_NULLS) tmp;

  return ptr;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * init_regu_variable () -
 *   return:
 *   regu(in):    :
 */
static void
stx_init_regu_variable (REGU_VARIABLE * regu)
{
  assert (regu);

  regu->type = TYPE_POS_VALUE;
  regu->flags = 0;
  regu->value.val_pos = 0;
  regu->vfetch_to = NULL;
  REGU_VARIABLE_XASL (regu) = NULL;
}
#endif

/*
 * stx_mark_struct_visited () -
 *   return: if successful, return NO_ERROR, otherwise
 *           ER_FAILED and error code is set to xasl_errcode
 *   ptr(in)    : pointer constant to be marked visited
 *   str(in)    : where the struct pointed by 'ptr' is stored
 *
 * Note: mark the given pointer constant as visited to avoid
 * duplicated storage of a struct which is pointed by more than one node
 */
static int
stx_mark_struct_visited (THREAD_ENTRY * thread_p, const void *ptr, void *str)
{
  int new_lwm;
  int block_no;
#if defined(SERVER_MODE)
  THREAD_ENTRY *thrd;
#else /* SERVER_MODE */
  void *thrd = NULL;
#endif /* !SERVER_MODE */
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  thrd = thread_p;

  block_no = PTR_BLOCK (ptr);
  new_lwm = xasl_unpack_info->ptr_lwm[block_no];

  if (xasl_unpack_info->ptr_max[block_no] == 0)
    {
      xasl_unpack_info->ptr_max[block_no] = START_PTR_PER_BLOCK;
      xasl_unpack_info->ptr_blocks[block_no] = (VISITED_PTR *)
        db_private_alloc (thrd, sizeof (VISITED_PTR) * xasl_unpack_info->ptr_max[block_no]);
    }
  else if (xasl_unpack_info->ptr_max[block_no] <= new_lwm)
    {
      xasl_unpack_info->ptr_max[block_no] *= 2;
      xasl_unpack_info->ptr_blocks[block_no] = (VISITED_PTR *)
        db_private_realloc (thrd, xasl_unpack_info->ptr_blocks[block_no],
                            sizeof (VISITED_PTR) * xasl_unpack_info->ptr_max[block_no]);
    }

  if (xasl_unpack_info->ptr_blocks[block_no] == (VISITED_PTR *) NULL)
    {
      stx_set_xasl_errcode (thread_p, ER_OUT_OF_VIRTUAL_MEMORY);
      return ER_FAILED;
    }

  xasl_unpack_info->ptr_blocks[block_no][new_lwm].ptr = ptr;
  xasl_unpack_info->ptr_blocks[block_no][new_lwm].str = str;

  xasl_unpack_info->ptr_lwm[block_no]++;

  return NO_ERROR;
}

/*
 * stx_get_struct_visited_ptr () -
 *   return: if the ptr is already visited, the offset of
 *           position where the node pointed by 'ptr' is stored,
 *           otherwise, ER_FAILED (xasl_errcode is NOT set)
 *   ptr(in)    : pointer constant to be checked if visited or not
 *
 * Note: check if the node pointed by `ptr` is already stored or
 * not to avoid multiple store of the same node
 */
static void *
stx_get_struct_visited_ptr (THREAD_ENTRY * thread_p, const void *ptr)
{
  int block_no;
  int element_no;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  block_no = PTR_BLOCK (ptr);

  if (xasl_unpack_info->ptr_lwm[block_no] <= 0)
    {
      return NULL;
    }

  for (element_no = 0; element_no < xasl_unpack_info->ptr_lwm[block_no]; element_no++)
    {
      if (ptr == xasl_unpack_info->ptr_blocks[block_no][element_no].ptr)
        {
          return (xasl_unpack_info->ptr_blocks[block_no][element_no].str);
        }
    }

  return NULL;
}

/*
 * stx_free_visited_ptrs () -
 *   return:
 *
 * Note: free memory allocated to manage visited ptr constants
 */
static void
stx_free_visited_ptrs (THREAD_ENTRY * thread_p)
{
  int i;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  for (i = 0; i < MAX_PTR_BLOCKS; i++)
    {
      xasl_unpack_info->ptr_lwm[i] = 0;
      xasl_unpack_info->ptr_max[i] = 0;
      if (xasl_unpack_info->ptr_blocks[i])
        {
          db_private_free_and_init (thread_p, xasl_unpack_info->ptr_blocks[i]);
          xasl_unpack_info->ptr_blocks[i] = (VISITED_PTR *) 0;
        }
    }
}

/*
 * stx_alloc_struct () -
 *   return:
 *   size(in)   : # of bytes of the node
 *
 * Note: allocate storage for structures pointed to from the xasl tree.
 */
static char *
stx_alloc_struct (THREAD_ENTRY * thread_p, int size)
{
  char *ptr;
  XASL_UNPACK_INFO *xasl_unpack_info = stx_get_xasl_unpack_info_ptr (thread_p);

  if (!size)
    {
      return NULL;
    }

  size = MAKE_ALIGN (size);     /* alignment */
  if (size > xasl_unpack_info->alloc_size)
    {                           /* need to alloc */
      int p_size;

      p_size = MAX (size, xasl_unpack_info->packed_size);
      p_size = MAKE_ALIGN (p_size);     /* alignment */
      ptr = db_private_alloc (thread_p, p_size);
      if (ptr == NULL)
        {
          return NULL;          /* error */
        }
      xasl_unpack_info->alloc_size = p_size;
      xasl_unpack_info->alloc_buf = ptr;
    }

  /* consume alloced buffer */
  ptr = xasl_unpack_info->alloc_buf;
  xasl_unpack_info->alloc_size -= size;
  xasl_unpack_info->alloc_buf += size;

  return ptr;
}

/*
 * stx_init_xasl_unpack_info () -
 *   return:
 *   xasl_stream(in)    : pointer to xasl stream
 *   xasl_stream_size(in)       :
 *
 * Note: initialize the xasl pack information.
 */
static int
stx_init_xasl_unpack_info (THREAD_ENTRY * thread_p, char *xasl_stream, int xasl_stream_size)
{
  int n;
#if defined(SERVER_MODE)
  XASL_UNPACK_INFO *xasl_unpack_info;
#endif /* SERVER_MODE */
  int head_offset, body_offset;

#define UNPACK_SCALE 3          /* TODO: assume */

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  head_offset = sizeof (XASL_UNPACK_INFO);
  head_offset = MAKE_ALIGN (head_offset);
  body_offset = xasl_stream_size * UNPACK_SCALE;
  body_offset = MAKE_ALIGN (body_offset);
#if defined(SERVER_MODE)
  xasl_unpack_info = db_private_alloc (thread_p, head_offset + body_offset);
  stx_set_xasl_unpack_info_ptr (thread_p, xasl_unpack_info);
#else /* SERVER_MODE */
  xasl_unpack_info = db_private_alloc (NULL, head_offset + body_offset);
#endif /* SERVER_MODE */
  if (xasl_unpack_info == NULL)
    {
      return ER_FAILED;
    }
  xasl_unpack_info->packed_xasl = xasl_stream;
  xasl_unpack_info->packed_size = xasl_stream_size;
  for (n = 0; n < MAX_PTR_BLOCKS; ++n)
    {
      xasl_unpack_info->ptr_blocks[n] = (VISITED_PTR *) 0;
      xasl_unpack_info->ptr_lwm[n] = 0;
      xasl_unpack_info->ptr_max[n] = 0;
    }
  xasl_unpack_info->alloc_size = xasl_stream_size * UNPACK_SCALE;
  xasl_unpack_info->alloc_buf = (char *) xasl_unpack_info + head_offset;
#if defined (SERVER_MODE)
  xasl_unpack_info->thrd = thread_p;
#endif /* SERVER_MODE */

  return NO_ERROR;
}

/*
 * stx_get_xasl_unpack_info_ptr () -
 *   return:
 */
static XASL_UNPACK_INFO *
stx_get_xasl_unpack_info_ptr (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  return (XASL_UNPACK_INFO *) thread_p->xasl_unpack_info_ptr;
#else /* SERVER_MODE */
  return (XASL_UNPACK_INFO *) xasl_unpack_info;
#endif /* SERVER_MODE */
}

#if defined(SERVER_MODE)
/*
 * stx_set_xasl_unpack_info_ptr () -
 *   return:
 *   ptr(in)    :
 */
static void
stx_set_xasl_unpack_info_ptr (THREAD_ENTRY * thread_p, XASL_UNPACK_INFO * ptr)
{
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  thread_p->xasl_unpack_info_ptr = ptr;
}
#endif /* SERVER_MODE */

/*
 * stx_get_xasl_errcode () -
 *   return:
 */
static int
stx_get_xasl_errcode (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  return thread_p->xasl_errcode;
#else /* SERVER_MODE */
  return stx_Xasl_errcode;
#endif /* SERVER_MODE */
}

/*
 * stx_set_xasl_errcode () -
 *   return:
 *   errcode(in)        :
 */
static void
stx_set_xasl_errcode (UNUSED_ARG THREAD_ENTRY * thread_p, int errcode)
{
  assert (errcode == NO_ERROR); /* TODO - trace */

#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  thread_p->xasl_errcode = errcode;
#else /* SERVER_MODE */
  stx_Xasl_errcode = errcode;
#endif /* SERVER_MODE */
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * stx_unpack_char () -
 *   return:
 *   tmp(in)    :
 *   ptr(in)    :
 */
static char *
stx_unpack_char (char *tmp, char *ptr)
{
  int i;

  tmp = or_unpack_int (tmp, &i);
  *ptr = i;

  return tmp;
}

/*
 * stx_unpack_long () -
 *   return:
 *   tmp(in)    :
 *   ptr(in)    :
 */
static char *
stx_unpack_long (char *tmp, long *ptr)
{
  int i;

  tmp = or_unpack_int (tmp, &i);
  *ptr = i;

  return tmp;
}
#endif

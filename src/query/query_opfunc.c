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
 * query_opfunc.c - The manipulation of data stored in the XASL nodes
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <float.h>
#include <math.h>

#include "system_parameter.h"

#include "error_manager.h"
#include "memory_alloc.h"
#include "object_representation.h"
#include "external_sort.h"
#include "extendible_hash.h"

#include "fetch.h"
#include "list_file.h"
#include "xasl_support.h"
#include "object_primitive.h"
#include "object_domain.h"
#include "set_object.h"
#include "arithmetic.h"
#include "page_buffer.h"

#include "query_executor.h"
#include "databases_file.h"

/* this must be the last header file included!!! */
#include "dbval.h"

static int qdata_coerce_result_to_domain (DB_VALUE * result_p, TP_DOMAIN * domain_p);

static int qdata_process_distinct_or_sort (THREAD_ENTRY * thread_p, AGGREGATE_TYPE * agg_p, QUERY_ID query_id);

static int qdata_convert_dbvals_to_set (THREAD_ENTRY * thread_p,
                                        DB_TYPE stype,
                                        REGU_VARIABLE * func,
                                        VAL_DESCR * val_desc_p, OID * obj_oid_p, QFILE_TUPLE tuple);

static int qdata_group_concat_first_value (THREAD_ENTRY * thread_p, AGGREGATE_TYPE * agg_p, DB_VALUE * dbvalue);

static int qdata_group_concat_value (THREAD_ENTRY * thread_p,
                                     AGGREGATE_TYPE * agg_p, VAL_DESCR * val_desc_p, DB_VALUE * dbvalue);

static int qdata_insert_substring_function (THREAD_ENTRY * thread_p,
                                            FUNCTION_TYPE * function_p,
                                            VAL_DESCR * val_desc_p, OID * obj_oid_p, QFILE_TUPLE tuple);

static int qdata_elt (THREAD_ENTRY * thread_p, FUNCTION_TYPE * function_p,
                      VAL_DESCR * val_desc_p, OID * obj_oid_p, QFILE_TUPLE tuple);

static int qdata_update_agg_interpolate_func_value_and_domain (AGGREGATE_TYPE * agg_p, DB_VALUE * val);

/*
 * qdata_set_value_list_to_null () -
 *   return:
 *   val_list(in)       : Value List
 *
 * Note: Set all db_values on the value list to null.
 */
void
qdata_set_value_list_to_null (VAL_LIST * val_list_p)
{
  QPROC_DB_VALUE_LIST db_val_list;

  if (val_list_p == NULL)
    {
      return;
    }

  db_val_list = val_list_p->valp;
  while (db_val_list)
    {
      pr_clear_value (db_val_list->val);
      db_val_list = db_val_list->next;
    }
}

/*
 * COPY ROUTINES
 */

/*
 * qdata_copy_db_value () -
 *   return: int (true on success, false on failure)
 *   dbval1(in) : Destination db_value node
 *   dbval2(in) : Source db_value node
 *
 * Note: Copy source value to destination value.
 */
int
qdata_copy_db_value (DB_VALUE * dest_p, DB_VALUE * src_p)
{
  PR_TYPE *pr_type_p;
  DB_TYPE src_type;

  assert (dest_p != NULL);
  assert (src_p != NULL);
  assert (dest_p != src_p);

  /* check if there is nothing to do, so we don't clobber
   * a db_value if we happen to try to copy it to itself
   */
  if (dest_p == src_p)
    {
      return true;
    }

  /* clear any value from a previous iteration */
  pr_clear_value (dest_p);

  if (DB_IS_NULL (src_p))
    {
      db_make_null (dest_p);
    }
  else
    {
      src_type = DB_VALUE_DOMAIN_TYPE (src_p);
      pr_type_p = PR_TYPE_FROM_ID (src_type);
      if (pr_type_p == NULL)
        {
          return false;
        }

      assert (DB_IS_NULL (dest_p));
      (*(pr_type_p->setval)) (dest_p, src_p, true);

#if !defined(NDEBUG)
      assert (!DB_IS_NULL (dest_p));

      switch (pr_type_p->id)
        {
        case DB_TYPE_VARCHAR:
        case DB_TYPE_VARBIT:
          assert (dest_p->need_clear == true);
          break;

        default:
          break;
        }
#endif
    }

  return true;
}

/*
 * qdata_copy_db_value_to_tuple_value () -
 *   return: int (true on success, false on failure)
 *   dbval(in)  : Source dbval node
 *   tvalp(in)  :  Tuple value
 *   tval_size(out)      : Set to the tuple value size
 *
 * Note: Copy an db_value to an tuple value.
 * THIS ROUTINE ASSUMES THAT THE VALUE WILL FIT IN THE TPL!!!!
 */
int
qdata_copy_db_value_to_tuple_value (DB_VALUE * dbval_p, char *tuple_val_p, int *tuple_val_size)
{
  char *val_p;
  int val_size, align;
  OR_BUF buf;
  PR_TYPE *pr_type;
  DB_TYPE dbval_type;

  if (DB_IS_NULL (dbval_p))
    {
      QFILE_PUT_TUPLE_VALUE_FLAG (tuple_val_p, V_UNBOUND);
      QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_val_p, 0);
      *tuple_val_size = QFILE_TUPLE_VALUE_HEADER_SIZE;
    }
  else
    {
      QFILE_PUT_TUPLE_VALUE_FLAG (tuple_val_p, V_BOUND);
      val_p = (char *) tuple_val_p + QFILE_TUPLE_VALUE_HEADER_SIZE;

      dbval_type = DB_VALUE_DOMAIN_TYPE (dbval_p);
      pr_type = PR_TYPE_FROM_ID (dbval_type);

      val_size = pr_data_writeval_disk_size (dbval_p);

      OR_BUF_INIT (buf, val_p, val_size);

      if (pr_type == NULL || (*(pr_type->data_writeval)) (&buf, dbval_p) != NO_ERROR)
        {
          return ER_FAILED;
        }

      /* I don't know if the following is still true. */
      /* since each tuple data value field is already aligned with
       * MAX_ALIGNMENT, val_size by itself can be used to find the maximum
       * alignment for the following field which is next val_header
       */

      align = DB_ALIGN (val_size, MAX_ALIGNMENT);       /* to align for the next field */
      *tuple_val_size = QFILE_TUPLE_VALUE_HEADER_SIZE + align;
      QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_val_p, align);

#if !defined(NDEBUG)
      /* suppress valgrind UMW error */
      memset (tuple_val_p + QFILE_TUPLE_VALUE_HEADER_SIZE + val_size, 0, align - val_size);
#endif
    }

  return NO_ERROR;
}

/*
 * qdata_copy_valptr_list_to_tuple () -
 *   return: NO_ERROR, or ER_code
 *   list_id_p(in)    :
 *   valptr_list_p(in)    : Value pointer list
 *   val_desc_p(in)     : Value descriptor
 *   tuple_record_p(in) : Tuple descriptor
 *
 * Note: Copy valptr_list values to tuple descriptor.  Regu variables
 * that are hidden columns are not copied to the list file tuple
 */
int
qdata_copy_valptr_list_to_tuple (THREAD_ENTRY * thread_p,
                                 QFILE_LIST_ID * list_id_p,
                                 VALPTR_LIST * valptr_list_p,
                                 VAL_DESCR * val_desc_p, QFILE_TUPLE_RECORD * tuple_record_p)
{
  REGU_VARIABLE_LIST reg_var_p;
  DB_VALUE *dbval_p;
  DB_TYPE val_type;
  TP_DOMAIN *val_dom;
  char *tuple_p;
  int k, count, tval_size, tlen, tpl_size;
  int n_size, toffset;

  assert (list_id_p != NULL);

  tpl_size = 0;
  tlen = QFILE_TUPLE_LENGTH_SIZE;
  toffset = 0;                  /* tuple offset position */

  /* skip the length of the tuple, we'll fill it in after we know what it is */
  tuple_p = (char *) (tuple_record_p->tpl) + tlen;
  toffset += tlen;

  /* copy each value into the tuple */
  count = 0;
  reg_var_p = valptr_list_p->valptrp;
  for (k = 0; k < valptr_list_p->valptr_cnt; k++, reg_var_p = reg_var_p->next)
    {
      if (REGU_VARIABLE_IS_FLAGED (&reg_var_p->value, REGU_VARIABLE_HIDDEN_COLUMN))
        {
          continue;             /* skip hidden cols */
        }

      if (fetch_peek_dbval (thread_p, &reg_var_p->value, val_desc_p, NULL, NULL, &dbval_p) != NO_ERROR)
        {
          goto exit_on_error;
        }

      if (dbval_p == NULL)
        {
          goto exit_on_error;
        }

      /* set result list file domain
       */

      if (count >= list_id_p->type_list.type_cnt)
        {
          assert (false);
          goto exit_on_error;
        }

      val_type = DB_VALUE_DOMAIN_TYPE (dbval_p);
      assert (DB_IS_NULL (dbval_p) || val_type != DB_TYPE_VARIABLE);

      if (TP_DOMAIN_TYPE (list_id_p->type_list.domp[count]) == DB_TYPE_VARIABLE)
        {
          if (DB_IS_NULL (dbval_p) || val_type == DB_TYPE_NULL || val_type == DB_TYPE_VARIABLE)
            {
              /* In this case, we cannot resolve the value's domain.
               * We will try to do for the next tuple.
               */
              ;
            }
          else
            {
              val_dom = tp_domain_resolve_value (dbval_p);
              if (val_dom == NULL)
                {
                  assert (false);
                  goto exit_on_error;
                }

              list_id_p->type_list.domp[count] = val_dom;
            }
        }
      else
        {
          if (qdata_coerce_result_to_domain (dbval_p, list_id_p->type_list.domp[count]) != NO_ERROR)
            {
              goto exit_on_error;
            }
        }

      count++;

      n_size = qdata_get_tuple_value_size_from_dbval (dbval_p);
      if (n_size == ER_FAILED)
        {
          goto exit_on_error;
        }

      if ((tuple_record_p->size - toffset) < n_size)
        {
          /* no space left in tuple to put next item, increase the tuple size
           * by the max of n_size and DB_PAGE_SIZE since we can't compute the
           * actual tuple size without re-evaluating the expressions.  This
           * guarantees that we can at least get the next value into the tuple.
           */
          tpl_size = MAX (tuple_record_p->size, QFILE_TUPLE_LENGTH_SIZE);
          tpl_size += MAX (n_size, DB_PAGESIZE);
          if (tuple_record_p->size == 0)
            {
              tuple_record_p->tpl = (char *) malloc (tpl_size);
              if (tuple_record_p->tpl == NULL)
                {
                  goto exit_on_error;
                }
            }
          else
            {
              tuple_record_p->tpl = (char *) realloc (tuple_record_p->tpl, tpl_size);
              if (tuple_record_p->tpl == NULL)
                {
                  goto exit_on_error;
                }
            }

          tuple_record_p->size = tpl_size;
          tuple_p = (char *) (tuple_record_p->tpl) + toffset;
        }

      if (qdata_copy_db_value_to_tuple_value (dbval_p, tuple_p, &tval_size) != NO_ERROR)
        {
          return ER_FAILED;
        }

      tlen += tval_size;
      tuple_p += tval_size;
      toffset += tval_size;
    }

  /* now that we know the tuple size, set it. */
  QFILE_PUT_TUPLE_LENGTH (tuple_record_p->tpl, tlen);

  return NO_ERROR;

exit_on_error:

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
    }

  return ER_FAILED;
}

/*
 * qdata_generate_tuple_desc_for_valptr_list () -
 *   return: QPROC_TPLDESCR_SUCCESS on success or
 *           QP_TPLDESCR_RETRY_xxx,
 *           QPROC_TPLDESCR_FAILURE
 *   list_id_p(in)    :
 *   valptr_list(in)    : Value pointer list
 *   vd(in)     : Value descriptor
 *   tdp(in)    : Tuple descriptor
 *
 * Note: Generate tuple descriptor for given valptr_list values.
 * Regu variables that are hidden columns are not copied
 * to the list file tuple
 */
QPROC_TPLDESCR_STATUS
qdata_generate_tuple_desc_for_valptr_list (THREAD_ENTRY * thread_p,
                                           QFILE_LIST_ID * list_id_p,
                                           VALPTR_LIST * valptr_list_p,
                                           VAL_DESCR * val_desc_p, QFILE_TUPLE_DESCRIPTOR * tuple_desc_p)
{
  REGU_VARIABLE_LIST reg_var_p;
  DB_VALUE *dbval_p;
  DB_TYPE val_type;
  TP_DOMAIN *val_dom;
  int k, count;
  int value_size;
  QPROC_TPLDESCR_STATUS status = QPROC_TPLDESCR_SUCCESS;

  assert (list_id_p != NULL);

  tuple_desc_p->tpl_size = QFILE_TUPLE_LENGTH_SIZE;     /* set tuple size as header size */
  tuple_desc_p->f_cnt = 0;

  /* copy each value pointer into the each tdp field */
  count = 0;
  reg_var_p = valptr_list_p->valptrp;
  for (k = 0; k < valptr_list_p->valptr_cnt; k++, reg_var_p = reg_var_p->next)
    {
      if (REGU_VARIABLE_IS_FLAGED (&reg_var_p->value, REGU_VARIABLE_HIDDEN_COLUMN))
        {
          continue;             /* skip hidden cols */
        }

      if (fetch_peek_dbval (thread_p, &reg_var_p->value, val_desc_p, NULL, NULL, &dbval_p) != NO_ERROR)
        {
          status = QPROC_TPLDESCR_FAILURE;
          goto exit_with_status;
        }

      if (dbval_p == NULL)
        {
          status = QPROC_TPLDESCR_FAILURE;
          goto exit_with_status;
        }

      /* set result list file domain
       */

      if (count >= list_id_p->type_list.type_cnt)
        {
          assert (false);
          status = QPROC_TPLDESCR_FAILURE;
          goto exit_with_status;
        }

      val_type = DB_VALUE_DOMAIN_TYPE (dbval_p);
      assert (DB_IS_NULL (dbval_p) || val_type != DB_TYPE_VARIABLE);

      if (TP_DOMAIN_TYPE (list_id_p->type_list.domp[count]) == DB_TYPE_VARIABLE)
        {
          if (DB_IS_NULL (dbval_p) || val_type == DB_TYPE_NULL || val_type == DB_TYPE_VARIABLE)
            {
              /* In this case, we cannot resolve the value's domain.
               * We will try to do for the next tuple.
               */
              ;
            }
          else
            {
              val_dom = tp_domain_resolve_value (dbval_p);
              if (val_dom == NULL)
                {
                  assert (false);
                  status = QPROC_TPLDESCR_FAILURE;
                  goto exit_with_status;
                }

              list_id_p->type_list.domp[count] = val_dom;
            }
        }
      else
        {
          if (qdata_coerce_result_to_domain (dbval_p, list_id_p->type_list.domp[count]) != NO_ERROR)
            {
              status = QPROC_TPLDESCR_FAILURE;
              goto exit_with_status;
            }
        }

      count++;

      tuple_desc_p->f_valp[tuple_desc_p->f_cnt] = dbval_p;

      /* SET data-type cannot use tuple descriptor */
      if (pr_is_set_type (DB_VALUE_DOMAIN_TYPE (dbval_p)))
        {
          status = QPROC_TPLDESCR_RETRY_SET_TYPE;
          goto exit_with_status;
        }

      /* add aligned field size to tuple size */
      value_size = qdata_get_tuple_value_size_from_dbval (dbval_p);
      if (value_size == ER_FAILED)
        {
          status = QPROC_TPLDESCR_FAILURE;
          goto exit_with_status;
        }
      tuple_desc_p->tpl_size += value_size;
      tuple_desc_p->f_cnt += 1; /* increase field number */
    }

  /* BIG RECORD cannot use tuple descriptor */
  if (tuple_desc_p->tpl_size >= QFILE_MAX_TUPLE_SIZE_IN_PAGE)
    {
      status = QPROC_TPLDESCR_RETRY_BIG_REC;
      goto exit_with_status;
    }

  assert (status == QPROC_TPLDESCR_SUCCESS);

  return status;

exit_with_status:

  return status;
}

/*
 * qdata_set_valptr_list_unbound () -
 *   return: NO_ERROR, or ER_code
 *   valptr_list(in)    : Value pointer list
 *   vd(in)     : Value descriptor
 *
 * Note: Set valptr_list values UNBOUND.
 */
int
qdata_set_valptr_list_unbound (THREAD_ENTRY * thread_p, VALPTR_LIST * valptr_list_p, VAL_DESCR * val_desc_p)
{
  REGU_VARIABLE_LIST reg_var_p;
  DB_VALUE *dbval_p;
  int i;

  reg_var_p = valptr_list_p->valptrp;
  for (i = 0; i < valptr_list_p->valptr_cnt; i++)
    {
      if (fetch_peek_dbval (thread_p, &reg_var_p->value, val_desc_p, NULL, NULL, &dbval_p) != NO_ERROR)
        {
          return ER_FAILED;
        }

      if (dbval_p != NULL)
        {
          if (db_value_domain_init (dbval_p, DB_VALUE_DOMAIN_TYPE (dbval_p),
                                    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE) != NO_ERROR)
            {
              return ER_FAILED;
            }
        }

      reg_var_p = reg_var_p->next;
    }

  return NO_ERROR;
}

/*
 * ARITHMETIC EXPRESSION EVALUATION ROUTINES
 */

static int
qdata_coerce_result_to_domain (DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_TYPE res_type, dom_type;

  assert (result_p != NULL);
  assert (domain_p != NULL);

  if (result_p == NULL || domain_p == NULL)
    {
      return ER_FAILED;
    }

  res_type = DB_VALUE_DOMAIN_TYPE (result_p);
  dom_type = TP_DOMAIN_TYPE (domain_p);

  if (res_type == DB_TYPE_OID)
    {
      if (dom_type != DB_TYPE_OBJECT)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                  pr_type_name (res_type), pr_type_name (dom_type));
          return ER_TP_CANT_COERCE;
        }
    }
#if 1
  else if (TP_IS_CHAR_TYPE (res_type) && TP_IS_CHAR_TYPE (dom_type))
    {
      int common_coll = -1;

      /* check collation */
      LANG_RT_COMMON_COLL (DB_GET_STRING_COLLATION (result_p), TP_DOMAIN_COLLATION (domain_p), common_coll);
      if (common_coll == -1)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QSTR_INCOMPATIBLE_COLLATIONS, 0);

          return ER_QSTR_INCOMPATIBLE_COLLATIONS;
        }
    }
#endif
  else if (res_type != dom_type || (res_type == DB_TYPE_NUMERIC
#if 0                           /* TODO - refer qfile_unify_types() */
                                    && (result_p->domain.numeric_info.scale != domain_p->scale)
#else
                                    && (result_p->domain.numeric_info.precision != domain_p->precision
                                        || result_p->domain.numeric_info.scale != domain_p->scale)
#endif
           ))
    {
      if (tp_value_coerce (result_p, result_p, domain_p) != DOMAIN_COMPATIBLE)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                  pr_type_name (res_type), pr_type_name (dom_type));
          return ER_TP_CANT_COERCE;
        }
    }

  return NO_ERROR;
}

/*
 * qdata_concatenate_dbval () -
 *   return: NO_ERROR, or ER_code
 *   dbval1(in)		  : First db_value node
 *   dbval2(in)		  : Second db_value node
 *   result_p(out)	  : Resultant db_value node
 *   max_allowed_size(in) : max allowed size for result
 *   warning_context(in)  : used only to display truncation warning context
 *
 * Note: Concatenates a db_values to string db value.
 *	 Value to be added is truncated in case the allowed size would be
 *	 exceeded . Truncation is done without modifying the value (a new
 *	 temporary value is used).
 *	 A warning is logged the first time the allowed size is exceeded
 *	 (when the value to add has already exceeded the size, no warning is
 *	 logged).
 */
int
qdata_concatenate_dbval (UNUSED_ARG THREAD_ENTRY * thread_p,
                         DB_VALUE * dbval1_p,
                         DB_VALUE * dbval2_p, DB_VALUE * result_p,
                         const int max_allowed_size, const char *warning_context)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1, tmp_dbval2, tmp_dbval3;
  DB_TYPE dbval1_type, dbval2_type;
  int res_size = 0, val_size = 0;
  bool warning_size_exceeded = false;
  int spare_bytes = 0;

  dbval1_type = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  dbval2_type = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_dbval1, &tmp_dbval2, &tmp_dbval3);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, dbval1_p, dbval2_p))
    {
      goto done;
    }

  dbval1_p = db_value_cast_arg (dbval1_p, &tmp_dbval1, DB_TYPE_VARCHAR, &error_status);
  dbval2_p = db_value_cast_arg (dbval2_p, &tmp_dbval2, DB_TYPE_VARCHAR, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  dbval1_type = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  dbval2_type = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  assert (dbval1_type == DB_TYPE_VARCHAR);
  assert (dbval2_type == DB_TYPE_VARCHAR);

  res_size = DB_GET_STRING_SIZE (dbval1_p);
  val_size = DB_GET_STRING_SIZE (dbval2_p);
  if (res_size >= max_allowed_size)
    {
      assert (warning_size_exceeded == false);
    }
  else if (res_size + val_size > max_allowed_size)
    {
      warning_size_exceeded = true;
      error_status = db_string_limit_size_string (dbval2_p, &tmp_dbval3, max_allowed_size - res_size, &spare_bytes);
      if (error_status != NO_ERROR)
        {
          assert (er_errid () != NO_ERROR);
          goto exit_on_error;
        }

      error_status = qdata_add_chars_to_dbval (dbval1_p, &tmp_dbval3, result_p);

      if (spare_bytes > 0)
        {
          /* The adjusted 'tmp_dbval3' string was truncated to the last full
           * multibyte character.
           * Increase the 'result' with 'spare_bytes' remained from the
           * last truncated multibyte character.
           * This prevents GROUP_CONCAT to add other single-byte chars
           * (or char with fewer bytes than 'spare_bytes' to current
           * aggregate.
           */
          qstr_make_typed_string (DB_VALUE_DOMAIN_TYPE (result_p),
                                  result_p, DB_VALUE_PRECISION (result_p),
                                  DB_PULL_STRING (result_p),
                                  DB_GET_STRING_SIZE (result_p) + spare_bytes, DB_GET_STRING_COLLATION (dbval1_p));
        }
    }
  else
    {
      error_status = qdata_add_chars_to_dbval (dbval1_p, dbval2_p, result_p);
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  if (warning_size_exceeded == true)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_QPROC_SIZE_STRING_TRUNCATED, 1, warning_context);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_dbval1, &tmp_dbval2, &tmp_dbval3);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_dbval1, &tmp_dbval2, &tmp_dbval3);

  return error_status;
}

/*
 * Aggregate Expression Evaluation Routines
 */

static int
qdata_process_distinct_or_sort (THREAD_ENTRY * thread_p, AGGREGATE_TYPE * agg_p, QUERY_ID query_id)
{
  QFILE_TUPLE_VALUE_TYPE_LIST type_list;
  QFILE_LIST_ID *list_id_p;
  int ls_flag = QFILE_FLAG_DISTINCT;

  /* since max(distinct a) == max(a), handle these without distinct
     processing */
  if (agg_p->function == PT_MAX || agg_p->function == PT_MIN)
    {
      agg_p->option = Q_ALL;
      return NO_ERROR;
    }

  type_list.type_cnt = 1;
  type_list.domp = (TP_DOMAIN **) malloc (sizeof (TP_DOMAIN *));

  if (type_list.domp == NULL)
    {
      return ER_FAILED;
    }

  type_list.domp[0] = tp_domain_resolve_default (DB_TYPE_VARIABLE);

  /* if the agg has ORDER BY force setting 'QFILE_FLAG_ALL' :
   * in this case, no additional SORT_LIST will be created, but the one
   * in the AGGREGATE_TYPE structure will be used */
  if (agg_p->sort_list != NULL)
    {
      ls_flag = QFILE_FLAG_ALL;
    }
  list_id_p = qfile_open_list (thread_p, &type_list, NULL, query_id, ls_flag);

  if (list_id_p == NULL)
    {
      free_and_init (type_list.domp);
      return ER_FAILED;
    }

  free_and_init (type_list.domp);

  qfile_close_list (thread_p, agg_p->list_id);
  qfile_destroy_list (thread_p, agg_p->list_id);

  if (qfile_copy_list_id (agg_p->list_id, list_id_p, true) != NO_ERROR)
    {
      QFILE_FREE_AND_INIT_LIST_ID (list_id_p);
      return ER_FAILED;
    }

  QFILE_FREE_AND_INIT_LIST_ID (list_id_p);

  return NO_ERROR;
}

/*
 * qdata_initialize_aggregate_list () -
 *   return: NO_ERROR, or ER_code
 *   agg_list(in)       : Aggregate expression node list
 *   query_id(in)       : Associated query id
 *
 * Note: Initialize the aggregate expression list.
 */
int
qdata_initialize_aggregate_list (THREAD_ENTRY * thread_p, AGGREGATE_TYPE * agg_list_p, QUERY_ID query_id)
{
  AGGREGATE_TYPE *agg_p;

  for (agg_p = agg_list_p; agg_p != NULL; agg_p = agg_p->next)
    {

      /* the value of groupby_num() remains unchanged;
         it will be changed while evaluating groupby_num predicates
         against each group at 'xs_eval_grbynum_pred()' */
      if (agg_p->function == PT_GROUPBY_NUM)
        {
          /* nothing to do with groupby_num() */
          continue;
        }

      agg_p->accumulator.curr_cnt = 0;

      assert (DB_IS_NULL (agg_p->accumulator.value));
      if (db_value_domain_init (agg_p->accumulator.value,
                                DB_VALUE_DOMAIN_TYPE (agg_p->accumulator.value),
                                DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE) != NO_ERROR)
        {
          return ER_FAILED;
        }

      /* This set is made, because if class is empty, aggregate
       * results should return NULL, except count(*) and count
       */
      if (agg_p->function == PT_COUNT_STAR || agg_p->function == PT_COUNT)
        {
          DB_MAKE_BIGINT (agg_p->accumulator.value, 0);
        }

      /* create temporary list file to handle distincts */
      if (agg_p->option == Q_DISTINCT || agg_p->sort_list != NULL)
        {
          if (qdata_process_distinct_or_sort (thread_p, agg_p, query_id) != NO_ERROR)
            {
              return ER_FAILED;
            }
        }

    }

  return NO_ERROR;
}

/*
 * qdata_aggregate_value_to_accumulator () - aggregate a value to accumulator
 *   returns: error code or NO_ERROR
 *   thread_p(in): thread
 *   acc(in): accumulator
 *   func_type(in): function type
 *   value(in): value
 */
int
qdata_aggregate_value_to_accumulator (UNUSED_ARG THREAD_ENTRY * thread_p,
                                      AGGREGATE_ACCUMULATOR * acc, FUNC_TYPE func_type, DB_VALUE * value)
{
  int rc = DB_UNK;
  bool can_compare = false;
  DB_TYPE val_type;
  TP_DOMAIN *double_domain_ptr = tp_domain_resolve_default (DB_TYPE_DOUBLE);
  DB_VALUE add_val;
  DB_VALUE squared;
  bool copy_operator = false;

  if (DB_IS_NULL (value))
    {
      return NO_ERROR;
    }

  DB_MAKE_NULL (&add_val);
  DB_MAKE_NULL (&squared);

  /* aggregate new value */
  switch (func_type)
    {
    case PT_MIN:
      if (acc->curr_cnt < 1)
        {
          /* we have new minimum */
          copy_operator = true;
        }
      else
        {
          rc = tp_value_compare (value, acc->value, 1, 1, &can_compare);
          if (!can_compare)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                      pr_type_name (DB_VALUE_TYPE (value)), pr_type_name (DB_VALUE_TYPE (acc->value)));
              return ER_FAILED; /* can't compare */
            }

          if (rc == DB_LT)
            {
              /* we have new minimum */
              copy_operator = true;
            }
          else if (!(rc == DB_GT || rc == DB_EQ))
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                      pr_type_name (DB_VALUE_TYPE (value)), pr_type_name (DB_VALUE_TYPE (acc->value)));
              return ER_FAILED; /* can't compare */
            }
        }
      break;

    case PT_MAX:
      if (acc->curr_cnt < 1)
        {
          /* we have new maximum */
          copy_operator = true;
        }
      else
        {
          rc = tp_value_compare (value, acc->value, 1, 1, &can_compare);
          if (!can_compare)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                      pr_type_name (DB_VALUE_TYPE (value)), pr_type_name (DB_VALUE_TYPE (acc->value)));
              return ER_FAILED; /* can't compare */
            }

          if (rc == DB_GT)
            {
              /* we have new maximum */
              copy_operator = true;
            }
          else if (!(rc == DB_LT || rc == DB_EQ))
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                      pr_type_name (DB_VALUE_TYPE (value)), pr_type_name (DB_VALUE_TYPE (acc->value)));
              return ER_FAILED; /* can't compare */
            }
        }
      break;

    case PT_COUNT:
      if (acc->curr_cnt < 1)
        {
          /* first value */
          assert (DB_VALUE_DOMAIN_TYPE (acc->value) == DB_TYPE_BIGINT);
          (void) pr_clear_value (acc->value);
          DB_MAKE_BIGINT (acc->value, 1);
        }
      else
        {
          /* increment */
          assert (DB_VALUE_DOMAIN_TYPE (acc->value) == DB_TYPE_BIGINT);
          DB_MAKE_BIGINT (acc->value, DB_GET_BIGINT (acc->value) + 1);
        }
      break;

    case PT_AVG:
    case PT_SUM:
      /* for SUM
       *   Return Type: bigint for int arguments,
       *                numeric for bigint arguments,
       *                otherwise the same as the argument data type
       */
      val_type = DB_VALUE_DOMAIN_TYPE (value);
      if (val_type == DB_TYPE_INTEGER)
        {
          if (tp_value_coerce (value, value, tp_domain_resolve_default (DB_TYPE_BIGINT)) != DOMAIN_COMPATIBLE)
            {
              return ER_FAILED;
            }
        }
      else if (val_type == DB_TYPE_BIGINT)
        {
          if (tp_value_coerce (value, value, tp_domain_resolve_default (DB_TYPE_NUMERIC)) != DOMAIN_COMPATIBLE)
            {
              return ER_FAILED;
            }
        }

      if (acc->curr_cnt < 1)
        {
          copy_operator = true;
        }
      else
        {
          (void) pr_clear_value (&add_val);

          /* values are added up in acc.value */
          if (qdata_add_dbval (acc->value, value, &add_val) != NO_ERROR)
            {
              return ER_FAILED;
            }

          /* check for add success */
          if (!DB_IS_NULL (&add_val))
            {
              (void) pr_clear_value (acc->value);
              pr_clone_value (&add_val, acc->value);
            }

          (void) pr_clear_value (&add_val);
        }
      break;

    case PT_STDDEV:
    case PT_STDDEV_POP:
    case PT_STDDEV_SAMP:
    case PT_VARIANCE:
    case PT_VAR_POP:
    case PT_VAR_SAMP:
      /* coerce value to DOUBLE domain */
      if (tp_value_coerce (value, value, double_domain_ptr) != DOMAIN_COMPATIBLE)
        {
          return ER_FAILED;
        }

      if (acc->curr_cnt < 1)
        {
          /* calculate X^2 */
          if (qdata_multiply_dbval (value, value, &squared) != NO_ERROR)
            {
              return ER_FAILED;
            }

          /* clear values */
          (void) pr_clear_value (acc->value);
          (void) pr_clear_value (acc->value2);

          /* set values */
          pr_clone_value (value, acc->value);
          pr_clone_value (&squared, acc->value2);
        }
      else
        {
          /* compute X^2 */
          if (qdata_multiply_dbval (value, value, &squared) != NO_ERROR)
            {
              return ER_FAILED;
            }

          /* acc.value += X */
          (void) pr_clear_value (&add_val);

          if (qdata_add_dbval (acc->value, value, &add_val) != NO_ERROR)
            {
              pr_clear_value (&squared);
              return ER_FAILED;
            }

          /* check for add success */
          if (!DB_IS_NULL (&add_val))
            {
              (void) pr_clear_value (acc->value);
              pr_clone_value (&add_val, acc->value);
            }

          (void) pr_clear_value (&add_val);

          /* acc.value2 += X^2 */
          (void) pr_clear_value (&add_val);

          if (qdata_add_dbval (acc->value2, &squared, &add_val) != NO_ERROR)
            {
              pr_clear_value (&squared);
              return ER_FAILED;
            }

          /* check for add success */
          if (!DB_IS_NULL (&add_val))
            {
              (void) pr_clear_value (acc->value2);
              pr_clone_value (&add_val, acc->value2);
            }

          (void) pr_clear_value (&add_val);

          /* done with squared */
          pr_clear_value (&squared);
        }
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      return ER_FAILED;
    }

  /* copy operator if necessary */
  if (copy_operator)
    {
      assert (DB_IS_NULL (acc->value)
              || !TP_IS_CHAR_BIT_TYPE (DB_VALUE_DOMAIN_TYPE (acc->value)) || acc->value->need_clear);

      (void) pr_clear_value (acc->value);

      pr_clone_value (value, acc->value);
    }

  /* clear value and exit nicely */
  return NO_ERROR;
}

/*
 * qdata_evaluate_aggregate_list () -
 *   return: NO_ERROR, or ER_code
 *   agg_list(in): aggregate expression node list
 *   val_desc_p(in): value descriptor
 *
 * Note: Evaluate given aggregate expression list.
 * Note2: If alt_acc_list is not provided, default accumulators will be used.
 *        Alternate accumulators can not be used for DISTINCT processing or
 *        the GROUP_CONCAT and MEDIAN function.
 */
int
qdata_evaluate_aggregate_list (THREAD_ENTRY * thread_p, AGGREGATE_TYPE * agg_list_p, VAL_DESCR * val_desc_p)
{
  int error = NO_ERROR;
  AGGREGATE_TYPE *agg_p;
  AGGREGATE_ACCUMULATOR *accumulator;
  DB_VALUE dbval;
  DB_TYPE val_type;
  TP_DOMAIN *val_dom;
  PR_TYPE *pr_type_p;
  OR_BUF buf;
  char *disk_repr_p = NULL;
  int dbval_size;

  DB_MAKE_NULL (&dbval);

  for (agg_p = agg_list_p; agg_p != NULL && error == NO_ERROR; agg_p = agg_p->next)
    {
      assert (DB_IS_NULL (&dbval));

      /*
       * the value of groupby_num() remains unchanged;
       * it will be changed while evaluating groupby_num predicates
       * against each group at 'xs_eval_grbynum_pred()'
       */
      if (agg_p->function == PT_GROUPBY_NUM)
        {
          /* nothing to do with groupby_num() */
          continue;
        }

      if (agg_p->flag_agg_optimize)
        {
          continue;
        }

      /* determine accumulator */
      accumulator = &(agg_p->accumulator);

      if (agg_p->function == PT_COUNT_STAR)
        {                       /* increment and continue */
          accumulator->curr_cnt++;
          continue;
        }

      assert (DB_IS_NULL (&dbval));

      /*
       * fetch operand value. aggregate regulator variable should only
       * contain constants
       */
      error = fetch_copy_dbval (thread_p, &agg_p->operand, val_desc_p, NULL, NULL, &dbval);
      if (error != NO_ERROR)
        {
          pr_clear_value (&dbval);
          return error;
        }

      /* eliminate null values */
      if (DB_IS_NULL (&dbval))
        {
          if ((agg_p->function == PT_COUNT || agg_p->function == PT_COUNT_STAR) && DB_IS_NULL (accumulator->value))
            {
              /* we might get a NULL count; correct that */
              DB_MAKE_BIGINT (accumulator->value, 0);
            }

          continue;
        }

      /*
       * handle distincts by inserting each operand into a list file,
       * which will be distinct-ified and counted/summed/averaged
       * in qdata_finalize_aggregate_list ()
       */
      if (agg_p->option == Q_DISTINCT || agg_p->sort_list != NULL)
        {
          if (agg_p->sort_list != NULL)
            {
              assert (agg_p->function == PT_GROUP_CONCAT);

              error = qdata_update_agg_interpolate_func_value_and_domain (agg_p, &dbval);
              if (error != NO_ERROR)
                {
                  pr_clear_value (&dbval);
                  return error;
                }
            }

          /* set list_id domain, if it's not set
           */

          val_type = DB_VALUE_DOMAIN_TYPE (&dbval);
          assert (DB_IS_NULL (&dbval) || val_type != DB_TYPE_VARIABLE);

          assert (agg_p->list_id != NULL);
          assert (agg_p->list_id->type_list.type_cnt == 1);

          if (TP_DOMAIN_TYPE (agg_p->list_id->type_list.domp[0]) == DB_TYPE_VARIABLE)
            {
              if (DB_IS_NULL (&dbval) || val_type == DB_TYPE_NULL || val_type == DB_TYPE_VARIABLE)
                {
                  /* In this case, we cannot resolve the value's domain.
                   * We will try to do for the next tuple.
                   */
                  ;
                }
              else
                {
                  val_dom = tp_domain_resolve_value (&dbval);
                  if (val_dom == NULL)
                    {
                      assert (false);
                      pr_clear_value (&dbval);
                      return ER_FAILED;
                    }

                  agg_p->list_id->type_list.domp[0] = val_dom;
                }
            }
          else
            {
              error = qdata_coerce_result_to_domain (&dbval, agg_p->list_id->type_list.domp[0]);
              if (error != NO_ERROR)
                {
                  pr_clear_value (&dbval);
                  return error;
                }
            }

          val_type = DB_VALUE_DOMAIN_TYPE (&dbval);
          pr_type_p = PR_TYPE_FROM_ID (val_type);
          if (pr_type_p == NULL)
            {
              assert (false);
              pr_clear_value (&dbval);
              return ER_FAILED;
            }

          dbval_size = pr_data_writeval_disk_size (&dbval);
          if ((dbval_size != 0) && (disk_repr_p = (char *) malloc (dbval_size)))
            {
              OR_BUF_INIT (buf, disk_repr_p, dbval_size);

              error = (*(pr_type_p->data_writeval)) (&buf, &dbval);
              if (error != NO_ERROR)
                {
                  free_and_init (disk_repr_p);
                  pr_clear_value (&dbval);
                  return error;
                }
            }
          else
            {
              assert (false);
              pr_clear_value (&dbval);
              return ER_FAILED;
            }

          error = qfile_add_item_to_list (thread_p, disk_repr_p, dbval_size, agg_p->list_id);
          if (error != NO_ERROR)
            {
              free_and_init (disk_repr_p);
              pr_clear_value (&dbval);
              return error;
            }

          free_and_init (disk_repr_p);
          pr_clear_value (&dbval);
          continue;
        }

      if (agg_p->function == PT_GROUP_CONCAT)
        {
          /* group concat function requires special care */
          if (accumulator->curr_cnt < 1)
            {
              error = qdata_group_concat_first_value (thread_p, agg_p, &dbval);
            }
          else
            {
              error = qdata_group_concat_value (thread_p, agg_p, val_desc_p, &dbval);
            }
        }
      else
        {
          /* aggregate value */
          error = qdata_aggregate_value_to_accumulator (thread_p, accumulator, agg_p->function, &dbval);
        }

      /* increment tuple count */
      accumulator->curr_cnt++;

      /* clear value */
      pr_clear_value (&dbval);

      /* handle error */
      if (error != NO_ERROR)
        {
          return error;
        }
    }                           /* for (agg_p = ...) */

#if 1                           /* TODO - */
  assert (DB_IS_NULL (&dbval));
#endif

  assert (error == NO_ERROR);

  return NO_ERROR;
}

/*
 * qdata_evaluate_aggregate_optimize () -
 *   return:
 *   agg_ptr(in)        :
 *   cls_oid(in)   :
 *   hfid(in)   :
 */
int
qdata_evaluate_aggregate_optimize (THREAD_ENTRY * thread_p, AGGREGATE_TYPE * agg_p, OID * cls_oid, HFID * hfid_p)
{
  DB_BIGINT oid_count = 0, null_count = 0, key_count = 0;
  int flag_btree_stat_needed = true;
  VPID root_vpid;
  DB_IDXKEY idxkey;

  assert (cls_oid != NULL);

  if (!agg_p->flag_agg_optimize)
    {
      return ER_FAILED;
    }

  if (hfid_p->vfid.fileid < 0)
    {
      assert (false);
      return ER_FAILED;
    }

  if (agg_p->function == PT_MIN || agg_p->function == PT_MAX)
    {
      flag_btree_stat_needed = false;
    }

  if (agg_p->function == PT_COUNT_STAR)
    {
      assert (false);
      if (BTID_IS_NULL (&agg_p->btid))
        {
          if (heap_get_num_objects (thread_p, hfid_p, &oid_count) != NO_ERROR)
            {
              return ER_FAILED;
            }
          flag_btree_stat_needed = false;
        }
    }

  if (flag_btree_stat_needed)
    {
      assert (false);
      if (BTID_IS_NULL (&agg_p->btid))
        {
          return ER_FAILED;
        }

      /* TODO: */
      oid_count = 0;
      null_count = 0;
      key_count = 0;
    }

  root_vpid.volid = agg_p->btid.vfid.volid;
  root_vpid.pageid = agg_p->btid.root_pageid;

  DB_IDXKEY_MAKE_NULL (&idxkey);

  switch (agg_p->function)
    {
    case PT_COUNT:
      assert (false);
      if (agg_p->option == Q_ALL)
        {
          DB_MAKE_BIGINT (agg_p->accumulator.value, oid_count - null_count);
        }
      else
        {
          DB_MAKE_BIGINT (agg_p->accumulator.value, key_count);
        }
      break;

    case PT_COUNT_STAR:
      assert (false);
      agg_p->accumulator.curr_cnt = oid_count;
      break;

    case PT_MIN:
    case PT_MAX:
      DB_MAKE_NULL (agg_p->accumulator.value);
      if (btree_find_min_or_max_key (thread_p, cls_oid, &agg_p->btid,
                                     &root_vpid, &idxkey, agg_p->function == PT_MIN ? true : false) != NO_ERROR)
        {
          db_idxkey_clear (&idxkey);
          return ER_FAILED;
        }

      /* check iff is not empty index */
      if (!DB_IDXKEY_IS_NULL (&idxkey))
        {
          assert (idxkey.size == 2);

          /* copy the first element */
          if (DB_IS_NULL (&(idxkey.vals[0])))
            {
              /* give up this way */
              db_idxkey_clear (&idxkey);
              return ER_FAILED;
            }

          (void) pr_clear_value (agg_p->accumulator.value);
          db_value_clone (&(idxkey.vals[0]), agg_p->accumulator.value);
          assert (tp_valid_indextype (DB_VALUE_DOMAIN_TYPE (agg_p->accumulator.value)));
        }
      break;

    default:
      assert (false);
      return ER_FAILED;         /* is impossible */
      break;
    }

  db_idxkey_clear (&idxkey);

  return NO_ERROR;
}

/*
 * qdata_finalize_aggregate_list () -
 *   return: NO_ERROR, or ER_code
 *   agg_list(in)       : Aggregate expression node list
 *   vd(in): Value descriptor for positional values (optional)
 *   keep_list_file(in) : whether keep the list file for reuse
 *
 * Note: Make the final evaluation on the aggregate expression list.
 */
int
qdata_finalize_aggregate_list (THREAD_ENTRY * thread_p,
                               AGGREGATE_TYPE * agg_list_p, VAL_DESCR * vd, bool keep_list_file)
{
  AGGREGATE_TYPE *agg_p;
  DB_TYPE agg_val_type;
  DB_VALUE sqr_val;
  DB_VALUE dbval;
  DB_VALUE xavgval, xavg_1val, x2avgval;
  DB_VALUE xavg2val, varval;
  DB_VALUE dval;
  DB_VALUE add_val;
  double dtmp;
  QFILE_LIST_ID *list_id_p;
  QFILE_LIST_SCAN_ID scan_id;
  SCAN_CODE scan_code;
  QFILE_TUPLE_RECORD tuple_record = {
    NULL, 0
  };
  char *tuple_p;
  PR_TYPE *pr_type_p;
  OR_BUF buf;
  int error = NO_ERROR;

  DB_MAKE_NULL (&sqr_val);
  DB_MAKE_NULL (&dbval);
  DB_MAKE_NULL (&xavgval);
  DB_MAKE_NULL (&xavg_1val);
  DB_MAKE_NULL (&x2avgval);
  DB_MAKE_NULL (&xavg2val);
  DB_MAKE_NULL (&varval);
  DB_MAKE_NULL (&dval);
  DB_MAKE_NULL (&add_val);

  for (agg_p = agg_list_p; agg_p != NULL; agg_p = agg_p->next)
    {
      assert (DB_IS_NULL (&dbval));

      /* the value of groupby_num() remains unchanged;
       * it will be changed while evaluating groupby_num predicates
       * against each group at 'xs_eval_grbynum_pred()'
       */
      if (agg_p->function == PT_GROUPBY_NUM)
        {
          /* nothing to do with groupby_num() */
          continue;
        }

      agg_val_type = DB_VALUE_DOMAIN_TYPE (agg_p->accumulator.value);

      /* set count-star aggregate values */
      if (agg_p->function == PT_COUNT_STAR)
        {
          DB_MAKE_BIGINT (agg_p->accumulator.value, agg_p->accumulator.curr_cnt);
        }
      else if (agg_p->function == PT_SUM)
        {
          if (TP_IS_SET_TYPE (agg_val_type))
            {
              ;                 /* TODO - for catalog access */
            }
          else if (!TP_IS_NUMERIC_TYPE (agg_val_type))
            {
              if (tp_value_coerce
                  (agg_p->accumulator.value, agg_p->accumulator.value, &tp_Double_domain) != DOMAIN_COMPATIBLE)
                {
                  return ER_FAILED;
                }
            }
        }

      (void) pr_clear_value (&dbval);

      /* process list file for sum/avg/count distinct */
      if ((agg_p->option == Q_DISTINCT || agg_p->sort_list != NULL)
          && agg_p->function != PT_MAX && agg_p->function != PT_MIN)
        {

          if (agg_p->flag_agg_optimize == false)
            {
              list_id_p = agg_p->list_id =
                qfile_sort_list (thread_p, agg_p->list_id, agg_p->sort_list, agg_p->option, false);

              if (list_id_p == NULL)
                {
                  assert (DB_IS_NULL (&dbval));
                  return ER_FAILED;
                }

              if (agg_p->function == PT_COUNT)
                {
                  /* TODO - need to use int64_t type */
                  DB_MAKE_BIGINT (agg_p->accumulator.value, (DB_BIGINT) (list_id_p->tuple_cnt));
                }
              else
                {
                  pr_type_p = list_id_p->type_list.domp[0]->type;

                  /* scan list file, accumulating total for sum/avg */
                  error = qfile_open_list_scan (list_id_p, &scan_id);
                  if (error != NO_ERROR)
                    {
                      assert (DB_IS_NULL (&dbval));
                      qfile_close_list (thread_p, list_id_p);
                      qfile_destroy_list (thread_p, list_id_p);
                      return error;
                    }

                  while (true)
                    {
                      assert (DB_IS_NULL (&dbval));

                      scan_code = qfile_scan_list_next (thread_p, &scan_id, &tuple_record, PEEK);
                      if (scan_code != S_SUCCESS)
                        {
                          break;
                        }

                      tuple_p = ((char *) tuple_record.tpl + QFILE_TUPLE_LENGTH_SIZE);
                      if (QFILE_GET_TUPLE_VALUE_FLAG (tuple_p) == V_UNBOUND)
                        {
                          continue;
                        }

                      or_init (&buf,
                               (char *) tuple_p +
                               QFILE_TUPLE_VALUE_HEADER_SIZE, QFILE_GET_TUPLE_VALUE_LENGTH (tuple_p));

                      assert (DB_IS_NULL (&dbval));
                      error = (*(pr_type_p->data_readval)) (&buf, &dbval, list_id_p->type_list.domp[0], -1, true);
                      if (error != NO_ERROR)
                        {
                          pr_clear_value (&dbval);
                          qfile_close_scan (thread_p, &scan_id);
                          qfile_close_list (thread_p, list_id_p);
                          qfile_destroy_list (thread_p, list_id_p);
                          return error;
                        }

                      if (agg_p->function == PT_VARIANCE
                          || agg_p->function == PT_STDDEV
                          || agg_p->function == PT_VAR_POP
                          || agg_p->function == PT_STDDEV_POP
                          || agg_p->function == PT_VAR_SAMP || agg_p->function == PT_STDDEV_SAMP)
                        {
                          if (tp_value_coerce (&dbval, &dbval, &tp_Double_domain) != DOMAIN_COMPATIBLE)
                            {
                              pr_clear_value (&dbval);
                              qfile_close_scan (thread_p, &scan_id);
                              qfile_close_list (thread_p, list_id_p);
                              qfile_destroy_list (thread_p, list_id_p);
                              return ER_FAILED;
                            }
                        }

                      if (DB_IS_NULL (agg_p->accumulator.value))
                        {
                          /* first iteration: can't add to a null agg_ptr->value */
                          PR_TYPE *tmp_pr_type;
                          DB_TYPE dbval_type = DB_VALUE_DOMAIN_TYPE (&dbval);

                          tmp_pr_type = PR_TYPE_FROM_ID (dbval_type);
                          if (tmp_pr_type == NULL)
                            {
                              assert (false);
                              pr_clear_value (&dbval);
                              qfile_close_scan (thread_p, &scan_id);
                              qfile_close_list (thread_p, list_id_p);
                              qfile_destroy_list (thread_p, list_id_p);
                              return ER_FAILED;
                            }

                          if (agg_p->function == PT_STDDEV
                              || agg_p->function == PT_VARIANCE
                              || agg_p->function == PT_STDDEV_POP
                              || agg_p->function == PT_VAR_POP
                              || agg_p->function == PT_STDDEV_SAMP || agg_p->function == PT_VAR_SAMP)
                            {
                              error = qdata_multiply_dbval (&dbval, &dbval, &sqr_val);
                              if (error != NO_ERROR)
                                {
                                  pr_clear_value (&dbval);
                                  qfile_close_scan (thread_p, &scan_id);
                                  qfile_close_list (thread_p, list_id_p);
                                  qfile_destroy_list (thread_p, list_id_p);
                                  return error;
                                }

                              (void) pr_clear_value (agg_p->accumulator.value2);

                              (*(tmp_pr_type->setval)) (agg_p->accumulator.value2, &sqr_val, true);
                            }

                          if (agg_p->function == PT_GROUP_CONCAT)
                            {
                              error = qdata_group_concat_first_value (thread_p, agg_p, &dbval);
                              if (error != NO_ERROR)
                                {
                                  (void) pr_clear_value (&dbval);
                                  qfile_close_scan (thread_p, &scan_id);
                                  qfile_close_list (thread_p, list_id_p);
                                  qfile_destroy_list (thread_p, list_id_p);
                                  return error;
                                }
                            }
                          else
                            {
                              (void) pr_clear_value (agg_p->accumulator.value);

                              (*(tmp_pr_type->setval)) (agg_p->accumulator.value, &dbval, true);
                            }
                        }
                      else
                        {
                          if (agg_p->function == PT_STDDEV
                              || agg_p->function == PT_VARIANCE
                              || agg_p->function == PT_STDDEV_POP
                              || agg_p->function == PT_VAR_POP
                              || agg_p->function == PT_STDDEV_SAMP || agg_p->function == PT_VAR_SAMP)
                            {
                              error = qdata_multiply_dbval (&dbval, &dbval, &sqr_val);
                              if (error != NO_ERROR)
                                {
                                  pr_clear_value (&dbval);
                                  qfile_close_scan (thread_p, &scan_id);
                                  qfile_close_list (thread_p, list_id_p);
                                  qfile_destroy_list (thread_p, list_id_p);
                                  return error;
                                }

                              (void) pr_clear_value (&add_val);

                              error = qdata_add_dbval (agg_p->accumulator.value2, &sqr_val, &add_val);
                              if (error != NO_ERROR)
                                {
                                  pr_clear_value (&dbval);
                                  pr_clear_value (&sqr_val);
                                  qfile_close_scan (thread_p, &scan_id);
                                  qfile_close_list (thread_p, list_id_p);
                                  qfile_destroy_list (thread_p, list_id_p);
                                  return error;
                                }

                              /* check for add success */
                              if (!DB_IS_NULL (&add_val))
                                {
                                  (void) pr_clear_value (agg_p->accumulator.value2);
                                  pr_clone_value (&add_val, agg_p->accumulator.value2);
                                }

                              pr_clear_value (&add_val);
                            }

                          if (agg_p->function == PT_GROUP_CONCAT)
                            {
                              error = qdata_group_concat_value (thread_p, agg_p, vd, &dbval);
                              if (error != NO_ERROR)
                                {
                                  pr_clear_value (&dbval);
                                  qfile_close_scan (thread_p, &scan_id);
                                  qfile_close_list (thread_p, list_id_p);
                                  qfile_destroy_list (thread_p, list_id_p);
                                  return error;
                                }
                            }
                          else
                            {
                              (void) pr_clear_value (&add_val);

                              error = qdata_add_dbval (agg_p->accumulator.value, &dbval, &add_val);
                              if (error != NO_ERROR)
                                {
                                  pr_clear_value (&dbval);
                                  qfile_close_scan (thread_p, &scan_id);
                                  qfile_close_list (thread_p, list_id_p);
                                  qfile_destroy_list (thread_p, list_id_p);
                                  return error;
                                }

                              /* check for add success */
                              if (!DB_IS_NULL (&add_val))
                                {
                                  (void) pr_clear_value (agg_p->accumulator.value);
                                  pr_clone_value (&add_val, agg_p->accumulator.value);
                                }

                              pr_clear_value (&add_val);
                            }
                        }

                      pr_clear_value (&dbval);
                    }

                  assert (DB_IS_NULL (&dbval));

                  qfile_close_scan (thread_p, &scan_id);
                  agg_p->accumulator.curr_cnt = list_id_p->tuple_cnt;
                }
            }

          /* close and destroy temporary list files */
          if (!keep_list_file)
            {
              qfile_close_list (thread_p, agg_p->list_id);
              qfile_destroy_list (thread_p, agg_p->list_id);
            }
        }

      assert (DB_IS_NULL (&dbval));

      if (agg_p->function == PT_GROUP_CONCAT && !DB_IS_NULL (agg_p->accumulator.value))
        {
          db_string_fix_string_size (agg_p->accumulator.value);
        }

      /* compute averages */
      if (agg_p->accumulator.curr_cnt > 0
          && (agg_p->function == PT_AVG
              || agg_p->function == PT_STDDEV
              || agg_p->function == PT_VARIANCE
              || agg_p->function == PT_STDDEV_POP
              || agg_p->function == PT_VAR_POP || agg_p->function == PT_STDDEV_SAMP || agg_p->function == PT_VAR_SAMP))
        {
          /* compute AVG(X) = SUM(X)/COUNT(X) */
          DB_MAKE_DOUBLE (&dbval, agg_p->accumulator.curr_cnt);
          error = qdata_divide_dbval (agg_p->accumulator.value, &dbval, &xavgval);
          if (error != NO_ERROR)
            {
              return error;
            }

          if (agg_p->function == PT_AVG)
            {
              if (tp_value_coerce (&xavgval, agg_p->accumulator.value, &tp_Double_domain) != DOMAIN_COMPATIBLE)
                {
                  return ER_FAILED;
                }

              pr_clear_value (&dbval);
              continue;
            }

          if (agg_p->function == PT_STDDEV_SAMP || agg_p->function == PT_VAR_SAMP)
            {
              /* compute SUM(X^2) / (n-1) */
              if (agg_p->accumulator.curr_cnt > 1)
                {
                  DB_MAKE_DOUBLE (&dbval, (double) (agg_p->accumulator.curr_cnt - 1));
                }
              else
                {
                  /* when not enough samples, return NULL */
                  DB_MAKE_NULL (agg_p->accumulator.value);

                  pr_clear_value (&dbval);
                  continue;
                }
            }
          else
            {
              assert (agg_p->function == PT_STDDEV
                      || agg_p->function == PT_STDDEV_POP
                      || agg_p->function == PT_VARIANCE || agg_p->function == PT_VAR_POP);
              /* compute SUM(X^2) / n */
              DB_MAKE_DOUBLE (&dbval, (double) agg_p->accumulator.curr_cnt);
            }

          error = qdata_divide_dbval (agg_p->accumulator.value2, &dbval, &x2avgval);
          if (error != NO_ERROR)
            {
              return error;
            }

          /* compute {SUM(X) / (n)} OR  {SUM(X) / (n-1)} for xxx_SAMP agg */
          error = qdata_divide_dbval (agg_p->accumulator.value, &dbval, &xavg_1val);
          if (error != NO_ERROR)
            {
              return error;
            }

          /* compute AVG(X) * {SUM(X) / (n)} , AVG(X) * {SUM(X) / (n-1)} for
           * xxx_SAMP agg*/
          error = qdata_multiply_dbval (&xavgval, &xavg_1val, &xavg2val);
          if (error != NO_ERROR)
            {
              return error;
            }

          /* compute VAR(X) = SUM(X^2)/(n) - AVG(X) * {SUM(X) / (n)} OR
           * VAR(X) = SUM(X^2)/(n-1) - AVG(X) * {SUM(X) / (n-1)}  for
           * xxx_SAMP aggregates */
          error = qdata_subtract_dbval (&x2avgval, &xavg2val, &varval);
          if (error != NO_ERROR)
            {
              return error;
            }

          if (agg_p->function == PT_VARIANCE || agg_p->function == PT_STDDEV
              || agg_p->function == PT_VAR_POP
              || agg_p->function == PT_STDDEV_POP
              || agg_p->function == PT_VAR_SAMP || agg_p->function == PT_STDDEV_SAMP)
            {
              (void) pr_clear_value (agg_p->accumulator.value);
              pr_clone_value (&varval, agg_p->accumulator.value);
            }

          if (agg_p->function == PT_STDDEV || agg_p->function == PT_STDDEV_POP || agg_p->function == PT_STDDEV_SAMP)
            {
              db_value_domain_init (&dval, DB_TYPE_DOUBLE, DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
              /* Construct TP_DOMAIN whose type is DB_TYPE_DOUBLE     */
              if (tp_value_coerce (&varval, &dval, &tp_Double_domain) != DOMAIN_COMPATIBLE)
                {
                  return ER_FAILED;
                }

              dtmp = DB_GET_DOUBLE (&dval);

              /* mathematically, dtmp should be zero or positive; however, due
               * to some precision errors, in some cases it can be a very small
               * negative number of which we cannot extract the square root */
              dtmp = (dtmp < 0.0f ? 0.0f : dtmp);

              dtmp = sqrt (dtmp);
              DB_MAKE_DOUBLE (&dval, dtmp);

              (void) pr_clear_value (agg_p->accumulator.value);
              pr_clone_value (&dval, agg_p->accumulator.value);
            }
        }

      (void) pr_clear_value (&dbval);

    }                           /* for (agg_p = ... ) */

  assert (DB_IS_NULL (&dbval));

  return NO_ERROR;
}

/*
 * MISCELLANEOUS
 */

/*
 * qdata_get_tuple_value_size_from_dbval () - Return the tuple value size
 *	for the db_value
 *   return: tuple_value_size or ER_FAILED
 *   dbval(in)  : db_value node
 */
int
qdata_get_tuple_value_size_from_dbval (DB_VALUE * dbval_p)
{
  int val_size, align;
  int tuple_value_size = 0;
  PR_TYPE *type_p;
  DB_TYPE dbval_type;

  if (DB_IS_NULL (dbval_p))
    {
      tuple_value_size = QFILE_TUPLE_VALUE_HEADER_SIZE;
    }
  else
    {
      dbval_type = DB_VALUE_DOMAIN_TYPE (dbval_p);
      type_p = PR_TYPE_FROM_ID (dbval_type);
      if (type_p)
        {
          if (type_p->data_lengthval == NULL)
            {
              val_size = type_p->disksize;
            }
          else
            {
              val_size = (*(type_p->data_lengthval)) (dbval_p, 1);
              if (pr_is_string_type (dbval_type))
                {
                  int precision = DB_VALUE_PRECISION (dbval_p);
                  int string_length = DB_GET_STRING_LENGTH (dbval_p);

                  if (precision == TP_FLOATING_PRECISION_VALUE)
                    {
                      precision = DB_MAX_STRING_LENGTH;
                    }

                  assert_release (string_length <= precision);

                  if (val_size < 0)
                    {
                      return ER_FAILED;
                    }
                  else if (string_length > precision)
                    {
                      /* The size of db_value is greater than it's precision.
                       * This case is abnormal (assertion failure).
                       * Code below is remained for backward compatibility.
                       */
                      if (db_string_truncate (dbval_p, precision) != NO_ERROR)
                        {
                          return ER_FAILED;
                        }
                      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
                              ER_DATA_IS_TRUNCATED_TO_PRECISION, 2, precision, string_length);

                      val_size = (*(type_p->data_lengthval)) (dbval_p, 1);
                    }
                }
            }

          align = DB_ALIGN (val_size, MAX_ALIGNMENT);   /* to align for the next field */
          tuple_value_size = QFILE_TUPLE_VALUE_HEADER_SIZE + align;
        }
    }

  return tuple_value_size;
}

/*
 * qdata_get_single_tuple_from_list_id () -
 *   return: NO_ERROR or error code
 *   list_id(in)        : List file identifier
 *   single_tuple(in)   : VAL_LIST
 */
int
qdata_get_single_tuple_from_list_id (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id_p, VAL_LIST * single_tuple_p)
{
  QFILE_TUPLE_RECORD tuple_record = { NULL, 0 };
  QFILE_LIST_SCAN_ID scan_id;
  OR_BUF buf;
  PR_TYPE *pr_type_p;
  QFILE_TUPLE_VALUE_FLAG flag;
  int length;
  TP_DOMAIN *domain_p;
  char *ptr;
  int tuple_count, value_count, i;
  QPROC_DB_VALUE_LIST value_list;
  int error_code;

  tuple_count = list_id_p->tuple_cnt;
  value_count = list_id_p->type_list.type_cnt;

  /* value_count can be greater than single_tuple_p->val_cnt
   * when the subquery has a hidden column.
   * Under normal situation, those are same.
   */
  if (tuple_count > 1 || value_count < single_tuple_p->val_cnt)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_QRY_SINGLE_TUPLE, 0);
      return ER_QPROC_INVALID_QRY_SINGLE_TUPLE;
    }

  if (tuple_count == 1)
    {
      error_code = qfile_open_list_scan (list_id_p, &scan_id);
      if (error_code != NO_ERROR)
        {
          return error_code;
        }

      if (qfile_scan_list_next (thread_p, &scan_id, &tuple_record, PEEK) != S_SUCCESS)
        {
          qfile_close_scan (thread_p, &scan_id);
          return ER_FAILED;
        }

      for (i = 0, value_list = single_tuple_p->valp; i < single_tuple_p->val_cnt; i++, value_list = value_list->next)
        {
          domain_p = list_id_p->type_list.domp[i];
          if (domain_p == NULL || domain_p->type == NULL)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_QRY_SINGLE_TUPLE, 0);
              return ER_QPROC_INVALID_QRY_SINGLE_TUPLE;
            }

          if (db_value_domain_init (value_list->val,
                                    TP_DOMAIN_TYPE (domain_p), domain_p->precision, domain_p->scale) != NO_ERROR)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_QRY_SINGLE_TUPLE, 0);
              return ER_QPROC_INVALID_QRY_SINGLE_TUPLE;
            }

          pr_type_p = domain_p->type;
          if (pr_type_p == NULL)
            {
              return ER_FAILED;
            }

          flag = (QFILE_TUPLE_VALUE_FLAG) qfile_locate_tuple_value (tuple_record.tpl, i, &ptr, &length);
          OR_BUF_INIT (buf, ptr, length);
          if (flag == V_BOUND)
            {
              if ((*(pr_type_p->data_readval)) (&buf, value_list->val, domain_p, -1, true) != NO_ERROR)
                {
                  qfile_close_scan (thread_p, &scan_id);
                  return ER_FAILED;
                }
            }
          else
            {
              /* If value is NULL, properly initialize the result */
              db_value_domain_init (value_list->val, pr_type_p->id, DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
            }
        }

      qfile_close_scan (thread_p, &scan_id);
    }

  return NO_ERROR;
}

/*
 * qdata_get_valptr_type_list () -
 *   return: NO_ERROR, or ER_code
 *   valptr_list(in)    : Value pointer list
 *   type_list(out)     : Set to the result type list
 *
 * Note: Find the result type list of value pointer list and set to
 * type list.  Regu variables that are hidden columns are not
 * entered as part of the type list because they are not entered
 * in the list file.
 */
int
qdata_get_valptr_type_list (UNUSED_ARG THREAD_ENTRY * thread_p,
                            VALPTR_LIST * valptr_list_p, QFILE_TUPLE_VALUE_TYPE_LIST * type_list_p)
{
  REGU_VARIABLE_LIST reg_var_p;
  int i, count;

  if (type_list_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      return ER_FAILED;
    }

  count = 0;
  reg_var_p = valptr_list_p->valptrp;
  for (i = 0; i < valptr_list_p->valptr_cnt; i++, reg_var_p = reg_var_p->next)
    {
      if (REGU_VARIABLE_IS_FLAGED (&reg_var_p->value, REGU_VARIABLE_HIDDEN_COLUMN))
        {
          continue;             /* skip hidden cols */
        }

      count++;
    }

  type_list_p->type_cnt = count;
  type_list_p->domp = NULL;

  if (type_list_p->type_cnt != 0)
    {
      type_list_p->domp = (TP_DOMAIN **) malloc (sizeof (TP_DOMAIN *) * type_list_p->type_cnt);
      if (type_list_p->domp == NULL)
        {
          return ER_FAILED;
        }
    }

  count = 0;
  reg_var_p = valptr_list_p->valptrp;
  for (i = 0; i < type_list_p->type_cnt; i++, reg_var_p = reg_var_p->next)
    {
      if (REGU_VARIABLE_IS_FLAGED (&reg_var_p->value, REGU_VARIABLE_HIDDEN_COLUMN))
        {
          continue;             /* skip hidden cols */
        }

      type_list_p->domp[count] = tp_domain_resolve_default (DB_TYPE_VARIABLE);

      count++;
    }

  return NO_ERROR;
}

/*
 * qdata_convert_dbvals_to_set () -
 *   return: NO_ERROR, or ER_code
 *   stype(in)  : set type
 *   func(in)   : regu variable (guaranteed TYPE_FUNC)
 *   vd(in)     : Value descriptor
 *   obj_oid(in): object identifier
 *   tpl(in)    : list file tuple
 *
 * Note: Convert a list of vars into a sequence and return a pointer to it.
 */
static int
qdata_convert_dbvals_to_set (THREAD_ENTRY * thread_p, DB_TYPE stype,
                             REGU_VARIABLE * regu_func_p, VAL_DESCR * val_desc_p, OID * obj_oid_p, QFILE_TUPLE tuple)
{
  FUNCTION_TYPE *funcp = NULL;
  DB_VALUE dbval;
  DB_COLLECTION *collection_p = NULL;
  SETOBJ *setobj_p = NULL;
  int n, size;
  REGU_VARIABLE_LIST operand = NULL;
  int error_code = NO_ERROR;
  TP_DOMAIN *set_dom = NULL, *dbval_dom = NULL;

  funcp = regu_func_p->value.funcp;

  DB_MAKE_NULL (&dbval);

  if (stype == DB_TYPE_SEQUENCE)
    {
      size = 0;
      for (operand = funcp->operand; operand != NULL; operand = operand->next)
        {
          size++;
        }

      collection_p = db_seq_create (NULL, NULL, size);
    }
  else
    {
      return ER_FAILED;
    }

  error_code = set_get_setobj (collection_p, &setobj_p, 1);
  if (error_code != NO_ERROR || !setobj_p)
    {
      goto error;
    }

  assert (setobj_p != NULL);
  assert (setobj_p->domain != NULL);
  assert (setobj_p->domain->is_cached);
  assert (setobj_p->domain->next == NULL);
  assert (setobj_p->domain->setdomain == NULL);
  assert (TP_IS_SET_TYPE (TP_DOMAIN_TYPE (setobj_p->domain)));

  set_dom = tp_domain_construct (stype, NULL, 0, 0, NULL);
  if (set_dom == NULL)
    {
      goto error;
    }

  assert (set_dom->is_cached == 0);
  assert (set_dom->next_list == NULL);
  assert (set_dom->next == NULL);
  assert (set_dom->setdomain == NULL);

  n = 0;
  for (operand = funcp->operand; operand != NULL; operand = operand->next)
    {
      if (fetch_copy_dbval (thread_p, &operand->value, val_desc_p, obj_oid_p, tuple, &dbval) != NO_ERROR)
        {
          goto error;
        }

      assert (DB_VALUE_DOMAIN_TYPE (&dbval) != DB_TYPE_VARIABLE);

      /* add value domains */
      if (DB_IS_NULL (&dbval) || DB_VALUE_DOMAIN_TYPE (&dbval) == DB_TYPE_NULL)
        {
          /* In this case, we cannot resolve the value's domain.
           */
          ;
        }
      else
        {
          dbval_dom = tp_domain_resolve_value (&dbval);
          if (dbval_dom == NULL)
            {
              assert (false);
              goto error;
            }
          assert (dbval_dom->is_cached);
          assert (dbval_dom->next == NULL);

          /* !!! should copy component set domains !!! */
          dbval_dom = tp_domain_copy (dbval_dom);
          if (dbval_dom == NULL)
            {
              assert (false);
              goto error;
            }
          assert (dbval_dom->is_cached == 0);
          assert (dbval_dom->next == NULL);

          if (tp_domain_attach (&(set_dom->setdomain), dbval_dom) != NO_ERROR)
            {
              assert (false);
              goto error;
            }
        }

      /* using setobj_put_value transfers "ownership" of the
       * db_value memory to the set. This avoids a redundant clone/free.
       */
      error_code = setobj_put_value (setobj_p, n, &dbval);

      if (error_code != NO_ERROR)
        {
          goto error;
        }

      n++;
    }

  set_dom = tp_domain_cache (set_dom);
  assert (set_dom->is_cached);
  assert (set_dom->next == NULL);

  setobj_put_domain (setobj_p, set_dom);

  set_make_collection (funcp->value, collection_p);

  return NO_ERROR;

error:
  pr_clear_value (&dbval);
  tp_domain_free (set_dom);

  return ((error_code == NO_ERROR) ? ER_FAILED : error_code);
}

/*
 * qdata_evaluate_function () -
 *   return: NO_ERROR, or ER_code
 *   func(in)   :
 *   vd(in)     :
 *   obj_oid(in)        :
 *   tpl(in)    :
 *
 * Note: Evaluate given function.
 */
int
qdata_evaluate_function (THREAD_ENTRY * thread_p,
                         REGU_VARIABLE * function_p, VAL_DESCR * val_desc_p, OID * obj_oid_p, QFILE_TUPLE tuple)
{
  FUNCTION_TYPE *funcp;

  /* should sync with fetch_peek_dbval ()
   */

  funcp = function_p->value.funcp;

  /* clear any value from a previous iteration */
  pr_clear_value (funcp->value);

  switch (funcp->ftype)
    {
    case F_SEQUENCE:
#if 0                           /* TODO - */
      assert (false);           /* should not reach here */
#endif
      return qdata_convert_dbvals_to_set (thread_p, DB_TYPE_SEQUENCE, function_p, val_desc_p, obj_oid_p, tuple);

    case F_INSERT_SUBSTRING:
      return qdata_insert_substring_function (thread_p, funcp, val_desc_p, obj_oid_p, tuple);
    case F_ELT:
      return qdata_elt (thread_p, funcp, val_desc_p, obj_oid_p, tuple);

    default:
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      return ER_FAILED;
    }
}

/*
 * qdata_list_dbs () - lists all databases names
 *   return: NO_ERROR, or ER_code
 *   result_p(out) : resultant db_value node
 */
int
qdata_list_dbs (UNUSED_ARG THREAD_ENTRY * thread_p, DB_VALUE * result_p)
{
  const char *prm_value;

  prm_value = prm_get_string_value (PRM_ID_HA_DB_LIST);

  if (prm_value == NULL)
    {
      db_make_null (result_p);
    }
  else
    {
      if (db_make_string (result_p, prm_value) != NO_ERROR)
        {
          goto error;
        }
    }

  return NO_ERROR;

error:
  return er_errid ();
}

/*
 * qdata_group_concat_first_value() - concatenates the first value
 *   return: NO_ERROR, or ER_code
 *   thread_p(in) :
 *   agg_p(in)	  : GROUP_CONCAT aggregate
 *   dbvalue(in)  : current value
 */
int
qdata_group_concat_first_value (THREAD_ENTRY * thread_p, AGGREGATE_TYPE * agg_p, DB_VALUE * dbvalue)
{
  int max_allowed_size;
  DB_VALUE tmp_val;

  DB_MAKE_NULL (&tmp_val);

  assert (DB_IS_NULL (agg_p->accumulator.value));

  /* init the aggregate value domain */
  if (db_value_domain_init (agg_p->accumulator.value, DB_TYPE_VARCHAR,
                            DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE) != NO_ERROR)
    {
      pr_clear_value (dbvalue);
      return ER_FAILED;
    }

  if (db_string_make_empty_typed_string (thread_p, agg_p->accumulator.value,
                                         DB_TYPE_VARCHAR, DB_DEFAULT_PRECISION, LANG_COLL_ANY /* TODO - */ )
      != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* concat the first value */

  max_allowed_size = (int) prm_get_bigint_value (PRM_ID_GROUP_CONCAT_MAX_LEN);

  if (qdata_concatenate_dbval (thread_p, agg_p->accumulator.value, dbvalue,
                               &tmp_val, max_allowed_size, "GROUP_CONCAT()") != NO_ERROR)
    {
      pr_clear_value (dbvalue);
      return ER_FAILED;
    }

  /* check for concat success */
  if (!DB_IS_NULL (&tmp_val))
    {
      (void) pr_clear_value (agg_p->accumulator.value);
      pr_clone_value (&tmp_val, agg_p->accumulator.value);
    }

  (void) pr_clear_value (&tmp_val);

  return NO_ERROR;
}

/*
 * qdata_group_concat_value() - concatenates a value
 *   return: NO_ERROR, or ER_code
 *   thread_p(in) :
 *   agg_p(in)	  : GROUP_CONCAT aggregate
 *   val_desc_p(in): value descriptor
 *   dbvalue(in)  : current value
 */
static int
qdata_group_concat_value (THREAD_ENTRY * thread_p, AGGREGATE_TYPE * agg_p, VAL_DESCR * val_desc_p, DB_VALUE * dbvalue)
{
  int max_allowed_size;
  DB_VALUE *sep_val;
  DB_VALUE tmp_val;

  DB_MAKE_NULL (&tmp_val);

  max_allowed_size = (int) prm_get_bigint_value (PRM_ID_GROUP_CONCAT_MAX_LEN);

  /* fetch seprator value. aggregate regulator variable should only
   * contain constants
   */
  if (fetch_peek_dbval (thread_p, &agg_p->group_concat_sep, val_desc_p, NULL, NULL, &sep_val) != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* add separator if specified (it may be the case for bit string) */
  if (!DB_IS_NULL (sep_val))
    {
      if (qdata_concatenate_dbval (thread_p, agg_p->accumulator.value,
                                   sep_val, &tmp_val, max_allowed_size, "GROUP_CONCAT()") != NO_ERROR)
        {
          return ER_FAILED;
        }

      /* check for concat success */
      if (!DB_IS_NULL (&tmp_val))
        {
          (void) pr_clear_value (agg_p->accumulator.value);
          pr_clone_value (&tmp_val, agg_p->accumulator.value);
        }

    }

  pr_clear_value (&tmp_val);

  if (qdata_concatenate_dbval (thread_p, agg_p->accumulator.value,
                               dbvalue, &tmp_val, max_allowed_size, "GROUP_CONCAT()") != NO_ERROR)
    {
      pr_clear_value (dbvalue);
      return ER_FAILED;
    }

  /* check for concat success */
  if (!DB_IS_NULL (&tmp_val))
    {
      (void) pr_clear_value (agg_p->accumulator.value);
      pr_clone_value (&tmp_val, agg_p->accumulator.value);
    }

  pr_clear_value (&tmp_val);

  return NO_ERROR;
}

/*
 * qdata_regu_list_to_regu_array () - extracts the regu variables from
 *				  function list to an array. Array must be
 *				  allocated by caller
 *   return: NO_ERROR, or ER_FAILED code
 *   funcp(in)		: function structure pointer
 *   array_size(in)     : max size of array (in number of entries)
 *   regu_array(out)    : array of pointers to regu-vars
 *   num_regu		: number of regu vars actually found in list
 */

int
qdata_regu_list_to_regu_array (FUNCTION_TYPE * function_p,
                               const int array_size, REGU_VARIABLE * regu_array[], int *num_regu)
{
  REGU_VARIABLE_LIST operand = function_p->operand;
  int i, num_args = 0;


  assert (array_size > 0);
  assert (regu_array != NULL);
  assert (function_p != NULL);
  assert (num_regu != NULL);

  *num_regu = 0;
  /* initialize the argument array */
  for (i = 0; i < array_size; i++)
    {
      regu_array[i] = NULL;
    }

  while (operand)
    {
      if (num_args >= array_size)
        {
          return ER_FAILED;
        }

      regu_array[num_args] = &operand->value;
      *num_regu = ++num_args;
      operand = operand->next;
    }
  return NO_ERROR;
}

/*
 * qdata_insert_substring_function () - Evaluates insert() function.
 *   return: NO_ERROR, or ER_FAILED code
 *   thread_p   : thread context
 *   funcp(in)  : function structure pointer
 *   vd(in)     : value descriptor
 *   obj_oid(in): object identifier
 *   tpl(in)    : tuple
 */
static int
qdata_insert_substring_function (THREAD_ENTRY * thread_p,
                                 FUNCTION_TYPE * function_p, VAL_DESCR * val_desc_p, OID * obj_oid_p, QFILE_TUPLE tuple)
{
  DB_VALUE *args[NUM_F_INSERT_SUBSTRING_ARGS];
  REGU_VARIABLE *regu_array[NUM_F_INSERT_SUBSTRING_ARGS];
  int i, error_status = NO_ERROR;
  int num_regu = 0;


  /* initialize the argument array */
  for (i = 0; i < NUM_F_INSERT_SUBSTRING_ARGS; i++)
    {
      args[i] = NULL;
      regu_array[i] = NULL;
    }

  error_status = qdata_regu_list_to_regu_array (function_p, NUM_F_INSERT_SUBSTRING_ARGS, regu_array, &num_regu);
  if (num_regu != NUM_F_INSERT_SUBSTRING_ARGS)
    {
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_GENERIC_FUNCTION_FAILURE, 0);
      goto error;
    }
  if (error_status != NO_ERROR)
    {
      goto error;
    }

  for (i = 0; i < NUM_F_INSERT_SUBSTRING_ARGS; i++)
    {
      error_status = fetch_peek_dbval (thread_p, regu_array[i], val_desc_p, obj_oid_p, tuple, &args[i]);
      if (error_status != NO_ERROR)
        {
          goto error;
        }
    }

  error_status = db_string_insert_substring (args[0], args[1], args[2], args[3], function_p->value);
  if (error_status != NO_ERROR)
    {
      goto error;
    }

  return NO_ERROR;

error:
  /* no error message set, keep message already set */
  return ER_FAILED;
}

/*
 * qdata_elt() - returns the argument with the index in the parameter list
 *		equal to the value passed in the first argument. Returns
 *		NULL if the first arguments is NULL, is 0, is negative or is
 *		greater than the number of the other arguments.
 */
static int
qdata_elt (THREAD_ENTRY * thread_p, FUNCTION_TYPE * function_p,
           VAL_DESCR * val_desc_p, OID * obj_oid_p, QFILE_TUPLE tuple)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_index;
  DB_VALUE *index = NULL;
  REGU_VARIABLE_LIST operand;
  int idx = 0;
  DB_VALUE *operand_value = NULL;

  /* should sync with fetch_peek_dbval ()
   */

  assert (function_p);
  assert (function_p->value);
  assert (function_p->operand);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_index);

  /* arg check null *********************************************************
   */
  error_status = fetch_peek_dbval (thread_p, &function_p->operand->value, val_desc_p, obj_oid_p, tuple, &index);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  if (DB_IS_NULL_NARGS (1, index))
    {
      DB_MAKE_NULL (function_p->value);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  index = db_value_cast_arg (index, &tmp_index, DB_TYPE_INTEGER, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (index) == DB_TYPE_INTEGER);

  idx = DB_GET_INT (index);
  if (idx <= 0)
    {
      /* index is 0 or is negative */
      DB_MAKE_NULL (function_p->value);
      goto done;
    }

  idx--;
  operand = function_p->operand->next;

  while (idx > 0 && operand != NULL)
    {
      operand = operand->next;
      idx--;
    }

  if (operand == NULL)
    {
      /* index greater than number of arguments */
      DB_MAKE_NULL (function_p->value);
      goto done;
    }

  error_status = fetch_peek_dbval (thread_p, &operand->value, val_desc_p, obj_oid_p, tuple, &operand_value);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* MySQL ELT() returns the string at the index number specified
   * in the list of arguments.
   */
  if (tp_value_coerce (operand_value, function_p->value, db_type_to_db_domain (DB_TYPE_VARCHAR)) != DOMAIN_COMPATIBLE)
    {
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
              pr_type_name (DB_VALUE_TYPE (operand_value)), pr_type_name (DB_TYPE_VARCHAR));
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_index);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (function_p->value);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_index);

  return error_status;
}

/*
 * qdata_get_index_cardinality () - gets the cardinality of an index using its name
 *			      and partial key count
 *   return: NO_ERROR, or error code
 *   thread_p(in)   : thread context
 *   db_class_name(in): string DB_VALUE holding name of class
 *   db_index_name(in): string DB_VALUE holding name of index (as it appears
 *			in '_db_index' system catalog table
 *   db_key_position(in): integer DB_VALUE holding the partial key index
 *   result_p(out)    : cardinality (integer or NULL DB_VALUE)
 */
int
qdata_get_index_cardinality (THREAD_ENTRY * thread_p,
                             DB_VALUE * db_class_name,
                             DB_VALUE * db_index_name, DB_VALUE * db_key_position, DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_db_class_name, tmp_db_index_name, tmp_db_key_position;
  char class_name[SM_MAX_IDENTIFIER_LENGTH];
  char index_name[SM_MAX_IDENTIFIER_LENGTH];
  int key_pos = 0;
  int cardinality = 0;
  int str_class_name_len;
  int str_index_name_len;

  assert (db_class_name != result_p);
  assert (db_index_name != result_p);
  assert (db_key_position != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_db_class_name, &tmp_db_index_name, &tmp_db_key_position);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (3, db_class_name, db_index_name, db_key_position))
    {
      DB_MAKE_NULL (result_p);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  db_class_name = db_value_cast_arg (db_class_name, &tmp_db_class_name, DB_TYPE_VARCHAR, &error_status);
  db_index_name = db_value_cast_arg (db_index_name, &tmp_db_index_name, DB_TYPE_VARCHAR, &error_status);
  db_key_position = db_value_cast_arg (db_key_position, &tmp_db_key_position, DB_TYPE_INTEGER, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  str_class_name_len = MIN (SM_MAX_IDENTIFIER_LENGTH - 1, DB_GET_STRING_SIZE (db_class_name));
  strncpy (class_name, DB_PULL_STRING (db_class_name), str_class_name_len);
  class_name[str_class_name_len] = '\0';

  str_index_name_len = MIN (SM_MAX_IDENTIFIER_LENGTH - 1, DB_GET_STRING_SIZE (db_index_name));
  strncpy (index_name, DB_PULL_STRING (db_index_name), str_index_name_len);
  index_name[str_index_name_len] = '\0';

  key_pos = DB_GET_INT (db_key_position);

  error_status = catalog_get_index_info_by_name (thread_p, class_name, index_name, key_pos, &cardinality);
  if (error_status == NO_ERROR)
    {
      if (cardinality < 0)
        {
          DB_MAKE_NULL (result_p);
        }
      else
        {
          DB_MAKE_INT (result_p, cardinality);
        }
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_db_class_name, &tmp_db_index_name, &tmp_db_key_position);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_db_class_name, &tmp_db_index_name, &tmp_db_key_position);

  return error_status;
}

/*
 * qdata_tuple_to_values_array () - construct an array of values from a
 *				    tuple descriptor
 * return : error code or NO_ERROR
 * thread_p (in)    : thread entry
 * tuple (in)	    : tuple descriptor
 * values (in/out)  : values array
 *
 * Note: Values are cloned in the values array
 */
int
qdata_tuple_to_values_array (UNUSED_ARG THREAD_ENTRY * thread_p, QFILE_TUPLE_DESCRIPTOR * tuple, DB_VALUE ** values)
{
  DB_VALUE *vals;
  int error = NO_ERROR, i;

  assert_release (tuple != NULL);
  assert_release (values != NULL);

  vals = (DB_VALUE *) malloc (tuple->f_cnt * sizeof (DB_VALUE));
  if (vals == NULL)
    {
      error = ER_FAILED;
      goto error_return;
    }

  for (i = 0; i < tuple->f_cnt; i++)
    {
      DB_MAKE_NULL (&vals[i]);
      error = pr_clone_value (tuple->f_valp[i], &vals[i]);
      if (error != NO_ERROR)
        {
          goto error_return;
        }
    }

  *values = vals;
  return NO_ERROR;

error_return:
  if (vals != NULL)
    {
      int j;
      for (j = 0; j < i; j++)
        {
          pr_clear_value (&vals[j]);
        }
      free_and_init (vals);
    }
  *values = NULL;
  return error;
}

/*
 * qdata_update_agg_interpolate_func_value_and_domain () -
 *   return: NO_ERROR, or error code
 *   agg_p(in): aggregate type
 *   val(in):
 *
 */
static int
qdata_update_agg_interpolate_func_value_and_domain (AGGREGATE_TYPE * agg_p, DB_VALUE * dbval)
{
  int error = NO_ERROR;
  DB_TYPE dbval_type, agg_type;
  DB_DOMAIN *agg_domain = NULL;
  TP_DOMAIN_STATUS status;

  assert (dbval != NULL);
  assert (agg_p != NULL);
  assert (agg_p->sort_list != NULL);
  assert (agg_p->list_id != NULL);
  assert (agg_p->list_id->type_list.type_cnt == 1);

  if (agg_p->function != PT_GROUP_CONCAT)
    {
      assert (false);
      error = ER_ARG_CAN_NOT_BE_CASTED_TO_DESIRED_DOMAIN;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
              error, 2, qdump_function_type_string (agg_p->function), "DOUBLE, DATETIME, TIME");
      goto end;
    }

  if (DB_IS_NULL (dbval))
    {
      goto end;
    }

  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval);
  if (TP_IS_CHAR_TYPE (dbval_type) || TP_IS_DATE_OR_TIME_TYPE (dbval_type))
    {
      agg_domain = tp_domain_resolve_value (dbval);

      status = DOMAIN_COMPATIBLE;
    }
  else
    {
      agg_domain = tp_domain_resolve_default (DB_TYPE_DOUBLE);

      status = tp_value_coerce (dbval, dbval, agg_domain);
      if (status != DOMAIN_COMPATIBLE)
        {
          if (TP_IS_CHAR_TYPE (dbval_type))
            {
              /* try datetime */
              agg_domain = tp_domain_resolve_default (DB_TYPE_DATETIME);
              status = tp_value_coerce (dbval, dbval, agg_domain);

              /* try time */
              if (status != DOMAIN_COMPATIBLE)
                {
                  agg_domain = tp_domain_resolve_default (DB_TYPE_TIME);
                  status = tp_value_coerce (dbval, dbval, agg_domain);
                }
            }
        }
    }

  if (status != DOMAIN_COMPATIBLE)
    {
      error = ER_ARG_CAN_NOT_BE_CASTED_TO_DESIRED_DOMAIN;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 2,
              qdump_function_type_string (agg_p->function), "DOUBLE, DATETIME, TIME");
      goto end;
    }

  /* set list_id domain, if it's not set */
  agg_type = TP_DOMAIN_TYPE (agg_domain);
  if (TP_DOMAIN_TYPE (agg_p->list_id->type_list.domp[0]) != agg_type)
    {
      agg_p->list_id->type_list.domp[0] = agg_domain;
    }

end:

  return error;
}

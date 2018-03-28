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
 * fetch.c - Object/Tuple value fetch routines
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include "porting.h"
#include "error_manager.h"
#include "system_parameter.h"
#include "db.h"
#include "storage_common.h"
#include "memory_alloc.h"
#include "oid.h"
#include "object_primitive.h"
#include "object_representation.h"
#include "arithmetic.h"
#include "session.h"
#include "fetch.h"
#include "list_file.h"
#include "string_opfunc.h"
#include "server_interface.h"

#include "rye_server_shm.h"

/* this must be the last header file included!!! */
#include "dbval.h"

static int fetch_peek_arith (THREAD_ENTRY * thread_p,
                             REGU_VARIABLE * regu_var, VAL_DESCR * vd,
                             OID * obj_oid, QFILE_TUPLE tpl, DB_VALUE ** peek_dbval);
static int fetch_peek_dbval_pos (REGU_VARIABLE * regu_var, QFILE_TUPLE tpl,
                                 int pos, TP_DOMAIN * pos_domain, DB_VALUE ** peek_dbval, QFILE_TUPLE * next_tpl);
static int fetch_peek_min_max_value_of_width_bucket_func (THREAD_ENTRY *
                                                          thread_p,
                                                          REGU_VARIABLE *
                                                          regu_var,
                                                          VAL_DESCR * vd,
                                                          OID * obj_oid,
                                                          QFILE_TUPLE tpl, DB_VALUE ** min, DB_VALUE ** max);

/*
 * fetch_peek_arith () -
 *   return: NO_ERROR or ER_code
 *   regu_var(in/out): Regulator Variable of an ARITH node.
 *   vd(in): Value Descriptor
 *   obj_oid(in): Object Identifier
 *   tpl(in): Tuple
 *   peek_dbval(out): Set to the value resulting from the fetch operation
 */
static int
fetch_peek_arith (THREAD_ENTRY * thread_p, REGU_VARIABLE * regu_var,
                  VAL_DESCR * vd, OID * obj_oid, QFILE_TUPLE tpl, DB_VALUE ** peek_dbval)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_left, tmp_right, tmp_third;
  ARITH_TYPE *arithptr = regu_var->value.arithptr;
  DB_VALUE *peek_left, *peek_right, *peek_third, *peek_fourth;
  DB_VALUE tmp_val;
#if 0                           /* TODO - */
  TP_DOMAIN *original_domain = NULL;
#endif
  TP_DOMAIN *target_domain, *arg1, *arg2, *arg3;
//  TP_DOMAIN tmp_arg1_domain, tmp_arg2_domain;
  TP_DOMAIN_STATUS dom_status;
  DB_LOGICAL ev_res = V_UNKNOWN;

  /* arg init tmp ************************************************************
   */
  DB_MAKE_NULL_NARGS (4, &tmp_left, &tmp_right, &tmp_third, &tmp_val);

  if (REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST))
    {
      *peek_dbval = arithptr->value;

      return NO_ERROR;
    }

  assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));

  peek_left = NULL;
  peek_right = NULL;
  peek_third = NULL;
  peek_fourth = NULL;

  if (thread_get_recursion_depth (thread_p) > prm_get_integer_value (PRM_ID_MAX_RECURSION_SQL_DEPTH))
    {
      int error = ER_MAX_RECURSION_SQL_DEPTH;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, prm_get_integer_value (PRM_ID_MAX_RECURSION_SQL_DEPTH));
      return error;
    }
  thread_inc_recursion_depth (thread_p);

  /* clear any previous result */
  pr_clear_value (arithptr->value);

  /* fetch values */
  switch (arithptr->opcode)
    {
    case T_SUBSTRING:
    case T_LPAD:
    case T_RPAD:
    case T_REPLACE:
    case T_TRANSLATE:
    case T_TO_CHAR:
    case T_TO_DATE:
    case T_TO_TIME:
    case T_TO_DATETIME:
    case T_TO_NUMBER:
    case T_INSTR:
    case T_MID:
    case T_DATE_ADD:
    case T_DATE_SUB:
    case T_INDEX_PREFIX:

      /* fetch lhs, rhs, and third value */
      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }

      if (DB_IS_NULL (peek_left))
        {
          PRIM_SET_NULL (arithptr->value);      /* done */
          goto fetch_peek_arith_end;
        }

      if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
        {
          goto error;
        }

      if (arithptr->opcode == T_SUBSTRING && arithptr->thirdptr == NULL)
        {
          break;
        }
      if (fetch_peek_dbval (thread_p, arithptr->thirdptr, vd, obj_oid, tpl, &peek_third) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_STR_TO_DATE:
    case T_DATE_FORMAT:
    case T_TIME_FORMAT:
    case T_FORMAT:
      if (fetch_peek_dbval (thread_p, arithptr->thirdptr, vd, obj_oid, tpl, &peek_third) != NO_ERROR)
        {
          goto error;
        }
    case T_ADD:
    case T_SUB:
    case T_MUL:
    case T_DIV:
    case T_MOD:
    case T_POSITION:
    case T_FINDINSET:
    case T_MONTHS_BETWEEN:
    case T_TRIM:
    case T_SHA_TWO:
    case T_LTRIM:
    case T_RTRIM:
    case T_POWER:
    case T_ROUND:
    case T_LOG:
    case T_TRUNC:
    case T_STRCAT:
    case T_NULLIF:
    case T_BIT_AND:
    case T_BIT_OR:
    case T_BIT_XOR:
    case T_BITSHIFT_LEFT:
    case T_BITSHIFT_RIGHT:
    case T_INTDIV:
    case T_STRCMP:
    case T_ATAN2:
    case T_ADDDATE:
    case T_SUBDATE:
    case T_DATEDIFF:
    case T_TIMEDIFF:
      /* fetch lhs and rhs value */
      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }

      if (DB_IS_NULL (peek_left))
        {
          PRIM_SET_NULL (arithptr->value);      /* done */
          goto fetch_peek_arith_end;
        }

      if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_FROM_UNIXTIME:

      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }
      if (arithptr->rightptr != NULL)
        {
          if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
            {
              goto error;
            }
        }
      if (arithptr->thirdptr != NULL)
        {
          if (fetch_peek_dbval (thread_p, arithptr->thirdptr, vd, obj_oid, tpl, &peek_third) != NO_ERROR)
            {
              goto error;
            }
        }
      break;

    case T_SUBSTRING_INDEX:
    case T_CONCAT_WS:
    case T_FIELD:
    case T_INDEX_CARDINALITY:
      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }
      if (arithptr->rightptr != NULL)
        {
          if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
            {
              goto error;
            }
        }
      if (fetch_peek_dbval (thread_p, arithptr->thirdptr, vd, obj_oid, tpl, &peek_third) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_CONV:
      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }
      if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
        {
          goto error;
        }
      if (fetch_peek_dbval (thread_p, arithptr->thirdptr, vd, obj_oid, tpl, &peek_third) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LOCATE:
      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }
      if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
        {
          goto error;
        }
      if (arithptr->thirdptr != NULL)
        {
          if (fetch_peek_dbval (thread_p, arithptr->thirdptr, vd, obj_oid, tpl, &peek_third) != NO_ERROR)
            {
              goto error;
            }
        }
      break;

    case T_CONCAT:
      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }

      if (DB_IS_NULL (peek_left))
        {
          PRIM_SET_NULL (arithptr->value);      /* done */
          goto fetch_peek_arith_end;
        }

      if (arithptr->rightptr != NULL)
        {
          if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
            {
              goto error;
            }
        }
      break;

    case T_REPEAT:
    case T_LEAST:
    case T_GREATEST:
    case T_CAST:
      /* fetch both lhs and rhs value */
      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }

      if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LEFT:
    case T_RIGHT:
      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }

      if (DB_IS_NULL (peek_left))
        {
          PRIM_SET_NULL (arithptr->value);      /* done */
          goto fetch_peek_arith_end;
        }

      if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_MAKEDATE:
    case T_WEEK:
      /* fetch both lhs and rhs value */
      if (fetch_peek_dbval (thread_p, arithptr->leftptr,
                            vd, obj_oid, tpl, &peek_left) != NO_ERROR
          || fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_MAKETIME:
      if (fetch_peek_dbval (thread_p, arithptr->leftptr,
                            vd, obj_oid, tpl, &peek_left) != NO_ERROR
          || fetch_peek_dbval (thread_p, arithptr->rightptr,
                               vd, obj_oid, tpl, &peek_right) != NO_ERROR
          || fetch_peek_dbval (thread_p, arithptr->thirdptr, vd, obj_oid, tpl, &peek_third) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_CASE:
    case T_DECODE:
    case T_IF:
    case T_IFNULL:
    case T_NVL:
    case T_PREDICATE:
    case T_COALESCE:
    case T_NVL2:
      /* defer fetch values */
      break;

    case T_LAST_DAY:
#if defined(ENABLE_UNUSED_FUNCTION)
    case T_UNPLUS:
#endif /* ENABLE_UNUSED_FUNCTION */
    case T_UNMINUS:
    case T_OCTET_LENGTH:
    case T_BIT_LENGTH:
    case T_CHAR_LENGTH:
    case T_LOWER:
    case T_UPPER:
    case T_HEX:
    case T_ASCII:
    case T_SPACE:
    case T_MD5:
    case T_SHA_ONE:
    case T_TO_BASE64:
    case T_FROM_BASE64:
    case T_BIN:
    case T_EXTRACT:
    case T_FLOOR:
    case T_CEIL:
    case T_SIGN:
    case T_ABS:
    case T_EXP:
    case T_SQRT:
    case T_BIT_NOT:
    case T_REVERSE:
    case T_BIT_COUNT:
    case T_ACOS:
    case T_ASIN:
    case T_SIN:
    case T_COS:
    case T_TAN:
    case T_COT:
    case T_DEGREES:
    case T_RADIANS:
    case T_LN:
    case T_LOG2:
    case T_LOG10:
    case T_ATAN:
    case T_DATE:
    case T_TIME:
    case T_ISNULL:
    case T_RAND:
    case T_DRAND:
    case T_RANDOM:
    case T_DRANDOM:
    case T_TYPEOF:
    case T_INET_ATON:
    case T_INET_NTOA:
      /* fetch rhs value */
      if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_YEAR:
    case T_MONTH:
    case T_DAY:
    case T_QUARTER:
    case T_WEEKDAY:
    case T_DAYOFWEEK:
    case T_DAYOFYEAR:
    case T_TODAYS:
    case T_FROMDAYS:

    case T_HOUR:
    case T_MINUTE:
    case T_SECOND:
    case T_TIMETOSEC:
    case T_SECTOTIME:
      /* fetch rhs value */
      if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_UNIX_TIMESTAMP:
    case T_DEFAULT:
      if (arithptr->rightptr)
        {
          if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
            {
              goto error;
            }
        }
      break;

    case T_LIKE_LOWER_BOUND:
    case T_LIKE_UPPER_BOUND:
      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }
      if (arithptr->rightptr)
        {
          if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
            {
              goto error;
            }
        }
      break;

    case T_SYS_DATE:
    case T_SYS_TIME:
    case T_SYS_DATETIME:
    case T_UTC_TIME:
    case T_UTC_DATE:
    case T_HA_STATUS:
    case T_SHARD_GROUPID:
    case T_SHARD_LOCKNAME:
    case T_SHARD_NODEID:
    case T_PI:
    case T_LIST_DBS:
    case T_TRACE_STATS:
      /* nothing to fetch */
      break;

    case T_CHR:
      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_WIDTH_BUCKET:
      if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
        {
          goto error;
        }

      /* get peek_right, peek_third
       * we use PT_BETWEEN with PT_BETWEEN_GE_LT to represent the two args.
       */
      if (fetch_peek_min_max_value_of_width_bucket_func (thread_p,
                                                         arithptr->rightptr,
                                                         vd, obj_oid, tpl, &peek_right, &peek_third) != NO_ERROR)
        {
          goto error;
        }

      if (fetch_peek_dbval (thread_p, arithptr->thirdptr, vd, obj_oid, tpl, &peek_fourth) != NO_ERROR)
        {
          goto error;
        }
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      goto error;
    }

  /* evaluate arithmetic expression */

  switch (arithptr->opcode)
    {
    case T_ADD:
      {
        if (qdata_add_dbval (peek_left, peek_right, arithptr->value) != NO_ERROR)
          {
            goto error;
          }
      }
      break;

    case T_BIT_NOT:
      if (qdata_bit_not_dbval (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_BIT_AND:
      if (qdata_bit_and_dbval (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_BIT_OR:
      if (qdata_bit_or_dbval (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_BIT_XOR:
      if (qdata_bit_xor_dbval (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_BITSHIFT_LEFT:
    case T_BITSHIFT_RIGHT:
      if (qdata_bit_shift_dbval (peek_left, peek_right, arithptr->opcode, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_INTDIV:
      if (qdata_divmod_dbval (peek_left, peek_right, arithptr->opcode, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_BIT_COUNT:
      if (db_bit_count_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_SUB:
      if (qdata_subtract_dbval (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_MUL:
      if (qdata_multiply_dbval (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_DIV:
      if (qdata_divide_dbval (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

#if defined(ENABLE_UNUSED_FUNCTION)
    case T_UNPLUS:
      if (!qdata_copy_db_value (arithptr->value, peek_right))
        {
          goto error;
        }
      break;
#endif /* ENABLE_UNUSED_FUNCTION */

    case T_UNMINUS:
      if (qdata_unary_minus_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_DEFAULT:
      if (arithptr->rightptr)
        {
          if (qdata_copy_db_value (arithptr->value, peek_right) != true)
            {
              goto error;
            }
        }
      else
        {
          PRIM_SET_NULL (arithptr->value);
        }
      break;

    case T_MOD:
      if (db_mod_dbval (arithptr->value, peek_left, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_FLOOR:
      if (db_floor_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_CEIL:
      if (db_ceil_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_SIGN:
      if (db_sign_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_ABS:
      if (db_abs_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_EXP:
      if (db_exp_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_SQRT:
      if (db_sqrt_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_SIN:
      if (db_sin_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_COS:
      if (db_cos_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TAN:
      if (db_tan_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_COT:
      if (db_cot_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LN:
      if (db_log_generic_dbval (arithptr->value, peek_right, -1 /* convention for e base */ ) !=
          NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LOG2:
      if (db_log_generic_dbval (arithptr->value, peek_right, 2) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LOG10:
      if (db_log_generic_dbval (arithptr->value, peek_right, 10) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_ACOS:
      if (db_acos_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_ASIN:
      if (db_asin_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_DEGREES:
      if (db_degrees_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_DATE:
      if (db_date_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TIME:
      if (db_time_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_RADIANS:
      if (db_radians_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_POWER:
      if (db_power_dbval (arithptr->value, peek_left, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_ROUND:
      if (db_round_dbval (arithptr->value, peek_left, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LOG:
      if (db_log_dbval (arithptr->value, peek_left, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TRUNC:
      if (db_trunc_dbval (arithptr->value, peek_left, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_CHR:
      if (db_string_chr (arithptr->value, peek_left) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_INSTR:
      if (db_string_instr (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_POSITION:
      if (db_string_position (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_FINDINSET:
      if (db_find_string_in_in_set (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_SUBSTRING:
      if (db_string_substring (arithptr->misc_operand, peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_OCTET_LENGTH:
      if (DB_IS_NULL (peek_right))
        {
          PRIM_SET_NULL (arithptr->value);
        }
      else
        {
          peek_right = db_value_cast_arg (peek_right, &tmp_right, DB_TYPE_VARCHAR, &error_status);

          if (error_status != NO_ERROR)
            {
              assert (er_errid () != NO_ERROR);
              goto error;
            }

          db_make_int (arithptr->value, db_get_string_size (peek_right));
        }
      break;

    case T_BIT_LENGTH:
      if (DB_IS_NULL (peek_right))
        {
          PRIM_SET_NULL (arithptr->value);
        }
      else
        {
          int len = 0;

          peek_right = db_value_cast_arg (peek_right, &tmp_right, DB_TYPE_VARBIT, &error_status);

          if (error_status != NO_ERROR)
            {
              assert (er_errid () != NO_ERROR);
              goto error;
            }

          db_get_varbit (peek_right, &len);
          db_make_int (arithptr->value, len);
        }
      break;

    case T_CHAR_LENGTH:
      if (DB_IS_NULL (peek_right))
        {
          PRIM_SET_NULL (arithptr->value);
        }
      else
        {
          peek_right = db_value_cast_arg (peek_right, &tmp_right, DB_TYPE_VARCHAR, &error_status);

          if (error_status != NO_ERROR)
            {
              assert (er_errid () != NO_ERROR);
              goto error;
            }

          db_make_int (arithptr->value, DB_GET_STRING_LENGTH (peek_right));
        }
      break;

    case T_LOWER:
      if (db_string_lower (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_UPPER:
      if (db_string_upper (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_HEX:
      if (db_hex (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_ASCII:
      if (db_ascii (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_CONV:
      if (db_conv (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_BIN:
      if (db_bigint_to_binary_string (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_MD5:
      if (db_string_md5 (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_SHA_ONE:
      if (db_string_sha_one (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_SHA_TWO:
      if (db_string_sha_two (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TO_BASE64:
      if (db_string_to_base64 (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_FROM_BASE64:
      if (db_string_from_base64 (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_SPACE:
      if (db_string_space (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TRIM:
      if (db_string_trim (arithptr->misc_operand, peek_right, peek_left, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LTRIM:
      if (db_string_trim (LEADING, peek_right, peek_left, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_RTRIM:
      if (db_string_trim (TRAILING, peek_right, peek_left, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_FROM_UNIXTIME:
      if (db_from_unixtime (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LPAD:
      if (db_string_pad (LEADING, peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_RPAD:
      if (db_string_pad (TRAILING, peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_REPLACE:
      if (db_string_replace (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TRANSLATE:
      if (db_string_translate (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LAST_DAY:
      if (db_last_day (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TIME_FORMAT:
      if (db_time_format (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_YEAR:
      if (db_get_date_item (peek_right, PT_YEARF, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_MONTH:
      if (db_get_date_item (peek_right, PT_MONTHF, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_DAY:
      if (db_get_date_item (peek_right, PT_DAYF, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_HOUR:
      if (db_get_time_item (peek_right, PT_HOURF, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_MINUTE:
      if (db_get_time_item (peek_right, PT_MINUTEF, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_SECOND:
      if (db_get_time_item (peek_right, PT_SECONDF, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_QUARTER:
      if (db_get_date_quarter (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_WEEKDAY:
      if (db_get_date_weekday (peek_right, PT_WEEKDAY, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_DAYOFWEEK:
      if (db_get_date_weekday (peek_right, PT_DAYOFWEEK, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_DAYOFYEAR:
      if (db_get_date_dayofyear (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TODAYS:
      if (db_get_date_totaldays (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_FROMDAYS:
      if (db_get_date_from_days (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TIMETOSEC:
      if (db_convert_time_to_sec (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_SECTOTIME:
      if (db_convert_sec_to_time (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LIKE_LOWER_BOUND:
    case T_LIKE_UPPER_BOUND:
      if (db_like_bound (peek_left, peek_right, arithptr->value, (arithptr->opcode == T_LIKE_LOWER_BOUND)) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_MAKEDATE:
      if (db_add_days_to_year (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_MAKETIME:
      if (db_convert_to_time (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_WEEK:
      if (db_get_date_week (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_UNIX_TIMESTAMP:
      if (db_unix_timestamp (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_MONTHS_BETWEEN:
      if (db_months_between (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_ATAN2:
      if (db_atan2_dbval (arithptr->value, peek_left, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_ATAN:
      if (db_atan_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_FORMAT:
      if (db_format (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_DATE_FORMAT:
      if (db_date_format (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }

      break;

    case T_STR_TO_DATE:
      if (db_str_to_date (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_ADDDATE:
      if (db_date_add_interval_days (arithptr->value, peek_left, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_DATE_ADD:
      if (DB_IS_NULL (peek_third))
        {
          PRIM_SET_NULL (arithptr->value);
        }
      else
        {
          int unit;

          peek_third = db_value_cast_arg (peek_third, &tmp_third, DB_TYPE_INTEGER, &error_status);

          if (error_status != NO_ERROR)
            {
              assert (er_errid () != NO_ERROR);
              goto error;
            }

          unit = DB_GET_INTEGER (peek_third);
          if (db_date_add_interval_expr (arithptr->value, peek_left, peek_right, unit) != NO_ERROR)
            {
              goto error;
            }
        }
      break;

    case T_SUBDATE:
      if (db_date_sub_interval_days (arithptr->value, peek_left, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_DATEDIFF:
      if (db_date_diff (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TIMEDIFF:
      if (db_time_diff (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_DATE_SUB:
      if (DB_IS_NULL (peek_third))
        {
          PRIM_SET_NULL (arithptr->value);
        }
      else
        {
          int unit;

          peek_third = db_value_cast_arg (peek_third, &tmp_third, DB_TYPE_INTEGER, &error_status);

          if (error_status != NO_ERROR)
            {
              assert (er_errid () != NO_ERROR);
              goto error;
            }

          unit = DB_GET_INTEGER (peek_third);
          if (db_date_sub_interval_expr (arithptr->value, peek_left, peek_right, unit) != NO_ERROR)
            {
              goto error;
            }
        }
      break;

    case T_SYS_DATE:
      DB_MAKE_ENCODED_DATE (arithptr->value, &vd->sys_datetime.date);
      break;

    case T_SYS_TIME:
      {
        DB_TIME db_time;

        db_time = vd->sys_datetime.time / 1000;
        DB_MAKE_ENCODED_TIME (arithptr->value, &db_time);
        break;
      }

    case T_SYS_DATETIME:
      DB_MAKE_DATETIME (arithptr->value, &vd->sys_datetime);
      break;

    case T_UTC_TIME:
      {
        DB_TIME db_time;
        DB_VALUE timezone;
        int timezone_val;

        db_time = vd->sys_datetime.time / 1000;

        /* extract the timezone part */
        if (db_sys_timezone (&timezone) != NO_ERROR)
          {
            goto error;
          }
        timezone_val = DB_GET_INT (&timezone);
        db_time = db_time + timezone_val * 60 + SECONDS_OF_ONE_DAY;
        db_time = db_time % SECONDS_OF_ONE_DAY;

        DB_MAKE_ENCODED_TIME (arithptr->value, &db_time);
        break;
      }

    case T_UTC_DATE:
      {
        DB_VALUE timezone;
        DB_BIGINT timezone_milis;
        DB_DATETIME db_datetime;
        DB_DATE db_date;

        /* extract the timezone part */
        if (db_sys_timezone (&timezone) != NO_ERROR)
          {
            goto error;
          }
        timezone_milis = DB_GET_INT (&timezone) * 60000;
        if (db_add_int_to_datetime (&vd->sys_datetime, timezone_milis, &db_datetime) != NO_ERROR)
          {
            goto error;
          }

        db_date = db_datetime.date;
        DB_MAKE_ENCODED_DATE (arithptr->value, &db_date);
        break;
      }

    case T_HA_STATUS:
      db_make_string (arithptr->value, logtb_find_current_ha_status (thread_p));
      break;

    case T_SHARD_GROUPID:
      db_make_int (arithptr->value, logtb_find_current_shard_groupid (thread_p));
      break;

    case T_SHARD_LOCKNAME:
      db_make_string (arithptr->value, logtb_find_current_shard_lockname (thread_p));
      break;

    case T_SHARD_NODEID:
      db_make_int (arithptr->value, svr_shm_get_nodeid ());
      break;

    case T_TO_CHAR:
      if (db_to_char (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }

      break;

    case T_TO_DATE:
      if (db_to_date (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TO_TIME:
      if (db_to_time (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TO_DATETIME:
      if (db_to_datetime (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TO_NUMBER:
      if (db_to_number (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_CAST:
      target_domain = tp_domain_resolve_value (peek_left);
      assert (TP_DOMAIN_TYPE (target_domain) != DB_TYPE_NULL);
      assert (TP_DOMAIN_TYPE (target_domain) != DB_TYPE_VARIABLE);

      dom_status = tp_value_cast (peek_right, arithptr->value, target_domain);
      if (dom_status != DOMAIN_COMPATIBLE)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                  pr_type_name (DB_VALUE_DOMAIN_TYPE (peek_right)), pr_type_name (TP_DOMAIN_TYPE (target_domain)));
          goto error;
        }
      break;

    case T_CASE:
    case T_DECODE:
    case T_IF:
      {
        /* Syntax
         *   DECODE (expr, search, result [, search , result]... [, default])
         * Return Value
         *   The data type of the first result argument.
         */

        /* set pred is not constant */
        REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_NOT_CONST);
        assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));
        /* fetch values */
        ev_res = eval_pred (thread_p, arithptr->pred, vd, obj_oid);

        switch (ev_res)
          {
          case V_FALSE:
          case V_UNKNOWN:      /* unknown pred result, including cases of NULL pred operands */
            if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
              {
                goto error;
              }

            if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
              {
                goto error;
              }

            arg1 = tp_domain_resolve_value (peek_left);
            arg2 = tp_domain_resolve_value (peek_right);

            if (DB_VALUE_DOMAIN_TYPE (peek_left) == DB_TYPE_NULL        /* TODO - */
                || TP_IS_FLOATING_NUMBER_TYPE (DB_VALUE_DOMAIN_TYPE (peek_left)))
              {
                target_domain = tp_infer_common_domain (arg1, arg2, false);
              }
            else
              {
                target_domain = tp_infer_common_domain (arg1, arg2, true);
              }

            dom_status = tp_value_coerce (peek_right, arithptr->value, target_domain);
            if (dom_status != DOMAIN_COMPATIBLE)
              {
                er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE,
                        2, pr_type_name (DB_VALUE_DOMAIN_TYPE (peek_right)),
                        pr_type_name (TP_DOMAIN_TYPE (target_domain)));
                goto error;
              }

            break;

          case V_TRUE:
            if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
              {
                goto error;
              }

            if (qdata_copy_db_value (arithptr->value, peek_left) != true)
              {
                goto error;
              }
            break;

          default:
            goto error;
          }                     /* switch */
      }
      break;

    case T_PREDICATE:
      /* set pred is not constant */
      REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_NOT_CONST);
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));
      /* return 0,1 or NULL accordingly */
      peek_left = &tmp_val;

      ev_res = eval_pred (thread_p, arithptr->pred, vd, obj_oid);

      switch (ev_res)
        {
        case V_UNKNOWN:
          DB_MAKE_NULL (peek_left);
          break;
        case V_FALSE:
          DB_MAKE_INT (peek_left, 0);
          break;
        case V_TRUE:
          DB_MAKE_INT (peek_left, 1);
          break;
        default:
          goto error;
        }

      if (qdata_copy_db_value (arithptr->value, peek_left) != true)
        {
          goto error;
        }
      break;

    case T_NVL:
    case T_IFNULL:
    case T_COALESCE:
      {
        /* Syntax
         *   IFNULL (exp, replacement-exp)
         *   NVL (exp, replacement-exp)
         * Return Value
         *   The data type of the return value is always the same as
         *   the data type of the base expression.
         *
         * Syntax
         *   COALESCE (expr [, expr]...)
         * Return Value
         *   Data type of the first argument.
         */
        DB_VALUE *src;

        if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
          {
            goto error;
          }

        if (DB_IS_NULL (peek_left))
          {
            if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
              {
                goto error;
              }

            arg1 = tp_domain_resolve_value (peek_left);
            arg2 = tp_domain_resolve_value (peek_right);

            if (DB_VALUE_DOMAIN_TYPE (peek_left) == DB_TYPE_NULL        /* TODO - */
                || TP_IS_FLOATING_NUMBER_TYPE (DB_VALUE_DOMAIN_TYPE (peek_left)))
              {
                target_domain = tp_infer_common_domain (arg1, arg2, false);
              }
            else
              {
                target_domain = tp_infer_common_domain (arg1, arg2, true);
              }

            src = peek_right;
          }
        else
          {
            target_domain = tp_domain_resolve_value (peek_left);

            src = peek_left;
          }

        if (tp_value_coerce (src, arithptr->value, target_domain) != DOMAIN_COMPATIBLE)
          {
            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                    pr_type_name (DB_VALUE_DOMAIN_TYPE (src)), pr_type_name (TP_DOMAIN_TYPE (target_domain)));
            goto error;
          }
      }
      break;
    case T_NVL2:
      {
        /* Syntax
         *   NVL2 (expr1, expr2, expr3)
         * Return Value
         *   The data type of the return value is always the data type of expr2
         */
        DB_VALUE *src;

        if (fetch_peek_dbval (thread_p, arithptr->leftptr, vd, obj_oid, tpl, &peek_left) != NO_ERROR)
          {
            goto error;
          }

        if (DB_IS_NULL (peek_left))
          {
            if (peek_third == NULL)
              {
                if (fetch_peek_dbval (thread_p, arithptr->thirdptr, vd, obj_oid, tpl, &peek_third) != NO_ERROR)
                  {
                    goto error;
                  }
              }

            if (peek_right == NULL)
              {
                if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
                  {
                    goto error;
                  }
              }

            arg2 = tp_domain_resolve_value (peek_right);
            arg3 = tp_domain_resolve_value (peek_third);

            if (DB_VALUE_DOMAIN_TYPE (peek_right) == DB_TYPE_NULL       /* TODO - */
                || TP_IS_FLOATING_NUMBER_TYPE (DB_VALUE_DOMAIN_TYPE (peek_right)))
              {
                target_domain = tp_infer_common_domain (arg2, arg3, false);
              }
            else
              {
                target_domain = tp_infer_common_domain (arg2, arg3, true);
              }

            src = peek_third;
          }
        else
          {
            if (peek_right == NULL)
              {
                if (fetch_peek_dbval (thread_p, arithptr->rightptr, vd, obj_oid, tpl, &peek_right) != NO_ERROR)
                  {
                    goto error;
                  }
              }

            if (DB_VALUE_DOMAIN_TYPE (peek_right) == DB_TYPE_NULL)
              {                 /* TODO - */
                target_domain = tp_domain_resolve_default (DB_TYPE_VARCHAR);
              }
            else
              {
                target_domain = tp_domain_resolve_value (peek_right);
              }

            src = peek_right;
          }

        if (tp_value_coerce (src, arithptr->value, target_domain) != DOMAIN_COMPATIBLE)
          {
            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                    pr_type_name (DB_VALUE_DOMAIN_TYPE (src)), pr_type_name (TP_DOMAIN_TYPE (target_domain)));

            goto error;
          }
      }
      break;

    case T_ISNULL:
      if (DB_IS_NULL (peek_right))
        {
          DB_MAKE_INTEGER (arithptr->value, 1);
        }
      else
        {
          DB_MAKE_INTEGER (arithptr->value, 0);
        }
      break;

    case T_CONCAT:
      /* MySQL CONCAT() function is used to add two or more strings.
       */
      target_domain = tp_domain_resolve_default (DB_TYPE_VARCHAR);

      if (arithptr->rightptr != NULL)
        {
          if (qdata_strcat_dbval (peek_left, peek_right, arithptr->value) != NO_ERROR)
            {
              goto error;
            }
        }
      else
        {
          if (tp_value_coerce (peek_left, arithptr->value, target_domain) != DOMAIN_COMPATIBLE)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                      pr_type_name (DB_VALUE_DOMAIN_TYPE (peek_left)), pr_type_name (TP_DOMAIN_TYPE (target_domain)));
              goto error;
            }
        }
      break;

    case T_CONCAT_WS:
      /* MySQL CONCAT_WS() function is used to join two or more strings
       * with separator.
       */
      target_domain = tp_domain_resolve_default (DB_TYPE_VARCHAR);

      if (DB_IS_NULL (peek_third))
        {
          PRIM_SET_NULL (arithptr->value);
          break;
        }

      if (arithptr->rightptr != NULL)
        {
          if (DB_IS_NULL (peek_left) && DB_IS_NULL (peek_right))
            {
              PRIM_SET_NULL (arithptr->value);
            }
          else if (DB_IS_NULL (peek_left))
            {
              if (tp_value_coerce (peek_right, arithptr->value, target_domain) != DOMAIN_COMPATIBLE)
                {
                  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE,
                          2, pr_type_name (DB_VALUE_DOMAIN_TYPE (peek_right)),
                          pr_type_name (TP_DOMAIN_TYPE (target_domain)));
                  goto error;
                }
            }
          else if (DB_IS_NULL (peek_right))
            {
              if (tp_value_coerce (peek_left, arithptr->value, target_domain) != DOMAIN_COMPATIBLE)
                {
                  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE,
                          2, pr_type_name (DB_VALUE_DOMAIN_TYPE (peek_left)),
                          pr_type_name (TP_DOMAIN_TYPE (target_domain)));
                  goto error;
                }
            }
          else
            {
              if (qdata_strcat_dbval (peek_left, peek_third, &tmp_val) != NO_ERROR)
                {
                  goto error;
                }
              if (qdata_strcat_dbval (&tmp_val, peek_right, arithptr->value) != NO_ERROR)
                {
                  pr_clear_value (&tmp_val);
                  goto error;
                }
              pr_clear_value (&tmp_val);
            }
        }
      else
        {
          if (DB_IS_NULL (peek_left))
            {
              PRIM_SET_NULL (arithptr->value);
            }
          else
            {
              if (tp_value_coerce (peek_left, arithptr->value, target_domain) != DOMAIN_COMPATIBLE)
                {
                  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE,
                          2, pr_type_name (DB_VALUE_DOMAIN_TYPE (peek_left)),
                          pr_type_name (TP_DOMAIN_TYPE (target_domain)));
                  goto error;
                }
            }
        }
      break;

    case T_FIELD:
      /* arg check null ****************************************************
       */
      if (DB_IS_NULL (peek_third))
        {
          db_make_int (arithptr->value, 0);
          break;
        }

      if (REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FIELD_COMPARE))
        {
          int cmp_res = DB_UNK;

          /* arg cast type ***************************************************
           */
          peek_left = db_value_cast_arg (peek_left, &tmp_left, DB_TYPE_VARCHAR, &error_status);
          peek_right = db_value_cast_arg (peek_right, &tmp_right, DB_TYPE_VARCHAR, &error_status);
          peek_third = db_value_cast_arg (peek_third, &tmp_third, DB_TYPE_VARCHAR, &error_status);

          if (error_status != NO_ERROR)
            {
              assert (er_errid () != NO_ERROR);
              goto error;
            }

          cmp_res = tp_value_compare (peek_third, peek_left, 1, 0, NULL);

          if (cmp_res == DB_EQ)
            {
              db_make_int (arithptr->value, 1);
              break;
            }

          cmp_res = tp_value_compare (peek_third, peek_right, 1, 0, NULL);

          if (cmp_res == DB_EQ)
            {
              db_make_int (arithptr->value, 2);
              break;
            }

          if (REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FIELD_NESTED))
            {
              /* we have a T_FIELD parent, return level */
              db_make_int (arithptr->value, -3);
            }
          else
            {
              /* no parent and no match */
              db_make_int (arithptr->value, 0);
            }
        }
      else
        {
          int i;

          /* arg cast type ***************************************************
           */
          peek_left = db_value_cast_arg (peek_left, &tmp_left, DB_TYPE_INTEGER, &error_status);

          if (error_status != NO_ERROR)
            {
              assert (er_errid () != NO_ERROR);
              goto error;
            }

          i = DB_GET_INTEGER (peek_left);
          if (i > 0)
            {
              db_make_int (arithptr->value, i);
            }
          else
            {
              int cmp_res = DB_UNK;

              /* arg cast type ***************************************************
               */
              peek_right = db_value_cast_arg (peek_right, &tmp_right, DB_TYPE_VARCHAR, &error_status);
              peek_third = db_value_cast_arg (peek_third, &tmp_third, DB_TYPE_VARCHAR, &error_status);

              if (error_status != NO_ERROR)
                {
                  assert (er_errid () != NO_ERROR);
                  goto error;
                }

              cmp_res = tp_value_compare (peek_third, peek_right, 1, 0, NULL);

              if (cmp_res == DB_EQ)
                {
                  /* match */
                  db_make_int (arithptr->value, -i);
                }
              else
                {
                  if (REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FIELD_NESTED))
                    {
                      /* we have a T_FIELD parent, return level */
                      db_make_int (arithptr->value, i - 1);
                    }
                  else
                    {
                      /* no parent and no match */
                      db_make_int (arithptr->value, 0);
                    }
                }
            }
        }
      break;

    case T_REPEAT:
      if (db_string_repeat (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LEFT:
      {
        assert (peek_right != NULL);

        peek_right = db_value_cast_arg (peek_right, &tmp_right, DB_TYPE_INTEGER, &error_status);

        if (error_status != NO_ERROR)
          {
            assert (er_errid () != NO_ERROR);
            goto error;
          }

        db_make_int (&tmp_val, 0);

        if (db_string_substring (SUBSTRING, peek_left, &tmp_val, peek_right, arithptr->value) != NO_ERROR)
          {
            goto error;
          }
      }
      break;

    case T_RIGHT:
      if (DB_IS_NULL (peek_right))
        {
          PRIM_SET_NULL (arithptr->value);
        }
      else
        {
          assert (peek_right != NULL);

          if (db_string_char_length (peek_left, &tmp_val) != NO_ERROR)
            {
              goto error;
            }

          if (DB_IS_NULL (&tmp_val))
            {
              PRIM_SET_NULL (arithptr->value);
              break;
            }

          peek_right = db_value_cast_arg (peek_right, &tmp_right, DB_TYPE_INTEGER, &error_status);

          if (error_status != NO_ERROR)
            {
              assert (er_errid () != NO_ERROR);
              goto error;
            }

          /* If len, defined as second argument, is negative value,
           * RIGHT function returns the entire string.
           * It's same behavior with LEFT and SUBSTRING.
           */
          if (DB_GET_INTEGER (peek_right) < 0)
            {
              db_make_int (&tmp_val, 0);
            }
          else
            {
              db_make_int (&tmp_val, DB_GET_INTEGER (&tmp_val) - DB_GET_INTEGER (peek_right) + 1);
            }

          if (db_string_substring (SUBSTRING, peek_left, &tmp_val, peek_right, arithptr->value) != NO_ERROR)
            {
              goto error;
            }
        }
      break;

    case T_LOCATE:
      if (DB_IS_NULL (peek_left) || DB_IS_NULL (peek_right) || (arithptr->thirdptr && DB_IS_NULL (peek_third)))
        {
          PRIM_SET_NULL (arithptr->value);
        }
      else
        {
          if (!arithptr->thirdptr)
            {
              if (db_string_position (peek_left, peek_right, arithptr->value) != NO_ERROR)
                {
                  goto error;
                }
            }
          else
            {
              DB_VALUE tmp_len, tmp_arg3;
              int tmp;

              DB_MAKE_NULL (&tmp_val);

              peek_third = db_value_cast_arg (peek_third, &tmp_third, DB_TYPE_INTEGER, &error_status);
              if (error_status != NO_ERROR)
                {
                  assert (er_errid () != NO_ERROR);
                  goto error;
                }

              tmp = DB_GET_INTEGER (peek_third);

              if (tmp < 1)
                {
                  db_make_int (&tmp_arg3, 1);
                }
              else
                {
                  db_make_int (&tmp_arg3, tmp);
                }

              if (db_string_char_length (peek_right, &tmp_len) != NO_ERROR)
                {
                  goto error;
                }
              if (DB_IS_NULL (&tmp_len))
                {
                  goto error;
                }

              db_make_int (&tmp_len, DB_GET_INTEGER (&tmp_len) - DB_GET_INTEGER (&tmp_arg3) + 1);

              if (db_string_substring (SUBSTRING, peek_right, &tmp_arg3, &tmp_len, &tmp_val) != NO_ERROR)
                {
                  goto error;
                }
              assert (DB_IS_NULL (&tmp_val) || DB_VALUE_DOMAIN_TYPE (&tmp_val) == DB_TYPE_VARCHAR);

              if (db_string_position (peek_left, &tmp_val, arithptr->value) != NO_ERROR)
                {
                  (void) pr_clear_value (&tmp_val);
                  goto error;
                }

              if (DB_GET_INTEGER (arithptr->value) > 0)
                {
                  db_make_int (arithptr->value, DB_GET_INTEGER (arithptr->value) + DB_GET_INTEGER (&tmp_arg3) - 1);
                }

              (void) pr_clear_value (&tmp_val);
            }
        }
      break;

    case T_SUBSTRING_INDEX:
      assert (peek_third != NULL);

      peek_third = db_value_cast_arg (peek_third, &tmp_third, DB_TYPE_INTEGER, &error_status);

      if (error_status != NO_ERROR)
        {
          assert (er_errid () != NO_ERROR);
          goto error;
        }

      if (db_string_substring_index (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_MID:
      if (DB_IS_NULL (peek_left) || DB_IS_NULL (peek_right) || DB_IS_NULL (peek_third))
        {
          PRIM_SET_NULL (arithptr->value);
        }
      else
        {
          DB_VALUE tmp_len, tmp_arg2, tmp_arg3;
          int pos, len;

          assert (peek_left != NULL);
          assert (peek_right != NULL);
          assert (peek_third != NULL);

          peek_left = db_value_cast_arg (peek_left, &tmp_right, DB_TYPE_VARCHAR, &error_status);
          peek_right = db_value_cast_arg (peek_right, &tmp_right, DB_TYPE_INTEGER, &error_status);
          peek_third = db_value_cast_arg (peek_third, &tmp_third, DB_TYPE_INTEGER, &error_status);

          if (error_status != NO_ERROR)
            {
              assert (er_errid () != NO_ERROR);
              goto error;
            }

          pos = DB_GET_INTEGER (peek_right);

          len = DB_GET_INTEGER (peek_third);

          if (pos < 0)
            {
              if (db_string_char_length (peek_left, &tmp_len) != NO_ERROR)
                {
                  goto error;
                }

              if (DB_IS_NULL (&tmp_len))
                {
                  goto error;
                }

              pos = pos + DB_GET_INTEGER (&tmp_len) + 1;
            }

          if (pos < 1)
            {
              db_make_int (&tmp_arg2, 1);
            }
          else
            {
              db_make_int (&tmp_arg2, pos);
            }

          if (len < 1)
            {
              db_make_int (&tmp_arg3, 0);
            }
          else
            {
              db_make_int (&tmp_arg3, len);
            }

          if (db_string_substring (SUBSTRING, peek_left, &tmp_arg2, &tmp_arg3, arithptr->value) != NO_ERROR)
            {
              goto error;
            }
        }
      break;

    case T_STRCMP:
      if (DB_IS_NULL (peek_left) || DB_IS_NULL (peek_right))
        {
          PRIM_SET_NULL (arithptr->value);
        }
      else
        {
          int cmp;

          if (db_string_compare (peek_left, peek_right, arithptr->value) != NO_ERROR)
            {
              goto error;
            }

          cmp = DB_GET_INTEGER (arithptr->value);
          if (cmp < 0)
            {
              cmp = -1;
            }
          else if (cmp > 0)
            {
              cmp = 1;
            }
          db_make_int (arithptr->value, cmp);
        }
      break;

    case T_REVERSE:
      if (db_string_reverse (peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_NULLIF:             /* when a = b then null else a end */
      {
        int cmp_res = DB_UNK;

        if (DB_IS_NULL (peek_left))
          {
            PRIM_SET_NULL (arithptr->value);
            break;
          }

        assert (DB_VALUE_DOMAIN_TYPE (peek_left) != DB_TYPE_NULL);

        arg1 = tp_domain_resolve_value (peek_left);
        arg2 = tp_domain_resolve_value (peek_right);

        target_domain = tp_infer_common_domain (arg1, arg2, true);

        cmp_res = tp_value_compare (peek_left, peek_right, 1, 0, NULL);

        if (cmp_res == DB_EQ)
          {
            PRIM_SET_NULL (arithptr->value);
          }
        else if (tp_value_coerce (peek_left, arithptr->value, target_domain) != DOMAIN_COMPATIBLE)
          {
            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                    pr_type_name (DB_VALUE_DOMAIN_TYPE (peek_left)), pr_type_name (TP_DOMAIN_TYPE (target_domain)));
            goto error;
          }
      }
      break;

    case T_EXTRACT:
      if (qdata_extract_dbval (arithptr->misc_operand, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_LEAST:
      {
        int cmp_result;
        bool can_compare = false;

        cmp_result = tp_value_compare (peek_left, peek_right, 1, 0, &can_compare);
        if (!can_compare)
          {
            goto error;
          }

        if (cmp_result == DB_EQ || cmp_result == DB_LT)
          {
            pr_clone_value (peek_left, arithptr->value);
          }
        else if (cmp_result == DB_GT)
          {
            pr_clone_value (peek_right, arithptr->value);
          }
        else
          {
            if (DB_IS_NULL (peek_right) || DB_IS_NULL (peek_left))
              {
                pr_clear_value (arithptr->value);
                PRIM_SET_NULL (arithptr->value);
              }
            else
              {
                assert_release (false);
              }
            break;
          }

        arg1 = tp_domain_resolve_value (peek_left);
        arg2 = tp_domain_resolve_value (peek_right);

        target_domain = tp_infer_common_domain (arg1, arg2, true);

        if (tp_value_coerce (arithptr->value, arithptr->value, target_domain) != DOMAIN_COMPATIBLE)
          {
            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                    pr_type_name (DB_VALUE_DOMAIN_TYPE (arithptr->value)),
                    pr_type_name (TP_DOMAIN_TYPE (target_domain)));
            goto error;
          }

        break;
      }

    case T_GREATEST:
      {
        int cmp_result;
        bool can_compare = false;

        cmp_result = tp_value_compare (peek_left, peek_right, 1, 0, &can_compare);
        if (!can_compare)
          {
            goto error;
          }

        if (cmp_result == DB_EQ || cmp_result == DB_GT)
          {
            pr_clone_value (peek_left, arithptr->value);
          }
        else if (cmp_result == DB_LT)
          {
            pr_clone_value (peek_right, arithptr->value);
          }
        else
          {
            if (DB_IS_NULL (peek_right) || DB_IS_NULL (peek_left))
              {
                pr_clear_value (arithptr->value);
                PRIM_SET_NULL (arithptr->value);
              }
            else
              {
                assert_release (false);
              }
            break;
          }

        arg1 = tp_domain_resolve_value (peek_left);
        arg2 = tp_domain_resolve_value (peek_right);

        target_domain = tp_infer_common_domain (arg1, arg2, true);

        if (tp_value_coerce (arithptr->value, arithptr->value, target_domain) != DOMAIN_COMPATIBLE)
          {
            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                    pr_type_name (DB_VALUE_DOMAIN_TYPE (arithptr->value)),
                    pr_type_name (TP_DOMAIN_TYPE (target_domain)));
            goto error;
          }

        break;
      }

    case T_STRCAT:
      if (qdata_strcat_dbval (peek_left, peek_right, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_PI:
      db_make_double (arithptr->value, PI);
      break;

    case T_RAND:
    case T_RANDOM:
      /*
         rand()     random()
         ==========================
         1888577227   1552124581
         1888577227     25078176
         1888577227   1382212507
         1888577227    809665978
         1888577227    338073425
       */
      /* random(), drandom() is not constant */
      REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_NOT_CONST);
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));
      if (DB_IS_NULL (peek_right))
        {
          /* When random functions are called without a seed, peek_right is null.
           * In this case, rand() or drand() uses a random value stored on value descriptor
           * to generate the same number during executing one SELECT statement.
           * But, each random() or drandom() picks up a seed value to generate different
           * numbers at every call.
           */
          if (arithptr->opcode == T_RAND)
            {
              db_make_int (arithptr->value, (int) vd->lrand);
            }
          else
            {
              long int r;
              struct drand48_data *rand_buf_p;

              rand_buf_p = qmgr_get_rand_buf (thread_p);
              lrand48_r (rand_buf_p, &r);
              db_make_int (arithptr->value, r);
            }
        }
      else
        {
          /* There are two types of seed:
           *  1) given by user (rightptr's type is TYPE_DBVAL)
           *   e.g, select rand(1) from table;
           *  2) fetched from tuple (rightptr's type is TYPE_CONSTANT)
           *   e.g, select rand(i) from table;
           *
           * Regarding the former case, rand(1) will generate a sequence of pseudo-random
           * values up to the number of rows. However, on the latter case, rand(i) generates
           * values depending on the column i's value. If, for example, there are three
           * tuples which include column i of 1 in a table, results of the above statements
           * are as follows.
           *
           *       rand(1)             rand(i)
           * =============       =============
           *      89400484            89400484
           *     976015093            89400484
           *    1792756325            89400484
           */

          peek_right = db_value_cast_arg (peek_right, &tmp_right, DB_TYPE_INTEGER, &error_status);

          if (error_status != NO_ERROR)
            {
              assert (er_errid () != NO_ERROR);
              goto error;
            }

          if (arithptr->rightptr->type == TYPE_CONSTANT || arithptr->rightptr->type == TYPE_ATTR_ID)
            {
              struct drand48_data buf;
              long int r;

              srand48_r ((long) DB_GET_INTEGER (peek_right), &buf);
              lrand48_r (&buf, &r);
              db_make_int (arithptr->value, r);
            }
          else
            {
              long int r;

              if (arithptr->rand_seed == NULL)
                {
                  arithptr->rand_seed = (struct drand48_data *) malloc (sizeof (struct drand48_data));

                  if (arithptr->rand_seed == NULL)
                    {
                      goto error;
                    }

                  srand48_r ((long) DB_GET_INTEGER (peek_right), arithptr->rand_seed);
                }

              lrand48_r (arithptr->rand_seed, &r);
              db_make_int (arithptr->value, r);
            }
        }
      break;

    case T_DRAND:
    case T_DRANDOM:
      /*
         drand()                 drandom()
         ====================================================
         3.169502079803728e-01     6.982659359365755e-01
         3.169502079803728e-01     1.548283380935338e-01
         3.169502079803728e-01     6.572457666271774e-01
         3.169502079803728e-01     3.592826622184653e-01
         3.169502079803728e-01     8.825680046663109e-01
       */
      /* random(), drandom() is not constant */
      REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_NOT_CONST);
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));
      if (DB_IS_NULL (peek_right))
        {
          if (arithptr->opcode == T_DRAND)
            {
              db_make_double (arithptr->value, (double) vd->drand);
            }
          else
            {
              double r;
              struct drand48_data *rand_buf_p;

              rand_buf_p = qmgr_get_rand_buf (thread_p);
              drand48_r (rand_buf_p, &r);
              db_make_double (arithptr->value, r);
            }
        }
      else
        {
          long int s_seed;

          if (!TP_IS_NUMERIC_TYPE (DB_VALUE_DOMAIN_TYPE (peek_right)))
            {
              peek_right = db_value_cast_arg (peek_right, &tmp_right, DB_TYPE_DOUBLE, &error_status);

              if (error_status != NO_ERROR)
                {
                  assert (er_errid () != NO_ERROR);
                  goto error;
                }
            }

          switch (db_value_type (peek_right))
            {
            case DB_TYPE_INTEGER:
              s_seed = (long) DB_GET_INT (peek_right);
              break;
            case DB_TYPE_BIGINT:
              s_seed = (long) DB_GET_BIGINT (peek_right);
              break;
            case DB_TYPE_DOUBLE:
              s_seed = (long) DB_GET_DOUBLE (peek_right);
              break;
            case DB_TYPE_NUMERIC:      /* TODO - */
              {
                DB_BIGINT bint;
                numeric_coerce_num_to_bigint (db_locate_numeric (peek_right), DB_VALUE_SCALE (peek_right), &bint);
                s_seed = (long) bint;   /* DB_GET_BIGINT */
              }
              break;
            default:
              assert (false);   /* is impossible */
              goto error;
              break;
            }

          if (arithptr->rightptr->type == TYPE_CONSTANT || arithptr->rightptr->type == TYPE_ATTR_ID)
            {
              struct drand48_data buf;
              double r;

              srand48_r (s_seed, &buf);
              drand48_r (&buf, &r);
              db_make_double (arithptr->value, r);
            }
          else
            {
              double r;

              if (arithptr->rand_seed == NULL)
                {
                  arithptr->rand_seed = (struct drand48_data *) malloc (sizeof (struct drand48_data));

                  if (arithptr->rand_seed == NULL)
                    {
                      goto error;
                    }

                  srand48_r (s_seed, arithptr->rand_seed);
                }

              drand48_r (arithptr->rand_seed, &r);
              db_make_double (arithptr->value, r);
            }
        }
      break;

    case T_LIST_DBS:
      if (qdata_list_dbs (thread_p, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TYPEOF:
      if (db_typeof_dbval (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_INDEX_CARDINALITY:
      assert (peek_left != NULL);
      assert (peek_third != NULL);

      peek_third = db_value_cast_arg (peek_third, &tmp_third, DB_TYPE_INTEGER, &error_status);

      if (error_status != NO_ERROR)
        {
          assert (er_errid () != NO_ERROR);
          goto error;
        }

      if (qdata_get_index_cardinality (thread_p, peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_INET_ATON:
      if (db_inet_aton (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_INET_NTOA:
      if (db_inet_ntoa (arithptr->value, peek_right) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_TRACE_STATS:
      /* session info is not constant */
      REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_NOT_CONST);
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));
      if (session_get_trace_stats (thread_p, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_WIDTH_BUCKET:
      if (db_width_bucket (arithptr->value, peek_left, peek_right, peek_third, peek_fourth) != NO_ERROR)
        {
          goto error;
        }
      break;

    case T_INDEX_PREFIX:
      if (db_string_index_prefix (peek_left, peek_right, peek_third, arithptr->value) != NO_ERROR)
        {
          goto error;
        }
      break;

    default:
      break;
    }

fetch_peek_arith_end:

  /* check for the first time */
  if (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST)
      && !REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST))
    {
      int not_const = 0;

      assert (arithptr->pred == NULL);

      if (arithptr->leftptr == NULL || REGU_VARIABLE_IS_FLAGED (arithptr->leftptr, REGU_VARIABLE_FETCH_ALL_CONST))
        {
          ;                     /* is_const, go ahead */
        }
      else
        {
          not_const++;
        }

      if (arithptr->rightptr == NULL || REGU_VARIABLE_IS_FLAGED (arithptr->rightptr, REGU_VARIABLE_FETCH_ALL_CONST))
        {
          ;                     /* is_const, go ahead */
        }
      else
        {
          not_const++;
        }

      if (arithptr->thirdptr == NULL || REGU_VARIABLE_IS_FLAGED (arithptr->thirdptr, REGU_VARIABLE_FETCH_ALL_CONST))
        {
          ;                     /* is_const, go ahead */
        }
      else
        {
          not_const++;
        }

      if (not_const == 0)
        {
          REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_ALL_CONST);
          assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST));
        }
      else
        {
          REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_NOT_CONST);
          assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));
        }
    }

  assert (REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST)
          || REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST));

#if !defined(NDEBUG)
  switch (arithptr->opcode)
    {
      /* set pred is not constant */
    case T_CASE:
    case T_DECODE:
    case T_IF:
    case T_PREDICATE:
      /* session info is not constant */
    case T_TRACE_STATS:
      /* random(), drandom() is not constant */
    case T_RAND:
    case T_RANDOM:
    case T_DRAND:
    case T_DRANDOM:

      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));
      assert (REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST));
      break;
    default:
      break;
    }

  /* set pred is not constant */
  if (arithptr->pred != NULL)
    {
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));
      assert (REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST));
    }
#endif

  *peek_dbval = arithptr->value;

  thread_dec_recursion_depth (thread_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (4, &tmp_left, &tmp_right, &tmp_third, &tmp_val);

  return NO_ERROR;

error:
  thread_dec_recursion_depth (thread_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (4, &tmp_left, &tmp_right, &tmp_third, &tmp_val);

  return ER_FAILED;
}

/*
 * fetch_peek_dbval () - returns a POINTER to an existing db_value
 *   return: NO_ERROR or ER_code
 *   regu_var(in/out): Regulator Variable
 *   vd(in): Value Descriptor
 *   obj_oid(in): Object Identifier
 *   tpl(in): Tuple
 *   peek_dbval(out): Set to the value ref resulting from the fetch operation
 *
 */
int
fetch_peek_dbval (THREAD_ENTRY * thread_p, REGU_VARIABLE * regu_var,
                  VAL_DESCR * vd, OID * obj_oid, QFILE_TUPLE tpl, DB_VALUE ** peek_dbval)
{
  int error = NO_ERROR;
  FUNCTION_TYPE *funcp = NULL;

  switch (regu_var->type)
    {
    case TYPE_ATTR_ID:         /* fetch object attribute value */
      /* is not constant */
      REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_NOT_CONST);
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));
      *peek_dbval = regu_var->value.attr_descr.cache_dbvalp;
      if (*peek_dbval != NULL)
        {
          /* we have a cached pointer already */
          break;
        }
      else
        {
          *peek_dbval = heap_attrinfo_access (regu_var->value.attr_descr.id, regu_var->value.attr_descr.cache_attrinfo);
          if (*peek_dbval == NULL)
            {
              goto exit_on_error;
            }
        }
      regu_var->value.attr_descr.cache_dbvalp = *peek_dbval;    /* cache */
      break;

    case TYPE_OID:             /* fetch object identifier value */
      REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_ALL_CONST);
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST));
      *peek_dbval = &regu_var->value.dbval;
      DB_MAKE_OID (*peek_dbval, obj_oid);
      break;

    case TYPE_POS_VALUE:       /* fetch positional value */
#if defined(QP_DEBUG)
      assert (vd->dbval_ptr != NULL);
      assert (vd->dbval_cnt > 0);

      if (regu_var->value.val_pos < 0 || regu_var->value.val_pos > (vd->dbval_cnt - 1))
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_VALLIST_INDEX, 1, regu_var->value.val_pos);
          goto exit_on_error;
        }
#endif

      REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_ALL_CONST);
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST));
      *peek_dbval = (DB_VALUE *) vd->dbval_ptr + regu_var->value.val_pos;
      break;

    case TYPE_CONSTANT:        /* fetch constant-column value */
      /* is not constant */
      REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_NOT_CONST);
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));

      /* execute linked query */
      EXECUTE_REGU_VARIABLE_XASL (thread_p, regu_var, vd);
      if (CHECK_REGU_VARIABLE_XASL_STATUS (regu_var) != XASL_SUCCESS)
        {
          goto exit_on_error;
        }
      *peek_dbval = regu_var->value.dbvalptr;
      break;

    case TYPE_ORDERBY_NUM:
      /* is not constant */
      REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_NOT_CONST);
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));
      *peek_dbval = regu_var->value.dbvalptr;
      break;

    case TYPE_DBVAL:           /* fetch db_value */
      REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_ALL_CONST);
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST));
      *peek_dbval = &regu_var->value.dbval;
      break;

    case TYPE_INARITH:         /* compute and fetch arithmetic expr. value */
    case TYPE_OUTARITH:
      error = fetch_peek_arith (thread_p, regu_var, vd, obj_oid, tpl, peek_dbval);
      if (error != NO_ERROR)
        {
          goto exit_on_error;
        }

      assert (REGU_VARIABLE_IS_FLAGED (regu_var,
                                       REGU_VARIABLE_FETCH_ALL_CONST)
              || REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST));
      break;

    case TYPE_FUNC:            /* fetch function value */
      /* is not constant */
      REGU_VARIABLE_SET_FLAG (regu_var, REGU_VARIABLE_FETCH_NOT_CONST);
      assert (!REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST));

      error = qdata_evaluate_function (thread_p, regu_var, vd, obj_oid, tpl);
      if (error != NO_ERROR)
        {
          goto exit_on_error;
        }

      funcp = regu_var->value.funcp;
      assert (funcp != NULL);
      *peek_dbval = funcp->value;
      break;

    case TYPE_POSITION:        /* fetch list file tuple value */
      assert (false);           /* should be avoided */
      /* fall through */

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      goto exit_on_error;
    }

  assert (REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST)
          || REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST));

  return NO_ERROR;

exit_on_error:

  return ER_FAILED;
}

/*
 * fetch_peek_dbval_pos () -
 *   return: NO_ERROR or ER_code
 *   regu_var(in/out): Regulator Variable
 *   tpl(in): Tuple
 *   pos(in):
 *   pos_domain(in):
 *   peek_dbval(out): Set to the value ref resulting from the fetch operation
 *   next_tpl(out): Set to the next tuple ref
 */
static int
fetch_peek_dbval_pos (REGU_VARIABLE * regu_var, QFILE_TUPLE tpl,
                      int pos, TP_DOMAIN * pos_domain, DB_VALUE ** peek_dbval, QFILE_TUPLE * next_tpl)
{
  int length;
  PR_TYPE *pr_type;
  OR_BUF buf;
  char *ptr;

  assert (tpl != NULL);
  assert (pos >= 0);
  assert (pos_domain != NULL);

  /* assume regu_var->type == TYPE_POSITION */
  pr_clear_value (regu_var->vfetch_to);
  *peek_dbval = regu_var->vfetch_to;

  /* locate value position in the tuple */
  if (qfile_locate_tuple_value_r (tpl, pos, &ptr, &length) == V_BOUND)
    {
      pr_type = pos_domain->type;
      if (pr_type == NULL || pr_type->id == DB_TYPE_VARIABLE)
        {
#if 1                           /* TODO - */
          assert (false);       /* should avoid */
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INCOMPATIBLE_TYPES, 0);
#endif
          return ER_QPROC_INCOMPATIBLE_TYPES;
        }

      OR_BUF_INIT (buf, ptr, length);
      /* read value from the tuple */
      if ((*(pr_type->data_readval)) (&buf, *peek_dbval, pos_domain, -1, false /* Don't copy */ ) != NO_ERROR)
        {
          return ER_FAILED;
        }
    }

  /* next position pointer */
  *next_tpl = ptr + length;

  return NO_ERROR;
}

/*
 * fetch_peek_min_max_value_of_width_bucket_func () -
 *   return: NO_ERROR or ER_code
 *   regu_var(in): Regulator Variable of an ARITH node.
 *   vd(in): Value Descriptor
 *   obj_oid(in): Object Identifier
 *   tpl(in): Tuple
 *   min(out): the lower bound of width_bucket
 *   max(out): the upper bound of width_bucket
 */
static int
fetch_peek_min_max_value_of_width_bucket_func (THREAD_ENTRY * thread_p,
                                               REGU_VARIABLE * regu_var,
                                               VAL_DESCR * vd,
                                               OID * obj_oid, QFILE_TUPLE tpl, DB_VALUE ** min, DB_VALUE ** max)
{
  int er_status = NO_ERROR;
  PRED_EXPR *pred_expr;
  PRED *pred;
  EVAL_TERM *eval_term1, *eval_term2;

  assert (min != NULL && max != NULL);

  if (regu_var == NULL || regu_var->type != TYPE_INARITH)
    {
      er_status = ER_QPROC_INVALID_XASLNODE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 0);

      goto error;
    }

  pred_expr = regu_var->value.arithptr->pred;
  if (pred_expr == NULL || pred_expr->type != T_PRED)
    {
      er_status = ER_QPROC_INVALID_XASLNODE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 0);

      goto error;
    }

  pred = &pred_expr->pe.pred;
  if (pred->lhs == NULL || pred->lhs->type != T_EVAL_TERM)
    {
      er_status = ER_QPROC_INVALID_XASLNODE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 0);

      goto error;
    }

  eval_term1 = &pred->lhs->pe.eval_term;
  if (eval_term1->et_type != T_COMP_EVAL_TERM)
    {
      er_status = ER_QPROC_INVALID_XASLNODE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 0);

      goto error;
    }

  /* lower bound, error info is already set in fetch_peek_dbval */
  er_status = fetch_peek_dbval (thread_p, eval_term1->et.et_comp.comp_rhs, vd, obj_oid, tpl, min);
  if (er_status != NO_ERROR)
    {
      if (er_errid () == NO_ERROR)
        {
          er_status = ER_QPROC_INVALID_XASLNODE;
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 0);
        }

      goto error;
    }

  eval_term2 = &pred->rhs->pe.eval_term;
  if (eval_term2->et_type != T_COMP_EVAL_TERM)
    {
      er_status = ER_QPROC_INVALID_XASLNODE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 0);

      goto error;
    }

  /* upper bound, error info is already set in fetch_peek_dbval  */
  er_status = fetch_peek_dbval (thread_p, eval_term2->et.et_comp.comp_rhs, vd, obj_oid, tpl, max);
  if (er_status != NO_ERROR)
    {
      if (er_errid () == NO_ERROR)
        {
          er_status = ER_QPROC_INVALID_XASLNODE;
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 0);
        }

      goto error;
    }

error:

  return er_status;
}

/*
 * fetch_copy_dbval () - returns a COPY of a db_value which the caller
 *                    must clear
 *   return: NO_ERROR or ER_code
 *   regu_var(in/out): Regulator Variable
 *   vd(in): Value Descriptor
 *   obj_oid(in): Object Identifier
 *   tpl(in): Tuple
 *   dbval(out): Set to the value resulting from the fetch operation
 *
 * This routine uses the value description indicated by the regulator variable
 * to fetch the indicated value and store it in the dbval parameter.
 * The value may be fetched from either a heap file object instance,
 * or from a list file tuple, or from an all constants regulator variable
 * content. If the value is fetched from a heap file object instance,
 * then tpl parameter should be given NULL. Likewise, if the value is fetched
 * from a list file tuple, then obj_oid parameter should be given NULL.
 * If the value is fetched from all constant values referred to
 * by the regulator variable, then all of the parameters obj_oid,
 * tpl should be given NULL values.
 *
 * If the value description in the regulator variable refers other cases
 * such as constant value, arithmetic expression, the resultant value
 * is computed and stored in the db_value.
 *
 * see fetch_peek_dbval().
 */
int
fetch_copy_dbval (THREAD_ENTRY * thread_p, REGU_VARIABLE * regu_var,
                  VAL_DESCR * vd, OID * obj_oid, QFILE_TUPLE tpl, DB_VALUE * dbval)
{
  int result;
  DB_VALUE *readonly_val, copy_val, *tmp;

  db_make_null (&copy_val);

  result = fetch_peek_dbval (thread_p, regu_var, vd, obj_oid, tpl, &readonly_val);
  if (result != NO_ERROR)
    {
      return result;
    }

  /*
   * This routine needs to ensure that a copy happens.  If readonly_val
   * points to the same db_value as dbval, qdata_copy_db_value() won't copy.
   * This can happen with scans that are PEEKING and routines that
   * are sending the COPY flag to fetch_val_list() like the group by
   * code.  If this happens we use a stack variable for the copy and
   * then transfer ownership to the returned dbval
   */
  if (dbval == readonly_val)
    {
      tmp = &copy_val;
    }
  else
    {
      tmp = dbval;
    }

  if (!qdata_copy_db_value (tmp, readonly_val))
    {
      result = ER_FAILED;
    }

  if (tmp == &copy_val)
    {
      /*
       * transfer ownership to the real db_value via a
       * structure copy.  Make sure you clear the previous value.
       */
      pr_clear_value (dbval);
      *dbval = *tmp;
    }

  return result;
}

/*
 * fetch_val_list () - fetches all the values for the given regu variable list
 *   return: NO_ERROR or ER_code
 *   regu_list(in/out): Regulator Variable list
 *   vd(in): Value Descriptor
 *   obj_oid(in): Object Identifier
 *   tpl(in): Tuple
 *   tpl_type_list(in): data type of each column of Tuple
 *   peek(int):
 */
int
fetch_val_list (THREAD_ENTRY * thread_p, REGU_VARIABLE_LIST regu_list,
                VAL_DESCR * vd, OID * obj_oid, QFILE_TUPLE tpl, QFILE_TUPLE_VALUE_TYPE_LIST * tpl_type_list, int peek)
{
  REGU_VARIABLE_LIST regup;
  QFILE_TUPLE next_tpl;
  int rc, pos, next_pos;
  TP_DOMAIN *pos_domain = NULL;
  DB_VALUE *peek_val;

  if (peek)
    {
      next_tpl = tpl + QFILE_TUPLE_LENGTH_SIZE;
      next_pos = 0;

      for (regup = regu_list; regup != NULL; regup = regup->next)
        {
          if (regup->value.type == TYPE_POSITION)
            {
              assert (tpl != NULL);
              assert (tpl_type_list != NULL);

              pos = regup->value.value.pos_descr.pos_no;
              assert (pos < tpl_type_list->type_cnt);
              assert (tpl_type_list->domp[pos] != NULL);
              pos_domain = tpl_type_list->domp[pos];

              if (pos >= next_pos)
                {
                  pos -= next_pos;
                  next_pos = regup->value.value.pos_descr.pos_no + 1;
                }
              else
                {
                  next_tpl = tpl + QFILE_TUPLE_LENGTH_SIZE;
                  next_pos = 0;
                }

              /* at fetch_peek_dbval_pos(), regup->value.vfetch_to is cleared */
              rc = fetch_peek_dbval_pos (&regup->value, next_tpl, pos, pos_domain, &peek_val, &next_tpl);
            }
          else
            {
              if (pr_is_set_type (DB_VALUE_DOMAIN_TYPE (regup->value.vfetch_to)))
                {
                  pr_clear_value (regup->value.vfetch_to);
                }
              rc = fetch_peek_dbval (thread_p, &regup->value, vd, obj_oid, tpl, &peek_val);
            }

          if (rc != NO_ERROR)
            {
              pr_clear_value (regup->value.vfetch_to);
              return ER_FAILED;
            }
          PR_SHARE_VALUE (peek_val, regup->value.vfetch_to);
        }
    }
  else
    {
      DB_VALUE copy_val;

      next_tpl = tpl + QFILE_TUPLE_LENGTH_SIZE;
      next_pos = 0;

      /*
       * These DB_VALUES must persist across object fetches, so we must
       * use fetch_copy_dbval and NOT peek here.
       */
      for (regup = regu_list; regup != NULL; regup = regup->next)
        {
          if (regup->value.type == TYPE_POSITION)
            {
              assert (tpl != NULL);
              assert (tpl_type_list != NULL);

              pos = regup->value.value.pos_descr.pos_no;
              assert (pos < tpl_type_list->type_cnt);
              assert (tpl_type_list->domp[pos] != NULL);
              pos_domain = tpl_type_list->domp[pos];

              if (pos >= next_pos)
                {
                  pos -= next_pos;
                  next_pos = regup->value.value.pos_descr.pos_no + 1;
                }
              else
                {
                  next_tpl = tpl + QFILE_TUPLE_LENGTH_SIZE;
                  next_pos = 0;
                }

              /* at fetch_peek_dbval_pos(), regup->value.vfetch_to is cleared */
              rc = fetch_peek_dbval_pos (&regup->value, next_tpl, pos, pos_domain, &peek_val, &next_tpl);

              if (rc != NO_ERROR)
                {
                  pr_clear_value (regup->value.vfetch_to);
                  return ER_FAILED;
                }

              assert (regup->value.vfetch_to == peek_val);

              db_make_null (&copy_val);

              if (!qdata_copy_db_value (&copy_val, regup->value.vfetch_to))
                {
                  pr_clear_value (regup->value.vfetch_to);
                  return ER_FAILED;
                }

              /*
               * transfer ownership to the real db_value via a
               * structure copy.  Make sure you clear the previous value.
               */
              pr_clear_value (regup->value.vfetch_to);
              *(regup->value.vfetch_to) = copy_val;
            }
          else
            {
              assert (false);   /* is impossible */

              if (pr_is_set_type (DB_VALUE_DOMAIN_TYPE (regup->value.vfetch_to)))
                {
                  pr_clear_value (regup->value.vfetch_to);
                }
              if (fetch_copy_dbval (thread_p, &regup->value, vd, obj_oid, tpl, regup->value.vfetch_to) != NO_ERROR)
                {
                  return ER_FAILED;
                }
            }
        }
    }

  return NO_ERROR;
}

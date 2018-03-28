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
 * query_evaluator.c - Predicate evaluator
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>

#include "system_parameter.h"
#include "error_manager.h"
#include "memory_alloc.h"
#include "object_representation.h"
#include "heap_file.h"
#include "slotted_page.h"
#include "fetch.h"
#include "list_file.h"

#include "object_primitive.h"
#include "set_object.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define UNKNOWN_CARD   -2       /* Unknown cardinality of a set member */

static DB_LOGICAL eval_negative (DB_LOGICAL res);
#if defined (ENABLE_UNUSED_FUNCTION)
static DB_LOGICAL eval_logical_result (DB_LOGICAL res1, DB_LOGICAL res2);
#endif
static DB_LOGICAL eval_value_rel_cmp (DB_VALUE * dbval1, DB_VALUE * dbval2, REL_OP rel_operator);
static DB_LOGICAL eval_some_eval (DB_VALUE * item, DB_SET * set, REL_OP rel_operator);
#if defined (ENABLE_UNUSED_FUNCTION)
static int eval_item_card_set (DB_VALUE * item, DB_SET * set, REL_OP rel_operator);
#endif
static DB_LOGICAL eval_some_list_eval (THREAD_ENTRY * thread_p,
                                       DB_VALUE * item, QFILE_LIST_ID * list_id, REL_OP rel_operator);
#if defined (ENABLE_UNUSED_FUNCTION)
static int eval_item_card_sort_list (THREAD_ENTRY * thread_p, DB_VALUE * item, QFILE_LIST_ID * list_id);
static DB_LOGICAL eval_sub_multi_set_to_sort_list (THREAD_ENTRY * thread_p, DB_SET * set1, QFILE_LIST_ID * list_id);
static DB_LOGICAL eval_sub_sort_list_to_multi_set (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id, DB_SET * set);
static DB_LOGICAL eval_sub_sort_list_to_sort_list (THREAD_ENTRY * thread_p,
                                                   QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2);
static DB_LOGICAL eval_eq_multi_set_to_sort_list (THREAD_ENTRY * thread_p, DB_SET * set, QFILE_LIST_ID * list_id);
static DB_LOGICAL eval_ne_multi_set_to_sort_list (THREAD_ENTRY * thread_p, DB_SET * set, QFILE_LIST_ID * list_id);
static DB_LOGICAL eval_le_multi_set_to_sort_list (THREAD_ENTRY * thread_p, DB_SET * set, QFILE_LIST_ID * list_id);
static DB_LOGICAL eval_lt_multi_set_to_sort_list (THREAD_ENTRY * thread_p, DB_SET * set, QFILE_LIST_ID * list_id);
static DB_LOGICAL eval_le_sort_list_to_multi_set (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id, DB_SET * set);
static DB_LOGICAL eval_lt_sort_list_to_multi_set (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id, DB_SET * set);
static DB_LOGICAL eval_eq_sort_list_to_sort_list (THREAD_ENTRY * thread_p,
                                                  QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2);
static DB_LOGICAL eval_ne_sort_list_to_sort_list (THREAD_ENTRY * thread_p,
                                                  QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2);
static DB_LOGICAL eval_le_sort_list_to_sort_list (THREAD_ENTRY * thread_p,
                                                  QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2);
static DB_LOGICAL eval_lt_sort_list_to_sort_list (THREAD_ENTRY * thread_p,
                                                  QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2);
static DB_LOGICAL eval_multi_set_to_sort_list (THREAD_ENTRY * thread_p,
                                               DB_SET * set, QFILE_LIST_ID * list_id, REL_OP rel_operator);
static DB_LOGICAL eval_sort_list_to_multi_set (THREAD_ENTRY * thread_p,
                                               QFILE_LIST_ID * list_id, DB_SET * set, REL_OP rel_operator);
static DB_LOGICAL eval_sort_list_to_sort_list (THREAD_ENTRY * thread_p,
                                               QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2, REL_OP rel_operator);
static DB_LOGICAL eval_set_list_cmp (THREAD_ENTRY * thread_p,
                                     COMP_EVAL_TERM * et_comp, VAL_DESCR * vd, DB_VALUE * dbval1, DB_VALUE * dbval2);
#endif

/*
 * eval_negative () - negate the result
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   res(in): result
 */
static DB_LOGICAL
eval_negative (DB_LOGICAL res)
{
  /* negate the result */
  if (res == V_TRUE)
    {
      return V_FALSE;
    }
  else if (res == V_FALSE)
    {
      return V_TRUE;
    }

  /* V_ERROR, V_UNKNOWN */
  return res;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * eval_logical_result () - evaluate the given two results
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   res1(in): first result
 *   res2(in): second result
 */
static DB_LOGICAL
eval_logical_result (DB_LOGICAL res1, DB_LOGICAL res2)
{
  if (res1 == V_ERROR || res2 == V_ERROR)
    {
      return V_ERROR;
    }

  if (res1 == V_TRUE && res2 == V_TRUE)
    {
      return V_TRUE;
    }
  else if (res1 == V_FALSE || res2 == V_FALSE)
    {
      return V_FALSE;
    }

  return V_UNKNOWN;
}
#endif

/*
 * Predicate Evaluation
 */

/*
 * eval_value_rel_cmp () - Compare two db_values according to the given
 *                       relational operator
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   dbval1(in): first db_value
 *   dbval2(in): second db_value
 *   rel_operator(in): Relational operator
 */
static DB_LOGICAL
eval_value_rel_cmp (DB_VALUE * dbval1, DB_VALUE * dbval2, REL_OP rel_operator)
{
  int result;
  bool can_compare = false;

  /*
   * we get here for either an ordinal comparison.
   */

  if (rel_operator == R_EQ_TORDER)
    {
      /* do total order comparison */
      result = tp_value_compare (dbval1, dbval2, 1, 1, &can_compare);
    }
  else
    {
      /* do ordinal comparison, but NULL's still yield UNKNOWN */
      result = tp_value_compare (dbval1, dbval2, 1, 0, &can_compare);
    }

  if (!can_compare)
    {
      return V_ERROR;
    }

  if (result == DB_UNK && rel_operator != R_NULLSAFE_EQ)
    {
      return V_UNKNOWN;
    }

  switch (rel_operator)
    {
    case R_EQ:
      return ((result == DB_EQ) ? V_TRUE : V_FALSE);
    case R_EQ_TORDER:
      return ((result == DB_EQ) ? V_TRUE : V_FALSE);
    case R_LT:
      return ((result == DB_LT) ? V_TRUE : V_FALSE);
    case R_LE:
      return (((result == DB_LT) || (result == DB_EQ)) ? V_TRUE : V_FALSE);
    case R_GT:
      return ((result == DB_GT) ? V_TRUE : V_FALSE);
    case R_GE:
      return (((result == DB_GT) || (result == DB_EQ)) ? V_TRUE : V_FALSE);
    case R_NE:
      return ((result != DB_EQ) ? V_TRUE : V_FALSE);
    case R_NULLSAFE_EQ:
      if (result == DB_EQ)
        {
          return V_TRUE;
        }
      else
        {
          if (DB_IS_NULL (dbval1))
            {
              if (DB_IS_NULL (dbval2))
                {
                  return V_TRUE;
                }
              else
                {
                  return V_FALSE;
                }
            }
          else
            {
              return V_FALSE;
            }
        }
      break;
    default:
      return V_ERROR;
    }
}

/*
 * eval_some_eval () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN, V_ERROR)
 *   item(in): db_value item
 *   set(in): collection of elements
 *   rel_operator(in): relational comparison operator
 */

static DB_LOGICAL
eval_some_eval (DB_VALUE * item, DB_SET * set, REL_OP rel_operator)
{
  int i;
  DB_LOGICAL res, t_res;
  DB_VALUE elem_val;

  PRIM_SET_NULL (&elem_val);

  res = V_FALSE;

  for (i = 0; i < set_size (set); i++)
    {
      if (set_get_element (set, i, &elem_val) != NO_ERROR)
        {
          return V_ERROR;
        }

      t_res = eval_value_rel_cmp (item, &elem_val, rel_operator);
      pr_clear_value (&elem_val);
      if (t_res == V_TRUE)
        {
          return V_TRUE;
        }
      else if (t_res == V_ERROR)
        {
          return V_ERROR;
        }
      else if (t_res == V_UNKNOWN)
        {
          res = V_UNKNOWN;      /* never returns here. we should proceed */
        }
    }

  return res;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * eval_item_card_set () -
 *   return: int (cardinality)
 *           >= 0 : normal cardinality
 *           ER_FAILED : ERROR
 *           UNKNOWN_CARD : unknown cardinality value
 *   item(in): db_value item
 *   set(in): collection of elements
 *   rel_operator(in): relational comparison operator
 *
 * Note: This routine returns the number of set elements (cardinality)
 *              which are determined to hold the given relationship with the
 *              specified item value. If the relationship is the equality
 *              relationship, the returned value means the cardinality of the
 *              given element in the set and must always be less than equal
 *              to 1 for the case of basic sets.
 */
static int
eval_item_card_set (DB_VALUE * item, DB_SET * set, REL_OP rel_operator)
{
  int num, i;
  DB_LOGICAL res;
  DB_VALUE elem_val;

  PRIM_SET_NULL (&elem_val);

  num = 0;

  for (i = 0; i < set_size (set); i++)
    {
      if (set_get_element (set, i, &elem_val) != NO_ERROR)
        {
          return ER_FAILED;
        }
      if (db_value_is_null (&elem_val))
        {
          pr_clear_value (&elem_val);
          return UNKNOWN_CARD;
        }

      res = eval_value_rel_cmp (item, &elem_val, rel_operator);
      pr_clear_value (&elem_val);

      if (res == V_ERROR)
        {
          return ER_FAILED;
        }

      if (res == V_TRUE)
        {
          num++;
        }
    }

  return num;
}
#endif

/*
 * List File Related Evaluation
 */

/*
 * eval_some_list_eval () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN, V_ERROR)
 *   item(in): db_value item
 *   list_id(in): list file identifier
 *   rel_operator(in): relational comparison operator
 *
 * Note: This routine tries to determine whether a specific relation
 *              as determined by the relational operator rel_operator holds between
 *              the given bound item value and at least one member of the
 *              given list of elements. It returns V_TRUE, V_FALSE,
 *              V_UNKNOWN, V_ERROR using the following reasoning:
 *
 *              V_TRUE:     - there exists a value in the list that is
 *                            determined to hold the relationship.
 *              V_FALSE:    - all the values in the list are determined not
 *                            to hold the relationship, or
 *                          - the list is empty
 *              V_UNKNOWN:  - list has no value determined to hold the rel.
 *                            and at least one value which can not be
 *                            determined to fail to hold the relationship.
 *              V_ERROR:    - an error occurred.
 *
 *  Note: The IN relationship can be stated as item has the equality rel. with
 *        one of the list elements.
 */
static DB_LOGICAL
eval_some_list_eval (THREAD_ENTRY * thread_p, DB_VALUE * item, QFILE_LIST_ID * list_id, REL_OP rel_operator)
{
  DB_LOGICAL res, t_res;
  QFILE_LIST_SCAN_ID s_id;
  QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
  DB_VALUE list_val;
  SCAN_CODE qp_scan;
  PR_TYPE *pr_type;
  OR_BUF buf;
  int length;
  char *ptr;

  /* assert */
  if (list_id->type_list.domp == NULL)
    {
      return V_ERROR;
    }

  PRIM_SET_NULL (&list_val);

  if (list_id->tuple_cnt == 0)
    {
      return V_FALSE;           /* empty set */
    }

  pr_type = list_id->type_list.domp[0]->type;
  if (pr_type == NULL)
    {
      return V_ERROR;
    }

  if (qfile_open_list_scan (list_id, &s_id) != NO_ERROR)
    {
      return V_ERROR;
    }

  res = V_FALSE;
  while ((qp_scan = qfile_scan_list_next (thread_p, &s_id, &tplrec, PEEK)) == S_SUCCESS)
    {
      if (qfile_locate_tuple_value (tplrec.tpl, 0, &ptr, &length) == V_UNBOUND)
        {
          res = V_UNKNOWN;
        }
      else
        {
          OR_BUF_INIT (buf, ptr, length);

          if ((*(pr_type->data_readval)) (&buf, &list_val, list_id->type_list.domp[0], -1, true) != NO_ERROR)
            {
              return V_ERROR;
            }

          t_res = eval_value_rel_cmp (item, &list_val, rel_operator);
          if (t_res == V_TRUE || t_res == V_ERROR)
            {
              pr_clear_value (&list_val);
              qfile_close_scan (thread_p, &s_id);
              return t_res;
            }
          else if (t_res == V_UNKNOWN)
            {
              res = V_UNKNOWN;
            }
          pr_clear_value (&list_val);
        }
    }

  qfile_close_scan (thread_p, &s_id);

  return (qp_scan == S_END) ? res : V_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * eval_item_card_sort_list () -
 *   return: int (cardinality, UNKNOWN_CARD, ER_FAILED for error cases)
 *   item(in): db_value item
 *   list_id(in): list file identifier
 *
 * Note: This routine returns the number of set elements (cardinality)
 *              which are determined to hold the equality relationship with
 *              specified item value. The list file values must have already
 *              been sorted.
 */
static int
eval_item_card_sort_list (THREAD_ENTRY * thread_p, DB_VALUE * item, QFILE_LIST_ID * list_id)
{
  QFILE_LIST_SCAN_ID s_id;
  QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
  DB_VALUE list_val;
  SCAN_CODE qp_scan;
  PR_TYPE *pr_type;
  OR_BUF buf;
  DB_LOGICAL rc;
  int length;
  int card;
  char *ptr;

  /* assert */
  if (list_id->type_list.domp == NULL)
    {
      return ER_FAILED;
    }

  PRIM_SET_NULL (&list_val);
  card = 0;

  if (qfile_open_list_scan (list_id, &s_id) != NO_ERROR)
    {
      return ER_FAILED;
    }

  pr_type = list_id->type_list.domp[0]->type;
  if (pr_type == NULL)
    {
      qfile_close_scan (thread_p, &s_id);
      return ER_FAILED;
    }

  while ((qp_scan = qfile_scan_list_next (thread_p, &s_id, &tplrec, PEEK)) == S_SUCCESS)
    {
      if (qfile_locate_tuple_value (tplrec.tpl, 0, &ptr, &length) == V_UNBOUND)
        {
          qfile_close_scan (thread_p, &s_id);
          return UNKNOWN_CARD;
        }

      OR_BUF_INIT (buf, ptr, length);

      (*(pr_type->data_readval)) (&buf, &list_val, list_id->type_list.domp[0], -1, true);

      rc = eval_value_rel_cmp (item, &list_val, R_LT);
      if (rc == V_ERROR)
        {
          pr_clear_value (&list_val);
          return ER_FAILED;
        }
      else if (rc == V_TRUE)
        {
          pr_clear_value (&list_val);
          continue;
        }

      rc = eval_value_rel_cmp (item, &list_val, R_EQ);
      pr_clear_value (&list_val);

      if (rc == V_ERROR)
        {
          return ER_FAILED;
        }
      else if (rc == V_TRUE)
        {
          card++;
        }
      else
        {
          break;
        }
    }

  qfile_close_scan (thread_p, &s_id);

  return (qp_scan == S_END) ? card : ER_FAILED;
}

/*
 * eval_sub_multi_set_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   set1(in): DB_SET representation
 * 	 list_id(in): Sorted LIST FILE identifier
 *
 * Note: Find if given multi_set is a subset of the given list file.
 *              The list file must be of one column and treated like a
 *              multi_set. The routine uses the same semantics of finding
 *              subset relationship between two multi_sets.
 *
 * Note: in a sorted list file of one column , ALL the NULL values, tuples
 *       appear at the beginning of the list file.
 */
static DB_LOGICAL
eval_sub_multi_set_to_sort_list (THREAD_ENTRY * thread_p, DB_SET * set1, QFILE_LIST_ID * list_id)
{
  int i, k, card, card1, card2;
  DB_LOGICAL res;
  DB_LOGICAL rc;
  DB_VALUE elem_val, elem_val2;
  int found;

  PRIM_SET_NULL (&elem_val);
  PRIM_SET_NULL (&elem_val2);

  card = set_size (set1);
  if (card == 0)
    {
      return V_TRUE;            /* empty set */
    }

  res = V_TRUE;
  for (i = 0; i < card; i++)
    {
      if (set_get_element (set1, i, &elem_val) != NO_ERROR)
        {
          return V_ERROR;
        }
      if (db_value_is_null (&elem_val))
        {
          return V_UNKNOWN;
        }

      /* search for the value to see if value has already been considered */
      found = false;
      for (k = 0; !found && k < i; k++)
        {
          if (set_get_element (set1, k, &elem_val2) != NO_ERROR)
            {
              pr_clear_value (&elem_val);
              return V_ERROR;
            }
          if (db_value_is_null (&elem_val2))
            {
              pr_clear_value (&elem_val2);
              continue;
            }

          rc = eval_value_rel_cmp (&elem_val, &elem_val2, R_EQ);
          if (rc == V_ERROR)
            {
              pr_clear_value (&elem_val);
              pr_clear_value (&elem_val2);
              return V_ERROR;
            }
          else if (rc == V_TRUE)
            {
              found = true;
            }
          pr_clear_value (&elem_val2);
        }

      if (found)
        {
          pr_clear_value (&elem_val);
          continue;
        }

      card1 = eval_item_card_set (&elem_val, set1, R_EQ);
      if (card1 == ER_FAILED)
        {
          pr_clear_value (&elem_val);
          return V_ERROR;
        }
      else if (card1 == UNKNOWN_CARD)
        {
          pr_clear_value (&elem_val);
          return V_UNKNOWN;
        }

      card2 = eval_item_card_sort_list (thread_p, &elem_val, list_id);
      if (card2 == ER_FAILED)
        {
          pr_clear_value (&elem_val);
          return V_ERROR;
        }
      else if (card2 == UNKNOWN_CARD)
        {
          pr_clear_value (&elem_val);
          return V_UNKNOWN;
        }

      if (card1 > card2)
        {
          pr_clear_value (&elem_val);
          return V_FALSE;
        }
    }

  pr_clear_value (&elem_val);
  return res;
}

/*
 * eval_sub_sort_list_to_multi_set () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 * 	 list_id(in): Sorted LIST FILE identifier
 * 	 set(in): DB_SETrepresentation
 *
 * Note: Find if the given list file is a subset of the given multi_set
 *              The list file must be of one column and treated like a
 *              multi_set. The routine uses the same semantics of finding
 *              subset relationship between two multi_sets.
 *
 * Note: in a sorted list file of one column , ALL the NULL values, tuples
 *       appear at the beginning of the list file.
 */
static DB_LOGICAL
eval_sub_sort_list_to_multi_set (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id, DB_SET * set)
{
  int card1, card2;
  DB_LOGICAL res, rc;
  DB_VALUE list_val, list_val2;
  QFILE_LIST_SCAN_ID s_id;
  QFILE_TUPLE_RECORD tplrec, p_tplrec;
  char *p_tplp;
  SCAN_CODE qp_scan;
  PR_TYPE *pr_type;
  OR_BUF buf;
  int length;
  int list_on;
  int tpl_len;
  char *ptr;

  /* assert */
  if (list_id->type_list.domp == NULL)
    {
      return V_ERROR;
    }

  PRIM_SET_NULL (&list_val);
  PRIM_SET_NULL (&list_val2);

  if (list_id->tuple_cnt == 0)
    {
      return V_TRUE;            /* empty set */
    }

  if (qfile_open_list_scan (list_id, &s_id) != NO_ERROR)
    {
      return V_ERROR;
    }

  res = V_TRUE;
  pr_type = list_id->type_list.domp[0]->type;

  tplrec.size = 0;
  tplrec.tpl = NULL;
  p_tplrec.size = DB_PAGESIZE;
  p_tplrec.tpl = (QFILE_TUPLE) malloc (DB_PAGESIZE);
  if (p_tplrec.tpl == NULL)
    {
      return V_ERROR;
    }

  list_on = false;
  card1 = 0;
  while ((qp_scan = qfile_scan_list_next (thread_p, &s_id, &tplrec, PEEK)) == S_SUCCESS)
    {
      if (qfile_locate_tuple_value (tplrec.tpl, 0, &ptr, &length) == V_UNBOUND)
        {
          qfile_close_scan (thread_p, &s_id);
          free_and_init (p_tplrec.tpl);
          return V_UNKNOWN;
        }

      pr_clear_value (&list_val);
      OR_BUF_INIT (buf, ptr, length);

      (*(pr_type->data_readval)) (&buf, &list_val, list_id->type_list.domp[0], -1, true);

      if (list_on == true)
        {
          p_tplp = (char *) p_tplrec.tpl + QFILE_TUPLE_LENGTH_SIZE;

          or_init (&buf, p_tplp + QFILE_TUPLE_VALUE_HEADER_SIZE, QFILE_GET_TUPLE_VALUE_LENGTH (p_tplp));

          (*(pr_type->data_readval)) (&buf, &list_val2, list_id->type_list.domp[0], -1, true);

          rc = eval_value_rel_cmp (&list_val, &list_val2, R_EQ);
          if (rc == V_ERROR)
            {
              pr_clear_value (&list_val);
              pr_clear_value (&list_val2);
              qfile_close_scan (thread_p, &s_id);
              free_and_init (p_tplrec.tpl);
              return V_ERROR;
            }
          else if (rc != V_TRUE)
            {
              card2 = eval_item_card_set (&list_val2, set, R_EQ);
              if (card2 == ER_FAILED)
                {
                  pr_clear_value (&list_val);
                  pr_clear_value (&list_val2);
                  qfile_close_scan (thread_p, &s_id);
                  free_and_init (p_tplrec.tpl);
                  return V_ERROR;
                }
              else if (card2 == UNKNOWN_CARD)
                {
                  pr_clear_value (&list_val);
                  pr_clear_value (&list_val2);
                  qfile_close_scan (thread_p, &s_id);
                  free_and_init (p_tplrec.tpl);
                  return V_UNKNOWN;
                }

              if (card1 > card2)
                {
                  pr_clear_value (&list_val);
                  pr_clear_value (&list_val2);
                  qfile_close_scan (thread_p, &s_id);
                  free_and_init (p_tplrec.tpl);
                  return V_FALSE;
                }
              card1 = 0;
            }
          pr_clear_value (&list_val2);
        }

      tpl_len = QFILE_GET_TUPLE_LENGTH (tplrec.tpl);
      if (p_tplrec.size < tpl_len)
        {
          p_tplrec.size = tpl_len;
          p_tplrec.tpl = (QFILE_TUPLE) realloc (p_tplrec.tpl, tpl_len);
          if (p_tplrec.tpl == NULL)
            {
              return (DB_LOGICAL) ER_OUT_OF_VIRTUAL_MEMORY;
            }
        }
      memcpy (p_tplrec.tpl, tplrec.tpl, tpl_len);
      list_on = true;
      card1++;
    }

  if (qp_scan != S_END)
    {
      pr_clear_value (&list_val);
      qfile_close_scan (thread_p, &s_id);
      free_and_init (p_tplrec.tpl);
      return V_ERROR;
    }

  if (list_on == true)
    {
      p_tplp = (char *) p_tplrec.tpl + QFILE_TUPLE_LENGTH_SIZE; /* no unbound value */

      or_init (&buf, p_tplp + QFILE_TUPLE_VALUE_HEADER_SIZE, QFILE_GET_TUPLE_VALUE_LENGTH (p_tplp));

      (*(pr_type->data_readval)) (&buf, &list_val2, list_id->type_list.domp[0], -1, true);

      card2 = eval_item_card_set (&list_val2, set, R_EQ);
      if (card2 == ER_FAILED)
        {
          pr_clear_value (&list_val);
          pr_clear_value (&list_val2);
          qfile_close_scan (thread_p, &s_id);
          free_and_init (p_tplrec.tpl);
          return V_ERROR;
        }
      else if (card2 == UNKNOWN_CARD)
        {
          res = V_UNKNOWN;
        }
      else if (card1 > card2)
        {
          pr_clear_value (&list_val);
          pr_clear_value (&list_val2);
          qfile_close_scan (thread_p, &s_id);
          free_and_init (p_tplrec.tpl);
          return V_FALSE;
        }
    }

  pr_clear_value (&list_val);
  pr_clear_value (&list_val2);
  qfile_close_scan (thread_p, &s_id);
  free_and_init (p_tplrec.tpl);
  return res;
}

/*
 * eval_sub_sort_list_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 * 	 list_id1(in): First Sorted LIST FILE identifier
 * 	 list_id2(in): Second Sorted LIST FILE identifier
 *
 * Note: Find if the first list file is a subset of the second list
 *              file. The list files must be of one column and treated like
 *              a multi_set. The routine uses the same semantics of finding
 *              subset relationship between two multi_sets.
 *
 * Note: in a sorted list file of one column , ALL the NULL values, tuples
 *       appear at the beginning of the list file.
 */
static DB_LOGICAL
eval_sub_sort_list_to_sort_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2)
{
  int card1, card2;
  DB_LOGICAL res, rc;
  DB_VALUE list_val, list_val2;
  QFILE_LIST_SCAN_ID s_id;
  QFILE_TUPLE_RECORD tplrec, p_tplrec;
  char *p_tplp;
  SCAN_CODE qp_scan;
  PR_TYPE *pr_type;
  OR_BUF buf;
  int length;
  int list_on;
  int tpl_len;
  char *ptr;

  /* assert */
  if (list_id1->type_list.domp == NULL)
    {
      return V_ERROR;
    }

  PRIM_SET_NULL (&list_val);
  PRIM_SET_NULL (&list_val2);

  if (list_id1->tuple_cnt == 0)
    {
      return V_TRUE;            /* empty set */
    }

  if (qfile_open_list_scan (list_id1, &s_id) != NO_ERROR)
    {
      return V_ERROR;
    }

  res = V_TRUE;
  pr_type = list_id1->type_list.domp[0]->type;

  tplrec.size = 0;
  tplrec.tpl = NULL;
  p_tplrec.size = DB_PAGESIZE;
  p_tplrec.tpl = (QFILE_TUPLE) malloc (DB_PAGESIZE);
  if (p_tplrec.tpl == NULL)
    {
      return V_ERROR;
    }

  list_on = false;
  card1 = 0;
  while ((qp_scan = qfile_scan_list_next (thread_p, &s_id, &tplrec, PEEK)) == S_SUCCESS)
    {
      if (qfile_locate_tuple_value (tplrec.tpl, 0, &ptr, &length) == V_UNBOUND)
        {
          qfile_close_scan (thread_p, &s_id);
          free_and_init (p_tplrec.tpl);
          return V_UNKNOWN;
        }

      OR_BUF_INIT (buf, ptr, length);

      (*(pr_type->data_readval)) (&buf, &list_val, list_id1->type_list.domp[0], -1, true);

      if (list_on == true)
        {
          p_tplp = (char *) p_tplrec.tpl + QFILE_TUPLE_LENGTH_SIZE;

          or_init (&buf, p_tplp + QFILE_TUPLE_VALUE_HEADER_SIZE, QFILE_GET_TUPLE_VALUE_LENGTH (p_tplp));

          (*(pr_type->data_readval)) (&buf, &list_val2, list_id1->type_list.domp[0], -1, true);

          rc = eval_value_rel_cmp (&list_val, &list_val2, R_EQ);

          if (rc == V_ERROR)
            {
              pr_clear_value (&list_val);
              pr_clear_value (&list_val2);
              qfile_close_scan (thread_p, &s_id);
              free_and_init (p_tplrec.tpl);
              return V_ERROR;
            }
          else if (rc != V_TRUE)
            {
              card2 = eval_item_card_sort_list (thread_p, &list_val2, list_id2);
              if (card2 == ER_FAILED)
                {
                  pr_clear_value (&list_val);
                  pr_clear_value (&list_val2);
                  qfile_close_scan (thread_p, &s_id);
                  free_and_init (p_tplrec.tpl);
                  return V_ERROR;
                }
              else if (card2 == UNKNOWN_CARD)
                {
                  pr_clear_value (&list_val);
                  pr_clear_value (&list_val2);
                  qfile_close_scan (thread_p, &s_id);
                  free_and_init (p_tplrec.tpl);
                  return V_UNKNOWN;
                }

              if (card1 > card2)
                {
                  pr_clear_value (&list_val);
                  pr_clear_value (&list_val2);
                  qfile_close_scan (thread_p, &s_id);
                  free_and_init (p_tplrec.tpl);
                  return V_FALSE;
                }
              card1 = 0;
            }
          pr_clear_value (&list_val2);
        }

      tpl_len = QFILE_GET_TUPLE_LENGTH (tplrec.tpl);
      if (p_tplrec.size < tpl_len)
        {
          p_tplrec.size = tpl_len;
          p_tplrec.tpl = (QFILE_TUPLE) realloc (p_tplrec.tpl, tpl_len);
          if (p_tplrec.tpl == NULL)
            {
              return (DB_LOGICAL) ER_OUT_OF_VIRTUAL_MEMORY;
            }
        }
      memcpy (p_tplrec.tpl, tplrec.tpl, tpl_len);
      list_on = true;
      card1++;
    }

  if (qp_scan != S_END)
    {
      pr_clear_value (&list_val);
      qfile_close_scan (thread_p, &s_id);
      free_and_init (p_tplrec.tpl);
      return V_ERROR;
    }

  if (list_on == true)
    {
      p_tplp = (char *) p_tplrec.tpl + QFILE_TUPLE_LENGTH_SIZE; /* no unbound value */

      or_init (&buf, p_tplp + QFILE_TUPLE_VALUE_HEADER_SIZE, QFILE_GET_TUPLE_VALUE_LENGTH (p_tplp));

      if ((*(pr_type->data_readval)) (&buf, &list_val2, list_id1->type_list.domp[0], -1, true) != NO_ERROR)
        {
          /* TODO: look once more, need to free (tpl)? */
          pr_clear_value (&list_val);
          qfile_close_scan (thread_p, &s_id);
          free_and_init (p_tplrec.tpl);
          return V_ERROR;
        }

      card2 = eval_item_card_sort_list (thread_p, &list_val2, list_id2);
      if (card2 == ER_FAILED)
        {
          pr_clear_value (&list_val);
          pr_clear_value (&list_val2);
          qfile_close_scan (thread_p, &s_id);
          free_and_init (p_tplrec.tpl);
          return V_ERROR;
        }
      else if (card2 == UNKNOWN_CARD)
        {
          res = V_UNKNOWN;
        }
      else if (card1 > card2)
        {
          pr_clear_value (&list_val);
          pr_clear_value (&list_val2);
          qfile_close_scan (thread_p, &s_id);
          free_and_init (p_tplrec.tpl);
          return V_FALSE;
        }
    }

  pr_clear_value (&list_val);
  pr_clear_value (&list_val2);
  qfile_close_scan (thread_p, &s_id);
  free_and_init (p_tplrec.tpl);

  return res;
}

/*
 * eval_eq_multi_set_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   set(in): DB_SET representation
 *   list_id(in): Sorted LIST FILE identifier
 *
 * Note: Find if given multi_set is equal to the given list file.
 *              The routine uses the same semantics of finding equality
 *              relationship between two multi_sets.
 */
static DB_LOGICAL
eval_eq_multi_set_to_sort_list (THREAD_ENTRY * thread_p, DB_SET * set, QFILE_LIST_ID * list_id)
{
  DB_LOGICAL res1, res2;

  res1 = eval_sub_multi_set_to_sort_list (thread_p, set, list_id);
  res2 = eval_sub_sort_list_to_multi_set (thread_p, list_id, set);

  return eval_logical_result (res1, res2);
}

/*
 * eval_ne_multi_set_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   set(in): DB_SET representation
 *   list_id(in): Sorted LIST FILE identifier
 *
 * Note: Find if given multi_set is not equal to the given list file.
 */
static DB_LOGICAL
eval_ne_multi_set_to_sort_list (THREAD_ENTRY * thread_p, DB_SET * set, QFILE_LIST_ID * list_id)
{
  DB_LOGICAL res;

  res = eval_eq_multi_set_to_sort_list (thread_p, set, list_id);
  /* negate the result */
  return eval_negative (res);
}

/*
 * eval_le_multi_set_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   set(in): DB_SET representation
 *   list_id(in): Sorted LIST FILE identifier
 *
 * Note: Find if given multi_set is a subset of the given list file.
 */
static DB_LOGICAL
eval_le_multi_set_to_sort_list (THREAD_ENTRY * thread_p, DB_SET * set, QFILE_LIST_ID * list_id)
{
  return eval_sub_multi_set_to_sort_list (thread_p, set, list_id);
}

/*
 * eval_lt_multi_set_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   set(in): DB_SET representation
 *   list_id(in): Sorted LIST FILE identifier
 *
 * Note: Find if given multi_set is a proper subset of the given list file.
 */
static DB_LOGICAL
eval_lt_multi_set_to_sort_list (THREAD_ENTRY * thread_p, DB_SET * set, QFILE_LIST_ID * list_id)
{
  DB_LOGICAL res1, res2;

  res1 = eval_sub_multi_set_to_sort_list (thread_p, set, list_id);
  res2 = eval_ne_multi_set_to_sort_list (thread_p, set, list_id);

  return eval_logical_result (res1, res2);
}

/*
 * eval_le_sort_list_to_multi_set () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   list_id(in): Sorted LIST FILE identifier
 *   set(in): Multi_set disk representation
 *
 * Note: Find if given list file is a subset of the multi_set.
 */
static DB_LOGICAL
eval_le_sort_list_to_multi_set (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id, DB_SET * set)
{
  return eval_sub_sort_list_to_multi_set (thread_p, list_id, set);
}

/*
 * eval_lt_sort_list_to_multi_set () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   list_id(in): Sorted LIST FILE identifier
 *   set(in): DB_SET representation
 *
 * Note: Find if given list file is a proper subset of the multi_set.
 */
static DB_LOGICAL
eval_lt_sort_list_to_multi_set (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id, DB_SET * set)
{
  DB_LOGICAL res1, res2;

  res1 = eval_sub_sort_list_to_multi_set (thread_p, list_id, set);
  res2 = eval_ne_multi_set_to_sort_list (thread_p, set, list_id);

  return eval_logical_result (res1, res2);
}

/*
 * eval_eq_sort_list_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   list_id1(in): First Sorted LIST FILE identifier
 *   list_id2(in): Second Sorted LIST FILE identifier
 *
 * Note: Find if the first list file is equal to the second list file.
 *              The list files must be of one column and treated like
 *              multi_sets. The routine uses the same semantics of finding
 *              equality relationship between two multi_sets.
 */
static DB_LOGICAL
eval_eq_sort_list_to_sort_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2)
{
  DB_LOGICAL res1, res2;

  res1 = eval_sub_sort_list_to_sort_list (thread_p, list_id1, list_id2);
  res2 = eval_sub_sort_list_to_sort_list (thread_p, list_id2, list_id1);

  return eval_logical_result (res1, res2);
}

/*
 * eval_ne_sort_list_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   list_id1(in): First Sorted LIST FILE identifier
 *   list_id2(in): Second Sorted LIST FILE identifier
 *
 * Note: Find if the first list file is not equal to the second one.
 */
static DB_LOGICAL
eval_ne_sort_list_to_sort_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2)
{
  DB_LOGICAL res;

  res = eval_eq_sort_list_to_sort_list (thread_p, list_id1, list_id2);
  /* negate the result */
  return eval_negative (res);
}

/*
 * eval_le_sort_list_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   list_id1(in): First Sorted LIST FILE identifier
 *   list_id2(in): Second Sorted LIST FILE identifier
 *
 * Note: Find if the first list file is a subset if the second one.
 */
static DB_LOGICAL
eval_le_sort_list_to_sort_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2)
{
  return eval_sub_sort_list_to_sort_list (thread_p, list_id1, list_id2);
}

/*
 * eval_lt_sort_list_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   list_id1(in): First Sorted LIST FILE identifier
 *   list_id2(in): Second Sorted LIST FILE identifier
 *
 * Note: Find if the first list file is a proper subset if the second
 *       list file.
 */
static DB_LOGICAL
eval_lt_sort_list_to_sort_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2)
{
  DB_LOGICAL res1, res2;

  res1 = eval_sub_sort_list_to_sort_list (thread_p, list_id1, list_id2);
  res2 = eval_ne_sort_list_to_sort_list (thread_p, list_id1, list_id2);

  return eval_logical_result (res1, res2);
}

/*
 * eval_multi_set_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   set(in): DB_SET representation
 *   list_id(in): Sorted LIST FILE identifier
 *   rel_operator(in): Relational Operator
 *
 * Note: Find if given multi_set and the list file satisfy the
 *              relationship indicated by the relational operator. The list
 *              file must be of one column, sorted and is treated like a
 *              multi_set.
 */
static DB_LOGICAL
eval_multi_set_to_sort_list (THREAD_ENTRY * thread_p, DB_SET * set, QFILE_LIST_ID * list_id, REL_OP rel_operator)
{
  switch (rel_operator)
    {
    case R_LT:
      return eval_lt_multi_set_to_sort_list (thread_p, set, list_id);
    case R_LE:
      return eval_le_multi_set_to_sort_list (thread_p, set, list_id);
    case R_GT:
      return eval_lt_sort_list_to_multi_set (thread_p, list_id, set);
    case R_GE:
      return eval_le_sort_list_to_multi_set (thread_p, list_id, set);
    case R_EQ:
      return eval_eq_multi_set_to_sort_list (thread_p, set, list_id);
    case R_NE:
      return eval_ne_multi_set_to_sort_list (thread_p, set, list_id);
    default:
      return V_ERROR;
    }
}

/*
 * eval_sort_list_to_multi_set () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   list_id(in): Sorted LIST FILE identifier
 *   set(in): DB_SET representation
 *   rel_operator(in): Relational Operator
 *
 * Note: Find if given list file and the multi_set satisfy the
 *              relationship indicated by the relational operator. The list
 *              file must be of one column, sorted and is treated like a
 *              multi_set.
 */
static DB_LOGICAL
eval_sort_list_to_multi_set (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id, DB_SET * set, REL_OP rel_operator)
{
  switch (rel_operator)
    {
    case R_LT:
      return eval_lt_sort_list_to_multi_set (thread_p, list_id, set);
    case R_LE:
      return eval_le_sort_list_to_multi_set (thread_p, list_id, set);
    case R_GT:
      return eval_lt_multi_set_to_sort_list (thread_p, set, list_id);
    case R_GE:
      return eval_le_multi_set_to_sort_list (thread_p, set, list_id);
    case R_EQ:
      return eval_eq_multi_set_to_sort_list (thread_p, set, list_id);
    case R_NE:
      return eval_ne_multi_set_to_sort_list (thread_p, set, list_id);
    default:
      return V_ERROR;
    }
}

/*
 * eval_sort_list_to_sort_list () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   list_id1(in): First Sorted LIST FILE identifier
 *   list_id2(in): Second Sorted LIST FILE identifier
 *   rel_operator(in): Relational Operator
 *
 * Note: Find if first list file and the second list file satisfy the
 *              relationship indicated by the relational operator. The list
 *              files must be of one column, sorted and are treated like
 *              multi_sets.
 */
static DB_LOGICAL
eval_sort_list_to_sort_list (THREAD_ENTRY * thread_p,
                             QFILE_LIST_ID * list_id1, QFILE_LIST_ID * list_id2, REL_OP rel_operator)
{
  switch (rel_operator)
    {
    case R_LT:
      return eval_lt_sort_list_to_sort_list (thread_p, list_id1, list_id2);
    case R_LE:
      return eval_le_sort_list_to_sort_list (thread_p, list_id1, list_id2);
    case R_GT:
      return eval_lt_sort_list_to_sort_list (thread_p, list_id2, list_id1);
    case R_GE:
      return eval_le_sort_list_to_sort_list (thread_p, list_id2, list_id1);
    case R_EQ:
      return eval_eq_sort_list_to_sort_list (thread_p, list_id1, list_id2);
    case R_NE:
      return eval_ne_sort_list_to_sort_list (thread_p, list_id1, list_id2);
    default:
      return V_ERROR;
    }
}

/*
 * eval_set_list_cmp () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   et_comp(in): compound evaluation term
 *   vd(in):
 *   dbval1(in): lhs db_value, if lhs is a set.
 *   dbval2(in): rhs db_value, if rhs is a set.
 *
 * Note: Perform set/set, set/list, and list/list comparisons.
 */
static DB_LOGICAL
eval_set_list_cmp (THREAD_ENTRY * thread_p, COMP_EVAL_TERM * et_comp,
                   VAL_DESCR * vd, DB_VALUE * dbval1, DB_VALUE * dbval2)
{
  QFILE_LIST_ID *t_list_id;
  QFILE_SORTED_LIST_ID *lhs_srlist_id, *rhs_srlist_id;

  if (et_comp->comp_lhs->type == TYPE_LIST_ID)
    {
      /* execute linked query */
      EXECUTE_REGU_VARIABLE_XASL (thread_p, et_comp->comp_lhs, vd);
      if (CHECK_REGU_VARIABLE_XASL_STATUS (et_comp->comp_lhs) != XASL_SUCCESS)
        {
          return V_ERROR;
        }

      /*
       * lhs value refers to a list file. for efficiency reasons
       * first sort the list file
       */
      lhs_srlist_id = et_comp->comp_lhs->value.srlist_id;
      if (lhs_srlist_id->sorted == false)
        {
          if (lhs_srlist_id->list_id->tuple_cnt > 1)
            {
              t_list_id = qfile_sort_list (thread_p, lhs_srlist_id->list_id, NULL, Q_ALL, true);
              if (t_list_id == NULL)
                {
                  return V_ERROR;
                }
            }
          lhs_srlist_id->sorted = true;
        }

      /* rhs value can only be either a set or a list file */
      if (et_comp->comp_rhs->type == TYPE_LIST_ID)
        {
          /* execute linked query */
          EXECUTE_REGU_VARIABLE_XASL (thread_p, et_comp->comp_rhs, vd);
          if (CHECK_REGU_VARIABLE_XASL_STATUS (et_comp->comp_rhs) != XASL_SUCCESS)
            {
              return V_ERROR;
            }

          /*
           * rhs value refers to a list file. for efficiency reasons
           * first sort the list file
           */
          rhs_srlist_id = et_comp->comp_rhs->value.srlist_id;
          if (rhs_srlist_id->sorted == false)
            {
              if (rhs_srlist_id->list_id->tuple_cnt > 1)
                {
                  t_list_id = qfile_sort_list (thread_p, rhs_srlist_id->list_id, NULL, Q_ALL, true);
                  if (t_list_id == NULL)
                    {
                      return V_ERROR;
                    }
                }
              rhs_srlist_id->sorted = true;
            }

          /* compare two list files */
          return eval_sort_list_to_sort_list (thread_p,
                                              lhs_srlist_id->list_id, rhs_srlist_id->list_id, et_comp->comp_rel_op);
        }
      else
        {
          /* compare list file and set */
          return eval_sort_list_to_multi_set (thread_p,
                                              lhs_srlist_id->list_id, DB_GET_SET (dbval2), et_comp->comp_rel_op);
        }
    }
  else if (et_comp->comp_rhs->type == TYPE_LIST_ID)
    {
      /* execute linked query */
      EXECUTE_REGU_VARIABLE_XASL (thread_p, et_comp->comp_rhs, vd);
      if (CHECK_REGU_VARIABLE_XASL_STATUS (et_comp->comp_rhs) != XASL_SUCCESS)
        {
          return V_ERROR;
        }

      /*
       * rhs value refers to a list file. for efficiency reasons
       * first sort the list file
       */
      rhs_srlist_id = et_comp->comp_rhs->value.srlist_id;
      if (rhs_srlist_id->sorted == false)
        {
          if (rhs_srlist_id->list_id->tuple_cnt > 1)
            {
              t_list_id = qfile_sort_list (thread_p, rhs_srlist_id->list_id, NULL, Q_ALL, true);
              if (t_list_id == NULL)
                {
                  return V_ERROR;
                }
            }
          rhs_srlist_id->sorted = true;
        }

      /* lhs must be a set value, compare set and list */
      return eval_multi_set_to_sort_list (thread_p, DB_GET_SET (dbval1), rhs_srlist_id->list_id, et_comp->comp_rel_op);
    }

  return V_UNKNOWN;
}
#endif

/*
 * Main Predicate Evaluation Routines
 */

DB_LOGICAL
eval_limit_count_is_0 (THREAD_ENTRY * thread_p, REGU_VARIABLE * rv, VAL_DESCR * vd)
{
  DB_VALUE *limit_row_count_valp, zero_val;

  if (fetch_peek_dbval (thread_p, rv, vd, NULL, NULL, &limit_row_count_valp) != NO_ERROR)
    {
      return V_UNKNOWN;
    }

  db_make_int (&zero_val, 0);

  return eval_value_rel_cmp (limit_row_count_valp, &zero_val, R_EQ);
}

/*
 * eval_pred () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   pr(in): Predicate Expression Tree
 *   vd(in): Value descriptor for positional values (optional)
 *   obj_oid(in): Object Identifier
 *
 * Note: This is the main predicate expression evalution routine. It
 *              evaluates the given predicate predicate expression on the
 *              specified evaluation item to see if the evaluation item
 *              satisfies the indicate predicate. It uses a 3-valued logic
 *              and returns V_TRUE, V_FALSE or V_UNKNOWN. If an error occurs,
 *              necessary error code is set and V_ERROR is returned.
 */
DB_LOGICAL
eval_pred (THREAD_ENTRY * thread_p, PRED_EXPR * pr, VAL_DESCR * vd, OID * obj_oid)
{
  COMP_EVAL_TERM *et_comp;
  ALSM_EVAL_TERM *et_alsm;
  LIKE_EVAL_TERM *et_like;
  DB_VALUE *peek_val1, *peek_val2, *peek_val3;
  DB_LOGICAL result = V_UNKNOWN;
  int regexp_res;
  PRED_EXPR *t_pr;
  QFILE_SORTED_LIST_ID *srlist_id;

  peek_val1 = NULL;
  peek_val2 = NULL;
  peek_val3 = NULL;

  if (thread_get_recursion_depth (thread_p) > prm_get_integer_value (PRM_ID_MAX_RECURSION_SQL_DEPTH))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_MAX_RECURSION_SQL_DEPTH, 1,
              prm_get_integer_value (PRM_ID_MAX_RECURSION_SQL_DEPTH));

      return V_ERROR;
    }

  thread_inc_recursion_depth (thread_p);

  switch (pr->type)
    {
    case T_PRED:
      switch (pr->pe.pred.bool_op)
        {
        case B_AND:
          /* 'pt_to_pred_expr()' will generate right-linear tree */
          result = V_TRUE;
          for (t_pr = pr;
               result == V_TRUE && t_pr->type == T_PRED && t_pr->pe.pred.bool_op == B_AND; t_pr = t_pr->pe.pred.rhs)
            {
              if (result == V_UNKNOWN)
                {
                  result = eval_pred (thread_p, t_pr->pe.pred.lhs, vd, obj_oid);
                  result = (result == V_TRUE) ? V_UNKNOWN : result;
                }
              else
                {
                  result = eval_pred (thread_p, t_pr->pe.pred.lhs, vd, obj_oid);
                }

              if (result == V_FALSE || result == V_ERROR)
                {
                  goto exit;
                }
            }

          if (result == V_UNKNOWN)
            {
              result = eval_pred (thread_p, t_pr, vd, obj_oid);
              result = (result == V_TRUE) ? V_UNKNOWN : result;
            }
          else
            {
              result = eval_pred (thread_p, t_pr, vd, obj_oid);
            }
          break;

        case B_OR:
          /* 'pt_to_pred_expr()' will generate right-linear tree */
          result = V_FALSE;
          for (t_pr = pr;
               result == V_FALSE && t_pr->type == T_PRED && t_pr->pe.pred.bool_op == B_OR; t_pr = t_pr->pe.pred.rhs)
            {
              if (result == V_UNKNOWN)
                {
                  result = eval_pred (thread_p, t_pr->pe.pred.lhs, vd, obj_oid);
                  result = (result == V_FALSE) ? V_UNKNOWN : result;
                }
              else
                {
                  result = eval_pred (thread_p, t_pr->pe.pred.lhs, vd, obj_oid);
                }

              if (result == V_TRUE || result == V_ERROR)
                {
                  goto exit;
                }
            }

          if (result == V_UNKNOWN)
            {
              result = eval_pred (thread_p, t_pr, vd, obj_oid);
              result = (result == V_FALSE) ? V_UNKNOWN : result;
            }
          else
            {
              result = eval_pred (thread_p, t_pr, vd, obj_oid);
            }
          break;

        case B_XOR:
          {
            DB_LOGICAL result_lhs, result_rhs;

            result_lhs = eval_pred (thread_p, pr->pe.pred.lhs, vd, obj_oid);
            result_rhs = eval_pred (thread_p, pr->pe.pred.rhs, vd, obj_oid);

            if (result_lhs == V_ERROR || result_rhs == V_ERROR)
              result = V_ERROR;
            else if (result_lhs == V_UNKNOWN || result_rhs == V_UNKNOWN)
              result = V_UNKNOWN;
            else if (result_lhs == result_rhs)
              result = V_FALSE;
            else
              result = V_TRUE;
          }
          break;

        case B_IS:
        case B_IS_NOT:
          {
            DB_LOGICAL result_lhs, result_rhs;
            DB_LOGICAL _v_true, _v_false;

            _v_true = (pr->pe.pred.bool_op == B_IS) ? V_TRUE : V_FALSE;
            _v_false = V_TRUE - _v_true;

            result_lhs = eval_pred (thread_p, pr->pe.pred.lhs, vd, obj_oid);
            result_rhs = eval_pred (thread_p, pr->pe.pred.rhs, vd, obj_oid);

            if (result_lhs == V_ERROR || result_rhs == V_ERROR)
              result = V_ERROR;
            else if (result_lhs == result_rhs)
              result = _v_true;
            else
              result = _v_false;
          }
          break;

        default:
          result = V_ERROR;
          break;
        }
      break;

    case T_EVAL_TERM:
      switch (pr->pe.eval_term.et_type)
        {
        case T_COMP_EVAL_TERM:
          /*
           * compound evaluation terms are used to test relationships
           * such as equality, greater than etc. between two items
           * Each datatype defines its own meaning of relationship
           * indicated by one of the relational operators.
           */
          et_comp = &pr->pe.eval_term.et.et_comp;

          switch (et_comp->comp_rel_op)
            {
            case R_NULL:
              if (fetch_peek_dbval (thread_p, et_comp->comp_lhs, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
                {
                  result = V_ERROR;
                  goto exit;
                }

              if (db_value_is_null (peek_val1))
                {
                  result = V_TRUE;
                }
              else
                {
                  assert (DB_VALUE_DOMAIN_TYPE (peek_val1) != DB_TYPE_OID);

                  result = V_FALSE;
                }
              break;

            case R_EXISTS:
              /* leaf node should refer to a list file */
              if (et_comp->comp_lhs->type == TYPE_LIST_ID)
                {
                  /* execute linked query */
                  EXECUTE_REGU_VARIABLE_XASL (thread_p, et_comp->comp_lhs, vd);
                  if (CHECK_REGU_VARIABLE_XASL_STATUS (et_comp->comp_lhs) != XASL_SUCCESS)
                    {
                      result = V_ERROR;
                      goto exit;
                    }

                  srlist_id = et_comp->comp_lhs->value.srlist_id;
                  result = ((srlist_id->list_id->tuple_cnt > 0) ? V_TRUE : V_FALSE);
                }
              else
                {
                  assert (false);       /* impossible */
                  result = V_ERROR;
                  goto exit;
                }
              break;

            default:
              /*
               * fetch left hand size and right hand size values, if one of
               * values are unbound, result = V_UNKNOWN
               */
              if (et_comp->comp_lhs->type == TYPE_LIST_ID || et_comp->comp_rhs->type == TYPE_LIST_ID)
                {
                  assert (false);       /* impossible */
                  result = V_ERROR;
                  goto exit;
                }

              if (fetch_peek_dbval (thread_p, et_comp->comp_lhs, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
                {
                  result = V_ERROR;
                  goto exit;
                }
              else if (db_value_is_null (peek_val1))
                {
                  if (et_comp->comp_rel_op != R_EQ_TORDER && et_comp->comp_rel_op != R_NULLSAFE_EQ)
                    {
                      result = V_UNKNOWN;
                      goto exit;
                    }
                }

              if (fetch_peek_dbval (thread_p, et_comp->comp_rhs, vd, obj_oid, NULL, &peek_val2) != NO_ERROR)
                {
                  result = V_ERROR;
                  goto exit;
                }
              else if (db_value_is_null (peek_val2))
                {
                  if (et_comp->comp_rel_op != R_EQ_TORDER && et_comp->comp_rel_op != R_NULLSAFE_EQ)
                    {
                      result = V_UNKNOWN;
                      goto exit;
                    }
                }

              /*
               * general case: compare values, db_value_compare will
               * take care of any coercion necessary.
               */
              assert (et_comp->comp_lhs != NULL);
              assert (et_comp->comp_rhs != NULL);

              result = eval_value_rel_cmp (peek_val1, peek_val2, et_comp->comp_rel_op);
              break;
            }

          break;

        case T_ALSM_EVAL_TERM:
          et_alsm = &pr->pe.eval_term.et.et_alsm;

          /*
           * Note: According to ANSI, if the set or list file is empty,
           * the result of comparison is true/false for ALL/SOME,
           * regardless of whether lhs value is bound or not.
           */
          if (et_alsm->elemset->type != TYPE_LIST_ID)
            {
              /* fetch set (group of items) value */
              if (fetch_peek_dbval (thread_p, et_alsm->elemset, vd, obj_oid, NULL, &peek_val2) != NO_ERROR)
                {
                  result = V_ERROR;
                  goto exit;
                }
              else if (db_value_is_null (peek_val2))
                {
                  result = V_UNKNOWN;
                  goto exit;
                }

#if 1                           /* TODO - */
              if (!TP_IS_SET_TYPE (DB_VALUE_DOMAIN_TYPE (peek_val2)))
                {
                  /* is not comparable */
                  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                          ER_TP_CANT_COERCE, 2,
                          pr_type_name (DB_VALUE_DOMAIN_TYPE (peek_val2)), pr_type_name (DB_TYPE_SEQUENCE));

                  result = V_ERROR;
                  goto exit;
                }
#endif

              if (set_size (DB_GET_SET (peek_val2)) == 0)
                {
                  /* empty set */
                  result = V_FALSE;
                  goto exit;
                }
            }
          else
            {
              /* execute linked query */
              EXECUTE_REGU_VARIABLE_XASL (thread_p, et_alsm->elemset, vd);
              if (CHECK_REGU_VARIABLE_XASL_STATUS (et_alsm->elemset) != XASL_SUCCESS)
                {
                  result = V_ERROR;
                  goto exit;
                }
              else
                {
                  /* check of empty list file */
                  srlist_id = et_alsm->elemset->value.srlist_id;
                  if (srlist_id->list_id->tuple_cnt == 0)
                    {
                      result = V_FALSE;
                      goto exit;
                    }
                }
            }

          /* fetch item value */
          if (fetch_peek_dbval (thread_p, et_alsm->elem, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
            {
              result = V_ERROR;
              goto exit;
            }
          else if (db_value_is_null (peek_val1))
            {
              result = V_UNKNOWN;
              goto exit;
            }

          if (et_alsm->elemset->type == TYPE_LIST_ID)
            {
              /* rhs value is a list, use list evaluation routines */
              srlist_id = et_alsm->elemset->value.srlist_id;
              result = eval_some_list_eval (thread_p, peek_val1, srlist_id->list_id, et_alsm->alsm_rel_op);
            }
          else
            {
              /* rhs value is a set, use set evaluation routines */
              result = eval_some_eval (peek_val1, DB_GET_SET (peek_val2), et_alsm->alsm_rel_op);
            }
          break;

        case T_LIKE_EVAL_TERM:
          et_like = &pr->pe.eval_term.et.et_like;

          /* fetch source text expression */
          if (fetch_peek_dbval (thread_p, et_like->src, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
            {
              result = V_ERROR;
              goto exit;
            }
          else if (db_value_is_null (peek_val1))
            {
              result = V_UNKNOWN;
              goto exit;
            }

          /* fetch pattern regular expression */
          if (fetch_peek_dbval (thread_p, et_like->pattern, vd, obj_oid, NULL, &peek_val2) != NO_ERROR)
            {
              result = V_ERROR;
              goto exit;
            }
          else if (db_value_is_null (peek_val2))
            {
              result = V_UNKNOWN;
              goto exit;
            }

          if (et_like->esc_char)
            {
              /* fetch escape regular expression */
              if (fetch_peek_dbval (thread_p, et_like->esc_char, vd, obj_oid, NULL, &peek_val3) != NO_ERROR)
                {
                  result = V_ERROR;
                  goto exit;
                }
            }
          /* evaluate regular expression match */
          /* Note: Currently only STRING type is supported */
          db_string_like (peek_val1, peek_val2, peek_val3, &regexp_res);
          result = (DB_LOGICAL) regexp_res;
          break;

        case T_RLIKE_EVAL_TERM:
          /* evaluate rlike */
          result = eval_pred_rlike7 (thread_p, pr, vd, obj_oid);
          break;

        default:
          result = V_ERROR;
          break;
        }
      break;

    case T_NOT_TERM:
      result = eval_pred (thread_p, pr->pe.not_term, vd, obj_oid);
      /* negate the result */
      result = eval_negative (result);
      break;

    default:
      result = V_ERROR;
    }

exit:

  thread_dec_recursion_depth (thread_p);

  return result;
}

/*
 * eval_pred_comp0 () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   pr(in): Predicate Expression Tree
 *   vd(in): Value descriptor for positional values (optional)
 *   obj_oid(in): Object Identifier
 *
 * Note: single node regular comparison predicate
 */
DB_LOGICAL
eval_pred_comp0 (THREAD_ENTRY * thread_p, PRED_EXPR * pr, VAL_DESCR * vd, OID * obj_oid)
{
  COMP_EVAL_TERM *et_comp;
  DB_VALUE *peek_val1, *peek_val2;

  peek_val1 = NULL;
  peek_val2 = NULL;

  et_comp = &pr->pe.eval_term.et.et_comp;

  /*
   * fetch left hand size and right hand size values, if one of
   * values are unbound, return V_UNKNOWN
   */
  if (fetch_peek_dbval (thread_p, et_comp->comp_lhs, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
    {
      return V_ERROR;
    }
  else if (db_value_is_null (peek_val1) && et_comp->comp_rel_op != R_NULLSAFE_EQ)
    {
      return V_UNKNOWN;
    }

  if (fetch_peek_dbval (thread_p, et_comp->comp_rhs, vd, obj_oid, NULL, &peek_val2) != NO_ERROR)
    {
      return V_ERROR;
    }
  else if (db_value_is_null (peek_val2) && et_comp->comp_rel_op != R_NULLSAFE_EQ)
    {
      return V_UNKNOWN;
    }

  /*
   * general case: compare values, db_value_compare will
   * take care of any coercion necessary.
   */
  assert (et_comp->comp_lhs != NULL);
  assert (et_comp->comp_rhs != NULL);

  return eval_value_rel_cmp (peek_val1, peek_val2, et_comp->comp_rel_op);
}

/*
 * eval_pred_comp1 () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   pr(in): Predicate Expression Tree
 *   vd(in): Value descriptor for positional values (optional)
 *   obj_oid(in): Object Identifier
 *
 * Note: single leaf node NULL predicate
 */
DB_LOGICAL
eval_pred_comp1 (THREAD_ENTRY * thread_p, PRED_EXPR * pr, VAL_DESCR * vd, OID * obj_oid)
{
  COMP_EVAL_TERM *et_comp;
  DB_VALUE *peek_val1;
  DB_LOGICAL res = V_FALSE;

  peek_val1 = NULL;

  et_comp = &pr->pe.eval_term.et.et_comp;

  if (fetch_peek_dbval (thread_p, et_comp->comp_lhs, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
    {
      return V_ERROR;
    }

  if (db_value_is_null (peek_val1))
    {
      res = V_TRUE;
    }
  else
    {
      assert (DB_VALUE_DOMAIN_TYPE (peek_val1) != DB_TYPE_OID);
      res = V_FALSE;
    }

  return res;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * eval_pred_comp2 () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   pr(in): Predicate Expression Tree
 *   vd(in): Value descriptor for positional values (optional)
 *   obj_oid(in): Object Identifier
 *
 * Note: single node EXIST predicate
 */
DB_LOGICAL
eval_pred_comp2 (THREAD_ENTRY * thread_p, PRED_EXPR * pr, VAL_DESCR * vd, OID * obj_oid)
{
  COMP_EVAL_TERM *et_comp;
  DB_VALUE *peek_val1;

  peek_val1 = NULL;

  et_comp = &pr->pe.eval_term.et.et_comp;

  /* evaluate EXISTS predicate, if specified */
  /* leaf node should refer to either a set or list file */
  if (et_comp->comp_lhs->type == TYPE_LIST_ID)
    {
      /* execute linked query */
      EXECUTE_REGU_VARIABLE_XASL (thread_p, et_comp->comp_lhs, vd);
      if (CHECK_REGU_VARIABLE_XASL_STATUS (et_comp->comp_lhs) != XASL_SUCCESS)
        {
          return V_ERROR;
        }
      else
        {
          QFILE_SORTED_LIST_ID *srlist_id;

          srlist_id = et_comp->comp_lhs->value.srlist_id;
          return (srlist_id->list_id->tuple_cnt > 0) ? V_TRUE : V_FALSE;
        }
    }
  else
    {
      if (fetch_peek_dbval (thread_p, et_comp->comp_lhs, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
        {
          return V_ERROR;
        }
      else if (db_value_is_null (peek_val1))
        {
          return V_UNKNOWN;
        }

#if 1                           /* TODO - */
      if (!TP_IS_SET_TYPE (DB_VALUE_DOMAIN_TYPE (peek_val1)))
        {
          /* is not comparable */
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                  ER_TP_CANT_COERCE, 2,
                  pr_type_name (DB_VALUE_DOMAIN_TYPE (peek_val1)), pr_type_name (DB_TYPE_SEQUENCE));

          return V_ERROR;
        }
#endif

      return (set_size (DB_GET_SET (peek_val1)) > 0) ? V_TRUE : V_FALSE;
    }
}

/*
 * eval_pred_comp3 () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   pr(in): Predicate Expression Tree
 *   vd(in): Value descriptor for positional values (optional)
 *   obj_oid(in): Object Identifier
 *
 * Note: single node lhs or rhs list file predicate
 */
DB_LOGICAL
eval_pred_comp3 (THREAD_ENTRY * thread_p, PRED_EXPR * pr, VAL_DESCR * vd, OID * obj_oid)
{
  COMP_EVAL_TERM *et_comp;
  DB_VALUE *peek_val1, *peek_val2;

  peek_val1 = NULL;
  peek_val2 = NULL;

  et_comp = &pr->pe.eval_term.et.et_comp;

  /*
   * fetch left hand size and right hand size values, if one of
   * values are unbound, result = V_UNKNOWN
   */
  if (et_comp->comp_lhs->type != TYPE_LIST_ID)
    {
      if (fetch_peek_dbval (thread_p, et_comp->comp_lhs, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
        {
          return V_ERROR;
        }
      else if (db_value_is_null (peek_val1))
        {
          return V_UNKNOWN;
        }
    }

  if (et_comp->comp_rhs->type != TYPE_LIST_ID)
    {
      if (fetch_peek_dbval (thread_p, et_comp->comp_rhs, vd, obj_oid, NULL, &peek_val2) != NO_ERROR)
        {
          return V_ERROR;
        }
      else if (db_value_is_null (peek_val2))
        {
          return V_UNKNOWN;
        }
    }

  if (et_comp->comp_lhs->type == TYPE_LIST_ID || et_comp->comp_rhs->type == TYPE_LIST_ID)
    {
      assert (false);           /* is impossible */
      return eval_set_list_cmp (thread_p, et_comp, vd, peek_val1, peek_val2);
    }
  else
    {
      return V_UNKNOWN;
    }
}
#endif

/*
 * eval_pred_alsm4 () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   pr(in): Predicate Expression Tree
 *   vd(in): Value descriptor for positional values (optional)
 *   obj_oid(in): Object Identifier
 *
 * Note: single node all/some predicate with a set
 */
DB_LOGICAL
eval_pred_alsm4 (THREAD_ENTRY * thread_p, PRED_EXPR * pr, VAL_DESCR * vd, OID * obj_oid)
{
  ALSM_EVAL_TERM *et_alsm;
  DB_VALUE *peek_val1, *peek_val2;

  peek_val1 = NULL;
  peek_val2 = NULL;

  et_alsm = &pr->pe.eval_term.et.et_alsm;

  /*
   * Note: According to ANSI, if the set or list file is empty,
   *       the result of comparison is true/false for ALL/SOME,
   *       regardles of whether lhs value is bound or not.
   */
  if (fetch_peek_dbval (thread_p, et_alsm->elemset, vd, obj_oid, NULL, &peek_val2) != NO_ERROR)
    {
      return V_ERROR;
    }
  else if (db_value_is_null (peek_val2))
    {
      return V_UNKNOWN;
    }

#if 1                           /* TODO - */
  if (!TP_IS_SET_TYPE (DB_VALUE_DOMAIN_TYPE (peek_val2)))
    {
      /* is not comparable */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
              ER_TP_CANT_COERCE, 2, pr_type_name (DB_VALUE_DOMAIN_TYPE (peek_val2)), pr_type_name (DB_TYPE_SEQUENCE));

      return V_ERROR;
    }
#endif

  if (set_size (DB_GET_SET (peek_val2)) == 0)
    {
      /* empty set */
      return V_FALSE;
    }

  /* fetch item value */
  if (fetch_peek_dbval (thread_p, et_alsm->elem, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
    {
      return V_ERROR;
    }
  else if (db_value_is_null (peek_val1))
    {
      return V_UNKNOWN;
    }

  /* rhs value is a set, use set evaluation routines */
  return eval_some_eval (peek_val1, DB_GET_SET (peek_val2), et_alsm->alsm_rel_op);
}

/*
 * eval_pred_alsm5 () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   pr(in): Predicate Expression Tree
 *   vd(in): Value descriptor for positional values (optional)
 *   obj_oid(in): Object Identifier
 *
 * Note: single node all/some  predicate with a list file
 */
DB_LOGICAL
eval_pred_alsm5 (THREAD_ENTRY * thread_p, PRED_EXPR * pr, VAL_DESCR * vd, OID * obj_oid)
{
  ALSM_EVAL_TERM *et_alsm;
  DB_VALUE *peek_val1;
  QFILE_SORTED_LIST_ID *srlist_id;

  peek_val1 = NULL;

  et_alsm = &pr->pe.eval_term.et.et_alsm;

  /* execute linked query */
  EXECUTE_REGU_VARIABLE_XASL (thread_p, et_alsm->elemset, vd);
  if (CHECK_REGU_VARIABLE_XASL_STATUS (et_alsm->elemset) != XASL_SUCCESS)
    {
      return V_ERROR;
    }

  /*
   * Note: According to ANSI, if the set or list file is empty,
   *       the result of comparison is true/false for ALL/SOME,
   *       regardless of whether lhs value is bound or not.
   */
  srlist_id = et_alsm->elemset->value.srlist_id;
  if (srlist_id->list_id->tuple_cnt == 0)
    {
      return V_FALSE;
    }

  /* fetch item value */
  if (fetch_peek_dbval (thread_p, et_alsm->elem, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
    {
      return V_ERROR;
    }
  else if (db_value_is_null (peek_val1))
    {
      return V_UNKNOWN;
    }

  return eval_some_list_eval (thread_p, peek_val1, srlist_id->list_id, et_alsm->alsm_rel_op);
}

/*
 * eval_pred_like6 () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   pr(in): Predicate Expression Tree
 *   vd(in): Value descriptor for positional values (optional)
 *   obj_oid(in): Object Identifier
 *
 * Note: single node like predicate
 */
DB_LOGICAL
eval_pred_like6 (THREAD_ENTRY * thread_p, PRED_EXPR * pr, VAL_DESCR * vd, OID * obj_oid)
{
  LIKE_EVAL_TERM *et_like;
  DB_VALUE *peek_val1, *peek_val2, *peek_val3;
  int regexp_res;

  peek_val1 = NULL;
  peek_val2 = NULL;
  peek_val3 = NULL;

  et_like = &pr->pe.eval_term.et.et_like;

  /* fetch source text expression */
  if (fetch_peek_dbval (thread_p, et_like->src, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
    {
      return V_ERROR;
    }
  else if (db_value_is_null (peek_val1))
    {
      return V_UNKNOWN;
    }

  /* fetch pattern regular expression */
  if (fetch_peek_dbval (thread_p, et_like->pattern, vd, obj_oid, NULL, &peek_val2) != NO_ERROR)
    {
      return V_ERROR;
    }
  else if (db_value_is_null (peek_val2))
    {
      return V_UNKNOWN;
    }

  if (et_like->esc_char)
    {
      /* fetch escape regular expression */
      if (fetch_peek_dbval (thread_p, et_like->esc_char, vd, obj_oid, NULL, &peek_val3) != NO_ERROR)
        {
          return V_ERROR;
        }
    }

  /* evaluate regular expression match */
  /* Note: Currently only STRING type is supported */
  db_string_like (peek_val1, peek_val2, peek_val3, &regexp_res);

  return (DB_LOGICAL) regexp_res;
}

/*
 * eval_pred_rlike7 () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 *   pr(in): Predicate Expression Tree
 *   vd(in): Value descriptor for positional values (optional)
 *   obj_oid(in): Object Identifier
 *
 * Note: single node like predicate
 */
DB_LOGICAL
eval_pred_rlike7 (THREAD_ENTRY * thread_p, PRED_EXPR * pr, VAL_DESCR * vd, OID * obj_oid)
{
  RLIKE_EVAL_TERM *et_rlike;
  DB_VALUE *peek_val1, *peek_val2, *peek_val3;
  int regexp_res;

  peek_val1 = NULL;
  peek_val2 = NULL;
  peek_val3 = NULL;

  et_rlike = &pr->pe.eval_term.et.et_rlike;

  /* fetch source text expression */
  if (fetch_peek_dbval (thread_p, et_rlike->src, vd, obj_oid, NULL, &peek_val1) != NO_ERROR)
    {
      return V_ERROR;
    }
  else if (db_value_is_null (peek_val1))
    {
      return V_UNKNOWN;
    }

  /* fetch pattern */
  if (fetch_peek_dbval (thread_p, et_rlike->pattern, vd, obj_oid, NULL, &peek_val2) != NO_ERROR)
    {
      return V_ERROR;
    }
  else if (db_value_is_null (peek_val2))
    {
      return V_UNKNOWN;
    }

  /* fetch case sensitiveness */
  if (fetch_peek_dbval (thread_p, et_rlike->case_sensitive, vd, obj_oid, NULL, &peek_val3) != NO_ERROR)
    {
      return V_ERROR;
    }
  else if (db_value_is_null (peek_val3))
    {
      return V_UNKNOWN;
    }

  /* evaluate regular expression match */
  db_string_rlike (peek_val1, peek_val2, peek_val3,
                   &et_rlike->compiled_regex, &et_rlike->compiled_pattern, &regexp_res);

  return (DB_LOGICAL) regexp_res;
}

/*
 * eval_fnc () -
 *   return:
 *   pr(in): Predicate Expression Tree
 */
PR_EVAL_FNC
eval_fnc (UNUSED_ARG THREAD_ENTRY * thread_p, PRED_EXPR * pr)
{
  COMP_EVAL_TERM *et_comp;
  ALSM_EVAL_TERM *et_alsm;

  if (pr == NULL)
    {
      return NULL;
    }

  if (pr->type == T_EVAL_TERM)
    {
      switch (pr->pe.eval_term.et_type)
        {
        case T_COMP_EVAL_TERM:
          et_comp = &pr->pe.eval_term.et.et_comp;

          if (et_comp->comp_rel_op == R_NULL)
            {
              return (PR_EVAL_FNC) eval_pred_comp1;
            }

          if (et_comp->comp_rel_op == R_EXISTS
              || et_comp->comp_lhs->type == TYPE_LIST_ID || et_comp->comp_rhs->type == TYPE_LIST_ID)
            {
              assert (false);   /* is impossible */
              return NULL;
            }

          return (PR_EVAL_FNC) eval_pred_comp0;

        case T_ALSM_EVAL_TERM:
          et_alsm = &pr->pe.eval_term.et.et_alsm;

          return ((et_alsm->elemset->type != TYPE_LIST_ID) ?
                  (PR_EVAL_FNC) eval_pred_alsm4 : (PR_EVAL_FNC) eval_pred_alsm5);

        case T_LIKE_EVAL_TERM:
          return (PR_EVAL_FNC) eval_pred_like6;

        case T_RLIKE_EVAL_TERM:
          return (PR_EVAL_FNC) eval_pred_rlike7;

        default:
          assert (false);       /* is impossible */
          return NULL;
        }
    }

  /* general case */
  return (PR_EVAL_FNC) eval_pred;
}

/*
 * eval_data_filter () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 * 	 oid(in): pointer to OID
 *   recdesp(in): pointer to RECDES (record descriptor)
 *   filterp(in): pointer to FILTER_INFO (filter information)
 *
 * Note: evaluate data filter(predicates) given as FILTER_INFO.
 */
DB_LOGICAL
eval_data_filter (THREAD_ENTRY * thread_p, OID * oid, RECDES * recdesp, FILTER_INFO * filterp)
{
  SCAN_PRED *scan_predp;
  SCAN_ATTRS *scan_attrsp;
  DB_LOGICAL ev_res;

  assert (oid != NULL);
  assert (recdesp != NULL);
  if (oid == NULL || recdesp == NULL)
    {
      return V_ERROR;
    }

  if (!filterp)
    {
      return V_TRUE;
    }

  scan_predp = filterp->scan_pred;
  scan_attrsp = filterp->scan_attrs;
  if (!scan_predp || !scan_attrsp)
    {
      return V_ERROR;
    }

  if (scan_attrsp->attr_cache && scan_predp->regu_list)
    {
      /* read the predicate values from the heap into the attribute cache */
      if (heap_attrinfo_read_dbvalues (thread_p, oid, recdesp, scan_attrsp->attr_cache) != NO_ERROR)
        {
          return V_ERROR;
        }
    }

  /* evaluate the predicates of the data filter */
  ev_res = V_TRUE;
  if (scan_predp->pr_eval_fnc && scan_predp->pred_expr)
    {
      ev_res = (*scan_predp->pr_eval_fnc) (thread_p, scan_predp->pred_expr, filterp->val_descr, oid);
    }

  if (ev_res == V_TRUE && scan_predp->regu_list && filterp->val_list)
    {
      /*
       * fetch the values for the regu variable list of the data filter
       * from the cached attribute information
       */
      if (fetch_val_list (thread_p, scan_predp->regu_list, filterp->val_descr, oid, NULL, NULL, PEEK) != NO_ERROR)
        {
          return V_ERROR;
        }
    }

  return ev_res;
}

/*
 * eval_key_filter () -
 *   return: DB_LOGICAL (V_TRUE, V_FALSE, V_UNKNOWN or V_ERROR)
 * 	 key(in): pointer to DB_VALUE (key value)
 *   filterp(in): pointer to FILTER_INFO (filter information)
 *
 * Note: evaluate key filter(predicates) given as FILTER_INFO
 */
DB_LOGICAL
eval_key_filter (THREAD_ENTRY * thread_p, const DB_IDXKEY * key, FILTER_INFO * filterp)
{
  OR_INDEX *indexp = NULL;
  int i, j;
  SCAN_PRED *scan_predp;
  SCAN_ATTRS *scan_attrsp;
  DB_LOGICAL ev_res;
  HEAP_ATTRVALUE *attrvalue;
  DB_VALUE *valp;

  if (key == NULL)
    {
      assert (false);
      return V_ERROR;
    }

  if (filterp == NULL)
    {
      assert (false);
      return V_TRUE;
    }

  scan_predp = filterp->scan_pred;
  scan_attrsp = filterp->scan_attrs;
  if (scan_predp == NULL || scan_attrsp == NULL)
    {
      assert (false);
      return V_ERROR;
    }

  indexp = &(filterp->classrepr->indexes[filterp->indx_id]);
  assert (key->size == indexp->n_atts + 1);

  ev_res = V_TRUE;

  if (scan_predp->pr_eval_fnc && scan_predp->pred_expr)
    {
      if (filterp->classrepr == NULL || filterp->indx_id < 0)
        {
          assert (false);
          return V_ERROR;
        }

      /* for all attributes specified in the filter */
      for (i = 0; i < scan_attrsp->num_attrs; i++)
        {
          /* for the attribute ID array of the index key */
          for (j = 0; j < indexp->n_atts; j++)
            {
              if (scan_attrsp->attr_ids[i] != indexp->atts[j]->id)
                {
                  continue;
                }

              /* now, find the attr */
              attrvalue = heap_attrvalue_locate (scan_attrsp->attr_ids[i], scan_attrsp->attr_cache);
              if (attrvalue == NULL)
                {
                  return V_ERROR;
                }

              valp = &(attrvalue->dbvalue);
              assert (valp->need_clear == false);

#if 1                           /* TODO - remove me */
              if (pr_clear_value (valp) != NO_ERROR)
                {
                  return V_ERROR;
                }
#endif

              /* peek j-th element value from the key */
              *valp = key->vals[j];
              valp->need_clear = false;

              attrvalue->state = HEAP_WRITTEN_ATTRVALUE;

              break;            /* immediately exit inner-loop */
            }

          if (j >= indexp->n_atts)
            {
              /*
               * the attribute exists in key filter scan cache, but it is
               * not a member of attributes consisting index key
               */
              DB_VALUE null;

              assert (false);
              DB_MAKE_NULL (&null);
              if (heap_attrinfo_set (NULL, scan_attrsp->attr_ids[i], &null, scan_attrsp->attr_cache) != NO_ERROR)
                {
                  return V_ERROR;
                }
            }
        }

      /*
       * evaluate the predicates of the key filter
       * using the given key value
       */
      ev_res = (*scan_predp->pr_eval_fnc) (thread_p, scan_predp->pred_expr, filterp->val_descr, NULL /* obj_oid */ );
    }

  return ev_res;
}

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
 * type_checking.c - auxiliary functions for parse tree translation
 */

#ident "$Id$"

#include "config.h"

#include <assert.h>
#include <stdarg.h>
#include <ctype.h>
#include <float.h>
#include <math.h>
#include <limits.h>

#include <sys/time.h>

#include "error_manager.h"
#include "parser.h"
#include "parser_message.h"
#include "set_object.h"
#include "arithmetic.h"
#include "string_opfunc.h"
#include "object_domain.h"
#include "semantic_check.h"
#include "xasl_generation.h"
#include "language_support.h"
#include "schema_manager.h"
#include "system_parameter.h"
#include "network_interface_cl.h"
#include "object_template.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define PT_ARE_COMPARABLE_NUMERIC_TYPE(typ1, typ2)			      \
   ((PT_IS_NUMERIC_TYPE (typ1) && PT_IS_NUMERIC_TYPE (typ2)) ||		      \
    (PT_IS_NUMERIC_TYPE (typ1) && typ2 == PT_TYPE_MAYBE) ||		      \
    (typ1 == PT_TYPE_MAYBE && PT_IS_NUMERIC_TYPE (typ2)))

/* Two types are comparable if they are NUMBER types or same CHAR type */
#define PT_ARE_COMPARABLE(typ1, typ2)					      \
  ((typ1 == typ2) || PT_ARE_COMPARABLE_NUMERIC_TYPE (typ1, typ2))

typedef struct compare_between_operator
{
  PT_OP_TYPE left;
  PT_OP_TYPE right;
  PT_OP_TYPE between;
} COMPARE_BETWEEN_OPERATOR;

static COMPARE_BETWEEN_OPERATOR pt_Compare_between_operator_table[] = {
  {PT_GE, PT_LE, PT_BETWEEN_GE_LE},
  {PT_GE, PT_LT, PT_BETWEEN_GE_LT},
  {PT_GT, PT_LE, PT_BETWEEN_GT_LE},
  {PT_GT, PT_LT, PT_BETWEEN_GT_LT},
  {PT_EQ, PT_EQ, PT_BETWEEN_EQ_NA},
  {PT_GT_INF, PT_LE, PT_BETWEEN_INF_LE},
  {PT_GT_INF, PT_EQ, PT_BETWEEN_INF_LE},
  {PT_GT_INF, PT_LT, PT_BETWEEN_INF_LT},
  {PT_GE, PT_LT_INF, PT_BETWEEN_GE_INF},
  {PT_EQ, PT_LT_INF, PT_BETWEEN_GE_INF},
  {PT_GT, PT_LT_INF, PT_BETWEEN_GT_INF}
};

#define COMPARE_BETWEEN_OPERATOR_COUNT \
        sizeof(pt_Compare_between_operator_table) / \
        sizeof(COMPARE_BETWEEN_OPERATOR)

static PT_NODE *pt_eval_range_expr_arguments (PARSER_CONTEXT * parser,
					      PT_NODE * expr);
static bool pt_is_range_expression (const PT_OP_TYPE op);
static PT_NODE *pt_eval_type_pre (PARSER_CONTEXT * parser, PT_NODE * node,
				  void *arg, int *continue_walk);
static PT_NODE *pt_eval_type (PARSER_CONTEXT * parser, PT_NODE * node,
			      void *arg, int *continue_walk);
static void pt_chop_to_one_select_item (PARSER_CONTEXT * parser,
					PT_NODE * node);
static PT_NODE *pt_eval_expr_type (PARSER_CONTEXT * parser, PT_NODE * node);
static PT_NODE *pt_eval_opt_type (PARSER_CONTEXT * parser, PT_NODE * node);
static PT_NODE *pt_eval_function_type (PARSER_CONTEXT * parser,
				       PT_NODE * node);
static const char *pt_class_name (const PT_NODE * type);
#if defined (ENABLE_UNUSED_FUNCTION)
static int pt_set_default_data_type (PARSER_CONTEXT * parser,
				     PT_TYPE_ENUM type, PT_NODE ** dtp);
#endif
static int pt_character_length_for_node (PT_NODE * node,
					 const PT_TYPE_ENUM coerce_type);

/*
 * pt_eval_range_expr_arguments -
 *  return	: expr or NULL on error
 *  parser(in)	: the parser context
 *  expr(in)	: the SQL expression
 *
 */
static PT_NODE *
pt_eval_range_expr_arguments (PARSER_CONTEXT * parser, PT_NODE * expr)
{
  PT_NODE *arg2;

  assert (expr != NULL);

  arg2 = expr->info.expr.arg2;
  if (arg2 == NULL)
    {
      assert (false);
      return NULL;
    }

  /* for range expressions the second argument may be a collection or a
     query. */
  if (PT_IS_QUERY_NODE_TYPE (arg2->node_type))
    {
      /* the select list must have only one element and the first argument
         has to be of the same type as the argument from the select list */
      PT_NODE *arg2_list = NULL;

      expr->info.expr.arg2->info.query.all_distinct = PT_DISTINCT;

      arg2_list = pt_get_select_list (parser, arg2);
      if (PT_IS_COLLECTION_TYPE (arg2_list->type_enum)
	  && arg2_list->node_type == PT_FUNCTION)
	{
	  expr->type_enum = PT_TYPE_LOGICAL;
	  return expr;
	}
      if (pt_length_of_select_list (arg2_list, EXCLUDE_HIDDEN_COLUMNS) != 1)
	{
	  PT_ERRORm (parser, arg2, MSGCAT_SET_PARSER_SEMANTIC,
		     MSGCAT_SEMANTIC_NOT_SINGLE_COL);
	  return NULL;
	}

    }
  else if (PT_IS_COLLECTION_TYPE (arg2->type_enum))
    {
      ;				/* do nothing */
    }
  else if (PT_IS_HOSTVAR (arg2))
    {
      ;				/* do nothing */
    }
  else
    {
      /* This is a semantic error */
      return NULL;
    }

  return expr;
}

/*
 * pt_is_range_expression () - return true if the expression is evaluated
 *				 as a logical expression
 *  return  : true if the expression is of type logical
 *  op (in) : the expression
 */
static bool
pt_is_range_expression (const PT_OP_TYPE op)
{
  switch (op)
    {
    case PT_IS_IN:
    case PT_IS_NOT_IN:
      return true;
    default:
      return false;
    }
  return false;
}

/*
 * pt_is_symmetric_op () -
 *   return:
 *   op(in):
 */
bool
pt_is_symmetric_op (const PT_OP_TYPE op)
{
  switch (op)
    {
    case PT_FUNCTION_HOLDER:
    case PT_ASSIGN:
    case PT_IS_IN:
    case PT_IS_NOT_IN:
    case PT_IS_NULL:
    case PT_IS_NOT_NULL:
    case PT_POSITION:
    case PT_FINDINSET:
    case PT_SUBSTRING:
    case PT_SUBSTRING_INDEX:
    case PT_OCTET_LENGTH:
    case PT_BIT_LENGTH:
    case PT_CHAR_LENGTH:
    case PT_BIN:
    case PT_TRIM:
    case PT_LTRIM:
    case PT_RTRIM:
    case PT_LIKE_LOWER_BOUND:
    case PT_LIKE_UPPER_BOUND:
    case PT_LPAD:
    case PT_RPAD:
    case PT_REPEAT:
    case PT_REPLACE:
    case PT_TRANSLATE:
    case PT_LAST_DAY:
    case PT_MONTHS_BETWEEN:
    case PT_SYS_DATE:
    case PT_SYS_TIME:
    case PT_SYS_DATETIME:
    case PT_UTC_TIME:
    case PT_UTC_DATE:
    case PT_TO_CHAR:
    case PT_TO_DATE:
    case PT_TO_TIME:
    case PT_TO_DATETIME:
    case PT_TO_NUMBER:
    case PT_CAST:
    case PT_EXTRACT:
    case PT_INST_NUM:
    case PT_ROWNUM:
    case PT_ORDERBY_NUM:
    case PT_CURRENT_USER:
    case PT_HA_STATUS:
    case PT_SHARD_GROUPID:
    case PT_SHARD_LOCKNAME:
    case PT_SHARD_NODEID:
    case PT_EXPLAIN:
    case PT_CHR:
    case PT_ROUND:
    case PT_TRUNC:
    case PT_INSTR:
    case PT_TIME_FORMAT:
    case PT_TIMEF:
    case PT_YEARF:
    case PT_MONTHF:
    case PT_DAYF:
    case PT_DAYOFMONTH:
    case PT_HOURF:
    case PT_MINUTEF:
    case PT_SECONDF:
    case PT_QUARTERF:
    case PT_WEEKDAY:
    case PT_DAYOFWEEK:
    case PT_DAYOFYEAR:
    case PT_TODAYS:
    case PT_FROMDAYS:
    case PT_TIMETOSEC:
    case PT_SECTOTIME:
    case PT_WEEKF:
    case PT_MAKETIME:
    case PT_MAKEDATE:
    case PT_SCHEMA:
    case PT_DATABASE:
    case PT_VERSION:
    case PT_UNIX_TIMESTAMP:
    case PT_FROM_UNIXTIME:
    case PT_IS:
    case PT_IS_NOT:
    case PT_CONCAT:
    case PT_CONCAT_WS:
    case PT_FIELD:
    case PT_LEFT:
    case PT_RIGHT:
    case PT_LOCATE:
    case PT_MID:
    case PT_REVERSE:
    case PT_ADDDATE:
    case PT_DATE_ADD:
    case PT_SUBDATE:
    case PT_DATE_SUB:
    case PT_FORMAT:
    case PT_ATAN2:
    case PT_DATE_FORMAT:
    case PT_USER:
    case PT_STR_TO_DATE:
    case PT_LIST_DBS:
    case PT_IF:
    case PT_POWER:
    case PT_TYPEOF:
    case PT_INDEX_CARDINALITY:
    case PT_RAND:
    case PT_RANDOM:
    case PT_DRAND:
    case PT_DRANDOM:
    case PT_PI:
    case PT_ABS:
    case PT_BETWEEN_EQ_NA:
    case PT_BETWEEN_GE_INF:
    case PT_BETWEEN_GT_INF:
    case PT_BETWEEN_INF_LE:
    case PT_BETWEEN_INF_LT:
    case PT_LT_INF:
    case PT_GT_INF:
    case PT_CASE:
    case PT_DECODE:
    case PT_LIKE_ESCAPE:
    case PT_RLIKE:
    case PT_NOT_RLIKE:
    case PT_RLIKE_BINARY:
    case PT_NOT_RLIKE_BINARY:
    case PT_CONV:
    case PT_IFNULL:
    case PT_NVL:
    case PT_NVL2:
    case PT_COALESCE:
    case PT_WIDTH_BUCKET:
    case PT_TRACE_STATS:
    case PT_INDEX_PREFIX:
    case PT_SHA_ONE:
    case PT_SHA_TWO:
      return false;

    default:
      return true;
    }
}

/*
 * pt_eval_type_pre () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
pt_eval_type_pre (PARSER_CONTEXT * parser, PT_NODE * node,
		  UNUSED_ARG void *arg, int *continue_walk)
{
  PT_NODE *arg1, *arg2;
  PT_NODE *derived_table;

  /* To ensure that after exit of recursive expression node the evaluation of
     type will continue in normal mode */
  *continue_walk = PT_CONTINUE_WALK;

  switch (node->node_type)
    {
    case PT_SPEC:
      derived_table = node->info.spec.derived_table;
      if (pt_is_query (derived_table))
	{
	  /* exclude outer join spec from folding */
	  derived_table->info.query.has_outer_spec =
	    (node->info.spec.join_type == PT_JOIN_LEFT_OUTER
	     || node->info.spec.join_type == PT_JOIN_RIGHT_OUTER
	     || (node->next
		 && (node->next->info.spec.join_type == PT_JOIN_LEFT_OUTER
		     || (node->next->info.spec.join_type ==
			 PT_JOIN_RIGHT_OUTER)))) ? 1 : 0;
	}
      break;

    case PT_SORT_SPEC:
      /* if sort spec expression is query, mark it as such */
      if (node->info.sort_spec.expr
	  && PT_IS_QUERY (node->info.sort_spec.expr))
	{
	  node->info.sort_spec.expr->info.query.is_sort_spec = 1;
	}
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      /* propagate to children */
      arg1 = node->info.query.q.union_.arg1;
      arg2 = node->info.query.q.union_.arg2;
      arg1->info.query.has_outer_spec = node->info.query.has_outer_spec;
      arg2->info.query.has_outer_spec = node->info.query.has_outer_spec;

      /* rewrite limit clause as numbering expression and add it
       * to the corresponding predicate
       */
      if (node->info.query.limit && node->info.query.rewrite_limit)
	{
	  PT_NODE *limit, *t_node;
	  PT_NODE **expr_pred;

	  /* If both ORDER BY clause and LIMIT clause are specified,
	   * we will rewrite LIMIT to ORDERBY_NUM().
	   * For example,
	   *   (SELECT ...) UNION (SELECT ...) ORDER BY a LIMIT 10
	   *   will be rewritten to:
	   *   (SELECT ...) UNION (SELECT ...) ORDER BY a FOR ORDERBY_NUM() <= 10
	   *
	   * If LIMIT clause is only specified, we will rewrite the query
	   * at query optimization step. See qo_rewrite_queries() function
	   * for more information.
	   */
	  if (node->info.query.order_by != NULL)
	    {
	      expr_pred = &node->info.query.orderby_for;
	      limit = pt_limit_to_numbering_expr (parser,
						  node->info.query.limit,
						  PT_ORDERBY_NUM, false);
	      if (limit != NULL)
		{
		  t_node = *expr_pred;
		  while (t_node && t_node->next)
		    {
		      t_node = t_node->next;
		    }
		  if (!t_node)
		    {
		      t_node = *expr_pred = limit;
		    }
		  else
		    {
		      t_node->next = limit;
		    }

		  node->info.query.rewrite_limit = 0;
		}
	      else
		{
		  PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			     MSGCAT_SEMANTIC_OUT_OF_MEMORY);
		}
	    }
	}
      break;

    case PT_SELECT:
      /* rewrite limit clause as numbering expression and add it
       * to the corresponding predicate
       */
      if (node->info.query.limit && node->info.query.rewrite_limit)
	{
	  PT_NODE *limit, *t_node;
	  PT_NODE **expr_pred;

	  if (node->info.query.order_by)
	    {
	      expr_pred = &node->info.query.orderby_for;
	      limit = pt_limit_to_numbering_expr (parser,
						  node->info.query.limit,
						  PT_ORDERBY_NUM, false);
	    }
	  else if (node->info.query.q.select.group_by)
	    {
	      expr_pred = &node->info.query.q.select.having;
	      limit = pt_limit_to_numbering_expr (parser,
						  node->info.query.limit, 0,
						  true);
	    }
	  else if (node->info.query.all_distinct == PT_DISTINCT)
	    {
	      /* When a distinct query has neither orderby nor groupby clause,
	       * limit must be orderby_num predicate.
	       */
	      expr_pred = &node->info.query.orderby_for;
	      limit = pt_limit_to_numbering_expr (parser,
						  node->info.query.limit,
						  PT_ORDERBY_NUM, false);
	    }
	  else
	    {
	      expr_pred = &node->info.query.q.select.where;
	      limit = pt_limit_to_numbering_expr (parser,
						  node->info.query.limit,
						  PT_INST_NUM, false);
	    }

	  if (limit)
	    {
	      t_node = *expr_pred;
	      while (t_node && t_node->next)
		{
		  t_node = t_node->next;
		}
	      if (!t_node)
		{
		  t_node = *expr_pred = limit;
		}
	      else
		{
		  t_node->next = limit;
		}

	      node->info.query.rewrite_limit = 0;
	    }
	  else
	    {
	      PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_OUT_OF_MEMORY);
	    }
	}
      break;

    case PT_INSERT:
      /* mark inserted sub-query as belonging to insert statement */
      if (node->info.insert.value_clauses->info.node_list.list_type ==
	  PT_IS_SUBQUERY)
	{
	  node->info.insert.value_clauses->info.node_list.list->info.query.
	    is_insert_select = 1;
	}
      break;

    case PT_DELETE:
      /* rewrite limit clause as numbering expression and add it
       * to search condition
       */
      if (node->info.delete_.limit && node->info.delete_.rewrite_limit)
	{
	  PT_NODE *t_node = node->info.delete_.search_cond;
	  PT_NODE *limit =
	    pt_limit_to_numbering_expr (parser, node->info.delete_.limit,
					PT_INST_NUM, false);
	  if (limit)
	    {
	      while (t_node && t_node->next)
		{
		  t_node = t_node->next;
		}
	      if (!t_node)
		{
		  node->info.delete_.search_cond = limit;
		}
	      else
		{
		  t_node->next = limit;
		}

	      node->info.delete_.rewrite_limit = 0;
	    }
	  else
	    {
	      PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_OUT_OF_MEMORY);
	    }
	}
      break;

    case PT_UPDATE:
      /* rewrite limit clause as numbering expression and add it
       * to search condition
       */
      if (node->info.update.limit && node->info.update.rewrite_limit)
	{
	  PT_NODE **expr_pred = NULL;
	  PT_NODE *limit = NULL;

	  if (node->info.update.order_by)
	    {
	      expr_pred = &(node->info.update.orderby_for);
	      limit = pt_limit_to_numbering_expr (parser,
						  node->info.update.limit,
						  PT_ORDERBY_NUM, false);
	    }
	  else
	    {
	      expr_pred = &(node->info.update.search_cond);
	      limit = pt_limit_to_numbering_expr (parser,
						  node->info.update.limit,
						  PT_INST_NUM, false);
	    }
	  if (limit)
	    {
	      *expr_pred = parser_append_node (limit, *expr_pred);
	      node->info.update.rewrite_limit = 0;
	    }
	  else
	    {
	      PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_OUT_OF_MEMORY);
	    }
	}
      break;

    default:
      break;
    }

  return node;
}

/*
 * pt_eval_type () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
pt_eval_type (PARSER_CONTEXT * parser, PT_NODE * node,
	      UNUSED_ARG void *arg, UNUSED_ARG int *continue_walk)
{
  PT_NODE *dt = NULL, *arg1 = NULL, *arg2 = NULL;

  switch (node->node_type)
    {
    case PT_EXPR:
      node = pt_eval_expr_type (parser, node);
      if (node == NULL)
	{
	  assert (false);
	  PT_INTERNAL_ERROR (parser, "pt_eval_type");
	  return NULL;
	}
      break;

    case PT_FUNCTION:
      node = pt_eval_function_type (parser, node);
      if (node == NULL)
	{
	  assert (false);
	  PT_INTERNAL_ERROR (parser, "pt_eval_type");
	  return NULL;
	}
      break;

    case PT_DELETE:
      break;

    case PT_UPDATE:
      break;

    case PT_SELECT:
      if (node->info.query.q.select.list)
	{
	  node->type_enum = node->info.query.q.select.list->type_enum;
	  dt = node->info.query.q.select.list->data_type;

	  if (dt)
	    {
	      if (node->data_type)
		{
		  parser_free_tree (parser, node->data_type);
		  node->data_type = NULL;
		}

	      node->data_type = parser_copy_tree_list (parser, dt);
	    }
	}
      break;

    case PT_INSERT:
      if (node->info.insert.spec)
	{
	  node->type_enum = PT_TYPE_OBJECT;
	  dt = parser_new_node (parser, PT_DATA_TYPE);
	  if (dt)
	    {
	      dt->type_enum = PT_TYPE_OBJECT;
	      node->data_type = dt;
	      dt->info.data_type.entity =
		parser_copy_tree (parser,
				  node->info.insert.spec->info.spec.
				  flat_entity_list);
	    }
	}
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      {
	PT_NODE *attrs1, *attrs2;
	int cnt1, cnt2;

	arg1 = node->info.query.q.union_.arg1;
	arg2 = node->info.query.q.union_.arg2;

	attrs1 = pt_get_select_list (parser, arg1);
	attrs2 = pt_get_select_list (parser, arg2);

	cnt1 = pt_length_of_select_list (attrs1, EXCLUDE_HIDDEN_COLUMNS);
	cnt2 = pt_length_of_select_list (attrs2, EXCLUDE_HIDDEN_COLUMNS);

	if (attrs1 == NULL || attrs2 == NULL || cnt1 != cnt2)
	  {
	    PT_ERRORmf2 (parser, attrs1, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_ARITY_MISMATCH, cnt1, cnt2);

	    break;
	  }

	node->type_enum = node->info.query.q.union_.arg1->type_enum;
	dt = node->info.query.q.union_.arg1->data_type;
	if (dt)
	  {
	    node->data_type = parser_copy_tree_list (parser, dt);
	  }
      }
      break;

    case PT_VALUE:
    case PT_NAME:
    case PT_DOT_:
      /* these cases have types already assigned to them by
         parser and semantic name resolution. */
      break;

    case PT_HOST_VAR:
      if (node->type_enum == PT_TYPE_NONE
	  && node->info.host_var.var_type == PT_HOST_IN)
	{
	  /* type is not known yet (i.e, compile before bind a value) */
	  node->type_enum = PT_TYPE_MAYBE;
	}
      break;

    case PT_SET_OPT_LVL:
    case PT_GET_OPT_LVL:
      node = pt_eval_opt_type (parser, node);
      break;

    default:
      break;
    }

  return node;
}

/*
 * pt_chop_to_one_select_item () -
 *   return: none
 *   parser(in):
 *   node(in/out): an EXISTS subquery
 */
static void
pt_chop_to_one_select_item (PARSER_CONTEXT * parser, PT_NODE * node)
{
  if (pt_is_query (node))
    {
      if (node->node_type == PT_SELECT)
	{
	  /* chop to one select item */
	  if (node->info.query.q.select.list
	      && node->info.query.q.select.list->next)
	    {
	      parser_free_tree (parser, node->info.query.q.select.list->next);
	      node->info.query.q.select.list->next = NULL;
	    }
	}
      else
	{
	  pt_chop_to_one_select_item (parser, node->info.query.q.union_.arg1);
	  pt_chop_to_one_select_item (parser, node->info.query.q.union_.arg2);
	}

      /* remove unneeded order by */
      if (node->info.query.order_by)
	{
	  parser_free_tree (parser, node->info.query.order_by);
	  node->info.query.order_by = NULL;
	}
    }
}

/*
 * pt_eval_expr_type () -
 *   return:
 *   parser(in):
 *   node(in):
 */
static PT_NODE *
pt_eval_expr_type (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_OP_TYPE op;
  PT_NODE *arg1 = NULL, *arg2 = NULL;

  /* by the time we get here, the leaves have already been typed.
   * this is because this function is called from a post function
   *  of a parser_walk_tree, after all leaves have been visited.
   */

  op = node->info.expr.op;

  switch (op)
    {
    case PT_BETWEEN_AND:
    case PT_BETWEEN_GE_LE:
    case PT_BETWEEN_GE_LT:
    case PT_BETWEEN_GT_LE:
    case PT_BETWEEN_GT_LT:
      /* these expressions will be handled by PT_BETWEEN */
      goto exit_on_end;
      break;

    case PT_IS_IN:
    case PT_IS_NOT_IN:
      /* keep out set-type lhs */
      arg1 = node->info.expr.arg1;
      if (PT_IS_COLLECTION_TYPE (arg1->type_enum))
	{
	  PT_ERRORmf2 (parser, arg1, MSGCAT_SET_PARSER_SEMANTIC,
		       MSGCAT_SEMANTIC_OP_NOT_DEFINED_ON_1,
		       pt_show_binopcode (op),
		       pt_show_type_enum (arg1->type_enum));
	  node->type_enum = PT_TYPE_NONE;
	  goto exit_on_end;
	}

      arg2 = node->info.expr.arg2;
      if (pt_is_query (arg2))
	{
	  /* keep out set-type rhs suquery */
	  if (PT_IS_COLLECTION_TYPE (arg2->type_enum))
	    {
	      PT_ERRORmf2 (parser, arg2, MSGCAT_SET_PARSER_SEMANTIC,
			   MSGCAT_SEMANTIC_OP_NOT_DEFINED_ON_1,
			   pt_show_binopcode (op),
			   pt_show_type_enum (arg2->type_enum));
	      node->type_enum = PT_TYPE_NONE;
	      goto exit_on_end;
	    }
	}
      else if (arg2->node_type == PT_VALUE)
	{
	  PT_NODE *elem;

	  assert (PT_IS_COLLECTION_TYPE (arg2->type_enum));

	  /* keep out set-type rhs value */
	  for (elem = arg2->info.value.data_value.set; elem != NULL;
	       elem = elem->next)
	    {
	      if (PT_IS_COLLECTION_TYPE (elem->type_enum))
		{
		  PT_ERRORmf2 (parser, elem,
			       MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_OP_NOT_DEFINED_ON_1,
			       pt_show_binopcode (op),
			       pt_show_type_enum (elem->type_enum));
		  node->type_enum = PT_TYPE_NONE;
		  goto exit_on_end;
		}
	    }

	  if (arg2->info.value.data_value.set != NULL
	      && arg2->info.value.data_value.set->next == NULL)
	    {
	      /* only one element in set. convert expr as EQ/NE expr. */
	      PT_NODE *new_arg2;

	      new_arg2 = arg2->info.value.data_value.set;

	      /* free arg2 */
	      arg2->info.value.data_value.set = NULL;
	      parser_free_tree (parser, node->info.expr.arg2);

	      /* rewrite arg2 */
	      node->info.expr.arg2 = new_arg2;
	      node->info.expr.op = (op == PT_IS_IN) ? PT_EQ : PT_NE;
	    }
	}
      else if (arg2->node_type == PT_FUNCTION)
	{
	  PT_NODE *farg;

	  assert (PT_IS_COLLECTION_TYPE (arg2->type_enum));
	  assert (arg2->info.function.function_type == F_SEQUENCE);

	  /* keep out set-type rhs function */
	  for (farg = arg2->info.function.arg_list; farg != NULL;
	       farg = farg->next)
	    {
	      if (PT_IS_COLLECTION_TYPE (farg->type_enum))
		{
		  PT_ERRORmf2 (parser, farg, MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_OP_NOT_DEFINED_ON_1,
			       pt_show_binopcode (op),
			       pt_show_type_enum (farg->type_enum));
		  node->type_enum = PT_TYPE_NONE;
		  goto exit_on_end;
		}
	    }
	}
      else
	{
	  assert (false);	/* is impossible */
	  PT_ERRORmf2 (parser, arg2, MSGCAT_SET_PARSER_SEMANTIC,
		       MSGCAT_SEMANTIC_OP_NOT_DEFINED_ON_1,
		       pt_show_binopcode (op),
		       pt_show_type_enum (arg2->type_enum));
	  node->type_enum = PT_TYPE_NONE;
	  goto exit_on_end;
	}
      break;

#if 1				/* TODO - */
    case PT_RAND:
    case PT_RANDOM:
      /* to keep mysql compatibility we should consider a NULL argument
         as the value 0. This is the only place where we can perform
         this check */
      arg1 = node->info.expr.arg1;
      if (PT_IS_NULL_NODE (arg1))
	{
	  arg1->type_enum = PT_TYPE_INTEGER;
	  db_make_int (&arg1->info.value.db_value, 0);
	}
      break;

    case PT_DRAND:
    case PT_DRANDOM:
      /* to keep mysql compatibility we should consider a NULL argument
         as the value 0. This is the only place where we can perform
         this check */
      arg1 = node->info.expr.arg1;
      if (PT_IS_NULL_NODE (arg1))
	{
	  arg1->type_enum = PT_TYPE_DOUBLE;
	  db_make_double (&arg1->info.value.db_value, 0);
	}
      break;
#endif

    case PT_EXISTS:
      arg1 = node->info.expr.arg1;
      if (pt_is_query (arg1))
	{
	  pt_chop_to_one_select_item (parser, arg1);
	}
      else
	{
	  assert (false);	/* is impossible */
	  node->type_enum = PT_TYPE_NONE;
	  goto exit_on_end;
	}
      break;

    default:
      break;
    }

  if (pt_is_range_expression (op))
    {
      if (pt_eval_range_expr_arguments (parser, node) == NULL)
	{
	  node->type_enum = PT_TYPE_NONE;
	  goto exit_on_end;
	}
    }

exit_on_end:

  if (node->type_enum == PT_TYPE_NONE)
    {
      if (pt_is_operator_logical (op))
	{
	  node->type_enum = PT_TYPE_LOGICAL;
	}
      else
	{
	  node->type_enum = PT_TYPE_MAYBE;
	}
    }

  assert (node->type_enum != PT_TYPE_NONE);

  return node;
}

/*
 * pt_eval_opt_type () -
 *   return:
 *   parser(in):
 *   node(in/out):
 */
static PT_NODE *
pt_eval_opt_type (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_MISC_TYPE option;
  PT_NODE *arg1, *arg2;

  switch (node->node_type)
    {
    case PT_GET_OPT_LVL:
      option = node->info.get_opt_lvl.option;
      if (option == PT_OPT_COST)
	{
	  arg1 = node->info.get_opt_lvl.args;
	  if (PT_IS_CHAR_STRING_TYPE (arg1->type_enum))
	    {
	      node->type_enum = PT_TYPE_VARCHAR;
	    }
	  else
	    {
	      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_WANT_TYPE,
			  pt_show_type_enum (PT_TYPE_VARCHAR));
	      node->type_enum = PT_TYPE_NONE;
	      node = NULL;
	    }
	}
      else
	{
	  node->type_enum = PT_TYPE_INTEGER;
	}
      break;

    case PT_SET_OPT_LVL:
      node->type_enum = PT_TYPE_NONE;
      option = node->info.set_opt_lvl.option;
      arg1 = node->info.set_opt_lvl.val;

      switch (option)
	{
	case PT_OPT_LVL:
	  if (arg1->type_enum != PT_TYPE_INTEGER)
	    {
	      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_WANT_TYPE,
			  pt_show_type_enum (PT_TYPE_INTEGER));
	      node = NULL;
	    }
	  break;

	case PT_OPT_COST:
	  arg2 = arg1->next;
	  if (!PT_IS_CHAR_STRING_TYPE (arg1->type_enum)
	      || !PT_IS_CHAR_STRING_TYPE (arg2->type_enum))
	    {
	      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_WANT_TYPE,
			  pt_show_type_enum (PT_TYPE_VARCHAR));
	      node = NULL;
	    }
	  break;

	default:
	  break;
	}
      break;

    default:
      break;
    }

  return node;
}

/* pt_character_length_for_node() -
    return: number of characters that a value of the given type can possibly
	    occuppy when cast to a CHAR type.
    node(in): node with type whose character length is to be returned.
    coerce_type(in): string type that node will be cast to
*/
static int
pt_character_length_for_node (PT_NODE * node, const PT_TYPE_ENUM coerce_type)
{
  int precision = TP_FLOATING_PRECISION_VALUE;

  switch (node->type_enum)
    {
    case PT_TYPE_DOUBLE:
      precision = TP_DOUBLE_AS_CHAR_LENGTH;
      break;
    case PT_TYPE_BIGINT:
      precision = TP_BIGINT_AS_CHAR_LENGTH;
      break;
    case PT_TYPE_INTEGER:
      precision = TP_INTEGER_AS_CHAR_LENGTH;
      break;
    case PT_TYPE_TIME:
      precision = TP_TIME_AS_CHAR_LENGTH;
      break;
    case PT_TYPE_DATE:
      precision = TP_DATE_AS_CHAR_LENGTH;
      break;
    case PT_TYPE_DATETIME:
      precision = TP_DATETIME_AS_CHAR_LENGTH;
      break;
    case PT_TYPE_NUMERIC:
      precision = node->data_type->info.data_type.precision;
      if (precision == 0 || precision == DB_DEFAULT_PRECISION)
	{
	  precision = DB_DEFAULT_NUMERIC_PRECISION;
	}
      precision++;		/* for sign */

      if (node->data_type->info.data_type.dec_scale
	  &&
	  (node->data_type->info.data_type.dec_scale != DB_DEFAULT_SCALE
	   || node->data_type->info.data_type.dec_scale
	   != DB_DEFAULT_NUMERIC_SCALE))
	{
	  precision++;		/* for decimal point */
	}
      break;
    case PT_TYPE_VARCHAR:
      precision = node->data_type->info.data_type.precision;
      if (precision == DB_DEFAULT_PRECISION)
	{
	  precision = DB_MAX_VARCHAR_PRECISION;
	}
      break;
    case PT_TYPE_NULL:
      precision = 0;
      break;

    default:
      /* for host vars */
      switch (coerce_type)
	{
	case PT_TYPE_VARCHAR:
	  precision = DB_MAX_VARCHAR_PRECISION;
	  break;
	default:
	  precision = TP_FLOATING_PRECISION_VALUE;
	  break;
	}
      break;
    }

  return precision;
}

/*
 * pt_eval_function_type () -
 *   return: returns a node of the same type.
 *   parser(in): parser global context info for reentrancy
 *   node(in): a parse tree node of type PT_FUNCTION denoting an
 *             an expression with aggregate functions.
 */

static PT_NODE *
pt_eval_function_type (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *arg_list;
  PT_TYPE_ENUM arg_type;
  FUNC_TYPE fcode;
  bool check_agg_single_arg = false;
  bool is_agg_function = false;
#if 0
  PT_NODE *arg = NULL;
#endif

  is_agg_function = pt_is_aggregate_function (parser, node);
  arg_list = node->info.function.arg_list;
  fcode = node->info.function.function_type;
  if (!arg_list && fcode != PT_COUNT_STAR && fcode != PT_GROUPBY_NUM)
    {
      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
		  MSGCAT_SEMANTIC_FUNCTION_NO_ARGS,
		  pt_short_print (parser, node));
      return node;
    }

  /*
   * Should only get one arg to function; set to 'false' if the function
   * accepts more than one.
   */
  check_agg_single_arg = true;
#if 1
  arg_type = (arg_list) ? arg_list->type_enum : PT_TYPE_NONE;
  if (PT_IS_NULL_NODE (arg_list))
    {
      arg_type = PT_TYPE_NULL;
    }
#endif

#if 1
  switch (fcode)
    {
    case PT_STDDEV:
    case PT_STDDEV_POP:
    case PT_STDDEV_SAMP:
    case PT_VARIANCE:
    case PT_VAR_POP:
    case PT_VAR_SAMP:
      break;

    case PT_AVG:
#if 1				/* TODO - */
      arg_type = PT_TYPE_DOUBLE;
#endif
      break;

    case PT_SUM:
#if 1				/* TODO - */
      if (arg_type == PT_TYPE_MAYBE)
	{
	  ;			/* go ahead */
	}
      else if (arg_list && PT_IS_COLLECTION_TYPE (arg_list->type_enum))
	{
	  ;			/* go ahead *//* TODO - for catalog access */
	}
      else if (!PT_IS_NUMERIC_TYPE (arg_type))
	{
	  arg_type = PT_TYPE_DOUBLE;
	}
#endif
      break;

    case PT_MAX:
    case PT_MIN:
    case PT_COUNT:
      break;

    case PT_GROUP_CONCAT:
      check_agg_single_arg = false;

      if (arg_list->next)
	{
	  PT_TYPE_ENUM sep_type;

	  sep_type = arg_list->next->type_enum;
	  if (!PT_IS_CHAR_STRING_TYPE (sep_type))
	    {
	      PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			   MSGCAT_SEMANTIC_INCOMPATIBLE_OPDS,
			   pt_show_function (PT_GROUP_CONCAT),
			   pt_show_type_enum (sep_type));
	      break;
	    }
	}
      break;

    case F_ELT:
    case F_INSERT_SUBSTRING:
      break;

    default:
      check_agg_single_arg = false;
      break;
    }
#else
  switch (fcode)
    {
    case PT_STDDEV:
    case PT_STDDEV_POP:
    case PT_STDDEV_SAMP:
    case PT_VARIANCE:
    case PT_VAR_POP:
    case PT_VAR_SAMP:
    case PT_AVG:
      arg_type = PT_TYPE_DOUBLE;
      break;

    case PT_SUM:
      if (arg_type == PT_TYPE_MAYBE)
	{
	  ;			/* go ahead */
	}
      else if (arg_list && PT_IS_COLLECTION_TYPE (arg_list->type_enum))
	{
	  ;			/* go ahead *//* TODO - for catalog access */
	}
      else if (!PT_IS_NUMERIC_TYPE (arg_type))
	{
	  arg_type = PT_TYPE_DOUBLE;
	}
      break;

    case PT_MAX:
    case PT_MIN:
      if (!PT_IS_NUMERIC_TYPE (arg_type)
	  && !PT_IS_STRING_TYPE (arg_type)
	  && !PT_IS_DATE_TIME_TYPE (arg_type)
	  && arg_type != PT_TYPE_MAYBE && arg_type != PT_TYPE_NULL)
	{
	  PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
		       MSGCAT_SEMANTIC_INCOMPATIBLE_OPDS,
		       pt_show_function (fcode),
		       pt_show_type_enum (arg_type));
	}
      break;

    case PT_COUNT:
      break;

    case PT_GROUP_CONCAT:
      {
	PT_TYPE_ENUM sep_type;
	sep_type = (arg_list->next) ? arg_list->next->type_enum :
	  PT_TYPE_NONE;
	check_agg_single_arg = false;

	if (!PT_IS_NUMERIC_TYPE (arg_type)
	    && !PT_IS_CHAR_STRING_TYPE (arg_type)
	    && !PT_IS_DATE_TIME_TYPE (arg_type)
	    && arg_type != PT_TYPE_MAYBE && arg_type != PT_TYPE_NULL)
	  {
	    PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_INCOMPATIBLE_OPDS,
			 pt_show_function (fcode),
			 pt_show_type_enum (arg_type));
	    break;
	  }

	if (!PT_IS_CHAR_STRING_TYPE (sep_type) && sep_type != PT_TYPE_NONE)
	  {
	    PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_INCOMPATIBLE_OPDS,
			 pt_show_function (fcode),
			 pt_show_type_enum (sep_type));
	    break;
	  }

	if ((arg_type == PT_TYPE_VARBIT)
	    && sep_type != PT_TYPE_VARBIT && sep_type != PT_TYPE_NONE)
	  {
	    PT_ERRORmf3 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_OP_NOT_DEFINED_ON,
			 pt_show_function (fcode),
			 pt_show_type_enum (arg_type),
			 pt_show_type_enum (sep_type));
	    break;
	  }

	if ((arg_type != PT_TYPE_VARBIT) && (sep_type == PT_TYPE_VARBIT))
	  {
	    PT_ERRORmf3 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_OP_NOT_DEFINED_ON,
			 pt_show_function (fcode),
			 pt_show_type_enum (arg_type),
			 pt_show_type_enum (sep_type));
	    break;
	  }
      }
      break;

    case F_ELT:
      {
	/* all types used in the arguments list */
	bool has_arg_type[PT_TYPE_MAX - PT_TYPE_MIN] = { false };

	/* a subset of argument types given to ELT that can not be cast to CHAR VARYING */
	PT_TYPE_ENUM bad_types[4] = {
	  PT_TYPE_NONE, PT_TYPE_NONE, PT_TYPE_NONE, PT_TYPE_NONE
	};

	PT_NODE *arg = arg_list;

	size_t i = 0;		/* used to index has_arg_type */
	size_t num_bad = 0;	/* used to index bad_types */

	memset (has_arg_type, 0, sizeof (has_arg_type));

	/* check the index argument (null, numeric or host var) */
	if (PT_IS_NUMERIC_TYPE (arg->type_enum) ||
	    PT_IS_CHAR_STRING_TYPE (arg->type_enum) ||
	    arg->type_enum == PT_TYPE_NONE ||
	    PT_IS_NULL_NODE (arg) || arg->type_enum == PT_TYPE_MAYBE)
	  {
	    arg = arg->next;
	  }
	else
	  {
	    arg_type = PT_TYPE_NONE;
	    PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON_INDEX,
			 pt_show_function (fcode),
			 pt_show_type_enum (arg->type_enum));
	    break;
	  }

	/* make a list of all other argument types (null, [N]CHAR [VARYING], or host var) */
	while (arg)
	  {
	    if (arg->type_enum < PT_TYPE_MAX)
	      {
		has_arg_type[arg->type_enum - PT_TYPE_MIN] = true;
		arg = arg->next;
	      }
	    else
	      {
		assert (false);	/* invalid data type */
		arg_type = PT_TYPE_NONE;
		PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			     MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON,
			     pt_show_function (fcode),
			     pt_show_type_enum (arg->type_enum));
		break;
	      }
	  }

	/* look for unsupported argument types in the list */
	while (i < (sizeof (has_arg_type) / sizeof (has_arg_type[0])))
	  {
	    if (has_arg_type[i])
	      {
		if (!(PT_IS_NUMERIC_TYPE (PT_TYPE_MIN + i) ||
		      PT_IS_CHAR_STRING_TYPE (PT_TYPE_MIN + i) ||
		      PT_IS_DATE_TIME_TYPE (PT_TYPE_MIN + i) ||
		      PT_TYPE_MIN + i == PT_TYPE_LOGICAL ||
		      PT_TYPE_MIN + i == PT_TYPE_NONE ||
		      PT_TYPE_MIN + i == PT_TYPE_NULL ||
		      PT_TYPE_MIN + i == PT_TYPE_MAYBE))
		  {
		    /* type is not NULL, unknown and is not known coercible
		       to CHAR VARYING */
		    size_t k = 0;

		    while (k < num_bad && bad_types[k] != PT_TYPE_MIN + i)
		      {
			k++;
		      }

		    if (k == num_bad)
		      {
			bad_types[num_bad++] = PT_TYPE_MIN + i;

			if (num_bad ==
			    sizeof (bad_types) / sizeof (bad_types[0]))
			  {
			    break;
			  }
		      }
		  }
	      }

	    i++;
	  }

	/* report any unsupported arguments */
	switch (num_bad)
	  {
	  case 1:
	    arg_type = PT_TYPE_NONE;
	    PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON,
			 pt_show_function (fcode),
			 pt_show_type_enum (bad_types[0]));
	    break;
	  case 2:
	    arg_type = PT_TYPE_NONE;
	    PT_ERRORmf3 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON_2,
			 pt_show_function (fcode),
			 pt_show_type_enum (bad_types[0]),
			 pt_show_type_enum (bad_types[1]));
	    break;
	  case 3:
	    arg_type = PT_TYPE_NONE;
	    PT_ERRORmf4 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON_3,
			 pt_show_function (fcode),
			 pt_show_type_enum (bad_types[0]),
			 pt_show_type_enum (bad_types[1]),
			 pt_show_type_enum (bad_types[2]));
	    break;
	  case 4:
	    arg_type = PT_TYPE_NONE;
	    PT_ERRORmf5 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON_4,
			 pt_show_function (fcode),
			 pt_show_type_enum (bad_types[0]),
			 pt_show_type_enum (bad_types[1]),
			 pt_show_type_enum (bad_types[2]),
			 pt_show_type_enum (bad_types[3]));
	    break;
	  }
      }
      break;

    case F_INSERT_SUBSTRING:
      {
	PT_TYPE_ENUM arg1_type = PT_TYPE_NONE, arg2_type = PT_TYPE_NONE,
	  arg3_type = PT_TYPE_NONE, arg4_type = PT_TYPE_NONE;
	PT_NODE *arg_array[NUM_F_INSERT_SUBSTRING_ARGS];
	int num_args = 0;
	/* arg_list to array */
	if (pt_node_list_to_array (parser, arg_list, arg_array,
				   NUM_F_INSERT_SUBSTRING_ARGS,
				   &num_args) != NO_ERROR)
	  {
	    break;
	  }
	if (num_args != NUM_F_INSERT_SUBSTRING_ARGS)
	  {
	    assert (false);
	    break;
	  }

	arg1_type = arg_array[0]->type_enum;
	arg2_type = arg_array[1]->type_enum;
	arg3_type = arg_array[2]->type_enum;
	arg4_type = arg_array[3]->type_enum;
	/* check arg2 and arg3 */
	if (!PT_IS_NUMERIC_TYPE (arg2_type)
	    && !PT_IS_CHAR_STRING_TYPE (arg2_type)
	    && arg2_type != PT_TYPE_MAYBE && arg2_type != PT_TYPE_NULL)
	  {
	    arg_type = PT_TYPE_NONE;
	    PT_ERRORmf5 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON_4,
			 pt_show_function (fcode),
			 pt_show_type_enum (arg1_type),
			 pt_show_type_enum (arg2_type),
			 pt_show_type_enum (arg3_type),
			 pt_show_type_enum (arg4_type));
	    break;
	  }

	if (!PT_IS_NUMERIC_TYPE (arg3_type)
	    && !PT_IS_CHAR_STRING_TYPE (arg3_type)
	    && arg3_type != PT_TYPE_MAYBE && arg3_type != PT_TYPE_NULL)
	  {
	    arg_type = PT_TYPE_NONE;
	    PT_ERRORmf5 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON_4,
			 pt_show_function (fcode),
			 pt_show_type_enum (arg1_type),
			 pt_show_type_enum (arg2_type),
			 pt_show_type_enum (arg3_type),
			 pt_show_type_enum (arg4_type));
	    break;
	  }

	/* check arg1 */
	if (!PT_IS_NUMERIC_TYPE (arg1_type)
	    && !PT_IS_CHAR_STRING_TYPE (arg1_type)
	    && !PT_IS_DATE_TIME_TYPE (arg1_type)
	    && arg1_type != PT_TYPE_MAYBE && arg1_type != PT_TYPE_NULL)
	  {
	    arg_type = PT_TYPE_NONE;
	    PT_ERRORmf5 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON_4,
			 pt_show_function (fcode),
			 pt_show_type_enum (arg1_type),
			 pt_show_type_enum (arg2_type),
			 pt_show_type_enum (arg3_type),
			 pt_show_type_enum (arg4_type));
	    break;
	  }
	/* check arg4 */
	if (!PT_IS_NUMERIC_TYPE (arg4_type)
	    && !PT_IS_CHAR_STRING_TYPE (arg4_type)
	    && !PT_IS_DATE_TIME_TYPE (arg4_type)
	    && arg4_type != PT_TYPE_MAYBE && arg4_type != PT_TYPE_NULL)
	  {
	    arg_type = PT_TYPE_NONE;
	    PT_ERRORmf5 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON_4,
			 pt_show_function (fcode),
			 pt_show_type_enum (arg1_type),
			 pt_show_type_enum (arg2_type),
			 pt_show_type_enum (arg3_type),
			 pt_show_type_enum (arg4_type));
	    break;
	  }
      }
      break;

    default:
      check_agg_single_arg = false;
      break;
    }
#endif

  if (is_agg_function && check_agg_single_arg)
    {
      if (arg_list->next)
	{
	  PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
		      MSGCAT_SEMANTIC_AGG_FUN_WANT_1_ARG,
		      pt_short_print (parser, node));
	}
    }

  if (node->type_enum == PT_TYPE_NONE)
    {
      /* determine function result type */
      switch (fcode)
	{
	case PT_COUNT:
	case PT_COUNT_STAR:
	case PT_GROUPBY_NUM:
	  node->type_enum = PT_TYPE_BIGINT;
	  assert (node->data_type == NULL);
	  break;

	case F_SEQUENCE:
#if 1				/* TODO - */
	  assert (false);	/* should not reach here */
#endif
	  node->type_enum = PT_TYPE_SEQUENCE;
	  pt_add_type_to_set (parser, arg_list, &node->data_type);
	  break;

	case PT_SUM:
	  node->type_enum = PT_TYPE_MAYBE;
	  assert (node->data_type == NULL);
	  break;

	case PT_AVG:
	case PT_STDDEV:
	case PT_STDDEV_POP:
	case PT_STDDEV_SAMP:
	case PT_VARIANCE:
	case PT_VAR_POP:
	case PT_VAR_SAMP:
	  node->type_enum = arg_type;
	  assert (node->data_type == NULL);
	  break;

	case PT_GROUP_CONCAT:
	  {
	    node->type_enum = PT_TYPE_VARCHAR;
	    node->data_type = pt_make_prim_data_type (parser,
						      PT_TYPE_VARCHAR);
	    if (node->data_type == NULL)
	      {
		node->type_enum = PT_TYPE_NONE;
		assert (false);
	      }
	  }
	  break;

	case F_INSERT_SUBSTRING:
	  {
	    PT_TYPE_ENUM arg1_type = PT_TYPE_NONE, arg2_type = PT_TYPE_NONE,
	      arg3_type = PT_TYPE_NONE, arg4_type = PT_TYPE_NONE;
	    PT_TYPE_ENUM arg1_orig_type, arg2_orig_type, arg3_orig_type,
	      arg4_orig_type;
	    PT_NODE *arg_array[NUM_F_INSERT_SUBSTRING_ARGS];
	    int num_args;

	    /* arg_list to array */
	    if (pt_node_list_to_array (parser, arg_list, arg_array,
				       NUM_F_INSERT_SUBSTRING_ARGS,
				       &num_args) != NO_ERROR)
	      {
		break;
	      }
	    if (num_args != NUM_F_INSERT_SUBSTRING_ARGS)
	      {
		assert (false);
		break;
	      }

	    arg1_orig_type = arg1_type = arg_array[0]->type_enum;
	    arg2_orig_type = arg2_type = arg_array[1]->type_enum;
	    arg3_orig_type = arg3_type = arg_array[2]->type_enum;
	    arg4_orig_type = arg4_type = arg_array[3]->type_enum;
	    arg_type = PT_TYPE_NONE;

	    /* validate and/or convert arguments */
	    /* arg1 should be VAR-str, but compatible with arg4
	     * (except when arg4 is BIT - no casting to varbit on arg1) */
	    if (!(PT_IS_CHAR_STRING_TYPE (arg1_type)))
	      {
		arg_type = PT_TYPE_VARCHAR;
	      }
	    else
	      {
		arg_type = arg1_type;
	      }

	    /* set result type and precision */
	    if (arg_type == PT_TYPE_VARBIT)
	      {
		node->type_enum = PT_TYPE_VARBIT;
		node->data_type = pt_make_prim_data_type (parser,
							  PT_TYPE_VARBIT);
	      }
	    else
	      {
		arg_type = node->type_enum = PT_TYPE_VARCHAR;
		node->data_type = pt_make_prim_data_type (parser,
							  PT_TYPE_VARCHAR);
	      }

	    /* final check of arg and arg4 type matching */
	    if ((arg_type == PT_TYPE_VARBIT) && (arg4_type != PT_TYPE_VARBIT))
	      {
		arg_type = PT_TYPE_NONE;
		PT_ERRORmf5 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			     MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON_4,
			     pt_show_function (fcode),
			     pt_show_type_enum (arg1_orig_type),
			     pt_show_type_enum (arg2_orig_type),
			     pt_show_type_enum (arg3_orig_type),
			     pt_show_type_enum (arg4_orig_type));
	      }
	    else if ((arg4_type == PT_TYPE_VARBIT)
		     && (arg_type != PT_TYPE_VARBIT))
	      {
		arg_type = PT_TYPE_NONE;
		PT_ERRORmf5 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			     MSGCAT_SEMANTIC_FUNC_NOT_DEFINED_ON_4,
			     pt_show_function (fcode),
			     pt_show_type_enum (arg1_orig_type),
			     pt_show_type_enum (arg2_orig_type),
			     pt_show_type_enum (arg3_orig_type),
			     pt_show_type_enum (arg4_orig_type));
	      }
	  }
	  break;

	case F_ELT:
	  {
	    PT_NODE *arg = arg_list;
	    int max_precision = TP_FLOATING_PRECISION_VALUE;

	    /*
	       Look for the first argument of character string type and obtain its
	       category (CHAR/NCHAR).
	       All other arguments should be converted to this type, which is also
	       the return type.
	     */

	    arg_type = PT_TYPE_NONE;
	    arg = arg->next;

	    while (arg && arg_type == PT_TYPE_NONE)
	      {
		if (PT_IS_CHAR_STRING_TYPE (arg->type_enum))
		  {
		    arg_type = PT_TYPE_VARCHAR;
		  }
		else
		  {
		    arg = arg->next;
		  }
	      }

	    if (arg_type == PT_TYPE_NONE)
	      {
		/* no [N]CHAR [VARYING] argument passed; convert them all to VARCHAR */
		arg_type = PT_TYPE_VARCHAR;
	      }

	    /* take the maximum precision among all value arguments */
	    arg = arg_list->next;

	    while (arg)
	      {
		int precision = TP_FLOATING_PRECISION_VALUE;

		precision = pt_character_length_for_node (arg, arg_type);
		if (max_precision != TP_FLOATING_PRECISION_VALUE)
		  {
		    if (precision == TP_FLOATING_PRECISION_VALUE ||
			max_precision < precision)
		      {
			max_precision = precision;
		      }
		  }

		arg = arg->next;
	      }

	    /* Return the selected data type and precision */

	    node->data_type = pt_make_prim_data_type (parser, arg_type);

	    if (node->data_type)
	      {
		node->type_enum = arg_type;
		node->data_type->info.data_type.precision = max_precision;
		node->data_type->info.data_type.dec_scale = 0;
	      }
	  }
	  break;

	default:
	  /* otherwise, f(x) has same type as x */
	  node->type_enum = arg_type;
	  node->data_type = parser_copy_tree_list (parser,
						   arg_list->data_type);
	  break;
	}
    }

//  assert (node->type_enum != PT_TYPE_NONE);

  return node;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * pt_evaluate_db_value_expr () - apply op to db_value opds & place it in result
 *   return: 1 if evaluation succeeded, 0 otherwise
 *   parser(in): handle to the parser context
 *   expr(in): the expression to be applied
 *   op(in): a PT_OP_TYPE (the desired operation)
 *   arg1(in): 1st db_value operand
 *   arg2(in): 2nd db_value operand
 *   arg3(in): 3rd db_value operand
 *   result(out): a newly set db_value if successful, untouched otherwise
 *   domain(in): domain of result (for arithmetic & set ops)
 *   o1(in): a PT_NODE containing the source line & column position of arg1
 *   o2(in): a PT_NODE containing the source line & column position of arg2
 *   o3(in): a PT_NODE containing the source line & column position of arg3
 *   qualifier(in): trim qualifier or datetime component specifier
 */

int
pt_evaluate_db_value_expr (PARSER_CONTEXT * parser,
			   PT_NODE * expr,
			   PT_OP_TYPE op,
			   DB_VALUE * arg1,
			   DB_VALUE * arg2,
			   DB_VALUE * arg3,
			   DB_VALUE * result,
			   TP_DOMAIN * domain,
			   PT_NODE * o1, PT_NODE * o2,
			   PT_NODE * o3, PT_MISC_TYPE qualifier)
{
  DB_TYPE typ;
  DB_TYPE typ1, typ2;
  PT_TYPE_ENUM rTyp;
  int cmp;
  DB_VALUE_COMPARE_RESULT cmp_result;
  DB_VALUE_COMPARE_RESULT cmp_result2;
  DB_VALUE tmp_val;
  int error, i;
  DB_DATA_STATUS truncation;
  TP_DOMAIN_STATUS status;
  PT_NODE *between_ge_lt, *between_ge_lt_arg1, *between_ge_lt_arg2;
  DB_VALUE *width_bucket_arg2 = NULL, *width_bucket_arg3 = NULL;

  assert (parser != NULL);

  if (!arg1 || !result)
    {
      return 0;
    }

  typ = TP_DOMAIN_TYPE (domain);
  rTyp = (PT_TYPE_ENUM) pt_db_to_type_enum (typ);

  typ1 = (arg1) ? DB_VALUE_TYPE (arg1) : DB_TYPE_NULL;
  typ2 = (arg2) ? DB_VALUE_TYPE (arg2) : DB_TYPE_NULL;
  cmp = 0;
  DB_MAKE_NULL (result);

  switch (op)
    {
    case PT_NOT:
      if (typ1 == DB_TYPE_NULL)
	{
	  DB_MAKE_NULL (result);	/* not NULL = NULL */
	}
      else if (DB_GET_INTEGER (arg1))
	{
	  db_make_int (result, false);	/* not true = false */
	}
      else
	{
	  db_make_int (result, true);	/* not false = true */
	}
      break;

    case PT_BIT_NOT:
      if (qdata_bit_not_dbval (arg1, result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_BIT_AND:
      if (qdata_bit_and_dbval (arg1, arg2, result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_BIT_OR:
      if (qdata_bit_or_dbval (arg1, arg2, result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_BIT_XOR:
      if (qdata_bit_xor_dbval (arg1, arg2, result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_BITSHIFT_LEFT:
      if (qdata_bit_shift_dbval (arg1, arg2, T_BITSHIFT_LEFT,
				 result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_BITSHIFT_RIGHT:
      if (qdata_bit_shift_dbval (arg1, arg2, T_BITSHIFT_RIGHT,
				 result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_DIV:
      if (qdata_divmod_dbval (arg1, arg2, T_INTDIV, result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_IF:
      {				/* Obs: when this case occurs both args are the same type */
	if (DB_IS_NULL (arg1))
	  {
	    if (db_value_clone (arg3, result) != NO_ERROR)
	      {
		return 0;
	      }
	  }
	else
	  {
	    if (DB_GET_INTEGER (arg1))
	      {
		if (db_value_clone (arg2, result) != NO_ERROR)
		  {
		    return 0;
		  }
	      }
	    else
	      {
		if (db_value_clone (arg3, result) != NO_ERROR)
		  {
		    return 0;
		  }
	      }
	  }
      }
      break;

    case PT_IFNULL:
    case PT_COALESCE:
    case PT_NVL:
      {
	DB_VALUE *src;
	TP_DOMAIN *target_domain = NULL;
	PT_NODE *target_node;

	if (typ == DB_TYPE_VARIABLE)
	  {
	    TP_DOMAIN *arg1_domain, *arg2_domain;

	    arg1_domain = tp_domain_resolve_value (arg1);
	    arg2_domain = tp_domain_resolve_value (arg2);

	    target_domain = tp_infer_common_domain (arg1_domain, arg2_domain,
						    false);
	  }
	else
	  {
	    target_domain = domain;
	  }

	if (typ1 == DB_TYPE_NULL)
	  {
	    src = arg2;
	    target_node = o2;
	  }
	else
	  {
	    src = arg1;
	    target_node = o1;
	  }

	if (tp_value_coerce (src, result, target_domain) != DOMAIN_COMPATIBLE)
	  {
	    rTyp =
	      (PT_TYPE_ENUM)
	      pt_db_to_type_enum (TP_DOMAIN_TYPE (target_domain));
	    PT_ERRORmf2 (parser, target_node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_CANT_COERCE_TO,
			 pt_short_print (parser, target_node),
			 pt_show_type_enum (rTyp));

	    return 0;
	  }
      }
      break;

    case PT_NVL2:
      {
	DB_VALUE *src;
	PT_NODE *target_node;
	TP_DOMAIN *target_domain = NULL;

	target_domain = tp_domain_resolve_value (arg2);

	if (typ1 == DB_TYPE_NULL)
	  {
	    src = arg3;
	    target_node = o3;
	  }
	else
	  {
	    src = arg2;
	    target_node = o2;
	  }

	if (tp_value_coerce (src, result, target_domain) != DOMAIN_COMPATIBLE)
	  {
	    rTyp =
	      (PT_TYPE_ENUM)
	      pt_db_to_type_enum (TP_DOMAIN_TYPE (target_domain));
	    PT_ERRORmf2 (parser, target_node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_CANT_COERCE_TO,
			 pt_short_print (parser, target_node),
			 pt_show_type_enum (rTyp));
	    return 0;
	  }
      }
      break;

    case PT_ISNULL:
      if (DB_IS_NULL (arg1))
	{
	  db_make_int (result, true);
	}
      else
	{
	  db_make_int (result, false);
	}
      break;

    case PT_UNARY_MINUS:
      if (qdata_unary_minus_dbval (result, arg1) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_IS_NULL:
      if (typ1 == DB_TYPE_NULL)
	{
	  db_make_int (result, true);
	}
      else
	{
	  db_make_int (result, false);
	}
      break;

    case PT_IS_NOT_NULL:
      if (typ1 == DB_TYPE_NULL)
	{
	  db_make_int (result, false);
	}
      else
	{
	  db_make_int (result, true);
	}
      break;

    case PT_IS:
    case PT_IS_NOT:
      {
	int _true, _false;

	_true = (op == PT_IS) ? 1 : 0;
	_false = 1 - _true;

	if ((o1 && o1->node_type != PT_VALUE)
	    || (o2 && o2->node_type != PT_VALUE))
	  {
	    return 0;
	  }
	if (DB_IS_NULL (arg1))
	  {
	    if (DB_IS_NULL (arg2))
	      {
		db_make_int (result, _true);
	      }
	    else
	      {
		db_make_int (result, _false);
	      }
	  }
	else
	  {
	    if (DB_IS_NULL (arg2))
	      {
		db_make_int (result, _false);
	      }
	    else
	      {
		if (DB_GET_INTEGER (arg1) == DB_GET_INTEGER (arg2))
		  {
		    db_make_int (result, _true);
		  }
		else
		  {
		    db_make_int (result, _false);
		  }
	      }
	  }
      }
      break;

    case PT_TYPEOF:
      if (db_typeof_dbval (result, arg1) != NO_ERROR)
	{
	  DB_MAKE_NULL (result);
	}
      break;

    case PT_CONCAT_WS:
      if (DB_VALUE_TYPE (arg3) == DB_TYPE_NULL)
	{
	  DB_MAKE_NULL (result);
	  break;
	}
      /* no break here */
    case PT_CONCAT:
      if (typ1 == DB_TYPE_NULL || (typ2 == DB_TYPE_NULL && o2))
	{
	  if (op != PT_CONCAT_WS)
	    {
	      DB_MAKE_NULL (result);
	      break;
	    }
	}

      /* screen out cases we don't evaluate */
      if (!PT_IS_CHAR_STRING_TYPE (rTyp))
	{
	  return 0;
	}

      switch (typ)
	{
	case DB_TYPE_VARCHAR:
//      case DB_TYPE_VARBIT:
	  if (o2)
	    {
	      if (op == PT_CONCAT_WS)
		{
		  if (typ1 == DB_TYPE_NULL)
		    {
		      if (db_value_clone (arg2, result) != NO_ERROR)
			{
			  return 0;
			}
		    }
		  else if (typ2 == DB_TYPE_NULL)
		    {
		      if (db_value_clone (arg1, result) != NO_ERROR)
			{
			  return 0;
			}
		    }
		  else
		    {
		      if (db_string_concatenate (arg1, arg3, &tmp_val,
						 &truncation) < 0
			  || truncation != DATA_STATUS_OK)
			{
			  PT_ERRORc (parser, o1, er_msg ());
			  return 0;
			}
		      if (db_string_concatenate (&tmp_val, arg2, result,
						 &truncation) < 0
			  || truncation != DATA_STATUS_OK)
			{
			  PT_ERRORc (parser, o1, er_msg ());
			  return 0;
			}
		    }
		}
	      else
		{
		  if (db_string_concatenate (arg1, arg2, result, &truncation)
		      < 0 || truncation != DATA_STATUS_OK)
		    {
		      PT_ERRORc (parser, o1, er_msg ());
		      return 0;
		    }
		}
	    }
	  else
	    {
	      if (db_value_clone (arg1, result) != NO_ERROR)
		{
		  return 0;
		}
	    }
	  break;

	default:
	  return 0;
	}

      break;

    case PT_FIELD:
      if (o1->node_type != PT_VALUE
	  || (o2 && o2->node_type != PT_VALUE) || o3->node_type != PT_VALUE)
	{
	  return 0;
	}

      if (DB_IS_NULL (arg3))
	{
	  db_make_int (result, 0);
	  break;
	}

      if (o3 && o3->next && o3->next->info.value.data_value.i == 1)
	{
	  if (tp_value_compare (arg3, arg1, 1, 0, NULL) == DB_EQ)
	    {
	      db_make_int (result, 1);
	    }
	  else if (tp_value_compare (arg3, arg2, 1, 0, NULL) == DB_EQ)
	    {
	      db_make_int (result, 2);
	    }
	  else
	    {
	      db_make_int (result, 0);
	    }
	}
      else
	{
	  i = DB_GET_INTEGER (arg1);
	  if (i > 0)
	    {
	      db_make_int (result, i);
	    }
	  else
	    {
	      if (tp_value_compare (arg3, arg2, 1, 0, NULL) == DB_EQ)
		{
		  if (o3 && o3->next)
		    {
		      db_make_int (result, o3->next->info.value.data_value.i);
		    }
		}
	      else
		{
		  db_make_int (result, 0);
		}
	    }
	}
      break;

    case PT_LEFT:
      {
	DB_VALUE tmp_val2;

	if (tp_value_coerce (arg2, &tmp_val2, &tp_Integer_domain) !=
	    DOMAIN_COMPATIBLE)
	  {
	    PT_ERRORmf2 (parser, o1, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_CANT_COERCE_TO,
			 parser_print_tree (parser, o2),
			 pt_show_type_enum (PT_TYPE_INTEGER));
	    return 0;
	  }

	db_make_int (&tmp_val, 0);

	error = db_string_substring (SUBSTRING, arg1, &tmp_val, &tmp_val2,
				     result);
	if (error < 0)
	  {
	    PT_ERRORc (parser, o1, er_msg ());
	    return 0;
	  }
      }
      break;

    case PT_RIGHT:
      if (DB_IS_NULL (arg1) || DB_IS_NULL (arg2))
	{
	  DB_MAKE_NULL (result);
	}
      else
	{
	  DB_VALUE tmp_val2;

	  if (QSTR_IS_VARBIT (typ1))
	    {
	      PT_ERRORc (parser, o1, er_msg ());
	      return 0;
	    }
	  else
	    {
	      if (db_string_char_length (arg1, &tmp_val) != NO_ERROR)
		{
		  PT_ERRORc (parser, o1, er_msg ());
		  return 0;
		}
	    }

	  if (DB_IS_NULL (&tmp_val))
	    {
	      PT_ERRORc (parser, o1, er_msg ());
	      return 0;
	    }

	  if (tp_value_coerce (arg2, &tmp_val2, &tp_Integer_domain) !=
	      DOMAIN_COMPATIBLE)
	    {
	      PT_ERRORmf2 (parser, o1, MSGCAT_SET_PARSER_SEMANTIC,
			   MSGCAT_SEMANTIC_CANT_COERCE_TO,
			   parser_print_tree (parser, o2),
			   pt_show_type_enum (PT_TYPE_INTEGER));
	      return 0;
	    }

	  /* If len, defined as second argument, is negative value,
	   * RIGHT function returns the entire string.
	   * It's same behavior with LEFT and SUBSTRING.
	   */
	  if (DB_GET_INTEGER (&tmp_val2) < 0)
	    {
	      db_make_int (&tmp_val, 0);
	    }
	  else
	    {
	      db_make_int (&tmp_val,
			   DB_GET_INTEGER (&tmp_val) -
			   DB_GET_INTEGER (&tmp_val2) + 1);
	    }
	  error = db_string_substring (SUBSTRING, arg1, &tmp_val, &tmp_val2,
				       result);
	  if (error < 0)
	    {
	      PT_ERRORc (parser, o1, er_msg ());
	      return 0;
	    }
	}
      break;

    case PT_REPEAT:
      error = db_string_repeat (arg1, arg2, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

      /* break is not needed because of return(s) */
    case PT_SPACE:
      error = db_string_space (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_LOCATE:
      if (DB_IS_NULL (arg1) || DB_IS_NULL (arg2) || (o3 && DB_IS_NULL (arg3)))
	{
	  DB_MAKE_NULL (result);
	}
      else
	{
	  if (!o3)
	    {
	      if (db_string_position (arg1, arg2, result) != NO_ERROR)
		{
		  PT_ERRORc (parser, o1, er_msg ());
		  return 0;
		}
	    }
	  else
	    {
	      DB_VALUE tmp_len, tmp_arg3;
	      int tmp = DB_GET_INTEGER (arg3);
	      if (tmp < 1)
		{
		  db_make_int (&tmp_arg3, 1);
		}
	      else
		{
		  db_make_int (&tmp_arg3, tmp);
		}

	      if (db_string_char_length (arg2, &tmp_len) != NO_ERROR)
		{
		  PT_ERRORc (parser, o2, er_msg ());
		  return 0;
		}
	      if (DB_IS_NULL (&tmp_len))
		{
		  PT_ERRORc (parser, o2, er_msg ());
		  return 0;
		}

	      db_make_int (&tmp_len,
			   DB_GET_INTEGER (&tmp_len) -
			   DB_GET_INTEGER (&tmp_arg3) + 1);

	      if (db_string_substring (SUBSTRING, arg2, &tmp_arg3,
				       &tmp_len, &tmp_val) != NO_ERROR)
		{
		  PT_ERRORc (parser, o2, er_msg ());
		  return 0;
		}

	      if (db_string_position (arg1, &tmp_val, result) != NO_ERROR)
		{
		  PT_ERRORc (parser, o1, er_msg ());
		  return 0;
		}
	      if (DB_GET_INTEGER (result) > 0)
		{
		  db_make_int (result,
			       DB_GET_INTEGER (result) +
			       DB_GET_INTEGER (&tmp_arg3) - 1);
		}
	    }
	}
      break;

    case PT_MID:
      if (DB_IS_NULL (arg1) || DB_IS_NULL (arg2) || DB_IS_NULL (arg3))
	{
	  DB_MAKE_NULL (result);
	}
      else
	{
	  DB_VALUE tmp_len, tmp_arg2, tmp_arg3;
	  int pos, len;

	  pos = DB_GET_INTEGER (arg2);
	  len = DB_GET_INTEGER (arg3);

	  if (pos < 0)
	    {
	      if (db_string_char_length (arg1, &tmp_len) != NO_ERROR)
		{
		  PT_ERRORc (parser, o1, er_msg ());
		  return 0;
		}

	      if (DB_IS_NULL (&tmp_len))
		{
		  PT_ERRORc (parser, o1, er_msg ());
		  return 0;
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

	  error = db_string_substring (SUBSTRING, arg1, &tmp_arg2, &tmp_arg3,
				       result);
	  if (error < 0)
	    {
	      PT_ERRORc (parser, o1, er_msg ());
	      return 0;
	    }
	}
      break;

    case PT_STRCMP:
      if (DB_IS_NULL (arg1) || DB_IS_NULL (arg2))
	{
	  DB_MAKE_NULL (result);
	}
      else
	{
	  if (db_string_compare (arg1, arg2, result) != NO_ERROR)
	    {
	      PT_ERRORc (parser, o1, er_msg ());
	      return 0;
	    }

	  cmp = DB_GET_INTEGER (result);
	  if (cmp < 0)
	    {
	      cmp = -1;
	    }
	  else if (cmp > 0)
	    {
	      cmp = 1;
	    }
	  db_make_int (result, cmp);
	}
      break;

    case PT_REVERSE:
      if (db_string_reverse (arg1, result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_BIT_COUNT:
      if (db_bit_count_dbval (result, arg1) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_EXISTS:
      if (TP_IS_SET_TYPE (typ1))
	{
	  if (db_set_size (db_get_set (arg1)) > 0)
	    {
	      db_make_int (result, true);
	    }
	  else
	    {
	      db_make_int (result, false);
	    }
	}
      else
	{
	  db_make_int (result, true);
	}
      break;

    case PT_AND:
      if ((typ1 == DB_TYPE_NULL && typ2 == DB_TYPE_NULL)
	  || (typ1 == DB_TYPE_NULL && DB_GET_INTEGER (arg2))
	  || (typ2 == DB_TYPE_NULL && DB_GET_INTEGER (arg1)))
	{
	  DB_MAKE_NULL (result);
	}
      else if (typ1 != DB_TYPE_NULL && DB_GET_INTEGER (arg1)
	       && typ2 != DB_TYPE_NULL && DB_GET_INTEGER (arg2))
	{
	  db_make_int (result, true);
	}
      else
	{
	  db_make_int (result, false);
	}
      break;

    case PT_OR:
      if ((typ1 == DB_TYPE_NULL && typ2 == DB_TYPE_NULL)
	  || (typ1 == DB_TYPE_NULL && !DB_GET_INTEGER (arg2))
	  || (typ2 == DB_TYPE_NULL && !DB_GET_INTEGER (arg1)))
	{
	  DB_MAKE_NULL (result);
	}
      else if (typ1 != DB_TYPE_NULL && !DB_GET_INTEGER (arg1)
	       && typ2 != DB_TYPE_NULL && !DB_GET_INTEGER (arg2))
	{
	  db_make_int (result, false);
	}
      else
	{
	  db_make_int (result, true);
	}
      break;

    case PT_XOR:
      if (typ1 == DB_TYPE_NULL || typ2 == DB_TYPE_NULL)
	{
	  DB_MAKE_NULL (result);
	}
      else if ((!DB_GET_INTEGER (arg1) && !DB_GET_INTEGER (arg2))
	       || (DB_GET_INTEGER (arg1) && DB_GET_INTEGER (arg2)))
	{
	  db_make_int (result, false);
	}
      else
	{
	  db_make_int (result, true);
	}
      break;

    case PT_PLUS:
      if (qdata_add_dbval (arg1, arg2, result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_MINUS:
      if (qdata_subtract_dbval (arg1, arg2, result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TIMES:
      if (qdata_multiply_dbval (arg1, arg2, result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_DIVIDE:
      if (qdata_divide_dbval (arg1, arg2, result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_STRCAT:
      if (qdata_strcat_dbval (arg1, arg2, result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_MOD:
    case PT_MODULUS:
      error = db_mod_dbval (result, arg1, arg2);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_PI:
      db_make_double (result, 3.14159265358979323846264338);
      break;

    case PT_RAND:
      /* rand() and drand() should always generate the same value during a statement.
       * To support it, we add lrand and drand member to PARSER_CONTEXT.
       */
      if (DB_IS_NULL (arg1))
	{
	  db_make_int (result, parser->lrand);
	}
      else
	{
	  srand48 (DB_GET_INTEGER (arg1));
	  db_make_int (result, lrand48 ());
	}
      break;

    case PT_DRAND:
      if (DB_IS_NULL (arg1))
	{
	  db_make_double (result, parser->drand);
	}
      else
	{
	  srand48 (DB_GET_INTEGER (arg1));
	  db_make_double (result, drand48 ());
	}
      break;

    case PT_RANDOM:
      /* Generate seed internally if there is no seed given as argument.
       * rand() on select list gets a random value by fetch_peek_arith().
       * But, if rand() is specified on VALUES clause of insert statement,
       * it gets a random value by the following codes.
       * In this case, DB_VALUE(arg1) of NULL type is passed.
       */
      if (DB_IS_NULL (arg1))
	{
	  struct timeval t;
	  gettimeofday (&t, NULL);
	  srand48 ((long) (t.tv_usec + lrand48 ()));
	}
      else
	{
	  srand48 (DB_GET_INTEGER (arg1));
	}
      db_make_int (result, lrand48 ());
      break;

    case PT_DRANDOM:
      if (DB_IS_NULL (arg1))
	{
	  struct timeval t;
	  gettimeofday (&t, NULL);
	  srand48 ((long) (t.tv_usec + lrand48 ()));
	}
      else
	{
	  srand48 (DB_GET_INTEGER (arg1));
	}
      db_make_double (result, drand48 ());
      break;

    case PT_FLOOR:
      error = db_floor_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_CEIL:
      error = db_ceil_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_SIGN:
      error = db_sign_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_ABS:
      error = db_abs_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_POWER:
      error = db_power_dbval (result, arg1, arg2);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_ROUND:
      error = db_round_dbval (result, arg1, arg2);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_LOG:
      error = db_log_dbval (result, arg1, arg2);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_EXP:
      error = db_exp_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_SQRT:
      error = db_sqrt_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_SIN:
      error = db_sin_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_COS:
      error = db_cos_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TAN:
      error = db_tan_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_COT:
      error = db_cot_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_ACOS:
      error = db_acos_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_ASIN:
      error = db_asin_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_ATAN:
      error = db_atan_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_ATAN2:
      error = db_atan2_dbval (result, arg1, arg2);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_DEGREES:
      error = db_degrees_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_DATEF:
      error = db_date_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TIMEF:
      error = db_time_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_RADIANS:
      error = db_radians_dbval (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_LN:
      error = db_log_generic_dbval (result, arg1, -1 /* e convention */ );
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_LOG2:
      error = db_log_generic_dbval (result, arg1, 2);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_LOG10:
      error = db_log_generic_dbval (result, arg1, 10);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TRUNC:
      error = db_trunc_dbval (result, arg1, arg2);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_CHR:
      error = db_string_chr (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_INSTR:
      error = db_string_instr (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_LEAST:
      cmp_result =
	(DB_VALUE_COMPARE_RESULT) tp_value_compare (arg1, arg2, 1, 0, NULL);
      if (cmp_result == DB_EQ || cmp_result == DB_LT)
	{
	  pr_clone_value ((DB_VALUE *) arg1, result);
	}
      else if (cmp_result == DB_GT)
	{
	  pr_clone_value ((DB_VALUE *) arg2, result);
	}
      else
	{
	  assert_release (DB_IS_NULL (arg1) || DB_IS_NULL (arg2));
	  DB_MAKE_NULL (result);
	  return 1;		/* TODO - */
	}

      if (tp_value_coerce (result, result, domain) != DOMAIN_COMPATIBLE)
	{
	  PT_ERRORmf2 (parser, o2, MSGCAT_SET_PARSER_SEMANTIC,
		       MSGCAT_SEMANTIC_CANT_COERCE_TO,
		       pt_short_print (parser, o2), pt_show_type_enum (rTyp));
	  return 0;
	}
      break;

    case PT_GREATEST:
      cmp_result =
	(DB_VALUE_COMPARE_RESULT) tp_value_compare (arg1, arg2, 1, 0, NULL);
      if (cmp_result == DB_EQ || cmp_result == DB_GT)
	{
	  pr_clone_value ((DB_VALUE *) arg1, result);
	}
      else if (cmp_result == DB_LT)
	{
	  pr_clone_value ((DB_VALUE *) arg2, result);
	}
      else
	{
	  assert_release (DB_IS_NULL (arg1) || DB_IS_NULL (arg2));
	  DB_MAKE_NULL (result);
	  return 1;		/* TODO - */
	}

      if (tp_value_coerce (result, result, domain) != DOMAIN_COMPATIBLE)
	{
	  PT_ERRORmf2 (parser, o2, MSGCAT_SET_PARSER_SEMANTIC,
		       MSGCAT_SEMANTIC_CANT_COERCE_TO,
		       pt_short_print (parser, o2), pt_show_type_enum (rTyp));
	  return 0;
	}
      break;

    case PT_POSITION:
      error = db_string_position (arg1, arg2, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_FINDINSET:
      error = db_find_string_in_in_set (arg1, arg2, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_SUBSTRING:
      error =
	db_string_substring (pt_misc_to_qp_misc_operand
			     (qualifier), arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_OCTET_LENGTH:
      if (PT_IS_NULL_NODE (o1))
	{
	  DB_MAKE_NULL (result);
	  return 1;		/* TODO - */
	}

      if (!PT_IS_CHAR_STRING_TYPE (o1->type_enum))
	{
	  return 0;
	}

      db_make_int (result, db_get_string_size (arg1));
      break;

    case PT_BIT_LENGTH:
      if (PT_IS_NULL_NODE (o1))
	{
	  DB_MAKE_NULL (result);
	  return 1;		/* TODO - */
	}

      if (!PT_IS_BIT_STRING_TYPE (o1->type_enum))
	{
	  return 0;
	}

      {
	int len = 0;

	/* must be a varbit gadget */
	(void) db_get_varbit (arg1, &len);
	db_make_int (result, len);
      }
      break;

    case PT_CHAR_LENGTH:
      if (PT_IS_NULL_NODE (o1))
	{
	  DB_MAKE_NULL (result);
	  return 1;		/* TODO - */
	}
      else if (!PT_IS_CHAR_STRING_TYPE (o1->type_enum))
	{
	  return 0;
	}
      db_make_int (result, DB_GET_STRING_LENGTH (arg1));
      break;

    case PT_LOWER:
      error = db_string_lower (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_UPPER:
      error = db_string_upper (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_HEX:
      error = db_hex (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_ASCII:
      error = db_ascii (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_CONV:
      error = db_conv (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_BIN:
      error = db_bigint_to_binary_string (arg1, result);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TRIM:
      error =
	db_string_trim (pt_misc_to_qp_misc_operand (qualifier),
			arg2, arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_LIKE_LOWER_BOUND:
    case PT_LIKE_UPPER_BOUND:
      error = db_like_bound (arg1, arg2, result, (op == PT_LIKE_LOWER_BOUND));
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_LTRIM:
      error = db_string_trim (LEADING, arg2, arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_RTRIM:
      error = db_string_trim (TRAILING, arg2, arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_FROM_UNIXTIME:
      error = db_from_unixtime (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_SUBSTRING_INDEX:
      error = db_string_substring_index (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_MD5:
      error = db_string_md5 (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_SHA_ONE:
      error = db_string_sha_one (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_SHA_TWO:
      error = db_string_sha_two (arg1, arg2, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TO_BASE64:
      error = db_string_to_base64 (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_FROM_BASE64:
      error = db_string_from_base64 (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_LPAD:
      error = db_string_pad (LEADING, arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_RPAD:
      error = db_string_pad (TRAILING, arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_REPLACE:
      error = db_string_replace (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TRANSLATE:
      error = db_string_translate (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_LAST_DAY:
      error = db_last_day (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, expr, er_msg ());
	  return 0;
	}
      break;

    case PT_UNIX_TIMESTAMP:
      /* check iff empty arg */
      error = db_unix_timestamp (DB_IS_NULL (arg1) ? NULL : arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_STR_TO_DATE:
      error = db_str_to_date (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TIME_FORMAT:
      error = db_time_format (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_YEARF:
    case PT_MONTHF:
    case PT_DAYF:
      error = db_get_date_item (arg1, op, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_DAYOFMONTH:
      /* day of month is handled like PT_DAYF */
      error = db_get_date_item (arg1, PT_DAYF, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_HOURF:
    case PT_MINUTEF:
    case PT_SECONDF:
      error = db_get_time_item (arg1, op, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_QUARTERF:
      error = db_get_date_quarter (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_WEEKDAY:
    case PT_DAYOFWEEK:
      error = db_get_date_weekday (arg1, op, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_DAYOFYEAR:
      error = db_get_date_dayofyear (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TODAYS:
      error = db_get_date_totaldays (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_FROMDAYS:
      error = db_get_date_from_days (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TIMETOSEC:
      error = db_convert_time_to_sec (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_SECTOTIME:
      error = db_convert_sec_to_time (arg1, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_MAKEDATE:
      error = db_add_days_to_year (arg1, arg2, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_MAKETIME:
      error = db_convert_to_time (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_WEEKF:
      error = db_get_date_week (arg1, arg2, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_SCHEMA:
    case PT_DATABASE:
      DB_MAKE_NULL (result);
      error = db_make_string (&tmp_val, db_get_database_name ());
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}

      error = db_value_clone (&tmp_val, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_VERSION:
      DB_MAKE_NULL (result);
      error = db_make_string (&tmp_val, db_get_database_version ());
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}

      error = db_value_clone (&tmp_val, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_MONTHS_BETWEEN:
      error = db_months_between (arg1, arg2, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_FORMAT:
      error = db_format (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_DATE_FORMAT:
      error = db_date_format (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_ADDDATE:
      error = db_date_add_interval_days (result, arg1, arg2);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_DATEDIFF:
      error = db_date_diff (arg1, arg2, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TIMEDIFF:
      error = db_time_diff (arg1, arg2, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, expr, er_msg ());
	  return 0;
	}
      break;

    case PT_SUBDATE:
      error = db_date_sub_interval_days (result, arg1, arg2);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_DATE_ADD:
      error = db_date_add_interval_expr (result, arg1, arg2,
					 o3->info.expr.qualifier);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_DATE_SUB:
      error = db_date_sub_interval_expr (result, arg1, arg2,
					 o3->info.expr.qualifier);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_SYS_DATE:
      {
	DB_DATETIME *tmp_datetime;

	db_value_domain_init (result, DB_TYPE_DATE, DB_DEFAULT_PRECISION,
			      DB_DEFAULT_SCALE);

	tmp_datetime = db_get_datetime (&parser->sys_datetime);

	db_value_put_encoded_date (result, &tmp_datetime->date);
      }
      break;

    case PT_UTC_DATE:
      {
	DB_VALUE timezone;
	DB_BIGINT timezone_milis;
	DB_DATETIME db_datetime;
	DB_DATETIME *tmp_datetime;

	/* extract the timezone part */
	db_sys_timezone (&timezone);
	timezone_milis = DB_GET_INTEGER (&timezone) * 60000;
	tmp_datetime = db_get_datetime (&parser->sys_datetime);
	db_add_int_to_datetime (tmp_datetime, timezone_milis, &db_datetime);

	DB_MAKE_ENCODED_DATE (result, &db_datetime.date);
      }
      break;

    case PT_UTC_TIME:
      {
	DB_TIME db_time;
	DB_VALUE timezone;
	int timezone_val;
	DB_DATETIME *tmp_datetime;

	tmp_datetime = db_get_datetime (&parser->sys_datetime);
	db_time = tmp_datetime->time / 1000;
	/* extract the timezone part */
	db_sys_timezone (&timezone);
	timezone_val = DB_GET_INTEGER (&timezone);
	db_time = db_time + timezone_val * 60 + SECONDS_OF_ONE_DAY;
	db_time = db_time % SECONDS_OF_ONE_DAY;

	DB_MAKE_ENCODED_TIME (result, &db_time);
      }
      break;

    case PT_SYS_TIME:
      {
	DB_DATETIME *tmp_datetime;
	DB_TIME tmp_time;

	db_value_domain_init (result, DB_TYPE_TIME, DB_DEFAULT_PRECISION,
			      DB_DEFAULT_SCALE);

	tmp_datetime = db_get_datetime (&parser->sys_datetime);
	tmp_time = tmp_datetime->time / 1000;

	db_value_put_encoded_time (result, &tmp_time);
      }
      break;

    case PT_SYS_DATETIME:
      {
	DB_DATETIME *tmp_datetime;

	db_value_domain_init (result, DB_TYPE_DATETIME,
			      DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	tmp_datetime = db_get_datetime (&parser->sys_datetime);

	db_make_datetime (result, tmp_datetime);
      }
      break;

    case PT_CURRENT_USER:
      {
	char *username = au_user_name ();

	error = db_make_string (result, username);
	if (error < 0)
	  {
	    db_string_free (username);
	    PT_ERRORc (parser, o1, er_msg ());
	    return 0;
	  }

	result->need_clear = true;
      }
      break;

    case PT_USER:
      {
	char *user = NULL;

	user = db_get_user_and_host_name ();
	DB_MAKE_NULL (result);

	error = db_make_string (&tmp_val, user);
	if (error < 0)
	  {
	    PT_ERRORc (parser, o1, er_msg ());
	    free_and_init (user);
	    return 0;
	  }
	tmp_val.need_clear = true;

	error = pr_clone_value (&tmp_val, result);
	if (error < 0)
	  {
	    PT_ERRORc (parser, o1, er_msg ());
	    return 0;
	  }
      }
      break;

#if 1				/* TODO - trace */
    case PT_HA_STATUS:
    case PT_SHARD_GROUPID:
    case PT_SHARD_LOCKNAME:
    case PT_SHARD_NODEID:
    case PT_EXPLAIN:
      assert (false);
      DB_MAKE_NULL (result);
      break;
#endif

    case PT_TO_CHAR:
      error = db_to_char (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TO_DATE:
      error = db_to_date (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TO_TIME:
      error = db_to_time (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TO_DATETIME:
      error = db_to_datetime (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_TO_NUMBER:
      error = db_to_number (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_CAST:
      {
	PT_NODE *cast_type = NULL;
	TP_DOMAIN *cast_domain = NULL;

	cast_type = expr->info.expr.cast_type;
	if (cast_type == NULL)
	  {
	    assert (false);
	    return 0;
	  }

	cast_domain = pt_node_data_type_to_db_domain (parser, cast_type,
						      cast_type->type_enum);
	if (cast_domain != NULL)
	  {
	    cast_domain = tp_domain_cache (cast_domain);
	  }

	if (cast_domain == NULL
	    || TP_DOMAIN_TYPE (cast_domain) == DB_TYPE_NULL
	    || TP_DOMAIN_TYPE (cast_domain) == DB_TYPE_VARIABLE)
	  {
	    return 0;
	  }
	status = tp_value_cast (arg1, result, cast_domain);
	if (status != DOMAIN_COMPATIBLE)
	  {
	    assert (expr->node_type == PT_EXPR);
	    PT_ERRORmf2 (parser, o1, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_CANT_COERCE_TO,
			 pt_short_print (parser, o1),
			 pt_show_type_enum (rTyp));
	    return 0;
	  }
      }
      break;

    case PT_CASE:
    case PT_DECODE:
      /* If arg3 = NULL, then arg2 = NULL and arg1 != NULL.  For this case,
       * we've already finished checking case_search_condition. */
      if (arg3 && (DB_VALUE_TYPE (arg3) == DB_TYPE_INTEGER
		   && DB_GET_INTEGER (arg3) != 0))
	{
	  if (tp_value_coerce (arg1, result, domain) != DOMAIN_COMPATIBLE)
	    {
	      PT_ERRORmf2 (parser, o1, MSGCAT_SET_PARSER_SEMANTIC,
			   MSGCAT_SEMANTIC_CANT_COERCE_TO,
			   pt_short_print (parser, o1),
			   pt_show_type_enum (rTyp));
	      return 0;
	    }
	}
      else
	{
	  if (tp_value_coerce (arg2, result, domain) != DOMAIN_COMPATIBLE)
	    {
	      PT_ERRORmf2 (parser, o2, MSGCAT_SET_PARSER_SEMANTIC,
			   MSGCAT_SEMANTIC_CANT_COERCE_TO,
			   pt_short_print (parser, o2),
			   pt_show_type_enum (rTyp));
	      return 0;
	    }
	}
      break;

    case PT_NULLIF:
      if (tp_value_compare (arg1, arg2, 1, 0, NULL) == DB_EQ)
	{
	  DB_MAKE_NULL (result);
	}
      else
	{
	  pr_clone_value ((DB_VALUE *) arg1, result);
	}
      break;

    case PT_EXTRACT:
      if (qdata_extract_dbval
	  (pt_misc_to_qp_misc_operand (expr->info.expr.qualifier), arg1,
	   result) != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_EQ:
    case PT_NE:
    case PT_GE:
    case PT_GT:
    case PT_LT:
    case PT_LE:

    case PT_NULLSAFE_EQ:

    case PT_IS_IN:
    case PT_IS_NOT_IN:
    case PT_LIKE:
    case PT_NOT_LIKE:
    case PT_RLIKE:
    case PT_NOT_RLIKE:
    case PT_RLIKE_BINARY:
    case PT_NOT_RLIKE_BINARY:
    case PT_BETWEEN:
    case PT_NOT_BETWEEN:
    case PT_RANGE:

      if (op != PT_BETWEEN && op != PT_NOT_BETWEEN
	  && op != PT_RANGE && (op != PT_EQ || qualifier != PT_EQ_TORDER))
	{
	  if ((typ1 == DB_TYPE_NULL || typ2 == DB_TYPE_NULL)
	      && op != PT_NULLSAFE_EQ)
	    {
	      DB_MAKE_NULL (result);	/* NULL comp_op any = NULL */
	      break;
	    }
	}

      switch (op)
	{
	case PT_EQ:
	  if (qualifier == PT_EQ_TORDER)
	    {
	      cmp_result =
		(DB_VALUE_COMPARE_RESULT) tp_value_compare (arg1, arg2, 1, 1,
							    NULL);
	      cmp =
		(cmp_result == DB_UNK) ? -1 : (cmp_result == DB_EQ) ? 1 : 0;
	      break;
	    }

	  /* fall through */

	  cmp_result =
	    (DB_VALUE_COMPARE_RESULT) db_value_compare (arg1, arg2);
	  cmp = (cmp_result == DB_UNK) ? -1 : (cmp_result == DB_EQ) ? 1 : 0;
	  break;

	case PT_NE:
	  cmp_result =
	    (DB_VALUE_COMPARE_RESULT) db_value_compare (arg1, arg2);
	  cmp = (cmp_result == DB_UNK) ? -1 : (cmp_result != DB_EQ) ? 1 : 0;
	  break;

	case PT_NULLSAFE_EQ:
	  if ((o1 && o1->node_type != PT_VALUE)
	      || (o2 && o2->node_type != PT_VALUE))
	    {
	      return 0;
	    }
	  if (arg1 == NULL || arg1->domain.general_info.is_null)
	    {
	      if (arg2 == NULL || arg2->domain.general_info.is_null)
		{
		  cmp_result = DB_EQ;
		}
	      else
		{
		  cmp_result = DB_NE;
		}
	    }
	  else
	    {
	      if (arg2 == NULL || arg2->domain.general_info.is_null)
		{
		  cmp_result = DB_NE;
		}
	      else
		{
		  cmp_result = (DB_VALUE_COMPARE_RESULT)
		    db_value_compare (arg1, arg2);
		}
	    }
	  cmp = (cmp_result == DB_EQ) ? 1 : 0;
	  break;

	case PT_GE:
	  cmp_result =
	    (DB_VALUE_COMPARE_RESULT) db_value_compare (arg1, arg2);
	  cmp = (cmp_result == DB_UNK) ? -1 : (cmp_result == DB_EQ
					       || cmp_result ==
					       DB_GT) ? 1 : 0;
	  break;

	case PT_GT:
	  cmp_result =
	    (DB_VALUE_COMPARE_RESULT) db_value_compare (arg1, arg2);
	  cmp = (cmp_result == DB_UNK) ? -1 : (cmp_result == DB_GT) ? 1 : 0;
	  break;

	case PT_LE:
	  cmp_result =
	    (DB_VALUE_COMPARE_RESULT) db_value_compare (arg1, arg2);
	  cmp = (cmp_result == DB_UNK) ? -1 : (cmp_result == DB_EQ
					       || cmp_result ==
					       DB_LT) ? 1 : 0;
	  break;

	case PT_LT:
	  cmp_result =
	    (DB_VALUE_COMPARE_RESULT) db_value_compare (arg1, arg2);
	  cmp = (cmp_result == DB_UNK) ? -1 : (cmp_result == DB_LT) ? 1 : 0;
	  break;

	case PT_IS_IN:
	  cmp = set_issome (arg1, db_get_set (arg2), PT_IS_IN, 1);
	  break;

	case PT_IS_NOT_IN:
	  cmp = set_issome (arg1, db_get_set (arg2), PT_IS_IN, 1);
	  if (cmp == 1)
	    cmp = 0;
	  else if (cmp == 0)
	    cmp = 1;
	  break;

	case PT_LIKE:
	case PT_NOT_LIKE:
	  {
	    DB_VALUE *esc_char = arg3;
	    DB_VALUE slash_char;
	    char const *slash_str = "\\";

	    if (db_string_like (arg1, arg2, esc_char, &cmp))
	      {
		/* db_string_like() also checks argument types */
		return 0;
	      }

	    cmp = ((op == PT_LIKE && cmp == V_TRUE)
		   || (op == PT_NOT_LIKE && cmp == V_FALSE)) ? 1 : 0;
	  }
	  break;

	case PT_RLIKE:
	case PT_NOT_RLIKE:
	case PT_RLIKE_BINARY:
	case PT_NOT_RLIKE_BINARY:
	  {
	    int err = db_string_rlike (arg1, arg2, arg3, NULL, NULL, &cmp);

	    switch (err)
	      {
	      case NO_ERROR:
		break;
	      case ER_REGEX_COMPILE_ERROR:	/* fall through */
	      case ER_REGEX_EXEC_ERROR:
		PT_ERRORc (parser, o1, er_msg ());
	      default:
		return 0;
	      }

	    /* negate result if using NOT syntax of operator */
	    if (op == PT_NOT_RLIKE || op == PT_NOT_RLIKE_BINARY)
	      {
		switch (cmp)
		  {
		  case V_TRUE:
		    cmp = V_FALSE;
		    break;

		  case V_FALSE:
		    cmp = V_TRUE;
		    break;

		  default:
		    break;
		  }
	      }
	  }
	  break;

	case PT_BETWEEN:
	  /* special handling for PT_BETWEEN and PT_NOT_BETWEEN */
	  cmp_result =
	    (DB_VALUE_COMPARE_RESULT) db_value_compare (arg2, arg1);
	  cmp_result2 =
	    (DB_VALUE_COMPARE_RESULT) db_value_compare (arg1, arg3);
	  if (((cmp_result == DB_UNK) && (cmp_result2 == DB_UNK))
	      || ((cmp_result == DB_UNK)
		  && ((cmp_result2 == DB_LT)
		      || (cmp_result2 == DB_EQ)))
	      || ((cmp_result2 == DB_UNK)
		  && ((cmp_result == DB_LT) || (cmp_result == DB_EQ))))
	    {
	      cmp = -1;
	    }
	  else if (((cmp_result != DB_UNK)
		    &&
		    (!((cmp_result == DB_LT) || (cmp_result == DB_EQ))))
		   || ((cmp_result2 != DB_UNK)
		       &&
		       (!((cmp_result2 == DB_LT) || (cmp_result2 == DB_EQ)))))
	    {
	      cmp = 0;
	    }
	  else
	    {
	      cmp = 1;
	    }
	  break;

	case PT_NOT_BETWEEN:
	  /* special handling for PT_BETWEEN and PT_NOT_BETWEEN */
	  cmp_result =
	    (DB_VALUE_COMPARE_RESULT) db_value_compare (arg2, arg1);
	  cmp_result2 =
	    (DB_VALUE_COMPARE_RESULT) db_value_compare (arg1, arg3);
	  if (((cmp_result == DB_UNK) && (cmp_result2 == DB_UNK))
	      || ((cmp_result == DB_UNK)
		  && ((cmp_result2 == DB_LT)
		      || (cmp_result2 == DB_EQ)))
	      || ((cmp_result2 == DB_UNK)
		  && ((cmp_result == DB_LT) || (cmp_result == DB_EQ))))
	    {
	      cmp = -1;
	    }
	  else if (((cmp_result != DB_UNK)
		    &&
		    (!((cmp_result == DB_LT) || (cmp_result == DB_EQ))))
		   || ((cmp_result2 != DB_UNK)
		       &&
		       (!((cmp_result2 == DB_LT) || (cmp_result2 == DB_EQ)))))
	    {
	      cmp = 1;
	    }
	  else
	    {
	      cmp = 0;
	    }
	  break;

	case PT_RANGE:
	  break;

	default:
	  return 0;
	}

      if (cmp == 1)
	db_make_int (result, 1);
      else if (cmp == 0)
	db_make_int (result, 0);
      else
	DB_MAKE_NULL (result);
      break;

    case PT_INDEX_CARDINALITY:
      /* constant folding for this expression is never performed :
       * is always resolved on server*/
      return 0;

    case PT_LIST_DBS:
    case PT_ASSIGN:
    case PT_LIKE_ESCAPE:
    case PT_BETWEEN_AND:
    case PT_BETWEEN_GE_LE:
    case PT_BETWEEN_GE_LT:
    case PT_BETWEEN_GT_LE:
    case PT_BETWEEN_GT_LT:
    case PT_BETWEEN_EQ_NA:
    case PT_BETWEEN_INF_LE:
    case PT_BETWEEN_INF_LT:
    case PT_BETWEEN_GE_INF:
    case PT_BETWEEN_GT_INF:
      /* these don't need to be handled */
      return 0;

    case PT_INET_ATON:
      error = db_inet_aton (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_INET_NTOA:
      error = db_inet_ntoa (result, arg1);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_WIDTH_BUCKET:
      between_ge_lt = o2->info.expr.arg2;
      assert (between_ge_lt != NULL
	      && between_ge_lt->node_type == PT_EXPR
	      && between_ge_lt->info.expr.op == PT_BETWEEN_GE_LT);

      between_ge_lt_arg1 = between_ge_lt->info.expr.arg1;
      assert (between_ge_lt_arg1 != NULL
	      && between_ge_lt_arg1->node_type == PT_VALUE);

      between_ge_lt_arg2 = between_ge_lt->info.expr.arg2;
      assert (between_ge_lt_arg2 != NULL
	      && between_ge_lt_arg2->node_type == PT_VALUE);

      width_bucket_arg2 = pt_value_to_db (parser, between_ge_lt_arg1);
      if (width_bucket_arg2 == NULL)
	{
	  /* error is set in pt_value_to_db */
	  return 0;
	}
      width_bucket_arg3 = pt_value_to_db (parser, between_ge_lt_arg2);
      if (width_bucket_arg3 == NULL)
	{
	  return 0;
	}

      /* get all the parameters */
      error = db_width_bucket (result, arg1, width_bucket_arg2,
			       width_bucket_arg3, arg3);
      if (error != NO_ERROR)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    case PT_INDEX_PREFIX:
      error = db_string_index_prefix (arg1, arg2, arg3, result);
      if (error < 0)
	{
	  PT_ERRORc (parser, o1, er_msg ());
	  return 0;
	}
      break;

    default:
      break;
    }

  return 1;

overflow:
  PT_ERRORmf (parser, o1, MSGCAT_SET_PARSER_SEMANTIC,
	      MSGCAT_SEMANTIC_DATA_OVERFLOW_ON, pt_show_type_enum (rTyp));
  return 0;
}

/*
 * pt_evaluate_function_w_args () - evaluate the function to a DB_VALUE
 *   return: 1, if successful,
 *           0, if not successful.
 *   parser(in): parser global context info for reentrancy
 *   fcode(in): function code
 *   args(in): array of arguments' values
 *   num_args(in): number of arguments
 *   result(out): result value of function (if evaluated)
 */
int
pt_evaluate_function_w_args (PARSER_CONTEXT * parser, FUNC_TYPE fcode,
			     DB_VALUE * args[], const int num_args,
			     DB_VALUE * result)
{
  int error = NO_ERROR, i;

  assert (parser != NULL);
  assert (args != NULL);
  assert (result != NULL);

  if (!args || !result)
    {
      return 0;
    }

  /* init array vars */
  for (i = 0; i < num_args; i++)
    {
      assert (args[i] != NULL);
    }

  switch (fcode)
    {
    case F_INSERT_SUBSTRING:
      {
	error = db_string_insert_substring (args[0], args[1], args[2],
					    args[3], result);
	if (error != NO_ERROR)
	  {
	    return 0;
	  }
      }
      break;
    case F_ELT:
      error = db_string_elt (result, args, num_args);
      if (error != NO_ERROR)
	{
	  return 0;
	}
      break;
    default:
      /* a supported function doesn't have const folding code */
      assert (false);
      break;
    }
  return 1;
}

/*
 * pt_evaluate_function () - evaluate constant function
 *   return: NO_ERROR, if evaluation successfull,
 *	     an error code, if unsuccessful
 *   parser(in): parser global context info for reentrancy
 *   func(in): a parse tree representation of a possibly constant function
 *   dbval_res(in/out): the result DB_VALUE of evaluation
 */
int
pt_evaluate_function (PARSER_CONTEXT * parser, PT_NODE * func,
		      DB_VALUE * dbval_res)
{
  PT_NODE *operand;
  DB_VALUE dummy, **arg_array;
  FUNC_TYPE fcode;
  int error = NO_ERROR, i;
  int num_args = 0;
  bool all_args_const = false;

  /* init array variables */
  arg_array = NULL;

  if (func == NULL)
    {
      return ER_FAILED;
    }

  if (func->node_type != PT_FUNCTION)
    {
      return ER_FAILED;
    }

  fcode = func->info.function.function_type;
  /* only functions wrapped with expressions are supported */
  if (!pt_is_expr_wrapped_function (parser, func))
    {
      return ER_FAILED;
    }

  db_make_null (dbval_res);

  /* count function's arguments */
  operand = func->info.function.arg_list;
  num_args = 0;
  while (operand)
    {
      ++num_args;
      operand = operand->next;
    }
  assert (num_args > 0);

  arg_array = (DB_VALUE **) calloc (num_args, sizeof (DB_VALUE *));
  if (arg_array == NULL)
    {
      goto end;
    }

  /* convert all operands to DB_VALUE arguments */
  /* for some functions this may not be necessary :
   * you need to break from this loop and solve them at next steps */
  all_args_const = true;
  operand = func->info.function.arg_list;
  for (i = 0; i < num_args; i++)
    {
      if (operand != NULL && operand->node_type == PT_VALUE)
	{
	  DB_VALUE *arg = NULL;

	  arg = pt_value_to_db (parser, operand);
	  if (arg == NULL)
	    {
	      all_args_const = false;
	      break;
	    }
	  else
	    {
	      arg_array[i] = arg;
	    }
	}
      else
	{
	  db_make_null (&dummy);
	  arg_array[i] = &dummy;
	  all_args_const = false;
	  break;
	}
      operand = operand->next;
    }

  if (all_args_const && i == num_args)
    {
      TP_DOMAIN *domain;

      /* use the caching variant of this function ! */
      domain = pt_xasl_node_to_domain (parser, func);

      /* check if we received an error getting the domain */
      if (pt_has_error (parser))
	{
	  pr_clear_value (dbval_res);
	  error = ER_FAILED;
	  goto end;
	}

      if (pt_evaluate_function_w_args (parser, fcode, arg_array, num_args,
				       dbval_res) != 1)
	{
	  error = ER_FAILED;
	  goto end;
	}
    }
  else
    {
      error = ER_FAILED;
    }
end:
  if (arg_array != NULL)
    {
      free_and_init (arg_array);
    }

  return error;
}
#endif

/*
 * pt_semantic_type () - sets data types for all expressions in a parse tree
 * 			 and evaluates constant sub expressions
 *   return:
 *   parser(in):
 *   tree(in/out):
 *   sc_info_ptr(in):
 */

PT_NODE *
pt_semantic_type (PARSER_CONTEXT * parser,
		  PT_NODE * tree, SEMANTIC_CHK_INFO * sc_info_ptr)
{
  SEMANTIC_CHK_INFO sc_info = { NULL, false };

  if (pt_has_error (parser))
    {
      return NULL;
    }

  if (sc_info_ptr)
    {
      /* do type checking */
      tree =
	parser_walk_tree (parser, tree, pt_eval_type_pre,
			  sc_info_ptr, pt_eval_type, sc_info_ptr);
    }
  else
    {
      sc_info.top_node = tree;
      /* do type checking */
      tree = parser_walk_tree (parser, tree, pt_eval_type_pre,
			       &sc_info, pt_eval_type, &sc_info);
    }

  if (pt_has_error (parser))
    {
      tree = NULL;
    }

  return tree;
}


/*
 * pt_class_name () - return the class name of a data_type node
 *   return:
 *   type(in): a data_type node
 */
static const char *
pt_class_name (const PT_NODE * type)
{
  if (!type
      || type->node_type != PT_DATA_TYPE
      || !type->info.data_type.entity
      || type->info.data_type.entity->node_type != PT_NAME)
    {
      return NULL;
    }

  return type->info.data_type.entity->info.name.original;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_set_default_data_type () -
 *   return:
 *   parser(in):
 *   type(in):
 *   dtp(in):
 */
static int
pt_set_default_data_type (PARSER_CONTEXT * parser,
			  PT_TYPE_ENUM type, PT_NODE ** dtp)
{
  PT_NODE *dt;
  int error = NO_ERROR;

  dt = parser_new_node (parser, PT_DATA_TYPE);
  if (dt == NULL)
    {
      return ER_GENERIC_ERROR;
    }

  dt->type_enum = type;
  switch (type)
    {
    case PT_TYPE_VARCHAR:
      dt->info.data_type.precision = TP_FLOATING_PRECISION_VALUE;
      dt->info.data_type.collation_id = LANG_COERCIBLE_COLL;
      break;

    case PT_TYPE_VARBIT:
      dt->info.data_type.precision = TP_FLOATING_PRECISION_VALUE;
      break;

    case PT_TYPE_NUMERIC:
      dt->info.data_type.precision = TP_FLOATING_PRECISION_VALUE;
      /*
       * FIX ME!! Is it the case that this will always happen in
       * zero-scale context?  That's certainly the case when we're
       * coercing from integers, but what about floats and doubles?
       */
      dt->info.data_type.dec_scale = DB_DEFAULT_NUMERIC_SCALE;
      break;

    default:
      PT_INTERNAL_ERROR (parser, "type check");
      error = ER_GENERIC_ERROR;
      break;
    }

  *dtp = dt;
  return error;
}
#endif

/*
 * pt_coerce_value () - coerce a PT_VALUE into another PT_VALUE of
 * 			compatible type
 *   return: NO_ERROR on success, non-zero for ERROR
 *   parser(in):
 *   src(in): a pointer to the original PT_VALUE
 *   dest(out): a pointer to the coerced PT_VALUE
 *   desired_type(in): the desired type of the coerced result
 *   data_type(in): the data type list of a (desired) set type or
 *                  the data type of an object or NULL
 */
int
pt_coerce_value (PARSER_CONTEXT * parser, PT_NODE * src, PT_NODE * dest,
		 PT_TYPE_ENUM desired_type, PT_NODE * data_type)
{
  PT_TYPE_ENUM original_type;
  PT_NODE *dest_next;
  int err = NO_ERROR;
  PT_NODE *temp = NULL;
  bool is_collation_change = false;

  assert (src != NULL);
  assert (dest != NULL);
  assert (data_type == NULL || PT_IS_PARAMETERIZED_TYPE (desired_type));

  dest_next = dest->next;

  original_type = src->type_enum;
  if (PT_IS_NULL_NODE (src))
    {
      original_type = PT_TYPE_NULL;
    }

  if (PT_HAS_COLLATION (original_type)
      && PT_HAS_COLLATION (desired_type)
      && src->data_type != NULL
      && data_type != NULL
      && src->data_type->info.data_type.collation_id
      != data_type->info.data_type.collation_id)
    {
      is_collation_change = true;
    }

  if ((original_type == (PT_TYPE_ENUM) desired_type
       && original_type != PT_TYPE_NUMERIC
       && desired_type != PT_TYPE_OBJECT
       && !is_collation_change) || original_type == PT_TYPE_NULL)
    {
      if (src != dest)
	{
	  *dest = *src;
	  dest->next = dest_next;
	}
      return NO_ERROR;
    }

  if (data_type == NULL && PT_IS_PARAMETERIZED_TYPE (desired_type))
    {
      assert (false);		/* is impossible */
#if 1				/* TODO - */
      err = ER_GENERIC_ERROR;
      return err;
#else
      err = pt_set_default_data_type (parser,
				      (PT_TYPE_ENUM) desired_type,
				      &data_type);
      if (err < 0)
	{
	  return err;
	}
#endif
    }

  if (original_type == PT_TYPE_NUMERIC
      && desired_type == PT_TYPE_NUMERIC
      && src->data_type != NULL
      && (src->data_type->info.data_type.precision ==
	  data_type->info.data_type.precision)
      && (src->data_type->info.data_type.dec_scale ==
	  data_type->info.data_type.dec_scale) && !is_collation_change)
    {				/* exact match */

      if (src != dest)
	{
	  *dest = *src;
	  dest->next = dest_next;
	}
      return NO_ERROR;
    }

  if (original_type == PT_TYPE_NONE && src->node_type != PT_HOST_VAR)
    {
      if (src != dest)
	{
	  *dest = *src;
	  dest->next = dest_next;
	}
      dest->type_enum = (PT_TYPE_ENUM) desired_type;
      dest->data_type = parser_copy_tree_list (parser, data_type);
      /* don't return, in case further coercion is needed
         set original type to match desired type to avoid confusing
         type check below */
    }

  assert (err == NO_ERROR);

  switch (src->node_type)
    {
    case PT_HOST_VAR:
      /* binding of host variables may be delayed in the case of an esql
       * PREPARE statement until an OPEN cursor or an EXECUTE statement.
       * in this case we seem to have no choice but to assume each host
       * variable is typeless and can be coerced into any desired type.
       */
      if (parser->set_host_var == 0)
	{
	  dest->type_enum = (PT_TYPE_ENUM) desired_type;
	  dest->data_type = parser_copy_tree_list (parser, data_type);
	  return NO_ERROR;
	}

      /* otherwise fall through to the PT_VALUE case */

    case PT_VALUE:
      {
	DB_VALUE *db_src = NULL;
	DB_VALUE db_dest;
	TP_DOMAIN *desired_domain;

	db_src = pt_value_to_db (parser, src);
	if (!db_src)
	  {
	    err = ER_GENERIC_ERROR;
	    break;
	  }

	db_make_null (&db_dest);

	/* be sure to use the domain caching versions */
	if (data_type)
	  {
	    desired_domain = pt_node_data_type_to_db_domain
	      (parser, (PT_NODE *) data_type, (PT_TYPE_ENUM) desired_type);
	    /* need a caching version of this function ? */
	    if (desired_domain != NULL)
	      {
		desired_domain = tp_domain_cache (desired_domain);
	      }
	  }
	else
	  {
	    desired_domain =
	      pt_xasl_type_enum_to_domain ((PT_TYPE_ENUM) desired_type);
	  }

	err = tp_value_coerce (db_src, &db_dest, desired_domain);

	switch (err)
	  {
	  case DOMAIN_INCOMPATIBLE:
	    err = ER_IT_INCOMPATIBLE_DATATYPE;
	    break;
	  case DOMAIN_OVERFLOW:
	    err = ER_IT_DATA_OVERFLOW;
	    break;
	  case DOMAIN_ERROR:
	    err = er_errid ();
	    break;
	  default:
	    break;
	  }

	if (err == DOMAIN_COMPATIBLE && src->node_type == PT_HOST_VAR)
	  {
	    /* when the type of the host variable is compatible to coerce,
	     * it is enough. NEVER change the node type to PT_VALUE. */
	    db_value_clear (&db_dest);
	    return NO_ERROR;
	  }

	if (src->info.value.db_value_is_in_workspace)
	  {
	    (void) db_value_clear (db_src);
	  }

	if (err >= 0)
	  {
	    temp = pt_dbval_to_value (parser, &db_dest);
	    (void) db_value_clear (&db_dest);
	    if (!temp)
	      {
		err = ER_GENERIC_ERROR;
	      }
	    else
	      {
		temp->line_number = dest->line_number;
		temp->column_number = dest->column_number;
		temp->alias_print = dest->alias_print;
		*dest = *temp;
		if (data_type != NULL)
		  {
		    dest->data_type =
		      parser_copy_tree_list (parser, data_type);
		    if (dest->data_type == NULL)
		      {
			err = ER_GENERIC_ERROR;
		      }
		  }
		dest->next = dest_next;
		temp->info.value.db_value_is_in_workspace = 0;
		parser_free_node (parser, temp);
	      }
	  }
      }
      break;

    case PT_FUNCTION:
      if (src == dest)
	{
	  switch (src->info.function.function_type)
	    {
	    case F_SEQUENCE:
#if 0				/* TODO - */
	      assert (false);	/* should not reach here */
#endif
	      switch (desired_type)
		{
		case PT_TYPE_SEQUENCE:
		  dest->info.function.function_type = F_SEQUENCE;
		  dest->type_enum = PT_TYPE_SEQUENCE;
		  break;
		default:
		  break;
		}
	      break;

	    default:
	      break;
	    }
	}
      break;

    default:
      assert (err == NO_ERROR);
      break;
    }

  if (err == ER_IT_DATA_OVERFLOW)
    {
      PT_ERRORmf2 (parser, src, MSGCAT_SET_PARSER_SEMANTIC,
		   MSGCAT_SEMANTIC_OVERFLOW_COERCING_TO,
		   pt_short_print (parser, src),
		   pt_show_type_enum ((PT_TYPE_ENUM) desired_type));
    }
  else if (err < 0)
    {
      PT_ERRORmf2 (parser, src, MSGCAT_SET_PARSER_SEMANTIC,
		   MSGCAT_SEMANTIC_CANT_COERCE_TO,
		   pt_short_print (parser, src),
		   (desired_type == PT_TYPE_OBJECT
		    ? pt_class_name (data_type)
		    : pt_show_type_enum ((PT_TYPE_ENUM) desired_type)));
    }

  return err;
}


/*
 * pt_converse_op () - Figure out the converse of a relational operator,
 * 	so that we can flip a relational expression into a canonical form
 *   return:
 *   op(in):
 */

PT_OP_TYPE
pt_converse_op (PT_OP_TYPE op)
{
  switch (op)
    {
    case PT_EQ:
      return PT_EQ;
    case PT_LT:
      return PT_GT;
    case PT_LE:
      return PT_GE;
    case PT_GT:
      return PT_LT;
    case PT_GE:
      return PT_LE;
    case PT_NE:
      return PT_NE;
    case PT_NULLSAFE_EQ:
      return PT_NULLSAFE_EQ;
    default:
      return (PT_OP_TYPE) 0;
    }
}


/*
 * pt_is_between_range_op () -
 *   return:
 *   op(in):
 */
int
pt_is_between_range_op (PT_OP_TYPE op)
{
  switch (op)
    {
    case PT_BETWEEN_AND:
    case PT_BETWEEN_GE_LE:
    case PT_BETWEEN_GE_LT:
    case PT_BETWEEN_GT_LE:
    case PT_BETWEEN_GT_LT:
    case PT_BETWEEN_EQ_NA:
    case PT_BETWEEN_INF_LE:
    case PT_BETWEEN_INF_LT:
    case PT_BETWEEN_GE_INF:
    case PT_BETWEEN_GT_INF:
      return 1;
    default:
      return 0;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_is_comp_op () -
 *   return:
 *   op(in):
 */
int
pt_is_comp_op (PT_OP_TYPE op)
{
  switch (op)
    {
    case PT_LIKE:
    case PT_NOT_LIKE:
    case PT_IS_IN:
    case PT_IS_NOT_IN:
    case PT_IS_NULL:
    case PT_IS_NOT_NULL:
    case PT_IS:
    case PT_IS_NOT:
    case PT_EQ:
    case PT_NE:
    case PT_GE:
    case PT_GT:
    case PT_LT:
    case PT_LE:
    case PT_NULLSAFE_EQ:
    case PT_GT_INF:
    case PT_LT_INF:
    case PT_BETWEEN:
    case PT_BETWEEN_AND:
    case PT_BETWEEN_GE_LE:
    case PT_BETWEEN_GE_LT:
    case PT_BETWEEN_GT_LE:
    case PT_BETWEEN_GT_LT:
    case PT_BETWEEN_EQ_NA:
    case PT_BETWEEN_INF_LE:
    case PT_BETWEEN_INF_LT:
    case PT_BETWEEN_GE_INF:
    case PT_BETWEEN_GT_INF:
    case PT_RANGE:
      return 1;
    default:
      return 0;
    }
}
#endif

/*
 * pt_negate_op () -
 *   return:
 *   op(in):
 */
PT_OP_TYPE
pt_negate_op (PT_OP_TYPE op)
{
  switch (op)
    {
    case PT_EQ:
      return PT_NE;
    case PT_NE:
      return PT_EQ;
    case PT_GT:
      return PT_LE;
    case PT_GE:
      return PT_LT;
    case PT_LT:
      return PT_GE;
    case PT_LE:
      return PT_GT;
    case PT_BETWEEN:
      return PT_NOT_BETWEEN;
    case PT_NOT_BETWEEN:
      return PT_BETWEEN;
    case PT_IS_IN:
      return PT_IS_NOT_IN;
    case PT_IS_NOT_IN:
      return PT_IS_IN;
    case PT_LIKE:
      return PT_NOT_LIKE;
    case PT_NOT_LIKE:
      return PT_LIKE;
    case PT_RLIKE:
      return PT_NOT_RLIKE;
    case PT_NOT_RLIKE:
      return PT_RLIKE;
    case PT_RLIKE_BINARY:
      return PT_NOT_RLIKE_BINARY;
    case PT_NOT_RLIKE_BINARY:
      return PT_RLIKE_BINARY;
    case PT_IS_NULL:
      return PT_IS_NOT_NULL;
    case PT_IS_NOT_NULL:
      return PT_IS_NULL;
    case PT_IS:
      return PT_IS_NOT;
    case PT_IS_NOT:
      return PT_IS;
    default:
      return (PT_OP_TYPE) 0;
    }
}


/*
 * pt_comp_to_between_op () -
 *   return:
 *   left(in):
 *   right(in):
 *   type(in):
 *   between(out):
 */
int
pt_comp_to_between_op (PT_OP_TYPE left,
		       PT_OP_TYPE right,
		       PT_COMP_TO_BETWEEN_OP_CODE_TYPE
		       type, PT_OP_TYPE * between)
{
  size_t i;

  for (i = 0; i < COMPARE_BETWEEN_OPERATOR_COUNT; i++)
    {
      if (left == pt_Compare_between_operator_table[i].left
	  && right == pt_Compare_between_operator_table[i].right)
	{
	  *between = pt_Compare_between_operator_table[i].between;

	  return 0;
	}
    }

  if (type == PT_RANGE_INTERSECTION)
    {				/* range intersection */
      if ((left == PT_GE && right == PT_EQ)
	  || (left == PT_EQ && right == PT_LE))
	{
	  *between = PT_BETWEEN_EQ_NA;
	  return 0;
	}
    }

  return -1;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_between_to_comp_op () -
 *   return:
 *   between(in):
 *   left(out):
 *   right(out):
 */
int
pt_between_to_comp_op (PT_OP_TYPE between,
		       PT_OP_TYPE * left, PT_OP_TYPE * right)
{
  size_t i;

  for (i = 0; i < COMPARE_BETWEEN_OPERATOR_COUNT; i++)
    {
      if (between == pt_Compare_between_operator_table[i].between)
	{
	  *left = pt_Compare_between_operator_table[i].left;
	  *right = pt_Compare_between_operator_table[i].right;

	  return 0;
	}
    }

  return -1;
}
#endif

/*
 * pt_get_collation_info () - get the collation info of parse tree node
 *
 *   return: true if node has collation
 *   node(in): a parse tree node
 *   coll_infer(out): collation inference data
 */
bool
pt_get_collation_info (PT_NODE * node, int *collation_id)
{
  bool has_collation = false;

  assert (node != NULL);
  assert (collation_id != NULL);

  *collation_id = LANG_COERCIBLE_COLL;	/* init */

  if (node->data_type != NULL)
    {
      if (PT_HAS_COLLATION (node->type_enum))
	{
	  *collation_id = node->data_type->info.data_type.collation_id;
	  has_collation = true;
	}
    }
  else if (node->type_enum == PT_TYPE_MAYBE
	   || (node->node_type == PT_VALUE
	       && PT_HAS_COLLATION (node->type_enum)))
    {
      *collation_id = LANG_SYS_COLLATION;
      has_collation = true;
    }

  return has_collation;
}

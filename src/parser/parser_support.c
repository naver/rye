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
 * parser_support.c - Utility functions for parse trees
 */

#ident "$Id$"


#include "config.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <ctype.h>


#include "chartype.h"
#include "parser.h"
#include "parser_message.h"
#include "memory_alloc.h"
#include "intl_support.h"
#include "error_manager.h"
#include "work_space.h"
#include "oid.h"
#include "class_object.h"
#include "xasl_support.h"
#include "optimizer.h"
#include "transform.h"
#include "object_primitive.h"
#include "heap_file.h"
#include "object_representation.h"
#include "query_opfunc.h"
#include "parser_support.h"
#include "system_parameter.h"
#include "xasl_generation.h"
#include "schema_manager.h"
#include "object_print.h"

#define DEFAULT_VAR "."

struct pt_host_vars
{
  PT_NODE *inputs;
};

#define COMPATIBLE_WITH_INSTNUM(node) \
        (pt_is_expr_node(node) \
         && PT_EXPR_INFO_IS_FLAGED(node, PT_EXPR_INFO_INSTNUM_C))

#define NOT_COMPATIBLE_WITH_INSTNUM(node) \
        (pt_is_dot_node(node) || pt_is_attr(node) \
         || pt_is_correlated_subquery(node) \
         || (pt_is_expr_node(node) \
             && PT_EXPR_INFO_IS_FLAGED(node, PT_EXPR_INFO_INSTNUM_NC)))

#define COMPATIBLE_WITH_GROUPBYNUM(node) \
        ((pt_is_function(node) \
          && node->info.function.function_type == PT_GROUPBY_NUM) \
         || \
         (pt_is_expr_node(node) \
          && PT_EXPR_INFO_IS_FLAGED(node, PT_EXPR_INFO_GROUPBYNUM_C)))

#define NOT_COMPATIBLE_WITH_GROUPBYNUM(node) \
        (pt_is_dot_node(node) || pt_is_attr(node) || pt_is_query(node) \
         || (pt_is_expr_node(node) \
             && PT_EXPR_INFO_IS_FLAGED(node, PT_EXPR_INFO_GROUPBYNUM_NC)))

int qp_Packing_er_code = NO_ERROR;

#if !defined(NDEBUG)
static const int PACKING_MMGR_CHUNK_SIZE = 4;	/* for code coverage */
static const int PACKING_MMGR_BLOCK_SIZE = 1;	/* for code coverage */
#else
static const int PACKING_MMGR_CHUNK_SIZE = ONE_K;
static const int PACKING_MMGR_BLOCK_SIZE = 10;
#endif

static int packing_heap_num_slot = 0;
static HL_HEAPID *packing_heap = NULL;
static int packing_level = 0;

static void pt_free_packing_buf (int slot);

static bool pt_datatypes_match (const PT_NODE * a, const PT_NODE * b);
#if defined (ENABLE_UNUSED_FUNCTION)
static PT_NODE *pt_get_select_from_spec (const PT_NODE * spec);
static PT_NODE *pt_insert_host_var (PARSER_CONTEXT * parser, PT_NODE * h_var,
				    PT_NODE * list);
static PT_NODE *pt_collect_host_info (PARSER_CONTEXT * parser, PT_NODE * node,
				      void *h_var, int *continue_walk);

static void *regu_bytes_alloc (int length);
#endif
static void regu_dbvallist_init (QPROC_DB_VALUE_LIST ptr);
static void regu_var_init (REGU_VARIABLE * ptr);
static void regu_varlist_init (REGU_VARIABLE_LIST ptr);
static void regu_vallist_init (VAL_LIST * ptr);
static void regu_outlist_init (OUTPTR_LIST * ptr);
static void regu_pred_init (PRED_EXPR * ptr);
static ARITH_TYPE *regu_arith_no_value_alloc (void);
static void regu_arith_init (ARITH_TYPE * ptr);
static FUNCTION_TYPE *regu_function_alloc (void);
static void regu_func_init (FUNCTION_TYPE * ptr);
static AGGREGATE_TYPE *regu_aggregate_alloc (void);
static void regu_agg_init (AGGREGATE_TYPE * ptr);
static XASL_NODE *regu_xasl_alloc (PROC_TYPE type);
static void regu_xasl_node_init (XASL_NODE * ptr, PROC_TYPE type);
static ACCESS_SPEC_TYPE *regu_access_spec_alloc (TARGET_TYPE type);
static void regu_spec_init (ACCESS_SPEC_TYPE * ptr, TARGET_TYPE type);
static SORT_LIST *regu_sort_alloc (void);
static void regu_sort_list_init (SORT_LIST * ptr);
static void regu_init_oid (OID * oidptr);
static QFILE_LIST_ID *regu_listid_alloc (void);
static void regu_listid_init (QFILE_LIST_ID * ptr);
static void regu_srlistid_init (QFILE_SORTED_LIST_ID * ptr);
static void regu_domain_init (SM_DOMAIN * ptr);
static void regu_cache_attrinfo_init (HEAP_CACHE_ATTRINFO * ptr);
#if defined (ENABLE_UNUSED_FUNCTION)
static PT_NODE *pt_make_dotted_identifier (PARSER_CONTEXT * parser,
					   const char *identifier_str);
static PT_NODE *pt_make_dotted_identifier_internal (PARSER_CONTEXT * parser,
						    const char
						    *identifier_str,
						    int depth);
static PT_NODE *pt_make_pred_name_int_val (PARSER_CONTEXT * parser,
					   PT_OP_TYPE op_type,
					   const char *col_name,
					   const int int_value);
static PT_NODE *pt_make_pred_name_string_val (PARSER_CONTEXT * parser,
					      PT_OP_TYPE op_type,
					      const char *identifier_str,
					      const char *str_value);
static PT_NODE *pt_make_pred_with_identifiers (PARSER_CONTEXT * parser,
					       PT_OP_TYPE op_type,
					       const char *lhs_identifier,
					       const char *rhs_identifier);
static PT_NODE *pt_make_if_with_expressions (PARSER_CONTEXT * parser,
					     PT_NODE * pred,
					     PT_NODE * expr1,
					     PT_NODE * expr2,
					     const char *alias);
static PT_NODE *pt_make_if_with_strings (PARSER_CONTEXT * parser,
					 PT_NODE * pred,
					 const char *string1,
					 const char *string2,
					 const char *alias);
static PT_NODE *pt_make_like_col_expr (PARSER_CONTEXT * parser,
				       PT_NODE * rhs_expr,
				       const char *col_name);
static PT_NODE *pt_make_outer_select_for_show_stmt (PARSER_CONTEXT * parser,
						    PT_NODE * inner_select,
						    const char *select_alias);
static PT_NODE *pt_make_sort_spec_with_identifier (PARSER_CONTEXT * parser,
						   const char *identifier,
						   PT_MISC_TYPE sort_mode);
static PT_NODE *pt_make_sort_spec_with_number (PARSER_CONTEXT * parser,
					       const int number_pos,
					       PT_MISC_TYPE sort_mode);
static char *pt_help_show_create_table (PARSER_CONTEXT * parser,
					PT_NODE * table_name);
#endif
static int pt_get_query_limit_from_orderby_for (PARSER_CONTEXT * parser,
						PT_NODE * orderby_for,
						DB_VALUE * upper_limit,
						bool * has_limit);
static int pt_get_query_limit_from_limit (PARSER_CONTEXT * parser,
					  PT_NODE * limit,
					  DB_VALUE * limit_val);
#if defined (ENABLE_UNUSED_FUNCTION)
static PT_NODE *pt_is_spec_referenced (PARSER_CONTEXT * parser,
				       PT_NODE * node, void *void_arg,
				       int *continue_walk);
#endif

/*
 * pt_make_integer_value () -
 *   return:  return a PT_NODE for the integer value
 *   parser(in): parser context
 *   value_int(in): integer value to make up a PT_NODE
 */
PT_NODE *
pt_make_integer_value (PARSER_CONTEXT * parser, const int value_int)
{
  PT_NODE *node;

  node = parser_new_node (parser, PT_VALUE);
  if (node)
    {
      node->type_enum = PT_TYPE_INTEGER;
      node->info.value.data_value.i = value_int;
    }
  return node;
}

/*
 * pt_make_string_value () -
 *   return:  return a PT_NODE for the string value
 *   parser(in): parser context
 *   value_string(in): string value to make up a PT_NODE
 */

PT_NODE *
pt_make_string_value (PARSER_CONTEXT * parser, const char *value_string)
{
  PT_NODE *node;

  node = parser_new_node (parser, PT_VALUE);
  if (node)
    {
      if (value_string == NULL)
	{
	  PT_SET_NULL_NODE (node);
	}
      else
	{
	  node->info.value.data_value.str =
	    pt_append_bytes (parser, NULL, value_string,
			     strlen (value_string));
	  node->type_enum = PT_TYPE_VARCHAR;
	  node->info.value.string_type = ' ';
	  PT_NODE_PRINT_VALUE_TO_TEXT (parser, node);
	}
    }

  return node;
}

/*
 * pt_and () - Create a PT_AND node with arguments of the nodes passed in
 *   return:
 *   parser(in):
 *   arg1(in):
 *   arg2(in):
 */
PT_NODE *
pt_and (PARSER_CONTEXT * parser, const PT_NODE * arg1, const PT_NODE * arg2)
{
  PT_NODE *node;

  node = parser_new_node (parser, PT_EXPR);
  if (node)
    {
      parser_init_node (node);
      node->info.expr.op = PT_AND;
      node->info.expr.arg1 = (PT_NODE *) arg1;
      node->info.expr.arg2 = (PT_NODE *) arg2;
    }

  return node;
}

/*
 * pt_union () - Create a PT_UNION node with arguments of the nodes passed in
 *   return:
 *   parser(in):
 *   arg1(in/out):
 *   arg2(in/out):
 */
PT_NODE *
pt_union (PARSER_CONTEXT * parser, PT_NODE * arg1, PT_NODE * arg2)
{
  PT_NODE *node;
  int arg1_corr = 0, arg2_corr = 0, corr;

  node = parser_new_node (parser, PT_UNION);

  if (node)
    {
      parser_init_node (node);
      /* set query id # */
      node->info.query.id = (UINTPTR) node;

      node->info.query.q.union_.arg1 = arg1;
      node->info.query.q.union_.arg2 = arg2;

      if (arg1)
	{
	  arg1->info.query.is_subquery = PT_IS_UNION_SUBQUERY;
	  arg1_corr = arg1->info.query.correlation_level;
	}
      if (arg2)
	{
	  arg2->info.query.is_subquery = PT_IS_UNION_SUBQUERY;
	  arg2_corr = arg2->info.query.correlation_level;
	}
      if (arg1_corr)
	{
	  corr = arg1_corr;
	  if (arg2_corr && arg2_corr < corr)
	    {
	      corr = arg2_corr;
	    }
	}
      else
	{
	  corr = arg2_corr;
	}

      if (corr)
	{
	  corr--;
	}

      node->info.query.correlation_level = corr;
    }

  return node;
}

/*
 * pt_name () - Create a PT_NAME node using the name string passed in
 *   return:
 *   parser(in):
 *   name(in):
 */
PT_NODE *
pt_name (PARSER_CONTEXT * parser, const char *name)
{
  PT_NODE *node;

  node = parser_new_node (parser, PT_NAME);
  if (node)
    {
      parser_init_node (node);
      node->info.name.original = pt_append_string (parser, NULL, name);
    }

  return node;
}

/*
 * pt_table_option () - Create a PT_TABLE_OPTION node
 *   return: the new node or NULL on error
 *   parser(in):
 *   option(in): the type of the table option
 *   val(in): a value associated with the table option or NULL
 */
PT_NODE *
pt_table_option (PARSER_CONTEXT * parser, const PT_TABLE_OPTION_TYPE option,
		 PT_NODE * val)
{
  PT_NODE *node;

  node = parser_new_node (parser, PT_TABLE_OPTION);
  if (node)
    {
      parser_init_node (node);
      node->info.table_option.option = option;
      node->info.table_option.val = val;
    }

  return node;
}

/*
 * pt_expression () - Create a PT_EXPR node using the arguments passed in
 *   return:
 *   parser(in):
 *   op(in): the expression operation type
 *   arg1(in):
 *   arg2(in):
 *   arg3(in):
 */
PT_NODE *
pt_expression (PARSER_CONTEXT * parser, PT_OP_TYPE op, PT_NODE * arg1,
	       PT_NODE * arg2, PT_NODE * arg3)
{
  PT_NODE *node;

  node = parser_new_node (parser, PT_EXPR);
  if (node)
    {
      parser_init_node (node);
      node->info.expr.op = op;
      node->info.expr.arg1 = arg1;
      node->info.expr.arg2 = arg2;
      node->info.expr.arg3 = arg3;
    }

  return node;
}

PT_NODE *
pt_expression_0 (PARSER_CONTEXT * parser, PT_OP_TYPE op)
{
  return pt_expression (parser, op, NULL, NULL, NULL);
}

PT_NODE *
pt_expression_1 (PARSER_CONTEXT * parser, PT_OP_TYPE op, PT_NODE * arg1)
{
  return pt_expression (parser, op, arg1, NULL, NULL);
}

PT_NODE *
pt_expression_2 (PARSER_CONTEXT * parser, PT_OP_TYPE op, PT_NODE * arg1,
		 PT_NODE * arg2)
{
  return pt_expression (parser, op, arg1, arg2, NULL);
}

#if defined (ENABLE_UNUSED_FUNCTION)
PT_NODE *
pt_expression_3 (PARSER_CONTEXT * parser, PT_OP_TYPE op, PT_NODE * arg1,
		 PT_NODE * arg2, PT_NODE * arg3)
{
  return pt_expression (parser, op, arg1, arg2, arg3);
}
#endif

/*
 * pt_node_list () - Create a PT_NODE_LIST node using the arguments passed in
 *   return:
 *   parser(in):
 *   list_type(in):
 *   list(in):
 */
PT_NODE *
pt_node_list (PARSER_CONTEXT * parser, PT_MISC_TYPE list_type, PT_NODE * list)
{
  PT_NODE *node;

  if (list != NULL && list->node_type == PT_NODE_LIST)
    {
      assert (false);		/* not permit nesting */
      return NULL;
    }

  node = parser_new_node (parser, PT_NODE_LIST);
  if (node)
    {
      node->info.node_list.list_type = list_type;
      node->info.node_list.list = list;
    }

  return node;
}

/*
 * pt_entity () - Create a PT_SPEC node using the node string passed
 *                for the entity name
 *   return:
 *   parser(in):
 *   entity_name(in):
 *   range_var(in):
 *   flat_list(in):
 */
PT_NODE *
pt_entity (PARSER_CONTEXT * parser, const PT_NODE * entity_name,
	   const PT_NODE * range_var, const PT_NODE * flat_list)
{
  PT_NODE *node;

  assert (flat_list == NULL || flat_list->next == NULL);

  node = parser_new_node (parser, PT_SPEC);
  if (node)
    {
      parser_init_node (node);
      node->info.spec.entity_name = (PT_NODE *) entity_name;
      node->info.spec.range_var = (PT_NODE *) range_var;
      node->info.spec.flat_entity_list = (PT_NODE *) flat_list;
    }

  return node;
}

/*
 * pt_datatypes_match () -
 *   return:  1 if the two data types are not virtual objects or the same
 * 	      class of virtual object.  0 otherwise.
 *   a(in):
 *   b(in): data types to compare
 */

static bool
pt_datatypes_match (const PT_NODE * a, const PT_NODE * b)
{
  if (!a && !b)
    {
      return true;		/* both non objects, ok */
    }
  if (!a || !b)
    {
      return true;		/* typed and untyped node, ignore difference */
    }
  if (a->type_enum != PT_TYPE_OBJECT && b->type_enum != PT_TYPE_OBJECT)
    {
      return true;		/* both non objects again, ok */
    }

  if (a->type_enum != PT_TYPE_OBJECT || b->type_enum != PT_TYPE_OBJECT)
    {
      return false;
    }

  /* both the same flavor virtual objects */
  return true;
}

/*
 * pt_name_equal () - Tests name nodes for equality
 *   return: true on equal
 *   parser(in):
 *   name1(in):
 *   name2(in):
 *
 * Note :
 * Assumes semantic processing has resolved name information
 */
bool
pt_name_equal (UNUSED_ARG PARSER_CONTEXT * parser, const PT_NODE * name1,
	       const PT_NODE * name2)
{
  if (!name1 || !name2)
    {
      return false;
    }

  CAST_POINTER_TO_NODE (name1);
  CAST_POINTER_TO_NODE (name2);

  if (name1->node_type != PT_NAME)
    {
      return false;
    }

  if (name2->node_type != PT_NAME)
    {
      return false;
    }

  /* identity */
  if (name1 == name2)
    {
      return true;
    }

  /* are the id's equal? */
  if (name1->info.name.spec_id != name2->info.name.spec_id)
    {
      return false;
    }

  /* raw names the same? (NULL not allowed here) */
  if (!name1->info.name.original)
    {
      return false;
    }
  if (!name2->info.name.original)
    {
      return false;
    }

  if (name1->info.name.meta_class != name2->info.name.meta_class)
    {
      if (name1->info.name.meta_class != PT_NORMAL)
	{
	  return false;
	}
      if (name2->info.name.meta_class != PT_NORMAL)
	{
	  return false;
	}
    }

  if (intl_identifier_casecmp (name1->info.name.original,
			       name2->info.name.original) != 0)
    {
      return false;
    }


  if (!pt_datatypes_match (name1->data_type, name2->data_type))
    {
      return false;
    }

  return true;
}

/*
 * pt_find_name () - Looks for a name on a list
 *   return:
 *   parser(in):
 *   name(in):
 *   list(in):
 */
PT_NODE *
pt_find_name (PARSER_CONTEXT * parser, const PT_NODE * name,
	      const PT_NODE * list)
{
  while (list)
    {
      if (pt_name_equal (parser, name, list))
	{
	  return (PT_NODE *) list;
	}
      list = list->next;
    }

  return NULL;
}

/*
 * pt_is_aggregate_function () -
 *   return: true in arg if node is a PT_FUNCTION
 * 	     node with a PT_MIN, PT_MAX, PT_SUM, PT_AVG, or PT_COUNT type
 *   parser(in):
 *   node(in):
 */
bool
pt_is_aggregate_function (UNUSED_ARG PARSER_CONTEXT * parser,
			  const PT_NODE * node)
{
  FUNC_TYPE function_type;

  if (node->node_type == PT_FUNCTION)
    {
      function_type = node->info.function.function_type;
      if (function_type == PT_MIN
	  || function_type == PT_MAX
	  || function_type == PT_SUM
	  || function_type == PT_AVG
	  || function_type == PT_STDDEV
	  || function_type == PT_STDDEV_POP
	  || function_type == PT_STDDEV_SAMP
	  || function_type == PT_VARIANCE
	  || function_type == PT_VAR_POP
	  || function_type == PT_VAR_SAMP
	  || function_type == PT_GROUPBY_NUM
	  || function_type == PT_COUNT
	  || function_type == PT_COUNT_STAR
	  || function_type == PT_GROUP_CONCAT)
	{
	  return true;
	}
    }

  return false;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * pt_is_expr_wrapped_function () -
 *   return: true if node is a PT_FUNCTION node with which may be evaluated
 *	     like an expression
 *   parser(in): parser context
 *   node(in): PT_FUNTION node
 */
bool
pt_is_expr_wrapped_function (PARSER_CONTEXT * parser, const PT_NODE * node)
{
  FUNC_TYPE function_type;

  if (node->node_type == PT_FUNCTION)
    {
      function_type = node->info.function.function_type;
      if (function_type == F_INSERT_SUBSTRING || function_type == F_ELT)
	{
	  return true;
	}
    }

  return false;
}
#endif

/*
 * pt_find_spec_in_statement () - find the node spec in given statement
 *   return: the spec with same id as the name, or NULL
 *   parser(in):
 *   stmt(in):
 *   name(in):
 */
PT_NODE *
pt_find_spec_in_statement (PARSER_CONTEXT * parser, const PT_NODE * stmt,
			   const PT_NODE * name)
{
  PT_NODE *spec = NULL;

  switch (stmt->node_type)
    {
    case PT_SPEC:
      assert (false);		/* TODO - trace */
      spec = pt_find_spec (parser, stmt, name);
      break;

    case PT_DELETE:
      spec = pt_find_spec (parser, stmt->info.delete_.spec, name);
      break;

    case PT_UPDATE:
      spec = pt_find_spec (parser, stmt->info.update.spec, name);
      break;

    default:
      assert (false);		/* TODO - trace */
      break;
    }

  return (PT_NODE *) spec;
}

/*
 * pt_find_spec () -
 *   return: the spec in the from list with same id as the name, or NULL
 *   parser(in):
 *   from(in):
 *   name(in):
 */
PT_NODE *
pt_find_spec (UNUSED_ARG PARSER_CONTEXT * parser, const PT_NODE * from,
	      const PT_NODE * name)
{
  PT_NODE *spec = NULL;

  assert (name != NULL);

  if (name != NULL)
    {
      for (spec = (PT_NODE *) from; spec; spec = spec->next)
	{
	  if (spec->info.spec.id == name->info.name.spec_id)
	    {
	      break;
	    }
	}
    }

  return spec;
}

/*
 * pt_find_aggregate_names - find names within select_stack
 *  returns: unmodified tree
 *  parser(in): parser context
 *  tree(in): tree to search into
 *  arg(in/out): a PT_AGG_NAME_INFO structure
 *  continue_walk(in/out): walk type
 *
 * NOTE: this function is called on an aggregate function or it's arguments
 * and it returns the maximum level within the stack that owns PT_NAMEs within
 * the called-on tree.
 */
PT_NODE *
pt_find_aggregate_names (PARSER_CONTEXT * parser, PT_NODE * tree,
			 void *arg, int *continue_walk)
{
  PT_AGG_NAME_INFO *info = (PT_AGG_NAME_INFO *) arg;
  PT_NODE *node = NULL, *select_stack;
  int level = 0;

  switch (tree->node_type)
    {
    case PT_SELECT:
      *continue_walk = PT_LIST_WALK;
      break;

    case PT_DOT_:
      assert (false);		/* TODO - trace */
      node = tree->info.dot.arg2;
      break;

    case PT_NAME:
      node = tree;
      break;

    default:
      break;
    }

  if (node == NULL || node->node_type != PT_NAME)
    {
      /* nothing to do */
      return tree;
    }
  else
    {
      info->name_count++;
    }

  select_stack = info->select_stack;
  while (select_stack != NULL)
    {
      PT_NODE *select = select_stack->info.pointer.node;

      if (select == NULL || select->node_type != PT_SELECT)
	{
	  PT_INTERNAL_ERROR (parser, "stack entry is not SELECT");
	  return tree;
	}

      if (level > info->max_level
	  && pt_find_spec (parser, select->info.query.q.select.from, node))
	{
	  /* found! */
	  info->max_level = level;
	}

      /* next stack level */
      select_stack = select_stack->next;
      level++;
    }

  return tree;
}

/*
 * pt_find_aggregate_functions_pre () - finds aggregate functions in a tree
 *  returns: unmodified tree
 *  parser(in): parser context
 *  tree(in): tree to search into
 *  arg(in/out): a PT_AGG_FIND_INFO structure
 *  continue_walk(in/out): walk type
 *
 * NOTE: this routine searches for aggregate functions that belong to the
 * SELECT statement at the base of the stack
 */
PT_NODE *
pt_find_aggregate_functions_pre (PARSER_CONTEXT * parser, PT_NODE * tree,
				 void *arg, int *continue_walk)
{
  PT_AGG_FIND_INFO *info = (PT_AGG_FIND_INFO *) arg;
  PT_NODE *select_stack = info->select_stack;

  if (tree == NULL)
    {
      /* nothing to do */
      return tree;
    }

  if (pt_is_aggregate_function (parser, tree))
    {
      if (tree->info.function.function_type == PT_COUNT_STAR
	  || tree->info.function.function_type == PT_GROUPBY_NUM)
	{
	  /* found count(*), groupby_num() */
	  if (select_stack == NULL)
	    {
	      /* no spec stack, this was not called on a select */
	      info->out_of_context_count++;
	    }
	  else if (select_stack->next == NULL)
	    {
	      /* first level on spec stack, this function belongs to the
	         callee statement */
	      info->base_count++;
	    }
	}
      else
	{
	  PT_AGG_NAME_INFO name_info;
	  name_info.select_stack = info->select_stack;
	  name_info.max_level = -1;
	  name_info.name_count = 0;

	  (void) parser_walk_tree (parser, tree->info.function.arg_list,
				   pt_find_aggregate_names, &name_info,
				   pt_continue_walk, NULL);

	  if (name_info.max_level == 0)
	    {
	      /* only names from base SELECT were found */
	      info->base_count++;
	    }
	  else if (name_info.max_level < 0 && name_info.name_count > 0)
	    {
	      /* no names within stack limit were found */
	      info->out_of_context_count++;
	    }
	  else if (name_info.name_count == 0)
	    {
	      /* no names were found at all */
	      if (select_stack == NULL)
		{
		  info->out_of_context_count++;
		}
	      else if (select_stack->next == NULL)
		{
		  info->base_count++;
		}
	    }
	}
    }
  else if (tree->node_type == PT_SELECT)
    {
      /* we must evaluate nexts before pushing SELECT on stack */
      if (tree->next)
	{
	  (void) parser_walk_tree (parser, tree->next,
				   pt_find_aggregate_functions_pre, info,
				   pt_find_aggregate_functions_post, info);
	  *continue_walk = PT_LEAF_WALK;
	}

      /* don't walk selects unless necessary */
      if (info->stop_on_subquery)
	{
	  *continue_walk = PT_STOP_WALK;
	}

      /* stack push */
      info->select_stack =
	pt_pointer_stack_push (parser, info->select_stack, tree);
    }

  return tree;
}

/*
 * pt_find_aggregate_functions_post () - finds aggregate functions in a tree
 *  returns: unmodified tree
 *  parser(in): parser context
 *  tree(in): tree to search into
 *  arg(in/out): a PT_AGG_FIND_INFO structure
 *  continue_walk(in/out): walk type
 */
PT_NODE *
pt_find_aggregate_functions_post (PARSER_CONTEXT * parser, PT_NODE * tree,
				  void *arg, int *continue_walk)
{
  PT_AGG_FIND_INFO *info = (PT_AGG_FIND_INFO *) arg;

  if (tree->node_type == PT_SELECT)
    {
      info->select_stack =
	pt_pointer_stack_pop (parser, info->select_stack, NULL);
    }

  /* nothing can stop us! */
  *continue_walk = PT_CONTINUE_WALK;

  return tree;
}

/*
 * pt_has_non_idx_sarg_coll_pre () - pre function for determining if a tree has
 *				     contains a node with a collation that
 *				     renders it unusable for key range/filter
 *   returns: input node
 *   parser(in): parser to use
 *   tree(in): tree node to analyze
 *   arg(out): integer, will be set to "1" if node is found unfit
 *   continue_walk(out): to be set to PT_STOP_WALK where necessary
 */
PT_NODE *
pt_has_non_idx_sarg_coll_pre (UNUSED_ARG PARSER_CONTEXT * parser,
			      PT_NODE * tree, void *arg, int *continue_walk)
{
  int *mark = (int *) arg;

  assert (tree != NULL);
  assert (arg != NULL);
  assert (continue_walk != NULL);

  if (PT_HAS_COLLATION (tree->type_enum) && (tree->data_type != NULL))
    {
      int collation_id = tree->data_type->info.data_type.collation_id;
      LANG_COLLATION *lang_coll = lang_get_collation (collation_id);

      if (!lang_coll->options.allow_index_opt)
	{
	  assert (false);	/* TODO - trace */
	  *mark = 1;
	  *continue_walk = PT_STOP_WALK;
	}
    }

  return tree;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_is_inst_or_orderby_num_node_post () -
 *   return:
 *   parser(in):
 *   tree(in):
 *   arg(in/out):
 *   continue_walk(in/out):
 */
PT_NODE *
pt_is_inst_or_orderby_num_node_post (PARSER_CONTEXT * parser, PT_NODE * tree,
				     void *arg, int *continue_walk)
{
  bool *has_inst_orderby_num = (bool *) arg;

  if (*has_inst_orderby_num)
    {
      *continue_walk = PT_STOP_WALK;
    }
  else
    {
      *continue_walk = PT_CONTINUE_WALK;
    }

  return tree;
}

/*
 * pt_is_inst_or_orderby_num_node () -
 *   return:
 *   parser(in):
 *   tree(in):
 *   arg(in/out): true if node is an INST_NUM or ORDERBY_NUM expression node
 *   continue_walk(in/out):
 */
PT_NODE *
pt_is_inst_or_orderby_num_node (PARSER_CONTEXT * parser, PT_NODE * tree,
				void *arg, int *continue_walk)
{
  bool *has_inst_orderby_num = (bool *) arg;

  if (PT_IS_INSTNUM (tree) || PT_IS_ORDERBYNUM (tree))
    {
      *has_inst_orderby_num = true;
    }

  if (*has_inst_orderby_num)
    {
      *continue_walk = PT_STOP_WALK;
    }
  else if (PT_IS_QUERY_NODE_TYPE (tree->node_type))
    {
      *continue_walk = PT_LIST_WALK;
    }

  return tree;
}
#endif

/*
 * pt_is_ddl_statement () - test PT_NODE statement types,
 * 			    without exposing internals
 *   return:
 *   node(in):
 */
int
pt_is_ddl_statement (const PT_NODE * node)
{
  return STMT_TYPE_IS_DDL (pt_node_to_stmt_type (node));
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_is_dml_statement () - test PT_NODE statement types,
 *                          without exposing internals
 *   return:
 *   node(in):
 */
int
pt_is_dml_statement (const PT_NODE * node)
{
  if (node)
    {
      switch (node->node_type)
	{
	case PT_INSERT:
	case PT_DELETE:
	case PT_UPDATE:
	  return true;

	case PT_SELECT:
	  if (PT_SELECT_INFO_IS_FLAGED (node, PT_SELECT_INFO_FOR_UPDATE))
	    {
	      return true;
	    }
	  break;

	default:
	  break;
	}
    }
  return false;
}
#endif

/*
 * pt_is_attr () -
 *   return:
 *   node(in/out):
 */
int
pt_is_attr (PT_NODE * node)
{
  if (node == NULL)
    {
      return false;
    }

  node = pt_get_end_path_node (node);

  if (node->node_type == PT_NAME)
    {
      if (node->info.name.meta_class == PT_NORMAL
	  || node->info.name.meta_class == PT_OID_ATTR)
	{
	  return true;
	}
    }

  return false;
}

/*
 * pt_instnum_compatibility () -
 *   return:
 *   expr(in/out):
 */
int
pt_instnum_compatibility (PT_NODE * expr)
{
  PT_NODE *arg1 = NULL, *arg2 = NULL, *arg3 = NULL;

  if (expr->node_type != PT_EXPR)
    {
      return true;
    }

  /* attr and subquery is not compatible with inst_num() */

  if (expr->info.expr.op != PT_IF)
    {
      arg1 = expr->info.expr.arg1;
      if (arg1)
	{
	  if (COMPATIBLE_WITH_INSTNUM (arg1))
	    {
	      PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_INSTNUM_C);
	    }
	  if (NOT_COMPATIBLE_WITH_INSTNUM (arg1))
	    {
	      PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_INSTNUM_NC);
	    }
	}
    }

  arg2 = expr->info.expr.arg2;
  if (arg2)
    {
      if (COMPATIBLE_WITH_INSTNUM (arg2))
	{
	  PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_INSTNUM_C);
	}
      if (NOT_COMPATIBLE_WITH_INSTNUM (arg2))
	{
	  PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_INSTNUM_NC);
	}
    }

  if (expr->info.expr.op != PT_CASE && expr->info.expr.op != PT_DECODE)
    {
      arg3 = expr->info.expr.arg3;
      if (arg3)
	{
	  if (COMPATIBLE_WITH_INSTNUM (arg3))
	    {
	      PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_INSTNUM_C);
	    }
	  if (NOT_COMPATIBLE_WITH_INSTNUM (arg3))
	    {
	      PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_INSTNUM_NC);
	    }
	}
    }

  switch (expr->info.expr.op)
    {
    case PT_AND:
      /* AND hides inst_num() compatibility */
      return true;
    case PT_IS_NULL:
    case PT_IS_NOT_NULL:
    case PT_EXISTS:
    case PT_ASSIGN:
    case PT_IFNULL:
      /* those operator cannot have inst_num() */
      PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_INSTNUM_NC);
      break;
    default:
      break;
    }

  /* detect semantic error in pt_semantic_check_local */
  if (PT_EXPR_INFO_IS_FLAGED (expr, PT_EXPR_INFO_INSTNUM_NC))
    {
      if (expr->info.expr.op != PT_IF)
	{
	  if (arg1 && pt_is_instnum (arg1))
	    {
	      PT_EXPR_INFO_SET_FLAG (arg1, PT_EXPR_INFO_INSTNUM_NC);
	    }
	}
      if (arg2 && pt_is_instnum (arg2))
	{
	  PT_EXPR_INFO_SET_FLAG (arg2, PT_EXPR_INFO_INSTNUM_NC);
	}
      if (expr->info.expr.op != PT_CASE && expr->info.expr.op != PT_DECODE)
	{
	  if (arg3 && pt_is_instnum (arg3))
	    {
	      PT_EXPR_INFO_SET_FLAG (arg3, PT_EXPR_INFO_INSTNUM_NC);
	    }
	}
    }

  /* expression is not compatible with inst_num() */
  if (PT_EXPR_INFO_IS_FLAGED (expr, PT_EXPR_INFO_INSTNUM_C)
      && PT_EXPR_INFO_IS_FLAGED (expr, PT_EXPR_INFO_INSTNUM_NC))
    {
      /* to prevent repeated error */
      PT_EXPR_INFO_CLEAR_FLAG (expr, PT_EXPR_INFO_INSTNUM_C);
      return false;
    }

  return true;
}

/*
 * pt_groupbynum_compatibility () -
 *   return:
 *   expr(in):
 */
int
pt_groupbynum_compatibility (PT_NODE * expr)
{
  PT_NODE *arg1, *arg2, *arg3;

  if (expr->node_type != PT_EXPR)
    {
      return true;
    }

  /* attr and subquery is not compatible with groupby_num() */
  arg1 = expr->info.expr.arg1;
  if (arg1)
    {
      if (COMPATIBLE_WITH_GROUPBYNUM (arg1))
	{
	  PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_GROUPBYNUM_C);
	}
      if (NOT_COMPATIBLE_WITH_GROUPBYNUM (arg1))
	{
	  PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_GROUPBYNUM_NC);
	}
    }

  arg2 = expr->info.expr.arg2;
  if (arg2)
    {
      if (COMPATIBLE_WITH_GROUPBYNUM (arg2))
	{
	  PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_GROUPBYNUM_C);
	}
      if (NOT_COMPATIBLE_WITH_GROUPBYNUM (arg2))
	{
	  PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_GROUPBYNUM_NC);
	}
    }

  arg3 = expr->info.expr.arg3;
  if (arg3)
    {
      if (COMPATIBLE_WITH_GROUPBYNUM (arg3))
	{
	  PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_GROUPBYNUM_C);
	}
      if (NOT_COMPATIBLE_WITH_GROUPBYNUM (arg3))
	{
	  PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_GROUPBYNUM_NC);
	}
    }

  switch (expr->info.expr.op)
    {
    case PT_AND:
      /* AND hides groupby_num() compatibility */
      return true;
    case PT_IS_NULL:
    case PT_IS_NOT_NULL:
    case PT_EXISTS:
    case PT_ASSIGN:
      /* those operator cannot have groupby_num() */
      PT_EXPR_INFO_SET_FLAG (expr, PT_EXPR_INFO_GROUPBYNUM_NC);
      break;
    default:
      break;
    }

  /* expression is not compatible with groupby_num() */
  if (PT_EXPR_INFO_IS_FLAGED (expr, PT_EXPR_INFO_GROUPBYNUM_C)
      && PT_EXPR_INFO_IS_FLAGED (expr, PT_EXPR_INFO_GROUPBYNUM_NC))
    {
      /* to prevent repeated error */
      PT_EXPR_INFO_CLEAR_FLAG (expr, PT_EXPR_INFO_GROUPBYNUM_C);
      return false;
    }

  return true;
}

/*
 * pt_check_instnum_pre () - Identify if the expression tree has inst_num()
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in/out):
 */
PT_NODE *
pt_check_instnum_pre (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		      UNUSED_ARG void *arg, int *continue_walk)
{
  if (node->node_type == PT_SELECT)
    {
      *continue_walk = PT_STOP_WALK;
    }

  return node;
}

/*
 * pt_check_instnum_post () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in/out):
 *   continue_walk(in/out):
 */
PT_NODE *
pt_check_instnum_post (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		       void *arg, int *continue_walk)
{
  bool *inst_num = (bool *) arg;

  if (node->node_type == PT_EXPR
      && (node->info.expr.op == PT_INST_NUM
	  || node->info.expr.op == PT_ROWNUM))
    {
      *inst_num = true;
    }

  if (node->node_type == PT_SELECT)
    {
      *continue_walk = PT_CONTINUE_WALK;
    }

  return node;
}

/*
 * pt_check_groupbynum_pre () - Identify if the expression has groupby_num()
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in/out):
 */
PT_NODE *
pt_check_groupbynum_pre (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
			 UNUSED_ARG void *arg, int *continue_walk)
{
  if (node->node_type == PT_SELECT)
    {
      *continue_walk = PT_STOP_WALK;
    }

  return node;
}

/*
 * pt_check_groupbynum_post () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in/out):
 *   continue_walk(in/out):
 */
PT_NODE *
pt_check_groupbynum_post (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
			  void *arg, int *continue_walk)
{
  bool *grby_num = (bool *) arg;

  if (node->node_type == PT_FUNCTION
      && node->info.function.function_type == PT_GROUPBY_NUM)
    {
      *grby_num = true;
    }

  if (node->node_type == PT_SELECT)
    {
      *continue_walk = PT_CONTINUE_WALK;
    }

  return node;
}

/*
 * pt_check_orderbynum_pre () - Identify if the expression has orderby_num()
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in/out):
 */
PT_NODE *
pt_check_orderbynum_pre (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
			 UNUSED_ARG void *arg, int *continue_walk)
{
  if (node->node_type == PT_SELECT)
    {
      *continue_walk = PT_STOP_WALK;
    }

  return node;
}

/*
 * pt_check_orderbynum_post () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in/out):
 *   continue_walk(in/out):
 */
PT_NODE *
pt_check_orderbynum_post (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
			  void *arg, int *continue_walk)
{
  bool *ordby_num = (bool *) arg;

  if (node->node_type == PT_EXPR && node->info.expr.op == PT_ORDERBY_NUM)
    {
      *ordby_num = true;
    }

  if (node->node_type == PT_SELECT)
    {
      *continue_walk = PT_CONTINUE_WALK;
    }

  return node;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_expr_disallow_op_pre () - looks if the expression op is in the list
 *				  given as argument and throws an error if
 *				  found
 *
 * return: node
 * parser(in):
 * node(in):
 * arg(in): integer list with forbidden operators. arg[0] keeps the number of
 *	    operators
 * continue_wals (in/out):
 */
PT_NODE *
pt_expr_disallow_op_pre (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
			 int *continue_walk)
{
  int *op_list = (int *) arg;
  int i;

  if (!PT_IS_EXPR_NODE (node))
    {
      return node;
    }

  if (*continue_walk == PT_STOP_WALK)
    {
      return node;
    }

  assert (op_list != NULL);

  for (i = 1; i <= op_list[0]; i++)
    {
      if ((PT_NODE_TYPE) op_list[i] == node->info.expr.op)
	{
	  PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
		      MSGCAT_SEMANTIC_NOT_ALLOWED_HERE,
		      pt_show_binopcode (node->info.expr.op));
	}
    }
  return node;
}

/*
 * pt_arg1_part () - returns arg1 for union, intersection or difference
 *   return:
 *   node(in):
 */
PT_NODE *
pt_arg1_part (const PT_NODE * node)
{
  if (node
      && (node->node_type == PT_INTERSECTION
	  || node->node_type == PT_DIFFERENCE || node->node_type == PT_UNION))
    {
      return node->info.query.q.union_.arg1;
    }

  return NULL;
}

/*
 * pt_arg2_part () - returns arg2 for union, intersection or difference
 *   return:
 *   node(in):
 */
PT_NODE *
pt_arg2_part (const PT_NODE * node)
{
  if (node
      && (node->node_type == PT_INTERSECTION
	  || node->node_type == PT_DIFFERENCE || node->node_type == PT_UNION))
    {
      return node->info.query.q.union_.arg2;
    }

  return NULL;
}

/*
 * pt_select_list_part () - returns select list from select statement
 *   return:
 *   node(in):
 */
PT_NODE *
pt_select_list_part (const PT_NODE * node)
{
  if (node && node->node_type == PT_SELECT)
    {
      return node->info.query.q.select.list;
    }

  return NULL;
}

/*
 * pt_from_list_part () - returns from list from select statement
 *   return:
 *   node(in):
 */
PT_NODE *
pt_from_list_part (const PT_NODE * node)
{
  if (node && node->node_type == PT_SELECT)
    {
      return node->info.query.q.select.from;
    }

  return NULL;
}

/*
 * pt_where_part () - returns where part from select statement
 *   return:
 *   node(in):
 */
PT_NODE *
pt_where_part (const PT_NODE * node)
{
  if (node)
    {
      if (node->node_type == PT_SELECT)
	{
	  return node->info.query.q.select.where;
	}

      if (node->node_type == PT_UPDATE)
	{
	  return node->info.update.search_cond;
	}

      if (node->node_type == PT_DELETE)
	{
	  return node->info.delete_.search_cond;
	}
    }

  return NULL;
}

/*
 * pt_order_by_part () - returns order by part from select statement
 *   return:
 *   node(in):
 */
PT_NODE *
pt_order_by_part (const PT_NODE * node)
{
  if (node && node->node_type == PT_SELECT)
    {
      return node->info.query.order_by;
    }

  return NULL;
}

/*
 * pt_group_by_part () - returns group by part from select statement
 *   return:
 *   node(in):
 */
PT_NODE *
pt_group_by_part (const PT_NODE * node)
{
  if (node && node->node_type == PT_SELECT)
    {
      return node->info.query.q.select.group_by;
    }

  return NULL;
}

/*
 * pt_having_part () - returns having part from select statement
 *   return:
 *   node(in):
 */
PT_NODE *
pt_having_part (const PT_NODE * node)
{
  if (node && node->node_type == PT_SELECT)
    {
      return node->info.query.q.select.having;
    }

  return NULL;
}

/*
 * pt_left_part () - returns arg1 for PT_DOT_ and PT_EXPR
 *   return:
 *   node(in):
 */
PT_NODE *
pt_left_part (const PT_NODE * node)
{
  if (node == NULL)
    {
      return NULL;
    }
  if (node->node_type == PT_EXPR)
    {
      return node->info.expr.arg1;
    }
  if (node->node_type == PT_DOT_)
    {
      return node->info.dot.arg1;
    }
  return NULL;
}

/*
 * pt_right_part () - returns arg2 for PT_DOT_ and PT_EXPR
 *   return:
 *   node(in):
 */
PT_NODE *
pt_right_part (const PT_NODE * node)
{
  if (node == NULL)
    {
      return NULL;
    }
  if (node->node_type == PT_EXPR)
    {
      return node->info.expr.arg2;
    }
  if (node->node_type == PT_DOT_)
    {
      return node->info.dot.arg2;
    }
  return NULL;
}
#endif

/*
 * pt_get_end_path_node () -
 *   return: the original name node at the end of a path expression
 *   node(in):
 */
PT_NODE *
pt_get_end_path_node (PT_NODE * node)
{
  while (node != NULL && node->node_type == PT_DOT_)
    {
      node = node->info.dot.arg2;
    }
  return node;
}

/*
 * pt_get_first_arg () -
 *   return: the first argument of an expression node
 *   node(in):
 */
PT_NODE *
pt_get_first_arg (PT_NODE * node)
{
  PT_NODE *arg1 = NULL;

  assert (PT_IS_EXPR_NODE (node));

  if (!PT_IS_EXPR_NODE (node))
    {
      return NULL;
    }

  arg1 = node->info.expr.arg1;

  return arg1;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_operator_part () - returns operator for PT_EXPR
 *   return:
 *   node(in):
 */
int
pt_operator_part (const PT_NODE * node)
{
  if (node)
    {
      if (node->node_type == PT_EXPR)
	{
	  return node->info.expr.op;
	}
    }

  return 0;
}

/*
 * pt_class_part () -
 *   return:
 *   node(in):
 */
PT_NODE *
pt_class_part (const PT_NODE * node)
{
  if (node)
    {
      if (node->node_type == PT_UPDATE)
	{
	  return node->info.update.spec;
	}

      if (node->node_type == PT_DELETE)
	{
	  return node->info.delete_.spec;
	}

      if (node->node_type == PT_INSERT)
	{
	  return node->info.insert.spec;
	}

      if (node->node_type == PT_SELECT)
	{
	  return node->info.query.q.select.from;
	}
    }

  return NULL;
}

/*
 * pt_class_names_part () -
 *   return:
 *   node(in):
 */
PT_NODE *
pt_class_names_part (const PT_NODE * node)
{
  PT_NODE *temp;

  temp = pt_class_part (node);
  if (temp)
    {
      node = temp;
    }

  if (node && node->node_type == PT_SPEC)
    {
      return node->info.spec.flat_entity_list;
    }

  return NULL;
}
#endif

/*
 * pt_attrs_part () -
 *   return:
 *   node(in):
 */
PT_NODE *
pt_attrs_part (const PT_NODE * node)
{
  if (node && node->node_type == PT_INSERT)
    {
      return node->info.insert.attr_list;
    }

  return NULL;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_values_part () -
 *   return:
 *   node(in):
 */
PT_NODE *
pt_values_part (const PT_NODE * node)
{
  if (node
      && node->node_type == PT_INSERT
      && (node->info.insert.value_clauses->info.node_list.list_type
	  != PT_IS_SUBQUERY))
    {
      return node->info.insert.value_clauses;
    }

  return NULL;
}
#endif

/*
 * pt_get_subquery_of_insert_select () -
 *   return:
 *   node(in):
 */
PT_NODE *
pt_get_subquery_of_insert_select (const PT_NODE * node)
{
  PT_NODE *ptr_values = NULL;
  PT_NODE *ptr_subquery = NULL;

  if (node == NULL || node->node_type != PT_INSERT)
    {
      return NULL;
    }

  ptr_values = node->info.insert.value_clauses;
  assert (ptr_values != NULL);
  assert (ptr_values->node_type == PT_NODE_LIST);

  if (ptr_values->info.node_list.list_type != PT_IS_SUBQUERY)
    {
      return NULL;
    }

  assert (ptr_values->next == NULL);
  ptr_subquery = ptr_values->info.node_list.list;
  assert (PT_IS_QUERY (ptr_subquery));
  assert (ptr_subquery->next == NULL);

  return ptr_subquery;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_string_part () -
 *   return:
 *   node(in):
 */
const char *
pt_string_part (const PT_NODE * node)
{
  if (node && node->node_type == PT_NAME)
    {
      return node->info.name.original;
    }

  return NULL;
}

/*
 * pt_qualifier_part () -
 *   return:
 *   node(in):
 */
const char *
pt_qualifier_part (const PT_NODE * node)
{
  if (node && node->node_type == PT_NAME)
    {
      return node->info.name.resolved;
    }

  return NULL;
}

/*
 * pt_object_part () -
 *   return:
 *   node(in):
 */
void *
pt_object_part (const PT_NODE * node)
{
  if (node && node->node_type == PT_NAME)
    {
      return node->info.name.db_object;
    }

  return NULL;
}

/*
 * pt_node_next () - return the next node in a list
 *   return:
 *   node(in):
 */
PT_NODE *
pt_node_next (const PT_NODE * node)
{
  if (node)
    {
      return node->next;
    }
  return NULL;
}

/*
 * pt_set_node_etc () - sets the etc void pointer of a node
 *   return:
 *   node(in):
 *   etc(in):
 */
void
pt_set_node_etc (PT_NODE * node, const void *etc)
{
  if (node)
    {
      node->etc = (void *) etc;
    }
}
#endif

/*
 * pt_node_etc () - return the etc void pointer from a node
 *   return:
 *   node(in):
 */
void *
pt_node_etc (const PT_NODE * node)
{
  if (node)
    {
      return node->etc;
    }
  return NULL;
}

/*
 * pt_null_etc () - sets the etc void pointer to null
 *   return:
 *   node(in/out):
 */
void
pt_null_etc (PT_NODE * node)
{
  if (node)
    {
      node->etc = NULL;
    }
}

/*
 * pt_record_warning () - creates a new PT_ZZ_ERROR_MSG node  appends it
 *                        to parser->warning
 *   return:
 *   parser(in): pointer to parser structure
 *   stmt_no(in): source statement where warning was detected
 *   line_no(in): source line number where warning was detected
 *   col_no(in): source column number where warning was detected
 *   msg(in): a helpful explanation of the warning
 */
void
pt_record_warning (PARSER_CONTEXT * parser, int line_no,
		   int col_no, const char *msg)
{
  PT_NODE *node = NULL;

  node = parser_new_node (parser, PT_ZZ_ERROR_MSG);
  if (node != NULL)
    {
      node->line_number = line_no;
      node->column_number = col_no;
      node->info.error_msg.error_message =
	pt_append_string (parser, NULL, msg);
      parser->warnings = parser_append_node (node, parser->warnings);
    }
}

#if 0				/* unused */
/*
 * pt_get_warnings () - return the etc void pointer from a parser
 *   return:
 *   parser(in):
 */
PT_NODE *
pt_get_warnings (const PARSER_CONTEXT * parser)
{
  if (parser)
    {
      return parser->warnings;
    }
  return NULL;
}
#endif

/*
 * pt_reset_error () - resets the errors recorded in a parser to none
 *   return:
 *   parser(in/out):
 */
void
pt_reset_error (PARSER_CONTEXT * parser)
{
  if (parser)
    {
      if (pt_has_error (parser))
	{
	  parser_free_tree (parser, parser->error_msgs);
	  parser->error_msgs = NULL;
	}
    }
  return;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_column_updatable () - takes a subquery expansion of a class, and tests
 * 	it for column aka object-master updatability
 *   return: true on updatable
 *   parser(in):
 *   statement(in):
 */
bool
pt_column_updatable (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  bool updatable = (statement != NULL);

  while (updatable && statement)
    {
      switch (statement->node_type)
	{
	case PT_SELECT:
	  if (statement->info.query.q.select.group_by
	      || statement->info.query.q.select.from->info.spec.derived_table
	      || statement->info.query.all_distinct == PT_DISTINCT)
	    {
	      updatable = false;
	    }

	  if (updatable)
	    {
	      updatable = !pt_has_aggregate (parser, statement);
	    }
	  break;

	case PT_UNION:
	  if (statement->info.query.all_distinct == PT_DISTINCT)
	    {
	      updatable = false;
	    }

	  if (updatable)
	    {
	      updatable = pt_column_updatable (parser,
					       statement->info.query.q.
					       union_.arg1);
	    }

	  if (updatable)
	    {
	      updatable = pt_column_updatable (parser,
					       statement->info.query.q.
					       union_.arg2);
	    }
	  break;

	case PT_DIFFERENCE:
	case PT_INTERSECTION:
	default:
	  updatable = false;
	  break;
	}
      statement = statement->next;
    }

  return updatable;
}
#endif

/*
 * pt_has_error () - returns true if there are errors recorder for this parser
 *   return:
 *   parser(in):
 */
int
pt_has_error (const PARSER_CONTEXT * parser)
{
  if (parser)
    {
      return (parser->error_msgs != NULL);
    }

  return 0;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_get_select_from_spec () - return a select query_spec's from PT_NAME node
 *   return:  spec's from PT_NAME node if all OK, null otherwise
 *   spec(in): a parsed SELECT query specification
 */
static PT_NODE *
pt_get_select_from_spec (const PT_NODE * spec)
{
  PT_NODE *from_spec, *from_name;

  if (!spec
      || !(from_spec = pt_from_list_part (spec))
      || !pt_length_of_list (from_spec)
      || from_spec->node_type != PT_SPEC
      || !(from_name = from_spec->info.spec.entity_name)
      || from_name->node_type != PT_NAME)
    {
      return NULL;
    }

  return from_name;
}

/*
 * pt_get_select_from_name () - return a select query_spec's from entity name
 *   return:  spec's from entity name if all OK, null otherwise
 *   parser(in): the parser context
 *   spec(in): a parsed SELECT query specification
 */
const char *
pt_get_select_from_name (PARSER_CONTEXT * parser, const PT_NODE * spec)
{
  PT_NODE *from_name;
  char *result = NULL;

  from_name = pt_get_select_from_spec (spec);
  if (from_name != NULL)
    {
      if (from_name->info.name.resolved == NULL)
	{
	  result = (char *) from_name->info.name.original;
	}
      else
	{
	  result = pt_append_string (parser, NULL,
				     from_name->info.name.resolved);
	  result = pt_append_string (parser, result, ".");
	  result = pt_append_string (parser, result,
				     from_name->info.name.original);
	}
    }

  return result;
}

/*
 * pt_get_spec_name () - get this SELECT query's from spec name so that
 * 	'select ... from class foo' yields 'class foo'
 *   return:  selqry's from spec name
 *   parser(in): the parser context
 *   selqry(in): a SELECT query
 */
const char *
pt_get_spec_name (PARSER_CONTEXT * parser, const PT_NODE * selqry)
{
  char *result = NULL;
  PT_NODE *from_spec;

  from_spec = pt_from_list_part (selqry);
  if (from_spec && from_spec->node_type == PT_SPEC)
    {
      result = pt_append_string (parser, result,
				 pt_get_select_from_name (parser, selqry));
    }

  return result;
}
#endif

/*
 * pt_has_aggregate () -
 *   return: true if statement has an aggregate node in its parse tree
 *   parser(in):
 *   node(in/out):
 *
 * Note :
 * for aggregate select statement, set agg flag for next re-check
 */
bool
pt_has_aggregate (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_AGG_FIND_INFO info;
  PT_NODE *save_next;
  info.select_stack = NULL;
  info.base_count = 0;
  info.out_of_context_count = 0;
  info.stop_on_subquery = false;

  if (!node)
    {
      return false;
    }

  if (node->node_type == PT_SELECT)
    {
      bool found = false;

      /* STEP 1: check agg flag */
      if (PT_SELECT_INFO_IS_FLAGED (node, PT_SELECT_INFO_HAS_AGG))
	{
	  /* we've been here before */
	  return true;
	}

      /* STEP 2: check GROUP BY, HAVING */
      if (node->info.query.q.select.group_by
	  || node->info.query.q.select.having)
	{
	  found = true;
	  /* fall trough, we need to check for loose scan */
	}

      /* STEP 3: check select_list */
      save_next = node->next;
      node->next = NULL;
      (void) parser_walk_tree (parser, node, pt_find_aggregate_functions_pre,
			       &info, pt_find_aggregate_functions_post,
			       &info);
      node->next = save_next;

      if (info.base_count > 0)
	{
	  found = true;
	}

      if (found)
	{
	  PT_SELECT_INFO_SET_FLAG (node, PT_SELECT_INFO_HAS_AGG);
	  return true;
	}
    }
  else
    {
      save_next = node->next;
      node->next = NULL;
      (void) parser_walk_tree (parser, node, pt_find_aggregate_functions_pre,
			       &info, pt_find_aggregate_functions_post,
			       &info);
      node->next = save_next;

      if (info.out_of_context_count > 0)
	{
	  return true;
	}
    }

  return false;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_has_inst_or_orderby_num () - check if tree has an INST_NUM or ORDERBY_NUM
 *				   node somewhere
 *   return: true if tree has INST_NUM/ORDERBY_NUM
 *   parser(in):
 *   node(in):
 */
bool
pt_has_inst_or_orderby_num (PARSER_CONTEXT * parser, PT_NODE * node)
{
  bool has_inst_orderby_num = false;

  (void) parser_walk_tree (parser, node,
			   pt_is_inst_or_orderby_num_node,
			   &has_inst_orderby_num,
			   pt_is_inst_or_orderby_num_node_post,
			   &has_inst_orderby_num);

  return has_inst_orderby_num;
}

/*
 * pt_insert_host_var () - insert a host_var into a list based on
 *                         its ordinal position
 *   return: a list of PT_HOST_VAR type nodes
 *   parser(in): the parser context
 *   h_var(in): a PT_HOST_VAR type node
 *   list(in/out): a list of PT_HOST_VAR type nodes
 */
static PT_NODE *
pt_insert_host_var (PARSER_CONTEXT * parser, PT_NODE * h_var, PT_NODE * list)
{
  PT_NODE *temp, *tail, *new_node;

  if (!list || list->info.host_var.index > h_var->info.host_var.index)
    {
      /* the new node goes before the rest of the list */
      new_node = parser_copy_tree (parser, h_var);
      if (new_node == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "parser_copy_tree");
	  return NULL;
	}

      new_node->next = list;
      list = new_node;
    }
  else
    {
      tail = temp = list;
      while (temp && temp->info.host_var.index <= h_var->info.host_var.index)
	{
	  tail = temp;
	  temp = temp->next;
	}

      if (tail->info.host_var.index < h_var->info.host_var.index)
	{
	  new_node = parser_copy_tree (parser, h_var);
	  if (new_node == NULL)
	    {
	      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
	      return NULL;
	    }

	  tail->next = new_node;
	  new_node->next = temp;
	}
    }

  return list;
}

/*
 * pt_collect_host_info () - collect host_var or cursor info from this node
 *   return: node
 *   parser(in): the parser context used in deriving this node
 *   node(in): a node of the parse tree of an esql statement
 *   h_var(in/out): a PT_HOST_VARS for depositing host_var or cursor info
 *   continue_walk(in/out): flag that tells when to stop traversal
 *
 * Note :
 * if node is a host_var then
 *   append a copy of node into h_var.inputs
 * or if node is an UPDATE or DELETE current of cursor then
 *   save cursor name into h_var.cursor
 */
static PT_NODE *
pt_collect_host_info (PARSER_CONTEXT * parser, PT_NODE * node,
		      void *h_var, int *continue_walk)
{
  PT_HOST_VARS *hvars = (PT_HOST_VARS *) h_var;

  switch (node->node_type)
    {
    case PT_HOST_VAR:
      switch (node->info.host_var.var_type)
	{
	case PT_HOST_IN:	/* an input host variable */
	  hvars->inputs = pt_insert_host_var (parser, node, hvars->inputs);
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

/*
 * pt_host_info () - collect & return host_var & cursor info
 * 	from a parsed embedded statement
 *   return:  PT_HOST_VARS
 *   parser(in): the parser context used in deriving stmt
 *   stmt(in): parse tree of a an esql statement
 *
 * Note :
 * caller assumes responsibility for freeing PT_HOST_VARS via pt_free_host_info
 */

PT_HOST_VARS *
pt_host_info (PARSER_CONTEXT * parser, PT_NODE * stmt)
{
  PT_HOST_VARS *result = (PT_HOST_VARS *) calloc (1,
						  sizeof (PT_HOST_VARS));

  if (result)
    {
      memset (result, 0, sizeof (PT_HOST_VARS));

      (void) parser_walk_tree (parser, stmt, pt_collect_host_info,
			       (void *) result, NULL, NULL);
    }

  return result;
}

/*
 * pt_free_host_info () - deallocate a PT_HOST_VARS structure
 *   return:
 *   hv(in): a PT_HOST_VARS structure created by pt_host_info
 */
void
pt_free_host_info (PT_HOST_VARS * hv)
{
  if (hv)
    {
      free_and_init (hv);
    }
}
#endif

/*
 * pt_get_name () - return a PT_NAME's original name
 *   return: nam's original name if all OK, NULL otherwise.
 *   nam(in): a PT_NAME node
 */

const char *
pt_get_name (PT_NODE * nam)
{
  if (nam && nam->node_type == PT_NAME)
    {
      assert (nam->info.name.original != NULL);

      return nam->info.name.original;
    }
  else
    {
      return NULL;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_get_input_host_vars () - return a PT_HOST_VARS' list of input host_vars
 *   return:  hv's list of input host_vars
 *   hv(in): a PT_HOST_VARS structure created by pt_host_info
 */

PT_NODE *
pt_get_input_host_vars (const PT_HOST_VARS * hv)
{
  if (hv)
    {
      return hv->inputs;
    }
  else
    {
      return NULL;
    }
}

/*
 * pt_get_proxy_spec_name () - return a proxy query_spec's "from" entity name
 *   return: qspec's from entity name if all OK, NULL otherwise
 *   qspec(in): a proxy's SELECT query specification
 */

const char *
pt_get_proxy_spec_name (const char *qspec)
{
  PT_NODE *qtree;
  PARSER_CONTEXT *parser = NULL;
  const char *from_name = NULL, *result;
  size_t newlen;

  /* the parser and its strings go away upon return, but the
   * caller probably wants the proxy_spec_name to remain, so   */
  static char tblname[256], *name;
  static size_t namelen = 256;

  name = tblname;

  if (qspec
      && (parser = parser_create_parser ())
      && (qtree = parser_parse_string (parser, qspec))
      && !pt_has_error (parser) && qtree)
    {
      from_name = pt_get_spec_name (parser, qtree);
    }

  if (from_name == NULL)
    {
      result = NULL;		/* no, it failed */
    }
  else
    {
      /* copy from_name into tblname but do not overrun it! */
      newlen = strlen (from_name) + 1;
      if (newlen + 1 > namelen)
	{
	  /* get a bigger name buffer */
	  if (name != tblname)
	    {
	      free_and_init (name);
	    }
	  name = (char *) malloc (newlen);
	  namelen = newlen;
	}


      if (name)
	{
	  strcpy (name, from_name);
	}

      result = name;
    }

  if (parser != NULL)
    {
      parser_free_parser (parser);
    }

  return result;
}
#endif

/*
 * pt_register_orphan () - Accepts PT_NODE and puts it on the parser's
 * 	orphan list for subsequent freeing
 *   return: none
 *   parser(in):
 *   orphan(in):
 */
void
pt_register_orphan (PARSER_CONTEXT * parser, const PT_NODE * orphan)
{
  PT_NODE *dummy;

  if (orphan)
    {
      /* this node has already been freed. */
      if (orphan->node_type == PT_LAST_NODE_NUMBER)
	{
	  assert_release (false);
	  return;
	}

      dummy = parser_new_node (parser, PT_EXPR);
      dummy->info.expr.op = PT_NOT;	/* probably not necessary */
      dummy->info.expr.arg1 = (PT_NODE *) orphan;
      parser->orphans = parser_append_node (dummy, parser->orphans);
    }
}

/*
 * pt_register_orphan_db_value () - Accepts a db_value, wraps a PT_VALUE node
 * 	around it for convenience, and puts it on the parser's orphan
 * 	list for subsequent freeing
 *   return: none
 *   parser(in):
 *   orphan(in):
 */
void
pt_register_orphan_db_value (PARSER_CONTEXT * parser, const DB_VALUE * orphan)
{
  PT_NODE *dummy;

  if (orphan)
    {
      dummy = parser_new_node (parser, PT_VALUE);
      dummy->info.value.db_value_is_in_workspace = 1;
      dummy->info.value.db_value = *orphan;	/* structure copy */
      parser->orphans = parser_append_node (dummy, parser->orphans);
    }
}

/*
 * pt_free_orphans () - Frees all of the registered orphans
 *   return:
 *   parser(in):
 */
void
pt_free_orphans (PARSER_CONTEXT * parser)
{
  PT_NODE *ptr, *next;

  ptr = parser->orphans;
  while (ptr)
    {
      next = ptr->next;
      ptr->next = NULL;		/* cut off link */
      parser_free_tree (parser, ptr);
      ptr = next;		/* next to the link */
    }

  parser->orphans = NULL;
}

/*
 * pt_sort_spec_cover () -
 *   return:  true or false
 *   cur_list(in): current PT_SORT_SPEC list pointer
 *   new_list(in): new PT_SORT_SPEC list pointer
 */

bool
pt_sort_spec_cover (PT_NODE * cur_list, PT_NODE * new_list)
{
  PT_NODE *s1, *s2;
  QFILE_TUPLE_VALUE_POSITION *p1, *p2;

  if (new_list == NULL)
    {
      return false;
    }

  for (s1 = cur_list, s2 = new_list; s1 && s2; s1 = s1->next, s2 = s2->next)
    {
      p1 = &(s1->info.sort_spec.pos_descr);
      p2 = &(s2->info.sort_spec.pos_descr);

      if (p1->pos_no <= 0)
	{
	  s1 = NULL;		/* mark as end-of-sort */
	}

      if (p2->pos_no <= 0)
	{
	  s2 = NULL;		/* mark as end-of-sort */
	}

      /* end-of-sort check */
      if (s1 == NULL || s2 == NULL)
	{
	  break;
	}

      /* equality check */
      if (p1->pos_no != p2->pos_no
	  || s1->info.sort_spec.asc_or_desc != s2->info.sort_spec.asc_or_desc
	  || (s1->info.sort_spec.nulls_first_or_last !=
	      s2->info.sort_spec.nulls_first_or_last))
	{
	  return false;
	}
    }

  return (s2 == NULL) ? true : false;
}

/*
 *
 * Function group:
 * Query Processor memory management module
 *
 */


/*
 *       		  MEMORY FUNCTIONS FOR STRINGS
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * regu_bytes_alloc () - Memory allocation function for void *.
 *   return: void *
 *   length(in): length of the bytes to be allocated
 *   length(in) :
 */
static void *
regu_bytes_alloc (int length)
{
  void *ptr;

  if ((ptr = pt_alloc_packing_buf (length)) == (void *) NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      return ptr;
    }
}

/*
 * regu_string_alloc () - Memory allocation function for CHAR *.
 *   return: char *
 *   length(in) : length of the string to be allocated
 */
char *
regu_string_alloc (int length)
{
  return (char *) regu_bytes_alloc (length);
}

/*
 * regu_string_db_alloc () -
 *   return: char *
 *   length(in) : length of the string to be allocated
 *
 * Note: Memory allocation function for CHAR * using malloc.
 */
char *
regu_string_db_alloc (int length)
{
  char *ptr;

  return (ptr = (char *) malloc (length));
}

/*
 * regu_string_ws_alloc () -
 *   return: char *
 *   length(in) : length of the string to be allocated
 *
 * Note: Memory allocation function for CHAR * using malloc.
 */

char *
regu_string_ws_alloc (int length)
{
  char *ptr;

  return (ptr = (char *) db_ws_alloc (length));
}

/*
 * regu_strdup () - Duplication function for string.
 *   return: char *
 *   srptr(in)  : pointer to the source string
 *   alloc(in)  : pointer to an allocation function
 */
char *
regu_strdup (const char *srptr, char *(*alloc) (int))
{
  char *dtptr;
  int len;

  if ((dtptr = alloc ((len = strlen (srptr) + 1))) == NULL)
    {
      return NULL;
    }

  /* because alloc may be bound to regu_bytes_alloc (which is a fixed-len
   * buffer allocator), we must guard against copying strings longer than
   * DB_MAX_STRING_LENGTH.  Otherwise, we get a corrupted heap seg fault.
   */
  len = (len > DB_MAX_STRING_LENGTH ? DB_MAX_STRING_LENGTH : len);
  dtptr[0] = '\0';
  strncat (dtptr, srptr, len);
  dtptr[len - 1] = '\0';
  return dtptr;
}

/*
 * regu_strcmp () - String comparison function.
 *   return: int
 *   name1(in)  : pointer to the first string
 *   name2(in)  : pointer to the second string
 *   function_strcmp(in): pointer to the function strcmp or ansisql_strcmp
 */
int
regu_strcmp (const char *name1, const char *name2,
	     int (*function_strcmp) (const char *, const char *))
{
  int i;

  if (name1 == NULL && name2 == NULL)
    {
      return 0;
    }
  else if (name1 == NULL)
    {
      return -2;
    }
  else if (name2 == NULL)
    {
      return 2;
    }
  else if ((i = function_strcmp (name1, name2)) == 0)
    {
      return 0;
    }
  else
    {
      return ((i < 0) ? -1 : 1);
    }
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 *       		MEMORY FUNCTIONS FOR DB_VALUE
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * regu_dbval_db_alloc () -
 *   return: DB_VALUE *
 *
 * Note: Memory allocation function for DB_VALUE using malloc.
 */
DB_VALUE *
regu_dbval_db_alloc (void)
{
  DB_VALUE *ptr;

  ptr = (DB_VALUE *) malloc (sizeof (DB_VALUE));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_dbval_init (ptr);
      return ptr;
    }
}
#endif

/*
 * regu_dbval_alloc () -
 *   return: DB_VALUE *
 *
 * Note: Memory allocation function for X_VARIABLE.
 */
DB_VALUE *
regu_dbval_alloc (void)
{
  DB_VALUE *ptr;

  ptr = (DB_VALUE *) pt_alloc_packing_buf (sizeof (DB_VALUE));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_dbval_init (ptr);
      return ptr;
    }
}

/*
 * regu_dbval_init () - Initialization function for DB_VALUE.
 *   return: int
 *   ptr(in)    : pointer to an DB_VALUE
 */
int
regu_dbval_init (DB_VALUE * ptr)
{
  if (db_value_domain_init (ptr, DB_TYPE_NULL,
			    DB_DEFAULT_PRECISION,
			    DB_DEFAULT_SCALE) != NO_ERROR)
    {
      return false;
    }
  else
    {
      return true;
    }
}


/*
 * regu_dbval_type_init () -
 *   return: int
 *   ptr(in)    : pointer to an DB_VALUE
 *   type(in)   : a primitive data type
 *
 * Note: Initialization function for DB_VALUE with type argument.
 */
int
regu_dbval_type_init (DB_VALUE * ptr, DB_TYPE type)
{
  if (ptr != NULL
      && db_value_domain_init (ptr, type, DB_DEFAULT_PRECISION,
			       DB_DEFAULT_SCALE) == NO_ERROR)
    {
      return true;
    }

  return false;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * regu_dbvalptr_array_alloc () -
 *   return: DB_VALUE **
 *   size(in): size of the array to be allocated
 *
 * Note: Memory allocation function for arrays of DB_VALUE pointers
 *       allocated with the default memory manager.
 */
DB_VALUE **
regu_dbvalptr_array_alloc (int size)
{
  DB_VALUE **ptr;

  if (size == 0)
    return NULL;

  ptr = (DB_VALUE **) pt_alloc_packing_buf (sizeof (DB_VALUE *) * size);
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      return ptr;
    }
}
#endif

/*
 * regu_dbvallist_alloc () -
 *   return: QPROC_DB_VALUE_LIST
 *
 * Note: Memory allocation function for QPROC_DB_VALUE_LIST with the
 *              allocation of a DB_VALUE for the value field.
 */
QPROC_DB_VALUE_LIST
regu_dbvallist_alloc (void)
{
  QPROC_DB_VALUE_LIST ptr;

  ptr = regu_dbvlist_alloc ();
  if (ptr == NULL)
    {
      return NULL;
    }

  ptr->val = regu_dbval_alloc ();
  if (ptr->val == NULL)
    {
      return NULL;
    }

  return ptr;
}

/*
 * regu_dbvlist_alloc () -
 *   return: QPROC_DB_VALUE_LIST
 *
 * Note: Memory allocation function for QPROC_DB_VALUE_LIST.
 */
QPROC_DB_VALUE_LIST
regu_dbvlist_alloc (void)
{
  QPROC_DB_VALUE_LIST ptr;
  int size;

  size = (int) sizeof (struct qproc_db_value_list);
  ptr = (QPROC_DB_VALUE_LIST) pt_alloc_packing_buf (size);

  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_dbvallist_init (ptr);
      return ptr;
    }
}

/*
 * regu_dbvallist_init () -
 *   return:
 *   ptr(in)    : pointer to an QPROC_DB_VALUE_LIST
 *
 * Note: Initialization function for QPROC_DB_VALUE_LIST.
 */
static void
regu_dbvallist_init (QPROC_DB_VALUE_LIST ptr)
{
  ptr->next = NULL;
  ptr->val = NULL;
}

/*
 *       	       MEMORY FUNCTIONS FOR REGU_VARIABLE
 */

/*
 * regu_var_alloc () -
 *   return: REGU_VARIABLE *
 *
 * Note: Memory allocation function for REGU_VARIABLE.
 */
REGU_VARIABLE *
regu_var_alloc (void)
{
  REGU_VARIABLE *ptr;

  ptr = (REGU_VARIABLE *) pt_alloc_packing_buf (sizeof (REGU_VARIABLE));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_var_init (ptr);
      return ptr;
    }
}

/*
 * regu_var_init () -
 *   return:
 *   ptr(in)    : pointer to a regu_variable
 *
 * Note: Initialization function for REGU_VARIABLE.
 */
static void
regu_var_init (REGU_VARIABLE * ptr)
{
  ptr->type = TYPE_POS_VALUE;
  ptr->flags = 0;
  ptr->value.val_pos = 0;
  ptr->vfetch_to = NULL;
  REGU_VARIABLE_XASL (ptr) = NULL;

  DB_MAKE_NULL (&(ptr->value.dbval));
}

/*
 * regu_varlist_alloc () -
 *   return: REGU_VARIABLE_LIST
 *
 * Note: Memory allocation function for REGU_VARIABLE_LIST.
 */
REGU_VARIABLE_LIST
regu_varlist_alloc (void)
{
  REGU_VARIABLE_LIST ptr;
  int size;

  size = (int) sizeof (struct regu_variable_list_node);
  ptr = (REGU_VARIABLE_LIST) pt_alloc_packing_buf (size);

  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_varlist_init (ptr);
      return ptr;
    }
}

/*
 * regu_varlist_init () -
 *   return:
 *   ptr(in)    : pointer to a regu_variable_list
 *
 * Note: Initialization function for regu_variable_list.
 */
static void
regu_varlist_init (REGU_VARIABLE_LIST ptr)
{
  ptr->next = NULL;
  regu_var_init (&ptr->value);
}

/*
 * regu_varptr_array_alloc () -
 *   return: REGU_VARIABLE **
 *   size: size of the array to be allocated
 *
 * Note: Memory allocation function for arrays of REGU_VARIABLE
 *       pointers allocated with the default memory manager.
 */
REGU_VARIABLE **
regu_varptr_array_alloc (int size)
{
  REGU_VARIABLE **ptr;

  if (size <= 0)
    {
      assert (false);
      return NULL;
    }

  ptr = (REGU_VARIABLE **) pt_alloc_packing_buf (sizeof (REGU_VARIABLE *) *
						 size);
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      return ptr;
    }
}

/*
 *       	       MEMORY FUNCTIONS FOR POINTER LISTS
 */
/*
 * regu_vallist_alloc () -
 *   return: VAL_LIST
 *
 * Note: Memory allocation function for VAL_LIST.
 */
VAL_LIST *
regu_vallist_alloc (void)
{
  VAL_LIST *ptr;

  ptr = (VAL_LIST *) pt_alloc_packing_buf (sizeof (VAL_LIST));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_vallist_init (ptr);
      return ptr;
    }
}

/*
 * regu_vallist_init () -
 *   return:
 *   ptr(in)    : pointer to a value list
 *
 * Note: Initialization function for VAL_LIST.
 */
static void
regu_vallist_init (VAL_LIST * ptr)
{
  ptr->val_cnt = 0;
  ptr->valp = NULL;
}

/*
 * regu_outlist_alloc () -
 *   return: OUTPTR_LIST *
 *
 * Note: Memory allocation function for OUTPTR_LIST.
 */
OUTPTR_LIST *
regu_outlist_alloc (void)
{
  OUTPTR_LIST *ptr;

  ptr = (OUTPTR_LIST *) pt_alloc_packing_buf (sizeof (OUTPTR_LIST));

  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_outlist_init (ptr);
      return ptr;
    }
}

/*
 * regu_outlist_init () -
 *   return:
 *   ptr(in)    : pointer to an output pointer list
 *
 * Note: Initialization function for OUTPTR_LIST.
 */
static void
regu_outlist_init (OUTPTR_LIST * ptr)
{
  ptr->valptr_cnt = 0;
  ptr->valptrp = NULL;
}

/*
 * regu_outlistptr_array_alloc () - Allocate an array of OUTPTR_LIST pointers.
 *
 * return	 : Allocated memory pointer.
 * int size (in) : Array size.
 */
OUTPTR_LIST **
regu_outlistptr_array_alloc (int size)
{
  OUTPTR_LIST **ptr;

  if (size <= 0)
    {
      assert (false);
      return NULL;
    }

  ptr = (OUTPTR_LIST **) pt_alloc_packing_buf (sizeof (OUTPTR_LIST *) * size);
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      return ptr;
    }
}

/*
 *       	   MEMORY FUNCTIONS FOR EXPRESSION STRUCTURES
 */

/*
 * regu_pred_alloc () -
 *   return: PRED_EXPR *
 *
 * Note: Memory allocation function for PRED_EXPR.
 */
PRED_EXPR *
regu_pred_alloc (void)
{
  PRED_EXPR *ptr;

  ptr = (PRED_EXPR *) pt_alloc_packing_buf (sizeof (PRED_EXPR));

  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_pred_init (ptr);
      return ptr;
    }
}

/*
 * regu_pred_init () -
 *   return:
 *   ptr(in)    : pointer to a predicate expression
 *
 * Note: Initialization function for PRED_EXPR.
 */
static void
regu_pred_init (PRED_EXPR * ptr)
{
  ptr->type = T_NOT_TERM;
  ptr->pe.not_term = NULL;
}

/*
 * regu_arith_alloc () -
 *   return: ARITH_TYPE *
 *
 * Note: Memory allocation function for ARITH_TYPE with the allocation
 *       of a db_value for the value field.
 */
ARITH_TYPE *
regu_arith_alloc (void)
{
  ARITH_TYPE *arithptr;

  arithptr = regu_arith_no_value_alloc ();
  if (arithptr == NULL)
    {
      return NULL;
    }

  arithptr->value = regu_dbval_alloc ();
  if (arithptr->value == NULL)
    {
      return NULL;
    }

  return arithptr;
}

/*
 * regu_arith_no_value_alloc () -
 *   return: ARITH_TYPE *
 *
 * Note: Memory allocation function for ARITH_TYPE.
 */
static ARITH_TYPE *
regu_arith_no_value_alloc (void)
{
  ARITH_TYPE *ptr;

  ptr = (ARITH_TYPE *) pt_alloc_packing_buf (sizeof (ARITH_TYPE));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_arith_init (ptr);
      return ptr;
    }
}

/*
 * regu_arith_init () -
 *   return:
 *   ptr(in)    : pointer to an arithmetic node
 *
 * Note: Initialization function for ARITH_TYPE.
 */
static void
regu_arith_init (ARITH_TYPE * ptr)
{
  ptr->value = NULL;
  ptr->opcode = T_ADD;
  ptr->leftptr = NULL;
  ptr->rightptr = NULL;
  ptr->thirdptr = NULL;
  ptr->misc_operand = LEADING;
  ptr->rand_seed = NULL;
}

/*
 * regu_func_alloc () -
 *   return: FUNCTION_TYPE *
 *
 * Note: Memory allocation function for FUNCTION_TYPE with the
 *       allocation of a db_value for the value field
 */
FUNCTION_TYPE *
regu_func_alloc (void)
{
  FUNCTION_TYPE *funcp;

  funcp = regu_function_alloc ();
  if (funcp == NULL)
    {
      return NULL;
    }

  funcp->value = regu_dbval_alloc ();
  if (funcp->value == NULL)
    {
      return NULL;
    }

  return funcp;
}

/*
 * regu_function_alloc () -
 *   return: FUNCTION_TYPE *
 *
 * Note: Memory allocation function for FUNCTION_TYPE.
 */
static FUNCTION_TYPE *
regu_function_alloc (void)
{
  FUNCTION_TYPE *ptr;

  ptr = (FUNCTION_TYPE *) pt_alloc_packing_buf (sizeof (FUNCTION_TYPE));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_func_init (ptr);
      return ptr;
    }
}

/*
 * regu_func_init () -
 *   return:
 *   ptr(in)    : pointer to a function structure
 *
 * Note: Initialization function for FUNCTION_TYPE.
 */
static void
regu_func_init (FUNCTION_TYPE * ptr)
{
  ptr->value = NULL;
  ptr->ftype = (FUNC_TYPE) 0;
  ptr->operand = NULL;
}

/*
 * regu_agg_alloc () -
 *   return: AGGREGATE_TYPE *
 *
 * Note: Memory allocation function for AGGREGATE_TYPE with the
 *       allocation of a DB_VALUE for the value field and a list id
 *       structure for the list_id field.
 */
AGGREGATE_TYPE *
regu_agg_alloc (void)
{
  AGGREGATE_TYPE *aggptr;

  aggptr = regu_aggregate_alloc ();
  if (aggptr == NULL)
    {
      return NULL;
    }

  aggptr->accumulator.value = regu_dbval_alloc ();
  if (aggptr->accumulator.value == NULL)
    {
      return NULL;
    }

  aggptr->accumulator.value2 = regu_dbval_alloc ();
  if (aggptr->accumulator.value2 == NULL)
    {
      return NULL;
    }

  aggptr->list_id = regu_listid_alloc ();
  if (aggptr->list_id == NULL)
    {
      return NULL;
    }

  return aggptr;
}

/*
 * regu_agg_grbynum_alloc () -
 *   return:
 */
AGGREGATE_TYPE *
regu_agg_grbynum_alloc (void)
{
  AGGREGATE_TYPE *aggptr;

  aggptr = regu_aggregate_alloc ();
  if (aggptr == NULL)
    {
      return NULL;
    }

  aggptr->accumulator.value = NULL;
  aggptr->accumulator.value2 = NULL;
  aggptr->list_id = NULL;

  return aggptr;
}

/*
 * regu_aggregate_alloc () -
 *   return: AGGREGATE_TYPE *
 *
 * Note: Memory allocation function for AGGREGATE_TYPE.
 */
static AGGREGATE_TYPE *
regu_aggregate_alloc (void)
{
  AGGREGATE_TYPE *ptr;

  ptr = (AGGREGATE_TYPE *) pt_alloc_packing_buf (sizeof (AGGREGATE_TYPE));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_agg_init (ptr);
      return ptr;
    }
}

/*
 * regu_agg_init () -
 *   return:
 *   ptr(in)    : pointer to an aggregate structure
 *
 * Note: Initialization function for AGGREGATE_TYPE.
 */
static void
regu_agg_init (AGGREGATE_TYPE * ptr)
{
  ptr->next = NULL;
  ptr->accumulator.value = NULL;
  ptr->accumulator.value2 = NULL;
  ptr->accumulator.curr_cnt = 0;
  ptr->function = (FUNC_TYPE) 0;
  ptr->option = (QUERY_OPTIONS) 0;
  regu_var_init (&ptr->operand);
  ptr->list_id = NULL;
  ptr->sort_list = NULL;
}

/*
 *       		 MEMORY FUNCTIONS FOR XASL TREE
 */

/*
 * regu_xasl_node_alloc () -
 *   return: XASL_NODE *
 *   type(in)   : xasl proc type
 *
 * Note: Memory allocation function for XASL_NODE with the allocation
 *       a QFILE_LIST_ID structure for the list id field.
 */
XASL_NODE *
regu_xasl_node_alloc (PROC_TYPE type)
{
  XASL_NODE *xasl;

  xasl = regu_xasl_alloc (type);
  if (xasl == NULL)
    {
      return NULL;
    }

  xasl->list_id = regu_listid_alloc ();
  if (xasl->list_id == NULL)
    {
      return NULL;
    }

  return xasl;
}

/*
 * regu_xasl_alloc () -
 *   return: XASL_NODE *
 *   type(in): xasl proc type
 *
 * Note: Memory allocation function for XASL_NODE.
 */
static XASL_NODE *
regu_xasl_alloc (PROC_TYPE type)
{
  XASL_NODE *ptr;

  ptr = (XASL_NODE *) pt_alloc_packing_buf (sizeof (XASL_NODE));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_xasl_node_init (ptr, type);
      return ptr;
    }
}

/*
 * regu_xasl_node_init () -
 *   return:
 *   ptr(in)    : pointer to an xasl structure
 *   type(in)   : xasl proc type
 *
 * Note: Initialization function for XASL_NODE.
 */
static void
regu_xasl_node_init (XASL_NODE * ptr, PROC_TYPE type)
{
  memset ((char *) ptr, 0x00, sizeof (XASL_NODE));

  ptr->type = type;
  ptr->option = Q_ALL;
  ptr->topn_items = NULL;

  switch (type)
    {
    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:
      ptr->option = Q_DISTINCT;
      break;

    case BUILDLIST_PROC:
      EHID_SET_NULL (&(ptr->proc.buildlist.upd_del_ehid));
      break;

    case BUILDVALUE_PROC:
      break;

    case SCAN_PROC:
      break;

    case UPDATE_PROC:
      break;

    case DELETE_PROC:
      break;

    case INSERT_PROC:
      break;
    }
}

/*
 * regu_spec_alloc () -
 *   return: ACCESS_SPEC_TYPE *
 *   type(in)   : target type: TARGET_CLASS/TARGET_LIST
 *
 * Note: Memory allocation function for ACCESS_SPEC_TYPE with the
 *       allocation of a QFILE_LIST_ID structure for the list_id field of
 *       list file target.
 */
ACCESS_SPEC_TYPE *
regu_spec_alloc (TARGET_TYPE type)
{
  ACCESS_SPEC_TYPE *ptr;

  ptr = regu_access_spec_alloc (type);
  if (ptr == NULL)
    {
      return NULL;
    }

  return ptr;
}

/*
 * regu_access_spec_alloc () -
 *   return: ACCESS_SPEC_TYPE *
 *   type(in): TARGET_CLASS/TARGET_LIST
 *
 * Note: Memory allocation function for ACCESS_SPEC_TYPE.
 */
static ACCESS_SPEC_TYPE *
regu_access_spec_alloc (TARGET_TYPE type)
{
  ACCESS_SPEC_TYPE *ptr;

  ptr = (ACCESS_SPEC_TYPE *) pt_alloc_packing_buf (sizeof (ACCESS_SPEC_TYPE));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_spec_init (ptr, type);
      return ptr;
    }
}

/*
 * regu_spec_init () -
 *   return:
 *   ptr(in)    : pointer to an access specification structure
 *   type(in)   : TARGET_CLASS/TARGET_LIST
 *
 * Note: Initialization function for ACCESS_SPEC_TYPE.
 */
static void
regu_spec_init (ACCESS_SPEC_TYPE * ptr, TARGET_TYPE type)
{
  ptr->type = type;
  ptr->access = SEQUENTIAL;
  ptr->indexptr = NULL;
  ptr->where_key = NULL;
  ptr->where_pred = NULL;

  if (type == TARGET_CLASS)
    {
      ptr->s.cls_node.cls_regu_list_key = NULL;
      ptr->s.cls_node.cls_regu_list_pred = NULL;
      ptr->s.cls_node.cls_regu_list_rest = NULL;
      ptr->s.cls_node.cls_regu_list_pk_next = NULL;
      ACCESS_SPEC_HFID (ptr).vfid.fileid = NULL_FILEID;
      ACCESS_SPEC_HFID (ptr).vfid.volid = NULL_VOLID;
      ACCESS_SPEC_HFID (ptr).hpgid = NULL_PAGEID;
      regu_init_oid (&ACCESS_SPEC_CLS_OID (ptr));
    }
  else if (type == TARGET_LIST)
    {
      ptr->s.list_node.list_regu_list_pred = NULL;
      ptr->s.list_node.list_regu_list_rest = NULL;
      ACCESS_SPEC_XASL_NODE (ptr) = NULL;
    }

  memset ((void *) &ptr->s_id, 0, sizeof (SCAN_ID));
  ptr->fixed_scan = false;
  ptr->qualified_block = false;
  ptr->fetch_type = QPROC_FETCH_INNER;
  ptr->next = NULL;
  ptr->flags = 0;
}

/*
 * regu_index_alloc () -
 *   return: INDX_INFO *
 *
 * Note: Memory allocation function for INDX_INFO.
 */
INDX_INFO *
regu_index_alloc (void)
{
  INDX_INFO *ptr;

  ptr = (INDX_INFO *) pt_alloc_packing_buf (sizeof (INDX_INFO));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_index_init (ptr);
      return ptr;
    }
}

/*
 * regu_index_init () -
 *   return:
 *   ptr(in)    : pointer to an index structure
 *
 * Note: Initialization function for INDX_INFO.
 */
void
regu_index_init (INDX_INFO * ptr)
{
  OID_SET_NULL (&ptr->class_oid);
  ptr->coverage = 0;
  ptr->range_type = R_KEYLIST;
  ptr->key_info.key_cnt = 0;
  ptr->key_info.key_ranges = NULL;
  ptr->key_info.is_constant = false;
  ptr->key_info.key_limit_l = NULL;
  ptr->key_info.key_limit_u = NULL;
  ptr->key_info.key_limit_reset = false;
  ptr->orderby_desc = 0;
  ptr->groupby_desc = 0;
  ptr->use_desc_index = 0;
  ptr->orderby_skip = 0;
  ptr->groupby_skip = 0;
}

/*
 * regu_keyrange_init () -
 *   return:
 *   ptr(in)    : pointer to an key range structure
 *
 * Note: Initialization function for KEY_RANGE.
 */
void
regu_keyrange_init (KEY_RANGE * ptr)
{
  ptr->range = NA_NA;
  ptr->key1 = NULL;
  ptr->key2 = NULL;
}

/*
 * regu_keyrange_array_alloc () -
 *   return: KEY_RANGE *
 *
 * Note: Memory allocation function for KEY_RANGE.
 */
KEY_RANGE *
regu_keyrange_array_alloc (int size)
{
  KEY_RANGE *ptr;
  int i;

  if (size <= 0)
    {
      assert (false);		/* TODO - trace */
      return NULL;
    }

  ptr = (KEY_RANGE *) pt_alloc_packing_buf (sizeof (KEY_RANGE) * size);
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      for (i = 0; i < size; i++)
	{
	  regu_keyrange_init (ptr + i);
	}
      return ptr;
    }
}

/*
 * regu_sort_list_alloc () -
 *   return: SORT_LIST *
 *
 * Note: Memory allocation function for SORT_LIST.
 */
SORT_LIST *
regu_sort_list_alloc (void)
{
  SORT_LIST *ptr;

  ptr = regu_sort_alloc ();
  if (ptr == NULL)
    {
      return NULL;
    }
  return ptr;
}

/*
 * regu_sort_alloc () -
 *   return: SORT_LIST *
 *
 * Note: Memory allocation function for SORT_LIST.
 */
static SORT_LIST *
regu_sort_alloc (void)
{
  SORT_LIST *ptr;

  ptr = (SORT_LIST *) pt_alloc_packing_buf (sizeof (SORT_LIST));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_sort_list_init (ptr);
      return ptr;
    }
}

/*
 * regu_sort_list_init () -
 *   return:
 *   ptr(in)    : pointer to a list of sorting specifications
 *
 * Note: Initialization function for SORT_LIST.
 */
static void
regu_sort_list_init (SORT_LIST * ptr)
{
  ptr->next = NULL;
  ptr->pos_descr.pos_no = 0;
  ptr->s_order = S_ASC;
  ptr->s_nulls = S_NULLS_FIRST;
}

/*
 *       	       MEMORY FUNCTIONS FOR PHYSICAL ID'S
 */

/*
 * regu_init_oid () -
 *   return:
 *   oidptr(in) : pointer to an oid structure
 *
 * Note: Initialization function for OID.
 */
static void
regu_init_oid (OID * oidptr)
{
  OID_SET_NULL (oidptr);
}

/*
 *       	     MEMORY FUNCTIONS FOR LIST ID
 */

/*
 * regu_listid_alloc () -
 *   return: QFILE_LIST_ID *
 *
 * Note: Memory allocation function for QFILE_LIST_ID.
 */
static QFILE_LIST_ID *
regu_listid_alloc (void)
{
  QFILE_LIST_ID *ptr;

  ptr = (QFILE_LIST_ID *) pt_alloc_packing_buf (sizeof (QFILE_LIST_ID));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_listid_init (ptr);
      return ptr;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * regu_listid_db_alloc () -
 *   return: QFILE_LIST_ID *
 *
 * Note: Memory allocation function for QFILE_LIST_ID using malloc.
 */
QFILE_LIST_ID *
regu_listid_db_alloc (void)
{
  QFILE_LIST_ID *ptr;

  ptr = (QFILE_LIST_ID *) malloc (sizeof (QFILE_LIST_ID));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_listid_init (ptr);
      return ptr;
    }
}
#endif

/*
 * regu_listid_init () -
 *   return:
 *   ptr(in)    : pointer to a list_id structure
 *
 * Note: Initialization function for QFILE_LIST_ID.
 */
static void
regu_listid_init (QFILE_LIST_ID * ptr)
{
  QFILE_CLEAR_LIST_ID (ptr);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * regu_cp_listid () -
 *   return: bool
 *   dst_list_id(in)    : pointer to the destination list_id
 *   src_list_id(in)    : pointer to the source list_id
 *
 * Note: Copy function for QFILE_LIST_ID.
 */
int
regu_cp_listid (QFILE_LIST_ID * dst_list_id, QFILE_LIST_ID * src_list_id)
{
  return cursor_copy_list_id (dst_list_id, src_list_id);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * regu_free_listid () -
 *   return:
 *   list_id(in)        : pointer to a list_id structure
 *
 * Note: Free function for QFILE_LIST_ID using free_and_init.
 */
void
regu_free_listid (QFILE_LIST_ID * list_id)
{
  if (list_id != NULL)
    {
      cursor_free_list_id (list_id, true);
    }
}

/*
 * regu_srlistid_alloc () -
 *   return: QFILE_SORTED_LIST_ID *
 *
 * Note: Memory allocation function for QFILE_SORTED_LIST_ID.
 */
QFILE_SORTED_LIST_ID *
regu_srlistid_alloc (void)
{
  QFILE_SORTED_LIST_ID *ptr;
  int size;

  size = (int) sizeof (QFILE_SORTED_LIST_ID);
  ptr = (QFILE_SORTED_LIST_ID *) pt_alloc_packing_buf (size);

  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_srlistid_init (ptr);
      return ptr;
    }
}

/*
 * regu_srlistid_init () -
 *   return:
 *   ptr(in)    : pointer to a srlist_id structure
 *
 * Note: Initialization function for QFILE_SORTED_LIST_ID.
 */
static void
regu_srlistid_init (QFILE_SORTED_LIST_ID * ptr)
{
  ptr->sorted = false;
  ptr->list_id = NULL;
}

/*
 *       		 MEMORY FUNCTIONS FOR SM_DOMAIN
 */

/*
 * regu_domain_db_alloc () -
 *   return: SM_DOMAIN *
 *
 * Note: Memory allocation function for SM_DOMAIN using malloc.
 */
SM_DOMAIN *
regu_domain_db_alloc (void)
{
  SM_DOMAIN *ptr;

  ptr = (SM_DOMAIN *) malloc (sizeof (SM_DOMAIN));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_domain_init (ptr);
      return ptr;
    }
}

/*
 * regu_domain_init () -
 *   return:
 *   ptr(in)    : pointer to a schema manager domain
 *
 * Note: Initialization function for SM_DOMAIN.
 */
static void
regu_domain_init (SM_DOMAIN * ptr)
{
  ptr->next = NULL;
  ptr->next_list = NULL;
  ptr->type = PR_TYPE_FROM_ID (DB_TYPE_INTEGER);
  ptr->precision = 0;
  ptr->scale = 0;
  ptr->class_mop = NULL;
  ptr->self_ref = 0;
  ptr->setdomain = NULL;
  OID_SET_NULL (&ptr->class_oid);
  ptr->collation_id = 0;
  ptr->is_cached = 0;
  ptr->built_in_index = 0;
  ptr->is_parameterized = 0;
}

/*
 * regu_free_domain () -
 *   return:
 *   ptr(in)    : pointer to a schema manager domain
 *
 * Note: Free function for SM_DOMAIN using free_and_init.
 */
void
regu_free_domain (SM_DOMAIN * ptr)
{
  if (ptr != NULL)
    {
      regu_free_domain (ptr->next);
      regu_free_domain (ptr->setdomain);
      free_and_init (ptr);
    }
}


/*
 * regu_cp_domain () -
 *   return: SM_DOMAIN *
 *   ptr(in)    : pointer to a schema manager domain
 *
 * Note: Copy function for SM_DOMAIN.
 */
SM_DOMAIN *
regu_cp_domain (SM_DOMAIN * ptr)
{
  SM_DOMAIN *new_ptr;

  if (ptr == NULL)
    {
      return NULL;
    }

  new_ptr = regu_domain_db_alloc ();
  if (new_ptr == NULL)
    {
      return NULL;
    }
  *new_ptr = *ptr;

  if (ptr->next != NULL)
    {
      new_ptr->next = regu_cp_domain (ptr->next);
      if (new_ptr->next == NULL)
	{
	  free_and_init (new_ptr);
	  return NULL;
	}
    }

  if (ptr->setdomain != NULL)
    {
      new_ptr->setdomain = regu_cp_domain (ptr->setdomain);
      if (new_ptr->setdomain == NULL)
	{
	  regu_free_domain (new_ptr->next);
	  new_ptr->next = NULL;
	  free_and_init (new_ptr);
	  return NULL;
	}
    }

  return new_ptr;
}

/*
 * regu_int_init () -
 *   return:
 *   ptr(in)    : pointer to an int
 *
 * Note: Initialization function for int.
 */
void
regu_int_init (int *ptr)
{
  *ptr = 0;
}

/*
 * regu_int_array_alloc () -
 *   return: int *
 *   size(in): size of the array to be allocated
 *
 * Note: Memory allocation function for arrays of int
 */
int *
regu_int_array_alloc (int size)
{
  int *ptr;
  int i;

  if (size <= 0)
    {
      assert (false);		/* TODO - trace */
      return NULL;
    }

  ptr = (int *) pt_alloc_packing_buf (sizeof (int) * size);
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      for (i = 0; i < size; i++)
	{
	  regu_int_init (ptr + i);
	}
      return ptr;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * regu_int_pointer_array_alloc () -
 *   return: int **
 *   size(in): size of the array to be allocated
 *
 * Note: Memory allocation function for arrays of int pointers
 */
int **
regu_int_pointer_array_alloc (int size)
{
  int **ptr;
  int i;

  if (size == 0)
    {
      return NULL;
    }

  ptr = (int **) pt_alloc_packing_buf (sizeof (int *) * size);
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      for (i = 0; i < size; i++)
	{
	  *(ptr + i) = 0;
	}
      return ptr;
    }
}

/*
 * regu_int_array_db_alloc () -
 *   return: int *
 *   size(in): size of the array to be allocated
 *
 * Note: Memory allocation function for arrays of int using malloc.
 */
int *
regu_int_array_db_alloc (int size)
{
  int *ptr;
  int i;

  if (size == 0)
    return NULL;

  ptr = (int *) malloc (sizeof (int) * size);
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      for (i = 0; i < size; i++)
	{
	  regu_int_init (ptr + i);
	}
      return ptr;
    }
}
#endif

/*
 * regu_cache_attrinfo_alloc () -
 *   return: HEAP_CACHE_ATTRINFO *
 *
 * Note: Memory allocation function for HEAP_CACHE_ATTRINFO
 */
HEAP_CACHE_ATTRINFO *
regu_cache_attrinfo_alloc (void)
{
  HEAP_CACHE_ATTRINFO *ptr;
  int size;

  size = (int) sizeof (HEAP_CACHE_ATTRINFO);
  ptr = (HEAP_CACHE_ATTRINFO *) pt_alloc_packing_buf (size);
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      regu_cache_attrinfo_init (ptr);
      return ptr;
    }
}

/*
 * regu_cache_attrinfo_init () -
 *   return:
 *   ptr(in)    : pointer to a cache_attrinfo structure
 *
 * Note: Initialization function for HEAP_CACHE_ATTRINFO.
 */
static void
regu_cache_attrinfo_init (HEAP_CACHE_ATTRINFO * ptr)
{
  memset (ptr, 0, sizeof (HEAP_CACHE_ATTRINFO));
}

/*
 * regu_oid_init () -
 *   return:
 *   ptr(in)    : pointer to a OID
 *
 * Note: Initialization function for OID.
 */
void
regu_oid_init (OID * ptr)
{
  OID_SET_NULL (ptr);
}

/*
 * regu_oid_array_alloc () -
 *   return: OID *
 *   size(in): size of the array to be allocated
 *
 * Note: Memory allocation function for arrays of OID
 */
OID *
regu_oid_array_alloc (int size)
{
  OID *ptr;
  int i;

  if (size <= 0)
    {
      assert (false);		/* TODO - trace */
      return NULL;
    }

  ptr = (OID *) pt_alloc_packing_buf (sizeof (OID) * size);
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      for (i = 0; i < size; i++)
	{
	  regu_oid_init (ptr + i);
	}
      return ptr;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * regu_hfid_init () -
 *   return:
 *   ptr(in)    : pointer to a HFID
 *
 * Note: Initialization function for HFID.
 */
void
regu_hfid_init (HFID * ptr)
{
  HFID_SET_NULL (ptr);
}

/*
 * regu_hfid_array_alloc () -
 *   return: HFID *
 *   size(in): size of the array to be allocated
 *
 * Note: Memory allocation function for arrays of HFID
 */
HFID *
regu_hfid_array_alloc (int size)
{
  HFID *ptr;
  int i;

  if (size == 0)
    {
      return NULL;
    }

  ptr = (HFID *) pt_alloc_packing_buf (sizeof (HFID) * size);
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      for (i = 0; i < size; i++)
	{
	  regu_hfid_init (ptr + i);
	}
      return ptr;
    }
}
#endif

/*
 * regu_upddel_class_info_init () -
 *   return:
 *   ptr(in)    : pointer to a UPDDEL_CLASS_INFO
 *
 * Note: Initialization function for UPDDEL_CLASS_INFO.
 */
void
regu_upddel_class_info_init (UPDDEL_CLASS_INFO * ptr)
{
  OID_SET_NULL (&(ptr->class_oid));
  HFID_SET_NULL (&(ptr->class_hfid));
  ptr->no_attrs = 0;
  ptr->att_id = NULL;
  ptr->has_uniques = 0;
}

/*
 * regu_upddel_class_info_alloc () -
 *   return: UPDDEL_CLASS_INFO *
 *
 * Note: Memory allocation function for UPDDEL_CLASS_INFO
 */
UPDDEL_CLASS_INFO *
regu_upddel_class_info_alloc (void)
{
  UPDDEL_CLASS_INFO *upd_cls;

  upd_cls =
    (UPDDEL_CLASS_INFO *) pt_alloc_packing_buf (sizeof (UPDDEL_CLASS_INFO));
  if (upd_cls == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }

  regu_upddel_class_info_init (upd_cls);

  return upd_cls;
}

/*
 * regu_odku_info_alloc () - memory allocation for ODKU_INFO objects
 * return : allocated object or NULL
 */
ODKU_INFO *
regu_odku_info_alloc (void)
{
  ODKU_INFO *ptr;

  ptr = (ODKU_INFO *) pt_alloc_packing_buf (sizeof (ODKU_INFO));
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  ptr->assignments = NULL;
  ptr->attr_ids = NULL;
  ptr->cons_pred = NULL;
  ptr->no_assigns = 0;
  ptr->attr_info = NULL;
  return ptr;
}

/*
 * regu_update_assignment_init () -
 *   return:
 *   ptr(in)    : pointer to a UPDATE_ASSIGNMENT
 *
 * Note: Initialization function for UPDATE_ASSIGNMENT.
 */
void
regu_update_assignment_init (UPDATE_ASSIGNMENT * ptr)
{
  ptr->att_idx = -1;
  ptr->regu_var = NULL;
}

/*
 * regu_update_assignment_array_alloc () -
 *   return: UPDATE_ASSIGNMENT *
 *   size(in): size of the array to be allocated
 *
 * Note: Memory allocation function for arrays of UPDATE_ASSIGNMENT
 */
UPDATE_ASSIGNMENT *
regu_update_assignment_array_alloc (int size)
{
  UPDATE_ASSIGNMENT *ptr;
  int i;

  if (size <= 0)
    {
      assert (false);		/* TODO - trace */
      return NULL;
    }

  ptr =
    (UPDATE_ASSIGNMENT *) pt_alloc_packing_buf (sizeof (UPDATE_ASSIGNMENT) *
						size);
  if (ptr == NULL)
    {
      regu_set_error_with_zero_args (ER_REGU_NO_SPACE);
      return NULL;
    }
  else
    {
      for (i = 0; i < size; i++)
	{
	  regu_update_assignment_init (&ptr[i]);
	}
      return ptr;
    }
}

/* pt_enter_packing_buf() - mark the beginning of another level of packing
 *   return: none
 */
void
pt_enter_packing_buf (void)
{
  ++packing_level;
}

/* pt_alloc_packing_buf() - allocate space for packing
 *   return: pointer to the allocated space if all OK, NULL otherwise
 *   size(in): the amount of space to be allocated
 */
char *
pt_alloc_packing_buf (int size)
{
  char *res;
  HL_HEAPID *tmp_heap = NULL;
  HL_HEAPID heap_id;
  int i;

  if (size <= 0)
    {
      return NULL;
    }

  if (packing_heap == NULL)
    {
      packing_heap_num_slot = PACKING_MMGR_BLOCK_SIZE;
      packing_heap = (HL_HEAPID *) calloc (packing_heap_num_slot,
					   sizeof (HL_HEAPID));
      if (packing_heap == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1,
		  PACKING_MMGR_BLOCK_SIZE * sizeof (HL_HEAPID));
	  return NULL;
	}
    }
  else if (packing_heap_num_slot == packing_level - 1)
    {
      tmp_heap = (HL_HEAPID *) realloc (packing_heap,
					(packing_heap_num_slot +
					 PACKING_MMGR_BLOCK_SIZE) *
					sizeof (HL_HEAPID));
      if (tmp_heap == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1,
		  PACKING_MMGR_BLOCK_SIZE * sizeof (HL_HEAPID));
	  return NULL;
	}

      packing_heap_num_slot += PACKING_MMGR_BLOCK_SIZE;

      packing_heap = tmp_heap;

      for (i = 0; i < PACKING_MMGR_BLOCK_SIZE; i++)
	{
	  packing_heap[packing_heap_num_slot - i - 1] = 0;
	}
    }

  heap_id = packing_heap[packing_level - 1];
  if (heap_id == 0)
    {
      heap_id = db_create_ostk_heap (PACKING_MMGR_CHUNK_SIZE);
      packing_heap[packing_level - 1] = heap_id;
    }

  if (heap_id == 0)
    {
      /* make sure an error is set, one way or another */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, PACKING_MMGR_CHUNK_SIZE);
      res = NULL;
    }
  else
    {
      res = db_ostk_alloc (heap_id, size);
    }

  if (res == NULL)
    {
      qp_Packing_er_code = -1;
    }

  return res;
}

/* pt_free_packing_buf() - free packing space
 *   return: none
 *   slot(in): index of the packing space
 */
static void
pt_free_packing_buf (int slot)
{
  if (packing_heap && slot >= 0 && packing_heap[slot])
    {
      db_destroy_ostk_heap (packing_heap[slot]);
      packing_heap[slot] = 0;
    }
}

/* pt_exit_packing_buf() - mark the end of another level of packing
 *   return: none
 */
void
pt_exit_packing_buf (void)
{
  --packing_level;
  pt_free_packing_buf (packing_level);
}

/* pt_final_packing_buf() - free all resources for packing
 *   return: none
 */
void
pt_final_packing_buf (void)
{
  int i;

  for (i = 0; i < packing_level; i++)
    {
      pt_free_packing_buf (i);
    }

  free_and_init (packing_heap);
  packing_level = packing_heap_num_slot = 0;
}



/*
 *
 * Function group:
 * Query process regulator
 *
 */


/*
 * regu_set_error_with_zero_args () -
 *   return:
 *   err_type(in)       : error code
 *
 * Note: Error reporting function for error messages with no arguments.
 */
void
regu_set_error_with_zero_args (int err_type)
{
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, err_type, 0);
  qp_Packing_er_code = err_type;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * regu_set_error_with_one_args () -
 *   return:
 *   err_type(in)       : error code
 *   infor(in)  : message
 *
 * Note: Error reporting function for error messages with one string argument.
 */
void
regu_set_error_with_one_args (int err_type, const char *infor)
{
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, err_type, 1, infor);
  qp_Packing_er_code = err_type;
}

/*
 * regu_set_global_error () -
 *   return:
 *
 * Note: Set the client side query processor global error code.
 */
void
regu_set_global_error (void)
{
  qp_Packing_er_code = ER_REGU_SYSTEM;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * pt_limit_to_numbering_expr () -rewrite limit expr to xxx_num() expr
 *   return: expr node with numbering
 *   limit(in): limit node
 *   num_op(in):
 *   is_gry_num(in):
 *
 */
PT_NODE *
pt_limit_to_numbering_expr (PARSER_CONTEXT * parser, PT_NODE * limit,
			    PT_OP_TYPE num_op, bool is_gby_num)
{
  PT_NODE *lhs, *sum, *part1, *part2, *node;

  if (limit == NULL)
    {
      return NULL;
    }

  lhs = sum = part1 = part2 = node = NULL;

  if (is_gby_num == true)
    {
      lhs = parser_new_node (parser, PT_FUNCTION);
      if (lhs != NULL)
	{
	  lhs->type_enum = PT_TYPE_INTEGER;
	  lhs->info.function.function_type = PT_GROUPBY_NUM;
	  lhs->info.function.arg_list = NULL;
	  lhs->info.function.all_or_distinct = PT_ALL;
	}
    }
  else
    {
      lhs = parser_new_node (parser, PT_EXPR);
      if (lhs != NULL)
	{
	  lhs->info.expr.op = num_op;
	}
    }

  if (lhs == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return NULL;
    }

  if (limit->next == NULL)
    {
      node = parser_new_node (parser, PT_EXPR);
      if (node == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  goto error_exit;
	}

      if (is_gby_num)
	{
	  PT_EXPR_INFO_SET_FLAG (node, PT_EXPR_INFO_GROUPBYNUM_LIMIT);
	}
      node->info.expr.op = PT_LE;
      node->info.expr.arg1 = lhs;
      lhs = NULL;

      node->info.expr.arg2 = parser_copy_tree (parser, limit);
      if (node->info.expr.arg2 == NULL)
	{
	  goto error_exit;
	}
    }
  else
    {
      part1 = parser_new_node (parser, PT_EXPR);
      if (part1 == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  goto error_exit;
	}

      part1->info.expr.op = PT_GT;
      part1->type_enum = PT_TYPE_LOGICAL;
      part1->info.expr.arg1 = lhs;
      lhs = NULL;

      part1->info.expr.arg2 = parser_copy_tree (parser, limit);
      if (part1->info.expr.arg2 == NULL)
	{
	  goto error_exit;
	}

      part2 = parser_new_node (parser, PT_EXPR);
      if (part2 == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  goto error_exit;
	}

      part2->info.expr.op = PT_LE;
      part2->type_enum = PT_TYPE_LOGICAL;
      part2->info.expr.arg1 = parser_copy_tree (parser,
						part1->info.expr.arg1);
      if (part2->info.expr.arg1 == NULL)
	{
	  goto error_exit;
	}

      sum = parser_new_node (parser, PT_EXPR);
      if (sum == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  goto error_exit;
	}

      sum->info.expr.op = PT_PLUS;
      sum->type_enum = PT_TYPE_NUMERIC;
      sum->data_type = parser_new_node (parser, PT_DATA_TYPE);
      if (sum->data_type == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  goto error_exit;
	}

      sum->data_type->type_enum = PT_TYPE_NUMERIC;
      sum->data_type->info.data_type.precision = DB_DEFAULT_NUMERIC_PRECISION;
      sum->data_type->info.data_type.dec_scale = DB_DEFAULT_NUMERIC_SCALE;

      sum->info.expr.arg1 = parser_copy_tree (parser, limit);
      sum->info.expr.arg2 = parser_copy_tree (parser, limit->next);
      if (sum->info.expr.arg1 == NULL || sum->info.expr.arg2 == NULL)
	{
	  goto error_exit;
	}

      part2->info.expr.arg2 = sum;
      sum = NULL;

      node = parser_new_node (parser, PT_EXPR);
      if (node == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  goto error_exit;
	}

      if (is_gby_num)
	{
	  PT_EXPR_INFO_SET_FLAG (part1, PT_EXPR_INFO_GROUPBYNUM_LIMIT);
	  PT_EXPR_INFO_SET_FLAG (part2, PT_EXPR_INFO_GROUPBYNUM_LIMIT);
	}
      node->info.expr.op = PT_AND;
      node->type_enum = PT_TYPE_LOGICAL;
      node->info.expr.arg1 = part1;
      node->info.expr.arg2 = part2;
    }

  return node;

error_exit:
  if (lhs)
    {
      parser_free_tree (parser, lhs);
    }
  if (sum)
    {
      parser_free_tree (parser, sum);
    }
  if (part1)
    {
      parser_free_tree (parser, part1);
    }
  if (part2)
    {
      parser_free_tree (parser, part2);
    }
  if (node)
    {
      parser_free_tree (parser, node);
    }

  return NULL;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_copy_statement_flags () - Copies the special flags relevant for statement
 *                              execution from one node to another. This is
 *                              useful for executing statements that are
 *                              generated by rewriting existing statements
 *                              (see CREATE ... AS SELECT for an example).
 *   source(in): the statement to copy the flags from
 *   destination(in/out): the statement to copy the flags to
 *
 * Note: Not all the PT_NODE flags are copied, only the ones needed for correct
 *       execution of a statement are copied.
 */
void
pt_copy_statement_flags (PT_NODE * source, PT_NODE * destination)
{
  destination->recompile = source->recompile;
  destination->si_datetime = source->si_datetime;
}

/*
 * pt_fixup_column_type() - Fixes the type of a SELECT column so that it can
 *                          be used for view creation and for CREATE AS SELECT
 *                          statements
 *   col(in/out): the SELECT statement column
 *
 * Note: modifies TP_FLOATING_PRECISION_VALUE precision for char/bit constants
 *       This code is mostly a hack needed because string literals do not have
 *       the proper precision set. A better fix is to modify
 *       pt_db_value_initialize () so that the precision information is set.
 */
void
pt_fixup_column_type (PT_NODE * col)
{
  int fixed_precision = 0;

  if (col->node_type == PT_VALUE)
    {
      switch (col->type_enum)
	{
	case PT_TYPE_VARCHAR:
	  if (col->info.value.data_value.str != NULL)
	    {
	      fixed_precision = col->info.value.data_value.str->length;
	      if (fixed_precision == 0)
		{
		  fixed_precision = 1;
		}
	    }
	  break;

	case PT_TYPE_VARBIT:
	  switch (col->info.value.string_type)
	    {
	    case 'B':
	      if (col->info.value.data_value.str != NULL)
		{
		  fixed_precision = col->info.value.data_value.str->length;
		  if (fixed_precision == 0)
		    {
		      fixed_precision = 1;
		    }
		}
	      break;

	    case 'X':
	      if (col->info.value.data_value.str != NULL)
		{
		  fixed_precision = col->info.value.data_value.str->length;
		  if (fixed_precision == 0)
		    {
		      fixed_precision = 1;
		    }
		}
	      break;

	    default:
	      break;
	    }
	  break;

	default:
	  break;
	}
    }

}
#endif

/*
 * pt_node_list_to_array() - returns an array of nodes(PT_NODE) from a
 *			     PT_NODE list. Used mainly to convert a list of
 *			     argument nodes to an array of argument nodes
 *   return: NO_ERROR on success, ER_GENERIC_ERROR on failure
 *   parser(in): Parser context
 *   arg_list(in): List of nodes (arguments) chained on next
 *   arg_array(out): array of nodes (arguments)
 *   array_size(in): the (allocated) size of array
 *   num_args(out): the number of nodes found in list
 *
 * Note: the arg_array must be allocated and sized to 'array_size'
 */
int
pt_node_list_to_array (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * arg_list,
		       PT_NODE * arg_array[], const int array_size,
		       int *num_args)
{
  PT_NODE *arg = NULL;
  int error = NO_ERROR, len = 0;

  assert (array_size > 0);
  assert (arg_array != NULL);
  assert (arg_list != NULL);
  assert (num_args != NULL);

  *num_args = 0;

  for (arg = arg_list; arg != NULL; arg = arg->next)
    {
      if (len >= array_size)
	{
	  return ER_GENERIC_ERROR;
	}
      arg_array[len] = arg;
      *num_args = ++len;
    }
  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_make_dotted_identifier() - returns an identifier node (type PT_NAME) or
 *			         a PT_DOT node tree
 *
 *   return: node with constructed identifier, NULL if construction fails
 *   parser(in): Parser context
 *   identifier_str(in): string containing full identifier name. Dots ('.')
 *			 are used to delimit class names and column
 *			 names; for this reason, column and class names should
 *			 not contain dots.
 */
static PT_NODE *
pt_make_dotted_identifier (PARSER_CONTEXT * parser,
			   const char *identifier_str)
{
  return pt_make_dotted_identifier_internal (parser, identifier_str, 0);
}

/*
 * pt_make_dotted_identifier_internal() - builds an identifier node
 *				    (type PT_NAME) or tree (type PT_DOT)
 *
 *   return: node with constructed identifier, NULL if construction fails
 *   parser(in): Parser context
 *   identifier_str(in): string containing full identifier name (with dots);
 *			 length must be smaller than maximum allowed
 *			 identifier length
 *   depth(in): depth of current constructed node relative to PT_DOT subtree
 *
 *  Note : the depth argument is used to flag the PT_NAME node corresponding
 *	   to the first scoping name as 'meta_class = PT_NORMAL'.
 *	   This applies only to dotted identifier names.
 */
static PT_NODE *
pt_make_dotted_identifier_internal (PARSER_CONTEXT * parser,
				    const char *identifier_str, int depth)
{
  PT_NODE *identifier = NULL;
  char *p_dot = NULL;

  assert (depth >= 0);
  if (strlen (identifier_str) >= SM_MAX_IDENTIFIER_LENGTH)
    {
      assert (false);
      return NULL;
    }

  p_dot = strrchr (identifier_str, '.');

  if (p_dot != NULL)
    {
      char string_name1[SM_MAX_IDENTIFIER_LENGTH] = { 0 };
      char string_name2[SM_MAX_IDENTIFIER_LENGTH] = { 0 };
      PT_NODE *name1 = NULL;
      PT_NODE *name2 = NULL;
      int position = p_dot - identifier_str;
      int remaining = strlen (identifier_str) - position - 1;

      assert ((remaining > 0) && (remaining < strlen (identifier_str) - 1));
      assert ((position > 0) && (position < strlen (identifier_str) - 1));

      strncpy (string_name1, identifier_str, position);
      string_name1[position] = '\0';
      strncpy (string_name2, p_dot + 1, remaining);
      string_name2[remaining] = '\0';

      /* create PT_DOT_  - must be left - balanced */
      name1 = pt_make_dotted_identifier_internal (parser, string_name1,
						  depth + 1);
      name2 = pt_name (parser, string_name2);
      if (name1 == NULL || name2 == NULL)
	{
	  return NULL;
	}

      identifier = parser_new_node (parser, PT_DOT_);
      if (identifier == NULL)
	{
	  return NULL;
	}

      identifier->info.dot.arg1 = name1;
      identifier->info.dot.arg2 = name2;
    }
  else
    {
      identifier = pt_name (parser, identifier_str);
      if (identifier == NULL)
	{
	  return NULL;
	}

      /* it is a dotted identifier, make the first name PT_NORMAL */
      if (depth != 0)
	{
	  identifier->info.name.meta_class = PT_NORMAL;
	}
    }

  assert (identifier != NULL);

  return identifier;
}
#endif

/*
 * pt_add_table_name_to_from_list() - builds a corresponding node for a table
 *				'spec' and adds it to the end of the FROM list
 *                              of a SELECT node
 *
 *   return: newly build PT_NODE, NULL if construction fails
 *   parser(in): Parser context
 *   select(in): SELECT node
 *   table_name(in): table name (should not contain dots), may be NULL if spec
 *		     is a subquery (instead of a table)
 *   table_alias(in): alias of the table
 *   auth_bypass(in): bit mask of privileges flags that will bypass
 *                    authorizations
 */
PT_NODE *
pt_add_table_name_to_from_list (PARSER_CONTEXT * parser, PT_NODE * select,
				const char *table_name,
				const char *table_alias,
				const DB_AUTH auth_bypass)
{
  PT_NODE *spec = NULL;
  PT_NODE *from_item = NULL;
  PT_NODE *range_var = NULL;

  if (table_name != NULL)
    {
      from_item = pt_name (parser, table_name);
      if (from_item == NULL)
	{
	  return NULL;
	}
    }
  if (table_alias != NULL)
    {
      range_var = pt_name (parser, table_alias);
      if (range_var == NULL)
	{
	  return NULL;
	}
    }

  spec = pt_entity (parser, from_item, range_var, NULL);
  if (spec == NULL)
    {
      return NULL;
    }

  spec->info.spec.meta_class = PT_CLASS;
  select->info.query.q.select.from =
    parser_append_node (spec, select->info.query.q.select.from);
  spec->info.spec.auth_bypass_mask = auth_bypass;

  return spec;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_make_pred_name_int_val() - builds a predicate node (PT_EXPR) using a
 *			    column identifier on LHS and a integer value on
 *			    RHS
 *
 *   return: newly build node (PT_NODE)
 *   parser(in): Parser context
 *   op_type(in): operator type; should be a binary operator that makes sense
 *                for the passed arguments (such as PT_EQ, PT_GT, ...)
 *   col_name(in): column name (may contain dots)
 *   int_value(in): integer to assign to PT_VALUE RHS node
 */
static PT_NODE *
pt_make_pred_name_int_val (PARSER_CONTEXT * parser, PT_OP_TYPE op_type,
			   const char *col_name, const int int_value)
{
  PT_NODE *pred_rhs = NULL;
  PT_NODE *pred_lhs = NULL;
  PT_NODE *pred = NULL;

  /* create PT_VALUE for rhs */
  pred_rhs = pt_make_integer_value (parser, int_value);
  /* create PT_NAME for lhs */
  pred_lhs = pt_make_dotted_identifier (parser, col_name);

  pred = parser_make_expression (parser, op_type, pred_lhs, pred_rhs, NULL);
  return pred;
}

/*
 * pt_make_pred_name_string_val() - builds a predicate node (PT_EXPR) using an
 *			            identifier on LHS and a string value on
 *			            RHS
 *
 *   return: newly build node (PT_NODE)
 *   parser(in): Parser context
 *   op_type(in): operator type; should be a binary operator that makes sense
 *                for the passed arguments (such as PT_EQ, PT_GT, ...)
 *   identifier_str(in): column name (may contain dots)
 *   str_value(in): string to assign to PT_VALUE RHS node
 */
static PT_NODE *
pt_make_pred_name_string_val (PARSER_CONTEXT * parser, PT_OP_TYPE op_type,
			      const char *identifier_str,
			      const char *str_value)
{
  PT_NODE *pred_rhs = NULL;
  PT_NODE *pred_lhs = NULL;
  PT_NODE *pred = NULL;

  /* create PT_VALUE for rhs */
  pred_rhs = pt_make_string_value (parser, str_value);
  /* create PT_NAME for lhs */
  pred_lhs = pt_make_dotted_identifier (parser, identifier_str);

  pred = parser_make_expression (parser, op_type, pred_lhs, pred_rhs, NULL);

  return pred;
}

/*
 * pt_make_pred_with_identifiers() - builds a predicate node (PT_EXPR) using
 *			    an identifier on LHS and another identifier on
 *			    RHS
 *
 *   return: newly build node (PT_NODE)
 *   parser(in): Parser context
 *   op_type(in): operator type; should be a binary operator that makes sense
 *                for the passed arguments (such as PT_EQ, PT_GT, ...)
 *   lhs_identifier(in): LHS column name (may contain dots)
 *   rhs_identifier(in): RHS column name (may contain dots)
 */
static PT_NODE *
pt_make_pred_with_identifiers (PARSER_CONTEXT * parser, PT_OP_TYPE op_type,
			       const char *lhs_identifier,
			       const char *rhs_identifier)
{
  PT_NODE *dot1 = NULL;
  PT_NODE *dot2 = NULL;
  PT_NODE *pred_rhs = NULL;
  PT_NODE *pred_lhs = NULL;
  PT_NODE *pred = NULL;

  /* create PT_DOT_ for lhs */
  pred_lhs = pt_make_dotted_identifier (parser, lhs_identifier);
  /* create PT_DOT_ for rhs */
  pred_rhs = pt_make_dotted_identifier (parser, rhs_identifier);

  pred = parser_make_expression (parser, op_type, pred_lhs, pred_rhs, NULL);

  return pred;
}

/*
 * pt_make_if_with_expressions() - builds an IF (pred, expr_true, expr_false)
 *				operator node (PT_EXPR) given two expression
 *				nodes for true/false values
 *
 *   return: newly build node (PT_NODE)
 *   parser(in): Parser context
 *   pred(in): a node for expression used as predicate
 *   expr1(in): expression node for true value of predicate
 *   expr2(in): expression node for false value of predicate
 *   alias(in): alias for this new node
 */
static PT_NODE *
pt_make_if_with_expressions (PARSER_CONTEXT * parser, PT_NODE * pred,
			     PT_NODE * expr1, PT_NODE * expr2,
			     const char *alias)
{
  PT_NODE *if_node = NULL;

  if_node = parser_make_expression (parser, PT_IF, pred, expr1, expr2);

  if (alias != NULL)
    {
      if_node->alias_print = pt_append_string (parser, NULL, alias);
    }
  return if_node;
}

/*
 * pt_make_if_with_strings() - builds an IF (pred, expr_true, expr_false)
 *			       operator node (PT_EXPR) using two strings as
 *			       true/false values
 *
 *   return: newly build node (PT_NODE)
 *   parser(in): Parser context
 *   pred(in): a node for expression used as predicate
 *   string1(in): string used to build a value node for true value of predicate
 *   string1(in): string used to build a value node for false value of predicate
 *   alias(in): alias for this new node
 */
static PT_NODE *
pt_make_if_with_strings (PARSER_CONTEXT * parser, PT_NODE * pred,
			 const char *string1, const char *string2,
			 const char *alias)
{
  PT_NODE *val1_node = NULL;
  PT_NODE *val2_node = NULL;
  PT_NODE *if_node = NULL;

  val1_node = pt_make_string_value (parser, string1);
  val2_node = pt_make_string_value (parser, string2);

  if_node = pt_make_if_with_expressions (parser,
					 pred, val1_node, val2_node, alias);
  return if_node;
}

/*
 * pt_make_like_col_expr() - builds a LIKE operator node (PT_EXPR) using
 *			    an identifier on LHS an expression node on RHS
 *			    '<col_name> LIKE <rhs_expr>'
 *
 *   return: newly build node (PT_NODE)
 *   parser(in): Parser context
 *   rhs_expr(in): expression node
 *   col_name(in): LHS column name (may contain dots)
 */
static PT_NODE *
pt_make_like_col_expr (PARSER_CONTEXT * parser, PT_NODE * rhs_expr,
		       const char *col_name)
{
  PT_NODE *like_lhs = NULL;
  PT_NODE *like_node = NULL;

  like_lhs = pt_make_dotted_identifier (parser, col_name);
  like_node =
    parser_make_expression (parser, PT_LIKE, like_lhs, rhs_expr, NULL);

  return like_node;
}

/*
 * pt_make_outer_select_for_show_stmt() - builds a SELECT node and wrap the
 *				      inner supplied SELECT node
 *		'SELECT * FROM (<inner_select>) <select_alias>'
 *
 *   return: newly build node (PT_NODE), NULL if construction fails
 *   parser(in): Parser context
 *   inner_select(in): PT_SELECT node
 *   select_alias(in): alias for the 'FROM specs'
 */
static PT_NODE *
pt_make_outer_select_for_show_stmt (PARSER_CONTEXT * parser,
				    PT_NODE * inner_select,
				    const char *select_alias)
{
  /* SELECT * from ( SELECT .... ) <select_alias>;  */
  PT_NODE *val_node = NULL;
  PT_NODE *outer_node = NULL;
  PT_NODE *alias_subquery = NULL;
  PT_NODE *from_item = NULL;

  assert (inner_select != NULL);
  assert (inner_select->node_type == PT_SELECT);

  val_node = parser_new_node (parser, PT_VALUE);
  if (val_node)
    {
      val_node->type_enum = PT_TYPE_STAR;
    }
  else
    {
      return NULL;
    }

  outer_node = parser_new_node (parser, PT_SELECT);
  if (outer_node == NULL)
    {
      return NULL;
    }

  outer_node->info.query.q.select.list =
    parser_append_node (val_node, outer_node->info.query.q.select.list);
  inner_select->info.query.is_subquery = PT_IS_SUBQUERY;

  alias_subquery = pt_name (parser, select_alias);
  /* add to FROM an empty entity, the entity will be populated later */
  from_item = pt_add_table_name_to_from_list (parser,
					      outer_node, NULL, NULL,
					      DB_AUTH_NONE);

  if (from_item == NULL)
    {
      return NULL;
    }

  from_item->info.spec.derived_table = inner_select;
  from_item->info.spec.meta_class = 0;
  from_item->info.spec.range_var = alias_subquery;
  from_item->info.spec.derived_table_type = PT_IS_SUBQUERY;
  from_item->info.spec.join_type = PT_JOIN_NONE;

  return outer_node;
}

/*
 * pt_make_sort_spec_with_identifier() - builds a SORT_SPEC for GROUP BY or
 *				        ORDER BY using a column indentifier
 *
 *   return: newly build node (PT_NODE), NULL if construction fails
 *   parser(in): Parser context
 *   identifier(in): full name of identifier
 *   sort_mode(in): sorting ascendint or descending; if this parameter is not
 *		    PT_ASC or PT_DESC, the function will return NULL
 */
static PT_NODE *
pt_make_sort_spec_with_identifier (PARSER_CONTEXT * parser,
				   const char *identifier,
				   PT_MISC_TYPE sort_mode)
{
  PT_NODE *group_by_node = NULL;
  PT_NODE *group_by_col = NULL;

  if (sort_mode != PT_ASC && sort_mode != PT_DESC)
    {
      assert (false);
      return NULL;
    }
  group_by_node = parser_new_node (parser, PT_SORT_SPEC);
  if (group_by_node == NULL)
    {
      return NULL;
    }

  group_by_col = pt_make_dotted_identifier (parser, identifier);
  group_by_node->info.sort_spec.asc_or_desc = sort_mode;
  group_by_node->info.sort_spec.expr = group_by_col;

  return group_by_node;
}

/*
 * pt_make_sort_spec_with_number() - builds a SORT_SPEC for ORDER BY using
 *					a numeric indentifier
 *  used in : < ORDER BY <x> <ASC|DESC> >
 *
 *   return: newly build node (PT_NODE), NULL if construction fails
 *   parser(in): Parser context
 *   number_pos(in): position number for ORDER BY
 *   sort_mode(in): sorting ascendint or descending; if this parameter is not
 *		    PT_ASC or PT_DESC, the function will return NULL
 */
static PT_NODE *
pt_make_sort_spec_with_number (PARSER_CONTEXT * parser,
			       const int number_pos, PT_MISC_TYPE sort_mode)
{
  PT_NODE *sort_spec_node = NULL;
  PT_NODE *sort_spec_num = NULL;

  if (sort_mode != PT_ASC && sort_mode != PT_DESC)
    {
      assert (false);
      return NULL;
    }
  sort_spec_node = parser_new_node (parser, PT_SORT_SPEC);
  if (sort_spec_node == NULL)
    {
      return NULL;
    }

  sort_spec_num = pt_make_integer_value (parser, number_pos);
  sort_spec_node->info.sort_spec.asc_or_desc = sort_mode;
  sort_spec_node->info.sort_spec.expr = sort_spec_num;

  return sort_spec_node;
}

/*
 * pt_help_show_create_table() help to generate create table string.
 * return string of create table.
 * parser(in) : Parser context
 * table_name(in): table name node
 */
static char *
pt_help_show_create_table (PARSER_CONTEXT * parser, PT_NODE * table_name)
{
  DB_OBJECT *class_op;
  CLASS_HELP *class_schema = NULL;
  PARSER_VARCHAR *buffer;
  char **line_ptr;

  /* look up class in all schema's  */
  class_op = sm_find_class (table_name->info.name.original);
  if (class_op == NULL)
    {
      if (er_errid () != NO_ERROR)
	{
	  PT_ERRORc (parser, table_name, er_msg ());
	}
      return NULL;
    }

  if (!db_is_class (class_op))
    {
      PT_ERRORmf2 (parser, table_name, MSGCAT_SET_PARSER_SEMANTIC,
		   MSGCAT_SEMANTIC_IS_NOT_A, table_name->info.name.original,
		   pt_show_misc_type (PT_CLASS));
    }

  class_schema = obj_print_help_class (class_op, OBJ_PRINT_SHOW_CREATE_TABLE);
  if (class_schema == NULL)
    {
      int error;

      error = er_errid ();
      assert (error != NO_ERROR);
      if (error == ER_AU_SELECT_FAILURE)
	{
	  PT_ERRORmf2 (parser, table_name, MSGCAT_SET_PARSER_RUNTIME,
		       MSGCAT_RUNTIME_IS_NOT_AUTHORIZED_ON,
		       "select", sm_class_name (class_op));
	}
      else
	{
	  PT_ERRORc (parser, table_name, er_msg ());
	}
      return NULL;
    }

  buffer = NULL;
  /* class name */
  buffer = pt_append_nulstring (parser, buffer, "CREATE TABLE ");
  buffer = pt_append_nulstring (parser, buffer, class_schema->name);

  /* attributes and constraints */
  if (class_schema->attributes != NULL || class_schema->constraints != NULL)
    {
      buffer = pt_append_nulstring (parser, buffer, " (");
      if (class_schema->attributes != NULL)
	{
	  for (line_ptr = class_schema->attributes; *line_ptr != NULL;
	       line_ptr++)
	    {
	      if (line_ptr != class_schema->attributes)
		{
		  buffer = pt_append_nulstring (parser, buffer, ", ");
		}
	      buffer = pt_append_nulstring (parser, buffer, *line_ptr);
	    }
	}
      if (class_schema->constraints != NULL)
	{
	  for (line_ptr = class_schema->constraints; *line_ptr != NULL;
	       line_ptr++)
	    {
	      if (line_ptr != class_schema->constraints
		  || class_schema->attributes != NULL)
		{
		  buffer = pt_append_nulstring (parser, buffer, ", ");
		}
	      buffer = pt_append_nulstring (parser, buffer, *line_ptr);
	    }
	}
      buffer = pt_append_nulstring (parser, buffer, ")");
    }

  /* collation */
  if (class_schema->collation != NULL)
    {
      buffer = pt_append_nulstring (parser, buffer, " COLLATE ");
      buffer = pt_append_nulstring (parser, buffer, class_schema->collation);
    }

  if (class_schema != NULL)
    {
      obj_print_help_free_class (class_schema);
    }

  return ((char *) pt_get_varchar_bytes (buffer));
}

/*
 * pt_is_spec_referenced() - check if the current node references the spec id
 *			     passed as parameter
 *   return: the current node
 *   parser(in): Parser context
 *   node(in):
 *   void_arg(in): must contain an address to the id of the spec. If the spec id
 *		   is referenced then reference of this parameter is modified to
 *		   0.
 *   continue_walk(in):
 *
 */
static PT_NODE *
pt_is_spec_referenced (PARSER_CONTEXT * parser, PT_NODE * node,
		       void *void_arg, int *continue_walk)
{
  UINTPTR spec_id = *(UINTPTR *) void_arg;

  *continue_walk = PT_CONTINUE_WALK;

  if (node->node_type == PT_NAME && node->info.name.spec_id == spec_id
      && node->info.name.meta_class != PT_INDEX_NAME)
    {
      *(UINTPTR *) void_arg = 0;
      *continue_walk = PT_STOP_WALK;
      return node;
    }

  if (node->node_type == PT_SPEC)
    {
      /* The only part of a spec node that could contain references to
       * the given spec_id are derived tables and on_cond.
       * All the rest of the name nodes for the spec are not references,
       * but range variables, class names, etc.
       * We don't want to mess with these. We'll handle the ones that
       * we want by hand. */
      parser_walk_tree (parser, node->info.spec.derived_table,
			pt_is_spec_referenced, void_arg, pt_continue_walk,
			NULL);
      parser_walk_tree (parser, node->info.spec.on_cond,
			pt_is_spec_referenced, void_arg, pt_continue_walk,
			NULL);
      /* don't visit any other leaf nodes */
      *continue_walk = PT_LIST_WALK;
      return node;
    }

  /* Data type nodes can not contain any valid references.  They do
     contain class names and other things we don't want. */
  if (node->node_type == PT_DATA_TYPE)
    {
      *continue_walk = PT_LIST_WALK;
    }

  return node;
}
#endif

/*
 * pt_convert_to_logical_expr () -  if necessary, creates a logically correct
 *				    expression from the given node
 *
 *   return: - the same node if conversion was not necessary, OR
 *           - a new PT_EXPR: (node <> 0), OR
 *	     - NULL on failures
 *   parser (in): Parser context
 *   node (in): the node to be checked and wrapped
 *   use_parens (in): set to true if parantheses are needed around the original node
 *
 *   Note: we see if the given node is of type PT_TYPE_LOGICAL, and if not,
 *         we create an expression of the form "(node <> 0)" - with parens
 */
PT_NODE *
pt_convert_to_logical_expr (PARSER_CONTEXT * parser, PT_NODE * node,
			    bool use_parens_inside, bool use_parens_outside)
{
  PT_NODE *expr = NULL;
  PT_NODE *zero = NULL;

  (void) use_parens_inside;
  (void) use_parens_outside;

  /* If there's nothing to be done, go away */
  if (node == NULL || node->type_enum == PT_TYPE_LOGICAL)
    {
      return node;
    }

  /* allocate a new node for the zero value */
  zero = parser_new_node (parser, PT_VALUE);
  if (NULL == zero)
    {
      return NULL;
    }

  zero->info.value.data_value.i = 0;
  zero->type_enum = PT_TYPE_INTEGER;

  /* make a new expression comparing the node to zero */
  expr = parser_make_expression (parser, PT_NE, node, zero, NULL);
  if (expr != NULL)
    {
      expr->type_enum = PT_TYPE_LOGICAL;
    }

  return expr;
}

/*
 * pt_is_operator_logical() - returns TRUE if the operator has a logical
 *			      return type (i.e. <, >, AND etc.) and FALSE
 *			      otherwise.
 *
 *   return: boolean
 *   op(in): the operator
 */
bool
pt_is_operator_logical (PT_OP_TYPE op)
{
  switch (op)
    {
    case PT_OR:
    case PT_XOR:
    case PT_AND:
    case PT_IS_NOT:
    case PT_IS:
    case PT_NOT:
    case PT_EXISTS:
    case PT_LIKE_ESCAPE:
    case PT_LIKE:
    case PT_NOT_LIKE:
    case PT_RLIKE:
    case PT_NOT_RLIKE:
    case PT_RLIKE_BINARY:
    case PT_NOT_RLIKE_BINARY:
    case PT_EQ:
    case PT_NE:
    case PT_GT:
    case PT_GE:
    case PT_LT:
    case PT_LE:
    case PT_NULLSAFE_EQ:
    case PT_IS_NOT_NULL:
    case PT_IS_NULL:
    case PT_NOT_BETWEEN:
    case PT_BETWEEN:
    case PT_IS_IN:
    case PT_IS_NOT_IN:
    case PT_BETWEEN_GE_LE:
    case PT_BETWEEN_GT_LE:
    case PT_BETWEEN_GE_LT:
    case PT_BETWEEN_GT_LT:
    case PT_BETWEEN_EQ_NA:
    case PT_BETWEEN_GE_INF:
    case PT_BETWEEN_GT_INF:
    case PT_BETWEEN_INF_LE:
    case PT_BETWEEN_INF_LT:
    case PT_RANGE:
      return true;
    default:
      return false;
    }
}

/*
 * pt_sort_spec_cover_groupby () -
 *   return: true if group list is covered by sort list
 *   sort_list(in):
 *   group_list(in):
 */
bool
pt_sort_spec_cover_groupby (PARSER_CONTEXT * parser, PT_NODE * sort_list,
			    PT_NODE * group_list, PT_NODE * tree)
{
  PT_NODE *s1, *s2, *save_node, *col;
  QFILE_TUPLE_VALUE_POSITION pos_descr;
  int i;

  if (group_list == NULL)
    {
      return false;
    }

  s1 = sort_list;
  s2 = group_list;

  while (s1 && s2)
    {
      pt_to_pos_descr (parser, &pos_descr, s2->info.sort_spec.expr,
		       tree, NULL);
      if (pos_descr.pos_no > 0)
	{
	  col = tree->info.query.q.select.list;
	  for (i = 1; i < pos_descr.pos_no && col; i++)
	    {
	      col = col->next;
	    }
	  if (col != NULL)
	    {
	      col = pt_get_end_path_node (col);

	      if (col->node_type == PT_NAME
		  && PT_NAME_INFO_IS_FLAGED (col, PT_NAME_INFO_CONSTANT))
		{
		  s2 = s2->next;
		  continue;	/* skip out constant order */
		}
	    }
	}

      save_node = s1->info.sort_spec.expr;
      CAST_POINTER_TO_NODE (s1->info.sort_spec.expr);

      if (!pt_name_equal (parser, s1->info.sort_spec.expr,
			  s2->info.sort_spec.expr)
	  || (s1->info.sort_spec.asc_or_desc !=
	      s2->info.sort_spec.asc_or_desc)
	  || (s1->info.sort_spec.nulls_first_or_last !=
	      s2->info.sort_spec.nulls_first_or_last))
	{
	  s1->info.sort_spec.expr = save_node;
	  return false;
	}

      s1->info.sort_spec.expr = save_node;

      s1 = s1->next;
      s2 = s2->next;
    }

  return (s2 == NULL) ? true : false;
}

/*
 * pt_mark_spec_list_for_delete () - mark specs that will be deleted
 *   return:  none
 *   parser(in): the parser context
 *   delete_statement(in): a delete statement
 */
int
pt_mark_spec_list_for_delete (PARSER_CONTEXT * parser,
			      PT_NODE * delete_statement)
{
  PT_NODE *node, *from;
  int spec_count;

  assert (delete_statement->node_type == PT_DELETE);
  assert (delete_statement->info.delete_.target_classes != NULL);
  assert (delete_statement->info.delete_.target_classes->next == NULL);

  spec_count = 0;

  for (node = delete_statement->info.delete_.target_classes;
       node != NULL; node = node->next)
    {
      from = pt_find_spec_in_statement (parser, delete_statement, node);
      if (from == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "invalid spec id");
	  return ER_FAILED;
	}

      if (!(from->info.spec.flag & PT_SPEC_FLAG_DELETE))
	{
	  from->info.spec.flag |= PT_SPEC_FLAG_DELETE;
	  spec_count++;
	}
    }

#if 1				/* TODO - */
  if (spec_count > 1)
    {
      PT_ERRORm (parser, delete_statement, MSGCAT_SET_PARSER_RUNTIME,
		 MSGCAT_RUNTIME_DELETE_EMPTY);
      return ER_FAILED;
    }
#endif

  return NO_ERROR;
}

/*
 * pt_mark_spec_list_for_update () - mark specs that will be updated
 *   return:  none
 *   parser(in): the parser context
 *   statement(in): an update/merge statement
 */
int
pt_mark_spec_list_for_update (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  PT_NODE *lhs, *node_tmp, *node;
  int spec_count;

  assert (statement->node_type == PT_UPDATE);

  spec_count = 0;

  /* set flags for updatable specs */
  for (node = statement->info.update.assignment;
       node != NULL; node = node->next)
    {
      for (lhs = node->info.expr.arg1; lhs != NULL; lhs = lhs->next)
	{
	  node_tmp = pt_find_spec_in_statement (parser, statement, lhs);
	  if (node_tmp == NULL)
	    {
	      PT_INTERNAL_ERROR (parser, "invalid spec id");
	      return ER_FAILED;
	    }

	  if (!(node_tmp->info.spec.flag & PT_SPEC_FLAG_UPDATE))
	    {
	      node_tmp->info.spec.flag |= PT_SPEC_FLAG_UPDATE;
	      spec_count++;
	    }
	}
    }

#if 1				/* TODO - */
  if (spec_count > 1)
    {
      PT_ERRORm (parser, statement, MSGCAT_SET_PARSER_RUNTIME,
		 MSGCAT_RUNTIME_UPDATE_EMPTY);
      return ER_FAILED;
    }
#endif

  return NO_ERROR;
}

/*
 * pt_check_grammar_charset_collation () - validates a pair of charset and
 *	  collation nodes and return the associated identifiers
 *   return:  error status
 *   parser(in): the parser context
 *   coll_node(in): node containing collation string (PT_VALUE)
 *   coll_id(in): validated value for collation
 */
int
pt_check_grammar_charset_collation (PARSER_CONTEXT * parser,
				    PT_NODE * coll_node, int *coll_id)
{
  LANG_COLLATION *lang_coll;
  UNUSED_VAR int coll_charset;

  assert (coll_id != NULL);

  *coll_id = LANG_SYS_COLLATION;

  if (coll_node != NULL)
    {
      assert (coll_node->node_type == PT_VALUE);
      assert (coll_node->info.value.data_value.str != NULL);

      lang_coll =
	lang_get_collation_by_name ((const char *) coll_node->info.value.
				    data_value.str->bytes);

      if (lang_coll != NULL)
	{
	  *coll_id = lang_coll->coll.coll_id;
	  coll_charset = (int) lang_coll->codeset;
	}
      else
	{
	  PT_ERRORmf (parser, coll_node, MSGCAT_SET_PARSER_SEMANTIC,
		      MSGCAT_SEMANTIC_UNKNOWN_COLL,
		      coll_node->info.value.data_value.str->bytes);
	  return ER_GENERIC_ERROR;
	}
    }

  return NO_ERROR;
}

/*
 * pt_get_query_limit_from_limit () - get the value of the LIMIT clause of a
 *				      query
 * return : error code or NO_ERROR
 * parser (in)	      : parser context
 * limit (in)	      : limit node
 * limit_val (in/out) : limit value
 *
 * Note: this function get the LIMIT clause value of a query as a
 *  DB_TYPE_BIGINT value. If the LIMIT clause contains a lower limit, the
 *  returned value is computed as lower bound + range. (i.e.: if it was
 *  specified as LIMIT x, y this function returns x+y)
 */
static int
pt_get_query_limit_from_limit (PARSER_CONTEXT * parser, PT_NODE * limit,
			       DB_VALUE * limit_val)
{
  DB_VALUE *limit_const = NULL;
  int save_set_host_var;
  TP_DOMAIN *domainp = NULL;
  int error = NO_ERROR;

  DB_MAKE_NULL (limit_val);

  if (limit == NULL)
    {
      return NO_ERROR;
    }

  domainp = tp_domain_resolve_default (DB_TYPE_BIGINT);

  save_set_host_var = parser->set_host_var;
  parser->set_host_var = 1;

  assert (PT_IS_CONST (limit));
  limit_const = pt_value_to_db (parser, limit);
  if (pr_clone_value (limit_const, limit_val) != NO_ERROR)
    {
      error = ER_FAILED;
      goto cleanup;
    }
  if (pt_has_error (parser))
    {
      error = ER_FAILED;
      goto cleanup;
    }

  if (DB_IS_NULL (limit_val))
    {
      error = ER_FAILED;
      goto cleanup;
    }

  if (tp_value_coerce (limit_val, limit_val, domainp) != DOMAIN_COMPATIBLE)
    {
      error = ER_FAILED;
      goto cleanup;
    }

  if (limit->next)
    {
      DB_VALUE *range_const = NULL;
      DB_VALUE range;

      DB_MAKE_NULL (&range);

      /* LIMIT x,y => return x + y */
      assert (PT_IS_CONST (limit->next));
      range_const = pt_value_to_db (parser, limit->next);
      if (pt_has_error (parser))
	{
	  error = ER_FAILED;
	  goto cleanup;
	}

      if (DB_IS_NULL (range_const))
	{
	  error = ER_FAILED;
	  goto cleanup;
	}

      if (tp_value_coerce (range_const, &range, domainp) != DOMAIN_COMPATIBLE)
	{
	  error = ER_FAILED;
	  goto cleanup;
	}

      /* add range to current limit */
      DB_MAKE_BIGINT (limit_val,
		      DB_GET_BIGINT (limit_val) + DB_GET_BIGINT (&range));

      (void) pr_clear_value (&range);
    }

cleanup:
  if (error != NO_ERROR)
    {
      (void) pr_clear_value (limit_val);
    }

  parser->set_host_var = save_set_host_var;

  return error;
}

/*
 * pt_get_query_limit_value () - get the limit value from a query
 * return : error code or NO_ERROR
 * parser (in)	      : parser context
 * query (in)	      : query
 * limit_val (in/out) : limit value
 */
int
pt_get_query_limit_value (PARSER_CONTEXT * parser, PT_NODE * query,
			  DB_VALUE * limit_val)
{
  assert_release (limit_val != NULL);
  DB_MAKE_NULL (limit_val);

  if (query == NULL || !PT_IS_QUERY (query))
    {
      return NO_ERROR;
    }

  if (query->info.query.limit)
    {
      return pt_get_query_limit_from_limit (parser, query->info.query.limit,
					    limit_val);
    }

  if (query->info.query.orderby_for != NULL)
    {
      int error = NO_ERROR;
      bool has_limit = false;
      error = pt_get_query_limit_from_orderby_for (parser,
						   query->info.query.
						   orderby_for, limit_val,
						   &has_limit);
      if (error != NO_ERROR || !has_limit)
	{
	  return ER_FAILED;
	}
    }

  return NO_ERROR;
}

/*
 * pt_check_ordby_num_for_multi_range_opt () - checks if limit/order by for is
 *					       valid for multi range opt
 *
 * return	       : true/false
 * parser (in)	       : parser context
 * query (in)	       : query
 * mro_candidate (out) : if the only failed condition is upper limit for
 *			 orderby_num (), this plan may still use multi range
 *			 optimization if that limit changes
 * cannot_eval (out)   : upper limit is null or could not be evaluated
 */
bool
pt_check_ordby_num_for_multi_range_opt (PARSER_CONTEXT * parser,
					PT_NODE * query, bool * mro_candidate,
					bool * cannot_eval)
{
  DB_VALUE limit_val;

  int save_set_host_var;
  bool valid = false;

  assert_release (query != NULL);

  if (cannot_eval != NULL)
    {
      *cannot_eval = true;
    }
  if (mro_candidate != NULL)
    {
      *mro_candidate = false;
    }

  if (!PT_IS_QUERY (query))
    {
      return false;
    }

  DB_MAKE_NULL (&limit_val);

  save_set_host_var = parser->set_host_var;
  parser->set_host_var = 1;

  if (pt_get_query_limit_value (parser, query, &limit_val) != NO_ERROR)
    {
      goto end;
    }

  if (DB_IS_NULL (&limit_val))
    {
      goto end_mro_candidate;
    }

  if (cannot_eval)
    {
      /* upper limit was successfully evaluated */
      *cannot_eval = false;
    }
  if (DB_GET_BIGINT (&limit_val) >
      prm_get_integer_value (PRM_ID_MULTI_RANGE_OPT_LIMIT))
    {
      goto end_mro_candidate;
    }
  else
    {
      valid = true;
      goto end;
    }

end_mro_candidate:
  /* should be here if multi range optimization could not be validated because
   * upper limit is too large or it could not be evaluated. However, the query
   * may still use optimization for different host variable values.
   */
  if (mro_candidate != NULL)
    {
      *mro_candidate = true;
    }

end:
  parser->set_host_var = save_set_host_var;
  return valid;
}

/*
 * pt_get_query_limit_from_orderby_for () - get upper limit value for
 *					    orderby_for expression
 *
 * return	      : true if a valid order by for expression, else false
 * parser (in)	      : parser context
 * orderby_for (in)   : order by for node
 * upper_limit (out)  : DB_VALUE pointer that will save the upper limit
 *
 * Note:  Only operations that can reduce to ORDERBY_NUM () </<= VALUE are
 *	  allowed:
 *	  1. ORDERBY_NUM () LE/LT EXPR (which evaluates to a value).
 *	  2. EXPR (which evaluates to a values) GE/GT ORDERBY_NUM ().
 *	  3. Any number of #1 and #2 expressions linked by PT_AND logical
 *	     operator.
 *	  Lower limits are allowed.
 */
static int
pt_get_query_limit_from_orderby_for (PARSER_CONTEXT * parser,
				     PT_NODE * orderby_for,
				     DB_VALUE * upper_limit, bool * has_limit)
{
  int op;
  UNUSED_VAR PT_NODE *arg_ordby_num = NULL;
  PT_NODE *rhs = NULL;
  PT_NODE *arg1 = NULL, *arg2 = NULL;
  PT_NODE *save_next = NULL;
  DB_VALUE *limit_const = NULL;
  DB_VALUE limit;
  int error = NO_ERROR;
  bool lt = false;

  if (orderby_for == NULL || upper_limit == NULL)
    {
      return NO_ERROR;
    }

  assert_release (has_limit != NULL);

  if (!PT_IS_EXPR_NODE (orderby_for))
    {
      goto unusable_expr;
    }

  if (orderby_for->or_next != NULL)
    {
      /* OR operator is now useful */
      goto unusable_expr;
    }

  if (orderby_for->next)
    {
      /* AND operator */
      save_next = orderby_for->next;
      orderby_for->next = NULL;
      error = pt_get_query_limit_from_orderby_for (parser, orderby_for,
						   upper_limit, has_limit);
      orderby_for->next = save_next;
      if (error != NO_ERROR)
	{
	  goto unusable_expr;
	}

      return pt_get_query_limit_from_orderby_for (parser, orderby_for->next,
						  upper_limit, has_limit);
    }

  op = orderby_for->info.expr.op;

  if (op != PT_LT && op != PT_LE && op != PT_GT && op != PT_GE
      && op != PT_BETWEEN)
    {
      goto unusable_expr;
    }

  arg1 = orderby_for->info.expr.arg1;
  arg2 = orderby_for->info.expr.arg2;
  if (arg1 == NULL || arg2 == NULL)
    {
      /* safe guard */
      goto unusable_expr;
    }

  /* look for orderby_for argument */
  if (PT_IS_EXPR_NODE (arg1) && arg1->info.expr.op == PT_ORDERBY_NUM)
    {
      arg_ordby_num = arg1;
      rhs = arg2;
    }
  else if (PT_IS_EXPR_NODE (arg2) && arg2->info.expr.op == PT_ORDERBY_NUM)
    {
      arg_ordby_num = arg2;
      rhs = arg1;
      /* reverse operators */
      switch (op)
	{
	case PT_LE:
	  op = PT_GT;
	  break;
	case PT_LT:
	  op = PT_GE;
	  break;
	case PT_GT:
	  op = PT_LE;
	  break;
	case PT_GE:
	  op = PT_LT;
	  break;
	default:
	  break;
	}
    }
  else
    {
      /* orderby_for argument was not found, this is not a valid expression */
      goto unusable_expr;
    }

  if (op == PT_GE || op == PT_GT)
    {
      /* lower limits are acceptable but not useful */
      return NO_ERROR;
    }

  if (op == PT_LT)
    {
      lt = true;
    }
  else if (op == PT_BETWEEN)
    {
      PT_NODE *between_and = orderby_for->info.expr.arg2;
      UNUSED_VAR PT_NODE *between_upper = NULL;
      int between_op;

      assert (between_and != NULL && between_and->node_type == PT_EXPR);
      between_upper = between_and->info.expr.arg2;
      between_op = between_and->info.expr.op;
      switch (between_op)
	{
	case PT_BETWEEN_GT_LT:
	case PT_BETWEEN_GE_LT:
	  lt = true;
	  /* fall through */
	case PT_BETWEEN_AND:
	case PT_BETWEEN_GE_LE:
	case PT_BETWEEN_GT_LE:
	  assert (between_and->info.expr.arg2 != NULL);
	  rhs = between_and->info.expr.arg2;
	  break;
	case PT_BETWEEN_EQ_NA:
	case PT_BETWEEN_GE_INF:
	case PT_BETWEEN_GT_INF:
	  /* lower limits are acceptable but not useful */
	  return NO_ERROR;
	default:
	  /* be conservative */
	  goto unusable_expr;
	}
    }

  /* evaluate the rhs expression */
  DB_MAKE_NULL (&limit);
  if (PT_IS_CONST (rhs))
    {
      limit_const = pt_value_to_db (parser, rhs);
      if (pr_clone_value (limit_const, &limit) != NO_ERROR)
	{
	  goto unusable_expr;
	}
    }

  if (DB_IS_NULL (&limit)
      || tp_value_coerce (&limit, &limit,
			  tp_domain_resolve_default (DB_TYPE_BIGINT)) !=
      DOMAIN_COMPATIBLE)
    {
      /* has unusable upper_limit */
      (void) pr_clear_value (upper_limit);
      *has_limit = true;
      return NO_ERROR;
    }

  if (lt)
    {
      /* ORDERBY_NUM () < n => ORDERBY_NUM <= n - 1 */
      DB_MAKE_BIGINT (&limit, (DB_GET_BIGINT (&limit) - 1));
    }
  if (DB_IS_NULL (upper_limit)
      || (DB_GET_BIGINT (upper_limit) > DB_GET_BIGINT (&limit)))
    {
      /* update upper limit */
      (void) pr_clear_value (upper_limit);
      if (pr_clone_value (&limit, upper_limit) != NO_ERROR)
	{
	  goto unusable_expr;
	}
    }

  *has_limit = true;
  return NO_ERROR;

unusable_expr:
  *has_limit = false;
  (void) pr_clear_value (upper_limit);
  return ER_FAILED;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_find_node_type_pre () - Use parser_walk_tree to find a node with a
 *			      specific node type.
 *
 * return	      : node.
 * parser (in)	      : parser context.
 * node (in)	      : node in parse tree.
 * arg (in)	      : int array containing node type and found.
 * continue_walk (in) : continue walk.
 *
 * NOTE: Make sure to set found to 0 before calling parser_walk_tree.
 */
PT_NODE *
pt_find_node_type_pre (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
		       int *continue_walk)
{
  PT_NODE_TYPE node_type = *((int *) arg);
  int *found_p = ((int *) arg) + 1;

  if (*found_p || *continue_walk == PT_STOP_WALK || node == NULL)
    {
      return node;
    }
  if (node->node_type == node_type)
    {
      *found_p = 1;
      *continue_walk = PT_STOP_WALK;
    }
  return node;
}

/*
 * pt_find_op_type_pre () - Use parser_walk_tree to find an operator of a
 *			    specific type.
 *
 * return	      : node.
 * parser (in)	      : parser context.
 * node (in)	      : node in parse tree.
 * arg (in)	      : int array containing expr type and found.
 * continue_walk (in) : continue walk.
 *
 * NOTE: Make sure to set found to 0 before calling parser_walk_tree.
 */
PT_NODE *
pt_find_op_type_pre (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
		     int *continue_walk)
{
  PT_OP_TYPE op_type = *((int *) arg);
  int *found_p = ((int *) arg) + 1;

  if (*found_p || *continue_walk == PT_STOP_WALK || node == NULL)
    {
      return node;
    }
  if (node->node_type == PT_EXPR && node->info.expr.op == op_type)
    {
      *found_p = 1;
      *continue_walk = PT_STOP_WALK;
    }
  return node;
}
#endif

/*
 * pt_free_statement_xasl_id () - free XASL_ID object of a prepared statement
 * return : void
 * statement (in) : statement
 */
void
pt_free_statement_xasl_id (PT_NODE * statement)
{
  if (statement == NULL)
    {
      return;
    }

  if (statement->xasl_id != NULL)
    {
      free_and_init (statement->xasl_id);
    }

}

/*
 * pt_recompile_for_limit_optimizations () - verify is query plan should be
 *					     regenerated for a query due to
 *					     limit change
 * return : true/false
 * parser (in) : parser context
 * statement (in) : statement
 * xasl_flag (in) : flag which contains plan information
 *
 * Note: before executing a query plan, we have to verify if the generated
 *  plan is still valid with the new limit values. There are four cases:
 *   I. MRO is used but the new limit value is either too large or invalid
 *      for this plan. In this case we have to recompile.
 *  II. MRO is not used but the new limit value is valid for generating such
 *	a plan. In this case we have to recompile.
 * III. MRO is not used and the new limit value is invalid for generating this
 *	plan. In this case we don't have to recompile
 *  VI. MRO is used and the new limit value is valid for this plan. In this
 *	case we don't have to recompile
 *  The same rules apply to SORT-LIMIT plans
 */
bool
pt_recompile_for_limit_optimizations (PARSER_CONTEXT * parser,
				      PT_NODE * statement, int xasl_flag)
{
  DB_VALUE limit_val;
  DB_BIGINT val = 0;
  int limit_opt_flag =
    (MRO_CANDIDATE | MRO_IS_USED | SORT_LIMIT_CANDIDATE | SORT_LIMIT_USED);

  if (!(xasl_flag & limit_opt_flag))
    {
      return false;
    }

  if (pt_get_query_limit_value (parser, statement, &limit_val) != NO_ERROR)
    {
      val = 0;
    }
  else if (DB_IS_NULL (&limit_val))
    {
      val = 0;
    }
  else
    {
      val = DB_GET_BIGINT (&limit_val);
    }

  /* verify MRO */
  if (xasl_flag & (MRO_CANDIDATE | MRO_IS_USED))
    {
      if (val == 0
	  || val >
	  (DB_BIGINT) prm_get_integer_value (PRM_ID_MULTI_RANGE_OPT_LIMIT))
	{
	  if (xasl_flag & MRO_IS_USED)
	    {
	      /* Need to recompile because limit is not suitable for MRO
	       * anymore
	       */
	      return true;
	    }
	}
      else if ((xasl_flag & MRO_CANDIDATE) && !(xasl_flag & MRO_IS_USED))
	{
	  /* Suitable limit for MRO but MRO is not used. Recompile to use
	   * MRO
	   */
	  return true;
	}
      return false;
    }

  /* verify SORT-LIMIT */
  if (xasl_flag & (SORT_LIMIT_CANDIDATE | SORT_LIMIT_USED))
    {
      if (val == 0
	  || val >
	  (DB_BIGINT) prm_get_integer_value (PRM_ID_SORT_LIMIT_MAX_COUNT))
	{
	  if (xasl_flag & SORT_LIMIT_USED)
	    {
	      /* Need to recompile because limit is not suitable for
	       * SORT-LIMIT anymore
	       */
	      return true;
	    }
	}
      else if ((xasl_flag & SORT_LIMIT_CANDIDATE)
	       && !(xasl_flag & SORT_LIMIT_USED))
	{
	  /* Suitable limit for SORT-LIMIT but SORT-LIMIT is not used.
	   * Recompile to use SORT-LIMIT
	   */
	  return true;
	}
      return false;
    }

  return false;
}

/*
 * pt_make_query_explain () -
 *   return: node
 */
PT_NODE *
pt_make_query_explain (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  PT_NODE *select, *exp_func;

  if (!PT_IS_QUERY (statement))
    {
      assert (false);
      return NULL;
    }

  select = parser_new_node (parser, PT_SELECT);
  if (select == NULL)
    {
      return NULL;
    }

  exp_func =
    parser_make_expression (parser, PT_EXPLAIN, statement, NULL, NULL);
  if (exp_func == NULL)
    {
      return NULL;
    }

  exp_func->alias_print = pt_append_string (parser, NULL, "explain");
  select->info.query.q.select.list =
    parser_append_node (exp_func, select->info.query.q.select.list);

  return select;
}

/*
 * pt_make_query_show_trace () -
 *   return: node
 */
PT_NODE *
pt_make_query_show_trace (PARSER_CONTEXT * parser)
{
  PT_NODE *select, *trace_func;

  select = parser_new_node (parser, PT_SELECT);
  if (select == NULL)
    {
      return NULL;
    }

  trace_func =
    parser_make_expression (parser, PT_TRACE_STATS, NULL, NULL, NULL);
  if (trace_func == NULL)
    {
      return NULL;
    }

  trace_func->alias_print = pt_append_string (parser, NULL, "trace");
  select->info.query.q.select.list =
    parser_append_node (trace_func, select->info.query.q.select.list);

  parser->query_trace = false;

  return select;
}

/*
 * pt_has_non_groupby_column_node () - Use parser_walk_tree to check having
 *                                     clause.
 * return	      : node.
 * parser (in)	      : parser context.
 * node (in)	      : name node in having clause.
 * arg (in)	      : pt_non_groupby_col_info
 * continue_walk (in) : continue walk.
 *
 * NOTE: Make sure to set has_non_groupby_col to false before calling
 *       parser_walk_tree.
 */
PT_NODE *
pt_has_non_groupby_column_node (PARSER_CONTEXT * parser, PT_NODE * node,
				void *arg, int *continue_walk)
{
  PT_NON_GROUPBY_COL_INFO *info = NULL;
  PT_NODE *groupby_p = NULL;

  if (arg == NULL)
    {
      assert (false);
      return node;
    }

  info = (PT_NON_GROUPBY_COL_INFO *) arg;
  groupby_p = info->groupby;

  if (node == NULL || groupby_p == NULL)
    {
      assert (false);
      return node;
    }

  if (!PT_IS_NAME_NODE (node))
    {
      return node;
    }

  for (; groupby_p; groupby_p = groupby_p->next)
    {
      if (!(PT_IS_SORT_SPEC_NODE (groupby_p)
	    && PT_IS_NAME_NODE (groupby_p->info.sort_spec.expr)))
	{
	  continue;
	}

      if (pt_name_equal (parser, groupby_p->info.sort_spec.expr, node))
	{
	  return node;
	}
    }

  /* the name node is not associated with groupby columns. */
  info->has_non_groupby_col = true;
  *continue_walk = PT_STOP_WALK;

  return node;
}

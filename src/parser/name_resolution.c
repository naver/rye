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
 * name_resolution.c - resolving related functions
 */

#ident "$Id$"

#include "config.h"

#include <assert.h>

#include "porting.h"
#include "error_manager.h"
#include "parser.h"
#include "parser_message.h"
#include "semantic_check.h"
#include "dbtype.h"
#include "object_domain.h"
#include "memory_alloc.h"
#include "intl_support.h"
#include "memory_hash.h"
#include "system_parameter.h"
#include "object_print.h"
#include "execute_schema.h"
#include "schema_manager.h"
#include "transform.h"
#include "execute_statement.h"

/* this must be the last header file included!!! */
#include "dbval.h"

extern int parser_function_code;


#define PT_NAMES_HASH_SIZE                50

typedef struct scopes SCOPES;
struct scopes
{
  SCOPES *next;			/* next outermost scope         */
  PT_NODE *specs;		/* list of PT_SPEC nodes */
  unsigned short correlation_level;
  /* how far up the stack was a name found? */
  short location;		/* for outer join */
};

typedef struct pt_bind_names_arg PT_BIND_NAMES_ARG;
struct pt_bind_names_arg
{
  SCOPES *scopes;
  SEMANTIC_CHK_INFO *sc_info;
};

enum
{
  REQUIRE_ALL_MATCH = false,
  DISCARD_NO_MATCH = true
};

static const char *CPTR_PT_NAME_IN_GROUP_HAVING = "name_in_group_having";

static PT_NODE *pt_bind_name_or_path_in_scope (PARSER_CONTEXT * parser,
					       PT_BIND_NAMES_ARG * bind_arg,
					       PT_NODE * in_node);
static void pt_bind_type_of_host_var (PARSER_CONTEXT * parser, PT_NODE * hv);
static void pt_bind_types (PARSER_CONTEXT * parser, PT_NODE * spec);
static void pt_bind_scope (PARSER_CONTEXT * parser,
			   PT_BIND_NAMES_ARG * bind_arg);
static PT_NODE *pt_mark_location (PARSER_CONTEXT * parser, PT_NODE * node,
				  void *arg, int *continue_walk);
static PT_NODE *pt_bind_names_post (PARSER_CONTEXT * parser, PT_NODE * node,
				    void *arg, int *continue_walk);
static PT_NODE *pt_bind_names (PARSER_CONTEXT * parser, PT_NODE * node,
			       void *arg, int *continue_walk);
static int pt_find_attr_in_class_list (PARSER_CONTEXT * parser,
				       PT_NODE * flat, PT_NODE * attr);
static int pt_find_name_in_spec (PARSER_CONTEXT * parser, PT_NODE * spec,
				 PT_NODE * name);
static int pt_check_unique_exposed (PARSER_CONTEXT * parser,
				    const PT_NODE * p);
static PT_NODE *pt_get_all_attributes_and_types (PARSER_CONTEXT * parser,
						 PT_NODE * cls,
						 PT_NODE * from);
static void pt_get_attr_data_type (PARSER_CONTEXT * parser,
				   DB_ATTRIBUTE * att, PT_NODE * attr);
static PT_NODE *pt_resolve_correlation (PARSER_CONTEXT * parser,
					PT_NODE * in_node, PT_NODE * scope,
					PT_NODE * exposed_spec, int col_name,
					PT_NODE ** p_entity);
static PT_NODE *pt_get_resolution (PARSER_CONTEXT * parser,
				   PT_BIND_NAMES_ARG * bind_arg,
				   PT_NODE * scope, PT_NODE * in_node,
				   PT_NODE ** p_entity, int col_name);
static PT_NODE *pt_is_correlation_name (PARSER_CONTEXT * parser,
					PT_NODE * scope, PT_NODE * nam);
static PT_NODE *pt_is_on_list (PARSER_CONTEXT * parser, const PT_NODE * p,
			       const PT_NODE * list);
static PT_NODE *pt_name_list_union (PARSER_CONTEXT * parser, PT_NODE * list,
				    PT_NODE * additions);
static PT_NODE *pt_make_subclass_list (PARSER_CONTEXT * parser,
				       DB_OBJECT * db, int line_num,
				       int col_num, UINTPTR id,
				       PT_MISC_TYPE meta_class,
				       MHT_TABLE * names_mht);
static PT_NODE *pt_make_flat_name_list (PARSER_CONTEXT * parser,
					PT_NODE * spec,
					PT_NODE * spec_parent);
static int pt_must_have_exposed_name (PARSER_CONTEXT * parser, PT_NODE * p);
static PT_NODE *pt_object_to_data_type (PARSER_CONTEXT * parser,
					PT_NODE * class_list);
static int pt_resolve_hint_args (PARSER_CONTEXT * parser, PT_NODE ** arg_list,
				 PT_NODE * spec_list, bool discard_no_match);
static int pt_resolve_hint (PARSER_CONTEXT * parser, PT_NODE * node);
static PT_NODE *pt_find_outer_entity_in_scopes (PARSER_CONTEXT * parser,
						SCOPES * scopes,
						UINTPTR spec_id,
						short *scope_location);
static PT_NODE *pt_undef_names_pre (PARSER_CONTEXT * parser, PT_NODE * node,
				    void *arg, int *continue_walk);
static PT_NODE *pt_undef_names_post (PARSER_CONTEXT * parser, PT_NODE * node,
				     void *arg, int *continue_walk);
static void fill_in_insert_default_function_arguments (PARSER_CONTEXT *
						       parser,
						       PT_NODE * const node);

static const char *pt_get_unique_exposed_name (PARSER_CONTEXT * parser,
					       PT_NODE * first_spec);

static bool is_pt_name_in_group_having (PT_NODE * node);

static PT_NODE *pt_mark_pt_name (PARSER_CONTEXT * parser, PT_NODE * node,
				 void *chk_parent, int *continue_walk);

static PT_NODE *pt_mark_group_having_pt_name (PARSER_CONTEXT * parser,
					      PT_NODE * node,
					      void *chk_parent,
					      int *continue_walk);

static void pt_resolve_group_having_alias_pt_sort_spec (PARSER_CONTEXT *
							parser,
							PT_NODE * node,
							PT_NODE *
							select_list);

static void pt_resolve_group_having_alias_pt_name (PARSER_CONTEXT * parser,
						   PT_NODE ** node_p,
						   PT_NODE * select_list);

static void pt_resolve_group_having_alias_pt_expr (PARSER_CONTEXT * parser,
						   PT_NODE * node,
						   PT_NODE * select_list);

static void pt_resolve_group_having_alias_internal (PARSER_CONTEXT * parser,
						    PT_NODE ** node_p,
						    PT_NODE * select_list);

static PT_NODE *pt_resolve_group_having_alias (PARSER_CONTEXT * parser,
					       PT_NODE * node,
					       void *chk_parent,
					       int *continue_walk);

/*
 * pt_undef_names_pre () - Set error if name matching spec is found. Used in
 *			   insert to make sure no "correlated" names are used
 * 			   in subqueries.
 *
 * return	      : Unchanged node argument.
 * parser (in)	      : Parser context.
 * node (in)	      : Parse tree node.
 * arg (in)	      : Insert spec.
 * continue_walk (in) : Continue walk.
 *
 * NOTE: Insert spec will store in etc the correlation level in regard with
 *	 INSERT VALUE clause. Only if level is greater than 0, names should
 *	 be undefined. INSERT INTO SET attr_i = EXPR (attr_j1, attr_j2, ...)
 *	 is allowed.
 *	 If etc is NULL, correlation level is ignored and all names are
 *	 undefined.
 */
static PT_NODE *
pt_undef_names_pre (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
		    int *continue_walk)
{
  PT_NODE *spec = (PT_NODE *) arg;
  short *level_p = NULL;

  if (spec == NULL)
    {
      *continue_walk = PT_STOP_WALK;
      return node;
    }

  level_p = (short *) spec->etc;

  switch (node->node_type)
    {
    case PT_NAME:
      if (level_p == NULL || *level_p > 0)
	{
	  /* Using "correlated" names in INSERT VALUES clause is incorrect
	   * except when they are arguments of the DEFAULT() function.
	   */
	  if (node->info.name.spec_id == spec->info.spec.id &&
	      !PT_NAME_INFO_IS_FLAGED (node, PT_NAME_INFO_FILL_DEFAULT))
	    {
	      int save_custom_print = parser->custom_print;
	      parser->custom_print |= PT_SUPPRESS_RESOLVED;
	      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_IS_NOT_DEFINED,
			  pt_short_print (parser, node));
	      parser->custom_print = save_custom_print;
	    }
	}
      break;
    case PT_SELECT:
    case PT_UNION:
    case PT_INTERSECTION:
    case PT_DIFFERENCE:
    case PT_INSERT:
      if (level_p != NULL)
	{
	  (*level_p)++;
	}
      break;
    default:
      break;
    }

  return node;
}

/*
 * pt_undef_names_post () - Function to be used with pt_undef_names_pre. Helps
 *			    with counting the correlation level.
 *
 * return	      : Unchanged node argument.
 * parser (in)	      : Parser context.
 * node (in)	      : Parse tree node.
 * arg (in)	      : Insert spec.
 * continue_walk (in) : Continue walk.
 */
static PT_NODE *
pt_undef_names_post (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		     void *arg, UNUSED_ARG int *continue_walk)
{
  PT_NODE *spec = (PT_NODE *) arg;
  short *level_p = NULL;

  if (spec == NULL)
    {
      return node;
    }

  level_p = (short *) spec->etc;
  if (level_p == NULL)
    {
      return node;
    }

  switch (node->node_type)
    {
    case PT_SELECT:
    case PT_UNION:
    case PT_INTERSECTION:
    case PT_DIFFERENCE:
    case PT_INSERT:
      (*level_p)--;
      break;
    default:
      break;
    }

  return node;
}

/*
 * pt_resolved() -  check if this path expr was previously resolved
 *   return:  true if expr was previously resolved
 *   expr(in): a path expression
 */

int
pt_resolved (const PT_NODE * expr)
{
  if (expr)
    {
      switch (expr->node_type)
	{
	case PT_NAME:
	  return (expr->info.name.spec_id != 0);
	case PT_DOT_:
	  return (pt_resolved (expr->info.dot.arg1)
		  && pt_resolved (expr->info.dot.arg2));
	default:
	  break;
	}
    }
  return 0;
}


/*
 * pt_bind_name_or_path_in_scope() - tries to resolve in_node using all the
 *     entity_spec_lists in scopes and returns in_node if successfully resolved
 *   return:  in_node's resolution if successful, NULL if in_node is unresolved
 *   parser(in): the parser context
 *   bind_arg(in): a list of scopes for resolving names & path expressions
 *   in_node(in): an attribute reference or path expression to be resolved
 *
 * Note :
 * Unfortunately, we can't push the check for naked parameters
 * into pt_get_resolution() because parameters that have the
 * name as an attribute of some (possibly enclosing) scope must be
 * to the attribute and not the parameter (by our convention).
 * when naked parameters are eliminated, this mickey mouse stuff
 * will go away...
 */
static PT_NODE *
pt_bind_name_or_path_in_scope (PARSER_CONTEXT * parser,
			       PT_BIND_NAMES_ARG * bind_arg,
			       PT_NODE * in_node)
{
  PT_NODE *prev_entity = NULL;
  PT_NODE *node = NULL;
  SCOPES *scopes = bind_arg->scopes;
  SCOPES *scope;
  int level = 0;
  PT_NODE *temp, *entity;
  short scope_location;
  bool error_saved = false;

  /* skip hint argument name, index name */
  if (in_node->node_type == PT_NAME
      && (in_node->info.name.meta_class == PT_HINT_NAME
	  || in_node->info.name.meta_class == PT_INDEX_NAME))
    {
      return in_node;
    }

  /* skip resolved nodes */
  if (pt_resolved (in_node))
    {
      return in_node;
    }

  if (er_errid () != NO_ERROR)
    {
      er_stack_push ();
      error_saved = true;
    }

  /* resolve all name nodes and path expressions */
  if (scopes)
    {
      for (scope = scopes; scope != NULL; scope = scope->next)
	{
	  node = pt_get_resolution (parser, bind_arg, scope->specs, in_node,
				    &prev_entity, 1);
	  if (node)
	    {
	      break;
	    }
	  level++;
	}
    }
  else
    {
      /* resolve class attributes
       * and anything else that can be resolved without an enclosing scope.
       */
      node = pt_get_resolution (parser, bind_arg, NULL, in_node,
				&prev_entity, 1);
    }

  if (node)
    {
      /* set the correlation of either the name or arg2 of the path */
      /* expression. */
      if (node->node_type == PT_DOT_)
	{
	  node->info.dot.arg2->info.name.correlation_level = level;
	}
      else
	{
	  node->info.name.correlation_level = level;
	}

      /* all  is well, name is resolved */
      /* check correlation level of scope */
      scope = scopes;
      while (level > 0 && scope)
	{
	  /* it was correlated. Choose the closest correlation scope.
	   * That is the same as choosing the smallest non-zero number.
	   */
	  if (!scope->correlation_level
	      || scope->correlation_level > (unsigned short) level)
	    {
	      scope->correlation_level = level;
	    }
	  level = level - 1;
	  scope = scope->next;
	}
    }

  if (node == NULL)
    {
      /* If pt_name in group by/ having, maybe it's alias. We will try
       * to resolve it later.
       */
      if (is_pt_name_in_group_having (in_node) == false)
	{
	  if (!pt_has_error (parser))
	    {
	      if (er_errid () != NO_ERROR)
		{
		  PT_ERRORc (parser, in_node, er_msg ());
		}
	      else
		{
		  PT_ERRORmf (parser, in_node, MSGCAT_SET_PARSER_SEMANTIC,
			      MSGCAT_SEMANTIC_IS_NOT_DEFINED,
			      pt_short_print (parser, in_node));
		}
	    }
	}
    }
  else
    {
      /* outer join restriction check */
      for (temp = node; temp->node_type == PT_DOT_;
	   temp = temp->info.dot.arg2)
	{
	  ;
	}
      if (temp->node_type == PT_NAME && temp->info.name.location > 0)
	{
	  /* PT_NAME node within outer join condition */
	  if (temp != node)
	    {
	      /* node->node_type is PT_DOT_; that menas path expression */
	      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_OUTERJOIN_PATH_EXPR,
			  pt_short_print (parser, node));
	      node = NULL;
	    }
	  else
	    {
	      /* check scope */
	      scope_location = temp->info.name.location;
	      entity = pt_find_outer_entity_in_scopes
		(parser, scopes, temp->info.name.spec_id, &scope_location);
	      if (!entity)
		{
		  /* cannot resolve within the outer join scope */
		  PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			      MSGCAT_SEMANTIC_OUTERJOIN_SCOPE,
			      pt_short_print (parser, node));
		  node = NULL;
		}
	      else if (entity->info.spec.location < 0
		       || (entity->info.spec.location >
			   temp->info.name.location)
		       || scope_location > entity->info.spec.location)
		{
		  /* cannot resolve within the outer join scope */
		  PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			      MSGCAT_SEMANTIC_OUTERJOIN_SCOPE,
			      pt_short_print (parser, node));
		  node = NULL;
		}
	    }
	}
    }

  if (error_saved)
    {
      er_stack_pop ();
    }

  return node;
}


/*
 * pt_bind_type_of_host_var() -  set the type of a host variable to
 *                               the type of its DB_VALUE
 *   return:  none
 *   parser(in/out): the parser context
 *   hv(in/out): an input host variable
 */
static void
pt_bind_type_of_host_var (PARSER_CONTEXT * parser, PT_NODE * hv)
{
  DB_VALUE *val = NULL;

  val = pt_host_var_db_value (parser, hv);
  if (val)
    {
      (void) pt_bind_type_from_dbval (parser, hv, val);
    }
  /* else :
     There isn't a host var yet.  This happens if someone does a
     db_compile_statement before doing db_push_values, as might
     happen in a dynamic esql PREPARE statement where the host
     vars might not be supplied until some later EXECUTE or OPEN
     CURSOR statement.
     In this case, we'll have to rely on pt_coerce_value and
     pt_value_to_dbval to fix things up later. */
}

/*
 * pt_bind_types() -  bind name types for a derived table.
 *   return:  void
 *   parser(in/out): the parser context
 *   spec(in/out): an entity spec describing a derived table
 *
 * Note :
 * if spec.derived_table_type is set expr then assert: spec.derived_table.type
 * is a set type in any case, check that:
 *   the number of derived columns in spec.as_attr_list matches
 *   the number of attributes in spec.derived_table.select_list
 * foreach column c in spec.as_attr_list and foreach attribute a
 * in spec.derived_table.select_list do make c assume a's datatype and
 * tag it with spec's id
 */
static void
pt_bind_types (PARSER_CONTEXT * parser, PT_NODE * spec)
{
  PT_NODE *derived_table, *cols, *select_list, *col, *att;
  int col_cnt, attr_cnt;

  if (!parser
      || !spec
      || spec->node_type != PT_SPEC
      || (derived_table = spec->info.spec.derived_table) == NULL)
    {
      return;
    }


  if (spec->info.spec.as_attr_list == NULL)
    {
      PT_NODE *range_var;
      int i, id;

      /* must be a subquery derived table */
      /* select_list must have passed star expansion */
      select_list = pt_get_select_list (parser, derived_table);
      if (!select_list)
	{
	  return;
	}

      range_var = spec->info.spec.range_var;
      for (att = select_list, i = 0; att; att = att->next, i++)
	{
	  if (att->alias_print)
	    {
	      col = pt_name (parser, att->alias_print);
	    }
	  else
	    {
	      if (att->node_type == PT_NAME
		  && att->info.name.original != NULL
		  && att->info.name.original[0] != '\0')
		{
		  col = pt_name (parser, att->info.name.original);
		}
	      else if (att->node_type == PT_VALUE
		       && att->info.value.text != NULL
		       && att->info.value.text[0] != '\0')
		{
		  col = pt_name (parser, att->info.value.text);
		}
	      else
		{		/* generate column name */
		  id = i;
		  col = pt_name (parser, mq_generate_name
				 (parser,
				  range_var->info.name.original, &id));
		}
	    }

	  spec->info.spec.as_attr_list =
	    parser_append_node (col, spec->info.spec.as_attr_list);
	}
    }				/* if (spec->info.spec.as_attr_list == NULL) */

  cols = spec->info.spec.as_attr_list;
  col_cnt = pt_length_of_list (cols);

  /* must be a subquery derived table */
  /* select_list must have passed star expansion */
  select_list = pt_get_select_list (parser, derived_table);
  if (!select_list)
    {
      return;
    }

  /* select_list attributes must match derived columns in number */
  attr_cnt = pt_length_of_select_list (select_list, INCLUDE_HIDDEN_COLUMNS);
  if (col_cnt != attr_cnt)
    {
      PT_ERRORmf3 (parser, spec, MSGCAT_SET_PARSER_SEMANTIC,
		   MSGCAT_SEMANTIC_ATT_CNT_NE_DERIVED_C,
		   pt_short_print (parser, spec), attr_cnt, col_cnt);
    }
  else
    {
      /* derived columns assume the type of their matching attributes */
      for (col = cols, att = select_list;
	   col && att; col = col->next, att = att->next)
	{
	  col->type_enum = att->type_enum;
	  if (att->data_type)
	    {
	      col->data_type = parser_copy_tree_list (parser, att->data_type);
	    }

	  /* tag it as resolved */
	  col->info.name.spec_id = spec->info.spec.id;
	  if (col->info.name.meta_class == 0)
	    {
	      /* only set it to PT_NORMAL if it wasn't set before */
	      col->info.name.meta_class = PT_NORMAL;
	    }
	}
    }
}

/*
 * pt_bind_scope() -  bind names and types of derived tables in current scope.
 *   return:  void
 *   parser(in): the parser context
 *   bind_arg(in/out): a list of scopes with the current scope on "top"
 *
 * Note :
 * this definition of a derived table's scope allows us to resolve the 3rd f in
 *   select f.ssn from faculty1 f, (select ssn from faculty g where f=g) h(ssn)
 * and still catch the 1st illegal forward reference to f in
 *   select n from (select ssn from faculty g where f=g) h(n), faculty f
 */
static void
pt_bind_scope (PARSER_CONTEXT * parser, PT_BIND_NAMES_ARG * bind_arg)
{
  SCOPES *scopes = bind_arg->scopes;
  PT_NODE *spec;
  PT_NODE *table;
  PT_NODE *next;

  spec = scopes->specs;
  if (!spec)
    {
      return;
    }

  table = spec->info.spec.derived_table;
  if (table)
    {
      /* evaluate the names of the current table.
       * The name scope of the first spec, is only the outer level scopes.
       * The outer scopes are pointed to by scopes->next.
       * The null "scopes" spec is kept to maintain correlation
       * level calculation.
       */
      next = scopes->specs;
      scopes->specs = NULL;
      table = parser_walk_tree (parser, table, pt_bind_names, bind_arg,
				pt_bind_names_post, bind_arg);
      spec->info.spec.derived_table = table;
      scopes->specs = next;

      /* must bind any expr types in table. pt_bind_types requires it. */
      table = pt_semantic_type (parser, table, bind_arg->sc_info);
      spec->info.spec.derived_table = table;

      pt_bind_types (parser, spec);
    }

  while (spec->next)
    {
      next = spec->next;	/* save next spec pointer */
      /* The scope of table is the current scope plus the previous tables.
       * By temporarily nulling the previous next pointer, the scope
       * is restricted at this level to the previous spec's. */
      spec->next = NULL;
      table = next->info.spec.derived_table;
      if (table)
	{
	  table = parser_walk_tree (parser, table, pt_bind_names, bind_arg,
				    pt_bind_names_post, bind_arg);
	  if (table)
	    {
	      next->info.spec.derived_table = table;
	    }

	  /* must bind any expr types in table. pt_bind_types requires it. */
	  table = pt_semantic_type (parser, table, bind_arg->sc_info);
	  next->info.spec.derived_table = table;

	  pt_bind_types (parser, next);
	}
      spec->next = next;
      spec = next;
    }

}

/*
 * pt_mark_location () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
pt_mark_location (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		  void *arg, UNUSED_ARG int *continue_walk)
{
  short *location = (short *) arg;

  switch (node->node_type)
    {
    case PT_EXPR:
      node->info.expr.location = *location;
      break;
    case PT_NAME:
      node->info.name.location = *location;
      break;
    case PT_VALUE:
      node->info.value.location = *location;
      break;
    default:
      break;
    }

  return node;
}				/* pt_mark_location() */

/*
 * pt_set_is_view_spec () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
PT_NODE *
pt_set_is_view_spec (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		     UNUSED_ARG void *arg, UNUSED_ARG int *continue_walk)
{
  if (!node)
    {
      return node;
    }

  if (pt_is_query (node))
    {
      /* Reset query id # */
      node->info.query.id = (UINTPTR) node;
      node->info.query.is_view_spec = 1;
    }

  return node;
}				/* pt_set_is_view_spec */

/*
 * pt_bind_names_post() -  bind names & path expressions of this statement node
 *   return:  node
 *   parser(in): the parser context
 *   node(in): a parse tree node
 *   arg(in): a list of scopes for resolving names & path expressions
 *   continue_walk(in): flag that tells when to stop tree traversal
 */
static PT_NODE *
pt_bind_names_post (PARSER_CONTEXT * parser,
		    PT_NODE * node, void *arg, int *continue_walk)
{
  PT_BIND_NAMES_ARG *bind_arg = (PT_BIND_NAMES_ARG *) arg;

  *continue_walk = PT_CONTINUE_WALK;

  if (!node)
    {
      return node;
    }

  switch (node->node_type)
    {
    case PT_UPDATE:
      {
	PT_NODE *temp, *lhs, *assignments = NULL, *spec = NULL;
	int error = NO_ERROR;

	assignments = node->info.update.assignment;
	spec = node->info.update.spec;
	assert (spec != NULL);

	/* this is only to eliminate oid names from lhs of assignments
	 * per ANSI. This is because name resolution for OID's conflicts
	 * with ANSI.
	 */
	for (temp = assignments; temp && error == NO_ERROR; temp = temp->next)
	  {
	    for (lhs = temp->info.expr.arg1;
		 lhs && error == NO_ERROR; lhs = lhs->next)
	      {
		if (lhs->node_type == PT_NAME)
		  {
		    /* make it print like ANSI */
		    if (lhs->info.name.meta_class == PT_OID_ATTR)
		      {
			/* must re-resolve the name */
			lhs->info.name.original = lhs->info.name.resolved;
			lhs->info.name.spec_id = 0;
			if (!pt_find_name_in_spec (parser, spec, lhs))
			  {
			    error = MSGCAT_SEMANTIC_IS_NOT_DEFINED;
			    PT_ERRORmf (parser, lhs,
					MSGCAT_SET_PARSER_SEMANTIC, error,
					pt_short_print (parser, lhs));
			  }
		      }
		  }
	      }
	  }
      }

      break;

    case PT_VALUE:
      {
	PT_NODE *elem;
	int is_function = 0;

	if (node->type_enum == PT_TYPE_STAR)
	  {
	    break;
	  }

	if (node && PT_IS_COLLECTION_TYPE (node->type_enum))
	  {
	    pt_semantic_type (parser, node, bind_arg->sc_info);
	    if (node == NULL)
	      {
		return node;
	      }

	    elem = node->info.value.data_value.set;
	    while (elem)
	      {
		if (elem->node_type != PT_VALUE)
		  {
		    is_function = 1;
		    break;
		  }
		elem = elem->next;
	      }
	  }

	if (!is_function)
	  {
	    if (pt_value_to_db (parser, node) == NULL)
	      {
		parser_free_tree (parser, node);
		node = NULL;
	      }
	    else
	      {
		/*
		 * pt_value_to_db has already filled the contents
		 * of node->info.value.db_value; we don't need to
		 * repeat that work here.
		 * The type info from "node" drove that process, and
		 * is assumed to be good.  If there was any touch-up
		 * work required (e.g., adding a data_type node),
		 * pt_value_to_db took care of it.
		 */
	      }
	  }
	else
	  {
	    PT_NODE *arg_list = node->info.value.data_value.set;
	    /* roll back error messages for set values.
	     * this is a set function reference, which we just
	     * realized, since the syntax for constant sets
	     * and set functions is the same.
	     * Convert the node to a set function.
	     */
	    node->node_type = PT_FUNCTION;
	    /* make info set up properly */
	    memset (&(node->info), 0, sizeof (node->info));
	    node->info.function.arg_list = arg_list;

	    if (node->type_enum == PT_TYPE_SEQUENCE)
	      {
		node->info.function.function_type = F_SEQUENCE;
	      }
	    else
	      {
		node->info.function.function_type = (FUNC_TYPE) 0;

		/* now we need to type the innards of the set ... */
		/* first we tag this not typed so the type will
		 * be recomputed from scratch. */
		node->type_enum = PT_TYPE_NONE;
	      }

	    pt_semantic_type (parser, node, bind_arg->sc_info);
	  }

      }
      break;

    case PT_EXPR:
      (void) pt_instnum_compatibility (node);
      break;

    default:
      break;
    }

  return node;
}

void
pt_set_fill_default_in_path_expression (PT_NODE * node)
{
  if (node == NULL)
    {
      return;
    }
  if (node->node_type == PT_DOT_)
    {
      pt_set_fill_default_in_path_expression (node->info.dot.arg1);
      pt_set_fill_default_in_path_expression (node->info.dot.arg2);
    }
  else if (node->node_type == PT_NAME)
    {
      PT_NAME_INFO_SET_FLAG (node, PT_NAME_INFO_FILL_DEFAULT);

      /* We also need to clear the spec id because this PT_NAME node might be
         a copy of a node that has been resolved without filling in the
         default value. The parser_copy_tree() call in
         fill_in_insert_default_function_arguments() is an example.
         We mark the current node as not resolved so that it is resolved
         again, this time filling in the default value. */
      node->info.name.spec_id = 0;
    }
  else
    {
      assert (false);
    }
}

/*
 * fill_in_insert_default_function_arguments () - Fills in the argument of the
 *                                                DEFAULT function when used
 *                                                for INSERT
 *   parser(in):
 *   node(in):
 * Note: When parsing statements such as "INSERT INTO tbl VALUES (1, DEFAULT)"
 *       the column names corresponding to DEFAULT values are not yet known.
 *       When performing name resolution the names and default values can be
 *       filled in.
 */
static void
fill_in_insert_default_function_arguments (PARSER_CONTEXT * parser,
					   PT_NODE * const node)
{
  PT_NODE *crt_attr = NULL;
  PT_NODE *crt_value = NULL;
  PT_NODE *crt_list = NULL;
  SM_CLASS *smclass = NULL;
  SM_ATTRIBUTE *attr = NULL;
  PT_NODE *cls_name = NULL;
  PT_NODE *values_list = NULL;
  PT_NODE *attrs_list = NULL;

  assert (node->node_type == PT_INSERT);

  /* if an attribute has a default expression as default value
   * and that expression refers to the current date and time,
   * then we make sure that we mark this statement as one that
   * needs the system datetime from the server
   */
  cls_name = node->info.insert.spec->info.spec.entity_name;
  values_list = node->info.insert.value_clauses;
  attrs_list = pt_attrs_part (node);

  au_fetch_class_force (cls_name->info.name.db_object, &smclass, S_LOCK);
  if (smclass)
    {
      for (attr = smclass->attributes; attr != NULL; attr = attr->next)
	{
	  if (DB_IS_DATETIME_DEFAULT_EXPR (attr->default_value.default_expr))
	    {
	      node->si_datetime = true;
	      db_make_null (&parser->sys_datetime);
	      break;
	    }
	}
    }

  for (crt_list = values_list; crt_list != NULL; crt_list = crt_list->next)
    {
      /*
       * If the statement such as "INSERT INTO tbl DEFAULT" is given,
       * we rewrite it to "INSERT INTO tbl VALUES (DEFAULT, DEFAULT, ...)"
       * to support "server-side insertion" simply.
       * In this situation, the server will get "default value" from
       * "original_value" of the current representation, but sometimes
       * it is not the latest default value. (See the comment for sm_attribute
       * structure on class_object.h for more information.)
       * However, the client always knows it, so it's better for the server to
       * get the value from the client.
       */
      if (crt_list->info.node_list.list_type == PT_IS_DEFAULT_VALUE)
	{
	  PT_NODE *crt_value_list = NULL;

	  assert (crt_list->info.node_list.list == NULL);

	  for (crt_attr = attrs_list; crt_attr != NULL;
	       crt_attr = crt_attr->next)
	    {
	      crt_value = parser_new_node (parser, PT_EXPR);

	      if (crt_value == NULL)
		{
		  if (crt_value_list != NULL)
		    {
		      parser_free_tree (parser, crt_value_list);
		    }
		  PT_ERROR (parser, node, "allocation error");
		  return;
		}

	      crt_value->info.expr.op = PT_DEFAULTF;
	      crt_value->info.expr.arg1 = parser_copy_tree (parser, crt_attr);

	      if (crt_value->info.expr.arg1 == NULL)
		{
		  parser_free_node (parser, crt_value);
		  if (crt_value_list != NULL)
		    {
		      parser_free_tree (parser, crt_value_list);
		    }
		  PT_ERROR (parser, node, "allocation error");
		  return;
		}

	      pt_set_fill_default_in_path_expression (crt_value->info.
						      expr.arg1);

	      if (crt_value_list == NULL)
		{
		  crt_value_list = crt_value;
		}
	      else
		{
		  crt_value_list =
		    parser_append_node (crt_value, crt_value_list);
		}
	    }
	  crt_list->info.node_list.list = crt_value_list;
	  crt_list->info.node_list.list_type = PT_IS_VALUE;
	}
      else
	{
	  for (crt_attr = attrs_list,
	       crt_value = crt_list->info.node_list.list;
	       crt_attr != NULL && crt_value != NULL;
	       crt_attr = crt_attr->next, crt_value = crt_value->next)
	    {
	      PT_NODE *crt_arg = NULL;

	      if (crt_value->node_type != PT_EXPR)
		{
		  continue;
		}
	      if (crt_value->info.expr.op != PT_DEFAULTF)
		{
		  continue;
		}
	      if (crt_value->info.expr.arg1 != NULL)
		{
		  continue;
		}
	      crt_arg = parser_copy_tree (parser, crt_attr);
	      if (crt_arg == NULL)
		{
		  PT_ERROR (parser, node, "allocation error");
		  return;
		}
	      crt_value->info.expr.arg1 = crt_arg;
	      pt_set_fill_default_in_path_expression (crt_arg);
	    }
	}
    }
}

/*
 * pt_bind_names () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in/out):
 *   continue_walk(in/out):
 */
static PT_NODE *
pt_bind_names (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
	       int *continue_walk)
{
  PT_BIND_NAMES_ARG *bind_arg = (PT_BIND_NAMES_ARG *) arg;
  SCOPES scopestack;
  PT_NODE *prev_attr = NULL, *attr = NULL, *next_attr = NULL, *as_attr = NULL;
  PT_NODE *resolved_attrs = NULL, *spec = NULL;
  PT_NODE *derived_table = NULL, *flat = NULL, *range_var = NULL;
  bool do_resolve = true;
  PT_NODE *seq = NULL;
  PT_NODE *save = NULL;
  short level;
  void *save_etc = NULL;

  *continue_walk = PT_CONTINUE_WALK;

  if (!node || !parser)
    {
      return node;
    }

  /* treat scopes as the next outermost scope */
  scopestack.next = bind_arg->scopes;
  scopestack.specs = NULL;
  scopestack.correlation_level = 0;
  scopestack.location = 0;

  /* prepend local scope to scopestack and then bind names */
  switch (node->node_type)
    {
    case PT_SELECT:
      scopestack.specs = node->info.query.q.select.from;
      bind_arg->scopes = &scopestack;
      pt_bind_scope (parser, bind_arg);

      if (pt_has_error (parser))
	{
	  /* this node will be registered to orphan list and freed later */
	  node = NULL;
	  goto select_end;
	}

      /* resolve '*' for rewritten multicolumn subquery during parsing
       * STEP 1: remove sequence from select_list
       * STEP 2: resolve '*', if exists
       * STEP 3: restore sequence
       */

      /* STEP 1 */
      seq = NULL;
      if (node->info.query.q.select.list->node_type == PT_VALUE
	  && PT_IS_COLLECTION_TYPE (node->info.query.q.select.list->type_enum)
	  && pt_length_of_select_list (node->info.query.q.select.list,
				       EXCLUDE_HIDDEN_COLUMNS) == 1)
	{
	  seq = node->info.query.q.select.list;
	  node->info.query.q.select.list = seq->info.value.data_value.set;
	  seq->info.value.data_value.set = NULL;	/* cut-off link */
	}

      /* STEP 2 */
      if (node->info.query.q.select.list)
	{			/* resolve "*" */
	  if (node->info.query.q.select.list->node_type == PT_VALUE
	      && node->info.query.q.select.list->type_enum == PT_TYPE_STAR)
	    {
	      PT_NODE *next = node->info.query.q.select.list->next;

	      /* To consider 'select *, xxx ...', release "*" node only. */
	      node->info.query.q.select.list->next = NULL;
	      parser_free_node (parser, node->info.query.q.select.list);

	      node->info.query.q.select.list =
		pt_resolve_star (parser,
				 node->info.query.q.select.from, NULL);
	      if (next != NULL)
		{
		  parser_append_node (next, node->info.query.q.select.list);
		}

	      if (!node->info.query.q.select.list)
		{
		  unsigned int save_custom = parser->custom_print;
		  PT_NODE *from = node->info.query.q.select.from;

		  parser->custom_print =
		    parser->custom_print | PT_SUPPRESS_RESOLVED;
		  PT_ERRORmf (parser, from, MSGCAT_SET_PARSER_SEMANTIC,
			      MSGCAT_SEMANTIC_NO_ATTRIBUTES_IN_CLS,
			      pt_short_print (parser, from));
		  parser->custom_print = save_custom;
		  parser_free_tree (parser, node);
		  node = NULL;
		  goto select_end;
		}
	    }
	  else
	    {			/* resolve "class_name.*" */
	      prev_attr = NULL;
	      attr = node->info.query.q.select.list;

	      while (attr && do_resolve)
		{
		  do_resolve = false;

		  /* STEP 2-1) find "class_name.*" */
		  while (attr)
		    {
		      if (attr->node_type == PT_NAME
			  && attr->type_enum == PT_TYPE_STAR)
			{
			  /* find "class_name.*" */
			  do_resolve = true;
			  break;
			}

		      prev_attr = attr;	/* save previous attr */
		      attr = attr->next;
		    }

		  if (attr == NULL)	/* consume attr list */
		    {
		      break;
		    }

		  /* STEP 2-2) do resolve */
		  if (do_resolve == true)
		    {
		      /* STEP 2-2-1) assign spec_id into PT_NAME */
		      for (spec = node->info.query.q.select.from;
			   spec; spec = spec->next)
			{
			  derived_table = spec->info.spec.derived_table;
			  if (derived_table == NULL)
			    {
			      flat = spec->info.spec.flat_entity_list;

			      if (pt_str_compare (attr->info.name.original,
						  flat->info.name.resolved,
						  CASE_INSENSITIVE) == 0)
				{
				  /* find spec
				   * set attr's spec_id */
				  attr->info.name.spec_id =
				    flat->info.name.spec_id;
				  break;
				}
			    }
			  else
			    {	/* derived table */
			      range_var = spec->info.spec.range_var;
			      if (pt_str_compare (attr->info.name.original,
						  range_var->info.name.
						  original,
						  CASE_INSENSITIVE) == 0)
				{
				  break;
				}
			    }
			}	/* for */

		      if (spec == NULL)
			{	/* error */
			  do_resolve = false;
			  node->info.query.q.select.list = NULL;
			  break;
			}
		      else
			{
			  /* STEP 2-2-2) recreate select_list */
			  if (derived_table == NULL)
			    {
			      resolved_attrs = parser_append_node
				(attr->next, pt_resolve_star
				 (parser,
				  node->info.query.q.select.from, attr));
			    }
			  else
			    {
			      for (as_attr = spec->info.spec.as_attr_list;
				   as_attr; as_attr = as_attr->next)
				{
				  as_attr->info.name.resolved =
				    range_var->info.name.original;
				}
			      resolved_attrs = parser_append_node
				(attr->next, parser_copy_tree_list
				 (parser, spec->info.spec.as_attr_list));
			    }

			  if (prev_attr == NULL)
			    {
			      node->info.query.q.select.list = resolved_attrs;
			    }
			  else
			    {
			      prev_attr->next = NULL;
			      node->info.query.q.select.list =
				parser_append_node (resolved_attrs,
						    node->info.query.q.select.
						    list);
			    }

			  if (resolved_attrs == NULL ||
			      node->info.query.q.select.list == NULL)
			    {
			      node->info.query.q.select.list = NULL;
			      break;
			    }
			}
		    }		/* if (do_resolve) */

		  next_attr = attr->next;
		  attr->next = NULL;
		  parser_free_tree (parser, attr);
		  attr = next_attr;

		  /* reposition prev_attr */
		  for (prev_attr = resolved_attrs;
		       prev_attr->next != attr; prev_attr = prev_attr->next)
		    {
		      ;
		    }
		}
	    }

	  if (!node->info.query.q.select.list)
	    {
	      unsigned int save_custom = parser->custom_print;

	      parser->custom_print =
		(parser->custom_print | PT_SUPPRESS_RESOLVED);
	      PT_ERRORmf (parser, attr, MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_IS_NOT_DEFINED,
			  pt_short_print (parser, attr));
	      parser->custom_print = save_custom;
	      parser_free_tree (parser, node);
	      node = NULL;
	      goto select_end;
	    }
	}

      /* STEP 3 */
      if (seq)
	{
	  seq->info.value.data_value.set = node->info.query.q.select.list;
	  node->info.query.q.select.list = seq;
	}

      (void) pt_resolve_hint (parser, node);

      parser_walk_leaves (parser, node, pt_bind_names, bind_arg,
			  pt_bind_names_post, bind_arg);

      /* capture minimum correlation */
      if (!node->info.query.correlation_level)
	{
	  node->info.query.correlation_level = scopestack.correlation_level;
	}
      else if (scopestack.correlation_level
	       && (scopestack.correlation_level <
		   node->info.query.correlation_level))
	{
	  node->info.query.correlation_level = scopestack.correlation_level;
	}

      /* capture type enum and data_type from first column in select list */
      if (node && node->info.query.q.select.list)
	{
	  node->type_enum = node->info.query.q.select.list->type_enum;
	  if (node->info.query.q.select.list->data_type)
	    {
	      node->data_type =
		parser_copy_tree_list (parser,
				       node->info.query.q.select.list->
				       data_type);
	    }
	}

    select_end:

      /* remove this level's scope */
      bind_arg->scopes = bind_arg->scopes->next;

      /* don't revisit leaves */
      *continue_walk = PT_LIST_WALK;
      break;

    case PT_UNION:
    case PT_INTERSECTION:
    case PT_DIFFERENCE:
      {
	int arg1_corr, arg2_corr, corr;
	PT_NODE *arg1, *arg2;
	PT_NODE *select_node = NULL, *order_by_link = NULL;
	int index_of_order_by_link = -1;

	/* treat this just like a select with no from, so that
	 * we can properly get correlation level of sub-queries.
	 */
	bind_arg->scopes = &scopestack;

	/* change order by link in UNION/INTERSECTION/DIFFERENCE query
	 * into the tail of first select query's order by list
	 * for bind names (It will be restored after bind names.)
	 */

	if (node->info.query.order_by)
	  {
	    index_of_order_by_link = 0;

	    select_node = node;
	    while (select_node)
	      {
		switch (select_node->node_type)
		  {
		  case PT_SELECT:
		    goto l_select_node;
		    break;
		  case PT_UNION:
		  case PT_INTERSECTION:
		  case PT_DIFFERENCE:
		    select_node = select_node->info.query.q.union_.arg1;
		    break;
		  default:
		    assert (false);	/* is impossible */
		    select_node = NULL;
		    break;
		  }
	      }

	  l_select_node:
	    if (select_node)
	      {
		if (!select_node->info.query.order_by)
		  {
		    select_node->info.query.order_by =
		      node->info.query.order_by;
		  }
		else
		  {
		    index_of_order_by_link++;
		    order_by_link = select_node->info.query.order_by;
		    while (order_by_link->next)
		      {
			order_by_link = order_by_link->next;
			index_of_order_by_link++;
		      }

		    order_by_link->next = node->info.query.order_by;
		  }

		node->info.query.order_by = NULL;
	      }
	  }

	parser_walk_leaves (parser, node, pt_bind_names, bind_arg,
			    pt_bind_names_post, bind_arg);

	arg1 = node->info.query.q.union_.arg1;
	arg2 = node->info.query.q.union_.arg2;

	if (arg1 && arg2)
	  {
	    arg1_corr = arg1->info.query.correlation_level;
	    arg2_corr = arg2->info.query.correlation_level;
	    if (arg1_corr)
	      {
		corr = arg1_corr;
		if (arg2_corr)
		  {
		    if (arg2_corr < corr)
		      {
			corr = arg2_corr;
		      }
		  }
	      }
	    else
	      {
		corr = arg2_corr;
	      }
	    /* must reduce the correlation level 1, for this level of scoping */
	    if (corr)
	      {
		corr--;
	      }
	    node->info.query.correlation_level = corr;

	    /* capture type enum and data_type from arg1 */
	    node->type_enum = arg1->type_enum;
	    if (arg1->data_type)
	      {
		node->data_type =
		  parser_copy_tree_list (parser, arg1->data_type);
	      }
	  }

	/* Restore order by link */
	if (select_node != NULL && index_of_order_by_link >= 0)
	  {
	    if (order_by_link)
	      {
		node->info.query.order_by = order_by_link->next;
		order_by_link->next = NULL;
	      }
	    else
	      {
		node->info.query.order_by = select_node->info.query.order_by;
		select_node->info.query.order_by = NULL;
	      }
	  }
      }

      /* remove this level's scope */
      bind_arg->scopes = bind_arg->scopes->next;

      /* don't revisit leaves */
      *continue_walk = PT_LIST_WALK;
      break;

    case PT_UPDATE:
      scopestack.specs = node->info.update.spec;
      assert (scopestack.specs != NULL);
      bind_arg->scopes = &scopestack;
      pt_bind_scope (parser, bind_arg);

      (void) pt_resolve_hint (parser, node);

      parser_walk_leaves (parser, node, pt_bind_names, bind_arg,
			  pt_bind_names_post, bind_arg);

      /* remove this level's scope */
      bind_arg->scopes = bind_arg->scopes->next;

      /* don't revisit leaves */
      *continue_walk = PT_LIST_WALK;
      break;

    case PT_DELETE:
      scopestack.specs = node->info.delete_.spec;
      bind_arg->scopes = &scopestack;
      pt_bind_scope (parser, bind_arg);

      (void) pt_resolve_hint (parser, node);

      parser_walk_leaves (parser, node, pt_bind_names, bind_arg,
			  pt_bind_names_post, bind_arg);

      /* remove this level's scope */
      bind_arg->scopes = bind_arg->scopes->next;

      /* don't revisit leaves */
      *continue_walk = PT_LIST_WALK;
      break;

    case PT_INSERT:
      scopestack.specs = node->info.insert.spec;
      bind_arg->scopes = &scopestack;
      pt_bind_scope (parser, bind_arg);

      if (node->info.insert.attr_list == NULL)
	{
	  node->info.insert.attr_list =
	    pt_resolve_star (parser, node->info.insert.spec, NULL);
	}

      fill_in_insert_default_function_arguments (parser, node);

      /* Do not handle ON DUPLICATE KEY UPDATE yet, we need to resolve the
       * other nodes first.
       */
      save = node->info.insert.odku_assignments;
      node->info.insert.odku_assignments = NULL;

      parser_walk_leaves (parser, node, pt_bind_names, bind_arg,
			  pt_bind_names_post, bind_arg);

      /* Check for double assignments */
      pt_no_double_insert_assignments (parser, node);
      if (pt_has_error (parser))
	{
	  goto insert_end;
	}

      /* flag any "correlated" names as undefined.
       * only names in subqueries and sub-inserts should be undefined.
       * use spec->etc to store the correlation level in value_clauses.
       */
      save_etc = node->info.insert.spec->etc;
      level = 0;
      node->info.insert.spec->etc = &level;
      parser_walk_tree (parser, node->info.insert.value_clauses,
			pt_undef_names_pre, node->info.insert.spec,
			pt_undef_names_post, node->info.insert.spec);
      node->info.insert.spec->etc = save_etc;

      if (save != NULL)
	{
	  SCOPES extended_scope;
	  PT_NODE *value_list =
	    node->info.insert.value_clauses->info.node_list.list;
	  extended_scope.next = NULL;

	  /* restore ON DUPLICATE KEY UPDATE node */
	  node->info.insert.odku_assignments = save;

	  /* pt_undef_names_pre may have generated an error */
	  if (pt_has_error (parser))
	    {
	      goto insert_end;
	    }

	  if (PT_IS_SELECT (value_list))
	    {
	      /* Some assignments may reference attributes from the select
	       * query that need to be resolved too. Add the specs from
	       * the select statement as a scope in the stack.
	       */
	      extended_scope.next = bind_arg->scopes->next;
	      extended_scope.specs = value_list->info.query.q.select.from;
	      scopestack.correlation_level = 0;
	      scopestack.location = 0;
	      bind_arg->scopes->next = &extended_scope;
	    }

	  parser_walk_tree (parser, node->info.insert.odku_assignments,
			    pt_bind_names, bind_arg, pt_bind_names_post,
			    bind_arg);

	  if (PT_IS_SELECT (value_list))
	    {
	      /* restore original scopes */
	      bind_arg->scopes->next = extended_scope.next;
	    }
	}

    insert_end:

      /* remove this level's scope */
      bind_arg->scopes = bind_arg->scopes->next;

      /* don't revisit leaves */
      *continue_walk = PT_LIST_WALK;
      break;

    case PT_CREATE_INDEX:
    case PT_ALTER_INDEX:
    case PT_DROP_INDEX:
      scopestack.specs = node->info.index.indexed_class;
      bind_arg->scopes = &scopestack;
      pt_bind_scope (parser, bind_arg);

      parser_walk_leaves (parser, node, pt_bind_names, bind_arg,
			  pt_bind_names_post, bind_arg);

      bind_arg->scopes = bind_arg->scopes->next;

      *continue_walk = PT_LIST_WALK;
      break;

    case PT_DATA_TYPE:
      /* don't visit leaves unless this is an object which might contain a
         name (i.e. CAST(value AS name) ) */
      if (node->type_enum != PT_TYPE_OBJECT)
	{
	  *continue_walk = PT_LIST_WALK;
	}
      break;

    case PT_NAME:
      {
	PT_NODE *temp;

	if (node->type_enum == PT_TYPE_MAYBE)
	  {
	    /* reset spec_id to rebind the name/type */
	    node->info.name.spec_id = 0;
	  }

	temp = pt_bind_name_or_path_in_scope (parser, bind_arg, node);
	if (temp)
	  {
	    node = temp;
	  }

	/* don't visit leaves */
	*continue_walk = PT_LIST_WALK;
      }
      break;

    case PT_DOT_:
      {
	PT_NODE *temp;
	temp = pt_bind_name_or_path_in_scope (parser, bind_arg, node);
	if (temp)
	  {
	    node = temp;
	  }

	if (!(node->node_type == PT_DOT_
	      && (node->info.dot.arg2->node_type == PT_FUNCTION)))
	  {
	    /* don't revisit leaves */
	    *continue_walk = PT_LIST_WALK;
	  }

	/* handle dot print format; do not print resolved name for arg2.
	 * for example: (CLASS_A, CLASS_B, CLASS_C is class)
	 *    CLASS_A.b.CLASS_B.c.CLASS_C.name;
	 * -> CLASS_A.b.c.name;
	 */
	if (node->node_type == PT_DOT_)
	  {
	    PT_NODE *arg2;

	    for (temp = node; temp->node_type == PT_DOT_;
		 temp = temp->info.dot.arg1)
	      {
		/* arg2 is PT_NAME node */
		arg2 = temp->info.dot.arg2;
		if (arg2 && arg2->node_type == PT_NAME)
		  {
		    arg2->info.name.custom_print |= PT_SUPPRESS_RESOLVED;
		  }
	      }			/* for (temp = ...) */
	  }
      }
      break;

    case PT_FUNCTION:
#if 1				/* TODO - defense code */
      if (node->info.function.function_type == PT_GENERIC)
	{
	  assert (false);	/* is impossible */
	  if (parser_function_code != PT_EMPTY)
	    {
	      PT_ERRORmf (parser, node,
			  MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_INVALID_INTERNAL_FUNCTION,
			  node->info.function.generic_name);
	    }
	  else
	    {
	      PT_ERRORmf (parser, node,
			  MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_UNKNOWN_FUNCTION,
			  node->info.function.generic_name);
	    }
	}
#endif
      break;

    case PT_SPEC:
      if (bind_arg->scopes)
	{
	  node->info.spec.location = bind_arg->scopes->location++;
	}
      if (node->info.spec.on_cond)
	{
	  switch (node->info.spec.join_type)
	    {
	    case PT_JOIN_INNER:
	    case PT_JOIN_LEFT_OUTER:
	    case PT_JOIN_RIGHT_OUTER:
	      parser_walk_tree (parser, node->info.spec.on_cond,
				pt_mark_location, &(node->info.spec.location),
				NULL, NULL);
	      break;
	      /*case PT_JOIN_FULL_OUTER: *//* not supported */

	    case PT_JOIN_NONE:
	    default:
	      break;
	    }			/* switch (node->info.spec.join_type) */
	  parser_walk_tree (parser, node->info.spec.on_cond,
			    pt_bind_names, bind_arg, pt_bind_names_post,
			    bind_arg);
	}
      {
	PT_NODE *entity_name = node->info.spec.entity_name;
	PT_NODE *derived_table = node->info.spec.derived_table;

	if (entity_name && entity_name->node_type == PT_NAME)
	  {
	    entity_name->info.name.location = node->info.spec.location;
	    if (entity_name->info.name.db_object
		&& db_is_system_table (entity_name->info.name.db_object))
	      {
		bind_arg->sc_info->system_class = true;
	      }
	  }

	/* check iff invalid correlated subquery located in FROM caluse
	 */
	if (derived_table)
	  {
	    if (derived_table->info.query.correlation_level == 1)
	      {
		PT_ERRORmf (parser, derived_table,
			    MSGCAT_SET_PARSER_SEMANTIC,
			    MSGCAT_SEMANTIC_IS_NOT_DEFINED,
			    pt_short_print (parser, derived_table));
	      }
	  }
      }

      *continue_walk = PT_LIST_WALK;
      break;

    case PT_HOST_VAR:
      pt_bind_type_of_host_var (parser, node);
      break;

    default:
      break;
    }

  return node;
}

/*
 * pt_find_attr_in_class_list () - trying to resolve X.attr
 *   return: returns a PT_NAME list or NULL
 *   parser(in):
 *   flat(in): list of PT_NAME nodes (class names)
 *   attr(in): a PT_NAME (an attribute name)
 */
static int
pt_find_attr_in_class_list (PARSER_CONTEXT * parser, PT_NODE * flat,
			    PT_NODE * attr)
{
  DB_ATTRIBUTE *att = 0;
  DB_OBJECT *db = 0;
  PT_NODE *cname = flat;

  if (!flat || !attr)
    {
      return 0;
    }

  if (attr->node_type != PT_NAME)
    {
      PT_INTERNAL_ERROR (parser, "resolution");
      return 0;
    }

  /* For Each class name on the list */
  while (cname)
    {
      if (cname->node_type != PT_NAME)
	{
	  PT_INTERNAL_ERROR (parser, "resolution");
	  return 0;
	}

      /* Get the object */
      db = cname->info.name.db_object;
      if (!db)
	{
	  PT_INTERNAL_ERROR (parser, "resolution");
	  return 0;
	}

      /* Does db have an attribute named 'name'? */
      att =
	(DB_ATTRIBUTE *) db_get_attribute_force (db,
						 attr->info.name.original);

      if (att == NULL || attr->info.name.meta_class == PT_OID_ATTR)
	{
	  int db_err;
	  db_err = er_errid ();
	  if (db_err == ER_AU_SELECT_FAILURE
	      || db_err == ER_AU_AUTHORIZATION_FAILURE)
	    {
	      PT_ERRORc (parser, attr, er_msg ());
	    }
	  return 0;
	}

      /* set its type */
      pt_get_attr_data_type (parser, att, attr);

      if (PT_NAME_INFO_IS_FLAGED (attr, PT_NAME_INFO_FILL_DEFAULT))
	{
	  if (attr->info.name.default_value != NULL)
	    {
	      /* default value was already set */
	      return 1;
	    }
	  if (att->default_value.default_expr != DB_DEFAULT_NONE)
	    {
	      /* if the default value is an expression, make a node for it */
	      PT_OP_TYPE op =
		pt_op_type_from_default_expr_type (att->default_value.
						   default_expr);
	      assert (op != (PT_OP_TYPE) 0);
	      attr->info.name.default_value = pt_expression_0 (parser, op);
	    }
	  else
	    {
	      /* just set the default value */
	      attr->info.name.default_value =
		pt_dbval_to_value (parser, &att->default_value.value);
	      if (attr->info.name.default_value == NULL)
		{
		  PT_INTERNAL_ERROR (parser, "resolution");
		  return 0;
		}
	    }
	}

      cname = cname->next;
    }
  attr->info.name.spec_id = flat->info.name.spec_id;

  return 1;
}

/*
 * pt_find_name_in_spec () - Given a spec, see if name can be resolved to this
 *   return: 0 if name is NOT an attribute of spec
 *   parser(in):
 *   spec(in):
 *   name(in): a PT_NAME (an attribute name)
 */
static int
pt_find_name_in_spec (PARSER_CONTEXT * parser, PT_NODE * spec, PT_NODE * name)
{
  int ok;
  PT_NODE *col;
  PT_NODE *range_var;
  const char *resolved_name;

  if (spec == NULL)
    {
      return 0;
    }

  if (name->info.name.meta_class == PT_CLASS)
    {
      /* should resolve to a class name later, don't search attributes */
      return 0;
    }

  resolved_name = name->info.name.resolved;
  range_var = spec->info.spec.range_var;
  if (resolved_name && range_var)
    {
      if (pt_str_compare (resolved_name, range_var->info.name.original,
			  CASE_INSENSITIVE) != 0)
	{
	  return 0;
	}
    }

  if (!spec->info.spec.derived_table)
    {
      ok = pt_find_attr_in_class_list (parser,
				       spec->info.spec.flat_entity_list,
				       name);
    }
  else
    {
      col = pt_is_on_list (parser, name, spec->info.spec.as_attr_list);
      ok = (col != NULL);
      if (col && !name->info.name.spec_id)
	{
	  name->type_enum = col->type_enum;
	  if (col->data_type)
	    {
	      name->data_type =
		parser_copy_tree_list (parser, col->data_type);
	    }
	  name->info.name.spec_id = spec->info.spec.id;
	  name->info.name.meta_class = PT_NORMAL;
	}
    }

  return ok;
}


/*
 * pt_check_unique_exposed () - make sure the exposed names in the
 *                              range_var field are all distinct
 *   return: 1 if exposed names are all unique, 0 if duplicate name
 *   parser(in):
 *   p(in): a PT_SPEC node (list)

 * Note :
 * Assumes that the exposed name (range_var) is a PT_NAME node but
 * doesn't check this. Else, crash and burn.
 */
static int
pt_check_unique_exposed (PARSER_CONTEXT * parser, const PT_NODE * p)
{
  PT_NODE *q;

  while (p)
    {
      q = p->next;		/* q = next spec */
      while (q)
	{			/* check that p->range !=
				   q->range to the end of list */
	  if (!pt_str_compare (p->info.spec.range_var->info.name.original,
			       q->info.spec.range_var->info.name.original,
			       CASE_INSENSITIVE))
	    {
	      PT_ERRORmf (parser, q, MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_AMBIGUOUS_EXPOSED_NM,
			  q->info.spec.range_var->info.name.original);
	      return 0;
	    }
	  q = q->next;		/* check the next one inner loop */
	}
      p = p->next;		/* go to next one outer loop */
    }
  return 1;			/* OK */
}

/*
 * pt_check_unique_names () - make sure the spec names are different.
 *
 *   return: 1 if names are all unique, 0 if duplicate name
 *   parser(in):
 *   p(in): a PT_SPEC node (list)

 * Note :
 * If names in range_var are resolved, use pt_check_unique_exposed () instead.
 * This was specially created for DELETE statement which needs to verify
 * that specs have unique names before calling pt_class_pre_fetch ();
 * otherwise it crashes.
 */
int
pt_check_unique_names (PARSER_CONTEXT * parser, const PT_NODE * p)
{
  PT_NODE *q;

  while (p)
    {
      const char *p_name = NULL;
      if (p->node_type != PT_SPEC)
	{
	  p = p->next;
	  continue;
	}
      if (p->info.spec.range_var && PT_IS_NAME_NODE (p->info.spec.range_var))
	{
	  p_name = p->info.spec.range_var->info.name.original;
	}
      else if (p->info.spec.entity_name
	       && PT_IS_NAME_NODE (p->info.spec.entity_name))
	{
	  p_name = p->info.spec.entity_name->info.name.original;
	}
      else
	{
	  p = p->next;
	  continue;
	}
      q = p->next;		/* q = next spec */
      while (q)
	{			/* check that p->range !=
				   q->range to the end of list */
	  const char *q_name = NULL;
	  if (q->node_type != PT_SPEC)
	    {
	      q = q->next;
	      continue;
	    }
	  if (q->info.spec.range_var
	      && PT_IS_NAME_NODE (q->info.spec.range_var))
	    {
	      q_name = q->info.spec.range_var->info.name.original;
	    }
	  else if (q->info.spec.entity_name
		   && PT_IS_NAME_NODE (q->info.spec.entity_name))
	    {
	      q_name = q->info.spec.entity_name->info.name.original;
	    }
	  else
	    {
	      q = q->next;
	      continue;
	    }
	  if (!pt_str_compare (p_name, q_name, CASE_INSENSITIVE))
	    {
	      PT_ERRORmf (parser, q, MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_DUPLICATE_CLASS_OR_ALIAS, q_name);
	      return 0;
	    }
	  q = q->next;		/* check the next one inner loop */
	}
      p = p->next;		/* go to next one outer loop */
    }
  return 1;			/* OK */
}

/*
 * pt_add_class_to_entity_list () -
 *   return:
 *   parser(in):
 *   class(in):
 *   entity(in):
 *   parent(in):
 *   id(in):
 *   meta_class(in):
 */
PT_NODE *
pt_add_class_to_entity_list (PARSER_CONTEXT * parser,
			     DB_OBJECT * class_,
			     PT_NODE * entity,
			     const PT_NODE * parent,
			     UINTPTR id, PT_MISC_TYPE meta_class)
{
  PT_NODE *flat_list;

  flat_list = pt_make_subclass_list (parser, class_, parent->line_number,
				     parent->column_number, id, meta_class,
				     NULL);

  return pt_name_list_union (parser, entity, flat_list);
}

/*
 * pt_domain_to_data_type () - create and return a PT_DATA_TYPE node that
 *                             corresponds to a DB_DOMAIN dom
 *   return: PT_NODE * to a data_type node
 *   parser(in):
 *   domain(in/out):
 *
 * Note : Won't work if type is OBJECT and class name is NULL
 */
PT_NODE *
pt_domain_to_data_type (PARSER_CONTEXT * parser, DB_DOMAIN * domain)
{
  DB_DOMAIN *dom;
  DB_OBJECT *db;
  PT_NODE *result = NULL, *s;
  PT_TYPE_ENUM t;

  t = (PT_TYPE_ENUM) pt_db_to_type_enum (TP_DOMAIN_TYPE (domain));
  switch (t)
    {
    case PT_TYPE_NUMERIC:
    case PT_TYPE_VARBIT:
    case PT_TYPE_VARCHAR:
      result = parser_new_node (parser, PT_DATA_TYPE);
      if (result == NULL)
	{
	  return NULL;
	}
      result->type_enum = t;
      /* some of these types won't have all of the three, but that's okay */
      result->info.data_type.precision = db_domain_precision (domain);
      result->info.data_type.dec_scale = db_domain_scale (domain);
      result->info.data_type.collation_id = db_domain_collation_id (domain);
      assert (!PT_IS_CHAR_STRING_TYPE (t)
	      || result->info.data_type.collation_id >= 0);
      break;

    case PT_TYPE_OBJECT:
      /* get the object */
      if (!(result = parser_new_node (parser, PT_DATA_TYPE)))
	{
	  return NULL;
	}
      result->type_enum = t;
      result->info.data_type.entity = NULL;
      result->info.data_type.virt_type_enum = PT_TYPE_OBJECT;
      while (domain)
	{
	  db = db_domain_class (domain);
	  if (db)
	    {
	      /* prim_type = PT_TYPE_OBJECT, attach db_object, attach name */
	      result->info.data_type.entity
		= pt_add_class_to_entity_list (parser, db,
					       result->info.data_type.entity,
					       result, (UINTPTR) result,
					       PT_CLASS);
	    }
	  domain = (DB_DOMAIN *) db_domain_next (domain);
	}
      break;

    case PT_TYPE_SEQUENCE:
      /* set of what? */
      dom = (DB_DOMAIN *) db_domain_set (domain);
      /* make list of types in set */
      while (dom)
	{
	  s = pt_domain_to_data_type (parser, dom);	/* recursion here */

	  if (s)
	    {
	      if (result)
		{
		  /*
		   * We want to make sure that the flat name list
		   * hanging off of the first PT_DATA_TYPE node is the
		   * union of all flat name lists from all nodes in
		   * this list; this makes certain things much easier
		   * later on.
		   * PRESERVE THE ORDER OF THESE LISTS!
		   */
		  s->info.data_type.entity =
		    pt_name_list_union (parser, s->info.data_type.entity,
					result->info.data_type.entity);
		  s->next = result;
		  result->info.data_type.entity = NULL;
		}
	      result = s;
	    }

	  dom = (DB_DOMAIN *) db_domain_next (dom);
	}

      /*
       * Now run back over the flattened name list and ensure that
       * they all have the same spec id.
       */
      if (result)
	{
	  for (s = result->info.data_type.entity; s; s = s->next)
	    {
	      s->info.name.spec_id = (UINTPTR) result;
	    }
	}
      break;

    default:
      if (!(result = parser_new_node (parser, PT_DATA_TYPE)))
	{
	  return NULL;
	}
      result->type_enum = t;
      break;
    }

  return result;
}

/*
 * pt_flat_spec_pre () - resolve the entity spec into a flat name list
 *                       and attach it
 *   return:
 *   parser(in):
 *   node(in/out):
 *   chk_parent(in):
 *   continue_walk(in/out):
 */
PT_NODE *
pt_flat_spec_pre (PARSER_CONTEXT * parser,
		  PT_NODE * node, void *chk_parent, int *continue_walk)
{
  PT_NODE *q, *derived_table;
  PT_NODE *result = node;
  PT_NODE **spec_parent = (PT_NODE **) chk_parent;

  *continue_walk = PT_CONTINUE_WALK;

  if (!node)
    {
      return 0;
    }

  /* if node type is entity_spec(list) process the list */

  switch (node->node_type)
    {
    case PT_INSERT:
    case PT_SELECT:
    case PT_UPDATE:
    case PT_DELETE:
    case PT_GRANT:
    case PT_REVOKE:
      *spec_parent = node;
      break;
    default:
      break;
    }

  if (node->node_type == PT_SPEC)
    {
      /* don't let parser_walk_tree go to rest of list. List is handled here */
      *continue_walk = PT_LEAF_WALK;
      while (node)
	{
	  /* if a flat list has not been calculated, calculate it. */
	  derived_table = node->info.spec.derived_table;
	  if (!node->info.spec.flat_entity_list && !derived_table)
	    {
	      /* this sets the persistent entity_spec id.
	       * the address of the node may be changed through copying,
	       * but this id won't. The number used is the address, just
	       * as an easy way to generate a unique number.
	       */
	      node->info.spec.id = (UINTPTR) node;

	      q = pt_make_flat_name_list (parser, node, *spec_parent);

	      node->info.spec.flat_entity_list = q;
	    }

	  if (!derived_table)
	    {
	      /* entity_spec list are not allowed to
	         have derived column names (for now) */
	      if (node->info.spec.as_attr_list)
		{
		  PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			      MSGCAT_SEMANTIC_WANT_NO_DERIVED_COLS,
			      pt_short_print_l
			      (parser, node->info.spec.as_attr_list));
		}
	    }
	  else
	    {
	      if (!node->info.spec.id)
		{
		  node->info.spec.id = (UINTPTR) node;
		}

	      parser_walk_tree (parser, derived_table, pt_flat_spec_pre,
				chk_parent, pt_continue_walk, NULL);
	    }

	  node = node->next;	/* next item on spec list */
	}

      /* and then do additional checks */
      if (result->node_type == PT_SPEC)
	{
	  if (!pt_must_have_exposed_name (parser, result))
	    {
	      return 0;
	    }
	  if (!pt_check_unique_exposed (parser, result))
	    {
	      return 0;
	    }
	}
    }

  return result;
}

/*
 * pt_get_all_attributes_and_types() -
 *   return:  cls' list of attributes if all OK, NULL otherwise.
 *   parser(in): handle to parser context
 *   cls(in): a PT_NAME node naming a class in the database
 *   from(in): the entity_spec from which cls was derived
 *             as a flat_entity_list item
 */
static PT_NODE *
pt_get_all_attributes_and_types (PARSER_CONTEXT * parser,
				 PT_NODE * cls, PT_NODE * from)
{
  PT_NODE *result = NULL, *tail, *node;
  DB_ATTRIBUTE *att;
  DB_OBJECT *object;

  if (cls == NULL
      || cls->node_type != PT_NAME
      || (object = cls->info.name.db_object) == NULL
      || from == NULL || from->node_type != PT_SPEC)
    {
      return NULL;
    }

  att = (DB_ATTRIBUTE *) db_get_attributes_force (object);

  if (att != NULL)
    {
      /* make result anchor the list */
      result = tail = pt_name (parser, db_attribute_name (att));
      if (result == NULL)
	{
	  return NULL;
	}
      result->line_number = from->line_number;
      result->column_number = from->column_number;

      /* set its type */
      pt_get_attr_data_type (parser, att, result);

      result->info.name.spec_id = from->info.spec.id;

      /* advance to next attribute */
      att = db_attribute_next (att);

      /* for the rest of the attributes do */
      while (att != NULL)
	{
	  /* make new node & copy attribute name into it */
	  node = pt_name (parser, db_attribute_name (att));
	  if (node == NULL)
	    {
	      goto on_error;
	    }
	  node->line_number = from->line_number;
	  node->column_number = from->column_number;

	  /* set its type */
	  pt_get_attr_data_type (parser, att, node);

	  node->info.name.spec_id = from->info.spec.id;

	  /* append to list */
	  tail->next = node;
	  tail = node;

	  /* advance to next attribute */
	  att = db_attribute_next (att);
	}
    }

  return result;

on_error:
  if (result != NULL)
    {
      parser_free_tree (parser, result);
    }

  return NULL;
}

/*
 * pt_get_attr_data_type () - Given an attribute(att) whose name is in p,
 *      find its data_type and attach the data-type to the name
 *   return:
 *   parser(in):
 *   att(in): a db_attribute
 *   attr(in/out): a PT_NAME node corresponding to att
 *   db(in):
 */
static void
pt_get_attr_data_type (PARSER_CONTEXT * parser, DB_ATTRIBUTE * att,
		       PT_NODE * attr)
{
  DB_DOMAIN *dom;

  if (att == NULL || attr == NULL)
    {
      return;
    }

  dom = db_attribute_domain (att);
  attr->etc = dom;		/* used for getting additional db-specific
				 * domain information in the Versant driver
				 */
  attr->type_enum = (PT_TYPE_ENUM) pt_db_to_type_enum (TP_DOMAIN_TYPE (dom));
  if (PT_IS_COMPLEX_TYPE (attr->type_enum))
    {
      attr->data_type = pt_domain_to_data_type (parser, dom);
    }

  attr->info.name.meta_class = PT_NORMAL;

  if (att->flags & SM_ATTFLAG_SHARD_KEY)
    {
      PT_NAME_INFO_SET_FLAG (attr, PT_NAME_FOR_SHARD_KEY);
    }
}

/*
 * pt_resolve_correlation () - Given an exposed spec, return the name node
 *     of its oid.
 *   return:
 *   parser(in):
 *   in_node(in):
 *   scope(in):
 *   exposed_spec(in):
 *   col_name(in):
 *   p_entity(in):
 *
 * Note:
 * Also check for some semantic errors
 *    - disallow OIDs of derived spec
 *    - disallow selectors except inside path expression
 */
static PT_NODE *
pt_resolve_correlation (PARSER_CONTEXT * parser,
			PT_NODE * in_node,
			UNUSED_ARG PT_NODE * scope,
			PT_NODE * exposed_spec,
			int col_name, PT_NODE ** p_entity)
{
  PT_NODE *corr_name = NULL;

  /* If so, name resolves to scope's flat list of entities */
  if (exposed_spec)
    {
      /* the exposed name of a derived table may not be used alone,
         ie, "select e from (select a from c) e" is disallowed. */
      if (col_name
	  && exposed_spec->info.spec.derived_table
	  && exposed_spec->info.spec.range_var != in_node)
	{
	  if (PT_NAME_INFO_IS_FLAGED (in_node, PT_NAME_FOR_UPDATE))
	    {
	      in_node->info.name.spec_id = exposed_spec->info.spec.id;
	      return in_node;
	    }
	  else
	    {
	      PT_ERRORmf (parser, in_node, MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_WANT_NO_REF_TO_DRVTB,
			  pt_short_print (parser, in_node));
	      return NULL;
	    }
	}

      if (!exposed_spec->info.spec.range_var)
	{
	  PT_INTERNAL_ERROR (parser, "resolution");
	  return NULL;
	}

      corr_name = pt_name (parser, "");
      PT_NODE_COPY_NUMBER_OUTERLINK (corr_name, in_node);
      in_node->next = NULL;
      in_node->or_next = NULL;

      corr_name->info.name.meta_class = PT_OID_ATTR;
      if (PT_NAME_INFO_IS_FLAGED (in_node, PT_NAME_INFO_GENERATED_OID))
	{
	  PT_NAME_INFO_SET_FLAG (corr_name, PT_NAME_INFO_GENERATED_OID);
	}

      if (PT_NAME_INFO_IS_FLAGED (in_node, PT_NAME_FOR_UPDATE))
	{
	  PT_NAME_INFO_SET_FLAG (corr_name, PT_NAME_FOR_UPDATE);
	}

      parser_free_tree (parser, in_node);

      corr_name->info.name.spec_id = exposed_spec->info.spec.id;
      corr_name->info.name.resolved =
	exposed_spec->info.spec.range_var->info.name.original;

      /* attach the data type */
      corr_name->type_enum = PT_TYPE_OBJECT;
      if (exposed_spec->info.spec.flat_entity_list)
	{
	  corr_name->data_type = pt_object_to_data_type
	    (parser, exposed_spec->info.spec.flat_entity_list);
	}

      *p_entity = exposed_spec;
    }

  return corr_name;
}

/*
 * pt_get_resolution() -  try to resolve a name or path expr using this scope
 *   return:  if in_node is an X.Z with X an exposed name,
 *               collapse it into Z, return Z
 *          if in_node is an X that resolves to scope, return X
 *          if in_node has no resolution in this scope, return NULL
 *   parser(in): the parser context
 *   bind_arg(in): a list of scopes for resolving method calls
 *   scope(in): a list of PT_SPEC nodes (ie, a 'from' clause)
 *   in_node(in/out): an attribute reference or path expression to be resolved
 *   p_entity(out): entity_spec of X if in_node is an X.Z
 *   col_name(in): true on top level call.
 */
static PT_NODE *
pt_get_resolution (PARSER_CONTEXT * parser,
		   PT_BIND_NAMES_ARG * bind_arg,
		   PT_NODE * scope,
		   PT_NODE * in_node, PT_NODE ** p_entity, int col_name)
{
  PT_NODE *exposed_spec, *spec, *savespec, *arg1, *arg2;
  PT_NODE *temp;

  if (!in_node)
    {
      return NULL;
    }

  exposed_spec = NULL;

  if (in_node->node_type == PT_NAME)
    {
      /* Has this name been resolved? */
      if (in_node->info.name.spec_id)
	{
	  *p_entity = NULL;
	  if (in_node->type_enum == PT_TYPE_OBJECT && in_node->data_type)
	    {
	      temp = scope;
	      while (temp && temp->info.spec.id != in_node->info.name.spec_id)
		{
		  temp = temp->next;
		}
	      if (temp)
		{
		  *p_entity = temp;
		  return in_node;
		}
	    }
	  return NULL;
	}

      if (col_name)
	{
	  ;
	}
      else
	{
	  /* We are on the left of a dot node. (because we are recursing)
	   * Here, correlation names have precedence over column names.
	   * (for ANSI compatibility).
	   * For unqualified names, column names have precedence.
	   * For qualifier names, correlation names have precedence.
	   */
	  exposed_spec = pt_is_correlation_name (parser, scope, in_node);
	  if (exposed_spec)
	    {
	      return pt_resolve_correlation
		(parser, in_node, scope, exposed_spec, col_name, p_entity);
	    }
	}

      /* Else, is this an attribute of a unique entity within scope? */
      for (savespec = NULL, spec = scope; spec; spec = spec->next)
	{
	  if (pt_find_name_in_spec (parser, spec, in_node))
	    {
	      if (savespec)
		{
		  PT_ERRORmf (parser, in_node, MSGCAT_SET_PARSER_SEMANTIC,
			      MSGCAT_SEMANTIC_AMBIGUOUS_REF_TO,
			      in_node->info.name.original);
		  return NULL;
		}
	      savespec = spec;
	    }
	}

      if (savespec)
	{
	  /* if yes, set the resolution and the resolved name */
	  in_node->info.name.resolved =
	    savespec->info.spec.range_var->info.name.original;

	  *p_entity = savespec;

	  return in_node;
	}

      if (col_name)
	{
	  /* Failing finding a column name for a name NOT on the
	   * left of a dot, try and resolve it as a correlation name (oid).
	   * For unqualified names, column names have precedence.
	   * For qualifier names, correlation names have precedence.
	   */
	  exposed_spec = pt_is_correlation_name (parser, scope, in_node);
	  if (exposed_spec)
	    {
	      return pt_resolve_correlation
		(parser, in_node, scope, exposed_spec, col_name, p_entity);
	    }
	}

      /* no resolution in this scope */
      return NULL;
    }

  assert (exposed_spec == NULL);

  /* Is it a DOT expression X.Z? */
  if (in_node->node_type == PT_DOT_)
    {
      arg1 = in_node->info.dot.arg1;
      arg2 = in_node->info.dot.arg2;
      /* if bad arg2, OR if already resolved, then return same node */
      if (!arg2 || arg2->info.name.spec_id || arg2->node_type != PT_NAME)
	{
	  *p_entity = NULL;
	  return in_node;
	}

      /* Check if this is an exposed name in the current scope. */
      exposed_spec = pt_is_correlation_name (parser, scope, in_node);
      if (exposed_spec)
	{
	  return pt_resolve_correlation
	    (parser, in_node, scope, exposed_spec, col_name, p_entity);
	}

      assert (exposed_spec == NULL);

      /* if arg1 not in scope, return NULL to indicate not in scope. */
      arg1 = pt_get_resolution (parser, bind_arg, scope, arg1, p_entity, 0);
      if (arg1 == NULL)
	{
	  *p_entity = NULL;
	  return NULL;
	}

      /* given X.Z, resolve (X) */
      in_node->info.dot.arg1 = arg1;
      /* Note : this should not get run, now that parameters
       * are evaluated at run time, instead of converted to values
       * at compile time.
       * We need to be careful here.  We may have a path expression
       * anchored by a parameter (PT_VALUE node) object.
       *  If so, we evaluate the path expression here and
       *  return the appropriate PT_VALUE node.
       */
      if (arg1->node_type == PT_VALUE && arg1->type_enum == PT_TYPE_OBJECT)
	{
	  /* signal as not resolved */
	  return NULL;
	}

      /* If arg1 is an exposed name, replace expr with resolved arg2 */
      if (PT_IS_OID_NAME (arg1))
	{
	  /* Arg1 was an exposed name */
	  if (pt_find_name_in_spec (parser, *p_entity, arg2))
	    {
	      /* only mark it resolved if it was found!
	         transfer the info from arg1 to arg2 */
	      arg2->info.name.resolved = arg1->info.name.resolved;
	      /* don't loose list */
	      arg2->next = in_node->next;
	      /* save alias */
	      arg2->alias_print = in_node->alias_print;
	      PT_NAME_INFO_SET_FLAG (arg2, PT_NAME_INFO_DOT_NAME);
	      /* replace expr with resolved arg2 */
	      in_node->info.dot.arg2 = NULL;
	      in_node->next = NULL;
	      parser_free_tree (parser, in_node);
	      in_node = arg2;
	    }
	  else
	    {
	      temp = arg1->data_type;
	      if (temp)
		{
		  temp = temp->info.data_type.entity;
		}
	      if (!temp)
		{
		  /* resolution error */
		  PT_ERRORmf2 (parser, arg1, MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_CLASS_HAS_NO_ATTR,
			       pt_short_print (parser, arg1),
			       pt_short_print (parser, arg2));
		}
	      else if (temp->next)
		{
		  PT_ERRORmf2 (parser, arg1, MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_CLASSES_HAVE_NO_ATTR,
			       pt_short_print_l (parser, temp),
			       pt_short_print (parser, arg2));
		}
	      else
		{
		  temp->info.name.resolved = NULL;
		  PT_ERRORmf2 (parser, arg1, MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_CLASS_DOES_NOT_HAVE,
			       pt_short_print_l (parser, temp),
			       pt_short_print (parser, arg2));
		}
	      /* signal as not resolved */
	      return NULL;
	    }
	}
      else
	{
	  /* This is NOT an exposed name, it must be an object attribute.
	   * It must also be a legitimate root for this path expression.
	   */
	  if (arg1->type_enum != PT_TYPE_OBJECT)
	    {
	      PT_ERRORmf2 (parser, arg1, MSGCAT_SET_PARSER_SEMANTIC,
			   MSGCAT_SEMANTIC_IS_NOT_OBJECT_TYPE,
			   pt_short_print (parser, arg1),
			   pt_show_type_enum (arg1->type_enum));
	      return NULL;
	    }
	  else if (arg1->data_type == NULL)
	    {
	      PT_INTERNAL_ERROR (parser, "Resolution");
	      return NULL;
	    }

	  if (!pt_find_attr_in_class_list
	      (parser, arg1->data_type->info.data_type.entity, arg2))
	    {
	      temp = arg1->data_type;
	      if (temp)
		{
		  temp = temp->info.data_type.entity;
		}
	      if (!temp)
		{
		  /* resolution error */
		  PT_ERRORmf2 (parser, arg1, MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_DOM_OBJ_HASNO_ATT_X,
			       pt_short_print (parser, arg1),
			       pt_short_print (parser, arg2));
		}
	      else if (temp->next)
		{
		  PT_ERRORmf2 (parser, arg1, MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_CLASSES_HAVE_NO_ATTR,
			       pt_short_print_l (parser, temp),
			       pt_short_print (parser, arg2));
		}
	      else
		{
		  temp->info.name.resolved = NULL;
		  PT_ERRORmf2 (parser, arg1, MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_CLASS_DOES_NOT_HAVE,
			       pt_short_print_l (parser, temp),
			       pt_short_print (parser, arg2));
		}
	      *p_entity = NULL;
	      return NULL;	/* not bound */
	    }

	  /* we have a good path expression,
	   * but do not permit path expression anymore
	   */
	  PT_ERRORmf3 (parser, in_node, MSGCAT_SET_PARSER_SEMANTIC,
		       MSGCAT_SEMANTIC_INCONSISTENT_PATH,
		       pt_short_print (parser, in_node),
		       pt_short_print (parser, arg2),
		       pt_short_print (parser, arg1));

	  /* signal as not resolved */
	  return NULL;
	}

      return in_node;
    }				/* end if-a-dot-expression */

  /* Else got some node type we shouldn't have */
  PT_INTERNAL_ERROR (parser, "Resolution");

  return NULL;
}


/*
 * pt_is_correlation_name() - checks nam is an exposed name of some
 *	                      entity_spec from scope
 *   return:  the entity spec of which nam is the correlation name.
 *	    Else 0 if not found or error.
 *   parser(in/out): the parser context
 *   scope(in): a list of PT_SPEC nodes
 *   nam(in): a PT_NAME node
 */

static PT_NODE *
pt_is_correlation_name (PARSER_CONTEXT * parser,
			PT_NODE * scope, PT_NODE * nam)
{
  PT_NODE *specs;
  PT_NODE *owner = NULL;

  assert (nam != NULL
	  && (nam->node_type == PT_NAME || nam->node_type == PT_DOT_));

  if (nam->node_type == PT_DOT_)
    {
      owner = nam->info.dot.arg1;
      if (owner->node_type != PT_NAME)
	{
	  /* could not be owner.correlation */
	  return NULL;
	}
      nam = nam->info.dot.arg2;
    }

  for (specs = scope; specs; specs = specs->next)
    {
      if (specs->node_type != PT_SPEC)
	{
	  PT_INTERNAL_ERROR (parser, "resolution");
	  return 0;
	}
      if (specs->info.spec.range_var
	  && pt_str_compare (nam->info.name.original,
			     specs->info.spec.range_var->info.name.original,
			     CASE_INSENSITIVE) == 0)
	{
	  if (!owner)
	    {
	      return specs;
	    }
	  else
	    {
	      PT_NODE *entity_name;

	      entity_name = specs->info.spec.entity_name;
	      if (entity_name
		  && entity_name->node_type == PT_NAME
		  && entity_name->info.name.resolved
		  /* actual class ownership test is done for spec
		   * no need to repeat that here. */
		  && (pt_str_compare (entity_name->info.name.resolved,
				      owner->info.name.original,
				      CASE_INSENSITIVE) == 0))
		{
		  return specs;
		}
	    }
	}
    }
  return 0;
}

/*
 * pt_find_entity () -
 *   return: the entity spec of an entity of a spec in the scope with
 * 	     an id matching the "match" spec
 *   parser(in): the parser context
 *   scope(in): a list of PT_SPEC nodes
 *   id(in): of a PT_SPEC node
 */
PT_NODE *
pt_find_entity (PARSER_CONTEXT * parser, const PT_NODE * scope, UINTPTR id)
{
  const PT_NODE *spec;

  for (spec = scope; spec; spec = spec->next)
    {
      if (spec->node_type != PT_SPEC)
	{
	  PT_INTERNAL_ERROR (parser, "resolution");
	  return 0;
	}

      if (spec->info.spec.id == id)
	{
	  return (PT_NODE *) spec;
	}
    }

  return NULL;
}


/*
 * pt_is_on_list () - check whether name node p is equal to
 *                    something on the list
 *   return: Pointer to matching item or NULL
 *   parser(in):
 *   p(in): A PT_NAME node
 *   list(in): A LIST of PT_NAME nodes
 *
 * Note :
 * two strings of length zero match
 * A NULL string does NOT match a zero length string
 */
static PT_NODE *
pt_is_on_list (PARSER_CONTEXT * parser, const PT_NODE * p,
	       const PT_NODE * list)
{
  if (!p)
    {
      return 0;
    }
  if (p->node_type != PT_NAME)
    {
      return 0;
    }
  while (list)
    {
      if (list->node_type != PT_NAME)
	{
	  PT_INTERNAL_ERROR (parser, "resolution");
	  return 0;		/* this is an error */
	}

      if (pt_str_compare (p->info.name.original, list->info.name.original,
			  CASE_INSENSITIVE) == 0)
	{
	  return (PT_NODE *) list;	/* found a match */
	}
      list = list->next;
    }
  return 0;			/* no match */
}

/*
 * pt_name_list_union () -
 *   return:
 *   parser(in):
 *   list(in/out): A list of PT_NAME nodes
 *   additions(in): A list of PT_NAME nodes
 *
 * Note :
 *    PT_NAME lists  ( list1 union list2 )
 *    PRESERVING ORDER OF LIST!
 */
static PT_NODE *
pt_name_list_union (PARSER_CONTEXT * parser, PT_NODE * list,
		    PT_NODE * additions)
{
  PT_NODE *result, *temp;
  if (!list)
    {
      return additions;
    }
  if (!additions)
    {
      return list;
    }
  result = list;
  while (additions)
    {
      temp = additions;
      additions = additions->next;
      temp->next = NULL;
      if (!pt_is_on_list (parser, temp, list))
	{
	  list = parser_append_node (temp, list);
	}
      else
	{
	  parser_free_node (parser, temp);
	}
    }

  return result;
}

/*
 * pt_make_flat_name_list () - create a list of its name and all of its
 *                             subclass names, recursively
 *   return: Returns a pointer to (list of) PT_NAME node(s)
 *   parser(in):
 *   db(in): db_object to find subclasses for
 *   line_num(in): input line_num (for error messages)
 *   col_num(in): input column num (for error messages)
 *   id(in):
 *   meta_class(in): parent class for pt_name nodes we create
 *   names_mht(in): memory hash table used to avoid duplicates
 */

static PT_NODE *
pt_make_subclass_list (PARSER_CONTEXT * parser,
		       DB_OBJECT * db,
		       int line_num,
		       int col_num,
		       UINTPTR id,
		       PT_MISC_TYPE meta_class, MHT_TABLE * names_mht)
{
  const char *classname;
  PT_NODE *result = 0;		/* will be returned */

  if (!parser)
    {
      return 0;
    }
  /* get the name of THIS class and put it in a PT_NAME node */
  if (!db)
    {
      return 0;
    }
  classname = sm_class_name (db);
  if (!classname)
    {
      PT_INTERNAL_ERROR (parser, "resolution");
      return 0;
    }				/* not a class name (error) */


  /* Check to see if this classname is already known, and
   * only add a (name) node if we have never seen it before.
   * Note: Even if we have visited it, we still need to recursively
   * check its subclasses (see dbl below) in order to maintain
   * the correct ordering of classnames found via our depth-first search.
   */
  if (!names_mht || !mht_get (names_mht, classname))
    {
      result = pt_name (parser, classname);
      result->line_number = line_num;
      result->column_number = col_num;
      result->info.name.db_object = db;
      result->info.name.spec_id = id;
      result->info.name.meta_class = meta_class;

      if (names_mht)
	{
	  mht_put (names_mht, classname, (void *) true);
	}
    }

  return result;
}


/*
 * pt_make_flat_name_list () - Create flat name list from entity spec
 *   return: returns a list of PT_NAME nodes representing the class(es)
 *           referred to by the entity spec
 *   parser(in):
 *   spec(in): A PT_SPEC node representing a single entity spec
 *   spec_parent(in):

 * Note :
 *   Case (A == 'sub-list (A,B,..)'):
 *        Set list(A)= set-theoretic union of the names list(A), list(B), ...
 *        (There is no ONLY or ALL in this case)
 *   Case (A ==  'ONLY X' ||  A == 'X'  (only is implied)):
 *        Set list(A) = the name 'X' which must be an existing class name.
 *        Attach the db_object * to the db_object field in the name node.
 *   Case (A ==  'ALL  X'):
 *        Set list(A) = the name 'X' as above union the names of all
 *            subclasses of X (recursively).
 *   Case (A == 'ALL  X EXCEPT Y'):
 *        Set list(A) = list(ALL X) - list(Y)
 *                (set-theoretic difference of the lists)
 *        Additionally:
 *               list(Y) must be a subset of list(X), else error.
 *               list(X)-list(Y)  must be non-empty, else error.
 */
static PT_NODE *
pt_make_flat_name_list (PARSER_CONTEXT * parser, PT_NODE * spec,
			UNUSED_ARG PT_NODE * spec_parent)
{
  PT_NODE *result = NULL;	/* the list of names to return */
  PT_NODE *name;
  DB_OBJECT *db;		/* a temp for class object */
  const char *class_name = NULL;	/* a temp to extract name from class */

  /* check brain damage */
  if (spec == NULL)
    {
      return NULL;
    }
  if (spec->node_type != PT_SPEC)
    {
      assert (false);
      PT_INTERNAL_ERROR (parser, "resolution");
      return NULL;
    }

  name = spec->info.spec.entity_name;
  if (name == NULL)
    {
      /* is a derived table */
      assert (false);
      PT_INTERNAL_ERROR (parser, "resolution");
      return NULL;
    }				/* internal error */

  /* If name field points to a name node (i.e. is not a sublist ) then .. */
  if (name->node_type == PT_NAME)
    {
      class_name = name->info.name.original;

      /* Get the name */
      name->info.name.spec_id = spec->info.spec.id;
      name->info.name.meta_class = spec->info.spec.meta_class;
      /* Make sure this is the name of a class */
      db = pt_find_users_class (parser, name);
      name->info.name.db_object = db;
      if (db == NULL)
	{
	  return NULL;		/* error already set */
	}

      /* an error. Isn't a class name */
      /* create a new name node with this
         class name on it. Return it. */
      result = pt_name (parser, class_name);
      result->line_number = spec->line_number;
      result->column_number = spec->column_number;
      result->info.name.db_object = db;
      result->info.name.spec_id = spec->info.spec.id;
      result->info.name.meta_class = spec->info.spec.meta_class;
      return result;		/* there can be no except part */
    }

  assert (false);		/* is impossible */
  PT_INTERNAL_ERROR (parser, "resolution");
  return NULL;			/* internal error, wasn't a name or a sublist */
}


/*
 * pt_must_have_exposed_name () - MUST assign a name (even a default one)
 *      because later checks assume the range_var field is non-empty
 *   return: 0 if error or can't assign a name. 1 if successful
 *   parser(in):
 *   p(in): a PT_SPEC node (list)
 *
 * Note :
 *  	For each item on the entity_spec list:
 *        if not an entity spec, return 0
 *        if .range_var already assigned, continue
 *        if .range_var can be given the default (only class name) do it.
 *        if can't assign name, return 0
 *      Return 1 if every spec on list has a .range_var or can be given one
 *         Set the .resolved field in each item in the flat list to
 *         the corresponding exposed name. This is because in a query like:
 *            select x.name,y.name from p x, p y  both entity spec lists
 *         generate the same flat list. We want to be able to tell them
 *         apart later when we use them.
 */

static int
pt_must_have_exposed_name (PARSER_CONTEXT * parser, PT_NODE * p)
{
  PT_NODE *q = 0, *r;
  PT_NODE *spec_first = p;

  while (p)
    {
      if (p->node_type == PT_SPEC)
	{
	  /* if needs a name */
	  if (p->info.spec.range_var == NULL)
	    {
	      q = p->info.spec.entity_name;
	      /* if an exposed name is not given,
	       * then exposed name is itself. */
	      if (q && q->node_type == PT_NAME)
		{		/* not a sub list */
		  q->info.name.spec_id = p->info.spec.id;
		  q->info.name.meta_class = p->info.spec.meta_class;
		  p->info.spec.range_var = parser_copy_tree (parser, q);
		  p->info.spec.range_var->info.name.resolved = NULL;
		}
	      else
		{
		  const char *unique_exposed_name;
		  /*
		     Was sublist, they didn't give a correlation variable name so
		     We generate a unique name and attach it.
		   */
		  r = parser_new_node (parser, PT_NAME);

		  if (r == NULL)
		    {
		      PT_INTERNAL_ERROR (parser, "allocate new node");
		      return 0;
		    }

		  r->info.name.spec_id = p->info.spec.id;
		  r->info.name.meta_class = p->info.spec.meta_class;

		  unique_exposed_name =
		    pt_get_unique_exposed_name (parser, spec_first);

		  if (unique_exposed_name == NULL)
		    {
		      PT_INTERNAL_ERROR (parser, "allocate new table name");
		      return 0;
		    }

		  r->info.name.original = unique_exposed_name;
		  r->line_number = p->line_number;
		  r->column_number = p->column_number;
		  p->info.spec.range_var = r;
		}
	    }
	  p->info.spec.range_var->info.name.meta_class =
	    p->info.spec.meta_class;
	  /* If we get here, the item has a name. Copy name-pointer to the
	     resolved field of each item on flat entity list  */
	  q = p->info.spec.flat_entity_list;
	  while (q)
	    {
	      q->info.name.resolved
		= p->info.spec.range_var->info.name.original;
	      q = q->next;
	    }
	  p = p->next;
	}
    }				/* continue while() */

  return 1;
}


/*
 * pt_object_to_data_type () - create a PT_DATA_TYPE node that corresponds
 *                             to it and return it
 *   return: PT_NODE * to a data_type node
 *   parser(in):
 *   class_list(in):
 */
static PT_NODE *
pt_object_to_data_type (PARSER_CONTEXT * parser, PT_NODE * class_list)
{
  PT_NODE *result;
  result = parser_new_node (parser, PT_DATA_TYPE);
  if (result == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return NULL;
    }

  result->type_enum = PT_TYPE_OBJECT;
  result->info.data_type.entity = parser_copy_tree_list (parser, class_list);
  result->info.data_type.virt_type_enum = PT_TYPE_OBJECT;
  result->line_number = class_list->line_number;
  result->column_number = class_list->column_number;
  return result;
}

/*
 * pt_resolve_star () - resolve the '*' as in a query
 *      Replace the star with an equivalent list x.a, x.b, y.a, y.d ...
 *   return:
 *   parser(in):
 *   from(in): a PT_SELECT node
 *   attr(in): NULL if "*", non-NULL if "class_name.*"
 *
 * Note :
 * ASSUMES
 *    Flat entity lists in the 'from' clause have already been created.
 *    Items in from list all have or have been given an exposed name.
 */
PT_NODE *
pt_resolve_star (PARSER_CONTEXT * parser, PT_NODE * from, PT_NODE * attr)
{
  PT_NODE *flat_list, *derived_table;
  PT_NODE *spec_att, *attr_name, *range, *result = NULL;
  PT_NODE *spec = from;

  while (spec)
    {
      if (attr)
	{			/* resolve "class_name.*" */
	  if (attr->info.name.spec_id != spec->info.spec.id)
	    {
	      spec = spec->next;	/* skip to next spec */
	      continue;
	    }
	}

      flat_list = spec->info.spec.flat_entity_list;
      assert (pt_length_of_list (flat_list) <= 1);

      /* spec_att := all attributes of this entity spec */
      spec_att = NULL;
      derived_table = spec->info.spec.derived_table;
      if (derived_table)
	{
	  spec_att =
	    parser_copy_tree_list (parser, spec->info.spec.as_attr_list);
	}
      else
	{
	  /* get attribute list for this class flat */
	  spec_att = pt_get_all_attributes_and_types (parser,
						      flat_list, spec);
	}

      range = spec->info.spec.range_var;
      for (attr_name = spec_att; attr_name; attr_name = attr_name->next)
	{
	  if (range)
	    {
	      attr_name->info.name.resolved = range->info.name.original;
	    }

	  PT_NAME_INFO_SET_FLAG (attr_name, (attr) ? PT_NAME_INFO_DOT_STAR
				 : PT_NAME_INFO_STAR);
	}

      if (result)
	{
	  /* attach spec_att to end of result */
	  attr_name = result;
	  while (attr_name->next)
	    {
	      attr_name = attr_name->next;
	    }
	  attr_name->next = spec_att;
	}
      else
	{
	  result = spec_att;
	}

      if (attr)
	{
	  break;
	}
      spec = spec->next;
    }

  return result;
}

/*
 * pt_resolve_hint_args () -
 *   return: NO_ERROR on success, non-zero for ERROR
 *   parser(in):
 *   arg_list(in/out):
 *   spec_list(in):
 *   discard_no_match(in): remove unmatched node from arg_list
 */
static int
pt_resolve_hint_args (PARSER_CONTEXT * parser, PT_NODE ** arg_list,
		      PT_NODE * spec_list, bool discard_no_match)
{

  PT_NODE *arg, *spec, *range, *prev, *tmp;

  assert (arg_list != NULL);

  prev = NULL;
  arg = *arg_list;

  while (arg != NULL)
    {
      if (arg->node_type != PT_NAME || arg->info.name.original == NULL)
	{
	  goto exit_on_error;
	}

      /* check if the specified class name exists in spec list */
      for (spec = spec_list; spec; spec = spec->next)
	{
	  if (spec->node_type != PT_SPEC)
	    {
	      PT_INTERNAL_ERROR (parser, "resolution");
	      goto exit_on_error;
	    }

	  if ((range = spec->info.spec.range_var)
	      && !pt_str_compare (range->info.name.original,
				  arg->info.name.original, CASE_INSENSITIVE))
	    {
	      /* found match */
	      arg->info.name.spec_id = spec->info.spec.id;
	      arg->info.name.meta_class = PT_HINT_NAME;
	      break;
	    }
	}

      /* not found */
      if (spec == NULL)
	{
	  if (discard_no_match)
	    {
	      tmp = arg;
	      arg = arg->next;
	      tmp->next = NULL;
	      parser_free_node (parser, tmp);

	      if (prev == NULL)
		{
		  *arg_list = arg;
		}
	      else
		{
		  prev->next = arg;
		}
	    }
	  else
	    {
	      goto exit_on_error;
	    }
	}
      else
	{
	  prev = arg;
	  arg = arg->next;
	}
    }

  return NO_ERROR;
exit_on_error:

  return ER_FAILED;
}

/*
 * pt_resolve_hint () -
 *   return:
 *   parser(in):
 *   node(in):
 */
static int
pt_resolve_hint (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_HINT_ENUM hint = PT_HINT_NONE;
  PT_NODE **ordered = NULL, **use_nl = NULL, **use_idx = NULL;
  PT_NODE *spec_list = NULL;

  if (!node || !parser)
    {
      assert (false);
      return ER_FAILED;
    }

  switch (node->node_type)
    {
    case PT_SELECT:
      hint = node->info.query.q.select.hint;
#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
      ordered = &node->info.query.q.select.ordered_hint;
#endif
      use_nl = &node->info.query.q.select.use_nl_hint;
      use_idx = &node->info.query.q.select.use_idx_hint;
      spec_list = node->info.query.q.select.from;
      break;
    case PT_DELETE:
      hint = node->info.delete_.hint;
#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
      ordered = &node->info.delete_.ordered_hint;
#endif
      use_nl = &node->info.delete_.use_nl_hint;
      use_idx = &node->info.delete_.use_idx_hint;
      spec_list = node->info.delete_.spec;
      break;
    case PT_UPDATE:
      hint = node->info.update.hint;
#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
      ordered = &node->info.update.ordered_hint;
#endif
      use_nl = &node->info.update.use_nl_hint;
      use_idx = &node->info.update.use_idx_hint;
      spec_list = node->info.update.spec;
      assert (spec_list != NULL);
      break;
    default:
      PT_INTERNAL_ERROR (parser, "Invalid statement in hints resolving");
      return ER_FAILED;
    }

#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
  if (hint & PT_HINT_ORDERED)
    {
      if (pt_resolve_hint_args (parser, ordered, spec_list,
				REQUIRE_ALL_MATCH) != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }
#endif

#if 0
  if (hint & PT_HINT_Y)
    {				/* not used */
    }
#endif /* 0 */

  if (hint & PT_HINT_USE_NL)
    {
      if (pt_resolve_hint_args (parser, use_nl, spec_list,
				REQUIRE_ALL_MATCH) != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }

  if (hint & PT_HINT_USE_IDX)
    {
      if (pt_resolve_hint_args (parser, use_idx, spec_list,
				REQUIRE_ALL_MATCH) != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }

  return NO_ERROR;

exit_on_error:

  /* clear hint info */
  node->info.query.q.select.hint = PT_HINT_NONE;
  if (ordered != NULL && *ordered != NULL)
    {
      parser_free_tree (parser, *ordered);
    }
  if (use_nl != NULL && *use_nl != NULL)
    {
      parser_free_tree (parser, *use_nl);
    }
  if (use_idx != NULL && *use_idx != NULL)
    {
      parser_free_tree (parser, *use_idx);
    }

  switch (node->node_type)
    {
    case PT_SELECT:
#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
      node->info.query.q.select.ordered_hint = NULL;
#endif
      node->info.query.q.select.use_nl_hint = NULL;
      node->info.query.q.select.use_idx_hint = NULL;
      break;
    case PT_DELETE:
#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
      node->info.delete_.ordered_hint = NULL;
#endif
      node->info.delete_.use_nl_hint = NULL;
      node->info.delete_.use_idx_hint = NULL;
      break;
    case PT_UPDATE:
#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
      node->info.update.ordered_hint = NULL;
#endif
      node->info.update.use_nl_hint = NULL;
      node->info.update.use_idx_hint = NULL;
      break;
    default:
      break;
    }

  return ER_FAILED;
}

/*
 * pt_resolve_using_index () -
 *   return:
 *   parser(in):
 *   index(in):
 *   from(in):
 */
PT_NODE *
pt_resolve_using_index (PARSER_CONTEXT * parser,
			PT_NODE * index, PT_NODE * from)
{
  PT_NODE *spec, *range, *entity;
  SM_CLASS_CONSTRAINT *cons;
  DB_OBJECT *classop;
  SM_CLASS *class_;
  int errid;

  if (index == NULL)
    {
      assert (false);		/* is impossible */
      return index;
    }

  assert (index != NULL);

  if (index->info.name.original == NULL
      && index->etc != (void *) PT_IDX_HINT_CLASS_NONE)
    {
      /* the case of USING INDEX NONE */
      return index;
    }

  if (index->info.name.spec_id != 0)	/* already resolved */
    {
      return index;
    }

  if (index->info.name.resolved != NULL)
    {
      /* index name is specified by class name as "class.index" */

      /* check if the specified class name exists in spec list */
      for (spec = from; spec; spec = spec->next)
	{
	  if (spec->node_type != PT_SPEC)
	    {
	      PT_INTERNAL_ERROR (parser, "resolution");
	      return NULL;
	    }

	  range = spec->info.spec.range_var;
	  entity = spec->info.spec.entity_name;
	  if (range && entity
	      && !pt_str_compare (range->info.name.original,
				  index->info.name.resolved,
				  CASE_INSENSITIVE))
	    {
	      classop = sm_find_class (entity->info.name.original);
	      if (au_fetch_class (classop, &class_, S_LOCK, AU_SELECT)
		  != NO_ERROR)
		{
		  errid = er_errid ();
		  if (errid == ER_AU_SELECT_FAILURE
		      || errid == ER_AU_AUTHORIZATION_FAILURE)
		    {
		      PT_ERRORc (parser, entity, er_msg ());
		    }
		  else
		    {
		      PT_INTERNAL_ERROR (parser, "resolution");
		    }

		  return NULL;
		}
	      if (index->info.name.original)
		{
		  cons = classobj_find_class_index (class_,
						    index->info.name.
						    original);
		  if (cons == NULL
		      || cons->index_status != INDEX_STATUS_COMPLETED)
		    {
		      /* the index is not for the specified class */
		      index->info.name.resolved = NULL;
		      return index;	/* invalid index; ignore */
		    }
		}
	      index->info.name.spec_id = spec->info.spec.id;
	      index->info.name.meta_class = PT_INDEX_NAME;
	      /* "class.index" is valid */
	      return index;
	    }
	}

      /* the specified class in "class.index" does not exist in spec list */
      index->info.name.resolved = NULL;
      return index;		/* invalid index; ignore */
    }
  else
    {				/* if (index->info.name.resolved != NULL) */
      /* index name without class name specification */

      /* find the class of the index from spec list */
      for (spec = from; spec; spec = spec->next)
	{
	  if (spec->node_type != PT_SPEC)
	    {
	      PT_INTERNAL_ERROR (parser, "resolution");
	      return NULL;
	    }

	  range = spec->info.spec.range_var;
	  entity = spec->info.spec.entity_name;
	  if (range != NULL
	      && entity != NULL && entity->info.name.original != NULL)
	    {
	      classop = sm_find_class (entity->info.name.original);
	      if (classop == NULL)
		{
		  break;
		}
	      if (au_fetch_class (classop, &class_, S_LOCK,
				  AU_SELECT) != NO_ERROR)
		{
		  errid = er_errid ();
		  if (errid == ER_AU_SELECT_FAILURE
		      || errid == ER_AU_AUTHORIZATION_FAILURE)
		    {
		      PT_ERRORc (parser, entity, er_msg ());
		    }
		  else
		    {
		      PT_INTERNAL_ERROR (parser, "resolution");
		    }

		  return NULL;
		}

	      cons = classobj_find_class_index (class_,
						index->info.name.original);
	      if (cons != NULL
		  && cons->index_status == INDEX_STATUS_COMPLETED)
		{
		  /* found the first matched class; resolve index name */
		  index->info.name.resolved = range->info.name.original;
		  index->info.name.spec_id = spec->info.spec.id;
		  index->info.name.meta_class = PT_INDEX_NAME;
		  break;
		}
	    }
	}

    }

  return index;
}

/*
 * pt_str_compare () -
 *   return: 0 if two strings are equal. 1 if not equal
 *   p(in): A string
 *   q(in): A string
 *
 * Note :
 * two NULL strings are considered a match.
 * two strings of length zero match
 * A NULL string does NOT match a zero length string
 */
int
pt_str_compare (const char *p, const char *q, CASE_SENSITIVENESS case_flag)
{
  if (!p && !q)
    {
      return 0;
    }
  if (!p || !q)
    {
      return 1;
    }

  if (case_flag == CASE_INSENSITIVE)
    {
      return intl_identifier_casecmp (p, q);
    }
  else
    {
      return intl_identifier_cmp (p, q);
    }
}

/*
 * pt_get_unique_exposed_name () -
 *   return:
 *
 *   parser(in):
 *   first_spec(in):
 */
static const char *
pt_get_unique_exposed_name (PARSER_CONTEXT * parser, PT_NODE * first_spec)
{
  char name_buf[32];
  int i = 1;

  if (first_spec->node_type != PT_SPEC)
    {
      assert (first_spec->node_type == PT_SPEC);
      return NULL;
    }

  while (1)
    {
      snprintf (name_buf, 32, "__t%u", i);
      if (pt_name_occurs_in_from_list (parser, name_buf, first_spec) == 0)
	{
	  return pt_append_string (parser, NULL, name_buf);
	}
      i++;
    }

  return NULL;
}

/*
 * is_pt_name_in_group_having () -
 *   return:
 *   node(in):
 */
static bool
is_pt_name_in_group_having (PT_NODE * node)
{
  if (node == NULL || node->node_type != PT_NAME || node->etc == NULL)
    {
      return false;
    }

  if (intl_identifier_casecmp
      ((char *) node->etc, CPTR_PT_NAME_IN_GROUP_HAVING) == 0)
    {
      return true;
    }

  return false;
}

/*
 * pt_mark_pt_name () -
 *   return:
 *   parser(in):
 *   node(in/out):
 *   chk_parent(in):
 *   continue_walk(in/out):
 */
static PT_NODE *
pt_mark_pt_name (PARSER_CONTEXT * parser, PT_NODE * node,
		 UNUSED_ARG void *chk_parent, int *continue_walk)
{
  *continue_walk = PT_CONTINUE_WALK;

  if (node == NULL || node->node_type != PT_NAME)
    {
      return node;
    }
  node->etc =
    (void *) pt_append_string (parser, NULL, CPTR_PT_NAME_IN_GROUP_HAVING);

  return node;
}

/*
 * pt_mark_group_having_pt_name () - Mark the PT_NAME in group by / having.
 *   return:
 *   parser(in):
 *   node(in/out):
 *   chk_parent(in):
 *   continue_walk(in/out):
 */
static PT_NODE *
pt_mark_group_having_pt_name (PARSER_CONTEXT * parser, PT_NODE * node,
			      UNUSED_ARG void *chk_parent, int *continue_walk)
{
  *continue_walk = PT_CONTINUE_WALK;

  if (node == NULL || node->node_type != PT_SELECT)
    {
      return node;
    }

  if (node->info.query.q.select.group_by != NULL)
    {
      node->info.query.q.select.group_by =
	parser_walk_tree (parser, node->info.query.q.select.group_by,
			  pt_mark_pt_name, NULL, NULL, NULL);
    }

  if (node->info.query.q.select.having != NULL)
    {
      node->info.query.q.select.having =
	parser_walk_tree (parser, node->info.query.q.select.having,
			  pt_mark_pt_name, NULL, NULL, NULL);
    }

  return node;
}

/*
 * pt_resolve_group_having_alias_pt_sort_spec () -
 *   return:
 *   parser(in):
 *   node(in/out):
 *   select_list(in):
 */
static void
pt_resolve_group_having_alias_pt_sort_spec (PARSER_CONTEXT * parser,
					    PT_NODE * node,
					    PT_NODE * select_list)
{
  if (node != NULL && node->node_type == PT_SORT_SPEC)
    {
      pt_resolve_group_having_alias_internal (parser,
					      &(node->info.sort_spec.expr),
					      select_list);
    }
}

/*
 * pt_resolve_group_having_alias_pt_name () -
 *   return:
 *   parser(in):
 *   node_p(in/out):
 *   select_list(in):
 */
static void
pt_resolve_group_having_alias_pt_name (PARSER_CONTEXT * parser,
				       PT_NODE ** node_p,
				       PT_NODE * select_list)
{
  PT_NODE *col;
  char *n_str;
  PT_NODE *node;

  assert (node_p != NULL);

  node = *node_p;

  if (node == NULL || node->node_type != PT_NAME)
    {
      return;
    }

  /* It have been resolved. */
  if (node->info.name.resolved != NULL)
    {
      return;
    }

  n_str = parser_print_tree (parser, *node_p);

  for (col = select_list; col != NULL; col = col->next)
    {
      if (col->alias_print != NULL
	  && intl_identifier_casecmp (n_str, col->alias_print) == 0)
	{
	  parser_free_node (parser, *node_p);
	  *node_p = parser_copy_tree (parser, col);
	  if ((*node_p) != NULL)
	    {
	      (*node_p)->next = NULL;
	    }
	  break;
	}
    }

  /* We can not resolve the pt_name. */
  if (col == NULL)
    {
      PT_ERRORmf (parser, (*node_p), MSGCAT_SET_PARSER_SEMANTIC,
		  MSGCAT_SEMANTIC_IS_NOT_DEFINED,
		  pt_short_print (parser, (*node_p)));
    }
}

/*
 * pt_resolve_group_having_alias_pt_expr () -
 *   return:
 *   parser(in):
 *   node(in/out):
 *   select_list(in):
 */
static void
pt_resolve_group_having_alias_pt_expr (PARSER_CONTEXT * parser,
				       PT_NODE * node, PT_NODE * select_list)
{
  if (node == NULL || node->node_type != PT_EXPR)
    {
      return;
    }

  /* Resolve arg1 */
  if (node->info.expr.arg1 != NULL
      && node->info.expr.arg1->node_type == PT_NAME)
    {
      pt_resolve_group_having_alias_pt_name (parser, &node->info.expr.arg1,
					     select_list);
    }
  else if (node->info.expr.arg1 != NULL
	   && node->info.expr.arg1->node_type == PT_EXPR)
    {
      pt_resolve_group_having_alias_pt_expr (parser, node->info.expr.arg1,
					     select_list);
    }
  else
    {

    }

  /* Resolve arg2 */
  if (node->info.expr.arg2 != NULL
      && node->info.expr.arg2->node_type == PT_NAME)
    {
      pt_resolve_group_having_alias_pt_name (parser, &node->info.expr.arg2,
					     select_list);
    }
  else if (node->info.expr.arg2 != NULL
	   && node->info.expr.arg2->node_type == PT_EXPR)
    {
      pt_resolve_group_having_alias_pt_expr (parser, node->info.expr.arg2,
					     select_list);
    }
  else
    {

    }

  /* Resolve arg3 */
  if (node->info.expr.arg3 != NULL
      && node->info.expr.arg3->node_type == PT_NAME)
    {
      pt_resolve_group_having_alias_pt_name (parser, &node->info.expr.arg3,
					     select_list);
    }
  else if (node->info.expr.arg3 != NULL
	   && node->info.expr.arg3->node_type == PT_EXPR)
    {
      pt_resolve_group_having_alias_pt_expr (parser, node->info.expr.arg3,
					     select_list);
    }
  else
    {

    }
}

/*
 * pt_resolve_group_having_alias_internal () - Rosolve alias name in groupby and having clause.
 *   return:
 *   parser(in):
 *   node_p(in/out):
 *   select_list(in):
 */
static void
pt_resolve_group_having_alias_internal (PARSER_CONTEXT * parser,
					PT_NODE ** node_p,
					PT_NODE * select_list)
{
  assert (node_p != NULL);
  assert ((*node_p) != NULL);

  switch ((*node_p)->node_type)
    {
    case PT_NAME:
      pt_resolve_group_having_alias_pt_name (parser, node_p, select_list);
      break;
    case PT_EXPR:
      pt_resolve_group_having_alias_pt_expr (parser, *node_p, select_list);
      break;
    case PT_SORT_SPEC:
      pt_resolve_group_having_alias_pt_sort_spec (parser, *node_p,
						  select_list);
      break;
    default:
      return;
    }
  return;
}

/*
 * pt_resolve_group_having_alias () - Resolve alias name in groupby and having clause. We
 *     resolve groupby/having alias after bind_name, it means when the alias name is same
 *     with table attribute, we choose table attribute firstly.
 *   return:
 *   parser(in):
 *   node(in/out):
 *   chk_parent(in):
 *   continue_walk(in/out):
 */
static PT_NODE *
pt_resolve_group_having_alias (PARSER_CONTEXT * parser, PT_NODE * node,
			       UNUSED_ARG void *chk_parent,
			       int *continue_walk)
{
  PT_NODE *pt_cur;

  *continue_walk = PT_CONTINUE_WALK;

  if (node == NULL || node->node_type != PT_SELECT)
    {
      return node;
    }

  /* support for alias in GROUP BY */
  pt_cur = node->info.query.q.select.group_by;
  while (pt_cur != NULL)
    {
      pt_resolve_group_having_alias_internal (parser, &pt_cur,
					      node->info.query.q.select.list);
      pt_cur = pt_cur->next;
    }

  /* support for alias in HAVING */
  pt_cur = node->info.query.q.select.having;
  while (pt_cur != NULL)
    {
      pt_resolve_group_having_alias_internal (parser, &pt_cur,
					      node->info.query.q.select.list);
      pt_cur = pt_cur->next;
    }
  return node;
}

/*
 * pt_resolve_names () -
 *   return:
 *   parser(in):
 *   statement(in):
 *   sc_info(in):
 */
PT_NODE *
pt_resolve_names (PARSER_CONTEXT * parser, PT_NODE * statement,
		  SEMANTIC_CHK_INFO * sc_info)
{
  PT_BIND_NAMES_ARG bind_arg;
  PT_NODE *chk_parent = NULL;

  bind_arg.scopes = NULL;
  bind_arg.sc_info = sc_info;

  assert (sc_info != NULL);

  /* Replace each Entity Spec with an Equivalent flat list */
  statement =
    parser_walk_tree (parser, statement, pt_flat_spec_pre,
		      &chk_parent, pt_continue_walk, NULL);

  /* resolve names in search conditions, assignments, and assignations */
  if (!pt_has_error (parser))
    {
      PT_NODE *idx_name = NULL;
      if (statement->node_type == PT_CREATE_INDEX
	  || statement->node_type == PT_ALTER_INDEX
	  || statement->node_type == PT_DROP_INDEX)
	{
	  /* backup the name of the index because it is not part of the
	     table spec yet */
	  idx_name = statement->info.index.index_name;
	  statement->info.index.index_name = NULL;
	}

      /* Before pt_bind_name, we mark PT_NAME in group by/ having. */
      statement =
	parser_walk_tree (parser, statement, pt_mark_group_having_pt_name,
			  NULL, NULL, NULL);

      statement =
	parser_walk_tree (parser, statement, pt_bind_names, &bind_arg,
			  pt_bind_names_post, &bind_arg);
      if (statement && (statement->node_type == PT_CREATE_INDEX
			|| statement->node_type == PT_ALTER_INDEX
			|| statement->node_type == PT_DROP_INDEX))
	{
	  statement->info.index.index_name = idx_name;
	}

      /* Resolve alias in group by/having. */
      statement =
	parser_walk_tree (parser, statement, pt_resolve_group_having_alias,
			  NULL, NULL, NULL);

    }

  /* Flag specs from FOR UPDATE clause with PT_SPEC_FLAG_FOR_UPDATE_CLAUSE and
   * clear the for_update list. From now on the specs from FOR UPDATE clause can
   * be determined using this flag together with PT_SELECT_INFO_FOR_UPDATE
   * flag */
  if (statement != NULL && statement->node_type == PT_SELECT
      && PT_SELECT_INFO_IS_FLAGED (statement, PT_SELECT_INFO_FOR_UPDATE))
    {
      PT_NODE *spec = NULL;

      /* Flag all specs */
      for (spec = statement->info.query.q.select.from; spec != NULL;
	   spec = spec->next)
	{
	  if (spec->info.spec.derived_table == NULL)
	    {
	      spec->info.spec.flag |= PT_SPEC_FLAG_FOR_UPDATE_CLAUSE;
	    }
	}
    }

  return statement;
}

/*
 * pt_find_outer_entity_in_scopes () -
 *   return:
 *   parser(in):
 *   scopes(in):
 *   spec_id(in):
 *   scope_location(in):
 */
static PT_NODE *
pt_find_outer_entity_in_scopes (PARSER_CONTEXT * parser,
				SCOPES * scopes,
				UINTPTR spec_id, short *scope_location)
{
  PT_NODE *spec, *temp;
  int location = 0;
  if (scopes == NULL)
    {
      PT_INTERNAL_ERROR (parser, "resolution");
      return NULL;
    }

  for (spec = scopes->specs; spec; spec = spec->next)
    {
      if (spec->node_type != PT_SPEC)
	{
	  PT_INTERNAL_ERROR (parser, "resolution");
	  return NULL;
	}
      if (spec->info.spec.join_type == PT_JOIN_NONE)
	{
	  location = spec->info.spec.location;
	}
      if (spec->info.spec.id == spec_id)
	{
	  for (temp = spec;
	       temp && temp->info.spec.location < *scope_location;
	       temp = temp->next)
	    {
	      if (temp->info.spec.join_type == PT_JOIN_NONE)
		{
		  location = temp->info.spec.location;
		}
	    }
	  *scope_location = location;
	  return spec;
	}
    }

  return NULL;
}

/*
 * pt_op_type_from_default_expr_type () - returns the corresponding PT_OP_TYPE
 *					  for the given default expression
 *   return: a PT_OP_TYPE (the desired operation)
 *   expr_type(in): a DB_DEFAULT_EXPR_TYPE (the default expression)
 */
PT_OP_TYPE
pt_op_type_from_default_expr_type (DB_DEFAULT_EXPR_TYPE expr_type)
{
  switch (expr_type)
    {
    case DB_DEFAULT_SYSDATE:
      return PT_SYS_DATE;

    case DB_DEFAULT_SYSDATETIME:
      return PT_SYS_DATETIME;

    case DB_DEFAULT_UNIX_TIMESTAMP:
      return PT_UNIX_TIMESTAMP;

    case DB_DEFAULT_USER:
      return PT_USER;

    case DB_DEFAULT_CURR_USER:
      return PT_CURRENT_USER;

    default:
      return (PT_OP_TYPE) 0;
    }
}

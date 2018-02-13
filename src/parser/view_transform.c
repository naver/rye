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
 * view_transform.c - Functions for the translation of virtual queries
 */

#ident "$Id$"

#include <assert.h>

#include "view_transform.h"
#include "parser.h"
#include "parser_message.h"
#include "schema_manager.h"
#include "semantic_check.h"
#include "optimizer.h"
#include "execute_schema.h"

#include "dbi.h"
#include "transform.h"
#include "object_accessor.h"
#include "locator_cl.h"

#define PT_PUSHABLE_TERM(p) \
  ((p)->out.pushable && (p)->out.correlated_found == false)

#define MAX_CYCLE 300

#define MQ_IS_OUTER_JOIN_SPEC(s)                               \
    (						               \
     ((s)->info.spec.join_type == PT_JOIN_LEFT_OUTER           \
    || (s)->info.spec.join_type == PT_JOIN_RIGHT_OUTER) ||     \
     ((s)->next &&                                             \
      ((s)->next->info.spec.join_type == PT_JOIN_LEFT_OUTER    \
    || (s)->next->info.spec.join_type == PT_JOIN_RIGHT_OUTER)) \
    )

typedef enum
{ FIND_ID_INLINE_VIEW = 0, FIND_ID_VCLASS } FIND_ID_TYPE;

typedef struct find_id_info
{
  struct
  {				/* input section */
    PT_NODE *spec;
    PT_NODE *others_spec_list;
    PT_NODE *attr_list;
    PT_NODE *query_list;
  } in;
  FIND_ID_TYPE type;
  struct
  {				/* output section */
    bool found;
    bool others_found;
    bool correlated_found;
    bool pushable;
  } out;
} FIND_ID_INFO;

typedef struct mq_bump_core_info
{
  int match_level;
  int increment;
}
MQ_BUMP_CORR_INFO;

typedef struct check_pushable_info
{
  bool check_query;
  bool check_xxxnum;

  bool query_found;
  bool xxxnum_found;		/* rownum, inst_num(), orderby_num(), groupby_num() */
} CHECK_PUSHABLE_INFO;

static unsigned int top_cycle = 0;
static DB_OBJECT *cycle_buffer[MAX_CYCLE];

typedef struct exists_info EXISTS_INFO;
struct exists_info
{
  PT_NODE *spec;
  int referenced;
};

typedef struct pt_reset_select_spec_info PT_RESET_SELECT_SPEC_INFO;
struct pt_reset_select_spec_info
{
  UINTPTR id;
  PT_NODE **statement;
};

typedef struct replace_name_info REPLACE_NAME_INFO;
struct replace_name_info
{
  PT_NODE *path;
  UINTPTR spec_id;
  PT_NODE *newspec;		/* for new sharedd attr specs */
};

typedef struct spec_reset_info SPEC_RESET_INFO;
struct spec_reset_info
{
  PT_NODE *statement;
  PT_NODE *old_next;
};

typedef struct extra_specs_frame PT_EXTRA_SPECS_FRAME;
struct extra_specs_frame
{
  struct extra_specs_frame *next;
  PT_NODE *extra_specs;
};

typedef struct mq_lambda_arg MQ_LAMBDA_ARG;
struct mq_lambda_arg
{
  PT_NODE *name_list;
  PT_NODE *tree_list;
  PT_EXTRA_SPECS_FRAME *spec_frames;
};

static PT_NODE *mq_bump_corr_pre (PARSER_CONTEXT * parser, PT_NODE * node,
				  void *void_arg, int *continue_walk);
static PT_NODE *mq_bump_corr_post (PARSER_CONTEXT * parser, PT_NODE * node,
				   void *void_arg, int *continue_walk);
static PT_NODE *mq_union_bump_correlation (PARSER_CONTEXT * parser,
					   PT_NODE * left, PT_NODE * right);
static DB_AUTH mq_compute_authorization (DB_OBJECT * class_object);
static DB_AUTH mq_compute_query_authorization (PT_NODE * statement);
static void mq_set_union_query (PARSER_CONTEXT * parser, PT_NODE * statement,
				PT_MISC_TYPE is_union);
static PT_NODE *mq_rewrite_agg_names (PARSER_CONTEXT * parser, PT_NODE * node,
				      void *void_arg, int *continue_walk);
static PT_NODE *mq_rewrite_agg_names_post (PARSER_CONTEXT * parser,
					   PT_NODE * node, void *void_arg,
					   int *continue_walk);
static PT_NODE *mq_substitute_select_in_statement (PARSER_CONTEXT * parser,
						   PT_NODE * statement,
						   PT_NODE * query_spec,
						   PT_NODE * class_);
static PT_NODE *mq_substitute_subquery_in_statement (PARSER_CONTEXT * parser,
						     PT_NODE * statement,
						     PT_NODE * query_spec,
						     PT_NODE * class_,
						     PT_NODE * order_by,
						     int what_for);
static int mq_translatable_class (PARSER_CONTEXT * parser, PT_NODE * class_);
static bool mq_is_union_translation (PARSER_CONTEXT * parser, PT_NODE * spec);
static PT_NODE *mq_translate_tree (PARSER_CONTEXT * parser, PT_NODE * tree,
				   PT_NODE * spec_list, PT_NODE * order_by,
				   int what_for);
static PT_NODE *mq_corr_subq_pre (PARSER_CONTEXT * parser,
				  PT_NODE * node, void *void_arg,
				  int *continue_walk);
static bool mq_has_corr_subqueries (PARSER_CONTEXT * parser, PT_NODE * node);
static PT_NODE *pt_check_pushable (PARSER_CONTEXT * parser, PT_NODE * tree,
				   void *arg, int *continue_walk);
static bool pt_pushable_query_in_pos (PARSER_CONTEXT * parser,
				      PT_NODE * query, int pos);
static PT_NODE *pt_find_only_name_id (PARSER_CONTEXT * parser, PT_NODE * tree,
				      void *arg, int *continue_walk);
static bool pt_sargable_term (PARSER_CONTEXT * parser, PT_NODE * term,
			      FIND_ID_INFO * infop);
static bool mq_is_pushable_subquery (PARSER_CONTEXT * parser, PT_NODE * query,
				     bool is_only_spec);
static int pt_check_copypush_subquery (PARSER_CONTEXT * parser,
				       PT_NODE * query);
static void pt_copypush_terms (PARSER_CONTEXT * parser, PT_NODE * spec,
			       PT_NODE * query, PT_NODE * term_list,
			       FIND_ID_TYPE type);
static int mq_copypush_sargable_terms_helper (PARSER_CONTEXT * parser,
					      PT_NODE * statement,
					      PT_NODE * spec,
					      PT_NODE * new_query,
					      FIND_ID_INFO * infop);
static int mq_copypush_sargable_terms (PARSER_CONTEXT * parser,
				       PT_NODE * statement, PT_NODE * spec);
static PT_NODE *mq_rewrite_vclass_spec_as_derived (PARSER_CONTEXT * parser,
						   PT_NODE * statement,
						   PT_NODE * spec,
						   PT_NODE * query_spec);
static PT_NODE *mq_translate_select (PARSER_CONTEXT * parser,
				     PT_NODE * select_statement);
static void mq_check_update (PARSER_CONTEXT * parser,
			     PT_NODE * update_statement);
static void mq_check_delete (PARSER_CONTEXT * parser, PT_NODE * delete_stmt);
static PT_NODE *mq_translate_update (PARSER_CONTEXT * parser,
				     PT_NODE * update_statement);
static PT_NODE *mq_translate_insert (PARSER_CONTEXT * parser,
				     PT_NODE * insert_statement);
static PT_NODE *mq_translate_delete (PARSER_CONTEXT * parser,
				     PT_NODE * delete_statement);
static PT_NODE *mq_check_rewrite_select (PARSER_CONTEXT * parser,
					 PT_NODE * select_statement);
static PT_NODE *mq_push_paths (PARSER_CONTEXT * parser, PT_NODE * statement,
			       void *void_arg, int *continue_walk);
static PT_NODE *mq_translate_local (PARSER_CONTEXT * parser,
				    PT_NODE * statement, void *void_arg,
				    int *continue_walk);
static int mq_check_using_index (PARSER_CONTEXT * parser,
				 PT_NODE * using_index);
static PT_NODE *mq_set_types (PARSER_CONTEXT * parser, PT_NODE * query_spec,
			      PT_NODE * attributes,
			      DB_OBJECT * vclass_object);
static PT_NODE *mq_translate_subqueries (PARSER_CONTEXT * parser,
					 DB_OBJECT * class_object,
					 PT_NODE * attributes,
					 DB_AUTH * authorization);
static bool mq_check_cycle (DB_OBJECT * class_object);

static PT_NODE *mq_mark_location (PARSER_CONTEXT * parser, PT_NODE * node,
				  void *arg, int *continue_walk);
static PT_NODE *mq_check_non_updatable_vclass_oid (PARSER_CONTEXT * parser,
						   PT_NODE * node, void *arg,
						   int *continue_walk);
static PT_NODE *mq_translate_helper (PARSER_CONTEXT * parser, PT_NODE * node);


static PT_NODE *mq_lookup_symbol (PARSER_CONTEXT * parser,
				  PT_NODE * attr_list, PT_NODE * attr);

static PT_NODE *mq_coerce_resolved (PARSER_CONTEXT * parser, PT_NODE * node,
				    void *void_arg, int *continue_walk);
static PT_NODE *mq_reset_all_ids (PARSER_CONTEXT * parser, PT_NODE * node,
				  void *void_arg, int *continue_walk);
static PT_NODE *mq_clear_all_ids (PARSER_CONTEXT * parser, PT_NODE * node,
				  void *void_arg, int *continue_walk);
static PT_NODE *mq_clear_other_ids (PARSER_CONTEXT * parser, PT_NODE * node,
				    void *void_arg, int *continue_walk);
static PT_NODE *mq_reset_spec_ids (PARSER_CONTEXT * parser, PT_NODE * node,
				   void *void_arg, int *continue_walk);
static PT_NODE *mq_get_references_node (PARSER_CONTEXT * parser,
					PT_NODE * node, void *void_arg,
					int *continue_walk);
static PT_NODE *mq_set_references_local (PARSER_CONTEXT * parser,
					 PT_NODE * statement, PT_NODE * spec);
static PT_NODE *mq_reset_select_spec_node (PARSER_CONTEXT * parser,
					   PT_NODE * node, void *void_arg,
					   int *continue_walk);
static PT_NODE *mq_reset_select_specs (PARSER_CONTEXT * parser,
				       PT_NODE * node, void *void_arg,
				       int *continue_walk);
static PT_NODE *mq_reset_spec_distr_subpath_pre (PARSER_CONTEXT * parser,
						 PT_NODE * spec,
						 void *void_arg,
						 int *continue_walk);
static PT_NODE *mq_reset_spec_distr_subpath_post (PARSER_CONTEXT * parser,
						  PT_NODE * spec,
						  void *void_arg,
						  int *continue_walk);
#if defined (ENABLE_UNUSED_FUNCTION)
static void mq_invert_insert_select (PARSER_CONTEXT * parser, PT_NODE * attr,
				     PT_NODE * subquery);
static void mq_invert_insert_subquery (PARSER_CONTEXT * parser,
				       PT_NODE ** attr, PT_NODE * subquery);
#endif
static PT_NODE *mq_lambda_node_pre (PARSER_CONTEXT * parser, PT_NODE * tree,
				    void *void_arg, int *continue_walk);
static PT_NODE *mq_lambda_node (PARSER_CONTEXT * parser, PT_NODE * node,
				void *void_arg, int *continue_walk);
#if defined (ENABLE_UNUSED_FUNCTION)
static PT_NODE *mq_set_virt_object (PARSER_CONTEXT * parser, PT_NODE * node,
				    void *void_arg, int *continue_walk);
#endif
static PT_NODE *mq_fix_derived (PARSER_CONTEXT * parser,
				PT_NODE * select_statement, PT_NODE * spec);
static PT_NODE *mq_translate_value (PARSER_CONTEXT * parser, PT_NODE * value);
static PT_NODE *mq_fetch_subqueries_for_update_local (PARSER_CONTEXT * parser,
						      PT_NODE * class_,
						      DB_AUTH what_for,
						      PARSER_CONTEXT **
						      qry_cache);
static PT_NODE *mq_reset_specs_from_column (PARSER_CONTEXT * parser,
					    PT_NODE * statement,
					    PT_NODE * column);

static PT_NODE *mq_fetch_attributes (PARSER_CONTEXT * parser,
				     PT_NODE * class_);

static PT_NODE *mq_lambda (PARSER_CONTEXT * parser, PT_NODE * tree_with_names,
			   PT_NODE * name_node, PT_NODE * corresponding_tree);

static PT_NODE *mq_class_lambda (PARSER_CONTEXT * parser, PT_NODE * statement,
				 PT_NODE * class_,
				 PT_NODE * corresponding_spec,
				 PT_NODE * class_where_part,
				 PT_NODE * class_group_by_part,
				 PT_NODE * class_having_part);

static PT_NODE *mq_fix_derived_in_union (PARSER_CONTEXT * parser,
					 PT_NODE * statement,
					 UINTPTR spec_id);

static PT_NODE *mq_fetch_subqueries (PARSER_CONTEXT * parser,
				     PT_NODE * class_);

static PT_NODE *mq_fetch_subqueries_for_update (PARSER_CONTEXT * parser,
						PT_NODE * class_,
						DB_AUTH what_for);

static PT_NODE *mq_rename_resolved (PARSER_CONTEXT * parser, PT_NODE * spec,
				    PT_NODE * statement, const char *newname);

static PT_NODE *mq_reset_ids_and_references (PARSER_CONTEXT * parser,
					     PT_NODE * statement,
					     PT_NODE * spec);

static PT_NODE *mq_reset_ids_and_references_helper (PARSER_CONTEXT * parser,
						    PT_NODE * statement,
						    PT_NODE * spec,
						    bool
						    get_spec_referenced_attr);

static void mq_insert_symbol (PARSER_CONTEXT * parser, PT_NODE ** listhead,
			      PT_NODE * attr);

static const char *get_authorization_name (DB_AUTH auth);

static PT_NODE *mq_add_dummy_from_pre (PARSER_CONTEXT * parser,
				       PT_NODE * node, void *arg,
				       int *continue_walk);
static PT_NODE *mq_update_order_by (PARSER_CONTEXT * parser,
				    PT_NODE * statement, PT_NODE * query_spec,
				    PT_NODE * class_);

static PT_NODE *mq_reset_references_to_query_string (PARSER_CONTEXT * parser,
						     PT_NODE * node,
						     void *arg,
						     int *continue_walk);

static PT_NODE *pt_check_for_update_subquery (PARSER_CONTEXT * parser,
					      PT_NODE * node, void *arg,
					      int *continue_walk);

static int pt_check_for_update_clause (PARSER_CONTEXT * parser,
				       PT_NODE * query, bool root);

static int pt_for_update_prepare_query (PARSER_CONTEXT * parser,
					PT_NODE * query);

/*
 * mq_is_outer_join_spec () - determine if a spec is outer joined in a spec list
 *  returns: boolean
 *   parser(in): parser context
 *   spec(in): table spec to check
 */
bool
mq_is_outer_join_spec (PARSER_CONTEXT * parser, PT_NODE * spec)
{
  if (spec == NULL)
    {
      /* should not be here */
      PT_INTERNAL_ERROR (parser, "function called with wrong arguments");
      return false;
    }

  assert (spec->node_type == PT_SPEC);

  if (spec->info.spec.join_type == PT_JOIN_LEFT_OUTER)
    {
      /* directly on the right side of a left outer join */
      return true;
    }

  spec = spec->next;
  while (spec)
    {
      switch (spec->info.spec.join_type)
	{
	case PT_JOIN_NONE:
	case PT_JOIN_CROSS:
	  /* joins from this point forward do not matter */
	  return false;

	case PT_JOIN_RIGHT_OUTER:
	  /* right outer joined */
	  return true;

#if 1				/* TODO - */
	case PT_JOIN_NATURAL:	/* not used */
	case PT_JOIN_INNER:
	case PT_JOIN_LEFT_OUTER:
	case PT_JOIN_FULL_OUTER:	/* not used */
	case PT_JOIN_UNION:	/* not used */
	  break;
#endif
	}

      spec = spec->next;
    }

  /* if we reached this point, it's not outer joined */
  return false;
}


/*
 * mq_bump_corr_pre() -  Bump the correlation level of all matching
 *                       correlated queries
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_bump_corr_pre (PARSER_CONTEXT * parser, PT_NODE * node, void *void_arg,
		  int *continue_walk)
{
  MQ_BUMP_CORR_INFO *info = (MQ_BUMP_CORR_INFO *) void_arg;

  *continue_walk = PT_CONTINUE_WALK;

  if (!PT_IS_QUERY_NODE_TYPE (node->node_type))
    return node;

  /* Can not increment threshold for list portion of walk.
   * Since those queries are not sub-queries of this query.
   * Consequently, we recurse separately for the list leading
   * from a query.
   */
  if (node->next)
    {
      node->next = mq_bump_correlation_level (parser, node->next,
					      info->increment,
					      info->match_level);
    }

  *continue_walk = PT_LEAF_WALK;

  if (node->info.query.correlation_level != 0)
    {
      if (node->info.query.correlation_level >= info->match_level)
	{
	  node->info.query.correlation_level += info->increment;
	}
    }
  else
    {
      /* if the correlation level is 0, there cannot be correlated
       * subqueries crossing this level
       */
      *continue_walk = PT_STOP_WALK;
    }

  /* increment threshold as we dive into selects and unions */
  info->match_level++;

  return node;

}				/* mq_bump_corr_pre */


/*
 * mq_bump_corr_post() - Unwind the info stack
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_bump_corr_post (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		   void *void_arg, UNUSED_ARG int *continue_walk)
{
  MQ_BUMP_CORR_INFO *info = (MQ_BUMP_CORR_INFO *) void_arg;

  if (!PT_IS_QUERY_NODE_TYPE (node->node_type))
    {
      return node;
    }

  info->match_level--;

  return node;

}				/* mq_bump_corr_post */


/*
 * mq_bump_correlation_level() - Bump the correlation level of all matching
 *                               correlated queries
 *   return:
 *   parser(in):
 *   node(in):
 *   increment(in):
 *   match(in):
 */
PT_NODE *
mq_bump_correlation_level (PARSER_CONTEXT * parser, PT_NODE * node,
			   int increment, int match)
{
  MQ_BUMP_CORR_INFO info;
  info.match_level = match;
  info.increment = increment;

  return parser_walk_tree (parser, node,
			   mq_bump_corr_pre, &info, mq_bump_corr_post, &info);
}

/*
 * mq_union_bump_correlation() - Union left and right sides,
 *                               bumping correlation numbers
 *   return:
 *   parser(in):
 *   left(in):
 *   right(in):
 */
static PT_NODE *
mq_union_bump_correlation (PARSER_CONTEXT * parser, PT_NODE * left,
			   PT_NODE * right)
{
  if (left->info.query.correlation_level)
    {
      left = mq_bump_correlation_level (parser, left, 1,
					left->info.query.correlation_level);
    }

  if (right->info.query.correlation_level)
    {
      right = mq_bump_correlation_level (parser, right, 1,
					 right->info.query.correlation_level);
    }

  return pt_union (parser, left, right);
}

/*
 * mq_compute_authorization() -
 *   return: authorization in terms of the what_for mask
 *   class_object(in):
 */
static DB_AUTH
mq_compute_authorization (DB_OBJECT * class_object)
{
  DB_AUTH auth = (DB_AUTH) 0;

  if (db_check_authorization (class_object, DB_AUTH_SELECT) == NO_ERROR)
    {
      auth = (DB_AUTH) (auth + DB_AUTH_SELECT);
    }
  if (db_check_authorization (class_object, DB_AUTH_INSERT) == NO_ERROR)
    {
      auth = (DB_AUTH) (auth + DB_AUTH_INSERT);
    }
  if (db_check_authorization (class_object, DB_AUTH_UPDATE) == NO_ERROR)
    {
      auth = (DB_AUTH) (auth + DB_AUTH_UPDATE);
    }
  if (db_check_authorization (class_object, DB_AUTH_DELETE) == NO_ERROR)
    {
      auth = (DB_AUTH) (auth + DB_AUTH_DELETE);
    }

  return auth;
}

/*
 * mq_compute_query_authorization() -
 *   return: authorization intersection of a query
 *   statement(in):
 */
static DB_AUTH
mq_compute_query_authorization (PT_NODE * statement)
{
  PT_NODE *spec;
  PT_NODE *flat;
  DB_AUTH auth = (DB_AUTH) 0;

  switch (statement->node_type)
    {
    case PT_SELECT:
      spec = statement->info.query.q.select.from;

      if (spec == NULL || spec->next != NULL)
	{
	  auth = DB_AUTH_SELECT;
	}
      else
	{
	  auth = DB_AUTH_ALL;
	  /* select authorization is computed at semantic check */
	  /* its moot to compute other authorization on entire join, since
	   * its non-updateable
	   */
	  for (flat = spec->info.spec.flat_entity_list; flat != NULL;
	       flat = flat->next)
	    {
	      auth &= mq_compute_authorization (flat->info.name.db_object);
	    }
	}
      break;

    case PT_UNION:
      auth =
	mq_compute_query_authorization (statement->info.query.q.union_.arg1);
      auth = (DB_AUTH)
	(auth &
	 mq_compute_query_authorization (statement->info.query.q.union_.
					 arg2));
      break;

    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      /* select authorization is computed at semantic check */
      /* again moot to compute other authorization, since this is not
       * updatable.
       */
      auth = DB_AUTH_SELECT;
      break;

    default:			/* should not get here, that is an error! */
#if defined(RYE_DEBUG)
      fprintf (stdout, "Illegal parse node type %d, in %s, at line %d. \n",
	       statement->node_type, __FILE__, __LINE__);
#endif /* RYE_DEBUG */
      break;
    }

  return auth;
}


/*
 * mq_set_union_query() - Mark top level selects as PT_IS__UNION_QUERY
 *   return: B_AUTH authorization
 *   parser(in):
 *   statement(in):
 *   is_union(in):
 */
static void
mq_set_union_query (PARSER_CONTEXT * parser, PT_NODE * statement,
		    PT_MISC_TYPE is_union)
{
  if (statement)
    switch (statement->node_type)
      {
      case PT_SELECT:
	statement->info.query.is_subquery = is_union;
	break;

      case PT_UNION:
      case PT_DIFFERENCE:
      case PT_INTERSECTION:
	statement->info.query.is_subquery = is_union;
	mq_set_union_query (parser, statement->info.query.q.union_.arg1,
			    is_union);
	mq_set_union_query (parser, statement->info.query.q.union_.arg2,
			    is_union);
	break;

      default:
	/* should not get here, that is an error! */
	/* its almost certainly recoverable, so ignore it */
	assert (0);
	break;
      }
}


/*
 * mq_rewrite_agg_names() - re-sets PT_NAME node ids for conversion of
 *     aggregate selects. It also coerces path expressions into names, and
 *     pushes subqueries and SET() functions down into the derived table.
 *     It places each name on the referenced attrs list.
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in/out):
 *   continue_walk(in/out):
 */
static PT_NODE *
mq_rewrite_agg_names (PARSER_CONTEXT * parser, PT_NODE * node,
		      void *void_arg, int *continue_walk)
{
  PT_AGG_REWRITE_INFO *info = (PT_AGG_REWRITE_INFO *) void_arg;
  PT_AGG_NAME_INFO name_info;
  PT_NODE *old_from = info->from;
  PT_NODE *new_from = info->new_from;
  PT_NODE *derived_select = info->derived_select;
  PT_NODE *node_next;
  PT_TYPE_ENUM type;
  PT_NODE *data_type;
  PT_NODE *temp;
  PT_NODE *arg2;
  PT_NODE *temparg2;
  int i = 0;
  int line_no, col_no, is_hidden_column;
  bool agg_found;

  *continue_walk = PT_CONTINUE_WALK;

  switch (node->node_type)
    {
    case PT_DOT_:
      if ((arg2 = node->info.dot.arg2)
	  && pt_find_entity (parser, old_from,
			     node->info.dot.arg2->info.name.spec_id))
	{
	  /* we should put this in the referenced name list, ie
	   * the select list of the derived select statement (if not
	   * already there) then change this node to a generated name.
	   */
	  node_next = node->next;
	  type = node->type_enum;
	  data_type = parser_copy_tree_list (parser, node->data_type);

	  node->next = NULL;
	  temp = derived_select->info.query.q.select.list;
	  i = 0;
	  while (temp)
	    {
	      if (temp->node_type == PT_DOT_
		  && (temparg2 = temp->info.dot.arg2)
		  && pt_name_equal (parser, temparg2, arg2))
		{
		  break;
		}
	      temp = temp->next;
	      i++;
	    }
	  line_no = node->line_number;
	  col_no = node->column_number;
	  is_hidden_column = node->is_hidden_column;
	  if (!temp)
	    {
	      /* This was not found. add it */
	      derived_select->info.query.q.select.list =
		parser_append_node (node,
				    derived_select->info.query.q.select.list);
	    }
	  else
	    {
	      parser_free_tree (parser, node);
	    }
	  node = pt_name (parser, mq_generate_name (parser, "a", &i));
	  node->info.name.meta_class = PT_NORMAL;
	  node->info.name.spec_id = new_from->info.spec.id;
	  node->next = node_next;
	  node->type_enum = type;
	  node->data_type = data_type;
	  node->line_number = line_no;
	  node->column_number = col_no;
	  node->is_hidden_column = is_hidden_column;

	  mq_insert_symbol (parser, &new_from->info.spec.as_attr_list, node);
	}
      break;

    case PT_NAME:
      /* is the name an attribute name ? */
      if ((node->info.name.meta_class == PT_NORMAL
	   || node->info.name.meta_class == PT_OID_ATTR)
	  && pt_find_entity (parser, old_from, node->info.name.spec_id))
	{
	  /* we should put this in the referenced name list, ie
	   * the select list of the derived select statement (if not
	   * already there) then change this node to a generated name.
	   */
	  node_next = node->next;
	  type = node->type_enum;
	  data_type = parser_copy_tree_list (parser, node->data_type);

	  node->next = NULL;
	  temp = derived_select->info.query.q.select.list;
	  i = 0;
	  while (temp)
	    {
	      if (temp->node_type == PT_NAME
		  && pt_name_equal (parser, temp, node))
		{
		  break;
		}
	      temp = temp->next;
	      i++;
	    }
	  line_no = node->line_number;
	  col_no = node->column_number;
	  is_hidden_column = node->is_hidden_column;
	  if (!temp)
	    {
	      derived_select->info.query.q.select.list =
		parser_append_node (node,
				    derived_select->info.query.q.select.list);
	    }
	  else
	    {
	      parser_free_tree (parser, node);
	    }

	  node = pt_name (parser, mq_generate_name (parser, "a", &i));
	  node->info.name.meta_class = PT_NORMAL;
	  node->info.name.spec_id = new_from->info.spec.id;
	  node->next = node_next;
	  node->type_enum = type;
	  node->data_type = data_type;
	  node->line_number = line_no;
	  node->column_number = col_no;
	  node->is_hidden_column = is_hidden_column;

	  mq_insert_symbol (parser, &new_from->info.spec.as_attr_list, node);

#if 0
	push_complete:
#endif

	  /* once we push it, we don't need to dive in */
	  *continue_walk = PT_LIST_WALK;
	}
      break;

    case PT_FUNCTION:
      /* We need to push the set functions down with their subqueries.
         init. info->from is already set at mq_rewrite_aggregate_as_derived */
      agg_found = false;

      /* init name finding structure */
      name_info.max_level = -1;
      name_info.name_count = 0;
      name_info.select_stack = info->select_stack;

      if (node && PT_IS_COLLECTION_TYPE (node->type_enum))
	{
	  if (!pt_is_query (node->info.function.arg_list))
	    {
	      (void) parser_walk_tree (parser, node, pt_find_aggregate_names,
				       &name_info, pt_continue_walk, NULL);

	      agg_found =
		(name_info.max_level == 0 || name_info.name_count == 0);
	    }
	}
      else if (pt_is_aggregate_function (parser, node))
	{
	  if (node->info.function.function_type == PT_COUNT_STAR
	      || node->info.function.function_type == PT_GROUPBY_NUM)
	    {
	      /* found count(*), groupby_num() */
	      agg_found = (info->depth == 0);
	    }
	  else
	    {
	      /* check for aggregation function
	       * for example: SELECT (SELECT max(x.i) FROM y ...) ... FROM x
	       */
	      (void) parser_walk_tree (parser, node->info.function.arg_list,
				       pt_find_aggregate_names,
				       &name_info, pt_continue_walk, NULL);
	      agg_found =
		(name_info.max_level == 0 || name_info.name_count == 0);
	    }
	}

      /* rewrite if necessary */
      if (agg_found)
	{
	  node_next = node->next;
	  type = node->type_enum;
	  data_type = parser_copy_tree_list (parser, node->data_type);

	  node->next = NULL;
	  for (i = 0, temp = derived_select->info.query.q.select.list;
	       temp; temp = temp->next, i++)
	    ;			/* empty */

	  derived_select->info.query.q.select.list =
	    parser_append_node (node,
				derived_select->info.query.q.select.list);
	  line_no = node->line_number;
	  col_no = node->column_number;
	  is_hidden_column = node->is_hidden_column;
	  node = pt_name (parser, mq_generate_name (parser, "a", &i));
	  node->info.name.meta_class = PT_NORMAL;
	  node->info.name.spec_id = new_from->info.spec.id;
	  node->next = node_next;
	  node->type_enum = type;
	  node->data_type = data_type;
	  node->line_number = line_no;
	  node->column_number = col_no;
	  node->is_hidden_column = is_hidden_column;

	  mq_insert_symbol (parser, &new_from->info.spec.as_attr_list, node);

	  /* once we push it, we don't need to dive in */
	  *continue_walk = PT_LIST_WALK;
	}
      break;
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
    case PT_SELECT:
      /* Can not increment level for list portion of walk.
       * Since those queries are not sub-queries of this query.
       * Consequently, we recurse separately for the list leading
       * from a query.  Can't just call pt_to_uncorr_subquery_list()
       * directly since it needs to do a leaf walk and we want to do a full
       * walk on the next list.
       */
      if (node->next)
	{
	  node->next = parser_walk_tree (parser, node->next,
					 mq_rewrite_agg_names, info,
					 mq_rewrite_agg_names_post, info);
	}

      if (node->info.query.correlation_level == 0)
	{
	  /* no need to dive into the uncorrelated subquery */
	  *continue_walk = PT_STOP_WALK;
	}
      else
	{
	  *continue_walk = PT_LEAF_WALK;
	}

      /* push SELECT on stack */
      if (node->node_type == PT_SELECT)
	{
	  info->select_stack =
	    pt_pointer_stack_push (parser, info->select_stack, node);
	}

      info->depth++;		/* increase query depth as we dive into subqueries */
      break;

    default:
      break;
    }

  return node;
}

/*
 * mq_rewrite_agg_names_post() -
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in/out):
 *   continue_walk(in):
 */
static PT_NODE *
mq_rewrite_agg_names_post (PARSER_CONTEXT * parser,
			   PT_NODE * node, void *void_arg, int *continue_walk)
{
  PT_AGG_REWRITE_INFO *info = (PT_AGG_REWRITE_INFO *) void_arg;

  *continue_walk = PT_CONTINUE_WALK;

  switch (node->node_type)
    {
    case PT_SELECT:
      info->select_stack =
	pt_pointer_stack_pop (parser, info->select_stack, NULL);
      /* fall trough */

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      info->depth--;		/* decrease query depth */
      break;

    default:
      break;
    }

  return node;
}

/*
 * mq_substitute_select_in_statement() - takes a subquery expansion of a class_,
 *      in the form of a select, and a parse tree containing references to
 *      the class and its attributes, and substitutes matching select
 *      expressions for each attribute, and matching referenced classes
 *      for each class
 *   return: PT_NODE *, parse tree with local db table/class queries
 * 	    expanded to local db expressions
 *   parser(in): parser context
 *   statement(in/out): statement into which class will be expanded
 *   query_spec(in): query of class that will be expanded
 *   class(in): class name of class that will be expanded
 */
static PT_NODE *
mq_substitute_select_in_statement (PARSER_CONTEXT * parser,
				   PT_NODE * statement,
				   PT_NODE * query_spec, PT_NODE * class_)
{
  PT_NODE *query_spec_from, *query_spec_columns;
  PT_NODE *attributes = NULL;
  PT_NODE *attr;
  PT_NODE *col;

  /* Replace columns/attributes.
   * for each column/attribute name in table/class class,
   * replace with actual select column.
   */

  query_spec_columns = query_spec->info.query.q.select.list;
  query_spec_from = query_spec->info.query.q.select.from;
  if (query_spec_from == NULL)
    {
      PT_INTERNAL_ERROR (parser, "translate");
      return NULL;
    }

  /* get vclass spec attrs */
  attributes = mq_fetch_attributes (parser, class_);
  if (attributes == NULL)
    {
      return NULL;
    }

  col = query_spec_columns;
  attr = attributes;
  for (; col && attr; col = col->next, attr = attr->next)
    {
      /* set spec_id */
      attr->info.name.spec_id = class_->info.name.spec_id;
    }

  while (col)
    {
      if (col->is_hidden_column)
	{
	  col = col->next;
	  continue;
	}
      break;
    }

  if (col)
    {				/* error */
      PT_ERRORmf (parser, class_, MSGCAT_SET_PARSER_RUNTIME,
		  MSGCAT_RUNTIME_QSPEC_COLS_GT_ATTRS,
		  class_->info.name.original);
      statement = NULL;
    }
  if (attr)
    {				/* error */
      PT_ERRORmf (parser, class_, MSGCAT_SET_PARSER_RUNTIME,
		  MSGCAT_RUNTIME_ATTRS_GT_QSPEC_COLS,
		  class_->info.name.original);
      statement = NULL;
    }

  /* substitute attributes for query_spec_columns in statement */
  statement = mq_lambda (parser, statement, attributes, query_spec_columns);

  /* replace table */
  if (statement)
    {
      statement = mq_class_lambda (parser, statement,
				   class_, query_spec_from,
				   query_spec->info.query.q.select.where,
				   query_spec->info.query.q.select.group_by,
				   query_spec->info.query.q.select.having);
      if (PT_SELECT_INFO_IS_FLAGED (query_spec, PT_SELECT_INFO_HAS_AGG))
	{
	  /* mark as agg select */
	  if (statement && statement->node_type == PT_SELECT)
	    {
	      PT_SELECT_INFO_SET_FLAG (statement, PT_SELECT_INFO_HAS_AGG);
	    }
	}
    }

  parser_free_tree (parser, attributes);

  return statement;
}

/*
 * mq_is_pushable_subquery () - check if a subquery is pushable
 *  returns: true if pushable, false otherwise
 *   parser(in): parser context
 *   query(in): query to check
 *   is_only_spec(in): true if query is not joined in parent statement
 *
 * NOTE: a subquery is pushable if it's select list can be "pushed" up in it's
 *   parent query without altering the output. For example, the following:
 *       SELECT * FROM (SELECT * FROM t WHERE t.i > 2), u
 *   can be rewritten as
 *       SELECT * FROM t, u WHERE t.i > 2
 *   thus, "SELECT * FROM t WHERE t.i > 2" is called "pushable".
 *
 * NOTE: inst_num(), groupby_num() and rownum are only pushable if the
 *   query is not joined in the parent statement.
 *
 * NOTE: a query with joins is only pushable if it's not joined in the parent
 *   statement
 */
static bool
mq_is_pushable_subquery (PARSER_CONTEXT * parser, PT_NODE * query,
			 bool is_only_spec)
{
  CHECK_PUSHABLE_INFO cpi;

  /* check nulls */
  if (query == NULL)
    {
      PT_INTERNAL_ERROR (parser, "wrong arguments passed to function");
      return false;
    }

  assert (parser);
  assert (PT_IS_QUERY_NODE_TYPE (query->node_type));

  /* check for non-SELECTs */
  if (query->node_type != PT_SELECT)
    {
      /* not pushable */
      return false;
    }

  /* check for joins */
  if (!is_only_spec && query->info.query.q.select.from
      && query->info.query.q.select.from->next)
    {
      /* parent statement and subquery both have joins; not pushable */
      return false;
    }

  /* check for DISTINCT */
  if (pt_is_distinct (query))
    {
      /* not pushable */
      return false;
    }

  /* check for aggregate or orderby_for */
  if (pt_has_aggregate (parser, query) || query->info.query.orderby_for)
    {
      /* not pushable */
      return false;
    }

  /* check select list */
  cpi.check_query = false;	/* subqueries are pushable */
  cpi.check_xxxnum = !is_only_spec;

  cpi.query_found = false;
  cpi.xxxnum_found = false;

  parser_walk_tree (parser, query->info.query.q.select.list,
		    pt_check_pushable, (void *) &cpi, NULL, NULL);

  if (cpi.query_found || cpi.xxxnum_found)
    {
      /* query not pushable */
      return false;
    }

  /* check where clause */
  cpi.check_query = false;	/* subqueries are pushable */
  cpi.check_xxxnum = !is_only_spec;

  cpi.query_found = false;
  cpi.xxxnum_found = false;

  parser_walk_tree (parser, query->info.query.q.select.where,
		    pt_check_pushable, (void *) &cpi, NULL, NULL);

  if (cpi.query_found || cpi.xxxnum_found)
    {
      /* query not pushable */
      return false;
    }

  /* if we got this far, query is pushable */
  return true;
}

/*
 * mq_update_order_by() - update the position number of order by clause and
 * 			add hidden column(s) at the end of the output list if
 * 			necessary.
 *   return: PT_NODE *, parse tree with local db table/class queries expanded
 *    to local db expressions
 *   parser(in): parser context
 *   statement(in): statement into which class will be expanded
 *   query_spec(in): query of class that will be expanded
 *   class(in): class name of class that will be expanded
 *
 *   Note:
 *   It includes 3 steps to update the position number of order by clause.
 *   1) Get the attr info from the vclass;
 *   2) Find the corresponding attr by the position number of order by clause;
 *   3) Compare the corresponding attr with the attr of output list,
 *     a) if attr is in output list, update the position number of order by
 *     clause;
 *     b) if not, append a hidden column at the end of the output list, and
 *     update the position number of order by clause.
 *
 */
static PT_NODE *
mq_update_order_by (PARSER_CONTEXT * parser, PT_NODE * statement,
		    PT_NODE * query_spec, PT_NODE * class_)
{
  PT_NODE *order, *val;
  PT_NODE *attributes = NULL;
  PT_NODE *attr;
  PT_NODE *node, *result;
  PT_NODE *save_data_type;
  int attr_count;
  int i;

  assert (statement->node_type == PT_SELECT
	  && query_spec->info.query.order_by != NULL);

  statement->info.query.order_by =
    parser_append_node (parser_copy_tree_list
			(parser, query_spec->info.query.order_by),
			statement->info.query.order_by);


  /* 1 get vclass spec attrs */
  attributes = mq_fetch_attributes (parser, class_);
  if (attributes == NULL)
    {
      return NULL;
    }

  attr_count = 0;
  for (attr = attributes; attr != NULL; attr = attr->next)
    {
      /* set spec_id */
      attr->info.name.spec_id = class_->info.name.spec_id;
      attr_count++;
    }

  /* update the position number of order by clause */
  for (order = statement->info.query.order_by; order != NULL;
       order = order->next)
    {
      assert (order->node_type == PT_SORT_SPEC);

      val = order->info.sort_spec.expr;
      assert (val->node_type == PT_VALUE);

      if (attr_count < val->info.value.data_value.i)
	{
	  /* order by is a hidden column and not in attribute list, in this
	   * case, we need append a hidden column at the end of the output
	   * list.
	   */

	  /* 2 find from spec columns */
	  for (i = 1, attr = query_spec->info.query.q.select.list;
	       i < val->info.value.data_value.i; i++)
	    {
	      assert (attr != NULL);
	      attr = attr->next;
	    }

	  save_data_type = attr->data_type;
	  attr->data_type = NULL;

	  for (i = 1, node = statement->info.query.q.select.list;
	       node != NULL; node = node->next)
	    {
	      i++;
	    }
	  node = NULL;
	}
      else
	{
	  /* 2 find the corresponding attribute */
	  for (i = 1, attr = attributes; i < val->info.value.data_value.i;
	       i++)
	    {
	      assert (attr != NULL);
	      attr = attr->next;
	    }

	  assert (attr != NULL && attr->node_type == PT_NAME);
	  save_data_type = attr->data_type;
	  attr->data_type = NULL;

	  for (node = statement->info.query.q.select.list, i = 1;
	       node != NULL; node = node->next, i++)
	    {
	      /* 3 check whether attr is found in output list */
	      if (pt_name_equal (parser, node, attr))
		{
		  /* if yes, update position number of order by clause */
		  val->info.value.data_value.i = i;
		  val->info.value.db_value.data.i = i;
		  val->info.value.text = NULL;
		  order->info.sort_spec.pos_descr.pos_no = i;
		  break;
		}
	    }
	}

      /* if attr is not found in output list, append a hidden
       * column at the end of the output list.
       */
      if (node == NULL)
	{
	  result = parser_copy_tree (parser, attr);
	  if (result == NULL)
	    {
	      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
	      parser_free_tree (parser, attributes);
	      return NULL;
	    }
	  /* mark as a hidden column */
	  result->is_hidden_column = 1;
	  parser_append_node (result, statement->info.query.q.select.list);

	  /* update position number of order by clause */
	  val->info.value.data_value.i = i;
	  val->info.value.db_value.data.i = i;
	  val->info.value.text = NULL;
	  order->info.sort_spec.pos_descr.pos_no = i;
	}

      attr->data_type = save_data_type;
    }

  parser_free_tree (parser, attributes);

  return statement;
}

/*
 * mq_substitute_subquery_in_statement() - This takes a subquery expansion of
 *      a class_, in the form of a select, or union of selects,
 *      and a parse tree containing references to the class and its attributes,
 *      and substitutes matching select expressions for each attribute,
 *      and matching referenced classes for each class
 *   return: PT_NODE *, parse tree with local db table/class queries
 * 	     expanded to local db expressions
 *   parser(in):
 *   statement(in):
 *   query_spec(in):
 *   class(in):
 *   order_by(in):
 *   what_for(in):
 *
 * Note:
 * 1) Order-by is passed down into sub portions of the unions, intersections and
 *    differences.
 *    This gives better algorithmic order when order by is present, allowing
 *    sorting on smaller pieces, followed by linear merges.
 *
 * 2) All/distinct is NOT similarly passed down, since it is NOT a transitive
 *    operation with mixtures of union, intersection and difference.
 *    It may be true that if the top level guy is distinct, you will
 *    get the same results if all sub levels are also distinct.
 *    Anyway, it is safe not to do this, and may be not be safe to do.
 */
static PT_NODE *
mq_substitute_subquery_in_statement (PARSER_CONTEXT * parser,
				     PT_NODE * statement,
				     PT_NODE * query_spec,
				     PT_NODE * class_,
				     PT_NODE * order_by, int what_for)
{
  PT_NODE *tmp_result, *result, *arg1, *arg2, *statement_next;
  PT_NODE *class_spec, *statement_spec = NULL;
  PT_NODE *derived_table, *derived_spec, *derived_class;
  bool is_pushable_query, is_outer_joined;
  bool is_only_spec, is_upd_del_spec = false, rewrite_as_derived;

  result = tmp_result = NULL;	/* init */
  class_spec = NULL;
  rewrite_as_derived = false;

  statement_next = statement->next;
  switch (query_spec->node_type)
    {
    case PT_SELECT:
      /* make a local copy of the statement */
      tmp_result = parser_copy_tree (parser, statement);
      if (tmp_result == NULL)
	{
	  if (!pt_has_error (parser))
	    {
	      PT_INTERNAL_ERROR (parser, "failed to copy node tree");
	    }

	  goto exit_on_error;
	}

      /* get statement spec */
      switch (tmp_result->node_type)
	{
	case PT_SELECT:
	  statement_spec = tmp_result->info.query.q.select.from;
	  break;

	case PT_UPDATE:
	  statement_spec = tmp_result->info.update.spec;
	  break;

	case PT_DELETE:
	  statement_spec = tmp_result->info.delete_.spec;
	  break;

#if 0
	case PT_INSERT:
	  /* since INSERT can not have a spec list or statement conditions,
	     there is nothing to check */
	  statement_spec = tmp_result->info.insert.spec;
	  break;
#endif

	default:
	  /* should not get here */
	  assert (false);
	  PT_INTERNAL_ERROR (parser, "unknown node");
	  goto exit_on_error;
	}

      /* check found spec */
      class_spec = pt_find_spec (parser, statement_spec, class_);
      if (class_spec == NULL)
	{
	  /* class_'s spec was not found in spec list */
	  PT_INTERNAL_ERROR (parser, "class spec not found");
	  goto exit_on_error;
	}
      else
	{
	  /* check for (non-pushable) spec set */
	  rewrite_as_derived =
	    (class_spec->info.spec.entity_name != NULL
	     && class_spec->info.spec.entity_name->node_type == PT_SPEC);
	}

      /* do not rewrite vclass_query as a derived table if spec belongs to an
       * insert statement.
       */
      if (tmp_result->node_type == PT_INSERT)
	{
	  rewrite_as_derived = false;
	}
      else
	{
	  /* determine if class_spec is the only spec in the statement */
	  is_only_spec = (statement_spec->next == NULL ? true : false);

	  /* determine if spec is used for update/delete */
	  is_upd_del_spec = class_spec->info.spec.flag
	    & (PT_SPEC_FLAG_UPDATE | PT_SPEC_FLAG_DELETE);

	  /* determine if spec is outer joined */
	  is_outer_joined = mq_is_outer_join_spec (parser, class_spec);

	  /* determine if vclass_query is pushable */
	  is_pushable_query = mq_is_pushable_subquery (parser, query_spec,
						       is_only_spec);

	  /* rewrite vclass_query as a derived table  if spec is outer joined
	   * or if query is not pushable
	   */
	  rewrite_as_derived =
	    rewrite_as_derived || !is_pushable_query || is_outer_joined;

	  if (PT_IS_QUERY (tmp_result))
	    {
	      rewrite_as_derived = rewrite_as_derived
		|| (tmp_result->info.query.all_distinct == PT_DISTINCT);
	    }
	}

      if (rewrite_as_derived)
	{
	  /* rewrite vclass query as a derived table */
	  PT_NODE *tmp_class = NULL;

	  /* rewrite vclass spec */
	  class_spec =
	    mq_rewrite_vclass_spec_as_derived (parser, tmp_result, class_spec,
					       query_spec);

	  /* get derived expending spec node */
	  if (!class_spec
	      || !(derived_table = class_spec->info.spec.derived_table)
	      || !(derived_spec = derived_table->info.query.q.select.from)
	      || !(derived_class = derived_spec->info.spec.flat_entity_list))
	    {
	      /* error */
	      goto exit_on_error;
	    }

	  tmp_class = parser_copy_tree (parser, class_);
	  if (tmp_class == NULL)
	    {
	      goto exit_on_error;
	    }
	  tmp_class->info.name.spec_id = derived_class->info.name.spec_id;

	  /* now, derived_table has been derived.  */
	  if (pt_has_aggregate (parser, query_spec))
	    {
	      /* simply move WHERE's aggregate terms to HAVING.
	       * in mq_class_lambda(), this HAVING will be merged with
	       * query_spec HAVING.
	       */
	      derived_table->info.query.q.select.having =
		derived_table->info.query.q.select.where;
	      derived_table->info.query.q.select.where = NULL;
	    }

	  /* merge HINT of vclass spec */
	  derived_table->info.query.q.select.hint = (PT_HINT_ENUM)
	    (derived_table->info.query.q.select.hint |
	     query_spec->info.query.q.select.hint);
#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
	  derived_table->info.query.q.select.ordered_hint =
	    parser_append_node (parser_copy_tree_list
				(parser,
				 query_spec->info.query.q.select.
				 ordered_hint),
				derived_table->info.query.q.select.
				ordered_hint);
#endif

	  derived_table->info.query.q.select.use_nl_hint =
	    parser_append_node (parser_copy_tree_list
				(parser,
				 query_spec->info.query.q.select.use_nl_hint),
				derived_table->info.query.q.select.
				use_nl_hint);

	  derived_table->info.query.q.select.use_idx_hint =
	    parser_append_node (parser_copy_tree_list
				(parser,
				 query_spec->info.query.q.select.
				 use_idx_hint),
				derived_table->info.query.q.select.
				use_idx_hint);

	  if (!order_by || query_spec->info.query.orderby_for)
	    {
	      if (query_spec->info.query.order_by)
		{
		  /* update the position number of order by clause */
		  derived_table =
		    mq_update_order_by (parser, derived_table, query_spec,
					tmp_class);
		  if (derived_table == NULL)
		    {
		      goto exit_on_error;
		    }
		  derived_table->info.query.order_siblings =
		    query_spec->info.query.order_siblings;
		}

	      if (query_spec->info.query.orderby_for)
		{
		  derived_table->info.query.orderby_for =
		    parser_append_node (parser_copy_tree_list
					(parser,
					 query_spec->info.query.orderby_for),
					derived_table->info.query.
					orderby_for);
		}
	    }

	  /* merge USING INDEX clause of vclass spec */
	  if (query_spec->info.query.q.select.using_index)
	    {
	      PT_NODE *ui;

	      ui =
		parser_copy_tree_list (parser,
				       query_spec->info.query.q.select.
				       using_index);
	      derived_table->info.query.q.select.using_index =
		parser_append_node (ui,
				    derived_table->info.query.q.select.
				    using_index);
	    }

	  class_spec->info.spec.derived_table =
	    mq_substitute_select_in_statement (parser,
					       class_spec->info.spec.
					       derived_table, query_spec,
					       tmp_class);

	  if ((tmp_result->node_type == PT_UPDATE
	       || tmp_result->node_type == PT_DELETE) && is_upd_del_spec)
	    {
#if 1
	      assert (false);	/* is not permit */
#endif

	      goto exit_on_error;
	    }

	  if (tmp_class)
	    {
	      parser_free_tree (parser, tmp_class);
	    }

	  derived_table = class_spec->info.spec.derived_table;
	  if (derived_table == NULL)
	    {			/* error */
	      goto exit_on_error;
	    }

	  if (PT_IS_QUERY (derived_table))
	    {
	      if (query_spec->info.query.all_distinct == PT_DISTINCT)
		{
		  derived_table->info.query.all_distinct = PT_DISTINCT;
		}
	    }

	  result = tmp_result;
	}
      else
	{
	  /* expand vclass_query in parent statement */
	  if (tmp_result->node_type == PT_SELECT)
	    {
	      /* merge HINT of vclass spec */
	      tmp_result->info.query.q.select.hint = (PT_HINT_ENUM)
		(tmp_result->info.query.q.select.hint |
		 query_spec->info.query.q.select.hint);

#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
	      tmp_result->info.query.q.select.ordered_hint =
		parser_append_node (parser_copy_tree_list
				    (parser,
				     query_spec->info.query.q.select.
				     ordered_hint),
				    tmp_result->info.query.q.select.
				    ordered_hint);
#endif

	      tmp_result->info.query.q.select.use_nl_hint =
		parser_append_node (parser_copy_tree_list
				    (parser,
				     query_spec->info.query.q.select.
				     use_nl_hint),
				    tmp_result->info.query.q.select.
				    use_nl_hint);

	      tmp_result->info.query.q.select.use_idx_hint =
		parser_append_node (parser_copy_tree_list
				    (parser,
				     query_spec->info.query.q.select.
				     use_idx_hint),
				    tmp_result->info.query.q.select.
				    use_idx_hint);

	      assert (query_spec->info.query.orderby_for == NULL);
	      if (!order_by && query_spec->info.query.order_by)
		{
		  /* update the position number of order by clause and add a
		   * hidden column into the output list if necessary.
		   */
		  tmp_result =
		    mq_update_order_by (parser, tmp_result, query_spec,
					class_);
		  if (tmp_result == NULL)
		    {
		      goto exit_on_error;
		    }
		}
	    }

	  /* merge USING INDEX clause of vclass spec */
	  if (query_spec->info.query.q.select.using_index)
	    {
	      PT_NODE *ui;

	      ui = parser_copy_tree_list (parser,
					  query_spec->info.query.q.select.
					  using_index);
	      if (tmp_result->node_type == PT_SELECT)
		{
		  tmp_result->info.query.q.select.using_index =
		    parser_append_node (ui,
					tmp_result->info.query.q.select.
					using_index);
		}
	      else if (tmp_result->node_type == PT_UPDATE)
		{
		  tmp_result->info.update.using_index =
		    parser_append_node (ui,
					tmp_result->info.update.using_index);
		}
	      else if (tmp_result->node_type == PT_DELETE)
		{
		  tmp_result->info.delete_.using_index =
		    parser_append_node (ui,
					tmp_result->info.delete_.using_index);
		}
	    }

	  result = mq_substitute_select_in_statement (parser, tmp_result,
						      query_spec, class_);
	}

      /* set query id # */
      if (result)
	{
	  if (PT_IS_QUERY (result))
	    {
	      result->info.query.id = (UINTPTR) result;
	    }
	}
      else
	{
	  goto exit_on_error;
	}

      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      if (pt_has_aggregate (parser, statement))
	{
	  /* this error will not occur now unless there is a system error.
	   * The above condition will cause the query to be rewritten earlier.
	   */
	  PT_ERRORm (parser, statement, MSGCAT_SET_PARSER_RUNTIME,
		     MSGCAT_RUNTIME_REL_RESTRICTS_AGG_2);
	  result = NULL;
	}
      else
	{
	  PT_NODE *inside_order_by = NULL;
	  assert (statement->node_type == PT_SELECT && order_by == NULL);

	  arg1 = query_spec->info.query.q.union_.arg1;
	  arg2 = query_spec->info.query.q.union_.arg2;

	  if (query_spec->info.query.order_by != NULL)
	    {
	      /* update the position number of order by clause */
	      statement = mq_update_order_by (parser, statement, query_spec,
					      class_);
	      if (statement == NULL)
		{
		  goto exit_on_error;
		}
	      inside_order_by = statement->info.query.order_by;
	      statement->info.query.order_by = NULL;
	    }

	  arg1 = mq_substitute_subquery_in_statement (parser, statement, arg1,
						      class_, order_by,
						      what_for);
	  arg2 = mq_substitute_subquery_in_statement (parser, statement, arg2,
						      class_, order_by,
						      what_for);

	  if (arg1 && arg2)
	    {
	      result = mq_union_bump_correlation (parser, arg1, arg2);
	      /* reset node_type in case it was difference or intersection */
	      if (result)
		{
		  result->node_type = query_spec->node_type;
		}
	    }
	  else
	    {
	      if (query_spec->node_type == PT_INTERSECTION)
		{
		  result = NULL;
		}
	      else if (query_spec->node_type == PT_DIFFERENCE)
		{
		  result = arg1;
		}
	      else
		{
		  if (arg1)
		    {
		      result = arg1;
		    }
		  else
		    {
		      result = arg2;
		    }
		}
	    }

	  if (result != NULL)
	    {
	      if (query_spec->info.query.order_by != NULL)
		{
		  result->info.query.order_by = inside_order_by;
		  result->info.query.order_siblings =
		    query_spec->info.query.order_siblings;
		}

	      if (query_spec->info.query.orderby_for != NULL)
		{
		  result->info.query.orderby_for =
		    parser_append_node (parser_copy_tree_list (parser,
							       query_spec->
							       info.query.
							       orderby_for),
					result->info.query.orderby_for);
		}
	    }
	}			/* else */
      break;

    default:
      /* should not get here, that is an error! */
      assert (0);
      break;
    }

  if (result && PT_IS_QUERY (result))
    {
      if (query_spec->info.query.all_distinct == PT_DISTINCT)
	{
	  if (rewrite_as_derived == true)
	    {
	      /* result has been substituted. skip and go ahead        */
	    }
	  else
	    {
	      result->info.query.all_distinct = PT_DISTINCT;
	    }
	}
    }

  if (result)
    {
      result->next = statement_next;
    }

  return result;

exit_on_error:

  if (tmp_result)
    {
      parser_free_tree (parser, tmp_result);
    }

  return NULL;
}

/*
 * mq_translatable_class() -
 *   return: 1 on translatable
 *   parser(in):
 *   class(in):
 */
static int
mq_translatable_class (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * class_)
{
  /* vclasses, aka views, are otherwise translatable */
  if (db_is_vclass (class_->info.name.db_object))
    {
      return 1;
    }

  return 0;
}

/*
 * mq_is_union_translation() - tests a spec for a union translation
 *   return:
 *   parser(in):
 *   spec(in):
 */
static bool
mq_is_union_translation (PARSER_CONTEXT * parser, PT_NODE * spec)
{
  PT_NODE *entity;
  PT_NODE *subquery = NULL;
  int had_some_real_classes = 0;
  int had_some_virtual_classes = 0;

  if (spec == NULL)
    {
      return false;
    }
  else if (spec->info.spec.derived_table)
    {
      return false;
    }

  for (entity = spec->info.spec.flat_entity_list; entity != NULL;
       entity = entity->next)
    {
      if (!mq_translatable_class (parser, entity))
	{
	  /* no translation for above cases */
	  had_some_real_classes++;
	}
      else
	{
	  had_some_virtual_classes++;

	  assert (subquery == NULL);
	  subquery = mq_fetch_subqueries (parser, entity);
	  if (subquery && subquery->node_type != PT_SELECT)
	    {
	      parser_free_tree (parser, subquery);
	      return true;
	    }
	  parser_free_tree (parser, subquery);
	  subquery = NULL;
	}
    }

  if (had_some_virtual_classes > 1)
    {
      return true;
    }
  if (had_some_virtual_classes && had_some_real_classes)
    {
      return true;
    }

  return false;
}


/*
 * mq_translate_tree() - translates a tree against a list of classes
 *   return: PT_NODE *, parse tree with view and virtual class queries expanded
 *          to leaf classes or local db tables/classes
 *   parser(in):
 *   tree(in/out):
 *   spec_list(in):
 *   order_by(in):
 *   what_for(in):
 */
static PT_NODE *
mq_translate_tree (PARSER_CONTEXT * parser, PT_NODE * tree,
		   PT_NODE * spec_list, PT_NODE * order_by, int what_for)
{
  PT_NODE *entity;
  PT_NODE *class_spec;
  PT_NODE *tree_union;
  PT_NODE *my_class;
  PT_NODE *pt_tmp;
  PT_NODE *real_classes;
  PT_NODE *real_flat_classes;
  PT_NODE *real_part;
  PT_NODE *substituted;
  PT_NODE *my_spec;
  int had_some_virtual_classes;
  int delete_old_node = false;

  /* for each table/class in class list,
   * do leaf expansion or vclass/view expansion.
   */

  pt_tmp = tree;
  for (class_spec = spec_list; class_spec != NULL;
       class_spec = class_spec->next)
    {
      /* need to loop through entity specs!
       * Currently, theres no way to represent the all correct results
       * in a parse tree or in xasl.
       */
      bool skip_auth_check = false;
      int update_flag = class_spec->info.spec.flag & PT_SPEC_FLAG_UPDATE;
      int delete_flag = class_spec->info.spec.flag & PT_SPEC_FLAG_DELETE;
      bool fetch_for_update;

      if ((what_for == DB_AUTH_SELECT)
	  || (what_for == DB_AUTH_UPDATE && !update_flag)
	  || (what_for == DB_AUTH_DELETE && !delete_flag))
	{
	  /* used either in a query or an UPDATE/DELETE that does not alter the
	     subquery */
	  fetch_for_update = false;
	}
      else
	{
	  fetch_for_update = true;
	}

      had_some_virtual_classes = 0;
      real_classes = NULL;
      tree_union = NULL;

      if (((int) class_spec->info.spec.auth_bypass_mask & what_for) ==
	  what_for)
	{
	  assert (what_for != DB_AUTH_NONE);
	  skip_auth_check = true;
	}

      if (class_spec->info.spec.derived_table)
	{
	  /* no translation per se, but need to fix up proxy objects */
	  tree = mq_fix_derived_in_union (parser, tree,
					  class_spec->info.spec.id);
	}

      assert (class_spec->info.spec.flat_entity_list == NULL
	      || class_spec->info.spec.flat_entity_list->next == NULL);
      for (entity = class_spec->info.spec.flat_entity_list;
	   entity != NULL; entity = entity->next)
	{
	  if (mq_translatable_class (parser, entity) == 0)
	    {
	      /* no translation for above cases */
	      my_class = parser_copy_tree (parser, entity);
	      if (!my_class)
		{
		  return NULL;
		}

	      /* check for authorization bypass: this feature should be
	       * used only for specs in SHOW statements;
	       * Note : all classes expanded under the current spec
	       * sub-tree will be skipped by the authorization process
	       */
	      if (!skip_auth_check &&
		  (db_check_authorization (my_class->info.name.db_object,
					   (DB_AUTH) what_for) != NO_ERROR))
		{
		  PT_ERRORmf2 (parser, entity, MSGCAT_SET_PARSER_RUNTIME,
			       MSGCAT_RUNTIME_IS_NOT_AUTHORIZED_ON,
			       get_authorization_name (what_for),
			       sm_class_name (my_class->info.name.db_object));
		  return NULL;
		}
	      my_class->next = real_classes;
	      real_classes = my_class;
	    }
	  else
	    {
	      PT_NODE *subquery = NULL;

	      had_some_virtual_classes = 1;

	      if (pt_has_error (parser))
		{
		  return NULL;
		}

	      if (!fetch_for_update)
		{
		  subquery = mq_fetch_subqueries (parser, entity);
		}
	      else
		{
		  subquery = mq_fetch_subqueries_for_update (parser,
							     entity,
							     (DB_AUTH)
							     what_for);
		  assert (subquery == NULL);
		}

	      if (subquery != NULL)
		{
		  assert (what_for != DB_AUTH_INSERT);
		  assert (subquery->next == NULL);	/* is impossible */

		  if (tree == NULL || pt_has_error (parser))
		    {
		      /* an error was discovered parsing the sub query. */
		      return NULL;
		    }

#if defined(RYE_DEBUG)
		  fprintf (stdout, "\n<subqueries of %s are>\n  %s\n",
			   entity->info.name.original,
			   parser_print_tree_list (parser, subquery));
#endif /* RYE_DEBUG */
		  substituted = mq_substitute_subquery_in_statement
		    (parser, tree, subquery, entity, order_by, what_for);
#ifdef RYE_DEBUG
		  fprintf (stdout,
			   "\n<substituted %s with subqueries is>\n  %s\n",
			   entity->info.name.original,
			   parser_print_tree_list (parser, substituted));
#endif /* RYE_DEBUG */

		  if (substituted != NULL)
		    {
		      if (tree_union != NULL)
			{
			  if (what_for == DB_AUTH_SELECT)
			    {
			      tree_union = mq_union_bump_correlation
				(parser, tree_union, substituted);
			      if (tree_union && order_by)
				{
				  tree_union->info.query.order_by =
				    parser_copy_tree_list (parser, order_by);
				}
			    }
			  else
			    {
			      parser_append_node (substituted, tree_union);
			    }
			}
		      else
			{
			  tree_union = substituted;
			}
		    }

		  parser_free_tree (parser, subquery);
		}
	      else
		{
		  if (er_has_error () || pt_has_error (parser))
		    {
		      return NULL;
		    }

		  /* a virtual class with no subquery */
		}
	    }
	}

      if (had_some_virtual_classes)
	{
	  delete_old_node = true;
	  /* at least some of the classes were virtual
	     were any "real" classes members of the class spec? */
	  real_part = NULL;
	  if (real_classes != NULL)
	    {
	      real_flat_classes =
		parser_copy_tree_list (parser, real_classes);

	      for (entity = real_classes; entity != NULL;
		   entity = entity->next)
		{
		  /* finish building new entity spec */
		  entity->info.name.resolved = NULL;
		}

	      my_spec = pt_entity (parser, real_classes,
				   parser_copy_tree (parser,
						     class_spec->info.spec.
						     range_var),
				   real_flat_classes);
	      my_spec->info.spec.id = class_spec->info.spec.id;

	      real_part =
		mq_class_lambda (parser, parser_copy_tree (parser, tree),
				 real_flat_classes, my_spec, NULL, NULL,
				 NULL);
	    }

	  /* if the class spec had mixed real and virtual parts,
	     recombine them. */
	  if (real_part != NULL)
	    {
	      if (tree_union != NULL)
		{
		  tree = mq_union_bump_correlation (parser,
						    real_part, tree_union);
		}
	      else
		{
		  /* there were some vclasses, but all have vacuous
		     query specs. */
		  tree = real_part;
		}
	    }
	  else if (tree_union != NULL)
	    {
	      tree = tree_union;
	    }
	  else
	    {
	      if (tree
		  && tree->node_type != PT_SELECT
		  && tree->node_type != PT_UNION
		  && tree->node_type != PT_DIFFERENCE
		  && tree->node_type != PT_INTERSECTION)
		{
		  tree = NULL;
		}
	    }
	}
      else
	{
	  /* Getting here means there were NO vclasses.
	   *    all classes involved are "real" classes,
	   *    so don't rewrite this tree.
	   */
	}
    }

/*
 *  We need to free pt_tmp at this point if the original tree pointer has
 *  been reassgned.  We can't simply parser_free_tree() the node since the new tree
 *  may still have pointers to the lower nodes in the tree.  So, we set
 *  the NEXT pointer to NULL and then free it so the new tree is not
 *  corrupted.
 */
  if (delete_old_node && (tree != pt_tmp))
    {
      PT_NODE_COPY_NUMBER_OUTERLINK (tree, pt_tmp);
      pt_tmp->next = NULL;
      parser_free_tree (parser, pt_tmp);
    }

  tree = mq_reset_ids_in_statement (parser, tree);
  return tree;
}

/*
 * mq_corr_subq_pre() - Checks for subqueries
 *                                 which are correlated level 1
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in/out):
 */
static PT_NODE *
mq_corr_subq_pre (PARSER_CONTEXT * parser,
		  PT_NODE * node, void *void_arg, int *continue_walk)
{
  bool *found = (bool *) void_arg;

  if (!node || *found)
    return node;

  *continue_walk = PT_CONTINUE_WALK;

  if (pt_is_query (node))
    {
      /* don't dive into subqueries */
      *continue_walk = PT_LIST_WALK;

      /* found correlated subquery */
      if (node->info.query.correlation_level == 1)
	{
	  *found = true;
	}
    }
  else if (pt_is_aggregate_function (parser, node))
    {
      /* don't dive into aggreate functions */
      *continue_walk = PT_LIST_WALK;
    }

  if (*found)
    {
      /* don't walk */
      *continue_walk = PT_STOP_WALK;
    }

  return node;

}				/* mq_corr_subq_pre */


/*
 * mq_has_corr_subqueries() - checks subqueries which are correlated level 1
 *   return: true on checked
 *   parser(in):
 *   node(in):
 *
 * Description:
 *      Returns true if the query contains
 *      subqueries which are correlated level 1.
 */
static bool
mq_has_corr_subqueries (PARSER_CONTEXT * parser, PT_NODE * node)
{
  bool found = false;

  (void) parser_walk_tree (parser, node->info.query.q.select.list,
			   mq_corr_subq_pre, &found, NULL, NULL);

  if (!found)
    {
      (void) parser_walk_tree (parser, node->info.query.q.select.having,
			       mq_corr_subq_pre, &found, NULL, NULL);
    }

  return found;

}


/*
 * pt_check_pushable() - check for pushable
 *   return:
 *   parser(in):
 *   tree(in):
 *   arg(in/out):
 *   continue_walk(in):
 *
 * Note:
 *  subquery, rownum, inst_num(), orderby_num(), groupby_num()
 *  does not pushable if we find these in corresponding item
 *  in select_list of query
 */
static PT_NODE *
pt_check_pushable (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * tree,
		   void *arg, int *continue_walk)
{
  CHECK_PUSHABLE_INFO *cinfop = (CHECK_PUSHABLE_INFO *) arg;

  if (!tree || *continue_walk == PT_STOP_WALK)
    {
      return tree;
    }

  switch (tree->node_type)
    {
    case PT_SELECT:
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      if (cinfop->check_query)
	{
	  cinfop->query_found = true;	/* not pushable */
	}
      break;

    case PT_EXPR:
      if (tree->info.expr.op == PT_ROWNUM
	  || tree->info.expr.op == PT_INST_NUM
	  || tree->info.expr.op == PT_ORDERBY_NUM)
	{
	  if (cinfop->check_xxxnum)
	    {
	      cinfop->xxxnum_found = true;	/* not pushable */
	    }
	}
      break;

    case PT_FUNCTION:
      if (tree->info.function.function_type == PT_GROUPBY_NUM)
	{
	  if (cinfop->check_xxxnum)
	    {
	      cinfop->xxxnum_found = true;	/* not pushable */
	    }
	}
      break;

    default:
      break;
    }				/* switch (tree->node_type) */

  if (cinfop->query_found || cinfop->xxxnum_found)
    {
      /* not pushable */
      /* do not need to traverse anymore */
      *continue_walk = PT_STOP_WALK;
    }

  return tree;
}

/*
 * pt_pushable_query_in_pos() -
 *   return: true on pushable query
 *   parser(in):
 *   query(in):
 *   pos(in):
 */
static bool
pt_pushable_query_in_pos (PARSER_CONTEXT * parser, PT_NODE * query, int pos)
{
  bool pushable = false;	/* guess as not pushable */

  switch (query->node_type)
    {
    case PT_SELECT:
      {
	CHECK_PUSHABLE_INFO cinfo;
	PT_NODE *list;
	int i;

	/* Traverse select list */
	for (list = query->info.query.q.select.list, i = 0;
	     list; list = list->next, i++)
	  {
	    /* init */
	    cinfo.check_query = (i == pos) ? true : false;
	    cinfo.check_xxxnum = true;	/* always check */

	    cinfo.query_found = false;
	    cinfo.xxxnum_found = false;

	    switch (list->node_type)
	      {
	      case PT_SELECT:
	      case PT_UNION:
	      case PT_DIFFERENCE:
	      case PT_INTERSECTION:
		if (i == pos)
		  {
		    cinfo.query_found = true;	/* not pushable */
		  }
		break;

	      case PT_EXPR:
		/* always check for rownum, inst_num(), orderby_num() */
		if (list->info.expr.op == PT_ROWNUM
		    || list->info.expr.op == PT_INST_NUM
		    || list->info.expr.op == PT_ORDERBY_NUM)
		  {
		    cinfo.xxxnum_found = true;	/* not pushable */
		  }
		else
		  {		/* do traverse */
		    parser_walk_leaves (parser, list, pt_check_pushable,
					&cinfo, NULL, NULL);
		  }
		break;

	      case PT_FUNCTION:
		/* always check for groupby_num() */
		if (list->info.function.function_type == PT_GROUPBY_NUM)
		  {
		    cinfo.xxxnum_found = true;	/* not pushable */
		  }
		else
		  {		/* do traverse */
		    parser_walk_leaves (parser, list, pt_check_pushable,
					&cinfo, NULL, NULL);
		  }
		break;

	      default:		/* do traverse */
		parser_walk_leaves (parser, list, pt_check_pushable, &cinfo,
				    NULL, NULL);
		break;
	      }			/* switch (list->node_type) */

	    /* check for subquery, rownum, inst_num(),
	     * orderby_num(), groupby_num(): does not pushable if we
	     * find these in corresponding item in select_list of query
	     */
	    if (cinfo.query_found || cinfo.xxxnum_found)
	      {
		break;		/* not pushable */
	      }

	  }			/* for (list = ...) */

	if (list == NULL)
	  {			/* check all select list */
	    pushable = true;	/* OK */
	  }
      }
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      if (pt_pushable_query_in_pos (parser, query->info.query.q.union_.arg1,
				    pos)
	  && pt_pushable_query_in_pos (parser,
				       query->info.query.q.union_.arg2, pos))
	{
	  pushable = true;	/* OK */
	}
      break;

    default:			/* should not get here, that is an error! */
#if defined(RYE_DEBUG)
      fprintf (stdout, "Illegal parse node type %d, in %s, at line %d. \n",
	       query->node_type, __FILE__, __LINE__);
#endif /* RYE_DEBUG */
      break;
    }				/* switch (query->node_type) */

  return pushable;
}

/*
 * pt_find_only_name_id() - returns true if node name with the given
 *                          spec id is found
 *   return:
 *   parser(in):
 *   tree(in):
 *   arg(in/out):
 *   continue_walk(in):
 */
static PT_NODE *
pt_find_only_name_id (PARSER_CONTEXT * parser, PT_NODE * tree,
		      void *arg, int *continue_walk)
{
  FIND_ID_INFO *infop = (FIND_ID_INFO *) arg;
  PT_NODE *spec, *node = tree;

  /* do not need to traverse anymore */
  if (!tree || *continue_walk == PT_STOP_WALK)
    {
      return tree;
    }

  switch (node->node_type)
    {
    case PT_SELECT:
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      /* simply give up when we find query in predicate
       * refer QA fixed/fderiv3.sql:line165
       */
      infop->out.others_found = true;
      break;

    case PT_DOT_:
      /* only check left side of DOT expression, right side is of no interest */
      *continue_walk = PT_LIST_WALK;

      do
	{
	  node = node->info.dot.arg1;
	}
      while (node && node->node_type == PT_DOT_);

      if (node == NULL)
	{
	  /* nothing found ... */
	  break;
	}
      /* fall trough */

    case PT_NAME:
      spec = infop->in.spec;
      /* match specified spec */
      if (node->info.name.spec_id == spec->info.spec.id)
	{
	  infop->out.found = true;
	  /* check for subquery: does not pushable if we find
	   * subquery in corresponding item in select_list of query
	   */
	  if (infop->out.pushable)
	    {
	      PT_NODE *attr, *query;
	      UINTPTR save_spec_id;
	      int i;

	      for (attr = infop->in.attr_list, i = 0; attr;
		   attr = attr->next, i++)
		{

		  if (attr->node_type != PT_NAME)
		    {
		      attr = NULL;	/* unknown error */
		      break;
		    }

		  save_spec_id = attr->info.name.spec_id;	/* save */
		  attr->info.name.spec_id = node->info.name.spec_id;

		  /* found match in as_attr_list */
		  if (pt_name_equal (parser, node, attr))
		    {
		      /* check for each query */
		      for (query = infop->in.query_list;
			   query && infop->out.pushable; query = query->next)
			{
			  infop->out.pushable =
			    pt_pushable_query_in_pos (parser, query, i);
			}	/* for (query = ... ) */
		      break;
		    }

		  attr->info.name.spec_id = save_spec_id;	/* restore */
		}		/* for (attr = ... ) */

	      if (!attr)
		{
		  /* impossible case. simply give up */
		  infop->out.pushable = false;
		}
	    }
	}
      else
	{
	  /* check for other spec */
	  for (spec = infop->in.others_spec_list; spec; spec = spec->next)
	    {
	      if (node->info.name.spec_id == spec->info.spec.id)
		{
		  infop->out.others_found = true;
		  break;
		}
	    }

	  /* not found in other spec */
	  if (!spec)
	    {
	      /* is correlated other spec */
	      infop->out.correlated_found = true;
	    }
	}
      break;

    case PT_EXPR:
      /* simply give up when we find rownum, inst_num(), orderby_num()
       * in predicate
       */
      if (node->info.expr.op == PT_ROWNUM
	  || node->info.expr.op == PT_INST_NUM
	  || node->info.expr.op == PT_ORDERBY_NUM)
	{
	  infop->out.others_found = true;
	}
      break;

    case PT_FUNCTION:
      /* simply give up when we find groupby_num() in predicate
       */
      if (node->info.function.function_type == PT_GROUPBY_NUM)
	{
	  infop->out.others_found = true;
	}
      break;

    case PT_DATA_TYPE:
      /* don't walk data type */
      *continue_walk = PT_LIST_WALK;
      break;

    default:
      break;
    }				/* switch (node->node_type) */

  if (infop->out.others_found)
    {
      /* do not need to traverse anymore */
      *continue_walk = PT_STOP_WALK;
    }

  return tree;
}

/*
 * pt_sargable_term() -
 *   return:
 *   parser(in):
 *   term(in): CNF expression
 *   infop(in):
 */
static bool
pt_sargable_term (PARSER_CONTEXT * parser, PT_NODE * term,
		  FIND_ID_INFO * infop)
{
  /* init output section */
  infop->out.found = false;
  infop->out.others_found = false;
  infop->out.correlated_found = false;
  infop->out.pushable = true;	/* guess as true */

  parser_walk_leaves (parser, term, pt_find_only_name_id, infop, NULL, NULL);

  return infop->out.found && !infop->out.others_found;
}

/*
 * pt_check_copypush_subquery () - check derived subquery to push sargable term
 *                                 into the derived subquery
 *   return:
 *   parser(in):
 *   query(in):
 *
 * Note:
 *  assumes cnf conversion is done
 */
static int
pt_check_copypush_subquery (PARSER_CONTEXT * parser, PT_NODE * query)
{
  int copy_cnt;

  if (query == NULL)
    {
      return 0;
    }

  /* init */
  copy_cnt = 0;

  switch (query->node_type)
    {
    case PT_SELECT:
      if (query->info.query.order_by && query->info.query.orderby_for)
	{
	  copy_cnt++;		/* found not-pushable query */
	}
      else if (pt_has_aggregate (parser, query)
	       && query->info.query.q.select.group_by == NULL)
	{
	  copy_cnt++;		/* found not-pushable query */
	}
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      copy_cnt += pt_check_copypush_subquery (parser,
					      query->info.query.q.union_.
					      arg1);
      copy_cnt +=
	pt_check_copypush_subquery (parser, query->info.query.q.union_.arg2);
      break;

    default:
      break;
    }

  return copy_cnt;
}

/*
 * pt_copypush_terms() - push sargable term into the derived subquery
 *   return:
 *   parser(in):
 *   spec(in):
 *   query(in/out):
 *   term_list(in):
 *   type(in):
 *
 * Note:
 *  assumes cnf conversion is done
 */
static void
pt_copypush_terms (PARSER_CONTEXT * parser, PT_NODE * spec, PT_NODE * query,
		   PT_NODE * term_list, FIND_ID_TYPE type)
{
  PT_NODE *push_term_list;

  if (query == NULL || term_list == NULL)
    {
      return;
    }

  switch (query->node_type)
    {
    case PT_SELECT:
      /* copy terms */
      push_term_list = parser_copy_tree_list (parser, term_list);

      /* substitute as_attr_list's columns for select_list's columns
       * in search condition */
      if (type == FIND_ID_INLINE_VIEW)
	{
	  push_term_list = mq_lambda (parser, push_term_list,
				      spec->info.spec.as_attr_list,
				      query->info.query.q.select.list);
	}

      /* copy and put it in query's search condition */
      if (query->info.query.order_by && query->info.query.orderby_for)
	{
	  ;
	}
      else
	{
	  if (pt_has_aggregate (parser, query))
	    {
	      if (query->info.query.q.select.group_by)
		{
		  /* push into HAVING clause */
		  query->info.query.q.select.having =
		    parser_append_node (push_term_list,
					query->info.query.q.select.having);
		}
	    }
	  else
	    {
	      /* push into WHERE clause */
	      query->info.query.q.select.where =
		parser_append_node (push_term_list,
				    query->info.query.q.select.where);
	    }
	}
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      (void) pt_copypush_terms (parser, spec, query->info.query.q.union_.arg1,
				term_list, type);
      (void) pt_copypush_terms (parser, spec, query->info.query.q.union_.arg2,
				term_list, type);
      break;

    default:
      break;
    }				/* switch (query->node_type) */

  return;
}

/*
 * mq_copypush_sargable_terms_helper() -
 *   return:
 *   parser(in):
 *   statement(in):
 *   spec(in):
 *   new_query(in/out):
 *   infop(in):
 */
static int
mq_copypush_sargable_terms_helper (PARSER_CONTEXT * parser,
				   PT_NODE * statement, PT_NODE * spec,
				   PT_NODE * new_query, FIND_ID_INFO * infop)
{
  PT_NODE *term, *new_term, *push_term_list;
  int push_cnt, push_correlated_cnt, copy_cnt;
  PT_NODE *temp;
  int nullable_cnt;		/* nullable terms count */
  PT_NODE *save_next;
  bool is_afterjoinable;

  /* init */
  push_term_list = NULL;
  push_cnt = 0;
  push_correlated_cnt = 0;

  copy_cnt = -1;

  for (term = statement->info.query.q.select.where; term; term = term->next)
    {
      /* check for nullable-term */
      if (term->node_type == PT_EXPR)
	{
	  save_next = term->next;
	  term->next = NULL;	/* cut-off link */

	  nullable_cnt = 0;	/* init */
	  (void) parser_walk_tree (parser, term, NULL, NULL,
				   qo_check_nullable_expr, &nullable_cnt);

	  term->next = save_next;	/* restore link */

	  if (nullable_cnt)
	    {
	      continue;		/* do not copy-push nullable-term */
	    }
	}
      if (pt_sargable_term (parser, term, infop) && PT_PUSHABLE_TERM (infop))
	{
	  /* copy term */
	  new_term = parser_copy_tree (parser, term);
	  /* for term, mark as copy-pushed term */
	  if (term->node_type == PT_EXPR)
	    {
	      /* check for after-join term */
	      is_afterjoinable = false;	/* init */
	      for (temp = spec; temp; temp = temp->next)
		{
		  if (temp->info.spec.join_type == PT_JOIN_LEFT_OUTER
		      || temp->info.spec.join_type == PT_JOIN_RIGHT_OUTER
		      || temp->info.spec.join_type == PT_JOIN_FULL_OUTER)
		    {
		      is_afterjoinable = true;
		      break;
		    }
		}

	      if (is_afterjoinable)
		{
		  ;		/* may be after-join term. give up */
		}
	      else
		{
		  if (copy_cnt == -1)	/* very the first time */
		    {
		      copy_cnt =
			pt_check_copypush_subquery (parser, new_query);
		    }

		  if (copy_cnt == 0)	/* not found not-pushable query */
		    {
		      PT_EXPR_INFO_SET_FLAG (term, PT_EXPR_INFO_COPYPUSH);
		    }
		}

	      PT_EXPR_INFO_CLEAR_FLAG (new_term, PT_EXPR_INFO_COPYPUSH);
	    }
	  push_term_list = parser_append_node (new_term, push_term_list);

	  push_cnt++;
	  if (infop->out.correlated_found)
	    {
	      push_correlated_cnt++;
	    }
	}
    }				/* for (term = ...) */

  if (push_cnt)
    {
      /* copy and push term in new_query's search condition */
      (void) pt_copypush_terms (parser, spec, new_query, push_term_list,
				infop->type);

      if (push_correlated_cnt)
	{
	  /* set correlation level */
	  if (new_query->info.query.correlation_level == 0)
	    {
	      new_query->info.query.correlation_level =
		statement->info.query.correlation_level + 1;
	    }
	}

      /* free alloced */
      parser_free_tree (parser, push_term_list);
    }

  return push_cnt;
}

/*
 * mq_copypush_sargable_terms() -
 *   return:
 *   parser(in):
 *   statement(in):
 *   spec(in):
 */
static int
mq_copypush_sargable_terms (PARSER_CONTEXT * parser, PT_NODE * statement,
			    PT_NODE * spec)
{
  PT_NODE *derived_table;
  int push_cnt = 0;		/* init */
  FIND_ID_INFO info;

  if (statement->node_type == PT_SELECT
      && spec->info.spec.derived_table_type == PT_IS_SUBQUERY
      && (derived_table = spec->info.spec.derived_table)
      && PT_IS_QUERY (derived_table))
    {
      info.type = FIND_ID_INLINE_VIEW;	/* inline view */
      /* init input section */
      info.in.spec = spec;
      info.in.others_spec_list = statement->info.query.q.select.from;
      info.in.attr_list = spec->info.spec.as_attr_list;
      info.in.query_list = derived_table;

      push_cnt = mq_copypush_sargable_terms_helper (parser, statement,
						    spec, derived_table,
						    &info);
    }

  return push_cnt;
}

/*
 * mq_rewrite_vclass_spec_as_derived() -
 *   return: rewritten SPEC with spec as simple derived select subquery
 *   parser(in):
 *   statement(in):
 *   spec(in):
 *   query_spec(in):
 */
static PT_NODE *
mq_rewrite_vclass_spec_as_derived (PARSER_CONTEXT * parser,
				   PT_NODE * statement,
				   PT_NODE * spec, PT_NODE * query_spec)
{
  PT_NODE *new_query = parser_new_node (parser, PT_SELECT);
  PT_NODE *new_spec;
  PT_NODE *v_attr_list, *v_attr;
  PT_NODE *from, *entity_name;
  PT_NODE *col;

  if (new_query == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return NULL;
    }

  /* mark as a derived vclass spec query */
  new_query->info.query.vspec_as_derived = 1;

  new_query->info.query.q.select.list =
    mq_get_references (parser, statement, spec);

  for (col = new_query->info.query.q.select.list; col; col = col->next)
    {
      if (col->is_hidden_column)
	{
	  col->is_hidden_column = 0;
	}
    }

  v_attr_list =
    mq_fetch_attributes (parser, spec->info.spec.flat_entity_list);

  /* exclude the first oid attr, append non-exist attrs to select list */
  if (v_attr_list && v_attr_list->type_enum == PT_TYPE_OBJECT)
    {
      v_attr_list = v_attr_list->next;	/* skip oid attr */
    }

  for (v_attr = v_attr_list; v_attr; v_attr = v_attr->next)
    {
      v_attr->info.name.spec_id = spec->info.spec.id;	/* init spec id */
      mq_insert_symbol (parser, &new_query->info.query.q.select.list, v_attr);
    }				/* for (v_attr = ...) */

  /* free alloced */
  if (v_attr_list)
    {
      parser_free_tree (parser, v_attr_list);
    }

  new_spec = parser_copy_tree (parser, spec);
  if (new_spec == NULL)
    {
      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
      return NULL;
    }

  new_query->info.query.q.select.from = new_spec;
  new_query->info.query.is_subquery = PT_IS_SUBQUERY;

  /* remove outer join info, which is included in the spec too */
  new_spec->info.spec.join_type = PT_JOIN_NONE;
  parser_free_tree (parser, new_spec->info.spec.on_cond);
  new_spec->info.spec.on_cond = NULL;

  /* free old class spec stuff */
  parser_free_tree (parser, spec->info.spec.flat_entity_list);
  spec->info.spec.flat_entity_list = NULL;
  parser_free_tree (parser, spec->info.spec.entity_name);
  spec->info.spec.entity_name = NULL;

  spec->info.spec.as_attr_list =
    parser_copy_tree_list (parser, new_query->info.query.q.select.list);
  spec->info.spec.derived_table_type = PT_IS_SUBQUERY;

  /* move sargable terms */
  if ((statement->node_type == PT_SELECT)
      && (from = new_query->info.query.q.select.from)
      && (entity_name = from->info.spec.entity_name)
      && (entity_name->node_type != PT_SPEC))
    {
      FIND_ID_INFO info;

      info.type = FIND_ID_VCLASS;	/* vclass */
      /* init input section */
      info.in.spec = spec;
      info.in.others_spec_list = statement->info.query.q.select.from;
      info.in.attr_list = mq_fetch_attributes (parser, entity_name);
      if (query_spec)
	{
	  /* check only specified query spec of the vclass */
	  info.in.query_list = query_spec;
	}
      else
	{
	  /* check all query spec of the vclass */
	  info.in.query_list = mq_fetch_subqueries (parser, entity_name);
	}

      (void) mq_copypush_sargable_terms_helper (parser, statement, spec,
						new_query, &info);

      parser_free_tree (parser, info.in.attr_list);
      if (query_spec)
	{
	  ;			/* do nothing */
	}
      else
	{
	  parser_free_tree (parser, info.in.query_list);
	}
    }

  new_query = mq_reset_ids_and_references (parser, new_query, new_spec);

  spec->info.spec.derived_table = new_query;

  return spec;
}

/*
 * mq_rewrite_aggregate_as_derived() -
 *   return: rewritten select statement with derived table
 *           subquery to form accumulation on
 *   parser(in):
 *   agg_sel(in):
 */
PT_NODE *
mq_rewrite_aggregate_as_derived (PARSER_CONTEXT * parser, PT_NODE * agg_sel)
{
  PT_NODE *derived, *range, *spec;
  PT_AGG_REWRITE_INFO info;
  PT_NODE *col, *tmp, *as_attr_list;
  int idx;

  /* create new subquery as derived */
  derived = parser_new_node (parser, PT_SELECT);

  if (derived == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return NULL;
    }

  /* move hint, from, where, group_by, using_index part over */
  derived->info.query.q.select.hint = agg_sel->info.query.q.select.hint;
  agg_sel->info.query.q.select.hint = PT_HINT_NONE;

#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
  derived->info.query.q.select.ordered_hint =
    agg_sel->info.query.q.select.ordered_hint;
  agg_sel->info.query.q.select.ordered_hint = NULL;
#endif

  derived->info.query.q.select.use_nl_hint =
    agg_sel->info.query.q.select.use_nl_hint;
  agg_sel->info.query.q.select.use_nl_hint = NULL;

  derived->info.query.q.select.use_idx_hint =
    agg_sel->info.query.q.select.use_idx_hint;
  agg_sel->info.query.q.select.use_idx_hint = NULL;

  derived->info.query.q.select.from = agg_sel->info.query.q.select.from;
  agg_sel->info.query.q.select.from = NULL;

  derived->info.query.q.select.where = agg_sel->info.query.q.select.where;
  /* move original group_by to where in place */
  agg_sel->info.query.q.select.where = agg_sel->info.query.q.select.having;
  agg_sel->info.query.q.select.having = NULL;

  derived->info.query.q.select.group_by =
    agg_sel->info.query.q.select.group_by;
  agg_sel->info.query.q.select.group_by = NULL;
  /* move agg flag */
  PT_SELECT_INFO_SET_FLAG (derived, PT_SELECT_INFO_HAS_AGG);
  PT_SELECT_INFO_CLEAR_FLAG (agg_sel, PT_SELECT_INFO_HAS_AGG);

  derived->info.query.q.select.using_index =
    agg_sel->info.query.q.select.using_index;
  agg_sel->info.query.q.select.using_index = NULL;

  /* set correlation level */
  derived->info.query.correlation_level =
    agg_sel->info.query.correlation_level;
  if (derived->info.query.correlation_level)
    {
      derived = mq_bump_correlation_level (parser, derived, 1,
					   derived->info.query.
					   correlation_level);
    }

  /* derived tables are always subqueries */
  derived->info.query.is_subquery = PT_IS_SUBQUERY;

  /* move spec over */
  info.from = derived->info.query.q.select.from;
  info.derived_select = derived;
  info.select_stack = pt_pointer_stack_push (parser, NULL, derived);

  /* set derived range variable */
  range = parser_copy_tree (parser, info.from->info.spec.range_var);
  if (range == NULL)
    {
      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
      return NULL;
    }


  /* construct new spec */
  spec = parser_new_node (parser, PT_SPEC);
  if (spec == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return NULL;
    }

  spec->info.spec.derived_table = derived;
  spec->info.spec.derived_table_type = PT_IS_SUBQUERY;
  spec->info.spec.range_var = range;
  spec->info.spec.id = (UINTPTR) spec;
  range->info.name.spec_id = (UINTPTR) spec;
  info.new_from = spec;

  /* construct derived select list, convert agg_select names and paths */
  info.depth = 0;		/* init */
  agg_sel->info.query.q.select.list =
    parser_walk_tree (parser, agg_sel->info.query.q.select.list,
		      mq_rewrite_agg_names, &info, mq_rewrite_agg_names_post,
		      &info);

  info.depth = 0;		/* init */
  agg_sel->info.query.q.select.where =
    parser_walk_tree (parser, agg_sel->info.query.q.select.where,
		      mq_rewrite_agg_names, &info, mq_rewrite_agg_names_post,
		      &info);

  /* cleanup */
  (void) pt_pointer_stack_pop (parser, info.select_stack, NULL);

  if (!derived->info.query.q.select.list)
    {
      /* we are doing something without names. Must be count(*) */
      derived->info.query.q.select.list =
	pt_resolve_star (parser, derived->info.query.q.select.from, NULL);

      /* reconstruct as_attr_list */
      idx = 0;
      as_attr_list = NULL;
      for (col = derived->info.query.q.select.list; col; col = col->next)
	{
	  tmp = pt_name (parser, mq_generate_name (parser, "a", &idx));
	  tmp->info.name.meta_class = PT_NORMAL;
	  tmp->info.name.resolved = range->info.name.original;
	  tmp->info.name.spec_id = spec->info.spec.id;
	  tmp->type_enum = col->type_enum;
	  tmp->data_type = parser_copy_tree (parser, col->data_type);
	  as_attr_list = parser_append_node (tmp, as_attr_list);
	}

      spec->info.spec.as_attr_list = as_attr_list;
    }

  agg_sel->info.query.q.select.from = spec;

  return agg_sel;
}

/*
 * mq_translate_select() - recursively expands each sub-query in the where part
 *     Then it expands this select statement against the classes which
 *     appear in the from list
 *   return: translated parse tree
 *   parser(in):
 *   select_statement(in):
 */
static PT_NODE *
mq_translate_select (PARSER_CONTEXT * parser, PT_NODE * select_statement)
{
  PT_NODE *from;
  PT_NODE *order_by = NULL;
  PT_MISC_TYPE all_distinct = PT_ALL;

  if (select_statement)
    {
      from = select_statement->info.query.q.select.from;

      order_by = select_statement->info.query.order_by;
      select_statement->info.query.order_by = NULL;
      all_distinct = select_statement->info.query.all_distinct;

      /* for each table/class in select_statements from part,
         do leaf expansion or vclass/view expansion. */

      select_statement = mq_translate_tree (parser, select_statement,
					    from, order_by, DB_AUTH_SELECT);
    }

  /* restore the into part. and order by, if they are not already set. */
  if (select_statement)
    {
      if (!select_statement->info.query.order_by)
	{
	  select_statement->info.query.order_by = order_by;
	}

      if (all_distinct == PT_DISTINCT)
	{
	  /* only set this to distinct. If the current spec is "all"
	     bute the view is on a "distinct" query, the result
	     is still distinct. */
	  select_statement->info.query.all_distinct = all_distinct;
	}
    }

  return select_statement;
}

/*
 * mq_check_update() - checks duplicated column names
 *   return:
 *   parser(in):
 *   update_statement(in):
 */
static void
mq_check_update (PARSER_CONTEXT * parser, PT_NODE * update_statement)
{
  pt_no_double_updates (parser, update_statement);
}

/*
 * mq_check_delete() - checks for duplicate classes in table_list
 *   parser(in): parser context
 *   delete_stmt(in): delete statement
 *
 * NOTE: applies to multi-table delete statements:
 *         DELETE <table_list> FROM <table_reference> ...
 * NOTE: all errors will be returned as parser errors
 */
static void
mq_check_delete (PARSER_CONTEXT * parser, PT_NODE * delete_stmt)
{
  PT_NODE *table, *search;

  if (delete_stmt == NULL)
    {
      return;
    }

  assert (delete_stmt->node_type == PT_DELETE);
  assert (delete_stmt->info.delete_.target_classes != NULL);
  assert (delete_stmt->info.delete_.target_classes->next == NULL);

  for (table = delete_stmt->info.delete_.target_classes; table;
       table = table->next)
    {
      for (search = table->next; search; search = search->next)
	{
	  /* check if search is duplicate of table */
	  if (!pt_str_compare (table->info.name.resolved,
			       search->info.name.resolved, CASE_INSENSITIVE)
	      && table->info.name.spec_id == search->info.name.spec_id)
	    {
	      /* same class found twice in table_list */
	      PT_ERRORmf (parser, search,
			  MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_DUPLICATE_CLASS_OR_ALIAS,
			  search->info.name.resolved);
	      return;
	    }
	}
    }
}

/*
 * mq_translate_update() - leaf expansion or vclass/view expansion for update
 *   return:
 *   parser(in):
 *   update_statement(in):
 */
static PT_NODE *
mq_translate_update (PARSER_CONTEXT * parser, PT_NODE * update_statement)
{
  PT_NODE *from;
  PT_NODE save = *update_statement;

  from = update_statement->info.update.spec;

  /* set flags for updatable specs */
  if (pt_mark_spec_list_for_update (parser, update_statement) != NO_ERROR)
    {
      return NULL;
    }

  update_statement = mq_translate_tree (parser, update_statement,
					from, NULL, DB_AUTH_UPDATE);

  if (update_statement)
    {
      mq_check_update (parser, update_statement);
    }
  if (update_statement == NULL && !pt_has_error (parser))
    {
      PT_ERRORm (parser, &save, MSGCAT_SET_PARSER_RUNTIME,
		 MSGCAT_RUNTIME_UPDATE_EMPTY);
    }

  return update_statement;
}

/*
 * mq_translate_insert() - leaf expansion or vclass/view expansion
 *   return:
 *   parser(in):
 *   insert_statement(in):
 */
static PT_NODE *
mq_translate_insert (PARSER_CONTEXT * parser, PT_NODE * insert_statement)
{
  PT_NODE *from = NULL, *val = NULL, *attr = NULL, **val_hook = NULL;
  PT_NODE *flat = NULL, *temp = NULL, **last = NULL;
  PT_NODE save = *insert_statement;
  PT_NODE *subquery = NULL;
  PT_SPEC_INFO *from_spec = NULL;
  bool viable;
  SEMANTIC_CHK_INFO sc_info = { NULL, false };
  int what_for = DB_AUTH_INSERT;

  insert_statement->next = NULL;
  from = insert_statement->info.insert.spec;

  if (insert_statement->info.insert.odku_assignments != NULL)
    {
      what_for = DB_AUTH_INSERT_UPDATE;
    }

  if (insert_statement->info.insert.do_replace)
    {
      assert (insert_statement->info.insert.odku_assignments == NULL);
      what_for = DB_AUTH_REPLACE;
    }

  insert_statement = mq_translate_tree (parser, insert_statement, from,
					NULL, what_for);

  /* check there are no double assignments after translate */
  pt_no_double_insert_assignments (parser, insert_statement);
  if (pt_has_error (parser))
    {
      return NULL;
    }

  if (insert_statement)
    {
      PT_NODE *t_save = insert_statement;	/* save start node pointer */
      PT_NODE *head = NULL;
      PT_NODE *prev = NULL;

      while (insert_statement)
	{
	  PT_NODE *crt_list = insert_statement->info.insert.value_clauses;
	  bool multiple_tuples_insert = crt_list->next != NULL;

	  if (crt_list->info.node_list.list_type == PT_IS_VALUE)
	    {
	      /* deal with case 3 */
	      attr = pt_attrs_part (insert_statement);
	      val_hook = &crt_list->info.node_list.list;
	      val = *val_hook;

	      while (attr && val)
		{
		  if (val->node_type == PT_INSERT && val->etc)
		    {
		      PT_NODE *val_next = val->next;
		      PT_NODE *flat;
		      DB_OBJECT *real_class = NULL;

		      /* TODO what about multiple tuples insert? What should
		         this code do? We give up processing for now. */
		      if (multiple_tuples_insert)
			{
			  return NULL;
			}

		      /* this is case 3 above. Need to choose the appropriate
		       * nested insert statement. */
		      /* do you solve your problem? */
		      if (head)
			{	/* after the first loop */
			  val->next = head;
			  head = val;
			  val->etc = NULL;
			}
		      else
			{	/* the first loop */
			  val->next = (PT_NODE *) val->etc;
			  head = val;
			  val->etc = NULL;
			}

		      if (attr->data_type
			  && attr->data_type->info.data_type.entity)
			{
			  real_class =
			    attr->data_type->info.data_type.entity->info.name.
			    db_object;
			}

		      /* if there is a real class this must match, use it.
		       * otherwise it must be a "db_object" type, so any
		       * will do. */
		      if (real_class)
			{
			  while (val)
			    {
			      if (val->info.insert.spec
				  && (flat = val->info.insert.spec->
				      info.spec.flat_entity_list)
				  && flat->info.name.db_object == real_class)
				{
				  break;	/* found it */
				}
			      prev = val;
			      val = val->next;
			    }
			}

		      if (val)
			{
			  if (val == head)
			    {
			      head = head->next;
			    }
			  else
			    {
			      prev->next = val->next;
			    }
			}
		      else
			{
			  val = parser_new_node (parser, PT_VALUE);
			  if (val == NULL)
			    {
			      PT_INTERNAL_ERROR (parser, "allocate new node");
			      return NULL;
			    }

			  PT_SET_NULL_NODE (val);
			}

		      val->next = val_next;
		      /* and finally replace it */
		      *val_hook = val;
		    }

		  attr = attr->next;
		  val_hook = &val->next;
		  val = *val_hook;
		}
	    }

	  insert_statement = insert_statement->next;
	}

      if (head)
	{
	  parser_free_tree (parser, head);
	}

      insert_statement = t_save;

      /* Now deal with case 1, 2 */
      last = &insert_statement;

      /* now pick a viable insert statement */
      while (*last)
	{
	  temp = *last;
	  from = temp->info.insert.spec;
	  from_spec = &(from->info.spec);
	  /* try to retrieve info from derived table */
	  if (from_spec->derived_table_type == PT_IS_SUBQUERY)
	    {
	      from = from_spec->derived_table->info.query.q.select.from;
	      temp->info.insert.spec = from;
	    }
	  flat = from->info.spec.flat_entity_list;
	  if (flat == NULL)
	    {
	      assert (false);
	      return NULL;
	    }

	  viable = false;
	  if (db_is_class (flat->info.name.db_object))
	    {
	      viable = true;
	    }

	  if (viable)
	    {
	      /* propagate temp's type information upward now because
	       * mq_check_insert_compatibility calls pt_class_assignable
	       * which expects accurate type information.
	       */
	      sc_info.top_node = temp;
	      pt_semantic_type (parser, temp, &sc_info);
	    }

	  /* here we just go to the next item in the list.
	   * If it is a nested insert, the correct one will
	   * be selected from the list at the outer level.
	   * If it is a top level insert, the correct one
	   * will be determined at run time.
	   */
	  last = &temp->next;
	}
    }

  if (insert_statement == NULL)
    {
      if (!pt_has_error (parser))
	{
	  PT_ERRORm (parser, &save, MSGCAT_SET_PARSER_RUNTIME,
		     MSGCAT_RUNTIME_INSERT_EMPTY);
	}
    }

  if (pt_has_error (parser))
    {
      return NULL;
    }

  assert (insert_statement != NULL);

  subquery = pt_get_subquery_of_insert_select (insert_statement);

  if (subquery != NULL && PT_IS_SELECT (subquery)
      && insert_statement->info.insert.odku_assignments != NULL)
    {
      /* The odku_assignments might refer nodes from the SELECT statements.
       * Even though name resolving was already performed on odku_assigments,
       * we have to redo it here. If the SELECT target was a view, it has been
       * rewritten by mq_translate_local and names which referenced it were
       * not updated.
       */
      SEMANTIC_CHK_INFO sc_info = { NULL, false };
      PT_NODE *odku = insert_statement->info.insert.odku_assignments;

      from = insert_statement->info.insert.spec;
      /* reset spec_id for names not referencing the insert statement */
      odku = parser_walk_tree (parser, odku, mq_clear_other_ids, from, NULL,
			       NULL);
      if (odku == NULL)
	{
	  return NULL;
	}

      /* redo name resolving */
      insert_statement = pt_resolve_names (parser, insert_statement,
					   &sc_info);
      if (insert_statement == NULL || pt_has_error (parser))
	{
	  return NULL;
	}

      /* need to recheck this in case something went wrong */
      insert_statement = pt_check_odku_assignments (parser, insert_statement);
    }

  insert_check_names_in_value_clauses (parser, insert_statement);
  if (pt_has_error (parser))
    {
      return NULL;
    }

  return insert_statement;
}

/*
 * mq_translate_delete() - leaf expansion or vclass/view expansion
 *   return:
 *   parser(in):
 *   delete_statement(in):
 */
static PT_NODE *
mq_translate_delete (PARSER_CONTEXT * parser, PT_NODE * delete_statement)
{
  PT_NODE *from;
  PT_NODE save = *delete_statement;

  /* set flags for deletable specs */
  if (pt_mark_spec_list_for_delete (parser, delete_statement) != NO_ERROR)
    {
      return NULL;
    }

  from = delete_statement->info.delete_.spec;
  delete_statement = mq_translate_tree (parser, delete_statement,
					from, NULL, DB_AUTH_DELETE);
  if (delete_statement != NULL)
    {
      /* check delete statement */
      mq_check_delete (parser, delete_statement);

      if (pt_has_error (parser))
	{
	  /* checking failed */
	  return NULL;
	}
    }
  else if (!pt_has_error (parser))
    {
      PT_ERRORm (parser, &save, MSGCAT_SET_PARSER_RUNTIME,
		 MSGCAT_RUNTIME_DELETE_EMPTY);
    }

  return delete_statement;
}


/*
 * mq_check_rewrite_select() -
 *   return: rewrited parse tree
 *   parser(in):
 *   select_statement(in):
 *
 * Note:
 * 	1) virtual specs which are part of a join AND which would
 * 	   translate to a union, are rewritten as derived.
 * 	   (This is an optimization to avoid multiplying the number
 * 	   of subqueries, joins and unions occuring.)
 *
 * 	2) virtual specs of aggregate selects which would translate
 * 	   to a union are rewritten as derived.
 */
static PT_NODE *
mq_check_rewrite_select (PARSER_CONTEXT * parser, PT_NODE * select_statement)
{
  PT_NODE *from;

  /* Convert to cnf and tag taggable terms */
  select_statement->info.query.q.select.where =
    pt_cnf (parser, select_statement->info.query.q.select.where);
  if (select_statement->info.query.q.select.having)
    {
      select_statement->info.query.q.select.having =
	pt_cnf (parser, select_statement->info.query.q.select.having);
    }

  from = select_statement->info.query.q.select.from;
  if (from && (from->next || pt_has_aggregate (parser, select_statement)))
    {
      /* when translating joins, its important to maintain linearity
       * of the translation. The cross-product of unions is exponential.
       * Therefore, we convert cross-products of unions to cross-products
       * of derived tables.
       */

      if (mq_is_union_translation (parser, from))
	{
	  select_statement->info.query.q.select.from = from =
	    mq_rewrite_vclass_spec_as_derived (parser, select_statement,
					       from, NULL);
	  if (from == NULL)
	    {
	      return NULL;
	    }
	}
      else if (from->info.spec.derived_table_type == PT_IS_SUBQUERY)
	{
	  (void) mq_copypush_sargable_terms (parser, select_statement, from);
	}

      while (from->next)
	{
	  if (mq_is_union_translation (parser, from->next))
	    {
	      from->next =
		mq_rewrite_vclass_spec_as_derived (parser, select_statement,
						   from->next, NULL);
	    }
	  else if (from->next->info.spec.derived_table_type == PT_IS_SUBQUERY)
	    {
	      (void) mq_copypush_sargable_terms (parser, select_statement,
						 from->next);
	    }
	  from = from->next;
	}
    }
  else
    {
      /* see 'xtests/10010_vclass_set.sql' and 'err_xtests/check21.sql' */
      if (select_statement->info.query.is_subquery == 0
	  && select_statement->info.query.is_view_spec == 0
	  && select_statement->info.query.oids_included == DB_NO_OIDS
	  && mq_is_union_translation (parser, from))
	{
	  select_statement->info.query.q.select.from =
	    mq_rewrite_vclass_spec_as_derived (parser, select_statement, from,
					       NULL);
	}
      else if (from && from->info.spec.derived_table_type == PT_IS_SUBQUERY)
	{
	  (void) mq_copypush_sargable_terms (parser, select_statement, from);
	}
    }

  return select_statement;
}

/*
 * mq_push_paths() - rewrites from specs, and path specs, to things
 * 	             mq_translate_select can handle
 *   return:
 *   parser(in):
 *   statement(in):
 *   void_arg(in):
 *   cw(in):
 */
static PT_NODE *
mq_push_paths (PARSER_CONTEXT * parser, PT_NODE * statement,
	       UNUSED_ARG void *void_arg, UNUSED_ARG int *continue_walk)
{
  if (statement == NULL)
    {
      return NULL;
    }

  switch (statement->node_type)
    {
    case PT_SELECT:
      if (statement->info.query.is_subquery == PT_IS_SUBQUERY
	  && statement->info.query.oids_included == DB_ROW_OIDS)
	{
	  /* if we do not check this condition, it could be infinite loop
	     because 'mq_push_paths()' is to be re-applied to the subquery
	     of 'spec->derived_table', which was generated by
	     'mq_rewrite_vclass_spec_as_derived()' */
	}
      else
	{
	  statement = mq_check_rewrite_select (parser, statement);
	  if (statement == NULL)
	    {
	      break;
	    }
	}

      break;

    default:
      break;
    }

  return statement;
}


/*
 * mq_translate_local() - recursively expands each query against a view or
 * 			  virtual class
 *   return:
 *   parser(in):
 *   statement(in):
 *   void_arg(in):
 *   cw(in):
 */
static PT_NODE *
mq_translate_local (PARSER_CONTEXT * parser,
		    PT_NODE * statement, UNUSED_ARG void *void_arg,
		    UNUSED_ARG int *continue_walk)
{
  int line, column;
  PT_NODE *next;
  PT_NODE *indexp, *spec, *using_index;
  bool aggregate_rewrote_as_derived = false;

  if (statement == NULL)
    {
      return statement;
    }

  next = statement->next;
  statement->next = NULL;

  /* try to track original source line and column */
  line = statement->line_number;
  column = statement->column_number;

  switch (statement->node_type)
    {
    case PT_SELECT:
      statement = mq_translate_select (parser, statement);

      if (statement)
	{
	  if (pt_has_aggregate (parser, statement)
	      && mq_has_corr_subqueries (parser, statement))
	    {
	      /* We need to push correlated subqueries
	       * from the select list into the derived table
	       * because we have no other way of generating correct XASL
	       * for correlated subqueries on aggregate queries.
	       */
	      statement = mq_rewrite_aggregate_as_derived (parser, statement);
	      aggregate_rewrote_as_derived = true;
	    }
	}

      break;

    case PT_UPDATE:
      statement = mq_translate_update (parser, statement);
      break;

    case PT_INSERT:
      statement = mq_translate_insert (parser, statement);
      break;

    case PT_DELETE:
      statement = mq_translate_delete (parser, statement);
      break;

    default:
      break;
    }

  if (statement)
    {
      switch (statement->node_type)
	{
	case PT_SELECT:
	  statement->info.query.is_subquery = PT_IS_SUBQUERY;
	  break;

	case PT_UNION:
	case PT_DIFFERENCE:
	case PT_INTERSECTION:
	  statement->info.query.is_subquery = PT_IS_SUBQUERY;
	  mq_set_union_query (parser, statement->info.query.q.union_.arg1,
			      PT_IS_UNION_SUBQUERY);
	  mq_set_union_query (parser, statement->info.query.q.union_.arg2,
			      PT_IS_UNION_SUBQUERY);
	  break;

	default:
	  break;
	}

      statement->line_number = line;
      statement->column_number = column;
      /* beware of simply restoring next because the newly rewritten
       * statement can be a list.  so we must append next to statement.
       * (The number of bugs caused by this multipurpose use of node->next
       * tells us it's not a good idea.)
       */
      parser_append_node (next, statement);
    }

  /* resolving using index */
  using_index = NULL;
  spec = NULL;
  if (!pt_has_error (parser) && statement)
    {
      switch (statement->node_type)
	{
	case PT_SELECT:
	  spec = statement->info.query.q.select.from;
	  if (aggregate_rewrote_as_derived && spec != NULL)
	    {
	      PT_NODE *derived_table = spec->info.spec.derived_table;
	      assert (derived_table != NULL);
	      using_index = derived_table->info.query.q.select.using_index;
	      spec = derived_table->info.query.q.select.from;
	    }
	  else
	    {
	      using_index = statement->info.query.q.select.using_index;
	    }
	  break;

	case PT_UPDATE:
	  using_index = statement->info.update.using_index;
	  spec = statement->info.update.spec;
	  break;

	case PT_DELETE:
	  using_index = statement->info.delete_.using_index;
	  spec = statement->info.delete_.spec;
	  break;

	default:
	  break;
	}
    }

  /* resolve using index */
  indexp = using_index;
  if (indexp != NULL && spec != NULL)
    {
      for (; indexp; indexp = indexp->next)
	{
	  if (pt_resolve_using_index (parser, indexp, spec) == NULL)
	    {
	      return NULL;
	    }
	}
    }

  /* semantic check on using index */
  if (using_index != NULL)
    {
      if (mq_check_using_index (parser, using_index) != NO_ERROR)
	{
	  return NULL;
	}
    }

  return statement;
}


/*
 * mq_check_using_index() - check the using index clause for semantic errors
 *   return: error code
 *   parser(in): current parser
 *   using_index(in): list of PT_NODEs in USING INDEX clause
 */
static int
mq_check_using_index (PARSER_CONTEXT * parser, PT_NODE * using_index)
{
  PT_NODE *hint_none = NULL;
  PT_NODE *hint_use = NULL;

  bool has_errors = false;

  PT_NODE *index_hint = NULL;
  PT_NODE *node = NULL, *search_node = NULL;
  bool is_hint_class_none = false;
  bool is_hint_use = false;

  /* check for valid using_index node */
  if (using_index == NULL)
    {
      return NO_ERROR;
    }

  /* Gathering basic information about the index hints */
  node = using_index;
  while (node != NULL)
    {
      if (node->etc == (void *) PT_IDX_HINT_NONE)
	{
	  /* USING INDEX NONE node found */
	  assert (node->info.name.original == NULL
		  && node->info.name.resolved == NULL);
	  hint_none = node;
	}
      else if (node->etc == (void *) PT_IDX_HINT_CLASS_NONE)
	{
	  is_hint_class_none = true;
	}
      else if (node->etc == (void *) PT_IDX_HINT_USE)
	{
	  /* found USE INDEX idx or USING INDEX idx node */
	  if (node->info.name.original != NULL
	      && node->info.name.resolved != NULL)
	    {
	      /* check iff existing index */
	      is_hint_use = true;
	      if (hint_use == NULL)
		{
		  hint_use = node;
		}
	    }
	}
      else
	{
	  /* all hint nodes must have the etc flag set from the grammar */
	  assert (false);
	}

      node = node->next;
    }

  /* check for USING INDEX NONE and USE INDEX; error if both found */
  if (hint_none != NULL)
    {
      if (hint_use != NULL)
	{
	  index_hint = hint_use;
	  has_errors = true;
	}

      if (has_errors)
	{
	  /* USE INDEX idx ... USING INDEX NONE case was found */
	  PT_ERRORmf2 (parser, using_index, MSGCAT_SET_PARSER_SEMANTIC,
		       MSGCAT_SEMANTIC_INDEX_HINT_CONFLICT,
		       "using index none",
		       parser_print_tree (parser, index_hint));
	  return ER_PT_SEMANTIC;
	}
    }

  /*
   * USING INDEX t.none is incompatible with USE INDEX [t.]idx
   * Check for USING INDEX class.NONE, class.any-index[(+)] or
   * USE INDEX (class.any-index) ... USING INDEX class.NONE
   */
  node = using_index;
  while (node != NULL && is_hint_class_none && is_hint_use)
    {
      if (node->info.name.original == NULL
	  && node->info.name.resolved != NULL
	  && node->etc == (void *) PT_IDX_HINT_CLASS_NONE)
	{
	  /* search trough all nodes again and check for other index hints
	     on class_name */
	  search_node = using_index;
	  while (search_node != NULL)
	    {
	      if (search_node->info.name.original != NULL
		  && search_node->info.name.resolved != NULL
		  && search_node->etc == (void *) PT_IDX_HINT_USE
		  &&
		  intl_identifier_casecmp (node->info.name.resolved,
					   search_node->info.name.resolved) ==
		  0)
		{
		  /* class_name.idx_name and class_name.none found in USE
		     INDEX and/or USING INDEX clauses */
		  PT_ERRORmf2 (parser, using_index,
			       MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_INDEX_HINT_CONFLICT,
			       parser_print_tree (parser, node),
			       parser_print_tree (parser, search_node));

		  return ER_PT_SEMANTIC;
		}

	      search_node = search_node->next;
	    }
	}

      node = node->next;
    }

  /* no error */
  return NO_ERROR;
}


/*
 * mq_fetch_subqueries() - ask the schema manager for the cached parser
 *            	           containing the compiled subqueries of the class
 *   return:
 *   parser(in):
 *   class(in):
 */
static PT_NODE *
mq_fetch_subqueries (PARSER_CONTEXT * parser, PT_NODE * class_)
{
  PARSER_CONTEXT *query_cache;
  DB_OBJECT *class_object;
  PT_NODE *subquery = NULL;
#if !defined(NDEBUG)
  int save_host_var_count = parser->host_var_count;
#endif

  if (!class_ || !(class_object = class_->info.name.db_object)
      || db_is_class (class_object))
    {
      return NULL;
    }

  query_cache = sm_virtual_queries (parser, class_object);

#if !defined(NDEBUG)
  assert (save_host_var_count == parser->host_var_count);
#endif

  if (query_cache == NULL || query_cache->view_cache == NULL)
    {
      return NULL;
    }

  if (!(query_cache->view_cache->vquery_authorization & DB_AUTH_SELECT))
    {
      PT_ERRORmf2 (parser, class_, MSGCAT_SET_PARSER_RUNTIME,
		   MSGCAT_RUNTIME_IS_NOT_AUTHORIZED_ON,
		   get_authorization_name (DB_AUTH_SELECT),
		   sm_class_name (class_object));
      return NULL;
    }

  subquery =
    parser_copy_tree_list (parser, query_cache->view_cache->vquery_for_query);

  return subquery;
}


/*
 * mq_set_types() - sets the type of each item in the select list to
 * match the class's attribute type
 *   return:
 *   parser(in):
 *   query_spec(in):
 *   attributes(in):
 *   vclass_object(in):
 */
static PT_NODE *
mq_set_types (PARSER_CONTEXT * parser, PT_NODE * query_spec,
	      PT_NODE * attributes, DB_OBJECT * vclass_object)
{
  PT_NODE *col, *prev_col, *new_col;
  PT_NODE *attr;
  PT_NODE *col_type;
  PT_NODE *flat = NULL;

  if (query_spec == NULL)
    {
      return NULL;
    }

  switch (query_spec->node_type)
    {
    case PT_SELECT:
      if (query_spec->info.query.q.select.from != NULL)
	{
	  flat = query_spec->info.query.q.select.from->info.spec.
	    flat_entity_list;
	}
      else
	{
	  flat = NULL;
	}

      while (flat)
	{
	  flat->info.name.virt_object = vclass_object;
	  flat = flat->next;
	}

      attr = attributes;
      col = query_spec->info.query.q.select.list;
      prev_col = NULL;
      while (col && attr)
	{
	  /* should check type compatibility here */

	  if (PT_IS_NULL_NODE (col))
	    {
	      /* These are compatible with anything */
	    }
	  else if (attr->type_enum == PT_TYPE_OBJECT)
	    {
	      if (attr->data_type != NULL)
		{
		  /* don't raise an error for the oid placeholder
		   * the column may not be an object for non-updatable views
		   */
		  if (!(col_type = col->data_type) ||
		      col->type_enum != PT_TYPE_OBJECT)
		    {
		      if (attr != attributes)
			{
			  PT_ERRORmf (parser, col,
				      MSGCAT_SET_PARSER_RUNTIME,
				      MSGCAT_RUNTIME_QSPEC_INCOMP_W_ATTR,
				      attr->info.name.original);
			  return NULL;
			}
		    }
		}
	    }
	  else if (col->type_enum != attr->type_enum)
	    {
	      if (col->node_type == PT_VALUE)
		{
		  (void) pt_coerce_value (parser, col, col,
					  attr->type_enum, attr->data_type);
		  /* this should also set an error code if it fails */
		}
	      else
		{		/* need to CAST */
		  new_col = pt_type_cast_vclass_query_spec_column (parser,
								   attr, col);
		  if (new_col != col)
		    {
		      if (prev_col == NULL)
			{
			  query_spec->info.query.q.select.list = new_col;
			  query_spec->type_enum = new_col->type_enum;
			  if (query_spec->data_type)
			    {
			      parser_free_tree (parser,
						query_spec->data_type);
			    }
			  query_spec->data_type =
			    parser_copy_tree_list (parser,
						   new_col->data_type);
			}
		      else
			{
			  prev_col->next = new_col;
			}

		      col = new_col;
		    }
		}
	    }

	  /* save previous link */
	  prev_col = col;

	  /* advance to next attribute and column */
	  attr = attr->next;
	  col = col->next;
	}

      /* skip hidden column */
      while (col)
	{
	  if (col->is_hidden_column)
	    {
	      col = col->next;
	      continue;
	    }
	  break;
	}

      if (col)
	{
	  PT_ERRORmf (parser, query_spec, MSGCAT_SET_PARSER_RUNTIME,
		      MSGCAT_RUNTIME_QSPEC_COLS_GT_ATTRS,
		      sm_class_name (vclass_object));
	  return NULL;
	}

      if (attr)
	{
	  PT_ERRORmf (parser, query_spec, MSGCAT_SET_PARSER_RUNTIME,
		      MSGCAT_RUNTIME_ATTRS_GT_QSPEC_COLS,
		      sm_class_name (vclass_object));
	  return NULL;
	}

      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      mq_set_types (parser, query_spec->info.query.q.union_.arg1,
		    attributes, vclass_object);
      mq_set_types (parser, query_spec->info.query.q.union_.arg2,
		    attributes, vclass_object);
      break;

    default:
      /* could flag an error here, this should not happen */
      break;
    }

  return query_spec;
}


/*
 * mq_add_dummy_from_pre () - adds a dummy "FROM db-root" to view definitions
 *			  that do not have one.

 *   Note:      This is required so that the view handling code remains
 *		consistent with the assumption that each SELECT in a view
 *              has some hidden OID columns.
 *	        This only happens for views or sub-queries of views.
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_add_dummy_from_pre (PARSER_CONTEXT * parser, PT_NODE * node,
		       UNUSED_ARG void *arg, int *continue_walk)
{
  PT_NODE *fake_from;

  if (!node)
    {
      return node;
    }

  if (node->node_type != PT_SELECT || node->info.query.q.select.from != NULL)
    {
      return node;
    }

  fake_from = pt_add_table_name_to_from_list (parser, node, CT_ROOT_NAME,
					      NULL, DB_AUTH_NONE);
  if (fake_from == NULL)
    {
      *continue_walk = PT_STOP_WALK;
      return NULL;
    }

  return node;
}

/*
 * mq_translate_subqueries() - Translates virtual instance population
 *                             queries of any class
 *   return: a select or union of selects
 *   parser(in):
 *   class_object(in):
 *   attributes(in):
 *   authorization(in/out):
 */
static PT_NODE *
mq_translate_subqueries (PARSER_CONTEXT * parser,
			 DB_OBJECT * class_object,
			 PT_NODE * attributes, DB_AUTH * authorization)
{
  DB_QUERY_SPEC *db_query_spec;
  PT_NODE *result;
  PT_NODE *query_spec;
  PT_NODE *statements;
  PT_NODE *local_query;
  const char *query_spec_string;

  if (db_is_class (class_object))
    {
      return NULL;
    }

  assert (authorization != NULL);

  /* get query spec's */
  db_query_spec = db_get_query_specs (class_object);

  statements = NULL;
  local_query = NULL;

  while (db_query_spec)
    {
      /* parse and compile the next query spec */
      query_spec_string = db_query_spec_string (db_query_spec);
      result =
	parser_parse_string_use_sys_charset (parser, query_spec_string);

      /* a system error, that allowed a syntax error to be in
       * a query spec string. May want to augment the error messages
       * provided by parser_parse_string.
       */
      if (!result)
	{
	  return NULL;
	}

      query_spec = result;

      query_spec = parser_walk_tree (parser, query_spec,
				     mq_add_dummy_from_pre, NULL, NULL, NULL);
      if (query_spec == NULL)
	{
	  return NULL;
	}

      parser_walk_tree (parser, query_spec, pt_set_is_view_spec, NULL, NULL,
			NULL);

      /* apply semantic checks */
      query_spec = pt_compile (parser, query_spec);

      /* a system error, that allowed a semantic error to be in
       * a query spec string. May want to augment the error messages
       * provided by parser_parse_string.
       */
      if (!query_spec)
	{
	  return NULL;
	}

      *authorization = (DB_AUTH) (*authorization &
				  mq_compute_query_authorization
				  (query_spec));

      /* this will recursively expand the query spec into
       * local queries.
       * The mq_push_paths will convert the expression as CNF. if subquery is
       * in the expression, it may be copied several times. To avoid repeatedly
       * convert the expressions in subqueries and improve performance, we
       * put mq_push_paths as post function.
       */
      local_query =
	parser_walk_tree (parser, query_spec, NULL, NULL, mq_push_paths,
			  NULL);
      local_query =
	parser_walk_tree (parser, query_spec, NULL, NULL, mq_translate_local,
			  NULL);

      mq_set_types (parser, local_query, attributes, class_object);

      /* Reset references to positions in query_spec_string for each node
       * in this tree. These nodes will be used in other contexts and these
       * references are meaningless
       */
      local_query =
	parser_walk_tree (parser, local_query,
			  mq_reset_references_to_query_string, NULL, NULL,
			  NULL);
      if (local_query == NULL)
	{
	  return NULL;
	}

      if (statements == NULL)
	{
	  statements = local_query;
	}
      else if (local_query)
	{
	  statements = pt_union (parser, statements, local_query);
	}

      db_query_spec = db_query_spec_next (db_query_spec);
    }

  return statements;
}

/*
 * mq_check_cycle() -
 *   return: true if the class object is found in the cycle detection buffer
 *           fasle if not found, and add the object to the buffer
 *   class_object(in):
 */
static bool
mq_check_cycle (DB_OBJECT * class_object)
{
  unsigned int i, max, enter;

  enter = top_cycle % MAX_CYCLE;
  max = top_cycle < MAX_CYCLE ? top_cycle : MAX_CYCLE;

  for (i = 0; i < max; i++)
    {
      if (cycle_buffer[i] == class_object)
	{
	  return true;
	}
    }

  /* otherwise increment top cycle and enter object in buffer */
  cycle_buffer[enter] = class_object;
  top_cycle++;

  return false;
}


/*
 * mq_free_virtual_query_cache() - Clear parse trees used for view translation,
 *                                 and the cached parser
 *   return: none
 *   parser(in):
 */
void
mq_free_virtual_query_cache (PARSER_CONTEXT * parser)
{
  VIEW_CACHE_INFO *info;

  /*  symbols is used to hold the virtual query cache */
  info = (VIEW_CACHE_INFO *) parser->view_cache;

  parser_free_tree (parser, info->attrs);
  parser_free_tree (parser, info->vquery_for_query);

  parser_free_parser (parser);

  return;
}

/*
 * mq_virtual_queries() - recursively expands each query against a view or
 *                        virtual class
 *   return:
 *   class_object(in):
 */
PARSER_CONTEXT *
mq_virtual_queries (DB_OBJECT * class_object)
{
  char buf[2000];
  const char *cname = sm_class_name (class_object);
  PARSER_CONTEXT *parser = parser_create_parser ();
  PT_NODE *statements;
  VIEW_CACHE_INFO *symbols;
  DB_OBJECT *me = db_get_user ();
  DB_OBJECT *owner = db_get_owner (class_object);

  assert (class_object != NULL);

  if (parser == NULL)
    {
      return NULL;
    }

  snprintf (buf, sizeof (buf), "select * from [%s]; ", cname);
  statements = parser_parse_string (parser, buf);

  parser->view_cache = (VIEW_CACHE_INFO *)
    parser_alloc (parser, sizeof (VIEW_CACHE_INFO));
  symbols = parser->view_cache;

  if (symbols == NULL)
    {
      PT_INTERNAL_ERROR (parser, "parser_alloc");
      return NULL;
    }

  if (owner != me)
    {
      symbols->vquery_authorization = mq_compute_authorization (class_object);
    }
  else
    {
      /* no authorization check */
      symbols->vquery_authorization = DB_AUTH_ALL;
    }

  if (statements)
    {
      if (mq_check_cycle (class_object))
	{
	  PT_ERRORmf (parser, statements, MSGCAT_SET_PARSER_RUNTIME,
		      MSGCAT_RUNTIME_CYCLIC_QUERY_SPEC, cname);
	}
    }

  if (statements && !pt_has_error (parser))
    {
      statements = pt_compile (parser, statements);

      if (statements && !pt_has_error (parser))
	{
	  symbols->attrs = statements->info.query.q.select.list;

	  statements->info.query.q.select.list = NULL;
	  parser_free_tree (parser, statements);

	  if (owner != me)
	    {
	      /* set user to owner to translate query specification. */
	      AU_SET_USER (owner);
	    }

	  symbols->vquery_for_query =
	    mq_translate_subqueries (parser, class_object, symbols->attrs,
				     &(symbols->vquery_authorization));
	}
    }

  if (owner != me)
    {
      /* set user to me */
      AU_SET_USER (me);
    }

  /* end cycle check */
  if (top_cycle > 0)
    {
      top_cycle--;
      cycle_buffer[top_cycle % MAX_CYCLE] = NULL;
    }
  else
    {
      top_cycle = 0;
    }

  return parser;
}

/*
 * mq_mark_location() -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_mark_location (PARSER_CONTEXT * parser, PT_NODE * node,
		  void *arg, UNUSED_ARG int *continue_walk)
{
  short *locp = (short *) arg;

  if (!locp && node->node_type == PT_SELECT)
    {
      short location = 0;
      PT_NODE *spec, *on_cond;

      for (spec = node->info.query.q.select.from; spec; spec = spec->next)
	{
	  spec->info.spec.location = location++;
	  on_cond = spec->info.spec.on_cond;
	  if (on_cond)
	    {
	      switch (spec->info.spec.join_type)
		{
		case PT_JOIN_INNER:
		case PT_JOIN_LEFT_OUTER:
		case PT_JOIN_RIGHT_OUTER:
		  parser_walk_tree (parser, on_cond,
				    mq_mark_location,
				    &(spec->info.spec.location), NULL, NULL);
		  break;
		  /*case PT_JOIN_FULL_OUTER:  not supported */

		case PT_JOIN_NONE:
		default:
		  break;
		}		/* switch (spec->info.spec.join_type) */

	      /* ON cond will be moved at optimize_queries */
	    }

	  if (spec->info.spec.entity_name)
	    {
	      PT_NODE *node = spec->info.spec.entity_name;

	      if (node->node_type == PT_NAME)
		{
		  node->info.name.location = spec->info.spec.location;
		}
	      else if (node->node_type == PT_SPEC)
		{
		  node->info.spec.location = spec->info.spec.location;
		}
	      else
		{
		  /* dummy else. this case will not happen */
		  assert (0);
		}
	    }
	}
    }
  else if (locp)
    {
      if (node->node_type == PT_EXPR)
	{
	  node->info.expr.location = *locp;
	}
      else if (node->node_type == PT_NAME)
	{
	  node->info.name.location = *locp;
	}
      else if (node->node_type == PT_VALUE)
	{
	  node->info.value.location = *locp;
	}
    }

  return node;
}

/*
 * mq_check_non_updatable_vclass_oid() -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_check_non_updatable_vclass_oid (PARSER_CONTEXT * parser, PT_NODE * node,
				   UNUSED_ARG void *void_arg,
				   UNUSED_ARG int *continue_walk)
{
  PT_NODE *dt;
  PT_NODE *cls;

  switch (node->node_type)
    {
    case PT_NAME:
      if (node->type_enum == PT_TYPE_OBJECT
	  && (dt = node->data_type)
	  && dt->type_enum == PT_TYPE_OBJECT
	  && (cls = dt->info.data_type.entity))
	{
	  if (db_is_vclass (cls->info.name.db_object))
	    {
	      /* is non-updatable vclass oid */
	      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_RUNTIME,
			  MSGCAT_RUNTIME_NO_VID_FOR_NON_UPDATABLE_VIEW,
			  /* use function to get name */
			  sm_class_name (cls->info.name.db_object));
	    }
	}
      break;
    default:
      break;
    }

  return node;
}

/*
 * mq_translate_helper() - main workhorse for mq_translate
 *   return:
 *   parser(in):
 *   node(in):
 */
static PT_NODE *
mq_translate_helper (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *next;
  int err = NO_ERROR;

  if (!node)
    {
      return NULL;
    }

  /* save and zero link */
  next = node->next;
  node->next = NULL;

  switch (node->node_type)
    {
      /* only translate translatable statements */
    case PT_SELECT:
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      /*
       * The mq_push_paths will convert the expression as CNF. if subquery is
       * in the expression, it may be copied several times. To avoid repeatedly
       * convert the expressions in subqueries and improve performance, we
       * put mq_push_paths as post function.
       */
      node = parser_walk_tree (parser, node, NULL, NULL, mq_push_paths, NULL);
      node = parser_walk_tree (parser, node, NULL, NULL, mq_translate_local,
			       NULL);

      node = parser_walk_tree (parser, node,
			       mq_mark_location, NULL,
			       mq_check_non_updatable_vclass_oid, NULL);

      if (pt_has_error (parser))
	{
	  goto exit_on_error;
	}

      if (node)
	{
	  node->info.query.is_subquery = (PT_MISC_TYPE) (-1);
	  if (node->node_type != PT_SELECT)
	    {
	      mq_set_union_query (parser, node->info.query.q.union_.arg1,
				  PT_IS_UNION_QUERY);
	      mq_set_union_query (parser, node->info.query.q.union_.arg2,
				  PT_IS_UNION_QUERY);
	    }
	}

      if (node)
	{
	  /* mq_optimize works for queries only. Queries generated
	   * for update, insert or delete will go thru this path
	   * when mq_translate is called, so will still get this
	   * optimization step applied. */
	  node = mq_optimize (parser, node);
	}
      break;

    case PT_INSERT:
    case PT_DELETE:
    case PT_UPDATE:
      /*
       * The mq_push_paths will convert the expression as CNF. if subquery is
       * in the expression, it may be copied several times. To avoid repeatedly
       * convert the expressions in subqueries and improve performance, we
       * put mq_push_paths as post function.
       */
      node = parser_walk_tree (parser, node, NULL, NULL, mq_push_paths, NULL);
      node = parser_walk_tree (parser, node, NULL, NULL, mq_translate_local,
			       NULL);

      node = parser_walk_tree (parser, node,
			       mq_mark_location, NULL,
			       mq_check_non_updatable_vclass_oid, NULL);

      if (pt_has_error (parser))
	{
	  goto exit_on_error;
	}

      if (node)
	{
	  node = mq_optimize (parser, node);
	}
      break;

    default:
      break;
    }

  /* process FOR UPDATE clause */
  err = pt_for_update_prepare_query (parser, node);

  /* restore link */
  if (node)
    {
      node->next = next;
    }

  if (err != NO_ERROR || pt_has_error (parser))
    {
      goto exit_on_error;
    }

  return node;

exit_on_error:

  return NULL;
}

/*
 * pt_check_for_update_subquery() - check if there is a subquery with
 *				    FOR UPDATE clause.
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in/out): an address of an int. 0 for root, 1 if not root and error
 *                code if a subquery with FOR UPDATE clause was found.
 *   continue_walk(in):
 */
static PT_NODE *
pt_check_for_update_subquery (PARSER_CONTEXT * parser, PT_NODE * node,
			      void *arg, int *continue_walk)
{
  if (!*(int *) arg)
    {
      /* Processed the root node. All remaining PT_SELECT nodes are
         subqueries */
      *(int *) arg = 1;
    }
  else if (node->node_type == PT_SELECT)
    {
      if (PT_SELECT_INFO_IS_FLAGED (node, PT_SELECT_INFO_FOR_UPDATE))
	{
	  /* found a subquery with FOR UPDATE clause */
	  PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
		     MSGCAT_SEMANTIC_INVALID_USE_FOR_UPDATE_CLAUSE);
	  *(int *) arg = ER_FAILED;
	  *continue_walk = PT_STOP_WALK;
	  return node;
	}
    }

  return node;
}

/*
 * pt_check_for_update_clause() - check the query for invalid use of FOR UPDATE
 *				  clause
 *   return: NO_ERROR or error code
 *   parser(in):
 *   query(in): statement to be checked
 *   root(in): true if this is the main statement and false otherwise.
 *
 *  NOTE: Always call this function with root set to true. false value is used
 *	  internally.
 */
static int
pt_check_for_update_clause (PARSER_CONTEXT * parser, PT_NODE * query,
			    bool root)
{
  int error_code = 0;
  PT_NODE *spec = NULL, *next = NULL;

  if (query == NULL)
    {
      return NO_ERROR;
    }

  /* check for subqueries with FOR UPDATE clause */
  if (root)
    {
      next = query->next;
      query->next = NULL;
      parser_walk_tree (parser, query, pt_check_for_update_subquery,
			&error_code, NULL, NULL);
      query->next = next;
      if (error_code < 0)
	{
	  return error_code;
	}
    }

  /* FOR UPDATE is availbale only in SELECT statements */
  if (query->node_type != PT_SELECT)
    {
      if (!root && PT_IS_QUERY (query))
	{
	  PT_ERRORm (parser, query, MSGCAT_SET_PARSER_SEMANTIC,
		     MSGCAT_SEMANTIC_INVALID_USE_FOR_UPDATE_CLAUSE);
	  return ER_FAILED;
	}
      return NO_ERROR;
    }

  /* Skip check if this is not a query with FOR UPDATE clause */
  if (root && !PT_SELECT_INFO_IS_FLAGED (query, PT_SELECT_INFO_FOR_UPDATE))
    {
      return NO_ERROR;
    }

  /* check for aggregate functions, GROUP BY and DISTINCT */
  if (pt_has_aggregate (parser, query) || pt_is_distinct (query))
    {
      PT_ERRORm (parser, query, MSGCAT_SET_PARSER_SEMANTIC,
		 MSGCAT_SEMANTIC_INVALID_USE_FOR_UPDATE_CLAUSE);
      return ER_FAILED;
    }

  /* check derived tables */
  for (spec = query->info.query.q.select.from; spec != NULL;
       spec = spec->next)
    {
      if ((!root || (spec->info.spec.flag & PT_SPEC_FLAG_FOR_UPDATE_CLAUSE))
	  && spec->info.spec.derived_table != NULL)
	{
	  error_code =
	    pt_check_for_update_clause (parser,
					spec->info.spec.derived_table, false);
	  if (error_code != NO_ERROR)
	    {
	      return error_code;
	    }
	}
    }

  return NO_ERROR;
}

/*
 * pt_for_update_prepare_query() - check FOR UPDATE clause and reflag specs from
 *				   FOR UPDATE
 *   return: returns the modified query.
 *   parser(in):
 *   query(in/out): query with FOR UPDATE clause for which the check and spec
 *		    flagging is made.
 */
static int
pt_for_update_prepare_query (PARSER_CONTEXT * parser, PT_NODE * query)
{
  int err = NO_ERROR;
  PT_NODE *spec = NULL;

  if (query == NULL)
    {
      return NO_ERROR;
    }

  err = pt_check_for_update_clause (parser, query, true);
  if (err != NO_ERROR)
    {
      return err;
    }

  if (query->node_type != PT_SELECT
      || !PT_SELECT_INFO_IS_FLAGED (query, PT_SELECT_INFO_FOR_UPDATE))
    {
      return NO_ERROR;
    }

  for (spec = query->info.query.q.select.from; spec != NULL;
       spec = spec->next)
    {
      if (spec->info.spec.derived_table == NULL)
	{
	  spec->info.spec.flag |= PT_SPEC_FLAG_FOR_UPDATE_CLAUSE;
	}
    }

  assert (err == NO_ERROR);

  return err;
}

/*
 * mq_translate() - expands each query against a view or virtual class
 *   return:
 *   parser(in):
 *   node(in):
 */
PT_NODE *
mq_translate (PARSER_CONTEXT * parser, PT_NODE * volatile node)
{
  volatile PT_NODE *return_node = NULL;

  if (!node)
    {
      return NULL;
    }

  /* set up an environment for longjump to return to if there is an out
   * of memory error in pt_memory.c. DO NOT RETURN unless PT_CLEAR_JMP_ENV
   * is called to clear the environment.
   */
  PT_SET_JMP_ENV (parser);

  return_node = mq_translate_helper (parser, node);

  PT_CLEAR_JMP_ENV (parser);

  return (PT_NODE *) return_node;
}



/*
 *
 * Function group:
 * Functions for the translation of virtual queries
 *
 */


/*
 * pt_lookup_symbol() -
 *   return: symbol we are looking for, or NULL if not found
 *   parser(in):
 *   attr_list(in): attribute list to look for attr in
 *   attr(in): attr to look for
 */
static PT_NODE *
mq_lookup_symbol (PARSER_CONTEXT * parser, PT_NODE * attr_list,
		  PT_NODE * attr)
{
  PT_NODE *list;

  if (!attr || attr->node_type != PT_NAME)
    {
      PT_INTERNAL_ERROR (parser, "resolution");
      return NULL;
    }

  for (list = attr_list;
       (list != NULL) && (!pt_name_equal (parser, list, attr));
       list = list->next)
    {
      ;				/* do nothing */
    }

  return list;
}

/*
 * mq_insert_symbol() - appends the symbol to the entities
 *   return: none
 *   parser(in): parser environment
 *   listhead(in/out): entity_spec to add symbol to
 *   attr(in): the attribute to add to the symbol table
 */
static void
mq_insert_symbol (PARSER_CONTEXT * parser, PT_NODE ** listhead,
		  PT_NODE * attr)
{
  PT_NODE *new_node;

  if (!attr || attr->node_type != PT_NAME)
    {
      PT_INTERNAL_ERROR (parser, "translate");
      return;
    }

  new_node = mq_lookup_symbol (parser, *listhead, attr);
  if (new_node == NULL)
    {
      new_node = parser_copy_tree (parser, attr);
      *listhead = parser_append_node (new_node, *listhead);
    }
}

/*
 * mq_generate_name() - generates printable names
 *   return:
 *   parser(in):
 *   root(in):
 *   version(in):
 */
const char *
mq_generate_name (PARSER_CONTEXT * parser, const char *root, int *version)
{
  const char *generatedname;
  char temp[20];

  (*version)++;

  sprintf (temp, "_%d", *version);

  /* avoid "stepping" on root */
  generatedname = pt_append_string (parser,
				    pt_append_string (parser, NULL, root),
				    temp);
  return generatedname;
}

/*
 * mq_coerce_resolved() - re-sets PT_NAME node resolution to match
 *                        a new printable name
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in/out):
 */
static PT_NODE *
mq_coerce_resolved (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		    void *void_arg, int *continue_walk)
{
  PT_NODE *range = (PT_NODE *) void_arg;
  *continue_walk = PT_CONTINUE_WALK;

  /* if its not a name, leave it alone */
  if (node->node_type == PT_NAME)
    {

      if (node->info.name.spec_id == range->info.name.spec_id	/* same entity spec */
	  && node->info.name.resolved	/* and has a resolved name, */
	  && node->info.name.meta_class != PT_CLASS
	  && node->info.name.meta_class != PT_VCLASS)
	{
	  /* set the attribute resolved name */
	  node->info.name.resolved = range->info.name.original;
	}

      /* sub nodes of PT_NAME are not names with range variables */
      *continue_walk = PT_LIST_WALK;
    }
  else if (node->node_type == PT_SPEC
	   && node->info.spec.id == range->info.name.spec_id)
    {
      PT_NODE *flat = node->info.spec.flat_entity_list;
      /* sub nodes of PT_SPEC include flat class lists with
       * range variables. Set them even though they are "class" names.
       */

      for (; flat != NULL; flat = flat->next)
	{
	  flat->info.name.resolved = range->info.name.original;
	}
    }

  return node;
}


/*
 * mq_reset_all_ids() - re-sets PT_NAME node ids
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_reset_all_ids (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		  void *void_arg, UNUSED_ARG int *continue_walk)
{
  PT_NODE *spec = (PT_NODE *) void_arg;

  if (node->node_type == PT_NAME
      && node->info.name.spec_id == spec->info.spec.id)
    {
      node->info.name.spec_id = (UINTPTR) spec;
      if (node->info.name.resolved	/* has a resolved name */
	  && node->info.name.meta_class != PT_CLASS
	  && node->info.name.meta_class != PT_VCLASS)
	{
	  /* set the attribute resolved name */
	  node->info.name.resolved =
	    spec->info.spec.range_var->info.name.original;
	}

    }

  if (node->spec_ident == spec->info.spec.id)
    {
      node->spec_ident = (UINTPTR) spec;
    }

  return node;
}


/*
 * mq_reset_ids() - re-sets path entities of a spec by removing unreferenced
 *         paths, reseting ids of remaining paths, and recursing on sub-paths
 *   return:
 *   parser(in):
 *   statement(in):
 *   spec(in):
 */
PT_NODE *
mq_reset_ids (PARSER_CONTEXT * parser, PT_NODE * statement, PT_NODE * spec)
{
  PT_NODE *range;

  /* make sure range var always has same id as spec */
  range = spec->info.spec.range_var;
  if (range)
    {
      assert (range->node_type == PT_NAME);
      range->info.name.spec_id = spec->info.spec.id;
    }

  statement =
    parser_walk_tree (parser, statement, mq_reset_all_ids, spec, NULL, NULL);

  /* spec may or may not be part of statement. If it is, this is
     redundant. If its not, this will reset self references, such
     as in path specs. */
  (void) parser_walk_tree (parser, spec, mq_reset_all_ids, spec, NULL, NULL);

  /* finally, set spec id */
  spec->info.spec.id = (UINTPTR) spec;

  return statement;
}

/*
 * mq_clear_all_ids() - clear previously resolved PT_NAME node
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_clear_all_ids (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		  void *void_arg, int *continue_walk)
{
  UINTPTR *spec_id_ptr = (UINTPTR *) void_arg;

  if (node->node_type == PT_NAME)
    {
      if ((spec_id_ptr != NULL && node->info.name.spec_id == (*spec_id_ptr))
	  || (spec_id_ptr == NULL))
	{
	  node->info.name.spec_id = 0;
	}
    }

  if (pt_is_query (node))
    {
      *continue_walk = PT_LIST_WALK;
    }
  else
    {
      *continue_walk = PT_CONTINUE_WALK;
    }

  return node;
}

/*
 * mq_clear_other_ids () - clear ids for all nodes except the ones referencing
 *			   the spec list specified in void_arg
 * return : node
 * parser (in) : parser context
 * node (in)   : node to reset
 * void_arg (in) : spec list
 * continue_walk (in) :
 */
static PT_NODE *
mq_clear_other_ids (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		    void *void_arg, UNUSED_ARG int *continue_walk)
{
  if (node->node_type == PT_NAME)
    {
      PT_NODE *filter_spec = (PT_NODE *) void_arg;
      while (filter_spec != NULL)
	{
	  if (node->info.name.spec_id == filter_spec->info.spec.id)
	    {
	      return node;
	    }
	  filter_spec = filter_spec->next;
	}
      node->info.name.spec_id = 0;
    }
  return node;
}

/*
 * mq_clear_ids () - recursively clear previously resolved PT_NAME nodes
 *   return:
 *   parser(in):
 *   statement(in):
 *   spec(in):
 */
PT_NODE *
mq_clear_ids (PARSER_CONTEXT * parser, PT_NODE * node, PT_NODE * spec)
{
  node = parser_walk_tree (parser, node, mq_clear_all_ids,
			   (spec != NULL ? &spec->info.spec.id : NULL),
			   pt_continue_walk, NULL);

  return node;
}

/*
 * mq_reset_spec_ids() - resets spec ids for a spec node
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_reset_spec_ids (PARSER_CONTEXT * parser, PT_NODE * node,
		   UNUSED_ARG void *void_arg, UNUSED_ARG int *continue_walk)
{
  PT_NODE *spec = NULL;

  switch (node->node_type)
    {
    case PT_SELECT:
      for (spec = node->info.query.q.select.from; spec; spec = spec->next)
	{
	  /* might not be necessary to reset paths and references, but it's
	     a good failsafe */
	  mq_set_references (parser, node, spec);
	}
      break;

    case PT_UPDATE:
      for (spec = node->info.update.spec; spec; spec = spec->next)
	{
	  /* only reset IDs, in case query will be rewritten as SELECT for
	     broker execution */
	  mq_reset_ids (parser, node, spec);
	}
      break;

    case PT_DELETE:
      for (spec = node->info.delete_.spec; spec; spec = spec->next)
	{
	  /* only reset IDs, in case query will be rewritten as SELECT for
	     broker execution */
	  mq_reset_ids (parser, node, spec);
	}
      break;

    case PT_INSERT:
      if (node->info.insert.odku_assignments)
	{
	  PT_NODE *values = node->info.insert.value_clauses;
	  PT_NODE *select = values->info.node_list.list;

	  if (select != NULL && select->node_type == PT_SELECT)
	    {
	      PT_NODE *assignments = node->info.insert.odku_assignments;

	      spec = select->info.query.q.select.from;
	      for (; spec; spec = spec->next)
		{
		  for (; assignments; assignments = assignments->next)
		    {
		      parser_walk_tree (parser, assignments, mq_reset_all_ids,
					spec, NULL, NULL);
		    }
		}
	    }
	}
      break;

    default:
      break;
    }

  return (node);

}

/*
 * mq_reset_ids_in_statement() - walks the statement and for each spec,
 *                               reset ids that reference that spec
 *   return:
 *   parser(in):
 *   statement(in):
 */
PT_NODE *
mq_reset_ids_in_statement (PARSER_CONTEXT * parser, PT_NODE * statement)
{

  statement = parser_walk_tree (parser, statement, mq_reset_spec_ids, NULL,
				NULL, NULL);

  return (statement);

}

/*
 * mq_get_references_node() - gets referenced PT_NAME nodes
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in/out):
 */
static PT_NODE *
mq_get_references_node (PARSER_CONTEXT * parser, PT_NODE * node,
			void *void_arg, int *continue_walk)
{
  PT_NODE *spec = (PT_NODE *) void_arg;

  if (node->node_type == PT_NAME
      && node->info.name.spec_id == spec->info.spec.id)
    {
      node->info.name.spec_id = (UINTPTR) spec;

      if (node->info.name.meta_class != PT_HINT_NAME
	  && node->info.name.meta_class != PT_INDEX_NAME)
	{
	  /* filter out hint argument name, index name nodes */
	  mq_insert_symbol (parser, &spec->info.spec.referenced_attrs, node);
	}
    }

  if (node->node_type == PT_SPEC)
    {
      /* The only part of a spec node that could contain references to
       * the given spec_id are derived tables and on_cond.
       * All the rest of the name nodes for the spec are not references,
       * but range variables, class names, etc.
       * We don't want to mess with these. We'll handle the ones that
       * we want by hand. */
      node->info.spec.derived_table =
	parser_walk_tree (parser, node->info.spec.derived_table,
			  mq_get_references_node, spec, pt_continue_walk,
			  NULL);
      node->info.spec.on_cond =
	parser_walk_tree (parser, node->info.spec.on_cond,
			  mq_get_references_node, spec, pt_continue_walk,
			  NULL);
      /* don't visit any other leaf nodes */
      *continue_walk = PT_LIST_WALK;
    }

  /* Data type nodes can not contain any valid references.  They do
     contain class names and other things we don't want. */
  if (node->node_type == PT_DATA_TYPE)
    {
      *continue_walk = PT_LIST_WALK;
    }

  if (node->spec_ident == spec->info.spec.id)
    {
      node->spec_ident = (UINTPTR) spec;
    }

  return node;
}


/*
 * mq_reset_ids_and_references() - re-sets path entities of a spec by
 *      removing unreferenced paths, reseting ids of remaining paths,
 *      and recursing on sub-paths
 *   return:
 *   parser(in):
 *   statement(in):
 *   spec(in):
 */
static PT_NODE *
mq_reset_ids_and_references (PARSER_CONTEXT * parser, PT_NODE * statement,
			     PT_NODE * spec)
{
  return mq_reset_ids_and_references_helper (parser, statement, spec,
					     true /* default */ );
}

/*
 * mq_reset_ids_and_references_helper() -
 *   return:
 *   parser(in):
 *   statement(in):
 *   spec(in):
 *   get_spec_referenced_attr(in):
 */
static PT_NODE *
mq_reset_ids_and_references_helper (PARSER_CONTEXT * parser,
				    PT_NODE * statement, PT_NODE * spec,
				    bool get_spec_referenced_attr)
{
  statement = mq_reset_ids (parser, statement, spec);

  parser_free_tree (parser, spec->info.spec.referenced_attrs);
  spec->info.spec.referenced_attrs = NULL;

  statement = parser_walk_tree (parser, statement, mq_get_references_node,
				spec, pt_continue_walk, NULL);

  /* spec may or may not be part of statement. If it is, this is
     redundant. If its not, this will reset catch self references, such
     as in path specs. */
  if (get_spec_referenced_attr)
    {
      (void) parser_walk_tree (parser, spec, mq_get_references_node,
			       spec, pt_continue_walk, NULL);
    }

  return statement;
}


/*
 * mq_get_references() - returns a copy of a list of referenced names for
 *                       the given entity spec
 *   return:
 *   parser(in):
 *   statement(in):
 *   spec(in):
 */
PT_NODE *
mq_get_references (PARSER_CONTEXT * parser, PT_NODE * statement,
		   PT_NODE * spec)
{
  return mq_get_references_helper (parser, statement, spec,
				   true /* default */ );
}

/*
 * mq_get_references_helper() -
 *   return:
 *   parser(in):
 *   statement(in):
 *   spec(in):
 *   get_spec_referenced_attr(in):
 */
PT_NODE *
mq_get_references_helper (PARSER_CONTEXT * parser, PT_NODE * statement,
			  PT_NODE * spec, bool get_spec_referenced_attr)
{
  PT_NODE *references;

  (void) mq_reset_ids_and_references_helper (parser, statement, spec,
					     get_spec_referenced_attr);

  references = spec->info.spec.referenced_attrs;
  spec->info.spec.referenced_attrs = NULL;

  return references;
}


/*
 * mq_set_references_local() - sets the referenced attr list of entity
 *                             specifications and its sub-entities
 *   return:
 *   parser(in):
 *   statement(in):
 *   spec(in):
 */
static PT_NODE *
mq_set_references_local (PARSER_CONTEXT * parser, PT_NODE * statement,
			 PT_NODE * spec)
{
  parser_free_tree (parser, spec->info.spec.referenced_attrs);
  spec->info.spec.referenced_attrs = NULL;

  statement = parser_walk_tree (parser, statement, mq_get_references_node,
				spec, pt_continue_walk, NULL);

  return statement;
}


/*
 * mq_set_references() - sets the referenced attr list of an entity
 *                       specification and all sub-entities
 *   return:
 *   parser(in):
 *   statement(in):
 *   spec(in):
 */
PT_NODE *
mq_set_references (PARSER_CONTEXT * parser, PT_NODE * statement,
		   PT_NODE * spec)
{
  /* don't mess with pseudo specs */
  if (!spec)
    {
      return statement;
    }

  statement = mq_reset_ids (parser, statement, spec);

  statement = mq_set_references_local (parser, statement, spec);

  return statement;
}


/*
 * mq_reset_select_spec_node() - re-sets copied spec symbol table information
 * for a select which has just been substituted as a lambda argument in a view
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_reset_select_spec_node (PARSER_CONTEXT * parser, PT_NODE * node,
			   void *void_arg, UNUSED_ARG int *continue_walk)
{
  PT_RESET_SELECT_SPEC_INFO *info = (PT_RESET_SELECT_SPEC_INFO *) void_arg;

  if (node->node_type == PT_SPEC && node->info.spec.id == info->id)
    {
      *info->statement =
	mq_reset_ids_and_references (parser, *info->statement, node);
    }

  return node;
}

/*
 * mq_reset_select_specs() - re-sets spec symbol table information for a select
 *      which has just been substituted as a lambda argument in a view
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_reset_select_specs (PARSER_CONTEXT * parser, PT_NODE * node,
		       void *void_arg, UNUSED_ARG int *continue_walk)
{
  PT_NODE **statement = (PT_NODE **) void_arg;
  PT_RESET_SELECT_SPEC_INFO info;
  PT_NODE *spec;

  if (node->node_type == PT_SELECT)
    {
      spec = node->info.query.q.select.from;
      info.statement = statement;
      for (; spec != NULL; spec = spec->next)
	{
	  info.id = spec->info.spec.id;

	  /* now we know which specs must get reset.
	   * we need to find each instance of this spec in the
	   * statement, and reset it. */
	  *statement = parser_walk_tree (parser, *statement,
					 mq_reset_select_spec_node, &info,
					 NULL, NULL);
	}
    }

  return node;
}


/*
 * mq_reset_specs_from_column() - finds every select in column, then resets
 *                                id's and paths from that selects spec
 *   return:
 *   parser(in):
 *   statement(in):
 *   column(in):
 */
static PT_NODE *
mq_reset_specs_from_column (PARSER_CONTEXT * parser, PT_NODE * statement,
			    PT_NODE * column)
{
  parser_walk_tree (parser, column, mq_reset_select_specs, &statement, NULL,
		    NULL);

  return statement;
}


/*
 * mq_reset_spec_distr_subpath_pre() - moving specs from the sub-path list to
 *      the immediate path_entities list, and resetting ids in the statement
 *   return:
 *   parser(in):
 *   spec(in):
 *   void_arg(in):
 *   continue_walk(in/out):
 */
static PT_NODE *
mq_reset_spec_distr_subpath_pre (UNUSED_ARG PARSER_CONTEXT * parser,
				 PT_NODE * spec, void *void_arg,
				 int *continue_walk)
{
  SPEC_RESET_INFO *info = (SPEC_RESET_INFO *) void_arg;

  if (spec == info->old_next)
    {
      *continue_walk = PT_STOP_WALK;
    }
  else
    {
      *continue_walk = PT_CONTINUE_WALK;
    }

  return spec;
}

/*
 * mq_reset_spec_distr_subpath_post() -
 *   return:
 *   parser(in):
 *   spec(in):
 *   void_arg(in):
 *   continue_walk(in/out):
 */
static PT_NODE *
mq_reset_spec_distr_subpath_post (PARSER_CONTEXT * parser, PT_NODE * spec,
				  void *void_arg, int *continue_walk)
{
  SPEC_RESET_INFO *info = (SPEC_RESET_INFO *) void_arg;

  *continue_walk = PT_CONTINUE_WALK;	/* un-prune other sub-branches */

  if (spec != info->old_next && spec->node_type == PT_SPEC)
    {
      /* now that the sub-specs (if any) are attached, we can reset spec_ids
       * and references.
       */
      info->statement =
	mq_reset_ids_and_references (parser, info->statement, spec);
    }

  return spec;
}


/*
 * mq_rename_resolved() - re-sets name resolution to of an entity spec
 *                        and a tree to match a new printable name
 *   return:
 *   parser(in):
 *   spec(in):
 *   statement(in):
 *   newname(in):
 */
static PT_NODE *
mq_rename_resolved (PARSER_CONTEXT * parser, PT_NODE * spec,
		    PT_NODE * statement, const char *newname)
{
  if (!spec || !spec->info.spec.range_var || !statement)
    {
      return statement;
    }

  spec->info.spec.range_var->info.name.original = newname;

  /* this is just to make sure the id is properly set.
     Its probably not necessary.  */
  spec->info.spec.range_var->info.name.spec_id = spec->info.spec.id;

  statement = parser_walk_tree (parser, statement, mq_coerce_resolved,
				spec->info.spec.range_var, pt_continue_walk,
				NULL);

  return statement;
}

/*
 * mq_regenerate_if_ambiguous() - regenerate the exposed name
 *                                if ambiguity is detected
 *   return:
 *   parser(in):
 *   spec(in):
 *   statement(in):
 *   from(in):
 */
PT_NODE *
mq_regenerate_if_ambiguous (PARSER_CONTEXT * parser, PT_NODE * spec,
			    PT_NODE * statement, PT_NODE * from)
{
  const char *newexposedname;
  const char *generatedname;
  int ambiguous;
  int i;


  newexposedname = spec->info.spec.range_var->info.name.original;

  if (1 < pt_name_occurs_in_from_list (parser, newexposedname, from))
    {
      /* Ambiguity is detected. rename the newcomer's
       * printable name to fix this.
       */
      i = 0;
      ambiguous = true;

      while (ambiguous)
	{
	  generatedname = mq_generate_name (parser, newexposedname, &i);

	  ambiguous = 0 < pt_name_occurs_in_from_list
	    (parser, generatedname, from);
	}

      /* generatedname is now non-ambiguous */
      statement = mq_rename_resolved (parser, spec, statement, generatedname);
    }

  return statement;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * mq_invert_insert_select() - invert sub-query select lists
 *   return:
 *   parser(in):
 *   attr(in):
 *   subquery(in):
 */
static void
mq_invert_insert_select (PARSER_CONTEXT * parser, PT_NODE * attr,
			 PT_NODE * subquery)
{
  PT_NODE **value;
  PT_NODE *value_next;
  PT_NODE *result;

  value = &subquery->info.query.q.select.list;

  while (*value)
    {
      /* ignore the the hidden columns
       * e.g. append when check order by
       *      see mq_update_order_by (...)
       */
      if ((*value)->is_hidden_column == 1)
	{
	  value = &(*value)->next;
	  continue;
	}

      if (!attr)
	{
	  /* system error, should be caught in semantic pass */
	  PT_ERRORm (parser, (*value), MSGCAT_SET_PARSER_RUNTIME,
		     MSGCAT_RUNTIME_ATTRS_GT_QSPEC_COLS);
	  return;
	}
      value_next = (*value)->next;
      (*value)->next = NULL;

      (*value) = mq_translate_value (parser, *value);

      result = pt_invert (parser, attr, *value);
      if (result == NULL)
	{
	  /* error not invertable/updatable */
	  /* don't want to repeat this error */
	  if (!pt_has_error (parser))
	    {
	      PT_ERRORmf (parser, attr, MSGCAT_SET_PARSER_RUNTIME,
			  MSGCAT_RUNTIME_VASG_TGT_UNINVERTBL,
			  pt_short_print (parser, attr));
	    }
	  return;
	}

      if (result->next)
	{
	  parser_free_tree (parser, result->next);
	}

      result->next = NULL;
      (*value) = result;	/* the right hand side */

      attr = attr->next;
      (*value)->next = value_next;

      value = &(*value)->next;
    }
}

/*
 * mq_invert_insert_subquery() - invert sub-query select lists
 *   return:
 *   parser(in):
 *   attr(in):
 *   subquery(in):
 */
static void
mq_invert_insert_subquery (PARSER_CONTEXT * parser, PT_NODE ** attr,
			   PT_NODE * subquery)
{
  PT_NODE *attr_next;
  PT_NODE *result;

  switch (subquery->node_type)
    {
    case PT_SELECT:
      mq_invert_insert_select (parser, *attr, subquery);
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      mq_invert_insert_subquery (parser, attr,
				 subquery->info.query.q.union_.arg1);
      if (!pt_has_error (parser))
	{
	  mq_invert_insert_subquery (parser, attr,
				     subquery->info.query.q.union_.arg2);
	}
      break;

    default:
      /* should not get here, that is an error! */
      /* its almost certainly recoverable, so ignore it */
      assert (0);
      break;
    }

  while (*attr && !pt_has_error (parser))
    {
      attr_next = (*attr)->next;
      (*attr)->next = NULL;

      pt_find_var (*attr, &result);

      if (!result)
	{
	  /* error not invertable/updatable already set */
	  return;
	}

      (*attr) = result;		/* the name */

      (*attr)->next = attr_next;

      attr = &(*attr)->next;
    }
}
#endif

/*
 * mq_make_derived_spec() -
 *   return:
 *   parser(in):
 *   node(in):
 *   subquery(in):
 *   idx(in):
 *   spec_ptr(out):
 *   attr_list_ptr(out):
 */
PT_NODE *
mq_make_derived_spec (PARSER_CONTEXT * parser, PT_NODE * node,
		      PT_NODE * subquery, int *idx, PT_NODE ** spec_ptr,
		      PT_NODE ** attr_list_ptr)
{
  PT_NODE *range, *spec, *as_attr_list, *col, *next, *tmp;

  /* remove unnecessary ORDER BY clause.
     if select list has orderby_num(), can not remove ORDER BY clause
     for example: (i, j) = (select i, orderby_num() from t order by i) */
  if (subquery->info.query.orderby_for == NULL
      && subquery->info.query.order_by)
    {
      for (col = pt_get_select_list (parser, subquery); col; col = col->next)
	{
	  if (col->node_type == PT_EXPR
	      && col->info.expr.op == PT_ORDERBY_NUM)
	    {
	      break;		/* can not remove ORDER BY clause */
	    }
	}

      if (!col)
	{
	  parser_free_tree (parser, subquery->info.query.order_by);
	  subquery->info.query.order_by = NULL;
	  subquery->info.query.order_siblings = 0;

	  for (col = pt_get_select_list (parser, subquery);
	       col && col->next; col = next)
	    {
	      next = col->next;
	      if (next->is_hidden_column)
		{
		  parser_free_tree (parser, next);
		  col->next = NULL;
		  break;
		}
	    }
	}
    }

  /* set line number to range name */
  range = pt_name (parser, "av1861");

  /* construct new spec */
  spec = parser_new_node (parser, PT_SPEC);

  if (spec == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return NULL;
    }

  spec->info.spec.derived_table = subquery;
  spec->info.spec.derived_table = mq_reset_ids_in_statement (parser,
							     spec->info.spec.
							     derived_table);
  spec->info.spec.derived_table_type = PT_IS_SUBQUERY;
  spec->info.spec.range_var = range;
  spec->info.spec.id = (UINTPTR) spec;
  range->info.name.spec_id = (UINTPTR) spec;

  /* add new spec to the spec list */
  node->info.query.q.select.from = parser_append_node (spec,
						       node->info.query.q.
						       select.from);
  /* set spec as unique */
  node = mq_regenerate_if_ambiguous (parser, spec, node,
				     node->info.query.q.select.from);

  /* construct new attr_list */
  spec->info.spec.as_attr_list = as_attr_list = NULL;	/* init */
  for (col = pt_get_select_list (parser, subquery); col; col = col->next)
    {
      tmp = pt_name (parser, mq_generate_name (parser, "av", idx));
      tmp->info.name.meta_class = PT_NORMAL;
      tmp->info.name.resolved = spec->info.spec.range_var->info.name.original;
      tmp->info.name.spec_id = spec->info.spec.id;
      tmp->type_enum = col->type_enum;
      tmp->data_type = parser_copy_tree (parser, col->data_type);
      PT_NAME_INFO_SET_FLAG (tmp, PT_NAME_GENERATED_DERIVED_SPEC);
      /* keep out hidden columns from derived select list */
      if (subquery->info.query.order_by && col->is_hidden_column)
	{
	  col->is_hidden_column = 0;
	  tmp->is_hidden_column = 0;
	  spec->info.spec.as_attr_list =
	    parser_append_node (tmp, spec->info.spec.as_attr_list);
	}
      else
	{
	  spec->info.spec.as_attr_list =
	    parser_append_node (tmp, spec->info.spec.as_attr_list);
	  as_attr_list =
	    parser_append_node (parser_copy_tree (parser, tmp), as_attr_list);
	}
    }

  /* save spec, attr */
  if (spec_ptr)
    {
      *spec_ptr = spec;
    }

  if (attr_list_ptr)
    {
      *attr_list_ptr = as_attr_list;
    }

  return node;
}				/* mq_make_derived_spec */

/*
 * mq_class_lambda() - replace class specifiers with their corresponding
 *                     virtual from list
 *   return:
 *   parser(in):
 *   statement(in):
 *   class(in):
 *   corresponding_spec(in):
 *   class_where_part(in):
 *   class_group_by_part(in):
 *   class_having_part(in):
 *
 * Note:
 * A subset of general statements is handled, being
 *      select - replace the "entity_spec" node in from list
 *               containing class in its flat_entity_list
 *               append the where_part, if any.
 *      update - replace the "entity_spec" node in entity_spec
 *               if it contains class in its flat_entity_list
 *               append the where_part, if any.
 *      insert - replace the "name" node equal to class
 *      union, difference, intersection
 *             - the recursive result of this function on both arguments.
 */
static PT_NODE *
mq_class_lambda (PARSER_CONTEXT * parser, PT_NODE * statement,
		 PT_NODE * class_, PT_NODE * corresponding_spec,
		 PT_NODE * class_where_part,
		 PT_NODE * class_group_by_part, PT_NODE * class_having_part)
{
  PT_NODE *spec;
  PT_NODE **specptr = NULL;
  PT_NODE **where_part = NULL;
  PT_NODE *newspec = NULL;
  PT_NODE *oldnext = NULL;
  PT_NODE *assign, *result;
  PT_NODE *attr_names = NULL;
  bool for_update = false;
  PT_NODE **lhs, **rhs, *lhs_next, *rhs_next;
  const char *newresolved = class_->info.name.resolved;

  if (statement == NULL)
    {
      return NULL;
    }

  switch (statement->node_type)
    {
    case PT_SELECT:
      statement->info.query.is_subquery = PT_IS_SUBQUERY;

      specptr = &statement->info.query.q.select.from;
      where_part = &statement->info.query.q.select.where;

      if (class_group_by_part || class_having_part)
	{
	  /* check for derived */
	  if (statement->info.query.vspec_as_derived == 1)
	    {
	      /* set GROUP BY */
	      if (class_group_by_part)
		{
		  if (statement->info.query.q.select.group_by)
		    {
		      /* this is impossible case. give up */
		      goto exit_on_error;
		    }
		  else
		    {
		      statement->info.query.q.select.group_by =
			parser_copy_tree_list (parser, class_group_by_part);
		    }
		}

	      /* merge HAVING */
	      if (class_having_part)
		{
		  PT_NODE **having_part;

		  having_part = &statement->info.query.q.select.having;

		  *having_part =
		    parser_append_node (parser_copy_tree_list
					(parser, class_having_part),
					*having_part);
		}
	    }
	  else
	    {
	      /* system error */
	      goto exit_on_error;
	    }
	}

      break;

    case PT_UPDATE:
      specptr = &statement->info.update.spec;
      where_part = &statement->info.update.search_cond;

      for (assign = statement->info.update.assignment; assign != NULL;
	   assign = assign->next)
	{
	  /* get lhs, rhs */
	  lhs = &(assign->info.expr.arg1);
	  rhs = &(assign->info.expr.arg2);
	  if (pt_is_query (*rhs))
	    {
	      /* multi-column update with subquery */
	      rhs = &((*rhs)->info.query.q.select.list);
	    }

	  for (; *lhs && *rhs; *lhs = lhs_next, *rhs = rhs_next)
	    {
	      /* cut-off and save next link */
	      lhs_next = (*lhs)->next;
	      (*lhs)->next = NULL;
	      rhs_next = (*rhs)->next;
	      (*rhs)->next = NULL;

	      *rhs = mq_translate_value (parser, *rhs);

	      result = pt_invert (parser, *lhs, *rhs);
	      if (result == NULL)
		{
		  /* error not invertible/updatable */
		  PT_ERRORmf (parser, assign, MSGCAT_SET_PARSER_RUNTIME,
			      MSGCAT_RUNTIME_VASG_TGT_UNINVERTBL,
			      pt_short_print (parser, *lhs));
		  goto exit_on_error;
		}

	      if (*lhs)
		{
		  parser_free_tree (parser, *lhs);
		}
	      *lhs = result->next;	/* the name */
	      result->next = NULL;
	      *rhs = result;	/* the right hand side */

	      lhs = &((*lhs)->next);
	      rhs = &((*rhs)->next);
	    }
	}
      break;

    case PT_DELETE:
      specptr = &statement->info.delete_.spec;
      where_part = &statement->info.delete_.search_cond;
      break;

#if 0				/* this is impossible case */
    case PT_INSERT:
      specptr = &statement->info.insert.spec;

      crt_list = statement->info.insert.value_clauses;
      if (crt_list->info.node_list.list_type == PT_IS_DEFAULT_VALUE
	  || crt_list->info.node_list.list_type == PT_IS_VALUE)
	{
	  for (; crt_list != NULL; crt_list = crt_list->next)
	    {
	      /* Inserting the default values in the original class will
	         "insert" the default view values in the view. We don't need
	         to do anything. */
	      if (crt_list->info.node_list.list_type == PT_IS_DEFAULT_VALUE)
		{
		  continue;
		}
	      assert (crt_list->info.node_list.list_type == PT_IS_VALUE);

	      /* We need to invert expressions now. */
	      if (attr_names == NULL)
		{
		  /* We'll also build a list of attribute names. */
		  build_att_names_list = true;
		}
	      else
		{
		  /* The list of attribute names has already been built. */
		  build_att_names_list = false;
		}

	      attr = pt_attrs_part (statement);
	      value = &crt_list->info.node_list.list;
	      while (*value)
		{
		  if (attr == NULL)
		    {
		      /* System error, should have been caught in the semantic
		         pass */
		      PT_ERRORm (parser, (*value), MSGCAT_SET_PARSER_RUNTIME,
				 MSGCAT_RUNTIME_ATTRS_GT_QSPEC_COLS);
		      goto exit_on_error;
		    }

		  attr_next = attr->next;
		  attr->next = NULL;
		  value_next = (*value)->next;
		  (*value)->next = NULL;

		  (*value) = mq_translate_value (parser, *value);

		  result = pt_invert (parser, attr, *value);
		  if (result == NULL)
		    {
		      /* error not invertable/updatable */
		      PT_ERRORmf (parser, attr, MSGCAT_SET_PARSER_RUNTIME,
				  MSGCAT_RUNTIME_VASG_TGT_UNINVERTBL,
				  pt_short_print (parser, attr));
		      goto exit_on_error;
		    }

		  if (build_att_names_list)
		    {
		      if (attr_names_crt == NULL)
			{
			  /* This is the first attribute in the name list. */
			  attr_names_crt = attr_names = result->next;
			}
		      else
			{
			  attr_names_crt->next = result->next;
			  attr_names_crt = attr_names_crt->next;
			}
		      result->next = NULL;
		    }
		  else
		    {
		      parser_free_tree (parser, result->next);
		      result->next = NULL;
		    }

		  attr->next = attr_next;
		  attr = attr->next;

		  (*value) = result;	/* the right hand side */
		  (*value)->next = value_next;
		  value = &(*value)->next;
		}
	    }

	  if (attr_names != NULL)
	    {
	      parser_free_tree (parser, statement->info.insert.attr_list);
	      statement->info.insert.attr_list = attr_names;
	      attr_names = NULL;
	    }
	}
      else if (crt_list->info.node_list.list_type == PT_IS_SUBQUERY)
	{
	  assert (crt_list->next == NULL);
	  assert (crt_list->info.node_list.list->next == NULL);

	  mq_invert_insert_subquery (parser,
				     &statement->info.insert.attr_list,
				     crt_list->info.node_list.list);
	}
      else
	{
	  assert (false);
	  /* system error */
	  goto exit_on_error;
	}
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      statement->info.query.q.union_.arg1 =
	mq_class_lambda (parser, statement->info.query.q.union_.arg1,
			 class_, corresponding_spec, class_where_part,
			 class_group_by_part, class_having_part);
      statement->info.query.q.union_.arg2 =
	mq_class_lambda (parser, statement->info.query.q.union_.arg2,
			 class_, corresponding_spec, class_where_part,
			 class_group_by_part, class_having_part);
      break;
#endif /* this is impossible case */

    default:
      /* system error */
      goto exit_on_error;
    }

  /* handle is a where parts of view sub-querys */
  if (where_part)
    {
      /* force sub expressions to be parenthesized for correct
       * printing. Otherwise, the associativity may be wrong when
       * the statement is printed and sent to a local database
       */
      if (class_where_part && class_where_part->node_type == PT_EXPR)
	{
	  class_where_part->info.expr.paren_type = 1;
	}
      if ((*where_part) && (*where_part)->node_type == PT_EXPR)
	{
	  (*where_part)->info.expr.paren_type = 1;
	}
      /* The "where clause" is in the form of a list of CNF "and" terms.
       * In order to "and" together the view's "where clause" with the
       * statement's, we must maintain this list of terms.
       * Using a 'PT_AND' node here will have the effect of losing the
       * "and" terms on the tail of either list.
       */
      *where_part =
	parser_append_node (parser_copy_tree_list (parser, class_where_part),
			    *where_part);
    }

  if (specptr)
    {
      spec = *specptr;
      while (spec && class_->info.name.spec_id != spec->info.spec.id)
	{
	  specptr = &spec->next;
	  spec = *specptr;
	}

      if (spec)
	{
	  SPEC_RESET_INFO spec_reset;

	  newspec = parser_copy_tree_list (parser, corresponding_spec);
	  oldnext = spec->next;
	  spec->next = NULL;
	  spec_reset.statement = statement;
	  spec_reset.old_next = oldnext;
	  if (newspec)
	    {
	      if (newspec->info.spec.entity_name == NULL)
		{
		  newspec->info.spec.entity_name =
		    spec->info.spec.entity_name;
		  /* spec will be free later, we don't want the entity_name
		   * will be freed
		   */
		  spec->info.spec.entity_name = NULL;
		}

	      newspec->info.spec.range_var->info.name.original =
		spec->info.spec.range_var->info.name.original;
	      newspec->info.spec.location = spec->info.spec.location;
	      /* move join info */
	      if (spec->info.spec.join_type != PT_JOIN_NONE)
		{
		  newspec->info.spec.join_type = spec->info.spec.join_type;
		  newspec->info.spec.on_cond = spec->info.spec.on_cond;
		  spec->info.spec.on_cond = NULL;
		}

	      /* move spec flag */
	      newspec->info.spec.flag = spec->info.spec.flag;
	    }

	  if (statement->node_type == PT_SELECT)
	    {
	      for_update =
		(PT_SELECT_INFO_IS_FLAGED
		 (statement, PT_SELECT_INFO_FOR_UPDATE)
		 && (spec->info.spec.flag & PT_SPEC_FLAG_FOR_UPDATE_CLAUSE));
	    }

	  parser_free_tree (parser, spec);

	  if (newspec)
	    {
	      *specptr = newspec;
	      parser_append_node (oldnext, newspec);

	      newspec = parser_walk_tree (parser, newspec,
					  mq_reset_spec_distr_subpath_pre,
					  &spec_reset,
					  mq_reset_spec_distr_subpath_post,
					  &spec_reset);

	      statement = spec_reset.statement;
	    }
	  else
	    {
	      PT_INTERNAL_ERROR (parser, "translate");
	      goto exit_on_error;
	    }
	}
      else
	{
	  /* we are doing a null substitution. ie the classes don't match
	     the spec. The "correct translation" is NULL.  */
	  goto exit_on_error;
	}
    }

  if (statement)
    {
      /* The spec id's are those copied from the cache.
       * They are unique in this statment tree, but will not be unique
       * if this tree is once more translated against the same
       * virtual class_. Now, the newly introduced entity specs,
       * are gone through and the id's for each and each name reset
       * again to a new (uncopied) unique number, to preserve the uniqueness
       * of the specs.
       */
      for (spec = newspec; spec != NULL; spec = spec->next)
	{
	  if (spec == oldnext)
	    {
	      break;		/* these are already ok */
	    }

	  if (for_update)
	    {
	      assert (statement->node_type == PT_SELECT);

	      if (spec->info.spec.derived_table == NULL)
		{
		  spec->info.spec.flag |= PT_SPEC_FLAG_FOR_UPDATE_CLAUSE;
		}
	    }
	}


      if (newspec)
	{
	  if (!PT_IS_QUERY_NODE_TYPE (statement->node_type))
	    {
	      /* PT_INSERT, PT_UPDATE, PT_DELETE */
	      statement = mq_rename_resolved (parser, newspec,
					      statement, newresolved);
	      newspec = newspec->next;
	    }
	  for (spec = newspec; spec != NULL; spec = spec->next)
	    {
	      if (spec == oldnext || statement == NULL)
		{
		  break;	/* these are already ok */
		}
	      if (spec->info.spec.range_var->alias_print)
		{
		  char *temp;
		  temp = pt_append_string (parser, NULL, newresolved);
		  temp = pt_append_string (parser, temp, ":");
		  temp = pt_append_string (parser, temp,
					   spec->info.spec.range_var->
					   alias_print);
		  spec->info.spec.range_var->alias_print = temp;
		}
	      else
		{
		  spec->info.spec.range_var->alias_print = newresolved;
		}
	      statement = mq_regenerate_if_ambiguous (parser, spec,
						      statement,
						      statement->info.query.q.
						      select.from);
	    }
	}
    }

  return statement;

exit_on_error:
  if (attr_names != NULL)
    {
      parser_free_tree (parser, attr_names);
    }
  return NULL;
}


/*
 * mq_lambda_node_pre() - creates extra spec frames for each select
 *   return:
 *   parser(in):
 *   tree(in):
 *   void_arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_lambda_node_pre (PARSER_CONTEXT * parser, PT_NODE * tree, void *void_arg,
		    UNUSED_ARG int *continue_walk)
{
  MQ_LAMBDA_ARG *lambda_arg = (MQ_LAMBDA_ARG *) void_arg;
  PT_EXTRA_SPECS_FRAME *spec_frame;

  if (tree->node_type == PT_SELECT)
    {
      spec_frame =
	(PT_EXTRA_SPECS_FRAME *) malloc (sizeof (PT_EXTRA_SPECS_FRAME));

      if (spec_frame == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "malloc");
	  return NULL;
	}
      spec_frame->next = lambda_arg->spec_frames;
      spec_frame->extra_specs = NULL;
      lambda_arg->spec_frames = spec_frame;
    }

  return tree;

}				/* mq_lambda_node_pre */


/*
 * mq_lambda_node() - applies the lambda test to the node passed to it,
 *             and conditionally substitutes a copy of its corresponding tree
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_lambda_node (PARSER_CONTEXT * parser, PT_NODE * node, void *void_arg,
		UNUSED_ARG int *continue_walk)
{
  MQ_LAMBDA_ARG *lambda_arg = (MQ_LAMBDA_ARG *) void_arg;
  PT_NODE *save_node_next, *result, *arg1, *spec;
  PT_NODE *dt1, *dt2;
  PT_EXTRA_SPECS_FRAME *spec_frame;
  PT_NODE *save_data_type;
  PT_NODE *name, *tree;

  result = node;

  switch (node->node_type)
    {

    case PT_DOT_:
      /* Check if the recursive call left an "illegal" path expression */
      arg1 = node->info.dot.arg1;
      if (arg1 != NULL)
	{
	  save_node_next = node->next;
	  if (PT_IS_QUERY_NODE_TYPE (arg1->node_type))
	    {
	      assert (false);
	      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_RUNTIME,
			  MSGCAT_RUNTIME_BAD_CONSTR_IN_PATH,
			  pt_short_print (parser, node));
	    }
	  else if (PT_IS_OID_NAME (arg1))
	    {
	      /* we have an artificial path, from a view that selects
	       * an oid, eg
	       *      create view foo (a) as select x from x
	       * It would be nice to translate this to just the RHS,
	       * but subsequent path translation would have nothing to key
	       * off of.
	       */
	      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_RUNTIME,
			  MSGCAT_RUNTIME_BAD_CONSTR_IN_PATH,
			  pt_short_print (parser, node));
	    }
	  else if (PT_IS_NULL_NODE (arg1))
	    {
	      /* someone did a select a.b from view, where a is a null
	       * the result is also NULL.
	       */

	      node->info.dot.arg1 = NULL;
	      node->next = NULL;

	      result = arg1;

	      parser_free_tree (parser, node);	/* re-use this memory */

	      /* if this name was in a name list, keep the list tail */
	      result->next = save_node_next;
	    }
	}
      break;

    case PT_NAME:
      for (name = lambda_arg->name_list, tree = lambda_arg->tree_list;
	   name && tree; name = name->next, tree = tree->next)
	{
	  /* If the names are equal, substitute new sub tree
	   * Here we DON't want to do the usual strict name-datatype matching.
	   * This is where we project one object attribute as another, so
	   * we deliberately allow the loosely typed match by nulling
	   * the data_type.
	   */
	  save_data_type = name->data_type;	/* save */
	  name->data_type = NULL;

	  if (pt_name_equal (parser, node, name))
	    {
	      save_node_next = node->next;
	      node->next = NULL;

	      result = parser_copy_tree (parser, tree);	/* substitute */

	      /* Keep hidden column information during view translation */
	      if (result)
		{
		  result->line_number = node->line_number;
		  result->column_number = node->column_number;
		  result->is_hidden_column = node->is_hidden_column;
		  result->buffer_pos = node->buffer_pos;
#if 0
		  result->info.name.original = node->info.name.original;
#endif /* 0 */
		}

	      /* we may have just copied a whole query,
	       * if so, reset its id's */
	      result = mq_reset_specs_from_column (parser, result, tree);

	      parser_free_tree (parser, node);	/* re-use this memory */

	      result->next = save_node_next;

	      name->data_type = save_data_type;	/* restore */

	      break;		/* exit for-loop */
	    }

	  /* name did not match. go ahead */
	  name->data_type = save_data_type;	/* restore */
	}

      break;

    case PT_SELECT:
      /* maintain virtual data type information */
      if ((dt1 = result->data_type)
	  && result->info.query.q.select.list
	  && (dt2 = result->info.query.q.select.list->data_type))
	{
	  parser_free_tree (parser, result->data_type);
	  result->data_type = parser_copy_tree_list (parser, dt2);
	}
      /* pop the extra spec frame and add any extra specs to the from list */
      spec_frame = lambda_arg->spec_frames;
      lambda_arg->spec_frames = lambda_arg->spec_frames->next;
      result->info.query.q.select.from =
	parser_append_node (spec_frame->extra_specs,
			    result->info.query.q.select.from);

      /* adding specs may have created ambiguous spec names */
      for (spec = spec_frame->extra_specs; spec != NULL; spec = spec->next)
	{
	  result = mq_regenerate_if_ambiguous (parser, spec, result,
					       result->info.query.q.select.
					       from);
	}

      free_and_init (spec_frame);
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      /* maintain virtual data type information */
      if ((dt1 = result->data_type)
	  && result->info.query.q.union_.arg1
	  && (dt2 = result->info.query.q.union_.arg1->data_type))
	{
	  parser_free_tree (parser, result->data_type);
	  result->data_type = parser_copy_tree_list (parser, dt2);
	}
      break;

    default:
      break;
    }

  return result;
}

/*
 * mq_lambda() - modifies name nodes with copies of a corresponding tree
 *   return:
 *   parser(in):
 *   tree_with_names(in):
 *   name_node_list(in):
 *   corresponding_tree_list(in):
 */
static PT_NODE *
mq_lambda (PARSER_CONTEXT * parser, PT_NODE * tree_with_names,
	   PT_NODE * name_node_list, PT_NODE * corresponding_tree_list)
{
  MQ_LAMBDA_ARG lambda_arg;
  PT_NODE *tree;
  PT_NODE *name;

  lambda_arg.name_list = name_node_list;
  lambda_arg.tree_list = corresponding_tree_list;
  lambda_arg.spec_frames = NULL;

  for (name = lambda_arg.name_list, tree = lambda_arg.tree_list;
       name && tree; name = name->next, tree = tree->next)
    {
      if (tree->node_type == PT_EXPR)
	{
	  /* make sure it will print with proper precedance.
	   * we don't want to replace "name" with "1+2"
	   * in 4*name, and get 4*1+2. It should be 4*(1+2) instead.
	   */
	  tree->info.expr.paren_type = 1;
	}

      if (name->node_type != PT_NAME)
	{			/* unkonwn error */
	  tree = tree_with_names;
	  goto exit_on_error;
	}

    }

  tree = parser_walk_tree (parser, tree_with_names,
			   mq_lambda_node_pre, &lambda_arg,
			   mq_lambda_node, &lambda_arg);

exit_on_error:

  return tree;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * mq_set_virt_object() - checks and sets name nodes of object type
 *                        virtual object information
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
mq_set_virt_object (PARSER_CONTEXT * parser, PT_NODE * node, void *void_arg,
		    int *continue_walk)
{
  PT_NODE *spec = (PT_NODE *) void_arg;
  PT_NODE *dt;
  PT_NODE *cls;

  if (node->node_type == PT_NAME
      && node->info.name.spec_id == spec->info.spec.id
      && (dt = node->data_type)
      && node->type_enum == PT_TYPE_OBJECT
      && (cls = dt->info.data_type.entity))
    {
      if (db_is_vclass (cls->info.name.db_object))
	{
	  dt->info.data_type.entity = NULL;
	  parser_free_tree (parser, cls);
	}
    }

  return node;
}
#endif

/*
 * mq_fix_derived() - fixes derived table and checks for virtual object types
 *   return:
 *   parser(in):
 *   select_statement(in):
 *   spec(in):
 */
static PT_NODE *
mq_fix_derived (PARSER_CONTEXT * parser, PT_NODE * select_statement,
		PT_NODE * spec)
{
  PT_NODE *attr = spec->info.spec.as_attr_list;
  PT_NODE *attr_next;
  PT_NODE *dt;
  PT_NODE *cls;
  int had_virtual, any_had_virtual;

  any_had_virtual = 0;
  while (attr)
    {
      dt = attr->data_type;
      had_virtual = 0;
      if (dt && attr->type_enum == PT_TYPE_OBJECT)
	{
	  cls = dt->info.data_type.entity;
	  while (cls)
	    {
	      if (db_is_vclass (cls->info.name.db_object))
		{
#if 1				/* TODO - */
		  assert (false);	/* is impossible */
		  PT_INTERNAL_ERROR (parser, "translate");
		  return select_statement;	/* something wrong */
#else
		  had_virtual = 1;
#endif
		}
	      cls = cls->next;
	    }
	}
      attr_next = attr->next;
      if (had_virtual)
	{
	  any_had_virtual = 1;
	}
      attr = attr_next;
    }

  mq_reset_ids (parser, select_statement, spec);

  assert (any_had_virtual == 0);
#if defined (ENABLE_UNUSED_FUNCTION)
  if (any_had_virtual)
    {
      select_statement =
	parser_walk_tree (parser, select_statement, mq_set_virt_object, spec,
			  NULL, NULL);
    }
#endif

  return select_statement;
}


/*
 * mq_fix_derived_in_union() - fixes the derived tables in queries
 *   return:
 *   parser(in):
 *   statement(in):
 *   spec_id(in):
 *
 * Note:
 * It performs two functions
 *      1) In a given select, the outer level derived table spec
 *         is not in general the SAME spec being manipulated here.
 *         This spec is a copy of the outer spec, with the same id.
 *         Thus, we use the spec_id to find the derived table of interest
 *         to 'fix up'.
 *      2) Since the statement may have been translated to a union,
 *         there may be multiple derived tables to fix up. This
 *         recurses for unions to do so.
 */
PT_NODE *
mq_fix_derived_in_union (PARSER_CONTEXT * parser, PT_NODE * statement,
			 UINTPTR spec_id)
{
  PT_NODE *spec;

  if (statement == NULL)
    {
      return NULL;
    }

  switch (statement->node_type)
    {
    case PT_SELECT:
      spec = statement->info.query.q.select.from;
      while (spec && spec->info.spec.id != spec_id)
	{
	  spec = spec->next;
	}
      if (spec)
	{
	  statement = mq_fix_derived (parser, statement, spec);
	}
      else
	{
	  PT_INTERNAL_ERROR (parser, "translate");
	}
      break;

    case PT_DELETE:
      spec = statement->info.delete_.spec;
      while (spec && spec->info.spec.id != spec_id)
	{
	  spec = spec->next;
	}
      if (spec)
	{
	  statement = mq_fix_derived (parser, statement, spec);
	}
      else
	{
	  PT_INTERNAL_ERROR (parser, "translate");
	}
      break;

    case PT_UPDATE:
      spec = statement->info.update.spec;
      while (spec && spec->info.spec.id != spec_id)
	{
	  spec = spec->next;
	}
      if (spec)
	{
	  statement = mq_fix_derived (parser, statement, spec);
	}
      else
	{
	  PT_INTERNAL_ERROR (parser, "translate");
	}
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      statement->info.query.q.union_.arg1 =
	mq_fix_derived_in_union
	(parser, statement->info.query.q.union_.arg1, spec_id);
      statement->info.query.q.union_.arg2 =
	mq_fix_derived_in_union
	(parser, statement->info.query.q.union_.arg2, spec_id);
      break;

    default:
      PT_INTERNAL_ERROR (parser, "translate");
      break;
    }

  return statement;
}


/*
 * mq_translate_value() - translate a virtual object to the real object
 *   return:
 *   parser(in):
 *   value(in):
 */
static PT_NODE *
mq_translate_value (PARSER_CONTEXT * parser, PT_NODE * value)
{
  PT_NODE *data_type, *class_;
  DB_OBJECT *real_object, *real_class;
  DB_VALUE *db_value;

  if (value->node_type == PT_VALUE
      && value->type_enum == PT_TYPE_OBJECT
      && (data_type = value->data_type)
      && (class_ = data_type->info.data_type.entity)
      && class_->node_type == PT_NAME
      && db_is_vclass (class_->info.name.db_object))
    {
      assert (false);
      real_object = value->info.value.data_value.op;
      if (real_object)
	{
	  real_class = sm_get_class (real_object);
	  class_->info.name.db_object = sm_get_class (real_object);
	  class_->info.name.original =
	    sm_class_name (class_->info.name.db_object);
	  value->info.value.data_value.op = real_object;

	  db_value = pt_value_to_db (parser, value);
	  if (db_value)
	    {
	      DB_MAKE_OBJECT (db_value, value->info.value.data_value.op);
	    }
	}
    }

  return value;
}


/*
 * mq_fetch_subqueries_for_update_local() - ask the schema manager for the
 *      cached parser containing the compiled subqueries of the class_.
 *      If that is not already cached, the schema manager will call back to
 *      compute the subqueries
 *   return:
 *   parser(in):
 *   class(in):
 *   what_for(in):
 *   qry_cache(out):
 */
static PT_NODE *
mq_fetch_subqueries_for_update_local (PARSER_CONTEXT * parser,
				      PT_NODE * class_,
				      DB_AUTH what_for,
				      PARSER_CONTEXT ** qry_cache)
{
  PARSER_CONTEXT *query_cache;
  DB_OBJECT *class_object;
#if !defined(NDEBUG)
  int save_host_var_count = parser->host_var_count;
#endif

  if (!class_ || !(class_object = class_->info.name.db_object)
      || !qry_cache || db_is_class (class_object))
    {
      return NULL;
    }

  *qry_cache = NULL;

  query_cache = sm_virtual_queries (parser, class_object);

#if !defined(NDEBUG)
  assert (save_host_var_count == parser->host_var_count);
#endif

  if (query_cache == NULL || query_cache->view_cache == NULL)
    {
      return NULL;
    }

  if (!(query_cache->view_cache->vquery_authorization & what_for))
    {
      PT_ERRORmf2 (parser, class_, MSGCAT_SET_PARSER_RUNTIME,
		   MSGCAT_RUNTIME_IS_NOT_AUTHORIZED_ON,
		   get_authorization_name (what_for),
		   sm_class_name (class_object));
      return NULL;
    }

  PT_ERRORmf (parser, class_, MSGCAT_SET_PARSER_RUNTIME,
	      MSGCAT_RUNTIME_VCLASS_NOT_UPDATABLE,
	      /* use function to get name.
	       * class_->info.name.original is not always set. */
	      sm_class_name (class_object));

  return NULL;
}

/*
 * mq_fetch_subqueries_for_update() - just like ..._for_update_local except
 *      it does not have an output argument for qry_cache
 *   return:
 *   parser(in):
 *   class(in):
 *   what_for(in):
 */
static PT_NODE *
mq_fetch_subqueries_for_update (PARSER_CONTEXT * parser, PT_NODE * class_,
				DB_AUTH what_for)
{
  PARSER_CONTEXT *query_cache;
  PT_NODE *subquery;

  subquery = mq_fetch_subqueries_for_update_local (parser, class_,
						   what_for, &query_cache);
  assert (query_cache == NULL);
  assert (subquery == NULL);

  return NULL;
}

/*
 * mq_fetch_attributes() - fetch class's subqueries
 *   return: PT_NODE list of its attribute names, including oid attr
 *   parser(in):
 *   class(in):
 */
static PT_NODE *
mq_fetch_attributes (PARSER_CONTEXT * parser, PT_NODE * class_)
{
  PARSER_CONTEXT *query_cache;
  DB_OBJECT *class_object;
  PT_NODE *attributes = NULL;
#if !defined(NDEBUG)
  int save_host_var_count = parser->host_var_count;
#endif

  if (!class_ || !(class_object = class_->info.name.db_object)
      || db_is_class (class_object))
    {
      return NULL;
    }

  query_cache = sm_virtual_queries (parser, class_object);

#if !defined(NDEBUG)
  assert (save_host_var_count == parser->host_var_count);
#endif

  if (query_cache == NULL || query_cache->view_cache == NULL)
    {
      return NULL;
    }

  if (!(query_cache->view_cache->vquery_authorization & DB_AUTH_SELECT))
    {
      PT_ERRORmf2 (parser, class_, MSGCAT_SET_PARSER_RUNTIME,
		   MSGCAT_RUNTIME_IS_NOT_AUTHORIZED_ON,
		   get_authorization_name (DB_AUTH_SELECT),
		   sm_class_name (class_object));
      return NULL;
    }

  attributes = parser_copy_tree_list (parser, query_cache->view_cache->attrs);

  return attributes;
}

static const char *
get_authorization_name (DB_AUTH auth)
{
  switch (auth)
    {
    case DB_AUTH_NONE:
      return "";

    case DB_AUTH_SELECT:
      return "SELECT";

    case DB_AUTH_INSERT:
      return "INSERT";

    case DB_AUTH_UPDATE:
      return "UPDATE";

    case DB_AUTH_DELETE:
      return "DELETE";

    case DB_AUTH_ALTER:
      return "ALTER";

    case DB_AUTH_REPLACE:
      return "REPLACE";

    case DB_AUTH_INSERT_UPDATE:
      return "INSERT/UPDATE";

    case DB_AUTH_UPDATE_DELETE:
      return "UPDATE/DELETE";

    case DB_AUTH_INSERT_UPDATE_DELETE:
      return "INSERT/UPDATE/DELETE";

    default:
      return "";
    }
}

/*
 * mq_reset_references_to_query_string () - reset references to the position
 *					    in the original query string
 *   parser(in): parser context
 *   node(in): node
 *   arg(in/out): parent node stack
 *   continue_walk(in/out): walk type
 *
 * NOTE: This function resets the value of line_number, column_number and
 * buffer_pos for each node. This is called on the parse trees of translated
 * views. Since these values point to the view query and not to the actual
 * query that is being executed, they will not hold useful information
 */
static PT_NODE *
mq_reset_references_to_query_string (PARSER_CONTEXT * parser, PT_NODE * node,
				     UNUSED_ARG void *arg,
				     UNUSED_ARG int *continue_walk)
{
  if (node == NULL || parser == NULL)
    {
      return node;
    }

  node->line_number = 0;
  node->column_number = 0;
  node->buffer_pos = -1;

  return node;
}

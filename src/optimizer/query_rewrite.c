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
 * query_rewrite.c - Query rewrite optimization
 */

#ident "$Id$"

#include "parser.h"
#include "parser_message.h"
#include "parse_tree.h"
#include "optimizer.h"
#include "xasl_generation.h"
#include "system_parameter.h"
#include "semantic_check.h"
#include "execute_schema.h"
#include "view_transform.h"
#include "parser.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define DB_MAX_LITERAL_PRECISION 255

typedef struct spec_id_info SPEC_ID_INFO;
struct spec_id_info
{
  UINTPTR id;
  bool appears;
};

typedef struct pt_name_spec_info PT_NAME_SPEC_INFO;
struct pt_name_spec_info
{
  PT_NODE *c_name;		/* attr name which will be reduced to constant */
  int c_name_num;
  int query_num;		/* query number */
  PT_NODE *s_point_list;	/* list of other specs name.
				 * these are joined with spec of c_name */
};

typedef enum
{
  DNF_RANGE_VALID = 0,
  DNF_RANGE_ALWAYS_FALSE = 1,
  DNF_RANGE_ALWAYS_TRUE = 2
} DNF_MERGE_RANGE_RESULT;

typedef struct qo_reset_location_info RESET_LOCATION_INFO;
struct qo_reset_location_info
{
  PT_NODE *start_spec;
  short start;
  short end;
  bool found_outerjoin;
};

static PT_NODE *qo_reset_location (PARSER_CONTEXT * parser, PT_NODE * node,
				   void *arg, int *continue_walk);

/*
 * qo_get_name_by_spec_id () - looks for a name with a matching id
 *   return: PT_NODE *
 *   parser(in): parser environment
 *   node(in): (name) node to compare id's with
 *   arg(in): info of spec and result
 *   continue_walk(in):
 */
static PT_NODE *
qo_get_name_by_spec_id (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
			void *arg, int *continue_walk)
{
  SPEC_ID_INFO *info = (SPEC_ID_INFO *) arg;

  if (node->node_type == PT_NAME && node->info.name.spec_id == info->id)
    {
      *continue_walk = PT_STOP_WALK;
      info->appears = true;
    }

  return node;
}

/*
 * qo_check_nullable_expr () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
PT_NODE *
qo_check_nullable_expr (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
			void *arg, UNUSED_ARG int *continue_walk)
{
  int *nullable_cntp = (int *) arg;

  if (node->node_type == PT_EXPR)
    {
      /* check for nullable term: expr(..., NULL, ...) can be non-NULL
       */
      switch (node->info.expr.op)
	{
	case PT_IS_NULL:
	case PT_CASE:
	case PT_COALESCE:
	case PT_NVL:
	case PT_NVL2:
	case PT_DECODE:
	case PT_IF:
	case PT_IFNULL:
	case PT_ISNULL:
	case PT_CONCAT_WS:
	  /* NEED FUTURE OPTIMIZATION */
	  (*nullable_cntp)++;
	  break;
	default:
	  break;
	}
    }

  return node;
}

/*
 * qo_collect_name_spec () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
qo_collect_name_spec (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
		      int *continue_walk)
{
  PT_NAME_SPEC_INFO *info = (PT_NAME_SPEC_INFO *) arg;

  *continue_walk = PT_CONTINUE_WALK;

  switch (node->node_type)
    {
    case PT_DOT_:
      node = pt_get_end_path_node (node);
      if (node->node_type != PT_NAME)
	{
	  break;		/* impossible case, give up */
	}

      /* FALL THROUGH */

    case PT_NAME:
      if (info->c_name->info.name.location > 0
	  && info->c_name->info.name.location < node->info.name.location)
	{
	  /* next outer join location */
	}
      else
	{
	  if (node->info.name.spec_id == info->c_name->info.name.spec_id)
	    {
	      /* check for name spec is same */
	      if (pt_name_equal (parser, node, info->c_name))
		{
		  info->c_name_num++;	/* found reduced attr */
		}
	    }
	  else
	    {
	      PT_NODE *point, *s_name;

	      /* check for spec in other spec */
	      for (point = info->s_point_list; point; point = point->next)
		{
		  s_name = point;
		  CAST_POINTER_TO_NODE (s_name);
		  if (s_name->info.name.spec_id == node->info.name.spec_id)
		    break;
		}

	      /* not found */
	      if (!point)
		{
		  info->s_point_list =
		    parser_append_node (pt_point (parser, node),
					info->s_point_list);
		}
	    }
	}

      *continue_walk = PT_LIST_WALK;
      break;

    case PT_SELECT:
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      /* simply give up when we find query in predicate
       */
      info->query_num++;
      break;

    case PT_EXPR:
      if (PT_HAS_COLLATION (info->c_name->type_enum)
	  && node->info.expr.op == PT_CAST
	  && PT_HAS_COLLATION (node->type_enum)
	  && node->info.expr.arg1 != NULL
	  && node->info.expr.arg1->node_type == PT_NAME
	  && node->info.expr.arg1->info.name.spec_id
	  == info->c_name->info.name.spec_id)
	{
	  int cast_coll = LANG_SYS_COLLATION;
	  int name_coll = LANG_SYS_COLLATION;

	  if (node->data_type != NULL)
	    {
	      cast_coll = node->data_type->info.data_type.collation_id;
	    }

	  if (cast_coll != name_coll)
	    {
	      /* predicate evaluates with different collation */
	      info->query_num++;
	    }
	}
      break;
    default:
      break;
    }				/* switch (node->node_type) */

  if (info->query_num > 0)
    {
      *continue_walk = PT_STOP_WALK;
    }

  return node;
}

/*
 * qo_collect_name_spec_post () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
qo_collect_name_spec_post (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
			   void *arg, int *continue_walk)
{
  PT_NAME_SPEC_INFO *info = (PT_NAME_SPEC_INFO *) arg;

  *continue_walk = PT_CONTINUE_WALK;

  if (info->query_num > 0)
    {
      *continue_walk = PT_STOP_WALK;
    }

  return node;
}

/*
 * qo_is_cast_attr () -
 *   return:
 *   expr(in):
 */
static int
qo_is_cast_attr (PT_NODE * expr)
{
  PT_NODE *arg1;

  /* check for CAST-expr  */
  if (!expr || expr->node_type != PT_EXPR || expr->info.expr.op != PT_CAST ||
      !(arg1 = expr->info.expr.arg1))
    {
      return 0;
    }

  return pt_is_attr (arg1);
}

/*
 * qo_is_reduceable_const () -
 *   return:
 *   expr(in):
 */
static int
qo_is_reduceable_const (PT_NODE * expr)
{
  while (expr && expr->node_type == PT_EXPR)
    {
      if (expr->info.expr.op == PT_CAST)
	{
	  expr = expr->info.expr.arg1;
	}
      else
	{
	  return false;		/* give up */
	}
    }

  return PT_IS_CONST_INPUT_HOSTVAR (expr);
}

/*
 * qo_reduce_equality_terms () -
 *   return:
 *   parser(in):
 *   node(in):
 *   where(in):
 *
 *  Obs: modified to support PRIOR operator as follows:
 *    -> PRIOR field = exp1 AND PRIOR field = exp2 =>
 *	 PRIOR field = exp1 AND exp1 = exp2
 *    -> PRIOR ? -> replace with ?
 */

static PT_NODE *
qo_reduce_equality_terms_helper (PARSER_CONTEXT * parser, PT_NODE * node,
				 PT_NODE * where)
{
  PT_NODE *from;
  PT_NODE *accumulator, *expr, *arg1, *arg2, *temp, *next;
  PT_NODE *join_term, *join_term_list, *s_name1, *s_name2;
  PT_NAME_SPEC_INFO info1, info2;
  int spec1_cnt, spec2_cnt;
  bool found_equality_term, found_join_term;
  PT_NODE *spec, *derived_table, *attr, *col;
  int i, num_check, idx;
  PT_NODE *expr_prev, *expr_next;
  bool copy_arg2;

  /* init */
  accumulator = NULL;
  join_term_list = NULL;

  expr_prev = expr_next = NULL;
  for (expr = where; expr != NULL; expr = expr_next)
    {
      expr_next = expr->next;

      col = NULL;		/* init - reserve for constant column of derived-table */

      /* check for 1st phase; keep out OR conjunct; 1st init
       */
      found_equality_term = (expr->or_next == NULL) ? true : false;

      if (found_equality_term != true)
	{
	  expr_prev = expr;
	  continue;		/* give up */
	}

      /* check for 2nd phase; '=', 'range ( =)'
       */
      found_equality_term = false;	/* 2nd init */

      if (expr->info.expr.op == PT_EQ
	  && expr->info.expr.arg1 && expr->info.expr.arg2)
	{			/* 'opd = opd' */
	  found_equality_term = true;	/* pass 2nd phase */
	  num_check = 2;
	}
      else if (expr->info.expr.op == PT_RANGE)
	{			/* 'opd range (opd =)' */
	  PT_NODE *between_and;

	  between_and = expr->info.expr.arg2;
	  if (between_and->or_next == NULL	/* has only one range */
	      && between_and->info.expr.op == PT_BETWEEN_EQ_NA)
	    {
	      found_equality_term = true;	/* pass 2nd phase */
	      num_check = 1;
	    }
	}

      if (found_equality_term != true)
	{
	  expr_prev = expr;
	  continue;		/* give up */
	}

      /* check for 3rd phase; 'attr = const', 'attr range (const =)'
       */
      found_equality_term = false;	/* 3rd init */

      for (i = 0; i < num_check; i++)
	{
	  arg1 = (i == 0) ? expr->info.expr.arg1 : expr->info.expr.arg2;
	  arg2 = (i == 0) ? expr->info.expr.arg2 : expr->info.expr.arg1;

	  if (expr->info.expr.op == PT_RANGE)
	    {
	      arg2 = arg2->info.expr.arg1;
	    }

	  if (pt_is_attr (arg1))
	    {
	      if (qo_is_reduceable_const (arg2))
		{
		  found_equality_term = true;
		  break;	/* immediately break */
		}
	      else if (pt_is_attr (arg2))
		{
		  ;		/* nop */
		}
	      else if (qo_is_cast_attr (arg2))
		{
		  arg2 = arg2->info.expr.arg1;
		}
	      else
		{
		  continue;	/* not found. step to next */
		}

	      if (node->node_type == PT_SELECT)
		{
		  from = node->info.query.q.select.from;
		}
	      else
		{
		  from = NULL;	/* not found. step to next */
		}

	      for (spec = from; spec; spec = spec->next)
		{
		  if (spec->info.spec.id == arg2->info.name.spec_id)
		    {
		      break;	/* found match */
		    }
		}

	      /* if arg2 is derived alias col, get its corresponding
	       * constant column from derived-table
	       */
	      if (spec
		  && spec->info.spec.derived_table_type == PT_IS_SUBQUERY
		  && (derived_table = spec->info.spec.derived_table)
		  && derived_table->node_type == PT_SELECT)
		{
		  /* traverse as_attr_list */
		  for (attr = spec->info.spec.as_attr_list, idx = 0;
		       attr; attr = attr->next, idx++)
		    {
		      if (pt_name_equal (parser, attr, arg2))
			{
			  break;	/* found match */
			}
		    }		/* for (attr = ...) */

		  /* get corresponding column */
		  col = pt_get_select_list (parser, derived_table);
		  for (; col && idx; col = col->next, idx--)
		    {
		      ;		/* step to next */
		    }

		  if (attr && col && qo_is_reduceable_const (col))
		    {
		      /* add additional equailty-term; is reduced */
		      where =
			parser_append_node (parser_copy_tree (parser, expr),
					    where);

		      /* reset arg1, arg2 */
		      arg1 = arg2;
		      arg2 = col;

		      found_equality_term = true;
		      break;	/* immediately break */
		    }
		}		/* if arg2 is derived alias-column */
	    }			/* if (pt_is_attr(arg1)) */
	}			/* for (i = 0; ...) */

      if (found_equality_term != true)
	{
	  expr_prev = expr;
	  continue;		/* give up */
	}

      /*
       * now, finally pass all check
       */

      if (pt_is_attr (arg2))
	{
	  temp = arg1;
	  arg1 = arg2;
	  arg2 = temp;
	}

      /* at here, arg1 is reduced attr */

      /* move 'expr' from 'where' list to 'accumulator' list */
      if (expr_prev == NULL)
	{
	  assert (expr == where);
	  where = expr->next;
	}
      else
	{
	  expr_prev->next = expr->next;
	}

      if (col)
	{
	  /* corresponding constant column of derived-table */
	  expr_prev = expr;
	}
      else
	{
	  expr->next = accumulator;
	  accumulator = expr;
	}

      /* Restart where at beginning of WHERE clause because
         we may find new terms after substitution, and must
         substitute entire where clause because incoming
         order is arbitrary. */

      temp = pt_get_end_path_node (arg1);

      info1.c_name = temp;
      info2.c_name = temp;

      /* save reduced join terms */
      for (temp = where; temp; temp = temp->next)
	{
	  if (temp == expr)
	    {
	      /* this is the working equality_term, skip and go ahead */
	      continue;
	    }

	  if (temp->node_type != PT_EXPR ||
	      !pt_is_symmetric_op (temp->info.expr.op))
	    {
	      /* skip and go ahead */
	      continue;
	    }

	  next = temp->next;	/* save and cut-off link */
	  temp->next = NULL;

	  /* check for already added join term */
	  for (join_term = join_term_list; join_term;
	       join_term = join_term->next)
	    {
	      if (join_term->etc == (void *) temp)
		{
		  break;	/* found */
		}
	    }

	  /* check for not added join terms */
	  if (join_term == NULL)
	    {

	      found_join_term = false;	/* init */

	      /* check for attr of other specs */
	      if (temp->or_next == NULL)
		{
		  info1.c_name_num = 0;
		  info1.query_num = 0;
		  info1.s_point_list = NULL;
		  (void) parser_walk_tree (parser, temp->info.expr.arg1,
					   qo_collect_name_spec, &info1,
					   qo_collect_name_spec_post, &info1);

		  info2.c_name_num = 0;
		  info2.query_num = 0;
		  info2.s_point_list = NULL;
		  if (info1.query_num == 0)
		    {
		      (void) parser_walk_tree (parser, temp->info.expr.arg2,
					       qo_collect_name_spec, &info2,
					       qo_collect_name_spec_post,
					       &info2);
		    }

		  if (info1.query_num == 0 && info2.query_num == 0)
		    {
		      /* check for join term related to reduced attr
		       * lhs and rhs has name of other spec
		       *   CASE 1: X.c_name          = Y.attr
		       *   CASE 2: X.c_name + Y.attr = ?
		       *   CASE 3:            Y.attr =          X.c_name
		       *   CASE 4:                 ? = Y.attr + X.c_name
		       */

		      spec1_cnt = pt_length_of_list (info1.s_point_list);
		      spec2_cnt = pt_length_of_list (info2.s_point_list);

		      if (info1.c_name_num)
			{
			  if (spec1_cnt == 0)
			    {	/* CASE 1 */
			      if (spec2_cnt == 1)
				{
				  found_join_term = true;
				}
			    }
			  else if (spec1_cnt == 1)
			    {	/* CASE 2 */
			      if (spec2_cnt == 0)
				{
				  found_join_term = true;
				}
			      else if (spec2_cnt == 1)
				{
				  s_name1 = info1.s_point_list;
				  s_name2 = info2.s_point_list;
				  CAST_POINTER_TO_NODE (s_name1);
				  CAST_POINTER_TO_NODE (s_name2);
				  if (s_name1->info.name.spec_id ==
				      s_name2->info.name.spec_id)
				    {
				      /* X.c_name + Y.attr = Y.attr */
				      found_join_term = true;
				    }
				  else
				    {
				      /* X.c_name + Y.attr = Z.attr */
				      ;	/* nop */
				    }
				}
			    }
			}
		      else if (info2.c_name_num)
			{
			  if (spec2_cnt == 0)
			    {	/* CASE 3 */
			      if (spec1_cnt == 1)
				{
				  found_join_term = true;
				}
			    }
			  else if (spec2_cnt == 1)
			    {	/* CASE 4 */
			      if (spec1_cnt == 0)
				{
				  found_join_term = true;
				}
			      else if (spec1_cnt == 1)
				{
				  s_name1 = info1.s_point_list;
				  s_name2 = info2.s_point_list;
				  CAST_POINTER_TO_NODE (s_name1);
				  CAST_POINTER_TO_NODE (s_name2);
				  if (s_name1->info.name.spec_id ==
				      s_name2->info.name.spec_id)
				    {
				      /* Y.attr = Y.attr + X.c_name */
				      found_join_term = true;
				    }
				  else
				    {
				      /* Z.attr = Y.attr + X.c_name */
				      ;	/* nop */
				    }
				}
			    }
			}
		    }

		  /* free name list */
		  if (info1.s_point_list)
		    {
		      parser_free_tree (parser, info1.s_point_list);
		    }
		  if (info2.s_point_list)
		    {
		      parser_free_tree (parser, info2.s_point_list);
		    }
		}		/* if (temp->or_next == NULL) */

	      if (found_join_term)
		{
		  join_term = parser_copy_tree (parser, temp);
		  assert (join_term != NULL);
		  if (join_term != NULL)
		    {
		      join_term->etc = (void *) temp;	/* mark as added */
		      join_term_list = parser_append_node (join_term,
							   join_term_list);
		    }
		}

	    }			/* if (join_term == NULL) */

	  temp->next = next;	/* restore link */
	}			/* for (term = where; ...) */

      copy_arg2 = false;	/* init */

      if (PT_IS_PARAMETERIZED_TYPE (arg1->type_enum))
	{
	  TP_DOMAIN *dom;
	  DB_VALUE *dbval;
	  DB_TYPE dbval_res_type;

	  /* don't replace node's data type precision, scale
	   */
	  if (PT_IS_CONST (arg2))
	    {
#if 0				/* TODO - do not delete me */
	      if (parser->set_host_var == 0)
		{
		  /* currently, not binding host variables */
		  ;		/* go ahead */
		}
	      else
#endif
		{
		  DB_VALUE dbval_res;

		  DB_MAKE_NULL (&dbval_res);

		  dom = pt_node_to_db_domain (parser, arg1, NULL);
		  dom = tp_domain_cache (dom);

		  dbval = pt_value_to_db (parser, arg2);
		  if (dbval == NULL)
		    {
		      continue;	/* give up */
		    }

		  if (tp_value_coerce (dbval, &dbval_res, dom) !=
		      DOMAIN_COMPATIBLE)
		    {
		      continue;	/* give up */
		    }

		  /* check iff too big literal string */
		  dbval_res_type = DB_VALUE_DOMAIN_TYPE (&dbval_res);
		  if (pr_is_string_type (dbval_res_type))
		    {
		      int string_length = DB_GET_STRING_LENGTH (&dbval_res);

		      if (string_length > DB_MAX_LITERAL_PRECISION)
			{
			  pr_clear_value (&dbval_res);
			  continue;	/* give up */
			}
		    }

		  temp = pt_dbval_to_value (parser, &dbval_res);
		  pr_clear_value (&dbval_res);
		}
	    }
	  else if (qo_is_reduceable_const (arg2))
	    {
	      /* is CAST expr */
	      temp = parser_copy_tree_list (parser, arg2);
	      if (temp == NULL)
		{
		  PT_ERRORm (parser, arg2, MSGCAT_SET_PARSER_SEMANTIC,
			     MSGCAT_SEMANTIC_OUT_OF_MEMORY);
		  continue;	/* give up */
		}
	    }
	  else
	    {
#if 0				/* TODO - may be dead-code */
	      assert (false);
#endif
	      continue;		/* give up */
	    }

	  arg2 = temp;

	  copy_arg2 = true;	/* mark as copy */
	}

      /* replace 'arg1' in 'where' with 'arg2' with location checking */
      temp = pt_get_end_path_node (arg1);

      if (node->node_type == PT_SELECT)
	{			/* query with WHERE condition */
	  node->info.query.q.select.list =
	    pt_lambda_with_arg (parser, node->info.query.q.select.list,
				arg1, arg2,
				(temp->info.name.location > 0 ? true
				 : false),
				1 /* type: check normal func data_type */ ,
				true /* dont_replace */ );
	}
      where =
	pt_lambda_with_arg (parser, where, arg1, arg2,
			    (temp->info.name.location > 0 ? true
			     : false),
			    1 /* type: check normal func data_type */ ,
			    false /* dont_replace: DEFAULT */ );

      /* Leave "where" pointing at the begining of the
         rest of the predicate. We still gurantee loop
         termination because we have removed a term.
         future iterations which do not fall into this
         case will advance to the next term. */
      expr_prev = NULL;
      expr_next = where;

      /* free copied constant column */
      if (copy_arg2)
	{
	  parser_free_tree (parser, arg2);
	}
    }				/* for (expr = where; ...) */

  where = parser_append_node (accumulator, where);

  if (join_term_list)
    {
      /* mark as transitive join terms and append to the WHERE clause */
      for (join_term = join_term_list; join_term; join_term = join_term->next)
	{
	  PT_EXPR_INFO_SET_FLAG (join_term, PT_EXPR_INFO_TRANSITIVE);
	  join_term->etc = (void *) NULL;	/* clear */
	}

      where = parser_append_node (join_term_list, where);
    }

#if !defined(NDEBUG)
  /* check iff cycle ie exists */
  for (expr = where; expr != NULL; expr = expr->next)
    {
      for (temp = expr->next; temp != NULL; temp = temp->next)
	{
	  assert (expr != temp);
	}
    }
#endif

  return where;
}

static void
qo_reduce_equality_terms (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *subquery_ptr;

  switch (node->node_type)
    {
    case PT_SELECT:
      node->info.query.q.select.where =
	qo_reduce_equality_terms_helper (parser, node,
					 node->info.query.q.select.where);
      node->info.query.q.select.having =
	qo_reduce_equality_terms_helper (parser, node,
					 node->info.query.q.select.having);
      break;

    case PT_UPDATE:
      node->info.update.search_cond =
	qo_reduce_equality_terms_helper (parser, node,
					 node->info.update.search_cond);
      break;

    case PT_DELETE:
      node->info.delete_.search_cond =
	qo_reduce_equality_terms_helper (parser, node,
					 node->info.delete_.search_cond);
      break;

    case PT_INSERT:
      subquery_ptr = pt_get_subquery_of_insert_select (node);
      if (subquery_ptr && subquery_ptr->node_type == PT_SELECT)
	{
	  subquery_ptr->info.query.q.select.where =
	    qo_reduce_equality_terms_helper (parser, node,
					     subquery_ptr->info.query.q.
					     select.where);
	}
      break;

    default:
      break;
    }

}

/*
 * qo_reduce_order_by_for () - move orderby_num() to groupby_num()
 *   return: NO_ERROR if successful, otherwise returns error number
 *   parser(in): parser global context info for reentrancy
 *   node(in): query node has ORDER BY
 *
 * Note:
 *   It modifies parser's heap of PT_NODEs(parser->error_msgs)
 *   and effects that remove order by for clause
 */
static int
qo_reduce_order_by_for (PARSER_CONTEXT * parser, PT_NODE * node)
{
  int error = NO_ERROR;
  PT_NODE *ord_num, *grp_num;

  if (node->node_type != PT_SELECT)
    {
      return error;
    }

  ord_num = NULL;
  grp_num = NULL;

  /* move orderby_num() to groupby_num() */
  if (node->info.query.orderby_for)
    {
      /* generate orderby_num(), groupby_num() */
      if (!(ord_num = parser_new_node (parser, PT_EXPR))
	  || !(grp_num = parser_new_node (parser, PT_FUNCTION)))
	{
	  if (ord_num)
	    {
	      parser_free_tree (parser, ord_num);
	    }
	  PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
		     MSGCAT_SEMANTIC_OUT_OF_MEMORY);
	  goto exit_on_error;
	}

      ord_num->type_enum = PT_TYPE_BIGINT;
      ord_num->info.expr.op = PT_ORDERBY_NUM;
      PT_EXPR_INFO_SET_FLAG (ord_num, PT_EXPR_INFO_ORDERBYNUM_C);

      grp_num->type_enum = PT_TYPE_BIGINT;
      grp_num->info.function.function_type = PT_GROUPBY_NUM;
      grp_num->info.function.arg_list = NULL;
      grp_num->info.function.all_or_distinct = PT_ALL;

      /* replace orderby_num() to groupby_num() */
      node->info.query.orderby_for =
	pt_lambda_with_arg (parser, node->info.query.orderby_for,
			    ord_num, grp_num, false /* loc_check: DEFAULT */ ,
			    0 /* type: DEFAULT */ ,
			    false /* dont_replace: DEFAULT */ );

      /* Even though node->info.q.query.q.select has no orderby_num so far,
       * it is a safe guard to prevent potential rewrite problem.
       */
      node->info.query.q.select.list =
	pt_lambda_with_arg (parser, node->info.query.q.select.list,
			    ord_num, grp_num, false /* loc_check: DEFAULT */ ,
			    0 /* type: DEFAULT */ ,
			    false /* dont_replace: DEFAULT */ );

      node->info.query.q.select.having =
	parser_append_node (node->info.query.orderby_for,
			    node->info.query.q.select.having);

      node->info.query.orderby_for = NULL;

      parser_free_tree (parser, ord_num);
      parser_free_tree (parser, grp_num);
    }

exit_on_end:

  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      /* missing compiler error list */
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }
  goto exit_on_end;
}

/*
 * reduce_order_by () -
 *   return: NO_ERROR, if successful, otherwise returns error number
 *   parser(in): parser global context info for reentrancy
 *   node(in): query node has ORDER BY
 *
 * Note:
 *   It modifies parser's heap of PT_NODEs(parser->error_msgs)
 *   and effects that reduce the constant orders
 */
static int
qo_reduce_order_by (PARSER_CONTEXT * parser, PT_NODE * node)
{
  int error = NO_ERROR;
  PT_NODE *order, *order_next, *order_prev, *col, *col2, *col2_next;
  PT_NODE *r, *new_r;
  int i, j;
  int const_order_count, order_move_count;
  bool need_merge_check;
  bool has_orderbynum_with_groupby;

  if (node->node_type != PT_SELECT)
    {
      return error;
    }

  /* init */
  const_order_count = order_move_count = 0;
  need_merge_check = false;
  has_orderbynum_with_groupby = false;

  /* check for merge order by to group by( without DISTINCT and HAVING clause)
   */

  if (node->info.query.all_distinct == PT_DISTINCT)
    {
      ;				/* give up */
    }
  else
    {
      /* if we have rollup, we do not skip the order by */
      if (node->info.query.q.select.group_by
	  && node->info.query.q.select.having == NULL
	  && node->info.query.q.select.group_by->with_rollup == 0
	  && node->info.query.order_by)
	{
	  bool ordbynum_flag;

	  ordbynum_flag = false;	/* init */

	  /* check for orderby_num() in the select list */
	  (void) parser_walk_tree (parser, node->info.query.q.select.list,
				   pt_check_orderbynum_pre, NULL,
				   pt_check_orderbynum_post, &ordbynum_flag);

	  if (ordbynum_flag)
	    {			/* found orderby_num() in the select list */
	      has_orderbynum_with_groupby = true;	/* give up */
	    }
	  else
	    {
	      need_merge_check = true;	/* mark to checking */
	    }
	}
    }

  /* the first phase, do check the current order by */
  if (need_merge_check)
    {
      if (pt_sort_spec_cover (node->info.query.q.select.group_by,
			      node->info.query.order_by))
	{
	  if (qo_reduce_order_by_for (parser, node) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  if (node->info.query.orderby_for == NULL)
	    {
	      /* clear unnecessary node info */
	      parser_free_tree (parser, node->info.query.order_by);
	      node->info.query.order_by = NULL;
	    }

	  need_merge_check = false;	/* clear */
	}
    }

  order_prev = NULL;
  for (order = node->info.query.order_by; order; order = order_next)
    {
      order_next = order->next;

      r = order->info.sort_spec.expr;

      /*
         safe guard: check for integer value.
       */
      if (r->node_type != PT_VALUE)
	{
	  goto exit_on_error;
	}

      col = node->info.query.q.select.list;
      for (i = 1; i < r->info.value.data_value.i; i++)
	{
	  if (col == NULL)
	    {			/* impossible case */
	      break;
	    }
	  col = col->next;
	}

      /*
         safe guard: invalid parse tree
       */
      if (col == NULL)
	{
	  goto exit_on_error;
	}

      col = pt_get_end_path_node (col);
      if (col->node_type == PT_NAME)
	{
	  if (PT_NAME_INFO_IS_FLAGED (col, PT_NAME_INFO_CONSTANT))
	    {
	      /* remove constant order node
	       */
	      if (order_prev == NULL)
		{		/* the first */
		  node->info.query.order_by = order->next;	/* re-link */
		}
	      else
		{
		  order_prev->next = order->next;	/* re-link */
		}
	      order->next = NULL;	/* cut-off */
	      parser_free_tree (parser, order);

	      const_order_count++;	/* increase const entry remove count */

	      continue;		/* go ahead */
	    }

	  /* for non-constant order, change order position to
	   * the same left-most col's position
	   */
	  col2 = node->info.query.q.select.list;
	  for (j = 1; j < i; j++)
	    {
	      col2_next = col2->next;	/* save next link */

	      col2 = pt_get_end_path_node (col2);

	      /* change to the same left-most col */
	      if (pt_name_equal (parser, col2, col))
		{
		  new_r = parser_new_node (parser, PT_VALUE);
		  if (new_r == NULL)
		    {
		      error = MSGCAT_SEMANTIC_OUT_OF_MEMORY;
		      PT_ERRORm (parser, col, MSGCAT_SET_PARSER_SEMANTIC,
				 error);
		      goto exit_on_error;
		    }

		  new_r->type_enum = PT_TYPE_INTEGER;
		  new_r->info.value.data_value.i = j;
		  pt_value_to_db (parser, new_r);
		  parser_free_tree (parser, r);
		  order->info.sort_spec.expr = new_r;
		  order->info.sort_spec.pos_descr.pos_no = j;

		  order_move_count++;	/* increase entry move count */

		  break;	/* exit for-loop */
		}

	      col2 = col2_next;	/* restore next link */
	    }
	}

      order_prev = order;	/* go ahead */
    }

  if (order_move_count > 0)
    {
      PT_NODE *match;

      /* now check for duplicate entries.
       *  - If they match on ascending/descending, remove the second.
       *  - If they do not, generate an error.
       */
      for (order = node->info.query.order_by; order; order = order->next)
	{
	  while ((match =
		  pt_find_order_value_in_list (parser,
					       order->info.sort_spec.expr,
					       order->next)))
	    {
	      if ((order->info.sort_spec.asc_or_desc !=
		   match->info.sort_spec.asc_or_desc)
		  || (pt_to_null_ordering (order) !=
		      pt_to_null_ordering (match)))
		{
		  error = MSGCAT_SEMANTIC_SORT_DIR_CONFLICT;
		  PT_ERRORmf (parser, match, MSGCAT_SET_PARSER_SEMANTIC,
			      error, pt_short_print (parser, match));
		  goto exit_on_error;
		}
	      else
		{
		  order->next = pt_remove_from_list (parser,
						     match, order->next);
		}
	    }			/* while */
	}			/* for (order = ...) */
    }

  if (const_order_count > 0)
    {				/* is reduced */
      /* the second phase, do check with reduced order by */
      if (need_merge_check)
	{
	  if (pt_sort_spec_cover (node->info.query.q.select.group_by,
				  node->info.query.order_by))
	    {
	      if (qo_reduce_order_by_for (parser, node) != NO_ERROR)
		{
		  goto exit_on_error;
		}

	      if (node->info.query.orderby_for == NULL)
		{
		  /* clear unnecessary node info */
		  parser_free_tree (parser, node->info.query.order_by);
		  node->info.query.order_by = NULL;
		}

	      need_merge_check = false;	/* clear */
	    }
	}
      else
	{
	  if (node->info.query.order_by == NULL)
	    {
	      /* move orderby_num() to inst_num() */
	      if (node->info.query.orderby_for)
		{
		  PT_NODE *ord_num, *ins_num;

		  ord_num = NULL;
		  ins_num = NULL;

		  /* generate orderby_num(), inst_num() */
		  if (!(ord_num = parser_new_node (parser, PT_EXPR))
		      || !(ins_num = parser_new_node (parser, PT_EXPR)))
		    {
		      if (ord_num)
			{
			  parser_free_tree (parser, ord_num);
			}
		      PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
				 MSGCAT_SEMANTIC_OUT_OF_MEMORY);
		      goto exit_on_error;
		    }

		  ord_num->type_enum = PT_TYPE_BIGINT;
		  ord_num->info.expr.op = PT_ORDERBY_NUM;
		  PT_EXPR_INFO_SET_FLAG (ord_num, PT_EXPR_INFO_ORDERBYNUM_C);

		  ins_num->type_enum = PT_TYPE_BIGINT;
		  ins_num->info.expr.op = PT_INST_NUM;
		  PT_EXPR_INFO_SET_FLAG (ins_num, PT_EXPR_INFO_INSTNUM_C);

		  /* replace orderby_num() to inst_num() */
		  node->info.query.orderby_for =
		    pt_lambda_with_arg (parser, node->info.query.orderby_for,
					ord_num, ins_num,
					false /* loc_check: DEFAULT */ ,
					0 /* type: DEFAULT */ ,
					false /* dont_replace: DEFAULT */ );

		  node->info.query.q.select.list =
		    pt_lambda_with_arg (parser,
					node->info.query.q.select.list,
					ord_num, ins_num,
					false /* loc_check: DEFAULT */ ,
					0 /* type: DEFAULT */ ,
					false /* dont_replace: DEFAULT */ );

		  node->info.query.q.select.where =
		    parser_append_node (node->info.query.orderby_for,
					node->info.query.q.select.where);

		  node->info.query.orderby_for = NULL;

		  parser_free_tree (parser, ord_num);
		  parser_free_tree (parser, ins_num);
		}
	      else if (has_orderbynum_with_groupby == true)
		{
		  PT_NODE *ord_num, *grp_num;

		  ord_num = NULL;
		  grp_num = NULL;

		  /* generate orderby_num(), groupby_num() */
		  if (!(ord_num = parser_new_node (parser, PT_EXPR))
		      || !(grp_num = parser_new_node (parser, PT_FUNCTION)))
		    {
		      if (ord_num)
			{
			  parser_free_tree (parser, ord_num);
			}
		      PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
				 MSGCAT_SEMANTIC_OUT_OF_MEMORY);
		      goto exit_on_error;
		    }

		  ord_num->type_enum = PT_TYPE_BIGINT;
		  ord_num->info.expr.op = PT_ORDERBY_NUM;
		  PT_EXPR_INFO_SET_FLAG (ord_num, PT_EXPR_INFO_ORDERBYNUM_C);

		  grp_num->type_enum = PT_TYPE_BIGINT;
		  grp_num->info.function.function_type = PT_GROUPBY_NUM;
		  grp_num->info.function.arg_list = NULL;
		  grp_num->info.function.all_or_distinct = PT_ALL;

		  /* replace orderby_num() to groupby_num() */
		  node->info.query.q.select.list =
		    pt_lambda_with_arg (parser,
					node->info.query.q.select.list,
					ord_num, grp_num,
					false /* loc_check: DEFAULT */ ,
					0 /* type: DEFAULT */ ,
					false /* dont_replace: DEFAULT */ );

		  parser_free_tree (parser, ord_num);
		  parser_free_tree (parser, grp_num);
		}
	    }
	}
    }

exit_on_end:

  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      /* missing compiler error list */
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }
  goto exit_on_end;
}

/*
 * qo_converse_sarg_terms () -
 *   return:
 *   parser(in):
 *   where(in): CNF list of WHERE clause
 *
 * Note:
 *      Convert terms of the form 'constant op attr' to 'attr op constant'
 *      by traversing expression tree with prefix order (left child,
 *      right child, and then parent). Convert 'attr op attr' so, LHS has more
 *      common attribute.
 *
 * 	examples:
 *  	0. where 5 = a                     -->  where a = 5
 *  	1. where -5 = -a                   -->  where a = 5
 *  	2. where -5 = -(-a)                -->  where a = -5
 *  	3. where 5 = -a                    -->  where a = -5
 *  	4. where 5 = -(-a)                 -->  where a = 5
 *  	5. where 5 > x.a and/or x.a = y.b  -->  where x.a < 5 and/or x.a = y.b
 *  	6. where b = a or c = a            -->  where a = b or a = c
 *  	7. where b = -a or c = a           -->  where a = -b or a = c
 *  	8. where b = a or c = a            -->  where a = b or a = c
 *  	9. where a = b or b = c or d = b   -->  where b = a or b = c or b = d
 */
static void
qo_converse_sarg_terms (PARSER_CONTEXT * parser, PT_NODE * where)
{
  PT_NODE *cnf_node, *dnf_node, *arg1, *arg2, *arg1_arg1, *arg2_arg1;
  PT_OP_TYPE op_type;
  PT_NODE *attr, *attr_list;
  int arg1_cnt, arg2_cnt;


  /* traverse CNF list */
  for (cnf_node = where; cnf_node; cnf_node = cnf_node->next)
    {
      attr_list = NULL;		/* init */

      /* STEP 1: traverse DNF list to generate attr_list */
      for (dnf_node = cnf_node; dnf_node; dnf_node = dnf_node->or_next)
	{

	  if (dnf_node->node_type != PT_EXPR)
	    {
	      continue;
	    }

	  op_type = dnf_node->info.expr.op;
	  /* not CNF/DNF form; give up */
	  if (op_type == PT_AND || op_type == PT_OR)
	    {
	      if (attr_list)
		{
		  parser_free_tree (parser, attr_list);
		  attr_list = NULL;
		}

	      break;		/* immediately, exit loop */
	    }

	  arg1 = dnf_node->info.expr.arg1;

	  arg1_arg1 = ((pt_is_expr_node (arg1)
			&& arg1->info.expr.op == PT_UNARY_MINUS)
		       ? arg1->info.expr.arg1 : NULL);
	  while (pt_is_expr_node (arg1)
		 && arg1->info.expr.op == PT_UNARY_MINUS)
	    {
	      arg1 = arg1->info.expr.arg1;
	    }

	  if (op_type == PT_BETWEEN && arg1_arg1 && pt_is_attr (arg1))
	    {
	      /* term in the form of '-attr between opd1 and opd2'
	         convert to '-attr >= opd1 and -attr <= opd2' */

	      /* check for one range spec */
	      if (cnf_node == dnf_node && dnf_node->or_next == NULL)
		{
		  arg2 = dnf_node->info.expr.arg2;
		  assert (arg2->node_type == PT_EXPR);
		  /* term of '-attr >= opd1' */
		  dnf_node->info.expr.arg2 = arg2->info.expr.arg1;
		  op_type = dnf_node->info.expr.op = PT_GE;
		  /* term of '-attr <= opd2' */
		  arg2->info.expr.arg1 =
		    parser_copy_tree (parser, dnf_node->info.expr.arg1);
		  arg2->info.expr.op = PT_LE;
		  /* term of 'and' */
		  arg2->next = dnf_node->next;
		  dnf_node->next = arg2;
		}
	    }

	  arg2 = dnf_node->info.expr.arg2;

	  while (pt_is_expr_node (arg2)
		 && arg2->info.expr.op == PT_UNARY_MINUS)
	    {
	      arg2 = arg2->info.expr.arg1;
	    }

	  /* add sargable attribute to attr_list */
	  if (arg1 && arg2 && pt_converse_op (op_type) != 0)
	    {
	      if (pt_is_attr (arg1))
		{
		  for (attr = attr_list; attr; attr = attr->next)
		    {
		      if (pt_name_equal (parser, attr, arg1))
			{
			  attr->line_number++;	/* increase attribute count */
			  break;
			}
		    }

		  /* not found; add new attribute */
		  if (attr == NULL)
		    {
		      attr = pt_point (parser, arg1);
		      if (attr != NULL)
			{
			  attr->line_number = 1;	/* set attribute count */

			  attr_list = parser_append_node (attr_list, attr);
			}
		    }
		}

	      if (pt_is_attr (arg2))
		{
		  for (attr = attr_list; attr; attr = attr->next)
		    {
		      if (pt_name_equal (parser, attr, arg2))
			{
			  attr->line_number++;	/* increase attribute count */
			  break;
			}
		    }

		  /* not found; add new attribute */
		  if (attr == NULL)
		    {
		      attr = pt_point (parser, arg2);

		      if (attr != NULL)
			{
			  attr->line_number = 1;	/* set attribute count */

			  attr_list = parser_append_node (attr_list, attr);
			}
		    }
		}
	    }
	}

      /* STEP 2: re-traverse DNF list to converse sargable terms */
      for (dnf_node = cnf_node; dnf_node; dnf_node = dnf_node->or_next)
	{
	  if (dnf_node->node_type != PT_EXPR)
	    continue;

	  /* filter out unary minus nodes */
	  while ((arg1 = dnf_node->info.expr.arg1)
		 && (arg2 = dnf_node->info.expr.arg2))
	    {
	      op_type = pt_converse_op (dnf_node->info.expr.op);
	      arg1_arg1 = ((pt_is_expr_node (arg1)
			    && arg1->info.expr.op == PT_UNARY_MINUS)
			   ? arg1->info.expr.arg1 : NULL);
	      arg2_arg1 = ((pt_is_expr_node (arg2)
			    && arg2->info.expr.op == PT_UNARY_MINUS)
			   ? arg2->info.expr.arg1 : NULL);

	      if (arg1_arg1 && arg2_arg1)
		{
		  /* term in the form of '-something op -something' */
		  dnf_node->info.expr.arg1 = arg1->info.expr.arg1;
		  arg1->info.expr.arg1 = NULL;
		  parser_free_tree (parser, arg1);
		  dnf_node->info.expr.arg2 = arg2->info.expr.arg1;
		  arg2->info.expr.arg1 = NULL;
		  parser_free_tree (parser, arg2);
		}
	      else if (op_type != 0 && arg1_arg1
		       && (pt_is_attr (arg1_arg1)
			   || (pt_is_expr_node (arg1_arg1)
			       && arg1_arg1->info.expr.op == PT_UNARY_MINUS))
		       && pt_is_const (arg2))
		{
		  /* term in the form of '-attr op const' or
		     '-(-something) op const' */
		  dnf_node->info.expr.arg1 = arg1->info.expr.arg1;
		  arg1->info.expr.arg1 = arg2;
		  dnf_node->info.expr.arg2 = arg1;
		}
	      else if (op_type != 0 && arg2_arg1
		       && (pt_is_attr (arg2->info.expr.arg1)
			   || (pt_is_expr_node (arg2_arg1)
			       && arg2_arg1->info.expr.op == PT_UNARY_MINUS))
		       && pt_is_const (arg1))
		{
		  /* term in the form of 'const op -attr' or
		     'const op -(-something)' */
		  dnf_node->info.expr.arg2 = arg2->info.expr.arg1;
		  arg2->info.expr.arg1 = arg1;
		  dnf_node->info.expr.arg1 = arg2;
		}
	      else
		{
		  break;
		}

	      /* swap term's operator */
	      dnf_node->info.expr.op = op_type;
	      assert (dnf_node->info.expr.op != 0);
	    }

	  op_type = dnf_node->info.expr.op;
	  arg1 = dnf_node->info.expr.arg1;
	  arg2 = dnf_node->info.expr.arg2;

	  if (op_type == PT_AND)
	    {
	      /* not CNF form; what do I have to do? */

	      /* traverse left child */
	      qo_converse_sarg_terms (parser, arg1);
	      /* traverse right child */
	      qo_converse_sarg_terms (parser, arg2);

	    }
	  else if (op_type == PT_OR)
	    {
	      /* not DNF form; what do I have to do? */

	      /* traverse left child */
	      qo_converse_sarg_terms (parser, arg1);
	      /* traverse right child */
	      qo_converse_sarg_terms (parser, arg2);

	    }
	  /* sargable term, where 'op_type' is one of
	   * '=', '<' '<=', '>', or '>='
	   */
	  else if (arg1 && arg2
		   && (op_type = pt_converse_op (op_type)) != 0
		   && pt_is_attr (arg2))
	    {

	      if (pt_is_attr (arg1))
		{
		  /* term in the form of 'attr op attr' */

		  arg1_cnt = arg2_cnt = 0;	/* init */
		  for (attr = attr_list; attr; attr = attr->next)
		    {
		      if (pt_name_equal (parser, attr, arg1))
			{
			  arg1_cnt = attr->line_number;
			}
		      else if (pt_name_equal (parser, attr, arg2))
			{
			  arg2_cnt = attr->line_number;
			}

		      if (arg1_cnt && arg2_cnt)
			{
			  break;	/* already found both arg1, arg2 */
			}
		    }

		  if (!arg1_cnt || !arg2_cnt)
		    {
		      /* something wrong; skip and go ahead */
		      continue;
		    }

		  /* swap */
		  if (arg1_cnt < arg2_cnt)
		    {
		      dnf_node->info.expr.arg1 = arg2;
		      dnf_node->info.expr.arg2 = arg1;
		      dnf_node->info.expr.op = op_type;
		      assert (dnf_node->info.expr.op != 0);
		    }
		}
	      else
		{
		  /* term in the form of 'non-attr op attr' */

		  /* swap */

		  dnf_node->info.expr.arg1 = arg2;
		  dnf_node->info.expr.arg2 = arg1;
		  dnf_node->info.expr.op = op_type;
		  assert (dnf_node->info.expr.op != 0);
		}
	    }
	}

      if (attr_list)
	{
	  parser_free_tree (parser, attr_list);
	  attr_list = NULL;
	}
    }
}

/*
 * qo_search_comp_pair_term () -
 *   return:
 *   parser(in):
 *   start(in):
 */
static PT_NODE *
qo_search_comp_pair_term (PARSER_CONTEXT * parser, PT_NODE * start)
{
  PT_NODE *node;
  PT_OP_TYPE op_type1, op_type2;
  PT_NODE *arg_prior, *arg_prior_start;

  arg_prior = arg_prior_start = NULL;

  switch (start->info.expr.op)
    {
    case PT_GE:
    case PT_GT:
      op_type1 = PT_LE;
      op_type2 = PT_LT;
      break;
    case PT_LE:
    case PT_LT:
      op_type1 = PT_GE;
      op_type2 = PT_GT;
      break;
    default:
      return NULL;
    }

  arg_prior_start = start->info.expr.arg1;	/* original value */

  /* search CNF list */
  for (node = start; node; node = node->next)
    {
      if (node->node_type != PT_EXPR || node->or_next != NULL)
	{
	  /* neither expression node nor one predicate term */
	  continue;
	}

      if (node->info.expr.location != start->info.expr.location)
	{
	  continue;
	}

      arg_prior = pt_get_first_arg (node);
      if (arg_prior == NULL)
	{
	  continue;
	}

      if (node->info.expr.op == op_type1 || node->info.expr.op == op_type2)
	{
	  if (pt_is_attr (arg_prior)
	      && (pt_check_path_eq (parser, arg_prior, arg_prior_start) == 0))
	    {
	      /* found 'attr op expr' term */
	      break;
	    }
	}
    }

  return node;
}

/*
 * qo_reduce_comp_pair_terms () - Convert a pair of comparison terms to one
 *			       BETWEEN term
 *   return:
 *   parser(in):
 *   wherep(in): pointer to WHERE
 *
 * Note:
 * 	examples:
 *  	1) where a<=20 and a=>10        -->  where a between 10 and(ge_le) 20
 *  	2) where a<20 and a>10          -->  where a between 10 gt_lt 20
 *  	3) where a<B.b and a>=B.c       -->  where a between B.c ge_lt B.b
 */
static void
qo_reduce_comp_pair_terms (PARSER_CONTEXT * parser, PT_NODE ** wherep)
{
  PT_NODE *node, *pair, *lower, *upper, *prev;

  /* traverse CNF list */
  for (node = *wherep; node; node = node->next)
    {
      if (node->node_type != PT_EXPR
	  || !pt_is_attr (node->info.expr.arg1) || node->or_next != NULL)
	{
	  /* neither expression node, LHS is attribute, nor one predicate
	     term */
	  continue;
	}

      switch (node->info.expr.op)
	{
	case PT_GT:
	case PT_GE:
	  lower = node;
	  upper = pair = qo_search_comp_pair_term (parser, node);
	  break;
	case PT_LT:
	case PT_LE:
	  lower = pair = qo_search_comp_pair_term (parser, node);
	  upper = node;
	  break;
	default:
	  /* not comparison term; continue to next node */
	  continue;
	}
      if (!pair)
	{
	  /* there's no pair comparison term having the same attribute */
	  continue;
	}

      /* the node will be converted to BETWEEN node and the pair node will be
         converted to the right operand(arg2) of BETWEEN node denoting the
         range of BETWEEN such as BETWEEN_GE_LE, BETWEEN_GE_LT, BETWEEN_GT_LE,
         and BETWEEN_GT_LT */

      /* make the pair node to the right operand of BETWEEN node */
      if (pt_comp_to_between_op (lower->info.expr.op, upper->info.expr.op,
				 PT_REDUCE_COMP_PAIR_TERMS,
				 &pair->info.expr.op) != 0)
	{
	  /* cannot be occurred but something wrong */
	  continue;
	}
      parser_free_tree (parser, pair->info.expr.arg1);
      pair->info.expr.arg1 = lower->info.expr.arg2;
      pair->info.expr.arg2 = upper->info.expr.arg2;
      /* should set pair->info.expr.arg1 before pair->info.expr.arg2 */
      /* make the node to BETWEEN node */
      node->info.expr.op = PT_BETWEEN;
      /* revert BETWEEN_GE_LE to BETWEEN_AND */
      if (pair->info.expr.op == PT_BETWEEN_GE_LE)
	{
	  pair->info.expr.op = PT_BETWEEN_AND;
	}
      node->info.expr.arg2 = pair;

      /* adjust linked list */
      for (prev = node; prev->next != pair; prev = prev->next)
	{
	  ;
	}
      prev->next = pair->next;
      pair->next = NULL;
    }
}

#if 0
/*
 * pt_is_ascii_string_value_node () -
 *   return: whether the node is a non-national string value (CHAR or VARCHAR)
 *   node(in):
 */
static bool
pt_is_ascii_string_value_node (const PT_NODE * const node)
{
  return PT_IS_VALUE_NODE (node) && PT_IS_CHAR_STRING_TYPE (node->type_enum);
}
#endif

static PT_NODE *
qo_allocate_like_bound_for_index_scan (PARSER_CONTEXT * const parser,
				       PT_NODE * const like,
				       PT_NODE * const pattern,
				       PT_NODE * const escape,
				       const bool allocate_lower_bound)
{
  PT_NODE *bound = NULL;
  PT_NODE *expr_pattern = NULL;
  PT_NODE *expr_escape = NULL;

  bound =
    pt_expression_2 (parser,
		     allocate_lower_bound ? PT_LIKE_LOWER_BOUND :
		     PT_LIKE_UPPER_BOUND, NULL, NULL);
  if (bound == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto error_exit;
    }

  bound->info.expr.location = like->info.expr.location;

  bound->type_enum = pattern->type_enum;

  expr_pattern = parser_copy_tree (parser, pattern);
  if (expr_pattern == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto error_exit;
    }

  bound->info.expr.arg1 = expr_pattern;

  if (escape != NULL)
    {
      if (PT_IS_NULL_NODE (escape))
	{
	  expr_escape = pt_make_string_value (parser, "\\");
	}
      else
	{
	  expr_escape = parser_copy_tree (parser, escape);
	}

      if (expr_escape == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  goto error_exit;
	}
    }

  bound->info.expr.arg2 = expr_escape;

  /* copy data type */
  assert (bound->data_type == NULL);
  bound->data_type = parser_copy_tree (parser, pattern->data_type);

  return bound;

error_exit:
  if (bound != NULL)
    {
      parser_free_tree (parser, bound);
    }
  return NULL;
}

/*
 * qo_rewrite_like_for_index_scan ()
 *   parser(in):
 *   like(in):
 *   pattern(in):
 *   escape(in):
 *
 * Note: See the notes of the db_get_info_for_like_optimization function for
 *       details on what rewrites can be performed. This function will always
 *       rewrite to form 3.1.
 */
static void
qo_rewrite_like_for_index_scan (PARSER_CONTEXT * const parser,
				PT_NODE * const like,
				PT_NODE * const pattern,
				PT_NODE * const escape)
{
  PT_NODE *between = NULL;
  PT_NODE *between_and = NULL;
  PT_NODE *lower = NULL;
  PT_NODE *upper = NULL;
  PT_NODE *match_col = NULL;

  between = pt_expression_1 (parser, PT_BETWEEN, NULL);
  if (between == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto error_exit;
    }

  between->type_enum = PT_TYPE_LOGICAL;
  between->info.expr.location = like->info.expr.location;

  match_col = parser_copy_tree (parser, like->info.expr.arg1);
  if (match_col == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto error_exit;
    }

  between->info.expr.arg1 = match_col;

  between_and = pt_expression_2 (parser, PT_BETWEEN_GE_LT, NULL, NULL);
  if (between_and == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto error_exit;
    }

  between->info.expr.arg2 = between_and;

  between_and->type_enum = PT_TYPE_LOGICAL;
  between_and->info.expr.location = like->info.expr.location;

  if (pattern->data_type != NULL)
    {
      int coll_id;
      DB_VALUE *value;

      if (pt_get_collation_info (match_col, &coll_id) == true)
	{
	  assert (coll_id >= 0);
	  pattern->data_type->info.data_type.collation_id = coll_id;

	  value = pt_value_to_db (parser, pattern);
	  if (value != NULL && TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (value)))
	    {
	      db_string_put_cs_and_collation (value, coll_id);
	    }
	}
    }

  lower = qo_allocate_like_bound_for_index_scan (parser, like, pattern,
						 escape, true);
  if (lower == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto error_exit;
    }

  between_and->info.expr.arg1 = lower;

  upper = qo_allocate_like_bound_for_index_scan (parser, like, pattern,
						 escape, false);
  if (upper == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto error_exit;
    }

  between_and->info.expr.arg2 = upper;

  between->next = like->next;
  like->next = between;

  return;

error_exit:
  if (between != NULL)
    {
      parser_free_tree (parser, between);
      between = NULL;
    }
  return;
}

/*
 * qo_check_like_expression_pre - Checks to see if an expression is safe to
 *                                use in the LIKE rewrite optimization
 *                                performed by qo_rewrite_like_for_index_scan
 *
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in/out): A pointer to a bool value that represents whether the
 *                expression is safe for the rewrite.
 *   continue_walk(in/out):
 *
 * Note: Expressions are first filtered by the pt_is_pseudo_const function.
 *       However, in addition to what that function considers a "constant"
 *       for index scans, we also include PT_NAME and PT_DOT nodes and query
 *       nodes. Some of them might be pseudo-constant and usable during the
 *       index scan, but since we have no easy way to tell we prefer to
 *       exclude them.
 */
static PT_NODE *
qo_check_like_expression_pre (UNUSED_ARG PARSER_CONTEXT * parser,
			      PT_NODE * node, void *arg, int *continue_walk)
{
  bool *const like_expression_not_safe = (bool *) arg;

  if (node == NULL)
    {
      return node;
    }

  if (PT_IS_QUERY (node) || PT_IS_NAME_NODE (node) || PT_IS_DOT_NODE (node))
    {
      *like_expression_not_safe = true;
      *continue_walk = PT_STOP_WALK;
      return node;
    }

  return node;
}

/*
 * qo_rewrite_like_terms ()
 *   return:
 *   parser(in):
 *   cnf_list(in):
 */
static void
qo_rewrite_like_terms (PARSER_CONTEXT * parser, PT_NODE ** cnf_list)
{
  PT_NODE *cnf_node = NULL;

  for (cnf_node = *cnf_list; cnf_node != NULL; cnf_node = cnf_node->next)
    {
      PT_NODE *crt_expr = NULL;

      for (crt_expr = cnf_node; crt_expr != NULL;
	   crt_expr = crt_expr->or_next)
	{
	  PT_NODE *compared_expr = NULL;
	  PT_NODE *pattern = NULL;
	  PT_NODE *escape = NULL;
	  PT_NODE *arg2 = NULL;
	  PT_TYPE_ENUM pattern_type, escape_type = PT_TYPE_NONE;

	  if (!PT_IS_EXPR_NODE_WITH_OPERATOR (crt_expr, PT_LIKE))
	    {
	      /* TODO Investigate optimizing PT_NOT_LIKE expressions also. */
	      continue;
	    }

	  compared_expr = pt_get_first_arg (crt_expr);
	  if (compared_expr == NULL)
	    {
	      continue;
	    }

	  if (!pt_is_attr (compared_expr))
	    {
	      /* LHS is not an attribute
	       * so it cannot currently have an index.
	       * The transformation could still be useful as it might provide
	       * faster execution time in some scenarios.
	       */
	      continue;
	    }

	  if (!PT_IS_CHAR_STRING_TYPE (compared_expr->type_enum))
	    {
	      continue;
	    }

	  arg2 = crt_expr->info.expr.arg2;
	  if (PT_IS_EXPR_NODE_WITH_OPERATOR (arg2, PT_LIKE_ESCAPE))
	    {
	      /* TODO LIKE handling might be easier if the parser saved the
	         escape sequence in arg3 of the PT_LIKE node. */
	      pattern = arg2->info.expr.arg1;
	      escape = arg2->info.expr.arg2;
	      assert (escape != NULL);
	    }
	  else
	    {
	      pattern = arg2;
	      escape = NULL;
	    }

	  pattern_type = pattern->type_enum;

	  if (escape != NULL)
	    {
	      escape_type = escape->type_enum;
	    }

	  if (crt_expr == cnf_node && crt_expr->or_next == NULL)
	    {
	      /* The LIKE predicate in CNF is not chained in an OR list, so we
	         can easily split it into several predicates chained with
	         AND. Supporting the case:
	         col LIKE expr1 OR predicate
	         would make it difficult to rewrite the query because we need
	         to preserve the CNF.
	       */
	      /* TODO We should check that the column is indexed. Otherwise
	         it might not be worth the effort to do this rewrite. */
	      if (pt_is_pseudo_const (pattern)
		  && (escape == NULL || PT_IS_NULL_NODE (escape)
		      || pt_is_pseudo_const (escape)))
		{
		  bool like_expression_not_safe = false;

		  (void *) parser_walk_tree (parser, pattern,
					     qo_check_like_expression_pre,
					     &like_expression_not_safe, NULL,
					     NULL);
		  if (like_expression_not_safe)
		    {
		      continue;
		    }
		  (void *) parser_walk_tree (parser, escape,
					     qo_check_like_expression_pre,
					     &like_expression_not_safe, NULL,
					     NULL);
		  if (like_expression_not_safe)
		    {
		      continue;
		    }
		  qo_rewrite_like_for_index_scan (parser, crt_expr, pattern,
						  escape);
		}
	    }
	}
    }
}

/*
 * qo_set_value_to_range_list () -
 *   return:
 *   parser(in):
 *   node(in):
 */
static PT_NODE *
qo_set_value_to_range_list (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *set_val, *list, *last, *range;

  list = last = NULL;
  if (node->node_type == PT_VALUE)
    {
      assert (PT_IS_COLLECTION_TYPE (node->type_enum));

      set_val = node->info.value.data_value.set;
    }
  else if (node->node_type == PT_FUNCTION)
    {
      assert (PT_IS_COLLECTION_TYPE (node->type_enum));
      assert (node->info.function.function_type == F_SEQUENCE);

      set_val = node->info.function.arg_list;
    }
  else
    {
      assert (false);		/* is impossible */
      set_val = NULL;
    }

  while (set_val)
    {
      range = parser_new_node (parser, PT_EXPR);
      if (range == NULL)
	{
	  goto exit_on_error;
	}

      range->type_enum = PT_TYPE_LOGICAL;
      range->info.expr.op = PT_BETWEEN_EQ_NA;
      range->info.expr.arg1 = parser_copy_tree (parser, set_val);
      range->info.expr.arg2 = NULL;
      range->info.expr.location = set_val->info.expr.location;
#if defined(RYE_DEBUG)
      range->next = NULL;
      range->or_next = NULL;
#endif /* RYE_DEBUG */
      if (last)
	{
	  last->or_next = range;
	}
      else
	{
	  list = range;
	}
      last = range;
      set_val = set_val->next;
    }

  return list;

exit_on_error:
  if (list)
    {
      parser_free_tree (parser, list);
    }

  return NULL;
}


/*
 * qo_convert_to_range_helper () -
 *   return:
 *   parser(in):
 *   node(in):
 */
static void
qo_convert_to_range_helper (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *between_and, *sibling, *last, *prev, *in_arg2;
  PT_OP_TYPE op_type;
  PT_NODE *node_prior = NULL;
  PT_NODE *sibling_prior = NULL;

  assert (PT_IS_EXPR_NODE (node));

  node_prior = pt_get_first_arg (node);
  if (node_prior == NULL)
    {
      assert (false);		/* something wrong */
      return;			/* error; stop converting */
    }

  /* convert the given node to RANGE node */

  /* construct BETWEEN_AND node as arg2(RHS) of RANGE node */
  op_type = node->info.expr.op;
  switch (op_type)
    {
    case PT_EQ:
      between_and = parser_new_node (parser, PT_EXPR);
      if (!between_and)
	{
	  return;		/* error; stop converting */
	}
      between_and->type_enum = PT_TYPE_LOGICAL;
      between_and->info.expr.op = PT_BETWEEN_EQ_NA;
      between_and->info.expr.arg1 = node->info.expr.arg2;
      between_and->info.expr.arg2 = NULL;
      between_and->info.expr.location = node->info.expr.location;
#if defined(RYE_DEBUG)
      between_and->next = NULL;
      between_and->or_next = NULL;
#endif /* RYE_DEBUG */
      break;
    case PT_GT:
    case PT_GE:
    case PT_LT:
    case PT_LE:
      between_and = parser_new_node (parser, PT_EXPR);
      if (!between_and)
	{
	  return;		/* error; stop converting */
	}
      between_and->type_enum = PT_TYPE_LOGICAL;
      between_and->info.expr.op = (op_type == PT_GT ? PT_BETWEEN_GT_INF :
				   (op_type == PT_GE ? PT_BETWEEN_GE_INF :
				    (op_type == PT_LT ? PT_BETWEEN_INF_LT :
				     PT_BETWEEN_INF_LE)));
      between_and->info.expr.arg1 = node->info.expr.arg2;
      between_and->info.expr.arg2 = NULL;
      between_and->info.expr.location = node->info.expr.location;
#if defined(RYE_DEBUG)
      between_and->next = NULL;
      between_and->or_next = NULL;
#endif
      break;
    case PT_BETWEEN:
      between_and = node->info.expr.arg2;
      assert (between_and->node_type == PT_EXPR);
      /* replace PT_BETWEEN_AND with PT_BETWEEN_GE_LE */
      if (between_and->info.expr.op == PT_BETWEEN_AND)
	{
	  between_and->info.expr.op = PT_BETWEEN_GE_LE;
	}
      break;
    case PT_IS_IN:
      in_arg2 = node->info.expr.arg2;
      if (PT_IS_COLLECTION_TYPE (node->type_enum)
	  || PT_IS_QUERY_NODE_TYPE (in_arg2->node_type)
	  || !PT_IS_COLLECTION_TYPE (in_arg2->type_enum))
	{
	  /* subquery cannot be converted to RANGE */
	  return;
	}
      between_and = qo_set_value_to_range_list (parser, in_arg2);
      if (!between_and)
	{
#if 1				/* TODO - trace */
	  assert (false);
#endif
	  return;		/* error; stop converting */
	}
      /* free the converted set value node, which is the operand of IN */
      parser_free_tree (parser, in_arg2);
      break;
    case PT_RANGE:
      /* already converted. do nothing */
      return;
    default:
      /* unsupported operator; only PT_EQ, PT_GT, PT_GE, PT_LT, PT_LE, and
         PT_BETWEEN can be converted to RANGE */
      return;			/* error; stop converting */
    }
#if 0
  between_and->next = between_and->or_next = NULL;
#endif
  /* change the node to RANGE */
  node->info.expr.op = PT_RANGE;
  node->info.expr.arg2 = last = between_and;
  while (last->or_next)
    {
      last = last->or_next;
    }


  /* link all nodes in the list whose LHS is the same attribute with the
     RANGE node */

  /* search DNF list from the next to the node and keep track of the pointer
     to previous node */
  prev = node;
  while ((sibling = prev->or_next))
    {
      if (sibling->node_type != PT_EXPR)
	{
	  /* sibling is not an expression node */
	  prev = prev->or_next;
	  continue;
	}

      sibling_prior = pt_get_first_arg (sibling);
      if (sibling_prior == NULL)
	{
	  assert (false);	/* something wrong */
	  prev = prev->or_next;
	  continue;
	}

      if (!pt_is_attr (sibling_prior) && !pt_is_instnum (sibling_prior))
	{
	  /* LHS is not an attribute */
	  prev = prev->or_next;
	  continue;
	}

      if ((node_prior->node_type != sibling_prior->node_type)
	  || (pt_is_attr (node_prior)
	      && pt_is_attr (sibling_prior)
	      && pt_check_path_eq (parser, node_prior, sibling_prior)))
	{
	  /* pt_check_path_eq() return non-zero if two are different */
	  prev = prev->or_next;
	  continue;
	}

      /* found a node of the same attribute */

      /* construct BETWEEN_AND node as the tail of RANGE node's range list */
      op_type = sibling->info.expr.op;
      switch (op_type)
	{
	case PT_EQ:
	  between_and = parser_new_node (parser, PT_EXPR);
	  if (!between_and)
	    {
	      return;		/* error; stop converting */
	    }
	  between_and->type_enum = PT_TYPE_LOGICAL;
	  between_and->info.expr.op = PT_BETWEEN_EQ_NA;
	  between_and->info.expr.arg1 = sibling->info.expr.arg2;
	  between_and->info.expr.arg2 = NULL;
	  between_and->info.expr.location = sibling->info.expr.location;
#if defined(RYE_DEBUG)
	  between_and->next = NULL;
	  between_and->or_next = NULL;
#endif /* RYE_DEBUG */
	  break;
	case PT_GT:
	case PT_GE:
	case PT_LT:
	case PT_LE:
	  between_and = parser_new_node (parser, PT_EXPR);
	  if (!between_and)
	    {
	      return;		/* error; stop converting */
	    }
	  between_and->type_enum = PT_TYPE_LOGICAL;
	  between_and->info.expr.op = (op_type == PT_GT ? PT_BETWEEN_GT_INF :
				       (op_type == PT_GE ? PT_BETWEEN_GE_INF :
					(op_type ==
					 PT_LT ? PT_BETWEEN_INF_LT :
					 PT_BETWEEN_INF_LE)));
	  between_and->info.expr.arg1 = sibling->info.expr.arg2;
	  between_and->info.expr.arg2 = NULL;
	  between_and->info.expr.location = sibling->info.expr.location;
#if defined(RYE_DEBUG)
	  between_and->next = NULL;
	  between_and->or_next = NULL;
#endif
	  break;
	case PT_BETWEEN:
	  between_and = sibling->info.expr.arg2;
	  assert (between_and->node_type == PT_EXPR);
	  /* replace PT_BETWEEN_AND with PT_BETWEEN_GE_LE */
	  if (between_and->info.expr.op == PT_BETWEEN_AND)
	    {
	      between_and->info.expr.op = PT_BETWEEN_GE_LE;
	    }
	  break;
	case PT_IS_IN:
	  in_arg2 = sibling->info.expr.arg2;
	  if (PT_IS_COLLECTION_TYPE (sibling->type_enum)
	      || PT_IS_QUERY_NODE_TYPE (in_arg2->node_type)
	      || !PT_IS_COLLECTION_TYPE (in_arg2->type_enum))
	    {
	      /* subquery cannot be converted to RANGE */
	      prev = prev->or_next;
	      continue;
	    }
	  between_and = qo_set_value_to_range_list (parser, in_arg2);
	  if (!between_and)
	    {
	      prev = prev->or_next;
	      continue;
	    }
	  /* free the converted set value node, which is the operand of IN */
	  parser_free_tree (parser, in_arg2);
	  break;
	default:
	  /* unsupported operator; continue to next node */
	  prev = prev->or_next;
	  continue;
	}			/* switch (op_type) */
#if 0
      between_and->next = between_and->or_next = NULL;
#endif
      /* append to the range list */
      last->or_next = between_and;
      last = between_and;
      while (last->or_next)
	{
	  last = last->or_next;
	}

      /* delete the node and its arg1(LHS), and adjust linked list */
      prev->or_next = sibling->or_next;
      sibling->next = sibling->or_next = NULL;
      sibling->info.expr.arg2 = NULL;	/* parser_free_tree() will handle 'arg1' */
      parser_free_tree (parser, sibling);
    }
}

/*
 * qo_merge_range_helper () -
 *   return: valid
 *   parser(in):
 *   node(in):
 */
static DNF_MERGE_RANGE_RESULT
qo_merge_range_helper (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *range;

  for (range = node->info.expr.arg2; range; range = range->or_next)
    {
      if (range->info.expr.op == PT_BETWEEN_EQ_NA
	  && range->info.expr.arg2 != NULL)
	{
	  parser_free_tree (parser, range->info.expr.arg2);
	  range->info.expr.arg2 = NULL;
	}
    }

  return DNF_RANGE_VALID;
}

/*
 * qo_convert_to_range () - Convert comparison term to RANGE term
 *   return:
 *   parser(in):
 *   wherep(in): pointer to WHERE list
 *
 * Note:
 * 	examples:
 *  	1. WHERE a<=20 AND a=>10   -->  WHERE a RANGE(10 GE_LE 20)
 *  	2. WHERE a<10              -->  WHERE a RANGE(10 INF_LT)
 *  	3. WHERE a>=20             -->  WHERE a RANGE(20 GE_INF)
 *  	4. WHERE a<10 OR a>=20     -->  WHERE a RANGE(10 INF_LT, 20 GE_INF)
 */
static void
qo_convert_to_range (PARSER_CONTEXT * parser, PT_NODE ** wherep)
{
  PT_NODE *cnf_node, *dnf_node, *cnf_prev, *dnf_prev;
  PT_NODE *arg1_prior;
  DNF_MERGE_RANGE_RESULT result;

  /* traverse CNF list and keep track of the pointer to previous node */
  cnf_prev = NULL;
  while ((cnf_node = (cnf_prev ? cnf_prev->next : *wherep)))
    {

      /* traverse DNF list and keep track of the pointer to previous node */
      dnf_prev = NULL;
      while ((dnf_node = (dnf_prev ? dnf_prev->or_next : cnf_node)))
	{
	  if (dnf_node->node_type != PT_EXPR)
	    {
	      /* dnf_node is not an expression node */
	      dnf_prev = dnf_prev ? dnf_prev->or_next : dnf_node;
	      continue;
	    }

	  arg1_prior = pt_get_first_arg (dnf_node);
	  if (arg1_prior == NULL)
	    {
	      assert (false);	/* something wrong */
	      dnf_prev = dnf_prev ? dnf_prev->or_next : dnf_node;
	      continue;
	    }

	  if (!pt_is_attr (arg1_prior) && !pt_is_instnum (arg1_prior))
	    {
	      /* LHS is not an attribute */
	      dnf_prev = dnf_prev ? dnf_prev->or_next : dnf_node;
	      continue;
	    }

#if 0				/* TODO - wait for removing auto parameterize */
	  if (dnf_node == cnf_node && dnf_node->or_next == NULL
	      && !pt_is_instnum (arg1_prior))
	    {
	      if (dnf_node->info.expr.op == PT_EQ)
		{
		  /* do not convert one predicate '=' term to RANGE */
		  dnf_prev = dnf_prev ? dnf_prev->or_next : dnf_node;
		  continue;
		}
	      else if (dnf_node->info.expr.op == PT_GT
		       || dnf_node->info.expr.op == PT_GE
		       || dnf_node->info.expr.op == PT_LT
		       || dnf_node->info.expr.op == PT_LE)
		{
		  if (!PT_IS_CONST (dnf_node->info.expr.arg2))
		    {
		      /* not constant; cannot be merged */
		      dnf_prev = dnf_prev ? dnf_prev->or_next : dnf_node;
		      continue;
		    }
		}
	    }
#else
	  if (dnf_node == cnf_node && dnf_node->or_next == NULL
	      && dnf_node->info.expr.op == PT_EQ
	      && !pt_is_instnum (arg1_prior))
	    {
	      /* do not convert one predicate '=' term to RANGE */
	      dnf_prev = dnf_prev ? dnf_prev->or_next : dnf_node;
	      continue;
	    }
#endif

	  switch (dnf_node->info.expr.op)
	    {
	    case PT_EQ:
	    case PT_GT:
	    case PT_GE:
	    case PT_LT:
	    case PT_LE:
	    case PT_BETWEEN:
	    case PT_IS_IN:
	    case PT_RANGE:

	      /* should be pure constant in list */
	      if (dnf_node->info.expr.op == PT_IS_IN
		  && dnf_node->info.expr.arg2 != NULL
		  && PT_IS_COLLECTION_TYPE (dnf_node->info.expr.arg2->
					    type_enum)
		  && dnf_node->or_next == NULL)
		{
		  /*
		   * skip merge in list
		   * server will eliminate duplicate keys
		   * this is because merging huge in list takes
		   * too much time.
		   */
		  qo_convert_to_range_helper (parser, dnf_node);
		  break;
		}

	      /* convert all comparison nodes in the DNF list which have
	         the same attribute as its LHS into one RANGE node
	         containing multi-range spec */
	      qo_convert_to_range_helper (parser, dnf_node);

	      if (dnf_node->info.expr.op == PT_RANGE)
		{
		  /* merge range specs in the RANGE node */
		  result = qo_merge_range_helper (parser, dnf_node);
		  assert (result == DNF_RANGE_VALID);
		}
	      break;

	    default:
	      break;
	    }

	  dnf_prev = dnf_prev ? dnf_prev->or_next : dnf_node;
	}

      cnf_prev = cnf_prev ? cnf_prev->next : cnf_node;
    }
}

/*
 * qo_rewrite_outerjoin () - Rewrite outer join to inner join
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): SELECT node
 *   arg(in):
 *   continue_walk(in):
 *
 * Note: do parser_walk_tree() pre function
 */
static PT_NODE *
qo_rewrite_outerjoin (PARSER_CONTEXT * parser, PT_NODE * node,
		      UNUSED_ARG void *arg, int *continue_walk)
{
  PT_NODE *spec, *prev_spec, *expr, *ns;
  SPEC_ID_INFO info;
  RESET_LOCATION_INFO locate_info;
  int nullable_cnt;		/* nullable terms count */
  bool rewrite_again;

  if (node->node_type != PT_SELECT)
    {
      return node;
    }

  do
    {
      rewrite_again = false;
      /* traverse spec list */
      prev_spec = NULL;
      for (spec = node->info.query.q.select.from;
	   spec; prev_spec = spec, spec = spec->next)
	{
	  if (spec->info.spec.join_type == PT_JOIN_LEFT_OUTER
	      || spec->info.spec.join_type == PT_JOIN_RIGHT_OUTER)
	    {
	      if (spec->info.spec.join_type == PT_JOIN_LEFT_OUTER)
		{
		  info.id = spec->info.spec.id;
		}
	      else if (prev_spec != NULL)
		{
		  info.id = prev_spec->info.spec.id;
		}

	      info.appears = false;
	      nullable_cnt = 0;

	      /* search where list */
	      for (expr = node->info.query.q.select.where;
		   expr; expr = expr->next)
		{
		  if (expr->node_type == PT_EXPR
		      && expr->info.expr.location == 0
		      && expr->info.expr.op != PT_IS_NULL
		      && expr->or_next == NULL)
		    {
		      (void) parser_walk_leaves (parser, expr,
						 qo_get_name_by_spec_id,
						 &info,
						 qo_check_nullable_expr,
						 &nullable_cnt);
		      /* have found a term which makes outer join to inner */
		      if (info.appears && nullable_cnt == 0)
			{
			  rewrite_again = true;
			  spec->info.spec.join_type = PT_JOIN_INNER;

			  locate_info.start = spec->info.spec.location;
			  locate_info.end = locate_info.start;
			  (void) parser_walk_tree (parser,
						   node->info.query.q.select.
						   where, qo_reset_location,
						   &locate_info, NULL, NULL);

			  /* rewrite the following connected right outer join
			   * to inner join */
			  for (ns = spec->next;	/* traverse next spec */
			       ns && ns->info.spec.join_type != PT_JOIN_NONE;
			       ns = ns->next)
			    {
			      if (ns->info.spec.join_type ==
				  PT_JOIN_RIGHT_OUTER)
				{
				  ns->info.spec.join_type = PT_JOIN_INNER;
				  locate_info.start = ns->info.spec.location;
				  locate_info.end = locate_info.start;
				  (void) parser_walk_tree (parser,
							   node->info.query.q.
							   select.where,
							   qo_reset_location,
							   &locate_info, NULL,
							   NULL);
				}
			    }
			  break;
			}
		    }
		}
	    }

	  if (spec->info.spec.derived_table
	      && spec->info.spec.derived_table_type == PT_IS_SUBQUERY)
	    {
	      /* apply qo_rewrite_outerjoin() to derived table's subquery */
	      (void) parser_walk_tree (parser, spec->info.spec.derived_table,
				       qo_rewrite_outerjoin, NULL, NULL,
				       NULL);
	    }
	}
    }
  while (rewrite_again);

  *continue_walk = PT_LIST_WALK;

  return node;
}

/*
 * qo_reset_location () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
qo_reset_location (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		   void *arg, UNUSED_ARG int *continue_walk)
{
  RESET_LOCATION_INFO *infop = (RESET_LOCATION_INFO *) arg;

  if (node->node_type == PT_EXPR
      && node->info.expr.location >= infop->start
      && node->info.expr.location <= infop->end)
    {
      node->info.expr.location = 0;
    }

  if (node->node_type == PT_NAME
      && node->info.name.location >= infop->start
      && node->info.name.location <= infop->end)
    {
      node->info.name.location = 0;
    }

  if (node->node_type == PT_VALUE
      && node->info.value.location >= infop->start
      && node->info.value.location <= infop->end)
    {
      node->info.value.location = 0;
    }

  return node;
}

/*
 * qo_rewrite_innerjoin () - Rewrite explicit(ordered) inner join
 *			  to implicit(unordered) inner join
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): SELECT node
 *   arg(in):
 *   continue_walk(in):
 *
 * Note: If join order hint is set, skip and go ahead.
 *   do parser_walk_tree() pre function
 */
static PT_NODE *
qo_rewrite_innerjoin (PARSER_CONTEXT * parser, PT_NODE * node,
		      UNUSED_ARG void *arg, int *continue_walk)
{
  PT_NODE *spec, *spec2;
  RESET_LOCATION_INFO info;	/* spec location reset info */

  if (node->node_type != PT_SELECT)
    {
      return node;
    }

  if (node->info.query.q.select.hint & PT_HINT_ORDERED)
    {
      /* join hint: force join left-to-right.
       * skip and go ahead.
       */
      return node;
    }

  info.start = 0;
  info.end = 0;
  info.found_outerjoin = false;

  /* traverse spec list to find disconnected spec list */
  for (info.start_spec = spec = node->info.query.q.select.from;
       spec; spec = spec->next)
    {

      switch (spec->info.spec.join_type)
	{
	case PT_JOIN_LEFT_OUTER:
	case PT_JOIN_RIGHT_OUTER:
	  /* case PT_JOIN_FULL_OUTER: */
	  info.found_outerjoin = true;
	  break;
	default:
	  break;
	}

      if (spec->info.spec.join_type == PT_JOIN_NONE
	  && info.found_outerjoin == false && info.start < info.end)
	{
	  /* rewrite explicit inner join to implicit inner join */
	  for (spec2 = info.start_spec; spec2 != spec; spec2 = spec2->next)
	    {
	      if (spec2->info.spec.join_type == PT_JOIN_INNER)
		{
		  spec2->info.spec.join_type = PT_JOIN_NONE;
		}
	    }

	  /* reset location of spec list */
	  (void) parser_walk_tree (parser, node->info.query.q.select.where,
				   qo_reset_location, &info, NULL, NULL);

	  /* reset start spec, found_outerjoin */
	  info.start = spec->info.spec.location;
	  info.start_spec = spec;
	  info.found_outerjoin = false;
	}

      info.end = spec->info.spec.location;

      if (spec->info.spec.derived_table
	  && spec->info.spec.derived_table_type == PT_IS_SUBQUERY)
	{
	  /* apply qo_rewrite_innerjoin() to derived table's subquery */
	  (void) parser_walk_tree (parser, spec->info.spec.derived_table,
				   qo_rewrite_innerjoin, NULL, NULL, NULL);
	}
    }

  if (info.found_outerjoin == false && info.start < info.end)
    {
      /* rewrite explicit inner join to implicit inner join */
      for (spec2 = info.start_spec; spec2; spec2 = spec2->next)
	{
	  if (spec2->info.spec.join_type == PT_JOIN_INNER)
	    {
	      spec2->info.spec.join_type = PT_JOIN_NONE;
	    }
	}

      /* reset location of spec list */
      (void) parser_walk_tree (parser, node->info.query.q.select.where,
			       qo_reset_location, &info, NULL, NULL);
    }

  *continue_walk = PT_LIST_WALK;

  return node;
}

/*
 * qo_rewrite_query_as_derived () -
 *   return: rewritten select statement with derived table subquery
 *   parser(in):
 *   query(in):
 *
 * Note: returned result depends on global schema state.
 */
static PT_NODE *
qo_rewrite_query_as_derived (PARSER_CONTEXT * parser, PT_NODE * query)
{
  PT_NODE *new_query = NULL, *derived = NULL;
  PT_NODE *range = NULL, *spec = NULL, *temp, *node = NULL;
  PT_NODE **head;
  int i = 0;
  SEMANTIC_CHK_INFO sc_info = { NULL, false };

  /* set line number to range name */
  range = pt_name (parser, "d3201");
  if (range == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto exit_on_error;
    }

  /* construct new spec
   * We are now copying the query and updating the spec_id references
   */
  spec = parser_new_node (parser, PT_SPEC);
  if (spec == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto exit_on_error;
    }

  derived = parser_copy_tree (parser, query);
  derived = mq_reset_ids_in_statement (parser, derived);
  if (derived == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto exit_on_error;
    }

  /* increase correlation level of the query */
  if (query->info.query.correlation_level)
    {
      derived =
	mq_bump_correlation_level (parser, derived, 1,
				   derived->info.query.correlation_level);
    }

  if (derived == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto exit_on_error;
    }

  spec->info.spec.derived_table = derived;
  spec->info.spec.derived_table_type = PT_IS_SUBQUERY;
  spec->info.spec.range_var = range;
  spec->info.spec.id = (UINTPTR) spec;
  range->info.name.spec_id = (UINTPTR) spec;

  new_query = parser_new_node (parser, PT_SELECT);
  if (new_query == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto exit_on_error;
    }

  if (query->info.query.correlation_level)
    {
      new_query->info.query.correlation_level =
	query->info.query.correlation_level;
    }

  new_query->info.query.q.select.from = spec;
  new_query->info.query.is_subquery = query->info.query.is_subquery;


  temp = pt_get_select_list (parser, spec->info.spec.derived_table);
  head = &new_query->info.query.q.select.list;

  while (temp)
    {
      /* generate as_attr_list */
      if (temp->node_type == PT_NAME && temp->info.name.original != NULL)
	{
	  /* we have the original name */
	  node = pt_name (parser, temp->info.name.original);
	}
      else
	{
	  /* don't have name for attribute; generate new name */
	  node = pt_name (parser, mq_generate_name (parser, "a", &i));
	}

      if (node == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  goto exit_on_error;
	}
      /* set line, column number */
      node->line_number = temp->line_number;
      node->column_number = temp->column_number;

      node->info.name.meta_class = PT_NORMAL;
      node->info.name.resolved = range->info.name.original;
      node->info.name.spec_id = spec->info.spec.id;
      node->type_enum = temp->type_enum;
      node->data_type = parser_copy_tree (parser, temp->data_type);
      spec->info.spec.as_attr_list =
	parser_append_node (node, spec->info.spec.as_attr_list);
      /* keep out hidden columns from derived select list */
      if (query->info.query.order_by && temp->is_hidden_column)
	{
	  temp->is_hidden_column = 0;
	}
      else
	{
	  *head = parser_copy_tree (parser, node);
	  head = &((*head)->next);
	}

      temp = temp->next;
    }

  /* move query id # */
  new_query->info.query.id = query->info.query.id;
  query->info.query.id = 0;

  sc_info.top_node = new_query;
  pt_semantic_type (parser, new_query, &sc_info);

  return new_query;

exit_on_error:

  if (node != NULL)
    {
      parser_free_node (parser, node);
    }
  if (new_query != NULL)
    {
      parser_free_node (parser, new_query);
    }
  if (derived != NULL)
    {
      parser_free_node (parser, derived);
    }
  if (spec != NULL)
    {
      parser_free_node (parser, spec);
    }
  if (range != NULL)
    {
      parser_free_node (parser, range);
    }
  return NULL;
}

/*
 * qo_rewrite_hidden_col_as_derived () - Rewrite subquery with ORDER BY
 *				      hidden column as derived one
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): QUERY node
 *   parent_node(in):
 *
 * Note: Keep out hidden column from derived select list
 */
static PT_NODE *
qo_rewrite_hidden_col_as_derived (PARSER_CONTEXT * parser, PT_NODE * node,
				  PT_NODE * parent_node)
{
  PT_NODE *t_node, *next, *derived;

  switch (node->node_type)
    {
    case PT_SELECT:
      if (node->info.query.order_by)
	{
	  bool remove_order_by = true;	/* guessing */

	  /* check parent and node context */
	  if (parent_node == NULL || node->info.query.orderby_for)
	    {
	      remove_order_by = false;
	    }
	  else
	    {
	      for (t_node = node->info.query.q.select.list;
		   t_node; t_node = t_node->next)
		{
		  if (t_node->node_type == PT_EXPR
		      && t_node->info.expr.op == PT_ORDERBY_NUM)
		    {
		      remove_order_by = false;
		      break;
		    }
		}
	    }

	  /* remove unnecessary ORDER BY clause */
	  if (remove_order_by == true)
	    {
	      parser_free_tree (parser, node->info.query.order_by);
	      node->info.query.order_by = NULL;

	      for (t_node = node->info.query.q.select.list;
		   t_node && t_node->next; t_node = next)
		{
		  next = t_node->next;
		  if (next->is_hidden_column)
		    {
		      parser_free_tree (parser, next);
		      t_node->next = NULL;
		      break;
		    }
		}
	    }
	  else
	    {
	      for (t_node = node->info.query.q.select.list;
		   t_node; t_node = t_node->next)
		{
		  if (t_node->is_hidden_column)
		    {
		      /* make derived query */
		      derived = qo_rewrite_query_as_derived (parser, node);
		      if (derived == NULL)
			{
			  break;
			}

		      PT_NODE_MOVE_NUMBER_OUTERLINK (derived, node);
		      derived->info.query.is_subquery =
			node->info.query.is_subquery;

		      /* free old composite query */
		      parser_free_tree (parser, node);
		      node = derived;
		      break;
		    }
		}
	    }			/* else */
	}
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      node->info.query.q.union_.arg1 =
	qo_rewrite_hidden_col_as_derived (parser,
					  node->info.query.q.union_.arg1,
					  NULL);
      node->info.query.q.union_.arg2 =
	qo_rewrite_hidden_col_as_derived (parser,
					  node->info.query.q.union_.arg2,
					  NULL);
      break;
    default:
      return node;
    }

  return node;
}

/*
 * qo_rewrite_index_hints () - Rewrite index hint list, removing useless hints
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): QUERY node
 *   parent_node(in):
 */
static void
qo_rewrite_index_hints (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  PT_NODE *using_index, *hint_node, *prev_node, *next_node;

  bool is_sorted, is_idx_reversed, is_idx_match_nokl, is_hint_masked;

  PT_NODE *hint_none, *root_node;
  PT_NODE dummy_hint_local, *dummy_hint;

  switch (statement->node_type)
    {
    case PT_SELECT:
      using_index = statement->info.query.q.select.using_index;
      break;
    case PT_UPDATE:
      using_index = statement->info.update.using_index;
      break;
    case PT_DELETE:
      using_index = statement->info.delete_.using_index;
      break;
    default:
      /* USING index clauses are not allowed for other query types */
      assert (false);
      return;
    }

  if (using_index == NULL)
    {
      /* no index hints, nothing to do here */
      return;
    }

  /* Main logic - we can safely assume that pt_check_using_index() has already
   * checked for possible semantic errors or incompatible index hints. */

  /* basic rewrite, for USING INDEX NONE */
  hint_node = using_index;
  prev_node = NULL;
  hint_none = NULL;
  while (hint_node != NULL)
    {
      if (hint_node->etc == (void *) PT_IDX_HINT_NONE)
	{
	  hint_none = hint_node;
	  break;
	}
      prev_node = (prev_node == NULL) ? hint_node : prev_node->next;
      hint_node = hint_node->next;
    }

  if (hint_none != NULL)
    {
      /* keep only the using_index_none hint stored in hint_none */
      /* update links and discard the first part of the hint list */
      if (prev_node != NULL)
	{
	  prev_node->next = NULL;
	  parser_free_tree (parser, using_index);
	  using_index = NULL;
	}
      /* update links and discard the last part of the hint list */
      hint_node = hint_none->next;
      if (hint_node != NULL)
	{
	  parser_free_tree (parser, hint_node);
	  hint_node = NULL;
	}
      /* update links and keep only the USING INDEX NONE node */
      hint_none->next = NULL;
      using_index = hint_none;
      goto exit;
    }

  /* there is no USING INDEX NONE in the query;
   * the dummy node is necessary for faster operation;
   * use local variable dummy_hint */
  dummy_hint = &dummy_hint_local;
  dummy_hint->next = using_index;

  /* just need something, so that this node won't be kept later */
  dummy_hint->etc = (void *) PT_IDX_HINT_USE;
  root_node = prev_node = dummy_hint;
  hint_node = using_index;

  /* remove duplicate index hints and sort them; keep the same order for
   * the hints of the same type with keylimit */
  /* order: class_none, ignored, forced, used */
  is_sorted = false;
  while (!is_sorted)
    {
      prev_node = root_node;
      hint_node = prev_node->next;
      is_sorted = true;
      while ((next_node = hint_node->next) != NULL)
	{
	  is_idx_reversed = false;
	  is_idx_match_nokl = false;
	  if (PT_IDX_HINT_ORDER (hint_node) > PT_IDX_HINT_ORDER (next_node))
	    {
	      is_idx_reversed = true;
	    }
	  else if (hint_node->etc == next_node->etc)
	    {
	      /* if hints have the same type, check if they need to be swapped
	       * or are identical and one of them needs to be removed */
	      int res_cmp_tbl_names = -1;

	      /* unless USING INDEX NONE, which is rewritten above, all
	       * existing indexes should have table names already resolved */

	      /* compare the tables on which the indexes are defined */
	      if (hint_node->info.name.resolved != NULL
		  && next_node->info.name.resolved != NULL)
		{
		  res_cmp_tbl_names = intl_identifier_casecmp
		    (hint_node->info.name.resolved,
		     next_node->info.name.resolved);
		}

	      if (res_cmp_tbl_names == 0)
		{
		  /* also compare index names */
		  if (hint_node->info.name.original != NULL
		      && next_node->info.name.original != NULL)
		    {
		      /* index names can be null if t.none */
		      int res_cmp_idx_names;

		      res_cmp_idx_names = intl_identifier_casecmp
			(hint_node->info.name.original,
			 next_node->info.name.original);
		      if (res_cmp_idx_names == 0)
			{
			  is_idx_match_nokl = true;
			}
		      else
			{
			  is_idx_reversed = (res_cmp_idx_names > 0);
			}
		    }
		  else
		    {
		      /* hints are of the same type, name.original is either
		       * NULL or not NULL for both hints */
		      assert (hint_node->info.name.original == NULL
			      && next_node->info.name.original == NULL);
		      /* both hints are "same-table.none"; identical */
		      is_idx_match_nokl = true;
		    }
		}
	      else
		{
		  is_idx_reversed = (res_cmp_tbl_names > 0);
		}

	      if (is_idx_match_nokl)
		{
		  /* The same index is used in both hints; examine the
		   * keylimit clauses; if search_node does not have keylimit,
		   * the IF below will skip, and search_node will be deleted */
		  if (next_node->info.name.indx_key_limit != NULL)
		    {
		      /* search_node has keylimit */
		      if (hint_node->info.name.indx_key_limit != NULL)
			{
			  /* hint_node has keylimit; no action is performed;
			   * we want to preserve the order of index hints for
			   * the same index, with keylimit */
			  is_idx_reversed = false;
			  is_idx_match_nokl = false;
			}
		      else
			{
			  /* special case; need to delete hint_node and keep
			   * search_node, because this one has keylimit;
			   */
			  assert (!is_idx_reversed);
			  is_idx_reversed = true;
			  /* reverse the two nodes so the code below can be
			   * reused for this situation */
			}
		    }		/* endif (search_node) */
		}		/* endif (is_idx_match_nokl) */
	    }

	  if (is_idx_reversed)
	    {
	      /* Interchange the two hints */
	      hint_node->next = next_node->next;
	      next_node->next = hint_node;
	      prev_node->next = next_node;
	      is_sorted = false;
	      /* update hint_node and search_node, for possible delete */
	      hint_node = prev_node->next;
	      next_node = hint_node->next;
	    }

	  if (is_idx_match_nokl)
	    {
	      /* remove search_node */
	      hint_node->next = next_node->next;
	      next_node->next = NULL;
	      parser_free_node (parser, next_node);
	      /* node removed, use prev_node and hint_node in next loop */
	      continue;
	    }
	  prev_node = prev_node->next;
	  hint_node = prev_node->next;
	}
    }

  /* Find index hints to remove later.
   * At this point, the only index hints that can be found in using_index are
   * USE INDEX and USING INDEX {idx|t.none}...
   * Need to ignore duplicate hints, and hints that are masked by applying
   * the hint operation rules.
   */
  hint_node = root_node->next;
  while (hint_node != NULL)
    {
      next_node = hint_node->next;
      prev_node = hint_node;
      while (next_node != NULL)
	{
	  if (next_node->etc == hint_node->etc)
	    {
	      /* same hint type; duplicates were already removed, skip hint */
	      prev_node = next_node;
	      next_node = next_node->next;
	      continue;
	    }

	  /* Main logic for removing redundant/masked index hints */
	  /* The hint list is now sorted, first by index type, then by table
	   * and index name, so the next_node type is the same as hint_node
	   * or lower in importance (class.none > use),
	   * so it is not necessary to check next_index hint type */
	  is_hint_masked = false;

	  if (hint_node->etc == (void *) PT_IDX_HINT_CLASS_NONE
	      && next_node->info.name.resolved != NULL
	      && intl_identifier_casecmp (hint_node->info.name.resolved,
					  next_node->info.name.resolved) == 0)
	    {
	      is_hint_masked = true;
	    }

	  if (is_hint_masked)
	    {
	      /* hint search_node is masked; remove it from the hint list */
	      prev_node->next = next_node->next;
	      next_node->next = NULL;
	      parser_free_node (parser, next_node);
	      next_node = prev_node;
	    }
	  prev_node = next_node;
	  next_node = next_node->next;
	}
      hint_node = hint_node->next;
    }

  /* remove the dummy first node, if any */
  using_index = root_node->next;
  root_node->next = NULL;

exit:
  /* Save changes to query node */
  switch (statement->node_type)
    {
    case PT_SELECT:
      statement->info.query.q.select.using_index = using_index;
      break;
    case PT_UPDATE:
      statement->info.update.using_index = using_index;
      break;
    case PT_DELETE:
      statement->info.delete_.using_index = using_index;
      break;
    default:
      break;
    }
}

/*
 * qo_rewrite_subqueries () - Rewrite uncorrelated subquery to join query
 *   return: PT_NODE *
 *   parser(in):
 *   node(in): SELECT node
 *   arg(in):
 *   continue_walk(in):
 *
 * Note: do parser_walk_tree() pre function
 */
static PT_NODE *
qo_rewrite_subqueries (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
		       int *continue_walk)
{
  PT_NODE *cnf_node, *arg1, *arg2, *select_list;
  PT_OP_TYPE op_type;
  PT_NODE *new_spec, *new_attr;
  int *idx = (int *) arg;
  bool do_rewrite;
  PT_NODE *save_next, *arg1_next, *new_attr_next, *tmp;
  PT_OP_TYPE saved_op_type;

  if (node->node_type != PT_SELECT)
    {
      return node;
    }

  /* traverse CNF list */
  for (cnf_node = node->info.query.q.select.where; cnf_node;
       cnf_node = cnf_node->next)
    {

      if (cnf_node->or_next != NULL)
	{
	  continue;
	}

      if (cnf_node->node_type != PT_EXPR)
	{
	  continue;
	}

      op_type = cnf_node->info.expr.op;
      arg1 = cnf_node->info.expr.arg1;
      arg2 = cnf_node->info.expr.arg2;

      if (arg1 && arg2 && (op_type == PT_EQ || op_type == PT_IS_IN))
	{
	  /* go ahead */
	}
      else
	{
	  continue;
	}

      do_rewrite = false;
      select_list = NULL;

      /* should be 'attr op uncorr-subquery' */

      if (pt_is_attr (arg1))
	{
	  select_list = pt_get_select_list (parser, arg2);
	  if (select_list != NULL && arg2->info.query.correlation_level == 0)
	    {
	      assert (pt_length_of_select_list (select_list,
						EXCLUDE_HIDDEN_COLUMNS) == 1);

	      /* match 'indexable-attr op indexable-uncorr-subquery' */
	      do_rewrite = true;
	    }
	}

      if (do_rewrite)
	{
	  /* rewrite subquery to join with derived table */
	  switch (op_type)
	    {
	    case PT_EQ:	/* arg1 = set_func_elements */
	    case PT_IS_IN:	/* arg1 = set_func_elements, attr */
	      /* make new derived spec and append it to FROM */
	      if (mq_make_derived_spec (parser, node, arg2,
					idx, &new_spec, &new_attr) == NULL)
		{
		  return NULL;
		}

	      /* convert to 'attr op attr' */
	      cnf_node->info.expr.arg1 = arg1;
	      arg1 = arg1->next;
	      cnf_node->info.expr.arg1->next = NULL;

	      cnf_node->info.expr.arg2 = new_attr;
	      saved_op_type = cnf_node->info.expr.op;
	      cnf_node->info.expr.op = PT_EQ;

	      if (new_attr != NULL)
		{
		  new_attr = new_attr->next;
		  cnf_node->info.expr.arg2->next = NULL;
		}

	      /* save, cut-off link */
	      save_next = cnf_node->next;
	      cnf_node->next = NULL;

	      /* create the following 'attr op attr' */
	      for (tmp = NULL;
		   arg1 && new_attr;
		   arg1 = arg1_next, new_attr = new_attr_next)
		{
		  tmp = parser_new_node (parser, PT_EXPR);
		  if (tmp == NULL)
		    {
		      PT_INTERNAL_ERROR (parser, "allocate new node");
		      return NULL;
		    }

		  /* save, cut-off link */
		  arg1_next = arg1->next;
		  arg1->next = NULL;
		  new_attr_next = new_attr->next;
		  new_attr->next = NULL;

		  tmp->info.expr.arg1 = arg1;
		  tmp->info.expr.arg2 = new_attr;
		  tmp->info.expr.op = PT_EQ;

		  cnf_node = parser_append_node (tmp, cnf_node);
		}

	      if (tmp)
		{		/* move to the last cnf */
		  cnf_node = tmp;
		}
	      cnf_node->next = save_next;	/* restore link */

	      /* apply qo_rewrite_subqueries() to derived table's subquery */
	      (void) parser_walk_tree (parser,
				       new_spec->info.spec.derived_table,
				       qo_rewrite_subqueries, idx, NULL,
				       NULL);
	      break;

	    default:
	      break;
	    }
	}
    }				/* for (cnf_node = ...) */

  *continue_walk = PT_LIST_WALK;

  return node;
}

/*
 * qo_optimize_queries () - checks all subqueries for rewrite optimizations
 *   return: PT_NODE *
 *   parser(in): parser environment
 *   node(in): possible query
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
qo_optimize_queries (PARSER_CONTEXT * parser, PT_NODE * node,
		     UNUSED_ARG void *arg, UNUSED_ARG int *continue_walk)
{
  int level;
  PT_NODE **wherep, **havingp, *dummy;
  PT_NODE *t_node, *spec;
  PT_NODE *limit, *derived;
  PT_NODE **orderby_for_p;

  dummy = NULL;
  wherep = havingp = &dummy;
  orderby_for_p = &dummy;

  switch (node->node_type)
    {
    case PT_SELECT:
      /* Put all join conditions together with WHERE clause for rewrite
         optimization. But we can distinguish a join condition from
         each other and from WHERE clause by location information
         that were marked at 'pt_bind_names()'. We'll recover the parse
         tree of join conditions using the location information in shortly. */
      t_node = node->info.query.q.select.where;
      while (t_node && t_node->next)
	{
	  t_node = t_node->next;
	}
      for (spec = node->info.query.q.select.from; spec; spec = spec->next)
	{
	  if (spec->node_type == PT_SPEC && spec->info.spec.on_cond)
	    {
	      if (!t_node)
		{
		  t_node = node->info.query.q.select.where
		    = spec->info.spec.on_cond;
		}
	      else
		{
		  t_node->next = spec->info.spec.on_cond;
		}
	      spec->info.spec.on_cond = NULL;
	      while (t_node->next)
		{
		  t_node = t_node->next;
		}
	    }
	}
      wherep = &node->info.query.q.select.where;
      havingp = &node->info.query.q.select.having;
      spec = node->info.query.q.select.from;
      orderby_for_p = &node->info.query.orderby_for;
      qo_rewrite_index_hints (parser, node);
      break;

    case PT_UPDATE:
      wherep = &node->info.update.search_cond;
      orderby_for_p = &node->info.update.orderby_for;
      qo_rewrite_index_hints (parser, node);
      break;

    case PT_DELETE:
      wherep = &node->info.delete_.search_cond;
      qo_rewrite_index_hints (parser, node);
      break;

    case PT_INSERT:
      {
	PT_NODE *const subquery_ptr = pt_get_subquery_of_insert_select (node);

	if (subquery_ptr == NULL || subquery_ptr->node_type != PT_SELECT)
	  {
	    return node;
	  }
	wherep = &subquery_ptr->info.query.q.select.where;
      }
      break;

    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      node->info.query.q.union_.arg1 =
	qo_rewrite_hidden_col_as_derived (parser,
					  node->info.query.q.union_.arg1,
					  NULL);
      node->info.query.q.union_.arg2 =
	qo_rewrite_hidden_col_as_derived (parser,
					  node->info.query.q.union_.arg2,
					  NULL);

      /* If LIMIT clause is specified without ORDER BY clause,
       * we will rewrite the UNION query as derived.
       * For example,
       *   (SELECT ...) UNION (SELECT ...) LIMIT 10
       *   will be rewritten to:
       *   SELECT * FROM ((SELECT ...) UNION (SELECT ...)) T
       *     WHERE INST_NUM() <= 10
       */
      if (node->info.query.limit && node->info.query.rewrite_limit)
	{
	  limit = pt_limit_to_numbering_expr (parser, node->info.query.limit,
					      PT_INST_NUM, false);
	  if (limit != NULL)
	    {
	      node->info.query.rewrite_limit = 0;

	      derived = qo_rewrite_query_as_derived (parser, node);
	      if (derived != NULL)
		{
		  PT_NODE_MOVE_NUMBER_OUTERLINK (derived, node);
		  assert (derived->info.query.q.select.where == NULL);
		  derived->info.query.q.select.where = limit;
		  derived->info.query.limit =
		    parser_copy_tree_list (parser, node->info.query.limit);

		  node = derived;
		}
	    }
	}

      orderby_for_p = &node->info.query.orderby_for;
      break;

    case PT_EXPR:
      switch (node->info.expr.op)
	{
	case PT_EQ:
	case PT_NE:
	case PT_NULLSAFE_EQ:
	  node->info.expr.arg1 =
	    qo_rewrite_hidden_col_as_derived (parser, node->info.expr.arg1,
					      node);
	  /* fall through */

	  /* keep out hidden column subquery from UPDATE assignment */
	case PT_ASSIGN:
	  /* quantified equality comparisons */
	case PT_IS_IN:
	case PT_IS_NOT_IN:
	  node->info.expr.arg2 =
	    qo_rewrite_hidden_col_as_derived (parser, node->info.expr.arg2,
					      node);
	  break;
	default:
	  break;
	}
      /* no WHERE clause */
      return node;

    default:
      /* no WHERE clause */
      return node;
    }

  qo_get_optimization_param (&level, QO_PARAM_LEVEL);
  if (OPTIMIZATION_ENABLED (level))
    {

      if (node->node_type == PT_SELECT)
	{
	  int continue_walk;
	  int idx = 0;

	  /* rewrite uncorrelated subquery to join query */
	  qo_rewrite_subqueries (parser, node, &idx, &continue_walk);
	}

      /* rewrite optimization on WHERE, HAVING clause
       */

      /* convert to CNF and tag taggable terms */
      if (*wherep)
	{
	  *wherep = pt_cnf (parser, *wherep);
	}
      if (*havingp)
	{
	  *havingp = pt_cnf (parser, *havingp);
	}
      if (*orderby_for_p)
	{
	  *orderby_for_p = pt_cnf (parser, *orderby_for_p);
	}

      /* in HAVING clause with GROUP BY,
       * move non-aggregate terms to WHERE clause
       */
      if (PT_IS_SELECT (node) && node->info.query.q.select.group_by
	  && *havingp)
	{
	  PT_NODE *prev, *cnf, *next;
	  PT_NON_GROUPBY_COL_INFO col_info;
	  PT_AGG_FIND_INFO info;
	  bool can_move;

	  col_info.groupby = node->info.query.q.select.group_by;

	  prev = NULL;		/* init */
	  for (cnf = *havingp; cnf; cnf = next)
	    {
	      next = cnf->next;	/* save and cut-off link */
	      cnf->next = NULL;

	      col_info.has_non_groupby_col = false;	/* on the supposition */
	      (void) parser_walk_tree (parser, cnf,
				       pt_has_non_groupby_column_node,
				       &col_info, NULL, NULL);
	      can_move = (col_info.has_non_groupby_col == false);

	      if (can_move)
		{
		  /* init agg info */
		  info.stop_on_subquery = false;
		  info.out_of_context_count = 0;
		  info.base_count = 0;
		  info.select_stack =
		    pt_pointer_stack_push (parser, NULL, node);

		  /* search for aggregate of this select */
		  (void) parser_walk_tree (parser, cnf,
					   pt_find_aggregate_functions_pre,
					   &info,
					   pt_find_aggregate_functions_post,
					   &info);
		  can_move = (info.base_count == 0);

		  /* cleanup */
		  info.select_stack =
		    pt_pointer_stack_pop (parser, info.select_stack, NULL);
		}

	      /* Not found aggregate function in cnf node and no ROLLUP clause.
	       * So, move it from HAVING clause to WHERE clause.
	       */
	      if (can_move
		  && !node->info.query.q.select.group_by->with_rollup)
		{
		  /* delete cnf node from HAVING clause */
		  if (!prev)
		    {		/* very the first node */
		      *havingp = next;
		    }
		  else
		    {
		      prev->next = next;
		    }

		  /* add cnf node to WHERE clause
		   */
		  *wherep = parser_append_node (*wherep, cnf);
		}
	      else
		{		/* do nothing and go ahead */
		  cnf->next = next;	/* restore link */
		  prev = cnf;	/* save previous */
		}
	    }
	}

      /* reduce equality terms */
      (void) qo_reduce_equality_terms (parser, node);

      /* convert terms of the form 'const op attr' to 'attr op const' */
      if (*wherep)
	{
	  qo_converse_sarg_terms (parser, *wherep);
	}
      if (*havingp)
	{
	  qo_converse_sarg_terms (parser, *havingp);
	}

      /* reduce a pair of comparison terms into one BETWEEN term */
      if (*wherep)
	{
	  qo_reduce_comp_pair_terms (parser, wherep);
	}
      if (*havingp)
	{
	  qo_reduce_comp_pair_terms (parser, havingp);
	}

      /* convert a leftmost LIKE term to a BETWEEN (GE_LT) term */
      if (*wherep)
	{
	  qo_rewrite_like_terms (parser, wherep);
	}
      if (*havingp)
	{
	  qo_rewrite_like_terms (parser, havingp);
	}

      /* convert comparison terms to RANGE */
      if (*wherep)
	{
	  qo_convert_to_range (parser, wherep);
	}
      if (*havingp)
	{
	  qo_convert_to_range (parser, havingp);
	}

      if (node->node_type == PT_SELECT)
	{
	  int continue_walk;

	  /* rewrite outer join to inner join */
	  qo_rewrite_outerjoin (parser, node, NULL, &continue_walk);

	  /* rewrite explicit inner join to implicit inner join */
	  qo_rewrite_innerjoin (parser, node, NULL, &continue_walk);

	  if (qo_reduce_order_by (parser, node) != NO_ERROR)
	    {
	      return node;	/* give up */
	    }
	}

    }

  if (node->node_type == PT_SELECT)
    {
      if (node->info.query.is_subquery == PT_IS_SUBQUERY)
	{
	  if (node->info.query.single_tuple == 1)
	    {
	      node = qo_rewrite_hidden_col_as_derived (parser, node, NULL);
	    }
	}
    }

  return node;
}

/*
 * qo_optimize_queries_post () -
 *   return:
 *   parser(in):
 *   tree(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
qo_optimize_queries_post (PARSER_CONTEXT * parser, PT_NODE * tree,
			  UNUSED_ARG void *arg, UNUSED_ARG int *continue_walk)
{
  PT_NODE *node, *prev, *next, *spec;
  short location;

  switch (tree->node_type)
    {
    case PT_SELECT:
      prev = NULL;
      for (node = tree->info.query.q.select.where; node; node = next)
	{
	  next = node->next;
	  node->next = NULL;

	  if (node->node_type == PT_EXPR)
	    location = node->info.expr.location;
	  else if (node->node_type == PT_VALUE)
	    location = node->info.value.location;
	  else
	    location = -1;

	  if (location > 0)
	    {
	      for (spec = tree->info.query.q.select.from;
		   spec && spec->info.spec.location != location;
		   spec = spec->next)
		/* nop */ ;
	      if (spec)
		{
		  if (spec->info.spec.join_type == PT_JOIN_LEFT_OUTER
		      || spec->info.spec.join_type == PT_JOIN_RIGHT_OUTER
		      || spec->info.spec.join_type == PT_JOIN_INNER)
		    {
		      node->next = spec->info.spec.on_cond;
		      spec->info.spec.on_cond = node;

		      if (prev)
			{
			  prev->next = next;
			}
		      else
			{
			  tree->info.query.q.select.where = next;
			}
		    }
		  else
		    {		/* already converted to inner join */
		      /* clear on cond location */
		      if (node->node_type == PT_EXPR)
			{
			  node->info.expr.location = 0;
			}
		      else if (node->node_type == PT_VALUE)
			{
			  node->info.value.location = 0;
			}

		      /* Here - at the last stage of query optimize,
		       * remove copy-pushed term */
		      if (node->node_type == PT_EXPR
			  && PT_EXPR_INFO_IS_FLAGED (node,
						     PT_EXPR_INFO_COPYPUSH))
			{
			  parser_free_tree (parser, node);

			  if (prev)
			    {
			      prev->next = next;
			    }
			  else
			    {
			      tree->info.query.q.select.where = next;
			    }
			}
		      else
			{
			  prev = node;
			  node->next = next;
			}
		    }
		}
	      else
		{
		  /* might be impossible
		   * might be outer join error
		   */
		  PT_ERRORf (parser, node, "check outer join syntax at '%s'",
			     pt_short_print (parser, node));

		  prev = node;
		  node->next = next;
		}
	    }
	  else
	    {
	      /* Here - at the last stage of query optimize,
	       * remove copy-pushed term */
	      if (node->node_type == PT_EXPR
		  && PT_EXPR_INFO_IS_FLAGED (node, PT_EXPR_INFO_COPYPUSH))
		{
		  parser_free_tree (parser, node);

		  if (prev)
		    {
		      prev->next = next;
		    }
		  else
		    {
		      tree->info.query.q.select.where = next;
		    }
		}
	      else
		{
		  prev = node;
		  node->next = next;
		}
	    }
	}

      break;
    default:
      break;
    }

  return tree;
}

/*
 * mq_optimize () - optimize statements by a variety of rewrites
 *   return: void
 *   parser(in): parser environment
 *   statement(in): select tree to optimize
 *
 * Note: rewrite only if optimization is enabled
 */
PT_NODE *
mq_optimize (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  return parser_walk_tree (parser, statement,
			   qo_optimize_queries, NULL,
			   qo_optimize_queries_post, NULL);
}

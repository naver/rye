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
 * plan_generation.c - Generate XASL trees from query optimizer plans
 */

#ident "$Id$"

#include "config.h"

#include <assert.h>

#include "xasl_support.h"

#include "parser.h"
#include "xasl_generation.h"

#include "optimizer.h"
#include "query_graph.h"
#include "query_planner.h"
#include "query_bitset.h"
#include "system_parameter.h"

typedef int (*ELIGIBILITY_FN) (QO_TERM *);

static XASL_NODE *make_scan_proc (QO_ENV * env);
static XASL_NODE *make_buildlist_proc (QO_ENV * env, PT_NODE * namelist);

static XASL_NODE *init_class_scan_proc (QO_ENV * env, XASL_NODE * xasl, QO_PLAN * plan);
static XASL_NODE *init_list_scan_proc (QO_ENV * env, XASL_NODE * xasl,
                                       XASL_NODE * list, PT_NODE * namelist, BITSET * predset);

static XASL_NODE *add_access_spec (QO_ENV *, XASL_NODE *, QO_PLAN *);
static XASL_NODE *add_scan_proc (QO_ENV * env, XASL_NODE * xasl, XASL_NODE * scan);
static XASL_NODE *add_uncorrelated (QO_ENV * env, XASL_NODE * xasl, XASL_NODE * sub);
static XASL_NODE *add_subqueries (QO_ENV * env, XASL_NODE * xasl, BITSET *);
static XASL_NODE *add_sort_spec (QO_ENV *, XASL_NODE *, QO_PLAN *, DB_VALUE *, bool);
static XASL_NODE *add_if_predicate (QO_ENV *, XASL_NODE *, PT_NODE *);
static XASL_NODE *add_after_join_predicate (QO_ENV *, XASL_NODE *, PT_NODE *);
static XASL_NODE *add_instnum_predicate (QO_ENV *, XASL_NODE *, PT_NODE *);

static PT_NODE *make_pred_from_bitset (QO_ENV * env, BITSET * predset, ELIGIBILITY_FN safe);
static void make_pred_from_plan (QO_ENV * env, QO_PLAN * plan,
                                 PT_NODE ** key_access_pred,
                                 PT_NODE ** access_pred, QO_XASL_INDEX_INFO * qo_index_infop);
static PT_NODE *make_if_pred_from_plan (QO_ENV * env, QO_PLAN * plan);
static PT_NODE *make_instnum_pred_from_plan (QO_ENV * env, QO_PLAN * plan);
static PT_NODE *make_namelist_from_projected_segs (QO_ENV * env, QO_PLAN * plan);

static XASL_NODE *gen_outer (QO_ENV *, QO_PLAN *, BITSET *, XASL_NODE *, XASL_NODE *, XASL_NODE *);
static XASL_NODE *gen_inner (QO_ENV *, QO_PLAN *, BITSET *, BITSET *, XASL_NODE *, XASL_NODE *);
static XASL_NODE *preserve_info (QO_ENV * env, QO_PLAN * plan, XASL_NODE * xasl);

static int is_normal_access_term (QO_TERM *);
static int is_normal_if_term (QO_TERM *);
static int is_after_join_term (QO_TERM *);
static int is_totally_after_join_term (QO_TERM *);
static int is_always_true (QO_TERM *);

static QO_XASL_INDEX_INFO *qo_get_xasl_index_info (QO_ENV * env, QO_PLAN * plan);
static void qo_free_xasl_index_info (QO_ENV * env, QO_XASL_INDEX_INFO * info);

static bool qo_validate_regu_var_for_limit (REGU_VARIABLE * var_p);
static bool qo_get_limit_from_instnum_pred (PARSER_CONTEXT * parser,
                                            PRED_EXPR * pred, REGU_PTR_LIST * lower, REGU_PTR_LIST * upper);
static bool qo_get_limit_from_eval_term (PARSER_CONTEXT * parser,
                                         PRED_EXPR * pred, REGU_PTR_LIST * lower, REGU_PTR_LIST * upper);

static void regu_ptr_list_free (REGU_PTR_LIST list);
static REGU_PTR_LIST regu_ptr_list_add_regu (REGU_VARIABLE * var_p, REGU_PTR_LIST list);

static bool qo_check_seg_belongs_to_range_term (QO_PLAN * subplan, QO_ENV * env, int seg_idx);
static int qo_check_plan_index_for_multi_range_opt (PT_NODE *
                                                    orderby_nodes,
                                                    PT_NODE *
                                                    orderby_sort_list,
                                                    QO_PLAN * plan,
                                                    bool * is_valid, int *first_col_idx_pos, bool * reverse);
static int qo_check_terms_for_multiple_range_opt (QO_PLAN * plan, int first_sort_col_idx, bool * can_optimize);
static bool qo_check_subqueries_for_multi_range_opt (QO_PLAN * plan, int sort_col_idx_pos);

static int qo_check_subplans_for_multi_range_opt (QO_PLAN * parent,
                                                  QO_PLAN * plan, QO_PLAN * sortplan, bool * is_valid, bool * seen);
static bool qo_check_subplan_join_cond_for_multi_range_opt (QO_PLAN * parent, QO_PLAN * subplan, QO_PLAN * sort_plan);
static XASL_NODE *make_sort_limit_proc (QO_ENV * env, QO_PLAN * plan, PT_NODE * namelist, XASL_NODE * xasl);
static PT_NODE *qo_get_orderby_num_upper_bound_node (PARSER_CONTEXT * parser,
                                                     PT_NODE * orderby_for, bool * is_new_node);

/*
 * make_scan_proc () -
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 */
static XASL_NODE *
make_scan_proc (QO_ENV * env)
{
  return ptqo_to_scan_proc (QO_ENV_PARSER (env), NULL, NULL, NULL, NULL, NULL, NULL);
}


/*
 * make_buildlist_proc () -
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   namelist(in): The list of names to use as input to and output from this
 *		   node
 */
static XASL_NODE *
make_buildlist_proc (QO_ENV * env, PT_NODE * namelist)
{
  return pt_skeleton_buildlist_proc (QO_ENV_PARSER (env), namelist);
}


/*
 * mark_access_as_outer_join () - mark aan xasl proc's access spec
 *				  as left outer join
 *   return:
 *   parser(in): The parser environment
 *   xasl(in): The already allocated node to be initialized
 */
static void
mark_access_as_outer_join (UNUSED_ARG PARSER_CONTEXT * parser, XASL_NODE * xasl)
{
  ACCESS_SPEC_TYPE *access;

  for (access = xasl->spec_list; access; access = access->next)
    {
      access->fetch_type = QPROC_FETCH_OUTER;
    }
}

/*
 * init_class_scan_proc () -
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   xasl(in): The already allocated node to be initialized
 *   plan(in): The plan from which to initialize the scan proc
 *
 * Note: Take a BUILDwhatever skeleton and flesh it out as a scan
 *	gadget.  Don't mess with any other fields than you absolutely
 *	must:  they may have already been initialized by other
 *	routines.
 */
static XASL_NODE *
init_class_scan_proc (QO_ENV * env, XASL_NODE * xasl, QO_PLAN * plan)
{
  PARSER_CONTEXT *parser;
  PT_NODE *spec;
  PT_NODE *key_pred;
  PT_NODE *access_pred;
  PT_NODE *if_pred;
  PT_NODE *after_join_pred;
  QO_XASL_INDEX_INFO *info;

  parser = QO_ENV_PARSER (env);

  spec = QO_NODE_ENTITY_SPEC (plan->plan_un.scan.node);

  info = qo_get_xasl_index_info (env, plan);
  make_pred_from_plan (env, plan, &key_pred, &access_pred, info);
  xasl = ptqo_to_scan_proc (parser, plan, xasl, spec, key_pred, access_pred, info);

  /* free pointer node list */
  parser_free_tree (parser, key_pred);
  parser_free_tree (parser, access_pred);

  if (xasl)
    {
      after_join_pred = make_pred_from_bitset (env, &(plan->sarged_terms), is_after_join_term);
      if_pred = make_if_pred_from_plan (env, plan);

      xasl = add_after_join_predicate (env, xasl, after_join_pred);
      xasl = add_if_predicate (env, xasl, if_pred);

      /* free pointer node list */
      parser_free_tree (parser, after_join_pred);
      parser_free_tree (parser, if_pred);
    }

  if (info)
    {
      qo_free_xasl_index_info (env, info);
    }

  return xasl;
}

/*
 * init_list_scan_proc () -
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   xasl(in): The already allocated node to be initialized
 *   listfile(in): The buildlist proc node for the list file to be scanned
 *   namelist(in): The list of names (columns) to be retrieved from the file
 *   predset(in): A bitset of predicates to be added to the access spec
 *
 * Note: Take a BUILDwhatever skeleton and flesh it out as a scan
 *	gadget.  Don't mess with any other fields than you absolutely
 *	must:  they may have already been initialized by other
 *	routines.
 */
static XASL_NODE *
init_list_scan_proc (QO_ENV * env, XASL_NODE * xasl, XASL_NODE * listfile, PT_NODE * namelist, BITSET * predset)
{
  PT_NODE *access_pred, *if_pred, *after_join_pred, *instnum_pred;

  if (xasl)
    {
      access_pred = make_pred_from_bitset (env, predset, is_normal_access_term);
      if_pred = make_pred_from_bitset (env, predset, is_normal_if_term);
      after_join_pred = make_pred_from_bitset (env, predset, is_after_join_term);
      instnum_pred = make_pred_from_bitset (env, predset, is_totally_after_join_term);

      xasl = ptqo_to_list_scan_proc (QO_ENV_PARSER (env), xasl, SCAN_PROC, listfile, namelist, access_pred);

      xasl = add_if_predicate (env, xasl, if_pred);
      xasl = add_after_join_predicate (env, xasl, after_join_pred);
      xasl = add_instnum_predicate (env, xasl, instnum_pred);

      /* free pointer node list */
      parser_free_tree (QO_ENV_PARSER (env), access_pred);
      parser_free_tree (QO_ENV_PARSER (env), if_pred);
      parser_free_tree (QO_ENV_PARSER (env), after_join_pred);
      parser_free_tree (QO_ENV_PARSER (env), instnum_pred);
    }

  return xasl;
}

/*
 * add_access_spec () -
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   xasl(in): The XASL block to twiddle
 *   plan(in):
 */
static XASL_NODE *
add_access_spec (QO_ENV * env, XASL_NODE * xasl, QO_PLAN * plan)
{
  PARSER_CONTEXT *parser;
  PT_NODE *tree;
  PT_NODE *class_spec;
  PT_NODE *key_pred = NULL;
  PT_NODE *access_pred = NULL;
  PT_NODE *if_pred = NULL;
  PT_NODE *instnum_pred = NULL;
  QO_XASL_INDEX_INFO *info = NULL;

  if (xasl == NULL)
    {                           /* may be invalid argument */
      return xasl;
    }

  assert (plan->plan_type == QO_PLANTYPE_SCAN);

  parser = QO_ENV_PARSER (env);
  tree = QO_ENV_PT_TREE (env);
  assert (tree != NULL);
  assert (tree->node_type == PT_SELECT);

  class_spec = QO_NODE_ENTITY_SPEC (plan->plan_un.scan.node);

  info = qo_get_xasl_index_info (env, plan);
  make_pred_from_plan (env, plan, &key_pred, &access_pred, info);

  xasl->spec_list = pt_to_spec_list (parser, class_spec,
                                     key_pred, access_pred, tree->info.query.pk_next, plan, info, NULL);
  if (xasl->spec_list == NULL)
    {
      goto exit_on_error;
    }

  xasl->val_list = pt_to_val_list (parser, class_spec->info.spec.id);
  if (xasl->val_list == NULL)
    {
      goto exit_on_error;
    }

  if_pred = make_if_pred_from_plan (env, plan);
  instnum_pred = make_instnum_pred_from_plan (env, plan);

  xasl = add_if_predicate (env, xasl, if_pred);
  xasl = add_instnum_predicate (env, xasl, instnum_pred);

success:

  /* free pointer node list */
  parser_free_tree (parser, key_pred);
  parser_free_tree (parser, access_pred);
  parser_free_tree (parser, if_pred);
  parser_free_tree (parser, instnum_pred);

  qo_free_xasl_index_info (env, info);

  return xasl;

exit_on_error:

  xasl = NULL;
  goto success;
}

/*
 * add_scan_proc () - Add the scan proc to the end of xasl's scan_ptr list
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   xasl(in): The XASL block to receive the scan block
 *   scan(in): The scanproc to be added
 */
static XASL_NODE *
add_scan_proc (UNUSED_ARG QO_ENV * env, XASL_NODE * xasl, XASL_NODE * scan)
{
  XASL_NODE *xp;

  if (xasl)
    {
      for (xp = xasl; xp->scan_ptr; xp = xp->scan_ptr)
        ;
      xp->scan_ptr = scan;
    }
  else
    xasl = NULL;

  return xasl;
}

/*
 * add_uncorrelated () - Add the scan proc to the *head* of the list of
 *			 scanprocs in xasl->scan_ptr
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   xasl(in): The XASL block to receive the scan block
 *   sub(in): The XASL thing to be added to xasl's list of uncorrelated
 *	      "subqueries"
 */
static XASL_NODE *
add_uncorrelated (UNUSED_ARG QO_ENV * env, XASL_NODE * xasl, XASL_NODE * sub)
{

  if (xasl && sub)
    {
      xasl->aptr_list = pt_remove_xasl (pt_append_xasl (xasl->aptr_list, sub), xasl);
    }
  else
    {
      xasl = NULL;
    }

  return xasl;
}

/*
 * add_subqueries () - Add the xasl trees for the subqueries to the xasl node
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   xasl(in): The XASL block to receive the scan block
 *   subqueries(in): A bitset representing the correlated subqueries that
 *		     should be tacked onto xasl
 *
 * Note: Because of the way the outer driver controls
 *	things, we never have to worry about subqueries that nested
 *	deeper than one, so there is no ordering that needs to be
 *	maintained here; we can just put these guys on the d-list in
 *	any convenient order.
 */
static XASL_NODE *
add_subqueries (QO_ENV * env, XASL_NODE * xasl, BITSET * subqueries)
{
  BITSET_ITERATOR bi;
  int i;
  XASL_NODE *sub_xasl;
  QO_SUBQUERY *subq;

  if (xasl)
    {
      for (i = bitset_iterate (subqueries, &bi); i != -1; i = bitset_next_member (&bi))
        {
          subq = &env->subqueries[i];
          sub_xasl = (XASL_NODE *) subq->node->info.query.xasl;
          if (sub_xasl)
            {
              if (bitset_is_empty (&(subq->nodes)))
                {               /* uncorrelated */
                  xasl->aptr_list = pt_remove_xasl (pt_append_xasl (xasl->aptr_list, sub_xasl), xasl);
                }
              else
                {               /* correlated */
                  xasl->dptr_list = pt_remove_xasl (pt_append_xasl (xasl->dptr_list, sub_xasl), xasl);
                }
            }
        }
    }

  return xasl;
}

/*
 * add_sort_spec () -
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   xasl(in): The XASL node that should build a sorted result
 *   plan(in): The plan that needs sorting
 *   instnum_flag(in): instnum indicator
 */
static XASL_NODE *
add_sort_spec (QO_ENV * env, XASL_NODE * xasl, QO_PLAN * plan, DB_VALUE * ordby_val, UNUSED_ARG bool instnum_flag)
{
  UNUSED_VAR QO_PLAN *subplan;

  subplan = plan->plan_un.sort.subplan;

  if (xasl && plan->plan_un.sort.sort_type == SORT_LIMIT)
    {
      /* setup ORDER BY list here */
      int ordbynum_flag;
      QO_LIMIT_INFO *limit_infop;
      PARSER_CONTEXT *parser = QO_ENV_PARSER (env);
      PT_NODE *query = QO_ENV_PT_TREE (env);
      PT_NODE *upper_bound = NULL, *save_next = NULL;
      bool free_upper_bound = false;

      xasl->orderby_list = pt_to_orderby (parser, query->info.query.order_by, query);
      XASL_CLEAR_FLAG (xasl, XASL_SKIP_ORDERBY_LIST);

      xasl->orderby_limit = NULL;
      /* A SORT-LIMIT plan can only handle the upper limit of the orderby_num
       * predicate. This is because the orderby_num pred will be applied
       * twice: once for the SORT-LIMIT plan and once for the top plan.
       * If the lower bound is evaluated twice, some tuples are lost.
       */
      upper_bound = query->info.query.orderby_for;
      upper_bound = qo_get_orderby_num_upper_bound_node (parser, upper_bound, &free_upper_bound);
      if (upper_bound == NULL)
        {
          /* Must have an upper limit if we're considering a SORT-LIMIT
           * plan.
           */
          return NULL;
        }
      save_next = upper_bound->next;
      upper_bound->next = NULL;
      ordbynum_flag = 0;
      xasl->ordbynum_pred = pt_to_pred_expr_with_arg (parser, upper_bound, &ordbynum_flag);
      upper_bound->next = save_next;
      if (free_upper_bound)
        {
          parser_free_tree (parser, upper_bound);
        }

      if (ordbynum_flag & PT_PRED_ARG_ORDBYNUM_CONTINUE)
        {
          xasl->ordbynum_flag = XASL_ORDBYNUM_FLAG_SCAN_CONTINUE;
        }
      limit_infop = qo_get_key_limit_from_ordbynum (parser, plan, xasl, false);
      if (limit_infop)
        {
          xasl->orderby_limit = limit_infop->upper;
          free_and_init (limit_infop);
        }
      xasl->ordbynum_val = ordby_val;
    }

  return xasl;
}

/*
 * add_if_predicate () - Tack the predicate onto the XASL node's if_pred list
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   xasl(in): The XASL block to which we should add the predicate
 *   pred(in): The pt predicate to tacked on to xasl
 */
static XASL_NODE *
add_if_predicate (QO_ENV * env, XASL_NODE * xasl, PT_NODE * pred)
{
  PARSER_CONTEXT *parser;

  if (xasl && pred)
    {
      parser = QO_ENV_PARSER (env);
      xasl->if_pred = pt_to_pred_expr (parser, pred);
    }

  return xasl;
}

/*
 * add_after_join_predicate () -
 *   return:
 *   env(in):
 *   xasl(in):
 *   pred(in):
 */
static XASL_NODE *
add_after_join_predicate (QO_ENV * env, XASL_NODE * xasl, PT_NODE * pred)
{
  PARSER_CONTEXT *parser;

  if (xasl && pred)
    {
      parser = QO_ENV_PARSER (env);
      xasl->after_join_pred = pt_to_pred_expr (parser, pred);
    }

  return xasl;
}

static XASL_NODE *
add_instnum_predicate (QO_ENV * env, XASL_NODE * xasl, PT_NODE * pred)
{
  PARSER_CONTEXT *parser;
  int flag;

  if (xasl && pred)
    {
      parser = QO_ENV_PARSER (env);

      flag = 0;
      xasl->instnum_pred = pt_to_pred_expr_with_arg (parser, pred, &flag);
      if (flag & PT_PRED_ARG_INSTNUM_CONTINUE)
        {
          xasl->instnum_flag = XASL_INSTNUM_FLAG_SCAN_CONTINUE;
        }
    }

  return xasl;
}


/*
 * is_normal_access_term () -
 *   return:
 *   term(in):
 */
static int
is_normal_access_term (QO_TERM * term)
{
  if (!bitset_is_empty (&(QO_TERM_SUBQUERIES (term))))
    {
      return 0;
    }

  if (QO_TERM_CLASS (term) == QO_TC_OTHER ||
      /* QO_TERM_CLASS(term) == QO_TC_DURING_JOIN || */
      /* nl outer join treats during join terms as sarged terms of inner */
      QO_TERM_CLASS (term) == QO_TC_AFTER_JOIN || QO_TERM_CLASS (term) == QO_TC_TOTALLY_AFTER_JOIN)
    {
      return 0;
    }

  return 1;
}

/*
 * is_normal_if_term () -
 *   return:
 *   term(in):
 */
static int
is_normal_if_term (QO_TERM * term)
{
  if (!bitset_is_empty (&(QO_TERM_SUBQUERIES (term))))
    {
      return 1;
    }
  if (QO_TERM_CLASS (term) == QO_TC_OTHER)
    {
      return 1;
    }

  return 0;
}

/*
 * is_after_join_term () -
 *   return:
 *   term(in):
 */
static int
is_after_join_term (QO_TERM * term)
{
  if (!bitset_is_empty (&(QO_TERM_SUBQUERIES (term))))
    {
      return 0;
    }
  if (QO_TERM_CLASS (term) == QO_TC_AFTER_JOIN)
    {
      return 1;
    }

  return 0;
}

/*
 * is_totally_after_join_term () -
 *   return:
 *   term(in):
 */
static int
is_totally_after_join_term (QO_TERM * term)
{
  if (!bitset_is_empty (&(QO_TERM_SUBQUERIES (term))))
    {
      return 0;
    }
  if (QO_TERM_CLASS (term) == QO_TC_TOTALLY_AFTER_JOIN)
    {
      return 1;
    }

  return 0;
}


/*
 * is_always_true () -
 *   return:
 *   term(in):
 */
static int
is_always_true (UNUSED_ARG QO_TERM * term)
{
  return true;
}

/*
 * make_pred_from_bitset () -
 *   return: PT_NODE *
 *   env(in): The optimizer environment
 *   predset(in): The bitset of predicates to turn into a predicate tree
 *   safe(in): A function to test whether a particular term should be
 *	       put on a predicate
 *
 * Note: make pred_info * style predicates from a bitset of conjuncts.
 *    use only those conjuncts that can be put on an access pred.
 */
static PT_NODE *
make_pred_from_bitset (QO_ENV * env, BITSET * predset, ELIGIBILITY_FN safe)
{
  PARSER_CONTEXT *parser;
  PT_NODE *pred_list, *pointer, *prev, *curr;
  BITSET_ITERATOR bi;
  int i;
  QO_TERM *term;
  bool found;
  PT_NODE *pt_expr;
  double cmp;

  parser = QO_ENV_PARSER (env);

  pred_list = NULL;             /* init */
  for (i = bitset_iterate (predset, &bi); i != -1; i = bitset_next_member (&bi))
    {
      term = QO_ENV_TERM (env, i);

      /* Don't ever let one of our fabricated terms find its way into
       * the predicate; that will cause serious confusion.
       */
      if (QO_IS_FAKE_TERM (term) || !(*safe) (term))
        {
          continue;
        }

      /* We need to use predicate pointer.
       * modifying WHERE clause structure in place gives us no way
       * to compile the query if the optimizer bails out.
       */
      pt_expr = QO_TERM_PT_EXPR (term);
      if (pt_expr == NULL)
        {
          /* is possible ? */
          goto exit_on_error;
        }

      pointer = pt_point (parser, pt_expr);
      if (pointer == NULL)
        {
          goto exit_on_error;
        }

      /* set AND predicate evaluation selectivity, rank;
       */
      pointer->info.pointer.sel = QO_TERM_SELECTIVITY (term);
      pointer->info.pointer.rank = QO_TERM_RANK (term);

      /* insert to the AND predicate list by descending order of
       * (selectivity, rank) vector; this order is used at
       * pt_to_pred_expr_with_arg()
       */
      found = false;            /* init */
      prev = NULL;              /* init */
      for (curr = pred_list; curr; curr = curr->next)
        {
          cmp = curr->info.pointer.sel - pointer->info.pointer.sel;

          if (cmp == 0)
            {                   /* same selectivity, re-compare rank */
              cmp = curr->info.pointer.rank - pointer->info.pointer.rank;
            }

          if (cmp <= 0)
            {
              pointer->next = curr;
              if (prev == NULL)
                {               /* very the first */
                  pred_list = pointer;
                }
              else
                {
                  prev->next = pointer;
                }
              found = true;
              break;
            }

          prev = curr;
        }

      /* append to the predicate list */
      if (found == false)
        {
          if (prev == NULL)
            {                   /* very the first */
              pointer->next = pred_list;
              pred_list = pointer;
            }
          else
            {                   /* very the last */
              prev->next = pointer;
            }
        }
    }

  return pred_list;

exit_on_error:

  if (pred_list)
    {
      parser_free_tree (parser, pred_list);
    }

  return NULL;
}

/*
 * make_pred_from_plan () -
 *   return:
 *   env(in): The optimizer environment
 *   plan(in): Query plan
 *   key_predp(in): Index information of query plan.
 *		    Predicate tree to be used as key filter
 *   predp(in): Predicate tree to be used as data filter
 *   qo_index_infop(in):
 *
 * Note: Make a PT_NODE * style predicate from a bitset of conjuncts.
 *     Splits sargs into key filter predicates and data filter predicates.
 */
static void
make_pred_from_plan (QO_ENV * env, QO_PLAN * plan,
                     PT_NODE ** key_predp, PT_NODE ** predp, QO_XASL_INDEX_INFO * qo_index_infop)
{
  /* initialize output parameter */
  if (key_predp != NULL)
    {
      *key_predp = NULL;
    }

  if (predp != NULL)
    {
      *predp = NULL;
    }

  /* This is safe guard code - DO NOT DELETE ME
   */
  do
    {
      /* exclude key-range terms from key-filter terms */
      bitset_difference (&(plan->plan_un.scan.kf_terms), &(plan->plan_un.scan.terms));

      /* exclude key-range terms from sarged terms */
      bitset_difference (&(plan->sarged_terms), &(plan->plan_un.scan.terms));
      /* exclude key-filter terms from sarged terms */
      bitset_difference (&(plan->sarged_terms), &(plan->plan_un.scan.kf_terms));
    }
  while (0);

  /* if key filter(predicates) is not required */
  if (predp != NULL && (key_predp == NULL || qo_index_infop == NULL))
    {
      *predp = make_pred_from_bitset (env, &(plan->sarged_terms), is_normal_access_term);
      return;
    }

  /* make predicate list for key filter */
  if (key_predp != NULL)
    {
      *key_predp = make_pred_from_bitset (env, &(plan->plan_un.scan.kf_terms), is_always_true);
    }

  /* make predicate list for data filter */
  if (predp != NULL)
    {
      *predp = make_pred_from_bitset (env, &(plan->sarged_terms), is_normal_access_term);
    }
}

/*
 * make_if_pred_from_plan () -
 *   return:
 *   env(in):
 *   plan(in):
 */
static PT_NODE *
make_if_pred_from_plan (QO_ENV * env, QO_PLAN * plan)
{
  return make_pred_from_bitset (env, &(plan->sarged_terms), is_normal_if_term);
}

/*
 * make_instnum_pred_from_plan () -
 *   return:
 *   env(in):
 *   plan(in):
 */
static PT_NODE *
make_instnum_pred_from_plan (QO_ENV * env, QO_PLAN * plan)
{
  /* is it enough? */
  return make_pred_from_bitset (env, &(plan->sarged_terms), is_totally_after_join_term);
}                               /* make_instnum_pred_from_plan() */

/*
 * make_namelist_from_projected_segs () -
 *   return: PT_NODE *
 *   env(in): The optimizer environment
 *   plan(in): he plan whose projected segments need to be put into a name list
 *
 * Note: Take a bitset of segment indexes and produce a name list
 *	suitable for creating the outptr_list member of a buildlist
 *	proc.  This is used by the creators of temporary list files: sorts.
 *
 *	In the interests of sanity, the elements in the list appear
 *	in the same order as the indexes in the scan of the bitset.
 */
static PT_NODE *
make_namelist_from_projected_segs (QO_ENV * env, QO_PLAN * plan)
{
  PARSER_CONTEXT *parser;
  PT_NODE *namelist;
  PT_NODE **namelistp;
  BITSET_ITERATOR bi;
  int i;

  parser = QO_ENV_PARSER (env);
  namelist = NULL;
  namelistp = &namelist;

  for (i = bitset_iterate (&((plan->info)->projected_segs), &bi);
       namelistp != NULL && i != -1; i = bitset_next_member (&bi))
    {
      QO_SEGMENT *seg;
      PT_NODE *name;

      seg = QO_ENV_SEG (env, i);
      name = pt_point (parser, QO_SEG_PT_NODE (seg));

      *namelistp = name;
      namelistp = &name->next;
    }

  return namelist;
}

/*
 * make_outer_instnum () -
 *   return:
 *   env(in):
 *   outer(in):
 *   plan(in):
 */
static void
make_outer_instnum (QO_ENV * env, QO_PLAN * outer, QO_PLAN * plan)
{
  int t;
  BITSET_ITERATOR iter;
  QO_TERM *termp;

  for (t = bitset_iterate (&(plan->sarged_terms), &iter); t != -1; t = bitset_next_member (&iter))
    {

      termp = QO_ENV_TERM (env, t);

      if (is_totally_after_join_term (termp))
        {
          bitset_add (&(outer->sarged_terms), t);
        }
    }
}

/*
 * gen_outer () -
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   plan(in): The (sub)plan to generate code for
 *   subqueries(in): The set of subqueries that need to be reevaluated every
 *		     time a new row is produced by plan
 *   inner_scans(in): A list of scan
 *   fetches(in): A list of fetch procs that should be executed every time plan
 *		  produces a new row
 *   xasl(in): The xasl node that is receiving the various procs we generate
 */
static XASL_NODE *
gen_outer (QO_ENV * env, QO_PLAN * plan, BITSET * subqueries,
           XASL_NODE * inner_scans, XASL_NODE * fetches, XASL_NODE * xasl)
{
  PARSER_CONTEXT *parser;
  XASL_NODE *scan, *listfile;
  QO_PLAN *outer, *inner;
  JOIN_TYPE join_type = NO_JOIN;
  QO_TERM *term;
  int i;
  BITSET_ITERATOR bi;
  BITSET new_subqueries;
  BITSET fake_subqueries;
  BITSET predset;
  BITSET taj_terms;

  if (env == NULL)
    {
      return NULL;
    }
  parser = QO_ENV_PARSER (env);

  if (parser == NULL || plan == NULL || xasl == NULL)
    {
      return NULL;
    }

  bitset_init (&new_subqueries, env);
  bitset_init (&fake_subqueries, env);
  bitset_init (&predset, env);
  bitset_init (&taj_terms, env);

  /* set subqueries */
  bitset_assign (&new_subqueries, subqueries);
  bitset_union (&new_subqueries, &(plan->subqueries));

  /* set predicates */
  bitset_assign (&predset, &(plan->sarged_terms));

  switch (plan->plan_type)
    {
    case QO_PLANTYPE_JOIN:
      join_type = plan->plan_un.join.join_type;

      /*
       * The join terms may be EMPTY if this "join" is actually a
       * cartesian product, or if it has been implemented as an
       * index scan on the inner term (in which case it has already
       * been placed in the inner plan as the index term).
       */
      bitset_union (&predset, &(plan->plan_un.join.join_terms));

      /* outer join could have terms classed as AFTER JOIN TERM;
       * setting after join terms to merged list scan
       */
      if (IS_OUTER_JOIN_TYPE (join_type))
        {
          bitset_union (&predset, &(plan->plan_un.join.during_join_terms));
          bitset_union (&predset, &(plan->plan_un.join.after_join_terms));
        }
      break;

    default:
      break;
    }

  /*
   * Because this routine tail-calls itself in several common cases, we
   * could implement those tail calls with a loop back to the beginning
   * of the code.  However, because these calls won't get very deep in
   * practice (~10 deep), I've left the code as is in the interest of
   * clarity.
   */

  switch (plan->plan_type)
    {
    case QO_PLANTYPE_SCAN:
      /*
       * This case only needs to attach the access spec to the incoming
       * XASL node.  The remainder of the interesting initialization
       * (e.g., the val list) of that XASL node is expected to be
       * performed by the caller.
       */
      xasl = add_access_spec (env, xasl, plan);
      xasl = add_scan_proc (env, xasl, inner_scans);
      xasl = add_subqueries (env, xasl, &new_subqueries);
      break;

    case QO_PLANTYPE_SORT:
      /*
       * check for top level plan
       */
      if (plan->top_rooted)
        {
          if (plan->plan_un.sort.sort_type == SORT_TEMP)
            {
              ;                 /* nop */
            }
          else
            {
              /* SORT-LIMIT plans should never be top rooted */
              assert (plan->plan_un.sort.sort_type != SORT_LIMIT);
              xasl = gen_outer (env, plan->plan_un.sort.subplan, &new_subqueries, inner_scans, fetches, xasl);
              return xasl;
            }
        }

      /*
       * If inner_scans is not empty, this plan is really a subplan of
       * some outer join node, and we need to make xasl scan the
       * contents of the temp file intended to be created by this plan.
       * If not, we're really at the "top" of a tree (we haven't gone
       * through a join node yet) and we can simply recurse, tacking on
       * our sort spec after the recursion. The exception to this rule is
       * the SORT-LIMIT plan which must always be working on a temp file.
       */
      if (inner_scans != NULL || plan->plan_un.sort.sort_type == SORT_LIMIT)
        {
          PT_NODE *namelist = NULL;

          namelist = make_namelist_from_projected_segs (env, plan);
          if (plan->plan_un.sort.sort_type == SORT_LIMIT)
            {
              listfile = make_sort_limit_proc (env, plan, namelist, xasl);
            }
          else
            {
              listfile = make_buildlist_proc (env, namelist);
              listfile = gen_outer (env, plan->plan_un.sort.subplan, &EMPTY_SET, NULL, NULL, listfile);
              listfile = add_sort_spec (env, listfile, plan, xasl->ordbynum_val, false);
            }

          xasl = add_uncorrelated (env, xasl, listfile);
          xasl = init_list_scan_proc (env, xasl, listfile, namelist, &(plan->sarged_terms));
          if (namelist)
            {
              parser_free_tree (parser, namelist);
            }
          xasl = add_scan_proc (env, xasl, inner_scans);
          xasl = add_subqueries (env, xasl, &new_subqueries);
        }
      else
        {
          xasl = gen_outer (env, plan->plan_un.sort.subplan, &new_subqueries, inner_scans, fetches, xasl);
          xasl = add_sort_spec (env, xasl, plan, NULL, true /*add instnum pred */ );
        }
      break;

    case QO_PLANTYPE_JOIN:

      outer = plan->plan_un.join.outer;
      inner = plan->plan_un.join.inner;

      switch (plan->plan_un.join.join_method)
        {
        case QO_JOINMETHOD_NL_JOIN:
        case QO_JOINMETHOD_IDX_JOIN:
          for (i = bitset_iterate (&(plan->plan_un.join.join_terms), &bi); i != -1; i = bitset_next_member (&bi))
            {
              term = QO_ENV_TERM (env, i);
              if (QO_IS_FAKE_TERM (term))
                {
                  bitset_union (&fake_subqueries, &(QO_TERM_SUBQUERIES (term)));
                }
            }                   /* for (i = ... ) */

          bitset_difference (&new_subqueries, &fake_subqueries);

          for (i = bitset_iterate (&predset, &bi); i != -1; i = bitset_next_member (&bi))
            {
              term = QO_ENV_TERM (env, i);
              if (is_totally_after_join_term (term))
                {
                  bitset_add (&taj_terms, i);
                }
              else if (is_normal_access_term (term))
                {
                  /* Check if join term can be pushed to key filter instead of
                   * sargable terms. The index used for inner index scan must
                   * include all term segments that belong to inner node
                   */
                  if (qo_is_index_covering_scan (inner) || qo_plan_multi_range_opt (inner))
                    {
                      /* Coverage indexes and indexes using multi range
                       * optimization are certified to include segments from
                       * inner node
                       */
                      bitset_add (&(inner->plan_un.scan.kf_terms), i);
                      bitset_difference (&predset, &(inner->plan_un.scan.kf_terms));
                    }
                  else if (qo_is_iscan (inner))
                    {
                      /* check that index covers all segments */
                      BITSET term_segs, index_segs;
                      QO_INDEX_ENTRY *idx_entryp = NULL;
                      int j;

                      /* create bitset including index segments */
                      bitset_init (&index_segs, env);
                      idx_entryp = inner->plan_un.scan.index->head;
                      for (j = 0; j < idx_entryp->nsegs; j++)
                        {
                          if (idx_entryp->seg_idxs[j] == -1)
                            {
                              continue;
                            }
                          bitset_add (&index_segs, idx_entryp->seg_idxs[j]);
                        }

                      /* create bitset including term segments that belong to
                       * inner node
                       */
                      bitset_init (&term_segs, env);
                      bitset_union (&term_segs, &term->segments);
                      bitset_intersect (&term_segs, &(QO_NODE_SEGS (inner->plan_un.scan.node)));

                      /* check that term_segs is covered by index_segs */
                      bitset_difference (&term_segs, &index_segs);
                      if (bitset_is_empty (&term_segs))
                        {
                          /* safe to add term to key filter terms */
                          bitset_add (&(inner->plan_un.scan.kf_terms), i);
                          bitset_difference (&predset, &(inner->plan_un.scan.kf_terms));
                        }
                    }
                }
            }                   /* for (i = ... ) */
          /* exclude totally after join term and push into inner */
          bitset_difference (&predset, &taj_terms);

          /*
           * In case of outer join, we should not use sarg terms as key filter terms.
           * If not, a term, which should be applied after single scan, can be applied
           * during btree_range_search. It means that there can be no records fetched
           * by single scan due to key filtering, and null records can be returned
           * by scan_handle_single_scan. It might lead to making a wrong result.
           */
          scan = gen_inner (env, inner, &predset, &new_subqueries, inner_scans, fetches);
          if (scan)
            {
              if (IS_OUTER_JOIN_TYPE (join_type))
                {
                  mark_access_as_outer_join (parser, scan);
                }
            }
          bitset_assign (&new_subqueries, &fake_subqueries);
          make_outer_instnum (env, outer, plan);
          xasl = gen_outer (env, outer, &new_subqueries, scan, NULL, xasl);
          break;

        default:
          break;
        }                       /* switch (plan->plan_un.join.join_method) */

      break;

    case QO_PLANTYPE_WORST:
      xasl = NULL;
      break;
    }

  bitset_delset (&taj_terms);
  bitset_delset (&predset);
  bitset_delset (&fake_subqueries);
  bitset_delset (&new_subqueries);

  return xasl;
}

/*
 * gen_inner () -
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   plan(in): The (sub)plan to generate code for
 *   predset(in): The predicates being pushed down from above
 *   subqueries(in): The subqueries inherited from enclosing plans
 *   inner_scans(in): A list of inner scan procs to be put on this scan's
 *		      scan_ptr list
 *   fetches(in): A list of fetch procs to be run every time plan produces
 *		  a new row
 */
static XASL_NODE *
gen_inner (QO_ENV * env, QO_PLAN * plan, BITSET * predset,
           BITSET * subqueries, XASL_NODE * inner_scans, UNUSED_ARG XASL_NODE * fetches)
{
  XASL_NODE *scan, *listfile;
  PT_NODE *namelist;
  BITSET new_subqueries;

  /*
   * All of the rationale about ordering, etc. presented in the
   * comments in gen_outer also applies here.
   */

  scan = NULL;
  bitset_init (&new_subqueries, env);
  bitset_assign (&new_subqueries, subqueries);
  bitset_union (&new_subqueries, &(plan->subqueries));

  switch (plan->plan_type)
    {
    case QO_PLANTYPE_SCAN:
      /*
       * For nl-join and idx-join, we push join edge to sarg term of
       * inner scan to filter out unsatisfied records earlier.
       */
      bitset_union (&(plan->sarged_terms), predset);

      scan = init_class_scan_proc (env, scan, plan);
      scan = add_scan_proc (env, scan, inner_scans);
      scan = add_subqueries (env, scan, &new_subqueries);
      break;

    case QO_PLANTYPE_JOIN:
      /*
       * These aren't supposed to show up, but if they do just take the
       * conservative approach of treating them like a sort and
       * whacking their results into a temporary file, and then scan
       * that file.
       */
    case QO_PLANTYPE_SORT:
      /* check for sort type */
      QO_ASSERT (env, plan->plan_un.sort.sort_type == SORT_TEMP);

      namelist = make_namelist_from_projected_segs (env, plan);
      listfile = make_buildlist_proc (env, namelist);
      listfile = gen_outer (env, plan, &EMPTY_SET, NULL, NULL, listfile);
      scan = make_scan_proc (env);
      scan = init_list_scan_proc (env, scan, listfile, namelist, predset);
      if (namelist)
        parser_free_tree (env->parser, namelist);
      scan = add_scan_proc (env, scan, inner_scans);
      scan = add_subqueries (env, scan, &new_subqueries);
      scan = add_uncorrelated (env, scan, listfile);
      break;

    case QO_PLANTYPE_WORST:
      /*
       * This case should never arise.
       */
      scan = NULL;
      break;
    }

  bitset_delset (&new_subqueries);

  return scan;
}

/*
 * preserve_info () -
 *   return: XASL_NODE *
 *   env(in): The optimizer environment
 *   plan(in): The plan from which the xasl tree was generated
 *   xasl(in): The generated xasl tree
 */
static XASL_NODE *
preserve_info (QO_ENV * env, QO_PLAN * plan, XASL_NODE * xasl)
{
  QO_SUMMARY *summary;
  PARSER_CONTEXT *parser;
  PT_NODE *select;

  if (xasl != NULL)
    {
      parser = QO_ENV_PARSER (env);
      select = QO_ENV_PT_TREE (env);
      summary = (QO_SUMMARY *) parser_alloc (parser, sizeof (QO_SUMMARY));
      if (summary)
        {
          summary->cost = qo_plan_cost (plan);
          summary->cardinality = (plan->info)->cardinality;
          summary->xasl = xasl;
          select->info.query.q.select.qo_summary = summary;
        }
      else
        {
          xasl = NULL;
        }

      /* save info for derived table size estimation */
      if (plan != NULL && xasl != NULL)
        {
          xasl->projected_size = (plan->info)->projected_size;
          xasl->cardinality = (plan->info)->cardinality;
        }
    }

  return xasl;
}

/*
 * qo_to_xasl () -
 *   return: XASL_NODE *
 *   plan(in): The (already optimized) select statement to generate code for
 *   xasl(in): The XASL block for the root of the plan
 *
 * Note: Create an XASL tree from the QO_PLAN tree associated with
 *	'select'.  In essence, this takes the entity specs from the
 *	from part of 'select' and produces a collection of access
 *	specs that will do the right thing.  It also distributes the
 *	predicates in the where part across those access specs.  The
 *	caller shouldn't have to do anything for the from part or the
 *	where part, but it must still take care of all of the other
 *	grunge, such as setting up the code for the select list
 *	expressions, etc.
 */
XASL_NODE *
qo_to_xasl (QO_PLAN * plan, XASL_NODE * xasl)
{
  QO_ENV *env;
  XASL_NODE *lastxasl;

  if (plan && xasl && (env = (plan->info)->env))
    {
      xasl = gen_outer (env, plan, &EMPTY_SET, NULL, NULL, xasl);

      lastxasl = xasl;
      while (lastxasl)
        {
          /*
           * Don't consider only scan pointers here; it's quite
           * possible that the correlated subqueries might depend on
           * values retrieved by a fetch proc that lives on an fptr.
           */
          if (lastxasl->scan_ptr)
            {
              lastxasl = lastxasl->scan_ptr;
            }
          else
            {
              break;
            }
        }
      (void) pt_set_dptr (env->parser, env->pt_tree->info.query.q.select.list, lastxasl, MATCH_ALL);

      xasl = preserve_info (env, plan, xasl);
    }
  else
    {
      xasl = NULL;
    }

  if (xasl == NULL)
    {
      int level;

      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_FAILED_ASSERTION, 1, "xasl != NULL");

      qo_get_optimization_param (&level, QO_PARAM_LEVEL);
      if (PLAN_DUMP_ENABLED (level))
        {
          fprintf (stderr, "*** XASL generation failed ***\n");
        }
#if defined(RYE_DEBUG)
      else
        {
          fprintf (stderr, "*** XASL generation failed ***\n");
          fprintf (stderr, "*** %s ***\n", er_msg ());
        }
#endif /* RYE_DEBUG */
    }

  return xasl;
}

/*
 * qo_plan_iscan_sort_list () - get after index scan PT_SORT_SPEC list
 *   return: SORT_LIST *
 *   plan(in): QO_PLAN
 */
PT_NODE *
qo_plan_iscan_sort_list (QO_PLAN * plan)
{
  return plan->iscan_sort_list;
}

/*
 * qo_plan_skip_orderby () - check the plan info for order by
 *   return: true/false
 *   plan(in): QO_PLAN
 */
bool
qo_plan_skip_orderby (QO_PLAN * plan)
{
  return ((plan->plan_type == QO_PLANTYPE_SORT
           && (plan->plan_un.sort.sort_type == SORT_DISTINCT
               || plan->plan_un.sort.sort_type == SORT_ORDERBY)) ? false : true);
}

/*
 * qo_plan_skip_groupby () - check the plan info for order by
 *   return: true/false
 *   plan(in): QO_PLAN
 */
bool
qo_plan_skip_groupby (QO_PLAN * plan)
{
  return (plan->plan_type == QO_PLANTYPE_SCAN &&
          plan->plan_un.scan.index && plan->plan_un.scan.index->head->groupby_skip) ? true : false;
}

/*
 * qo_is_index_desc_scan () - check the plan info for descending index scan
 *   return: true/false
 *   plan(in): QO_PLAN
 */
bool
qo_is_index_desc_scan (QO_PLAN * plan)
{
  assert (plan != NULL);
  assert (plan->info != NULL);
  assert (plan->info->env != NULL);

  if (qo_is_interesting_order_scan (plan))
    {
      if (plan->plan_un.scan.index && plan->plan_un.scan.index->head)
        {
          return plan->use_iscan_descending;
        }
    }

  return false;
}

/*
 * qo_is_index_covering_scan () - check the plan info for covering index scan
 *   return: true/false
 *   plan(in): QO_PLAN
 */
bool
qo_is_index_covering_scan (QO_PLAN * plan)
{
  assert (plan != NULL);
  assert (plan->info != NULL);
  assert (plan->info->env != NULL);

  if (qo_is_interesting_order_scan (plan))
    {
      if (plan->plan_un.scan.index_cover == true)
        {
          assert (plan->plan_un.scan.index->head);
          assert (plan->plan_un.scan.index->head->cover_segments);

          return true;
        }
    }

  return false;
}

/*
 * qo_is_index_mro_scan () - check the plan info for multi range opt scan
 *   return: true/false
 *   plan(in): QO_PLAN
 */
bool
qo_is_index_mro_scan (QO_PLAN * plan)
{
  assert (plan != NULL);
  assert (plan->info != NULL);
  assert (plan->info->env != NULL);

  if (qo_is_interesting_order_scan (plan))
    {
      if (plan->multi_range_opt_use == PLAN_MULTI_RANGE_OPT_USE)
        {
          assert (plan->plan_un.scan.index->head);

          assert (QO_ENTRY_MULTI_COL (plan->plan_un.scan.index->head));

          return true;
        }
    }

  return false;
}

/*
 * qo_plan_multi_range_opt () - check the plan info for multi range opt
 *   return: true/false
 *   plan(in): QO_PLAN
 */
bool
qo_plan_multi_range_opt (QO_PLAN * plan)
{
  assert (plan != NULL);
  assert (plan->info != NULL);
  assert (plan->info->env != NULL);

  if (plan != NULL && plan->multi_range_opt_use == PLAN_MULTI_RANGE_OPT_USE)
    {
#if !defined(NDEBUG)
      if (qo_is_interesting_order_scan (plan))
        {
          assert (qo_is_index_mro_scan (plan));
        }
#endif

      return true;
    }

  return false;
}

/******************************************************************************
 *  qo_xasl support functions
 *****************************************************************************/

/*
 * qo_get_xasl_index_info () -
 *   return: QO_XASL_INDEX_INFO structure which contains index information
 *	     needed for XASL generation
 *   env(in): The environment
 *   plan(in): The plan from which to initialize the scan proc
 *
 * Note: The term expression array <term_exprs> is returned in index
 *     definition order. i.e. For multi-column indexes, you can create
 *     a sequence key from the expression array in the order that they
 *     are returned.
 */
static QO_XASL_INDEX_INFO *
qo_get_xasl_index_info (QO_ENV * env, QO_PLAN * plan)
{
  int nterms, nsegs, nkfterms;
  QO_NODE_INDEX_ENTRY *ni_entryp;
  QO_INDEX_ENTRY *index_entryp;
  QO_XASL_INDEX_INFO *index_infop;
  int t, i, j, pos;
  BITSET_ITERATOR iter;
  QO_TERM *termp;

  if (!qo_is_interesting_order_scan (plan))
    {
      return NULL;              /* give up */
    }

  assert (plan->plan_un.scan.index != NULL);

  /* if no index scan terms, no index scan */
  nterms = bitset_cardinality (&(plan->plan_un.scan.terms));
  nkfterms = bitset_cardinality (&(plan->plan_un.scan.kf_terms));

  /* pointer to QO_NODE_INDEX_ENTRY structure in QO_PLAN */
  ni_entryp = plan->plan_un.scan.index;
  /* pointer to linked list of index node, 'head' field(QO_INDEX_ENTRY
     strucutre) of QO_NODE_INDEX_ENTRY */
  index_entryp = (ni_entryp)->head;
  /* number of indexed segments */
  nsegs = index_entryp->nsegs;  /* nsegs == nterms ? */

  /* support full range indexes */
  if (nterms <= 0 && nkfterms <= 0 && bitset_cardinality (&(plan->sarged_terms)) == 0)
    {
      if (qo_is_iscan_from_groupby (plan) || qo_is_iscan_from_orderby (plan))
        {
          ;                     /* go ahead */
        }
      else
        {
          assert (qo_is_iscan (plan));

          /* generate full range PK scan */
          if (index_entryp->constraints->type == SM_CONSTRAINT_PRIMARY_KEY)
            {
              ;                 /* go ahead */
            }
          else
            {
              assert (false);
              return NULL;      /* give up */
            }
        }
    }

  /* allocate QO_XASL_INDEX_INFO structure */
  index_infop = (QO_XASL_INDEX_INFO *) malloc (sizeof (QO_XASL_INDEX_INFO));
  if (index_infop == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (QO_XASL_INDEX_INFO));
      goto error;
    }

  if (nterms == 0)
    {
      index_infop->nterms = 0;
      index_infop->term_exprs = NULL;
      index_infop->ni_entry = ni_entryp;
      return index_infop;
    }

  index_infop->nterms = nterms;
  index_infop->term_exprs = (PT_NODE **) malloc (nterms * sizeof (PT_NODE *));
  if (index_infop->term_exprs == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, nterms * sizeof (PT_NODE *));
      goto error;
    }

  index_infop->ni_entry = ni_entryp;

  /* Make 'term_expr[]' array from the given index terms in order of the
     'seg_idx[]' array of the associated index. */

  /* for all index scan terms */
  for (t = bitset_iterate (&(plan->plan_un.scan.terms), &iter); t != -1; t = bitset_next_member (&iter))
    {
      /* pointer to QO_TERM denoted by number 't' */
      termp = QO_ENV_TERM (env, t);

      /* Find the matching segment in the segment index array to determine
         the array position to store the expression. We're using the
         'index_seg[]' array of the term to find its segment index */
      pos = -1;
      for (i = 0; i < termp->can_use_index && pos == -1; i++)
        {
          for (j = 0; j < nsegs; j++)
            {
              if (i >= sizeof (termp->index_seg) / sizeof (termp->index_seg[0]))
                {
                  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_FAILED_ASSERTION, 1, "false");
                  goto error;
                }

              if ((index_entryp->seg_idxs[j]) == QO_SEG_IDX (termp->index_seg[i]))
                {
                  pos = j;
                  break;
                }
            }
        }

      /* always, pos != -1 and 0 <= pos < nsegs */
      if (pos < 0)
        {
          er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_FAILED_ASSERTION, 1, "pos >= 0");
          goto error;
        }

      index_infop->term_exprs[pos] = QO_TERM_PT_EXPR (termp);
    }                           /* for (t = bitset_iterate(...); ...) */

  /* return QO_XASL_INDEX_INFO */
  return index_infop;

error:
  /* malloc error */
  qo_free_xasl_index_info (env, index_infop);
  return NULL;
}

/*
 * qo_free_xasl_index_info () -
 *   return: void
 *   env(in): The environment
 *   info(in): Information structure (QO_XASL_INDEX_INFO)
 *
 * Note: Free the memory occupied by the QO_XASL_INDEX_INFO
 */
static void
qo_free_xasl_index_info (UNUSED_ARG QO_ENV * env, QO_XASL_INDEX_INFO * info)
{
  if (info)
    {
      if (info->term_exprs)
        {
          free_and_init (info->term_exprs);
        }
      /*      DEALLOCATE (env, info->term_exprs); */

      free_and_init (info);
      /*    DEALLOCATE(env, info); */
    }
}

/*
 * qo_xasl_get_num_terms () - Return the number of terms in the array
 *   return: int
 *   info(in): Pointer to info structure
 */
int
qo_xasl_get_num_terms (QO_XASL_INDEX_INFO * info)
{
  return info->nterms;
}

/*
 * qo_xasl_get_terms () - Return a point to the NULL terminated list
 *			  of TERM expressions
 *   return: PT_NODE **
 *   info(in): Pointer to info structure
 */
PT_NODE **
qo_xasl_get_terms (QO_XASL_INDEX_INFO * info)
{
  return info->term_exprs;
}

/*
 * regu_ptr_list_free () - iterates over a linked list of regu var pointers
 *                         and frees each node (the containing node, NOT the
 *                         actual REGU_VARIABLE).
 *   list(in): the REGU_PTR_LIST. It can be NULL.
 *   return:
 */
static void
regu_ptr_list_free (REGU_PTR_LIST list)
{
  REGU_PTR_LIST next;

  while (list)
    {
      next = list->next;
      free_and_init (list);
      list = next;
    }
}

/*
 * regu_ptr_list_add_regu () - adds a pointer to a regu variable to the list,
 *                             initializing the list if required.
 *
 * regu(in): REGU_VAR ptr to add to the head of the list
 * list(in): the initial list. It can be NULL - in this case it will be initialised.
 * return: the list with the added element, or NULL on error.
 */
static REGU_PTR_LIST
regu_ptr_list_add_regu (REGU_VARIABLE * var_p, REGU_PTR_LIST list)
{
  REGU_PTR_LIST node;

  if (!var_p)
    {
      return list;
    }

  node = (REGU_PTR_LIST) malloc (sizeof (struct regu_ptr_list_node));
  if (!node)
    {
      regu_ptr_list_free (list);
      return NULL;
    }

  node->next = list;
  node->var_p = var_p;

  return node;
}

static bool
qo_validate_regu_var_for_limit (REGU_VARIABLE * var_p)
{
  if (var_p == NULL)
    {
      return true;
    }

  if (var_p->type == TYPE_DBVAL)
    {
      return true;
    }
  else if (var_p->type == TYPE_POS_VALUE)
    {
      return true;
    }
  else if (var_p->type == TYPE_INARITH && var_p->value.arithptr)
    {
      struct arith_list_node *aptr = var_p->value.arithptr;

      return (qo_validate_regu_var_for_limit (aptr->leftptr)
              && qo_validate_regu_var_for_limit (aptr->rightptr) && qo_validate_regu_var_for_limit (aptr->thirdptr));
    }

  return false;
}

/*
 * qo_get_limit_from_eval_term () - get lower and upper limits from an
 *                                  eval term involving instnum
 *   return:   true on success.
 *   parser(in):
 *   pred(in): the predicate expression.
 *   lower(out): lower limit node
 *   upper(out): upper limit node
 *
 *   Note: handles terms of the form:
 *         instnum rel_op value/hostvar
 *         value/hostvar rel_op instnum
 */
static bool
qo_get_limit_from_eval_term (PARSER_CONTEXT * parser, PRED_EXPR * pred, REGU_PTR_LIST * lower, REGU_PTR_LIST * upper)
{
  REGU_VARIABLE *lhs, *rhs;
  REL_OP op;
  PT_NODE *node_one = NULL;
  REGU_VARIABLE *regu_one, *regu_low;

  if (pred == NULL || pred->type != T_EVAL_TERM || pred->pe.eval_term.et_type != T_COMP_EVAL_TERM)
    {
      return false;
    }

  lhs = pred->pe.eval_term.et.et_comp.comp_lhs;
  rhs = pred->pe.eval_term.et.et_comp.comp_rhs;
  op = pred->pe.eval_term.et.et_comp.comp_rel_op;

  if (!lhs || !rhs)
    {
      return false;
    }
  if (op != R_LE && op != R_LT && op != R_GE && op != R_GT && op != R_EQ)
    {
      return false;
    }

  /* the TYPE_CONSTANT regu variable must be instnum, otherwise it would not
   * be accepted by the parser */

  /* switch the ops to transform into instnum rel_op value/hostvar */
  if (rhs->type == TYPE_CONSTANT)
    {
      rhs = pred->pe.eval_term.et.et_comp.comp_lhs;
      lhs = pred->pe.eval_term.et.et_comp.comp_rhs;
      switch (op)
        {
        case R_LE:
          op = R_GE;
          break;
        case R_LT:
          op = R_GT;
          break;
        case R_GE:
          op = R_LE;
          break;
        case R_GT:
          op = R_LT;
          break;
        default:
          break;
        }
    }

  if (lhs->type != TYPE_CONSTANT || !qo_validate_regu_var_for_limit (rhs))
    {
      return false;
    }

  /* Bring every accepted relation to a form similar to
   * lower < rownum <= upper.
   */
  switch (op)
    {
    case R_EQ:
      /* decrement node value for lower, but remember current value
       * for upper
       */
      node_one = pt_make_integer_value (parser, 1);
      if (!node_one)
        {
          return false;
        }

      if (!(regu_one = pt_to_regu_variable (parser, node_one, UNBOX_AS_VALUE))
          || !(regu_low = pt_make_regu_arith (rhs, regu_one, NULL, T_SUB)))
        {
          parser_free_node (parser, node_one);
          return false;
        }

      *lower = regu_ptr_list_add_regu (regu_low, *lower);
      *upper = regu_ptr_list_add_regu (rhs, *upper);
      break;

    case R_LE:
      *upper = regu_ptr_list_add_regu (rhs, *upper);
      break;

    case R_LT:
      /* decrement node value */
      node_one = pt_make_integer_value (parser, 1);
      if (!node_one)
        {
          return false;
        }

      if (!(regu_one = pt_to_regu_variable (parser, node_one, UNBOX_AS_VALUE))
          || !(regu_low = pt_make_regu_arith (rhs, regu_one, NULL, T_SUB)))
        {
          parser_free_node (parser, node_one);
          return false;
        }

      *upper = regu_ptr_list_add_regu (regu_low, *upper);
      break;

    case R_GE:
      /* decrement node value for lower */
      node_one = pt_make_integer_value (parser, 1);
      if (!node_one)
        {
          return false;
        }

      if (!(regu_one = pt_to_regu_variable (parser, node_one, UNBOX_AS_VALUE))
          || !(regu_low = pt_make_regu_arith (rhs, regu_one, NULL, T_SUB)))
        {
          parser_free_node (parser, node_one);
          return false;
        }

      *lower = regu_ptr_list_add_regu (regu_low, *lower);
      break;

    case R_GT:
      /* leave node value as it is */
      *lower = regu_ptr_list_add_regu (rhs, *lower);
      break;

    default:
      break;
    }

  if (node_one)
    {
      parser_free_node (parser, node_one);
    }

  return true;
}

/*
 * qo_get_limit_from_instnum_pred () - get lower and upper limits from an
 *                                     instnum predicate
 *   return: true if successful
 *   parser(in):
 *   pred(in): the predicate expression
 *   lower(out): lower limit node
 *   upper(out): upper limit node
 */
static bool
qo_get_limit_from_instnum_pred (PARSER_CONTEXT * parser, PRED_EXPR * pred, REGU_PTR_LIST * lower, REGU_PTR_LIST * upper)
{
  if (pred == NULL)
    {
      return false;
    }

  if (pred->type == T_PRED && pred->pe.pred.bool_op == B_AND)
    {
      return (qo_get_limit_from_instnum_pred (parser, pred->pe.pred.lhs,
                                              lower, upper)
              && qo_get_limit_from_instnum_pred (parser, pred->pe.pred.rhs, lower, upper));
    }

  if (pred->type == T_EVAL_TERM)
    {
      return qo_get_limit_from_eval_term (parser, pred, lower, upper);
    }

  return false;
}

/*
 * qo_get_key_limit_from_instnum () - creates a keylimit node from an
 *                                    instnum predicate, if possible.
 *   return:     a new node, or NULL if a keylimit node cannot be
 *               initialized (not necessarily an error)
 *   parser(in): the parser context
 *   plan (in):  the query plan
 *   xasl (in):  the full XASL node
 */
QO_LIMIT_INFO *
qo_get_key_limit_from_instnum (PARSER_CONTEXT * parser, QO_PLAN * plan, XASL_NODE * xasl)
{
  REGU_PTR_LIST lower = NULL, upper = NULL, ptr = NULL;
  QO_LIMIT_INFO *limit_infop = NULL;

  if (xasl == NULL || xasl->instnum_pred == NULL || plan == NULL)
    {
      return NULL;
    }

  switch (plan->plan_type)
    {
    case QO_PLANTYPE_SCAN:
      if (!qo_is_interesting_order_scan (plan))
        {
          return NULL;
        }
      break;

    case QO_PLANTYPE_JOIN:
      /* only allow inner joins */
      if (plan->plan_un.join.join_type != JOIN_INNER)
        {
          return NULL;
        }
      break;

    default:
      return NULL;
    }

  /* get lower and upper limits */
  if (!qo_get_limit_from_instnum_pred (parser, xasl->instnum_pred, &lower, &upper))
    {
      return NULL;
    }
  /* not having upper limit is not helpful */
  if (upper == NULL)
    {
      regu_ptr_list_free (lower);
      return NULL;
    }

  limit_infop = (QO_LIMIT_INFO *) malloc (sizeof (QO_LIMIT_INFO));
  if (limit_infop == NULL)
    {
      regu_ptr_list_free (lower);
      regu_ptr_list_free (upper);
      return NULL;
    }

  limit_infop->lower = limit_infop->upper = NULL;


  /* upper limit */
  limit_infop->upper = upper->var_p;
  ptr = upper->next;
  while (ptr)
    {
      limit_infop->upper = pt_make_regu_arith (limit_infop->upper, ptr->var_p, NULL, T_LEAST);
      if (!limit_infop->upper)
        {
          regu_ptr_list_free (upper);
          regu_ptr_list_free (lower);
          free_and_init (limit_infop);
          return NULL;
        }

      ptr = ptr->next;
    }
  regu_ptr_list_free (upper);

  if (lower)
    {
      limit_infop->lower = lower->var_p;
      ptr = lower->next;
      while (ptr)
        {
          limit_infop->lower = pt_make_regu_arith (limit_infop->lower, ptr->var_p, NULL, T_GREATEST);
          if (!limit_infop->lower)
            {
              regu_ptr_list_free (lower);
              free_and_init (limit_infop);
              return NULL;
            }

          ptr = ptr->next;
        }
      regu_ptr_list_free (lower);
    }

  return limit_infop;
}

/*
 * qo_get_key_limit_from_ordbynum () - creates a keylimit node from an
 *                                     orderby_num predicate, if possible.
 *   return:     a new node, or NULL if a keylimit node cannot be
 *               initialized (not necessarily an error)
 *   parser(in): the parser context
 *   plan (in):  the query plan
 *   xasl (in):  the full XASL node
 *   ignore_lower (in): generate key limit even if ordbynum has a lower limit
 */
QO_LIMIT_INFO *
qo_get_key_limit_from_ordbynum (PARSER_CONTEXT * parser, UNUSED_ARG QO_PLAN * plan, XASL_NODE * xasl, bool ignore_lower)
{
  REGU_PTR_LIST lower = NULL, upper = NULL, ptr = NULL;
  QO_LIMIT_INFO *limit_infop;

  if (xasl == NULL || xasl->ordbynum_pred == NULL)
    {
      return NULL;
    }

  /* get lower and upper limits */
  if (!qo_get_limit_from_instnum_pred (parser, xasl->ordbynum_pred, &lower, &upper))
    {
      return NULL;
    }
  /* having a lower limit, or not having upper limit is not helpful */
  if (upper == NULL || (lower != NULL && !ignore_lower))
    {
      regu_ptr_list_free (lower);
      regu_ptr_list_free (upper);
      return NULL;
    }

  limit_infop = (QO_LIMIT_INFO *) malloc (sizeof (QO_LIMIT_INFO));
  if (!limit_infop)
    {
      regu_ptr_list_free (lower);
      regu_ptr_list_free (upper);
      return NULL;
    }

  limit_infop->lower = limit_infop->upper = NULL;

  /* upper limit */
  limit_infop->upper = upper->var_p;
  ptr = upper->next;
  while (ptr)
    {
      limit_infop->upper = pt_make_regu_arith (limit_infop->upper, ptr->var_p, NULL, T_LEAST);
      if (!limit_infop->upper)
        {
          regu_ptr_list_free (upper);
          regu_ptr_list_free (lower);
          free_and_init (limit_infop);
          return NULL;
        }

      ptr = ptr->next;
    }

  regu_ptr_list_free (upper);
  regu_ptr_list_free (lower);

  return limit_infop;
}

/*
 * qo_check_iscan_for_multi_range_opt () - check that current index scan can
 *					   use multi range key-limit
 *					   optimization
 *
 * return	    : true/false
 * plan (in)	    : index scan plan
 *
 * Note: The optimization requires a series of conditions to be met:
 *	 For single table case:
 *	 - valid order by for condition
 *	    -> the upper limit has to be less than multi_range_opt_limit
 *	       system parameter
 *	    -> the expression should look like: LIMIT n,
						ORDERBY_NUM </<= n,
 *						n > ORDERBY_NUM
 *	       or AND operator on ORDERBY_NUM valid expressions.
 *	    -> lower limit is not allowed
 *	 - index scan with no data filter
 *	 - order by columns should occupy consecutive positions in index and
 *	   the ordering should match all columns (or all should be reversed)
 *	 - index access keys have multiple key ranges, but only one range
 *	   column
 *	 - The generic case that uses multi range optimization is the
 *	   following:
 *	    SELECT ... FROM table
 *		WHERE col_1 = ? AND col_2 = ? AND ...
 *		    AND col_(j) IN (?,?,...)
 *		    AND col_(j+1) = ? AND ... AND col_(p-1) = ?
 *		ORDER BY col_(p) [ASC/DESC] [, col_(p2) [ASC/DESC], ...]
 *		FOR ordbynum_pred / LIMIT n
 */
bool
qo_check_iscan_for_multi_range_opt (QO_PLAN * plan)
{
  QO_ENV *env = NULL;
  bool can_optimize = 0;
  PT_NODE *col = NULL, *query = NULL, *select_list = NULL;
  int error = NO_ERROR;
  bool multi_range_optimize = false;
  int first_col_idx_pos = -1, i = 0;
  PT_NODE *orderby_nodes = NULL, *point = NULL, *name = NULL;
  PARSER_CONTEXT *parser = NULL;
  bool reverse = false;
  PT_NODE *order_by = NULL;
  PT_MISC_TYPE all_distinct;


  if (plan == NULL)
    {
      return false;
    }

  if (!qo_is_iscan (plan))
    {
      return false;
    }

  env = plan->info->env;
  parser = env->parser;

  query = QO_ENV_PT_TREE (env);
  if (!PT_IS_SELECT (query))
    {
      return false;
    }
  if ((query->info.query.q.select.hint & PT_HINT_NO_MULTI_RANGE_OPT) != 0)
    {
      /* NO_MULTI_RANGE_OPT was hinted */
      return false;
    }
  all_distinct = query->info.query.all_distinct;
  order_by = query->info.query.order_by;

  if (order_by == NULL || all_distinct == PT_DISTINCT)
    {
      return false;
    }

  if (query->info.query.orderby_for == NULL)
    {
      return false;
    }

  select_list = pt_get_select_list (parser, query);
  if (select_list == NULL)
    {
      assert (false);
      return false;
    }

  /* create a list of pointers to the names referenced in order by */
  for (col = order_by; col != NULL; col = col->next)
    {
      i = col->info.sort_spec.pos_descr.pos_no;
      if (i <= 0)
        {
          goto exit;
        }
      name = select_list;
      while (--i > 0)
        {
          name = name->next;
          if (name == NULL)
            {
              goto exit;
            }
        }
      if (!PT_IS_NAME_NODE (name))
        {
          goto exit;
        }
      point = pt_point (parser, name);
      orderby_nodes = parser_append_node (point, orderby_nodes);
    }

  /* verify that the index used for scan contains all order by columns in the
   * right order and with the right ordering (or reversed ordering)
   */
  error =
    qo_check_plan_index_for_multi_range_opt (orderby_nodes, order_by,
                                             plan, &can_optimize, &first_col_idx_pos, &reverse);
  if (error != NO_ERROR || !can_optimize)
    {
      goto exit;
    }

  /* check scan terms and key filter terms to verify that multi range
   * optimization is applicable
   */
  error = qo_check_terms_for_multiple_range_opt (plan, first_col_idx_pos, &can_optimize);
  if (error != NO_ERROR || !can_optimize)
    {
      goto exit;
    }

  /* make sure that correlated subqueries may not affect the results obtained
   * with multiple range optimization
   */
  can_optimize = qo_check_subqueries_for_multi_range_opt (plan, first_col_idx_pos);
  if (!can_optimize)
    {
      goto exit;
    }

  /* check a valid range */
  if (!pt_check_ordby_num_for_multi_range_opt (parser, query, &env->multi_range_opt_candidate, NULL))
    {
      goto exit;
    }

  /* all conditions were met, so multi range optimization can be applied */
  multi_range_optimize = true;

  plan->plan_un.scan.index->head->use_descending = reverse;
  plan->plan_un.scan.index->head->first_sort_column = first_col_idx_pos;
  plan->use_iscan_descending = reverse;

exit:
  if (orderby_nodes != NULL)
    {
      parser_free_tree (parser, orderby_nodes);
    }
  return multi_range_optimize;
}

/*
 * qo_check_plan_index_for_multi_range_opt () - check if the index of index
 *						scan plan can use multi range
 *						key-limit optimization
 *
 * return		   : error code
 * orderby_nodes (in)	   : list of pointer to the names of order by columns
 * orderby_sort_list (in)  : list of PT_SORT_SPEC for the order by columns
 * plan (in)		   : current plan to check
 * is_valid (out)	   : true/false
 * first_col_idx_pos (out) : position in index for the first sort column
 * reverse (out)	   : true if the index has to be reversed in order to
 *			     use multiple range optimization, false otherwise
 *
 * NOTE: In order to be compatible with multi range optimization, the index of
 *	 index scan plan must meet the next conditions:
 *	 - index should cover all order by columns and their positions in
 *	   index should be consecutive (in the same order as in the order by
 *	   clause).
 *	 - column ordering should either match in both order by clause and
 *	   index, or should all be reversed.
 */
static int
qo_check_plan_index_for_multi_range_opt (PT_NODE * orderby_nodes,
                                         PT_NODE * orderby_sort_list,
                                         QO_PLAN * plan, bool * is_valid, int *first_col_idx_pos, bool * reverse)
{
  int i = 0, seg_idx = -1;
  QO_INDEX_ENTRY *index_entryp = NULL;
  QO_ENV *env = NULL;
  PT_NODE *orderby_node = NULL;
  PT_NODE *orderby_sort_column = NULL;
  PT_NODE *n = NULL, *save_next = NULL;

  assert (plan != NULL && orderby_nodes != NULL && is_valid != NULL && first_col_idx_pos != NULL && reverse != NULL);

  *is_valid = false;
  *reverse = false;
  *first_col_idx_pos = -1;

  if (!qo_is_iscan (plan))
    {
      return NO_ERROR;
    }
  if (plan->plan_un.scan.index == NULL || plan->plan_un.scan.index->head == NULL)
    {
      return NO_ERROR;
    }
  if (plan->info->env == NULL)
    {
      return NO_ERROR;
    }

  env = plan->info->env;
  index_entryp = plan->plan_un.scan.index->head;

  if (index_entryp->constraints == NULL
      || index_entryp->constraints->asc_desc == NULL || index_entryp->constraints->num_atts < index_entryp->nsegs)
    {
      assert (false);
      return NO_ERROR;          /* give up */
    }

  /* look for the first order by column */
  orderby_node = orderby_nodes;
  CAST_POINTER_TO_NODE (orderby_node);
  assert (orderby_node->node_type == PT_NAME);

  orderby_sort_column = orderby_sort_list;
  assert (orderby_sort_column->node_type == PT_SORT_SPEC);

  for (i = 0; i < index_entryp->nsegs; i++)
    {
      seg_idx = index_entryp->seg_idxs[i];
      if (seg_idx < 0)
        {
          continue;
        }
      n = QO_SEG_PT_NODE (QO_ENV_SEG (env, seg_idx));
      CAST_POINTER_TO_NODE (n);
      if (n && n->node_type == PT_NAME && pt_name_equal (env->parser, orderby_node, n))
        {
          if (i == 0)
            {
              /* MRO cannot apply */
              return NO_ERROR;
            }
          if (index_entryp->constraints->asc_desc[i] != (orderby_sort_column->info.sort_spec.asc_or_desc == PT_DESC))
            {
              /* order in index does not match order in order by clause,
               * but reversed index may work
               */
              *reverse = true;
            }
          break;
        }
    }

  if (i == index_entryp->nsegs)
    {
      /* order by node was not found */
      return NO_ERROR;
    }

  if (index_entryp->first_sort_column != -1)
    {
      assert (index_entryp->first_sort_column == i);
    }

  *first_col_idx_pos = i;
  /* order by node was found, check that all nodes occupy consecutive
   * positions in index
   */
  for (orderby_node = orderby_nodes->next, i = i + 1,
       orderby_sort_column = orderby_sort_list->next;
       orderby_node != NULL && orderby_sort_column != NULL
       && i < index_entryp->nsegs; i++, orderby_sort_column = orderby_sort_column->next)
    {
      if (i >= index_entryp->constraints->num_atts)
        {
          return NO_ERROR;
        }

      seg_idx = index_entryp->seg_idxs[i];
      if (seg_idx < 0)
        {
          return NO_ERROR;
        }

      save_next = orderby_node->next;
      CAST_POINTER_TO_NODE (orderby_node);
      n = QO_SEG_PT_NODE (QO_ENV_SEG (env, seg_idx));
      CAST_POINTER_TO_NODE (n);
      if (n == NULL || n->node_type != PT_NAME || !pt_name_equal (env->parser, orderby_node, n))
        {
          /* order by columns do not match the columns in index */
          return NO_ERROR;
        }
      if ((*reverse ? !index_entryp->constraints->asc_desc[i] : index_entryp->constraints->asc_desc[i]) !=
          (orderby_sort_column->info.sort_spec.asc_or_desc == PT_DESC))
        {
          return NO_ERROR;
        }
      orderby_node = save_next;
    }
  if (orderby_node != NULL)
    {
      /* there are order by columns left */
      return NO_ERROR;
    }
  /* all segments in index matched columns in order by list */
  *is_valid = true;

  return NO_ERROR;
}

/*
 * qo_check_plan_for_multiple_ranges_limit_opt () - check the plan to find out
 *                                                  if multiple ranges key
 *						    limit optimization can be
 *						    used
 *
 * return	     : error_code
 * parser(in)	     : parser context
 * plan(in)       : plan to check
 * idx_col(in)       : first sort column position in index
 * can_optimize(out) : true/false if optimization is allowed/not allowed
 *
 *   Note: Check that all columns that come before the first sort column (on
 *	   the left side of the sort column) in the index are either in an
 *	   equality term, or in a key list term. Only one column should be in
 *	   a key list term.
 *	   Also check all terms in the environment to see if there is any
 *	   data filter
 */
static int
qo_check_terms_for_multiple_range_opt (QO_PLAN * plan, int first_sort_col_idx, bool * can_optimize)
{
  int t, i, j, pos, s, seg_idx;
  BITSET_ITERATOR iter_t, iter_s;
  QO_TERM *termp = NULL;
  QO_ENV *env = NULL;
  QO_INDEX_ENTRY *index_entryp = NULL;
  QO_NODE *node_of_plan = NULL;
  int *used_cols = NULL;
  int kl_terms = 0;

  assert (can_optimize != NULL);

  *can_optimize = false;

  if (plan == NULL || plan->info == NULL || plan->plan_un.scan.index == NULL || !qo_is_interesting_order_scan (plan))
    {
      return NO_ERROR;
    }

  env = plan->info->env;
  if (env == NULL)
    {
      return NO_ERROR;
    }

  index_entryp = plan->plan_un.scan.index->head;
  if (index_entryp == NULL)
    {
      return NO_ERROR;
    }

  node_of_plan = plan->plan_un.scan.node;
  if (node_of_plan == NULL)
    {
      return NO_ERROR;
    }

  /* index columns that are used in terms */
  used_cols = (int *) malloc (first_sort_col_idx * sizeof (int));
  if (!used_cols)
    {
      return ER_FAILED;
    }
  for (i = 0; i < first_sort_col_idx; i++)
    {
      used_cols[i] = 0;
    }

  /* check all index scan terms */
  for (t = bitset_iterate (&(plan->plan_un.scan.terms), &iter_t); t != -1; t = bitset_next_member (&iter_t))
    {
      termp = QO_ENV_TERM (env, t);
      assert (!QO_TERM_IS_FLAGED (termp, QO_TERM_NON_IDX_SARG_COLL));

      pos = -1;
      for (i = 0; i < termp->can_use_index && i < 2 && pos == -1; i++)
        {
          for (j = 0; j < index_entryp->nsegs; j++)
            {
              if ((index_entryp->seg_idxs[j] == QO_SEG_IDX (termp->index_seg[i])))
                {
                  pos = j;
                  break;
                }
            }
        }
      if (pos == -1)
        {
          free_and_init (used_cols);
          return NO_ERROR;
        }

      if (pos < first_sort_col_idx)
        {
          used_cols[pos]++;
          /* only helpful if term is equality or key list */
          switch (QO_TERM_PT_EXPR (termp)->info.expr.op)
            {
            case PT_EQ:
              break;
            case PT_IS_IN:
              kl_terms++;
              break;
            case PT_RANGE:
              {
                PT_NODE *between_and;

                between_and = QO_TERM_PT_EXPR (termp)->info.expr.arg2;
                if (PT_IS_EXPR_NODE (between_and) && between_and->info.expr.op == PT_BETWEEN_EQ_NA)
                  {
                    kl_terms++;
                  }
                else
                  {
                    free_and_init (used_cols);
                    return NO_ERROR;
                  }
              }
              break;
            default:
              free_and_init (used_cols);
              return NO_ERROR;
            }
        }
    }                           /* for (t = bitset_iterate(...); ...) */

  /* check key list terms */
  if (kl_terms > 1)
    {
      free_and_init (used_cols);
      return NO_ERROR;
    }

  /* check all key filter terms */
  for (t = bitset_iterate (&(plan->plan_un.scan.kf_terms), &iter_t); t != -1; t = bitset_next_member (&iter_t))
    {
      termp = QO_ENV_TERM (env, t);
      assert (!QO_TERM_IS_FLAGED (termp, QO_TERM_NON_IDX_SARG_COLL));

      pos = -1;
      for (i = 0; i < termp->can_use_index && i < 2 && pos == -1; i++)
        {
          for (j = 0; j < index_entryp->nsegs; j++)
            {
              if ((index_entryp->seg_idxs[j] == QO_SEG_IDX (termp->index_seg[i])))
                {
                  pos = j;
                  break;
                }
            }
        }
      if (pos == -1)
        {
          if (termp->can_use_index == 0)
            {
              continue;
            }
          free_and_init (used_cols);
          return NO_ERROR;
        }

      if (pos < first_sort_col_idx)
        {
          /* for key filter terms we are only interested if it is an eq term */
          if (QO_TERM_PT_EXPR (termp)->info.expr.op == PT_EQ)
            {
              used_cols[pos]++;
            }
        }
    }                           /* for (t = bitset_iterate(...); ...) */

  /* check used columns */
  for (i = 0; i < first_sort_col_idx; i++)
    {
      if (used_cols[i] == 0)
        {
          free_and_init (used_cols);
          return NO_ERROR;
        }
    }
  free_and_init (used_cols);

  /* check all segments in all terms in environment for data filter */
  for (t = 0; t < env->nterms; t++)
    {
      termp = QO_ENV_TERM (env, t);
      if (QO_TERM_IS_FLAGED (termp, QO_TERM_NON_IDX_SARG_COLL))
        {
          return NO_ERROR;
        }

      for (s = bitset_iterate (&(termp->segments), &iter_s); s != -1; s = bitset_next_member (&iter_s))
        {
          bool found = false;
          if (QO_SEG_HEAD (QO_ENV_SEG (env, s)) != node_of_plan)
            {
              continue;
            }
          seg_idx = s;

          for (i = 0; i < index_entryp->nsegs; i++)
            {
              if (seg_idx == index_entryp->seg_idxs[i])
                {
                  found = true;
                  break;
                }
            }
          if (!found)
            {
              /* data filter */
              return NO_ERROR;
            }
        }
    }

  *can_optimize = true;
  return NO_ERROR;
}

/*
 * qo_check_subqueries_for_multi_range_opt () - check that there are not
 *						subqueries that may invalidate
 *						multiple range optimization
 *
 * return		 : false if invalidated, true otherwise
 * plan (in)		 : the plan that refers to the table that has the
 *			   order by columns
 * sort_col_idx_pos (in) : position in index for the first sort column
 *
 * NOTE:  If there are terms containing correlated subqueries, and if they
 *	  refer to the node of the sort plan, then the affected segments must
 *	  appear in index before the first sort column (and the segment must
 *	  not belong to the range term).
 */
static bool
qo_check_subqueries_for_multi_range_opt (QO_PLAN * plan, int sort_col_idx_pos)
{
  QO_ENV *env = NULL;
  QO_SUBQUERY *subq = NULL;
  int i, s, t, seg_idx, i_seg_idx, ts;
  QO_NODE *node_of_plan = NULL;
  BITSET_ITERATOR iter_t, iter_ts;
  QO_TERM *term = NULL;

  assert (plan != NULL && plan->info->env != NULL
          && plan->plan_type == QO_PLANTYPE_SCAN && plan->plan_un.scan.index != NULL);

  env = plan->info->env;
  node_of_plan = plan->plan_un.scan.node;

  /* for each sub-query */
  for (s = 0; s < env->nsubqueries; s++)
    {
      subq = QO_ENV_SUBQUERY (env, s);

      /* for each term this sub-query belongs to */
      for (t = bitset_iterate (&(subq->terms), &iter_t); t != -1; t = bitset_next_member (&iter_t))
        {
          term = QO_ENV_TERM (env, t);

          for (ts = bitset_iterate (&(term->segments), &iter_ts); ts != -1; ts = bitset_next_member (&iter_ts))
            {
              bool found = false;
              if (QO_SEG_HEAD (QO_ENV_SEG (env, t)) != node_of_plan)
                {
                  continue;
                }
              seg_idx = ts;
              /* try to find the segment in index */
              for (i = 0; i < sort_col_idx_pos; i++)
                {
                  i_seg_idx = plan->plan_un.scan.index->head->seg_idxs[i];
                  if (i_seg_idx == seg_idx)
                    {
                      if (qo_check_seg_belongs_to_range_term (plan, env, seg_idx))
                        {
                          return false;
                        }
                      break;
                    }
                }
              if (!found)
                {
                  /* the segment was not found before the first sort column */
                  return false;
                }
            }
        }
    }
  return true;
}

/*
 * qo_check_seg_belongs_to_range_term () - checks the segment if it is a range
 *					   term
 *
 * return	: true or false
 * subplan (in) : the subplan possibly containing the RANGE expression
 * env (in)	: optimizer environment
 * seg_idx (in) : index of the segment that needs checking
 *
 * NOTE:  Returns true if the specified subplan contains a term that
 *        references the given segment in a RANGE expression
 *        (t.i in (1,2,3) would be an example).
 *        Used in keylimit for multiple key ranges in joins optimization.
 *	  Scan terms, key filter terms and also sarged terms must all be
 *	  checked to cover all cases.
 */
static bool
qo_check_seg_belongs_to_range_term (QO_PLAN * subplan, QO_ENV * env, int seg_idx)
{
  int t, u;
  BITSET_ITERATOR iter, iter_s;

  assert (subplan->plan_type == QO_PLANTYPE_SCAN);
  if (subplan->plan_type != QO_PLANTYPE_SCAN)
    {
      return false;
    }

  for (t = bitset_iterate (&(subplan->plan_un.scan.terms), &iter); t != -1; t = bitset_next_member (&iter))
    {
      QO_TERM *termp = QO_ENV_TERM (env, t);
      BITSET *segs = &(QO_TERM_SEGS (termp));
      if (!segs)
        {
          continue;
        }
      for (u = bitset_iterate (segs, &iter_s); u != -1; u = bitset_next_member (&iter_s))
        {
          if (u == seg_idx)
            {
              PT_NODE *node = QO_TERM_PT_EXPR (termp);
              if (!node)
                {
                  continue;
                }

              switch (node->info.expr.op)
                {
                case PT_IS_IN:
                case PT_RANGE:
                  return true;
                default:
                  continue;
                }
            }
        }
    }
  for (t = bitset_iterate (&(subplan->plan_un.scan.kf_terms), &iter); t != -1; t = bitset_next_member (&iter))
    {
      QO_TERM *termp = QO_ENV_TERM (env, t);
      BITSET *segs = &(QO_TERM_SEGS (termp));
      if (!segs)
        {
          continue;
        }
      for (u = bitset_iterate (segs, &iter_s); u != -1; u = bitset_next_member (&iter_s))
        {
          if (u == seg_idx)
            {
              PT_NODE *node = QO_TERM_PT_EXPR (termp);
              if (!node)
                {
                  continue;
                }

              switch (node->info.expr.op)
                {
                case PT_IS_IN:
                case PT_RANGE:
                  return true;
                default:
                  continue;
                }
            }
        }
    }
  for (t = bitset_iterate (&(subplan->sarged_terms), &iter); t != -1; t = bitset_next_member (&iter))
    {
      QO_TERM *termp = QO_ENV_TERM (env, t);
      BITSET *segs = &(QO_TERM_SEGS (termp));
      if (!segs)
        {
          continue;
        }
      for (u = bitset_iterate (segs, &iter_s); u != -1; u = bitset_next_member (&iter_s))
        {
          if (u == seg_idx)
            {
              PT_NODE *node = QO_TERM_PT_EXPR (termp);
              if (!node)
                {
                  continue;
                }

              switch (node->info.expr.op)
                {
                case PT_IS_IN:
                case PT_RANGE:
                  return true;
                default:
                  continue;
                }
            }
        }
    }
  return false;
}

/*
 * qo_check_join_for_multi_range_opt () - check if join plan can make use of
 *					  multi range key-limit optimization
 *
 * return    : true/false
 * plan (in) : join plan
 *
 * NOTE:  The current join plan has to meet a series of conditions in order
 *	  to use the multi range optimization:
 *	  - Has at least an index scan subplan that can make use of multi
 *	  range optimization (as if there would be no joins)
 *	  - The sort plan (that uses multi range optimization) edges:
 *	    - Segments used to join other "outer-more" scans must also belong
 *	      to index (no data filter).
 *	    - Segments use to join other "inner-more" scans must belong to
 *	      index (no data filter), they must be positioned before the first
 *	      sorting column, and they cannot be in a range term (in order
 *	      to avoid filtering the results obtained after top n sorting).
 */
bool
qo_check_join_for_multi_range_opt (QO_PLAN * plan)
{
  QO_PLAN *sort_plan = NULL;
  int error = NO_ERROR;
  bool can_optimize = true;

  /* verify that this is a valid join for multi range optimization */
  if (plan == NULL || plan->plan_type != QO_PLANTYPE_JOIN || plan->plan_un.join.join_type != JOIN_INNER)
    {
      return false;
    }

  assert (plan->info->env && plan->info->env->pt_tree);
  if (!PT_IS_SELECT (plan->info->env->pt_tree))
    {
      return false;
    }
  if (((QO_ENV_PT_TREE (plan->info->env))->info.query.q.select.hint & PT_HINT_NO_MULTI_RANGE_OPT) != 0)
    {
      /* NO_MULTI_RANGE_OPT was hinted */
      return false;
    }

  /* first must find an index scan subplan that can apply multi range
   * optimization
   */
  error = qo_find_subplan_using_multi_range_opt (plan, &sort_plan);
  if (error != NO_ERROR || sort_plan == NULL)
    {
      /* error finding subplan or no subplan was found */
      return false;
    }

  /* check all join conditions */
  error = qo_check_subplans_for_multi_range_opt (NULL, plan, sort_plan, &can_optimize, NULL);
  if (error != NO_ERROR || !can_optimize)
    {
      return false;
    }

  /* all conditions are met, multi range optimization may be used */
  return true;
}

/*
 * qo_check_subplans_for_multi_range_opt () - verify that join conditions do
 *					      not invalidate the multi range
 *					      optimization
 *
 * return		 : error code
 * parent (in)		 : join node that contains sub-plans
 * plan (in)		 : current plan to verify
 * sortplan (in)	 : the plan that refers to the order by table
 * is_valid (out)	 : is_valid is true if optimization can be applied
 *			   otherwise it is set on false
 * sort_col_idx_pos (in) : position in index for the first sort column
 * seen (in/out)	 : flag to remember that the sort plan was passed.
 *			   all scan plans that are met after this flag was set
 *			   are potential suspect scans ("to the right" of the
 *			   order by table
 *
 * NOTE: 1. *seen should be false when the function is called for root plan or
 *	    it should be left as NULL.
 *	 2. checks all sub-plans at the right of sort plan in the join chain.
 *	    the sub-plans in the left can invalidate the optimization only if
 *	    the join term acts as data filter, which was already checked at a
 *	    previous step (see qo_check_terms_for_multiple_range_opt).
 *	 Check the comment on qo_check_join_for_multi_range_opt for more
 *	 details.
 */
static int
qo_check_subplans_for_multi_range_opt (QO_PLAN * parent, QO_PLAN * plan,
                                       QO_PLAN * sortplan, bool * is_valid, bool * seen)
{
  int error = NO_ERROR;
  bool dummy = false;

  if (seen == NULL)
    {
      seen = &dummy;
    }

  if (plan->plan_type == QO_PLANTYPE_SCAN)
    {
      if (*seen)
        {
          if (parent == NULL)
            {
              *is_valid = false;
              goto exit;
            }
          *is_valid = qo_check_subplan_join_cond_for_multi_range_opt (parent, plan, sortplan);
          return NO_ERROR;
        }
      if (plan == sortplan)
        {
          *seen = true;
        }
      *is_valid = true;
      return NO_ERROR;
    }
  else if (plan->plan_type == QO_PLANTYPE_JOIN)
    {
      if (qo_plan_multi_range_opt (plan))
        {
          /* plan->multi_range_opt_use == PLAN_MULTI_RANGE_OPT_USE */
          /* already checked and the plan can use multi range opt */
          *is_valid = true;
          /* sort plan is somewhere in the subtree of this plan */
          *seen = true;
          return NO_ERROR;
        }
      else if (plan->multi_range_opt_use == PLAN_MULTI_RANGE_OPT_CANNOT_USE)
        {
          /* already checked and the plan cannot use multi range opt */
          *is_valid = false;
          return NO_ERROR;
        }
      /* this must be the first time current plan is checked for multi range
       * optimization
       */
      error = qo_check_subplans_for_multi_range_opt (plan, plan->plan_un.join.outer, sortplan, is_valid, seen);
      if (error != NO_ERROR || !*is_valid)
        {
          /* mark the plan for future checks */
          plan->multi_range_opt_use = PLAN_MULTI_RANGE_OPT_CANNOT_USE;
          return error;
        }
      error = qo_check_subplans_for_multi_range_opt (plan, plan->plan_un.join.inner, sortplan, is_valid, seen);
      if (error != NO_ERROR || !*is_valid)
        {
          /* mark the plan for future checks */
          plan->multi_range_opt_use = PLAN_MULTI_RANGE_OPT_CANNOT_USE;
          return error;
        }
      return NO_ERROR;
    }

  /* a case we have not foreseen? Be conservative. */
  *is_valid = false;

exit:
  return error;
}

/*
 * qo_check_subplan_join_cond_for_multi_range_opt () - validate a given
 *						       subplan for multi range
 *						       key-limit optimization.
 *
 * return         : true if valid, false otherwise
 * parent (in)    : join plan that contains current subplan
 * subplan (in)   : the subplan that is verified at current step
 * sort_plan (in) : the subplan that refers to the table that has the order by
 *		    columns
 * is_outer (in)  : The position of sort plan relative to sub-plan in the join
 *		    chain. If true, sort plan must be position to the left in
 *		    the chain (is "outer-more"), otherwise it must be to the
 *		    right ("inner-more").
 *
 * NOTE:  The function checks the join conditions between sub-plan and
 *	  sort-plan. It is supposed that sort-plan is outer and sub-plan is
 *	  inner in this join.
 */
static bool
qo_check_subplan_join_cond_for_multi_range_opt (QO_PLAN * parent, QO_PLAN * subplan, QO_PLAN * sort_plan)
{
  QO_ENV *env = NULL;
  QO_NODE *node_of_sort_table = NULL, *node_of_subplan = NULL;
  BITSET join_terms;
  BITSET_ITERATOR iter_t, iter_n, iter_segs;
  QO_TERM *jt = NULL;
  QO_NODE *jn = NULL;
  int t, n, seg_idx, k, k_seg_idx;
  QO_NODE *seg_node = NULL;
  bool is_jterm_relevant;
  bool is_valid;

  if (sort_plan == NULL || sort_plan->info == NULL)
    {
      assert (false);
      return false;
    }

  env = sort_plan->info->env;

  QO_ASSERT (env, parent != NULL);
  QO_ASSERT (env, parent->plan_type == QO_PLANTYPE_JOIN);
  QO_ASSERT (env, subplan != NULL);
  QO_ASSERT (env, subplan->plan_type == QO_PLANTYPE_SCAN);
  QO_ASSERT (env, sort_plan != NULL);
  QO_ASSERT (env, sort_plan->plan_un.scan.node != NULL);
  QO_ASSERT (env, sort_plan->plan_un.scan.node->info != NULL);

  if (sort_plan->plan_un.scan.index == NULL)
    {
      return false;
    }

  node_of_sort_table = sort_plan->plan_un.scan.node;
  node_of_subplan = subplan->plan_un.scan.node;

  assert (node_of_sort_table != NULL);
  assert (node_of_subplan != NULL);

  /*
   * Scan all the parent's join terms: jt.
   *   If jt is a valid join-term (is a join between sub-plan and sort plan),
   *   the segment that belong to the sort plan must be positioned in index
   *   before the first sort column and it must not be in a range term.
   */

  bitset_init (&join_terms, env);

  bitset_union (&join_terms, &(parent->plan_un.join.join_terms));
  bitset_union (&join_terms, &(subplan->plan_un.scan.terms));

  for (t = bitset_iterate (&join_terms, &iter_t), is_valid = true;
       t != -1 && is_valid; t = bitset_next_member (&iter_t))
    {
      assert (is_valid == true);

      jt = QO_ENV_TERM (env, t);
      assert (jt != NULL);

      is_jterm_relevant = false;
      for (n = bitset_iterate (&(jt->nodes), &iter_n); n != -1; n = bitset_next_member (&iter_n))
        {
          jn = QO_ENV_NODE (env, n);

          assert (jn != NULL);

          if (jn == node_of_subplan)
            {
              is_jterm_relevant = true;
              break;
            }
        }

      if (!is_jterm_relevant)
        {
          continue;
        }

      for (n = bitset_iterate (&(jt->nodes), &iter_n); n != -1 && is_valid; n = bitset_next_member (&iter_n))
        {
          assert (is_valid == true);

          jn = QO_ENV_NODE (env, n);

          assert (jn != NULL);

          if (jn != node_of_sort_table)
            {
              continue;
            }

          /* there is a join term t that references the nodes used in sub-plan
           * and sort plan.
           */
          for (seg_idx = bitset_iterate (&(jt->segments), &iter_segs);
               seg_idx != -1 && is_valid; seg_idx = bitset_next_member (&iter_segs))
            {
              bool found = false;

              assert (is_valid == true);

              seg_node = QO_SEG_HEAD (QO_ENV_SEG (env, seg_idx));
              if (seg_node != node_of_sort_table)
                {
                  continue;
                }
              /* seg node refer to the order by table */
              for (k = 0; k < sort_plan->plan_un.scan.index->head->first_sort_column; k++)
                {
                  k_seg_idx = sort_plan->plan_un.scan.index->head->seg_idxs[k];
                  if (k_seg_idx == seg_idx)
                    {
                      /* seg_idx was found before the first sort column */
                      if (qo_check_seg_belongs_to_range_term (sort_plan, env, seg_idx))
                        {
                          ;     /* give up */
                        }
                      else
                        {
                          found = true;
                        }

                      break;
                    }
                }

              if (!found)
                {
                  /* seg_idx was not found before the first sort column */
                  is_valid = false;
                }
            }
        }
    }

  bitset_delset (&join_terms);

  return is_valid;
}

/*
 * qo_find_subplan_using_multi_range_opt () - finds an index scan plan that
 *					      may use multi range key-limit
 *					      optimization.
 *
 * return	  : error code
 * plan (in)	  : current node in plan tree
 * result (out)   : plan with multi range optimization
 *
 * NOTE : Leave result or join_idx NULL if they are not what you are looking
 *	  for.
 */
int
qo_find_subplan_using_multi_range_opt (QO_PLAN * plan, QO_PLAN ** result)
{
  int error = NO_ERROR;

  if (result != NULL)
    {
      *result = NULL;
    }

  if (plan == NULL)
    {
      return NO_ERROR;
    }

  if (plan->plan_type == QO_PLANTYPE_JOIN && plan->plan_un.join.join_type == JOIN_INNER)
    {
      error = qo_find_subplan_using_multi_range_opt (plan->plan_un.join.outer, result);
      if (error != NO_ERROR || (result != NULL && *result != NULL))
        {
          return NO_ERROR;
        }

      return qo_find_subplan_using_multi_range_opt (plan->plan_un.join.inner, result);
    }
  else if (qo_is_interesting_order_scan (plan))
    {
      if (qo_plan_multi_range_opt (plan))
        {
          if (result != NULL)
            {
              *result = plan;
            }
        }
      return NO_ERROR;
    }
  return NO_ERROR;
}

/*
 * make_sort_limit_proc () - make sort limit xasl node
 * return : xasl proc on success, NULL on error
 * env (in) : optimizer environment
 * plan (in) : query plan
 * namelist (in) : list of segments referenced by nodes in the plan
 * xasl (in) : top xasl
 */
static XASL_NODE *
make_sort_limit_proc (QO_ENV * env, QO_PLAN * plan, PT_NODE * namelist, XASL_NODE * xasl)
{
  PARSER_CONTEXT *parser;
  XASL_NODE *listfile = NULL;
  PT_NODE *new_order_by = NULL, *node_list = NULL;
  PT_NODE *order_by, *statement;

  parser = QO_ENV_PARSER (env);
  statement = QO_ENV_PT_TREE (env);
  order_by = statement->info.query.order_by;

  if (xasl->ordbynum_val == NULL)
    {
#if 1                           /* TODO - */
      assert (false);           /* is impossible */
#else
      /* If orderbynum_val is NULL, we're probably somewhere in a subplan
       * and orderbynum_val is set for the upper XASL level. Try to find
       * the ORDERBY_NUM node and use the node->etc pointer which is set to
       * the orderby_num val
       */

      if (statement->info.query.orderby_for == NULL)
        {
          /* we should not create a sort_limit proc without an orderby_for
           * predicate.
           */
          assert_release (false);
          listfile = NULL;
          goto cleanup;
        }

      parser_walk_tree (parser, statement->info.query.orderby_for,
                        pt_get_numbering_node_etc, &xasl->ordbynum_val, NULL, NULL);
#endif
      if (xasl->ordbynum_val == NULL)
        {
          assert_release (false);
          listfile = NULL;
          goto cleanup;
        }
    }
  /* make o copy of the namelist to extend it with expressions from the
   * ORDER BY clause. The extended list will be used to generate the internal
   * listfile scan but will not be used for the actual XASL node.
   */
  node_list = parser_copy_tree_list (parser, namelist);
  if (node_list == NULL)
    {
      listfile = NULL;
      goto cleanup;
    }

  /* set new SORT_SPEC list based on the position of items in the node_list */
  new_order_by = pt_set_orderby_for_sort_limit_plan (parser, statement, node_list);
  if (new_order_by == NULL)
    {
      listfile = NULL;
      goto cleanup;
    }

  statement->info.query.order_by = new_order_by;

  listfile = make_buildlist_proc (env, node_list);
  listfile = gen_outer (env, plan->plan_un.sort.subplan, &EMPTY_SET, NULL, NULL, listfile);
  listfile = add_sort_spec (env, listfile, plan, xasl->ordbynum_val, false);

cleanup:
  if (node_list != NULL)
    {
      parser_free_tree (parser, node_list);
    }
  if (new_order_by != NULL)
    {
      parser_free_tree (parser, new_order_by);
    }

  statement->info.query.order_by = order_by;

  return listfile;
}

/*
 * qo_get_orderby_num_upper_bound_node () - get the node which represents the
 *					    upper bound predicate of an
 *					    orderby_num predicate
 * return : node or NULL
 * parser (in)		: parser context
 * orderby_for (in)	: orderby_for predicate list
 * is_new_node (in/out) : if a new node was created, free_node is set to true
 *			  and caller must free the returned node
 *
 * Note: A NULL return indicates that this function either found no upper
 * bound or that it found several predicates which specify an upper bound
 * for the ORDERBY_NUM predicate.
 */
static PT_NODE *
qo_get_orderby_num_upper_bound_node (PARSER_CONTEXT * parser, PT_NODE * orderby_for, bool * is_new_node)
{
  PT_NODE *left = NULL, *right = NULL;
  PT_NODE *save_next;
  PT_OP_TYPE op;
  bool free_left = false, free_right = false;
  *is_new_node = false;

  if (orderby_for == NULL || !PT_IS_EXPR_NODE (orderby_for) || orderby_for->or_next != NULL)
    {
      /* orderby_for must be an expression containing only AND predicates */
      assert (false);
      return NULL;
    }

  /* Ranges for ORDERBY_NUM predicates have already been merged (see
   * qo_reduce_order_by). If the code below finds more than one upper bound,
   * this is an error.
   */
  if (orderby_for->next != NULL)
    {
      save_next = orderby_for->next;
      orderby_for->next = NULL;

      right = save_next;
      left = orderby_for;

      left = qo_get_orderby_num_upper_bound_node (parser, left, &free_left);
      right = qo_get_orderby_num_upper_bound_node (parser, right, &free_right);

      orderby_for->next = save_next;

      if (left != NULL)
        {
          if (right != NULL)
            {
              /* There should be exactly one upper bound */
              if (free_left)
                {
                  parser_free_tree (parser, left);
                }
              if (free_right)
                {
                  parser_free_tree (parser, right);
                }
              return NULL;
            }
          *is_new_node = free_left;
          return left;
        }
      else
        {
          /* If right is NULL, the orderby_num pred is invalid and we messed
           * something up somewhere. If it is not NULL, this is the node
           * we are looking for.
           */
          *is_new_node = free_right;
          return right;
        }
    }

  op = orderby_for->info.expr.op;
  /* look for orderby_num < argument */
  if (PT_IS_EXPR_NODE (orderby_for->info.expr.arg1) && orderby_for->info.expr.arg1->info.expr.op == PT_ORDERBY_NUM)
    {
      left = orderby_for->info.expr.arg1;
      right = orderby_for->info.expr.arg2;
    }
  else
    {
      left = orderby_for->info.expr.arg2;
      right = orderby_for->info.expr.arg1;
      if (!PT_IS_EXPR_NODE (left) || left->info.expr.op != PT_ORDERBY_NUM)
        {
          /* could not find ORDERBY_NUM argument */
          return NULL;
        }

      /* Verify operator. If LE, LT then reverse it. */
      switch (op)
        {
        case PT_LE:
          op = PT_GE;
          break;
        case PT_LT:
          op = PT_GT;
          break;
        case PT_GE:
          op = PT_LE;
          break;
        case PT_GT:
          op = PT_LT;
          break;
        default:
          break;
        }
    }

  if (op == PT_LE || op == PT_LT)
    {
      return orderby_for;
    }

  if (op == PT_BETWEEN)
    {
      /* construct new predicate for ORDERBY_NUM from BETWEEN expr. */
      PT_NODE *new_node;

      if (!PT_IS_EXPR_NODE (right) || right->info.expr.op != PT_BETWEEN_AND)
        {
          return NULL;
        }

      new_node = parser_new_node (parser, PT_EXPR);
      if (new_node == NULL)
        {
          return NULL;
        }
      new_node->info.expr.op = PT_LE;

      new_node->info.expr.arg1 = parser_copy_tree (parser, left);
      if (new_node->info.expr.arg1 == NULL)
        {
          parser_free_tree (parser, new_node);
          return NULL;
        }

      new_node->info.expr.arg2 = parser_copy_tree (parser, right->info.expr.arg2);
      if (new_node->info.expr.arg2 == NULL)
        {
          parser_free_tree (parser, new_node);
          return NULL;
        }

      *is_new_node = true;
      return new_node;
    }

  /* Any other comparison operator is unusable */
  return NULL;
}

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
 * xasl_generation.c - Generate XASL from the parse tree
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <assert.h>
#include <search.h>

#include "misc_string.h"
#include "error_manager.h"
#include "parser.h"
#include "query_executor.h"
#include "xasl_generation.h"
#include "xasl_support.h"
#include "db.h"
#include "environment_variable.h"
#include "parser.h"
#include "schema_manager.h"
#include "view_transform.h"
#include "locator_cl.h"
#include "optimizer.h"
#include "parser_message.h"
#include "set_object.h"
#include "object_print.h"
#include "object_representation.h"
#include "heap_file.h"
#include "intl_support.h"
#include "system_parameter.h"
#include "execute_schema.h"
#include "porting.h"
#include "list_file.h"
#include "execute_statement.h"
#include "query_graph.h"
#include "transform.h"
#include "query_planner.h"
#include "semantic_check.h"

/* this must be the last header file included!!! */
#include "dbval.h"

extern void qo_plan_lite_print (QO_PLAN * plan, FILE * f, int howfar);

typedef enum
{ SORT_LIST_AFTER_ISCAN = 1,
  SORT_LIST_ORDERBY,
  SORT_LIST_GROUPBY,
  SORT_LIST_AFTER_GROUPBY
} SORT_LIST_MODE;

typedef struct set_numbering_node_etc_info
{
  DB_VALUE **instnum_valp;
  DB_VALUE **ordbynum_valp;
} SET_NUMBERING_NODE_ETC_INFO;

static PRED_EXPR *pt_make_pred_term_not (PRED_EXPR * arg1);
static PRED_EXPR *pt_make_pred_term_comp (REGU_VARIABLE * arg1,
					  REGU_VARIABLE * arg2, REL_OP rop);
static PRED_EXPR *pt_make_pred_term_some_all (REGU_VARIABLE * arg1,
					      REGU_VARIABLE * arg2,
					      REL_OP rop, QL_FLAG some_all);
static PRED_EXPR *pt_make_pred_term_like (REGU_VARIABLE * arg1,
					  REGU_VARIABLE * arg2,
					  REGU_VARIABLE * arg3);
static PRED_EXPR *pt_make_pred_term_rlike (REGU_VARIABLE * arg1,
					   REGU_VARIABLE * arg2,
					   REGU_VARIABLE * case_sensitive);
static PRED_EXPR *pt_make_pred_term_is (PARSER_CONTEXT * parser,
					PT_NODE * arg1,
					PT_NODE * arg2, const BOOL_OP bop);
static PRED_EXPR *pt_to_pred_expr_local_with_arg (PARSER_CONTEXT * parser,
						  PT_NODE * node, int *argp);

#if defined (ENABLE_UNUSED_FUNCTION)
static int hhhhmmss (const DB_TIME * time, char *buf, int buflen);
static int hhmiss (const DB_TIME * time, char *buf, int buflen);
static int yyyymmdd (const DB_DATE * date, char *buf, int buflen);
static int yymmdd (const DB_DATE * date, char *buf, int buflen);
static int yyyymmddhhmissms (const DB_DATETIME * datetime, char *buf,
			     int buflen);
static int mmddyyyyhhmissms (const DB_DATETIME * datetime, char *buf,
			     int buflen);

static char *host_var_name (unsigned int custom_print);
#endif
static TABLE_INFO *pt_table_info_alloc (void);
static PT_NODE *pt_fillin_pseudo_spec (PARSER_CONTEXT * parser,
				       PT_NODE * spec);
static PT_NODE *pt_to_aggregate_node (PARSER_CONTEXT * parser, PT_NODE * tree,
				      void *arg, int *continue_walk);
static ACCESS_SPEC_TYPE *pt_make_access_spec (TARGET_TYPE spec_type,
					      ACCESS_METHOD access,
					      INDX_INFO * indexptr,
					      PRED_EXPR * where_key,
					      PRED_EXPR * where_pred);
static int pt_cnt_attrs (const REGU_VARIABLE_LIST attr_list);
static void pt_fill_in_attrid_array (REGU_VARIABLE_LIST attr_list,
				     ATTR_ID * attr_array, int *next_pos);
static SORT_LIST *pt_to_sort_list (PARSER_CONTEXT * parser,
				   PT_NODE * node_list, PT_NODE * col_list,
				   SORT_LIST_MODE sort_mode);

static int set_has_objs (DB_SET * seq);
static REGU_VARIABLE *pt_make_regu_hostvar (PARSER_CONTEXT * parser,
					    const PT_NODE * node);
static REGU_VARIABLE *pt_make_regu_constant (PARSER_CONTEXT * parser,
					     DB_VALUE * db_value,
					     const PT_NODE * node);
static REGU_VARIABLE *pt_make_regu_pred (PRED_EXPR * pred);
static REGU_VARIABLE *pt_make_function (PARSER_CONTEXT * parser,
					int function_code,
					const REGU_VARIABLE_LIST arg_list,
					const DB_TYPE result_type,
					const PT_NODE * node);
static REGU_VARIABLE *pt_function_to_regu (PARSER_CONTEXT * parser,
					   PT_NODE * function);
static REGU_VARIABLE *pt_make_regu_subquery (PARSER_CONTEXT * parser,
					     XASL_NODE * xasl,
					     const UNBOX unbox,
					     const PT_NODE * node);
#if defined (ENABLE_UNUSED_FUNCTION)
static REGU_VARIABLE *pt_make_regu_insert (PARSER_CONTEXT * parser,
					   PT_NODE * statement);
#endif
static PT_NODE *pt_set_numbering_node_etc_pre (PARSER_CONTEXT * parser,
					       PT_NODE * node, void *arg,
					       int *continue_walk);
static REGU_VARIABLE *pt_make_regu_numbering (PARSER_CONTEXT * parser,
					      const PT_NODE * node);
static void pt_to_misc_operand (REGU_VARIABLE * regu,
				PT_MISC_TYPE misc_specifier);
#if defined (ENABLE_UNUSED_FUNCTION)
static REGU_VARIABLE *pt_make_position_regu_variable (PARSER_CONTEXT * parser,
						      const PT_NODE * node,
						      int i);
#endif
static REGU_VARIABLE *pt_to_regu_attr_descr (PARSER_CONTEXT * parser,
					     DB_OBJECT * class_object,
					     HEAP_CACHE_ATTRINFO *
					     cache_attrinfo, PT_NODE * attr);
static PT_NODE *pt_make_empty_string (PARSER_CONTEXT * parser,
				      PT_TYPE_ENUM e);
static XASL_NODE *pt_find_oid_scan_block (XASL_NODE * xasl, OID * oid);
static PT_NODE *pt_numbering_set_continue_post (PARSER_CONTEXT * parser,
						PT_NODE * node, void *arg,
						int *continue_walk);
static PT_NODE *pt_append_assignment_references (PARSER_CONTEXT * parser,
						 PT_NODE * assignments,
						 PT_NODE * from,
						 PT_NODE * select_list);
static ODKU_INFO *pt_to_odku_info (PARSER_CONTEXT * parser, PT_NODE * insert,
				   XASL_NODE * xasl);

#define APPEND_TO_XASL(xasl_head, list, xasl_tail)                      \
    if (xasl_head) {                                                    \
        /* append xasl_tail to end of linked list denoted by list */    \
        XASL_NODE **NAME2(list,ptr) = &xasl_head->list;                 \
        while ( (*NAME2(list,ptr)) ) {                                  \
            NAME2(list,ptr) = &(*NAME2(list,ptr))->list;                \
        }                                                               \
        (*NAME2(list,ptr)) = xasl_tail;                                 \
    } else {                                                            \
        xasl_head = xasl_tail;                                          \
    }

#define VALIDATE_REGU_KEY(r) ((r)->type == TYPE_CONSTANT  || \
                              (r)->type == TYPE_DBVAL     || \
                              (r)->type == TYPE_POS_VALUE || \
                              (r)->type == TYPE_INARITH)

typedef struct xasl_supp_info
{
  PT_NODE *query_list;		/* ??? */

  /* XASL cache related information */
  OID *class_oid_list;		/* list of class OIDs referenced
				 * in the XASL */
  int *tcard_list;		/* list of #pages of the class OIDs */
  int n_oid_list;		/* number OIDs in the list */
  int oid_list_size;		/* size of the list */
} XASL_SUPP_INFO;

typedef struct uncorr_info
{
  XASL_NODE *xasl;
  int level;
} UNCORR_INFO;

typedef struct corr_info
{
  XASL_NODE *xasl_head;
  UINTPTR id;
} CORR_INFO;

FILE *query_Plan_dump_fp = NULL;
char *query_Plan_dump_filename = NULL;

static XASL_SUPP_INFO xasl_Supp_info = { NULL, NULL, NULL, 0, 0 };

static const int OID_LIST_GROWTH = 10;


static RANGE op_type_to_range (const PT_OP_TYPE op_type, const int nterms);
#if defined (ENABLE_UNUSED_FUNCTION)
static int pt_to_single_key (PARSER_CONTEXT * parser, PT_NODE ** term_exprs,
			     int nterms, KEY_INFO * key_infop);
#endif
static int pt_to_list_key (PARSER_CONTEXT * parser, PT_NODE ** term_exprs,
			   int nterms, KEY_INFO * key_infop);
static int pt_to_rangelist_key (PARSER_CONTEXT * parser,
				PT_NODE ** term_exprs, int nterms,
				KEY_INFO * key_infop, int rangelist_idx);
static int pt_to_key_limit (PARSER_CONTEXT * parser, PT_NODE * key_limit,
			    QO_LIMIT_INFO * limit_infop, KEY_INFO * key_infop,
			    bool key_limit_reset);
static int pt_instnum_to_key_limit (PARSER_CONTEXT * parser, QO_PLAN * plan,
				    XASL_NODE * xasl);
static int pt_ordbynum_to_key_limit_multiple_ranges (PARSER_CONTEXT * parser,
						     QO_PLAN * plan,
						     XASL_NODE * xasl);
static INDX_INFO *pt_to_index_info (PARSER_CONTEXT * parser,
				    DB_OBJECT * class_,
				    PRED_EXPR * where_pred,
				    QO_PLAN * plan,
				    QO_XASL_INDEX_INFO * qo_index_infop);
static ACCESS_SPEC_TYPE *pt_to_class_spec_list (PARSER_CONTEXT * parser,
						PT_NODE * spec,
						PT_NODE * where_key_part,
						PT_NODE * where_part,
						PT_NODE * pk_next_part,
						QO_PLAN * plan,
						QO_XASL_INDEX_INFO *
						index_pred);
static ACCESS_SPEC_TYPE *pt_to_subquery_table_spec_list (PARSER_CONTEXT *
							 parser,
							 PT_NODE * spec,
							 PT_NODE * subquery,
							 PT_NODE *
							 where_part);
static XASL_NODE *pt_find_xasl (XASL_NODE * list, XASL_NODE * match);
static void pt_set_aptr (PARSER_CONTEXT * parser, PT_NODE * select_node,
			 XASL_NODE * xasl);
static PT_NODE *pt_uncorr_pre (PARSER_CONTEXT * parser, PT_NODE * node,
			       void *arg, int *continue_walk);
static PT_NODE *pt_uncorr_post (PARSER_CONTEXT * parser, PT_NODE * node,
				void *arg, int *continue_walk);
static XASL_NODE *pt_to_uncorr_subquery_list (PARSER_CONTEXT * parser,
					      PT_NODE * node);
static PT_NODE *pt_corr_pre (PARSER_CONTEXT * parser, PT_NODE * node,
			     void *arg, int *continue_walk);
static XASL_NODE *pt_to_corr_subquery_list (PARSER_CONTEXT * parser,
					    PT_NODE * node, UINTPTR id);
static OUTPTR_LIST *pt_to_outlist (PARSER_CONTEXT * parser,
				   PT_NODE * node_list, UNBOX unbox);
static XASL_NODE *pt_gen_optimized_plan (PARSER_CONTEXT * parser,
					 PT_NODE * select_node,
					 QO_PLAN * plan, XASL_NODE * xasl);
static XASL_NODE *pt_to_buildlist_proc (PARSER_CONTEXT * parser,
					PT_NODE * select_node,
					QO_PLAN * qo_plan);
static XASL_NODE *pt_to_buildvalue_proc (PARSER_CONTEXT * parser,
					 PT_NODE * select_node,
					 QO_PLAN * qo_plan);
static XASL_NODE *pt_to_union_proc (PARSER_CONTEXT * parser, PT_NODE * node,
				    PROC_TYPE type);
static XASL_NODE *pt_plan_set_query (PARSER_CONTEXT * parser, PT_NODE * node,
				     PROC_TYPE proc_type);
static XASL_NODE *pt_plan_query (PARSER_CONTEXT * parser,
				 PT_NODE * select_node);
static XASL_NODE *parser_generate_xasl_proc (PARSER_CONTEXT * parser,
					     PT_NODE * node,
					     PT_NODE * query_list);
static PT_NODE *parser_generate_xasl_pre (PARSER_CONTEXT * parser,
					  PT_NODE * node, void *arg,
					  int *continue_walk);
static int pt_spec_to_xasl_class_oid_list (PARSER_CONTEXT * parser,
					   const PT_NODE * spec,
					   OID ** oid_listp,
					   int **tcard_listp, int *nump,
					   int *sizep);
static PT_NODE *parser_generate_xasl_post (PARSER_CONTEXT * parser,
					   PT_NODE * node, void *arg,
					   int *continue_walk);
static XASL_NODE *pt_make_aptr_parent_node (PARSER_CONTEXT * parser,
					    PT_NODE * node, PROC_TYPE type);
static int pt_to_constraint_pred (PARSER_CONTEXT * parser, XASL_NODE * xasl,
				  PT_NODE * spec, PT_NODE * non_null_attrs,
				  PT_NODE * attr_list, int attr_offset);

static REGU_VARIABLE_LIST pt_to_regu_variable_list (PARSER_CONTEXT * p,
						    PT_NODE * node,
						    UNBOX unbox,
						    VAL_LIST * value_list,
						    int *attr_offsets);

static REGU_VARIABLE *pt_attribute_to_regu (PARSER_CONTEXT * parser,
					    PT_NODE * attr);

static TP_DOMAIN *pt_xasl_data_type_to_domain (PARSER_CONTEXT * parser,
					       PT_NODE * node);
static DB_VALUE *pt_index_value (const VAL_LIST * value, int index);

static PT_NODE *pt_query_set_reference (PARSER_CONTEXT * parser,
					PT_NODE * node);

static REGU_VARIABLE_LIST
pt_to_position_regu_variable_list (PARSER_CONTEXT * parser,
				   PT_NODE * node_list, VAL_LIST * value_list,
				   int *attr_offsets);

static DB_VALUE *pt_regu_to_dbvalue (PARSER_CONTEXT * parser,
				     REGU_VARIABLE * regu);

#if defined (ENABLE_UNUSED_FUNCTION)
static int look_for_unique_btid (DB_OBJECT * classop, const char *name,
				 BTID * btid);
#endif

static void pt_split_having_grbynum (PARSER_CONTEXT * parser,
				     PT_NODE * having, PT_NODE ** having_part,
				     PT_NODE ** grbynum_part);

static int pt_split_attrs (PARSER_CONTEXT * parser, TABLE_INFO * table_info,
			   PT_NODE * pred, PT_NODE ** pred_attrs,
			   PT_NODE ** rest_attrs, int **pred_offsets,
			   int **rest_offsets);

static int pt_to_index_attrs (PARSER_CONTEXT * parser,
			      TABLE_INFO * table_info,
			      QO_XASL_INDEX_INFO * index_pred, PT_NODE * pred,
			      PT_NODE ** pred_attrs, int **pred_offsets);

static PT_NODE *pt_flush_class_and_null_xasl (PARSER_CONTEXT *
					      parser,
					      PT_NODE * tree,
					      void *void_arg,
					      int *continue_walk);


static VAL_LIST *pt_clone_val_list (PARSER_CONTEXT * parser,
				    PT_NODE * attribute_list);

static AGGREGATE_TYPE *pt_to_aggregate (PARSER_CONTEXT * parser,
					PT_NODE * select_node,
					OUTPTR_LIST * out_list,
					VAL_LIST * value_list,
					REGU_VARIABLE_LIST regu_list,
					PT_NODE * out_names,
					DB_VALUE ** grbynum_valp);

static SYMBOL_INFO *pt_push_symbol_info (PARSER_CONTEXT * parser,
					 PT_NODE * select_node);

static void pt_pop_symbol_info (PARSER_CONTEXT * parser);

static ACCESS_SPEC_TYPE *pt_make_class_access_spec (PARSER_CONTEXT * parser,
						    PT_NODE * flat,
						    DB_OBJECT * class_,
						    TARGET_TYPE scan_type,
						    ACCESS_METHOD access,
						    INDX_INFO * indexptr,
						    PRED_EXPR * where_key,
						    PRED_EXPR * where_pred,
						    REGU_VARIABLE_LIST
						    attr_list_key,
						    REGU_VARIABLE_LIST
						    attr_list_pred,
						    REGU_VARIABLE_LIST
						    attr_list_rest,
						    REGU_VARIABLE_LIST
						    pk_next,
						    OUTPTR_LIST *
						    output_val_list,
						    REGU_VARIABLE_LIST
						    regu_val_list,
						    HEAP_CACHE_ATTRINFO *
						    cache_key,
						    HEAP_CACHE_ATTRINFO *
						    cache_pred,
						    HEAP_CACHE_ATTRINFO *
						    cache_rest);

static ACCESS_SPEC_TYPE *pt_make_list_access_spec (XASL_NODE * xasl,
						   ACCESS_METHOD access,
						   INDX_INFO * indexptr,
						   PRED_EXPR * where_pred,
						   REGU_VARIABLE_LIST
						   attr_list_pred,
						   REGU_VARIABLE_LIST
						   attr_list_rest);

static SORT_LIST *pt_to_after_iscan (PARSER_CONTEXT * parser,
				     PT_NODE * iscan_list, PT_NODE * root);

static SORT_LIST *pt_to_groupby (PARSER_CONTEXT * parser,
				 PT_NODE * group_list, PT_NODE * root);

static SORT_LIST *pt_to_after_groupby (PARSER_CONTEXT * parser,
				       PT_NODE * group_list, PT_NODE * query);

static TABLE_INFO *pt_find_table_info (UINTPTR spec_id,
				       TABLE_INFO * exposed_list);

static int pt_is_subquery (PT_NODE * node);

static int *pt_make_identity_offsets (PT_NODE * attr_list);

static VAL_LIST *pt_make_val_list (PARSER_CONTEXT * parser,
				   const PT_NODE * attribute_list);

static TABLE_INFO *pt_make_table_info (PARSER_CONTEXT * parser,
				       PT_NODE * table_spec);

static SYMBOL_INFO *pt_symbol_info_alloc (void);

static PRED_EXPR *pt_make_pred_expr_pred (PRED_EXPR * arg1,
					  PRED_EXPR * arg2, BOOL_OP bop);

static OUTPTR_LIST *pt_make_outlist_from_vallist (PARSER_CONTEXT * parser,
						  VAL_LIST * val_list_p);

static void pt_init_precision_and_scale (DB_VALUE * value,
					 const PT_NODE * node);

static SORT_LIST *pt_agg_orderby_to_sort_list (PARSER_CONTEXT * parser,
					       PT_NODE * order_list,
					       PT_NODE * agg_args_list);
static int pt_set_limit_optimization_flags (PARSER_CONTEXT * parser,
					    QO_PLAN * plan, XASL_NODE * xasl);

static void
pt_init_xasl_supp_info ()
{
  /* XASL cache related information */
  if (xasl_Supp_info.class_oid_list)
    {
      free_and_init (xasl_Supp_info.class_oid_list);
    }

  if (xasl_Supp_info.tcard_list)
    {
      free_and_init (xasl_Supp_info.tcard_list);
    }

  xasl_Supp_info.n_oid_list = xasl_Supp_info.oid_list_size = 0;
}

/*
 * pt_make_pred_expr_pred () - makes a pred expr logical node (AND/OR)
 *   return:
 *   arg1(in):
 *   arg2(in):
 *   bop(in):
 */
static PRED_EXPR *
pt_make_pred_expr_pred (PRED_EXPR * arg1, PRED_EXPR * arg2, BOOL_OP bop)
{
  PRED_EXPR *pred = NULL;

  if (arg1 != NULL && arg2 != NULL)
    {
      pred = regu_pred_alloc ();

      if (pred)
	{
	  pred->type = T_PRED;
	  pred->pe.pred.lhs = arg1;
	  pred->pe.pred.rhs = arg2;
	  pred->pe.pred.bool_op = bop;
	}
    }

  return pred;
}

/*
 * pt_make_pred_term_not () - makes a pred expr one argument term (NOT)
 *   return:
 *   arg1(in):
 *
 * Note :
 * This can make a predicate term for an indirect term
 */
static PRED_EXPR *
pt_make_pred_term_not (PRED_EXPR * arg1)
{
  PRED_EXPR *pred = NULL;

  if (arg1 != NULL)
    {
      pred = regu_pred_alloc ();

      if (pred)
	{
	  pred->type = T_NOT_TERM;
	  pred->pe.not_term = arg1;
	}
    }

  return pred;
}


/*
 * pt_make_pred_term_comp () - makes a pred expr term comparison node
 *   return:
 *   arg1(in):
 *   arg2(in):
 *   rop(in):
 */
static PRED_EXPR *
pt_make_pred_term_comp (REGU_VARIABLE * arg1,
			REGU_VARIABLE * arg2, REL_OP rop)
{
  PRED_EXPR *pred = NULL;

  if (arg1 != NULL && (arg2 != NULL || rop == R_EXISTS || rop == R_NULL))
    {
#if 1				/* TODO - trace */
      if (arg1 != NULL && arg1->type == TYPE_LIST_ID)
	{
	  if (rop != R_EXISTS)
	    {
	      assert (false);	/* is impossible */
	      return NULL;
	    }
	}

      if (arg2 != NULL && arg2->type == TYPE_LIST_ID)
	{
	  assert (false);	/* is impossible */
	  return NULL;
	}
#endif

      pred = regu_pred_alloc ();

      if (pred)
	{
	  COMP_EVAL_TERM *et_comp = &pred->pe.eval_term.et.et_comp;

	  pred->type = T_EVAL_TERM;
	  pred->pe.eval_term.et_type = T_COMP_EVAL_TERM;
	  et_comp->comp_lhs = arg1;
	  et_comp->comp_rhs = arg2;
	  et_comp->comp_rel_op = rop;
	}
    }

  return pred;
}

/*
 * pt_make_pred_term_some_all () - makes a pred expr term some/all
 * 				   comparison node
 *   return:
 *   arg1(in):
 *   arg2(in):
 *   rop(in):
 *   some_all(in):
 */
static PRED_EXPR *
pt_make_pred_term_some_all (REGU_VARIABLE * arg1,
			    REGU_VARIABLE * arg2, REL_OP rop,
			    QL_FLAG some_all)
{
  PRED_EXPR *pred = NULL;

  if (arg1 != NULL && arg2 != NULL)
    {
      pred = regu_pred_alloc ();

      if (pred)
	{
	  ALSM_EVAL_TERM *et_alsm = &pred->pe.eval_term.et.et_alsm;

	  pred->type = T_EVAL_TERM;
	  pred->pe.eval_term.et_type = T_ALSM_EVAL_TERM;
	  et_alsm->elem = arg1;
	  et_alsm->elemset = arg2;
	  et_alsm->alsm_rel_op = rop;
	  et_alsm->eq_flag = some_all;

	  assert (et_alsm->alsm_rel_op == R_EQ);
	  assert (et_alsm->eq_flag == F_SOME);
	}
    }

  return pred;
}

/*
 * pt_make_pred_term_like () - makes a pred expr term like comparison node
 *   return:
 *   arg1(in):
 *   arg2(in):
 *   esc(in):
 */
static PRED_EXPR *
pt_make_pred_term_like (REGU_VARIABLE * arg1,
			REGU_VARIABLE * arg2, REGU_VARIABLE * arg3)
{
  PRED_EXPR *pred = NULL;

  if (arg1 != NULL && arg2 != NULL)
    {
      pred = regu_pred_alloc ();

      if (pred)
	{
	  LIKE_EVAL_TERM *et_like = &pred->pe.eval_term.et.et_like;

	  pred->type = T_EVAL_TERM;
	  pred->pe.eval_term.et_type = T_LIKE_EVAL_TERM;
	  et_like->src = arg1;
	  et_like->pattern = arg2;
	  et_like->esc_char = arg3;
	}
    }

  return pred;
}

/*
 * pt_make_pred_term_rlike () - makes a pred expr term of regex comparison node
 *   return: predicate expression
 *   arg1(in): source string regu var
 *   arg2(in): pattern regu var
 *   case_sensitive(in): sensitivity flag regu var
 */
static PRED_EXPR *
pt_make_pred_term_rlike (REGU_VARIABLE * arg1, REGU_VARIABLE * arg2,
			 REGU_VARIABLE * case_sensitive)
{
  PRED_EXPR *pred = NULL;
  RLIKE_EVAL_TERM *et_rlike = NULL;

  if (arg1 == NULL || arg2 == NULL || case_sensitive == NULL)
    {
      return NULL;
    }

  pred = regu_pred_alloc ();
  if (pred == NULL)
    {
      return NULL;
    }

  et_rlike = &pred->pe.eval_term.et.et_rlike;
  pred->type = T_EVAL_TERM;
  pred->pe.eval_term.et_type = T_RLIKE_EVAL_TERM;
  et_rlike->src = arg1;
  et_rlike->pattern = arg2;
  et_rlike->case_sensitive = case_sensitive;
  et_rlike->compiled_regex = NULL;
  et_rlike->compiled_pattern = NULL;

  return pred;
}

/*
 * pt_make_pred_term_is () - makes a pred expr term for IS/IS NOT
 *     return:
 *   parser(in):
 *   arg1(in):
 *   arg2(in):
 *   op(in):
 *
 */
static PRED_EXPR *
pt_make_pred_term_is (PARSER_CONTEXT * parser, PT_NODE * arg1, PT_NODE * arg2,
		      const BOOL_OP bop)
{
  PT_NODE *dummy1, *dummy2;
  PRED_EXPR *pred_rhs, *pred = NULL;

  if (arg1 == NULL || arg2 == NULL)
    {
      return NULL;
    }

  dummy1 = parser_new_node (parser, PT_VALUE);
  dummy2 = parser_new_node (parser, PT_VALUE);

  if (dummy1 && dummy2)
    {
      dummy2->type_enum = PT_TYPE_INTEGER;
      dummy2->info.value.data_value.i = 1;

      if (arg2->type_enum == PT_TYPE_LOGICAL)
	{
	  /* term for TRUE/FALSE */
	  dummy1->type_enum = PT_TYPE_INTEGER;
	  dummy1->info.value.data_value.i = arg2->info.value.data_value.i;
	}
      else
	{
	  /* term for UNKNOWN */
	  dummy1->type_enum = PT_TYPE_NONE;
	}

      /* make a R_EQ pred term for rhs boolean val */
      pred_rhs =
	pt_make_pred_term_comp (pt_to_regu_variable (parser, dummy1,
						     UNBOX_AS_VALUE),
				pt_to_regu_variable (parser, dummy2,
						     UNBOX_AS_VALUE), R_EQ);

      pred = pt_make_pred_expr_pred (pt_to_pred_expr (parser, arg1),
				     pred_rhs, bop);
    }
  else
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
    }

  return pred;
}


/*
 * pt_to_pred_expr_local_with_arg () - converts a parse expression tree
 * 				       to pred expressions
 *   return: A NULL return indicates an error occurred
 *   parser(in):
 *   node(in): should be something that will evaluate into a boolean
 *   argp(out):
 */
static PRED_EXPR *
pt_to_pred_expr_local_with_arg (PARSER_CONTEXT * parser, PT_NODE * node,
				int *argp)
{
  PRED_EXPR *pred = NULL;
  void *saved_etc;
  int dummy;
  PT_NODE *save_node;
  REGU_VARIABLE *regu_var1 = NULL, *regu_var2 = NULL, *regu_var3 = NULL;

  if (!argp)
    {
      argp = &dummy;
    }

  if (node)
    {
      save_node = node;

      CAST_POINTER_TO_NODE (node);

      if (node->node_type == PT_EXPR)
	{
	  /* to get information for inst_num() scan typr from
	     pt_to_regu_variable(), borrow 'parser->etc' field */
	  saved_etc = parser->etc;
	  parser->etc = NULL;

	  /* set regu variables */
	  if (node->info.expr.op == PT_EQ
	      || node->info.expr.op == PT_NE || node->info.expr.op == PT_GE
	      || node->info.expr.op == PT_GT || node->info.expr.op == PT_LT
	      || node->info.expr.op == PT_LE
	      || node->info.expr.op == PT_NULLSAFE_EQ)
	    {
	      regu_var1 = pt_to_regu_variable (parser,
					       node->info.expr.arg1,
					       UNBOX_AS_VALUE);
	      regu_var2 = pt_to_regu_variable (parser,
					       node->info.expr.arg2,
					       UNBOX_AS_VALUE);
	    }
	  else if (node->info.expr.op == PT_IS_NOT_IN
		   || node->info.expr.op == PT_IS_IN)
	    {
	      regu_var1 = pt_to_regu_variable (parser, node->info.expr.arg1,
					       UNBOX_AS_VALUE);
	      regu_var2 = pt_to_regu_variable (parser, node->info.expr.arg2,
					       UNBOX_AS_TABLE);
	    }

	  switch (node->info.expr.op)
	    {
	      /* Logical operators */
	    case PT_AND:
	      pred = pt_make_pred_expr_pred
		(pt_to_pred_expr (parser, node->info.expr.arg1),
		 pt_to_pred_expr (parser, node->info.expr.arg2), B_AND);
	      break;

	    case PT_OR:
	      /* set information for inst_num() scan type */
	      *argp |= PT_PRED_ARG_INSTNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_GRBYNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_ORDBYNUM_CONTINUE;
	      pred = pt_make_pred_expr_pred
		(pt_to_pred_expr (parser, node->info.expr.arg1),
		 pt_to_pred_expr (parser, node->info.expr.arg2), B_OR);
	      break;

	    case PT_NOT:
	      /* We cannot certain what we have to do if NOT predicate
	         set information for inst_num() scan type */
	      *argp |= PT_PRED_ARG_INSTNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_GRBYNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_ORDBYNUM_CONTINUE;
	      pred = pt_make_pred_term_not
		(pt_to_pred_expr (parser, node->info.expr.arg1));
	      break;

	      /* one to one comparisons */
	    case PT_EQ:
	      assert (node->node_type == PT_EXPR);
	      pred = pt_make_pred_term_comp (regu_var1, regu_var2,
					     ((node->info.expr.qualifier ==
					       PT_EQ_TORDER)
					      ? R_EQ_TORDER : R_EQ));
	      break;

	    case PT_NULLSAFE_EQ:
	      pred = pt_make_pred_term_comp (regu_var1, regu_var2,
					     R_NULLSAFE_EQ);
	      break;

	    case PT_IS:
	      pred = pt_make_pred_term_is (parser, node->info.expr.arg1,
					   node->info.expr.arg2, B_IS);
	      break;

	    case PT_IS_NOT:
	      pred = pt_make_pred_term_is (parser, node->info.expr.arg1,
					   node->info.expr.arg2, B_IS_NOT);
	      break;

	    case PT_ISNULL:
	      regu_var1 = pt_to_regu_variable (parser, node->info.expr.arg1,
					       UNBOX_AS_VALUE);
	      pred = pt_make_pred_term_comp (regu_var1, NULL, R_NULL);
	      break;

	    case PT_XOR:
	      pred = pt_make_pred_expr_pred
		(pt_to_pred_expr (parser, node->info.expr.arg1),
		 pt_to_pred_expr (parser, node->info.expr.arg2), B_XOR);
	      break;

	    case PT_NE:
	      /* We cannot certain what we have to do if NOT predicate */
	      /* set information for inst_num() scan type */
	      *argp |= PT_PRED_ARG_INSTNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_GRBYNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_ORDBYNUM_CONTINUE;
	      pred = pt_make_pred_term_comp (regu_var1, regu_var2, R_NE);
	      break;

	    case PT_GE:
	      pred = pt_make_pred_term_comp (regu_var1, regu_var2, R_GE);
	      break;

	    case PT_GT:
	      pred = pt_make_pred_term_comp (regu_var1, regu_var2, R_GT);
	      break;

	    case PT_LT:
	      pred = pt_make_pred_term_comp (regu_var1, regu_var2, R_LT);
	      break;

	    case PT_LE:
	      pred = pt_make_pred_term_comp (regu_var1, regu_var2, R_LE);
	      break;

	    case PT_EXISTS:
	      regu_var1 = pt_to_regu_variable (parser, node->info.expr.arg1,
					       UNBOX_AS_TABLE);
	      pred = pt_make_pred_term_comp (regu_var1, NULL, R_EXISTS);
	      break;

	    case PT_IS_NULL:
	    case PT_IS_NOT_NULL:
	      regu_var1 = pt_to_regu_variable (parser, node->info.expr.arg1,
					       UNBOX_AS_VALUE);
	      pred = pt_make_pred_term_comp (regu_var1, NULL, R_NULL);

	      if (node->info.expr.op == PT_IS_NOT_NULL)
		{
		  pred = pt_make_pred_term_not (pred);
		}
	      break;

	    case PT_NOT_BETWEEN:
	      /* set information for inst_num() scan type */
	      *argp |= PT_PRED_ARG_INSTNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_GRBYNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_ORDBYNUM_CONTINUE;

	    case PT_BETWEEN:
	    case PT_RANGE:
	      /* set information for inst_num() scan type */
	      if (node->info.expr.arg2 && node->info.expr.arg2->or_next)
		{
		  *argp |= PT_PRED_ARG_INSTNUM_CONTINUE;
		  *argp |= PT_PRED_ARG_GRBYNUM_CONTINUE;
		  *argp |= PT_PRED_ARG_ORDBYNUM_CONTINUE;
		}

	      {
		PT_NODE *arg1, *arg2, *lower, *upper;
		PRED_EXPR *pred1, *pred2;
		REGU_VARIABLE *regu;
		REL_OP op1 = 0, op2 = 0;

		arg1 = node->info.expr.arg1;
		regu = pt_to_regu_variable (parser, arg1, UNBOX_AS_VALUE);

		/* only PT_RANGE has 'or_next' link;
		   PT_BETWEEN and PT_NOT_BETWEEN do not have 'or_next' */

		/* for each range spec of RANGE node */
		for (arg2 = node->info.expr.arg2; arg2; arg2 = arg2->or_next)
		  {
		    if (!arg2 || arg2->node_type != PT_EXPR
			|| !pt_is_between_range_op (arg2->info.expr.op))
		      {
			/* error! */
			break;
		      }
		    lower = arg2->info.expr.arg1;
		    upper = arg2->info.expr.arg2;

		    switch (arg2->info.expr.op)
		      {
		      case PT_BETWEEN_AND:
		      case PT_BETWEEN_GE_LE:
			op1 = R_GE;
			op2 = R_LE;
			break;
		      case PT_BETWEEN_GE_LT:
			op1 = R_GE;
			op2 = R_LT;
			break;
		      case PT_BETWEEN_GT_LE:
			op1 = R_GT;
			op2 = R_LE;
			break;
		      case PT_BETWEEN_GT_LT:
			op1 = R_GT;
			op2 = R_LT;
			break;
		      case PT_BETWEEN_EQ_NA:
			/* special case;
			   if this range spec is derived from '=' or 'IN' */
			op1 = R_EQ;
			op2 = (REL_OP) 0;
			break;
		      case PT_BETWEEN_INF_LE:
			op1 = R_LE;
			op2 = (REL_OP) 0;
			break;
		      case PT_BETWEEN_INF_LT:
			op1 = R_LT;
			op2 = (REL_OP) 0;
			break;
		      case PT_BETWEEN_GE_INF:
			op1 = R_GE;
			op2 = (REL_OP) 0;
			break;
		      case PT_BETWEEN_GT_INF:
			op1 = R_GT;
			op2 = (REL_OP) 0;
			break;
		      default:
			break;
		      }

		    if (op1)
		      {
			regu_var1 = pt_to_regu_variable (parser, lower,
							 UNBOX_AS_VALUE);
			pred1 = pt_make_pred_term_comp (regu, regu_var1, op1);
		      }
		    else
		      {
			pred1 = NULL;
		      }

		    if (op2)
		      {
			regu_var2 = pt_to_regu_variable (parser, upper,
							 UNBOX_AS_VALUE);
			pred2 = pt_make_pred_term_comp (regu, regu_var2, op2);
		      }
		    else
		      {
			pred2 = NULL;
		      }

		    /* make AND predicate of both two expressions */
		    if (pred1 && pred2)
		      {
			pred1 = pt_make_pred_expr_pred (pred1, pred2, B_AND);
		      }

		    /* make NOT predicate of BETWEEN predicate */
		    if (node->info.expr.op == PT_NOT_BETWEEN)
		      {
			pred1 = pt_make_pred_term_not (pred1);
		      }

		    /* make OR predicate */
		    pred = (pred)
		      ? pt_make_pred_expr_pred (pred1, pred, B_OR) : pred1;
		  }		/* for (arg2 = node->info.expr.arg2; ...) */
	      }
	      break;

	      /* one to many comparisons */
	    case PT_IS_NOT_IN:
	    case PT_IS_IN:
	      /* set information for inst_num() scan type */
	      *argp |= PT_PRED_ARG_INSTNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_GRBYNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_ORDBYNUM_CONTINUE;
	      pred = pt_make_pred_term_some_all (regu_var1, regu_var2,
						 R_EQ, F_SOME);

	      if (node->info.expr.op == PT_IS_NOT_IN)
		{
		  pred = pt_make_pred_term_not (pred);
		}
	      break;

	      /* like comparison */
	    case PT_NOT_LIKE:
	    case PT_LIKE:
	      /* set information for inst_num() scan type */
	      *argp |= PT_PRED_ARG_INSTNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_GRBYNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_ORDBYNUM_CONTINUE;
	      {
		REGU_VARIABLE *regu_escape = NULL;
		PT_NODE *arg2 = node->info.expr.arg2;

		regu_var1 = pt_to_regu_variable (parser, node->info.expr.arg1,
						 UNBOX_AS_VALUE);

		if (arg2
		    && arg2->node_type == PT_EXPR
		    && arg2->info.expr.op == PT_LIKE_ESCAPE)
		  {
		    /* this should be an escape character expression */
		    if ((arg2->info.expr.arg2->node_type != PT_VALUE)
			&& (arg2->info.expr.arg2->node_type != PT_HOST_VAR))
		      {
			PT_ERRORm (parser, arg2, MSGCAT_SET_PARSER_SEMANTIC,
				   MSGCAT_SEMANTIC_WANT_ESC_LIT_STRING);
			break;
		      }

		    regu_escape = pt_to_regu_variable (parser,
						       arg2->info.expr.arg2,
						       UNBOX_AS_VALUE);
		    arg2 = arg2->info.expr.arg1;
		  }

		regu_var2 = pt_to_regu_variable (parser,
						 arg2, UNBOX_AS_VALUE);

		pred = pt_make_pred_term_like (regu_var1,
					       regu_var2, regu_escape);

		if (node->info.expr.op == PT_NOT_LIKE)
		  {
		    pred = pt_make_pred_term_not (pred);
		  }
	      }
	      break;

	      /* regex like comparison */
	    case PT_RLIKE:
	    case PT_NOT_RLIKE:
	    case PT_RLIKE_BINARY:
	    case PT_NOT_RLIKE_BINARY:
	      /* set information for inst_num() scan type */
	      *argp |= PT_PRED_ARG_INSTNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_GRBYNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_ORDBYNUM_CONTINUE;
	      {
		regu_var1 = pt_to_regu_variable (parser, node->info.expr.arg1,
						 UNBOX_AS_VALUE);

		regu_var2 = pt_to_regu_variable (parser, node->info.expr.arg2,
						 UNBOX_AS_VALUE);

		regu_var3 = pt_to_regu_variable (parser, node->info.expr.arg3,
						 UNBOX_AS_VALUE);

		pred = pt_make_pred_term_rlike
		  (regu_var1, regu_var2, regu_var3);

		if (node->info.expr.op == PT_NOT_RLIKE ||
		    node->info.expr.op == PT_NOT_RLIKE_BINARY)
		  {
		    pred = pt_make_pred_term_not (pred);
		  }
	      }
	      break;

	      /* this is an error ! */
	    default:
	      pred = NULL;
	      break;
	    }			/* switch (node->info.expr.op) */

	  /* to get information for inst_num() scan typr from
	     pt_to_regu_variable(), borrow 'parser->etc' field */
	  if (parser->etc)
	    {
	      *argp |= PT_PRED_ARG_INSTNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_GRBYNUM_CONTINUE;
	      *argp |= PT_PRED_ARG_ORDBYNUM_CONTINUE;
	    }

	  parser->etc = saved_etc;
	}
      else if (node->node_type == PT_HOST_VAR)
	{
	  /* It should be ( ? ). */
	  /* The predicate expression is ( ( ? <> 0 ) ). */

	  PT_NODE *arg2;
	  bool is_logical = false;

	  /* we may have type_enum set to PT_TYPE_LOGICAL by type checking,
	     if this is the case set it to PT_TYPE_INTEGER to avoid
	     recursion */
	  if (node->type_enum == PT_TYPE_LOGICAL)
	    {
	      node->type_enum = PT_TYPE_INTEGER;
	      is_logical = true;
	    }

	  arg2 = parser_new_node (parser, PT_VALUE);

	  if (arg2)
	    {
	      arg2->type_enum = PT_TYPE_INTEGER;
	      arg2->info.value.data_value.i = 0;

	      regu_var1 = pt_to_regu_variable (parser, node, UNBOX_AS_VALUE);
	      regu_var2 = pt_to_regu_variable (parser, arg2, UNBOX_AS_VALUE);
	      pred = pt_make_pred_term_comp (regu_var1, regu_var2, R_NE);
	    }
	  else
	    {
	      PT_INTERNAL_ERROR (parser, "allocate new node");
	    }

	  /* restore original type */
	  if (is_logical)
	    {
	      node->type_enum = PT_TYPE_LOGICAL;
	    }
	}
      else
	{
	  /* We still need to generate a predicate so that hierarchical queries
	   * or aggregate queries with false predicates return the correct answer.
	   */
	  PT_NODE *arg1 = parser_new_node (parser, PT_VALUE);
	  PT_NODE *arg2 = parser_new_node (parser, PT_VALUE);

	  if (arg1 && arg2)
	    {
	      arg1->type_enum = PT_TYPE_INTEGER;
	      if (node->type_enum == PT_TYPE_LOGICAL
		  && node->info.value.data_value.i != 0)
		{
		  arg1->info.value.data_value.i = 1;
		}
	      else
		{
		  arg1->info.value.data_value.i = 0;
		}
	      arg2->type_enum = PT_TYPE_INTEGER;
	      arg2->info.value.data_value.i = 1;

	      regu_var1 = pt_to_regu_variable (parser, arg1, UNBOX_AS_VALUE);
	      regu_var2 = pt_to_regu_variable (parser, arg2, UNBOX_AS_VALUE);
	      pred = pt_make_pred_term_comp (regu_var1, regu_var2, R_EQ);
	    }
	  else
	    {
	      PT_INTERNAL_ERROR (parser, "allocate new node");
	    }
	}

      node = save_node;		/* restore */
    }

  if (node && pred == NULL)
    {
      if (!pt_has_error (parser))
	{
	  PT_INTERNAL_ERROR (parser, "generate predicate");
	}
    }

  return pred;
}

/*
 * pt_to_pred_expr_with_arg () - converts a list of expression tree to
 * 	xasl 'pred' expressions, where each item of the list represents
 * 	a conjunctive normal form term
 *   return: A NULL return indicates an error occurred
 *   parser(in):
 *   node_list(in):
 *   argp(out):
 */
PRED_EXPR *
pt_to_pred_expr_with_arg (PARSER_CONTEXT * parser, PT_NODE * node_list,
			  int *argp)
{
  PRED_EXPR *cnf_pred, *dnf_pred, *temp;
  PT_NODE *node, *cnf_node, *dnf_node;
  int dummy;
  int num_dnf, i;

  if (!argp)
    {
      argp = &dummy;
    }
  *argp = 0;

  /* convert CNF list into right-linear chains of AND terms */
  cnf_pred = NULL;
  for (node = node_list; node; node = node->next)
    {
      cnf_node = node;

      CAST_POINTER_TO_NODE (cnf_node);

      if (cnf_node->or_next)
	{
	  /* if term has OR, set information for inst_num() scan type */
	  *argp |= PT_PRED_ARG_INSTNUM_CONTINUE;
	  *argp |= PT_PRED_ARG_GRBYNUM_CONTINUE;
	  *argp |= PT_PRED_ARG_ORDBYNUM_CONTINUE;
	}


      dnf_pred = NULL;

      num_dnf = 0;
      for (dnf_node = cnf_node; dnf_node; dnf_node = dnf_node->or_next)
	{
	  num_dnf++;
	}

      while (num_dnf)
	{
	  dnf_node = cnf_node;
	  for (i = 1; i < num_dnf; i++)
	    {
	      dnf_node = dnf_node->or_next;
	    }

	  /* get the last dnf_node */
	  temp = pt_to_pred_expr_local_with_arg (parser, dnf_node, argp);
	  if (temp == NULL)
	    {
	      goto error;
	    }

	  /* set PT_PRED_ARG_INSTNUM_CONTINUE flag for numbering in each
	   * node of the predicate
	   */
	  parser_walk_tree (parser, dnf_node, NULL, NULL,
			    pt_numbering_set_continue_post, argp);

	  dnf_pred = (dnf_pred)
	    ? pt_make_pred_expr_pred (temp, dnf_pred, B_OR) : temp;

	  if (dnf_pred == NULL)
	    {
	      goto error;
	    }

	  num_dnf--;		/* decrease to the previous dnf_node */
	}			/* while (num_dnf) */

      cnf_pred = (cnf_pred)
	? pt_make_pred_expr_pred (dnf_pred, cnf_pred, B_AND) : dnf_pred;

      if (cnf_pred == NULL)
	{
	  goto error;
	}
    }				/* for (node = node_list; ...) */

  return cnf_pred;

error:
  PT_INTERNAL_ERROR (parser, "predicate");
  return NULL;
}

/*
 * pt_to_pred_expr () -
 *   return:
 *   parser(in):
 *   node(in):
 */
PRED_EXPR *
pt_to_pred_expr (PARSER_CONTEXT * parser, PT_NODE * node)
{
  return pt_to_pred_expr_with_arg (parser, node, NULL);
}

/*
 * pt_xasl_type_enum_to_domain () - Given a PT_TYPE_ENUM generate a domain
 *                                  for it and cache it
 *   return:
 *   type(in):
 */
TP_DOMAIN *
pt_xasl_type_enum_to_domain (const PT_TYPE_ENUM type)
{
  TP_DOMAIN *dom;

  dom = pt_type_enum_to_db_domain (type);
  return tp_domain_cache (dom);
}

/*
 * pt_xasl_node_to_domain () - Given a PT_NODE generate a domain
 *                             for it and cache it
 *   return:
 *   parser(in):
 *   node(in):
 */
TP_DOMAIN *
pt_xasl_node_to_domain (PARSER_CONTEXT * parser, PT_NODE * node)
{
  TP_DOMAIN *dom;

  dom = pt_node_to_db_domain (parser, node, NULL);
  if (dom)
    {
      return tp_domain_cache (dom);
    }
  else
    {
      PT_ERRORc (parser, node, er_msg ());
      return NULL;
    }
}

/*
 * pt_xasl_data_type_to_domain () - Given a PT_DATA_TYPE node generate
 *                                  a domain for it and cache it
 *   return:
 *   parser(in):
 *   node(in):
 */
static TP_DOMAIN *
pt_xasl_data_type_to_domain (PARSER_CONTEXT * parser, PT_NODE * node)
{
  TP_DOMAIN *dom;

  dom = pt_data_type_to_db_domain (parser, node, NULL);
  return tp_domain_cache (dom);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * hhhhmmss () - print a time value as 'hhhhmmss'
 *   return:
 *   time(in):
 *   buf(out):
 *   buflen(in):
 */
static int
hhhhmmss (const DB_TIME * time, char *buf, int buflen)
{
  const char date_fmt[] = "00%H%M%S";
  DB_DATE date;

  /* pick any valid date, even though we're interested only in time,
   * to pacify db_strftime */
  db_date_encode (&date, 12, 31, 1970);

  return db_strftime (buf, buflen, date_fmt, &date, (DB_TIME *) time);
}

/*
 * hhmiss () - print a time value as 'hh:mi:ss'
 *   return:
 *   time(in):
 *   buf(out):
 *   buflen(in):
 */
static int
hhmiss (const DB_TIME * time, char *buf, int buflen)
{
  const char date_fmt[] = "%H:%M:%S";
  DB_DATE date;

  /* pick any valid date, even though we're interested only in time,
   * to pacify db_strftime */
  db_date_encode (&date, 12, 31, 1970);

  return db_strftime (buf, buflen, date_fmt, &date, (DB_TIME *) time);
}

/*
 * hhmissms () - print a time value as 'hh:mi:ss.ms'
 *   return:
 *   time(in):
 *   buf(out):
 *   buflen(in):
 */
static int
hhmissms (const unsigned int mtime, char *buf, int buflen)
{
  DB_DATETIME datetime;
  int month, day, year;
  int hour, minute, second, millisecond;
  int retval;

  datetime.date = 0;
  datetime.time = mtime;

  db_datetime_decode (&datetime, &month, &day, &year,
		      &hour, &minute, &second, &millisecond);

  /* "H:%M:%S.MS"; */
  retval = sprintf (buf, "%d:%d:%d.%d", hour, minute, second, millisecond);

  return retval;
}

/*
 * yyyymmdd () - print a date as 'yyyymmdd'
 *   return:
 *   date(in):
 *   buf(out):
 *   buflen(in):
 */
static int
yyyymmdd (const DB_DATE * date, char *buf, int buflen)
{
  const char date_fmt[] = "%Y%m%d";
  DB_TIME time = 0;

  return db_strftime (buf, buflen, date_fmt, (DB_DATE *) date, &time);
}


/*
 * yymmdd () - print a date as 'yyyy-mm-dd'
 *   return:
 *   date(in):
 *   buf(out):
 *   buflen(in):
 */
static int
yymmdd (const DB_DATE * date, char *buf, int buflen)
{
  const char date_fmt[] = "%Y-%m-%d";
  DB_TIME time = 0;

  return db_strftime (buf, buflen, date_fmt, (DB_DATE *) date, &time);
}

/*
 * yyyymmddhhmissms () - print utime as 'yyyy-mm-dd:hh:mi:ss.ms'
 *   return:
 *   datetime(in):
 *   buf(out):
 *   buflen(in):
 */
static int
yyyymmddhhmissms (const DB_DATETIME * datetime, char *buf, int buflen)
{
  int month, day, year;
  int hour, minute, second, millisecond;
  int retval;

  /* extract date & time from datetime */
  db_datetime_decode (datetime, &month, &day, &year,
		      &hour, &minute, &second, &millisecond);

  /* "%Y-%m-%d:%H:%M:%S.MS"; */
  retval = sprintf (buf, "%d-%d-%d:%d:%d:%d.%d",
		    year, month, day, hour, minute, second, millisecond);

  return retval;
}


/*
 * mmddyyyyhhmissms () - print utime as 'mm/dd/yyyy hh:mi:ss.ms'
 *   return:
 *   datetime(in):
 *   buf(in):
 *   buflen(in):
 */
static int
mmddyyyyhhmissms (const DB_DATETIME * datetime, char *buf, int buflen)
{
  int month, day, year;
  int hour, minute, second, millisecond;
  int retval;

  /* extract date & time from datetime */
  db_datetime_decode (datetime, &month, &day, &year,
		      &hour, &minute, &second, &millisecond);

  /* "%m/%d/%Y %H:%M:%S.MS"; */
  retval = sprintf (buf, "%d/%d/%d %d:%d:%d.%d",
		    month, day, year, hour, minute, second, millisecond);

  return retval;
}
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * host_var_name () -  manufacture a host variable name
 *   return:  a host variable name
 *   custom_print(in): a custom_print member
 */
static char *
host_var_name (unsigned int custom_print)
{
  return (char *) "?";
}
#endif


static PT_NODE *
pt_query_set_reference (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *query, *spec, *temp;

  query = node;
  while (query
	 && (query->node_type == PT_UNION
	     || query->node_type == PT_INTERSECTION
	     || query->node_type == PT_DIFFERENCE))
    {
      query = query->info.query.q.union_.arg1;
    }

  if (query)
    {
      spec = query->info.query.q.select.from;
    }
  if (query && spec)
    {
      /* recalculate referenced attributes */
      for (temp = spec; temp; temp = temp->next)
	{
	  node = mq_set_references (parser, node, temp);
	}
    }

  return node;
}


/*
 * pt_split_having_grbynum () - Make a two lists of predicates, one "simply"
 *      having predicates, and one containing groupby_num() function
 *   return:
 *   parser(in):
 *   having(in/out):
 *   having_part(out):
 *   grbynum_part(out):
 */
static void
pt_split_having_grbynum (PARSER_CONTEXT * parser, PT_NODE * having,
			 PT_NODE ** having_part, PT_NODE ** grbynum_part)
{
  PT_NODE *next;
  bool grbynum_flag;

  *having_part = NULL;
  *grbynum_part = NULL;

  while (having)
    {
      next = having->next;
      having->next = NULL;

      grbynum_flag = false;
      (void) parser_walk_tree (parser, having, pt_check_groupbynum_pre, NULL,
			       pt_check_groupbynum_post, &grbynum_flag);

      if (grbynum_flag)
	{
	  having->next = *grbynum_part;
	  *grbynum_part = having;
	}
      else
	{
	  having->next = *having_part;
	  *having_part = having;
	}

      having = next;
    }
}


/*
 * pt_make_identity_offsets () - Create an attr_offset array that
 *                               has 0 for position 0, 1 for position 1, etc
 *   return:
 *   attr_list(in):
 */
static int *
pt_make_identity_offsets (PT_NODE * attr_list)
{
  int *offsets;
  int num_attrs, i;

  if ((num_attrs = pt_length_of_list (attr_list)) == 0)
    {
      return NULL;
    }

  if ((offsets = (int *) malloc ((num_attrs + 1) * sizeof (int))) == NULL)
    {
      return NULL;
    }

  for (i = 0; i < num_attrs; i++)
    {
      offsets[i] = i;
    }
  offsets[i] = -1;

  return offsets;
}


/*
 * pt_split_attrs () - Split the attr_list into two lists without destroying
 *      the original list
 *   return:
 *   parser(in):
 *   table_info(in):
 *   pred(in):
 *   pred_attrs(out):
 *   rest_attrs(out):
 *   pred_offsets(out):
 *   rest_offsets(out):
 *
 * Note :
 * Those attrs that are found in the pred are put on the pred_attrs list,
 * those attrs not found in the pred are put on the rest_attrs list
 */
static int
pt_split_attrs (PARSER_CONTEXT * parser, TABLE_INFO * table_info,
		PT_NODE * pred, PT_NODE ** pred_attrs, PT_NODE ** rest_attrs,
		int **pred_offsets, int **rest_offsets)
{
  PT_NODE *tmp, *pointer, *real_attrs;
  PT_NODE *pred_nodes;
  int cur_pred, cur_rest, num_attrs, i;
  PT_NODE *attr_list = table_info->attribute_list;
  PT_NODE *node, *save_node, *save_next, *ref_node;

  pred_nodes = NULL;		/* init */
  *pred_attrs = NULL;
  *rest_attrs = NULL;
  *pred_offsets = NULL;
  *rest_offsets = NULL;
  cur_pred = 0;
  cur_rest = 0;

  if (!attr_list)
    return 1;			/* nothing to do */

  num_attrs = pt_length_of_list (attr_list);
  if ((*pred_offsets = (int *) malloc (num_attrs * sizeof (int))) == NULL)
    {
      goto exit_on_error;
    }

  if ((*rest_offsets = (int *) malloc (num_attrs * sizeof (int))) == NULL)
    {
      goto exit_on_error;
    }

  if (!pred)
    {
      *rest_attrs = pt_point_l (parser, attr_list);
      for (i = 0; i < num_attrs; i++)
	{
	  (*rest_offsets)[i] = i;
	}
      return 1;
    }

  /* mq_get_references() is destructive to the real set of referenced
   * attrs, so we need to squirrel it away. */
  real_attrs = table_info->class_spec->info.spec.referenced_attrs;
  table_info->class_spec->info.spec.referenced_attrs = NULL;

  /* Traverse pred */
  for (node = pred; node; node = node->next)
    {
      save_node = node;		/* save */

      CAST_POINTER_TO_NODE (node);

      if (node)
	{
	  /* save and cut-off node link */
	  save_next = node->next;
	  node->next = NULL;

	  ref_node = mq_get_references (parser, node, table_info->class_spec);
	  pred_nodes = parser_append_node (ref_node, pred_nodes);

	  /* restore node link */
	  node->next = save_next;
	}

      node = save_node;		/* restore */
    }				/* for (node = ...) */

  table_info->class_spec->info.spec.referenced_attrs = real_attrs;

  tmp = attr_list;
  i = 0;
  while (tmp)
    {
      pointer = pt_point (parser, tmp);
      if (pointer == NULL)
	{
	  goto exit_on_error;
	}

      if (pt_find_attribute (parser, tmp, pred_nodes) != -1)
	{
	  *pred_attrs = parser_append_node (pointer, *pred_attrs);
	  (*pred_offsets)[cur_pred++] = i;
	}
      else
	{
	  *rest_attrs = parser_append_node (pointer, *rest_attrs);
	  (*rest_offsets)[cur_rest++] = i;
	}
      tmp = tmp->next;
      i++;
    }

  if (pred_nodes)
    {
      parser_free_tree (parser, pred_nodes);
    }

  return 1;

exit_on_error:

  parser_free_tree (parser, *pred_attrs);
  parser_free_tree (parser, *rest_attrs);
  free_and_init (*pred_offsets);
  free_and_init (*rest_offsets);
  if (pred_nodes)
    {
      parser_free_tree (parser, pred_nodes);
    }

  return 0;
}


/*
 * pt_to_index_attrs () - Those attrs that are found in the key-range pred
 *                        and key-filter pred are put on the pred_attrs list
 *   return:
 *   parser(in):
 *   table_info(in):
 *   index_pred(in):
 *   key_filter_pred(in):
 *   pred_attrs(out):
 *   pred_offsets(out):
 */
static int
pt_to_index_attrs (PARSER_CONTEXT * parser, TABLE_INFO * table_info,
		   UNUSED_ARG QO_XASL_INDEX_INFO * index_pred,
		   PT_NODE * key_filter_pred, PT_NODE ** pred_attrs,
		   int **pred_offsets)
{
  PT_NODE *tmp, *pointer, *real_attrs;
  PT_NODE *pred_nodes;
  int cur_pred, num_attrs, i;
  PT_NODE *attr_list = table_info->attribute_list;
  PT_NODE *node, *save_node, *save_next, *ref_node;

  pred_nodes = NULL;		/* init */
  *pred_attrs = NULL;
  *pred_offsets = NULL;
  cur_pred = 0;

  if (!attr_list)
    return 1;			/* nothing to do */

  num_attrs = pt_length_of_list (attr_list);
  *pred_offsets = (int *) malloc (num_attrs * sizeof (int));
  if (*pred_offsets == NULL)
    {
      goto exit_on_error;
    }

  /* mq_get_references() is destructive to the real set of referenced
     attrs, so we need to squirrel it away. */
  real_attrs = table_info->class_spec->info.spec.referenced_attrs;
  table_info->class_spec->info.spec.referenced_attrs = NULL;

  /* Traverse key-filter pred */
  for (node = key_filter_pred; node; node = node->next)
    {
      save_node = node;		/* save */

      CAST_POINTER_TO_NODE (node);

      if (node)
	{
	  /* save and cut-off node link */
	  save_next = node->next;
	  node->next = NULL;

	  /* exclude path entities */
	  ref_node = mq_get_references_helper (parser, node,
					       table_info->class_spec, false);
	  pred_nodes = parser_append_node (ref_node, pred_nodes);

	  /* restore node link */
	  node->next = save_next;
	}

      node = save_node;		/* restore */
    }				/* for (node = ...) */

  table_info->class_spec->info.spec.referenced_attrs = real_attrs;

  if (!pred_nodes)		/* there is not key-filter pred */
    {
      return 1;
    }

  tmp = attr_list;
  i = 0;
  while (tmp)
    {
      if (pt_find_attribute (parser, tmp, pred_nodes) != -1)
	{
	  if ((pointer = pt_point (parser, tmp)) == NULL)
	    {
	      goto exit_on_error;
	    }
	  *pred_attrs = parser_append_node (pointer, *pred_attrs);
	  (*pred_offsets)[cur_pred++] = i;
	}
      tmp = tmp->next;
      i++;
    }

  if (pred_nodes)
    {
      parser_free_tree (parser, pred_nodes);
    }

  return 1;

exit_on_error:

  parser_free_tree (parser, *pred_attrs);
  free_and_init (*pred_offsets);
  if (pred_nodes)
    {
      parser_free_tree (parser, pred_nodes);
    }
  return 0;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_flush_classes () - Flushes each class encountered
 *   return:
 *   parser(in):
 *   node(in):
 *   void_arg(in):
 *   continue_walk(in/out):
 */
PT_NODE *
pt_flush_classes (PARSER_CONTEXT * parser, PT_NODE * node,
		  void *arg, int *continue_walk)
{
  PT_NODE *class_;
  int isvirt;
  MOP clsmop = NULL;

  /* If parser->dont_flush is asserted, skip the flushing. */
  if (node->node_type == PT_SPEC)
    {
      for (class_ = node->info.spec.flat_entity_list;
	   class_; class_ = class_->next)
	{
	  clsmop = class_->info.name.db_object;
	  if (clsmop == NULL)
	    {
	      assert (false);
	      PT_ERROR (parser, node, "Generic error");
	    }
	  /* if class object is not dirty and doesn't contain any
	   * dirty instances, do not flush the class and its instances */
	  if (WS_ISDIRTY (class_->info.name.db_object)
	      || ws_has_dirty_objects (class_->info.name.db_object, &isvirt))
	    {
	      if (sm_flush_objects (class_->info.name.db_object) != NO_ERROR)
		{
		  PT_ERRORc (parser, class_, er_msg ());
		}
	    }
	  /* Also test if we need to flush partitions of each class */
	  if (!locator_is_class (clsmop))
	    {
	      continue;
	    }
	}
    }

  return node;
}
#endif

/*
 * pt_flush_class_and_null_xasl () - Flushes each class encountered
 *   return:
 *   parser(in):
 *   tree(in):
 *   void_arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
pt_flush_class_and_null_xasl (UNUSED_ARG PARSER_CONTEXT * parser,
			      PT_NODE * tree,
			      UNUSED_ARG void *void_arg,
			      UNUSED_ARG int *continue_walk)
{
#if 1				/* TODO - trace */
  assert (!ws_has_updated ());
#else
  if (ws_has_updated ())
    {
      tree = pt_flush_classes (parser, tree, void_arg, continue_walk);
    }
#endif

  if (PT_IS_QUERY_NODE_TYPE (tree->node_type))
    {
      tree->info.query.xasl = NULL;
    }

  return tree;
}

/*
 * pt_is_subquery () -
 *   return: true if symbols comes from a subquery of a UNION-type thing
 *   node(in):
 */
static int
pt_is_subquery (PT_NODE * node)
{
  PT_MISC_TYPE subquery_type = node->info.query.is_subquery;

  return (subquery_type != 0);
}

/*
 * pt_table_info_alloc () - Allocates and inits an TABLE_INFO structure
 * 	                    from temporary memory
 *   return:
 *   pt_table_info_alloc(in):
 */
static TABLE_INFO *
pt_table_info_alloc (void)
{
  TABLE_INFO *table_info;

  table_info = (TABLE_INFO *) pt_alloc_packing_buf (sizeof (TABLE_INFO));

  if (table_info)
    {
      table_info->next = NULL;
      table_info->class_spec = NULL;
      table_info->exposed = NULL;
      table_info->spec_id = 0;
      table_info->attribute_list = NULL;
      table_info->value_list = NULL;
      table_info->is_fetch = 0;
    }

  return table_info;
}

/*
 * pt_symbol_info_alloc () - Allocates and inits an SYMBOL_INFO structure
 *                           from temporary memory
 *   return:
 */
static SYMBOL_INFO *
pt_symbol_info_alloc (void)
{
  SYMBOL_INFO *symbols;

  symbols = (SYMBOL_INFO *) pt_alloc_packing_buf (sizeof (SYMBOL_INFO));

  if (symbols)
    {
      symbols->stack = NULL;
      symbols->table_info = NULL;
      symbols->current_class = NULL;
      symbols->cache_attrinfo = NULL;
      symbols->current_listfile = NULL;
      symbols->listfile_unbox = UNBOX_AS_VALUE;
      symbols->listfile_value_list = NULL;

      /* only used for server inserts and updates */
      symbols->listfile_attr_offset = 0;

      symbols->query_node = NULL;
    }

  return symbols;
}


/*
 * pt_is_single_tuple () -
 *   return: true if select can be determined to return exactly one tuple
 *           This means an aggregate function was used with no group_by clause
 *   parser(in):
 *   select_node(in):
 */
int
pt_is_single_tuple (PARSER_CONTEXT * parser, PT_NODE * select_node)
{
  if (select_node->info.query.q.select.group_by != NULL)
    {
      return false;
    }

  return pt_has_aggregate (parser, select_node);
}


/*
 * pt_fillin_pseudo_spec () -
 *   return:
 *   parser(in):
 *   spec(in/out):
 */
static PT_NODE *
pt_fillin_pseudo_spec (PARSER_CONTEXT * parser, PT_NODE * spec)
{
  PT_NODE *chk_parent = NULL;

  if (spec != NULL)
    {
      return spec;		/* OK */
    }


  /* at here, need a pseudo spec */

  spec = parser_new_node (parser, PT_SPEC);
  if (spec == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return NULL;
    }

  spec->info.spec.id = (UINTPTR) spec;
  spec->info.spec.meta_class = PT_CLASS;
  spec->info.spec.entity_name = pt_name (parser, CT_ROOT_NAME);
  if (spec->info.spec.entity_name == NULL)
    {
      parser_free_node (parser, spec);
      return NULL;
    }

  spec = parser_walk_tree (parser, spec, pt_flat_spec_pre,
			   &chk_parent, pt_continue_walk, NULL);

  return spec;
}

/*
 * pt_make_val_list () - Makes a val list with a DB_VALUE place holder
 *                       for every attribute on an attribute list
 *   return:
 *   attribute_list(in):
 */
static VAL_LIST *
pt_make_val_list (UNUSED_ARG PARSER_CONTEXT * parser,
		  const PT_NODE * attribute_list)
{
  VAL_LIST *value_list = NULL;
  QPROC_DB_VALUE_LIST dbval_list;
  QPROC_DB_VALUE_LIST *dbval_list_tail;
  const PT_NODE *attribute;

  value_list = regu_vallist_alloc ();

  if (value_list)
    {
      value_list->val_cnt = 0;
      value_list->valp = NULL;
      dbval_list_tail = &value_list->valp;

      for (attribute = attribute_list; attribute != NULL;
	   attribute = attribute->next)
	{
	  dbval_list = regu_dbvallist_alloc ();
	  if (dbval_list
	      && regu_dbval_type_init (dbval_list->val,
				       pt_node_to_db_type (attribute)))
	    {
	      pt_init_precision_and_scale (dbval_list->val, attribute);

	      value_list->val_cnt++;
	      (*dbval_list_tail) = dbval_list;
	      dbval_list_tail = &dbval_list->next;
	      dbval_list->next = NULL;
	    }
	  else
	    {
	      value_list = NULL;
	      break;
	    }
	}
    }

  return value_list;
}


/*
 * pt_clone_val_list () - Makes a val list with a DB_VALUE place holder
 *                        for every attribute on an attribute list
 *   return:
 *   parser(in):
 *   attribute_list(in):
 */
static VAL_LIST *
pt_clone_val_list (PARSER_CONTEXT * parser, PT_NODE * attribute_list)
{
  VAL_LIST *value_list = NULL;
  QPROC_DB_VALUE_LIST dbval_list;
  QPROC_DB_VALUE_LIST *dbval_list_tail;
  PT_NODE *attribute;
  REGU_VARIABLE *regu = NULL;

  value_list = regu_vallist_alloc ();

  if (value_list)
    {
      value_list->val_cnt = 0;
      value_list->valp = NULL;
      dbval_list_tail = &value_list->valp;

      for (attribute = attribute_list; attribute != NULL;
	   attribute = attribute->next)
	{
	  dbval_list = regu_dbvlist_alloc ();
	  regu = pt_attribute_to_regu (parser, attribute);
	  if (dbval_list && regu)
	    {
	      dbval_list->val = pt_regu_to_dbvalue (parser, regu);

	      value_list->val_cnt++;
	      (*dbval_list_tail) = dbval_list;
	      dbval_list_tail = &dbval_list->next;
	      dbval_list->next = NULL;
	    }
	  else
	    {
	      value_list = NULL;
	      break;
	    }
	}
    }

  return value_list;
}


/*
 * pt_find_table_info () - Finds the table_info associated with an exposed name
 *   return:
 *   spec_id(in):
 *   exposed_list(in):
 */
static TABLE_INFO *
pt_find_table_info (UINTPTR spec_id, TABLE_INFO * exposed_list)
{
  TABLE_INFO *table_info;

  table_info = exposed_list;

  /* look down list until name matches, or NULL reached */
  while (table_info && table_info->spec_id != spec_id)
    {
      table_info = table_info->next;
    }

  return table_info;
}

/*
 * pt_to_aggregate_node () - test for aggregate function nodes,
 * 	                     convert them to aggregate_list_nodes
 *   return:
 *   parser(in):
 *   tree(in):
 *   arg(in/out):
 *   continue_walk(in/out):
 */
static PT_NODE *
pt_to_aggregate_node (PARSER_CONTEXT * parser, PT_NODE * tree,
		      void *arg, int *continue_walk)
{
  bool is_agg = 0;
  REGU_VARIABLE *regu = NULL;
  AGGREGATE_TYPE *aggregate_list;
  AGGREGATE_INFO *info = (AGGREGATE_INFO *) arg;
  REGU_VARIABLE_LIST out_list;
  REGU_VARIABLE_LIST regu_list;
  REGU_VARIABLE_LIST regu_temp;
  VAL_LIST *value_list;
  QPROC_DB_VALUE_LIST value_temp;
  MOP classop;
  PT_NODE *group_concat_sep_node_save = NULL;
  DB_VALUE *sep_val;
  REGU_VARIABLE *sep_regu;

  *continue_walk = PT_CONTINUE_WALK;

  is_agg = pt_is_aggregate_function (parser, tree);
  if (is_agg)
    {
      FUNC_TYPE code = tree->info.function.function_type;

      if (code == PT_GROUPBY_NUM)
	{
	  aggregate_list = regu_agg_grbynum_alloc ();
	  if (aggregate_list == NULL)
	    {
	      PT_ERROR (parser, tree,
			msgcat_message (MSGCAT_CATALOG_RYE,
					MSGCAT_SET_PARSER_SEMANTIC,
					MSGCAT_SEMANTIC_OUT_OF_MEMORY));
	      return tree;
	    }
	  aggregate_list->next = info->head_list;
	  aggregate_list->option = Q_ALL;
	  if (info->grbynum_valp)
	    {
	      if (!(*(info->grbynum_valp)))
		{
		  *(info->grbynum_valp) = regu_dbval_alloc ();
		  regu_dbval_type_init (*(info->grbynum_valp),
					DB_TYPE_INTEGER);
		}
	      aggregate_list->accumulator.value = *(info->grbynum_valp);
	    }
	  aggregate_list->function = code;
	}
      else
	{
	  aggregate_list = regu_agg_alloc ();
	  if (aggregate_list == NULL)
	    {
	      PT_ERROR (parser, tree,
			msgcat_message (MSGCAT_CATALOG_RYE,
					MSGCAT_SET_PARSER_SEMANTIC,
					MSGCAT_SEMANTIC_OUT_OF_MEMORY));
	      return tree;
	    }
	  aggregate_list->next = info->head_list;
	  aggregate_list->option =
	    (tree->info.function.all_or_distinct == PT_ALL)
	    ? Q_ALL : Q_DISTINCT;
	  aggregate_list->function = code;
	  /* others will be set after resolving arg_list */
	}

      aggregate_list->flag_agg_optimize = false;
      BTID_SET_NULL (&aggregate_list->btid);
      if (info->flag_agg_optimize
	  && (aggregate_list->function == PT_COUNT_STAR
	      || aggregate_list->function == PT_COUNT
	      || aggregate_list->function == PT_MAX
	      || aggregate_list->function == PT_MIN))
	{
	  bool need_unique_index;

	  classop = sm_find_class (info->class_name);
	  if (aggregate_list->function == PT_COUNT_STAR
	      || aggregate_list->function == PT_COUNT)
	    {
	      need_unique_index = true;
	    }
	  else
	    {
	      need_unique_index = false;
	    }

	  if (aggregate_list->function == PT_COUNT_STAR)
	    {
	      (void) sm_find_index (classop, NULL, 0,
				    need_unique_index, &aggregate_list->btid);
	      /* If btree does not exist, optimize with heap */
	      aggregate_list->flag_agg_optimize = true;
	    }
	  else
	    {
	      if (tree->info.function.arg_list->node_type == PT_NAME)
		{
		  (void) sm_find_index (classop,
					&tree->info.
					function.arg_list->info.name.original,
					1, need_unique_index,
					&aggregate_list->btid);
		  if (!BTID_IS_NULL (&aggregate_list->btid))
		    {
		      aggregate_list->flag_agg_optimize = true;
		    }
		}
	    }
	}

      if (aggregate_list->function != PT_COUNT_STAR
	  && aggregate_list->function != PT_GROUPBY_NUM)
	{
	  regu = pt_to_regu_variable (parser,
				      tree->info.function.arg_list,
				      UNBOX_AS_VALUE);

	  if (regu == NULL)
	    {
	      PT_ERRORmf (parser, tree, MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_IS_NOT_DEFINED,
			  pt_short_print (parser, tree));
	      return NULL;
	    }

	  regu_dbval_type_init (aggregate_list->accumulator.value,
				pt_node_to_db_type (tree));
	  regu_dbval_type_init (aggregate_list->accumulator.value2,
				pt_node_to_db_type (tree));

	  if (aggregate_list->function == PT_GROUP_CONCAT)
	    {
	      /* store SEPARATOR for GROUP_CONCAT */
	      group_concat_sep_node_save = tree->info.function.arg_list->next;
	      if (group_concat_sep_node_save != NULL)
		{
		  sep_val =
		    pt_value_to_db (parser, group_concat_sep_node_save);
		  if (sep_val == NULL
		      || DB_VALUE_DOMAIN_TYPE (sep_val) != DB_TYPE_VARCHAR)
		    {
		      assert (false);	/* is impossible */
		      PT_ERRORmf (parser, group_concat_sep_node_save,
				  MSGCAT_SET_PARSER_SEMANTIC,
				  MSGCAT_SEMANTIC_IS_NOT_DEFINED,
				  pt_short_print (parser, tree));
		      return NULL;
		    }

		  /* set the next argument pointer (the separator
		   * argument) to NULL in order to avoid impacting
		   * the regu vars generation.
		   */
		  tree->info.function.arg_list->next = NULL;
		}
	      else
		{
		  sep_val = regu_dbval_alloc ();

		  /* set default separator, if one is not specified , only if
		   * argument is not bit */
		  if (tree->type_enum != PT_TYPE_VARBIT)
		    {
		      const char *comma_str = ",";
		      char *str_val;
		      int len;

		      len = strlen (comma_str);
		      str_val = pt_alloc_packing_buf (len + 1);
		      memcpy (str_val, ",", len + 1);
		      DB_MAKE_VARCHAR (sep_val, DB_DEFAULT_PRECISION,
				       str_val, 1,
				       LANG_COLL_ANY /* TODO - */ );
		      sep_val->need_clear = false;
		    }
		  else
		    {
		      db_value_domain_init (sep_val,
					    pt_node_to_db_type (tree),
					    DB_DEFAULT_PRECISION,
					    DB_DEFAULT_SCALE);
		    }
		}
	    }
	  else
	    {
	      sep_val = regu_dbval_alloc ();

	      db_value_domain_init (sep_val,
				    pt_node_to_db_type (tree),
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	    }

	  assert (sep_val != NULL);

	  if (info->out_list && info->value_list && info->regu_list)
	    {
	      int *attr_offsets;
	      PT_NODE *pt_val;

	      /* handle the buildlist case.
	       * append regu to the out_list, and create a new value
	       * to append to the value_list  */

	      pt_val = parser_new_node (parser, PT_VALUE);
	      if (pt_val == NULL)
		{
		  PT_INTERNAL_ERROR (parser, "allocate new node");
		  return NULL;
		}

	      pt_val->type_enum = PT_TYPE_INTEGER;
	      pt_val->info.value.data_value.i = 0;
	      parser_append_node (pt_val, info->out_names);

	      attr_offsets =
		pt_make_identity_offsets (tree->info.function.arg_list);
	      value_list = pt_make_val_list (parser,
					     tree->info.function.arg_list);
	      regu_list = pt_to_position_regu_variable_list
		(parser, tree->info.function.arg_list,
		 value_list, attr_offsets);
	      free_and_init (attr_offsets);

	      out_list = regu_varlist_alloc ();
	      if (!value_list || !regu_list || !out_list)
		{
		  PT_ERROR (parser, tree,
			    msgcat_message (MSGCAT_CATALOG_RYE,
					    MSGCAT_SET_PARSER_SEMANTIC,
					    MSGCAT_SEMANTIC_OUT_OF_MEMORY));
		  return NULL;
		}

	      aggregate_list->operand.type = TYPE_CONSTANT;
	      aggregate_list->operand.value.dbvalptr = value_list->valp->val;

	      regu_list->value.value.pos_descr.pos_no =
		info->out_list->valptr_cnt;

	      /* append value holder to value_list */
	      info->value_list->val_cnt++;
	      value_temp = info->value_list->valp;
	      while (value_temp->next)
		{
		  value_temp = value_temp->next;
		}
	      value_temp->next = value_list->valp;

	      /* append out_list to info->out_list */
	      info->out_list->valptr_cnt++;
	      out_list->next = NULL;
	      out_list->value = *regu;
	      regu_temp = info->out_list->valptrp;
	      while (regu_temp->next)
		{
		  regu_temp = regu_temp->next;
		}
	      regu_temp->next = out_list;

	      /* append regu to info->regu_list */
	      regu_temp = info->regu_list;
	      while (regu_temp->next)
		{
		  regu_temp = regu_temp->next;
		}
	      regu_temp->next = regu_list;
	    }
	  else
	    {
	      /* handle the buildvalue case, simply uses regu as the operand */
	      aggregate_list->operand = *regu;
	    }
	}
      else
	{
	  /* We are set up for count(*).
	   * Make sure that Q_DISTINCT isn't set in this case.  Even
	   * though it is ignored by the query processing proper, it
	   * seems to cause the setup code to build the extendible hash
	   * table it needs for a "select count(distinct foo)" query,
	   * which adds a lot of unnecessary overhead.
	   */
	  aggregate_list->option = Q_ALL;

	  regu_dbval_type_init (aggregate_list->accumulator.value,
				DB_TYPE_BIGINT);
	  regu_dbval_type_init (aggregate_list->accumulator.value2,
				DB_TYPE_BIGINT);

	  sep_val = regu_dbval_alloc ();

	  db_value_domain_init (sep_val,
				pt_node_to_db_type (tree),
				DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
	}

      assert (sep_val != NULL);

      sep_regu = pt_make_regu_constant (parser, sep_val, NULL);
      if (sep_regu == NULL)
	{
	  PT_ERRORmf (parser, tree,
		      MSGCAT_SET_PARSER_SEMANTIC,
		      MSGCAT_SEMANTIC_IS_NOT_DEFINED,
		      pt_short_print (parser, tree));
	  return NULL;
	}

      aggregate_list->group_concat_sep = *sep_regu;

      /* record the value for pt_to_regu_variable to use in "out arith" */
      tree->etc = (void *) aggregate_list->accumulator.value;

      info->head_list = aggregate_list;

      *continue_walk = PT_LIST_WALK;

      /* GROUP_CONCAT : process ORDER BY and restore SEPARATOR node (just to
       * keep original tree)*/
      if (aggregate_list->function == PT_GROUP_CONCAT)
	{
	  /* Separator of GROUP_CONCAT is not a 'real' argument of
	   * GROUP_CONCAT, but for convenience it is kept in 'arg_list' of
	   * PT_FUNCTION.
	   * It is not involved in sorting process, so conversion of ORDER BY
	   * to SORT_LIST must be performed before restoring separator
	   * argument into the arg_list*/
	  if (tree->info.function.order_by != NULL)
	    {
	      /* convert to SORT_LIST */
	      aggregate_list->sort_list =
		pt_agg_orderby_to_sort_list (parser,
					     tree->info.function.order_by,
					     tree->info.function.arg_list);
	    }
	  else
	    {
	      aggregate_list->sort_list = NULL;
	    }

	  /* restore group concat separator node */
	  tree->info.function.arg_list->next = group_concat_sep_node_save;
	}
      else
	{
	  /* only GROUP_CONCAT agg supports ORDER BY */
	  assert (tree->info.function.order_by == NULL);
	  assert (group_concat_sep_node_save == NULL);
	}
    }

  if (tree->node_type == PT_DOT_)
    {
      /* This path must have already appeared in the group-by, and is
       * resolved. Convert it to a name so that we can use it to get
       * the correct list position later.
       */
      PT_NODE *next = tree->next;
      tree = tree->info.dot.arg2;
      tree->next = next;
    }

  if (PT_IS_QUERY (tree))
    {
      /* this is a sub-query. It has its own aggregation scope.
       * Do not proceed down the leaves. */
      *continue_walk = PT_LIST_WALK;
    }

  if (tree->node_type == PT_NAME)
    {
      if (!pt_find_name (parser, tree, info->out_names)
	  && (info->out_list && info->value_list && info->regu_list))
	{
	  int *attr_offsets;
	  PT_NODE *pointer;

	  pointer = pt_point (parser, tree);

	  /* append the name on the out list */
	  info->out_names = parser_append_node (pointer, info->out_names);

	  attr_offsets = pt_make_identity_offsets (pointer);
	  value_list = pt_make_val_list (parser, pointer);
	  regu_list = pt_to_position_regu_variable_list
	    (parser, pointer, value_list, attr_offsets);
	  free_and_init (attr_offsets);

	  out_list = regu_varlist_alloc ();
	  if (!value_list || !regu_list || !out_list)
	    {
	      PT_ERROR (parser, pointer,
			msgcat_message (MSGCAT_CATALOG_RYE,
					MSGCAT_SET_PARSER_SEMANTIC,
					MSGCAT_SEMANTIC_OUT_OF_MEMORY));
	      return NULL;
	    }

	  /* fix count for list position */
	  regu_list->value.value.pos_descr.pos_no =
	    info->out_list->valptr_cnt;

	  /* append value holder to value_list */
	  info->value_list->val_cnt++;
	  value_temp = info->value_list->valp;
	  while (value_temp->next)
	    {
	      value_temp = value_temp->next;
	    }
	  value_temp->next = value_list->valp;

	  regu = pt_to_regu_variable (parser, tree, UNBOX_AS_VALUE);

	  if (!regu)
	    {
	      return NULL;
	    }

	  /* append out_list to info->out_list */
	  info->out_list->valptr_cnt++;
	  out_list->next = NULL;
	  out_list->value = *regu;
	  regu_temp = info->out_list->valptrp;
	  while (regu_temp->next)
	    {
	      regu_temp = regu_temp->next;
	    }
	  regu_temp->next = out_list;

	  /* append regu to info->regu_list */
	  regu_temp = info->regu_list;
	  while (regu_temp->next)
	    {
	      regu_temp = regu_temp->next;
	    }
	  regu_temp->next = regu_list;
	}
      *continue_walk = PT_LIST_WALK;
    }

  if (tree->node_type == PT_SPEC || tree->node_type == PT_DATA_TYPE)
    {
      /* These node types cannot have sub-expressions.
       * Do not proceed down the leaves */
      *continue_walk = PT_LIST_WALK;
    }
  return tree;
}

/*
 * pt_find_attribute () -
 *   return: index of a name in an attribute symbol list,
 *           or -1 if the name is not found in the list
 *   parser(in):
 *   name(in):
 *   attributes(in):
 */
int
pt_find_attribute (PARSER_CONTEXT * parser, const PT_NODE * name,
		   const PT_NODE * attributes)
{
  const PT_NODE *attr, *save_attr;
  int i = 0;

  if (name)
    {
      CAST_POINTER_TO_NODE (name);

      if (name->node_type == PT_NAME)
	{
	  for (attr = attributes; attr != NULL; attr = attr->next)
	    {
	      save_attr = attr;	/* save */

	      CAST_POINTER_TO_NODE (attr);

	      /* are we looking up sort_spec list ?
	       * currently only group by causes this case. */
	      if (attr->node_type == PT_SORT_SPEC)
		{
		  attr = attr->info.sort_spec.expr;
		}

	      if (!name->info.name.resolved)
		{
		  /* are we looking up a path expression name?
		   * currently only group by causes this case. */
		  if (attr->node_type == PT_DOT_
		      && pt_name_equal (parser, name, attr->info.dot.arg2))
		    {
		      return i;
		    }
		}

	      if (pt_name_equal (parser, name, attr))
		{
		  return i;
		}
	      i++;

	      attr = save_attr;	/* restore */
	    }
	}
    }

  return -1;
}

/*
 * pt_index_value () -
 *   return: the DB_VALUE at the index position in a VAL_LIST
 *   value(in):
 *   index(in):
 */
static DB_VALUE *
pt_index_value (const VAL_LIST * value, int index)
{
  QPROC_DB_VALUE_LIST dbval_list;
  DB_VALUE *dbval = NULL;

  if (value && index >= 0)
    {
      dbval_list = value->valp;
      while (dbval_list && index)
	{
	  dbval_list = dbval_list->next;
	  index--;
	}

      if (dbval_list)
	{
	  dbval = dbval_list->val;
	}
    }

  return dbval;
}

/*
 * pt_to_aggregate () - Generates an aggregate list from a select node
 *   return: aggregate XASL node
 *   parser(in): parser context
 *   select_node(in): SELECT statement node
 *   out_list(in): outptr list to generate intermediate file
 *   value_list(in): value list
 *   regu_list(in): regulist to read values from intermediate file
 *   out_names(in): outptr name nodes
 *   grbynum_valp(in): groupby_num() dbvalue
 */
static AGGREGATE_TYPE *
pt_to_aggregate (PARSER_CONTEXT * parser, PT_NODE * select_node,
		 OUTPTR_LIST * out_list,
		 VAL_LIST * value_list,
		 REGU_VARIABLE_LIST regu_list,
		 PT_NODE * out_names, DB_VALUE ** grbynum_valp)
{
  PT_NODE *select_list, *from, *where, *having;
  AGGREGATE_INFO info;

  select_list = select_node->info.query.q.select.list;
  from = select_node->info.query.q.select.from;
  where = select_node->info.query.q.select.where;
  having = select_node->info.query.q.select.having;

  info.head_list = NULL;
  info.out_list = out_list;
  info.value_list = value_list;
  info.regu_list = regu_list;
  info.out_names = out_names;
  info.grbynum_valp = grbynum_valp;

  /* init */
  info.class_name = NULL;
  info.flag_agg_optimize = false;

  if (pt_is_single_tuple (parser, select_node))
    {
      if (where == NULL
	  && pt_length_of_list (from) == 1
	  && pt_length_of_list (from->info.spec.flat_entity_list) == 1)
	{
	  if (from->info.spec.entity_name)
	    {
	      info.class_name =
		from->info.spec.entity_name->info.name.original;
	      info.flag_agg_optimize = true;
	    }
	}
    }

  if (!pt_has_error (parser))
    {
      select_node->info.query.q.select.list =
	parser_walk_tree (parser, select_list, pt_to_aggregate_node, &info,
			  pt_continue_walk, NULL);
    }

  if (!pt_has_error (parser))
    {
      select_node->info.query.q.select.having =
	parser_walk_tree (parser, having, pt_to_aggregate_node, &info,
			  pt_continue_walk, NULL);
    }

  return info.head_list;
}


/*
 * pt_make_table_info () - Sets up symbol table entry for an entity spec
 *   return:
 *   parser(in):
 *   table_spec(in):
 */
static TABLE_INFO *
pt_make_table_info (PARSER_CONTEXT * parser, PT_NODE * table_spec)
{
  TABLE_INFO *table_info;

  if (table_spec == NULL)
    {
      assert (false);
      return NULL;
    }

  table_info = pt_table_info_alloc ();
  if (table_info == NULL)
    {
      return NULL;
    }

  table_info->class_spec = table_spec;
  if (table_spec->info.spec.range_var)
    {
      table_info->exposed =
	table_spec->info.spec.range_var->info.name.original;
    }

  table_info->spec_id = table_spec->info.spec.id;

  /* for classes, it is safe to prune unreferenced attributes.
   * we do not have the same luxury with derived tables, so get them all
   * (and in order). */
  table_info->attribute_list =
    (table_spec->info.spec.flat_entity_list != NULL
     && table_spec->info.spec.derived_table == NULL)
    ? table_spec->info.spec.referenced_attrs
    : table_spec->info.spec.as_attr_list;

  table_info->value_list = pt_make_val_list (parser,
					     table_info->attribute_list);

  if (!table_info->value_list)
    {
      PT_ERRORm (parser, table_info->attribute_list,
		 MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_OUT_OF_MEMORY);
      return NULL;
    }

  return table_info;
}


/*
 * pt_push_symbol_info () - Sets up symbol table information
 *                          for a select statement
 *   return:
 *   parser(in):
 *   select_node(in):
 */
static SYMBOL_INFO *
pt_push_symbol_info (PARSER_CONTEXT * parser, PT_NODE * select_node)
{
  PT_NODE *table_spec;
  SYMBOL_INFO *symbols = NULL;
  TABLE_INFO *table_info;

  symbols = pt_symbol_info_alloc ();

  if (symbols)
    {
      /* push symbols on stack */
      symbols->stack = parser->symbols;
      parser->symbols = symbols;

      symbols->query_node = select_node;

      if (select_node->node_type == PT_SELECT)
	{
	  /* check iff need a pseudo spec */
	  select_node->info.query.q.select.from =
	    pt_fillin_pseudo_spec (parser,
				   select_node->info.query.q.select.from);

	  for (table_spec = select_node->info.query.q.select.from;
	       table_spec != NULL; table_spec = table_spec->next)
	    {
	      table_info = pt_make_table_info (parser, table_spec);
	      if (!table_info)
		{
		  symbols = NULL;
		  break;
		}
	      table_info->next = symbols->table_info;
	      symbols->table_info = table_info;
	    }

	  if (symbols)
	    {
	      symbols->current_class = NULL;
	      symbols->current_listfile = NULL;
	      symbols->listfile_unbox = UNBOX_AS_VALUE;
	    }
	}
    }

  return symbols;
}


/*
 * pt_pop_symbol_info () - Cleans up symbol table information
 *                         for a select statement
 *   return: none
 *   parser(in):
 *   select_node(in):
 */
static void
pt_pop_symbol_info (PARSER_CONTEXT * parser)
{
  SYMBOL_INFO *symbols = NULL;

  if (parser->symbols)
    {
      /* allocated from pt_alloc_packing_buf */
      symbols = parser->symbols->stack;
      parser->symbols = symbols;
    }
  else
    {
      if (!pt_has_error (parser))
	{
	  PT_INTERNAL_ERROR (parser, "generate");
	}
    }
}


/*
 * pt_make_access_spec () - Create an initialized ACCESS_SPEC_TYPE structure,
 *	                    ready to be specialized for class or list
 *   return:
 *   spec_type(in):
 *   access(in):
 *   indexptr(in):
 *   where_key(in):
 *   where_pred(in):
 */
static ACCESS_SPEC_TYPE *
pt_make_access_spec (TARGET_TYPE spec_type,
		     ACCESS_METHOD access,
		     INDX_INFO * indexptr,
		     PRED_EXPR * where_key, PRED_EXPR * where_pred)
{
  ACCESS_SPEC_TYPE *spec = NULL;

  /* validation check */
  if (access == INDEX)
    {
      assert (indexptr != NULL);
      if (indexptr)
	{
	  if (indexptr->coverage)
	    {
	      assert (where_pred == NULL);	/* no data-filter */
	      if (where_pred == NULL)
		{
		  spec = regu_spec_alloc (spec_type);
		}
	    }
	  else
	    {
	      spec = regu_spec_alloc (spec_type);
	    }
	}
    }
  else
    {
      spec = regu_spec_alloc (spec_type);
    }

  if (spec)
    {
      spec->type = spec_type;
      spec->access = access;
      spec->indexptr = indexptr;
      spec->where_key = where_key;
      spec->where_pred = where_pred;
      spec->next = NULL;
    }

  return spec;
}


/*
 * pt_cnt_attrs () - Count the number of regu variables in the list that
 *                   are coming from the heap (ATTR_ID)
 *   return:
 *   attr_list(in):
 */
static int
pt_cnt_attrs (const REGU_VARIABLE_LIST attr_list)
{
  int cnt = 0;
  REGU_VARIABLE_LIST tmp;

  for (tmp = attr_list; tmp; tmp = tmp->next)
    {
      if (tmp->value.type == TYPE_ATTR_ID)
	{
	  cnt++;
	}
      else if (tmp->value.type == TYPE_FUNC)
	{
	  /* need to check all the operands for the function */
	  cnt += pt_cnt_attrs (tmp->value.value.funcp->operand);
	}
    }

  return cnt;
}


/*
 * pt_fill_in_attrid_array () - Fill in the attrids of the regu variables
 *                              in the list that are comming from the heap
 *   return:
 *   attr_list(in):
 *   attr_array(in):
 *   next_pos(in): holds the next spot in the array to be filled in with the
 *                 next attrid
 */
static void
pt_fill_in_attrid_array (REGU_VARIABLE_LIST attr_list, ATTR_ID * attr_array,
			 int *next_pos)
{
  REGU_VARIABLE_LIST tmp;

  for (tmp = attr_list; tmp; tmp = tmp->next)
    {
      if (tmp->value.type == TYPE_ATTR_ID)
	{
	  attr_array[*next_pos] = tmp->value.value.attr_descr.id;
	  *next_pos = *next_pos + 1;
	}
      else if (tmp->value.type == TYPE_FUNC)
	{
	  /* need to check all the operands for the function */
	  pt_fill_in_attrid_array (tmp->value.value.funcp->operand,
				   attr_array, next_pos);
	}
    }
}


/*
 * pt_make_class_access_spec () - Create an initialized
 *                                ACCESS_SPEC_TYPE TARGET_CLASS structure
 *   return:
 *   parser(in):
 *   flat(in):
 *   class(in):
 *   scan_type(in):
 *   access(in):
 *   indexptr(in):
 *   where_key(in):
 *   where_pred(in):
 *   attr_list_key(in):
 *   attr_list_pred(in):
 *   attr_list_rest(in):
 *   pk_next(in):
 *   cache_key(in):
 *   cache_pred(in):
 *   cache_rest(in):
 */
static ACCESS_SPEC_TYPE *
pt_make_class_access_spec (PARSER_CONTEXT * parser,
			   PT_NODE * flat,
			   DB_OBJECT * class_,
			   TARGET_TYPE scan_type,
			   ACCESS_METHOD access,
			   INDX_INFO * indexptr,
			   PRED_EXPR * where_key,
			   PRED_EXPR * where_pred,
			   REGU_VARIABLE_LIST attr_list_key,
			   REGU_VARIABLE_LIST attr_list_pred,
			   REGU_VARIABLE_LIST attr_list_rest,
			   REGU_VARIABLE_LIST pk_next,
			   OUTPTR_LIST * output_val_list,
			   REGU_VARIABLE_LIST regu_val_list,
			   HEAP_CACHE_ATTRINFO * cache_key,
			   HEAP_CACHE_ATTRINFO * cache_pred,
			   HEAP_CACHE_ATTRINFO * cache_rest)
{
  ACCESS_SPEC_TYPE *spec;
  HFID *hfid;
  OID *cls_oid;
  int attrnum;

  spec = pt_make_access_spec (scan_type, access, indexptr,
			      where_key, where_pred);
  if (spec == NULL)
    {
      return NULL;
    }

  assert (class_ != NULL);

  /* Make sure we have a lock on this class */
  if (locator_fetch_class (class_, S_LOCK) == NULL)
    {
      PT_ERRORc (parser, flat, er_msg ());
      return NULL;
    }

  hfid = sm_get_heap (class_);
  if (hfid == NULL)
    {
      return NULL;
    }

  cls_oid = WS_OID (class_);
  if (cls_oid == NULL || OID_ISNULL (cls_oid))
    {
      return NULL;
    }

  spec->s.cls_node.cls_regu_list_key = attr_list_key;
  spec->s.cls_node.cls_regu_list_pred = attr_list_pred;
  spec->s.cls_node.cls_regu_list_rest = attr_list_rest;
  spec->s.cls_node.cls_regu_list_pk_next = pk_next;
  spec->s.cls_node.cls_output_val_list = output_val_list;
  spec->s.cls_node.cls_regu_val_list = regu_val_list;
  spec->s.cls_node.hfid = *hfid;
  spec->s.cls_node.cls_oid = *cls_oid;

  spec->s.cls_node.num_attrs_key = pt_cnt_attrs (attr_list_key);
  spec->s.cls_node.attrids_key =
    regu_int_array_alloc (spec->s.cls_node.num_attrs_key);


  assert_release (spec->s.cls_node.num_attrs_key != 0
		  || attr_list_key == NULL);

  attrnum = 0;
  /* for multi-column index, need to modify attr_id */
  pt_fill_in_attrid_array (attr_list_key,
			   spec->s.cls_node.attrids_key, &attrnum);
  spec->s.cls_node.cache_key = cache_key;
  spec->s.cls_node.num_attrs_pred = pt_cnt_attrs (attr_list_pred);
  spec->s.cls_node.attrids_pred =
    regu_int_array_alloc (spec->s.cls_node.num_attrs_pred);
  attrnum = 0;
  pt_fill_in_attrid_array (attr_list_pred,
			   spec->s.cls_node.attrids_pred, &attrnum);
  spec->s.cls_node.cache_pred = cache_pred;
  spec->s.cls_node.num_attrs_rest = pt_cnt_attrs (attr_list_rest);
  spec->s.cls_node.attrids_rest =
    regu_int_array_alloc (spec->s.cls_node.num_attrs_rest);
  attrnum = 0;
  pt_fill_in_attrid_array (attr_list_rest,
			   spec->s.cls_node.attrids_rest, &attrnum);
  spec->s.cls_node.cache_rest = cache_rest;

  return spec;
}


/*
 * pt_make_list_access_spec () - Create an initialized
 *                               ACCESS_SPEC_TYPE TARGET_LIST structure
 *   return:
 *   xasl(in):
 *   access(in):
 *   indexptr(in):
 *   where_pred(in):
 *   attr_list_pred(in):
 *   attr_list_rest(in):
 */
static ACCESS_SPEC_TYPE *
pt_make_list_access_spec (XASL_NODE * xasl,
			  ACCESS_METHOD access,
			  INDX_INFO * indexptr,
			  PRED_EXPR * where_pred,
			  REGU_VARIABLE_LIST attr_list_pred,
			  REGU_VARIABLE_LIST attr_list_rest)
{
  ACCESS_SPEC_TYPE *spec;

  if (!xasl)
    {
      return NULL;
    }

  spec = pt_make_access_spec (TARGET_LIST, access,
			      indexptr, NULL, where_pred);

  if (spec)
    {
      spec->s.list_node.list_regu_list_pred = attr_list_pred;
      spec->s.list_node.list_regu_list_rest = attr_list_rest;
      spec->s.list_node.xasl_node = xasl;
    }

  return spec;
}


/*
 * pt_to_pos_descr () - Translate PT_SORT_SPEC node to QFILE_TUPLE_VALUE_POSITION node
 *   return:
 *   parser(in):
 *   pos_p(out):
 *   node(in):
 *   root(in):
 *   referred_node(in/out): optional parameter to get real name or expression node
 *                          referred by a position
 */
void
pt_to_pos_descr (PARSER_CONTEXT * parser, QFILE_TUPLE_VALUE_POSITION * pos_p,
		 PT_NODE * node, PT_NODE * root, PT_NODE ** referred_node)
{
  PT_NODE *temp;
  char *node_str = NULL;
  int i;

  pos_p->pos_no = -1;		/* init */

  if (referred_node != NULL)
    {
      *referred_node = NULL;
    }

  switch (root->node_type)
    {
    case PT_SELECT:

      if (node->node_type == PT_EXPR || node->node_type == PT_DOT_)
	{
	  unsigned int save_custom;

	  save_custom = parser->custom_print;	/* save */
	  parser->custom_print |= PT_CONVERT_RANGE;

	  node_str = parser_print_tree (parser, node);

	  parser->custom_print = save_custom;	/* restore */
	}

      /* when do lex analysis, will CHECK nodes in groupby or orderby list
       * whether in select_item list by alias and positions,if yes,some
       * substitution will done,so can not just compare by node_type,
       * alias_print also be considered. As function resolve_alias_in_name_node(),
       * two round comparison will be done : first compare with node_type, if not
       * found, second round check will execute if alias_print is not NULL,
       * compare it with select_item whose alias_print is also not NULL.*/

      /* first round search */
      i = 1;			/* PT_SORT_SPEC pos_no start from 1 */
      for (temp = root->info.query.q.select.list; temp != NULL;
	   temp = temp->next)
	{
	  if (node->node_type == temp->node_type)
	    {
	      if (node->node_type == PT_NAME)
		{
		  if (pt_name_equal (parser, node, temp))
		    {
		      pos_p->pos_no = i;
		    }
		}
	      else if (node->node_type == PT_EXPR
		       || node->node_type == PT_DOT_)
		{
		  if (pt_str_compare
		      (node_str, parser_print_tree (parser, temp),
		       CASE_INSENSITIVE) == 0)
		    {
		      pos_p->pos_no = i;
		    }
		}
	    }
	  else if (pt_check_compatible_node_for_orderby (parser, temp, node))
	    {
	      pos_p->pos_no = i;
	    }

	  if (pos_p->pos_no == -1)
	    {			/* not found match */
	      if (node->node_type == PT_VALUE && node->alias_print == NULL)
		{
		  assert_release (node->node_type == PT_VALUE
				  && node->type_enum == PT_TYPE_INTEGER);
		  if (node->node_type == PT_VALUE
		      && node->type_enum == PT_TYPE_INTEGER)
		    {
		      if (node->info.value.data_value.i == i)
			{
			  pos_p->pos_no = i;

			  if (referred_node != NULL)
			    {
			      *referred_node = temp;
			    }
			}
		    }
		}
	    }

	  if (pos_p->pos_no != -1)
	    {
	      break;		/* found match */
	    }

	  i++;
	}

      /*if not found, second round search in select items with alias_print */
      if (pos_p->pos_no == -1 && node->alias_print != NULL)
	{
	  for (i = 1, temp = root->info.query.q.select.list; temp != NULL;
	       temp = temp->next, i++)
	    {
	      if (temp->alias_print == NULL)
		{
		  continue;
		}

	      if (pt_str_compare (node->alias_print, temp->alias_print,
				  CASE_INSENSITIVE) == 0)
		{
		  pos_p->pos_no = i;
		  break;
		}
	    }
	}

      break;

    case PT_UNION:
    case PT_INTERSECTION:
    case PT_DIFFERENCE:
      pt_to_pos_descr (parser, pos_p, node, root->info.query.q.union_.arg1,
		       referred_node);
      break;

    default:
      /* an error */
      break;
    }

}


/*
 * pt_to_sort_list () - Translate a list of PT_SORT_SPEC nodes
 *                      to SORT_LIST list
 *   return:
 *   parser(in):
 *   node_list(in):
 *   col_list(in):
 *   sort_mode(in):
 */
static SORT_LIST *
pt_to_sort_list (PARSER_CONTEXT * parser, PT_NODE * node_list,
		 PT_NODE * col_list, SORT_LIST_MODE sort_mode)
{
  SORT_LIST *sort_list, *sort, *lastsort;
  PT_NODE *node, *expr, *col;
  int i, k;
  int adjust_for_hidden_col_from = -1;
  DB_TYPE dom_type = DB_TYPE_NULL;

  sort_list = sort = lastsort = NULL;
  i = 0;			/* SORT_LIST pos_no start from 0 */

  /* check if a hidden column is in the select list; if it is the case, store
     the position in 'adjust_for_hidden_col_from' - index starting from 1
     !! Only one column is supported! If we deal with more than two columns
     then we deal with SELECT ... FOR UPDATE and we must skip the check
     This adjustement is needed for UPDATE statements with SELECT subqueries,
     executed on broker; in this case,
     the class OID field in select list is marked as hidden, and the
     coresponding sort value is skipped in 'qdata_get_valptr_type_list', but
     the sorting position is not adjusted - this code anticipates the
     problem */
  if (sort_mode == SORT_LIST_ORDERBY)
    {
      for (col = col_list, k = 1; col && adjust_for_hidden_col_from != -2;
	   col = col->next, k++)
	{
	  switch (col->node_type)
	    {
	    case PT_NAME:
	      if (col->info.name.hidden_column)
		{
		  if (adjust_for_hidden_col_from != -1)
		    {
		      adjust_for_hidden_col_from = -2;
		    }
		  else
		    {
		      adjust_for_hidden_col_from = k;
		    }
		  break;
		}

	    default:
	      break;
	    }
	}
    }

  for (node = node_list; node != NULL; node = node->next)
    {
      /* safe guard: invalid parse tree */
      if (node->node_type != PT_SORT_SPEC ||
	  (expr = node->info.sort_spec.expr) == NULL)
	{
	  regu_set_error_with_zero_args (ER_REGU_SYSTEM);
	  return NULL;
	}

      /* check for end-of-sort */
      if (node->info.sort_spec.pos_descr.pos_no <= 0)
	{
	  if (sort_mode == SORT_LIST_AFTER_ISCAN ||
	      sort_mode == SORT_LIST_ORDERBY)
	    {			/* internal error */
	      if (!pt_has_error (parser))
		{
		  PT_INTERNAL_ERROR (parser, "generate order_by");
		}
	      return NULL;
	    }
	  else if (sort_mode == SORT_LIST_AFTER_GROUPBY)
	    {
	      /* i-th GROUP BY element does not appear in the select list.
	       * stop building sort_list */
	      break;
	    }
	}

      if (sort_mode == SORT_LIST_GROUPBY)
	{
	  col = expr;
	}
      else
	{
	  /* get domain from corresponding column node */
	  for (col = col_list, k = 1; col; col = col->next, k++)
	    {
	      if (node->info.sort_spec.pos_descr.pos_no == k)
		{
		  break;	/* match */
		}
	    }
	}

      if (col && col->type_enum != PT_TYPE_NONE)
	{			/* is resolved */
	  TP_DOMAIN *col_domain;

	  col_domain = pt_xasl_node_to_domain (parser, col);

	  /* internal error */
	  if (col_domain == NULL)
	    {
	      if (!pt_has_error (parser))
		{
		  PT_INTERNAL_ERROR
		    (parser,
		     (sort_mode == SORT_LIST_AFTER_ISCAN)
		     ? "generate after_iscan"
		     : (sort_mode == SORT_LIST_ORDERBY)
		     ? "generate order_by"
		     : (sort_mode == SORT_LIST_GROUPBY)
		     ? "generate group_by" : "generate after_group_by");
		}
	      return NULL;
	    }

	  dom_type = TP_DOMAIN_TYPE (col_domain);
	}

      /* GROUP BY ?  or  ORDER BY ? are not allowed */

      if (node->info.sort_spec.expr->node_type == PT_HOST_VAR
	  && dom_type == DB_TYPE_VARIABLE)
	{
	  if (sort_mode == SORT_LIST_ORDERBY)
	    {
	      PT_ERRORmf (parser, col,
			  MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_NO_ORDERBY_ALLOWED,
			  pt_short_print (parser, col));
	      return NULL;
	    }
	  else if (sort_mode == SORT_LIST_GROUPBY)
	    {
	      PT_ERRORmf (parser, expr,
			  MSGCAT_SET_PARSER_SEMANTIC,
			  MSGCAT_SEMANTIC_NO_GROUPBY_ALLOWED,
			  pt_short_print (parser, expr));
	      return NULL;
	    }
	}

      sort = regu_sort_list_alloc ();
      if (!sort)
	{
	  regu_set_error_with_zero_args (ER_REGU_SYSTEM);
	  return NULL;
	}

      /* set values */
      sort->s_order =
	(node->info.sort_spec.asc_or_desc == PT_ASC) ? S_ASC : S_DESC;
      sort->s_nulls = pt_to_null_ordering (node);
      sort->pos_descr = node->info.sort_spec.pos_descr;

      /* PT_SORT_SPEC pos_no start from 1, SORT_LIST pos_no start from 0 */
      if (sort_mode == SORT_LIST_GROUPBY)
	{
	  /* set i-th position */
	  sort->pos_descr.pos_no = i++;
	}
      else
	{
	  sort->pos_descr.pos_no--;
	  if (adjust_for_hidden_col_from > -1)
	    {
	      assert (sort_mode == SORT_LIST_ORDERBY);
	      /* adjust for hidden column */
	      if (node->info.sort_spec.pos_descr.pos_no >=
		  adjust_for_hidden_col_from)
		{
		  sort->pos_descr.pos_no--;
		  assert (sort->pos_descr.pos_no >= 0);
		}
	    }
	}

      /* link up */
      if (sort_list)
	{
	  lastsort->next = sort;
	}
      else
	{
	  sort_list = sort;
	}

      lastsort = sort;
    }

  return sort_list;
}


/*
 * pt_to_after_iscan () - Translate a list of after iscan PT_SORT_SPEC nodes
 *                        to SORT_LIST list
 *   return:
 *   parser(in):
 *   iscan_list(in):
 *   root(in):
 */
static SORT_LIST *
pt_to_after_iscan (PARSER_CONTEXT * parser, PT_NODE * iscan_list,
		   PT_NODE * root)
{
  return pt_to_sort_list (parser, iscan_list,
			  pt_get_select_list (parser, root),
			  SORT_LIST_AFTER_ISCAN);
}


/*
 * pt_to_orderby () - Translate a list of order by PT_SORT_SPEC nodes
 *                    to SORT_LIST list
 *   return:
 *   parser(in):
 *   order_list(in):
 *   root(in):
 */
SORT_LIST *
pt_to_orderby (PARSER_CONTEXT * parser, PT_NODE * order_list, PT_NODE * root)
{
  return pt_to_sort_list (parser, order_list,
			  pt_get_select_list (parser, root),
			  SORT_LIST_ORDERBY);
}


/*
 * pt_to_groupby () - Translate a list of group by PT_SORT_SPEC nodes
 *                    to SORT_LIST list.(ALL ascending)
 *   return:
 *   parser(in):
 *   group_list(in):
 *   root(in):
 */
static SORT_LIST *
pt_to_groupby (PARSER_CONTEXT * parser, PT_NODE * group_list, PT_NODE * root)
{
  return pt_to_sort_list (parser, group_list,
			  pt_get_select_list (parser, root),
			  SORT_LIST_GROUPBY);
}


/*
 * pt_to_after_groupby () - Translate a list of after group by PT_SORT_SPEC
 *                          nodes to SORT_LIST list.(ALL ascending)
 *   return:
 *   parser(in):
 *   group_list(in):
 *   query(in):
 */
static SORT_LIST *
pt_to_after_groupby (PARSER_CONTEXT * parser, PT_NODE * group_list,
		     PT_NODE * query)
{
  SORT_LIST *sort_list;

  assert (query->node_type == PT_SELECT);

  /* if we have rollup, we do not skip the order by */
  if (query->info.query.q.select.group_by->with_rollup)
    {
      return NULL;		/* give up */
    }

  sort_list = pt_to_sort_list (parser, group_list,
			       pt_get_select_list (parser, query),
			       SORT_LIST_AFTER_GROUPBY);

  return sort_list;
}


/*
 * set_has_objs () - set dbvalptr to the DB_VALUE form of val
 *   return: nonzero if set has some objs, zero otherwise
 *   seq(in): a set/seq db_value
 */
static int
set_has_objs (DB_SET * seq)
{
  int found = 0, i, siz;
  DB_VALUE elem;

  siz = db_seq_size (seq);
  for (i = 0; i < siz && !found; i++)
    {
      if (db_set_get (seq, i, &elem) < 0)
	{
	  return 0;
	}

      if (DB_VALUE_DOMAIN_TYPE (&elem) == DB_TYPE_OBJECT)
	{
	  found = 1;
	}

      db_value_clear (&elem);
    }

  return found;
}


/*
 * pt_make_regu_hostvar () - takes a pt_node of host variable and make
 *                           a regu_variable of host variable reference
 *   return:
 *   parser(in/out):
 *   node(in):
 */
static REGU_VARIABLE *
pt_make_regu_hostvar (PARSER_CONTEXT * parser, const PT_NODE * node)
{
  REGU_VARIABLE *regu;
  UNUSED_VAR DB_VALUE *val;

  regu = regu_var_alloc ();
  if (regu)
    {
      assert (node->info.host_var.index < parser->host_var_count);
      val = &(parser->host_variables[node->info.host_var.index]);

      regu->type = TYPE_POS_VALUE;
      regu->value.val_pos = node->info.host_var.index;
    }
  else
    {
      regu = NULL;
    }

  return regu;
}

/*
 * pt_make_regu_constant () - takes a db_value and db_type and makes
 *                            a regu_variable constant
 *   return: A NULL return indicates an error occurred
 *   parser(in):
 *   db_value(in/out):
 *   node(in):
 */
static REGU_VARIABLE *
pt_make_regu_constant (PARSER_CONTEXT * parser, DB_VALUE * db_value,
		       UNUSED_ARG const PT_NODE * node)
{
  REGU_VARIABLE *regu = NULL;
  DB_VALUE tmp_val;
  DB_TYPE typ;
  int is_null;
  DB_SET *set = NULL;

  db_make_null (&tmp_val);
  if (db_value)
    {
      regu = regu_var_alloc ();
      if (regu)
	{
	  regu->type = TYPE_DBVAL;
	  typ = DB_VALUE_DOMAIN_TYPE (db_value);
	  is_null = DB_IS_NULL (db_value);
	  if (is_null)
	    {
	      regu->value.dbvalptr = db_value;
	    }
	  else if (typ == DB_TYPE_OBJECT)
	    {
	      OID *oid;

	      oid = ws_identifier (DB_GET_OBJECT (db_value));
	      if (oid == NULL)
		{
		  db_value_put_null (db_value);
		  regu->value.dbvalptr = db_value;
		}
	      else
		{
		  db_make_object (db_value, ws_mop (oid, NULL));
		  regu->value.dbvalptr = db_value;
		}
	    }
	  else if (pr_is_set_type (typ)
		   && (set = db_get_set (db_value)) != NULL
		   && set_has_objs (set))
	    {
#if 1				/* TODO - trace */
	      assert (false);
#endif
	      return NULL;
	    }
	  else
	    {
	      regu->value.dbvalptr = db_value;
	    }

	  /* db_value may be in a pt_node that will be freed before mapping
	   * the xasl to a stream. This makes sure that we have captured
	   * the contents of the variable. It also uses the in-line db_value
	   * of a regu variable, saving xasl space.
	   */
	  db_value = regu->value.dbvalptr;
	  regu->value.dbvalptr = NULL;

	  DB_MAKE_NULL (&regu->value.dbval);
	  db_value_clone (db_value, &regu->value.dbval);

	  /* we need to register the dbvalue within the regu constant
	   * as an orphan that the parser should free later. We can't
	   * free it until after the xasl has been packed.
	   */
	  pt_register_orphan_db_value (parser, &regu->value.dbval);

	  (void) pr_clear_value (&tmp_val);
	}
    }

  return regu;
}


/*
 * pt_make_regu_arith () - takes a regu_variable pair,
 *                         and makes an regu arith type
 *   return: A NULL return indicates an error occurred
 *   arg1(in):
 *   arg2(in):
 *   arg3(in):
 *   op(in):
 */
REGU_VARIABLE *
pt_make_regu_arith (REGU_VARIABLE * arg1, REGU_VARIABLE * arg2,
		    REGU_VARIABLE * arg3, OPERATOR_TYPE op)
{
  REGU_VARIABLE *regu = NULL;
  ARITH_TYPE *arith;
  DB_VALUE *dbval;

  arith = regu_arith_alloc ();
  dbval = regu_dbval_alloc ();
  regu = regu_var_alloc ();

  if (arith == NULL || dbval == NULL || regu == NULL)
    {
      return NULL;
    }

  regu_dbval_type_init (dbval, DB_TYPE_VARIABLE);
  arith->value = dbval;
  arith->opcode = op;
  arith->leftptr = arg1;
  arith->rightptr = arg2;
  arith->thirdptr = arg3;
  arith->pred = NULL;
  arith->rand_seed = NULL;
  regu->type = TYPE_INARITH;
  regu->value.arithptr = arith;

  return regu;
}

/*
 * pt_make_regu_pred () - takes a pred expr and makes a special arith
 *			  regu variable, with T_PREDICATE as opcode,
 *			  that holds the predicate expression.
 *
 *   return: A NULL return indicates an error occurred
 *   pred(in):
 */
static REGU_VARIABLE *
pt_make_regu_pred (PRED_EXPR * pred)
{
  REGU_VARIABLE *regu = NULL;
  ARITH_TYPE *arith = NULL;
  DB_VALUE *dbval = NULL;

  if (pred == NULL)
    {
      return NULL;
    }

  arith = regu_arith_alloc ();
  dbval = regu_dbval_alloc ();
  regu = regu_var_alloc ();

  if (arith == NULL || dbval == NULL || regu == NULL)
    {
      return NULL;
    }

  regu_dbval_type_init (dbval, DB_TYPE_INTEGER);
  arith->value = dbval;
  arith->opcode = T_PREDICATE;
  arith->leftptr = NULL;
  arith->rightptr = NULL;
  arith->thirdptr = NULL;
  arith->pred = pred;
  arith->rand_seed = NULL;
  regu->type = TYPE_INARITH;
  regu->value.arithptr = arith;

  return regu;
}

/*
 * pt_make_function () - takes a pt_data_type and a regu variable and makes
 *                       a regu function
 *   return: A NULL return indicates an error occurred
 *   parser(in):
 *   function_code(in):
 *   arg_list(in):
 *   result_type(in):
 *   node(in):
 */
static REGU_VARIABLE *
pt_make_function (UNUSED_ARG PARSER_CONTEXT * parser, int function_code,
		  const REGU_VARIABLE_LIST arg_list,
		  const DB_TYPE result_type, UNUSED_ARG const PT_NODE * node)
{
  REGU_VARIABLE *regu;

  regu = regu_var_alloc ();
  if (regu == NULL)
    {
      return NULL;
    }

  regu->type = TYPE_FUNC;
  regu->value.funcp = regu_func_alloc ();

  if (regu->value.funcp)
    {
      regu->value.funcp->operand = arg_list;
      regu->value.funcp->ftype = (FUNC_TYPE) function_code;

      regu_dbval_type_init (regu->value.funcp->value, result_type);
    }

  return regu;
}


/*
 * pt_function_to_regu () - takes a PT_FUNCTION and converts to a regu_variable
 *   return: A NULL return indicates an error occurred
 *   parser(in):
 *   function(in/out):
 *
 * Note :
 * currently only aggregate functions are known and handled
 */
static REGU_VARIABLE *
pt_function_to_regu (PARSER_CONTEXT * parser, PT_NODE * function)
{
  REGU_VARIABLE *regu = NULL;
  DB_VALUE *dbval;
  bool is_aggregate;
  REGU_VARIABLE_LIST args;
  DB_TYPE result_type = DB_TYPE_SEQUENCE;

  is_aggregate = pt_is_aggregate_function (parser, function);

  if (is_aggregate)
    {
      /* This procedure assumes that pt_to_aggregate ()
       * has already run, setting up the DB_VALUE for the aggregate value. */
      dbval = (DB_VALUE *) function->etc;
      if (dbval)
	{
	  regu = regu_var_alloc ();

	  if (regu)
	    {
	      regu->type = TYPE_CONSTANT;
	      regu->value.dbvalptr = dbval;
	    }
	  else
	    {
	      PT_ERROR (parser, function,
			msgcat_message (MSGCAT_CATALOG_RYE,
					MSGCAT_SET_PARSER_SEMANTIC,
					MSGCAT_SEMANTIC_OUT_OF_MEMORY));
	      return NULL;
	    }
	}
      else
	{
	  PT_ERRORm (parser, function, MSGCAT_SET_PARSER_RUNTIME,
		     MSGCAT_RUNTIME_NESTED_AGGREGATE);
	}
    }
  else
    {
      /* change the generic code to the server side generic code */
      assert (function->info.function.function_type != PT_GENERIC);

      if (function->info.function.function_type < F_TOP_TABLE_FUNC)
	{
	  args = pt_to_regu_variable_list (parser,
					   function->info.function.arg_list,
					   UNBOX_AS_TABLE, NULL, NULL);
	}
      else
	{
	  args = pt_to_regu_variable_list (parser,
					   function->info.function.arg_list,
					   UNBOX_AS_VALUE, NULL, NULL);
	}

      switch (function->info.function.function_type)
	{
	case F_SEQUENCE:
#if 0				/* TODO - */
	  assert (false);	/* should not reach here */
#endif
	  result_type = DB_TYPE_SEQUENCE;
	  break;
	case F_IDXKEY:
	  result_type = DB_TYPE_VARIABLE;
	  break;
	case F_INSERT_SUBSTRING:
	case F_ELT:
	  result_type = pt_node_to_db_type (function);
	  break;
	default:
	  PT_ERRORf (parser, function,
		     "Internal error in generate(%d)", __LINE__);
	}

      if (args)
	{
	  regu = pt_make_function (parser,
				   function->info.function.function_type,
				   args, result_type, function);
	}
    }

  return regu;
}

/*
 * pt_make_regu_subquery () - Creates a regu variable that executes a
 *			      sub-query and stores its results.
 *
 * return      : Pointer to generated regu variable.
 * parser (in) : Parser context.
 * xasl (in)   : XASL node for sub-query.
 * unbox (in)  : UNBOX value (as table or as value).
 * node (in)   : Parse tree node for sub-query.
 */
static REGU_VARIABLE *
pt_make_regu_subquery (PARSER_CONTEXT * parser, XASL_NODE * xasl,
		       const UNBOX unbox, const PT_NODE * node)
{
  REGU_VARIABLE *regu = NULL;
  QFILE_SORTED_LIST_ID *srlist_id = NULL;

  if (xasl)
    {
      regu = regu_var_alloc ();
      if (regu == NULL)
	{
	  return NULL;
	}

      /* set as linked to regu var */
      XASL_SET_FLAG (xasl, XASL_LINK_TO_REGU_VARIABLE);
      REGU_VARIABLE_XASL (regu) = xasl;

      xasl->is_single_tuple = (unbox != UNBOX_AS_TABLE);
      if (xasl->is_single_tuple)
	{
	  if (!xasl->single_tuple)
	    {
	      xasl->single_tuple = pt_make_val_list (parser, node);
	    }

	  if (xasl->single_tuple)
	    {
	      regu->type = TYPE_CONSTANT;
	      regu->value.dbvalptr = xasl->single_tuple->valp->val;
	    }
	  else
	    {
	      PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_OUT_OF_MEMORY);
	      regu = NULL;
	    }
	}
      else
	{
	  srlist_id = regu_srlistid_alloc ();
	  if (srlist_id)
	    {
	      regu->type = TYPE_LIST_ID;
	      regu->value.srlist_id = srlist_id;
	      srlist_id->list_id = xasl->list_id;
	    }
	  else
	    {
	      regu = NULL;
	    }
	}
    }

  return regu;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_make_regu_insert () - Creates a regu variable that executes an insert
 *			    statement and stored the OID of inserted object.
 *
 * return	  : Pointer to generated regu variable.
 * parser (in)	  : Parser context.
 * statement (in) : Parse tree node for insert statement.
 */
static REGU_VARIABLE *
pt_make_regu_insert (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  XASL_NODE *xasl = NULL;
  REGU_VARIABLE *regu = NULL;

  if (statement == NULL || statement->node_type != PT_INSERT)
    {
      assert (false);
      return NULL;
    }

  /* Generate xasl for insert statement */
  xasl = pt_to_insert_xasl (NULL, parser, statement);
  if (xasl == NULL)
    {
      return NULL;
    }

  /* Create the value to store the inserted object */
  xasl->proc.insert.obj_oid = db_value_create ();
  if (xasl->proc.insert.obj_oid == NULL)
    {
      return NULL;
    }

  regu = regu_var_alloc ();
  if (regu == NULL)
    {
      return regu;
    }

  /* set as linked to regu var */
  XASL_SET_FLAG (xasl, XASL_LINK_TO_REGU_VARIABLE);
  REGU_VARIABLE_XASL (regu) = xasl;
  regu->type = TYPE_CONSTANT;
  regu->value.dbvalptr = xasl->proc.insert.obj_oid;

  return regu;
}
#endif

/*
 * pt_set_numbering_node_etc_pre () -
 *   return:
 *   parser(in):
 *   node(in/out):
 *   arg(in/out):
 *   continue_walk(in):
 */
static PT_NODE *
pt_set_numbering_node_etc_pre (UNUSED_ARG PARSER_CONTEXT * parser,
			       PT_NODE * node, void *arg, int *continue_walk)
{
  SET_NUMBERING_NODE_ETC_INFO *info = (SET_NUMBERING_NODE_ETC_INFO *) arg;

  if (node->node_type == PT_EXPR)
    {
      if (info->instnum_valp && (node->info.expr.op == PT_INST_NUM
				 || node->info.expr.op == PT_ROWNUM))
	{
	  if (*info->instnum_valp == NULL)
	    {
	      *info->instnum_valp = regu_dbval_alloc ();
	    }

	  node->etc = *info->instnum_valp;
	}

      if (info->ordbynum_valp && node->info.expr.op == PT_ORDERBY_NUM)
	{
	  if (*info->ordbynum_valp == NULL)
	    {
	      *info->ordbynum_valp = regu_dbval_alloc ();
	    }

	  node->etc = *info->ordbynum_valp;
	}
    }
  else if (node->node_type != PT_FUNCTION)
    {
      *continue_walk = PT_STOP_WALK;
    }

  return node;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_get_numbering_node_etc () - get the DB_VALUE reference of the
 *				  ORDERBY_NUM expression
 * return : node
 * parser (in) : parser context
 * node (in)   : node
 * arg (in)    : pointer to DB_VALUE *
 * continue_walk (in) :
 */
PT_NODE *
pt_get_numbering_node_etc (PARSER_CONTEXT * parser, PT_NODE * node, void *arg,
			   int *continue_walk)
{
  if (node == NULL)
    {
      return node;
    }

  if (PT_IS_EXPR_NODE (node) && node->info.expr.op == PT_ORDERBY_NUM)
    {
      DB_VALUE **val_ptr = (DB_VALUE **) arg;
      *continue_walk = PT_STOP_WALK;
      *val_ptr = (DB_VALUE *) node->etc;
    }

  return node;
}
#endif

/*
 * pt_set_numbering_node_etc () - set etc values of parse tree nodes INST_NUM
 *                                and ORDERBY_NUM to pointers of corresponding
 *                                reguvars from XASL node
 *   return:
 *   parser(in):
 *   node_list(in):
 *   instnum_valp(out):
 *   ordbynum_valp(out):
 */
void
pt_set_numbering_node_etc (PARSER_CONTEXT * parser, PT_NODE * node_list,
			   DB_VALUE ** instnum_valp,
			   DB_VALUE ** ordbynum_valp)
{
  PT_NODE *node, *save_node, *save_next;
  SET_NUMBERING_NODE_ETC_INFO info;

  if (node_list)
    {
      info.instnum_valp = instnum_valp;
      info.ordbynum_valp = ordbynum_valp;

      for (node = node_list; node; node = node->next)
	{
	  save_node = node;

	  CAST_POINTER_TO_NODE (node);

	  if (node)
	    {
	      /* save and cut-off node link */
	      save_next = node->next;
	      node->next = NULL;

	      (void) parser_walk_tree (parser, node,
				       pt_set_numbering_node_etc_pre, &info,
				       pt_continue_walk, NULL);

	      node->next = save_next;
	    }

	  node = save_node;
	}			/* for (node = ...) */
    }
}


/*
 * pt_make_regu_numbering () - make a regu_variable constant for
 *                             inst_num() and orderby_num()
 *   return:
 *   parser(in):
 *   node(in):
 */
static REGU_VARIABLE *
pt_make_regu_numbering (PARSER_CONTEXT * parser, const PT_NODE * node)
{
  REGU_VARIABLE *regu = NULL;
  DB_VALUE *dbval;

  /* 'etc' field of PT_NODEs which belong to inst_num() or orderby_num()
     expression was set to points to XASL_INSTNUM_VAL() or XASL_ORDBYNUM_VAL()
     by pt_set_numbering_node_etc() */
  dbval = (DB_VALUE *) node->etc;

  if (dbval)
    {
      regu = regu_var_alloc ();
      if (regu)
	{
	  regu->type = TYPE_CONSTANT;
	  regu->value.dbvalptr = dbval;
	}
    }
  else
    {
      PT_INTERNAL_ERROR (parser, "generate inst_num or orderby_num");
    }

  return regu;
}


/*
 * pt_to_misc_operand () - maps PT_MISC_TYPE of PT_LEADING, PT_TRAILING,
 *      PT_BOTH, PT_YEAR, PT_MONTH, PT_DAY, PT_HOUR, PT_MINUTE, and PT_SECOND
 *      to the corresponding MISC_OPERAND
 *   return:
 *   regu(in/out):
 *   misc_specifier(in):
 */
static void
pt_to_misc_operand (REGU_VARIABLE * regu, PT_MISC_TYPE misc_specifier)
{
  if (regu && regu->value.arithptr)
    {
      regu->value.arithptr->misc_operand =
	pt_misc_to_qp_misc_operand (misc_specifier);
    }
}


/*
 * pt_make_prim_data_type () -
 *   return:
 *   parser(in):
 *   e(in):
 */
PT_NODE *
pt_make_prim_data_type (PARSER_CONTEXT * parser, PT_TYPE_ENUM e)
{
  PT_NODE *dt = NULL;

  dt = parser_new_node (parser, PT_DATA_TYPE);

  if (dt == NULL)
    {
      assert (false);
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return NULL;
    }

  dt->type_enum = e;
  dt->info.data_type.precision = DB_DEFAULT_PRECISION;

  if (PT_HAS_COLLATION (e))
    {
      dt->info.data_type.collation_id = LANG_COERCIBLE_COLL;
    }

  switch (e)
    {
    case PT_TYPE_INTEGER:
    case PT_TYPE_BIGINT:
    case PT_TYPE_DOUBLE:
    case PT_TYPE_DATE:
    case PT_TYPE_TIME:
    case PT_TYPE_DATETIME:
      dt->data_type = NULL;
      break;

    case PT_TYPE_VARCHAR:
      dt->info.data_type.precision = DB_MAX_VARCHAR_PRECISION;
      break;

    case PT_TYPE_VARBIT:
      dt->info.data_type.precision = DB_MAX_VARBIT_PRECISION;
      break;

    case PT_TYPE_NUMERIC:
      dt->info.data_type.precision = DB_DEFAULT_NUMERIC_PRECISION;
      dt->info.data_type.dec_scale = DB_DEFAULT_NUMERIC_SCALE;
      break;

    default:
      /* error handling is required.. */
      parser_free_tree (parser, dt);
      dt = NULL;
    }

  return dt;
}

/*
 * pt_make_empty_string() -
 *   return:
 *   parser(in):
 *   e(in):
 */
static PT_NODE *
pt_make_empty_string (PARSER_CONTEXT * parser, PT_TYPE_ENUM e)
{
  PT_NODE *empty_str;

  empty_str = parser_new_node (parser, PT_VALUE);
  if (empty_str == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      return NULL;
    }

  empty_str->type_enum = e;
  empty_str->info.value.string_type = ' ';

  empty_str->info.value.data_value.str =
    pt_append_nulstring (parser, NULL, "");

  PT_NODE_PRINT_VALUE_TO_TEXT (parser, empty_str);

  empty_str->data_type = pt_make_prim_data_type (parser, e);

  return empty_str;
}

/*
 * pt_to_regu_variable () - converts a parse expression tree to regu_variables
 *   return:
 *   parser(in):
 *   node(in): should be something that will evaluate to an expression
 *             of names and constant
 *   unbox(in):
 */
REGU_VARIABLE *
pt_to_regu_variable (PARSER_CONTEXT * parser, PT_NODE * node, UNBOX unbox)
{
  REGU_VARIABLE *regu = NULL;
  XASL_NODE *xasl;
  DB_VALUE *value, *val = NULL;
  PT_NODE *save_next = NULL;
  REGU_VARIABLE *r1 = NULL, *r2 = NULL, *r3 = NULL;
  PT_NODE *empty_str = NULL;

  if (node == NULL)
    {
      val = regu_dbval_alloc ();
      if (db_value_domain_init (val, DB_TYPE_VARCHAR,
				DB_DEFAULT_PRECISION,
				DB_DEFAULT_SCALE) == NO_ERROR)
	{
	  regu = pt_make_regu_constant (parser, val, NULL);
	}

      if (regu == NULL)
	{
	  if (!pt_has_error (parser))
	    {
	      PT_INTERNAL_ERROR (parser, "generate var");
	    }
	}

      if (val != NULL)
	{
	  pr_clear_value (val);
	}

      return regu;
    }

  CAST_POINTER_TO_NODE (node);

  if (node != NULL && node->type_enum == PT_TYPE_LOGICAL
      && (node->node_type == PT_EXPR || node->node_type == PT_VALUE))
    {
      regu = pt_make_regu_pred (pt_to_pred_expr (parser, node));
    }
  else if (node != NULL)
    {
      /* save and cut-off node link */
      save_next = node->next;
      node->next = NULL;

      switch (node->node_type)
	{
	case PT_DOT_:
	  /* a path expression. XASL fetch procs or equivalent should
	   * already be done for it
	   * return the regu variable for the right most name in the
	   * path expression.
	   */
	  switch (node->info.dot.arg2->info.name.meta_class)
	    {
	    case PT_NORMAL:
	    default:
	      regu = pt_attribute_to_regu (parser, node->info.dot.arg2);
	      break;
	    }
	  break;

	case PT_EXPR:
	  if (node->info.expr.op == PT_FUNCTION_HOLDER)
	    {
	      regu = pt_function_to_regu (parser, node->info.expr.arg1);
	      return regu;
	    }

	  switch (node->info.expr.op)
	    {
	    case PT_PLUS:
	    case PT_MINUS:
	    case PT_TIMES:
	    case PT_DIVIDE:
	    case PT_MOD:
	    case PT_MODULUS:
	    case PT_POWER:
	    case PT_SHA_TWO:
	    case PT_ROUND:
	    case PT_LOG:
	    case PT_TRUNC:
	    case PT_POSITION:
	    case PT_FINDINSET:
	    case PT_LPAD:
	    case PT_RPAD:
	    case PT_REPLACE:
	    case PT_TRANSLATE:
	    case PT_MONTHS_BETWEEN:
	    case PT_FORMAT:
	    case PT_ATAN:
	    case PT_ATAN2:
	    case PT_DATE_FORMAT:
	    case PT_STR_TO_DATE:
	    case PT_TIME_FORMAT:
	    case PT_DATEDIFF:
	    case PT_TIMEDIFF:
	    case PT_TO_NUMBER:
	    case PT_LEAST:
	    case PT_GREATEST:
	    case PT_CASE:
	    case PT_NULLIF:
	    case PT_COALESCE:
	    case PT_NVL:
	    case PT_DECODE:
	    case PT_STRCAT:
	    case PT_BIT_AND:
	    case PT_BIT_OR:
	    case PT_BIT_XOR:
	    case PT_BITSHIFT_LEFT:
	    case PT_BITSHIFT_RIGHT:
	    case PT_DIV:
	    case PT_IFNULL:
	    case PT_CONCAT:
	    case PT_LEFT:
	    case PT_RIGHT:
	    case PT_STRCMP:
	    case PT_REPEAT:
	    case PT_WEEKF:
	    case PT_MAKEDATE:
	    case PT_INDEX_PREFIX:
	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      if (node->info.expr.op == PT_CONCAT
		  && node->info.expr.arg2 == NULL)
		{
		  r2 = NULL;
		}
	      else
		{
		  r2 = pt_to_regu_variable (parser,
					    node->info.expr.arg2, unbox);
		}

	      if (node->info.expr.op == PT_ATAN
		  && node->info.expr.arg2 == NULL)
		{
		  /* If ATAN has only one arg, treat it as an unary op */
		  r2 = r1;
		  r1 = NULL;
		}

	      if (node->info.expr.op == PT_DATE_FORMAT
		  || node->info.expr.op == PT_STR_TO_DATE
		  || node->info.expr.op == PT_TIME_FORMAT
		  || node->info.expr.op == PT_FORMAT
		  || node->info.expr.op == PT_INDEX_PREFIX)
		{
		  r3 = pt_to_regu_variable (parser,
					    node->info.expr.arg3, unbox);
		}
	      break;

	    case PT_DEFAULTF:
	      {
		PT_NODE *arg1;

		arg1 = node->info.expr.arg1;

		r1 = NULL;
		if (arg1 != NULL
		    && arg1->node_type == PT_NAME
		    && arg1->info.name.default_value != NULL)
		  {
		    r2 = pt_to_regu_variable (parser,
					      arg1->info.name.default_value,
					      unbox);
		  }
		else
		  {
		    /* is view column */
		    r2 = NULL;
		  }
	      }
	      break;

	    case PT_UNIX_TIMESTAMP:
	      r1 = NULL;
	      if (node->info.expr.arg1 != NULL)
		{
		  r2 = pt_to_regu_variable (parser,
					    node->info.expr.arg1, unbox);
		}
	      else
		{
		  r2 = NULL;
		}

	      break;

	    case PT_UNARY_MINUS:
	    case PT_RAND:
	    case PT_DRAND:
	    case PT_RANDOM:
	    case PT_DRANDOM:
	    case PT_FLOOR:
	    case PT_CEIL:
	    case PT_SIGN:
	    case PT_EXP:
	    case PT_SQRT:
	    case PT_ACOS:
	    case PT_ASIN:
	    case PT_COS:
	    case PT_SIN:
	    case PT_TAN:
	    case PT_COT:
	    case PT_DEGREES:
	    case PT_DATEF:
	    case PT_TIMEF:
	    case PT_RADIANS:
	    case PT_LN:
	    case PT_LOG2:
	    case PT_LOG10:
	    case PT_ABS:
	    case PT_OCTET_LENGTH:
	    case PT_BIT_LENGTH:
	    case PT_CHAR_LENGTH:
	    case PT_LOWER:
	    case PT_UPPER:
	    case PT_HEX:
	    case PT_ASCII:
	    case PT_LAST_DAY:
	    case PT_CAST:
	    case PT_EXTRACT:
	    case PT_BIN:
	    case PT_MD5:
	    case PT_SHA_ONE:
	    case PT_SPACE:
	    case PT_BIT_NOT:
	    case PT_REVERSE:
	    case PT_BIT_COUNT:
	    case PT_ISNULL:
	    case PT_TYPEOF:
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
	    case PT_INET_ATON:
	    case PT_INET_NTOA:
	    case PT_TO_BASE64:
	    case PT_FROM_BASE64:
	      r1 = NULL;

	      r2 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);

	      break;

	    case PT_LIKE_LOWER_BOUND:
	    case PT_LIKE_UPPER_BOUND:
	    case PT_CHR:
	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      if (!node->info.expr.arg2)
		{
		  r2 = NULL;
		}
	      else
		{
		  r2 =
		    pt_to_regu_variable (parser, node->info.expr.arg2, unbox);
		}

	      break;

	    case PT_DATE_ADD:
	    case PT_DATE_SUB:
	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      r2 = pt_to_regu_variable (parser, node->info.expr.arg2, unbox);
	      /* store the info.expr.qualifier which is the unit parameter
	         into a constant regu variable */
	      val = regu_dbval_alloc ();
	      if (val)
		{
		  assert (node->info.expr.arg3->node_type == PT_EXPR);
		  DB_MAKE_INT (val,
			       node->info.expr.arg3->info.expr.qualifier);
		  r3 = pt_make_regu_constant (parser, val, NULL);
		}

	      break;

	    case PT_ADDDATE:
	    case PT_SUBDATE:
	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      r2 = pt_to_regu_variable (parser, node->info.expr.arg2, unbox);

	      break;

	    case PT_INSTR:
	    case PT_SUBSTRING:
	    case PT_NVL2:
	    case PT_CONCAT_WS:
	    case PT_FIELD:
	    case PT_LOCATE:
	    case PT_MID:
	    case PT_SUBSTRING_INDEX:
	    case PT_MAKETIME:
	    case PT_INDEX_CARDINALITY:
	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      if (node->info.expr.arg2 == NULL
		  && node->info.expr.op == PT_CONCAT_WS)
		{
		  r2 = NULL;
		}
	      else
		{
		  r2 = pt_to_regu_variable (parser,
					    node->info.expr.arg2, unbox);
		}

	      if (node->info.expr.arg3 == NULL
		  && (node->info.expr.op == PT_LOCATE ||
		      node->info.expr.op == PT_SUBSTRING))
		{
		  r3 = NULL;
		}
	      else
		{
		  PT_NODE *arg3_next = NULL;

		  /* save and cut-off node link */
		  if (node->info.expr.op == PT_FIELD && node->info.expr.arg3)
		    {
		      arg3_next = node->info.expr.arg3->next;
		      node->info.expr.arg3->next = NULL;
		    }

		  r3 = pt_to_regu_variable (parser,
					    node->info.expr.arg3, unbox);

		  /* restore node link */
		  if (node->info.expr.op == PT_FIELD && node->info.expr.arg3)
		    {
		      node->info.expr.arg3->next = arg3_next;
		    }
		}

	      break;

	    case PT_CONV:
	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      r2 = pt_to_regu_variable (parser, node->info.expr.arg2, unbox);
	      r3 = pt_to_regu_variable (parser, node->info.expr.arg3, unbox);

	      break;

	    case PT_TO_CHAR:
	    case PT_TO_DATE:
	    case PT_TO_TIME:
	    case PT_TO_DATETIME:
	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      r2 = pt_to_regu_variable (parser, node->info.expr.arg2, unbox);
	      r3 = pt_to_regu_variable (parser, node->info.expr.arg3, unbox);

	      break;

	    case PT_SYS_DATE:
	    case PT_SYS_TIME:
	    case PT_SYS_DATETIME:
	    case PT_UTC_TIME:
	    case PT_UTC_DATE:
	    case PT_PI:
	    case PT_HA_STATUS:
	    case PT_SHARD_GROUPID:
	    case PT_SHARD_LOCKNAME:
	    case PT_SHARD_NODEID:
	    case PT_EXPLAIN:
	    case PT_LIST_DBS:
	      /* go ahead */

	      break;

	    case PT_IF:
	      r1 = pt_to_regu_variable (parser, node->info.expr.arg2, unbox);
	      r2 = pt_to_regu_variable (parser, node->info.expr.arg3, unbox);

	      break;

	    case PT_WIDTH_BUCKET:
	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      r2 = pt_to_regu_variable (parser, node->info.expr.arg2, unbox);
	      r3 = pt_to_regu_variable (parser, node->info.expr.arg3, unbox);

	      break;

	    case PT_TRACE_STATS:
	      r1 = NULL;
	      r2 = NULL;
	      r3 = NULL;

	      break;

	    default:
	      break;
	    }

	  switch (node->info.expr.op)
	    {
	    case PT_PLUS:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_ADD);
	      break;

	    case PT_MINUS:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_SUB);
	      break;

	    case PT_TIMES:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_MUL);
	      break;

	    case PT_DIVIDE:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_DIV);
	      break;

	    case PT_UNARY_MINUS:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_UNMINUS);
	      break;

	    case PT_BIT_NOT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_BIT_NOT);
	      break;

	    case PT_BIT_AND:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_BIT_AND);
	      break;

	    case PT_BIT_OR:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_BIT_OR);
	      break;

	    case PT_BIT_XOR:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_BIT_XOR);
	      break;

	    case PT_BITSHIFT_LEFT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_BITSHIFT_LEFT);
	      break;

	    case PT_BITSHIFT_RIGHT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_BITSHIFT_RIGHT);
	      break;

	    case PT_DIV:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_INTDIV);
	      break;

	    case PT_IF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_IF);
	      if (regu == NULL)
		{
		  break;
		}
	      regu->value.arithptr->pred =
		pt_to_pred_expr (parser, node->info.expr.arg1);
	      break;

	    case PT_IFNULL:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_IFNULL);
	      break;

	    case PT_CONCAT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_CONCAT);
	      break;

	    case PT_CONCAT_WS:
	      regu = pt_make_regu_arith (r1, r2, r3, T_CONCAT_WS);
	      break;

	    case PT_FIELD:
#if 1				/* TODO - */
	      if (r1 == NULL)
		{
		  break;
		}
#endif
	      REGU_VARIABLE_SET_FLAG (r1, REGU_VARIABLE_FIELD_NESTED);
	      regu = pt_make_regu_arith (r1, r2, r3, T_FIELD);

	      if (node->info.expr.arg3 && node->info.expr.arg3->next
		  && node->info.expr.arg3->next->info.value.data_value.i == 1)
		{
		  /* bottom level T_FIELD */
		  REGU_VARIABLE_SET_FLAG (regu, REGU_VARIABLE_FIELD_COMPARE);
		}
	      break;

	    case PT_LEFT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_LEFT);
	      break;

	    case PT_RIGHT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_RIGHT);
	      break;

	    case PT_REPEAT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_REPEAT);
	      break;

	    case PT_TIME_FORMAT:
	      regu = pt_make_regu_arith (r1, r2, r3, T_TIME_FORMAT);
	      break;

	    case PT_DATE_SUB:
	      regu = pt_make_regu_arith (r1, r2, r3, T_DATE_SUB);
	      break;

	    case PT_DATE_ADD:
	      regu = pt_make_regu_arith (r1, r2, r3, T_DATE_ADD);
	      break;

	    case PT_LOCATE:
	      regu = pt_make_regu_arith (r1, r2, r3, T_LOCATE);
	      break;

	    case PT_MID:
	      regu = pt_make_regu_arith (r1, r2, r3, T_MID);
	      break;

	    case PT_STRCMP:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_STRCMP);
	      break;

	    case PT_REVERSE:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_REVERSE);
	      break;

	    case PT_BIT_COUNT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_BIT_COUNT);
	      break;

	    case PT_ISNULL:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_ISNULL);
	      break;

	    case PT_YEARF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_YEAR);
	      break;

	    case PT_MONTHF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_MONTH);
	      break;

	    case PT_DAYOFMONTH:
	    case PT_DAYF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_DAY);
	      break;

	    case PT_HOURF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_HOUR);
	      break;

	    case PT_MINUTEF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_MINUTE);
	      break;

	    case PT_SECONDF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_SECOND);
	      break;

	    case PT_DEFAULTF:
	      regu = pt_make_regu_arith (NULL, r2, NULL, T_DEFAULT);
	      break;

	    case PT_UNIX_TIMESTAMP:
	      regu = pt_make_regu_arith (NULL, r2, NULL, T_UNIX_TIMESTAMP);
	      break;

	    case PT_LIKE_LOWER_BOUND:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_LIKE_LOWER_BOUND);
	      break;

	    case PT_LIKE_UPPER_BOUND:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_LIKE_UPPER_BOUND);
	      break;

	    case PT_QUARTERF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_QUARTER);
	      break;

	    case PT_WEEKDAY:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_WEEKDAY);
	      break;

	    case PT_DAYOFWEEK:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_DAYOFWEEK);
	      break;

	    case PT_DAYOFYEAR:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_DAYOFYEAR);
	      break;

	    case PT_TODAYS:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_TODAYS);
	      break;

	    case PT_FROMDAYS:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_FROMDAYS);
	      break;

	    case PT_TIMETOSEC:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_TIMETOSEC);
	      break;

	    case PT_SECTOTIME:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_SECTOTIME);
	      break;

	    case PT_MAKEDATE:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_MAKEDATE);
	      break;

	    case PT_MAKETIME:
	      regu = pt_make_regu_arith (r1, r2, r3, T_MAKETIME);
	      break;

	    case PT_WEEKF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_WEEK);
	      break;

	    case PT_SCHEMA:
	    case PT_DATABASE:
	      {
		PT_NODE *dbname_val;
		char *dbname;

		dbname_val = parser_new_node (parser, PT_VALUE);
		if (dbname_val == NULL)
		  {
		    PT_INTERNAL_ERROR (parser, "allocate new node");
		    return NULL;
		  }

		dbname = db_get_database_name ();
		if (dbname)
		  {
		    dbname_val->type_enum = PT_TYPE_VARCHAR;
		    dbname_val->info.value.string_type = ' ';

		    dbname_val->info.value.data_value.str =
		      pt_append_nulstring (parser, NULL, dbname);
		    PT_NODE_PRINT_VALUE_TO_TEXT (parser, dbname_val);

		    db_string_free (dbname);

		    assert (dbname_val->data_type == NULL);
		    dbname_val->data_type = pt_make_prim_data_type (parser,
								    PT_TYPE_VARCHAR);
		    if (dbname_val->data_type == NULL)
		      {
			PT_INTERNAL_ERROR (parser, "allocate new node");
			return NULL;
		      }
		  }
		else
		  {
		    PT_SET_NULL_NODE (dbname_val);
		  }

		regu = pt_to_regu_variable (parser, dbname_val, unbox);
		break;
	      }
	    case PT_VERSION:
	      {
		PT_NODE *dbversion_val;
		char *dbversion;

		dbversion_val = parser_new_node (parser, PT_VALUE);
		if (dbversion_val == NULL)
		  {
		    PT_INTERNAL_ERROR (parser, "allocate new node");
		    return NULL;
		  }

		dbversion = db_get_database_version ();
		if (dbversion)
		  {
		    dbversion_val->type_enum = PT_TYPE_VARCHAR;
		    dbversion_val->info.value.string_type = ' ';

		    dbversion_val->info.value.data_value.str =
		      pt_append_nulstring (parser, NULL, dbversion);
		    PT_NODE_PRINT_VALUE_TO_TEXT (parser, dbversion_val);

		    db_string_free (dbversion);

		    assert (dbversion_val->data_type == NULL);
		    dbversion_val->data_type = pt_make_prim_data_type (parser,
								       PT_TYPE_VARCHAR);
		    if (dbversion_val->data_type == NULL)
		      {
			PT_INTERNAL_ERROR (parser, "allocate new node");
			return NULL;
		      }
		  }
		else
		  {
		    PT_SET_NULL_NODE (dbversion_val);
		  }

		regu = pt_to_regu_variable (parser, dbversion_val, unbox);
		break;
	      }

	    case PT_MOD:
	    case PT_MODULUS:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_MOD);
	      parser->etc = (void *) 1;
	      break;

	    case PT_PI:
	      regu = pt_make_regu_arith (NULL, NULL, NULL, T_PI);
	      break;

	    case PT_RAND:
	      regu = pt_make_regu_arith (NULL, r2, NULL, T_RAND);
	      break;

	    case PT_DRAND:
	      regu = pt_make_regu_arith (NULL, r2, NULL, T_DRAND);
	      break;

	    case PT_RANDOM:
	      regu = pt_make_regu_arith (NULL, r2, NULL, T_RANDOM);
	      break;

	    case PT_DRANDOM:
	      regu = pt_make_regu_arith (NULL, r2, NULL, T_DRANDOM);
	      break;

	    case PT_FLOOR:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_FLOOR);
	      break;

	    case PT_CEIL:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_CEIL);
	      break;

	    case PT_SIGN:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_SIGN);
	      break;

	    case PT_POWER:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_POWER);
	      break;

	    case PT_ROUND:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_ROUND);
	      break;

	    case PT_LOG:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_LOG);
	      break;

	    case PT_EXP:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_EXP);
	      break;

	    case PT_SQRT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_SQRT);
	      break;

	    case PT_COS:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_COS);
	      break;

	    case PT_SIN:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_SIN);
	      break;

	    case PT_TAN:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_TAN);
	      break;

	    case PT_COT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_COT);
	      break;

	    case PT_ACOS:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_ACOS);
	      break;

	    case PT_ASIN:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_ASIN);
	      break;

	    case PT_ATAN:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_ATAN);
	      break;

	    case PT_ATAN2:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_ATAN2);
	      break;

	    case PT_DEGREES:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_DEGREES);
	      break;

	    case PT_DATEF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_DATE);
	      break;

	    case PT_TIMEF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_TIME);
	      break;

	    case PT_RADIANS:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_RADIANS);
	      break;

	    case PT_LN:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_LN);
	      break;

	    case PT_LOG2:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_LOG2);
	      break;

	    case PT_LOG10:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_LOG10);
	      break;

	    case PT_FORMAT:
	      regu = pt_make_regu_arith (r1, r2, r3, T_FORMAT);
	      break;

	    case PT_DATE_FORMAT:
	      regu = pt_make_regu_arith (r1, r2, r3, T_DATE_FORMAT);
	      break;

	    case PT_STR_TO_DATE:
	      regu = pt_make_regu_arith (r1, r2, r3, T_STR_TO_DATE);
	      break;

	    case PT_ADDDATE:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_ADDDATE);
	      break;

	    case PT_DATEDIFF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_DATEDIFF);
	      break;

	    case PT_TIMEDIFF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_TIMEDIFF);
	      break;

	    case PT_SUBDATE:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_SUBDATE);
	      break;

	    case PT_TRUNC:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_TRUNC);
	      break;

	    case PT_ABS:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_ABS);
	      break;

	    case PT_CHR:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_CHR);
	      break;

	    case PT_INSTR:
	      regu = pt_make_regu_arith (r1, r2, r3, T_INSTR);
	      break;

	    case PT_POSITION:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_POSITION);
	      break;

	    case PT_FINDINSET:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_FINDINSET);
	      break;

	    case PT_SUBSTRING:
	      regu = pt_make_regu_arith (r1, r2, r3, T_SUBSTRING);
	      assert (node->node_type == PT_EXPR);
	      pt_to_misc_operand (regu, node->info.expr.qualifier);
	      break;

	    case PT_SUBSTRING_INDEX:
	      regu = pt_make_regu_arith (r1, r2, r3, T_SUBSTRING_INDEX);
	      break;

	    case PT_OCTET_LENGTH:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_OCTET_LENGTH);
	      break;

	    case PT_BIT_LENGTH:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_BIT_LENGTH);
	      break;

	    case PT_CHAR_LENGTH:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_CHAR_LENGTH);
	      break;

	    case PT_LOWER:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_LOWER);
	      break;

	    case PT_UPPER:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_UPPER);
	      break;

	    case PT_HEX:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_HEX);
	      break;

	    case PT_ASCII:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_ASCII);
	      break;

	    case PT_CONV:
	      regu = pt_make_regu_arith (r1, r2, r3, T_CONV);
	      break;

	    case PT_BIN:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_BIN);
	      break;

	    case PT_MD5:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_MD5);
	      break;

	    case PT_SHA_ONE:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_SHA_ONE);
	      break;

	    case PT_SHA_TWO:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_SHA_TWO);
	      break;

	    case PT_FROM_BASE64:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_FROM_BASE64);
	      break;

	    case PT_TO_BASE64:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_TO_BASE64);
	      break;

	    case PT_SPACE:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_SPACE);
	      break;

	    case PT_LTRIM:
	      if (node->info.expr.arg2 == NULL)
		{
		  empty_str = pt_make_empty_string (parser, PT_TYPE_VARCHAR);
		}
	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      r2 = (node->info.expr.arg2)
		? pt_to_regu_variable (parser, node->info.expr.arg2, unbox)
		: pt_to_regu_variable (parser, empty_str, unbox);

	      regu = pt_make_regu_arith (r1, r2, NULL, T_LTRIM);
#if 1				/* TODO - */
	      if (regu == NULL)
		{
		  break;
		}
#endif
	      if (node->info.expr.arg2 == NULL)
		{
		  parser_free_tree (parser, empty_str);
		}
	      break;

	    case PT_RTRIM:
	      if (node->info.expr.arg2 == NULL)
		{
		  empty_str = pt_make_empty_string (parser, PT_TYPE_VARCHAR);
		}

	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      r2 = (node->info.expr.arg2)
		? pt_to_regu_variable (parser, node->info.expr.arg2, unbox)
		: pt_to_regu_variable (parser, empty_str, unbox);

	      regu = pt_make_regu_arith (r1, r2, NULL, T_RTRIM);
	      if (node->info.expr.arg2 == NULL)
		{
		  parser_free_tree (parser, empty_str);
		}
	      break;

	    case PT_FROM_UNIXTIME:
	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      r3 = pt_to_regu_variable (parser, node->info.expr.arg3, unbox);
	      r2 = (node->info.expr.arg2)
		? pt_to_regu_variable (parser, node->info.expr.arg2, unbox)
		: NULL;

	      regu = pt_make_regu_arith (r1, r2, r3, T_FROM_UNIXTIME);
	      break;

	    case PT_LPAD:
	      if (node->info.expr.arg3 == NULL)
		{
		  empty_str = pt_make_empty_string (parser, PT_TYPE_VARCHAR);
		}

	      r3 = (node->info.expr.arg3)
		? pt_to_regu_variable (parser, node->info.expr.arg3,
				       unbox)
		: pt_to_regu_variable (parser, empty_str, unbox);
	      regu = pt_make_regu_arith (r1, r2, r3, T_LPAD);
#if 1				/* TODO - */
	      if (regu == NULL)
		{
		  break;
		}
#endif
	      if (node->info.expr.arg3 == NULL)
		{
		  parser_free_tree (parser, empty_str);
		}
	      break;

	    case PT_RPAD:
	      if (node->info.expr.arg3 == NULL)
		{
		  empty_str = pt_make_empty_string (parser, PT_TYPE_VARCHAR);
		}

	      r3 = (node->info.expr.arg3)
		? pt_to_regu_variable (parser, node->info.expr.arg3,
				       unbox)
		: pt_to_regu_variable (parser, empty_str, unbox);
	      regu = pt_make_regu_arith (r1, r2, r3, T_RPAD);
#if 1				/* TODO - */
	      if (regu == NULL)
		{
		  break;
		}
#endif
	      if (node->info.expr.arg3 == NULL)
		{
		  parser_free_tree (parser, empty_str);
		}
	      break;

	    case PT_REPLACE:
	      if (node->info.expr.arg3 == NULL)
		{
		  empty_str = pt_make_empty_string (parser, PT_TYPE_VARCHAR);
		}

	      r3 = (node->info.expr.arg3)
		? pt_to_regu_variable (parser, node->info.expr.arg3,
				       unbox)
		: pt_to_regu_variable (parser, empty_str, unbox);
	      regu = pt_make_regu_arith (r1, r2, r3, T_REPLACE);
#if 1				/* TODO - */
	      if (regu == NULL)
		{
		  break;
		}
#endif
	      if (node->info.expr.arg3 == NULL)
		{
		  parser_free_tree (parser, empty_str);
		}
	      break;

	    case PT_TRANSLATE:
	      if (node->info.expr.arg3 == NULL)
		{
		  empty_str = pt_make_empty_string (parser, PT_TYPE_VARCHAR);
		}

	      r3 = (node->info.expr.arg3)
		? pt_to_regu_variable (parser, node->info.expr.arg3,
				       unbox)
		: pt_to_regu_variable (parser, empty_str, unbox);
	      regu = pt_make_regu_arith (r1, r2, r3, T_TRANSLATE);
#if 1				/* TODO - */
	      if (regu == NULL)
		{
		  break;
		}
#endif
	      if (node->info.expr.arg3 == NULL)
		{
		  parser_free_tree (parser, empty_str);
		}
	      break;

	    case PT_LAST_DAY:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_LAST_DAY);
	      break;

	    case PT_MONTHS_BETWEEN:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_MONTHS_BETWEEN);
	      break;

	    case PT_SYS_DATE:
	      regu = pt_make_regu_arith (NULL, NULL, NULL, T_SYS_DATE);
	      break;

	    case PT_SYS_TIME:
	      regu = pt_make_regu_arith (NULL, NULL, NULL, T_SYS_TIME);
	      break;

	    case PT_SYS_DATETIME:
	      regu = pt_make_regu_arith (NULL, NULL, NULL, T_SYS_DATETIME);
	      break;

	    case PT_UTC_TIME:
	      regu = pt_make_regu_arith (NULL, NULL, NULL, T_UTC_TIME);
	      break;

	    case PT_UTC_DATE:
	      regu = pt_make_regu_arith (NULL, NULL, NULL, T_UTC_DATE);
	      break;

	    case PT_HA_STATUS:
	      regu = pt_make_regu_arith (NULL, NULL, NULL, T_HA_STATUS);
	      break;

	    case PT_SHARD_GROUPID:
	      regu = pt_make_regu_arith (NULL, NULL, NULL, T_SHARD_GROUPID);
	      break;

	    case PT_SHARD_LOCKNAME:
	      regu = pt_make_regu_arith (NULL, NULL, NULL, T_SHARD_LOCKNAME);
	      break;

	    case PT_SHARD_NODEID:
	      regu = pt_make_regu_arith (NULL, NULL, NULL, T_SHARD_NODEID);
	      break;

	    case PT_EXPLAIN:
	      {
		COMPILE_CONTEXT *contextp;
		PT_NODE *plan_val = NULL;

		contextp = &(parser->context);
		if (contextp->sql_plan_text == NULL)
		  {
		    assert (false);
		    PT_INTERNAL_ERROR (parser,
				       "parser->context->sql_plan_text == NULL");
		    return NULL;
		  }

		plan_val = parser_new_node (parser, PT_VALUE);
		if (plan_val == NULL)
		  {
		    PT_INTERNAL_ERROR (parser, "allocate new node");
		    return NULL;
		  }

		plan_val->type_enum = PT_TYPE_VARCHAR;
		plan_val->info.value.string_type = ' ';

		plan_val->info.value.data_value.str =
		  pt_append_nulstring (parser, NULL, contextp->sql_plan_text);
		PT_NODE_PRINT_VALUE_TO_TEXT (parser, plan_val);

		assert (plan_val->data_type == NULL);
		plan_val->data_type =
		  pt_make_prim_data_type (parser, PT_TYPE_VARCHAR);
		if (plan_val->data_type == NULL)
		  {
		    PT_INTERNAL_ERROR (parser, "allocate new node");
		    return NULL;
		  }

		regu = pt_to_regu_variable (parser, plan_val, unbox);

		parser_free_node (parser, plan_val);
		break;
	      }

	    case PT_CURRENT_USER:
	      {
		PT_NODE *current_user_val;
		char *username;

		username = au_user_name ();
		if (username == NULL)
		  {
		    PT_INTERNAL_ERROR (parser, "get user name");
		    return NULL;
		  }

		current_user_val = parser_new_node (parser, PT_VALUE);
		if (current_user_val == NULL)
		  {
		    db_string_free (username);
		    PT_INTERNAL_ERROR (parser, "allocate new node");
		    return NULL;
		  }

		current_user_val->type_enum = PT_TYPE_VARCHAR;
		current_user_val->info.value.string_type = ' ';
		current_user_val->info.value.data_value.str =
		  pt_append_nulstring (parser, NULL, username);
		PT_NODE_PRINT_VALUE_TO_TEXT (parser, current_user_val);

		assert (current_user_val->data_type == NULL);
		current_user_val->data_type = pt_make_prim_data_type (parser,
								      PT_TYPE_VARCHAR);
		if (current_user_val->data_type == NULL)
		  {
		    db_string_free (username);
		    PT_INTERNAL_ERROR (parser, "allocate new node");
		    return NULL;
		  }

		regu = pt_to_regu_variable (parser, current_user_val, unbox);

		db_string_free (username);
		parser_free_node (parser, current_user_val);
		break;
	      }

	    case PT_USER:
	      {
		char *user = NULL;
		PT_NODE *current_user_val = NULL;

		user = db_get_user_and_host_name ();
		current_user_val = parser_new_node (parser, PT_VALUE);
		if (current_user_val == NULL)
		  {
		    PT_INTERNAL_ERROR (parser, "allocate new node");
		    free_and_init (user);
		    return NULL;
		  }
		current_user_val->type_enum = PT_TYPE_VARCHAR;
		current_user_val->info.value.string_type = ' ';

		current_user_val->info.value.data_value.str =
		  pt_append_nulstring (parser, NULL, user);
		PT_NODE_PRINT_VALUE_TO_TEXT (parser, current_user_val);

		assert (current_user_val->data_type == NULL);
		current_user_val->data_type = pt_make_prim_data_type (parser,
								      PT_TYPE_VARCHAR);
		if (current_user_val->data_type == NULL)
		  {
		    PT_INTERNAL_ERROR (parser, "allocate new node");
		    free_and_init (user);
		    return NULL;
		  }

		regu = pt_to_regu_variable (parser, current_user_val, unbox);

		free_and_init (user);
		parser_free_node (parser, current_user_val);
		break;
	      }

	    case PT_TO_CHAR:
	      regu = pt_make_regu_arith (r1, r2, r3, T_TO_CHAR);
	      break;

	    case PT_TO_DATE:
	      regu = pt_make_regu_arith (r1, r2, r3, T_TO_DATE);
	      break;

	    case PT_TO_TIME:
	      regu = pt_make_regu_arith (r1, r2, r3, T_TO_TIME);
	      break;

	    case PT_TO_DATETIME:
	      regu = pt_make_regu_arith (r1, r2, r3, T_TO_DATETIME);
	      break;

	    case PT_TO_NUMBER:
	      r3 = pt_to_regu_variable (parser, node->info.expr.arg3, unbox);

	      regu = pt_make_regu_arith (r1, r2, r3, T_TO_NUMBER);
	      break;

	    case PT_TRIM:
	      if (node->info.expr.arg2 == NULL)
		{
		  empty_str = pt_make_empty_string (parser, PT_TYPE_VARCHAR);
		}

	      r1 = pt_to_regu_variable (parser, node->info.expr.arg1, unbox);
	      r2 = (node->info.expr.arg2)
		? pt_to_regu_variable (parser, node->info.expr.arg2, unbox)
		: pt_to_regu_variable (parser, empty_str, unbox);

	      regu = pt_make_regu_arith (r1, r2, NULL, T_TRIM);
#if 1				/* TODO - */
	      if (regu == NULL)
		{
		  break;
		}
#endif

	      assert (node->node_type == PT_EXPR);
	      pt_to_misc_operand (regu, node->info.expr.qualifier);

	      if (node->info.expr.arg2 == NULL)
		{
		  parser_free_tree (parser, empty_str);
		}
	      break;

	    case PT_INST_NUM:
	    case PT_ROWNUM:
	    case PT_ORDERBY_NUM:
	      regu = pt_make_regu_numbering (parser, node);
	      break;

	    case PT_LEAST:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_LEAST);
	      break;

	    case PT_GREATEST:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_GREATEST);
	      break;

	    case PT_CAST:
	      {
		TP_DOMAIN *domain = NULL;
		DB_VALUE *cast_val;

		assert (r1 == NULL);
		assert (r2 != NULL);
		assert (node->node_type == PT_EXPR);

		domain = pt_xasl_data_type_to_domain (parser,
						      node->info.
						      expr.cast_type);
		if (domain == NULL)
		  {
		    break;
		  }

		assert (TP_DOMAIN_TYPE (domain) != DB_TYPE_NULL);
		assert (TP_DOMAIN_TYPE (domain) != DB_TYPE_VARIABLE);

		cast_val = regu_dbval_alloc ();
		if (cast_val)
		  {
		    if (db_value_domain_init (cast_val,
					      TP_DOMAIN_TYPE (domain),
					      domain->precision,
					      domain->scale) != NO_ERROR)
		      {
			break;
		      }

		    if (TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (cast_val)))
		      {
			db_string_put_cs_and_collation (cast_val,
							domain->collation_id);
		      }
		  }

		r1 = pt_make_regu_constant (parser, cast_val, NULL);

		regu = pt_make_regu_arith (r1, r2, NULL, T_CAST);
	      }
	      break;

	    case PT_CASE:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_CASE);
	      if (regu == NULL)
		{
		  break;
		}
	      regu->value.arithptr->pred =
		pt_to_pred_expr (parser, node->info.expr.arg3);
	      break;

	    case PT_NULLIF:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_NULLIF);
	      break;

	    case PT_COALESCE:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_COALESCE);
	      break;

	    case PT_NVL:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_NVL);
	      break;

	    case PT_NVL2:
	      regu = pt_make_regu_arith (r1, r2, r3, T_NVL2);
	      break;

	    case PT_DECODE:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_DECODE);
	      if (regu == NULL)
		{
		  break;
		}
	      regu->value.arithptr->pred =
		pt_to_pred_expr (parser, node->info.expr.arg3);
	      break;

	    case PT_EXTRACT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_EXTRACT);
	      assert (node->node_type == PT_EXPR);
	      pt_to_misc_operand (regu, node->info.expr.qualifier);
	      break;

	    case PT_STRCAT:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_STRCAT);
	      break;

	    case PT_LIST_DBS:
	      regu = pt_make_regu_arith (NULL, NULL, NULL, T_LIST_DBS);
	      break;

	    case PT_TYPEOF:
	      regu = pt_make_regu_arith (NULL, r2, NULL, T_TYPEOF);
	      break;

	    case PT_INDEX_CARDINALITY:
	      regu = pt_make_regu_arith (r1, r2, r3, T_INDEX_CARDINALITY);
	      if (parser->parent_proc_xasl != NULL)
		{
		  XASL_SET_FLAG (parser->parent_proc_xasl,
				 XASL_NO_FIXED_SCAN);
		}
	      else
		{
		  /* should not happen */
		  assert (false);
		}
	      break;

	    case PT_INET_ATON:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_INET_ATON);
	      break;

	    case PT_INET_NTOA:
	      regu = pt_make_regu_arith (r1, r2, NULL, T_INET_NTOA);
	      break;

	    case PT_WIDTH_BUCKET:
	      regu = pt_make_regu_arith (r1, r2, r3, T_WIDTH_BUCKET);
	      break;

	    case PT_TRACE_STATS:
	      regu = pt_make_regu_arith (r1, r2, r3, T_TRACE_STATS);
	      break;

	    case PT_INDEX_PREFIX:
	      regu = pt_make_regu_arith (r1, r2, r3, T_INDEX_PREFIX);
	      break;

	    default:
	      break;
	    }

	  break;

	case PT_HOST_VAR:
	  regu = pt_make_regu_hostvar (parser, node);
	  break;

	case PT_NODE_LIST:
	  assert (false);	/* is impossible */
	  regu = NULL;
	  break;

	case PT_VALUE:
	  value = pt_value_to_db (parser, node);
	  if (value)
	    {
	      regu = pt_make_regu_constant (parser, value, node);
	    }
	  break;

	case PT_NAME:
	  if (node->info.name.db_object
	      && node->info.name.meta_class != PT_OID_ATTR)
	    {
#if 1				/* TODO -trace */
	      assert (false);	/* is impossible */
#endif

	      assert (regu == NULL);
	      regu = NULL;
	    }
	  else
	    {
	      regu = pt_attribute_to_regu (parser, node);
	    }

	  if (regu && node->info.name.hidden_column)
	    {
	      REGU_VARIABLE_SET_FLAG (regu, REGU_VARIABLE_HIDDEN_COLUMN);
	    }

	  break;

	case PT_FUNCTION:
	  regu = pt_function_to_regu (parser, node);
	  break;

	case PT_SELECT:
	case PT_UNION:
	case PT_DIFFERENCE:
	case PT_INTERSECTION:
	  xasl = (XASL_NODE *) node->info.query.xasl;
	  if (xasl)
	    {
	      PT_NODE *select_list = pt_get_select_list (parser, node);
	      if (unbox != UNBOX_AS_TABLE
		  && pt_length_of_select_list (select_list,
					       EXCLUDE_HIDDEN_COLUMNS) != 1)
		{
		  PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_RUNTIME,
			      MSGCAT_RUNTIME_WANT_ONE_COL,
			      parser_print_tree (parser, node));
		}

	      regu = pt_make_regu_subquery (parser, xasl, unbox, node);
	    }
	  break;

#if defined (ENABLE_UNUSED_FUNCTION)
	case PT_INSERT:
	  regu = pt_make_regu_insert (parser, node);
	  break;
#endif

	default:
	  /* force error */
	  regu = NULL;
	}

      node->next = save_next;
    }

  if (regu == NULL)
    {
      if (er_errid () == NO_ERROR && !pt_has_error (parser))
	{
	  PT_INTERNAL_ERROR (parser, "generate var");
	}
    }

  if (val != NULL)
    {
      pr_clear_value (val);
    }

  return regu;
}


/*
 * pt_to_regu_variable_list () - converts a parse expression tree list
 *                               to regu_variable_list
 *   return: A NULL return indicates an error occurred
 *   parser(in):
 *   node_list(in):
 *   unbox(in):
 *   value_list(in):
 *   attr_offsets(in):
 */
static REGU_VARIABLE_LIST
pt_to_regu_variable_list (PARSER_CONTEXT * parser,
			  PT_NODE * node_list,
			  UNBOX unbox,
			  VAL_LIST * value_list, int *attr_offsets)
{
  REGU_VARIABLE_LIST regu_list = NULL;
  REGU_VARIABLE_LIST *tail = NULL;
  REGU_VARIABLE *regu;
  PT_NODE *node, *save_next;
  int i = 0;

  tail = &regu_list;

  for (node = node_list; node != NULL; node = node->next)
    {
      /* save and cut-off node link */
      save_next = node->next;
      node->next = NULL;

      (*tail) = regu_varlist_alloc ();
      regu = pt_to_regu_variable (parser, node, unbox);

      if (attr_offsets && value_list && regu)
	{
	  regu->vfetch_to = pt_index_value (value_list, attr_offsets[i]);
	}
      i++;

      if (regu && *tail)
	{
	  (*tail)->value = *regu;
	  tail = &(*tail)->next;
	}
      else
	{
	  regu_list = NULL;
	  node->next = save_next;	/* restore node link */
	  break;
	}

      node->next = save_next;	/* restore node link */
    }

  return regu_list;
}


/*
 * pt_regu_to_dbvalue () -
 *   return:
 *   parser(in):
 *   regu(in):
 */
static DB_VALUE *
pt_regu_to_dbvalue (PARSER_CONTEXT * parser, REGU_VARIABLE * regu)
{
  DB_VALUE *val = NULL;

  if (regu->type == TYPE_CONSTANT)
    {
      val = regu->value.dbvalptr;
    }
  else if (regu->type == TYPE_DBVAL)
    {
      val = &regu->value.dbval;
    }
  else
    {
      if (!pt_has_error (parser))
	{
	  PT_INTERNAL_ERROR (parser, "generate val");
	}
    }

  return val;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_make_position_regu_variable () - converts a parse expression tree list
 *                                     to regu_variable_list
 *   return:
 *   parser(in):
 *   node(in):
 *   i(in):
 */
static REGU_VARIABLE *
pt_make_position_regu_variable (PARSER_CONTEXT * parser,
				const PT_NODE * node, int i)
{
  REGU_VARIABLE *regu = NULL;

  regu = regu_var_alloc ();

  if (regu)
    {
      regu->type = TYPE_POSITION;
      regu->value.pos_descr.pos_no = i;
    }

  return regu;
}
#endif

/*
 * pt_to_position_regu_variable_list () - converts a parse expression tree
 *                                        list to regu_variable_list
 *   return:
 *   parser(in):
 *   node_list(in):
 *   value_list(in):
 *   attr_offsets(in):
 */
static REGU_VARIABLE_LIST
pt_to_position_regu_variable_list (PARSER_CONTEXT * parser,
				   PT_NODE * node_list, VAL_LIST * value_list,
				   int *attr_offsets)
{
  REGU_VARIABLE_LIST regu_list = NULL;
  REGU_VARIABLE_LIST *tail = NULL;
  PT_NODE *node;
  int i = 0;

  tail = &regu_list;

  for (node = node_list; node != NULL; node = node->next)
    {
      (*tail) = regu_varlist_alloc ();

      /* it would be better form to call pt_make_position_regu_variable,
       * but this avoids additional allocation do to regu variable
       * and regu_variable_list bizarreness.
       */
      if (*tail)
	{
	  (*tail)->value.type = TYPE_POSITION;

	  if (attr_offsets)
	    {
	      (*tail)->value.value.pos_descr.pos_no = attr_offsets[i];
	    }
	  else
	    {
	      (*tail)->value.value.pos_descr.pos_no = i;
	    }

	  if (value_list)
	    {
	      if (attr_offsets)
		{
		  (*tail)->value.vfetch_to =
		    pt_index_value (value_list, attr_offsets[i]);
		}
	      else
		{
		  (*tail)->value.vfetch_to = pt_index_value (value_list, i);
		}
	    }

	  tail = &(*tail)->next;
	  i++;
	}
      else
	{
	  PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
		     MSGCAT_SEMANTIC_OUT_OF_MEMORY);
	  regu_list = NULL;
	  break;
	}
    }

  return regu_list;
}

/*
 * pt_to_regu_attr_descr () -
 *   return: int
 *   attr_descr(in): pointer to an attribute descriptor
 *   attr_id(in): attribute id
 *   type(in): attribute type
 */

static REGU_VARIABLE *
pt_to_regu_attr_descr (UNUSED_ARG PARSER_CONTEXT * parser,
		       DB_OBJECT * class_object,
		       HEAP_CACHE_ATTRINFO * cache_attrinfo, PT_NODE * attr)
{
  const char *attr_name = attr->info.name.original;
  int attr_id;
  SM_DOMAIN *smdomain = NULL;
  REGU_VARIABLE *regu;
  ATTR_DESCR *attr_descr;

  if (sm_att_info (class_object, attr_name, &attr_id, &smdomain) != NO_ERROR
      || (smdomain == NULL) || !(regu = regu_var_alloc ()))
    {
      return NULL;
    }

  attr_descr = &regu->value.attr_descr;
  UT_CLEAR_ATTR_DESCR (attr_descr);

  regu->type = TYPE_ATTR_ID;

  attr_descr->id = attr_id;
  attr_descr->cache_attrinfo = cache_attrinfo;

  if (smdomain)
    {
      attr_descr->type = smdomain->type->id;
    }

  return regu;
}


/*
 * pt_attribute_to_regu () - Convert an attribute spec into a REGU_VARIABLE
 *   return:
 *   parser(in):
 *   attr(in):
 *
 * Note :
 * If "current_class" is non-null, use it to create a TYPE_ATTRID REGU_VARIABLE
 * Otherwise, create a TYPE_CONSTANT REGU_VARIABLE pointing to the symbol
 * table's value_list DB_VALUE, in the position matching where attr is
 * found in attribute_list.
 */
static REGU_VARIABLE *
pt_attribute_to_regu (PARSER_CONTEXT * parser, PT_NODE * attr)
{
  REGU_VARIABLE *regu = NULL;
  SYMBOL_INFO *symbols;
  DB_VALUE *dbval = NULL;
  TABLE_INFO *table_info;
  int list_index;

  CAST_POINTER_TO_NODE (attr);

  if (attr && attr->node_type == PT_NAME)
    {
      symbols = parser->symbols;
    }
  else
    {
      symbols = NULL;		/* error */
    }

  if (symbols && attr)
    {
      /* check the current scope first */
      table_info = pt_find_table_info (attr->info.name.spec_id,
				       symbols->table_info);

      if (table_info)
	{
	  /* We have found the attribute at this scope.
	   * If we had not, the attribute must have been a correlated
	   * reference to an attribute at an outer scope. The correlated
	   * case is handled below in this "if" statement's "else" clause.
	   * Determine if this is relative to a particular class
	   * or if the attribute should be relative to the placeholder.
	   */

	  if (symbols->current_class
	      && (table_info->spec_id
		  == symbols->current_class->info.name.spec_id))
	    {
	      /* determine if this is an attribute, or an oid identifier */
	      if (PT_IS_OID_NAME (attr))
		{
#if 1				/* TODO - */
		  if (!PT_NAME_INFO_IS_FLAGED
		      (attr, PT_NAME_INFO_GENERATED_OID))
		    {
		      PT_ERRORmf (parser, attr, MSGCAT_SET_PARSER_SEMANTIC,
				  MSGCAT_SEMANTIC_IS_NOT_DEFINED,
				  pt_short_print (parser, attr));

		      regu = NULL;
		    }
		  else
		    {
#endif
		      regu = regu_var_alloc ();
		      if (regu)
			{
			  regu->type = TYPE_OID;
			}
#if 1				/* TODO - */
		    }
#endif
		}
	      else
		{
		  /* this is an attribute reference */
		  if (symbols->current_class->info.name.db_object)
		    {
		      regu = pt_to_regu_attr_descr
			(parser,
			 symbols->current_class->info.name.db_object,
			 symbols->cache_attrinfo, attr);
		    }
		  else
		    {
		      /* system error, we should have understood this name. */
		      if (!pt_has_error (parser))
			{
			  PT_INTERNAL_ERROR (parser, "generate attr");
			}
		      regu = NULL;
		    }
		}
	    }
	  else if (symbols->current_listfile
		   && (list_index = pt_find_attribute
		       (parser, attr, symbols->current_listfile)) >= 0)
	    {
	      /* add in the listfile attribute offset.  This is used
	       * primarily for server update and insert constraint predicates
	       * because the server update prepends two columns onto the
	       * select list of the listfile.
	       */
	      list_index += symbols->listfile_attr_offset;

	      if (symbols->listfile_value_list)
		{
		  regu = regu_var_alloc ();
		  if (regu)
		    {
		      regu->type = TYPE_CONSTANT;
		      dbval = pt_index_value (symbols->listfile_value_list,
					      list_index);

		      if (dbval)
			{
			  regu->value.dbvalptr = dbval;
			}
		      else
			{
			  regu = NULL;
			}
		    }
		}
	      else
		{
#if 1				/* TODO - */
		  /* system error, we should have understood this name. */
		  if (!pt_has_error (parser))
		    {
		      PT_INTERNAL_ERROR (parser, "generate attr");
		    }
		  regu = NULL;
#else
		  /* here we need the position regu variable to access
		   * the list file directly, as in list access spec predicate
		   * evaluation.
		   */
		  regu = pt_make_position_regu_variable (parser, attr,
							 list_index);
#endif
		}
	    }
	  else
	    {
	      /* Here, we are determining attribute reference information
	       * relative to the list of attribute placeholders
	       * which will be fetched from the class(es). The "type"
	       * of the attribute no longer affects how the placeholder
	       * is referenced.
	       */
#if 1				/* TODO - */
	      if (PT_IS_OID_NAME (attr)
		  && !PT_NAME_INFO_IS_FLAGED (attr,
					      PT_NAME_INFO_GENERATED_OID))
		{
		  PT_ERRORmf (parser, attr, MSGCAT_SET_PARSER_SEMANTIC,
			      MSGCAT_SEMANTIC_IS_NOT_DEFINED,
			      pt_short_print (parser, attr));

		  regu = NULL;
		}
	      else
		{
#endif
		  regu = regu_var_alloc ();
		  if (regu)
		    {
		      regu->type = TYPE_CONSTANT;
		      dbval =
			pt_index_value (table_info->value_list,
					pt_find_attribute (parser, attr,
							   table_info->
							   attribute_list));
		      if (dbval)
			{
			  regu->value.dbvalptr = dbval;
			}
		      else
			{
			  if (PT_IS_OID_NAME (attr))
			    {
			      if (regu)
				{
				  regu->type = TYPE_OID;
				}
			    }
			  else
			    {
			      regu = NULL;
			    }
			}
		    }
#if 1				/* TODO - */
		}
#endif
	    }
	}
      else if ((regu = regu_var_alloc ()))
	{
	  /* The attribute is correlated variable.
	   * Find it in an enclosing scope(s).
	   * Note that this subquery has also just been determined to be
	   * a correlated subquery.
	   */
	  if (symbols->stack == NULL)
	    {
	      if (!pt_has_error (parser))
		{
		  PT_INTERNAL_ERROR (parser, "generate attr");
		}

	      regu = NULL;
	    }
	  else
	    {
	      while (symbols->stack && !table_info)
		{
		  symbols = symbols->stack;
		  /* mark successive enclosing scopes correlated,
		   * until the attribute's "home" is found. */
		  table_info = pt_find_table_info (attr->info.name.spec_id,
						   symbols->table_info);
		}

	      if (table_info)
		{
		  regu->type = TYPE_CONSTANT;
		  dbval =
		    pt_index_value (table_info->value_list,
				    pt_find_attribute (parser, attr,
						       table_info->
						       attribute_list));
		  if (dbval)
		    {
		      regu->value.dbvalptr = dbval;
		    }
		  else
		    {
		      regu = NULL;
		    }
		}
	      else
		{
		  if (!pt_has_error (parser))
		    {
		      PT_INTERNAL_ERROR (parser, "generate attr");
		    }

		  regu = NULL;
		}
	    }
	}
    }
  else
    {
      regu = NULL;
    }

  if (regu == NULL && !pt_has_error (parser))
    {
      PT_INTERNAL_ERROR (parser, "generate attr");
    }

  return regu;
}


/*
 * op_type_to_range () -
 *   return:
 *   op_type(in):
 *   nterms(in):
 */
static RANGE
op_type_to_range (const PT_OP_TYPE op_type, const int nterms)
{
  switch (op_type)
    {
    case PT_EQ:
      return EQ_NA;
    case PT_GT:
      return (nterms > 1) ? GT_LE : GT_INF;
    case PT_GE:
      return (nterms > 1) ? GE_LE : GE_INF;
    case PT_LT:
      return (nterms > 1) ? GE_LT : INF_LT;
    case PT_LE:
      return (nterms > 1) ? GE_LE : INF_LE;
    case PT_BETWEEN:
      return GE_LE;
    case PT_IS_IN:
      return EQ_NA;
    case PT_BETWEEN_AND:
    case PT_BETWEEN_GE_LE:
      return GE_LE;
    case PT_BETWEEN_GE_LT:
      return GE_LT;
    case PT_BETWEEN_GT_LE:
      return GT_LE;
    case PT_BETWEEN_GT_LT:
      return GT_LT;
    case PT_BETWEEN_EQ_NA:
      return EQ_NA;
    case PT_BETWEEN_INF_LE:
      return (nterms > 1) ? GE_LE : INF_LE;
    case PT_BETWEEN_INF_LT:
      return (nterms > 1) ? GE_LT : INF_LT;
    case PT_BETWEEN_GE_INF:
      return (nterms > 1) ? GE_LE : GE_INF;
    case PT_BETWEEN_GT_INF:
      return (nterms > 1) ? GT_LE : GT_INF;
    default:
      return NA_NA;		/* error */
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_to_single_key () - Create an key information(KEY_INFO) in INDX_INFO
 *      structure for index scan with range spec of R_ON, R_FROM and R_TO.
 *   return: 0 on success
 *   parser(in):
 *   term_exprs(in):
 *   nterms(in):
 *   key_infop(out):
 */
static int
pt_to_single_key (PARSER_CONTEXT * parser, PT_NODE ** term_exprs, int nterms,
		  KEY_INFO * key_infop)
{
  PT_NODE *lhs, *rhs, *tmp, *midx_key;
  PT_OP_TYPE op_type;
  REGU_VARIABLE *regu_var;
  int i;

  assert (nterms > 0);

  midx_key = NULL;
  regu_var = NULL;
  key_infop->key_cnt = 0;
  key_infop->key_ranges = NULL;
  key_infop->is_constant = 1;

  for (i = 0; i < nterms; i++)
    {
      /* If nterms > 1, then it should be multi-column index and
         all term_exprs[0 .. nterms - 1] are equality expression.
         (Even though nterms == 1, it can be multi-column index.) */

      /* op type, LHS side and RHS side of this term expression */
      op_type = term_exprs[i]->info.expr.op;
      lhs = term_exprs[i]->info.expr.arg1;
      rhs = term_exprs[i]->info.expr.arg2;

      /* incidentally we may have a term with only one range left out by
         pt_to_index_info(), which semantically is the same as PT_EQ */
      if (op_type == PT_RANGE)
	{
	  assert (PT_IS_EXPR_NODE_WITH_OPERATOR (rhs, PT_BETWEEN_EQ_NA));
	  assert (rhs->or_next == NULL);

	  /* has only one range */
	  rhs = rhs->info.expr.arg1;
	}

      regu_var = pt_to_regu_variable (parser, rhs, UNBOX_AS_VALUE);
      if (regu_var == NULL)
	{
	  goto error;
	}

      if (!VALIDATE_REGU_KEY (regu_var))
	{
	  /* correlated join index case swap LHS and RHS */
	  tmp = rhs;
	  rhs = lhs;
	  lhs = tmp;

	  /* try on RHS */
	  regu_var = pt_to_regu_variable (parser, rhs, UNBOX_AS_VALUE);
	  if (regu_var == NULL || !VALIDATE_REGU_KEY (regu_var))
	    {
	      goto error;
	    }
	}

      /* is the key value constant(value or host variable)? */
      key_infop->is_constant &= (rhs->node_type == PT_VALUE ||
				 rhs->node_type == PT_HOST_VAR);

      /* make one PT_NODE for midx key value
         by concatenating all RHS of the terms */
      midx_key = parser_append_node (pt_point (parser, rhs), midx_key);
    }				/* for (i = 0; i < nterms; i++) */

  assert (midx_key != NULL);

  if (midx_key)
    {
      /* make a idxkey regu variable for multi-column index */
      tmp = parser_new_node (parser, PT_FUNCTION);
      if (tmp == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  goto error;
	}
      tmp->type_enum = PT_TYPE_MAYBE;
      tmp->info.function.function_type = F_IDXKEY;
      tmp->info.function.arg_list = midx_key;
      regu_var = pt_to_regu_variable (parser, tmp, UNBOX_AS_VALUE);
      parser_free_tree (parser, tmp);
      midx_key = NULL;		/* already free */
    }

  /* set KEY_INFO structure */
  key_infop->key_cnt = 1;	/* single range */
  key_infop->key_ranges = regu_keyrange_array_alloc (1);
  if (!key_infop->key_ranges)
    {
      goto error;
    }
  key_infop->key_ranges[0].range = EQ_NA;
  key_infop->key_ranges[0].key1 = regu_var;
  key_infop->key_ranges[0].key2 = NULL;

  return 0;

/* error handling */
error:
  if (midx_key)
    {
      parser_free_tree (parser, midx_key);
    }

  return -1;
}
#endif

/*
 * pt_to_list_key () - Create an key information(KEY_INFO) in INDX_INFO
 * 	structure for index scan with range spec of R_LIST
 *   return: 0 on success
 *   parser(in):
 *   term_exprs(in):
 *   nterms(in):
 *   key_infop(out): Construct a list of key values
 */
static int
pt_to_list_key (PARSER_CONTEXT * parser, PT_NODE ** term_exprs, int nterms,
		KEY_INFO * key_infop)
{
  UNUSED_VAR PT_NODE *lhs, *rhs;
  PT_NODE *elem, *tmp, **midxkey_list;
  PT_OP_TYPE op_type;
  REGU_VARIABLE **regu_var_list, *regu_var;
  int i, j, n_elem;
//  DB_VALUE db_value;
//  DB_COLLECTION *db_collectionp = NULL;

  assert (nterms > 0);

  midxkey_list = NULL;
  regu_var_list = NULL;
  key_infop->key_cnt = 0;
  key_infop->key_ranges = NULL;
  key_infop->is_constant = 1;
  n_elem = 0;

  /* get number of elements of the EQ, IN predicate */
  op_type = term_exprs[nterms - 1]->info.expr.op;
  rhs = term_exprs[nterms - 1]->info.expr.arg2;

  if (op_type == PT_EQ)
    {
      n_elem = 1;
    }
  else if (op_type == PT_IS_IN)
    {
      if (rhs->node_type == PT_EXPR && rhs->info.expr.op == PT_CAST)
	{
	  /* strip CAST operator off */
	  rhs = rhs->info.expr.arg1;
	}

      switch (rhs->node_type)
	{
	case PT_FUNCTION:
	  if (rhs->info.function.function_type != F_SEQUENCE)
	    {
	      goto error;
	    }

	  n_elem = pt_length_of_list (rhs->info.function.arg_list);
	  break;

	default:
	  assert (false);
	  goto error;
	}
    }

  if (n_elem <= 0)
    {
      goto error;
    }

  /* allocate regu variable list and sequence value list */
  regu_var_list = regu_varptr_array_alloc (n_elem);
  if (regu_var_list == NULL)
    {
      goto error;
    }

  midxkey_list = (PT_NODE **) malloc (sizeof (PT_NODE *) * n_elem);
  if (midxkey_list == NULL)
    {
      goto error;
    }
  memset (midxkey_list, 0, sizeof (PT_NODE *) * n_elem);

  for (i = 0; i < nterms; i++)
    {
      /* If nterms > 1, then it should be multi-column index and
         all term_exprs[0 .. nterms - 1] are equality expression.
         (Even though nterms == 1, it can be multi-column index.) */

      /* op type, LHS side and RHS side of this term expression */
      op_type = term_exprs[i]->info.expr.op;
      lhs = term_exprs[i]->info.expr.arg1;
      rhs = term_exprs[i]->info.expr.arg2;

      if (op_type != PT_IS_IN)
	{
	  /* PT_EQ */

	  regu_var = pt_to_regu_variable (parser, rhs, UNBOX_AS_VALUE);
	  if (regu_var == NULL)
	    {
	      goto error;
	    }

	  if (!VALIDATE_REGU_KEY (regu_var))
	    {
	      /* correlated join index case swap LHS and RHS */
	      lhs = term_exprs[i]->info.expr.arg2;
	      rhs = term_exprs[i]->info.expr.arg1;

	      /* try on RHS */
	      regu_var = pt_to_regu_variable (parser, rhs, UNBOX_AS_VALUE);
	      if (regu_var == NULL || !VALIDATE_REGU_KEY (regu_var))
		{
		  goto error;
		}
	    }

	  /* is the key value constant(value or host variable)? */
	  key_infop->is_constant &= (rhs->node_type == PT_VALUE ||
				     rhs->node_type == PT_HOST_VAR);

	  /* make one PT_NODE for sequence key
	     value by concatenating all RHS of the terms */
	  for (j = 0; j < n_elem; j++)
	    {
	      midxkey_list[j] =
		parser_append_node (pt_point (parser, rhs), midxkey_list[j]);
	    }
	}
      else
	{
	  /* PT_IS_IN */
	  assert (rhs->node_type == PT_FUNCTION);

	  if (rhs->node_type == PT_FUNCTION)
	    {
	      /* PT_FUNCTION */

	      for (j = 0, elem = rhs->info.function.arg_list;
		   j < n_elem && elem; j++, elem = elem->next)
		{

		  regu_var_list[j] = pt_to_regu_variable (parser, elem,
							  UNBOX_AS_VALUE);
		  if (regu_var_list[j] == NULL ||
		      !VALIDATE_REGU_KEY (regu_var_list[j]))
		    {
		      goto error;
		    }

		  /* is the key value constant(value or host variable)? */
		  key_infop->is_constant &= (elem->node_type == PT_VALUE ||
					     elem->node_type == PT_HOST_VAR);

		  /* make one PT_NODE for
		     sequence key value by concatenating all RHS of the
		     terms */
		  midxkey_list[j] =
		    parser_append_node (pt_point (parser, elem),
					midxkey_list[j]);
		}		/* for (j = 0, = ...) */
	    }
	  else
	    {
	      goto error;
	    }
	}
    }

  /* make a idxkey regu variable for multi-column index */
  for (i = 0; i < n_elem; i++)
    {
      if (midxkey_list[i] == NULL)
	{
	  goto error;
	}

      tmp = parser_new_node (parser, PT_FUNCTION);
      if (tmp == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  goto error;
	}
      tmp->type_enum = PT_TYPE_MAYBE;
      tmp->info.function.function_type = F_IDXKEY;
      tmp->info.function.arg_list = midxkey_list[i];
      regu_var_list[i] = pt_to_regu_variable (parser, tmp, UNBOX_AS_VALUE);
      parser_free_tree (parser, tmp);
      midxkey_list[i] = NULL;	/* already free */
    }

  /* set KEY_INFO structure */
  key_infop->key_cnt = n_elem;	/* n_elem ranges */
  key_infop->key_ranges = regu_keyrange_array_alloc (n_elem);
  if (key_infop->key_ranges == NULL)
    {
      goto error;
    }

  for (i = 0; i < n_elem; i++)
    {
      key_infop->key_ranges[i].range = EQ_NA;
      key_infop->key_ranges[i].key1 = regu_var_list[i];
      key_infop->key_ranges[i].key2 = NULL;
    }

  if (midxkey_list)
    {
      free_and_init (midxkey_list);
    }

  return 0;

/* error handling */
error:

  if (midxkey_list)
    {
      for (i = 0; i < n_elem; i++)
	{
	  if (midxkey_list[i])
	    {
	      parser_free_tree (parser, midxkey_list[i]);
	    }
	}
      free_and_init (midxkey_list);
    }

  return -1;
}

/*
 * pt_to_rangelist_key () - Create an key information(KEY_INFO) in INDX_INFO
 * 	structure for index scan with range spec of R_RANGELIST
 *   return:
 *   parser(in):
 *   term_exprs(in):
 *   nterms(in):
 *   key_infop(out): Construct a list of search range values
 *   rangelist_idx(in):
 */
static int
pt_to_rangelist_key (PARSER_CONTEXT * parser,
		     PT_NODE ** term_exprs, int nterms,
		     KEY_INFO * key_infop, int rangelist_idx)
{
  UNUSED_VAR PT_NODE *lhs;
  PT_NODE *rhs, *llim, *ulim, *elem, *tmp;
  PT_NODE **midxkey_list1 = NULL, **midxkey_list2 = NULL;
  PT_OP_TYPE op_type;
  REGU_VARIABLE **regu_var_list1, **regu_var_list2, *regu_var;
  RANGE *range_list = NULL;
  int i, j, n_elem;
  int list_count1, list_count2;
  int num_index_term;
  REGU_VARIABLE_LIST requ_list;

  assert (nterms > 0);

  midxkey_list1 = midxkey_list2 = NULL;
  regu_var_list1 = regu_var_list2 = NULL;
  key_infop->key_cnt = 0;
  key_infop->key_ranges = NULL;
  key_infop->is_constant = 1;
  n_elem = 0;

  /* get number of elements of the RANGE predicate */
  rhs = term_exprs[rangelist_idx]->info.expr.arg2;
  for (elem = rhs, n_elem = 0; elem; elem = elem->or_next, n_elem++)
    {
      ;
    }
  if (n_elem <= 0)
    {
      goto error;
    }

  /* allocate regu variable list and sequence value list */
  regu_var_list1 = regu_varptr_array_alloc (n_elem);
  regu_var_list2 = regu_varptr_array_alloc (n_elem);
  range_list = (RANGE *) malloc (sizeof (RANGE) * n_elem);
  if (regu_var_list1 == NULL || regu_var_list2 == NULL || range_list == NULL)
    {
      goto error;
    }

  memset (range_list, 0, sizeof (RANGE) * n_elem);

  midxkey_list1 = (PT_NODE **) malloc (sizeof (PT_NODE *) * n_elem);
  if (midxkey_list1 == NULL)
    {
      goto error;
    }

  memset (midxkey_list1, 0, sizeof (PT_NODE *) * n_elem);

  midxkey_list2 = (PT_NODE **) malloc (sizeof (PT_NODE *) * n_elem);
  if (midxkey_list2 == NULL)
    {
      goto error;
    }

  memset (midxkey_list2, 0, sizeof (PT_NODE *) * n_elem);

  /* for each term */
  for (i = 0; i < nterms; i++)
    {
      /* If nterms > 1, then it should be multi-column index and
         all term_expr[0 .. nterms - 1] are equality expression.
         (Even though nterms == 1, it can be multi-column index.) */

      /* op type, LHS side and RHS side of this term expression */
      op_type = term_exprs[i]->info.expr.op;
      lhs = term_exprs[i]->info.expr.arg1;
      rhs = term_exprs[i]->info.expr.arg2;

      llim = ulim = NULL;	/* init */

      if (op_type != PT_RANGE)
	{
	  assert (i != rangelist_idx);

	  /* PT_EQ */

	  regu_var = pt_to_regu_variable (parser, rhs, UNBOX_AS_VALUE);
	  if (regu_var == NULL)
	    {
	      goto error;
	    }
	  if (!VALIDATE_REGU_KEY (regu_var))
	    {
	      /* correlated join index case swap LHS and RHS */
	      lhs = term_exprs[i]->info.expr.arg2;
	      rhs = term_exprs[i]->info.expr.arg1;

	      /* try on RHS */
	      regu_var = pt_to_regu_variable (parser, rhs, UNBOX_AS_VALUE);
	      if (regu_var == NULL || !VALIDATE_REGU_KEY (regu_var))
		{
		  goto error;
		}
	    }

	  /* is the key value constant(value or host variable)? */
	  key_infop->is_constant &= (rhs->node_type == PT_VALUE
				     || rhs->node_type == PT_HOST_VAR);

	  for (j = 0; j < n_elem; j++)
	    {
	      if (i == nterms - 1)
		{		/* the last term */
		  range_list[j] = op_type_to_range (op_type, nterms);
		}

	      /* make one PT_NODE for sequence key
	         value by concatenating all RHS of the terms */
	      midxkey_list1[j] = parser_append_node (pt_point (parser,
							       rhs),
						     midxkey_list1[j]);
	      midxkey_list2[j] = parser_append_node (pt_point (parser,
							       rhs),
						     midxkey_list2[j]);
	    }
	}
      else
	{
	  assert ((i == rangelist_idx) || (rhs->or_next == NULL));

	  /* PT_RANGE */
	  for (j = 0, elem = rhs; j < n_elem && elem;
	       j++, elem = elem->or_next)
	    {
	      /* range type and spec(lower limit and upper limit) from
	         operands of RANGE expression */
	      op_type = elem->info.expr.op;
	      switch (op_type)
		{
		case PT_BETWEEN_EQ_NA:
		  llim = elem->info.expr.arg1;
		  ulim = llim;
		  break;
		case PT_BETWEEN_INF_LE:
		case PT_BETWEEN_INF_LT:
		  llim = NULL;
		  ulim = elem->info.expr.arg1;
		  break;
		case PT_BETWEEN_GE_INF:
		case PT_BETWEEN_GT_INF:
		  llim = elem->info.expr.arg1;
		  ulim = NULL;
		  break;
		default:
		  llim = elem->info.expr.arg1;
		  ulim = elem->info.expr.arg2;
		  break;
		}

	      if (llim)
		{
		  regu_var_list1[j] = pt_to_regu_variable (parser, llim,
							   UNBOX_AS_VALUE);
		  if (regu_var_list1[j] == NULL ||
		      !VALIDATE_REGU_KEY (regu_var_list1[j]))
		    {
		      goto error;
		    }

		  /* is the key value constant(value or host variable)? */
		  key_infop->is_constant &= (llim->node_type == PT_VALUE ||
					     llim->node_type == PT_HOST_VAR);
		}
	      else
		{
		  regu_var_list1[j] = NULL;
		}		/* if (llim) */

	      if (ulim)
		{
		  regu_var_list2[j] = pt_to_regu_variable (parser, ulim,
							   UNBOX_AS_VALUE);
		  if (regu_var_list2[j] == NULL ||
		      !VALIDATE_REGU_KEY (regu_var_list2[j]))
		    {
		      goto error;
		    }

		  /* is the key value constant(value or host variable)? */
		  key_infop->is_constant &= (ulim->node_type == PT_VALUE ||
					     ulim->node_type == PT_HOST_VAR);
		}
	      else
		{
		  regu_var_list2[j] = NULL;
		}		/* if (ulim) */

	      if (i == nterms - 1)
		{		/* the last term */
		  range_list[j] = op_type_to_range (op_type, nterms);
		}

	      /* make one PT_NODE for sequence
	         key value by concatenating all RHS of the terms */
	      if (llim)
		{
		  midxkey_list1[j] =
		    parser_append_node (pt_point (parser, llim),
					midxkey_list1[j]);
		}
	      if (ulim)
		{
		  midxkey_list2[j] =
		    parser_append_node (pt_point (parser, ulim),
					midxkey_list2[j]);
		}
	    }			/* for (j = 0, elem = rhs; ... ) */

	  if (i == rangelist_idx)
	    {
	      assert (j == n_elem);
	      /* OK; nop */
	    }
	  else
	    {
	      int k;

	      assert (j == 1);

	      for (k = j; k < n_elem; k++)
		{
		  if (i == nterms - 1)
		    {		/* the last term */
		      range_list[k] = op_type_to_range (op_type, nterms);
		    }

		  if (llim)
		    {
		      midxkey_list1[k] =
			parser_append_node (pt_point (parser, llim),
					    midxkey_list1[k]);
		    }
		  if (ulim)
		    {
		      midxkey_list2[k] =
			parser_append_node (pt_point (parser, ulim),
					    midxkey_list2[k]);
		    }
		}		/* for */
	    }
	}			/* else (op_type != PT_RANGE) */
    }				/* for (i = 0; i < nterms; i++) */

  /* make a idxkey regu variable for multi-column index */
  for (i = 0; i < n_elem; i++)
    {
      if (midxkey_list1[i])
	{
	  tmp = parser_new_node (parser, PT_FUNCTION);
	  if (tmp == NULL)
	    {
	      PT_INTERNAL_ERROR (parser, "allocate new node");
	      goto error;
	    }

	  tmp->type_enum = PT_TYPE_MAYBE;
	  tmp->info.function.function_type = F_IDXKEY;
	  tmp->info.function.arg_list = midxkey_list1[i];
	  regu_var_list1[i] = pt_to_regu_variable (parser, tmp,
						   UNBOX_AS_VALUE);
	  parser_free_tree (parser, tmp);
	  midxkey_list1[i] = NULL;	/* already free */
	}
    }
  free_and_init (midxkey_list1);

  /* make a idxkey regu variable for multi-column index */
  for (i = 0; i < n_elem; i++)
    {
      if (midxkey_list2[i])
	{
	  tmp = parser_new_node (parser, PT_FUNCTION);
	  if (tmp == NULL)
	    {
	      PT_INTERNAL_ERROR (parser, "allocate new node");
	      goto error;
	    }
	  tmp->type_enum = PT_TYPE_MAYBE;
	  tmp->info.function.function_type = F_IDXKEY;
	  tmp->info.function.arg_list = midxkey_list2[i];
	  regu_var_list2[i] = pt_to_regu_variable (parser, tmp,
						   UNBOX_AS_VALUE);
	  parser_free_tree (parser, tmp);
	  midxkey_list2[i] = NULL;	/* already free */
	}
    }
  free_and_init (midxkey_list2);

  num_index_term = 0;		/* to make compiler be silent */
  for (i = 0; i < n_elem; i++)
    {
      list_count1 = list_count2 = 0;

      if (regu_var_list1[i] != NULL)
	{
	  for (requ_list = regu_var_list1[i]->value.funcp->operand;
	       requ_list; requ_list = requ_list->next)
	    {
	      list_count1++;
	    }
	}

      if (regu_var_list2[i] != NULL)
	{
	  for (requ_list = regu_var_list2[i]->value.funcp->operand;
	       requ_list; requ_list = requ_list->next)
	    {
	      list_count2++;
	    }
	}

      if (i == 0)
	{
	  num_index_term = MAX (list_count1, list_count2);
	}
      else
	{
	  if (num_index_term != MAX (list_count1, list_count2))
	    {
	      assert_release (false);
	      goto error;
	    }
	}
    }

  /* set KEY_INFO structure */
  key_infop->key_cnt = n_elem;	/* n_elem ranges */
  key_infop->key_ranges = regu_keyrange_array_alloc (n_elem);
  if (!key_infop->key_ranges)
    {
      goto error;
    }

  for (i = 0; i < n_elem; i++)
    {
      key_infop->key_ranges[i].range = range_list[i];
      key_infop->key_ranges[i].key1 = regu_var_list1[i];
      key_infop->key_ranges[i].key2 = regu_var_list2[i];
    }

  if (range_list)
    {
      free_and_init (range_list);
    }

  return 0;

/* error handling */
error:

  if (midxkey_list1)
    {
      for (i = 0; i < n_elem; i++)
	{
	  if (midxkey_list1[i])
	    {
	      parser_free_tree (parser, midxkey_list1[i]);
	    }
	}
      free_and_init (midxkey_list1);
    }
  if (midxkey_list2)
    {
      for (i = 0; i < n_elem; i++)
	{
	  if (midxkey_list2[i])
	    {
	      parser_free_tree (parser, midxkey_list2[i]);
	    }
	}
      free_and_init (midxkey_list2);
    }

  if (range_list)
    {
      free_and_init (range_list);
    }

  return -1;
}


/*
 * pt_to_key_limit () - Create index key limit regu variables
 *   return:
 *   parser(in):
 *   key_limit(in):
 *   key_infop(in):
 *   key_limit_reset(in);
 */
static int
pt_to_key_limit (PARSER_CONTEXT * parser, PT_NODE * key_limit,
		 QO_LIMIT_INFO * limit_infop, KEY_INFO * key_infop,
		 bool key_limit_reset)
{
  REGU_VARIABLE *regu_var_u = NULL, *regu_var_l = NULL;
  PT_NODE *limit_u, *limit_l;

  /* at least one of them should be NULL, although they both can */
  assert (!key_limit || !limit_infop);

  limit_u = key_limit;
  if (limit_u)
    {
      regu_var_u = pt_to_regu_variable (parser, limit_u, UNBOX_AS_VALUE);
      if (regu_var_u == NULL)
	{
	  goto error;
	}

      limit_l = limit_u->next;
      if (limit_l)
	{
	  regu_var_l = pt_to_regu_variable (parser, limit_l, UNBOX_AS_VALUE);
	  if (regu_var_l == NULL)
	    {
	      goto error;
	    }
	}
    }

  if (limit_infop)
    {
      regu_var_u = limit_infop->upper;
      regu_var_l = limit_infop->lower;
    }

  if (key_infop->key_limit_u)
    {
      if (regu_var_u)
	{
	  key_infop->key_limit_u = pt_make_regu_arith (key_infop->key_limit_u,
						       regu_var_u,
						       NULL, T_LEAST);
	  if (key_infop->key_limit_u == NULL)
	    {
	      goto error;
	    }
	}
    }
  else
    {
      key_infop->key_limit_u = regu_var_u;
    }

  if (key_infop->key_limit_l)
    {
      if (regu_var_l)
	{
	  key_infop->key_limit_l = pt_make_regu_arith (key_infop->key_limit_l,
						       regu_var_l,
						       NULL, T_GREATEST);
	  if (key_infop->key_limit_l == NULL)
	    {
	      goto error;
	    }
	}
    }
  else
    {
      key_infop->key_limit_l = regu_var_l;
    }

  key_infop->key_limit_reset = key_limit_reset;

  return NO_ERROR;

error:

  return ER_FAILED;
}


/*
 * pt_instnum_to_key_limit () - try to convert instnum to keylimit
 *   return:
 *   parser(in):
 *   plan(in):
 *   xasl(in):
 */
static int
pt_instnum_to_key_limit (PARSER_CONTEXT * parser, QO_PLAN * plan,
			 XASL_NODE * xasl)
{
  XASL_NODE *xptr;
  ACCESS_SPEC_TYPE *spec_list;
  QO_LIMIT_INFO *limit_infop;
  int ret = NO_ERROR;

  /* If ANY of the spec lists has a data filter, we cannot convert
   * instnum to keylimit, because in the worst case scenario the data filter
   * could require all the records in a join operation and chose only the last
   * Cartesian tuple, and any keylimit on the joined tables would be wrong.
   */
  for (xptr = xasl; xptr; xptr = xptr->scan_ptr)
    {
      for (spec_list = xptr->spec_list; spec_list;
	   spec_list = spec_list->next)
	{
	  if (spec_list->where_pred)
	    {
	      /* this is not an error, just halt the optimization tentative */
	      return NO_ERROR;
	    }
	}
    }

  /* if there is an orderby_num pred, meaning order by was not skipped */
  if (xasl->ordbynum_pred || xasl->if_pred || xasl->after_join_pred)
    {
      /* can't optimize */
      return NO_ERROR;
    }

  limit_infop = qo_get_key_limit_from_instnum (parser, plan, xasl);
  if (!limit_infop)
    {
      return NO_ERROR;
    }
  if (!limit_infop->upper)
    {
      free_and_init (limit_infop);
      return NO_ERROR;
    }

  /* there is at least an upper limit, but we need to take some decisions
   * depending on the presence of a lower limit and the query complexity */

  /* do we have a join or other non-trivial select? */
  if (xasl->scan_ptr)
    {
      /* If we are joining multiple tables, we cannot afford to use the
       * lower limit: it should be applied only at the higher, join level
       * and not at lower table scan levels. Discard the lower limit.*/
      limit_infop->lower = NULL;
    }
  else
    {
      /* a trivial select: we can keep the lower limit, but we must adjust
       * the upper limit. qo_get_key_limit_from_instnum gets a lower and an
       * upper limit, but keylimit requires a min and a count (i.e.
       * how many should we skip, and then how many should we fetch) */
      if (limit_infop->lower)
	{
	  limit_infop->upper = pt_make_regu_arith (limit_infop->upper,
						   limit_infop->lower, NULL,
						   T_SUB);
	  if (limit_infop->upper == NULL)
	    {
	      goto exit_on_error;
	    }
	}

      /* we must also delete the instnum predicate, because we don't want
       * two sets of lower limits for the same data */
      assert (xasl->instnum_pred);
      xasl->instnum_pred = NULL;
    }

  /* cannot handle for join; skip and go ahead */
  if (xasl->scan_ptr == NULL)
    {
      /* set the key limit to all the eligible spec lists (the ones
       * that have index scans.) */
      for (xptr = xasl; xptr; xptr = xptr->scan_ptr)
	{
	  for (spec_list = xptr->spec_list; spec_list;
	       spec_list = spec_list->next)
	    {
	      if (!spec_list->indexptr)
		{
		  continue;
		}

	      ret = pt_to_key_limit (parser, NULL, limit_infop,
				     &(spec_list->indexptr->key_info), false);
	      if (ret != NO_ERROR)
		{
		  goto exit_on_error;
		}
	    }
	}
    }

  /* we're done with the generated key limit tree */
  free_and_init (limit_infop);

  return NO_ERROR;

exit_on_error:
  if (limit_infop)
    {
      free_and_init (limit_infop);
    }
  return ER_FAILED;
}


/*
 * pt_to_index_info () - Create an INDX_INFO structure for communication
 * 	to a class access spec for eventual incorporation into an index scan
 *   return:
 *   parser(in):
 *   class(in):
 *   where_pred(in):
 *   plan(in):
 *   qo_index_infop(in):
 */
static INDX_INFO *
pt_to_index_info (PARSER_CONTEXT * parser, DB_OBJECT * class_,
		  PRED_EXPR * where_pred,
		  QO_PLAN * plan, QO_XASL_INDEX_INFO * qo_index_infop)
{
  int nterms;
  int rangelist_idx = -1;
  PT_NODE **term_exprs;
  PT_NODE *pt_expr;
  PT_OP_TYPE op_type = PT_LAST_OPCODE;
  INDX_INFO *indx_infop;
  QO_NODE_INDEX_ENTRY *ni_entryp;
  QO_INDEX_ENTRY *index_entryp;
  KEY_INFO *key_infop;
  int rc;
  int i;

  assert (parser != NULL);
  assert (class_ != NULL);
  assert (plan != NULL);
  assert (qo_index_infop->ni_entry != NULL);
  assert (qo_index_infop->ni_entry->head != NULL);

  if (!qo_is_interesting_order_scan (plan))
    {
      assert (false);
      PT_INTERNAL_ERROR (parser, "index plan generation - invalid plan");
      return NULL;
    }

  ni_entryp = qo_index_infop->ni_entry;
  index_entryp = ni_entryp->head;
  assert (index_entryp != NULL);
  assert (index_entryp->class_ != NULL);

  if (class_ != index_entryp->class_->mop)
    {
      assert (false);
      PT_INTERNAL_ERROR (parser, "index plan generation - invalid plan");
      return NULL;
    }

  /* get array of term expressions and number of them which are associated
     with this index */
  nterms = qo_xasl_get_num_terms (qo_index_infop);
  term_exprs = qo_xasl_get_terms (qo_index_infop);

  if (class_ == NULL || nterms < 0 || index_entryp == NULL)
    {
      PT_INTERNAL_ERROR (parser, "index plan generation - invalid arg");
      return NULL;
    }

  if (nterms > 0)
    {
      rangelist_idx = -1;	/* init */
      for (i = 0; i < nterms; i++)
	{
	  pt_expr = term_exprs[i];
	  assert (pt_expr != NULL);
	  if (pt_expr->info.expr.op == PT_RANGE)
	    {
	      assert (pt_expr->info.expr.arg2 != NULL);

	      if (pt_expr->info.expr.arg2)
		{
		  PT_NODE *between_and;

		  between_and = pt_expr->info.expr.arg2;
		  if (between_and->or_next)
		    {
		      /* is RANGE (r1, r2, ...) */
		      rangelist_idx = i;
		      break;
		    }
		}
	    }
	}

      if (rangelist_idx == -1)
	{
	  /* The last term expression in the array(that is, [nterms - 1]) is
	     interesting because the multi-column index scan depends on it.
	     For example:
	     a = ? AND b = ? AND c = ?
	     a = ? AND b = ? AND c RANGE (r1)
	     a = ? AND b = ? AND c RANGE (r1, r2, ...)
	   */
	  rangelist_idx = nterms - 1;
	  op_type = term_exprs[rangelist_idx]->info.expr.op;
	}
      else
	{
	  /* Have non-last EQUAL range term and is only one.
	     For example:
	     a = ? AND b RANGE (r1=, r2=, ...) AND c = ?
	     a = ? AND b RANGE (r1=, r2=, ...) AND c RANGE (r1)

	     but, the following is not permitted.
	     a = ? AND b RANGE (r1=, r2=, ...) AND c RANGE (r1, r2, ...)
	   */
	  op_type = PT_RANGE;
	}
    }

  /* make INDX_INFO structure and fill it up using information in
     QO_XASL_INDEX_INFO structure */
  indx_infop = regu_index_alloc ();
  if (indx_infop == NULL)
    {
      PT_INTERNAL_ERROR (parser, "index plan generation - memory alloc");
      return NULL;
    }

  /* BTID */
  indx_infop->indx_id.type = T_BTID;
  BTID_COPY (&(indx_infop->indx_id.i.btid),
	     &(index_entryp->constraints->index_btid));

  /* check for covered index scan */
  indx_infop->coverage = 0;	/* init */
  if (qo_is_index_covering_scan (plan))
    {
      assert (index_entryp->cover_segments == true);
      assert (where_pred == NULL);	/* no data-filter */

      if (index_entryp->cover_segments == true && where_pred == NULL)
	{
	  indx_infop->coverage = 1;
	}
    }

  if (indx_infop->coverage)
    {
      COLL_OPT collation_opt;

      qo_check_coll_optimization (index_entryp, &collation_opt);

      indx_infop->coverage = collation_opt.allow_index_opt;
    }

  indx_infop->class_oid = class_->ws_oid;
  indx_infop->use_desc_index = index_entryp->use_descending;
  indx_infop->orderby_skip = index_entryp->orderby_skip;
  indx_infop->groupby_skip = index_entryp->groupby_skip;

  /* 0 for now, see gen optimized plan for its computation */
  indx_infop->orderby_desc = 0;
  indx_infop->groupby_desc = 0;

  /* key limits */
  key_infop = &indx_infop->key_info;
  if (pt_to_key_limit (parser, index_entryp->key_limit,
		       NULL, key_infop, false) != NO_ERROR)
    {
      PT_INTERNAL_ERROR (parser, "index plan generation - invalid key limit");
      return NULL;
    }

  if (nterms == 0)
    {
      DB_VALUE db_value;
      PT_NODE *tmp, *idx_key;
      REGU_VARIABLE *regu_var1, *regu_var2;
      SM_ATTRIBUTE *attrp;
      DB_DOMAIN *dom;
      DB_TYPE dom_type = DB_TYPE_NULL;

      DB_MAKE_NULL (&db_value);
      idx_key = NULL;
      regu_var1 = NULL;
      regu_var2 = NULL;

      assert (index_entryp->constraints->num_atts > 0);
      assert (index_entryp->constraints->attributes != NULL);
      attrp = index_entryp->constraints->attributes[0];

      assert (attrp != NULL);
      assert (attrp->sma_domain != NULL);
      dom = attrp->sma_domain;
      dom_type = TP_DOMAIN_TYPE (dom);

      assert (tp_valid_indextype (dom_type));

      /* set lower key */
      if (db_value_domain_min (&db_value, dom_type,
			       dom->precision, dom->scale,
			       dom->collation_id) != NO_ERROR)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  return NULL;
	}

      idx_key = pt_dbval_to_value (parser, &db_value);
      if (idx_key == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  return NULL;
	}

      /* make a idxkey regu variable for multi-column index */
      tmp = parser_new_node (parser, PT_FUNCTION);
      if (tmp == NULL)
	{
	  parser_free_tree (parser, idx_key);
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  return NULL;
	}

      tmp->type_enum = PT_TYPE_MAYBE;
      tmp->info.function.function_type = F_IDXKEY;
      tmp->info.function.arg_list = idx_key;
      regu_var1 = pt_to_regu_variable (parser, tmp, UNBOX_AS_VALUE);
      if (regu_var1 == NULL)
	{
	  parser_free_tree (parser, tmp);
	  PT_INTERNAL_ERROR (parser, "index plan generation - memory alloc");
	  return NULL;
	}
      parser_free_tree (parser, tmp);

      /* set upper key */
      if (db_value_domain_max (&db_value, dom_type,
			       dom->precision, dom->scale,
			       dom->collation_id) != NO_ERROR)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  return NULL;
	}

      idx_key = pt_dbval_to_value (parser, &db_value);
      if (idx_key == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  return NULL;
	}

      /* make a idxkey regu variable for multi-column index */
      tmp = parser_new_node (parser, PT_FUNCTION);
      if (tmp == NULL)
	{
	  parser_free_tree (parser, idx_key);
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  return NULL;
	}

      tmp->type_enum = PT_TYPE_MAYBE;
      tmp->info.function.function_type = F_IDXKEY;
      tmp->info.function.arg_list = idx_key;
      regu_var2 = pt_to_regu_variable (parser, tmp, UNBOX_AS_VALUE);
      if (regu_var2 == NULL)
	{
	  parser_free_tree (parser, tmp);
	  PT_INTERNAL_ERROR (parser, "index plan generation - memory alloc");
	  return NULL;
	}
      parser_free_tree (parser, tmp);

      key_infop->key_cnt = 1;
      key_infop->is_constant = 1;
      key_infop->key_ranges = regu_keyrange_array_alloc (1);
      if (key_infop->key_ranges == NULL)
	{
	  parser_free_tree (parser, tmp);
	  PT_INTERNAL_ERROR (parser, "index plan generation - memory alloc");
	  return NULL;
	}

      key_infop->key_ranges[0].range = GE_LE;
      key_infop->key_ranges[0].key1 = regu_var1;
      key_infop->key_ranges[0].key2 = regu_var2;

      indx_infop->range_type = R_RANGELIST;

      return indx_infop;
    }

  assert (nterms > 0);

  /* scan range spec and index key information */
  switch (op_type)
    {
    case PT_EQ:
    case PT_IS_IN:
      rc = pt_to_list_key (parser, term_exprs, nterms, key_infop);
      indx_infop->range_type = R_KEYLIST;
      break;

    case PT_RANGE:
      rc = pt_to_rangelist_key (parser, term_exprs, nterms,
				key_infop, rangelist_idx);
      for (i = 0; i < key_infop->key_cnt; i++)
	{
	  if (key_infop->key_ranges[i].range != EQ_NA)
	    {
	      break;
	    }
	}
      if (i < key_infop->key_cnt)
	{
	  indx_infop->range_type = R_RANGELIST;
	}
      else
	{
	  indx_infop->range_type = R_KEYLIST;	/* attr IN (?, ?) */
	}
      break;

    default:
      /* the other operators are not applicable to index scan */
      rc = -1;
      break;
    }

  if (rc < 0)
    {
      assert (false);
      PT_INTERNAL_ERROR (parser, "index plan generation - invalid key value");
      return NULL;
    }

  return indx_infop;
}

/*
 * pt_to_class_spec_list () - Convert a PT_NODE flat class list to
 *     an ACCESS_SPEC_LIST list of representing the classes to be selected from
 *   return:
 *   parser(in):
 *   spec(in):
 *   where_key_part(in):
 *   where_part(in):
 *   pk_next_part(in):
 *   plan(in):
 *   index_pred(in):
 */
static ACCESS_SPEC_TYPE *
pt_to_class_spec_list (PARSER_CONTEXT * parser, PT_NODE * spec,
		       PT_NODE * where_key_part, PT_NODE * where_part,
		       PT_NODE * pk_next_part, QO_PLAN * plan,
		       QO_XASL_INDEX_INFO * index_pred)
{
  SYMBOL_INFO *symbols;
  ACCESS_SPEC_TYPE *access;
  ACCESS_SPEC_TYPE *access_list = NULL;
  PT_NODE *flat;
  PT_NODE *class_;
  PRED_EXPR *where_key = NULL;
  REGU_VARIABLE_LIST regu_attributes_key;
  HEAP_CACHE_ATTRINFO *cache_key = NULL;
  PT_NODE *key_attrs = NULL;
  int *key_offsets = NULL;
  PRED_EXPR *where = NULL;
  REGU_VARIABLE_LIST regu_attributes_pred, regu_attributes_rest;
  REGU_VARIABLE_LIST regu_pk_next;
  TABLE_INFO *table_info;
  INDX_INFO *index_info;
  HEAP_CACHE_ATTRINFO *cache_pred = NULL, *cache_rest = NULL;
  PT_NODE *pred_attrs = NULL, *rest_attrs = NULL;
  int *pred_offsets = NULL, *rest_offsets = NULL;
  int *pk_next_offsets = NULL;
  OUTPTR_LIST *output_val_list = NULL;
  REGU_VARIABLE_LIST regu_val_list = NULL;


  assert (parser != NULL);

  if (spec == NULL)
    {
      return NULL;
    }

  flat = spec->info.spec.flat_entity_list;
  if (flat == NULL || flat->next != NULL)
    {
      assert (flat == NULL || flat->next == NULL);

      return NULL;
    }

  symbols = parser->symbols;
  if (symbols == NULL)
    {
      return NULL;
    }

  table_info = pt_find_table_info (flat->info.name.spec_id,
				   symbols->table_info);

  if (table_info)
    {
      for (class_ = flat; class_ != NULL; class_ = class_->next)
	{
	  /* The scans have changed to grab the val list before
	   * predicate evaluation since evaluation now does comparisons
	   * using DB_VALUES instead of disk rep.  Thus, the where
	   * predicate does NOT want to generate TYPE_ATTR_ID regu
	   * variables, but rather TYPE_CONSTANT regu variables.
	   * This is driven off the symbols->current class variable
	   * so we need to generate the where pred first.
	   */

	  if (index_pred == NULL)
	    {
	      if (!pt_split_attrs (parser, table_info, where_part,
				   &pred_attrs, &rest_attrs,
				   &pred_offsets, &rest_offsets))
		{
		  return NULL;
		}

	      cache_pred = regu_cache_attrinfo_alloc ();
	      cache_rest = regu_cache_attrinfo_alloc ();

	      symbols->current_class = class_;
	      symbols->cache_attrinfo = cache_pred;

	      where = pt_to_pred_expr (parser, where_part);

	      regu_attributes_pred = pt_to_regu_variable_list (parser,
							       pred_attrs,
							       UNBOX_AS_VALUE,
							       table_info->
							       value_list,
							       pred_offsets);

	      symbols->cache_attrinfo = cache_rest;

	      regu_attributes_rest = pt_to_regu_variable_list (parser,
							       rest_attrs,
							       UNBOX_AS_VALUE,
							       table_info->
							       value_list,
							       rest_offsets);

	      output_val_list = NULL;
	      regu_val_list = NULL;

	      parser_free_tree (parser, pred_attrs);
	      parser_free_tree (parser, rest_attrs);
	      free_and_init (pred_offsets);
	      free_and_init (rest_offsets);

	      access = pt_make_class_access_spec (parser, flat,
						  class_->info.name.db_object,
						  TARGET_CLASS, SEQUENTIAL,
						  NULL, NULL, where, NULL,
						  regu_attributes_pred,
						  regu_attributes_rest,
						  NULL,
						  output_val_list,
						  regu_val_list, NULL,
						  cache_pred, cache_rest);
	    }
	  else
	    {
	      if (!pt_to_index_attrs (parser, table_info, index_pred,
				      where_key_part, &key_attrs,
				      &key_offsets))
		{
		  return NULL;
		}
	      if (!pt_split_attrs (parser, table_info, where_part,
				   &pred_attrs, &rest_attrs,
				   &pred_offsets, &rest_offsets))
		{
		  parser_free_tree (parser, key_attrs);
		  free_and_init (key_offsets);
		  return NULL;
		}

	      cache_key = regu_cache_attrinfo_alloc ();
	      cache_pred = regu_cache_attrinfo_alloc ();
	      cache_rest = regu_cache_attrinfo_alloc ();

	      symbols->current_class = class_;
	      symbols->cache_attrinfo = cache_key;

	      where_key = pt_to_pred_expr (parser, where_key_part);

	      regu_attributes_key = pt_to_regu_variable_list (parser,
							      key_attrs,
							      UNBOX_AS_VALUE,
							      table_info->
							      value_list,
							      key_offsets);

	      symbols->cache_attrinfo = cache_pred;

	      where = pt_to_pred_expr (parser, where_part);

	      regu_attributes_pred = pt_to_regu_variable_list (parser,
							       pred_attrs,
							       UNBOX_AS_VALUE,
							       table_info->
							       value_list,
							       pred_offsets);

	      symbols->cache_attrinfo = cache_rest;

	      regu_attributes_rest = pt_to_regu_variable_list (parser,
							       rest_attrs,
							       UNBOX_AS_VALUE,
							       table_info->
							       value_list,
							       rest_offsets);

	      regu_pk_next = pt_to_regu_variable_list (parser,
						       pk_next_part,
						       UNBOX_AS_VALUE,
						       table_info->
						       value_list,
						       pk_next_offsets);

	      output_val_list = pt_make_outlist_from_vallist (parser,
							      table_info->
							      value_list);

	      regu_val_list =
		pt_to_position_regu_variable_list (parser, rest_attrs,
						   table_info->value_list,
						   rest_offsets);

	      parser_free_tree (parser, key_attrs);
	      parser_free_tree (parser, pred_attrs);
	      parser_free_tree (parser, rest_attrs);
	      free_and_init (key_offsets);
	      free_and_init (pred_offsets);
	      free_and_init (rest_offsets);
	      free_and_init (pk_next_offsets);

	      /*
	       * pt_make_class_spec() will return NULL if passed a
	       * NULL INDX_INFO *, so there isn't any need to check
	       * return values here.
	       */
	      index_info = pt_to_index_info (parser,
					     class_->info.name.db_object,
					     where, plan, index_pred);
	      if (index_info != NULL)
		{
		  access = pt_make_class_access_spec (parser, flat,
						      class_->info.name.
						      db_object, TARGET_CLASS,
						      INDEX,
						      index_info,
						      where_key, where,
						      regu_attributes_key,
						      regu_attributes_pred,
						      regu_attributes_rest,
						      regu_pk_next,
						      output_val_list,
						      regu_val_list,
						      cache_key, cache_pred,
						      cache_rest);
		}
	      else
		{
		  assert (false);
		  /* an error condition */
		  access = NULL;
		}
	    }

	  if (access == NULL
	      || (regu_attributes_pred == NULL
		  && regu_attributes_rest == NULL
		  && table_info->attribute_list) || pt_has_error (parser))
	    {
	      /* an error condition */
	      access = NULL;
	    }

	  if (access)
	    {
	      if (spec->info.spec.flag & PT_SPEC_FLAG_FOR_UPDATE_CLAUSE)
		{
		  assert (spec->info.spec.derived_table == NULL);

		  access->flags |= ACCESS_SPEC_FLAG_FOR_UPDATE;
		}

	      access->next = access_list;
	      access_list = access;
	    }
	  else
	    {
	      /* an error condition */
	      access_list = NULL;
	      break;
	    }
	}

      symbols->current_class = NULL;
      symbols->cache_attrinfo = NULL;

    }

  return access_list;
}

/*
 * pt_to_subquery_table_spec_list () - Convert a QUERY PT_NODE
 * 	an ACCESS_SPEC_LIST list for its list file
 *   return:
 *   parser(in):
 *   spec(in):
 *   subquery(in):
 *   where_part(in):
 */
static ACCESS_SPEC_TYPE *
pt_to_subquery_table_spec_list (PARSER_CONTEXT * parser,
				PT_NODE * spec,
				PT_NODE * subquery, PT_NODE * where_part)
{
  XASL_NODE *subquery_proc;
  PT_NODE *saved_current_class;
  REGU_VARIABLE_LIST regu_attributes_pred, regu_attributes_rest;
  ACCESS_SPEC_TYPE *access;
  PRED_EXPR *where = NULL;
  TABLE_INFO *tbl_info;
  PT_NODE *pred_attrs = NULL, *rest_attrs = NULL;
  int *pred_offsets = NULL, *rest_offsets = NULL;

  subquery_proc = (XASL_NODE *) subquery->info.query.xasl;

  tbl_info = pt_find_table_info (spec->info.spec.id,
				 parser->symbols->table_info);

  if (!pt_split_attrs (parser, tbl_info, where_part,
		       &pred_attrs, &rest_attrs,
		       &pred_offsets, &rest_offsets))
    {
      return NULL;
    }

  /* This generates a list of TYPE_POSITION regu_variables
   * There information is stored in a QFILE_TUPLE_VALUE_POSITION, which
   * describes a type and index into a list file.
   */
  regu_attributes_pred = pt_to_position_regu_variable_list (parser,
							    pred_attrs,
							    tbl_info->
							    value_list,
							    pred_offsets);
  regu_attributes_rest =
    pt_to_position_regu_variable_list (parser, rest_attrs,
				       tbl_info->value_list, rest_offsets);

  parser_free_tree (parser, pred_attrs);
  parser_free_tree (parser, rest_attrs);
  free_and_init (pred_offsets);
  free_and_init (rest_offsets);

  parser->symbols->listfile_unbox = UNBOX_AS_VALUE;
  parser->symbols->current_listfile = NULL;

  /* The where predicate is now evaluated after the val list has been
   * fetched.  This means that we want to generate "CONSTANT" regu
   * variables instead of "POSITION" regu variables which would happen
   * if parser->symbols->current_listfile != NULL.
   * pred should never user the current instance for fetches
   * either, so we turn off the current_class, if there is one.
   */
  saved_current_class = parser->symbols->current_class;
  parser->symbols->current_class = NULL;
  where = pt_to_pred_expr (parser, where_part);
  parser->symbols->current_class = saved_current_class;

  access = pt_make_list_access_spec (subquery_proc, SEQUENTIAL,
				     NULL, where,
				     regu_attributes_pred,
				     regu_attributes_rest);

  if (access && subquery_proc
      && (regu_attributes_pred || regu_attributes_rest
	  || !spec->info.spec.as_attr_list))
    {
      return access;
    }

  return NULL;
}

/*
 * pt_to_spec_list () - Convert a PT_NODE spec to an ACCESS_SPEC_LIST list of
 *      representing the classes to be selected from
 *   return:
 *   parser(in):
 *   spec(in):
 *   where_key_part(in):
 *   where_part(in):
 *   pk_next_part(in):
 *   plan(in):
 *   index_part(in):
 *   src_derived_tbl(in):
 */
ACCESS_SPEC_TYPE *
pt_to_spec_list (PARSER_CONTEXT * parser, PT_NODE * spec,
		 PT_NODE * where_key_part, PT_NODE * where_part,
		 PT_NODE * pk_next_part, QO_PLAN * plan,
		 QO_XASL_INDEX_INFO * index_part,
		 UNUSED_ARG PT_NODE * src_derived_tbl)
{
  ACCESS_SPEC_TYPE *access = NULL;

  if (spec->info.spec.flat_entity_list != NULL
      && spec->info.spec.derived_table == NULL)
    {
      access =
	pt_to_class_spec_list (parser, spec, where_key_part, where_part,
			       pk_next_part, plan, index_part);
    }
  else
    {
      /* derived table
         index_part better be NULL here! */
      if (spec->info.spec.derived_table_type == PT_IS_SUBQUERY)
	{
	  access = pt_to_subquery_table_spec_list
	    (parser, spec, spec->info.spec.derived_table, where_part);
	}
    }

  return access;
}


/*
 * pt_to_val_list () -
 *   return: val_list corresponding to the entity spec
 *   parser(in):
 *   id(in):
 */
VAL_LIST *
pt_to_val_list (PARSER_CONTEXT * parser, UINTPTR id)
{
  SYMBOL_INFO *symbols;
  VAL_LIST *val_list = NULL;
  TABLE_INFO *table_info;

  if (parser)
    {
      symbols = parser->symbols;
      table_info = pt_find_table_info (id, symbols->table_info);

      if (table_info)
	{
	  val_list = table_info->value_list;
	}
    }

  return val_list;
}

/*
 * pt_find_xasl () - appends the from list to the end of the to list
 *   return:
 *   list(in):
 *   match(in):
 */
static XASL_NODE *
pt_find_xasl (XASL_NODE * list, XASL_NODE * match)
{
  XASL_NODE *xasl = list;

  while (xasl && xasl != match)
    {
      xasl = xasl->next;
    }

  return xasl;
}

/*
 * pt_append_xasl () - appends the from list to the end of the to list
 *   return:
 *   to(in):
 *   from_list(in):
 */
XASL_NODE *
pt_append_xasl (XASL_NODE * to, XASL_NODE * from_list)
{
  XASL_NODE *xasl = to;
  XASL_NODE *next;
  XASL_NODE *from = from_list;

  if (!xasl)
    {
      return from_list;
    }

  while (xasl->next)
    {
      xasl = xasl->next;
    }

  while (from)
    {
      next = from->next;

      if (pt_find_xasl (to, from))
	{
	  /* already on list, do nothing
	   * necessarily, the rest of the nodes are on the list,
	   * since they are linked to from.
	   */
	  from = NULL;
	}
      else
	{
	  xasl->next = from;
	  xasl = from;
	  from->next = NULL;
	  from = next;
	}
    }

  return to;
}


/*
 * pt_remove_xasl () - removes an xasl node from an xasl list
 *   return:
 *   xasl_list(in):
 *   remove(in):
 */
XASL_NODE *
pt_remove_xasl (XASL_NODE * xasl_list, XASL_NODE * remove)
{
  XASL_NODE *list = xasl_list;

  if (!list)
    {
      return list;
    }

  if (list == remove)
    {
      xasl_list = remove->next;
      remove->next = NULL;
    }
  else
    {
      while (list->next && list->next != remove)
	{
	  list = list->next;
	}

      if (list->next == remove)
	{
	  list->next = remove->next;
	  remove->next = NULL;
	}
    }

  return xasl_list;
}

/*
 * pt_set_dptr () - If this xasl node should have a dptr list from
 * 	"correlated == 1" queries, they will be set
 *   return:
 *   parser(in):
 *   node(in):
 *   xasl(in):
 *   id(in):
 */
void
pt_set_dptr (PARSER_CONTEXT * parser, PT_NODE * node, XASL_NODE * xasl,
	     UINTPTR id)
{
  if (xasl)
    {
      xasl->dptr_list = pt_remove_xasl (pt_append_xasl (xasl->dptr_list,
							pt_to_corr_subquery_list
							(parser, node, id)),
					xasl);
    }
}

/*
 * pt_set_aptr () - If this xasl node should have an aptr list from
 * 	"correlated > 1" queries, they will be set
 *   return:
 *   parser(in):
 *   select_node(in):
 *   xasl(in):
 */
static void
pt_set_aptr (PARSER_CONTEXT * parser, PT_NODE * select_node, XASL_NODE * xasl)
{
  if (xasl)
    {
      xasl->aptr_list = pt_remove_xasl (pt_append_xasl (xasl->aptr_list,
							pt_to_uncorr_subquery_list
							(parser,
							 select_node)), xasl);
    }
}


/*
 * pt_uncorr_pre () - builds xasl list of locally correlated (level 1) queries
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in/out):
 *   continue_walk(in/out):
 */
static PT_NODE *
pt_uncorr_pre (PARSER_CONTEXT * parser, PT_NODE * node,
	       void *arg, int *continue_walk)
{
  UNCORR_INFO *info = (UNCORR_INFO *) arg;

  *continue_walk = PT_CONTINUE_WALK;

  if (!PT_IS_QUERY_NODE_TYPE (node->node_type))
    {
      return node;
    }

  /* Can not increment level for list portion of walk.
   * Since those queries are not sub-queries of this query.
   * Consequently, we recurse separately for the list leading
   * from a query.  Can't just call pt_to_uncorr_subquery_list()
   * directly since it needs to do a leaf walk and we want to do a full
   * walk on the next list.
   */
  if (node->next)
    {
      node->next = parser_walk_tree (parser, node->next, pt_uncorr_pre, info,
				     pt_uncorr_post, info);
    }

  *continue_walk = PT_LEAF_WALK;

  /* increment level as we dive into subqueries */
  info->level++;

  return node;
}

/*
 * pt_uncorr_post () - decrement level of correlation after passing selects
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in/out):
 *   continue_walk(in/out):
 */
static PT_NODE *
pt_uncorr_post (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
		void *arg, UNUSED_ARG int *continue_walk)
{
  UNCORR_INFO *info = (UNCORR_INFO *) arg;
  XASL_NODE *xasl;

  switch (node->node_type)
    {
    case PT_SELECT:
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      info->level--;
      xasl = (XASL_NODE *) node->info.query.xasl;

      if (xasl && pt_is_subquery (node))
	{
	  if (node->info.query.correlation_level == 0)
	    {
	      /* add to this level */
	      node->info.query.correlation_level = info->level;
	    }

	  if (node->info.query.correlation_level == info->level)
	    {
	      /* order is important. we are on the way up, so putting things
	       * at the tail of the list will end up deeper nested queries
	       * being first, which is required.
	       */
	      info->xasl = pt_append_xasl (info->xasl, xasl);
	    }
	}

    default:
      break;
    }

  return node;
}

/*
 * pt_to_uncorr_subquery_list () - Gather the correlated level > 1 subqueries
 * 	include nested queries, such that nest level + 2 = correlation level
 *	exclude the node being passed in
 *   return:
 *   parser(in):
 *   node(in):
 */
static XASL_NODE *
pt_to_uncorr_subquery_list (PARSER_CONTEXT * parser, PT_NODE * node)
{
  UNCORR_INFO info;

  info.xasl = NULL;
  info.level = 2;

  node = parser_walk_leaves (parser, node, pt_uncorr_pre, &info,
			     pt_uncorr_post, &info);

  return info.xasl;
}

/*
 * pt_corr_pre () - builds xasl list of locally correlated (level 1) queries
 * 	directly reachable. (no nested queries, which are already handled)
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
pt_corr_pre (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node,
	     void *arg, int *continue_walk)
{
  XASL_NODE *xasl;
  CORR_INFO *info = (CORR_INFO *) arg;

  *continue_walk = PT_CONTINUE_WALK;

  switch (node->node_type)
    {
    case PT_SELECT:
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      *continue_walk = PT_LIST_WALK;
      xasl = (XASL_NODE *) node->info.query.xasl;

      if (xasl
	  && node->info.query.correlation_level == 1
	  && (info->id == MATCH_ALL || node->spec_ident == info->id))
	{
	  info->xasl_head = pt_append_xasl (xasl, info->xasl_head);
	}

    default:
      break;
    }

  return node;
}

/*
 * pt_to_corr_subquery_list () - Gather the correlated level == 1 subqueries.
 *	exclude nested queries. including the node being passed in
 *   return:
 *   parser(in):
 *   node(in):
 *   id(in):
 */
static XASL_NODE *
pt_to_corr_subquery_list (PARSER_CONTEXT * parser, PT_NODE * node, UINTPTR id)
{
  CORR_INFO info;

  info.xasl_head = NULL;
  info.id = id;

  node = parser_walk_tree (parser, node, pt_corr_pre, &info,
			   pt_continue_walk, NULL);

  return info.xasl_head;
}

/*
 * pt_to_outlist () - Convert a pt_node list to an outlist (of regu_variables)
 *   return:
 *   parser(in):
 *   node_list(in):
 *   unbox(in):
 */
static OUTPTR_LIST *
pt_to_outlist (PARSER_CONTEXT * parser, PT_NODE * node_list, UNBOX unbox)
{
  OUTPTR_LIST *outlist;
  PT_NODE *node = NULL, *node_next, *col;
  int count = 0;
  REGU_VARIABLE *regu;
  REGU_VARIABLE_LIST *regulist;
  PT_NODE *save_node = NULL, *save_next = NULL;
  XASL_NODE *xasl = NULL;
  QFILE_SORTED_LIST_ID *srlist_id;
  QPROC_DB_VALUE_LIST value_list = NULL;
  int i;
  bool skip_hidden;

  /* defense code; not permit nesting */
  if (node_list && node_list->node_type == PT_NODE_LIST)
    {
      assert (false);		/* is impossible */
      goto exit_on_error;
    }

  outlist = regu_outlist_alloc ();
  if (outlist == NULL)
    {
      PT_ERRORm (parser, node_list, MSGCAT_SET_PARSER_SEMANTIC,
		 MSGCAT_SEMANTIC_OUT_OF_MEMORY);
      goto exit_on_error;
    }

  regulist = &outlist->valptrp;

  for (node = node_list, node_next = node ? node->next : NULL;
       node != NULL; node = node_next, node_next = node ? node->next : NULL)
    {
      save_node = node;		/* save */

      CAST_POINTER_TO_NODE (node);
      if (node)
	{
	  /* reset flag for new node */
	  skip_hidden = false;

	  /* save and cut-off node link */
	  save_next = node->next;
	  node->next = NULL;

	  /* get column list */
	  col = node;
	  if (PT_IS_QUERY_NODE_TYPE (node->node_type))
	    {
	      /* hidden columns from subquery should not get referenced in
	         select list */
	      skip_hidden = true;

	      xasl = (XASL_NODE *) node->info.query.xasl;
	      if (xasl == NULL)
		{
		  goto exit_on_error;
		}

	      xasl->is_single_tuple = (unbox != UNBOX_AS_TABLE);
	      assert (xasl->is_single_tuple);
	      if (xasl->is_single_tuple)
		{
		  col = pt_get_select_list (parser, node);
		  if (!xasl->single_tuple)
		    {
		      xasl->single_tuple = pt_make_val_list (parser, col);
		      if (xasl->single_tuple == NULL)
			{
			  PT_ERRORm (parser, col, MSGCAT_SET_PARSER_SEMANTIC,
				     MSGCAT_SEMANTIC_OUT_OF_MEMORY);
			  goto exit_on_error;
			}
		    }

		  value_list = xasl->single_tuple->valp;
		}
	    }

	  /* make outlist */
	  for (i = 0; col; col = col->next, i++)
	    {
	      if (skip_hidden && col->is_hidden_column && i > 0)
		{
		  /* we don't need this node; also, we assume the first column
		     of the subquery is NOT hidden */
		  continue;
		}

	      *regulist = regu_varlist_alloc ();
	      if (*regulist == NULL)
		{
		  goto exit_on_error;
		}

	      if (PT_IS_QUERY_NODE_TYPE (node->node_type))
		{
		  regu = regu_var_alloc ();
		  if (regu == NULL)
		    {
		      goto exit_on_error;
		    }

		  if (i == 0)
		    {
		      /* set as linked to regu var */
		      XASL_SET_FLAG (xasl, XASL_LINK_TO_REGU_VARIABLE);
		      REGU_VARIABLE_XASL (regu) = xasl;
		    }

		  if (xasl->is_single_tuple)
		    {
		      regu->type = TYPE_CONSTANT;
		      regu->value.dbvalptr = value_list->val;
		      /* move to next db_value holder */
		      value_list = value_list->next;
		    }
		  else
		    {
		      srlist_id = regu_srlistid_alloc ();
		      if (srlist_id == NULL)
			{
			  goto exit_on_error;
			}

		      regu->type = TYPE_LIST_ID;
		      regu->value.srlist_id = srlist_id;
		      srlist_id->list_id = xasl->list_id;
		    }
		}
	      else if (col->node_type == PT_EXPR
		       && col->info.expr.op == PT_ORDERBY_NUM)
		{
		  regu = regu_var_alloc ();
		  if (regu == NULL)
		    {
		      goto exit_on_error;
		    }

		  regu->type = TYPE_ORDERBY_NUM;
		  regu->value.dbvalptr = (DB_VALUE *) col->etc;
		}
	      else
		{
#if 1				/* TODO - */
		  /* check iff not permit oid attr */
		  if (PT_IS_OID_NAME (col))
		    {
		      if (!PT_NAME_INFO_IS_FLAGED
			  (col, PT_NAME_INFO_GENERATED_OID))
			{
			  PT_ERRORmf (parser, node_list,
				      MSGCAT_SET_PARSER_SEMANTIC,
				      MSGCAT_SEMANTIC_IS_NOT_DEFINED,
				      pt_short_print (parser, col));

			  goto exit_on_error;
			}
		    }
#endif

		  regu = pt_to_regu_variable (parser, col, unbox);
		}

	      if (regu == NULL)
		{
		  goto exit_on_error;
		}

	      /* append to outlist */
	      (*regulist)->value = *regu;

	      regulist = &(*regulist)->next;

	      count++;
	    }			/* for (i = 0; ...) */

	  /* restore node link */
	  node->next = save_next;
	}

      node = save_node;		/* restore */
    }

  outlist->valptr_cnt = count;

  return outlist;

exit_on_error:

  /* restore node link */
  if (node)
    {
      node->next = save_next;
    }

  node = save_node;		/* restore */

  return NULL;
}


/*
 * ptqo_to_scan_proc () - Convert a spec pt_node to a SCAN_PROC
 *   return:
 *   parser(in):
 *   plan(in):
 *   xasl(in):
 *   spec(in):
 *   where_key_part(in):
 *   where_part(in):
 *   info(in):
 */
XASL_NODE *
ptqo_to_scan_proc (PARSER_CONTEXT * parser,
		   QO_PLAN * plan,
		   XASL_NODE * xasl,
		   PT_NODE * spec,
		   PT_NODE * where_key_part,
		   PT_NODE * where_part, QO_XASL_INDEX_INFO * info)
{
  if (xasl == NULL)
    {
      xasl = regu_xasl_node_alloc (SCAN_PROC);
    }

  if (!xasl)
    {
      PT_ERROR (parser, spec,
		msgcat_message (MSGCAT_CATALOG_RYE,
				MSGCAT_SET_PARSER_SEMANTIC,
				MSGCAT_SEMANTIC_OUT_OF_MEMORY));
      return NULL;
    }

  if (spec != NULL)
    {
      xasl->spec_list = pt_to_spec_list (parser, spec,
					 where_key_part, where_part, NULL,
					 plan, info, NULL);
      if (xasl->spec_list == NULL)
	{
	  goto exit_on_error;
	}

      xasl->val_list = pt_to_val_list (parser, spec->info.spec.id);
    }

  return xasl;

exit_on_error:

  return NULL;
}


/*
 * pt_skeleton_buildlist_proc () - Construct a partly
 *                                 initialized BUILDLIST_PROC
 *   return:
 *   parser(in):
 *   namelist(in):
 */
XASL_NODE *
pt_skeleton_buildlist_proc (PARSER_CONTEXT * parser, PT_NODE * namelist)
{
  XASL_NODE *xasl;

  assert (parser != NULL);

  xasl = regu_xasl_node_alloc (BUILDLIST_PROC);
  if (xasl == NULL)
    {
      goto exit_on_error;
    }

  xasl->outptr_list = pt_to_outlist (parser, namelist, UNBOX_AS_VALUE);
  if (xasl->outptr_list == NULL)
    {
      goto exit_on_error;
    }

  return xasl;

exit_on_error:

  return NULL;
}


/*
 * ptqo_to_list_scan_proc () - Convert an spec pt_node to a SCAN_PROC
 *   return:
 *   parser(in):
 *   xasl(in):
 *   proc_type(in):
 *   listfile(in):
 *   namelist(in):
 *   pred(in):
 */
XASL_NODE *
ptqo_to_list_scan_proc (PARSER_CONTEXT * parser,
			XASL_NODE * xasl,
			PROC_TYPE proc_type,
			XASL_NODE * listfile,
			PT_NODE * namelist, PT_NODE * pred)
{
  if (xasl == NULL)
    {
      xasl = regu_xasl_node_alloc (proc_type);
    }

  if (xasl && listfile)
    {
      PRED_EXPR *pred_expr = NULL;
      REGU_VARIABLE_LIST regu_attributes = NULL;
      PT_NODE *saved_current_class;
      int *attr_offsets;

      parser->symbols->listfile_unbox = UNBOX_AS_VALUE;
      parser->symbols->current_listfile = NULL;

      /* The where predicate is now evaluated after the val list has been
       * fetched.  This means that we want to generate "CONSTANT" regu
       * variables instead of "POSITION" regu variables which would happen
       * if parser->symbols->current_listfile != NULL.
       * pred should never user the current instance for fetches
       * either, so we turn off the current_class, if there is one.
       */
      saved_current_class = parser->symbols->current_class;
      parser->symbols->current_class = NULL;
      pred_expr = pt_to_pred_expr (parser, pred);
      parser->symbols->current_class = saved_current_class;

      /* Need to create a value list using the already allocated
       * DB_VALUE data buckets on some other XASL_PROC's val list.
       * Actually, these should be simply global, but aren't.
       */
      xasl->val_list = pt_clone_val_list (parser, namelist);

      /* handle the buildlist case.
       * append regu to the out_list, and create a new value
       * to append to the value_list
       */
      attr_offsets = pt_make_identity_offsets (namelist);
      regu_attributes =
	pt_to_position_regu_variable_list (parser, namelist,
					   xasl->val_list, attr_offsets);

      free_and_init (attr_offsets);

      xasl->spec_list = pt_make_list_access_spec (listfile, SEQUENTIAL, NULL,
						  pred_expr, regu_attributes,
						  NULL);

      if (xasl->spec_list == NULL || xasl->val_list == NULL)
	{
	  xasl = NULL;
	}
    }
  else
    {
      xasl = NULL;
    }

  return xasl;
}


/*
 * pt_gen_optimized_plan () - Translate a PT_SELECT node to a XASL plan
 *   return:
 *   parser(in):
 *   select_node(in):
 *   plan(in):
 *   xasl(in):
 */
static XASL_NODE *
pt_gen_optimized_plan (PARSER_CONTEXT * parser, PT_NODE * select_node,
		       QO_PLAN * plan, XASL_NODE * xasl)
{
  XASL_NODE *ret = NULL;

  assert (parser != NULL);

  if (xasl && select_node && !pt_has_error (parser))
    {
      ret = qo_to_xasl (plan, xasl);

      if (ret == NULL)
	{
	  xasl->spec_list = NULL;
	  xasl->scan_ptr = NULL;
	}
      else
	{
	  /* if the user asked for a descending scan, force it on all iscans */
	  if (select_node->info.query.q.select.hint & PT_HINT_USE_IDX_DESC)
	    {
	      XASL_NODE *ptr;
	      for (ptr = xasl; ptr; ptr = ptr->scan_ptr)
		{
		  if (ptr->spec_list && ptr->spec_list->indexptr)
		    {
		      ptr->spec_list->indexptr->use_desc_index = 1;
		    }
		}
	    }

	  if (select_node->info.query.q.select.hint & PT_HINT_NO_IDX_DESC)
	    {
	      XASL_NODE *ptr;
	      for (ptr = xasl; ptr; ptr = ptr->scan_ptr)
		{
		  if (ptr->spec_list && ptr->spec_list->indexptr)
		    {
		      ptr->spec_list->indexptr->use_desc_index = 0;
		    }
		}
	    }

	  /* check direction of the first order by column.
	   * see also scan_get_index_oidset() in scan_manager.c
	   */
	  if (xasl->spec_list && select_node->info.query.order_by
	      && xasl->spec_list->indexptr)
	    {
	      PT_NODE *ob = select_node->info.query.order_by;

	      if (ob->info.sort_spec.asc_or_desc == PT_DESC)
		{
		  xasl->spec_list->indexptr->orderby_desc = 1;
		}
	    }

	  /* check direction of the first group by column.
	   * see also scan_get_index_oidset() in scan_manager.c
	   */
	  if (xasl->spec_list
	      && select_node->info.query.q.select.group_by
	      && xasl->spec_list->indexptr)
	    {
	      PT_NODE *gb = select_node->info.query.q.select.group_by;

	      if (gb->info.sort_spec.asc_or_desc == PT_DESC)
		{
		  xasl->spec_list->indexptr->groupby_desc = 1;
		}
	    }
	}
    }

  return ret;
}

/*
 * pt_to_buildlist_proc () - Translate a PT_SELECT node to
 *                           a XASL buildlist proc
 *   return:
 *   parser(in):
 *   select_node(in):
 *   qo_plan(in):
 */
static XASL_NODE *
pt_to_buildlist_proc (PARSER_CONTEXT * parser, PT_NODE * select_node,
		      QO_PLAN * qo_plan)
{
  XASL_NODE *xasl, *save_parent_proc_xasl;
  PT_NODE *saved_current_class;
  int groupby_ok = 1;
  AGGREGATE_TYPE *aggregate = NULL;
  SYMBOL_INFO *symbols;
  PT_NODE *from, *limit;
  UNBOX unbox;
  PT_NODE *having_part, *grbynum_part;
  int grbynum_flag, ordbynum_flag;
  bool orderby_skip = false, orderby_ok = true;
  bool groupby_skip = false;
  BUILDLIST_PROC_NODE *buildlist;

  assert (parser != NULL);

  symbols = parser->symbols;
  if (symbols == NULL)
    {
      return NULL;
    }

  if (select_node == NULL || select_node->node_type != PT_SELECT)
    {
      assert (false);
      return NULL;
    }

  if (select_node->info.query.q.select.from == NULL)
    {
      return NULL;
    }

  xasl = regu_xasl_node_alloc (BUILDLIST_PROC);
  if (xasl == NULL)
    {
      return NULL;
    }

  /* save this XASL node for children to access */
  save_parent_proc_xasl = parser->parent_proc_xasl;
  parser->parent_proc_xasl = xasl;

  buildlist = &xasl->proc.buildlist;
  xasl->next = NULL;

  limit = select_node->info.query.limit;
  if (limit)
    {
      if (limit->next)
	{
	  limit = limit->next;
	}
      xasl->limit_row_count =
	pt_to_regu_variable (parser, limit, UNBOX_AS_VALUE);
    }
  else
    {
      xasl->limit_row_count = NULL;
    }

  /* set references of INST_NUM and ORDERBY_NUM values in parse tree */
  pt_set_numbering_node_etc (parser, select_node->info.query.q.select.list,
			     &xasl->instnum_val, &xasl->ordbynum_val);
  pt_set_numbering_node_etc (parser, select_node->info.query.q.select.where,
			     &xasl->instnum_val, &xasl->ordbynum_val);
  pt_set_numbering_node_etc (parser, select_node->info.query.orderby_for,
			     &xasl->instnum_val, &xasl->ordbynum_val);

  /* assume parse tree correct, and PT_DISTINCT only other possibility */
  if (select_node->info.query.all_distinct == PT_ALL)
    {
      xasl->option = Q_ALL;
    }
  else
    {
      xasl->option = Q_DISTINCT;
    }

  unbox = UNBOX_AS_VALUE;

  if (pt_has_aggregate (parser, select_node))
    {
      int *attr_offsets;
      PT_NODE *group_out_list, *group;

      group_out_list = NULL;
      for (group = select_node->info.query.q.select.group_by;
	   group; group = group->next)
	{
	  /* safe guard: invalid parse tree */
	  if (group->node_type != PT_SORT_SPEC)
	    {
	      if (group_out_list)
		{
		  parser_free_tree (parser, group_out_list);
		}
	      goto exit_on_error;
	    }

	  group_out_list =
	    parser_append_node (pt_point
				(parser, group->info.sort_spec.expr),
				group_out_list);
	}

      /* this one will be altered further on and it's the actual output of the
         initial scan; will contain group key and aggregate expressions */
      xasl->outptr_list =
	pt_to_outlist (parser, group_out_list, UNBOX_AS_VALUE);

      if (xasl->outptr_list == NULL)
	{
	  if (group_out_list)
	    {
	      parser_free_tree (parser, group_out_list);
	    }
	  goto exit_on_error;
	}

      buildlist->g_val_list = pt_make_val_list (parser, group_out_list);

      if (buildlist->g_val_list == NULL)
	{
	  PT_ERRORm (parser, group_out_list, MSGCAT_SET_PARSER_SEMANTIC,
		     MSGCAT_SEMANTIC_OUT_OF_MEMORY);
	  if (group_out_list)
	    {
	      parser_free_tree (parser, group_out_list);
	    }
	  goto exit_on_error;
	}

      attr_offsets = pt_make_identity_offsets (group_out_list);

      /* regulist for loading from listfile */
      buildlist->g_regu_list =
	pt_to_position_regu_variable_list (parser,
					   group_out_list,
					   buildlist->g_val_list,
					   attr_offsets);

      free_and_init (attr_offsets);

      aggregate = pt_to_aggregate (parser, select_node,
				   xasl->outptr_list,
				   buildlist->g_val_list,
				   buildlist->g_regu_list,
				   group_out_list, &buildlist->g_grbynum_val);

      if (pt_has_error (parser))
	{
	  goto exit_on_error;
	}

      /* set current_listfile only around call to make g_outptr_list
       * and havein_pred */
      symbols->current_listfile = group_out_list;
      symbols->listfile_value_list = buildlist->g_val_list;

      buildlist->g_outptr_list =
	pt_to_outlist (parser, select_node->info.query.q.select.list, unbox);

      if (buildlist->g_outptr_list == NULL)
	{
	  if (group_out_list)
	    {
	      parser_free_tree (parser, group_out_list);
	    }
	  goto exit_on_error;
	}

      /* pred should never user the current instance for fetches
       * either, so we turn off the current_class, if there is one. */
      saved_current_class = parser->symbols->current_class;
      parser->symbols->current_class = NULL;
      pt_split_having_grbynum (parser,
			       select_node->info.query.q.select.having,
			       &having_part, &grbynum_part);
      buildlist->g_having_pred = pt_to_pred_expr (parser, having_part);
      grbynum_flag = 0;
      buildlist->g_grbynum_pred =
	pt_to_pred_expr_with_arg (parser, grbynum_part, &grbynum_flag);
      if (grbynum_flag & PT_PRED_ARG_GRBYNUM_CONTINUE)
	{
	  buildlist->g_grbynum_flag = XASL_G_GRBYNUM_FLAG_SCAN_CONTINUE;
	}
      if (grbynum_part != NULL
	  && PT_EXPR_INFO_IS_FLAGED (grbynum_part,
				     PT_EXPR_INFO_GROUPBYNUM_LIMIT))
	{
	  if (grbynum_part->next != NULL)
	    {
	      buildlist->g_grbynum_flag |= XASL_G_GRBYNUM_FLAG_LIMIT_GT_LT;
	    }
	  else
	    {
	      buildlist->g_grbynum_flag |= XASL_G_GRBYNUM_FLAG_LIMIT_LT;
	    }
	}

      select_node->info.query.q.select.having =
	parser_append_node (having_part, grbynum_part);

      parser->symbols->current_class = saved_current_class;
      symbols->current_listfile = NULL;
      symbols->listfile_value_list = NULL;
      if (group_out_list)
	{
	  parser_free_tree (parser, group_out_list);
	}

      buildlist->g_agg_list = aggregate;

      buildlist->g_with_rollup =
	select_node->info.query.q.select.group_by->with_rollup;
    }
  else
    {
      xasl->outptr_list =
	pt_to_outlist (parser, select_node->info.query.q.select.list, unbox);

      if (xasl->outptr_list == NULL)
	{
	  goto exit_on_error;
	}
    }

  /* the calls pt_to_outlist and pt_to_spec_list
   * record information in the "symbol_info" structure
   * used by subsequent calls, and must be done first, before
   * calculating subquery lists, etc.
   */

  pt_set_aptr (parser, select_node, xasl);

  if (qo_plan == NULL
      || !pt_gen_optimized_plan (parser, select_node, qo_plan, xasl))
    {
      for (from = select_node->info.query.q.select.from; from != NULL;
	   from = from->next)
	{
	  if (from->info.spec.join_type != PT_JOIN_NONE)
	    {
	      PT_ERRORm (parser, from, MSGCAT_SET_PARSER_RUNTIME,
			 MSGCAT_RUNTIME_OUTER_JOIN_OPT_FAILED);
	      break;
	    }
	}

      xasl = NULL;		/* give up */

      goto exit_on_error;
    }

  if (xasl->outptr_list)
    {
      if (qo_plan)
	{			/* is optimized plan */
	  xasl->after_iscan_list =
	    pt_to_after_iscan (parser,
			       qo_plan_iscan_sort_list (qo_plan),
			       select_node);
	}
      else
	{
	  xasl->after_iscan_list = NULL;
	}

      if (select_node->info.query.order_by)
	{
	  ordbynum_flag = 0;
	  xasl->ordbynum_pred =
	    pt_to_pred_expr_with_arg (parser,
				      select_node->info.query.orderby_for,
				      &ordbynum_flag);
	  if (ordbynum_flag & PT_PRED_ARG_ORDBYNUM_CONTINUE)
	    {
	      xasl->ordbynum_flag = XASL_ORDBYNUM_FLAG_SCAN_CONTINUE;
	    }

	  /* check order by opt */
	  if (qo_plan && qo_plan_skip_orderby (qo_plan)
	      && !qo_plan_multi_range_opt (qo_plan))
	    {
	      orderby_skip = true;

	      /* move orderby_num() to inst_num() */
	      if (xasl->ordbynum_val)
		{
		  if (xasl->instnum_pred)
		    {
		      PRED_EXPR *pred =
			pt_make_pred_expr_pred (xasl->instnum_pred,
						xasl->ordbynum_pred, B_AND);
		      if (!pred)
			{
			  goto exit_on_error;
			}
		      xasl->instnum_pred = pred;
		    }
		  else
		    {
		      xasl->instnum_pred = xasl->ordbynum_pred;
		    }

		  /* When we set instnum_val to point to the DBVALUE
		   * referenced by ordbynum_val, we lose track the DBVALUE
		   * originally stored in instnum_val. This is an important
		   * value because it is referenced by any regu var that was
		   * converted from ROWNUM in the select list (before we knew
		   * we were going to optimize away the ORDER BY clause).
		   * We will save the dbval in save_instnum_val and update it
		   * whenever we update the new instnum_val.
		   */
		  xasl->save_instnum_val = xasl->instnum_val;
		  xasl->instnum_val = xasl->ordbynum_val;
		  xasl->instnum_flag = xasl->ordbynum_flag;

		  xasl->ordbynum_pred = NULL;
		  xasl->ordbynum_val = NULL;
		  xasl->ordbynum_flag = 0;
		}
	    }
	  else
	    {
	      xasl->orderby_list =
		pt_to_orderby (parser,
			       select_node->info.query.order_by, select_node);
	      /* clear flag */
	      XASL_CLEAR_FLAG (xasl, XASL_SKIP_ORDERBY_LIST);
	    }

	  /* sanity check */
	  orderby_ok = ((xasl->orderby_list != NULL) || orderby_skip);
	}
      else if (select_node->info.query.order_by == NULL
	       && select_node->info.query.orderby_for != NULL
	       && xasl->option == Q_DISTINCT)
	{
	  ordbynum_flag = 0;
	  xasl->ordbynum_pred =
	    pt_to_pred_expr_with_arg (parser,
				      select_node->info.query.orderby_for,
				      &ordbynum_flag);
	  if (ordbynum_flag & PT_PRED_ARG_ORDBYNUM_CONTINUE)
	    {
	      xasl->ordbynum_flag = XASL_ORDBYNUM_FLAG_SCAN_CONTINUE;
	    }
	}

      /* union fields for BUILDLIST_PROC_NODE - BUILDLIST_PROC */
      if (select_node->info.query.q.select.group_by)
	{
	  if (qo_plan && qo_plan_skip_groupby (qo_plan))
	    {
	      groupby_skip = true;
	    }

	  /* finish group by processing */
	  buildlist->groupby_list =
	    pt_to_groupby (parser,
			   select_node->info.query.q.select.group_by,
			   select_node);

	  /* Build SORT_LIST of the list file created by GROUP BY */
	  buildlist->after_groupby_list =
	    pt_to_after_groupby (parser,
				 select_node->info.query.q.select.group_by,
				 select_node);

	  /* This is a having subquery list. If it has correlated
	   * subqueries, they must be run each group */
	  buildlist->eptr_list =
	    pt_to_corr_subquery_list (parser,
				      select_node->info.query.q.select.having,
				      0);
	  assert (buildlist->eptr_list == NULL);	/* TODO - trace */

	  /* otherwise should be run once, at beginning.
	     these have already been put on the aptr list above */
	  groupby_ok = (buildlist->groupby_list
			&& buildlist->g_outptr_list
			&& (buildlist->g_having_pred
			    || buildlist->g_grbynum_pred
			    || !select_node->info.query.q.select.having));

	  if (groupby_skip)
	    {
	      groupby_ok = 1;
	    }
	}
      else
	{
	  /* with no group by, a build-list proc should not be built
	     a build-value proc should be built instead */
	  buildlist->groupby_list = NULL;
	  buildlist->g_regu_list = NULL;
	  buildlist->g_val_list = NULL;
	  buildlist->g_having_pred = NULL;
	  buildlist->g_grbynum_pred = NULL;
	  buildlist->g_grbynum_val = NULL;
	  buildlist->g_grbynum_flag = 0;
	  buildlist->g_agg_list = NULL;
	  buildlist->eptr_list = NULL;
	  buildlist->g_with_rollup = 0;
	}

      EHID_SET_NULL (&(buildlist->upd_del_ehid));

      /* save single tuple info */
      if (select_node->info.query.single_tuple == 1)
	{
	  xasl->is_single_tuple = true;
	}
    }				/* end xasl->outptr_list */

  /* verify everything worked */
  if (!xasl->outptr_list || !xasl->spec_list || !xasl->val_list
      || !groupby_ok || !orderby_ok || pt_has_error (parser))
    {
      goto exit_on_error;
    }

  /* convert instnum to key limit (optimization) */
  if (pt_instnum_to_key_limit (parser, qo_plan, xasl) != NO_ERROR)
    {
      goto exit_on_error;
    }

  xasl->orderby_limit = NULL;
  if (xasl->ordbynum_pred)
    {
      QO_LIMIT_INFO *limit_infop =
	qo_get_key_limit_from_ordbynum (parser, qo_plan, xasl, false);
      if (limit_infop)
	{
	  xasl->orderby_limit = limit_infop->upper;
	  free_and_init (limit_infop);
	}
    }

  if (pt_set_limit_optimization_flags (parser, qo_plan, xasl) != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* restore old parent xasl */
  parser->parent_proc_xasl = save_parent_proc_xasl;

  return xasl;

exit_on_error:

  /* restore old parent xasl */
  parser->parent_proc_xasl = save_parent_proc_xasl;

  return NULL;
}

/*
 * pt_to_buildvalue_proc () - Make a buildvalue xasl proc
 *   return:
 *   parser(in):
 *   select_node(in):
 *   qo_plan(in):
 */
static XASL_NODE *
pt_to_buildvalue_proc (PARSER_CONTEXT * parser, PT_NODE * select_node,
		       QO_PLAN * qo_plan)
{
  XASL_NODE *xasl, *save_parent_proc_xasl;
  BUILDVALUE_PROC_NODE *buildvalue;
  AGGREGATE_TYPE *aggregate;
  PT_NODE *saved_current_class;
  XASL_NODE *dptr_head;

  if (!select_node || select_node->node_type != PT_SELECT ||
      !select_node->info.query.q.select.from)
    {
      return NULL;
    }

  xasl = regu_xasl_node_alloc (BUILDVALUE_PROC);
  if (!xasl)
    {
      return NULL;
    }

  /* save parent xasl */
  save_parent_proc_xasl = parser->parent_proc_xasl;
  parser->parent_proc_xasl = xasl;

  buildvalue = &xasl->proc.buildvalue;
  xasl->next = NULL;

  /* set references of INST_NUM and ORDERBY_NUM values in parse tree */
  pt_set_numbering_node_etc (parser, select_node->info.query.q.select.list,
			     &xasl->instnum_val, &xasl->ordbynum_val);
  pt_set_numbering_node_etc (parser, select_node->info.query.q.select.where,
			     &xasl->instnum_val, &xasl->ordbynum_val);
  pt_set_numbering_node_etc (parser, select_node->info.query.orderby_for,
			     &xasl->instnum_val, &xasl->ordbynum_val);

  /* assume parse tree correct, and PT_DISTINCT only other possibility */
  xasl->option = ((select_node->info.query.all_distinct == PT_ALL)
		  ? Q_ALL : Q_DISTINCT);

  aggregate = pt_to_aggregate (parser, select_node, NULL, NULL,
			       NULL, NULL, &buildvalue->grbynum_val);

  /* the calls pt_to_out_list, pt_to_spec_list, and pt_to_if_pred,
   * record information in the "symbol_info" structure
   * used by subsequent calls, and must be done first, before
   * calculating subquery lists, etc.
   */
  xasl->outptr_list = pt_to_outlist (parser,
				     select_node->info.query.q.select.list,
				     UNBOX_AS_VALUE);

  if (xasl->outptr_list == NULL)
    {
      goto exit_on_error;
    }

  pt_set_aptr (parser, select_node, xasl);

  if (!qo_plan || !pt_gen_optimized_plan (parser, select_node, qo_plan, xasl))
    {
      PT_NODE *from;

      from = select_node->info.query.q.select.from;
      while (from)
	{
	  if (from->info.spec.join_type != PT_JOIN_NONE)
	    {
	      PT_ERRORm (parser, from, MSGCAT_SET_PARSER_RUNTIME,
			 MSGCAT_RUNTIME_OUTER_JOIN_OPT_FAILED);
	      goto exit_on_error;
	    }
	  from = from->next;
	}

      xasl = NULL;		/* give up */

      goto exit_on_error;
    }

  /* save info for derived table size estimation */
  xasl->projected_size = 1;
  xasl->cardinality = 1.0;

  /* pred should never user the current instance for fetches
   * either, so we turn off the current_class, if there is one.
   */
  saved_current_class = parser->symbols->current_class;
  parser->symbols->current_class = NULL;
  buildvalue->having_pred =
    pt_to_pred_expr (parser, select_node->info.query.q.select.having);
  parser->symbols->current_class = saved_current_class;

  if (xasl->scan_ptr)
    {
      dptr_head = xasl->scan_ptr;
      while (dptr_head->scan_ptr)
	{
	  dptr_head = dptr_head->scan_ptr;
	}
    }
  else
    {
      dptr_head = xasl;
    }
  pt_set_dptr (parser, select_node->info.query.q.select.having,
	       dptr_head, MATCH_ALL);

  /* union fields from BUILDVALUE_PROC_NODE - BUILDVALUE_PROC */
  buildvalue->agg_list = aggregate;

  /* this is not useful, set it to NULL.
   * it was set by the old parser, and apparently used, but the use was
   * apparently redundant.
   */
  buildvalue->outarith_list = NULL;

  /* verify everything worked */
  if (!xasl->outptr_list ||
      !xasl->spec_list || !xasl->val_list || pt_has_error (parser))
    {
      goto exit_on_error;
    }

  /* convert instnum to key limit (optimization) */
  if (pt_instnum_to_key_limit (parser, qo_plan, xasl) != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* convert ordbynum to key limit if we have iscan with multiple key ranges */
  if (qo_plan && qo_plan_multi_range_opt (qo_plan))
    {
      if (pt_ordbynum_to_key_limit_multiple_ranges (parser, qo_plan, xasl) !=
	  NO_ERROR)
	{
	  goto exit_on_error;
	}
    }

  /* restore old parent xasl */
  parser->parent_proc_xasl = save_parent_proc_xasl;

  return xasl;

exit_on_error:

  /* restore old parent xasl */
  parser->parent_proc_xasl = save_parent_proc_xasl;

  return NULL;
}


/*
 * pt_to_union_proc () - converts a PT_NODE tree of a query
 * 	                 union/intersection/difference to an XASL tree
 *   return: XASL_NODE, NULL indicates error
 *   parser(in): context
 *   node(in): a query union/difference/intersection
 *   type(in): xasl PROC type
 */
static XASL_NODE *
pt_to_union_proc (PARSER_CONTEXT * parser, PT_NODE * node, PROC_TYPE type)
{
  XASL_NODE *xasl = NULL;
  XASL_NODE *left, *right = NULL;
  SORT_LIST *orderby = NULL;
  int ordbynum_flag;

  /* note that PT_UNION, PT_DIFFERENCE, and PT_INTERSECTION node types
   * share the same node structure */
  left = (XASL_NODE *) node->info.query.q.union_.arg1->info.query.xasl;
  right = (XASL_NODE *) node->info.query.q.union_.arg2->info.query.xasl;

  /* orderby can legitimately be null */
  orderby = pt_to_orderby (parser, node->info.query.order_by, node);

  if (left && right && (orderby || !node->info.query.order_by))
    {
      /* don't allocate till everything looks ok. */
      xasl = regu_xasl_node_alloc (type);
    }

  if (xasl)
    {
      xasl->proc.union_.left = left;
      xasl->proc.union_.right = right;

      /* assume parse tree correct, and PT_DISTINCT only other possibility */
      xasl->option = (node->info.query.all_distinct == PT_ALL)
	? Q_ALL : Q_DISTINCT;

      xasl->orderby_list = orderby;

      /* clear flag */
      XASL_CLEAR_FLAG (xasl, XASL_SKIP_ORDERBY_LIST);

      /* save single tuple info */
      if (node->info.query.single_tuple == 1)
	{
	  xasl->is_single_tuple = true;
	}

      /* set 'etc' field of PT_NODEs which belong to inst_num() and
         orderby_num() expression in order to use at
         pt_make_regu_numbering() */
      pt_set_numbering_node_etc (parser,
				 node->info.query.orderby_for,
				 NULL, &xasl->ordbynum_val);
      ordbynum_flag = 0;
      xasl->ordbynum_pred =
	pt_to_pred_expr_with_arg (parser,
				  node->info.query.orderby_for,
				  &ordbynum_flag);

      if (ordbynum_flag & PT_PRED_ARG_ORDBYNUM_CONTINUE)
	{
	  xasl->ordbynum_flag = XASL_ORDBYNUM_FLAG_SCAN_CONTINUE;
	}

      pt_set_aptr (parser, node, xasl);

      /* save info for derived table size estimation */
      switch (node->node_type)
	{
	case PT_UNION:
	  xasl->projected_size =
	    MAX (left->projected_size, right->projected_size);
	  xasl->cardinality = left->cardinality + right->cardinality;
	  break;
	case PT_DIFFERENCE:
	  xasl->projected_size = left->projected_size;
	  xasl->cardinality = left->cardinality;
	  break;
	case PT_INTERSECTION:
	  xasl->projected_size =
	    MAX (left->projected_size, right->projected_size);
	  xasl->cardinality = MIN (left->cardinality, right->cardinality);
	  break;
	default:
	  break;
	}

      if (node->info.query.limit)
	{
	  PT_NODE *limit;

	  limit = node->info.query.limit;
	  if (limit->next)
	    {
	      limit = limit->next;
	    }
	  xasl->limit_row_count = pt_to_regu_variable (parser, limit,
						       UNBOX_AS_VALUE);
	}
    }				/* end xasl */
  else
    {
      xasl = NULL;
    }

  return xasl;
}


/*
 * pt_plan_set_query () - converts a PT_NODE tree of
 *                        a query union to an XASL tree
 *   return: XASL_NODE, NULL indicates error
 *   parser(in): context
 *   node(in): a query union/difference/intersection
 *   proc_type(in): xasl PROC type
 */
static XASL_NODE *
pt_plan_set_query (PARSER_CONTEXT * parser, PT_NODE * node,
		   PROC_TYPE proc_type)
{
  XASL_NODE *xasl;

  /* no optimization for now */
  xasl = pt_to_union_proc (parser, node, proc_type);

  return xasl;
}

/*
 * pt_plan_query () -
 *   return: XASL_NODE, NULL indicates error
 *   parser(in): context
 *   select_node(in): of PT_SELECT type
 */
static XASL_NODE *
pt_plan_query (PARSER_CONTEXT * parser, PT_NODE * select_node)
{
  XASL_NODE *xasl;
  QO_PLAN *plan = NULL;
  int level;
#if defined (ENABLE_UNUSED_FUNCTION)
  int trace_format;
#endif
  bool hint_ignored = false;

  if (select_node->node_type != PT_SELECT)
    {
      return NULL;
    }

  /* Check for join, path expr, and index optimizations */
  plan = qo_optimize_query (parser, select_node);

  /* optimization fails, ignore join hint and retry optimization */
  if (!plan && select_node->info.query.q.select.hint != PT_HINT_NONE)
    {
      hint_ignored = true;

      /* init hint */
      select_node->info.query.q.select.hint = PT_HINT_NONE;

#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
      if (select_node->info.query.q.select.ordered_hint)
	{
	  parser_free_tree (parser,
			    select_node->info.query.q.select.ordered_hint);
	  select_node->info.query.q.select.ordered_hint = NULL;
	}
#endif

      if (select_node->info.query.q.select.use_nl_hint)
	{
	  parser_free_tree (parser,
			    select_node->info.query.q.select.use_nl_hint);
	  select_node->info.query.q.select.use_nl_hint = NULL;
	}
      if (select_node->info.query.q.select.use_idx_hint)
	{
	  parser_free_tree (parser,
			    select_node->info.query.q.select.use_idx_hint);
	  select_node->info.query.q.select.use_idx_hint = NULL;
	}

      select_node->alias_print = NULL;

#if defined(RYE_DEBUG)
      PT_NODE_PRINT_TO_ALIAS (parser, select_node, PT_CONVERT_RANGE);
#endif /* RYE_DEBUG */

      plan = qo_optimize_query (parser, select_node);
    }

  if (pt_is_single_tuple (parser, select_node))
    {
      xasl = pt_to_buildvalue_proc (parser, select_node, plan);
    }
  else
    {
      xasl = pt_to_buildlist_proc (parser, select_node, plan);
    }

  /* Print out any needed post-optimization info.  Leave a way to find
   * out about environment info if we aren't able to produce a plan.
   * If this happens in the field at least we'll be able to glean some info */
  qo_get_optimization_param (&level, QO_PARAM_LEVEL);
  if (level >= 0x100 && plan)
    {
      if (query_Plan_dump_fp == NULL)
	{
	  query_Plan_dump_fp = stdout;
	}
      fputs ("\nQuery plan:\n", query_Plan_dump_fp);
      qo_plan_dump (plan, query_Plan_dump_fp);
    }

  if (level >= 0x100)
    {
      unsigned int save_custom;

      if (query_Plan_dump_fp == NULL)
	{
	  query_Plan_dump_fp = stdout;
	}

      if (DETAILED_DUMP (level))
	{
	  save_custom = parser->custom_print;
	  parser->custom_print |= PT_CONVERT_RANGE;
	  fprintf (query_Plan_dump_fp, "\nQuery stmt:%s\n\n%s\n\n",
		   ((hint_ignored) ? " [Warning: HINT ignored]" : ""),
		   parser_print_tree (parser, select_node));
	  parser->custom_print = save_custom;
	}

      if (xasl != NULL && plan != NULL)
	{
	  if (select_node->info.query.order_by
	      && qo_plan_skip_orderby (plan)
	      && !qo_plan_multi_range_opt (plan))
	    {
	      if (xasl->spec_list && xasl->spec_list->indexptr)
		{
//              assert (xasl->spec_list->indexptr->orderby_skip);
		  if (DETAILED_DUMP (level))
		    {
		      fprintf (query_Plan_dump_fp,
			       "/* ---> skip ORDER BY */\n");
		    }
		  else if (SIMPLE_DUMP (level))
		    {
		      fprintf (query_Plan_dump_fp, " skip ORDER BY\n");
		    }
		}
	    }

	  if (select_node->info.query.q.select.group_by
	      && qo_plan_skip_groupby (plan))
	    {
	      if (xasl->spec_list && xasl->spec_list->indexptr)
		{
//              assert (xasl->spec_list->indexptr->groupby_skip);
		  if (DETAILED_DUMP (level))
		    {
		      fprintf (query_Plan_dump_fp,
			       "/* ---> skip GROUP BY */\n");
		    }
		  else if (SIMPLE_DUMP (level))
		    {
		      fprintf (query_Plan_dump_fp, " skip GROUP BY\n");
		    }
		}
	    }
	}
    }

  if (xasl != NULL && plan != NULL)
    {
      int plan_len;
      size_t sizeloc;
      char *ptr, *sql_plan = NULL;
      COMPILE_CONTEXT *contextp;
      FILE *fp;

      contextp = &parser->context;
      fp = port_open_memstream (&ptr, &sizeloc);
      if (fp)
	{
	  qo_plan_lite_print (plan, fp, 0);
	  if (select_node->info.query.order_by
	      && qo_plan_skip_orderby (plan)
	      && !qo_plan_multi_range_opt (plan))
	    {
	      if (xasl->spec_list && xasl->spec_list->indexptr)
		{
//            assert (&& xasl->spec_list->indexptr->orderby_skip);
		  fprintf (fp, "\n skip ORDER BY\n");
		}
	    }

	  if (select_node->info.query.q.select.group_by
	      && qo_plan_skip_groupby (plan))
	    {
	      if (xasl->spec_list && xasl->spec_list->indexptr)
		{
//              assert (xasl->spec_list->indexptr->groupby_skip);
		  fprintf (fp, "\n skip GROUP BY\n");
		}
	    }

	  port_close_memstream (fp, &ptr, &sizeloc);

	  if (ptr)
	    {
	      sql_plan = pt_alloc_packing_buf (sizeloc + 1);
	      if (sql_plan == NULL)
		{
		  goto error_exit;
		}

	      strncpy (sql_plan, ptr, sizeloc);
	      sql_plan[sizeloc] = '\0';
	      free (ptr);
	    }
	}

      if (sql_plan)
	{
	  char *plan_text = NULL;

	  plan_len = strlen (sql_plan);

	  if (contextp->sql_plan_alloc_size == 0)
	    {
	      int size = MAX (1024, plan_len * 2);

	      contextp->sql_plan_text = NULL;
	      plan_text = parser_alloc (parser, size);
	      if (plan_text == NULL)
		{
		  goto error_exit;
		}
	      plan_text[0] = '\0';

	      contextp->sql_plan_alloc_size = size;
	      contextp->sql_plan_text = plan_text;
	    }
	  else if (contextp->sql_plan_alloc_size -
		   strlen (contextp->sql_plan_text) < plan_len)
	    {
	      char *ptr;
	      int size = (contextp->sql_plan_alloc_size + plan_len) * 2;

	      ptr = parser_alloc (parser, size);
	      if (ptr == NULL)
		{
		  goto error_exit;
		}

	      ptr[0] = '\0';
	      strcpy (ptr, contextp->sql_plan_text);

	      contextp->sql_plan_text = ptr;
	      contextp->sql_plan_alloc_size = size;
	    }

	  strcat ((char *) (contextp->sql_plan_text), sql_plan);
	}
    }

#if defined (ENABLE_UNUSED_FUNCTION)
  if (parser->query_trace == true && !qo_need_skip_execution ()
      && plan != NULL && xasl != NULL)
    {
      trace_format = prm_get_integer_value (PRM_ID_QUERY_TRACE_FORMAT);

      if (trace_format == QUERY_TRACE_TEXT)
	{
	  qo_top_plan_print_text (parser, xasl, select_node, plan);
	}
      else if (trace_format == QUERY_TRACE_JSON)
	{
	  qo_top_plan_print_json (parser, xasl, select_node, plan);
	}
    }
#endif

error_exit:
  if (plan != NULL)
    {
      qo_plan_discard (plan);
    }

  return xasl;
}


/*
 * parser_generate_xasl_proc () - Creates xasl proc for parse tree.
 * 	Also used for direct recursion, not for subquery recursion
 *   return:
 *   parser(in):
 *   node(in): pointer to a query structure
 *   query_list(in): pointer to the generated xasl-tree
 */
static XASL_NODE *
parser_generate_xasl_proc (PARSER_CONTEXT * parser, PT_NODE * node,
			   PT_NODE * query_list)
{
  XASL_NODE *xasl = NULL;
  PT_NODE *query;
  bool query_Plan_dump_fp_open = false;

  /* we should propagate abort error from the server */
  if (PT_IS_QUERY (node))
    {
      /* check for cached query xasl */
      for (query = query_list; query; query = query->next)
	{
	  if (query->info.query.xasl
	      && query->info.query.id == node->info.query.id)
	    {
	      /* found cached query xasl */
	      node->info.query.xasl = query->info.query.xasl;
	      node->info.query.correlation_level
		= query->info.query.correlation_level;

	      return (XASL_NODE *) node->info.query.xasl;
	    }
	}			/* for (query = ... ) */

      /* not found cached query xasl */
      switch (node->node_type)
	{
	case PT_SELECT:
	  /* This function is reenterable by pt_plan_query
	   * so, query_Plan_dump_fp should be open once at first call
	   * and be closed at that call.
	   */
	  if (query_Plan_dump_filename != NULL)
	    {
	      if (query_Plan_dump_fp == NULL || query_Plan_dump_fp == stdout)
		{
		  query_Plan_dump_fp = fopen (query_Plan_dump_filename, "a");
		  if (query_Plan_dump_fp != NULL)
		    {
		      query_Plan_dump_fp_open = true;
		    }
		}
	    }

	  if (query_Plan_dump_fp == NULL)
	    {
	      query_Plan_dump_fp = stdout;
	    }

	  node->info.query.xasl = xasl = pt_plan_query (parser, node);

	  /* close file handle if this function open it */
	  if (query_Plan_dump_fp_open == true)
	    {
	      assert (query_Plan_dump_fp != NULL
		      && query_Plan_dump_fp != stdout);

	      fclose (query_Plan_dump_fp);
	      query_Plan_dump_fp = stdout;
	    }
	  break;

	case PT_UNION:
	  xasl = pt_plan_set_query (parser, node, UNION_PROC);
	  node->info.query.xasl = xasl;
	  break;

	case PT_DIFFERENCE:
	  xasl = pt_plan_set_query (parser, node, DIFFERENCE_PROC);
	  node->info.query.xasl = xasl;
	  break;

	case PT_INTERSECTION:
	  xasl = pt_plan_set_query (parser, node, INTERSECTION_PROC);
	  node->info.query.xasl = xasl;
	  break;

	default:
	  if (er_errid () == NO_ERROR && !pt_has_error (parser))
	    {
	      PT_INTERNAL_ERROR (parser, "generate xasl");
	    }
	  /* should never get here */
	  break;
	}
    }

  if (pt_has_error (parser))
    {
      xasl = NULL;		/* signal error occurred */
    }

  if (xasl)
    {
      if (node->node_type == PT_SELECT && node->info.query.xasl)
	{
	  assert (node->info.query.upd_del_class_cnt <= 1);
	  xasl->upd_del_class_cnt = node->info.query.upd_del_class_cnt;
	}

      /* set as zero correlation-level; this uncorrelated subquery need to
       * be executed at most one time */
      if (node->info.query.correlation_level == 0)
	{
	  XASL_SET_FLAG (xasl, XASL_ZERO_CORR_LEVEL);
	}

/* BUG FIX - COMMENT OUT: DO NOT REMOVE ME FOR USE IN THE FUTURE */
#if 0
      /* cache query xasl */
      if (node->info.query.id)
	{
	  query = parser_new_node (parser, node->node_type);
	  query->info.query.id = node->info.query.id;
	  query->info.query.xasl = node->info.query.xasl;
	  query->info.query.correlation_level =
	    node->info.query.correlation_level;

	  query_list = parser_append_node (query, query_list);
	}
#endif /* 0 */
    }
  else
    {
      if (er_errid () == NO_ERROR && !pt_has_error (parser))
	{
	  PT_INTERNAL_ERROR (parser, "generate xasl");
	}
    }

  return xasl;
}

/*
 * pt_spec_to_xasl_class_oid_list () - get class OID list
 *                                     from the spec node list
 *   return:
 *   parser(in):
 *   spec(in):
 *   oid_listp(out):
 *   tcard_listp(out):
 *   nump(out):
 *   sizep(out):
 */
static int
pt_spec_to_xasl_class_oid_list (UNUSED_ARG PARSER_CONTEXT * parser,
				const PT_NODE * spec, OID ** oid_listp,
				int **tcard_listp, int *nump, int *sizep)
{
  PT_NODE *flat;
  OID *oid, *v_oid, *o_list = NULL;
  int *t_list = NULL;
  DB_OBJECT *class_obj;
  SM_CLASS *smclass;
  void *oldptr;
  size_t o_num, o_size, prev_o_num;

  if (*oid_listp == NULL || *tcard_listp == NULL)
    {
      *oid_listp = (OID *) malloc (sizeof (OID) * OID_LIST_GROWTH);
      *tcard_listp = (int *) malloc (sizeof (int) * OID_LIST_GROWTH);
      *sizep = OID_LIST_GROWTH;
    }

  if (*oid_listp == NULL || *tcard_listp == NULL || *nump >= *sizep)
    {
      goto error;
    }

  o_num = *nump;
  o_size = *sizep;
  o_list = *oid_listp;
  t_list = *tcard_listp;

  /* traverse spec list which is a FROM clause */
  for (; spec; spec = spec->next)
    {
      /* traverse flat entity list which are resolved classes */
      for (flat = spec->info.spec.flat_entity_list; flat; flat = flat->next)
	{
	  /* get the OID of the class object which is fetched before */
	  oid = ((flat->info.name.db_object != NULL)
		 ? ws_identifier (flat->info.name.db_object) : NULL);
	  v_oid = NULL;
	  while (oid != NULL)
	    {
	      prev_o_num = o_num;
	      (void) lsearch (oid, o_list, &o_num, sizeof (OID), oid_compare);

	      if (o_num > prev_o_num && o_num > (size_t) (*nump))
		{
		  *(t_list + o_num - 1) = -1;	/* init #pages */

		  /* get #pages of the given class
		   */
		  class_obj = flat->info.name.db_object;

		  assert (class_obj != NULL);
		  assert (locator_is_class (class_obj));
		  assert (!OID_ISTEMP (WS_OID (class_obj)));

		  if (class_obj != NULL
		      && locator_is_class (class_obj)
		      && !OID_ISTEMP (WS_OID (class_obj)))
		    {
		      if (au_fetch_class (class_obj, &smclass, S_LOCK,
					  AU_SELECT) == NO_ERROR)
			{
			  if (smclass && smclass->stats)
			    {
			      assert (smclass->stats->heap_num_pages >= 0);
			      *(t_list + o_num - 1) =
				smclass->stats->heap_num_pages;
			    }
			}
		    }
		}

	      if (o_num >= o_size)
		{
		  o_size += OID_LIST_GROWTH;
		  oldptr = (void *) o_list;
		  o_list = (OID *) realloc (o_list, o_size * sizeof (OID));
		  if (o_list == NULL)
		    {
		      free_and_init (oldptr);
		      *oid_listp = NULL;
		      goto error;
		    }

		  oldptr = (void *) t_list;
		  t_list = (int *) realloc (t_list, o_size * sizeof (int));
		  if (t_list == NULL)
		    {
		      free_and_init (oldptr);
		      free_and_init (o_list);
		      *oid_listp = NULL;
		      *tcard_listp = NULL;
		      goto error;
		    }
		}

	      if (v_oid == NULL)
		{
		  /* get the OID of the view object */
		  v_oid = ((flat->info.name.virt_object != NULL)
			   ? ws_identifier (flat->info.name.
					    virt_object) : NULL);
		  oid = v_oid;
		}
	      else
		{
		  break;
		}
	    }
	}
    }

  *nump = o_num;
  *sizep = o_size;
  *oid_listp = o_list;
  *tcard_listp = t_list;

  return o_num;

error:
  if (o_list)
    {
      free_and_init (o_list);
    }

  if (t_list)
    {
      free_and_init (t_list);
    }

  *nump = *sizep = 0;
  *oid_listp = NULL;
  *tcard_listp = NULL;

  return -1;
}


/*
 * pt_make_aptr_parent_node () - Builds a BUILDLIST proc for the query node and
 *				 attaches it as the aptr to the xasl node.
 *				 A list scan spec from the aptr's list file is
 *				 attached to the xasl node.
 *
 * return      : XASL node.
 * parser (in) : Parser context.
 * node (in)   : Parser node containing sub-query.
 * type (in)   : XASL proc type.
 *
 * NOTE: This function should not be used in the INSERT ... VALUES case.
 */
static XASL_NODE *
pt_make_aptr_parent_node (PARSER_CONTEXT * parser, PT_NODE * node,
			  PROC_TYPE type)
{
  XASL_NODE *aptr = NULL;
  XASL_NODE *xasl = NULL;
  REGU_VARIABLE_LIST regu_attributes;

  assert (node != NULL);

  xasl = regu_xasl_node_alloc (type);
  if (xasl && node)
    {
      if (PT_IS_QUERY_NODE_TYPE (node->node_type))
	{
	  PT_NODE *namelist;

	  namelist = NULL;

	  aptr = parser_generate_xasl (parser, node);
	  if (aptr)
	    {
	      XASL_CLEAR_FLAG (aptr, XASL_TOP_MOST_XASL);

	      if (type == UPDATE_PROC)
		{
		  PT_NODE *col;

		  for (col = pt_get_select_list (parser, node);
		       col; col = col->next)
		    {
		      if (PT_IS_QUERY_NODE_TYPE (col->node_type))
			{
			  namelist = parser_append_node
			    (pt_point_l
			     (parser, pt_get_select_list (parser,
							  col)), namelist);
			}
		      else
			{
			  namelist =
			    parser_append_node (pt_point (parser, col),
						namelist);
			}
		    }
		}
	      else
		{
		  namelist = pt_get_select_list (parser, node);
		}

	      aptr->next = (XASL_NODE *) 0;
	      xasl->aptr_list = aptr;
	      xasl->val_list = pt_make_val_list (parser, namelist);
	      if (xasl->val_list)
		{
		  int *attr_offsets;

		  attr_offsets = pt_make_identity_offsets (namelist);
		  regu_attributes = pt_to_position_regu_variable_list
		    (parser, namelist, xasl->val_list, attr_offsets);
		  free_and_init (attr_offsets);

		  if (regu_attributes)
		    {
		      xasl->spec_list = pt_make_list_access_spec
			(aptr, SEQUENTIAL, NULL, NULL, regu_attributes, NULL);
		    }
		}
	      else
		{
		  PT_ERRORm (parser, namelist, MSGCAT_SET_PARSER_SEMANTIC,
			     MSGCAT_SEMANTIC_OUT_OF_MEMORY);
		}

	      if (type == UPDATE_PROC && namelist)
		{
		  parser_free_tree (parser, namelist);
		}
	    }
	  if (type == INSERT_PROC)
	    {
	      xasl->proc.insert.no_val_lists = 0;
	      xasl->proc.insert.valptr_lists = NULL;
	    }
	}
      else
	{
	  /* Shouldn't be here */
	  assert (false);
	  return NULL;
	}
    }

  if (pt_has_error (parser))
    {
      pt_report_to_ersys (parser, PT_SEMANTIC);
      goto exit_on_error;
    }

  return xasl;

exit_on_error:

  return NULL;
}


/*
 * pt_to_constraint_pred () - Builds predicate of NOT NULL conjuncts.
 * 	Then generates the corresponding filter predicate
 *   return: NO_ERROR on success, non-zero for ERROR
 *   parser(in):
 *   xasl(in): value list contains the attributes the predicate must point to
 *   spec(in): spec that generated the list file for the above value list
 *   non_null_attrs(in): list of attributes to make into a constraint pred
 *   attr_list(in): corresponds to the list file's value list positions
 *   attr_offset(in): the additional offset into the value list. This is
 * 		      necessary because the update prepends 1 column for
 *		      each class that will be updated on the select list of
 *		      the aptr query
 *
 *   NOTE: on outer joins, the OID of a node in not_null_attrs can be null.
 *	In this case, constraint verification should be skipped, because there
 *	will be nothing to update.
 */
static int
pt_to_constraint_pred (PARSER_CONTEXT * parser, XASL_NODE * xasl,
		       PT_NODE * spec, PT_NODE * non_null_attrs,
		       PT_NODE * attr_list, int attr_offset)
{
  PT_NODE *pt_pred =
    NULL, *node, *conj, *next, *oid_is_null_expr, *constraint;
  PT_NODE *name, *spec_list = NULL;
  PRED_EXPR *pred = NULL;
  TABLE_INFO *ti = NULL;

  assert (parser != NULL);
  assert (xasl != NULL);
  assert (spec != NULL);

  node = non_null_attrs;

  parser->symbols = pt_symbol_info_alloc ();
  if (parser->symbols == NULL)
    {
      goto outofmem;
    }

  while (node)
    {
      /* we don't want a DAG so we need to NULL the next pointer as
       * we create a conjunct for each of the non_null_attrs.  Thus
       * we must save the next pointer for the loop.
       */
      next = node->next;
      node->next = NULL;

      constraint = parser_new_node (parser, PT_EXPR);
      if (constraint == NULL)
	{
	  goto outofmem;
	}

      oid_is_null_expr = NULL;

      name = node;
      CAST_POINTER_TO_NODE (name);
      assert (PT_IS_NAME_NODE (name));

      /* look for spec in spec list */
      spec_list = spec;
      while (spec_list)
	{
	  if (spec_list->info.spec.id == name->info.name.spec_id)
	    {
	      break;
	    }
	  spec_list = spec_list->next;
	}

      if (spec_list == NULL)
	{
	  assert (false);
	  goto outofmem;
	}

      /* create not null constraint */
      constraint->next = NULL;
      constraint->line_number = node->line_number;
      constraint->column_number = node->column_number;
      constraint->type_enum = PT_TYPE_LOGICAL;
      constraint->info.expr.op = PT_IS_NOT_NULL;
      constraint->info.expr.arg1 = node;

      if (mq_is_outer_join_spec (parser, spec_list))
	{
	  /* need rewrite */
	  /* verify not null constraint only if OID is not null */
	  /* create OID is NULL expression */
	  oid_is_null_expr = parser_new_node (parser, PT_EXPR);
	  if (!oid_is_null_expr)
	    {
	      goto outofmem;
	    }
	  oid_is_null_expr->type_enum = PT_TYPE_LOGICAL;
	  oid_is_null_expr->info.expr.op = PT_IS_NULL;
	  oid_is_null_expr->info.expr.arg1 =
	    pt_spec_to_oid_attr (parser, spec_list);
	  if (!oid_is_null_expr->info.expr.arg1)
	    {
	      goto outofmem;
	    }

	  /* create an OR expression, first argument OID is NULL, second
	   * argument the constraint. This way, constraint check will be
	   * skipped if OID is NULL
	   */
	  conj = parser_new_node (parser, PT_EXPR);
	  if (!conj)
	    {
	      goto outofmem;
	    }
	  conj->type_enum = PT_TYPE_LOGICAL;
	  conj->info.expr.op = PT_OR;
	  conj->info.expr.arg1 = oid_is_null_expr;
	  conj->info.expr.arg2 = constraint;
	}
      else
	{
	  conj = constraint;
	}
      /* add spec to table info */
      ti = pt_make_table_info (parser, spec_list);
      if (ti)
	{
	  ti->next = parser->symbols->table_info;
	  parser->symbols->table_info = ti;
	}

      conj->next = pt_pred;
      pt_pred = conj;
      node = next;		/* go to the next node */
    }

  parser->symbols->current_listfile = attr_list;
  parser->symbols->listfile_value_list = xasl->val_list;
  parser->symbols->listfile_attr_offset = attr_offset;

  pred = pt_to_pred_expr (parser, pt_pred);

  conj = pt_pred;
  while (conj)
    {
      conj->info.expr.arg1 = NULL;
      conj = conj->next;
    }
  if (pt_pred)
    {
      parser_free_tree (parser, pt_pred);
    }

  /* symbols are allocated with pt_alloc_packing_buf,
   * and freed at end of xasl generation. */
  parser->symbols = NULL;

  if (xasl->type == INSERT_PROC)
    {
      xasl->proc.insert.cons_pred = pred;
    }
  else if (xasl->type == UPDATE_PROC)
    {
      xasl->proc.update.cons_pred = pred;
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      return ER_GENERIC_ERROR;
    }

  return NO_ERROR;

outofmem:
  PT_ERRORm (parser, spec, MSGCAT_SET_PARSER_RUNTIME,
	     MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
  if (pt_pred)
    {
      parser_free_tree (parser, pt_pred);
    }

  return MSGCAT_RUNTIME_RESOURCES_EXHAUSTED;

}

/*
 * pt_to_insert_xasl () - Converts an insert parse tree to an XASL tree for
 *			  insert server execution.
 *
 * return	  : Xasl node.
 * session(in) : contains the SQL query that has been compiled
 * parser (in)	  : Parser context.
 * statement (in) : Parse tree node for insert statement.
 */
XASL_NODE *
pt_to_insert_xasl (DB_SESSION * session, PARSER_CONTEXT * parser,
		   PT_NODE * statement)
{
  XASL_NODE *xasl = NULL;
  INSERT_PROC_NODE *insert = NULL;
  PT_NODE *value_clauses = NULL, *query = NULL, *val_list = NULL;
  PT_NODE *attr = NULL, *attrs = NULL;
  PT_NODE *non_null_attrs = NULL, *default_expr_attrs = NULL;
  MOBJ class_;
  OID *class_oid = NULL;
  DB_OBJECT *class_obj = NULL;
  HFID *hfid = NULL;
  int no_vals, no_default_expr;
  int a, i, has_uniques;
  int error = NO_ERROR;

  assert (parser != NULL);
  assert (statement != NULL);

  has_uniques = statement->info.insert.has_uniques;
  non_null_attrs = statement->info.insert.non_null_attrs;

  value_clauses = statement->info.insert.value_clauses;
  attrs = pt_attrs_part (statement);

  class_obj =
    statement->info.insert.spec->info.spec.flat_entity_list->info.name.
    db_object;

  /* check iff is from pt_prepare_insert ()
   */
  if (session != NULL)
    {
      if (session->shardkey_required)
	{
	  /* check iff DML for global table joined with shard table
	   * with shard key lock
	   */
	  if (sm_is_shard_table (class_obj) == false)
	    {
	      PT_INTERNAL_ERROR (parser,
				 "Error setting shard info to global table");
	      return NULL;
	    }
	  assert (!sm_is_system_table (class_obj));
	}
    }

  class_ = locator_create_heap_if_needed (class_obj);
  if (class_ == NULL)
    {
      return NULL;
    }

  hfid = sm_heap (class_);
  if (hfid == NULL)
    {
      return NULL;
    }

  if (locator_flush_class (class_obj) != NO_ERROR)
    {
      return NULL;
    }

  error = check_for_default_expr (parser, attrs, &default_expr_attrs,
				  class_obj);
  if (error != NO_ERROR)
    {
      return NULL;
    }
  no_default_expr = pt_length_of_list (default_expr_attrs);

  if (value_clauses->info.node_list.list_type == PT_IS_SUBQUERY)
    {
      query = value_clauses->info.node_list.list;
      assert (PT_IS_QUERY (query));
      no_vals = pt_length_of_select_list (pt_get_select_list (parser, query),
					  EXCLUDE_HIDDEN_COLUMNS);
      /* also add columns referenced in assignments */
      if (PT_IS_SELECT (query)
	  && statement->info.insert.odku_assignments != NULL)
	{
	  PT_NODE *select_list = query->info.query.q.select.list;
	  PT_NODE *select_from = query->info.query.q.select.from;
	  PT_NODE *assigns = statement->info.insert.odku_assignments;
	  select_list =
	    pt_append_assignment_references (parser, assigns, select_from,
					     select_list);
	  if (select_list == NULL)
	    {
	      PT_INTERNAL_ERROR (parser, "Error appending odku references to "
				 "select list");
	      return NULL;
	    }
	}
    }
  else
    {
      val_list = value_clauses->info.node_list.list;
      no_vals = pt_length_of_list (val_list);
    }

  if (value_clauses->info.node_list.list_type == PT_IS_SUBQUERY)
    {
      xasl =
	pt_make_aptr_parent_node (parser, value_clauses->info.node_list.list,
				  INSERT_PROC);
    }
  else
    {
      /* INSERT VALUES */
      int n;
      TABLE_INFO *ti;

      xasl = regu_xasl_node_alloc (INSERT_PROC);
      if (xasl == NULL)
	{
	  return NULL;
	}

      pt_init_xasl_supp_info ();

      /* init parser->symbols */
      parser->symbols = pt_symbol_info_alloc ();
      ti = pt_make_table_info (parser, statement->info.insert.spec);
      if (ti == NULL)
	{
	  PT_ERRORm (parser, statement->info.insert.spec,
		     MSGCAT_SET_PARSER_RUNTIME,
		     MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
	  return NULL;
	}
      if (parser->symbols->table_info != NULL)
	{
	  ti->next = parser->symbols->table_info;
	}
      parser->symbols->table_info = ti;

      value_clauses =
	parser_walk_tree (parser, value_clauses, parser_generate_xasl_pre,
			  NULL, parser_generate_xasl_post, &xasl_Supp_info);

      if ((n = xasl_Supp_info.n_oid_list) > 0
	  && (xasl->class_oid_list = regu_oid_array_alloc (n))
	  && (xasl->tcard_list = regu_int_array_alloc (n)))
	{
	  xasl->n_oid_list = n;
	  (void) memcpy (xasl->class_oid_list, xasl_Supp_info.class_oid_list,
			 sizeof (OID) * n);
	  (void) memcpy (xasl->tcard_list, xasl_Supp_info.tcard_list,
			 sizeof (int) * n);
	}

      pt_init_xasl_supp_info ();

      insert = &xasl->proc.insert;

      /* generate xasl->val_list */
      xasl->val_list = pt_make_val_list (parser, attrs);
      if (xasl->val_list == NULL)
	{
	  return NULL;
	}

      parser->symbols->current_class = statement->info.insert.spec;
      parser->symbols->listfile_value_list = xasl->val_list;
      parser->symbols->current_listfile = pt_attrs_part (statement);
      parser->symbols->listfile_attr_offset = 0;
      parser->symbols->listfile_unbox = UNBOX_AS_VALUE;

      /* count the number of value lists in values clause */
      for (insert->no_val_lists = 0, val_list = value_clauses;
	   val_list != NULL;
	   insert->no_val_lists++, val_list = val_list->next)
	{
	  ;
	}

      /* alloc valptr_lists for each list of values */
      insert->valptr_lists =
	regu_outlistptr_array_alloc (insert->no_val_lists);
      if (insert->valptr_lists == NULL)
	{
	  return NULL;
	}

      for (i = 0, val_list = value_clauses; val_list != NULL;
	   i++, val_list = val_list->next)
	{
	  assert (i < insert->no_val_lists);
	  if (i >= insert->no_val_lists)
	    {
	      PT_INTERNAL_ERROR (parser, "Generated insert xasl is corrupted:"
				 " incorrect number of value lists");
	    }
	  insert->valptr_lists[i] =
	    pt_to_outlist (parser, val_list->info.node_list.list,
			   UNBOX_AS_VALUE);
	  if (insert->valptr_lists[i] == NULL)
	    {
	      return NULL;
	    }
	}
    }

  if (xasl)
    {
      /* check iff DML to catalog table, shard table
       */
      if (sm_is_system_table (class_obj))
	{
	  assert (!sm_is_shard_table (class_obj));
	  XASL_SET_FLAG (xasl, XASL_TO_CATALOG_TABLE);
	}
      if (sm_is_shard_table (class_obj))
	{
	  assert (!sm_is_system_table (class_obj));
	  XASL_SET_FLAG (xasl, XASL_TO_SHARD_TABLE);
	}

      insert = &xasl->proc.insert;
      insert->class_hfid = *hfid;
      class_oid = ws_identifier (class_obj);
      if (class_oid != NULL)
	{
	  insert->class_oid = *class_oid;
	}
      else
	{
	  error = ER_HEAP_UNKNOWN_OBJECT;
	}

      insert->has_uniques = has_uniques;
      insert->do_replace = (statement->info.insert.do_replace ? 1 : 0);
      if (statement->info.insert.hint & PT_HINT_FORCE_PAGE_ALLOCATION)
	{
	  insert->force_page_allocation = 1;
	}
      else
	{
	  insert->force_page_allocation = 0;
	}

      if (error >= 0 && (no_vals + no_default_expr > 0))
	{
	  insert->att_id = regu_int_array_alloc (no_vals + no_default_expr);
	  if (insert->att_id)
	    {
	      /* the identifiers of the attributes that have a default
	         expression are placed first
	       */
	      int save_au;
	      AU_DISABLE (save_au);
	      for (attr = default_expr_attrs, a = 0; error >= 0 &&
		   a < no_default_expr; attr = attr->next, ++a)
		{
		  if ((insert->att_id[a] =
		       sm_att_id (class_obj, attr->info.name.original)) < 0)
		    {
		      error = er_errid ();
		    }
		}

	      for (attr = attrs, a = no_default_expr;
		   error >= 0 && a < no_default_expr + no_vals;
		   attr = attr->next, ++a)
		{
		  if ((insert->att_id[a] =
		       sm_att_id (class_obj, attr->info.name.original)) < 0)
		    {
		      error = er_errid ();
		    }
		}
	      AU_ENABLE (save_au);
	      insert->vals = NULL;
	      insert->no_vals = no_vals + no_default_expr;
	      insert->no_default_expr = no_default_expr;
	    }
	  else
	    {
	      error = er_errid ();
	    }
	}
    }
  else
    {
      error = er_errid ();
    }

  if (xasl != NULL && error >= 0)
    {
      error = pt_to_constraint_pred (parser, xasl,
				     statement->info.insert.spec,
				     non_null_attrs, attrs, 0);
    }

  if (pt_has_error (parser))
    {
      pt_report_to_ersys (parser, PT_SEMANTIC);
      xasl = NULL;
    }

  /* fill in XASL cache related information */
  if (xasl)
    {
      OID *oid;

      /* OID of the user who is creating this XASL */
      oid = ws_identifier (db_get_user ());
      if (oid != NULL)
	{
	  COPY_OID (&xasl->creator_oid, oid);
	}
      else
	{
	  OID_SET_NULL (&xasl->creator_oid);
	}

      /* list of class OIDs used in this XASL */
      if (xasl->aptr_list != NULL)
	{
	  XASL_NODE *aptr = xasl->aptr_list;

	  /*
	   * in case of 'insert into foo select a from b'
	   * so there is no serial oid list from values list
	   */
	  assert (xasl->n_oid_list == 0);

	  /* reserve spec oid space by 1+ */
	  xasl->class_oid_list = regu_oid_array_alloc (1 + aptr->n_oid_list);

	  xasl->tcard_list = regu_int_array_alloc (1 + aptr->n_oid_list);
	  if (xasl->class_oid_list == NULL || xasl->tcard_list == NULL)
	    {
	      return NULL;
	    }

	  xasl->n_oid_list = 1 + aptr->n_oid_list;

	  /* copy aptr oids to xasl */
	  (void) memcpy (xasl->class_oid_list + 1,
			 aptr->class_oid_list,
			 sizeof (OID) * aptr->n_oid_list);
	  (void) memcpy (xasl->tcard_list + 1, aptr->tcard_list,
			 sizeof (int) * aptr->n_oid_list);

	  /* set spec oid */
	  xasl->class_oid_list[0] = insert->class_oid;
	  xasl->tcard_list[0] = -1;	/* init #pages */

	  xasl->dbval_cnt = aptr->dbval_cnt;
	}
      else
	{
	  /* reserve spec oid space by 1+ */
	  OID *o_list = regu_oid_array_alloc (1 + xasl->n_oid_list);
	  int *t_list = regu_int_array_alloc (1 + xasl->n_oid_list);

	  if (o_list == NULL || t_list == NULL)
	    {
	      return NULL;
	    }

	  /* copy previous serial oids to new space */
	  (void) memcpy (o_list + 1,
			 xasl->class_oid_list,
			 sizeof (OID) * xasl->n_oid_list);
	  (void) memcpy (t_list + 1, xasl->tcard_list,
			 sizeof (int) * xasl->n_oid_list);

	  xasl->class_oid_list = o_list;
	  xasl->tcard_list = t_list;

	  /* set spec oid */
	  xasl->n_oid_list += 1;
	  xasl->class_oid_list[0] = insert->class_oid;
	  xasl->tcard_list[0] = -1;	/* init #pages */
	}
    }

  if (xasl && statement->info.insert.odku_assignments)
    {
      int save_au;

      AU_DISABLE (save_au);
      xasl->proc.insert.odku = pt_to_odku_info (parser, statement, xasl);
      AU_ENABLE (save_au);
      if (xasl->proc.insert.odku == NULL)
	{
	  if (pt_has_error (parser))
	    {
	      pt_report_to_ersys (parser, PT_SEMANTIC);
	    }
	  return NULL;
	}
    }

  if (prm_get_bool_value (PRM_ID_XASL_DEBUG_DUMP))
    {
      if (xasl)
	{
	  qdump_print_xasl (xasl);
	}
      else
	{
	  printf ("<NULL XASL generation>\n");
	}
    }

  return xasl;
}

/*
 * pt_append_assignment_references () - append names referenced in right side
 *					of ON DUPLICATE KEY UPDATE to the
 *					SELECT list of an INSERT...SELECT
 *					statement
 * return : updated node or NULL
 * parser (in)	    : parser context
 * assignments (in) : assignments
 * from (in)	    : SELECT spec list
 * select_list (in/out) : SELECT list
 */
static PT_NODE *
pt_append_assignment_references (PARSER_CONTEXT * parser,
				 PT_NODE * assignments, PT_NODE * from,
				 PT_NODE * select_list)
{
  PT_NODE *spec;
  TABLE_INFO *table_info;
  PT_NODE *ref_nodes;
  PT_NODE *save_next;
  PT_NODE *save_ref = NULL;

  if (assignments == NULL)
    {
      return select_list;
    }

  parser->symbols = pt_symbol_info_alloc ();
  if (parser->symbols == NULL)
    {
      PT_ERRORm (parser, from, MSGCAT_SET_PARSER_RUNTIME,
		 MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
      return NULL;
    }

  for (spec = from; spec != NULL; spec = spec->next)
    {
      save_ref = spec->info.spec.referenced_attrs;
      spec->info.spec.referenced_attrs = NULL;
      table_info = pt_make_table_info (parser, spec);
      if (!table_info)
	{
	  spec->info.spec.referenced_attrs = save_ref;
	  return NULL;
	}

      parser->symbols->table_info = table_info;
      /* make sure we only get references from assignments, not from the spec
       * also: call mq_get_references_helper with false for the last argument
       */
      ref_nodes = mq_get_references_helper (parser, assignments, spec, false);
      if (pt_has_error (parser))
	{
	  spec->info.spec.referenced_attrs = save_ref;
	  return NULL;
	}
      while (ref_nodes)
	{
	  save_next = ref_nodes->next;
	  ref_nodes->next = NULL;
	  if (pt_find_name (parser, ref_nodes, select_list) == NULL)
	    {
	      parser_append_node (ref_nodes, select_list);
	    }
	  ref_nodes = save_next;
	}
      spec->info.spec.referenced_attrs = save_ref;
    }
  parser->symbols = NULL;
  return select_list;
}

/*
 * pt_to_odku_info () - build a ODKU_INFO for an
 *			INSERT...ON DUPLICATE KEY UPDATE statement
 * return : ODKU info or NULL
 * parser (in)	: parser context
 * insert (in)	: insert statement
 * xasl (in)	: INSERT XASL node
 */
static ODKU_INFO *
pt_to_odku_info (PARSER_CONTEXT * parser, PT_NODE * insert, XASL_NODE * xasl)
{
  PT_NODE *insert_spec = NULL;
  PT_NODE *select_specs = NULL;
  PT_NODE *select_list = NULL;
  PT_NODE *assignments = NULL;
  PT_NODE *node = NULL;
  PT_NODE *spec = NULL, *constraint = NULL, *save = NULL, *pt_pred = NULL;
  int insert_subquery;
  PT_ASSIGNMENTS_HELPER assignments_helper;
  DB_OBJECT *cls_obj = NULL;
  int i = 0;
  UNUSED_VAR int error = NO_ERROR;
  ODKU_INFO *odku = NULL;
  TABLE_INFO *ti = NULL;
  DB_ATTRIBUTE *attr = NULL;

  assert (insert->node_type == PT_INSERT);
  assert (insert->info.insert.odku_assignments != NULL);
  parser->symbols = pt_symbol_info_alloc ();

  if (parser->symbols == NULL)
    {
      PT_ERRORm (parser, spec, MSGCAT_SET_PARSER_RUNTIME,
		 MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
      error = MSGCAT_RUNTIME_RESOURCES_EXHAUSTED;
      goto exit_on_error;
    }

  odku = regu_odku_info_alloc ();
  if (odku == NULL)
    {
      goto exit_on_error;
    }

  insert_spec = insert->info.insert.spec;

  insert_subquery =
    PT_IS_SELECT (insert->info.insert.value_clauses->info.node_list.list);

  if (insert_subquery)
    {
      select_specs =
	insert->info.insert.value_clauses->info.node_list.list->info.query.q.
	select.from;
      select_list =
	insert->info.insert.value_clauses->info.node_list.list->info.query.q.
	select.list;
    }
  else
    {
      select_list = NULL;
      select_specs = NULL;
    }

  odku->no_assigns = 0;
  assignments = insert->info.insert.odku_assignments;

  /* init update attribute ids */
  pt_init_assignments_helper (parser, &assignments_helper, assignments);
  while (pt_get_next_assignment (&assignments_helper) != NULL)
    {
      odku->no_assigns++;
    }

  odku->attr_ids = regu_int_array_alloc (odku->no_assigns);
  if (odku->attr_ids == NULL)
    {
      goto exit_on_error;
    }

  odku->assignments = regu_update_assignment_array_alloc (odku->no_assigns);
  if (odku->assignments == NULL)
    {
      goto exit_on_error;
    }

  odku->attr_info = regu_cache_attrinfo_alloc ();
  if (!odku->attr_info)
    {
      goto exit_on_error;
    }

  /* build table info */
  ti = pt_make_table_info (parser, insert_spec);
  if (ti == NULL)
    {
      PT_ERRORm (parser, spec, MSGCAT_SET_PARSER_RUNTIME,
		 MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
      error = MSGCAT_RUNTIME_RESOURCES_EXHAUSTED;
      goto exit_on_error;
    }

  ti->next = parser->symbols->table_info;
  parser->symbols->table_info = ti;

  for (spec = select_specs; spec != NULL; spec = spec->next)
    {
      ti = pt_make_table_info (parser, spec);
      if (ti == NULL)
	{
	  PT_ERRORm (parser, spec, MSGCAT_SET_PARSER_RUNTIME,
		     MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
	  error = MSGCAT_RUNTIME_RESOURCES_EXHAUSTED;
	  goto exit_on_error;
	}

      ti->next = parser->symbols->table_info;
      parser->symbols->table_info = ti;
    }

  /* init symbols */
  parser->symbols->current_class = insert_spec->info.spec.entity_name;
  parser->symbols->cache_attrinfo = odku->attr_info;
  parser->symbols->current_listfile = select_list;
  parser->symbols->listfile_value_list = xasl->val_list;
  parser->symbols->listfile_attr_offset = 0;

  cls_obj = insert_spec->info.spec.entity_name->info.name.db_object;

  pt_init_assignments_helper (parser, &assignments_helper, assignments);
  i = 0;
  while (pt_get_next_assignment (&assignments_helper))
    {
      attr = db_get_attribute (cls_obj,
			       assignments_helper.lhs->info.name.original);
      if (attr == NULL)
	{
	  error = er_errid ();
	  goto exit_on_error;
	}

      odku->attr_ids[i] = attr->id;
      odku->assignments[i].att_idx = i;

      if (pt_is_query (assignments_helper.rhs))
	{
	  XASL_NODE *rhs_xasl = NULL;

	  rhs_xasl = parser_generate_xasl (parser, assignments_helper.rhs);
	  if (rhs_xasl == NULL)
	    {
	      error = er_errid ();
	      goto exit_on_error;
	    }
	}
      odku->assignments[i].regu_var =
	pt_to_regu_variable (parser, assignments_helper.rhs, UNBOX_AS_VALUE);

      i++;
    }

  if (insert->info.insert.odku_non_null_attrs)
    {
      /* build constraint pred */
      pt_init_assignments_helper (parser, &assignments_helper, assignments);
      node = insert->info.insert.odku_non_null_attrs;
      while (node)
	{
	  save = node->next;
	  CAST_POINTER_TO_NODE (node);
	  do
	    {
	      pt_get_next_assignment (&assignments_helper);
	    }
	  while (assignments_helper.lhs != NULL
		 && !pt_name_equal (parser, assignments_helper.lhs, node));

	  if (assignments_helper.lhs == NULL
	      || assignments_helper.rhs == NULL)
	    {
	      /* I don't think this should happen */
	      assert (false);
	      break;
	    }

	  constraint = parser_new_node (parser, PT_EXPR);
	  if (constraint == NULL)
	    {
	      PT_ERRORm (parser, spec, MSGCAT_SET_PARSER_RUNTIME,
			 MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
	      error = MSGCAT_RUNTIME_RESOURCES_EXHAUSTED;
	      goto exit_on_error;
	    }

	  constraint->next = pt_pred;
	  constraint->line_number = node->line_number;
	  constraint->column_number = node->column_number;
	  constraint->info.expr.op = PT_IS_NOT_NULL;
	  constraint->info.expr.arg1 = assignments_helper.rhs;
	  pt_pred = constraint;

	  node = save;
	}

      odku->cons_pred = pt_to_pred_expr (parser, pt_pred);
      if (odku->cons_pred == NULL)
	{
	  assert (false);
	  goto exit_on_error;
	}
    }

  if (pt_pred != NULL)
    {
      parser_free_tree (parser, pt_pred);
    }
  return odku;

exit_on_error:
  if (er_errid () == NO_ERROR && !pt_has_error (parser))
    {
      PT_INTERNAL_ERROR (parser, "ODKU Info generation failed");
      error = ER_FAILED;
    }
  if (pt_pred != NULL)
    {
      parser_free_tree (parser, pt_pred);
    }
  return NULL;
}

/*
 * pt_copy_upddel_hints_to_select () - copy hints from delete/update statement
 *				       to select statement.
 *   return: NO_ERROR or error code.
 *   parser(in):
 *   node(in): delete/update statement that provides the hints to be
 *	       copied to the select statement.
 *   select_stmt(in): select statement that will receive hints.
 *
 * Note :
 * The hints that are copied from delete/update statement to SELECT statement
 * are: ORDERED, USE_DESC_IDX, NO_COVERING_INDEX, NO_DESC_IDX, USE_NL, USE_IDX,
 *	NO_MULTI_RANGE_OPT, RECOMPILE.
 */
int
pt_copy_upddel_hints_to_select (PARSER_CONTEXT * parser, PT_NODE * node,
				PT_NODE * select_stmt)
{
  int err = NO_ERROR;
  int hint_flags = PT_HINT_ORDERED | PT_HINT_USE_IDX_DESC
    | PT_HINT_NO_COVERING_IDX | PT_HINT_NO_IDX_DESC | PT_HINT_USE_NL
    | PT_HINT_USE_IDX | PT_HINT_NO_MULTI_RANGE_OPT
    | PT_HINT_RECOMPILE | PT_HINT_NO_SORT_LIMIT;
  PT_NODE *arg = NULL;

  switch (node->node_type)
    {
    case PT_DELETE:
      hint_flags &= node->info.delete_.hint;
      break;
    case PT_UPDATE:
      hint_flags &= node->info.update.hint;
      break;
    default:
      return NO_ERROR;
    }
  select_stmt->info.query.q.select.hint |= hint_flags;
  select_stmt->recompile = node->recompile;

#if 0				/* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
  if (hint_flags & PT_HINT_ORDERED)
    {
      switch (node->node_type)
	{
	case PT_DELETE:
	  arg = node->info.delete_.ordered_hint;
	  break;
	case PT_UPDATE:
	  arg = node->info.update.ordered_hint;
	  break;
	default:
	  break;
	}
      if (arg != NULL)
	{
	  arg = parser_copy_tree_list (parser, arg);
	  if (arg == NULL)
	    {
	      goto exit_on_error;
	    }
	}
      select_stmt->info.query.q.select.ordered_hint = arg;
    }
#endif

  if (hint_flags & PT_HINT_USE_NL)
    {
      switch (node->node_type)
	{
	case PT_DELETE:
	  arg = node->info.delete_.use_nl_hint;
	  break;
	case PT_UPDATE:
	  arg = node->info.update.use_nl_hint;
	  break;
	default:
	  break;
	}
      if (arg != NULL)
	{
	  arg = parser_copy_tree_list (parser, arg);
	  if (arg == NULL)
	    {
	      goto exit_on_error;
	    }
	}
      select_stmt->info.query.q.select.use_nl_hint = arg;
    }

  if (hint_flags & PT_HINT_USE_IDX)
    {
      switch (node->node_type)
	{
	case PT_DELETE:
	  arg = node->info.delete_.use_idx_hint;
	  break;
	case PT_UPDATE:
	  arg = node->info.update.use_idx_hint;
	  break;
	default:
	  break;
	}
      if (arg != NULL)
	{
	  arg = parser_copy_tree_list (parser, arg);
	  if (arg == NULL)
	    {
	      goto exit_on_error;
	    }
	}
      select_stmt->info.query.q.select.use_idx_hint = arg;
    }

  return NO_ERROR;

exit_on_error:
  if (pt_has_error (parser))
    {
      err = er_errid ();
    }
  else
    {
      err = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, err, 1, "");
    }

  return err;
}

/*
 * pt_to_upd_del_query () - Creates a query based on the given select list,
 * 	from list, and where clause
 *   return: PT_NODE *, query statement or NULL if error
 *   parser(in):
 *   select_list(in):
 *   from(in):
 *   where(in):
 *   using_index(in):
 *   order_by(in):
 *   orderby_for(in):
 *   for_update(in): true if query is used in update operation
 *
 * Note :
 * Prepends the class oid and the instance oid onto the select list for use
 * during the update or delete operation.
 * If the operation is a server side update, the prepended class oid is
 * put in the list file otherwise the class oid is a hidden column and
 * not put in the list file
 */
PT_NODE *
pt_to_upd_del_query (PARSER_CONTEXT * parser, PT_NODE * select_names,
		     PT_NODE * select_list,
		     PT_NODE * from,
		     PT_NODE * where, PT_NODE * using_index,
		     PT_NODE * order_by, PT_NODE * orderby_for,
		     PT_SELECT_PURPOSE purpose)
{
  PT_NODE *statement = NULL, *from_temp = NULL, *node = NULL;
  PT_NODE *save_next = NULL, *spec = NULL;

  assert (parser != NULL);

  statement = parser_new_node (parser, PT_SELECT);
  if (statement == NULL)
    {
      return NULL;
    }

  statement->info.query.q.select.list =
    parser_copy_tree_list (parser, select_list);

  statement->info.query.q.select.from = parser_copy_tree_list (parser, from);
  statement->info.query.q.select.using_index =
    parser_copy_tree_list (parser, using_index);

  statement->info.query.q.select.where =
    parser_copy_tree_list (parser, where);

  if (purpose == PT_FOR_UPDATE
      && statement->info.query.q.select.from->next != NULL)
    {
      /* this is a multi-table update statement */
      for (spec = statement->info.query.q.select.from; spec;
	   spec = spec->next)
	{
	  PT_NODE *name = NULL, *val = NULL, *last_val = NULL;

	  if ((spec->info.spec.flag & PT_SPEC_FLAG_UPDATE) == 0)
	    {
	      /* class will not be updated, nothing to do */
	      continue;
	    }

	  if (!mq_is_outer_join_spec (parser, spec))
	    {
	      /* spec is not outer joined in list; no need to rewrite */
	      continue;
	    }

	  /*
	   * Class will be updated and is outer joined.
	   *
	   * We must rewrite all expressions that will be assigned to
	   * attributes of this class as
	   *
	   *     IF (class_oid IS NULL, NULL, expr)
	   *
	   * so that expr will evaluate and/or fail only if an assignment
	   * will be done.
	   */

	  name = select_names;
	  val = statement->info.query.q.select.list;
	  for (; name && val; name = name->next, val = val->next)
	    {
	      PT_NODE *if_expr = NULL, *isnull_expr = NULL;
	      PT_NODE *bool_expr = NULL;
	      PT_NODE *nv = NULL, *class_oid = NULL;
	      DB_TYPE dom_type;
	      DB_VALUE nv_value, *nv_valp;
	      TP_DOMAIN *dom;

	      assert (!PT_IS_NULL_NODE (name));

	      if (name->info.name.spec_id != spec->info.spec.id)
		{
		  /* attribute does not belong to the class */
		  last_val = val;
		  continue;
		}

	      /* build class oid node */
	      class_oid = pt_spec_to_oid_attr (parser, spec);
	      if (class_oid == NULL)
		{
		  assert (false);
		  PT_INTERNAL_ERROR (parser, "error building oid attr");
		  parser_free_tree (parser, statement);
		  return NULL;
		}

	      /* allocate new parser nodes */
	      isnull_expr = parser_new_node (parser, PT_EXPR);
	      if (isnull_expr == NULL)
		{
		  assert (false);
		  PT_INTERNAL_ERROR (parser, "out of memory");
		  parser_free_tree (parser, statement);
		  return NULL;
		}

	      /* (class_oid IS NULL) logical expression */
	      isnull_expr->info.expr.op = PT_ISNULL;
	      isnull_expr->info.expr.arg1 = class_oid;

	      bool_expr =
		pt_convert_to_logical_expr (parser, isnull_expr, 1, 1);
	      /* NULL value node */
	      dom_type = pt_type_enum_to_db (name->type_enum);
	      dom = tp_domain_resolve_default (dom_type);
	      if (dom == NULL)
		{
		  assert (false);
		  PT_INTERNAL_ERROR (parser, "error building domain");
		  parser_free_tree (parser, statement);
		  return NULL;
		}

	      if (db_value_domain_default (&nv_value,
					   dom_type,
					   dom->precision,
					   dom->scale,
					   dom->collation_id) != NO_ERROR)
		{
		  assert (false);
		  PT_INTERNAL_ERROR (parser, "error building default val");
		  parser_free_tree (parser, statement);
		  return NULL;
		}
	      nv = pt_dbval_to_value (parser, &nv_value);

	      assert (!PT_IS_NULL_NODE (nv));
	      assert (nv->type_enum != PT_TYPE_NONE);

	      nv_valp = pt_value_to_db (parser, nv);
	      if (nv_valp == NULL)
		{
		  assert (false);
		  PT_INTERNAL_ERROR (parser, "error building default val");
		  parser_free_tree (parser, statement);
		  return NULL;
		}

	      /* set as NULL value */
	      (void) pr_clear_value (nv_valp);

	      assert (!PT_IS_NULL_NODE (nv));
	      assert (nv->type_enum != PT_TYPE_NONE);

	      /* IF expr node */
	      if_expr = parser_new_node (parser, PT_EXPR);
	      if (bool_expr == NULL || nv == NULL || if_expr == NULL)
		{
		  /* free allocated nodes */
		  if (bool_expr)
		    {
		      parser_free_tree (parser, bool_expr);
		    }

		  if (nv)
		    {
		      parser_free_node (parser, nv);
		    }

		  if (if_expr)
		    {
		      parser_free_node (parser, if_expr);
		    }

		  assert (false);
		  PT_INTERNAL_ERROR (parser, "out of memory");
		  parser_free_tree (parser, statement);
		  return NULL;
		}

	      /* IF (ISNULL(class_oid)<>0, NULL, val) expression */
	      if_expr->info.expr.op = PT_IF;
	      if_expr->info.expr.arg1 = bool_expr;
	      if_expr->info.expr.arg2 = nv;
	      if_expr->info.expr.arg3 = val;
	      if_expr->type_enum = name->type_enum;
	      if_expr->data_type =
		parser_copy_tree_list (parser, name->data_type);

	      /* rebuild links */
	      PT_NODE_MOVE_NUMBER_OUTERLINK (if_expr, val);
	      val = if_expr;

	      if (last_val != NULL)
		{
		  last_val->next = val;
		}
	      else
		{
		  statement->info.query.q.select.list = val;
		}


	      /* remember this node as previous assignment node */
	      last_val = val;
	    }
	}
    }

  /* add the class and instance OIDs to the select list */
  from_temp = statement->info.query.q.select.from;
  statement->info.query.upd_del_class_cnt = 0;
  for (node = from; node != NULL; node = node->next)
    {
      if (node->node_type != PT_SPEC)
	{
	  assert (false);
	  PT_INTERNAL_ERROR (parser, "Invalid node type");
	  parser_free_tree (parser, statement);
	  return NULL;
	}

      if (node->info.spec.flag & (PT_SPEC_FLAG_UPDATE | PT_SPEC_FLAG_DELETE))
	{
	  save_next = node->next;
	  node->next = NULL;
	  statement->info.query.q.select.from = node;
	  statement = pt_add_row_oid_name (parser, statement);
	  assert (statement != NULL);
	  node->next = save_next;

	  statement->info.query.upd_del_class_cnt++;
	}
    }
  statement->info.query.q.select.from = from_temp;

  if (statement->info.query.upd_del_class_cnt != 1)
    {
      assert (false);		/* is impossible */
      parser_free_tree (parser, statement);
      return NULL;
    }

  /* don't allow orderby_for without order_by */
  assert (!((orderby_for != NULL) && (order_by == NULL)));

  statement->info.query.order_by = parser_copy_tree_list (parser, order_by);

  if (statement->info.query.order_by != NULL)
    {
      /* translate col names into col numbers */
      if (pt_check_order_by (parser, statement) != NO_ERROR)
	{
	  /* leave the error code set by check_order_by, will be
	     handled by the calling function */
	  parser_free_tree (parser, statement);
	  return NULL;
	}
    }

  statement->info.query.orderby_for =
    parser_copy_tree_list (parser, orderby_for);

  /* is query for update or delete */
  PT_SELECT_INFO_SET_FLAG (statement, PT_SELECT_INFO_UPD_DEL);

  return statement;
}


/*
 * pt_to_delete_xasl () - Converts an delete parse tree to
 *                        an XASL graph for an delete
 *   return:
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in): delete parse tree
 */
XASL_NODE *
pt_to_delete_xasl (DB_SESSION * session, PT_NODE * statement)
{
  PARSER_CONTEXT *parser;
  XASL_NODE *xasl = NULL;
  DELETE_PROC_NODE *delete_ = NULL;
  UPDDEL_CLASS_INFO *class_info = NULL;
  PT_NODE *aptr_statement = NULL;
  PT_NODE *from;
  PT_NODE *where;
  PT_NODE *using_index;
  PT_NODE *cl_name_node;
  HFID *hfid;
  OID *class_oid;
  DB_OBJECT *class_obj;
  int no_classes = 0, no_subclasses = 0;
  int error = NO_ERROR;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  assert (parser != NULL);
  assert (statement != NULL);

  from = statement->info.delete_.spec;
  where = statement->info.delete_.search_cond;
  using_index = statement->info.delete_.using_index;

  if (from && from->node_type == PT_SPEC && from->info.spec.range_var)
    {
      if (((aptr_statement = pt_to_upd_del_query (parser, NULL, NULL,
						  from,
						  where, using_index,
						  NULL, NULL, PT_FOR_DELETE))
	   == NULL)
	  || pt_copy_upddel_hints_to_select (parser, statement,
					     aptr_statement) != NO_ERROR
	  || ((aptr_statement = mq_translate (parser, aptr_statement)) ==
	      NULL)
	  ||
	  ((xasl =
	    pt_make_aptr_parent_node (parser, aptr_statement,
				      DELETE_PROC)) == NULL))
	{
	  error = ER_FAILED;
	  goto error_return;
	}

    }

  if (aptr_statement)
    {
      parser_free_tree (parser, aptr_statement);
      aptr_statement = NULL;
    }

  if (xasl != NULL)
    {
      PT_NODE *node;

      delete_ = &xasl->proc.delete_;

      node = statement->info.delete_.spec;
      no_classes = 0;
      while (node != NULL)
	{
	  if (node->info.spec.flag & PT_SPEC_FLAG_DELETE)
	    {
	      no_classes++;
	    }
	  node = node->next;
	}

      if (no_classes != 1)
	{
	  assert (false);
	  error = ER_FAILED;
	  goto error_return;
	}

      delete_->class_info = regu_upddel_class_info_alloc ();
      if (delete_->class_info == NULL)
	{
	  error = ER_FAILED;
	  goto error_return;
	}

      /* we iterate through updatable classes from left to right and fill
       * the structures from right to left because we must match the order
       * of OID's in the generated SELECT statement */
      for (node = statement->info.delete_.spec;
	   node != NULL && error == NO_ERROR; node = node->next)
	{
	  if (!(node->info.spec.flag & PT_SPEC_FLAG_DELETE))
	    {
	      /* skip classes from which we're not deleting */
	      continue;
	    }

	  class_info = delete_->class_info;

	  /* setup members not needed for DELETE */
	  class_info->att_id = NULL;
	  class_info->no_attrs = 0;
	  /* assume it always has uniques */
	  class_info->has_uniques = 1;

	  cl_name_node = node->info.spec.flat_entity_list;
	  assert (cl_name_node != NULL);
	  assert (cl_name_node->next == NULL);
	  class_obj = cl_name_node->info.name.db_object;

	  no_subclasses = 0;
	  while (cl_name_node)
	    {
	      no_subclasses++;
	      cl_name_node = cl_name_node->next;
	    }

	  if (no_subclasses != 1)
	    {
	      assert (false);
	      error = ER_FAILED;
	      goto error_return;
	    }

	  OID_SET_NULL (&(class_info->class_oid));
	  HFID_SET_NULL (&(class_info->class_hfid));

	  cl_name_node = node->info.spec.flat_entity_list;
	  assert (cl_name_node != NULL);
	  assert (cl_name_node->next == NULL);

	  class_obj = cl_name_node->info.name.db_object;

	  if (session->shardkey_required)
	    {
	      /* check iff DML for global table joined with shard table
	       * with shard key lock
	       */
	      if (sm_is_shard_table (class_obj) == false)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_SHARD_CANT_UPDATE_DELETE_GLOBAL_TABLE, 0);
		  error = ER_SHARD_CANT_UPDATE_DELETE_GLOBAL_TABLE;
		  goto error_return;
		}
	      assert (!sm_is_system_table (class_obj));
	    }

	  /* check iff DML to catalog table, shard table
	   */
	  if (sm_is_system_table (class_obj))
	    {
	      assert (!sm_is_shard_table (class_obj));
	      XASL_SET_FLAG (xasl, XASL_TO_CATALOG_TABLE);
	    }
	  if (sm_is_shard_table (class_obj))
	    {
	      assert (!sm_is_system_table (class_obj));
	      XASL_SET_FLAG (xasl, XASL_TO_SHARD_TABLE);
	    }

	  /* get class oid */
	  class_oid = ws_identifier (class_obj);
	  if (class_oid == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_UNKNOWN_OBJECT, 3, 0, 0, 0);
	      error = ER_HEAP_UNKNOWN_OBJECT;
	      goto error_return;
	    }

	  /* get hfid */
	  hfid = sm_get_heap (class_obj);
	  if (hfid == NULL)
	    {
	      error = ER_FAILED;
	      goto error_return;
	    }

	  COPY_OID (&(class_info->class_oid), class_oid);
	  HFID_COPY (&(class_info->class_hfid), hfid);

	}

      assert (!OID_ISNULL (&(xasl->proc.delete_.class_info->class_oid)));
      assert (!HFID_IS_NULL (&(xasl->proc.delete_.class_info->class_hfid)));
    }

  if (pt_has_error (parser) || error < 0)
    {
      pt_report_to_ersys (parser, PT_SEMANTIC);
      xasl = NULL;
    }

  /* fill in XASL cache related information */
  if (xasl)
    {
      OID *oid;

      /* OID of the user who is creating this XASL */
      if ((oid = ws_identifier (db_get_user ())) != NULL)
	{
	  COPY_OID (&xasl->creator_oid, oid);
	}
      else
	{
	  OID_SET_NULL (&xasl->creator_oid);
	}


      /* list of class OIDs used in this XASL */
      if (xasl->aptr_list != NULL)
	{
	  xasl->n_oid_list = xasl->aptr_list->n_oid_list;
	  xasl->aptr_list->n_oid_list = 0;
	  xasl->class_oid_list = xasl->aptr_list->class_oid_list;
	  xasl->aptr_list->class_oid_list = NULL;
	  xasl->tcard_list = xasl->aptr_list->tcard_list;
	  xasl->aptr_list->tcard_list = NULL;
	  xasl->dbval_cnt = xasl->aptr_list->dbval_cnt;
	}

      if (statement->info.delete_.limit)
	{
	  PT_NODE *limit = statement->info.delete_.limit;

	  if (limit->next)
	    {
	      limit = limit->next;
	    }
	  xasl->limit_row_count =
	    pt_to_regu_variable (parser, limit, UNBOX_AS_VALUE);
	}
    }

  if (prm_get_bool_value (PRM_ID_XASL_DEBUG_DUMP))
    {
      if (xasl)
	{
	  qdump_print_xasl (xasl);
	}
      else
	{
	  printf ("<NULL XASL generation>\n");
	}
    }

  return xasl;

error_return:

  assert (error != NO_ERROR);

  if (aptr_statement != NULL)
    {
      parser_free_tree (parser, aptr_statement);
    }

  if (pt_has_error (parser))
    {
      pt_report_to_ersys (parser, PT_SEMANTIC);
    }

  return NULL;
}


/*
 * pt_to_update_xasl () - Converts an update parse tree to
 * 			  an XASL graph for an update
 *   return:
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in): update parse tree
 *   non_null_attrs(in):
 */
XASL_NODE *
pt_to_update_xasl (DB_SESSION * session, PT_NODE * statement,
		   PT_NODE ** non_null_attrs)
{
  PARSER_CONTEXT *parser;
  XASL_NODE *xasl = NULL;
  UPDATE_PROC_NODE *update = NULL;
  UPDDEL_CLASS_INFO *upd_cls = NULL;
  PT_NODE *assigns = statement->info.update.assignment;
  PT_NODE *aptr_statement = NULL;
  PT_NODE *p = NULL;
  PT_NODE *cl_name_node = NULL;
  int no_classes = 0, no_subclasses = 0;
  PT_NODE *from = NULL;
  PT_NODE *where = NULL;
  PT_NODE *using_index = NULL;
  int no_vals = 0;
  int error = NO_ERROR;
  int a = 0, assign_idx = 0;
  PT_NODE *att_name_node = NULL;
  OID *class_oid = NULL;
  DB_OBJECT *class_obj = NULL;
  HFID *hfid = NULL;
  PT_NODE *order_by = NULL;
  PT_NODE *orderby_for = NULL;
  PT_ASSIGNMENTS_HELPER assign_helper;
  PT_NODE **links = NULL;
  UPDATE_ASSIGNMENT *assign = NULL;
  PT_NODE *select_names = NULL;
  PT_NODE *select_values = NULL;
  OID *oid = NULL;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  assert (parser != NULL);
  assert (statement != NULL);

  from = statement->info.update.spec;
  where = statement->info.update.search_cond;
  using_index = statement->info.update.using_index;
  order_by = statement->info.update.order_by;
  orderby_for = statement->info.update.orderby_for;

  /* flush all classes */
  p = from;
  while (p != NULL)
    {
      cl_name_node = p->info.spec.flat_entity_list;

      while (cl_name_node != NULL)
	{
	  error = locator_flush_class (cl_name_node->info.name.db_object);
	  if (error != NO_ERROR)
	    {
	      goto cleanup;
	    }
	  cl_name_node = cl_name_node->next;
	}

      p = p->next;
    }

  if (from == NULL || from->node_type != PT_SPEC
      || from->info.spec.range_var == NULL)
    {
      PT_INTERNAL_ERROR (parser, "update");
      error = ER_FAILED;
      goto cleanup;
    }

  /* get assignments lists for select statement generation */
  error =
    pt_get_assignment_lists (parser, &select_names, &select_values, &no_vals,
			     statement->info.update.assignment, &links);
  if (error != NO_ERROR)
    {
      PT_INTERNAL_ERROR (parser, "update");
      goto cleanup;
    }

  aptr_statement =
    pt_to_upd_del_query (parser, select_names, select_values, from,
			 where, using_index, order_by,
			 orderby_for, PT_FOR_UPDATE);
  /* restore assignment list here because we need to iterate through
   * assignments later*/
  pt_restore_assignment_links (statement->info.update.assignment, links, -1);

  if (aptr_statement == NULL)
    {
      error = er_errid ();
      if (error == NO_ERROR)
	{
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
	}
      goto cleanup;
    }

  error = pt_copy_upddel_hints_to_select (parser, statement, aptr_statement);
  if (error != NO_ERROR)
    {
      goto cleanup;
    }

  aptr_statement = mq_translate (parser, aptr_statement);
  if (aptr_statement == NULL)
    {
      error = er_errid ();
      if (error == NO_ERROR)
	{
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
	}
      goto cleanup;
    }

  xasl = pt_make_aptr_parent_node (parser, aptr_statement, UPDATE_PROC);
  if (xasl == NULL || xasl->aptr_list == NULL)
    {
      error = er_errid ();
      if (error == NO_ERROR)
	{
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
	}
      goto cleanup;
    }

  /* flush all classes and count classes for update */
  no_classes = 0;
  p = from;
  while (p != NULL)
    {
      if (p->info.spec.flag & PT_SPEC_FLAG_UPDATE)
	{
	  no_classes++;
	}

      cl_name_node = p->info.spec.flat_entity_list;
      while (cl_name_node != NULL)
	{
	  error = locator_flush_class (cl_name_node->info.name.db_object);
	  if (error != NO_ERROR)
	    {
	      goto cleanup;
	    }
	  cl_name_node = cl_name_node->next;
	}
      p = p->next;
    }

  update = &xasl->proc.update;

  if (no_classes != 1)
    {
      assert (false);
      error = ER_FAILED;
      goto cleanup;
    }

  update->class_info = regu_upddel_class_info_alloc ();
  if (update->class_info == NULL)
    {
      error = er_errid ();
      goto cleanup;
    }

  update->no_assigns = no_vals;

  update->assigns = regu_update_assignment_array_alloc (update->no_assigns);
  if (update->assigns == NULL)
    {
      error = er_errid ();
      goto cleanup;
    }

  /* we iterate through updatable classes from left to right and fill
   * the structures from right to left because we must match the order
   * of OID's in the generated SELECT statement */
  for (p = from; p != NULL && error == NO_ERROR; p = p->next)
    {
      /* ignore, this class will not be updated */
      if (!(p->info.spec.flag & PT_SPEC_FLAG_UPDATE))
	{
	  continue;
	}

      upd_cls = update->class_info;

      /* count subclasses of current class */
      no_subclasses = 0;
      cl_name_node = p->info.spec.flat_entity_list;
      assert (cl_name_node != NULL);
      assert (cl_name_node->next == NULL);
      while (cl_name_node)
	{
	  no_subclasses++;
	  cl_name_node = cl_name_node->next;
	}

#if 1				/* TODO - */
      if (no_subclasses != 1)
	{
	  assert (false);
	  error = ER_FAILED;
	  goto cleanup;
	}
#endif

      /* count class assignments */
      a = 0;
      pt_init_assignments_helper (parser, &assign_helper, assigns);
      while (pt_get_next_assignment (&assign_helper) != NULL)
	{
	  if (assign_helper.lhs->info.name.spec_id == p->info.spec.id)
	    {
	      a++;
	    }
	}
      upd_cls->no_attrs = a;

      /* allocate array for subclasses attributes ids */
      OID_SET_NULL (&(upd_cls->class_oid));

      HFID_SET_NULL (&(upd_cls->class_hfid));

      upd_cls->att_id = regu_int_array_alloc (upd_cls->no_attrs);
      if (upd_cls->att_id == NULL)
	{
	  error = er_errid ();
	  goto cleanup;
	}

      cl_name_node = p->info.spec.flat_entity_list;
      class_obj = cl_name_node->info.name.db_object;

      upd_cls->has_uniques = (p->info.spec.flag & PT_SPEC_FLAG_HAS_UNIQUE);

      cl_name_node = p->info.spec.flat_entity_list;
      assert (cl_name_node != NULL);
      assert (cl_name_node->next == NULL);

      class_obj = cl_name_node->info.name.db_object;

      if (session->shardkey_required)
	{
	  /* check iff DML for global table joined with shard table
	   * with shard key lock
	   */
	  if (sm_is_shard_table (class_obj) == false)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_SHARD_CANT_UPDATE_DELETE_GLOBAL_TABLE, 0);
	      error = ER_SHARD_CANT_UPDATE_DELETE_GLOBAL_TABLE;
	      goto cleanup;
	    }
	  assert (!sm_is_system_table (class_obj));
	}

      /* check iff DML to catalog table, shard table
       */
      if (sm_is_system_table (class_obj))
	{
	  assert (!sm_is_shard_table (class_obj));
	  XASL_SET_FLAG (xasl, XASL_TO_CATALOG_TABLE);
	}
      if (sm_is_shard_table (class_obj))
	{
	  assert (!sm_is_system_table (class_obj));
	  XASL_SET_FLAG (xasl, XASL_TO_SHARD_TABLE);
	}

      /* get class oid */
      class_oid = ws_identifier (class_obj);
      if (class_oid == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HEAP_UNKNOWN_OBJECT, 3, 0, 0, 0);
	  error = ER_HEAP_UNKNOWN_OBJECT;
	  goto cleanup;
	}

      /* get hfid */
      hfid = sm_get_heap (class_obj);
      if (hfid == NULL)
	{
	  error = er_errid ();
	  goto cleanup;
	}

      upd_cls->class_oid = *class_oid;
      upd_cls->class_hfid = *hfid;

      /* Calculate attribute ids and link each assignment
       * to classes and attributes */
      pt_init_assignments_helper (parser, &assign_helper, assigns);
      assign_idx = a = 0;
      while ((att_name_node = pt_get_next_assignment (&assign_helper))
	     != NULL)
	{
	  if (att_name_node->info.name.spec_id
	      == cl_name_node->info.name.spec_id)
	    {
	      assign = &update->assigns[assign_idx];
	      assign->att_idx = a;
	      upd_cls->att_id[a] =
		sm_att_id (class_obj, att_name_node->info.name.original);

	      if (upd_cls->att_id[a] < 0)
		{
		  error = er_errid ();
		  goto cleanup;
		}
	      /* count attributes for current class */
	      a++;
	    }
	  /* count assignments */
	  assign_idx++;
	}
    }

  assert (!OID_ISNULL (&(xasl->proc.update.class_info->class_oid)));
  assert (!HFID_IS_NULL (&(xasl->proc.update.class_info->class_hfid)));

  /* store number of ORDER BY keys in XASL tree */
  update->no_orderby_keys =
    pt_length_of_list (aptr_statement->info.query.q.select.list) -
    pt_length_of_select_list (aptr_statement->info.query.q.select.list,
			      EXCLUDE_HIDDEN_COLUMNS);
  assert (update->no_orderby_keys >= 0);

  /* generate xasl for non-null constraints predicates */
  error =
    pt_get_assignment_lists (parser, &select_names, &select_values, &no_vals,
			     statement->info.update.assignment, &links);
  if (error != NO_ERROR)
    {
      goto cleanup;
    }

  /* aptr_statement could be derived */
  if (aptr_statement->info.query.upd_del_class_cnt > 1)
    {
      assert (false);		/* is impossible */
      error = ER_FAILED;
      pt_restore_assignment_links (statement->info.update.assignment, links,
				   -1);
      goto cleanup;
    }

  /* need to jump upd_del_class_cnt {OID} */
  error = pt_to_constraint_pred (parser, xasl, statement->info.update.spec,
				 *non_null_attrs, select_names,
				 aptr_statement->info.query.
				 upd_del_class_cnt);
  pt_restore_assignment_links (statement->info.update.assignment, links, -1);
  if (error != NO_ERROR)
    {
      goto cleanup;
    }

  /* fill in XASL cache related information */
  /* OID of the user who is creating this XASL */
  oid = ws_identifier (db_get_user ());
  if (oid != NULL)
    {
      COPY_OID (&xasl->creator_oid, oid);
    }
  else
    {
      OID_SET_NULL (&xasl->creator_oid);
    }

  /* list of class OIDs used in this XASL */
  assert (xasl->aptr_list != NULL);
  assert (xasl->class_oid_list == NULL);
  assert (xasl->tcard_list == NULL);

  if (xasl->aptr_list != NULL)
    {
      xasl->n_oid_list = xasl->aptr_list->n_oid_list;
      xasl->aptr_list->n_oid_list = 0;
      xasl->class_oid_list = xasl->aptr_list->class_oid_list;
      xasl->aptr_list->class_oid_list = NULL;
      xasl->tcard_list = xasl->aptr_list->tcard_list;
      xasl->aptr_list->tcard_list = NULL;
      xasl->dbval_cnt = xasl->aptr_list->dbval_cnt;
    }

  if (statement->info.update.limit)
    {
      PT_NODE *limit = statement->info.update.limit;

      if (limit->next)
	{
	  limit = limit->next;
	}
      xasl->limit_row_count =
	pt_to_regu_variable (parser, limit, UNBOX_AS_VALUE);
    }

  if (prm_get_bool_value (PRM_ID_XASL_DEBUG_DUMP))
    {
      if (xasl)
	{
	  qdump_print_xasl (xasl);
	}
      else
	{
	  printf ("<NULL XASL generation>\n");
	}
    }

cleanup:

  if (aptr_statement != NULL)
    {
      parser_free_tree (parser, aptr_statement);
    }

  if (pt_has_error (parser))
    {
      pt_report_to_ersys (parser, PT_SEMANTIC);
      xasl = NULL;
    }
  else if (error != NO_ERROR)
    {
      xasl = NULL;
    }

  return xasl;
}


/*
 * parser_generate_xasl_pre () - builds xasl for query nodes,
 *                     and remembers uncorrelated queries
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
parser_generate_xasl_pre (PARSER_CONTEXT * parser, PT_NODE * node,
			  UNUSED_ARG void *arg, int *continue_walk)
{
  *continue_walk = PT_CONTINUE_WALK;

  switch (node->node_type)
    {
    case PT_SELECT:
#if defined(RYE_DEBUG)
      PT_NODE_PRINT_TO_ALIAS (parser, node, PT_CONVERT_RANGE);
#endif /* RYE_DEBUG */

      /* fall through */
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      if (!node->info.query.xasl)
	{
	  (void) pt_query_set_reference (parser, node);
	  pt_push_symbol_info (parser, node);
	}
      break;

    default:
      break;
    }

  if (pt_has_error (parser) || er_errid () == ER_LK_UNILATERALLY_ABORTED)
    {
      *continue_walk = PT_STOP_WALK;
    }

  return node;
}


/*
 * parser_generate_xasl_post () - builds xasl for query nodes
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in/out):
 */
static PT_NODE *
parser_generate_xasl_post (PARSER_CONTEXT * parser, PT_NODE * node,
			   void *arg, int *continue_walk)
{
  XASL_NODE *xasl;
  XASL_SUPP_INFO *info = (XASL_SUPP_INFO *) arg;

  if (*continue_walk == PT_STOP_WALK)
    {
      return node;
    }

  *continue_walk = PT_CONTINUE_WALK;

  assert (node != NULL);

  switch (node->node_type)
    {
    case PT_SELECT:
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      assert (node->info.query.xasl == NULL);

      /* build XASL for the query */
      xasl = parser_generate_xasl_proc (parser, node, info->query_list);
      pt_pop_symbol_info (parser);
      if (node->node_type == PT_SELECT)
	{
	  /* fill in XASL cache related information;
	     list of class OIDs used in this XASL */
	  if (xasl
	      && pt_spec_to_xasl_class_oid_list (parser,
						 node->info.query.q.select.
						 from, &info->class_oid_list,
						 &info->tcard_list,
						 &info->n_oid_list,
						 &info->oid_list_size) < 0)
	    {
	      /* might be memory allocation error */
	      PT_INTERNAL_ERROR (parser, "generate xasl");
	      xasl = NULL;
	    }
	}
      break;

    default:
      break;
    }

  if (pt_has_error (parser) || er_errid () == ER_LK_UNILATERALLY_ABORTED)
    {
      *continue_walk = PT_STOP_WALK;
    }

  return node;
}


/*
 * parser_generate_xasl () - Creates xasl proc for parse tree.
 *   return:
 *   parser(in):
 *   node(in): pointer to a query structure
 */
XASL_NODE *
parser_generate_xasl (PARSER_CONTEXT * parser, PT_NODE * node)
{
  XASL_NODE *xasl = NULL;
  PT_NODE *next;

  assert (parser != NULL);
  assert (node != NULL);

  next = node->next;
  node->next = NULL;

  node = parser_walk_tree (parser, node,
			   pt_flush_class_and_null_xasl, NULL, NULL, NULL);
  if (node == NULL)
    {
      return NULL;
    }

  switch (node->node_type)
    {
    case PT_SELECT:
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      /* do not treat the top level like a subquery, even if it
       * is a subquery with respect to something else (eg insert). */
      node->info.query.is_subquery = (PT_MISC_TYPE) 0;

      if (node)
	{
	  node = parser_walk_tree (parser, node, NULL, NULL, pt_do_cnf, NULL);
	}

      if (node)
	{
	  if (xasl_Supp_info.query_list)
	    {
	      parser_free_tree (parser, xasl_Supp_info.query_list);
	    }
	  /* add dummy node at the head of list */
	  xasl_Supp_info.query_list = parser_new_node (parser, PT_SELECT);
	  xasl_Supp_info.query_list->info.query.xasl = NULL;

	  /* XASL cache related information */
	  pt_init_xasl_supp_info ();

	  node =
	    parser_walk_tree (parser, node, parser_generate_xasl_pre, NULL,
			      parser_generate_xasl_post, &xasl_Supp_info);

	  parser_free_tree (parser, xasl_Supp_info.query_list);
	  xasl_Supp_info.query_list = NULL;
	}

      if (node && !pt_has_error (parser))
	{
	  node->next = next;
	  xasl = (XASL_NODE *) node->info.query.xasl;
	}
      break;

    default:
      break;
    }

  /* fill in XASL cache related information */
  if (xasl)
    {
      OID *oid = NULL;
      int n;
      DB_OBJECT *user = NULL;

      /* OID of the user who is creating this XASL */
      user = db_get_user ();
      if (user != NULL)
	{
	  oid = ws_identifier (user);
	}

      if (user != NULL && oid != NULL)
	{
	  COPY_OID (&xasl->creator_oid, oid);
	}
      else
	{
	  OID_SET_NULL (&xasl->creator_oid);
	}

      /* list of class OIDs used in this XASL */
      xasl->n_oid_list = 0;
      xasl->class_oid_list = NULL;
      xasl->tcard_list = NULL;

      if ((n = xasl_Supp_info.n_oid_list) > 0
	  && (xasl->class_oid_list = regu_oid_array_alloc (n))
	  && (xasl->tcard_list = regu_int_array_alloc (n)))
	{
	  xasl->n_oid_list = n;
	  (void) memcpy (xasl->class_oid_list,
			 xasl_Supp_info.class_oid_list, sizeof (OID) * n);
	  (void) memcpy (xasl->tcard_list,
			 xasl_Supp_info.tcard_list, sizeof (int) * n);
	}

      xasl->dbval_cnt = parser->host_var_count;
    }

  /* free what were allocated in pt_spec_to_xasl_class_oid_list() */
  pt_init_xasl_supp_info ();

  if (xasl)
    {
      XASL_SET_FLAG (xasl, XASL_TOP_MOST_XASL);
    }

  if (prm_get_bool_value (PRM_ID_XASL_DEBUG_DUMP))
    {
      if (xasl)
	{
	  qdump_print_xasl (xasl);
	}
      else
	{
	  printf ("<NULL XASL generation>\n");
	}
    }

  return xasl;
}


/*
 * pt_make_outlist_from_vallist () - make an outlist with const regu
 *    variables from a vallist
 *   return:
 *   parser(in):
 *   val_list_p(in):
 */
static OUTPTR_LIST *
pt_make_outlist_from_vallist (UNUSED_ARG PARSER_CONTEXT * parser,
			      VAL_LIST * val_list_p)
{
  QPROC_DB_VALUE_LIST vallist = val_list_p->valp;
  REGU_VARIABLE_LIST regulist = NULL, regu_list = NULL;
  int i;

  OUTPTR_LIST *outptr_list = regu_outlist_alloc ();
  if (!outptr_list)
    {
      return NULL;
    }

  outptr_list->valptr_cnt = val_list_p->val_cnt;
  outptr_list->valptrp = NULL;

  for (i = 0; i < val_list_p->val_cnt; i++)
    {
      regu_list = regu_varlist_alloc ();

      if (!outptr_list->valptrp)
	{
	  outptr_list->valptrp = regu_list;
	  regulist = regu_list;
	}

      regu_list->next = NULL;
      regu_list->value.type = TYPE_CONSTANT;
      regu_list->value.value.dbvalptr = vallist->val;

      if (regulist != regu_list)
	{
	  regulist->next = regu_list;
	  regulist = regu_list;
	}

      vallist = vallist->next;
    }

  return outptr_list;
}


/*
 * pt_init_precision_and_scale () -
 *   return:
 *   value(in):
 *   node(in):
 */
static void
pt_init_precision_and_scale (DB_VALUE * value, const PT_NODE * node)
{
  PT_NODE *dt;
  DB_TYPE domain_type;

  if (!value || !node)
    {
      return;
    }

  CAST_POINTER_TO_NODE (node);

  if (node->data_type == NULL)
    {
      return;
    }

  dt = node->data_type;
  domain_type = pt_type_enum_to_db (dt->type_enum);

  switch (domain_type)
    {
    case DB_TYPE_VARCHAR:
    case DB_TYPE_VARBIT:
      value->domain.char_info.length = dt->info.data_type.precision;
      break;

    case DB_TYPE_NUMERIC:
      value->domain.numeric_info.precision = dt->info.data_type.precision;
      value->domain.numeric_info.scale = dt->info.data_type.dec_scale;
      break;

    default:
      ;				/* Do nothing. This suppresses compiler's warnings. */
    }
}

/*
 * pt_agg_orderby_to_sort_list() - Translates a list of order by PT_SORT_SPEC
 *				   nodes from a aggregate function to a XASL
 *				   SORT_LIST list
 *
 *   return: newly created XASL SORT_LIST
 *   parser(in): parser context
 *   order_list(in): list of PT_SORT_SPEC nodes
 *   agg_args_list(in): list of aggregate function arguments
 *
 *  Note : Code is similar to 'pt_to_sort_list', but tweaked for ORDERBY's for
 *	   aggregate functions.
 *	   Although the existing single aggregate supporting ORDER BY, allows
 *	   only one ORDER BY item, this functions handles the general case of
 *	   multiple ORDER BY items. However, it doesn't handle the 'hidden'
 *	   argument case (see 'pt_to_sort_list'), so it may require extension
 *	   in order to support multiple ORDER BY items.
 */
static SORT_LIST *
pt_agg_orderby_to_sort_list (PARSER_CONTEXT * parser, PT_NODE * order_list,
			     UNUSED_ARG PT_NODE * agg_args_list)
{
  SORT_LIST *sort_list = NULL;
  SORT_LIST *sort = NULL;
  SORT_LIST *lastsort = NULL;
  PT_NODE *node = NULL;
  UNUSED_VAR int i;

  i = 0;			/* SORT_LIST pos_no start from 0 */

  for (node = order_list; node != NULL; node = node->next)
    {
      /* safe guard: invalid parse tree */
      if (node->node_type != PT_SORT_SPEC ||
	  node->info.sort_spec.expr == NULL)
	{
	  regu_set_error_with_zero_args (ER_REGU_SYSTEM);
	  return NULL;
	}

      /* check for end-of-sort */
      if (node->info.sort_spec.pos_descr.pos_no <= 0)
	{
	  /* internal error */
	  if (!pt_has_error (parser))
	    {
	      PT_INTERNAL_ERROR (parser, "generate order_by");
	    }
	  return NULL;
	}

      sort = regu_sort_list_alloc ();
      if (!sort)
	{
	  regu_set_error_with_zero_args (ER_REGU_SYSTEM);
	  return NULL;
	}

      /* set values */
      sort->s_order =
	(node->info.sort_spec.asc_or_desc == PT_ASC) ? S_ASC : S_DESC;
      sort->s_nulls = pt_to_null_ordering (node);
      sort->pos_descr = node->info.sort_spec.pos_descr;

      /* PT_SORT_SPEC pos_no start from 1, SORT_LIST pos_no start from 0 */
      sort->pos_descr.pos_no--;
      assert (sort->pos_descr.pos_no >= 0);

      /* link up */
      if (sort_list)
	{
	  lastsort->next = sort;
	}
      else
	{
	  sort_list = sort;
	}

      lastsort = sort;
    }

  return sort_list;
}

/*
 * pt_find_oid_scan_block () -
 *   return:       the XASL node or NULL
 *   xasl (in):	   the beginning of the XASL chain
 *   oi (in):      the OID we're looking for
 *
 *   note: in trying to optimize a general query (multiple tables, joins etc.)
 *         for using (index) keylimit for "ORDER BY ... LIMIT n" queries,
 *         we need to gather information that's scattered around the generated
 *         XASL blocks and the plan tree that was selected by the optimizer,
 *         and was used to generate the afore mentioned XASL.
 *         This method acts as a "link": it connects an xasl block with
 *         the (sub?) plan that generated it.
 */
static XASL_NODE *
pt_find_oid_scan_block (XASL_NODE * xasl, OID * oid)
{
  for (; xasl; xasl = xasl->scan_ptr)
    {
      /* only check required condition: OID match. Other, more sophisticated
         conditions should be checked from the caller */
      if (xasl->spec_list &&
	  xasl->spec_list->indexptr &&
	  oid_compare (&xasl->spec_list->indexptr->class_oid, oid) == 0)
	{
	  return xasl;
	}
    }
  return NULL;
}

/*
 * pt_ordbynum_to_key_limit_multiple_ranges () - add key limit to optimize
 *						 index access with multiple
 *						 key ranges
 *
 *   return     : NO_ERROR if key limit is generated successfully, ER_FAILED
 *		  otherwise
 *   parser(in) : parser context
 *   plan(in)   : root plan (must support multi range key limit optimization)
 *   xasl(in)   : xasl node
 */
static int
pt_ordbynum_to_key_limit_multiple_ranges (PARSER_CONTEXT * parser,
					  QO_PLAN * plan, XASL_NODE * xasl)
{
  QO_LIMIT_INFO *limit_infop;
  QO_PLAN *subplan = NULL;
  XASL_NODE *scan = NULL;
  int ret = 0;

  if (!plan)			/* simple plan, nothing to do */
    {
      goto error_exit;
    }

  if (!xasl || !xasl->spec_list)
    {
      goto error_exit;
    }

  if (!xasl->orderby_list || !xasl->ordbynum_pred)
    {
      goto error_exit;
    }

  /* find the subplan with multiple key range */
  if (qo_find_subplan_using_multi_range_opt (plan, &subplan) != NO_ERROR)
    {
      goto error_exit;
    }
  if (subplan == NULL)
    {
      goto error_exit;
    }

  scan = pt_find_oid_scan_block (xasl,
				 &(subplan->plan_un.scan.index->
				   head->class_->oid));
  if (scan == NULL)
    {
      goto error_exit;
    }

  /* check that we have index scan */
  if (scan->spec_list->type != TARGET_CLASS ||
      scan->spec_list->access != INDEX || !scan->spec_list->indexptr)
    {
      goto error_exit;
    }

  /* no data filter */
  if (scan->spec_list->where_pred)
    {
      goto error_exit;
    }

  /* generate key limit expression from limit/ordbynum */
  limit_infop = qo_get_key_limit_from_ordbynum (parser, plan, xasl, true);
  if (!limit_infop)
    {
      goto error_exit;
    }

  /* set an auto-resetting key limit for the iscan */
  ret = pt_to_key_limit (parser, NULL, limit_infop,
			 &scan->spec_list->indexptr->key_info, true);
  free_and_init (limit_infop);

  if (ret != NO_ERROR)
    {
      goto error_exit;
    }

  return NO_ERROR;

error_exit:
  assert (0);
  PT_INTERNAL_ERROR (parser, "Error generating key limit for multiple range \
			     key limit optimization");
  return ER_FAILED;
}

/*
 * pt_to_pos_descr_groupby () - Translate PT_SORT_SPEC node to
 *				QFILE_TUPLE_VALUE_POSITION node
 *   return:
 *   parser(in):
 *   pos_p(out):
 *   node(in):
 *   root(in):
 */
void
pt_to_pos_descr_groupby (PARSER_CONTEXT * parser,
			 QFILE_TUPLE_VALUE_POSITION * pos_p,
			 PT_NODE * node, PT_NODE * root)
{
  PT_NODE *temp;
  char *node_str = NULL;
  int i;

  pos_p->pos_no = -1;		/* init */

  switch (root->node_type)
    {
    case PT_SELECT:
      i = 1;			/* PT_SORT_SPEC pos_no start from 1 */

      if (node->node_type == PT_EXPR)
	{
	  unsigned int save_custom;

	  save_custom = parser->custom_print;	/* save */
	  parser->custom_print |= PT_CONVERT_RANGE;

	  node_str = parser_print_tree (parser, node);

	  parser->custom_print = save_custom;	/* restore */
	}

      for (temp = root->info.query.q.select.group_by; temp != NULL;
	   temp = temp->next)
	{
	  PT_NODE *expr = NULL;
	  if (temp->node_type != PT_SORT_SPEC)
	    {
	      continue;
	    }

	  expr = temp->info.sort_spec.expr;

	  if (node->node_type == PT_NAME)
	    {
	      if (pt_name_equal (parser, expr, node))
		{
		  pos_p->pos_no = i;
		}
	    }
	  else if (node->node_type == PT_EXPR)
	    {
	      if (pt_str_compare (node_str, parser_print_tree (parser, expr),
				  CASE_INSENSITIVE) == 0)
		{
		  pos_p->pos_no = i;
		}
	    }
	  else
	    {			/* node type must be an integer */
	      if (node->info.value.data_value.i == i)
		{
		  pos_p->pos_no = i;
		}
	    }

	  if (pos_p->pos_no != -1)
	    {
	      break;		/* found match */
	    }

	  i++;
	}

      break;

    case PT_UNION:
    case PT_INTERSECTION:
    case PT_DIFFERENCE:
      pt_to_pos_descr_groupby (parser, pos_p, node,
			       root->info.query.q.union_.arg1);
      break;

    default:
      /* an error */
      break;
    }

}

/*
 * pt_numbering_set_continue_post () - set PT_PRED_ARG_INSTNUM_CONTINUE,
 * PT_PRED_ARG_GRBYNUM_CONTINUE and PT_PRED_ARG_ORDBYNUM_CONTINUE flag
 * for numbering node
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in/out):
 *   continue_walk(in/out):
 */
PT_NODE *
pt_numbering_set_continue_post (UNUSED_ARG PARSER_CONTEXT * parser,
				PT_NODE * node, void *arg,
				UNUSED_ARG int *continue_walk)
{
  PT_NODE *child = NULL;
  int *flagp = (int *) arg;
  PT_NODE *children[3];
  int i;

  if (!node)
    {
      return NULL;
    }

  if (node->node_type == PT_EXPR && node->type_enum != PT_TYPE_LOGICAL)
    {
      children[0] = node->info.expr.arg1;
      children[1] = node->info.expr.arg2;
      children[2] = node->info.expr.arg3;

      for (i = 0; i < 3; i++)
	{
	  child = children[i];
	  if (child == NULL)
	    {
	      continue;
	    }

	  if ((child->node_type == PT_FUNCTION
	       && child->info.function.function_type == PT_GROUPBY_NUM)
	      || (child->node_type == PT_EXPR
		  && PT_IS_NUMBERING_AFTER_EXECUTION (child->info.expr.op)))
	    {
	      /* we have a subexpression with numbering functions and we
	       * don't have a logical operator therefore we set the continue
	       * flag to ensure we treat all values in the pred evaluation
	       */
	      *flagp |= PT_PRED_ARG_INSTNUM_CONTINUE;
	      *flagp |= PT_PRED_ARG_GRBYNUM_CONTINUE;
	      *flagp |= PT_PRED_ARG_ORDBYNUM_CONTINUE;
	    }
	}
    }

  return node;
}

/*
 * pt_set_orderby_for_sort_limit_plan () - setup ORDER BY list to be applied
 *					   to a SORT-LIMIT plan
 * return : ORDER BY list on success, NULL on error
 * parser (in) : parser context
 * statement (in) : statement
 * nodes_list (in/out) : list of nodes referenced by the plan
 *
 * Note: if an ORDER BY spec is not a name, the node it references is added
 * to the nodes_list nodes.
 */
PT_NODE *
pt_set_orderby_for_sort_limit_plan (PARSER_CONTEXT * parser,
				    PT_NODE * statement, PT_NODE * nodes_list)
{
  PT_NODE *order_by = NULL, *select_list = NULL, *new_order_by = NULL;
  PT_NODE *sort_spec = NULL, *node = NULL, *name = NULL, *sort = NULL;
  PT_NODE *prev = NULL;
  int pos = 0, names_count = 0, added_count = 0;
  bool add_node = false;

  order_by = statement->info.query.order_by;
  select_list = pt_get_select_list (parser, statement);

  /* count nodes in name_list, we will add new nodes at the end of the list */
  node = nodes_list;
  names_count = 0;
  while (node)
    {
      prev = node;
      names_count++;
      node = node->next;
    }

  if (prev == NULL || names_count <= 0)
    {
      assert (false);		/* is impossible */
      PT_INTERNAL_ERROR (parser, "invalid node_list");
      goto error_return;
    }

  /* create a new ORDER BY list which reflects positions of nodes in the
   * nodes_list */
  for (sort_spec = order_by; sort_spec != NULL; sort_spec = sort_spec->next)
    {
      add_node = true;

      if (sort_spec->node_type != PT_SORT_SPEC)
	{
	  assert_release (sort_spec->node_type == PT_SORT_SPEC);
	  goto error_return;
	}
      /* find the node which is referenced by this sort_spec */
      for (pos = 1, node = select_list; node != NULL;
	   pos++, node = node->next)
	{
	  if (pos == sort_spec->info.sort_spec.pos_descr.pos_no)
	    {
	      break;
	    }
	}

      if (node == NULL)
	{
	  assert_release (node != NULL);
	  goto error_return;
	}

      CAST_POINTER_TO_NODE (node);

      if (node->node_type == PT_NAME)
	{
	  /* SORT-LIMIT plans are build over the subset of classes referenced
	   * in the ORDER BY clause. This means that any name referenced in
	   * ORDER BY must also be a segment for one of the classes of this
	   * subplan. We just need to update the sort_spec position.
	   */
	  for (pos = 1, name = nodes_list; name != NULL;
	       pos++, name = name->next)
	    {
	      if (pt_name_equal (parser, name, node))
		{
		  break;
		}
	    }

	  if (name != NULL)
	    {
	      add_node = false;
	    }
	}

      if (add_node)
	{
	  /* this node was not found in the node_list. In order to be able to
	   * execute the ORDER BY clause, we have to add it to the list
	   */
	  pos = names_count + added_count + 1;
	  name = pt_point (parser, node);
	  if (name == NULL)
	    {
	      PT_INTERNAL_ERROR (parser, "allocate new node");
	      goto error_return;
	    }

	  /* add node to the end of name_list */
	  prev->next = name;
	  prev = prev->next;
	  added_count++;
	}
      else
	{
	  /* just point to it */
	  name = pt_point (parser, name);
	  if (name == NULL)
	    {
	      PT_INTERNAL_ERROR (parser, "allocate new node");
	      goto error_return;
	    }
	}

      /* create a new sort_spec for the original one and add it to the list */
      sort = parser_new_node (parser, PT_SORT_SPEC);
      if (sort == NULL)
	{
	  PT_INTERNAL_ERROR (parser, "allocate new node");
	  goto error_return;
	}

      sort->info.sort_spec.expr = name;
      sort->info.sort_spec.pos_descr.pos_no = pos;
      sort->info.sort_spec.asc_or_desc =
	sort_spec->info.sort_spec.asc_or_desc;

      CAST_POINTER_TO_NODE (name);

      new_order_by = parser_append_node (sort, new_order_by);
    }

  return new_order_by;

error_return:
  if (new_order_by != NULL)
    {
      parser_free_tree (parser, new_order_by);
    }
  return NULL;
}

/*
 * pt_to_null_ordering () - get null ordering from a sort spec
 * return : null ordering
 * sort_spec (in) : sort spec
 */
SORT_NULLS
pt_to_null_ordering (PT_NODE * sort_spec)
{
  assert_release (sort_spec != NULL);
  assert_release (sort_spec->node_type == PT_SORT_SPEC);

  switch (sort_spec->info.sort_spec.nulls_first_or_last)
    {
    case PT_NULLS_FIRST:
      return S_NULLS_FIRST;

    case PT_NULLS_LAST:
      return S_NULLS_LAST;

    case PT_NULLS_DEFAULT:
    default:
      break;
    }

  if (sort_spec->info.sort_spec.asc_or_desc == PT_ASC)
    {
      return S_NULLS_FIRST;
    }

  return S_NULLS_LAST;
}

/*
 * pt_set_limit_optimization_flags () - setup XASL flags according to
 *					query limit optimizations applied
 *					during plan generation
 * return : error code or NO_ERROR
 * parser (in)	: parser context
 * qo_plan (in) : query plan
 * xasl (in)	: xasl node
 */
static int
pt_set_limit_optimization_flags (PARSER_CONTEXT * parser, QO_PLAN * qo_plan,
				 XASL_NODE * xasl)
{
  if (qo_plan == NULL)
    {
      return NO_ERROR;
    }

  /* Set SORT-LIMIT flags */
  if (qo_has_sort_limit_subplan (qo_plan))
    {
      xasl->header.xasl_flag |= SORT_LIMIT_USED;
      xasl->header.xasl_flag |= SORT_LIMIT_CANDIDATE;
    }
  else
    {
      switch (qo_plan->info->env->use_sort_limit)
	{
	case QO_SL_USE:
	  /* A SORT-LIMIT plan can be created but planner found a better plan.
	   * In this case, there is no point in recompiling the plan a second
	   * time. There are cases in which suppling a smaller limit to the
	   * query will cause planner to choose a SORT-LIMIT plan over the
	   * current one but, since there is no way to know if this is the
	   * case, it is better to consider that this query will never use
	   * SORT-LIMIT.
	   */
	  break;

	case QO_SL_INVALID:
	  /* A SORT-LIMIT plan cannot be generated for this query */
	  break;

	case QO_SL_POSSIBLE:
	  /* The query might produce a SORT-LIMIT plan but the supplied limit.
	   * could not be evaluated.
	   */
	  xasl->header.xasl_flag |= SORT_LIMIT_CANDIDATE;
	  break;
	}
    }

  /* Set MULTI-RANGE-OPTIMIZATION flags */
  if (qo_plan_multi_range_opt (qo_plan))
    {
      /* qo_plan->multi_range_opt_use == PLAN_MULTI_RANGE_OPT_USE */
      /* convert ordbynum to key limit if we have iscan with multiple key
       * ranges */
      int err = pt_ordbynum_to_key_limit_multiple_ranges (parser, qo_plan,
							  xasl);
      if (err != NO_ERROR)
	{
	  return err;
	}

      xasl->header.xasl_flag |= MRO_CANDIDATE;
      xasl->header.xasl_flag |= MRO_IS_USED;
    }
  else if (qo_plan->multi_range_opt_use == PLAN_MULTI_RANGE_OPT_CAN_USE)
    {
      /* Query could use multi range optimization, but limit was too
       * large */
      xasl->header.xasl_flag |= MRO_CANDIDATE;
    }

  return NO_ERROR;
}

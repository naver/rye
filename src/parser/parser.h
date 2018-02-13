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
 * parser.h - Parser module functions
 */

#ifndef _PARSER_H_
#define _PARSER_H_

#ident "$Id$"

#include <stdio.h>
#include <stdarg.h>
#include "dbtype.h"
#include "dbdef.h"
#include "parse_tree.h"

#ifdef __cplusplus
extern "C"
{
#endif

  typedef enum
  { CASE_INSENSITIVE, CASE_SENSITIVE } CASE_SENSITIVENESS;

  extern PT_NODE *parser_main (PARSER_CONTEXT * p);
  extern void parser_final (void);

  extern void parser_init_func_vectors (void);

  extern PARSER_CONTEXT *parser_create_parser (void);
  extern void parser_free_parser (PARSER_CONTEXT * parser);

  extern PT_NODE *parser_parse_string (PARSER_CONTEXT * parser,
				       const char *buffer);
  extern PT_NODE *parser_parse_string_use_sys_charset (PARSER_CONTEXT *
						       parser,
						       const char *buffer);
#if defined(ENABLE_UNUSED_FUNCTION)
  extern PT_NODE **parser_parse_binary (PARSER_CONTEXT * parser,
					const char *buffer, size_t size);
#endif
  extern PT_NODE *parser_create_node (const PARSER_CONTEXT * parser);
  extern PT_NODE *parser_new_node (PARSER_CONTEXT * parser,
				   PT_NODE_TYPE node);
  extern PT_NODE *parser_init_node (PT_NODE * node);
  extern void parser_free_node (const PARSER_CONTEXT * parser,
				PT_NODE * node);
  extern void parser_free_tree (PARSER_CONTEXT * parser, PT_NODE * tree);

  extern PT_NODE *pt_pop (PARSER_CONTEXT * parser);
  extern int pt_push (PARSER_CONTEXT * parser, PT_NODE * q);
  extern PT_NODE *pt_top (PARSER_CONTEXT * parser);

  extern PT_NODE *parser_copy_tree (PARSER_CONTEXT * parser,
				    const PT_NODE * tree);
  extern PT_NODE *parser_copy_tree_list (PARSER_CONTEXT * parser,
					 PT_NODE * tree);
  extern PT_NODE *parser_append_node (PT_NODE * node, PT_NODE * list);
  extern PT_NODE *parser_append_node_or (PT_NODE * node, PT_NODE * list);
  extern PT_NODE *pt_point (PARSER_CONTEXT * parser, const PT_NODE * tree);
  extern PT_NODE *pt_point_l (PARSER_CONTEXT * parser, const PT_NODE * tree);
  extern PT_NODE *pt_pointer_stack_push (PARSER_CONTEXT * parser,
					 PT_NODE * stack, PT_NODE * node);
  extern PT_NODE *pt_pointer_stack_pop (PARSER_CONTEXT * parser,
					PT_NODE * stack, PT_NODE ** node);

  extern PT_NODE *parser_walk_leaves (PARSER_CONTEXT * parser,
				      PT_NODE * node,
				      PT_NODE_WALK_FUNCTION pre_function,
				      void *pre_argument,
				      PT_NODE_WALK_FUNCTION post_function,
				      void *post_argument);
  extern PT_NODE *parser_walk_tree (PARSER_CONTEXT * parser,
				    PT_NODE * node,
				    PT_NODE_WALK_FUNCTION pre_function,
				    void *pre_argument,
				    PT_NODE_WALK_FUNCTION post_function,
				    void *post_argument);
  extern PT_NODE *pt_continue_walk (PARSER_CONTEXT * parser, PT_NODE * tree,
				    void *arg, int *continue_walk);

  extern char *parser_print_tree (PARSER_CONTEXT * parser,
				  const PT_NODE * node);
  extern char *parser_print_tree_with_quotes (PARSER_CONTEXT * parser,
					      const PT_NODE * node);
  extern char *parser_print_tree_list (PARSER_CONTEXT * parser,
				       const PT_NODE * p);
  extern char *pt_print_alias (PARSER_CONTEXT * parser, const PT_NODE * node);
  extern char *pt_short_print (PARSER_CONTEXT * parser, const PT_NODE * p);
  extern char *pt_short_print_l (PARSER_CONTEXT * parser, const PT_NODE * p);

  extern void *parser_alloc (const PARSER_CONTEXT * parser, const int length);
  extern char *pt_append_string (const PARSER_CONTEXT * parser,
				 const char *old_string,
				 const char *new_tail);

  extern PARSER_VARCHAR *pt_append_bytes (const PARSER_CONTEXT * parser,
					  PARSER_VARCHAR * old_bytes,
					  const char *new_tail,
					  const int length);
  extern PARSER_VARCHAR *pt_append_varchar (const PARSER_CONTEXT * parser,
					    PARSER_VARCHAR * old_bytes,
					    const PARSER_VARCHAR * new_tail);
  extern PARSER_VARCHAR *pt_append_nulstring (const PARSER_CONTEXT * parser,
					      PARSER_VARCHAR * old_bytes,
					      const char *new_tail);
  extern PARSER_VARCHAR *pt_append_name (const PARSER_CONTEXT * parser,
					 PARSER_VARCHAR * string,
					 const char *name);
  extern const unsigned char *pt_get_varchar_bytes (const PARSER_VARCHAR *
						    string);
  extern int pt_get_varchar_length (const PARSER_VARCHAR * string);

  extern PARSER_VARCHAR *pt_print_bytes (PARSER_CONTEXT * parser,
					 const PT_NODE * node);
  extern PARSER_VARCHAR *pt_print_bytes_l (PARSER_CONTEXT * parser,
					   const PT_NODE * node);
  extern PARSER_VARCHAR *pt_print_bytes_spec_list (PARSER_CONTEXT * parser,
						   const PT_NODE * node);
#if defined(ENABLE_UNUSED_FUNCTION)
  extern PARSER_VARCHAR *pt_print_class_name (PARSER_CONTEXT * parser,
					      PT_NODE * p);
#endif
  extern PARSER_VARCHAR *pt_print_and_list (PARSER_CONTEXT * parser,
					    const PT_NODE * node);
  extern PARSER_VARCHAR *pt_print_bytes_alias (PARSER_CONTEXT * parser,
					       const PT_NODE * node);
  extern PARSER_VARCHAR *pt_print_node_value (PARSER_CONTEXT * parser,
					      const PT_NODE * val);
  extern PARSER_VARCHAR *pt_print_db_value (PARSER_CONTEXT * parser,
					    const struct db_value *val);

#if defined(ENABLE_UNUSED_FUNCTION)
  extern char *pt_print_query_spec_no_list (PARSER_CONTEXT * parser,
					    const PT_NODE * node);
#endif

  extern PT_NODE *pt_compile (PARSER_CONTEXT * parser, PT_NODE * statement);

  extern PT_NODE *pt_semantic_type (PARSER_CONTEXT * parser, PT_NODE * tree,
				    SEMANTIC_CHK_INFO * sc_info);

  extern void pt_report_to_ersys (const PARSER_CONTEXT * parser,
				  const PT_ERROR_TYPE error_type);

  extern void pt_record_error (PARSER_CONTEXT * parser,
			       int line_no, int col_no, const char *msg,
			       const char *context);

  extern void pt_frob_warning (PARSER_CONTEXT * parser,
			       const PT_NODE * statement, const char *fmt,
			       ...);

  extern void pt_frob_error (PARSER_CONTEXT * parser,
			     const PT_NODE * statement, const char *fmt, ...);

  extern void pt_end_query (PARSER_CONTEXT * parser, QUERY_ID query_id_self,
			    PT_NODE * statement);

  extern void pt_set_host_variables (PARSER_CONTEXT * parser,
				     int count, DB_VALUE * values);
  extern DB_VALUE *pt_host_var_db_value (PARSER_CONTEXT * parser,
					 PT_NODE * hv);
  extern DB_VALUE *pt_db_value_initialize (PARSER_CONTEXT * parser,
					   PT_NODE * value,
					   DB_VALUE * db_value,
					   int *more_type_info_needed);

  extern PT_NODE *pt_spec_to_oid_attr (PARSER_CONTEXT * parser,
				       PT_NODE * spec);
  extern int pt_length_of_list (const PT_NODE * list);
  extern int pt_length_of_select_list (PT_NODE * list, int hidden_col);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern PT_NODE *pt_get_node_from_list (PT_NODE * list, int index);
#endif

  extern PT_NODE *pt_get_select_list (PARSER_CONTEXT * parser,
				      PT_NODE * query);
  extern PT_OP_TYPE pt_op_type_from_default_expr_type (DB_DEFAULT_EXPR_TYPE
						       expr_type);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern bool pt_is_reserved_word (const char *s);
#endif
  extern bool pt_is_keyword (const char *s);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern bool pt_is_const_expr_node (PT_NODE * node);
#endif

  extern PT_NODE *pt_add_row_oid_name (PARSER_CONTEXT * parser,
				       PT_NODE * stmt);
  extern PT_NODE *pt_add_column_oid (PARSER_CONTEXT * parser, PT_NODE * stmt);

  extern PT_NODE *pt_class_pre_fetch (PARSER_CONTEXT * parser,
				      PT_NODE * statement);

  extern DB_TYPE pt_type_enum_to_db (const PT_TYPE_ENUM dt);
  extern PT_TYPE_ENUM pt_db_to_type_enum (const DB_TYPE t);
  extern DB_DOMAIN *pt_type_enum_to_db_domain (const PT_TYPE_ENUM t);
  extern void pt_put_type_enum (PARSER_CONTEXT * parser,
				PT_NODE * node, PT_NODE * data_type);
  extern DB_DOMAIN *pt_data_type_to_db_domain (PARSER_CONTEXT * parser,
					       PT_NODE * dt,
					       const char *class_name);
  extern DB_DOMAIN *pt_node_data_type_to_db_domain (PARSER_CONTEXT * parser,
						    PT_NODE * dt,
						    PT_TYPE_ENUM type);
  extern DB_DOMAIN *pt_node_to_db_domain (PARSER_CONTEXT * parser,
					  PT_NODE * node,
					  const char *class_name);
  extern DB_TYPE pt_node_to_db_type (const PT_NODE * node);

  extern PT_NODE *pt_dbval_to_value (PARSER_CONTEXT * parser,
				     const DB_VALUE * val);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern PT_NODE *pt_sm_attribute_default_value_to_node (PARSER_CONTEXT *
							 parser,
							 const SM_ATTRIBUTE *
							 default_value);
#endif
  extern DB_VALUE *pt_seq_value_to_db (PARSER_CONTEXT * parser,
				       PT_NODE * values, DB_VALUE * db_value,
				       PT_NODE ** el_types);
  extern DB_AUTH pt_auth_to_db_auth (const PT_NODE * auth);
  extern DB_DOMAIN *pt_string_to_db_domain (const char *s,
					    const char *class_name);

  extern DB_VALUE *pt_value_to_db (PARSER_CONTEXT * parser, PT_NODE * value);

  extern int pt_coerce_value (PARSER_CONTEXT * parser,
			      PT_NODE * src,
			      PT_NODE * dest,
			      PT_TYPE_ENUM desired_type,
			      PT_NODE * elem_type_list);

  extern PT_NODE *pt_bind_type_from_dbval (PARSER_CONTEXT * parser,
					   PT_NODE * node, DB_VALUE * val);

  extern PT_NODE *pt_cnf (PARSER_CONTEXT * parser, PT_NODE * node);

#if defined (ENABLE_UNUSED_FUNCTION)
  extern PT_NODE *pt_sort_in_desc_order (PT_NODE * vlist);
#endif

  extern void pt_add_type_to_set (PARSER_CONTEXT * parser,
				  const PT_NODE * val, PT_NODE ** set);

  extern PT_OP_TYPE pt_converse_op (PT_OP_TYPE op);
  extern int pt_is_between_range_op (PT_OP_TYPE op);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern int pt_is_comp_op (PT_OP_TYPE op);
#endif
  extern PT_OP_TYPE pt_negate_op (PT_OP_TYPE op);
  extern int pt_comp_to_between_op (PT_OP_TYPE left, PT_OP_TYPE right,
				    PT_COMP_TO_BETWEEN_OP_CODE_TYPE code,
				    PT_OP_TYPE * between);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern int pt_between_to_comp_op (PT_OP_TYPE between,
				    PT_OP_TYPE * left, PT_OP_TYPE * right);
#endif

  extern int pt_evaluate_def_val (PARSER_CONTEXT * parser, PT_NODE * tree,
				  DB_VALUE * db_values);
#if defined(ENABLE_UNUSED_FUNCTION)
  extern int pt_evaluate_db_value_expr (PARSER_CONTEXT * parser,
					PT_NODE * expr,
					PT_OP_TYPE op, DB_VALUE * arg1,
					DB_VALUE * arg2, DB_VALUE * arg3,
					DB_VALUE * result, TP_DOMAIN * domain,
					PT_NODE * o1, PT_NODE * o2,
					PT_NODE * o3, PT_MISC_TYPE qualifier);
  extern int pt_evaluate_function_w_args (PARSER_CONTEXT * parser,
					  FUNC_TYPE fcode,
					  DB_VALUE * args[],
					  const int num_args,
					  DB_VALUE * result);

  extern int pt_evaluate_function (PARSER_CONTEXT * parser, PT_NODE * func,
				   DB_VALUE * dbval_res);
#endif

  extern bool pt_is_symmetric_op (PT_OP_TYPE op);

  extern void mq_free_virtual_query_cache (PARSER_CONTEXT * parser);
  extern PARSER_CONTEXT *mq_virtual_queries (DB_OBJECT * class_obj);

  extern RYE_STMT_TYPE pt_node_to_stmt_type (const PT_NODE * node);

  extern int pt_check_if_query (PARSER_CONTEXT * parser, PT_NODE * stmt);

  extern PT_NODE *pt_count_input_markers (PARSER_CONTEXT * parser,
					  PT_NODE * node, void *arg,
					  int *continue_walk);

#if defined(ENABLE_UNUSED_FUNCTION)
  extern int pt_identifier_or_keyword (const char *text);
  extern KEYWORD_RECORD *pt_get_keyword_rec (int *rec_count);
  extern void pt_string_to_data_type (PARSER_CONTEXT * parser, const char *s,
				      PT_NODE * node);
#endif				/* ENABLE_UNUSED_FUNCTION */

  extern const char *pt_show_binopcode (PT_OP_TYPE n);	/* printable opcode */
  extern const char *pt_show_type_enum (PT_TYPE_ENUM t);
  extern const char *pt_show_function (FUNC_TYPE c);
  extern const char *pt_show_misc_type (PT_MISC_TYPE p);	/* return  misc_type */
#if defined(ENABLE_UNUSED_FUNCTION)
  extern const char *pt_show_node_type (PT_NODE * p);	/*return node_type */
#endif
  extern const char *pt_show_priv (PT_PRIV_TYPE t);

  extern PT_NODE *pt_lambda_with_arg (PARSER_CONTEXT * parser_ptr,
				      PT_NODE * expression,
				      PT_NODE * name,
				      PT_NODE * named_expression,
				      bool loc_check,
				      int type, bool dont_replace);

  extern PT_NODE *pt_resolve_names (PARSER_CONTEXT * parser,
				    PT_NODE * statement,
				    SEMANTIC_CHK_INFO * sc_info);

  extern PT_NODE *pt_resolve_using_index (PARSER_CONTEXT * parser,
					  PT_NODE * index, PT_NODE * from);

  extern int pt_get_correlation_level (PT_NODE * node);

  extern PT_NODE *pt_get_subquery_list (PT_NODE * node);
  extern int pt_get_expression_number (PT_NODE * node);
  extern void pt_select_list_to_one_col (PARSER_CONTEXT * parser,
					 PT_NODE * node, bool do_one);
  extern PT_NODE *pt_rewrite_set_eq_set (PARSER_CONTEXT * parser,
					 PT_NODE * exp);
  extern int pt_null_where (PARSER_CONTEXT * parser, PT_NODE * node);

#if defined (ENABLE_UNUSED_FUNCTION)
  extern int pt_get_one_tuple_from_list_id (PARSER_CONTEXT * parser,
					    PT_NODE * tree,
					    struct db_value *vals, int cnt);
#endif

  extern PT_NODE *pt_add_class_to_entity_list (PARSER_CONTEXT * r,
					       struct db_object *db,
					       PT_NODE * entity,
					       const PT_NODE * parent,
					       UINTPTR id,
					       PT_MISC_TYPE meta_class);

  extern int pt_check_unique_names (PARSER_CONTEXT * parser,
				    const PT_NODE * p);

  extern PT_NODE *pt_gather_constraints (PARSER_CONTEXT * parser,
					 PT_NODE * tree);
  extern int pt_check_set_count_set (PARSER_CONTEXT * parser, PT_NODE * arg1,
				     PT_NODE * arg2);
  extern int pt_get_expression_count (PT_NODE * node);

#if defined (ENABLE_UNUSED_FUNCTION)
  extern int pt_validate_query_spec (PARSER_CONTEXT * parser,
				     PT_NODE * s, struct db_object *c);
#endif
  extern PT_NODE *mq_regenerate_if_ambiguous (PARSER_CONTEXT * parser,
					      PT_NODE * spec,
					      PT_NODE * statement,
					      PT_NODE * from);
  extern PT_NODE *pt_do_cnf (PARSER_CONTEXT * parser, PT_NODE * node,
			     void *arg, int *continue_walk);
  extern PT_NODE *pt_find_entity (PARSER_CONTEXT * parser,
				  const PT_NODE * scope, UINTPTR id);
  extern int pt_find_var (PT_NODE * p, PT_NODE ** result);
  extern PT_NODE *pt_dump (PT_NODE * node);
  extern MISC_OPERAND pt_misc_to_qp_misc_operand (PT_MISC_TYPE
						  misc_specifier);
  extern void pt_report_to_ersys_with_statement (PARSER_CONTEXT * parser,
						 const PT_ERROR_TYPE
						 error_type,
						 PT_NODE * statement);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern PT_NODE **pt_init_one_statement_parser (PARSER_CONTEXT * parser,
						 FILE * file);
#endif

  extern int pt_resolved (const PT_NODE * expr);
  extern const char *mq_generate_name (PARSER_CONTEXT * parser,
				       const char *root, int *version);
  extern void pt_no_double_insert_assignments (PARSER_CONTEXT * parser,
					       PT_NODE * stmt);
  extern void pt_no_double_updates (PARSER_CONTEXT * parser, PT_NODE * stmt);
  extern void *pt_internal_error (PARSER_CONTEXT * parser, const char *file,
				  int line, const char *what);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern void pt_void_internal_error (PARSER_CONTEXT * parser,
				      const char *file, int line,
				      const char *what);
#endif
  extern DB_OBJECT *pt_check_user_owns_class (PARSER_CONTEXT * parser,
					      PT_NODE * cls_ref);
  extern PT_NODE *pt_domain_to_data_type (PARSER_CONTEXT * parser,
					  DB_DOMAIN * domain);
  extern PT_NODE *pt_flat_spec_pre (PARSER_CONTEXT * parser, PT_NODE * p,
				    void *scope, int *continue_walk);
  extern DB_QUERY_RESULT *pt_new_query_result_descriptor (PARSER_CONTEXT *
							  parser,
							  PT_NODE * query);
  extern PT_NODE *pt_remove_from_list (PARSER_CONTEXT * parser,
				       PT_NODE * node, PT_NODE * list);
  extern int pt_check_path_eq (PARSER_CONTEXT * parser, const PT_NODE * p,
			       const PT_NODE * q);

  extern DB_QUERY_TYPE *pt_get_titles (PARSER_CONTEXT * parser,
				       PT_NODE * query);
  extern DB_QUERY_TYPE *pt_fillin_type_size (PARSER_CONTEXT * parser,
					     PT_NODE * query,
					     DB_QUERY_TYPE * list);
  extern void pt_free_query_etc_area (PARSER_CONTEXT * parser,
				      PT_NODE * query);
  DB_OBJECT *pt_find_users_class (PARSER_CONTEXT * parser, PT_NODE * name);
  DB_ATTRIBUTE *db_get_attribute_force (DB_OBJECT * obj, const char *name);
  DB_ATTRIBUTE *db_get_attributes_force (DB_OBJECT * obj);

  extern PT_NODE *pt_set_is_view_spec (PARSER_CONTEXT * parser,
				       PT_NODE * node, void *dummy,
				       int *continue_walk);

  extern PT_NODE *pt_resolve_star (PARSER_CONTEXT * parser, PT_NODE * from,
				   PT_NODE * attr);

  extern PT_NODE *pt_bind_param_node (PARSER_CONTEXT * parser, PT_NODE * node,
				      void *arg, int *continue_walk);
  extern int pt_str_compare (const char *p, const char *q,
			     CASE_SENSITIVENESS case_flag);

  extern PT_NODE *pt_make_prim_data_type (PARSER_CONTEXT * parser,
					  PT_TYPE_ENUM e);

  extern int pt_find_attribute (PARSER_CONTEXT * parser,
				const PT_NODE * name,
				const PT_NODE * attributes);

  extern PT_NODE *pt_make_string_value (PARSER_CONTEXT * parser,
					const char *value_string);

  extern PT_NODE *pt_make_integer_value (PARSER_CONTEXT * parser,
					 const int value_int);

  extern PT_NODE *pt_and (PARSER_CONTEXT * parser_ptr,
			  const PT_NODE * expression1,
			  const PT_NODE * expression2);
  extern PT_NODE *pt_union (PARSER_CONTEXT * parser_ptr,
			    PT_NODE * expression1, PT_NODE * expression2);
  extern PT_NODE *pt_name (PARSER_CONTEXT * parser_ptr, const char *name);
  extern PT_NODE *pt_table_option (PARSER_CONTEXT * parser,
				   const PT_TABLE_OPTION_TYPE option,
				   PT_NODE * val);
  extern PT_NODE *pt_expression (PARSER_CONTEXT * parser_ptr, PT_OP_TYPE op,
				 PT_NODE * arg1, PT_NODE * arg2,
				 PT_NODE * arg3);
  extern PT_NODE *pt_expression_0 (PARSER_CONTEXT * parser_ptr,
				   PT_OP_TYPE op);
  extern PT_NODE *pt_expression_1 (PARSER_CONTEXT * parser_ptr, PT_OP_TYPE op,
				   PT_NODE * arg1);
  extern PT_NODE *pt_expression_2 (PARSER_CONTEXT * parser_ptr, PT_OP_TYPE op,
				   PT_NODE * arg1, PT_NODE * arg2);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern PT_NODE *pt_expression_3 (PARSER_CONTEXT * parser_ptr, PT_OP_TYPE op,
				   PT_NODE * arg1, PT_NODE * arg2,
				   PT_NODE * arg3);
#endif
  extern PT_NODE *pt_node_list (PARSER_CONTEXT * parser_ptr,
				PT_MISC_TYPE list_type, PT_NODE * list);
  extern PT_NODE *pt_entity (PARSER_CONTEXT * parser,
			     const PT_NODE * entity_name,
			     const PT_NODE * range_var,
			     const PT_NODE * flat_list);
  extern bool pt_name_equal (PARSER_CONTEXT * parser,
			     const PT_NODE * name1, const PT_NODE * name2);
  extern PT_NODE *pt_find_name (PARSER_CONTEXT * parser, const PT_NODE * name,
				const PT_NODE * list);
  extern bool pt_is_aggregate_function (PARSER_CONTEXT * parser,
					const PT_NODE * node);
#if defined(ENABLE_UNUSED_FUNCTION)
  extern bool pt_is_expr_wrapped_function (PARSER_CONTEXT * parser,
					   const PT_NODE * node);
#endif
  extern PT_NODE *pt_find_spec (PARSER_CONTEXT * parser, const PT_NODE * from,
				const PT_NODE * name);
  extern PT_NODE *pt_find_spec_in_statement (PARSER_CONTEXT * parser,
					     const PT_NODE * stmt,
					     const PT_NODE * name);
  extern PT_NODE *pt_find_aggregate_names (PARSER_CONTEXT * parser,
					   PT_NODE * tree, void *arg,
					   int *continue_walk);
  extern PT_NODE *pt_find_aggregate_functions_pre (PARSER_CONTEXT * parser,
						   PT_NODE * tree, void *arg,
						   int *continue_walk);
  extern PT_NODE *pt_find_aggregate_functions_post (PARSER_CONTEXT * parser,
						    PT_NODE * tree, void *arg,
						    int *continue_walk);
  extern PT_NODE *pt_has_non_idx_sarg_coll_pre (PARSER_CONTEXT * parser,
						PT_NODE * tree,
						void *arg,
						int *continue_walk);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern PT_NODE *pt_is_inst_or_orderby_num_node (PARSER_CONTEXT * parser,
						  PT_NODE * tree, void *arg,
						  int *continue_walk);
  extern PT_NODE *pt_is_inst_or_orderby_num_node_post (PARSER_CONTEXT *
						       parser, PT_NODE * tree,
						       void *arg,
						       int *continue_walk);
#endif
  extern PT_NODE *pt_add_table_name_to_from_list (PARSER_CONTEXT * parser,
						  PT_NODE * select,
						  const char *table_name,
						  const char *table_alias,
						  const DB_AUTH auth_bypass);

  extern int pt_is_ddl_statement (const PT_NODE * node);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern int pt_is_dml_statement (const PT_NODE * node);
#endif
  extern int pt_is_attr (PT_NODE * node);

  extern int pt_instnum_compatibility (PT_NODE * expr);
  extern int pt_groupbynum_compatibility (PT_NODE * expr);
  extern PT_NODE *pt_check_instnum_pre (PARSER_CONTEXT * parser,
					PT_NODE * node, void *arg,
					int *continue_walk);
  extern PT_NODE *pt_check_instnum_post (PARSER_CONTEXT * parser,
					 PT_NODE * node, void *arg,
					 int *continue_walk);
  extern PT_NODE *pt_check_groupbynum_pre (PARSER_CONTEXT * parser,
					   PT_NODE * node, void *arg,
					   int *continue_walk);
  extern PT_NODE *pt_check_groupbynum_post (PARSER_CONTEXT * parser,
					    PT_NODE * node, void *arg,
					    int *continue_walk);
  extern PT_NODE *pt_check_orderbynum_pre (PARSER_CONTEXT * parser,
					   PT_NODE * node, void *arg,
					   int *continue_walk);
  extern PT_NODE *pt_check_orderbynum_post (PARSER_CONTEXT * parser,
					    PT_NODE * node, void *arg,
					    int *continue_walk);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern PT_NODE *pt_expr_disallow_op_pre (PARSER_CONTEXT * parser,
					   PT_NODE * node, void *arg,
					   int *continue_walk);

  extern PT_NODE *pt_class_names_part (const PT_NODE * stmt);
#endif
  extern PT_NODE *pt_attrs_part (const PT_NODE * insert_statement);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern PT_NODE *pt_left_part (const PT_NODE * expr);
  extern PT_NODE *pt_from_list_part (const PT_NODE * node);
  extern PT_NODE *pt_arg1_part (const PT_NODE * node);
  extern PT_NODE *pt_arg2_part (const PT_NODE * node);
  extern PT_NODE *pt_order_by_part (const PT_NODE * node);
  extern PT_NODE *pt_group_by_part (const PT_NODE * node);
  extern PT_NODE *pt_having_part (const PT_NODE * node);
  extern PT_NODE *pt_class_part (const PT_NODE * statement);
  extern void *pt_object_part (const PT_NODE * name_node);
  extern int pt_operator_part (const PT_NODE * expr);
  extern const char *pt_qualifier_part (const PT_NODE * tbl);
  extern PT_NODE *pt_right_part (const PT_NODE * expr);
#endif
  extern PT_NODE *pt_get_end_path_node (PT_NODE * node);
  extern PT_NODE *pt_get_first_arg (PT_NODE * node);
  extern PT_NODE *pt_get_first_arg_ignore_prior (PT_NODE * node);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern const char *pt_string_part (const PT_NODE * tbl);
  extern PT_NODE *pt_values_part (const PT_NODE * insert_statement);
#endif
  extern PT_NODE *pt_get_subquery_of_insert_select (const PT_NODE *
						    insert_statement);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern PT_NODE *pt_select_list_part (const PT_NODE * stmt);
  extern PT_NODE *pt_where_part (const PT_NODE * stmt);
  extern void pt_set_node_etc (PT_NODE * node, const void *etc);
#endif

  extern void pt_null_etc (PT_NODE * node);
  extern void *pt_node_etc (const PT_NODE * col);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern PT_NODE *pt_node_next (const PT_NODE * node);
#endif

  extern void pt_record_warning (PARSER_CONTEXT * parser,
				 int line_no, int col_no, const char *msg);
#if 0				/* unused */
  extern PT_NODE *pt_get_warnings (const PARSER_CONTEXT * parser);
#endif
  extern PT_NODE *pt_get_errors (PARSER_CONTEXT * parser);

  extern PT_NODE *pt_get_next_error (PT_NODE * errors,
				     int *line_no, int *col_no,
				     const char **msg);
  extern void pt_reset_error (PARSER_CONTEXT * parser);
  extern int pt_has_error (const PARSER_CONTEXT * parser);

#if defined (ENABLE_UNUSED_FUNCTION)
  extern bool pt_column_updatable (PARSER_CONTEXT * parser, PT_NODE * query);
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
  extern const char *pt_get_select_from_name (PARSER_CONTEXT * parser,
					      const PT_NODE * spec);
  extern const char *pt_get_proxy_spec_name (const char *qspec);
  extern const char *pt_get_spec_name (PARSER_CONTEXT * parser,
				       const PT_NODE * selqry);
#endif
  extern const char *pt_get_name (PT_NODE * nam);
  extern PT_NODE *pt_get_parameters (PARSER_CONTEXT * parser,
				     PT_NODE * statement);

  extern bool pt_has_aggregate (PARSER_CONTEXT * parser, PT_NODE * node);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern bool pt_has_inst_or_orderby_num (PARSER_CONTEXT * parser,
					  PT_NODE * node);

  extern PT_NODE *pt_get_input_host_vars (const PT_HOST_VARS * hv);
  extern PT_HOST_VARS *pt_host_info (PARSER_CONTEXT * parser, PT_NODE * stmt);
  extern void pt_free_host_info (PT_HOST_VARS * hv);
#endif

  extern void pt_register_orphan (PARSER_CONTEXT * parser,
				  const PT_NODE * orphan);
  extern void pt_register_orphan_db_value (PARSER_CONTEXT * parser,
					   const DB_VALUE * orphan);
  extern void pt_free_orphans (PARSER_CONTEXT * parser);

  extern bool pt_sort_spec_cover (PT_NODE * cur_list, PT_NODE * new_list);
  extern bool pt_sort_spec_cover_groupby (PARSER_CONTEXT * parser,
					  PT_NODE * sort_list,
					  PT_NODE * group_list,
					  PT_NODE * tree);

  extern int dbcs_get_next (PARSER_CONTEXT * parser);
  extern void dbcs_start_input (void);

  extern void parser_free_lcks_classes (PARSER_CONTEXT * parser);

  extern PT_NODE *pt_limit_to_numbering_expr (PARSER_CONTEXT * parser,
					      PT_NODE * limit,
					      PT_OP_TYPE num_op,
					      bool is_gby_num);

#if defined (ENABLE_UNUSED_FUNCTION)
  extern void pt_copy_statement_flags (PT_NODE * source,
				       PT_NODE * destination);
#endif
  extern void pt_set_fill_default_in_path_expression (PT_NODE * node);
#if defined (ENABLE_UNUSED_FUNCTION)
  void pt_fixup_column_type (PT_NODE * col);
#endif
  extern int pt_node_list_to_array (PARSER_CONTEXT * parser,
				    PT_NODE * arg_list,
				    PT_NODE * arg_array[],
				    const int array_size, int *num_args);
  extern int pt_check_order_by (PARSER_CONTEXT * parser, PT_NODE * query);

  extern PT_NODE *parser_make_expression (PARSER_CONTEXT * parser,
					  PT_OP_TYPE OP, PT_NODE * arg1,
					  PT_NODE * arg2, PT_NODE * arg3);
  extern PT_NODE *parser_keyword_func (PT_NODE * func_node, PT_NODE * args);

  extern PT_NODE *pt_convert_to_logical_expr (PARSER_CONTEXT * parser,
					      PT_NODE * node,
					      bool use_parens_inside,
					      bool use_parens_outside);
  extern bool pt_is_operator_logical (PT_OP_TYPE op);
  extern bool pt_is_pseudo_const (PT_NODE * expr);
  extern PT_OP_TYPE pt_op_type_from_default_expr (DB_DEFAULT_EXPR_TYPE
						  expr_type);
  extern int pt_mark_spec_list_for_update (PARSER_CONTEXT * parser,
					   PT_NODE * statement);
  extern int pt_mark_spec_list_for_delete (PARSER_CONTEXT * parser,
					   PT_NODE * delete_statement);
  extern void pt_init_assignments_helper (PARSER_CONTEXT * parser,
					  PT_ASSIGNMENTS_HELPER * helper,
					  PT_NODE * assignment);
  extern PT_NODE *pt_get_next_assignment (PT_ASSIGNMENTS_HELPER * helper);
  extern void pt_restore_assignment_links (PT_NODE * assigns,
					   PT_NODE ** links, int count);
  extern int pt_get_assignment_lists (PARSER_CONTEXT * parser,
				      PT_NODE ** select_names,
				      PT_NODE ** select_values,
				      int *no_vals,
				      PT_NODE * assign,
				      PT_NODE *** old_links);

  extern PT_NODE *pt_sort_spec_list_to_name_node_list (PARSER_CONTEXT *
						       parser,
						       PT_NODE *
						       sort_spec_list);

  extern int pt_check_grammar_charset_collation (PARSER_CONTEXT * parser,
						 PT_NODE * coll_node,
						 int *coll_id);
  extern bool pt_get_collation_info (PT_NODE * node, int *collation_id);

#if defined (ENABLE_UNUSED_FUNCTION)
  extern PT_NODE *pt_find_node_type_pre (PARSER_CONTEXT * parser,
					 PT_NODE * node, void *arg,
					 int *continue_walk);
  extern PT_NODE *pt_find_op_type_pre (PARSER_CONTEXT * parser,
				       PT_NODE * node, void *arg,
				       int *continue_walk);
#endif

  extern int pt_get_query_limit_value (PARSER_CONTEXT * parser,
				       PT_NODE * query, DB_VALUE * limit_val);
  extern bool pt_check_ordby_num_for_multi_range_opt (PARSER_CONTEXT * parser,
						      PT_NODE * query,
						      bool * mro_candidate,
						      bool * cannot_eval);

  extern void pt_free_statement_xasl_id (PT_NODE * statement);
  extern bool pt_recompile_for_limit_optimizations (PARSER_CONTEXT * parser,
						    PT_NODE * statement,
						    int xasl_flag);

  extern PT_NODE *pt_make_query_explain (PARSER_CONTEXT * parser,
					 PT_NODE * statement);
  extern PT_NODE *pt_make_query_show_trace (PARSER_CONTEXT * parser);

  extern void insert_check_names_in_value_clauses (PARSER_CONTEXT * parser,
						   PT_NODE *
						   insert_statement);
  extern int pt_name_occurs_in_from_list (PARSER_CONTEXT * parser,
					  const char *name,
					  PT_NODE * from_list);

  extern PT_NODE *pt_has_non_groupby_column_node (PARSER_CONTEXT * parser,
						  PT_NODE * node, void *arg,
						  int *continue_walk);

  extern void regu_set_error_with_zero_args (int err_type);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern void regu_set_error_with_one_args (int err_type, const char *infor);
  extern void regu_set_global_error (void);
#endif

#ifdef __cplusplus
}
#endif

#endif				/* _PARSER_H_ */

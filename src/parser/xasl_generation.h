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
 * xasl_generation.h - Generate XASL from parse tree
 */

#ifndef _XASL_GENERATION_H_
#define _XASL_GENERATION_H_

#ident "$Id$"

#include "query_executor.h"
#include "parser.h"
#include "object_domain.h"
#include "dbtype.h"
#include "optimizer.h"

#define MATCH_ALL       1

#define PT_PRED_ARG_INSTNUM_CONTINUE    0x01
#define PT_PRED_ARG_GRBYNUM_CONTINUE    0x02
#define PT_PRED_ARG_ORDBYNUM_CONTINUE   0x04

typedef struct table_info TABLE_INFO;
struct table_info
{
  struct table_info *next;      /* usual list link */
  PT_NODE *class_spec;          /* SHARED pointer to parse tree entity spec */
  const char *exposed;          /* SHARED pointer to entity spec exposed name */
  UINTPTR spec_id;
  PT_NODE *attribute_list;      /* is a list of names which appear anywhere in
                                   a select statement with the exposed name */
  VAL_LIST *value_list;         /* is a list of DB_VALUES which correspond by
                                   position in list to the attributes named
                                   in attribute_list */
  int is_fetch;
};

/*
 * This structure represents the global information needed to be stored
 * to translate a parse tree into XASL. It is only used in the process of
 * translation. A new one is "pushed" on the stack every time a query
 * statement is to be translated, and "popped" when its translation is
 * complete. Nested select statements will then yield a stack of these
 * which can be used to resolve attribute names referenced.
 */
typedef enum
{ UNBOX_AS_VALUE, UNBOX_AS_TABLE } UNBOX;
typedef struct symbol_info SYMBOL_INFO;
struct symbol_info
{
  SYMBOL_INFO *stack;           /* enclosing scope symbol table */
  TABLE_INFO *table_info;       /* list of exposed class names with
                                   corresponding attributes and values */
  PT_NODE *current_class;       /* value of a class to resolve */
  HEAP_CACHE_ATTRINFO *cache_attrinfo;
  PT_NODE *current_listfile;
  VAL_LIST *listfile_value_list;
  UNBOX listfile_unbox;
  int listfile_attr_offset;
  PT_NODE *query_node;          /* the query node that is being translated */
};



typedef struct aggregate_info AGGREGATE_INFO;
struct aggregate_info
{
  AGGREGATE_TYPE *head_list;
  OUTPTR_LIST *out_list;
  VAL_LIST *value_list;
  REGU_VARIABLE_LIST regu_list;
  PT_NODE *out_names;
  DB_VALUE **grbynum_valp;
  const char *class_name;
  int flag_agg_optimize;
};

extern char *query_Plan_dump_filename;
extern FILE *query_Plan_dump_fp;

extern REGU_VARIABLE *pt_to_regu_variable (PARSER_CONTEXT * p, PT_NODE * node, UNBOX unbox);
extern PRED_EXPR *pt_to_pred_expr (PARSER_CONTEXT * p, PT_NODE * node);
extern PRED_EXPR *pt_to_pred_expr_with_arg (PARSER_CONTEXT * p, PT_NODE * node, int *argp);
extern XASL_NODE *parser_generate_xasl (PARSER_CONTEXT * p, PT_NODE * node);
extern REGU_VARIABLE *pt_make_regu_arith (REGU_VARIABLE * arg1,
                                          REGU_VARIABLE * arg2, REGU_VARIABLE * arg3, OPERATOR_TYPE op);
extern TP_DOMAIN *pt_xasl_type_enum_to_domain (const PT_TYPE_ENUM type);
extern TP_DOMAIN *pt_xasl_node_to_domain (PARSER_CONTEXT * parser, PT_NODE * node);
extern PT_NODE *pt_to_upd_del_query (PARSER_CONTEXT * parser,
                                     PT_NODE * select_names,
                                     PT_NODE * select_list, PT_NODE * from,
                                     PT_NODE * where,
                                     PT_NODE * using_index,
                                     PT_NODE * order_by, PT_NODE * orderby_for, PT_SELECT_PURPOSE purpose);
extern XASL_NODE *pt_to_insert_xasl (DB_SESSION * session, PARSER_CONTEXT * parser, PT_NODE * node);
extern XASL_NODE *pt_to_update_xasl (DB_SESSION * session, PT_NODE * statement, PT_NODE ** non_null_attrs);
extern XASL_NODE *pt_to_delete_xasl (DB_SESSION * session, PT_NODE * node);
extern XASL_NODE *pt_append_xasl (XASL_NODE * to, XASL_NODE * from_list);
extern XASL_NODE *pt_remove_xasl (XASL_NODE * xasl_list, XASL_NODE * remove);
extern ACCESS_SPEC_TYPE *pt_to_spec_list (PARSER_CONTEXT * parser,
                                          PT_NODE * flat,
                                          PT_NODE * where_key_part,
                                          PT_NODE * where_part,
                                          PT_NODE * pk_next_part,
                                          QO_PLAN * plan, QO_XASL_INDEX_INFO * indx, PT_NODE * src_derived_table);
extern VAL_LIST *pt_to_val_list (PARSER_CONTEXT * parser, UINTPTR id);
extern SORT_LIST *pt_to_orderby (PARSER_CONTEXT * parser, PT_NODE * order_list, PT_NODE * root);
extern XASL_NODE *pt_skeleton_buildlist_proc (PARSER_CONTEXT * parser, PT_NODE * namelist);
extern XASL_NODE *ptqo_to_scan_proc (PARSER_CONTEXT * parser,
                                     QO_PLAN * plan,
                                     XASL_NODE * xasl,
                                     PT_NODE * spec,
                                     PT_NODE * where_key_part, PT_NODE * where_part, QO_XASL_INDEX_INFO * info);
extern XASL_NODE *ptqo_to_list_scan_proc (PARSER_CONTEXT * parser,
                                          XASL_NODE * xasl,
                                          PROC_TYPE type, XASL_NODE * listfile, PT_NODE * namelist, PT_NODE * pred);
extern void pt_set_dptr (PARSER_CONTEXT * parser, PT_NODE * node, XASL_NODE * xasl, UINTPTR id);
#if defined (ENABLE_UNUSED_FUNCTION)
extern PT_NODE *pt_flush_classes (PARSER_CONTEXT * parser, PT_NODE * tree, void *arg, int *continue_walk);
#endif
extern int pt_is_single_tuple (PARSER_CONTEXT * parser, PT_NODE * select_node);
extern void pt_to_pos_descr (PARSER_CONTEXT * parser,
                             QFILE_TUPLE_VALUE_POSITION * pos_p,
                             PT_NODE * node, PT_NODE * root, PT_NODE ** referred_node);
extern void pt_to_pos_descr_groupby (PARSER_CONTEXT * parser,
                                     QFILE_TUPLE_VALUE_POSITION * pos_p, PT_NODE * node, PT_NODE * root);
extern void pt_set_numbering_node_etc (PARSER_CONTEXT * parser,
                                       PT_NODE * node_list, DB_VALUE ** instnum_valp, DB_VALUE ** ordbynum_valp);
#if defined (ENABLE_UNUSED_FUNCTION)
extern PT_NODE *pt_get_numbering_node_etc (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk);
#endif
extern int pt_copy_upddel_hints_to_select (PARSER_CONTEXT * parser, PT_NODE * node, PT_NODE * select_stmt);
extern PT_NODE *pt_set_orderby_for_sort_limit_plan (PARSER_CONTEXT * parser, PT_NODE * statement, PT_NODE * name_list);
extern SORT_NULLS pt_to_null_ordering (PT_NODE * sort_spec);
#endif /* _XASL_GENERATION_H_ */

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
 * parse_tree.h - Parse tree structures and types
 */

#ifndef _PARSE_TREE_H_
#define _PARSE_TREE_H_

#ident "$Id$"

#include <setjmp.h>
#include "jansson.h"

#include "config.h"

#include "query_evaluator.h"
#include "cursor.h"
#include "string_opfunc.h"
#include "message_catalog.h"
#include "authenticate.h"
#include "system_parameter.h"

#define MAX_PRINT_ERROR_CONTEXT_LENGTH 64

#define PT_ERROR( parser, node, msg ) \
    pt_frob_error( parser, node, msg )

#define PT_ERRORm(parser, node, setNo, msgNo) \
    pt_frob_error(parser, node, \
                  msgcat_message (MSGCAT_CATALOG_RYE, setNo, msgNo))


#define PT_ERRORc( parser, node, msg ) \
    pt_frob_error( parser, node, "%s", msg )

#define PT_ERRORf( parser, node, msg, arg1) \
    pt_frob_error( parser, node, msg, arg1 )

#define PT_ERRORmf(parser, node, setNo, msgNo, arg1) \
    PT_ERRORf(parser, node, \
              msgcat_message (MSGCAT_CATALOG_RYE, setNo, msgNo), arg1)

#define PT_ERRORf2( parser, node, msg, arg1, arg2) \
    pt_frob_error( parser, node, msg, arg1, arg2 )

#define PT_ERRORmf2(parser, node, setNo, msgNo, arg1, arg2) \
    PT_ERRORf2(parser, node, \
               msgcat_message (MSGCAT_CATALOG_RYE, setNo, msgNo), \
               arg1, arg2)

#define PT_ERRORf3( parser, node, msg, arg1, arg2, arg3) \
    pt_frob_error( parser, node, msg, arg1, arg2, arg3 )

#define PT_ERRORmf3(parser, node, setNo, msgNo, arg1, arg2, arg3) \
    PT_ERRORf3(parser, node, \
               msgcat_message (MSGCAT_CATALOG_RYE, setNo, msgNo), \
               arg1, arg2, arg3)

#define PT_ERRORf4( parser, node, msg, arg1, arg2, arg3, arg4) \
    pt_frob_error( parser, node, msg, arg1, arg2, arg3, arg4 )

#define PT_ERRORmf4(parser, node, setNo, msgNo, arg1, arg2, arg3, arg4) \
    PT_ERRORf4(parser, node, \
               msgcat_message (MSGCAT_CATALOG_RYE, setNo, msgNo), \
               arg1, arg2, arg3, arg4)

#define PT_ERRORf5( parser, node, msg, arg1, arg2, arg3, arg4, arg5) \
    pt_frob_error( parser, node, msg, arg1, arg2, arg3, arg4 , arg5)

#define PT_ERRORmf5(parser, node, setNo, msgNo, arg1, arg2, arg3, arg4 , \
		    arg5) \
    PT_ERRORf5(parser, node, \
               msgcat_message (MSGCAT_CATALOG_RYE, setNo, msgNo), \
               arg1, arg2, arg3, arg4 ,arg5)

#define PT_WARNING( parser, node, msg ) \
    pt_frob_warning( parser, node, msg )

#define PT_WARNINGm(parser, node, setNo, msgNo) \
    PT_WARNING(parser, node, \
               msgcat_message (MSGCAT_CATALOG_RYE, setNo, msgNo))

#define PT_WARNINGc( parser, node, msg ) \
    PT_WARNING( parser, node, msg )

#define PT_WARNINGf( parser, node, msg, arg1) \
    pt_frob_warning( parser, node, msg, arg1 )

#define PT_WARNINGmf(parser, node, setNo, msgNo, arg1) \
    PT_WARNINGf(parser, node, \
                msgcat_message (MSGCAT_CATALOG_RYE, setNo, msgNo), arg1)

#define PT_WARNINGf2( parser, node, msg, arg1, arg2) \
    pt_frob_warning( parser, node, msg, arg1, arg2 )

#define PT_WARNINGmf2(parser, node, setNo, msgNo, arg1, arg2) \
    PT_WARNINGf2(parser, node, \
                 msgcat_message (MSGCAT_CATALOG_RYE, setNo, msgNo), \
                 arg1, arg2)

#define PT_WARNINGf3( parser, node, msg, arg1, arg2, arg3) \
    pt_frob_warning( parser, node, msg, arg1, arg2, arg3 )

#define PT_WARNINGmf3(parser, node, setNo, msgNo, arg1, arg2, arg3) \
    PT_WARNINGf3(parser, node, \
                 msgcat_message (MSGCAT_CATALOG_RYE, setNo, msgNo), \
                 arg1, arg2, arg3)


#define PT_SET_JMP_ENV(parser) \
    do { \
      if (setjmp((parser)->jmp_env) != 0) { \
	pt_record_error((parser), (parser)->line, (parser)->column, \
			msgcat_message (MSGCAT_CATALOG_RYE, \
			                MSGCAT_SET_PARSER_SEMANTIC, \
				        MSGCAT_SEMANTIC_OUT_OF_MEMORY), \
					NULL); \
	(parser)->jmp_env_active = 0; \
	if ((parser)->au_save) \
	    AU_ENABLE((parser)->au_save); \
	return NULL; \
      } \
        else (parser)->jmp_env_active = 1; \
    } while(0)

#define PT_CLEAR_JMP_ENV(parser) \
    do { \
      (parser)->jmp_env_active = 0; \
    } while(0)

#define PT_INTERNAL_ERROR(parser, what) \
	pt_internal_error((parser), __FILE__, __LINE__, (what))

#define PT_IS_QUERY_NODE_TYPE(x) \
    (  (x) == PT_SELECT     || (x) == PT_UNION \
    || (x) == PT_DIFFERENCE || (x) == PT_INTERSECTION)

#define PT_SET_NULL_NODE(n)				\
    do { 						\
      assert ((n) != NULL);				\
      (n)->is_null_node = 1; 				\
      (n)->type_enum = PT_TYPE_NULL; /* TODO - */	\
    } while(0)

#define PT_IS_NULL_NODE(n)				\
   (((n) && (n)->is_null_node == 1)			\
    || ((n) && (n)->type_enum == PT_TYPE_NULL) /* TODO - */ ? true : false)

#define PT_IS_NUMERIC_TYPE(t) \
        ( (((t) == PT_TYPE_INTEGER)  || \
	   ((t) == PT_TYPE_BIGINT)   || \
	   ((t) == PT_TYPE_DOUBLE)   || \
	   ((t) == PT_TYPE_LOGICAL) || \
	   ((t) == PT_TYPE_NUMERIC)) ? true : false )

#define PT_IS_DISCRETE_NUMBER_TYPE(t) \
        ( (((t) == PT_TYPE_INTEGER)  || \
           ((t) == PT_TYPE_BIGINT)) ? true : false )

#define PT_IS_COLLECTION_TYPE(t) \
        ( (((t) == PT_TYPE_SEQUENCE)) ? true : false )

#define PT_IS_STRING_TYPE(t) \
        ( (((t) == PT_TYPE_VARCHAR)  || \
	   ((t) == PT_TYPE_VARBIT))  ? true : false )

#define PT_IS_CHAR_STRING_TYPE(t) \
        ( (((t) == PT_TYPE_VARCHAR)) ? true : false )

#define PT_IS_BIT_STRING_TYPE(t) \
        ( ((t) == PT_TYPE_VARBIT) ? true : false )

#define PT_IS_COMPLEX_TYPE(t) \
        ( (((t) == PT_TYPE_NUMERIC)   || \
	   ((t) == PT_TYPE_VARCHAR)   || \
	   ((t) == PT_TYPE_VARBIT)    || \
	   ((t) == PT_TYPE_OBJECT)    || \
	   ((t) == PT_TYPE_SEQUENCE)) ? true : false )

#define PT_IS_DATE_TIME_TYPE(t) \
        ( (((t) == PT_TYPE_DATE)       || \
	   ((t) == PT_TYPE_TIME)       || \
	   ((t) == PT_TYPE_DATETIME)) ? true : false )

#define PT_HAS_DATE_PART(t) \
        ( (((t) == PT_TYPE_DATE)       || \
	   ((t) == PT_TYPE_DATETIME)) ? true : false )

#define PT_HAS_TIME_PART(t) \
        ( (((t) == PT_TYPE_TIME)       || \
	   ((t) == PT_TYPE_DATETIME)) ? true : false )

#define PT_IS_PRIMITIVE_TYPE(t) \
        ( (((t) == PT_TYPE_OBJECT) || \
	   ((t) == PT_TYPE_NONE))  ? false : true )

#define PT_IS_PARAMETERIZED_TYPE(t) \
        ( (((t) == PT_TYPE_NUMERIC)  || \
	   ((t) == PT_TYPE_VARCHAR)  || \
	   ((t) == PT_TYPE_VARBIT))     ? true : false )

#define PT_HAS_COLLATION(t) \
        ( (((t) == PT_TYPE_VARCHAR))  ? true : false )

#define pt_is_select(n) PT_IS_SELECT(n)
#define pt_is_union(n) PT_IS_UNION(n)
#define pt_is_intersection(n) PT_IS_INTERSECTION(n)
#define pt_is_difference(n) PT_IS_DIFFERENCE(n)
#define pt_is_query(n) PT_IS_QUERY(n)
#define pt_is_correlated_subquery(n) PT_IS_CORRELATED_SUBQUERY(n)
#define pt_is_dot_node(n) PT_IS_DOT_NODE(n)
#define pt_is_expr_node(n) PT_IS_EXPR_NODE(n)
#define pt_is_function(n) PT_IS_FUNCTION(n)
#define pt_is_name_node(n) PT_IS_NAME_NODE(n)
#define pt_is_oid_name(n) PT_IS_OID_NAME(n)
#define pt_is_value_node(n) PT_IS_VALUE_NODE(n)
#define pt_is_hostvar(n) PT_IS_HOSTVAR(n)
#define pt_is_input_hostvar(n) PT_IS_INPUT_HOSTVAR(n)
#define pt_is_const(n) PT_IS_CONST(n)
#define pt_is_const_input_hostvar(n) PT_IS_CONST_INPUT_HOSTVAR(n)
#define pt_is_cast_const_input_hostvar(n) PT_IS_CAST_CONST_INPUT_HOSTVAR(n)
#define pt_is_instnum(n) PT_IS_INSTNUM(n)
#define pt_is_orderbynum(n) PT_IS_ORDERBYNUM(n)
#define pt_is_distinct(n) PT_IS_DISTINCT(n)
#define pt_is_meta(n) PT_IS_META(n)
#define pt_is_unary(op) PT_IS_UNARY(op)

#define PT_IS_SELECT(n) \
        ( (n) ? ((n)->node_type == PT_SELECT) : false )

#define PT_IS_UNION(n) \
        ( (n) ? ((n)->node_type == PT_UNION) : false )

#define PT_IS_INTERSECTION(n) \
        ( (n) ? ((n)->node_type == PT_INTERSECTION) : false )

#define PT_IS_DIFFERENCE(n) \
        ( (n) ? ((n)->node_type == PT_DIFFERENCE) : false )

#define PT_IS_QUERY(n) \
        ( (n) ? (PT_IS_QUERY_NODE_TYPE((n)->node_type)) : false )

#define PT_IS_CORRELATED_SUBQUERY(n) \
        ( (PT_IS_QUERY((n)) && (n)->info.query.correlation_level > 0) ? \
          true : false )

#define PT_IS_DOT_NODE(n) \
        ( (n) ? ((n)->node_type == PT_DOT_) : false )

#define PT_IS_EXPR_NODE(n) \
        ( (n) ? ((n)->node_type == PT_EXPR) : false )

#define PT_IS_ASSIGN_NODE(n) \
        ( (n) ? ((n)->node_type == PT_EXPR && \
                 (n)->info.expr.op == PT_ASSIGN) \
              : false )

#define PT_IS_FUNCTION(n) \
        ( (n) ? ((n)->node_type == PT_FUNCTION) : false )

#define PT_IS_NAME_NODE(n) \
        ( (n) ? ((n)->node_type == PT_NAME) : false )

#define PT_IS_OID_NAME(n) \
        ( (n) ? ((n)->node_type == PT_NAME && \
                 (n)->info.name.meta_class == PT_OID_ATTR) \
              : false )

#define PT_IS_VALUE_NODE(n) \
        ( (n) ? ((n)->node_type == PT_VALUE) : false )

#define PT_IS_HOSTVAR(n) \
        ( (n) ? ((n)->node_type == PT_HOST_VAR) : false )

#define PT_IS_INPUT_HOSTVAR(n) \
        ( (n) ? ((n)->node_type == PT_HOST_VAR && \
                 (n)->info.host_var.var_type == PT_HOST_IN) \
              : false )

#define PT_IS_CONST(n) \
        ( (n) ? ((n)->node_type == PT_VALUE || \
                 (n)->node_type == PT_HOST_VAR) \
              : false )

#define PT_IS_CONST_INPUT_HOSTVAR(n) \
        ( (n) ? ((n)->node_type == PT_VALUE || \
                 ((n)->node_type == PT_HOST_VAR && \
                  (n)->info.host_var.var_type == PT_HOST_IN)) \
              : false )

#define PT_IS_INSTNUM(n) \
        ( (n) ? ((n)->node_type == PT_EXPR && \
         ((n)->info.expr.op == PT_INST_NUM || \
          (n)->info.expr.op == PT_ROWNUM)) \
              : false )

#define PT_IS_ORDERBYNUM(n) \
        ( (n) ? ((n)->node_type == PT_EXPR && \
         ((n)->info.expr.op == PT_ORDERBY_NUM)) \
              : false )

#define PT_IS_DISTINCT(n) \
        ( ((n) && PT_IS_QUERY_NODE_TYPE((n)->node_type) ? \
           (n)->info.query.all_distinct != PT_ALL : false) )

#define PT_IS_META(n) \
        ( ((n) ? ((n)->node_type == PT_NAME ? \
                  ((n)->info.name.meta_class == PT_OID_ATTR) : false) \
              : false) )
#define PT_IS_HINT_NODE(n) \
        ( (n) ? ((n)->node_type == PT_NAME && \
                 (n)->info.name.meta_class == PT_HINT_NAME) \
            : false )

#define PT_IS_UNARY(op) \
        ( ((op) == PT_NOT || \
           (op) == PT_IS_NULL || \
           (op) == PT_IS_NOT_NULL || \
           (op) == PT_EXISTS || \
           (op) == PT_UNARY_MINUS) ? true : false )

#define PT_DOES_FUNCTION_HAVE_DIFFERENT_ARGS(op) \
        ((op) == PT_MODULUS || (op) == PT_SUBSTRING || \
         (op) == PT_LPAD || (op) == PT_RPAD || \
         (op) == PT_TO_CHAR || (op) == PT_TO_NUMBER || \
         (op) == PT_POWER || (op) == PT_ROUND || \
         (op) == PT_TRUNC || (op) == PT_INSTR || \
         (op) == PT_LEAST || (op) == PT_GREATEST || \
	 (op) == PT_FIELD || \
	 (op) == PT_REPEAT || (op) == PT_SUBSTRING_INDEX || \
	 (op) == PT_MAKEDATE || (op) == PT_MAKETIME || (op) == PT_IF || \
	 (op) == PT_STR_TO_DATE)

#define PT_IS_NUMBERING_AFTER_EXECUTION(op) \
        ( ((op) == PT_INST_NUM || \
           (op) == PT_ROWNUM || \
           (op) == PT_ORDERBY_NUM) ? true : false )

#define PT_IS_EXPR_NODE_WITH_OPERATOR(n, op_type) \
        ( (PT_IS_EXPR_NODE (n)) ? ((n)->info.expr.op == (op_type)) : false )

#define PT_NODE_DATA_TYPE(n) \
	( (n) ? (n)->data_type : NULL )

#define PT_IS_SORT_SPEC_NODE(n) \
        ( (n) ? ((n)->node_type == PT_SORT_SPEC) : false )

#if !defined (SERVER_MODE)
/* the following defines support host variable binding for internal statements.
   internal statements can be generated on TEXT handling, and these statements
   can include host variables derived from original statement. so, to look up
   host variable table by internal statements at parser, keep the parser,
   i.e parent_parser */

#define CLEAR_HOST_VARIABLES(parser_) \
    do { DB_VALUE *hv; int i; \
        for (i = 0, hv = parser_->host_variables; \
             i < (parser_->host_var_count); i++, hv++) \
            db_value_clear(hv); \
        free_and_init(parser_->host_variables); } while (0)

#define SET_HOST_VARIABLES_IF_INTERNAL_STATEMENT(parser_) \
    do { if (parent_parser) { \
             if (parser_->host_variables != NULL && \
                 parser_->host_variables != parent_parser->host_variables) { \
                 CLEAR_HOST_VARIABLES(parser_); } \
             parser_->host_variables = parent_parser->host_variables; \
             parser_->host_var_count = parent_parser->host_var_count; \
             parser_->set_host_var = 1; } } while (0)

#define RESET_HOST_VARIABLES_IF_INTERNAL_STATEMENT(parser_) \
    do { if (parent_parser) { \
             parser_->host_variables = NULL; parser_->host_var_count = 0; \
             parser_->set_host_var = 0; } } while (0)

#endif /* !SERVER_MODE */


/* NODE FUNCTION DECLARATIONS */

#define PT_NODE_INIT_OUTERLINK(n)                        \
    do {                                                 \
        if ((n)) {                                       \
            (n)->next          = NULL;                   \
            (n)->or_next       = NULL;                   \
            (n)->etc           = NULL;                   \
            (n)->alias_print   = NULL;                   \
        }                                                \
    } while (0)

#define PT_NODE_COPY_NUMBER_OUTERLINK(t, s)              \
    do {                                                 \
        if ((t) && (s)) {                                \
            (t)->line_number   = (s)->line_number;       \
            (t)->column_number = (s)->column_number;     \
            (t)->next          = (s)->next;              \
            (t)->or_next       = (s)->or_next;           \
            (t)->etc           = (s)->etc;               \
            (t)->alias_print   = (s)->alias_print;       \
        }                                                \
    } while (0)

#define PT_NODE_MOVE_NUMBER_OUTERLINK(t, s)              \
    do {                                                 \
        PT_NODE_COPY_NUMBER_OUTERLINK((t), (s));         \
        PT_NODE_INIT_OUTERLINK((s));                     \
    } while (0)

#define PT_NODE_PRINT_TO_ALIAS(p, n, c)                         \
    do {                                                        \
        unsigned int _save_custom;                              \
                                                                \
        if (!(p) || !(n) || (n->alias_print))                   \
          break;                                                \
        _save_custom = (p)->custom_print;                       \
        (p)->custom_print |= (c);                               \
	if (((p)->custom_print & PT_SHORT_PRINT) != 0)		\
	  {							\
	    (n)->alias_print = pt_short_print ((p), (n));	\
	  }							\
	else							\
	  {							\
	    (n)->alias_print = parser_print_tree ((p), (n));    \
	  }							\
        (p)->custom_print = _save_custom;                       \
    } while (0)

#define PT_NODE_PRINT_VALUE_TO_TEXT(p, n)			\
    do {							\
	if (!(p) || !(n) || (n)->node_type != PT_VALUE		\
	    || (n)->info.value.text)				\
	  {							\
	    break;						\
	  }							\
	(n)->info.value.text = parser_print_tree ((p), (n));	\
    } while (0)

#define CAST_POINTER_TO_NODE(p)                             \
    do {                                                    \
        while ((p) && (p)->node_type == PT_POINTER)       \
        {                                                   \
            (p) = (p)->info.pointer.node;                   \
        }                                                   \
    } while (0)

#define PT_EMPTY	INT_MAX

#if defined (ENABLE_UNUSED_FUNCTION)
#define MAX_NUM_PLAN_TRACE        100
#endif

/*
 Enumerated types of parse tree statements
  WARNING ------ WARNING ----- WARNING
 Member functions parser_new_node, parser_init_node, parser_print_tree, which take a node as an argument
 are accessed by function tables indexed by node_type. The functions in
 the tables must appear in EXACTLY the same order as the node types
 defined below. If you add a new node type you must create the
 functions to manipulate it and put these in the tables. Else crash and burn.
*/

/* enumeration for parser_walk_tree() */
enum
{
  PT_STOP_WALK = 0,
  PT_CONTINUE_WALK,
  PT_LEAF_WALK,
  PT_LIST_WALK
};

enum pt_custom_print
{
  PT_SUPPRESS_RESOLVED = 0x2,
#if 0
  PT_SUPPRESS_META_ATTR_CLASS = 0x4,    /* unused */
#endif
  PT_SUPPRESS_INTO = 0x8,
#if 0
  PT_SUPPRESS_SELECTOR = 0x40,  /* unused */
#endif
  PT_SUPPRESS_SELECT_LIST = 0x80,
  PT_SUPPRESS_QUOTES = 0x200,
  PT_PRINT_ALIAS = 0x400,
  PT_PAD_BYTE = 0x8000,
  PT_CONVERT_RANGE = 0x10000,
  PT_PRINT_DB_VALUE = 0x40000,
  PT_SUPPRESS_INDEX = 0x80000,
  PT_SUPPRESS_ORDERING = 0x100000,
  PT_PRINT_QUOTES = 0x200000,
  PT_FORCE_ORIGINAL_TABLE_NAME = 0x400000,      /* this is for PT_NAME nodes.
                                                 * prints original table name
                                                 * instead of printing resolved
                                                 * NOTE: spec_id must point to
                                                 * original table
                                                 */
  PT_SUPPRESS_CHARSET_PRINT = 0x800000,
#if 0
  PT_PRINT_DIFFERENT_SYSTEM_PARAMETERS = 0x1000000,     /* print session
                                                         * parameters
                                                         */
#endif
  PT_SHORT_PRINT = 0x2000000,   /* PT_NODE_PRINT_TO_ALIAS
                                 * calls pt_short_print
                                 * instead pt_print_tree
                                 */
  PT_SUPPRESS_BIGINT_CAST = 0x4000000,
  PT_CHARSET_COLLATE_FULL = 0x8000000,
  PT_CHARSET_COLLATE_USER_ONLY = 0x10000000
};

/* all statement node types should be assigned their API statement enumeration */
typedef enum pt_node_type PT_NODE_TYPE;
enum pt_node_type
{
  PT_ALTER = RYE_STMT_ALTER_CLASS,
  PT_ALTER_INDEX = RYE_STMT_ALTER_INDEX,
  PT_ALTER_USER = RYE_STMT_ALTER_USER,
  PT_ALTER_SERIAL = RYE_STMT_ALTER_SERIAL,
  PT_COMMIT_WORK = RYE_STMT_COMMIT_WORK,
  PT_CREATE_ENTITY = RYE_STMT_CREATE_CLASS,
  PT_CREATE_INDEX = RYE_STMT_CREATE_INDEX,
  PT_CREATE_USER = RYE_STMT_CREATE_USER,
  PT_CREATE_SERIAL = RYE_STMT_CREATE_SERIAL,
  PT_DROP = RYE_STMT_DROP_CLASS,
  PT_DROP_INDEX = RYE_STMT_DROP_INDEX,
  PT_DROP_USER = RYE_STMT_DROP_USER,
  PT_DROP_SERIAL = RYE_STMT_DROP_SERIAL,
  PT_RENAME = RYE_STMT_RENAME_CLASS,
  PT_ROLLBACK_WORK = RYE_STMT_ROLLBACK_WORK,
  PT_GRANT = RYE_STMT_GRANT,
  PT_REVOKE = RYE_STMT_REVOKE,
  PT_UPDATE_STATS = RYE_STMT_UPDATE_STATS,
  PT_INSERT = RYE_STMT_INSERT,
  PT_SELECT = RYE_STMT_SELECT,
  PT_UPDATE = RYE_STMT_UPDATE,
  PT_DELETE = RYE_STMT_DELETE,
  PT_GET_XACTION = RYE_STMT_GET_ISO_LVL,
  /* should have separate pt node type for RYE_STMT_GET_TIMEOUT,
     It will also be tagged PT_GET_XACTION  */
  PT_GET_OPT_LVL = RYE_STMT_GET_OPT_LVL,
  PT_SET_OPT_LVL = RYE_STMT_SET_OPT_LVL,
  PT_SET_SYS_PARAMS = RYE_STMT_SET_SYS_PARAMS,
  PT_SAVEPOINT = RYE_STMT_SAVEPOINT,

  PT_DIFFERENCE = RYE_MAX_STMT_TYPE,    /* these enumerations must be
                                           distinct from statements */
  PT_INTERSECTION,              /* difference intersection and union are
                                   reported as RYE_STMT_SELECT. */
  PT_UNION,

  PT_ZZ_ERROR_MSG,
  PT_ATTR_DEF,
  PT_AUTH_CMD,
  PT_CONSTRAINT,
  PT_DATA_DEFAULT,
  PT_DATA_TYPE,
  PT_DOT_,
  PT_EXPR,
  PT_FUNCTION,
  PT_HOST_VAR,
  PT_ISOLATION_LVL,
  PT_NAME,
  PT_SORT_SPEC,
  PT_SPEC,
  PT_TIMEOUT,
  PT_VALUE,
  PT_POINTER,
  PT_NODE_LIST,
  PT_TABLE_OPTION,
  PT_ATTR_ORDERING,
  PT_QUERY_TRACE,
  PT_NODE_NUMBER,               /* This is the number of node types */
  PT_LAST_NODE_NUMBER = PT_NODE_NUMBER
};


/* Enumerated Data Types for expressions with a VALUE */
typedef enum pt_type_enum PT_TYPE_ENUM;
enum pt_type_enum
{
  PT_TYPE_NONE = 1000,          /* type not known yet */
  PT_TYPE_MIN = PT_TYPE_NONE,
  /* primitive types */
  PT_TYPE_INTEGER,
  PT_TYPE_DOUBLE,
  PT_TYPE_DATE,
  PT_TYPE_TIME,
  PT_TYPE_DATETIME,
  PT_TYPE_NUMERIC,
  PT_TYPE_VARCHAR,
  PT_TYPE_VARBIT,
  PT_TYPE_LOGICAL,
  PT_TYPE_MAYBE,

  /* special values */
  PT_TYPE_NULL,                 /* in assignment and defaults */
  PT_TYPE_STAR,                 /* select (*), count (*),   will be expanded later */

  /* non primitive types */
  PT_TYPE_OBJECT,
  PT_TYPE_SEQUENCE,
  PT_TYPE_COMPOUND,

  PT_TYPE_EXPR_SET,             /* type of parentheses expr set, avail for parser only */
  PT_TYPE_RESULTSET,

  PT_TYPE_BIGINT,

  PT_TYPE_MAX
};

/* Enumerated priviledges for Grant, Revoke */
typedef enum
{
  PT_NO_PRIV = 2000,            /* this value to initialize the node */
  PT_ADD_PRIV,
  PT_ALL_PRIV,
  PT_ALTER_PRIV,
  PT_DELETE_PRIV,
  PT_DROP_PRIV,
  /* PT_EXECUTE_PRIV, unused */
  /* PT_GRANT_OPTION_PRIV,   avail for revoke only */
  /* PT_INDEX_PRIV, unused */
  PT_INSERT_PRIV,
  PT_REFERENCES_PRIV,           /* for ANSI compatibility */
  PT_SELECT_PRIV,
  PT_UPDATE_PRIV
} PT_PRIV_TYPE;

/* Enumerated Misc Types */
typedef enum
{
  PT_MISC_DUMMY = 3000,
  PT_ALL,
#if 0                           /* unused */
  PT_ONLY,
#endif
  PT_DISTINCT,
  PT_DEFAULT,
  PT_ASC,
  PT_DESC,
  PT_GRANT_OPTION,
  PT_NO_GRANT_OPTION,
  PT_CLASS,
  PT_VCLASS,
  PT_OID_ATTR,
  PT_NORMAL,
  PT_HINT_NAME,                 /* hint argument name */
  PT_INDEX_NAME,
  PT_IS_SUBQUERY,               /* query is sub-query, not directly producing result */
  PT_IS_UNION_SUBQUERY,         /* in a union subquery */
  PT_IS_UNION_QUERY,            /* query directly producing result in top level union */
  PT_IS_VALUE,                  /* used by value clause of insert */
  PT_IS_DEFAULT_VALUE,
  PT_ATTRIBUTE,
  PT_NO_ISOLATION_LEVEL,        /* value for uninitialized isolation level */
  PT_SERIALIZABLE,
  PT_REPEATABLE_READ,
  PT_READ_COMMITTED,
  PT_READ_UNCOMMITTED,
  PT_ISOLATION_LEVEL,           /* get transaction option */
  PT_LOCK_TIMEOUT,
  PT_HOST_IN,                   /* kind of host variable */
  PT_INVALIDATE_XACTION,
  PT_PRINT,
  PT_EXPRESSION,

  PT_CHAR_STRING,               /* denotes the flavor of a literal string */
  PT_BIT_STRING,
  PT_HEX_STRING,

  PT_MATCH_REGULAR,
  PT_MATCH_PARTIAL,             /* referential integrity constraints       */

  PT_LEADING,                   /* trim operation qualifiers */
  PT_TRAILING,
  PT_BOTH,
  PT_NOPUT,
  PT_INPUT,
  PT_OUTPUT,
  PT_INPUTOUTPUT,

  PT_MILLISECOND,               /* datetime components for extract operation */
  PT_SECOND,
  PT_MINUTE,
  PT_HOUR,
  PT_DAY,
  PT_WEEK,
  PT_MONTH,
  PT_QUARTER,
  PT_YEAR,
  /* mysql units types */
  PT_SECOND_MILLISECOND,
  PT_MINUTE_MILLISECOND,
  PT_MINUTE_SECOND,
  PT_HOUR_MILLISECOND,
  PT_HOUR_SECOND,
  PT_HOUR_MINUTE,
  PT_DAY_MILLISECOND,
  PT_DAY_SECOND,
  PT_DAY_MINUTE,
  PT_DAY_HOUR,
  PT_YEAR_MONTH,

  PT_SIMPLE_CASE,
  PT_SEARCHED_CASE,

  PT_OPT_LVL,                   /* Variants of "get/set optimization" statement */
  PT_OPT_COST,

  PT_SUBSTR_ORG,
  PT_SUBSTR,                    /* substring qualifier */

  PT_EQ_TORDER,

  PT_SP_IN,
  PT_SP_OUT,
  PT_SP_INOUT,

  PT_NULLS_DEFAULT,
  PT_NULLS_FIRST,
  PT_NULLS_LAST,

  PT_PERSIST_ON,
  PT_PERSIST_OFF,

  PT_TRACE_ON,
  PT_TRACE_OFF,
  PT_TRACE_FORMAT_TEXT,
  PT_TRACE_FORMAT_JSON
} PT_MISC_TYPE;

/* Enumerated join type */
typedef enum
{
  PT_JOIN_NONE = 0x00,          /* 0000 0000 */
  PT_JOIN_CROSS = 0x01,         /* 0000 0001 */
  PT_JOIN_NATURAL = 0x02,       /* 0000 0010 -- not used */
  PT_JOIN_INNER = 0x04,         /* 0000 0100 */
  PT_JOIN_LEFT_OUTER = 0x08,    /* 0000 1000 */
  PT_JOIN_RIGHT_OUTER = 0x10,   /* 0001 0000 */
  PT_JOIN_FULL_OUTER = 0x20,    /* 0010 0000 -- not used */
  PT_JOIN_UNION = 0x40          /* 0100 0000 -- not used */
} PT_JOIN_TYPE;

typedef enum
{
  PT_HINT_NONE = 0x00,          /* 0000 0000 *//* no hint */
  PT_HINT_ORDERED = 0x01,       /* 0000 0001 *//* force join left-to-right */
#if 0
  PT_HINT_RESERVED_01 = 0x02,   /* 0000 0010 -- not used */
  PT_HINT_RESERVED_02 = 0x04,   /* 0000 0100 -- not used */
  PT_HINT_RESERVED_03 = 0x08,   /* 0000 1000 -- not used */
#endif
  PT_HINT_USE_NL = 0x10,        /* 0001 0000 *//* force nl-join */
  PT_HINT_USE_IDX = 0x20,       /* 0010 0000 *//* force idx-join */
#if 0
  PT_HINT_RESERVED_04 = 0x40,   /* 0100 0000 -- not used */
  PT_HINT_RESERVED_05 = 0x80,   /* 1000 0000 -- not used */
#endif
  PT_HINT_RECOMPILE = 0x0100,   /* 0000 0001 0000 0000 *//* recompile */
#if 0
  PT_HINT_LK_TIMEOUT = 0x0200,  /* 0000 0010 0000 0000 -- not used */
  PT_HINT_NO_LOGGING = 0x0400,  /* 0000 0100 0000 0000 -- not used */
  PT_HINT_RESERVED_06 = 0x0800, /* 0000 1000 0000 0000 -- not used */
#endif
  PT_HINT_QUERY_CACHE = 0x1000, /* 0001 0000 0000 0000 *//* query_cache */
  PT_HINT_REEXECUTE = 0x2000,   /* 0010 0000 0000 0000 *//* reexecute */
#if 0                           /* not used */
  PT_HINT_JDBC_CACHE = 0x4000,  /* 0100 0000 0000 0000 *//* jdbc_cache */
  PT_HINT_NO_STATS = 0x8000,    /* 1000 0000 0000 0000 *//* no_stats */
#endif
  PT_HINT_USE_IDX_DESC = 0x10000,       /* 0001 0000 0000 0000 0000 *//* descending index scan */
  PT_HINT_NO_COVERING_IDX = 0x20000,    /* 0010 0000 0000 0000 0000 *//* do not use covering index scan */
  PT_HINT_FORCE_PAGE_ALLOCATION = 0x40000,      /* 0100 0000 0000 0000 0000 -- force new page allocation for insert */
  PT_HINT_NO_IDX_DESC = 0x80000,        /* 1000 0000 0000 0000 0000 *//* do not use descending index scan */
  PT_HINT_NO_MULTI_RANGE_OPT = 0x100000,        /* 0001 0000 0000 0000 0000 0000 */
  /* do not use multi range optimization */
#if 0
  PT_HINT_USE_UPDATE_IDX = 0x200000,    /* 0010 0000 0000 0000 0000 0000 -- not used */
  /* use index for merge update */
  PT_HINT_USE_INSERT_IDX = 0x400000,    /* 0100 0000 0000 0000 0000 0000 -- not used */
  /* do not generate SORT-LIMIT plan */
#endif
  PT_HINT_NO_SORT_LIMIT = 0x800000      /* 1000 0000 0000 0000 0000 0000 */
#if 0
    PT_HINT_RESERVED_08 = 0x1000000,    /* 0001 0000 0000 0000 0000 0000 0000 -- not used */
  PT_HINT_RESERVED_09 = 0x2000000,      /* 0010 0000 0000 0000 0000 0000 0000 -- not used */
  PT_HINT_RESERVED_10 = 0x4000000       /* 0100 0000 0000 0000 0000 0000 0000 -- not used */
#endif
} PT_HINT_ENUM;


/* Codes for error messages */

typedef enum
{
  PT_NO_ERROR = 4000,
  PT_USAGE,
  PT_NODE_TABLE_OVERFLOW,
  PT_NAMES_TABLE_OVERFLOW,
  PT_CANT_OPEN_FILE,
  PT_STACK_OVERFLOW,
  PT_STACK_UNDERFLOW,
  PT_PARSE_ERROR,
  PT_ILLEGAL_TYPE_IN_FUNC,
  PT_NO_ARG_IN_FUNC
} PT_ERROR_CODE;

/* Codes for alter/add */

typedef enum
{
  PT_ADD_ATTR_MTHD = 5000,
  PT_DROP_ATTR_MTHD,
  PT_RENAME_ATTR_MTHD,
  PT_DROP_CONSTRAINT,
  PT_RENAME_ENTITY,
  PT_DROP_INDEX_CLAUSE,
  PT_CHANGE_ATTR,
  PT_CHANGE_OWNER,
  PT_CHANGE_PK
} PT_ALTER_CODE;

/* Codes for constraint types */

typedef enum
{
  PT_CONSTRAIN_UNKNOWN = 7000,
  PT_CONSTRAIN_PRIMARY_KEY,
  PT_CONSTRAIN_NULL,
  PT_CONSTRAIN_NOT_NULL,
  PT_CONSTRAIN_UNIQUE,
  PT_CONSTRAIN_INDEX,
  PT_CONSTRAIN_CHECK
} PT_CONSTRAINT_TYPE;


typedef enum
{
  PT_TABLE_OPTION_SHARD_KEY = 9000
} PT_TABLE_OPTION_TYPE;


typedef enum
{
  PT_AND = 400, PT_OR, PT_NOT,
  PT_BETWEEN, PT_NOT_BETWEEN,
  PT_LIKE, PT_NOT_LIKE,
  PT_IS_IN, PT_IS_NOT_IN,
  PT_IS_NULL, PT_IS_NOT_NULL,
  PT_IS, PT_IS_NOT,
  PT_EXISTS,
  PT_EQ, PT_NE, PT_GE, PT_GT, PT_LT, PT_LE, PT_NULLSAFE_EQ,
  PT_GT_INF, PT_LT_INF,         /* internal use only */
  PT_PLUS, PT_MINUS,
  PT_TIMES, PT_DIVIDE, PT_UNARY_MINUS,
  PT_BIT_NOT, PT_BIT_XOR, PT_BIT_AND, PT_BIT_OR, PT_BIT_COUNT,
  PT_BITSHIFT_LEFT, PT_BITSHIFT_RIGHT, PT_DIV, PT_MOD,
  PT_IF, PT_IFNULL, PT_ISNULL, PT_XOR,
  PT_ASSIGN,                    /* as in x=y */
  PT_BETWEEN_AND,
  PT_BETWEEN_GE_LE, PT_BETWEEN_GE_LT, PT_BETWEEN_GT_LE, PT_BETWEEN_GT_LT,
  PT_BETWEEN_EQ_NA,
  PT_BETWEEN_INF_LE, PT_BETWEEN_INF_LT, PT_BETWEEN_GE_INF, PT_BETWEEN_GT_INF,
  PT_RANGE,                     /* internal use only */
  PT_MODULUS, PT_RAND, PT_DRAND,
  PT_POSITION, PT_SUBSTRING, PT_OCTET_LENGTH, PT_BIT_LENGTH,
  PT_SUBSTRING_INDEX, PT_MD5, PT_SPACE,
  PT_CHAR_LENGTH, PT_LOWER, PT_UPPER, PT_TRIM,
  PT_LTRIM, PT_RTRIM, PT_LPAD, PT_RPAD, PT_REPLACE, PT_TRANSLATE,
  PT_REPEAT,
  PT_LAST_DAY, PT_MONTHS_BETWEEN, PT_SYS_DATE,
  PT_TO_CHAR, PT_TO_DATE, PT_TO_NUMBER,
  PT_SYS_TIME, PT_SYS_DATETIME, PT_UTC_TIME, PT_UTC_DATE,
  PT_TO_TIME, PT_TO_DATETIME,
  PT_INST_NUM, PT_ROWNUM, PT_ORDERBY_NUM,
  PT_EXTRACT,
  PT_LIKE_ESCAPE,
  PT_CAST,
  PT_CASE,
  PT_CURRENT_USER,
  PT_HA_STATUS,
  PT_SHARD_GROUPID, PT_SHARD_LOCKNAME, PT_SHARD_NODEID,
  PT_EXPLAIN,

  PT_FLOOR, PT_CEIL, PT_SIGN, PT_POWER, PT_ROUND, PT_ABS, PT_TRUNC,
  PT_CHR, PT_INSTR, PT_LEAST, PT_GREATEST,

  PT_STRCAT, PT_NULLIF, PT_COALESCE, PT_NVL, PT_NVL2, PT_DECODE,
  PT_RANDOM, PT_DRANDOM,

  PT_LOG, PT_EXP, PT_SQRT,

  PT_CONCAT, PT_CONCAT_WS, PT_FIELD, PT_LEFT, PT_RIGHT,
  PT_LOCATE, PT_MID, PT_STRCMP, PT_REVERSE,

  PT_ACOS, PT_ASIN, PT_ATAN, PT_ATAN2,
  PT_COS, PT_SIN, PT_COT, PT_TAN,
  PT_DEGREES, PT_RADIANS,
  PT_PI,
  PT_FORMAT,
  PT_LN, PT_LOG2, PT_LOG10,
  PT_TIME_FORMAT,
  PT_UNIX_TIMESTAMP,
  PT_FROM_UNIXTIME,
  PT_SCHEMA,
  PT_DATABASE,
  PT_VERSION,
  /* datetime */
  PT_ADDDATE,                   /* 2 fixed parameter */
  PT_DATE_ADD,                  /* INTERVAL variant */
  PT_SUBDATE,
  PT_DATE_SUB,
  PT_DATE_FORMAT,
  PT_DATEF,
  PT_TIMEF, PT_YEARF, PT_MONTHF, PT_DAYF,
  PT_HOURF, PT_MINUTEF, PT_SECONDF,
  PT_DAYOFMONTH,
  PT_WEEKDAY,
  PT_DAYOFWEEK,
  PT_DAYOFYEAR,
  PT_QUARTERF,
  PT_TODAYS,
  PT_FROMDAYS,
  PT_TIMETOSEC,
  PT_SECTOTIME,
  PT_MAKEDATE,
  PT_MAKETIME,
  PT_WEEKF,
  PT_USER,
  PT_DATEDIFF,
  PT_TIMEDIFF,
  PT_STR_TO_DATE,
  PT_DEFAULTF,
  PT_LIST_DBS,
  PT_LIKE_LOWER_BOUND, PT_LIKE_UPPER_BOUND,

  PT_TYPEOF,
  PT_FUNCTION_HOLDER,           /* special operator : wrapper for PT_FUNCTION node */
  PT_INDEX_CARDINALITY,
  PT_BIN,
  PT_FINDINSET,

  PT_HEX,
  PT_ASCII,
  PT_CONV,

  /* rlike operator */
  PT_RLIKE, PT_NOT_RLIKE, PT_RLIKE_BINARY, PT_NOT_RLIKE_BINARY,

  /* inet */
  PT_INET_ATON, PT_INET_NTOA,

  /* width_bucket */
  PT_WIDTH_BUCKET,

  PT_TRACE_STATS,
  PT_INDEX_PREFIX,

  PT_SHA_ONE,
  PT_SHA_TWO,
  PT_TO_BASE64,
  PT_FROM_BASE64,

  /* This is the last entry. Please add a new one before it. */
  PT_LAST_OPCODE
} PT_OP_TYPE;

typedef enum
{
#if 0
  PT_RANGE_MERGE,
  PT_RANGE_INTERSECTION,
#endif
  PT_REDUCE_COMP_PAIR_TERMS
} PT_COMP_TO_BETWEEN_OP_CODE_TYPE;

typedef enum
{
  PT_SYNTAX,
  PT_SEMANTIC,
  PT_EXECUTION
} PT_ERROR_TYPE;

typedef enum
{
  EXCLUDE_HIDDEN_COLUMNS,
  INCLUDE_HIDDEN_COLUMNS
} PT_INCLUDE_OR_EXCLUDE_HIDDEN_COLUMNS;

/* Flags for spec structures */
typedef enum
{
  PT_SPEC_FLAG_NONE = 0x0,      /* the spec will not be altered */
  PT_SPEC_FLAG_UPDATE = 0x01,   /* the spec will be updated */
  PT_SPEC_FLAG_DELETE = 0x02,   /* the spec will be deleted */
  PT_SPEC_FLAG_HAS_UNIQUE = 0x04,       /* the spec has unique */
  PT_SPEC_FLAG_FOR_UPDATE_CLAUSE = 0x08 /* Used with FOR UPDATE clause */
} PT_SPEC_FLAG;

/*
 * Type definitions
 */

typedef struct parser_varchar PARSER_VARCHAR;

//typedef struct parser_context PARSER_CONTEXT;

typedef struct parser_node PT_NODE;
typedef struct pt_alter_info PT_ALTER_INFO;
typedef struct pt_alter_user_info PT_ALTER_USER_INFO;
typedef struct pt_attr_def_info PT_ATTR_DEF_INFO;
typedef struct pt_attr_ordering_info PT_ATTR_ORDERING_INFO;
typedef struct pt_auth_cmd_info PT_AUTH_CMD_INFO;
typedef struct pt_create_entity_info PT_CREATE_ENTITY_INFO;
typedef struct pt_index_info PT_INDEX_INFO;
typedef struct pt_create_user_info PT_CREATE_USER_INFO;
typedef struct pt_data_default_info PT_DATA_DEFAULT_INFO;
typedef struct pt_data_type_info PT_DATA_TYPE_INFO;
typedef struct pt_delete_info PT_DELETE_INFO;
typedef struct pt_dot_info PT_DOT_INFO;
typedef struct pt_drop_info PT_DROP_INFO;
typedef struct pt_drop_user_info PT_DROP_USER_INFO;
typedef struct pt_spec_info PT_SPEC_INFO;
typedef struct pt_expr_info PT_EXPR_INFO;
typedef struct pt_file_path_info PT_FILE_PATH_INFO;
typedef struct pt_function_info PT_FUNCTION_INFO;
typedef struct pt_get_opt_lvl_info PT_GET_OPT_LVL_INFO;
typedef struct pt_get_xaction_info PT_GET_XACTION_INFO;
typedef struct pt_grant_info PT_GRANT_INFO;
typedef struct pt_host_var_info PT_HOST_VAR_INFO;
typedef struct pt_insert_info PT_INSERT_INFO;
typedef struct pt_isolation_lvl_info PT_ISOLATION_LVL_INFO;
typedef struct pt_name_info PT_NAME_INFO;
typedef struct pt_rename_info PT_RENAME_INFO;
typedef struct pt_revoke_info PT_REVOKE_INFO;
typedef struct pt_rollback_work_info PT_ROLLBACK_WORK_INFO;
typedef struct pt_union_info PT_UNION_INFO;
typedef struct pt_savepoint_info PT_SAVEPOINT_INFO;
typedef struct pt_select_info PT_SELECT_INFO;
typedef struct pt_query_info PT_QUERY_INFO;
typedef struct pt_set_opt_lvl_info PT_SET_OPT_LVL_INFO;
typedef struct pt_set_sys_params_info PT_SET_SYS_PARAMS_INFO;
typedef struct pt_sort_spec_info PT_SORT_SPEC_INFO;
typedef struct pt_timeout_info PT_TIMEOUT_INFO;
typedef struct pt_update_info PT_UPDATE_INFO;
typedef struct pt_update_stats_info PT_UPDATE_STATS_INFO;
typedef union pt_data_value PT_DATA_VALUE;
typedef struct pt_value_info PT_VALUE_INFO;
typedef struct PT_ZZ_ERROR_MSG_INFO PT_ZZ_ERROR_MSG_INFO;
typedef struct pt_constraint_info PT_CONSTRAINT_INFO;
typedef struct pt_pointer_info PT_POINTER_INFO;
typedef union pt_statement_info PT_STATEMENT_INFO;
typedef struct pt_node_list_info PT_NODE_LIST_INFO;
typedef struct pt_table_option_info PT_TABLE_OPTION_INFO;

typedef struct pt_agg_check_info PT_AGG_CHECK_INFO;
typedef struct pt_agg_rewrite_info PT_AGG_REWRITE_INFO;
typedef struct pt_agg_find_info PT_AGG_FIND_INFO;
typedef struct pt_agg_name_info PT_AGG_NAME_INFO;
typedef struct pt_non_groupby_col_info PT_NON_GROUPBY_COL_INFO;

typedef struct pt_host_vars PT_HOST_VARS;
typedef struct cursor_id PT_CURSOR_ID;
typedef struct qfile_list_id PT_LIST_ID;
typedef struct view_cache_info VIEW_CACHE_INFO;
typedef struct semantic_chk_info SEMANTIC_CHK_INFO;
typedef struct parser_hint PT_HINT;
typedef struct pt_trace_info PT_TRACE_INFO;

typedef PT_NODE *(*PT_NODE_FUNCTION) (PARSER_CONTEXT * p, PT_NODE * tree, void *arg);

typedef PT_NODE *(*PT_NODE_WALK_FUNCTION) (PARSER_CONTEXT * p, PT_NODE * tree, void *arg, int *continue_walk);

typedef void (*PT_NODE_APPLY_FUNCTION) (PARSER_CONTEXT * p, PT_NODE * tree, PT_NODE_FUNCTION f, void *arg);

typedef PARSER_VARCHAR *(*PT_PRINT_VALUE_FUNC) (PARSER_CONTEXT * parser, const PT_NODE * val);
typedef PT_NODE *(*PARSER_INIT_NODE_FUNC) (PT_NODE *);
typedef PARSER_VARCHAR *(*PARSER_PRINT_NODE_FUNC) (PARSER_CONTEXT * parser, PT_NODE * node);
typedef PT_NODE *(*PARSER_APPLY_NODE_FUNC) (PARSER_CONTEXT * parser, PT_NODE * p, PT_NODE_FUNCTION g, void *arg);

extern PARSER_INIT_NODE_FUNC *pt_init_f;
extern PARSER_PRINT_NODE_FUNC *pt_print_f;
extern PARSER_APPLY_NODE_FUNC *pt_apply_f;

/* This is for loose reference to init node function vector */
typedef void (*PARSER_GENERIC_VOID_FUNCTION) ();

struct semantic_chk_info
{
  PT_NODE *top_node;            /* top_node_arg  */
  bool system_class;            /* system class(es) is(are) referenced */
};

struct view_cache_info
{
  PT_NODE *attrs;
  PT_NODE *vquery_for_query;
  DB_AUTH vquery_authorization;
};

struct parser_hint
{
  const char *tokens;
  PT_NODE *arg_list;
  PT_HINT_ENUM hint;
};



struct pt_alter_info
{
  PT_NODE *entity_name;         /* PT_NAME */
  PT_ALTER_CODE code;           /* value will be PT_ADD_ATTR, PT_DROP_ATTR ... */
  PT_MISC_TYPE entity_type;     /* PT_VCLASS, ... */
  union
  {
    struct
    {
      PT_NODE *attr_def_list;   /* PT_ATTR_DEF */
      PT_NODE *attr_old_name;   /* PT_NAME used for CHANGE <old> <new> */
      PT_NODE *attr_mthd_name_list;     /* PT_NAME(list) */
      PT_MISC_TYPE attr_type;   /* PT_NORMAL_ATTR, PT_CLASS_ATTR */
    } attr_mthd;
    struct
    {
      PT_MISC_TYPE element_type;        /* PT_ATTRIBUTE */
      PT_MISC_TYPE meta;        /* PT_META, PT_NORMAL */
      PT_NODE *old_name;
      PT_NODE *new_name;
    } rename;
    struct
    {
      bool unique;
    } index;
    struct
    {
      PT_NODE *user_name;       /* user name for PT_CHANGE_OWNER */
    } user;
    struct
    {
      PT_NODE *index_name;
    } chg_pk;
  } alter_clause;
  PT_NODE *constraint_list;     /* constraints from ADD and CHANGE clauses */
  PT_HINT_ENUM hint;
};

/* ALTER USER INFO */
struct pt_alter_user_info
{
  PT_NODE *user_name;           /* PT_NAME */
  PT_NODE *password;            /* PT_VALUE (string) */
  bool is_encrypted;            /* password is encrypted ? */
};

/* Info for ATTR_DEF */
struct pt_attr_def_info
{
  PT_NODE *attr_name;           /* PT_NAME */
  PT_NODE *data_default;        /* PT_DATA_DEFAULT */
  PT_NODE *ordering_info;       /* PT_ATTR_ORDERING */
  PT_MISC_TYPE attr_type;       /* PT_NORMAL or PT_META */
  int size_constraint;          /* max length of STRING */
  short constrain_not_null;
};

/* Info for ALTER TABLE ADD COLUMN [FIRST | AFTER column_name ] */
struct pt_attr_ordering_info
{
  PT_NODE *after;               /* PT_NAME */
  bool first;
};

/* Info for AUTH_CMD */
struct pt_auth_cmd_info
{
  PT_NODE *attr_mthd_list;      /* PT_NAME (list of attr names)  */
  PT_PRIV_TYPE auth_cmd;        /* enum PT_SELECT_PRIV, PT_ALL_PRIV,... */
};

/* Info for a CREATE ENTITY node */
struct pt_create_entity_info
{
  PT_MISC_TYPE entity_type;     /* enum PT_CLASS, PT_VCLASS .. */
  PT_NODE *entity_name;         /* PT_NAME */
  PT_NODE *attr_def_list;       /* PT_ATTR_DEF (list) */
  PT_NODE *table_option_list;   /* PT_TABLE_OPTION (list) */
  PT_NODE *as_query_list;       /* PT_SELECT (list) */
  PT_NODE *object_id_list;      /* PT_NAME list */
  PT_NODE *update;              /* PT_EXPR (list ) */
  PT_NODE *constraint_list;     /* PT_CONSTRAINT (list) */
  PT_NODE *internal_stmts;      /* internally created statements to handle TEXT */
  PT_NODE *create_like;         /* PT_NAME */
  unsigned or_replace:1;        /* OR REPLACE clause for create view */
  unsigned is_shard:1;          /* create SHARD table */
};

/* CREATE/DROP INDEX INFO */
struct pt_index_info
{
  PT_NODE *indexed_class;       /* PT_SPEC */
  PT_NODE *column_names;        /* PT_SORT_SPEC (list) */
  PT_NODE *index_name;          /* PT_NAME */
  bool unique;                  /* UNIQUE specified? */
};

/* CREATE USER INFO */
struct pt_create_user_info
{
  PT_NODE *user_name;           /* PT_NAME */
  PT_NODE *password;            /* PT_VALUE (string) */
  bool is_encrypted;            /* password is encrypted ? */
};

/* Info for DATA_DEFAULT */
struct pt_data_default_info
{
  PT_NODE *default_value;       /*  PT_VALUE (list)   */
  DB_DEFAULT_EXPR_TYPE default_expr;    /* if it is a pseudocolumn,
                                         * do not evaluate expr */
};

/* Info for DATA_TYPE node */
struct pt_data_type_info
{
  PT_NODE *entity;              /* class PT_NAME list for PT_TYPE_OBJECT */
  PT_NODE *virt_data_type;      /* for non-primitive types- sets, etc. */
  PT_TYPE_ENUM virt_type_enum;  /* type enumeration tage PT_TYPE_??? */
  int precision;                /* for float and int, length of char */
  int dec_scale;                /* decimal scale */
  int collation_id;             /* collation identifier (strings) */
  bool has_coll_spec;           /* this is used only when defining collatable
                                 * types: true if collation was explicitly
                                 * set, false otherwise (collation defaulted
                                 * to that of the system) */
};


/* DELETE */
struct pt_delete_info
{
  PT_NODE *target_classes;      /* PT_NAME */
  PT_NODE *spec;                /* PT_SPEC (list) */
  PT_NODE *search_cond;         /* PT_EXPR */
  PT_NODE *using_index;         /* PT_NAME (list) */
  PT_NODE *internal_stmts;      /* internally created statements to handle TEXT */
#if 0                           /* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
  PT_NODE *ordered_hint;        /* ORDERED_HINT hint's arguments (PT_NAME list) */
#endif
  PT_NODE *use_nl_hint;         /* USE_NL hint's arguments (PT_NAME list) */
  PT_NODE *use_idx_hint;        /* USE_IDX hint's arguments (PT_NAME list) */
  PT_NODE *limit;               /* PT_VALUE limit clause parameter */
  PT_HINT_ENUM hint;            /* hint flag */
  unsigned rewrite_limit:1;     /* need to rewrite the limit clause */
};

/* DOT_INFO*/
struct pt_dot_info
{
  PT_NODE *arg1;                /* PT_EXPR etc.  first argument */
  PT_NODE *arg2;                /* PT_EXPR etc.  possible second argument */
  PT_OP_TYPE op;                /* binary or unary op code */
};

/* DROP ENTITY
  as in DROP VCLASS X,Y,Z; (different from ALTER .... or DROP VIEW )
 */
struct pt_drop_info
{
  PT_NODE *spec_list;           /* PT_SPEC (list) */
  PT_NODE *internal_stmts;      /* internally created statements to handle TEXT */
  PT_MISC_TYPE entity_type;     /* PT_VCLASS, PT_CLASS   */
  bool if_exists;               /* IF EXISTS clause for DROP TABLE */
};

/* DROP USER INFO */
struct pt_drop_user_info
{
  PT_NODE *user_name;           /* PT_NAME */
};

/* Info for a ENTITY spec and spec_list */
struct pt_spec_info
{
  PT_NODE *entity_name;         /* PT_NAME */
  PT_NODE *derived_table;       /* a subquery */
  PT_NODE *range_var;           /* PT_NAME */
  PT_NODE *as_attr_list;        /* PT_NAME */
  PT_NODE *referenced_attrs;    /* PT_NAME list of referenced attrs */
  PT_NODE *flat_entity_list;    /* PT_NAME (list) resolved class's */
  UINTPTR id;                   /* entity spec unique id # */
  PT_MISC_TYPE meta_class;      /* enum 0 or PT_META  */
  PT_MISC_TYPE derived_table_type;      /* PT_IS_SUBQUERY */
  PT_NODE *on_cond;
  PT_NODE *using_cond;          /* -- does not support named columns join */
  PT_JOIN_TYPE join_type;
  short location;               /* n-th position in FROM (start from 0); init val = -1 */
  bool natural;                 /* -- does not support natural join */
  DB_AUTH auth_bypass_mask;     /* flag to bypass normal authorization :
                                 * used only by SHOW statements currently */
  PT_SPEC_FLAG flag;            /* flag wich marks this spec for DELETE or
                                 * UPDATE operations */
};

/* Info for Expressions
   This includes binary and unary operations + * - etc
 */
struct pt_expr_info
{
  PT_NODE *arg1;                /* PT_EXPR etc.  first argument */
  PT_NODE *arg2;                /* PT_EXPR etc.  possible second argument */
  PT_NODE *value;               /* only set if we evaluate it */
  PT_OP_TYPE op;                /* binary or unary op code */
  int paren_type;               /* 0 - none, else - ()  */
  PT_NODE *arg3;                /* possible third argument (like, between, or case) */
  PT_NODE *cast_type;           /* PT_DATA_TYPE, resultant cast domain */
  PT_MISC_TYPE qualifier;       /* trim qualifier (LEADING, TRAILING, BOTH),
                                 * datetime extract field specifier (YEAR,
                                 * ..., SECOND), or case expr type specifier
                                 * (NULLIF, COALESCE, SIMPLE_CASE, SEARCHED_CASE)
                                 */
#define PT_EXPR_INFO_CNF_DONE       1   /* CNF conversion has done? */
#if 0                           /* unused */
#define PT_EXPR_INFO_EMPTY_RANGE    2   /* empty RANGE spec? */
#endif
#define PT_EXPR_INFO_INSTNUM_C      4   /* compatible with inst_num() */
#define PT_EXPR_INFO_INSTNUM_NC     8   /* not compatible with inst_num() */
#define PT_EXPR_INFO_GROUPBYNUM_C  16   /* compatible with groupby_num() */
#define PT_EXPR_INFO_GROUPBYNUM_NC 32   /* not compatible with groupby_num() */
#define PT_EXPR_INFO_ORDERBYNUM_C \
               PT_EXPR_INFO_INSTNUM_C   /* compatible with orderby_num() */
#define PT_EXPR_INFO_ORDERBYNUM_NC \
              PT_EXPR_INFO_INSTNUM_NC   /* not compatible with orderby_num() */
#define PT_EXPR_INFO_TRANSITIVE    64   /* always true transitive join term ? */
#define PT_EXPR_INFO_LEFT_OUTER   128   /* Oracle's left outer join operator */
#define PT_EXPR_INFO_RIGHT_OUTER  256   /* Oracle's right outer join operator */
#define PT_EXPR_INFO_COPYPUSH     512   /* term which is copy-pushed into
                                         * the derived subquery ?
                                         * is removed at the last rewrite stage
                                         * of query optimizer */
#define PT_EXPR_INFO_SHARD_KEY	1024    /* is shard key column expr ? */
#if 0                           /* unused */
#define	PT_EXPR_INFO_CAST_NOFAIL 1024   /* flag for non failing cast operation;
                                         * at runtime will return null DB_VALUE
                                         * instead of failing */
#define PT_EXPR_INFO_CAST_SHOULD_FOLD 2048      /* flag which controls if a cast
                                                   expr should be folded */
#endif
#define PT_EXPR_INFO_GROUPBYNUM_LIMIT 4096      /* flag that marks if the
                                                 * expression resulted from a
                                                 * GROUP BY ... LIMIT statement */
  int flag;                     /* flags */
#define PT_EXPR_INFO_IS_FLAGED(e, f)    ((e)->info.expr.flag & (int) (f))
#define PT_EXPR_INFO_SET_FLAG(e, f)     (e)->info.expr.flag |= (int) (f)
#define PT_EXPR_INFO_CLEAR_FLAG(e, f)   (e)->info.expr.flag &= (int) ~(f)

  short continued_case;         /* 0 - false, 1 - true */
  short location;               /* 0 : WHERE; n : join condition of n-th FROM */
  PT_TYPE_ENUM recursive_type;  /* common type for recursive expression
                                 * arguments (like PT_GREATEST, PT_LEAST,...) */
};


/* FILE PATH INFO */
struct pt_file_path_info
{
  PT_NODE *string;              /* PT_VALUE: a C or ANSI string */
};

/* FUNCTIONS ( COUNT, AVG, ....)  */
struct pt_function_info
{
  PT_NODE *arg_list;            /* PT_EXPR(list) */
  FUNC_TYPE function_type;      /* PT_COUNT, PT_AVG, ... */
  PT_MISC_TYPE all_or_distinct; /* will be PT_ALL or PT_DISTINCT */
  const char *generic_name;     /* only for type PT_GENERIC */
  PT_NODE *order_by;            /* ordering PT_SORT_SPEC for GROUP_CONCAT */
};

/* Info for Get Optimization Level statement */
struct pt_get_opt_lvl_info
{
  PT_NODE *args;
  PT_MISC_TYPE option;          /* PT_OPT_LVL, PT_OPT_COST */
};

/* Info for Get Transaction statement */
struct pt_get_xaction_info
{
  PT_MISC_TYPE option;          /* PT_ISOLATION_LEVEL or PT_LOCK_TIMEOUT */
};

/* GRANT INFO */
struct pt_grant_info
{
  PT_NODE *auth_cmd_list;       /* PT_AUTH_CMD(list) */
  PT_NODE *user_list;           /* PT_NAME  */
  PT_NODE *spec_list;           /* PT_SPEC */
  PT_MISC_TYPE grant_option;    /* = PT_GRANT_OPTION or PT_NO_GRANT_OPTION */
};

/* Info for Host_Var */
struct pt_host_var_info
{
  const char *str;              /* ??? */
  PT_MISC_TYPE var_type;        /* PT_HOST_IN */
  int index;                    /* for PT_HOST_VAR ordering */
};

/* Info for lists of PT_NODE */
struct pt_node_list_info
{
  PT_MISC_TYPE list_type;       /* e.g. PT_IS_VALUE */
  PT_NODE *list;                /* the list of nodes */
};

/* Info for Insert */
struct pt_insert_info
{
  PT_NODE *spec;                /* PT_SPEC */
  PT_NODE *attr_list;           /* PT_NAME */
  PT_NODE *value_clauses;       /* PT_NODE_LIST(list) or PT_NODE_LIST(PT_SELECT) */
  PT_HINT_ENUM hint;            /* hint flag */
  PT_NODE *odku_assignments;    /* ON DUPLICATE KEY UPDATE assignments */
  bool do_replace;              /* REPLACE statement was given */
  PT_NODE *non_null_attrs;      /* attributes with not null constraint */
  PT_NODE *odku_non_null_attrs; /* attributes with not null constraint in
                                 * odku assignments
                                 */
  int has_uniques;              /* class has unique constraints */
};

/* Info for Transaction Isolation Level */
struct pt_isolation_lvl_info
{
  PT_MISC_TYPE schema;
  PT_MISC_TYPE instances;
  PT_NODE *level;               /* PT_VALUE */
};

/* Info for Names
  This includes identifiers
  */

#define NAME_FROM_PT_DOT 1
#define NAME_FROM_CLASSNAME_DOT_STAR 2  /* classname.* */
#define NAME_FROM_STAR 3        /* * */
#define NAME_IN_PATH_EXPR 4

struct pt_name_info
{
  UINTPTR spec_id;              /* unique identifier for entity specs */
  const char *original;         /* the string of the original name */
  const char *resolved;         /* the string of the resolved name */
  DB_OBJECT *db_object;         /* the object, if this is a class or instance */
  DB_OBJECT *virt_object;       /* the top level view this this class is being viewed through. */
  PT_MISC_TYPE meta_class;      /* 0 or PT_META or PT_CLASS */
  PT_NODE *default_value;       /* PT_VALUE the default value of the attribute */
  unsigned int custom_print;
  unsigned short correlation_level;     /* for correlated attributes */
  char hidden_column;           /* used for updates and deletes for
                                 * the class OID column */

#define PT_NAME_INFO_DOT_SPEC        1  /* x, y of x.y.z */
#define PT_NAME_INFO_DOT_NAME        2  /* z of x.y.z */
#define PT_NAME_INFO_STAR            4  /* * */
#define PT_NAME_INFO_DOT_STAR        8  /* classname.* */
#define PT_NAME_INFO_CONSTANT       16
#if 0                           /* unused */
#define PT_NAME_INFO_EXTERNAL       32  /* in case of TEXT type at attr
                                           definition or attr.object at attr
                                           description */
#endif
#define PT_NAME_INFO_DESC           64  /* DESC on an index column name */
#define PT_NAME_INFO_FILL_DEFAULT  128  /* whether default_value should be
                                           filled in */
#define PT_NAME_INFO_GENERATED_OID 256  /* set when a PT_NAME node
                                           that maps to an OID is generated
                                           internally for statement processing
                                           and execution */
#if 0                           /* unused */
#define PT_NAME_ALLOW_REUSABLE_OID 512  /* ignore the REUSABLE_OID
                                           restrictions for this name */
#endif
#define PT_NAME_GENERATED_DERIVED_SPEC 1024     /* attribute generated from
                                                 * derived spec
                                                 */
#define PT_NAME_FOR_UPDATE         2048 /* Table name in FOR UPDATE clause */
#define PT_NAME_FOR_SHARD_KEY      4096 /* Shardlet Key Column */


  short flag;
#define PT_NAME_INFO_IS_FLAGED(e, f)    ((e)->info.name.flag & (short) (f))
#define PT_NAME_INFO_SET_FLAG(e, f)     (e)->info.name.flag |= (short) (f)
#define PT_NAME_INFO_CLEAR_FLAG(e, f)   (e)->info.name.flag &= (short) ~(f)
  short location;               /* 0: WHERE; n: join condition of n-th FROM */
  PT_NODE *indx_key_limit;      /* key limits for index name */
};

enum
{
  PT_IDX_HINT_USE = 0,
  PT_IDX_HINT_CLASS_NONE = -1,
  PT_IDX_HINT_NONE = -2
};

/* PT_IDX_HINT_ORDER is used in index hint rewrite, to sort the index hints
 * in the using_index clause in the order implied by this define; this is
 * needed in order to avoid additional loops through the hint list */
#define PT_IDX_HINT_ORDER(hint_node)					  \
  ((hint_node->etc == (void *) PT_IDX_HINT_CLASS_NONE) ? 1 :		  \
    (hint_node->etc == (void *) PT_IDX_HINT_USE ? 2 : 0))

/* Info RENAME  */
struct pt_rename_info
{
  PT_NODE *old_name;            /* PT_NAME */
  PT_NODE *in_class;            /* PT_NAME */
  PT_NODE *new_name;            /* PT_NAME */
  PT_MISC_TYPE meta;
  PT_MISC_TYPE attr_or_mthd;
  PT_MISC_TYPE entity_type;
};


/* Info REVOKE  */
struct pt_revoke_info
{
  PT_NODE *auth_cmd_list;
  PT_NODE *user_list;
  PT_NODE *spec_list;
};

/* Info ROLLBACK  */
struct pt_rollback_work_info
{
  PT_NODE *save_name;           /* PT_NAME */
};

/* Info for a UNION/DIFFERENCE/INTERSECTION node */
struct pt_union_info
{
  PT_NODE *arg1;                /* PT_SELECT_EXPR 1st argument */
  PT_NODE *arg2;                /* PT_SELECT_EXPR 2nd argument */
};

/* Info for an SAVEPOINT node */
struct pt_savepoint_info
{
  PT_NODE *save_name;           /* PT_NAME */
};

/* Info for a SELECT node */
struct pt_select_info
{
  PT_NODE *list;                /* PT_EXPR PT_NAME */
  PT_NODE *from;                /* PT_SPEC (list) */
  PT_NODE *where;               /* PT_EXPR        */
  PT_NODE *group_by;            /* PT_EXPR (list) */
  PT_NODE *having;              /* PT_EXPR        */
  PT_NODE *using_index;         /* PT_NAME (list) */
#if 0                           /* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
  PT_NODE *ordered_hint;        /* ORDERED_HINT hint's arguments (PT_NAME list) */
#endif
  PT_NODE *use_nl_hint;         /* USE_NL hint's arguments (PT_NAME list) */
  PT_NODE *use_idx_hint;        /* USE_IDX hint's arguments (PT_NAME list) */
  struct qo_summary *qo_summary;
  PT_HINT_ENUM hint;
  short flag;                   /* flags */
};

#if 0                           /* unused */
#define PT_SELECT_INFO_ANSI_JOIN	1       /* has ANSI join? */
#define PT_SELECT_INFO_ORACLE_OUTER	2       /* has Oracle's outer join operator? */
#endif
#define PT_SELECT_INFO_DUMMY		4       /* is dummy (i.e., 'SELECT * FROM x') ? */
#define PT_SELECT_INFO_HAS_AGG		8       /* has any type of aggregation? */
#if 0
#define PT_SELECT_INFO_FLAG_RESERVED_01	16      /* reserved */
#define PT_SELECT_INFO_FLAG_RESERVED_02	32      /* reserved */
#endif

#if 0                           /* TODO - remove */
#define PT_SELECT_INFO_IDX_SCHEMA	64      /* is show index query */
#define PT_SELECT_INFO_COLS_SCHEMA	128     /* is show columns query */
#define PT_SELECT_FULL_INFO_COLS_SCHEMA	256     /* is show columns query */
#endif

#if 0                           /* unused */
#define	PT_SELECT_INFO_FLAG_RESERVED_03        512      /* reserved */
#endif
#define PT_SELECT_INFO_FOR_UPDATE	1024    /* FOR UPDATE clause is active */
#if 0                           /* unused */
#define PT_SELECT_INFO_FLAG_RESERVED_04   2048  /* reserved */
#endif
#define PT_SELECT_INFO_UPD_DEL   	4096    /* is query for update or delete */

#define PT_SELECT_INFO_IS_FLAGED(s, f)  \
          ((s)->info.query.q.select.flag & (short) (f))
#define PT_SELECT_INFO_SET_FLAG(s, f)   \
          (s)->info.query.q.select.flag |= (short) (f)
#define PT_SELECT_INFO_CLEAR_FLAG(s, f) \
          (s)->info.query.q.select.flag &= (short) ~(f)

/* common with union and select info */
struct pt_query_info
{
  int correlation_level;        /* for correlated subqueries */
  PT_MISC_TYPE all_distinct;    /* enum value is PT_ALL or PT_DISTINCT */
  PT_MISC_TYPE is_subquery;     /* PT_IS_SUB_QUERY, PT_IS_UNION_QUERY, or 0 */
  char is_view_spec;            /* 0 - normal, 1 - view query spec */
  char oids_included;           /* DB_NO_OIDS/0 DB_ROW_OIDS/1 */
  int upd_del_class_cnt;        /* number of classes affected by update or
                                 * delete in the generated SELECT statement */
  unsigned has_outer_spec:1;    /* has outer join spec ? */
  unsigned is_sort_spec:1;      /* query is a sort spec expression */
  unsigned is_insert_select:1;  /* query is a sub-select for insert statement */
  unsigned single_tuple:1;      /* is single-tuple query ? */
  unsigned vspec_as_derived:1;  /* is derived from vclass spec ? */
  unsigned reexecute:1;         /* should be re-executed; not from the result cache */
  unsigned do_cache:1;          /* do cache the query result */
  unsigned do_not_cache:1;      /* do not cache the query result */
  unsigned order_siblings:1;    /* flag ORDER SIBLINGS BY */
  unsigned rewrite_limit:1;     /* need to rewrite the limit clause */
  PT_NODE *order_by;            /* PT_EXPR (list) */
  PT_NODE *orderby_for;         /* PT_EXPR (list) */
  PT_NODE *qcache_hint;         /* enable/disable query cache */
  PT_NODE *limit;               /* PT_VALUE (list) limit clause parameter(s) */
  PT_NODE *pk_next;             /* PT_HOST_VAR (list) limit clause parameter(s) */
  void *xasl;                   /* xasl proc pointer */
  UINTPTR id;                   /* query unique id # */
  PT_HINT_ENUM hint;            /* hint flag */
  union
  {
    PT_SELECT_INFO select;
    PT_UNION_INFO union_;
  } q;
};


/* Info for Set Optimization Level statement */
struct pt_set_opt_lvl_info
{
  PT_NODE *val;                 /* PT_VALUE */
  PT_MISC_TYPE option;          /* PT_OPT_LVL, PT_OPT_COST */
};

/* Info for Set Parameters statement */
struct pt_set_sys_params_info
{
  PT_MISC_TYPE persist;
  PT_NODE *val;                 /* PT_VALUE */
};

/* Info for Set Transaction statement */
struct pt_set_xaction_info
{
  PT_NODE *xaction_modes;       /* PT_ISOLATION_LVL, PT_TIMEOUT (list) */
};

/* Info for OrderBy/GroupBy */
struct pt_sort_spec_info
{
  PT_NODE *expr;                /* PT_EXPR, PT_VALUE, PT_NAME */
  QFILE_TUPLE_VALUE_POSITION pos_descr; /* Value position descriptor */
  PT_MISC_TYPE asc_or_desc;     /* enum value will be PT_ASC or PT_DESC    */
  PT_MISC_TYPE nulls_first_or_last;     /* enum value will be
                                         * PT_NULLS_DEFAULT, PT_NULLS_FIRST
                                         * or PT_NULLS_LAST */
};

/* Info for Transaction Timeout */
struct pt_timeout_info
{
  PT_NODE *val;                 /* PT_VALUE */
};

/* Info for UPDATE node */
struct pt_update_info
{
  PT_NODE *spec;                /*  SPEC  */
  PT_NODE *assignment;          /*  EXPR(list)   */
  PT_NODE *search_cond;         /*  EXPR         */
  PT_NODE *using_index;         /* PT_NAME (list) */
  PT_NODE *internal_stmts;      /* internally created statements to handle TEXT */
#if 0                           /* TEMPORARY COMMENTED CODE: DO NOT REMOVE ME !!! */
  PT_NODE *ordered_hint;        /* ORDERED_HINT hint's arguments (PT_NAME list) */
#endif
  PT_NODE *use_nl_hint;         /* USE_NL hint's arguments (PT_NAME list) */
  PT_NODE *use_idx_hint;        /* USE_IDX hint's arguments (PT_NAME list) */
  PT_NODE *limit;               /* PT_VALUE limit clause parameter */
  PT_NODE *order_by;            /* PT_EXPR (list) */
  PT_NODE *orderby_for;         /* PT_EXPR */
  PT_HINT_ENUM hint;            /* hint flag */
  unsigned has_unique:1;        /* whether there's unique constraint */
  unsigned server_update:1;     /* whether it can be server-side update */
  unsigned rewrite_limit:1;     /* need to rewrite the limit clause */
};

/* UPDATE STATISTICS INFO */
struct pt_update_stats_info
{
  int update_stats;             /* 1 iff UPDATE */
  PT_NODE *class_list;          /* PT_NAME */
  int all_classes;              /* 1 iff ALL CLASSES, -1 iff CATALOG */
  int with_fullscan;            /* 1 iff WITH FULLSCAN */
};

/* Info for table options */
struct pt_table_option_info
{
  PT_TABLE_OPTION_TYPE option;  /* The table option type */
  PT_NODE *val;                 /* PT_VALUE */
};

/* Info for VALUE nodes
  these are intended to parallel the definitions in dbi.h and be
  identical whenever possible
  */

/* enum Time Zones */
typedef enum pt_time_zones
{
  PT_TIMEZONE_EASTERN,
  PT_TIMEZONE_CENTRAL,
  PT_TIMEZONE_MOUNTAIN,
  PT_TIMEZONE_PACIFIC
} PT_TIMEZONE;

/* typedefs for TIME and DATE */
typedef long PT_TIME;
typedef long PT_DATE;
typedef DB_DATETIME PT_DATETIME;

/* Union of datavalues */
union pt_data_value
{
  long i;
  DB_BIGINT bigint;
  float f;
  double d;
  PARSER_VARCHAR *str;          /* keeps as string different data type:
                                 * string data types (char, nchar, byte)
                                 * date and time data types
                                 * numeric
                                 */
  void *p;                      /* what is this */
  DB_OBJECT *op;
  PT_TIME time;
  PT_DATE date;
  PT_DATETIME datetime;
  PT_NODE *set;                 /* constant sets */
  int b;
};


/* Info for the VALUE node */
struct pt_value_info
{
  const char *text;             /* printed text of a value or of an expression
                                 * folded to a value.
                                 * NOTE: this is not the actual value of the
                                 * node. Use value in data_value instead.
                                 */
  PT_DATA_VALUE data_value;     /* see above UNION defs */
  DB_VALUE db_value;
  short db_value_is_initialized;
  short db_value_is_in_workspace;
  short location;               /* 0 : WHERE; n : join condition of n-th FROM */
  char string_type;             /* ' ', 'N', 'B', or 'X' */
};


/* Info for the ZZ_ERROR_MSG node */
struct PT_ZZ_ERROR_MSG_INFO
{
  char *error_message;          /* a helpful explanation of the error */
};

/* Info for the CONSTRAINT node */
struct pt_constraint_info
{
  PT_NODE *name;
  PT_CONSTRAINT_TYPE type;
  short deferrable;
  short initially_deferred;
  union
  {
    struct
    {
      PT_NODE *attrs;           /* List of attribute names */
    } primary_key;
    struct
    {
      PT_NODE *attr;            /* Attribute name          */
    } not_null;
    struct
    {
      PT_NODE *attrs;           /* List of attribute names */
    } unique;
    struct
    {
      PT_NODE *attrs;           /* List of attribute names */
    } index;
    struct
    {
      PT_NODE *expr;            /* Search condition */
    } check;
  } un;
};

/* Info for the POINTER node */
struct pt_pointer_info
{
  PT_NODE *node;                /* original node pointer */
  double sel;                   /* selectivity factor of the predicate */
  int rank;                     /* rank factor for the same selectivity */
};

struct pt_trace_info
{
  PT_MISC_TYPE on_off;
  PT_MISC_TYPE format;
};

/* pt_insert_value_info -
 * Parse tree node info used to replace nodes in insert value list after being
 * evaluated.
 */
struct pt_insert_value_info
{
  PT_NODE *original_node;       /* original node before first evaluation.
                                 * if this is NULL, it is considered that
                                 * evaluated value cannot change on different
                                 * execution, and reevaluation is not needed
                                 */
  DB_VALUE value;               /* evaluated value */
  int is_evaluated;             /* true if value was already evaluated */
};

/* Info field of the basic NODE
  If 'xyz' is the name of the field, then the structure type should be
  struct PT_XYZ_INFO xyz;
  List in alphabetical order.
  */

union pt_statement_info
{
  PT_ZZ_ERROR_MSG_INFO error_msg;
  PT_ALTER_INFO alter;
  PT_ALTER_USER_INFO alter_user;
  PT_ATTR_DEF_INFO attr_def;
  PT_ATTR_ORDERING_INFO attr_ordering;
  PT_AUTH_CMD_INFO auth_cmd;
  PT_CONSTRAINT_INFO constraint;
  PT_CREATE_ENTITY_INFO create_entity;
  PT_CREATE_USER_INFO create_user;
  PT_DATA_DEFAULT_INFO data_default;
  PT_DATA_TYPE_INFO data_type;
  PT_DELETE_INFO delete_;
  PT_DOT_INFO dot;
  PT_DROP_INFO drop;
  PT_DROP_USER_INFO drop_user;
  PT_EXPR_INFO expr;
  PT_FILE_PATH_INFO file_path;
  PT_FUNCTION_INFO function;
  PT_GET_OPT_LVL_INFO get_opt_lvl;
  PT_GET_XACTION_INFO get_xaction;
  PT_GRANT_INFO grant;
  PT_HOST_VAR_INFO host_var;
  PT_INDEX_INFO index;
  PT_INSERT_INFO insert;
  PT_ISOLATION_LVL_INFO isolation_lvl;
  PT_NAME_INFO name;
  PT_NODE_LIST_INFO node_list;
  PT_QUERY_INFO query;
  PT_RENAME_INFO rename;
  PT_REVOKE_INFO revoke;
  PT_ROLLBACK_WORK_INFO rollback_work;
  PT_SAVEPOINT_INFO savepoint;
  PT_SET_OPT_LVL_INFO set_opt_lvl;
  PT_SET_SYS_PARAMS_INFO set_sys_params;
  PT_SORT_SPEC_INFO sort_spec;
  PT_SPEC_INFO spec;
  PT_TABLE_OPTION_INFO table_option;
  PT_TIMEOUT_INFO timeout;
  PT_UPDATE_INFO update;
  PT_UPDATE_STATS_INFO update_stats;
  PT_VALUE_INFO value;
  PT_POINTER_INFO pointer;
  PT_TRACE_INFO trace;
};


/*
 * auxiliary structures for tree walking operations related to aggregates
 */
struct pt_agg_check_info
{
  PT_NODE *from;                /* initial spec list */
  PT_NODE *group_by;            /* group by list */
  int depth;                    /* current depth */
};

struct pt_agg_rewrite_info
{
  PT_NODE *select_stack;        /* SELECT statement stack (0 = base) */
  PT_NODE *from;                /* initial spec list */
  PT_NODE *new_from;            /* new spec */
  PT_NODE *derived_select;      /* initial select (that is being derived) */
  int depth;
};

struct pt_agg_find_info
{
  PT_NODE *select_stack;        /* SELECT statement stack (0 = base) */
  int base_count;               /* # of aggregate functions that belong to the
                                   statement at the base of the stack */
  int out_of_context_count;     /* # of aggregate functions that do not belong
                                   to any statement within the stack */
  bool stop_on_subquery;        /* walk subqueries? */
};

struct pt_agg_name_info
{
  PT_NODE *select_stack;        /* SELECT statement stack (0 = base) */
  int max_level;                /* maximum level within the stack that is
                                   is referenced by PT_NAMEs */
  int name_count;               /* # of PT_NAME nodes found */
};

struct pt_non_groupby_col_info
{
  PT_NODE *groupby;
  bool has_non_groupby_col;
};

/*
 * variable string for parser
 */
struct parser_varchar
{
  int length;
  unsigned char bytes[1];
};

/*
 * The parser node structure
 */
struct parser_node
{
  PT_NODE_TYPE node_type;       /* the type of SQL statement this represents */
  int parser_id;                /* which parser did I come from */
  int line_number;              /* the user line number originating this  */
  int column_number;            /* the user column number originating this  */
  int buffer_pos;               /* position in the parse buffer of the string
                                   originating this */
  char *sql_user_text;          /* user input sql string */
  int sql_user_text_len;        /* user input sql string length (one statement) */

  PT_NODE *next;                /* forward link for NULL terminated list */
  PT_NODE *or_next;             /* forward link for DNF list */
  void *etc;                    /* application specific info hook */
  UINTPTR spec_ident;           /* entity spec equivalence class */
  PT_NODE *data_type;           /* for non-primitive types, Sets, objects stec. */
  XASL_ID *xasl_id;             /* XASL_ID for this SQL statement */
  const char *alias_print;      /* the column alias */
  PT_TYPE_ENUM type_enum;       /* type enumeration tag PT_TYPE_??? */

  unsigned recompile:1;         /* the statement should be recompiled - used for plan cache */
  unsigned si_datetime:1;       /* get server info; SYS_DATETIME */
  unsigned use_plan_cache:1;    /* used for plan cache */
  unsigned is_hidden_column:1;
  unsigned is_paren:1;
  unsigned with_rollup:1;       /* WITH ROLLUP clause for GROUP BY */
  unsigned do_not_fold:1;       /* disables constant folding on the node */
  unsigned is_cnf_start:1;
  unsigned do_not_replace_orderby:1;    /* when checking query in create/alter
                                           view, do not replace order by */
  unsigned is_server_query_ended:1;     /* temporary variable to set DB_QUERY_RESULT */
  unsigned is_null_node:1;      /* for NULL value */
  PT_STATEMENT_INFO info;       /* depends on 'node_type' field */
};

/* 20 (for the LOCAL_TRANSACTION_ID keyword) + null char + 3 for expansion */
#define MAX_KEYWORD_SIZE (20 + 1 + 3)

typedef struct keyword_record KEYWORD_RECORD;
struct keyword_record
{
  short value;
  char keyword[MAX_KEYWORD_SIZE];
  short unreserved;             /* keyword can be used as an identifier, 0 means it is reserved and cannot be used as an identifier, nonzero means it can be  */
};

typedef struct pt_plan_trace_info
{
  QUERY_TRACE_FORMAT format;
  union
  {
    char *text_plan;
    json_t *json_plan;
  } trace;
} PT_PLAN_TRACE_INFO;

typedef int (*PT_CASECMP_FUN) (const char *s1, const char *s2);
typedef int (*PT_INT_FUNCTION) (PARSER_CONTEXT * c);

/*
 * COMPILE_CONTEXT cover from user input query string to gnerated xasl
 */
typedef struct compile_context COMPILE_CONTEXT;
struct compile_context
{
  struct xasl_node *xasl;

  const char *sql_user_text;    /* original query statement that user input */
  int sql_user_text_len;        /* length of sql_user_text */

  const char *sql_hash_text;    /* rewritten query string which is used as hash key */

  const char *sql_plan_text;    /* plans for this query */
  int sql_plan_alloc_size;      /* query_plan alloc size */
};

struct parser_context
{
  PT_INT_FUNCTION next_char;    /* the next character function */
  PT_INT_FUNCTION next_byte;    /* the next byte function */
  PT_CASECMP_FUN casecmp;       /* for case insensitive comparisons */

  int id;                       /* internal parser id */

  const char *original_buffer;  /* pointer to the original parse buffer */
  const char *buffer;           /* for parse buffer */
  FILE *file;                   /* for parse file */
  int stack_top;                /* parser stack top */
  int stack_size;               /* total number of slots in node_stack */
  PT_NODE **node_stack;         /* the parser stack */
  PT_NODE *orphans;             /* list of parse tree fragments freed later */

  char *error_buffer;           /* for parse error messages            */

  PT_NODE *error_msgs;          /* list of parsing error messages */
  PT_NODE *warnings;            /* list of warning messages */

  PT_PRINT_VALUE_FUNC print_db_value;

  jmp_buf jmp_env;              /* environment for longjumping on
                                   out of memory errors. */
  unsigned int custom_print;
  int jmp_env_active;           /* flag to indicate jmp_env status */

  QUERY_ID query_id;            /* id assigned to current query */
  DB_VALUE *host_variables;     /* host variables place holder;
                                   DB_VALUE array */
  int host_var_count;           /* number of input host variables */
  int line, column;             /* current input line and column */

  void *etc;                    /* application context */

  VIEW_CACHE_INFO *view_cache;  /* parsing cache using in view transformation */
  struct symbol_info *symbols;  /* a place to keep information
                                 * used in generating query processing
                                 * (xasl) procedures. */
  char **lcks_classes;

  size_t input_buffer_length;
  size_t input_buffer_position;

  int au_save;                  /* authorization to restore if longjmp while
                                   authorization turned off */

  DB_VALUE sys_datetime;

  int num_lcks_classes;

  long int lrand;               /* integer random value used by rand() */
  double drand;                 /* floating-point random value used by drand() */

  COMPILE_CONTEXT context;
  struct xasl_node *parent_proc_xasl;

  bool query_trace;
#if defined (ENABLE_UNUSED_FUNCTION)
  int num_plan_trace;
  PT_PLAN_TRACE_INFO plan_trace[MAX_NUM_PLAN_TRACE];
#endif

  unsigned has_internal_error:1;        /* 0 or 1 */
  unsigned set_host_var:1;      /* 1 if the user has set host variables */
  unsigned print_type_ambiguity:1;      /* pt_print_value sets it to 1
                                           when it printed a value whose type
                                           cannot be clearly determined from
                                           the string representation */
  unsigned is_in_and_list:1;    /* set to 1 when the caller immediately above is
                                   pt_print_and_list(). Used because AND lists (CNF
                                   trees) can be printed via print_and_list or
                                   straight via pt_print_expr().
                                   We need to keep print_and_list because it could
                                   get called before we get a chance to mark the
                                   CNF start nodes. */
  unsigned is_holdable:1;       /* set to true if result must be available across
                                   commits */
  unsigned is_autocommit:1;
};

/* used in assignments enumeration */
typedef struct pt_assignments_helper PT_ASSIGNMENTS_HELPER;
struct pt_assignments_helper
{
  PARSER_CONTEXT *parser;       /* parser context */
  PT_NODE *assignment;          /* current assignment node in the assignments
                                 * list */
  PT_NODE *lhs;                 /* left side of the assignment */
  PT_NODE *rhs;                 /* right side of the assignment */
  bool is_n_column;             /* true if the assignment is a multi-column
                                 * assignment */
};

typedef enum pt_select_purpose PT_SELECT_PURPOSE;
enum pt_select_purpose
{
  PT_FOR_DELETE = 1,
  PT_FOR_UPDATE
};

void *parser_allocate_string_buffer (const PARSER_CONTEXT * parser, const int length, const int align);

#if !defined (SERVER_MODE)
#ifdef __cplusplus
extern "C"
{
#endif
  extern PARSER_CONTEXT *parent_parser;
#ifdef __cplusplus
}
#endif
#endif

#endif                          /* _PARSE_TREE_H_ */

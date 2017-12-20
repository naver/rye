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
 * rsql_grammar.y - SQL grammar file
 */



%{
#define YYMAXDEPTH	1000000

/* #define PARSER_DEBUG */

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include <errno.h>

#include "parser.h"
#include "parser_message.h"
#include "dbdef.h"
#include "chartype.h"
#include "language_support.h"
#include "environment_variable.h"
#include "transaction_cl.h"
#include "rsql_grammar_scan.h"
#include "system_parameter.h"
#include "memory_alloc.h"

/* Bit mask to be used to check constraints of a column.
 */
#define COLUMN_CONSTRAINT_UNIQUE		(0x01)
#define COLUMN_CONSTRAINT_PRIMARY_KEY		(0x02)
#define COLUMN_CONSTRAINT_NULL			(0x04)
#define COLUMN_CONSTRAINT_OTHERS		(0x08)
#define COLUMN_CONSTRAINT_DEFAULT		(0x10)

#ifdef PARSER_DEBUG
#define DBG_PRINT printf("rule matched at line: %d\n", __LINE__);
#define PRINT_(a) printf(a)
#define PRINT_1(a, b) printf(a, b)
#define PRINT_2(a, b, c) printf(a, b, c)
#else
#define DBG_PRINT
#define PRINT_(a)
#define PRINT_1(a, b)
#define PRINT_2(a, b, c)
#endif

#define STACK_SIZE	128

typedef struct function_map FUNCTION_MAP;
struct function_map
{
  const char *keyword;
  PT_OP_TYPE op;
};


static FUNCTION_MAP functions[] = {
  {"abs", PT_ABS},
  {"acos", PT_ACOS},
  {"asin", PT_ASIN},
  {"atan", PT_ATAN},
  {"atan2", PT_ATAN2},
  {"bin", PT_BIN},
  {"bit_count", PT_BIT_COUNT},
  {"ceil", PT_CEIL},
  {"ceiling", PT_CEIL},
  {"char_length", PT_CHAR_LENGTH},
  {"character_length", PT_CHAR_LENGTH},
  {"concat", PT_CONCAT},
  {"concat_ws", PT_CONCAT_WS},
  {"cos", PT_COS},
  {"cot", PT_COT},
  {"curtime", PT_SYS_TIME},
  {"curdate", PT_SYS_DATE},
  {"utc_time", PT_UTC_TIME},
  {"utc_date", PT_UTC_DATE},
  {"datediff", PT_DATEDIFF},
  {"timediff",PT_TIMEDIFF},
  {"date_format", PT_DATE_FORMAT},
  {"dayofmonth", PT_DAYOFMONTH},
  {"dayofyear", PT_DAYOFYEAR},
  {"decode", PT_DECODE},
  {"degrees", PT_DEGREES},
  {"drand", PT_DRAND},
  {"drandom", PT_DRANDOM},
  {"exp", PT_EXP},
  {"field", PT_FIELD},
  {"floor", PT_FLOOR},
  {"from_days", PT_FROMDAYS},
  {"greatest", PT_GREATEST},
  {"groupby_num", PT_GROUPBY_NUM},
  {"ha_status", PT_HA_STATUS},
  {"index_cardinality", PT_INDEX_CARDINALITY},
  {"inst_num", PT_INST_NUM},
  {"instr", PT_INSTR},
  {"instrb", PT_INSTR},
  {"last_day", PT_LAST_DAY},
  {"length", PT_CHAR_LENGTH},
  {"lengthb", PT_CHAR_LENGTH},
  {"least", PT_LEAST},
  {"like_match_lower_bound", PT_LIKE_LOWER_BOUND},
  {"like_match_upper_bound", PT_LIKE_UPPER_BOUND},
  {"list_dbs", PT_LIST_DBS},
  {"locate", PT_LOCATE},
  {"ln", PT_LN},
  {"log2", PT_LOG2},
  {"log10", PT_LOG10},
  {"log", PT_LOG},
  {"lpad", PT_LPAD},
  {"ltrim", PT_LTRIM},
  {"makedate", PT_MAKEDATE},
  {"maketime", PT_MAKETIME},
  {"mid", PT_MID},
  {"months_between", PT_MONTHS_BETWEEN},
  {"format", PT_FORMAT},
  {"now", PT_SYS_DATETIME},
  {"nvl", PT_NVL},
  {"nvl2", PT_NVL2},
  {"orderby_num", PT_ORDERBY_NUM},
  {"power", PT_POWER},
  {"pow", PT_POWER},
  {"pi", PT_PI},
  {"radians", PT_RADIANS},
  {"rand", PT_RAND},
  {"random", PT_RANDOM},
  {"repeat", PT_REPEAT},
  {"space", PT_SPACE},
  {"reverse", PT_REVERSE},
  {"round", PT_ROUND},
  {"rpad", PT_RPAD},
  {"rtrim", PT_RTRIM},
  {"sec_to_time", PT_SECTOTIME},
  {"shard_groupid", PT_SHARD_GROUPID},
  {"shard_lockname", PT_SHARD_LOCKNAME},
  {"shard_nodeid", PT_SHARD_NODEID},
  {"sign", PT_SIGN},
  {"sin", PT_SIN},
  {"sqrt", PT_SQRT},
  {"strcmp", PT_STRCMP},
  {"substr", PT_SUBSTRING},
  {"substring_index", PT_SUBSTRING_INDEX},
  {"find_in_set", PT_FINDINSET},
  {"md5", PT_MD5},
  {"sha1", PT_SHA_ONE},
  {"sha2", PT_SHA_TWO},  	
  {"substrb", PT_SUBSTRING},
  {"tan", PT_TAN},
  {"time_format", PT_TIME_FORMAT},
  {"to_char", PT_TO_CHAR},
  {"to_date", PT_TO_DATE},
  {"to_datetime", PT_TO_DATETIME},
  {"to_days", PT_TODAYS},
  {"time_to_sec", PT_TIMETOSEC},
  {"to_number", PT_TO_NUMBER},
  {"to_time", PT_TO_TIME},
  {"trunc", PT_TRUNC},
  {"unix_timestamp", PT_UNIX_TIMESTAMP},
  {"typeof", PT_TYPEOF},
  {"from_unixtime", PT_FROM_UNIXTIME},
  {"weekday", PT_WEEKDAY},
  {"dayofweek", PT_DAYOFWEEK},
  {"version", PT_VERSION},
  {"quarter", PT_QUARTERF},
  {"week", PT_WEEKF},
  {"hex", PT_HEX},
  {"ascii", PT_ASCII},
  {"conv", PT_CONV},
  {"inet_aton", PT_INET_ATON},
  {"inet_ntoa", PT_INET_NTOA},
  {"width_bucket", PT_WIDTH_BUCKET},
  {"trace_stats", PT_TRACE_STATS},
  {"to_base64", PT_TO_BASE64},
  {"from_base64", PT_FROM_BASE64}
};


static int parser_groupby_exception = 0;




/* xxxnum_check: 0 not allowed, no compatibility check
		 1 allowed, compatibility check (search_condition)
		 2 allowed, no compatibility check (select_list) */
static int parser_instnum_check = 0;
static int parser_groupbynum_check = 0;
static int parser_orderbynum_check = 0;
static int parser_within_join_condition = 0;

/* xxx_check: 0 not allowed
              1 allowed */
static int parser_subquery_check = 1;
static int parser_hostvar_check = 1;

/* check sys_date, sys_time, sys_datetime local_transaction_id */
static bool parser_si_datetime = false;

/* check the condition that the result of a query is not able to be cached */
static bool parser_cannot_cache = false;

/* check iff top select statement; currently, not used */
static int parser_select_level = -1;

typedef struct
{
  PT_NODE *c1;
  PT_NODE *c2;
} container_2;

typedef struct
{
  PT_NODE *c1;
  PT_NODE *c2;
  PT_NODE *c3;
} container_3;

typedef struct
{
  PT_NODE *c1;
  PT_NODE *c2;
  PT_NODE *c3;
  PT_NODE *c4;
} container_4;

typedef struct
{
  PT_NODE *c1;
  PT_NODE *c2;
  PT_NODE *c3;
  PT_NODE *c4;
  PT_NODE *c5;
  PT_NODE *c6;
  PT_NODE *c7;
  PT_NODE *c8;
  PT_NODE *c9;
  PT_NODE *c10;
} container_10;

#define PT_EMPTY INT_MAX


#define TO_NUMBER(a)			((UINTPTR)(a))
#define FROM_NUMBER(a)			((PT_NODE*)(UINTPTR)(a))


#define SET_CONTAINER_2(a, i, j)		a.c1 = i, a.c2 = j
#define SET_CONTAINER_3(a, i, j, k)		a.c1 = i, a.c2 = j, a.c3 = k
#define SET_CONTAINER_4(a, i, j, k, l)		a.c1 = i, a.c2 = j, a.c3 = k, a.c4 = l

#define CONTAINER_AT_0(a)			(a).c1
#define CONTAINER_AT_1(a)			(a).c2
#define CONTAINER_AT_2(a)			(a).c3
#define CONTAINER_AT_3(a)			(a).c4
#define CONTAINER_AT_4(a)			(a).c5
#define CONTAINER_AT_5(a)			(a).c6
#define CONTAINER_AT_6(a)			(a).c7
#define CONTAINER_AT_7(a)			(a).c8
#define CONTAINER_AT_8(a)			(a).c9
#define CONTAINER_AT_9(a)			(a).c10

#define PARSER_SAVE_ERR_CONTEXT(node, context) \
  if ((node) && (node)->buffer_pos == -1) \
    { \
     (node)->buffer_pos = context; \
    }

void rsql_yyerror_explicit (int line, int column);
void rsql_yyerror (const char *s);

FUNCTION_MAP *keyword_offset (const char *name);

static PT_NODE *parser_make_expr_with_func (PARSER_CONTEXT * parser,
					    FUNC_TYPE func_code,
					    PT_NODE * args_list);
static PT_NODE *parser_make_link (PT_NODE * list, PT_NODE * node);
static PT_NODE *parser_make_link_or (PT_NODE * list, PT_NODE * node);



static void parser_save_and_set_cannot_cache (bool value);
static void parser_restore_cannot_cache (void);

static void parser_save_and_set_si_datetime (int value);
static void parser_restore_si_datetime (void);

static void parser_save_and_set_wjc (int value);
static void parser_restore_wjc (void);

static void parser_save_and_set_ic (int value);
static void parser_restore_ic (void);

static void parser_save_and_set_gc (int value);
static void parser_restore_gc (void);

static void parser_save_and_set_oc (int value);
static void parser_restore_oc (void);

static void parser_save_and_set_sqc (int value);
static void parser_restore_sqc (void);

#if defined (ENABLE_UNUSED_FUNCTION)
static void parser_save_and_set_hvar (int value);
static void parser_restore_hvar (void);
#endif

static void parser_save_alter_node (PT_NODE * node);
static PT_NODE *parser_get_alter_node (void);

static void parser_save_attr_def_one (PT_NODE * node);
static PT_NODE *parser_get_attr_def_one (void);

static void parser_push_orderby_node (PT_NODE * node);
static PT_NODE *parser_top_orderby_node (void);
static PT_NODE *parser_pop_orderby_node (void);

static void parser_push_select_stmt_node (PT_NODE * node);
static PT_NODE *parser_top_select_stmt_node (void);
static PT_NODE *parser_pop_select_stmt_node (void);
#if defined (ENABLE_UNUSED_FUNCTION)
static bool parser_is_select_stmt_node_empty (void);
#endif

static void parser_push_hint_node (PT_NODE * node);
static PT_NODE *parser_top_hint_node (void);
static PT_NODE *parser_pop_hint_node (void);
static bool parser_is_hint_node_empty (void);

static void parser_initialize_parser_context (void);
static PT_NODE *parser_make_date_lang (int arg_cnt, PT_NODE * arg3);
static PT_NODE *parser_make_number_lang (const int argc);
static void parser_remove_dummy_select (PT_NODE ** node);
static int parser_count_list (PT_NODE * list);
static int parser_count_prefix_columns (PT_NODE * list, int * arg_count);

static void resolve_alias_in_expr_node (PT_NODE * node, PT_NODE * list);
static void resolve_alias_in_name_node (PT_NODE ** node, PT_NODE * list);
static char * pt_check_identifier (PARSER_CONTEXT *parser, PT_NODE *p,
				   const char *str, const int str_size);
static PT_NODE * pt_create_string_literal (PARSER_CONTEXT *parser,
						const PT_TYPE_ENUM char_type,
						const char *str);
static PT_NODE * pt_create_date_value (PARSER_CONTEXT *parser,
				       const PT_TYPE_ENUM type,
				       const char *str);

static bool allow_attribute_ordering;

#if defined (ENABLE_UNUSED_FUNCTION)
int parse_one_statement (int state);
#endif

int g_msg[1024];
int msg_ptr;

int yybuffer_pos;

#define push_msg(a) _push_msg(a, __LINE__)

void _push_msg (int code, int line);
void pop_msg (void);

char *g_query_string;
int g_query_string_len;
PT_NODE *g_last_stmt;
int g_original_buffer_len;


/* 
 * The default YYLTYPE structure is extended so that locations can hold
 * context information
 */
typedef struct YYLTYPE
{

  int first_line;
  int first_column;
  int last_line;
  int last_column;
  int buffer_pos; /* position in the buffer being parsed */

} YYLTYPE;
#define YYLTYPE_IS_DECLARED 1

/*
 * The behavior of location propagation when a rule is matched must
 * take into account the context information. The left-side symbol in a rule
 * will have the same context information as the last symbol from its 
 * right side
 */
#define YYLLOC_DEFAULT(Current, Rhs, N)				        \
    do									\
      if (YYID (N))							\
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	  (Current).buffer_pos   = YYRHSLOC (Rhs, N).buffer_pos;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	  (Current).buffer_pos   = YYRHSLOC (Rhs, 0).buffer_pos;	\
	}								\
    while (YYID (0))

/* 
 * YY_LOCATION_PRINT -- Print the location on the stream.
 * This macro was not mandated originally: define only if we know
 * we won't break user code: when these are the locations we know.  
 */

#define YY_LOCATION_PRINT(File, Loc)			\
    fprintf (File, "%d.%d-%d.%d",			\
	     (Loc).first_line, (Loc).first_column,	\
	     (Loc).last_line,  (Loc).last_column)

%}

%initial-action {yybuffer_pos = 0;}
%locations
%glr-parser
%error_verbose


%union
{
  int number;
  bool boolean;
  PT_NODE *node;
  char *cptr;
  container_2 c2;
  container_3 c3;
  container_4 c4;
  container_10 c10;
}





/* define rule type (number) */
/*{{{*/
%type <boolean> opt_explain
%type <boolean> opt_unique
%type <boolean> opt_encrypt
%type <number> opt_of_inner_left_right
%type <number> opt_class_type
%type <number> opt_of_attr_column
%type <number> isolation_level_name
%type <number> all_distinct
%type <number> of_avg_max_etc
%type <number> of_leading_trailing_both
%type <number> datetime_field
%type <number> opt_with_fullscan
%type <number> comp_op
%type <number> binary_type
%type <number> varchar_type
%type <number> set_type
%type <number> of_container
%type <number> of_global_or_shard
%type <number> of_class_table_type
%type <number> opt_with_grant_option
%type <number> like_op
%type <number> rlike_op
%type <number> null_op
%type <number> is_op
%type <number> in_op
%type <number> between_op
%type <number> opt_varying
%type <number> opt_asc_or_desc
%type <number> opt_with_rollup
%type <number> opt_or_replace
%type <number> column_constraint_def
%type <number> constraint_list
%type <number> opt_nulls_first_or_last
%type <number> opt_persist
%type <number> query_trace_spec
%type <number> opt_trace_output_format
%type <number> opt_if_exists
/*}}}*/

/* define rule type (node) */
/*{{{*/
%type <node> stmt
%type <node> stmt_
%type <node> create_stmt
%type <node> set_stmt
%type <node> get_stmt
%type <node> auth_stmt
%type <node> transaction_stmt
%type <node> alter_stmt
%type <node> alter_clause_list
%type <node> rename_stmt
%type <node> rename_class_pair
%type <node> drop_stmt
%type <node> opt_index_column_name_list
%type <node> index_column_name_list
%type <node> index_column_name_list_sub
%type <node> index_column_name
%type <node> analyze_or_update_statistics_stmt
%type <node> analyze_or_update
%type <node> classes_or_tables
%type <node> opt_level_spec
%type <node> char_string_literal_list
%type <node> join_table_spec
%type <node> table_spec
%type <node> join_condition
%type <node> class_spec_list
%type <node> class_spec
%type <node> class_name
%type <node> class_name_list
%type <node> opt_identifier
%type <node> normal_attribute_list
%type <node> normal_attribute
%type <node> normal_column_attribute
%type <node> query_number_list
%type <node> insert_or_replace_stmt
%type <node> insert_set_stmt
%type <node> replace_set_stmt
%type <node> insert_set_stmt_header
%type <node> opt_attr_list
%type <node> insert_value_clause
%type <node> insert_value_clause_list
%type <node> insert_stmt_value_clause
%type <node> insert_expression_value_clause
%type <node> insert_value_list
%type <node> insert_value
%type <node> update_stmt
%type <node> opt_as_identifier
%type <node> update_assignment_list
%type <node> update_assignment
%type <node> paren_simple_path_id_list
%type <node> delete_stmt
%type <node> author_cmd_list
%type <node> authorized_cmd
%type <node> opt_password
%type <node> opt_attr_def_list
%type <node> opt_group_concat_separator
%type <node> opt_agg_order_by
%type <node> opt_table_option_list
%type <node> table_option_list
%type <node> table_option
%type <node> opt_constraint_id
%type <node> opt_constraint_opt_id
%type <node> of_unique_check
%type <node> unique_constraint
%type <node> check_constraint
%type <node> view_attr_def_list
%type <node> attr_def_list
%type <node> attr_def
%type <node> attr_constraint_def
%type <node> attr_index_def
%type <node> attr_def_one
%type <node> view_attr_def
%type <node> timeout_spec
%type <node> rsql_query_stmt
%type <node> rsql_query
%type <node> rsql_query_without_values_query
%type <node> select_expression
%type <node> select_expression_without_values_query
%type <node> table_op
%type <node> select_or_subquery
%type <node> select_or_subquery_without_values_query
%type <node> select_stmt
%type <node> opt_from_clause
%type <node> select_list
%type <node> alias_enabled_expression_list_top
%type <node> alias_enabled_expression_list
%type <node> alias_enabled_expression_
%type <node> expression_list
%type <node> host_param_input
%type <node> opt_where_clause
%type <node> opt_groupby_clause
%type <node> group_spec_list
%type <node> group_spec
%type <node> opt_having_clause
%type <node> index_name
%type <node> index_name_keylimit
%type <node> opt_using_index_clause
%type <node> index_name_keylimit_list
%type <node> opt_update_orderby_clause
%type <node> opt_orderby_clause
%type <node> sort_spec_list
%type <node> expression_
%type <node> normal_expression
%type <node> expression_strcat
%type <node> expression_add_sub
%type <node> expression_bitshift
%type <node> expression_bitand
%type <node> expression_bitor
%type <node> term
%type <node> factor
%type <node> factor_
%type <node> primary
%type <node> primary_w_collate
%type <node> boolean
%type <node> case_expr
%type <node> opt_else_expr
%type <node> simple_when_clause_list
%type <node> simple_when_clause
%type <node> searched_when_clause_list
%type <node> searched_when_clause
%type <node> extract_expr
%type <node> opt_expression_list
%type <node> search_condition
%type <node> boolean_term
%type <node> boolean_term_is
%type <node> boolean_term_xor
%type <node> boolean_factor
%type <node> predicate
%type <node> predicate_expression
%type <node> range_list
%type <node> range_
%type <node> subquery
%type <node> path_expression
%type <node> opt_prec_1
%type <node> opt_padding
%type <node> literal_
%type <node> literal_w_o_param
%type <node> identifier_list
%type <node> index_column_identifier_list
%type <node> identifier
%type <node> index_column_identifier
%type <node> string_literal_or_input_hv
%type <node> escape_literal
%type <node> char_string_literal
%type <node> char_string
%type <node> bit_string_literal
%type <node> bit_string
%type <node> unsigned_integer
%type <node> unsigned_real
%type <node> date_or_time_literal
%type <node> insert_name_clause
%type <node> replace_name_clause
%type <node> insert_name_clause_header
%type <node> opt_for_search_condition
%type <node> path_header
%type <node> path_id_list
%type <node> path_id
%type <node> simple_path_id
%type <node> simple_path_id_list
%type <node> generic_function
%type <node> generic_function_id
%type <node> pred_lhs
%type <node> reserved_func
%type <node> sort_spec
%type <node> on_class_list
%type <node> from_id_list
%type <node> to_id_list
%type <node> grant_head
%type <node> grant_cmd
%type <node> revoke_cmd
%type <node> search_condition_query
%type <node> search_condition_expression
%type <node> opt_uint_or_host_input
%type <node> opt_select_limit_clause
%type <node> limit_options
%type <node> opt_PK_next_options
%type <node> PK_next_options
%type <node> PK_next_options_sub
%type <node> opt_upd_del_limit_clause
%type <node> on_duplicate_key_update
%type <node> opt_attr_ordering_info
%type <node> show_stmt
%type <node> opt_table_spec_index_hint
%type <node> opt_table_spec_index_hint_list
%type <node> delete_name
%type <node> collation_spec
%type <node> opt_collation
%type <node> opt_for_update_clause
/*}}}*/

/* define rule type (cptr) */
/*{{{*/
%type <cptr> uint_text
/*}}}*/

/* define rule type (container) */
/*{{{*/

%type <c4> isolation_level_spec
%type <c4> opt_constraint_attr_list
%type <c4> constraint_attr_list
%type <c4> constraint_attr

%type <c3> delete_from_using

%type <c2> extended_table_spec_list
%type <c2> opt_of_where_cursor
%type <c2> data_type
%type <c2> primitive_type
%type <c2> opt_prec_2
%type <c2> in_pred_operand
%type <c2> opt_as_identifier_attr_name
%type <c2> insert_assignment_list
%type <c2> expression_queue
/*}}}*/

/* Token define */
/*{{{*/
%token ABSOLUTE_
%token ACTION
%token ADD
%token AFTER
%token ALL
%token ALLOCATE
%token ALTER
%token AND
%token ARE
%token AS
%token ASC
%token ASSERTION
%token ASYNC
%token AT
%token ATTRIBUTE
%token AVG
%token BEGIN_
%token BETWEEN
%token BIGINT
%token BINARY
%token BIT_LENGTH
%token BITSHIFT_LEFT
%token BITSHIFT_RIGHT
%token BOOLEAN_
%token BOTH_
%token BREADTH
%token BY
%token CASCADE
%token CASCADED
%token CASE
%token CAST
%token CATALOG
%token CHANGE
%token CHAR_
%token CHECK
%token CLASS
%token CLASSES
%token CLOSE
%token COALESCE
%token COLLATE
%token COLUMN
%token COMMIT
%token COMP_NULLSAFE_EQ
%token CONNECTION
%token CONSTRAINT
%token CONSTRAINTS
%token CONTINUE
%token CONVERT
%token CORRESPONDING
%token COUNT
%token CREATE
%token CROSS
%token CURRENT
%token CURRENT_DATE
%token CURRENT_DATETIME
%token CURRENT_TIME
%token CURRENT_USER
%token CURSOR
%token CYCLE
%token DATA
%token DATABASE
%token DATA_TYPE_
%token Date
%token DATETIME
%token DAY_
%token DAY_MILLISECOND
%token DAY_SECOND
%token DAY_MINUTE
%token DAY_HOUR
%token DECLARE
%token DEFAULT
%token DEFERRABLE
%token DEFERRED
%token DELETE_
%token DEPTH
%token DESC
%token DESCRIPTOR
%token DIAGNOSTICS
%token DIFFERENCE_
%token DISCONNECT
%token DISTINCT
%token DIV
%token Domain
%token Double
%token DROP
%token DUPLICATE_
%token EACH
%token ELSE
%token ELSEIF
%token END
%token EQUALS
%token ESCAPE
%token EXCEPT
%token EXCEPTION
%token EXEC
%token EXECUTE
%token EXISTS
%token EXTRACT
%token False
%token FETCH
%token File
%token FIRST
%token FLOAT_
%token For
%token FOREIGN
%token FOUND
%token FROM
%token FULL
%token GENERAL
%token GET
%token GLOBAL
%token GO
%token GOTO
%token GRANT
%token GROUP_
%token HAVING
%token HOUR_
%token HOUR_MILLISECOND
%token HOUR_SECOND
%token HOUR_MINUTE
%token IDENTITY
%token IF
%token IMMEDIATE
%token IN_
%token INDEX
%token INDICATOR
%token INITIALLY
%token INNER
%token INOUT
%token INPUT_
%token INSERT
%token INTEGER
%token INTERSECT
%token INTERSECTION
%token INTERVAL
%token INTO
%token IS
%token ISOLATION
%token JOIN
%token KEY
%token KEYLIMIT
%token LANGUAGE
%token LAST
%token LEADING_
%token LEAVE
%token LEFT
%token LEVEL
%token LIKE
%token LIMIT
%token LIST
%token LOOP
%token LOWER
%token MATCH
%token Max
%token MILLISECOND_
%token Min
%token MINUTE_
%token MINUTE_MILLISECOND
%token MINUTE_SECOND
%token MOD
%token MODIFY
%token MODULE
%token MONTH_
%token NA
%token NATIONAL
%token NATURAL
%token NCHAR
%token NEXT
%token NO
%token NONE
%token NOT
%token Null
%token NULLIF
%token NUMERIC
%token OBJECT
%token OCTET_LENGTH
%token OF
%token OFF_
%token ON_
%token OPEN
%token OPTIMIZATION
%token OPTION
%token OR
%token ORDER
%token OUT_
%token OUTER
%token OUTPUT
%token OVERLAPS
%token PARAMETERS
%token PARTIAL
%token PERSIST
%token POSITION
%token PRECISION
%token PRESERVE
%token PRIMARY
%token PRIVILEGES
%token PROMOTE
%token READ
%token REBUILD
%token RECURSIVE
%token REF
%token REFERENCES
%token REFERENCING
%token REGEXP
%token RELATIVE_
%token RENAME
%token REPLACE
%token RESIGNAL
%token RESTRICT
%token RETURN
%token RETURNS
%token REVOKE
%token RIGHT
%token RLIKE
%token ROLE
%token ROLLBACK
%token ROLLUP
%token ROUTINE
%token ROW
%token ROWNUM
%token ROWS
%token SAVEPOINT
%token SCHEMA
%token SCOPE
%token SCROLL
%token SEARCH
%token SECOND_
%token SECOND_MILLISECOND
%token SECTION
%token SELECT
%token SENSITIVE
%token SEQUENCE
%token SEQUENCE_OF
%token SERIALIZABLE
%token SESSION
%token SESSION_USER
%token SET
%token SHARD
%token SIGNAL
%token SIMILAR
%token SIZE_
%token SmallInt
%token SQL
%token SQLCODE
%token SQLERROR
%token SQLEXCEPTION
%token SQLSTATE
%token SQLWARNING
%token STATISTICS
%token String
%token SUBSTRING_
%token SUM
%token SYS_DATE
%token SYS_DATETIME
%token SYS_TIME_
%token SYSTEM_USER
%token TABLE
%token TEMPORARY
%token THEN
%token Time
%token TIMEZONE_HOUR
%token TIMEZONE_MINUTE
%token TO
%token TRAILING_
%token TRANSACTION
%token TRANSLATE
%token TRANSLATION
%token TRIGGER
%token TRIM
%token True
%token TRUNCATE
%token Union
%token UNIQUE
%token UNKNOWN
%token UNTERMINATED_STRING
%token UNTERMINATED_IDENTIFIER
%token UPDATE
%token UPPER
%token USAGE
%token USE
%token USER
%token USING
%token VALUE
%token VALUES
%token VARBINARY
%token VARCHAR
%token VARYING
%token VIEW
%token WHEN
%token WHENEVER
%token WHERE
%token WHILE
%token WITH
%token WITHOUT
%token WORK
%token WRITE
%token XOR
%token YEAR_
%token YEAR_MONTH
%token ZONE

%token RIGHT_ARROW
%token STRCAT
%token COMP_NOT_EQ
%token COMP_GE
%token COMP_LE
%token PARAM_HEADER

%token <cptr> ACCESS
%token <cptr> ADDDATE
%token <cptr> ANALYZE
%token <cptr> ARCHIVE
%token <cptr> CACHE
%token <cptr> CAPACITY
%token <cptr> CHARACTER_SET_
%token <cptr> CHARSET
%token <cptr> CHR
%token <cptr> COLLATION
%token <cptr> COLUMNS
%token <cptr> COMMITTED
%token <cptr> COST
%token <cptr> DATE_ADD
%token <cptr> DATE_SUB
%token <cptr> DECREMENT
%token <cptr> ELT
%token <cptr> ENCRYPT
%token <cptr> EXPLAIN
%token <cptr> FULLSCAN
%token <cptr> GE_INF_
%token <cptr> GE_LE_
%token <cptr> GE_LT_
%token <cptr> GRANTS
%token <cptr> GROUP_CONCAT
%token <cptr> GROUPS
%token <cptr> GT_INF_
%token <cptr> GT_LE_
%token <cptr> GT_LT_
%token <cptr> HEADER
%token <cptr> HEAP
%token <cptr> IFNULL
%token <cptr> INCREMENT
%token <cptr> INDEXES
%token <cptr> INDEX_PREFIX
%token <cptr> INF_LE_
%token <cptr> INF_LT_
%token <cptr> INSTANCES
%token <cptr> ISNULL
%token <cptr> KEYS
%token <cptr> JAVA
%token <cptr> JSON
%token <cptr> LCASE
%token <cptr> LOCK_
%token <cptr> LOG
%token <cptr> MAXIMUM
%token <cptr> MEMBERS
%token <cptr> MINVALUE
%token <cptr> NAME
%token <cptr> NOCACHE
%token <cptr> NOMAXVALUE
%token <cptr> NOMINVALUE
%token <cptr> NULLS
%token <cptr> OFFSET
%token <cptr> OWNER
%token <cptr> PAGE
%token <cptr> PASSWORD
%token <cptr> QUARTER
%token <cptr> RANGE_
%token <cptr> REMOVE
%token <cptr> REORGANIZE
%token <cptr> REPEATABLE
%token <cptr> SEPARATOR
%token <cptr> SHOW
%token <cptr> SLOTS
%token <cptr> SLOTTED
%token <cptr> STABILITY
%token <cptr> STATEMENT
%token <cptr> STATUS
%token <cptr> STDDEV
%token <cptr> STDDEV_POP
%token <cptr> STDDEV_SAMP
%token <cptr> STR_TO_DATE
%token <cptr> SUBDATE
%token <cptr> SYSTEM
%token <cptr> TABLES
%token <cptr> TEXT
%token <cptr> TIMEOUT
%token <cptr> TRACE
%token <cptr> UCASE
%token <cptr> UNCOMMITTED
%token <cptr> VAR_POP
%token <cptr> VAR_SAMP
%token <cptr> VARIANCE
%token <cptr> VOLUME
%token <cptr> WEEK


%token <cptr> IdName
%token <cptr> BracketDelimitedIdName
%token <cptr> BacktickDelimitedIdName
%token <cptr> DelimitedIdName
%token <cptr> UNSIGNED_INTEGER
%token <cptr> UNSIGNED_REAL
%token <cptr> CHAR_STRING
%token <cptr> NCHAR_STRING
%token <cptr> BIT_STRING
%token <cptr> HEX_STRING
%token <cptr> CPP_STYLE_HINT
%token <cptr> C_STYLE_HINT
%token <cptr> SQL_STYLE_HINT

/*}}}*/

%%

stmt_done
	: stmt ';'
		{{

			if ($1 != NULL)
			  {
			    if (!parser_statement_OK)
			      {
			        parser_statement_OK = 1;
			      }

			    pt_push (this_parser, $1);

			#ifdef PARSER_DEBUG
			    printf ("node: %s\n", parser_print_tree (this_parser, $1));
			#endif
			  }

		DBG_PRINT}}
	| stmt
		{{

			if ($1 != NULL)
			  {
			    if (!parser_statement_OK)
			      {
			        parser_statement_OK = 1;
			      }

			    pt_push (this_parser, $1);

			#ifdef PARSER_DEBUG
			    printf ("node: %s\n", parser_print_tree (this_parser, $1));
			#endif
			  }

		DBG_PRINT}}
	;



stmt
	:
		{{
			msg_ptr = 0;

			if (this_parser->original_buffer)
			  {
			    int pos = @$.buffer_pos;

			    if (g_original_buffer_len == 0)
			      {
				g_original_buffer_len = strlen (this_parser->original_buffer);
			      }

			    /* below assert & defense code is for yybuffer_pos mismatch
			     * (like unput in lexer and do not modify yybuffer_pos)
			     */
			    assert (pos <= g_original_buffer_len);

			    if (pos > g_original_buffer_len)
			      {
				pos = g_original_buffer_len;
			      }

			    g_query_string = this_parser->original_buffer + pos;
			
			    /* set query length of previous statement */
			    if (g_last_stmt)
			      {
				/* remove ';' character in user sql text */
				int len = g_query_string - g_last_stmt->sql_user_text - 1; 
				g_last_stmt->sql_user_text_len = len;
				g_query_string_len = len;
			      }
			    
			    while (char_isspace (*g_query_string))
			      {
			        g_query_string++;
			      }
			  }

		DBG_PRINT}}
		{{

			parser_initialize_parser_context ();

			parser_statement_OK = 1;
			parser_instnum_check = 0;
			parser_groupbynum_check = 0;
			parser_orderbynum_check = 0;

			parser_subquery_check = 1;
			parser_hostvar_check = 1;

			parser_select_level = -1;

			parser_within_join_condition = 0;

			parser_save_and_set_si_datetime (false);

			allow_attribute_ordering = false;

		DBG_PRINT}}
	stmt_
		{{

			#ifdef PARSER_DEBUG
			if (msg_ptr == 0)
			  printf ("Good!!!\n");
			#endif

			if (msg_ptr > 0)
			  {
			    rsql_yyerror (NULL);
			  }

			/* set query length of last statement (but do it every time)
                         * Not last statement's length will be updated later.
                         */
			if (this_parser->original_buffer)
			  {
			    int pos = @$.buffer_pos;
			    PT_NODE *node = $3;
			  
			    if (g_original_buffer_len == 0)
			      {
				g_original_buffer_len = strlen (this_parser->original_buffer);
			      }

			    /* below assert & defense code is for yybuffer_pos mismatch
			     * (like unput in lexer and do not modify yybuffer_pos)
			     */
			    assert (pos <= g_original_buffer_len);

			    if (pos > g_original_buffer_len)
			      {
				pos = g_original_buffer_len;
			      }

			    if (node)
			      {
				char *curr_ptr = this_parser->original_buffer + pos;
				int len = curr_ptr - g_query_string;
				node->sql_user_text_len = len;
				g_query_string_len = len;
			      }

			    g_last_stmt = node;
			  }

		DBG_PRINT}}
		{{

			PT_NODE *node = $3;

			if (node)
			  {
			    node->si_datetime = (parser_si_datetime == true) ? 1 : 0;
			  }

			parser_restore_si_datetime ();

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;
stmt_
	: create_stmt
		{ $$ = $1; }
	| alter_stmt
		{ $$ = $1; }
	| rename_stmt
		{ $$ = $1; }
	| analyze_or_update_statistics_stmt
		{ $$ = $1; }
	| drop_stmt
		{ $$ = $1; }
	| rsql_query_stmt
		{ $$ = $1; }
	| insert_or_replace_stmt
		{ $$ = $1; }
	| update_stmt
		{ $$ = $1; }
	| delete_stmt
		{ $$ = $1; }
        | show_stmt
                { $$ = $1; }
	| auth_stmt
		{ $$ = $1; }
	| transaction_stmt
		{ $$ = $1; }
	| set_stmt
		{ $$ = $1; }
	| get_stmt
		{ $$ = $1; }
	| DATA_TYPE_ data_type
		{{

			PT_NODE *dt, *set_dt;
			PT_TYPE_ENUM typ;

			typ = TO_NUMBER (CONTAINER_AT_0 ($2));
			dt = CONTAINER_AT_1 ($2);

			if (!dt)
			  {
			    dt = parser_new_node (this_parser, PT_DATA_TYPE);
			    if (dt)
			      {
				dt->type_enum = typ;
				dt->data_type = NULL;
			      }
			  }
			else
			  {
			    if (PT_IS_COLLECTION_TYPE (typ))
			      {
				set_dt = parser_new_node (this_parser, PT_DATA_TYPE);
				if (set_dt)
				  {
				    set_dt->type_enum = typ;
				    set_dt->data_type = dt;
				    dt = set_dt;
				  }
			      }
			  }

			if (PT_HAS_COLLATION (typ))
			  {
			    dt->info.data_type.collation_id = LANG_SYS_COLLATION;
			  }

			$$ = dt;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

set_stmt
	: SET OPTIMIZATION
		{ push_msg(MSGCAT_SYNTAX_INVALID_SET_OPT_LEVEL); }
	  LEVEL opt_of_to_eq opt_level_spec
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_SET_OPT_LVL);
			if (node)
			  {
			    node->info.set_opt_lvl.option = PT_OPT_LVL;
			    node->info.set_opt_lvl.val = $6;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SET OPTIMIZATION
		{ push_msg(MSGCAT_SYNTAX_INVALID_SET_OPT_COST); }
	  COST opt_of char_string_literal opt_of_to_eq literal_
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_SET_OPT_LVL);
			if (node)
			  {
			    node->info.set_opt_lvl.option = PT_OPT_COST;
			    if ($6)
			      ($6)->next = $8;
			    node->info.set_opt_lvl.val = $6;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SET
		{ push_msg(MSGCAT_SYNTAX_INVALID_SET_SYS_PARAM); }
	  opt_persist SYSTEM PARAMETERS char_string_literal_list
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_SET_SYS_PARAMS);
			if (node)
                          {
			    node->info.set_sys_params.persist = $3;
			    node->info.set_sys_params.val = $6;
                          }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SET TRACE query_trace_spec opt_trace_output_format
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_QUERY_TRACE);

			if (node)
			  {
			    node->info.trace.on_off = $3;
			    node->info.trace.format = $4;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_persist
        : /* empty */
                {{
                        $$ = PT_PERSIST_OFF;
                DBG_PRINT}}
        | PERSIST
                {{
                        $$ = PT_PERSIST_ON;
                DBG_PRINT}}
        ;

query_trace_spec
	: ON_
		{{
			$$ = PT_TRACE_ON;
		DBG_PRINT}}
	| OFF_
		{{
			$$ = PT_TRACE_OFF;
		DBG_PRINT}}
	;

opt_trace_output_format
	: /* empty */
		{{
			$$ = PT_TRACE_FORMAT_TEXT;
		DBG_PRINT}}
	| OUTPUT TEXT
		{{
			$$ = PT_TRACE_FORMAT_TEXT;
		DBG_PRINT}}
	| OUTPUT JSON
		{{
			$$ = PT_TRACE_FORMAT_JSON;
		DBG_PRINT}}
	;

get_stmt
	: GET OPTIMIZATION
		{ push_msg(MSGCAT_SYNTAX_INVALID_GET_OPT_LEVEL); }
	  LEVEL
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_GET_OPT_LVL);
			if (node)
			  {
			    node->info.get_opt_lvl.option = PT_OPT_LVL;
			    node->info.get_opt_lvl.args = NULL;
			  }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GET OPTIMIZATION
		{ push_msg(MSGCAT_SYNTAX_INVALID_GET_OPT_COST); }
	  COST opt_of char_string_literal
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_GET_OPT_LVL);
			if (node)
			  {
			    node->info.get_opt_lvl.option = PT_OPT_COST;
			    node->info.get_opt_lvl.args = $6;
			  }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GET TRANSACTION
		{ push_msg(MSGCAT_SYNTAX_INVALID_GET_TRAN_ISOL); }
	  ISOLATION LEVEL
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_GET_XACTION);

			if (node)
			  {
			    node->info.get_xaction.option = PT_ISOLATION_LEVEL;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GET TRANSACTION
		{ push_msg(MSGCAT_SYNTAX_INVALID_GET_TRAN_LOCK); }
	  LOCK_ TIMEOUT
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_GET_XACTION);

			if (node)
			  {
			    node->info.get_xaction.option = PT_LOCK_TIMEOUT;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;




create_stmt
        : CREATE                                        /* 1 */
                {                                       /* 2 */
                        PT_NODE* qc = parser_new_node(this_parser, PT_CREATE_ENTITY);
                        parser_push_hint_node(qc);
                }
          opt_hint_list                                 /* 3 */
          of_global_or_shard                            /* 4 */
          of_class_table_type                           /* 5 */
          class_name                                    /* 6 */
          opt_attr_def_list                             /* 7 */
          opt_table_option_list                         /* 8 */
                {{

                        PT_NODE *qc = parser_pop_hint_node ();
                        PARSER_SAVE_ERR_CONTEXT (qc, @$.buffer_pos)

                        if (qc)
                          {
                            qc->info.create_entity.is_shard = $4;
                            qc->info.create_entity.entity_type = (PT_MISC_TYPE) $5;
                            qc->info.create_entity.entity_name = $6;
                            qc->info.create_entity.attr_def_list = $7;
                            qc->info.create_entity.table_option_list = $8;

                            if (qc->info.create_entity.is_shard)
                              {
                                /* try shard table */
                                if (qc->info.create_entity.table_option_list == NULL)
                                  {
                                    PT_ERRORf (this_parser, qc,
		                               "check syntax at %s, expecting 'SHARD BY' expression.",
		                               pt_short_print (this_parser, qc));
                                  }
                              }
                            else
                              {
                                /* try gloal table */
                                if (qc->info.create_entity.table_option_list != NULL)
                                  {
                                    PT_ERRORf (this_parser, qc,
		                               "check syntax at %s, illegal 'SHARD BY' expression.",
		                               pt_short_print (this_parser, qc));
                                  }
                              }

                            pt_gather_constraints (this_parser, qc);
                          }

                        $$ = qc;
                        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
	| CREATE 					/* 1 */
	  opt_or_replace				/* 2 */
	  VIEW 						/* 3 */
	  class_name 					/* 4 */
          '('						/* 5 */
	  view_attr_def_list				/* 6 */
          ')'						/* 7 */
          AS						/* 8 */
	  rsql_query					/* 9 */
		{{

			PT_NODE *qc = parser_new_node (this_parser, PT_CREATE_ENTITY);

			if (qc)
			  {
			    qc->info.create_entity.or_replace = $2;

			    qc->info.create_entity.entity_name = $4;
			    qc->info.create_entity.entity_type = PT_VCLASS;

			    qc->info.create_entity.attr_def_list = $6;
			    qc->info.create_entity.as_query_list = $9;

			    pt_gather_constraints (this_parser, qc);
			  }

			$$ = qc;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CREATE					/*  1 */
		{					/*  2 */
			PT_NODE* node = parser_new_node (this_parser, PT_CREATE_INDEX);
			parser_push_hint_node (node);
			push_msg (MSGCAT_SYNTAX_INVALID_CREATE_INDEX);
		}
	  opt_hint_list					/*  3 */
	  opt_unique					/*  4 */
	  INDEX						/*  5 */
		{ pop_msg(); }  			/*  6 */
	  identifier				        /*  7 */
	  ON_						/*  8 */
	  class_name					/*  9 */
	  index_column_name_list			/* 10 */
	{{

			PT_NODE *node = parser_pop_hint_node ();
			PT_NODE *ocs = parser_new_node(this_parser, PT_SPEC);
			PARSER_SAVE_ERR_CONTEXT (node, @$.buffer_pos)

			if (node && ocs)
			  {
			    PT_NODE *col, *temp;
			    int arg_count = 0, prefix_col_count = 0;

			    ocs->info.spec.entity_name = $9;
			    ocs->info.spec.meta_class = PT_CLASS;

			    PARSER_SAVE_ERR_CONTEXT (ocs, @9.buffer_pos)

			    node->info.index.indexed_class = ocs;
			    node->info.index.unique = $4;
			    node->info.index.index_name = $7;
			    if (node->info.index.index_name)
			      {
				node->info.index.index_name->info.name.meta_class = PT_INDEX_NAME;
			      }

			    col = $10;
			    for (temp = col; temp != NULL; temp = temp->next)
			      {
			        if (temp->info.sort_spec.expr->node_type == PT_EXPR)
			          {
			            /* Not allowed function index. */
                                    assert (false);
			            PT_ERRORm (this_parser, node,
			                       MSGCAT_SET_PARSER_SYNTAX,
			                       MSGCAT_SYNTAX_INVALID_CREATE_INDEX);
			          }
			      }
			      
			    prefix_col_count =
				parser_count_prefix_columns (col, &arg_count);
			    
			    if (prefix_col_count > 1 ||
				(prefix_col_count == 1 && arg_count > 1))
			      {
				PT_ERRORm (this_parser, node,
					   MSGCAT_SET_PARSER_SEMANTIC,
					   MSGCAT_SEMANTIC_MULTICOL_PREFIX_INDX_NOT_ALLOWED);
			      }
			    else if (arg_count == 1
                                     && (prefix_col_count == 1
				         || col->info.sort_spec.expr->node_type == PT_FUNCTION))
			      {
				    PT_ERRORmf (this_parser, col->info.sort_spec.expr,
					        MSGCAT_SET_PARSER_SEMANTIC,
					        MSGCAT_SEMANTIC_FUNCTION_CANNOT_BE_USED_FOR_INDEX,
					        pt_show_function (col->info.sort_spec.expr->info.function.function_type));
			      }
			     node->info.index.column_names = col;
			  }
		      $$ = node;
		      PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CREATE
		{ push_msg(MSGCAT_SYNTAX_INVALID_CREATE_USER); }
	  USER
	  identifier
	  opt_password
	  opt_encrypt
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_CREATE_USER);

			if (node)
			  {
			    node->info.create_user.user_name = $4;
			    node->info.create_user.password = $5;
			    node->info.create_user.is_encrypted = $6;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CREATE IdName
		{{

			push_msg (MSGCAT_SYNTAX_INVALID_CREATE);
			rsql_yyerror_explicit (@2.first_line, @2.first_column);

		DBG_PRINT}}
	| CREATE					/* 1 */
		{					/* 2 */
			PT_NODE* qc = parser_new_node(this_parser, PT_CREATE_ENTITY);
			parser_push_hint_node(qc);
		}
	  opt_hint_list					/* 3 */
          of_global_or_shard                            /* 4 */
	  of_class_table_type				/* 5 */
	  class_name					/* 6 */
	  LIKE						/* 7 */
	  class_name					/* 8 */
		{{

			PT_NODE *qc = parser_pop_hint_node ();

			if (qc)
			  {
                   qc->info.create_entity.is_shard = $4;
			    qc->info.create_entity.entity_name = $6;
			    qc->info.create_entity.entity_type = PT_CLASS;
			    qc->info.create_entity.create_like = $8;
			  }

			$$ = qc;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CREATE					/* 1 */
		{					/* 2 */
			PT_NODE* qc = parser_new_node(this_parser, PT_CREATE_ENTITY);
			parser_push_hint_node(qc);
		}
	  opt_hint_list					/* 3 */
          of_global_or_shard                            /* 4 */
	  of_class_table_type				/* 5 */
	  class_name					/* 6 */
	  '('						/* 7 */
	  LIKE						/* 8 */
	  class_name					/* 9 */
	  ')'						/* 10 */
		{{

			PT_NODE *qc = parser_pop_hint_node ();

			if (qc)
			  {
                   qc->info.create_entity.is_shard = $4;
			    qc->info.create_entity.entity_name = $6;
			    qc->info.create_entity.entity_type = PT_CLASS;
			    qc->info.create_entity.create_like = $9;
			  }

			$$ = qc;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

alter_stmt
	: ALTER						/* 1 */
		{					/* 2 */
			PT_NODE* node = parser_new_node(this_parser, PT_ALTER);
			parser_push_hint_node(node);
		}
	  opt_hint_list					/* 3 */
	  opt_class_type				/* 4 */
	  class_name					/* 5 */
	  alter_clause_list				/* 6 */
		{{

			PT_NODE *node = NULL;
			int entity_type = ($4 == PT_EMPTY ? PT_MISC_DUMMY : $4);

			for (node = $6; node != NULL; node = node->next)
			  {
			    node->info.alter.entity_type = entity_type;
			    node->info.alter.entity_name = parser_copy_tree (this_parser, $5);
			    pt_gather_constraints (this_parser, node);
			    if (node->info.alter.code == PT_RENAME_ENTITY)
			      {
				node->info.alter.alter_clause.rename.element_type = entity_type;
				/* We can get the original name from info.alter.entity_name */
				node->info.alter.alter_clause.rename.old_name = NULL;
			      }
			  }
			parser_free_tree (this_parser, $5);

			$$ = $6;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ALTER
	  USER
	  identifier
	  PASSWORD
	  char_string_literal
	  opt_encrypt
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_ALTER_USER);

			if (node)
			  {
			    node->info.alter_user.user_name = $3;
			    node->info.alter_user.password = $5;
			    node->info.alter_user.is_encrypted = $6;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ALTER						/*  1 */
		{					/*  2 */
			PT_NODE* node = parser_new_node(this_parser, PT_ALTER_INDEX);
			parser_push_hint_node(node);
		}
	  opt_hint_list					/*  3 */
	  INDEX						/*  4 */
	  identifier					/*  5 */
	  ON_						/*  6 */
	  class_name					/*  7 */
	  opt_index_column_name_list			/*  8 */
	  REBUILD					/*  9 */
		{{

			PT_NODE *node = parser_pop_hint_node ();
			PT_NODE *ocs = parser_new_node(this_parser, PT_SPEC);

			if (node && ocs)
			  {
			    PT_NODE *col, *temp;
			    node->info.index.unique = false; /* unused at ALTER */
			    node->info.index.index_name = $5;
			    if (node->info.index.index_name)
			      {
				node->info.index.index_name->info.name.meta_class = PT_INDEX_NAME;
			      }

			    ocs->info.spec.entity_name = $7;
			    ocs->info.spec.meta_class = PT_CLASS;

			    node->info.index.indexed_class = ocs;
			    col = $8;
			    for (temp = col; temp != NULL; temp = temp->next)
			      {
			        if (temp->info.sort_spec.expr->node_type == PT_EXPR)
			          {
			            /* Not allowed function index. */
                                    assert (false);
			            PT_ERRORm (this_parser, node,
			                       MSGCAT_SET_PARSER_SYNTAX,
			                       MSGCAT_SYNTAX_INVALID_CREATE_INDEX);
			          }
			      }

			    node->info.index.column_names = col;

			    $$ = node;
			    PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			  }

		DBG_PRINT}}
	;
	
alter_clause_list
	: /* The first node in the list is the one that was pushed for hints. */
		{

			PT_NODE *node = parser_pop_hint_node ();
			parser_save_alter_node (node);
		}
	 alter_clause_for_alter_list
		{{

			$$ = parser_get_alter_node ();
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

rename_stmt
	: RENAME opt_class_type rename_class_pair
		{{

			PT_NODE *node = NULL;
			int entity_type = ($2 == PT_EMPTY ? PT_CLASS : $2);

			for (node = $3; node != NULL; node = node->next)
			  {
			    node->info.rename.entity_type = entity_type;
			  }

			$$ = $3;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

rename_class_pair
	:  class_name as_or_to class_name
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_RENAME);
			if (node)
			  {
			    node->info.rename.old_name = $1;
			    node->info.rename.new_name = $3;
			    node->info.rename.entity_type = PT_CLASS;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

as_or_to
	: AS
	| TO
	;

drop_stmt
	: DROP opt_class_type opt_if_exists class_spec_list
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_DROP);
			if (node)
			  {
			    node->info.drop.if_exists = ($3 == 1);
			    node->info.drop.spec_list = $4;

			    if ($2 == PT_EMPTY)
			      node->info.drop.entity_type = PT_MISC_DUMMY;
			    else
			      node->info.drop.entity_type = $2;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DROP						/* 1 */
		{					/* 2 */
			PT_NODE* node = parser_new_node(this_parser, PT_DROP_INDEX);
			parser_push_hint_node(node);
		}
	  opt_hint_list					/* 3 */
	  INDEX						/* 4 */
	  identifier					/* 5 */
	  ON_						/* 6 */
	  class_name					/* 7 */
	  opt_index_column_name_list			/* 8 */
		{{

			PT_NODE *node = parser_pop_hint_node ();
			PT_NODE *ocs = parser_new_node(this_parser, PT_SPEC);

			if (node && ocs)
			  {
			    PT_NODE *col, *temp;
			    node->info.index.unique = false; /* unused at DROP */
			    node->info.index.index_name = $5;
			    if (node->info.index.index_name)
			      {
				node->info.index.index_name->info.name.meta_class = PT_INDEX_NAME;
			      }

			    ocs->info.spec.entity_name = $7;
			    ocs->info.spec.meta_class = PT_CLASS;
			    PARSER_SAVE_ERR_CONTEXT (ocs, @7.buffer_pos)
			    node->info.index.indexed_class = ocs;

			    col = $8;
			    for (temp = col; temp != NULL; temp = temp->next)
			      {
			        if (temp->info.sort_spec.expr->node_type == PT_EXPR)
			          {
			            /* Not allowed function index. */
                                    assert (false);
			            PT_ERRORm (this_parser, node,
			                           MSGCAT_SET_PARSER_SYNTAX,
			                           MSGCAT_SYNTAX_INVALID_CREATE_INDEX);
			          }
			      }
			    node->info.index.column_names = col;

			    $$ = node;
			    PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			  }

		DBG_PRINT}}
	| DROP USER identifier
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_DROP_USER);

			if (node)
			  {
			    node->info.drop_user.user_name = $3;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_explain
        : /* empty */
                {{

                        $$ = false;

                DBG_PRINT}}
        | EXPLAIN
                {{

                        $$ = true;

                DBG_PRINT}}
        ;

opt_unique
	: /* empty */
		{{

			$$ = false;

		DBG_PRINT}}
	| UNIQUE
		{{

			$$ = true;

		DBG_PRINT}}
	;

opt_index_column_name_list
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| index_column_name_list
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

index_column_name_list
	: '(' index_column_name_list_sub ')'
		{{

		      $$ = $2;
		      PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

index_column_name_list_sub
        : index_column_name_list_sub ',' index_column_name
                {{

                      $$ = parser_make_link ($1, $3);
                      PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
        | index_column_name
                {{

                      $$ = $1;
                      PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
        ;

index_column_name
        : identifier
                {{

                      PT_NODE *node = parser_new_node (this_parser, PT_SORT_SPEC);
                      if (node)
                        {
                          node->info.sort_spec.asc_or_desc = PT_ASC;
                          node->info.sort_spec.expr = $1;
                        }
                      $$ = node;
                      PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
        | identifier ASC
                {{

                      PT_NODE *node = parser_new_node (this_parser, PT_SORT_SPEC);
                      if (node)
                        {
                          node->info.sort_spec.asc_or_desc = PT_ASC;
                          node->info.sort_spec.expr = $1;
                        }
                      $$ = node;
                      PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
        | identifier DESC
                {{

                      PT_NODE *node = parser_new_node (this_parser, PT_SORT_SPEC);
                      if (node)
                        {
                          node->info.sort_spec.asc_or_desc = PT_DESC;
                          node->info.sort_spec.expr = $1;
                        }
                      $$ = node;
                      PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
        ;

analyze_or_update_statistics_stmt
	: analyze_or_update STATISTICS ON_ class_name_list opt_with_fullscan
		{{

			PT_NODE *ups = parser_new_node (this_parser, PT_UPDATE_STATS);
			if (ups)
			  {
			    ups->info.update_stats.update_stats = $1;
			    ups->info.update_stats.class_list = $4;
			    ups->info.update_stats.all_classes = 0;
			    ups->info.update_stats.with_fullscan = $5;
			  }
			$$ = ups;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| analyze_or_update STATISTICS ON_ ALL classes_or_tables opt_with_fullscan
		{{

			PT_NODE *ups = parser_new_node (this_parser, PT_UPDATE_STATS);
			if (ups)
			  {
			    ups->info.update_stats.update_stats = $1;
			    ups->info.update_stats.class_list = NULL;
			    ups->info.update_stats.all_classes = 1;
			    ups->info.update_stats.with_fullscan = $6;
			  }
			$$ = ups;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| analyze_or_update STATISTICS ON_ CATALOG classes_or_tables opt_with_fullscan
		{{

			PT_NODE *ups = parser_new_node (this_parser, PT_UPDATE_STATS);
			if (ups)
			  {
			    ups->info.update_stats.update_stats = $1;
			    ups->info.update_stats.class_list = NULL;
			    ups->info.update_stats.all_classes = -1;
			    ups->info.update_stats.with_fullscan = $6;
			  }
			$$ = ups;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

analyze_or_update
        : ANALYZE
                {{

                        $$ = 0;

                DBG_PRINT}}
        | UPDATE
                {{

                        $$ = 1;

                DBG_PRINT}}
        ;

classes_or_tables
        : CLASSES
        | TABLES
        ;

opt_with_fullscan
        : /* empty */
                {{

                        $$ = 0;

                DBG_PRINT}}
        | WITH FULLSCAN
                {{

                        $$ = 1;

                DBG_PRINT}}
        ;

opt_of_to_eq
	: /* empty */
	| TO
	| '='
	;

opt_level_spec
	: ON_
		{{

			PT_NODE *val = parser_new_node (this_parser, PT_VALUE);
			if (val)
                          {
			    val->type_enum = PT_TYPE_INTEGER;
			    val->info.value.data_value.i = 1;
                          }
			$$ = val;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| OFF_
		{{

			PT_NODE *val = parser_new_node (this_parser, PT_VALUE);
			if (val)
                          {
			    val->type_enum = PT_TYPE_INTEGER;
			    val->info.value.data_value.i = 0;
                          }
			$$ = val;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| unsigned_integer
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| host_param_input
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

char_string_literal_list
	: char_string_literal_list ',' char_string_literal
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| char_string_literal
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

extended_table_spec_list
	: extended_table_spec_list ',' table_spec
		{{

			container_2 ctn;
			PT_NODE *n1 = CONTAINER_AT_0 ($1);
			PT_NODE *n2 = $3;
			int number = TO_NUMBER (CONTAINER_AT_1 ($1));
			SET_CONTAINER_2 (ctn, parser_make_link (n1, n2), FROM_NUMBER (number));
			$$ = ctn;

		DBG_PRINT}}
	| extended_table_spec_list join_table_spec
		{{

			container_2 ctn;
			PT_NODE *n1 = CONTAINER_AT_0 ($1);
			PT_NODE *n2 = $2;
			SET_CONTAINER_2 (ctn, parser_make_link (n1, n2), FROM_NUMBER (1));
			$$ = ctn;

		DBG_PRINT}}
	| table_spec
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, $1, FROM_NUMBER (0));
			$$ = ctn;

		DBG_PRINT}}
	;

join_table_spec
	: CROSS JOIN table_spec
		{{

			PT_NODE *sopt = $3;
			if (sopt)
			  sopt->info.spec.join_type = PT_JOIN_CROSS;
			$$ = sopt;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| opt_of_inner_left_right JOIN table_spec join_condition
		{{

			PT_NODE *sopt = $3;
			bool natural = false;

			if (sopt)
			  {
			    sopt->info.spec.natural = natural;
			    sopt->info.spec.join_type = $1;
			    sopt->info.spec.on_cond = $4;
			  }
			$$ = sopt;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

join_condition
	: ON_
		{{
			parser_save_and_set_wjc (1);
			parser_save_and_set_ic (1);
		DBG_PRINT}}
	  search_condition
		{{
			PT_NODE *condition = $3;
			bool instnum_flag = false;
			
			parser_restore_wjc ();
			parser_restore_ic ();
			
			(void) parser_walk_tree (this_parser, condition,
									 pt_check_instnum_pre, NULL,
									 pt_check_instnum_post, &instnum_flag);
			if (instnum_flag)
			  {
			    PT_ERRORmf(this_parser, condition, MSGCAT_SET_PARSER_SEMANTIC,
					       MSGCAT_SEMANTIC_EXPR_NOT_ALLOWED_IN_JOIN_COND,
					       "INST_NUM()/ROWNUM");
			  }
			
			$$ = condition;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}


	;

opt_of_inner_left_right
	: /* empty */
		{{

			$$ = PT_JOIN_INNER;

		DBG_PRINT}}
	| INNER opt_outer
		{{

			$$ = PT_JOIN_INNER;

		DBG_PRINT}}
	| LEFT opt_outer
		{{

			$$ = PT_JOIN_LEFT_OUTER;

		DBG_PRINT}}
	| RIGHT opt_outer
		{{

			$$ = PT_JOIN_RIGHT_OUTER;

		DBG_PRINT}}
	;

opt_outer
	: /* empty */
	| OUTER
	;

table_spec
	: class_spec opt_as_identifier_attr_name opt_table_spec_index_hint_list
		{{
			PT_NODE *range_var = NULL;
			PT_NODE *ent = $1;
			if (ent)
			  {
			    PT_NODE *stmt = parser_is_hint_node_empty () ? NULL : parser_pop_hint_node ();

			    range_var = CONTAINER_AT_0 ($2);
			    if (range_var != NULL)
			      {
					if (ent->info.spec.range_var != NULL)
					  {
						parser_free_tree (this_parser,
										  ent->info.spec.range_var);
					  }
					ent->info.spec.range_var = CONTAINER_AT_0 ($2);
			      }
			    ent->info.spec.as_attr_list = CONTAINER_AT_1 ($2);

			    if ($3)
			      {
				PT_NODE *hint = NULL, *alias = NULL;
				char *qualifier_name = NULL;

				/* Get qualifier */
				alias = CONTAINER_AT_0 ($2);
				if (alias)
				  {
				    qualifier_name = alias->info.name.original;
				  }
				else if (ent->info.spec.entity_name != NULL)
				  {
				    qualifier_name = ent->info.spec.entity_name->info.name.original;
				  }

				/* Resolve table spec index hint names */
				hint = $3;
				while (hint && qualifier_name)
				  {
				    hint->info.name.resolved = 
				      pt_append_string (this_parser, NULL, qualifier_name);

				    hint = hint->next;
				  }

				/* This is an index hint inside a table_spec. Copy index
				   name list to USING INDEX clause */
				if (stmt)
				  {
				    /* copy to using_index */
				    switch (stmt->node_type)
				      {
					case PT_SELECT:
					  stmt->info.query.q.select.using_index =
					    parser_make_link ($3, stmt->info.query.q.select.using_index);
					break;

					case PT_DELETE:
					  stmt->info.delete_.using_index =
					    parser_make_link ($3, stmt->info.delete_.using_index);
					break;

					case PT_UPDATE:
					  stmt->info.update.using_index =
					    parser_make_link ($3, stmt->info.update.using_index);
					  break;

					default:
					  /* if index hint has been specified in
					   * table_spec clause and statement is not
					   * SELECT, UPDATE or DELETE raise error
					   */
					  PT_ERRORm (this_parser, ent, 
					    MSGCAT_SET_PARSER_SYNTAX,
					    MSGCAT_SYNTAX_INVALID_INDEX_HINT);
					break;
				      }
				  }
			      }

			    if (stmt)
			      {
				/* push back node */
				parser_push_hint_node (stmt);
			      }

			  }

			$$ = ent;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| subquery opt_as_identifier_attr_name
		{{

			PT_NODE *ent = parser_new_node (this_parser, PT_SPEC);
			if (ent)
			  {
			    PT_NODE *stmt = parser_pop_hint_node ();
			    if (stmt && (stmt->node_type == PT_DELETE || stmt->node_type == PT_UPDATE))
			      {
			        PT_NODE *sel = $1;
				if (sel && sel->node_type == PT_SELECT)
				  {
				    PT_SELECT_INFO_CLEAR_FLAG (sel, PT_SELECT_INFO_DUMMY);
				  }
			      }
			    parser_push_hint_node (stmt);

			    ent->info.spec.derived_table = $1;
			    ent->info.spec.derived_table_type = PT_IS_SUBQUERY;

			    ent->info.spec.range_var = CONTAINER_AT_0 ($2);
			    ent->info.spec.as_attr_list = CONTAINER_AT_1 ($2);

			    parser_remove_dummy_select (&ent);
			  }
			$$ = ent;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_table_spec_index_hint
	: USE index_or_key '(' identifier_list ')'
		{{

			PT_NODE *list = $4;
			while (list)
			  {
			    list->info.name.meta_class = PT_INDEX_NAME;
			    list = list->next;
			  }

			$$ = $4;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_table_spec_index_hint_list
	: /* empty */
		{{

			$$ = 0;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| opt_table_spec_index_hint_list ',' opt_table_spec_index_hint
		{{

			$$ = parser_make_link($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos);

		DBG_PRINT}}
	| opt_table_spec_index_hint
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos);

		DBG_PRINT}}
	;

opt_as_identifier_attr_name
	: /* empty */
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, NULL, NULL);
			$$ = ctn;

		DBG_PRINT}}
	| opt_as identifier '(' identifier_list ')'
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, $2, $4);
			$$ = ctn;

		DBG_PRINT}}
	| opt_as identifier
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, $2, NULL);
			$$ = ctn;

		DBG_PRINT}}
	;

opt_as
	: /* empty */
	| AS
	;

class_spec_list
	: class_spec_list  ',' class_spec
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| class_spec
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

class_spec
	: class_name
		{{

			PT_NODE *ocs = parser_new_node (this_parser, PT_SPEC);
			if (ocs)
			  {
			    ocs->info.spec.entity_name = $1;
			    ocs->info.spec.meta_class = PT_CLASS;
			  }

			$$ = ocs;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

class_name
	: identifier '.' identifier
		{{

			PT_NODE *user_node = $1;
			PT_NODE *name_node = $3;

			if (name_node != NULL && user_node != NULL)
			  {
			    name_node->info.name.resolved = pt_append_string (this_parser, NULL,
			                                                      user_node->info.name.original);
			  }
			if (user_node != NULL)
			  {
			    parser_free_tree (this_parser, user_node);
			  }

			$$ = name_node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| identifier
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

class_name_list
	: class_name_list ',' class_name
		{{

			$$ = parser_make_link($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| class_name
		{{
		
			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_class_type
	: /* empty */
		{{

			$$ = PT_EMPTY;

		DBG_PRINT}}
	| VIEW
		{{

			$$ = PT_VCLASS;

		DBG_PRINT}}
	| CLASS
		{{

			$$ = PT_CLASS;

		DBG_PRINT}}
	| TABLE
		{{

			$$ = PT_CLASS;

		DBG_PRINT}}
	;

alter_clause_for_alter_list
	: ADD    alter_add_clause
	| DROP   alter_drop_clause
	| RENAME alter_rename_clause
	| MODIFY alter_modify_clause
	| CHANGE alter_change_clause
	| OWNER TO identifier
		{{
			PT_NODE *alt = parser_get_alter_node();

			if (alt)
			  {
			    alt->info.alter.code = PT_CHANGE_OWNER;
			    alt->info.alter.alter_clause.user.user_name = $3;
			  }
		DBG_PRINT}}
	;

alter_rename_clause
	: opt_to class_name
		{{

			PT_NODE *node = parser_get_alter_node ();

			if (node)
			  {
			    node->info.alter.code = PT_RENAME_ENTITY;
			    node->info.alter.alter_clause.rename.new_name = $2;
			  }

		DBG_PRINT}}
	| opt_of_attr_column identifier as_or_to identifier
		{{

			PT_NODE *node = parser_get_alter_node ();
			PT_MISC_TYPE etyp = $1;

			if (node)
			  {
			    node->info.alter.code = PT_RENAME_ATTR_MTHD;

			    if (etyp == PT_EMPTY)
			      etyp = PT_ATTRIBUTE;

			    node->info.alter.alter_clause.rename.element_type = etyp;
			    node->info.alter.alter_clause.rename.meta = PT_NORMAL;

			    node->info.alter.alter_clause.rename.new_name = $4;
			    node->info.alter.alter_clause.rename.old_name = $2;
			  }

		DBG_PRINT}}
	;

opt_of_attr_column
	: /* empty */
		{{

			$$ = PT_EMPTY;

		DBG_PRINT}}
	| ATTRIBUTE
		{{

			$$ = PT_ATTRIBUTE;

		DBG_PRINT}}
	| COLUMN
		{{

			$$ = PT_ATTRIBUTE;

		DBG_PRINT}}
	;

opt_identifier
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| identifier
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

alter_add_clause
	: opt_of_column_attribute
	  		{ allow_attribute_ordering = true; }
	  opt_attr_def_list
	  		{ allow_attribute_ordering = false; }
		{{

			PT_NODE *node = parser_get_alter_node ();
			if (node)
			  {
			    node->info.alter.code = PT_ADD_ATTR_MTHD;
			    node->info.alter.alter_clause.attr_mthd.attr_def_list = $3;
			  }

		DBG_PRINT}}
	| opt_of_column_attribute
			{ allow_attribute_ordering = true; }
	  attr_def_list
	  		{ allow_attribute_ordering = false; }
		{{

			PT_NODE *node = parser_get_alter_node ();
			if (node)
			  {
			    node->info.alter.code = PT_ADD_ATTR_MTHD;
			    node->info.alter.alter_clause.attr_mthd.attr_def_list = $3;
			  }

		DBG_PRINT}}
	;

opt_of_column_attribute
	: /* empty */
	| COLUMN
	| ATTRIBUTE
	;

alter_drop_clause
	: opt_unique index_or_key identifier
		{{

			PT_NODE *node = parser_get_alter_node ();

			if (node)
			  {
			    node->info.alter.code = PT_DROP_INDEX_CLAUSE;
			    node->info.alter.alter_clause.index.unique = $1;
			    node->info.alter.constraint_list = $3;
			  }

		DBG_PRINT}}
	| CONSTRAINT identifier
		{{

			PT_NODE *node = parser_get_alter_node ();

			if (node)
			  {
			    node->info.alter.code = PT_DROP_CONSTRAINT;
			    node->info.alter.constraint_list = $2;
			  }

		DBG_PRINT}}
	| opt_of_attr_column normal_attribute_list
		{{

			PT_NODE *node = parser_get_alter_node ();

			if (node)
			  {
			    node->info.alter.code = PT_DROP_ATTR_MTHD;
			    node->info.alter.alter_clause.attr_mthd.attr_mthd_name_list = $2;
			  }

		DBG_PRINT}}
	;

normal_attribute_list
	: normal_attribute_list ',' normal_attribute
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| normal_attribute
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

alter_modify_clause
	: opt_of_column_attribute
	  { allow_attribute_ordering = true; }
	  attr_def_one
	  { allow_attribute_ordering = false; }
		{{

			PT_NODE *node = parser_get_alter_node ();

			if (node)
			  {
			    node->info.alter.code = PT_CHANGE_ATTR;
			    node->info.alter.alter_clause.attr_mthd.attr_def_list = $3;
			    /* no name change for MODIFY */
			  }

		DBG_PRINT}}
	;

alter_change_clause
	: normal_column_attribute
	  { allow_attribute_ordering = true; }
	  attr_def_one
	  { allow_attribute_ordering = false; }
		{{

			PT_NODE *node = parser_get_alter_node ();

			if (node)
			  {
			    PT_NODE *att = NULL;
			    
			    node->info.alter.code = PT_CHANGE_ATTR;
			    node->info.alter.alter_clause.attr_mthd.attr_def_list = $3;
			    node->info.alter.alter_clause.attr_mthd.attr_old_name = $1;
			    
			    att = node->info.alter.alter_clause.attr_mthd.attr_def_list;
			    att->info.attr_def.attr_type = 
			      node->info.alter.alter_clause.attr_mthd.attr_old_name->info.name.meta_class;
			  }

		DBG_PRINT}}
	| PRIMARY KEY identifier
		{{

			PT_NODE *node = parser_get_alter_node ();

			if (node)
			  {
			    node->info.alter.code = PT_CHANGE_PK;
			    node->info.alter.alter_clause.chg_pk.index_name = $3;
			  }

		DBG_PRINT}}		
	;

normal_attribute
	: identifier
		{{

			$1->info.name.meta_class = PT_NORMAL;
			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

query_number_list
	: query_number_list ',' unsigned_integer
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| unsigned_integer
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

normal_column_attribute
	: opt_of_column_attribute identifier
		{{

			PT_NODE * node = $2;
			if (node)
			  {
			    node->info.name.meta_class = PT_NORMAL;
			  }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

insert_or_replace_stmt
	: insert_name_clause insert_stmt_value_clause on_duplicate_key_update
		{{

			PT_NODE *ins = $1;

			if (ins)
			  {
			    ins->info.insert.odku_assignments = $3;
			    ins->info.insert.value_clauses = $2;
			  }

			$$ = ins;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| insert_name_clause insert_stmt_value_clause
		{{

			PT_NODE *ins = $1;

			if (ins)
			  {
			    ins->info.insert.value_clauses = $2;
			  }

			$$ = ins;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| replace_name_clause insert_stmt_value_clause
		{{

			PT_NODE *ins = $1;

			if (ins)
			  {
			    ins->info.insert.value_clauses = $2;
			  }

			$$ = ins;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| insert_set_stmt on_duplicate_key_update
		{{

			PT_NODE *ins = $1;
			if (ins)
			  {
			    ins->info.insert.odku_assignments = $2;
			  }

			$$ = ins;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| insert_set_stmt
		{{
			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| replace_set_stmt
		{{
			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

insert_set_stmt
	: insert_stmt_keyword
	  insert_set_stmt_header
		{{
			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		}}
	;

replace_set_stmt
	: replace_stmt_keyword
	  insert_set_stmt_header
		{{
			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		}}
	;

insert_stmt_keyword
	: INSERT
		{
			PT_NODE* ins = parser_new_node (this_parser, PT_INSERT);
			parser_push_hint_node (ins);
		}
	;

replace_stmt_keyword
	: REPLACE
		{
			PT_NODE* ins = parser_new_node (this_parser, PT_INSERT);
			if (ins)
			  {
			    ins->info.insert.do_replace = true;
			  }
			parser_push_hint_node (ins);
		}
	;

insert_set_stmt_header
	: opt_hint_list
	  opt_into
	  class_name
	  SET
	  insert_assignment_list
		{{

			PT_NODE *ins = parser_pop_hint_node ();
			PT_NODE *ocs = parser_new_node (this_parser, PT_SPEC);
			PT_NODE *nls = pt_node_list (this_parser, PT_IS_VALUE, CONTAINER_AT_1 ($5));

			if (ocs)
			  {
			    ocs->info.spec.entity_name = $3;
			    ocs->info.spec.meta_class = PT_CLASS;
			  }

			if (ins)
			  {
			    ins->info.insert.spec = ocs;
			    ins->info.insert.attr_list = CONTAINER_AT_0 ($5);
			    ins->info.insert.value_clauses = nls;
			  }

			$$ = ins;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

insert_assignment_list
	: insert_assignment_list ',' identifier '=' expression_
		{{

			parser_make_link (CONTAINER_AT_0 ($1), $3);
			parser_make_link (CONTAINER_AT_1 ($1), $5);

			$$ = $1;

		DBG_PRINT}}
	| insert_assignment_list ',' identifier '=' DEFAULT
		{{
			PT_NODE *arg = parser_copy_tree (this_parser, $3);

			if (arg)
			  {
			    pt_set_fill_default_in_path_expression (arg);
			  }
			parser_make_link (CONTAINER_AT_0 ($1), $3);
			parser_make_link (CONTAINER_AT_1 ($1),
					  parser_make_expression (this_parser, PT_DEFAULTF, arg, NULL, NULL));

			$$ = $1;

		DBG_PRINT}}
	| identifier '=' expression_
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, $1, $3);

			$$ = ctn;

		DBG_PRINT}}
	| identifier '=' DEFAULT
		{{

			container_2 ctn;
			PT_NODE *arg = parser_copy_tree (this_parser, $1);

			if (arg)
			  {
			    pt_set_fill_default_in_path_expression (arg);
			  }
			SET_CONTAINER_2 (ctn, $1,
			  parser_make_expression (this_parser, PT_DEFAULTF, arg, NULL, NULL));

			$$ = ctn;

		DBG_PRINT}}
	;

on_duplicate_key_update
	: ON_ DUPLICATE_ KEY UPDATE
	  update_assignment_list
		{{

			$$ = $5;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

insert_name_clause
	: insert_stmt_keyword
	  insert_name_clause_header
		{{
			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		}}
	;

replace_name_clause
	: replace_stmt_keyword
	  insert_name_clause_header
		{{
			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		}}
	;

insert_name_clause_header
	: opt_hint_list
	  opt_into
	  class_name
	  opt_attr_list
		{{

			PT_NODE *ins = parser_pop_hint_node ();
			PT_NODE *ocs = parser_new_node (this_parser, PT_SPEC);

			if (ocs)
			  {
			    ocs->info.spec.entity_name = $3;
			    ocs->info.spec.meta_class = PT_CLASS;
			  }

			if (ins)
			  {
			    ins->info.insert.spec = ocs;
			    ins->info.insert.attr_list = $4;
			  }

			$$ = ins;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_attr_list
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| '(' ')'
		{{

			$$ = NULL;

		DBG_PRINT}}
	| '(' identifier_list ')'
		{{

			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

insert_stmt_value_clause
	: insert_expression_value_clause
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| rsql_query_without_values_query
		{{

			PT_NODE *nls = pt_node_list (this_parser, PT_IS_SUBQUERY, $1);
			$$ = nls;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

insert_expression_value_clause
	: of_value_values insert_value_clause_list
		{{

			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DEFAULT opt_values
		{{

			PT_NODE *nls = pt_node_list (this_parser, PT_IS_DEFAULT_VALUE, NULL);
			$$ = nls;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

of_value_values
	: VALUE
	| VALUES
	;

opt_values
	: /* [empty] */
	| VALUES
	;

opt_into
	: /* [empty] */
	| INTO
	;

insert_value_clause_list
	: insert_value_clause_list ',' insert_value_clause
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| insert_value_clause
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

insert_value_clause
	: '(' insert_value_list ')'
		{{

			PT_NODE *nls = pt_node_list (this_parser, PT_IS_VALUE, $2);
			$$ = nls;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| '('')'
		{{

			PT_NODE *nls = pt_node_list (this_parser, PT_IS_VALUE, NULL);

			$$ = nls;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DEFAULT opt_values
		{{

			PT_NODE *nls = pt_node_list (this_parser, PT_IS_DEFAULT_VALUE, NULL);
			$$ = nls;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

insert_value_list
	: insert_value_list ',' insert_value
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| insert_value
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

insert_value
	: select_stmt
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| normal_expression
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DEFAULT
		{{

			/* The argument will be filled in later, when the
			   corresponding column name is known.
			   See fill_in_insert_default_function_arguments(). */
			$$ = parser_make_expression (this_parser, PT_DEFAULTF, NULL, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

show_stmt
        : SHOW TRACE
                {{
                        PT_NODE *node = NULL;

                        node = pt_make_query_show_trace (this_parser);

                        $$ = node;

                DBG_PRINT}}
	;

update_head
	: UPDATE
		{
			PT_NODE* node = parser_new_node(this_parser, PT_UPDATE);
			parser_push_hint_node(node);
		}
	  opt_hint_list
	;

update_stmt
	: update_head
		{
			PT_NODE * node = parser_pop_hint_node();
			parser_push_orderby_node (node);
			parser_push_hint_node (node);
		}
	  extended_table_spec_list
	  SET
	  update_assignment_list
	  opt_of_where_cursor
	  opt_using_index_clause
	  opt_update_orderby_clause
	  opt_upd_del_limit_clause
		{{

			PT_NODE *node = parser_pop_hint_node ();

			node->info.update.spec = CONTAINER_AT_0 ($3);
			node->info.update.assignment = $5;

			if (CONTAINER_AT_0 ($6))
			  {
			    node->info.update.search_cond = CONTAINER_AT_1 ($6);
			  }

			node->info.update.using_index =
			  (node->info.update.using_index ?
			   parser_make_link (node->info.update.using_index, $7) : $7);

			/* set LIMIT node */
			node->info.update.limit = $9;
			node->info.update.rewrite_limit = 1;
			
			if (node->info.update.spec->next)
			{
			  /* Multi-table update */
			  
			  /* Multi-table update cannot have ORDER BY or LIMIT */
			  if (node->info.update.order_by)
			    {
			      PT_ERRORmf(this_parser, node->info.update.order_by,
			  	       MSGCAT_SET_PARSER_SEMANTIC,
			  	       MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "ORDER BY");
			    }
			  else if (node->info.update.limit)
			    {
			      PT_ERRORmf(this_parser, node->info.update.limit,
			  	       MSGCAT_SET_PARSER_SEMANTIC,
			  	       MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "LIMIT");
			    }
			}
		      else
			{
			  /* Single-table update */
			  
			  if (node->info.update.limit
			      && node->info.update.search_cond)
			    {
			      /* For UPDATE statements that have LIMIT clause don't allow
			       * inst_num in search condition
			       */
			      bool instnum_flag = false;
			      (void) parser_walk_tree (this_parser, node->info.update.search_cond,
						       pt_check_instnum_pre, NULL,
						       pt_check_instnum_post, &instnum_flag);
			      if (instnum_flag)
				{
				  PT_ERRORmf(this_parser, node->info.update.search_cond,
					     MSGCAT_SET_PARSER_SEMANTIC,
					     MSGCAT_SEMANTIC_NOT_ALLOWED_IN_LIMIT_CLAUSE, "INST_NUM()/ROWNUM");
				}
			    }
			  else if (node->info.update.limit
				   && node->info.update.orderby_for)
			    {
			      bool ordbynum_flag = false;
			       /* check for a ORDERBY_NUM it the orderby_for tree */
			      (void) parser_walk_tree (this_parser, node->info.update.orderby_for,
						       pt_check_orderbynum_pre, NULL,
						       pt_check_orderbynum_post, &ordbynum_flag);

			      if (ordbynum_flag)
				{
				  PT_ERRORmf(this_parser, node->info.update.orderby_for,
					     MSGCAT_SET_PARSER_SEMANTIC,
					     MSGCAT_SEMANTIC_NOT_ALLOWED_IN_LIMIT_CLAUSE, "ORDERBY_NUM()");
				}
			    }
			}

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;


opt_of_where_cursor
	: /* empty */
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, 0, NULL);
			$$ = ctn;

		DBG_PRINT}}
	| WHERE
		{
			parser_save_and_set_ic(1);
			DBG_PRINT
		}
	  search_condition
	  	{
			parser_restore_ic();
			DBG_PRINT
		}
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, FROM_NUMBER (1), $3);
			$$ = ctn;

		DBG_PRINT}}
	;


opt_as_identifier
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| AS identifier
		{{

			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| identifier
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

update_assignment_list
	: update_assignment_list ',' update_assignment
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| update_assignment
		{{

			$$ = $1;
		        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

update_assignment
	: simple_path_id '=' expression_
		{{

			$$ = parser_make_expression (this_parser, PT_ASSIGN, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| simple_path_id '=' DEFAULT
		{{

			PT_NODE *node, *node_df = NULL;
			node = parser_copy_tree (this_parser, $1);
			if (node)
			  {
			    pt_set_fill_default_in_path_expression (node);
			    node_df = parser_make_expression (this_parser, PT_DEFAULTF, node, NULL, NULL);
			  }
			$$ = parser_make_expression (this_parser, PT_ASSIGN, $1, node_df, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| paren_simple_path_id_list '=' primary_w_collate
		{{

			PT_NODE *exp = parser_make_expression (this_parser, PT_ASSIGN, $1, NULL, NULL);
			PT_NODE *arg1, *arg2, *list, *tmp;
			PT_NODE *e1, *e2 = NULL, *e1_next, *e2_next;
			bool is_subquery = false;
			arg1 = $1;
			arg2 = $3;

			/* primary is parentheses expr set value */
			if (arg2->node_type == PT_VALUE &&
			    (PT_IS_NULL_NODE (arg2) || arg2->type_enum == PT_TYPE_EXPR_SET))
			  {
                            /* flatten multi-column assignment expr */
                            if (arg1->next != NULL)
                              {
				/* get elements and free set node */
                                e1 = arg1;
                                exp->info.expr.arg1 = NULL; /* cut-off link */

                                parser_free_node (this_parser, exp);    /* free exp, arg1 */
                                if (PT_IS_NULL_NODE (arg2))
                                  {
                                    ;                   /* nop */
                                  }
                                else
                                  {
                                    e2 = arg2->info.value.data_value.set;
                                    arg2->info.value.data_value.set = NULL;     /* cut-off link */
                                  }
                                parser_free_node (this_parser, arg2);

                                list = NULL;            /* init */
                                for (; e1; e1 = e1_next)
                                  {
                                    e1_next = e1->next;
                                    e1->next = NULL;
                                    if (PT_IS_NULL_NODE (arg2))
                                      {
                                        e2 = parser_new_node (this_parser, PT_VALUE);
                                        if (e2 == NULL)
                                          {
                                            break;        /* error */
                                          }
                                        PT_SET_NULL_NODE (e2);
                                      }
                                    else
                                      {
                                        if (e2 == NULL)
                                          break;        /* error */
                                      }
                                    e2_next = e2->next;
                                    e2->next = NULL;

                                    tmp = parser_new_node (this_parser, PT_EXPR);
                                    if (tmp)
                                      {
                                        tmp->info.expr.op = PT_ASSIGN;
                                        tmp->info.expr.arg1 = e1;
                                        tmp->info.expr.arg2 = e2;
                                      }
                                    list = parser_make_link (tmp, list);

                                    e2 = e2_next;
                                  }

                                PARSER_SAVE_ERR_CONTEXT (list, @$.buffer_pos)
                                /* expression number check */
                                if (e1 || e2)
                                  {
                                    PT_ERRORf (this_parser, list,
                                               "check syntax at %s, different number of elements in each expression.",
                                               pt_show_binopcode (PT_ASSIGN));
                                  }

                                $$ = list;
		                PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
                              }
                            else
			      {
				exp->info.expr.arg2 = arg2;
				$$ = exp;
				PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
                              }
			  }
			else
			  {
			    if (pt_is_query (arg2))
			      {
				/* primary is subquery. go ahead */
				is_subquery = true;
			      }

                            exp->info.expr.arg1 = arg1;
                            exp->info.expr.arg2 = arg2;

			    $$ = exp;
			    PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			    PICE (exp);

			    /* unknown error check */
			    if (is_subquery == false)
			      {
				PT_ERRORf (this_parser, exp, "check syntax at %s",
					   pt_show_binopcode (PT_ASSIGN));
			      }
			  }

		DBG_PRINT}}
	;

paren_simple_path_id_list
	: '(' simple_path_id_list ')'
		{{

                        $$ = $2;
                        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

delete_name
	: identifier
		{{

			PT_NODE *node = $1;
			node->info.name.meta_class = PT_CLASS;

			$$ = node;
		        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| identifier '.' '*'
		{{

			PT_NODE *node = $1;
			node->info.name.meta_class = PT_CLASS;

			$$ = node;
		        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

delete_from_using
	: delete_name FROM extended_table_spec_list
		{{

			container_3 ctn;
			SET_CONTAINER_3(ctn, $1, CONTAINER_AT_0 ($3), CONTAINER_AT_1 ($3));

			$$ = ctn;

		DBG_PRINT}}
	| FROM delete_name USING extended_table_spec_list
		{{

			container_3 ctn;
			SET_CONTAINER_3(ctn, $2, CONTAINER_AT_0 ($4), CONTAINER_AT_1 ($4));

			$$ = ctn;

		DBG_PRINT}}
	| FROM table_spec
		{{

			container_3 ctn;
			SET_CONTAINER_3(ctn, NULL, $2, FROM_NUMBER(0));

			$$ = ctn;

		DBG_PRINT}}
	| table_spec
		{{

			container_3 ctn;
			SET_CONTAINER_3(ctn, NULL, $1, FROM_NUMBER(0));

			$$ = ctn;

		DBG_PRINT}}
	;

delete_stmt
	: DELETE_				/* $1 */
		{				/* $2 */
			PT_NODE* node = parser_new_node(this_parser, PT_DELETE);
			parser_push_hint_node(node);
		}
	  opt_hint_list 			/* $3 */
	  delete_from_using			/* $4 */
	  opt_of_where_cursor 			/* $5 */
	  opt_using_index_clause 		/* $6 */
	  opt_upd_del_limit_clause		/* $7 */
		{{

			PT_NODE *del = parser_pop_hint_node ();

			if (del)
			  {
			    del->info.delete_.target_classes = CONTAINER_AT_0 ($4);
			    del->info.delete_.spec = CONTAINER_AT_1 ($4);

			    pt_check_unique_names (this_parser,
						   del->info.delete_.spec);

			    if (TO_NUMBER (CONTAINER_AT_0 ($5)))
			      {
				del->info.delete_.search_cond = CONTAINER_AT_1 ($5);
			      }

			    del->info.delete_.using_index =
			      (del->info.delete_.using_index ?
			       parser_make_link (del->info.delete_.using_index, $6) : $6);

			    del->info.delete_.limit = $7;
			    del->info.delete_.rewrite_limit = 1;

			    /* In a multi-table case the LIMIT clauses is not allowed. */
   			    if (del->info.delete_.spec->next)
   			      {
   				if (del->info.delete_.limit)
   				  {
   				    PT_ERRORmf(this_parser, del->info.delete_.limit,
   					       MSGCAT_SET_PARSER_SEMANTIC,
   					       MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "LIMIT");
   				  }
			      }
			    else
			      {
				/* if the delete is single-table no need to specify
				 * the delete table. In this case add the name of supplied
				 * spec to the target_classes list. */
				if (!del->info.delete_.target_classes) 
				  {
				    if (del->info.delete_.spec->info.spec.range_var)
				      {
					del->info.delete_.target_classes =
					  parser_copy_tree(this_parser, del->info.delete_.spec->info.spec.range_var);
				      }
				    else
				      {
					del->info.delete_.target_classes =
					  parser_copy_tree(this_parser, del->info.delete_.spec->info.spec.entity_name);
				      }
				  }

				/* set LIMIT node */
				if (del->info.delete_.limit && del->info.delete_.search_cond)
				  {
				    /* For DELETE statements that have LIMIT clause don't
				     * allow inst_num in search condition */
				    bool instnum_flag = false;
				    (void) parser_walk_tree (this_parser, del->info.delete_.search_cond,
							     pt_check_instnum_pre, NULL,
							     pt_check_instnum_post, &instnum_flag);
				    if (instnum_flag)
				      {
					PT_ERRORmf(this_parser, del->info.delete_.search_cond,
						   MSGCAT_SET_PARSER_SEMANTIC,
						   MSGCAT_SEMANTIC_NOT_ALLOWED_IN_LIMIT_CLAUSE, "INST_NUM()/ROWNUM");
				      }
				  }
			      }

			  }
			$$ = del;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

auth_stmt
	 : grant_head opt_with_grant_option
		{{

			PT_NODE *node = $1;
			PT_MISC_TYPE w = PT_NO_GRANT_OPTION;
			if ($2)
			  w = PT_GRANT_OPTION;

			if (node)
			  {
			    node->info.grant.grant_option = w;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| revoke_cmd on_class_list from_id_list
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_REVOKE);

			if (node)
			  {
			    node->info.revoke.user_list = $3;
			    node->info.revoke.spec_list = $2;
			    node->info.revoke.auth_cmd_list = $1;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| revoke_cmd from_id_list on_class_list
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_REVOKE);

			if (node)
			  {
			    node->info.revoke.user_list = $2;
			    node->info.revoke.spec_list = $3;
			    node->info.revoke.auth_cmd_list = $1;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

revoke_cmd
	: REVOKE
		{ push_msg(MSGCAT_SYNTAX_MISSING_AUTH_COMMAND_LIST); }
	  author_cmd_list
		{ pop_msg(); }
		{ $$ = $3; }
	;

grant_cmd
	: GRANT
		{ push_msg(MSGCAT_SYNTAX_MISSING_AUTH_COMMAND_LIST); }
	  author_cmd_list
		{ pop_msg(); }
		{ $$ = $3; }
	;

grant_head
	: grant_cmd on_class_list to_id_list
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_GRANT);

			if (node)
			  {
			    node->info.grant.user_list = $3;
			    node->info.grant.spec_list = $2;
			    node->info.grant.auth_cmd_list = $1;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| grant_cmd to_id_list on_class_list
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_GRANT);

			if (node)
			  {
			    node->info.grant.user_list = $2;
			    node->info.grant.spec_list = $3;
			    node->info.grant.auth_cmd_list = $1;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_with_grant_option
	: /* empty */
		{{

			$$ = 0;

		DBG_PRINT}}
	| WITH GRANT OPTION
		{{

			$$ = 1;

		DBG_PRINT}}
	;

on_class_list
	: ON_
		{ push_msg(MSGCAT_SYNTAX_MISSING_CLASS_SPEC_LIST); }
	  class_spec_list
		{ pop_msg(); }
		{ $$ = $3; }
	;

to_id_list
	: TO
		{ push_msg(MSGCAT_SYNTAX_MISSING_IDENTIFIER_LIST); }
	  identifier_list
		{ pop_msg(); }
		{ $$ = $3; }
	;

from_id_list
	: FROM
		{ push_msg(MSGCAT_SYNTAX_MISSING_IDENTIFIER_LIST); }
	  identifier_list
		{ pop_msg(); }
		{ $$ = $3; }
	;

author_cmd_list
	: author_cmd_list ',' authorized_cmd
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| authorized_cmd
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

authorized_cmd
	: SELECT
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_AUTH_CMD);
			node->info.auth_cmd.auth_cmd = PT_SELECT_PRIV;
			node->info.auth_cmd.attr_mthd_list = NULL;
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| INSERT
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_AUTH_CMD);

			if (node)
			  {
			    node->info.auth_cmd.auth_cmd = PT_INSERT_PRIV;
			    node->info.auth_cmd.attr_mthd_list = NULL;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DELETE_
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_AUTH_CMD);

			if (node)
			  {
			    node->info.auth_cmd.auth_cmd = PT_DELETE_PRIV;
			    node->info.auth_cmd.attr_mthd_list = NULL;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}

	| UPDATE '(' identifier_list ')'
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_AUTH_CMD);
			PARSER_SAVE_ERR_CONTEXT (node, @$.buffer_pos)
			if (node)
			  {
			    node->info.auth_cmd.auth_cmd = PT_UPDATE_PRIV;
			    PT_ERRORmf (this_parser, node, MSGCAT_SET_PARSER_SYNTAX,
					MSGCAT_SYNTAX_ATTR_IN_PRIVILEGE,
					parser_print_tree_list (this_parser, $3));

			    node->info.auth_cmd.attr_mthd_list = $3;
			  }

			$$ = node;
		        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| UPDATE
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_AUTH_CMD);

			if (node)
			  {
			    node->info.auth_cmd.auth_cmd = PT_UPDATE_PRIV;
			    node->info.auth_cmd.attr_mthd_list = NULL;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ALTER
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_AUTH_CMD);

			if (node)
			  {
			    node->info.auth_cmd.auth_cmd = PT_ALTER_PRIV;
			    node->info.auth_cmd.attr_mthd_list = NULL;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ADD
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_AUTH_CMD);

			if (node)
			  {
			    node->info.auth_cmd.auth_cmd = PT_ADD_PRIV;
			    node->info.auth_cmd.attr_mthd_list = NULL;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DROP
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_AUTH_CMD);

			if (node)
			  {
			    node->info.auth_cmd.auth_cmd = PT_DROP_PRIV;
			    node->info.auth_cmd.attr_mthd_list = NULL;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| REFERENCES
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_AUTH_CMD);

			if (node)
			  {
			    node->info.auth_cmd.auth_cmd = PT_REFERENCES_PRIV;
			    node->info.auth_cmd.attr_mthd_list = NULL;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ALL PRIVILEGES
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_AUTH_CMD);

			if (node)
			  {
			    node->info.auth_cmd.auth_cmd = PT_ALL_PRIV;
			    node->info.auth_cmd.attr_mthd_list = NULL;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ALL
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_AUTH_CMD);

			if (node)
			  {
			    node->info.auth_cmd.auth_cmd = PT_ALL_PRIV;
			    node->info.auth_cmd.attr_mthd_list = NULL;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_password
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| PASSWORD
		{ push_msg(MSGCAT_SYNTAX_INVALID_PASSWORD); }
	  char_string_literal
		{ pop_msg(); }
		{{

			$$ = $3;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_encrypt
	: /* empty */
		{{
			$$ = false;

		DBG_PRINT}}
	| ENCRYPT
		{{
			$$ = true;

		DBG_PRINT}}
	;

opt_attr_def_list
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| '(' attr_def_list ')'
		{{

			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_table_option_list
	:
		{{

			$$ = NULL;

		DBG_PRINT}}
	| table_option_list
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

of_global_or_shard
        : GLOBAL
                {{

                        $$ = 0;

                DBG_PRINT}}
        | SHARD
                {{

                        $$ = 1;

                DBG_PRINT}}
        ;

of_class_table_type
	: CLASS
		{{

			$$ = PT_CLASS;

		DBG_PRINT}}
	| TABLE
		{{

			$$ = PT_CLASS;

		DBG_PRINT}}
	;

opt_or_replace
	: /*empty*/
		{{

			$$ = 0;

		DBG_PRINT}}
	| OR REPLACE
		{{

			$$ = 1;

		DBG_PRINT}}
	;

opt_if_exists
	: /*empty*/
		{{

			$$ = 0;

		DBG_PRINT}}
	| IF EXISTS
		{{

			$$ = 1;

		DBG_PRINT}}
	;

table_option_list
	: table_option_list ',' table_option
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| table_option_list table_option
		{{

			$$ = parser_make_link ($1, $2);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| table_option
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

table_option
	: SHARD BY identifier
		{{
			$$ = pt_table_option (this_parser, PT_TABLE_OPTION_SHARD_KEY, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		  DBG_PRINT}}
		
	;

opt_constraint_id
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| CONSTRAINT identifier
		{{

			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_constraint_opt_id
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| CONSTRAINT opt_identifier
		{{

			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

of_unique_check
	: unique_constraint
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| check_constraint
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_constraint_attr_list
	: /* empty */
		{{

			container_4 ctn;
			SET_CONTAINER_4 (ctn, FROM_NUMBER (0), FROM_NUMBER (0), FROM_NUMBER (0),
					 FROM_NUMBER (0));
			$$ = ctn;

		DBG_PRINT}}
	| constraint_attr_list
		{{

			$$ = $1;

		DBG_PRINT}}
	;

constraint_attr_list
	: constraint_attr_list ',' constraint_attr
		{{

			container_4 ctn = $1;
			container_4 ctn_new = $3;

			if (TO_NUMBER (ctn_new.c1))
			  {
			    ctn.c1 = ctn_new.c1;
			    ctn.c2 = ctn_new.c2;
			  }

			if (TO_NUMBER (ctn_new.c3))
			  {
			    ctn.c3 = ctn_new.c3;
			    ctn.c4 = ctn_new.c4;
			  }

			$$ = ctn;

		DBG_PRINT}}
	| constraint_attr
		{{

			$$ = $1;

		DBG_PRINT}}
	;

unique_constraint
	: PRIMARY KEY opt_identifier '(' index_column_identifier_list ')'
		{{		
		
			PT_NODE *node = parser_new_node (this_parser, PT_CONSTRAINT);		
		
			if (node)		
			  {		
			    node->info.constraint.type = PT_CONSTRAIN_PRIMARY_KEY;		
			    node->info.constraint.name = $3;		
			    node->info.constraint.un.unique.attrs = $5;		
			  }		
		
			$$ = node;		
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)		
		
		DBG_PRINT}}
	| UNIQUE opt_of_index_key opt_identifier index_column_name_list
		{{

			PT_NODE *node = NULL;
			PT_NODE *sort_spec_cols = $4, *name_cols = NULL, *temp;

			for (temp = sort_spec_cols; temp != NULL; temp = temp->next)
			  {
			    if (temp->info.sort_spec.expr->node_type == PT_EXPR)
			      {
			         /* Not allowed function index. */
                                 assert (false);
			         PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SYNTAX,
			                    MSGCAT_SYNTAX_INVALID_CREATE_INDEX);
			      }
			  }
			name_cols = pt_sort_spec_list_to_name_node_list (this_parser, sort_spec_cols);
			assert (name_cols != NULL);
			if (name_cols)
			  {
			    /* create constraint node */
			    node = parser_new_node (this_parser, PT_CONSTRAINT);
			    PARSER_SAVE_ERR_CONTEXT (node, @$.buffer_pos)
			    if (node)
			      {
				node->info.constraint.type = PT_CONSTRAIN_UNIQUE;
				node->info.constraint.name = $3;
				node->info.constraint.un.unique.attrs = name_cols;
			      }
			    parser_free_tree (this_parser, sort_spec_cols);
			  }

			$$ = node;
		        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

index_column_identifier_list
	: index_column_identifier_list ',' index_column_identifier
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| index_column_identifier
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

index_column_identifier
	: identifier opt_asc_or_desc
		{{

			if ($2)
			  {
			    PT_NAME_INFO_SET_FLAG ($1, PT_NAME_INFO_DESC);
			  }
			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_asc_or_desc
	: /* empty */
		{{

			$$ = 0;

		DBG_PRINT}}
	| ASC
		{{

			$$ = 0;

		DBG_PRINT}}
	| DESC
		{{

			$$ = 1;

		DBG_PRINT}}
	;

check_constraint
	: CHECK '(' search_condition ')'
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_CONSTRAINT);

			if (node)
			  {
			    node->info.constraint.type = PT_CONSTRAIN_CHECK;
			    node->info.constraint.un.check.expr = $3;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;


/* bool_deferrable, deferrable value, bool_initially_deferred, initially_deferred value */
constraint_attr
	: NOT DEFERRABLE
		{{

			container_4 ctn;
			ctn.c1 = FROM_NUMBER (1);
			ctn.c2 = FROM_NUMBER (0);
			ctn.c3 = FROM_NUMBER (0);
			ctn.c4 = FROM_NUMBER (0);
			$$ = ctn;

		DBG_PRINT}}
	| DEFERRABLE
		{{

			container_4 ctn;
			ctn.c1 = FROM_NUMBER (1);
			ctn.c2 = FROM_NUMBER (1);
			ctn.c3 = FROM_NUMBER (0);
			ctn.c4 = FROM_NUMBER (0);
			$$ = ctn;

		DBG_PRINT}}
	| INITIALLY DEFERRED
		{{

			container_4 ctn;
			ctn.c1 = FROM_NUMBER (0);
			ctn.c2 = FROM_NUMBER (0);
			ctn.c3 = FROM_NUMBER (1);
			ctn.c4 = FROM_NUMBER (1);
			$$ = ctn;

		DBG_PRINT}}
	| INITIALLY IMMEDIATE
		{{

			container_4 ctn;
			ctn.c1 = FROM_NUMBER (0);
			ctn.c2 = FROM_NUMBER (0);
			ctn.c3 = FROM_NUMBER (1);
			ctn.c4 = FROM_NUMBER (0);
			$$ = ctn;

		DBG_PRINT}}
	;

view_attr_def_list
	: view_attr_def_list ',' view_attr_def
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| view_attr_def
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

view_attr_def
        : identifier
          data_type
                {{

                        PT_NODE *dt;
                        PT_TYPE_ENUM typ;
                        PT_NODE *node = parser_new_node (this_parser, PT_ATTR_DEF);

                        if (node)
                          {
                            node->type_enum = typ = TO_NUMBER (CONTAINER_AT_0 ($2));
                            node->data_type = dt = CONTAINER_AT_1 ($2);
                            node->info.attr_def.attr_name = $1;
                            if (typ == DB_TYPE_VARCHAR && dt)
                              {
                                node->info.attr_def.size_constraint = dt->info.data_type.precision;
                              }
                          }

                        $$ = node;
                        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
	;

attr_def_list
	: attr_def_list ','  attr_def
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| attr_def
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

attr_def
	: attr_constraint_def
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| attr_index_def
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| attr_def_one
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

attr_constraint_def
	: opt_constraint_opt_id
	  of_unique_check
	  opt_constraint_attr_list
		{{

			PT_NODE *name = $1;
			PT_NODE *constraint = $2;

			if (constraint)
			  {
			    if (constraint->node_type == PT_CONSTRAINT)
			      {
				/* If both the constraint name and the index name are
				   given we ignore the constraint name because that is
				   what MySQL does for UNIQUE constraints. */
				if (constraint->info.constraint.name == NULL)
				  {
				    constraint->info.constraint.name = name;
				  }
				if (TO_NUMBER (CONTAINER_AT_0 ($3)))
				  {
				    constraint->info.constraint.deferrable = (short)TO_NUMBER (CONTAINER_AT_1 ($3));
				  }
				if (TO_NUMBER (CONTAINER_AT_2 ($3)))
				  {
				    constraint->info.constraint.initially_deferred =
				      (short)TO_NUMBER (CONTAINER_AT_3 ($3));
				  }
			      }
			    else
			      {
				if (TO_NUMBER (CONTAINER_AT_0 ($3)) || TO_NUMBER (CONTAINER_AT_2 ($3)))
				  {
				    PT_ERRORm (this_parser, constraint, MSGCAT_SET_PARSER_SYNTAX,
					       MSGCAT_SYNTAX_INVALID_CREATE_INDEX);	
				  }
				else 
				  {
				    if (constraint->info.index.index_name == NULL)
				      {
					constraint->info.index.index_name = name;
					if (name == NULL)
					  {
					    PT_ERRORm (this_parser, constraint, 
						       MSGCAT_SET_PARSER_SYNTAX,
					               MSGCAT_SYNTAX_INVALID_CREATE_INDEX);
					  }
				      }
				  }
			      }
			  }

			$$ = constraint;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

attr_index_def
	: index_or_key
	  identifier
	  index_column_name_list
		{{
			PT_NODE *node = NULL;
			PT_NODE *sort_spec_cols = NULL, *name_cols = NULL, *temp;
			
			sort_spec_cols = $3;
			for (temp = sort_spec_cols; temp != NULL; temp = temp->next)
			  {
			    if (temp->info.sort_spec.expr->node_type == PT_EXPR)
			      {
			         /* Not allowed function index. */
                                 assert (false);
			         PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SYNTAX,
			                    MSGCAT_SYNTAX_INVALID_CREATE_INDEX);
			      }
			  }
			name_cols = pt_sort_spec_list_to_name_node_list (this_parser, sort_spec_cols);
			if(name_cols == NULL)
			  {
			    PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SYNTAX,
			                    MSGCAT_SYNTAX_INVALID_CREATE_INDEX);
			  }
			else
			  {
			    /* create constraint node */
			    node = parser_new_node (this_parser, PT_CONSTRAINT);
			    PARSER_SAVE_ERR_CONTEXT (node, @$.buffer_pos)
			    if (node)
			      {
				node->info.constraint.type = PT_CONSTRAIN_INDEX;
				node->info.constraint.name = $2;
				node->info.constraint.un.index.attrs = name_cols;
			      }
			    parser_free_tree (this_parser, sort_spec_cols);
			  }

			$$ = node;
		        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

attr_def_one
	: identifier
	  data_type
		{{

			PT_NODE *dt;
			PT_TYPE_ENUM typ;
			PT_NODE *node = parser_new_node (this_parser, PT_ATTR_DEF);

			if (node)
			  {
			    node->type_enum = typ = TO_NUMBER (CONTAINER_AT_0 ($2));
			    node->data_type = dt = CONTAINER_AT_1 ($2);
			    node->info.attr_def.attr_name = $1;
			    if (typ == DB_TYPE_VARCHAR && dt)
                              {
			        node->info.attr_def.size_constraint = dt->info.data_type.precision;
                              }
			  }

			parser_save_attr_def_one (node);

		DBG_PRINT}}
	  constraint_list
	  opt_attr_ordering_info								%dprec 2
		{{

			PT_NODE *node = parser_get_attr_def_one ();

			node->info.attr_def.attr_type = PT_NORMAL;
			if (node != NULL)
			  {
			    node->info.attr_def.ordering_info = $5;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| identifier
	  data_type
	  opt_attr_ordering_info 									%dprec 1
		{{

			PT_NODE *dt;
			PT_TYPE_ENUM typ;
			PT_NODE *node = parser_new_node (this_parser, PT_ATTR_DEF);

			if (node)
			  {
			    node->type_enum = typ = TO_NUMBER (CONTAINER_AT_0 ($2));
			    node->data_type = dt = CONTAINER_AT_1 ($2);
			    node->info.attr_def.attr_name = $1;
			    if (typ == PT_TYPE_VARCHAR && dt)
                              {
			        node->info.attr_def.size_constraint = dt->info.data_type.precision;
                              }
			    node->info.attr_def.attr_type = PT_NORMAL;
			    node->info.attr_def.ordering_info = $3;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_attr_ordering_info
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| FIRST
		{{

			PT_NODE *ord = parser_new_node (this_parser, PT_ATTR_ORDERING);
			PARSER_SAVE_ERR_CONTEXT (ord, @$.buffer_pos)
			if (ord)
			  {
			    ord->info.attr_ordering.first = true;
			    if (!allow_attribute_ordering)
			      {
				PT_ERRORmf(this_parser, ord,
					   MSGCAT_SET_PARSER_SEMANTIC,
					   MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "FIRST");
			      }
			  }

			$$ = ord;

		DBG_PRINT}}
	| AFTER identifier
		{{

			PT_NODE *ord = parser_new_node (this_parser, PT_ATTR_ORDERING);
			PARSER_SAVE_ERR_CONTEXT (ord, @$.buffer_pos)
			if (ord)
			  {
			    ord->info.attr_ordering.after = $2;
			    if (!allow_attribute_ordering)
			      {
				PT_ERRORmf(this_parser, ord,
					   MSGCAT_SET_PARSER_SEMANTIC,
					   MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "AFTER column");
			      }
			  }

			$$ = ord;

		DBG_PRINT}}
	;

constraint_list
	: constraint_list column_constraint_def
		{{
			unsigned char mask = $1;
			unsigned char new_bit = $2;
			unsigned char merged = mask | new_bit;

			/* Check the constraints according to the following rules:
			 *   1. A constraint should be specified once.
			 */
			if (((mask & new_bit) ^ new_bit) == 0)
			  {
			    PT_ERROR (this_parser, pt_top(this_parser),
				      "Multiple definitions exist for a constraint");
			  }

			$$ = merged;
		}}
	| column_constraint_def
		{{
			$$ = $1;
		}}
	;

column_constraint_def
	: column_unique_constraint_def
		{{
			$$ = COLUMN_CONSTRAINT_UNIQUE;
		}}
	| column_primary_constraint_def
		{{
			$$ = COLUMN_CONSTRAINT_PRIMARY_KEY;
		}}
	| column_null_constraint_def
		{{
			$$ = COLUMN_CONSTRAINT_NULL;
		}}
	| column_other_constraint_def
		{{
			$$ = COLUMN_CONSTRAINT_OTHERS;
		}}
	| column_default_constraint_def
		{{
			$$ = COLUMN_CONSTRAINT_DEFAULT;
		}}
	;

column_unique_constraint_def
	: opt_constraint_id UNIQUE opt_key opt_constraint_attr_list
		{{

			PT_NODE *node = parser_get_attr_def_one ();
			PT_NODE *constrant_name = $1;
			PT_NODE *constraint = parser_new_node (this_parser, PT_CONSTRAINT);

			if (constraint)
			  {
			    constraint->info.constraint.type = PT_CONSTRAIN_UNIQUE;
			    constraint->info.constraint.un.unique.attrs
			      = parser_copy_tree (this_parser, node->info.attr_def.attr_name);

			    constraint->info.constraint.un.unique.attrs->info.name.meta_class
			      = PT_NORMAL;

			    constraint->info.constraint.name = $1;

			    if (TO_NUMBER (CONTAINER_AT_0 ($4)))
			      {
				constraint->info.constraint.deferrable =
				  (short)TO_NUMBER (CONTAINER_AT_1 ($4));
			      }

			    if (TO_NUMBER (CONTAINER_AT_2 ($4)))
			      {
				constraint->info.constraint.initially_deferred =
				  (short)TO_NUMBER (CONTAINER_AT_3 ($4));
			      }
			  }

			parser_make_link (node, constraint);

		DBG_PRINT}}
	;

column_primary_constraint_def
	: opt_constraint_id PRIMARY KEY opt_constraint_attr_list
		{{

			PT_NODE *node = parser_get_attr_def_one ();
			PT_NODE *constrant_name = $1;
			PT_NODE *constraint = parser_new_node (this_parser, PT_CONSTRAINT);

			if (constraint)
			  {
			    constraint->info.constraint.type = PT_CONSTRAIN_PRIMARY_KEY;
			    constraint->info.constraint.un.unique.attrs
			      = parser_copy_tree (this_parser, node->info.attr_def.attr_name);

			    constraint->info.constraint.name = $1;

			    if (TO_NUMBER (CONTAINER_AT_0 ($4)))
			      {
				constraint->info.constraint.deferrable =
				  (short)TO_NUMBER (CONTAINER_AT_1 ($4));
			      }

			    if (TO_NUMBER (CONTAINER_AT_2 ($4)))
			      {
				constraint->info.constraint.initially_deferred =
				  (short)TO_NUMBER (CONTAINER_AT_3 ($4));
			      }
			  }

			parser_make_link (node, constraint);

		DBG_PRINT}}
	;

column_null_constraint_def
	: opt_constraint_id Null opt_constraint_attr_list
		{{

			PT_NODE *node = parser_get_attr_def_one ();
			PT_NODE *constrant_name = $1;
			PT_NODE *constraint = parser_new_node (this_parser, PT_CONSTRAINT);

						/* to support null in ODBC-DDL, ignore it */
			if (constraint)
			  {
			    constraint->info.constraint.type = PT_CONSTRAIN_NULL;
			    constraint->info.constraint.name = $1;

			    if (TO_NUMBER (CONTAINER_AT_0 ($3)))
			      {
				constraint->info.constraint.deferrable =
				  (short)TO_NUMBER (CONTAINER_AT_1 ($3));
			      }

			    if (TO_NUMBER (CONTAINER_AT_2 ($3)))
			      {
				constraint->info.constraint.initially_deferred =
				  (short)TO_NUMBER (CONTAINER_AT_3 ($3));
			      }
			  }

			parser_make_link (node, constraint);

		DBG_PRINT}}
	| opt_constraint_id NOT Null opt_constraint_attr_list
		{{

			PT_NODE *node = parser_get_attr_def_one ();
			PT_NODE *constrant_name = $1;
			PT_NODE *constraint = parser_new_node (this_parser, PT_CONSTRAINT);


			if (constraint)
			  {
			    constraint->info.constraint.type = PT_CONSTRAIN_NOT_NULL;
			    constraint->info.constraint.un.not_null.attr
			      = parser_copy_tree (this_parser, node->info.attr_def.attr_name);
			    /*
			     * This should probably be deferred until semantic
			     * analysis time; leave it this way for now.
			     */
			    node->info.attr_def.constrain_not_null = 1;

			    constraint->info.constraint.name = $1;

			    if (TO_NUMBER (CONTAINER_AT_0 ($4)))
			      {
				constraint->info.constraint.deferrable =
				  (short)TO_NUMBER (CONTAINER_AT_1 ($4));
			      }

			    if (TO_NUMBER (CONTAINER_AT_2 ($4)))
			      {
				constraint->info.constraint.initially_deferred =
				  (short)TO_NUMBER (CONTAINER_AT_3 ($4));
			      }
			  }

			parser_make_link (node, constraint);

		DBG_PRINT}}
	;

column_other_constraint_def
	: opt_constraint_id CHECK '(' search_condition ')' opt_constraint_attr_list
		{{

			PT_NODE *node = parser_get_attr_def_one ();
			PT_NODE *constrant_name = $1;
			PT_NODE *constraint = parser_new_node (this_parser, PT_CONSTRAINT);

			if (constraint)
			  {
			    constraint->info.constraint.type = PT_CONSTRAIN_CHECK;
			    constraint->info.constraint.un.check.expr = $4;

			    constraint->info.constraint.name = $1;

			    if (TO_NUMBER (CONTAINER_AT_0 ($6)))
			      {
				constraint->info.constraint.deferrable =
				  (short)TO_NUMBER (CONTAINER_AT_1 ($6));
			      }

			    if (TO_NUMBER (CONTAINER_AT_2 ($6)))
			      {
				constraint->info.constraint.initially_deferred =
				  (short)TO_NUMBER (CONTAINER_AT_3 ($6));
			      }
			  }

			parser_make_link (node, constraint);

		DBG_PRINT}}
	;

index_or_key
	: INDEX
	| KEY
	;

opt_of_index_key
	: /* empty */
	| INDEX
	| KEY
	;

opt_key
	: /* empty */
	| KEY
	;

column_default_constraint_def
	: DEFAULT expression_
		{{

			PT_NODE *attr_node;
			PT_NODE *node = parser_new_node (this_parser, PT_DATA_DEFAULT);

			if (node)
			  {
			    PT_NODE *def;
			    node->info.data_default.default_value = $2;
			    PARSER_SAVE_ERR_CONTEXT (node, @2.buffer_pos)

			    def = node->info.data_default.default_value;

			    if (def && def->node_type == PT_EXPR)
			      {
				switch (def->info.expr.op)
				  {
				  case PT_SYS_DATE:
				    node->info.data_default.default_expr = DB_DEFAULT_SYSDATE;
				    break;
				  case PT_SYS_DATETIME:
				    node->info.data_default.default_expr = DB_DEFAULT_SYSDATETIME;
				    break;
				  case PT_USER:
				    node->info.data_default.default_expr = DB_DEFAULT_USER;
				    break;
				  case PT_CURRENT_USER:
				    node->info.data_default.default_expr = DB_DEFAULT_CURR_USER;
				    break;
				  case PT_UNIX_TIMESTAMP:
				    node->info.data_default.default_expr = DB_DEFAULT_UNIX_TIMESTAMP;
				    break;
				  default:
				    node->info.data_default.default_expr = DB_DEFAULT_NONE;
				    break;
				  }
			      }
			    else
			      {
				node->info.data_default.default_expr = DB_DEFAULT_NONE;
			      }
			  }

			attr_node = parser_get_attr_def_one ();
			attr_node->info.attr_def.data_default = node;

		DBG_PRINT}}
	;


/* container order : level, schema, instances, async_ws */
isolation_level_spec
	: expression_
		{{

			container_4 ctn;
			SET_CONTAINER_4 (ctn, $1, FROM_NUMBER (PT_NO_ISOLATION_LEVEL),
					 FROM_NUMBER (PT_NO_ISOLATION_LEVEL), FROM_NUMBER (0));
			$$ = ctn;

		DBG_PRINT}}
	| SERIALIZABLE
		{{

			container_4 ctn;
			SET_CONTAINER_4 (ctn, NULL, FROM_NUMBER (PT_SERIALIZABLE),
					 FROM_NUMBER (PT_SERIALIZABLE), FROM_NUMBER (0));
			$$ = ctn;

		DBG_PRINT}}
	| CURSOR STABILITY
		{{

			container_4 ctn;
			SET_CONTAINER_4 (ctn, NULL, FROM_NUMBER (PT_NO_ISOLATION_LEVEL),
					 FROM_NUMBER (PT_READ_COMMITTED), FROM_NUMBER (0));
			$$ = ctn;

		DBG_PRINT}}
	| isolation_level_name								%dprec 1
		{{

			container_4 ctn;
			PT_MISC_TYPE level = 0;
			if ($1 != PT_REPEATABLE_READ)
			  level = $1;

			SET_CONTAINER_4 (ctn, NULL, FROM_NUMBER (PT_NO_ISOLATION_LEVEL),
					 FROM_NUMBER (level), FROM_NUMBER (0));
			$$ = ctn;

		DBG_PRINT}}
	| isolation_level_name of_schema_class 						%dprec 1
		{{

			container_4 ctn;
			PT_MISC_TYPE schema = 0;
			PT_MISC_TYPE level = 0;
			int error = 0;

			schema = $1;

			if ($1 == PT_READ_UNCOMMITTED)
			  {
			    schema = PT_READ_COMMITTED;
			    error = -1;
			  }

			level = PT_NO_ISOLATION_LEVEL;

			SET_CONTAINER_4 (ctn, NULL, FROM_NUMBER (schema), FROM_NUMBER (level),
					 FROM_NUMBER (error));
			$$ = ctn;

		DBG_PRINT}}
	| isolation_level_name INSTANCES						%dprec 1
		{{

			container_4 ctn;
			PT_MISC_TYPE schema = 0;
			PT_MISC_TYPE level = 0;

			schema = PT_NO_ISOLATION_LEVEL;
			level = $1;

			SET_CONTAINER_4 (ctn, NULL, FROM_NUMBER (schema), FROM_NUMBER (level),
					 FROM_NUMBER (0));
			$$ = ctn;

		DBG_PRINT}}
	| isolation_level_name of_schema_class ',' isolation_level_name INSTANCES	%dprec 10
		{{

			container_4 ctn;
			PT_MISC_TYPE schema = 0;
			PT_MISC_TYPE level = 0;
			int error = 0;

			level = $4;
			schema = $1;

			if ($1 == PT_READ_UNCOMMITTED)
			  {
			    schema = PT_READ_COMMITTED;
			    error = -1;
			  }

			SET_CONTAINER_4 (ctn, NULL, FROM_NUMBER (schema), FROM_NUMBER (level),
					 FROM_NUMBER (error));
			$$ = ctn;

		DBG_PRINT}}
	| isolation_level_name INSTANCES ',' isolation_level_name of_schema_class	%dprec 10
		{{
			container_4 ctn;
			PT_MISC_TYPE schema = 0;
			PT_MISC_TYPE level = 0;
			int error = 0;

			level = $1;
			schema = $4;

			if ($4 == PT_READ_UNCOMMITTED)
			  {
			    schema = PT_READ_COMMITTED;
			    error = -1;
			  }

			SET_CONTAINER_4 (ctn, NULL, FROM_NUMBER (schema), FROM_NUMBER (level),
					 FROM_NUMBER (error));
			$$ = ctn;

		DBG_PRINT}}
	;

of_schema_class
	: SCHEMA
	| CLASS
	;

isolation_level_name
	: REPEATABLE READ
		{{

			$$ = PT_REPEATABLE_READ;

		DBG_PRINT}}
	| READ COMMITTED
		{{

			$$ = PT_READ_COMMITTED;

		DBG_PRINT}}
	| READ UNCOMMITTED
		{{

			$$ = PT_READ_UNCOMMITTED;

		DBG_PRINT}}
	;

timeout_spec
	: '-' UNSIGNED_INTEGER
		{{

			PT_NODE *val = parser_new_node (this_parser, PT_VALUE);

			if (val)
			  {
                            val->info.value.text = $2;

                            if (strlen (val->info.value.text) == 1
                                && val->info.value.text[0] == '1')
                              {
			        val->type_enum = PT_TYPE_INTEGER;
			        val->info.value.data_value.i = -1;
                              }
                            else
                              {
                               PT_ERRORmf (this_parser, val,
                                           MSGCAT_SET_PARSER_SYNTAX,
                                           MSGCAT_SYNTAX_INVALID_UNSIGNED_INT32, $2);
                              }
			  }

			$$ = val;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| unsigned_integer
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| unsigned_real
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| host_param_input
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;


transaction_stmt
	: COMMIT opt_work
		{{

			PT_NODE *comm = parser_new_node (this_parser, PT_COMMIT_WORK);
			$$ = comm;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ROLLBACK opt_work TO opt_savepoint expression_
		{{

			PT_NODE *roll = parser_new_node (this_parser, PT_ROLLBACK_WORK);

			if (roll)
			  {
			    roll->info.rollback_work.save_name = $5;
			  }

			$$ = roll;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ROLLBACK opt_work
		{{

			PT_NODE *roll = parser_new_node (this_parser, PT_ROLLBACK_WORK);
			$$ = roll;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SAVEPOINT expression_
		{{

			PT_NODE *svpt = parser_new_node (this_parser, PT_SAVEPOINT);

			if (svpt)
			  {
			    svpt->info.savepoint.save_name = $2;
			  }

			$$ = svpt;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;


opt_savepoint
	: /* empty */
	| SAVEPOINT
	;

opt_work
	: /* empty */
	| WORK
	;

opt_to
	: /* empty */
	| TO
	;

uint_text
	: UNSIGNED_INTEGER
		{{

			$$ = $1;

		DBG_PRINT}}
	;

rsql_query_stmt
	: 	{ parser_select_level++; }
	  opt_explain
	  rsql_query
		{{
                        if ($2)
                          {
                            $$ = pt_make_query_explain (this_parser, $3);
                          }
                        else
                          {
                            $$ = $3;
                          }

			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			parser_select_level--;

		DBG_PRINT}}
	;

rsql_query
	:
		{{

			parser_save_and_set_cannot_cache (false);
			parser_save_and_set_ic (0);
			parser_save_and_set_gc (0);
			parser_save_and_set_oc (0);
			parser_save_and_set_wjc (0);
			parser_save_and_set_sqc (1);

		DBG_PRINT}}
	  select_expression
		{{

			PT_NODE *node = $2;
			parser_push_orderby_node (node);

		DBG_PRINT}}
	  opt_orderby_clause
		{{

			PT_NODE *node = parser_pop_orderby_node ();

			if (node && parser_cannot_cache)
			  {
			    node->info.query.reexecute = 1;
			    node->info.query.do_cache = 0;
			    node->info.query.do_not_cache = 1;
			  }

			parser_restore_cannot_cache ();
			parser_restore_ic ();
			parser_restore_gc ();
			parser_restore_oc ();
			parser_restore_wjc ();
			parser_restore_sqc ();

			if (parser_subquery_check == 0)
			    PT_ERRORmf(this_parser, pt_top(this_parser),
				MSGCAT_SET_PARSER_SEMANTIC,
				MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "Subquery");

			if (node)
			  {
			    /* handle ORDER BY NULL */
			    PT_NODE *order = node->info.query.order_by;
			    if (order && PT_IS_NULL_NODE (order->info.sort_spec.expr))
			      {
				if (!node->info.query.q.select.group_by)
				  {
				    PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
					       MSGCAT_SEMANTIC_ORDERBYNULL_REQUIRES_GROUPBY);
				  }
				else
				  {
				    parser_free_tree (this_parser, node->info.query.order_by);
				    node->info.query.order_by = NULL;
				  }
			      }
			  }

			parser_push_orderby_node (node);

		DBG_PRINT}}
	opt_select_limit_clause
	opt_for_update_clause
		{{

			PT_NODE *node = parser_pop_orderby_node ();
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;
	
rsql_query_without_values_query
	:
		{{

			parser_save_and_set_cannot_cache (false);
			parser_save_and_set_ic (0);
			parser_save_and_set_gc (0);
			parser_save_and_set_oc (0);
			parser_save_and_set_wjc (0);
			parser_save_and_set_sqc (1);

		DBG_PRINT}}
	  select_expression_without_values_query
		{{

			PT_NODE *node = $2;
			parser_push_orderby_node (node);

		DBG_PRINT}}
	  opt_orderby_clause
		{{

			PT_NODE *node = parser_pop_orderby_node ();

			if (node && parser_cannot_cache)
			  {
			    node->info.query.reexecute = 1;
			    node->info.query.do_cache = 0;
			    node->info.query.do_not_cache = 1;
			  }

			parser_restore_cannot_cache ();
			parser_restore_ic ();
			parser_restore_gc ();
			parser_restore_oc ();
			parser_restore_wjc ();
			parser_restore_sqc ();

			if (parser_subquery_check == 0)
			    PT_ERRORmf(this_parser, pt_top(this_parser),
				MSGCAT_SET_PARSER_SEMANTIC,
				MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "Subquery");

			if (node)
			  {
			    /* handle ORDER BY NULL */
			    PT_NODE *order = node->info.query.order_by;
			    if (order && PT_IS_NULL_NODE (order->info.sort_spec.expr))
			      {
				if (!node->info.query.q.select.group_by)
				  {
				    PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
					       MSGCAT_SEMANTIC_ORDERBYNULL_REQUIRES_GROUPBY);
				  }
				else
				  {
				    parser_free_tree (this_parser, node->info.query.order_by);
				    node->info.query.order_by = NULL;
				  }
			      }
			  }

			parser_push_orderby_node (node);

		DBG_PRINT}}
	opt_select_limit_clause
	opt_for_update_clause
		{{

			PT_NODE *node = parser_pop_orderby_node ();
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

select_expression
	: select_expression
    {{
        PT_NODE *node = $1;
        parser_push_orderby_node (node);      
      }}
    opt_orderby_clause 
    {{
      
        PT_NODE *node = parser_pop_orderby_node ();
        
        if (node && parser_cannot_cache)
        {
          node->info.query.reexecute = 1;
          node->info.query.do_cache = 0;
          node->info.query.do_not_cache = 1;
        }
        

        if (parser_subquery_check == 0)
          PT_ERRORmf(this_parser, pt_top(this_parser),
                     MSGCAT_SET_PARSER_SEMANTIC,
                     MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "Subquery");
        
        if (node)
        {

         PT_NODE *order = node->info.query.order_by;
          if (order && PT_IS_NULL_NODE (order->info.sort_spec.expr))
          {
            if (!node->info.query.q.select.group_by)
            {
              PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
                         MSGCAT_SEMANTIC_ORDERBYNULL_REQUIRES_GROUPBY);
            }
            else
            {
              parser_free_tree (this_parser, node->info.query.order_by);
              node->info.query.order_by = NULL;
            }
          }
        }
        
        parser_push_orderby_node (node);
      
              DBG_PRINT}}
      opt_select_limit_clause
      opt_for_update_clause
		{{
                        
			PT_NODE *node = parser_pop_orderby_node ();
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
            
            DBG_PRINT}}
     table_op select_or_subquery
     {{
         
         PT_NODE *stmt = $8;
         PT_NODE *arg1 = $1;
         
         if (stmt)
         {
			    stmt->info.query.id = (UINTPTR) stmt;
			    stmt->info.query.q.union_.arg1 = $1;
			    stmt->info.query.q.union_.arg2 = $9;
			    
			    if (arg1 != NULL
			        && arg1->info.query.is_subquery != PT_IS_SUBQUERY
			        && arg1->info.query.order_by != NULL)
			      {
			        PT_ERRORm (this_parser, stmt,
			               MSGCAT_SET_PARSER_SYNTAX,
			               MSGCAT_SYNTAX_INVALID_UNION_ORDERBY);
			      }
         }


			$$ = stmt;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| select_or_subquery
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;
	
select_expression_without_values_query
	: select_expression_without_values_query
    {{
        PT_NODE *node = $1;
        parser_push_orderby_node (node);      
      }}
    opt_orderby_clause 
    {{
      
        PT_NODE *node = parser_pop_orderby_node ();
        
        if (node && parser_cannot_cache)
        {
          node->info.query.reexecute = 1;
          node->info.query.do_cache = 0;
          node->info.query.do_not_cache = 1;
        }
        

        if (parser_subquery_check == 0)
          PT_ERRORmf(this_parser, pt_top(this_parser),
                     MSGCAT_SET_PARSER_SEMANTIC,
                     MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "Subquery");
        
        if (node)
        {

         PT_NODE *order = node->info.query.order_by;
          if (order && PT_IS_NULL_NODE (order->info.sort_spec.expr))
          {
            if (!node->info.query.q.select.group_by)
            {
              PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
                         MSGCAT_SEMANTIC_ORDERBYNULL_REQUIRES_GROUPBY);
            }
            else
            {
              parser_free_tree (this_parser, node->info.query.order_by);
              node->info.query.order_by = NULL;
            }
          }
        }
        
        parser_push_orderby_node (node);
        
		DBG_PRINT}}
      opt_select_limit_clause
      opt_for_update_clause
		{{
                        
			PT_NODE *node = parser_pop_orderby_node ();
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
            
            DBG_PRINT}}
     table_op select_or_subquery_without_values_query
     {{
         
         PT_NODE *stmt = $8;
         PT_NODE *arg1 = $1;
         
         if (stmt)
         {
			    stmt->info.query.id = (UINTPTR) stmt;
			    stmt->info.query.q.union_.arg1 = $1;
			    stmt->info.query.q.union_.arg2 = $9;
			    if (arg1 != NULL
			        && arg1->info.query.is_subquery != PT_IS_SUBQUERY
			        && arg1->info.query.order_by != NULL)
			      {
			        PT_ERRORm (this_parser, stmt,
			               MSGCAT_SET_PARSER_SYNTAX,
			               MSGCAT_SYNTAX_INVALID_UNION_ORDERBY);
			      }
         }


			$$ = stmt;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| select_or_subquery_without_values_query
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

table_op
	: Union all_distinct
		{{
			PT_MISC_TYPE isAll = $2;
			PT_NODE *node = parser_new_node (this_parser, PT_UNION);
			if (node)
			  {
			    if (isAll == PT_EMPTY)
			      isAll = PT_DISTINCT;
			    node->info.query.all_distinct = isAll;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DIFFERENCE_ all_distinct
		{{

			PT_MISC_TYPE isAll = $2;
			PT_NODE *node = parser_new_node (this_parser, PT_DIFFERENCE);
			if (node)
			  {
			    if (isAll == PT_EMPTY)
			      isAll = PT_DISTINCT;
			    node->info.query.all_distinct = isAll;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| EXCEPT all_distinct
		{{

			PT_MISC_TYPE isAll = $2;
			PT_NODE *node = parser_new_node (this_parser, PT_DIFFERENCE);
			if (node)
			  {
			    if (isAll == PT_EMPTY)
			      isAll = PT_DISTINCT;
			    node->info.query.all_distinct = isAll;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| INTERSECTION all_distinct
		{{

			PT_MISC_TYPE isAll = $2;
			PT_NODE *node = parser_new_node (this_parser, PT_INTERSECTION);
			if (node)
			  {
			    if (isAll == PT_EMPTY)
			      isAll = PT_DISTINCT;
			    node->info.query.all_distinct = isAll;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| INTERSECT all_distinct
		{{

			PT_MISC_TYPE isAll = $2;
			PT_NODE *node = parser_new_node (this_parser, PT_INTERSECTION);
			if (node)
			  {
			    if (isAll == PT_EMPTY)
			      isAll = PT_DISTINCT;
			    node->info.query.all_distinct = isAll;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

select_or_subquery
	: select_stmt
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| subquery
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;
	
select_or_subquery_without_values_query
	: select_stmt
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| subquery
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

select_stmt
	:
	SELECT			/* $1 */
		{{
				/* $2 */
			PT_NODE *node;

			if (parser_select_level >= 0)
			  parser_select_level++;

			node = parser_new_node (this_parser, PT_SELECT);

			if (node)
			  {
			    node->info.query.q.select.hint = PT_HINT_NONE;
			  }

			parser_push_select_stmt_node (node);
			parser_push_hint_node (node);

		DBG_PRINT}}
	opt_hint_list 		/* $3 */
	all_distinct            /* $4 */
	select_list 		/* $5 */
		{{
				/* $6 */
			PT_MISC_TYPE isAll = $4;
			PT_NODE *node = parser_top_select_stmt_node ();
			if (node)
			  {
			    if (isAll == PT_EMPTY)
			      isAll = PT_ALL;
			    node->info.query.all_distinct = isAll;
			    node->info.query.q.select.list = $5;
			  }

		}}
	opt_from_clause  	/* $7 */
		{{
			$$ = $7;
		}}
	;

opt_from_clause
	: /* empty */
		{{

			PT_NODE *n;
			PT_NODE *node = parser_pop_select_stmt_node ();

			parser_pop_hint_node ();

			if (node)
			  {
			    PARSER_SAVE_ERR_CONTEXT (node, @$.buffer_pos)
			    n = node->info.query.q.select.list;
			    if (n && n->type_enum == PT_TYPE_STAR)
			      {
				/* "select *" is not valid, raise an error */
				PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
					   MSGCAT_SEMANTIC_NO_TABLES_USED);
			      }

			    node->info.query.id = (UINTPTR) node;
			    node->info.query.all_distinct = PT_ALL;
			  }

			if (parser_select_level >= 0)
			  parser_select_level--;

			$$ = node;

		DBG_PRINT}}
	| FROM				/* $1 */
	  extended_table_spec_list	/* $2 */
		{{			/* $3 */

		DBG_PRINT}}
	  opt_where_clause		/* $4 */
	  opt_groupby_clause		/* $5 */
	  opt_with_rollup		/* $6 */
	  opt_having_clause 		/* $7 */
	  opt_using_index_clause	/* $8 */
		{{

			PT_NODE *n;
			bool is_dummy_select;
			PT_NODE *node = parser_pop_select_stmt_node ();
			int with_rollup = $6;
			parser_pop_hint_node ();

			is_dummy_select = false;

			if (node)
			  {
			    PARSER_SAVE_ERR_CONTEXT (node, @$.buffer_pos)
			    n = node->info.query.q.select.list;
			    if (n && n->next == NULL && n->node_type == PT_VALUE
			        && n->type_enum == PT_TYPE_STAR)
			      {
				/* select * from ... */
				is_dummy_select = true;	/* Here, guess as TRUE */
			      }
			    else if (n && n->next == NULL && n->node_type == PT_NAME
				     && n->type_enum == PT_TYPE_STAR)
			      {
				/* select A.* from */
				is_dummy_select = true;	/* Here, guess as TRUE */
			      }
			    else
			      {
				is_dummy_select = false;	/* not dummy */
			      }

			    node->info.query.q.select.from = n = CONTAINER_AT_0 ($2);
			    if (n && n->next)
			      is_dummy_select = false;	/* not dummy */

			    node->info.query.q.select.where = n = $4;
			    if (n)
			      is_dummy_select = false;	/* not dummy */

			    node->info.query.q.select.group_by = n = $5;
			    if (n)
			      is_dummy_select = false;	/* not dummy */

			    if (with_rollup)
			      {
				if (!node->info.query.q.select.group_by)
				  {
				    PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
					       MSGCAT_SEMANTIC_WITHROLLUP_REQUIRES_GROUPBY);
				  }
				else
				  {
				    node->info.query.q.select.group_by->with_rollup = 1;
				  }
			      }

			    node->info.query.q.select.having = n = $7;
			    if (n)
			      is_dummy_select = false;	/* not dummy */

			    node->info.query.q.select.using_index =
			      (node->info.query.q.select.using_index ?
			       parser_make_link (node->info.query.q.select.using_index, $8) : $8);

			    node->info.query.id = (UINTPTR) node;
			  }

			if (node->info.query.all_distinct != PT_ALL)
			  is_dummy_select = false;	/* not dummy */
			if (is_dummy_select == true)
			  {
			    /* mark as dummy */
			    PT_SELECT_INFO_SET_FLAG (node, PT_SELECT_INFO_DUMMY);
			  }

			if (parser_select_level >= 0)
			  parser_select_level--;

			$$ = node;
		        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_hint_list
	: /* empty */
	| hint_list
	;

hint_list
	: hint_list CPP_STYLE_HINT
		{{

			PT_NODE *node = parser_top_hint_node ();
			char *hint_comment = $2;
			(void) pt_get_hint (hint_comment, parser_hint_table, node);

		DBG_PRINT}}
	| hint_list SQL_STYLE_HINT
		{{

			PT_NODE *node = parser_top_hint_node ();
			char *hint_comment = $2;
			(void) pt_get_hint (hint_comment, parser_hint_table, node);

		DBG_PRINT}}
	| hint_list C_STYLE_HINT
		{{

			PT_NODE *node = parser_top_hint_node ();
			char *hint_comment = $2;
			(void) pt_get_hint (hint_comment, parser_hint_table, node);

		DBG_PRINT}}
	| CPP_STYLE_HINT
		{{

			PT_NODE *node = parser_top_hint_node ();
			char *hint_comment = $1;
			(void) pt_get_hint (hint_comment, parser_hint_table, node);

		DBG_PRINT}}
	| SQL_STYLE_HINT
		{{

			PT_NODE *node = parser_top_hint_node ();
			char *hint_comment = $1;
			(void) pt_get_hint (hint_comment, parser_hint_table, node);

		DBG_PRINT}}
	| C_STYLE_HINT
		{{

			PT_NODE *node = parser_top_hint_node ();
			char *hint_comment = $1;
			(void) pt_get_hint (hint_comment, parser_hint_table, node);

		DBG_PRINT}}
	;

all_distinct
	: /* empty */
		{{

			$$ = PT_EMPTY;

		DBG_PRINT}}
	| ALL
		{{

			$$ = PT_ALL;

		DBG_PRINT}}
	| DISTINCT
		{{

			$$ = PT_DISTINCT;

		DBG_PRINT}}
	| UNIQUE
		{{

			$$ = PT_DISTINCT;

		DBG_PRINT}}
	;

select_list
	: '*'
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);
			if (node)
			  node->type_enum = PT_TYPE_STAR;
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}

	| '*' ',' alias_enabled_expression_list_top
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);
			if (node)
			  node->type_enum = PT_TYPE_STAR;

			$$ = parser_make_link (node, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}

	| alias_enabled_expression_list_top
		{{
			 $$ = $1;
			 PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		DBG_PRINT}}
	;

alias_enabled_expression_list_top
	:	
		{{

			parser_save_and_set_ic (2);
			parser_save_and_set_gc (2);
			parser_save_and_set_oc (2);

		DBG_PRINT}}
	  alias_enabled_expression_list
		{{

			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			parser_restore_ic ();
			parser_restore_gc ();
			parser_restore_oc ();

		DBG_PRINT}}
	;

alias_enabled_expression_list
	: alias_enabled_expression_list  ',' alias_enabled_expression_
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| alias_enabled_expression_
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

alias_enabled_expression_
	: normal_expression opt_as_identifier %dprec 2
		{{

			PT_NODE *subq, *id;
			PT_NODE *node = $1;
			if (node->node_type == PT_VALUE && node->type_enum == PT_TYPE_EXPR_SET)
			  {
			    node->type_enum = PT_TYPE_SEQUENCE;	/* for print out */
			    PT_ERRORf (this_parser, node,
				       "check syntax at %s, illegal parentheses set expression.",
				       pt_short_print (this_parser, node));
			  }
			else if (PT_IS_QUERY_NODE_TYPE (node->node_type))
			  {
			    /* mark as single tuple query */
			    node->info.query.single_tuple = 1;

			    if ((subq = pt_get_subquery_list (node)) && subq->next)
			      {
				/* illegal multi-column subquery */
				PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
					   MSGCAT_SEMANTIC_NOT_SINGLE_COL);
			      }
			  }


			id = $2;
			if (id && id->node_type == PT_NAME)
			  {
			    if (node->type_enum == PT_TYPE_STAR)
			      {
				PT_ERROR (this_parser, id,
					  "please check syntax after '*', expecting ',' or FROM in select statement.");
			      }
			    else
			      {
				node->alias_print = pt_makename (id->info.name.original);
				parser_free_node (this_parser, id);
			      }
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| predicate_expression opt_as_identifier %dprec 1
		{{

			PT_NODE *id;
			PT_NODE *node = $1;

			id = $2;
			if (id && id->node_type == PT_NAME)
			  {
			    node->alias_print = pt_makename (id->info.name.original);
			    parser_free_node (this_parser, id);
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

expression_list
	: expression_queue
		{{

			$$ = CONTAINER_AT_0($1);

		DBG_PRINT}}
	;

expression_queue
	: expression_queue  ',' expression_
		{{
			container_2 new_q;

			PT_NODE* q_head = CONTAINER_AT_0($1);
			PT_NODE* q_tail = CONTAINER_AT_1($1);
			q_tail->next = $3;

			SET_CONTAINER_2(new_q, q_head, $3);
			$$ = new_q;
			PARSER_SAVE_ERR_CONTEXT (q_head, @$.buffer_pos)

		DBG_PRINT}}
	| expression_
		{{
			container_2 new_q;

			SET_CONTAINER_2(new_q, $1, $1);
			$$ = new_q;
			PARSER_SAVE_ERR_CONTEXT ($1, @$.buffer_pos)

		DBG_PRINT}}
	;

host_param_input
	: '?'
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_HOST_VAR);

			if (node)
			  {
				PARSER_SAVE_ERR_CONTEXT (node, @$.buffer_pos)
			    node->info.host_var.var_type = PT_HOST_IN;
			    node->info.host_var.str = pt_makename ("?");
			    node->info.host_var.index = parser_input_host_index++;
			  }
			if (parser_hostvar_check == 0)
			  {
			    PT_ERRORmf(this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
				       MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "host variable");
			  }

			$$ = node;
		        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| PARAM_HEADER uint_text
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_HOST_VAR);

			if (node)
			  {
				PARSER_SAVE_ERR_CONTEXT (node, @$.buffer_pos)
			    node->info.host_var.var_type = PT_HOST_IN;
			    node->info.host_var.str = pt_makename ("?");
			    node->info.host_var.index = atol ($2);
			    if (node->info.host_var.index >= parser_input_host_index)
			      {
				parser_input_host_index = node->info.host_var.index + 1;
			      }
			  }
			if (parser_hostvar_check == 0)
			  {
			    PT_ERRORmf(this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
				       MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "host variable");
			  }

			$$ = node;
		        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_where_clause
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	|	{
			parser_save_and_set_ic (1);
		}
	  WHERE search_condition
		{{

			parser_restore_ic ();
			$$ = $3;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_groupby_clause
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| GROUP_ BY group_spec_list
		{{

			$$ = $3;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_with_rollup
	: /*empty*/
		{{

			$$ = 0;

		DBG_PRINT}}
	| WITH ROLLUP
		{{

			$$ = 1;

		DBG_PRINT}}
	;

group_spec_list
	: group_spec_list ',' group_spec
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| group_spec
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;


// SR +3
group_spec
	:
		{
			parser_groupby_exception = 0;
		}
	  expression_
	  opt_asc_or_desc 	  /* $3 */
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_SORT_SPEC);

			PARSER_SAVE_ERR_CONTEXT (node, @$.buffer_pos)

			switch (parser_groupby_exception)
			  {
			  case PT_COUNT:
			  case PT_INST_NUM:
			  case PT_ORDERBY_NUM:
			  case PT_ROWNUM:
			    PT_ERROR (this_parser, node,
				      "expression is not allowed as group by spec");
			    break;

			  case PT_IS_SUBQUERY:
			    PT_ERROR (this_parser, node,
				      "subquery is not allowed to group as spec");
			    break;

			  case PT_EXPR:
			    PT_ERROR (this_parser, node,
				      "search condition is not allowed as group by spec");
			    break;
			  }

			if (node)
			  {
			    node->info.sort_spec.asc_or_desc = PT_ASC;
			    node->info.sort_spec.expr = $2;
			    if ($3)
			      {
				node->info.sort_spec.asc_or_desc = PT_DESC;
			      }
			  }

			$$ = node;

		DBG_PRINT}}
	;


opt_having_clause
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	|	{ parser_save_and_set_gc(1); }
	  HAVING search_condition
		{{

			parser_restore_gc ();
			$$ = $3;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_using_index_clause
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| USING INDEX index_name_keylimit_list
		{{

			$$ = $3;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| USING INDEX NONE
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_NAME);

			if (node)
			  {
			    node->info.name.original = NULL;
			    node->info.name.resolved = NULL;
			    node->info.name.meta_class = PT_INDEX_NAME;
			    node->etc = (void *) PT_IDX_HINT_NONE;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

index_name_keylimit_list
	: index_name_keylimit_list ',' index_name_keylimit
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| index_name_keylimit
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

index_name_keylimit
	: index_name KEYLIMIT opt_uint_or_host_input
		{{
		
			PT_NODE *node = $1;
			PARSER_SAVE_ERR_CONTEXT (node, @$.buffer_pos)

			if (node)
			  {
			    if (node->etc == (void *) PT_IDX_HINT_NONE
				|| node->etc == (void *) PT_IDX_HINT_CLASS_NONE)
			      {
				PT_ERRORm(this_parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_KEYLIMIT_INDEX_NONE);
			      }
			    else
			      {
				/* set key limit */
				node->info.name.indx_key_limit = $3;
			      }
			  }
			$$ = node;
			
		DBG_PRINT}}
	| index_name KEYLIMIT opt_uint_or_host_input ',' opt_uint_or_host_input
		{{
		
			PT_NODE *node = $1;
			if (node)
			  {
			    if (node->etc == (void *) PT_IDX_HINT_NONE
				|| node->etc == (void *) PT_IDX_HINT_CLASS_NONE)
			      {
				PT_ERRORm(this_parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_KEYLIMIT_INDEX_NONE);
			      }
			    else
			      {
				/* set key limits */
				node->info.name.indx_key_limit = $5;
				if ($5)
				  {
				    ($5)->next = $3;
				  }
			      }
			  }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			
		DBG_PRINT}}
	| index_name
		{{
		
			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			
		DBG_PRINT}}
	;

index_name
	: class_name
		{{

			PT_NODE *node = $1;
			node->info.name.meta_class = PT_INDEX_NAME;
			node->etc = (void *) PT_IDX_HINT_USE;
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| identifier '.' NONE
		{{
		
			PT_NODE *node = $1;
			node->info.name.meta_class = PT_INDEX_NAME;
			node->info.name.resolved = node->info.name.original;
			node->info.name.original = NULL;
			node->etc = (void *) PT_IDX_HINT_CLASS_NONE;
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		
		DBG_PRINT}}
	;

opt_for_update_clause
	: /* empty */
		{ $$ = NULL; }
	| For UPDATE
		{{

			PT_NODE *node = parser_top_orderby_node ();
			
			if (node != NULL && node->node_type == PT_SELECT)
			  {
			    PT_SELECT_INFO_SET_FLAG(node, PT_SELECT_INFO_FOR_UPDATE);
			  }
			else
			  {
			    PT_ERRORm (this_parser, node,
				       MSGCAT_SET_PARSER_SEMANTIC,
				       MSGCAT_SEMANTIC_INVALID_USE_FOR_UPDATE_CLAUSE);
			  }
			  
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_update_orderby_clause
	: /* empty */
		{ $$ = NULL; }
	| ORDER BY
	  sort_spec_list
		{ parser_save_and_set_oc (1); }
	  opt_for_search_condition
		{{
			PT_NODE *stmt = parser_pop_orderby_node ();

			parser_restore_oc ();

			stmt->info.update.order_by = $3;
			stmt->info.update.orderby_for = $5;

			$$ = stmt;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		DBG_PRINT}}
	;

opt_orderby_clause
	: /* empty */
		{ $$ = NULL; }
	| ORDER
	  BY
		{{
			PT_NODE *stmt = parser_top_orderby_node ();

			if (stmt && !stmt->info.query.q.select.from)
			    PT_ERRORmf(this_parser, pt_top(this_parser),
				MSGCAT_SET_PARSER_SEMANTIC,
				MSGCAT_SEMANTIC_NOT_ALLOWED_HERE, "ORDER BY");

		DBG_PRINT}}
	  sort_spec_list
		{{

			PT_NODE *stmt = parser_top_orderby_node ();

			parser_save_and_set_oc (1);

		DBG_PRINT}}
	  opt_for_search_condition
		{{

			PT_NODE *col, *order, *n, *temp, *list = NULL;
			PT_NODE *stmt = parser_top_orderby_node ();
			bool found_star;
			int index_of_col;
			char *n_str, *c_str;
			bool is_col, is_alias;
                        unsigned int save_custom;

                        save_custom = this_parser->custom_print;
                        this_parser->custom_print |= PT_SUPPRESS_QUOTES;

			parser_restore_oc ();
			if (stmt)
			  {
			    stmt->info.query.orderby_for = $6;
			    /* support for alias in FOR */
			    n = stmt->info.query.orderby_for;
			    while (n)
			      {
				resolve_alias_in_expr_node (n, stmt->info.query.q.select.list);
				n = n->next;
			      }
			  }

			if (stmt)
			  {
			    stmt->info.query.order_by = order = $4;
			    if (order)
			      {				/* not dummy */
				PT_SELECT_INFO_CLEAR_FLAG (stmt, PT_SELECT_INFO_DUMMY);
				if (pt_is_query (stmt))
				  {
				    /* UNION, INTERSECT, DIFFERENCE, SELECT */
				    temp = stmt;
				    while (temp)
				      {
					switch (temp->node_type)
					  {
					  case PT_SELECT:
					    goto start_check;
					    break;
					  case PT_UNION:
					  case PT_INTERSECTION:
					  case PT_DIFFERENCE:
					    temp = temp->info.query.q.union_.arg1;
					    break;
					  default:
					    temp = NULL;
					    break;
					  }
				      }

				  start_check:
				    if (temp)
				      {
				        list = temp->info.query.q.select.list;
				      }
				    found_star = false;

				    if (list && list->node_type == PT_VALUE
					&& list->type_enum == PT_TYPE_STAR)
				      {
					/* found "*" */
					found_star = true;
				      }
				    else
				      {
					for (col = list; col; col = col->next)
					  {
					    if (col->node_type == PT_NAME
						&& col->type_enum == PT_TYPE_STAR)
					      {
						/* found "classname.*" */
						found_star = true;
						break;
					      }
					  }
				      }

				    for (; order; order = order->next)
				      {
					is_alias = false;
					is_col = false;

					n = order->info.sort_spec.expr;
					if (n == NULL)
					  {
					    break;
					  }

					if (n->node_type == PT_VALUE)
					  {
					    continue;
					  }

					n_str = parser_print_tree (this_parser, n);
					if (n_str == NULL)
					  {
					    continue;
					  }

					for (col = list, index_of_col = 1; col;
					     col = col->next, index_of_col++)
					  {
					    c_str = parser_print_tree (this_parser, col);
					    if (c_str == NULL)
					      {
					        continue;
					      }

					    if (col->alias_print
						&& intl_identifier_casecmp (n_str, col->alias_print) == 0)
                                              {
                                                is_alias = true;
                                              }

					    if (intl_identifier_casecmp (n_str, c_str) == 0)
					      {
					        is_col = true;
                                              }

					    if (is_alias || is_col)
					      {
						if (found_star)
						  {
						    temp = parser_copy_tree (this_parser, col);
						    temp->next = NULL;
						  }
						else
						  {
						    temp = parser_new_node (this_parser, PT_VALUE);
						    if (temp == NULL)
						      {
						        break;
						      }

						    temp->type_enum = PT_TYPE_INTEGER;
						    temp->info.value.data_value.i = index_of_col;
						  }

						parser_free_node (this_parser, order->info.sort_spec.expr);
						order->info.sort_spec.expr = temp;

					        if (is_alias && is_col)
					          {
					            /* alias/col name ambiguity, raise error */
					            PT_ERRORmf (this_parser, order, MSGCAT_SET_PARSER_SEMANTIC,
					  	        	MSGCAT_SEMANTIC_AMBIGUOUS_COLUMN_IN_ORDERING,
						        	n_str);
					            break;
					          }
					      }
					  }
				      }
				  }
			      }
			  }

                        this_parser->custom_print = save_custom;

			$$ = stmt;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_uint_or_host_input
	: unsigned_integer
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| host_param_input
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_select_limit_clause
	: /* empty */
		{ $$ = NULL; }
	| LIMIT limit_options opt_PK_next_options
	        {{

			PT_NODE *node = $2;
			if (node)
			  {
			    if (node->node_type == PT_SELECT) 
			      {
				if (!node->info.query.q.select.from)
				  {
				    PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
				    	   MSGCAT_SEMANTIC_NO_TABLES_USED);
				  }
				  
				/* not dummy */
				PT_SELECT_INFO_CLEAR_FLAG (node, PT_SELECT_INFO_DUMMY);

				/* For queries that have LIMIT clause don't allow
				 * inst_num, groupby_num, orderby_num in where, having, for
				 * respectively.
				 */
				if (node->info.query.q.select.having)
				  {
				    bool grbynum_flag = false;
				    (void) parser_walk_tree (this_parser, node->info.query.q.select.having,
				    			 pt_check_groupbynum_pre, NULL,
				    			 pt_check_groupbynum_post, &grbynum_flag);
				    if (grbynum_flag)
				      {
				        PT_ERRORmf(this_parser, node->info.query.q.select.having,
				    	       MSGCAT_SET_PARSER_SEMANTIC,
				    	       MSGCAT_SEMANTIC_NOT_ALLOWED_IN_LIMIT_CLAUSE, "GROUPBY_NUM()");
				      }
				  }
				if (node->info.query.q.select.where)
				  {
				    bool instnum_flag = false;
				    (void) parser_walk_tree (this_parser, node->info.query.q.select.where,
				    			 pt_check_instnum_pre, NULL,
				    			 pt_check_instnum_post, &instnum_flag);
				    if (instnum_flag)
				      {
				        PT_ERRORmf(this_parser, node->info.query.q.select.where,
				    	       MSGCAT_SET_PARSER_SEMANTIC,
				    	       MSGCAT_SEMANTIC_NOT_ALLOWED_IN_LIMIT_CLAUSE, "INST_NUM()/ROWNUM");
				      }
				  }
			      }

			    if (node->info.query.orderby_for)
			      {
				bool ordbynum_flag = false;
				(void) parser_walk_tree (this_parser, node->info.query.orderby_for,
							 pt_check_orderbynum_pre, NULL,
							 pt_check_orderbynum_post, &ordbynum_flag);
				if (ordbynum_flag)
				  {
				    PT_ERRORmf(this_parser, node->info.query.orderby_for,
					       MSGCAT_SET_PARSER_SEMANTIC,
					       MSGCAT_SEMANTIC_NOT_ALLOWED_IN_LIMIT_CLAUSE, "ORDERBY_NUM()");
				  }
			      }

			    node->info.query.pk_next = $3;
			  }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

limit_options
	: opt_uint_or_host_input
		{{

			PT_NODE *node = parser_top_orderby_node ();
			if (node)
			  {
			    node->info.query.limit = $1;
			    node->info.query.rewrite_limit = 1;
			  }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| opt_uint_or_host_input ',' opt_uint_or_host_input
		{{

			PT_NODE *node = parser_top_orderby_node ();
			if (node)
			  {
			    PT_NODE *limit1 = $1;
			    PT_NODE *limit2 = $3;
			    if (limit1)
			      {
				limit1->next = limit2;
			      }
			    node->info.query.limit = limit1;
			    node->info.query.rewrite_limit = 1;
			  }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| opt_uint_or_host_input OFFSET opt_uint_or_host_input
		{{

			PT_NODE *node = parser_top_orderby_node ();
			if (node)
			  {
			    PT_NODE *limit1 = $3;
			    PT_NODE *limit2 = $1;
			    if (limit1)
			      {
				limit1->next = limit2;
			      }
			    node->info.query.limit = limit1;
			    node->info.query.rewrite_limit = 1;
			  }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_PK_next_options
        : /* empty */
                {{

                        $$ = NULL;

                DBG_PRINT}}
        | PK_next_options
                {{

                      $$ = $1;
                      PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
        ;

PK_next_options
	: PRIMARY KEY NEXT '(' PK_next_options_sub ')'
                {{

                      $$ = $5;
                      PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
	;

PK_next_options_sub
        : PK_next_options_sub ',' host_param_input
                {{

                      $$ = parser_make_link ($1, $3);
                      PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
        | host_param_input
                {{

                      $$ = $1;
                      PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
        ;

opt_upd_del_limit_clause
	: /* empty */
		{ $$ = NULL; }
	| LIMIT opt_uint_or_host_input
		{{

			  $$ = $2;
			  PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_for_search_condition
	: /* empty */
		{ $$ = NULL; }
	| For search_condition
		{ $$ = $2; }
	;

sort_spec_list
	: sort_spec_list ',' sort_spec
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| sort_spec
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

sort_spec
	: expression_ ASC opt_nulls_first_or_last
		{{
			PT_NODE *node = parser_new_node (this_parser, PT_SORT_SPEC);

			if (node)
			  {
			    node->info.sort_spec.asc_or_desc = PT_ASC;
			    node->info.sort_spec.expr = $1;
			    node->info.sort_spec.nulls_first_or_last = $3;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_ DESC opt_nulls_first_or_last
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_SORT_SPEC);

			if (node)
			  {
			    node->info.sort_spec.asc_or_desc = PT_DESC;
			    node->info.sort_spec.expr = $1;
			    node->info.sort_spec.nulls_first_or_last = $3;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_ opt_nulls_first_or_last
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_SORT_SPEC);

			if (node)
			  {
			    node->info.sort_spec.asc_or_desc = PT_ASC;
			    node->info.sort_spec.expr = $1;
			    node->info.sort_spec.nulls_first_or_last = $2;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_nulls_first_or_last
	: /* empty */
		{{

			$$ = PT_NULLS_DEFAULT;

		DBG_PRINT}}
	| NULLS FIRST
		{{

			$$ = PT_NULLS_FIRST;

		DBG_PRINT}}
	| NULLS LAST
		{{

			$$ = PT_NULLS_LAST;

		DBG_PRINT}}
	;

expression_
	: normal_expression
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| predicate_expression
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;


normal_expression
	: expression_strcat
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

expression_strcat
	: expression_strcat STRCAT expression_bitor
		{{

			$$ = parser_make_expression (this_parser, PT_STRCAT, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_bitor
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

expression_bitor
	: expression_bitor '|' expression_bitand
		{{

			$$ = parser_make_expression (this_parser, PT_BIT_OR, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_bitand
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

expression_bitand
	: expression_bitand '&' expression_bitshift
		{{

			$$ = parser_make_expression (this_parser, PT_BIT_AND, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_bitshift
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

expression_bitshift
	: expression_bitshift BITSHIFT_LEFT expression_add_sub
		{{

			$$ = parser_make_expression (this_parser, PT_BITSHIFT_LEFT, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_bitshift BITSHIFT_RIGHT expression_add_sub
		{{

			$$ = parser_make_expression (this_parser, PT_BITSHIFT_RIGHT, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_add_sub
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

expression_add_sub
	: expression_add_sub '+' term
		{{

			$$ = parser_make_expression (this_parser, PT_PLUS, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_add_sub '-' term
		{{

			$$ = parser_make_expression (this_parser, PT_MINUS, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| term
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

term
	: term '*' factor
		{{

			$$ = parser_make_expression (this_parser, PT_TIMES, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| term '/' factor
		{{

			$$ = parser_make_expression (this_parser, PT_DIVIDE, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| term DIV factor
		{{

			$$ = parser_make_expression (this_parser, PT_DIV, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| term MOD factor
		{{

			$$ = parser_make_expression (this_parser, PT_MOD, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| factor
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

factor
	: factor '^' factor_
		{{

			$$ = parser_make_expression (this_parser, PT_BIT_XOR, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| factor_
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

factor_
	: primary_w_collate
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| '-' factor_
		{{

			$$ = parser_make_expression (this_parser, PT_UNARY_MINUS, $2, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| '+' factor_
		{{

			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| '~' primary
		{{

			$$ = parser_make_expression (this_parser, PT_BIT_NOT, $2, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

primary_w_collate
	: primary
		{{
			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}

primary
	: reserved_func		%dprec 10
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| case_expr		%dprec 9
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| extract_expr		%dprec 8
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| literal_w_o_param	%dprec 7
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| path_expression	%dprec 5
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| '(' expression_list ')' %dprec 4
		{{
			PT_NODE *exp = $2;
			PT_NODE *val, *tmp;
			bool is_single_expression = true;

			if (exp && exp->next != NULL)
			  {
			    is_single_expression = false;
			  }

			if (is_single_expression)
			  {
			    if (exp && exp->node_type == PT_EXPR)
			      {
				exp->info.expr.paren_type = 1;
			      }

			    if (exp)
			      {
				exp->is_paren = 1;
			      }

			    $$ = exp;
			    PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			  }
			else
			  {
			    val = parser_new_node (this_parser, PT_VALUE);
			    if (val)
			      {
				for (tmp = exp; tmp; tmp = tmp->next)
				  {
				    if (tmp->node_type == PT_VALUE && tmp->type_enum == PT_TYPE_EXPR_SET)
				      {
					tmp->type_enum = PT_TYPE_SEQUENCE;
				      }
				  }

				val->info.value.data_value.set = exp;
				val->type_enum = PT_TYPE_EXPR_SET;
			      }

			    exp = val;
			    $$ = exp;
			    PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			    parser_groupby_exception = PT_EXPR;
			  }

		DBG_PRINT}}
	| '(' search_condition_query ')' %dprec 2
		{{

			PT_NODE *exp = $2;

			if (exp && exp->node_type == PT_EXPR)
			  {
			    exp->info.expr.paren_type = 1;
			  }

			$$ = exp;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			parser_groupby_exception = PT_EXPR;

		DBG_PRINT}}
	| subquery    %dprec 1
		{{
			parser_groupby_exception = PT_IS_SUBQUERY;
			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		DBG_PRINT}}
	;

search_condition_query
	: search_condition_expression
		{{

			PT_NODE *node = $1;
			parser_push_orderby_node (node);

		DBG_PRINT}}
	  opt_orderby_clause
		{{

			PT_NODE *node = parser_pop_orderby_node ();
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

search_condition_expression
	: search_condition
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

reserved_func
	: COUNT '(' '*' ')'
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_FUNCTION);

			if (node)
			  {
                            node->type_enum = PT_TYPE_BIGINT;
			    node->info.function.arg_list = NULL;
			    node->info.function.function_type = PT_COUNT_STAR;
			  }


			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			parser_groupby_exception = PT_COUNT;

		DBG_PRINT}}
	| COUNT '(' of_distinct_unique expression_ ')'
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_FUNCTION);

			if (node)
			  {
                            node->type_enum = PT_TYPE_BIGINT;
			    node->info.function.all_or_distinct = PT_DISTINCT;
			    node->info.function.function_type = PT_COUNT;
			    node->info.function.arg_list = $4;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			parser_groupby_exception = PT_COUNT;

		DBG_PRINT}}
	| COUNT '(' opt_all expression_ ')'
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_FUNCTION);

			if (node)
			  {
                            node->type_enum = PT_TYPE_BIGINT;
			    node->info.function.all_or_distinct = PT_ALL;
			    node->info.function.function_type = PT_COUNT;
			    node->info.function.arg_list = $4;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			parser_groupby_exception = PT_COUNT;

		DBG_PRINT}}
	| of_avg_max_etc '(' of_distinct_unique expression_ ')'
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_FUNCTION);
			
			if (node != NULL)
			  {
			    node->info.function.function_type = $1;

			    if ($1 == PT_MAX || $1 == PT_MIN)
			      node->info.function.all_or_distinct = PT_ALL;
			    else
			      node->info.function.all_or_distinct = PT_DISTINCT;

			    node->info.function.arg_list = $4;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			parser_groupby_exception = PT_COUNT;

		DBG_PRINT}}
	| of_avg_max_etc '(' opt_all expression_ ')'
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_FUNCTION);

			if (node != NULL)
			  {
			    node->info.function.function_type = $1;
			    node->info.function.all_or_distinct = PT_ALL;
			    node->info.function.arg_list = $4;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			parser_groupby_exception = PT_COUNT;

		DBG_PRINT}}
	| GROUP_CONCAT
		{ push_msg(MSGCAT_SYNTAX_INVALID_GROUP_CONCAT); }
		'(' of_distinct_unique expression_ opt_agg_order_by opt_group_concat_separator ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_FUNCTION);

			if (node)
			  {
			    node->info.function.function_type = PT_GROUP_CONCAT;
			    node->info.function.all_or_distinct = PT_DISTINCT;
			    node->info.function.arg_list = parser_make_link ($5, $7);
			    node->info.function.order_by = $6;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GROUP_CONCAT '(' opt_all opt_expression_list opt_agg_order_by opt_group_concat_separator ')'
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_FUNCTION);
			
			PT_NODE *args_list = $4;
			
			if (parser_count_list(args_list) != 1)
		    {
			  PT_ERRORm (this_parser, node, MSGCAT_SET_PARSER_SYNTAX,
					      MSGCAT_SYNTAX_INVALID_GROUP_CONCAT);
		    }
			
			if (node)
			  {
			    node->info.function.function_type = PT_GROUP_CONCAT;
			    node->info.function.all_or_distinct = PT_ALL;
			    node->info.function.arg_list = parser_make_link ($4, $6);
			    node->info.function.order_by = $5;
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| INSERT
	  { push_msg(MSGCAT_SYNTAX_INVALID_INSERT_SUBSTRING); }
	  '(' expression_list ')'
	  { pop_msg(); }
		{{
			PT_NODE *args_list = $4;
			PT_NODE *node = NULL;

			node = parser_make_expr_with_func (this_parser, F_INSERT_SUBSTRING, args_list);
			if (parser_count_list (args_list) != 4)
			  {
			    PT_ERRORmf (this_parser, args_list,
					MSGCAT_SET_PARSER_SEMANTIC,
					MSGCAT_SEMANTIC_INVALID_INTERNAL_FUNCTION,
					"insert");
			  }

			$$ = node;		
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		DBG_PRINT}}
	| ELT '(' opt_expression_list ')'
		{{
		    PT_NODE *args_list = $3;
		    PT_NODE *node = NULL;

		    node = parser_make_expr_with_func (this_parser, F_ELT, args_list);
		    if (parser_count_list(args_list) < 1)
		    {
			PT_ERRORmf (this_parser, args_list,
				    MSGCAT_SET_PARSER_SEMANTIC,
				    MSGCAT_SEMANTIC_INVALID_INTERNAL_FUNCTION,
				    "elt");
		    }

		    $$ = node;
		    PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		DBG_PRINT}}
	| POSITION '(' expression_ IN_ expression_ ')'
		{{

			$$ = parser_make_expression (this_parser, PT_POSITION, $3, $5, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SUBSTRING_
		{ push_msg(MSGCAT_SYNTAX_INVALID_SUBSTRING); }
	  '(' expression_ FROM expression_ For expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_SUBSTRING, $4, $6, $8);
			node->info.expr.qualifier = PT_SUBSTR_ORG;
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SUBSTRING_
		{ push_msg(MSGCAT_SYNTAX_INVALID_SUBSTRING); }
	  '(' expression_ FROM expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_SUBSTRING, $4, $6, NULL);
			node->info.expr.qualifier = PT_SUBSTR_ORG;
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SUBSTRING_
		{ push_msg(MSGCAT_SYNTAX_INVALID_SUBSTRING); }
	  '(' expression_ ',' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_SUBSTRING, $4, $6, $8);
			node->info.expr.qualifier = PT_SUBSTR_ORG;
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SUBSTRING_
		{ push_msg(MSGCAT_SYNTAX_INVALID_SUBSTRING); }
	  '(' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_SUBSTRING, $4, $6, NULL);
			node->info.expr.qualifier = PT_SUBSTR_ORG;
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| Date
		{ push_msg(MSGCAT_SYNTAX_INVALID_DATE); }
	  '(' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_DATEF, $4, NULL, NULL);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| Time
		{ push_msg(MSGCAT_SYNTAX_INVALID_TIME); }
	  '(' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_TIMEF, $4, NULL, NULL);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ADDDATE
		{ push_msg(MSGCAT_SYNTAX_INVALID_ADDDATE); }
	  '(' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_ADDDATE, $4, $6, NULL);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| adddate_name
		{ push_msg(MSGCAT_SYNTAX_INVALID_DATE_ADD); }
	  '(' expression_ ',' INTERVAL expression_ datetime_field ')'
		{ pop_msg(); }
		{{

			PT_NODE *node;
			PT_NODE *node_unit = parser_new_node (this_parser, PT_EXPR);

			if (node_unit)
			  {
			    node_unit->info.expr.qualifier = $8;
			    node_unit->type_enum = PT_TYPE_INTEGER;
			  }

			node = parser_make_expression (this_parser, PT_DATE_ADD, $4, $7, node_unit);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SUBDATE
		{ push_msg(MSGCAT_SYNTAX_INVALID_SUBDATE); }
	  '(' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_SUBDATE, $4, $6, NULL);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| subdate_name
		{ push_msg(MSGCAT_SYNTAX_INVALID_DATE_SUB); }
	  '(' expression_ ',' INTERVAL expression_ datetime_field ')'
		{ pop_msg(); }
		{{

			PT_NODE *node;
			PT_NODE *node_unit = parser_new_node (this_parser, PT_EXPR);

			if (node_unit)
			  {
			    node_unit->info.expr.qualifier = $8;
			    node_unit->type_enum = PT_TYPE_INTEGER;
			  }

			node = parser_make_expression (this_parser, PT_DATE_SUB, $4, $7, node_unit);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| YEAR_
		{ push_msg(MSGCAT_SYNTAX_INVALID_YEAR); }
		'(' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_YEARF, $4, NULL, NULL); /* 1 parameter */
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| MONTH_
		{ push_msg(MSGCAT_SYNTAX_INVALID_MONTH); }
		'(' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_MONTHF, $4, NULL, NULL); /* 1 parameter */
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DAY_
		{ push_msg(MSGCAT_SYNTAX_INVALID_DAY); }
		'(' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_DAYF, $4, NULL, NULL); /* 1 parameter */
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| HOUR_
		{ push_msg(MSGCAT_SYNTAX_INVALID_HOUR); }
		'(' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_HOURF, $4, NULL, NULL); /* 1 parameter */
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| MINUTE_
		{ push_msg(MSGCAT_SYNTAX_INVALID_MINUTE); }
		'(' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_MINUTEF, $4, NULL, NULL); /* 1 parameter */
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SECOND_
		{ push_msg(MSGCAT_SYNTAX_INVALID_SECOND); }
		'(' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_SECONDF, $4, NULL, NULL); /* 1 parameter */
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DATABASE
		{ push_msg(MSGCAT_SYNTAX_INVALID_DATABASE); }
	  '(' ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_DATABASE, NULL, NULL, NULL);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SCHEMA
		{ push_msg(MSGCAT_SYNTAX_INVALID_SCHEMA); }
	  '(' ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_SCHEMA, NULL, NULL, NULL);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| TRIM
		{ push_msg(MSGCAT_SYNTAX_INVALID_TRIM); }
	  '(' of_leading_trailing_both expression_ FROM expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_TRIM, $7, $5, NULL);
			node->info.expr.qualifier = $4;
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| TRIM
		{ push_msg(MSGCAT_SYNTAX_INVALID_TRIM); }
	  '(' of_leading_trailing_both FROM expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_TRIM, $6, NULL, NULL);
			node->info.expr.qualifier = $4;
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| TRIM
		{ push_msg(MSGCAT_SYNTAX_INVALID_TRIM); }
	  '(' expression_ FROM  expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_TRIM, $6, $4, NULL);
			node->info.expr.qualifier = PT_BOTH;
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| TRIM
		{ push_msg(MSGCAT_SYNTAX_INVALID_TRIM); }
	  '(' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_TRIM, $4, NULL, NULL);
			node->info.expr.qualifier = PT_BOTH;
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CHR
		{ push_msg(MSGCAT_SYNTAX_INVALID_CHR); }
	  '(' expression_ ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_CHR, $4, NULL, NULL);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CAST
		{ push_msg(MSGCAT_SYNTAX_INVALID_CAST); }
	  '(' expression_ AS data_type ')'
		{ pop_msg(); }
		{{

			PT_NODE *expr = parser_make_expression (this_parser, PT_CAST, $4, NULL, NULL);
			PT_TYPE_ENUM typ = TO_NUMBER (CONTAINER_AT_0 ($6));
			PT_NODE *dt = CONTAINER_AT_1 ($6);
			PT_NODE *set_dt;

			if (!dt)
			  {
			    dt = parser_new_node (this_parser, PT_DATA_TYPE);
			    dt->type_enum = TO_NUMBER (CONTAINER_AT_0 ($6));
			    dt->data_type = NULL;
			  }
			else if (PT_IS_COLLECTION_TYPE (typ))
			  {
			    set_dt = parser_new_node (this_parser, PT_DATA_TYPE);
			    set_dt->type_enum = typ;
			    set_dt->data_type = dt;
			    dt = set_dt;
			  }

			expr->info.expr.cast_type = dt;
			$$ = expr;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| of_dates
		{{

			PT_NODE *expr = parser_make_expression (this_parser, PT_SYS_DATE, NULL, NULL, NULL);
			$$ = expr;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| of_times
		{{

			PT_NODE *expr = parser_make_expression (this_parser, PT_SYS_TIME, NULL, NULL, NULL);
			$$ = expr;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| of_datetimes
		{{

			PT_NODE *expr = parser_make_expression (this_parser, PT_SYS_DATETIME, NULL, NULL, NULL);
			$$ = expr;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| of_users
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_EXPR);
			if (node)
			  node->info.expr.op = PT_CURRENT_USER;

			parser_cannot_cache = true;

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| of_users
		{ push_msg(MSGCAT_SYNTAX_INVALID_SYSTEM_USER); }
	  '(' ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_USER, NULL, NULL, NULL);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DEFAULT '('
		{ push_msg(MSGCAT_SYNTAX_INVALID_DEFAULT); }
	  simple_path_id ')'
		{ pop_msg(); }
		{{

			PT_NODE *node = NULL;
			PT_NODE *path = $4;

			if (path != NULL)
			  {
			    pt_set_fill_default_in_path_expression (path);
			    node = parser_make_expression (this_parser, PT_DEFAULTF, path, NULL, NULL);
			    PICE (node);
			  }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ROWNUM
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_EXPR);

			if (node)
			  {
			    node->info.expr.op = PT_ROWNUM;
			    PT_EXPR_INFO_SET_FLAG (node, PT_EXPR_INFO_INSTNUM_C);
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
			parser_groupby_exception = PT_ROWNUM;

			if (parser_instnum_check == 0)
			  PT_ERRORmf2 (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
				       MSGCAT_SEMANTIC_INSTNUM_COMPATIBILITY_ERR,
				       "INST_NUM() or ROWNUM", "INST_NUM() or ROWNUM");

		DBG_PRINT}}
	| OCTET_LENGTH
		{ push_msg(MSGCAT_SYNTAX_INVALID_OCTET_LENGTH); }
	  '(' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_OCTET_LENGTH, $4, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| BIT_LENGTH
		{ push_msg(MSGCAT_SYNTAX_INVALID_BIT_LENGTH); }
	  '(' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_BIT_LENGTH, $4, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| LOWER
		{ push_msg(MSGCAT_SYNTAX_INVALID_LOWER); }
	  '(' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_LOWER, $4, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| LCASE
		{ push_msg(MSGCAT_SYNTAX_INVALID_LOWER); }
	  '(' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_LOWER, $4, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| UPPER
		{ push_msg(MSGCAT_SYNTAX_INVALID_UPPER); }
	  '(' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_UPPER, $4, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| UCASE
		{ push_msg(MSGCAT_SYNTAX_INVALID_UPPER); }
	  '(' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_UPPER, $4, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| IF
		{ push_msg (MSGCAT_SYNTAX_INVALID_IF); }
	  '(' search_condition ',' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_IF, $4, $6, $8);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| IFNULL
		{ push_msg (MSGCAT_SYNTAX_INVALID_IFNULL); }
	  '(' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_IFNULL, $4, $6, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ISNULL
		{ push_msg (MSGCAT_SYNTAX_INVALID_ISNULL); }
	  '(' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_ISNULL, $4, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| LEFT
		{ push_msg(MSGCAT_SYNTAX_INVALID_LEFT); }
	  '(' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{
			PT_NODE *node =
			  parser_make_expression (this_parser, PT_LEFT, $4, $6, NULL);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| RIGHT
		{ push_msg(MSGCAT_SYNTAX_INVALID_RIGHT); }
	  '(' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{
			PT_NODE *node =
			  parser_make_expression (this_parser, PT_RIGHT, $4, $6, NULL);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| MOD
		{ push_msg(MSGCAT_SYNTAX_INVALID_MODULUS); }
	  '(' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{
			PT_NODE *node =
			  parser_make_expression (this_parser, PT_MODULUS, $4, $6, NULL);
			PICE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| TRUNCATE
		{ push_msg(MSGCAT_SYNTAX_INVALID_TRUNCATE); }
	  '(' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_TRUNC, $4, $6, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| TRANSLATE
		{ push_msg(MSGCAT_SYNTAX_INVALID_TRANSLATE); }
	  '(' expression_  ',' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_TRANSLATE, $4, $6, $8);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| REPLACE
		{ push_msg(MSGCAT_SYNTAX_INVALID_TRANSLATE); }
	  '(' expression_  ',' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_REPLACE, $4, $6, $8);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| REPLACE
		{ push_msg(MSGCAT_SYNTAX_INVALID_REPLACE); }
	  '(' expression_  ',' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_REPLACE, $4, $6, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| STR_TO_DATE
		{ push_msg(MSGCAT_SYNTAX_INVALID_STRTODATE); }
	  '(' expression_  ',' string_literal_or_input_hv ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_STR_TO_DATE, $4, $6, parser_make_date_lang (2, NULL));
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| STR_TO_DATE
		{ push_msg(MSGCAT_SYNTAX_INVALID_STRTODATE); }
	  '(' expression_  ',' Null ')'
		{ pop_msg(); }
		{{
			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);
			if (node)
			  {
			    PT_SET_NULL_NODE (node);
			  }

			$$ = parser_make_expression (this_parser, PT_STR_TO_DATE, $4, node, parser_make_date_lang (2, NULL));
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| INDEX_PREFIX
		{ push_msg(MSGCAT_SYNTAX_INVALID_INDEX_PREFIX); }
	  '(' expression_  ',' expression_ ',' expression_ ')'
		{ pop_msg(); }
		{{

			$$ = parser_make_expression (this_parser, PT_INDEX_PREFIX, $4, $6, $8);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

of_dates
	: SYS_DATE
	| CURRENT_DATE
	| CURRENT_DATE
		{ push_msg(MSGCAT_SYNTAX_INVALID_CURRENT_DATE); }
	  '(' ')'
		{ pop_msg(); }
	;

of_times
	: SYS_TIME_
	| CURRENT_TIME
	| CURRENT_TIME
		{ push_msg(MSGCAT_SYNTAX_INVALID_CURRENT_TIME); }
	  '(' ')'
		{ pop_msg(); }
	;

of_datetimes
	: SYS_DATETIME
	| CURRENT_DATETIME
	| CURRENT_DATETIME
		{ push_msg(MSGCAT_SYNTAX_INVALID_CURRENT_DATETIME); }
	  '(' ')'
		{ pop_msg(); }
	;

of_users
	: CURRENT_USER
	| SYSTEM_USER
	| USER
	;

of_avg_max_etc
	: AVG
		{{

			$$ = PT_AVG;

		DBG_PRINT}}
	| Max
		{{

			$$ = PT_MAX;

		DBG_PRINT}}
	| Min
		{{

			$$ = PT_MIN;

		DBG_PRINT}}
	| SUM
		{{

			$$ = PT_SUM;

		DBG_PRINT}}
	| STDDEV
		{{

			$$ = PT_STDDEV;

		DBG_PRINT}}
	| STDDEV_POP
		{{

			$$ = PT_STDDEV_POP;

		DBG_PRINT}}
	| STDDEV_SAMP
		{{

			$$ = PT_STDDEV_SAMP;

		DBG_PRINT}}
	| VAR_POP
		{{

			$$ = PT_VAR_POP;

		DBG_PRINT}}
	| VAR_SAMP
		{{

			$$ = PT_VAR_SAMP;

		DBG_PRINT}}
	| VARIANCE
		{{

			$$ = PT_VARIANCE;

		DBG_PRINT}}
	;

of_distinct_unique
	: DISTINCT
	| UNIQUE
	;

opt_group_concat_separator
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| SEPARATOR char_string
		{{

			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SEPARATOR bit_string_literal
		{{

			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_agg_order_by
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| ORDER BY sort_spec
		{{

			$$ = $3;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

of_leading_trailing_both
	: LEADING_
		{{

			$$ = PT_LEADING;

		DBG_PRINT}}
	| TRAILING_
		{{

			$$ = PT_TRAILING;

		DBG_PRINT}}
	| BOTH_
		{{

			$$ = PT_BOTH;

		DBG_PRINT}}
	;

case_expr
	: NULLIF '(' expression_ ',' expression_ ')'
		{{
			$$ = parser_make_expression (this_parser, PT_NULLIF, $3, $5, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		DBG_PRINT}}
	| COALESCE '(' expression_list ')'
		{{
			PT_NODE *prev, *expr, *arg, *tmp;
			int count = parser_count_list ($3);
			int i;
			arg = $3;

			expr = parser_new_node (this_parser, PT_EXPR);
			if (expr)
			  {
			    expr->info.expr.op = PT_COALESCE;
			    expr->info.expr.arg1 = arg;
			    expr->info.expr.arg2 = NULL;
			    expr->info.expr.arg3 = NULL;
			    expr->info.expr.continued_case = 1;
			  }

			PICE (expr);
			prev = expr;

			if (count > 1)
			  {
			    tmp = arg;
			    arg = arg->next;
			    tmp->next = NULL;
			    if (prev)
			      prev->info.expr.arg2 = arg;
			    PICE (prev);
			  }
			for (i = 3; i <= count; i++)
			  {
			    tmp = arg;
			    arg = arg->next;
			    tmp->next = NULL;

			    expr = parser_new_node (this_parser, PT_EXPR);
			    if (expr)
			      {
				expr->info.expr.op = PT_COALESCE;
				expr->info.expr.arg1 = prev;
				expr->info.expr.arg2 = arg;
				expr->info.expr.arg3 = NULL;
				expr->info.expr.continued_case = 1;
			      }
			    if (prev && prev->info.expr.continued_case >= 1)
			      prev->info.expr.continued_case++;
			    PICE (expr);
			    prev = expr;
			  }

			if (expr->info.expr.arg2 == NULL)
			  {
			    expr->info.expr.arg2 = parser_new_node (this_parser, PT_VALUE);

			    if (expr->info.expr.arg2)
			      {
				    PT_SET_NULL_NODE (expr->info.expr.arg2);
				    expr->info.expr.arg2->is_hidden_column = 1;
			      }
			  }

			$$ = expr;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CASE expression_ simple_when_clause_list opt_else_expr END
		{{

			int i;
			PT_NODE *case_oper = $2;
			PT_NODE *node, *prev, *tmp, *curr, *p;

			int count = parser_count_list ($3);
			node = prev = $3;
			if (node)
			  node->info.expr.continued_case = 0;

			tmp = $3;
			do
			  {
			    (tmp->info.expr.arg3)->info.expr.arg1 =
			      parser_copy_tree_list (this_parser, case_oper);
			  }
			while ((tmp = tmp->next))
			  ;

			curr = node;
			for (i = 2; i <= count; i++)
			  {
			    curr = curr->next;
			    if (curr)
			      curr->info.expr.continued_case = 1;
			    if (prev)
			      prev->info.expr.arg2 = curr;	/* else res */
			    PICE (prev);
			    prev->next = NULL;
			    prev = curr;
			  }

			p = $4;
			if (prev)
			  prev->info.expr.arg2 = p;
			PICE (prev);

			if (prev && !prev->info.expr.arg2)
			  {
			    p = parser_new_node (this_parser, PT_VALUE);
			    if (p)
			      {
				    PT_SET_NULL_NODE (p);
			      }
			    prev->info.expr.arg2 = p;
			    PICE (prev);
			  }

			if (case_oper)
			  parser_free_node (this_parser, case_oper);

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CASE searched_when_clause_list opt_else_expr END
		{{

			int i;
			PT_NODE *node, *prev, *curr, *p;

			int count = parser_count_list ($2);
			node = prev = $2;
			if (node)
			  node->info.expr.continued_case = 0;

			curr = node;
			for (i = 2; i <= count; i++)
			  {
			    curr = curr->next;
			    if (curr)
			      curr->info.expr.continued_case = 1;
			    if (prev)
			      prev->info.expr.arg2 = curr;	/* else res */
			    PICE (prev);
			    prev->next = NULL;
			    prev = curr;
			  }

			p = $3;
			if (prev)
			  prev->info.expr.arg2 = p;
			PICE (prev);

			if (prev && !prev->info.expr.arg2)
			  {
			    p = parser_new_node (this_parser, PT_VALUE);
			    if (p)
			      {
				    PT_SET_NULL_NODE (p);
			      }
			    prev->info.expr.arg2 = p;
			    PICE (prev);
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_else_expr
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| ELSE expression_
		{{

			$$ = $2;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

simple_when_clause_list
	: simple_when_clause_list simple_when_clause
		{{

			$$ = parser_make_link ($1, $2);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| simple_when_clause
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

simple_when_clause
	: WHEN expression_ THEN expression_
		{{

			PT_NODE *node, *p, *q;
			p = $2;
			node = parser_new_node (this_parser, PT_EXPR);
			if (node)
			  {
			    node->info.expr.op = PT_CASE;
			    node->info.expr.qualifier = PT_SIMPLE_CASE;
			    q = parser_new_node (this_parser, PT_EXPR);
			    if (q)
			      {
				q->info.expr.op = PT_EQ;
				q->info.expr.arg2 = p;
				node->info.expr.arg3 = q;
				PICE (q);
			      }
			  }

			p = $4;
			if (node)
			  node->info.expr.arg1 = p;
			PICE (node);

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

searched_when_clause_list
	: searched_when_clause_list searched_when_clause
		{{

			$$ = parser_make_link ($1, $2);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| searched_when_clause
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

searched_when_clause
	: WHEN search_condition THEN expression_
		{{

			PT_NODE *node, *p;
			node = parser_new_node (this_parser, PT_EXPR);
			if (node)
			  {
			    node->info.expr.op = PT_CASE;
			    node->info.expr.qualifier = PT_SEARCHED_CASE;
			  }

			p = $2;
			if (node)
			  node->info.expr.arg3 = p;
			PICE (node);

			p = $4;
			if (node)
			  node->info.expr.arg1 = p;
			PICE (node);

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;


extract_expr
	: EXTRACT '(' datetime_field FROM expression_ ')'
		{{

			PT_NODE *tmp;
			tmp = parser_make_expression (this_parser, PT_EXTRACT, $5, NULL, NULL);
			if (tmp)
			  tmp->info.expr.qualifier = $3;
			$$ = tmp;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

adddate_name
	: DATE_ADD
	| ADDDATE
	;

subdate_name
	: DATE_SUB
	| SUBDATE
	;

datetime_field
	: YEAR_
		{{

			$$ = PT_YEAR;

		DBG_PRINT}}
	| MONTH_
		{{

			$$ = PT_MONTH;

		DBG_PRINT}}
	| DAY_
		{{

			$$ = PT_DAY;

		DBG_PRINT}}
	| HOUR_
		{{

			$$ = PT_HOUR;

		DBG_PRINT}}
	| MINUTE_
		{{

			$$ = PT_MINUTE;

		DBG_PRINT}}
	| SECOND_
		{{

			$$ = PT_SECOND;

		DBG_PRINT}}
	| MILLISECOND_
		{{

			$$ = PT_MILLISECOND;

		DBG_PRINT}}
	| WEEK
		{{

			$$ = PT_WEEK;

		DBG_PRINT}}
	| QUARTER
		{{

			$$ = PT_QUARTER;

    		DBG_PRINT}}
        | SECOND_MILLISECOND
		{{

			$$ = PT_SECOND_MILLISECOND;

    		DBG_PRINT}}
	| MINUTE_MILLISECOND
		{{

			$$ = PT_MINUTE_MILLISECOND;

    		DBG_PRINT}}
	| MINUTE_SECOND
		{{

			$$ = PT_MINUTE_SECOND;

    		DBG_PRINT}}
	| HOUR_MILLISECOND
		{{

			$$ = PT_HOUR_MILLISECOND;

    		DBG_PRINT}}
	| HOUR_SECOND
		{{

			$$ = PT_HOUR_SECOND;

    		DBG_PRINT}}
	| HOUR_MINUTE
		{{

			$$ = PT_HOUR_MINUTE;

    		DBG_PRINT}}
	| DAY_MILLISECOND
		{{

			$$ = PT_DAY_MILLISECOND;

    		DBG_PRINT}}
	| DAY_SECOND
		{{

			$$ = PT_DAY_SECOND;

    		DBG_PRINT}}
	| DAY_MINUTE
		{{

			$$ = PT_DAY_MINUTE;

    		DBG_PRINT}}
	| DAY_HOUR
		{{

			$$ = PT_DAY_HOUR;

    		DBG_PRINT}}
	| YEAR_MONTH
		{{

			$$ = PT_YEAR_MONTH;

    		DBG_PRINT}}
	;

generic_function
	: identifier '(' opt_expression_list ')'
		{{

			PT_NODE *func_node = NULL;
                        PT_NODE *name = $1;

			  func_node = parser_keyword_func (name, $3);

			if (func_node == NULL)
			  {
			    /* set error for invalid function */
                            if (!pt_has_error (this_parser))
                              {
			        PT_ERRORmf (this_parser, name,
			    		    MSGCAT_SET_PARSER_SEMANTIC,
					    MSGCAT_SEMANTIC_INVALID_INTERNAL_FUNCTION,
					    name->info.name.original);
                               }
			      			  
			    func_node = parser_new_node (this_parser, PT_FUNCTION);
			    if (func_node)
			      {
                                func_node->info.function.arg_list = $3;
                                func_node->info.function.function_type = PT_GENERIC;
                                func_node->info.function.generic_name = name->info.name.original;
			      }

			    parser_cannot_cache = true;
			  }

			$$ = func_node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

generic_function_id
	: generic_function
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

opt_expression_list
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| expression_list
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

search_condition
	: search_condition OR boolean_term_xor
		{{
			PT_NODE *arg1 = pt_convert_to_logical_expr(this_parser, $1, 1,1);
			PT_NODE *arg2 = pt_convert_to_logical_expr(this_parser, $3, 1,1);
			$$ = parser_make_expression (this_parser, PT_OR, arg1, arg2, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| boolean_term_xor
		{{

			$$ = pt_convert_to_logical_expr(this_parser, $1, 1, 1);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

boolean_term_xor
	: boolean_term_xor XOR boolean_term_is
		{{
			PT_NODE *arg1 = pt_convert_to_logical_expr(this_parser, $1, 1,1);
			PT_NODE *arg2 = pt_convert_to_logical_expr(this_parser, $3, 1,1);
			$$ = parser_make_expression (this_parser, PT_XOR, arg1, arg2, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| boolean_term_is
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		DBG_PRINT}}
	;

boolean_term_is
	: boolean_term_is is_op boolean
		{{
			PT_NODE *arg = pt_convert_to_logical_expr(this_parser, $1, 1,1);
			$$ = parser_make_expression (this_parser, $2, arg, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| boolean_term
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

is_op
	: IS NOT
		{{

			$$ = PT_IS_NOT;

		DBG_PRINT}}
	| IS
		{{

			$$ = PT_IS;

		DBG_PRINT}}
	;

boolean_term
	: boolean_term AND boolean_factor
		{{
			PT_NODE *arg1 = pt_convert_to_logical_expr(this_parser, $1, 1,1);
			PT_NODE *arg2 = pt_convert_to_logical_expr(this_parser, $3, 1,1);
			$$ = parser_make_expression (this_parser, PT_AND, arg1, arg2, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| boolean_factor
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

boolean_factor
	: NOT predicate
		{{

			PT_NODE *arg = pt_convert_to_logical_expr(this_parser, $2, 1,1);
			$$ = parser_make_expression (this_parser, PT_NOT, arg, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| '!' predicate
		{{

			PT_NODE *arg = pt_convert_to_logical_expr(this_parser, $2, 1,1);
			$$ = parser_make_expression (this_parser, PT_NOT, arg, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| predicate
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

predicate
	: EXISTS subquery
		{{

			$$ = parser_make_expression (this_parser, PT_EXISTS, $2, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

predicate_expression
	: pred_lhs comp_op normal_expression
		{{

			PT_NODE *e, *opd1, *opd2, *subq;
			PT_OP_TYPE op;
			bool found_paren_set_expr = false;

			opd2 = $3;
			e = parser_make_expression (this_parser, $2, $1, NULL, NULL);

			if (e && !pt_has_error (this_parser))
			  {

			    e->info.expr.arg2 = opd2;
			    opd1 = e->info.expr.arg1;
			    op = e->info.expr.op;

			    /* convert parentheses set expr value into sequence */
			    if (opd1)
			      {
				if (opd1->node_type == PT_VALUE &&
				    opd1->type_enum == PT_TYPE_EXPR_SET)
				  {
				    opd1->type_enum = PT_TYPE_SEQUENCE;
				    found_paren_set_expr = true;
				  }
				else if (PT_IS_QUERY_NODE_TYPE (opd1->node_type))
				  {
				    subq = pt_get_subquery_list (opd1);
				    if (subq != NULL && subq->next == NULL)
				      {
					/* single-column subquery */
				      }
				    else
				      {
					found_paren_set_expr = true;
				      }
				  }
			      }
			    if (opd2)
			      {
				if (opd2->node_type == PT_VALUE &&
				    opd2->type_enum == PT_TYPE_EXPR_SET)
				  {
				    opd2->type_enum = PT_TYPE_SEQUENCE;
				    found_paren_set_expr = true;
				  }
				else if (PT_IS_QUERY_NODE_TYPE (opd2->node_type))
				  {
				    if ((subq = pt_get_subquery_list (opd2)) && subq->next == NULL)
				      {
					/* single-column subquery */
				      }
				    else
				      {
					found_paren_set_expr = true;
				      }
				  }
			      }
			    if (op == PT_EQ || op == PT_NE)
			      {
				/* expression number check */
				if (found_paren_set_expr == true &&
				    pt_check_set_count_set (this_parser, opd1, opd2))
				  {
				    if (PT_IS_QUERY_NODE_TYPE (opd1->node_type))
				      {
					pt_select_list_to_one_col (this_parser, opd1, true);
				      }
				    if (PT_IS_QUERY_NODE_TYPE (opd2->node_type))
				      {
					pt_select_list_to_one_col (this_parser, opd2, true);
				      }
				    /* rewrite parentheses set expr equi-comparions predicate
				     * as equi-comparison predicates tree of each elements.
				     * for example, (a, b) = (x, y) -> a = x and b = y
				     */
				    if (op == PT_EQ
                                        && opd1 && PT_IS_COLLECTION_TYPE (opd1->type_enum)
                                        && opd2 && PT_IS_COLLECTION_TYPE (opd2->type_enum))
				      {
					e = pt_rewrite_set_eq_set (this_parser, e);
				      }
				  }
				/* mark as single tuple list */
				if (PT_IS_QUERY_NODE_TYPE (opd1->node_type))
				  {
				    opd1->info.query.single_tuple = 1;
				  }
				if (PT_IS_QUERY_NODE_TYPE (opd2->node_type))
				  {
				    opd2->info.query.single_tuple = 1;
				  }
			      }
			    else
			      {
				if (found_paren_set_expr == true)
				  {			/* operator check */
				    PT_ERRORf (this_parser, e,
					       "check syntax at %s, illegal operator.",
					       pt_show_binopcode (op));
				  }
			      }
			  }				/* if (e) */
			PICE (e);

			$$ = e;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| pred_lhs like_op normal_expression ESCAPE escape_literal
		{{

			PT_NODE *esc = parser_make_expression (this_parser, PT_LIKE_ESCAPE, $3, $5, NULL);
			PT_NODE *node = parser_make_expression (this_parser, $2, $1, esc, NULL);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| pred_lhs like_op normal_expression
		{{

			$$ = parser_make_expression (this_parser, $2, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| pred_lhs rlike_op normal_expression
		{{

			/* case sensitivity flag */
			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);
			if (node)
			  {
			    node->type_enum = PT_TYPE_INTEGER;
			    node->info.value.data_value.i = 
			      ($2 == PT_RLIKE_BINARY || $2 == PT_NOT_RLIKE_BINARY ? 1 : 0);

			    $$ = parser_make_expression (this_parser, $2, $1, $3, node);
			  }
			else
			  {
			    $$ = NULL;
			  }
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| pred_lhs null_op
		{{

			$$ = parser_make_expression (this_parser, $2, $1, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| pred_lhs between_op normal_expression AND normal_expression
		{{

			PT_NODE *node = parser_make_expression (this_parser, PT_BETWEEN_AND, $3, $5, NULL);
			$$ = parser_make_expression (this_parser, $2, $1, node, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| pred_lhs in_op in_pred_operand
		{{

			PT_NODE *node = parser_make_expression (this_parser, $2, $1, NULL, NULL);
			PT_NODE *t = CONTAINER_AT_1 ($3);
			bool is_paren = (bool)TO_NUMBER (CONTAINER_AT_0 ($3));
			int lhs_cnt, rhs_cnt = 0;
			PT_NODE *v, *lhs, *rhs, *subq;
			bool found_match = false;
			bool found_paren_set_expr = false;

			PARSER_SAVE_ERR_CONTEXT (node, @$.buffer_pos)
			if (node)
			  {
			    lhs = node->info.expr.arg1;
			    /* convert lhs parentheses set expr value into
			     * sequence value */
			    if (lhs)
			      {
				if (lhs->node_type == PT_VALUE && lhs->type_enum == PT_TYPE_EXPR_SET)
				  {
				    lhs->type_enum = PT_TYPE_SEQUENCE;
				    found_paren_set_expr = true;
                                    PT_ERRORmf2 (this_parser, lhs, MSGCAT_SET_PARSER_SYNTAX,
                                                 MSGCAT_SYNTAX_ERROR_MSG1,
                                                 pt_show_binopcode ($2), "one-column");
				  }
				else if (PT_IS_QUERY_NODE_TYPE (lhs->node_type))
				  {
				    found_paren_set_expr = true; /* guess */
				    subq = pt_get_subquery_list (lhs);
                                    if (subq)
                                      {
				        if (subq->next == NULL)
				          {
				    	    /* single column subquery */
				            found_paren_set_expr = false;
				          }
                                        else
                                          {
					    /* multi column subquery */
                                            PT_ERRORm (this_parser, lhs,
                                                       MSGCAT_SET_PARSER_SEMANTIC,
                                                       MSGCAT_SEMANTIC_NOT_SINGLE_COL);
                                          }
                                      }
                                    else
                                      {
                                        /* found "*" */
                                      }
				  }
			      }

			    if (is_paren == true)
			      {				/* convert to multi-set */
				v = parser_new_node (this_parser, PT_VALUE);
				if (v)
				  {
				    v->info.value.data_value.set = t;
				    v->type_enum = PT_TYPE_SEQUENCE;
				  }			/* if (v) */
				node->info.expr.arg2 = v;
			      }
			    else
			      {
				/* convert subquery-starting parentheses set expr
				 * ( i.e., {subquery, x, y, ...} ) into multi-set */
				if (t->node_type == PT_VALUE && t->type_enum == PT_TYPE_EXPR_SET)
				  {
				    is_paren = true;	/* mark as parentheses set expr */
				    t->type_enum = PT_TYPE_SEQUENCE;
				  }
				node->info.expr.arg2 = t;
			      }

			    rhs = node->info.expr.arg2;
			    if (is_paren == true)
			      {
				rhs = rhs->info.value.data_value.set;
			      }
			    else if (rhs->node_type == PT_VALUE
				     && !(PT_IS_COLLECTION_TYPE (rhs->type_enum)
					  || rhs->type_enum == PT_TYPE_EXPR_SET))
			      {
				PT_ERRORmf2 (this_parser, rhs, MSGCAT_SET_PARSER_SYNTAX,
					     MSGCAT_SYNTAX_ERROR_MSG1,
					     pt_show_binopcode ($2),
					     "SELECT or '('");
			      }

			    /* for each rhs elements, convert parentheses
			     * set expr value into sequence value */
			    for (t = rhs; t; t = t->next)
			      {
				if (t->node_type == PT_VALUE && t->type_enum == PT_TYPE_EXPR_SET)
				  {
				    t->type_enum = PT_TYPE_SEQUENCE;
				    found_paren_set_expr = true;
                                    PT_ERRORmf2 (this_parser, t, MSGCAT_SET_PARSER_SYNTAX,
                                                 MSGCAT_SYNTAX_ERROR_MSG1,
                                                 pt_show_binopcode ($2), "one-column");
				  }
                                else if (PT_IS_QUERY_NODE_TYPE (t->node_type))
                                  {
                                    found_paren_set_expr = true; /* guess */
                                    subq = pt_get_subquery_list (t);
                                    if (subq)
                                      {
                                        if (subq->next == NULL)
                                          {
                                            /* single column subquery */
                                            found_paren_set_expr = false;
                                          }
                                        else
                                          {
                                            /* multi column subquery */
                                            PT_ERRORm (this_parser, t,
                                                       MSGCAT_SET_PARSER_SEMANTIC,
                                                       MSGCAT_SEMANTIC_NOT_SINGLE_COL);
                                          }
                                      }
                                    else
                                      {
                                        /* found "*" */
                                      }
                                  }
			      }

			    if (found_paren_set_expr == true)
			      {
				/* expression number check */
				if ((lhs_cnt = pt_get_expression_count (lhs)) < 0)
				  {
				    found_match = true;
				  }
				else
				  {
				    for (t = rhs; t; t = t->next)
				      {
					rhs_cnt = pt_get_expression_count (t);
					if ((rhs_cnt < 0) || (lhs_cnt == rhs_cnt))
					  {
					    /* can not check negative rhs_cnt. go ahead */
					    found_match = true;
					    break;
					  }
				      }
				  }

				if (found_match == false)
				  {
				    PT_ERRORmf2 (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
						 MSGCAT_SEMANTIC_ATT_CNT_COL_CNT_NE,
						 lhs_cnt, rhs_cnt);
				  }
			      }
			  }

			$$ = node;

		DBG_PRINT}}
	;
	| pred_lhs RANGE_ '(' range_list ')'
		{{

			$$ = parser_make_expression (this_parser, PT_RANGE, $1, $4, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| pred_lhs IdName
		{{
			PT_ERRORm (this_parser, $1, MSGCAT_SET_PARSER_SYNTAX,
				    MSGCAT_SYNTAX_INVALID_RELATIONAL_OP);

		DBG_PRINT}}
	;

pred_lhs
	: normal_expression
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

comp_op
	:  '='
		{{

			$$ = PT_EQ;

		DBG_PRINT}}
	| COMP_NOT_EQ 
		{{

			$$ = PT_NE;

		DBG_PRINT}}
	| '>' 
		{{

			$$ = PT_GT;

		DBG_PRINT}}
	| COMP_GE 
		{{

			$$ = PT_GE;

		DBG_PRINT}}
	| '<'  
		{{

			$$ = PT_LT;

		DBG_PRINT}}
	| COMP_LE 
		{{

			$$ = PT_LE;

		DBG_PRINT}}
	| '=''=' 
		{{

			push_msg (MSGCAT_SYNTAX_INVALID_EQUAL_OP);
			rsql_yyerror_explicit (@1.first_line, @1.first_column);

		DBG_PRINT}}
	| '!''=' 
		{{

			push_msg (MSGCAT_SYNTAX_INVALID_NOT_EQUAL);
			rsql_yyerror_explicit (@1.first_line, @1.first_column);

		DBG_PRINT}}
	| COMP_NULLSAFE_EQ 
		{{

			$$ = PT_NULLSAFE_EQ;

		DBG_PRINT}}
	;

like_op
	: NOT LIKE
		{{

			$$ = PT_NOT_LIKE;

		DBG_PRINT}}
	| LIKE
		{{

			$$ = PT_LIKE;

		DBG_PRINT}}
	;

rlike_op
	: rlike_or_regexp
		{{

			$$ = PT_RLIKE;

		DBG_PRINT}}
	| NOT rlike_or_regexp
		{{

			$$ = PT_NOT_RLIKE;

		DBG_PRINT}}
	| rlike_or_regexp BINARY
		{{

			$$ = PT_RLIKE_BINARY;

		DBG_PRINT}}
	| NOT rlike_or_regexp BINARY
		{{

			$$ = PT_NOT_RLIKE_BINARY;

		DBG_PRINT}}
	;

rlike_or_regexp
	: RLIKE
	| REGEXP
	;

null_op
	: IS NOT Null
		{{

			$$ = PT_IS_NOT_NULL;

		DBG_PRINT}}
	| IS Null
		{{

			$$ = PT_IS_NULL;

		DBG_PRINT}}
	;


between_op
	: NOT BETWEEN
		{{

			$$ = PT_NOT_BETWEEN;

		DBG_PRINT}}
	| BETWEEN
		{{

			$$ = PT_BETWEEN;

		DBG_PRINT}}
	;

in_op
	: IN_
		{{

			$$ = PT_IS_IN;

		DBG_PRINT}}
	| NOT IN_
		{{

			$$ = PT_IS_NOT_IN;

		DBG_PRINT}}
	;

in_pred_operand
	: expression_
		{{
			container_2 ctn;
			PT_NODE *node = $1;
			PT_NODE *exp = NULL;
			bool is_single_expression = true;

			if (node != NULL)
			  {
			    if (node->node_type == PT_VALUE
				&& node->type_enum == PT_TYPE_EXPR_SET)
			      {
				exp = node->info.value.data_value.set;
				node->info.value.data_value.set = NULL;
				parser_free_node (this_parser, node);
			      }
			    else
			      {
				exp = node;
			      }
			  }

			if (exp && exp->next != NULL)
			  {
			    is_single_expression = false;
			  }

			if (is_single_expression && exp && exp->is_paren == 0)
			  {
                            /* check iff invalid grammar */
                            if (!PT_IS_QUERY_NODE_TYPE (exp->node_type))
                              {
                                PT_ERRORf (this_parser, exp,
                                           "check syntax at %s, expecting '('.",
                                           pt_short_print (this_parser, exp));
                              }
			    SET_CONTAINER_2 (ctn, FROM_NUMBER (0), exp);
			  }
			else
			  {
			    SET_CONTAINER_2 (ctn, FROM_NUMBER (1), exp);
			  }

			$$ = ctn;
		DBG_PRINT}}
	;

range_list
	: range_list OR range_
		{{

			$$ = parser_make_link_or ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| range_
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

range_
	: expression_ GE_LE_ expression_
		{{

			$$ = parser_make_expression (this_parser, PT_BETWEEN_GE_LE, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_ GE_LT_ expression_
		{{

			$$ = parser_make_expression (this_parser, PT_BETWEEN_GE_LT, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_ GT_LE_ expression_
		{{

			$$ = parser_make_expression (this_parser, PT_BETWEEN_GT_LE, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_ GT_LT_ expression_
		{{

			$$ = parser_make_expression (this_parser, PT_BETWEEN_GT_LT, $1, $3, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_ '='
		{{

			$$ = parser_make_expression (this_parser, PT_BETWEEN_EQ_NA, $1, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_ GE_INF_ Max
		{{

			$$ = parser_make_expression (this_parser, PT_BETWEEN_GE_INF, $1, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| expression_ GT_INF_ Max
		{{

			$$ = parser_make_expression (this_parser, PT_BETWEEN_GT_INF, $1, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| Min INF_LE_ expression_
		{{

			$$ = parser_make_expression (this_parser, PT_BETWEEN_INF_LE, $3, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| Min INF_LT_ expression_
		{{

			$$ = parser_make_expression (this_parser, PT_BETWEEN_INF_LT, $3, NULL, NULL);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

subquery
	: '(' rsql_query ')'
		{{

			PT_NODE *stmt = $2;

			if (parser_within_join_condition)
			  {
			    PT_ERRORm (this_parser, stmt, MSGCAT_SET_PARSER_SYNTAX,
				       MSGCAT_SYNTAX_JOIN_COND_SUBQ);
			  }

			if (stmt)
			  stmt->info.query.is_subquery = PT_IS_SUBQUERY;
			$$ = stmt;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;


path_expression
	: path_header '.' '*'			%dprec 3
		{{

			PT_NODE *node = $1;
			if (node)
                          {
			    node->type_enum = PT_TYPE_STAR;
                          }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| path_id_list				%dprec 2
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

path_id_list
	: path_id_list path_dot path_id			%dprec 1
		{{

			PT_NODE *dot = parser_new_node (this_parser, PT_DOT_);
			if (dot)
			  {
			    dot->info.dot.arg1 = $1;
			    dot->info.dot.arg2 = $3;
			  }

			$$ = dot;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| path_header					%dprec 2
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

path_header
	: path_id
		{{

			PT_NODE *node = $1;
			if (node && node->node_type == PT_NAME)
                          {
			    node->info.name.meta_class = PT_NORMAL;
                          }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

path_dot
	: '.'
	| RIGHT_ARROW
	;

path_id
	: identifier
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| generic_function_id
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

simple_path_id
	: identifier '.' identifier
		{{

			PT_NODE *dot = parser_new_node (this_parser, PT_DOT_);
			if (dot)
			  {
			    dot->info.dot.arg1 = $1;
			    dot->info.dot.arg2 = $3;
			  }

			$$ = dot;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| identifier
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

data_type
	: set_type primitive_type
		{{

			container_2 ctn;
			PT_TYPE_ENUM typ, e;
			PT_NODE *dt;

			typ = $1;
			e = TO_NUMBER (CONTAINER_AT_0 ($2));
                            if (PT_IS_COLLECTION_TYPE (e))
                              {
                                rsql_yyerror("nested data type definition");
                              }

			dt = CONTAINER_AT_1 ($2);

			if (!dt)
			  {
			    dt = parser_new_node (this_parser, PT_DATA_TYPE);
			    if (dt)
			      {
				dt->type_enum = e;
				dt->data_type = NULL;
			      }
			  }

			SET_CONTAINER_2 (ctn, FROM_NUMBER (typ), dt);
			$$ = ctn;

		DBG_PRINT}}
	| set_type '(' data_type ')'
		{{

			container_2 ctn;
			PT_TYPE_ENUM typ, e;
			PT_NODE *dt;

			typ = $1;
                        e = TO_NUMBER (CONTAINER_AT_0 ($3));
                            if (PT_IS_COLLECTION_TYPE (e))
                              {
                                rsql_yyerror("nested data type definition");
                              }

			dt = CONTAINER_AT_1 ($3);

                        if (!dt)
                          {
                            dt = parser_new_node (this_parser, PT_DATA_TYPE);
                            if (dt)
                              {
                                dt->type_enum = e;
                                dt->data_type = NULL;
                              }
                          }

			SET_CONTAINER_2 (ctn, FROM_NUMBER (typ), dt);
			$$ = ctn;

		DBG_PRINT}}
	| primitive_type
		{{

			$$ = $1;

		DBG_PRINT}}
	;

binary_type
        : BINARY opt_varying
                {{

                          $$ = PT_TYPE_VARBIT;

                DBG_PRINT}}
        | VARBINARY opt_varying
                {{

                          $$ = PT_TYPE_VARBIT;

                DBG_PRINT}}
        ;

varchar_type
	: CHAR_ opt_varying
		{{

			if ($2)
			  $$ = PT_TYPE_VARCHAR;
			else
			  $$ = PT_TYPE_VARCHAR;

		DBG_PRINT}}
	| VARCHAR
		{{

			$$ = PT_TYPE_VARCHAR;

		DBG_PRINT}}
	| NATIONAL CHAR_ opt_varying
		{{

			  $$ = PT_TYPE_VARCHAR;

		DBG_PRINT}}
	| NCHAR	opt_varying
		{{

			  $$ = PT_TYPE_VARCHAR;

		DBG_PRINT}}
	; 

opt_varying
	: /* empty */
		{{

			$$ = 0;

		DBG_PRINT}}
	| VARYING
		{{

			$$ = 1;

		DBG_PRINT}}
	;

primitive_type
	: INTEGER opt_padding
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, FROM_NUMBER (PT_TYPE_INTEGER), NULL);
			$$ = ctn;

		DBG_PRINT}}
	| SmallInt
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, FROM_NUMBER (PT_TYPE_INTEGER), NULL);
			$$ = ctn;

		DBG_PRINT}}
	| BIGINT
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, FROM_NUMBER (PT_TYPE_BIGINT), NULL);
			$$ = ctn;

		DBG_PRINT}}
   	| FLOAT_ opt_prec_1 /* is synonym of Double */
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, FROM_NUMBER (PT_TYPE_DOUBLE), NULL);
			$$ = ctn;

		DBG_PRINT}}
	| Double PRECISION
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, FROM_NUMBER (PT_TYPE_DOUBLE), NULL);
			$$ = ctn;

		DBG_PRINT}}
	| Double
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, FROM_NUMBER (PT_TYPE_DOUBLE), NULL);
			$$ = ctn;

		DBG_PRINT}}
	| Date
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, FROM_NUMBER (PT_TYPE_DATE), NULL);
			$$ = ctn;

		DBG_PRINT}}
	| Time
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, FROM_NUMBER (PT_TYPE_TIME), NULL);
			$$ = ctn;

		DBG_PRINT}}
	| DATETIME
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, FROM_NUMBER (PT_TYPE_DATETIME), NULL);
			$$ = ctn;

		DBG_PRINT}}
	| OBJECT
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, FROM_NUMBER (PT_TYPE_OBJECT), NULL);
			$$ = ctn;

		DBG_PRINT}}
	| String
	  opt_collation
		{{

			container_2 ctn;
			PT_TYPE_ENUM typ = PT_TYPE_VARCHAR;
			PT_NODE *dt = parser_new_node (this_parser, PT_DATA_TYPE);
			PT_NODE *coll_node = $2;
			if (dt)
			  {
			    int coll_id;

			    dt->type_enum = typ;
			    dt->info.data_type.precision = DB_MAX_VARCHAR_PRECISION;

			    if (pt_check_grammar_charset_collation
				  (this_parser, coll_node, &coll_id) == NO_ERROR)
			      {
				dt->info.data_type.collation_id = coll_id;
			      }
			    else
			      {
				dt->info.data_type.collation_id = -1;
			      }

			    if (coll_node)
			      {
				dt->info.data_type.has_coll_spec = true;
			      }
			    else
			      {
				dt->info.data_type.has_coll_spec = false;
			      }
			  }
			SET_CONTAINER_2 (ctn, FROM_NUMBER (typ), dt);
			$$ = ctn;

			if (coll_node)
			  {
			    parser_free_node (this_parser, coll_node);
			  }

		DBG_PRINT}}
	| class_name
		{{

			container_2 ctn;
			PT_TYPE_ENUM typ = PT_TYPE_OBJECT;
			PT_NODE *dt = parser_new_node (this_parser, PT_DATA_TYPE);

			if (dt)
			  {
			    dt->type_enum = typ;
			    dt->info.data_type.entity = $1;
			  }

			SET_CONTAINER_2 (ctn, FROM_NUMBER (typ), dt);
			$$ = ctn;

		DBG_PRINT}}
	| binary_type
	  opt_prec_1
		{{

			container_2 ctn;
			PT_TYPE_ENUM typ = $1;
			PT_NODE *len = NULL, *dt = NULL;
			int l = 1;

			len = $2;

               assert (typ == PT_TYPE_VARBIT);

			if (len)
			  {
                   int maxlen = DB_MAX_VARBIT_PRECISION;

			    l = len->info.value.data_value.i;

			    if (l > maxlen)
			      {
				 PT_ERRORmf (this_parser, len, MSGCAT_SET_PARSER_SYNTAX,
					     MSGCAT_SYNTAX_MAX_BITLEN, maxlen);
			      }

			    l = (l > maxlen ? maxlen : l);
			  }
			else
			  {
			    l = DB_MAX_VARBIT_PRECISION;
			  }

			dt = parser_new_node (this_parser, PT_DATA_TYPE);
			if (dt)
			  {
			    dt->type_enum = typ;
			    dt->info.data_type.precision = l;
			  }

			SET_CONTAINER_2 (ctn, FROM_NUMBER (typ), dt);
			$$ = ctn;
			if (len)
                          {
			    parser_free_node (this_parser, len);
                          }

		DBG_PRINT}}
        | varchar_type
          opt_prec_1
          opt_collation
                {{

                        container_2 ctn;
                        PT_TYPE_ENUM typ = $1;
                        PT_NODE *len = NULL, *dt = NULL;
                        int l = 1;
                        PT_NODE *coll_node = NULL;

                        len = $2;
                        coll_node = $3;

                        assert (typ == PT_TYPE_VARCHAR);

                        if (len)
                          {
                            int maxlen = DB_MAX_VARCHAR_PRECISION;

                            l = len->info.value.data_value.i;

                            if (l > maxlen)
                              {
                                PT_ERRORmf (this_parser, len, MSGCAT_SET_PARSER_SYNTAX,
                                            MSGCAT_SYNTAX_MAX_BYTELEN, maxlen);
                              }

                            l = (l > maxlen ? maxlen : l);
                          }
                        else
                          {
                            l = DB_MAX_VARCHAR_PRECISION;
                          }

                        dt = parser_new_node (this_parser, PT_DATA_TYPE);
                        if (dt)
                          {
                            int coll_id;

                            dt->type_enum = typ;
                            dt->info.data_type.precision = l;

                            if (pt_check_grammar_charset_collation
                                (this_parser, coll_node, &coll_id) == NO_ERROR)
                              {
                                dt->info.data_type.collation_id = coll_id;
                              }
                            else
                              {
                                dt->info.data_type.collation_id = -1;
                              }

                            if (coll_node)
                              {
                                dt->info.data_type.has_coll_spec = true;
                              }
                            else
                              {
                                dt->info.data_type.has_coll_spec = false;
                              }
                          }

                        SET_CONTAINER_2 (ctn, FROM_NUMBER (typ), dt);
                        $$ = ctn;
                        if (len)
                          {
                            parser_free_node (this_parser, len);
                          }

                        if (coll_node)
                          {
                            parser_free_node (this_parser, coll_node);
                          }

		DBG_PRINT}}
	| NUMERIC opt_prec_2
		{{

			container_2 ctn;
			PT_TYPE_ENUM typ;
			PT_NODE *prec, *scale, *dt;
			prec = CONTAINER_AT_0 ($2);
			scale = CONTAINER_AT_1 ($2);

			dt = parser_new_node (this_parser, PT_DATA_TYPE);
			typ = PT_TYPE_NUMERIC;

			if (dt)
			  {
			    dt->type_enum = typ;
			    dt->info.data_type.precision = prec ? prec->info.value.data_value.i : DB_DEFAULT_NUMERIC_PRECISION;
			    dt->info.data_type.dec_scale =
			      scale ? scale->info.value.data_value.i : DB_DEFAULT_NUMERIC_SCALE;

			    if (scale && prec)
			      if (scale->info.value.data_value.i > prec->info.value.data_value.i)
				{
				  PT_ERRORmf2 (this_parser, dt,
					       MSGCAT_SET_PARSER_SEMANTIC,
					       MSGCAT_SEMANTIC_INV_PREC_SCALE,
					       prec->info.value.data_value.i,
					       scale->info.value.data_value.i);
				}
			    if (prec)
			      if (prec->info.value.data_value.i > DB_MAX_NUMERIC_PRECISION)
				{
				  PT_ERRORmf2 (this_parser, dt,
					       MSGCAT_SET_PARSER_SEMANTIC,
					       MSGCAT_SEMANTIC_PREC_TOO_BIG,
					       prec->info.value.data_value.i,
					       DB_MAX_NUMERIC_PRECISION);
				}
			  }

			SET_CONTAINER_2 (ctn, FROM_NUMBER (typ), dt);
			$$ = ctn;

			if (prec)
			  parser_free_node (this_parser, prec);
			if (scale)
			  parser_free_node (this_parser, scale);

		DBG_PRINT}}
	;

opt_prec_1
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| '(' unsigned_integer ')'
		{{

			$$ = $2;

		DBG_PRINT}}
	;

opt_padding
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| '(' unsigned_integer ')'
		{{

			$$ = NULL;

		DBG_PRINT}}
	;

opt_prec_2
	: /* empty */
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, NULL, NULL);
			$$ = ctn;

		DBG_PRINT}}
	| '(' unsigned_integer ')'
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, $2, NULL);
			$$ = ctn;

		DBG_PRINT}}
	| '(' unsigned_integer ',' unsigned_integer ')'
		{{

			container_2 ctn;
			SET_CONTAINER_2 (ctn, $2, $4);
			$$ = ctn;

		DBG_PRINT}}
	;

opt_collation
	: /* empty */
		{{

			$$ = NULL;

		DBG_PRINT}}
	| collation_spec
		{{

			$$=$1;

		DBG_PRINT}}
	;

collation_spec
	: COLLATE char_string_literal
		{{

			$$ = $2;

		DBG_PRINT}}
	| COLLATE IdName
		{{
			PT_NODE *node;

			node = parser_new_node (this_parser, PT_VALUE);

			if (node)
			  {
			    node->type_enum = PT_TYPE_VARCHAR;
			    node->info.value.string_type = ' ';
			    node->info.value.data_value.str =
			      pt_append_bytes (this_parser, NULL, $2, strlen ($2));
			    PT_NODE_PRINT_VALUE_TO_TEXT (this_parser, node);
			  }

			$$ = node;
		DBG_PRINT}}
	;

set_type
	: SEQUENCE_OF
		{{

			$$ = PT_TYPE_SEQUENCE;

		DBG_PRINT}}
	| of_container opt_of
		{{

			$$ = $1;

		DBG_PRINT}}
	;

opt_of
	: /* empty */
	| OF
	;

literal_
	: literal_w_o_param
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

literal_w_o_param
	: unsigned_integer
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| unsigned_real
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| char_string_literal
		{{
		
			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| bit_string_literal
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| host_param_input
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| Null
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);
			if (node)
			  PT_SET_NULL_NODE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| NA
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);
			if (node)
			  PT_SET_NULL_NODE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| date_or_time_literal
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| boolean
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

boolean
	: True
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);
			if (node)
			  {
			    node->info.value.text = "true";
			    node->info.value.data_value.i = 1;
			    node->type_enum = PT_TYPE_LOGICAL;
			  }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| False
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);
			if (node)
			  {
			    node->info.value.text = "false";
			    node->info.value.data_value.i = 0;
			    node->type_enum = PT_TYPE_LOGICAL;
			  }
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| UNKNOWN
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);
			if (node)
			  PT_SET_NULL_NODE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

of_container
	: SEQUENCE
		{{

			$$ = PT_TYPE_SEQUENCE;

		DBG_PRINT}}
	| LIST
		{{

			$$ = PT_TYPE_SEQUENCE;

		DBG_PRINT}}
	;

identifier_list
	: identifier_list ',' identifier
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| identifier
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

simple_path_id_list
	: simple_path_id_list ',' simple_path_id
		{{

			$$ = parser_make_link ($1, $3);
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| simple_path_id
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

identifier
	: IdName
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);

			if (p)
			  {
			    int size_in;
			    char *str_name = $1;

			    size_in = strlen(str_name);

			    PARSER_SAVE_ERR_CONTEXT (p, @$.buffer_pos)
			    str_name = pt_check_identifier (this_parser, p, 
							    str_name, size_in);
			    p->info.name.original = str_name;
			  }
			$$ = p;

		DBG_PRINT}}
	| BracketDelimitedIdName
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);

			if (p)
			  {
			    int size_in;
			    char *str_name = $1;

			    size_in = strlen(str_name);		    

			    PARSER_SAVE_ERR_CONTEXT (p, @$.buffer_pos)
			    str_name = pt_check_identifier (this_parser, p, 
							    str_name, size_in);
			    p->info.name.original = str_name;
			  }
			$$ = p;

		DBG_PRINT}}
	| BacktickDelimitedIdName
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);

			if (p)
			  {
			    int size_in;
			    char *str_name = $1;

			    size_in = strlen(str_name);	

			    PARSER_SAVE_ERR_CONTEXT (p, @$.buffer_pos)
			    str_name = pt_check_identifier (this_parser, p, 
							    str_name, size_in);
			    p->info.name.original = str_name;
			  }
			$$ = p;

		DBG_PRINT}}
	| DelimitedIdName
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);

			if (p)
			  {
			    int size_in;
			    char *str_name = $1;

			    size_in = strlen(str_name);

			    PARSER_SAVE_ERR_CONTEXT (p, @$.buffer_pos)
			    str_name = pt_check_identifier (this_parser, p,
							    str_name, size_in);
			    p->info.name.original = str_name;
			  }
			$$ = p;

		DBG_PRINT}}
/*{{{*/
	| ANALYZE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ARCHIVE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CACHE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CAPACITY
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CHARACTER_SET_
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CHARSET
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| CHR
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			    p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| COLLATION
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| COLUMNS
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| COMMITTED
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| COST
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DECREMENT
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ELT
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ENCRYPT
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| EXPLAIN
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}		
        | FULLSCAN
                {{

                        PT_NODE *p = parser_new_node (this_parser, PT_NAME);
                        if (p)
                          p->info.name.original = $1;
                        $$ = p;
                        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
	| GE_INF_
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GE_LE_
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GE_LT_
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GRANTS
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GROUP_CONCAT
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GROUPS
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GT_INF_
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GT_LE_
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| GT_LT_
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| HEADER
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| HEAP
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| INCREMENT
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| INDEX_PREFIX
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| INDEXES
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}		
	| INF_LE_
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| INF_LT_
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| INSTANCES
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| JAVA
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| JSON
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;

		DBG_PRINT}}
	| KEYS
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| LOCK_
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| LOG
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| MAXIMUM
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| MEMBERS
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| MINVALUE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| NAME
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| NOCACHE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| NOMAXVALUE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| NOMINVALUE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| OFFSET
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| OWNER
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| PAGE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| PASSWORD
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| RANGE_
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| REMOVE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| REORGANIZE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| REPEATABLE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SEPARATOR
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
        | SHOW
                {{

                        PT_NODE *p = parser_new_node (this_parser, PT_NAME);
                        if (p)
                          p->info.name.original = $1;
                        $$ = p;
                        PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

                DBG_PRINT}}
	| SLOTS
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SLOTTED
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| STABILITY
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| STATEMENT
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| STATUS
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| STDDEV
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| STDDEV_POP
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| STDDEV_SAMP
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SYSTEM
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| TABLES
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| TEXT
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;

		DBG_PRINT}}
	| TIMEOUT
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| TRACE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| UNCOMMITTED
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| VAR_POP
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| VAR_SAMP
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| VARIANCE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| VOLUME
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ADDDATE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DATE_ADD
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| DATE_SUB
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| IFNULL
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| ISNULL
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| LCASE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| QUARTER
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| STR_TO_DATE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| SUBDATE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| UCASE
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| WEEK
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p)
			  p->info.name.original = $1;
			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
/*}}}*/
	| NULLS
		{{

			PT_NODE *p = parser_new_node (this_parser, PT_NAME);
			if (p != NULL)
			  {
			    p->info.name.original = $1;
			  }

			$$ = p;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

escape_literal
	: string_literal_or_input_hv
		{{

			PT_NODE *node = $1;
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}	
	| Null
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);
			if (node)
			  PT_SET_NULL_NODE (node);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

string_literal_or_input_hv
	: char_string_literal
		{{
			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| host_param_input
		{{
			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
        ;


char_string_literal
	: char_string_literal CHAR_STRING
		{{

			PT_NODE *str = $1;
			if (str)
			  {
			    str->info.value.data_value.str =
			      pt_append_bytes (this_parser, str->info.value.data_value.str, $2,
					       strlen ($2));
			    str->info.value.text = NULL;
			    PT_NODE_PRINT_VALUE_TO_TEXT (this_parser, str);
			  }

			$$ = str;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| char_string
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

char_string
	: CHAR_STRING
		{{
			PT_NODE *node = NULL;

			node = pt_create_string_literal (this_parser,
						         PT_TYPE_VARCHAR,
							 $1);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| NCHAR_STRING
		{{
			PT_NODE *node = NULL;

			node = pt_create_string_literal (this_parser,
							      PT_TYPE_VARCHAR,
							      $1);
			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;


bit_string_literal
	: bit_string_literal CHAR_STRING
		{{

			PT_NODE *str = $1;
			if (str)
			  {
			    str->info.value.data_value.str =
			      pt_append_bytes (this_parser, str->info.value.data_value.str, $2,
					       strlen ($2));
			    str->info.value.text = NULL;
			    PT_NODE_PRINT_VALUE_TO_TEXT (this_parser, str);
			  }

			$$ = str;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| bit_string
		{{

			$$ = $1;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

bit_string
	: BIT_STRING
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);

			if (node)
			  {
			    node->type_enum = PT_TYPE_VARBIT;
			    node->info.value.string_type = 'B';
			    node->info.value.data_value.str =
			      pt_append_bytes (this_parser, NULL, $1, strlen ($1));
			    PT_NODE_PRINT_VALUE_TO_TEXT (this_parser, node);
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	| HEX_STRING
		{{

			PT_NODE *node = parser_new_node (this_parser, PT_VALUE);

			if (node)
			  {
			    node->type_enum = PT_TYPE_VARBIT;
			    node->info.value.string_type = 'X';
			    node->info.value.data_value.str =
			      pt_append_bytes (this_parser, NULL, $1, strlen ($1));
			    PT_NODE_PRINT_VALUE_TO_TEXT (this_parser, node);
			  }

			$$ = node;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

unsigned_integer
	: UNSIGNED_INTEGER
		{{

			PT_NODE *val = parser_new_node (this_parser, PT_VALUE);
			if (val)
			  {
			    val->info.value.text = $1;

			    if ((strlen (val->info.value.text) <= 9) ||
				(strlen (val->info.value.text) == 10 &&
				 (val->info.value.text[0] == '0' || val->info.value.text[0] == '1')))
			      {
				val->info.value.data_value.i = atol ($1);
				val->type_enum = PT_TYPE_INTEGER;
			      }
			    else if ((strlen (val->info.value.text) <= 18) ||
				     (strlen (val->info.value.text) == 19 &&
				      (val->info.value.text[0] >= '0' &&
				       val->info.value.text[0] <= '8')))
			      {
				val->info.value.data_value.bigint = atoll ($1);
				val->type_enum = PT_TYPE_BIGINT;
			      }
			    else
			      {
				const char *max_big_int = "9223372036854775807";

				if ((strlen (val->info.value.text) == 19) &&
				    (strcmp (val->info.value.text, max_big_int) <= 0))
				  {
				    val->info.value.data_value.bigint = atoll ($1);
				    val->type_enum = PT_TYPE_BIGINT;
				  }
				else
				  {
				    val->type_enum = PT_TYPE_NUMERIC;
				    val->info.value.data_value.str =
				      pt_append_bytes (this_parser, NULL,
						       val->info.value.text,
						       strlen (val->info.
							       value.text));
				  }
			      }
			  }

			$$ = val;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

unsigned_real
	: UNSIGNED_REAL
		{{

			PT_NODE *val = parser_new_node (this_parser, PT_VALUE);
			if (val)
			  {

			    if (strchr ($1, 'E') != NULL || strchr ($1, 'e') != NULL
			        || strchr ($1, 'F') != NULL || strchr ($1, 'f') != NULL)
			      {

				val->info.value.text = $1;
				val->type_enum = PT_TYPE_DOUBLE;
				val->info.value.data_value.d = atof ($1);
			      }
			    else
			      {
				val->info.value.text = $1;
				val->type_enum = PT_TYPE_NUMERIC;
				val->info.value.data_value.str =
				  pt_append_bytes (this_parser, NULL,
						   val->info.value.text,
						   strlen (val->info.value.
							   text));
			      }
			  }

			$$ = val;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)

		DBG_PRINT}}
	;

date_or_time_literal
	: Date CHAR_STRING
		{{
			PT_NODE *val;

			val = pt_create_date_value (this_parser, PT_TYPE_DATE, $2);

			$$ = val;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		DBG_PRINT}}
	| Time CHAR_STRING
		{{
			PT_NODE *val;

			val = pt_create_date_value (this_parser, PT_TYPE_TIME, $2);

			$$ = val;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		DBG_PRINT}}
	| DATETIME CHAR_STRING
		{{
			PT_NODE *val;

			val = pt_create_date_value (this_parser, PT_TYPE_DATETIME, $2);			

			$$ = val;
			PARSER_SAVE_ERR_CONTEXT ($$, @$.buffer_pos)
		DBG_PRINT}}
	;

opt_all
	: /* empty */
	| ALL
	;


%%

extern FILE *yyin;

void
_push_msg (int code, int line)
{
  PRINT_2 ("push msg called: %d at line %d\n", code, line);
  g_msg[msg_ptr++] = code;
}

void
pop_msg ()
{
  msg_ptr--;
}


int yyline = 0;
int yycolumn = 0;
int yycolumn_end = 0;

int parser_function_code = PT_EMPTY;

static PT_NODE *
parser_make_expr_with_func (PARSER_CONTEXT * parser, FUNC_TYPE func_code,
			    PT_NODE * args_list)
{
  PT_NODE *node = NULL;
  PT_NODE *node_function = NULL;

  assert (func_code == F_INSERT_SUBSTRING || func_code == F_ELT);

  node_function = parser_new_node (parser, PT_FUNCTION);
  if (node_function != NULL)
    {
      node_function->info.function.function_type = func_code;
      node_function->info.function.arg_list = args_list;

      node =
	parser_make_expression (parser, PT_FUNCTION_HOLDER, node_function, NULL,
				NULL);
    }

  return node;
}

PT_NODE *
parser_make_expression (PARSER_CONTEXT * parser, PT_OP_TYPE OP, PT_NODE * arg1, PT_NODE * arg2,
			PT_NODE * arg3)
{
  PT_NODE *expr;
  expr = parser_new_node (parser, PT_EXPR);
  if (expr)
    {
      expr->info.expr.op = OP;
      if (pt_is_operator_logical (expr->info.expr.op))
	{
	  expr->type_enum = PT_TYPE_LOGICAL;
	}

      expr->info.expr.arg1 = arg1;
      expr->info.expr.arg2 = arg2;
      expr->info.expr.arg3 = arg3;

      if (parser_instnum_check == 1 && !pt_instnum_compatibility (expr))
	{
	  PT_ERRORmf2 (parser, expr, MSGCAT_SET_PARSER_SEMANTIC,
		       MSGCAT_SEMANTIC_INSTNUM_COMPATIBILITY_ERR,
		       "INST_NUM() or ROWNUM", "INST_NUM() or ROWNUM");
	}

      if (parser_groupbynum_check == 1 && !pt_groupbynum_compatibility (expr))
	{
	  PT_ERRORmf2 (parser, expr, MSGCAT_SET_PARSER_SEMANTIC,
		       MSGCAT_SEMANTIC_INSTNUM_COMPATIBILITY_ERR,
		       "GROUPBY_NUM()", "GROUPBY_NUM()");
	}

      if (parser_orderbynum_check == 1 && !pt_orderbynum_compatibility (expr))
	{
	  PT_ERRORmf2 (parser, expr, MSGCAT_SET_PARSER_SEMANTIC,
		       MSGCAT_SEMANTIC_INSTNUM_COMPATIBILITY_ERR,
		       "ORDERBY_NUM()", "ORDERBY_NUM()");
	}

      if (OP == PT_SYS_TIME || OP == PT_SYS_DATE
	  || OP == PT_SYS_DATETIME
	  || OP == PT_UTC_TIME || OP == PT_UTC_DATE || OP == PT_UNIX_TIMESTAMP)
	{
	  parser_si_datetime = true;
	  parser_cannot_cache = true;
	}
    }

  return expr;
}

static PT_NODE *
parser_make_link (PT_NODE * list, PT_NODE * node)
{
  parser_append_node (node, list);
  return list;
}

static PT_NODE *
parser_make_link_or (PT_NODE * list, PT_NODE * node)
{
  parser_append_node_or (node, list);
  return list;
}

static bool parser_cannot_cache_stack_default[STACK_SIZE];
static bool *parser_cannot_cache_stack = parser_cannot_cache_stack_default;
static int parser_cannot_cache_sp = 0;
static int parser_cannot_cache_limit = STACK_SIZE;

static void
parser_save_and_set_cannot_cache (bool value)
{
  if (parser_cannot_cache_sp >= parser_cannot_cache_limit)
    {
      int new_size = parser_cannot_cache_limit * 2 * sizeof (bool);
      bool *new_p = malloc (new_size);
      if (new_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, new_size);
	  return;
	}

      memcpy (new_p, parser_cannot_cache_stack, parser_cannot_cache_limit * sizeof (bool));
      if (parser_cannot_cache_stack != parser_cannot_cache_stack_default)
	free (parser_cannot_cache_stack);

      parser_cannot_cache_stack = new_p;
      parser_cannot_cache_limit *= 2;
    }

  assert (parser_cannot_cache_sp >= 0);
  parser_cannot_cache_stack[parser_cannot_cache_sp++] = parser_cannot_cache;
  parser_cannot_cache = value;
}

static void
parser_restore_cannot_cache ()
{
  assert (parser_cannot_cache_sp >= 1);
  parser_cannot_cache = parser_cannot_cache_stack[--parser_cannot_cache_sp];
}

static int parser_si_datetime_saved;

static void
parser_save_and_set_si_datetime (int value)
{
  parser_si_datetime_saved = parser_si_datetime;
  parser_si_datetime = value;
}

static void
parser_restore_si_datetime ()
{
  parser_si_datetime = parser_si_datetime_saved;
}

static int parser_wjc_stack_default[STACK_SIZE];
static int *parser_wjc_stack = parser_wjc_stack_default;
static int parser_wjc_sp = 0;
static int parser_wjc_limit = STACK_SIZE;

static void
parser_save_and_set_wjc (int value)
{
  if (parser_wjc_sp >= parser_wjc_limit)
    {
      int new_size = parser_wjc_limit * 2 * sizeof (int);
      int *new_p = malloc (new_size);
      if (new_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, new_size);
	  return;
	}

      memcpy (new_p, parser_wjc_stack, parser_wjc_limit * sizeof (int));
      if (parser_wjc_stack != parser_wjc_stack_default)
	free (parser_wjc_stack);

      parser_wjc_stack = new_p;
      parser_wjc_limit *= 2;
    }

  assert (parser_wjc_sp >= 0);
  parser_wjc_stack[parser_wjc_sp++] = parser_within_join_condition;
  parser_within_join_condition = value;
}

static void
parser_restore_wjc ()
{
  assert (parser_wjc_sp >= 1);
  parser_within_join_condition = parser_wjc_stack[--parser_wjc_sp];
}

static int parser_instnum_stack_default[STACK_SIZE];
static int *parser_instnum_stack = parser_instnum_stack_default;
static int parser_instnum_sp = 0;
static int parser_instnum_limit = STACK_SIZE;

static void
parser_save_and_set_ic (int value)
{
  if (parser_instnum_sp >= parser_instnum_limit)
    {
      int new_size = parser_instnum_limit * 2 * sizeof (int);
      int *new_p = malloc (new_size);
      if (new_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, new_size);
	  return;
	}

      memcpy (new_p, parser_instnum_stack, parser_instnum_limit * sizeof (int));
      if (parser_instnum_stack != parser_instnum_stack_default)
	free (parser_instnum_stack);

      parser_instnum_stack = new_p;
      parser_instnum_limit *= 2;
    }

  assert (parser_instnum_sp >= 0);
  parser_instnum_stack[parser_instnum_sp++] = parser_instnum_check;
  parser_instnum_check = value;
}

static void
parser_restore_ic ()
{
  assert (parser_instnum_sp >= 1);
  parser_instnum_check = parser_instnum_stack[--parser_instnum_sp];
}

static int parser_groupbynum_stack_default[STACK_SIZE];
static int *parser_groupbynum_stack = parser_groupbynum_stack_default;
static int parser_groupbynum_sp = 0;
static int parser_groupbynum_limit = STACK_SIZE;

static void
parser_save_and_set_gc (int value)
{
  if (parser_groupbynum_sp >= parser_groupbynum_limit)
    {
      int new_size = parser_groupbynum_limit * 2 * sizeof (int);
      int *new_p = malloc (new_size);
      if (new_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, new_size);
	  return;
	}

      memcpy (new_p, parser_groupbynum_stack, parser_groupbynum_limit * sizeof (int));
      if (parser_groupbynum_stack != parser_groupbynum_stack_default)
	free (parser_groupbynum_stack);

      parser_groupbynum_stack = new_p;
      parser_groupbynum_limit *= 2;
    }

  assert (parser_groupbynum_sp >= 0);
  parser_groupbynum_stack[parser_groupbynum_sp++] = parser_groupbynum_check;
  parser_groupbynum_check = value;
}

static void
parser_restore_gc ()
{
  assert (parser_groupbynum_sp >= 1);
  parser_groupbynum_check = parser_groupbynum_stack[--parser_groupbynum_sp];
}

static int parser_orderbynum_stack_default[STACK_SIZE];
static int *parser_orderbynum_stack = parser_orderbynum_stack_default;
static int parser_orderbynum_sp = 0;
static int parser_orderbynum_limit = STACK_SIZE;

static void
parser_save_and_set_oc (int value)
{
  if (parser_orderbynum_sp >= parser_orderbynum_limit)
    {
      int new_size = parser_orderbynum_limit * 2 * sizeof (int);
      int *new_p = malloc (new_size);
      if (new_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, new_size);
	  return;
	}

      memcpy (new_p, parser_orderbynum_stack, parser_orderbynum_limit * sizeof (int));
      if (parser_orderbynum_stack != parser_orderbynum_stack_default)
	free (parser_orderbynum_stack);

      parser_orderbynum_stack = new_p;
      parser_orderbynum_limit *= 2;
    }

  assert (parser_orderbynum_sp >= 0);
  parser_orderbynum_stack[parser_orderbynum_sp++] = parser_orderbynum_check;
  parser_orderbynum_check = value;
}

static void
parser_restore_oc ()
{
  assert (parser_orderbynum_sp >= 1);
  parser_orderbynum_check = parser_orderbynum_stack[--parser_orderbynum_sp];
}

static int parser_sqc_stack_default[STACK_SIZE];
static int *parser_sqc_stack = parser_sqc_stack_default;
static int parser_sqc_sp = 0;
static int parser_sqc_limit = STACK_SIZE;

static void
parser_save_and_set_sqc (int value)
{
  if (parser_sqc_sp >= parser_sqc_limit)
    {
      int new_size = parser_sqc_limit * 2 * sizeof (int);
      int *new_p = malloc (new_size);
      if (new_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, new_size);
	  return;
	}

      memcpy (new_p, parser_sqc_stack, parser_sqc_limit * sizeof (int));
      if (parser_sqc_stack != parser_sqc_stack_default)
	free (parser_sqc_stack);

      parser_sqc_stack = new_p;
      parser_sqc_limit *= 2;
    }

  assert (parser_sqc_sp >= 0);
  parser_sqc_stack[parser_sqc_sp++] = parser_subquery_check;
  parser_subquery_check = value;
}

static void
parser_restore_sqc ()
{
  assert (parser_sqc_sp >= 1);
  parser_subquery_check = parser_sqc_stack[--parser_sqc_sp];
}

static int parser_hvar_stack_default[STACK_SIZE];
static int *parser_hvar_stack = parser_hvar_stack_default;
static int parser_hvar_sp = 0;
static int parser_hvar_limit = STACK_SIZE;

#if defined (ENABLE_UNUSED_FUNCTION)
static void
parser_save_and_set_hvar (int value)
{
  if (parser_hvar_sp >= parser_hvar_limit)
    {
      int new_size = parser_hvar_limit * 2 * sizeof (int);
      int *new_p = malloc (new_size);
      if (new_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, new_size);
	  return;
	}

      memcpy (new_p, parser_hvar_stack, parser_hvar_limit * sizeof (int));
      if (parser_hvar_stack != parser_hvar_stack_default)
	free (parser_hvar_stack);

      parser_hvar_stack = new_p;
      parser_hvar_limit *= 2;
    }

  assert (parser_hvar_sp >= 0);
  parser_hvar_stack[parser_hvar_sp++] = parser_hostvar_check;
  parser_hostvar_check = value;
}

static void
parser_restore_hvar ()
{
  assert (parser_hvar_sp >= 1);
  parser_hostvar_check = parser_hvar_stack[--parser_hvar_sp];
}
#endif

static PT_NODE *parser_alter_node_saved;

static void
parser_save_alter_node (PT_NODE * node)
{
  parser_alter_node_saved = node;
}

static PT_NODE *
parser_get_alter_node ()
{
  return parser_alter_node_saved;
}

static PT_NODE *parser_attr_def_one_saved;

static void
parser_save_attr_def_one (PT_NODE * node)
{
  parser_attr_def_one_saved = node;
}

static PT_NODE *
parser_get_attr_def_one ()
{
  return parser_attr_def_one_saved;
}

static PT_NODE *parser_orderby_node_stack_default[STACK_SIZE];
static PT_NODE **parser_orderby_node_stack = parser_orderby_node_stack_default;
static int parser_orderby_node_sp = 0;
static int parser_orderby_node_limit = STACK_SIZE;

static void
parser_push_orderby_node (PT_NODE * node)
{
  if (parser_orderby_node_sp >= parser_orderby_node_limit)
    {
      int new_size = parser_orderby_node_limit * 2 * sizeof (PT_NODE **);
      PT_NODE **new_p = malloc (new_size);
      if (new_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, new_size);
	  return;
	}

      memcpy (new_p, parser_orderby_node_stack, parser_orderby_node_limit * sizeof (PT_NODE *));
      if (parser_orderby_node_stack != parser_orderby_node_stack_default)
	free (parser_orderby_node_stack);

      parser_orderby_node_stack = new_p;
      parser_orderby_node_limit *= 2;
    }

  assert (parser_orderby_node_sp >= 0);
  parser_orderby_node_stack[parser_orderby_node_sp++] = node;
}

static PT_NODE *
parser_top_orderby_node ()
{
  assert (parser_orderby_node_sp >= 1);
  return parser_orderby_node_stack[parser_orderby_node_sp - 1];
}

static PT_NODE *
parser_pop_orderby_node ()
{
  assert (parser_orderby_node_sp >= 1);
  return parser_orderby_node_stack[--parser_orderby_node_sp];
}

static PT_NODE *parser_select_node_stack_default[STACK_SIZE];
static PT_NODE **parser_select_node_stack = parser_select_node_stack_default;
static int parser_select_node_sp = 0;
static int parser_select_node_limit = STACK_SIZE;

static void
parser_push_select_stmt_node (PT_NODE * node)
{
  if (parser_select_node_sp >= parser_select_node_limit)
    {
      int new_size = parser_select_node_limit * 2 * sizeof (PT_NODE **);
      PT_NODE **new_p = malloc (new_size);
      if (new_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, new_size);
	  return;
	}

      memcpy (new_p, parser_select_node_stack, parser_select_node_limit * sizeof (PT_NODE *));
      if (parser_select_node_stack != parser_select_node_stack_default)
	free (parser_select_node_stack);

      parser_select_node_stack = new_p;
      parser_select_node_limit *= 2;
    }

  assert (parser_select_node_sp >= 0);
  parser_select_node_stack[parser_select_node_sp++] = node;
}

static PT_NODE *
parser_top_select_stmt_node ()
{
  assert (parser_select_node_sp >= 1);
  return parser_select_node_stack[parser_select_node_sp - 1];
}

static PT_NODE *
parser_pop_select_stmt_node ()
{
  assert (parser_select_node_sp >= 1);
  return parser_select_node_stack[--parser_select_node_sp];
}

#if defined (ENABLE_UNUSED_FUNCTION)
static bool
parser_is_select_stmt_node_empty ()
{
  return parser_select_node_sp < 1;
}
#endif

static PT_NODE *parser_hint_node_stack_default[STACK_SIZE];
static PT_NODE **parser_hint_node_stack = parser_hint_node_stack_default;
static int parser_hint_node_sp = 0;
static int parser_hint_node_limit = STACK_SIZE;

static void
parser_push_hint_node (PT_NODE * node)
{
  if (parser_hint_node_sp >= parser_hint_node_limit)
    {
      int new_size = parser_hint_node_limit * 2 * sizeof (PT_NODE **);
      PT_NODE **new_p = malloc (new_size);
      if (new_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, new_size);
	  return;
	}

      memcpy (new_p, parser_hint_node_stack, parser_hint_node_limit * sizeof (PT_NODE *));
      if (parser_hint_node_stack != parser_hint_node_stack_default)
	free (parser_hint_node_stack);

      parser_hint_node_stack = new_p;
      parser_hint_node_limit *= 2;
    }

  assert (parser_hint_node_sp >= 0);
  parser_hint_node_stack[parser_hint_node_sp++] = node;
}

static PT_NODE *
parser_top_hint_node ()
{
  assert (parser_hint_node_sp >= 1);
  return parser_hint_node_stack[parser_hint_node_sp - 1];
}

static PT_NODE *
parser_pop_hint_node ()
{
  assert (parser_hint_node_sp >= 1);
  return parser_hint_node_stack[--parser_hint_node_sp];
}

static bool
parser_is_hint_node_empty ()
{
  return parser_hint_node_sp < 1;
}

static int
parser_count_list (PT_NODE * list)
{
  int i = 0;
  PT_NODE *p = list;

  while (p)
    {
      p = p->next;
      i++;
    }

  return i;
}

static int
parser_count_prefix_columns (PT_NODE * list, int * arg_count)
{
  int i = 0;
  PT_NODE *p = list;
  
  *arg_count = 0;
  
  while (p)
    {
      if (p->node_type == PT_SORT_SPEC)
	{
	  if (p->info.sort_spec.expr->node_type == PT_FUNCTION)
	    {
	      PT_NODE *expr = p->info.sort_spec.expr;
	      PT_NODE *arg_list = expr->info.function.arg_list;
	      if ((arg_list != NULL) && (arg_list->next == NULL)
		  && (arg_list->node_type == PT_VALUE))
		{
		  /* it might be a prefixed column */
		  i++;
		}
	    }
	}

      /* count all arguments */
      *arg_count = (*arg_count) + 1;
      p = p->next;
    }

  return i;
}

static void
parser_initialize_parser_context (void)
{
  parser_select_node_sp = 0;
  parser_orderby_node_sp = 0;
  parser_orderbynum_sp = 0;
  parser_groupbynum_sp = 0;
  parser_instnum_sp = 0;
  parser_wjc_sp = 0;
  parser_cannot_cache_sp = 0;
  parser_hint_node_sp = 0;
}


static void
parser_remove_dummy_select (PT_NODE ** ent_inout)
{
  PT_NODE *ent = *ent_inout;

  if (ent
      && ent->info.spec.derived_table_type == PT_IS_SUBQUERY
      && ent->info.spec.as_attr_list == NULL /* no attr_list */ )
    {

      PT_NODE *subq, *new_ent;

      /* remove dummy select from FROM clause
       *
       * for example:
       * case 1 (simple spec):
       *              FROM (SELECT * FROM x) AS s
       * -> FROM x AS s
       * case 2 (nested spec):
       *              FROM (SELECT * FROM (SELECT a, b FROM bas) y(p, q)) x
       * -> FROM (SELECT a, b FROM bas) x(p, q)
       */
      if ((subq = ent->info.spec.derived_table)
	  && subq->node_type == PT_SELECT
	  && PT_SELECT_INFO_IS_FLAGED (subq, PT_SELECT_INFO_DUMMY)
	  && subq->info.query.q.select.from)
	{
	  if (PT_SELECT_INFO_IS_FLAGED (subq, PT_SELECT_INFO_FOR_UPDATE))
  	    {
  	      /* the FOR UPDATE clause cannot be used in subqueries */
  	      PT_ERRORm (this_parser, subq, MSGCAT_SET_PARSER_SEMANTIC,
			 MSGCAT_SEMANTIC_INVALID_USE_FOR_UPDATE_CLAUSE);
	    }
	  else
	    {
	      new_ent = subq->info.query.q.select.from;
	      subq->info.query.q.select.from = NULL;

	      /* free, reset new_spec's range_var, as_attr_list */
	      if (new_ent->info.spec.range_var)
		{
		  parser_free_node (this_parser, new_ent->info.spec.range_var);
		  new_ent->info.spec.range_var = NULL;
		}

	      new_ent->info.spec.range_var = ent->info.spec.range_var;
	      ent->info.spec.range_var = NULL;

	      /* free old ent, reset to new_ent */
	      parser_free_node (this_parser, ent);
	      *ent_inout = new_ent;
	    }
	}
    }
}


static PT_NODE *
parser_make_date_lang (int arg_cnt, PT_NODE * arg3)
{

  if (arg3 && arg_cnt == 3)
    {
      char *lang_str;
      PT_NODE *date_lang = parser_new_node (this_parser, PT_VALUE);

      if (date_lang)
	{
	  date_lang->type_enum = PT_TYPE_INTEGER;
	  if (arg3->type_enum != PT_TYPE_VARCHAR)
	    {
	      PT_ERROR (this_parser, arg3,
			"argument 3 must be character string");
	    }
	  else if (arg3->info.value.data_value.str != NULL)
	    {
	      int flag = 0;

	      lang_str = (char *) arg3->info.value.data_value.str->bytes;
	      if (lang_set_flag_from_lang (lang_str, 1, 1, &flag))
		{
		  PT_ERROR (this_parser, arg3, "check syntax at 'date_lang'");
		}
	      date_lang->info.value.data_value.i = (long) flag;
	    }
	}
      parser_free_node (this_parser, arg3);

      return date_lang;
    }
  else
    {
      PT_NODE *date_lang = parser_new_node (this_parser, PT_VALUE);
      if (date_lang)
	{
	  int flag = 0;

	  date_lang->type_enum = PT_TYPE_INTEGER;
	  
	  if (lang_set_flag_from_lang (NULL, (arg_cnt == 1) ? 0 : 1, 0, &flag))
            {
	      PT_ERROR (this_parser, pt_top(this_parser), "check syntax at 'date_lang'");
            }
	  date_lang->info.value.data_value.i = (long) flag;
	}

      return date_lang;
    }
}

static PT_NODE *
parser_make_number_lang (const int argc)
{
  PT_NODE *number_lang = parser_new_node (this_parser, PT_VALUE);

  if (number_lang)
    {
      int flag = 0;

      number_lang->type_enum = PT_TYPE_INTEGER;

      if (lang_set_flag_from_lang (NULL, (argc >= 2) ? 1 : 0, (argc == 3) ? 1 : 0, &flag))
        {
	  PT_ERROR (this_parser, pt_top(this_parser), "check syntax at 'number_lang'");
        }
      number_lang->info.value.data_value.i = (long) flag;
    }

  return number_lang;
}

PT_NODE *
parser_main (PARSER_CONTEXT * parser)
{
  PT_NODE *statement = NULL;
  long desc_index = 0;
  long i, top;
  int rv;

  PARSER_CONTEXT *this_parser_saved;

  if (!parser)
    return 0;

  parser_output_host_index = parser_input_host_index = desc_index = 0;

  this_parser_saved = this_parser;

  this_parser = parser;

  dbcs_start_input ();

  yyline = 1;
  yycolumn = yycolumn_end = 1;
  yybuffer_pos=0;
  rsql_yylloc.buffer_pos=0;

  g_query_string = NULL;
  g_query_string_len = 0;
  g_last_stmt = NULL;
  g_original_buffer_len = 0;

  rv = yyparse ();
  pt_cleanup_hint (parser, parser_hint_table);

  if (pt_has_error (parser) || parser->stack_top <= 0 || !parser->node_stack)
    {
      statement = NULL;
    }
  else
    {
      statement = parser->node_stack[0];
      
      /* record parser_input_host_index into parser->host_var_count for later use;
	 e.g. parser_set_host_variables() */
      parser->host_var_count = parser_input_host_index;
      if (parser->host_var_count > 0)
	{
	  /* allocate place holder for host variables */
	  parser->host_variables = (DB_VALUE *)
	      malloc (parser->host_var_count * sizeof (DB_VALUE));
	  if (parser->host_variables)
	    {
	      memset (parser->host_variables, 0,
    			  parser->host_var_count * sizeof (DB_VALUE));
	    }
	  else
	    {
	      statement = NULL;
	      goto end;
	    }

	  for (i = 0; i < parser->host_var_count; i++)
	    {
	      DB_MAKE_NULL (&parser->host_variables[i]);
	    }
	}
    }

end:
  this_parser = this_parser_saved;
  return statement;
}

#if defined (ENABLE_UNUSED_FUNCTION)
extern int parser_yyinput_single_mode;
int
parse_one_statement (int state)
{
  int rv;

  if (state == 0)
    {
      return 0;
    }

  parser_yyinput_single_mode = 1;
  yybuffer_pos=0;
  rsql_yylloc.buffer_pos=0;

  g_query_string = NULL;
  g_query_string_len = 0;
  g_last_stmt = NULL;
  g_original_buffer_len = 0;


  rv = yyparse ();
  pt_cleanup_hint (this_parser, parser_hint_table);

  if (!parser_statement_OK)
    {
      parser_statement_OK = 1;
    }

  if (!parser_yyinput_single_mode)	/* eof */
    {
      return 1;
    }

  return 0;
}
#endif

PT_HINT parser_hint_table[] = {
  {"ORDERED", NULL, PT_HINT_ORDERED}
  ,
  {"USE_NL", NULL, PT_HINT_USE_NL}
  ,
  {"USE_IDX", NULL, PT_HINT_USE_IDX}
  ,
  {"RECOMPILE", NULL, PT_HINT_RECOMPILE}
  ,
  {"QUERY_CACHE", NULL, PT_HINT_QUERY_CACHE}
  ,
  {"REEXECUTE", NULL, PT_HINT_REEXECUTE}
  ,
  {"USE_DESC_IDX", NULL, PT_HINT_USE_IDX_DESC}
  ,
  {"NO_COVERING_IDX", NULL, PT_HINT_NO_COVERING_IDX}
  ,
  {"FORCE_PAGE_ALLOCATION", NULL, PT_HINT_FORCE_PAGE_ALLOCATION}
  ,
  {"NO_DESC_IDX", NULL, PT_HINT_NO_IDX_DESC}
  ,
  {"NO_MULTI_RANGE_OPT", NULL, PT_HINT_NO_MULTI_RANGE_OPT}
  ,
  {"NO_SORT_LIMIT", NULL, PT_HINT_NO_SORT_LIMIT}
  ,
  {NULL, NULL, -1}		/* mark as end */
};



static int
function_keyword_cmp (const void *f1, const void *f2)
{
  return strcasecmp (((FUNCTION_MAP *) f1)->keyword,
		     ((FUNCTION_MAP *) f2)->keyword);
}



FUNCTION_MAP *
keyword_offset (const char *text)
{
  static bool function_keyword_sorted = false;
  FUNCTION_MAP dummy;
  FUNCTION_MAP *result_key;

  if (function_keyword_sorted == false)
    {
      qsort (functions,
	     (sizeof (functions) / sizeof (functions[0])),
	     sizeof (functions[0]), function_keyword_cmp);

      function_keyword_sorted = true;
    }

  if (!text)
    {
      return NULL;
    }

  if (strlen (text) >= MAX_KEYWORD_SIZE)
    {
      return NULL;
    }

  dummy.keyword = text;

  result_key =
    (FUNCTION_MAP *) bsearch (&dummy, functions,
			      (sizeof (functions) / sizeof (functions[0])),
			      sizeof (FUNCTION_MAP), function_keyword_cmp);

  return result_key;
}


PT_NODE *
parser_keyword_func (PT_NODE * func_node, PT_NODE * args)
{
  const char *name;
  PT_NODE *node;
  PT_NODE *a1, *a2, *a3;
  FUNCTION_MAP *key;
  int c;
  PT_NODE *val, *between_ge_lt, *between;

  name = func_node->info.name.original;

  parser_function_code = PT_EMPTY;
  c = parser_count_list (args);
  key = keyword_offset (name);
  if (key == NULL)
    {
      PT_ERRORmf (this_parser, func_node, MSGCAT_SET_PARSER_SEMANTIC,
                  MSGCAT_SEMANTIC_UNKNOWN_FUNCTION, name);
      return NULL;
    }
  assert (key->op != PT_EXPLAIN);

  parser_function_code = key->op;

  a1 = a2 = a3 = NULL;
  switch (key->op)
    {
      /* arg 0 */
    case PT_PI:
    case PT_SYS_TIME:
    case PT_SYS_DATE:
    case PT_SYS_DATETIME:
    case PT_UTC_TIME:
    case PT_UTC_DATE:
    case PT_VERSION:
      if (c != 0)
	{
	  return NULL;
	}
      return parser_make_expression (this_parser, key->op, NULL, NULL, NULL);

    case PT_TRACE_STATS:
      if (c != 0)
	{
	  return NULL;
	}
      parser_cannot_cache = true;
      return parser_make_expression (this_parser, key->op, NULL, NULL, NULL);

      /* arg 0 or 1 */
    case PT_RAND:
    case PT_RANDOM:
    case PT_DRAND:
    case PT_DRANDOM:
      {
	PT_NODE *expr;

	parser_cannot_cache = true;

	if (c < 0 || c > 1)
	  {
	    return NULL;
	  }

	if (c == 1)
	  {
	    a1 = args;
	  }
	expr = parser_make_expression (this_parser, key->op, a1, NULL, NULL);
	expr->do_not_fold = 1;
	return expr;
      }

      /* arg 1 */
    case PT_ABS:
    case PT_BIN:
    case PT_CEIL:
    case PT_CHAR_LENGTH:	/* char_length, length, lengthb */
    case PT_EXP:
    case PT_FLOOR:
    case PT_LAST_DAY:
    case PT_SIGN:
    case PT_SQRT:
    case PT_ACOS:
    case PT_ASIN:
    case PT_COS:
    case PT_SIN:
    case PT_TAN:
    case PT_COT:
    case PT_DEGREES:
    case PT_RADIANS:
    case PT_LN:
    case PT_LOG2:
    case PT_LOG10:
    case PT_TYPEOF:
    case PT_MD5:
    case PT_SPACE:
    case PT_DAYOFMONTH:
    case PT_WEEKDAY:
    case PT_DAYOFWEEK:
    case PT_DAYOFYEAR:
    case PT_TODAYS:
    case PT_FROMDAYS:
    case PT_TIMETOSEC:
    case PT_SECTOTIME:
    case PT_QUARTERF:
    case PT_HEX:
    case PT_ASCII:
    case PT_INET_ATON:
    case PT_INET_NTOA:
    case PT_SHA_ONE:	
    case PT_TO_BASE64:
    case PT_FROM_BASE64:
      if (c != 1)
        {
	  return NULL;
	}
      a1 = args;
      return parser_make_expression (this_parser, key->op, a1, NULL, NULL);

    case PT_UNIX_TIMESTAMP:
      if (c > 1)
	{
	  return NULL;
	}
      if (c == 1)
	{
	  a1 = args;
	  return parser_make_expression (this_parser, key->op, a1, NULL, NULL);
	}
      else /* no arguments */
	{
	  return parser_make_expression (this_parser, key->op, NULL, NULL, NULL);
	}

      /* arg 2 */
    case PT_DATE_FORMAT:
    case PT_TIME_FORMAT:
    case PT_FORMAT:    
      if (c != 2)
        {
	return NULL;
        }
      a1 = args;
      a2 = a1->next;
      a1->next = NULL;
      return parser_make_expression (this_parser, key->op, a1, a2, parser_make_date_lang (2, NULL));

    case PT_LOG:
    case PT_MONTHS_BETWEEN:
    case PT_NVL:
    case PT_POWER:
    case PT_ATAN2:
    case PT_DATEDIFF:
    case PT_TIMEDIFF:
    case PT_REPEAT:
    case PT_MAKEDATE:
    case PT_FINDINSET:
    case PT_SHA_TWO:
      if (c != 2)
        {
	return NULL;
        }
      a1 = args;
      a2 = a1->next;
      a1->next = NULL;
      return parser_make_expression (this_parser, key->op, a1, a2, NULL);

    case PT_ATAN:
      if (c == 1)
	{
	  a1 = args;
	  return parser_make_expression (this_parser, key->op, a1, NULL, NULL);
	}
      else if (c == 2)
	{
	  a1 = args;
	  a2 = a1->next;
	  a1->next = NULL;
	  return parser_make_expression (this_parser, PT_ATAN2, a1, a2, NULL);
	}
      else
	{
	  return NULL;
	}

      /* arg 3 */
    case PT_SUBSTRING_INDEX:
    case PT_NVL2:
    case PT_MAKETIME:
    case PT_INDEX_CARDINALITY:
    case PT_CONV:
      if (c != 3)
        {
	return NULL;
        }
      a1 = args;
      a2 = a1->next;
      a3 = a2->next;
      a1->next = a2->next = NULL;
      return parser_make_expression (this_parser, key->op, a1, a2, a3);
      
      /* arg 4 */
    case PT_WIDTH_BUCKET:
      if (c != 4)
        {
          return NULL;
        }

      a1 = args;
      a2 = a1->next;  /* a2 is the lower bound, a2->next is the upper bound */
      a3 = a2->next->next; 
      a1->next = a2->next->next = a3->next = NULL;

      val = parser_new_node (this_parser, PT_VALUE);
      if (val == NULL)
        {
          return NULL;
        }
      PT_SET_NULL_NODE (val);

      between_ge_lt = parser_make_expression (this_parser, PT_BETWEEN_GE_LT, a2, a2->next, NULL);
      if (between_ge_lt == NULL)
        {
          return NULL;
        }
      a2->next = NULL;

      between = parser_make_expression (this_parser, PT_BETWEEN, val, between_ge_lt, NULL);
      if (between == NULL)
        {
          return NULL;
        }

      between->do_not_fold = 1;

      return parser_make_expression (this_parser, key->op, a1, between, a3);

      /* arg 1 + default */
    case PT_ROUND:
      if (c == 1)
    {
      a1 = args;
      a2 = parser_new_node (this_parser, PT_VALUE);
      if (a2)
        {
          a2->type_enum = PT_TYPE_INTEGER;
          a2->info.value.data_value.i = 0;  
        }
        
      return parser_make_expression (this_parser, key->op, a1, a2, NULL);
    }
      
      if (c != 2)
        {
          return NULL;
        }

      a1 = args;
      a2 = a1->next;
      a1->next = NULL;
      
      return parser_make_expression (this_parser, key->op, a1, a2, NULL);
      
    case PT_TRUNC:
      if (c == 1)
    {
      a1 = args;
      a2 = parser_new_node (this_parser, PT_VALUE);
      if (a2)
	    {
	      /* default fmt value */
	      if (a1->node_type == PT_VALUE
	          && PT_IS_NUMERIC_TYPE (a1->type_enum))
	        {
	          a2->type_enum = PT_TYPE_INTEGER;
	          a2->info.value.data_value.i = 0;
	        }
	      else
	        {
	          a2->type_enum = PT_TYPE_VARCHAR;
	          a2->info.value.data_value.str = 
	            pt_append_bytes (this_parser, NULL, "default", 7);
	        }
	    }

	  return parser_make_expression (this_parser, key->op, a1, a2, NULL);
	}

      if (c != 2)
        {
	return NULL;
        }
      a1 = args;
      a2 = a1->next;
      a1->next = NULL;
      
      /* prevent user input "default" */
      if (a2->node_type == PT_VALUE 
          && a2->type_enum == PT_TYPE_VARCHAR
          && strcasecmp ((const char *) a2->info.value.data_value.str->bytes, "default") == 0)
        {
          PT_ERRORf (this_parser, a2, "check syntax at %s",
                     parser_print_tree (this_parser, a2));
          return NULL;
        }
      
      return parser_make_expression (this_parser, key->op, a1, a2, NULL);

      /* arg 2 + default */
    case PT_INSTR:		/* instr, instrb */
      if (c == 2)
	{
	  a3 = parser_new_node (this_parser, PT_VALUE);
	  if (a3)
	    {
	      a3->type_enum = PT_TYPE_INTEGER;
	      a3->info.value.data_value.i = 1;
	    }

	  a1 = args;
	  a2 = a1->next;
	  a1->next = NULL;

	  return parser_make_expression (this_parser, key->op, a1, a2, a3);
	}

      if (c != 3)
        {
	return NULL;
        }
      a1 = args;
      a2 = a1->next;
      a3 = a2->next;
      a1->next = a2->next = NULL;
      return parser_make_expression (this_parser, key->op, a1, a2, a3);

      /* arg 1 or 2 */
    case PT_WEEKF:
      if (c < 1 || c > 2)
        {
   	return NULL;
        }
      if (c == 1)
	{
	  a1 = args;
	  a2 = parser_new_node (this_parser, PT_VALUE);
	  if (a2)
	   {
	     a2->info.value.data_value.i = 0; /* default */
	     a2->type_enum = PT_TYPE_INTEGER;
	   }
	  return parser_make_expression (this_parser, key->op, a1, a2, NULL);
	}
      else
	{
	  a1 = args;
	  a2 = a1->next;
	  a1->next = NULL;
	  return parser_make_expression (this_parser, key->op, a1, a2, NULL);
	}

    case PT_LTRIM:
    case PT_RTRIM:
    case PT_LIKE_LOWER_BOUND:
    case PT_LIKE_UPPER_BOUND:
      if (c < 1 || c > 2)
	{
	return NULL;
        }
      a1 = args;
      if (a1)
	{
	a2 = a1->next;
        }
      a1->next = NULL;
      return parser_make_expression (this_parser, key->op, a1, a2, a3);

    case PT_FROM_UNIXTIME:
      if (c < 1 || c > 2)
	{
	return NULL;
        }
      a1 = args;
      if (a1)
	{
	a2 = a1->next;
        }
      a1->next = NULL;
      a3 = parser_make_date_lang (2, NULL);
      return parser_make_expression (this_parser, key->op, a1, a2, a3);

      /* arg 1 or 2 */
    case PT_TO_NUMBER:
      if (c < 1 || c > 2)
	{
	  push_msg (MSGCAT_SYNTAX_INVALID_TO_NUMBER);
	  rsql_yyerror_explicit (10, 10);
	  return NULL;
	}

      if (c == 2)
	{
	  PT_NODE *node = args->next;

	  if (node->node_type != PT_VALUE
              || node->type_enum != PT_TYPE_VARCHAR)
	    {
	      push_msg (MSGCAT_SYNTAX_INVALID_TO_NUMBER);
	      rsql_yyerror_explicit (10, 10);
	      return NULL;
	    }
	}

      a1 = args;
      if (a1)
	{
	a2 = a1->next;
        }
      a1->next = NULL;
      return parser_make_expression (this_parser, key->op, a1, a2, parser_make_number_lang (c));

      /* arg 2 or 3 */
    case PT_LPAD:
    case PT_RPAD:
    case PT_SUBSTRING:		/* substr, substrb */

      if (c < 2 || c > 3)
	{
	return NULL;
        }

      a1 = args;
      a2 = a1->next;
      if (a2)
	{
	  a3 = a2->next;
	  a2->next = NULL;
	}

      a1->next = NULL;

      node = parser_make_expression (this_parser, key->op, a1, a2, a3);
      if (key->op == PT_SUBSTRING)
	{
	  node->info.expr.qualifier = PT_SUBSTR;
	  PICE (node);
	}

      return node;

    case PT_ORDERBY_NUM:
      if (c != 0)
	{
	return NULL;
        }

      node = parser_new_node (this_parser, PT_EXPR);
      if (node)
	{
	  node->info.expr.op = PT_ORDERBY_NUM;
	  PT_EXPR_INFO_SET_FLAG (node, PT_EXPR_INFO_ORDERBYNUM_C);
	}

      if (parser_orderbynum_check == 0)
	{
	PT_ERRORmf2 (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
		     MSGCAT_SEMANTIC_INSTNUM_COMPATIBILITY_ERR,
		     "ORDERBY_NUM()", "ORDERBY_NUM()");
        }

      parser_groupby_exception = PT_ORDERBY_NUM;
      return node;

    case PT_INST_NUM:
      if (c != 0)
	{
	return NULL;
        }

      node = parser_new_node (this_parser, PT_EXPR);

      if (node)
	{
	  node->info.expr.op = PT_INST_NUM;
	  PT_EXPR_INFO_SET_FLAG (node, PT_EXPR_INFO_INSTNUM_C);
	}

      if (parser_instnum_check == 0)
	{
	PT_ERRORmf2 (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
		     MSGCAT_SEMANTIC_INSTNUM_COMPATIBILITY_ERR,
		     "INST_NUM() or ROWNUM", "INST_NUM() or ROWNUM");
        }

      parser_groupby_exception = PT_INST_NUM;
      return node;

    case PT_TO_CHAR:
    case PT_TO_DATE:
    case PT_TO_TIME:
    case PT_TO_DATETIME:
      if (c < 1 || c > 3)
	{
	return NULL;
        }

      a1 = args;
      a2 = a1->next;
      a1->next = NULL;

      if (a2)
	{
	  a3 = a2->next;
	}

      if (a2)
	{
	  a2->next = NULL;
	}

      if (c == 1)
	{
	  a2 = parser_new_node (this_parser, PT_VALUE);

	  if (a2)
	    {
	      PT_SET_NULL_NODE (a2);
	    }
	}

      return parser_make_expression (this_parser, key->op, a1, a2,
				     parser_make_date_lang (c, a3));

    case PT_DECODE:
      {
	int i;
	PT_NODE *case_oper, *p, *q, *r, *nodep, *node, *curr, *prev;
	int count;

	if (c < 3)
	{
	  return NULL;
        }

	case_oper = args;
	p = args->next;
	args->next = NULL;
	curr = p->next;
	p->next = NULL;

	node = parser_new_node (this_parser, PT_EXPR);
	if (node)
	  {
	    node->info.expr.op = PT_DECODE;
	    q = parser_new_node (this_parser, PT_EXPR);
	    if (q)
	      {
		q->info.expr.op = PT_EQ;
		q->info.expr.qualifier = PT_EQ_TORDER;
		q->info.expr.arg1 =
		  parser_copy_tree_list (this_parser, case_oper);
		q->info.expr.arg2 = p;
		node->info.expr.arg3 = q;
		PICE (q);
	      }

	    p = curr->next;
	    curr->next = NULL;
	    node->info.expr.arg1 = curr;
	    PICE (node);
	  }

	prev = node;
	count = parser_count_list (p);
	for (i = 1; i <= count; i++)
	  {
	    if (i % 2 == 0)
	      {
		r = p->next;
		p->next = NULL;
		nodep = parser_new_node (this_parser, PT_EXPR);
		if (nodep)
		  {
		    nodep->info.expr.op = PT_DECODE;
		    q = parser_new_node (this_parser, PT_EXPR);
		    if (q)
		      {
			q->info.expr.op = PT_EQ;
			q->info.expr.qualifier = PT_EQ_TORDER;
			q->info.expr.arg1 =
			  parser_copy_tree_list (this_parser, case_oper);
			q->info.expr.arg2 = p;
			nodep->info.expr.arg3 = q;
			PICE (nodep);
		      }
		    nodep->info.expr.arg1 = r;
		    nodep->info.expr.continued_case = 1;
		  }
		PICE (nodep);

		if (prev)
                  {
		    prev->info.expr.arg2 = nodep;
                  }
		PICE (prev);
		prev = nodep;

                if (r)
                  {
		    p = r->next;
		    r->next = NULL;
                  }
	      }
	  }

	/* default value */
	if (i % 2 == 0)
	  {
	    if (prev)
	{
	      prev->info.expr.arg2 = p;
        }
	    PICE (prev);
	  }
	else if (prev && prev->info.expr.arg2 == NULL)
	  {
	    p = parser_new_node (this_parser, PT_VALUE);
	    if (p)
	      {
		    PT_SET_NULL_NODE (p);
	      }
	    prev->info.expr.arg2 = p;
	    PICE (prev);
	  }

	if (case_oper)
	{
	  parser_free_node (this_parser, case_oper);
        }

	return node;
      }

    case PT_LEAST:
    case PT_GREATEST:
      {
	PT_NODE *prev, *expr, *arg, *tmp;
	int i;
	arg = args;

	if (c < 1)
	{
	  return NULL;
        }

	expr = parser_new_node (this_parser, PT_EXPR);
	if (expr)
	  {
	    expr->info.expr.op = key->op;
	    expr->info.expr.arg1 = arg;
	    expr->info.expr.arg2 = NULL;
	    expr->info.expr.arg3 = NULL;
	    expr->info.expr.continued_case = 1;
	  }

	PICE (expr);
	prev = expr;

	if (c > 1)
	  {
	    tmp = arg;
	    arg = arg->next;
	    tmp->next = NULL;

	    if (prev)
	{
	      prev->info.expr.arg2 = arg;
        }
	    PICE (prev);
	  }
	for (i = 3; i <= c; i++)
	  {
	    tmp = arg;
	    arg = arg->next;
	    tmp->next = NULL;

	    expr = parser_new_node (this_parser, PT_EXPR);
	    if (expr)
	      {
		expr->info.expr.op = key->op;
		expr->info.expr.arg1 = prev;
		expr->info.expr.arg2 = arg;
		expr->info.expr.arg3 = NULL;
		expr->info.expr.continued_case = 1;
	      }

	    if (prev && prev->info.expr.continued_case >= 1)
	      prev->info.expr.continued_case++;
	    PICE (expr);
	    prev = expr;
	  }

	if (expr && expr->info.expr.arg2 == NULL)
	  {
	    expr->info.expr.arg2 =
	      parser_copy_tree_list (this_parser, expr->info.expr.arg1);
	    expr->info.expr.arg2->is_hidden_column = 1;
	  }

	return expr;
      }

    case PT_CONCAT:
    case PT_CONCAT_WS:
    case PT_FIELD:
      {
	PT_NODE *prev, *expr, *arg, *tmp, *sep, *val;
	int i, ws;
	arg = args;

	ws = (key->op != PT_CONCAT) ? 1 : 0;
	if (c < 1 + ws)
	{
	  return NULL;
        }

	if (key->op != PT_CONCAT)
	  {
	    sep = arg;
	    arg = arg->next;
	    sep->next = NULL;
	  }
	else
	  {
	    sep = NULL;
	  }

	expr = parser_new_node (this_parser, PT_EXPR);
	if (expr)
	  {
	    expr->info.expr.op = key->op;
	    expr->info.expr.arg1 = arg;
	    expr->info.expr.arg2 = NULL;
	    if (key->op == PT_FIELD && sep)
	      {
		val = parser_new_node (this_parser, PT_VALUE);
		if (val)
		  {
		    val->type_enum = PT_TYPE_INTEGER;
		    val->info.value.data_value.i = 1;
		    val->is_hidden_column = 1;
		  }
		expr->info.expr.arg3 = parser_copy_tree (this_parser, sep);
		expr->info.expr.arg3->next = val;
	      }
	    else
	      {
		expr->info.expr.arg3 = sep;
	      }
	    expr->info.expr.continued_case = 1;
	  }

	PICE (expr);
	prev = expr;

	if (c > 1 + ws)
	  {
	    tmp = arg;
	    arg = arg->next;
	    tmp->next = NULL;

	    if (prev)
	      prev->info.expr.arg2 = arg;
	    PICE (prev);
	  }
	for (i = 3 + ws; i <= c; i++)
	  {
	    tmp = arg;
	    arg = arg->next;
	    tmp->next = NULL;

	    expr = parser_new_node (this_parser, PT_EXPR);
	    if (expr)
	      {
		expr->info.expr.op = key->op;
		expr->info.expr.arg1 = prev;
		expr->info.expr.arg2 = arg;
		if (sep)
		  {
		    expr->info.expr.arg3 =
		      parser_copy_tree (this_parser, sep);
		    if (key->op == PT_FIELD)
		      {
			val = parser_new_node (this_parser, PT_VALUE);
			if (val)
			  {
			    val->type_enum = PT_TYPE_INTEGER;
			    val->info.value.data_value.i = i - ws;
			    val->is_hidden_column = 1;
			  }
			expr->info.expr.arg3->next = val;
		      }
		  }
		else
		  {
		    expr->info.expr.arg3 = NULL;
		  }
		expr->info.expr.continued_case = 1;
	      }

	    if (prev && prev->info.expr.continued_case >= 1)
	      prev->info.expr.continued_case++;
	    PICE (expr);
	    prev = expr;
	  }

	if (key->op == PT_FIELD && expr && expr->info.expr.arg2 == NULL)
	  {
	    val = parser_new_node (this_parser, PT_VALUE);
	    if (val)
	      {
		    PT_SET_NULL_NODE (val);
		    val->is_hidden_column = 1;
	      }
	    expr->info.expr.arg2 = val;
	  }

	return expr;
      }

    case PT_LOCATE:
      if (c < 2 || c > 3)
	{
	return NULL;
        }

      a1 = args;
      a2 = a1->next;
      if (a2)
	{
	  a3 = a2->next;
	  a2->next = NULL;
	}
      a1->next = NULL;

      node = parser_make_expression (this_parser, key->op, a1, a2, a3);
      return node;

    case PT_HA_STATUS:
    case PT_SHARD_GROUPID:
    case PT_SHARD_LOCKNAME:
    case PT_SHARD_NODEID:
      if (c != 0)
	{
        return NULL;
        }

      node = parser_new_node (this_parser, PT_EXPR);

      if (node)
        {
          node->info.expr.op = key->op;
        }
      return node;

    case PT_MID:
      if (c != 3)
	{
	return NULL;
        }

      a1 = args;
      a2 = a1->next;
      a3 = a2->next;
      a1->next = NULL;
      a2->next = NULL;
      a3->next = NULL;

      node = parser_make_expression (this_parser, key->op, a1, a2, a3);
      return node;

    case PT_STRCMP:
      if (c != 2)
	{
	return NULL;
        }

      a1 = args;
      a2 = a1->next;
      a1->next = NULL;

      node = parser_make_expression (this_parser, key->op, a1, a2, a3);
      return node;

    case PT_REVERSE:
      if (c != 1)
	{
	return NULL;
        }

      a1 = args;
      node = parser_make_expression (this_parser, key->op, a1, NULL, NULL);
      return node;

    case PT_BIT_COUNT:
      if (c != 1)
	{
	return NULL;
        }

      a1 = args;
      node = parser_make_expression (this_parser, key->op, a1, NULL, NULL);
      return node;

    case PT_GROUPBY_NUM:
      if (c != 0)
	{
	return NULL;
        }

      node = parser_new_node (this_parser, PT_FUNCTION);

      if (node)
	{
	  node->info.function.function_type = PT_GROUPBY_NUM;
	  node->info.function.arg_list = NULL;
	  node->info.function.all_or_distinct = PT_ALL;
	}

      if (parser_groupbynum_check == 0)
	{
	PT_ERRORmf2 (this_parser, node, MSGCAT_SET_PARSER_SEMANTIC,
		     MSGCAT_SEMANTIC_INSTNUM_COMPATIBILITY_ERR,
		     "GROUPBY_NUM()", "GROUPBY_NUM()");
        }
      return node;

    case PT_LIST_DBS:
      if (c != 0)
	{
	return NULL;
         }

     node = parser_make_expression (this_parser, key->op, NULL, NULL, NULL);
      return node;

    default:
      return NULL;
    }

  return NULL;
}

static void
resolve_alias_in_expr_node (PT_NODE * node, PT_NODE * list)
{
  if (!node)
    {
      return;
    }

  switch (node->node_type)
    {
    case PT_SORT_SPEC:
      if (node->info.sort_spec.expr
	  && node->info.sort_spec.expr->node_type == PT_NAME)
	{
	  resolve_alias_in_name_node (&node->info.sort_spec.expr, list);
	}
      else
	{
	  resolve_alias_in_expr_node (node->info.sort_spec.expr, list);
	}
      break;

    case PT_EXPR:
      if (node->info.expr.arg1 && node->info.expr.arg1->node_type == PT_NAME)
	{
	  resolve_alias_in_name_node (&node->info.expr.arg1, list);
	}
      else
	{
	  resolve_alias_in_expr_node (node->info.expr.arg1, list);
	}
      if (node->info.expr.arg2 && node->info.expr.arg2->node_type == PT_NAME)
	{
	  resolve_alias_in_name_node (&node->info.expr.arg2, list);
	}
      else
	{
	  resolve_alias_in_expr_node (node->info.expr.arg2, list);
	}
      if (node->info.expr.arg3 && node->info.expr.arg3->node_type == PT_NAME)
	{
	  resolve_alias_in_name_node (&node->info.expr.arg3, list);
	}
      else
	{
	  resolve_alias_in_expr_node (node->info.expr.arg3, list);
	}
      break;

    default:;
    }
}


static void
resolve_alias_in_name_node (PT_NODE ** node, PT_NODE * list)
{
  PT_NODE *col;
  char *n_str, *c_str;
  bool resolved = false;

  n_str = parser_print_tree (this_parser, *node);

  for (col = list; col; col = col->next)
    {
      if (col->node_type == PT_NAME)
	{
	  c_str = parser_print_tree (this_parser, col);
	  if (c_str == NULL)
	    {
	      continue;
	    }

	  if (intl_identifier_casecmp (n_str, c_str) == 0)
	    {
	      resolved = true;
	      break;
	    }
	}
    }

  if (resolved != true)
    {
      for (col = list; col; col = col->next)
	{
	  if (col->alias_print
	      && intl_identifier_casecmp (n_str, col->alias_print) == 0)
	    {
	      parser_free_node (this_parser, *node);
	      *node = parser_copy_tree (this_parser, col);
	      (*node)->next = NULL;
	      break;
	    }
	}
    }
}

static char *
pt_check_identifier (PARSER_CONTEXT *parser, PT_NODE *p, const char *str,
		     const int str_size)
{
  (void) intl_identifier_fix ((char *) str, -1);
  return (char *)str;
}

static PT_NODE *
pt_create_string_literal (PARSER_CONTEXT *parser, const PT_TYPE_ENUM char_type,
			  const char *str)
{
  int str_size = strlen (str);
  PT_NODE *node = NULL;
  char *invalid_pos = NULL;
  int composed_size;

  assert (char_type == PT_TYPE_VARCHAR);

    node = parser_new_node (parser, PT_VALUE);

    if (node)
      {
	node->type_enum = char_type;
	node->info.value.string_type = ' ';
	node->info.value.data_value.str = pt_append_bytes (parser, NULL, str, str_size);
	PT_NODE_PRINT_VALUE_TO_TEXT (parser, node);
      }

  return node;
}

static PT_NODE *
pt_create_date_value (PARSER_CONTEXT *parser, const PT_TYPE_ENUM type,
		      const char *str)
{
  PT_NODE *node = NULL;

  node = parser_new_node (parser, PT_VALUE);

  if (node)
    {
      node->type_enum = type;

      node->info.value.data_value.str = pt_append_bytes (parser, NULL, str, strlen (str));
      PT_NODE_PRINT_VALUE_TO_TEXT (parser, node);
    }

  return node;
}

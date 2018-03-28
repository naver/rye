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
 * rsql.h : header file for rsql
 *
 */

#ifndef _RSQL_H_
#define _RSQL_H_

#ident "$Id$"

#include "config.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <locale.h>

#include "porting.h"
#include "language_support.h"
#include "message_catalog.h"
#include "util_func.h"
#include "misc_string.h"
#include "dbi.h"
#include "error_manager.h"
#include "object_print.h"
#include "memory_alloc.h"

#define MSGCAT_RSQL_SET_RSQL	  1

/*
 * MESSAGE NUMBERS
 */
enum
{
  RSQL_MSG_USAGE = 40,
  RSQL_MSG_BAD_MODE = 41,
  RSQL_MSG_BAD_ARGS = 42,
  RSQL_MSG_NO_ENV = 43,
  RSQL_MSG_EXEC_FAILURE = 44,
  RSQL_MSG_BOTH_MODES = 45,

  RSQL_EXECUTE_END_MSG_FORMAT = 46,
  RSQL_START_POSITION_ERR_FORMAT = 47,
  RSQL_EXACT_POSITION_ERR_FORMAT = 48,
  RSQL_INITIAL_HELP_MSG = 49,
  RSQL_ERROR_PREFIX = 50,
  RSQL_INITIAL_RSQL_TITLE = 51,
  RSQL_TRANS_TERMINATE_PROMPT_TEXT = 52,
  RSQL_TRANS_TERMINATE_PROMPT_RETRY_TEXT = 53,
  RSQL_STAT_COMMITTED_TEXT = 54,
  RSQL_STAT_ROLLBACKED_TEXT = 55,
  RSQL_STAT_EDITOR_SAVED_TEXT = 56,
  RSQL_STAT_READ_DONE_TEXT = 57,
  RSQL_STAT_EDITOR_PRINTED_TEXT = 58,
  RSQL_STAT_CD_TEXT = 59,
  RSQL_PASSWD_PROMPT_TEXT = 61,
  RSQL_RESULT_STMT_TITLE_FORMAT = 62,
  RSQL_STAT_NONSCR_EMPTY_RESULT_TEXT = 63,
  RSQL_STAT_CHECKPOINT_TEXT = 64,
  RSQL_STAT_RESTART_TEXT = 65,
  RSQL_KILLTRAN_TITLE_TEXT = 66,
  RSQL_KILLTRAN_FORMAT = 67,
  RSQL_STAT_KILLTRAN_TEXT = 68,
  RSQL_STAT_KILLTRAN_FAIL_TEXT = 69,
  RSQL_ROWS = 70,
  RSQL_ROW = 71,
  RSQL_ARG_AUTO = 75,
  RSQL_ARG_AUTO_HELP = 76,
  RSQL_PROMPT = 79,
  RSQL_NAME = 80,
  RSQL_ADMIN_PROMPT = 81,

  RSQL_HELP_SCHEMA_TITLE_TEXT = 145,
  RSQL_HELP_NONE_TEXT = 146,
  RSQL_HELP_SQL_TITLE_TEXT = 151,
  RSQL_HELP_SESSION_CMD_TITLE_TEXT = 152,
  RSQL_E_FILENAMEMISSED_TEXT = 178,
  RSQL_E_CANTEXECPAGER_TEXT = 179,
  RSQL_E_NOMOREMEMORY_TEXT = 180,
  RSQL_E_TOOLONGLINE_TEXT = 184,
  RSQL_E_TOOMANYLINES_TEXT = 185,
  RSQL_E_TOOMANYFILENAMES_TEXT = 188,
  RSQL_E_SESSCMDNOTFOUND_TEXT = 190,
  RSQL_E_SESSCMDAMBIGUOUS_TEXT = 191,
  RSQL_E_RSQLCMDAMBIGUOUS_TEXT = 193,
  RSQL_E_INVALIDARGCOM_TEXT = 194,
  RSQL_E_UNKNOWN_TEXT = 196,
  RSQL_E_CANT_EDIT_TEXT = 197,

  RSQL_HELP_CLASS_HEAD_TEXT = 203,
  RSQL_HELP_ATTRIBUTE_HEAD_TEXT = 206,
  RSQL_HELP_SHARD_SPEC_HEAD_TEXT = 210,
  RSQL_HELP_QUERY_SPEC_HEAD_TEXT = 212,
  RSQL_HELP_SQL_NAME_HEAD_TEXT = 222,
  RSQL_HELP_SQL_DESCRIPTION_HEAD_TEXT = 223,
  RSQL_HELP_SQL_SYNTAX_HEAD_TEXT = 224,
  RSQL_HELP_SQL_EXAMPLE_HEAD_TEXT = 225,
  RSQL_HELP_SESSION_CMD_TEXT = 231,
  RSQL_HELP_CONSTRAINT_HEAD_TEXT = 232,
  RSQL_HELP_INFOCMD_TEXT = 233,
  RSQL_E_CLASSNAMEMISSED_TEXT = 236
};

#define SCRATCH_TEXT_LEN (ONE_K * 4)    /* 4096 */

/* error codes defined in rsql level */
enum
{
  RSQL_ERR_OS_ERROR = 1,
  RSQL_ERR_NO_MORE_MEMORY,
  RSQL_ERR_TOO_LONG_LINE,
  RSQL_ERR_TOO_MANY_LINES,
  RSQL_ERR_TOO_MANY_FILE_NAMES,
  RSQL_ERR_SQL_ERROR,
  RSQL_ERR_SESS_CMD_NOT_FOUND,
  RSQL_ERR_SESS_CMD_AMBIGUOUS,
  RSQL_ERR_FILE_NAME_MISSED,
  RSQL_ERR_RYE_STMT_NOT_FOUND,
  RSQL_ERR_RYE_STMT_AMBIGUOUS,
  RSQL_ERR_CANT_EXEC_PAGER,
  RSQL_ERR_INVALID_ARG_COMBINATION,
  RSQL_ERR_CANT_EDIT,
  RSQL_ERR_INFO_CMD_HELP,
  RSQL_ERR_CLASS_NAME_MISSED,
  RSQL_ERR_LOC_INIT
};

/* session command numbers */
typedef enum
{
  S_CMD_UNKNOWN,
  S_CMD_QUERY,
/* File stuffs */
  S_CMD_READ,
  S_CMD_WRITE,
  S_CMD_APPEND,
  S_CMD_PRINT,
  S_CMD_SHELL,
  S_CMD_CD,
  S_CMD_EXIT,

/* Edit stuffs */
  S_CMD_CLEAR,
  S_CMD_EDIT,
  S_CMD_LIST,

/* Command stuffs */
  S_CMD_RUN,
  S_CMD_XRUN,
  S_CMD_COMMIT,
  S_CMD_ROLLBACK,
  S_CMD_AUTOCOMMIT,
  S_CMD_CHECKPOINT,
  S_CMD_KILLTRAN,
  S_CMD_RESTART,

/* Environment stuffs */
  S_CMD_SHELL_CMD,
  S_CMD_EDIT_CMD,
  S_CMD_PRINT_CMD,
  S_CMD_PAGER_CMD,
  S_CMD_NOPAGER_CMD,
  S_CMD_COLUMN_WIDTH,
  S_CMD_STRING_WIDTH,
  S_CMD_GROUPID,

/* Help stuffs */
  S_CMD_HELP,
  S_CMD_SCHEMA,
  S_CMD_DATABASE,
  S_CMD_INFO,

/* More environment stuff */
  S_CMD_SET_PARAM,
  S_CMD_GET_PARAM,
  S_CMD_PLAN_DUMP,
  S_CMD_ECHO,
  S_CMD_DATE,
  S_CMD_TIME,
  S_CMD_LINE_OUTPUT,

/* Histogram profile stuff */
  S_CMD_HISTO,
  S_CMD_CLR_HISTO,
  S_CMD_DUMP_HISTO,
  S_CMD_DUMP_CLR_HISTO,

/* cmd history stuffs */
  S_CMD_HISTORY_READ,
  S_CMD_HISTORY_LIST,

  S_CMD_TRACE
} SESSION_CMD;

typedef enum rsql_statement_state
{
  RSQL_STATE_GENERAL = 0,
  RSQL_STATE_C_COMMENT,
  RSQL_STATE_CPP_COMMENT,
  RSQL_STATE_SQL_COMMENT,
  RSQL_STATE_SINGLE_QUOTE,
  RSQL_STATE_MYSQL_QUOTE,
  RSQL_STATE_DOUBLE_QUOTE_IDENTIFIER,
  RSQL_STATE_BACKTICK_IDENTIFIER,
  RSQL_STATE_BRACKET_IDENTIFIER,
  RSQL_STATE_STATEMENT_END
} RSQL_STATEMENT_STATE;


/* iq_ function return status */
enum
{
  RSQL_FAILURE = -1,
  RSQL_SUCCESS = 0
};

typedef struct
{
  const char *db_name;
  const char *user_name;
  const char *passwd;
  const char *in_file_name;
  const char *out_file_name;
  const char *command;
  bool sa_mode;
  bool cs_mode;
  bool single_line_execution;
  bool column_output;
  bool line_output;
  bool auto_commit;
  bool nopager;
  bool continue_on_error;
  bool write_on_standby;
  int string_width;
  int groupid;
  bool time_on;
} RSQL_ARGUMENT;

typedef struct
{
  char *name;
  int width;
} RSQL_COLUMN_WIDTH_INFO;

typedef struct rsql_query RSQL_QUERY;
struct rsql_query
{
  char *query;
  int length;
  int alloc_size;
};

/* The file streams we are to use */
extern FILE *rsql_Input_fp;
extern FILE *rsql_Output_fp;
extern FILE *rsql_Error_fp;

extern char rsql_Editor_cmd[];
extern char rsql_Shell_cmd[];
extern char rsql_Print_cmd[];
extern char rsql_Pager_cmd[];
extern char rsql_Scratch_text[];
extern int rsql_Error_code;


extern int rsql_Line_lwm;
extern int rsql_Row_count;
extern int rsql_Num_failures;

extern int (*rsql_text_utf8_to_console) (const char *, const int, char **, int *);
extern int (*rsql_text_console_to_utf8) (const char *, const int, char **, int *);

extern void rsql_display_msg (const char *string);
extern void rsql_exit (int exit_status);
extern int rsql (const char *argv0, RSQL_ARGUMENT * rsql_arg);
extern const char *rsql_get_message (int message_index);

extern char *rsql_get_real_path (const char *pathname);
extern void rsql_invoke_system (const char *command);
extern int rsql_invoke_system_editor (void);
extern void rsql_fputs (const char *str, FILE * fp);
extern void rsql_fputs_console_conv (const char *str, FILE * fp);
extern FILE *rsql_popen (const char *cmd, FILE * fd);
extern void rsql_pclose (FILE * pf, FILE * fd);
extern void rsql_display_rsql_err (int line_no, int col_no);
extern void rsql_display_session_err (DB_SESSION * session, int line_no);
extern int rsql_append_more_line (int indent, const char *line);
extern void rsql_display_more_lines (const char *title);
extern void rsql_free_more_lines (void);
extern void rsql_check_server_down (void);
extern char *rsql_get_tmp_buf (size_t size);
extern void nonscr_display_error (void);

extern SESSION_CMD rsql_get_session_cmd_no (const char *input);

extern void rsql_results (const RSQL_ARGUMENT * rsql_arg,
                          DB_QUERY_RESULT * result, DB_QUERY_TYPE * attr_spec, int line_no, RYE_STMT_TYPE stmt_type);

extern const char *rsql_edit_contents_get (void);
extern int rsql_edit_contents_append (const char *str, bool flag_append_new_line);
extern RSQL_STATEMENT_STATE rsql_walk_statement (const char *str, RSQL_STATEMENT_STATE state);
extern bool rsql_is_statement_complete (RSQL_STATEMENT_STATE state);
extern bool rsql_is_statement_in_block (RSQL_STATEMENT_STATE state);
extern void rsql_edit_contents_clear (void);
extern void rsql_edit_contents_finalize (void);
extern int rsql_edit_read_file (FILE * fp);
extern int rsql_edit_write_file (FILE * fp);

extern const char *rsql_errmsg (int code);

extern void rsql_help_menu (void);
extern void rsql_help_schema (const char *class_name);
extern void rsql_help_info (const char *command, int aucommit_flag);
extern void rsql_killtran (const char *argument);

extern char *rsql_db_value_as_string (DB_VALUE * value, int *length);

extern int rsql_set_column_width_info (const char *column_name, int column_width);
extern int rsql_get_column_width (const char *column_name);

extern int rsql_query_init (RSQL_QUERY * query, int size);
extern int rsql_query_final (RSQL_QUERY * query);
extern int rsql_query_clear (RSQL_QUERY * query);
extern int rsql_query_append_string (RSQL_QUERY * query, char *str, int str_length, bool flag_append_new_line);

#endif /* _RSQL_H_ */

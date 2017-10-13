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
 * db_vdb.c - Stubs for SQL interface functions.
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <sys/timeb.h>
#include <time.h>
#include "db.h"
#include "dbi.h"
#include "db_query.h"
#include "error_manager.h"
#include "chartype.h"
#include "system_parameter.h"
#include "environment_variable.h"
#include "memory_alloc.h"
#include "parser.h"
#include "parser_message.h"
#include "object_domain.h"
#include "schema_manager.h"
#include "view_transform.h"
#include "execute_statement.h"
#include "xasl_generation.h"	/* TODO: remove */
#include "locator_cl.h"
#include "server_interface.h"
#include "query_manager.h"
#include "network_interface_cl.h"
#include "transaction_cl.h"

#define BUF_SIZE 1024

#define MAX_SERVER_TIME_CACHE	60	/* secs */

enum
{
  StatementInitialStage = 0,
  StatementCompiledStage,
  StatementPreparedStage,
  StatementExecutedStage
};

static struct timeb base_server_timeb = { 0, 0, 0, 0 };
static struct timeb base_client_timeb = { 0, 0, 0, 0 };


static DB_SESSION *db_open_local (void);
static int db_execute_and_keep_statement_local (DB_SESSION * session,
						DB_QUERY_RESULT ** result);

/*
 * db_statement_count() - This function returns the number of statements
 *    in a session.
 * return : number of statements in the session
 * session(in): compiled session
 */
int
db_statement_count (DB_SESSION * session)
{
  if (session == NULL)
    {
      return 0;
    }

  return 1;
}

/*
 * db_open_local() - Starts a new SQL empty compile session
 * returns : new DB_SESSION
 */
static DB_SESSION *
db_open_local (void)
{
  DB_SESSION *session = NULL;

  session = (DB_SESSION *) malloc (sizeof (DB_SESSION));
  if (session == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (DB_SESSION));
      return NULL;
    }

  session->parser = parser_create_parser ();
  if (session->parser == NULL)
    {
      free_and_init (session);
      return NULL;
    }

  assert (session->parser->query_id == NULL_QUERY_ID);

  session->stage = StatementInitialStage;
  session->type_list = NULL;
  session->line_offset = 0;
  session->statement = NULL;

  /* initialization */
  session->groupid = NULL_GROUPID;
  session->shardkeys = NULL;
  session->num_shardkeys = 0;
  session->shardkey_coll_id = LANG_COERCIBLE_COLL;
  session->shardkey_required = false;
  session->shardkey_exhausted = false;
  session->from_migrator = false;

  return session;
}

/*
 * db_open_buffer_local() - Please refer to the db_open_buffer() function
 * returns  : new DB_SESSION
 * buffer(in): contains query text to be compiled
 */
DB_SESSION *
db_open_buffer_local (const char *buffer)
{
  DB_SESSION *session;

  CHECK_1ARG_NULL (buffer);

  session = db_open_local ();

  if (session)
    {
      session->statement = parser_parse_string (session->parser, buffer);

      assert (session->statement == NULL || session->statement->next == NULL);
    }

  return session;
}

/*
 * db_open_buffer() - Starts a new SQL compile session on a nul terminated
 *    string
 * return:new DB_SESSION
 * buffer(in) : contains query text to be compiled
 */
DB_SESSION *
db_open_buffer (const char *buffer)
{
  DB_SESSION *session;

  CHECK_1ARG_NULL (buffer);
#if 0				/* TODO - #955 set PERSIST */
  CHECK_CONNECT_NULL ();
#endif

  session = db_open_buffer_local (buffer);

  return session;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_make_session_for_one_statement_execution() -
 * return:
 * file(in) :
 */
DB_SESSION *
db_make_session_for_one_statement_execution (FILE * file)
{
  DB_SESSION *session;

  CHECK_CONNECT_NULL ();

  session = db_open_local ();
  if (session)
    {
      pt_init_one_statement_parser (session->parser, file);
      parse_one_statement (0);
    }

  return session;
}

/*
 * db_get_parser_line_col() -
 * return:
 * session(in) :
 * line(out) :
 * col(out) :
 */
int
db_get_parser_line_col (DB_SESSION * session, int *line, int *col)
{
  if (line)
    {
      *line = session->parser->line;
    }
  if (col)
    {
      *col = session->parser->column;
    }
  return 0;
}
#endif

/*
 * db_calculate_current_server_time () -
 * return:
 * parser(in) :
 * server_info(in) :
 */
static void
db_calculate_current_server_time (PARSER_CONTEXT * parser,
				  SERVER_INFO * server_info)
{
  if (base_server_timeb.time != 0)
    {
      struct tm *c_time_struct;
      DB_DATETIME datetime;

      struct timeb curr_server_timeb;
      struct timeb curr_client_timeb;
      int diff_mtime;
      int diff_time;

      ftime (&curr_client_timeb);
      diff_time = curr_client_timeb.time - base_client_timeb.time;
      diff_mtime = curr_client_timeb.millitm - base_client_timeb.millitm;

      if (diff_time > MAX_SERVER_TIME_CACHE)
	{
	  base_server_timeb.time = 0;
	}
      else
	{
	  curr_server_timeb.time = base_server_timeb.time;
	  curr_server_timeb.millitm = base_server_timeb.millitm;

	  /* timeb.millitm is unsigned short, so should prevent underflow */
	  if (diff_mtime < 0)
	    {
	      curr_server_timeb.time--;
	      curr_server_timeb.millitm += 1000;
	    }

	  curr_server_timeb.time += diff_time;
	  curr_server_timeb.millitm += diff_mtime;

	  if (curr_server_timeb.millitm >= 1000)
	    {
	      curr_server_timeb.time++;
	      curr_server_timeb.millitm -= 1000;
	    }

	  c_time_struct = localtime (&curr_server_timeb.time);
	  if (c_time_struct == NULL)
	    {
	      base_server_timeb.time = 0;
	    }
	  else
	    {
	      db_datetime_encode (&datetime, c_time_struct->tm_mon + 1,
				  c_time_struct->tm_mday,
				  c_time_struct->tm_year + 1900,
				  c_time_struct->tm_hour,
				  c_time_struct->tm_min,
				  c_time_struct->tm_sec,
				  curr_server_timeb.millitm);

	      server_info->value[0] = &parser->sys_datetime;
	      DB_MAKE_DATETIME (server_info->value[0], &datetime);
	    }
	}
    }

  if (base_server_timeb.time == 0)
    {
      server_info->info_bits |= SI_SYS_DATETIME;
      server_info->value[0] = &parser->sys_datetime;
    }
}

/*
 * db_set_base_server_time() -
 * return:
 * server_info(in) :
 */
static void
db_set_base_server_time (SERVER_INFO * server_info)
{
  if (server_info->info_bits & SI_SYS_DATETIME)
    {
      struct tm c_time_struct;
      DB_DATETIME *dt = &server_info->value[0]->data.datetime;
      DB_TIME time_val;

      time_val = dt->time / 1000;	/* milliseconds to seconds */
      db_tm_encode (&c_time_struct, &dt->date, &time_val);

      base_server_timeb.millitm = dt->time % 1000;	/* set milliseconds */

      base_server_timeb.time = mktime (&c_time_struct);
      ftime (&base_client_timeb);
    }
}

/*
 * db_compile_statement_local() -
 * return:
 * session(in) :
 */
int
db_compile_statement_local (DB_SESSION * session)
{
  PARSER_CONTEXT *parser;
  PT_NODE *statement = NULL;
  DB_QUERY_TYPE *qtype;
  int cmd_type;
  int err;
  SERVER_INFO server_info;
  static long seed = 0;

  /* obvious error checking - invalid parameter */
  if (!session || !session->parser)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return er_errid ();
    }
  /* no statement was given in the session */
  if (!session->statement)
    {
      /* if the parser already has something wrong - syntax error */
      if (pt_has_error (session->parser))
	{
	  pt_report_to_ersys (session->parser, PT_SYNTAX);
	  return er_errid ();
	}

      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_EMPTY_STATEMENT, 0);
      return er_errid ();
    }

  assert (session->statement->next == NULL);

  /*
   * Compilation Stage
   */

  /* the statements in this session have been parsed without error
     and it is time to compile the next statement */
  parser = session->parser;
  statement = session->statement;
  statement->use_plan_cache = 0;

  /* check if the statement is already processed */
  if (session->stage >= StatementPreparedStage)
    {
      return NO_ERROR;
    }

  /* forget about any previous parsing errors, if any */
  pt_reset_error (parser);

  /* get type list describing the output columns titles of the given query */
  cmd_type = pt_node_to_cmd_type (statement);
  qtype = NULL;
  if (cmd_type == RYE_STMT_SELECT)
    {
      qtype = pt_get_titles (parser, statement);
      /* to prevent a memory leak, register the query type list to session */
      session->type_list = qtype;
    }

  /* prefetch and lock classes to avoid deadlock */
  (void) pt_class_pre_fetch (parser, statement);
  if (pt_has_error (parser))
    {
      pt_report_to_ersys_with_statement (parser, PT_SYNTAX, statement);
      return er_errid ();
    }

  /* get sys_date, sys_time, sys_datetime values from the server */
  server_info.info_bits = 0;	/* init */

  if (seed == 0)
    {
      srand48 (seed = (long) time (NULL));
    }

  /* do semantic check for the statement */
  session->statement = pt_compile (parser, statement);

  if (session->statement == NULL || pt_has_error (parser))
    {
      pt_report_to_ersys_with_statement (parser, PT_SEMANTIC, statement);
      return er_errid ();
    }
  statement = session->statement;

  /* get type list describing the output columns titles of the given query */
  if (cmd_type == RYE_STMT_SELECT)
    {
      /* for a select-type query of the form:
         SELECT * FROM class c
         we store into type_list the nice column headers:
         c.attr1    attr2    c.attr3
         before they get fully resolved by mq_translate(). */
      if (!qtype)
	{
	  qtype = pt_get_titles (parser, statement);
	  /* to prevent a memory leak,
	     register the query type list to session */
	  session->type_list = qtype;
	}
      if (qtype)
	{
	  /* NOTE, this is here on purpose. If something is busting
	     because it tries to continue corresponding this type
	     information and the list file columns after having jacked
	     with the list file, by for example adding a hidden OID
	     column, fix the something else.
	     This needs to give the results as user views the
	     query, ie related to the original text. It may guess
	     wrong about attribute/column updatability.
	     Thats what they asked for. */
	  qtype = pt_fillin_type_size (parser, statement, qtype);
	}
    }

  /* translate views or virtual classes into base classes */
  session->statement = mq_translate (parser, statement);
  if (session->statement == NULL || pt_has_error (parser))
    {
      pt_report_to_ersys_with_statement (parser, PT_SEMANTIC, statement);
      return er_errid ();
    }
  statement = session->statement;

  /* prefetch and lock translated real classes to avoid deadlock */
  (void) pt_class_pre_fetch (parser, statement);
  if (pt_has_error (parser))
    {
      pt_report_to_ersys_with_statement (parser, PT_SYNTAX, statement);
      return er_errid ();
    }

  /* so now, the statement is compiled */
  session->statement = statement;
  assert (session->statement->next == NULL);
  session->stage = StatementCompiledStage;

  /*
   * Preparation Stage
   */

  statement->xasl_id = NULL;	/* bullet proofing */

  /* now, prepare the statement by calling do_prepare_statement() */
  err = do_prepare_statement (session, statement);
  if (err < 0)
    {
      if (pt_has_error (parser))
	{
	  pt_report_to_ersys_with_statement (parser, PT_SEMANTIC, statement);
	  return er_errid ();
	}
      return err;
    }

  /* so now, the statement is prepared */
  session->stage = StatementPreparedStage;

  return NO_ERROR;
}

/*
 * db_compile_statement() - This function compiles the next statement in the
 *    session. The first compilation reports any syntax errors that occurred
 *    in the entire session.
 * return: an integer that is the relative statement ID of the next statement
 *    within the session, with the first statement being statement number 1.
 *    If there are no more statements in the session (end of statements), 0 is
 *    returned. If an error occurs, the return code is negative.
 * session(in) : session handle
 */
int
db_compile_statement (DB_SESSION * session)
{
  er_clear ();

#if 0				/* TODO - #955 set PERSIST */
  CHECK_CONNECT_MINUSONE ();
#endif

  return db_compile_statement_local (session);
}

/*
 * db_get_cacheinfo() -
 * return:
 * session(in) :
 * stmt_ndx(in) :
 * life_time(out) :
 */
bool
db_get_cacheinfo (DB_SESSION * session, bool * use_plan_cache)
{
  /* obvious error checking - invalid parameter */
  if (!session || !session->parser || !session->statement)
    {
      return false;
    }

  assert (session->statement->next == NULL);

  if (use_plan_cache)
    {
      if (session->statement->use_plan_cache)
	{
	  *use_plan_cache = true;
	}
      else
	{
	  *use_plan_cache = false;
	}
    }

  return true;
}

/*
 * db_get_errors() - This function returns a list of errors that occurred during
 *    compilation. NULL is returned if no errors occurred.
 * returns : compilation error list
 * session(in): session handle
 *
 * note : A call to the db_get_next_error() function can be used to examine
 *    each error. You do not free this list of errors.
 */
DB_SESSION_ERROR *
db_get_errors (DB_SESSION * session)
{
  DB_SESSION_ERROR *result;

  if (!session || !session->parser)
    {
      result = NULL;
    }
  else
    {
      result = pt_get_errors (session->parser);
    }

  return result;
}

/*
 * db_get_next_error() - This function returns the line and column number of
 *    the next error that was passed in the compilation error list.
 * return : next error in compilation error list
 * errors (in) : DB_SESSION_ERROR iterator
 * line(out): source line number of error
 * col(out): source column number of error
 *
 * note : Do not free this list of errors.
 */
DB_SESSION_ERROR *
db_get_next_error (DB_SESSION_ERROR * errors, int *line, int *col)
{
  DB_SESSION_ERROR *result;
  const char *e_msg = NULL;

  if (!errors)
    {
      return NULL;
    }

  result = pt_get_next_error (errors, line, col, &e_msg);
  if (e_msg)
    {
      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_PT_ERROR, 1, e_msg);
    }

  return result;
}

#if 0				/* unused */
/*
 * db_get_warnings: This function returns a list of warnings that occurred
 *    during the compilation. NULL is returned if no warnings are found.
 *    A non-NULL return value indicates that one or more warnings occurred
 *    during compilation.
 * returns: DB_SESSION_WARNING iterator if there were any compilation warnings,
 *          NULL, otherwise.
 * session(in): session handle
 *
 * note : Do not free this list of warnings.
 */
DB_SESSION_WARNING *
db_get_warnings (DB_SESSION * session)
{
  DB_SESSION_WARNING *result;

  if (!session || !session->parser)
    {
      result = NULL;
    }
  else
    {
      result = pt_get_warnings (session->parser);
    }

  return result;
}

/*
 * db_get_next_warning: This function returns the line and column number of the
 *    next warning that was passed in the compilation warning list.
 * returns: DB_SESSION_WARNING iterator if there are more compilation warnings
 *          NULL, otherwise.
 * warnings(in) : DB_SESSION_WARNING iterator
 * line(out): source line number of warning
 * col(out): source column number of warning
 *
 * note : Do not free this list of warnings.
 */
DB_SESSION_WARNING *
db_get_next_warning (DB_SESSION_WARNING * warnings, int *line, int *col)
{
  DB_SESSION_WARNING *result;
  int stmt_no;
  const char *e_msg = NULL;

  if (!warnings)
    {
      return NULL;
    }

  result = pt_get_next_error (warnings, &stmt_no, line, col, &e_msg);
  if (e_msg)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_PT_ERROR, 1, e_msg);
    }

  return result;
}
#endif

/*
 * db_session_set_holdable () - mark session as holdable
 * return : void
 * session (in) :
 * holdable (in) :
 */
void
db_session_set_holdable (DB_SESSION * session, bool holdable)
{
  if (session == NULL || session->parser == NULL)
    {
      return;
    }
  session->parser->is_holdable = holdable ? 1 : 0;
}

void
db_session_set_autocommit_mode (DB_SESSION * session, bool autocommit_mode)
{
  if (session == NULL || session->parser == NULL)
    {
      return;
    }
  session->parser->is_autocommit = autocommit_mode ? 1 : 0;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_get_line_col_of_1st_error() - get the source line & column of first error
 * returns: 1 if there were any query compilation errors, 0, otherwise.
 * session(in) : contains the SQL query that has just been compiled
 * linecol(out): the source line & column of first error if any
 *
 * note : DO NOT USE THIS FUNCTION.  USE db_get_errors & db_get_next_error
 *	  instead.  This function is provided for the sole purpose of
 *	  facilitating conversion of old code.
 */
int
db_get_line_col_of_1st_error (DB_SESSION * session, DB_QUERY_ERROR * linecol)
{
  if (!session || !session->parser || !pt_has_error (session->parser))
    {
      if (linecol)
	{
	  linecol->err_lineno = linecol->err_posno = 0;
	}
      return 0;
    }
  else
    {
      PT_NODE *errors;
      const char *msg;

      errors = pt_get_errors (session->parser);
      if (linecol)
	{
	  pt_get_next_error (errors, &linecol->err_lineno,
			     &linecol->err_posno, &msg);
	}
      return 1;
    }
}

/*
 * db_get_input_markers() -
 * return : host variable input markers list in statement
 * session(in): compilation session
 * stmt(in): statement number of compiled statement
 */
DB_MARKER *
db_get_input_markers (DB_SESSION * session)
{
  PARSER_CONTEXT *parser;
  DB_MARKER *result = NULL;
  PT_HOST_VARS *hv;

  if (!session || !(parser = session->parser)
      || !session->statement || pt_has_error (parser))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      result = NULL;
    }
  else
    {
      assert (session->statement->next == NULL);

      hv = pt_host_info (parser, session->statement);
      result = pt_get_input_host_vars (hv);
      pt_free_host_info (hv);
    }

  return result;
}

/*
 * db_marker_next: This function returns the next marker in the list
 * return : the next host variable (input/output) marker in the list or NULL
 * marker(in): DB_MARKER
 */
DB_MARKER *
db_marker_next (DB_MARKER * marker)
{
  DB_MARKER *result = NULL;

  if (marker)
    {
      result = pt_node_next (marker);
    }

  return result;
}

/*
 * db_marker_domain() - This function returns the domain of an host variable
 *    (input/output) marker
 * return : domain of marker
 * marker(in): DB_MARKER
 */
DB_DOMAIN *
db_marker_domain (DB_MARKER * marker)
{
  DB_DOMAIN *result = NULL;

  if (marker)
    {
      result = pt_node_to_db_domain (NULL, marker, NULL);
    }
  /* it is safet to call pt_node_to_db_domain() without parser */

  return result;
}
#endif

/*
 * db_get_query_type_list() - This function returns a type list that describes
 *    the columns of a SELECT statement. This includes the column title, data
 *    type, and size. The statement ID must have been returned by a previously
 *    successful call to the db_compile_statement() function. The query type
 *    list is freed by using the db_query_format_free() function.
 * return : query type.
 * session(in): session handle
 * stmt(in): statement id
 */
DB_QUERY_TYPE *
db_get_query_type_list (DB_SESSION * session)
{
  PT_NODE *statement;
  DB_QUERY_TYPE *qtype;
  int cmd_type;

  /* obvious error checking - invalid parameter */
  if (!session || !session->parser)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return NULL;
    }
  /* no statement was given in the session */
  if (!session->statement)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_EMPTY_STATEMENT, 0);
      return NULL;
    }

  assert (session->statement->next == NULL);

  /* invalid parameter */
  statement = session->statement;
  /* check if the statement is compiled and prepared */
  if (session->stage < StatementPreparedStage)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return NULL;
    }

  /* make DB_QUERY_TYPE structure to return */

  cmd_type = pt_node_to_cmd_type (statement);
  if (cmd_type == RYE_STMT_SELECT)
    {
      PT_NODE *select_list = pt_get_select_list (session->parser, statement);
      if (pt_length_of_select_list (select_list, EXCLUDE_HIDDEN_COLUMNS) > 0)
	{
	  /* duplicate one from stored list */
	  qtype = db_cp_query_type (session->type_list, true);
	}
      else
	{
	  qtype = NULL;
	}
    }
  else
    {
      /* make new one containing single value */
      qtype = db_alloc_query_format (1);
      if (qtype)
	{
	  switch (cmd_type)
	    {
	    case RYE_STMT_INSERT:
	      /* the type of result of INSERT is object */
	      qtype->db_type = DB_TYPE_OBJECT;
	      break;
	    case RYE_STMT_GET_ISO_LVL:
	    case RYE_STMT_GET_TIMEOUT:
	    case RYE_STMT_GET_OPT_LVL:
	      /* the type of result of some command is integer */
	      qtype->db_type = DB_TYPE_INTEGER;
	      break;
	    default:
	      break;
	    }
	}
    }

  return qtype;
}

/*
 * db_get_query_type_ptr() - This function returns query_type of query result
 * return : result->query_type
 * result(in): query result
 */
DB_QUERY_TYPE *
db_get_query_type_ptr (DB_QUERY_RESULT * result)
{
  return (result->query_type);
}

/*
 * db_get_statement_type() - This function returns query statement node type
 * return : stmt's node type
 * session(in): contains the SQL query that has been compiled
 * stmt(in): statement id returned by a successful compilation
 */
int
db_get_statement_type (DB_SESSION * session)
{
  int retval;

  if (!session || !session->parser || !session->statement)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      retval = er_errid ();
    }
  else
    {
      assert (session->statement->next == NULL);

      retval = pt_node_to_cmd_type (session->statement);
    }

  return retval;
}

/*
 * db_push_values() - This function set session->parser->host_variables
 *   & host_var_count
 * return : integer, negative implies error.
 * session(in): contains the SQL query that has been compiled
 * count(in): number of elements in in_values table
 * in_values(in): a table of host_variable initialized DB_VALUEs
 */
int
db_push_values (DB_SESSION * session, int count, DB_VALUE * in_values)
{
  PARSER_CONTEXT *parser;

  if (session)
    {
      parser = session->parser;
      if (parser)
	{
	  pt_set_host_variables (parser, count, in_values);

	  if (parser->host_var_count > 0 && parser->set_host_var == 0)
	    {
	      if (pt_has_error (session->parser))
		{
		  /* This error can occur when using the statement pooling */
		  pt_report_to_ersys (session->parser, PT_SEMANTIC);
		  /* forget about any previous compilation errors, if any */
		  pt_reset_error (session->parser);

		  return ER_PT_SEMANTIC;
		}
	    }
	}
    }

  return NO_ERROR;
}

int
db_get_host_var_count (DB_SESSION * session)
{
  PARSER_CONTEXT *parser;

  if (session)
    {
      parser = session->parser;
      if (parser)
	{
	  return parser->host_var_count;
	}
    }

  return 0;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_get_hostvars() -
 * return:
 * session(in) :
 */
DB_VALUE *
db_get_hostvars (DB_SESSION * session)
{
  return session->parser->host_variables;
}

/*
 * db_get_lock_classes() -
 * return:
 * session(in) :
 */
char **
db_get_lock_classes (DB_SESSION * session)
{
  if (session == NULL || session->parser == NULL)
    {
      return NULL;
    }

  return (char **) (session->parser->lcks_classes);
}
#endif

bool
db_is_shard_table_query (DB_SESSION * session)
{
  char **classes;
  int num_classes;
  int i;
  DB_OBJECT *class_obj;

  if (session == NULL || session->parser == NULL)
    {
      return false;
    }

  num_classes = session->parser->num_lcks_classes;
  classes = (char **) (session->parser->lcks_classes);

  if (num_classes <= 0 || classes == NULL)
    {
      return false;
    }

  for (i = 0; i < num_classes; i++)
    {
      class_obj = sm_find_class (classes[i]);
      if (sm_is_shard_table (class_obj))
	{
	  return true;
	}
    }

  return false;
}

int
db_get_shard_key_values (DB_SESSION * session,
			 int *num_shard_values, int *num_shard_pos,
			 char **value_out_buf, int value_out_buf_size,
			 int *pos_out_buf, int pos_out_buf_size)
{
  int i;

  *num_shard_values = 0;
  *num_shard_pos = 0;

  if (session == NULL || session->statement == NULL)
    {
      return 0;
    }

  assert (session->statement->next == NULL);

  for (i = 0; i < session->num_shardkeys; i++)
    {
      SHARDKEY_INFO *shard_key = &(session->shardkeys[i]);
      DB_VALUE *tmp_val;

      if (shard_key->value->node_type == PT_VALUE)
	{
	  tmp_val = &shard_key->value->info.value.db_value;
	  if (*num_shard_values < value_out_buf_size &&
	      db_value_type (tmp_val) == DB_TYPE_VARCHAR)
	    {
	      value_out_buf[*num_shard_values] = db_get_string (tmp_val);
	      *num_shard_values = *num_shard_values + 1;
	    }
	}
      else if (shard_key->value->node_type == PT_HOST_VAR)
	{
	  if (*num_shard_values < pos_out_buf_size)
	    {
	      pos_out_buf[*num_shard_pos] =
		shard_key->value->info.host_var.index;
	      *num_shard_pos = *num_shard_pos + 1;
	    }
	}
    }

  return session->num_shardkeys;
}

bool
db_is_select_for_update (DB_SESSION * session)
{
  if (session == NULL || session->statement == NULL)
    {
      return false;
    }

  assert (session->statement->next == NULL);

  if (session->statement->node_type == PT_SELECT)
    {
      if (PT_SELECT_INFO_IS_FLAGED
	  (session->statement, PT_SELECT_INFO_FOR_UPDATE))
	{
	  return true;
	}
    }

  return false;
}

static void
db_set_result_column_type (DB_QUERY_RESULT * qres, DB_QUERY_TYPE * query_type)
{
  QFILE_TUPLE_VALUE_TYPE_LIST *type_list;
  DB_QUERY_TYPE *p;
  int i;

  type_list = &qres->res.s.cursor_id.list_id.type_list;

  for (i = 0, p = query_type;
       i < type_list->type_cnt && p != NULL; i++, p = p->next)
    {
      assert (type_list->domp[i] != NULL);
      if (type_list->domp[i] != NULL)
	{
	  p->db_type = TP_DOMAIN_TYPE (type_list->domp[i]);
	  p->domain = type_list->domp[i];
	  /* at here, don't have to setting p->src_domain */
	}
    }
}

/*
 * db_execute_and_keep_statement_local() - This function executes the SQL
 *    statement identified by the stmt argument and returns the result.
 *    The statement ID must have already been returned by a successful call
 *    to the db_open_buffer() function that
 *    came from a call to the db_compile_statement()function. The compiled
 *    statement is preserved, and may be executed again within the same
 *    transaction.
 * return : error status, if execution failed
 *          number of affected objects, if a success & stmt is a SELECT,
 *          UPDATE, DELETE, or INSERT
 * session(in) : contains the SQL query that has been compiled
 * stmt(in) : int returned by a successful compilation
 * result(out): query results descriptor
 */
static int
db_execute_and_keep_statement_local (DB_SESSION * session,
				     DB_QUERY_RESULT ** result)
{
  PARSER_CONTEXT *parser;
  PT_NODE *statement;
  DB_QUERY_RESULT *qres;
  DB_VALUE *val;
  int err = NO_ERROR;
  SERVER_INFO server_info;
  RYE_STMT_TYPE stmt_type;

  if (result != NULL)
    {
      *result = NULL;
    }

  /* obvious error checking - invalid parameter */
  if (!session || !session->parser)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_INVALID_SESSION, 0);
      return er_errid ();
    }
  /* no statement was given in the session */
  if (!session->statement)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_IT_EMPTY_STATEMENT, 0);
      return er_errid ();
    }

  assert (session->statement->next == NULL);

#if !defined(NDEBUG)
  /* check iff valid DDL group id
   */
  if (pt_is_ddl_statement (session->statement))
    {
      assert (!(session->groupid > GLOBAL_GROUPID));
    }
#endif

  /* valid host variable was not set before */
  if (session->parser->host_var_count > 0
      && session->parser->set_host_var == 0)
    {
      if (pt_has_error (session->parser))
	{
	  pt_report_to_ersys (session->parser, PT_SEMANTIC);
	  /* forget about any previous compilation errors, if any */
	  pt_reset_error (session->parser);
	}
      else
	{
	  /* parsed statement has some host variable parameters
	     (input marker '?'), but no host variable (DB_VALUE array) was set
	     by db_push_values() API */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_UCI_TOO_FEW_HOST_VARS,
		  0);
	}
      return er_errid ();
    }

  /* if the parser already has something wrong - semantic error */
  if (session->stage < StatementExecutedStage
      && pt_has_error (session->parser))
    {
      pt_report_to_ersys (session->parser, PT_SEMANTIC);
      return er_errid ();
    }

  /*
   * Execution Stage
   */
  er_clear ();

  parser = session->parser;

  /* initialization */
  assert (parser != NULL);
  parser->query_id = NULL_QUERY_ID;
  parser->is_in_and_list = false;

  /* now, we have a statement to execute */
  statement = session->statement;

  /* if the statement was not compiled and prepared, do it */
  if (session->stage < StatementPreparedStage)
    {
      if (db_compile_statement_local (session) != NO_ERROR)
	{
	  return er_errid ();
	}
    }

  /* forget about any previous compilation errors, if any */
  pt_reset_error (parser);

  /* get sys_date, sys_time, sys_datetime values from the server */
  server_info.info_bits = 0;	/* init */

  if (statement->si_datetime
      || (statement->node_type == PT_CREATE_ENTITY
	  || statement->node_type == PT_ALTER))
    {
      /* Some create and alter statement require the server datetime
       * even though it does not explicitly refer datetime-related pseudocolumns.
       * For instance,
       *   create table foo (a datetime default sysdatetime);
       *   create view v_foo as select * from foo;
       */
      db_calculate_current_server_time (parser, &server_info);
    }

  /* request to the server */
  if (server_info.info_bits)
    {
      (void) qp_get_server_info (&server_info);
      db_set_base_server_time (&server_info);
    }

  /* skip ddl execution in case of parameter or opt. level */
  if (pt_is_ddl_statement (statement) == true)
    {
      if (prm_get_bool_value (PRM_ID_BLOCK_DDL_STATEMENT))
	{
	  const char *cp = statement->sql_user_text;

	  if (cp == NULL)
	    {
	      cp = statement->alias_print;
	    }

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BLOCK_DDL_STMT, 1,
		  cp ? cp : "unknown");
	  return ER_BLOCK_DDL_STMT;
	}

      /* if QO_PARAM_LEVEL indicate no execution, just return */
      if (qo_need_skip_execution ())
	{
	  return NO_ERROR;
	}

      /* DDL is auto-commit */
      (void) tran_commit ();

      err = locator_lock_system_ddl_lock ();
      if (err != NO_ERROR)
	{
	  return err;
	}
    }

  pt_null_etc (statement);

  /* now, execute the statement */
  err = do_execute_statement (session, statement);

  if (err < 0)
    {
      /* Do not override original error id with */
      if (er_errid () == NO_ERROR
	  && pt_has_error (parser) && err != ER_QPROC_INVALID_XASLNODE)
	{
	  pt_report_to_ersys_with_statement (parser, PT_EXECUTION, statement);
	  err = er_errid ();
	}
      /* free the allocated list_id area before leaving */
      pt_free_query_etc_area (parser, statement);
    }

  /* so now, the statement is executed */
  session->stage = StatementExecutedStage;

  /* execution succeeded, maybe. process result of the query */
  if (result && !(err < 0))
    {
      qres = NULL;
      stmt_type = pt_node_to_cmd_type (statement);

      switch (stmt_type)
	{
	case RYE_STMT_SELECT:
	  /* Check whether pt_new_query_result_descriptor() fails.
	     Similar tests are required for RYE_STMT_INSERT */
	  qres = pt_new_query_result_descriptor (parser, statement);
	  if (qres)
	    {
	      /* get number of rows as result */
	      err = db_query_tuple_count (qres);
	      db_set_result_column_type (qres, session->type_list);
	      qres->query_type = db_cp_query_type (session->type_list, false);

	      qres->is_server_query_ended = statement->is_server_query_ended;
	      statement->is_server_query_ended = false;
	    }
	  else
	    {
	      err = er_errid ();
	    }
	  break;

	case RYE_STMT_INSERT:
	  val = db_value_create ();
	  if (val)
	    {
	      db_make_object (val, NULL);

	      assert (DB_VALUE_DOMAIN_TYPE (val) == DB_TYPE_OBJECT
		      && DB_IS_NULL (val));

	      /* got a result, so use it */
	      qres = db_get_db_value_query_result (val);
	      if (qres)
		{
		  ;
		}
	      else
		{
		  err = er_errid ();
		}

	      /* db_get_db_value_query_result copied val, so free val */
	      db_value_free (val);
	      pt_null_etc (statement);
	    }
	  else
	    {
	      err = er_errid ();
	    }
	  break;

	case RYE_STMT_GET_ISO_LVL:
	case RYE_STMT_GET_TIMEOUT:
	case RYE_STMT_GET_OPT_LVL:
	  val = (DB_VALUE *) pt_node_etc (statement);
	  if (val)
	    {
	      /* got a result, so use it */
	      qres = db_get_db_value_query_result (val);
	      if (qres)
		{
		  /* get number of rows as result */
		  err = db_query_tuple_count (qres);
		  assert (err == 1);
		}
	      else
		{
		  err = er_errid ();
		}

	      /* db_get_db_value_query_result copied val, so free val */
	      db_value_free (val);
	      pt_null_etc (statement);
	    }
	  else
	    {
	      /* avoid changing err. it should have been
	         meaningfully set. if err = 0, uci_static will set
	         SQLCA to SQL_NOTFOUND! */
	    }
	  break;

	default:
	  break;
	}

      *result = qres;
    }

  /* Do not override original error id with  */
  /* last error checking */
  if (er_errid () == NO_ERROR
      && pt_has_error (parser) && err != ER_QPROC_INVALID_XASLNODE)
    {
      pt_report_to_ersys_with_statement (parser, PT_EXECUTION, statement);
      err = er_errid ();
    }

  /* reset the parser values */
  if (statement->si_datetime)
    {
      db_make_null (&parser->sys_datetime);
    }

  assert (statement == session->statement);
  assert (session->statement->next == NULL);

  if (pt_is_ddl_statement (statement) == true)
    {
      if (err == NO_ERROR)
	{
	  if (db_get_client_type () == BOOT_CLIENT_REPL_BROKER)
	    {
	      /* We will delay commit until catalog table is updated */
	      return err;
	    }
	  (void) tran_commit ();
	}
      else
	{
	  (void) tran_abort ();
	}
    }

  return err;
}

/*
 * db_execute_and_keep_statement() - Please refer to the
 *         db_execute_and_keep_statement_local() function
 * return : error status, if execution failed
 *          number of affected objects, if a success & stmt is a SELECT,
 *          UPDATE, DELETE, or INSERT
 * session(in) : contains the SQL query that has been compiled
 * stmt(in) : int returned by a successful compilation
 * result(out): query results descriptor
 */
int
db_execute_and_keep_statement (DB_SESSION * session,
			       DB_QUERY_RESULT ** result)
{
  int err;

#if 0				/* TODO - #955 set PERSIST */
  CHECK_CONNECT_MINUSONE ();
#endif

  assert (session != NULL);
  assert (session->parser != NULL);
  assert (session->statement != NULL);
  assert (session->statement->next == NULL);

  err = db_execute_and_keep_statement_local (session, result);

  return err;
}

/*
 * db_execute_statement_local() - This function executes the SQL statement
 *    identified by the stmt argument and returns the result. The
 *    statement ID must have already been returned by a previously successful
 *    call to the db_compile_statement() function.
 * returns  : error status, if execution failed
 *            number of affected objects, if a success & stmt is a
 *            SELECT, UPDATE, DELETE, or INSERT
 * session(in) : contains the SQL query that has been compiled
 * stmt(in) : int returned by a successful compilation
 * result(out): query results descriptor
 *
 * note : You must free the results of calling this function by using the
 *    db_query_end() function. The resources for the identified compiled
 *    statement (not its result) are freed. Consequently, the statement may
 *    not be executed again.
 */
int
db_execute_statement_local (DB_SESSION * session, DB_QUERY_RESULT ** result)
{
  int err;

  if (session == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  err = db_execute_and_keep_statement_local (session, result);

  if (session->statement != NULL)
    {
      assert (session->statement->next == NULL);

      /* free XASL_ID allocated by query_prepare()
         before freeing the statement */
      pt_free_statement_xasl_id (session->statement);
      parser_free_tree (session->parser, session->statement);
      session->statement = NULL;
    }

  return err;
}

/*
 * db_execute_statement() - Please refer to the
 *    db_execute_statement_local() function
 * returns  : error status, if execution failed
 *            number of affected objects, if a success & stmt is a
 *            SELECT, UPDATE, DELETE, or INSERT
 * session(in) : contains the SQL query that has been compiled
 * stmt(in) : int returned by a successful compilation
 * result(out): query results descriptor
 */
int
db_execute_statement (DB_SESSION * session, DB_QUERY_RESULT ** result)
{
  int err;

  CHECK_CONNECT_MINUSONE ();

  err = db_execute_statement_local (session, result);
  if (err < 0 || db_get_errors (session))
    {
      if (err >= 0)
	{
	  err = er_errid ();
	  if (err == NO_ERROR)
	    {
	      err = ER_FAILED;	/* may be grammar syntax error */
	    }
	}
    }

  return err;
}

/*
 * db_drop_statement() - This function frees the resources allocated to a
 *    compiled statement
 * return : void
 * session(in) : session handle
 * stmt(in) : statement id returned by a successful compilation
 */
void
db_drop_statement (DB_SESSION * session)
{
  if (session->statement != NULL)
    {
      assert (session->statement->next == NULL);

      pt_free_statement_xasl_id (session->statement);
      parser_free_tree (session->parser, session->statement);
      session->statement = NULL;
      session->stage = StatementInitialStage;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_drop_all_statements() - This function frees the resources allocated
 *    to a session's compiled statements
 * rerutn : void
 * session(in) : session handle contains the SQL queries that have been
 *   compiled
 */
void
db_drop_all_statements (DB_SESSION * session)
{
  db_drop_statement (session);
}
#endif

/*
 * db_close_session_local() - This function frees all resources of this session
 *    except query results
 * return : void
 * session(in) : session handle
 */
void
db_close_session_local (DB_SESSION * session)
{
  PARSER_CONTEXT *parser;
  int i;

  if (session == NULL)
    {
      return;
    }

  parser = session->parser;
  assert (parser != NULL);

  pt_end_query (parser, NULL_QUERY_ID, session->statement);
  assert (parser->query_id == NULL_QUERY_ID);

  if (session->type_list)
    {
      db_free_query_format (session->type_list);
    }

  if (session->statement)
    {
      assert (session->statement->next == NULL);

      pt_free_statement_xasl_id (session->statement);
      parser_free_tree (parser, session->statement);
      session->statement = NULL;
    }

  if (parser->host_variables)
    {
      DB_VALUE *hv;

      for (i = 0, hv = parser->host_variables;
	   i < parser->host_var_count; i++, hv++)
	{
	  db_value_clear (hv);
	}
      free_and_init (parser->host_variables);
    }

  parser->host_var_count = 0;

  pt_free_orphans (session->parser);
  parser_free_parser (session->parser);

  /* free shard info */
  if (session->shardkeys)
    {
      free_and_init (session->shardkeys);
    }

  free_and_init (session);
}

/*
 * db_close_session() - Please refer to the db_close_session_local() function
 * return: void
 * session(in) : session handle
 */
void
db_close_session (DB_SESSION * session)
{
  db_close_session_local (session);
}

/*
 * db_free_query() - If an implicit query was executed, free the query on the
 *   server.
 * returns  : void
 * session(in) : session handle
 */
void
db_free_query (DB_SESSION * session)
{
  pt_end_query (session->parser, NULL_QUERY_ID, NULL);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_get_parser() - This function returns session's parser
 * returns: session->parser
 * session (in): session handle
 *
 * note : This is a debugging function.
 */
PARSER_CONTEXT *
db_get_parser (DB_SESSION * session)
{
  return session->parser;
}
#endif

/*
 * db_session_set_groupid() -
 * returns: void
 *   session (in): session handle
 *   groupid (in):
 */
void
db_session_set_groupid (DB_SESSION * session, int groupid)
{
  if (session == NULL || session->parser == NULL
      || session->statement == NULL)
    {
      return;
    }

  assert (session->statement->next == NULL);

  session->groupid = groupid;
}

/*
 * db_session_set_from_migrator() -
 * returns: void
 *   session (in): session handle
 *   from_migrator (in):
 */
void
db_session_set_from_migrator (DB_SESSION * session, bool from_migrator)
{
  if (session == NULL || session->parser == NULL
      || session->statement == NULL)
    {
      return;
    }

  assert (session->statement->next == NULL);

  session->from_migrator = from_migrator;
}

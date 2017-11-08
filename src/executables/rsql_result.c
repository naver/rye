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
 * rsql_result.c : Query execution / result handling routine
 */

#ident "$Id$"

#include "config.h"

#include <float.h>
#include <signal.h>

#include "rsql.h"
#include "memory_alloc.h"
#include "porting.h"

/* this must be the last header file included!!! */
#include "dbval.h"

/* max columns to display each data type
 * NOTE: some of these are totally dependent on report-writer's
 * rendering library.
 */
#define	MAX_INTEGER_DISPLAY_LENGTH	  11
#define	MAX_BIGINT_DISPLAY_LENGTH	  20
#define	MAX_DOUBLE_DISPLAY_LENGTH	  (DBL_DIG + 9)
#define	MAX_TIME_DISPLAY_LENGTH		  11
#define	MAX_UTIME_DISPLAY_LENGTH	  25
#define MAX_DATETIME_DISPLAY_LENGTH       29
#define	MAX_DATE_DISPLAY_LENGTH		  10
#define	MAX_DEFAULT_DISPLAY_LENGTH	  20
#define STRING_TYPE_PREFIX_SUFFIX_LENGTH  2
#define BIT_TYPE_PREFIX_SUFFIX_LENGTH     3

/* structure for current query result information */
typedef struct
{
  DB_QUERY_RESULT *query_result;
  int num_attrs;
  char **attr_names;
  int *attr_lengths;
  DB_TYPE *attr_types;
  int max_attr_name_length;
  RYE_STMT_TYPE curr_stmt_type;
  int curr_stmt_line_no;
} CUR_RESULT_INFO;

typedef struct
{
  RYE_STMT_TYPE stmt_type;
  const char *cmd_string;
} RSQL_CMD_STRING_TABLE;

static RSQL_CMD_STRING_TABLE rsql_Cmd_string_table[] = {
  {RYE_STMT_SELECT, "SELECT"},
  {RYE_STMT_SELECT_UPDATE, "SELECT"},
  {RYE_STMT_GET_ISO_LVL, "GET ISOLATION LEVEL"},
  {RYE_STMT_GET_TIMEOUT, "GET LOCK TIMEOUT"},
  {RYE_STMT_GET_OPT_LVL, "GET OPTIMIZATION"},
  {RYE_STMT_UPDATE, "UPDATE"},
  {RYE_STMT_DELETE, "DELETE"},
  {RYE_STMT_INSERT, "INSERT"},
  {RYE_STMT_ALTER_CLASS, "ALTER"},
  {RYE_STMT_COMMIT_WORK, "COMMIT"},
  {RYE_STMT_CREATE_CLASS, "CREATE"},
  {RYE_STMT_CREATE_INDEX, "CREATE INDEX"},
  {RYE_STMT_DROP_DATABASE, "DROP LDB"},
  {RYE_STMT_DROP_CLASS, "DROP"},
  {RYE_STMT_DROP_INDEX, "DROP INDEX"},
  {RYE_STMT_ALTER_INDEX, "ALTER INDEX"},
  {RYE_STMT_DROP_LABEL, "DROP "},
  {RYE_STMT_RENAME_CLASS, "RENAME"},
  {RYE_STMT_ROLLBACK_WORK, "ROLLBACK"},
  {RYE_STMT_GRANT, "GRANT"},
  {RYE_STMT_REVOKE, "REVOKE"},
  {RYE_STMT_CREATE_USER, "CREATE USER"},
  {RYE_STMT_DROP_USER, "DROP USER"},
  {RYE_STMT_ALTER_USER, "ALTER USER"},
  {RYE_STMT_UPDATE_STATS, "UPDATE STATISTICS"},
  {RYE_STMT_SCOPE, "SCOPE"},
  {RYE_STMT_REGISTER_DATABASE, "REGISTER"},
  {RYE_STMT_SET_OPT_LVL, "SET OPTIMIZATION"},
  {RYE_STMT_SET_SYS_PARAMS, "SET SYSTEM PARAMETERS"},
  {RYE_STMT_SAVEPOINT, "SAVEPOINT"},
  {RYE_STMT_ON_LDB, "ON LDB"},
  {RYE_STMT_GET_LDB, "GET LDB"},
  {RYE_STMT_SET_LDB, "SET LDB"},
  {RYE_STMT_ALTER_SERIAL, "ALTER SERIAL"},
  {RYE_STMT_CREATE_SERIAL, "CREATE SERIAL"},
  {RYE_STMT_DROP_SERIAL, "DROP SERIAL"}
};

static const char *rsql_Isolation_level_string[] = {
  "UNKNOWN",
  "READ COMMITTED SCHEMA, READ UNCOMMITTED INSTANCES",
  "READ COMMITTED SCHEMA, READ COMMITTED INSTANCES",
  "REPEATABLE READ SCHEMA, READ UNCOMMITTED INSTANCES",
  "REPEATABLE READ SCHEMA, READ COMMITTED INSTANCES",
  "REPEATABLE READ SCHEMA, REPEATABLE READ INSTANCES",
  "SERIALIZABLE"
};

static const char *rsql_cmd_string (RYE_STMT_TYPE stmt_type,
				    const char *default_string);
static void display_empty_result (int stmt_type, int line_no);
static char **get_current_result (int **len,
				  const CUR_RESULT_INFO * result_info);
static int write_results_to_stream (const RSQL_ARGUMENT * rsql_arg, FILE * fp,
				    const CUR_RESULT_INFO * result_info);
static char *uncontrol_strdup (const char *from);
static char *uncontrol_strndup (const char *from, int length);
static int calculate_width (int column_width,
			    int string_width,
			    int origin_width,
			    DB_TYPE attr_type, bool is_null);
static bool is_string_type (DB_TYPE type);
static bool is_bit_type (DB_TYPE type);
static bool is_cuttable_type_by_string_width (DB_TYPE type);
static bool is_type_that_has_suffix (DB_TYPE type);

/*
 * rsql_results() - display the result
 *   return: none
 *   rsql_arg(in): rsql argument
 *   result(in): query result structure.
 *   attr_spec(in): result attribute spec structure
 *   line_no(in): line number on which the statement appears
 *   stmt_type(in): query statement type
 *
 * Note: If `result' is NULL, no results is assumed.
 */
void
rsql_results (const RSQL_ARGUMENT * rsql_arg, DB_QUERY_RESULT * result,
	      DB_QUERY_TYPE * attr_spec, int line_no, RYE_STMT_TYPE stmt_type)
{
  int i;
  DB_QUERY_TYPE *t;		/* temp ptr for attr_spec */
  int err;
  int *attr_name_lengths = NULL;	/* attribute name length array */
  CUR_RESULT_INFO result_info;
  int num_attrs = 0;
  char **attr_names = NULL;
  int *attr_lengths = NULL;
  DB_TYPE *attr_types = NULL;
  int max_attr_name_length = 0;

  /* trivial case - no results */
  if (result == NULL
      || (err = db_query_first_tuple (result)) == DB_CURSOR_END)
    {
      display_empty_result (stmt_type, line_no);
      return;
    }

  if (err < 0)
    {
      rsql_Error_code = RSQL_ERR_SQL_ERROR;
      goto error;
    }

  for (t = attr_spec; t != NULL; t = db_query_format_next (t), num_attrs++)
    {
      ;
    }

  /* allocate pointer array for attr names and int array for attr lengths */
  attr_names = (char **) malloc (sizeof (char *) * num_attrs);
  if (attr_names == NULL)
    {
      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
      goto error;
    }
  for (i = 0; i < num_attrs; i++)
    {
      attr_names[i] = (char *) NULL;
    }
  attr_name_lengths = (int *) malloc (sizeof (int) * num_attrs);
  attr_lengths = (int *) malloc (sizeof (int) * num_attrs);
  attr_types = (DB_TYPE *) malloc (sizeof (DB_TYPE) * num_attrs);
  if (attr_name_lengths == NULL || attr_lengths == NULL || attr_types == NULL)
    {
      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
      goto error;
    }

  /* get the result attribute names */

  max_attr_name_length = 0;

  for (i = 0, t = attr_spec; t != NULL; t = db_query_format_next (t), i++)
    {
      const char *temp;

      temp = db_query_format_name (t);
      if (temp == NULL)
	{
	  attr_names[i] = (char *) malloc (7);
	  if (attr_names[i] == NULL)
	    {
	      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
	      goto error;
	    }
	  strcpy (attr_names[0], "Result");
	}
      else
	{
	  bool is_console_conv = false;

	  /* console encoded attribute name */
	  if (rsql_text_utf8_to_console != NULL)
	    {
	      char *attr_name_console_encoded = NULL;
	      int attr_name_console_length = -1;

	      /* try to convert attribute name from utf-8 to console */
	      if ((*rsql_text_utf8_to_console) (temp,
						strlen (temp),
						&attr_name_console_encoded,
						&attr_name_console_length)
		  == NO_ERROR)
		{
		  if (attr_name_console_encoded != NULL)
		    {
		      free_and_init (attr_names[i]);
		      attr_names[i] = attr_name_console_encoded;
		      is_console_conv = true;
		    }
		}
	    }

	  if (!is_console_conv)
	    {
	      attr_names[i] = uncontrol_strdup (temp);
	      if (attr_names[i] == NULL)
		{
		  goto error;
		}
	    }
	}
      attr_name_lengths[i] = strlen (attr_names[i]);
      max_attr_name_length = MAX (max_attr_name_length, attr_name_lengths[i]);
      attr_types[i] = db_query_format_type (t);

      switch (attr_types[i])
	{
	case DB_TYPE_INTEGER:
	  attr_lengths[i] =
	    MAX (MAX_INTEGER_DISPLAY_LENGTH, attr_name_lengths[i]);
	  break;
	case DB_TYPE_BIGINT:
	  attr_lengths[i] =
	    MAX (MAX_BIGINT_DISPLAY_LENGTH, attr_name_lengths[i]);
	  break;
	case DB_TYPE_DOUBLE:
	  attr_lengths[i] =
	    MAX (MAX_DOUBLE_DISPLAY_LENGTH, attr_name_lengths[i]);
	  break;
	case DB_TYPE_TIME:
	  attr_lengths[i] =
	    -MAX (MAX_TIME_DISPLAY_LENGTH, attr_name_lengths[i]);
	  break;
	case DB_TYPE_DATETIME:
	  attr_lengths[i] =
	    -MAX (MAX_DATETIME_DISPLAY_LENGTH, attr_name_lengths[i]);
	  break;
	case DB_TYPE_DATE:
	  attr_lengths[i] =
	    -MAX (MAX_DATE_DISPLAY_LENGTH, attr_name_lengths[i]);
	  break;
	default:
	  attr_lengths[i] = -MAX_DEFAULT_DISPLAY_LENGTH;
	  break;
	}
    }

  result_info.query_result = result;
  result_info.num_attrs = num_attrs;
  result_info.attr_names = attr_names;
  result_info.attr_lengths = attr_lengths;
  result_info.attr_types = attr_types;
  result_info.max_attr_name_length = max_attr_name_length;
  result_info.curr_stmt_type = stmt_type;
  result_info.curr_stmt_line_no = line_no;

  if (write_results_to_stream (rsql_arg, rsql_Output_fp, &result_info) ==
      RSQL_FAILURE)
    {
      if (rsql_Error_code == RSQL_ERR_SQL_ERROR)
	{
	  rsql_display_rsql_err (0, 0);
	}
      else
	{
	  nonscr_display_error ();
	}
    }
  /* free memories */
  if (attr_names != NULL)
    {
      for (i = 0; i < num_attrs; i++)
	{
	  if (attr_names[i] != NULL)
	    {
	      free_and_init (attr_names[i]);
	    }
	}
      free_and_init (attr_names);
    }
  if (attr_name_lengths != NULL)
    {
      free_and_init (attr_name_lengths);
    }
  if (attr_lengths != NULL)
    {
      free_and_init (attr_lengths);
    }
  if (attr_types != NULL)
    {
      free_and_init (attr_types);
    }

  return;

error:

  if (rsql_Error_code == RSQL_ERR_SQL_ERROR)
    {
      rsql_display_rsql_err (line_no, 0);
      rsql_check_server_down ();
      /* for correct rsql return code */
      rsql_Num_failures++;
    }

  /* free memories */
  if (attr_names != NULL)
    {
      for (i = 0; i < num_attrs; i++)
	{
	  if (attr_names[i] != NULL)
	    {
	      free_and_init (attr_names[i]);
	    }
	}
      free_and_init (attr_names);
    }
  if (attr_name_lengths != NULL)
    {
      free_and_init (attr_name_lengths);
    }
  if (attr_lengths != NULL)
    {
      free_and_init (attr_lengths);
    }
  if (attr_types != NULL)
    {
      free_and_init (attr_types);
    }
}

/*
 * rsql_cmd_string() - return the command string associated with a statement enum
 *   return:  const char*
 *   stmt_type(in): statement enum
 *   default_string(in): default command string if stmt_type is invallid
 */
static const char *
rsql_cmd_string (RYE_STMT_TYPE stmt_type, const char *default_string)
{
  int i;
  int table_size = DIM (rsql_Cmd_string_table);

  for (i = 0; i < table_size; i++)
    {
      if (rsql_Cmd_string_table[i].stmt_type == stmt_type)
	{
	  return (rsql_Cmd_string_table[i].cmd_string);
	}
    }
  return default_string;
}

/*
 * display_empty_result() - display the empty result message
 *   return: none
 *   stmt_type(in): current statement type
 *   line_no(in): current statement line number
 */
static void
display_empty_result (int stmt_type, int line_no)
{
  FILE *pf;			/* pipe stream to pager */

  snprintf (rsql_Scratch_text, SCRATCH_TEXT_LEN,
	    msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL,
			    RSQL_RESULT_STMT_TITLE_FORMAT),
	    rsql_cmd_string ((RYE_STMT_TYPE) stmt_type, ""), line_no);

  pf = rsql_popen (rsql_Pager_cmd, rsql_Output_fp);

  rsql_fputs ("\n=== ", pf);
  rsql_fputs_console_conv (rsql_Scratch_text, pf);
  rsql_fputs (" ===\n\n", pf);
  rsql_fputs_console_conv (msgcat_message (MSGCAT_CATALOG_RSQL,
					   MSGCAT_RSQL_SET_RSQL,
					   RSQL_STAT_NONSCR_EMPTY_RESULT_TEXT),
			   pf);
  rsql_fputs ("\n", pf);

  rsql_pclose (pf, rsql_Output_fp);

  return;
}

/*
 * get_current_result() - get the attribute values of the current result
 *   return: pointer newly allocated value array. On error, NULL.
 *   lengths(out): lengths of returned values
 *   result_info(in): pointer to current query result info structure
 *
 * Note:
 *   Caller should be responsible for free the return array and its elements.
 */
static char **
get_current_result (int **lengths, const CUR_RESULT_INFO * result_info)
{
  int i;
  char **val = NULL;		/* temporary array for values */
  int *len = NULL;		/* temporary array for lengths */
  DB_VALUE db_value;
  RYE_STMT_TYPE stmt_type = result_info->curr_stmt_type;
  DB_QUERY_RESULT *result = result_info->query_result;
  int num_attrs = result_info->num_attrs;

  val = (char **) malloc (sizeof (char *) * num_attrs);
  if (val == NULL)
    {
      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
      goto error;
    }
  memset (val, 0, sizeof (char *) * num_attrs);

  len = (int *) malloc (sizeof (int) * num_attrs);
  if (len == NULL)
    {
      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
      goto error;
    }
  memset (len, 0, sizeof (int) * num_attrs);

  (void) db_query_set_copy_tplvalue (result, 0 /* peek */ );

  /* get attribute values */
  for (i = 0; i < num_attrs; i++)
    {
      DB_TYPE value_type;
      DB_MAKE_NULL (&db_value);

      if (db_query_get_tuple_value (result, i, &db_value) < 0)
	{
	  rsql_Error_code = RSQL_ERR_SQL_ERROR;
	  goto error;
	}

      value_type = DB_VALUE_TYPE (&db_value);

      /*
       * This assert is intended to validate that the server returned the
       * expected types for the query results. See the note in
       * pt_print_value () regarding XASL caching.
       */
      /*
       * TODO add a similar check to the ux_* and/or cci_* and/or the server
       *      functions so that the results' types returned through sockets in
       *      CS_MODE are validated.
       */
      assert (value_type == DB_TYPE_NULL
	      /* UNKNOWN, maybe host variable */
	      || result_info->attr_types[i] == DB_TYPE_NULL
	      || result_info->attr_types[i] == DB_TYPE_VARIABLE
	      || value_type == result_info->attr_types[i]);

#if 0				/* TODO - */
      /* reset to the valid result type
       */
      if (result_info->attr_types[i] == DB_TYPE_VARIABLE)
	{
	  if (value_type != DB_TYPE_NULL)
	    {
	      assert (value_type != DB_TYPE_VARIABLE);
	      result_info->attr_types[i] = value_type;

	      switch (result_info->attr_types[i])
		{
		case DB_TYPE_INTEGER:
		  result_info->attr_lengths[i] =
		    MAX (MAX_INTEGER_DISPLAY_LENGTH,
			 strlen (result_info->attr_names[i]));
		  break;
		case DB_TYPE_BIGINT:
		  result_info->attr_lengths[i] =
		    MAX (MAX_BIGINT_DISPLAY_LENGTH,
			 strlen (result_info->attr_names[i]));
		  break;
		case DB_TYPE_DOUBLE:
		  result_info->attr_lengths[i] =
		    MAX (MAX_DOUBLE_DISPLAY_LENGTH,
			 strlen (result_info->attr_names[i]));
		  break;
		case DB_TYPE_TIME:
		  result_info->attr_lengths[i] =
		    -MAX (MAX_TIME_DISPLAY_LENGTH,
			  strlen (result_info->attr_names[i]));
		  break;
		case DB_TYPE_DATETIME:
		  result_info->attr_lengths[i] =
		    -MAX (MAX_DATETIME_DISPLAY_LENGTH,
			  strlen (result_info->attr_names[i]));
		  break;
		case DB_TYPE_DATE:
		  result_info->attr_lengths[i] =
		    -MAX (MAX_DATE_DISPLAY_LENGTH,
			  strlen (result_info->attr_names[i]));
		  break;
		default:
		  result_info->attr_lengths[i] = -MAX_DEFAULT_DISPLAY_LENGTH;
		  break;
		}

	    }
	}
#endif

      switch (value_type)
	{
	case DB_TYPE_NULL:	/* null value */
	  val[i] = (char *) malloc (5);
	  if (val[i] == NULL)
	    {
	      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
	      goto error;
	    }
	  strcpy (val[i], "NULL");
	  break;

	default:		/* other types */
	  /*
	   * If we are printing the isolation level, we need to
	   * interpret it for the user, not just return a meaningless number.
	   *
	   * Also interpret a lock timeout value of -1
	   */
	  if (stmt_type == RYE_STMT_GET_ISO_LVL)
	    {
	      int iso_lvl;

	      iso_lvl = DB_GET_INTEGER (&db_value);

	      val[i] = (char *) malloc (128);
	      if (val[i] == NULL)
		{
		  rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
		  goto error;
		}

	      if (iso_lvl < 1 || iso_lvl > 6)
		{
		  assert (false);
		  iso_lvl = 0;
		}

	      sprintf (val[i], "%s", rsql_Isolation_level_string[iso_lvl]);
	    }
	  else if ((stmt_type == RYE_STMT_GET_TIMEOUT)
		   && (DB_GET_DOUBLE (&db_value) == -1.0))
	    {
	      val[i] = (char *) malloc (9);
	      if (val[i] == NULL)
		{
		  rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
		  goto error;
		}
	      strcpy (val[i], "INFINITE");
	    }
	  else
	    {
	      char *temp;

	      temp = rsql_db_value_as_string (&db_value, &len[i]);
	      if (temp == NULL)
		{
		  rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
		  goto error;
		}
	      temp[len[i]] = '\0';
	      val[i] = temp;
	    }
	}

      if (len[i] == 0 && val[i])
	{
	  len[i] = strlen (val[i]);
	}
    }

  if (lengths)
    {
      *lengths = len;
    }
  return (val);

error:
  if (val != NULL)
    {
      for (i = 0; i < num_attrs; i++)
	{
	  if (val[i] != NULL)
	    {
	      free_and_init (val[i]);
	    }
	}
      free_and_init (val);
    }
  if (len != NULL)
    {
      free_and_init (len);
    }
  return ((char **) NULL);
}

/*
 * write_results_to_stream()
 *   return: RSQL_FAILURE/RSQL_SUCCESS
 *   rsql_arg(in): rsql argument
 *   fp(in): file stream pointer
 *   result_info(in): pointer to current query result info structure
 *
 * Note: This function may set rsql_Error_code RSQL_ERR_SQL_ERROR to indicate
 *       the error
 */
static int
write_results_to_stream (const RSQL_ARGUMENT * rsql_arg, FILE * fp,
			 const CUR_RESULT_INFO * result_info)
{
  typedef char **value_array;
  volatile value_array val;	/* attribute values array */
  int *len;			/* attribute values lengths */
  volatile int error;		/* to switch return of RSQL_FAILURE/RSQL_SUCCESS */
  int i;			/* loop counter */
  int object_no;		/* result object count */
  int e;			/* error code from DBI */
  FILE *pf = NULL;		/* pipe stream to pager */
  int n;			/* # of cols for a line */
  RYE_STMT_TYPE stmt_type = result_info->curr_stmt_type;
  DB_QUERY_RESULT *result = result_info->query_result;
  DB_TYPE *attr_types = result_info->attr_types;
  int line_no = result_info->curr_stmt_line_no;
  int num_attrs = result_info->num_attrs;
  int *attr_lengths = result_info->attr_lengths;
  char **attr_names = result_info->attr_names;
  char *value = NULL;
  int max_attr_name_length = result_info->max_attr_name_length;
  int column_width;
  int rsql_string_width = rsql_arg->string_width;
  int value_width;
  bool is_null;

  val = (char **) NULL;
  len = NULL;
  error = FALSE;

  /* simple code without signal, setjmp, longjmp
   */

  pf = rsql_popen (rsql_Pager_cmd, fp);

  rsql_fputs ("\n=== ", pf);
  snprintf (rsql_Scratch_text, SCRATCH_TEXT_LEN,
	    rsql_get_message (RSQL_RESULT_STMT_TITLE_FORMAT),
	    rsql_cmd_string (stmt_type, "UNKNOWN"), line_no);
  rsql_fputs (rsql_Scratch_text, pf);
  rsql_fputs (" ===\n\n", pf);

  if (db_query_first_tuple (result) < 0)
    {
      rsql_Error_code = RSQL_ERR_SQL_ERROR;
      error = TRUE;
      goto done;
    }

  if (!rsql_arg->line_output)
    {
      for (n = i = 0; i < num_attrs; i++)
	{
	  fprintf (pf, "  %*s", (int) (attr_lengths[i]), attr_names[i]);
	  n += 2 + ((attr_lengths[i] > 0) ? attr_lengths[i] :
		    -attr_lengths[i]);
	}
      putc ('\n', pf);
      for (; n > 0; n--)
	{
	  putc ('=', pf);
	}
      putc ('\n', pf);
    }

  for (object_no = 1;; object_no++)
    {
      rsql_Row_count = object_no;
      /* free previous result */
      if (val != NULL)
	{
	  assert (num_attrs == result_info->num_attrs);
	  for (i = 0; i < num_attrs; i++)
	    {
	      free_and_init (val[i]);
	    }
	  free_and_init (val);
	}
      if (len)
	{
	  free_and_init (len);
	}

      val = get_current_result (&len, result_info);
      if (val == NULL)
	{
	  rsql_Error_code = RSQL_ERR_SQL_ERROR;
	  error = TRUE;
	  goto done;
	}

      if (!rsql_arg->line_output)
	{
	  int padding_size;

	  for (i = 0; i < num_attrs; i++)
	    {
	      if (strcmp ("NULL", val[i]) == 0)
		{
		  is_null = true;
		}
	      else
		{
		  is_null = false;
		}

	      column_width = rsql_get_column_width (attr_names[i]);
	      value_width =
		calculate_width (column_width,
				 rsql_string_width,
				 len[i], attr_types[i], is_null);

	      padding_size = (attr_lengths[i] > 0) ?
		MAX (attr_lengths[i] -
		     (value_width), 0)
		: MIN (attr_lengths[i] + (value_width), 0);

	      fprintf (pf, "  ");
	      if (padding_size > 0)
		{
		  /* right justified */
		  fprintf (pf, "%*s", (int) padding_size, "");
		}

	      value = val[i];
	      if (is_type_that_has_suffix (attr_types[i]) && is_null == false)
		{
		  value[value_width - 1] = '\'';
		}

	      fwrite (value, 1, value_width, pf);

	      if (padding_size < 0)
		{
		  /* left justified */
		  fprintf (pf, "%*s", (int) (-padding_size), "");
		}
	    }
	  putc ('\n', pf);
	  /* fflush(pf); */
	}
      else
	{
	  fprintf (pf, "<%05d>", object_no);
	  for (i = 0; i < num_attrs; i++)
	    {
	      fprintf (pf, "%*c", (int) ((i == 0) ? 1 : 8), ' ');
	      fprintf (pf, "%*s: %s\n", (int) (-max_attr_name_length),
		       attr_names[i], val[i]);
	    }
	  /* fflush(pf); */
	}

      /* advance to next */
      e = db_query_next_tuple (result);
      if (e < 0)
	{
	  rsql_Error_code = RSQL_ERR_SQL_ERROR;
	  error = TRUE;
	  goto done;
	}
      else if (e == DB_CURSOR_END)
	{
	  break;
	}
    }
  putc ('\n', pf);

done:

  if (pf)
    {
      rsql_pclose (pf, fp);
    }

  /* free result */
  if (val != NULL)
    {
      for (i = 0; i < num_attrs; i++)
	{
	  free_and_init (val[i]);
	}
      free_and_init (val);
    }
  if (len)
    {
      free_and_init (len);
    }

  return ((error) ? RSQL_FAILURE : RSQL_SUCCESS);
}


/*
 * calcluate_width() - calculate column's width
 *   return: width
 *   column_width(in): column width
 *   string_width(in): string width
 *   origin_width(in): real width
 *   attr_type(in): type
 *   is_null(in): check null
 */
int
calculate_width (int column_width, int string_width,
		 int origin_width, DB_TYPE attr_type, bool is_null)
{
  int result = 0;

  if (column_width > 0)
    {
      if (is_null)
	{
	  result = column_width;
	}
      else if (is_string_type (attr_type))
	{
	  result = column_width + STRING_TYPE_PREFIX_SUFFIX_LENGTH;
	}
      else if (is_bit_type (attr_type))
	{
	  result = column_width + BIT_TYPE_PREFIX_SUFFIX_LENGTH;
	}
      else
	{
	  result = column_width;
	}
    }
  else if (is_cuttable_type_by_string_width (attr_type) && string_width > 0)
    {
      if (is_null)
	{
	  result = string_width;
	}
      else if (is_string_type (attr_type))
	{
	  result = string_width + STRING_TYPE_PREFIX_SUFFIX_LENGTH;
	}
      else if (is_bit_type (attr_type))
	{
	  result = string_width + BIT_TYPE_PREFIX_SUFFIX_LENGTH;
	}
      else
	{
	  result = string_width;
	}
    }
  else
    {
      result = origin_width;
    }

  if (result > origin_width)
    {
      result = origin_width;
    }
  if (result < 0)
    {
      result = 0;
    }

  return result;
}

/*
 * is_string_type() - check whether it is a string type or not
 *   return: bool
 *   type(in): type
 */
static bool
is_string_type (DB_TYPE type)
{
  switch (type)
    {
    case DB_TYPE_VARCHAR:
      return true;
    default:
      return false;
    }
  return false;
}

/*
 * is_bit_type() - check whether it is a bit type or not
 *   return: bool
 *   type(in): type
 */
static bool
is_bit_type (DB_TYPE type)
{
  switch (type)
    {
    case DB_TYPE_VARBIT:
      return true;
    default:
      return false;
    }
  return false;
}

/*
 * is_cuttable_type_by_string_width() - check whether it is cuttable type by string_width or not
 *   return: bool
 *   type(in): type
 */
static bool
is_cuttable_type_by_string_width (DB_TYPE type)
{
  return (is_string_type (type) || is_bit_type (type));
}

/*
 * is_type_that_has_suffix() - check whether this type has suffix or not
 *   return: bool
 *   type(in): type
 */
static bool
is_type_that_has_suffix (DB_TYPE type)
{
  return (is_string_type (type) || is_bit_type (type));
}

/*
 * uncontrol_strndup() - variation of strdup()
 *   return:  newly allocated string
 *   from(in): source string
 *   length(in): length of source string
 */
static char *
uncontrol_strndup (const char *from, int length)
{
  char *to;

  /* allocate memory for `to' */
  to = (char *) malloc (length + 1);
  if (to == NULL)
    {
      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
      return ((char *) NULL);
    }

  memcpy (to, from, length);
  to[length] = 0;

  return to;
}

/*
 * uncontrol_strdup() - variation of strdup()
 *   return:  newly allocated string
 *   from(in): source string
 */
static char *
uncontrol_strdup (const char *from)
{
  return uncontrol_strndup (from, strlen (from));
}

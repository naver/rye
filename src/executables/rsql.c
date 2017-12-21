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
 * rsql.c : rsql main module
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <signal.h>
#include <wctype.h>
#include <editline/readline.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "rsql.h"
#include "system_parameter.h"
#include "message_catalog.h"
#include "porting.h"
#include "release_string.h"
#include "error_manager.h"
#include "language_support.h"
#include "intl_support.h"
#include "network.h"
#include "schema_manager.h"
#include "optimizer.h"
#include "environment_variable.h"
#include "tcp.h"
#include "db.h"
#include "parser.h"
#include "network_interface_cl.h"
#include "utility.h"
#include "connection_support.h"
#include "boot_cl.h"

/* input type specification for rsql_execute_statements() */
enum
{
  FILE_INPUT = 0,		/* FILE stream */
  STRING_INPUT = 1,		/* null-terminated string */
  EDITOR_INPUT = 2		/* command buffer */
};

/* the return value of rsql_do_session_cmd() */
enum
{
  DO_CMD_SUCCESS = 0,
  DO_CMD_FAILURE = 1,
  DO_CMD_EXIT = 2
};

#define RSQL_SESSION_COMMAND_PREFIX(C)	(((C) == ';') || ((C) == '!'))

/* size of input buffer */
#define LINE_BUFFER_SIZE         (4 * ONE_K)
#define COLUMN_WIDTH_INFO_LIST_INIT_SIZE 24
#define NOT_FOUND -1

int (*rsql_text_utf8_to_console) (const char *, const int, char **,
				  int *) = NULL;

int (*rsql_text_console_to_utf8) (const char *, const int, char **,
				  int *) = NULL;

int rsql_Row_count;
int rsql_Num_failures;

/* command editor lines */
int rsql_Line_lwm = -1;

/* default environment command names */
char rsql_Print_cmd[PATH_MAX] = "lpr";
char rsql_Pager_cmd[PATH_MAX] = "more";
char rsql_Editor_cmd[PATH_MAX] = "vi";

char rsql_Shell_cmd[PATH_MAX] = "csh";

/* tty file stream which is used for conversation with users.
 * In batch mode, this will be set to "/dev/null"
 */
static FILE *rsql_Tty_fp = NULL;

/* scratch area to make a message text to be displayed.
 * NOTE: Never put chars more than sizeof(rsql_Scratch_text).
 */
char rsql_Scratch_text[SCRATCH_TEXT_LEN];

int rsql_Error_code = NO_ERROR;

static char rsql_Prompt[100];
static char rsql_Name[100];

/*
 * Handles for the various files
 */
FILE *rsql_Input_fp = NULL;
FILE *rsql_Output_fp = NULL;
FILE *rsql_Error_fp = NULL;

typedef enum
{
  RSQL_INPUT_UNKNOWN,
  RSQL_INPUT_FILE,
  RSQL_INPUT_EDIT_CONTENTS,
  RSQL_INPUT_COMMAND,
  RSQL_INPUT_PROMPT
} RSQL_INPUT_TYPE;

typedef struct rsql_input
{
  RSQL_INPUT_TYPE type;
  FILE *fp;
  char *fname;
  char *string_buffer;
  int string_buffer_size;
} RSQL_INPUT;

/*
 * Global longjmp environment to terminate the rsql() interpreter in the
 * event of fatal error.  This should be used rather than calling
 * exit(), primarily for the Windows version of the interpreter.
 *
 * Set rsql_Exit_status to the numeric status code to be returned from
 * the rsql() function after the longjmp has been performed.
 */
static jmp_buf rsql_Exit_env;
static int rsql_Exit_status = EXIT_SUCCESS;

/* this is non-zero if there is a dangling connection to a database */
static bool rsql_Database_connected = false;

static bool rsql_Is_interactive = false;
static bool rsql_Is_sigint_caught = false;
static bool rsql_Is_echo_on = false;
enum
{ HISTO_OFF, HISTO_ON };
static int rsql_Is_histo_on = HISTO_OFF;

static RSQL_COLUMN_WIDTH_INFO *rsql_column_width_info_list = NULL;
static int rsql_column_width_info_list_size = 0;
static int rsql_column_width_info_list_index = 0;

static bool rsql_Query_trace = false;

static void free_rsql_column_width_info_list ();
static int initialize_rsql_column_width_info_list ();
static int get_column_name_argument (char **column_name,
				     char **val_str, char *argument);
static void display_buffer (void);
static void start_rsql (RSQL_ARGUMENT * rsql_arg);
static void rsql_read_file (const char *file_name);
static void rsql_write_file (const char *file_name, int append_flag);
static void display_error (DB_SESSION * session, int stmt_start_line_no);
static void free_attr_spec (DB_QUERY_TYPE ** attr_spec);
static void rsql_print_database (void);
static void rsql_set_sys_param (const char *arg_str);
static void rsql_get_sys_param (const char *arg_str);
static void rsql_set_plan_dump (const char *arg_str);
static void rsql_exit_init (void);
static void rsql_exit_cleanup (void);
static void rsql_print_buffer (void);
static void rsql_change_working_directory (const char *dirname);
static void rsql_exit_session (int error);

static int rsql_execute_statements (const RSQL_ARGUMENT * rsql_arg,
				    char *stream, int stream_length,
				    int line_no);

static SESSION_CMD get_session_cmd (char **argument, char *cmd_line);
static int rsql_do_session_cmd (SESSION_CMD cmd, char *argumen,
				RSQL_ARGUMENT * rsql_arg);
static bool rsql_prompt_only_command (SESSION_CMD cmd);
static void rsql_set_trace (const char *arg_str);
static void rsql_display_trace (const RSQL_ARGUMENT * rsql_arg);
static SESSION_CMD get_query_or_rsql_cmd (char **argument,
					  RSQL_QUERY * query,
					  int *line_number,
					  RSQL_INPUT * input);
static int rsql_input_init (RSQL_INPUT * input, RSQL_INPUT_TYPE type);
static void rsql_input_final (RSQL_INPUT * input);
static bool rsql_input_end (RSQL_INPUT * input);
static char *rsql_read_line (RSQL_INPUT * input);
static int rsql_run_input (RSQL_INPUT * input, RSQL_ARGUMENT * rsql_arg);

/*
 * free_rsql_column_width_info_list() - free rsql_column_width_info_list
 * return: void
 */
static void
free_rsql_column_width_info_list ()
{
  int i;

  if (rsql_column_width_info_list == NULL)
    {
      rsql_column_width_info_list_size = 0;
      rsql_column_width_info_list_index = 0;

      return;
    }

  for (i = 0; i < rsql_column_width_info_list_size; i++)
    {
      if (rsql_column_width_info_list[i].name != NULL)
	{
	  free_and_init (rsql_column_width_info_list[i].name);
	}
    }

  if (rsql_column_width_info_list != NULL)
    {
      free_and_init (rsql_column_width_info_list);
    }

  rsql_column_width_info_list_size = 0;
  rsql_column_width_info_list_index = 0;
}

/*
 * initialize_rsql_column_width_info_list() - initialize rsql_column_width_info_list
 * return: int
 */
static int
initialize_rsql_column_width_info_list ()
{
  int i;

  rsql_column_width_info_list = malloc (sizeof (RSQL_COLUMN_WIDTH_INFO)
					* COLUMN_WIDTH_INFO_LIST_INIT_SIZE);
  if (rsql_column_width_info_list == NULL)
    {
      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;

      return RSQL_FAILURE;
    }

  rsql_column_width_info_list_size = COLUMN_WIDTH_INFO_LIST_INIT_SIZE;
  rsql_column_width_info_list_index = 0;
  for (i = 0; i < rsql_column_width_info_list_size; i++)
    {
      rsql_column_width_info_list[i].name = NULL;
      rsql_column_width_info_list[i].width = 0;
    }

  return RSQL_SUCCESS;
}

/*
 * rsql_display_msg() - displays the given msg to output device
 *   return: none
 *   string(in)
 */
void
rsql_display_msg (const char *string)
{
  rsql_fputs ("\n", rsql_Tty_fp);
  rsql_fputs_console_conv (string, rsql_Tty_fp);
  rsql_fputs ("\n", rsql_Tty_fp);
}

/*
 * display_buffer() - display command buffer into stdout
 *   return: none
 */
static void
display_buffer (void)
{
  int l = 1;
  FILE *pf;
  const char *edit_contents, *p;

  /* simple code without signal, setjmp, longjmp
   */

  pf = rsql_popen (rsql_Pager_cmd, rsql_Output_fp);

  edit_contents = rsql_edit_contents_get ();

  putc ('\n', pf);
  while (edit_contents != NULL && *edit_contents != '\0')
    {
      fprintf (pf, "%4d  ", l++);
      p = strchr (edit_contents, '\n');
      if (p)
	{
	  fwrite (edit_contents, 1, p - edit_contents, pf);
	  edit_contents = p + 1;
	}
      else
	{
	  fwrite (edit_contents, 1, strlen (edit_contents), pf);
	  edit_contents = NULL;
	}
      fprintf (pf, "\n");
    }
  putc ('\n', pf);

  rsql_pclose (pf, rsql_Output_fp);

}



/*
 * start_rsql()
 *   return: none
 *   sql_arg(in/out): RSQL_ARGUMENT structure
 *
 * Note:
 * There are four file pointers associated
 *      stdin     - input source
 *      stdout    - output file stream
 *      stderr    - error message file stream
 *      tty_fp    - conversation terminal file stream.
 *                  either NULL or stderr
 *
 * if -o is given, the output file descriptor is duplicated to STDOU_FILENO.
 * Also, if -i is given, -c is given or stdin is not a tty,
 *      `tty_fp' will be set to NULL. (No conversational messages)
 * Otherwise, `tty_fp' will be set to stderr
 *
 * If `single_line_execution' is true, it attemts to execute as soon as
 * it get a line. There is command buffer associated. This is effective
 * only when INTERACTIVE mode (stdin is tty).
 * If `command' is not NULL, it'll execute the command and exit and
 * `-i' option, preceding pipe (if any), `-s' option had no effect.
 */
static void
start_rsql (RSQL_ARGUMENT * rsql_arg)
{
  RSQL_INPUT input;
  int exit_code = EXIT_FAILURE;

  if (rsql_arg->column_output && rsql_arg->line_output)
    {
      rsql_Error_code = RSQL_ERR_INVALID_ARG_COMBINATION;
      goto fatal_error;
    }

  rsql_Output_fp = stdout;
  if (rsql_arg->out_file_name != NULL)
    {
      rsql_Output_fp = fopen (rsql_arg->out_file_name, "w");
      if (rsql_Output_fp == NULL)
	{
	  rsql_Error_code = RSQL_ERR_OS_ERROR;
	  goto fatal_error;
	}
    }

  if (initialize_rsql_column_width_info_list () != RSQL_SUCCESS)
    {
      goto fatal_error;
    }

  /* For batch file input and SQL/X command argument input */
  rsql_Tty_fp = NULL;

  if (rsql_arg->command)
    {
      if (rsql_edit_contents_append (rsql_arg->command, true) != RSQL_SUCCESS)
	{
	  goto fatal_error;
	}
      input.type = RSQL_INPUT_COMMAND;
    }
  else if (rsql_Is_interactive)
    {
      input.type = RSQL_INPUT_PROMPT;

      /* Start interactive conversation or single line execution */
      rsql_Tty_fp = rsql_Error_fp;
      if (lang_charset () == INTL_CODESET_UTF8)
	{
	  TEXT_CONVERSION *tc = lang_get_txt_conv ();

	  if (tc != NULL)
	    {
	      rsql_text_utf8_to_console = tc->utf8_to_text_func;
	      rsql_text_console_to_utf8 = tc->text_to_utf8_func;
	    }
	}

      stifle_history (prm_get_integer_value (PRM_ID_RSQL_HISTORY_NUM));
      using_history ();
    }
  else
    {
      input.type = RSQL_INPUT_FILE;
    }

  if (rsql_input_init (&input, input.type) != NO_ERROR)
    {
      goto fatal_error;
    }

  /* display product title */
  snprintf (rsql_Scratch_text, SCRATCH_TEXT_LEN, "\n\t%s\n\n",
	    rsql_get_message (RSQL_INITIAL_RSQL_TITLE));
  rsql_fputs_console_conv (rsql_Scratch_text, rsql_Tty_fp);

  snprintf (rsql_Scratch_text, SCRATCH_TEXT_LEN, "\n%s\n\n",
	    rsql_get_message (RSQL_INITIAL_HELP_MSG));
  rsql_fputs_console_conv (rsql_Scratch_text, rsql_Tty_fp);

  /*
   * rsql main
   */
  read_history (".rsql_history");
  exit_code = rsql_run_input (&input, rsql_arg);

  write_history (".rsql_history");

  rsql_input_final (&input);

  rsql_edit_contents_finalize ();
  rsql_exit_session (exit_code);

  return;

fatal_error:
  rsql_edit_contents_finalize ();
  if (histo_is_supported ())
    {
      if (rsql_Is_histo_on != HISTO_OFF)
	{
	  rsql_Is_histo_on = HISTO_OFF;
	  histo_stop ();
	}
    }

  db_end_session ();
  db_shutdown ();
  rsql_Database_connected = false;
  nonscr_display_error ();
  rsql_exit (EXIT_FAILURE);

  return;
}

/*
 * rsql_run_input() -
 *    return: EXIT_SUCCESS or EXIT_FAILURE
 *
 *    input(in/out):
 *    rsql_arg(in):
 */
static int
rsql_run_input (RSQL_INPUT * input, RSQL_ARGUMENT * rsql_arg)
{
  int exit_code = EXIT_SUCCESS;
  SESSION_CMD cmd = S_CMD_UNKNOWN;
  RSQL_QUERY query;
  int line_number = 0;
  char *argument = NULL;

  if (input == NULL)
    {
      assert (input != NULL);

      return EXIT_FAILURE;
    }

  if (rsql_query_init (&query, ONE_K) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  while (!rsql_input_end (input))
    {
#if 0				/* TODO - */
      if (db_Connect_status != DB_CONNECTION_STATUS_CONNECTED)
	{
	  rsql_Database_connected = false;
	  fputs ("!", rsql_Output_fp);
	  exit_code = EXIT_FAILURE;
	  break;
	}
#endif

      cmd = get_query_or_rsql_cmd (&argument, &query, &line_number, input);
      if (cmd == S_CMD_EXIT)
	{
	  exit_code = EXIT_SUCCESS;
	  break;
	}
      else if (cmd == S_CMD_RUN || cmd == S_CMD_XRUN)
	{
	  if (input->type == RSQL_INPUT_PROMPT)
	    {
	      RSQL_INPUT cmd_input;

	      cmd_input.type = RSQL_INPUT_EDIT_CONTENTS;
	      if (rsql_input_init (&cmd_input, cmd_input.type) != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      exit_code = rsql_run_input (&cmd_input, rsql_arg);

	      rsql_input_final (&cmd_input);

	      if (cmd == S_CMD_XRUN)
		{
		  rsql_edit_contents_clear ();
		}
	    }
	}
      else if (cmd == S_CMD_QUERY)
	{
	  (void) rsql_execute_statements (rsql_arg, query.query,
					  query.length, line_number);
	  rsql_query_clear (&query);
	}
      else if (cmd == S_CMD_UNKNOWN)
	{
	  nonscr_display_error ();
	}
      else
	{
	  assert (argument != NULL);

	  if (input->type == RSQL_INPUT_PROMPT
	      || !rsql_prompt_only_command (cmd))
	    {
	      if (rsql_do_session_cmd (cmd, argument, rsql_arg) ==
		  DO_CMD_EXIT)
		{
		  exit_code = EXIT_FAILURE;
		  break;
		}
	    }
	}

      if (input->type == RSQL_INPUT_PROMPT)
	{
	  line_number = 0;
	}
    }

  rsql_query_final (&query);
  return exit_code;

exit_on_error:

  return EXIT_FAILURE;
}

/*
 * rsql_input_final() -
 *
 *    input(in/out):
 */
static void
rsql_input_final (RSQL_INPUT * input)
{
  if (input == NULL)
    {
      assert (input != NULL);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");

      return;
    }

  input->string_buffer_size = 0;
  if (input->string_buffer != NULL)
    {
      free_and_init (input->string_buffer);
    }

  if (input->fname != NULL && input->fp != NULL)
    {
      assert (input->type == RSQL_INPUT_EDIT_CONTENTS
	      || input->type == RSQL_INPUT_COMMAND);

      fclose (input->fp);
      unlink (input->fname);
      free_and_init (input->fname);
    }

  input->fp = NULL;
}

/*
 * rsql_input_init() -
 *    return: NO_ERROR or error code
 *
 *    input(out):
 *    type(in):
 */
static int
rsql_input_init (RSQL_INPUT * input, RSQL_INPUT_TYPE type)
{
  int error = NO_ERROR;

  if (input == NULL)
    {
      assert (input != NULL);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");

      return error;
    }

  input->type = type;
  input->fp = NULL;
  input->fname = NULL;
  input->string_buffer = NULL;
  input->string_buffer_size = 0;

  switch (input->type)
    {
    case RSQL_INPUT_FILE:
    case RSQL_INPUT_EDIT_CONTENTS:
    case RSQL_INPUT_COMMAND:
      input->string_buffer = (char *) malloc (LINE_BUFFER_SIZE);
      if (input->string_buffer == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  LINE_BUFFER_SIZE);

	  GOTO_EXIT_ON_ERROR;
	}
      input->string_buffer_size = LINE_BUFFER_SIZE;
      if (input->type == RSQL_INPUT_FILE)
	{
	  input->fp = rsql_Input_fp;
	}
      else
	{
	  input->fname = tempnam (NULL, NULL);
	  if (input->fname == NULL)
	    {
	      error = ER_GENERIC_ERROR;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");

	      GOTO_EXIT_ON_ERROR;
	    }

	  input->fp = fopen (input->fname, "w");
	  if (input->fp == NULL)
	    {
	      free_and_init (input->fname);
	      GOTO_EXIT_ON_ERROR;
	    }
	  if (rsql_edit_write_file (input->fp) == RSQL_FAILURE)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  fclose (input->fp);
	  input->fp = NULL;

	  input->fp = fopen (input->fname, "r");
	  if (input->fp == NULL)
	    {
	      free_and_init (input->fname);
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      break;

    case RSQL_INPUT_PROMPT:
      break;
    default:
      assert (false);
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  input->string_buffer_size = 0;
  if (input->string_buffer != NULL)
    {
      free_and_init (input->string_buffer);
    }
  if (input->type == RSQL_INPUT_EDIT_CONTENTS)
    {
      if (input->fp != NULL)
	{
	  assert (input->fname != NULL);

	  fclose (input->fp);
	  input->fp = NULL;
	  unlink (input->fname);
	}
      if (input->fname != NULL)
	{
	  free_and_init (input->fname);
	}
    }

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  return error;
}

/*
 * rsql_input_end() -
 *    return: end: true,
 *
 *    input(in):
 */
static bool
rsql_input_end (RSQL_INPUT * input)
{
  int error;

  if (input == NULL)
    {
      assert (input != NULL);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");

      return true;
    }

  switch (input->type)
    {
    case RSQL_INPUT_FILE:
    case RSQL_INPUT_EDIT_CONTENTS:
    case RSQL_INPUT_COMMAND:
      if (feof (input->fp))
	{
	  return true;
	}
      else
	{
	  return false;
	}

    case RSQL_INPUT_PROMPT:
      return false;

    default:
      assert (false);
      return true;
    }
}

/*
 * rsql_read_line() -
 *    return: line buffer or NULL
 *
 *    input(in):
 */
static char *
rsql_read_line (RSQL_INPUT * input)
{
  char *line = NULL;
  char *utf8_line_read = NULL;
  unsigned char *utf8_line_buf = NULL;
  int line_length;
  int utf8_line_buf_size;

  switch (input->type)
    {
    case RSQL_INPUT_FILE:
    case RSQL_INPUT_EDIT_CONTENTS:
    case RSQL_INPUT_COMMAND:
      assert (input->string_buffer != NULL);
      assert (input->string_buffer_size >= 0);
      assert (input->fp != NULL);

      line = fgets ((char *) input->string_buffer,
		    input->string_buffer_size, input->fp);
      break;
    case RSQL_INPUT_PROMPT:
      assert (input->string_buffer_size >= 0);
      assert (input->fp == NULL);

      line = readline (rsql_Prompt);
      if (line == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (rsql_text_console_to_utf8 != NULL)
	{
	  line_length = strlen (line);
	  utf8_line_buf_size = INTL_UTF8_MAX_CHAR_SIZE * line_length;
	  utf8_line_buf = (unsigned char *) malloc (utf8_line_buf_size);
	  if (utf8_line_buf == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, utf8_line_buf_size);

	      GOTO_EXIT_ON_ERROR;
	    }

	  memset (utf8_line_buf, 0, utf8_line_buf_size);

	  utf8_line_read = (char *) utf8_line_buf;
	  if ((*rsql_text_console_to_utf8) (line, line_length,
					    &utf8_line_read,
					    &utf8_line_buf_size) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  free_and_init (line);
	  line = utf8_line_read;
	}

      if (input->string_buffer != NULL)
	{
	  free_and_init (input->string_buffer);
	}

      input->string_buffer = line;
      break;
    default:
      assert (false);
      line = NULL;
    }

  return line;

exit_on_error:
  if (line != NULL)
    {
      free_and_init (line);
    }

  return NULL;
}

/*
 * get_query_or_rsql_cmd_from_file -
 *    return SESSION_CMD
 *
 *    argument(out):
 *    query(out):
 *    line_number(in/out):
 *    input(in):
 */
static SESSION_CMD
get_query_or_rsql_cmd (char **argument, RSQL_QUERY * query,
		       int *line_number, RSQL_INPUT * input)
{
  char *line_read = NULL;
  int line_length;
  char *ptr;			/* loop pointer */
  bool read_whole_line;
  SESSION_CMD cmd = S_CMD_UNKNOWN;
  RSQL_STATEMENT_STATE state = RSQL_STATE_GENERAL;
  int error = NO_ERROR;

  *argument = NULL;
  /* check in string block or comment block or identifier block */
  bool is_in_block = false;

  /* For batch file input and SQL/X command argument input */
  rsql_Tty_fp = NULL;
  if (rsql_Is_interactive)
    {
      /* Start interactive conversation or single line execution */
      rsql_Tty_fp = rsql_Error_fp;
    }

  while ((line_read = rsql_read_line (input)) != NULL)
    {
      read_whole_line = false;

      /*
       * readline assure whole line user typed is located in
       * returned buffer
       * (but it doesn't contain '\n' in it)
       */
      if (input->type == RSQL_INPUT_PROMPT)
	{
	  read_whole_line = true;
	  (*line_number)++;
	}

      fflush (rsql_Output_fp);

      line_length = strlen (line_read);
      /* remove new line character */
      for (ptr = line_read + line_length - 1; line_length > 0; ptr--)
	{
	  if (*ptr == '\n')
	    {
	      read_whole_line = true;
	      (*line_number)++;
	    }

	  if (*ptr == '\n' || *ptr == '\r')
	    {
	      *ptr = '\0';
	      line_length--;
	    }
	  else
	    {
	      break;
	    }
	}

      if (RSQL_SESSION_COMMAND_PREFIX (line_read[0]) && is_in_block == false)
	{
	  if (rsql_Is_interactive)
	    {
	      add_history (line_read);
	    }

	  cmd = get_session_cmd (argument, line_read);

	  return cmd;
	}
      else
	{
	  error = rsql_query_append_string (query, line_read, line_length,
					    read_whole_line);
	  if (error != NO_ERROR)
	    {
	      return S_CMD_UNKNOWN;
	    }

	  /* read_whole_line == false means that
	   * fgets couldn't read whole line (exceeds buffer size)
	   * so we should concat next line before execute it.
	   */
	  if (read_whole_line == true || rsql_input_end (input))
	    {
	      state = rsql_walk_statement (line_read, state);

	      if (rsql_is_statement_complete (state))
		{
		  /* if eof is not reached,
		   * execute it only on single line execution mode with
		   * complete statement
		   */
		  return S_CMD_QUERY;
		}

	      /* because we don't want to execute session commands
	       * in string block or comment block or identifier block
	       */
	      is_in_block = rsql_is_statement_in_block (state);
	    }
	}
    }

  return S_CMD_EXIT;
}

/*
 * get_session_cmd -
 *   return: SESSION_CMD
 *
 *   argument(out):
 *   line_number(in/out):
 *   cmd_line(in):
 */
static SESSION_CMD
get_session_cmd (char **argument, char *cmd_line)
{
  SESSION_CMD cmd = S_CMD_UNKNOWN;
  char *sess_cmd, *sess_end;
  char sess_end_char;

  assert (argument != NULL);
  assert (cmd_line != NULL);

  *argument = NULL;

  /* get start session cmd */
  while (*cmd_line != '\0' && iswspace ((wint_t) (*cmd_line)))
    {
      cmd_line++;
    }
  if (*cmd_line == ';')
    {
      cmd_line++;
    }
  sess_cmd = (char *) cmd_line;

  /* get end session cmd */
  sess_end = NULL;
  while (*cmd_line != '\0' && !iswspace ((wint_t) (*cmd_line)))
    {
      cmd_line++;
    }
  if (iswspace ((wint_t) (*cmd_line)))
    {
      sess_end = cmd_line;
      sess_end_char = *cmd_line;
      *cmd_line++ = '\0';	/* put null-termination */
    }
  else
    {
      sess_end_char = '\0';
    }

  /* get start argument */
  while (*cmd_line != '\0' && iswspace ((wint_t) (*cmd_line)))
    {
      cmd_line++;
    }
  *argument = (char *) cmd_line;

  /* Now, `sess_cmd' points to null-terminated session command name and
   * `argument' points to remaining argument (it may be '\0' if not given).
   */

  cmd = rsql_get_session_cmd_no (sess_cmd);

  if (sess_end != NULL)
    {
      *sess_end = sess_end_char;
    }

  return cmd;
}

/*
 * rsql_prompt_only_command() -
 *    return:
 *
 *    cmd(in):
 */
static bool
rsql_prompt_only_command (SESSION_CMD cmd)
{
  switch (cmd)
    {
    case S_CMD_SHELL:
    case S_CMD_CLEAR:
    case S_CMD_EDIT:
    case S_CMD_LIST:
    case S_CMD_RUN:
    case S_CMD_XRUN:
      return true;
    default:
      return false;
    }

  return false;
}

/*
 * rsql_do_session_cmd() -
 *    return: DO_CMD_SUCCESS, DO_CMD_FAILURE = 1,  DO_CMD_EXIT = 2
 *
 *    cmd(in):
 *    argument(in):
 *    rsql_arg(in):
 */
static int
rsql_do_session_cmd (SESSION_CMD cmd, char *argument,
		     RSQL_ARGUMENT * rsql_arg)
{
  HIST_ENTRY *hist_entry;

  er_clear ();

  switch (cmd)
    {
      /* File stuffs */

    case S_CMD_READ:		/* read a file */
      rsql_read_file (argument);
      break;

    case S_CMD_WRITE:		/* write to a file */
    case S_CMD_APPEND:		/* append to a file */
      rsql_write_file (argument, cmd == S_CMD_APPEND);
      break;

    case S_CMD_PRINT:
      rsql_print_buffer ();
      break;

    case S_CMD_SHELL:		/* invoke shell */
      rsql_invoke_system (rsql_Shell_cmd);
      rsql_fputs ("\n", rsql_Tty_fp);
      break;

    case S_CMD_CD:
      rsql_change_working_directory (argument);
      break;

    case S_CMD_EXIT:		/* exit */
      return DO_CMD_EXIT;

      /* Edit stuffs */

    case S_CMD_CLEAR:		/* clear editor buffer */
      rsql_edit_contents_clear ();
      break;

    case S_CMD_EDIT:		/* invoke system editor */
      if (rsql_invoke_system_editor () != RSQL_SUCCESS)
	{
	  return DO_CMD_FAILURE;
	}
      break;

    case S_CMD_LIST:		/* display buffer */
      display_buffer ();
      break;

    case S_CMD_COMMIT:
      if (db_commit_transaction () < 0)
	{
	  rsql_display_rsql_err (0, 0);
	  rsql_check_server_down ();
	}
      else
	{
	  rsql_display_msg (rsql_get_message (RSQL_STAT_COMMITTED_TEXT));
	}
      break;

    case S_CMD_ROLLBACK:
      if (db_abort_transaction () < 0)
	{
	  rsql_display_rsql_err (0, 0);
	  rsql_check_server_down ();
	}
      else
	{
	  rsql_display_msg (rsql_get_message (RSQL_STAT_ROLLBACKED_TEXT));
	}
      break;

    case S_CMD_AUTOCOMMIT:
      if (!strcasecmp (argument, "on"))
	{
	  rsql_arg->auto_commit = true;
	}
      else if (!strcasecmp (argument, "off"))
	{
	  rsql_arg->auto_commit = false;
	}

      fprintf (rsql_Output_fp, "AUTOCOMMIT IS %s\n",
	       (rsql_arg->auto_commit
		&& prm_get_bool_value (PRM_ID_RSQL_AUTO_COMMIT) ? "ON" :
		"OFF"));
      break;

    case S_CMD_CHECKPOINT:
      if (au_is_dba_group_member (Au_user))
	{
	  db_checkpoint ();
	  if (db_error_code () != NO_ERROR)
	    {
	      rsql_display_rsql_err (0, 0);
	    }
	  else
	    {
	      rsql_display_msg (rsql_get_message (RSQL_STAT_CHECKPOINT_TEXT));
	    }
	}
      else
	{
	  fprintf (rsql_Output_fp, "Checkpointing is only allowed for"
		   " the rsql started with --user=DBA\n");
	}
      break;

    case S_CMD_KILLTRAN:
      if (au_is_dba_group_member (Au_user))
	{
	  rsql_killtran ((argument[0] == '\0') ? NULL : argument);
	}
      else
	{
	  fprintf (rsql_Output_fp, "Killing transaction is only allowed"
		   " for the rsql started with --user=DBA\n");
	}
      break;

    case S_CMD_RESTART:
      if (rsql_Database_connected)
	{
	  rsql_Database_connected = false;
	  db_end_session ();
	  db_shutdown ();
	}

#if 1				/* TODO - */
      if (argument[0] != '\0')
	{
	  rsql_arg->db_name = argument;	/* set new db_name */
	}
#endif

      if (db_restart_ex (UTIL_RSQL_NAME, rsql_arg->db_name,
			 rsql_arg->user_name, rsql_arg->passwd,
			 db_get_client_type ()) != NO_ERROR)
	{
	  rsql_Error_code = RSQL_ERR_SQL_ERROR;
	  rsql_display_rsql_err (0, 0);
	  rsql_check_server_down ();
	}
      else
	{
	  if (au_is_dba_group_member (Au_user))
	    {
	      au_disable ();
	    }
	  rsql_Database_connected = true;

	  rsql_display_msg (rsql_get_message (RSQL_STAT_RESTART_TEXT));
	}
      rsql_print_database ();
      break;

      /* Environment stuffs */
    case S_CMD_SHELL_CMD:
    case S_CMD_EDIT_CMD:
    case S_CMD_PRINT_CMD:
    case S_CMD_PAGER_CMD:
      if (*argument == '\0')
	{
	  fprintf (rsql_Output_fp, "\n\t%s\n\n",
		   (cmd == S_CMD_SHELL_CMD) ? rsql_Shell_cmd :
		   (cmd == S_CMD_EDIT_CMD) ? rsql_Editor_cmd :
		   (cmd == S_CMD_PRINT_CMD) ? rsql_Print_cmd :
		   rsql_Pager_cmd);
	}
      else
	{
	  strncpy ((cmd == S_CMD_SHELL_CMD) ? rsql_Shell_cmd :
		   (cmd == S_CMD_EDIT_CMD) ? rsql_Editor_cmd :
		   (cmd == S_CMD_PRINT_CMD) ? rsql_Print_cmd :
		   rsql_Pager_cmd, argument, PATH_MAX - 1);
	}
      break;

    case S_CMD_NOPAGER_CMD:
      rsql_Pager_cmd[0] = '\0';
      break;

      /* Help stuffs */
    case S_CMD_HELP:
      rsql_help_menu ();
      break;

    case S_CMD_SCHEMA:
      rsql_help_schema ((argument[0] == '\0') ? NULL : argument);
      if (rsql_arg->auto_commit
	  && prm_get_bool_value (PRM_ID_RSQL_AUTO_COMMIT))
	{
	  if (db_commit_transaction () < 0)
	    {
	      rsql_display_rsql_err (0, 0);
	      rsql_check_server_down ();
	    }
	  else
	    {
	      rsql_display_msg (rsql_get_message (RSQL_STAT_COMMITTED_TEXT));
	    }
	}
      break;

    case S_CMD_INFO:
      rsql_help_info ((argument[0] == '\0') ? NULL : argument,
		      rsql_arg->auto_commit
		      && prm_get_bool_value (PRM_ID_RSQL_AUTO_COMMIT));
      break;

    case S_CMD_DATABASE:
      rsql_print_database ();
      break;

    case S_CMD_SET_PARAM:
      rsql_set_sys_param (argument);
      break;

    case S_CMD_GET_PARAM:
      rsql_get_sys_param (argument);
      break;

    case S_CMD_PLAN_DUMP:
      rsql_set_plan_dump ((argument[0] == '\0') ? NULL : argument);
      break;

    case S_CMD_ECHO:
      if (!strcasecmp (argument, "on"))
	{
	  rsql_Is_echo_on = true;
	}
      else if (!strcasecmp (argument, "off"))
	{
	  rsql_Is_echo_on = false;
	}
      else
	{
	  fprintf (rsql_Output_fp, "ECHO IS %s\n",
		   (rsql_Is_echo_on ? "ON" : "OFF"));
	}
      break;

    case S_CMD_DATE:
      {
	time_t tloc = time (NULL);
	struct tm tmloc;
	char str[80];
	utility_localtime (&tloc, &tmloc);
	strftime (str, 80, "%a %B %d %H:%M:%S %Z %Y", &tmloc);
	fprintf (rsql_Output_fp, "\n\t%s\n", str);
      }
      break;

    case S_CMD_TIME:
      if (!strcasecmp (argument, "on"))
	{
	  rsql_arg->time_on = true;
	}
      else if (!strcasecmp (argument, "off"))
	{
	  rsql_arg->time_on = false;
	}
      else
	{
	  fprintf (rsql_Output_fp, "TIME IS %s\n",
		   (rsql_arg->time_on ? "ON" : "OFF"));
	}
      break;

    case S_CMD_LINE_OUTPUT:
      if (strcasecmp (argument, "on") == 0)
	{
	  if (rsql_arg->column_output)
	    {
	      rsql_Error_code = RSQL_ERR_INVALID_ARG_COMBINATION;
	      nonscr_display_error ();
	      break;
	    }
	  rsql_arg->line_output = true;
	}
      else if (strcasecmp (argument, "off") == 0)
	{
	  rsql_arg->line_output = false;
	}
      else
	{
	  fprintf (rsql_Output_fp, "LINE_OUTPUT IS %s\n",
		   (rsql_arg->line_output ? "ON" : "OFF"));
	}
      break;

    case S_CMD_COLUMN_WIDTH:
      {
	char *column_name = NULL;
	int width = 0, result;

	if (*argument == '\0')
	  {
	    int i;

	    for (i = 0; i < rsql_column_width_info_list_size; i++)
	      {
		if (rsql_column_width_info_list[i].name == NULL)
		  {
		    break;
		  }

		if (rsql_column_width_info_list[i].width <= 0)
		  {
		    continue;
		  }

		fprintf (rsql_Output_fp,
			 "COLUMN-WIDTH %s : %d\n",
			 rsql_column_width_info_list[i].name,
			 rsql_column_width_info_list[i].width);
	      }
	  }
	else
	  {
	    char *val_str = NULL;

	    result =
	      get_column_name_argument (&column_name, &val_str, argument);
	    if (result == RSQL_FAILURE)
	      {
		fprintf (rsql_Error_fp, "ERROR: Column name is too long.\n");
		break;
	      }

	    if (val_str == NULL)
	      {
		width = rsql_get_column_width (column_name);
		fprintf (rsql_Output_fp, "COLUMN-WIDTH %s : %d\n",
			 column_name, width);
	      }
	    else
	      {
		trim (val_str);
		result = parse_int (&width, val_str, 10);
		if (result != 0 || width < 0)
		  {
		    fprintf (rsql_Error_fp, "ERROR: Invalid argument(%s).\n",
			     val_str);
		    break;
		  }

		if (rsql_set_column_width_info (column_name, width) !=
		    RSQL_SUCCESS)
		  {
		    return DO_CMD_FAILURE;
		  }
	      }
	  }
      }
      break;

    case S_CMD_STRING_WIDTH:
      {
	int string_width = 0, result;

	if (*argument != '\0')
	  {
	    trim (argument);

	    result = parse_int (&string_width, argument, 10);

	    if (result != 0 || string_width < 0)
	      {
		fprintf (rsql_Error_fp,
			 "ERROR: Invalid string-width(%s).\n", argument);
	      }

	    rsql_arg->string_width = string_width;
	  }
	else
	  {
	    fprintf (rsql_Output_fp,
		     "STRING-WIDTH : %d\n", rsql_arg->string_width);
	  }
      }
      break;

    case S_CMD_GROUPID:
      {
	int groupid = GLOBAL_GROUPID, result;

	if (*argument != '\0')
	  {
	    trim (argument);

	    result = parse_int (&groupid, argument, 10);

	    if (result != 0 || groupid < GLOBAL_GROUPID)
	      {
#if 0				/* TODO - */
		fprintf (rsql_Error_fp,
			 "ERROR: Invalid groupid(%s).\n", argument);
#endif
	      }

	    rsql_arg->groupid = groupid;
	  }
	else
	  {
	    fprintf (rsql_Output_fp, "GROUPID : %d\n", rsql_arg->groupid);
	  }
      }
      break;


    case S_CMD_HISTO:
      if (histo_is_supported ())
	{
	  if (!strcasecmp (argument, "on"))
	    {
	      if (histo_start (false) == NO_ERROR)
		{
		  rsql_Is_histo_on = HISTO_ON;
		}
	      else
		{
		  if (er_errid () == ER_AU_DBA_ONLY)
		    {
		      fprintf (rsql_Output_fp,
			       "Histogram is allowed only for DBA\n");
		    }
		  else
		    {
		      fprintf (rsql_Output_fp, "Error on .hist command\n");
		    }
		}
	    }
	  else if (!strcasecmp (argument, "off"))
	    {
	      (void) histo_stop ();
	      rsql_Is_histo_on = HISTO_OFF;
	    }
	  else
	    {
	      fprintf (rsql_Output_fp, ".hist IS %s\n",
		       (rsql_Is_histo_on == HISTO_OFF ? "OFF" : "ON"));
	    }
	}
      else
	{
	  fprintf (rsql_Output_fp,
		   "Histogram is possible when the rsql started with "
		   "`communication_histogram=yes'\n");
	}
      break;

    case S_CMD_CLR_HISTO:
      if (histo_is_supported ())
	{
	  if (rsql_Is_histo_on == HISTO_ON)
	    {
	      histo_clear ();
	    }
	  else
	    {
	      fprintf (rsql_Output_fp, ".hist IS currently OFF\n");
	    }
	}
      else
	{
	  fprintf (rsql_Output_fp, "Histogram on execution statistics "
		   "is only allowed for the rsql started "
		   "with `communication_histogram=yes'\n");
	}
      break;

    case S_CMD_DUMP_HISTO:
      if (histo_is_supported ())
	{
	  if (rsql_Is_histo_on == HISTO_ON)
	    {
	      histo_print (rsql_Output_fp);
	      fprintf (rsql_Output_fp, "\n");
	    }
	  else
	    {
	      fprintf (rsql_Output_fp, ".hist IS currently OFF\n");
	    }
	}
      else
	{
	  fprintf (rsql_Output_fp, "Histogram on execution statistics "
		   "is only allowed for the rsql started "
		   "with `communication_histogram=yes'\n");
	}
      break;

    case S_CMD_DUMP_CLR_HISTO:
      if (histo_is_supported ())
	{
	  if (rsql_Is_histo_on == HISTO_ON)
	    {
	      histo_print (rsql_Output_fp);
	      histo_clear ();
	      fprintf (rsql_Output_fp, "\n");
	    }
	  else
	    {
	      fprintf (rsql_Output_fp, ".hist IS currently OFF\n");
	    }
	}
      else
	{
	  fprintf (rsql_Output_fp, "Histogram on execution statistics "
		   "is only allowed for the rsql started "
		   "with `communication_histogram=yes'\n");
	}
      break;

    case S_CMD_HISTORY_READ:
      if (rsql_Is_interactive)
	{
	  if (argument[0] != '\0')
	    {
	      int i = atoi (argument);
	      if (i > 0)
		{
		  HIST_ENTRY *hist;
		  hist = history_get (history_base + i - 1);
		  if (hist != NULL)
		    {
		      if (rsql_edit_contents_append (hist->line, true) !=
			  RSQL_SUCCESS)
			{
			  return DO_CMD_FAILURE;
			}
		    }
		  else
		    {
		      fprintf (rsql_Error_fp,
			       "ERROR: Invalid history number(%s).\n",
			       argument);
		    }
		}
	      else
		{
		  fprintf (rsql_Error_fp, "ERROR: Invalid history number\n");
		}
	    }
	  else
	    {
	      fprintf (rsql_Error_fp,
		       "ERROR: HISTORYRead {history_number}\n");
	    }
	}
      break;

    case S_CMD_HISTORY_LIST:
      if (rsql_Is_interactive)
	{
	  /* rewind history */
	  int i;

	  while (next_history ())
	    {
	      ;
	    }

	  for (i = 0, hist_entry = current_history (); hist_entry;
	       hist_entry = previous_history (), i++)
	    {
	      fprintf (rsql_Output_fp, "----< %d >----\n", i + 1);
	      fprintf (rsql_Output_fp, "%s\n\n", hist_entry->line);
	    }
	}
      break;
    case S_CMD_TRACE:
      if (rsql_arg->sa_mode == false)
	{
	  rsql_set_trace ((argument[0] == '\0') ? NULL : argument);
	}
      else
	{
	  fprintf (rsql_Error_fp, "Auto trace isn't allowed in SA mode.\n");
	}
      break;

      /* Command stuffs */
    case S_CMD_RUN:
    case S_CMD_XRUN:
      fprintf (rsql_Error_fp, "Invalid Command.\n");
      break;
    case S_CMD_QUERY:
    case S_CMD_UNKNOWN:
      assert (false);
      return DO_CMD_FAILURE;
    }

  return DO_CMD_SUCCESS;
}

/*
 * rsql_read_file() - read a file into command editor
 *   return: none
 *   file_name(in): input file name
 */
static void
rsql_read_file (const char *file_name)
{
  static char current_file[PATH_MAX] = "";
  char *p, *q;			/* pointer to string */
  FILE *fp = (FILE *) NULL;	/* file stream */

  p = rsql_get_real_path (file_name);	/* get real path name */

  if (p == NULL || p[0] == '\0')
    {
      /*
       * No filename given; use the last one we were given.  If we've
       * never received one before we have a genuine error.
       */
      if (current_file[0] != '\0')
	{
	  p = current_file;
	}
      else
	{
	  rsql_Error_code = RSQL_ERR_FILE_NAME_MISSED;
	  goto error;
	}
    }

  for (q = p; *q != '\0' && !iswspace ((wint_t) (*q)); q++)
    ;

  /* trim trailing blanks */
  for (; *q != '\0' && iswspace ((wint_t) (*q)); q++)
    {
      *q = '\0';
    }

  if (*q != '\0')
    {				/* contains more than one file name */
      rsql_Error_code = RSQL_ERR_TOO_MANY_FILE_NAMES;
      goto error;
    }

  fp = fopen (p, "r");
  if (fp == NULL)
    {
      rsql_Error_code = RSQL_ERR_OS_ERROR;
      goto error;
    }

  /*
   * We've successfully read the file, so remember its name for
   * subsequent reads.
   */
  strncpy (current_file, p, sizeof (current_file));

  if (rsql_edit_read_file (fp) == RSQL_FAILURE)
    {
      goto error;
    }

  fclose (fp);

  rsql_display_msg (rsql_get_message (RSQL_STAT_READ_DONE_TEXT));

  return;

error:
  if (fp != NULL)
    {
      fclose (fp);
    }
  nonscr_display_error ();
}

/*
 * rsql_write_file() - write (or append) the current content of editor into
 *                   user specified file
 *   return: none
 *   file_name(in): output file name
 *   append_flag(in): true if append
 */
static void
rsql_write_file (const char *file_name, int append_flag)
{
  static char current_file[PATH_MAX] = "";
  /* the name of the last file written */
  char *p, *q;			/* pointer to string */
  FILE *fp = (FILE *) NULL;	/* file stream */

  p = rsql_get_real_path (file_name);	/* get real path name */

  if (p == NULL || p[0] == '\0')
    {
      /*
       * No filename given; use the last one we were given.  If we've
       * never received one before we have a genuine error.
       */
      if (current_file[0] != '\0')
	p = current_file;
      else
	{
	  rsql_Error_code = RSQL_ERR_FILE_NAME_MISSED;
	  goto error;
	}
    }

  for (q = p; *q != '\0' && !iswspace ((wint_t) (*q)); q++)
    ;

  /* trim trailing blanks */
  for (; *q != '\0' && iswspace ((wint_t) (*q)); q++)
    {
      *q = '\0';
    }

  if (*q != '\0')
    {				/* contains more than one file name */
      rsql_Error_code = RSQL_ERR_TOO_MANY_FILE_NAMES;
      goto error;
    }

  fp = fopen (p, (append_flag) ? "a" : "w");
  if (fp == NULL)
    {
      rsql_Error_code = RSQL_ERR_OS_ERROR;
      goto error;
    }

  /*
   * We've successfully opened the file, so remember its name for
   * subsequent writes.
   */
  strncpy (current_file, p, sizeof (current_file));

  if (rsql_edit_write_file (fp) == RSQL_FAILURE)
    {
      goto error;
    }

  fclose (fp);

  rsql_display_msg (rsql_get_message (RSQL_STAT_EDITOR_SAVED_TEXT));

  return;

error:
  if (fp != NULL)
    {
      fclose (fp);
    }
  nonscr_display_error ();
}

/*
 * rsql_print_buffer()
 *   return: none
 *
 * Note:
 *   copy command editor buffer into temporary file and
 *   invoke the user preferred print command to print
 */
static void
rsql_print_buffer (void)
{
  char *cmd = NULL;
  char *fname = (char *) NULL;	/* pointer to temp file name */
  FILE *fp = (FILE *) NULL;	/* pointer to stream */

  /* create a temp file and open it */

  fname = tmpnam ((char *) NULL);
  if (fname == NULL)
    {
      rsql_Error_code = RSQL_ERR_OS_ERROR;
      goto error;
    }

  fp = fopen (fname, "w");
  if (fp == NULL)
    {
      rsql_Error_code = RSQL_ERR_OS_ERROR;
      goto error;
    }

  /* write the content of editor to the temp file */
  if (rsql_edit_write_file (fp) == RSQL_FAILURE)
    {
      goto error;
    }

  fclose (fp);
  fp = (FILE *) NULL;

  /* invoke the print command */
  cmd = rsql_get_tmp_buf (1 + strlen (rsql_Print_cmd) + 3 + strlen (fname));
  if (cmd == NULL)
    {
      goto error;
    }
  /*
   * Parenthesize the print command and supply its input through stdin,
   * just in case it's a pipe or something odd.
   */
  sprintf (cmd, "(%s) <%s", rsql_Print_cmd, fname);
  rsql_invoke_system (cmd);

  unlink (fname);

  rsql_display_msg (rsql_get_message (RSQL_STAT_EDITOR_PRINTED_TEXT));

  return;

error:
  if (fp != NULL)
    {
      fclose (fp);
    }
  if (fname != NULL)
    {
      unlink (fname);
    }
  nonscr_display_error ();
}

/*
 * rsql_change_working_directory()
 *   return: none
 *   dirname(in)
 *
 * Note:
 *   cd to the named directory; if dirname is NULL, cd to
 *   the home directory.
 */
static void
rsql_change_working_directory (const char *dirname)
{
  const char *msg;
  char buf[100 + PATH_MAX];

  msg = rsql_get_message (RSQL_STAT_CD_TEXT);

  dirname = rsql_get_real_path (dirname);

  if (dirname == NULL)
    {
      dirname = getenv ("HOME");
    }

  if (dirname == NULL || chdir (dirname) == -1)
    {
      rsql_Error_code = RSQL_ERR_OS_ERROR;
      nonscr_display_error ();
    }
  else
    {
      snprintf (buf, sizeof (buf) - 1, "\n%s %s.\n\n", msg, dirname);
      rsql_fputs_console_conv (buf, rsql_Tty_fp);
    }
}

/*
 * display_error()
 *   return: none
 *   session(in)
 *   stmt_start_line_no(in)
 */
static void
display_error (DB_SESSION * session, int stmt_start_line_no)
{
#if 1				/* TODO - #955 set PERSIST */
#if defined (CS_MODE)
  if (db_Connect_status != DB_CONNECTION_STATUS_CONNECTED)
    {
      rsql_Error_code = RSQL_ERR_SQL_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_NO_CONNECT, 0);
      PT_ERRORc (session->parser, NULL, db_error_string (3));
    }
#endif
#endif

  if (rsql_Error_code == RSQL_ERR_SQL_ERROR)
    {
      rsql_display_session_err (session, stmt_start_line_no);
      rsql_check_server_down ();
    }
  else
    {
      nonscr_display_error ();

      /* let users read this message before the next overwrites */
      sleep (3);
    }
}

/*
 * rsql_execute_statements() - execute statements
 *   return: >0 if some statement failed, zero otherwise
 *   rsql_arg(in)
 *   type(in)
 *   stream(in)
 *   line_no(in): starting line no of each stmt
 *
 * Note:
 *   If `type' is STRING_INPUT, it regards `stream' points to command string.
 *   If `type' is FILE_INPUT, it regards `stream' points FILE stream of input
 *   If `type' is EDITOR_INPUT, it attempts to get input string from command
 *   buffer.
 */
static int
rsql_execute_statements (const RSQL_ARGUMENT * rsql_arg,
			 char *stream, int stream_length, int line_no)
{
  int num_stmts = 0;		/* # of stmts executed */
  DB_SESSION *session = NULL;	/* query compilation session id */
  DB_QUERY_TYPE *attr_spec = NULL;	/* result attribute spec. */
  int total;			/* number of statements to execute */
  bool do_abort_transaction = false;	/* flag for transaction abort */
//  char save_end;

  rsql_Is_sigint_caught = false;

  rsql_Num_failures = 0;
  er_clear ();
#if 1				/* TODO - #955 set PERSIST */
  if (db_Connect_status != DB_CONNECTION_STATUS_NOT_CONNECTED)
    {
      db_set_interrupt (0);
    }
#else
  db_set_interrupt (0);
#endif

//  save_end = stream[stream_length];
  stream[stream_length] = '\0';
  session = db_open_buffer (stream);
  if (!session)
    {
      rsql_Error_code = RSQL_ERR_SQL_ERROR;
      goto error;
    }

  /*
   * Make sure that there weren't any syntax errors; if there were, the
   * entire concept of "compile next statement" doesn't make sense, and
   * you run the risk of getting stuck in an infinite loop in the
   * following section (especially if the '-e' switch is on).
   */
  if (db_get_errors (session))
    {
      rsql_Error_code = RSQL_ERR_SQL_ERROR;
      if (rsql_Is_interactive)
	{
	  add_history (stream);
	}
      goto error;
    }
  else
    {
      total = db_statement_count (session);
      if (rsql_Is_interactive)
	{
	  add_history (stream);
	}

      /* It is assumed we must always enter the for loop below */
      total = MAX (total, 1);
    }

  /* execute the statements one-by-one */

  for (num_stmts = 0; num_stmts < total; num_stmts++)
    {
      struct timeval start_time, end_time, elapsed_time;
      RYE_STMT_TYPE stmt_type;	/* statement type */
      DB_QUERY_RESULT *result = NULL;	/* result pointer */
      int db_error;
      char stmt_msg[LINE_BUFFER_SIZE];
      int n, avail_size = sizeof (stmt_msg);

      /* Start the execution of stms */
      stmt_msg[0] = '\0';

      if (rsql_arg->time_on)
	{
	  (void) gettimeofday (&start_time, NULL);
	}

      if (db_compile_statement (session) != NO_ERROR)
	{
	  /*
	   * Transaction should be aborted if an error occurs during
	   * compilation on auto commit mode.
	   */
	  if (rsql_arg->auto_commit
	      && prm_get_bool_value (PRM_ID_RSQL_AUTO_COMMIT))
	    {
	      do_abort_transaction = true;
	    }

	  /* compilation error */
	  rsql_Error_code = RSQL_ERR_SQL_ERROR;
	  /* Do not continue if there are no statments in the buffer */
	  if (rsql_arg->continue_on_error
	      && (db_error_code () != ER_IT_EMPTY_STATEMENT))
	    {
	      display_error (session, 0);
	      /* do_abort_transaction() should be called after display_error()
	       * because in some cases it deallocates the parser containing
	       * the error message
	       */
	      if (do_abort_transaction)
		{
		  db_abort_transaction ();
		  do_abort_transaction = false;
		}
	      rsql_Num_failures += 1;
	      continue;
	    }
	  else
	    {
	      goto error;
	    }
	}

      attr_spec = db_get_query_type_list (session);
      stmt_type = (RYE_STMT_TYPE) db_get_statement_type (session);

      db_session_set_groupid (session, rsql_arg->groupid);
      db_error = db_execute_and_keep_statement (session, &result);

      if (db_error < 0)
	{
	  rsql_Error_code = RSQL_ERR_SQL_ERROR;
	  if (rsql_arg->auto_commit
	      && prm_get_bool_value (PRM_ID_RSQL_AUTO_COMMIT)
	      && stmt_type != RYE_STMT_ROLLBACK_WORK)
	    {
	      do_abort_transaction = true;
	    }

	  if (rsql_arg->continue_on_error)
	    {
	      display_error (session, line_no);
	      if (do_abort_transaction)
		{
		  db_abort_transaction ();
		  do_abort_transaction = false;
		}
	      rsql_Num_failures += 1;

	      free_attr_spec (&attr_spec);

	      continue;
	    }
	  goto error;
	}

      n = snprintf (stmt_msg, LINE_BUFFER_SIZE, "Execute OK.");
      n = MIN (MAX (n, 0), avail_size - 1);
      avail_size -= n;

      rsql_Row_count = 0;
      switch (stmt_type)
	{
	case RYE_STMT_SELECT:
	  {
//          const char *msg_p;

	    rsql_results (rsql_arg, result, attr_spec, line_no, stmt_type);

	    rsql_Row_count = db_error;

#if 0
	    msg_p = ((rsql_Row_count > 1)
		     ? rsql_get_message (RSQL_ROWS)
		     : rsql_get_message (RSQL_ROW));
#endif
	    n = snprintf (stmt_msg, LINE_BUFFER_SIZE,
			  rsql_get_message (RSQL_ROWS), rsql_Row_count,
			  "selected");
	    n = MIN (MAX (n, 0), avail_size - 1);
	    avail_size -= n;
	    break;
	  }

	case RYE_STMT_GET_ISO_LVL:
	case RYE_STMT_GET_TIMEOUT:
	case RYE_STMT_GET_OPT_LVL:
	  if (result != NULL)
	    {
	      rsql_results (rsql_arg, result, db_get_query_type_ptr (result),
			    line_no, stmt_type);
	    }
	  break;

	case RYE_STMT_UPDATE:
	case RYE_STMT_DELETE:
	case RYE_STMT_INSERT:
	  {
	    const char *msg_p;

	    msg_p = ((db_error > 1)
		     ? rsql_get_message (RSQL_ROWS)
		     : rsql_get_message (RSQL_ROW));
	    n = snprintf (stmt_msg, LINE_BUFFER_SIZE, msg_p, db_error,
			  "affected");
	    n = MIN (MAX (n, 0), avail_size - 1);
	    avail_size -= n;
	    break;
	  }

	default:
	  break;
	}

      free_attr_spec (&attr_spec);

      if (result != NULL)
	{
	  db_query_end (result);
	  result = NULL;
	}
      else
	{
	  /*
	   * Even though there are no results, a query may have been
	   * run implicitly by the statement.  If so, we need to end the
	   * query on the server.
	   */
	  db_free_query (session);
	}

      if (rsql_arg->time_on)
	{
	  char time[100];

	  (void) gettimeofday (&end_time, NULL);

	  elapsed_time.tv_sec = end_time.tv_sec - start_time.tv_sec;
	  elapsed_time.tv_usec = end_time.tv_usec - start_time.tv_usec;
	  if (elapsed_time.tv_usec < 0)
	    {
	      elapsed_time.tv_sec--;
	      elapsed_time.tv_usec += 1000000;
	    }
	  n = sprintf (time, " (%ld.%06ld sec) ",
		       elapsed_time.tv_sec, elapsed_time.tv_usec);
	  n = MIN (MAX (n, 0), avail_size - 1);
	  avail_size -= n;
	  strncat (stmt_msg, time, n);
	}

      if (rsql_arg->auto_commit
	  && prm_get_bool_value (PRM_ID_RSQL_AUTO_COMMIT)
	  && stmt_type != RYE_STMT_COMMIT_WORK
	  && stmt_type != RYE_STMT_ROLLBACK_WORK)
	{
#if 1				/* TODO - #955 set PERSIST */
	  if (db_Connect_status != DB_CONNECTION_STATUS_NOT_CONNECTED)
	    {
#endif
	      db_error = db_commit_transaction ();
	      if (db_error < 0)
		{
		  rsql_Error_code = RSQL_ERR_SQL_ERROR;
		  do_abort_transaction = true;

		  if (rsql_arg->continue_on_error)
		    {
		      display_error (session, line_no);
		      if (do_abort_transaction)
			{
			  db_abort_transaction ();
			  do_abort_transaction = false;
			}
		      rsql_Num_failures += 1;
		      continue;
		    }
		  goto error;
		}
	      else
		{
		  strncat (stmt_msg,
			   rsql_get_message (RSQL_STAT_COMMITTED_TEXT),
			   avail_size - 1);
		}
#if 1				/* TODO - #955 set PERSIST */
	    }
#endif
	}
      fprintf (rsql_Output_fp, "%s\n", stmt_msg);
      db_drop_statement (session);
    }

  snprintf (rsql_Scratch_text, SCRATCH_TEXT_LEN,
	    rsql_get_message (RSQL_EXECUTE_END_MSG_FORMAT),
	    num_stmts - rsql_Num_failures);
  rsql_display_msg (rsql_Scratch_text);

  db_close_session (session);

  if (rsql_Query_trace == true)
    {
      rsql_display_trace (rsql_arg);
    }

  if (rsql_arg->auto_commit && prm_get_bool_value (PRM_ID_RSQL_AUTO_COMMIT))
    {
      if (db_Connect_status != DB_CONNECTION_STATUS_NOT_CONNECTED)
	{
	  db_abort_transaction ();
	  do_abort_transaction = false;
	}
    }

  return rsql_Num_failures;

error:

  display_error (session, line_no);

  /* Finish... */
  snprintf (rsql_Scratch_text, SCRATCH_TEXT_LEN,
	    rsql_get_message (RSQL_EXECUTE_END_MSG_FORMAT),
	    num_stmts - rsql_Num_failures);
  rsql_display_msg (rsql_Scratch_text);

  if (session)
    {
      db_close_session (session);
    }

  free_attr_spec (&attr_spec);

  if (do_abort_transaction)
    {
      db_abort_transaction ();
      do_abort_transaction = false;
    }

  return 1;
}

/*
 * free_attr_spec()
 *   return: none
 *   attr_spec(in/out)
 *
 * Note: Free memory alloced for attr_spec and set pointer to NULL
 */
static void
free_attr_spec (DB_QUERY_TYPE ** attr_spec)
{
  if (*attr_spec != NULL)
    {
      db_query_format_free (*attr_spec);
      *attr_spec = NULL;
    }
}

/*
 * rsql_print_database()
 */
static void
rsql_print_database (void)
{
  char *db_name;
  PRM_NODE_INFO connected_node;

  db_name = db_get_database_name ();
  connected_node = boot_get_host_connected ();

  if (db_name == NULL ||
      PRM_NODE_INFO_GET_IP (&connected_node) == INADDR_NONE)
    {
      fprintf (rsql_Error_fp, "\n\tNOT CONNECTED\n\n");
    }
  else
    {
      char *pstr;
      char converted_host_name[MAXHOSTNAMELEN + 1];
      const char *ha_state;

      prm_node_info_to_str (converted_host_name, sizeof (converted_host_name),
			    &connected_node);

      pstr = strchr (db_name, '@');
      if (pstr != NULL)
	{
	  *pstr = '\0';
	}

      ha_state = HA_STATE_NAME (db_get_server_state ());
      fprintf (rsql_Output_fp, "\n\t%s@%s [%s]\n\n", db_name,
	       converted_host_name, ha_state);
    }

  db_ws_free (db_name);
}

/*
 * rsql_set_sys_param()
 *   return: none
 *   arg_str(in)
 *
 * Note: Parse the arg string to find out what system parameter to
 *       clobber, then clobber it.  Originally introduced to allow us
 *       to fiddle with optimizer parameters.
 */
static void
rsql_set_sys_param (const char *arg_str)
{
  char plantype[128];
  char val[128];
  char ans[4096];
  char names[4096];
  int level;
  bool persist = false;
  const int len = sizeof (ans);
  int error = NO_ERROR;

  if (arg_str == NULL)
    {
      return;
    }

  if (strncmp (arg_str, "cost", 4) == 0
      && sscanf (arg_str, "cost %127s %127s", plantype, val) == 2)
    {
      if (qo_plan_set_cost_fn (plantype, val[0]))
	{
	  snprintf (ans, 128, "cost %s: %s", plantype, val);
	}
      else
	{
	  snprintf (ans, 128, "error: unknown cost parameter %s", plantype);
	}
    }
  else if (strncmp (arg_str, "level", 5) == 0
	   && sscanf (arg_str, "level %d", &level) == 1)
    {
      qo_set_optimization_param (NULL, QO_PARAM_LEVEL, level);
      snprintf (ans, 128, "level %d", level);
    }
  else
    {
      if (strncmp (arg_str, "persist", 7) == 0
	  && sscanf (arg_str, "persist %4095s", ans) == 1)
	{
	  persist = true;
	}
      else
	{
	  strncpy (ans, arg_str, len - 1);
	  assert (persist == false);
	}

      error = db_set_system_parameters (names, sizeof (names), ans, persist);
      (void) db_get_system_parameters (names, sizeof (names));
      snprintf (ans, len - 1, "%s%s",
		(error != NO_ERROR ? "error: set " : ""), names);
    }
  ans[len - 1] = '\0';

  rsql_append_more_line (0, ans);
  rsql_display_more_lines ("Set Param Input");
  rsql_free_more_lines ();
}

/*
 * rsql_get_sys_param()
 *   return:
 *   arg_str(in)
 */
static void
rsql_get_sys_param (const char *arg_str)
{
  char plantype[128];
  int cost;
  char ans[4096];
  char names[4096];
  int level;
  const int len = sizeof (ans);
  int error = NO_ERROR;

  if (arg_str == NULL)
    {
      return;
    }

  if (strncmp (arg_str, "cost", 4) == 0
      && sscanf (arg_str, "cost %127s", plantype) == 1)
    {
      cost = qo_plan_get_cost_fn (plantype);
      if (cost == 'u')
	{
	  snprintf (ans, len, "error: unknown cost parameter %s", arg_str);
	}
      else
	{
	  snprintf (ans, len, "cost %s: %c", arg_str, (char) cost);
	}
    }
  else if (strncmp (arg_str, "level", 5) == 0
	   && sscanf (arg_str, "level") == 0)
    {
      qo_get_optimization_param (&level, QO_PARAM_LEVEL);
      snprintf (ans, len, "level %d", level);
    }
  else
    {
      strncpy (names, arg_str, len - 1);
      error = db_get_system_parameters (names, sizeof (names));
      snprintf (ans, len - 1, "%s%s",
		(error != NO_ERROR ? "error: get " : ""), names);
    }
  ans[len - 1] = '\0';

  rsql_append_more_line (0, ans);
  rsql_display_more_lines ("Get Param Input");
  rsql_free_more_lines ();
}

/*
 * rsql_set_plan_dump()
 *   return:
 *   arg_str(in)
 */
static void
rsql_set_plan_dump (const char *arg_str)
{
  int level;
  char line[128];

  qo_get_optimization_param (&level, QO_PARAM_LEVEL);

  if (arg_str != NULL)
    {
      if (!strncmp (arg_str, "simple", 6))
	{
	  level &= ~0x200;
	  level |= 0x100;
	  qo_set_optimization_param (NULL, QO_PARAM_LEVEL, level);
	}
      else if (!strncmp (arg_str, "detail", 6))
	{
	  level &= ~0x100;
	  level |= 0x200;
	  qo_set_optimization_param (NULL, QO_PARAM_LEVEL, level);
	}
      else if (!strncmp (arg_str, "off", 3))
	{
	  level &= ~(0x100 | 0x200);
	  qo_set_optimization_param (NULL, QO_PARAM_LEVEL, level);
	}
    }

  if (PLAN_DUMP_ENABLED (level))
    {
      if (SIMPLE_DUMP (level))
	{
	  snprintf (line, 128, "plan simple (opt level %d)", level);
	}
      if (DETAILED_DUMP (level))
	{
	  snprintf (line, 128, "plan detail (opt level %d)", level);
	}
    }
  else
    {
      snprintf (line, 128, "plan off (opt level %d)", level);
    }

  rsql_append_more_line (0, line);
  rsql_display_more_lines ("Plan Dump");
  rsql_free_more_lines ();
}

/*
 * signal_intr() - Interrupt handler for rsql
 *   return: none
 *   sig_no(in)
 */
static void
signal_intr (UNUSED_ARG int sig_no)
{
  if (rsql_Is_interactive)
    {
      db_set_interrupt (1);
    }
  rsql_Is_sigint_caught = true;
}

static bool
check_client_alive ()
{
  if (rsql_Is_sigint_caught == true)
    {
      return false;
    }

  return true;
}

/*
 * rsql_exit_session() - handling the default action of the last outstanding
 *                     transaction (i.e., commit or abort)
 *   return:  none
 *   error(in): EXIT_FAILURE or EXIT_SUCCESS
 *
 * Note: this function never return.
 */
static void
rsql_exit_session (int error)
{
  char line_buf[LINE_BUFFER_SIZE];
  bool commit_on_shutdown = false;
  bool prm_commit_on_shutdown = prm_get_commit_on_shutdown ();

  free_rsql_column_width_info_list ();

  if (!db_commit_is_needed ())
    {
      /* when select statements exist only in session,
         marks end of transaction to flush audit records
         for those statements */
      db_abort_transaction ();
    }

  if (rsql_Is_interactive && !prm_commit_on_shutdown
      && db_commit_is_needed () && !feof (rsql_Input_fp))
    {
      FILE *tf;

      tf = rsql_Error_fp;

      /* interactive, default action is abort but there was update */
      fprintf (tf, rsql_get_message (RSQL_TRANS_TERMINATE_PROMPT_TEXT));
      fflush (tf);
      for (; fgets (line_buf, LINE_BUFFER_SIZE, rsql_Input_fp) != NULL;)
	{
	  if (line_buf[0] == 'y' || line_buf[0] == 'Y')
	    {
	      commit_on_shutdown = true;
	      break;
	    }
	  if (line_buf[0] == 'n' || line_buf[0] == 'N')
	    {
	      commit_on_shutdown = false;
	      break;
	    }

	  fprintf (tf,
		   rsql_get_message (RSQL_TRANS_TERMINATE_PROMPT_RETRY_TEXT));
	  fflush (tf);
	}

      if (commit_on_shutdown && db_commit_transaction () < 0)
	{
	  nonscr_display_error ();
	  error = EXIT_FAILURE;
	}
    }

  if (histo_is_supported ())
    {
      if (rsql_Is_histo_on != HISTO_OFF)
	{
	  rsql_Is_histo_on = HISTO_OFF;
	  histo_stop ();
	}
    }
  db_end_session ();

  if (db_shutdown () < 0)
    {
      rsql_Database_connected = false;
      nonscr_display_error ();
      rsql_exit (EXIT_FAILURE);
    }
  else
    {
      rsql_Database_connected = false;
      rsql_exit (error);
    }
}

/*
 * rsql_exit_init()
 *   return: none
 *
 * Note:
 *    Initialize various state variables we keep to let us know what
 *    cleanup operations need to be performed when the rsql() function
 *    exits.  This should properly initialize everything that is tested
 *    by the rsql_exit_cleanup function.
 */
static void
rsql_exit_init (void)
{
  rsql_Exit_status = EXIT_SUCCESS;
  rsql_Database_connected = false;

  rsql_Input_fp = stdin;
  rsql_Output_fp = stdout;
  rsql_Error_fp = stderr;
}

/*
 * rsql_exit_cleanup()
 *   return: none
 *
 * Note:
 *    Called by rsql() when the exit longjmp has been taken.
 *    Examine the various state variables we keep and perform any
 *    termination cleanup operations that need to be performed.
 *    For the Windows implementation, it is especially important that the
 *    rsql() function return cleanly.
 */
static void
rsql_exit_cleanup ()
{
  FILE *oldout;

  if (rsql_Input_fp != NULL && rsql_Input_fp != stdin)
    {
      (void) fclose (rsql_Input_fp);
      rsql_Input_fp = NULL;
    }

  oldout = rsql_Output_fp;
  if (rsql_Output_fp != NULL && rsql_Output_fp != stdout)
    {
      (void) fclose (rsql_Output_fp);
      rsql_Output_fp = NULL;
    }

  if (rsql_Error_fp != NULL && rsql_Error_fp != oldout
      && rsql_Error_fp != stdout && rsql_Error_fp != stderr)
    {
      (void) fclose (rsql_Error_fp);
      rsql_Error_fp = NULL;
    }

  if (rsql_Database_connected)
    {
      if (histo_is_supported ())
	{
	  if (rsql_Is_histo_on != HISTO_OFF)
	    {
	      rsql_Is_histo_on = HISTO_OFF;
	      histo_stop ();
	    }
	}

      rsql_Database_connected = false;
      db_end_session ();
      db_shutdown ();
    }

#if 0
  /* Note that this closes a global resource, the "kernel" message catalog.
   * This is ok for the Unix implementation as the entire process is about
   * to exit.  For the Windows implementation, it happens to be ok since
   * the test driver application that calls rsql() won't use this catalog.
   * If this ever changes however, we'll probably have to maintain some sort
   * of internal reference counter on this catalog so that it won't be freed
   * until all the nested users close it.
   */
  lang_final ();
#endif
}

/*
 * rsql_exit()
 *   return:  none
 *   exit_status(in)
 * Note:
 *    This should be called rather than exit() any place that the code wants
 *    to terminate the rsql interpreter program.  Rather than exit(), it
 *    will longjmp back to the rsql() function which will clean up and
 *    return the status code to the calling function.  Usually the calling
 *    function is main() but under Windows, the caller may be a more complex
 *    application.
 */
void
rsql_exit (int exit_status)
{
  rsql_Exit_status = exit_status;
  longjmp (rsql_Exit_env, 1);
}

/*
 * rsql() - "main" interface function for the rsql interpreter
 *   return: EXIT_SUCCESS, EXIT_FAILURE
 *   rsql_arg(in)
 */
int
rsql (const char *argv0, RSQL_ARGUMENT * rsql_arg)
{
  char *env;
  int client_type;
  int avail_size;
  int err_code;

  /* Establish a globaly accessible longjmp environment so we can terminate
   * on severe errors without calling exit(). */
  rsql_exit_init ();

  if (setjmp (rsql_Exit_env))
    {
      /* perform any dangling cleanup operations */
      rsql_exit_cleanup ();
      return rsql_Exit_status;
    }

  /* initialize message catalog for argument parsing and usage() */
  if (utility_initialize () != NO_ERROR)
    {
      rsql_exit (EXIT_FAILURE);
    }

  /* set up prompt and message fields. */
  if (rsql_arg->user_name != NULL
      && strcasecmp (rsql_arg->user_name, "DBA") == 0)
    {
      strncpy (rsql_Prompt, rsql_get_message (RSQL_ADMIN_PROMPT),
	       sizeof (rsql_Prompt));
    }
  else
    {
      strncpy (rsql_Prompt, rsql_get_message (RSQL_PROMPT),
	       sizeof (rsql_Prompt));
    }
  rsql_Prompt[sizeof (rsql_Prompt) - 1] = '\0';

  avail_size = sizeof (rsql_Prompt) - strlen (rsql_Prompt) - 1;
  if (avail_size > 0)
    {
      strncat (rsql_Prompt, " ", avail_size);
    }
  STRNCPY (rsql_Name, rsql_get_message (RSQL_NAME), sizeof (rsql_Name));

  /* it is necessary to be opening rsql_Input_fp at this point
   */
  if (rsql_arg->in_file_name != NULL)
    {
      rsql_Input_fp = fopen (rsql_arg->in_file_name, "r");
      if (rsql_Input_fp == NULL)
	{
	  rsql_Error_code = RSQL_ERR_OS_ERROR;
	  goto error;
	}
    }

  if ((rsql_arg->in_file_name == NULL) && isatty (fileno (stdin)))
    {
      rsql_Is_interactive = true;
    }

  /* initialize error log file */
  if (er_init ("./rsql.err", ER_EXIT_DEFAULT) != NO_ERROR)
    {
      printf ("Failed to initialize error manager.\n");
      rsql_Error_code = RSQL_ERR_OS_ERROR;
      goto error;
    }

  if (lang_init () != NO_ERROR)
    {
      printf ("Failed to initialize lang manager.\n");
      rsql_Error_code = RSQL_ERR_OS_ERROR;
      goto error;
    }

  /*
   * login and restart database
   */
  if (rsql_arg->user_name != NULL
      && strcasecmp (rsql_arg->user_name, "DBA") == 0)
    {
      client_type = BOOT_CLIENT_ADMIN_RSQL;

      if (rsql_arg->write_on_standby)
	{
	  client_type = BOOT_CLIENT_ADMIN_RSQL_WOS;
	}
    }
  else
    {
      client_type = BOOT_CLIENT_RSQL;
    }

  if (rsql_arg->db_name != NULL)
    {
      err_code = db_restart_ex (argv0, rsql_arg->db_name,
				rsql_arg->user_name, rsql_arg->passwd,
				client_type);
      if (err_code != NO_ERROR)
	{
	  if (!rsql_Is_interactive || rsql_arg->passwd != NULL ||
	      err_code != ER_AU_INVALID_PASSWORD)
	    {
	      /* not INTERACTIVE mode, or password is given already, or
	       * the error code is not password related
	       */
	      rsql_Error_code = RSQL_ERR_SQL_ERROR;
	      goto error;
	    }

	  /* get password interactively if interactive mode */
	  rsql_arg->passwd =
	    getpass (rsql_get_message (RSQL_PASSWD_PROMPT_TEXT));
	  if (rsql_arg->passwd[0] == '\0')
	    rsql_arg->passwd = (char *) NULL;	/* to fit into db_login protocol */

	  /* try again */
	  err_code = db_restart_ex (argv0, rsql_arg->db_name,
				    rsql_arg->user_name, rsql_arg->passwd,
				    client_type);
	  if (err_code != NO_ERROR)
	    {
	      rsql_Error_code = RSQL_ERR_SQL_ERROR;
	      goto error;
	    }
	}

      assert (Au_user != NULL);

      if (au_is_dba_group_member (Au_user))
	{
	  au_disable ();
	}
    }

  /* allow environmental setting of the "-s" command line flag
   * to enable automated testing */
  if (prm_get_bool_value (PRM_ID_RSQL_SINGLE_LINE_MODE))
    {
      rsql_arg->single_line_execution = true;
    }

  /* record the connection so we know how to clean up on exit */
  rsql_Database_connected = true;

  rsql_Editor_cmd[PATH_MAX - 1] = '\0';
  rsql_Shell_cmd[PATH_MAX - 1] = '\0';
  rsql_Print_cmd[PATH_MAX - 1] = '\0';
  rsql_Pager_cmd[PATH_MAX - 1] = '\0';

  env = getenv ("EDITOR");
  if (env)
    {
      strncpy (rsql_Editor_cmd, env, PATH_MAX - 1);
    }

  env = getenv ("SHELL");
  if (env)
    {
      strncpy (rsql_Shell_cmd, env, PATH_MAX - 1);
    }

  if (rsql_arg->nopager)
    {
      rsql_Pager_cmd[0] = '\0';
    }

  /* initialize language parameters
   */
  if (lang_init () != NO_ERROR)
    {
      if (er_errid () == NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOC_INIT, 1,
		  "Failed to initialize language module");
	}

      rsql_Error_code = RSQL_ERR_LOC_INIT;
      goto error;
    }

  if (lang_set_charset_lang () != NO_ERROR)
    {
      rsql_Error_code = RSQL_ERR_LOC_INIT;
      goto error;
    }

  lang_init_console_txt_conv ();

  if (rsql_Is_interactive)
    {
      /* handling Ctrl-C */
      if (os_set_signal_handler (SIGINT, signal_intr) == SIG_ERR)
	{
	  rsql_Error_code = RSQL_ERR_OS_ERROR;
	  goto error;
	}

      if (os_set_signal_handler (SIGQUIT, signal_intr) == SIG_ERR)
	{
	  rsql_Error_code = RSQL_ERR_OS_ERROR;
	  goto error;
	}
    }

  css_register_check_client_alive_fn (check_client_alive);

  start_rsql (rsql_arg);

  rsql_exit (EXIT_SUCCESS);	/* not reachable code, actually */

error:
  nonscr_display_error ();
  rsql_exit (EXIT_FAILURE);
  return EXIT_FAILURE;		/* won't get here really */
}

/*
 * rsql_get_message() - get a string of the rsql-utility from the catalog
 *   return: message string
 *   message_index(in): an index of the message string
 */
const char *
rsql_get_message (int message_index)
{
  return (msgcat_message (MSGCAT_CATALOG_RSQL,
			  MSGCAT_RSQL_SET_RSQL, message_index));
}

/*
 * rsql_set_column_width_info() - insert column_name and column_width
 *                                in rsql_column_width_info_list
 *   return: int
 *   column_name(in): column_name
 *   column_width(in): column_width
 */
int
rsql_set_column_width_info (const char *column_name, int column_width)
{
  RSQL_COLUMN_WIDTH_INFO *temp_list;
  char *temp_name;
  int i, index;

  if (column_name == NULL || column_width < 0)
    {
      rsql_Error_code = RSQL_ERR_INVALID_ARG_COMBINATION;

      return RSQL_FAILURE;
    }

  if (rsql_column_width_info_list == NULL)
    {
      if (initialize_rsql_column_width_info_list () != RSQL_SUCCESS)
	{
	  return RSQL_FAILURE;
	}
    }

  if (rsql_column_width_info_list_index >= rsql_column_width_info_list_size)
    {
      temp_list = realloc (rsql_column_width_info_list,
			   sizeof (RSQL_COLUMN_WIDTH_INFO)
			   * (rsql_column_width_info_list_size * 2));
      if (temp_list == NULL)
	{
	  rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;

	  return RSQL_FAILURE;
	}

      rsql_column_width_info_list_size *= 2;
      rsql_column_width_info_list = temp_list;
      for (i = rsql_column_width_info_list_index;
	   i < rsql_column_width_info_list_size; i++)
	{
	  rsql_column_width_info_list[i].name = NULL;
	  rsql_column_width_info_list[i].width = 0;
	}
    }

  index = NOT_FOUND;
  for (i = 0; i < rsql_column_width_info_list_index; i++)
    {
      if (strcasecmp (column_name, rsql_column_width_info_list[i].name) == 0)
	{
	  index = i;
	  break;
	}
    }

  if (index == NOT_FOUND)
    {
      index = rsql_column_width_info_list_index;
      rsql_column_width_info_list_index++;
    }

  if (rsql_column_width_info_list[index].name == NULL)
    {
      temp_name = strdup (column_name);
      if (temp_name == NULL)
	{
	  rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;

	  return RSQL_FAILURE;
	}

      rsql_column_width_info_list[index].name = temp_name;
      rsql_column_width_info_list[index].width = column_width;
    }
  else
    {
      rsql_column_width_info_list[index].width = column_width;
    }

  return RSQL_SUCCESS;
}

/*
 * rsql_get_column_width() - get column_width related column_name
 *   return: column_width
 *   column_name(in): column_name
 */
int
rsql_get_column_width (const char *column_name)
{
  char name_without_space[1024];
  char *result;
  int i;

  if (column_name == NULL)
    {
      return 0;
    }

  if (rsql_column_width_info_list == NULL)
    {
      return 0;
    }

  strncpy (name_without_space, column_name, sizeof (name_without_space) - 1);
  name_without_space[sizeof (name_without_space) - 1] = '\0';
  result = trim (name_without_space);
  if (result == NULL)
    {
      return 0;
    }

  for (i = 0; i < rsql_column_width_info_list_index; i++)
    {
      if (strcasecmp (result, rsql_column_width_info_list[i].name) == 0)
	{
	  return rsql_column_width_info_list[i].width;
	}
    }

  return 0;
}

/*
 * get_column_name_argument() - get column_name and value pointer from argument
 *   return: int
 *   column_name(out): column name
 *   val_str(out): value string in argument
 *   argument(in): argument
 */
static int
get_column_name_argument (char **column_name, char **val_str, char *argument)
{
  char *p;

  assert (column_name != NULL && val_str != NULL && argument != NULL);

  *column_name = NULL;
  *val_str = NULL;

  /* argument : "column_name=value" */
  *column_name = argument;

  p = strrchr (*column_name, '=');
  if (p != NULL)
    {
      *p = '\0';
      *val_str = (p + 1);
    }

  trim (*column_name);

  /* max column_name size is 254 */
  if (strlen (*column_name) > 254)
    {
      return RSQL_FAILURE;
    }

  return RSQL_SUCCESS;
}

/*
 * rsql_set_trace() - set auto trace on or off
 *   return:
 *   arg_str(in):
 */
static void
rsql_set_trace (const char *arg_str)
{
  char line[128];
  char format[128], *p;

  if (arg_str != NULL)
    {
      if (strncmp (arg_str, "on", 2) == 0)
	{
	  prm_set_bool_value (PRM_ID_QUERY_TRACE, true);
	  rsql_Query_trace = true;

	  if (sscanf (arg_str, "on %127s", format) == 1)
	    {
	      p = trim (format);

	      if (strncmp (p, "text", 4) == 0)
		{
		  prm_set_integer_value (PRM_ID_QUERY_TRACE_FORMAT,
					 QUERY_TRACE_TEXT);
		}
	      else if (strncmp (p, "json", 4) == 0)
		{
		  prm_set_integer_value (PRM_ID_QUERY_TRACE_FORMAT,
					 QUERY_TRACE_JSON);
		}
	    }
	}
      else if (!strncmp (arg_str, "off", 3))
	{
	  prm_set_bool_value (PRM_ID_QUERY_TRACE, false);
	  rsql_Query_trace = false;
	}
    }

  if (prm_get_bool_value (PRM_ID_QUERY_TRACE) == true)
    {
      if (prm_get_integer_value (PRM_ID_QUERY_TRACE_FORMAT)
	  == QUERY_TRACE_JSON)
	{
	  snprintf (line, 128, "trace on json");
	}
      else
	{
	  snprintf (line, 128, "trace on text");
	}
    }
  else
    {
      snprintf (line, 128, "trace off");
    }

  rsql_append_more_line (0, line);
  rsql_display_more_lines ("Query Trace");
  rsql_free_more_lines ();
}

/*
 * rsql_display_trace() -
 *   return:
 *   rsql_arg(in)
 */
static void
rsql_display_trace (const RSQL_ARGUMENT * rsql_arg)
{
  const char *stmts = NULL;
  DB_SESSION *session = NULL;
  int db_error;
  DB_QUERY_RESULT *result = NULL;
  DB_VALUE trace;
  FILE *pf;

  er_clear ();
  db_set_interrupt (0);
  db_make_null (&trace);

  stmts = "SHOW TRACE";

  session = db_open_buffer (stmts);
  if (session == NULL)
    {
      return;
    }

  if (db_compile_statement (session) != NO_ERROR)
    {
      goto end;
    }

  db_session_set_groupid (session, rsql_arg->groupid);
  db_error = db_execute_and_keep_statement (session, &result);

  if (db_error < 0)
    {
      goto end;
    }

  (void) db_query_set_copy_tplvalue (result, 0 /* peek */ );

  if (db_query_first_tuple (result) < 0)
    {
      goto end;
    }

  if (db_query_get_tuple_value (result, 0, &trace) < 0)
    {
      goto end;
    }

  if (DB_VALUE_TYPE (&trace) == DB_TYPE_VARCHAR)
    {
      pf = rsql_popen (rsql_Pager_cmd, rsql_Output_fp);
      fprintf (pf, "\n=== Auto Trace ===\n");
      fprintf (pf, "%s\n", db_get_string (&trace));
      rsql_pclose (pf, rsql_Output_fp);
    }

end:

  if (result != NULL)
    {
      db_query_end (result);
    }

  if (session != NULL)
    {
      db_close_session (session);
    }

  return;
}

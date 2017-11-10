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
 * rsql_support.c : Utilities for rsql module
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdarg.h>
#include <signal.h>
#include <pwd.h>
#include "porting.h"
#include "rsql.h"
#include "memory_alloc.h"
#include "system_parameter.h"
#include "intl_support.h"

/* fixed stop position of a tab */
#define TAB_STOP        8

/* number of lines at each expansion of more line pointer array */
#define	MORE_LINE_EXPANSION_UNIT	40

/* to build the current help message lines */
static char **iq_More_lines;	/* more message lines */
static int iq_Num_more_lines = 0;	/* number of more lines */

#define DEFAULT_DB_ERROR_MSG_LEVEL      3	/* current max */

/* editor buffer management */
typedef struct
{
  char *contents;
  int data_size;
  int alloc_size;
  RSQL_STATEMENT_STATE state;
} RSQL_EDIT_CONTENTS;

static RSQL_EDIT_CONTENTS rsql_Edit_contents =
  { NULL, 0, 0, RSQL_STATE_GENERAL };


static void iq_format_err (char *string, int buf_size, int line_no,
			   int col_no);
static bool iq_input_device_is_a_tty (void);
static bool iq_output_device_is_a_tty (void);
static int rsql_get_user_home (char *homebuf, int bufsize);

/*
 * iq_output_device_is_a_tty() - return if output stream is associated with
 *                               a "tty" device.
 *   return: true if the output device is a terminal
 */
static bool
iq_output_device_is_a_tty ()
{
  return (rsql_Output_fp == stdout && isatty (fileno (stdout)));
}

/*
 * iq_input_device_is_a_tty() - return if input stream is associated with
 *                              a "tty" device.
 *   return: true if the input device is a terminal
 */
static bool
iq_input_device_is_a_tty ()
{
  return (rsql_Input_fp == stdin && isatty (fileno (stdin)));
}

/*
 * rsql_get_user_home() - get user home directory from /etc/passwd file
  *   return: 0 if success, -1 otherwise
 *   homedir(in/out) : user home directory
 *   homedir_size(in) : size of homedir buffer
 */
static int
rsql_get_user_home (char *homedir, int homedir_size)
{
  struct passwd *ptr = NULL;
  uid_t userid = getuid ();

  setpwent ();

  while ((ptr = getpwent ()) != NULL)
    {
      if (userid == ptr->pw_uid)
	{
	  snprintf (homedir, homedir_size, "%s", ptr->pw_dir);
	  endpwent ();
	  return NO_ERROR;
	}
    }
  endpwent ();
  return ER_FAILED;
}

/*
 * rsql_get_real_path() - get the real pathname (without wild/meta chars) using
 *                      the default shell
 *   return: the real path name
 *   pathname(in)
 *
 * Note:
 *   the real path name returned from this function is valid until next this
 *   function call. The return string will not have any leading/trailing
 *   characters other than the path name itself. If error occurred from O.S,
 *   give up the extension and just return the `pathname'.
 */
char *
rsql_get_real_path (const char *pathname)
{
  static char real_path[PATH_MAX];	/* real path name */
  char home[PATH_MAX];

  if (pathname == NULL)
    {
      return NULL;
    }

  while (isspace (pathname[0]))
    {
      pathname++;
    }

  if (pathname[0] == '\0')
    {
      return NULL;
    }

  /*
   * Do tilde-expansion here.
   */
  if (pathname[0] == '~')
    {
      if (rsql_get_user_home (home, sizeof (home)) != NO_ERROR)
	{
	  return NULL;
	}

      snprintf (real_path, sizeof (real_path), "%s%s", home, &pathname[1]);
    }
  else
    {
      snprintf (real_path, sizeof (real_path), "%s", pathname);
    }

  return real_path;
}

/*
 * rsql_invoke_system() - execute the given command with the argument using
 *                      system()
 *   return: none
 *   command(in)
 */
void
rsql_invoke_system (const char *command)
{
  bool error_found = false;	/* TRUE if error found */

  if (system (command) == 127)
    {
      error_found = true;
      rsql_Error_code = RSQL_ERR_OS_ERROR;
    }

  if (error_found)
    {
      nonscr_display_error ();
    }
}

/*
 * rsql_invoke_system_editor()
 *   return: RSQL_SUCCESS/RSQL_FAILURE
 *
 * Note:
 *   copy command editor buffer into temporary file and
 *   invoke the user preferred system editor. After the
 *   edit is finished, read the file into editor buffer
 */
int
rsql_invoke_system_editor (void)
{
  char *cmd = NULL;
  char *fname = (char *) NULL;	/* pointer to temp file name */
  FILE *fp = (FILE *) NULL;	/* pointer to stream */

  if (!iq_output_device_is_a_tty ())
    {
      rsql_Error_code = RSQL_ERR_CANT_EDIT;
      goto error;
    }

  /* create a temp file and open it */
  fname = tempnam (NULL, NULL);

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

  /* invoke the system editor */
  cmd = rsql_get_tmp_buf (strlen (rsql_Editor_cmd + 1 + strlen (fname)));
  if (cmd == NULL)
    {
      goto error;
    }
  sprintf (cmd, "%s %s", rsql_Editor_cmd, fname);
  rsql_invoke_system (cmd);

  /* initialize editor buffer */
  rsql_edit_contents_clear ();

  fp = fopen (fname, "r");
  if (fp == NULL)
    {
      rsql_Error_code = RSQL_ERR_OS_ERROR;
      goto error;
    }

  /* read the temp file into editor */
  if (rsql_edit_read_file (fp) == RSQL_FAILURE)
    {
      goto error;
    }

  fclose (fp);
  unlink (fname);
  free (fname);

  return RSQL_SUCCESS;

error:
  if (fp != NULL)
    {
      fclose (fp);
    }
  if (fname != NULL)
    {
      unlink (fname);
      free (fname);
    }
  nonscr_display_error ();
  return RSQL_FAILURE;
}

/*
 * rsql_fputs()
 *   return: none
 *   str(in): string to be displayed
 *   fp(in) : FILE stream
 *
 * Note:
 *   `fputs' version to cope with "\1" in the string. This function displays
 *   `<', `>' alternatively.
 */
void
rsql_fputs (const char *str, FILE * fp)
{
  bool flag;			/* toggled at every "\1" */

  if (!fp)
    {
      return;
    }

  for (flag = false; *str != '\0'; str++)
    {
      if (*str == '\1')
	{
	  putc ((flag) ? '>' : '<', fp);
	  flag = !flag;
	}
      else
	{
	  putc (*str, fp);
	}
    }
}

/*
 * rsql_fputs_console_conv() - format and display a string to the RSQL console
 *			       with console conversion applied, if available
 *   return: none
 *   str(in): string to be displayed
 *   fp(in) : FILE stream
 *
 * Note:
 *   `fputs' version to cope with "\1" in the string. This function displays
 *   `<', `>' alternatively.
 */
void
rsql_fputs_console_conv (const char *str, FILE * fp)
{
  char *conv_buf = NULL;
  const char *conv_buf_ptr = NULL;
  int conv_buf_size = 0;

  if (!fp)
    {
      return;
    }

  if (rsql_text_utf8_to_console != NULL
      && (*rsql_text_utf8_to_console) (str, strlen (str), &conv_buf,
				       &conv_buf_size) == NO_ERROR
      && conv_buf != NULL)
    {
      conv_buf_ptr = conv_buf;
    }
  else
    {
      conv_buf_ptr = str;
    }

  rsql_fputs (conv_buf_ptr, fp);

  if (conv_buf != NULL)
    {
      free (conv_buf);
    }
}

/*
 * rsql_popen() - Open & return a pipe file stream to a pager
 *   return: pipe file stream to a pager if stdout is a tty,
 *           otherwise return fd.
 *   cmd(in) : popen command
 *   fd(in): currently open file descriptor
 *
 * Note: Caller should call rsql_pclose() after done.
 */
FILE *
rsql_popen (const char *cmd, FILE * fd)
{
  FILE *pf;			/* pipe stream to pager */

  pf = fd;
  if (cmd == NULL || cmd[0] == '\0')
    {
      return pf;
    }

  if (iq_output_device_is_a_tty () && iq_input_device_is_a_tty ())
    {
      pf = popen (cmd, "w");
      if (pf == NULL)
	{			/* pager failed, */
	  rsql_Error_code = RSQL_ERR_CANT_EXEC_PAGER;
	  nonscr_display_error ();
	  pf = fd;
	}
    }
  else
    {
      pf = fd;
    }

  return (pf);
}

/*
 * rsql_pclose(): close pipe file stream
 *   return: none
 *   pf(in): pipe stream pointer
 *   fd(in): This is the file descriptor for the output stream
 *           which was open prior to calling rsql_popen().
 *
 * Note:
 *   We determine if it's a pipe by comparing the pipe stream pointer (pf)
 *   with the prior file descriptor (fd).  If they are different, then a pipe
 *   was opened and will be closed.
 */
void
rsql_pclose (FILE * pf, FILE * fd)
{
  if (pf != fd)
    {
      pclose (pf);
    }
}

/*
 * iq_format_err() - format an error string with line and/or column number
 *   return: none
 *   string(out): output string buffer
 *   line_no(in): error line number
 *   col_no(in) : error column number
 */
static void
iq_format_err (char *string, int buf_size, int line_no, int col_no)
{
  if (line_no > 0)
    {
      if (col_no > 0)
	snprintf (string, buf_size,
		  msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL,
				  RSQL_EXACT_POSITION_ERR_FORMAT), line_no,
		  col_no);
      else
	snprintf (string, buf_size,
		  msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL,
				  RSQL_START_POSITION_ERR_FORMAT), line_no);
      strcat (string, "\n");
    }
}

/*
 * rsql_display_rsql_err() - display error message
 *   return:  none
 *   line_no(in): error line number
 *   col_no(in) : error column number
 *
 * Note:
 *   if `line_no' is positive, this error is regarded as associated with
 *   the given line number. if `col_no' is positive, it represents the
 *   error position represents the exact position, otherwise it tells where
 *   the stmt starts.
 */
void
rsql_display_rsql_err (int line_no, int col_no)
{
  rsql_Error_code = RSQL_ERR_SQL_ERROR;

  iq_format_err (rsql_Scratch_text, SCRATCH_TEXT_LEN, line_no, col_no);

  if (line_no > 0)
    {
      rsql_fputs ("\n", rsql_Error_fp);
      rsql_fputs_console_conv (rsql_Scratch_text, rsql_Error_fp);
    }
  nonscr_display_error ();
}

/*
 * rsql_display_session_err() - display all query compilation errors
 *                            for this session
 *   return: none
 *   session(in): context of query compilation
 *   line_no(in): statement starting line number
 */
void
rsql_display_session_err (DB_SESSION * session, int line_no)
{
  DB_SESSION_ERROR *err;
  int col_no = 0;

  rsql_Error_code = RSQL_ERR_SQL_ERROR;

  err = db_get_errors (session);

  do
    {
      err = db_get_next_error (err, &line_no, &col_no);
      if (line_no > 0)
	{
	  rsql_fputs ("\n", rsql_Error_fp);
	  iq_format_err (rsql_Scratch_text, SCRATCH_TEXT_LEN, line_no,
			 col_no);
	  rsql_fputs_console_conv (rsql_Scratch_text, rsql_Error_fp);
	}
      nonscr_display_error ();
    }
  while (err);

  return;
}

/*
 * rsql_append_more_line() - append the given line into the
 *                         more message line array
 *   return: RSQL_FAILURE/RSQL_SUCCESS
 *   indent(in): number of blanks to be prefixed
 *   line(in): new line to be put
 *
 * Note:
 *   After usage of the more lines, caller should free by calling
 *   free_more_lines(). The line cannot have control characters except tab,
 *   new-line and "\1".
 */
int
rsql_append_more_line (int indent, const char *line)
{
  int i, j;
  int n;			/* register copy of num_more_lines */
  int exp_len;			/* length of lines after tab expand */
  int new_num;			/* new # of entries */
  char *p;
  const char *q;
  char **t_lines;		/* temp pointer */
  char *conv_buf = NULL;
  int conv_buf_size = 0;

  if (rsql_text_utf8_to_console != NULL &&
      (*rsql_text_utf8_to_console) (line, strlen (line), &conv_buf,
				    &conv_buf_size) == NO_ERROR)
    {
      line = (conv_buf != NULL) ? conv_buf : line;
    }
  else
    {
      assert (conv_buf == NULL);
    }

  n = iq_Num_more_lines;

  if (n % MORE_LINE_EXPANSION_UNIT == 0)
    {
      new_num = n + MORE_LINE_EXPANSION_UNIT;
      if (n == 0)
	{
	  t_lines = (char **) malloc (sizeof (char *) * new_num);
	}
      else
	{
	  t_lines =
	    (char **) realloc (iq_More_lines, sizeof (char *) * new_num);
	}
      if (t_lines == NULL)
	{
	  rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
	  if (conv_buf != NULL)
	    {
	      assert (rsql_text_utf8_to_console != NULL);
	      free_and_init (conv_buf);
	    }
	  return (RSQL_FAILURE);
	}
      iq_More_lines = t_lines;
    }

  /* calculate # of bytes should be allocated to store
   * the given line in tab-expanded form
   */
  for (i = exp_len = 0, q = line; *q != '\0'; q++)
    {
      if (*q == '\n')
	{
	  exp_len += i + 1;
	  i = 0;
	}
      else if (*q == '\t')
	{
	  i += TAB_STOP - i % TAB_STOP;
	}
      else
	{
	  i++;
	}
    }
  exp_len += i + 1;

  iq_More_lines[n] = (char *) malloc (indent + exp_len);
  if (iq_More_lines[n] == NULL)
    {
      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
      if (conv_buf != NULL)
	{
	  assert (rsql_text_utf8_to_console != NULL);
	  free_and_init (conv_buf);
	}
      return (RSQL_FAILURE);
    }
  for (i = 0, p = iq_More_lines[n]; i < indent; i++)
    {
      *p++ = ' ';
    }

  /* copy the line with tab expansion */
  for (i = 0, q = line; *q != '\0'; q++)
    {
      if (*q == '\n')
	{
	  *p++ = *q;
	  i = 0;
	}
      else if (*q == '\t')
	{
	  for (j = TAB_STOP - i % TAB_STOP; j > 0; j--, i++)
	    {
	      *p++ = ' ';
	    }
	}
      else
	{
	  *p++ = *q;
	  i++;
	}
    }
  *p = '\0';

  iq_Num_more_lines++;

  if (conv_buf != NULL)
    {
      assert (rsql_text_utf8_to_console != NULL);
      free_and_init (conv_buf);
    }

  return (RSQL_SUCCESS);
}

/*
 * rsql_display_more_lines() - display lines in stdout.
 *   return: none
 *   title(in): optional title message
 *
 * Note: "\1" in line will be displayed `<' and `>', alternatively.
 */
void
rsql_display_more_lines (const char *title)
{
  int i;
  FILE *pf;			/* pipe stream to pager */

  /* simple code without signal, setjmp, longjmp
   */

  pf = rsql_popen (rsql_Pager_cmd, rsql_Output_fp);

  /* display title */
  if (title != NULL)
    {
      sprintf (rsql_Scratch_text, "\n=== %s ===\n\n", title);
      rsql_fputs (rsql_Scratch_text, pf);
    }

  for (i = 0; i < iq_Num_more_lines; i++)
    {
      rsql_fputs (iq_More_lines[i], pf);
      putc ('\n', pf);
    }
  putc ('\n', pf);

  rsql_pclose (pf, rsql_Output_fp);
}

/*
 * rsql_free_more_lines() - free more lines built by rsql_append_more_line()
 *   return: none
 */
void
rsql_free_more_lines (void)
{
  int i;

  if (iq_Num_more_lines > 0)
    {
      for (i = 0; i < iq_Num_more_lines; i++)
	{
	  if (iq_More_lines[i] != NULL)
	    {
	      free_and_init (iq_More_lines[i]);
	    }
	}
      free_and_init (iq_More_lines);
      iq_Num_more_lines = 0;
    }
}

/*
 * rsql_check_server_down() - check if server is down
 *   return: none
 *
 * Note: If server is down, this function exit
 */
void
rsql_check_server_down (void)
{
  if (db_error_code () == ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED)
    {
      nonscr_display_error ();

      fprintf (rsql_Error_fp, "Exiting ...\n");
      rsql_exit (EXIT_FAILURE);
    }
}

/*
 * rsql_get_tmp_buf()
 *   return: a pointer to a buffer for temporary formatting
 *   size(in): the number of characters required
 *
 * Note:
 *   This routine frees sprintf() users from having to worry
 *   too much about how much space they'll need; just call
 *   this with the number of characters required, and you'll
 *   get something that you don't have to worry about
 *   managing.
 *
 *   Don't free the pointer you get back from this routine
 */
char *
rsql_get_tmp_buf (size_t size)
{
  static char buf[1024];
  static char *bufp = NULL;
  static size_t bufsize = 0;

  if (size + 1 < sizeof (buf))
    {
      return buf;
    }
  else
    {
      /*
       * buf isn't big enough, so see if we have an already-malloc'ed
       * thing that is big enough.  If so, use it; if not, free it if
       * it exists, and then allocate a big enough one.
       */
      if (size + 1 < bufsize)
	{
	  return bufp;
	}
      else
	{
	  if (bufp)
	    {
	      free_and_init (bufp);
	      bufsize = 0;
	    }
	  bufsize = size + 1;
	  bufp = (char *) malloc (bufsize);
	  if (bufp == NULL)
	    {
	      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
	      bufsize = 0;
	      return NULL;
	    }
	  else
	    {
	      return bufp;
	    }
	}
    }
}

/*
 * nonscr_display_error() - format error message with global error code
 *   return: none
 */
void
nonscr_display_error (void)
{
  char *buffer = rsql_Scratch_text;
  int buf_length = SCRATCH_TEXT_LEN;
  int remaining;
  char *msg;
  const char *errmsg;
  int len_errmsg;
  char *con_buf_ptr = NULL;
  int con_buf_size = 0;

  remaining = buf_length;

  strncpy (buffer, "\n", remaining);
  remaining -= strlen ("\n");

  msg = msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL,
			RSQL_ERROR_PREFIX);
  strncat (buffer, msg, remaining);
  remaining -= strlen (msg);

  errmsg = rsql_errmsg (rsql_Error_code);
  len_errmsg = strlen (errmsg);

  if (rsql_text_utf8_to_console != NULL &&
      (*rsql_text_utf8_to_console) (errmsg, len_errmsg,
				    &con_buf_ptr, &con_buf_size) == NO_ERROR)
    {
      if (con_buf_ptr != NULL)
	{
	  errmsg = con_buf_ptr;
	  len_errmsg = con_buf_size;
	}
    }

  if (len_errmsg > (remaining - 3) /* "\n\n" + NULL */ )
    {
      /* error msg will split into 2 pieces which is separated by "......" */
      int print_len;
      const char *separator = "......";
      int separator_len = strlen (separator);

      print_len = (remaining - 3 - separator_len) / 2;
      strncat (buffer, errmsg, print_len);	/* first half */
      strncat (buffer, separator, separator_len);
      strncat (buffer, errmsg + len_errmsg - print_len, print_len);	/* second half */
      remaining -= (print_len * 2 + separator_len);
    }
  else
    {
      strncat (buffer, errmsg, remaining);
      remaining -= len_errmsg;
    }

  if (con_buf_ptr != NULL)
    {
      free_and_init (con_buf_ptr);
    }

  strncat (buffer, "\n\n", remaining);
  remaining -= strlen ("\n\n");

  buffer[buf_length - 1] = '\0';
  rsql_fputs (buffer, rsql_Error_fp);
}

/*
 * rsql_edit_contents_get () - get string of current editor contents
 *   return: pointer of contents
 */
const char *
rsql_edit_contents_get ()
{
  if (rsql_Edit_contents.data_size <= 0)
    {
      return ("");
    }
  return rsql_Edit_contents.contents;
}

static int
rsql_edit_contents_expand (int required_size)
{
  int new_alloc_size = rsql_Edit_contents.alloc_size;
  if (new_alloc_size >= required_size)
    return RSQL_SUCCESS;

  if (new_alloc_size <= 0)
    {
      new_alloc_size = 1024;
    }
  while (new_alloc_size < required_size)
    {
      new_alloc_size *= 2;
    }
  rsql_Edit_contents.contents =
    realloc (rsql_Edit_contents.contents, new_alloc_size);
  if (rsql_Edit_contents.contents == NULL)
    {
      rsql_Edit_contents.alloc_size = 0;
      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
      return RSQL_FAILURE;
    }
  rsql_Edit_contents.alloc_size = new_alloc_size;
  return RSQL_SUCCESS;
}

/*
 * rsql_edit_contents_append() - append string to current editor contents
 *   return: RSQL_SUCCESS/RSQL_FAILURE
 *   str(in): string to append
 *   flag_append_new_line(in): whether or not to append new line char
 */
int
rsql_edit_contents_append (const char *str, bool flag_append_new_line)
{
  int str_len, new_data_size;
  if (str == NULL)
    {
      return RSQL_SUCCESS;
    }
  str_len = strlen (str);
  new_data_size = rsql_Edit_contents.data_size + str_len;
  if (rsql_edit_contents_expand (new_data_size + 2) != RSQL_SUCCESS)
    {
      return RSQL_FAILURE;
    }
  memcpy (rsql_Edit_contents.contents + rsql_Edit_contents.data_size, str,
	  str_len);
  rsql_Edit_contents.data_size = new_data_size;
  if (flag_append_new_line)
    {
      rsql_Edit_contents.contents[rsql_Edit_contents.data_size++] = '\n';
    }
  rsql_Edit_contents.contents[rsql_Edit_contents.data_size] = '\0';
  return RSQL_SUCCESS;
}

/*
 * rsql_walk_statement () - parse str and change the state
 * return : NULL
 * str (in) : the new statement chunk received from input
 */
RSQL_STATEMENT_STATE
rsql_walk_statement (const char *str, RSQL_STATEMENT_STATE state)
{
  /* using flags but not adding many states in here may be not good choice,
   * but it will not change the state machine model and save a lot of states.
   */
  bool include_stmt = false;
  bool is_last_stmt_valid = true;
  const char *p;
  int str_length;

  if (str == NULL)
    {
      return state;
    }

  if (state == RSQL_STATE_CPP_COMMENT || state == RSQL_STATE_SQL_COMMENT)
    {
      /* these are single line comments and we're parsing a new line */
      state = RSQL_STATE_GENERAL;
    }

  if (state == RSQL_STATE_STATEMENT_END)
    {
      /* reset state in prev statement */
      state = RSQL_STATE_GENERAL;
    }

  str_length = strlen (str);
  /* run as state machine */
  for (p = str; p < str + str_length; p++)
    {
      switch (state)
	{
	case RSQL_STATE_GENERAL:
	  switch (*p)
	    {
	    case '/':
	      if (*(p + 1) == '/')
		{
		  state = RSQL_STATE_CPP_COMMENT;
		  p++;
		  break;
		}
	      if (*(p + 1) == '*')
		{
		  state = RSQL_STATE_C_COMMENT;
		  p++;
		  break;
		}
	      is_last_stmt_valid = true;
	      break;
	    case '-':
	      if (*(p + 1) == '-')
		{
		  state = RSQL_STATE_SQL_COMMENT;
		  p++;
		  break;
		}
	      is_last_stmt_valid = true;
	      break;
	    case '\'':
	      state = RSQL_STATE_SINGLE_QUOTE;
	      is_last_stmt_valid = true;
	      break;
	    case '"':
	      if (prm_get_bool_value (PRM_ID_ANSI_QUOTES) == false)
		{
		  state = RSQL_STATE_MYSQL_QUOTE;
		}
	      else
		{
		  state = RSQL_STATE_DOUBLE_QUOTE_IDENTIFIER;
		}
	      is_last_stmt_valid = true;
	      break;
	    case '`':
	      state = RSQL_STATE_BACKTICK_IDENTIFIER;
	      is_last_stmt_valid = true;
	      break;
	    case '[':
	      state = RSQL_STATE_BRACKET_IDENTIFIER;
	      is_last_stmt_valid = true;
	      break;
	    case ';':
	      include_stmt = true;
	      is_last_stmt_valid = false;
	      if (*(p + 1) == 0)
		{
		  state = RSQL_STATE_STATEMENT_END;
		}
	      break;
	    case ' ':
	    case '\t':
	      /* do not change is_last_stmt_valid */
	      break;
	    default:
	      if (!is_last_stmt_valid)
		{
		  is_last_stmt_valid = true;
		}
	      break;
	    }
	  break;

	case RSQL_STATE_C_COMMENT:
	  if (*p == '*' && *(p + 1) == '/')
	    {
	      state = RSQL_STATE_GENERAL;
	      p++;
	      break;
	    }
	  break;

	case RSQL_STATE_CPP_COMMENT:
	  if (*p == '\n')
	    {
	      state = RSQL_STATE_GENERAL;
	    }
	  break;

	case RSQL_STATE_SQL_COMMENT:
	  if (*p == '\n')
	    {
	      state = RSQL_STATE_GENERAL;
	    }
	  break;

	case RSQL_STATE_SINGLE_QUOTE:
	  if (*p == '\'')
	    {
	      if (*(p + 1) == '\'')
		{
		  /* escape by '' */
		  p++;
		}
	      else
		{
		  state = RSQL_STATE_GENERAL;
		}
	    }
	  break;

	case RSQL_STATE_MYSQL_QUOTE:
	  if (*p == '"')
	    {
	      if (*(p + 1) == '\"')
		{
		  /* escape by "" */
		  p++;
		}
	      else
		{
		  state = RSQL_STATE_GENERAL;
		}
	    }
	  break;

	case RSQL_STATE_DOUBLE_QUOTE_IDENTIFIER:
	  if (*p == '"')
	    {
	      state = RSQL_STATE_GENERAL;
	    }
	  break;

	case RSQL_STATE_BACKTICK_IDENTIFIER:
	  if (*p == '`')
	    {
	      state = RSQL_STATE_GENERAL;
	    }
	  break;

	case RSQL_STATE_BRACKET_IDENTIFIER:
	  if (*p == ']')
	    {
	      state = RSQL_STATE_GENERAL;
	    }
	  break;

	default:
	  /* should not be here */
	  break;
	}
    }

  /* when include other stmts and the last smt is non sense stmt. */
  if (include_stmt && !is_last_stmt_valid
      && (state == RSQL_STATE_SQL_COMMENT || state == RSQL_STATE_CPP_COMMENT
	  || state == RSQL_STATE_GENERAL))
    {
      state = RSQL_STATE_STATEMENT_END;
    }

  return state;
}

/*
 * rsql_is_statement_complete () - check if end of statement is reached
 * return : true if statement end is reached, false otherwise
 */
bool
rsql_is_statement_complete (RSQL_STATEMENT_STATE state)
{
  if (state == RSQL_STATE_STATEMENT_END)
    {
      return true;
    }
  else
    {
      return false;
    }
}

/*
 * rsql_is_statement_in_block () - check if statement state is string block or
 *                       comment block or identifier block
 *    return : true if yes, false otherwise
 *
 *    state(in):
 */
bool
rsql_is_statement_in_block (RSQL_STATEMENT_STATE state)
{
  if (state == RSQL_STATE_C_COMMENT || state == RSQL_STATE_SINGLE_QUOTE
      || state == RSQL_STATE_MYSQL_QUOTE
      || state == RSQL_STATE_DOUBLE_QUOTE_IDENTIFIER
      || state == RSQL_STATE_BACKTICK_IDENTIFIER
      || state == RSQL_STATE_BRACKET_IDENTIFIER)
    {
      return true;
    }
  else
    {
      return false;
    }
}

/*
 * rsql_edit_contents_clear() - clear current editor contents
 *   return: none
 * NOTE: allocated memory in rsql_Edit_contents is not freed.
 */
void
rsql_edit_contents_clear ()
{
  rsql_Edit_contents.data_size = 0;
  rsql_Edit_contents.state = RSQL_STATE_GENERAL;
}

void
rsql_edit_contents_finalize ()
{
  rsql_edit_contents_clear ();
  free_and_init (rsql_Edit_contents.contents);
  rsql_Edit_contents.alloc_size = 0;
}



/*
 * rsql_edit_read_file() - read chars from the given file stream into
 *                          current editor contents
 *   return: RSQL_FAILURE/RSQL_SUCCESS
 *   fp(in): file stream
 */
int
rsql_edit_read_file (FILE * fp)
{
  char line_buf[1024];
  bool is_first_read_line = true;

  while (fgets (line_buf, sizeof (line_buf), fp) != NULL)
    {
      char *line_begin = line_buf;

      if (is_first_read_line
	  && intl_is_bom_magic (line_buf, strlen (line_buf)))
	{
	  line_begin += 3;
	}

      is_first_read_line = false;

      if (rsql_edit_contents_append (line_begin, false) != RSQL_SUCCESS)
	return RSQL_FAILURE;
    }
  return RSQL_SUCCESS;
}

/*
 * rsql_edit_write_file() - write current editor contents to specified file
 *   return: RSQL_FAILURE/RSQL_SUCCESS
 *   fp(in): open file pointer
 */
int
rsql_edit_write_file (FILE * fp)
{
  char *p = rsql_Edit_contents.contents;
  int remain_size = rsql_Edit_contents.data_size;
  int write_len;
  while (remain_size > 0)
    {
      write_len =
	(int) fwrite (p + (rsql_Edit_contents.data_size - remain_size), 1,
		      remain_size, fp);
      if (write_len <= 0)
	{
	  rsql_Error_code = RSQL_ERR_OS_ERROR;
	  return RSQL_FAILURE;
	}
      remain_size -= write_len;
    }
  return RSQL_SUCCESS;
}

typedef struct
{
  int error_code;
  int msg_id;
} RSQL_ERR_MSG_MAP;

static RSQL_ERR_MSG_MAP rsql_Err_msg_map[] = {
  {RSQL_ERR_NO_MORE_MEMORY, RSQL_E_NOMOREMEMORY_TEXT},
  {RSQL_ERR_TOO_LONG_LINE, RSQL_E_TOOLONGLINE_TEXT},
  {RSQL_ERR_TOO_MANY_LINES, RSQL_E_TOOMANYLINES_TEXT},
  {RSQL_ERR_TOO_MANY_FILE_NAMES, RSQL_E_TOOMANYFILENAMES_TEXT},
  {RSQL_ERR_SESS_CMD_NOT_FOUND, RSQL_E_SESSCMDNOTFOUND_TEXT},
  {RSQL_ERR_SESS_CMD_AMBIGUOUS, RSQL_E_SESSCMDAMBIGUOUS_TEXT},
  {RSQL_ERR_FILE_NAME_MISSED, RSQL_E_FILENAMEMISSED_TEXT},
  {RSQL_ERR_RYE_STMT_AMBIGUOUS, RSQL_E_RSQLCMDAMBIGUOUS_TEXT},
  {RSQL_ERR_CANT_EXEC_PAGER, RSQL_E_CANTEXECPAGER_TEXT},
  {RSQL_ERR_INVALID_ARG_COMBINATION, RSQL_E_INVALIDARGCOM_TEXT},
  {RSQL_ERR_CANT_EDIT, RSQL_E_CANT_EDIT_TEXT},
  {RSQL_ERR_INFO_CMD_HELP, RSQL_HELP_INFOCMD_TEXT},
  {RSQL_ERR_CLASS_NAME_MISSED, RSQL_E_CLASSNAMEMISSED_TEXT}
};

/*
 * rsql_errmsg() - return an error message string according to the given
 *               error code
 *   return: error message
 *   code(in): error code
 */
const char *
rsql_errmsg (int code)
{
  int msg_map_size;
  const char *msg;

  if (code == RSQL_ERR_OS_ERROR)
    {
      return (strerror (errno));
    }
  else if (code == RSQL_ERR_SQL_ERROR)
    {
      msg = db_error_string (DEFAULT_DB_ERROR_MSG_LEVEL);
      return ((msg == NULL) ? "" : msg);
    }
  else
    {
      int i;

      msg_map_size = DIM (rsql_Err_msg_map);
      for (i = 0; i < msg_map_size; i++)
	{
	  if (code == rsql_Err_msg_map[i].error_code)
	    {
	      return (rsql_get_message (rsql_Err_msg_map[i].msg_id));
	    }
	}
      return (rsql_get_message (RSQL_E_UNKNOWN_TEXT));
    }
}

/*
 * rsql_query_init -
 *    return: NO_ERROR or error code
 *
 *    query(out):
 */
int
rsql_query_init (RSQL_QUERY * query, int size)
{
  int error = NO_ERROR;

  assert (query != NULL);

  query->query = NULL;
  query->alloc_size = 0;
  query->length = 0;

  query->query = (char *) malloc (size);
  if (query->query == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, size);

      return error;
    }
  query->query[0] = '\0';
  query->alloc_size = size;
  query->length = 0;

  return NO_ERROR;
}

/*
 * rsql_query_final -
 *    return NO_ERROR or error code
 *
 *    query(in/out):
 */
int
rsql_query_final (RSQL_QUERY * query)
{
  if (query->query != NULL)
    {
      free_and_init (query->query);
    }
  query->alloc_size = 0;
  query->length = 0;

  return NO_ERROR;
}

/*
 * rsql_query_clear -
 *    return NO_ERROR or error code
 *
 *    query(in/out):
 */
int
rsql_query_clear (RSQL_QUERY * query)
{
  assert (query != NULL);
  assert (query->query != NULL);
  assert (query->alloc_size != 0);

  query->length = 0;
  query->query[0] = '\0';

  return NO_ERROR;
}

/*
 * rsql_query_append_string -
 *    return: NO_ERROR or error code
 *
 *    query(in/out):
 *    str(in):
 *    str_length(in):
 *    flag_append_new_line(in):
 */
int
rsql_query_append_string (RSQL_QUERY * query, char *str, int str_length,
			  bool flag_append_new_line)
{
  char *area = NULL;
  int new_alloc_size = 0;
  int error = NO_ERROR;

  assert (query != NULL);
  assert (query->query != NULL);
  assert (query->length >= 0);
  assert (query->alloc_size >= 0);
  assert (query->length == strlen (query->query));

  if (str_length <= 0)
    {
      str_length = strlen (str);
    }

  assert (str_length >= 0);
  if (query->length + str_length + 1 >= query->alloc_size)
    {
      new_alloc_size = query->alloc_size + MAX (str_length + 1, ONE_K);
      area = realloc (query->query, new_alloc_size);
      if (area == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, new_alloc_size);

	  return error;
	}

      query->query = area;
      query->alloc_size = new_alloc_size;
    }

  memcpy (query->query + query->length, str, str_length);
  query->length = query->length + str_length;

  if (flag_append_new_line)
    {
      query->query[query->length++] = '\n';
    }

  query->query[query->length] = '\0';

  return NO_ERROR;
}

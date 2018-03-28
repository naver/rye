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
 * rsql_session.c : menu driver of rsql
 */

#ident "$Id$"

#include "config.h"

#include <stdarg.h>
#include <string.h>
#include <signal.h>

#include "porting.h"
#include "rsql.h"
#include "memory_alloc.h"
#include "util_func.h"
#include "network_interface_cl.h"

/* for short usage of `rsql_append_more_line()' and error check */
#define	APPEND_MORE_LINE(indent, line)	\
		do { \
		  if(rsql_append_more_line((indent), (line)) == RSQL_FAILURE) \
		    goto error; \
		} while(0)
#define	APPEND_HEAD_LINE(head_text)	\
		do { \
		  APPEND_MORE_LINE(0, ""); \
		  APPEND_MORE_LINE(1, (head_text)); \
		  APPEND_MORE_LINE(0, ""); \
		} while(0)


#define CMD_EMPTY_FLAG	  0x00000000
#define CMD_CHECK_CONNECT 0x00000001

/* session command table */
typedef struct
{
  const char *text;             /* lower case cmd name */
  SESSION_CMD cmd_no;           /* command number */
  unsigned int flags;
} SESSION_CMD_TABLE;

static SESSION_CMD_TABLE rsql_Session_cmd_table[] = {
  /* File stuffs */
  {"read", S_CMD_READ, CMD_EMPTY_FLAG},
  {"write", S_CMD_WRITE, CMD_EMPTY_FLAG},
  {"append", S_CMD_APPEND, CMD_EMPTY_FLAG},
  {"print", S_CMD_PRINT, CMD_EMPTY_FLAG},
  {"shell", S_CMD_SHELL, CMD_EMPTY_FLAG},
  {"!", S_CMD_SHELL, CMD_EMPTY_FLAG},
  {"cd", S_CMD_CD, CMD_EMPTY_FLAG},
  {"exit", S_CMD_EXIT, CMD_EMPTY_FLAG},
  /* Edit stuffs */
  {"clear", S_CMD_CLEAR, CMD_EMPTY_FLAG},
  {"edit", S_CMD_EDIT, CMD_EMPTY_FLAG},
  {"list", S_CMD_LIST, CMD_EMPTY_FLAG},
  /* Command stuffs */
  {"run", S_CMD_RUN, CMD_CHECK_CONNECT},
  {"xrun", S_CMD_XRUN, CMD_CHECK_CONNECT},
  {"commit", S_CMD_COMMIT, CMD_CHECK_CONNECT},
  {"rollback", S_CMD_ROLLBACK, CMD_CHECK_CONNECT},
  {"autocommit", S_CMD_AUTOCOMMIT, CMD_EMPTY_FLAG},
  {"checkpoint", S_CMD_CHECKPOINT, CMD_CHECK_CONNECT},
  {"killtran", S_CMD_KILLTRAN, CMD_CHECK_CONNECT},
  {"restart", S_CMD_RESTART, CMD_EMPTY_FLAG},
  /* Environment stuffs */
  {"shell_cmd", S_CMD_SHELL_CMD, CMD_EMPTY_FLAG},
  {"editor_cmd", S_CMD_EDIT_CMD, CMD_EMPTY_FLAG},
  {"print_cmd", S_CMD_PRINT_CMD, CMD_EMPTY_FLAG},
  {"pager_cmd", S_CMD_PAGER_CMD, CMD_EMPTY_FLAG},
  {"nopager", S_CMD_NOPAGER_CMD, CMD_EMPTY_FLAG},
  {"column-width", S_CMD_COLUMN_WIDTH, CMD_EMPTY_FLAG},
  {"string-width", S_CMD_STRING_WIDTH, CMD_EMPTY_FLAG},
  {"groupid", S_CMD_GROUPID, CMD_EMPTY_FLAG},
  {"set", S_CMD_SET_PARAM, CMD_CHECK_CONNECT},
  {"get", S_CMD_GET_PARAM, CMD_CHECK_CONNECT},
  {"plan", S_CMD_PLAN_DUMP, CMD_CHECK_CONNECT},
  {"echo", S_CMD_ECHO, CMD_EMPTY_FLAG},
  {"date", S_CMD_DATE, CMD_EMPTY_FLAG},
  {"time", S_CMD_TIME, CMD_EMPTY_FLAG},
  {"line-output", S_CMD_LINE_OUTPUT, CMD_EMPTY_FLAG},
  {".hist", S_CMD_HISTO, CMD_EMPTY_FLAG},
  {".clear_hist", S_CMD_CLR_HISTO, CMD_EMPTY_FLAG},
  {".dump_hist", S_CMD_DUMP_HISTO, CMD_EMPTY_FLAG},
  {".x_hist", S_CMD_DUMP_CLR_HISTO, CMD_EMPTY_FLAG},
  /* Help stuffs */
  {"help", S_CMD_HELP, CMD_EMPTY_FLAG},
  {"schema", S_CMD_SCHEMA, CMD_CHECK_CONNECT},
  {"database", S_CMD_DATABASE, CMD_CHECK_CONNECT},
  {"info", S_CMD_INFO, CMD_EMPTY_FLAG},
  /* history stuffs */
  {"historyread", S_CMD_HISTORY_READ, CMD_EMPTY_FLAG},
  {"historylist", S_CMD_HISTORY_LIST, CMD_EMPTY_FLAG},

  {"trace", S_CMD_TRACE, CMD_CHECK_CONNECT}
};

/*
 * rsql_get_session_cmd_no() - find a session command
 *   return: SESSION_CMD number if success.
 *           if error, -1 is returned and rsql_Error_code is set.
 *   input(in)
 *
 * Note:
 *   The search function succeed when there is only one entry which starts
 *   with the given string, or there is an entry which matches exactly.
 */
SESSION_CMD
rsql_get_session_cmd_no (const char *input)
{
  int i;                        /* loop counter */
  int input_cmd_length;         /* input command length */
  int num_matches = 0;          /* # of matched commands */
  int matched_index = -1;       /* last matched entry index */

  if (*input == '\0')
    {
      /* rsql>; */
      return S_CMD_QUERY;
    }

  input_cmd_length = strlen (input);
  num_matches = 0;
  matched_index = -1;
  for (i = 0; i < (int) DIM (rsql_Session_cmd_table); i++)
    {
      if (strncasecmp (input, rsql_Session_cmd_table[i].text, input_cmd_length) == 0)
        {
          int ses_cmd_length;

          ses_cmd_length = strlen (rsql_Session_cmd_table[i].text);
          if (ses_cmd_length == input_cmd_length)
            {
              return (rsql_Session_cmd_table[i].cmd_no);
            }
          num_matches++;
          matched_index = i;
        }
    }
  if (num_matches != 1)
    {
      rsql_Error_code = (num_matches > 1) ? RSQL_ERR_SESS_CMD_AMBIGUOUS : RSQL_ERR_SESS_CMD_NOT_FOUND;
      return S_CMD_UNKNOWN;
    }
#if defined (CS_MODE)
  if (rsql_Session_cmd_table[matched_index].flags & CMD_CHECK_CONNECT)
    {
      if (db_Connect_status != DB_CONNECTION_STATUS_CONNECTED)
        {
          rsql_Error_code = RSQL_ERR_SQL_ERROR;
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_NO_CONNECT, 0);
          return (-1);
        }
    }
#endif

  return (rsql_Session_cmd_table[matched_index].cmd_no);
}

/*
 * rsql_help_menu() - display appropriate help message
 *   return: none
 */
void
rsql_help_menu (void)
{
  if (rsql_append_more_line (0, msgcat_message (MSGCAT_CATALOG_RSQL,
                                                MSGCAT_RSQL_SET_RSQL, RSQL_HELP_SESSION_CMD_TEXT)) == RSQL_FAILURE)
    {
      goto error;
    }
  rsql_display_more_lines (msgcat_message (MSGCAT_CATALOG_RSQL,
                                           MSGCAT_RSQL_SET_RSQL, RSQL_HELP_SESSION_CMD_TITLE_TEXT));

  rsql_free_more_lines ();
  return;

error:
  nonscr_display_error ();
  rsql_free_more_lines ();
}

/*
 * rsql_help_schema() - display schema information for given class name
 *   return: none
 *   class_name(in)
 */
void
rsql_help_schema (const char *class_name)
{
  CLASS_HELP *class_schema = NULL;
  char **line_ptr;
  char class_title[2 * DB_MAX_IDENTIFIER_LENGTH + 2];
  char fixed_class_name[DB_MAX_IDENTIFIER_LENGTH];

  if (class_name == NULL || class_name[0] == 0)
    {
      rsql_Error_code = RSQL_ERR_CLASS_NAME_MISSED;
      goto error;
    }

  if (strlen (class_name) >= DB_MAX_IDENTIFIER_LENGTH)
    {
      rsql_Error_code = RSQL_ERR_TOO_LONG_LINE;
      goto error;
    }
  else
    {
      strcpy (fixed_class_name, class_name);
      /* check that both lower and upper case are not truncated */
      if (intl_identifier_fix (fixed_class_name, -1) != NO_ERROR)
        {
          rsql_Error_code = RSQL_ERR_TOO_LONG_LINE;
          goto error;
        }
      class_name = fixed_class_name;
    }

  class_schema = obj_print_help_class_name (class_name);
  if (class_schema == NULL)
    {
      rsql_Error_code = RSQL_ERR_SQL_ERROR;
      goto error;
    }

  snprintf (class_title, (2 * DB_MAX_IDENTIFIER_LENGTH + 2),
            msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL,
                            RSQL_HELP_CLASS_HEAD_TEXT), class_schema->class_type);
  APPEND_HEAD_LINE (class_title);
  APPEND_MORE_LINE (5, class_schema->name);

  APPEND_HEAD_LINE (msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL, RSQL_HELP_ATTRIBUTE_HEAD_TEXT));
  if (class_schema->attributes == NULL)
    {
      APPEND_MORE_LINE (5, msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL, RSQL_HELP_NONE_TEXT));
    }
  else
    {
      for (line_ptr = class_schema->attributes; *line_ptr != NULL; line_ptr++)
        {
          APPEND_MORE_LINE (5, *line_ptr);
        }
    }

  if (class_schema->constraints != NULL)
    {
      APPEND_HEAD_LINE (msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL, RSQL_HELP_CONSTRAINT_HEAD_TEXT));
      for (line_ptr = class_schema->constraints; *line_ptr != NULL; line_ptr++)
        {
          APPEND_MORE_LINE (5, *line_ptr);
        }
    }

  if (class_schema->object_id != NULL)
    {
      APPEND_MORE_LINE (0, "");
      APPEND_MORE_LINE (1, class_schema->object_id);
    }

  if (class_schema->shard_by != NULL)
    {
      APPEND_HEAD_LINE (msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL, RSQL_HELP_SHARD_SPEC_HEAD_TEXT));
      APPEND_MORE_LINE (5, class_schema->shard_by);
    }

  if (class_schema->query_spec != NULL)
    {
      APPEND_HEAD_LINE (msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL, RSQL_HELP_QUERY_SPEC_HEAD_TEXT));
      for (line_ptr = class_schema->query_spec; *line_ptr != NULL; line_ptr++)
        {
          APPEND_MORE_LINE (5, *line_ptr);
        }
    }

  rsql_display_more_lines (msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL, RSQL_HELP_SCHEMA_TITLE_TEXT));

  obj_print_help_free_class (class_schema);
  rsql_free_more_lines ();

  return;

error:

  if (class_schema != NULL)
    {
      obj_print_help_free_class (class_schema);
    }

  if (rsql_Error_code == RSQL_ERR_SQL_ERROR)
    {
      rsql_display_rsql_err (0, 0);
    }
  else
    {
      nonscr_display_error ();
    }
  rsql_free_more_lines ();
}

/*
 * rsql_help_info() - display database information for given command
 *   return: none
 *   command(in): "schema [<class name>]"
 *                "workspace"
 *                "lock"
 *                "stats [<class name>]"
 *                "plan"
 *                "qcache"
 *   aucommit_flag(in): auto-commit mode flag
 */
void
rsql_help_info (const char *command, int aucommit_flag)
{
  char *dup = NULL, *tok, *save;
  FILE *p_stream;               /* pipe stream to pager */
  void (*rsql_intr_save) (int sig);
  void (*rsql_pipe_save) (int sig);

  if (!command)
    {
      rsql_Error_code = RSQL_ERR_INFO_CMD_HELP;
      goto error;
    }

  dup = strdup (command);
  if (dup == NULL)
    {
      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
      goto error;
    }
  tok = strtok_r (dup, " \t", &save);
  if (tok != NULL &&
      (!strcasecmp (tok, "schema") || !strcasecmp (tok, "workspace") ||
       !strcasecmp (tok, "lock") || !strcasecmp (tok, "stats") ||
       !strcasecmp (tok, "logstat") || !strcasecmp (tok, "csstat") ||
       !strcasecmp (tok, "plan") || !strcasecmp (tok, "qcache") ||
       !strcasecmp (tok, "trantable") || !strcasecmp (tok, "serverstats")))
    {
      int result;

      rsql_intr_save = signal (SIGINT, SIG_IGN);
      rsql_pipe_save = signal (SIGPIPE, SIG_IGN);

      result = NO_ERROR;

      p_stream = rsql_popen (rsql_Pager_cmd, rsql_Output_fp);
      help_print_info (command, p_stream);
      if (aucommit_flag)
        {
          result = db_commit_transaction ();
        }
      rsql_pclose (p_stream, rsql_Output_fp);
      if (aucommit_flag)
        {
          if (result != NO_ERROR)
            {
              rsql_display_rsql_err (0, 0);
              rsql_check_server_down ();
            }
          else
            {
              rsql_display_msg (msgcat_message (MSGCAT_CATALOG_RSQL, MSGCAT_RSQL_SET_RSQL, RSQL_STAT_COMMITTED_TEXT));
            }
        }

      signal (SIGINT, rsql_intr_save);
      signal (SIGPIPE, rsql_pipe_save);
    }
  else
    {
      rsql_Error_code = RSQL_ERR_INFO_CMD_HELP;
      goto error;
    }

  free (dup);
  return;

error:
  nonscr_display_error ();
  if (dup)
    {
      free (dup);
    }
}

/*
 * rsql_killtran() - kill a transaction
 *   return: none
 *   argument: tran index or NULL (dump transaction list)
 */
void
rsql_killtran (const char *argument)
{
  TRANS_INFO *info = NULL;
  int tran_index = -1, i;
  FILE *p_stream;               /* pipe stream to pager */

  if (argument)
    {
      tran_index = atoi (argument);
    }

  info = logtb_get_trans_info (false);
  if (info == NULL)
    {
      rsql_Error_code = RSQL_ERR_NO_MORE_MEMORY;
      goto error;
    }

  /* dump transaction */
  if (tran_index <= 0)
    {
      /* simple code without signal, setjmp, longjmp
       */

      p_stream = rsql_popen (rsql_Pager_cmd, rsql_Output_fp);

      fprintf (p_stream, rsql_get_message (RSQL_KILLTRAN_TITLE_TEXT));
      for (i = 0; i < info->num_trans; i++)
        {
          fprintf (p_stream, rsql_get_message (RSQL_KILLTRAN_FORMAT),
                   info->tran[i].tran_index,
                   tran_get_tranlist_state_name (info->tran[i].state),
                   info->tran[i].db_user, info->tran[i].host_name,
                   info->tran[i].process_id, info->tran[i].program_name);
        }

      rsql_pclose (p_stream, rsql_Output_fp);
    }
  else
    {
      /* kill transaction */
      for (i = 0; i < info->num_trans; i++)
        {
          if (info->tran[i].tran_index == tran_index)
            {
              fprintf (rsql_Output_fp, rsql_get_message (RSQL_KILLTRAN_TITLE_TEXT));
              fprintf (rsql_Output_fp,
                       rsql_get_message (RSQL_KILLTRAN_FORMAT),
                       info->tran[i].tran_index,
                       tran_get_tranlist_state_name (info->tran[i].state),
                       info->tran[i].db_user, info->tran[i].host_name,
                       info->tran[i].process_id, info->tran[i].program_name);

              if (thread_kill_tran_index (info->tran[i].tran_index,
                                          info->tran[i].db_user,
                                          info->tran[i].host_name, info->tran[i].process_id) == NO_ERROR)
                {
                  rsql_display_msg (rsql_get_message (RSQL_STAT_KILLTRAN_TEXT));
                }
              else
                {
                  rsql_display_msg (rsql_get_message (RSQL_STAT_KILLTRAN_FAIL_TEXT));
                }
              break;
            }
        }
    }

  if (info)
    {
      logtb_free_trans_info (info);
    }

  return;

error:
  nonscr_display_error ();
  if (info)
    {
      logtb_free_trans_info (info);
    }

  return;
}

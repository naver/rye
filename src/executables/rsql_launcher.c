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
 * rsql_launcher.c : rsql invocation program
 */

#ident "$Id$"

#include <stdio.h>
#include <stdarg.h>
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#else
#include "getopt.h"
#endif

#include "rsql.h"
#include "message_catalog.h"
#include "environment_variable.h"
#include "intl_support.h"
#include "utility.h"
#include "util_support.h"

typedef const char *(*RSQL_GET_MESSAGE) (int message_index);
typedef int (*RSQL) (const char *argv0, RSQL_ARGUMENT * rsql_arg);

static void utility_rsql_usage (void);

/*
 * utility_rsql_usage() - display rsql usage
 */
static void
utility_rsql_usage (void)
{
  DSO_HANDLE util_sa_library;
  RSQL_GET_MESSAGE rsql_get_message;
  const char *message;
  DSO_HANDLE symbol = NULL;

  utility_load_library (&util_sa_library, LIB_UTIL_SA_NAME);
  if (util_sa_library == NULL)
    {
      utility_load_print_error (stderr);
      return;
    }
  utility_load_symbol (util_sa_library, &symbol, "rsql_get_message");
  if (symbol == NULL)
    {
      utility_load_print_error (stderr);
      return;
    }
  rsql_get_message = (RSQL_GET_MESSAGE) symbol;
  message = (*rsql_get_message) (RSQL_MSG_USAGE);
  fprintf (stderr, message, PRODUCT_STRING, UTIL_RSQL_NAME);
}

/*
 * utility_rsql_print - display a version of this utility
 *
 * return:
 *
 * NOTE:
 */
static void
utility_rsql_print (int message_num, ...)
{
  typedef const char *(*GET_MESSAGE) (int message_index);

  DSO_HANDLE util_sa_library;
  DSO_HANDLE symbol;
  GET_MESSAGE get_message_fn;

  utility_load_library (&util_sa_library, LIB_UTIL_SA_NAME);
  if (util_sa_library == NULL)
    {
      utility_load_print_error (stderr);
      return;
    }
  utility_load_symbol (util_sa_library, &symbol,
		       UTILITY_GENERIC_MSG_FUNC_NAME);
  if (symbol == NULL)
    {
      utility_load_print_error (stderr);
      return;
    }

  get_message_fn = symbol;

  {
    va_list ap;

    va_start (ap, message_num);
    vfprintf (stderr, get_message_fn (message_num), ap);
    va_end (ap);
  }
}

/*
 * main() - rsql main module.
 *   return: no return if no error,
 *           EXIT_FAILURE otherwise.
 */
int
main (int argc, char *argv[])
{
  char option_string[64];
  int error = 0;
  RSQL_ARGUMENT rsql_arg;
  DSO_HANDLE util_library;
  RSQL rsql;
  bool explicit_single_line = false;
  DSO_HANDLE symbol = NULL;

  GETOPT_LONG rsql_option[] = {
    {RSQL_SA_MODE_L, 0, 0, RSQL_SA_MODE_S},
    {RSQL_CS_MODE_L, 0, 0, RSQL_CS_MODE_S},
    {RSQL_USER_L, 1, 0, RSQL_USER_S},
    {RSQL_PASSWORD_L, 1, 0, RSQL_PASSWORD_S},
    {RSQL_ERROR_CONTINUE_L, 0, 0, RSQL_ERROR_CONTINUE_S},
    {RSQL_INPUT_FILE_L, 1, 0, RSQL_INPUT_FILE_S},
    {RSQL_OUTPUT_FILE_L, 1, 0, RSQL_OUTPUT_FILE_S},
    {RSQL_SINGLE_LINE_L, 0, 0, RSQL_SINGLE_LINE_S},
    {RSQL_COMMAND_L, 1, 0, RSQL_COMMAND_S},
    {RSQL_LINE_OUTPUT_L, 0, 0, RSQL_LINE_OUTPUT_S},
    {RSQL_NO_AUTO_COMMIT_L, 0, 0, RSQL_NO_AUTO_COMMIT_S},
    {RSQL_NO_PAGER_L, 0, 0, RSQL_NO_PAGER_S},
    {RSQL_NO_SINGLE_LINE_L, 0, 0, RSQL_NO_SINGLE_LINE_S},
    {RSQL_WRITE_ON_STANDBY_L, 0, 0, RSQL_WRITE_ON_STANDBY_S},
    {RSQL_STRING_WIDTH_L, 1, 0, RSQL_STRING_WIDTH_S},
    {RSQL_TIME_OFF_L, 0, 0, RSQL_TIME_OFF_S},
    {VERSION_L, 0, 0, VERSION_S},
    {0, 0, 0, 0}
  };

  memset (&rsql_arg, 0, sizeof (RSQL_ARGUMENT));
  rsql_arg.auto_commit = true;
  rsql_arg.single_line_execution = true;
  rsql_arg.string_width = 0;
  rsql_arg.groupid = NULL_GROUPID;
  rsql_arg.time_on = true;
  utility_make_getopt_optstring (rsql_option, option_string);

  while (1)
    {
      int option_index = 0;
      int option_key;

      option_key = getopt_long (argc, argv, option_string,
				rsql_option, &option_index);
      if (option_key == -1)
	{
	  break;
	}

      switch (option_key)
	{
	case RSQL_SA_MODE_S:
	  rsql_arg.sa_mode = true;
	  break;

	case RSQL_CS_MODE_S:
	  rsql_arg.cs_mode = true;
	  break;

	case RSQL_USER_S:
	  rsql_arg.user_name = optarg;
	  break;

	case RSQL_PASSWORD_S:
	  rsql_arg.passwd = optarg;
	  break;

	case RSQL_ERROR_CONTINUE_S:
	  rsql_arg.continue_on_error = true;
	  break;

	case RSQL_INPUT_FILE_S:
	  rsql_arg.in_file_name = optarg;
	  break;

	case RSQL_OUTPUT_FILE_S:
	  rsql_arg.out_file_name = optarg;
	  break;

	case RSQL_SINGLE_LINE_S:
	  explicit_single_line = true;
	  break;

	case RSQL_NO_SINGLE_LINE_S:
	  rsql_arg.single_line_execution = false;
	  break;

	case RSQL_COMMAND_S:
	  rsql_arg.command = optarg;
	  break;

	case RSQL_LINE_OUTPUT_S:
	  rsql_arg.line_output = true;
	  break;

	case RSQL_NO_AUTO_COMMIT_S:
	  rsql_arg.auto_commit = false;
	  break;

	case RSQL_NO_PAGER_S:
	  rsql_arg.nopager = true;
	  break;

	case RSQL_WRITE_ON_STANDBY_S:
	  rsql_arg.write_on_standby = true;
	  break;

	case RSQL_TIME_OFF_S:
	  rsql_arg.time_on = false;
	  break;

	case RSQL_STRING_WIDTH_S:
	  {
	    int string_width = 0, result;

	    result = parse_int (&string_width, optarg, 10);

	    if (result != 0 || string_width < 0)
	      {
		goto print_usage;
	      }

	    rsql_arg.string_width = string_width;
	  }
	  break;

	case VERSION_S:
	  utility_rsql_print (MSGCAT_UTIL_GENERIC_VERSION, UTIL_RSQL_NAME,
			      PRODUCT_STRING);
	  goto exit_on_end;

	default:
	  goto print_usage;
	}
    }

  if (argc - optind == 1)
    {
      rsql_arg.db_name = argv[optind];
    }
  else if (argc > optind)
    {
      utility_rsql_print (MSGCAT_UTIL_GENERIC_ARGS_OVER, argv[optind + 1]);
      goto print_usage;
    }
  else
    {
      assert (rsql_arg.db_name == NULL);

#if 0				/* unused */
      utility_rsql_print (MSGCAT_UTIL_GENERIC_MISS_DBNAME);
      goto print_usage;
#endif
    }

  if ((rsql_arg.sa_mode || rsql_arg.write_on_standby)
      && (rsql_arg.user_name == NULL
	  || strcasecmp (rsql_arg.user_name, "DBA")))
    {
      /* sa_mode, write_on_standby is allowed only to DBA */
      goto print_usage;
    }

  if (rsql_arg.sa_mode && rsql_arg.cs_mode)
    {
      /* Don't allow both at once. */
      goto print_usage;
    }
  else if (explicit_single_line && rsql_arg.single_line_execution == false)
    {
      /* Don't allow both at once. */
      goto print_usage;
    }
  else if (rsql_arg.sa_mode)
    {
      utility_load_library (&util_library, LIB_UTIL_SA_NAME);
    }
  else
    {
      utility_load_library (&util_library, LIB_UTIL_CS_NAME);
    }

  if (util_library == NULL)
    {
      utility_load_print_error (stderr);
      goto exit_on_error;
    }

  utility_load_symbol (util_library, &symbol, "rsql");
  if (symbol == NULL)
    {
      utility_load_print_error (stderr);
      goto exit_on_error;
    }

  rsql = (RSQL) symbol;
  error = (*rsql) (argv[0], &rsql_arg);

exit_on_end:

  return error;

print_usage:
  utility_rsql_usage ();
  error = EXIT_FAILURE;
  goto exit_on_end;

exit_on_error:
  error = ER_GENERIC_ERROR;
  goto exit_on_end;
}

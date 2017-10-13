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
 * broker_log_converter.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#else
#include "getopt.h"
#endif

#include "cas_common.h"
#include "cas_cci.h"
#include "broker_log_util.h"

static int get_args (int argc, char *argv[]);
static int open_file (char *infilename, char *outfilename, FILE ** infp,
		      FILE ** outfp);
static int log_converter (FILE * infp, FILE * outfp);
static void close_file (FILE * infp, FILE * outfp);
static int log_bind_value (const char *str, int bind_len, int lineno,
			   FILE * outfp);

static char add_query_info = 0;
static char add_query_id = 0;
static char *infilename = NULL;

int
main (int argc, char *argv[])
{
  int start_arg;
  char *outfilename = NULL;
  FILE *outfp, *infp;
  int res;

  if ((start_arg = get_args (argc, argv)) < 0)
    {
    return -1;
    }

  infilename = argv[start_arg];
  if (start_arg + 1 <= argc)
    {
    outfilename = argv[start_arg + 1];
    }

  if (open_file (infilename, outfilename, &infp, &outfp) < 0)
    {
    return -1;
    }

  res = log_converter (infp, outfp);

  close_file (infp, outfp);

  return res;
}

static int
log_converter (FILE * infp, FILE * outfp)
{
  char *linebuf;
  char query_flag = 0;
  int lineno = 0;
  const char *msg_p;
  T_STRING *linebuf_tstr = NULL;
  int execute_flag = 0;
  int prepare_flag = 0;
  char in_execute = 0;
  int exec_h_id = 0;
  int bind_len = 0;
  int query_id = 0;

  linebuf_tstr = t_string_make (1000);
  if (linebuf_tstr == NULL)
    {
      fprintf (stderr, "malloc error\n");
      goto error;
    }

  while (1)
    {
      if (ut_get_line (infp, linebuf_tstr, NULL, NULL) < 0)
	{
	  fprintf (stderr, "malloc error\n");
	  goto error;
	}
      if (t_string_len (linebuf_tstr) <= 0)
	break;
      linebuf = t_string_str (linebuf_tstr);
      lineno++;

      if (linebuf[strlen (linebuf) - 1] == '\n')
	linebuf[strlen (linebuf) - 1] = '\0';

      if (is_cas_log (linebuf))
	{
	  if (query_flag)
	    {
	      fprintf (outfp, "\n");
	      fprintf (outfp, "P %d %d\n", exec_h_id, prepare_flag);
	    }
	  query_flag = 0;

	  msg_p = get_msg_start_ptr (linebuf);
	  if (strncmp (msg_p, "execute", 7) == 0)
	    {
	      msg_p = ut_get_execute_type (msg_p, &prepare_flag,
					   &execute_flag);
	      if (msg_p == NULL)
		{
		  in_execute = 0;
		  continue;
		}
	      if (strncmp (msg_p, "srv_h_id ", 9) == 0)
		{
		  char *endp;
		  int result = 0;

		  in_execute = 1;
		  msg_p += 9;

		  result = str_to_int32 (&exec_h_id, &endp, msg_p, 10);
		  if (result != 0)
		    {
		      in_execute = 0;
		      continue;
		    }
		  msg_p = endp + 1;

		  fprintf (outfp, "Q ");
		  if (add_query_info == 1 && prepare_flag != 0x40)
		    {
		      fprintf (outfp, "/* %s */ ", infilename);
		    }
		  if (add_query_id == 1)
		    {
		      fprintf (outfp, "/* QUERY_ID %d */ ", query_id++);
		    }

		  fprintf (outfp, "%s%c", msg_p, CAS_RUN_NEW_LINE_CHAR);
		  query_flag = 1;
		}
	      else
		{
		  if (in_execute == 1)
		    {
		      fprintf (outfp, "E %d %d\n", exec_h_id, execute_flag);
		      fprintf (outfp, "C %d\n", exec_h_id);
		      fprintf (outfp, "T\n");
		    }
		  in_execute = 0;
		}
	    }
	  else if (strncmp (msg_p, "bind ", 5) == 0)
	    {
	      bind_len = t_string_bind_len (linebuf_tstr);
	      if (log_bind_value (msg_p, bind_len, lineno, outfp) < 0)
		goto error;
	    }
	}
      else if (query_flag)
	{
	  fprintf (outfp, "%s%c ", linebuf, CAS_RUN_NEW_LINE_CHAR);
	}
    }

  RYE_FREE_MEM (linebuf_tstr);
  return 0;

error:
  RYE_FREE_MEM (linebuf_tstr);
  return -1;
}

static int
open_file (char *infilename, char *outfilename, FILE ** infp, FILE ** outfp)
{
  if (strcmp (infilename, "-") == 0)
    {
      *infp = stdin;
    }
  else
    {
      *infp = fopen (infilename, "r");
      if (*infp == NULL)
	{
	  fprintf (stderr, "fopen error[%s]\n", infilename);
	  return -1;
	}
    }

  if (outfilename == NULL)
    {
      *outfp = stdout;
    }
  else
    {
      *outfp = fopen (outfilename, "w");
      if (*outfp == NULL)
	{
	  fprintf (stderr, "fopen error[%s]\n", outfilename);
	  return -1;
	}
    }

  return 0;
}

static void
close_file (FILE * infp, FILE * outfp)
{
  if (infp != stdin)
    fclose (infp);

  fflush (outfp);
  if (outfp != stdout)
    fclose (outfp);
}

typedef struct
{
  const char *typestr;
  int u_type;
} T_TYPE_TABLE;

static T_TYPE_TABLE typetable[] = {
  {"NULL", CCI_TYPE_NULL},
  {"VARCHAR", CCI_TYPE_VARCHAR},
  {"VARBINARY", CCI_TYPE_VARBIT},
  {"NUMERIC", CCI_TYPE_NUMERIC},
  {"BIGINT", CCI_TYPE_BIGINT},
  {"INT", CCI_TYPE_INT},
  {"DOUBLE", CCI_TYPE_DOUBLE},
  {"DATE", CCI_TYPE_DATE},
  {"TIME", CCI_TYPE_TIME},
  {"DATETIME", CCI_TYPE_DATETIME},
  {NULL, CCI_TYPE_NULL}
};

static int
log_bind_value (const char *str, int bind_len, int lineno, FILE * outfp)
{
  char *p, *q, *r;
  const char *value_p;
  int type;
  int i;
  int typetable_size = sizeof (typetable) / sizeof (T_TYPE_TABLE);

  p = strchr (str, ':');
  if (p == NULL)
    {
      fprintf (stderr, "log error [line:%d]\n", lineno);
      return -1;
    }
  p += 2;
  q = strchr (p, ' ');
  if (q == NULL)
    {
      if (strcmp (p, "NULL") == 0)
	{
	  value_p = "";
	}
      else
	{
	  fprintf (stderr, "log error [line:%d]\n", lineno);
	  return -1;
	}
    }
  else
    {
      if (bind_len > 0)
	{
	  r = strchr (q, ')');
	  if (r == NULL)
	    {
	      fprintf (stderr, "log error [line:%d]\n", lineno);
	      return -1;
	    }
	  *q = '\0';
	  *r = '\0';
	  value_p = r + 1;
	}
      else
	{
	  *q = '\0';
	  value_p = q + 1;
	}
    }

  type = -1;
  for (i = 0; i < typetable_size && typetable[i].typestr != NULL; i++)
    {
      if (strcmp (p, typetable[i].typestr) == 0)
	{
	  type = typetable[i].u_type;
	  break;
	}
    }

  if (type < 0)
    {
      fprintf (stderr, "log error [line:%d]\n", lineno);
      return -1;
    }

  if (bind_len > 0)
    {
      fprintf (outfp, "B %d %d ", type, bind_len);
      if (bind_len > 1)
	{
	  fwrite (value_p, bind_len - 1, 1, outfp);
	}
      fwrite ("\n", 1, 1, outfp);
    }
  else
    {
      fprintf (outfp, "B %d %s\n", type, value_p);
    }
  return 0;
}

static int
get_args (int argc, char *argv[])
{
  int c;

  while ((c = getopt (argc, argv, "iq")) != EOF)
    {
      switch (c)
	{
	case 'q':
	  add_query_info = 1;
	  break;
	case 'i':
	  add_query_id = 1;
	  break;
	default:
	  goto usage;
	}
    }

  if (optind + 1 >= argc)
    goto usage;

  return optind;

usage:
  fprintf (stderr,
	   "usage : %s [OPTION] infile outfile\n"
	   "\n"
	   "valid options:\n"
	   "  -i   add a unique id to each query as a comment.\n", argv[0]);
  return -1;
}

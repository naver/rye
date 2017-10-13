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
 * stack_dump.c - call stack dump
 */

#ident "$Id$"

static int er_resolve_function_name (const void *address,
				     const char *lib_file_name, char *buffer,
				     int buffer_size);

#if __WORDSIZE == 32

#include <stdio.h>
#include <string.h>

#include <ucontext.h>
#include <dlfcn.h>

#include "error_code.h"
#include "memory_hash.h"
#include "stack_dump.h"

#define PEEK_DATA(addr)	(*(size_t *)(addr))
#define MAXARGS		6
#define BUFFER_SIZE     1024

/*
 * er_dump_call_stack - dump call stack
 *   return:
 *   outfp(in):
 */
void
er_dump_call_stack (FILE * outfp)
{
  ucontext_t ucp;
  size_t frame_pointer_addr, next_frame_pointer_addr;
  size_t return_addr, arg;
  int i, nargs;
  Dl_info dl_info;
  const char *func_name_p;
  const void *func_addr_p = NULL;
  char buffer[BUFFER_SIZE];

  if (getcontext (&ucp) < 0)
    {
      return;
    }

  return_addr = ucp.uc_mcontext.gregs[REG_EIP];
  frame_pointer_addr = ucp.uc_mcontext.gregs[REG_EBP];

  while (frame_pointer_addr)
    {
      if (dladdr ((size_t *) return_addr, &dl_info) == 0)
	{
	  break;
	}

      if (dl_info.dli_fbase >= (const void *) 0x40000000)
	{
	  func_addr_p = (void *) ((size_t) ((const char *) return_addr) -
				  (size_t) dl_info.dli_fbase);
	}
      else
	{
	  func_addr_p = (void *) return_addr;
	}

      if (dl_info.dli_sname)
	{
	  func_name_p = dl_info.dli_sname;
	}
      else
	{
	  if (er_resolve_function_name (func_addr_p, dl_info.dli_fname,
					buffer, sizeof (buffer)) == NO_ERROR)
	    {
	      func_name_p = buffer;
	    }
	  else
	    {
	      func_name_p = "???";
	    }
	}

      fprintf (outfp, "%s(%p): %s", dl_info.dli_fname, func_addr_p,
	       func_name_p);

      next_frame_pointer_addr = PEEK_DATA (frame_pointer_addr);
      nargs = (next_frame_pointer_addr - frame_pointer_addr - 8) / 4;
      if (nargs > MAXARGS)
	{
	  nargs = MAXARGS;
	}

      fprintf (outfp, " (");
      if (nargs > 0)
	{
	  for (i = 1; i <= nargs; i++)
	    {
	      arg = PEEK_DATA (frame_pointer_addr + 4 * (i + 1));
	      fprintf (outfp, "%x", arg);
	      if (i < nargs)
		{
		  fprintf (outfp, ", ");
		}
	    }
	}
      fprintf (outfp, ")\n");

      if (next_frame_pointer_addr == 0)
	{
	  break;
	}

      return_addr = PEEK_DATA (frame_pointer_addr + 4);
      frame_pointer_addr = next_frame_pointer_addr;
    }

  fflush (outfp);
}

#else /* __WORDSIZE == 32 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include <execinfo.h>

#include "error_code.h"
#include "memory_hash.h"
#include "stack_dump.h"

#define MAX_TRACE       32
#define BUFFER_SIZE     1024

/*
 * er_dump_call_stack - dump call stack
 *   return:
 *   outfp(in):
 */
void
er_dump_call_stack (FILE * outfp)
{
  void *return_addr[MAX_TRACE];
  int i, trace_count;
  Dl_info dl_info;
  const char *func_name_p;
  const void *func_addr_p;
  char buffer[BUFFER_SIZE];

  trace_count = backtrace (return_addr, MAX_TRACE);

  for (i = 0; i < trace_count; i++)
    {
      if (dladdr (return_addr[i], &dl_info) == 0)
	{
	  break;
	}

      if (dl_info.dli_fbase >= (const void *) 0x40000000)
	{
	  func_addr_p = (void *) ((size_t) ((const char *) return_addr[i]) -
				  (size_t) dl_info.dli_fbase);
	}
      else
	{
	  func_addr_p = return_addr[i];
	}

      if (dl_info.dli_sname)
	{
	  func_name_p = dl_info.dli_sname;
	}
      else
	{
	  if (er_resolve_function_name (func_addr_p, dl_info.dli_fname,
					buffer, sizeof (buffer)) == NO_ERROR)
	    {
	      func_name_p = buffer;
	    }
	  else
	    {
	      func_name_p = "???";
	    }
	}

      fprintf (outfp, "%s(%p): %s\n", dl_info.dli_fname, func_addr_p,
	       func_name_p);
    }

  fflush (outfp);
}
#endif /* __WORDSIZE == 32 */

MHT_TABLE *fname_table;

static int
er_resolve_function_name (const void *address, const char *lib_file_name_p,
			  char *buffer, int buffer_size)
{
  FILE *output;
  char cmd_line[BUFFER_SIZE];
  char *func_name_p, *pos;
  char buf[BUFFER_SIZE], *key, *data;

  snprintf (buf, BUFFER_SIZE, "%p%s", address, lib_file_name_p);
  data = mht_get (fname_table, buf);
  if (data != NULL)
    {
      snprintf (buffer, buffer_size, data);
      return NO_ERROR;
    }

  snprintf (cmd_line, sizeof (cmd_line),
	    "addr2line -f -C -e %s %p 2>/dev/null", lib_file_name_p, address);

  output = popen (cmd_line, "r");
  if (!output)
    {
      return ER_FAILED;
    }

  func_name_p = fgets (buffer, buffer_size - 1, output);
  if (!func_name_p || !func_name_p[0])
    {
      pclose (output);
      return ER_FAILED;
    }

  pos = strchr (func_name_p, '\n');
  if (pos)
    {
      pos[0] = '\0';
    }

  pclose (output);

  key = strdup (buf);
  if (key == NULL)
    {
      return ER_FAILED;
    }

  data = strdup (func_name_p);
  if (data == NULL)
    {
      free (key);
      return ER_FAILED;
    }

  if (mht_put (fname_table, key, data) == NULL)
    {
      free (key);
      free (data);
      return ER_FAILED;
    }
  return NO_ERROR;
}

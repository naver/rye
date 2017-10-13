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
 * server.c - server main
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/resource.h>
#include <unistd.h>
#include <limits.h>

#include "porting.h"
#include "system_parameter.h"
#include "connection_error.h"
#include "network.h"
#include "environment_variable.h"
#include "boot_sr.h"
#include "perf_monitor.h"
#include "util_func.h"
#include "tcp.h"
#include "heartbeat.h"

static void register_fatal_signal_handler (int signo);
static void crash_handler (int signo, siginfo_t * siginfo, void *dummyp);

static const char *database_name = "";

static char executable_path[PATH_MAX];

/*
 * unmask_signal(): unmask the given signal
 *
 *   returns: 0 for SUCCESS, -1 for otherwise
 *   signo(IN): signo to handle
 *
 */

static int
unmask_signal (int signo)
{
  sigset_t sigset;

  sigemptyset (&sigset);
  sigaddset (&sigset, signo);
  return sigprocmask (SIG_UNBLOCK, &sigset, NULL);
}

/*
 * register_fatal_signal_hander(): register the handler of the given signal
 *
 *   returns: none
 *   signo(IN): signo to handle
 *
 */

static void
register_fatal_signal_handler (int signo)
{
  struct sigaction act;

  act.sa_handler = NULL;
  act.sa_sigaction = crash_handler;
  sigemptyset (&act.sa_mask);
  act.sa_flags = 0;
  act.sa_flags |= SA_SIGINFO;
  sigaction (signo, &act, NULL);
}

/*
 * crash_handler(): kill the server and spawn the new server process
 *
 *   returns: none
 *   signo(IN): signo to handle
 *   siginfo(IN): siginfo struct
 *   dummyp(IN): this argument will not be used,
 *               but remains to cope with its function prototype.
 *
 */

static void
crash_handler (int signo, siginfo_t * siginfo, UNUSED_ARG void *dummyp)
{
  int pid;

  if (signo != SIGABRT && siginfo != NULL && siginfo->si_code <= 0)
    {
      register_fatal_signal_handler (signo);
      return;
    }

  if (os_set_signal_handler (signo, SIG_DFL) == SIG_ERR)
    {
      return;
    }

  if (!BO_IS_SERVER_RESTARTED ()
      || !prm_get_bool_value (PRM_ID_AUTO_RESTART_SERVER))
    {
      return;
    }

  pid = fork ();
  if (pid == 0)
    {
      char err_log[PATH_MAX];
      int ppid;
      int fd, fd_max;

      fd_max = css_get_max_socket_fds ();

      for (fd = 3; fd < fd_max; fd++)
	{
	  close (fd);
	}

      ppid = getppid ();
      while (1)
	{
	  if (kill (ppid, 0) < 0)
	    {
	      break;
	    }
	  sleep (1);
	}

      unmask_signal (signo);

      if (prm_get_string_value (PRM_ID_ER_LOG_FILE) != NULL)
	{
	  snprintf (err_log, PATH_MAX, "%s.%d",
		    prm_get_string_value (PRM_ID_ER_LOG_FILE), ppid);
	  rename (prm_get_string_value (PRM_ID_ER_LOG_FILE), err_log);
	}

      execl (executable_path, executable_path, database_name, NULL);
      exit (0);
    }
}

/*
 * main(): server's main function
 *
 *   returns: 0 for SUCCESS, non-zero for ERROR
 *
 */
int
main (int argc, char **argv)
{
  char *binary_name;
  int ret_val = 0;
  sigset_t sigurg_mask;

  register_fatal_signal_handler (SIGABRT);
  register_fatal_signal_handler (SIGILL);
  register_fatal_signal_handler (SIGFPE);
  register_fatal_signal_handler (SIGBUS);
  register_fatal_signal_handler (SIGSEGV);
  register_fatal_signal_handler (SIGSYS);

  /* Block SIGURG signal except oob-handler thread */
  sigemptyset (&sigurg_mask);
  sigaddset (&sigurg_mask, SIGURG);
  sigprocmask (SIG_BLOCK, &sigurg_mask, NULL);

  if (argc < 2)
    {
      PRINT_AND_LOG_ERR_MSG ("Usage: server databasename\n");
      return 1;
    }

  fprintf (stdout, "\nThis may take a long time depending on the amount "
	   "of recovery works to do.\n");

  argv[0] = UTIL_SERVER_NAME;
  /* save executable path */
  binary_name = basename (argv[0]);
  (void) envvar_bindir_file (executable_path, PATH_MAX, binary_name);
  /* save database name */
  database_name = argv[1];

  hb_set_exec_path (executable_path);
  hb_set_argv (argv);

  /* create a new session */
  setsid ();

  ret_val = net_server_start (database_name);

  return ret_val;
}

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
 * broker_monitor.c -
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#else
#include "getopt.h"
#endif

#include <curses.h>

#include <sys/types.h>
#include <regex.h>
#include <sys/time.h>

#include "porting.h"
#include "cas_common.h"
#include "broker_config.h"
#include "broker_shm.h"
#include "broker_util.h"
#include "porting.h"
#include "cas_util.h"
#include "util_func.h"
#include "connection_defs.h"
#include "language_support.h"
#include "broker_admin_pub.h"

#define		DEFAULT_CHECK_PERIOD		300	/* seconds */
#define		MAX_APPL_NUM		100

#define         FIELD_DELIMITER          ' '

#define         FIELD_WIDTH_BROKER_NAME 20
#define         FIELD_WIDTH_AS_ID       10

#define         BROKER_MONITOR_FLAG_MASK     0x01
#define         UNUSABLE_DATABASES_FLAG_MASK 0x20

typedef enum
{
  FIELD_BROKER_NAME = 0,
  FIELD_PID,
  FIELD_PSIZE,
  FIELD_PORT,
  FIELD_APPL_SERVER_NUM_TOTAL,
  FIELD_APPL_SERVER_NUM_CLIENT_WAIT,
  FIELD_APPL_SERVER_NUM_BUSY,
  FIELD_APPL_SERVER_NUM_CLIENT_WAIT_IN_SEC,
  FIELD_APPL_SERVER_NUM_BUSY_IN_SEC,
  FIELD_JOB_QUEUE_ID,
  FIELD_THREAD,			/* = 10 */
  FIELD_CPU_USAGE,
  FIELD_CPU_TIME,
  FIELD_TPS,
  FIELD_QPS,
  FIELD_NUM_OF_SELECT_QUERIES,
  FIELD_NUM_OF_INSERT_QUERIES,
  FIELD_NUM_OF_UPDATE_QUERIES,
  FIELD_NUM_OF_DELETE_QUERIES,
  FIELD_NUM_OF_OTHERS_QUERIES,
  FIELD_LONG_TRANSACTION,	/* = 20 */
  FIELD_LONG_QUERY,
  FIELD_ERROR_QUERIES,
  FIELD_UNIQUE_ERROR_QUERIES,
  FIELD_CANCELED,
  FIELD_ACCESS_MODE,
  FIELD_SQL_LOG,
  FIELD_NUMBER_OF_CONNECTION,
  FIELD_ID,
  FIELD_LQS,
  FIELD_STATUS,			/* = 30 */
  FIELD_LAST_ACCESS_TIME,
  FIELD_DB_NAME,
  FIELD_HOST,
  FIELD_LAST_CONNECT_TIME,
  FIELD_CLIENT_IP,
  FIELD_CLIENT_VERSION,
  FIELD_SQL_LOG_MODE,
  FIELD_TRANSACTION_STIME,
  FIELD_CONNECT,
  FIELD_RESTART,		/* = 40 */
  FIELD_REQUEST,
  FIELD_NUMBER_OF_CONNECTION_REJECTED,
  FIELD_UNUSABLE_DATABASES,
  FIELD_NUMBER_OF_BROKER_REQ,
  FIELD_LAST
} FIELD_NAME;

typedef enum
{
  FIELD_T_STRING = 0,
  FIELD_T_INT,
  FIELD_T_FLOAT,
  FIELD_T_UINT64,
  FIELD_T_INT64,
  FIELD_T_TIME
} FIELD_TYPE;

typedef enum
{
  FIELD_LEFT_ALIGN = 0,
  FIELD_RIGHT_ALIGN
} FIELD_ALIGN;

struct status_field
{
  FIELD_NAME name;
  unsigned int width;
  char title[256];
  FIELD_ALIGN align;
};

struct status_field fields[FIELD_LAST] = {
  {FIELD_BROKER_NAME, FIELD_WIDTH_BROKER_NAME, "NAME", FIELD_LEFT_ALIGN},
  {FIELD_PID, 5, "PID", FIELD_RIGHT_ALIGN},
  {FIELD_PSIZE, 7, "PSIZE", FIELD_RIGHT_ALIGN},
  {FIELD_PORT, 5, "PORT", FIELD_RIGHT_ALIGN},
  {FIELD_APPL_SERVER_NUM_TOTAL, 5, "", FIELD_RIGHT_ALIGN},
  {FIELD_APPL_SERVER_NUM_CLIENT_WAIT, 6, "W", FIELD_RIGHT_ALIGN},
  {FIELD_APPL_SERVER_NUM_BUSY, 6, "B", FIELD_RIGHT_ALIGN},
  {FIELD_APPL_SERVER_NUM_CLIENT_WAIT_IN_SEC, 6, "", FIELD_RIGHT_ALIGN},
  {FIELD_APPL_SERVER_NUM_BUSY_IN_SEC, 6, "", FIELD_RIGHT_ALIGN},
  {FIELD_JOB_QUEUE_ID, 4, "JQ", FIELD_RIGHT_ALIGN},
  {FIELD_THREAD, 4, "THR", FIELD_RIGHT_ALIGN},
  {FIELD_CPU_USAGE, 6, "CPU", FIELD_RIGHT_ALIGN},
  {FIELD_CPU_TIME, 6, "CTIME", FIELD_RIGHT_ALIGN},
  {FIELD_TPS, 20, "TPS", FIELD_RIGHT_ALIGN},
  {FIELD_QPS, 20, "QPS", FIELD_RIGHT_ALIGN},
  {FIELD_NUM_OF_SELECT_QUERIES, 8, "SELECT", FIELD_RIGHT_ALIGN},
  {FIELD_NUM_OF_INSERT_QUERIES, 8, "INSERT", FIELD_RIGHT_ALIGN},
  {FIELD_NUM_OF_UPDATE_QUERIES, 8, "UPDATE", FIELD_RIGHT_ALIGN},
  {FIELD_NUM_OF_DELETE_QUERIES, 8, "DELETE", FIELD_RIGHT_ALIGN},
  {FIELD_NUM_OF_OTHERS_QUERIES, 8, "OTHERS", FIELD_RIGHT_ALIGN},
  /*
   * 5: width of long transaction count
   * 1: delimiter(/)
   * 4: width of long transaction time
   * output example :
   *    [long transaction count]/[long transaction time]
   *    10/60.0
   * */
  {FIELD_LONG_TRANSACTION, 5 + 1 + 4, "LONG-T", FIELD_RIGHT_ALIGN},
  /*
   * 5: width of long query count
   * 1: delimiter(/)
   * 4: width of long query time
   * output example :
   *    [long query count]/[long query time]
   *    10/60.0
   * */
  {FIELD_LONG_QUERY, 5 + 1 + 4, "LONG-Q", FIELD_RIGHT_ALIGN},
  {FIELD_ERROR_QUERIES, 13, "ERR-Q", FIELD_RIGHT_ALIGN},
  {FIELD_UNIQUE_ERROR_QUERIES, 13, "UNIQUE-ERR-Q", FIELD_RIGHT_ALIGN},
  {FIELD_CANCELED, 10, "CANCELED", FIELD_RIGHT_ALIGN},
  {FIELD_ACCESS_MODE, 13, "ACCESS_MODE", FIELD_RIGHT_ALIGN},
  {FIELD_SQL_LOG, 9, "SQL_LOG", FIELD_RIGHT_ALIGN},
  {FIELD_NUMBER_OF_CONNECTION, 9, "#CONNECT", FIELD_RIGHT_ALIGN},
  {FIELD_ID, FIELD_WIDTH_AS_ID, "ID", FIELD_RIGHT_ALIGN},
  {FIELD_LQS, 10, "LQS", FIELD_RIGHT_ALIGN},
  {FIELD_STATUS, 12, "STATUS", FIELD_LEFT_ALIGN},
  {FIELD_LAST_ACCESS_TIME, 19, "LAST ACCESS TIME", FIELD_RIGHT_ALIGN},
  {FIELD_DB_NAME, 16, "DB", FIELD_RIGHT_ALIGN},
  {FIELD_HOST, 16, "HOST", FIELD_RIGHT_ALIGN},
  {FIELD_LAST_CONNECT_TIME, 19, "LAST CONNECT TIME", FIELD_RIGHT_ALIGN},
  {FIELD_CLIENT_IP, 15, "CLIENT IP", FIELD_RIGHT_ALIGN},
  {FIELD_CLIENT_VERSION, 19, "CLIENT VERSION", FIELD_RIGHT_ALIGN},
  {FIELD_SQL_LOG_MODE, 15, "SQL_LOG_MODE", FIELD_RIGHT_ALIGN},
  {FIELD_TRANSACTION_STIME, 19, "TRANSACTION STIME", FIELD_RIGHT_ALIGN},
  {FIELD_CONNECT, 9, "#CONNECT", FIELD_RIGHT_ALIGN},
  {FIELD_RESTART, 9, "#RESTART", FIELD_RIGHT_ALIGN},
  {FIELD_REQUEST, 20, "#REQUEST", FIELD_RIGHT_ALIGN},
  {FIELD_NUMBER_OF_CONNECTION_REJECTED, 9, "#REJECT", FIELD_RIGHT_ALIGN},
  {FIELD_UNUSABLE_DATABASES, 100, "UNUSABLE_DATABASES", FIELD_LEFT_ALIGN},
  {FIELD_NUMBER_OF_BROKER_REQ, 30, "#BROKER-REQ", FIELD_LEFT_ALIGN}
};

/* structure for appl monitoring */
typedef struct appl_monitoring_item APPL_MONITORING_ITEM;
struct appl_monitoring_item
{
  INT64 num_query_processed;
  INT64 num_long_query;
  INT64 qps;
  INT64 lqs;
};

/* structure for broker monitoring */
typedef struct br_monitoring_item BR_MONITORING_ITEM;
struct br_monitoring_item
{
  UINT64 num_tx;
  UINT64 num_qx;
  UINT64 num_lt;
  UINT64 num_lq;
  UINT64 num_eq;
  UINT64 num_eq_ui;
  UINT64 num_interrupt;
  UINT64 tps;
  UINT64 qps;
  UINT64 lts;
  UINT64 lqs;
  UINT64 eqs_ui;
  UINT64 eqs;
  UINT64 its;
  UINT64 num_select_query;
  UINT64 num_insert_query;
  UINT64 num_update_query;
  UINT64 num_delete_query;
  UINT64 num_others_query;
  UINT64 num_interrupts;
  UINT64 num_request;
  UINT64 num_connect;
  UINT64 num_connect_reject;
  UINT64 num_client_wait;
  UINT64 num_client_wait_nsec;
  UINT64 num_busy;
  UINT64 num_busy_nsec;
  UINT64 num_restart;
  UINT64 num_request_stmt;
  UINT64 num_request_stmt_in_pool;
  int num_appl_server;
  int num_br_reject;
  int num_br_connect_fail;
  int num_br_ping_req;
  int num_br_cancel_req;
};

static void str_to_screen (const char *msg);
static void print_newline ();
static int get_char (void);
static void print_usage (void);
static int get_args (int argc, char *argv[], char *br_vector);
static void print_job_queue (T_MAX_HEAP_NODE *);
static void ip2str (in_addr_t ip, char *ip_str);
static void time2str (const time_t t, char *str);

static void print_monitor_header ();
static void
set_monitor_items (BR_MONITORING_ITEM * mnt_items,
		   T_BROKER_INFO * br_info, T_SHM_APPL_SERVER * shm_appl);
static void
print_monitor_items (BR_MONITORING_ITEM * mnt_items_cur,
		     BR_MONITORING_ITEM * mnt_items_old,
		     double elapsed_time, T_BROKER_INFO * br_info_p,
		     T_SHM_APPL_SERVER * shm_appl);

static void
appl_info_display (T_SHM_APPL_SERVER * shm_appl,
		   T_APPL_SERVER_INFO * as_info_p, int br_index,
		   int as_index,
		   APPL_MONITORING_ITEM * appl_mnt_old, time_t current_time,
		   double elapsed_time);
static void mgmt_monitor (void);
static int appl_monitor (char *br_vector, double elapsed_time);
static int brief_monitor (char *br_vector, double elapsed_time);

#ifdef GET_PSINFO
static void time_format (int t, char *time_str);
static void get_cpu_usage_string (char *buf_p, float usage);
#endif
static void print_appl_header (bool use_pdh_flag);
static int print_title (char *buf_p, int buf_offset, FIELD_NAME name,
			const char *new_title_p);
static void print_value (FIELD_NAME name, const void *value, FIELD_TYPE type);
static const char *get_access_mode_string (T_ACCESS_MODE_VALUE mode,
					   int replica_only_flag);
static const char *get_sql_log_mode_string (T_SQL_LOG_MODE_VALUE mode);
static const char *get_status_string (T_APPL_SERVER_INFO * as_info_p,
				      char appl_server);

static int unusable_databases_monitor (void);

static T_SHM_BROKER *shm_Br;
static bool display_Job_queue = false;
static int refresh_Sec = 0;
static int last_Access_sec = 0;
static bool tty_Mode = false;
static bool full_Info_flag = false;
static int state_Interval = 1;
static char service_Filter_value = SERVICE_UNKNOWN;

static unsigned int monitor_Flag = 0;
static bool mgmt_Monitor_flag = false;

static void
str_to_screen (const char *msg)
{
  (void) addstr (msg);
}

static void
str_out (const char *fmt, ...)
{
  va_list ap;
  char out_buf[1024];

  va_start (ap, fmt);
  if (refresh_Sec > 0 && !tty_Mode)
    {
      vsprintf (out_buf, fmt, ap);
      str_to_screen (out_buf);
    }
  else
    {
      vprintf (fmt, ap);
    }
  va_end (ap);
}

static void
print_newline ()
{
  if (refresh_Sec > 0 && !tty_Mode)
    {
      clrtoeol ();
      str_to_screen ("\n");
    }
  else
    {
      printf ("\n");
    }
}

static int
get_char (void)
{
  return getch ();
}


int
broker_monitor (int argc, char **argv)
{
  int shm_key_br_gl;
  int err, i;
  char *br_vector;
  UNUSED_VAR WINDOW *win;
  time_t time_old, time_cur;
  double elapsed_time;

  if (argc > 1 && strcmp (argv[1], "--version") == 0)
    {
      fprintf (stderr, "VERSION %s\n", makestring (BUILD_NUMBER));
      return 3;
    }

  err = broker_config_read (NULL, NULL, &shm_key_br_gl, NULL, 0);
  if (err < 0)
    {
      return 2;
    }

  ut_cd_work_dir ();

  shm_Br = rye_shm_attach (shm_key_br_gl, RYE_SHM_TYPE_BROKER_GLOBAL, true);
  if (shm_Br == NULL)
    {
      /* This means we have to launch broker */
      fprintf (stdout, "master shared memory open error[0x%x]\r\n",
	       shm_key_br_gl);
      return 1;
    }
  if (shm_Br->num_broker < 1 || shm_Br->num_broker > MAX_BROKER_NUM)
    {
      PRINT_AND_LOG_ERR_MSG ("broker configuration error\r\n");
      return 3;
    }

  br_vector = (char *) malloc (shm_Br->num_broker);
  if (br_vector == NULL)
    {
      PRINT_AND_LOG_ERR_MSG ("memory allocation error\r\n");
      return 3;
    }
  for (i = 0; i < shm_Br->num_broker; i++)
    {
      br_vector[i] = 0;
    }

  if (get_args (argc, argv, br_vector) < 0)
    {
      free (br_vector);
      return 3;
    }

  if (refresh_Sec > 0 && !tty_Mode)
    {
      win = initscr ();
      timeout (refresh_Sec * 1000);
      noecho ();
    }

  (void) time (&time_old);
  time_old--;

  while (1)
    {
      (void) time (&time_cur);
      elapsed_time = difftime (time_cur, time_old);

      if (refresh_Sec > 0 && !tty_Mode)
	{
	  move (0, 0);
	  refresh ();
	}

      if (shm_Br == NULL || shm_Br->shm_header.status != RYE_SHM_VALID)
	{
	  if (shm_Br)
	    {
	      rye_shm_detach (shm_Br);
	    }

	  shm_Br = rye_shm_attach (shm_key_br_gl,
				   RYE_SHM_TYPE_BROKER_GLOBAL, true);
	}
      else
	{
	  if (mgmt_Monitor_flag)
	    {
	      mgmt_monitor ();
	    }
	  else if (monitor_Flag == 0)
	    {
	      appl_monitor (br_vector, elapsed_time);
	    }
	  else
	    {
	      if (monitor_Flag & BROKER_MONITOR_FLAG_MASK)
		{
		  if ((monitor_Flag & ~BROKER_MONITOR_FLAG_MASK) != 0)
		    {
		      print_newline ();
		    }
		  brief_monitor (br_vector, elapsed_time);
		}

	      if (monitor_Flag & UNUSABLE_DATABASES_FLAG_MASK)
		{
		  if ((monitor_Flag & ~UNUSABLE_DATABASES_FLAG_MASK) != 0)
		    {
		      print_newline ();
		    }
		  unusable_databases_monitor ();
		}
	    }
	}

      if (refresh_Sec > 0 && !tty_Mode)
	{
	  int in_ch = 0;

	  refresh ();
	  clrtobot ();
	  move (0, 0);
	  refresh ();
	  in_ch = get_char ();

	  if (in_ch == 'q')
	    {
	      break;
	    }
	  else if (in_ch == '' || in_ch == '\r' || in_ch == '\n' || in_ch == ' ')
	    {
	      clear ();
	      refresh ();
	    }
	}
      else if (refresh_Sec > 0)
	{
	  for (i = 0; i < 10; i++)
	    {
	      THREAD_SLEEP (refresh_Sec * 100);
	    }
	  fflush (stdout);
	}
      else
	{
	  break;
	}

      if (elapsed_time > 0)
	{
	  time_old = time_cur;
	}
    }				/* end of while(1) */

  if (shm_Br != NULL)
    {
      rye_shm_detach (shm_Br);
    }

  if (refresh_Sec > 0 && !tty_Mode)
    {
      endwin ();
    }

  free (br_vector);
  return 0;
}

static void
print_usage (void)
{
  printf
    ("broker_monitor [-b] [-m] [-q] [-t] [-s <sec>] [-u] [-f] [<expr>]\n");
  printf ("\t<expr> part of broker name or SERVICE=[ON|OFF]\n");
  printf ("\t-q display job queue\n");
  printf ("\t-u display unusable database server\n");
  printf ("\t-b brief mode (show broker info)\n");
  printf ("\t-m display mgmt broker info\n");
  printf ("\t-s refresh time in sec\n");
  printf ("\t-f full info\n");
}

static int
get_args (int argc, char *argv[], char *br_vector)
{
  int c, j;
  int status;
  bool br_name_opt_flag = false;
  regex_t re;

  char optchars[] = "hbqts:l:fmcSPu";

  display_Job_queue = false;
  refresh_Sec = 0;
  last_Access_sec = 0;
  full_Info_flag = false;
  state_Interval = 1;
  service_Filter_value = SERVICE_UNKNOWN;
  while ((c = getopt (argc, argv, optchars)) != EOF)
    {
      switch (c)
	{
	case 't':
	  tty_Mode = true;
	  break;
	case 'q':
	  display_Job_queue = true;
	  break;
	case 's':
	  refresh_Sec = atoi (optarg);
	  break;
	case 'b':
	  monitor_Flag |= BROKER_MONITOR_FLAG_MASK;
	  break;
	case 'l':
	  state_Interval = last_Access_sec = atoi (optarg);
	  if (state_Interval < 1)
	    {
	      state_Interval = 1;
	    }
	  break;
	case 'f':
	  full_Info_flag = true;
	  break;
	case 'u':
	  monitor_Flag |= UNUSABLE_DATABASES_FLAG_MASK;
	  break;
	case 'm':
	  mgmt_Monitor_flag = true;
	  break;
	case 'h':
	case '?':
	  print_usage ();
	  return -1;
	}
    }

  for (; optind < argc; optind++)
    {
      if (br_name_opt_flag == false)
	{
	  if (strncasecmp (argv[optind], "SERVICE=", strlen ("SERVICE=")) ==
	      0)
	    {
	      char *value_p;
	      value_p = argv[optind] + strlen ("SERVICE=");
	      if (strcasecmp (value_p, "ON") == 0)
		{
		  service_Filter_value = SERVICE_ON;
		  break;
		}
	      else if (strcasecmp (value_p, "OFF") == 0)
		{
		  service_Filter_value = SERVICE_OFF;
		  break;
		}
	      else
		{
		  print_usage ();
		  return -1;
		}
	    }
	}

      br_name_opt_flag = true;
      if (regcomp (&re, argv[optind], 0) != 0)
	{
	  fprintf (stderr, "%s\r\n", argv[optind]);
	  return -1;
	}
      for (j = 0; j < shm_Br->num_broker; j++)
	{
	  status = regexec (&re, shm_Br->br_info[j].name, 0, NULL, 0);
	  if (status == 0)
	    {
	      br_vector[j] = 1;
	    }
	}
      regfree (&re);
    }

  if (br_name_opt_flag == false)
    {
      for (j = 0; j < shm_Br->num_broker; j++)
	br_vector[j] = 1;
    }

  return 0;
}

static void
print_job_queue (T_MAX_HEAP_NODE * job_queue)
{
  T_MAX_HEAP_NODE item;
  char first_flag = 1;
  char outbuf[1024];

  while (1)
    {
      char ip_str[64];
      char time_str[64];

      if (max_heap_delete (job_queue, &item) < 0)
	break;

      if (first_flag)
	{
	  sprintf (outbuf, "%5s  %s%9s%13s%13s", "ID", "PRIORITY", "IP",
		   "TIME", "REQUEST");
	  str_out ("%s", outbuf);
	  print_newline ();
	  first_flag = 0;
	}

      ip2str (item.ip, ip_str);
      time2str (item.recv_time.tv_sec, time_str);
      sprintf (outbuf, "%5d%7d%17s%10s ",
	       item.id, item.priority, ip_str, time_str);
      str_out ("%s", outbuf);
      print_newline ();
    }
  if (!first_flag)
    print_newline ();
}

static void
ip2str (in_addr_t ip_addr, char *ip_str)
{
  const unsigned char *ip = (const unsigned char *) &ip_addr;
  sprintf (ip_str, "%d.%d.%d.%d", (unsigned char) ip[0],
	   (unsigned char) ip[1],
	   (unsigned char) ip[2], (unsigned char) ip[3]);
}

static void
time2str (const time_t t, char *str)
{
  struct tm s_tm;

  if (localtime_r (&t, &s_tm) == NULL)
    {
      *str = '\0';
      return;
    }
  sprintf (str, "%02d:%02d:%02d", s_tm.tm_hour, s_tm.tm_min, s_tm.tm_sec);
}

static void
appl_info_display (T_SHM_APPL_SERVER * shm_appl,
		   T_APPL_SERVER_INFO * as_info_p, int br_index,
		   int as_index,
		   APPL_MONITORING_ITEM * appl_mnt_old, time_t current_time,
		   double elapsed_time)
{
  UINT64 qps;
  UINT64 lqs;
//  int col_len;
  time_t tran_start_time;
  char ip_str[16];
  int as_id;
  int psize;
#ifdef GET_PSINFO
  char buf[256];
#endif

  as_id = as_index + 1;

  if (as_info_p->service_flag != SERVICE_ON)
    {
      return;
    }

  if (last_Access_sec > 0)
    {
      if (as_info_p->uts_status != UTS_STATUS_BUSY
	  || current_time - as_info_p->last_access_time < last_Access_sec)
	{
	  return;
	}

      if (as_info_p->uts_status == UTS_STATUS_BUSY
	  && IS_APPL_SERVER_TYPE_CAS (shm_Br->br_info[br_index].appl_server)
	  && as_info_p->con_status == CON_STATUS_OUT_TRAN)
	{
	  return;
	}
    }

//  col_len = 0;

  print_value (FIELD_ID, &as_id, FIELD_T_INT);
  print_value (FIELD_PID, &as_info_p->pid, FIELD_T_INT);
  if (elapsed_time > 0)
    {
      qps = (as_info_p->num_queries_processed -
	     appl_mnt_old->num_query_processed) / elapsed_time;
      lqs = (as_info_p->num_long_queries -
	     appl_mnt_old->num_long_query) / elapsed_time;
      appl_mnt_old->num_query_processed = as_info_p->num_queries_processed;
      appl_mnt_old->num_long_query = as_info_p->num_long_queries;
      appl_mnt_old->qps = qps;
      appl_mnt_old->lqs = lqs;
    }
  else
    {
      qps = appl_mnt_old->qps;
      lqs = appl_mnt_old->lqs;
    }

  print_value (FIELD_QPS, &qps, FIELD_T_UINT64);
  print_value (FIELD_LQS, &lqs, FIELD_T_UINT64);
  psize = (int) (os_get_mem_size (as_info_p->pid, MEM_VSIZE) / ONE_K);
  print_value (FIELD_PSIZE, &psize, FIELD_T_INT);
  print_value (FIELD_STATUS,
	       get_status_string (as_info_p,
				  shm_Br->br_info[br_index].appl_server),
	       FIELD_T_STRING);

#ifdef GET_PSINFO
  get_psinfo (as_info_p->pid, &proc_info);

  get_cpu_usage_string (buf, proc_info.pcpu);
  print_value (FIELD_CPU_USAGE, buf, FIELD_T_STRING);

  time_format (proc_info.cpu_time, time_str);
  print_value (FIELD_CPU_TIME, time_str, FIELD_T_STRING);
#endif

  if (full_Info_flag)
    {
      print_value (FIELD_LAST_ACCESS_TIME, &(as_info_p->last_access_time),
		   FIELD_T_TIME);
      if (as_info_p->database_name[0] != '\0')
	{
	  char hostname[MAX_NODE_INFO_STR_LEN];
	  prm_node_info_to_str (hostname, sizeof (hostname),
				&as_info_p->db_node);
	  print_value (FIELD_DB_NAME, as_info_p->database_name,
		       FIELD_T_STRING);
	  print_value (FIELD_HOST, hostname, FIELD_T_STRING);
	  print_value (FIELD_LAST_CONNECT_TIME,
		       &(as_info_p->last_connect_time), FIELD_T_TIME);
	}
      else
	{
	  print_value (FIELD_DB_NAME, "-", FIELD_T_STRING);
	  print_value (FIELD_HOST, "-", FIELD_T_STRING);
	  print_value (FIELD_LAST_CONNECT_TIME, "-", FIELD_T_STRING);
	}

      print_value (FIELD_CLIENT_IP,
		   ut_get_ipv4_string (ip_str, sizeof (ip_str),
				       as_info_p->cas_clt_ip_addr),
		   FIELD_T_STRING);
      print_value (FIELD_CLIENT_VERSION, as_info_p->client_version,
		   FIELD_T_STRING);

      if (as_info_p->cur_sql_log_mode != shm_appl->sql_log_mode)
	{
	  print_value (FIELD_SQL_LOG_MODE,
		       get_sql_log_mode_string (as_info_p->cur_sql_log_mode),
		       FIELD_T_STRING);
	}
      else
	{
	  print_value (FIELD_SQL_LOG_MODE, "-", FIELD_T_STRING);
	}

      tran_start_time = as_info_p->transaction_start_time;
      if (tran_start_time != (time_t) 0)
	{
	  print_value (FIELD_TRANSACTION_STIME, &tran_start_time,
		       FIELD_T_TIME);
	}
      else
	{
	  print_value (FIELD_TRANSACTION_STIME, "-", FIELD_T_STRING);
	}
      print_value (FIELD_CONNECT, &(as_info_p->num_connect_requests),
		   FIELD_T_INT);
      print_value (FIELD_RESTART, &(as_info_p->num_restarts), FIELD_T_INT);
    }
  print_newline ();
  if (as_info_p->uts_status == UTS_STATUS_BUSY)
    {
      str_out ("SQL: %s", as_info_p->log_msg);
      print_newline ();
    }
}

static void
local_mgmt_monitor (const T_SHM_LOCAL_MGMT_INFO * shm_info_p)
{
  int i;

  const char *indent1 = "\t";
  const char *indent2 = "\t\t";

  str_out ("%s", indent1);
  str_out ("REQUEST");
  print_newline ();

  str_out ("%s", indent2);
  str_out ("connect:%d", shm_info_p->connect_req_count);
  str_out ("%c", FIELD_DELIMITER);
  str_out ("ping:%d", shm_info_p->ping_req_count);
  str_out ("%c", FIELD_DELIMITER);
  str_out ("cancel:%d", shm_info_p->cancel_req_count);
  str_out ("%c", FIELD_DELIMITER);
  str_out ("admin_req:%d", shm_info_p->admin_req_count);
  str_out ("%c", FIELD_DELIMITER);
  str_out ("db_connect:%d(%d)",
	   shm_info_p->db_connect_success, shm_info_p->db_connect_fail);
  print_newline ();

  str_out ("%s", indent1);
  str_out ("JOB_QUEUE");
  print_newline ();

  str_out ("%s", indent2);
  str_out ("admin_req:%d", shm_info_p->admin_req_queue.num_job);
  print_newline ();

  str_out ("%s", indent1);
  str_out ("CHILD_PROCESS");
  print_newline ();

  for (i = 0; i < shm_info_p->num_child_process; i++)
    {
      str_out ("%s", indent2);
      str_out ("pid=%d cmd=%s", shm_info_p->child_process_info[i].pid,
	       shm_info_p->child_process_info[i].cmd);
      print_newline ();
    }

  if (shm_Br->num_shard_version_info > 0)
    {
      str_out ("%s", indent1);
      str_out ("shard version info");
      print_newline ();

      for (i = 0; i < shm_Br->num_shard_version_info; i++)
	{
	  char time_buf[256];
	  if (shm_Br->shard_version_info[i].sync_time > 0)
	    {
	      struct timeval time_val;
	      time_val.tv_sec = shm_Br->shard_version_info[i].sync_time;
	      time_val.tv_usec = 0;
	      (void) er_datetime (&time_val, time_buf, sizeof (time_buf));
	    }
	  else
	    {
	      strcpy (time_buf, "-");
	    }
	  str_out ("%s", indent2);
	  str_out ("local_dbname:%s shard_info_version:%ld (%s)",
		   shm_Br->shard_version_info[i].local_dbname,
		   shm_Br->shard_version_info[i].shard_info_ver, time_buf);
	  print_newline ();
	}
    }
}

static void
shard_mgmt_monitor (const T_SHM_SHARD_MGMT_INFO * shm_info_p)
{
  int i;
  const char *indent1 = "\t";
  const char *indent2 = "\t\t";

  str_out ("%s", indent1);
  str_out ("REQUEST");
  print_newline ();

  str_out ("%s", indent2);
  str_out ("shard_mgmt:%d ping:%d",
	   shm_info_p->mgmt_req_count, shm_info_p->ping_req_count);
  print_newline ();

  str_out ("%s", indent1);
  str_out ("JOB_QUEUE");
  print_newline ();

  str_out ("%s", indent2);
  str_out ("shard_info:%d admin_req:%d admin_wait:%d",
	   shm_info_p->get_info_req_queue.num_job,
	   shm_info_p->admin_req_queue.num_job,
	   shm_info_p->wait_job_req_queue.num_job);
  print_newline ();

  str_out ("%s", indent1);
  str_out ("MIGRATION");
  print_newline ();

  str_out ("%s", indent2);
  str_out ("scheduled:%d running:%d fail:%d migrator:%d",
	   shm_info_p->rbl_scheduled_count,
	   shm_info_p->rbl_running_count,
	   shm_info_p->rbl_fail_count, shm_info_p->running_migrator_count);
  print_newline ();
  str_out ("%s", indent2);
  str_out ("complete:%d shard_keys:%d avg_time:%.1f",
	   shm_info_p->rbl_complete_count,
	   shm_info_p->rbl_complete_shard_keys,
	   shm_info_p->rbl_complete_avg_time);
  print_newline ();

  str_out ("%s", indent1);
  str_out ("NODE");
  print_newline ();

  for (i = 0; i < shm_info_p->num_shard_node_info; i++)
    {
      const char *ha_mode_str;
      switch (shm_info_p->shard_node_info[i].ha_state)
	{
	case HA_STATE_FOR_DRIVER_MASTER:
	case HA_STATE_FOR_DRIVER_TO_BE_MASTER:
	  ha_mode_str = "master";
	  break;
	case HA_STATE_FOR_DRIVER_SLAVE:
	case HA_STATE_FOR_DRIVER_TO_BE_SLAVE:
	  ha_mode_str = "slave";
	  break;
	case HA_STATE_FOR_DRIVER_REPLICA:
	  ha_mode_str = "replica";
	  break;
	case HA_STATE_FOR_DRIVER_UNKNOWN:
	default:
	  ha_mode_str = "unknown";
	  break;

	}

      str_out ("%s", indent2);
      str_out ("id:%d %s:%d %s (%s:%s)",
	       shm_info_p->shard_node_info[i].node_id,
	       shm_info_p->shard_node_info[i].host_ip,
	       shm_info_p->shard_node_info[i].port,
	       shm_info_p->shard_node_info[i].local_dbname,
	       shm_info_p->shard_node_info[i].host_name, ha_mode_str);
      print_newline ();
    }
}

static void
mgmt_monitor ()
{
  int br_idx;
  T_BROKER_INFO *br_info_p = NULL;
  T_SHM_APPL_SERVER *shm_appl = NULL;

  str_out ("%KEY:%s", shm_Br->broker_key);
  print_newline ();
  for (br_idx = 0; br_idx < shm_Br->num_broker; br_idx++)
    {
      br_info_p = &shm_Br->br_info[br_idx];

      if (br_info_p->broker_type == NORMAL_BROKER)
	{
	  continue;
	}

      str_out ("%% %s", br_info_p->name);

      if (br_info_p->service_flag != SERVICE_ON)
	{
	  str_out ("%c%s", FIELD_DELIMITER, "OFF");
	  print_newline ();
	  print_newline ();
	  continue;
	}

      shm_appl = rye_shm_attach (br_info_p->appl_server_shm_key,
				 RYE_SHM_TYPE_BROKER_LOCAL, true);
      if (shm_appl == NULL)
	{
	  str_out ("%c%s", FIELD_DELIMITER, "shared memory open error");
	  print_newline ();
	  print_newline ();
	  continue;
	}

      str_out ("%cPID=%d", FIELD_DELIMITER, br_info_p->broker_pid);
      str_out ("%cPORT=%d", FIELD_DELIMITER, br_info_p->port);

      if (br_info_p->broker_type == SHARD_MGMT)
	{
	  str_out ("%cGLOBAL_DBNAME=%s",
		   FIELD_DELIMITER, br_info_p->shard_global_dbname);
	}

      print_newline ();

      if (br_info_p->broker_type == SHARD_MGMT)
	{
	  shard_mgmt_monitor (&shm_appl->info.shard_mgmt_info);
	}
      else
	{
	  local_mgmt_monitor (&shm_appl->info.local_mgmt_info);
	}

      print_newline ();
      rye_shm_detach (shm_appl);
    }
}

static int
appl_monitor (char *br_vector, double elapsed_time)
{
  T_MAX_HEAP_NODE job_queue[JOB_QUEUE_MAX_SIZE + 1];
  T_SHM_APPL_SERVER *shm_appl;
  int i, j, k, appl_offset;

  static APPL_MONITORING_ITEM *appl_mnt_olds = NULL;

  time_t current_time;
#ifdef GET_PSINFO
  T_PSINFO proc_info;
  char time_str[32];
#endif

  if (appl_mnt_olds == NULL)
    {
      int n = 0;
      for (i = 0; i < shm_Br->num_broker; i++)
	{
	  n += shm_Br->br_info[i].appl_server_max_num;
	}

      appl_mnt_olds =
	(APPL_MONITORING_ITEM *) calloc (sizeof (APPL_MONITORING_ITEM), n);
      if (appl_mnt_olds == NULL)
	{
	  return -1;
	}
      memset ((char *) appl_mnt_olds, 0, sizeof (APPL_MONITORING_ITEM) * n);
    }

  for (i = 0; i < shm_Br->num_broker; i++)
    {
      if (br_vector[i] == 0 ||
	  shm_Br->br_info[i].broker_type != NORMAL_BROKER)
	{
	  continue;
	}

      if (service_Filter_value != SERVICE_UNKNOWN
	  && service_Filter_value != shm_Br->br_info[i].service_flag)
	{
	  continue;
	}

      str_out ("%% %s", shm_Br->br_info[i].name);

      if (shm_Br->br_info[i].service_flag == SERVICE_ON)
	{
	  shm_appl = rye_shm_attach (shm_Br->br_info[i].appl_server_shm_key,
				     RYE_SHM_TYPE_BROKER_LOCAL, true);
	  if (shm_appl == NULL)
	    {
	      str_out ("%c%s", FIELD_DELIMITER, "shared memory open error");
	      print_newline ();
	    }
	  else
	    {
	      print_newline ();
	      print_appl_header (false);
	      current_time = time (NULL);

	      /* CAS INFORMATION DISPLAY */
	      appl_offset = 0;

	      for (k = 0; k < i; k++)
		{
		  appl_offset += shm_Br->br_info[k].appl_server_max_num;
		}
	      for (j = 0; j < shm_Br->br_info[i].appl_server_max_num; j++)
		{
		  appl_info_display (shm_appl, &(shm_appl->info.as_info[j]),
				     i, j, &(appl_mnt_olds[appl_offset + j]),
				     current_time, elapsed_time);
		}		/* CAS INFORMATION DISPLAY */

	      print_newline ();

	      if (display_Job_queue == true)
		{
		  print_job_queue (job_queue);
		}

	      if (shm_appl)
		{
		  rye_shm_detach (shm_appl);
		}
	    }
	}
      else
	{			/* service_flag == OFF */
	  str_out ("%c%s", FIELD_DELIMITER, "OFF");
	  print_newline ();
	  print_newline ();
	}
    }

  return 0;
}

static void
print_monitor_header ()
{
  char buf[LINE_MAX];
  int buf_offset = 0;
  int i;
  static unsigned int tty_print_header = 0;

  if (tty_Mode == true && (tty_print_header++ % 20 != 0)
      && (monitor_Flag & ~BROKER_MONITOR_FLAG_MASK) == 0)
    {
      return;
    }

  buf_offset = 0;

  buf_offset = print_title (buf, buf_offset, FIELD_BROKER_NAME, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_PID, NULL);
  if (full_Info_flag)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_PSIZE, NULL);
    }
  buf_offset = print_title (buf, buf_offset, FIELD_PORT, NULL);

  if (full_Info_flag)
    {
      char field_title_with_interval[256];

      buf_offset = print_title (buf, buf_offset,
				FIELD_APPL_SERVER_NUM_TOTAL, "AS(T");
      buf_offset = print_title (buf, buf_offset,
				FIELD_APPL_SERVER_NUM_CLIENT_WAIT, NULL);
      buf_offset = print_title (buf, buf_offset,
				FIELD_APPL_SERVER_NUM_BUSY, NULL);
      sprintf (field_title_with_interval, "%d%s", state_Interval, "s-W");
      buf_offset = print_title (buf, buf_offset,
				FIELD_APPL_SERVER_NUM_CLIENT_WAIT_IN_SEC,
				field_title_with_interval);
      sprintf (field_title_with_interval, "%d%s", state_Interval, "s-B)");
      buf_offset = print_title (buf, buf_offset,
				FIELD_APPL_SERVER_NUM_BUSY_IN_SEC,
				field_title_with_interval);
    }
  else
    {
      buf_offset = print_title (buf, buf_offset,
				FIELD_APPL_SERVER_NUM_TOTAL, "AS");
    }

  buf_offset = print_title (buf, buf_offset, FIELD_JOB_QUEUE_ID, NULL);

#ifdef GET_PSINFO
  buf_offset = print_title (buf, buf_offset, FIELD_THREAD, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_CPU_USAGE, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_CPU_TIME, NULL);
#endif

  buf_offset = print_title (buf, buf_offset, FIELD_TPS, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_QPS, NULL);
  if (full_Info_flag == false)
    {
      buf_offset =
	print_title (buf, buf_offset, FIELD_NUM_OF_SELECT_QUERIES, NULL);
      buf_offset =
	print_title (buf, buf_offset, FIELD_NUM_OF_INSERT_QUERIES, NULL);
      buf_offset =
	print_title (buf, buf_offset, FIELD_NUM_OF_UPDATE_QUERIES, NULL);
      buf_offset =
	print_title (buf, buf_offset, FIELD_NUM_OF_DELETE_QUERIES, NULL);
      buf_offset =
	print_title (buf, buf_offset, FIELD_NUM_OF_OTHERS_QUERIES, NULL);
    }

  buf_offset = print_title (buf, buf_offset, FIELD_LONG_TRANSACTION, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_LONG_QUERY, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_ERROR_QUERIES, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_UNIQUE_ERROR_QUERIES,
			    NULL);
  if (full_Info_flag)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_CANCELED, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_ACCESS_MODE, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_SQL_LOG, NULL);
    }

  buf_offset =
    print_title (buf, buf_offset, FIELD_NUMBER_OF_CONNECTION, NULL);
  buf_offset =
    print_title (buf, buf_offset, FIELD_NUMBER_OF_CONNECTION_REJECTED, NULL);
  if (full_Info_flag)
    {
      buf_offset = print_title (buf, buf_offset,
				FIELD_NUMBER_OF_BROKER_REQ, NULL);
    }

  str_out ("%s", buf);
  print_newline ();

  for (i = 0; i < buf_offset; i++)
    {
      str_out ("%s", "=");
    }
  print_newline ();

  return;
}

static void
set_monitor_items (BR_MONITORING_ITEM * mnt_items,
		   T_BROKER_INFO * br_info_p, T_SHM_APPL_SERVER * shm_appl)
{
  int i;
  BR_MONITORING_ITEM *mnt_item_p = NULL;
  T_APPL_SERVER_INFO *as_info_p = NULL;

  assert (shm_appl != NULL);

  mnt_item_p = mnt_items;

  mnt_item_p->num_appl_server = br_info_p->appl_server_num;

  mnt_item_p->num_br_reject = br_info_p->reject_client_count;
  mnt_item_p->num_br_connect_fail = br_info_p->connect_fail_count;
  mnt_item_p->num_br_ping_req = br_info_p->ping_req_count;
  mnt_item_p->num_br_cancel_req = br_info_p->cancel_req_count;

  for (i = 0; i < br_info_p->appl_server_max_num; i++)
    {
      as_info_p = &(shm_appl->info.as_info[i]);

      mnt_item_p->num_request += as_info_p->num_request;
      mnt_item_p->num_connect += as_info_p->num_connect_requests;
      mnt_item_p->num_connect_reject += as_info_p->num_connect_rejected;
      if (full_Info_flag && as_info_p->service_flag == ON)
	{
	  time_t cur_time = time (NULL);
	  bool time_expired =
	    (cur_time - as_info_p->last_access_time >= state_Interval);

	  if (as_info_p->uts_status == UTS_STATUS_BUSY
	      && as_info_p->con_status != CON_STATUS_OUT_TRAN)
	    {
	      if (as_info_p->log_msg[0] == '\0')
		{
		  mnt_item_p->num_client_wait++;
		  if (time_expired)
		    {
		      mnt_item_p->num_client_wait_nsec++;
		    }
		}
	      else
		{
		  mnt_item_p->num_busy++;
		  if (time_expired)
		    {
		      mnt_item_p->num_busy_nsec++;
		    }
		}
	    }
	}
      mnt_item_p->num_request += as_info_p->num_requests_received;
      mnt_item_p->num_tx += as_info_p->num_transactions_processed;
      mnt_item_p->num_qx += as_info_p->num_queries_processed;
      mnt_item_p->num_lt += as_info_p->num_long_transactions;
      mnt_item_p->num_lq += as_info_p->num_long_queries;
      mnt_item_p->num_eq += as_info_p->num_error_queries;
      mnt_item_p->num_eq_ui += as_info_p->num_unique_error_queries;
      mnt_item_p->num_interrupts += as_info_p->num_interrupts;
      mnt_item_p->num_select_query += as_info_p->num_select_queries;
      mnt_item_p->num_insert_query += as_info_p->num_insert_queries;
      mnt_item_p->num_update_query += as_info_p->num_update_queries;
      mnt_item_p->num_delete_query += as_info_p->num_delete_queries;
      mnt_item_p->
	num_others_query = (mnt_item_p->num_qx
			    - mnt_item_p->num_select_query
			    - mnt_item_p->num_insert_query
			    - mnt_item_p->num_update_query
			    - mnt_item_p->num_delete_query);
    }
}

static void
print_monitor_items (BR_MONITORING_ITEM * mnt_items_cur,
		     BR_MONITORING_ITEM * mnt_items_old,
		     double elapsed_time, T_BROKER_INFO * br_info_p,
		     T_SHM_APPL_SERVER * shm_appl)
{
  BR_MONITORING_ITEM *mnt_item_cur_p = NULL;
  BR_MONITORING_ITEM *mnt_item_old_p = NULL;
  BR_MONITORING_ITEM mnt_item;
  char buf[256];
#ifdef GET_PSINFO
  T_PSINFO proc_info;
  char time_str[32];
#endif

  assert (shm_appl != NULL);

  mnt_item_cur_p = mnt_items_cur;
  mnt_item_old_p = mnt_items_old;

  if (elapsed_time > 0)
    {
      mnt_item.tps =
	(mnt_item_cur_p->num_tx - mnt_item_old_p->num_tx) / elapsed_time;
      mnt_item.qps =
	(mnt_item_cur_p->num_qx - mnt_item_old_p->num_qx) / elapsed_time;
      mnt_item.lts = mnt_item_cur_p->num_lt - mnt_item_old_p->num_lt;
      mnt_item.lqs = mnt_item_cur_p->num_lq - mnt_item_old_p->num_lq;
      mnt_item.eqs = mnt_item_cur_p->num_eq - mnt_item_old_p->num_eq;
      mnt_item.eqs_ui = mnt_item_cur_p->num_eq_ui - mnt_item_old_p->num_eq_ui;
      mnt_item.its =
	mnt_item_cur_p->num_interrupts - mnt_item_old_p->num_interrupt;
      mnt_item.num_select_query =
	mnt_item_cur_p->num_select_query - mnt_item_old_p->num_select_query;
      mnt_item.num_insert_query =
	mnt_item_cur_p->num_insert_query - mnt_item_old_p->num_insert_query;
      mnt_item.num_update_query =
	mnt_item_cur_p->num_update_query - mnt_item_old_p->num_update_query;
      mnt_item.num_delete_query =
	mnt_item_cur_p->num_delete_query - mnt_item_old_p->num_delete_query;
      mnt_item.num_others_query =
	mnt_item_cur_p->num_others_query - mnt_item_old_p->num_others_query;

      mnt_item.num_br_reject = mnt_item_cur_p->num_br_reject -
	mnt_item_old_p->num_br_reject;
      mnt_item.num_br_connect_fail = mnt_item_cur_p->num_br_connect_fail -
	mnt_item_old_p->num_br_connect_fail;
      mnt_item.num_br_ping_req = mnt_item_cur_p->num_br_ping_req -
	mnt_item_old_p->num_br_ping_req;
      mnt_item.num_br_cancel_req = mnt_item_cur_p->num_br_cancel_req -
	mnt_item_old_p->num_br_cancel_req;

      mnt_item_cur_p->tps = mnt_item.tps;
      mnt_item_cur_p->qps = mnt_item.qps;
      mnt_item_cur_p->lts = mnt_item.lts;
      mnt_item_cur_p->lqs = mnt_item.lqs;
      mnt_item_cur_p->eqs = mnt_item.eqs;
      mnt_item_cur_p->eqs_ui = mnt_item.eqs_ui;
      mnt_item_cur_p->its = mnt_item.its;

    }
  else
    {
      memcpy (&mnt_item, mnt_item_old_p, sizeof (mnt_item));
    }

  print_value (FIELD_PID, &(br_info_p->broker_pid), FIELD_T_INT);
  if (full_Info_flag)
    {
      int process_size;

      process_size = (int) (os_get_mem_size (br_info_p->broker_pid,
					     MEM_VSIZE) / ONE_K);
      print_value (FIELD_PSIZE, &process_size, FIELD_T_INT);
    }
  print_value (FIELD_PORT, &(br_info_p->port), FIELD_T_INT);

  print_value (FIELD_APPL_SERVER_NUM_TOTAL,
	       &mnt_item_cur_p->num_appl_server, FIELD_T_INT);

  if (full_Info_flag)
    {

      print_value (FIELD_APPL_SERVER_NUM_CLIENT_WAIT,
		   &mnt_item_cur_p->num_client_wait, FIELD_T_INT);
      print_value (FIELD_APPL_SERVER_NUM_BUSY, &mnt_item_cur_p->num_busy,
		   FIELD_T_INT);
      print_value (FIELD_APPL_SERVER_NUM_CLIENT_WAIT_IN_SEC,
		   &mnt_item_cur_p->num_client_wait_nsec, FIELD_T_INT);
      print_value (FIELD_APPL_SERVER_NUM_BUSY_IN_SEC,
		   &mnt_item_cur_p->num_busy_nsec, FIELD_T_INT);
    }

  print_value (FIELD_JOB_QUEUE_ID, &(shm_appl->job_queue[0].id), FIELD_T_INT);

#ifdef GET_PSINFO
  get_psinfo (br_info_p->pid, &proc_info);

  print_value (FIELD_THREAD, &(proc_info.num_thr), FIELD_T_INT);

  get_cpu_usage_string (buf, proc_info.pcpu);
  print_value (FIELD_CPU_USAGE, buf, FIELD_T_STRING);

  time_format (proc_info.cpu_time, time_str);
  print_value (FIELD_CPU_TIME, &time_str, FIELD_T_STRING);
#endif

  print_value (FIELD_TPS, &mnt_item.tps, FIELD_T_UINT64);
  print_value (FIELD_QPS, &mnt_item.qps, FIELD_T_UINT64);

  if (full_Info_flag == false)
    {
      print_value (FIELD_NUM_OF_SELECT_QUERIES,
		   &mnt_item.num_select_query, FIELD_T_UINT64);
      print_value (FIELD_NUM_OF_INSERT_QUERIES,
		   &mnt_item.num_insert_query, FIELD_T_UINT64);
      print_value (FIELD_NUM_OF_UPDATE_QUERIES,
		   &mnt_item.num_update_query, FIELD_T_UINT64);
      print_value (FIELD_NUM_OF_DELETE_QUERIES,
		   &mnt_item.num_delete_query, FIELD_T_UINT64);
      print_value (FIELD_NUM_OF_OTHERS_QUERIES,
		   &mnt_item.num_others_query, FIELD_T_UINT64);
    }
  sprintf (buf, "%lu/%-.1f", mnt_item.lts,
	   (shm_appl->long_transaction_time / 1000.0));
  print_value (FIELD_LONG_TRANSACTION, buf, FIELD_T_STRING);
  sprintf (buf, "%lu/%-.1f", mnt_item.lqs,
	   (shm_appl->long_query_time / 1000.0));
  print_value (FIELD_LONG_QUERY, buf, FIELD_T_STRING);
  print_value (FIELD_ERROR_QUERIES, &mnt_item.eqs, FIELD_T_UINT64);
  print_value (FIELD_UNIQUE_ERROR_QUERIES, &mnt_item.eqs_ui, FIELD_T_UINT64);

  if (full_Info_flag)
    {
      print_value (FIELD_CANCELED, &mnt_item.its, FIELD_T_INT64);
      print_value (FIELD_ACCESS_MODE,
		   get_access_mode_string (br_info_p->access_mode,
					   br_info_p->
					   replica_only_flag),
		   FIELD_T_STRING);
      print_value (FIELD_SQL_LOG,
		   get_sql_log_mode_string (br_info_p->sql_log_mode),
		   FIELD_T_STRING);
    }

  print_value (FIELD_NUMBER_OF_CONNECTION,
	       &mnt_item_cur_p->num_connect, FIELD_T_UINT64);
  print_value (FIELD_NUMBER_OF_CONNECTION_REJECTED,
	       &mnt_item_cur_p->num_connect_reject, FIELD_T_UINT64);

  if (full_Info_flag)
    {
      sprintf (buf, "%d,%d,%d,%d",
	       mnt_item.num_br_reject, mnt_item.num_br_connect_fail,
	       mnt_item.num_br_ping_req, mnt_item.num_br_cancel_req);
      print_value (FIELD_NUMBER_OF_BROKER_REQ, buf, FIELD_T_STRING);
    }

  print_newline ();

  return;
}

static int
brief_monitor (char *br_vector, double elapsed_time)
{
  T_BROKER_INFO *br_info_p = NULL;
  T_SHM_APPL_SERVER *shm_appl = NULL;
  static BR_MONITORING_ITEM **mnt_items_old = NULL;
  static BR_MONITORING_ITEM *mnt_items_cur_p = NULL;
  BR_MONITORING_ITEM *mnt_items_old_p = NULL;
  int br_index;
  int max_broker_name_size = FIELD_WIDTH_BROKER_NAME;
  int broker_name_size;

  for (br_index = 0; br_index < shm_Br->num_broker; br_index++)
    {
      if (br_vector[br_index] == 0)
	{
	  continue;
	}

      broker_name_size = strlen (shm_Br->br_info[br_index].name);
      if (broker_name_size > max_broker_name_size)
	{
	  max_broker_name_size = broker_name_size;
	}
    }
  if (max_broker_name_size > BROKER_NAME_LEN)
    {
      max_broker_name_size = BROKER_NAME_LEN;
    }
  fields[FIELD_BROKER_NAME].width = max_broker_name_size;

  print_monitor_header ();

  if (mnt_items_old == NULL)
    {
      mnt_items_old = (BR_MONITORING_ITEM **)
	calloc (sizeof (BR_MONITORING_ITEM *), shm_Br->num_broker);
      if (mnt_items_old == NULL)
	{
	  return -1;
	}
    }

  if (mnt_items_cur_p == NULL)
    {
      mnt_items_cur_p = (BR_MONITORING_ITEM *)
	malloc (sizeof (BR_MONITORING_ITEM));
      if (mnt_items_cur_p == NULL)
	{
	  str_out ("%s", "malloc error");
	  print_newline ();
	  return -1;
	}
    }

  for (br_index = 0; br_index < shm_Br->num_broker; br_index++)
    {
      char broker_name[BROKER_NAME_LEN + 1];

      br_info_p = &shm_Br->br_info[br_index];

      if (br_vector[br_index] == 0 || br_info_p->broker_type != NORMAL_BROKER)
	{
	  continue;
	}

      if (service_Filter_value != SERVICE_UNKNOWN
	  && service_Filter_value != br_info_p->service_flag)
	{
	  continue;
	}

      snprintf (broker_name, BROKER_NAME_LEN, "%s", br_info_p->name);
      broker_name[BROKER_NAME_LEN] = '\0';
      str_out ("*%c", FIELD_DELIMITER);
      print_value (FIELD_BROKER_NAME, broker_name, FIELD_T_STRING);

      if (br_info_p->service_flag != SERVICE_ON)
	{
	  str_out ("%c%s", FIELD_DELIMITER, "OFF");
	  print_newline ();
	  continue;
	}

      shm_appl = rye_shm_attach (br_info_p->appl_server_shm_key,
				 RYE_SHM_TYPE_BROKER_LOCAL, true);
      if (shm_appl == NULL)
	{
	  str_out ("%c%s", FIELD_DELIMITER, "shared memory open error");
	  print_newline ();
	  return -1;
	}

      memset (mnt_items_cur_p, 0, sizeof (BR_MONITORING_ITEM));

      if (mnt_items_old[br_index] == NULL)
	{
	  mnt_items_old[br_index] = (BR_MONITORING_ITEM *)
	    calloc (sizeof (BR_MONITORING_ITEM), 1);
	  if (mnt_items_old[br_index] == NULL)
	    {
	      str_out ("%s", "malloc error");
	      print_newline ();
	      goto error;
	    }
	}

      mnt_items_old_p = mnt_items_old[br_index];

      set_monitor_items (mnt_items_cur_p, br_info_p, shm_appl);

      print_monitor_items (mnt_items_cur_p, mnt_items_old_p,
			   elapsed_time, br_info_p, shm_appl);
      memcpy (mnt_items_old_p, mnt_items_cur_p, sizeof (BR_MONITORING_ITEM));

      if (shm_appl)
	{
	  rye_shm_detach (shm_appl);
	  shm_appl = NULL;
	}
    }

  return 0;

error:
  if (shm_appl)
    {
      rye_shm_detach (shm_appl);
    }

  return -1;
}

static void
print_appl_header (UNUSED_ARG bool use_pdh_flag)
{
  char buf[256];
  char line_buf[256];
  int buf_offset = 0;
  int i;

  buf_offset = print_title (buf, buf_offset, FIELD_ID, NULL);

  buf_offset = print_title (buf, buf_offset, FIELD_PID, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_QPS, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_LQS, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_PSIZE, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_STATUS, NULL);
#if 0
  buf_offset = print_title (buf, buf_offset, FIELD_PORT, NULL);
#endif
#ifdef GET_PSINFO
  buf_offset = print_title (buf, buf_offset, FIELD_CPU_USAGE, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_CPU_TIME, NULL);
#endif
  if (full_Info_flag)
    {
      buf_offset = print_title (buf, buf_offset, FIELD_LAST_ACCESS_TIME,
				NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_DB_NAME, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_HOST, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_LAST_CONNECT_TIME,
				NULL);

      buf_offset = print_title (buf, buf_offset, FIELD_CLIENT_IP, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_CLIENT_VERSION, NULL);

      buf_offset = print_title (buf, buf_offset, FIELD_SQL_LOG_MODE, NULL);

      buf_offset = print_title (buf, buf_offset, FIELD_TRANSACTION_STIME,
				NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_CONNECT, NULL);
      buf_offset = print_title (buf, buf_offset, FIELD_RESTART, NULL);

    }

  for (i = 0; i < buf_offset; i++)
    line_buf[i] = '-';
  line_buf[i] = '\0';

  str_out ("%s", line_buf);
  print_newline ();
  str_out ("%s", buf);
  print_newline ();
  str_out ("%s", line_buf);
  print_newline ();
}

static int
unusable_databases_monitor (void)
{
  T_SHM_APPL_SERVER *shm_appl = NULL;
  int i, j, u_index;
  char buf[LINE_MAX];
  int buf_offset;

  buf_offset = 0;
  buf_offset = print_title (buf, buf_offset, FIELD_BROKER_NAME, NULL);
  buf_offset = print_title (buf, buf_offset, FIELD_UNUSABLE_DATABASES, NULL);

  str_out ("%s", buf);
  print_newline ();
  for (i = strlen (buf); i > 0; i--)
    {
      str_out ("%s", "=");
    }
  print_newline ();

  for (i = 0; i < shm_Br->num_broker; i++)
    {
      str_out ("*%c", FIELD_DELIMITER);
      print_value (FIELD_BROKER_NAME, shm_Br->br_info[i].name,
		   FIELD_T_STRING);

      if (shm_Br->br_info[i].service_flag == SERVICE_ON)
	{
	  shm_appl = rye_shm_attach (shm_Br->br_info[i].appl_server_shm_key,
				     RYE_SHM_TYPE_BROKER_LOCAL, true);
	  if (shm_appl == NULL)
	    {
	      str_out ("%c%s", FIELD_DELIMITER, "shared memory open error");
	      print_newline ();
	    }
	  else
	    {
	      if (shm_appl->monitor_server_flag)
		{
		  u_index = shm_appl->unusable_databases_seq % 2;

		  for (j = 0; j < shm_appl->unusable_databases_cnt[u_index];
		       j++)
		    {
		      char host[MAX_NODE_INFO_STR_LEN];
		      prm_node_info_to_str (host, sizeof (host),
					    &shm_appl->
					    unusable_databases[u_index][j].
					    db_node);
		      str_out ("%s@%s ",
			       shm_appl->unusable_databases[u_index][j].
			       database_name, host);
		    }
		}
	      print_newline ();
	      rye_shm_detach (shm_appl);
	    }
	}
      else
	{
	  str_out ("%c%s", FIELD_DELIMITER, "OFF");
	  print_newline ();
	}
    }

  return 0;
}

static int
print_title (char *buf_p, int buf_offset, FIELD_NAME name,
	     const char *new_title_p)
{
  struct status_field *field_p = NULL;
  const char *title_p = NULL;

  assert (buf_p != NULL);
  assert (buf_offset >= 0);

  field_p = &fields[name];

  if (new_title_p != NULL)
    {
      title_p = new_title_p;
    }
  else
    {
      title_p = field_p->title;
    }

  switch (field_p->name)
    {
    case FIELD_BROKER_NAME:
      buf_offset += sprintf (buf_p + buf_offset, "%c%c",
			     FIELD_DELIMITER, FIELD_DELIMITER);
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  buf_offset += sprintf (buf_p + buf_offset, "%-*s",
				 field_p->width, title_p);
	}
      else
	{
	  buf_offset += sprintf (buf_p + buf_offset, "%*s",
				 field_p->width, title_p);
	}
      break;
    default:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  buf_offset += sprintf (buf_p + buf_offset, "%-*s",
				 field_p->width, title_p);
	}
      else
	{
	  buf_offset += sprintf (buf_p + buf_offset, "%*s",
				 field_p->width, title_p);
	}
      break;
    }
  buf_offset += sprintf (buf_p + buf_offset, "%c", FIELD_DELIMITER);

  return buf_offset;
}

static void
print_value (FIELD_NAME name, const void *value_p, FIELD_TYPE type)
{
  struct status_field *field_p = NULL;
  struct timeval time_val;
  char time_buf[256];

  assert (value_p != NULL);

  field_p = &fields[name];

  switch (type)
    {
    case FIELD_T_INT:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*d", field_p->width, *(const int *) value_p);
	}
      else
	{
	  str_out ("%*d", field_p->width, *(const int *) value_p);
	}
      break;
    case FIELD_T_STRING:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*s", field_p->width, (const char *) value_p);
	}
      else
	{
	  str_out ("%*s", field_p->width, (const char *) value_p);
	}
      break;
    case FIELD_T_FLOAT:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*f", field_p->width, *(const float *) value_p);
	}
      else
	{
	  str_out ("%*f", field_p->width, *(const float *) value_p);
	}
      break;
    case FIELD_T_UINT64:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*lu", field_p->width, *(const UINT64 *) value_p);
	}
      else
	{
	  str_out ("%*lu", field_p->width, *(const UINT64 *) value_p);
	}
      break;
    case FIELD_T_INT64:
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*ld", field_p->width, *(const INT64 *) value_p);
	}
      else
	{
	  str_out ("%*ld", field_p->width, *(const INT64 *) value_p);
	}
      break;
    case FIELD_T_TIME:
      time_val.tv_sec = *((const time_t *) value_p);
      time_val.tv_usec = 0;
      (void) er_datetime (&time_val, time_buf, sizeof (time_buf));
      if (field_p->align == FIELD_LEFT_ALIGN)
	{
	  str_out ("%-*s", field_p->width, time_buf);
	}
      else
	{
	  str_out ("%*s", field_p->width, time_buf);
	}
    default:
      break;
    }
  str_out ("%c", FIELD_DELIMITER);
}

static const char *
get_sql_log_mode_string (T_SQL_LOG_MODE_VALUE mode)
{
  switch (mode)
    {
    case SQL_LOG_MODE_NONE:
      return "NONE";
    case SQL_LOG_MODE_NOTICE:
      return "NOTICE";
    case SQL_LOG_MODE_ALL:
      return "ALL";
    default:
      return "-";
    }
}

static const char *
get_access_mode_string (T_ACCESS_MODE_VALUE mode, int replica_only_flag)
{
  switch (mode)
    {
    case READ_ONLY_ACCESS_MODE:
      return (replica_only_flag ? "RO-REPLICA" : "RO");
    case SLAVE_ONLY_ACCESS_MODE:
      return (replica_only_flag ? "SO-REPLICA" : "SO");
    case READ_WRITE_ACCESS_MODE:
      return (replica_only_flag ? "RW-REPLICA" : "RW");
    case REPL_ACCESS_MODE:
      return "REPLICATION";
    default:
      return "--";
    }
}

static const char *
get_status_string (T_APPL_SERVER_INFO * as_info_p, char appl_server)
{
  assert (as_info_p != NULL);

  if (as_info_p->uts_status == UTS_STATUS_BUSY)
    {
      if (IS_APPL_SERVER_TYPE_CAS (appl_server))
	{
	  if (as_info_p->con_status == CON_STATUS_OUT_TRAN)
	    {
	      return "CLOSE_WAIT";
	    }
	  else if (as_info_p->log_msg[0] == '\0')
	    {
	      return "CLIENT_WAIT";
	    }
	  else
	    {
	      return "BUSY";
	    }
	}
      else
	{
	  return "BUSY";
	}
    }
  else if (as_info_p->uts_status == UTS_STATUS_RESTART)
    {
      return "INITIALIZE";
    }
  else if (as_info_p->uts_status == UTS_STATUS_CON_WAIT)
    {
      return "CON WAIT";
    }
  else
    {
      return "IDLE";
    }

  return NULL;
}

#ifdef GET_PSINFO
static void
get_cpu_usage_string (char *buf_p, float usage)
{
  assert (buf_p != NULL);

  if (usage >= 0)
    {
      sprintf (buf_p, "%.2f", usage);
    }
  else
    {
      sprintf (buf_p, " - ");
    }
}

static void
time_format (int t, char *time_str)
{
  int min, sec;

  min = t / 60;
  sec = t % 60;
  sprintf (time_str, "%d:%02d", min, sec);
}
#endif

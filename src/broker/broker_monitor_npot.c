/*
 * Copyright 2017 NAVER Corp.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

/*
 * broker_monitor_npot.c -
 */

#include "config.h"

#include <stdio.h>
#include <assert.h>
#include <pwd.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <signal.h>

#include "porting.h"
#include "cci_common.h"
#include "broker_shm.h"
#include "error_manager.h"
#include "rye_server_shm.h"

#define MONITOR_INTERVAL	10

#define METRIC_PREFIX		"rye."

#define METRIC_BROKER_QPS	"qps"
#define METRIC_BROKER_ERROR	"error"
#define METRIC_BROKER_BUSY_CAS	"busy_cas"

#define TAG_HOST		"host"
#define TAG_BROKER_NAME		"broker_name"
#define TAG_DB_NAME		"db_name"
#define TAG_INSTANCE		"instance"
#define TAG_ITEM		"item"
#define TAG_SERVICE		"service"

#define TCP_CONNECT_HOST_ENV	"NPOT_CONNECT_HOST"
#define MY_HOSTNAME_ENV		"NPOT_MY_HOST"
#define SERVICE_TAG_ENV		"NPOT_SERVICE"

typedef struct t_rye_shm_info T_SHM_INFO;
struct t_rye_shm_info
{
  bool is_valid;
  key_t key;
  int shmid;
  RYE_SHM_TYPE shm_type;
  const char *user_name;
  struct t_rye_shm_info *next;
};

typedef struct
{
  char item_hash_key[64];
  const char *instance;
  char broker_db_name[BROKER_NAME_LEN];
  time_t last_check_time;
  int64_t last_value;
  uint64_t acc_time;
} T_MONITOR_ITEM;

typedef struct
{
  const char *metric;
  const char *item;
  bool is_cumulative;
  bool is_collecting_time;
} T_DB_STATS_INFO;

typedef struct
{
  const char *host;
  int port;
  const char *id;
  const char *pw;
} T_TCP_SEND_CONNECT_INFO;

static int get_args (int argc, char *argv[]);
static int set_my_hostname (void);
static int init_server_monitor_item (void);
static void npot_server_monitor (T_SHM_INFO * shm_info);
static void npot_broker_monitor (T_SHM_INFO * shm_info);
static char *get_pw_name (uid_t uid);
static T_SHM_INFO *make_shm_info (key_t key, int shmid, RYE_SHM_TYPE shm_type,
				  const char *user_name);
static bool is_shm_all_valid (void);
static void reset_shm_info_list (void);
static void clear_shm_info (void);
static int add_shm_info (key_t key, int shmid, RYE_SHM_TYPE shm_type,
			 const char *user_name);
static int find_all_rye_shm (void);
static int uid_equals (const void *key1, const void *key2);
static unsigned int uid_ht_hash (const void *key, unsigned int ht_size);
static unsigned int stat_info_ht_hash (const void *key, unsigned int ht_size);
static int stat_info_equals (const void *key1, const void *key2);
static int stat_info_ht_rem_data (void *key, void *data, void *args);
static void print_monitor_item (const char *metric_name,
				const char *item_name, const char *instance,
				const char *tag_broker_or_db,
				const char *broker_db_name, time_t check_time,
				int64_t value, uint64_t acc_time,
				bool is_cumulative_value,
				bool is_collecting_time);
static void send_data (const char *metric_name, const char *item_name,
		       const char *instance, const char *tag_broker_or_db,
		       const char *broker_db_name, time_t check_time,
		       const char *value_p);
static T_MONITOR_ITEM *clone_monitor_item (T_MONITOR_ITEM * item);
static int make_connect_info (char *arg);
static int tcp_connect (void);
static void tcp_send (const char *msg, int size);
static void tcp_close (void);

static T_SHM_INFO *shm_Info_list = NULL;
static char my_Hostname[64];
static CCI_MHT_TABLE *item_Ht;
static CCI_MHT_TABLE *uid_Ht;

static T_DB_STATS_INFO *db_Stats_info;
static T_TCP_SEND_CONNECT_INFO *tcp_Send_connect_info = NULL;
static int tcp_Send_fd = -1;
static char tag_Service[64];
static int repeat_Count = -1;

int
main (int argc, char *argv[])
{
  T_SHM_INFO *shm_info;

  signal (SIGPIPE, SIG_IGN);
  assert (BROKER_NAME_LEN >= SHM_DBNAME_SIZE);

  if (get_args (argc, argv) < 0)
    {
      return -1;
    }

  if (set_my_hostname () < 0)
    {
      return -1;
    }

  if (init_server_monitor_item () < 0)
    {
      return -1;
    }

  item_Ht = cci_mht_create ("mointor_item_hash", 1000, stat_info_ht_hash,
			    stat_info_equals);
  uid_Ht = cci_mht_create ("uid_ht", 10, uid_ht_hash, uid_equals);

  if (item_Ht == NULL || uid_Ht == NULL)
    {
      return -1;
    }

  while (repeat_Count != 0)
    {
      if (tcp_Send_connect_info != NULL)
	{
	  if (tcp_connect () < 0)
	    {
	      goto loop_end;
	    }
	}

      if (find_all_rye_shm () < 0)
	{
	  return -1;
	}

      if (is_shm_all_valid () == false)
	{
	  cci_mht_clear (item_Ht, stat_info_ht_rem_data, NULL);
	  clear_shm_info ();
	  sleep (1);
	  continue;
	}

      shm_info = shm_Info_list;
      while (shm_info)
	{
	  if (shm_info->is_valid)
	    {
	      if (shm_info->shm_type == RYE_SHM_TYPE_BROKER_GLOBAL)
		{
		  npot_broker_monitor (shm_info);
		}
	      else if (shm_info->shm_type == RYE_SHM_TYPE_SERVER)
		{
		  npot_server_monitor (shm_info);
		}
	    }
	  shm_info = shm_info->next;
	}

    loop_end:
      if (repeat_Count > 0)
	{
	  repeat_Count--;
	}
      if (repeat_Count != 0)
	{
	  sleep (MONITOR_INTERVAL);
	}
    }

  return 0;
}

static int
get_args (int argc, char *argv[])
{
  int opt;

  while ((opt = getopt (argc, argv, "c:r:")) != -1)
    {
      switch (opt)
	{
	case 'c':
	  if (make_connect_info (optarg) < 0)
	    {
	      return -1;
	    }
	  break;
	case 'r':
	  repeat_Count = atoi (optarg);
	  break;
	default:
	  return -1;
	}
    }

  return 0;
}

static int
set_my_hostname ()
{
  char *p;

  p = getenv (MY_HOSTNAME_ENV);
  if (p == NULL)
    {
      if (gethostname (my_Hostname, sizeof (my_Hostname)) < 0)
	{
	  return -1;
	}
    }
  else
    {
      strncpy (my_Hostname, p, sizeof (my_Hostname));
      my_Hostname[sizeof (my_Hostname) - 1] = '\0';
    }

  p = getenv (SERVICE_TAG_ENV);
  if (p == NULL)
    {
      tag_Service[0] = '\0';
    }
  else
    {
      snprintf (tag_Service, sizeof (tag_Service), "%s=%s", TAG_SERVICE, p);
    }

  return 0;
}

#define SET_DB_STATS_INFO(DB_STATS_INFO,METRIC,ITEM)	\
	do {						\
	  (DB_STATS_INFO)->metric = METRIC;		\
	  (DB_STATS_INFO)->item = ITEM;			\
	} while (0)

static int
init_server_monitor_item ()
{
  int i;

  db_Stats_info = malloc (sizeof (T_DB_STATS_INFO) *
			  MNT_SIZE_OF_SERVER_EXEC_STATS);
  if (db_Stats_info == NULL)
    {
      return -1;
    }

  memset (db_Stats_info, 0,
	  sizeof (T_DB_STATS_INFO) * MNT_SIZE_OF_SERVER_EXEC_STATS);

  for (i = 0; i < MNT_SIZE_OF_SERVER_EXEC_STATS; i++)
    {
      db_Stats_info[i].is_cumulative = mnt_stats_is_cumulative (i);
      db_Stats_info[i].is_collecting_time = mnt_stats_is_collecting_time (i);
    }

#if 0
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DISK_SECTOR_ALLOCS],
		     "disk", "sector_allocs");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DISK_SECTOR_DEALLOCS],
		     "disk", "sector_deallocs");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DISK_PAGE_ALLOCS],
		     "disk", "page_allocs");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DISK_PAGE_DEALLOCS],
		     "disk", "page_deallocs");
#endif

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES],
		     "datapage", "fetch");
#if 1				/* fetches sub-info */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_FILE_HEADER],
                     "datapage", "fetch_file_header");	/* 1 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_FILE_TAB],
                     "datapage", "fetch_file_tab");	/* 2 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_HEAP_HEADER],
                     "datapage", "fetch_heap_header");	/* 3 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_HEAP],
                     "datapage", "fetch_heap");	/* 4 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_VOLHEADER],
                     "datapage", "fetch_volheader");	/* 5 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_VOLBITMAP],
                     "datapage", "fetch_volbitmap");	/* 6 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_XASL],
                     "datapage", "fetch_xasl");	/* 7 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_QRESULT],
                     "datapage", "fetch_qresult");	/* 8 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_EHASH],
                     "datapage", "fetch_ehash");	/* 9 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_OVERFLOW],
                     "datapage", "fetch_overflow");	/* 10 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_AREA],
                     "datapage", "fetch_area");	/* 11 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_CATALOG],
                     "datapage", "fetch_catalog");	/* 12 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_BTREE_ROOT],
                     "datapage", "fetch_btree_root");	/* 13 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_BTREE],
                     "datapage", "fetch_btree");	/* 14 */

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_UNKNOWN],
                     "datapage", "fetch_unknown");	/* 0 */

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_OP_FILE_ALLOC_PAGES],
                     "datapage", "fetch_op_file_alloc_pages");	/* 15 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_OP_FILE_DEALLOC_PAGE],
                     "datapage", "fetch_op_file_dealloc_page");	/* 16 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_OP_HEAP_FIND_BEST_PAGE],
                     "datapage", "fetch_op_heap_find_best_page");	/* 17 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_OP_HEAP_BESTSPACE_SYNC],
                     "datapage", "fetch_op_heap_find_bestspace_sync");	/* 18 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_OP_HEAP_OVF_INSERT],
                     "datapage", "fetch_op_heap_ovf_insert");	/* 19 */
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_FETCHES_OP_HEAP_OVF_UPDATE],
                     "datapage", "fetch_op_heap_ovf_update");	/* 20 */
  /* reserve 21~24 */
#endif

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_IOREADS],
		     "datapage", "ioread");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_IOWRITES],
		     "datapage", "iowrite");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_LOG_PAGE_IOREADS],
		     "logpage", "ioread");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_LOG_PAGE_IOWRITES],
		     "logpage", "iowrite");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_LOG_CHECKPOINTS],
		     "event", "checkpoint");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DDL_LOCKS_REQUESTS],
		     "lock", "ddl");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_GLOBAL_LOCKS_REQUEST],
		     "lock", "global");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_SHARD_LOCKS_REQUEST],
		     "lock", "shard");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_TRAN_COMMITS],
		     "tran", "commit");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_TRAN_ROLLBACKS],
		     "tran", "rollback");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_TRAN_INTERRUPTS],
		     "tran", "interrupt");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_INSERTS],
		     "btree", "insert");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_DELETES],
		     "btree", "delete");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_UPDATES],
		     "btree", "update");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_LOAD_DATA],
		     "event", "create_index");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_COVERED],
		     "btree", "covered");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_NONCOVERED],
		     "btree", "noncovered");
#if 0
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_RESUMES],
		     "btree", "resumes");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_MULTIRANGE_OPTIMIZATION],
		     "btree", "multirange_optimization");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_SPLITS],
		     "btree", "splits");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_MERGES],
		     "btree", "merges");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_PAGE_ALLOCS],
		     "btree", "page_allocs");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_BTREE_PAGE_DEALLOCS],
		     "btree", "page_deallocs");
#endif

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_QUERY_SELECTS],
		     "query", "select");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_QUERY_INSERTS],
		     "query", "insert");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_QUERY_DELETES],
		     "query", "delete");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_QUERY_UPDATES],
		     "query", "update");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_QUERY_SSCANS],
		     "query", "sscan");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_QUERY_ISCANS],
		     "query", "iscan");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_QUERY_LSCANS],
		     "query", "lscan");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_QUERY_HOLDABLE_CURSORS],
		     "query", "holdablecursors");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_SORT_IO_PAGES],
		     "sort", "iopage");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_SORT_DATA_PAGES],
		     "sort", "datapage");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_NETWORK_REQUESTS],
		     "network", "request");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_HEAP_STATS_BESTSPACE_ENTRIES],
		     "heap", "bestspace");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_HA_LAST_FLUSHED_PAGEID],
		     "ha", "last_flushed_pageid");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_HA_EOF_PAGEID],
		     "ha", "eof_pageid");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_HA_CURRENT_PAGEID],
		     "ha", "current_pageid");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_HA_REQUIRED_PAGEID],
		     "ha", "required_pageid");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_HA_REPLICATION_DELAY],
		     "ha", "delay");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_PLAN_CACHE_ADD],
		     "plancache", "add");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_PLAN_CACHE_LOOKUP],
		     "plancache", "lookup");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_PLAN_CACHE_HIT],
		     "plancache", "hit");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_PLAN_CACHE_MISS],
		     "plancache", "miss");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_PLAN_CACHE_FULL],
		     "plancache", "full");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_PLAN_CACHE_DELETE],
		     "plancache", "delete");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_PLAN_CACHE_INVALID_XASL_ID],
		     "plancache", "invalid");
  SET_DB_STATS_INFO (&db_Stats_info
		     [MNT_STATS_PLAN_CACHE_QUERY_STRING_HASH_ENTRIES],
		     "plancache", "entry");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_PRIOR_LSA_LIST_SIZE],
		     "prior_lsa", "size");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_PRIOR_LSA_LIST_MAXED],
		     "prior_lsa", "maxed");
  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_PRIOR_LSA_LIST_REMOVED],
		     "prior_lsa", "removed");

  SET_DB_STATS_INFO (&db_Stats_info[MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO],
		     "buffer", "hit_ratio");

  return 0;
}

static void
npot_server_monitor (T_SHM_INFO * shm_info)
{
  MNT_SERVER_EXEC_STATS cur_global_stats;
  time_t check_time = time (NULL);
  char dbname[SHM_DBNAME_SIZE];
  int i, err;

  err = rye_server_shm_get_global_stats_from_key (&cur_global_stats, dbname,
						  shm_info->key,
						  MNT_SIZE_OF_SERVER_EXEC_STATS);
  if (err < 0)
    {
      return;
    }

  for (i = 0; i < MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO; i++)
    {
      if (db_Stats_info[i].metric == NULL)
	{
	  continue;
	}

      print_monitor_item (db_Stats_info[i].metric, db_Stats_info[i].item,
			  shm_info->user_name,
			  TAG_DB_NAME, dbname,
			  check_time, cur_global_stats.values[i],
			  cur_global_stats.acc_time[i],
			  db_Stats_info[i].is_cumulative,
			  db_Stats_info[i].is_collecting_time);
    }
}

static void
npot_broker_monitor (T_SHM_INFO * shm_info)
{
  T_SHM_BROKER *shm_br;
  int br_idx;

  shm_br = rye_shm_attach (shm_info->key, RYE_SHM_TYPE_BROKER_GLOBAL, true);
  if (shm_br == NULL)
    {
      return;
    }

  for (br_idx = 0; br_idx < shm_br->num_broker; br_idx++)
    {
      T_BROKER_INFO *br_info_p;
      T_SHM_APPL_SERVER *shm_appl;
      T_APPL_SERVER_INFO *as_info_p;
      int i;
      int num_qx = 0;
      int num_client_wait = 0;
      int num_busy = 0;
      int num_error = 0;
      time_t check_time = time (NULL);

      br_info_p = &shm_br->br_info[br_idx];

      if (br_info_p->service_flag != SERVICE_ON)
	{
	  continue;
	}
      if (br_info_p->broker_type != NORMAL_BROKER)
	{
	  continue;
	}

      shm_appl = rye_shm_attach (br_info_p->appl_server_shm_key,
				 RYE_SHM_TYPE_BROKER_LOCAL, true);
      if (shm_appl == NULL)
	{
	  continue;
	}

      for (i = 0; i < br_info_p->appl_server_max_num; i++)
	{
	  as_info_p = &(shm_appl->info.as_info[i]);

	  num_qx += as_info_p->num_queries_processed;
	  num_error += (as_info_p->num_error_queries -
			as_info_p->num_unique_error_queries);

	  if (as_info_p->uts_status == UTS_STATUS_BUSY
	      && as_info_p->con_status != CON_STATUS_OUT_TRAN)
	    {
	      if (as_info_p->log_msg[0] == '\0')
		{
		  num_client_wait++;
		}
	      else
		{
		  num_busy++;
		}
	    }
	}

      print_monitor_item (METRIC_BROKER_QPS, NULL, shm_info->user_name,
			  TAG_BROKER_NAME, br_info_p->name, check_time,
			  num_qx, 0, true, false);
      print_monitor_item (METRIC_BROKER_ERROR, NULL,
			  shm_info->user_name, TAG_BROKER_NAME,
			  br_info_p->name, check_time, num_error, 0, true,
			  false);
      print_monitor_item (METRIC_BROKER_BUSY_CAS, NULL, shm_info->user_name,
			  TAG_BROKER_NAME, br_info_p->name, check_time,
			  num_busy, 0, false, false);

      rye_shm_detach (shm_appl);
    }

  rye_shm_detach (shm_br);
}

static T_MONITOR_ITEM *
clone_monitor_item (T_MONITOR_ITEM * item)
{
  T_MONITOR_ITEM *p;

  p = malloc (sizeof (T_MONITOR_ITEM));
  if (p == NULL)
    {
      return NULL;
    }

  *p = *item;
  return p;
}

static void
set_monitor_item (T_MONITOR_ITEM * item, const char *metric,
		  const char *item_name, const char *instance,
		  const char *broker_db_name, time_t check_time,
		  int64_t value, uint64_t acc_time)
{
  if (item_name == NULL)
    {
      snprintf (item->item_hash_key, sizeof (item->item_hash_key),
		"%s", metric);
    }
  else
    {
      snprintf (item->item_hash_key, sizeof (item->item_hash_key),
		"%s.%s", metric, item_name);
    }

  item->instance = instance;
  strncpy (item->broker_db_name, broker_db_name,
	   sizeof (item->broker_db_name));
  item->broker_db_name[sizeof (item->broker_db_name) - 1] = '\0';
  item->last_check_time = check_time;
  item->last_value = value;
  item->acc_time = acc_time;
}

static void
copy_monitor_item_value (T_MONITOR_ITEM * dest, const T_MONITOR_ITEM * src)
{
  dest->last_check_time = src->last_check_time;
  dest->last_value = src->last_value;
  dest->acc_time = src->acc_time;
}

static void
print_monitor_item (const char *metric_name, const char *item_name,
		    const char *instance, const char *tag_broker_or_db,
		    const char *broker_db_name, time_t check_time,
		    int64_t value, uint64_t acc_time,
		    bool is_cumulative_value, bool is_collecting_time)
{
  char value_buf[128];
  char *value_p = NULL;
  char avg_time_buf[128];
  char *avg_time_p = NULL;

  if (is_cumulative_value)
    {
      T_MONITOR_ITEM cur_item;
      T_MONITOR_ITEM *prev_item;

      set_monitor_item (&cur_item, metric_name, item_name, instance,
			broker_db_name, check_time, value, acc_time);

      prev_item = cci_mht_get (item_Ht, &cur_item);
      if (prev_item == NULL)
	{
	  prev_item = clone_monitor_item (&cur_item);
	  cci_mht_put (item_Ht, prev_item, prev_item);
	}
      else
	{
	  if (prev_item->last_check_time < cur_item.last_check_time &&
	      prev_item->last_value <= cur_item.last_value)
	    {
	      double value_per_sec;
	      int64_t diff_value;

	      diff_value = cur_item.last_value - prev_item->last_value;
	      value_per_sec = (double) diff_value /
		(cur_item.last_check_time - prev_item->last_check_time);

	      sprintf (value_buf, "%.1f", value_per_sec);
	      value_p = value_buf;

	      if (is_collecting_time && diff_value > 0
		  && prev_item->acc_time <= cur_item.acc_time)
		{
		  double time_per_value;

		  time_per_value = (cur_item.acc_time - prev_item->acc_time);
		  time_per_value /= diff_value;

		  sprintf (avg_time_buf, "%.1f", time_per_value);
		  avg_time_p = avg_time_buf;
		}
	    }

	  copy_monitor_item_value (prev_item, &cur_item);
	}
    }
  else
    {
      sprintf (value_buf, "%ld", value);
      value_p = value_buf;
    }

  if (value_p != NULL)
    {
      send_data (metric_name, item_name, instance, tag_broker_or_db,
		 broker_db_name, check_time, value_p);

      if (avg_time_p != NULL)
	{
	  char perf_item_name[64];
	  snprintf (perf_item_name, sizeof (perf_item_name), "%s.%s",
		    metric_name, item_name);
	  send_data ("perf", perf_item_name, instance, tag_broker_or_db,
		     broker_db_name, check_time, avg_time_p);
	}
    }
}

static void
send_data (const char *metric_name, const char *item_name,
	   const char *instance, const char *tag_broker_or_db,
	   const char *broker_db_name, time_t check_time, const char *value_p)
{
  char tag_item_value[1024];

  if (item_name == NULL)
    {
      tag_item_value[0] = '\0';
    }
  else
    {
      snprintf (tag_item_value, sizeof (tag_item_value), "%s=%s",
		TAG_ITEM, item_name);
    }

  if (tcp_Send_connect_info == NULL)
    {
      printf ("%s%s %d %s %s=%s %s=%s %s=%s %s\n",
	      METRIC_PREFIX, metric_name, (int) check_time, value_p,
	      TAG_HOST, my_Hostname, tag_broker_or_db, broker_db_name,
	      TAG_INSTANCE, instance, tag_item_value);
    }
  else
    {
      char msg_buf[1024];
      int msg_size;

      msg_size = snprintf (msg_buf, sizeof (msg_buf),
			   "put %s%s %d %s %s=%s %s=%s %s=%s %s %s\n",
			   METRIC_PREFIX, metric_name, (int) check_time,
			   value_p, TAG_HOST, my_Hostname, tag_broker_or_db,
			   broker_db_name, TAG_INSTANCE, instance,
			   tag_item_value, tag_Service);
      assert (msg_size < (int) sizeof (msg_buf));
      tcp_send (msg_buf, msg_size);
    }
}

static int
find_all_rye_shm ()
{
  struct shminfo shminfo;
  int shm_max_idx;
  int i;

  shm_max_idx = shmctl (0, IPC_INFO, (void *) &shminfo);

  reset_shm_info_list ();

  for (i = 0; i <= shm_max_idx; i++)
    {
      int shmid;
      struct shmid_ds shmid_ds;
      char *user_name;
      RYE_SHM_TYPE shm_type;

      shmid = shmctl (i, SHM_STAT, &shmid_ds);
      if (shmid < 0 || (shmid_ds.shm_perm.mode & SHM_DEST) != 0)
	{
	  continue;
	}

      shm_type = rye_shm_check_shm (shmid_ds.shm_perm.__key,
				    RYE_SHM_TYPE_UNKNOWN, true);
      if (shm_type == RYE_SHM_TYPE_UNKNOWN)
	{
	  continue;
	}

      user_name = get_pw_name (shmid_ds.shm_perm.uid);
      if (user_name == NULL)
	{
	  return -1;
	}

      if (add_shm_info (shmid_ds.shm_perm.__key, shmid,
			shm_type, user_name) < 0)
	{
	  return -1;
	}
    }

  return 0;
}

static char *
get_pw_name (uid_t uid)
{
  struct passwd pwbuf;
  struct passwd *pwbufp = NULL;
  char buf[1024];
  char *pw_name;
  uid_t *uid_p;

  pw_name = cci_mht_get (uid_Ht, &uid);
  if (pw_name != NULL)
    {
      return pw_name;
    }

  getpwuid_r (uid, &pwbuf, buf, sizeof (buf), &pwbufp);
  if (pwbufp == NULL)
    {
      sprintf (buf, "%d", uid);
      pw_name = strdup (buf);
    }
  else
    {
      pw_name = strdup (pwbufp->pw_name);
    }

  uid_p = (uid_t *) malloc (sizeof (uid_t));

  if (pw_name == NULL || uid_p == NULL)
    {
      if (pw_name != NULL)
	{
	  free (pw_name);
	}

      if (uid_p != NULL)
	{
	  free (uid_p);
	}

      return NULL;
    }

  *uid_p = uid;

  cci_mht_put (uid_Ht, uid_p, pw_name);

  return pw_name;
}

static T_SHM_INFO *
make_shm_info (key_t key, int shmid, RYE_SHM_TYPE shm_type,
	       const char *user_name)
{
  T_SHM_INFO *shm_info;

  shm_info = malloc (sizeof (T_SHM_INFO));
  if (shm_info == NULL)
    {
      return NULL;
    }

  memset (shm_info, 0, sizeof (T_SHM_INFO));

  shm_info->key = key;
  shm_info->shmid = shmid;
  shm_info->shm_type = shm_type;
  shm_info->user_name = user_name;

  return shm_info;
}

static bool
is_shm_all_valid ()
{
  T_SHM_INFO *p = shm_Info_list;

  while (p)
    {
      if (p->is_valid == false)
	{
	  return false;
	}
      p = p->next;
    }

  return true;
}

static void
reset_shm_info_list ()
{
  T_SHM_INFO *p = shm_Info_list;

  while (p)
    {
      p->is_valid = false;
      p = p->next;
    }
}

static void
clear_shm_info ()
{
  T_SHM_INFO *p = shm_Info_list;
  T_SHM_INFO *rm;

  while (p)
    {
      rm = p;
      p = p->next;
      free (rm);
    }

  shm_Info_list = NULL;
}

static int
add_shm_info (key_t key, int shmid, RYE_SHM_TYPE shm_type,
	      const char *user_name)
{
  T_SHM_INFO *p = shm_Info_list;

  while (p)
    {
      if (p->key == key && p->shmid == shmid && p->shm_type == shm_type &&
	  strcmp (p->user_name, user_name) == 0)
	{
	  break;
	}
      p = p->next;
    }

  if (p == NULL)
    {
      p = make_shm_info (key, shmid, shm_type, user_name);
      if (p == NULL)
	{
	  return -1;
	}
      p->next = shm_Info_list;
      shm_Info_list = p;
    }

  p->is_valid = true;

  return 0;
}

static unsigned int
uid_ht_hash (const void *key, unsigned int ht_size)
{
  const uid_t *uid_p = key;
  assert (key != NULL);

  return (*uid_p % ht_size);
}

static int
uid_equals (const void *key1, const void *key2)
{
  const uid_t *uid1 = key1;
  const uid_t *uid2 = key2;

  assert (uid1 != NULL && uid2 != NULL);

  if (*uid1 == *uid2)
    {
      return 1;
    }
  else
    {
      return 0;
    }
}

static unsigned int
stat_info_ht_hash (const void *key, unsigned int ht_size)
{
  const T_MONITOR_ITEM *item = key;
  const char *p;
  unsigned int pseudo_key = 0;

  assert (key != NULL);

  p = item->item_hash_key;
  while (*p)
    {
      pseudo_key = (pseudo_key << 5) - pseudo_key + *p;
      p++;
    }

  p = item->broker_db_name;
  while (*p)
    {
      pseudo_key = (pseudo_key << 5) - pseudo_key + *p;
      p++;
    }

  p = item->instance;
  while (*p)
    {
      pseudo_key = (pseudo_key << 5) - pseudo_key + *p;
      p++;
    }

  return pseudo_key % ht_size;
}

static int
stat_info_equals (const void *key1, const void *key2)
{
  const T_MONITOR_ITEM *item1 = key1;
  const T_MONITOR_ITEM *item2 = key2;

  assert (item1 != NULL && item2 != NULL);

  if (strcmp (item1->item_hash_key, item2->item_hash_key) == 0 &&
      strcmp (item1->broker_db_name, item2->broker_db_name) == 0 &&
      strcmp (item1->instance, item2->instance) == 0)
    {
      return 1;
    }
  else
    {
      return 0;
    }
}

static int
stat_info_ht_rem_data (void *key, UNUSED_ARG void *data,
		       UNUSED_ARG void *args)
{
  assert (key == data && key != NULL);
  free (key);
  return 0;
}

static void
tcp_close ()
{
  if (tcp_Send_fd >= 0)
    {
      close (tcp_Send_fd);
      tcp_Send_fd = -1;
    }
}

static void
tcp_send (const char *msg, int size)
{
  int write_len;
  int read_len;
  char dummy_read_buf[1024];

  if (tcp_Send_fd < 0)
    {
      return;
    }

  while (size > 0)
    {
      write_len = send (tcp_Send_fd, msg, size, 0);
      if (write_len <= 0)
	{
	  tcp_close ();
	  return;
	}

      msg += write_len;
      size -= write_len;
    }

  while (true)
    {
      read_len = recv (tcp_Send_fd, dummy_read_buf, sizeof (dummy_read_buf),
		       MSG_PEEK | MSG_DONTWAIT);
      if (read_len == 0)
	{
	  break;
	}
      else if (read_len > 0)
	{
	  read_len = recv (tcp_Send_fd, dummy_read_buf, read_len, 0);
#if 0
	  write (1, dummy_read_buf, read_len);
#endif
	}

      if (read_len < 0)
	{
	  if (errno == EAGAIN)
	    {
	      break;
	    }
	  else
	    {
	      tcp_close ();
	      return;
	    }
	}
    }
}

static int
tcp_connect ()
{
  in_addr_t ip_addr;
  struct sockaddr_in sock_addr;
  int sock_addr_len;
  int ret;
  int sock_opt;
  int flags;
  int connect_timeout = 5;
  struct pollfd po[1] = { {0, 0, 0} };
  char msg[1024];
  int msg_size;

  if (tcp_Send_fd >= 0)
    {
      return 0;
    }

  ip_addr = hostname_to_ip (tcp_Send_connect_info->host);
  if (ip_addr == INADDR_NONE)
    {
      return -1;
    }

  tcp_Send_fd = socket (AF_INET, SOCK_STREAM, 0);
  if (tcp_Send_fd < 0)
    {
      return -1;
    }

  memset (&sock_addr, 0, sizeof (struct sockaddr_in));
  sock_addr.sin_family = AF_INET;
  sock_addr.sin_port = htons ((unsigned short) tcp_Send_connect_info->port);
  memcpy (&sock_addr.sin_addr, &ip_addr, 4);
  sock_addr_len = sizeof (struct sockaddr_in);

  flags = fcntl (tcp_Send_fd, F_GETFL);
  fcntl (tcp_Send_fd, F_SETFL, flags | O_NONBLOCK);

  ret = connect (tcp_Send_fd, (struct sockaddr *) &sock_addr, sock_addr_len);
  if (ret < 0)
    {
      if (errno == EINPROGRESS)
	{
	  po[0].fd = tcp_Send_fd;
	  po[0].events = POLLOUT;
	  po[0].revents = 0;

	  ret = poll (po, 1, connect_timeout);
	  if (ret <= 0 || po[0].revents & POLLERR || po[0].revents & POLLHUP)
	    {
	      goto connect_err;
	    }
	}
      else
	{
	  goto connect_err;
	}
    }

  if (fcntl (tcp_Send_fd, F_SETFL, flags) < 0)
    {
      assert (0);
    }

  sock_opt = 1;
  if (setsockopt (tcp_Send_fd, IPPROTO_TCP, TCP_NODELAY, (char *) &sock_opt,
		  sizeof (sock_opt)) < 0)
    {
      assert (0);
    }

  sock_opt = 1;
  if (setsockopt (tcp_Send_fd, SOL_SOCKET, SO_KEEPALIVE, (char *) &sock_opt,
		  sizeof (sock_opt)) < 0)
    {
      assert (0);
    }

  msg_size = snprintf (msg, sizeof (msg), "auth %s %s\n",
		       tcp_Send_connect_info->id, tcp_Send_connect_info->pw);
  assert (msg_size < (int) sizeof (msg));
  tcp_send (msg, msg_size);

  return (tcp_Send_fd >= 0 ? 0 : -1);

connect_err:
  tcp_close ();
  return -1;
}

static int
make_connect_info (char *arg)
{
  int port = 0;
  char *save_ptr = NULL;
  char *p;
  char *host, *id, *pw;

  if (arg == NULL || *arg == '\0')
    {
      arg = getenv (TCP_CONNECT_HOST_ENV);
    }

  if (arg == NULL)
    {
      return -1;
    }

  host = strtok_r (arg, ":", &save_ptr);
  if (host == NULL || *host == '\0')
    {
      return -1;
    }

  p = strtok_r (NULL, ":", &save_ptr);
  if (p != NULL)
    {
      char *end_p = NULL;
      str_to_int32 (&port, &end_p, p, 10);
    }
  if (port <= 0)
    {
      return -1;
    }

  id = strtok_r (NULL, ":", &save_ptr);
  pw = strtok_r (NULL, ":", &save_ptr);

  tcp_Send_connect_info = malloc (sizeof (T_TCP_SEND_CONNECT_INFO));
  if (tcp_Send_connect_info == NULL)
    {
      return -1;
    }

  tcp_Send_connect_info->host = host;
  tcp_Send_connect_info->port = port;
  tcp_Send_connect_info->id = (id ? id : "");
  tcp_Send_connect_info->pw = (pw ? pw : "");

  return 0;
}

/*
 * overwrite cs functions
 */

#define CS_FUNC_ER_SET er_set
void
CS_FUNC_ER_SET (UNUSED_ARG int severity, UNUSED_ARG const char *file_name,
		UNUSED_ARG const int line_no, UNUSED_ARG int err_id,
		UNUSED_ARG int num_args, ...)
{
}

#define CS_FUNC_ER_SET_WITH_OSERROR er_set_with_oserror
void
CS_FUNC_ER_SET_WITH_OSERROR (UNUSED_ARG int severity,
			     UNUSED_ARG const char *file_name,
			     UNUSED_ARG const int line_no,
			     UNUSED_ARG int err_id, UNUSED_ARG int num_args,
			     ...)
{
}

#define CS_FUNC_ER_SET_ERROR_POSITION er_set_error_position
int
CS_FUNC_ER_SET_ERROR_POSITION (UNUSED_ARG const char *file_name,
			       UNUSED_ARG int line_no)
{
  return -1;
}

#define CS_FUNC_ER_LOG_DEBUG _er_log_debug
void
CS_FUNC_ER_LOG_DEBUG (UNUSED_ARG const char *file_name,
		      UNUSED_ARG const int line_no,
		      UNUSED_ARG const char *fmt, ...)
{
}

#define CS_FUNC_ER_DATETIME er_datetime
int
CS_FUNC_ER_DATETIME (UNUSED_ARG struct timeval *tv_p, UNUSED_ARG char *tmbuf,
		     UNUSED_ARG int tmbuf_size)
{
  return 0;
}

#define CS_FUNC_PRM_GET_STRING_VALUE prm_get_string_value
char *
CS_FUNC_PRM_GET_STRING_VALUE (UNUSED_ARG PARAM_ID prm_id)
{
  assert (0);
  return NULL;
}

#define CS_FUNC_PRM_GET_BOOL_VALUE prm_get_bool_value
bool
CS_FUNC_PRM_GET_BOOL_VALUE (UNUSED_ARG PARAM_ID prm_id)
{
  assert (0);
  return true;
}

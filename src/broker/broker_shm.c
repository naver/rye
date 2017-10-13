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
 * broker_shm.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <sys/types.h>
#include <errno.h>
#include <assert.h>

#include <sys/ipc.h>
#include <sys/shm.h>
#include <netdb.h>

#include "cas_common.h"
#include "cas_error.h"
#include "broker_shm.h"
#include "broker_filename.h"
#include "broker_util.h"
#include "error_code.h"
#include "error_manager.h"

#define 	SHMODE			0644

static int get_host_ip (unsigned char *ip_addr);

static void broker_shm_set_as_info (T_APPL_SERVER_INFO * as_info_p,
				    T_BROKER_INFO * br_info_p, int as_index);

static const char *get_appl_server_name (void);

T_SHM_BROKER *
br_shm_init_shm_broker (int shm_key_br_gl, T_BROKER_INFO * br_info,
			int br_num)
{
  int i;
  int shm_size;
  T_SHM_BROKER *shm_br = NULL;
  unsigned char ip_addr[4];

  if (get_host_ip (ip_addr) < 0)
    {
      return NULL;
    }

  shm_size = sizeof (T_SHM_BROKER) + (br_num - 1) * sizeof (T_BROKER_INFO);

  shm_br = rye_shm_create (shm_key_br_gl, shm_size,
			   RYE_SHM_TYPE_BROKER_GLOBAL);

  if (shm_br == NULL)
    {
      return NULL;
    }

  get_random_string (shm_br->broker_key, SHM_BROKER_KEY_LEN);
  shm_br->shm_header.status = RYE_SHM_VALID;

  memcpy (shm_br->my_ip_addr, ip_addr, 4);
  shm_br->owner_uid = getuid ();

  /* create a new session */
  setsid ();

  shm_br->num_broker = br_num;

  for (i = 0; i < br_num; i++)
    {
      shm_br->br_info[i] = br_info[i];
    }

  return shm_br;
}

T_SHM_APPL_SERVER *
br_shm_init_shm_as (T_BROKER_INFO * br_info_p, int shm_key_br_gl)
{
  int as_index;
  T_SHM_APPL_SERVER *shm_as_p = NULL;
  T_APPL_SERVER_INFO *as_info_p = NULL;
  int shm_size;

  shm_size = sizeof (T_SHM_APPL_SERVER);
  if (br_info_p->broker_type == NORMAL_BROKER)
    {
      shm_size += (sizeof (T_APPL_SERVER_INFO) * (APPL_SERVER_NUM_LIMIT - 1));
    }

  shm_as_p = rye_shm_create (br_info_p->appl_server_shm_key, shm_size,
			     RYE_SHM_TYPE_BROKER_LOCAL);

  if (shm_as_p == NULL)
    {
      return NULL;
    }

  shm_as_p->shm_header.status = RYE_SHM_VALID;

  shm_as_p->shm_key_br_global = shm_key_br_gl;

  shm_as_p->cci_default_autocommit = br_info_p->cci_default_autocommit;
  shm_as_p->job_queue_size = br_info_p->job_queue_size;
  shm_as_p->job_queue[0].id = 0;	/* initialize max heap */
  shm_as_p->max_prepared_stmt_count = br_info_p->max_prepared_stmt_count;

  shm_as_p->monitor_hang_flag = br_info_p->monitor_hang_flag;
  shm_as_p->monitor_server_flag = br_info_p->monitor_server_flag;
  memset (shm_as_p->unusable_databases_cnt, 0,
	  sizeof (shm_as_p->unusable_databases_cnt));

  strcpy (shm_as_p->log_dir, br_info_p->log_dir);
  strcpy (shm_as_p->broker_name, br_info_p->name);

  shm_as_p->broker_port = br_info_p->port;
  shm_as_p->num_appl_server = br_info_p->appl_server_num;
  shm_as_p->sql_log_mode = br_info_p->sql_log_mode;
  shm_as_p->broker_log_mode = br_info_p->broker_log_mode;
  shm_as_p->sql_log_max_size = br_info_p->sql_log_max_size;
  shm_as_p->broker_log_max_size = br_info_p->broker_log_max_size;
  shm_as_p->long_query_time = br_info_p->long_query_time;
  shm_as_p->long_transaction_time = br_info_p->long_transaction_time;
  shm_as_p->appl_server_max_size = br_info_p->appl_server_max_size;
  shm_as_p->appl_server_hard_limit = br_info_p->appl_server_hard_limit;
  shm_as_p->session_timeout = br_info_p->session_timeout;
  shm_as_p->slow_log_mode = br_info_p->slow_log_mode;
  shm_as_p->query_timeout = br_info_p->query_timeout;
  shm_as_p->max_string_length = br_info_p->max_string_length;
  shm_as_p->keep_connection = br_info_p->keep_connection;
  shm_as_p->statement_pooling = br_info_p->statement_pooling;
  shm_as_p->access_mode = br_info_p->access_mode;
  shm_as_p->access_log = br_info_p->access_log;
  shm_as_p->access_log_max_size = br_info_p->access_log_max_size;

  shm_as_p->connect_order_random = br_info_p->connect_order_random;
  shm_as_p->replica_only_flag = br_info_p->replica_only_flag;

  shm_as_p->max_num_delayed_hosts_lookup =
    br_info_p->max_num_delayed_hosts_lookup;

  shm_as_p->cas_rctime = br_info_p->cas_rctime;

  strcpy (shm_as_p->preferred_hosts, br_info_p->preferred_hosts);
  strcpy (shm_as_p->appl_server_name, get_appl_server_name ());

  for (as_index = 0; as_index < br_info_p->appl_server_max_num; as_index++)
    {
      as_info_p = &(shm_as_p->info.as_info[as_index]);
      broker_shm_set_as_info (as_info_p, br_info_p, as_index);
    }

  return shm_as_p;
}

static void
broker_shm_set_as_info (T_APPL_SERVER_INFO * as_info_p,
			T_BROKER_INFO * br_info_p, int as_index)
{
  as_info_p->service_flag = SERVICE_OFF;
  as_info_p->last_access_time = time (NULL);
  as_info_p->transaction_start_time = (time_t) 0;

  as_info_p->mutex_flag[SHM_MUTEX_BROKER] = FALSE;
  as_info_p->mutex_flag[SHM_MUTEX_ADMIN] = FALSE;
  as_info_p->mutex_turn = SHM_MUTEX_BROKER;

  as_info_p->num_request = 0;
  as_info_p->num_requests_received = 0;
  as_info_p->num_transactions_processed = 0;
  as_info_p->num_queries_processed = 0;
  as_info_p->num_long_queries = 0;
  as_info_p->num_long_transactions = 0;
  as_info_p->num_error_queries = 0;
  as_info_p->num_interrupts = 0;
  as_info_p->num_connect_requests = 0;
  as_info_p->num_connect_rejected = 0;
  as_info_p->num_restarts = 0;
  as_info_p->database_name[0] = '\0';
  as_info_p->database_host[0] = '\0';
  as_info_p->database_user[0] = '\0';
  as_info_p->last_connect_time = 0;
  as_info_p->num_holdable_results = 0;
  as_info_p->cas_change_mode = CAS_CHANGE_MODE_DEFAULT;
  as_info_p->cur_sql_log_mode = br_info_p->sql_log_mode;
  as_info_p->cur_slow_log_mode = br_info_p->slow_log_mode;
  as_info_p->pid = 0;

  as_info_p->as_id = as_index;
  return;
}

static int
get_host_ip (unsigned char *ip_addr)
{
  char hostname[64];
  struct hostent *hp;

  if (gethostname (hostname, sizeof (hostname)) < 0)
    {
      fprintf (stderr, "gethostname error\n");
      return -1;
    }
  if ((hp = gethostbyname (hostname)) == NULL)
    {
      fprintf (stderr, "unknown host : %s\n", hostname);
      return -1;
    }
  memcpy ((void *) ip_addr, (void *) hp->h_addr_list[0], 4);

  return 0;
}

int
br_sem_init (sem_t * sem)
{
  return sem_init (sem, 1, 1);
}

int
br_sem_wait (sem_t * sem)
{
  return sem_wait (sem);
}

int
br_sem_post (sem_t * sem)
{
  return sem_post (sem);
}

int
br_sem_destroy (sem_t * sem)
{
  return sem_destroy (sem);
}

static const char *
get_appl_server_name ()
{
  return APPL_SERVER_CAS_NAME;
}

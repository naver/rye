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
 * network_interface_sr.c - Server side network interface functions
 *                          for client requests.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "porting.h"
#include "perf_monitor.h"
#include "memory_alloc.h"
#include "storage_common.h"
#include "xserver_interface.h"
#include "statistics_sr.h"
#include "btree_load.h"
#include "log_impl.h"
#include "boot_sr.h"
#include "locator_sr.h"
#include "server_interface.h"
#include "oid.h"
#include "error_manager.h"
#include "object_representation.h"
#include "network.h"
#include "log_comm.h"
#include "network_interface_sr.h"
#include "page_buffer.h"
#include "file_manager.h"
#include "boot_sr.h"
#include "arithmetic.h"
#include "query_manager.h"
#include "transaction_sr.h"
#include "release_string.h"
#include "thread.h"
#include "critical_section.h"
#include "statistics.h"
#include "chartype.h"
#include "heap_file.h"
#include "repl_log.h"
#include "server_support.h"
#include "connection_sr.h"
#include "repl_log_writer_sr.h"
#include "databases_file.h"
#include "event_log.h"
#include "backup_sr.h"

#include "rye_server_shm.h"

#define NET_COPY_AREA_SENDRECV_SIZE (OR_INT_SIZE * 3)
#define NET_SENDRECV_BUFFSIZE (OR_INT_SIZE)

#define STATDUMP_BUF_SIZE (4096)
#define QUERY_INFO_BUF_SIZE (2048 + STATDUMP_BUF_SIZE)

/* This file is only included in the server.  So set the on_server flag on */
unsigned int db_on_server = 1;

static bool need_to_abort_tran (THREAD_ENTRY * thread_p, int *errid);
static int server_capabilities (void);
static int check_client_capabilities (THREAD_ENTRY * thread_p, int client_cap,
				      const char *client_host);
static int er_log_slow_query (THREAD_ENTRY * thread_p,
			      EXECUTION_INFO * info,
			      int time,
			      MNT_SERVER_EXEC_STATS * diff_stats,
			      char *queryinfo_string);
static void event_log_slow_query (THREAD_ENTRY * thread_p,
				  EXECUTION_INFO * info,
				  int time,
				  MNT_SERVER_EXEC_STATS * diff_stats);
static void event_log_many_ioreads (THREAD_ENTRY * thread_p,
				    EXECUTION_INFO * info,
				    int time,
				    MNT_SERVER_EXEC_STATS * diff_stats);
static void event_log_temp_expand_pages (THREAD_ENTRY * thread_p,
					 EXECUTION_INFO * info);
static void check_reset_on_commit (THREAD_ENTRY * thread_p);

static FILE *dump_func_open_tmpfile (THREAD_ENTRY * thread_p,
				     unsigned int rid);
static void dump_func_send_result (THREAD_ENTRY * thread_p, unsigned int rid,
				   int send_chunk_size, FILE * tmpfp);


/*
 * need_to_abort_tran - check whether the transaction should be aborted
 *
 * return: true/false
 *
 *   thread_p(in): thread entry
 *   errid(out): the latest error code
 */
static bool
need_to_abort_tran (THREAD_ENTRY * thread_p, int *errid)
{
  LOG_TDES *tdes;
  bool flag_abort = false;

  assert (thread_p != NULL);


  *errid = er_errid ();
  if (*errid == ER_LK_UNILATERALLY_ABORTED
      || *errid == ER_DB_NO_MODIFICATIONS)
    {
      flag_abort = true;
    }

  /*
   * DEFENCE CODE:
   *  below block means ER_LK_UNILATERALLY_ABORTED occurs but another error
   *  set after that.
   *  So, re-set that error to rollback in client side.
   */
  tdes = logtb_get_current_tdes (thread_p);
  if (tdes != NULL && tdes->tran_abort_reason != TRAN_NORMAL
      && flag_abort == false)
    {
      flag_abort = true;

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_UNILATERALLY_ABORTED, 4,
	      thread_p->tran_index, tdes->client.db_user,
	      tdes->client.host_name, tdes->client.process_id);
    }

  return flag_abort;
}

/*
 * return_error_to_client -
 *
 * return:
 *
 *   rid(in):
 *
 * NOTE:
 */
void
return_error_to_client (THREAD_ENTRY * thread_p, unsigned int eid)
{
  LOG_TDES *tdes;
  int errid;
  bool flag_abort = false;
  void *area;
  char buffer[1024];
  int length = 1024;
  CSS_CONN_ENTRY *conn;

  assert (thread_p != NULL);

  conn = thread_p->conn_entry;
  assert (conn != NULL);

  tdes = logtb_get_current_tdes (thread_p);
  flag_abort = need_to_abort_tran (thread_p, &errid);

  /* check some errors which require special actions */
  /*
   * ER_LK_UNILATERALLY_ABORTED may have occurred due to deadlock.
   * If it happened, do server-side rollback of the transaction.
   * If ER_DB_NO_MODIFICATIONS error is occurred in server-side,
   * it means that the user tried to update the database
   * when the server was disabled to modify. (aka standby mode)
   */
  if (flag_abort == true)
    {
      tran_server_unilaterally_abort_tran (thread_p);
    }

  if (errid == ER_DB_NO_MODIFICATIONS)
    {
      conn->conn_reset_on_commit = true;
    }

  area = er_get_area_error (buffer, &length);
  if (area != NULL)
    {
      css_send_error_packet (conn, CSS_RID_FROM_EID (eid),
			     (char *) area, length);
    }

  if (tdes != NULL)
    {
      tdes->tran_abort_reason = TRAN_NORMAL;
    }
}

/*
 * server_capabilities -
 *
 * return:
 */
static int
server_capabilities (void)
{
  HA_STATE server_state = HA_STATE_UNKNOWN;
  int capabilities = 0;

  server_state = svr_shm_get_server_state ();
  if (server_state != HA_STATE_UNKNOWN)
    {
      capabilities |= NET_CAP_INTERRUPT_ENABLED;
    }

  if (db_Disable_modifications > 0 || server_state == HA_STATE_TO_BE_SLAVE)
    {
      capabilities |= NET_CAP_UPDATE_DISABLED;
    }
  if (css_is_ha_repl_delayed () == true)
    {
      capabilities |= NET_CAP_HA_REPL_DELAY;
    }
  if (prm_get_integer_value (PRM_ID_HA_MODE) == HA_MODE_REPLICA)
    {
      assert (svr_shm_get_server_state () == HA_STATE_SLAVE);
      capabilities |= NET_CAP_HA_REPLICA;
    }

  return capabilities;
}

/*
 * check_client_capabilities -
 *
 * return:
 *   client_cap(in): client capability
 *
 */
static int
check_client_capabilities (THREAD_ENTRY * thread_p, int client_cap,
			   const char *client_host)
{
  int server_cap;

  server_cap = server_capabilities ();
  /* interrupt-ability should be same */
  if ((server_cap ^ client_cap) & NET_CAP_INTERRUPT_ENABLED)
    {
      client_cap ^= NET_CAP_INTERRUPT_ENABLED;
    }

  if (client_cap & NET_CAP_HA_IGNORE_REPL_DELAY)
    {
      thread_p->conn_entry->ignore_repl_delay = true;

      er_log_debug (ARG_FILE_LINE,
		    "NET_CAP_HA_IGNORE_REPL_DELAY client %s %d\n",
		    client_host, client_cap & NET_CAP_HA_IGNORE_REPL_DELAY);
    }

  return client_cap;
}


/*
 * server_ping - return that the server is alive
 *   return:
 *   rid(in): request id
 */
void
server_ping (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
	     UNUSED_ARG int reqlen)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  int client_val, server_val;

  er_log_debug (ARG_FILE_LINE, "The server_ping() is called.");

  /* you can get something useful from the request */
  or_unpack_int (request, &client_val);

  /* you can envelope something useful into the reply */
  server_val = 0;
  or_pack_int (reply, server_val);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply, OR_INT_SIZE);
}

/*
 * server_ping_with_handshake -
 *
 * return:
 *
 *   rid(in): request id
 *   request(in):
 *   reqlen(in):
 *
 * NOTE: Handler for the SERVER_PING_WITH_HANDSHAKE request.
 *    We record the client's version string here and send back our own
 *    version string so the client can determine compatibility.
 */
int
server_ping_with_handshake (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, int reqlen)
{
  OR_ALIGNED_BUF (OR_VERSION_SIZE + (OR_INT_SIZE * 2) +
		  MAXHOSTNAMELEN) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  int reply_size = OR_ALIGNED_BUF_SIZE (a_reply);
  char *ptr;
  const char *client_host = "";
  int client_capabilities, client_bit_platform;
  int client_type;
  int strlen1;
  RYE_VERSION client_version;
  RYE_VERSION server_version = rel_cur_version ();

  if (reqlen > 0)
    {
      ptr = or_unpack_version (request, &client_version);
      ptr = or_unpack_int (ptr, &client_capabilities);
      ptr = or_unpack_int (ptr, &client_bit_platform);
      ptr = or_unpack_int (ptr, &client_type);
      ptr = or_unpack_string_nocopy (ptr, &client_host);

      css_set_client_version (thread_p, &client_version);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_SERVER_HAND_SHAKE, 1,
	      client_host);
      return_error_to_client (thread_p, rid);
      return CSS_UNPLANNED_SHUTDOWN;
    }

  /* check bits model */
  if (client_bit_platform != rel_bit_platform ())
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_DIFFERENT_BIT_PLATFORM,
	      2, rel_bit_platform (), client_bit_platform);
      return_error_to_client (thread_p, rid);
      return CSS_UNPLANNED_SHUTDOWN;
    }

  /*
   * 1. get the result of compatibility check.
   * 2. check if the both capabilities of client and server are compatible.
   * 3. check if the client has a capability to make it compatible.
   */
  if (rel_check_net_compatible (&client_version,
				&server_version) == REL_NOT_COMPATIBLE)
    {
      char buf1[REL_MAX_VERSION_LENGTH];
      char buf2[REL_MAX_VERSION_LENGTH];

      rel_version_to_string (&server_version, buf1, sizeof (buf1));
      rel_version_to_string (&client_version, buf2, sizeof (buf2));

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_DIFFERENT_RELEASE, 2,
	      buf1, buf2);
      return_error_to_client (thread_p, rid);
      return CSS_UNPLANNED_SHUTDOWN;
    }

  if (check_client_capabilities (thread_p, client_capabilities,
				 client_host) != client_capabilities)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_SERVER_HAND_SHAKE, 1,
	      client_host);
      return_error_to_client (thread_p, rid);
      return CSS_UNPLANNED_SHUTDOWN;
    }

  /* update connection counters for reserved connections */
  if (css_increment_num_conn (client_type) != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_CSS_CLIENTS_EXCEEDED, 1, NUM_NORMAL_TRANS);
      return_error_to_client (thread_p, rid);
      return CSS_UNPLANNED_SHUTDOWN;
    }

  thread_p->conn_entry->client_type = client_type;

  reply_size = OR_VERSION_SIZE + (OR_INT_SIZE * 2) +
    or_packed_string_length (boot_Host_name, &strlen1);
  ptr = or_pack_version (reply, &server_version);
  ptr = or_pack_int (ptr, server_capabilities ());
  ptr = or_pack_int (ptr, rel_bit_platform ());
  ptr = or_pack_string_with_length (ptr, boot_Host_name, strlen1);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply, reply_size);

  return CSS_NO_ERRORS;
}

/*
 * slocator_fetch -
 *
 * return:
 *
 *   thrd(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_fetch (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
		UNUSED_ARG int reqlen)
{
  OID oid;
  LOCK lock;
  OID class_oid;
  int prefetch;
  LC_COPYAREA *copy_area;
  char *ptr;
  int success;
  OR_ALIGNED_BUF (NET_COPY_AREA_SENDRECV_SIZE + OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *desc_ptr = NULL;
  int desc_size;
  char *content_ptr;
  int content_size;
  int num_objs = 0;

  ptr = or_unpack_oid (request, &oid);
  ptr = or_unpack_lock (ptr, &lock);
  ptr = or_unpack_oid (ptr, &class_oid);
  ptr = or_unpack_int (ptr, &prefetch);

  copy_area = NULL;
  success = xlocator_fetch (thread_p, &oid, lock, &class_oid, prefetch,
			    &copy_area);

  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  if (copy_area != NULL)
    {
      num_objs = locator_send_copy_area (copy_area, &content_ptr,
					 &content_size, &desc_ptr,
					 &desc_size);
    }
  else
    {
      desc_ptr = NULL;
      desc_size = 0;
      content_ptr = NULL;
      content_size = 0;
    }

  /* Send sizes of databuffer and copy area (descriptor + content) */

  ptr = or_pack_int (reply, num_objs);
  ptr = or_pack_int (ptr, desc_size);
  ptr = or_pack_int (ptr, content_size);
  ptr = or_pack_int (ptr, success);

  if (copy_area == NULL)
    {
      css_send_reply_to_client (thread_p->conn_entry, rid, 1,
				reply, OR_ALIGNED_BUF_SIZE (a_reply));
    }
  else
    {
      css_send_reply_to_client (thread_p->conn_entry, rid, 3,
				reply, OR_ALIGNED_BUF_SIZE (a_reply),
				desc_ptr, desc_size,
				content_ptr, content_size);
      locator_free_copy_area (copy_area);
    }
  if (desc_ptr)
    {
      free_and_init (desc_ptr);
    }
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * slocator_get_class -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_get_class (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
		    UNUSED_ARG int reqlen)
{
  OID class_oid, oid;
  LOCK lock;
  int prefetching;
  LC_COPYAREA *copy_area;
  int success;
  char *ptr;
  OR_ALIGNED_BUF (NET_COPY_AREA_SENDRECV_SIZE + OR_OID_SIZE +
		  OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *desc_ptr = NULL;
  int desc_size;
  char *content_ptr;
  int content_size;
  int num_objs = 0;

  ptr = or_unpack_oid (request, &class_oid);
  ptr = or_unpack_oid (ptr, &oid);
  ptr = or_unpack_lock (ptr, &lock);
  ptr = or_unpack_int (ptr, &prefetching);

  copy_area = NULL;
  success = xlocator_get_class (thread_p, &class_oid, &oid, lock,
				prefetching, &copy_area);
  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  if (copy_area != NULL)
    {
      num_objs = locator_send_copy_area (copy_area, &content_ptr,
					 &content_size, &desc_ptr,
					 &desc_size);
    }
  else
    {
      desc_ptr = NULL;
      desc_size = 0;
      content_ptr = NULL;
      content_size = 0;
    }

  /* Send sizes of databuffer and copy area (descriptor + content) */

  ptr = or_pack_int (reply, num_objs);
  ptr = or_pack_int (ptr, desc_size);
  ptr = or_pack_int (ptr, content_size);
  ptr = or_pack_oid (ptr, &class_oid);
  ptr = or_pack_int (ptr, success);

  if (copy_area == NULL)
    {
      css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
				OR_ALIGNED_BUF_SIZE (a_reply));
    }
  else
    {
      css_send_reply_to_client (thread_p->conn_entry, rid, 3,
				reply, OR_ALIGNED_BUF_SIZE (a_reply),
				desc_ptr, desc_size,
				content_ptr, content_size);
      locator_free_copy_area (copy_area);
    }

  if (desc_ptr)
    {
      free_and_init (desc_ptr);
    }
}
#endif

/*
 * slocator_fetch_all -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_fetch_all (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
		    UNUSED_ARG int reqlen)
{
  HFID hfid;
  LOCK lock;
  OID class_oid, last_oid;
  INT64 nobjects, nfetched;
  LC_COPYAREA *copy_area;
  int success;
  char *ptr;
  OR_ALIGNED_BUF (NET_COPY_AREA_SENDRECV_SIZE + (OR_INT_SIZE * 2) +
		  (OR_BIGINT_ALIGNED_SIZE * 2) + OR_OID_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *desc_ptr = NULL;
  int desc_size;
  char *content_ptr;
  int content_size;
  int num_objs = 0;

  ptr = or_unpack_hfid (request, &hfid);
  ptr = or_unpack_lock (ptr, &lock);
  ptr = or_unpack_oid (ptr, &class_oid);

  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_unpack_int64 (ptr, &nobjects);
  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_unpack_int64 (ptr, &nfetched);

  ptr = or_unpack_oid (ptr, &last_oid);

  copy_area = NULL;
  success = xlocator_fetch_all (thread_p, &hfid, &lock, &class_oid, &nobjects,
				&nfetched, &last_oid, &copy_area);

  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  if (copy_area != NULL)
    {
      num_objs = locator_send_copy_area (copy_area, &content_ptr,
					 &content_size, &desc_ptr,
					 &desc_size);
    }
  else
    {
      desc_ptr = NULL;
      desc_size = 0;
      content_ptr = NULL;
      content_size = 0;
    }

  /* Send sizes of databuffer and copy area (descriptor + content) */

  ptr = or_pack_int (reply, num_objs);
  ptr = or_pack_int (ptr, desc_size);
  ptr = or_pack_int (ptr, content_size);

  ptr = or_pack_lock (ptr, lock);

  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_pack_int64 (ptr, nobjects);
  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_pack_int64 (ptr, nfetched);

  ptr = or_pack_oid (ptr, &last_oid);
  ptr = or_pack_int (ptr, success);

  if (copy_area == NULL)
    {
      css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
				OR_ALIGNED_BUF_SIZE (a_reply));
    }
  else
    {
      css_send_reply_to_client (thread_p->conn_entry, rid, 3,
				reply, OR_ALIGNED_BUF_SIZE (a_reply),
				desc_ptr, desc_size,
				content_ptr, content_size);
      locator_free_copy_area (copy_area);
    }

  if (desc_ptr)
    {
      free_and_init (desc_ptr);
    }
}

/*
 * slocator_repl_force - process log applier's request to replicate data
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_repl_force (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
		     UNUSED_ARG int reqlen)
{
  int success;
  LC_COPYAREA *copy_area = NULL, *reply_copy_area = NULL;
  char *ptr;
  OR_ALIGNED_BUF (NET_COPY_AREA_SENDRECV_SIZE + OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  int content_size;
  char *content_ptr = NULL, *new_content_ptr = NULL;
  char *reply_content_ptr = NULL;
  int num_objs;
  char *packed = NULL;
  int packed_size;
  char *desc_ptr = NULL;
  int desc_size;
  UNUSED_VAR LC_COPYAREA_MANYOBJS *mobjs;
  LC_COPYAREA_MANYOBJS *reply_mobjs;
  int error;
  bool old_check_groupid;
  CSS_NET_PACKET *recv_packet;

  recv_packet = thread_p->recv_packet;

  ptr = or_unpack_int (request, &num_objs);
  ptr = or_unpack_int (ptr, &packed_size);
  ptr = or_unpack_int (ptr, &content_size);

  error = NO_ERROR;

  copy_area = locator_recv_allocate_copyarea (num_objs, &content_ptr,
					      content_size);
  if (copy_area)
    {
      if (num_objs > 0)
	{
	  packed = css_net_packet_get_buffer (recv_packet, 1,
					      packed_size, false);
	  if (packed == NULL)
	    {
	      error = ER_NET_SERVER_DATA_RECEIVE;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	      css_send_abort_to_client (thread_p->conn_entry, rid);
	      goto end;
	    }
	}

      locator_unpack_copy_area_descriptor (num_objs, copy_area, packed);
      mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (copy_area);

      if (content_size > 0)
	{
	  new_content_ptr = css_net_packet_get_buffer (recv_packet, 2,
						       content_size, false);

	  if (new_content_ptr == NULL)
	    {
	      error = ER_NET_SERVER_DATA_RECEIVE;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	      css_send_abort_to_client (thread_p->conn_entry, rid);
	      goto end;
	    }

	  memcpy (content_ptr, new_content_ptr, content_size);


	  /* make copy_area (estimated size) to report errors */
	  reply_copy_area =
	    locator_recv_allocate_copyarea (num_objs,
					    &reply_content_ptr,
					    content_size +
					    OR_INT_SIZE * num_objs);
	  reply_mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (reply_copy_area);
	  reply_mobjs->num_objs = 0;
	}

      /* client is HA applier
       * do not filter groupid
       */
      assert (LOG_IS_HA_CLIENT (logtb_find_current_client_type (thread_p)));

      old_check_groupid = thread_set_check_groupid (thread_p, false);

      success = xlocator_repl_force (thread_p, copy_area, &reply_copy_area);

      (void) thread_set_check_groupid (thread_p, old_check_groupid);

      /*
       * Send the descriptor and content to handle errors
       */

      num_objs = locator_send_copy_area (reply_copy_area, &reply_content_ptr,
					 &content_size, &desc_ptr,
					 &desc_size);

      ptr = or_pack_int (reply, num_objs);
      ptr = or_pack_int (ptr, desc_size);
      ptr = or_pack_int (ptr, content_size);
      ptr = or_pack_int (ptr, success);

      if (success != NO_ERROR && success != ER_LC_PARTIALLY_FAILED_TO_FLUSH)
	{
	  return_error_to_client (thread_p, rid);
	}

      css_send_reply_to_client (thread_p->conn_entry, rid, 3,
				reply, OR_ALIGNED_BUF_SIZE (a_reply),
				desc_ptr, desc_size,
				reply_content_ptr, content_size);
      if (desc_ptr != NULL)
	{
	  free_and_init (desc_ptr);
	}
    }

end:
  if (copy_area != NULL)
    {
      locator_free_copy_area (copy_area);
    }
  if (reply_copy_area != NULL)
    {
      locator_free_copy_area (reply_copy_area);
    }

  return;
}

/*
 * slocator_force -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_force (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
		UNUSED_ARG int reqlen)
{
  int success;
  LC_COPYAREA *copy_area = NULL;
  char *ptr;
  int error;
  OR_ALIGNED_BUF (OR_INT_SIZE * 3) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  int content_size;
  char *content_ptr = NULL, *new_content_ptr = NULL;
  int num_objs;
  char *packed_desc = NULL;
  int packed_desc_size;
  UNUSED_VAR LC_COPYAREA_MANYOBJS *mobjs;
  CSS_NET_PACKET *recv_packet = thread_p->recv_packet;

  ptr = or_unpack_int (request, &num_objs);
  ptr = or_unpack_int (ptr, &packed_desc_size);
  ptr = or_unpack_int (ptr, &content_size);

  error = NO_ERROR;

  copy_area = locator_recv_allocate_copyarea (num_objs, &content_ptr,
					      content_size);
  if (copy_area)
    {
      if (num_objs > 0)
	{
	  packed_desc = css_net_packet_get_buffer (recv_packet, 1,
						   packed_desc_size, false);
	  if (packed_desc == NULL)
	    {
	      error = ER_NET_SERVER_DATA_RECEIVE;
	    }
	}

      if (error)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  css_send_abort_to_client (thread_p->conn_entry, rid);
	  goto end;
	}
      else
	{
	  locator_unpack_copy_area_descriptor (num_objs, copy_area,
					       packed_desc);
	  mobjs = LC_MANYOBJS_PTR_IN_COPYAREA (copy_area);

	  if (content_size > 0)
	    {
	      new_content_ptr = css_net_packet_get_buffer (recv_packet, 2,
							   content_size,
							   false);
	      if (new_content_ptr == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_NET_SERVER_DATA_RECEIVE, 0);
		  css_send_abort_to_client (thread_p->conn_entry, rid);
		  goto end;
		}

	      memcpy (content_ptr, new_content_ptr, content_size);
	    }

	  success = xlocator_force (thread_p, copy_area);

	  /*
	   * Send the descriptor part since some information about the objects
	   * (e.g., OIDs) may be send back to client.
	   * Don't need to send the content since it is not updated.
	   */

	  locator_pack_copy_area_descriptor (num_objs, copy_area,
					     packed_desc);
	  ptr = or_pack_int (reply, success);
	  ptr = or_pack_int (ptr, packed_desc_size);
	  ptr = or_pack_int (ptr, 0);

	  if (success != NO_ERROR
	      && success != ER_LC_PARTIALLY_FAILED_TO_FLUSH)
	    {
	      return_error_to_client (thread_p, rid);
	    }

	  css_send_reply_to_client (thread_p->conn_entry, rid, 3,
				    reply, OR_ALIGNED_BUF_SIZE (a_reply),
				    packed_desc, packed_desc_size, NULL, 0);
	}
    }

end:
  if (copy_area != NULL)
    {
      locator_free_copy_area (copy_area);
    }
}

/*
 * slocator_fetch_lockset -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_fetch_lockset (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, UNUSED_ARG int reqlen)
{
  int success;
  LC_COPYAREA *copy_area;
  LC_LOCKSET *lockset;
  OR_ALIGNED_BUF (NET_SENDRECV_BUFFSIZE + NET_COPY_AREA_SENDRECV_SIZE +
		  OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *desc_ptr;
  int desc_size;
  char *content_ptr;
  int content_size;
  char *ptr;
  bool first_call;
  int num_objs;
  char *packed = NULL;
  int packed_size;
  int send_size;
  CSS_NET_PACKET *recv_packet = thread_p->recv_packet;

  ptr = or_unpack_int (request, &packed_size);

  packed = css_net_packet_get_buffer (recv_packet, 1, packed_size, false);

  if (packed_size == 0 || packed == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_SERVER_DATA_RECEIVE,
	      0);
      css_send_abort_to_client (thread_p->conn_entry, rid);
      return;
    }

  lockset = locator_allocate_and_unpack_lockset (packed, packed_size, true,
						 true, false);
  if ((lockset == NULL) || (lockset->length <= 0))
    {
      return_error_to_client (thread_p, rid);
      ptr = or_pack_int (reply, 0);
      ptr = or_pack_int (ptr, 0);
      ptr = or_pack_int (ptr, 0);
      ptr = or_pack_int (ptr, 0);
      ptr = or_pack_int (ptr, ER_FAILED);
      css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
				OR_ALIGNED_BUF_SIZE (a_reply));
      return;
    }

  assert (lockset != NULL);

  first_call = true;
  do
    {
      desc_ptr = NULL;
      num_objs = 0;

      copy_area = NULL;
      success = xlocator_fetch_lockset (thread_p, lockset, &copy_area);

      if (success != NO_ERROR)
	{
	  return_error_to_client (thread_p, rid);
	}

      if (copy_area != NULL)
	{
	  num_objs = locator_send_copy_area (copy_area, &content_ptr,
					     &content_size, &desc_ptr,
					     &desc_size);
	}
      else
	{
	  desc_ptr = NULL;
	  desc_size = 0;
	  content_ptr = NULL;
	  content_size = 0;
	}

      /* Send sizes of data buffer and copy area (descriptor + content) */

      send_size = locator_pack_lockset (lockset, first_call, false);

      packed = lockset->packed;
      packed_size = lockset->packed_size;

      ptr = or_pack_int (reply, send_size);
      ptr = or_pack_int (ptr, num_objs);
      ptr = or_pack_int (ptr, desc_size);
      ptr = or_pack_int (ptr, content_size);

      ptr = or_pack_int (ptr, success);

      css_send_reply_to_client (thread_p->conn_entry, rid, 4,
				reply, OR_ALIGNED_BUF_SIZE (a_reply),
				packed, send_size,
				desc_ptr, desc_size,
				content_ptr, content_size);

      if (copy_area != NULL)
	{
	  locator_free_copy_area (copy_area);
	}
      if (desc_ptr)
	{
	  free_and_init (desc_ptr);
	}

      first_call = false;
    }
  while (copy_area
	 && ((lockset->num_classes_of_reqobjs >
	      lockset->num_classes_of_reqobjs_processed)
	     || (lockset->num_reqobjs > lockset->num_reqobjs_processed)));

  locator_free_lockset (lockset);
}

/*
 * slocator_find_class_oid -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_find_class_oid (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, UNUSED_ARG int reqlen)
{
  LC_FIND_CLASSNAME found;
  const char *classname;
  OID class_oid;
  LOCK lock;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_OID_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_string_nocopy (request, &classname);
  ptr = or_unpack_oid (ptr, &class_oid);
  ptr = or_unpack_lock (ptr, &lock);

  found = xlocator_find_class_oid (thread_p, classname, &class_oid, lock);

  if (found == LC_CLASSNAME_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  ptr = or_pack_int (reply, found);
  ptr = or_pack_oid (ptr, &class_oid);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * slocator_reserve_classnames -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_reserve_classnames (THREAD_ENTRY * thread_p, unsigned int rid,
			     char *request, UNUSED_ARG int reqlen)
{
  LC_FIND_CLASSNAME reserved = LC_CLASSNAME_ERROR;
  int num_classes;
  const char **classnames;
  OID *class_oids;
  char *ptr;
  int i;
  int malloc_size;
  char *malloc_area;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_int (request, &num_classes);

  malloc_size = ((sizeof (char *) + sizeof (OID)) * num_classes);
  malloc_area = (char *) malloc (malloc_size);
  if (malloc_area != NULL)
    {
      classnames = (const char **) malloc_area;
      class_oids = (OID *) ((char *) malloc_area +
			    (sizeof (char *) * num_classes));
      for (i = 0; i < num_classes; i++)
	{
	  ptr = or_unpack_string_nocopy (ptr, &classnames[i]);
	  ptr = or_unpack_oid (ptr, &class_oids[i]);
	}
      reserved = xlocator_reserve_class_names (thread_p, num_classes,
					       classnames, class_oids);
    }

  if (reserved == LC_CLASSNAME_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, reserved);

  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));

  if (malloc_area)
    {
      free_and_init (malloc_area);
    }
}

/*
 * slocator_delete_class_name -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_delete_class_name (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, UNUSED_ARG int reqlen)
{
  const char *classname;
  LC_FIND_CLASSNAME deleted;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_unpack_string_nocopy (request, &classname);

  deleted = xlocator_delete_class_name (thread_p, classname);
  if (deleted == LC_CLASSNAME_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, deleted);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * slocator_rename_class_name -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_rename_class_name (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, UNUSED_ARG int reqlen)
{
  const char *oldname, *newname;
  OID class_oid;
  LC_FIND_CLASSNAME renamed;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_string_nocopy (request, &oldname);
  ptr = or_unpack_string_nocopy (ptr, &newname);
  ptr = or_unpack_oid (ptr, &class_oid);

  renamed = xlocator_rename_class_name (thread_p, oldname, newname,
					&class_oid);
  if (renamed == LC_CLASSNAME_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, renamed);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * slocator_assign_oid -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_assign_oid (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
		     UNUSED_ARG int reqlen)
{
  HFID hfid;
  int expected_length;
  OID class_oid, perm_oid;
  const char *classname;
  int success;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_OID_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_hfid (request, &hfid);
  ptr = or_unpack_int (ptr, &expected_length);
  ptr = or_unpack_oid (ptr, &class_oid);
  ptr = or_unpack_string_nocopy (ptr, &classname);

  success = (xlocator_assign_oid (thread_p, &hfid, &perm_oid, expected_length,
				  &class_oid, classname) == NO_ERROR)
    ? NO_ERROR : ER_FAILED;
  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  ptr = or_pack_int (reply, success);
  ptr = or_pack_oid (ptr, &perm_oid);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sqst_server_get_statistics -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sqst_server_get_statistics (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, UNUSED_ARG int reqlen)
{
  OID classoid;
  unsigned int timestamp;
  char *ptr;
  char *buffer;
  int buffer_length;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_oid (request, &classoid);
  ptr = or_unpack_int (ptr, (int *) &timestamp);

  buffer = xstats_get_statistics_from_server (thread_p, &classoid, timestamp,
					      &buffer_length);
  if (buffer == NULL && buffer_length < 0)
    {
      return_error_to_client (thread_p, rid);
      buffer_length = 0;
    }

  (void) or_pack_int (reply, buffer_length);

  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    buffer, buffer_length);
  if (buffer != NULL)
    {
      /* since this was copied to the client, we don't need it on the server */
      free_and_init (buffer);
    }
}

/*
 * slog_checkpoint -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slog_checkpoint (THREAD_ENTRY * thread_p, unsigned int rid,
		 UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  logpb_do_checkpoint ();

  (void) or_pack_int (reply, NO_ERROR);	/* dummy reply */

  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * slogtb_has_updated -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slogtb_has_updated (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
		    int reqlen)
{
  int has_updated;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  has_updated = logtb_has_updated (thread_p);

  (void) or_pack_int (reply, has_updated);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}
#endif /* ENABLE_UNUSED_FUNCTION */
/*
 * slogtb_set_interrupt -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slogtb_set_interrupt (THREAD_ENTRY * thread_p, UNUSED_ARG unsigned int rid,
		      char *request, UNUSED_ARG int reqlen)
{
  int set;

  (void) or_unpack_int (request, &set);
  xlogtb_set_interrupt (thread_p, set);

  /*
   *  No reply expected...
   */
}

/*
 * slogtb_set_suppress_repl_on_transaction -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slogtb_set_suppress_repl_on_transaction (THREAD_ENTRY * thread_p,
					 unsigned int rid, char *request,
					 UNUSED_ARG int reqlen)
{
  int set;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_unpack_int (request, &set);
  xlogtb_set_suppress_repl_on_transaction (thread_p, set);

  /* always success */
  (void) or_pack_int (reply, NO_ERROR);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}



/*
 * slogtb_reset_wait_msecs -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slogtb_reset_wait_msecs (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, UNUSED_ARG int reqlen)
{
  int wait_msecs;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_unpack_int (request, &wait_msecs);

  wait_msecs = xlogtb_reset_wait_msecs (thread_p, wait_msecs);

  (void) or_pack_int (reply, wait_msecs);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * slogtb_reset_isolation -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slogtb_reset_isolation (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, int reqlen)
{
  int isolation, error_code;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;

  ptr = or_unpack_int (request, &isolation);

  error_code = (int) xlogtb_reset_isolation (thread_p,
					     (TRAN_ISOLATION) isolation);
  if (error_code != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, error_code);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}
#endif

/*
 * slogpb_dump_stat -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slogpb_dump_stat (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
		  UNUSED_ARG int reqlen)
{
  FILE *outfp;
  int buffer_size;

  (void) or_unpack_int (request, &buffer_size);

  outfp = dump_func_open_tmpfile (thread_p, rid);
  if (outfp == NULL)
    {
      return;
    }

  xlogpb_dump_stat (outfp);

  dump_func_send_result (thread_p, rid, buffer_size, outfp);
}

/*
 * sacl_reload -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sacl_reload (THREAD_ENTRY * thread_p, unsigned int rid,
	     UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  int error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  error = xacl_reload (thread_p);
  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_errcode (reply, error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sacl_dump -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sacl_dump (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
	   UNUSED_ARG int reqlen)
{
  FILE *outfp;
  int buffer_size;

  (void) or_unpack_int (request, &buffer_size);

  outfp = dump_func_open_tmpfile (thread_p, rid);
  if (outfp == NULL)
    {
      return;
    }

  xacl_dump (thread_p, outfp);

  dump_func_send_result (thread_p, rid, buffer_size, outfp);
}

/*
 * slock_dump -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slock_dump (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
	    UNUSED_ARG int reqlen)
{
  FILE *outfp;
  int buffer_size;

  (void) or_unpack_int (request, &buffer_size);

  outfp = dump_func_open_tmpfile (thread_p, rid);
  if (outfp == NULL)
    {
      return;
    }

  xlock_dump (thread_p, outfp);

  dump_func_send_result (thread_p, rid, buffer_size, outfp);
}

/*
 * sheap_create -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sheap_create (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
	      UNUSED_ARG int reqlen)
{
  int error;
  HFID hfid;
  char *ptr;
  OID class_oid;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_HFID_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_hfid (request, &hfid);
  ptr = or_unpack_oid (ptr, &class_oid);

  error = xheap_create (thread_p, &hfid, &class_oid);
  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  ptr = or_pack_errcode (reply, error);
  ptr = or_pack_hfid (ptr, &hfid);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sheap_destroy -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sheap_destroy (THREAD_ENTRY * thread_p, unsigned int rid,
	       char *request, UNUSED_ARG int reqlen)
{
  int error;
  HFID hfid;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_unpack_hfid (request, &hfid);

  error = xheap_destroy (thread_p, &hfid);
  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_errcode (reply, error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * stran_server_commit -
 *
 * return:
 *
 *   thrd(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
stran_server_commit (THREAD_ENTRY * thread_p, unsigned int rid,
		     UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  TRAN_STATE state;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  state = xtran_server_commit (thread_p);

  if (state != TRAN_UNACTIVE_COMMITTED)
    {
      /* Likely the commit failed.. somehow */
      return_error_to_client (thread_p, rid);
    }
  else
    {
      thread_p->conn_entry->is_server_in_tran = false;
    }

  (void) or_pack_int (reply, (int) state);

  check_reset_on_commit (thread_p);

  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * stran_server_abort -
 *
 * return:
 *
 *   thrd(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
stran_server_abort (THREAD_ENTRY * thread_p, unsigned int rid,
		    UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  TRAN_STATE state;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  UNUSED_VAR char *ptr;

  state = xtran_server_abort (thread_p);

  if (state != TRAN_UNACTIVE_ABORTED)
    {
      /* Likely the abort failed.. somehow */
      return_error_to_client (thread_p, rid);
    }
  else
    {
      thread_p->conn_entry->is_server_in_tran = false;
    }

  ptr = or_pack_int (reply, state);

  check_reset_on_commit (thread_p);

  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

static void
check_reset_on_commit (THREAD_ENTRY * thread_p)
{
  bool reset_on_commit = false;
  HA_STATE server_state;
  int client_type;
  UNUSED_VAR char *hostname;
  bool has_updated;

  has_updated = logtb_has_updated (thread_p);

  client_type = logtb_find_current_client_type (thread_p);
  hostname = logtb_find_current_client_hostname (thread_p);
  server_state = svr_shm_get_server_state ();
  if (has_updated
      && server_state == HA_STATE_TO_BE_SLAVE
      && BOOT_NORMAL_CLIENT_TYPE (client_type))
    {
      reset_on_commit = true;
      er_log_debug (ARG_FILE_LINE, "check_reset_on_commit(): "
		    "(has_updated && to-be-standby && normal client) "
		    "DB_CONNECTION_STATUS_RESET\n");
    }
  else if (server_state == HA_STATE_SLAVE)
    {
      /* be aware that the order of if conditions
       * is important
       */
      if (BOOT_RSQL_CLIENT_TYPE (client_type))
	{
	  /* rsql client does not reset connection
	   * reset_on_commit = false;
	   */
	}
      else if (client_type == BOOT_CLIENT_READ_WRITE_BROKER)
	{
	  reset_on_commit = true;
	  er_log_debug (ARG_FILE_LINE, "check_reset_on_commit(): "
			"(standby && read-write broker) "
			"DB_CONNECTION_STATUS_RESET\n");
	}
      else if (BOOT_NORMAL_CLIENT_TYPE (client_type)
	       && thread_p->conn_entry->conn_reset_on_commit == true)
	{
	  /* db modification error occurred */
	  reset_on_commit = true;

	  er_log_debug (ARG_FILE_LINE, "check_reset_on_commit(): "
			"(standby && conn->reset_on_commit && normal client) "
			"DB_CONNECTION_STATUS_RESET\n");

	}
      else if (BOOT_BROKER_AND_DEFAULT_CLIENT_TYPE (client_type)
	       && css_is_ha_repl_delayed () == true)
	{
	  if (thread_p->conn_entry->ignore_repl_delay == false)
	    {
	      reset_on_commit = true;
	      er_log_debug (ARG_FILE_LINE, "check_reset_on_commit(): "
			    "(standby && replication delay "
			    "&& default and broker client) "
			    "DB_CONNECTION_STATUS_RESET\n");
	    }
	}
    }
  else if (server_state == HA_STATE_MASTER
	   && client_type == BOOT_CLIENT_SLAVE_ONLY_BROKER)
    {
      reset_on_commit = true;
      er_log_debug (ARG_FILE_LINE, "check_reset_on_commit(): "
		    "(active && slave only broker) "
		    "DB_CONNECTION_STATUS_RESET\n");
    }

  thread_p->conn_entry->conn_reset_on_commit = reset_on_commit;
}

/*
 * stran_server_has_updated -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
stran_server_has_updated (THREAD_ENTRY * thread_p, unsigned int rid,
			  UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  int has_updated;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  has_updated = xtran_server_has_updated (thread_p);

  (void) or_pack_int (reply, has_updated);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * stran_server_start_topop -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
stran_server_start_topop (THREAD_ENTRY * thread_p, unsigned int rid,
			  UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  int success;
  LOG_LSA topop_lsa;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_LOG_LSA_ALIGNED_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;

  success = (xtran_server_start_topop (thread_p, &topop_lsa) == NO_ERROR)
    ? NO_ERROR : ER_FAILED;
  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  ptr = or_pack_int (reply, success);
  ptr = or_pack_log_lsa (ptr, &topop_lsa);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * stran_server_end_topop -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
stran_server_end_topop (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, UNUSED_ARG int reqlen)
{
  TRAN_STATE state;
  LOG_LSA topop_lsa;
  int xresult;
  LOG_RESULT_TOPOP result;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_LOG_LSA_ALIGNED_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;

  (void) or_unpack_int (request, &xresult);
  result = (LOG_RESULT_TOPOP) xresult;

  state = xtran_server_end_topop (thread_p, result, &topop_lsa);

  ptr = or_pack_int (reply, (int) state);
  ptr = or_pack_log_lsa (ptr, &topop_lsa);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}
#endif

/*
 * stran_server_savepoint -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
stran_server_savepoint (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, UNUSED_ARG int reqlen)
{
  int success;
  const char *savept_name;
  LOG_LSA topop_lsa;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_LOG_LSA_ALIGNED_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;

  ptr = or_unpack_string_nocopy (request, &savept_name);

  success = (xtran_server_savepoint (thread_p, savept_name,
				     &topop_lsa) == NO_ERROR)
    ? NO_ERROR : ER_FAILED;
  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  ptr = or_pack_int (reply, success);
  ptr = or_pack_log_lsa (ptr, &topop_lsa);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * stran_server_partial_abort -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
stran_server_partial_abort (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, UNUSED_ARG int reqlen)
{
  TRAN_STATE state;
  const char *savept_name;
  LOG_LSA savept_lsa;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_LOG_LSA_ALIGNED_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;

  ptr = or_unpack_string_nocopy (request, &savept_name);

  state = xtran_server_partial_abort (thread_p, savept_name, &savept_lsa);
  if (state != TRAN_UNACTIVE_ABORTED)
    {
      /* Likely the abort failed.. somehow */
      return_error_to_client (thread_p, rid);
    }

  ptr = or_pack_int (reply, (int) state);
  ptr = or_pack_log_lsa (ptr, &savept_lsa);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * stran_server_is_active_and_has_updated -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
stran_server_is_active_and_has_updated (THREAD_ENTRY * thread_p,
					unsigned int rid,
					UNUSED_ARG char *request,
					UNUSED_ARG int reqlen)
{
  int isactive_has_updated;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  isactive_has_updated = xtran_server_is_active_and_has_updated (thread_p);

  (void) or_pack_int (reply, isactive_has_updated);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * stran_wait_server_active_trans -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
stran_wait_server_active_trans (THREAD_ENTRY * thread_p, unsigned int rid,
				UNUSED_ARG char *request,
				UNUSED_ARG int reqlen)
{
  int status;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  status = xtran_wait_server_active_trans (thread_p);

  (void) or_pack_int (reply, status);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * stran_is_blocked -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
stran_is_blocked (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
		  int reqlen)
{
  int tran_index;
  bool blocked;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_unpack_int (request, &tran_index);

  blocked = xtran_is_blocked (thread_p, tran_index);

  (void) or_pack_int (reply, blocked ? 1 : 0);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}
#endif

/*
 * sboot_initialize_server -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sboot_initialize_server (THREAD_ENTRY * thread_p, unsigned int rid,
			 UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  css_send_abort_to_client (thread_p->conn_entry, rid);
}

/*
 * sboot_register_client -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sboot_register_client (THREAD_ENTRY * thread_p, unsigned int rid,
		       char *request, UNUSED_ARG int reqlen)
{
  int xint;
  BOOT_CLIENT_CREDENTIAL client_credential;
  BOOT_SERVER_CREDENTIAL server_credential;
  int tran_index, client_lock_wait;
  TRAN_STATE tran_state;
  int area_size, strlen1, strlen2, strlen3;
  char *reply = NULL, *area = NULL, *ptr = NULL;

  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  memset (&client_credential, 0, sizeof (client_credential));
  memset (&server_credential, 0, sizeof (server_credential));

  ptr = or_unpack_int (request, &xint);
  client_credential.client_type = (BOOT_CLIENT_TYPE) xint;
  ptr = or_unpack_string_nocopy (ptr, &client_credential.client_info);
  ptr = or_unpack_string_nocopy (ptr, &client_credential.db_name);
  ptr = or_unpack_string_nocopy (ptr, &client_credential.db_user);
  ptr = or_unpack_string_nocopy (ptr, &client_credential.db_password);
  ptr = or_unpack_string_nocopy (ptr, &client_credential.program_name);
  ptr = or_unpack_string_nocopy (ptr, &client_credential.login_name);
  ptr = or_unpack_string_nocopy (ptr, &client_credential.host_name);
  ptr = or_unpack_int (ptr, &client_credential.process_id);
  ptr = or_unpack_int (ptr, &client_lock_wait);

  tran_index = xboot_register_client (thread_p, &client_credential,
				      client_lock_wait,
				      &tran_state, &server_credential);
  if (tran_index == NULL_TRAN_INDEX)
    {
#if 1				/* TODO - trace rye_cas crash */
      assert (false);
#endif
      return_error_to_client (thread_p, rid);
      area_size = 0;
      goto end;
    }

  area_size = OR_INT_SIZE	/* tran_index */
    + OR_INT_SIZE		/* tran_state */
    + or_packed_string_length (server_credential.db_full_name, &strlen1)	/* db_full_name */
    + or_packed_string_length (server_credential.host_name, &strlen2)	/* host_name */
    + OR_INT_SIZE		/* process_id */
    + OR_OID_SIZE		/* root_class_oid */
    + OR_HFID_SIZE		/* root_class_hfid */
    + OR_INT_SIZE		/* page_size */
    + OR_INT_SIZE;		/* log_page_size */

  area_size += OR_INT_SIZE;	/* db_charset */
  area_size += OR_INT_SIZE;	/* server start time */
  area_size += or_packed_string_length (server_credential.db_lang, &strlen3);

  area = (char *) malloc (area_size);
  if (area == NULL)
    {
      return_error_to_client (thread_p, rid);
      area_size = 0;
      goto end;
    }

  ptr = or_pack_int (area, tran_index);
  ptr = or_pack_int (ptr, (int) tran_state);
  ptr = or_pack_string_with_length (ptr, server_credential.db_full_name,
				    strlen1);
  ptr = or_pack_string_with_length (ptr, server_credential.host_name,
				    strlen2);
  ptr = or_pack_int (ptr, server_credential.process_id);
  ptr = or_pack_oid (ptr, &server_credential.root_class_oid);
  ptr = or_pack_hfid (ptr, &server_credential.root_class_hfid);
  ptr = or_pack_int (ptr, (int) server_credential.page_size);
  ptr = or_pack_int (ptr, (int) server_credential.log_page_size);
  ptr = or_pack_int (ptr, server_credential.db_charset);
  ptr = or_pack_int (ptr, server_credential.server_start_time);
  ptr = or_pack_string_with_length (ptr, server_credential.db_lang, strlen3);

end:
  ptr = or_pack_int (reply, area_size);
  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    area, area_size);

  if (area != NULL)
    {
      free_and_init (area);
    }
}

/*
 * sboot_notify_unregister_client -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sboot_notify_unregister_client (THREAD_ENTRY * thread_p, unsigned int rid,
				char *request, UNUSED_ARG int reqlen)
{
  int tran_index;
  int success = NO_ERROR;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_unpack_int (request, &tran_index);

  xboot_notify_unregister_client (thread_p, tran_index);

  (void) or_pack_int (reply, success);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sboot_add_volume_extension -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sboot_add_volume_extension (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, UNUSED_ARG int reqlen)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  const char *tmp_str;
  char *ptr;
  DBDEF_VOL_EXT_INFO ext_info;
  int tmp;
  VOLID volid;

  ptr = or_unpack_string_nocopy (request, &tmp_str);
  ext_info.fullname = tmp_str;
  ptr = or_unpack_int (ptr, &ext_info.max_npages);
  ptr = or_unpack_int (ptr, &ext_info.max_writesize_in_sec);
  ptr = or_unpack_int (ptr, &tmp);
  ext_info.purpose = (DB_VOLPURPOSE) tmp;
  ptr = or_unpack_int (ptr, &tmp);
  ext_info.overwrite = (bool) tmp;

  volid = xboot_add_volume_extension (thread_p, &ext_info);

  if (volid == NULL_VOLID)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, (int) volid);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sboot_find_number_permanent_volumes -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sboot_find_number_permanent_volumes (THREAD_ENTRY * thread_p,
				     unsigned int rid,
				     UNUSED_ARG char *request,
				     UNUSED_ARG int reqlen)
{
  int nvols;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  nvols = xboot_find_number_permanent_volumes (thread_p);

  (void) or_pack_int (reply, nvols);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sboot_find_number_temp_volumes -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sboot_find_number_temp_volumes (THREAD_ENTRY * thread_p, unsigned int rid,
				UNUSED_ARG char *request,
				UNUSED_ARG int reqlen)
{
  int nvols;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  nvols = xboot_find_number_temp_volumes (thread_p);

  (void) or_pack_int (reply, nvols);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sboot_find_last_temp -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sboot_find_last_temp (THREAD_ENTRY * thread_p, unsigned int rid,
		      UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  int nvols;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  nvols = xboot_find_last_temp (thread_p);

  (void) or_pack_int (reply, nvols);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sboot_find_number_bestspace_entries -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sboot_find_number_bestspace_entries (THREAD_ENTRY * thread_p,
				     unsigned int rid,
				     UNUSED_ARG char *request,
				     UNUSED_ARG int reqlen)
{
  int nbest;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  nbest = xboot_find_number_bestspace_entries (thread_p);

  (void) or_pack_int (reply, nbest);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sboot_get_server_state -
 */
void
sboot_get_server_state (THREAD_ENTRY * thread_p, unsigned int rid,
			UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  HA_STATE server_state;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  server_state = svr_shm_get_server_state ();
  or_pack_int (reply, (int) server_state);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sboot_notify_ha_apply_state -
 */
void
sboot_notify_ha_apply_state (THREAD_ENTRY * thread_p, unsigned int rid,
			     char *request, UNUSED_ARG int reqlen)
{
  int i, status;
  HA_APPLY_STATE state;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  PRM_NODE_INFO node_info = { INADDR_NONE, 0 };

  ptr = or_unpack_int (request, (int *) &node_info.ip);
  ptr = or_unpack_int (ptr, &node_info.port);
  ptr = or_unpack_int (ptr, &i);
  state = (HA_APPLY_STATE) i;

  if (state >= HA_APPLY_STATE_UNREGISTERED && state <= HA_APPLY_STATE_ERROR)
    {

      status = css_notify_ha_apply_state (thread_p, &node_info, state);
      if (status != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ERR_CSS_ERROR_FROM_SERVER,
		  1, "Error in log applier state");
	  return_error_to_client (thread_p, rid);
	}
    }
  else
    {
      status = ER_FAILED;
    }
  or_pack_int (reply, status);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sqst_update_statistics -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sqst_update_statistics (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, UNUSED_ARG int reqlen)
{
  int error, update_stats, with_fullscan;
  OID classoid;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_oid (request, &classoid);
  ptr = or_unpack_int (ptr, &update_stats);
  ptr = or_unpack_int (ptr, &with_fullscan);

  error =
    xstats_update_statistics (thread_p, &classoid,
			      (update_stats ? true : false),
			      (with_fullscan ? STATS_WITH_FULLSCAN :
			       STATS_WITH_SAMPLING));
  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_errcode (reply, error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sqst_update_all_statistics -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sqst_update_all_statistics (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, UNUSED_ARG int reqlen)
{
  int error, update_stats, with_fullscan;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_int (request, &update_stats);
  ptr = or_unpack_int (ptr, &with_fullscan);

  error =
    xstats_update_all_statistics (thread_p,
				  (update_stats ? true : false),
				  (with_fullscan ? STATS_WITH_FULLSCAN :
				   STATS_WITH_SAMPLING));
  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_errcode (reply, error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sbtree_add_index -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sbtree_add_index (THREAD_ENTRY * thread_p, unsigned int rid,
		  char *request, UNUSED_ARG int reqlen)
{
  BTID btid;
  BTID *return_btid = NULL;
  DB_TYPE *att_type = NULL;
  OID class_oid;
  int num_atts, attr_id;
  int i;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_BTID_ALIGNED_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_btid (request, &btid);
  ptr = or_unpack_int (ptr, &num_atts);
  assert (num_atts > 0);

  att_type = (DB_TYPE *) malloc (num_atts * sizeof (DB_TYPE));
  assert (att_type != NULL);	/* TODO - null check */
  for (i = 0; i < num_atts; i++)
    {
      ptr = or_unpack_int (ptr, (int *) (&(att_type[i])));
    }

  ptr = or_unpack_oid (ptr, &class_oid);
  ptr = or_unpack_int (ptr, &attr_id);

  return_btid =
    xbtree_add_index (thread_p, &btid, num_atts, att_type, &class_oid,
		      attr_id);
  if (return_btid == NULL)
    {
      return_error_to_client (thread_p, rid);
      ptr = or_pack_int (reply, er_errid ());
    }
  else
    {
      ptr = or_pack_int (reply, NO_ERROR);
    }
  ptr = or_pack_btid (ptr, &btid);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));

  free_and_init (att_type);
}

/*
 * sbtree_load_data -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sbtree_load_data (THREAD_ENTRY * thread_p, unsigned int rid,
		  char *request, UNUSED_ARG int reqlen)
{
  BTID btid;
  OID class_oid;
  HFID hfid;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;
  int error = NO_ERROR;

  ptr = or_unpack_btid (request, &btid);
  ptr = or_unpack_oid (ptr, &class_oid);
  ptr = or_unpack_hfid (ptr, &hfid);

  error = xbtree_load_data (thread_p, &btid, &class_oid, &hfid);
  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sbtree_delete_index -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sbtree_delete_index (THREAD_ENTRY * thread_p, unsigned int rid,
		     char *request, UNUSED_ARG int reqlen)
{
  BTID btid;
  int success;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_unpack_btid (request, &btid);

  success = xbtree_delete_index (thread_p, &btid);
  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, (int) success);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}


/*
 * sbtree_find_unique -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sbtree_find_unique (THREAD_ENTRY * thread_p, unsigned int rid,
		    char *request, UNUSED_ARG int reqlen)
{
  OID class_oid;
  BTID btid;
  OID oid;
  DB_IDXKEY key;
  char *ptr;
  int success;
  OR_ALIGNED_BUF (OR_OID_SIZE + OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = request;

  ptr = or_unpack_db_idxkey (ptr, &key);
  assert (key.size == 1);
  assert (DB_VALUE_DOMAIN_TYPE (&(key.vals[0])) == DB_TYPE_VARCHAR);

  ptr = or_unpack_btid (ptr, &btid);

  ptr = or_unpack_oid (ptr, &class_oid);

  OID_SET_NULL (&oid);
  success = xbtree_find_unique (thread_p, &class_oid, &btid, &key, &oid);
  if (success == BTREE_ERROR_OCCURRED)
    {
      return_error_to_client (thread_p, rid);
    }

  /* free storage */
  db_idxkey_clear (&key);

  ptr = or_pack_int (reply, success);
  ptr = or_pack_oid (ptr, &oid);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sdisk_get_purpose_and_space_info -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sdisk_get_purpose_and_space_info (THREAD_ENTRY * thread_p,
				  unsigned int rid, char *request,
				  UNUSED_ARG int reqlen)
{
  int int_volid;
  VOLID volid;
  DISK_VOLPURPOSE vol_purpose;
  VOL_SPACE_INFO space_info;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2 + OR_VOL_SPACE_INFO_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_unpack_int (request, &int_volid);
  volid = (VOLID) int_volid;

  volid = xdisk_get_purpose_and_space_info (thread_p, volid,
					    &vol_purpose, &space_info);

  if (volid == NULL_VOLID)
    {
      return_error_to_client (thread_p, rid);
    }

  ptr = or_pack_int (reply, vol_purpose);
  OR_PACK_VOL_SPACE_INFO (ptr, (&space_info));
  ptr = or_pack_int (ptr, (int) volid);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sdk_vlabel -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sdk_vlabel (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
	    UNUSED_ARG int reqlen)
{
  OR_ALIGNED_BUF (PATH_MAX) a_vol_fullname;
  char *vol_fullname = OR_ALIGNED_BUF_START (a_vol_fullname);
  const char *database_name;
  int int_volid;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  int area_length, strlen;
  char *area;

  ptr = or_unpack_string_nocopy (request, &database_name);
  ptr = or_unpack_int (ptr, &int_volid);

  if (xdisk_get_fullname (thread_p, database_name,
			  (VOLID) int_volid, vol_fullname) == NULL)
    {
      return_error_to_client (thread_p, rid);
      area_length = 0;
      area = NULL;
    }
  else
    {
      area_length = or_packed_string_length (vol_fullname, &strlen);
      area = (char *) malloc (area_length);
      if (area == NULL)
	{
	  return_error_to_client (thread_p, rid);
	  area_length = 0;
	}
      else
	{
	  (void) or_pack_string_with_length (area, vol_fullname, strlen);
	}
    }

  (void) or_pack_int (reply, area_length);
  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    area, area_length);
  if (area)
    {
      free_and_init (area);
    }
}

/*
 * sqfile_get_list_file_page -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sqfile_get_list_file_page (THREAD_ENTRY * thread_p, unsigned int rid,
			   char *request, UNUSED_ARG int reqlen)
{
  QUERY_ID query_id;
  int volid, pageid;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char page_buf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_page_buf;
  int page_size;
  int error = NO_ERROR;

  aligned_page_buf = PTR_ALIGN (page_buf, MAX_ALIGNMENT);

  ptr = or_unpack_ptr (request, &query_id);
  ptr = or_unpack_int (ptr, &volid);
  ptr = or_unpack_int (ptr, &pageid);
  assert (pageid != NULL_PAGEID);

  if (volid == NULL_VOLID && pageid == NULL_PAGEID)
    {
      assert (false);		/* something wrong */
      goto empty_page;
    }

  error = xqfile_get_list_file_page (thread_p, query_id, volid, pageid,
				     aligned_page_buf, &page_size);
  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
      goto empty_page;
    }

  if (page_size == 0)
    {
      goto empty_page;
    }

  ptr = or_pack_int (reply, page_size);
  ptr = or_pack_int (ptr, error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    aligned_page_buf, page_size);
  return;

empty_page:
  /* setup empty list file page and return it */
  qmgr_setup_empty_list_file (aligned_page_buf);
  page_size = QFILE_PAGE_HEADER_SIZE;
  ptr = or_pack_int (reply, page_size);
  ptr = or_pack_int (ptr, error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    aligned_page_buf, page_size);
}

/*
 * sqmgr_prepare_query - Process a SERVER_QM_PREPARE request
 *
 * return:
 *
 *   thrd(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 * Receive XASL stream and return XASL file id (QFILE_LIST_ID) as a result.
 * If xasl_buffer == NULL, the server will look up the XASL cache and then
 * return the cached XASL file id if found, otherwise return NULL QFILE_LIST_ID.
 * This function is a counter part to qmgr_prepare_query().
 */
void
sqmgr_prepare_query (THREAD_ENTRY * thread_p, unsigned int rid,
		     char *request, UNUSED_ARG int reqlen)
{
  XASL_ID xasl_id, *p;
  char *ptr;
  char *reply = NULL, *reply_buffer = NULL;
  int reply_buffer_size = 0, get_xasl_header = 0;
  OID user_oid;
  XASL_NODE_HEADER xasl_header;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE + OR_XASL_ID_SIZE) a_reply;
  int error = NO_ERROR;
  COMPILE_CONTEXT context = { NULL, NULL, 0, NULL, NULL, 0 };
  XASL_STREAM stream = { NULL, NULL, NULL, 0 };
  CSS_NET_PACKET *recv_packet = thread_p->recv_packet;

  reply = OR_ALIGNED_BUF_START (a_reply);

  /* unpack query alias string from the request data */
  ptr = or_unpack_string_nocopy (request, &context.sql_hash_text);

  /* unpack query plan from the request data */
  ptr = or_unpack_string_nocopy (ptr, &context.sql_plan_text);

  /* unpack query string from the request data */
  ptr = or_unpack_string_nocopy (ptr, &context.sql_user_text);

  /* unpack OID of the current user */
  ptr = or_unpack_oid (ptr, &user_oid);
  /* unpack size of XASL stream */
  ptr = or_unpack_int (ptr, &stream.xasl_stream_size);
  /* unpack get XASL node header boolean */
  ptr = or_unpack_int (ptr, &get_xasl_header);

  if (get_xasl_header)
    {
      /* need to get XASL node header */
      stream.xasl_header = &xasl_header;
      INIT_XASL_NODE_HEADER (stream.xasl_header);
    }

  if (stream.xasl_stream_size > 0)
    {
      ptr = css_net_packet_get_buffer (recv_packet, 1,
				       stream.xasl_stream_size, false);
      if (ptr == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_NET_SERVER_DATA_RECEIVE, 0);
	  css_send_abort_to_client (thread_p->conn_entry, rid);
	  return;
	}

      stream.xasl_stream = ptr;
    }

  /* call the server routine of query prepare */
  stream.xasl_id = &xasl_id;
  XASL_ID_SET_NULL (stream.xasl_id);

  p = xqmgr_prepare_query (thread_p, &context, &stream, &user_oid);

  if (stream.xasl_stream && !p)
    {
      return_error_to_client (thread_p, rid);
    }

  if (p)
    {
      if (get_xasl_header && !XASL_ID_IS_NULL (p))
	{
	  /* pack XASL node header */
	  reply_buffer_size = XASL_NODE_HEADER_SIZE;
	  reply_buffer = (char *) malloc (reply_buffer_size);
	  if (reply_buffer == NULL)
	    {
	      reply_buffer_size = 0;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, reply_buffer_size);
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	  else
	    {
	      ptr = reply_buffer;
	      OR_PACK_XASL_NODE_HEADER (ptr, stream.xasl_header);
	    }
	}
    }
  else
    {
      error = ER_FAILED;
    }

  if (error == NO_ERROR)
    {
      ptr = or_pack_int (reply, reply_buffer_size);
      ptr = or_pack_int (ptr, NO_ERROR);
      /* pack XASL file id as a reply */
      OR_PACK_XASL_ID (ptr, p);
    }
  else
    {
      ptr = or_pack_int (reply, 0);
      ptr = or_pack_int (ptr, error);
    }

  /* send reply and data to the client */
  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    reply_buffer, reply_buffer_size);

  if (reply_buffer != NULL)
    {
      free_and_init (reply_buffer);
    }
}

/*
 * sqmgr_execute_query - Process a SERVER_QM_EXECUTE request
 *
 * return:
 *
 *   thrd(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 * Receive XASL file id and parameter values if exist and return list file id
 * that contains query result. If an error occurs, return NULL QFILE_LIST_ID.
 * This function is a counter part to qmgr_execute_query().
 */
void
sqmgr_execute_query (THREAD_ENTRY * thread_p, unsigned int rid,
		     char *request, UNUSED_ARG int reqlen)
{
  XASL_ID xasl_id;
  QFILE_LIST_ID *list_id;
  int dbval_cnt, data_size, replydata_size, page_size;
  QUERY_ID query_id = NULL_QUERY_ID;
  char *ptr, *data = NULL, *reply, *replydata = NULL;
  PAGE_PTR page_ptr;
  char *page_ptr_copied = NULL;
  char page_buf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_page_buf;
  QUERY_FLAG query_flag;
  QUERY_EXECUTE_STATUS_FLAG query_execute_status_flag = 0;
  OR_ALIGNED_BUF (OR_INT_SIZE * 4 + OR_PTR_ALIGNED_SIZE
		  + OR_INT_SIZE) a_reply;
  int query_timeout;
  XASL_CACHE_ENTRY *xasl_cache_entry_p = NULL;
  char *shard_key_data = NULL;
  int shard_key_size = 0;
  int shard_groupid;
  DB_VALUE shard_key_val, *shard_key;

  int response_time = 0;
  struct timeval start;
  struct timeval end;
  int queryinfo_string_length = 0;
  char queryinfo_string[QUERY_INFO_BUF_SIZE];
  int trace_slow_msec, trace_ioread;

  MNT_SERVER_EXEC_STATS base_stats, current_stats, diff_stats;
  char *sql_id = NULL;
  int error_code = NO_ERROR;
  bool tran_abort = false;
  CSS_NET_PACKET *recv_packet = thread_p->recv_packet;

  EXECUTION_INFO info = { NULL, NULL, NULL };
  trace_slow_msec = prm_get_bigint_value (PRM_ID_SQL_TRACE_SLOW);
  trace_ioread = prm_get_integer_value (PRM_ID_SQL_TRACE_IOREADS);

  shard_key = &shard_key_val;
  DB_MAKE_NULL (shard_key);

  if (trace_slow_msec >= 0 || trace_ioread > 0)
    {
#if defined (ENABLE_UNUSED_FUNCTION)
      xmnt_server_start_stats (thread_p, false);
#endif
      xmnt_server_copy_stats (thread_p, &base_stats);
      gettimeofday (&start, NULL);

      if (trace_slow_msec >= 0)
	{
	  thread_p->event_stats.trace_slow_query = true;
	}
    }

  aligned_page_buf = PTR_ALIGN (page_buf, MAX_ALIGNMENT);

  reply = OR_ALIGNED_BUF_START (a_reply);

  /* unpack XASL file id (XASL_ID), number of parameter values,
     size of the reecieved data, and query execution mode flag
     from the request data */
  ptr = request;
  OR_UNPACK_XASL_ID (ptr, &xasl_id);
  ptr = or_unpack_int (ptr, &dbval_cnt);
  ptr = or_unpack_int (ptr, &data_size);
  ptr = or_unpack_int (ptr, &query_flag);
  ptr = or_unpack_int (ptr, &query_timeout);
  ptr = or_unpack_int (ptr, &shard_groupid);
  ptr = or_unpack_int (ptr, &shard_key_size);

  /* if the request contains parameter values for the query,
     allocate space for them */
  if (dbval_cnt)
    {
      /* receive parameter values (DB_VALUE) from the client */
      data = css_net_packet_get_buffer (recv_packet, 1, data_size, false);
      if (data == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_NET_SERVER_DATA_RECEIVE, 0);
	  css_send_abort_to_client (thread_p->conn_entry, rid);
	  return;		/* error */
	}
    }

  if (shard_key_size > 0)
    {
      shard_key_data = css_net_packet_get_buffer (recv_packet, 2,
						  shard_key_size, false);
      if (shard_key_data == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_NET_SERVER_DATA_RECEIVE, 0);
	  css_send_abort_to_client (thread_p->conn_entry, rid);
	  return;
	}

      or_unpack_db_value (shard_key_data, shard_key);
    }
  else
    {
      assert (DB_IS_NULL (shard_key));
      shard_key = NULL;
    }

  /* call the server routine of query execute */
  list_id = xqmgr_execute_query (thread_p, &xasl_id, &query_id,
				 dbval_cnt, data, &query_flag,
				 query_timeout, shard_groupid, shard_key,
				 &xasl_cache_entry_p);

  db_value_clear (shard_key);

  if (xasl_cache_entry_p)
    {
      info = xasl_cache_entry_p->sql_info;
    }

  if (list_id == NULL)
    {
      error_code = er_errid ();

      if (error_code != NO_ERROR)
	{
	  if (info.sql_hash_text != NULL)
	    {
	      if (qmgr_get_sql_id (thread_p, &sql_id,
				   info.sql_hash_text,
				   strlen (info.sql_hash_text)) != NO_ERROR)
		{
		  sql_id = NULL;
		}
	    }

	  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
		  ER_QUERY_EXECUTION_ERROR, 3, error_code,
		  sql_id ? sql_id : "(UNKNOWN SQL_ID)",
		  info.sql_user_text ? info.
		  sql_user_text : "(UNKNOWN USER_TEXT)");

	  if (sql_id != NULL)
	    {
	      free_and_init (sql_id);
	    }
	}

      if (xasl_cache_entry_p)
	{
	  tran_abort = need_to_abort_tran (thread_p, &error_code);
	  if (tran_abort == true)
	    {
	      /* Remove transaction id from xasl cache entry before
	       * return_error_to_client, where current transaction may be aborted.
	       * Otherwise, another transaction may be resumed and
	       * xasl_cache_entry_p may be removed by that transaction, during class
	       * deletion.
	       */
	      (void) qexec_remove_my_tran_id_in_xasl_entry (thread_p,
							    xasl_cache_entry_p,
							    true);
	      xasl_cache_entry_p = NULL;
	    }
	}

      return_error_to_client (thread_p, rid);
    }

  page_size = 0;
  page_ptr = NULL;
  if (list_id)
    {
      /* get the first page of the list file */
      if (VPID_ISNULL (&(list_id->first_vpid)))
	{
	  page_ptr = NULL;
	  query_execute_status_flag |= QUERY_EXECUTE_STATUS_QUERY_CLOSED;
	}
      else
	{
	  page_ptr = qmgr_get_old_page (thread_p, &(list_id->first_vpid),
					list_id->tfile_vfid);
	}

      if (page_ptr)
	{
	  /* calculate page size */
	  if (QFILE_GET_TUPLE_COUNT (page_ptr) == -2
	      || QFILE_GET_OVERFLOW_PAGE_ID (page_ptr) != NULL_PAGEID)
	    {
	      page_size = DB_PAGESIZE;
	    }
	  else
	    {
	      int offset = QFILE_GET_LAST_TUPLE_OFFSET (page_ptr);

	      page_size = (offset
			   + QFILE_GET_TUPLE_LENGTH (page_ptr + offset));

	      if (list_id->page_cnt == 1)
		{
		  query_execute_status_flag |=
		    QUERY_EXECUTE_STATUS_QUERY_CLOSED;
		}
	    }

	  memcpy (aligned_page_buf, page_ptr, page_size);
	  qmgr_free_old_page_and_init (thread_p, page_ptr,
				       list_id->tfile_vfid);
	  page_ptr = aligned_page_buf;
	}
      else
	{
	  return_error_to_client (thread_p, rid);
	}
    }

  if (page_ptr)
    {
      page_ptr_copied = (char *) malloc (page_size);
      if (page_ptr_copied)
	{
	  memcpy (page_ptr_copied, page_ptr, page_size);
	}
      else
	{
	  page_size = 0;
	  return_error_to_client (thread_p, rid);
	}
    }

  replydata_size = list_id ? or_listid_length (list_id) : 0;
  if (replydata_size)
    {
      /* pack list file id as a reply data */
      replydata = (char *) malloc (replydata_size);
      if (replydata)
	{
	  (void) or_pack_listid (replydata, list_id);
	}
      else
	{
	  replydata_size = 0;
	  return_error_to_client (thread_p, rid);
	}
    }

  /* pack 'QUERY_END' as a first argument of the reply */
  ptr = or_pack_int (reply, QUERY_END);
  /* pack size of list file id to return as a second argument of the reply */
  ptr = or_pack_int (ptr, replydata_size);
  /* pack size of a page to return as a third argumnet of the reply */
  ptr = or_pack_int (ptr, page_size);

  /* We may release the xasl cache entry when the transaction aborted.
   * To refer the contents of the freed entry for the case will cause defects.
   */
  if (tran_abort == false)
    {
      if (trace_slow_msec >= 0 || trace_ioread > 0)
	{
	  gettimeofday (&end, NULL);
	  response_time =
	    (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec -
						  start.tv_usec) / 1000;

	  xmnt_server_copy_stats (thread_p, &current_stats);
	  mnt_diff_stats (&diff_stats, &current_stats, &base_stats);

	  if (response_time >= trace_slow_msec)
	    {
	      queryinfo_string_length = er_log_slow_query (thread_p, &info,
							   response_time,
							   &diff_stats,
							   queryinfo_string);
	      event_log_slow_query (thread_p, &info, response_time,
				    &diff_stats);
	    }

	  if (trace_ioread > 0 &&
	      (diff_stats.values[MNT_STATS_DATA_PAGE_IOREADS] >=
	       trace_ioread))
	    {
	      event_log_many_ioreads (thread_p, &info, response_time,
				      &diff_stats);
	    }

#if defined (ENABLE_UNUSED_FUNCTION)
	  xmnt_server_stop_stats (thread_p);
#endif
	}

      if (thread_p->event_stats.temp_expand_pages > 0)
	{
	  event_log_temp_expand_pages (thread_p, &info);
	}
    }

  if (xasl_cache_entry_p)
    {
      (void) qexec_remove_my_tran_id_in_xasl_entry (thread_p,
						    xasl_cache_entry_p, true);
    }

  ptr = or_pack_int (ptr, queryinfo_string_length);

  /* query id to return as a fourth argument of the reply */
  ptr = or_pack_ptr (ptr, query_id);
  ptr = or_pack_int (ptr, query_execute_status_flag);

#if !defined(NDEBUG)
  /* suppress valgrind UMW error */
  memset (ptr, 0, OR_ALIGNED_BUF_SIZE (a_reply) - (ptr - reply));
#endif

  if (query_execute_status_flag & QUERY_EXECUTE_STATUS_QUERY_CLOSED)
    {
      if (query_id > 0 && xqmgr_end_query (thread_p, query_id) != NO_ERROR)
	{
	}
    }

  if (query_flag & AUTO_COMMIT_MODE)
    {
      if (logtb_has_updated (thread_p))
	{
	  CHECK_MODIFICATION_NO_RETURN (thread_p, error_code);
	  if (error_code != NO_ERROR)
	    {
	      return_error_to_client (thread_p, rid);
	      css_send_abort_to_client (thread_p->conn_entry, rid);

	      /* this transaction aborted in css_internal_request_handler () */

	      goto execute_end;
	    }
	}

      if (list_id == NULL)
	{
	  xtran_server_abort (thread_p);
	}
      else
	{
	  if (xtran_server_commit (thread_p) != TRAN_UNACTIVE_COMMITTED)
	    {
	      return_error_to_client (thread_p, rid);
	      css_send_abort_to_client (thread_p->conn_entry, rid);
	      goto execute_end;
	    }
	}

      thread_p->conn_entry->is_server_in_tran = false;
      check_reset_on_commit (thread_p);
    }

  css_send_reply_to_client (thread_p->conn_entry, rid, 4,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    replydata, replydata_size,
			    page_ptr_copied, page_size,
			    queryinfo_string, queryinfo_string_length);

execute_end:

  /* free QFILE_LIST_ID duplicated by xqmgr_execute_query() */
  if (page_ptr_copied)
    {
      free_and_init (page_ptr_copied);
    }
  if (replydata)
    {
      free_and_init (replydata);
    }
  if (list_id)
    {
      QFILE_FREE_AND_INIT_LIST_ID (list_id);
    }
}

/*
 * er_log_slow_query - log slow query to error log file
 * return:
 *   thread_p(in):
 *   info(in):
 *   time(in):
 *   diff_stats(in):
 *   queryinfo_string(out):
 */
static int
er_log_slow_query (THREAD_ENTRY * thread_p, EXECUTION_INFO * info,
		   int time, MNT_SERVER_EXEC_STATS * diff_stats,
		   char *queryinfo_string)
{
  char stat_buf[STATDUMP_BUF_SIZE];
  char *sql_id;
  int queryinfo_string_length;
  const char *line =
    "--------------------------------------------------------------------------------";
  const char *title = "Operation";

  if (prm_get_bool_value (PRM_ID_SQL_TRACE_EXECUTION_PLAN) == true)
    {
      mnt_server_dump_stats_to_buffer (diff_stats, stat_buf,
				       STATDUMP_BUF_SIZE, NULL);
    }
  else
    {
      info->sql_plan_text = NULL;
      stat_buf[0] = '\0';
    }


  if (info->sql_hash_text == NULL
      || qmgr_get_sql_id (thread_p, &sql_id, info->sql_hash_text,
			  strlen (info->sql_hash_text)) != NO_ERROR)
    {
      sql_id = NULL;
    }

  queryinfo_string_length =
    snprintf (queryinfo_string, QUERY_INFO_BUF_SIZE,
	      "%s\n%s\n%s\n %s\n\n /* SQL_ID: %s */ %s%s \n\n%s\n%s\n",
	      line, title, line,
	      info->sql_user_text ? info->
	      sql_user_text : "(UNKNOWN USER_TEXT)",
	      sql_id ? sql_id : "(UNKNOWN SQL_ID)",
	      info->sql_hash_text ? info->
	      sql_hash_text : "(UNKNOWN HASH_TEXT)",
	      info->sql_plan_text ? info->sql_plan_text : "", stat_buf, line);

  if (sql_id != NULL)
    {
      free (sql_id);
    }

  if (queryinfo_string_length >= QUERY_INFO_BUF_SIZE)
    {
      /* string is truncated */
      queryinfo_string_length = QUERY_INFO_BUF_SIZE - 1;
      queryinfo_string[queryinfo_string_length] = '\0';
    }

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	  ER_SLOW_QUERY, 2, time, queryinfo_string);

  return queryinfo_string_length;
}

/*
 * event_log_slow_query - log slow query to event log file
 * return:
 *   thread_p(in):
 *   info(in):
 *   time(in):
 *   diff_stats(in):
 *   num_bind_vals(in):
 *   bind_vals(in):
 */
static void
event_log_slow_query (THREAD_ENTRY * thread_p, EXECUTION_INFO * info,
		      int time, MNT_SERVER_EXEC_STATS * diff_stats)
{
  FILE *log_fp;
  int indent = 2;
  LOG_TDES *tdes;
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = logtb_get_current_tdes (thread_p);
  log_fp = event_log_start (thread_p, "SLOW_QUERY");

  if (tdes == NULL || log_fp == NULL)
    {
      return;
    }

  event_log_print_client_info (tran_index, indent);
  fprintf (log_fp, "%*csql: %s\n", indent, ' ',
	   info->sql_hash_text ? info->sql_hash_text : "(UNKNOWN HASH_TEXT)");

  if (tdes->num_exec_queries <= MAX_NUM_EXEC_QUERY_HISTORY)
    {
      event_log_bind_values (log_fp, tran_index, tdes->num_exec_queries - 1);
    }

  fprintf (log_fp, "%*ctime: %d\n", indent, ' ', time);
  fprintf (log_fp, "%*cbuffer: fetch=%lld, ioread=%lld, iowrite=%lld\n",
	   indent, ' ',
	   (long long int) diff_stats->values[MNT_STATS_DATA_PAGE_FETCHES],
	   (long long int) diff_stats->values[MNT_STATS_DATA_PAGE_IOREADS],
	   (long long int) diff_stats->values[MNT_STATS_DATA_PAGE_IOWRITES]);
  fprintf (log_fp, "%*cwait: cs=%d, lock=%d, latch=%d\n\n", indent, ' ',
	   TO_MSEC (thread_p->event_stats.cs_waits),
	   TO_MSEC (thread_p->event_stats.lock_waits),
	   TO_MSEC (thread_p->event_stats.latch_waits));

  event_log_end (thread_p);
}

/*
 * event_log_many_ioreads - log many ioreads to event log file
 * return:
 *   thread_p(in):
 *   info(in):
 *   time(in):
 *   diff_stats(in):
 *   num_bind_vals(in):
 *   bind_vals(in):
 */
static void
event_log_many_ioreads (THREAD_ENTRY * thread_p, EXECUTION_INFO * info,
			int time, MNT_SERVER_EXEC_STATS * diff_stats)
{
  FILE *log_fp;
  int indent = 2;
  LOG_TDES *tdes;
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  assert (tdes != NULL);
  log_fp = event_log_start (thread_p, "MANY_IOREADS");

  if (tdes == NULL || log_fp == NULL)
    {
      return;
    }

  event_log_print_client_info (tran_index, indent);
  fprintf (log_fp, "%*csql: %s\n", indent, ' ',
	   info->sql_hash_text ? info->sql_hash_text : "(UNKNOWN HASH_TEXT)");

  if (tdes->num_exec_queries <= MAX_NUM_EXEC_QUERY_HISTORY)
    {
      event_log_bind_values (log_fp, tran_index, tdes->num_exec_queries - 1);
    }

  fprintf (log_fp, "%*ctime: %d\n", indent, ' ', time);
  fprintf (log_fp, "%*cioreads: %lld\n\n", indent, ' ',
	   (long long int) diff_stats->values[MNT_STATS_DATA_PAGE_IOREADS]);

  event_log_end (thread_p);
}

/*
 * event_log_temp_expand_pages - log temp volume expand pages to event log file
 * return:
 *   thread_p(in):
 *   info(in):
 *   num_bind_vals(in):
 *   bind_vals(in):
 */
static void
event_log_temp_expand_pages (THREAD_ENTRY * thread_p, EXECUTION_INFO * info)
{
  FILE *log_fp;
  int indent = 2;
  LOG_TDES *tdes;
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  assert (tdes != NULL);
  log_fp = event_log_start (thread_p, "TEMP_VOLUME_EXPAND");

  if (tdes == NULL || log_fp == NULL)
    {
      return;
    }

  event_log_print_client_info (tran_index, indent);
  fprintf (log_fp, "%*csql: %s\n", indent, ' ',
	   info->sql_hash_text ? info->sql_hash_text : "(UNKNOWN HASH_TEXT)");

  if (tdes->num_exec_queries <= MAX_NUM_EXEC_QUERY_HISTORY)
    {
      event_log_bind_values (log_fp, tran_index, tdes->num_exec_queries - 1);
    }

  fprintf (log_fp, "%*ctime: %d\n", indent, ' ',
	   TO_MSEC (thread_p->event_stats.temp_expand_time));
  fprintf (log_fp, "%*cpages: %d\n\n", indent, ' ',
	   thread_p->event_stats.temp_expand_pages);

  event_log_end (thread_p);
}

/*
 * sqmgr_end_query -
 *
 * return:
 *
 *   thrd(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sqmgr_end_query (THREAD_ENTRY * thread_p, unsigned int rid,
		 char *request, UNUSED_ARG int reqlen)
{
  QUERY_ID query_id;
  int success = NO_ERROR;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_unpack_ptr (request, &query_id);
  if (query_id > 0)
    {
      success = xqmgr_end_query (thread_p, query_id);
      if (success != NO_ERROR)
	{
	  return_error_to_client (thread_p, rid);
	}
    }

  (void) or_pack_int (reply, (int) success);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sqmgr_drop_query_plan - Process a SERVER_QM_DROP_PLAN request
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 * Delete the XASL cache specified by either the query string or the XASL file
 * id upon request of the client.
 * This function is a counter part to qmgr_drop_query_plan().
 */
void
sqmgr_drop_query_plan (THREAD_ENTRY * thread_p, unsigned int rid,
		       char *request, UNUSED_ARG int reqlen)
{
  XASL_ID xasl_id;
  int status;
  char *ptr, *reply;
  const char *qstmt;
  OID user_oid;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  /* unpack query string from the request data */
  ptr = or_unpack_string_nocopy (request, &qstmt);
  /* unpack OID of the current user */
  ptr = or_unpack_oid (ptr, &user_oid);
  /* unpack XASL_ID */
  OR_UNPACK_XASL_ID (ptr, &xasl_id);

  /* call the server routine of query drop plan */
  status = xqmgr_drop_query_plan (thread_p, qstmt, &user_oid, &xasl_id);
  if (status != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  /* pack status (DB_IN32) as a reply */
  (void) or_pack_int (reply, status);

  /* send reply and data to the client */
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));

}

/*
 * sqmgr_drop_all_query_plans - Process a SERVER_QM_DROP_ALL_PLANS request
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 * Clear all XASL cache entries out upon request of the client.
 * This function is a counter part to qmgr_drop_all_query_plans().
 */
void
sqmgr_drop_all_query_plans (THREAD_ENTRY * thread_p, unsigned int rid,
			    UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  int status;
  char *reply;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  /* call the server routine of query drop plan */
  status = xqmgr_drop_all_query_plans (thread_p);
  if (status != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  /* pack status (DB_IN32) as a reply */
  (void) or_pack_int (reply, status);

  /* send reply and data to the client */
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sqmgr_dump_query_plans -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sqmgr_dump_query_plans (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, UNUSED_ARG int reqlen)
{
  FILE *outfp;
  int buffer_size;

  (void) or_unpack_int (request, &buffer_size);

  outfp = dump_func_open_tmpfile (thread_p, rid);
  if (outfp == NULL)
    {
      return;
    }

  xqmgr_dump_query_plans (thread_p, outfp);

  dump_func_send_result (thread_p, rid, buffer_size, outfp);
}

/*
 * sqmgr_get_query_info -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sqmgr_get_query_info (THREAD_ENTRY * thread_p, unsigned int rid,
		      char *request, UNUSED_ARG int reqlen)
{
  QUERY_ID query_id;
  int count;
  int length;
  char *buf;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_unpack_ptr (request, &query_id);

  count = xqmgr_get_query_info (thread_p, query_id);

  buf = (char *) qmgr_get_area_error_async (thread_p, &length, count,
					    query_id);

  (void) or_pack_int (reply, length);

  /* xqmgr_get_query_info() check query error,
   * if error was ER_LK_UNILATERALLY_ABORTED,
   * then rollback transaction
   */
  if (count < 0 && er_errid () == ER_LK_UNILATERALLY_ABORTED)
    {
      tran_server_unilaterally_abort_tran (thread_p);
    }
  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply), buf,
			    length);
  free_and_init (buf);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * smnt_server_start_stats -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
smnt_server_start_stats (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, int reqlen)
{
  int success, for_all_trans;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  or_unpack_int (request, &for_all_trans);

  success = xmnt_server_start_stats (thread_p, (bool) for_all_trans);
  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, (int) success);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * smnt_server_stop_stats -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
smnt_server_stop_stats (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, int reqlen)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  xmnt_server_stop_stats (thread_p);
  /* dummy reply message */
  (void) or_pack_int (reply, 1);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}
#endif

/*
 * smnt_server_copy_stats -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
smnt_server_copy_stats (THREAD_ENTRY * thread_p, unsigned int rid,
			UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  OR_ALIGNED_BUF (STAT_SIZE_PACKED) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  MNT_SERVER_EXEC_STATS stats;

  xmnt_server_copy_stats (thread_p, &stats);
  net_pack_stats (reply, &stats);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * smnt_server_copy_global_stats -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
smnt_server_copy_global_stats (THREAD_ENTRY * thread_p, unsigned int rid,
			       UNUSED_ARG char *request,
			       UNUSED_ARG int reqlen)
{
  OR_ALIGNED_BUF (STAT_SIZE_PACKED) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  MNT_SERVER_EXEC_STATS stats;

  xmnt_server_copy_global_stats (thread_p, &stats);
  net_pack_stats (reply, &stats);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sct_can_accept_new_repr -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sct_can_accept_new_repr (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, UNUSED_ARG int reqlen)
{
  OID classoid;
  HFID hfid;
  int success, can_accept;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;

  ptr = or_unpack_oid (request, &classoid);
  ptr = or_unpack_hfid (ptr, &hfid);

  success = xcatalog_is_acceptable_new_representation (thread_p, &classoid,
						       &hfid, &can_accept);
  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  ptr = or_pack_int (reply, (int) success);
  ptr = or_pack_int (ptr, (int) can_accept);

  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * xs_receive_data_from_client -
 *
 * return:
 *
 *   area(in):
 *   datasize(in):
 *   timeout (in):
 *
 * NOTE:
 */
int
xs_receive_data_from_client (THREAD_ENTRY * thread_p,
			     char **area, int *datasize, int timeout)
{
  unsigned int eid;
  bool continue_checking = true;
  int css_error;
  CSS_NET_PACKET *recv_packet;
  char *buffer;
  int buffer_size;
  int error;

  if (*area)
    {
      free_and_init (*area);
    }
  eid = thread_get_comm_request_id (thread_p);

  css_error = css_recv_data_packet_from_client (&recv_packet,
						thread_p->conn_entry,
						CSS_RID_FROM_EID (eid),
						timeout, 0);
  if (css_error != NO_ERRORS)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_SERVER_DATA_RECEIVE,
	      0);
      return ER_FAILED;
    }


  if (logtb_is_interrupted (thread_p, false, &continue_checking))
    {
      css_net_packet_free (recv_packet);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INTERRUPTED, 0);
      return ER_FAILED;
    }

  buffer_size = css_net_packet_get_recv_size (recv_packet, 0);
  buffer = css_net_packet_get_buffer (recv_packet, 0, -1, true);

  if (buffer == NULL)
    {
      css_net_packet_free (recv_packet);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_SERVER_DATA_RECEIVE,
	      0);
      return ER_FAILED;
    }

  error = NO_ERROR;

  if (recv_packet->header.packet_type == ERROR_TYPE)
    {
      error = er_set_area_error (buffer);
      free_and_init (buffer);
    }
  else if (recv_packet->header.packet_type == DATA_TYPE)
    {
      *area = buffer;
      *datasize = buffer_size;
    }
  else
    {
      assert (0);
    }

  if (error == NO_ERROR)
    {
      css_epoll_set_check (thread_p->conn_entry, true);
    }

  css_net_packet_free (recv_packet);

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * stest_performance -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
stest_performance (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
		   int reqlen)
{
  int return_size;
  OR_ALIGNED_BUF (10000) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  if (reqlen >= OR_INT_SIZE)
    {
      or_unpack_int (request, &return_size);
      if (return_size > 0)
	{
	  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
				    return_size);
	}
    }
}

/*
 * slocator_assign_oid_batch -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_assign_oid_batch (THREAD_ENTRY * thread_p, unsigned int rid,
			   char *request, int reqlen)
{
  int success;
  LC_OIDSET *oidset = NULL;

  /* skip over the word at the front reserved for the return code */
  oidset = locator_unpack_oid_set_to_new (thread_p, request + OR_INT_SIZE);
  if (oidset == NULL)
    {
      return_error_to_client (thread_p, rid);
      return;
    }

  success = xlocator_assign_oid_batch (thread_p, oidset);

  /* the buffer we send back is identical in size to the buffer that was
   * received so we can reuse it.
   */

  /* first word is reserved for return code */
  or_pack_int (request, success);
  if (success == NO_ERROR)
    {
      if (locator_pack_oid_set (request + OR_INT_SIZE, oidset) == NULL)
	{
	  /* trouble packing oidset for the return trip, severe error */
	  success = ER_FAILED;
	  or_pack_int (request, success);
	}
    }

  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  css_send_reply_to_client (thread_p->conn_entry, rid, 1, request, reqlen);

  locator_free_oid_set (thread_p, oidset);
}
#endif

/*
 * slocator_find_lockhint_class_oids -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_find_lockhint_class_oids (THREAD_ENTRY * thread_p,
				   unsigned int rid, char *request,
				   UNUSED_ARG int reqlen)
{
  int num_classes;
  const char **many_classnames;
  LOCK *many_locks = NULL;
  OID *guessed_class_oids = NULL;
  int quit_on_errors;
  LC_FIND_CLASSNAME allfind = LC_CLASSNAME_ERROR;
  LC_LOCKHINT *found_lockhint;
  LC_COPYAREA *copy_area;
  char *desc_ptr = NULL;
  int desc_size;
  char *content_ptr;
  int content_size;
  char *ptr;
  int num_objs = 0;
  char *packed = NULL;
  UNUSED_VAR int packed_size;
  int send_size = 0;
  int i;
  int malloc_size;
  char *malloc_area;
  OR_ALIGNED_BUF (NET_SENDRECV_BUFFSIZE + NET_COPY_AREA_SENDRECV_SIZE +
		  OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  found_lockhint = NULL;
  copy_area = NULL;

  ptr = or_unpack_int (request, &num_classes);
  ptr = or_unpack_int (ptr, &quit_on_errors);

  malloc_size = ((sizeof (char *) + sizeof (LOCK) + sizeof (int) +
		  sizeof (OID)) * num_classes);

  malloc_area = (char *) malloc (malloc_size);
  if (malloc_area != NULL)
    {
      many_classnames = (const char **) malloc_area;
      many_locks = (LOCK *) ((char *) malloc_area +
			     (sizeof (char *) * num_classes));
      guessed_class_oids = (OID *) ((char *) many_locks +
				    (sizeof (int) * num_classes));

      for (i = 0; i < num_classes; i++)
	{
	  ptr = or_unpack_string_nocopy (ptr, &many_classnames[i]);
	  ptr = or_unpack_lock (ptr, &many_locks[i]);
	  ptr = or_unpack_oid (ptr, &guessed_class_oids[i]);
	}

      allfind = xlocator_find_lockhint_class_oids (thread_p, num_classes,
						   many_classnames,
						   many_locks,
						   guessed_class_oids,
						   quit_on_errors,
						   &found_lockhint,
						   &copy_area);
    }
  if (allfind != LC_CLASSNAME_EXIST)
    {
      return_error_to_client (thread_p, rid);
    }

  if (found_lockhint != NULL && found_lockhint->length > 0)
    {
      send_size = locator_pack_lockhint (found_lockhint, true);

      packed = found_lockhint->packed;
      packed_size = found_lockhint->packed_size;

      if (!packed)
	{
	  return_error_to_client (thread_p, rid);
	  allfind = LC_CLASSNAME_ERROR;
	}
    }

  if (copy_area != NULL)
    {
      num_objs = locator_send_copy_area (copy_area, &content_ptr,
					 &content_size, &desc_ptr,
					 &desc_size);
    }
  else
    {
      desc_ptr = NULL;
      desc_size = 0;
      content_ptr = NULL;
      content_size = 0;
    }

  /* Send sizes of databuffer and copy area (descriptor + content) */

  ptr = or_pack_int (reply, send_size);
  ptr = or_pack_int (ptr, num_objs);
  ptr = or_pack_int (ptr, desc_size);
  ptr = or_pack_int (ptr, content_size);

  ptr = or_pack_int (ptr, allfind);

  if (copy_area == NULL && found_lockhint == NULL)
    {
      css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
				OR_ALIGNED_BUF_SIZE (a_reply));
    }
  else
    {
      css_send_reply_to_client (thread_p->conn_entry, rid, 4,
				reply, OR_ALIGNED_BUF_SIZE (a_reply),
				packed, send_size,
				desc_ptr, desc_size,
				content_ptr, content_size);
      if (copy_area != NULL)
	{
	  locator_free_copy_area (copy_area);
	}

      if (found_lockhint != NULL)
	{
	  locator_free_lockhint (found_lockhint);
	}

      if (desc_ptr)
	{
	  free_and_init (desc_ptr);
	}
    }

  if (malloc_area)
    {
      free_and_init (malloc_area);
    }
}

/*
 * slocator_fetch_lockhint_classes -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
slocator_fetch_lockhint_classes (THREAD_ENTRY * thread_p,
				 unsigned int rid, char *request,
				 UNUSED_ARG int reqlen)
{
  int success;
  LC_COPYAREA *copy_area;
  LC_LOCKHINT *lockhint;
  OR_ALIGNED_BUF (NET_SENDRECV_BUFFSIZE + NET_COPY_AREA_SENDRECV_SIZE +
		  OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *desc_ptr;
  int desc_size;
  char *content_ptr;
  int content_size;
  char *ptr;
  bool first_call;
  int num_objs;
  char *packed = NULL;
  int packed_size;
  int send_size;
  CSS_NET_PACKET *recv_packet = thread_p->recv_packet;

  ptr = or_unpack_int (request, &packed_size);

  packed = css_net_packet_get_buffer (recv_packet, 1, packed_size, false);

  if (packed_size == 0 || packed == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_SERVER_DATA_RECEIVE,
	      0);
      css_send_abort_to_client (thread_p->conn_entry, rid);
      return;
    }

  lockhint = locator_allocate_and_unpack_lockhint (packed, packed_size,
						   true, false);
  if ((lockhint == NULL) || (lockhint->length <= 0))
    {
      return_error_to_client (thread_p, rid);
      ptr = or_pack_int (reply, 0);
      ptr = or_pack_int (ptr, 0);
      ptr = or_pack_int (ptr, 0);
      ptr = or_pack_int (ptr, 0);
      ptr = or_pack_int (ptr, ER_FAILED);
      css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
				OR_ALIGNED_BUF_SIZE (a_reply));
      return;
    }

  assert (lockhint != NULL);

  first_call = true;
  do
    {
      desc_ptr = NULL;
      num_objs = 0;

      copy_area = NULL;
      success = xlocator_fetch_lockhint_classes (thread_p, lockhint,
						 &copy_area);
      if (success != NO_ERROR)
	{
	  return_error_to_client (thread_p, rid);
	}

      if (copy_area != NULL)
	{
	  num_objs = locator_send_copy_area (copy_area, &content_ptr,
					     &content_size, &desc_ptr,
					     &desc_size);
	}
      else
	{
	  desc_ptr = NULL;
	  desc_size = 0;
	  content_ptr = NULL;
	  content_size = 0;
	}

      /* Send sizes of databuffer and copy area (descriptor + content) */

      send_size = locator_pack_lockhint (lockhint, first_call);

      packed = lockhint->packed;
      packed_size = lockhint->packed_size;

      ptr = or_pack_int (reply, send_size);
      ptr = or_pack_int (ptr, num_objs);
      ptr = or_pack_int (ptr, desc_size);
      ptr = or_pack_int (ptr, content_size);

      ptr = or_pack_int (ptr, success);

      css_send_reply_to_client (thread_p->conn_entry, rid, 4,
				reply, OR_ALIGNED_BUF_SIZE (a_reply),
				packed, send_size,
				desc_ptr, desc_size,
				content_ptr, content_size);

      if (copy_area != NULL)
	{
	  locator_free_copy_area (copy_area);
	}
      if (desc_ptr)
	{
	  free_and_init (desc_ptr);
	}

      first_call = false;
    }
  while (copy_area
	 && ((lockhint->num_classes > lockhint->num_classes_processed)));

  locator_free_lockhint (lockhint);
}


/*
 * sthread_kill_tran_index -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sthread_kill_tran_index (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, UNUSED_ARG int reqlen)
{
  int success;
  int kill_tran_index;
  int kill_pid;
  const char *kill_user;
  const char *kill_host;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_int (request, &kill_tran_index);
  ptr = or_unpack_string_nocopy (ptr, &kill_user);
  ptr = or_unpack_string_nocopy (ptr, &kill_host);
  ptr = or_unpack_int (ptr, &kill_pid);

  success = (xthread_kill_tran_index (thread_p, kill_tran_index, kill_user,
				      kill_host, kill_pid) == NO_ERROR)
    ? NO_ERROR : ER_FAILED;
  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  ptr = or_pack_int (reply, success);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sthread_dump_cs_stat -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sthread_dump_cs_stat (THREAD_ENTRY * thread_p, unsigned int rid,
		      char *request, UNUSED_ARG int reqlen)
{
  FILE *outfp;
  int buffer_size;

  (void) or_unpack_int (request, &buffer_size);

  outfp = dump_func_open_tmpfile (thread_p, rid);
  if (outfp == NULL)
    {
      return;
    }

  csect_dump_statistics (outfp);

  dump_func_send_result (thread_p, rid, buffer_size, outfp);
}

/*
 * sthread_dump_server_stat -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sserver_stats_dump (THREAD_ENTRY * thread_p, unsigned int rid,
		    char *request, UNUSED_ARG int reqlen)
{
  FILE *outfp;
  int buffer_size;

  (void) or_unpack_int (request, &buffer_size);

  outfp = dump_func_open_tmpfile (thread_p, rid);
  if (outfp == NULL)
    {
      return;
    }

  server_stats_dump (outfp);

  dump_func_send_result (thread_p, rid, buffer_size, outfp);
}

/*
 * slogtb_get_pack_tran_table -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 */
void
slogtb_get_pack_tran_table (THREAD_ENTRY * thread_p, unsigned int rid,
			    char *request, UNUSED_ARG int reqlen)
{
  char *buffer, *ptr;
  int size;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  int error;
  int include_query_exec_info;

  (void) or_unpack_int (request, &include_query_exec_info);

  error =
    xlogtb_get_pack_tran_table (thread_p, &buffer, &size,
				include_query_exec_info);


  if (error != NO_ERROR)
    {
      ptr = or_pack_int (reply, 0);
      ptr = or_pack_int (ptr, error);
      css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
				OR_ALIGNED_BUF_SIZE (a_reply));
    }
  else
    {
      ptr = or_pack_int (reply, size);
      ptr = or_pack_int (ptr, error);
      css_send_reply_to_client (thread_p->conn_entry, rid, 2,
				reply, OR_ALIGNED_BUF_SIZE (a_reply),
				buffer, size);
      free_and_init (buffer);
    }
}

/*
 * slogtb_dump_trantable -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 */
void
slogtb_dump_trantable (THREAD_ENTRY * thread_p, unsigned int rid,
		       char *request, UNUSED_ARG int reqlen)
{
  FILE *outfp;
  int buffer_size;

  (void) or_unpack_int (request, &buffer_size);

  outfp = dump_func_open_tmpfile (thread_p, rid);
  if (outfp == NULL)
    {
      return;
    }

  xlogtb_dump_trantable (thread_p, outfp);

  dump_func_send_result (thread_p, rid, buffer_size, outfp);
}


/*
 * xlog_send_log_pages_to_client -
 *
 * return:
 * NOTE:
 */
int
xlog_send_log_pages_to_client (THREAD_ENTRY * thread_p,
			       char *logpg_area, int area_size,
			       INT64 first_pageid, int num_page,
			       int file_status)
{
  OR_ALIGNED_BUF (OR_INT64_SIZE + OR_INT_SIZE * 5) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  unsigned int rid, rc;
  char *ptr;

  rid = thread_get_comm_request_id (thread_p);

  /*
   * Client side caller must be expecting a reply/callback followed
   * by 2 ints, otherwise client will abort due to protocol error
   * Prompt_length tells the receiver how big the following message is.
   */
  if (first_pageid < 0 || num_page < 0)
    {
      assert (false);
      return ER_FAILED;
    }

  ptr = or_pack_int (reply, (int) GET_NEXT_LOG_PAGES);
  ptr = or_pack_int (ptr, area_size);
  ptr = or_pack_int64 (ptr, first_pageid);
  ptr = or_pack_int (ptr, num_page);
  ptr = or_pack_int (ptr, file_status);
  ptr = or_pack_int (ptr, svr_shm_get_server_state ());

#if 1
  if (ZIP_CHECK (area_size))
    {
      area_size = (int) GET_ZIP_LEN (area_size);
      assert (area_size > 0);
    }
#endif

  rc = css_send_reply_to_client (thread_p->conn_entry, rid, 2,
				 reply, OR_ALIGNED_BUF_SIZE (a_reply),
				 logpg_area, area_size);
  if (rc)
    {
      return ER_FAILED;
    }

  er_log_debug (ARG_FILE_LINE,
		"xlog_send_log_pages_to_client, reply(GET_NEXT_LOG_PAGES), area_size(%d)\n",
		area_size);

  return NO_ERROR;
}

/*
 * xlog_send_log_pages_to_migrator -
 *
 * return:
 * NOTE:
 */
int
xlog_send_log_pages_to_migrator (THREAD_ENTRY * thread_p,
				 char *logpg_area, int area_size,
				 UNUSED_ARG LOGWR_MODE mode)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  unsigned int rid, rc;
  UNUSED_VAR char *ptr;

  rid = thread_get_comm_request_id (thread_p);

  ptr = or_pack_int (reply, area_size);

#if 1
  if (ZIP_CHECK (area_size))
    {
      area_size = (int) GET_ZIP_LEN (area_size);
      assert (area_size > 0);
    }
#endif

  rc = css_send_reply_to_client (thread_p->conn_entry, rid, 2,
				 reply, OR_ALIGNED_BUF_SIZE (a_reply),
				 logpg_area, area_size);
  if (rc)
    {
      return ER_FAILED;
    }

  er_log_debug (ARG_FILE_LINE,
		"xlog_send_log_pages_to_migrator, area_size(%d)\n",
		area_size);

  return NO_ERROR;
}

/*
 * xlog_get_page_request_with_reply
 *
 * return:
 * NOTE:
 */
int
xlog_get_page_request_with_reply (THREAD_ENTRY * thread_p,
				  LOG_PAGEID * fpageid_ptr,
				  LOGWR_MODE * mode_ptr)
{
  char *reply = NULL;
  int reply_size;
  LOG_PAGEID first_pageid;
  int mode;
  char *ptr;
  int error;
  int remote_error;
  int compressed_protocol;

  /* Obtain success message from the client, without blocking the
     server. */
  error = xs_receive_data_from_client (thread_p, &reply, &reply_size,
				       prm_get_bigint_value
				       (PRM_ID_HA_COPY_LOG_TIMEOUT));
  if (error != NO_ERROR)
    {
      if (reply)
	{
	  free_and_init (reply);
	}

      return error;
    }

  assert (reply != NULL);
  ptr = or_unpack_int64 (reply, &first_pageid);
  ptr = or_unpack_int (ptr, &mode);
  ptr = or_unpack_int (ptr, &remote_error);
  ptr = or_unpack_int (ptr, &compressed_protocol);	/* ignore */
  free_and_init (reply);

  *fpageid_ptr = first_pageid;
  *mode_ptr = mode;

  er_log_debug (ARG_FILE_LINE, "xlog_get_page_request_with_reply, "
		"fpageid(%lld), mode(%s), compressed_protocol(%d)\n",
		first_pageid, LOGWR_MODE_NAME (mode), compressed_protocol);

  return (remote_error != NO_ERROR) ? remote_error : error;
}

/*
 * sheap_get_class_num_objs_and_pages -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sheap_get_class_num_objs_and_pages (THREAD_ENTRY * thread_p,
				    unsigned int rid, char *request,
				    UNUSED_ARG int reqlen)
{
  HFID hfid;
  int success, approximation, npages;
  DB_BIGINT nobjs;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2 + OR_INT64_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;

  ptr = or_unpack_hfid (request, &hfid);
  ptr = or_unpack_int (ptr, &approximation);

  success = xheap_get_class_num_objects_pages (thread_p, &hfid,
					       approximation, &nobjs,
					       &npages);
  if (success != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  ptr = or_pack_int64 (reply, nobjs);
  ptr = or_pack_int (ptr, (int) success);
  ptr = or_pack_int (ptr, (int) npages);

  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sqp_get_server_info -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
sqp_get_server_info (THREAD_ENTRY * thread_p, unsigned int rid,
		     char *request, UNUSED_ARG int reqlen)
{
  int success = NO_ERROR;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr, *buffer = NULL;
  int buffer_length;
  int info_bits, i;
  DB_VALUE value[SI_CNT];

  ptr = or_unpack_int (request, &info_bits);

  for (i = 0, buffer_length = 0; i < SI_CNT && success == NO_ERROR; i++)
    {
      DB_MAKE_NULL (&value[i]);
      if (info_bits & (1 << i))
	{
	  switch ((1 << i))
	    {
	    case SI_SYS_DATETIME:
	      success = ((db_sys_datetime (&value[i]) == NO_ERROR)
			 ? NO_ERROR : ER_FAILED);
	      break;

	    default:
	      break;
	    }
	  buffer_length += OR_VALUE_ALIGNED_SIZE (&value[i]);
	  /* increase buf length */
	}
    }
  if (success == ER_FAILED)
    {
      return_error_to_client (thread_p, rid);
    }

  if (success == NO_ERROR)
    {				/* buffer_length > 0 */
      buffer = (char *) malloc (buffer_length);

      if (buffer != NULL)
	{
	  for (i = 0, ptr = buffer; i < SI_CNT; i++)
	    {
	      if (info_bits & (1 << i))
		{
		  ptr = or_pack_db_value (ptr, &value[i]);
		  db_value_clear (&value[i]);
		}
	    }

#if !defined(NDEBUG)
	  /* suppress valgrind UMW error */
	  memset (ptr, 0, buffer_length - (ptr - buffer));
#endif
	}
      else
	{
	  buffer_length = 0;
	  success = ER_OUT_OF_VIRTUAL_MEMORY;
	}
    }
  ptr = or_pack_int (reply, buffer_length);
  ptr = or_pack_int (ptr, success);

  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    buffer, buffer_length);

  if (buffer != NULL)
    {
      free_and_init (buffer);
    }
}

/*
 * sprm_server_change_parameters () - Changes server's system parameter
 *				      values.
 *
 * return	 :
 * thread_p (in) :
 * rid (in)      :
 * request (in)  :
 * reqlen (in)   :
 */
void
sprm_server_change_parameters (THREAD_ENTRY * thread_p, unsigned int rid,
			       char *request, UNUSED_ARG int reqlen)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  SYSPRM_ASSIGN_VALUE *assignments = NULL;

  (void) sysprm_unpack_assign_values (request, &assignments);

  xsysprm_change_server_parameters (assignments);

  (void) or_pack_int (reply, PRM_ERR_NO_ERROR);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));

  sysprm_free_assign_values (&assignments);
}

/*
 * sprm_server_get_force_parameters () - Obtains values for server's system
 *					 parameters that are marked with
 *					 PRM_FORCE_SERVER flag.
 *
 * return	 :
 * thread_p (in) :
 * rid (in)	 :
 * request (in)	 :
 * reqlen (in)	 :
 */
void
sprm_server_get_force_parameters (THREAD_ENTRY * thread_p, unsigned int rid,
				  UNUSED_ARG char *request,
				  UNUSED_ARG int reqlen)
{
  SYSPRM_ASSIGN_VALUE *change_values;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  int area_size;
  char *area = NULL, *ptr = NULL;


  change_values = xsysprm_get_force_server_parameters ();
  if (change_values == NULL)
    {
      return_error_to_client (thread_p, rid);
    }

  area_size = sysprm_packed_assign_values_length (change_values, 0);
  area = (char *) malloc (area_size);
  if (area == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, area_size);
      return_error_to_client (thread_p, rid);
      area_size = 0;
    }
  ptr = or_pack_int (reply, area_size);
  ptr = or_pack_int (ptr, er_errid ());

  (void) sysprm_pack_assign_values (area, change_values);
  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    area, area_size);

  if (area != NULL)
    {
      free_and_init (area);
    }
  sysprm_free_assign_values (&change_values);
}

/*
 * sprm_server_obtain_parameters () - Obtains server's system parameter values
 *				      for the requested parameters.
 *
 * return	 :
 * thread_p (in) :
 * rid (in)	 :
 * request (in)	 :
 * reqlen (in)	 :
 */
void
sprm_server_obtain_parameters (THREAD_ENTRY * thread_p, unsigned int rid,
			       char *request, UNUSED_ARG int reqlen)
{
  SYSPRM_ERR rc = PRM_ERR_NO_ERROR;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr = NULL, *reply_data = NULL;
  int reply_data_size;
  SYSPRM_ASSIGN_VALUE *prm_values = NULL;

  (void) sysprm_unpack_assign_values (request, &prm_values);
  xsysprm_obtain_server_parameters (prm_values);
  reply_data_size = sysprm_packed_assign_values_length (prm_values, 0);
  reply_data = (char *) malloc (reply_data_size);
  if (reply_data == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, reply_data_size);
      rc = PRM_ERR_NO_MEM_FOR_PRM;
      reply_data_size = 0;
    }
  else
    {
      (void) sysprm_pack_assign_values (reply_data, prm_values);
    }
  ptr = or_pack_int (reply, reply_data_size);
  ptr = or_pack_int (ptr, rc);
  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    reply_data, reply_data_size);
  if (reply_data != NULL)
    {
      free_and_init (reply_data);
    }
  if (prm_values != NULL)
    {
      sysprm_free_assign_values (&prm_values);
    }
}

/*
 * sprm_server_dump_parameters -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 */
void
sprm_server_dump_parameters (THREAD_ENTRY * thread_p, unsigned int rid,
			     char *request, UNUSED_ARG int reqlen)
{
  FILE *outfp;
  int buffer_size;

  (void) or_unpack_int (request, &buffer_size);

  outfp = dump_func_open_tmpfile (thread_p, rid);
  if (outfp == NULL)
    {
      return;
    }

  xsysprm_dump_server_parameters (outfp);

  dump_func_send_result (thread_p, rid, buffer_size, outfp);
}

/*
 * srepl_set_info -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
srepl_set_info (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
		UNUSED_ARG int reqlen)
{
  int success = NO_ERROR;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;
  REPL_INFO repl_info = { 0, NULL };
  REPL_INFO_SCHEMA repl_schema = {
    0, REPL_UNKNOWN_DDL, NULL, NULL, NULL
  };

  if (log_does_allow_replication () == true)
    {
      ptr = or_unpack_int (request, &repl_info.repl_info_type);
      switch (repl_info.repl_info_type)
	{
	case REPL_INFO_TYPE_SCHEMA:
	  {
	    ptr = or_unpack_int (ptr, &repl_schema.statement_type);
	    ptr = or_unpack_int (ptr, &repl_schema.online_ddl_type);
	    ptr = or_unpack_string_nocopy (ptr, &repl_schema.name);
	    ptr = or_unpack_string_nocopy (ptr, &repl_schema.ddl);
	    ptr = or_unpack_string_nocopy (ptr, &repl_schema.db_user);

	    repl_info.info = (char *) &repl_schema;
	    break;
	  }
	default:
	  success = ER_FAILED;
	  break;
	}

      if (success == NO_ERROR)
	{
	  success = xrepl_set_info (thread_p, &repl_info);
	}
    }

  (void) or_pack_int (reply, success);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * srepl_log_get_eof_lsa -
 *
 * return:
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE:
 */
void
srepl_log_get_eof_lsa (THREAD_ENTRY * thread_p, unsigned int rid,
		       UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  OR_ALIGNED_BUF (OR_LOG_LSA_ALIGNED_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  LOG_LSA lsa;

  xrepl_log_get_eof_lsa (thread_p, &lsa);

  reply = OR_ALIGNED_BUF_START (a_reply);
  (void) or_pack_log_lsa (reply, &lsa);

  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * slogwr_get_log_pages -
 *
 * return:
 *
 *   thread_p(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * Note:
 */
void
slogwr_get_log_pages (THREAD_ENTRY * thread_p, unsigned int rid,
		      char *request, UNUSED_ARG int reqlen)
{
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;
  LOG_PAGEID first_pageid;
  LOGWR_MODE mode;
  int m, error, remote_error;
  int compressed_protocol;

  ptr = or_unpack_int64 (request, &first_pageid);
  ptr = or_unpack_int (ptr, &m);
  mode = (LOGWR_MODE) m;
  ptr = or_unpack_int (ptr, &remote_error);
  ptr = or_unpack_int (ptr, &compressed_protocol);
  assert (compressed_protocol == 1 || compressed_protocol == 0);

  error = xlogwr_get_log_pages (thread_p, first_pageid, mode,
				((compressed_protocol == 1) ? true : false));
  if (error == ER_INTERRUPTED)
    {
      return_error_to_client (thread_p, rid);
    }

  if (error == ER_NET_DATA_RECEIVE_TIMEDOUT)
    {
      css_end_server_request (thread_p->conn_entry);
    }
  else
    {
      ptr = or_pack_int (reply, (int) END_CALLBACK);
      ptr = or_pack_int (ptr, error);
      (void) css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
				       OR_ALIGNED_BUF_SIZE (a_reply));
    }

  return;
}

/*
 * ssession_find_or_create_session -
 *
 * return: void
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE: This function checks if a session is still active and creates a new
 * one if needed
 */
void
ssession_find_or_create_session (THREAD_ENTRY * thread_p, unsigned int rid,
				 char *request, UNUSED_ARG int reqlen)
{
  SESSION_KEY key = { DB_EMPTY_SESSION, INVALID_SOCKET };
  int area_size;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr = NULL, *area = NULL;
  char *db_user = NULL, *host = NULL, *program_name = NULL;
  char db_user_upper[DB_MAX_USER_LENGTH] = { '\0' };
  char server_session_key[SERVER_SESSION_KEY_SIZE];
  int error = NO_ERROR;

  ptr = or_unpack_int (request, (int *) (&key.id));
  ptr = or_unpack_stream (ptr, server_session_key, SERVER_SESSION_KEY_SIZE);
  ptr = or_unpack_string_alloc (ptr, &db_user);
  ptr = or_unpack_string_alloc (ptr, &host);
  ptr = or_unpack_string_alloc (ptr, &program_name);

  if (key.id == DB_EMPTY_SESSION
      || memcmp (server_session_key, xboot_get_server_session_key (),
		 SERVER_SESSION_KEY_SIZE) != 0
      || xsession_check_session (thread_p, &key) != NO_ERROR)
    {
      /* not an error yet */
      er_clear ();
      /* create new session */
      error = xsession_create_new (thread_p, &key);
      if (error != NO_ERROR)
	{
	  return_error_to_client (thread_p, rid);
	}
    }

  /* update session_id for this connection */
  assert (thread_p != NULL);
  assert (thread_p->conn_entry != NULL);

  key.fd = thread_p->conn_entry->fd;
  xsession_set_session_key (thread_p, &key);
  thread_p->conn_entry->session_id = key.id;

  area_size = 0;
  if (error == NO_ERROR)
    {
      /* key.id */
      area_size = OR_INT_SIZE;

      /* server session key */
      area_size += or_packed_stream_length (SERVER_SESSION_KEY_SIZE);

      area = (char *) malloc (area_size);
      if (area != NULL)
	{
	  ptr = or_pack_int (area, key.id);
	  ptr = or_pack_stream (ptr, xboot_get_server_session_key (),
				SERVER_SESSION_KEY_SIZE);
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, area_size);
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  area_size = 0;
	  return_error_to_client (thread_p, rid);
	}
    }

  if (db_user != NULL)
    {
      assert (host != NULL);
      assert (program_name != NULL);

      intl_identifier_upper (db_user, db_user_upper);
      css_set_user_access_status (db_user_upper, host, program_name);
    }

  free_and_init (db_user);
  free_and_init (host);
  free_and_init (program_name);

  ptr = or_pack_int (reply, area_size);
  ptr = or_pack_int (ptr, error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    area, area_size);
  if (area != NULL)
    {
      free_and_init (area);
    }
}

/*
 * ssession_end_session -
 *
 * return: void
 *
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * NOTE: This function ends the session with the id contained in the request
 */
void
ssession_end_session (THREAD_ENTRY * thread_p, unsigned int rid,
		      char *request, UNUSED_ARG int reqlen)
{
  int err = NO_ERROR;
  SESSION_KEY key;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  UNUSED_VAR char *ptr = NULL;

  (void) or_unpack_int (request, (int *) (&key.id));
  key.fd = thread_p->conn_entry->fd;

  err = xsession_end_session (thread_p, &key);

  ptr = or_pack_int (reply, err);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * slogin_user - login user
 * return: error code or NO_ERROR
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 */
void
slogin_user (THREAD_ENTRY * thread_p, unsigned int rid, char *request,
	     int reqlen)
{
  int err = NO_ERROR;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  const char *username = NULL;

  or_unpack_string_nocopy (request, &username);
  if (username == NULL)
    {
      return_error_to_client (thread_p, rid);
      err = ER_FAILED;
    }
  else
    {
      err = xlogin_user (thread_p, username);
      if (err != NO_ERROR)
	{
	  return_error_to_client (thread_p, rid);
	}
    }

  or_pack_int (reply, err);

  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}
#endif

/*
 * sboot_get_locales_info () - get info about locales
 * return : void
 * thread_p (in) :
 * rid (in) :
 * request (in) :
 * reqlen (in) :
 */
void
sboot_get_locales_info (THREAD_ENTRY * thread_p, unsigned int rid,
			UNUSED_ARG char *request, UNUSED_ARG int reqlen)
{
  int err = NO_ERROR;
  OR_ALIGNED_BUF (2 * OR_INT_SIZE) a_reply;
  char *reply = NULL, *ptr = NULL, *data_reply = NULL;
  int size = 0, i;
  int len_str;
  const int collation_cnt = lang_collation_count ();
  const int lang_cnt = lang_locales_count (false);
  const int locales_cnt = lang_locales_count (true);
  int found_coll = 0;

  reply = OR_ALIGNED_BUF_START (a_reply);

  /* compute size of packed information */
  for (i = 0; i < LANG_MAX_COLLATIONS; i++)
    {
      LANG_COLLATION *lc = lang_get_collation (i);

      assert (lc != NULL);
      if (i > LANG_COLL_UTF8_EN_CI
	  && lc->coll.coll_id == LANG_COLL_UTF8_EN_CI)
	{
	  continue;
	}
      found_coll++;

      size += 2 * OR_INT_SIZE;	/* collation id , codeset */
      size += or_packed_string_length (lc->coll.coll_name, &len_str);
      size += or_packed_string_length (lc->coll.checksum, &len_str);
    }

  assert (found_coll == collation_cnt);

  for (i = 0; i < lang_cnt; i++)
    {
      const LANG_LOCALE_DATA *lld = lang_get_first_locale_for_lang (i);

      assert (lld != NULL);

      do
	{
	  size += or_packed_string_length (lld->lang_name, &len_str);
	  size += OR_INT_SIZE;	/* codeset */
	  size += or_packed_string_length (lld->checksum, &len_str);

	  lld = lld->next_lld;
	}
      while (lld != NULL);
    }

  size += 2 * OR_INT_SIZE;	/* collation_cnt, locales_cnt */

  data_reply = (char *) malloc (size);
  if (data_reply != NULL)
    {
      ptr = or_pack_int (data_reply, collation_cnt);
      ptr = or_pack_int (ptr, locales_cnt);
      found_coll = 0;

      /* pack collation information : */
      for (i = 0; i < LANG_MAX_COLLATIONS; i++)
	{
	  LANG_COLLATION *lc = lang_get_collation (i);

	  assert (lc != NULL);

	  if (i > LANG_COLL_UTF8_EN_CI
	      && lc->coll.coll_id == LANG_COLL_UTF8_EN_CI)
	    {
	      continue;
	    }

	  found_coll++;

	  ptr = or_pack_int (ptr, lc->coll.coll_id);

	  len_str = strlen (lc->coll.coll_name);
	  ptr = or_pack_string_with_length (ptr, lc->coll.coll_name, len_str);

	  ptr = or_pack_int (ptr, (int) lc->codeset);

	  len_str = strlen (lc->coll.checksum);
	  ptr = or_pack_string_with_length (ptr, lc->coll.checksum, len_str);
	}
      assert (found_coll == collation_cnt);

      /* pack locale information : */
      for (i = 0; i < lang_cnt; i++)
	{
	  const LANG_LOCALE_DATA *lld = lang_get_first_locale_for_lang (i);

	  assert (lld != NULL);

	  do
	    {
	      len_str = strlen (lld->lang_name);
	      ptr = or_pack_string_with_length (ptr, lld->lang_name, len_str);

	      ptr = or_pack_int (ptr, lld->codeset);

	      len_str = strlen (lld->checksum);
	      ptr = or_pack_string_with_length (ptr, lld->checksum, len_str);

	      lld = lld->next_lld;
	    }
	  while (lld != NULL);
	}
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      size);
      return_error_to_client (thread_p, rid);
      size = 0;
      err = ER_FAILED;
    }

  ptr = or_pack_int (reply, size);
  ptr = or_pack_int (ptr, err);

  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    data_reply, size);

  if (data_reply != NULL)
    {
      free_and_init (data_reply);
    }
}

/*
 * slocator_lock_system_ddl_lock - Set global system DDL lock
 *
 * return:
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 */
void
slocator_lock_system_ddl_lock (THREAD_ENTRY * thread_p, unsigned int rid,
			       UNUSED_ARG char *request,
			       UNUSED_ARG int reqlen)
{
  int error = NO_ERROR;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  error = xlock_system_ddl_lock (thread_p);
  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, (int) error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * slock_change_class_xlock_to_ulock ()
 *
 * return:
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 */
void
slock_change_class_xlock_to_ulock (THREAD_ENTRY * thread_p,
				   unsigned int rid, char *request,
				   int reqlen)
{
  int error = NO_ERROR;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;
  OID class_oid;

  reply = OR_ALIGNED_BUF_START (a_reply);

  or_unpack_oid (request, &class_oid);

  error = xlock_change_class_xlock_to_ulock (thread_p, &class_oid);
  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}
#endif

/*
 * smigrator_get_log_pages -
 *
 * return:
 *
 *   thread_p(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * Note:
 */
void
smigrator_get_log_pages (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, UNUSED_ARG int reqlen)
{
  char *ptr;
  LOG_PAGEID first_pageid;
  int compressed_protocol;
  int error;

  ptr = or_unpack_int64 (request, &first_pageid);
  ptr = or_unpack_int (ptr, &compressed_protocol);
  assert (compressed_protocol == 1 || compressed_protocol == 0);

  error = xmigrator_get_log_pages (thread_p, first_pageid,
				   ((compressed_protocol ==
				     1) ? true : false));

  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  return;
}

/*
 * slogtb_update_group_id -
 *
 * return:
 *
 *   thread_p(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * Note:
 */
void
slogtb_update_group_id (THREAD_ENTRY * thread_p, unsigned int rid,
			char *request, UNUSED_ARG int reqlen)
{
  int migrator_id, group_id, target, on_off;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply, *ptr;
  int error;

  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_int (request, &migrator_id);
  ptr = or_unpack_int (ptr, &group_id);
  ptr = or_unpack_int (ptr, &target);
  ptr = or_unpack_int (ptr, &on_off);

  error = xlogtb_update_group_id (thread_p, migrator_id, group_id, target,
				  on_off);

  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * slogtb_block_global_dml -
 *
 * return:
 *
 *   thread_p(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * Note:
 */
void
slogtb_block_global_dml (THREAD_ENTRY * thread_p, unsigned int rid,
			 char *request, UNUSED_ARG int reqlen)
{
  int start_or_end;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;
  UNUSED_VAR char *ptr;
  int error;

  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_int (request, &start_or_end);

  error = xlogtb_block_global_dml (thread_p, start_or_end);

  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }

  (void) or_pack_int (reply, error);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * sbk_prepare_backup -
 *
 * return:
 *
 *   thread_p(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * Note:
 */
void
sbk_prepare_backup (THREAD_ENTRY * thread_p, unsigned int rid,
		    char *request, UNUSED_ARG int reqlen)
{
  int num_threads, do_compress, sleep_msecs, make_slave;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  int area_size, strlen1, strlen3;
  char *reply = NULL, *area = NULL, *ptr = NULL;
  int error;
  BK_BACKUP_SESSION *session;

  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_unpack_int (request, &num_threads);
  ptr = or_unpack_int (ptr, &do_compress);
  ptr = or_unpack_int (ptr, &sleep_msecs);
  ptr = or_unpack_int (ptr, &make_slave);

  session = (BK_BACKUP_SESSION *) malloc (sizeof (BK_BACKUP_SESSION));
  if (session == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (BK_BACKUP_SESSION));
      return_error_to_client (thread_p, rid);
      area_size = 0;
      goto end;
    }

  error = xbk_prepare_backup (thread_p, num_threads, do_compress,
			      sleep_msecs, make_slave, session);

  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
      area_size = 0;
      goto end;
    }

  area_size = OR_INT_SIZE	/* iopageid */
    + or_packed_string_length (session->bkuphdr->bk_magic, &strlen1)	/* magic */
    + OR_VERSION_SIZE		/* db_version */
    + OR_INT_SIZE		/* bk_hdr_version */
    + OR_BIGINT_ALIGNED_SIZE	/* db_creation */
    + OR_BIGINT_ALIGNED_SIZE	/* start_time */
    + or_packed_string_length (session->bkuphdr->db_name, &strlen3) + OR_INT_SIZE	/* db_iopagesize */
    + OR_LOG_LSA_ALIGNED_SIZE	/* chkpt_lsa */
    + OR_INT_SIZE		/* bkpagesize */
    + OR_INT_SIZE		/* first_arv_needed */
    + OR_INT_SIZE		/* saved_run_nxchkpt_atpageid */
    + OR_INT_SIZE;		/* num_volumes */

  area = (char *) malloc (area_size);
  if (area == NULL)
    {
      return_error_to_client (thread_p, rid);
      area_size = 0;
      goto end;
    }

  ptr = or_pack_int (area, session->bkuphdr->iopageid);
  ptr = or_pack_string_with_length (ptr, session->bkuphdr->bk_magic, strlen1);
  ptr = or_pack_version (ptr, &session->bkuphdr->bk_db_version);
  ptr = or_pack_int (ptr, session->bkuphdr->bk_hdr_version);
  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_pack_int64 (ptr, session->bkuphdr->db_creation);
  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_pack_int64 (ptr, session->bkuphdr->start_time);
  ptr = or_pack_string_with_length (ptr, session->bkuphdr->db_name, strlen3);
  ptr = or_pack_int (ptr, session->bkuphdr->db_iopagesize);
  ptr = or_pack_log_lsa (ptr, &session->bkuphdr->chkpt_lsa);
  ptr = or_pack_int (ptr, session->bkuphdr->bkpagesize);
  ptr = or_pack_int (ptr, session->first_arv_needed);
  ptr = or_pack_int (ptr, session->saved_run_nxchkpt_atpageid);
  assert (LSA_ISNULL (&(session->bkuphdr->backuptime_lsa)));
  assert (session->bkuphdr->end_time == -1);
  ptr = or_pack_int (ptr, session->num_perm_vols);

end:
  ptr = or_pack_int (reply, area_size);
  css_send_reply_to_client (thread_p->conn_entry, rid, 2,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply),
			    area, area_size);

  if (area != NULL)
    {
      free_and_init (area);
    }
}

/*
 * sbk_backup_volume -
 *
 * return:
 *
 *   thread_p(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * Note:
 */
void
sbk_backup_volume (THREAD_ENTRY * thread_p, unsigned int rid,
		   char *request, UNUSED_ARG int reqlen)
{
  int buf_size;
  int error;

  (void) or_unpack_int (request, &buf_size);

  error = xbk_backup_volume (thread_p, rid, buf_size);

  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }
}

/*
 * sbk_backup_log_volume -
 *
 * return:
 *
 *   thread_p(in):
 *   rid(in):
 *   request(in):
 *   reqlen(in):
 *
 * Note:
 */
void
sbk_backup_log_volume (THREAD_ENTRY * thread_p, unsigned int rid,
		       char *request, UNUSED_ARG int reqlen)
{
  int buf_size, delete_unneeded_logarchives;
  char *ptr;
  int error;

  ptr = or_unpack_int (request, &buf_size);
  ptr = or_unpack_int (ptr, &delete_unneeded_logarchives);

  error = xbk_backup_log_volume (thread_p, rid, buf_size,
				 delete_unneeded_logarchives);

  if (error != NO_ERROR)
    {
      return_error_to_client (thread_p, rid);
    }
}

static FILE *
dump_func_open_tmpfile (THREAD_ENTRY * thread_p, unsigned int rid)
{
  FILE *outfp;

  outfp = tmpfile ();
  if (outfp == NULL)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
			   1, "");
      css_send_abort_to_client (thread_p->conn_entry, rid);
      return NULL;
    }

  return outfp;
}

static void
dump_func_send_result (THREAD_ENTRY * thread_p, unsigned int rid,
		       int send_chunk_size, FILE * tmpfp)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *buffer = NULL;
  size_t send_size;
  int file_size;

  if (send_chunk_size <= 0)
    {
      css_send_abort_to_client (thread_p->conn_entry, rid);
      goto end;
    }

  buffer = (char *) malloc (send_chunk_size);
  if (buffer == NULL)
    {
      css_send_abort_to_client (thread_p->conn_entry, rid);
      goto end;
    }

  file_size = ftell (tmpfp);

  /*
   * Send the file in pieces
   */
  rewind (tmpfp);

  (void) or_pack_int (reply, (int) file_size);
  css_send_reply_to_client (thread_p->conn_entry, rid, 1, reply,
			    OR_ALIGNED_BUF_SIZE (a_reply));

  while (file_size > 0)
    {
      if (file_size > send_chunk_size)
	{
	  send_size = send_chunk_size;
	}
      else
	{
	  send_size = file_size;
	}

      file_size -= send_size;
      if (fread (buffer, 1, send_size, tmpfp) != send_size)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_GENERIC_ERROR, 1, "");
	  css_send_abort_to_client (thread_p->conn_entry, rid);
	  goto end;
	}
      css_send_reply_to_client (thread_p->conn_entry, rid, 1,
				buffer, send_size);
    }

end:
  fclose (tmpfp);
  free_and_init (buffer);
}

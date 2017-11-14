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
 * network_interface_cl.c - Interface functions for client requests.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>

#include "porting.h"
#include "network.h"
#include "network_interface_cl.h"
#include "memory_alloc.h"
#include "storage_common.h"
#if defined(CS_MODE)
#include "server_interface.h"
#include "boot_cl.h"
#else
#include "xserver_interface.h"
#endif
#include "boot_sr.h"
#include "locator_sr.h"
#include "oid.h"
#include "error_manager.h"
#include "object_representation.h"
#include "log_comm.h"
#include "arithmetic.h"
#include "query_executor.h"
#include "transaction_cl.h"
#include "language_support.h"
#include "statistics.h"
#include "system_parameter.h"
#include "transaction_sr.h"
#include "repl_log.h"
#include "connection_support.h"
#include "backup_cl.h"

/*
 * Use db_clear_private_heap instead of db_destroy_private_heap
 */
#define ENTER_SERVER() \
  do \
    { \
      assert (db_on_server == 0); \
      db_on_server++; \
      logtb_start_transaction_if_needed (NULL); \
    } \
  while (0)

#define EXIT_SERVER() \
  do \
    { \
      assert (db_on_server == 1); \
      db_on_server--; \
    } \
  while (0)

#define NET_COPY_AREA_SENDRECV_SIZE (OR_INT_SIZE * 3)
#define NET_SENDRECV_BUFFSIZE (OR_INT_SIZE)

#define PLAN_BUF_INITIAL_LENGTH (1024)

/*
 * Flag to indicate whether we've crossed the client/server boundary.
 * It really only comes into play in standalone.
 */
unsigned int db_on_server = 0;


static char *db_Execution_plan = NULL;
static int db_Execution_plan_length = -1;

#if defined(CS_MODE)
static char *pack_const_string (char *buffer, const char *cstring);
static char *pack_string_with_null_padding (char *buffer, const char *stream,
					    int len);
static int length_const_string (const char *cstring, int *strlen);
static int length_string_with_null_padding (int len);
static QFILE_LIST_ID *get_list_id_from_execute_res (int replydata_size_listid,
						    char *replydata_listid,
						    int replydata_size_page,
						    char *replydata_page);
static int make_copyarea_from_server_reply (int num_objs,
					    int packed_desc_size,
					    int content_size,
					    char *reply_packed_desc,
					    char *reply_content_ptr,
					    LC_COPYAREA ** reply_copy_area);
#endif /* CS_MODE */

#if defined(CS_MODE)
/*
 * pack_const_string -
 *
 * return:
 *
 *   buffer(in):
 *   cstring(in):
 *
 * NOTE:
 */
static char *
pack_const_string (char *buffer, const char *cstring)
{
  return or_pack_string (buffer, cstring);
}

/*
 * pack_string_with_null_padding - pack stream and add null.
 *                                 so stream is made as null terminated string.
 *
 * return:
 *
 *   buffer(in):
 *   stream(in):
 *   len(in):
 *
 * NOTE:
 */
static char *
pack_string_with_null_padding (char *buffer, const char *stream, int len)
{
  return or_pack_string_with_null_padding (buffer, stream, len);
}

/*
 * pack_const_string_with_length -
 *
 * return:
 *
 *   buffer(in):
 *   cstring(in):
 *   strlen(in):
 *
 * NOTE:
 */
static char *
pack_const_string_with_length (char *buffer, const char *cstring, int strlen)
{
  return or_pack_string_with_length (buffer, cstring, strlen);
}

/*
 * length_const_string -
 *
 * return:
 *
 *   cstring(in):
 *   strlen(out): strlen(cstring)
 */
static int
length_const_string (const char *cstring, int *strlen)
{
  return or_packed_string_length (cstring, strlen);
}


/*
 * length_string_with_null_padding - calculate length with null padding
 *
 * return:
 *
 *   len(in): stream length
 */
static int
length_string_with_null_padding (int len)
{
  return or_packed_stream_length (len + 1);	/* 1 for NULL padding */
}
#endif /* CS_MODE */

/*
 * locator_fetch -
 *
 * return:
 *
 *   oidp(in):
 *   lock(in):
 *   class_oid(in):
 *   prefetch(in):
 *   fetch_copyarea(in):
 */
int
locator_fetch (OID * oidp, LOCK lock, OID * class_oid, int prefetch,
	       LC_COPYAREA ** fetch_copyarea)
{
#if defined(CS_MODE)
  int success = ER_FAILED;
  int req_error;
  char *ptr;
  OR_ALIGNED_BUF ((OR_OID_SIZE * 2) + (OR_INT_SIZE * 2)) a_request;
  char *request;
  OR_ALIGNED_BUF (NET_COPY_AREA_SENDRECV_SIZE + OR_INT_SIZE) a_reply;
  char *reply;
  CSS_NET_PACKET *recv_packet = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_oid (request, oidp);
  ptr = or_pack_lock (ptr, lock);
  ptr = or_pack_oid (ptr, class_oid);
  ptr = or_pack_int (ptr, prefetch);
  *fetch_copyarea = NULL;

  req_error = net_client_request (NET_SERVER_LC_FETCH,
				  &recv_packet, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      int num_objs;
      int packed_desc_size;
      int content_size;

      ptr = or_unpack_int (reply, &num_objs);
      ptr = or_unpack_int (ptr, &packed_desc_size);
      ptr = or_unpack_int (ptr, &content_size);
      ptr = or_unpack_int (ptr, &success);

      if (success == NO_ERROR)
	{
	  char *reply_packed_desc;
	  char *reply_content_ptr;

	  reply_packed_desc = css_net_packet_get_buffer (recv_packet, 1,
							 packed_desc_size,
							 false);
	  reply_content_ptr = css_net_packet_get_buffer (recv_packet, 2,
							 content_size, false);
	  success = make_copyarea_from_server_reply (num_objs,
						     packed_desc_size,
						     content_size,
						     reply_packed_desc,
						     reply_content_ptr,
						     fetch_copyarea);
	}

      css_net_packet_free (recv_packet);
    }
  else
    {
      *fetch_copyarea = NULL;
    }

  return success;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xlocator_fetch (NULL, oidp, lock, class_oid, prefetch,
			    fetch_copyarea);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * locator_get_class -
 *
 * return:
 *
 *   class_oid(in):
 *   oid(in):
 *   lock(in):
 *   prefetching(in):
 *   fetch_copyarea(in):
 *
 * NOTE:
 */
int
locator_get_class (OID * class_oid, const OID * oid, LOCK lock,
		   int prefetching, LC_COPYAREA ** fetch_copyarea)
{
#if defined(CS_MODE)
  int req_error;
  char *ptr;
  int success = ER_FAILED;
  OR_ALIGNED_BUF ((OR_OID_SIZE * 2) + (OR_INT_SIZE * 2)) a_request;
  char *request;
  OR_ALIGNED_BUF (NET_COPY_AREA_SENDRECV_SIZE + OR_OID_SIZE +
		  OR_INT_SIZE) a_reply;
  char *reply;
  CSS_NET_PACKET *recv_packet = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_oid (request, class_oid);
  ptr = or_pack_oid (ptr, oid);
  ptr = or_pack_lock (ptr, lock);
  ptr = or_pack_int (ptr, prefetching);
  *fetch_copyarea = NULL;

  req_error = net_client_request (NET_SERVER_LC_GET_CLASS,
				  &recv_packet, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      int num_objs;
      int packed_desc_size;
      int content_size;

      ptr = or_unpack_int (reply, &num_objs);
      ptr = or_unpack_int (ptr, &packed_desc_size);
      ptr = or_unpack_int (ptr, &content_size);

      ptr = or_unpack_oid (ptr, class_oid);
      ptr = or_unpack_int (ptr, &success);

      if (success == NO_ERROR)
	{
	  char *reply_packed_desc;
	  char *reply_content_ptr;

	  reply_packed_desc = css_net_packet_get_buffer (recv_packet, 1,
							 packed_desc_size,
							 false);
	  reply_content_ptr = css_net_packet_get_buffer (recv_packet, 2,
							 content_size, false);
	  success = make_copyarea_from_server_reply (num_objs,
						     packed_desc_size,
						     content_size,
						     reply_packed_desc,
						     reply_content_ptr,
						     fetch_copyarea);
	}

      css_net_packet_free (recv_packet);
    }
  else
    {
      *fetch_copyarea = NULL;
    }

  return success;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xlocator_get_class (NULL, class_oid, oid, lock, prefetching,
				fetch_copyarea);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}
#endif

/*
 * locator_fetch_all -
 *
 * return:
 *
 *   hfid(in):
 *   lock(in):
 *   class_oidp(in):
 *   nobjects(in):
 *   nfetched(in):
 *   last_oidp(in):
 *   fetch_copyarea(in):
 *
 * NOTE:
 */
int
locator_fetch_all (const HFID * hfid, LOCK * lock, OID * class_oidp,
		   INT64 * nobjects, INT64 * nfetched, OID * last_oidp,
		   LC_COPYAREA ** fetch_copyarea)
{
#if defined(CS_MODE)
  int req_error;
  char *ptr;
  int success = ER_FAILED;
  OR_ALIGNED_BUF (OR_HFID_SIZE + OR_INT_SIZE + (OR_BIGINT_ALIGNED_SIZE * 2) +
		  (OR_OID_SIZE * 2)) a_request;
  char *request;
  OR_ALIGNED_BUF (NET_COPY_AREA_SENDRECV_SIZE + (OR_INT_SIZE * 2) +
		  (OR_BIGINT_ALIGNED_SIZE * 2) + OR_OID_SIZE) a_reply;
  char *reply;
  CSS_NET_PACKET *recv_packet = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_hfid (request, hfid);
  ptr = or_pack_lock (ptr, *lock);
  ptr = or_pack_oid (ptr, class_oidp);

  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_pack_int64 (ptr, *nobjects);
  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_pack_int64 (ptr, *nfetched);

  ptr = or_pack_oid (ptr, last_oidp);
  *fetch_copyarea = NULL;

  req_error = net_client_request (NET_SERVER_LC_FETCHALL,
				  &recv_packet, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (req_error == NO_ERROR)
    {
      int num_objs;
      int packed_desc_size;
      int content_size;

      ptr = or_unpack_int (reply, &num_objs);
      ptr = or_unpack_int (ptr, &packed_desc_size);
      ptr = or_unpack_int (ptr, &content_size);

      ptr = or_unpack_lock (ptr, lock);

      ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
      ptr = or_unpack_int64 (ptr, nobjects);
      ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
      ptr = or_unpack_int64 (ptr, nfetched);

      ptr = or_unpack_oid (ptr, last_oidp);
      ptr = or_unpack_int (ptr, &success);

      if (success == NO_ERROR)
	{
	  char *reply_packed_desc;
	  char *reply_content_ptr;

	  reply_packed_desc = css_net_packet_get_buffer (recv_packet, 1,
							 packed_desc_size,
							 false);
	  reply_content_ptr = css_net_packet_get_buffer (recv_packet, 2,
							 content_size, false);
	  success = make_copyarea_from_server_reply (num_objs,
						     packed_desc_size,
						     content_size,
						     reply_packed_desc,
						     reply_content_ptr,
						     fetch_copyarea);
	}

      css_net_packet_free (recv_packet);
    }
  else
    {
      *fetch_copyarea = NULL;
    }

  return success;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success =
    xlocator_fetch_all (NULL, hfid, lock, class_oidp, nobjects, nfetched,
			last_oidp, fetch_copyarea);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * locator_repl_force - flush copy area containing replication objects
 *                       and receive error occurred in server
 *
 * return:
 *
 *   copy_area(in):
 *   reply_copy_area(out):
 *
 * NOTE:
 */
int
locator_repl_force (UNUSED_ARG LC_COPYAREA * copy_area,
		    UNUSED_ARG LC_COPYAREA ** reply_copy_area)
{
#if defined(CS_MODE)
  int error_code = ER_FAILED;
  char *request;
  char *request_ptr;
  int request_size;
  OR_ALIGNED_BUF (NET_COPY_AREA_SENDRECV_SIZE + OR_INT_SIZE) a_reply;
  char *reply;
  char *desc_ptr = NULL;
  int desc_size;
  char *content_ptr;
  int content_size;
  int num_objs = 0;
  int req_error;
  CSS_NET_PACKET *recv_packet = NULL;

  request_size = NET_COPY_AREA_SENDRECV_SIZE;
  request = (char *) malloc (request_size);

  if (request == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  reply = OR_ALIGNED_BUF_START (a_reply);

  num_objs =
    locator_send_copy_area (copy_area, &content_ptr, &content_size, &desc_ptr,
			    &desc_size);

  request_ptr = or_pack_int (request, num_objs);
  request_ptr = or_pack_int (request_ptr, desc_size);
  request_ptr = or_pack_int (request_ptr, content_size);

  req_error = net_client_request (NET_SERVER_LC_REPL_FORCE,
				  &recv_packet, 3, 1,
				  request, request_size,
				  desc_ptr, desc_size,
				  content_ptr, content_size,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));

  if (req_error == NO_ERROR)
    {
      int num_objs;
      int packed_desc_size;
      int content_size;
      char *ptr;

      ptr = or_unpack_int (reply, &num_objs);
      ptr = or_unpack_int (ptr, &packed_desc_size);
      ptr = or_unpack_int (ptr, &content_size);

      ptr = or_unpack_int (ptr, &error_code);

      if (error_code == NO_ERROR)
	{
	  char *reply_packed_desc;
	  char *reply_content_ptr;

	  reply_packed_desc = css_net_packet_get_buffer (recv_packet, 1,
							 packed_desc_size,
							 false);
	  reply_content_ptr = css_net_packet_get_buffer (recv_packet, 2,
							 content_size, false);
	  error_code = make_copyarea_from_server_reply (num_objs,
							packed_desc_size,
							content_size,
							reply_packed_desc,
							reply_content_ptr,
							reply_copy_area);
	}

      css_net_packet_free (recv_packet);
    }
  else
    {
      assert (er_errid () != NO_ERROR);
      error_code = er_errid ();
    }

  if (desc_ptr)
    {
      free_and_init (desc_ptr);
    }
  if (request)
    {
      free_and_init (request);
    }

  return error_code;
#endif /* CS_MODE */
  return ER_FAILED;
}

/*
 * locator_force -
 *
 * return:
 *
 *   copy_area(in):
 *
 * NOTE:
 */
int
locator_force (LC_COPYAREA * copy_area)
{
#if defined(CS_MODE)
  int error_code = ER_FAILED;
  char *request;
  char *ptr;
  int request_size;
  OR_ALIGNED_BUF (OR_INT_SIZE * 3) a_reply;
  char *reply;
  char *desc_ptr = NULL;
  int desc_size;
  char *content_ptr;
  int content_size;
  int num_objs = 0;
  int req_error;

  request_size = OR_INT_SIZE	/* num_objs */
    + OR_INT_SIZE		/* desc_size */
    + OR_INT_SIZE;		/* content_size */

  request = (char *) malloc (request_size);
  if (request == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  reply = OR_ALIGNED_BUF_START (a_reply);

  num_objs = locator_send_copy_area (copy_area, &content_ptr, &content_size,
				     &desc_ptr, &desc_size);

  ptr = or_pack_int (request, num_objs);
  ptr = or_pack_int (ptr, desc_size);
  ptr = or_pack_int (ptr, content_size);

  req_error = net_client_request (NET_SERVER_LC_FORCE,
				  NULL, 3, 2,
				  request, request_size,
				  desc_ptr, desc_size,
				  content_ptr, content_size,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply),
				  desc_ptr, desc_size);
  if (!req_error)
    {
      (void) or_unpack_int (reply, &error_code);
      if (error_code == NO_ERROR)
	{
	  locator_unpack_copy_area_descriptor (num_objs, copy_area, desc_ptr);
	}
    }
  if (desc_ptr)
    {
      free_and_init (desc_ptr);
    }
  if (request)
    {
      free_and_init (request);
    }

  return error_code;
#else /* CS_MODE */
  int error_code = ER_FAILED;
  LC_COPYAREA *copy_area_clone;

  /* If xlocator_force returns error,
   * the original copy_area should not be changed.
   * So copy_area_clone will be used.
   */
  copy_area_clone = locator_allocate_copy_area_by_length (copy_area->length);
  if (copy_area_clone == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  memcpy (copy_area_clone->mem, copy_area->mem, copy_area->length);

  ENTER_SERVER ();

  error_code = xlocator_force (NULL, copy_area);

  EXIT_SERVER ();

  if (error_code != NO_ERROR)
    {
      /* Restore copy_area */
      memcpy (copy_area->mem, copy_area_clone->mem, copy_area->length);
    }

  locator_free_copy_area (copy_area_clone);

  return error_code;
#endif /* !CS_MODE */
}

/*
 * locator_fetch_lockset -
 *
 * return:
 *
 *   lockset(in):
 *   fetch_copyarea(in):
 *
 * NOTE:
 */
int
locator_fetch_lockset (LC_LOCKSET * lockset, LC_COPYAREA ** fetch_copyarea)
{
#if defined(CS_MODE)
  int success = ER_FAILED;
  int req_error;
  char *ptr;
  OR_ALIGNED_BUF (NET_SENDRECV_BUFFSIZE + NET_COPY_AREA_SENDRECV_SIZE +
		  OR_INT_SIZE) a_reply;
  char *reply;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  static unsigned int eid;
  char *packed = NULL;
  int packed_size;
  int send_size;
  CSS_NET_PACKET *recv_packet = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = NO_ERROR;

  if (lockset->first_fetch_lockset_call == true)
    {
      send_size = locator_pack_lockset (lockset, true, true);

      packed = lockset->packed;
      packed_size = lockset->packed_size;

      if (!packed)
	{
	  return ER_FAILED;
	}

      ptr = or_pack_int (request, send_size);

      req_error = net_client_request_send_msg (&eid,
					       NET_SERVER_LC_FETCH_LOCKSET,
					       2,
					       request,
					       OR_ALIGNED_BUF_SIZE
					       (a_request), packed,
					       send_size);
    }

  if (req_error == NO_ERROR)
    {
      /* Don't need to send the lockset information any more
         if first_fetch_lockset_call is false */
      packed = lockset->packed;
      packed_size = lockset->packed_size;

      req_error = net_client_request_recv_msg (&recv_packet, eid, -1, 2,
					       reply,
					       OR_ALIGNED_BUF_SIZE (a_reply),
					       packed, packed_size);
    }

  if (req_error == NO_ERROR)
    {
      int num_objs;
      int packed_desc_size;
      int content_size;
      int recv_packed_size;

      ptr = or_unpack_int (reply, &recv_packed_size);
      ptr = or_unpack_int (ptr, &num_objs);
      ptr = or_unpack_int (ptr, &packed_desc_size);
      ptr = or_unpack_int (ptr, &content_size);

      ptr = or_unpack_int (ptr, &success);

      if (packed_size < recv_packed_size)
	{
	  success = ER_NET_CANT_ALLOC_BUFFER;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, success, 0);
	}

      if (success == NO_ERROR)
	{
	  char *reply_packed_desc;
	  char *reply_content_ptr;

	  reply_packed_desc = css_net_packet_get_buffer (recv_packet, 2,
							 packed_desc_size,
							 false);
	  reply_content_ptr = css_net_packet_get_buffer (recv_packet, 3,
							 content_size, false);
	  success = make_copyarea_from_server_reply (num_objs,
						     packed_desc_size,
						     content_size,
						     reply_packed_desc,
						     reply_content_ptr,
						     fetch_copyarea);

	  locator_unpack_lockset (lockset, lockset->first_fetch_lockset_call,
				  false);
	}

      css_net_packet_free (recv_packet);
    }

  if (success != NO_ERROR)
    {
      *fetch_copyarea = NULL;
    }

  /*
   * We will not need to send the lockset structure any more. We do not
   * need to receive the classes and objects in the lockset structure
   * any longer
   */
  lockset->first_fetch_lockset_call = false;

  return success;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xlocator_fetch_lockset (NULL, lockset, fetch_copyarea);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * locator_find_class_oid -
 *
 * return:
 *
 *   class_name(in):
 *   class_oid(in):
 *   lock(in):
 *
 * NOTE:
 */
LC_FIND_CLASSNAME
locator_find_class_oid (const char *class_name, OID * class_oid, LOCK lock)
{
#if defined(CS_MODE)
  LC_FIND_CLASSNAME found = LC_CLASSNAME_ERROR;
  int xfound;
  int req_error;
  char *ptr;
  int request_size, strlen;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_OID_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = length_const_string (class_name, &strlen)
    + OR_OID_SIZE + OR_INT_SIZE;
  request = (char *) malloc (request_size);
  if (request)
    {
      ptr = pack_const_string_with_length (request, class_name, strlen);
      ptr = or_pack_oid (ptr, class_oid);
      ptr = or_pack_lock (ptr, lock);

      req_error = net_client_request (NET_SERVER_LC_FIND_CLASSOID,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  ptr = or_unpack_int (reply, &xfound);
	  found = (LC_FIND_CLASSNAME) xfound;
	  ptr = or_unpack_oid (ptr, class_oid);
	}
      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
    }

  return found;
#else /* CS_MODE */
  LC_FIND_CLASSNAME found = LC_CLASSNAME_ERROR;

  ENTER_SERVER ();

  found = xlocator_find_class_oid (NULL, class_name, class_oid, lock);

  EXIT_SERVER ();

  return found;
#endif /* !CS_MODE */
}

/*
 * locator_reserve_class_names -
 *
 * return:
 *
 *   num_classes(in)
 *   class_names(in):
 *   class_oids(in):
 *
 * NOTE:
 */
LC_FIND_CLASSNAME
locator_reserve_class_names (const int num_classes, const char **class_names,
			     OID * class_oids)
{
#if defined(CS_MODE)
  LC_FIND_CLASSNAME reserved = LC_CLASSNAME_ERROR;
  int xreserved;
  int request_size;
  int req_error;
  char *request, *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;
  int i;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = OR_INT_SIZE;
  for (i = 0; i < num_classes; ++i)
    {
      request_size +=
	length_const_string (class_names[i], NULL) + OR_OID_SIZE;
    }
  request = (char *) malloc (request_size);
  if (request)
    {
      ptr = or_pack_int (request, num_classes);
      for (i = 0; i < num_classes; ++i)
	{
	  ptr = pack_const_string (ptr, class_names[i]);
	  ptr = or_pack_oid (ptr, &class_oids[i]);
	}

      req_error = net_client_request (NET_SERVER_LC_RESERVE_CLASSNAME,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  (void) or_unpack_int (reply, &xreserved);
	  reserved = (LC_FIND_CLASSNAME) xreserved;
	}

      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
    }

  return reserved;
#else /* CS_MODE */
  LC_FIND_CLASSNAME reserved = LC_CLASSNAME_ERROR;

  ENTER_SERVER ();

  reserved = xlocator_reserve_class_names (NULL, num_classes, class_names,
					   class_oids);

  EXIT_SERVER ();

  return reserved;
#endif /* !CS_MODE */
}

/*
 * locator_delete_class_name -
 *
 * return:
 *
 *   class_name(in):
 *
 * NOTE:
 */
LC_FIND_CLASSNAME
locator_delete_class_name (const char *class_name)
{
#if defined(CS_MODE)
  LC_FIND_CLASSNAME deleted = LC_CLASSNAME_ERROR;
  int xdeleted;
  int req_error, request_size, strlen;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = length_const_string (class_name, &strlen);
  request = (char *) malloc (request_size);
  if (request)
    {
      (void) pack_const_string_with_length (request, class_name, strlen);
      req_error = net_client_request (NET_SERVER_LC_DELETE_CLASSNAME,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  or_unpack_int (reply, &xdeleted);
	  deleted = (LC_FIND_CLASSNAME) xdeleted;
	}

      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
    }

  return deleted;
#else /* CS_MODE */
  LC_FIND_CLASSNAME deleted = LC_CLASSNAME_ERROR;

  ENTER_SERVER ();

  deleted = xlocator_delete_class_name (NULL, class_name);

  EXIT_SERVER ();

  return deleted;
#endif /* !CS_MODE */
}

/*
 * locator_rename_class_name -
 *
 * return:
 *
 *   old_name(in):
 *   new_name(in):
 *   class_oid(in):
 *
 * NOTE:
 */
LC_FIND_CLASSNAME
locator_rename_class_name (const char *old_name, const char *new_name,
			   OID * class_oid)
{
#if defined(CS_MODE)
  LC_FIND_CLASSNAME renamed = LC_CLASSNAME_ERROR;
  int xrenamed;
  int request_size, strlen1, strlen2;
  int req_error;
  char *request, *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = length_const_string (old_name, &strlen1)
    + length_const_string (new_name, &strlen2) + OR_OID_SIZE;
  request = (char *) malloc (request_size);
  if (request)
    {
      ptr = pack_const_string_with_length (request, old_name, strlen1);
      ptr = pack_const_string_with_length (ptr, new_name, strlen2);
      ptr = or_pack_oid (ptr, class_oid);

      req_error = net_client_request (NET_SERVER_LC_RENAME_CLASSNAME,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  ptr = or_unpack_int (reply, &xrenamed);
	  renamed = (LC_FIND_CLASSNAME) xrenamed;
	}
      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
    }

  return renamed;
#else /* CS_MODE */
  LC_FIND_CLASSNAME renamed = LC_CLASSNAME_ERROR;

  ENTER_SERVER ();

  renamed = xlocator_rename_class_name (NULL, old_name, new_name, class_oid);

  EXIT_SERVER ();

  return renamed;
#endif /* !CS_MODE */
}

/*
 * locator_assign_oid -
 *
 * return:
 *
 *   hfid(in):
 *   perm_oid(in):
 *   expected_length(in):
 *   class_oid(in):
 *   class_name(in):
 *
 * NOTE:
 */
int
locator_assign_oid (const HFID * hfid, OID * perm_oid, int expected_length,
		    OID * class_oid, const char *class_name)
{
#if defined(CS_MODE)
  int success = ER_FAILED;
  int request_size, strlen;
  int req_error;
  char *request, *ptr;
  OR_ALIGNED_BUF (OR_OID_SIZE + OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = OR_HFID_SIZE + OR_INT_SIZE + OR_OID_SIZE
    + length_const_string (class_name, &strlen);
  request = (char *) malloc (request_size);
  if (request)
    {
      ptr = or_pack_hfid (request, hfid);
      ptr = or_pack_int (ptr, expected_length);
      ptr = or_pack_oid (ptr, class_oid);
      ptr = pack_const_string_with_length (ptr, class_name, strlen);

      req_error = net_client_request (NET_SERVER_LC_ASSIGN_OID,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  ptr = or_unpack_int (reply, &success);
	  ptr = or_unpack_oid (ptr, perm_oid);
	}
      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
    }

  return success;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xlocator_assign_oid (NULL, hfid, perm_oid, expected_length,
				 class_oid, class_name);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * locator_assign_oid_batch -
 *
 * return:
 *
 *   oidset(in):
 *
 * NOTE:
 */
int
locator_assign_oid_batch (LC_OIDSET * oidset)
{
#if defined(CS_MODE)
  int success = ER_FAILED;
  int packed_size;
  char *buffer, *ptr;
  int req_error;

  /*
   * Build a buffer in which to send and receive the goobers.  We'll
   * reuse the same buffer to receive the data as we used to send it.
   * First word is reserved for the return code.
   */
  packed_size = locator_get_packed_oid_set_size (oidset) + OR_INT_SIZE;
  buffer = (char *) malloc (packed_size);
  if (buffer == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      packed_size);
      return ER_FAILED;
    }

  ptr = buffer;
  ptr = or_pack_int (ptr, 0);

  if (locator_pack_oid_set (ptr, oidset) == NULL)
    {
      free_and_init (buffer);
      return ER_FAILED;
    }

  req_error = net_client_request (NET_SERVER_LC_ASSIGN_OID_BATCH,
				  NULL, 1, 1,
				  buffer, packed_size, buffer, packed_size);

  if (!req_error)
    {
      ptr = buffer;
      ptr = or_unpack_int (ptr, &success);
      if (success == NO_ERROR)
	{
	  if (locator_unpack_oid_set_to_exist (ptr, oidset) == false)
	    {
	      success = ER_FAILED;
	    }
	}
    }

  free_and_init (buffer);

  return success;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xlocator_assign_oid_batch (NULL, oidset);

  EXIT_SERVER ();

  return (success);
#endif /* !CS_MODE */
}
#endif

/*
 * locator_find_lockhint_class_oids -
 *
 * return:
 *
 *   num_classes(in):
 *   many_classnames(in):
 *   many_locks(in):
 *   guessed_class_oids(in):
 *   quit_on_errors(in):
 *   lockhint(in):
 *   fetch_copyarea(in):
 *
 * NOTE:
 */
LC_FIND_CLASSNAME
locator_find_lockhint_class_oids (int num_classes,
				  const char **many_classnames,
				  LOCK * many_locks,
				  OID * guessed_class_oids,
				  int quit_on_errors,
				  LC_LOCKHINT ** lockhint,
				  LC_COPYAREA ** fetch_copyarea)
{
#if defined(CS_MODE)
  LC_FIND_CLASSNAME allfind = LC_CLASSNAME_ERROR;
  int xallfind;
  int req_error;
  char *ptr;
  int request_size;
  char *request;
  OR_ALIGNED_BUF (NET_SENDRECV_BUFFSIZE + NET_COPY_AREA_SENDRECV_SIZE +
		  OR_INT_SIZE) a_reply;
  char *reply;
  int i;
  CSS_NET_PACKET *recv_packet = NULL;

  reply = OR_ALIGNED_BUF_START (a_reply);

  *lockhint = NULL;
  *fetch_copyarea = NULL;

  request_size = OR_INT_SIZE + OR_INT_SIZE;
  for (i = 0; i < num_classes; i++)
    {
      request_size += (length_const_string (many_classnames[i], NULL) +
		       OR_INT_SIZE + OR_OID_SIZE);
    }

  request = (char *) malloc (request_size);
  if (request == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
      return allfind;
    }

  ptr = or_pack_int (request, num_classes);
  ptr = or_pack_int (ptr, quit_on_errors);
  for (i = 0; i < num_classes; i++)
    {
      ptr = pack_const_string (ptr, many_classnames[i]);
      ptr = or_pack_lock (ptr, many_locks[i]);
      ptr = or_pack_oid (ptr, &guessed_class_oids[i]);
    }

  req_error = net_client_request (NET_SERVER_LC_FIND_LOCKHINT_CLASSOIDS,
				  &recv_packet, 1, 1,
				  request, request_size,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));

  if (!req_error)
    {
      int num_objs;
      int packed_desc_size;
      int content_size;
      int packed_size;
      int error;
      char *packed;
      char *reply_packed_desc;
      char *reply_content_ptr;

      ptr = or_unpack_int (reply, &packed_size);
      ptr = or_unpack_int (ptr, &num_objs);
      ptr = or_unpack_int (ptr, &packed_desc_size);
      ptr = or_unpack_int (ptr, &content_size);
      ptr = or_unpack_int (ptr, &xallfind);

      packed = css_net_packet_get_buffer (recv_packet, 1, packed_size, false);
      reply_packed_desc = css_net_packet_get_buffer (recv_packet, 2,
						     packed_desc_size, false);
      reply_content_ptr = css_net_packet_get_buffer (recv_packet, 3,
						     content_size, false);


      allfind = (LC_FIND_CLASSNAME) xallfind;

      if (packed_size > 0 && packed != NULL)
	{
	  *lockhint =
	    locator_allocate_and_unpack_lockhint (packed, packed_size, true,
						  false);
	}
      else
	{
	  *lockhint = NULL;
	}

      error = make_copyarea_from_server_reply (num_objs,
					       packed_desc_size,
					       content_size,
					       reply_packed_desc,
					       reply_content_ptr,
					       fetch_copyarea);
      if (error != NO_ERROR)
	{
	  allfind = LC_CLASSNAME_ERROR;
	}

      css_net_packet_free (recv_packet);
    }
  else
    {
      *lockhint = NULL;
      *fetch_copyarea = NULL;
    }

  if (request != NULL)
    {
      free_and_init (request);
    }

  return allfind;
#else /* CS_MODE */
  LC_FIND_CLASSNAME allfind = LC_CLASSNAME_ERROR;

  ENTER_SERVER ();

  allfind =
    xlocator_find_lockhint_class_oids (NULL, num_classes, many_classnames,
				       many_locks, guessed_class_oids,
				       quit_on_errors, lockhint,
				       fetch_copyarea);

  EXIT_SERVER ();

  return allfind;
#endif /* !CS_MODE */
}

/*
 * locator_fetch_lockhint_classes -
 *
 * return:
 *
 *   lockhint(in):
 *   fetch_copyarea(in):
 *
 * NOTE:
 */
int
locator_fetch_lockhint_classes (LC_LOCKHINT * lockhint,
				LC_COPYAREA ** fetch_copyarea)
{
#if defined(CS_MODE)
  static unsigned int eid;	/* TODO: remove static */
  int success = ER_FAILED;
  int req_error;
  char *ptr;
  OR_ALIGNED_BUF (NET_SENDRECV_BUFFSIZE + NET_COPY_AREA_SENDRECV_SIZE +
		  OR_INT_SIZE) a_reply;
  char *reply;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  char *packed = NULL;
  int packed_size;
  int send_size;
  CSS_NET_PACKET *recv_packet = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  *fetch_copyarea = NULL;

  req_error = NO_ERROR;

  if (lockhint->first_fetch_lockhint_call == true)
    {
      send_size = locator_pack_lockhint (lockhint, true);

      packed = lockhint->packed;
      packed_size = lockhint->packed_size;

      if (!packed)
	{
	  return ER_FAILED;
	}

      ptr = or_pack_int (request, send_size);

      req_error =
	net_client_request_send_msg (&eid,
				     NET_SERVER_LC_FETCH_LOCKHINT_CLASSES,
				     2,
				     request, OR_ALIGNED_BUF_SIZE (a_request),
				     packed, send_size);
    }

  if (req_error == NO_ERROR)
    {
      /* Don't need to send the lockhint information any more */
      packed = lockhint->packed;
      packed_size = lockhint->packed_size;
      req_error = net_client_request_recv_msg (&recv_packet, eid, -1, 2,
					       reply,
					       OR_ALIGNED_BUF_SIZE (a_reply),
					       packed, packed_size);
    }

  if (req_error == NO_ERROR)
    {
      int num_objs;
      int packed_desc_size;
      int content_size;
      int recv_packed_size;

      ptr = or_unpack_int (reply, &recv_packed_size);
      ptr = or_unpack_int (ptr, &num_objs);
      ptr = or_unpack_int (ptr, &packed_desc_size);
      ptr = or_unpack_int (ptr, &content_size);

      ptr = or_unpack_int (ptr, &success);

      if (packed_size < recv_packed_size)
	{
	  success = ER_NET_CANT_ALLOC_BUFFER;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, success, 0);
	}

      if (success == NO_ERROR)
	{
	  char *reply_packed_desc;
	  char *reply_content_ptr;

	  reply_packed_desc = css_net_packet_get_buffer (recv_packet, 2,
							 packed_desc_size,
							 false);
	  reply_content_ptr = css_net_packet_get_buffer (recv_packet, 3,
							 content_size, false);
	  success = make_copyarea_from_server_reply (num_objs,
						     packed_desc_size,
						     content_size,
						     reply_packed_desc,
						     reply_content_ptr,
						     fetch_copyarea);

	  locator_unpack_lockhint (lockhint,
				   lockhint->first_fetch_lockhint_call);
	}
      css_net_packet_free (recv_packet);
    }

  if (success != NO_ERROR)
    {
      *fetch_copyarea = NULL;
    }

  lockhint->first_fetch_lockhint_call = false;

  return success;

#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xlocator_fetch_lockhint_classes (NULL, lockhint, fetch_copyarea);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * heap_create -
 *
 * return:
 *
 *   hfid(in):
 *   class_oid(in):
 *
 * NOTE:
 */
int
heap_create (HFID * hfid, const OID * class_oid)
{
#if defined(CS_MODE)
  int error = ER_NET_CLIENT_DATA_RECEIVE;
  int req_error;
  char *ptr;
  OR_ALIGNED_BUF (OR_HFID_SIZE + OR_OID_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_HFID_SIZE) a_reply;
  char *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_hfid (request, hfid);
  ptr = or_pack_oid (ptr, class_oid);
  req_error = net_client_request (NET_SERVER_HEAP_CREATE,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      ptr = or_unpack_errcode (reply, &error);
      ptr = or_unpack_hfid (ptr, hfid);
    }

  return error;
#else /* CS_MODE */
  int success;

  ENTER_SERVER ();

  success = xheap_create (NULL, hfid, class_oid);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * heap_destroy -
 *
 * return:
 *
 *   hfid(in):
 *
 * NOTE:
 */
int
heap_destroy (const HFID * hfid)
{
#if defined(CS_MODE)
  int error = ER_NET_CLIENT_DATA_RECEIVE;
  int req_error;
  char *ptr;
  OR_ALIGNED_BUF (OR_HFID_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_pack_hfid (request, hfid);

  req_error = net_client_request (NET_SERVER_HEAP_DESTROY,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      ptr = or_unpack_errcode (reply, &error);
    }

  return error;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xheap_destroy (NULL, hfid);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * disk_get_purpose_and_space_info -
 *
 * return:
 *
 *   volid(in):
 *   vol_purpose(in):
 *   space_info(out):
 *
 * NOTE:
 */
VOLID
disk_get_purpose_and_space_info (VOLID volid,
				 DISK_VOLPURPOSE * vol_purpose,
				 VOL_SPACE_INFO * space_info)
{
#if defined(CS_MODE)
  int temp;
  int req_error;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2 + OR_VOL_SPACE_INFO_SIZE) a_reply;
  char *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_pack_int (request, (int) volid);

  req_error = net_client_request (NET_SERVER_DISK_GET_PURPOSE_AND_SPACE_INFO,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      ptr = or_unpack_int (reply, &temp);
      *vol_purpose = (DB_VOLPURPOSE) temp;
      OR_UNPACK_VOL_SPACE_INFO (ptr, space_info);
      ptr = or_unpack_int (ptr, &temp);
      volid = temp;
    }
  else
    {
      volid = NULL_VOLID;
    }

  return volid;
#else /* CS_MODE */

  ENTER_SERVER ();

  volid = xdisk_get_purpose_and_space_info (NULL, volid, vol_purpose,
					    space_info);

  EXIT_SERVER ();

  return volid;
#endif /* !CS_MODE */
}

/*
 * disk_get_fullname -
 *
 * return:
 *
 *   database_name(in):
 *   volid(in):
 *   vol_fullname(in):
 *
 * NOTE:
 */
char *
disk_get_fullname (const char *database_name, VOLID volid, char *vol_fullname)
{
#if defined(CS_MODE)
  int req_error;
  char *area = NULL;
  int area_size;
  char *ptr;
  int request_size, strlen;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;
  const char *name = NULL;
  char *ret_ptr = NULL;
  CSS_NET_PACKET *recv_packet = NULL;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = length_const_string (database_name, &strlen) + OR_INT_SIZE;

  request = (char *) malloc (request_size);
  if (request)
    {
      ptr = pack_const_string_with_length (request, database_name, strlen);
      ptr = or_pack_int (ptr, (int) volid);

      req_error = net_client_request (NET_SERVER_DISK_VLABEL,
				      &recv_packet, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  or_unpack_int (reply, &area_size);

	  area = css_net_packet_get_buffer (recv_packet, 1, area_size, false);
	  if (area != NULL)
	    {
	      or_unpack_string_nocopy (area, &name);
	      if (name != NULL)
		{
		  strcpy (vol_fullname, name);
		  ret_ptr = vol_fullname;
		}
	    }
	  css_net_packet_free (recv_packet);
	}

      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
    }

  return ret_ptr;
#else /* CS_MODE */

  ENTER_SERVER ();

  vol_fullname =
    xdisk_get_fullname (NULL, database_name, volid, vol_fullname);

  EXIT_SERVER ();

  return vol_fullname;
#endif /* !CS_MODE */
}

/*
 * log_reset_wait_msecs -
 *
 * return:
 *
 *   wait_msecs(in):    in milliseconds
 *
 * NOTE:
 */
int
log_reset_wait_msecs (int wait_msecs)
{
#if defined(CS_MODE)
  int wait = -1;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_pack_int (request, wait_msecs);

  req_error = net_client_request (NET_SERVER_LOG_RESET_WAIT_MSECS,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      (void) or_unpack_int (reply, &wait);
    }

  return wait;
#else /* CS_MODE */
  int wait = -1;

  ENTER_SERVER ();

  wait = xlogtb_reset_wait_msecs (NULL, wait_msecs);

  EXIT_SERVER ();

  return wait;
#endif /* !CS_MODE */
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * log_reset_isolation -
 *
 * return:
 *
 *   isolation(in):
 *
 * NOTE:
 */
int
log_reset_isolation (TRAN_ISOLATION isolation)
{
#if defined(CS_MODE)
  int req_error, error_code = ER_NET_CLIENT_DATA_RECEIVE;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request, *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_int (request, (int) isolation);

  req_error = net_client_request (NET_SERVER_LOG_RESET_ISOLATION,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &error_code);
    }

  return error_code;
#else /* CS_MODE */
  int error_code = NO_ERROR;

  ENTER_SERVER ();

  error_code = xlogtb_reset_isolation (NULL, isolation);

  EXIT_SERVER ();

  return error_code;
#endif /* !CS_MODE */
}
#endif

/*
 * log_set_interrupt -
 *
 * return:
 *
 *   set(in):
 *
 * NOTE:
 */
void
log_set_interrupt (UNUSED_ARG int set)
{
#if defined(CS_MODE)
#if 0
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;

  request = OR_ALIGNED_BUF_START (a_request);
  or_pack_int (request, set);

  (void) net_client_request (NET_SERVER_LOG_SET_INTERRUPT,
			     NULL, 1, -1,
			     request, OR_ALIGNED_BUF_SIZE (a_request));
#endif
#else /* CS_MODE */

  ENTER_SERVER ();

  xlogtb_set_interrupt (NULL, set);

  EXIT_SERVER ();
#endif /* !CS_MODE */
}

/*
 * log_checkpoint -
 *
 * return:
 *
 * NOTE:
 */
void
log_checkpoint (void)
{
#if defined(CS_MODE)
  int req_error, chk_int;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_LOG_CHECKPOINT,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));

  if (req_error == NO_ERROR)
    {
      ptr = or_unpack_int (reply, &chk_int);
      assert (chk_int == NO_ERROR);
    }
#else /* CS_MODE */
  /* Cannot run in standalone mode */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NOT_IN_STANDALONE, 1,
	  "checkpoint");
#endif /* !CS_MODE */
}

/*
 * log_dump_stat -
 *
 * return:
 *
 *   outfp(in):
 *
 * NOTE:
 */
void
log_dump_stat (FILE * outfp)
{
#if defined(CS_MODE)
  int req_error;

  if (outfp == NULL)
    {
      outfp = stdout;
    }

  req_error = net_client_request_recv_stream (NET_SERVER_LOG_DUMP_STAT,
					      NULL, 0, NULL, 0,
					      NULL, 0, outfp);
#else /* CS_MODE */

  ENTER_SERVER ();

  xlogpb_dump_stat (outfp);

  EXIT_SERVER ();
#endif /* !CS_MODE */
}

/*
 * log_set_suppress_repl_on_transaction -
 *
 * return:
 *
 *   set(in):
 *
 * NOTE:
 */
int
log_set_suppress_repl_on_transaction (int set)
{
#if defined(CS_MODE)
  int req_error = NO_ERROR;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *request, *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  or_pack_int (request, set);

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error =
    net_client_request (NET_SERVER_LOG_SET_SUPPRESS_REPL_ON_TRANSACTION,
			NULL, 1, 1,
			request, OR_ALIGNED_BUF_SIZE (a_request),
			reply, OR_ALIGNED_BUF_SIZE (a_reply));

  if (req_error == NO_ERROR)
    {
      or_unpack_int (reply, &req_error);
    }

  return req_error;
#else /* CS_MODE */
  ENTER_SERVER ();

  xlogtb_set_suppress_repl_on_transaction (NULL, set);

  EXIT_SERVER ();

  return NO_ERROR;
#endif /* !CS_MODE */
}

/*
 * tran_server_commit -
 *
 * return:
 *
 * NOTE:
 */
TRAN_STATE
tran_server_commit ()
{
#if defined(CS_MODE)
  TRAN_STATE tran_state = TRAN_UNACTIVE_UNKNOWN;
  int req_error, tran_state_int;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  if (net_client_is_server_in_transaction () == false)
    {
      /* already committed */
      if (net_client_reset_on_commit () && log_does_allow_replication ())
	{
	  db_Connect_status = DB_CONNECTION_STATUS_RESET;
	}
      return TRAN_UNACTIVE_COMMITTED;
    }

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_TM_SERVER_COMMIT,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      ptr = or_unpack_int (reply, &tran_state_int);
      tran_state = (TRAN_STATE) tran_state_int;

      if (net_client_reset_on_commit () && log_does_allow_replication ())
	{
	  /*
	   * fail-back action
	   * make the client to reconnect to the active server
	   */
	  db_Connect_status = DB_CONNECTION_STATUS_RESET;
	  er_log_debug (ARG_FILE_LINE,
			"tran_server_commit: DB_CONNECTION_STATUS_RESET\n");
	}
    }

  return tran_state;
#else /* CS_MODE */
  TRAN_STATE tran_state = TRAN_UNACTIVE_UNKNOWN;

  ENTER_SERVER ();

  tran_state = xtran_server_commit (NULL);

  EXIT_SERVER ();

  return tran_state;
#endif /* !CS_MODE */
}

/*
 * tran_server_abort -
 *
 * return:
 *
 * NOTE:
 */
TRAN_STATE
tran_server_abort (void)
{
#if defined(CS_MODE)
  TRAN_STATE tran_state = TRAN_UNACTIVE_UNKNOWN;
  int req_error, tran_state_int;
  char *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply;

  if (net_client_is_server_in_transaction () == false)
    {
      /* already committed */
      if (net_client_reset_on_commit () && log_does_allow_replication ())
	{
	  db_Connect_status = DB_CONNECTION_STATUS_RESET;
	}
      return TRAN_UNACTIVE_ABORTED;
    }

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_TM_SERVER_ABORT,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      ptr = or_unpack_int (reply, &tran_state_int);
      tran_state = (TRAN_STATE) tran_state_int;

      if (net_client_reset_on_commit () && log_does_allow_replication ())
	{
	  /*
	   * fail-back action
	   * make the client to reconnect to the active server
	   */
	  db_Connect_status = DB_CONNECTION_STATUS_RESET;
	  er_log_debug (ARG_FILE_LINE,
			"tran_server_abort: DB_CONNECTION_STATUS_RESET\n");
	}
    }

  return tran_state;
#else /* CS_MODE */
  TRAN_STATE tran_state = TRAN_UNACTIVE_UNKNOWN;

  ENTER_SERVER ();

  tran_state = xtran_server_abort (NULL);

  EXIT_SERVER ();

  return tran_state;
#endif /* !CS_MODE */
}

const char *
tran_get_tranlist_state_name (TRAN_STATE state)
{
  switch (state)
    {
    case TRAN_RECOVERY:
      return "(RECOVERY)";
    case TRAN_ACTIVE:
      return "(ACTIVE)";
    case TRAN_UNACTIVE_COMMITTED:
      return "(COMMITTED)";
    case TRAN_UNACTIVE_WILL_COMMIT:
      return "(COMMITTING)";
    case TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE:
      return "(COMMITTED1)";
    case TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE:
      return "(COMMITTED3)";
    case TRAN_UNACTIVE_ABORTED:
      return "(ABORTED)";
    case TRAN_UNACTIVE_UNILATERALLY_ABORTED:
      return "(KILLED)";
    case TRAN_UNACTIVE_UNKNOWN:
    default:
      return "(UNKNOWN)";
    }

  return "(UNKNOWN)";
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * tran_is_blocked -
 *
 * return:
 *
 *   tran_index(in):
 *
 * NOTE:
 */
bool
tran_is_blocked (int tran_index)
{
#if defined(CS_MODE)
  bool blocked = false;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;
  int temp;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_pack_int (request, tran_index);

  req_error = net_client_request (NET_SERVER_TM_ISBLOCKED,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      (void) or_unpack_int (reply, &temp);
      blocked = (temp == 1) ? true : false;
    }

  return blocked;
#else /* CS_MODE */
  bool blocked = false;

  ENTER_SERVER ();

  blocked = xtran_is_blocked (NULL, tran_index);

  EXIT_SERVER ();

  return blocked;
#endif /* !CS_MODE */
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * tran_server_has_updated -
 *
 * return:
 *
 * NOTE:
 */
int
tran_server_has_updated (void)
{
#if defined(CS_MODE)
  int has_updated = 0;		/* TODO */
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_TM_SERVER_HAS_UPDATED,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &has_updated);
    }

  return has_updated;
#else /* CS_MODE */
  int has_updated = 0;		/* TODO */

  ENTER_SERVER ();

  has_updated = xtran_server_has_updated (NULL);

  EXIT_SERVER ();

  return has_updated;
#endif /* !CS_MODE */
}

/*
 * tran_server_is_active_and_has_updated -
 *
 * return:
 *
 * NOTE:
 */
int
tran_server_is_active_and_has_updated (void)
{
#if defined(CS_MODE)
  int isactive_and_has_updated = 0;	/* TODO */
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error =
    net_client_request (NET_SERVER_TM_SERVER_ISACTIVE_AND_HAS_UPDATED,
			NULL, 0, 1, reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &isactive_and_has_updated);
    }

  return isactive_and_has_updated;
#else /* CS_MODE */
  int isactive_and_has_updated = 0;	/* TODO */

  ENTER_SERVER ();

  isactive_and_has_updated = xtran_server_is_active_and_has_updated (NULL);

  EXIT_SERVER ();

  return (isactive_and_has_updated);
#endif /* !CS_MODE */
}

/*
 * tran_wait_server_active_trans -
 *
 * return:
 *
 * NOTE:
 */
int
tran_wait_server_active_trans (void)
{
#if defined(CS_MODE)
  int status = 0;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_TM_WAIT_SERVER_ACTIVE_TRANS,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &status);
    }

  return status;
#else /* CS_MODE */
  return 0;
#endif /* !CS_MODE */
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * tran_server_start_topop -
 *
 * return:
 *
 *   topop_lsa(in):
 *
 * NOTE:
 */
int
tran_server_start_topop (LOG_LSA * topop_lsa)
{
#if defined(CS_MODE)
  int success = ER_FAILED;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_LOG_LSA_ALIGNED_SIZE) a_reply;
  char *reply;
  char *ptr;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_TM_SERVER_START_TOPOP,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      ptr = or_unpack_int (reply, &success);
      ptr = or_unpack_log_lsa (ptr, topop_lsa);
    }
  return success;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  if (xtran_server_start_topop (NULL, topop_lsa) != NO_ERROR)
    {
      success = ER_FAILED;
    }
  else
    {
      success = NO_ERROR;
    }

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * tran_server_end_topop -
 *
 * return:
 *
 *   result(in):
 *   topop_lsa(in):
 *
 * NOTE:
 */
TRAN_STATE
tran_server_end_topop (LOG_RESULT_TOPOP result, LOG_LSA * topop_lsa)
{
#if defined(CS_MODE)
  TRAN_STATE tran_state = TRAN_UNACTIVE_UNKNOWN;
  int req_error, tran_state_int;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_LOG_LSA_ALIGNED_SIZE) a_reply;
  char *reply;
  char *ptr;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_int (request, (int) result);
  req_error = net_client_request (NET_SERVER_TM_SERVER_END_TOPOP,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      ptr = or_unpack_int (reply, &tran_state_int);
      tran_state = (TRAN_STATE) tran_state_int;
      ptr = or_unpack_log_lsa (ptr, topop_lsa);
    }
  return tran_state;
#else /* CS_MODE */
  TRAN_STATE tran_state = TRAN_UNACTIVE_UNKNOWN;

  ENTER_SERVER ();

  tran_state = xtran_server_end_topop (NULL, result, topop_lsa);

  EXIT_SERVER ();

  return (tran_state);
#endif /* !CS_MODE */
}
#endif

/*
 * tran_server_savepoint -
 *
 * return:
 *
 *   savept_name(in):
 *   savept_lsa(in):
 *
 * NOTE:
 */
int
tran_server_savepoint (const char *savept_name, LOG_LSA * savept_lsa)
{
#if defined(CS_MODE)
  int success = ER_FAILED;
  int req_error, request_size, strlen;
  char *request, *reply;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_LOG_LSA_ALIGNED_SIZE) a_reply;
  char *ptr;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = length_const_string (savept_name, &strlen);
  request = (char *) malloc (request_size);
  if (request)
    {
      ptr = pack_const_string_with_length (request, savept_name, strlen);
      req_error = net_client_request (NET_SERVER_TM_SERVER_SAVEPOINT,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  ptr = or_unpack_int (reply, &success);
	  ptr = or_unpack_log_lsa (ptr, savept_lsa);
	}
      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
    }

  return success;

#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xtran_server_savepoint (NULL, savept_name, savept_lsa);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * tran_server_partial_abort -
 *
 * return:
 *
 *   savept_name(in):
 *   savept_lsa(in):
 *
 * NOTE:
 */
TRAN_STATE
tran_server_partial_abort (const char *savept_name, LOG_LSA * savept_lsa)
{
#if defined(CS_MODE)
  TRAN_STATE tran_state = TRAN_UNACTIVE_UNKNOWN;
  int req_error, tran_state_int, request_size, strlen;
  char *request, *reply;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_LOG_LSA_ALIGNED_SIZE) a_reply;
  char *ptr;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = length_const_string (savept_name, &strlen);
  request = (char *) malloc (request_size);
  if (request)
    {
      ptr = pack_const_string_with_length (request, savept_name, strlen);
      req_error = net_client_request (NET_SERVER_TM_SERVER_PARTIAL_ABORT,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  ptr = or_unpack_int (reply, &tran_state_int);
	  tran_state = (TRAN_STATE) tran_state_int;
	  ptr = or_unpack_log_lsa (ptr, savept_lsa);
	}
      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
    }

  return tran_state;
#else /* CS_MODE */
  TRAN_STATE tran_state = TRAN_UNACTIVE_UNKNOWN;

  ENTER_SERVER ();

  tran_state = xtran_server_partial_abort (NULL, savept_name, savept_lsa);

  EXIT_SERVER ();

  return tran_state;
#endif /* !CS_MODE */
}

/*
 * acl_reload -
 *
 * return:
 *
 */
int
acl_reload ()
{
#if defined(CS_MODE)
  int error = ER_NET_CLIENT_DATA_RECEIVE;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_ACL_RELOAD,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_errcode (reply, &error);
    }

  return error;
#else
  return NO_ERROR;
#endif /* !CS_MODE */
}


/*
 * acl_dump -
 *
 * return:
 *
 *   outfp(in):
 */
void
acl_dump (UNUSED_ARG FILE * outfp)
{
#if defined(CS_MODE)
  int req_error;

  if (outfp == NULL)
    {
      outfp = stdout;
    }

  req_error = net_client_request_recv_stream (NET_SERVER_ACL_DUMP,
					      NULL, 0, NULL, 0,
					      NULL, 0, outfp);
#endif /* !CS_MODE */
}

/*
 * lock_dump -
 *
 * return:
 *
 *   outfp(in):
 */
void
lock_dump (FILE * outfp)
{
#if defined(CS_MODE)
  int req_error;

  if (outfp == NULL)
    {
      outfp = stdout;
    }

  req_error = net_client_request_recv_stream (NET_SERVER_LK_DUMP,
					      NULL, 0, NULL, 0,
					      NULL, 0, outfp);
#else /* CS_MODE */

  ENTER_SERVER ();

  xlock_dump (NULL, outfp);

  EXIT_SERVER ();
#endif /* !CS_MODE */
}

/*
 * boot_initialize_server -
 *
 * return:
 *
 *   print_version(in):
 *   db_overwrite(in):
 *   db_desired_pagesize(in):
 *   db_name(in):
 *   db_path(in):
 *   vol_path(in):
 *   db_comments(in):
 *   db_npages(in):
 *   file_addmore_vols(in):
 *   db_server_host(in):
 *   log_path(in):
 *   log_npages(in):
 *   rootclass_oid(in):
 *   rootclass_hfid(in):
 *   client_prog_name(in):
 *   client_user_name(in):
 *   client_host_name(in):
 *   client_process_id(in):
 *   client_lock_wait(in):
 *
 * NOTE:
 */
int
boot_initialize_server (UNUSED_ARG const BOOT_CLIENT_CREDENTIAL *
			client_credential,
			UNUSED_ARG BOOT_DB_PATH_INFO * db_path_info,
			UNUSED_ARG bool db_overwrite,
			UNUSED_ARG const char *file_addmore_vols,
			UNUSED_ARG DKNPAGES db_npages,
			UNUSED_ARG PGLENGTH db_desired_pagesize,
			UNUSED_ARG DKNPAGES log_npages,
			UNUSED_ARG PGLENGTH db_desired_log_page_size,
			UNUSED_ARG OID * rootclass_oid,
			UNUSED_ARG HFID * rootclass_hfid,
			UNUSED_ARG int client_lock_wait)
{
#if defined(CS_MODE)
  /* Should not called in CS_MODE */
  assert (0);
  return NULL_TRAN_INDEX;
#else /* CS_MODE */
  int tran_index = NULL_TRAN_INDEX;

  ENTER_SERVER ();

  tran_index = xboot_initialize_server (NULL, client_credential, db_path_info,
					db_overwrite, file_addmore_vols,
					db_npages, db_desired_pagesize,
					log_npages, db_desired_log_page_size,
					rootclass_oid, rootclass_hfid,
					client_lock_wait);

  EXIT_SERVER ();

  return (tran_index);
#endif /* !CS_MODE */
}

/*
 * boot_register_client -
 *
 * return:
 *
 *   client_credential(in)
 *   client_lock_wait(in):
 *   tran_state(out):
 *   server_credential(out):
 */
int
boot_register_client (BOOT_CLIENT_CREDENTIAL * client_credential,
		      int client_lock_wait,
		      TRAN_STATE * tran_state,
		      BOOT_SERVER_CREDENTIAL * server_credential)
{
#if defined(CS_MODE)
  int tran_index = NULL_TRAN_INDEX;
  int request_size, area_size, req_error, temp_int;
  char *request, *reply, *area, *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  CSS_NET_PACKET *recv_packet = NULL;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = OR_INT_SIZE	/* client_type */
    + length_const_string (client_credential->client_info, NULL)	/* client_info */
    + length_const_string (client_credential->db_name, NULL)	/* db_name */
    + length_const_string (client_credential->db_user, NULL)	/* db_user */
    + length_const_string (client_credential->db_password, NULL)	/* db_password */
    + length_const_string (client_credential->program_name, NULL)	/* prog_name */
    + length_const_string (client_credential->login_name, NULL)	/* login_name */
    + length_const_string (client_credential->host_name, NULL)	/* host_name */
    + OR_INT_SIZE		/* process_id */
    + OR_INT_SIZE;		/* client_lock_wait */

  request = (char *) malloc (request_size);
  if (request)
    {
      ptr = or_pack_int (request, (int) client_credential->client_type);
      ptr = pack_const_string (ptr, client_credential->client_info);
      ptr = pack_const_string (ptr, client_credential->db_name);
      ptr = pack_const_string (ptr, client_credential->db_user);
      ptr = pack_const_string (ptr, client_credential->db_password);
      ptr = pack_const_string (ptr, client_credential->program_name);
      ptr = pack_const_string (ptr, client_credential->login_name);
      ptr = pack_const_string (ptr, client_credential->host_name);
      ptr = or_pack_int (ptr, client_credential->process_id);
      ptr = or_pack_int (ptr, client_lock_wait);

      req_error = net_client_request (NET_SERVER_BO_REGISTER_CLIENT,
				      &recv_packet, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  or_unpack_int (reply, &area_size);

	  area = css_net_packet_get_buffer (recv_packet, 1, area_size, true);
	  server_credential->alloc_buffer = area;

	  if (area != NULL && area_size > 0)
	    {
	      ptr = or_unpack_int (area, &tran_index);
	      ptr = or_unpack_int (ptr, &temp_int);
	      *tran_state = (TRAN_STATE) temp_int;
	      ptr =
		or_unpack_string_nocopy (ptr,
					 &server_credential->db_full_name);
	      ptr =
		or_unpack_string_nocopy (ptr, &server_credential->host_name);
	      ptr = or_unpack_int (ptr, &server_credential->process_id);
	      ptr = or_unpack_oid (ptr, &server_credential->root_class_oid);
	      ptr = or_unpack_hfid (ptr, &server_credential->root_class_hfid);
	      ptr = or_unpack_int (ptr, &temp_int);
	      server_credential->page_size = (PGLENGTH) temp_int;
	      ptr = or_unpack_int (ptr, &temp_int);
	      server_credential->log_page_size = (PGLENGTH) temp_int;
	      ptr = or_unpack_int (ptr, &server_credential->db_charset);
	      ptr =
		or_unpack_int (ptr, &server_credential->server_start_time);
	      ptr =
		or_unpack_string_nocopy (ptr, &server_credential->db_lang);
	    }

	  css_net_packet_free (recv_packet);
	}
      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
    }

  return tran_index;
#else /* CS_MODE */
  int tran_index = NULL_TRAN_INDEX;

  ENTER_SERVER ();

  tran_index = xboot_register_client (NULL, client_credential,
				      client_lock_wait,
				      tran_state, server_credential);
  EXIT_SERVER ();

  return tran_index;
#endif /* !CS_MODE */
}

/*
 * boot_unregister_client -
 *
 * return:
 *
 *   tran_index(in):
 *
 * NOTE:
 */
int
boot_unregister_client (int tran_index)
{
#if defined(CS_MODE)
  int success = ER_FAILED;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_pack_int (request, tran_index);

  er_stack_push ();

  req_error = net_client_request (NET_SERVER_BO_UNREGISTER_CLIENT,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  er_stack_pop ();

  if (!req_error)
    {
      or_unpack_int (reply, &success);
    }

  return success;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xboot_unregister_client (NULL, tran_index);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * boot_add_volume_extension -
 *
 * return:
 *
 *   ext_info(in):
 *
 * NOTE:
 */
VOLID
boot_add_volume_extension (DBDEF_VOL_EXT_INFO * ext_info)
{
#if defined(CS_MODE)
  int int_volid;
  VOLID volid = NULL_VOLID;
  int request_size, strlen2;
  char *request, *ptr;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = length_const_string (ext_info->fullname, &strlen2)
    + OR_INT_SIZE + OR_INT_SIZE + OR_INT_SIZE + OR_INT_SIZE;

  request = (char *) malloc (request_size);
  if (request)
    {
      ptr =
	pack_const_string_with_length (request, ext_info->fullname, strlen2);
      ptr = or_pack_int (ptr, (int) ext_info->max_npages);
      ptr = or_pack_int (ptr, (int) ext_info->max_writesize_in_sec);
      ptr = or_pack_int (ptr, (int) ext_info->purpose);
      ptr = or_pack_int (ptr, (int) ext_info->overwrite);
      req_error = net_client_request (NET_SERVER_BO_ADD_VOLEXT,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  or_unpack_int (reply, &int_volid);
	  volid = int_volid;
	}
      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
    }

  return volid;
#else /* CS_MODE */
  VOLID volid;

  ENTER_SERVER ();

  volid = xboot_add_volume_extension (NULL, ext_info);

  EXIT_SERVER ();

  return volid;
#endif /* !CS_MODE */
}

/*
 * boot_find_number_permanent_volumes -
 *
 * return:
 *
 * NOTE:
 */
int
boot_find_number_permanent_volumes (void)
{
#if defined(CS_MODE)
  int nvols = -1;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_BO_FIND_NPERM_VOLS,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &nvols);
    }

  return nvols;
#else /* CS_MODE */
  int nvols = -1;

  ENTER_SERVER ();

  nvols = xboot_find_number_permanent_volumes (NULL);

  EXIT_SERVER ();

  return nvols;
#endif /* !CS_MODE */
}

/*
 * boot_find_number_temp_volumes -
 *
 * return:
 *
 * NOTE:
 */
int
boot_find_number_temp_volumes (void)
{
#if defined(CS_MODE)
  int nvols = -1;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_BO_FIND_NTEMP_VOLS,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &nvols);
    }

  return nvols;
#else /* CS_MODE */
  int nvols = -1;

  ENTER_SERVER ();

  nvols = xboot_find_number_temp_volumes (NULL);

  EXIT_SERVER ();

  return nvols;
#endif /* !CS_MODE */
}

/*
 * boot_find_last_temp -
 *
 * return:
 *
 * NOTE:
 */
int
boot_find_last_temp (void)
{
#if defined(CS_MODE)
  int nvols = -1;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_BO_FIND_LAST_TEMP,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &nvols);
    }

  return nvols;
#else /* CS_MODE */
  int nvols = -1;

  ENTER_SERVER ();

  nvols = xboot_find_last_temp (NULL);

  EXIT_SERVER ();

  return nvols;
#endif /* !CS_MODE */
}

/*
 * boot_find_number_bestspace_entries -
 *
 * return:
 *
 * NOTE:
 */
int
boot_find_number_bestspace_entries (void)
{
#if defined(CS_MODE)
  int nbest = -1;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_BO_FIND_NBEST_ENTRIES,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &nbest);
    }

  return nbest;
#else /* CS_MODE */
  int nbest = -1;

  ENTER_SERVER ();

  nbest = xboot_find_number_bestspace_entries (NULL);

  EXIT_SERVER ();

  return nbest;
#endif /* !CS_MODE */
}

/*
 * boot_delete -
 *
 * return:
 *
 * NOTE:
 */
int
boot_delete (UNUSED_ARG const char *db_name, UNUSED_ARG bool force_delete)
{
#if defined(CS_MODE)
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	  ER_ONLY_IN_STANDALONE, 1, "delete database");
  return ER_FAILED;
#else /* CS_MODE */
  int error_code;

  ENTER_SERVER ();

  error_code = xboot_delete (NULL, db_name, force_delete);

  EXIT_SERVER ();

  return error_code;
#endif /* !CS_MODE */
}

/*
 * boot_restart_from_backup -
 *
 * return:
 *
 * NOTE:
 */
int
boot_restart_from_backup (UNUSED_ARG int print_restart,
			  UNUSED_ARG const char *db_name,
			  UNUSED_ARG BO_RESTART_ARG * r_args)
{
#if defined(CS_MODE)
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	  ER_ONLY_IN_STANDALONE, 1, "restart from backup");
  return NULL_TRAN_INDEX;
#else /* CS_MODE */
  int tran_index;

  ENTER_SERVER ();

  tran_index = xboot_restart_from_backup (NULL, print_restart, db_name,
					  r_args);

  EXIT_SERVER ();

  return tran_index;
#endif /* !CS_MODE */
}

/*
 * boot_shutdown_server -
 *
 * return:
 *
 * NOTE:
 */
bool
boot_shutdown_server (void)
{
#if defined(CS_MODE)
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_ONLY_IN_STANDALONE, 1, "");
  return false;
#else /* CS_MODE */
  bool result;

  ENTER_SERVER ();

  result = xboot_shutdown_server (NULL);

  EXIT_SERVER ();

  return result;
#endif /* !CS_MODE */
}

/*
 * csession_find_or_create_session - check if session is still active
 *                                     if not, create a new session
 *
 * return	   : error code or NO_ERROR
 * session_id (in/out) : the id of the session to end
 * server_session_key (in/out) :
 */
int
csession_find_or_create_session (SESSION_ID * session_id,
				 UNUSED_ARG char *server_session_key,
				 UNUSED_ARG const char *db_user,
				 UNUSED_ARG const char *host,
				 UNUSED_ARG const char *program_name)
{
#if defined (CS_MODE)
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply = NULL;
  char *request = NULL, *area = NULL;
  char *ptr;
  int request_size, area_size;
  int db_user_len, host_len, program_name_len;
  int error = NO_ERROR;
  CSS_NET_PACKET *recv_packet = NULL;

  reply = OR_ALIGNED_BUF_START (a_reply);
  request_size = OR_INT_SIZE;	/* session_id */
  request_size += or_packed_stream_length (SERVER_SESSION_KEY_SIZE);

  request_size += length_const_string (db_user, &db_user_len);
  request_size += length_const_string (host, &host_len);
  request_size += length_const_string (program_name, &program_name_len);

  reply = OR_ALIGNED_BUF_START (a_reply);

  request = (char *) malloc (request_size);
  if (request != NULL)
    {
      ptr = or_pack_int (request, ((int) *session_id));
      ptr = or_pack_stream (ptr, server_session_key, SERVER_SESSION_KEY_SIZE);
      ptr = pack_const_string_with_length (ptr, db_user, db_user_len);
      ptr = pack_const_string_with_length (ptr, host, host_len);
      ptr =
	pack_const_string_with_length (ptr, program_name, program_name_len);

      req_error = net_client_request (NET_SERVER_SES_CHECK_SESSION,
				      &recv_packet, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));

      if (req_error == NO_ERROR)
	{
	  ptr = or_unpack_int (reply, &area_size);
	  ptr = or_unpack_int (ptr, &error);
	  if (error != NO_ERROR)
	    {
	      css_net_packet_free (recv_packet);
	      free_and_init (request);
	      return error;
	    }

	  area = css_net_packet_get_buffer (recv_packet, 1, area_size, false);

	  if (area != NULL && area_size > 0)
	    {
	      ptr = or_unpack_int (area, (int *) session_id);
	      ptr = or_unpack_stream (ptr, server_session_key,
				      SERVER_SESSION_KEY_SIZE);
	    }

	  css_net_packet_free (recv_packet);
	}
      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
      return ER_FAILED;
    }

  return NO_ERROR;
#else
  int result = NO_ERROR;
  SESSION_KEY key;

  ENTER_SERVER ();

  key.fd = INVALID_SOCKET;
  if (db_Session_id == DB_EMPTY_SESSION)
    {
      result = xsession_create_new (NULL, &key);
    }
  else
    {
      key.id = db_Session_id;
      if (xsession_check_session (NULL, &key) != NO_ERROR)
	{
	  /* create new session */
	  if (xsession_create_new (NULL, &key) != NO_ERROR)
	    {
	      result = ER_FAILED;
	    }
	}
    }

  db_Session_id = key.id;
  *session_id = db_Session_id;

  EXIT_SERVER ();

  return result;
#endif
}

/*
 * csession_end_session - end the session identified by session_id
 *
 * return	   : error code or NO_ERROR
 * session_id (in) : the id of the session to end
 */
int
csession_end_session (SESSION_ID session_id)
{
#if defined (CS_MODE)
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  char *ptr;

  reply = OR_ALIGNED_BUF_START (a_reply);
  request = OR_ALIGNED_BUF_START (a_request);

  ptr = or_pack_int (request, session_id);

  req_error = net_client_request (NET_SERVER_SES_END_SESSION,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (req_error != NO_ERROR)
    {
      return ER_FAILED;
    }

  return NO_ERROR;
#else
  int result = NO_ERROR;
  SESSION_KEY key;

  ENTER_SERVER ();

  key.id = session_id;
  key.fd = INVALID_SOCKET;
  result = xsession_end_session (NULL, &key);

  EXIT_SERVER ();

  return result;
#endif
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * clogin_user () - login user
 * return	  : error code or NO_ERROR
 * username (in) : name of the user
 */
int
clogin_user (const char *username)
{
#if defined (CS_MODE)
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *request = NULL;
  int username_len, req_len;

  req_len = length_const_string (username, &username_len);

  request = (char *) malloc (req_len);
  if (request == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      req_len);
      return ER_FAILED;
    }

  pack_const_string_with_length (request, username, username_len);

  req_error = net_client_request (NET_SERVER_LOGIN_USER,
				  NULL, 1, 1,
				  request, req_len,
				  OR_ALIGNED_BUF_START (a_reply),
				  OR_ALIGNED_BUF_SIZE (a_reply));
  if (request != NULL)
    {
      free_and_init (request);
    }

  if (req_error != NO_ERROR)
    {
      return ER_FAILED;
    }

  or_unpack_int (OR_ALIGNED_BUF_START (a_reply), &req_error);

  return req_error;
#else
  int result = NO_ERROR;

  ENTER_SERVER ();

  result = xlogin_user (NULL, username);

  EXIT_SERVER ();

  return result;
#endif
}
#endif

/*
 * boot_get_server_state - get server's HA state
 *   return: server state
 */
HA_STATE
boot_get_server_state (void)
{
#if defined(CS_MODE)
  int req_error;
  int server_state;
  HA_STATE cur_state = HA_STATE_UNKNOWN;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_BO_GET_SERVER_STATE,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &server_state);
      cur_state = (HA_STATE) server_state;
    }

  return cur_state;
#else /* CS_MODE */
  /* Cannot run in standalone mode */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NOT_IN_STANDALONE, 1,
	  "get_server_state");
  return HA_STATE_NA;
#endif /* !CS_MODE */
}

/*
 * boot_notify_ha_apply_state - notify log applier's state to the server
 *   return: NO_ERROR or ER_FAILED
 *
 *   host_ip(in):
 *   state(in):
 */
#if !defined(CS_MODE)
int
boot_notify_ha_apply_state (UNUSED_ARG const char *host_ip,
			    UNUSED_ARG HA_APPLY_STATE state)
{
  /* Cannot run in standalone mode */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NOT_IN_STANDALONE, 1,
	  "log applier");
  return ER_FAILED;
}
#else /* CS_MODE */
int
boot_notify_ha_apply_state (const char *host_ip, HA_APPLY_STATE state)
{
  int req_error, status = ER_FAILED;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply, *request, *ptr;
  int request_size;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = OR_INT_SIZE;
  request_size += length_const_string (host_ip, NULL);

  request = (char *) malloc (request_size);
  if (request == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
      return ER_FAILED;
    }

  ptr = pack_const_string (request, host_ip);
  ptr = or_pack_int (ptr, (int) state);

  req_error = net_client_request (NET_SERVER_BO_NOTIFY_HA_APPLY_STATE,
				  NULL, 1, 1,
				  request, request_size,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &status);
    }

  free_and_init (request);

  return status;
}
#endif /* CS_MODE */

/*
 * stats_get_statistics_from_server () -
 *
 * return:
 *
 *   classoid(in):
 *   timestamp(in):
 *   length_ptr(in):
 *
 * NOTE:
 */
char *
stats_get_statistics_from_server (OID * classoid, unsigned int timestamp,
				  int *length_ptr)
{
#if defined(CS_MODE)
  int req_error;
  char *area = NULL;
  char *ptr;
  OR_ALIGNED_BUF (OR_OID_SIZE + OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;
  CSS_NET_PACKET *recv_packet = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_oid (request, classoid);
  ptr = or_pack_int (ptr, (int) timestamp);

  req_error = net_client_request (NET_SERVER_QST_GET_STATISTICS,
				  &recv_packet, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, length_ptr);

      area = css_net_packet_get_buffer (recv_packet, 1, -1, true);

      css_net_packet_free (recv_packet);

      return area;
    }
  else
    {
      return NULL;
    }
#else /* CS_MODE */
  char *area;

  ENTER_SERVER ();

  area =
    xstats_get_statistics_from_server (NULL, classoid, timestamp, length_ptr);

  EXIT_SERVER ();

  return area;
#endif /* !CS_MODE */
}

/*
 * stats_update_statistics -
 *
 * return:
 *
 *   classoid(in):
 *   update_stats(in): true iff UPDATE
 *   with_fullscan(in): true iff WITH FULLSCAN
 *
 * NOTE:
 */
int
stats_update_statistics (OID * classoid, int update_stats, int with_fullscan)
{
#if defined(CS_MODE)
  int error = ER_NET_CLIENT_DATA_RECEIVE;
  int req_error;
  OR_ALIGNED_BUF (OR_OID_SIZE + OR_INT_SIZE + OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;
  char *ptr;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_oid (request, classoid);
  ptr = or_pack_int (ptr, update_stats);
  ptr = or_pack_int (ptr, with_fullscan);

  req_error = net_client_request (NET_SERVER_QST_UPDATE_STATISTICS,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_errcode (reply, &error);
    }

  return error;
#else /* CS_MODE */
  int success;

  ENTER_SERVER ();

  success = xstats_update_statistics (NULL, classoid,
				      (update_stats ? true : false),
				      (with_fullscan ? STATS_WITH_FULLSCAN :
				       STATS_WITH_SAMPLING));

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * stats_update_all_statistics -
 *
 * return:
 *   update_stats(in): true iff UPDATE
 *   with_fullscan(in): true iff WITH FULLSCAN
 *
 * NOTE:
 */
int
stats_update_all_statistics (int update_stats, int with_fullscan)
{
#if defined(CS_MODE)
  int error = ER_NET_CLIENT_DATA_RECEIVE;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;
  char *ptr;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_int (request, update_stats);
  ptr = or_pack_int (ptr, with_fullscan);

  req_error = net_client_request (NET_SERVER_QST_UPDATE_ALL_STATISTICS,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_errcode (reply, &error);
    }

  return error;
#else /* CS_MODE */
  int success;

  ENTER_SERVER ();

  success =
    xstats_update_all_statistics (NULL,
				  (update_stats ? true : false),
				  (with_fullscan ? STATS_WITH_FULLSCAN :
				   STATS_WITH_SAMPLING));

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * btree_add_index () -
 *
 * return:
 *
 *   btid(in/out):
 *   num_atts(in):
 *   att_type(in):
 *   class_oid(in):
 *   attr_id(in):
 *
 * NOTE:
 */
int
btree_add_index (BTID * btid, int num_atts, DB_TYPE * att_type,
		 OID * class_oid, int attr_id)
{
#if defined(CS_MODE)
  int error = NO_ERROR;
  int req_error, request_size;
  int i;
  char *ptr;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_BTID_ALIGNED_SIZE) a_reply;
  char *reply;

  if (num_atts <= 0 || att_type == NULL)
    {
      return ER_FAILED;
    }

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = OR_BTID_ALIGNED_SIZE	/* BTID */
    + OR_INT_SIZE		/* num_atts */
    + num_atts * OR_INT_SIZE	/* att_type */
    + OR_OID_SIZE		/* class_oid */
    + OR_INT_SIZE;		/* attr_id */

  request = (char *) malloc (request_size);
  if (request)
    {
      ptr = or_pack_btid (request, btid);
      ptr = or_pack_int (ptr, num_atts);
      for (i = 0; i < num_atts; i++)
	{
	  ptr = or_pack_int (ptr, att_type[i]);
	}
      ptr = or_pack_oid (ptr, class_oid);
      ptr = or_pack_int (ptr, attr_id);

      req_error = net_client_request (NET_SERVER_BTREE_ADD_INDEX,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  ptr = or_unpack_int (reply, &error);
	  ptr = or_unpack_btid (ptr, btid);
	  if (error != NO_ERROR)
	    {
	      assert (VFID_ISNULL (&btid->vfid));
	      assert (btid->root_pageid == NULL_PAGEID);
	    }
	}

      free_and_init (request);
    }
  else
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, request_size);
    }

  return error;
#else /* CS_MODE */
  int error = NO_ERROR;

  ENTER_SERVER ();

  if (xbtree_add_index (NULL, btid, num_atts, att_type, class_oid, attr_id) ==
      NULL)
    {
      assert (VFID_ISNULL (&btid->vfid));
      assert (btid->root_pageid == NULL_PAGEID);

      error = er_errid ();
      assert (error != NO_ERROR);
    }

  EXIT_SERVER ();

  return error;
#endif /* !CS_MODE */
}

/*
 * btree_load_data -
 *
 * return: error code or NO_ERROR
 *
 *   btid(in):
 *   class_oid(in):
 *   hfid(in):
 *
 * NOTE:
 */
int
btree_load_data (BTID * btid, OID * class_oid, HFID * hfid)
{
#if defined(CS_MODE)
  char *request, *reply, *ptr;
  int error;
  OR_ALIGNED_BUF (OR_BTID_ALIGNED_SIZE + OR_OID_SIZE
		  + OR_HFID_SIZE) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  if (btid == NULL || BTID_IS_NULL (btid)
      || class_oid == NULL || OID_ISNULL (class_oid) || hfid == NULL)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");

      return error;
    }

  ptr = or_pack_btid (request, btid);
  ptr = or_pack_oid (ptr, class_oid);
  ptr = or_pack_hfid (ptr, hfid);

  error = net_client_request (NET_SERVER_BTREE_LOAD_DATA,
			      NULL, 1, 1,
			      request, OR_ALIGNED_BUF_SIZE (a_request),
			      reply, OR_ALIGNED_BUF_SIZE (a_reply));

  if (error == NO_ERROR)
    {
      ptr = or_unpack_int (reply, &error);
    }

  return error;
#else /* CS_MODE */
  int error = NO_ERROR;

  ENTER_SERVER ();

  error = xbtree_load_data (NULL, btid, class_oid, hfid);

  EXIT_SERVER ();

  return error;
#endif /* !CS_MODE */
}

/*
 * btree_delete_index -
 *
 * return:
 *
 *   btid(in):
 *
 * NOTE:
 */
int
btree_delete_index (BTID * btid)
{
#if defined(CS_MODE)
  int req_error, status = NO_ERROR;
  OR_ALIGNED_BUF (OR_BTID_ALIGNED_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_pack_btid (request, btid);

  req_error = net_client_request (NET_SERVER_BTREE_DEL_INDEX,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &status);
    }

  return status;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xbtree_delete_index (NULL, btid);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * btree_find_unique -
 *
 * return:
 *
 *   class_oid(in):
 *   btid(in):
 *   key(in):
 *   oid(in):
 *
 * NOTE:
 */
BTREE_SEARCH
btree_find_unique (OID * class_oid, BTID * btid, DB_IDXKEY * key, OID * oid)
{
  assert (class_oid != NULL);
  assert (!OID_ISNULL (class_oid));
  assert (btid != NULL);
  assert (key != NULL);
  assert (key->size == 1);
  assert (DB_VALUE_DOMAIN_TYPE (&(key->vals[0])) == DB_TYPE_VARCHAR);

  BTREE_SEARCH status = BTREE_ERROR_OCCURRED;

  if (class_oid == NULL || btid == NULL || key == NULL || oid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }
  else
    {
#if defined(CS_MODE)
      int req_error, request_size, key_size;
      char *ptr;
      char *request;
      OR_ALIGNED_BUF (OR_INT_SIZE + OR_OID_SIZE) a_reply;
      char *reply;

      reply = OR_ALIGNED_BUF_START (a_reply);

      key_size = OR_IDXKEY_ALIGNED_SIZE (key);
      request_size = key_size + OR_BTID_ALIGNED_SIZE + OR_OID_SIZE;

      request = (char *) malloc (request_size);
      if (request == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, request_size);
	  return status;
	}

      ptr = request;

      ptr = or_pack_db_idxkey (ptr, key);

      ptr = or_pack_btid (ptr, btid);

      ptr = or_pack_oid (ptr, class_oid);

      /* reset request_size as real packed size */
      request_size = ptr - request;

      req_error = net_client_request (NET_SERVER_BTREE_FIND_UNIQUE,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  int istatus;
	  ptr = or_unpack_int (reply, &istatus);
	  ptr = or_unpack_oid (ptr, oid);
	  status = (BTREE_SEARCH) istatus;
	}
      else
	{
	  OID_SET_NULL (oid);
	}

      free_and_init (request);
#else /* CS_MODE */

      ENTER_SERVER ();
      status = xbtree_find_unique (NULL, class_oid, btid, key, oid);
      EXIT_SERVER ();

#endif /* !CS_MODE */
    }

  return status;
}

/*
 * qfile_get_list_file_page -
 *
 * return:
 *
 *   query_id(in):
 *   volid(in):
 *   pageid(in):
 *   buffer(in):
 *   buffer_size(in):
 *
 * NOTE:
 */
int
qfile_get_list_file_page (QUERY_ID query_id, VOLID volid, PAGEID pageid,
			  char *buffer, UNUSED_ARG int *buffer_size)
{
#if defined(CS_MODE)
  int error = ER_NET_CLIENT_DATA_RECEIVE;
  int req_error;
  char *ptr;
  OR_ALIGNED_BUF (OR_PTR_SIZE + OR_INT_SIZE + OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_reply;
  char *reply;

  assert (buffer != NULL);
#if 1				/* TODO - trace */
  assert (pageid != NULL_PAGEID);
#endif

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_ptr (request, query_id);
  ptr = or_pack_int (ptr, (int) volid);
  ptr = or_pack_int (ptr, (int) pageid);

  req_error = net_client_request (NET_SERVER_LS_GET_LIST_FILE_PAGE,
				  NULL, 1, 2,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply),
				  buffer, -1);
  if (!req_error)
    {
      ptr = or_unpack_int (reply, buffer_size);
      ptr = or_unpack_int (ptr, &error);
    }

  return error;
#else /* CS_MODE */
  int success;
  int page_size;

  assert (buffer != NULL);

  ENTER_SERVER ();

  success = xqfile_get_list_file_page (NULL, query_id, volid, pageid,
				       buffer, &page_size);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * qmgr_prepare_query - Send a SERVER_QM_PREPARE request to the server
 *
 * Send XASL stream and receive XASL file id (XASL_ID) as a result.
 * If xasl_buffer == NULL, the server will look up the XASL cache and then
 * return the cached XASL file id if found, otherwise return NULL.
 * This function is a counter part to sqmgr_prepare_query().
 *
 * return: XASL_ID
 *
 *   context(in): query string & plan
 *   stream(in/out): xasl stream, size & xasl_header
 *   user_oid(in):
 *
 * NOTE: If xasl_header_p is not null, also XASL node header will be requested
 */
XASL_ID *
qmgr_prepare_query (COMPILE_CONTEXT * context, XASL_STREAM * stream,
		    const OID * user_oid)
{
#if defined(CS_MODE)
  int error = NO_ERROR;
  int req_error, request_size = 0;
  int sql_hash_text_len, sql_plan_text_len, reply_buffer_size = 0;
  char *request = NULL, *reply = NULL, *ptr = NULL, *reply_buffer = NULL;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE + OR_XASL_ID_SIZE) a_reply;
  int get_xasl_header = stream->xasl_header != NULL;
  CSS_NET_PACKET *recv_packet = NULL;

  INIT_XASL_NODE_HEADER (stream->xasl_header);

  reply = OR_ALIGNED_BUF_START (a_reply);

  /* sql hash text */
  request_size +=
    length_const_string (context->sql_hash_text, &sql_hash_text_len);

  /* sql plan text */
  request_size +=
    length_const_string (context->sql_plan_text, &sql_plan_text_len);

  /* sql user text */
  request_size +=
    length_string_with_null_padding (context->sql_user_text_len);

  request_size += OR_OID_SIZE;	/* user_oid */
  request_size += OR_INT_SIZE;	/* size */
  request_size += OR_INT_SIZE;	/* get_xasl_header */

  request = (char *) malloc (request_size);
  if (request == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, request_size);
      return NULL;
    }

  /* pack query alias string as a request data */
  ptr = pack_const_string_with_length (request, context->sql_hash_text,
				       sql_hash_text_len);
  /* pack query plan as a request data */
  ptr = pack_const_string_with_length (ptr, context->sql_plan_text,
				       sql_plan_text_len);
  /* pack query string as a request data */
  ptr = pack_string_with_null_padding (ptr, context->sql_user_text,
				       context->sql_user_text_len);


  /* pack OID of the current user */
  ptr = or_pack_oid (ptr, user_oid);
  /* pack size of XASL stream */
  ptr = or_pack_int (ptr, stream->xasl_stream_size);
  ptr = or_pack_int (ptr, get_xasl_header);

  /* send SERVER_QM_QUERY_PREPARE request with request data and XASL stream;
     receive XASL file id (XASL_ID) as a reply */
  req_error = net_client_request (NET_SERVER_QM_QUERY_PREPARE,
				  &recv_packet, 2, 1,
				  request, request_size,
				  (char *) stream->xasl_stream,
				  stream->xasl_stream_size,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      ptr = or_unpack_int (reply, &reply_buffer_size);
      ptr = or_unpack_int (ptr, &error);
      if (error == NO_ERROR)
	{
	  /* NULL XASL_ID will be returned when cache not found */
	  OR_UNPACK_XASL_ID (ptr, stream->xasl_id);

	  reply_buffer = css_net_packet_get_buffer (recv_packet, 1,
						    reply_buffer_size, false);
	  if (get_xasl_header && reply_buffer != NULL
	      && reply_buffer_size != 0)
	    {
	      ptr = reply_buffer;
	      OR_UNPACK_XASL_NODE_HEADER (ptr, stream->xasl_header);
	    }
	}
      else
	{
	  stream->xasl_id = NULL;
	}

      css_net_packet_free (recv_packet);
    }
  else
    {
      stream->xasl_id = NULL;
    }

  if (request != NULL)
    {
      free_and_init (request);
    }

  return stream->xasl_id;
#else /* CS_MODE */
  XASL_ID *p;

  ENTER_SERVER ();

  INIT_XASL_NODE_HEADER (stream->xasl_header);
  /* call the server routine of query prepare */
  p = xqmgr_prepare_query (NULL, context, stream, user_oid);
  if (p)
    {
      *stream->xasl_id = *p;
    }
  else
    {
      return NULL;
    }

  EXIT_SERVER ();

  return p;

#endif /* !CS_MODE */
}


/*
 * db_set_execution_plan
 *   plan(in):
 *   length(in):
 *
 * return:
 *
 */
void
db_set_execution_plan (char *plan, int length)
{
  int null_padded_length = 0;

  if (plan == NULL)
    {
      if (db_Execution_plan != NULL)
	{
	  db_Execution_plan[0] = '\0';
	}
      return;
    }

  null_padded_length = length + 1;

  if (db_Execution_plan == NULL)
    {
      db_Execution_plan_length = PLAN_BUF_INITIAL_LENGTH;
      while (db_Execution_plan_length < null_padded_length)
	{
	  db_Execution_plan_length *= 2;
	}
      db_Execution_plan =
	(char *) malloc (db_Execution_plan_length * sizeof (char));
    }
  else if (db_Execution_plan_length < null_padded_length)
    {
      while (db_Execution_plan_length < null_padded_length)
	{
	  db_Execution_plan_length *= 2;
	}

      free (db_Execution_plan);

      db_Execution_plan =
	(char *) malloc (db_Execution_plan_length * sizeof (char));
    }

  if (db_Execution_plan == NULL)
    {
      db_Execution_plan_length = -1;
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, db_Execution_plan_length);
      return;
    }

  strncpy (db_Execution_plan, plan, length);
  db_Execution_plan[length] = '\0';
}

/*
 * db_get_execution_plan
 *
 * return:
 *
 */
char *
db_get_execution_plan (void)
{
  if (db_Execution_plan == NULL)
    {
      return NULL;
    }

  return db_Execution_plan;
}

/*
 * db_free_execution_plan :
 *
 * return:
 *
 */
void
db_free_execution_plan (void)
{
  if (db_Execution_plan != NULL)
    {
      free_and_init (db_Execution_plan);
      db_Execution_plan_length = -1;
    }
}

/*
 * qmgr_execute_query - Send a SERVER_QM_EXECUTE request to the server
 *
 * Send XASL file id and parameter values if exist and receive list file id
 * that contains query result. If an error occurs, return NULL QFILE_LIST_ID.
 * This function is a counter part to sqmgr_execute_query().
 */
/*
 * qmgr_execute_query -
 *
 * return:
 *
 *   xasl_id(in):
 *   query_idp(in):
 *   dbval_cnt(in):
 *   dbvals(in):
 *   flag(in):
 *   query_timeout(in):
 *   shard_groupid(in):
 *   shard_key(in):
 *
 * NOTE:
 */
QFILE_LIST_ID *
qmgr_execute_query (const XASL_ID * xasl_id, QUERY_ID * query_idp,
		    int dbval_cnt, const DB_VALUE * dbvals,
		    QUERY_FLAG flag, int query_timeout,
		    int shard_groupid, DB_VALUE * shard_key,
		    UNUSED_ARG QUERY_EXECUTE_STATUS_FLAG * qe_status_flag)
{
#if defined(CS_MODE)
  QFILE_LIST_ID *list_id = NULL;
  int req_error, senddata_size, shard_keyval_size;
  char *request, *reply, *senddata = NULL, *shard_keyval_buf = NULL;
  char *ptr;
  OR_ALIGNED_BUF (OR_XASL_ID_SIZE + OR_INT_SIZE * 6) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE * 4 + OR_PTR_ALIGNED_SIZE +
		  OR_INT_SIZE) a_reply;
  int i;
  const DB_VALUE *dbval;
  CSS_NET_PACKET *recv_packet = NULL;
  char *replydata_page = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  /* make send data using if parameter values for host variables are given */
  senddata_size = 0;
  for (i = 0, dbval = dbvals; i < dbval_cnt; i++, dbval++)
    {
      senddata_size += OR_VALUE_ALIGNED_SIZE (dbval);
    }

  if (senddata_size != 0)
    {
      senddata = (char *) malloc (senddata_size);
      if (senddata == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, senddata_size);
	  goto execute_end;
	}

      ptr = senddata;
      for (i = 0, dbval = dbvals; i < dbval_cnt; i++, dbval++)
	{
	  ptr = or_pack_db_value (ptr, dbval);
	}

      /* change senddata_size as real packing size */
      senddata_size = ptr - senddata;
    }

/* init */
  shard_keyval_size = 0;
  shard_keyval_buf = NULL;

  if (shard_key != NULL)
    {
      assert (!DB_IS_NULL (shard_key));

      shard_keyval_size = OR_VALUE_ALIGNED_SIZE (shard_key);
      if (shard_keyval_size > 0)
	{
	  shard_keyval_buf = (char *) malloc (shard_keyval_size);
	  if (shard_keyval_buf == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, shard_keyval_size);
	      goto execute_end;
	    }

	  ptr = or_pack_db_value (shard_keyval_buf, shard_key);
	  shard_keyval_size = ptr - shard_keyval_buf;
	}
    }

  /* pack XASL file id (XASL_ID), number of parameter values,
     size of the send data, and query execution mode flag as a request data */
  ptr = request;
  OR_PACK_XASL_ID (ptr, xasl_id);
  ptr = or_pack_int (ptr, dbval_cnt);
  ptr = or_pack_int (ptr, senddata_size);
  ptr = or_pack_int (ptr, flag);
  ptr = or_pack_int (ptr, query_timeout);
  ptr = or_pack_int (ptr, shard_groupid);
  ptr = or_pack_int (ptr, shard_keyval_size);

  replydata_page = (char *) malloc (DB_PAGESIZE);
  if (replydata_page == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, DB_PAGESIZE);
      free_and_init (senddata);
      goto execute_end;
    }

  req_error = net_client_request (NET_SERVER_QM_QUERY_EXECUTE,
				  &recv_packet, 3, 3,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  senddata, senddata_size,
				  shard_keyval_buf, shard_keyval_size,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply),
				  NULL, 0, replydata_page, DB_PAGESIZE);

  if (!req_error)
    {
      int server_request;
      int replydata_size_listid;
      int replydata_size_page;
      int replydata_size_plan;
      char *replydata_plan;
      char *replydata_listid;
      char *page_ptr;

      ptr = or_unpack_int (reply, &server_request);
      assert ((QUERY_SERVER_REQUEST) server_request == QUERY_END);

      ptr = or_unpack_int (ptr, &replydata_size_listid);
      ptr = or_unpack_int (ptr, &replydata_size_page);
      ptr = or_unpack_int (ptr, &replydata_size_plan);

      ptr = or_unpack_ptr (ptr, query_idp);
      ptr = or_unpack_int (ptr, qe_status_flag);

      replydata_listid = css_net_packet_get_buffer (recv_packet, 1,
						    replydata_size_listid,
						    false);
      if (replydata_size_page > DB_PAGESIZE)
	{
	  page_ptr = css_net_packet_get_buffer (recv_packet, 2,
						replydata_size_page, true);
	}
      else
	{
	  page_ptr = replydata_page;
	  replydata_page = NULL;
	}

      list_id = get_list_id_from_execute_res (replydata_size_listid,
					      replydata_listid,
					      replydata_size_page, page_ptr);

      replydata_plan = css_net_packet_get_buffer (recv_packet, 3,
						  replydata_size_plan, false);
      if (replydata_plan != NULL)
	{
	  db_set_execution_plan (replydata_plan, replydata_size_plan);
	}

      css_net_packet_free (recv_packet);
    }

execute_end:

  if (senddata)
    {
      free_and_init (senddata);
    }
  if (shard_keyval_buf)
    {
      free_and_init (shard_keyval_buf);
    }
  if (replydata_page)
    {
      free_and_init (replydata_page);
    }

  return list_id;
#else /* CS_MODE */
  QFILE_LIST_ID *list_id = NULL;
  DB_VALUE *server_db_values = NULL;
  OID *oid;
  int i;

  ENTER_SERVER ();

  /* reallocate dbvals to use server allocation */
  if (dbval_cnt > 0)
    {
      size_t s = dbval_cnt * sizeof (DB_VALUE);

      server_db_values = (DB_VALUE *) malloc (s);
      if (server_db_values == NULL)
	{
	  goto cleanup;
	}
      for (i = 0; i < dbval_cnt; i++)
	{
	  DB_MAKE_NULL (&server_db_values[i]);
	}
      for (i = 0; i < dbval_cnt; i++)
	{
	  switch (DB_VALUE_TYPE (&dbvals[i]))
	    {
	    case DB_TYPE_OBJECT:
	      /* server cannot handle objects, convert to OID instead */
	      oid = ws_identifier (DB_GET_OBJECT (&dbvals[i]));
	      if (oid != NULL)
		{
		  DB_MAKE_OID (&server_db_values[i], oid);
		}
	      break;

	    default:
	      /* Clone value */
	      if (db_value_clone (&dbvals[i], &server_db_values[i]) !=
		  NO_ERROR)
		{
		  goto cleanup;
		}
	      break;
	    }
	}
    }
  else
    {
      /* No dbvals */
      server_db_values = NULL;
    }

  /* call the server routine of query execute */
  list_id = xqmgr_execute_query (NULL, xasl_id, query_idp, dbval_cnt,
				 server_db_values, &flag, query_timeout,
				 shard_groupid, shard_key, NULL);

cleanup:
  if (server_db_values != NULL)
    {
      for (i = 0; i < dbval_cnt; i++)
	{
	  db_value_clear (&server_db_values[i]);
	}
      free_and_init (server_db_values);
    }

  EXIT_SERVER ();

  return list_id;
#endif /* !CS_MODE */
}

/*
 * qmgr_end_query -
 *
 * return:
 *
 *   query_id(in):
 *
 * NOTE:
 */
int
qmgr_end_query (QUERY_ID query_id)
{
#if defined(CS_MODE)
  int status = ER_FAILED;
  int req_error;
  OR_ALIGNED_BUF (OR_PTR_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  or_pack_ptr (request, query_id);

  req_error = net_client_request (NET_SERVER_QM_QUERY_END,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &status);
    }

  return status;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xqmgr_end_query (NULL, query_id);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * qmgr_drop_query_plan - Send a SERVER_QM_DROP_PLAN request to the server
 *
 * Request the server to delete the XASL cache specified by either
 * the query string or the XASL file id. When the client want to delete
 * the old XASL cache entry and update it with new one, this function will be
 * used.
 * This function is a counter part to sqmgr_drop_query_plan().
 */
/*
 * qmgr_drop_query_plan -
 *
 * return:
 *
 *   qstmt(in):
 *   user_oid(in):
 *   xasl_id(in):
 *   delete(in):
 *
 * NOTE:
 */
int
qmgr_drop_query_plan (const char *qstmt, const OID * user_oid,
		      const XASL_ID * xasl_id)
{
#if defined(CS_MODE)
  int status = ER_FAILED;
  int req_error, request_size, strlen;
  char *request, *reply, *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = length_const_string (qstmt, &strlen)
    + OR_OID_SIZE + OR_XASL_ID_SIZE;

  request = (char *) malloc (request_size);
  if (request)
    {
      /* pack query string as a request data */
      ptr = pack_const_string_with_length (request, qstmt, strlen);
      /* pack OID of the current user */
      ptr = or_pack_oid (ptr, user_oid);
      /* pack XASL file id (XASL_ID) */
      OR_PACK_XASL_ID (ptr, xasl_id);

      /* send SERVER_QM_QUERY_DROP_PLAN request with request data;
         receive status code (int) as a reply */
      req_error = net_client_request (NET_SERVER_QM_QUERY_DROP_PLAN,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (!req_error)
	{
	  /* first argument should be status code (int) */
	  (void) or_unpack_int (reply, &status);
	}
      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, request_size);
    }

  return status;
#else /* CS_MODE */
  int status;

  ENTER_SERVER ();

  /* call the server routine of query drop plan */
  status = xqmgr_drop_query_plan (NULL, qstmt, user_oid, xasl_id);

  EXIT_SERVER ();

  return status;
#endif /* !CS_MODE */
}

/*
 * qm_query_drop_all_plan - Send a SERVER_QM_DROP_ALL_PLAN request to the server
 *
 * Request the server to clear all XASL cache entries out. When the client
 * want to delete all cached query plans, this function will be used.
 * This function is a counter part to sqmgr_drop_all_query_plans().
 */
/*
 * qmgr_drop_all_query_plans -
 *
 * return:
 *
 * NOTE:
 */
int
qmgr_drop_all_query_plans (void)
{
#if defined(CS_MODE)
  int status = ER_FAILED;
  int req_error, request_size;
  char *request, *reply;
  OR_ALIGNED_BUF (OR_XASL_ID_SIZE) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;

  request = OR_ALIGNED_BUF_START (a_request);
  request_size = OR_INT_SIZE;
  reply = OR_ALIGNED_BUF_START (a_reply);

  or_pack_int (request, 0);	/* dummy parameter */

  /* send SERVER_QM_QUERY_DROP_ALL_PLANS request with request data;
     receive status code (int) as a reply */
  req_error = net_client_request (NET_SERVER_QM_QUERY_DROP_ALL_PLANS,
				  NULL, 1, 1,
				  request, request_size,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      /* first argument should be status code (int) */
      (void) or_unpack_int (reply, &status);
    }

  return status;
#else /* CS_MODE */
  int status;

  ENTER_SERVER ();

  /* call the server routine of query drop plan */
  status = xqmgr_drop_all_query_plans (NULL);

  EXIT_SERVER ();

  return status;
#endif /* !CS_MODE */
}

/*
 * qmgr_dump_query_plans -
 *
 * return:
 *
 *   outfp(in):
 *
 * NOTE:
 */
void
qmgr_dump_query_plans (FILE * outfp)
{
#if defined(CS_MODE)
  int req_error;

  if (outfp == NULL)
    {
      outfp = stdout;
    }

  req_error = net_client_request_recv_stream (NET_SERVER_QM_QUERY_DUMP_PLANS,
					      NULL, 0, NULL, 0,
					      NULL, 0, outfp);
#else /* CS_MODE */

  ENTER_SERVER ();

  xqmgr_dump_query_plans (NULL, outfp);

  EXIT_SERVER ();
#endif /* !CS_MODE */
}

/*
 * qmgr_get_query_info -
 *
 * return:
 *
 *   query_result(in):
 *   done(in):
 *   count(in):
 *   error(in):
 *   error_string(in):
 *
 * NOTE:
 */
int
qmgr_get_query_info (UNUSED_ARG DB_QUERY_RESULT * query_result,
		     UNUSED_ARG int *done, UNUSED_ARG int *count,
		     UNUSED_ARG int *error, UNUSED_ARG char **error_string)
{
#if defined(CS_MODE)
  int req_error;
  OR_ALIGNED_BUF (OR_PTR_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *area = NULL, *reply, *ptr;
  int area_size;
  CSS_NET_PACKET *recv_packet = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  or_pack_ptr (request, query_result->res.s.query_id);

  req_error = net_client_request (NET_SERVER_QM_GET_QUERY_INFO,
				  &recv_packet, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      int dummy_int;
      const char *dummy_str = NULL;

      area = css_net_packet_get_buffer (recv_packet, 1, -1, false);
      if (area == NULL)
	{
	  css_net_packet_free (recv_packet);
	  return ER_NET_SERVER_DATA_RECEIVE;
	}

      ptr = or_unpack_int (area, &area_size);
      ptr = or_unpack_int (ptr, done);
      ptr = or_unpack_int (ptr, count);
      query_result->res.s.cursor_id.list_id.tuple_cnt = *count;

      /*
       * Have to unpack these fields unconditionally to keep from screwing
       * up others who need to understand the buffer layout.  (Well, I
       * suppose we could blow off the string if we wanted to [since it's
       * last], but sure as we do, someone will add something to the end of
       * this message format and forget about this problem.)
       */
      ptr = or_unpack_int (ptr, error ? error : &dummy_int);
      ptr = or_unpack_string_nocopy (ptr, &dummy_str);
      if (error_string)
	{
	  if (dummy_str == NULL)
	    {
	      *error_string = NULL;
	    }
	  else
	    {
	      /*
	       * we're handing it off to API-land and
	       * making them responsible for freeing it.  In that case, we'd
	       * better copy it into a buffer that can be free'd via ordinary free().
	       */
	      int size;

	      size = strlen (dummy_str) + 1;
	      *error_string = (char *) malloc (size);
	      if (*error_string == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
		  *done = ER_OUT_OF_VIRTUAL_MEMORY;
		}
	      else
		{
		  strcpy (*error_string, dummy_str);
		}
	    }
	}

      css_net_packet_free (recv_packet);

      return *done;
    }

  return req_error;

#else /* CS_MODE */
  /* Cannot run in standalone mode */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NOT_IN_STANDALONE, 1,
	  "query info");
  return ER_FAILED;
#endif /* !CS_MODE */
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qmgr_sync_query -
 *
 * return:
 *
 *   query_result(in):
 *   wait(in):
 *
 * NOTE:
 */
int
qmgr_sync_query (DB_QUERY_RESULT * query_result, int wait)
{
#if defined(CS_MODE)
  /* We are not using ASYNC_EXEC mode. */
  return NO_ERROR;
#else /* CS_MODE */
  /* Cannot run in standalone mode */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NOT_IN_STANDALONE, 1,
	  "query sync");
  return ER_NOT_IN_STANDALONE;
#endif /* !CS_MODE */
}
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * mnt_server_start_stats -
 *
 * return:
 *
 * NOTE:
 */
int
mnt_server_start_stats (bool for_all_trans)
{
#if defined(CS_MODE)
  int status = ER_FAILED;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *request;
  char *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  or_pack_int (request, for_all_trans);
  req_error = net_client_request (NET_SERVER_MNT_SERVER_START_STATS,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_int (reply, &status);
    }

  return (status);
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xmnt_server_start_stats (NULL, for_all_trans);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * mnt_server_stop_stats -
 *
 * return:
 *
 * NOTE:
 */
int
mnt_server_stop_stats (void)
{
#if defined(CS_MODE)
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;	/* need dummy reply message */
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_MNT_SERVER_STOP_STATS,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      return ER_FAILED;
    }
  return NO_ERROR;
#else /* CS_MODE */

  ENTER_SERVER ();

  xmnt_server_stop_stats (NULL);

  EXIT_SERVER ();
  return NO_ERROR;
#endif /* !CS_MODE */
}
#endif

/*
 * mnt_server_copy_stats -
 *
 * return:
 *
 *   to_stats(in):
 *
 * NOTE:
 */
int
mnt_server_copy_stats (MNT_SERVER_EXEC_STATS * to_stats)
{
  int status = NO_ERROR;
#if defined(CS_MODE)
  int req_error;
  OR_ALIGNED_BUF (STAT_SIZE_PACKED) a_reply;
  char *reply;


  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_MNT_SERVER_COPY_STATS,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      net_unpack_stats (reply, to_stats);
    }
  else
    {
      status = ER_FAILED;
    }
#else /* CS_MODE */

  ENTER_SERVER ();

  xmnt_server_copy_stats (NULL, to_stats);

  EXIT_SERVER ();
#endif /* !CS_MODE */

  return status;
}

/*
 * mnt_server_copy_global_stats -
 *
 * return:
 *
 *   to_stats(in):
 *
 * NOTE:
 */
int
mnt_server_copy_global_stats (MNT_SERVER_EXEC_STATS * to_stats)
{
  int status = NO_ERROR;
#if defined(CS_MODE)
  int req_error;
  OR_ALIGNED_BUF (STAT_SIZE_PACKED) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_MNT_SERVER_COPY_GLOBAL_STATS,
				  NULL, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      net_unpack_stats (reply, to_stats);
    }
  else
    {
      status = ER_FAILED;
    }
#else /* CS_MODE */

  ENTER_SERVER ();

  xmnt_server_copy_global_stats (NULL, to_stats);

  EXIT_SERVER ();
#endif /* !CS_MODE */

  return status;
}

/*
 * catalog_is_acceptable_new_representation -
 *
 * return:
 *
 *   class_id(in):
 *   hfid(in):
 *   can_accept(in):
 *
 * NOTE:
 */
int
catalog_is_acceptable_new_representation (OID * class_id, HFID * hfid,
					  int *can_accept)
{
#if defined(CS_MODE)
  int req_error, status, accept;
  OR_ALIGNED_BUF (OR_OID_SIZE + OR_HFID_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_reply;
  char *reply;
  char *ptr;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_oid (request, class_id);
  ptr = or_pack_hfid (ptr, hfid);

  req_error = net_client_request (NET_SERVER_CT_CAN_ACCEPT_NEW_REPR,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));

  if (req_error)
    {
      status = ER_FAILED;
    }
  else
    {
      ptr = or_unpack_int (reply, &status);
      ptr = or_unpack_int (ptr, &accept);
      *can_accept = accept;
    }

  return status;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success =
    xcatalog_is_acceptable_new_representation (NULL, class_id, hfid,
					       can_accept);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}


/*
 * thread_kill_tran_index -
 *
 * return:
 *
 *   kill_tran_index(in):
 *   kill_user(in):
 *   kill_host(in):
 *   kill_pid(in):
 *
 * NOTE:
 */
int
thread_kill_tran_index (UNUSED_ARG int kill_tran_index,
			UNUSED_ARG char *kill_user,
			UNUSED_ARG char *kill_host, UNUSED_ARG int kill_pid)
{
#if defined(CS_MODE)
  int success = ER_FAILED;
  int request_size, strlen1, strlen2;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply, *ptr;
  int req_error;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = OR_INT_SIZE + OR_INT_SIZE
    + length_const_string (kill_user, &strlen1)
    + length_const_string (kill_host, &strlen2);

  request = (char *) malloc (request_size);
  if (request)
    {
      ptr = or_pack_int (request, kill_tran_index);
      ptr = pack_const_string_with_length (ptr, kill_user, strlen1);
      ptr = pack_const_string_with_length (ptr, kill_host, strlen2);
      ptr = or_pack_int (ptr, kill_pid);

      req_error = net_client_request (NET_SERVER_CSS_KILL_TRANSACTION,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));

      if (!req_error)
	{
	  or_unpack_int (reply, &success);
	}

      free_and_init (request);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
    }

  return success;
#else /* CS_MODE */
  er_log_debug (ARG_FILE_LINE,
		"css_kill_client: THIS IS ONLY a C/S function");
  return ER_FAILED;
#endif /* !CS_MODE */
}

/*
 * thread_dump_cs_stat -
 *
 * return:
 *
 *   outfp(in):
 */
void
thread_dump_cs_stat (UNUSED_ARG FILE * outfp)
{
#if defined(CS_MODE)
  int req_error;

  if (outfp == NULL)
    {
      outfp = stdout;
    }

  req_error = net_client_request_recv_stream (NET_SERVER_CSS_DUMP_CS_STAT,
					      NULL, 0, NULL, 0,
					      NULL, 0, outfp);
#else /* CS_MODE */
  er_log_debug (ARG_FILE_LINE,
		"thread_dump_cs_stat: THIS IS ONLY a C/S function");
  return;
#endif /* !CS_MODE */
}

/*
 * thread_dump_server_stat -
 *
 * return:
 *
 *   outfp(in):
 */
void
thread_dump_server_stat (UNUSED_ARG FILE * outfp)
{
#if defined(CS_MODE)
  int req_error;

  if (outfp == NULL)
    {
      outfp = stdout;
    }

  req_error = net_client_request_recv_stream (NET_SERVER_CSS_DUMP_SERVER_STAT,
					      NULL, 0, NULL, 0,
					      NULL, 0, outfp);
#else /* CS_MODE */
  er_log_debug (ARG_FILE_LINE,
		"thread_dump_server_stat: THIS IS ONLY a C/S function");
  return;
#endif /* !CS_MODE */
}

/*
 * logtb_get_pack_tran_table -
 *
 * return:
 *
 *   buffer_p(in):
 *   size_p(in):
 *   include_query_exec_info(in):
 *
 * NOTE:
 */
int
logtb_get_pack_tran_table (char **buffer_p, int *size_p,
			   UNUSED_ARG bool include_query_exec_info)
{
#if defined(CS_MODE)
  int error = NO_ERROR;
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request = OR_ALIGNED_BUF_START (a_request);
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;
  CSS_NET_PACKET *recv_packet = NULL;

  /* --query-exec-info */
  or_pack_int (request, ((include_query_exec_info) ? 1 : 0));

  req_error = net_client_request (NET_SERVER_LOG_GETPACK_TRANTB,
				  &recv_packet, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (req_error)
    {
      error = er_errid ();
    }
  else
    {
      /* first word is buffer size, second is error code */
      ptr = or_unpack_int (reply, size_p);
      ptr = or_unpack_int (ptr, &error);

      *buffer_p = css_net_packet_get_buffer (recv_packet, 1, *size_p, true);

      css_net_packet_free (recv_packet);
    }

  return error;
#else /* CS_MODE */
  int error;

  ENTER_SERVER ();

  error = xlogtb_get_pack_tran_table (NULL, buffer_p, size_p, 0);

  EXIT_SERVER ();

  return error;
#endif /* !CS_MODE */
}

/*
 * logtb_free_trans_info - Free transaction table information
 *   return: none
 *   info(in): TRANS_INFO to be freed
 */
void
logtb_free_trans_info (TRANS_INFO * info)
{
  int i;

  if (info == NULL)
    {
      return;
    }

  for (i = 0; i < info->num_trans; i++)
    {
      if (info->tran[i].db_user != NULL)
	{
	  free_and_init (info->tran[i].db_user);
	}
      if (info->tran[i].program_name != NULL)
	{
	  free_and_init (info->tran[i].program_name);
	}
      if (info->tran[i].login_name != NULL)
	{
	  free_and_init (info->tran[i].login_name);
	}
      if (info->tran[i].host_name != NULL)
	{
	  free_and_init (info->tran[i].host_name);
	}

      if (info->include_query_exec_info)
	{
	  if (info->tran[i].query_exec_info.query_stmt)
	    {
	      free_and_init (info->tran[i].query_exec_info.query_stmt);
	    }
	  if (info->tran[i].query_exec_info.wait_for_tran_index_string)
	    {
	      free_and_init (info->tran[i].query_exec_info.
			     wait_for_tran_index_string);
	    }
	  if (info->tran[i].query_exec_info.sql_id)
	    {
	      free_and_init (info->tran[i].query_exec_info.sql_id);
	    }
	}
      else
	{
	  assert_release (info->tran[i].query_exec_info.query_stmt == NULL);
	  assert_release (info->tran[i].query_exec_info.
			  wait_for_tran_index_string == NULL);
	}
    }
  free_and_init (info);
}

/*
 * logtb_get_trans_info - Get transaction table information which identifies
 *                        active transactions
 * include_query_exec_info(in) :
 *   return: TRANS_INFO array or NULL
 */
TRANS_INFO *
logtb_get_trans_info (bool include_query_exec_info)
{
  TRANS_INFO *info = NULL;
  char *buffer, *ptr;
  int num_trans, bufsize, i;
  int error;

  error =
    logtb_get_pack_tran_table (&buffer, &bufsize, include_query_exec_info);
  if (error != NO_ERROR || buffer == NULL)
    {
      return NULL;
    }

  ptr = buffer;
  ptr = or_unpack_int (ptr, &num_trans);

#if 1
  assert (num_trans >= 0);
#else
  if (num_trans == 0)
    {
      /* can't happen, there must be at least one transaction */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      goto error;
    }
#endif

  i = sizeof (TRANS_INFO) + (MAX (num_trans - 1, 0) * sizeof (ONE_TRAN_INFO));
  info = (TRANS_INFO *) malloc (i);
  if (info == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      i);
      goto error;
    }
  memset (info, '\0', i);

  info->num_trans = num_trans;
  info->include_query_exec_info = include_query_exec_info;
  for (i = 0; i < num_trans; i++)
    {
      if ((ptr = or_unpack_int (ptr, &info->tran[i].tran_index)) == NULL
	  || (ptr = or_unpack_int (ptr, &info->tran[i].state)) == NULL
	  || (ptr = or_unpack_int (ptr, &info->tran[i].process_id)) == NULL
	  || (ptr = or_unpack_string (ptr, &info->tran[i].db_user)) == NULL
	  || (ptr =
	      or_unpack_string (ptr, &info->tran[i].program_name)) == NULL
	  || (ptr = or_unpack_string (ptr, &info->tran[i].login_name)) == NULL
	  || (ptr = or_unpack_string (ptr, &info->tran[i].host_name)) == NULL)
	{
	  goto error;
	}

      if (include_query_exec_info)
	{
	  if ((ptr = or_unpack_float (ptr, &info->tran[i].query_exec_info.
				      query_time)) == NULL
	      || (ptr = or_unpack_float (ptr, &info->tran[i].query_exec_info.
					 tran_time)) == NULL
	      || (ptr = or_unpack_string (ptr, &info->tran[i].query_exec_info.
					  wait_for_tran_index_string)) == NULL
	      || (ptr = or_unpack_string (ptr, &info->tran[i].query_exec_info.
					  query_stmt)) == NULL
	      || (ptr = or_unpack_string (ptr, &info->tran[i].query_exec_info.
					  sql_id)) == NULL)
	    {
	      goto error;
	    }
	  OR_UNPACK_XASL_ID (ptr, &info->tran[i].query_exec_info.xasl_id);
	}
    }

  if (((int) (ptr - buffer)) != bufsize)
    {
      /* unpacking didn't match size, garbage */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      goto error;
    }

  free_and_init (buffer);

  return info;

error:
  if (buffer != NULL)
    {
      free_and_init (buffer);
    }

  if (info != NULL)
    {
      logtb_free_trans_info (info);
    }

  return NULL;
}

/*
 * logtb_dump_trantable -
 *
 * return:
 *
 *   outfp(in):
 */
void
logtb_dump_trantable (FILE * outfp)
{
#if defined(CS_MODE)
  int req_error;

  if (outfp == NULL)
    {
      outfp = stdout;
    }

  req_error = net_client_request_recv_stream (NET_SERVER_LOG_DUMP_TRANTB,
					      NULL, 0, NULL, 0,
					      NULL, 0, outfp);
#else /* CS_MODE */

  ENTER_SERVER ();

  xlogtb_dump_trantable (NULL, outfp);

  EXIT_SERVER ();
#endif /* !CS_MODE */
}

/*
 * heap_get_class_num_objects_pages -
 *
 * return:
 *
 *   hfid(in):
 *   approximation(in):
 *   nobjs(in):
 *   npages(in):
 *
 * NOTE:
 */
int
heap_get_class_num_objects_pages (HFID * hfid, int approximation,
				  DB_BIGINT * nobjs, int *npages)
{
#if defined(CS_MODE)
  int req_error, status = ER_FAILED, num_pages;
  DB_BIGINT num_objs;
  OR_ALIGNED_BUF (OR_HFID_SIZE + OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2 + OR_INT64_SIZE) a_reply;
  char *reply;
  char *ptr;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_hfid (request, hfid);
  ptr = or_pack_int (ptr, approximation);

  req_error = net_client_request (NET_SERVER_HEAP_GET_CLASS_NOBJS_AND_NPAGES,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      ptr = or_unpack_int64 (reply, &num_objs);
      ptr = or_unpack_int (ptr, &status);
      ptr = or_unpack_int (ptr, &num_pages);
      *nobjs = num_objs;
      *npages = (int) num_pages;
    }

  return status;
#else /* CS_MODE */
  int success = ER_FAILED;

  ENTER_SERVER ();

  success = xheap_get_class_num_objects_pages (NULL, hfid, approximation,
					       nobjs, npages);

  EXIT_SERVER ();

  return success;
#endif /* !CS_MODE */
}

/*
 * qp_get_server_info -
 *
 * return:
 *
 *   server_info(in):
 *
 * NOTE:
 */
int
qp_get_server_info (SERVER_INFO * server_info)
{
#if defined(CS_MODE)
  int req_error, status = ER_FAILED;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_reply;
  char *reply;
  char *ptr, *area = NULL;
  int val_size, i;
  CSS_NET_PACKET *recv_packet = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_int (request, server_info->info_bits);

  req_error = net_client_request (NET_SERVER_QPROC_GET_SERVER_INFO,
				  &recv_packet, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));

  if (!req_error)
    {
      ptr = or_unpack_int (reply, &val_size);
      ptr = or_unpack_int (ptr, &status);
      if (status == NO_ERROR)
	{
	  area = css_net_packet_get_buffer (recv_packet, 1, val_size, false);
	  if (area == NULL)
	    {
	      status = ER_NET_SERVER_DATA_RECEIVE;
	    }
	  else
	    {
	      ptr = area;
	      for (i = 0; i < SI_CNT; i++)
		{
		  if (server_info->info_bits & (1 << i))
		    {
		      ptr = or_unpack_db_value (ptr, server_info->value[i]);
		    }
		}
	    }
	}
      css_net_packet_free (recv_packet);
    }

  return status;
#else /* CS_MODE */
  int success = NO_ERROR;
  int i;

  ENTER_SERVER ();

  for (i = 0; i < SI_CNT && success == NO_ERROR; i++)
    {
      if (server_info->info_bits & (1 << i))
	{
	  switch (1 << i)
	    {
	    case SI_SYS_DATETIME:
	      success = db_sys_datetime (server_info->value[i]);
	      break;
	    default:
	      break;
	    }
	}
    }

  EXIT_SERVER ();
  return success;
#endif /* !CS_MODE */
}

/*
 * sysprm_change_server_parameters () - Sends a list of assignments to server
 *					in order to change system parameter
 *					values.
 *
 * return	    : SYSPRM_ERR code.
 * assignments (in) : list of assignments.
 */
int
sysprm_change_server_parameters (const SYSPRM_ASSIGN_VALUE * assignments)
{
#if defined(CS_MODE)
  int rc = PRM_ERR_COMM_ERR;
  int request_size = 0, req_error = NO_ERROR;
  char *request = NULL, *reply = NULL;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = sysprm_packed_assign_values_length (assignments, 0);
  request = (char *) malloc (request_size);
  if (request)
    {
      (void) sysprm_pack_assign_values (request, assignments);
      req_error = net_client_request (NET_SERVER_PRM_SET_PARAMETERS,
				      NULL, 1, 1,
				      request, request_size,
				      reply, OR_ALIGNED_BUF_SIZE (a_reply));
      if (req_error == NO_ERROR)
	{
	  or_unpack_int (reply, &rc);
	}
      else
	{
	  rc = PRM_ERR_COMM_ERR;
	}

      free_and_init (request);
    }
  else
    {
      rc = PRM_ERR_NO_MEM_FOR_PRM;
    }
  return rc;
#else /* CS_MODE */
  ENTER_SERVER ();
  xsysprm_change_server_parameters (assignments);
  EXIT_SERVER ();

  return PRM_ERR_NO_ERROR;
#endif /* !CS_MODE */
}

/*
 * sysprm_obtain_server_parameters () - Obtain values for system parameters
 *					from server.
 *
 * return		   : SYSPRM_ERR code.
 * prm_values_ptr (in/out) : list of parameter values.
 */
int
sysprm_obtain_server_parameters (SYSPRM_ASSIGN_VALUE ** prm_values_ptr)
{
#if defined(CS_MODE)
  int rc = PRM_ERR_COMM_ERR;
  int req_error = NO_ERROR, request_size = 0, receive_size = 0;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_reply;
  char *reply = NULL, *request_data = NULL, *receive_data = NULL;
  char *ptr = NULL;
  SYSPRM_ASSIGN_VALUE *updated_prm_values = NULL;
  CSS_NET_PACKET *recv_packet = NULL;

  assert (prm_values_ptr != NULL && *prm_values_ptr != NULL);

  reply = OR_ALIGNED_BUF_START (a_reply);

  request_size = sysprm_packed_assign_values_length (*prm_values_ptr, 0);
  request_data = (char *) malloc (request_size);
  if (request_data == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      request_size);
      return PRM_ERR_NO_MEM_FOR_PRM;
    }

  (void) sysprm_pack_assign_values (request_data, *prm_values_ptr);
  req_error = net_client_request (NET_SERVER_PRM_GET_PARAMETERS,
				  &recv_packet, 1, 1,
				  request_data, request_size,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (req_error != NO_ERROR)
    {
      rc = PRM_ERR_COMM_ERR;
    }
  else
    {
      ptr = or_unpack_int (reply, &receive_size);
      ptr = or_unpack_int (ptr, &rc);

      receive_data = css_net_packet_get_buffer (recv_packet, 1, receive_size,
						false);

      if (rc != PRM_ERR_NO_ERROR || receive_data == NULL)
	{
	  goto cleanup;
	}

      (void) sysprm_unpack_assign_values (receive_data, &updated_prm_values);

      /* free old values */
      sysprm_free_assign_values (prm_values_ptr);
      /* update values */
      *prm_values_ptr = updated_prm_values;
    }

cleanup:
  if (request_data != NULL)
    {
      free_and_init (request_data);
    }

  css_net_packet_free (recv_packet);

  return rc;
#else /* CS_MODE */
  ENTER_SERVER ();
  xsysprm_obtain_server_parameters (*prm_values_ptr);
  EXIT_SERVER ();

  return PRM_ERR_NO_ERROR;
#endif /* !CS_MODE */
}

/*
 * sysprm_get_force_server_parameters () - Get from server values for system
 *					   parameters marked with
 *					   PRM_FORCE_SERVER flag.
 *
 * return	       : error code
 * change_values (out) : list of parameter values.
 */
int
sysprm_get_force_server_parameters (SYSPRM_ASSIGN_VALUE ** change_values)
{
#if defined (CS_MODE)
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_reply;
  int area_size, error;
  char *reply = NULL, *area = NULL, *ptr = NULL;
  CSS_NET_PACKET *recv_packet = NULL;

  assert (change_values != NULL);
  *change_values = NULL;

  reply = OR_ALIGNED_BUF_START (a_reply);

  req_error = net_client_request (NET_SERVER_PRM_GET_FORCE_PARAMETERS,
				  &recv_packet, 0, 1,
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (req_error != NO_ERROR)
    {
      error = req_error;
      goto error;
    }

  ptr = or_unpack_int (reply, &area_size);
  ptr = or_unpack_int (ptr, &error);
  if (error != NO_ERROR)
    {
      goto error;
    }

  area = css_net_packet_get_buffer (recv_packet, 1, area_size, false);

  if (area != NULL)
    {
      (void) sysprm_unpack_assign_values (area, change_values);
    }

  css_net_packet_free (recv_packet);

  return NO_ERROR;

error:
  css_net_packet_free (recv_packet);

  return error;
#else /* CS_MODE */
  assert (change_values != NULL);
  *change_values = NULL;
  return NO_ERROR;
#endif /* !CS_MODE */
}

/*
 * sysprm_dump_server_parameters -
 *
 * return:
 */
void
sysprm_dump_server_parameters (FILE * outfp)
{
#if defined(CS_MODE)
  int req_error;

  if (outfp == NULL)
    {
      outfp = stdout;
    }

  req_error = net_client_request_recv_stream (NET_SERVER_PRM_DUMP_PARAMETERS,
					      NULL, 0, NULL, 0,
					      NULL, 0, outfp);
#else /* CS_MODE */
  ENTER_SERVER ();

  xsysprm_dump_server_parameters (outfp);

  EXIT_SERVER ();
#endif /* !CS_MODE */
}

/*
 * repl_log_get_eof_lsa -
 *
 * return:
 *
 *   lsa(in):
 *
 * NOTE:
 */
int
repl_log_get_eof_lsa (LOG_LSA * lsa)
{
#if defined(CS_MODE)
  int req_error, success = ER_FAILED;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  OR_ALIGNED_BUF (OR_LOG_LSA_ALIGNED_SIZE) a_reply;
  char *reply;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);
  req_error = net_client_request (NET_SERVER_REPL_LOG_GET_EOF_LSA,
				  NULL, 1, 1,
				  request, OR_ALIGNED_BUF_SIZE (a_request),
				  reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (!req_error)
    {
      or_unpack_log_lsa (reply, lsa);
      success = NO_ERROR;
    }

  return success;
#else /* CS_MODE */
  LOG_LSA tmp_lsa;
  int r = ER_FAILED;

  ENTER_SERVER ();
  xrepl_log_get_eof_lsa (NULL, &tmp_lsa);
  if (lsa)
    {
      LSA_COPY (lsa, &tmp_lsa);
      r = NO_ERROR;
    }
  else
    {
      r = ER_FAILED;
    }
  EXIT_SERVER ();

  return r;
#endif /* !CS_MODE */
}

/*
 * repl_set_info -
 *
 * return:
 *
 *   repl_info(in):
 *
 * NOTE:
 */
int
repl_set_info (REPL_INFO * repl_info)
{
#if defined(CS_MODE)
  int req_error, success = ER_FAILED;
  int request_size = 0, strlen1, strlen2, strlen3;
  char *request = NULL, *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;
  REPL_INFO_SCHEMA *repl_schema;

  reply = OR_ALIGNED_BUF_START (a_reply);

  switch (repl_info->repl_info_type)
    {
    case REPL_INFO_TYPE_SCHEMA:
      repl_schema = (REPL_INFO_SCHEMA *) repl_info->info;
      request_size = OR_INT_SIZE	/* REPL_INFO.REPL_INFO_TYPE */
	+ OR_INT_SIZE		/* REPL_INFO_SCHEMA.statement_type */
	+ OR_INT_SIZE		/* REPL_INFO_SCHEMA.online_ddl_type */
	+ length_const_string (repl_schema->name, &strlen1)
	+ length_const_string (repl_schema->ddl, &strlen2)
	+ length_const_string (repl_schema->db_user, &strlen3);

      request = (char *) malloc (request_size);
      if (request)
	{
	  ptr = or_pack_int (request, REPL_INFO_TYPE_SCHEMA);
	  ptr = or_pack_int (ptr, repl_schema->statement_type);
	  ptr = or_pack_int (ptr, repl_schema->online_ddl_type);
	  ptr = pack_const_string_with_length (ptr, repl_schema->name,
					       strlen1);
	  ptr = pack_const_string_with_length (ptr, repl_schema->ddl,
					       strlen2);
	  ptr = pack_const_string_with_length (ptr, repl_schema->db_user,
					       strlen3);

	  req_error = net_client_request (NET_SERVER_REPL_INFO, NULL, 1, 1,
					  request, request_size, reply,
					  OR_ALIGNED_BUF_SIZE (a_reply));
	  if (!req_error)
	    {
	      or_unpack_int (reply, &success);
	    }
	  free_and_init (request);
	}
      break;

    default:
      break;
    }

  return success;
#else /* CS_MODE */
  int r = ER_FAILED;

  ENTER_SERVER ();
  r = xrepl_set_info (NULL, repl_info);
  EXIT_SERVER ();
  return r;
#endif /* !CS_MODE */
}

/*
 * rbl_get_log_pages -
 *
 * return:
 *
 *   page_id(in):
 *
 * NOTE:
 */
int
rbl_get_log_pages (UNUSED_ARG RBL_SYNC_CONTEXT * ctx_ptr)
{
#if defined(CS_MODE)
  OR_ALIGNED_BUF (OR_INT64_SIZE + OR_INT_SIZE) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *request, *reply;
  char *ptr;
  int compressed_protocol;
  int error = NO_ERROR;

  if (prm_get_bool_value (PRM_ID_MIGRATOR_COMPRESSED_PROTOCOL))
    {
      compressed_protocol = 1;
    }
  else
    {
      compressed_protocol = 0;
    }

  er_log_debug (ARG_FILE_LINE,
		"rbl_get_log_pages, lpageid(%lld), compressed_protocol(%d)",
		ctx_ptr->last_recv_pageid, compressed_protocol);

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_int64 (request, ctx_ptr->last_recv_pageid);
  ptr = or_pack_int (ptr, compressed_protocol);

  error =
    net_client_request_with_rbl_context (ctx_ptr,
					 request,
					 OR_ALIGNED_BUF_SIZE (a_request),
					 reply,
					 OR_ALIGNED_BUF_SIZE (a_reply));

  if (error != NO_ERROR)
    {
      if (error == ER_NET_SERVER_CRASHED)
	{
	  ctx_ptr->shutdown = true;
	}
      else if (error == ER_HA_LW_FAILED_GET_LOG_PAGE)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  ctx_ptr->last_recv_pageid);
	  ctx_ptr->shutdown = true;
	}
    }

  return error;

#else /* CS_MODE */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NOT_IN_STANDALONE, 1,
	  "copylog database");
  return ER_NOT_IN_STANDALONE;
#endif /* !CS_MODE */
}

bool
histo_is_supported (void)
{
  return prm_get_bool_value (PRM_ID_ENABLE_HISTO);
}

int
histo_start (bool for_all_trans)
{
#if defined (CS_MODE)
  return net_histo_start (for_all_trans);
#else /* CS_MODE */
  return mnt_start_stats (for_all_trans);
#endif /* !CS_MODE */
}

int
histo_stop (void)
{
#if defined (CS_MODE)
  return net_histo_stop ();
#else /* CS_MODE */
  return mnt_stop_stats ();
#endif /* !CS_MODE */
}

void
histo_print (FILE * stream)
{
#if defined (CS_MODE)
  net_histo_print (stream);
#else /* CS_MODE */
  mnt_print_stats (stream);
#endif /* !CS_MODE */
}

void
histo_print_global_stats (FILE * stream, bool cumulative, const char *substr,
			  const char *db_name)
{
#if defined (CS_MODE)
  net_histo_print_global_stats (stream, cumulative, substr, db_name);
#else /* CS_MODE */
  mnt_print_global_stats (stream, cumulative, substr, db_name);
#endif /* !CS_MODE */
}

void
histo_clear (void)
{
#if defined (CS_MODE)
  net_histo_clear ();
#else /* CS_MODE */
  mnt_reset_stats ();
#endif /* !CS_MODE */
}

/*
 * boot_get_server_locales () - get information about server locales
 *
 * return : error code or no error
 */
int
boot_get_server_locales (UNUSED_ARG LANG_COLL_COMPAT ** server_collations,
			 UNUSED_ARG LANG_LOCALE_COMPAT ** server_locales,
			 UNUSED_ARG int *server_coll_cnt,
			 UNUSED_ARG int *server_locales_cnt)
{
#if defined(CS_MODE)
  int req_error;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_reply;
  char *reply = NULL;
  char *reply_data = NULL;
  char *ptr = NULL;
  int reply_size = 0, temp_int, i;
  const char *temp_str;
  size_t size;
  CSS_NET_PACKET *recv_packet = NULL;

  assert (server_collations != NULL);
  assert (server_locales != NULL);
  assert (server_coll_cnt != NULL);
  assert (server_locales_cnt != NULL);

  *server_collations = NULL;
  *server_locales = NULL;
  *server_coll_cnt = 0;
  *server_locales_cnt = 0;

  req_error = net_client_request (NET_SERVER_BO_GET_LOCALES_INFO,
				  &recv_packet, 0, 1,
				  OR_ALIGNED_BUF_START (a_reply),
				  OR_ALIGNED_BUF_SIZE (a_reply));
  if (req_error != NO_ERROR)
    {
      goto error;
    }

  reply = OR_ALIGNED_BUF_START (a_reply);
  /* unpack data size */
  ptr = or_unpack_int (reply, &reply_size);
  /* unpack error code */
  ptr = or_unpack_int (ptr, &req_error);
  if (req_error != NO_ERROR)
    {
      goto error;
    }

  reply_data = css_net_packet_get_buffer (recv_packet, 1, reply_size, false);
  if (reply_data == NULL)
    {
      req_error = ER_NET_SERVER_DATA_RECEIVE;
      goto error;
    }

  ptr = or_unpack_int (reply_data, server_coll_cnt);
  ptr = or_unpack_int (ptr, server_locales_cnt);

  size = (*server_coll_cnt * sizeof (LANG_COLL_COMPAT));
  *server_collations = (LANG_COLL_COMPAT *) malloc (size);
  if (*server_collations == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      size);
      req_error = ER_FAILED;
      goto error;
    }

  size = (*server_locales_cnt * sizeof (LANG_LOCALE_COMPAT));
  *server_locales = (LANG_LOCALE_COMPAT *) malloc (size);
  if (*server_locales == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      size);
      req_error = ER_FAILED;
      goto error;
    }

  for (i = 0; i < *server_coll_cnt; i++)
    {
      LANG_COLL_COMPAT *ref_coll = &((*server_collations)[i]);

      ptr = or_unpack_int (ptr, &(ref_coll->coll_id));

      ptr = or_unpack_string_nocopy (ptr, &(temp_str));
      strncpy (ref_coll->coll_name, temp_str,
	       sizeof (ref_coll->coll_name) - 1);
      ref_coll->coll_name[sizeof (ref_coll->coll_name) - 1] = '\0';

      ptr = or_unpack_int (ptr, &(temp_int));
      ref_coll->codeset = (INTL_CODESET) temp_int;

      ptr = or_unpack_string_nocopy (ptr, &(temp_str));
      strncpy (ref_coll->checksum, temp_str, sizeof (ref_coll->checksum) - 1);
      ref_coll->checksum[sizeof (ref_coll->checksum) - 1] = '\0';
    }

  for (i = 0; i < *server_locales_cnt; i++)
    {
      LANG_LOCALE_COMPAT *ref_loc = &((*server_locales)[i]);

      ptr = or_unpack_string_nocopy (ptr, &(temp_str));
      strncpy (ref_loc->lang_name, temp_str, sizeof (ref_loc->lang_name) - 1);
      ref_loc->lang_name[sizeof (ref_loc->lang_name) - 1] = '\0';

      ptr = or_unpack_int (ptr, &(temp_int));
      ref_loc->codeset = (INTL_CODESET) temp_int;

      ptr = or_unpack_string_nocopy (ptr, &(temp_str));
      strncpy (ref_loc->checksum, temp_str, sizeof (ref_loc->checksum) - 1);
      ref_loc->checksum[sizeof (ref_loc->checksum) - 1] = '\0';
    }


  css_net_packet_free (recv_packet);

  return req_error;

error:
  css_net_packet_free (recv_packet);

  return req_error;
#else
  return -1;
#endif
}

/*
 * locator_lock_system_ddl_lock -
 *
 * return:
 *
 * NOTE:
 */
int
locator_lock_system_ddl_lock (void)
{
#if defined(CS_MODE)
  int error = NO_ERROR;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply;

  reply = OR_ALIGNED_BUF_START (a_reply);

  error = net_client_request (NET_SERVER_LC_LOCK_SYSTEM_DDL_LOCK,
			      NULL, 0, 1,
			      reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (error == NO_ERROR)
    {
      (void) or_unpack_int (reply, &error);
    }

  return error;
#else /* CS_MODE */
  int error = NO_ERROR;

  ENTER_SERVER ();

  error = xlock_system_ddl_lock (NULL);

  EXIT_SERVER ();

  return error;
#endif /* !CS_MODE */
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lock_change_class_xlock_to_ulock -
 *
 * return:
 *
 *  class_oid(in):
 */
int
lock_change_class_xlock_to_ulock (OID * class_oid)
{
#if defined(CS_MODE)
  OR_ALIGNED_BUF (OR_OID_SIZE) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *request, *reply;
  int error = NO_ERROR;

  assert (class_oid != NULL && !OID_ISNULL (class_oid));

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  or_pack_oid (request, class_oid);

  error = net_client_request (NET_SERVER_LC_LOCK_CHANGE_CLASS_XLOCK_TO_ULOCK,
			      NULL, 1, 1,
			      request, OR_ALIGNED_BUF_SIZE (a_request),
			      reply, OR_ALIGNED_BUF_SIZE (a_reply));
  if (error == NO_ERROR)
    {
      (void) or_unpack_int (reply, &error);
    }

  return error;
#else /* CS_MODE */
  return NO_ERROR;
#endif /* !CS_MODE */
}
#endif

#if defined(CS_MODE)
static QFILE_LIST_ID *
get_list_id_from_execute_res (int replydata_size_listid,
			      char *replydata_listid,
			      int replydata_size_page, char *replydata_page)
{
  QFILE_LIST_ID *list_id = NULL;

  if (replydata_size_listid)
    {
      if (replydata_listid == NULL ||
	  (replydata_size_page > 0 && replydata_page == NULL))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_DATA_TRUNCATED, 0);
	}
      else
	{

	  /* unpack list file id of query result from the reply data */
	  or_unpack_unbound_listid (replydata_listid, (void **) (&list_id));

	  if (list_id != NULL)
	    {
	      /* QFILE_LIST_ID shipped with last page */
	      if (replydata_size_page)
		{
		  list_id->last_pgptr = replydata_page;
		  replydata_page = NULL;
		}
	      else
		{
		  list_id->last_pgptr = NULL;
		}
	    }
	}
    }

  if (replydata_page != NULL)
    {
      free_and_init (replydata_page);
    }

  return list_id;
}
#endif

#if defined(CS_MODE)
static int
make_copyarea_from_server_reply (int num_objs,
				 int packed_desc_size,
				 int content_size,
				 char *reply_packed_desc,
				 char *reply_content_ptr,
				 LC_COPYAREA ** reply_copy_area)
{
  char *content_ptr = NULL;
  char *packed_desc = NULL;

  assert (reply_copy_area != NULL);

  if ((packed_desc_size <= 0 && content_size <= 0) || reply_copy_area == NULL)
    {
      return NO_ERROR;
    }

  if ((packed_desc_size > 0 && reply_packed_desc == NULL) ||
      (content_size > 0 && reply_content_ptr == NULL))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_SERVER_DATA_RECEIVE,
	      0);
      return ER_NET_SERVER_DATA_RECEIVE;
    }

  *reply_copy_area = locator_recv_allocate_copyarea (num_objs,
						     &packed_desc,
						     packed_desc_size,
						     &content_ptr,
						     content_size);
  if (*reply_copy_area == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_NET_CANT_ALLOC_BUFFER, 0);
      return ER_NET_CANT_ALLOC_BUFFER;
    }

  if (packed_desc != NULL && packed_desc_size > 0)
    {
      /** TODO: do not allocate packed_desc */

      memcpy (packed_desc, reply_packed_desc, packed_desc_size);

      locator_unpack_copy_area_descriptor (num_objs,
					   *reply_copy_area, packed_desc);
    }

  if (content_size > 0)
    {
      memcpy (content_ptr, reply_content_ptr, content_size);
    }

  if (packed_desc != NULL)
    {
      free_and_init (packed_desc);
    }

  return (NO_ERROR);
}
#endif

/*
 * logtb_update_group_id -
 *
 * return:
 */
int
logtb_update_group_id (UNUSED_ARG int migrator_id, UNUSED_ARG int group_id,
		       UNUSED_ARG int target, UNUSED_ARG int on_off)
{
#if defined(CS_MODE)
  OR_ALIGNED_BUF (OR_INT_SIZE * 4) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *request, *reply, *ptr;
  int error;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_int (request, migrator_id);
  ptr = or_pack_int (ptr, group_id);
  ptr = or_pack_int (ptr, target);
  ptr = or_pack_int (ptr, on_off);

  error = net_client_request (NET_SERVER_UPDATE_GROUP_ID, NULL, 1, 1,
			      request, OR_ALIGNED_BUF_SIZE (a_request),
			      reply, OR_ALIGNED_BUF_SIZE (a_reply));

  if (error == NO_ERROR)
    {
      or_unpack_int (reply, &error);
    }

  return error;
#else
  return NO_ERROR;
#endif
}

/*
 * logtb_block_globl_dml -
 *
 * return:
 */
int
logtb_block_globl_dml (UNUSED_ARG int start_or_end)
{
#if defined(CS_MODE)
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *request, *reply, *ptr;
  int error;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_int (request, start_or_end);

  error = net_client_request (NET_SERVER_BLOCK_GLOBAL_DML, NULL, 1, 1,
			      request, OR_ALIGNED_BUF_SIZE (a_request),
			      reply, OR_ALIGNED_BUF_SIZE (a_reply));

  if (error == NO_ERROR)
    {
      or_unpack_int (reply, &error);
    }

  return error;
#else
  return NO_ERROR;
#endif
}

/*
 * bk_prepare_backup -
 *
 * return:
 *
 *   num_threads(in):
 *   do_compress(in):
 *   sleep_msecs(in):
 *
 * NOTE:
 */
int
bk_prepare_backup (UNUSED_ARG int num_threads, UNUSED_ARG int do_compress,
		   UNUSED_ARG int sleep_msecs,
		   UNUSED_ARG int make_slave,
		   UNUSED_ARG BK_BACKUP_SESSION * session)
{
#if defined(CS_MODE)
  OR_ALIGNED_BUF (OR_INT_SIZE * 4) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *request, *reply, *ptr, *area, *str;
  int error, area_size, tmp_val;
  CSS_NET_PACKET *recv_packet = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_int (request, num_threads);
  ptr = or_pack_int (ptr, do_compress);
  ptr = or_pack_int (ptr, sleep_msecs);
  ptr = or_pack_int (ptr, make_slave);

  error = net_client_request (NET_SERVER_BK_PREPARE_BACKUP,
			      &recv_packet, 1, 1,
			      request, OR_ALIGNED_BUF_SIZE (a_request), reply,
			      OR_ALIGNED_BUF_SIZE (a_reply));

  if (error == NO_ERROR)
    {
      or_unpack_int (reply, &area_size);

      area = css_net_packet_get_buffer (recv_packet, 1, area_size, false);

      if (area != NULL && area_size > 0)
	{
	  ptr = or_unpack_int (area, &session->bkuphdr->iopageid);
	  ptr = or_unpack_string (ptr, &str);
	  strcpy (session->bkuphdr->bk_magic, str);
	  free_and_init (str);

	  ptr = or_unpack_version (ptr, &session->bkuphdr->bk_db_version);
	  ptr = or_unpack_int (ptr, &session->bkuphdr->bk_hdr_version);
	  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
	  ptr = or_unpack_int64 (ptr, &session->bkuphdr->db_creation);
	  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
	  ptr = or_unpack_int64 (ptr, &session->bkuphdr->start_time);

	  ptr = or_unpack_string (ptr, &str);
	  strcpy (session->bkuphdr->db_name, str);
	  free_and_init (str);

	  ptr = or_unpack_int (ptr, &tmp_val);
	  session->bkuphdr->db_iopagesize = (PGLENGTH) tmp_val;
	  ptr = or_unpack_log_lsa (ptr, &session->bkuphdr->chkpt_lsa);
	  ptr = or_unpack_int (ptr, &session->bkuphdr->bkpagesize);
	  ptr = or_unpack_int (ptr, &session->first_arv_needed);
	  ptr = or_unpack_int (ptr, &tmp_val);
	  session->saved_run_nxchkpt_atpageid = (LOG_PAGEID) tmp_val;
	  LSA_SET_NULL (&(session->bkuphdr->backuptime_lsa));
	  session->bkuphdr->end_time = -1;
	  ptr = or_unpack_int (ptr, &session->num_perm_vols);
	}

      css_net_packet_free (recv_packet);
    }

  return NO_ERROR;
#else /* CS_MODE */
  return NO_ERROR;
#endif /* !CS_MODE */
}

/*
 * bk_backup_volume -
 *
 * return:
 */
int
bk_backup_volume (UNUSED_ARG BK_BACKUP_SESSION * session)
{
#if defined(CS_MODE)
  unsigned int eid;
  int error, size, unzip_nbytes;
  char *request, *reply, *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  OR_ALIGNED_BUF (BK_PACKET_HDR_SIZE) a_reply;
  BK_PACKET_TYPE packet_type;
  CSS_NET_PACKET *recv_packet = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  (void) or_pack_int (request, session->bkup.iosize);

  error = net_client_request_send_msg (&eid, NET_SERVER_BK_BACKUP_VOLUME,
				       1, request,
				       OR_ALIGNED_BUF_SIZE (a_request));
  if (error != NO_ERROR)
    {
      goto end;
    }

  while (error == NO_ERROR)
    {
      error = net_client_request_recv_msg (NULL, eid, -1, 1,
					   reply,
					   OR_ALIGNED_BUF_SIZE (a_reply));
      if (error != NO_ERROR)
	{
	  goto end;
	}

      ptr = or_unpack_int (reply, (int *) &packet_type);
      ptr = or_unpack_int (ptr, &unzip_nbytes);

      if (packet_type == BK_PACKET_VOLS_BACKUP_END)
	{
	  break;
	}
      else if (packet_type == BK_PACKET_VOL_END)
	{
	  bk_end_vol_in_backup (session);
	  continue;
	}

      error = net_client_request_recv_msg (&recv_packet, eid, -1, 1,
					   session->bkup.buffer,
					   session->bkup.iosize);
      if (error != NO_ERROR)
	{
	  goto end;
	}

      size = css_net_packet_get_recv_size (recv_packet, 0);

      if (size > session->bkup.iosize)
	{
	  error = ER_NET_DATASIZE_MISMATCH;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  error, 2, session->bkup.iosize, size);
	  goto end;
	}

      if (packet_type == BK_PACKET_VOL_START)
	{
	  bk_start_vol_in_backup (session, 0);
	  if (session->bkuphdr->make_slave == true)
	    {
	      css_net_packet_free (recv_packet);
	      recv_packet = NULL;

	      continue;
	    }
	}

      error = bk_write_backup (session, size, unzip_nbytes);
      if (error != NO_ERROR)
	{
	  goto end;
	}

      css_net_packet_free (recv_packet);
      recv_packet = NULL;
    }

end:

  if (recv_packet != NULL)
    {
      css_net_packet_free (recv_packet);
    }

  return error;
#else /* CS_MODE */
  return NO_ERROR;
#endif /* !CS_MODE */
}

/*
 * bk_backup_log_volume -
 *
 * return:
 */
int
bk_backup_log_volume (UNUSED_ARG BK_BACKUP_SESSION * session,
		      UNUSED_ARG int delete_unneeded_logarchives)
{
#if defined(CS_MODE)
  unsigned int eid;
  int error, size, unzip_nbytes;
  char *request, *reply, *ptr;
  OR_ALIGNED_BUF (OR_INT_SIZE * 2) a_request;
  OR_ALIGNED_BUF (BK_PACKET_HDR_SIZE) a_reply;
  BK_PACKET_TYPE packet_type;
  CSS_NET_PACKET *recv_packet = NULL;

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_int (request, session->bkup.iosize);
  ptr = or_pack_int (ptr, delete_unneeded_logarchives);

  error = net_client_request_send_msg (&eid, NET_SERVER_BK_BACKUP_LOG_VOLUME,
				       1, request,
				       OR_ALIGNED_BUF_SIZE (a_request));
  if (error != NO_ERROR)
    {
      goto end;
    }

  while (error == NO_ERROR)
    {
      error = net_client_request_recv_msg (NULL, eid, -1, 1, reply,
					   OR_ALIGNED_BUF_SIZE (a_reply));
      if (error != NO_ERROR)
	{
	  goto end;
	}

      ptr = or_unpack_int (reply, (int *) &packet_type);
      ptr = or_unpack_int (ptr, &unzip_nbytes);

      if (packet_type == BK_PACKET_LOGS_BACKUP_END)
	{
	  assert (LSA_ISNULL (&(session->bkuphdr->backuptime_lsa)));
	  assert (session->bkuphdr->end_time == -1);

	  ptr = or_unpack_log_lsa (ptr, &(session->bkuphdr->backuptime_lsa));
	  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
	  ptr = or_unpack_int64 (ptr, &(session->bkuphdr->end_time));

	  assert (!LSA_ISNULL (&(session->bkuphdr->backuptime_lsa)));
	  assert (session->bkuphdr->end_time > 0);

	  break;		/* finally exit */
	}
      else if (packet_type == BK_PACKET_VOL_END)
	{
	  bk_end_vol_in_backup (session);
	  continue;
	}

      error = net_client_request_recv_msg (&recv_packet, eid, -1, 1,
					   session->bkup.buffer,
					   session->bkup.iosize);
      if (error != NO_ERROR)
	{
	  goto end;
	}

      size = css_net_packet_get_recv_size (recv_packet, 0);

      if (size > session->bkup.iosize)
	{
	  error = ER_NET_DATASIZE_MISMATCH;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  error, 2, session->bkup.iosize, size);
	  goto end;
	}

      if (packet_type == BK_PACKET_VOL_START)
	{
	  bk_start_vol_in_backup (session, 1);
	  if (session->bkuphdr->make_slave == true)
	    {
	      css_net_packet_free (recv_packet);
	      recv_packet = NULL;

	      continue;
	    }
	}

      error = bk_write_backup (session, size, unzip_nbytes);
      if (error != NO_ERROR)
	{
	  goto end;
	}

      css_net_packet_free (recv_packet);
      recv_packet = NULL;
    }

end:

  if (recv_packet != NULL)
    {
      css_net_packet_free (recv_packet);
    }

  return error;
#else /* CS_MODE */
  return NO_ERROR;
#endif /* !CS_MODE */
}

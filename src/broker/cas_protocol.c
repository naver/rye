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
 * cas_protocol.c
 */

#ident "$Id$"

#include <stdarg.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <assert.h>

#include "porting.h"
#include "release_string.h"
#include "cas_protocol.h"

#ifdef CCI
#include "cci_common.h"
#else
#include "cas_common.h"
#endif

static short br_msg_unpack_short (const char **ptr);
static int br_msg_unpack_int (const char **ptr);
static char br_msg_unpack_char (const char **ptr);

UINT64
br_msg_protocol_version (const RYE_VERSION * version)
{
  return (((UINT64) (version->major) << 48) |
          ((UINT64) (version->minor) << 32) | ((UINT64) (version->patch) << 16) | ((UINT64) (version->build)));
}

T_BROKER_REQUEST_MSG *
brreq_msg_alloc (int opcode_msg_size)
{
  T_BROKER_REQUEST_MSG *tmp_ptr;

#ifdef CCI
  tmp_ptr = (T_BROKER_REQUEST_MSG *) MALLOC (sizeof (T_BROKER_REQUEST_MSG) + opcode_msg_size);
#else
  tmp_ptr = (T_BROKER_REQUEST_MSG *) RYE_MALLOC (sizeof (T_BROKER_REQUEST_MSG) + opcode_msg_size);
#endif

  if (tmp_ptr)
    {
      tmp_ptr->op_code_msg = tmp_ptr->msg_buffer + BRREQ_MSG_SIZE;
    }

  return tmp_ptr;
}

void
brreq_msg_free (T_BROKER_REQUEST_MSG * ptr)
{
#ifdef CCI
  FREE_MEM (ptr);
#else
  RYE_FREE_MEM (ptr);
#endif
}

T_BROKER_REQUEST_MSG *
brreq_msg_clone (const T_BROKER_REQUEST_MSG * org_msg)
{
  T_BROKER_REQUEST_MSG *clone_msg;

  clone_msg = brreq_msg_alloc (org_msg->op_code_msg_size);
  if (clone_msg != NULL)
    {
      clone_msg->clt_version = org_msg->clt_version;
      clone_msg->clt_type = org_msg->clt_type;
      clone_msg->op_code = org_msg->op_code;
      clone_msg->op_code_msg_size = org_msg->op_code_msg_size;
      memcpy (clone_msg->msg_buffer, org_msg->msg_buffer, BRREQ_MSG_SIZE + org_msg->op_code_msg_size);
    }
  return clone_msg;
}

int
brreq_msg_unpack (T_BROKER_REQUEST_MSG * srv_con_msg)
{
  const char *ptr = srv_con_msg->msg_buffer;

  if (strncmp (ptr, BRREQ_MSG_MAGIC_STR, BRREQ_MSG_MAGIC_LEN) != 0)
    {
      return -1;
    }
  ptr += BRREQ_MSG_MAGIC_LEN;

  srv_con_msg->clt_version.major = br_msg_unpack_short (&ptr);
  srv_con_msg->clt_version.minor = br_msg_unpack_short (&ptr);
  srv_con_msg->clt_version.patch = br_msg_unpack_short (&ptr);
  srv_con_msg->clt_version.build = br_msg_unpack_short (&ptr);

  srv_con_msg->clt_type = br_msg_unpack_char (&ptr);
  srv_con_msg->op_code = br_msg_unpack_char (&ptr);

  srv_con_msg->op_code_msg_size = br_msg_unpack_short (&ptr);

  return 0;
}

char *
brreq_msg_pack (T_BROKER_REQUEST_MSG * srv_con_msg, char clt_type, char op_code, int op_code_msg_size)
{
  char *ptr = srv_con_msg->msg_buffer;

  memset (ptr, 0, BRREQ_MSG_SIZE);

  ptr = br_msg_pack_str (ptr, BRREQ_MSG_MAGIC_STR, BRREQ_MSG_MAGIC_LEN);

  ptr = br_msg_pack_short (ptr, (short) MAJOR_VERSION);
  ptr = br_msg_pack_short (ptr, (short) MINOR_VERSION);
  ptr = br_msg_pack_short (ptr, (short) PATCH_VERSION);
  ptr = br_msg_pack_short (ptr, (short) BUILD_SEQ);

  ptr = br_msg_pack_char (ptr, clt_type);
  ptr = br_msg_pack_char (ptr, op_code);

  srv_con_msg->op_code_msg_size = op_code_msg_size;
  ptr = br_msg_pack_short (ptr, srv_con_msg->op_code_msg_size);

  return srv_con_msg->op_code_msg;
}

void
brres_msg_pack (T_BROKER_RESPONSE_NET_MSG * res_msg, int result_code,
                int num_additional_msg, const int *additional_msg_size)
{
  int i;
  char *ptr = res_msg->msg_buffer;

  memset (res_msg, 0, sizeof (T_BROKER_RESPONSE_NET_MSG));


  ptr = br_msg_pack_int (ptr, BROKER_RESPONSE_MSG_SIZE);        /* msg size */

  ptr = br_msg_pack_short (ptr, (short) MAJOR_VERSION);
  ptr = br_msg_pack_short (ptr, (short) MINOR_VERSION);
  ptr = br_msg_pack_short (ptr, (short) PATCH_VERSION);
  ptr = br_msg_pack_short (ptr, (short) BUILD_SEQ);

  ptr = br_msg_pack_int (ptr, result_code);

  if (num_additional_msg > BROKER_RESPONSE_MAX_ADDITIONAL_MSG)
    {
      assert (false);
      num_additional_msg = BROKER_RESPONSE_MAX_ADDITIONAL_MSG;
    }

  for (i = 0; i < num_additional_msg; i++)
    {
      ptr = br_msg_pack_int (ptr, additional_msg_size[i]);
    }

  res_msg->msg_buffer_size = sizeof (int) + BROKER_RESPONSE_MSG_SIZE;
}

int
brres_msg_unpack (T_BROKER_RESPONSE * res, const char *msg_buffer, int msg_size)
{
  int i;
  const char *ptr = msg_buffer;

  memset (res, 0, sizeof (T_BROKER_RESPONSE));

  if (msg_size < (int) BROKER_RESPONSE_MSG_SIZE)
    {
      return -1;
    }

  res->svr_version.major = br_msg_unpack_short (&ptr);
  res->svr_version.minor = br_msg_unpack_short (&ptr);
  res->svr_version.patch = br_msg_unpack_short (&ptr);
  res->svr_version.build = br_msg_unpack_short (&ptr);

  res->result_code = br_msg_unpack_int (&ptr);

  for (i = 0; i < BROKER_RESPONSE_MAX_ADDITIONAL_MSG; i++)
    {
      res->additional_message_size[i] = br_msg_unpack_int (&ptr);

    }

  msg_size -= BROKER_RESPONSE_MSG_SIZE;

  return 0;
}

void
cas_status_info_init (char *info_ptr)
{
  memset (info_ptr, 0, CAS_STATUS_INFO_SIZE);
  info_ptr[CAS_STATUS_INFO_IDX_STATUS] = CAS_STATUS_INACTIVE;
}

int
brreq_msg_normal_broker_opcode_msg_size (const char *port_name, int add_size)
{
  int msg_size;

  msg_size = sizeof (int);
  if (port_name != NULL)
    {
      msg_size += strlen (port_name) + 1;
    }
  msg_size += add_size;

  return msg_size;
}

char *
brreq_msg_pack_port_name (char *ptr, const char *port_name)
{
  int size;

  size = (port_name == NULL ? 0 : strlen (port_name) + 1);

  ptr = br_msg_pack_int (ptr, size);
  ptr = br_msg_pack_str (ptr, port_name, size);

  return ptr;
}

const char *
brreq_msg_unpack_port_name (const T_BROKER_REQUEST_MSG * brreq_msg, const char **ret_msg_ptr, int *ret_msg_remain)
{
  int port_name_len;
  const char *port_name;
  const char *ptr;
  int msg_remain;

  msg_remain = brreq_msg->op_code_msg_size;
  ptr = brreq_msg->op_code_msg;

  if (msg_remain < 4)
    {
      goto error;
    }

  port_name_len = br_msg_unpack_int (&ptr);

  msg_remain -= sizeof (int);

  if (port_name_len <= 0 || msg_remain < port_name_len)
    {
      goto error;
    }

  port_name = ptr;
  assert (port_name[port_name_len - 1] == '\0');

  ptr += port_name_len;
  msg_remain -= port_name_len;

  if (ret_msg_ptr != NULL)
    {
      *ret_msg_ptr = ptr;
    }
  if (ret_msg_remain != NULL)
    {
      *ret_msg_remain = msg_remain;
    }

  return port_name;

error:
  if (ret_msg_ptr != NULL)
    {
      *ret_msg_ptr = NULL;
    }
  if (ret_msg_remain != NULL)
    {
      *ret_msg_remain = 0;
    }
  return "";
}

char *
br_msg_pack_int (char *ptr, int value)
{
  value = htonl (value);
  memcpy (ptr, &value, 4);
  return (ptr + 4);
}

char *
br_msg_pack_short (char *ptr, short value)
{
  value = htons (value);
  memcpy (ptr, &value, 2);
  return (ptr + 2);
}

char *
br_msg_pack_str (char *ptr, const char *str, int size)
{
  memcpy (ptr, str, size);
  return (ptr + size);
}

char *
br_msg_pack_char (char *ptr, char value)
{
  *ptr = value;
  return (ptr + 1);
}

static short
br_msg_unpack_short (const char **ptr)
{
  short tmp_value;
  memcpy (&tmp_value, *ptr, 2);
  *ptr += 2;
  return (ntohs (tmp_value));
}

static int
br_msg_unpack_int (const char **ptr)
{
  int tmp_int;
  memcpy (&tmp_int, *ptr, 4);
  *ptr += 4;
  return (ntohl (tmp_int));
}

static char
br_msg_unpack_char (const char **ptr)
{
  const char *value_ptr;
  value_ptr = *ptr;
  *ptr += 1;
  return (*value_ptr);
}

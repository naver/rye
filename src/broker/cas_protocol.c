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
#include "cas_protocol.h"

#ifdef CCI
#include "cci_common.h"
#else
#include "cas_common.h"
#endif

T_BROKER_REQUEST_MSG *
brreq_msg_alloc (int opcode_msg_size)
{
  T_BROKER_REQUEST_MSG *tmp_ptr;

#ifdef CCI
  tmp_ptr = (T_BROKER_REQUEST_MSG *) MALLOC (sizeof (T_BROKER_REQUEST_MSG)
					     + opcode_msg_size);
#else
  tmp_ptr = (T_BROKER_REQUEST_MSG *) RYE_MALLOC (sizeof (T_BROKER_REQUEST_MSG)
						 + opcode_msg_size);
#endif

  if (tmp_ptr)
    {
      tmp_ptr->broker_name = tmp_ptr->msg_buffer + BRREQ_MSG_IDX_BROKER_NAME;
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
      clone_msg->clt_protocol_ver = org_msg->clt_protocol_ver;
      clone_msg->clt_type = org_msg->clt_type;
      clone_msg->op_code = org_msg->op_code;
      clone_msg->op_code_msg_size = org_msg->op_code_msg_size;
      memcpy (clone_msg->msg_buffer, org_msg->msg_buffer,
	      BRREQ_MSG_SIZE + org_msg->op_code_msg_size);
    }
  return clone_msg;
}

int
brreq_msg_unpack (T_BROKER_REQUEST_MSG * srv_con_msg)
{
  char *msg_buffer = srv_con_msg->msg_buffer;
  char *ptr;
  short tmp_short;

  if (strncmp (msg_buffer, BRREQ_MSG_MAGIC_STR, BRREQ_MSG_MAGIC_LEN) != 0)
    {
      return -1;
    }

  ptr = msg_buffer + BRREQ_MSG_IDX_CLIENT_PROTO_VERSION;
  memcpy (&tmp_short, ptr, 2);
  srv_con_msg->clt_protocol_ver = ntohs (tmp_short);

  srv_con_msg->clt_type = msg_buffer[BRREQ_MSG_IDX_CLIENT_TYPE];

  srv_con_msg->op_code = msg_buffer[BRREQ_MSG_IDX_OP_CODE];

  ptr = msg_buffer + BRREQ_MSG_IDX_OP_CODE_MSG_SIZE;
  memcpy (&tmp_short, ptr, 2);
  srv_con_msg->op_code_msg_size = ntohs (tmp_short);

  return 0;
}

char *
brreq_msg_pack (T_BROKER_REQUEST_MSG * srv_con_msg, char clt_type,
		char op_code, int op_code_msg_size, const char *port_name)
{
  char *msg_buffer = srv_con_msg->msg_buffer;
  char *ptr;
  short tmp_short;

  memset (msg_buffer, 0, BRREQ_MSG_SIZE);

  memcpy (msg_buffer, BRREQ_MSG_MAGIC_STR, BRREQ_MSG_MAGIC_LEN);

  ptr = msg_buffer + BRREQ_MSG_IDX_CLIENT_PROTO_VERSION;
  tmp_short = htons ((short) CURRENT_PROTOCOL);
  memcpy (ptr, &tmp_short, 2);

  msg_buffer[BRREQ_MSG_IDX_CLIENT_TYPE] = clt_type;

  msg_buffer[BRREQ_MSG_IDX_OP_CODE] = op_code;

  if (port_name != NULL)
    {
      strcpy (srv_con_msg->broker_name, port_name);
    }

  srv_con_msg->op_code_msg_size = op_code_msg_size;

  ptr = msg_buffer + BRREQ_MSG_IDX_OP_CODE_MSG_SIZE;
  tmp_short = htons (srv_con_msg->op_code_msg_size);
  memcpy (ptr, &tmp_short, 2);

  return srv_con_msg->op_code_msg;
}

void
brres_msg_pack (T_BROKER_RESPONSE_NET_MSG * res_msg, int result_code,
		int num_additional_msg, const int *additional_msg_size)
{
  short tmp_short;
  int tmp_int;
  int i;
  char *ptr = res_msg->msg_buffer;

  memset (res_msg, 0, sizeof (T_BROKER_RESPONSE_NET_MSG));

  tmp_int = htonl (BROKER_RESPONSE_MSG_SIZE);	/* msg size */
  memcpy (ptr, &tmp_int, sizeof (int));
  ptr += sizeof (int);

  tmp_short = htons (CURRENT_PROTOCOL);
  memcpy (ptr, &tmp_short, sizeof (short));
  ptr += sizeof (short);

  tmp_int = htonl (result_code);
  memcpy (ptr, &tmp_int, sizeof (int));
  ptr += sizeof (int);

  if (num_additional_msg > BROKER_RESPONSE_MAX_ADDITIONAL_MSG)
    {
      assert (false);
      num_additional_msg = BROKER_RESPONSE_MAX_ADDITIONAL_MSG;
    }

  for (i = 0; i < num_additional_msg; i++)
    {
      tmp_int = htonl (additional_msg_size[i]);
      memcpy (ptr, &tmp_int, sizeof (int));
      ptr += sizeof (int);
    }

  res_msg->msg_buffer_size = sizeof (int) + BROKER_RESPONSE_MSG_SIZE;
}

int
brres_msg_unpack (T_BROKER_RESPONSE * res, const char *msg_buffer,
		  int msg_size)
{
  short tmp_short;
  int tmp_int;
  int i;

  memset (res, 0, sizeof (T_BROKER_RESPONSE));

  if (msg_size < (int) BROKER_RESPONSE_MSG_SIZE)
    {
      return -1;
    }

  memcpy (&tmp_short, msg_buffer, sizeof (short));
  res->srv_protocol_ver = ntohs (tmp_short);
  msg_buffer += sizeof (short);

  memcpy (&tmp_int, msg_buffer, sizeof (int));
  res->result_code = ntohl (tmp_int);
  msg_buffer += sizeof (int);

  for (i = 0; i < BROKER_RESPONSE_MAX_ADDITIONAL_MSG; i++)
    {
      memcpy (&tmp_int, msg_buffer, sizeof (int));
      res->additional_message_size[i] = ntohl (tmp_int);
      msg_buffer += sizeof (int);
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

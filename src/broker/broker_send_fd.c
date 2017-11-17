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
 * broker_send_fd.c -
 */

#ident "$Id$"

#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <stddef.h>
#include <errno.h>
#include <stdlib.h>
#include <memory.h>
#include <assert.h>

#include "porting.h"
#include "cas_protocol.h"
#include "broker_send_fd.h"
#include "broker_send_recv_msg.h"

#define CONTROLLEN (sizeof(struct cmsghdr) + sizeof(int))

#define SYSV

int
send_fd (int server_fd, int client_fd, int rid, struct timeval *recv_time)
{
  struct iovec iov[1];
  struct msghdr msg;
  int num_bytes;
  union
  {
    struct cmsghdr cm;
    char control[CONTROLLEN];
  } control_un;
  struct cmsghdr *cmptr;
  struct sendmsg_s send_msg;
  int *dataptr;

  assert (recv_time != NULL);

  /* set send message */
  send_msg.rid = rid;
  send_msg.recv_time = *recv_time;

  /* Pass the fd to the server */
  iov[0].iov_base = (char *) &send_msg;
  iov[0].iov_len = sizeof (struct sendmsg_s);
  msg.msg_iov = iov;
  msg.msg_iovlen = 1;
  msg.msg_namelen = 0;
  msg.msg_name = (caddr_t) 0;
  msg.msg_control = control_un.control;
  msg.msg_controllen = CONTROLLEN;
  msg.msg_flags = 0;

  cmptr = CMSG_FIRSTHDR (&msg);
  cmptr->cmsg_level = SOL_SOCKET;
  cmptr->cmsg_type = SCM_RIGHTS;
  cmptr->cmsg_len = CONTROLLEN;
  dataptr = (int *) CMSG_DATA (cmptr);
  *dataptr = client_fd;

  num_bytes = sendmsg (server_fd, &msg, 0);

  if (num_bytes < (signed int) sizeof (int))
    {
      return (-1);
    }
  return (num_bytes);
}

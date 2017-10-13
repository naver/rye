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
 * broker_recv_fd.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <stddef.h>
#include <errno.h>
#include <stdlib.h>

#include "porting.h"
#include "cas_protocol.h"
#include "broker_recv_fd.h"
#include "broker_send_recv_msg.h"

#include <sys/ioctl.h>

#define CONTROLLEN (sizeof(struct cmsghdr) + sizeof(int))

#define SYSV

int
recv_fd (int fd, int *rid, struct timeval *recv_time)
{
  int new_fd = 0, rc;
  struct iovec iov[1];
  struct msghdr msg;
  int pid;
  union
  {
    struct cmsghdr cm;
    char control[CONTROLLEN];
  } control_un;
  struct cmsghdr *cmptr = NULL;
  struct sendmsg_s send_msg;

  iov[0].iov_base = (char *) &send_msg;
  iov[0].iov_len = sizeof (struct sendmsg_s);
  msg.msg_iov = iov;
  msg.msg_iovlen = 1;
  msg.msg_name = (caddr_t) NULL;
  msg.msg_namelen = 0;
  msg.msg_control = control_un.control;
  msg.msg_controllen = CONTROLLEN;
  cmptr = CMSG_FIRSTHDR (&msg);
  rc = recvmsg (fd, &msg, 0);

  if (rc < (signed int) (sizeof (int)))
    {
#ifdef _DEBUG
      printf ("recvmsg failed. errno = %d. str=%s\n", errno,
	      strerror (errno));
#endif
      return (-1);
    }

  *rid = send_msg.rid;
  if (recv_time)
    {
      *recv_time = send_msg.recv_time;
    }

  pid = getpid ();
  new_fd = *(int *) CMSG_DATA (cmptr);

#ifdef SYSV
  ioctl (new_fd, SIOCSPGRP, (caddr_t) & pid);
#elif !defined(VMS)
  fcntl (new_fd, F_SETOWN, pid);
#endif

  return (new_fd);
}

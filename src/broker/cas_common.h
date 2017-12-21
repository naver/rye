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
 * cas_common.h -
 */

#ifndef	_CAS_COMMON_H_
#define	_CAS_COMMON_H_

#ident "$Id$"

#include <sys/types.h>
#include <sys/socket.h>

#include "porting.h"

#define makestring1(x) #x
#define makestring(x) makestring1(x)

#define MAX_SERVER_H_ID                 256
#define MAX_BIND_VALUE                  10240
#define MAX_QUERY_LEN                   100000
#define CAS_RUNNER_CONF                 "cas_runner.conf"
#define CAS_RUNNER_CONF_ENV             "CAS_RUNNER_CONF"
#define CAS_USE_DEFAULT_DB_PARAM        -2

#define ON	1
#define OFF	0

#define TRUE	1
#define FALSE	0

#define INT_STR_LEN     16

#define RYE_MALLOC(SIZE)            malloc(SIZE)
#define RYE_REALLOC(PTR, SIZE)      \
        ((PTR == NULL) ? malloc(SIZE) : realloc(PTR, SIZE))

#define RYE_FREE_MEM(PTR)	\
	do {			\
	  if ((PTR) != NULL) {  \
	    free(PTR);		\
	    PTR = NULL;	        \
	  }			\
	} while (0)

#define RYE_ALLOC_COPY_STR(PTR, STR)		\
	do {					\
	  if (STR == NULL)			\
	    PTR = NULL;				\
	  else {				\
	    PTR = (char *) RYE_MALLOC(strlen(STR) + 1);	\
	    if (PTR) {				\
	      strcpy(PTR, STR);			\
	    }					\
	  }					\
	} while (0)

#define RYE_CLOSE_SOCKET(X)		\
	do {			\
	  if (!IS_INVALID_SOCKET(X)) close(X);		\
	  (X) = INVALID_SOCKET;	\
	} while (0)

#define SLEEP_SEC(X)                    sleep(X)

#define THREAD_BEGIN(THR_ID, FUNC, ARG)		\
	do {					\
	  pthread_attr_t	thread_attr;	\
	  pthread_attr_init(&thread_attr);	\
	  pthread_attr_setdetachstate(&thread_attr, PTHREAD_CREATE_DETACHED); \
	  pthread_create(&(THR_ID), &thread_attr, FUNC, ARG);	\
	  pthread_attr_destroy(&thread_attr);  \
	} while (0)


#define READ_FROM_SOCKET(fd, buf, size)         read(fd, buf, size)
#define WRITE_TO_SOCKET(fd, buf, size)          write(fd, buf, size)

#define THREAD_FUNC     void*

typedef socklen_t T_SOCKLEN;

extern int uts_key_check_local_host (void);

#endif /* _CAS_COMMON_H_ */

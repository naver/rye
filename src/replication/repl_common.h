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

#ifndef REPL_LOG_COMMON_H_
#define REPL_LOG_COMMON_H_

#ident "$Id$"

#include <unistd.h>

#include "dbtype.h"
#include "log_impl.h"


#define LA_MAX_REPL_ITEM_WITHOUT_RELEASE_PB     50
#define LA_GET_PAGE_RETRY_COUNT                 100
#define LA_REPL_LIST_COUNT                      50

#if 1				/* FIXME-notout: willdel */
#define LA_PAGE_DOESNOT_EXIST                   0
#define LA_PAGE_EXST_IN_ACTIVE_LOG              1
#define LA_PAGE_EXST_IN_ARCHIVE_LOG             2
#endif


#define REPL_AGENT_NO_ERROR                     (0x00)
#define REPL_AGENT_NEED_RESTART		        (0x01)
#define REPL_AGENT_NEED_SHUTDOWN                (0x02)

typedef enum _rp_tran_type RP_TRAN_TYPE;
enum _rp_tran_type
{
  RP_TRAN_TYPE_CATALOG,
  RP_TRAN_TYPE_DDL,
  RP_TRAN_TYPE_DATA
};

typedef enum _rp_item_type RP_ITEM_TYPE;
enum _rp_item_type
{
  RP_ITEM_TYPE_CATALOG,
  RP_ITEM_TYPE_DDL,
  RP_ITEM_TYPE_DATA
};

typedef struct _rp_ddl_item RP_DDL_ITEM;
struct _rp_ddl_item
{
  RYE_STMT_TYPE stmt_type;	/* Statement Type */
  int ddl_type;			/* REPL_NON_BLOCKED_DDL or REPL_BLOCKED_DDL */
  char *db_user;
  char *query;			/* SQL string */

  LOG_LSA lsa;			/* the LSA of the replication log record */
};

typedef struct _rp_catalog_item RP_CATALOG_ITEM;
struct _rp_catalog_item
{
  int copyarea_op;
  char *class_name;
  DB_IDXKEY key;		/* PK */
  RECDES *recdes;

  LOG_LSA lsa;			/* the LSA of current */
};

typedef struct rp_data_item RP_DATA_ITEM;
struct rp_data_item
{
  LOG_RCVINDEX rcv_index;
  int groupid;
  char *class_name;
  DB_IDXKEY key;		/* PK */

  LOG_LSA lsa;			/* the LSA of the replication log record */
  LOG_LSA target_lsa;		/* the LSA of the target log record */
  RECDES *recdes;
};

typedef union _rp_item_info RP_ITEM_INFO;
union _rp_item_info
{
  RP_DATA_ITEM data;
  RP_CATALOG_ITEM catalog;
  RP_DDL_ITEM ddl;
};

typedef struct cirp_repl_item CIRP_REPL_ITEM;
struct cirp_repl_item
{
  CIRP_REPL_ITEM *next;
  RP_ITEM_TYPE item_type;

  RP_ITEM_INFO info;
};

extern int repl_Need_shutdown;

extern void rp_signal_handler (int signo);
extern void rp_clear_agent_flag (void);
extern void rp_set_agent_flag (const char *file_name, int line, int flag);
extern bool rp_agent_flag_enabled (int flag);
extern bool rp_need_restart (void);
extern bool rp_need_shutdown (const char *file_name, int line);

extern void cirp_free_repl_item (CIRP_REPL_ITEM * item);
extern CIRP_REPL_ITEM *cirp_new_repl_item_data (const LOG_LSA * lsa,
						const LOG_LSA * target_lsa);
extern CIRP_REPL_ITEM *cirp_new_repl_item_ddl (const LOG_LSA * lsa);
extern CIRP_REPL_ITEM *cirp_new_repl_catalog_item (const LOG_LSA * lsa);



#endif /* REPL_LOG_COMMON_H_ */

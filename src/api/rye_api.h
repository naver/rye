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
 * rye_api.h -
 */

#ifndef _RYE_API_H_
#define _RYE_API_H_

#include "config.h"
#include <stdlib.h>
#include "error_code.h"

typedef enum
{
  TRAN_UNKNOWN_ISOLATION = 0x00,	/*        0  0000 */

#if 0				/* unused */
  TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE = 0x01,	/*        0  0001 */
  TRAN_DEGREE_1_CONSISTENCY = 0x01,	/* Alias of above */

  TRAN_COMMIT_CLASS_COMMIT_INSTANCE = 0x02,	/*        0  0010 */
  TRAN_DEGREE_2_CONSISTENCY = 0x02,	/* Alias of above */
#endif

  TRAN_REP_CLASS_UNCOMMIT_INSTANCE = 0x03,	/*        0  0011 */
  TRAN_READ_UNCOMMITTED = 0x03,	/* Alias of above */

#if 0				/* unused */
  TRAN_REP_CLASS_COMMIT_INSTANCE = 0x04,	/*        0  0100 */
  TRAN_READ_COMMITTED = 0x04,	/* Alias of above */
  TRAN_CURSOR_STABILITY = 0x04,	/* Alias of above */

  TRAN_REP_CLASS_REP_INSTANCE = 0x05,	/*        0  0101 */
  TRAN_REP_READ = 0x05,		/* Alias of above */
  TRAN_DEGREE_2_9999_CONSISTENCY = 0x05,	/* Alias of above */

  TRAN_SERIALIZABLE = 0x06,	/*        0  0110 */
  TRAN_DEGREE_3_CONSISTENCY = 0x06,	/* Alias of above */
  TRAN_NO_PHANTOM_READ = 0x06,	/* Alias of above */
#endif

  TRAN_DEFAULT_ISOLATION = TRAN_REP_CLASS_UNCOMMIT_INSTANCE,

  TRAN_MINVALUE_ISOLATION = 0x01,	/* internal use only */
  TRAN_MAXVALUE_ISOLATION = 0x06	/* internal use only */
} DB_TRAN_ISOLATION;

typedef enum
{
  RYE_STMT_ALTER_CLASS = 0,
  RYE_STMT_ALTER_SERIAL = 1,
  RYE_STMT_COMMIT_WORK = 2,
  RYE_STMT_REGISTER_DATABASE = 3,
  RYE_STMT_CREATE_CLASS = 4,
  RYE_STMT_CREATE_INDEX = 5,
  RYE_STMT_CREATE_SERIAL = 7,
  RYE_STMT_DROP_DATABASE = 8,
  RYE_STMT_DROP_CLASS = 9,
  RYE_STMT_DROP_INDEX = 10,
  RYE_STMT_DROP_LABEL = 11,
  RYE_STMT_DROP_SERIAL = 13,
  RYE_STMT_RENAME_CLASS = 15,
  RYE_STMT_ROLLBACK_WORK = 16,
  RYE_STMT_GRANT = 17,
  RYE_STMT_REVOKE = 18,
  RYE_STMT_UPDATE_STATS = 19,
  RYE_STMT_INSERT = 20,
  RYE_STMT_SELECT = 21,
  RYE_STMT_UPDATE = 22,
  RYE_STMT_DELETE = 23,
  RYE_STMT_GET_ISO_LVL = 25,
  RYE_STMT_GET_TIMEOUT = 26,
  RYE_STMT_GET_OPT_LVL = 27,
  RYE_STMT_SET_OPT_LVL = 28,
  RYE_STMT_SCOPE = 29,
  RYE_STMT_SAVEPOINT = 32,
  RYE_STMT_ON_LDB = 38,
  RYE_STMT_GET_LDB = 39,
  RYE_STMT_SET_LDB = 40,
  RYE_STMT_CREATE_USER = 42,
  RYE_STMT_DROP_USER = 43,
  RYE_STMT_ALTER_USER = 44,
  RYE_STMT_SET_SYS_PARAMS = 45,
  RYE_STMT_ALTER_INDEX = 46,
  RYE_STMT_SELECT_UPDATE = 49,

  RYE_STMT_UNKNOWN = 53,
  RYE_MAX_STMT_TYPE = RYE_STMT_UNKNOWN
} RYE_STMT_TYPE;

#define RYE_STMT_TYPE_NAME(type)                                         \
  ((type) == RYE_STMT_ALTER_CLASS ? "RYE_STMT_ALTER_CLASS" :             \
   (type) == RYE_STMT_ALTER_SERIAL ? "RYE_STMT_ALTER_SERIAL" :           \
   (type) == RYE_STMT_COMMIT_WORK ? "RYE_STMT_COMMIT_WORK" :             \
   (type) == RYE_STMT_REGISTER_DATABASE ? "RYE_STMT_REGISTER_DATABASE" : \
   (type) == RYE_STMT_CREATE_CLASS ? "RYE_STMT_CREATE_CLASS" :           \
   (type) == RYE_STMT_CREATE_INDEX ? "RYE_STMT_CREATE_INDEX" :           \
   (type) == RYE_STMT_CREATE_SERIAL ? "RYE_STMT_CREATE_SERIAL" :         \
   (type) == RYE_STMT_DROP_DATABASE ? "RYE_STMT_DROP_DATABASE" :         \
   (type) == RYE_STMT_DROP_CLASS ? "RYE_STMT_DROP_CLASS" :               \
   (type) == RYE_STMT_DROP_INDEX ? "RYE_STMT_DROP_INDEX" :               \
   (type) == RYE_STMT_DROP_LABEL ? "RYE_STMT_DROP_LABEL" :               \
   (type) == RYE_STMT_DROP_SERIAL ? "RYE_STMT_DROP_SERIAL" :             \
   (type) == RYE_STMT_RENAME_CLASS ? "RYE_STMT_RENAME_CLASS" :           \
   (type) == RYE_STMT_ROLLBACK_WORK ? "RYE_STMT_ROLLBACK_WORK" :         \
   (type) == RYE_STMT_GRANT ? "RYE_STMT_GRANT" :                         \
   (type) == RYE_STMT_REVOKE ? "RYE_STMT_REVOKE" :                       \
   (type) == RYE_STMT_INSERT ? "RYE_STMT_INSERT" :                       \
   (type) == RYE_STMT_SELECT ? "RYE_STMT_SELECT" :                       \
   (type) == RYE_STMT_UPDATE ? "RYE_STMT_UPDATE" :                       \
   (type) == RYE_STMT_DELETE ? "RYE_STMT_DELETE" :                       \
   (type) == RYE_STMT_GET_ISO_LVL ? "RYE_STMT_GET_ISO_LVL" :             \
   (type) == RYE_STMT_GET_TIMEOUT ? "RYE_STMT_GET_TIMEOUT" :             \
   (type) == RYE_STMT_GET_OPT_LVL ? "RYE_STMT_GET_OPT_LVL" :             \
   (type) == RYE_STMT_SET_OPT_LVL ? "RYE_STMT_SET_OPT_LVL" :             \
   (type) == RYE_STMT_SCOPE ? "RYE_STMT_SCOPE" :                         \
   (type) == RYE_STMT_SAVEPOINT ? "RYE_STMT_SAVEPOINT" :                 \
   (type) == RYE_STMT_ON_LDB ? "RYE_STMT_ON_LDB" :                       \
   (type) == RYE_STMT_GET_LDB ? "RYE_STMT_GET_LDB" :                     \
   (type) == RYE_STMT_SET_LDB ? "RYE_STMT_SET_LDB" :                     \
   (type) == RYE_STMT_CREATE_USER ? "RYE_STMT_CREATE_USER" :             \
   (type) == RYE_STMT_DROP_USER ? "RYE_STMT_DROP_USER" :                 \
   (type) == RYE_STMT_ALTER_USER ? "RYE_STMT_ALTER_USER" :               \
   (type) == RYE_STMT_SET_SYS_PARAMS ? "RYE_STMT_SET_SYS_PARAMS" :       \
   (type) == RYE_STMT_ALTER_INDEX ? "RYE_STMT_ALTER_INDEX" :             \
   (type) == RYE_STMT_SELECT_UPDATE ? "RYE_STMT_SELECT_UPDATE" :         \
   (type) == RYE_STMT_UNKNOWN ? "RYE_STMT_UNKNOWN" : "invalid")

#define STMT_TYPE_IS_DDL(type)                          \
        (((type) == RYE_STMT_CREATE_CLASS)           \
          || ((type) == RYE_STMT_ALTER_CLASS)        \
          || ((type) == RYE_STMT_RENAME_CLASS)       \
          || ((type) == RYE_STMT_DROP_CLASS)         \
          || ((type) == RYE_STMT_CREATE_INDEX)       \
          || ((type) == RYE_STMT_ALTER_INDEX)        \
          || ((type) == RYE_STMT_DROP_INDEX)         \
          || ((type) == RYE_STMT_CREATE_USER)        \
          || ((type) == RYE_STMT_ALTER_USER)         \
          || ((type) == RYE_STMT_DROP_USER)          \
          || ((type) == RYE_STMT_GRANT)              \
          || ((type) == RYE_STMT_REVOKE)             \
          || ((type) == RYE_STMT_UPDATE_STATS) ? true : false)

#endif /* _RYE_API_H_ */

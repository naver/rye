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

#ifndef REPL_DEFS_H_
#define REPL_DEFS_H_

#ident "$Id$"

#define REPL_STATUS_BUSY                          (1)
#define REPL_STATUS_IDLE                          (0)

typedef enum
{
  REPL_FILTER_NONE,
  REPL_FILTER_INCLUDE_TBL,
  REPL_FILTER_EXCLUDE_TBL
} REPL_FILTER_TYPE;

typedef struct _repl_stats REPL_STATS;
struct _repl_stats
{
  unsigned long insert;
  unsigned long update;
  unsigned long delete;
  unsigned long schema;
  unsigned long fail;
  unsigned long commit;
};

#endif /* REPL_DEFS_H_ */

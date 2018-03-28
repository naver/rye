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
 * rbl_table_info.h -
 */

#ifndef RBL_TABLE_INFO_H_
#define RBL_TABLE_INFO_H_

#ident "$Id$"

#define SQL_BUF_SIZE 512

typedef struct table_info
{
  char *table_name;
  char *skey_col_name;
} TABLE_INFO;

extern char **rbl_get_shard_keys (CCI_CONN * conn, int gid, int *num_keys);
extern void rbl_free_shard_keys (char **keys, int n);
extern TABLE_INFO *rbl_get_all_shard_tables (CCI_CONN * conn, int *num_table);
extern TABLE_INFO *rbl_get_all_global_tables (CCI_CONN * conn, int *num_table);
extern void rbl_free_table_info (TABLE_INFO * info, int n);

#endif /* RBL_TABLE_INFO_H_ */

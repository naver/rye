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

#ifndef SHARD_CATALOG_H_
#define SHARD_CATALOG_H_

#ident "$Id$"

#include <unistd.h>

#include "dbtype.h"

#include "shard_defs.h"

typedef struct _shard_ct_shard_gid_skey_info SHARD_CT_SHARD_GID_SKEY_INFO;
struct _shard_ct_shard_gid_skey_info
{
  int gid;
  char skey[SHARD_SKEY_LENGTH];
};

typedef struct _shard_ct_shard_gid_removed_info
  SHARD_CT_SHARD_GID_REMOVED_INFO;
struct _shard_ct_shard_gid_removed_info
{
  int gid;
  DB_DATETIME rem_dt;
};

#if defined (ENABLE_UNUSED_FUNCTION)
extern int shard_get_ct_shard_gid_skey_info (const int gid, const char *skey,
					     SHARD_CT_SHARD_GID_SKEY_INFO *
					     shard_gid_skey_info);
#endif
extern int shard_delete_ct_shard_gid_skey_info_with_gid (const int gid);

#if defined (ENABLE_UNUSED_FUNCTION)
extern int shard_insert_ct_shard_gid_skey_info (SHARD_CT_SHARD_GID_SKEY_INFO *
						shard_gid_skey_info);

extern int shard_get_ct_shard_gid_removed_info (const int gid,
						SHARD_CT_SHARD_GID_REMOVED_INFO
						* shard_gid_removed_info);
#endif

extern int shard_delete_ct_shard_gid_removed_info_with_gid (const int gid);

extern int shard_insert_ct_shard_gid_removed_info (const int gid);

#endif /* SHARD_CATALOG_H_ */

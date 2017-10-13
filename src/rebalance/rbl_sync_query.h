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
 * rbl_sync_query.h -
 */

#ifndef RBL_SYNC_QUERY_H_
#define RBL_SYNC_QUERY_H_

#ident "$Id$"

#include "log_impl.h"

extern unsigned int rbl_tranid_hash (const void *key,
				     const unsigned int ht_size);
extern int rbl_compare_tranid_are_equal (const void *key1, const void *key2);
extern int rbl_sync_query_init (void);
extern int rbl_tran_list_add (TRANID tran_id, char *query);
extern int rbl_sync_execute_query (RBL_SYNC_CONTEXT * ctx, TRANID tran_id,
				   int gid);
extern void rbl_clear_tran_list (TRANID tran_id);

#endif /* RBL_SYNC_QUERY_H_ */

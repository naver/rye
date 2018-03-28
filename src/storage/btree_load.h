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
 * btree_load.h: Contains private information of B+tree Module
 */

#ifndef _BTREE_LOAD_H_
#define _BTREE_LOAD_H_

#ident "$Id$"

#include "btree.h"
#include "object_representation.h"
#include "error_manager.h"
#include "storage_common.h"
#include "oid.h"
#include "system_parameter.h"
#include "object_domain.h"

/*
 * Constants related to b+tree structure
 */

#define PEEK_KEY_VALUE 0
#define COPY_KEY_VALUE 1

/* The revision level of the the Btree should be incremented whenever there
 * is a disk representation change for the Btree structure.
 */
#define BTREE_CURRENT_REV_LEVEL 0

/* each non leaf node is supposed to have around 30% blank area during merge
   considerations of a delete operation */
#define FIXED_EMPTY   ( DB_PAGESIZE * 0.33 )

#define BTREE_MAX_ALIGN INT_ALIGNMENT	/* Maximum Alignment            */
					     /* Maximum Leaf Node Entry Size */
#define LEAFENTSZ(n)  ( n )
					     /* Maximum Non_Leaf Entry Size  */
#define NLEAFENTSZ(n) ( DISK_VPID_SIZE + n )

#define HEADER 0		/* Header (Oth) record of the page  */

#define BTREE_INVALID_INDEX_ID(btid) \
 ((btid) == NULL ||\
  (btid)->vfid.fileid == NULL_FILEID || (btid)->vfid.volid == NULL_VOLID ||\
  (btid)->root_pageid == NULL_PAGEID)

/* B+tree node types */
#define BTREE_LEAF_NODE 	((short)0)
#define BTREE_NON_LEAF_NODE 	((short)1)

#define NODE_HEADER_SIZE       BTREE_NUM_OIDS_OFFSET	/* Node Header Disk Size */

#define BTREE_IS_VALID_KEY_LEN(key_len) \
  ((key_len) > 0 && (key_len) <= BTREE_MAX_KEYLEN)

#define BTREE_GET_NODE_TYPE(node_level) \
  (((node_level) > 1) ? BTREE_NON_LEAF_NODE : BTREE_LEAF_NODE);


/*
 * B+tree load structures
 */

typedef struct btree_node BTREE_NODE;
struct btree_node
{				/* node of the file_contents linked list */
  BTREE_NODE *next;		/* Pointer to next node */
  VPID pageid;			/* Identifier of the page */
};

extern int btree_compare_key (THREAD_ENTRY * thread_p, BTID_INT * btid,
			      const DB_IDXKEY * key1, const DB_IDXKEY * key2,
			      int *start_colp);
/* Recovery routines */
extern int btree_rv_undo_create_index (THREAD_ENTRY * thread_p,
				       LOG_RCV * rcv);
extern void btree_rv_dump_create_index (FILE * fp, int length_ignore,
					void *data);
#endif /* _BTREE_LOAD_H_ */

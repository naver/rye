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
 * heap_file.c - heap file manager
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "porting.h"
#include "heap_file.h"
#include "storage_common.h"
#include "memory_alloc.h"
#include "system_parameter.h"
#include "oid.h"
#include "error_manager.h"
#include "locator.h"
#include "file_io.h"
#include "page_buffer.h"
#include "file_manager.h"
#include "disk_manager.h"
#include "slotted_page.h"
#include "object_representation.h"
#include "object_representation_sr.h"
#include "log_manager.h"
#include "lock_manager.h"
#include "memory_hash.h"
#include "critical_section.h"
#include "boot_sr.h"
#include "locator_sr.h"
#include "btree.h"
#include "thread.h"		/* MAX_NTHRDS */
#include "object_primitive.h"
#include "dbtype.h"
#include "db.h"
#include "object_print.h"
#include "xserver_interface.h"
#include "boot_sr.h"
#include "chartype.h"
#include "query_executor.h"
#include "fetch.h"
#include "server_interface.h"
#include "perf_monitor.h"
#if 1				/* TODO - test npush */
#include "transform.h"
#endif

#include "fault_injection.h"

/* For getting and dumping attributes */
#include "language_support.h"

/* For creating multi-column sequence keys (sets) */
#include "set_object.h"

#ifdef SERVER_MODE
#include "connection_error.h"
#endif

/* this must be the last header file included!!! */
#include "dbval.h"

#if !defined(SERVER_MODE)
#define pthread_mutex_init(a, b)
#define pthread_mutex_destroy(a)
#define pthread_mutex_lock(a)   0
#define pthread_mutex_trylock(a)   0
#define pthread_mutex_unlock(a)
#define thread_sleep(milliseconds)
#define thread_set_check_interrupt(thread_p, false) true
#endif /* not SERVER_MODE */

#define HEAP_BESTSPACE_SYNC_THRESHOLD (0.1f)

/* ATTRIBUTE LOCATION */

#define OR_FIXED_ATTRIBUTES_OFFSET_BY_OBJ(obj, nvars) \
  (OR_HEADER_SIZE + OR_VAR_TABLE_SIZE_INTERNAL(nvars, OR_GET_OFFSET_SIZE(obj)))

#define HEAP_GUESS_NUM_INDEXED_ATTRS 100

#define HEAP_CLASSREPR_MAXCACHE	100

#define HEAP_BESTSPACE_MHT_EST_SIZE 1000
#define HEAP_BESTSPACE_FREELIST_SIZE 100000

/* A good space to accept insertions */
#define HEAP_DROP_FREE_SPACE (int)(DB_PAGESIZE * 0.3)

#define HEAP_GUESS_MAX_REC_SIZE ONE_K

/*
 * Stop inserting when page has run below
 * this. leave it for updates
 */
#define HEAP_UNFILL_SPACE ((int)(DB_PAGESIZE * 0.10f))

#define HEAP_DEBUG_SCANCACHE_INITPATTERN (12345)

#define HEAP_ISJUNK_OID(oid) \
  ((oid)->slotid == HEAP_HEADER_AND_CHAIN_SLOTID || \
   (oid)->slotid < 0 || (oid)->volid < 0 || (oid)->pageid < 0)

#if defined(RYE_DEBUG)
#define HEAP_ISVALID_OID(oid) \
  (HEAP_ISJUNK_OID(oid)       \
   ? DISK_INVALID             \
   : disk_isvalid_page((oid)->volid, (oid)->pageid))
#else /* RYE_DEBUG */
#define HEAP_ISVALID_OID(oid) \
  (HEAP_ISJUNK_OID(oid)       \
   ? DISK_INVALID             \
   : DISK_VALID)
#endif /* !RYE_DEBUG */

typedef enum
{
  HEAP_FINDSPACE_FOUND,
  HEAP_FINDSPACE_NOTFOUND,
  HEAP_FINDSPACE_ERROR
} HEAP_FINDSPACE;

/*
 * Prefetching directions
 */

typedef enum
{
  HEAP_DIRECTION_NONE,		/* No prefetching           */
  HEAP_DIRECTION_LEFT,		/* Prefetching at the left  */
  HEAP_DIRECTION_RIGHT,		/* Prefetching at the right */
  HEAP_DIRECTION_BOTH		/* Prefetching at both directions.. left and right */
} HEAP_DIRECTION;

/*
 * Heap file header
 */

typedef struct heap_bestspace_entry HEAP_BESTSPACE_ENTRY;
struct heap_bestspace_entry
{
  HFID hfid;			/* heap file identifier */
  HEAP_BESTSPACE best;		/* best space info */
  HEAP_BESTSPACE_ENTRY *next;
};

typedef struct heap_chain HEAP_CHAIN;
struct heap_chain
{				/* Double-linked */
  /* the first must be class_oid */
  OID class_oid;
  VPID prev_vpid;		/* Previous page */
  VPID next_vpid;		/* Next page     */
};

typedef struct heap_chain_tolast HEAP_CHAIN_TOLAST;
struct heap_chain_tolast
{
  PAGE_PTR hdr_pgptr;
  PAGE_PTR last_pgptr;
  HEAP_HDR_STATS *heap_hdr;
};

#define HEAP_CHK_ADD_UNFOUND_RELOCOIDS 100

typedef struct heap_chk_relocoid HEAP_CHK_RELOCOID;
struct heap_chk_relocoid
{
  OID real_oid;
  OID reloc_oid;
};

typedef struct heap_chkall_relocoids HEAP_CHKALL_RELOCOIDS;
struct heap_chkall_relocoids
{
  MHT_TABLE *ht;		/* Hash table to be used to keep relocated records
				 * The key of hash table is the relocation OID, the date
				 * is the real OID
				 */
  bool verify;
  int max_unfound_reloc;
  int num_unfound_reloc;
  OID *unfound_reloc_oids;	/* The relocation OIDs that have not been
				 * found in hash table
				 */
};

#define DEFAULT_REPR_INCREMENT 16

enum
{ ZONE_VOID = 1, ZONE_FREE = 2, ZONE_LRU = 3 };

typedef struct heap_classrepr_entry HEAP_CLASSREPR_ENTRY;
struct heap_classrepr_entry
{
  int idx;			/* Cache index. Used to pass the index when
				 * a class representation is in the cache */
  int fcnt;			/* How many times this structure has been
				 * fixed. It cannot be deallocated until this
				 * value is zero.  */
  int zone;			/* ZONE_VOID, ZONE_LRU, ZONE_FREE */
  int force_decache;

  THREAD_ENTRY *next_wait_thrd;
  HEAP_CLASSREPR_ENTRY *hash_next;
  HEAP_CLASSREPR_ENTRY *lru_prev;	/* prev. entry in LRU list */
  HEAP_CLASSREPR_ENTRY *lru_next;	/* next. entry in LRU */
  HEAP_CLASSREPR_ENTRY *free_next;	/* next. free list */

  /* real data */
  OID class_oid;		/* Identifier of the class representation */

  OR_CLASSREP **repr;		/* A particular representation of the class */
  int max_reprid;
  REPR_ID last_reprid;
};

typedef struct heap_classrepr_lock HEAP_CLASSREPR_LOCK;
struct heap_classrepr_lock
{
  OID class_oid;
  HEAP_CLASSREPR_LOCK *lock_next;
  THREAD_ENTRY *next_wait_thrd;
};

typedef struct heap_classrepr_hash HEAP_CLASSREPR_HASH;
struct heap_classrepr_hash
{
  int idx;
  HEAP_CLASSREPR_ENTRY *hash_next;
  HEAP_CLASSREPR_LOCK *lock_next;
};

typedef struct heap_classrepr_LRU_list HEAP_CLASSREPR_LRU_LIST;
struct heap_classrepr_LRU_list
{
  HEAP_CLASSREPR_ENTRY *LRU_top;
  HEAP_CLASSREPR_ENTRY *LRU_bottom;
};

typedef struct heap_classrepr_free_list HEAP_CLASSREPR_FREE_LIST;
struct heap_classrepr_free_list
{
  HEAP_CLASSREPR_ENTRY *free_top;
  int free_cnt;
};

typedef struct heap_classrepr_cache HEAP_CLASSREPR_CACHE;
struct heap_classrepr_cache
{
  int num_entries;
  HEAP_CLASSREPR_ENTRY *area;
  int num_hash;
  HEAP_CLASSREPR_HASH *hash_table;
  HEAP_CLASSREPR_LOCK *lock_list;
  HEAP_CLASSREPR_LRU_LIST LRU_list;
  HEAP_CLASSREPR_FREE_LIST free_list;
  HFID *rootclass_hfid;

  pthread_mutex_t classrepr_mutex;
#ifdef DEBUG_CLASSREPR_CACHE
  int num_fix_entries;
  pthread_mutex_t num_fix_entries_mutex;
#endif				/* DEBUG_CLASSREPR_CACHE */
};

enum
{ NEED_TO_RETRY = 0, LOCK_ACQUIRED };

static HEAP_CLASSREPR_CACHE heap_Classrepr_cache = {
  -1,
  NULL,
  -1,
  NULL,
  NULL,
  {
   NULL,
   NULL},
  {
   NULL,
   -1},
  NULL,
  PTHREAD_MUTEX_INITIALIZER
#ifdef DEBUG_CLASSREPR_CACHE
    , 0, PTHREAD_MUTEX_INITIALIZER
#endif /* DEBUG_CLASSREPR_CACHE */
};

#define CLASSREPR_REPR_INCREMENT	10
#define CLASSREPR_HASH_SIZE  (heap_Classrepr_cache.num_entries * 2)
#define REPR_HASH(class_oid) (OID_PSEUDO_KEY(class_oid)%CLASSREPR_HASH_SIZE)

#define HEAP_MAYNEED_DECACHE_GUESSED_LASTREPRS(class_oid, hfid, recdes) \
  do {                                                        \
    if (heap_Classrepr != NULL && (hfid) != NULL) {             \
      if (heap_Classrepr->rootclass_hfid == NULL)               \
	heap_Classrepr->rootclass_hfid = boot_find_root_heap();   \
      if (HFID_EQ((hfid), heap_Classrepr->rootclass_hfid))      \
	(void) heap_classrepr_decache_and_lock(class_oid);  \
    }                                                         \
  } while (0)

#define HEAP_CHNGUESS_FUDGE_MININDICES (100)
#define HEAP_NBITS_IN_BYTE	     (8)
#define HEAP_NSHIFTS                   (3)	/* For multiplication/division by 8 */
#define HEAP_BITMASK                   (HEAP_NBITS_IN_BYTE - 1)
#define HEAP_NBITS_TO_NBYTES(bit_cnt)  \
  ((unsigned int)((bit_cnt) + HEAP_BITMASK) >> HEAP_NSHIFTS)
#define HEAP_NBYTES_TO_NBITS(byte_cnt) ((unsigned int)(byte_cnt) << HEAP_NSHIFTS)
#define HEAP_NBYTES_CLEARED(byte_ptr, byte_cnt) \
  memset((byte_ptr), '\0', (byte_cnt))
#define HEAP_BYTEOFFSET_OFBIT(bit_num) ((unsigned int)(bit_num) >> HEAP_NSHIFTS)
#define HEAP_BYTEGET(byte_ptr, bit_num) \
  ((unsigned char *)(byte_ptr) + HEAP_BYTEOFFSET_OFBIT(bit_num))

#define HEAP_BITMASK_INBYTE(bit_num)   \
  (1 << ((unsigned int)(bit_num) & HEAP_BITMASK))
#define HEAP_BIT_GET(byte_ptr, bit_num) \
  (*HEAP_BYTEGET(byte_ptr, bit_num) & HEAP_BITMASK_INBYTE(bit_num))
#define HEAP_BIT_SET(byte_ptr, bit_num) \
  (*HEAP_BYTEGET(byte_ptr, bit_num) = \
   *HEAP_BYTEGET(byte_ptr, bit_num) | HEAP_BITMASK_INBYTE(bit_num))
#define HEAP_BIT_CLEAR(byte_ptr, bit_num) \
  (*HEAP_BYTEGET(byte_ptr, bit_num) = \
   *HEAP_BYTEGET(byte_ptr, bit_num) & ~HEAP_BITMASK_INBYTE(bit_num))

typedef struct heap_sync_node HEAP_SYNC_NODE;
struct heap_sync_node
{
  HFID hfid;
  OID class_oid;
  HEAP_SYNC_NODE *next;
};
#define HEAP_SYNC_NODE_INITIALIZER \
  { {{NULL_FILEID, NULL_VOLID}, NULL_PAGEID}, {NULL_PAGEID, NULL_SLOTID, NULL_VOLID}, NULL}

typedef struct heap_sync_bestspace HEAP_SYNC_BESTSPACE;
struct heap_sync_bestspace
{
  int free_list_count;		/* number of entries in free */
  HEAP_SYNC_NODE *free_list;
  HEAP_SYNC_NODE *sync_list;

  bool stop_sync_bestspace;

  pthread_mutex_t sync_mutex;
};
#define HEAP_BESTSPACE_SYNC_INITIALIZER \
  { 0, NULL, NULL, false, PTHREAD_MUTEX_INITIALIZER}

typedef struct heap_bestspace_cache HEAP_BESTSPACE_CACHE;
struct heap_bestspace_cache
{
  int num_stats_entries;	/* number of cache entries in use */
  MHT_TABLE *hfid_ht;		/* HFID Hash table for best space */
  MHT_TABLE *vpid_ht;		/* VPID Hash table for best space */
  int num_alloc;
  int num_free;
  int free_list_count;		/* number of entries in free */
  HEAP_BESTSPACE_ENTRY *free_list;

  pthread_mutex_t bestspace_mutex;

  HEAP_SYNC_BESTSPACE bestspace_sync;
};

static int heap_Maxslotted_reclength;
static int heap_Slotted_overhead = 4;	/* sizeof (SPAGE_SLOT) */
static const int heap_Find_best_page_limit = 100;

static HEAP_CLASSREPR_CACHE *heap_Classrepr = NULL;

static HEAP_BESTSPACE_CACHE heap_Bestspace_cache_area =
  { 0, NULL, NULL, 0, 0, 0, NULL, PTHREAD_MUTEX_INITIALIZER,
  HEAP_BESTSPACE_SYNC_INITIALIZER
};

static HEAP_BESTSPACE_CACHE *heap_Bestspace = NULL;

static int heap_scancache_update_hinted_when_lots_space (THREAD_ENTRY *
							 thread_p,
							 HEAP_SCANCACHE *,
							 PAGE_PTR);

static int heap_classrepr_initialize_cache (void);
static int heap_classrepr_finalize_cache (void);
#if !defined(NDEBUG)
static bool heap_classrepr_has_lock (THREAD_ENTRY * thread_p,
				     const OID * class_oid);
#endif
static int heap_classrepr_lock_class (THREAD_ENTRY * thread_p,
				      HEAP_CLASSREPR_HASH * hash_anchor,
				      const OID * class_oid);
static int heap_classrepr_unlock_class_with_hash_anchor (HEAP_CLASSREPR_HASH *
							 hash_anchor,
							 const OID *
							 class_oid);

static int heap_classrepr_dump (THREAD_ENTRY * thread_p, FILE * fp,
				const OID * class_oid,
				const OR_CLASSREP * repr);
#ifdef DEBUG_CLASSREPR_CACHE
static int heap_classrepr_dump_cache (bool simple_dump);
#endif /* DEBUG_CLASSREPR_CACHE */

static int heap_classrepr_entry_reset (HEAP_CLASSREPR_ENTRY * cache_entry);
static int heap_classrepr_entry_remove_from_LRU (HEAP_CLASSREPR_ENTRY *
						 cache_entry);
static HEAP_CLASSREPR_ENTRY *heap_classrepr_entry_alloc (void);
static int heap_classrepr_entry_free (HEAP_CLASSREPR_ENTRY * cache_entry);

static void heap_bestspace_update (THREAD_ENTRY * thread_p,
				   PAGE_PTR pgptr, const HFID * hfid,
				   int prev_freespace);
static HEAP_FINDSPACE heap_find_page_in_bestspace_cache (THREAD_ENTRY *
							 thread_p,
							 const HFID * hfid,
							 int record_length,
							 int needed_space,
							 HEAP_SCANCACHE *
							 scan_cache,
							 PAGE_PTR * pgptr);
static PAGE_PTR heap_find_best_page (THREAD_ENTRY * thread_p,
				     const HFID * hfid,
				     int needed_space, bool isnew_rec,
				     int newrec_size,
				     HEAP_SCANCACHE * space_cache);
static int heap_read_full_search_vpid (THREAD_ENTRY * thread_p,
				       VPID * full_search_vpid,
				       const HFID * hfid);
static int heap_write_full_search_vpid (THREAD_ENTRY * thread_p,
					const HFID * hfid,
					VPID * full_search_vpid);
static int heap_bestspace_sync (THREAD_ENTRY * thread_p, DB_BIGINT * num_recs,
				const HFID * hfid, bool scan_all);
static PAGE_PTR heap_get_last_page (THREAD_ENTRY * thread_p,
				    const HFID * hfid,
				    HEAP_SCANCACHE * scan_cache,
				    VPID * last_vpid);
static bool heap_link_to_new (THREAD_ENTRY * thread_p, const VFID * vfid,
			      const VPID * new_vpid,
			      HEAP_CHAIN_TOLAST * link);

static bool heap_vpid_init_new (THREAD_ENTRY * thread_p, const VFID * vfid,
				const FILE_TYPE file_type, const VPID * vpid,
				INT32 ignore_npages, void *xchain);
static PAGE_PTR heap_vpid_alloc (THREAD_ENTRY * thread_p, const HFID * hfid,
				 int needed_space,
				 HEAP_SCANCACHE * scan_cache);
#if defined (ENABLE_UNUSED_FUNCTION)
static VPID *heap_vpid_remove (THREAD_ENTRY * thread_p, const HFID * hfid,
			       HEAP_HDR_STATS * heap_hdr, VPID * rm_vpid);
#endif
static int heap_vpid_next (const HFID * hfid, PAGE_PTR pgptr,
			   VPID * next_vpid);
#if defined (ENABLE_UNUSED_FUNCTION)
static int heap_vpid_prev (const HFID * hfid, PAGE_PTR pgptr,
			   VPID * prev_vpid);
#endif

static HFID *heap_create_internal (THREAD_ENTRY * thread_p, HFID * hfid,
				   int exp_npgs, const OID * class_oid);
#if defined(RYE_DEBUG)
static DISK_ISVALID heap_hfid_isvalid (HFID * hfid);
#endif /* RYE_DEBUG */
static int heap_insert_internal (THREAD_ENTRY * thread_p, const HFID * hfid,
				 OID * oid, RECDES * recdes,
				 HEAP_SCANCACHE * scan_cache,
				 bool ishome_insert, int guess_sumlen,
				 const OID * class_oid);
static PAGE_PTR heap_find_slot_for_insert (THREAD_ENTRY * thread_p,
					   const HFID * hfid,
					   HEAP_SCANCACHE *
					   scan_cache, OID * oid,
					   RECDES * recdes, OID * class_oid);
static int heap_insert_with_lock_internal (THREAD_ENTRY * thread_p,
					   const HFID * hfid, OID * oid,
					   OID * class_oid, RECDES * recdes,
					   HEAP_SCANCACHE * scan_cache,
					   bool ishome_insert,
					   int guess_sumlen);
static const OID *heap_delete_internal (THREAD_ENTRY * thread_p,
					const HFID * hfid, const OID * oid,
					HEAP_SCANCACHE * scan_cache,
					bool ishome_delete, OID * class_oid);
static int heap_scancache_start_internal (THREAD_ENTRY * thread_p,
					  HEAP_SCANCACHE * scan_cache,
					  const HFID * hfid,
					  const bool vpid_alloc,
					  const OID * class_oid,
					  int cache_last_fix_page);
static int heap_scancache_force_modify (THREAD_ENTRY * thread_p,
					HEAP_SCANCACHE * scan_cache);
static int heap_scancache_reset_modify (THREAD_ENTRY * thread_p,
					HEAP_SCANCACHE * scan_cache,
					const HFID * hfid,
					const OID * class_oid);
static int heap_scancache_quick_start_internal (HEAP_SCANCACHE * scan_cache);
static int heap_scancache_quick_end (THREAD_ENTRY * thread_p,
				     HEAP_SCANCACHE * scan_cache);
static int heap_scancache_end_internal (THREAD_ENTRY * thread_p,
					HEAP_SCANCACHE * scan_cache,
					bool scan_state);

static SCAN_CODE heap_get_internal (THREAD_ENTRY * thread_p, OID * class_oid,
				    OID * oid, RECDES * recdes,
				    HEAP_SCANCACHE * scan_cache,
				    int ispeeking);
static int heap_estimate_avg_length (THREAD_ENTRY * thread_p,
				     const HFID * hfid);
static int heap_get_capacity (THREAD_ENTRY * thread_p, const HFID * hfid,
			      INT64 * num_recs, INT64 * num_recs_relocated,
			      INT64 * num_recs_inovf, INT64 * num_pages,
			      int *avg_freespace, int *avg_freespace_nolast,
			      int *avg_reclength, int *avg_overhead);
#if 0				/* TODO: remove unused */
static int heap_moreattr_attrinfo (int attrid,
				   HEAP_CACHE_ATTRINFO * attr_info);
#endif

static int heap_attrinfo_recache_attrepr (HEAP_CACHE_ATTRINFO * attr_info,
					  int islast_reset);
static int heap_attrinfo_recache (THREAD_ENTRY * thread_p, REPR_ID reprid,
				  HEAP_CACHE_ATTRINFO * attr_info);
static int heap_attrinfo_check (const OID * inst_oid,
				HEAP_CACHE_ATTRINFO * attr_info);
static int heap_attrinfo_set_uninitialized (THREAD_ENTRY * thread_p,
					    OID * inst_oid, RECDES * recdes,
					    HEAP_CACHE_ATTRINFO * attr_info);
static int heap_attrinfo_get_disksize (HEAP_CACHE_ATTRINFO * attr_info,
				       int *offset_size_ptr);

static int heap_attrvalue_read (RECDES * recdes, HEAP_ATTRVALUE * value,
				HEAP_CACHE_ATTRINFO * attr_info);

static int heap_idxkey_get_value (RECDES * recdes, OR_ATTRIBUTE * att,
				  DB_VALUE * value,
				  HEAP_CACHE_ATTRINFO * attr_info);
#if defined (ENABLE_UNUSED_FUNCTION)
static OR_ATTRIBUTE *heap_locate_attribute (ATTR_ID attrid,
					    HEAP_CACHE_ATTRINFO * attr_info);
#endif

static int heap_classrepr_find_shard_key_column_id (THREAD_ENTRY * thread_p,
						    OR_CLASSREP * classrepr);

static int heap_idxkey_key_get (const OID * inst_oid, RECDES * recdes,
				DB_IDXKEY * key,
				OR_INDEX * indexp,
				HEAP_CACHE_ATTRINFO * attrinfo);

static int heap_dump_hdr (FILE * fp, HEAP_HDR_STATS * heap_hdr);

#if defined (ENABLE_UNUSED_FUNCTION)
static DISK_ISVALID heap_chkreloc_start (HEAP_CHKALL_RELOCOIDS * chk);
static DISK_ISVALID heap_chkreloc_end (HEAP_CHKALL_RELOCOIDS * chk);
static int heap_chkreloc_print_notfound (const void *ignore_reloc_oid,
					 void *ent, void *xchk);
static DISK_ISVALID heap_chkreloc_next (HEAP_CHKALL_RELOCOIDS * chk,
					PAGE_PTR pgptr);
#endif

static int heap_bestspace_initialize (void);
static int heap_bestspace_finalize (void);

static int heap_get_spage_type (void);

static int heap_bestspace_remove (THREAD_ENTRY * thread_p,
				  HEAP_BESTSPACE * best, const HFID * hfid);
#if defined (ENABLE_UNUSED_FUNCTION)
static int heap_bestspace_del_entry_by_vpid (THREAD_ENTRY * thread_p,
					     VPID * vpid);
#endif
static int heap_bestspace_append_hfid_to_sync_list (THREAD_ENTRY * thread_p,
						    const HFID * hfid,
						    const OID * class_oid);
static int heap_bestspace_del_all_entry_by_hfid (THREAD_ENTRY * thread_p,
						 const HFID * hfid);
static HEAP_SYNC_NODE *heap_bestspace_remove_sync_list (THREAD_ENTRY *
							thread_p);
static int heap_bestspace_remove_duplicated_hfid (THREAD_ENTRY * thread_p,
						  HEAP_SYNC_NODE * sync_list);
static int heap_bestspace_add (THREAD_ENTRY * thread_p, const HFID * hfid,
			       VPID * vpid, int freespace);
static int heap_bestspace_free_entry (THREAD_ENTRY * thread_p, void *data,
				      void *args);

#if defined (ENABLE_UNUSED_FUNCTION)
static void heap_bestspace_dump (FILE * out_fp);
static int heap_bestspace_print_hash_entry (FILE * outfp, const void *key,
					    void *ent, void *ignore);
#endif
static unsigned int heap_hash_vpid (const void *key_vpid,
				    unsigned int htsize);
static int heap_compare_vpid (const void *key_vpid1, const void *key_vpid2);
static unsigned int heap_hash_hfid (const void *key_hfid,
				    unsigned int htsize);
static int heap_compare_hfid (const void *key_hfid1, const void *key_hfid2);

#if defined (ENABLE_UNUSED_FUNCTION)
static int fill_string_to_buffer (char **start, char *end, const char *str);
#endif


/*
 * heap_hash_vpid () - Hash a page identifier
 *   return: hash value
 *   key_vpid(in): VPID to hash
 *   htsize(in): Size of hash table
 */
static unsigned int
heap_hash_vpid (const void *key_vpid, unsigned int htsize)
{
  const VPID *vpid = (VPID *) key_vpid;

  return ((vpid->pageid | ((unsigned int) vpid->volid) << 24) % htsize);
}

/*
 * heap_compare_vpid () - Compare two vpids keys for hashing
 *   return: int (key_vpid1 == key_vpid2 ?)
 *   key_vpid1(in): First key
 *   key_vpid2(in): Second key
 */
static int
heap_compare_vpid (const void *key_vpid1, const void *key_vpid2)
{
  const VPID *vpid1 = (VPID *) key_vpid1;
  const VPID *vpid2 = (VPID *) key_vpid2;

  return VPID_EQ (vpid1, vpid2);
}

/*
 * heap_hash_hfid () - Hash a file identifier
 *   return: hash value
 *   key_hfid(in): HFID to hash
 *   htsize(in): Size of hash table
 */
static unsigned int
heap_hash_hfid (const void *key_hfid, unsigned int htsize)
{
  const HFID *hfid = (HFID *) key_hfid;

  return ((hfid->hpgid | ((unsigned int) hfid->vfid.volid) << 24) % htsize);
}

/*
 * heap_compare_hfid () - Compare two hfids keys for hashing
 *   return: int (key_hfid1 == key_hfid2 ?)
 *   key_hfid1(in): First key
 *   key_hfid2(in): Second key
 */
static int
heap_compare_hfid (const void *key_hfid1, const void *key_hfid2)
{
  const HFID *hfid1 = (HFID *) key_hfid1;
  const HFID *hfid2 = (HFID *) key_hfid2;

  return HFID_EQ (hfid1, hfid2);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_bestspace_dump () -
 *   return:
 *
 *     out_fp(in/out):
 */
static void
heap_bestspace_dump (FILE * out_fp)
{
  int rc;

  rc = pthread_mutex_lock (&heap_Bestspace->bestspace_mutex);

  mht_dump (out_fp, heap_Bestspace->hfid_ht, false,
	    heap_bestspace_print_hash_entry, NULL);
  mht_dump (out_fp, heap_Bestspace->vpid_ht, false,
	    heap_bestspace_print_hash_entry, NULL);

  pthread_mutex_unlock (&heap_Bestspace->bestspace_mutex);
}

/*
 * heap_bestspace_print_hash_entry()- Print the entry
 *                              Will be used by mht_dump() function
 *  return:
 *
 *   outfp(in):
 *   key(in):
 *   ent(in):
 *   ignore(in):
 */
static int
heap_bestspace_print_hash_entry (FILE * outfp, UNUSED_ARG const void *key,
				 void *ent, UNUSED_ARG void *ignore)
{
  HEAP_BESTSPACE_ENTRY *entry = (HEAP_BESTSPACE_ENTRY *) ent;

  fprintf (outfp, "HFID:Volid = %6d, Fileid = %6d, Header-pageid = %6d"
	   ",VPID = %4d|%4d, freespace = %6d\n",
	   entry->hfid.vfid.volid, entry->hfid.vfid.fileid, entry->hfid.hpgid,
	   entry->best.vpid.volid, entry->best.vpid.pageid,
	   entry->best.freespace);

  return true;
}
#endif

/*
 * heap_stats_entry_free () - release all memory occupied by an best space
 *   return:  NO_ERROR
 *   data(in): a best space associated with the key
 *   args(in): NULL (not used here, but needed by mht_map)
 */
static int
heap_bestspace_free_entry (UNUSED_ARG THREAD_ENTRY * thread_p, void *data,
			   UNUSED_ARG void *args)
{
  HEAP_BESTSPACE_ENTRY *ent;

  ent = (HEAP_BESTSPACE_ENTRY *) data;
  assert_release (ent != NULL);

  if (ent)
    {
      if (heap_Bestspace->free_list_count < HEAP_BESTSPACE_FREELIST_SIZE)
	{
	  ent->next = heap_Bestspace->free_list;
	  heap_Bestspace->free_list = ent;

	  heap_Bestspace->free_list_count++;
	}
      else
	{
	  free_and_init (ent);

	  heap_Bestspace->num_free++;
	}
    }

  return NO_ERROR;
}

/*
 * heap_bestspace_add () -
 * return: NO_ERROR or error code
 *
 *   hfid(in):
 *   vpid(in):
 *   freespace(in):
 */
static int
heap_bestspace_add (THREAD_ENTRY * thread_p, const HFID * hfid,
		    VPID * vpid, int freespace)
{
  HEAP_BESTSPACE_ENTRY *ent;
  int error_code = NO_ERROR;

  assert (file_find_page (thread_p, &(hfid->vfid), vpid) == true);
  assert (prm_get_integer_value (PRM_ID_HF_MAX_BESTSPACE_ENTRIES) > 0);

  pthread_mutex_lock (&heap_Bestspace->bestspace_mutex);

  ent = (HEAP_BESTSPACE_ENTRY *) mht_get (heap_Bestspace->vpid_ht, vpid);

  if (ent)
    {
      ent->best.freespace = freespace;
      goto end;
    }

  if (heap_Bestspace->num_stats_entries >=
      prm_get_integer_value (PRM_ID_HF_MAX_BESTSPACE_ENTRIES))
    {
      error_code = ER_HF_MAX_BESTSPACE_ENTRIES;

      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_HF_MAX_BESTSPACE_ENTRIES, 1,
	      prm_get_integer_value (PRM_ID_HF_MAX_BESTSPACE_ENTRIES));

      mnt_stats_counter (thread_p, MNT_STATS_HEAP_STATS_BESTSPACE_MAXED, 1);

      goto exit_on_error;
    }

  if (heap_Bestspace->free_list_count > 0)
    {
      assert_release (heap_Bestspace->free_list != NULL);

      ent = heap_Bestspace->free_list;
      if (ent == NULL)
	{
	  assert (heap_Bestspace->free_list != NULL);

	  error_code = ER_FAILED;

	  goto exit_on_error;
	}
      heap_Bestspace->free_list = ent->next;
      ent->next = NULL;

      heap_Bestspace->free_list_count--;
    }
  else
    {
      ent = (HEAP_BESTSPACE_ENTRY *) malloc (sizeof (HEAP_BESTSPACE_ENTRY));
      if (ent == NULL)
	{
	  error_code = ER_OUT_OF_VIRTUAL_MEMORY;

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (HEAP_BESTSPACE_ENTRY));

	  goto exit_on_error;
	}

      heap_Bestspace->num_alloc++;
    }

  HFID_COPY (&ent->hfid, hfid);
  ent->best.vpid = *vpid;
  ent->best.freespace = freespace;
  ent->next = NULL;

  if (mht_put (heap_Bestspace->vpid_ht, &ent->best.vpid, ent) == NULL)
    {
      assert_release (false);

      error_code = er_errid ();
      if (error_code == NO_ERROR)
	{
	  assert (error_code != NO_ERROR);

	  error_code = ER_FAILED;
	}
      goto exit_on_error;
    }

  if (mht_put_new (heap_Bestspace->hfid_ht, &ent->hfid, ent) == NULL)
    {
      assert_release (false);
      (void) mht_rem (heap_Bestspace->vpid_ht, &ent->best.vpid, NULL, NULL);

      error_code = er_errid ();
      if (error_code == NO_ERROR)
	{
	  assert (error_code != NO_ERROR);

	  error_code = ER_FAILED;
	}
      goto exit_on_error;
    }

  heap_Bestspace->num_stats_entries++;
  mnt_stats_gauge (thread_p, MNT_STATS_HEAP_STATS_BESTSPACE_ENTRIES,
		   heap_Bestspace->num_stats_entries);

end:

  assert (mht_count (heap_Bestspace->vpid_ht) ==
	  mht_count (heap_Bestspace->hfid_ht));

  pthread_mutex_unlock (&heap_Bestspace->bestspace_mutex);

  return NO_ERROR;

exit_on_error:
  assert (mht_count (heap_Bestspace->vpid_ht) ==
	  mht_count (heap_Bestspace->hfid_ht));

  assert (error_code != NO_ERROR);

  if (ent != NULL)
    {
      (void) heap_bestspace_free_entry (thread_p, ent, NULL);
      ent = NULL;
    }

  pthread_mutex_unlock (&heap_Bestspace->bestspace_mutex);

  return error_code;
}

/*
 * heap_bestspace_del_all_entry_by_hfid () -
 *   return: deleted count
 *
 *   hfid(in):
 */
static int
heap_bestspace_del_all_entry_by_hfid (THREAD_ENTRY * thread_p,
				      const HFID * hfid)
{
  HEAP_BESTSPACE_ENTRY *ent;
  HEAP_SYNC_NODE *sync_node;
  int del_cnt = 0;

  pthread_mutex_lock (&heap_Bestspace->bestspace_mutex);

  while ((ent = (HEAP_BESTSPACE_ENTRY *) mht_get2 (heap_Bestspace->hfid_ht,
						   hfid, NULL)) != NULL)
    {
      (void) mht_rem2 (heap_Bestspace->hfid_ht, &ent->hfid, ent, NULL, NULL);
      (void) mht_rem (heap_Bestspace->vpid_ht, &ent->best.vpid, NULL, NULL);
      (void) heap_bestspace_free_entry (thread_p, ent, NULL);
      ent = NULL;

      del_cnt++;
    }

  assert (del_cnt <= heap_Bestspace->num_stats_entries);

  heap_Bestspace->num_stats_entries -= del_cnt;

  mnt_stats_gauge (thread_p, MNT_STATS_HEAP_STATS_BESTSPACE_ENTRIES,
		   heap_Bestspace->num_stats_entries);

  assert (mht_count (heap_Bestspace->vpid_ht) ==
	  mht_count (heap_Bestspace->hfid_ht));
  pthread_mutex_unlock (&heap_Bestspace->bestspace_mutex);

  pthread_mutex_lock (&heap_Bestspace->bestspace_sync.sync_mutex);

  heap_Bestspace->bestspace_sync.stop_sync_bestspace = true;

  while (heap_Bestspace->bestspace_sync.sync_list != NULL)
    {
      sync_node = heap_Bestspace->bestspace_sync.sync_list;
      heap_Bestspace->bestspace_sync.sync_list =
	heap_Bestspace->bestspace_sync.sync_list->next;

      sync_node->next = heap_Bestspace->bestspace_sync.free_list;
      heap_Bestspace->bestspace_sync.free_list = sync_node;
      heap_Bestspace->bestspace_sync.free_list_count++;
    }

  pthread_mutex_unlock (&heap_Bestspace->bestspace_sync.sync_mutex);

  return del_cnt;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_bestspace_del_entry_by_vpid () -
 *   return: NO_ERROR
 *
 *  vpid(in):
 */
static int
heap_bestspace_del_entry_by_vpid (THREAD_ENTRY * thread_p, VPID * vpid)
{
  HEAP_BESTSPACE_ENTRY *ent;
  int rc;

  rc = pthread_mutex_lock (&heap_Bestspace->bestspace_mutex);

  ent = (HEAP_BESTSPACE_ENTRY *) mht_get (heap_Bestspace->vpid_ht, vpid);
  if (ent == NULL)
    {
      goto end;
    }

  (void) mht_rem2 (heap_Bestspace->hfid_ht, &ent->hfid, ent, NULL, NULL);
  (void) mht_rem (heap_Bestspace->vpid_ht, &ent->best.vpid, NULL, NULL);
  (void) heap_bestspace_free_entry (thread_p, ent, NULL);
  ent = NULL;

  heap_Bestspace->num_stats_entries -= 1;

  mnt_stats_gauge (thread_p, MNT_STATS_HEAP_STATS_BESTSPACE_ENTRIES,
		   heap_Bestspace->num_stats_entries);

end:
  assert (mht_count (heap_Bestspace->vpid_ht) ==
	  mht_count (heap_Bestspace->hfid_ht));

  pthread_mutex_unlock (&heap_Bestspace->bestspace_mutex);

  return NO_ERROR;
}
#endif

/*
 * heap_bestspace_remove () -
 *   return: NO_ERROR or error code
 *
 *  best(out):
 *  hfid(in):
 */
static int
heap_bestspace_remove (THREAD_ENTRY * thread_p, HEAP_BESTSPACE * best,
		       const HFID * hfid)
{
  HEAP_BESTSPACE_ENTRY *ent = NULL;
  int error_code = NO_ERROR;

  best->freespace = -1;
  VPID_SET_NULL (&best->vpid);

  pthread_mutex_lock (&heap_Bestspace->bestspace_mutex);

  ent =
    (HEAP_BESTSPACE_ENTRY *) mht_get2 (heap_Bestspace->hfid_ht, hfid, NULL);
  if (ent == NULL)
    {
      /* not found */
      pthread_mutex_unlock (&heap_Bestspace->bestspace_mutex);

      return NO_ERROR;
    }

  assert (HFID_EQ (hfid, &ent->hfid));
  error_code = mht_rem2 (heap_Bestspace->hfid_ht, &ent->hfid,
			 ent, NULL, NULL);
  if (error_code != NO_ERROR)
    {
      goto error_on_exit;
    }

  error_code = mht_rem (heap_Bestspace->vpid_ht, &ent->best.vpid, NULL, NULL);
  if (error_code != NO_ERROR)
    {
      mht_put_new (heap_Bestspace->hfid_ht, &ent->hfid, ent);

      goto error_on_exit;
    }

  *best = ent->best;

  (void) heap_bestspace_free_entry (thread_p, ent, NULL);

  heap_Bestspace->num_stats_entries -= 1;

  mnt_stats_gauge (thread_p, MNT_STATS_HEAP_STATS_BESTSPACE_ENTRIES,
		   heap_Bestspace->num_stats_entries);


  assert (mht_count (heap_Bestspace->vpid_ht) ==
	  mht_count (heap_Bestspace->hfid_ht));

  pthread_mutex_unlock (&heap_Bestspace->bestspace_mutex);

  assert (file_find_page (thread_p, &(hfid->vfid), &(best->vpid)) == true);

  return NO_ERROR;

error_on_exit:
  if (error_code == NO_ERROR)
    {
      assert (error_code != NO_ERROR);

      error_code = ER_FAILED;
    }

  assert (mht_count (heap_Bestspace->vpid_ht) ==
	  mht_count (heap_Bestspace->hfid_ht));

  pthread_mutex_unlock (&heap_Bestspace->bestspace_mutex);

  return error_code;
}

/*
 * heap_bestspace_append_hfid_to_sync_list () -
 *   return: NO_ERROR or error code
 *
 *  hfid(in):
 */
static int
heap_bestspace_append_hfid_to_sync_list (UNUSED_ARG THREAD_ENTRY * thread_p,
					 const HFID * hfid,
					 const OID * class_oid)
{
  int error_code = NO_ERROR;
  HEAP_SYNC_NODE *sync_node;

  pthread_mutex_lock (&heap_Bestspace->bestspace_sync.sync_mutex);

  if (heap_Bestspace->bestspace_sync.free_list_count > 0)
    {
      assert_release (heap_Bestspace->bestspace_sync.free_list != NULL);

      sync_node = heap_Bestspace->bestspace_sync.free_list;
      if (sync_node == NULL)
	{
	  assert (heap_Bestspace->bestspace_sync.free_list != NULL);

	  error_code = ER_FAILED;

	  goto exit_on_error;
	}
      heap_Bestspace->bestspace_sync.free_list = sync_node->next;
      sync_node->next = NULL;

      heap_Bestspace->bestspace_sync.free_list_count--;
    }
  else
    {
      sync_node = (HEAP_SYNC_NODE *) malloc (sizeof (HEAP_SYNC_NODE));
      if (sync_node == NULL)
	{
	  error_code = ER_OUT_OF_VIRTUAL_MEMORY;

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (HEAP_SYNC_NODE));

	  goto exit_on_error;
	}
    }

  sync_node->hfid = *hfid;
  sync_node->class_oid = *class_oid;

  /* append sync_node to sync_list of heap_Bestspace->bestspace_sync */
  sync_node->next = heap_Bestspace->bestspace_sync.sync_list;
  heap_Bestspace->bestspace_sync.sync_list = sync_node;

  pthread_mutex_unlock (&heap_Bestspace->bestspace_sync.sync_mutex);

  return NO_ERROR;

exit_on_error:
  if (error_code == NO_ERROR)
    {
      assert (error_code != NO_ERROR);

      error_code = ER_FAILED;
    }

  pthread_mutex_unlock (&heap_Bestspace->bestspace_sync.sync_mutex);

  return error_code;
}

/*
 * heap_bestspace_remove_sync_list () -
 *   return: list
 */
static HEAP_SYNC_NODE *
heap_bestspace_remove_sync_list (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  HEAP_SYNC_NODE *sync_list = NULL;

  pthread_mutex_lock (&heap_Bestspace->bestspace_sync.sync_mutex);

  sync_list = heap_Bestspace->bestspace_sync.sync_list;
  heap_Bestspace->bestspace_sync.sync_list = NULL;

  pthread_mutex_unlock (&heap_Bestspace->bestspace_sync.sync_mutex);

  return sync_list;
}

/*
 * heap_bestspace_remove_sync_list () -
 *   return: NO_ERROR
 *
 *  sync_list(in/out):
 */
static int
heap_bestspace_remove_duplicated_hfid (UNUSED_ARG THREAD_ENTRY * thread_p,
				       HEAP_SYNC_NODE * sync_list)
{
  HEAP_SYNC_NODE *current_node;

  while (sync_list != NULL)
    {
      if (!HFID_IS_NULL (&sync_list->hfid))
	{
	  /* remove duplicated node */
	  current_node = sync_list->next;
	  while (current_node != NULL)
	    {
	      if (HFID_EQ (&current_node->hfid, &sync_list->hfid))
		{
		  HFID_SET_NULL (&current_node->hfid);
		}

	      current_node = current_node->next;
	    }
	}
      sync_list = sync_list->next;
    }

  return NO_ERROR;
}

/*
 * Scan page buffer and latch page manipulation
 */

static PAGE_PTR heap_scan_pb_lock_and_fetch (THREAD_ENTRY * thread_p,
					     VPID * vpid_ptr,
					     LOCK lock,
					     HEAP_SCANCACHE * scan_cache,
					     PAGE_TYPE ptype);

/*
 * heap_scan_pb_lock_and_fetch () -
 *   return:
 *   vpid_ptr(in):
 *   lock(in):
 *   scan_cache(in):
 */
static PAGE_PTR
heap_scan_pb_lock_and_fetch (THREAD_ENTRY * thread_p, VPID * vpid_ptr,
			     LOCK lock,
			     HEAP_SCANCACHE * scan_cache, PAGE_TYPE ptype)
{
  PAGE_PTR pgptr = NULL;
  LOCK page_lock;
  const HFID *hfid;

  assert (lock == S_LOCK || lock == X_LOCK);
  assert (ptype == PAGE_HEAP_HEADER || ptype == PAGE_HEAP);

  if (scan_cache != NULL)
    {
      if (scan_cache->page_latch == NULL_LOCK)
	{
	  page_lock = NULL_LOCK;
	}
      else
	{
	  assert (scan_cache->page_latch >= NULL_LOCK);
	  assert (lock >= NULL_LOCK);
	  page_lock = lock_Conv[scan_cache->page_latch][lock];
	  assert (page_lock != NA_LOCK);
	  assert (page_lock != NULL_LOCK);
	}

      hfid = &(scan_cache->hfid);
    }
  else
    {
      page_lock = lock;

      hfid = NULL;
    }

  pgptr = heap_pgbuf_fix (thread_p, hfid, vpid_ptr,
			  (page_lock ==
			   S_LOCK) ? PGBUF_LATCH_READ : PGBUF_LATCH_WRITE,
			  PGBUF_UNCONDITIONAL_LATCH, ptype);

  return pgptr;
}

/*
 * heap_is_big_length () -
 *   return: true/false
 *   length(in):
 */
bool
heap_is_big_length (int length)
{
  return (length > heap_Maxslotted_reclength) ? true : false;
}

/*
 * heap_get_spage_type () -
 *   return: the type of the slotted page of the heap file.
 */
static int
heap_get_spage_type (void)
{
  return ANCHORED;
}

/*
 * heap_scancache_update_hinted_when_lots_space () -
 *   return: NO_ERROR
 *   scan_cache(in):
 *   pgptr(in):
 */
static int
heap_scancache_update_hinted_when_lots_space (THREAD_ENTRY * thread_p,
					      HEAP_SCANCACHE * scan_cache,
					      PAGE_PTR pgptr)
{
  VPID *this_vpid;
  int ret = NO_ERROR;
  int freespace;

  this_vpid = pgbuf_get_vpid_ptr (pgptr);
  assert_release (this_vpid != NULL);
  if (!VPID_EQ (&scan_cache->collect_nxvpid, this_vpid))
    {
      /* This page's statistics was already collected. */
      return NO_ERROR;
    }

  freespace = spage_get_free_space_without_saving (thread_p, pgptr);
  if (freespace > HEAP_DROP_FREE_SPACE)
    {
      (void) heap_bestspace_add (thread_p, &scan_cache->hfid,
				 this_vpid, freespace);
    }

  ret = heap_vpid_next (&scan_cache->hfid, pgptr,
			&scan_cache->collect_nxvpid);

  return ret;
}

/*
 * heap_classrepr_initialize_cache () - Initialize the class representation cache
 *   return: NO_ERROR
 */
static int
heap_classrepr_initialize_cache (void)
{
  HEAP_CLASSREPR_ENTRY *cache_entry;
  HEAP_CLASSREPR_HASH *hash_entry;
  int i, ret = NO_ERROR;

  if (heap_Classrepr != NULL)
    {
      ret = heap_classrepr_finalize_cache ();
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }

  pthread_mutex_init (&heap_Classrepr_cache.classrepr_mutex, NULL);

  /* initialize hash entries table */
  heap_Classrepr_cache.num_entries = HEAP_CLASSREPR_MAXCACHE;

  heap_Classrepr_cache.area =
    (HEAP_CLASSREPR_ENTRY *) malloc (sizeof (HEAP_CLASSREPR_ENTRY)
				     * heap_Classrepr_cache.num_entries);
  if (heap_Classrepr_cache.area == NULL)
    {
      ret = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 1,
	      sizeof (HEAP_CLASSREPR_ENTRY) *
	      heap_Classrepr_cache.num_entries);
      goto exit_on_error;
    }

  cache_entry = heap_Classrepr_cache.area;
  for (i = 0; i < heap_Classrepr_cache.num_entries; i++)
    {
      cache_entry[i].idx = i;
      cache_entry[i].fcnt = 0;
      cache_entry[i].zone = ZONE_FREE;
      cache_entry[i].next_wait_thrd = NULL;
      cache_entry[i].hash_next = NULL;
      cache_entry[i].lru_prev = NULL;
      cache_entry[i].lru_next = NULL;
      cache_entry[i].force_decache = false;

      cache_entry[i].free_next = NULL;
      if (i < heap_Classrepr_cache.num_entries - 1)
	{
	  cache_entry[i].free_next = &cache_entry[i + 1];
	}

      OID_SET_NULL (&cache_entry[i].class_oid);
      cache_entry[i].max_reprid = DEFAULT_REPR_INCREMENT;
      cache_entry[i].repr = (OR_CLASSREP **)
	malloc (cache_entry[i].max_reprid * sizeof (OR_CLASSREP *));
      if (cache_entry[i].repr == NULL)
	{
	  ret = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 1,
		  cache_entry[i].max_reprid * sizeof (OR_CLASSREP *));
	  goto exit_on_error;
	}

      memset (cache_entry[i].repr, 0,
	      cache_entry[i].max_reprid * sizeof (OR_CLASSREP *));

      cache_entry[i].last_reprid = NULL_REPRID;
    }

  /* initialize hash bucket table */
  heap_Classrepr_cache.num_hash = CLASSREPR_HASH_SIZE;
  heap_Classrepr_cache.hash_table = (HEAP_CLASSREPR_HASH *)
    malloc (heap_Classrepr_cache.num_hash * sizeof (HEAP_CLASSREPR_HASH));
  if (heap_Classrepr_cache.hash_table == NULL)
    {
      ret = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 1,
	      heap_Classrepr_cache.num_hash * sizeof (HEAP_CLASSREPR_HASH));
      goto exit_on_error;
    }

  hash_entry = heap_Classrepr_cache.hash_table;
  for (i = 0; i < heap_Classrepr_cache.num_hash; i++)
    {
      hash_entry[i].idx = i;
      hash_entry[i].hash_next = NULL;
      hash_entry[i].lock_next = NULL;
    }

  /* initialize hash lock list */
  heap_Classrepr_cache.lock_list = NULL;

  /* initialize LRU list */
  heap_Classrepr_cache.LRU_list.LRU_top = NULL;
  heap_Classrepr_cache.LRU_list.LRU_bottom = NULL;

  /* initialize free list */
  heap_Classrepr_cache.free_list.free_top = &heap_Classrepr_cache.area[0];
  heap_Classrepr_cache.free_list.free_cnt = heap_Classrepr_cache.num_entries;

  heap_Classrepr = &heap_Classrepr_cache;

  return ret;

exit_on_error:

  heap_Classrepr_cache.num_entries = 0;

  return (ret == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_classrepr_finalize_cache () - Destroy any cached structures
 *   return: NO_ERROR
 *
 * Note: Any cached representations are deallocated at this moment and
 * the hash table is also removed.
 */
static int
heap_classrepr_finalize_cache (void)
{
  HEAP_CLASSREPR_ENTRY *cache_entry;
  HEAP_CLASSREPR_LOCK *lock_entry;
  int i, j;
  int ret = NO_ERROR;

  if (heap_Classrepr == NULL)
    {
      return NO_ERROR;		/* nop */
    }

#ifdef DEBUG_CLASSREPR_CACHE
  ret = heap_classrepr_dump_anyfixed ();
  if (ret != NO_ERROR)
    {
      return ret;
    }
#endif /* DEBUG_CLASSREPR_CACHE */

  /* finalize hash entries table */
  cache_entry = heap_Classrepr_cache.area;
  for (i = 0; i < heap_Classrepr_cache.num_entries; i++)
    {
      for (j = 0; j <= cache_entry[i].last_reprid; j++)
	{
	  if (cache_entry[i].repr[j] != NULL)
	    {
	      or_free_classrep (cache_entry[i].repr[j]);
	      cache_entry[i].repr[j] = NULL;
	    }
	}
      free_and_init (cache_entry[i].repr);
    }
  free_and_init (heap_Classrepr_cache.area);
  heap_Classrepr_cache.num_entries = -1;

  /* finalize hash bucket table */
  heap_Classrepr_cache.num_hash = -1;
  free_and_init (heap_Classrepr_cache.hash_table);

  /* finalize lock_list */
  while (heap_Classrepr_cache.lock_list != NULL)
    {
      lock_entry = heap_Classrepr_cache.lock_list;
      heap_Classrepr_cache.lock_list = lock_entry->lock_next;

      free_and_init (lock_entry);
    }

  pthread_mutex_destroy (&heap_Classrepr_cache.classrepr_mutex);

  heap_Classrepr = NULL;

  return ret;
}

/*
 * heap_classrepr_entry_reset () -
 *   return: NO_ERROR
 *   cache_entry(in):
 *
 * Note: Reset the given class representation entry.
 */
static int
heap_classrepr_entry_reset (HEAP_CLASSREPR_ENTRY * cache_entry)
{
  int i;
  int ret = NO_ERROR;

  if (cache_entry == NULL)
    {
      return NO_ERROR;		/* nop */
    }

  assert (cache_entry->fcnt == 0);

  /* free all classrepr */
  for (i = 0; i <= cache_entry->last_reprid; i++)
    {
      if (cache_entry->repr[i] != NULL)
	{
	  or_free_classrep (cache_entry->repr[i]);
	  cache_entry->repr[i] = NULL;
	}
    }

  cache_entry->force_decache = false;
  OID_SET_NULL (&cache_entry->class_oid);
  if (cache_entry->max_reprid > DEFAULT_REPR_INCREMENT)
    {
      OR_CLASSREP **t;

      t = cache_entry->repr;
      cache_entry->repr = (OR_CLASSREP **)
	malloc (DEFAULT_REPR_INCREMENT * sizeof (OR_CLASSREP *));
      if (cache_entry->repr == NULL)
	{
	  ret = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret,
		  1, DEFAULT_REPR_INCREMENT * sizeof (OR_CLASSREP *));
	  cache_entry->repr = t;
	}
      else
	{
	  free_and_init (t);
	  cache_entry->max_reprid = DEFAULT_REPR_INCREMENT;
	  memset (cache_entry->repr, 0,
		  DEFAULT_REPR_INCREMENT * sizeof (OR_CLASSREP *));
	}

    }
  cache_entry->last_reprid = NULL_REPRID;

  return ret;
}

/*
 * heap_classrepr_entry_remove_from_LRU () -
 *   return: NO_ERROR
 *   cache_entry(in):
 */
static int
heap_classrepr_entry_remove_from_LRU (HEAP_CLASSREPR_ENTRY * cache_entry)
{
  if (cache_entry)
    {
      if (cache_entry == heap_Classrepr_cache.LRU_list.LRU_top)
	{
	  heap_Classrepr_cache.LRU_list.LRU_top = cache_entry->lru_next;
	}
      else
	{
	  cache_entry->lru_prev->lru_next = cache_entry->lru_next;
	}

      if (cache_entry == heap_Classrepr_cache.LRU_list.LRU_bottom)
	{
	  heap_Classrepr_cache.LRU_list.LRU_bottom = cache_entry->lru_prev;
	}
      else
	{
	  cache_entry->lru_next->lru_prev = cache_entry->lru_prev;
	}

      cache_entry->lru_next = NULL;
      cache_entry->lru_prev = NULL;
      cache_entry->zone = ZONE_VOID;
    }

  return NO_ERROR;
}

/*
 * heap_classrepr_decache_entry_remove () -
 *   return: NO_ERROR
 *   class_oid(in):
 *
 * Note: Decache the guessed last representations (i.e., that with -1)
 * from the given class.
 *
 * Note: This function should be called when a class is updated.
 *       1: During normal update
 */
int
heap_classrepr_decache_and_lock (THREAD_ENTRY * thread_p,
				 const OID * class_oid)
{
  HEAP_CLASSREPR_ENTRY *cache_entry, *prev_entry, *cur_entry;
  HEAP_CLASSREPR_HASH *hash_anchor;
  int ret = NO_ERROR;

  if (class_oid == NULL)
    {
      assert (class_oid != NULL);

      return ER_FAILED;
    }

  hash_anchor = &heap_Classrepr->hash_table[REPR_HASH (class_oid)];

  cache_entry = NULL;

  /*
   * This infinite loop will be exited
   * when worker using cache_entry does not exist.
   */
  while (true)
    {
      pthread_mutex_lock (&heap_Classrepr->classrepr_mutex);

      for (cache_entry = hash_anchor->hash_next; cache_entry != NULL;
	   cache_entry = cache_entry->hash_next)
	{
	  if (OID_EQ (class_oid, &cache_entry->class_oid))
	    {
	      break;
	    }
	}
      /* class_oid cache_entry is not found */
      if (cache_entry == NULL)
	{
	  goto class_locking;
	}
      if (cache_entry->fcnt == 0)
	{
	  /* worker using cache_entry does not exist. */
	  break;
	}

      pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

      thread_sleep (1);
    }

  /* delete classrepr from hash chain */
  prev_entry = NULL;
  cur_entry = hash_anchor->hash_next;
  while (cur_entry != NULL)
    {
      if (cur_entry == cache_entry)
	{
	  break;
	}
      prev_entry = cur_entry;
      cur_entry = cur_entry->hash_next;
    }

  /* class_oid cache_entry is not found */
  if (cur_entry == NULL)
    {
      /* This cannot happen */
      goto exit_on_error;
    }

  if (prev_entry == NULL)
    {
      hash_anchor->hash_next = cur_entry->hash_next;
    }
  else
    {
      prev_entry->hash_next = cur_entry->hash_next;
    }
  cur_entry->hash_next = NULL;

  cache_entry->force_decache = true;

  /* Remove from LRU list */
  if (cache_entry->zone == ZONE_LRU)
    {
      (void) heap_classrepr_entry_remove_from_LRU (cache_entry);

      assert (cache_entry->zone == ZONE_VOID);
    }
  assert (cache_entry->lru_prev == NULL && cache_entry->lru_next == NULL);

  assert (cache_entry->fcnt == 0);
  /* move cache_entry to free_list */
  ret = heap_classrepr_entry_reset (cache_entry);
  if (ret == NO_ERROR)
    {
      ret = heap_classrepr_entry_free (cache_entry);
    }

class_locking:
  if (heap_classrepr_lock_class (thread_p, hash_anchor,
				 class_oid) != LOCK_ACQUIRED)
    {
      assert (false);

      ret = ER_FAILED;
    }

  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

  return ret;

exit_on_error:

  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

  return (ret == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_classrepr_decache () - Deache any unfixed class representations of
 *                           given class
 *   return: NO_ERROR
 *   class_oid(in):
 *
 * Note: Decache all class representations of given class. If a class
 * is not given all class representations are decached.
 *
 * Note: This function should be called when a class is updated.
 *       1: At the end/beginning of rollback since we do not have any
 *          idea of a heap identifier of rolled back objects and we
 *          expend too much time, searching for the OID, every time we
 *          rolled back an updated object.
 */
int
heap_classrepr_decache (THREAD_ENTRY * thread_p, const OID * class_oid)
{
  int ret;

  ret = heap_classrepr_decache_and_lock (thread_p, class_oid);
  if (ret != NO_ERROR)
    {
      return ret;
    }
  heap_classrepr_unlock_class (class_oid);

  return ret;
}

/*
 * heap_classrepr_free () - Free a class representation
 *   return: NO_ERROR
 *   classrep(in): The class representation structure
 *   idx_incache(in): An index if the desired class representation is part of
 *                    the cache, otherwise -1 (no part of cache)
 *
 * Note: Free a class representation. If the class representation was
 * part of the class representation cache, the fix count is
 * decremented and the class representation will continue be
 * cached. The representation entry will be subject for
 * replacement when the fix count is zero (no one is using it).
 * If the class representatin was not part of the cache, it is
 * freed.
 */
int
heap_classrepr_free (OR_CLASSREP * classrep, int *idx_incache)
{
  HEAP_CLASSREPR_ENTRY *cache_entry;
#if !defined(NDEBUG)
  HEAP_CLASSREPR_HASH *hash_anchor;
#endif
  int ret = NO_ERROR;

  if (*idx_incache < 0)
    {
      or_free_classrep (classrep);
      return NO_ERROR;
    }

  cache_entry = &heap_Classrepr_cache.area[*idx_incache];

#if !defined(NDEBUG)
  hash_anchor =
    &heap_Classrepr->hash_table[REPR_HASH (&cache_entry->class_oid)];
#endif

  pthread_mutex_lock (&heap_Classrepr->classrepr_mutex);

#if !defined(NDEBUG)
  {
    HEAP_CLASSREPR_ENTRY *tmp_cache_entry;

    for (tmp_cache_entry = hash_anchor->hash_next; tmp_cache_entry != NULL;
	 tmp_cache_entry = tmp_cache_entry->hash_next)
      {
	if (tmp_cache_entry == cache_entry)
	  {
	    break;
	  }
      }
    assert (tmp_cache_entry != NULL);
  }
#endif

  cache_entry->fcnt--;
  if (cache_entry->fcnt == 0)
    {
      /*
       * Is this entry declared to be decached
       */
#ifdef DEBUG_CLASSREPR_CACHE
      rv = pthread_mutex_lock (&heap_Classrepr_cache.num_fix_entries_mutex);
      heap_Classrepr_cache.num_fix_entries--;
      pthread_mutex_unlock (&heap_Classrepr_cache.num_fix_entries_mutex);
#endif /* DEBUG_CLASSREPR_CACHE */
      if (cache_entry->force_decache == true)
	{
	  /* cache_entry is already removed from LRU list. */

	  /* move cache_entry to free_list */
	  ret = heap_classrepr_entry_reset (cache_entry);
	  if (ret == NO_ERROR)
	    {
	      ret = heap_classrepr_entry_free (cache_entry);
	    }
	}
      else
	{
	  /* relocate entry to the top of LRU list */
	  if (cache_entry != heap_Classrepr_cache.LRU_list.LRU_top)
	    {
	      if (cache_entry->zone == ZONE_LRU)
		{
		  /* remove from LRU list */
		  (void) heap_classrepr_entry_remove_from_LRU (cache_entry);
		}

	      /* insert into LRU top */
	      assert (cache_entry->lru_prev == NULL
		      && cache_entry->lru_next == NULL);
	      cache_entry->lru_next = heap_Classrepr_cache.LRU_list.LRU_top;
	      if (heap_Classrepr_cache.LRU_list.LRU_top == NULL)
		{
		  heap_Classrepr_cache.LRU_list.LRU_bottom = cache_entry;
		}
	      else
		{
		  heap_Classrepr_cache.LRU_list.LRU_top->lru_prev =
		    cache_entry;
		}
	      heap_Classrepr_cache.LRU_list.LRU_top = cache_entry;
	      cache_entry->zone = ZONE_LRU;
	    }
	}
    }

  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

  *idx_incache = -1;

  return ret;
}

/*
 * heap_classrepr_has_cache_entry () -
 *   return: true or false
 *
 *   class_oid(in):
 */
bool
heap_classrepr_has_cache_entry (UNUSED_ARG THREAD_ENTRY * thread_p,
				const OID * class_oid)
{
  HEAP_CLASSREPR_ENTRY *cache_entry = NULL;
  HEAP_CLASSREPR_HASH *hash_anchor = NULL;
  bool found = false;

  hash_anchor = &heap_Classrepr->hash_table[REPR_HASH (class_oid)];

  found = false;
  pthread_mutex_lock (&heap_Classrepr->classrepr_mutex);
  for (cache_entry = hash_anchor->hash_next; cache_entry != NULL;
       cache_entry = cache_entry->hash_next)
    {
      if (OID_EQ (&cache_entry->class_oid, class_oid))
	{
	  found = true;
	}
    }
  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

  return found;
}

#if !defined(NDEBUG)
/*
 * heap_classrepr_has_lock () -
 *   return: true or false
 *
 *   class_oid(in):
 */
static bool
heap_classrepr_has_lock (UNUSED_ARG THREAD_ENTRY * thread_p,
			 UNUSED_ARG const OID * class_oid)
{
#if !defined(SERVER_MODE)
  return true;
#else
  HEAP_CLASSREPR_LOCK *cur_lock_entry = NULL;
  HEAP_CLASSREPR_HASH *hash_anchor = NULL;
  bool found = false;

  hash_anchor = &heap_Classrepr->hash_table[REPR_HASH (class_oid)];

  found = false;
  pthread_mutex_lock (&heap_Classrepr->classrepr_mutex);
  for (cur_lock_entry = hash_anchor->lock_next; cur_lock_entry != NULL;
       cur_lock_entry = cur_lock_entry->lock_next)
    {
      if (OID_EQ (&cur_lock_entry->class_oid, class_oid))
	{
	  found = true;
	}
    }
  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

  return found;
#endif
}
#endif

/*
 * heap_classrepr_lock_class () - Prevent other threads accessing class_oid
 *                              class representation.
 *   return: ER_FAILED, NEED_TO_RETRY or LOCK_ACQUIRED
 *   hash_anchor(in):
 *   class_oid(in):
 */
static int
heap_classrepr_lock_class (UNUSED_ARG THREAD_ENTRY * thread_p,
			   UNUSED_ARG HEAP_CLASSREPR_HASH * hash_anchor,
			   UNUSED_ARG const OID * class_oid)
{
#if !defined(SERVER_MODE)
  return LOCK_ACQUIRED;		/* lock acquired. */
#else
  HEAP_CLASSREPR_LOCK *cur_lock_entry;
  THREAD_ENTRY *cur_thrd_entry;

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
      if (thread_p == NULL)
	{
	  return ER_FAILED;
	}
    }
  cur_thrd_entry = thread_p;

  for (cur_lock_entry = hash_anchor->lock_next; cur_lock_entry != NULL;
       cur_lock_entry = cur_lock_entry->lock_next)
    {
      if (OID_EQ (&cur_lock_entry->class_oid, class_oid))
	{
	  cur_thrd_entry->next_wait_thrd = cur_lock_entry->next_wait_thrd;
	  cur_lock_entry->next_wait_thrd = cur_thrd_entry;

	  thread_lock_entry (cur_thrd_entry);
	  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);
	  thread_suspend_wakeup_and_unlock_entry (cur_thrd_entry,
						  THREAD_HEAP_CLSREPR_SUSPENDED);

	  pthread_mutex_lock (&heap_Classrepr->classrepr_mutex);
	  if (cur_thrd_entry->resume_status == THREAD_HEAP_CLSREPR_RESUMED)
	    {
	      return NEED_TO_RETRY;	/* traverse hash chain again */
	    }
	  else
	    {
	      /* probably due to an interrupt */
	      assert ((cur_thrd_entry->resume_status ==
		       THREAD_RESUME_DUE_TO_INTERRUPT));
	      return ER_FAILED;
	    }
	}
    }

  if (heap_Classrepr_cache.lock_list == NULL)
    {
      cur_lock_entry =
	(HEAP_CLASSREPR_LOCK *) malloc (sizeof (HEAP_CLASSREPR_LOCK));
      if (cur_lock_entry == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (HEAP_CLASSREPR_LOCK));

	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
    }
  else
    {
      cur_lock_entry = heap_Classrepr_cache.lock_list;
      heap_Classrepr_cache.lock_list = cur_lock_entry->lock_next;
    }
  cur_lock_entry->class_oid = *class_oid;
  cur_lock_entry->next_wait_thrd = NULL;
  cur_lock_entry->lock_next = hash_anchor->lock_next;
  hash_anchor->lock_next = cur_lock_entry;

  return LOCK_ACQUIRED;		/* lock acquired. */
#endif
}


/*
 * heap_classrepr_unlock_class () -
 *   return: NO_ERROR
 *   hash_anchor(in):
 *   class_oid(in):
 *   need_hash_mutex(in):
 */
int
heap_classrepr_unlock_class (const OID * class_oid)
{
  HEAP_CLASSREPR_HASH *hash_anchor = NULL;
  int ret = NO_ERROR;

  hash_anchor = &heap_Classrepr->hash_table[REPR_HASH (class_oid)];
  ret = pthread_mutex_lock (&heap_Classrepr->classrepr_mutex);
  ret = heap_classrepr_unlock_class_with_hash_anchor (hash_anchor, class_oid);
  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

  return ret;
}

/*
 * heap_classrepr_unlock_class_with_hash_anchor () -
 *   return: NO_ERROR
 *   hash_anchor(in):
 *   class_oid(in):
 *   need_hash_mutex(in):
 */
static int
heap_classrepr_unlock_class_with_hash_anchor (UNUSED_ARG HEAP_CLASSREPR_HASH *
					      hash_anchor,
					      UNUSED_ARG const OID *
					      class_oid)
{
#if !defined(SERVER_MODE)
  return NO_ERROR;
#else
  HEAP_CLASSREPR_LOCK *prev_lock_entry, *cur_lock_entry;
  THREAD_ENTRY *cur_thrd_entry;

  prev_lock_entry = NULL;
  cur_lock_entry = hash_anchor->lock_next;
  while (cur_lock_entry != NULL)
    {
      if (OID_EQ (&cur_lock_entry->class_oid, class_oid))
	{
	  break;
	}
      prev_lock_entry = cur_lock_entry;
      cur_lock_entry = cur_lock_entry->lock_next;
    }

  /* if lock entry is found, remove it from lock list */
  if (cur_lock_entry == NULL)
    {				/* this cannot happen */
      assert (cur_lock_entry != NULL);

      return ER_FAILED;
    }

  if (prev_lock_entry == NULL)
    {
      hash_anchor->lock_next = cur_lock_entry->lock_next;
    }
  else
    {
      prev_lock_entry->lock_next = cur_lock_entry->lock_next;
    }
  cur_lock_entry->lock_next = NULL;

  for (cur_thrd_entry = cur_lock_entry->next_wait_thrd;
       cur_thrd_entry != NULL;
       cur_thrd_entry = cur_lock_entry->next_wait_thrd)
    {
      cur_lock_entry->next_wait_thrd = cur_thrd_entry->next_wait_thrd;
      cur_thrd_entry->next_wait_thrd = NULL;

      thread_wakeup (cur_thrd_entry, THREAD_HEAP_CLSREPR_RESUMED);
    }

  OID_SET_NULL (&cur_lock_entry->class_oid);
  cur_lock_entry->next_wait_thrd = NULL;
  cur_lock_entry->lock_next = heap_Classrepr_cache.lock_list;
  heap_Classrepr_cache.lock_list = cur_lock_entry;

  return NO_ERROR;
#endif
}

/*
 * heap_classrepr_entry_alloc () -
 *   return:
 */
static HEAP_CLASSREPR_ENTRY *
heap_classrepr_entry_alloc (void)
{
  HEAP_CLASSREPR_HASH *hash_anchor;
  HEAP_CLASSREPR_ENTRY *cache_entry, *prev_entry, *cur_entry;
  OID class_oid;

  cache_entry = NULL;

/* check_free_list: */

  /* 1. Get entry from free list */
  if (heap_Classrepr_cache.free_list.free_top == NULL)
    {
      goto check_LRU_list;
    }

  if (heap_Classrepr_cache.free_list.free_top == NULL)
    {
      cache_entry = NULL;
    }
  else
    {
      cache_entry = heap_Classrepr_cache.free_list.free_top;
      heap_Classrepr_cache.free_list.free_top = cache_entry->free_next;
      heap_Classrepr_cache.free_list.free_cnt--;

      cache_entry->free_next = NULL;
      cache_entry->zone = ZONE_VOID;

      return cache_entry;
    }

check_LRU_list:
  /* 2. Get entry from LRU list */
  OID_SET_NULL (&class_oid);
  for (cache_entry = heap_Classrepr_cache.LRU_list.LRU_bottom;
       cache_entry != NULL; cache_entry = cache_entry->lru_prev)
    {
      if (cache_entry->fcnt == 0)
	{
	  /* remove from LRU list */
	  (void) heap_classrepr_entry_remove_from_LRU (cache_entry);
	  assert (cache_entry->zone == ZONE_VOID);
	  assert (cache_entry->lru_next == NULL
		  && cache_entry->lru_prev == NULL);

	  COPY_OID (&class_oid, &cache_entry->class_oid);
	  break;
	}
    }

  if (cache_entry == NULL)
    {
      goto expand_list;
    }

  /* delete classrepr from hash chain */
  hash_anchor = &heap_Classrepr->hash_table[REPR_HASH (&class_oid)];
  prev_entry = NULL;
  cur_entry = hash_anchor->hash_next;
  while (cur_entry != NULL)
    {
      if (cur_entry == cache_entry)
	{
	  break;
	}
      prev_entry = cur_entry;
      cur_entry = cur_entry->hash_next;
    }

  if (cur_entry == NULL || cache_entry->fcnt != 0)
    {
      goto check_LRU_list;
    }

  if (prev_entry == NULL)
    {
      hash_anchor->hash_next = cur_entry->hash_next;
    }
  else
    {
      prev_entry->hash_next = cur_entry->hash_next;
    }
  cur_entry->hash_next = NULL;

  (void) heap_classrepr_entry_reset (cache_entry);

end:

  return cache_entry;

expand_list:

  /* not supported */
  cache_entry = NULL;
  goto end;
}

/*
 * heap_classrepr_entry_free () -
 *   return: NO_ERROR
 *   cache_entry(in):
 */
static int
heap_classrepr_entry_free (HEAP_CLASSREPR_ENTRY * cache_entry)
{
  cache_entry->free_next = heap_Classrepr_cache.free_list.free_top;
  heap_Classrepr_cache.free_list.free_top = cache_entry;
  cache_entry->zone = ZONE_FREE;
  heap_Classrepr_cache.free_list.free_cnt++;

  return NO_ERROR;
}

/*
 * heap_classrepr_get () - Obtain the desired class representation
 *   return: classrepr
 *   class_oid(in): The class identifier
 *   class_recdes(in): The class recdes (when know) or NULL
 *   reprid(in): Representation of the class or NULL_REPRID for last one
 *   idx_incache(in): An index if the desired class representation is part
 *                    of the cache
 *
 * Note: Obtain the desired class representation for the given class.
 */
OR_CLASSREP *
heap_classrepr_get (THREAD_ENTRY * thread_p, OID * class_oid,
		    RECDES * class_recdes, REPR_ID reprid, int *idx_incache,
		    bool use_last_reprid)
{
  HEAP_CLASSREPR_ENTRY *cache_entry;
  HEAP_CLASSREPR_HASH *hash_anchor;
  OR_CLASSREP *repr = NULL;
  REPR_ID last_reprid;
  int r;

  hash_anchor = &heap_Classrepr->hash_table[REPR_HASH (class_oid)];

  /* search entry with class_oid from hash chain */
  r = pthread_mutex_lock (&heap_Classrepr->classrepr_mutex);

search_begin:
  for (cache_entry = hash_anchor->hash_next; cache_entry != NULL;
       cache_entry = cache_entry->hash_next)
    {
      if (OID_EQ (class_oid, &cache_entry->class_oid))
	{
	  break;
	}
    }

  if (cache_entry == NULL)
    {
      /* class_oid was not found. Lock class_oid. */
      r = heap_classrepr_lock_class (thread_p, hash_anchor, class_oid);
      if (r != LOCK_ACQUIRED)
	{
	  if (r == NEED_TO_RETRY)
	    {
	      goto search_begin;
	    }
	  else
	    {
	      assert (r == ER_FAILED);

	      pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

	      return NULL;
	    }
	}

      /* Get free entry */
      cache_entry = heap_classrepr_entry_alloc ();

      if (cache_entry == NULL)
	{
	  /* if all cache entry is busy, allocate memory for repr. */
	  if (class_recdes == NULL)
	    {
	      RECDES peek_recdes = RECDES_INITIALIZER;
	      HEAP_SCANCACHE scan_cache;

	      heap_scancache_quick_start (&scan_cache);
	      if (heap_get (thread_p, class_oid, &peek_recdes, &scan_cache,
			    PEEK) == S_SUCCESS)
		{
		  if (use_last_reprid == true)
		    {
		      repr = or_get_classrep (&peek_recdes, NULL_REPRID);
		    }
		  else
		    {
		      repr = or_get_classrep (&peek_recdes, reprid);
		    }
		}
	      heap_scancache_end (thread_p, &scan_cache);
	    }
	  else
	    {
	      if (use_last_reprid == true)
		{
		  repr = or_get_classrep (class_recdes, NULL_REPRID);
		}
	      else
		{
		  repr = or_get_classrep (class_recdes, reprid);
		}
	    }
	  *idx_incache = -1;

	  /* free lock for class_oid */
	  (void) heap_classrepr_unlock_class_with_hash_anchor (hash_anchor,
							       class_oid);

	  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

	  return repr;
	}

      /* New cache entry is acquired. Load class_oid classrepr info. on it */
      if (class_recdes == NULL)
	{
	  RECDES peek_recdes = RECDES_INITIALIZER;
	  HEAP_SCANCACHE scan_cache;

	  heap_scancache_quick_start (&scan_cache);
	  if (heap_get (thread_p, class_oid, &peek_recdes, &scan_cache, PEEK)
	      == S_SUCCESS)
	    {
	      last_reprid = or_class_repid (&peek_recdes);
	      assert (last_reprid > NULL_REPRID);

	      if (use_last_reprid == true || reprid == NULL_REPRID)
		{
		  reprid = last_reprid;
		}

	      if (reprid <= NULL_REPRID || reprid > last_reprid)
		{
		  assert (false);

		  (void) heap_classrepr_entry_reset (cache_entry);
		  (void) heap_classrepr_entry_free (cache_entry);
		  (void)
		    heap_classrepr_unlock_class_with_hash_anchor (hash_anchor,
								  class_oid);
		  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_CT_UNKNOWN_REPRID, 1, reprid);
		  return NULL;
		}

	      repr = or_get_classrep (&peek_recdes, reprid);

	      /* check if cache_entry->repr[last_reprid] is valid. */
	      if (last_reprid >= cache_entry->max_reprid)
		{
		  free_and_init (cache_entry->repr);
		  cache_entry->max_reprid = last_reprid + 1;
		  cache_entry->repr = (OR_CLASSREP **)
		    malloc (cache_entry->max_reprid * sizeof (OR_CLASSREP *));
		  if (cache_entry->repr == NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1,
			      cache_entry->max_reprid *
			      sizeof (OR_CLASSREP *));

		      (void) heap_classrepr_entry_reset (cache_entry);
		      (void) heap_classrepr_entry_free (cache_entry);
		      (void) heap_classrepr_unlock_class_with_hash_anchor
			(hash_anchor, class_oid);
		      pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

		      if (repr != NULL)
			{
			  or_free_classrep (repr);
			}
		      return NULL;
		    }
		  memset (cache_entry->repr, 0,
			  cache_entry->max_reprid * sizeof (OR_CLASSREP *));
		}

	      cache_entry->repr[reprid] = repr;

	      if (reprid != last_reprid)
		{		/* if last repr is not cached */
		  cache_entry->repr[last_reprid] =
		    or_get_classrep (&peek_recdes, last_reprid);
		}
	      cache_entry->last_reprid = last_reprid;
	    }
	  else
	    {
	      repr = NULL;
	    }
	  heap_scancache_end (thread_p, &scan_cache);
	}
      else
	{
	  last_reprid = or_class_repid (class_recdes);
	  assert (last_reprid > NULL_REPRID);

	  if (use_last_reprid == true || reprid == NULL_REPRID)
	    {
	      reprid = last_reprid;
	    }

	  if (reprid <= NULL_REPRID || reprid > last_reprid)
	    {
	      assert (false);

	      (void) heap_classrepr_entry_reset (cache_entry);
	      (void) heap_classrepr_entry_free (cache_entry);
	      (void) heap_classrepr_unlock_class_with_hash_anchor
		(hash_anchor, class_oid);
	      pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_CT_UNKNOWN_REPRID, 1, reprid);
	      return NULL;
	    }

	  repr = or_get_classrep (class_recdes, reprid);

	  if (last_reprid >= cache_entry->max_reprid)
	    {
	      free_and_init (cache_entry->repr);
	      cache_entry->max_reprid = last_reprid + 1;
	      cache_entry->repr = (OR_CLASSREP **)
		malloc (cache_entry->max_reprid * sizeof (OR_CLASSREP *));
	      if (cache_entry->repr == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1,
			  cache_entry->max_reprid * sizeof (OR_CLASSREP *));

		  (void) heap_classrepr_entry_reset (cache_entry);
		  (void) heap_classrepr_entry_free (cache_entry);
		  (void) heap_classrepr_unlock_class_with_hash_anchor
		    (hash_anchor, class_oid);
		  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

		  if (repr != NULL)
		    {
		      or_free_classrep (repr);
		    }

		  return NULL;
		}
	      memset (cache_entry->repr, 0,
		      cache_entry->max_reprid * sizeof (OR_CLASSREP *));

	    }

	  cache_entry->repr[reprid] = repr;

	  if (reprid != last_reprid)
	    {
	      cache_entry->repr[last_reprid] =
		or_get_classrep (class_recdes, last_reprid);
	    }
	  cache_entry->last_reprid = last_reprid;
	}

      if (repr == NULL)
	{
	  /* free cache_entry and return NULL */
	  (void) heap_classrepr_entry_reset (cache_entry);
	  (void) heap_classrepr_entry_free (cache_entry);
	  (void) heap_classrepr_unlock_class_with_hash_anchor
	    (hash_anchor, class_oid);
	  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

	  return NULL;
	}

      cache_entry->fcnt = 1;
      cache_entry->class_oid = *class_oid;
#ifdef DEBUG_CLASSREPR_CACHE
      r = pthread_mutex_lock (&heap_Classrepr_cache.num_fix_entries_mutex);
      heap_Classrepr_cache.num_fix_entries++;
      pthread_mutex_unlock (&heap_Classrepr_cache.num_fix_entries_mutex);

#endif /* DEBUG_CLASSREPR_CACHE */
      *idx_incache = cache_entry->idx;

      /* Add to hash chain, and remove lock for class_oid */
      cache_entry->hash_next = hash_anchor->hash_next;
      hash_anchor->hash_next = cache_entry;

#if !defined(NDEBUG)
      {
	HEAP_CLASSREPR_ENTRY *tmp_cache_entry;
	int num_found;

	num_found = 0;
	for (tmp_cache_entry = hash_anchor->hash_next;
	     tmp_cache_entry != NULL;
	     tmp_cache_entry = tmp_cache_entry->hash_next)
	  {
	    if (OID_EQ (&tmp_cache_entry->class_oid, &cache_entry->class_oid))
	      {
		num_found++;
	      }
	  }

	assert (num_found == 1);
      }
#endif

      (void) heap_classrepr_unlock_class_with_hash_anchor (hash_anchor,
							   class_oid);
    }
  else
    {
      /* now, we have already cache_entry for class_oid.
       * if it contains repr info for reprid, return it.
       * else load classrepr info for it */

      if (use_last_reprid == true || reprid == NULL_REPRID)
	{
	  reprid = cache_entry->last_reprid;
	}

      if (reprid <= NULL_REPRID || reprid > cache_entry->last_reprid
	  || reprid > cache_entry->max_reprid)
	{
	  assert (false);

	  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CT_UNKNOWN_REPRID, 1,
		  reprid);
	  return NULL;
	}

      assert (reprid > NULL_REPRID);

      /* reprid cannot be greater than cache_entry->last_reprid. */
      repr = cache_entry->repr[reprid];
      if (repr == NULL)
	{
	  /* load repr. info. for reprid of class_oid */
	  if (class_recdes == NULL)
	    {
	      RECDES peek_recdes = RECDES_INITIALIZER;
	      HEAP_SCANCACHE scan_cache;

	      heap_scancache_quick_start (&scan_cache);
	      if (heap_get (thread_p, class_oid, &peek_recdes, &scan_cache,
			    PEEK) == S_SUCCESS)
		{
		  repr = or_get_classrep (&peek_recdes, reprid);
		}
	      heap_scancache_end (thread_p, &scan_cache);
	    }
	  else
	    {
	      repr = or_get_classrep (class_recdes, reprid);
	    }
	  cache_entry->repr[reprid] = repr;
	}

      if (repr != NULL)
	{
	  cache_entry->fcnt++;
	  *idx_incache = cache_entry->idx;
	}

    }

  pthread_mutex_unlock (&heap_Classrepr->classrepr_mutex);

  return repr;
}

#ifdef DEBUG_CLASSREPR_CACHE
/*
 * heap_classrepr_dump_cache () - Dump the class representation cache
 *   return: NO_ERROR
 *   simple_dump(in):
 *
 * Note: Dump the class representation cache.
 */
static int
heap_classrepr_dump_cache (bool simple_dump)
{
  OR_CLASSREP *classrepr;
  HEAP_CLASSREPR_ENTRY *cache_entry;
  int i, j;
  int rv;
  int ret = NO_ERROR;

  if (heap_Classrepr == NULL)
    {
      return NO_ERROR;		/* nop */
    }

  (void) fflush (stderr);
  (void) fflush (stdout);

  fprintf (stdout, "*** Class Representation cache dump *** \n");
  fprintf (stdout, " Number of entries = %d, Number of used entries = %d\n",
	   heap_Classrepr->num_entries,
	   heap_Classrepr->num_entries - heap_Classrepr->free_list.free_cnt);

  for (cache_entry = heap_Classrepr->area, i = 0;
       i < heap_Classrepr->num_entries; cache_entry++, i++)
    {
      fprintf (stdout, " \nEntry_id %d\n", cache_entry->idx);

      rv = pthread_mutex_lock (&cache_entry->mutex);
      for (j = 0; j <= cache_entry->last_reprid; j++)
	{
	  classrepr = cache_entry->repr[j];
	  if (classrepr == NULL)
	    {
	      fprintf (stdout, ".....\n");
	      continue;
	    }
	  fprintf (stdout, " Fix count = %d, force_decache = %d\n",
		   cache_entry->fcnt, cache_entry->force_decache);

	  if (simple_dump == true)
	    {
	      fprintf (stdout, " Class_oid = %d|%d|%d, Reprid = %d\n",
		       (int) cache_entry->class_oid.volid,
		       cache_entry->class_oid.pageid,
		       (int) cache_entry->class_oid.slotid,
		       cache_entry->repr[j]->id);
	      fprintf (stdout, " Representation address = %p\n", classrepr);

	    }
	  else
	    {
	      ret = heap_classrepr_dump (&cache_entry->class_oid, classrepr);
	    }
	}

      pthread_mutex_unlock (&cache_entry->mutex);
    }

  return ret;
}
#endif /* DEBUG_CLASSREPR_CACHE */

/*
 * heap_classrepr_dump () - Dump schema of a given class representation
 *   return: NO_ERROR
 *   class_oid(in):
 *   repr(in): The class representation
 *
 * Note: Dump the class representation cache.
 */
static int
heap_classrepr_dump (THREAD_ENTRY * thread_p, FILE * fp,
		     const OID * class_oid, const OR_CLASSREP * repr)
{
  OR_ATTRIBUTE *volatile attrepr;
  volatile int i;
  int k, j;
  char *classname;
  const char *attr_name;
  DB_VALUE def_dbvalue;
  PR_TYPE *pr_type;
  int disk_length;
  OR_BUF buf;
  bool copy;
  RECDES recdes = RECDES_INITIALIZER;	/* Used to obtain attrnames */
  int ret = NO_ERROR;
  char *index_name = NULL;

  /*
   * The class is feteched to print the attribute names.
   *
   * This is needed since the name of the attributes is not contained
   * in the class representation structure.
   */

  recdes.data = NULL;
  recdes.area_size = 0;

  if (repr == NULL)
    {
      goto exit_on_error;
    }

  if (heap_get_alloc (thread_p, class_oid, &recdes) != NO_ERROR)
    {
      goto exit_on_error;
    }

  classname = heap_get_class_name (thread_p, class_oid);
  if (classname == NULL)
    {
      goto exit_on_error;
    }

  fprintf (fp, " Class-OID = %d|%d|%d, Classname = %s, reprid = %d,\n"
	   " Attrs: Tot = %d, Nfix = %d, Nvar = %d,\n"
	   " Total_length_of_fixattrs = %d,\n",
	   (int) class_oid->volid, class_oid->pageid,
	   (int) class_oid->slotid, classname, repr->id,
	   repr->n_attributes, (repr->n_attributes - repr->n_variable),
	   repr->n_variable, repr->fixed_length);
  free_and_init (classname);

  fprintf (fp, " Attribute Specifications:\n");
  for (i = 0, attrepr = repr->attributes;
       i < repr->n_attributes; i++, attrepr++)
    {

      attr_name = or_get_attrname (&recdes, attrepr->id);
      if (attr_name == NULL)
	{
	  attr_name = "?????";
	}

      fprintf (fp,
	       "\n Attrid = %d, Attrname = %s, type = %s,\n"
	       " location = %d, position = %d,\n",
	       attrepr->id, attr_name, pr_type_name (attrepr->type),
	       attrepr->location, attrepr->position);

      if (!OID_ISNULL (&attrepr->classoid)
	  && !OID_EQ (&attrepr->classoid, class_oid))
	{
	  classname = heap_get_class_name (thread_p, &attrepr->classoid);
	  if (classname == NULL)
	    {
	      goto exit_on_error;
	    }
	  fprintf (fp,
		   " Inherited from Class: oid = %d|%d|%d, Name = %s\n",
		   (int) attrepr->classoid.volid,
		   attrepr->classoid.pageid,
		   (int) attrepr->classoid.slotid, classname);
	  free_and_init (classname);
	}

      if (attrepr->n_btids > 0)
	{
	  fprintf (fp, " Number of Btids = %d,\n", attrepr->n_btids);
	  for (k = 0; k < attrepr->n_btids; k++)
	    {
	      index_name = NULL;
	      /* find index_name */
	      for (j = 0; j < repr->n_indexes; ++j)
		{
		  if (BTID_IS_EQUAL (&(repr->indexes[j].btid),
				     &(attrepr->btids[k])))
		    {
		      index_name = repr->indexes[j].btname;
		      break;
		    }
		}

	      fprintf (fp, " BTID: VFID %d|%d, Root_PGID %d, %s\n",
		       (int) attrepr->btids[k].vfid.volid,
		       attrepr->btids[k].vfid.fileid,
		       attrepr->btids[k].root_pageid,
		       (index_name == NULL) ? "unknown" : index_name);
	    }
	}

      /*
       * Dump the default value if any.
       */
      fprintf (fp, " Default disk value format:\n");
      fprintf (fp, "   length = %d, value = ",
	       attrepr->default_value.val_length);

      if (attrepr->default_value.val_length <= 0)
	{
	  fprintf (fp, "NULL");
	}
      else
	{
	  or_init (&buf, (char *) attrepr->default_value.value,
		   attrepr->default_value.val_length);
	  buf.error_abort = 1;

	  switch (_setjmp (buf.env))
	    {
	    case 0:
	      /* Do not copy the string--just use the pointer.  The pr_ routines
	       * for strings and sets have different semantics for length.
	       * A negative length value for strings means "don't copy the
	       * string, just use the pointer".
	       */

	      disk_length = attrepr->default_value.val_length;
	      copy = (pr_is_set_type (attrepr->type)) ? true : false;
	      pr_type = PR_TYPE_FROM_ID (attrepr->type);
	      if (pr_type)
		{
		  (*(pr_type->data_readval)) (&buf, &def_dbvalue,
					      attrepr->domain, disk_length,
					      copy);

		  db_value_fprint (stdout, &def_dbvalue);
		  (void) pr_clear_value (&def_dbvalue);
		}
	      else
		{
		  fprintf (fp, "PR_TYPE is NULL");
		}
	      break;
	    default:
	      /*
	       * An error was found during the reading of the attribute value
	       */
	      fprintf (fp, "Error transforming the default value\n");
	      break;
	    }
	}
      fprintf (fp, "\n");
    }

  free_and_init (recdes.data);

  return ret;

exit_on_error:

  if (recdes.data)
    {
      free_and_init (recdes.data);
    }

  fprintf (fp, "Dump has been aborted...");

  return (ret == NO_ERROR) ? ER_FAILED : ret;
}

#ifdef DEBUG_CLASSREPR_CACHE
/*
 * heap_classrepr_dump_anyfixed() - Dump class representation cache if
 *                                   any entry is fixed
 *   return: NO_ERROR
 *
 * Note: The class representation cache is dumped if any cache entry is fixed
 *
 * This is a debugging function that can be used to verify if
 * entries were freed after a set of operations (e.g., a
 * transaction or a API function).
 *
 * Note:
 * This function will not give you good results when there are
 * multiple users in the system (multiprocessing). However, it
 * can be used during shuttdown.
 */
int
heap_classrepr_dump_anyfixed (void)
{
  int ret = NO_ERROR;

  if (heap_Classrepr->num_fix_entries > 0)
    {
      er_log_debug (ARG_FILE_LINE, "heap_classrepr_dump_anyfixed:"
		    " Some entries are fixed\n");
      ret = heap_classrepr_dump_cache (true);
    }

  return ret;
}
#endif /* DEBUG_CLASSREPR_CACHE */

/*
 * heap_bestspace_update () - Update best space
 *   return: NO_ERROR
 *   pgptr(in): Page pointer
 *   hfid(in): Object heap file identifier
 *   prev_freespace(in):
 *
 */
static void
heap_bestspace_update (THREAD_ENTRY * thread_p, PAGE_PTR pgptr,
		       const HFID * hfid, int prev_freespace)
{
  VPID *vpid;
  int freespace;

  freespace = spage_get_free_space_without_saving (thread_p, pgptr);
  if (prev_freespace < freespace || freespace > HEAP_DROP_FREE_SPACE)
    {
      vpid = pgbuf_get_vpid_ptr (pgptr);
      assert_release (vpid != NULL);

      (void) heap_bestspace_add (thread_p, hfid, vpid, freespace);
    }
}

/*
 * heap_find_page_in_bestspace_cache () - Find a page within best space
 * 					  statistics with the needed space
 *   return: HEAP_FINDPSACE (found, not found, or error)
 *   hfid(in): Object heap file identifier
 *   needed_space(in): The needed space.
 *   scan_cache(in): Scan cache if any
 *   pgptr(out): Best page with enough space or NULL
 *
 * Note: Search for a page within the best space cache which has the
 * needed space. The free space fields of best space cache along
 * with some other index information are updated (as a side
 * effect) as the best space cache is accessed.
 */
static HEAP_FINDSPACE
heap_find_page_in_bestspace_cache (THREAD_ENTRY * thread_p,
				   const HFID * hfid,
				   UNUSED_ARG int record_length,
				   int needed_space,
				   HEAP_SCANCACHE * scan_cache,
				   PAGE_PTR * pgptr)
{
#define BEST_PAGE_SEARCH_MAX_COUNT 100

  HEAP_FINDSPACE found;
  int old_wait_msecs;
  int search_cnt;
//  HEAP_BESTSPACE_ENTRY *ent;
  HEAP_BESTSPACE best;
  RECDES peek_rec = RECDES_INITIALIZER;
//  int best_array_index = -1;
  HEAP_CHAIN *heap_header;

  assert (scan_cache == NULL || HFID_EQ (hfid, &(scan_cache->hfid)));

  *pgptr = NULL;

#if !defined(NDEBUG)
  if (scan_cache != NULL && !OID_ISNULL (&scan_cache->class_oid))
    {
      LC_FIND_CLASSNAME status;
      OID class_oid = NULL_OID_INITIALIZER;
      bool found_match = false;

      status =
	xlocator_find_class_oid (thread_p, CT_LOG_APPLIER_NAME, &class_oid,
				 NULL_LOCK);

#if defined(SERVER_MODE)
      assert (status == LC_CLASSNAME_EXIST);
#else
      assert (db_get_client_type () == BOOT_CLIENT_CREATEDB
	      || status == LC_CLASSNAME_EXIST);
#endif

      if (OID_EQ (&class_oid, &scan_cache->class_oid))
	{
	  found_match = true;
	}

#if 0				/* another check method */
      {
	char *class_name = NULL;

	class_name = heap_get_class_name (thread_p, &scan_cache->class_oid);
	if (class_name != NULL)
	  {
	    if (strcmp (class_name, CT_LOG_APPLIER_NAME) == 0
		|| strcmp (class_name, CT_LOG_ANALYZER_NAME) == 0)
	      {
		found_match = true;
	      }

	    /* cleanup */
	    free_and_init (class_name);
	  }
      }
#endif

      assert (*pgptr == NULL);

      if (found_match)
	{
	  /* not permit insert without sql hint force_page_allocation */
#if defined(SERVER_MODE)
	  assert (false);
#else
	  assert (db_get_client_type () == BOOT_CLIENT_CREATEDB);
#endif
	}

    }
#endif

  /*
   * If a page is busy, don't wait continue looking for other pages in our
   * statistics. This will improve some contentions on the heap at the
   * expenses of storage.
   */

  /* LK_FORCE_ZERO_WAIT doesn't set error when deadlock occurrs */
  old_wait_msecs = xlogtb_reset_wait_msecs (thread_p, LK_FORCE_ZERO_WAIT);

  found = HEAP_FINDSPACE_NOTFOUND;
  search_cnt = 0;
//  best_array_index = 0;

  while (found == HEAP_FINDSPACE_NOTFOUND)
    {
      best.freespace = -1;	/* init */

//      ent = NULL;
      while (search_cnt < BEST_PAGE_SEARCH_MAX_COUNT
	     && heap_bestspace_remove (thread_p, &best, hfid) == NO_ERROR)
	{
	  search_cnt++;

	  if (best.freespace == -1 || best.freespace >= needed_space)
	    {
	      break;
	    }
	}

      if (best.freespace == -1)
	{
	  break;		/* not found, exit loop */
	}

      *pgptr = heap_scan_pb_lock_and_fetch (thread_p, &best.vpid,
					    X_LOCK, scan_cache, PAGE_HEAP);
      if (*pgptr == NULL)
	{
	  /* Add the free space of the page */
	  (void) heap_bestspace_add (thread_p, hfid, &best.vpid,
				     best.freespace);

	  /*
	   * Either we timeout and we want to continue in this case, or
	   * we have another kind of problem.
	   */
	  switch (er_errid ())
	    {
	    case NO_ERROR:
	      /* In case of latch-timeout in pgbuf_fix,
	       * the timeout error(ER_LK_PAGE_TIMEOUT) is not set,
	       * because lock wait time is LK_FORCE_ZERO_WAIT.
	       * So we will just continue to find another page.
	       */
	      break;

	    case ER_INTERRUPTED:
	      found = HEAP_FINDSPACE_ERROR;
	      break;

	    default:
	      /*
	       * Something went wrong, we are unable to fetch this page.
	       */
	      found = HEAP_FINDSPACE_ERROR;
	      break;
	    }
	}
      else
	{
	  assert (file_find_page (thread_p, &(hfid->vfid), &best.vpid) ==
		  true);

	  best.freespace = spage_max_space_for_new_record (thread_p, *pgptr);
	  if (best.freespace >= needed_space)
	    {
	      assert (scan_cache == NULL
		      || !OID_ISNULL (&scan_cache->class_oid));

	      if (scan_cache == NULL)
		{
		  found = HEAP_FINDSPACE_FOUND;
		}
	      else
		{
		  /* defense code */
		  if (spage_get_record (*pgptr, HEAP_HEADER_AND_CHAIN_SLOTID,
					&peek_rec, PEEK) == S_SUCCESS)
		    {
		      heap_header = (HEAP_CHAIN *) peek_rec.data;
		      if (OID_EQ (&heap_header->class_oid,
				  &scan_cache->class_oid))
			{
			  found = HEAP_FINDSPACE_FOUND;
			}
		      else
			{
			  /* TODO - trace */
			  assert (OID_ISNULL (&heap_header->class_oid));
			}
		    }
		}
	    }
	  else
	    {
	      /* Add the free space of the page */
	      (void) heap_bestspace_add (thread_p, hfid, &best.vpid,
					 best.freespace);
	    }

	  if (found != HEAP_FINDSPACE_FOUND)
	    {
	      pgbuf_unfix_and_init (thread_p, *pgptr);
	    }
	}
    }

  /*
   * Reset back the timeout value of the transaction
   */
  (void) xlogtb_reset_wait_msecs (thread_p, old_wait_msecs);

  return found;
}

/*
 * heap_find_best_page () - Find a page with the needed space.
 *   return: pointer to page with enough space or NULL
 *   hfid(in): Object heap file identifier
 *   needed_space(in): The minimal space needed
 *   isnew_rec(in): Are we inserting a new record to the heap ?
 *   newrec_size(in): Size of the new record
 *   scan_cache(in/out): Scan cache used to estimate the best space pages
 *
 * Note: Find a page among the set of best pages of the heap which has
 * the needed space. If we do not find any page, a new page is
 * allocated. The heap header and the scan cache may be updated
 * as a side effect to reflect more accurate space on some of the
 * set of best pages.
 */
static PAGE_PTR
heap_find_best_page (THREAD_ENTRY * thread_p, const HFID * hfid,
		     int needed_space, UNUSED_ARG bool isnew_rec,
		     UNUSED_ARG int newrec_size, HEAP_SCANCACHE * scan_cache)
{
  PAGE_PTR pgptr = NULL;	/* The page with the best space */
  int total_space;

  /*
   * Try to use the space cache for as much information as possible to avoid
   * fetching and updating the header page a lot.
   */

  assert ((scan_cache == NULL)
	  || (scan_cache && scan_cache->cache_last_fix_page == false));
  assert (scan_cache == NULL || HFID_EQ (hfid, &(scan_cache->hfid)));

  assert (!heap_is_big_length (needed_space));
  /* Take into consideration the unfill factor for pages with objects */
  total_space = needed_space + heap_Slotted_overhead + HEAP_UNFILL_SPACE;
  if (heap_is_big_length (total_space))
    {
      total_space = needed_space + heap_Slotted_overhead;
    }

  pgptr = NULL;
  if ((scan_cache == NULL || scan_cache->vpid_alloc == false)
      && heap_find_page_in_bestspace_cache (thread_p, hfid,
					    needed_space, total_space,
					    scan_cache,
					    &pgptr) == HEAP_FINDSPACE_ERROR)
    {
      return NULL;
    }

  if (pgptr == NULL)
    {
      /*
       * None of the best pages has the needed space, allocate a new page.
       * Set the head to the index with the smallest free space, which may not
       * be accurate.
       */

      pgptr = heap_vpid_alloc (thread_p, hfid, total_space, scan_cache);
      assert (pgptr != NULL
	      || er_errid () == ER_INTERRUPTED
	      || er_errid () == ER_FILE_NOT_ENOUGH_PAGES_IN_DATABASE);
    }

  if (scan_cache != NULL)
    {
      /*
       * Update the space cache information to avoid reading the header page
       * at a later point.
       */
      scan_cache->pgptr = NULL;
    }

  return pgptr;
}

/*
 * heap_read_full_search_vpid () -
 *   return: NO_ERROR or error code
 *
 *   full_search_vpid (out):
 *   hfid(in): Heap file identifier
 */
static int
heap_read_full_search_vpid (THREAD_ENTRY * thread_p, VPID * full_search_vpid,
			    const HFID * hfid)
{
  PAGE_PTR hdr_pgptr = NULL;
  VPID header_vpid;
  int error_code = NO_ERROR;
  RECDES hdr_recdes = RECDES_INITIALIZER;	/* Record descriptor to point to space
						 * statistics */
  HEAP_HDR_STATS *heap_hdr;	/* Heap header                         */


  header_vpid.volid = hfid->vfid.volid;
  header_vpid.pageid = hfid->hpgid;

  hdr_pgptr = heap_pgbuf_fix (thread_p, hfid, &header_vpid,
			      PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
			      PAGE_HEAP_HEADER);
  if (hdr_pgptr == NULL)
    {
      /* something went wrong. Unable to fetch header page */
      error_code = er_errid ();

      goto exit_on_error;
    }

  if (spage_get_record (hdr_pgptr, HEAP_HEADER_AND_CHAIN_SLOTID,
			&hdr_recdes, PEEK) != S_SUCCESS)
    {
      error_code = er_errid ();

      goto exit_on_error;
    }

  heap_hdr = (HEAP_HDR_STATS *) hdr_recdes.data;

  *full_search_vpid = heap_hdr->full_search_vpid;

  pgbuf_unfix_and_init (thread_p, hdr_pgptr);

  return NO_ERROR;

exit_on_error:

  if (error_code != NO_ERROR)
    {
      assert (error_code != NO_ERROR);
      error_code = ER_FAILED;
    }

  if (hdr_pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, hdr_pgptr);
    }

  return error_code;
}

/*
 * heap_write_full_search_vpid () -
 *   return: NO_ERROR or error code
 *
 *   hfid(in): Heap file identifier
 *   full_search_vpid (in):
 */
static int
heap_write_full_search_vpid (THREAD_ENTRY * thread_p, const HFID * hfid,
			     VPID * full_search_vpid)
{
  PAGE_PTR hdr_pgptr = NULL;
  LOG_DATA_ADDR addr_hdr = LOG_ADDR_INITIALIZER;	/* Address of logging data */
  VPID header_vpid;
  int error_code = NO_ERROR;
  RECDES hdr_recdes = RECDES_INITIALIZER;	/* Record descriptor to point to space
						 * statistics */
  HEAP_HDR_STATS *heap_hdr;	/* Heap header                         */


  header_vpid.volid = hfid->vfid.volid;
  header_vpid.pageid = hfid->hpgid;

  hdr_pgptr = heap_pgbuf_fix (thread_p, hfid, &header_vpid,
			      PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH,
			      PAGE_HEAP_HEADER);
  if (hdr_pgptr == NULL)
    {
      /* something went wrong. Unable to fetch header page */
      error_code = er_errid ();

      goto exit_on_error;
    }

  if (spage_get_record (hdr_pgptr, HEAP_HEADER_AND_CHAIN_SLOTID,
			&hdr_recdes, PEEK) != S_SUCCESS)
    {
      error_code = er_errid ();

      goto exit_on_error;
    }

  heap_hdr = (HEAP_HDR_STATS *) hdr_recdes.data;

  heap_hdr->full_search_vpid = *full_search_vpid;

  addr_hdr.vfid = &hfid->vfid;
  addr_hdr.pgptr = hdr_pgptr;
  addr_hdr.offset = HEAP_HEADER_AND_CHAIN_SLOTID;
  log_skip_logging_set_lsa (thread_p, &addr_hdr);

  pgbuf_set_dirty (thread_p, hdr_pgptr, FREE);
  hdr_pgptr = NULL;

  return NO_ERROR;

exit_on_error:

  if (error_code != NO_ERROR)
    {
      assert (error_code != NO_ERROR);
      error_code = ER_FAILED;
    }

  if (hdr_pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, hdr_pgptr);
    }

  return error_code;
}


/*
 * heap_bestspace_sync () - Synchronize the statistics of best space
 *   return: NO_ERROR or error code
 *
 *   num_recs(out) the number of record
 *   hfid(in): Heap file identifier
 *   scan_all(in): Scan the whole heap or heap_Find_best_page_limit
 *
 * Note: Synchronize for best space, so that we can reuse heap space as
 * much as possible.
 *
 */
static int
heap_bestspace_sync (THREAD_ENTRY * thread_p, DB_BIGINT * num_recs,
		     const HFID * hfid, bool scan_all)
{
  PAGE_PTR pgptr = NULL;
  VPID vpid;
  VPID start_vpid = { NULL_PAGEID, NULL_VOLID };
  VPID next_vpid = { NULL_PAGEID, NULL_VOLID };
  VPID stopat_vpid = { NULL_PAGEID, NULL_VOLID };
  int num_pages = 0;
  DB_BIGINT num_records = 0;
  int free_space = 0;
  int dummy_npages = 0, nrecords = 0, dummy_rec_length;
  int num_iterations = 0, max_iterations;
  int error_code = NO_ERROR;

  *num_recs = 0;

  error_code = heap_read_full_search_vpid (thread_p, &next_vpid, hfid);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (VPID_ISNULL (&next_vpid))
    {
      /*
       * Start from beginning of heap due to lack of statistics.
       */
      next_vpid.volid = hfid->vfid.volid;
      next_vpid.pageid = hfid->hpgid;
      start_vpid = next_vpid;
    }
  stopat_vpid = next_vpid;

  num_iterations = 0;
  num_pages = file_get_numpages (thread_p, &hfid->vfid, NULL, NULL, NULL);
  max_iterations = MIN ((int) (num_pages * 0.2), heap_Find_best_page_limit);

  num_records = 0;
  do
    {
      if (scan_all == false)
	{
	  if (++num_iterations > max_iterations
	      || heap_Bestspace->bestspace_sync.stop_sync_bestspace == true)
	    {
	      er_log_debug (ARG_FILE_LINE, "heap_bestspace_sync: "
			    "num_iterations %d "
			    "next_vpid { pageid %d volid %d }\n",
			    num_iterations,
			    next_vpid.pageid, next_vpid.volid);

	      break;
	    }
	}

      vpid = next_vpid;
      if (VPID_ISNULL (&vpid))
	{
	  assert (!VPID_ISNULL (&vpid));

	  error_code = ER_FAILED;
	  break;
	}

      pgptr = heap_pgbuf_fix (thread_p, hfid, &vpid,
			      PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
			      PAGE_HEAP);
      if (pgptr == NULL)
	{
	  error_code = er_errid ();
	  if (error_code == NO_ERROR)
	    {
	      assert (error_code != NO_ERROR);

	      error_code = ER_FAILED;
	    }

	  break;
	}

      error_code = heap_vpid_next (hfid, pgptr, &next_vpid);
      if (error_code != NO_ERROR)
	{
	  assert (false);

	  pgbuf_unfix_and_init (thread_p, pgptr);
	  break;
	}

      if (VPID_ISNULL (&next_vpid))
	{
	  /*
	   * Go back to beginning of heap looking for good pages with a lot of
	   * free space
	   */
	  next_vpid.volid = hfid->vfid.volid;
	  next_vpid.pageid = hfid->hpgid;
	}

      spage_collect_statistics (pgptr, &dummy_npages, &nrecords,
				&dummy_rec_length);
      num_records += nrecords;

      free_space = spage_max_space_for_new_record (thread_p, pgptr);
      if (free_space > HEAP_DROP_FREE_SPACE)
	{
	  (void) heap_bestspace_add (thread_p, hfid, &vpid, free_space);
	}
      pgbuf_unfix_and_init (thread_p, pgptr);
    }
  while (!VPID_EQ (&next_vpid, &stopat_vpid));

  er_log_debug (ARG_FILE_LINE, "heap_bestspace_sync: "
		"scans from {%d|%d} to {%d|%d}, num_iterations(%d) "
		"max_iterations(%d)\n",
		start_vpid.volid, start_vpid.pageid,
		next_vpid.volid, next_vpid.pageid,
		num_iterations, max_iterations);

  if (error_code == NO_ERROR)
    {
      *num_recs = num_records;
    }

  if (scan_all == false)
    {
      /* Save the last position to be searched next time. */
      error_code = heap_write_full_search_vpid (thread_p, hfid, &next_vpid);
    }

  return error_code;
}


/*
 * heap_stats_sync_all_heap_files_if_needed () -
 *   return: NO_ERROR or error code
 */
int
heap_bestspace_sync_all_heap_files_if_needed (THREAD_ENTRY * thread_p)
{
  HEAP_SYNC_NODE *sync_list;
  HEAP_SYNC_NODE *node, *node_tail;
  HFID hfid;
  int error_code = NO_ERROR;
  DB_BIGINT dummy_num_recs;
  int node_count;

  sync_list = heap_bestspace_remove_sync_list (thread_p);
  if (sync_list == NULL)
    {
      return NO_ERROR;
    }

  error_code = heap_bestspace_remove_duplicated_hfid (thread_p, sync_list);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  node_count = 0;
  node_tail = node = sync_list;
  while (node != NULL)
    {
      if (!HFID_IS_NULL (&node->hfid) && !OID_ISNULL (&node->class_oid))
	{
	  if (heap_Bestspace->bestspace_sync.stop_sync_bestspace == false)
	    {
	      assert_release ((heap_get_hfid_from_class_oid
			       (thread_p, &node->class_oid,
				&hfid) == NO_ERROR)
			      && HFID_EQ (&node->hfid, &hfid));
	      heap_bestspace_sync (thread_p, &dummy_num_recs, &node->hfid,
				   false);
	    }
	}

      node_tail = node;
      node = node->next;
      node_count++;
    }

  pthread_mutex_lock (&heap_Bestspace->bestspace_sync.sync_mutex);

  assert (node_tail != NULL);
  assert (node_count >= 1);

  node_tail->next = heap_Bestspace->bestspace_sync.free_list;
  heap_Bestspace->bestspace_sync.free_list = sync_list;

  heap_Bestspace->bestspace_sync.free_list_count += node_count;

  heap_Bestspace->bestspace_sync.stop_sync_bestspace = false;

  pthread_mutex_unlock (&heap_Bestspace->bestspace_sync.sync_mutex);

  return NO_ERROR;
}

/*
 * heap_get_last_page () - Get the last page pointer.
 *   return: PAGE_PTR
 *   hfid(in): Object heap file identifier
 *   scan_cache(in): Scan cache
 *   last_vpid(out): VPID of the last page
 *
 * Note: The last vpid is saved on heap header. But we do not write log
 *       related to it. So, if the server stops unintentionally, heap header
 *       may have a wrong value of last vpid. This function will protect it.
 *
 *       The page pointer should be unfixed by the caller.
 *
 */
static PAGE_PTR
heap_get_last_page (THREAD_ENTRY * thread_p, const HFID * hfid,
		    HEAP_SCANCACHE * scan_cache, VPID * last_vpid)
{
  PAGE_PTR pgptr = NULL;

  assert (last_vpid != NULL);

  VPID_SET_NULL (last_vpid);
  if (file_find_last_page (thread_p, &hfid->vfid, last_vpid) == NULL)
    {
      goto exit_on_error;
    }

  /*
   * Fix a real last page.
   */
  pgptr =
    heap_scan_pb_lock_and_fetch (thread_p, last_vpid, X_LOCK,
				 scan_cache, PAGE_HEAP);
  if (pgptr == NULL)
    {
      goto exit_on_error;
    }

  assert (!VPID_ISNULL (last_vpid));
  assert (pgptr != NULL);
  return pgptr;

exit_on_error:

  if (pgptr != NULL)
    {
      pgbuf_unfix (thread_p, pgptr);
    }

  VPID_SET_NULL (last_vpid);

  return NULL;
}

/*
 * heap_link_to_new () - Chain previous last page to new page
 *   return: bool
 *   vfid(in): File where the new page belongs
 *   new_vpid(in): The new page
 *   link(in): Specifications of previous and header page
 *
 * Note: Link previous page with newly created page.
 */
static bool
heap_link_to_new (THREAD_ENTRY * thread_p, const VFID * vfid,
		  const VPID * new_vpid, HEAP_CHAIN_TOLAST * link)
{
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;
  HEAP_CHAIN chain;
  RECDES recdes = RECDES_INITIALIZER;
  int sp_success;

  /*
   * Now, Previous page should point to newly allocated page
   */

  /*
   * Update chain next field of previous last page
   * If previous best1 space page is the heap header page, it contains a heap
   * header instead of a chain.
   */

  addr.vfid = vfid;
  addr.offset = HEAP_HEADER_AND_CHAIN_SLOTID;

  if (link->hdr_pgptr == link->last_pgptr)
    {
      /*
       * Previous last page is the heap header page. Update the next_pageid
       * field.
       */

      addr.pgptr = link->hdr_pgptr;


      if (log_is_tran_in_system_op (thread_p) == true)
	{
	  log_append_undo_data (thread_p, RVHF_STATS, &addr,
				sizeof (*(link->heap_hdr)), link->heap_hdr);
	}

      link->heap_hdr->next_vpid = *new_vpid;

      log_append_redo_data (thread_p, RVHF_STATS, &addr,
			    sizeof (*(link->heap_hdr)), link->heap_hdr);

      pgbuf_set_dirty (thread_p, link->hdr_pgptr, DONT_FREE);
    }
  else
    {
      /*
       * Chain the old page to the newly allocated last page.
       */

      addr.pgptr = link->last_pgptr;

      recdes.area_size = recdes.length = sizeof (chain);
      recdes.type = REC_HOME;
      recdes.data = (char *) &chain;

      /* Get the chain record and put it in recdes...which points to chain */

      if (spage_get_record (addr.pgptr, HEAP_HEADER_AND_CHAIN_SLOTID, &recdes,
			    COPY) != S_SUCCESS)
	{
	  /* Unable to obtain chain record */
	  return false;		/* Initialization has failed */
	}


      if (log_is_tran_in_system_op (thread_p) == true)
	{
	  log_append_undo_data (thread_p, RVHF_CHAIN, &addr, recdes.length,
				recdes.data);
	}

      chain.next_vpid = *new_vpid;
      sp_success = spage_update (thread_p, addr.pgptr,
				 HEAP_HEADER_AND_CHAIN_SLOTID, &recdes);
      if (sp_success != SP_SUCCESS)
	{
	  /*
	   * This looks like a system error: size did not change, so why did
	   * it fail?
	   */
	  if (sp_success != SP_ERROR)
	    {
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_CANNOT_UPDATE_CHAIN_HDR, 4,
		      pgbuf_get_volume_id (addr.pgptr),
		      pgbuf_get_page_id (addr.pgptr), vfid->fileid,
		      pgbuf_get_volume_label (addr.pgptr));
	    }
	  return false;		/* Initialization has failed */
	}

      log_append_redo_data (thread_p, RVHF_CHAIN, &addr, recdes.length,
			    recdes.data);
      pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);
    }

  return true;
}

/*
 * heap_vpid_init_new () - Initialize a newly allocated heap page
 *   return: bool
 *   vfid(in): File where the new page belongs
 *   file_type(in):
 *   new_vpid(in): The new page
 *   ignore_npages(in): Number of contiguous allocated pages
 *                      (Ignored in this function. We allocate only one page)
 *   xlink(in): Chain to next and previous page
 */
static bool
heap_vpid_init_new (THREAD_ENTRY * thread_p, const VFID * vfid,
		    UNUSED_ARG const FILE_TYPE file_type,
		    const VPID * new_vpid, UNUSED_ARG INT32 ignore_npages,
		    void *xlink)
{
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;
  HEAP_CHAIN_TOLAST *link;
  HEAP_CHAIN chain;
  RECDES recdes = RECDES_INITIALIZER;
  INT16 slotid;
  int sp_success;

  link = (HEAP_CHAIN_TOLAST *) xlink;

  assert (link->heap_hdr != NULL);

  addr.vfid = vfid;
  addr.offset = -1;		/* No header slot is initialized */

  /*
   * fetch and initialize the new page. This page should point to previous
   * page.
   */

  addr.pgptr = pgbuf_fix_newpg (thread_p, new_vpid, PAGE_HEAP);
  if (addr.pgptr == NULL)
    {
      return false;		/* Initialization has failed */
    }

  /* Initialize the page and chain it with the previous last allocated page */
  spage_initialize (thread_p, addr.pgptr, heap_get_spage_type (),
		    HEAP_MAX_ALIGN, SAFEGUARD_RVSPACE);

  /*
   * Add a chain record.
   * Next to NULL and Prev to last allocated page
   */
  COPY_OID (&chain.class_oid, &(link->heap_hdr->class_oid));
  pgbuf_get_vpid (link->last_pgptr, &chain.prev_vpid);
  VPID_SET_NULL (&chain.next_vpid);

  recdes.area_size = recdes.length = sizeof (chain);
  recdes.type = REC_HOME;
  recdes.data = (char *) &chain;

  sp_success = spage_insert (thread_p, addr.pgptr, &recdes, &slotid);
  if (sp_success != SP_SUCCESS || slotid != HEAP_HEADER_AND_CHAIN_SLOTID)
    {
      /*
       * Initialization has failed !!
       */
      if (sp_success != SP_SUCCESS)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
		  1, "");
	}
      pgbuf_unfix_and_init (thread_p, addr.pgptr);
      return false;		/* Initialization has failed */
    }

  /*
   * We don't need to log before images for undos since allocation of pages is
   * an operation-destiny which does not depend on the transaction except for
   * newly created files. Pages may be shared by multiple concurrent
   * transactions, thus the deallocation cannot be undone. Note that new files
   * and their pages are deallocated when the transactions that create the
   * files are aborted.
   */

  log_append_redo_data (thread_p, RVHF_NEWPAGE, &addr,
			recdes.length, recdes.data);
  pgbuf_set_dirty (thread_p, addr.pgptr, FREE);
  addr.pgptr = NULL;

  /*
   * Now, link previous page to newly allocated page
   */
  return heap_link_to_new (thread_p, vfid, new_vpid, link);
}

/*
 * heap_vpid_alloc () - allocate, fetch, and initialize a new page
 *   return: ponter to newly allocated page or NULL
 *   hfid(in): Object heap file identifier
 *   needed_space(in): The minimal space needed on new page
 *   scan_cache(in): Scan cache
 *
 * Note: Allocate and initialize a new heap page. The heap header is
 * updated to reflect a newly allocated best space page and
 * the set of best space pages information may be updated to
 * include the previous best1 space page.
 */
static PAGE_PTR
heap_vpid_alloc (THREAD_ENTRY * thread_p, const HFID * hfid,
		 UNUSED_ARG int needed_space, HEAP_SCANCACHE * scan_cache)
{
  VPID vpid;			/* Volume and page identifiers */
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;	/* Address of logging data */
  PAGE_PTR new_pgptr = NULL;
  PAGE_PTR last_pgptr = NULL;
  HEAP_CHAIN_TOLAST tolast;
  VPID last_vpid;
  PAGE_PTR hdr_pgptr;
  HEAP_HDR_STATS *heap_hdr;
  RECDES hdr_recdes = RECDES_INITIALIZER;
  OID class_oid;

  vpid.volid = hfid->vfid.volid;
  vpid.pageid = hfid->hpgid;

  hdr_pgptr = heap_pgbuf_fix (thread_p, hfid, &vpid,
			      PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH,
			      PAGE_HEAP_HEADER);
  if (hdr_pgptr == NULL)
    {
      /* something went wrong. Unable to fetch header page */
      return NULL;
    }

  if (spage_get_record (hdr_pgptr, HEAP_HEADER_AND_CHAIN_SLOTID,
			&hdr_recdes, PEEK) != S_SUCCESS)
    {
      pgbuf_unfix_and_init (thread_p, hdr_pgptr);
      return NULL;
    }

  heap_hdr = (HEAP_HDR_STATS *) hdr_recdes.data;
  COPY_OID (&class_oid, &heap_hdr->class_oid);

  last_pgptr = heap_get_last_page (thread_p, hfid, scan_cache, &last_vpid);
  if (last_pgptr == NULL)
    {
      pgbuf_unfix_and_init (thread_p, hdr_pgptr);

      /* something went wrong, return error */
      return NULL;
    }
  assert (!VPID_ISNULL (&last_vpid));

  /*
   * Now allocate a new page as close as possible to the last allocated page.
   * Note that a new page is allocated when the best1 space page in the
   * statistics is the actual last page on the heap.
   */

  /*
   * Prepare initialization fields, so that current page will point to
   * previous page.
   */

  tolast.hdr_pgptr = hdr_pgptr;
  tolast.last_pgptr = last_pgptr;
  tolast.heap_hdr = heap_hdr;

  if (file_alloc_pages (thread_p, &hfid->vfid, &vpid, 1,
			&last_vpid, heap_vpid_init_new, &tolast) == NULL)
    {
      /* Unable to allocate a new page */
      pgbuf_unfix_and_init (thread_p, last_pgptr);
      pgbuf_unfix_and_init (thread_p, hdr_pgptr);

      return NULL;
    }

  pgbuf_unfix_and_init (thread_p, last_pgptr);

  addr.vfid = &hfid->vfid;
  addr.offset = HEAP_HEADER_AND_CHAIN_SLOTID;
  addr.pgptr = hdr_pgptr;
  hdr_pgptr = NULL;

  log_skip_logging_set_lsa (thread_p, &addr);
  pgbuf_set_dirty (thread_p, addr.pgptr, FREE);
  addr.pgptr = NULL;

  /*
   * Note: we fetch the page as old since it was initialized during the
   * allocation by heap_vpid_init_new, therefore, we care about the current
   * content of the page.
   */
  new_pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, X_LOCK,
					   scan_cache, PAGE_HEAP);

  (void) heap_bestspace_append_hfid_to_sync_list (thread_p, hfid, &class_oid);

  /*
   * Even though an error is returned from heap_scan_pb_lock_and_fetch,
   * we will just return new_pgptr (maybe NULL)
   * and do not deallocate the newly added page.
   * Because file_alloc_page was committed with top operation.
   * The added page will be used later by other insert operation.
   */

  return new_pgptr;		/* new_pgptr is lock and fetch */
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_vpid_remove () - Deallocate a heap page
 *   return: rm_vpid on success or NULL on error
 *   hfid(in): Object heap file identifier
 *   heap_hdr(in): The heap header stats
 *   rm_vpid(in): Page to remove
 *
 * Note: The given page is removed from the heap. The linked list of heap
 * pages is updated to remove this page, and the heap header may
 * be updated if this page was part of the statistics.
 */
static VPID *
heap_vpid_remove (THREAD_ENTRY * thread_p, const HFID * hfid,
		  HEAP_HDR_STATS * heap_hdr, VPID * rm_vpid)
{
  PAGE_PTR rm_pgptr = NULL;	/* Pointer to page to be removed    */
  RECDES rm_recdes = RECDES_INITIALIZER;	/* Record descriptor which holds the
						 * chain of the page to be removed
						 */
  HEAP_CHAIN *rm_chain;		/* Chain information of the page to be
				 * removed
				 */
  VPID vpid;			/* Real identifier of previous page */
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;	/* Log address of previous page     */
  RECDES recdes = RECDES_INITIALIZER;	/* Record descriptor to page header */
  HEAP_CHAIN chain;		/* Chain to next and prev page      */
  int sp_success;
  int i;

  /*
   * Make sure that this is not the header page since the header page cannot
   * be removed. If the header page is removed.. the heap is gone
   */

  addr.pgptr = NULL;
  if (rm_vpid->pageid == hfid->hpgid && rm_vpid->volid == hfid->vfid.volid)
    {
      er_log_debug (ARG_FILE_LINE, "heap_vpid_remove: Trying to remove header"
		    " page = %d|%d of heap file = %d|%d|%d",
		    (int) rm_vpid->volid, rm_vpid->pageid,
		    (int) hfid->vfid.volid, hfid->vfid.fileid, hfid->hpgid);
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      "");
      goto error;
    }

  /* Get the chain record */
  rm_pgptr = heap_scan_pb_lock_and_fetch (thread_p, rm_vpid, OLD_PAGE, X_LOCK,
					  NULL, PAGE_HEAP);
  if (rm_pgptr == NULL)
    {
      /* Look like a system error. Unable to obtain chain header record */
      goto error;
    }
  if (spage_get_record (rm_pgptr, HEAP_HEADER_AND_CHAIN_SLOTID, &rm_recdes,
			PEEK) != S_SUCCESS)
    {
      /* Look like a system error. Unable to obtain chain header record */
      goto error;
    }

  rm_chain = (HEAP_CHAIN *) rm_recdes.data;

  /*
   * UPDATE PREVIOUS PAGE
   *
   * Update chain next field of previous last page
   * If previous page is the heap header page, it contains a heap header
   * instead of a chain.
   */

  vpid = rm_chain->prev_vpid;
  addr.vfid = &hfid->vfid;
  addr.offset = HEAP_HEADER_AND_CHAIN_SLOTID;

  addr.pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, OLD_PAGE, X_LOCK,
					    NULL, PAGE_HEAP);
  if (addr.pgptr == NULL)
    {
      /* something went wrong, return */
      goto error;
    }

  /*
   * Make sure that the page to be removed is not referenced on the heap
   * statistics
   */

  assert (heap_hdr != NULL);

  /*
   * Is previous page the header page ?
   */
  if (vpid.pageid == hfid->hpgid && vpid.volid == hfid->vfid.volid)
    {
      /*
       * PREVIOUS PAGE IS THE HEADER PAGE.
       * It contains a heap header instead of a chain record
       */
      heap_hdr->next_vpid = rm_chain->next_vpid;
    }
  else
    {
      /*
       * PREVIOUS PAGE IS NOT THE HEADER PAGE.
       * It contains a chain...
       * We need to make sure that there is not references to the page to delete
       * in the statistics of the heap header
       */

      /* NOW check the PREVIOUS page */

      if (spage_get_record (addr.pgptr, HEAP_HEADER_AND_CHAIN_SLOTID,
			    &recdes, PEEK) != S_SUCCESS)
	{
	  /* Look like a system error. Unable to obtain header record */
	  goto error;
	}

      /* Copy the chain record to memory.. so we can log the changes */
      memcpy (&chain, recdes.data, sizeof (chain));

      /* Modify the chain of the previous page in memory */
      chain.next_vpid = rm_chain->next_vpid;

      /* Get the chain record */
      recdes.area_size = recdes.length = sizeof (chain);
      recdes.type = REC_HOME;
      recdes.data = (char *) &chain;

      /* Log the desired changes.. and then change the header */
      log_append_undoredo_data (thread_p, RVHF_CHAIN, &addr, sizeof (chain),
				sizeof (chain), recdes.data, &chain);

      /* Now change the record */

      sp_success = spage_update (thread_p, addr.pgptr,
				 HEAP_HEADER_AND_CHAIN_SLOTID, &recdes);
      if (sp_success != SP_SUCCESS)
	{
	  /*
	   * This look like a system error, size did not change, so why did it
	   * fail
	   */
	  if (sp_success != SP_ERROR)
	    {
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_GENERIC_ERROR, 1, "");
	    }
	  goto error;
	}

    }

  /* Now set dirty, free and unlock the previous page */
  pgbuf_set_dirty (thread_p, addr.pgptr, FREE);
  addr.pgptr = NULL;

  /*
   * UPDATE NEXT PAGE
   *
   * Update chain previous field of next page
   */

  if (!(VPID_ISNULL (&rm_chain->next_vpid)))
    {
      vpid = rm_chain->next_vpid;
      addr.offset = HEAP_HEADER_AND_CHAIN_SLOTID;

      addr.pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, OLD_PAGE,
						X_LOCK, NULL, PAGE_HEAP);
      if (addr.pgptr == NULL)
	{
	  /* something went wrong, return */
	  goto error;
	}

      /* Get the chain record */
      if (spage_get_record (addr.pgptr, HEAP_HEADER_AND_CHAIN_SLOTID,
			    &recdes, PEEK) != S_SUCCESS)
	{
	  /* Look like a system error. Unable to obtain header record */
	  goto error;
	}

      /* Copy the chain record to memory.. so we can log the changes */
      memcpy (&chain, recdes.data, sizeof (chain));

      /* Modify the chain of the next page in memory */
      chain.prev_vpid = rm_chain->prev_vpid;

      /* Log the desired changes.. and then change the header */
      log_append_undoredo_data (thread_p, RVHF_CHAIN, &addr, sizeof (chain),
				sizeof (chain), recdes.data, &chain);

      /* Now change the record */
      recdes.area_size = recdes.length = sizeof (chain);
      recdes.type = REC_HOME;
      recdes.data = (char *) &chain;

      sp_success = spage_update (thread_p, addr.pgptr,
				 HEAP_HEADER_AND_CHAIN_SLOTID, &recdes);
      if (sp_success != SP_SUCCESS)
	{
	  /*
	   * This look like a system error, size did not change, so why did it
	   * fail
	   */
	  if (sp_success != SP_ERROR)
	    {
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_GENERIC_ERROR, 1, "");
	    }
	  goto error;
	}

      /* Now set dirty, free and unlock the next page */

      pgbuf_set_dirty (thread_p, addr.pgptr, FREE);
      addr.pgptr = NULL;
    }

  /* Free the page to be deallocated and deallocate the page */
  pgbuf_unfix_and_init (thread_p, rm_pgptr);

  if (file_dealloc_page (thread_p, &hfid->vfid, rm_vpid) != NO_ERROR)
    {
      goto error;
    }

  (void) heap_bestspace_del_entry_by_vpid (thread_p, rm_vpid);

  return rm_vpid;

error:
  if (rm_pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, rm_pgptr);
    }
  if (addr.pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, addr.pgptr);
    }

  return NULL;
}
#endif

/*
 * heap_vpid_next () - Find next page of heap
 *   return: NO_ERROR
 *   hfid(in): Object heap file identifier
 *   pgptr(in): Current page pointer
 *   next_vpid(in/out): Next volume-page identifier
 *
 * Note: Find the next page of heap file.
 */
static int
heap_vpid_next (const HFID * hfid, PAGE_PTR pgptr, VPID * next_vpid)
{
  HEAP_CHAIN *chain;		/* Chain to next and prev page      */
  HEAP_HDR_STATS *heap_hdr;	/* Header of heap file              */
  RECDES recdes = RECDES_INITIALIZER;	/* Record descriptor to page header */
  int ret = NO_ERROR;

  /* Get either the heap header or chain record */
  if (spage_get_record (pgptr, HEAP_HEADER_AND_CHAIN_SLOTID, &recdes, PEEK)
      != S_SUCCESS)
    {
      /* Unable to get header/chain record for the given page */
      VPID_SET_NULL (next_vpid);
      ret = ER_FAILED;
    }
  else
    {
      pgbuf_get_vpid (pgptr, next_vpid);
      /* Is this the header page ? */
      if (next_vpid->pageid == hfid->hpgid
	  && next_vpid->volid == hfid->vfid.volid)
	{
	  heap_hdr = (HEAP_HDR_STATS *) recdes.data;
	  *next_vpid = heap_hdr->next_vpid;
	}
      else
	{
	  chain = (HEAP_CHAIN *) recdes.data;
	  *next_vpid = chain->next_vpid;
	}
    }

  return ret;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_vpid_prev () - Find previous page of heap
 *   return: NO_ERROR
 *   hfid(in): Object heap file identifier
 *   pgptr(in): Current page pointer
 *   prev_vpid(in/out): Previous volume-page identifier
 *
 * Note: Find the previous page of heap file.
 */
static int
heap_vpid_prev (const HFID * hfid, PAGE_PTR pgptr, VPID * prev_vpid)
{
  HEAP_CHAIN *chain;		/* Chain to next and prev page      */
  RECDES recdes = RECDES_INITIALIZER;	/* Record descriptor to page header */
  int ret = NO_ERROR;

  /* Get either the header or chain record */
  if (spage_get_record (pgptr, HEAP_HEADER_AND_CHAIN_SLOTID, &recdes, PEEK)
      != S_SUCCESS)
    {
      /* Unable to get header/chain record for the given page */
      VPID_SET_NULL (prev_vpid);
      ret = ER_FAILED;
    }
  else
    {
      pgbuf_get_vpid (pgptr, prev_vpid);
      /* Is this the header page ? */
      if (prev_vpid->pageid == hfid->hpgid
	  && prev_vpid->volid == hfid->vfid.volid)
	{
	  VPID_SET_NULL (prev_vpid);
	}
      else
	{
	  chain = (HEAP_CHAIN *) recdes.data;
	  *prev_vpid = chain->prev_vpid;
	}
    }

  return ret;
}
#endif

/*
 * heap_manager_initialize () -
 *   return: NO_ERROR
 *
 * Note: Initialization process of the heap file module. Find the
 * maximum size of an object that can be inserted in the heap.
 * Objects that overpass this size are stored in overflow.
 */
int
heap_manager_initialize (void)
{
  int ret;

#define HEAP_MAX_FIRSTSLOTID_LENGTH (sizeof (HEAP_HDR_STATS))

  heap_Maxslotted_reclength = (spage_max_record_size ()
			       - HEAP_MAX_FIRSTSLOTID_LENGTH);
  heap_Slotted_overhead = spage_slot_size ();

  /* Initialize the class representation cache */

  ret = heap_classrepr_initialize_cache ();
  if (ret != NO_ERROR)
    {
      return ret;
    }

  /* Initialize best space cache */
  ret = heap_bestspace_initialize ();
  if (ret != NO_ERROR)
    {
      return ret;
    }

  return ret;
}

/*
 * heap_manager_finalize () - Terminate the heap manager
 *   return: NO_ERROR
 * Note: Deallocate any cached structure.
 */
int
heap_manager_finalize (void)
{
  int ret;

  ret = heap_classrepr_finalize_cache ();
  if (ret != NO_ERROR)
    {
      return ret;
    }

  ret = heap_bestspace_finalize ();
  if (ret != NO_ERROR)
    {
      return ret;
    }

  return ret;
}

/*
 * heap_create_internal () - Create a heap file
 *   return: HFID * (hfid on success and NULL on failure)
 *   hfid(in/out): Object heap file identifier.
 *                 All fields in the identifier are set, except the volume
 *                 identifier which should have already been set by the caller.
 *   exp_npgs(in): Expected number of pages
 *   class_oid(in): OID of the class for which the heap will be created.
 *
 * Note: Creates a heap file on the disk volume associated with
 * hfid->vfid->volid.
 *
 * A set of sectors is allocated to improve locality of the heap.
 * The number of sectors to allocate is estimated from the number
 * of expected pages. The maximum number of allocated sectors is
 * 25% of the total number of sectors in disk. When the number of
 * pages cannot be estimated, a negative value can be passed to
 * indicate so. In this case, no sectors are allocated. The
 * number of expected pages are not allocated at this moment,
 * they are allocated as needs arrives.
 */
static HFID *
heap_create_internal (THREAD_ENTRY * thread_p, HFID * hfid, int exp_npgs,
		      const OID * class_oid)
{
  HEAP_HDR_STATS heap_hdr;	/* Heap file header            */
  VPID vpid;			/* Volume and page identifiers */
  RECDES recdes = RECDES_INITIALIZER;	/* Record descriptor           */
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;	/* Address of logging data     */
  INT16 slotid;
  int sp_success;
  FILE_HEAP_DES hfdes;
  bool top_op_active = false;

  if (hfid == NULL)
    {
      assert (false);
      GOTO_EXIT_ON_ERROR;
    }

#if !defined(NDEBUG)
#if defined(SERVER_MODE)
  assert (class_oid != NULL);
  assert (!OID_ISNULL (class_oid));
#else
  if (db_get_client_type () == BOOT_CLIENT_CREATEDB)
    {
      assert (class_oid == NULL || !OID_ISNULL (class_oid));
    }
  else
    {
      assert (class_oid != NULL);
      assert (!OID_ISNULL (class_oid));
    }
#endif
#endif

  /* create a file descriptor */
  if (class_oid != NULL)
    {
      hfdes.class_oid = *class_oid;
    }
  else
    {
      OID_SET_NULL (&hfdes.class_oid);
    }

  if (exp_npgs < 3)
    {
      exp_npgs = 3;
    }

  addr.pgptr = NULL;
  if (log_start_system_op (thread_p) == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }
  top_op_active = true;

  /*
   * Create the unstructured file for the heap
   * Create the header for the heap file. The header is used to speed
   * up insertions of objects and to find some simple information about the
   * heap.
   * We do not initialize the page during the allocation since the file is
   * new, and the file is going to be removed in the event of a crash.
   */

  if (file_create (thread_p, &hfid->vfid, exp_npgs, FILE_HEAP, &hfdes,
		   &vpid, 1) == NULL)
    {
      /* Unable to create the heap file */
      GOTO_EXIT_ON_ERROR;
    }

  addr.pgptr = pgbuf_fix_newpg (thread_p, &vpid, PAGE_HEAP_HEADER);
  if (addr.pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  assert (file_find_page (thread_p, &(hfid->vfid), &vpid) == true);

  hfid->hpgid = vpid.pageid;

  (void) heap_bestspace_del_all_entry_by_hfid (thread_p, hfid);

  /* Initialize header page */
  spage_initialize (thread_p, addr.pgptr, heap_get_spage_type (),
		    HEAP_MAX_ALIGN, SAFEGUARD_RVSPACE);

  /* Now insert header */
  COPY_OID (&heap_hdr.class_oid, &hfdes.class_oid);
  VFID_SET_NULL (&heap_hdr.ovf_vfid);
  VPID_SET_NULL (&heap_hdr.next_vpid);

  heap_hdr.full_search_vpid.volid = hfid->vfid.volid;
  heap_hdr.full_search_vpid.pageid = hfid->hpgid;

  heap_hdr.reserve1_for_future = 0;
  heap_hdr.reserve2_for_future = 0;

  recdes.area_size = recdes.length = sizeof (HEAP_HDR_STATS);
  recdes.type = REC_HOME;
  recdes.data = (char *) &heap_hdr;

  sp_success = spage_insert (thread_p, addr.pgptr, &recdes, &slotid);
  if (sp_success != SP_SUCCESS || slotid != HEAP_HEADER_AND_CHAIN_SLOTID)
    {
      /* something went wrong, destroy file and return error */
      if (sp_success != SP_SUCCESS)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HEAP_UNABLE_TO_CREATE_HEAP, 1,
		  fileio_get_volume_label (hfid->vfid.volid, PEEK));
	}

      GOTO_EXIT_ON_ERROR;
    }

  /*
   * Don't need to log before image (undo) since file and pages of the heap
   * are deallocated during undo (abort).
   */
  addr.vfid = &hfid->vfid;
  addr.offset = HEAP_HEADER_AND_CHAIN_SLOTID;
  log_append_redo_data (thread_p, RVHF_NEWHDR,
			&addr, sizeof (heap_hdr), &heap_hdr);
  pgbuf_set_dirty (thread_p, addr.pgptr, FREE);
  addr.pgptr = NULL;

  log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);
  top_op_active = false;

  file_new_declare_as_old (thread_p, &hfid->vfid);

  addr.vfid = NULL;
  addr.pgptr = NULL;
  addr.offset = 0;
  log_append_undo_data (thread_p, RVHF_CREATE, &addr, sizeof (HFID), hfid);

  if (FI_TEST_ARG_INT (thread_p, FI_TEST_HEAP_CREATE_ERROR1,
		       0, 0) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  return hfid;

exit_on_error:

  if (addr.pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, addr.pgptr);
    }

  if (hfid != NULL)
    {
      HFID_SET_NULL (hfid);
    }

  if (top_op_active == true)
    {
      log_end_system_op (thread_p, LOG_RESULT_TOPOP_ABORT);
    }

  return NULL;
}

#if defined(RYE_DEBUG)
/*
 * heap_hfid_isvalid () -
 *   return:
 *   hfid(in):
 */
static DISK_ISVALID
heap_hfid_isvalid (HFID * hfid)
{
  DISK_ISVALID valid_pg = DISK_VALID;

  if (hfid == NULL || HFID_IS_NULL (hfid))
    {
      return DISK_INVALID;
    }

  valid_pg = disk_isvalid_page (hfid->vfid.volid, hfid->vfid.fileid);
  if (valid_pg == DISK_VALID)
    {
      valid_pg = disk_isvalid_page (hfid->vfid.volid, hfid->hpgid);
    }

  return valid_pg;
}
#endif /* RYE_DEBUG */

/*
 * xheap_create () - Create a heap file
 *   return: int
 *   hfid(in/out): Object heap file identifier.
 *                 All fields in the identifier are set, except the volume
 *                 identifier which should have already been set by the caller.
 *   class_oid(in): OID of the class for which the heap will be created.
 *
 * Note: Creates an object heap file on the disk volume associated with
 * hfid->vfid->volid.
 */
int
xheap_create (THREAD_ENTRY * thread_p, HFID * hfid, const OID * class_oid)
{
  if (heap_create_internal (thread_p, hfid, -1, class_oid) == NULL)
    {
      return er_errid ();
    }
  else
    {
      return NO_ERROR;
    }
}

/*
 * xheap_destroy - Destroy heap
 *   return: NO_ERROR
 *   hfid(in): Object heap file identifier.
 *
 * Note: Destroy the heap file associated with the given heap
 */
int
xheap_destroy (THREAD_ENTRY * thread_p, const HFID * hfid)
{
  VFID vfid;
  int ret;

  assert (file_is_new_file (thread_p, &(hfid->vfid)) == FILE_OLD_FILE);

  if (heap_ovf_find_vfid (thread_p, hfid, &vfid, false) != NULL)
    {
      ret = file_destroy (thread_p, &vfid);
      if (ret != NO_ERROR)
	{
	  return ret;
	}
    }

  ret = file_destroy (thread_p, &hfid->vfid);
  if (ret == NO_ERROR)
    {
      (void) heap_bestspace_del_all_entry_by_hfid (thread_p, hfid);
    }

  return ret;
}

/*
 * heap_assign_address () - Assign a new location
 *   return: NO_ERROR / ER_FAILED
 *   hfid(in): Object heap file identifier
 *   oid(out): Object identifier.
 *   expected_length(in): Expected length
 *
 * Note: Assign an OID to an object and reserve the expected length for
 * the object. The following rules are observed for the expected length.
 *              1. A negative value is passed when only an approximation of
 *                 the length of the object is known. This approximation is
 *                 taken as the minimal length by this module. This case is
 *                 used when the transformer module (tfcl) skips some fileds
 *                 while walking through the object to find out its length.
 *                 a) Heap manager find the average length of objects in the
 *                    heap.
 *                    If the average length > abs(expected_length)
 *                    The average length is used instead
 *              2. A zero value, heap manager uses the average length of the
 *                 objects in the heap.
 *              3. If length is larger than one page, the size of an OID is
 *                 used since the object is going to be stored in overflow
 *              4. If length is > 0 and smaller than OID_SIZE
 *                 OID_SIZE is used as the expected length.
 */
int
heap_assign_address (THREAD_ENTRY * thread_p, const HFID * hfid, OID * oid,
		     int expected_length)
{
  RECDES recdes = RECDES_INITIALIZER;

  if (expected_length <= 0)
    {
      recdes.length = heap_estimate_avg_length (thread_p, hfid);
      if (recdes.length > (-expected_length))
	{
	  expected_length = recdes.length;
	}
      else
	{
	  expected_length = -expected_length;
	}
    }

  /*
   * Use the expected length only when it is larger than the size of an OID
   * and it is smaller than the maximum size of an object that can be stored
   * in the primary area (no in overflow). In any other case, use the the size
   * of an OID as the length.
   */

  recdes.length = ((expected_length > SSIZEOF (OID)
		    && !heap_is_big_length (expected_length))
		   ? expected_length : SSIZEOF (OID));

  recdes.data = NULL;
  recdes.type = REC_ASSIGN_ADDRESS;

  return heap_insert_internal (thread_p, hfid, oid, &recdes, NULL, false,
			       recdes.length, NULL);
}

/*
 * heap_assign_address_with_class_oid () - Assign a new location and lock the
 *                                       object
 *   return:
 *   hfid(in):
 *   class_oid(in):
 *   oid(in):
 *   expected_length(in):
 */
int
heap_assign_address_with_class_oid (THREAD_ENTRY * thread_p,
				    const HFID * hfid, OID * class_oid,
				    OID * oid, int expected_length)
{
  RECDES recdes = RECDES_INITIALIZER;
  int err = NO_ERROR;

#if 1				/* TODO - need more consideration */
  oid->groupid = NULL_GROUPID;	/* init */
#endif

  if (expected_length <= 0)
    {
      recdes.length = heap_estimate_avg_length (thread_p, hfid);
      if (recdes.length > (-expected_length))
	{
	  expected_length = recdes.length;
	}
      else
	{
	  expected_length = -expected_length;
	}
    }

  /*
   * Use the expected length only when it is larger than the size of an OID
   * and it is smaller than the maximum size of an object that can be stored
   * in the primary area (no in overflow). In any other case, use the the size
   * of an OID as the length.
   */

  recdes.length = ((expected_length > SSIZEOF (OID)
		    && !heap_is_big_length (expected_length))
		   ? expected_length : SSIZEOF (OID));

  recdes.data = NULL;
  recdes.type = REC_ASSIGN_ADDRESS;

  err = heap_insert_with_lock_internal (thread_p, hfid, oid, class_oid,
					&recdes, NULL, false, recdes.length);

#if 1
  assert (oid->groupid == NULL_GROUPID);
#endif

  return err;
}

/*
 * heap_insert_internal () - Insert a non-multipage object onto heap
 *   return: NO_ERROR / ER_FAILED
 *   hfid(in): Object heap file identifier
 *   oid(out): Object identifier.
 *   recdes(in): Record descriptor
 *   scan_cache(in/out): Scan cache used to estimate the best space pages
 *                       between heap changes.
 *   ishome_insert(in):
 *   guess_sumlen(in):
 *   class_oid(in): class oid for migrator
 *
 * Note: Insert an object that does not expand multiple pages onto the
 * given file heap. The object is inserted by the following algorithm:
 *              1: If the object can be inserted in the best1 space page
 *                 (usually last allocated page of heap) without overpassing
 *                 the reserved space on the page, the object is placed on
 *                 this page.
 *              2: If the object can be inserted in one of the best space
 *                 pages without overpassing the reserved space on the page,
 *                 the object is placed on this page.
 *              3: The object is inserted in a newly allocated page. Don't
 *                 care about reserve space here.
 *
 * Note: This function does not store objects in overflow.
 */
static int
heap_insert_internal (THREAD_ENTRY * thread_p, const HFID * hfid, OID * oid,
		      RECDES * recdes, HEAP_SCANCACHE * scan_cache,
		      bool ishome_insert, int guess_sumlen,
		      const OID * class_oid)
{
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;	/* Address of logging data */
  int sp_success;
  bool isnew_rec;
  RECDES *undo_recdes;
  HEAP_BESTSPACE best;

  assert (file_is_new_file (thread_p, &hfid->vfid) == FILE_OLD_FILE);
  assert (scan_cache == NULL || HFID_EQ (hfid, &(scan_cache->hfid)));

  addr.vfid = &hfid->vfid;

  if (recdes->type != REC_NEWHOME)
    {
      isnew_rec = true;
    }
  else
    {
      /*
       * This is an old object (relocated) and we do not have any idea on
       * the difference in length.
       */
      isnew_rec = false;
    }

  OID_SET_NULL (oid);
#if defined(RYE_DEBUG)
  if (heap_is_big_length (recdes->length))
    {
      er_log_debug (ARG_FILE_LINE,
		    "heap_insert_internal: This function does not accept"
		    " objects longer than %d. An object of %d was given\n",
		    heap_Maxslotted_reclength, recdes->length);
      return ER_FAILED;
    }
#endif

  addr.pgptr = heap_find_best_page (thread_p, hfid, recdes->length,
				    isnew_rec, guess_sumlen, scan_cache);
  if (addr.pgptr == NULL)
    {
      /* something went wrong. Unable to fetch hinted page. Return */
      return ER_FAILED;
    }

  /* Insert the object */
  sp_success = spage_insert (thread_p, addr.pgptr, recdes, &oid->slotid);
  if (sp_success == SP_SUCCESS)
    {
      RECDES tmp_recdes = RECDES_INITIALIZER;
      INT16 bytes_reserved;

      oid->volid = pgbuf_get_volume_id (addr.pgptr);
      oid->pageid = pgbuf_get_page_id (addr.pgptr);

      if (recdes->type == REC_ASSIGN_ADDRESS)
	{
	  bytes_reserved = (INT16) recdes->length;
	  tmp_recdes.type = recdes->type;
	  tmp_recdes.area_size = sizeof (bytes_reserved);
	  tmp_recdes.length = sizeof (bytes_reserved);
	  tmp_recdes.data = (char *) &bytes_reserved;
	  undo_recdes = &tmp_recdes;
	}
      else
	{
	  undo_recdes = recdes;
	}

      /* Log the insertion, set the page dirty, free, and unlock */
      addr.offset = oid->slotid;
      log_append_undoredo_recdes (thread_p, RVHF_INSERT, &addr, NULL,
				  undo_recdes, class_oid);
      pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);
    }
  else
    {
      if (sp_success != SP_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
		  1, "");
	}
      oid->slotid = NULL_SLOTID;
    }

  if (addr.pgptr != NULL)
    {
      best.vpid.volid = oid->volid;
      best.vpid.pageid = oid->pageid;

      best.freespace = spage_get_free_space_without_saving (thread_p,
							    addr.pgptr);

      (void) heap_bestspace_add (thread_p, hfid, &best.vpid, best.freespace);
    }

  /*
   * Cache the page for any future scan modifications
   */

  if (scan_cache != NULL && scan_cache->cache_last_fix_page == true
      && ishome_insert == true)
    {
      scan_cache->pgptr = addr.pgptr;
    }
  else
    {
      pgbuf_unfix_and_init (thread_p, addr.pgptr);
    }

  return NO_ERROR;
}



static PAGE_PTR
heap_find_slot_for_insert (THREAD_ENTRY * thread_p, const HFID * hfid,
			   HEAP_SCANCACHE * scan_cache,
			   OID * oid, RECDES * recdes,
			   UNUSED_ARG OID * class_oid)
{
  int slot_id = 0;
//  int slot_num;
  PAGE_PTR pgptr;

  assert (scan_cache == NULL || HFID_EQ (hfid, &(scan_cache->hfid)));

  pgptr = heap_find_best_page (thread_p, hfid, recdes->length,
			       (recdes->type != REC_NEWHOME),
			       recdes->length, scan_cache);
  if (pgptr == NULL)
    {
      return NULL;
    }

//  slot_num = spage_number_of_slots (pgptr);

  oid->volid = pgbuf_get_volume_id (pgptr);
  oid->pageid = pgbuf_get_page_id (pgptr);

  /* find REC_DELETED_WILL_REUSE slot or add new slot */
  /* slot_id == slot_num means add new slot */
  slot_id = spage_find_free_slot (pgptr, NULL, slot_id);
  oid->slotid = slot_id;

  if (slot_id == SP_ERROR)
    {
      OID_SET_NULL (oid);
      pgbuf_unfix_and_init (thread_p, pgptr);
      return NULL;
    }

#if !defined(NDEBUG)
  {
    VPID vpid;

    vpid.volid = oid->volid;
    vpid.pageid = oid->pageid;
    assert (file_find_page (thread_p, &(hfid->vfid), &vpid) == true);
  }
#endif

  return pgptr;
}


/*
 * heap_insert_with_lock_internal () -
 *   return:
 *   hfid(in):
 *   oid(in/out):
 *   class_oid(in):
 *   recdes(in):
 *   scan_cache(in):
 *   ishome_insert(in):
 *   guess_sumlen(in):
 */
static int
heap_insert_with_lock_internal (THREAD_ENTRY * thread_p, const HFID * hfid,
				OID * oid, OID * class_oid, RECDES * recdes,
				HEAP_SCANCACHE * scan_cache,
				bool ishome_insert,
				UNUSED_ARG int guess_sumlen)
{
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;	/* Address of logging data */
//  bool isnew_rec;
  RECDES tmp_recdes = RECDES_INITIALIZER, *undo_recdes = NULL;
  INT16 bytes_reserved;
  HEAP_BESTSPACE best;

  assert (file_is_new_file (thread_p, &hfid->vfid) == FILE_OLD_FILE);
  assert (scan_cache == NULL || HFID_EQ (hfid, &(scan_cache->hfid)));

  addr.vfid = &hfid->vfid;

#if 0
  if (recdes->type != REC_NEWHOME)
    {
      isnew_rec = true;
    }
  else
    {
      /*
       * This is an old object (relocated) and we do not have any idea on
       * the difference in length.
       */
      isnew_rec = false;
    }
#endif

  OID_SET_NULL (oid);

#if defined(RYE_DEBUG)
  if (heap_is_big_length (recdes->length))
    {
      er_log_debug (ARG_FILE_LINE,
		    "heap_insert_internal: This function does not accept"
		    " objects longer than %d. An object of %d was given\n",
		    heap_Maxslotted_reclength, recdes->length);
      return ER_FAILED;
    }
#endif

  addr.pgptr = heap_find_slot_for_insert (thread_p, hfid, scan_cache, oid,
					  recdes, class_oid);
  if (addr.pgptr == NULL)
    {
      return ER_FAILED;
    }

  /* insert a original record */
  if (spage_insert_at (thread_p, addr.pgptr, oid->slotid, recdes)
      != SP_SUCCESS)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      "");
      oid = NULL;
      goto unfix_end;
    }

  if (recdes->type == REC_ASSIGN_ADDRESS)
    {
      bytes_reserved = (INT16) recdes->length;
      tmp_recdes.type = recdes->type;
      tmp_recdes.area_size = sizeof (bytes_reserved);
      tmp_recdes.length = sizeof (bytes_reserved);
      tmp_recdes.data = (char *) &bytes_reserved;
      undo_recdes = &tmp_recdes;
    }
  else
    {
      undo_recdes = recdes;
    }

  /* Log the insertion, set the page dirty, free, and unlock */
  addr.offset = oid->slotid;
  if (recdes->type == REC_HOME)
    {
      log_append_undoredo_recdes (thread_p, RVHF_INSERT, &addr, NULL,
				  undo_recdes, class_oid);
    }
  else
    {
      log_append_undoredo_recdes (thread_p, RVHF_INSERT, &addr, NULL,
				  undo_recdes, NULL);
    }

  pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);

unfix_end:
  if (addr.pgptr != NULL && oid != NULL)
    {
      best.vpid.volid = oid->volid;
      best.vpid.pageid = oid->pageid;

      best.freespace = spage_get_free_space_without_saving (thread_p,
							    addr.pgptr);

      (void) heap_bestspace_add (thread_p, hfid, &best.vpid, best.freespace);
    }

/*
 * Cache the page for any future scan modifications
 */
  if (scan_cache != NULL && scan_cache->cache_last_fix_page == true
      && ishome_insert == true)
    {
      scan_cache->pgptr = addr.pgptr;
    }
  else				/* unfix the page */
    {
      pgbuf_unfix_and_init (thread_p, addr.pgptr);
    }

  return (oid != NULL) ? NO_ERROR : ER_FAILED;
}

/*
 * heap_insert () - Insert an object onto heap
 *   return: OID *(oid on success or NULL on failure)
 *   hfid(in): Object heap file identifier
 *   class_oid(in):
 *   oid(out): : Object identifier.
 *   recdes(in): recdes: Record descriptor
 *   scan_cache(in/out): Scan cache used to estimate the best space pages
 *                       between heap changes.
 *
 * Note: Insert an object onto the given file heap. The object is
 * inserted using the following algorithm:
 *              1: If the object cannot be inserted in a single page, it is
 *                 inserted in overflow as a multipage object. An overflow
 *                 relocation record is created in the heap as an address map
 *                 to the actual content of the object (the overflow address).
 *              2: If the object can be inserted in the last allocated page
 *                 without overpassing the reserved space on the page, the
 *                 object is placed on this page.
 *              3: If the object can be inserted in the hinted page without
 *                 overpassing the reserved space on the page, the object is
 *       	   placed on this page.
 *              4: The object is inserted in a newly allocated page. Don't
 *                 about reserve space here.
 */
OID *
heap_insert (THREAD_ENTRY * thread_p, const HFID * hfid, OID * class_oid,
	     OID * oid, RECDES * recdes, HEAP_SCANCACHE * scan_cache)
{
  /*
   * If a scan cache for updates is given, make sure that it is for the
   * same heap, otherwise, end the current one and start a new one.
   */
  if (scan_cache != NULL)
    {
      if (scan_cache->debug_initpattern != HEAP_DEBUG_SCANCACHE_INITPATTERN)
	{
	  er_log_debug (ARG_FILE_LINE, "heap_insert: Your scancache is not"
			" initialized");
	  scan_cache = NULL;
	}
      else
	{
	  if (!HFID_EQ (&scan_cache->hfid, hfid)
	      || OID_ISNULL (&scan_cache->class_oid))
	    {
	      if (heap_scancache_reset_modify (thread_p, scan_cache, hfid,
					       class_oid) != NO_ERROR)
		{
		  return NULL;
		}
	    }
	}
    }

  if (heap_is_big_length (recdes->length))
    {
      /* This is a multipage object. It must be stored in overflow */
      OID ovf_oid;
      RECDES map_recdes = RECDES_INITIALIZER;

      if (heap_ovf_insert (thread_p, hfid, &ovf_oid, recdes,
			   class_oid) == NULL)
	{
	  return NULL;
	}

      /* Add a map record to point to the record in overflow */
      map_recdes.type = REC_BIGONE;
      map_recdes.length = sizeof (ovf_oid);
      map_recdes.area_size = sizeof (ovf_oid);
      map_recdes.data = (char *) &ovf_oid;

      if (heap_insert_with_lock_internal (thread_p, hfid, oid, class_oid,
					  &map_recdes, scan_cache, true,
					  recdes->length) != NO_ERROR)
	{
	  /* Something went wrong, delete the overflow record */
	  (void) heap_ovf_delete (thread_p, hfid, &ovf_oid);
	  return NULL;
	}
    }
  else
    {
      recdes->type = REC_HOME;
      if (heap_insert_with_lock_internal (thread_p, hfid, oid, class_oid,
					  recdes, scan_cache, true,
					  recdes->length) != NO_ERROR)
	{
	  return NULL;
	}
    }

  if (heap_Classrepr->rootclass_hfid != NULL
      && HFID_EQ ((hfid), heap_Classrepr->rootclass_hfid))
    {

      if (log_add_to_modified_class_list (thread_p, oid) != NO_ERROR)
	{
	  return NULL;
	}
    }

  /* set shard groupid
   */
  if (recdes->type == REC_ASSIGN_ADDRESS)
    {
      oid->groupid = NULL_GROUPID;	/* clear */
    }
  else
    {
      oid->groupid = or_grp_id (recdes);
      assert (oid->groupid >= GLOBAL_GROUPID);
      assert (SHARD_GROUP_OWN (thread_p, oid->groupid));

#if !defined(NDEBUG)
      if (heap_classrepr_is_shard_table (thread_p, class_oid) == true)
	{
	  assert (oid->groupid > GLOBAL_GROUPID);	/* is shard table */
	}
      else
	{
	  assert (oid->groupid == GLOBAL_GROUPID);	/* is global table */
	}
#endif
    }

  return oid;
}

/*
 * heap_update () - Update an object
 *   return: OID *(oid on success or NULL on failure)
 *   hfid(in): Heap file identifier
 *   class_oid(in):
 *   oid(in/out): Object identifier
 *   recdes(in): Record descriptor
 *   old(in/out): Flag. Set to true, if content of object has been stored
 *                it is set to false (i.e., only the address was stored)
 *   scan_cache(in/out): Scan cache used to estimate the best space pages
 *                       between heap changes.
 *
 */
const OID *
heap_update (THREAD_ENTRY * thread_p, const HFID * hfid,
	     const OID * class_oid, OID * oid, RECDES * recdes,
	     bool * old, HEAP_SCANCACHE * scan_cache)
{
  VPID vpid;			/* Volume and page identifiers */
  VPID *vpidptr_incache;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;	/* Address of logging data     */
  LOG_DATA_ADDR forward_addr = LOG_ADDR_INITIALIZER;	/* Address of forward data     */
  PAGE_PTR hdr_pgptr = NULL;	/* Page pointer to header page */
  INT16 type;
  OID new_forward_oid;
  RECDES new_forward_recdes = RECDES_INITIALIZER;
  OID forward_oid;
  RECDES forward_recdes = RECDES_INITIALIZER;
  int sp_success;
  DISK_ISVALID oid_valid;

  int again_count = 0;
  int again_max = 20;
  VPID home_vpid;
  VPID newhome_vpid;

  assert (class_oid != NULL && !OID_ISNULL (class_oid));
  assert (file_is_new_file (thread_p, &hfid->vfid) == FILE_OLD_FILE);

  if (heap_Classrepr != NULL && hfid != NULL)
    {
      if (heap_Classrepr->rootclass_hfid == NULL)
	{
	  heap_Classrepr->rootclass_hfid = boot_find_root_heap ();
	}
      assert (HFID_EQ ((hfid), heap_Classrepr->rootclass_hfid) == false
	      || heap_classrepr_has_lock (thread_p, oid) == true);
    }

  if (hfid == NULL)
    {
      if (scan_cache != NULL)
	{
	  hfid = &scan_cache->hfid;
	}
      else
	{
	  er_log_debug (ARG_FILE_LINE,
			"heap_update: Bad interface a heap is needed");
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_HEAP, 3,
		  "", NULL_FILEID, NULL_PAGEID);
	  return NULL;
	}
    }

  /* check/set shard groupid
   */
  assert (oid->groupid == or_grp_id (recdes));
  assert (SHARD_GROUP_OWN (thread_p, oid->groupid));
  /* defense code */
  if (oid->groupid != or_grp_id (recdes))
    {
      assert (false);
      oid->groupid = or_grp_id (recdes);
    }

  assert (oid->groupid >= GLOBAL_GROUPID);

  oid_valid = HEAP_ISVALID_OID (oid);
  if (oid_valid != DISK_VALID)
    {
      if (oid_valid != DISK_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT, 3,
		  oid->volid, oid->pageid, oid->slotid);
	}
      return NULL;
    }

  *old = true;

try_again:

  addr.vfid = &hfid->vfid;
  forward_addr.vfid = &hfid->vfid;

  addr.pgptr = NULL;
  forward_addr.pgptr = NULL;
  hdr_pgptr = NULL;

  /*
   * Lock and fetch the page where the object is stored.
   */

  vpid.volid = oid->volid;
  vpid.pageid = oid->pageid;

  home_vpid.volid = oid->volid;
  home_vpid.pageid = oid->pageid;

  /*
   * If a scan cache for updates is given, make sure that it is for the
   * same heap, otherwise, end the current scan cache and start a new one.
   */

  if (scan_cache != NULL)
    {
      if (scan_cache->debug_initpattern != HEAP_DEBUG_SCANCACHE_INITPATTERN)
	{
	  er_log_debug (ARG_FILE_LINE, "heap_update: Your scancache is not"
			" initialized");
	  scan_cache = NULL;
	}
      else
	{
	  if (!HFID_EQ (&scan_cache->hfid, hfid)
	      || OID_ISNULL (&scan_cache->class_oid))
	    {
	      if (heap_scancache_reset_modify (thread_p, scan_cache, hfid,
					       class_oid) != NO_ERROR)
		{
		  goto error;
		}
	    }
	}

      /*
       * If the home page of object (OID) is the same as the cached page,
       * we do not need to fetch the page, already in the cache.
       */
      if (scan_cache != NULL
	  && scan_cache->cache_last_fix_page == true
	  && scan_cache->pgptr != NULL)
	{
	  vpidptr_incache = pgbuf_get_vpid_ptr (scan_cache->pgptr);
	  if (VPID_EQ (&vpid, vpidptr_incache))
	    {
	      /* We can skip the fetch operation */
	      addr.pgptr = scan_cache->pgptr;
	    }
	  else
	    {
	      /*
	       * Free the cached page
	       */
	      pgbuf_unfix_and_init (thread_p, scan_cache->pgptr);
	    }
	  /*
	   * Now remove the page from the scan cache. At the end this page or
	   * another one will be cached again.
	   */
	  scan_cache->pgptr = NULL;
	}
    }

  /*
   * If we do not have the home page already fetched, fetch it at this moment
   */

  if (addr.pgptr == NULL)
    {
      addr.pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid,
						X_LOCK, scan_cache,
						PAGE_HEAP);
      if (addr.pgptr == NULL)
	{
	  if (er_errid () == ER_PB_BAD_PAGEID)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid, oid->pageid,
		      oid->slotid);
	    }

	  /* something went wrong, return */
	  goto error;
	}
    }

  type = spage_get_record_type (addr.pgptr, oid->slotid);
  if (type == REC_UNKNOWN)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT, 3,
	      oid->volid, oid->pageid, oid->slotid);
      goto error;
    }

  assert (type != REC_UNKNOWN);
  recdes->type = type;

  switch (type)
    {
    case REC_RELOCATION:
      forward_recdes.data = (char *) &forward_oid;
      forward_recdes.length = sizeof (forward_oid);
      forward_recdes.area_size = sizeof (forward_oid);

      if (spage_get_record (addr.pgptr, oid->slotid, &forward_recdes, COPY)
	  != S_SUCCESS)
	{
	  /* Unable to get relocation record of the object */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HEAP_BAD_RELOCATION_RECORD, 3, oid->volid, oid->pageid,
		  oid->slotid);
	  goto error;
	}

      /* Lock and fetch the page of new home (relocated/forwarded) record */
      vpid.volid = forward_oid.volid;
      vpid.pageid = forward_oid.pageid;

      newhome_vpid.volid = forward_oid.volid;
      newhome_vpid.pageid = forward_oid.pageid;

      /*
       * To avoid a possible deadlock, make sure that you do not wait on a
       * single lock. If we need to wait, release locks and request them in
       * one operation. In case of failure, the already released locks have
       * been freed.
       *
       * Note: that we have not peeked, so we do not need to fix anything at
       *       this moment.
       */

      forward_addr.pgptr = heap_pgbuf_fix (thread_p, hfid, &vpid,
					   PGBUF_LATCH_WRITE,
					   PGBUF_CONDITIONAL_LATCH,
					   PAGE_HEAP);
      if (forward_addr.pgptr == NULL)
	{
	  pgbuf_unfix_and_init (thread_p, addr.pgptr);

	  forward_addr.pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid,
							    X_LOCK,
							    scan_cache,
							    PAGE_HEAP);
	  if (forward_addr.pgptr == NULL)
	    {
	      if (er_errid () == ER_PB_BAD_PAGEID)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HEAP_UNKNOWN_OBJECT, 3, forward_oid.volid,
			  forward_oid.pageid, forward_oid.slotid);
		}

	      goto error;
	    }
	  addr.pgptr = heap_pgbuf_fix (thread_p, hfid, &home_vpid,
				       PGBUF_LATCH_WRITE,
				       PGBUF_CONDITIONAL_LATCH, PAGE_HEAP);
	  if (addr.pgptr == NULL)
	    {
	      pgbuf_unfix_and_init (thread_p, forward_addr.pgptr);

	      if (again_count++ >= again_max)
		{
		  if (er_errid () == ER_PB_BAD_PAGEID)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid,
			      oid->pageid, oid->slotid);
		    }
		  else if (er_errid () == NO_ERROR)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_PAGE_LATCH_ABORTED, 2, home_vpid.volid,
			      home_vpid.pageid);
		    }

		  goto error;
		}
	      else
		{
		  goto try_again;
		}
	    }
	}

#if defined(RYE_DEBUG)
      if (spage_get_record_type (forward_addr.pgptr, forward_oid.slotid) !=
	  REC_NEWHOME)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_BAD_OBJECT_TYPE,
		  3, forward_oid.volid, forward_oid.pageid,
		  forward_oid.slotid);
	  goto error;
	}
#endif

      /*
       * Can we move the object back to its home (OID) page ?
       */
      if (heap_is_big_length (recdes->length)
	  || spage_update (thread_p, addr.pgptr, oid->slotid,
			   recdes) != SP_SUCCESS)
	{
	  /*
	   * CANNOT BE RETURNED TO ITS HOME PAGE (OID PAGE)
	   * Try to update the object at relocated home page (content page)
	   */
	  if (heap_is_big_length (recdes->length)
	      || spage_is_updatable (thread_p, forward_addr.pgptr,
				     forward_oid.slotid, recdes) == false)
	    {

	      /* Header of heap */
	      vpid.volid = hfid->vfid.volid;
	      vpid.pageid = hfid->hpgid;

	      hdr_pgptr = heap_pgbuf_fix (thread_p, hfid, &vpid,
					  PGBUF_LATCH_WRITE,
					  PGBUF_CONDITIONAL_LATCH,
					  PAGE_HEAP_HEADER);
	      if (hdr_pgptr == NULL)
		{
		  pgbuf_unfix_and_init (thread_p, addr.pgptr);
		  pgbuf_unfix_and_init (thread_p, forward_addr.pgptr);

		  hdr_pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid,
							   X_LOCK,
							   scan_cache,
							   PAGE_HEAP_HEADER);
		  if (hdr_pgptr == NULL)
		    {
		      goto error;
		    }

		  addr.pgptr = heap_pgbuf_fix (thread_p, hfid, &home_vpid,
					       PGBUF_LATCH_WRITE,
					       PGBUF_CONDITIONAL_LATCH,
					       PAGE_HEAP);
		  if (addr.pgptr == NULL)
		    {
		      pgbuf_unfix_and_init (thread_p, hdr_pgptr);

		      if (again_count++ >= again_max)
			{
			  if (er_errid () == ER_PB_BAD_PAGEID)
			    {
			      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid,
				      oid->pageid, oid->slotid);
			    }
			  else if (er_errid () == NO_ERROR)
			    {
			      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				      ER_PAGE_LATCH_ABORTED, 2,
				      home_vpid.volid, home_vpid.pageid);
			    }
			  goto error;
			}
		      else
			{
			  goto try_again;
			}
		    }
		  forward_addr.pgptr =
		    heap_pgbuf_fix (thread_p, hfid, &newhome_vpid,
				    PGBUF_LATCH_WRITE,
				    PGBUF_CONDITIONAL_LATCH, PAGE_HEAP);
		  if (forward_addr.pgptr == NULL)
		    {
		      pgbuf_unfix_and_init (thread_p, hdr_pgptr);
		      pgbuf_unfix_and_init (thread_p, addr.pgptr);

		      if (again_count++ >= again_max)
			{
			  if (er_errid () == ER_PB_BAD_PAGEID)
			    {
			      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				      ER_HEAP_UNKNOWN_OBJECT, 3,
				      forward_oid.volid, forward_oid.pageid,
				      forward_oid.slotid);
			    }
			  else if (er_errid () == NO_ERROR)
			    {
			      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				      ER_PAGE_LATCH_ABORTED, 2,
				      newhome_vpid.volid,
				      newhome_vpid.pageid);
			    }

			  goto error;
			}
		      else
			{
			  goto try_again;
			}
		    }
		}

	      if (heap_is_big_length (recdes->length))
		{
		  /*
		   * The object has increased in length, it is now a multipage
		   * object. It MUST BE STORED IN OVERFLOW
		   */
		  if (heap_ovf_insert (thread_p, hfid, &new_forward_oid,
				       recdes, class_oid) == NULL)
		    {
		      goto error;
		    }

		  new_forward_recdes.type = REC_BIGONE;
		}
	      else
		{
		  /*
		   * FIND A NEW HOME PAGE for the object
		   */
		  recdes->type = REC_NEWHOME;
		  if (heap_insert_internal (thread_p, hfid, &new_forward_oid,
					    recdes, scan_cache, false,
					    (recdes->length -
					     spage_get_record_length
					     (forward_addr.pgptr,
					      forward_oid.slotid)),
					    class_oid) != NO_ERROR)
		    {
		      /*
		       * Problems finding a new home. Return without any updates
		       */
		      goto error;
		    }
		  new_forward_recdes.type = REC_RELOCATION;
		}

	      /*
	       * Original record (i.e., at home) must point to new relocated
	       * content (either on overflow or on another heap page).
	       */

	      new_forward_recdes.data = (char *) &new_forward_oid;
	      new_forward_recdes.length = sizeof (new_forward_oid);
	      new_forward_recdes.area_size = sizeof (new_forward_oid);

	      sp_success = spage_update (thread_p, addr.pgptr, oid->slotid,
					 &new_forward_recdes);
	      if (sp_success != SP_SUCCESS)
		{
		  /*
		   * This is likely a system error since the length of forward
		   * records are smaller than any other record. Don't do anything
		   * undo the operation
		   */
		  if (sp_success != SP_ERROR)
		    {
		      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_GENERIC_ERROR, 1, "");
		    }
#if defined(RYE_DEBUG)
		  er_log_debug (ARG_FILE_LINE,
				"heap_update: ** SYSTEM ERROR ** the"
				" length of relocation records is the smallest"
				" allowed.. slotted update could not fail ...\n");
#endif
		  if (new_forward_recdes.type == REC_BIGONE)
		    {
		      (void) heap_ovf_delete (thread_p, hfid,
					      &new_forward_oid);
		    }
		  else
		    {
		      (void) heap_delete_internal (thread_p, hfid,
						   &new_forward_oid,
						   scan_cache, false, NULL);
		    }
		  goto error;
		}

	      if (new_forward_recdes.type == REC_BIGONE)
		{
		  spage_update_record_type (thread_p, addr.pgptr, oid->slotid,
					    REC_BIGONE);
		}

	      /* Log the changes and then set the page dirty */
	      addr.offset = oid->slotid;
	      log_append_undoredo_recdes (thread_p, RVHF_UPDATE, &addr,
					  &forward_recdes,
					  &new_forward_recdes, NULL);

	      /* Delete the old new home (i.e., relocated record) */
	      (void) heap_delete_internal (thread_p, hfid, &forward_oid,
					   scan_cache, false, NULL);
	      pgbuf_set_dirty (thread_p, forward_addr.pgptr, FREE);
	      forward_addr.pgptr = NULL;
	      pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);
	      pgbuf_unfix_and_init (thread_p, hdr_pgptr);
	    }
	  else
	    {
	      /*
	       * OBJECT CAN BE UPDATED AT RELOCATED HOME PAGE (content page).
	       * We do not need to change relocation record (OID home record).
	       *
	       * We log first, to avoid copying the before image (old) object.
	       * This operation is correct since we already know that there is
	       * space to insert the object
	       */

	      if (spage_get_record (forward_addr.pgptr, forward_oid.slotid,
				    &forward_recdes, PEEK) != S_SUCCESS)
		{
		  /* Unable to keep forward imagen of object for logging */
		  goto error;
		}

	      recdes->type = REC_NEWHOME;
	      forward_addr.offset = forward_oid.slotid;
	      log_append_undoredo_recdes (thread_p, RVHF_UPDATE,
					  &forward_addr, &forward_recdes,
					  recdes, class_oid);
	      sp_success =
		spage_update (thread_p, forward_addr.pgptr,
			      forward_oid.slotid, recdes);
	      if (sp_success != SP_SUCCESS)
		{
		  /*
		   * This is likely a system error since we have already checked
		   * for space. The page is lock in exclusive mode... How did it
		   * happen?
		   */
		  if (sp_success != SP_ERROR)
		    {
		      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_GENERIC_ERROR, 1, "");
		    }
#if defined(RYE_DEBUG)
		  er_log_debug (ARG_FILE_LINE,
				"heap_update: ** SYSTEM_ERROR ** update"
				" operation failed even when have already checked"
				" for space");
#endif
		  goto error;
		}
	      pgbuf_set_dirty (thread_p, forward_addr.pgptr, FREE);
	      forward_addr.pgptr = NULL;
	    }
	}
      else
	{
	  /*
	   * The object was returned to its home (OID) page.
	   * Remove the old relocated record (old new home)
	   */

	  /* Indicate that this is home record */
	  recdes->type = REC_HOME;
	  spage_update_record_type (thread_p, addr.pgptr, oid->slotid,
				    recdes->type);

	  addr.offset = oid->slotid;
	  log_append_undoredo_recdes (thread_p, RVHF_UPDATE, &addr,
				      &forward_recdes, recdes, class_oid);

	  /* Delete the relocated record (old home) */
	  (void) heap_delete_internal (thread_p, hfid, &forward_oid,
				       scan_cache, false, NULL);
	  pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);
	  pgbuf_set_dirty (thread_p, forward_addr.pgptr, FREE);
	  forward_addr.pgptr = NULL;
	}
      break;

    case REC_BIGONE:
      /*
       * The object stored in the heap page is a relocation_overflow record,
       * get the overflow address of the object
       */
      forward_recdes.data = (char *) &forward_oid;
      forward_recdes.length = sizeof (forward_oid);
      forward_recdes.area_size = sizeof (forward_oid);

      if (spage_get_record (addr.pgptr, oid->slotid, &forward_recdes,
			    COPY) != S_SUCCESS)
	{
	  /* Unable to peek overflow address of multipage object */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HEAP_BAD_RELOCATION_RECORD, 3, oid->volid, oid->pageid,
		  oid->slotid);
	  goto error;
	}


      /* Header of heap */
      vpid.volid = hfid->vfid.volid;
      vpid.pageid = hfid->hpgid;

      hdr_pgptr = heap_pgbuf_fix (thread_p, hfid, &vpid,
				  PGBUF_LATCH_WRITE, PGBUF_CONDITIONAL_LATCH,
				  PAGE_HEAP_HEADER);
      if (hdr_pgptr == NULL)
	{
	  pgbuf_unfix_and_init (thread_p, addr.pgptr);

	  hdr_pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid,
						   X_LOCK, scan_cache,
						   PAGE_HEAP_HEADER);
	  if (hdr_pgptr == NULL)
	    {
	      goto error;
	    }

	  addr.pgptr = heap_pgbuf_fix (thread_p, hfid, &home_vpid,
				       PGBUF_LATCH_WRITE,
				       PGBUF_CONDITIONAL_LATCH, PAGE_HEAP);
	  if (addr.pgptr == NULL)
	    {
	      pgbuf_unfix_and_init (thread_p, hdr_pgptr);

	      if (again_count++ >= again_max)
		{
		  if (er_errid () == ER_PB_BAD_PAGEID)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid,
			      oid->pageid, oid->slotid);
		    }
		  else if (er_errid () == NO_ERROR)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_PAGE_LATCH_ABORTED, 2, home_vpid.volid,
			      home_vpid.pageid);
		    }
		  goto error;
		}
	      else
		{
		  goto try_again;
		}
	    }
	}

      /* Is the object still a multipage one ? */
      if (heap_is_big_length (recdes->length))
	{
	  /* Update object in overflow */
	  if (heap_ovf_update (thread_p, hfid, &forward_oid, recdes) == NULL)
	    {
	      goto error;
	    }
	}
      else
	{
	  /*
	   * The object is not a multipage object any longer. Store the object
	   * in the normal heap file
	   */

	  /* Can we return the object to its home ? */

	  if (spage_update (thread_p, addr.pgptr, oid->slotid, recdes) !=
	      SP_SUCCESS)
	    {
	      /*
	       * The object cannot be returned to its home page, relocate the
	       * object
	       */
	      recdes->type = REC_NEWHOME;
	      if (heap_insert_internal (thread_p, hfid, &new_forward_oid,
					recdes, scan_cache, false,
					(heap_ovf_get_length
					 (thread_p,
					  &forward_oid) - recdes->length),
					class_oid) != NO_ERROR)
		{
		  /* Problems finding a new home. Return without any modifications */
		  goto error;
		}

	      /*
	       * Update the OID relocation record to points to new home
	       */

	      new_forward_recdes.type = REC_RELOCATION;
	      new_forward_recdes.data = (char *) &new_forward_oid;
	      new_forward_recdes.length = sizeof (new_forward_oid);
	      new_forward_recdes.area_size = sizeof (new_forward_oid);

	      sp_success = spage_update (thread_p, addr.pgptr, oid->slotid,
					 &new_forward_recdes);
	      if (sp_success != SP_SUCCESS)
		{
		  /*
		   * This is likely a system error since the length of forward
		   * records are smaller than any other record. Don't do anything
		   * undo the operation
		   */
		  if (sp_success != SP_ERROR)
		    {
		      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_GENERIC_ERROR, 1, "");
		    }
#if defined(RYE_DEBUG)
		  er_log_debug (ARG_FILE_LINE,
				"heap_update: ** SYSTEM ERROR ** the"
				" length of relocation records is the smallest"
				" allowed.. slotted update could not fail ...\n");
#endif
		  (void) heap_delete_internal (thread_p, hfid,
					       &new_forward_oid, scan_cache,
					       false, NULL);
		  goto error;
		}
	      spage_update_record_type (thread_p, addr.pgptr, oid->slotid,
					new_forward_recdes.type);
	      addr.offset = oid->slotid;
	      log_append_undoredo_recdes (thread_p, RVHF_UPDATE, &addr,
					  &forward_recdes,
					  &new_forward_recdes, NULL);
	    }
	  else
	    {
	      /*
	       * The record has been returned to its home
	       */
	      recdes->type = REC_HOME;
	      spage_update_record_type (thread_p, addr.pgptr, oid->slotid,
					recdes->type);
	      addr.offset = oid->slotid;
	      log_append_undoredo_recdes (thread_p, RVHF_UPDATE, &addr,
					  &forward_recdes, recdes, class_oid);
	    }

	  (void) heap_ovf_delete (thread_p, hfid, &forward_oid);
	  pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);
	}

      pgbuf_unfix_and_init (thread_p, hdr_pgptr);
      break;

    case REC_ASSIGN_ADDRESS:
      /* This is a new object since only the address has been assigned to it */

      *old = false;
      /* Fall thru REC_HOME */

    case REC_HOME:
      /* Does object still fit at home address (OID page) ? */

      if (heap_is_big_length (recdes->length)
	  || spage_is_updatable (thread_p, addr.pgptr, oid->slotid,
				 recdes) == false)
	{
	  /*
	   * DOES NOT FIT ON HOME PAGE (OID page) ANY LONGER,
	   * a new home must be found.
	   */

	  /* Header of heap */
	  vpid.volid = hfid->vfid.volid;
	  vpid.pageid = hfid->hpgid;

	  hdr_pgptr = heap_pgbuf_fix (thread_p, hfid, &vpid,
				      PGBUF_LATCH_WRITE,
				      PGBUF_CONDITIONAL_LATCH,
				      PAGE_HEAP_HEADER);
	  if (hdr_pgptr == NULL)
	    {
	      pgbuf_unfix_and_init (thread_p, addr.pgptr);

	      hdr_pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid,
						       X_LOCK,
						       scan_cache,
						       PAGE_HEAP_HEADER);
	      if (hdr_pgptr == NULL)
		{
		  goto error;
		}

	      addr.pgptr =
		heap_pgbuf_fix (thread_p, hfid, &home_vpid,
				PGBUF_LATCH_WRITE, PGBUF_CONDITIONAL_LATCH,
				PAGE_HEAP);
	      if (addr.pgptr == NULL)
		{
		  pgbuf_unfix_and_init (thread_p, hdr_pgptr);

		  if (again_count++ >= again_max)
		    {
		      if (er_errid () == ER_PB_BAD_PAGEID)
			{
			  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				  ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid,
				  oid->pageid, oid->slotid);
			}
		      else if (er_errid () == NO_ERROR)
			{
			  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				  ER_PAGE_LATCH_ABORTED, 2, home_vpid.volid,
				  home_vpid.pageid);
			}
		      goto error;
		    }
		  else
		    {
		      goto try_again;
		    }
		}
	    }

	  if (heap_is_big_length (recdes->length))
	    {
	      /*
	       * Object has became a multipage one.
	       * It must be stored in overflow
	       */
	      if (heap_ovf_insert (thread_p, hfid, &new_forward_oid, recdes,
				   class_oid) == NULL)
		{
		  goto error;
		}

	      new_forward_recdes.type = REC_BIGONE;
	    }
	  else
	    {
	      /*
	       * Relocate the object. Find a new home
	       */
	      int len;

	      len = recdes->length - spage_get_record_length (addr.pgptr,
							      oid->slotid);
	      recdes->type = REC_NEWHOME;
	      if (heap_insert_internal (thread_p, hfid, &new_forward_oid,
					recdes, scan_cache, false,
					len, class_oid) != NO_ERROR)
		{
		  /* Problems finding a new home. Return without any updates */
		  goto error;
		}
	      new_forward_recdes.type = REC_RELOCATION;
	    }

	  /*
	   * Original record (i.e., at home) must point to new overflow address
	   * or relocation address.
	   */

	  new_forward_recdes.data = (char *) &new_forward_oid;
	  new_forward_recdes.length = sizeof (new_forward_oid);
	  new_forward_recdes.area_size = sizeof (new_forward_oid);

	  /*
	   * We log first, to avoid copying the before image (old) object,
	   * instead we use the one that was peeked. This operation is fine
	   * since relocation records are the smallest record, thus they can
	   * always replace any object
	   */

	  /*
	   * Peek for the original content of the object. It is peeked using the
	   * forward recdes.
	   */
	  if (spage_get_record (addr.pgptr, oid->slotid, &forward_recdes,
				PEEK) != S_SUCCESS)
	    {
	      /* Unable to peek before image for logging purposes */
	      if (new_forward_recdes.type == REC_BIGONE)
		{
		  (void) heap_ovf_delete (thread_p, hfid, &new_forward_oid);
		}
	      else
		{
		  (void) heap_delete_internal (thread_p, hfid,
					       &new_forward_oid, scan_cache,
					       false, NULL);
		}
	      goto error;
	    }

	  addr.offset = oid->slotid;
	  log_append_undoredo_recdes (thread_p, RVHF_UPDATE, &addr,
				      &forward_recdes, &new_forward_recdes,
				      NULL);

	  sp_success = spage_update (thread_p, addr.pgptr, oid->slotid,
				     &new_forward_recdes);
	  if (sp_success != SP_SUCCESS)
	    {
	      /*
	       * This is likely a system error since the length of forward
	       * records are smaller than any other record. Don't do anything
	       * undo the operation
	       */
	      if (sp_success != SP_ERROR)
		{
		  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_GENERIC_ERROR, 1, "");
		}
#if defined(RYE_DEBUG)
	      er_log_debug (ARG_FILE_LINE,
			    "heap_update: ** SYSTEM ERROR ** the"
			    " length of relocation records is the smallest"
			    " allowed.. slotted update could not fail ...\n");
#endif
	      if (new_forward_recdes.type == REC_BIGONE)
		{
		  (void) heap_ovf_delete (thread_p, hfid, &new_forward_oid);
		}
	      else
		{
		  (void) heap_delete_internal (thread_p, hfid,
					       &new_forward_oid, scan_cache,
					       false, NULL);
		}
	      goto error;
	    }

	  spage_update_record_type (thread_p, addr.pgptr, oid->slotid,
				    new_forward_recdes.type);
	  pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);
	  pgbuf_unfix_and_init (thread_p, hdr_pgptr);
	}
      else
	{
	  /*
	   * The object can be UPDATED AT THE SAME HOME PAGE (OID PAGE)
	   *
	   * We log first, to avoid copying the before image (old) object,
	   * instead we use the one that was peeked. This operation is fine
	   * since we already know that there is space to update
	   */

	  if (spage_get_record (addr.pgptr, oid->slotid, &forward_recdes,
				PEEK) != S_SUCCESS)
	    {
	      /* Unable to peek before image for logging purposes */
	      goto error;
	    }

	  /* For the logging */
	  addr.offset = oid->slotid;
	  recdes->type = REC_HOME;

	  log_append_undoredo_recdes (thread_p, RVHF_UPDATE, &addr,
				      &forward_recdes, recdes, class_oid);

	  sp_success = spage_update (thread_p, addr.pgptr, oid->slotid,
				     recdes);
	  if (sp_success != SP_SUCCESS)
	    {
	      /*
	       * This is likely a system error since we have already checked
	       * for space
	       */
	      if (sp_success != SP_ERROR)
		{
		  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_GENERIC_ERROR, 1, "");
		}
#if defined(RYE_DEBUG)
	      er_log_debug (ARG_FILE_LINE,
			    "heap_update: ** SYSTEM_ERROR ** update"
			    " operation failed even when have already checked"
			    " for space");
#endif
	      goto error;
	    }
	  spage_update_record_type (thread_p, addr.pgptr, oid->slotid,
				    recdes->type);
	  pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);
	}
      break;

    case REC_NEWHOME:
    case REC_MARKDELETED:
    case REC_DELETED_WILL_REUSE:
    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_BAD_OBJECT_TYPE, 3,
	      oid->volid, oid->pageid, oid->slotid);
      goto error;
    }

  /*
   * Cache the page for any future scan modifications
   */
  if (scan_cache != NULL && scan_cache->cache_last_fix_page == true)
    {
      scan_cache->pgptr = addr.pgptr;
    }
  else
    {
      pgbuf_unfix_and_init (thread_p, addr.pgptr);
    }

  if (heap_Classrepr != NULL
      && heap_Classrepr->rootclass_hfid != NULL
      && HFID_EQ ((hfid), heap_Classrepr->rootclass_hfid))
    {
      if (log_add_to_modified_class_list (thread_p, oid) != NO_ERROR)
	{
	  goto error;
	}
    }

  return oid;

error:
  if (addr.pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, addr.pgptr);
    }
  if (forward_addr.pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, forward_addr.pgptr);
    }
  if (hdr_pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, hdr_pgptr);
    }

  return NULL;
}

/*
 * heap_delete () - Delete an object from heap file
 *   return: OID *(oid on success or NULL on failure)
 *   hfid(in): Heap file identifier
 *   oid(in): Object identifier
 *   scan_cache(in/out): Scan cache used to estimate the best space pages
 *                       between heap changes.
 *   class_oid(in): class oid for migrator
 *
 * Note: Delete the object associated with the given OID from the given
 * heap file. If the object has been relocated or stored in
 * overflow, both the relocation and the relocated record are deleted.
 */
const OID *
heap_delete (THREAD_ENTRY * thread_p, const HFID * hfid, const OID * oid,
	     HEAP_SCANCACHE * scan_cache, OID * class_oid)
{
  int ret = NO_ERROR;

  assert (file_is_new_file (thread_p, &hfid->vfid) == FILE_OLD_FILE);

  /*
   * If a scan cache for updates is given, make sure that it is for the
   * same heap, otherwise, end the current one and start a new one.
   */
  if (scan_cache != NULL)
    {
      if (scan_cache->debug_initpattern != HEAP_DEBUG_SCANCACHE_INITPATTERN)
	{
	  er_log_debug (ARG_FILE_LINE, "heap_delete: Your scancache is not"
			" initialized");
	  scan_cache = NULL;
	}
      else
	{
	  if (!HFID_EQ (&scan_cache->hfid, hfid))
	    {
	      /*
	       * A different heap is used, recache the hash
	       */
	      ret =
		heap_scancache_reset_modify (thread_p, scan_cache, hfid,
					     NULL);
	      if (ret != NO_ERROR)
		{
		  return NULL;
		}
	    }
	}
    }
  if (heap_Classrepr->rootclass_hfid != NULL
      && HFID_EQ ((hfid), heap_Classrepr->rootclass_hfid))
    {

      if (log_add_to_modified_class_list (thread_p, oid) != NO_ERROR)
	{
	  return NULL;
	}
    }

  return heap_delete_internal (thread_p, hfid, oid, scan_cache, true,
			       class_oid);
}

/*
 * heap_delete_internal () - Delete an object from heap file
 *   return: OID *(oid on success or NULL on failure)
 *   hfid(in): Heap file identifier
 *   oid(in): Object identifier
 *   scan_cache(in):
 *   ishome_delete(in):
 *   class_oid(in): class oid for migrator
 *
 * Note: Delete the object associated with the given OID from the given
 * heap file. If the object has been relocated or stored in
 * overflow, both the relocation and the relocated record are deleted.
 */
static const OID *
heap_delete_internal (THREAD_ENTRY * thread_p, const HFID * hfid,
		      const OID * oid, HEAP_SCANCACHE * scan_cache,
		      bool ishome_delete, OID * class_oid)
{
  VPID vpid;			/* Volume and page identifiers */
  VPID *vpidptr_incache;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;	/* Address of logging data     */
  LOG_DATA_ADDR forward_addr = LOG_ADDR_INITIALIZER;
  PAGE_PTR hdr_pgptr = NULL;
  INT16 type;
  int prev_freespace;
  OID forward_oid;
  RECDES forward_recdes = RECDES_INITIALIZER;
  RECDES undo_recdes = RECDES_INITIALIZER;
  DISK_ISVALID oid_valid;
  int again_count = 0;
  int again_max = 20;
  VPID home_vpid;

  oid_valid = HEAP_ISVALID_OID (oid);
  if (oid_valid != DISK_VALID)
    {
      if (oid_valid != DISK_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT, 3,
		  oid->volid, oid->pageid, oid->slotid);
	}
      return NULL;
    }

  if (scan_cache != NULL)
    {
      assert (HFID_EQ (hfid, &scan_cache->hfid));
    }

  assert (file_get_type (thread_p, &hfid->vfid) == FILE_HEAP
	  || er_errid () == ER_INTERRUPTED);

try_again:

  addr.vfid = &hfid->vfid;
  forward_addr.vfid = &hfid->vfid;

  addr.pgptr = NULL;
  forward_addr.pgptr = NULL;
  hdr_pgptr = NULL;

  /*
   * Lock and fetch the page where the object is stored.
   */

  vpid.volid = oid->volid;
  vpid.pageid = oid->pageid;

  home_vpid.volid = oid->volid;
  home_vpid.pageid = oid->pageid;

  if (scan_cache != NULL)
    {
      if (scan_cache != NULL && scan_cache->cache_last_fix_page == true
	  && scan_cache->pgptr != NULL)
	{
	  vpidptr_incache = pgbuf_get_vpid_ptr (scan_cache->pgptr);
	  if (VPID_EQ (&vpid, vpidptr_incache))
	    {
	      /* We can skip the fetch operation */
	      addr.pgptr = scan_cache->pgptr;
	    }
	  else
	    {
	      /*
	       * Free the cached page
	       */
	      pgbuf_unfix_and_init (thread_p, scan_cache->pgptr);
	    }
	  /*
	   * Now remove the page from the scan cache. At the end this page or
	   * another one will be cached again.
	   */
	  scan_cache->pgptr = NULL;
	}
    }

  if (addr.pgptr == NULL)
    {
      addr.pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid,
						X_LOCK, scan_cache,
						PAGE_HEAP);
      if (addr.pgptr == NULL)
	{
	  if (er_errid () == ER_PB_BAD_PAGEID)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid, oid->pageid,
		      oid->slotid);
	    }

	  /* something went wrong, return */
	  goto error;
	}
    }

  type = spage_get_record_type (addr.pgptr, oid->slotid);
  if (type == REC_UNKNOWN)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT, 3,
	      oid->volid, oid->pageid, oid->slotid);
      goto error;
    }

  switch (type)
    {
    case REC_RELOCATION:
      /*
       * The object stored on the page is a relocation record. The relocation
       * record is used as a map to find the actual location of the content of
       * the object.
       *
       * To avoid deadlocks, see heap_update. We do not move to a second page
       * until we are done with current page.
       */

      forward_recdes.data = (char *) &forward_oid;
      forward_recdes.length = sizeof (forward_oid);
      forward_recdes.area_size = sizeof (forward_oid);

      if (spage_get_record (addr.pgptr, oid->slotid, &forward_recdes,
			    COPY) != S_SUCCESS)
	{
	  /* Unable to peek relocation record of the object */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HEAP_BAD_RELOCATION_RECORD, 3, oid->volid, oid->pageid,
		  oid->slotid);
	  goto error;
	}

      /* Lock and fetch the page of new home (relocated/forwarded) record */
      vpid.volid = forward_oid.volid;
      vpid.pageid = forward_oid.pageid;

      /*
       * To avoid a possible deadlock, make sure that you do not wait on a
       * single lock. If we need to wait, release locks and request them in
       * one operation. In case of failure, the already released locks have
       * been freed.
       *
       * Note: that we have not peeked, so we do not need to fix anything at
       *       this moment.
       */

      forward_addr.pgptr = heap_pgbuf_fix (thread_p, hfid, &vpid,
					   PGBUF_LATCH_WRITE,
					   PGBUF_CONDITIONAL_LATCH,
					   PAGE_HEAP);
      if (forward_addr.pgptr == NULL)
	{
	  pgbuf_unfix_and_init (thread_p, addr.pgptr);

	  forward_addr.pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid,
							    X_LOCK,
							    scan_cache,
							    PAGE_HEAP);
	  if (forward_addr.pgptr == NULL)
	    {
	      if (er_errid () == ER_PB_BAD_PAGEID)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HEAP_UNKNOWN_OBJECT, 3, forward_oid.volid,
			  forward_oid.pageid, forward_oid.slotid);
		}

	      goto error;
	    }
	  addr.pgptr = heap_pgbuf_fix (thread_p, hfid, &home_vpid,
				       PGBUF_LATCH_WRITE,
				       PGBUF_CONDITIONAL_LATCH, PAGE_HEAP);
	  if (addr.pgptr == NULL)
	    {
	      pgbuf_unfix_and_init (thread_p, forward_addr.pgptr);

	      if (again_count++ >= again_max)
		{
		  if (er_errid () == ER_PB_BAD_PAGEID)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid,
			      oid->pageid, oid->slotid);
		    }
		  else if (er_errid () == NO_ERROR)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_PAGE_LATCH_ABORTED, 2, home_vpid.volid,
			      home_vpid.pageid);
		    }
		  goto error;
		}
	      else
		{
		  goto try_again;
		}
	    }
	}

#if defined(RYE_DEBUG)
      if (spage_get_record_type (forward_addr.pgptr, forward_oid.slotid) !=
	  REC_NEWHOME)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_BAD_OBJECT_TYPE,
		  3, forward_oid.volid, forward_oid.pageid,
		  forward_oid.slotid);
	  goto error;
	}
#endif

      /* Remove home and forward (relocated) objects */

      /* Find the contents of the record for logging purposes */
      if (spage_get_record (forward_addr.pgptr, forward_oid.slotid,
			    &undo_recdes, PEEK) != S_SUCCESS)
	{
	  goto error;
	}

      /* Log and delete the object, and set the page dirty */

      /* Remove the home object */
      addr.offset = oid->slotid;
      log_append_undoredo_recdes (thread_p, RVHF_DELETE, &addr,
				  &forward_recdes, NULL, NULL);

      prev_freespace =
	spage_get_free_space_without_saving (thread_p, addr.pgptr);

      (void) spage_delete (thread_p, addr.pgptr, oid->slotid);

      heap_bestspace_update (thread_p, addr.pgptr, hfid, prev_freespace);

      pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);

      forward_addr.offset = forward_oid.slotid;
      log_append_undoredo_recdes (thread_p, RVHF_DELETE, &forward_addr,
				  &undo_recdes, NULL, class_oid);

      prev_freespace =
	spage_get_free_space_without_saving (thread_p, forward_addr.pgptr);

      (void) spage_delete (thread_p, forward_addr.pgptr, forward_oid.slotid);

      heap_bestspace_update (thread_p, forward_addr.pgptr, hfid,
			     prev_freespace);

      pgbuf_set_dirty (thread_p, forward_addr.pgptr, FREE);
      forward_addr.pgptr = NULL;
      break;

    case REC_BIGONE:
      /*
       * The object stored in the heap page is a relocation_overflow record,
       * get the overflow address of the object
       */

      /* Header of heap */
      vpid.volid = hfid->vfid.volid;
      vpid.pageid = hfid->hpgid;

      hdr_pgptr = heap_pgbuf_fix (thread_p, hfid, &vpid,
				  PGBUF_LATCH_WRITE, PGBUF_CONDITIONAL_LATCH,
				  PAGE_HEAP_HEADER);
      if (hdr_pgptr == NULL)
	{
	  pgbuf_unfix_and_init (thread_p, addr.pgptr);

	  hdr_pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid,
						   X_LOCK, scan_cache,
						   PAGE_HEAP_HEADER);
	  if (hdr_pgptr == NULL)
	    {
	      goto error;
	    }

	  addr.pgptr = heap_pgbuf_fix (thread_p, hfid, &home_vpid,
				       PGBUF_LATCH_WRITE,
				       PGBUF_CONDITIONAL_LATCH, PAGE_HEAP);
	  if (addr.pgptr == NULL)
	    {
	      pgbuf_unfix_and_init (thread_p, hdr_pgptr);

	      if (again_count++ >= again_max)
		{
		  if (er_errid () == ER_PB_BAD_PAGEID)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid,
			      oid->pageid, oid->slotid);
		    }
		  else if (er_errid () == NO_ERROR)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_PAGE_LATCH_ABORTED, 2, home_vpid.volid,
			      home_vpid.pageid);
		    }
		  goto error;
		}
	      else
		{
		  goto try_again;
		}
	    }
	}

      forward_recdes.data = (char *) &forward_oid;
      forward_recdes.length = sizeof (forward_oid);
      forward_recdes.area_size = sizeof (forward_oid);

      if (spage_get_record (addr.pgptr, oid->slotid, &forward_recdes,
			    COPY) != S_SUCCESS)
	{
	  /* Unable to peek overflow address of multipage object */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HEAP_BAD_RELOCATION_RECORD, 3, oid->volid, oid->pageid,
		  oid->slotid);
	  goto error;
	}

      /* Remove the home object */
      addr.offset = oid->slotid;
      log_append_undoredo_recdes (thread_p, RVHF_DELETE, &addr,
				  &forward_recdes, NULL, NULL);

      prev_freespace =
	spage_get_free_space_without_saving (thread_p, addr.pgptr);

      (void) spage_delete (thread_p, addr.pgptr, oid->slotid);

      heap_bestspace_update (thread_p, addr.pgptr, hfid, prev_freespace);

      pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);

      /* Now remove the forward (relocated/forward) object */

      if (heap_ovf_delete (thread_p, hfid, &forward_oid) == NULL)
	{
	  goto error;
	}

      pgbuf_unfix_and_init (thread_p, hdr_pgptr);
      break;

    case REC_HOME:
    case REC_NEWHOME:
    case REC_ASSIGN_ADDRESS:
      /* Find the content of the record for logging purposes */
      if (spage_get_record (addr.pgptr, oid->slotid, &undo_recdes,
			    PEEK) != S_SUCCESS)
	{
	  /* Unable to peek before image for logging purposes */
	  goto error;
	}

      /* Log and remove the object */
      addr.offset = oid->slotid;
      if (type == REC_ASSIGN_ADDRESS)
	{
	  log_append_undoredo_recdes (thread_p, RVHF_DELETE, &addr,
				      &undo_recdes, NULL, NULL);
	}
      else
	{
	  log_append_undoredo_recdes (thread_p, RVHF_DELETE, &addr,
				      &undo_recdes, NULL, class_oid);
	}

      prev_freespace = spage_get_free_space_without_saving (thread_p,
							    addr.pgptr);

      (void) spage_delete (thread_p, addr.pgptr, oid->slotid);

      heap_bestspace_update (thread_p, addr.pgptr, hfid, prev_freespace);

      pgbuf_set_dirty (thread_p, addr.pgptr, DONT_FREE);
      break;

    case REC_MARKDELETED:
    case REC_DELETED_WILL_REUSE:
    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_BAD_OBJECT_TYPE, 3,
	      oid->volid, oid->pageid, oid->slotid);
      goto error;
    }

  /*
   * Cache the page for any future scan modifications
   */
  if (scan_cache != NULL && scan_cache->cache_last_fix_page == true
      && ishome_delete == true)
    {
      scan_cache->pgptr = addr.pgptr;
    }
  else
    {
      pgbuf_unfix_and_init (thread_p, addr.pgptr);
    }

  return oid;

error:
  if (addr.pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, addr.pgptr);
    }
  if (forward_addr.pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, forward_addr.pgptr);
    }
  if (hdr_pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, hdr_pgptr);
    }

  return NULL;
}

/*
 * heap_flush () - Flush all dirty pages where the object resides
 *   return:
 *   oid(in): Object identifier
 *
 * Note: Flush all dirty pages where the object resides.
 */
void
heap_flush (THREAD_ENTRY * thread_p, const OID * oid)
{
  VPID vpid;			/* Volume and page identifiers */
  PAGE_PTR pgptr = NULL;	/* Page pointer                */
  INT16 type;
  OID forward_oid;
  RECDES forward_recdes = RECDES_INITIALIZER;
  UNUSED_VAR int ret = NO_ERROR;

  if (HEAP_ISVALID_OID (oid) != DISK_VALID)
    {
      return;
    }

  /*
   * Lock and fetch the page where the object is stored
   */
  vpid.volid = oid->volid;
  vpid.pageid = oid->pageid;
  pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, S_LOCK,
				       NULL, PAGE_HEAP);
  if (pgptr == NULL)
    {
      if (er_errid () == ER_PB_BAD_PAGEID)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT,
		  3, oid->volid, oid->pageid, oid->slotid);
	}
      /* something went wrong, return */
      return;
    }

  type = spage_get_record_type (pgptr, oid->slotid);
  if (type == REC_UNKNOWN)
    {
      goto end;
    }

  /* If this page is dirty flush it */
  (void) pgbuf_flush_with_wal (thread_p, pgptr);

  switch (type)
    {
    case REC_RELOCATION:
      /*
       * The object stored on the page is a relocation record. The relocation
       * record is used as a map to find the actual location of the content of
       * the object.
       */
      forward_recdes.data = (char *) &forward_oid;
      forward_recdes.length = sizeof (forward_oid);
      forward_recdes.area_size = sizeof (forward_oid);

      if (spage_get_record (pgptr, oid->slotid, &forward_recdes, COPY)
	  != S_SUCCESS)
	{
	  /* Unable to get relocation record of the object */
	  goto end;
	}
      pgbuf_unfix_and_init (thread_p, pgptr);

      /* Fetch the new home page */
      vpid.volid = forward_oid.volid;
      vpid.pageid = forward_oid.pageid;

      pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, S_LOCK,
					   NULL, PAGE_HEAP);
      if (pgptr == NULL)
	{
	  if (er_errid () == ER_PB_BAD_PAGEID)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_UNKNOWN_OBJECT, 3, forward_oid.volid,
		      forward_oid.pageid, forward_oid.slotid);
	    }

	  return;
	}
      (void) pgbuf_flush_with_wal (thread_p, pgptr);
      break;

    case REC_BIGONE:
      /*
       * The object stored in the heap page is a relocation_overflow record,
       * get the overflow address of the object
       */
      forward_recdes.data = (char *) &forward_oid;
      forward_recdes.length = sizeof (forward_oid);
      forward_recdes.area_size = sizeof (forward_oid);

      if (spage_get_record (pgptr, oid->slotid, &forward_recdes, COPY)
	  != S_SUCCESS)
	{
	  /* Unable to peek overflow address of multipage object */
	  goto end;
	}

      pgbuf_unfix_and_init (thread_p, pgptr);
      ret = heap_ovf_flush (thread_p, &forward_oid);
      break;

    case REC_ASSIGN_ADDRESS:
    case REC_HOME:
    case REC_NEWHOME:
    case REC_MARKDELETED:
    case REC_DELETED_WILL_REUSE:
    default:
      break;
    }

end:
  if (pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, pgptr);
    }
}

/*
 * heap_scancache_start_internal () - Start caching information for a heap scan
 *   return: NO_ERROR
 *   scan_cache(in/out): Scan cache
 *   hfid(in): Heap file identifier of the scan cache or NULL
 *             If NULL is given heap_get is the only function that can
 *             be used with the scan cache.
 *   vpid_alloc(in): force new page allocation
 *   class_oid(in): Class identifier of scan cache
 *                  For any class, NULL or NULL_OID can be given
 *   cache_last_fix_page(in): Wheater or not to cache the last fetched page
 *                            between scan objects ?
 *
 */
static int
heap_scancache_start_internal (UNUSED_ARG THREAD_ENTRY * thread_p,
			       HEAP_SCANCACHE * scan_cache, const HFID * hfid,
			       const bool vpid_alloc,
			       const OID * class_oid, int cache_last_fix_page)
{
  int ret = NO_ERROR;

  if (class_oid != NULL)
    {
      /*
       * Scanning the instances of a specific class
       */
      scan_cache->class_oid = *class_oid;
    }
  else
    {
      /*
       * Scanning the instances of any class in the heap
       */
      OID_SET_NULL (&scan_cache->class_oid);
    }


  if (hfid == NULL)
    {
      HFID_SET_NULL (&scan_cache->hfid);
      scan_cache->hfid.vfid.volid = NULL_VOLID;
    }
  else
    {
      scan_cache->hfid.vfid.volid = hfid->vfid.volid;
      scan_cache->hfid.vfid.fileid = hfid->vfid.fileid;
      scan_cache->hfid.hpgid = hfid->hpgid;
      assert (file_get_type (thread_p, &hfid->vfid) == FILE_HEAP
	      || er_errid () == ER_INTERRUPTED);
    }

  scan_cache->vpid_alloc = vpid_alloc;

  scan_cache->page_latch = S_LOCK;

  scan_cache->cache_last_fix_page = cache_last_fix_page;
  scan_cache->pgptr = NULL;
  scan_cache->area = NULL;
  scan_cache->area_size = -1;
  scan_cache->collect_nxvpid.volid = scan_cache->hfid.vfid.volid;
  scan_cache->collect_nxvpid.pageid = scan_cache->hfid.hpgid;

  scan_cache->debug_initpattern = HEAP_DEBUG_SCANCACHE_INITPATTERN;
  scan_cache->read_committed_page = false;
  VPID_SET_NULL (&scan_cache->last_vpid);

  return ret;

#if 0
exit_on_error:
#endif

#if 1
  assert (false);		/* not permit here */
#endif

  HFID_SET_NULL (&scan_cache->hfid);
  scan_cache->vpid_alloc = false;
  scan_cache->hfid.vfid.volid = NULL_VOLID;
  OID_SET_NULL (&scan_cache->class_oid);
  scan_cache->page_latch = NULL_LOCK;
  scan_cache->cache_last_fix_page = false;
  scan_cache->pgptr = NULL;
  scan_cache->area = NULL;
  scan_cache->area_size = 0;
  VPID_SET_NULL (&scan_cache->collect_nxvpid);
  scan_cache->debug_initpattern = 0;
  scan_cache->read_committed_page = false;
  VPID_SET_NULL (&scan_cache->last_vpid);

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_scancache_start () - Start caching information for a heap scan
 *   return: NO_ERROR
 *   scan_cache(in/out): Scan cache
 *   hfid(in): Heap file identifier of the scan cache or NULL
 *             If NULL is given heap_get is the only function that can
 *             be used with the scan cache.
 *   class_oid(in): Class identifier of scan cache
 *                  For any class, NULL or NULL_OID can be given
 *   cache_last_fix_page(in): Wheater or not to cache the last fetched page
 *                            between scan objects ?
 */
int
heap_scancache_start (THREAD_ENTRY * thread_p, HEAP_SCANCACHE * scan_cache,
		      const HFID * hfid, const OID * class_oid,
		      int cache_last_fix_page)
{
  return heap_scancache_start_internal (thread_p, scan_cache, hfid, false,
					class_oid, cache_last_fix_page);
}

/*
 * heap_scancache_start_modify () - Start caching information for heap
 *                                modifications
 *   return: NO_ERROR
 *   scan_cache(in/out): Scan cache
 *   hfid(in): Heap file identifier of the scan cache or NULL
 *             If NULL is given heap_get is the only function that can
 *             be used with the scan cache.
 *   force_page_allocation(in): force new page allocation
 *   class_oid(in): Class identifier of scan cache
 *                  For any class, NULL or NULL_OID can be given
 *
 * Note: A scancache structure is started for heap modifications.
 * The scan_cache structure is used to modify objects of the heap
 * with heap_insert, heap_update, and heap_delete. The scan structure
 * is used to cache information about the latest used page which
 * can be used by the following function to guess where to insert
 * objects, or other updates and deletes on the same page.
 * Good when we are updating things in a sequential way.
 *
 * The heap manager automatically resets the scan_cache structure
 * when it is used with a different heap. That is, the scan_cache
 * is reset with the heap and class of the insertion, update, and
 * delete. Therefore, you could pass NULLs to hfid, and class_oid
 * to this function, but that it is not recommended.
 */
int
heap_scancache_start_modify (THREAD_ENTRY * thread_p,
			     HEAP_SCANCACHE * scan_cache, const HFID * hfid,
			     const int force_page_allocation,
			     const OID * class_oid)
{
  bool vpid_alloc;
  int ret = NO_ERROR;

  assert (force_page_allocation == 0 || force_page_allocation == 1);

  vpid_alloc = (force_page_allocation == 1) ? true : false;

  if (heap_scancache_start_internal (thread_p, scan_cache, hfid, vpid_alloc,
				     NULL, false) != NO_ERROR)
    {
      goto exit_on_error;
    }

  if (class_oid != NULL)
    {
      ret = heap_scancache_reset_modify (thread_p, scan_cache, hfid,
					 class_oid);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }
  else
    {
      scan_cache->page_latch = X_LOCK;
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_scancache_force_modify () -
 *   return: NO_ERROR
 *   scan_cache(in):
 */
static int
heap_scancache_force_modify (THREAD_ENTRY * thread_p,
			     HEAP_SCANCACHE * scan_cache)
{
  if (scan_cache == NULL
      || scan_cache->debug_initpattern != HEAP_DEBUG_SCANCACHE_INITPATTERN)
    {
      return NO_ERROR;
    }

  /* Free fetched page */
  if (scan_cache->pgptr != NULL)
    {
      pgbuf_unfix_and_init (thread_p, scan_cache->pgptr);
    }

  return NO_ERROR;
}

/*
 * heap_scancache_reset_modify () - Reset the current caching information
 *   return: NO_ERROR
 *   scan_cache(in/out): Scan cache
 *   hfid(in): Heap file identifier of the scan cache
 *   class_oid(in): Class identifier of scan cache
 *
 * Note: Any page that has been cached under the current scan cache is
 * freed and the scancache structure is reinitialized with the
 * new information.
 */
static int
heap_scancache_reset_modify (THREAD_ENTRY * thread_p,
			     HEAP_SCANCACHE * scan_cache, const HFID * hfid,
			     const OID * class_oid)
{
  int ret;

  ret = heap_scancache_force_modify (thread_p, scan_cache);
  if (ret != NO_ERROR)
    {
      return ret;
    }

  if (class_oid != NULL)
    {
      scan_cache->class_oid = *class_oid;
    }
  else
    {
      OID_SET_NULL (&scan_cache->class_oid);
    }

  if (!HFID_EQ (&scan_cache->hfid, hfid))
    {
      scan_cache->hfid.vfid.volid = hfid->vfid.volid;
      scan_cache->hfid.vfid.fileid = hfid->vfid.fileid;
      scan_cache->hfid.hpgid = hfid->hpgid;

      assert (file_get_type (thread_p, &hfid->vfid) == FILE_HEAP
	      || er_errid () == ER_INTERRUPTED);
    }

  scan_cache->page_latch = X_LOCK;

  return ret;
}

/*
 * heap_scancache_quick_start () - Start caching information for a heap scan
 *   return: NO_ERROR
 *   scan_cache(in/out): Scan cache
 *
 * Note: This is a quick way to initialize a scancahe structure. It
 * should be used only when we would like to peek only one object
 * (heap_get). This function will cache the last fetched page by default.
 *
 *  This function was created to avoid some of the overhead
 *  associated with scancahe(e.g., find best pages, lock the heap)
 *  since we are not really scanning the heap.
 *
 *  For other needs/uses, please refer to heap_scancache_start ().
 *
 * Note: Using many scancaches with the cached_fix page option at the
 * same time should be avoided since page buffers are fixed and
 * locked for future references and there is a limit of buffers
 * in the page buffer pool. This is analogous to fetching many
 * pages at the same time. The page buffer pool is expanded when
 * needed, however, developers must pay special attention to
 * avoid this situation.
 */
int
heap_scancache_quick_start (HEAP_SCANCACHE * scan_cache)
{
  heap_scancache_quick_start_internal (scan_cache);

  scan_cache->page_latch = S_LOCK;

  return NO_ERROR;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * heap_scancache_quick_start_modify () - Start caching information
 *                                      for a heap modifications
 *   return: NO_ERROR
 *   scan_cache(in/out): Scan cache
 */
int
heap_scancache_quick_start_modify (HEAP_SCANCACHE * scan_cache)
{
  heap_scancache_quick_start_internal (scan_cache);

  scan_cache->page_latch = X_LOCK;

  return NO_ERROR;
}
#endif

/*
 * heap_scancache_quick_start_internal () -
 *
 *   return: NO_ERROR
 *   scan_cache(in/out): Scan cache
 */
static int
heap_scancache_quick_start_internal (HEAP_SCANCACHE * scan_cache)
{
  HFID_SET_NULL (&scan_cache->hfid);
  scan_cache->hfid.vfid.volid = NULL_VOLID;
  scan_cache->vpid_alloc = false;
  OID_SET_NULL (&scan_cache->class_oid);
  scan_cache->page_latch = S_LOCK;
  scan_cache->cache_last_fix_page = true;
  scan_cache->pgptr = NULL;
  scan_cache->area = NULL;
  scan_cache->area_size = 0;
  VPID_SET_NULL (&scan_cache->collect_nxvpid);
  scan_cache->debug_initpattern = HEAP_DEBUG_SCANCACHE_INITPATTERN;
  scan_cache->read_committed_page = false;
  VPID_SET_NULL (&scan_cache->last_vpid);

  return NO_ERROR;
}

/*
 * heap_scancache_quick_end () - Stop caching information for a heap scan
 *   return: NO_ERROR
 *   scan_cache(in/out): Scan cache
 *
 * Note: Any fixed heap page on the given scan is freed and any memory
 * allocated by this scan is also freed. The scan_cache structure
 * is undefined.  This function does not update any space statistics.
 */
static int
heap_scancache_quick_end (THREAD_ENTRY * thread_p,
			  HEAP_SCANCACHE * scan_cache)
{
  int ret = NO_ERROR;

  if (scan_cache->debug_initpattern != HEAP_DEBUG_SCANCACHE_INITPATTERN)
    {
      er_log_debug (ARG_FILE_LINE, "heap_scancache_quick_end: Your scancache"
		    " is not initialized");
      ret = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 1, "");
    }
  else
    {
      if (scan_cache->cache_last_fix_page == true)
	{
	  /* Free fetched page */
	  if (scan_cache->pgptr != NULL)
	    {
	      pgbuf_unfix_and_init (thread_p, scan_cache->pgptr);
	    }
	}

      /* Free memory */
      if (scan_cache->area)
	{
	  free_and_init (scan_cache->area);
	}
    }

  HFID_SET_NULL (&scan_cache->hfid);
  scan_cache->hfid.vfid.volid = NULL_VOLID;
  scan_cache->vpid_alloc = false;
  OID_SET_NULL (&scan_cache->class_oid);
  scan_cache->page_latch = NULL_LOCK;
  scan_cache->pgptr = NULL;
  scan_cache->area = NULL;
  scan_cache->area_size = 0;
  VPID_SET_NULL (&scan_cache->collect_nxvpid);
  scan_cache->debug_initpattern = 0;
  scan_cache->read_committed_page = false;
  VPID_SET_NULL (&scan_cache->last_vpid);

  return ret;
}

/*
 * heap_scancache_end_internal () -
 *   return: NO_ERROR
 *   scan_cache(in):
 *   scan_state(in):
 */
static int
heap_scancache_end_internal (THREAD_ENTRY * thread_p,
			     HEAP_SCANCACHE * scan_cache,
			     UNUSED_ARG bool scan_state)
{
  int ret = NO_ERROR;

  if (scan_cache->debug_initpattern != HEAP_DEBUG_SCANCACHE_INITPATTERN)
    {
      er_log_debug (ARG_FILE_LINE,
		    "heap_scancache_end_internal: Your scancache"
		    " is not initialized");
      return ER_FAILED;
    }

  if (!OID_ISNULL (&scan_cache->class_oid))
    {
      OID_SET_NULL (&scan_cache->class_oid);
    }

  ret = heap_scancache_quick_end (thread_p, scan_cache);

  return ret;
}

/*
 * heap_scancache_end () - Stop caching information for a heap scan
 *   return: NO_ERROR
 *   scan_cache(in/out): Scan cache
 *
 * Note: Any fixed heap page on the given scan is freed and any memory
 * allocated by this scan is also freed. The scan_cache structure is undefined.
 */
int
heap_scancache_end (THREAD_ENTRY * thread_p, HEAP_SCANCACHE * scan_cache)
{
  UNUSED_VAR int ret;

  ret = heap_scancache_end_internal (thread_p, scan_cache, END_SCAN);

  return NO_ERROR;
}

/*
 * heap_scancache_end_when_scan_will_resume () -
 *   return:
 *   scan_cache(in):
 */
int
heap_scancache_end_when_scan_will_resume (THREAD_ENTRY * thread_p,
					  HEAP_SCANCACHE * scan_cache)
{
  UNUSED_VAR int ret;

  ret = heap_scancache_end_internal (thread_p, scan_cache, CONTINUE_SCAN);

  return NO_ERROR;
}

/*
 * heap_scancache_end_modify () - End caching information for a heap
 *				  modification cache
 *   return:
 *   scan_cache(in/out): Scan cache
 *
 * Note: Any fixed heap page on the given scan is freed. The heap
 * best find space statistics for the heap are completely updated
 * with the ones stored in the scan cache.
 */
void
heap_scancache_end_modify (THREAD_ENTRY * thread_p,
			   HEAP_SCANCACHE * scan_cache)
{
  int ret;

  ret = heap_scancache_force_modify (thread_p, scan_cache);
  if (ret == NO_ERROR)
    {
      ret = heap_scancache_quick_end (thread_p, scan_cache);
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_get_chn () - Get the chn of the object
 *   return: chn or NULL_CHN
 *   oid(in): Object identifier
 *
 * Note: Find the cache coherency number of the object.
 */
int
heap_get_chn (THREAD_ENTRY * thread_p, const OID * oid)
{
  RECDES recdes = RECDES_INITIALIZER;
  HEAP_SCANCACHE scan_cache;
  int chn;

  chn = heap_chnguess_get (thread_p, oid,
			   logtb_get_current_tran_index (thread_p));

  if (chn == NULL_CHN)
    {
      heap_scancache_quick_start (&scan_cache);
      if (heap_get (thread_p, oid, &recdes, &scan_cache, PEEK) == S_SUCCESS)
	{
	  chn = or_chn (&recdes);
	}
      heap_scancache_end (thread_p, &scan_cache);
    }

  return chn;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * heap_get () - Retrieve or peek an object
 *   return: SCAN_CODE
 *           (Either of S_SUCCESS,
 *                      S_SUCCESS_CHN_UPTODATE,
 *                      S_DOESNT_FIT,
 *                      S_DOESNT_EXIST,
 *                      S_ERROR)
 *   oid(in): Object identifier
 *   recdes(in/out): Record descriptor
 *   scan_cache(in/out): Scan cache or NULL
 *   ispeeking(in): PEEK when the object is peeked, scan_cache cannot be NULL
 *                  COPY when the object is copied
 *
 */
SCAN_CODE
heap_get (THREAD_ENTRY * thread_p, const OID * oid, RECDES * recdes,
	  HEAP_SCANCACHE * scan_cache, int ispeeking)
{

  return heap_get_internal (thread_p, NULL, (OID *) oid, recdes, scan_cache,
			    ispeeking);
}

/*
 * heap_get_internal () - Retrieve or peek an object
 *   return: SCAN_CODE
 *           (Either of S_SUCCESS,
 *                      S_SUCCESS_CHN_UPTODATE,
 *                      S_DOESNT_FIT,
 *                      S_DOESNT_EXIST,
 *                      S_ERROR)
 *   class_oid(out):
 *   oid(in/out): Object identifier; set shard groupid
 *   recdes(in/out): Record descriptor
 *   scan_cache(in/out): Scan cache or NULL
 *   ispeeking(in): PEEK when the object is peeked, scan_cache cannot be NULL
 *                  COPY when the object is copied
 *
 *   Note :
 *   If the type of record is REC_UNKNOWN(MARK_DELETED or DELETED_WILL_REUSE),
 *   this function returns S_DOESNT_EXIST and set warning.
 *   In this case, The caller should verify this.
 *   For example, In function scan_next_scan...
 *   If the current isolation level is uncommit_read and
 *   heap_get returns S_DOESNT_EXIST,
 *   then scan_next_scan ignores current record and continue.
 *   But if the isolation level is higher than uncommit_read,
 *   scan_next_scan returns an error.
 */
static SCAN_CODE
heap_get_internal (THREAD_ENTRY * thread_p, OID * class_oid, OID * oid,
		   RECDES * recdes, HEAP_SCANCACHE * scan_cache,
		   int ispeeking)
{
  VPID home_vpid, forward_vpid;
  VPID *vpidptr_incache;
  PAGE_PTR pgptr = NULL;
  PAGE_PTR forward_pgptr = NULL;
  INT16 type;
  OID forward_oid;
  RECDES forward_recdes = RECDES_INITIALIZER;
  SCAN_CODE scan;
  DISK_ISVALID oid_valid;
  int again_count = 0;
  int again_max = 20;
  const HFID *hfid;

  if (scan_cache != NULL)
    {
      hfid = &(scan_cache->hfid);
    }
  else
    {
      hfid = NULL;
    }

#if defined(RYE_DEBUG)
  if (scan_cache == NULL && ispeeking == PEEK)
    {
      er_log_debug (ARG_FILE_LINE, "heap_get: Using wrong interface."
		    " scan_cache cannot be NULL when peeking.");
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      "");
      return S_ERROR;
    }

  if (scan_cache != NULL
      && scan_cache->debug_initpatter != HEAP_DEBUG_SCANCACHE_INITPATTER)
    {
      er_log_debug (ARG_FILE_LINE,
		    "heap_get: Your scancache is not initialized");
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      "");
      return S_ERROR;
    }
#endif /* RYE_DEBUG */

  if (scan_cache == NULL)
    {
      /* It is possible only in case of ispeeking == COPY */
      if (recdes->data == NULL)
	{
	  er_log_debug (ARG_FILE_LINE, "heap_get: Using wrong interface."
			" recdes->area_size cannot be -1.");
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
		  1, "");
	  return S_ERROR;
	}
    }

  oid_valid = HEAP_ISVALID_OID (oid);
  if (oid_valid != DISK_VALID)
    {
      if (oid_valid != DISK_ERROR || er_errid () == NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT, 3,
		  oid->volid, oid->pageid, oid->slotid);
	}
      return S_DOESNT_EXIST;
    }

try_again:

  home_vpid.volid = oid->volid;
  home_vpid.pageid = oid->pageid;

  /*
   * Use previous scan page whenever possible, otherwise, deallocate the
   * page
   */

  if (scan_cache != NULL && scan_cache->cache_last_fix_page == true
      && scan_cache->pgptr != NULL)
    {
      vpidptr_incache = pgbuf_get_vpid_ptr (scan_cache->pgptr);
      if (VPID_EQ (&home_vpid, vpidptr_incache))
	{
	  /* We can skip the fetch operation */
	  pgptr = scan_cache->pgptr;
	}
      else
	{
	  /* Free the previous scan page and obtain a new page */
	  pgbuf_unfix_and_init (thread_p, scan_cache->pgptr);
	  pgptr = heap_scan_pb_lock_and_fetch (thread_p, &home_vpid,
					       S_LOCK, scan_cache, PAGE_HEAP);
	  if (pgptr == NULL)
	    {
	      if (er_errid () == ER_PB_BAD_PAGEID)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid, oid->pageid,
			  oid->slotid);
		}

	      /* something went wrong, return */
	      scan_cache->pgptr = NULL;
	      return S_ERROR;
	    }
	}
      scan_cache->pgptr = NULL;
    }
  else
    {
      pgptr = heap_scan_pb_lock_and_fetch (thread_p, &home_vpid,
					   S_LOCK, scan_cache, PAGE_HEAP);
      if (pgptr == NULL)
	{
	  if (er_errid () == ER_PB_BAD_PAGEID)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid, oid->pageid,
		      oid->slotid);
	    }

	  /* something went wrong, return */
	  return S_ERROR;
	}
    }

  if (class_oid != NULL)
    {
      RECDES chain_recdes = RECDES_INITIALIZER;
      HEAP_CHAIN *chain;

      if (spage_get_record (pgptr, HEAP_HEADER_AND_CHAIN_SLOTID,
			    &chain_recdes, PEEK) != S_SUCCESS)
	{
	  pgbuf_unfix_and_init (thread_p, pgptr);
	  return S_ERROR;
	}

      chain = (HEAP_CHAIN *) chain_recdes.data;
      COPY_OID (class_oid, &(chain->class_oid));

      /*
       * kludge, rootclass is identified with a NULL class OID but we must
       * substitute the actual OID here - think about this
       */
      if (OID_ISNULL (class_oid))
	{
	  /* rootclass class oid, substitute with global */
	  COPY_OID (class_oid, oid_Root_class_oid);
	  assert (class_oid->groupid == GLOBAL_GROUPID);
	}
    }

  type = spage_get_record_type (pgptr, oid->slotid);
  if (type == REC_UNKNOWN)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT, 3,
	      oid->volid, oid->pageid, oid->slotid);
      pgbuf_unfix_and_init (thread_p, pgptr);
      return S_DOESNT_EXIST;
    }

  assert (type != REC_UNKNOWN);
  recdes->type = type;

  switch (type)
    {
    case REC_RELOCATION:
      /*
       * The record stored on the page is a relocation record, get the new
       * home of the record
       */
      forward_recdes.data = (char *) &forward_oid;
      forward_recdes.length = sizeof (forward_oid);
      forward_recdes.area_size = sizeof (forward_oid);

      scan = spage_get_record (pgptr, oid->slotid, &forward_recdes, COPY);
      if (scan != S_SUCCESS)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HEAP_BAD_RELOCATION_RECORD, 3, oid->volid, oid->pageid,
		  oid->slotid);
	  pgbuf_unfix_and_init (thread_p, pgptr);
	  return scan;
	}

      /* Fetch the page of relocated (forwarded) record */
      forward_vpid.volid = forward_oid.volid;
      forward_vpid.pageid = forward_oid.pageid;

      /* try to fix forward page conditionally */
      forward_pgptr = heap_pgbuf_fix (thread_p, hfid, &forward_vpid,
				      PGBUF_LATCH_READ,
				      PGBUF_CONDITIONAL_LATCH, PAGE_HEAP);
      if (forward_pgptr == NULL)
	{
	  pgbuf_unfix_and_init (thread_p, pgptr);

	  /* try to fix forward page unconditionally */
	  forward_pgptr = heap_scan_pb_lock_and_fetch (thread_p,
						       &forward_vpid,
						       S_LOCK,
						       scan_cache, PAGE_HEAP);
	  if (forward_pgptr == NULL)
	    {
	      if (er_errid () == ER_PB_BAD_PAGEID)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HEAP_BAD_RELOCATION_RECORD, 3,
			  forward_oid.volid, forward_oid.pageid,
			  forward_oid.slotid);
		}

	      return S_ERROR;
	    }
	  pgbuf_unfix_and_init (thread_p, forward_pgptr);

	  if (again_count++ >= again_max)
	    {
	      if (er_errid () == ER_PB_BAD_PAGEID)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid,
			  oid->pageid, oid->slotid);
		}
	      else if (er_errid () == NO_ERROR)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_PAGE_LATCH_ABORTED, 2, forward_vpid.volid,
			  forward_vpid.pageid);
		}
	      return S_ERROR;
	    }

	  goto try_again;
	}

      assert (pgptr != NULL && forward_pgptr != NULL);
      assert (spage_get_record_type (forward_pgptr,
				     forward_oid.slotid) == REC_NEWHOME);

      if (ispeeking == COPY && recdes->data == NULL)
	{
	  /* It is guaranteed that scan_cache is not NULL. */
	  if (scan_cache->area == NULL)
	    {
	      /* Allocate an area to hold the object. Assume that
	         the object will fit in two pages for not better estimates.
	       */
	      scan_cache->area_size = DB_PAGESIZE * 2;
	      scan_cache->area = (char *) malloc (scan_cache->area_size);
	      if (scan_cache->area == NULL)
		{
		  scan_cache->area_size = -1;
		  pgbuf_unfix_and_init (thread_p, pgptr);
		  pgbuf_unfix_and_init (thread_p, forward_pgptr);
		  return S_ERROR;
		}
	    }
	  recdes->data = scan_cache->area;
	  recdes->area_size = scan_cache->area_size;
	  /* The allocated space is enough to save the instance. */
	}

      if (ispeeking == PEEK)
	{
	  scan = spage_get_record (forward_pgptr, forward_oid.slotid, recdes,
				   PEEK);
	}
      else
	{
	  scan = spage_get_record (forward_pgptr, forward_oid.slotid, recdes,
				   COPY);
	}

      if (scan_cache != NULL && scan_cache->cache_last_fix_page == true)
	{
	  /* Save the page for a future scan */
	  scan_cache->pgptr = pgptr;
	}
      else
	{
	  pgbuf_unfix_and_init (thread_p, pgptr);
	}

      pgbuf_unfix_and_init (thread_p, forward_pgptr);

      break;

    case REC_ASSIGN_ADDRESS:
      /* Object without content. only the address has been assigned */
      if (spage_check_slot_owner (thread_p, pgptr, oid->slotid))
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_HEAP_NODATA_NEWADDRESS, 3, oid->volid, oid->pageid,
		  oid->slotid);
	  scan = S_DOESNT_EXIST;
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_UNKNOWN_OBJECT,
		  3, oid->volid, oid->pageid, oid->slotid);
	  scan = S_ERROR;
	}

      if (scan_cache != NULL && scan_cache->cache_last_fix_page == true)
	{
	  /* Save the page for a future scan */
	  scan_cache->pgptr = pgptr;
	}
      else
	{
	  pgbuf_unfix_and_init (thread_p, pgptr);
	}

      break;

    case REC_HOME:
      if (ispeeking == COPY && recdes->data == NULL)
	{			/* COPY */
	  /* It is guaranteed that scan_cache is not NULL. */
	  if (scan_cache->area == NULL)
	    {
	      /* Allocate an area to hold the object. Assume that
	         the object will fit in two pages for not better estimates.
	       */
	      scan_cache->area_size = DB_PAGESIZE * 2;
	      scan_cache->area = (char *) malloc (scan_cache->area_size);
	      if (scan_cache->area == NULL)
		{
		  scan_cache->area_size = -1;
		  pgbuf_unfix_and_init (thread_p, pgptr);
		  return S_ERROR;
		}
	    }
	  recdes->data = scan_cache->area;
	  recdes->area_size = scan_cache->area_size;
	  /* The allocated space is enough to save the instance. */
	}

      if (ispeeking == PEEK)
	{
	  scan = spage_get_record (pgptr, oid->slotid, recdes, PEEK);
	}
      else
	{
	  scan = spage_get_record (pgptr, oid->slotid, recdes, COPY);
	}

      if (scan_cache != NULL && scan_cache->cache_last_fix_page == true)
	{
	  /* Save the page for a future scan */
	  scan_cache->pgptr = pgptr;
	}
      else
	{
	  pgbuf_unfix_and_init (thread_p, pgptr);
	}
      break;

    case REC_BIGONE:
      /* Get the address of the content of the multipage object in overflow */
      forward_recdes.data = (char *) &forward_oid;
      forward_recdes.length = sizeof (forward_oid);
      forward_recdes.area_size = sizeof (forward_oid);

      scan = spage_get_record (pgptr, oid->slotid, &forward_recdes, COPY);
      if (scan != S_SUCCESS)
	{
	  /* Unable to read overflow address of multipage object */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HEAP_BAD_RELOCATION_RECORD, 3, oid->volid, oid->pageid,
		  oid->slotid);
	  pgbuf_unfix_and_init (thread_p, pgptr);
	  return scan;
	}
      pgbuf_unfix_and_init (thread_p, pgptr);

      /*
       * Now get the content of the multipage object.
       */

      /* Try to reuse the previously allocated area */
      if (scan_cache != NULL && (ispeeking == PEEK || recdes->data == NULL))
	{
	  if (scan_cache->area == NULL)
	    {
	      /*
	       * Allocate an area to hold the object. Assume that the object
	       * will fit in two pages for not better estimates. We could call
	       * heap_ovf_get_length, but it may be better to just guess and
	       * realloc if needed.
	       * We could also check the estimates for average object length,
	       * but again, it may be expensive and may not be accurate
	       * for this object.
	       */
	      scan_cache->area_size = DB_PAGESIZE * 2;
	      scan_cache->area = (char *) malloc (scan_cache->area_size);
	      if (scan_cache->area == NULL)
		{
		  scan_cache->area_size = -1;
		  return S_ERROR;
		}
	    }
	  recdes->data = scan_cache->area;
	  recdes->area_size = scan_cache->area_size;

	  while ((scan = heap_ovf_get (thread_p, &forward_oid, recdes))
		 == S_DOESNT_FIT)
	    {
	      /*
	       * The object did not fit into such an area, reallocate a new
	       * area
	       */

	      recdes->area_size = -recdes->length;
	      recdes->data =
		(char *) realloc (scan_cache->area, recdes->area_size);
	      if (recdes->data == NULL)
		{
		  return S_ERROR;
		}
	      scan_cache->area_size = recdes->area_size;
	      scan_cache->area = recdes->data;
	    }
	  if (scan != S_SUCCESS)
	    {
	      recdes->data = NULL;
	    }
	}
      else
	{
	  scan = heap_ovf_get (thread_p, &forward_oid, recdes);
	}

      break;

    case REC_MARKDELETED:
    case REC_DELETED_WILL_REUSE:
    case REC_NEWHOME:
    default:
      scan = S_ERROR;
      pgbuf_unfix_and_init (thread_p, pgptr);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_BAD_OBJECT_TYPE, 3,
	      oid->volid, oid->pageid, oid->slotid);
      break;
    }

  if (scan == S_SUCCESS)
    {
      /* get shard groupid
       */
      oid->groupid = or_grp_id (recdes);
      assert (oid->groupid >= GLOBAL_GROUPID);

      /* do not check iff valid groupid; already done at index Mgr
       */
    }

  return scan;
}

/*
 * heap_get_with_class_oid () - Retrieve or peek an object and get its class oid
 *   return: SCAN_CODE
 *           (Either of S_SUCCESS, S_DOESNT_FIT, S_DOESNT_EXIST, S_ERROR)
 *   class_oid(out): Class OID for the object
 *   oid(in): Object identifier
 *   recdes(in/out): Record descriptor
 *   scan_cache(in/out): Scan cache or NULL
 *   ispeeking(in): PEEK when the object is peeked, scan_cache cannot be
 *                  NULL COPY when the object is copied
 *
 * Note: Same as heap_get, except that it will also return the class oid
 * for the object.  (see heap_get) description)
 */
SCAN_CODE
heap_get_with_class_oid (THREAD_ENTRY * thread_p, OID * class_oid,
			 const OID * oid, RECDES * recdes,
			 HEAP_SCANCACHE * scan_cache, int ispeeking)
{
  SCAN_CODE scan;

  if (class_oid == NULL)
    {
      assert (false);
      return S_ERROR;
    }

  scan =
    heap_get_internal (thread_p, class_oid, (OID *) oid, recdes, scan_cache,
		       ispeeking);
  if (scan != S_SUCCESS)
    {
      OID_SET_NULL (class_oid);
    }

  return scan;
}

/*
 * heap_next () - Retrieve or peek next object
 *   return: SCAN_CODE (Either of S_SUCCESS, S_DOESNT_FIT, S_END, S_ERROR)
 *   hfid(in):
 *   class_oid(in):
 *   next_oid(in/out): Object identifier of current record.
 *                     Will be set to next available record or NULL_OID when
 *                     there is not one.
 *   recdes(in/out): Pointer to a record descriptor. Will be modified to
 *                   describe the new record.
 *   scan_cache(in/out): Scan cache or NULL
 *   ispeeking(in): PEEK when the object is peeked, scan_cache cannot be NULL
 *                  COPY when the object is copied
 *
 */
SCAN_CODE
heap_next (THREAD_ENTRY * thread_p, const HFID * hfid, OID * class_oid,
	   OID * next_oid, RECDES * recdes, HEAP_SCANCACHE * scan_cache,
	   int ispeeking)
{
  VPID vpid;
  VPID *vpidptr_incache;
  PAGE_PTR pgptr = NULL;
  INT16 type = REC_UNKNOWN;
  OID oid;
  OID forward_oid;
  OID *peek_oid;
  RECDES forward_recdes = RECDES_INITIALIZER;
  SCAN_CODE scan = S_ERROR;
  int continue_looking;

  assert ((scan_cache == NULL && ispeeking == COPY)
	  || (scan_cache->debug_initpattern ==
	      HEAP_DEBUG_SCANCACHE_INITPATTERN
	      && !HFID_IS_NULL (&scan_cache->hfid)));


  if (scan_cache == NULL)
    {
      /* It is possible only in case of ispeeking == COPY */
      if (recdes->data == NULL)
	{
	  er_log_debug (ARG_FILE_LINE, "heap_next: Using wrong interface."
			" recdes->area_size cannot be -1.");
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
		  1, "");
	  return S_ERROR;
	}
    }

  if (scan_cache != NULL)
    {
      hfid = &scan_cache->hfid;
      if (!OID_ISNULL (&scan_cache->class_oid))
	{
	  class_oid = &scan_cache->class_oid;
	}
    }

  if (OID_ISNULL (next_oid))
    {
      /* Retrieve the first object of the heap */
      oid.volid = hfid->vfid.volid;
      oid.pageid = hfid->hpgid;
      oid.slotid = 0;		/* i.e., will get slot 1 */
      oid.groupid = NULL_GROUPID;
    }
  else
    {
      oid = *next_oid;
    }

  continue_looking = true;
  while (continue_looking == true)
    {
      continue_looking = false;

      while (true)
	{
	  vpid.volid = oid.volid;
	  vpid.pageid = oid.pageid;

	  /*
	   * Fetch the page where the object of OID is stored. Use previous
	   * scan page whenever possible, otherwise, deallocate the page.
	   */
	  if (scan_cache != NULL)
	    {
	      pgptr = NULL;
	      if (scan_cache->cache_last_fix_page == true
		  && scan_cache->pgptr != NULL)
		{
		  vpidptr_incache = pgbuf_get_vpid_ptr (scan_cache->pgptr);
		  if (VPID_EQ (&vpid, vpidptr_incache))
		    {
		      /* We can skip the fetch operation */
		      pgptr = scan_cache->pgptr;
		      scan_cache->pgptr = NULL;
		    }
		  else
		    {
		      /* Free the previous scan page */
		      pgbuf_unfix_and_init (thread_p, scan_cache->pgptr);
		    }
		}
	      if (pgptr == NULL)
		{
		  int retry_count = 0;

		retry_page_fix:
		  pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid,
						       S_LOCK,
						       scan_cache, PAGE_HEAP);
		  if (pgptr == NULL)
		    {
		      if (er_errid () == ER_PB_BAD_PAGEID)
			{
			  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				  ER_HEAP_UNKNOWN_OBJECT, 3, oid.volid,
				  oid.pageid, oid.slotid);
			}

		      /* something went wrong, return */
		      scan_cache->pgptr = NULL;
		      return S_ERROR;
		    }

		  if (scan_cache->read_committed_page == true)
		    {
		      LOG_LSA *lsa, commit_lsa;

		      lsa = pgbuf_get_lsa (pgptr);
		      logtb_get_commit_lsa (&commit_lsa);
		      assert (!LSA_ISNULL (&commit_lsa));
		      if (LSA_GE (lsa, &commit_lsa))
			{
			  pgbuf_unfix_and_init (thread_p, pgptr);

			  thread_sleep (1);

			  if (retry_count > 1000)
			    {
			      TR_TABLE_LOCK (thread_p);
			      (void) logtb_commit_lsa (thread_p);
			      TR_TABLE_UNLOCK (thread_p);
			    }
			  retry_count++;
			  goto retry_page_fix;
			}
		    }

		  if (heap_scancache_update_hinted_when_lots_space (thread_p,
								    scan_cache,
								    pgptr)
		      != NO_ERROR)
		    {
		      pgbuf_unfix_and_init (thread_p, pgptr);
		      /* something went wrong, return */
		      scan_cache->pgptr = NULL;
		      return S_ERROR;
		    }
		}
	    }
	  else
	    {
	      pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid,
						   S_LOCK, NULL, PAGE_HEAP);
	      if (pgptr == NULL)
		{
		  if (er_errid () == ER_PB_BAD_PAGEID)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_HEAP_UNKNOWN_OBJECT, 3, oid.volid,
			      oid.pageid, oid.slotid);
		    }

		  /* something went wrong, return */
		  return S_ERROR;
		}
	    }

	  /*
	   * Find the next object. Skip relocated records (i.e., new_home
	   * records). This records must be accessed through the relocation
	   * record (i.e., the object).
	   */

	  while (((scan = spage_next_record (pgptr, &oid.slotid,
					     &forward_recdes,
					     PEEK)) == S_SUCCESS)
		 && (oid.slotid == HEAP_HEADER_AND_CHAIN_SLOTID
		     || ((type = spage_get_record_type (pgptr,
							oid.slotid)) ==
			 REC_NEWHOME)
		     || type == REC_ASSIGN_ADDRESS || type == REC_UNKNOWN))
	    {
	      ;			/* Nothing */
	    }

	  if (scan != S_SUCCESS)
	    {
	      if (scan == S_END)
		{
		  if (scan_cache != NULL
		      && VPID_EQ (&vpid, &scan_cache->last_vpid))
		    {
		      pgbuf_unfix_and_init (thread_p, pgptr);
		      OID_SET_NULL (next_oid);
		      return scan;
		    }

		  /* Find next page of heap */
		  (void) heap_vpid_next (hfid, pgptr, &vpid);
		  pgbuf_unfix_and_init (thread_p, pgptr);
		  oid.volid = vpid.volid;
		  oid.pageid = vpid.pageid;
		  oid.slotid = -1;
		  if (oid.pageid == NULL_PAGEID)
		    {
		      OID_SET_NULL (next_oid);
		      return scan;
		    }
		}
	      else
		{
		  pgbuf_unfix_and_init (thread_p, pgptr);
		  return scan;
		}
	    }
	  else
	    {
	      break;
	    }
	}

      /*
       * A RECORD was found
       * If the next record is a relocation record, get the new home of the
       * record
       */
      assert (type != REC_UNKNOWN);
      recdes->type = type;

      switch (type)
	{
	case REC_RELOCATION:
	  /*
	   * The record stored on the page is a relocation record, get the new
	   * home of the record
	   */
	  peek_oid = (OID *) forward_recdes.data;
	  forward_oid = *peek_oid;

	  pgbuf_unfix_and_init (thread_p, pgptr);

	  /* Fetch the page of relocated (forwarded/new home) record */
	  vpid.volid = forward_oid.volid;
	  vpid.pageid = forward_oid.pageid;

	  pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid,
					       S_LOCK, scan_cache, PAGE_HEAP);
	  if (pgptr == NULL)
	    {
	      /* something went wrong, return */
	      if (er_errid () == ER_PB_BAD_PAGEID)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HEAP_BAD_RELOCATION_RECORD, 3,
			  oid.volid, oid.pageid, oid.slotid);
		}
	      return S_ERROR;
	    }

#if defined(RYE_DEBUG)
	  if (spage_get_record_type (pgptr, forward_oid.slotid) !=
	      REC_NEWHOME)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_BAD_OBJECT_TYPE, 3, forward_oid.volid,
		      forward_oid.pageid, forward_oid.slotid);
	      pgbuf_unfix_and_init (thread_p, pgptr);
	      return S_ERROR;
	    }
#endif

	  if (scan_cache != NULL && ispeeking == COPY && recdes->data == NULL)
	    {
	      /* It is guaranteed that scan_cache is not NULL. */
	      if (scan_cache->area == NULL)
		{
		  /* Allocate an area to hold the object. Assume that
		     the object will fit in two pages for not better estimates.
		   */
		  scan_cache->area_size = DB_PAGESIZE * 2;
		  scan_cache->area = (char *) malloc (scan_cache->area_size);
		  if (scan_cache->area == NULL)
		    {
		      scan_cache->area_size = -1;
		      pgbuf_unfix_and_init (thread_p, pgptr);
		      return S_ERROR;
		    }
		}
	      recdes->data = scan_cache->area;
	      recdes->area_size = scan_cache->area_size;
	      /* The allocated space is enough to save the instance. */
	    }

	  if (scan_cache != NULL && scan_cache->cache_last_fix_page == true)
	    {
	      if (ispeeking == PEEK)
		{
		  scan = spage_get_record (pgptr, forward_oid.slotid, recdes,
					   PEEK);
		}
	      else
		{
		  scan = spage_get_record (pgptr, forward_oid.slotid, recdes,
					   COPY);
		}
	      /* Save the page for a future scan */
	      scan_cache->pgptr = pgptr;
	    }
	  else
	    {
	      scan = spage_get_record (pgptr, forward_oid.slotid, recdes,
				       COPY);
	      pgbuf_unfix_and_init (thread_p, pgptr);
	    }

	  break;

	case REC_BIGONE:
	  /* Get the address of the content of the multipage object */
	  peek_oid = (OID *) forward_recdes.data;
	  forward_oid = *peek_oid;
	  pgbuf_unfix_and_init (thread_p, pgptr);

	  /* Now get the content of the multipage object. */
	  /* Try to reuse the previously allocated area */
	  if (scan_cache != NULL
	      && (ispeeking == PEEK || recdes->data == NULL))
	    {
	      /* It is guaranteed that scan_cache is not NULL. */
	      if (scan_cache->area == NULL)
		{
		  /*
		   * Allocate an area to hold the object. Assume that the object
		   * will fit in two pages for not better estimates.
		   * We could call heap_ovf_get_length, but it may be better
		   * to just guess and realloc if needed. We could also check
		   * the estimates for average object length, but again,
		   * it may be expensive and may not be accurate for the object.
		   */
		  scan_cache->area_size = DB_PAGESIZE * 2;
		  scan_cache->area = (char *) malloc (scan_cache->area_size);
		  if (scan_cache->area == NULL)
		    {
		      scan_cache->area_size = -1;
		      return S_ERROR;
		    }
		}
	      recdes->data = scan_cache->area;
	      recdes->area_size = scan_cache->area_size;

	      while ((scan = heap_ovf_get (thread_p, &forward_oid,
					   recdes)) == S_DOESNT_FIT)
		{
		  /*
		   * The object did not fit into such an area, reallocate a new
		   * area
		   */
		  recdes->area_size = -recdes->length;
		  recdes->data =
		    (char *) realloc (scan_cache->area, recdes->area_size);
		  if (recdes->data == NULL)
		    {
		      return S_ERROR;
		    }
		  scan_cache->area_size = recdes->area_size;
		  scan_cache->area = recdes->data;
		}
	      if (scan != S_SUCCESS)
		{
		  recdes->data = NULL;
		}
	    }
	  else
	    {
	      scan = heap_ovf_get (thread_p, &forward_oid, recdes);
	    }

	  break;

	case REC_HOME:
	  if (scan_cache != NULL && ispeeking == COPY && recdes->data == NULL)
	    {
	      /* It is guaranteed that scan_cache is not NULL. */
	      if (scan_cache->area == NULL)
		{
		  /* Allocate an area to hold the object. Assume that
		   * the object will fit in two pages for not better estimates.
		   */
		  scan_cache->area_size = DB_PAGESIZE * 2;
		  scan_cache->area = (char *) malloc (scan_cache->area_size);
		  if (scan_cache->area == NULL)
		    {
		      scan_cache->area_size = -1;
		      pgbuf_unfix_and_init (thread_p, pgptr);
		      return S_ERROR;
		    }
		}
	      recdes->data = scan_cache->area;
	      recdes->area_size = scan_cache->area_size;
	      /* The allocated space is enough to save the instance. */
	    }

	  if (scan_cache != NULL && scan_cache->cache_last_fix_page == true)
	    {
	      if (ispeeking == PEEK)
		{
		  scan = spage_get_record (pgptr, oid.slotid, recdes, PEEK);
		}
	      else
		{
		  scan = spage_get_record (pgptr, oid.slotid, recdes, COPY);
		}
	      /* Save the page for a future scan */
	      scan_cache->pgptr = pgptr;
	    }
	  else
	    {
	      scan = spage_get_record (pgptr, oid.slotid, recdes, COPY);
	      pgbuf_unfix_and_init (thread_p, pgptr);
	    }

	  break;

	case REC_NEWHOME:
	case REC_MARKDELETED:
	case REC_DELETED_WILL_REUSE:
	default:
	  /* This should never happen */
	  scan = S_ERROR;
	  pgbuf_unfix_and_init (thread_p, pgptr);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_BAD_OBJECT_TYPE,
		  3, oid.volid, oid.pageid, oid.slotid);
	  break;
	}

      if (scan == S_SUCCESS)
	{
	  /*
	   * Make sure that the found object is an instance of the desired
	   * class. If it isn't then continue looking.
	   */
	  if (class_oid == NULL || OID_ISNULL (class_oid) ||
	      !OID_IS_ROOTOID (&oid))
	    {
	      /* get shard groupid
	       */
	      oid.groupid = or_grp_id (recdes);
	      assert (oid.groupid >= GLOBAL_GROUPID);

	      /* filter-out group id
	       */
	      if (!SHARD_GROUP_OWN (thread_p, oid.groupid))
		{
		  continue_looking = true;
		}
	      else
		{
		  *next_oid = oid;
		}
	    }
	  else
	    {
	      continue_looking = true;
	    }
	}
    }

  return scan;
}

/*
 * heap_first () - Retrieve or peek first object of heap
 *   return: SCAN_CODE (Either of S_SUCCESS, S_DOESNT_FIT, S_END, S_ERROR)
 *   hfid(in):
 *   class_oid(in):
 *   oid(in/out): Object identifier of current record.
 *                Will be set to first available record or NULL_OID when there
 *                is not one.
 *   recdes(in/out): Pointer to a record descriptor. Will be modified to
 *                   describe the new record.
 *   scan_cache(in/out): Scan cache or NULL
 *   ispeeking(in): PEEK when the object is peeked, scan_cache cannot be NULL
 *                  COPY when the object is copied
 *
 */
SCAN_CODE
heap_first (THREAD_ENTRY * thread_p, const HFID * hfid, OID * class_oid,
	    OID * oid, RECDES * recdes, HEAP_SCANCACHE * scan_cache,
	    int ispeeking)
{
  /* Retrieve the first record of the file */
  OID_SET_NULL (oid);
  oid->volid = hfid->vfid.volid;

  return heap_next (thread_p, hfid, class_oid, oid, recdes, scan_cache,
		    ispeeking);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * heap_last () - Retrieve or peek last object of heap
 *   return: SCAN_CODE
 *           (Either of S_SUCCESS, S_DOESNT_FIT, S_END,
 *                      S_ERROR)
 *   hfid(in):
 *   class_oid(in):
 *   oid(in/out): Object identifier of current record.
 *                Will be set to last available record or NULL_OID when there is
 *                not one.
 *   recdes(in/out): Pointer to a record descriptor. Will be modified to
 *                   describe the new record.
 *   scan_cache(in/out): Scan cache or NULL
 *   ispeeking(in): PEEK when the object is peeked, scan_cache cannot be NULL
 *                  COPY when the object is copied
 *
 */
SCAN_CODE
heap_last (THREAD_ENTRY * thread_p, const HFID * hfid, OID * class_oid,
	   OID * oid, RECDES * recdes, HEAP_SCANCACHE * scan_cache,
	   int ispeeking)
{
  /* Retrieve the first record of the file */
  OID_SET_NULL (oid);
  oid->volid = hfid->vfid.volid;

  return heap_prev (thread_p, hfid, class_oid, oid, recdes, scan_cache,
		    ispeeking);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * heap_get_alloc () - get/retrieve an object by allocating and freeing area
 *   return: NO_ERROR
 *   oid(in): Object identifier
 *   recdes(in): Record descriptor
 *
 * Note: The object associated with the given OID is copied into the
 * allocated area pointed to by the record descriptor. If the
 * object does not fit in such an area. The area is freed and a
 * new area is allocated to hold the object.
 * The caller is responsible from deallocating the area.
 *
 * Note: The area in the record descriptor is one dynamically allocated
 * with malloc and free with free_and_init.
 */
int
heap_get_alloc (THREAD_ENTRY * thread_p, const OID * oid, RECDES * recdes)
{
  SCAN_CODE scan;
  char *new_area;
  int ret = NO_ERROR;

  if (recdes->data == NULL)
    {
      recdes->area_size = DB_PAGESIZE;	/* assume that only one page is needed */
      recdes->data = (char *) malloc (recdes->area_size);
      if (recdes->data == NULL)
	{
	  ret = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 1,
		  recdes->area_size);
	  goto exit_on_error;
	}
    }

  /* Get the object */
  while ((scan = heap_get (thread_p, oid, recdes, NULL, COPY)) != S_SUCCESS)
    {
      if (scan == S_DOESNT_FIT)
	{
	  /* Is more space needed ? */
	  new_area = (char *) realloc (recdes->data, -(recdes->length));
	  if (new_area == NULL)
	    {
	      ret = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 1,
		      -(recdes->length));
	      goto exit_on_error;
	    }
	  recdes->area_size = -recdes->length;
	  recdes->data = new_area;
	}
      else
	{
	  goto exit_on_error;
	}
    }

  return ret;

exit_on_error:

  if (recdes->data != NULL)
    {
      free_and_init (recdes->data);
      recdes->area_size = 0;
    }

  return (ret == NO_ERROR) ? ER_FAILED : ret;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_cmp () - Compare heap object with current content
 *   return: int (> 0 recdes is larger,
 *                     < 0 recdes is smaller, and
 *                     = 0 same)
 *   oid(in): The object to compare
 *   recdes(in): Compare object against this content
 *
 * Note: Compare the heap object against given content in ASCII format.
 */
int
heap_cmp (THREAD_ENTRY * thread_p, const OID * oid, RECDES * recdes)
{
  HEAP_SCANCACHE scan_cache;
  RECDES peek_recdes = RECDES_INITIALIZER;
  int compare;

  heap_scancache_quick_start (&scan_cache);
  if (heap_get (thread_p, oid, &peek_recdes, &scan_cache, PEEK) != S_SUCCESS)
    {
      compare = 1;
    }
  else if (recdes->length > peek_recdes.length)
    {
      compare = memcmp (recdes->data, peek_recdes.data, peek_recdes.length);
      if (compare == 0)
	{
	  compare = 1;
	}
    }
  else
    {
      compare = memcmp (recdes->data, peek_recdes.data, recdes->length);
      if (compare == 0 && recdes->length != peek_recdes.length)
	{
	  compare = -1;
	}
    }

  heap_scancache_end (thread_p, &scan_cache);

  return compare;
}

/*
 * heap_does_exist () - Does object exist?
 *   return: true/false
 *   class_oid(in): Class identifier of object or NULL
 *   oid(in): Object identifier
 *
 * Note: Check if the object associated with the given OID exist.
 * If the class of the object does not exist, the object does not
 * exist either. If the class is not given or a NULL_OID is
 * passed, the function finds the class oid.
 */
bool
heap_does_exist (THREAD_ENTRY * thread_p, OID * class_oid, const OID * oid)
{
  VPID vpid;
  OID tmp_oid;
  PAGE_PTR pgptr = NULL;
  bool doesexist = true;
  INT16 rectype;
  bool old_check_interrupt;

  old_check_interrupt = thread_set_check_interrupt (thread_p, false);

  if (HEAP_ISVALID_OID (oid) != DISK_VALID)
    {
      doesexist = false;
      goto exit_on_end;
    }

  /*
   * If the class is not NULL and it is different from the Rootclass,
   * make sure that it exist. Rootclass always exist.. not need to check
   * for it
   */
  if (class_oid != NULL && !OID_EQ (class_oid, oid_Root_class_oid)
      && HEAP_ISVALID_OID (class_oid) != DISK_VALID)
    {
      doesexist = false;
      goto exit_on_end;
    }

  while (doesexist)
    {
      if (oid->slotid == HEAP_HEADER_AND_CHAIN_SLOTID || oid->slotid < 0
	  || oid->pageid < 0 || oid->volid < 0)
	{
	  doesexist = false;
	  goto exit_on_end;
	}

      vpid.volid = oid->volid;
      vpid.pageid = oid->pageid;

      /* Fetch the page where the record is stored */

      pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, OLD_PAGE, S_LOCK,
					   NULL, PAGE_HEAP);
      if (pgptr == NULL)
	{
	  if (er_errid () == ER_PB_BAD_PAGEID)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid, oid->pageid,
		      oid->slotid);
	    }

	  /* something went wrong, give up */
	  doesexist = false;
	  goto exit_on_end;
	}

      doesexist = spage_is_slot_exist (pgptr, oid->slotid);
      rectype = spage_get_record_type (pgptr, oid->slotid);
      pgbuf_unfix_and_init (thread_p, pgptr);

      /*
       * Check the class
       */

      if (doesexist && rectype != REC_ASSIGN_ADDRESS)
	{
	  if (class_oid == NULL)
	    {
	      class_oid = &tmp_oid;
	      OID_SET_NULL (class_oid);
	    }

	  if (OID_ISNULL (class_oid))
	    {
	      /*
	       * Caller does not know the class of the object. Get the class
	       * identifier from disk
	       */
	      if (heap_get_class_oid (thread_p, class_oid, oid) == NULL)
		{
		  doesexist = false;
		  goto exit_on_end;
		}
	    }

	  /* If doesexist is true, then check its class */
	  if (!OID_IS_ROOTOID (class_oid))
	    {
	      /*
	       * Make sure that the class exist too. Loop with this
	       */
	      oid = class_oid;
	      class_oid = oid_Root_class_oid;
	    }
	  else
	    {
	      break;
	    }
	}
      else
	{
	  break;
	}
    }

exit_on_end:

  (void) thread_set_check_interrupt (thread_p, old_check_interrupt);

  return doesexist;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * heap_get_num_objects () - Count the number of objects
 *   return: NO_ERROR or error code
 *   hfid(in): Object heap file identifier
 *   nobjs(in):
 *
 * Note: Count the number of objects stored on the given heap.
 * This function is expensive since all pages of the heap are
 * fetched to find the number of objects.
 */
int
heap_get_num_objects (THREAD_ENTRY * thread_p, const HFID * hfid,
		      DB_BIGINT * nobjs)
{
  int error_code = NO_ERROR;

  /*
   * Get the heap header in exclusive mode and call the synchronization to
   * update the statistics of the heap. The number of record/objects is
   * updated.
   */

  *nobjs = 0;

  error_code = heap_bestspace_sync (thread_p, nobjs, hfid, true);

  return error_code;
}

/*
 * heap_estimate_num_objects () - Estimate the number of objects
 *   return: NO_ERROR or error code
 *   hfid(in): Object heap file identifier
 *   num_objects(out):
 *
 * Note: Estimate the number of objects stored on the given heap.
 */
int
heap_estimate_num_objects (THREAD_ENTRY * thread_p, const HFID * hfid,
			   DB_BIGINT * num_objects)
{
  VPID vpid;			/* Page-volume identifier            */
  PAGE_PTR hdr_pgptr = NULL;	/* Page pointer                      */
  RECDES hdr_recdes = RECDES_INITIALIZER;	/* Record descriptor to point to space
						 * statistics
						 */
  int npages = 0, nrecords = 0, rec_length = 0;

  *num_objects = 0;

  vpid.volid = hfid->vfid.volid;
  vpid.pageid = hfid->hpgid;

  /* TODO:[happy] random sampling */

  hdr_pgptr = heap_pgbuf_fix (thread_p, hfid, &vpid,
			      PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
			      PAGE_HEAP_HEADER);
  if (hdr_pgptr == NULL)
    {
      /* something went wrong. Unable to fetch header page */
      return ER_FAILED;
    }

  if (spage_get_record (hdr_pgptr, HEAP_HEADER_AND_CHAIN_SLOTID, &hdr_recdes,
			PEEK) != S_SUCCESS)
    {
      pgbuf_unfix_and_init (thread_p, hdr_pgptr);
      return ER_FAILED;
    }

  spage_collect_statistics (hdr_pgptr, &npages, &nrecords, &rec_length);

  pgbuf_unfix_and_init (thread_p, hdr_pgptr);

  npages = file_get_numpages (thread_p, &hfid->vfid, NULL, NULL, NULL);

  *num_objects = ((DB_BIGINT) nrecords) * npages;

#if 1				/* TODO - */
  if (*num_objects < 0)
    {
      *num_objects = DB_BIGINT_MAX;	/* truncate */
    }
#endif

  *num_objects = MAX (*num_objects, 1);
  assert (*num_objects >= 1);

  return NO_ERROR;
}

/*
 * heap_estimate_avg_length () - Estimate the average length of records
 *   return: avg length
 *   hfid(in): Object heap file identifier
 *
 * Note: Estimate the avergae length of the objects stored on the heap.
 * This function is mainly used when we are creating the OID of
 * an object of which we do not know its length.
 */
static int
heap_estimate_avg_length (UNUSED_ARG THREAD_ENTRY * thread_p,
			  UNUSED_ARG const HFID * hfid)
{
  /* TODO:[happy] random sampling */

  return HEAP_GUESS_MAX_REC_SIZE;
}

/*
 * heap_get_capacity () - Find space consumed by heap
 *   return: NO_ERROR
 *   hfid(in): Object heap file identifier
 *   num_recs(in/out): Total Number of objects
 *   num_recs_relocated(in/out):
 *   num_recs_inovf(in/out):
 *   num_pages(in/out): Total number of heap pages
 *   avg_freespace(in/out): Average free space per page
 *   avg_freespace_nolast(in/out): Average free space per page without taking in
 *                                 consideration last page
 *   avg_reclength(in/out): Average object length
 *   avg_overhead(in/out): Average overhead per page
 *
 * Note: Find the current storage facts/capacity for given heap.
 */
static int
heap_get_capacity (THREAD_ENTRY * thread_p, const HFID * hfid,
		   INT64 * num_recs, INT64 * num_recs_relocated,
		   INT64 * num_recs_inovf, INT64 * num_pages,
		   int *avg_freespace, int *avg_freespace_nolast,
		   int *avg_reclength, int *avg_overhead)
{
  VPID vpid;			/* Page-volume identifier            */
  PAGE_PTR pgptr = NULL;	/* Page pointer to header page       */
  RECDES recdes = RECDES_INITIALIZER;	/* Header record descriptor          */
  INT16 slotid;			/* Slot of one object                */
  OID *ovf_oid;
  int last_freespace;
  int ovf_len;
  int ovf_num_pages;
  int ovf_free_space;
  int ovf_overhead;
  int j;
  INT16 type = REC_UNKNOWN;
  int ret = NO_ERROR;
  INT64 sum_freespace = 0;
  INT64 sum_reclength = 0;
  INT64 sum_overhead = 0;
  PAGE_TYPE ptype;

  *num_recs = 0;
  *num_pages = 0;
  *avg_freespace = 0;
  *avg_reclength = 0;
  *avg_overhead = 0;
  *num_recs_relocated = 0;
  *num_recs_inovf = 0;
  last_freespace = 0;

  vpid.volid = hfid->vfid.volid;
  vpid.pageid = hfid->hpgid;
  ptype = PAGE_HEAP_HEADER;

  while (!VPID_ISNULL (&vpid))
    {
      pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, S_LOCK,
					   NULL, ptype);
      if (pgptr == NULL)
	{
	  /* something went wrong, return error */
	  goto exit_on_error;
	}

      slotid = -1;
      j = spage_number_of_records (pgptr);

      last_freespace = spage_get_free_space (thread_p, pgptr);

      *num_pages += 1;
      sum_freespace += last_freespace;
      sum_overhead += j * spage_slot_size ();

      while ((j--) > 0)
	{
	  if (spage_next_record (pgptr, &slotid, &recdes, PEEK) == S_SUCCESS)
	    {
	      if (slotid != HEAP_HEADER_AND_CHAIN_SLOTID)
		{
		  type = spage_get_record_type (pgptr, slotid);
		  switch (type)
		    {
		    case REC_RELOCATION:
		      *num_recs_relocated += 1;
		      sum_overhead += spage_get_record_length (pgptr, slotid);
		      break;
		    case REC_ASSIGN_ADDRESS:
		    case REC_HOME:
		    case REC_NEWHOME:
		      /*
		       * Note: for newhome (relocated), we are including the length
		       *       and number of records. In the relocation record (above)
		       *       we are just adding the overhead and number of
		       *       reclocation records.
		       *       for assign address, we assume the given size.
		       */
		      *num_recs += 1;
		      sum_reclength += spage_get_record_length (pgptr,
								slotid);
		      break;
		    case REC_BIGONE:
		      *num_recs += 1;
		      *num_recs_inovf += 1;
		      sum_overhead += spage_get_record_length (pgptr, slotid);

		      ovf_oid = (OID *) recdes.data;
		      if (heap_ovf_get_capacity (thread_p, ovf_oid, &ovf_len,
						 &ovf_num_pages,
						 &ovf_overhead,
						 &ovf_free_space) == NO_ERROR)
			{
			  sum_reclength += ovf_len;
			  *num_pages += ovf_num_pages;
			  sum_freespace += ovf_free_space;
			  sum_overhead += ovf_overhead;
			}
		      break;
		    case REC_MARKDELETED:
		      /*
		       * TODO Find out and document here why this is added to
		       * the overhead. The record has been deleted so its
		       * length should no longer have any meaning. Perhaps
		       * the length of the slot should have been added instead?
		       */
		      sum_overhead += spage_get_record_length (pgptr, slotid);
		      break;
		    case REC_DELETED_WILL_REUSE:
		    default:
		      break;
		    }
		}
	    }
	}
      (void) heap_vpid_next (hfid, pgptr, &vpid);
      pgbuf_unfix_and_init (thread_p, pgptr);

      ptype = PAGE_HEAP;
    }

  if (*num_pages > 0)
    {
      /*
       * Don't take in consideration the last page for free space
       * considerations since the average free space will be contaminated.
       */
      *avg_freespace_nolast = ((*num_pages > 1)
			       ? (int) ((sum_freespace - last_freespace) /
					(*num_pages - 1)) : 0);
      *avg_freespace = (int) (sum_freespace / *num_pages);
      *avg_overhead = (int) (sum_overhead / *num_pages);
    }

  if (*num_recs != 0)
    {
      *avg_reclength = (int) (sum_reclength / *num_recs);
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_get_class_oid () - Find class oid of given instance
 *   return: OID *(class_oid on success and NULL on failure)
 *   class_oid(out): The Class oid of the instance
 *   oid(in): The Object identifier of the instance
 *
 * Note: Find the class identifier of the given instance.
 */
OID *
heap_get_class_oid (THREAD_ENTRY * thread_p, OID * class_oid, const OID * oid)
{
  RECDES recdes = RECDES_INITIALIZER;
  HEAP_SCANCACHE scan_cache;
  DISK_ISVALID oid_valid;

  if (class_oid == NULL)
    {
      assert (false);
      return NULL;
    }

  heap_scancache_quick_start (&scan_cache);
  if (heap_get_with_class_oid (thread_p, class_oid, oid, &recdes,
			       &scan_cache, PEEK) != S_SUCCESS)
    {
      OID_SET_NULL (class_oid);
      class_oid = NULL;
    }
  else
    {
      oid_valid = HEAP_ISVALID_OID (class_oid);
      if (oid_valid != DISK_VALID)
	{
	  if (oid_valid != DISK_ERROR)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_UNKNOWN_OBJECT, 3, oid->volid, oid->pageid,
		      oid->slotid);
	    }
	  OID_SET_NULL (class_oid);
	  class_oid = NULL;
	}
    }

  heap_scancache_end (thread_p, &scan_cache);

  return class_oid;
}

/*
 * heap_get_class_name () - Find classname when oid is a class
 *   return: Classname or NULL. The classname space must be
 *           released by the caller.
 *   class_oid(in): The Class Object identifier
 *
 * Note: Find the name of the given class identifier. If the passed OID
 * is not a class, it return NULL.
 *
 * Note: Classname pointer must be released by the caller using free_and_init
 */
char *
heap_get_class_name (THREAD_ENTRY * thread_p, const OID * class_oid)
{
  return heap_get_class_name_alloc_if_diff (thread_p, class_oid, NULL);
}

/*
 * heap_get_class_name_alloc_if_diff () - Get the name of given class
 *                               name is malloc when different than given name
 *   return: guess_classname when it is the real name. Don't need to free.
 *           malloc classname when different from guess_classname.
 *           Must be free by caller (free_and_init)
 *           NULL some kind of error
 *   class_oid(in): The Class Object identifier
 *   guess_classname(in): Guess name of class
 *
 * Note: Find the name of the given class identifier. If the name is
 * the same as the guessed name, the guessed name is returned.
 * Otherwise, an allocated area with the name of the class is
 * returned. If an error is found or the passed OID is not a
 * class, NULL is returned.
 */
char *
heap_get_class_name_alloc_if_diff (THREAD_ENTRY * thread_p,
				   const OID * class_oid,
				   char *guess_classname)
{
  char *classname = NULL;
  char *copy_classname = NULL;
  RECDES recdes = RECDES_INITIALIZER;
  HEAP_SCANCACHE scan_cache;
  OID root_oid;

  OID_SET_NULL (&root_oid);	/* init */

  heap_scancache_quick_start (&scan_cache);
  if (heap_get_with_class_oid (thread_p, &root_oid, class_oid, &recdes,
			       &scan_cache, PEEK) == S_SUCCESS)
    {
      /* Make sure that this is a class */
      if (oid_is_root (&root_oid))
	{
	  classname = or_class_name (&recdes);
	  if (guess_classname == NULL
	      || strcmp (guess_classname, classname) != 0)
	    {
	      /*
	       * The names are different.. return a copy that must be freed.
	       */
	      copy_classname = strdup (classname);
	      if (copy_classname == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1,
			  (strlen (classname) + 1) * sizeof (char));
		}
	    }
	  else
	    {
	      /*
	       * The classnames are identical
	       */
	      copy_classname = guess_classname;
	    }
	}
    }
  else
    {
      if (er_errid () == ER_HEAP_NODATA_NEWADDRESS)
	{
	  er_clear ();		/* clear ER_HEAP_NODATA_NEWADDRESS */
	}
    }

  heap_scancache_end (thread_p, &scan_cache);

  return copy_classname;
}

/*
 * heap_get_class_name_of_instance () - Find classname of given instance
 *   return: Classname or NULL. The classname space must be
 *           released by the caller.
 *   inst_oid(in): The instance object identifier
 *
 * Note: Find the class name of the class of given instance identifier.
 *
 * Note: Classname pointer must be released by the caller using free_and_init
 */
char *
heap_get_class_name_of_instance (THREAD_ENTRY * thread_p,
				 const OID * inst_oid)
{
  char *classname = NULL;
  char *copy_classname = NULL;
  RECDES recdes = RECDES_INITIALIZER;
  HEAP_SCANCACHE scan_cache;
  OID class_oid;

  heap_scancache_quick_start (&scan_cache);
  if (heap_get_with_class_oid (thread_p, &class_oid, inst_oid, &recdes,
			       &scan_cache, PEEK) == S_SUCCESS)
    {
      if (heap_get (thread_p, &class_oid, &recdes, &scan_cache, PEEK) ==
	  S_SUCCESS)
	{
	  classname = or_class_name (&recdes);
	  copy_classname = (char *) malloc (strlen (classname) + 1);
	  if (copy_classname == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, strlen (classname) + 1);
	    }
	  else
	    {
	      strcpy (copy_classname, classname);
	    }
	}
    }

  heap_scancache_end (thread_p, &scan_cache);

  return copy_classname;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_get_class_name_with_is_class () - Find if object is a class.
 * if a class, returns its name, otherwise, get the name of its class
 *   return: Classname or NULL. The classname space must be
 *           released by the caller.
 *   oid(in): The Object identifier
 *   isclass(in/out): Set to true is object is a class, otherwise is set to
 *                    false
 *
 * Note: Find if the object associated with given oid is a class.
 * If the object is a class, returns its name, otherwise, returns
 * the name of its class.
 *
 * If the object does not exist or there is another error, NULL
 * is returned as the classname.
 *
 * Note: Classname pointer must be released by the caller using free_and_init
 */
char *
heap_get_class_name_with_is_class (THREAD_ENTRY * thread_p, const OID * oid,
				   int *isclass)
{
  char *classname = NULL;
  char *copy_classname = NULL;
  RECDES recdes = RECDES_INITIALIZER;
  HEAP_SCANCACHE scan_cache;
  OID class_oid;

  *isclass = false;

  heap_scancache_quick_start (&scan_cache);
  if (heap_get_with_class_oid (thread_p, &class_oid, oid, &recdes,
			       &scan_cache, PEEK) == S_SUCCESS)
    {
      /*
       * If oid is a class, get its name, otherwise, get the name of its class
       */
      *isclass = OID_IS_ROOTOID (&class_oid);
      if (heap_get (thread_p, ((*isclass == true) ? oid : &class_oid),
		    &recdes, &scan_cache, PEEK) == S_SUCCESS)
	{
	  classname = or_class_name (&recdes);
	  copy_classname = (char *) malloc (strlen (classname) + 1);
	  if (copy_classname == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, strlen (classname) + 1);
	    }
	  else
	    {
	      strcpy (copy_classname, classname);
	    }
	}
    }

  heap_scancache_end (thread_p, &scan_cache);

  return copy_classname;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * heap_attrinfo_start () - Initialize an attribute information structure
 *   return: NO_ERROR
 *   class_oid(in): The class identifier of the instances where values
 *                  attributes values are going to be read.
 *   requested_num_attrs(in): Number of requested attributes
 *                            If <=0 are given, it means interested on ALL.
 *   attrids(in): Array of requested attributes
 *   attr_info(in/out): The attribute information structure
 *
 * Note: Initialize an attribute information structure, so that values
 * of instances can be retrieved based on the desired attributes.
 * If the requested number of attributes is less than zero,
 * all attributes will be assumed instead. In this case
 * the attrids array should be NULL.
 *
 * The attrinfo structure is an structure where values of
 * instances can be read. For example an object is retrieved,
 * then some of its attributes are convereted to dbvalues and
 * placed in this structure.
 *
 * Note: The caller must call heap_attrinfo_end after he is done with
 * attribute information.
 */
int
heap_attrinfo_start (THREAD_ENTRY * thread_p, const OID * class_oid,
		     int requested_num_attrs, const ATTR_ID * attrids,
		     HEAP_CACHE_ATTRINFO * attr_info)
{
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  int getall;			/* Want all attribute values */
  int i;
  int ret = NO_ERROR;

  if (requested_num_attrs == 0)
    {
      /* initialize the attrinfo cache and return, there is nothing else to do */
      (void) memset (attr_info, '\0', sizeof (HEAP_CACHE_ATTRINFO));

      /* now set the num_values to -1 which indicates that this is an
       * empty HEAP_CACHE_ATTRINFO and shouldn't be operated on.
       */
      attr_info->num_values = -1;
      return NO_ERROR;
    }

  if (requested_num_attrs < 0)
    {
      getall = true;
    }
  else
    {
      getall = false;
    }

  /*
   * initialize attribute information
   *
   */

  attr_info->class_oid = *class_oid;
  attr_info->last_cacheindex = -1;
  attr_info->read_cacheindex = -1;

  attr_info->last_classrepr = NULL;
  attr_info->read_classrepr = NULL;

  OID_SET_NULL (&attr_info->inst_oid);
  attr_info->values = NULL;
  attr_info->num_values = -1;	/* initialize attr_info */

  /*
   * Find the most recent representation of the instances of the class, and
   * cache the structure that describe the attributes of this representation.
   * At the same time find the default values of attributes.
   */

  attr_info->last_classrepr = heap_classrepr_get (thread_p,
						  &attr_info->class_oid, NULL,
						  0,
						  &attr_info->last_cacheindex,
						  true);
  if (attr_info->last_classrepr == NULL)
    {
      goto exit_on_error;
    }

  /*
   * If the requested attributes is < 0, get all attributes of the last
   * representation.
   */

  if (requested_num_attrs < 0)
    {
      requested_num_attrs = attr_info->last_classrepr->n_attributes;
    }
  else if (requested_num_attrs > attr_info->last_classrepr->n_attributes)
    {
      fprintf (stdout, " XXX There are not that many attributes."
	       " Num_attrs = %d, Num_requested_attrs = %d\n",
	       attr_info->last_classrepr->n_attributes, requested_num_attrs);
      requested_num_attrs = attr_info->last_classrepr->n_attributes;
    }

  if (requested_num_attrs > 0)
    {
      attr_info->values =
	(HEAP_ATTRVALUE *) malloc (requested_num_attrs *
				   sizeof (*(attr_info->values)));
      if (attr_info->values == NULL)
	{
	  goto exit_on_error;
	}
    }
  else
    {
      attr_info->values = NULL;
    }

  attr_info->num_values = requested_num_attrs;

  /*
   * Set the attribute identifier of the desired attributes in the value
   * attribute information, and indicates that the current value is
   * unitialized. That is, it has not been read, set or whatever.
   */

  for (i = 0; i < attr_info->num_values; i++)
    {
      value = &attr_info->values[i];
      if (getall == true)
	{
	  value->attrid = -1;
	}
      else
	{
	  value->attrid = *attrids++;
	}
      value->state = HEAP_UNINIT_ATTRVALUE;
      value->last_attrepr = NULL;
      value->read_attrepr = NULL;
    }

  /*
   * Make last information to be recached for each individual attribute
   * value. Needed for WRITE and Default values
   */

  if (heap_attrinfo_recache_attrepr (attr_info, true) != NO_ERROR)
    {
      goto exit_on_error;
    }

  return ret;

exit_on_error:

  heap_attrinfo_end (thread_p, attr_info);

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_attrinfo_recache_attrepr () - Recache attribute information for given attrinfo for
 *                     each attribute value
 *   return: NO_ERROR
 *   attr_info(in/out): The attribute information structure
 *   islast_reset(in): Are we resetting information for last representation.
 *
 * Note: Recache the attribute information for given representation
 * identifier of the class in attr_info for each attribute value.
 * That is, set each attribute information to point to disk
 * related attribute information for given representation
 * identifier.
 * When we are resetting information for last representation,
 * attribute values are also initialized.
 */

static int
heap_attrinfo_recache_attrepr (HEAP_CACHE_ATTRINFO * attr_info,
			       int islast_reset)
{
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  int num_found_attrs;		/* Num of found attributes                 */
  int srch_num_attrs;		/* Num of attributes that can be searched  */
  OR_ATTRIBUTE *search_attrepr;	/* Information for disk attribute          */
  int i, curr_attr;
  bool isattr_found;
  int ret = NO_ERROR;

  /*
   * Initialize the value domain for dbvalues of all desired attributes
   */
  if (islast_reset == true)
    {
      srch_num_attrs = attr_info->last_classrepr->n_attributes;
    }
  else
    {
      srch_num_attrs = attr_info->read_classrepr->n_attributes;
    }

  for (num_found_attrs = 0, curr_attr = 0;
       curr_attr < attr_info->num_values; curr_attr++)
    {
      /*
       * Go over the list of attributes (instance, class attrs)
       * until the desired attribute is found
       */
      isattr_found = false;
      if (islast_reset == true)
	{
	  search_attrepr = attr_info->last_classrepr->attributes;
	}
      else
	{
	  search_attrepr = attr_info->read_classrepr->attributes;
	}

      value = &attr_info->values[curr_attr];

      if (value->attrid == -1)
	{
	  /* Case that we want all attributes */
	  value->attrid = search_attrepr[curr_attr].id;
	}

      for (i = 0;
	   isattr_found == false && i < srch_num_attrs; i++, search_attrepr++)
	{
	  /*
	   * Is this a desired instance attribute?
	   */
	  if (value->attrid == search_attrepr->id)
	    {
	      /*
	       * Found it.
	       * Initialize the attribute value information
	       */
	      isattr_found = true;
	      if (islast_reset == true)
		{
		  value->last_attrepr = search_attrepr;
		  /*
		   * The server does not work with DB_TYPE_OBJECT but DB_TYPE_OID
		   */
		  if (value->last_attrepr->type == DB_TYPE_OBJECT)
		    {
		      value->last_attrepr->type = DB_TYPE_OID;
		    }

		  if (value->state == HEAP_UNINIT_ATTRVALUE)
		    {
		      db_value_domain_init (&value->dbvalue,
					    value->last_attrepr->type,
					    value->last_attrepr->domain->
					    precision,
					    value->last_attrepr->domain->
					    scale);
		    }
		}
	      else
		{
		  value->read_attrepr = search_attrepr;
		  /*
		   * The server does not work with DB_TYPE_OBJECT but DB_TYPE_OID
		   */
		  if (value->read_attrepr->type == DB_TYPE_OBJECT)
		    {
		      value->read_attrepr->type = DB_TYPE_OID;
		    }
		}

	      num_found_attrs++;
	    }
	}

    }

  if (num_found_attrs != attr_info->num_values && islast_reset == true)
    {
      ret = ER_HEAP_UNKNOWN_ATTRS;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ret, 1,
	      attr_info->num_values - num_found_attrs);
      goto exit_on_error;
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_attrinfo_recache () - Recache attribute information for given attrinfo
 *   return: NO_ERROR
 *   reprid(in): Cache this class representation
 *   attr_info(in/out): The attribute information structure
 *
 * Note: Recache the attribute information for given representation
 * identifier of the class in attr_info. That is, set each
 * attribute information to point to disk related attribute
 * information for given representation identifier.
 */
static int
heap_attrinfo_recache (THREAD_ENTRY * thread_p, REPR_ID reprid,
		       HEAP_CACHE_ATTRINFO * attr_info)
{
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  int i;
  int ret = NO_ERROR;

  /*
   * If we do not need to cache anything (case of only clear values and
   * disk repr structure).. return
   */

  if (attr_info->read_classrepr != NULL)
    {
      if (attr_info->read_classrepr->id == reprid)
	{
	  return NO_ERROR;
	}

      /*
       * Do we need to free the current cached disk representation ?
       */
      if (attr_info->read_classrepr != attr_info->last_classrepr)
	{
	  ret = heap_classrepr_free (attr_info->read_classrepr,
				     &attr_info->read_cacheindex);
	  if (ret != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}
      attr_info->read_classrepr = NULL;
    }

  if (reprid == NULL_REPRID)
    {
      return NO_ERROR;
    }

  if (reprid == attr_info->last_classrepr->id)
    {
      /*
       * Take a short cut
       */
      if (attr_info->values != NULL)
	{
	  for (i = 0; i < attr_info->num_values; i++)
	    {
	      value = &attr_info->values[i];
	      value->read_attrepr = value->last_attrepr;
	    }
	}
      attr_info->read_classrepr = attr_info->last_classrepr;
      attr_info->read_cacheindex = -1;	/* Don't need to free this one */
      return NO_ERROR;
    }

  /*
   * Cache the desired class representation information
   */
  if (attr_info->values != NULL)
    {
      for (i = 0; i < attr_info->num_values; i++)
	{
	  value = &attr_info->values[i];
	  value->read_attrepr = NULL;
	}
    }
  attr_info->read_classrepr =
    heap_classrepr_get (thread_p, &attr_info->class_oid, NULL, reprid,
			&attr_info->read_cacheindex, false);
  if (attr_info->read_classrepr == NULL)
    {
      goto exit_on_error;
    }

  if (heap_attrinfo_recache_attrepr (attr_info, false) != NO_ERROR)
    {
      (void) heap_classrepr_free (attr_info->read_classrepr,
				  &attr_info->read_cacheindex);
      attr_info->read_classrepr = NULL;

      goto exit_on_error;
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_attrinfo_end () - Done with attribute information structure
 *   return: void
 *   attr_info(in/out): The attribute information structure
 *
 * Note: Release any memory allocated for attribute information related
 * reading of instances.
 */
void
heap_attrinfo_end (THREAD_ENTRY * thread_p, HEAP_CACHE_ATTRINFO * attr_info)
{
  UNUSED_VAR int ret = NO_ERROR;

  /* check to make sure the attr_info has been used */
  if (attr_info->num_values == -1)
    {
      return;
    }

  /*
   * Free any attribute and class representation information
   */
  ret = heap_attrinfo_clear_dbvalues (attr_info);
  ret = heap_attrinfo_recache (thread_p, NULL_REPRID, attr_info);

  if (attr_info->last_classrepr != NULL)
    {
      ret = heap_classrepr_free (attr_info->last_classrepr,
				 &attr_info->last_cacheindex);
      attr_info->last_classrepr = NULL;
    }

  if (attr_info->values)
    {
      free_and_init (attr_info->values);
    }
  OID_SET_NULL (&attr_info->class_oid);

  /*
   * Bash this so that we ensure that heap_attrinfo_end is idempotent.
   */
  attr_info->num_values = -1;

}

/*
 * heap_attrinfo_clear_dbvalues () - Clear current dbvalues of attribute
 *                                 information
 *   return: NO_ERROR
 *   attr_info(in/out): The attribute information structure
 *
 * Note: Clear any current dbvalues associated with attribute information.
 */
int
heap_attrinfo_clear_dbvalues (HEAP_CACHE_ATTRINFO * attr_info)
{
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  OR_ATTRIBUTE *attrepr;	/* Which one current repr of default one      */
  int i;
  int ret = NO_ERROR;

  /* check to make sure the attr_info has been used */
  if (attr_info->num_values == -1)
    {
      return NO_ERROR;
    }

  if (attr_info->values != NULL)
    {
      for (i = 0; i < attr_info->num_values; i++)
	{
	  value = &attr_info->values[i];
	  if (value->state != HEAP_UNINIT_ATTRVALUE)
	    {
	      /*
	       * Was the value set up from a default value or from a representation
	       * of the object
	       */
	      attrepr = ((value->read_attrepr != NULL)
			 ? value->read_attrepr : value->last_attrepr);
	      if (attrepr != NULL)
		{
		  if (pr_clear_value (&value->dbvalue) != NO_ERROR)
		    {
		      ret = ER_FAILED;
		    }
		  value->state = HEAP_UNINIT_ATTRVALUE;
		}
	    }
	}
    }
  OID_SET_NULL (&attr_info->inst_oid);

  return ret;
}

/*
 * heap_attrvalue_read () - Read attribute information of given attribute cache
 *                        and instance
 *   return: NO_ERROR
 *   recdes(in): Instance record descriptor
 *   value(in): Disk value attribute information
 *   attr_info(in/out): The attribute information structure
 *
 * Note: Read the dbvalue of the given value attribute information.
 */
static int
heap_attrvalue_read (RECDES * recdes, HEAP_ATTRVALUE * value,
		     HEAP_CACHE_ATTRINFO * attr_info)
{
  OR_BUF buf;
  PR_TYPE *pr_type;		/* Primitive type array function structure */
  OR_ATTRIBUTE *volatile attrepr;
  char *disk_data = NULL;
  int disk_bound = false;
  volatile int disk_length = -1;
  int ret = NO_ERROR;

  /* Initialize disk value information */
  disk_data = NULL;
  disk_bound = false;
  disk_length = -1;

  /*
   * Does attribute exist in this disk representation?
   */

  if (recdes == NULL || recdes->data == NULL || value->read_attrepr == NULL)
    {
      /*
       * Either the attribute is a class attr, or the attribute
       * does not exist in this disk representation, or we do not have
       * the disk object (recdes), get default value if any...
       */
      attrepr = value->last_attrepr;
      disk_length = value->last_attrepr->default_value.val_length;
      if (disk_length > 0)
	{
	  disk_data = (char *) value->last_attrepr->default_value.value;
	  disk_bound = true;
	}
    }
  else
    {
      attrepr = value->read_attrepr;
      /* Is it a fixed size attribute ? */
      if (value->read_attrepr->is_fixed != 0)
	{
	  /*
	   * A fixed attribute.
	   */
	  if (!OR_FIXED_ATT_IS_UNBOUND (recdes->data,
					attr_info->read_classrepr->n_variable,
					attr_info->read_classrepr->
					fixed_length,
					value->read_attrepr->position))
	    {
	      /*
	       * The fixed attribute is bound. Access its information
	       */
	      disk_data =
		((char *) recdes->data +
		 OR_FIXED_ATTRIBUTES_OFFSET_BY_OBJ (recdes->data,
						    attr_info->
						    read_classrepr->
						    n_variable) +
		 value->read_attrepr->location);
	      disk_length = tp_domain_disk_size (value->read_attrepr->domain);
	      disk_bound = true;
	    }
	}
      else
	{
	  /*
	   * A variable attribute
	   */
	  if (!OR_VAR_IS_NULL (recdes->data, value->read_attrepr->location))
	    {
	      /*
	       * The variable attribute is bound.
	       * Find its location through the variable offset attribute table.
	       */
	      disk_data = ((char *) recdes->data +
			   OR_VAR_OFFSET (recdes->data,
					  value->read_attrepr->location));

	      disk_bound = true;
	      switch (TP_DOMAIN_TYPE (attrepr->domain))
		{
		case DB_TYPE_SEQUENCE:
		  OR_VAR_LENGTH (disk_length, recdes->data,
				 value->read_attrepr->location,
				 attr_info->read_classrepr->n_variable);
		  break;
		default:
		  disk_length = -1;	/* remains can read without disk_length */
		}
	    }
	}
    }

  /*
   * From now on, I should only use attrepr.. it will point to either
   * a current value or a default one
   */

  /*
   * Clear/decache any old value
   */
  if (value->state != HEAP_UNINIT_ATTRVALUE)
    {
      (void) pr_clear_value (&value->dbvalue);
    }


  /*
   * Now make the dbvalue according to the disk data value
   */

  if (disk_data == NULL || disk_bound == false)
    {
      /* Unbound attribute, set it to null value */
      ret = db_value_domain_init (&value->dbvalue, attrepr->type,
				  attrepr->domain->precision,
				  attrepr->domain->scale);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}
      value->state = HEAP_READ_ATTRVALUE;
    }
  else
    {
      /*
       * Read the value according to disk information that was found
       */
      OR_BUF_INIT2 (buf, disk_data, disk_length);
      buf.error_abort = 1;

      switch (_setjmp (buf.env))
	{
	case 0:
	  /* Do not copy the string--just use the pointer.  The pr_ routines
	   * for strings and sets have different semantics for length.
	   * A negative length value for strings means "don't copy the string,
	   * just use the pointer".
	   * For sets, don't translate the set into memory representation
	   * at this time.  It will only be translated when needed.
	   */
	  pr_type = PR_TYPE_FROM_ID (attrepr->type);
	  if (pr_type)
	    {
	      (*(pr_type->data_readval)) (&buf, &value->dbvalue,
					  attrepr->domain, disk_length,
					  false);
	    }
	  value->state = HEAP_READ_ATTRVALUE;
	  break;
	default:
	  /*
	   * An error was found during the reading of the attribute value
	   */
	  (void) db_value_domain_init (&value->dbvalue, attrepr->type,
				       attrepr->domain->precision,
				       attrepr->domain->scale);
	  value->state = HEAP_UNINIT_ATTRVALUE;
	  ret = ER_FAILED;
	  break;
	}
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_idxkey_get_value () -
 *   return:
 *   recdes(in):
 *   att(in):
 *   value(out):
 *   attr_info(in):
 */
static int
heap_idxkey_get_value (RECDES * recdes, OR_ATTRIBUTE * att,
		       DB_VALUE * value, HEAP_CACHE_ATTRINFO * attr_info)
{
  char *disk_data = NULL;
  bool found = true;		/* Does attribute(att) exist in
				   this disk representation? */
  int i;

  /* Initialize disk value information */
  disk_data = NULL;
  db_make_null (value);

  if (recdes != NULL && recdes->data != NULL && att != NULL)
    {
      if (or_rep_id (recdes) != attr_info->last_classrepr->id)
	{
	  found = false;
	  for (i = 0; i < attr_info->read_classrepr->n_attributes; i++)
	    {
	      if (attr_info->read_classrepr->attributes[i].id == att->id)
		{
		  att = &attr_info->read_classrepr->attributes[i];
		  found = true;
		  break;
		}
	    }
	}

      if (found == false)
	{
	  /* It means that the representation has an attribute
	   * which was created after insertion of the record.
	   * In this case, return the default value of the attribute
	   * if it exists.
	   */
	  if (att->default_value.val_length > 0)
	    {
	      disk_data = att->default_value.value;
	    }
	}
      else
	{
	  /* Is it a fixed size attribute ? */
	  if (att->is_fixed != 0)
	    {			/* A fixed attribute.  */
	      if (!OR_FIXED_ATT_IS_UNBOUND (recdes->data,
					    attr_info->read_classrepr->
					    n_variable,
					    attr_info->read_classrepr->
					    fixed_length, att->position))
		{
		  /* The fixed attribute is bound. Access its information */
		  disk_data = ((char *) recdes->data +
			       OR_FIXED_ATTRIBUTES_OFFSET_BY_OBJ
			       (recdes->data,
				attr_info->read_classrepr->n_variable) +
			       att->location);
		}
	    }
	  else
	    {			/* A variable attribute */
	      if (!OR_VAR_IS_NULL (recdes->data, att->location))
		{
		  /* The variable attribute is bound.
		   * Find its location through the variable offset attribute table. */
		  disk_data = ((char *) recdes->data +
			       OR_VAR_OFFSET (recdes->data, att->location));
		}
	    }
	}
    }
  else
    {
      assert (0);
      return ER_FAILED;
    }

  if (disk_data != NULL)
    {
      OR_BUF buf;

      or_init (&buf, disk_data, -1);
      (*(att->domain->type->data_readval)) (&buf, value, att->domain,
					    -1, false);
    }

  return NO_ERROR;
}

/*
 * heap_attrinfo_read_dbvalues () - Find db_values of desired attributes of given
 *                                instance
 *   return: NO_ERROR
 *   inst_oid(in): The instance oid
 *   recdes(in): The instance Record descriptor
 *   attr_info(in/out): The attribute information structure which describe the
 *                      desired attributes
 *
 * Note: Find DB_VALUES of desired attributes of given instance.
 * The attr_info structure must have already been initialized
 * with the desired attributes.
 *
 * If the inst_oid and the recdes are NULL, then we must be
 * reading only class attributes which are found
 * in the last representation.
 */
int
heap_attrinfo_read_dbvalues (THREAD_ENTRY * thread_p, const OID * inst_oid,
			     RECDES * recdes, HEAP_CACHE_ATTRINFO * attr_info)
{
  int i;
  REPR_ID reprid;		/* The disk representation of the object      */
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  int ret = NO_ERROR;

  /* check to make sure the attr_info has been used */
  if (attr_info->num_values == -1)
    {
      return NO_ERROR;
    }

  /*
   * Make sure that we have the needed cached representation.
   */

  if (inst_oid != NULL && recdes != NULL)
    {
      reprid = or_rep_id (recdes);

      if (attr_info->read_classrepr == NULL
	  || attr_info->read_classrepr->id != reprid)
	{
	  /* Get the needed representation */
	  ret = heap_attrinfo_recache (thread_p, reprid, attr_info);
	  if (ret != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}
    }

  /*
   * Go over each attribute and read it
   */

  for (i = 0; i < attr_info->num_values; i++)
    {
      value = &attr_info->values[i];
      ret = heap_attrvalue_read (recdes, value, attr_info);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }

  /*
   * Cache the information of the instance
   */
  if (inst_oid != NULL && recdes != NULL)
    {
      attr_info->inst_oid = *inst_oid;
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_attrinfo_delete_lob ()
 *   return: NO_ERROR
 *   thread_p(in):
 *   recdes(in): The instance Record descriptor
 *   attr_info(in): The attribute information structure which describe the
 *                  desired attributes
 *
 */
int
heap_attrinfo_delete_lob (THREAD_ENTRY * thread_p,
			  RECDES * recdes, HEAP_CACHE_ATTRINFO * attr_info)
{
  int i;
  HEAP_ATTRVALUE *value;
  int ret = NO_ERROR;

  assert (attr_info && attr_info->num_values > 0);

  /*
   * Make sure that we have the needed cached representation.
   */

  if (recdes != NULL)
    {
      REPR_ID reprid;
      reprid = or_rep_id (recdes);
      if (attr_info->read_classrepr == NULL ||
	  attr_info->read_classrepr->id != reprid)
	{
	  /* Get the needed representation */
	  ret = heap_attrinfo_recache (thread_p, reprid, attr_info);
	  if (ret != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}
#endif

/*
 * heap_attrinfo_dump () - Dump value of attribute information
 *   return:
 *   attr_info(in): The attribute information structure
 *   dump_schema(in):
 *
 * Note: Dump attribute value of given attribute information.
 */
void
heap_attrinfo_dump (THREAD_ENTRY * thread_p, FILE * fp,
		    HEAP_CACHE_ATTRINFO * attr_info, bool dump_schema)
{
  int i;
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr   */

  /* check to make sure the attr_info has been used */
  if (attr_info->num_values == -1)
    {
      fprintf (fp, "  Empty attrinfo\n");
      return;
    }

  /*
   * Dump attribute schema information
   */

  if (dump_schema == true)
    {
      (void) heap_classrepr_dump (thread_p, fp, &attr_info->class_oid,
				  attr_info->read_classrepr);
    }

  for (i = 0; i < attr_info->num_values; i++)
    {
      value = &attr_info->values[i];
      fprintf (fp, "  Attrid = %d, state = %d, type = %s\n",
	       value->attrid, value->state,
	       pr_type_name (value->read_attrepr->type));
      /*
       * Dump the value in memory format
       */

      fprintf (fp, "  Memory_value_format:\n");
      fprintf (fp, "    value = ");
      db_value_fprint (fp, &value->dbvalue);
      fprintf (fp, "\n\n");
    }

}

/*
 * heap_attrvalue_locate () - Locate disk attribute value information
 *   return: attrvalue or NULL
 *   attrid(in): The desired attribute identifier
 *   attr_info(in/out): The attribute information structure which describe the
 *                      desired attributes
 *
 * Note: Locate the disk attribute value information of an attribute
 * information structure which have been already initialized.
 */
HEAP_ATTRVALUE *
heap_attrvalue_locate (ATTR_ID attrid, HEAP_CACHE_ATTRINFO * attr_info)
{
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  int i;

  for (i = 0, value = attr_info->values;
       i < attr_info->num_values; i++, value++)
    {
      if (attrid == value->attrid)
	{
	  return value;
	}
    }

  return NULL;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_locate_attribute () -
 *   return:
 *   attrid(in):
 *   attr_info(in):
 */
static OR_ATTRIBUTE *
heap_locate_attribute (ATTR_ID attrid, HEAP_CACHE_ATTRINFO * attr_info)
{
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  int i;

  for (i = 0, value = attr_info->values;
       i < attr_info->num_values; i++, value++)
    {
      if (attrid == value->attrid)
	{
	  /* Some altered attributes might have only
	   * the last representations of them.
	   */
	  return (value->read_attrepr != NULL) ?
	    value->read_attrepr : value->last_attrepr;
	}
    }

  return NULL;
}
#endif

/*
 * heap_locate_last_attrepr () -
 *   return:
 *   attrid(in):
 *   attr_info(in):
 */
OR_ATTRIBUTE *
heap_locate_last_attrepr (ATTR_ID attrid, HEAP_CACHE_ATTRINFO * attr_info)
{
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  int i;

  for (i = 0, value = attr_info->values;
       i < attr_info->num_values; i++, value++)
    {
      if (attrid == value->attrid)
	{
	  return value->last_attrepr;
	}
    }

  return NULL;
}

/*
 * heap_attrinfo_access () - Access an attribute value which has been already read
 *   return:
 *   attrid(in): The desired attribute identifier
 *   attr_info(in/out): The attribute information structure which describe the
 *                      desired attributes
 *
 * Note: Find DB_VALUE of desired attribute identifier.
 * The dbvalue attributes must have been read by now using the
 * function heap_attrinfo_read_dbvalues ()
 */
DB_VALUE *
heap_attrinfo_access (ATTR_ID attrid, HEAP_CACHE_ATTRINFO * attr_info)
{
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr   */

  /* check to make sure the attr_info has been used */
  if (attr_info->num_values == -1)
    {
      return NULL;
    }

  value = heap_attrvalue_locate (attrid, attr_info);
  if (value == NULL || value->state == HEAP_UNINIT_ATTRVALUE)
    {
      er_log_debug (ARG_FILE_LINE,
		    "heap_attrinfo_access: Unknown attrid = %d", attrid);
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      "");
      return NULL;
    }

  return &value->dbvalue;
}

/*
 * heap_attrinfo_check () -
 *   return: NO_ERROR
 *   inst_oid(in): The instance oid
 *   attr_info(in): The attribute information structure which describe the
 *                  desired attributes
 */
static int
heap_attrinfo_check (const OID * inst_oid, HEAP_CACHE_ATTRINFO * attr_info)
{
  int ret = NO_ERROR;

  if (inst_oid != NULL)
    {
      /*
       * The OIDs must be equal
       */
      if (!OID_EQ (&attr_info->inst_oid, inst_oid))
	{
	  if (!OID_ISNULL (&attr_info->inst_oid))
	    {
	      ret = ER_HEAP_WRONG_ATTRINFO;
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ret, 6, attr_info->inst_oid.volid,
		      attr_info->inst_oid.pageid, attr_info->inst_oid.slotid,
		      inst_oid->volid, inst_oid->pageid, inst_oid->slotid);
	      goto exit_on_error;
	    }

	  attr_info->inst_oid = *inst_oid;
	}
    }
  else
    {
      if (!OID_ISNULL (&attr_info->inst_oid))
	{
	  ret = ER_HEAP_WRONG_ATTRINFO;
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ret, 6, attr_info->inst_oid.volid,
		  attr_info->inst_oid.pageid, attr_info->inst_oid.slotid,
		  NULL_VOLID, NULL_PAGEID, NULL_SLOTID);
	  goto exit_on_error;
	}
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_attrinfo_set () - Set the value of given attribute
 *   return: NO_ERROR
 *   inst_oid(in): The instance oid
 *   attrid(in): The identifier of the attribute to be set
 *   attr_val(in): The memory value of the attribute
 *   attr_info(in/out): The attribute information structure which describe the
 *                      desired attributes
 *
 * Note: Set DB_VALUE of desired attribute identifier.
 */
int
heap_attrinfo_set (const OID * inst_oid, ATTR_ID attrid, DB_VALUE * attr_val,
		   HEAP_CACHE_ATTRINFO * attr_info)
{
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  PR_TYPE *pr_type;		/* Primitive type array function structure */
  TP_DOMAIN_STATUS dom_status;
  int ret = NO_ERROR;

  /*
   * check to make sure the attr_info has been used, should never be empty.
   */

  if (attr_info->num_values == -1)
    {
      return ER_FAILED;
    }

  ret = heap_attrinfo_check (inst_oid, attr_info);
  if (ret != NO_ERROR)
    {
      goto exit_on_error;
    }

  value = heap_attrvalue_locate (attrid, attr_info);
  if (value == NULL)
    {
      goto exit_on_error;
    }

#if 1				/* TODO - */
  if (value->last_attrepr->type == DB_TYPE_VARIABLE)
    {
//      assert (false);
      goto exit_on_error;
    }
#endif

  pr_type = PR_TYPE_FROM_ID (value->last_attrepr->type);
  if (pr_type == NULL)
    {
      goto exit_on_error;
    }

  ret = pr_clear_value (&value->dbvalue);
  if (ret != NO_ERROR)
    {
      goto exit_on_error;
    }

  ret = db_value_domain_init (&value->dbvalue,
			      value->last_attrepr->type,
			      value->last_attrepr->domain->precision,
			      value->last_attrepr->domain->scale);
  if (ret != NO_ERROR)
    {
      goto exit_on_error;
    }

  /*
   * As we use "writeval" to do the writing and that function gets
   * enough domain information, we can use non-exact domain matching
   * here to defer the coercion until it is written.
   */
  dom_status = tp_domain_check (value->last_attrepr->domain, attr_val,
				TP_EXACT_MATCH);
  if (dom_status == DOMAIN_COMPATIBLE)
    {
      /*
       * the domains match exactly, set the value and proceed.  Copy
       * the source only if it's a set-valued thing (that's the purpose
       * of the third argument).
       */
      ret = (*(pr_type->setval)) (&value->dbvalue, attr_val,
				  TP_IS_SET_TYPE (pr_type->id));
    }
  else
    {
      /* the domains don't match, must attempt coercion */
      dom_status = tp_value_coerce (attr_val, &value->dbvalue,
				    value->last_attrepr->domain);
      if (dom_status != DOMAIN_COMPATIBLE)
	{
	  ret = tp_domain_status_er_set (dom_status, ARG_FILE_LINE,
					 attr_val,
					 value->last_attrepr->domain);
	  assert (er_errid () != NO_ERROR);

	  DB_MAKE_NULL (&value->dbvalue);
	}
    }

  if (ret != NO_ERROR)
    {
      goto exit_on_error;
    }

  value->state = HEAP_WRITTEN_ATTRVALUE;

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_attrinfo_set_uninitialized () - Read unitialized attributes
 *   return: NO_ERROR
 *   inst_oid(in): The instance oid
 *   recdes(in): The instance record descriptor
 *   attr_info(in/out): The attribute information structure which describe the
 *                      desired attributes
 *
 * Note: Read the db values of the unitialized attributes from the
 * given recdes. This function is used when we are ready to
 * transform an object that has been updated/inserted in the server.
 * If the object has been updated, recdes must be the old object
 * (the one on disk), so we can set the rest of the uninitialized
 * attributes from the old object.
 * If the object is a new one, recdes should be NULL, since there
 * is not an object on disk, the rest of the unitialized
 * attributes are set from default values.
 */
static int
heap_attrinfo_set_uninitialized (THREAD_ENTRY * thread_p, OID * inst_oid,
				 RECDES * recdes,
				 HEAP_CACHE_ATTRINFO * attr_info)
{
  int i;
  REPR_ID reprid;		/* Representation of object                   */
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  int ret = NO_ERROR;

  ret = heap_attrinfo_check (inst_oid, attr_info);
  if (ret != NO_ERROR)
    {
      goto exit_on_error;
    }

  /*
   * Make sure that we have the needed cached representation.
   */

  if (recdes != NULL)
    {
      reprid = or_rep_id (recdes);
    }
  else
    {
      reprid = attr_info->last_classrepr->id;
    }

  if (attr_info->read_classrepr == NULL
      || attr_info->read_classrepr->id != reprid)
    {
      /* Get the needed representation */
      ret = heap_attrinfo_recache (thread_p, reprid, attr_info);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }

  /*
   * Go over the attribute values and set the ones that have not been
   * initialized
   */
  for (i = 0; i < attr_info->num_values; i++)
    {
      value = &attr_info->values[i];
      if (value->state == HEAP_UNINIT_ATTRVALUE)
	{
	  ret = heap_attrvalue_read (recdes, value, attr_info);
	  if (ret != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}

    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_attrinfo_get_disksize () - Find the disk size needed to transform the object
 *                        represented by attr_info
 *   return: size of the object
 *   attr_info(in/out): The attribute information structure
 *
 * Note: Find the disk size needed to transform the object represented
 * by the attribute information structure.
 */
static int
heap_attrinfo_get_disksize (HEAP_CACHE_ATTRINFO * attr_info,
			    int *offset_size_ptr)
{
  int i, size;
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr   */

  *offset_size_ptr = OR_BYTE_SIZE;

re_check:
  size = 0;
  for (i = 0; i < attr_info->num_values; i++)
    {
      value = &attr_info->values[i];

      if (value->last_attrepr->is_fixed != 0)
	{
	  size += tp_domain_disk_size (value->last_attrepr->domain);
	}
      else
	{
	  size += pr_data_writeval_disk_size (&value->dbvalue);
	}
    }

  size += OR_HEADER_SIZE;
  size +=
    OR_VAR_TABLE_SIZE_INTERNAL (attr_info->last_classrepr->n_variable,
				*offset_size_ptr);
  size +=
    OR_BOUND_BIT_BYTES (attr_info->last_classrepr->n_attributes -
			attr_info->last_classrepr->n_variable);

  if (*offset_size_ptr == OR_BYTE_SIZE && size > OR_MAX_BYTE)
    {
      *offset_size_ptr = OR_SHORT_SIZE;	/* 2byte */
      goto re_check;
    }
  if (*offset_size_ptr == OR_SHORT_SIZE && size > OR_MAX_SHORT)
    {
      *offset_size_ptr = BIG_VAR_OFFSET_SIZE;	/* 4byte */
      goto re_check;
    }

  return size;
}

/*
 * heap_attrinfo_transform_to_disk () - Transform to disk an attribute information
 *                               kind of instance
 *   return: SCAN_CODE
 *           (Either of S_SUCCESS, S_DOESNT_FIT,
 *                      S_ERROR)
 *   attr_info(in/out): The attribute information structure
 *   old_recdes(in): where the object's disk format is deposited
 *   new_recdes(in):
 *   shard_groupid(in):
 *
 * Note: Transform the object represented by attr_info to disk format
 */
SCAN_CODE
heap_attrinfo_transform_to_disk (THREAD_ENTRY * thread_p,
				 HEAP_CACHE_ATTRINFO * attr_info,
				 RECDES * old_recdes, RECDES * new_recdes,
				 int shard_groupid)
{
  OR_BUF orep, *buf;
  char *ptr_bound, *ptr_varvals;
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  DB_VALUE temp_dbvalue;
  PR_TYPE *pr_type;		/* Primitive type array function structure */
  unsigned int repid_bits;
  SCAN_CODE status;
  int i;
  DB_VALUE *dbvalue = NULL;
  int expected_size, tmp;
  volatile int offset_size;

  assert (shard_groupid != NULL_GROUPID);

  /* check to make sure the attr_info has been used, it should not be empty. */
  if (attr_info->num_values == -1)
    {
      return S_ERROR;
    }

  /*
   * Get any of the values that have not been set/read
   */
  if (heap_attrinfo_set_uninitialized (thread_p, &attr_info->inst_oid,
				       old_recdes, attr_info) != NO_ERROR)
    {
      return S_ERROR;
    }

  /* Start transforming the dbvalues into disk values for the object */
  OR_BUF_INIT2 (orep, new_recdes->data, new_recdes->area_size);
  buf = &orep;

  expected_size = heap_attrinfo_get_disksize (attr_info, &tmp);
  offset_size = tmp;

  switch (_setjmp (buf->env))
    {
    case 0:
      status = S_SUCCESS;

      /*
       * Store the representation of the class along with bound bit
       * flag information
       */

      repid_bits = attr_info->last_classrepr->id;
      /*
       * Do we have fixed value attributes ?
       */
      if ((attr_info->last_classrepr->n_attributes
	   - attr_info->last_classrepr->n_variable) != 0)
	{
	  repid_bits |= OR_BOUND_BIT_FLAG;
	}

      /* offset size */
      OR_SET_VAR_OFFSET_SIZE (repid_bits, offset_size);

      or_put_int (buf, repid_bits);

      /* do shard group id validation
       * permit minus group id for migrator
       */
      if (heap_classrepr_is_shard_table (thread_p, &(attr_info->class_oid)) ==
	  true)
	{
	  /* is shard table */
	  if (shard_groupid < GLOBAL_GROUPID)
	    {
	      /* is shard table insertion from migrator */

	      /* save shard groupid; convert minus to plus */
	      assert (-shard_groupid > GLOBAL_GROUPID);
	      or_put_int (buf, -shard_groupid);
	    }
	  else
	    {
	      if (shard_groupid == GLOBAL_GROUPID
		  || !SHARD_GROUP_OWN (thread_p, shard_groupid))
		{
		  assert (false);	/* is impossible */
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_SHARD_INVALID_GROUPID, 1, shard_groupid);
		  return S_ERROR;
		}

	      /* save shard groupid */
	      assert (shard_groupid > GLOBAL_GROUPID);
	      or_put_int (buf, shard_groupid);
	    }
	}
      else
	{
	  /* is global table */
	  if (shard_groupid != GLOBAL_GROUPID)
	    {
	      assert (false);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_SHARD_INVALID_GROUPID, 1, shard_groupid);
	      return S_ERROR;
	    }

	  /* save shard groupid */
	  assert (shard_groupid == GLOBAL_GROUPID);
	  or_put_int (buf, shard_groupid);
	}

      /*
       * Calculate the pointer address to variable offset attribute table,
       * fixed attributes, and variable attributes
       */

      ptr_bound = OR_GET_BOUND_BITS (buf->buffer,
				     attr_info->last_classrepr->n_variable,
				     attr_info->last_classrepr->fixed_length);

      /*
       * Variable offset table is relative to the beginning of the buffer
       */

      ptr_varvals = ptr_bound +
	OR_BOUND_BIT_BYTES (attr_info->last_classrepr->n_attributes -
			    attr_info->last_classrepr->n_variable);

      /* Need to make sure that the bound array is not past the allocated
       * buffer because OR_ENABLE_BOUND_BIT() will just slam the bound
       * bit without checking the length.
       */

      if (ptr_varvals >= buf->endptr)
	{
	  new_recdes->length = -expected_size;	/* set to negative */
	  return S_DOESNT_FIT;
	}

      for (i = 0; i < attr_info->num_values; i++)
	{
	  value = &attr_info->values[i];
	  dbvalue = &value->dbvalue;
	  pr_type = value->last_attrepr->domain->type;
	  if (pr_type == NULL)
	    {
	      return S_ERROR;
	    }

	  /*
	   * Is this a fixed or variable attribute ?
	   */
	  if (value->last_attrepr->is_fixed != 0)
	    {
	      /*
	       * Fixed attribute
	       * Write the fixed attributes values, if unbound, does not matter
	       * what value is stored. We need to set the appropiate bit in the
	       * bound bit array for fixed attributes. For variable attributes,
	       */
	      buf->ptr =
		buf->buffer + OR_FIXED_ATTRIBUTES_OFFSET_BY_OBJ (buf->buffer,
								 attr_info->
								 last_classrepr->
								 n_variable) +
		value->last_attrepr->location;

	      if (dbvalue == NULL || db_value_is_null (dbvalue) == true)
		{
		  /*
		   * This is an unbound value.
		   *  1) Set any value in the fixed array value table, so we can
		   *     advance to next attribute.
		   *  2) and set the bound bit as unbound
		   */
		  db_value_domain_init (&temp_dbvalue,
					value->last_attrepr->type,
					value->last_attrepr->domain->
					precision,
					value->last_attrepr->domain->scale);
		  dbvalue = &temp_dbvalue;
		  OR_CLEAR_BOUND_BIT (ptr_bound,
				      value->last_attrepr->position);

		  /*
		   * pad the appropriate amount, writeval needs to be modified
		   * to accept a domain so it can perform this padding.
		   */
		  or_pad (buf,
			  tp_domain_disk_size (value->last_attrepr->domain));

		}
	      else
		{
		  /*
		   * Write the value.
		   */
		  OR_ENABLE_BOUND_BIT (ptr_bound,
				       value->last_attrepr->position);
		  (*(pr_type->data_writeval)) (buf, dbvalue);
		}
	    }
	  else
	    {
	      /*
	       * Variable attribute
	       *  1) Set the offset to this value in the variable offset table
	       *  2) Set the value in the variable value portion of the disk
	       *     object (Only if the value is bound)
	       */

	      /*
	       * Write the offset onto the variable offset table and remember
	       * the current pointer to the variable offset table
	       */

	      buf->ptr = (char *) (OR_VAR_ELEMENT_PTR (buf->buffer,
						       value->last_attrepr->
						       location));
	      or_put_offset_internal (buf,
				      CAST_BUFLEN (ptr_varvals - buf->buffer),
				      offset_size);

	      if (dbvalue != NULL && db_value_is_null (dbvalue) != true)
		{
		  /*
		   * Now write the value and remember the current pointer
		   * to variable value array for the next element.
		   */
		  buf->ptr = ptr_varvals;

		  (*(pr_type->data_writeval)) (buf, dbvalue);
		  ptr_varvals = buf->ptr;
		}
	    }
	}

      if (attr_info->last_classrepr->n_variable > 0)
	{
	  /*
	   * The last element of the variable offset table points to the end of
	   * the object. The variable offset array starts with zero, so we can
	   * just access n_variable...
	   */

	  /* Write the offset to the end of the variable attributes table */
	  buf->ptr = ((char *)
		      (OR_VAR_ELEMENT_PTR (buf->buffer,
					   attr_info->last_classrepr->
					   n_variable)));
	  or_put_offset_internal (buf,
				  CAST_BUFLEN (ptr_varvals - buf->buffer),
				  offset_size);
	  buf->ptr = PTR_ALIGN (buf->ptr, INT_ALIGNMENT);
	}

      /* Record the length of the object */
      new_recdes->length = CAST_BUFLEN (ptr_varvals - buf->buffer);
      break;

      /*
       * if the longjmp status was anything other than ER_TF_BUFFER_OVERFLOW,
       * it represents an error condition and er_set will have been called
       */
    case ER_TF_BUFFER_OVERFLOW:

      status = S_DOESNT_FIT;


      /*
       * Give a hint of the needed space. The hint is given as a negative
       * value in the record descriptor length. Make sure that this length
       * is larger than the current record descriptor area.
       */

      new_recdes->length = -expected_size;	/* set to negative */

      if (new_recdes->area_size > -new_recdes->length)
	{
	  /*
	   * This may be an error. The estimated disk size is smaller
	   * than the current record descriptor area size. For now assume
	   * at least 20% above the current area descriptor. The main problem
	   * is that heap_attrinfo_get_disksize () guess its size as much as
	   * possible
	   */
	  new_recdes->length = -(int) (new_recdes->area_size * 1.20);
	}
      break;

    default:
      status = S_ERROR;
      break;
    }

  return status;
}

/*
 * heap_attrinfo_start_with_index () -
 *   return:
 *   class_oid(in):
 *   class_recdes(in):
 *   attr_info(in):
 *   idx_info(in):
 */
int
heap_attrinfo_start_with_index (THREAD_ENTRY * thread_p, OID * class_oid,
				RECDES * class_recdes,
				HEAP_CACHE_ATTRINFO * attr_info,
				HEAP_IDX_ELEMENTS_INFO * idx_info)
{
  ATTR_ID guess_attrids[HEAP_GUESS_NUM_INDEXED_ATTRS];
  ATTR_ID *set_attrids;
  int num_found_attrs;
  OR_CLASSREP *classrepr = NULL;
  int classrepr_cacheindex = -1;
  OR_ATTRIBUTE *search_attrepr;
  int i, j;
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr */
  int *num_btids;
  OR_INDEX *indexp;

  idx_info->has_single_col = 0;
  idx_info->has_multi_col = 0;
  idx_info->num_btids = 0;

  num_btids = &idx_info->num_btids;

  set_attrids = guess_attrids;
  attr_info->num_values = -1;	/* initialize attr_info */

  classrepr = heap_classrepr_get (thread_p, class_oid, class_recdes,
				  0, &classrepr_cacheindex, true);
  if (classrepr == NULL)
    {
      return ER_FAILED;
    }

  if (classrepr->n_attributes > HEAP_GUESS_NUM_INDEXED_ATTRS)
    {
      set_attrids = (ATTR_ID *) malloc (classrepr->n_attributes
					* sizeof (ATTR_ID));
      if (set_attrids == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, classrepr->n_attributes * sizeof (ATTR_ID));
	  (void) heap_classrepr_free (classrepr, &classrepr_cacheindex);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
    }
  else
    {
      set_attrids = guess_attrids;
    }

  /*
   * Read the number of BTID's in this class
   */
  *num_btids = classrepr->n_indexes;

  for (j = 0; j < *num_btids; j++)
    {
      indexp = &(classrepr->indexes[j]);
      if (indexp->n_atts == 1)
	{
	  idx_info->has_single_col = 1;
	}
      else if (indexp->n_atts > 1)
	{
	  idx_info->has_multi_col = 1;
	}
      /* check for already found both */
      if (idx_info->has_single_col && idx_info->has_multi_col)
	{
	  break;
	}
    }

  /*
   * Go over the list of attrs until all indexed attributes (OIDs, sets)
   * are found
   */
  for (i = 0, num_found_attrs = 0, search_attrepr = classrepr->attributes;
       i < classrepr->n_attributes; i++, search_attrepr++)
    {
      if (search_attrepr->n_btids <= 0)
	{
	  continue;
	}

      if (idx_info->has_single_col)
	{
	  for (j = 0; j < *num_btids; j++)
	    {
	      indexp = &(classrepr->indexes[j]);
	      if (indexp->n_atts == 1
		  && indexp->atts[0]->id == search_attrepr->id)
		{
		  set_attrids[num_found_attrs++] = search_attrepr->id;
		  break;
		}
	    }
	}
    }				/* for (i = 0 ...) */

  if (idx_info->has_multi_col == 0 && num_found_attrs == 0)
    {
      /* initialize the attrinfo cache and return, there is nothing else to do */
      /* (void) memset(attr_info, '\0', sizeof (HEAP_CACHE_ATTRINFO)); */

      /* now set the num_values to -1 which indicates that this is an
       * empty HEAP_CACHE_ATTRINFO and shouldn't be operated on.
       */
      attr_info->num_values = -1;

      /* free the class representation */
      (void) heap_classrepr_free (classrepr, &classrepr_cacheindex);
    }
  else
    {				/* num_found_attrs > 0 */
      /* initialize attribute information */
      attr_info->class_oid = *class_oid;
      attr_info->last_cacheindex = classrepr_cacheindex;
      attr_info->read_cacheindex = -1;
      attr_info->last_classrepr = classrepr;
      attr_info->read_classrepr = NULL;
      OID_SET_NULL (&attr_info->inst_oid);
      attr_info->num_values = num_found_attrs;

      if (num_found_attrs <= 0)
	{
	  attr_info->values = NULL;
	}
      else
	{
	  attr_info->values = (HEAP_ATTRVALUE *)
	    malloc ((num_found_attrs * sizeof (HEAP_ATTRVALUE)));
	  if (attr_info->values == NULL)
	    {
	      /* free the class representation */
	      (void) heap_classrepr_free (classrepr, &classrepr_cacheindex);
	      attr_info->num_values = -1;
	      goto error;
	    }
	}

      /*
       * Set the attribute identifier of the desired attributes in the value
       * attribute information, and indicates that the current value is
       * unitialized. That is, it has not been read, set or whatever.
       */
      for (i = 0; i < attr_info->num_values; i++)
	{
	  value = &attr_info->values[i];
	  value->attrid = set_attrids[i];
	  value->state = HEAP_UNINIT_ATTRVALUE;
	  value->last_attrepr = NULL;
	  value->read_attrepr = NULL;
	}

      /*
       * Make last information to be recached for each individual attribute
       * value. Needed for WRITE and Default values
       */
      if (heap_attrinfo_recache_attrepr (attr_info, true) != NO_ERROR)
	{
	  /* classrepr will be freed in heap_attrinfo_end */
	  heap_attrinfo_end (thread_p, attr_info);
	  goto error;
	}
    }

  if (set_attrids != guess_attrids)
    {
      free_and_init (set_attrids);
    }

  if (num_found_attrs == 0 && idx_info->has_multi_col)
    {
      return 1;
    }
  else
    {
      return num_found_attrs;
    }

  /* **** */
error:

  if (set_attrids != guess_attrids)
    {
      free_and_init (set_attrids);
    }

  return ER_FAILED;
}

/*
 * heap_classrepr_find_index_id () - Find the indicated index ID from the class repr
 *   return: ID of desired index or -1 if an error occurred.
 *   classrepr(in): The class representation.
 *   btid(in): The BTID of the interested index.
 *
 * Note: Locate the desired index by matching it with the passed BTID.
 * Return the ID of the index if found.
 */
int
heap_classrepr_find_index_id (OR_CLASSREP * classrepr, BTID * btid)
{
  int i;
  int id = -1;

  assert (classrepr != NULL);

  for (i = 0; i < classrepr->n_indexes; i++)
    {
      if (BTID_IS_EQUAL (&(classrepr->indexes[i].btid), btid))
	{
	  id = i;
	  break;
	}
    }

  return id;
}

/*
 * heap_classrepr_find_shard_key_column_id () - Find the shard key column ID from the class repr
 *   return: ID of shard key column or -1 if an error occurred.
 *   classrepr(in): The class representation.
 */
static int
heap_classrepr_find_shard_key_column_id (UNUSED_ARG THREAD_ENTRY * thread_p,
					 OR_CLASSREP * classrepr)
{
  OR_ATTRIBUTE *or_att;
  int i;
  int id = -1;

  assert (classrepr != NULL);

  or_att = classrepr->attributes;
  for (i = 0; i < classrepr->n_attributes; i++, or_att++)
    {
      if (or_att->is_shard_key)
	{
	  id = or_att->id;
	  break;
	}
    }

  return id;
}

bool
heap_classrepr_is_shard_table (THREAD_ENTRY * thread_p, OID * class_oid)
{
  bool is_shard_table = false;
  OR_CLASSREP *classrepr = NULL;
  int classrepr_cacheindex = -1;

  assert (class_oid != NULL);
  assert (!OID_ISNULL (class_oid));

  classrepr = heap_classrepr_get (thread_p, class_oid, NULL, 0,
				  &classrepr_cacheindex, true);
  if (classrepr == NULL)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* get the shard key attr id */
  if (heap_classrepr_find_shard_key_column_id (thread_p, classrepr) >= 0)
    {
      is_shard_table = true;
    }

done:

  /* free the class representation */
  if (classrepr)
    {
      (void) heap_classrepr_free (classrepr, &classrepr_cacheindex);
    }

  return is_shard_table;

exit_on_error:

  assert (er_errid () != NO_ERROR);

  goto done;
}

/*
 * heap_attrinfo_start_with_btid () - Initialize an attribute information structure
 *   return: ID for the index which corresponds to the passed BTID.
 *           If an error occurred, a -1 is returned.
 *   class_oid(in): The class identifier of the instances where values
 *                  attributes values are going to be read.
 *   btid(in): The BTID of the interested index.
 *   attr_info(in/out): The attribute information structure
 *
 * Note: Initialize an attribute information structure, so that values
 * of instances can be retrieved based on the desired attributes.
 *
 * There are currently three functions which can be used to
 * initialize the attribute information structure; heap_attrinfo_start(),
 * heap_attrinfo_start_with_index() and this one.  This function determines
 * which attributes belong to the passed BTID and populate the
 * information structure on those attributes.
 *
 * The attrinfo structure is an structure where values of
 * instances can be read. For example an object is retrieved,
 * then some of its attributes are convereted to dbvalues and
 * placed in this structure.
 *
 * Note: The caller must call heap_attrinfo_end after he is done with
 * attribute information.
 */
int
heap_attrinfo_start_with_btid (THREAD_ENTRY * thread_p, OID * class_oid,
			       BTID * btid, HEAP_CACHE_ATTRINFO * attr_info)
{
  ATTR_ID guess_attrids[HEAP_GUESS_NUM_INDEXED_ATTRS];
  ATTR_ID *set_attrids;
  OR_CLASSREP *classrepr = NULL;
  int i;
  int index_id = -1;
  int classrepr_cacheindex = -1;
  int num_found_attrs = 0;
  int error = NO_ERROR;

  /*
   *  We'll start by assuming that the number of attributes will fit into
   *  the preallocated array.
   */
  set_attrids = guess_attrids;

  attr_info->num_values = -1;	/* initialize attr_info */

  /*
   *  Get the class representation so that we can access the indexes.
   */
  classrepr = heap_classrepr_get (thread_p, class_oid, NULL, 0,
				  &classrepr_cacheindex, true);
  if (classrepr == NULL)
    {
      error = er_errid ();
      goto exit_on_error;
    }

  /*
   *  Get the index ID which corresponds to the BTID
   */
  index_id = heap_classrepr_find_index_id (classrepr, btid);
  if (index_id == -1)
    {
      error = ER_FAILED;

      goto exit_on_error;
    }

  /*
   *  Get the number of attributes associated with this index.
   *  Allocate a new attribute ID array if we have more attributes
   *  than will fit in the pre-allocated array.
   *  Fill the array with the attribute ID's
   *  Free the class representation.
   */
  num_found_attrs = classrepr->indexes[index_id].n_atts;
  if (num_found_attrs > HEAP_GUESS_NUM_INDEXED_ATTRS)
    {
      set_attrids = (ATTR_ID *) malloc (num_found_attrs * sizeof (ATTR_ID));
      if (set_attrids == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  num_found_attrs * sizeof (ATTR_ID));
	  goto exit_on_error;
	}
    }

  for (i = 0; i < num_found_attrs; i++)
    {
      set_attrids[i] = classrepr->indexes[index_id].atts[i]->id;
    }

  (void) heap_classrepr_free (classrepr, &classrepr_cacheindex);
  classrepr = NULL;

  /*
   *  Get the attribute information for the collected ID's
   */
  if (num_found_attrs > 0)
    {
      error = heap_attrinfo_start (thread_p, class_oid, num_found_attrs,
				   set_attrids, attr_info);
      if (error != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }

  /*
   *  Free the attribute ID array if it was dynamically allocated
   */
  if (set_attrids != guess_attrids)
    {
      free_and_init (set_attrids);
    }

  return index_id;

  /* **** */
exit_on_error:

  if (classrepr)
    {
      (void) heap_classrepr_free (classrepr, &classrepr_cacheindex);
    }

  if (set_attrids != guess_attrids)
    {
      free_and_init (set_attrids);
    }

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_FAILED;
    }

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_attrvalue_get_index () -
 *   return:
 *   value_index(in):
 *   attrid(in):
 *   n_btids(in):
 *   btids(in):
 *   idx_attrinfo(in):
 */
DB_VALUE *
heap_attrvalue_get_index (int value_index, ATTR_ID * attrid,
			  int *n_btids, BTID ** btids,
			  HEAP_CACHE_ATTRINFO * idx_attrinfo)
{
  HEAP_ATTRVALUE *value;	/* Disk value Attr info for a particular attr   */

  /* check to make sure the idx_attrinfo has been used, it should never
   * be empty.
   */
  if (idx_attrinfo->num_values == -1)
    {
      return NULL;
    }

  if (value_index > idx_attrinfo->num_values || value_index < 0)
    {
      *n_btids = 0;
      *btids = NULL;
      *attrid = NULL_ATTRID;
      return NULL;
    }
  else
    {
      value = &idx_attrinfo->values[value_index];
      *n_btids = value->last_attrepr->n_btids;
      *btids = value->last_attrepr->btids;
      *attrid = value->attrid;
      return &value->dbvalue;
    }

}
#endif

/*
 * heap_idxkey_key_get () -
 *   return:
 *   inst_oid(in):
 *   recdes(in):
 *   key(in):
 *   indexp(in):
 *   attrinfo(in):
 */
static int
heap_idxkey_key_get (const OID * inst_oid, RECDES * recdes,
		     DB_IDXKEY * key,
		     OR_INDEX * indexp, HEAP_CACHE_ATTRINFO * attrinfo)
{
  int error = NO_ERROR;
  OR_ATTRIBUTE **atts;
  int num_atts, i;

  assert (inst_oid != NULL);

  assert (key != NULL);
  assert (DB_IDXKEY_IS_NULL (key));

  assert (indexp != NULL);

  num_atts = indexp->n_atts;
  atts = indexp->atts;

  for (i = 0; i < num_atts; i++)
    {
      error =
	heap_idxkey_get_value (recdes, atts[i], &(key->vals[i]), attrinfo);
      if (error != NO_ERROR)
	{
	  assert (false);
	  goto exit_on_error;
	}

      key->size++;
    }

  assert (i == indexp->n_atts);

  if (inst_oid == NULL)
    {
      assert (false);		/* is impossible */
      error = ER_FAILED;
      goto exit_on_error;
    }

  /* append rightmost OID */
  if (OID_ISNULL (inst_oid))
    {
      assert (DB_IS_NULL (&(key->vals[i])));
    }
  else
    {
      assert (inst_oid->groupid >= GLOBAL_GROUPID);

      DB_MAKE_OID (&(key->vals[i]), inst_oid);
    }

  key->size++;
  assert (key->size == num_atts + 1);

  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  return error;

exit_on_error:

  assert (error != NO_ERROR);

  return error;
}


/*
 * heap_attrvalue_get_key () - Get B-tree key from attribute value(s)
 *   return: error code
 *   btid_index(in): Index into an array of BTID's from the OR_CLASSREP
 *                   structure contained in idx_attrinfo.
 *   idx_attrinfo(in): Pointer to attribute information structure.  This
 *                     structure contains the BTID's, the attributes and their
 *                     values.
 *   inst_oid(in):
 *   recdes(in):
 *   btid(in): Pointer to a BTID.  The value of the current BTID
 *             will be returned.
 *   key(in):
 *
 * Note: Return a B-tree key for the specified B-tree ID.
 *
 * If the specified B-tree ID is associated with a single
 * attribute the key will be the value of that attribute
 * and we will return a pointer to that DB_VALUE.
 *
 * If the BTID is associated with multiple attributes the
 * key will be a set containing the values of the attributes.
 * The set will be constructed and contained within the
 * passed DB_VALUE.  A pointer to this DB_VALUE is returned.
 * It is important for the caller to deallocate this memory
 * by calling pr_clear_value().
 */
int
heap_attrvalue_get_key (THREAD_ENTRY * thread_p, int btid_index,
			HEAP_CACHE_ATTRINFO * idx_attrinfo,
			const OID * inst_oid, RECDES * recdes,
			BTID * btid, DB_IDXKEY * key)
{
  int ret = NO_ERROR;
  OR_INDEX *indexp = NULL;
//  int n_atts;
  int reprid;

  assert (inst_oid != NULL);
  assert (OID_ISNULL (inst_oid) || inst_oid->groupid >= GLOBAL_GROUPID);

  assert (key != NULL);
  assert (DB_IDXKEY_IS_NULL (key));

  /*
   *  check to make sure the idx_attrinfo has been used, it should
   *  never be empty.
   */
  if ((idx_attrinfo->num_values == -1) ||
      (btid_index >= idx_attrinfo->last_classrepr->n_indexes))
    {
      return ER_FAILED;
    }

  /*
   * Make sure that we have the needed cached representation.
   */
  if (recdes != NULL)
    {
      reprid = or_rep_id (recdes);

      if (idx_attrinfo->read_classrepr == NULL ||
	  idx_attrinfo->read_classrepr->id != reprid)
	{
	  /* Get the needed representation */
	  ret = heap_attrinfo_recache (thread_p, reprid, idx_attrinfo);
	  if (ret != NO_ERROR)
	    {
	      return ret;
	    }
	}
    }

  indexp = &(idx_attrinfo->last_classrepr->indexes[btid_index]);

//  n_atts = indexp->n_atts;
  *btid = indexp->btid;

  /* Construct the key as a sequence of attribute
   * values.  The sequence is contained in the passed DB_VALUE.
   * A pointer to this DB_VALUE is returned.
   */
  assert (recdes != NULL);
  if (recdes != NULL)
    {
      ret = heap_idxkey_key_get (inst_oid, recdes, key, indexp, idx_attrinfo);
      if (ret != NO_ERROR)
	{
	  return ret;
	}
    }

  assert (ret == NO_ERROR);

  return ret;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_indexinfo_get_btid () -
 *   return:
 *   btid_index(in):
 *   attrinfo(in):
 */
BTID *
heap_indexinfo_get_btid (int btid_index, HEAP_CACHE_ATTRINFO * attrinfo)
{
  if (btid_index != -1 && btid_index < attrinfo->last_classrepr->n_indexes)
    {
      return &(attrinfo->last_classrepr->indexes[btid_index].btid);
    }
  else
    {
      return NULL;
    }
}

/*
 * heap_indexinfo_get_num_attrs () -
 *   return:
 *   btid_index(in):
 *   attrinfo(in):
 */
int
heap_indexinfo_get_num_attrs (int btid_index, HEAP_CACHE_ATTRINFO * attrinfo)
{
  if (btid_index != -1 && btid_index < attrinfo->last_classrepr->n_indexes)
    {
      return attrinfo->last_classrepr->indexes[btid_index].n_atts;
    }
  else
    {
      return 0;
    }
}

/*
 * heap_indexinfo_get_attrids () -
 *   return: NO_ERROR
 *   btid_index(in):
 *   attrinfo(in):
 *   attrids(in):
 */
int
heap_indexinfo_get_attrids (int btid_index, HEAP_CACHE_ATTRINFO * attrinfo,
			    ATTR_ID * attrids)
{
  int i;
  int ret = NO_ERROR;

  if (btid_index != -1 && (btid_index < attrinfo->last_classrepr->n_indexes))
    {
      for (i = 0; i < attrinfo->last_classrepr->indexes[btid_index].n_atts;
	   i++)
	{
	  attrids[i] =
	    attrinfo->last_classrepr->indexes[btid_index].atts[i]->id;
	}
    }

  return ret;
}

/*
 * heap_get_index_with_name () - get BTID of index with name index_name
 * return : error code or NO_ERROR
 * thread_p (in) :
 * class_oid (in) : class OID
 * index_name (in): index name
 * btid (in/out)  : btid
 */
int
heap_get_index_with_name (THREAD_ENTRY * thread_p, OID * class_oid,
			  const char *index_name, BTID * btid)
{
  OR_CLASSREP *classrep = NULL;
  OR_INDEX *indexp = NULL;
  int idx_in_cache, i;
  int error = NO_ERROR;

  BTID_SET_NULL (btid);

  /* get the class representation so that we can access the indexes */
  classrep = heap_classrepr_get (thread_p, class_oid, NULL, 0,
				 &idx_in_cache, true);
  if (classrep == NULL)
    {
      return ER_FAILED;
    }

  for (i = 0; i < classrep->n_indexes; i++)
    {
      if (strcasecmp (classrep->indexes[i].btname, index_name) == 0)
	{
	  BTID_COPY (btid, &classrep->indexes[i].btid);
	  break;
	}
    }
  if (classrep != NULL)
    {
      error = heap_classrepr_free (classrep, &idx_in_cache);
    }
  return error;
}
#endif

/*
 * heap_get_indexname_of_btid () -
 *   return: NO_ERROR
 *   class_oid(in):
 *   btid(in):
 *   btnamepp(in);
 */
int
heap_get_indexname_of_btid (THREAD_ENTRY * thread_p, OID * class_oid,
			    BTID * btid, char **btnamepp)
{
  OR_CLASSREP *classrepp = NULL;
  OR_INDEX *indexp;
  int idx_in_cache;
  int idx;
  int ret = NO_ERROR;

  /* initial value of output parameters */
  if (btnamepp)
    {
      *btnamepp = NULL;
    }

  /* get the class representation so that we can access the indexes */
  classrepp = heap_classrepr_get (thread_p, class_oid, NULL, 0,
				  &idx_in_cache, true);
  if (classrepp == NULL)
    {
      goto exit_on_error;
    }

  /* get the idx of the index which corresponds to the BTID */
  idx = heap_classrepr_find_index_id (classrepp, btid);
  if (idx < 0)
    {
      goto exit_on_error;
    }
  indexp = &(classrepp->indexes[idx]);

  if (btnamepp)
    {
      *btnamepp = strdup (indexp->btname);
    }

  /* free the class representation */
  ret = heap_classrepr_free (classrepp, &idx_in_cache);
  if (ret != NO_ERROR)
    {
      goto exit_on_error;
    }

  return ret;

exit_on_error:

  if (btnamepp && *btnamepp)
    {
      free_and_init (*btnamepp);
    }

  if (classrepp)
    {
      (void) heap_classrepr_free (classrepp, &idx_in_cache);
    }

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_prefetch () - Prefetch objects
 *   return: NO_ERROR
 *           fetch_area is set to point to fetching area
 *   class_oid(in): Class identifier for the instance oid
 *   oid(in): Object that must be fetched if its cached state is invalid
 *   prefetch(in): Prefetch structure
 *
 */
int
heap_prefetch (THREAD_ENTRY * thread_p, OID * class_oid, const OID * oid,
	       LC_COPYAREA_DESC * prefetch)
{
  VPID vpid;
  PAGE_PTR pgptr = NULL;
  int round_length;
  INT16 right_slotid, left_slotid;
  HEAP_DIRECTION direction;
  SCAN_CODE scan;
  int ret = NO_ERROR;

  /*
   * Prefetch other instances (i.e., neighbors) stored on the same page
   * of the given object OID. Relocated instances and instances in overflow are
   * not prefetched, nor instances that do not belong to the given class.
   * Prefetching stop once an error, such as out of space, is encountered.
   */

  vpid.volid = oid->volid;
  vpid.pageid = oid->pageid;

  pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, S_LOCK,
				       NULL, PAGE_HEAP);
  if (pgptr == NULL)
    {
      ret = er_errid ();
      if (ret == ER_PB_BAD_PAGEID)
	{
	  ret = ER_HEAP_UNKNOWN_OBJECT;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret,
		  3, oid->volid, oid->pageid, oid->slotid);
	}

      /*
       * Problems getting the page.. forget about prefetching...
       */
      return ret;
    }

  right_slotid = oid->slotid;
  left_slotid = oid->slotid;
  direction = HEAP_DIRECTION_BOTH;

  while (direction != HEAP_DIRECTION_NONE)
    {
      /*
       * Don't include the desired object again, forwarded instances, nor
       * instances that belong to other classes
       */

      /* Check to the right */
      if (direction == HEAP_DIRECTION_RIGHT
	  || direction == HEAP_DIRECTION_BOTH)
	{
	  scan = spage_next_record (pgptr, &right_slotid, prefetch->recdes,
				    COPY);
	  if (scan == S_SUCCESS
	      && spage_get_record_type (pgptr, right_slotid) == REC_HOME)
	    {
	      prefetch->mobjs->num_objs++;
	      COPY_OID (&((*prefetch->obj)->class_oid), class_oid);
	      (*prefetch->obj)->oid.volid = oid->volid;
	      (*prefetch->obj)->oid.pageid = oid->pageid;
	      (*prefetch->obj)->oid.slotid = right_slotid;
	      (*prefetch->obj)->length = prefetch->recdes->length;
	      (*prefetch->obj)->offset = *prefetch->offset;
	      (*prefetch->obj)->operation = LC_FETCH;
	      (*prefetch->obj) =
		LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (*prefetch->obj);
	      round_length =
		DB_ALIGN (prefetch->recdes->length, HEAP_MAX_ALIGN);
	      *prefetch->offset += round_length;
	      prefetch->recdes->data += round_length;
	      prefetch->recdes->area_size -= (round_length +
					      sizeof (*(*prefetch->obj)));
	    }
	  else if (scan != S_SUCCESS)
	    {
	      /* Stop prefetching objects from the right */
	      direction = ((direction == HEAP_DIRECTION_BOTH)
			   ? HEAP_DIRECTION_LEFT : HEAP_DIRECTION_NONE);
	    }
	}

      /* Check to the left */
      if (direction == HEAP_DIRECTION_LEFT
	  || direction == HEAP_DIRECTION_BOTH)
	{
	  scan = spage_previous_record (pgptr, &left_slotid, prefetch->recdes,
					COPY);
	  if (scan == S_SUCCESS && left_slotid != HEAP_HEADER_AND_CHAIN_SLOTID
	      && spage_get_record_type (pgptr, left_slotid) == REC_HOME)
	    {
	      prefetch->mobjs->num_objs++;
	      COPY_OID (&((*prefetch->obj)->class_oid), class_oid);
	      (*prefetch->obj)->oid.volid = oid->volid;
	      (*prefetch->obj)->oid.pageid = oid->pageid;
	      (*prefetch->obj)->oid.slotid = left_slotid;
	      (*prefetch->obj)->length = prefetch->recdes->length;
	      (*prefetch->obj)->offset = *prefetch->offset;
	      (*prefetch->obj)->operation = LC_FETCH;
	      (*prefetch->obj) =
		LC_NEXT_ONEOBJ_PTR_IN_COPYAREA (*prefetch->obj);
	      round_length =
		DB_ALIGN (prefetch->recdes->length, HEAP_MAX_ALIGN);
	      *prefetch->offset += round_length;
	      prefetch->recdes->data += round_length;
	      prefetch->recdes->area_size -= (round_length +
					      sizeof (*(*prefetch->obj)));
	    }
	  else if (scan != S_SUCCESS)
	    {
	      /* Stop prefetching objects from the right */
	      direction = ((direction == HEAP_DIRECTION_BOTH)
			   ? HEAP_DIRECTION_RIGHT : HEAP_DIRECTION_NONE);
	    }
	}
    }

  pgbuf_unfix_and_init (thread_p, pgptr);

  return ret;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_check_all_pages () - Validate all pages known by given heap vs file manger
 *   return: DISK_INVALID, DISK_VALID, DISK_ERROR
 *   hfid(in): : Heap identifier
 *
 * Note: Verify that all pages known by the given heap are valid. That
 * is, that they are valid from the point of view of the file manager.
 */
DISK_ISVALID
heap_check_all_pages (THREAD_ENTRY * thread_p, HFID * hfid)
{
  VPID vpid;			/* Page-volume identifier            */
  VPID *vpidptr_ofpgptr;
  PAGE_PTR pgptr = NULL;	/* Page pointer                      */
  RECDES hdr_recdes = RECDES_INITIALIZER;	/* Header record descriptor          */
  DISK_ISVALID valid_pg = DISK_VALID;
  DISK_ISVALID valid = DISK_VALID;
  INT32 npages = 0;
  int i;
  HEAP_CHKALL_RELOCOIDS chk;
  HEAP_CHKALL_RELOCOIDS *chk_objs = &chk;
  PAGE_TYPE ptype;

  valid_pg = heap_chkreloc_start (chk_objs);
  if (valid_pg != DISK_VALID)
    {
      chk_objs = NULL;
    }

  /* Scan every page of the heap to find out if they are valid */

  vpid.volid = hfid->vfid.volid;
  vpid.pageid = hfid->hpgid;
  ptype = HEAP_HEADER;

  while (!VPID_ISNULL (&vpid) && valid_pg == DISK_VALID)
    {
      npages++;

      valid_pg = file_isvalid_page_partof (thread_p, &vpid, &hfid->vfid);
      if (valid_pg != DISK_VALID)
	{
	  break;
	}

      pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, OLD_PAGE, S_LOCK,
					   NULL, ptype);
      if (pgptr == NULL)
	{
	  /* something went wrong, return */
	  valid_pg = DISK_ERROR;
	  break;
	}

      if (heap_vpid_next (hfid, pgptr, &vpid) != NO_ERROR)
	{
	  pgbuf_unfix_and_init (thread_p, pgptr);
	  /* something went wrong, return */
	  valid_pg = DISK_ERROR;
	  break;
	}

      vpidptr_ofpgptr = pgbuf_get_vpid_ptr (pgptr);
      if (VPID_EQ (&vpid, vpidptr_ofpgptr))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HEAP_CYCLE, 5,
		  vpid.volid, vpid.pageid, hfid->vfid.volid,
		  hfid->vfid.fileid, hfid->hpgid);
	  VPID_SET_NULL (&vpid);
	  valid_pg = DISK_ERROR;
	}

      if (chk_objs != NULL)
	{
	  valid_pg = heap_chkreloc_next (chk_objs, pgptr);
	}

      pgbuf_unfix_and_init (thread_p, pgptr);

      ptype = PAGE_HEAP;
    }

  if (chk_objs != NULL)
    {
      if (valid_pg == DISK_VALID)
	{
	  valid_pg = heap_chkreloc_end (chk_objs);
	}
      else
	{
	  chk_objs->verify = false;
	  (void) heap_chkreloc_end (chk_objs);
	}
    }

  if (valid_pg == DISK_VALID)
    {
      i = file_get_numpages (thread_p, &hfid->vfid, NULL, NULL, NULL);
      if (npages != i)
	{
	  if (i == -1)
	    {
	      valid_pg = DISK_ERROR;
	    }
	  else
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HEAP_MISMATCH_NPAGES, 5, hfid->vfid.volid,
		      hfid->vfid.fileid, hfid->hpgid, npages, i);
	      valid_pg = DISK_INVALID;
	    }
	}

      /*
       * Check the statistics entries in the header
       */

      /* Fetch the header page of the heap file */
      vpid.volid = hfid->vfid.volid;
      vpid.pageid = hfid->hpgid;

      pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, OLD_PAGE, S_LOCK,
					   NULL, PAGE_HEAP_HEADER);
      if (pgptr == NULL
	  || spage_get_record (pgptr, HEAP_HEADER_AND_CHAIN_SLOTID,
			       &hdr_recdes, PEEK) != S_SUCCESS)
	{
	  /* Unable to peek heap header record */
	  if (pgptr != NULL)
	    {
	      pgbuf_unfix_and_init (thread_p, pgptr);
	    }

	  return DISK_ERROR;
	}

#if defined(SA_MODE)
      if (prm_get_integer_value (PRM_ID_HF_MAX_BESTSPACE_ENTRIES) > 0)
	{
	  HEAP_BESTSPACE_ENTRY *ent;
	  void *last;
	  int rc;

	  rc = pthread_mutex_lock (&heap_Bestspace->bestspace_mutex);

	  last = NULL;
	  while ((ent =
		  (HEAP_BESTSPACE_ENTRY *) mht_get2 (heap_Bestspace->hfid_ht,
						     hfid, &last)) != NULL)
	    {
	      assert_release (!VPID_ISNULL (&ent->best.vpid));
	      if (!VPID_ISNULL (&ent->best.vpid))
		{
		  valid_pg = file_isvalid_page_partof (thread_p,
						       &ent->best.vpid,
						       &hfid->vfid);
		  if (valid_pg != DISK_VALID)
		    {
		      break;
		    }
		}
	      assert_release (ent->best.freespace > 0);
	    }

	  assert (mht_count (heap_Bestspace->vpid_ht) ==
		  mht_count (heap_Bestspace->hfid_ht));

	  pthread_mutex_unlock (&heap_Bestspace->bestspace_mutex);
	}
#endif

      pgbuf_unfix_and_init (thread_p, pgptr);

      /* Need to check for the overflow pages.... */
    }

  return valid_pg;
}

DISK_ISVALID
heap_check_heap_file (THREAD_ENTRY * thread_p, HFID * hfid)
{
  FILE_TYPE file_type;
  VPID vpid;
  FILE_HEAP_DES hfdes;
  DISK_ISVALID rv = DISK_VALID;

  file_type = file_get_type (thread_p, &hfid->vfid);
  if (file_type == FILE_UNKNOWN_TYPE || file_type != FILE_HEAP)
    {
      return DISK_ERROR;
    }

  if (file_find_nthpages (thread_p, &hfid->vfid, &vpid, 0, 1) == 1)
    {
      hfid->hpgid = vpid.pageid;

      if ((file_get_descriptor (thread_p, &hfid->vfid, &hfdes,
				sizeof (FILE_HEAP_DES)) > 0)
	  && !OID_ISNULL (&hfdes.class_oid))
	{
	  rv = heap_check_all_pages (thread_p, hfid);
	}
    }
  else
    {
      return DISK_ERROR;
    }

  return rv;
}

/*
 * heap_check_all_heaps () - Validate all pages of all known heap files
 *   return: DISK_INVALID, DISK_VALID, DISK_ERROR
 *
 * Note: Verify that all pages of all heap files are valid. That is,
 * that they are valid from the point of view of the file manager.
 */
DISK_ISVALID
heap_check_all_heaps (THREAD_ENTRY * thread_p)
{
  int num_files;
  HFID hfid;
  DISK_ISVALID allvalid = DISK_VALID;
  DISK_ISVALID valid = DISK_VALID;
  FILE_TYPE file_type;
  int i;

  /* Find number of files */
  num_files = file_get_numfiles (thread_p);
  if (num_files < 0)
    {
      return DISK_ERROR;
    }

  /* Go to each file, check only the heap files */
  for (i = 0; i < num_files && allvalid != DISK_ERROR; i++)
    {
      if (file_find_nthfile (thread_p, &hfid.vfid, i) != 1)
	{
	  break;
	}

      file_type = file_get_type (thread_p, &hfid.vfid);
      if (file_type == FILE_UNKNOWN_TYPE)
	{
	  return DISK_ERROR;
	}
      if (file_type != FILE_HEAP)
	{
	  continue;
	}

      valid = heap_check_heap_file (thread_p, &hfid);
      if (valid != DISK_VALID)
	{
	  allvalid = valid;
	}
    }

  return allvalid;
}
#endif

/*
 * heap_dump_hdr () - Dump heap file header
 *   return: NO_ERROR
 *   heap_hdr(in): Header structure
 */
static int
heap_dump_hdr (FILE * fp, HEAP_HDR_STATS * heap_hdr)
{
  int ret = NO_ERROR;

  fprintf (fp, "CLASS_OID = %2d|%4d|%2d, ",
	   heap_hdr->class_oid.volid, heap_hdr->class_oid.pageid,
	   heap_hdr->class_oid.slotid);
  fprintf (fp, "OVF_VFID = %4d|%4d, NEXT_VPID = %4d|%4d\n",
	   heap_hdr->ovf_vfid.volid, heap_hdr->ovf_vfid.fileid,
	   heap_hdr->next_vpid.volid, heap_hdr->next_vpid.pageid);

  fprintf (fp, "Next full search vpid = %4d|%4d\n",
	   heap_hdr->full_search_vpid.volid,
	   heap_hdr->full_search_vpid.pageid);

  return ret;
}

/*
 * heap_dump () - Dump heap file
 *   return:
 *   hfid(in): Heap file identifier
 *   dump_records(in): If true, objects are printed in ascii format, otherwise, the
 *              objects are not printed.
 *
 * Note: Dump a heap file. The objects are printed only when the value
 * of dump_records is true. This function is used for DEBUGGING PURPOSES.
 */
void
heap_dump (THREAD_ENTRY * thread_p, FILE * fp, HFID * hfid, bool dump_records)
{
  VPID vpid;			/* Page-volume identifier            */
  HEAP_HDR_STATS *heap_hdr;	/* Header of heap structure          */
  PAGE_PTR pgptr = NULL;	/* Page pointer                      */
  RECDES hdr_recdes = RECDES_INITIALIZER;	/* Header record descriptor          */
  VFID ovf_vfid;
  OID oid;
  HEAP_SCANCACHE scan_cache;
  HEAP_CACHE_ATTRINFO attr_info;
  RECDES peek_recdes = RECDES_INITIALIZER;
  FILE_HEAP_DES hfdes;
  PAGE_TYPE ptype;
  int ret = NO_ERROR;

  fprintf (fp, "\n\n*** DUMPING HEAP FILE: ");
  fprintf (fp, "volid = %d, Fileid = %d, Header-pageid = %d ***\n",
	   hfid->vfid.volid, hfid->vfid.fileid, hfid->hpgid);
  (void) file_dump_descriptor (thread_p, fp, &hfid->vfid);

  /* Fetch the header page of the heap file */

  vpid.volid = hfid->vfid.volid;
  vpid.pageid = hfid->hpgid;
  ptype = PAGE_HEAP_HEADER;

  pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, S_LOCK, NULL, ptype);
  if (pgptr == NULL)
    {
      /* Unable to fetch heap header page */
      return;
    }

  /* Peek the header record to dump */

  if (spage_get_record (pgptr, HEAP_HEADER_AND_CHAIN_SLOTID, &hdr_recdes,
			PEEK) != S_SUCCESS)
    {
      /* Unable to peek heap header record */
      pgbuf_unfix_and_init (thread_p, pgptr);
      return;
    }

  heap_hdr = (HEAP_HDR_STATS *) hdr_recdes.data;
  ret = heap_dump_hdr (fp, heap_hdr);
  if (ret != NO_ERROR)
    {
      pgbuf_unfix_and_init (thread_p, pgptr);
      return;
    }

  VFID_COPY (&ovf_vfid, &heap_hdr->ovf_vfid);
  pgbuf_unfix_and_init (thread_p, pgptr);

  /* now scan every page and dump it */
  vpid.volid = hfid->vfid.volid;
  vpid.pageid = hfid->hpgid;

  while (!VPID_ISNULL (&vpid))
    {
      pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, S_LOCK,
					   NULL, ptype);
      if (pgptr == NULL)
	{
	  /* something went wrong, return */
	  return;
	}
      spage_dump (thread_p, fp, pgptr, dump_records);
      (void) heap_vpid_next (hfid, pgptr, &vpid);
      pgbuf_unfix_and_init (thread_p, pgptr);

      ptype = PAGE_HEAP;
    }

  /* Dump file table configuration */
  if (file_dump (thread_p, fp, &hfid->vfid) != NO_ERROR)
    {
      return;
    }

  if (!VFID_ISNULL (&ovf_vfid))
    {
      /* There is an overflow file for this heap file */
      fprintf (fp, "\nOVERFLOW FILE INFORMATION FOR HEAP FILE\n\n");
      if (file_dump (thread_p, fp, &ovf_vfid) != NO_ERROR)
	{
	  return;
	}
    }

  /*
   * Dump schema definition
   */

  if (file_get_descriptor (thread_p, &hfid->vfid, &hfdes,
			   sizeof (FILE_HEAP_DES)) > 0
      && !OID_ISNULL (&hfdes.class_oid))
    {

      if (heap_attrinfo_start (thread_p, &hfdes.class_oid, -1, NULL,
			       &attr_info) != NO_ERROR)
	{
	  return;
	}

      ret = heap_classrepr_dump (thread_p, fp, &hfdes.class_oid,
				 attr_info.last_classrepr);
      if (ret != NO_ERROR)
	{
	  heap_attrinfo_end (thread_p, &attr_info);
	  return;
	}

      /* Dump individual Objects */
      if (dump_records == true)
	{
	  if (heap_scancache_start (thread_p, &scan_cache, hfid,
				    NULL, true) != NO_ERROR)
	    {
	      /* something went wrong, return */
	      heap_attrinfo_end (thread_p, &attr_info);
	      return;
	    }

	  OID_SET_NULL (&oid);
	  oid.volid = hfid->vfid.volid;

	  while (heap_next (thread_p, hfid, NULL, &oid, &peek_recdes,
			    &scan_cache, PEEK) == S_SUCCESS)
	    {
	      fprintf (fp,
		       "Object-OID = %2d|%4d|%2d,\n"
		       "  Length on disk = %d,\n",
		       oid.volid, oid.pageid, oid.slotid, peek_recdes.length);

	      if (heap_attrinfo_read_dbvalues
		  (thread_p, &oid, &peek_recdes, &attr_info) != NO_ERROR)
		{
		  fprintf (fp, "  Error ... continue\n");
		  continue;
		}
	      heap_attrinfo_dump (thread_p, fp, &attr_info, false);
	    }
	  heap_scancache_end (thread_p, &scan_cache);
	}
      heap_attrinfo_end (thread_p, &attr_info);
    }

  fprintf (fp, "\n\n*** END OF DUMP FOR HEAP FILE ***\n\n");
}

/*
 * heap_dump_all () - Dump all heap files
 *   return:
 *   dump_records(in): If true, objects are printed in ascii format, otherwise, the
 *              objects are not printed.
 */
void
heap_dump_all (THREAD_ENTRY * thread_p, FILE * fp, bool dump_records)
{
  int num_files;
  HFID hfid;
  VPID vpid;
  int i;

  /* Find number of files */
  num_files = file_get_numfiles (thread_p);
  if (num_files <= 0)
    {
      return;
    }

  /* Dump each heap file */
  for (i = 0; i < num_files; i++)
    {
      FILE_TYPE file_type = FILE_UNKNOWN_TYPE;

      if (file_find_nthfile (thread_p, &hfid.vfid, i) != 1)
	{
	  break;
	}

      file_type = file_get_type (thread_p, &hfid.vfid);
      if (file_type != FILE_HEAP)
	{
	  continue;
	}

      if (file_find_nthpages (thread_p, &hfid.vfid, &vpid, 0, 1) == 1)
	{
	  hfid.hpgid = vpid.pageid;
	  heap_dump (thread_p, fp, &hfid, dump_records);
	}
    }
}

/*
 * heap_dump_all_capacities () - Dump the capacities of all heap files.
 *   return:
 */
void
heap_dump_all_capacities (THREAD_ENTRY * thread_p, FILE * fp)
{
  HFID hfid;
  VPID vpid;
  int i;
  int num_files = 0;
  INT64 num_recs = 0;
  INT64 num_recs_relocated = 0;
  INT64 num_recs_inovf = 0;
  INT64 num_pages = 0;
  int avg_freespace = 0;
  int avg_freespace_nolast = 0;
  int avg_reclength = 0;
  int avg_overhead = 0;
  FILE_HEAP_DES hfdes;
  HEAP_CACHE_ATTRINFO attr_info;

  /* Find number of files */
  num_files = file_get_numfiles (thread_p);
  if (num_files <= 0)
    {
      return;
    }

  fprintf (fp, "IO_PAGESIZE = %d, DB_PAGESIZE = %d, Recv_overhead = %d\n",
	   IO_PAGESIZE, DB_PAGESIZE, IO_PAGESIZE - DB_PAGESIZE);

  /* Go to each file, check only the heap files */
  for (i = 0; i < num_files; i++)
    {
      FILE_TYPE file_type = FILE_UNKNOWN_TYPE;

      if (file_find_nthfile (thread_p, &hfid.vfid, i) != 1)
	{
	  break;
	}

      file_type = file_get_type (thread_p, &hfid.vfid);
      if (file_type != FILE_HEAP)
	{
	  continue;
	}

      if (file_find_nthpages (thread_p, &hfid.vfid, &vpid, 0, 1) == 1)
	{
	  hfid.hpgid = vpid.pageid;
	  if (heap_get_capacity (thread_p, &hfid, &num_recs,
				 &num_recs_relocated, &num_recs_inovf,
				 &num_pages, &avg_freespace,
				 &avg_freespace_nolast, &avg_reclength,
				 &avg_overhead) == NO_ERROR)
	    {
	      fprintf (fp,
		       "HFID:%d|%d|%d, Num_recs = %" PRId64
		       ", Num_reloc_recs = %" PRId64 ",\n"
		       "    Num_recs_inovf = %" PRId64
		       ", Avg_reclength = %d,\n"
		       "    Num_pages = %" PRId64
		       ", Avg_free_space_per_page = %d,\n"
		       "    Avg_free_space_per_page_without_lastpage = %d\n"
		       "    Avg_overhead_per_page = %d\n",
		       (int) hfid.vfid.volid, hfid.vfid.fileid,
		       hfid.hpgid, num_recs, num_recs_relocated,
		       num_recs_inovf, avg_reclength, num_pages,
		       avg_freespace, avg_freespace_nolast, avg_overhead);
	      /*
	       * Dump schema definition
	       */
	      if (file_get_descriptor (thread_p, &hfid.vfid, &hfdes,
				       sizeof (FILE_HEAP_DES)) > 0
		  && !OID_ISNULL (&hfdes.class_oid)
		  && heap_attrinfo_start (thread_p, &hfdes.class_oid, -1,
					  NULL, &attr_info) == NO_ERROR)
		{
		  (void) heap_classrepr_dump (thread_p, fp, &hfdes.class_oid,
					      attr_info.last_classrepr);
		  heap_attrinfo_end (thread_p, &attr_info);
		}
	      fprintf (fp, "\n");
	    }
	}
    }
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * heap_estimate_num_pages_needed () - Guess the number of pages needed to store a
 *                                set of instances
 *   return: int
 *   total_nobjs(in): Number of object to insert
 *   avg_obj_size(in): Average size of object
 *   num_attrs(in): Number of attributes
 *   num_var_attrs(in): Number of variable attributes
 *
 */
INT32
heap_estimate_num_pages_needed (THREAD_ENTRY * thread_p, int total_nobjs,
				int avg_obj_size, int num_attrs,
				int num_var_attrs)
{
  int nobj_page;
  INT32 npages;


  avg_obj_size += OR_HEADER_SIZE;

  if (num_attrs > 0)
    {
      avg_obj_size += CEIL_PTVDIV (num_attrs, 32) * sizeof (int);
    }
  if (num_var_attrs > 0)
    {
      avg_obj_size += (num_var_attrs + 1) * sizeof (int);
      /* Assume max padding of 3 bytes... */
      avg_obj_size += num_var_attrs * (sizeof (int) - 1);
    }

  avg_obj_size = DB_ALIGN (avg_obj_size, HEAP_MAX_ALIGN);

  /*
   * Find size of page available to store objects:
   * USER_SPACE_IN_PAGES = (DB_PAGESIZE * (1 - unfill_factor)
   *                        - SLOTTED PAGE HDR size overhead
   *                        - link of pages(i.e., sizeof(chain))
   *                        - slot overhead to store the link chain)
   */

  nobj_page = (DB_PAGESIZE - HEAP_UNFILL_SPACE - spage_header_size ()
	       - sizeof (HEAP_CHAIN) - spage_slot_size ());
  /*
   * Find the number of objects per page
   */

  nobj_page = nobj_page / (avg_obj_size + spage_slot_size ());

  /*
   * Find the number of pages. Add one page for file manager overhead
   */

  if (nobj_page > 0)
    {
      npages = CEIL_PTVDIV (total_nobjs, nobj_page);
      npages += file_guess_numpages_overhead (thread_p, NULL, npages);
    }
  else
    {
      /*
       * Overflow insertion
       */
      npages = overflow_estimate_npages_needed (thread_p, total_nobjs,
						avg_obj_size);

      /*
       * Find number of pages for the indirect record references (OIDs) to
       * overflow records
       */
      nobj_page = (DB_PAGESIZE - HEAP_UNFILL_SPACE - spage_header_size ()
		   - sizeof (HEAP_CHAIN) - spage_slot_size ());
      nobj_page = nobj_page / (sizeof (OID) + spage_slot_size ());
      /*
       * Now calculate the number of pages
       */
      nobj_page += CEIL_PTVDIV (total_nobjs, nobj_page);
      nobj_page += file_guess_numpages_overhead (thread_p, NULL, nobj_page);
      /*
       * Add the number of overflow pages and non-heap pages
       */
      npages = npages + nobj_page;
    }

  return npages;
}
#endif

/*
 *     	Check consistency of heap from the point of view of relocation
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_chkreloc_start () - Start validating consistency of relocated objects in
 *                        heap
 *   return: DISK_VALID, DISK_INVALID, DISK_ERROR
 *   chk(in): Structure for checking relocation objects
 *
 */
static DISK_ISVALID
heap_chkreloc_start (HEAP_CHKALL_RELOCOIDS * chk)
{
  chk->ht = mht_create ("Validate Relocation entries hash table",
			HEAP_CHK_ADD_UNFOUND_RELOCOIDS, oid_hash,
			oid_compare_equals);
  if (chk->ht == NULL)
    {
      chk->ht = NULL;
      chk->unfound_reloc_oids = NULL;
      chk->max_unfound_reloc = -1;
      chk->num_unfound_reloc = -1;
      return DISK_ERROR;
    }

  chk->unfound_reloc_oids =
    (OID *) malloc (sizeof (*chk->unfound_reloc_oids) *
		    HEAP_CHK_ADD_UNFOUND_RELOCOIDS);
  if (chk->unfound_reloc_oids == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (*chk->unfound_reloc_oids) *
	      HEAP_CHK_ADD_UNFOUND_RELOCOIDS);

      if (chk->ht != NULL)
	{
	  mht_destroy (chk->ht);
	}

      chk->ht = NULL;
      chk->unfound_reloc_oids = NULL;
      chk->max_unfound_reloc = -1;
      chk->num_unfound_reloc = -1;
      return DISK_ERROR;
    }

  chk->max_unfound_reloc = HEAP_CHK_ADD_UNFOUND_RELOCOIDS;
  chk->num_unfound_reloc = 0;
  chk->verify = true;

  return DISK_VALID;
}

/*
 * heap_chkreloc_end () - Finish validating consistency of relocated objects
 *                      in heap
 *   return: DISK_VALID, DISK_INVALID, DISK_ERROR
 *   chk(in): Structure for checking relocation objects
 *
 * Note: Scanning the unfound_reloc_oid list, remove those entries that
 * are also found in hash table (remove them from unfound_reloc
 * list and from hash table). At the end of the scan, if there
 * are any entries in either hash table or unfound_reloc_oid, the
 * heap is incosistent/corrupted.
 */
static DISK_ISVALID
heap_chkreloc_end (HEAP_CHKALL_RELOCOIDS * chk)
{
  HEAP_CHK_RELOCOID *forward;
  DISK_ISVALID valid_reloc = DISK_VALID;
  int i;

  /*
   * Check for any postponed unfound relocated OIDs that have not been
   * checked or found. If they are not in the hash table, it would be an
   * error. That is, we would have a relocated (content) object without an
   * object pointing to it. (relocation/home).
   */
  if (chk->verify == true)
    {
      for (i = 0; i < chk->num_unfound_reloc; i++)
	{
	  forward = (HEAP_CHK_RELOCOID *) mht_get (chk->ht,
						   &chk->unfound_reloc_oids
						   [i]);
	  if (forward != NULL)
	    {
	      /*
	       * The entry was found.
	       * Remove the entry and the memory space
	       */
	      /* mht_rem() has been updated to take a function and an arg pointer
	       * that can be called on the entry before it is removed.  We may
	       * want to take advantage of that here to free the memory associated
	       * with the entry
	       */
	      if (mht_rem (chk->ht, &chk->unfound_reloc_oids[i], NULL,
			   NULL) != NO_ERROR)
		{
		  valid_reloc = DISK_ERROR;
		}
	      else
		{
		  free_and_init (forward);
		}
	    }
	  else
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "Unable to find relocation/home object"
			    " for relocated_oid=%d|%d|%d\n",
			    (int) chk->unfound_reloc_oids[i].volid,
			    chk->unfound_reloc_oids[i].pageid,
			    (int) chk->unfound_reloc_oids[i].slotid);
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_GENERIC_ERROR, 1, "");
	      valid_reloc = DISK_INVALID;
	    }
	}
    }

  /*
   * If there are entries in the hash table, it would be problems. That is,
   * the relocated (content) objects were not found. That is, the home object
   * points to a dangling content object, or what it points is not a
   * relocated (newhome) object.
   */

  if (mht_count (chk->ht) > 0)
    {
      (void) mht_map (chk->ht, heap_chkreloc_print_notfound, chk);
      valid_reloc = DISK_INVALID;
    }

  mht_destroy (chk->ht);
  free_and_init (chk->unfound_reloc_oids);

  return valid_reloc;
}

/*
 * heap_chkreloc_print_notfound () - Print entry that does not have a relocated entry
 *   return: NO_ERROR
 *   ignore_reloc_oid(in): Key (relocated entry to real entry) of hash table
 *   ent(in): The entry associated with key (real oid)
 *   xchk(in): Structure for checking relocation objects
 *
 * Note: Print unfound relocated record information for this home
 * record with relocation address HEAP is inconsistent.
 */
static int
heap_chkreloc_print_notfound (const void *ignore_reloc_oid, void *ent,
			      void *xchk)
{
  HEAP_CHK_RELOCOID *forward = (HEAP_CHK_RELOCOID *) ent;
  HEAP_CHKALL_RELOCOIDS *chk = (HEAP_CHKALL_RELOCOIDS *) xchk;

  if (chk->verify == true)
    {
      er_log_debug (ARG_FILE_LINE, "Unable to find relocated record with"
		    " oid=%d|%d|%d for home object with oid=%d|%d|%d\n",
		    (int) forward->reloc_oid.volid,
		    forward->reloc_oid.pageid,
		    (int) forward->reloc_oid.slotid,
		    (int) forward->real_oid.volid,
		    forward->real_oid.pageid, (int) forward->real_oid.slotid);
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      "");
    }
  /* mht_rem() has been updated to take a function and an arg pointer
   * that can be called on the entry before it is removed.  We may
   * want to take advantage of that here to free the memory associated
   * with the entry
   */
  (void) mht_rem (chk->ht, &forward->reloc_oid, NULL, NULL);
  free_and_init (forward);

  return NO_ERROR;
}

/*
 * heap_chkreloc_next () - Verify consistency of relocation records on page heap
 *   return: DISK_VALID, DISK_INVALID, DISK_ERROR
 *   chk(in): Structure for checking relocation objects
 *   pgptr(in): Page pointer
 *
 * Note: While scanning objects of given page:
 *              1: if a relocation record is found, we check if that record
 *                 has already been seen (i.e., if it is in unfound_relc
 *                 list),
 *                 if it has been seen, we remove the entry from the
 *                 unfound_relc_oid list.
 *                 if it has not been seen, we add an entry to hash table
 *                 from reloc_oid to real_oid
 *                 Note: for optimization reasons, we may not scan the
 *                 unfound_reloc if it is too long, in this case the entry is
 *                 added to hash table.
 *              2: if a newhome (relocated) record is found, we check if the
 *                 real record has already been seen (i.e., check hash table),
 *                 if it has been seen, we remove the entry from hash table
 *                 otherwise, we add an entry into the unfound_reloc list
 */

#define HEAP_CHKRELOC_UNFOUND_SHORT 5

static DISK_ISVALID
heap_chkreloc_next (HEAP_CHKALL_RELOCOIDS * chk, PAGE_PTR pgptr)
{
  HEAP_CHK_RELOCOID *forward;
  INT16 type = REC_UNKNOWN;
  RECDES recdes = RECDES_INITIALIZER;
  OID oid;
  OID *peek_oid;
  void *ptr;
  bool found;
  int i;

  if (chk->verify != true)
    {
      return DISK_VALID;
    }

  oid.volid = pgbuf_get_volume_id (pgptr);
  oid.pageid = pgbuf_get_page_id (pgptr);
  oid.slotid = 0;		/* i.e., will get slot 1 */

  while (spage_next_record (pgptr, &oid.slotid, &recdes, PEEK) == S_SUCCESS)
    {
      if (oid.slotid == HEAP_HEADER_AND_CHAIN_SLOTID)
	{
	  continue;
	}
      type = spage_get_record_type (pgptr, oid.slotid);

      switch (type)
	{
	case REC_RELOCATION:
	  /*
	   * The record stored on the page is a relocation record,
	   * get the new home for the record
	   *
	   * If we have already entries waiting to be check and the list is
	   * not that big, check them. Otherwise, wait until the end for the
	   * check since searching the list may be expensive
	   */
	  peek_oid = (OID *) recdes.data;
	  found = false;
	  if (chk->num_unfound_reloc < HEAP_CHKRELOC_UNFOUND_SHORT)
	    {
	      /*
	       * Go a head and check since the list is very short.
	       */
	      for (i = 0; i < chk->num_unfound_reloc; i++)
		{
		  if (OID_EQ (&chk->unfound_reloc_oids[i], peek_oid))
		    {
		      /*
		       * Remove it from the unfound list
		       */
		      if ((i + 1) != chk->num_unfound_reloc)
			{
			  chk->unfound_reloc_oids[i] =
			    chk->unfound_reloc_oids[chk->num_unfound_reloc -
						    1];
			}
		      chk->num_unfound_reloc--;
		      found = true;
		      break;
		    }
		}
	    }
	  if (found == false)
	    {
	      /*
	       * Add it to hash table
	       */
	      forward =
		(HEAP_CHK_RELOCOID *) malloc (sizeof (HEAP_CHK_RELOCOID));
	      if (forward == NULL)
		{
		  /*
		   * Out of memory
		   */
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1,
			  sizeof (HEAP_CHK_RELOCOID));

		  return DISK_ERROR;
		}
	      forward->real_oid = oid;
	      forward->reloc_oid = *peek_oid;
	      if (mht_put (chk->ht, &forward->reloc_oid, forward) == NULL)
		{
		  /*
		   * Failure in mht_put
		   */
		  return DISK_ERROR;
		}
	    }
	  break;

	case REC_BIGONE:
	case REC_HOME:
	  break;

	case REC_NEWHOME:
	  /*
	   * Remove the object from hash table or insert the object in unfound
	   * reloc check list.
	   */
	  forward = (HEAP_CHK_RELOCOID *) mht_get (chk->ht, &oid);
	  if (forward != NULL)
	    {
	      /*
	       * The entry was found.
	       * Remove the entry and the memory space
	       */
	      /* mht_rem() has been updated to take a function and an arg pointer
	       * that can be called on the entry before it is removed.  We may
	       * want to take advantage of that here to free the memory associated
	       * with the entry
	       */
	      (void) mht_rem (chk->ht, &forward->reloc_oid, NULL, NULL);
	      free_and_init (forward);
	    }
	  else
	    {
	      /*
	       * The entry is not in hash table.
	       * Add entry into unfound_reloc list
	       */
	      if (chk->max_unfound_reloc <= chk->num_unfound_reloc)
		{
		  /*
		   * Need to realloc the area. Add 100 OIDs to it
		   */
		  i = (sizeof (*chk->unfound_reloc_oids)
		       * (chk->max_unfound_reloc +
			  HEAP_CHK_ADD_UNFOUND_RELOCOIDS));

		  ptr = realloc (chk->unfound_reloc_oids, i);
		  if (ptr == NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_OUT_OF_VIRTUAL_MEMORY, 1, i);
		      return DISK_ERROR;
		    }
		  else
		    {
		      chk->unfound_reloc_oids = (OID *) ptr;
		      chk->max_unfound_reloc +=
			HEAP_CHK_ADD_UNFOUND_RELOCOIDS;
		    }
		}
	      i = chk->num_unfound_reloc++;
	      chk->unfound_reloc_oids[i] = oid;
	    }
	  break;

	case REC_MARKDELETED:
	case REC_DELETED_WILL_REUSE:
	default:
	  break;
	}
    }

  return DISK_VALID;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * heap_stats_bestspace_initialize () - Initialize structure of best space
 *   return: NO_ERROR
 */
static int
heap_bestspace_initialize (void)
{
  int ret = NO_ERROR;

  if (heap_Bestspace != NULL)
    {
      ret = heap_bestspace_finalize ();
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }

  heap_Bestspace = &heap_Bestspace_cache_area;

  pthread_mutex_init (&heap_Bestspace->bestspace_mutex, NULL);

  heap_Bestspace->num_stats_entries = 0;

  heap_Bestspace->hfid_ht =
    mht_create ("Memory hash HFID to {bestspace}",
		HEAP_BESTSPACE_MHT_EST_SIZE, heap_hash_hfid,
		heap_compare_hfid);
  if (heap_Bestspace->hfid_ht == NULL)
    {
      goto exit_on_error;
    }

  heap_Bestspace->vpid_ht =
    mht_create ("Memory hash VPID to {bestspace}",
		HEAP_BESTSPACE_MHT_EST_SIZE, heap_hash_vpid,
		heap_compare_vpid);
  if (heap_Bestspace->vpid_ht == NULL)
    {
      goto exit_on_error;
    }

  heap_Bestspace->num_alloc = 0;
  heap_Bestspace->num_free = 0;
  heap_Bestspace->free_list_count = 0;
  heap_Bestspace->free_list = NULL;

  pthread_mutex_init (&heap_Bestspace->bestspace_sync.sync_mutex, NULL);
  heap_Bestspace->bestspace_sync.sync_list = NULL;
  heap_Bestspace->bestspace_sync.free_list_count = 0;
  heap_Bestspace->bestspace_sync.free_list = NULL;
  heap_Bestspace->bestspace_sync.stop_sync_bestspace = false;

  return ret;

exit_on_error:

  return (ret == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * heap_bestspace_finalize () - Finish best space information
 *   return: NO_ERROR
 *
 * Note: Destroy hash table and memory for entries.
 */
static int
heap_bestspace_finalize (void)
{
  HEAP_BESTSPACE_ENTRY *ent;
  HEAP_SYNC_NODE *sync_node;
  int ret = NO_ERROR;

  if (heap_Bestspace == NULL)
    {
      return NO_ERROR;
    }

  if (heap_Bestspace->vpid_ht != NULL)
    {
      (void) mht_map_no_key (NULL, heap_Bestspace->vpid_ht,
			     heap_bestspace_free_entry, NULL);
      while (heap_Bestspace->free_list_count > 0)
	{
	  ent = heap_Bestspace->free_list;
	  assert_release (ent != NULL);

	  heap_Bestspace->free_list = ent->next;
	  ent->next = NULL;

	  free (ent);

	  heap_Bestspace->free_list_count--;
	}
      assert_release (heap_Bestspace->free_list == NULL);
    }

  if (heap_Bestspace->vpid_ht != NULL)
    {
      mht_destroy (heap_Bestspace->vpid_ht);
      heap_Bestspace->vpid_ht = NULL;
    }

  if (heap_Bestspace->hfid_ht != NULL)
    {
      mht_destroy (heap_Bestspace->hfid_ht);
      heap_Bestspace->hfid_ht = NULL;
    }

  pthread_mutex_destroy (&heap_Bestspace->bestspace_mutex);

  heap_Bestspace->bestspace_sync.stop_sync_bestspace = true;

  while (heap_Bestspace->bestspace_sync.free_list != NULL)
    {
      sync_node = heap_Bestspace->bestspace_sync.free_list;
      heap_Bestspace->bestspace_sync.free_list =
	heap_Bestspace->bestspace_sync.free_list->next;

      free_and_init (sync_node);

      heap_Bestspace->bestspace_sync.free_list_count--;
    }
  assert_release (heap_Bestspace->bestspace_sync.free_list_count == 0);

  while (heap_Bestspace->bestspace_sync.sync_list != NULL)
    {
      sync_node = heap_Bestspace->bestspace_sync.sync_list;
      heap_Bestspace->bestspace_sync.sync_list =
	heap_Bestspace->bestspace_sync.sync_list->next;

      free_and_init (sync_node);
    }

  pthread_mutex_destroy (&heap_Bestspace->bestspace_sync.sync_mutex);

  heap_Bestspace = NULL;

  return ret;
}

/*
 * Recovery functions
 */

/*
 * heap_rv_redo_newpage_helper () - Redo the statistics or a new page
 *                                    allocation for a heap file
 *   return: int
 *   rcv(in): Recovery structure
 */
static int
heap_rv_redo_newpage_helper (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  RECDES recdes = RECDES_INITIALIZER;
  INT16 slotid;
  int sp_success;

  /* Initialize header page */
  spage_initialize (thread_p, rcv->pgptr, heap_get_spage_type (),
		    HEAP_MAX_ALIGN, SAFEGUARD_RVSPACE);

  /* Now insert first record (either statistics or chain record) */
  recdes.area_size = recdes.length = rcv->length;
  recdes.type = REC_HOME;
  recdes.data = (char *) rcv->data;
  sp_success = spage_insert (thread_p, rcv->pgptr, &recdes, &slotid);
  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);

  if (sp_success != SP_SUCCESS || slotid != HEAP_HEADER_AND_CHAIN_SLOTID)
    {
      if (sp_success != SP_SUCCESS)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
		  1, "");
	}
      /* something went wrong. Unable to redo initialization of new heap page */
      return er_errid ();
    }

  return NO_ERROR;
}

int
heap_rv_redo_newhdr (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  (void) pgbuf_set_page_ptype (thread_p, rcv->pgptr, PAGE_HEAP_HEADER);

  return heap_rv_redo_newpage_helper (thread_p, rcv);
}

int
heap_rv_redo_newpage (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  (void) pgbuf_set_page_ptype (thread_p, rcv->pgptr, PAGE_HEAP);

  return heap_rv_redo_newpage_helper (thread_p, rcv);
}

/*
 * heap_rv_undoredo_pagehdr () - Recover the header of a heap page
 *                    (either statistics/chain)
 *   return: int
 *   rcv(in): Recovery structure
 *
 * Note: Recover the update of the header or a heap page. The header
 * can be the heap header or a chain header.
 */
int
heap_rv_undoredo_pagehdr (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  RECDES recdes = RECDES_INITIALIZER;
  int sp_success;

  recdes.area_size = recdes.length = rcv->length;
  recdes.type = REC_HOME;
  recdes.data = (char *) rcv->data;

  sp_success = spage_update (thread_p, rcv->pgptr,
			     HEAP_HEADER_AND_CHAIN_SLOTID, &recdes);
  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);

  if (sp_success != SP_SUCCESS)
    {
      /* something went wrong. Unable to redo update statistics for chain */
      if (sp_success != SP_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
		  1, "");
	}
      return er_errid ();
    }
  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);

  return NO_ERROR;
}

/*
 * heap_rv_dump_statistics () - Dump statistics recovery information
 *   return: int
 *   ignore_length(in): Length of Recovery Data
 *   data(in): The data being logged
 *
 * Note: Dump statistics recovery information
 */
void
heap_rv_dump_statistics (FILE * fp, UNUSED_ARG int ignore_length, void *data)
{
  HEAP_HDR_STATS *heap_hdr;	/* Header of heap structure    */

  heap_hdr = (HEAP_HDR_STATS *) data;

  (void) heap_dump_hdr (fp, heap_hdr);
}

/*
 * heap_rv_dump_chain () - Dump chain recovery information
 *   return: int
 *   ignore_length(in): Length of Recovery Data
 *   data(in): The data being logged
 */
void
heap_rv_dump_chain (FILE * fp, UNUSED_ARG int ignore_length, void *data)
{
  HEAP_CHAIN *chain;

  chain = (HEAP_CHAIN *) data;
  fprintf (fp, "CLASS_OID = %2d|%4d|%2d, "
	   "PREV_VPID = %2d|%4d, NEXT_VPID = %2d|%4d\n",
	   chain->class_oid.volid, chain->class_oid.pageid,
	   chain->class_oid.slotid,
	   chain->prev_vpid.volid, chain->prev_vpid.pageid,
	   chain->next_vpid.volid, chain->next_vpid.pageid);
}

/*
 * heap_rv_redo_insert () - Redo the insertion of an object
 *   return: int
 *   rcv(in): Recovery structure
 *
 * Note: Redo the insertion of an object at a specific location (OID).
 */
int
heap_rv_redo_insert (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  INT16 slotid;
  RECDES recdes = RECDES_INITIALIZER;
  int sp_success;

  slotid = rcv->offset;
  recdes.type = *(INT16 *) (rcv->data);
  recdes.data = (char *) (rcv->data) + sizeof (recdes.type) + sizeof (OID);
  recdes.length = rcv->length - sizeof (recdes.type) - sizeof (OID);
  recdes.area_size = recdes.length;

  if (recdes.type == REC_ASSIGN_ADDRESS)
    {
      /*
       * The data here isn't really the data to be inserted (because there
       * wasn't any); instead it's the number of bytes that were reserved
       * for future insertion.  Change recdes.length to reflect the number
       * of bytes to reserve, but there's no need for a valid recdes.data:
       * spage_insert_for_recovery knows to ignore it in this case.
       */
      recdes.area_size = recdes.length = *(INT16 *) recdes.data;
      recdes.data = NULL;
    }

  sp_success = spage_insert_for_recovery (thread_p, rcv->pgptr, slotid,
					  &recdes);
  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);

  if (sp_success != SP_SUCCESS)
    {
      /* Unable to redo insertion */
      if (sp_success != SP_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
		  1, "");
	}
      return er_errid ();
    }

  return NO_ERROR;
}

/*
 * heap_rv_undo_insert () - Undo the insertion of an object.
 *   return: int
 *   rcv(in): Recovery structure
 *
 * Note: Delete an object for recovery purposes. The OID of the object
 * is reused since the object was never committed.
 */
int
heap_rv_undo_insert (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  INT16 slotid;

  slotid = rcv->offset;
  (void) spage_delete_for_recovery (thread_p, rcv->pgptr, slotid);
  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);

  return NO_ERROR;
}

/*
 * heap_rv_redo_delete () - Redo the deletion of an object
 *   return: int
 *   rcv(in): Recovery structure
 *
 * Note: Redo the deletion of an object.
 * The OID of the object is not reuse since we don't know if the object was a
 * newly created object.
 */
int
heap_rv_redo_delete (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  INT16 slotid;

  slotid = rcv->offset;
  (void) spage_delete (thread_p, rcv->pgptr, slotid);
  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);

  return NO_ERROR;
}

/*
 * heap_rv_undo_delete () - Undo the deletion of an object
 *   return: int
 *   rcv(in): Recovery structure
 */
int
heap_rv_undo_delete (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  return heap_rv_redo_insert (thread_p, rcv);
}

/*
 * heap_rv_undoredo_update () - Recover an update either for undo or redo
 *   return: int
 *   rcv(in): Recovery structure
 *
 * Note: Recover an update to an object in a slotted page
 */
int
heap_rv_undoredo_update (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  INT16 slotid;
  RECDES recdes = RECDES_INITIALIZER;
  int sp_success;

  slotid = rcv->offset;
  recdes.type = *(INT16 *) (rcv->data);
  recdes.data = (char *) (rcv->data) + sizeof (recdes.type) + sizeof (OID);
  recdes.length = rcv->length - sizeof (recdes.type) - sizeof (OID);
  recdes.area_size = recdes.length;

  if (recdes.area_size <= 0)
    {
      sp_success = SP_SUCCESS;
    }
  else
    {
      sp_success = spage_update (thread_p, rcv->pgptr, slotid, &recdes);
    }

  if (sp_success != SP_SUCCESS)
    {
      /* Unable to recover update for object */
      pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);
      if (sp_success != SP_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
		  1, "");
	}
      return er_errid ();
    }
  spage_update_record_type (thread_p, rcv->pgptr, slotid, recdes.type);
  pgbuf_set_dirty (thread_p, rcv->pgptr, DONT_FREE);

  return NO_ERROR;
}

int
heap_rv_undo_create (THREAD_ENTRY * thread_p, LOG_RCV * rcv)
{
  HFID *hfid;

  hfid = (HFID *) (rcv->data);

  return xheap_destroy (thread_p, hfid);
}

/*
 * xheap_get_class_num_objects_pages () -
 *   return: NO_ERROR
 *   hfid(in):
 *   approximation(in):
 *   nobjs(in):
 *   npages(in):
 */
int
xheap_get_class_num_objects_pages (THREAD_ENTRY * thread_p, const HFID * hfid,
				   int approximation, DB_BIGINT * nobjs,
				   int *npages)
{
  int ret = NO_ERROR;

  *npages = file_get_numpages (thread_p, &hfid->vfid, NULL, NULL, NULL);

  if (approximation)
    {
      ret = heap_estimate_num_objects (thread_p, hfid, nobjs);
    }
  else
    {
      ret = heap_get_num_objects (thread_p, hfid, nobjs);
    }

  return ret;
}

/*
 * heap_get_class_repr_id () -
 *   return:
 *   class_oid(in):
 */
REPR_ID
heap_get_class_repr_id (THREAD_ENTRY * thread_p, OID * class_oid)
{
  OR_CLASSREP *rep;
  REPR_ID id;
  int idx_incache = -1;

  if (!class_oid || !idx_incache)
    {
      return 0;
    }

  rep = heap_classrepr_get (thread_p, class_oid, NULL, 0, &idx_incache, true);
  if (rep == NULL)
    {
      return 0;
    }

  id = rep->id;
  (void) heap_classrepr_free (rep, &idx_incache);

  return id;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_attrinfo_set_uninitialized_global () -
 *   return: NO_ERROR
 *   inst_oid(in):
 *   recdes(in):
 *   attr_info(in):
 */
int
heap_attrinfo_set_uninitialized_global (THREAD_ENTRY * thread_p,
					OID * inst_oid, RECDES * recdes,
					HEAP_CACHE_ATTRINFO * attr_info)
{
  if (attr_info == NULL)
    {
      return ER_FAILED;
    }

  return heap_attrinfo_set_uninitialized (thread_p, inst_oid, recdes,
					  attr_info);
}
#endif

/*
 * heap_get_hfid_from_class_oid () - get HFID from class oid
 *   return: error_code
 *   class_oid(in): class oid
 *   hfid(out):  the resulting hfid
 */
int
heap_get_hfid_from_class_oid (THREAD_ENTRY * thread_p, const OID * class_oid,
			      HFID * hfid)
{
  int error_code = NO_ERROR;
  RECDES recdes = RECDES_INITIALIZER;
  HEAP_SCANCACHE scan_cache;

  if (class_oid == NULL || hfid == NULL)
    {
      return ER_FAILED;
    }

  error_code = heap_scancache_quick_start (&scan_cache);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (heap_get (thread_p, class_oid, &recdes, &scan_cache, PEEK) != S_SUCCESS)
    {
      heap_scancache_end (thread_p, &scan_cache);
      return ER_FAILED;
    }

  orc_class_hfid_from_record (&recdes, hfid);

  error_code = heap_scancache_end (thread_p, &scan_cache);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return error_code;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * heap_compact_pages () - compact all pages from hfid of specified class OID
 *   return: error_code
 *   class_oid(out):  the class oid
 */
int
heap_compact_pages (THREAD_ENTRY * thread_p, OID * class_oid)
{
  int ret = NO_ERROR;
  VPID vpid;
  PAGE_PTR hdr_pgptr = NULL;
  VPID next_vpid;
  PAGE_PTR pgptr = NULL;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;
  int lock_timeout = 2000;
  HFID hfid;
  DB_VALUE val;

  if (class_oid == NULL)
    {
      return ER_QPROC_INVALID_PARAMETER;
    }

  DB_MAKE_OID (&val, class_oid);
  if (lock_object (thread_p, &val, S_LOCK, LK_UNCOND_LOCK) != LK_GRANTED)
    {
      return ER_FAILED;
    }

  ret = heap_get_hfid_from_class_oid (thread_p, class_oid, &hfid);
  if (ret != NO_ERROR || HFID_IS_NULL (&hfid))
    {
      lock_unlock_object (thread_p, &val, LK_UNLOCK_TYPE_NORMAL);
      return ret;
    }

  addr.vfid = &hfid.vfid;
  addr.pgptr = NULL;
  addr.offset = 0;

  vpid.volid = hfid.vfid.volid;
  vpid.pageid = hfid.hpgid;

  hdr_pgptr = pgbuf_fix (thread_p, &vpid, OLD_PAGE, PGBUF_LATCH_READ,
			 PGBUF_UNCONDITIONAL_LATCH);
  if (hdr_pgptr == NULL)
    {
      lock_unlock_object (thread_p, &val, LK_UNLOCK_TYPE_NORMAL);
      ret = ER_FAILED;
      goto exit_on_error;
    }

  lock_unlock_object (thread_p, &val, LK_UNLOCK_TYPE_NORMAL);

  next_vpid = vpid;
  while (!VPID_ISNULL (&next_vpid))
    {
      vpid = next_vpid;
      pgptr = heap_scan_pb_lock_and_fetch (thread_p, &vpid, OLD_PAGE, X_LOCK,
					   NULL, PAGE_HEAP);
      if (pgptr == NULL)
	{
	  ret = ER_FAILED;
	  goto exit_on_error;
	}

      ret = heap_vpid_next (&hfid, pgptr, &next_vpid);
      if (ret != NO_ERROR)
	{
	  pgbuf_unfix_and_init (thread_p, pgptr);
	  goto exit_on_error;
	}

      if (spage_compact (pgptr) != NO_ERROR)
	{
	  pgbuf_unfix_and_init (thread_p, pgptr);
	  ret = ER_FAILED;
	  goto exit_on_error;
	}

      addr.pgptr = pgptr;
      log_skip_logging_set_lsa (thread_p, &addr);
      pgbuf_set_dirty (thread_p, pgptr, DONT_FREE);
      pgbuf_unfix_and_init (thread_p, pgptr);
    }

  pgbuf_unfix_and_init (thread_p, hdr_pgptr);
  hdr_pgptr = NULL;

  return ret;

exit_on_error:

  if (hdr_pgptr)
    {
      pgbuf_unfix_and_init (thread_p, hdr_pgptr);
    }

  return ret;
}
#endif

/*
 * heap_classrepr_dump_all () - dump all representations belongs to a class
 *   return: none
 *   fp(in): file pointer to print out
 *   class_oid(in): class oid to be dumped
 */
void
heap_classrepr_dump_all (THREAD_ENTRY * thread_p, FILE * fp, OID * class_oid)
{
  RECDES peek_recdes = RECDES_INITIALIZER;
  HEAP_SCANCACHE scan_cache;
  OR_CLASSREP **rep_all;
  int count, i;
  char *classname;
  bool need_free_classname = false;

  classname = heap_get_class_name (thread_p, class_oid);
  if (classname == NULL)
    {
      classname = (char *) "unknown";
    }
  else
    {
      need_free_classname = true;
    }

  heap_scancache_quick_start (&scan_cache);

  if (heap_get (thread_p, class_oid, &peek_recdes, &scan_cache,
		PEEK) == S_SUCCESS)
    {
      rep_all = or_get_all_representation (&peek_recdes, &count);
      assert (rep_all != NULL);
      if (rep_all != NULL)
	{
	  fprintf (fp, "*** Dumping representations of class %s\n"
		   "    Classname = %s, Class-OID = %d|%d|%d, #Repr = %d\n\n",
		   classname, classname, (int) class_oid->volid,
		   class_oid->pageid, (int) class_oid->slotid, count);

	  for (i = 0; i < count; i++)
	    {
	      assert (rep_all[i] != NULL);
	      heap_classrepr_dump (thread_p, fp, class_oid, rep_all[i]);
	      or_free_classrep (rep_all[i]);
	    }

	  fprintf (fp, "\n*** End of dump.\n");
	  free_and_init (rep_all);
	}
    }

  heap_scancache_end (thread_p, &scan_cache);

  if (need_free_classname)
    {
      free_and_init (classname);
    }
}

/*
 * heap_get_btid_from_index_name () - gets the BTID of an index using its name
 *				      and OID of class
 *
 *   return: NO_ERROR, or error code
 *   thread_p(in)   : thread context
 *   p_class_oid(in): OID of class
 *   index_name(in) : name of index
 *   p_found_btid(out): the BTREE ID of index
 *
 *  Note : the 'p_found_btid' argument must be a pointer to a BTID value,
 *	   the found BTID is 'BTID_COPY-ed' into it.
 *	   Null arguments are not allowed.
 *	   If an index name is not found, the 'p_found_btid' is returned as
 *	   NULL BTID and no error is set.
 *
 */
int
heap_get_btid_from_index_name (THREAD_ENTRY * thread_p,
			       const OID * p_class_oid,
			       const char *index_name, BTID * p_found_btid)
{
  int error = NO_ERROR;
  int classrepr_cacheindex = -1;
  int i;
  OR_CLASSREP *classrepr = NULL;
  OR_INDEX *curr_index = NULL;

  assert (p_found_btid != NULL);
  assert (p_class_oid != NULL);
  assert (index_name != NULL);

  BTID_SET_NULL (p_found_btid);

  /* get the BTID associated from the index name : the only structure
   * containing this info is OR_CLASSREP */

  /* get class representation */
  classrepr = heap_classrepr_get (thread_p, (OID *) p_class_oid, NULL, 0,
				  &classrepr_cacheindex, true);

  if (classrepr == NULL)
    {
      error = er_errid ();
      goto exit;
    }

  /* iterate through indexes looking for index name */
  for (i = 0; i < classrepr->n_indexes; i++)
    {
      curr_index = &(classrepr->indexes[i]);
      if (curr_index == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_UNEXPECTED, 0);
	  error = ER_UNEXPECTED;
	  goto exit_cleanup;
	}

      if (intl_identifier_casecmp (curr_index->btname, index_name) == 0)
	{
	  BTID_COPY (p_found_btid, &(curr_index->btid));
	  break;
	}
    }

exit_cleanup:
  if (classrepr)
    {
      (void) heap_classrepr_free (classrepr, &classrepr_cacheindex);
    }

exit:
  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * fill_string_to_buffer () - fill string into buffer
 *
 *   -----------------------------
 *   |        buffer             |
 *   -----------------------------
 *   ^                           ^
 *   |                           |
 *   start                       end
 *
 *   return: the count of characters (not include '\0') which has been
 *           filled into buffer; -1 means error.
 *   start(in/out): After filling, start move to the '\0' position.
 *   end(in): The first unavailble position.
 *   str(in):
 */
static int
fill_string_to_buffer (char **start, char *end, const char *str)
{
  int len = strlen (str);

  if (*start + len >= end)
    {
      return -1;
    }

  memcpy (*start, str, len);
  *start += len;
  **start = '\0';

  return len;
}
#endif

/*
 * heap_pgbuf_fix () -
 *
 */
PAGE_PTR
heap_pgbuf_fix (THREAD_ENTRY * thread_p, const HFID * hfid,
		const VPID * vpid,
		int requestmode, PGBUF_LATCH_CONDITION condition,
		const PAGE_TYPE ptype)
{
  PAGE_PTR page_ptr = NULL;
#if !defined(NDEBUG)
  VPID next_vpid;
#endif

  assert (ptype == PAGE_HEAP_HEADER || ptype == PAGE_HEAP);

  page_ptr =
    pgbuf_fix (thread_p, vpid, OLD_PAGE, requestmode, condition, ptype);

#if !defined(NDEBUG)
  if (hfid != NULL && page_ptr != NULL)
    {
      assert (file_find_page (thread_p, &(hfid->vfid), vpid) == true);

      if (spage_number_of_records (page_ptr) > 0)
	{
	  (void) heap_vpid_next (hfid, page_ptr, &next_vpid);

	  assert (VPID_ISNULL (&next_vpid)
		  || file_find_page (thread_p, &(hfid->vfid),
				     &next_vpid) == true);
	}
    }
#endif

  return page_ptr;
}

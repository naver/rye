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
 * heap_file.h: Heap file object manager (at Server)
 */

#ifndef _HEAP_FILE_H_
#define _HEAP_FILE_H_

#ident "$Id$"

#include "config.h"

#include "error_manager.h"
#include "storage_common.h"
#include "locator.h"
#include "file_manager.h"
#include "disk_manager.h"
#include "slotted_page.h"
#include "oid.h"
#include "object_representation_sr.h"
#include "thread.h"
#include "system_catalog.h"
#include "page_buffer.h"

#define HFID_EQ(hfid_ptr1, hfid_ptr2) \
  ((hfid_ptr1) == (hfid_ptr2) || \
   ((hfid_ptr1)->hpgid == (hfid_ptr2)->hpgid && \
    VFID_EQ(&((hfid_ptr1)->vfid), &((hfid_ptr2)->vfid))))

#define HEAP_HEADER_AND_CHAIN_SLOTID  0	/* Slot for chain and header */

#define HEAP_MAX_ALIGN INT_ALIGNMENT	/* maximum alignment for heap record */

/*
 * Heap scan structures
 */

typedef struct heap_bestspace HEAP_BESTSPACE;
struct heap_bestspace
{
  VPID vpid;			/* Vpid of one of the best pages */
  int freespace;		/* Estimated free space in this page */
};


typedef struct heap_scancache HEAP_SCANCACHE;
struct heap_scancache
{				/* Define a scan over the whole heap file  */
  int debug_initpattern;	/* A pattern which indicates that the
				 * structure has been initialized
				 */
  HFID hfid;			/* Heap file of scan                   */
  bool vpid_alloc;
  OID class_oid;		/* Class oid of scanned instances       */
  LOCK page_latch;		/* Indicates the latch/lock to be acquired
				 * on heap pages. Its value may be
				 * NULL_LOCK when it is secure to skip
				 * lock on heap pages. For example, the class
				 * of the heap has been locked with either
				 * S_LOCK, SIX_LOCK, or X_LOCK
				 */
  int cache_last_fix_page;	/* Indicates if page buffers and memory
				 * are cached (left fixed)
				 */
  PAGE_PTR pgptr;		/* Page pointer to last left fixed page */
  char *area;			/* Pointer to last left fixed memory
				 * allocated
				 */
  int area_size;		/* Size of allocated area               */
  VPID collect_nxvpid;		/* Next page where statistics are used  */
  bool read_committed_page;
  VPID last_vpid;
};

typedef enum
{
  HEAP_READ_ATTRVALUE,
  HEAP_WRITTEN_ATTRVALUE,
  HEAP_UNINIT_ATTRVALUE,
  HEAP_WRITTEN_LOB_ATTRVALUE
} HEAP_ATTRVALUE_STATE;

typedef struct heap_attrvalue HEAP_ATTRVALUE;
struct heap_attrvalue
{
  ATTR_ID attrid;		/* attribute identifier                       */
  HEAP_ATTRVALUE_STATE state;	/* State of the attribute value. Either of
				 * has been read, has been updated, or is
				 * unitialized
				 */
  OR_ATTRIBUTE *last_attrepr;	/* Used for default values                    */
  OR_ATTRIBUTE *read_attrepr;	/* Pointer to a desired attribute information */
  DB_VALUE dbvalue;		/* DB values of the attribute in memory       */
};

typedef struct heap_cache_attrinfo HEAP_CACHE_ATTRINFO;
struct heap_cache_attrinfo
{
  OID class_oid;		/* Class object identifier               */
  int last_cacheindex;		/* An index identifier when the
				 * last_classrepr was obtained from the
				 * classrepr cache. Otherwise, -1
				 */
  int read_cacheindex;		/* An index identifier when the
				 * read_classrepr was obtained from the
				 * classrepr cache. Otherwise, -1
				 */
  OR_CLASSREP *last_classrepr;	/* Currently cached catalog attribute
				 * info.
				 */
  OR_CLASSREP *read_classrepr;	/* Currently cached catalog attribute
				 * info.
				 */
  OID inst_oid;			/* Instance Object identifier            */
  int num_values;		/* Number of desired attribute values    */
  HEAP_ATTRVALUE *values;	/* Value for the attributes              */
};

/*
 * Heap file header
 */

typedef struct heap_hdr_stats HEAP_HDR_STATS;
struct heap_hdr_stats
{
  /* the first must be class_oid */
  OID class_oid;
  VFID ovf_vfid;		/* Overflow file identifier (if any)      */
  VPID next_vpid;		/* Next page (i.e., the 2nd page of heap
				 * file)
				 */
  VPID full_search_vpid;

  int reserve1_for_future;	/* Nothing reserved for future             */
  int reserve2_for_future;	/* Nothing reserved for future             */
};

#if 0				/* TODO: check not use - ksseo */
typedef struct heap_spacecache HEAP_SPACECACHE;
struct heap_spacecache
{				/* Define an alter space cache for heap file  */

  float remain_sumlen;		/* Total new length of records that it
				 * is predicted for the rest of space
				 * cache. If it is unknown -1 is stored.
				 * This value is used to estimate the
				 * number of pages to allocate at a
				 * particular time in space cache.
				 * If the value is < pagesize, only one
				 * page at a time is allocated.
				 */
};
#endif

enum
{ END_SCAN, CONTINUE_SCAN };

typedef struct heap_idx_elements_info HEAP_IDX_ELEMENTS_INFO;
struct heap_idx_elements_info
{
  int num_btids;		/* class has # of btids          */
  int has_single_col;		/* class has single column index */
  int has_multi_col;		/* class has multi-column index  */
};

/* class representations cache interface */
extern int heap_classrepr_decache (THREAD_ENTRY * thread_p,
				   const OID * class_oid);
extern int heap_classrepr_decache_and_lock (THREAD_ENTRY * thread_p,
					    const OID * class_oid);
extern int heap_classrepr_unlock_class (const OID * class_oid);
extern OR_CLASSREP *heap_classrepr_get (THREAD_ENTRY * thread_p,
					OID * class_oid,
					RECDES * class_recdes, REPR_ID reprid,
					int *idx_incache,
					bool use_last_reprid);
extern int heap_classrepr_free (OR_CLASSREP * classrep, int *idx_incache);
extern void heap_classrepr_dump_all (THREAD_ENTRY * thread_p, FILE * fp,
				     OID * class_oid);
extern bool
heap_classrepr_has_cache_entry (THREAD_ENTRY * thread_p,
				const OID * class_oid);
#ifdef DEBUG_CLASSREPR_CACHE
extern int heap_classrepr_dump_anyfixed (void);
#endif /* DEBUG_CLASSREPR_CACHE */


extern int heap_manager_initialize (void);
extern int heap_manager_finalize (void);
extern int heap_assign_address (THREAD_ENTRY * thread_p, const HFID * hfid,
				OID * oid, int expected_length);
extern int heap_assign_address_with_class_oid (THREAD_ENTRY * thread_p,
					       const HFID * hfid,
					       OID * class_oid, OID * oid,
					       int expected_length);
extern OID *heap_insert (THREAD_ENTRY * thread_p, const HFID * hfid,
			 OID * class_oid, OID * oid, RECDES * recdes,
			 HEAP_SCANCACHE * scan_cache);
extern const OID *heap_update (THREAD_ENTRY * thread_p, const HFID * hfid,
			       const OID * class_oid, OID * oid,
			       RECDES * recdes, bool * old,
			       HEAP_SCANCACHE * scan_cache);
extern const OID *heap_delete (THREAD_ENTRY * thread_p, const HFID * hfid,
			       const OID * oid, HEAP_SCANCACHE * scan_cache,
			       OID * class_oid);
extern void heap_flush (THREAD_ENTRY * thread_p, const OID * oid);
extern int heap_scancache_start (THREAD_ENTRY * thread_p,
				 HEAP_SCANCACHE * scan_cache,
				 const HFID * hfid, const OID * class_oid,
				 int cache_last_fix_page);
extern int heap_scancache_start_modify (THREAD_ENTRY * thread_p,
					HEAP_SCANCACHE * scan_cache,
					const HFID * hfid,
					const int force_page_allocation,
					const OID * class_oid);
extern int heap_scancache_quick_start (HEAP_SCANCACHE * scan_cache);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int heap_scancache_quick_start_modify (HEAP_SCANCACHE * scan_cache);
#endif
extern int heap_scancache_end (THREAD_ENTRY * thread_p,
			       HEAP_SCANCACHE * scan_cache);
extern int heap_scancache_end_when_scan_will_resume (THREAD_ENTRY * thread_p,
						     HEAP_SCANCACHE *
						     scan_cache);
extern void heap_scancache_end_modify (THREAD_ENTRY * thread_p,
				       HEAP_SCANCACHE * scan_cache);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int heap_get_chn (THREAD_ENTRY * thread_p, const OID * oid);
#endif
extern SCAN_CODE heap_get (THREAD_ENTRY * thread_p, const OID * oid,
			   RECDES * recdes, HEAP_SCANCACHE * scan_cache,
			   int ispeeking);
extern SCAN_CODE heap_get_with_class_oid (THREAD_ENTRY * thread_p,
					  OID * class_oid, const OID * oid,
					  RECDES * recdes,
					  HEAP_SCANCACHE * scan_cache,
					  int ispeeking);
extern SCAN_CODE heap_next (THREAD_ENTRY * thread_p, const HFID * hfid,
			    OID * class_oid, OID * next_oid, RECDES * recdes,
			    HEAP_SCANCACHE * scan_cache, int ispeeking);
extern SCAN_CODE heap_first (THREAD_ENTRY * thread_p, const HFID * hfid,
			     OID * class_oid, OID * oid, RECDES * recdes,
			     HEAP_SCANCACHE * scan_cache, int ispeeking);
extern int heap_get_alloc (THREAD_ENTRY * thread_p, const OID * oid,
			   RECDES * recdes);

#if defined (ENABLE_UNUSED_FUNCTION)
extern SCAN_CODE heap_last (THREAD_ENTRY * thread_p, const HFID * hfid,
			    OID * class_oid, OID * oid, RECDES * recdes,
			    HEAP_SCANCACHE * scan_cache, int ispeeking);
extern int heap_cmp (THREAD_ENTRY * thread_p, const OID * oid,
		     RECDES * recdes);

extern bool heap_does_exist (THREAD_ENTRY * thread_p, OID * class_oid,
			     const OID * oid);
#endif /* ENABLE_UNUSED_FUNCTION */

extern int heap_get_num_objects (THREAD_ENTRY * thread_p, const HFID * hfid,
				 DB_BIGINT * nobjs);

extern int heap_estimate_num_objects (THREAD_ENTRY * thread_p,
				      const HFID * hfid,
				      DB_BIGINT * num_objects);
#if defined(ENABLE_UNUSED_FUNCTION)
extern INT32 heap_estimate_num_pages_needed (THREAD_ENTRY * thread_p,
					     int total_nobjs,
					     int avg_obj_size, int num_attrs,
					     int num_var_attrs);
#endif

extern OID *heap_get_class_oid (THREAD_ENTRY * thread_p, OID * class_oid,
				const OID * oid);
extern char *heap_get_class_name (THREAD_ENTRY * thread_p,
				  const OID * class_oid);
extern char *heap_get_class_name_alloc_if_diff (THREAD_ENTRY * thread_p,
						const OID * class_oid,
						char *guess_classname);
extern char *heap_get_class_name_of_instance (THREAD_ENTRY * thread_p,
					      const OID * inst_oid);
#if defined (ENABLE_UNUSED_FUNCTION)
extern char *heap_get_class_name_with_is_class (THREAD_ENTRY * thread_p,
						const OID * oid,
						int *isclass);
#endif
extern int heap_attrinfo_start (THREAD_ENTRY * thread_p,
				const OID * class_oid,
				int requested_num_attrs,
				const ATTR_ID * attrid,
				HEAP_CACHE_ATTRINFO * attr_info);
extern void heap_attrinfo_end (THREAD_ENTRY * thread_p,
			       HEAP_CACHE_ATTRINFO * attr_info);
extern int heap_attrinfo_clear_dbvalues (HEAP_CACHE_ATTRINFO * attr_info);
extern int heap_attrinfo_read_dbvalues (THREAD_ENTRY * thread_p,
					const OID * inst_oid, RECDES * recdes,
					HEAP_CACHE_ATTRINFO * attr_info);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int heap_attrinfo_delete_lob (THREAD_ENTRY * thread_p, RECDES * recdes,
				     HEAP_CACHE_ATTRINFO * attr_info);
#endif
extern DB_VALUE *heap_attrinfo_access (ATTR_ID attrid,
				       HEAP_CACHE_ATTRINFO * attr_info);
extern int heap_attrinfo_set (const OID * inst_oid, ATTR_ID attrid,
			      DB_VALUE * attr_val,
			      HEAP_CACHE_ATTRINFO * attr_info);
extern SCAN_CODE heap_attrinfo_transform_to_disk (THREAD_ENTRY * thread_p,
						  HEAP_CACHE_ATTRINFO *
						  attr_info,
						  RECDES * old_recdes,
						  RECDES * new_recdes,
						  int shard_groupid);
extern int heap_attrinfo_start_with_index (THREAD_ENTRY * thread_p,
					   OID * class_oid,
					   RECDES * class_recdes,
					   HEAP_CACHE_ATTRINFO * attr_info,
					   HEAP_IDX_ELEMENTS_INFO * idx_info);
extern int heap_attrinfo_start_with_btid (THREAD_ENTRY * thread_p,
					  OID * class_oid, BTID * btid,
					  HEAP_CACHE_ATTRINFO * attr_info);

#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_VALUE *heap_attrvalue_get_index (int value_index,
					   ATTR_ID * attrid, int *n_btids,
					   BTID ** btids,
					   HEAP_CACHE_ATTRINFO *
					   idx_attrinfo);
#endif
extern HEAP_ATTRVALUE *heap_attrvalue_locate (ATTR_ID attrid,
					      HEAP_CACHE_ATTRINFO *
					      attr_info);
extern OR_ATTRIBUTE *heap_locate_last_attrepr (ATTR_ID attrid,
					       HEAP_CACHE_ATTRINFO *
					       attr_info);
extern int heap_attrvalue_get_key (THREAD_ENTRY * thread_p,
				   int btid_index,
				   HEAP_CACHE_ATTRINFO * idx_attrinfo,
				   const OID * inst_oid,
				   RECDES * recdes, BTID * btid,
				   DB_IDXKEY * key);

#if defined (ENABLE_UNUSED_FUNCTION)
extern BTID *heap_indexinfo_get_btid (int btid_index,
				      HEAP_CACHE_ATTRINFO * attrinfo);
extern int heap_indexinfo_get_num_attrs (int btid_index,
					 HEAP_CACHE_ATTRINFO * attrinfo);
extern int heap_indexinfo_get_attrids (int btid_index,
				       HEAP_CACHE_ATTRINFO * attrinfo,
				       ATTR_ID * attrids);
extern int heap_get_index_with_name (THREAD_ENTRY * thread_p,
				     OID * class_oid, const char *index_name,
				     BTID * btid);
#endif
extern int heap_get_indexname_of_btid (THREAD_ENTRY * thread_p,
				       OID * class_oid, BTID * btid,
				       char **btnamepp);

extern int heap_prefetch (THREAD_ENTRY * thread_p, OID * class_oid,
			  const OID * oid, LC_COPYAREA_DESC * prefetch);
#if defined (ENABLE_UNUSED_FUNCTION)
extern DISK_ISVALID heap_check_all_pages (THREAD_ENTRY * thread_p,
					  HFID * hfid);
extern DISK_ISVALID heap_check_heap_file (THREAD_ENTRY * thread_p,
					  HFID * hfid);
extern DISK_ISVALID heap_check_all_heaps (THREAD_ENTRY * thread_p);
#endif

/* Misc */
extern int xheap_get_class_num_objects_pages (THREAD_ENTRY * thread_p,
					      const HFID * hfid,
					      int approximation,
					      DB_BIGINT * nobjs, int *npages);

extern void heap_dump (THREAD_ENTRY * thread_p, FILE * fp, HFID * hfid,
		       bool dump_records);
extern void heap_dump_all (THREAD_ENTRY * thread_p, FILE * fp,
			   bool dump_records);
extern void heap_attrinfo_dump (THREAD_ENTRY * thread_p, FILE * fp,
				HEAP_CACHE_ATTRINFO * attr_info,
				bool dump_schema);
#if defined (RYE_DEBUG)
extern void heap_chnguess_dump (FILE * fp);
#endif /* RYE_DEBUG */
extern void heap_dump_all_capacities (THREAD_ENTRY * thread_p, FILE * fp);

extern REPR_ID heap_get_class_repr_id (THREAD_ENTRY * thread_p,
				       OID * class_oid);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int heap_attrinfo_set_uninitialized_global (THREAD_ENTRY * thread_p,
						   OID * inst_oid,
						   RECDES * recdes,
						   HEAP_CACHE_ATTRINFO *
						   attr_info);
#endif

/* Recovery functions */
extern int heap_rv_redo_newhdr (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_redo_newpage (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_undoredo_pagehdr (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern void heap_rv_dump_statistics (FILE * fp, int ignore_length,
				     void *data);
extern void heap_rv_dump_chain (FILE * fp, int ignore_length, void *data);
extern int heap_rv_undo_insert (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_redo_insert (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_undo_delete (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_redo_delete (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_undoredo_update (THREAD_ENTRY * thread_p, LOG_RCV * rcv);
extern int heap_rv_undo_create (THREAD_ENTRY * thread_p, LOG_RCV * rcv);

extern int heap_get_hfid_from_class_oid (THREAD_ENTRY * thread_p,
					 const OID * class_oid, HFID * hfid);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int heap_compact_pages (THREAD_ENTRY * thread_p, OID * class_oid);
#endif

extern int heap_get_btid_from_index_name (THREAD_ENTRY * thread_p,
					  const OID * p_class_oid,
					  const char *index_name,
					  BTID * p_found_btid);

extern int heap_bestspace_sync_all_heap_files_if_needed (THREAD_ENTRY *
							 thread_p);

extern int heap_classrepr_find_index_id (OR_CLASSREP * classrepr,
					 BTID * btid);
extern bool heap_classrepr_is_shard_table (THREAD_ENTRY * thread_p,
					   OID * class_oid);

extern VFID *heap_ovf_find_vfid (THREAD_ENTRY * thread_p, const HFID * hfid,
				 VFID * ovf_vfid, bool create);
extern OID *heap_ovf_insert (THREAD_ENTRY * thread_p, const HFID * hfid,
			     OID * ovf_oid, RECDES * recdes,
			     const OID * class_oid);
extern const OID *heap_ovf_update (THREAD_ENTRY * thread_p, const HFID * hfid,
				   const OID * ovf_oid, RECDES * recdes);
extern const OID *heap_ovf_delete (THREAD_ENTRY * thread_p, const HFID * hfid,
				   const OID * ovf_oid);
extern int heap_ovf_flush (THREAD_ENTRY * thread_p, const OID * ovf_oid);
extern int heap_ovf_get_length (THREAD_ENTRY * thread_p, const OID * ovf_oid);
extern SCAN_CODE heap_ovf_get (THREAD_ENTRY * thread_p, const OID * ovf_oid,
			       RECDES * recdes);
extern int heap_ovf_get_capacity (THREAD_ENTRY * thread_p,
				  const OID * ovf_oid, int *ovf_len,
				  int *ovf_num_pages, int *ovf_overhead,
				  int *ovf_free_space);
extern PAGE_PTR heap_pgbuf_fix (THREAD_ENTRY * thread_p, const HFID * hfid,
				const VPID * vpid,
				int requestmode,
				PGBUF_LATCH_CONDITION condition,
				const PAGE_TYPE ptype);
extern bool heap_is_big_length (int length);
#endif /* _HEAP_FILE_H_ */

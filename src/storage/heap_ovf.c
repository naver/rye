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
#include "overflow_file.h"
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

/*
 * heap_ovf_find_vfid () - Find overflow file identifier
 *   return: ovf_vfid or NULL
 *   hfid(in): Object heap file identifier
 *   ovf_vfid(in/out): Overflow file identifier.
 *   docreate(in): true/false. If true and the overflow file does not
 *                 exist, it is created.
 *
 * Note: Find overflow file identifier. If the overflow file does not
 * exist, it may be created depending of the value of argument create.
 */
VFID *
heap_ovf_find_vfid (THREAD_ENTRY * thread_p, const HFID * hfid,
		    VFID * ovf_vfid, bool docreate)
{
  HEAP_HDR_STATS *heap_hdr;	/* Header of heap structure    */
  LOG_DATA_ADDR addr_hdr = LOG_ADDR_INITIALIZER;	/* Address of logging data     */
  VPID vpid;			/* Page-volume identifier      */
  RECDES hdr_recdes;		/* Header record descriptor    */
  int mode;

  assert (file_is_new_file (thread_p, &(hfid->vfid)) == FILE_OLD_FILE);

  addr_hdr.vfid = &hfid->vfid;
  addr_hdr.offset = HEAP_HEADER_AND_CHAIN_SLOTID;

  /* Read the header page */
  vpid.volid = hfid->vfid.volid;
  vpid.pageid = hfid->hpgid;

  mode = (docreate == true ? PGBUF_LATCH_WRITE : PGBUF_LATCH_READ);
  addr_hdr.pgptr = heap_pgbuf_fix (thread_p, hfid, &vpid, mode,
				   PGBUF_UNCONDITIONAL_LATCH,
				   PAGE_HEAP_HEADER,
				   MNT_STATS_DATA_PAGE_FETCHES_HEAP);
  if (addr_hdr.pgptr == NULL)
    {
      /* something went wrong, return */
      return NULL;
    }

  /* Peek the header record */

  if (spage_get_record (addr_hdr.pgptr, HEAP_HEADER_AND_CHAIN_SLOTID,
			&hdr_recdes, PEEK) != S_SUCCESS)
    {
      pgbuf_unfix_and_init (thread_p, addr_hdr.pgptr);
      return NULL;
    }

  heap_hdr = (HEAP_HDR_STATS *) hdr_recdes.data;
  if (VFID_ISNULL (&heap_hdr->ovf_vfid))
    {
      if (docreate == true)
	{
	  FILE_OVF_HEAP_DES hfdes_ovf;
	  /*
	   * Create the overflow file. Try to create the overflow file in the
	   * same volume where the heap was defined
	   */

	  /*
	   * START A TOP SYSTEM OPERATION
	   */

	  if (log_start_system_op (thread_p) == NULL)
	    {
	      pgbuf_unfix_and_init (thread_p, addr_hdr.pgptr);
	      return NULL;
	    }


	  ovf_vfid->volid = hfid->vfid.volid;
	  /*
	   * At least three pages since a multipage object will take at least
	   * two pages
	   */


	  /* Initialize description of overflow heap file */
	  HFID_COPY (&hfdes_ovf.hfid, hfid);

	  if (file_create (thread_p, ovf_vfid, 3, FILE_MULTIPAGE_OBJECT_HEAP,
			   &hfdes_ovf, NULL, 0) != NULL)
	    {
	      /* Log undo, then redo */
	      log_append_undo_data (thread_p, RVHF_STATS, &addr_hdr,
				    sizeof (*heap_hdr), heap_hdr);
	      VFID_COPY (&heap_hdr->ovf_vfid, ovf_vfid);
	      log_append_redo_data (thread_p, RVHF_STATS, &addr_hdr,
				    sizeof (*heap_hdr), heap_hdr);
	      pgbuf_set_dirty (thread_p, addr_hdr.pgptr, DONT_FREE);

	      log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);
	      (void) file_new_declare_as_old (thread_p, ovf_vfid);
	    }
	  else
	    {
	      log_end_system_op (thread_p, LOG_RESULT_TOPOP_ABORT);
	      ovf_vfid = NULL;
	    }
	}
      else
	{
	  ovf_vfid = NULL;
	}
    }
  else
    {
      VFID_COPY (ovf_vfid, &heap_hdr->ovf_vfid);
    }

  pgbuf_unfix_and_init (thread_p, addr_hdr.pgptr);

  return ovf_vfid;
}

/*
 * heap_ovf_insert () - Insert the content of a multipage object in overflow
 *   return: OID *(ovf_oid on success or NULL on failure)
 *   hfid(in): Object heap file identifier
 *   ovf_oid(in/out): Overflow address
 *   recdes(in): Record descriptor
 *   class_oid(in): class oid for migrator
 *
 * Note: Insert the content of a multipage object in overflow.
 */
OID *
heap_ovf_insert (THREAD_ENTRY * thread_p, const HFID * hfid, OID * ovf_oid,
		 RECDES * recdes, const OID * class_oid)
{
  VFID ovf_vfid;
  VPID ovf_vpid;		/* Address of overflow insertion */

  if (heap_ovf_find_vfid (thread_p, hfid, &ovf_vfid, true) == NULL
      || overflow_insert (thread_p, &ovf_vfid, &ovf_vpid, recdes,
			  NULL, class_oid) == NULL)
    {
      return NULL;
    }

  ovf_oid->pageid = ovf_vpid.pageid;
  ovf_oid->volid = ovf_vpid.volid;
  ovf_oid->slotid = NULL_SLOTID;	/* Irrelevant */

  return ovf_oid;
}

/*
 * heap_ovf_update () - Update the content of a multipage object
 *   return: OID *(ovf_oid on success or NULL on failure)
 *   hfid(in): Object heap file identifier
 *   ovf_oid(in): Overflow address
 *   recdes(in): Record descriptor
 *
 * Note: Update the content of a multipage object.
 */
const OID *
heap_ovf_update (THREAD_ENTRY * thread_p, const HFID * hfid,
		 const OID * ovf_oid, RECDES * recdes)
{
  VFID ovf_vfid;
  VPID ovf_vpid;

  if (heap_ovf_find_vfid (thread_p, hfid, &ovf_vfid, false) == NULL)
    {
      return NULL;
    }

  ovf_vpid.pageid = ovf_oid->pageid;
  ovf_vpid.volid = ovf_oid->volid;

  if (overflow_update (thread_p, &ovf_vfid, &ovf_vpid, recdes) == NULL)
    {
      return NULL;
    }
  else
    {
      return ovf_oid;
    }
}

/*
 * heap_ovf_delete () - Delete the content of a multipage object
 *   return: OID *(ovf_oid on success or NULL on failure)
 *   hfid(in): Object heap file identifier
 *   ovf_oid(in): Overflow address
 *
 * Note: Delete the content of a multipage object.
 */
const OID *
heap_ovf_delete (THREAD_ENTRY * thread_p, const HFID * hfid,
		 const OID * ovf_oid)
{
  VFID ovf_vfid;
  VPID ovf_vpid;

  if (heap_ovf_find_vfid (thread_p, hfid, &ovf_vfid, false) == NULL)
    {
      return NULL;
    }

  ovf_vpid.pageid = ovf_oid->pageid;
  ovf_vpid.volid = ovf_oid->volid;

  if (overflow_delete (thread_p, &ovf_vfid, &ovf_vpid) == NULL)
    {
      return NULL;
    }
  else
    {
      return ovf_oid;
    }

}

/*
 * heap_ovf_flush () - Flush all overflow dirty pages where the object resides
 *   return: NO_ERROR
 *   ovf_oid(in): Overflow address
 *
 * Note: Flush all overflow dirty pages where the object resides.
 */
int
heap_ovf_flush (THREAD_ENTRY * thread_p, const OID * ovf_oid)
{
  VPID ovf_vpid;

  ovf_vpid.pageid = ovf_oid->pageid;
  ovf_vpid.volid = ovf_oid->volid;
  overflow_flush (thread_p, &ovf_vpid);

  return NO_ERROR;
}

/*
 * heap_ovf_get_length () - Find length of overflow object
 *   return: length
 *   ovf_oid(in): Overflow address
 *
 * Note: The length of the content of a multipage object associated
 * with the given overflow address is returned. In the case of
 * any error, -1 is returned.
 */
int
heap_ovf_get_length (THREAD_ENTRY * thread_p, const OID * ovf_oid)
{
  VPID ovf_vpid;

  ovf_vpid.pageid = ovf_oid->pageid;
  ovf_vpid.volid = ovf_oid->volid;

  return overflow_get_length (thread_p, &ovf_vpid);
}

/*
 * heap_ovf_get () - get/retrieve the content of a multipage object from overflow
 *   return: SCAN_CODE
 *           (Either of S_SUCCESS, S_DOESNT_FIT, S_END)
 *   ovf_oid(in): Overflow address
 *   recdes(in): Record descriptor
 *
 * Note: The content of a multipage object associated with the given
 * overflow address(oid) is placed into the area pointed to by
 * the record descriptor. If the content of the object does not
 * fit in such an area (i.e., recdes->area_size), an error is
 * returned and a hint of its length is returned as a negative
 * value in recdes->length. The length of the retrieved object is
 * set in the the record descriptor (i.e., recdes->length).
 */
SCAN_CODE
heap_ovf_get (THREAD_ENTRY * thread_p, const OID * ovf_oid, RECDES * recdes)
{
  VPID ovf_vpid;
  SCAN_CODE scan;

  ovf_vpid.pageid = ovf_oid->pageid;
  ovf_vpid.volid = ovf_oid->volid;

  scan = overflow_get (thread_p, &ovf_vpid, recdes);

  return scan;
}

/*
 * heap_ovf_get_capacity () - Find space consumed oveflow object
 *   return: NO_ERROR
 *   ovf_oid(in): Overflow address
 *   ovf_len(out): Length of overflow object
 *   ovf_num_pages(out): Total number of overflow pages
 *   ovf_overhead(out): System overhead for overflow record
 *   ovf_free_space(out): Free space for exapnsion of the overflow rec
 *
 * Note: Find the current storage facts/capacity of given overflow rec
 */
int
heap_ovf_get_capacity (THREAD_ENTRY * thread_p, const OID * ovf_oid,
		       int *ovf_len, int *ovf_num_pages, int *ovf_overhead,
		       int *ovf_free_space)
{
  VPID ovf_vpid;

  ovf_vpid.pageid = ovf_oid->pageid;
  ovf_vpid.volid = ovf_oid->volid;

  return overflow_get_capacity (thread_p, &ovf_vpid, ovf_len, ovf_num_pages,
				ovf_overhead, ovf_free_space);
}

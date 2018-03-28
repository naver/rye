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
 * locator_sr.h - Server transaction locator (interface)
 */

#ifndef _LOCATOR_SR_H_
#define _LOCATOR_SR_H_

#ident "$Id$"

#include "config.h"

#include "error_manager.h"
#include "oid.h"
#include "storage_common.h"
#include "locator.h"
#include "heap_file.h"
#include "lock_manager.h"
#include "extendible_hash.h"

#include "disk_manager.h"
#include "repl_log.h"
#include "thread.h"

extern EHID *locator_initialize (THREAD_ENTRY * thread_p, EHID * classname_table);
extern void locator_finalize (THREAD_ENTRY * thread_p);
extern int locator_drop_transient_class_name_entries (THREAD_ENTRY * thread_p, int tran_index, LOG_LSA * savep_lsa);
#if defined (ENABLE_UNUSED_FUNCTION)
extern DISK_ISVALID locator_check_class_names (THREAD_ENTRY * thread_p);
#endif
extern void locator_dump_class_names (THREAD_ENTRY * thread_p, FILE * out_fp);

extern int
locator_start_force_scan_cache (THREAD_ENTRY * thread_p,
                                HEAP_SCANCACHE * scan_cache,
                                const HFID * hfid, const int force_page_allocation, const OID * class_oid);
extern void locator_end_force_scan_cache (THREAD_ENTRY * thread_p, HEAP_SCANCACHE * scan_cache);
extern int locator_attribute_info_force (THREAD_ENTRY * thread_p,
                                         const HFID * hfid, OID * oid,
                                         HEAP_CACHE_ATTRINFO * attr_info,
                                         ATTR_ID * att_id, int n_att_id,
                                         LC_COPYAREA_OPERATION operation,
                                         HEAP_SCANCACHE * scan_cache, int *force_count);
extern LC_COPYAREA *locator_allocate_copy_area_by_attr_info (THREAD_ENTRY *
                                                             thread_p,
                                                             HEAP_CACHE_ATTRINFO
                                                             * attr_info,
                                                             RECDES *
                                                             old_recdes,
                                                             RECDES *
                                                             new_recdes,
                                                             int shard_groupid, const int copyarea_length_hint);
#if defined (ENABLE_UNUSED_FUNCTION)
extern DISK_ISVALID locator_check_by_class_oid (THREAD_ENTRY * thread_p, OID * cls_oid, HFID * hfid, bool repair);
#endif
extern int locator_add_or_remove_index (THREAD_ENTRY * thread_p,
                                        RECDES * recdes, OID * inst_oid,
                                        OID * class_oid, int is_insert, bool datayn, bool replyn, HFID * hfid);
extern int locator_update_index (THREAD_ENTRY * thread_p, RECDES * new_recdes,
                                 RECDES * old_recdes, ATTR_ID * att_id,
                                 int n_att_id, OID * inst_oid, OID * class_oid, bool data_update, bool replyn);
#endif /* _LOCATOR_SR_H_ */

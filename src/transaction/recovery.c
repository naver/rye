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
 * recovery.c -
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>

#include "recovery.h"
#include "log_manager.h"
#include "repl_log.h"
#include "btree.h"
#include "btree_load.h"
#include "system_catalog.h"
#include "disk_manager.h"
#include "extendible_hash.h"
#include "file_manager.h"
#include "overflow_file.h"

/*
 *
 *    		 THE ARRAY OF SERVER RECOVERY FUNCTIONS
 *
 * Note: When adding new entries, be sure to add the an entry to print it as
 * a string in rv_rcvindex_string().
 */
struct rvfun RV_fun[RCV_INDEX_END];

int
rv_init_rvfuns (void)
{
  struct rvfun *rv_fun;

  rv_fun = &RV_fun[RVDK_NEWVOL];
  rv_fun->recv_index = RVDK_NEWVOL;
  rv_fun->recv_string = "RVDK_NEWVOL";
  rv_fun->undofun = NULL;
  rv_fun->redofun = disk_rv_redo_dboutside_newvol;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = disk_rv_dump_hdr;

  rv_fun = &RV_fun[RVDK_FORMAT];
  rv_fun->recv_index = RVDK_FORMAT;
  rv_fun->recv_string = "RVDK_FORMAT";
  rv_fun->undofun = disk_rv_undo_format;
  rv_fun->redofun = disk_rv_redo_format;
  rv_fun->dump_undofun = log_rv_dump_char;
  rv_fun->dump_redofun = disk_rv_dump_hdr;

  rv_fun = &RV_fun[RVDK_INITMAP];
  rv_fun->recv_index = RVDK_INITMAP;
  rv_fun->recv_string = "RVDK_INITMAP";
  rv_fun->undofun = NULL;
  rv_fun->redofun = disk_rv_redo_init_map;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = disk_rv_dump_init_map;

  rv_fun = &RV_fun[RVDK_VHDR_SCALLOC];
  rv_fun->recv_index = RVDK_VHDR_SCALLOC;
  rv_fun->recv_string = "RVDK_VHDR_SCALLOC";
  rv_fun->undofun = disk_vhdr_rv_undoredo_free_sectors;
  rv_fun->redofun = disk_vhdr_rv_undoredo_free_sectors;
  rv_fun->dump_undofun = disk_vhdr_rv_dump_free_sectors;
  rv_fun->dump_redofun = disk_vhdr_rv_dump_free_sectors;

  rv_fun = &RV_fun[RVDK_VHDR_PGALLOC];
  rv_fun->recv_index = RVDK_VHDR_PGALLOC;
  rv_fun->recv_string = "RVDK_VHDR_PGALLOC";
  rv_fun->undofun = disk_vhdr_rv_undoredo_free_pages;
  rv_fun->redofun = disk_vhdr_rv_undoredo_free_pages;
  rv_fun->dump_undofun = disk_vhdr_rv_dump_free_pages;
  rv_fun->dump_redofun = disk_vhdr_rv_dump_free_pages;

  rv_fun = &RV_fun[RVDK_IDALLOC];
  rv_fun->recv_index = RVDK_IDALLOC;
  rv_fun->recv_string = "RVDK_IDALLOC";
  rv_fun->undofun = disk_rv_clear_alloctable;
  rv_fun->redofun = disk_rv_set_alloctable;
  rv_fun->dump_undofun = disk_rv_dump_alloctable;
  rv_fun->dump_redofun = disk_rv_dump_alloctable;

  rv_fun = &RV_fun[RVDK_IDDEALLOC_WITH_VOLHEADER];
  rv_fun->recv_index = RVDK_IDDEALLOC_WITH_VOLHEADER;
  rv_fun->recv_string = "RVDK_IDDEALLOC_WITH_VOLHEADER";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = disk_rv_dump_alloctable_with_vhdr;
  rv_fun->dump_redofun = disk_rv_dump_alloctable_with_vhdr;

  /* Never use this recovery index anymore. Only for backward compatibility */
  rv_fun = &RV_fun[RVDK_MAGIC];
  rv_fun->recv_index = RVDK_MAGIC;
  rv_fun->recv_string = "RVDK_MAGIC";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  /* Never use this recovery index anymore. Only for backward compatibility */
  rv_fun = &RV_fun[RVDK_CHANGE_CREATION];
  rv_fun->recv_index = RVDK_CHANGE_CREATION;
  rv_fun->recv_string = "RVDK_CHANGE_CREATION";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVDK_RESET_BOOT_HFID];
  rv_fun->recv_index = RVDK_RESET_BOOT_HFID;
  rv_fun->recv_string = "RVDK_RESET_BOOT_HFID";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVDK_LINK_PERM_VOLEXT];
  rv_fun->recv_index = RVDK_LINK_PERM_VOLEXT;
  rv_fun->recv_string = "RVDK_LINK_PERM_VOLEXT";
  rv_fun->undofun = disk_rv_undoredo_link;
  rv_fun->redofun = disk_rv_undoredo_link;
  rv_fun->dump_undofun = disk_rv_dump_link;
  rv_fun->dump_redofun = disk_rv_dump_link;

  rv_fun = &RV_fun[RVFL_CREATE_TMPFILE];
  rv_fun->recv_index = RVFL_CREATE_TMPFILE;
  rv_fun->recv_string = "RVFL_CREATE_TMPFILE";
  rv_fun->undofun = file_rv_undo_create_tmp;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = file_rv_dump_create_tmp;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVFL_FTAB_CHAIN];
  rv_fun->recv_index = RVFL_FTAB_CHAIN;
  rv_fun->recv_string = "RVFL_FTAB_CHAIN";
  rv_fun->undofun = log_rv_copy_char;
  rv_fun->redofun = file_rv_redo_ftab_chain;
  rv_fun->dump_undofun = file_rv_dump_ftab_chain;
  rv_fun->dump_redofun = file_rv_dump_ftab_chain;

  rv_fun = &RV_fun[RVFL_IDSTABLE];
  rv_fun->recv_index = RVFL_IDSTABLE;
  rv_fun->recv_string = "RVFL_IDSTABLE";
  rv_fun->undofun = log_rv_copy_char;
  rv_fun->redofun = log_rv_copy_char;
  rv_fun->dump_undofun = file_rv_dump_idtab;
  rv_fun->dump_redofun = file_rv_dump_idtab;

  /* Never use this recovery index anymore. Only for backward compatibility */
  rv_fun = &RV_fun[RVFL_MARKED_DELETED];
  rv_fun->recv_index = RVFL_MARKED_DELETED;
  rv_fun->recv_string = "RVFL_MARKED_DELETED";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVFL_ALLOCSET_SECT];
  rv_fun->recv_index = RVFL_ALLOCSET_SECT;
  rv_fun->recv_string = "RVFL_ALLOCSET_SECT";
  rv_fun->undofun = file_rv_allocset_undoredo_sector;
  rv_fun->redofun = file_rv_allocset_undoredo_sector;
  rv_fun->dump_undofun = file_rv_allocset_dump_sector;
  rv_fun->dump_redofun = file_rv_allocset_dump_sector;

  rv_fun = &RV_fun[RVFL_ALLOCSET_PAGETB_ADDRESS];
  rv_fun->recv_index = RVFL_ALLOCSET_PAGETB_ADDRESS;
  rv_fun->recv_string = "RVFL_ALLOCSET_PAGETB_ADDRESS";
  rv_fun->undofun = file_rv_allocset_undoredo_page;
  rv_fun->redofun = file_rv_allocset_undoredo_page;
  rv_fun->dump_undofun = file_rv_allocset_dump_page;
  rv_fun->dump_redofun = file_rv_allocset_dump_page;

  rv_fun = &RV_fun[RVFL_ALLOCSET_NEW];
  rv_fun->recv_index = RVFL_ALLOCSET_NEW;
  rv_fun->recv_string = "RVFL_ALLOCSET_NEW";
  rv_fun->undofun = log_rv_copy_char;
  rv_fun->redofun = log_rv_copy_char;
  rv_fun->dump_undofun = file_rv_dump_allocset;
  rv_fun->dump_redofun = file_rv_dump_allocset;

  rv_fun = &RV_fun[RVFL_ALLOCSET_LINK];
  rv_fun->recv_index = RVFL_ALLOCSET_LINK;
  rv_fun->recv_string = "RVFL_ALLOCSET_LINK";
  rv_fun->undofun = file_rv_allocset_undoredo_link;
  rv_fun->redofun = file_rv_allocset_undoredo_link;
  rv_fun->dump_undofun = file_rv_allocset_dump_link;
  rv_fun->dump_redofun = file_rv_allocset_dump_link;

  rv_fun = &RV_fun[RVFL_ALLOCSET_ADD_PAGES];
  rv_fun->recv_index = RVFL_ALLOCSET_ADD_PAGES;
  rv_fun->recv_string = "RVFL_ALLOCSET_ADD_PAGES";
  rv_fun->undofun = file_rv_allocset_undoredo_add_pages;
  rv_fun->redofun = file_rv_allocset_undoredo_add_pages;
  rv_fun->dump_undofun = file_rv_allocset_dump_add_pages;
  rv_fun->dump_redofun = file_rv_allocset_dump_add_pages;

  rv_fun = &RV_fun[RVFL_ALLOCSET_DELETE_PAGES];
  rv_fun->recv_index = RVFL_ALLOCSET_DELETE_PAGES;
  rv_fun->recv_string = "RVFL_ALLOCSET_DELETE_PAGES";
  rv_fun->undofun = file_rv_allocset_undoredo_delete_pages;
  rv_fun->redofun = file_rv_allocset_undoredo_delete_pages;
  rv_fun->dump_undofun = file_rv_allocset_dump_delete_pages;
  rv_fun->dump_redofun = file_rv_allocset_dump_delete_pages;

  rv_fun = &RV_fun[RVFL_ALLOCSET_SECT_SHIFT];
  rv_fun->recv_index = RVFL_ALLOCSET_SECT_SHIFT;
  rv_fun->recv_string = "RVFL_ALLOCSET_SECT_SHIFT";
  rv_fun->undofun = file_rv_allocset_undoredo_sectortab;
  rv_fun->redofun = file_rv_allocset_undoredo_sectortab;
  rv_fun->dump_undofun = file_rv_allocset_dump_sectortab;
  rv_fun->dump_redofun = file_rv_allocset_dump_sectortab;

  rv_fun = &RV_fun[RVFL_ALLOCSET_COPY];
  rv_fun->recv_index = RVFL_ALLOCSET_COPY;
  rv_fun->recv_string = "RVFL_ALLOCSET_COPY";
  rv_fun->undofun = log_rv_copy_char;
  rv_fun->redofun = log_rv_copy_char;
  rv_fun->dump_undofun = file_rv_dump_allocset;
  rv_fun->dump_redofun = file_rv_dump_allocset;

  rv_fun = &RV_fun[RVFL_FHDR];
  rv_fun->recv_index = RVFL_FHDR;
  rv_fun->recv_string = "RVFL_FHDR";
  rv_fun->undofun = log_rv_copy_char;
  rv_fun->redofun = file_rv_redo_fhdr;
  rv_fun->dump_undofun = file_rv_dump_fhdr;
  rv_fun->dump_redofun = file_rv_dump_fhdr;

  rv_fun = &RV_fun[RVFL_FHDR_ADD_LAST_ALLOCSET];
  rv_fun->recv_index = RVFL_FHDR_ADD_LAST_ALLOCSET;
  rv_fun->recv_string = "RVFL_FHDR_ADD_LAST_ALLOCSET";
  rv_fun->undofun = file_rv_fhdr_remove_last_allocset;
  rv_fun->redofun = file_rv_fhdr_add_last_allocset;
  rv_fun->dump_undofun = file_rv_fhdr_dump_last_allocset;
  rv_fun->dump_redofun = file_rv_fhdr_dump_last_allocset;

  rv_fun = &RV_fun[RVFL_FHDR_REMOVE_LAST_ALLOCSET];
  rv_fun->recv_index = RVFL_FHDR_REMOVE_LAST_ALLOCSET;
  rv_fun->recv_string = "RVFL_FHDR_REMOVE_LAST_ALLOCSET";
  rv_fun->undofun = file_rv_fhdr_add_last_allocset;
  rv_fun->redofun = file_rv_fhdr_remove_last_allocset;
  rv_fun->dump_undofun = file_rv_fhdr_dump_last_allocset;
  rv_fun->dump_redofun = file_rv_fhdr_dump_last_allocset;

  rv_fun = &RV_fun[RVFL_FHDR_CHANGE_LAST_ALLOCSET];
  rv_fun->recv_index = RVFL_FHDR_CHANGE_LAST_ALLOCSET;
  rv_fun->recv_string = "RVFL_FHDR_CHANGE_LAST_ALLOCSET";
  rv_fun->undofun = file_rv_fhdr_change_last_allocset;
  rv_fun->redofun = file_rv_fhdr_change_last_allocset;
  rv_fun->dump_undofun = file_rv_fhdr_dump_last_allocset;
  rv_fun->dump_redofun = file_rv_fhdr_dump_last_allocset;

  rv_fun = &RV_fun[RVFL_FHDR_ADD_PAGES];
  rv_fun->recv_index = RVFL_FHDR_ADD_PAGES;
  rv_fun->recv_string = "RVFL_FHDR_ADD_PAGES";
  rv_fun->undofun = file_rv_fhdr_undoredo_add_pages;
  rv_fun->redofun = file_rv_fhdr_undoredo_add_pages;
  rv_fun->dump_undofun = file_rv_fhdr_dump_add_pages;
  rv_fun->dump_redofun = file_rv_fhdr_dump_add_pages;

  rv_fun = &RV_fun[RVFL_FHDR_MARK_DELETED_PAGES];
  rv_fun->recv_index = RVFL_FHDR_MARK_DELETED_PAGES;
  rv_fun->recv_string = "RVFL_FHDR_MARK_DELETED_PAGES";
  rv_fun->undofun = file_rv_fhdr_undoredo_mark_deleted_pages;
  rv_fun->redofun = file_rv_fhdr_undoredo_mark_deleted_pages;
  rv_fun->dump_undofun = file_rv_fhdr_dump_mark_deleted_pages;
  rv_fun->dump_redofun = file_rv_fhdr_dump_mark_deleted_pages;

  rv_fun = &RV_fun[RVFL_FHDR_DELETE_PAGES];
  rv_fun->recv_index = RVFL_FHDR_DELETE_PAGES;
  rv_fun->recv_string = "RVFL_FHDR_DELETE_PAGES";
  rv_fun->undofun = NULL;
  rv_fun->redofun = file_rv_fhdr_delete_pages;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = file_rv_fhdr_delete_pages_dump;

  rv_fun = &RV_fun[RVFL_FHDR_FTB_EXPANSION];
  rv_fun->recv_index = RVFL_FHDR_FTB_EXPANSION;
  rv_fun->recv_string = "RVFL_FHDR_FTB_EXPANSION";
  rv_fun->undofun = file_rv_fhdr_undoredo_expansion;
  rv_fun->redofun = file_rv_fhdr_undoredo_expansion;
  rv_fun->dump_undofun = file_rv_fhdr_dump_expansion;
  rv_fun->dump_redofun = file_rv_fhdr_dump_expansion;

  rv_fun = &RV_fun[RVFL_FILEDESC];
  rv_fun->recv_index = RVFL_FILEDESC;
  rv_fun->recv_string = "RVFL_FILEDESC";
  rv_fun->undofun = log_rv_copy_char;
  rv_fun->redofun = log_rv_copy_char;
  rv_fun->dump_undofun = log_rv_dump_char;
  rv_fun->dump_redofun = log_rv_dump_char;

  rv_fun = &RV_fun[RVFL_DES_FIRSTREST_NEXTVPID];
  rv_fun->recv_index = RVFL_DES_FIRSTREST_NEXTVPID;
  rv_fun->recv_string = "RVFL_DES_FIRSTREST_NEXTVPID";
  rv_fun->undofun = file_rv_descriptor_undoredo_firstrest_nextvpid;
  rv_fun->redofun = file_rv_descriptor_undoredo_firstrest_nextvpid;
  rv_fun->dump_undofun = file_rv_descriptor_dump_firstrest_nextvpid;
  rv_fun->dump_redofun = file_rv_descriptor_dump_firstrest_nextvpid;

  rv_fun = &RV_fun[RVFL_DES_NREST_NEXTVPID];
  rv_fun->recv_index = RVFL_DES_NREST_NEXTVPID;
  rv_fun->recv_string = "RVFL_DES_NREST_NEXTVPID";
  rv_fun->undofun = file_rv_descriptor_undoredo_nrest_nextvpid;
  rv_fun->redofun = file_rv_descriptor_undoredo_nrest_nextvpid;
  rv_fun->dump_undofun = file_rv_descriptor_dump_nrest_nextvpid;
  rv_fun->dump_redofun = file_rv_descriptor_dump_nrest_nextvpid;

  rv_fun = &RV_fun[RVFL_TRACKER_REGISTER];
  rv_fun->recv_index = RVFL_TRACKER_REGISTER;
  rv_fun->recv_string = "RVFL_TRACKER_REGISTER";
  rv_fun->undofun = file_rv_tracker_undo_register;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = file_rv_tracker_dump_undo_register;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVFL_LOGICAL_NOOP];
  rv_fun->recv_index = RVFL_LOGICAL_NOOP;
  rv_fun->recv_string = "RVFL_LOGICAL_NOOP";
  rv_fun->undofun = NULL;
  rv_fun->redofun = file_rv_logical_redo_nop;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVHF_CREATE];
  rv_fun->recv_index = RVHF_CREATE;
  rv_fun->recv_string = "RVHF_CREATE";
  rv_fun->undofun = heap_rv_undo_create;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = heap_rv_dump_statistics;

  rv_fun = &RV_fun[RVHF_NEWHDR];
  rv_fun->recv_index = RVHF_NEWHDR;
  rv_fun->recv_string = "RVHF_NEWHDR";
  rv_fun->undofun = NULL;
  rv_fun->redofun = heap_rv_redo_newhdr;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = heap_rv_dump_chain;

  rv_fun = &RV_fun[RVHF_NEWPAGE];
  rv_fun->recv_index = RVHF_NEWPAGE;
  rv_fun->recv_string = "RVHF_NEWPAGE";
  rv_fun->undofun = NULL;
  rv_fun->redofun = heap_rv_redo_newpage;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = heap_rv_dump_chain;

  rv_fun = &RV_fun[RVHF_STATS];
  rv_fun->recv_index = RVHF_STATS;
  rv_fun->recv_string = "RVHF_STATS";
  rv_fun->undofun = heap_rv_undoredo_pagehdr;
  rv_fun->redofun = heap_rv_undoredo_pagehdr;
  rv_fun->dump_undofun = heap_rv_dump_statistics;
  rv_fun->dump_redofun = heap_rv_dump_statistics;

  rv_fun = &RV_fun[RVHF_CHAIN];
  rv_fun->recv_index = RVHF_CHAIN;
  rv_fun->recv_string = "RVHF_CHAIN";
  rv_fun->undofun = heap_rv_undoredo_pagehdr;
  rv_fun->redofun = heap_rv_undoredo_pagehdr;
  rv_fun->dump_undofun = heap_rv_dump_chain;
  rv_fun->dump_redofun = heap_rv_dump_chain;

  rv_fun = &RV_fun[RVHF_INSERT];
  rv_fun->recv_index = RVHF_INSERT;
  rv_fun->recv_string = "RVHF_INSERT";
  rv_fun->undofun = heap_rv_undo_insert;
  rv_fun->redofun = heap_rv_redo_insert;
  rv_fun->dump_undofun = log_rv_dump_char;
  rv_fun->dump_redofun = log_rv_dump_char;

  rv_fun = &RV_fun[RVHF_DELETE];
  rv_fun->recv_index = RVHF_DELETE;
  rv_fun->recv_string = "RVHF_DELETE";
  rv_fun->undofun = heap_rv_undo_delete;
  rv_fun->redofun = heap_rv_redo_delete;
  rv_fun->dump_undofun = log_rv_dump_char;
  rv_fun->dump_redofun = log_rv_dump_char;

  /* Never use this recovery index anymore. Only for backward compatibility */
  rv_fun = &RV_fun[RVHF_DELETE_NEWHOME];
  rv_fun->recv_index = RVHF_DELETE_NEWHOME;
  rv_fun->recv_string = "RVHF_DELETE_NEWHOME";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVHF_UPDATE];
  rv_fun->recv_index = RVHF_UPDATE;
  rv_fun->recv_string = "RVHF_UPDATE";
  rv_fun->undofun = heap_rv_undoredo_update;
  rv_fun->redofun = heap_rv_undoredo_update;
  rv_fun->dump_undofun = log_rv_dump_char;
  rv_fun->dump_redofun = log_rv_dump_char;

  /* Never use this recovery index anymore. Only for backward compatibility */
  rv_fun = &RV_fun[RVHF_UPDATE_TYPE];
  rv_fun->recv_index = RVHF_UPDATE_TYPE;
  rv_fun->recv_string = "RVHF_UPDATE_TYPE";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = log_rv_dump_char;
  rv_fun->dump_redofun = log_rv_dump_char;

  rv_fun = &RV_fun[RVOVF_NEWPAGE_LOGICAL_UNDO];
  rv_fun->recv_index = RVOVF_NEWPAGE_LOGICAL_UNDO;
  rv_fun->recv_string = "RVOVF_NEWPAGE_LOGICAL_UNDO";
  rv_fun->undofun = overflow_rv_newpage_logical_undo;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = overflow_rv_newpage_logical_dump_undo;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVOVF_NEWPAGE_INSERT];
  rv_fun->recv_index = RVOVF_NEWPAGE_INSERT;
  rv_fun->recv_string = "RVOVF_NEWPAGE_INSERT";
  rv_fun->undofun = NULL;
  rv_fun->redofun = overflow_rv_newpage_insert_redo;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = log_rv_dump_char;

  rv_fun = &RV_fun[RVOVF_NEWPAGE_LINK];
  rv_fun->recv_index = RVOVF_NEWPAGE_LINK;
  rv_fun->recv_string = "RVOVF_NEWPAGE_LINK";
  rv_fun->undofun = overflow_rv_newpage_link_undo;
  rv_fun->redofun = overflow_rv_link;
  rv_fun->dump_undofun = overflow_rv_link_dump;
  rv_fun->dump_redofun = overflow_rv_link_dump;

  rv_fun = &RV_fun[RVOVF_PAGE_UPDATE];
  rv_fun->recv_index = RVOVF_PAGE_UPDATE;
  rv_fun->recv_string = "RVOVF_PAGE_UPDATE";
  rv_fun->undofun = log_rv_copy_char;
  rv_fun->redofun = log_rv_copy_char;
  rv_fun->dump_undofun = overflow_rv_page_dump;
  rv_fun->dump_redofun = overflow_rv_page_dump;

  rv_fun = &RV_fun[RVOVF_CHANGE_LINK];
  rv_fun->recv_index = RVOVF_CHANGE_LINK;
  rv_fun->recv_string = "RVOVF_CHANGE_LINK";
  rv_fun->undofun = overflow_rv_link;
  rv_fun->redofun = overflow_rv_link;
  rv_fun->dump_undofun = overflow_rv_link_dump;
  rv_fun->dump_redofun = overflow_rv_link_dump;

  rv_fun = &RV_fun[RVEH_REPLACE];
  rv_fun->recv_index = RVEH_REPLACE;
  rv_fun->recv_string = "RVEH_REPLACE";
  rv_fun->undofun = log_rv_copy_char;
  rv_fun->redofun = log_rv_copy_char;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVEH_INSERT];
  rv_fun->recv_index = RVEH_INSERT;
  rv_fun->recv_string = "RVEH_INSERT";
  rv_fun->undofun = ehash_rv_insert_undo;
  rv_fun->redofun = ehash_rv_insert_redo;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVEH_DELETE];
  rv_fun->recv_index = RVEH_DELETE;
  rv_fun->recv_string = "RVEH_DELETE";
  rv_fun->undofun = ehash_rv_delete_undo;
  rv_fun->redofun = ehash_rv_delete_redo;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVEH_INIT_DIR];
  rv_fun->recv_index = RVEH_INIT_DIR;
  rv_fun->recv_string = "RVEH_INIT_DIR";
  rv_fun->undofun = log_rv_copy_char;
  rv_fun->redofun = ehash_rv_init_dir_redo;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVEH_INIT_NEW_DIR_PAGE];
  rv_fun->recv_index = RVEH_INIT_NEW_DIR_PAGE;
  rv_fun->recv_string = "RVEH_INIT_NEW_DIR_PAGE";
  rv_fun->undofun = NULL;
  rv_fun->redofun = ehash_rv_init_dir_new_page_redo;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVEH_INIT_BUCKET];
  rv_fun->recv_index = RVEH_INIT_BUCKET;
  rv_fun->recv_string = "RVEH_INIT_BUCKET";
  rv_fun->undofun = NULL;
  rv_fun->redofun = ehash_rv_init_bucket_redo;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVEH_CONNECT_BUCKET];
  rv_fun->recv_index = RVEH_CONNECT_BUCKET;
  rv_fun->recv_string = "RVEH_CONNECT_BUCKET";
  rv_fun->undofun = log_rv_copy_char;
  rv_fun->redofun = ehash_rv_connect_bucket_redo;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVEH_INC_COUNTER];
  rv_fun->recv_index = RVEH_INC_COUNTER;
  rv_fun->recv_string = "RVEH_INC_COUNTER";
  rv_fun->undofun = ehash_rv_increment;
  rv_fun->redofun = ehash_rv_increment;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVBT_NDHEADER_INS];
  rv_fun->recv_index = RVBT_NDHEADER_INS;
  rv_fun->recv_string = "RVBT_NDHEADER_INS";
  rv_fun->undofun = btree_rv_nodehdr_undo_insert;
  rv_fun->redofun = btree_rv_nodehdr_redo_insert;
  rv_fun->dump_undofun = btree_rv_nodehdr_dump;
  rv_fun->dump_redofun = btree_rv_nodehdr_dump;

  rv_fun = &RV_fun[RVBT_NDRECORD_UPD];
  rv_fun->recv_index = RVBT_NDRECORD_UPD;
  rv_fun->recv_string = "RVBT_NDRECORD_UPD";
  rv_fun->undofun = btree_rv_noderec_undoredo_update;
  rv_fun->redofun = btree_rv_noderec_undoredo_update;
  rv_fun->dump_undofun = btree_rv_nodehdr_dump;
  rv_fun->dump_redofun = btree_rv_nodehdr_dump;

  rv_fun = &RV_fun[RVBT_NDRECORD_INS];
  rv_fun->recv_index = RVBT_NDRECORD_INS;
  rv_fun->recv_string = "RVBT_NDRECORD_INS";
  rv_fun->undofun = btree_rv_noderec_undo_insert;
  rv_fun->redofun = btree_rv_noderec_redo_insert;
  rv_fun->dump_undofun = btree_rv_noderec_dump_slot_id;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVBT_NDRECORD_DEL];
  rv_fun->recv_index = RVBT_NDRECORD_DEL;
  rv_fun->recv_string = "RVBT_NDRECORD_DEL";
  rv_fun->undofun = btree_rv_noderec_redo_insert;
  rv_fun->redofun = btree_rv_noderec_undo_insert;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = btree_rv_noderec_dump_slot_id;

  rv_fun = &RV_fun[RVBT_DUMMY_1];
  rv_fun->recv_index = RVBT_DUMMY_1;
  rv_fun->recv_string = "RVBT_DUMMY_1";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVBT_DEL_PGRECORDS];
  rv_fun->recv_index = RVBT_DEL_PGRECORDS;
  rv_fun->recv_string = "RVBT_DEL_PGRECORDS";
  rv_fun->undofun = btree_rv_pagerec_insert;
  rv_fun->redofun = btree_rv_pagerec_delete;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = btree_rv_pagerec_dump;

  rv_fun = &RV_fun[RVBT_GET_NEWROOT];
  rv_fun->recv_index = RVBT_GET_NEWROOT;
  rv_fun->recv_string = "RVBT_GET_NEWROOT";
  rv_fun->undofun = NULL;
  rv_fun->redofun = btree_rv_newroot_redo_init;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVBT_GET_NEWPAGE];
  rv_fun->recv_index = RVBT_GET_NEWPAGE;
  rv_fun->recv_string = "RVBT_GET_NEWPAGE";
  rv_fun->undofun = NULL;
  rv_fun->redofun = btree_rv_newpage_redo_init;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVBT_NEW_PGALLOC];
  rv_fun->recv_index = RVBT_NEW_PGALLOC;
  rv_fun->recv_string = "RVBT_NEW_PGALLOC";
  rv_fun->undofun = btree_rv_newpage_undo_alloc;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = btree_rv_newpage_dump_undo_alloc;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVBT_KEYVAL_INSERT];
  rv_fun->recv_index = RVBT_KEYVAL_INSERT;
  rv_fun->recv_string = "RVBT_KEYVAL_INSERT";
  rv_fun->undofun = btree_rv_keyval_undo_insert;
  rv_fun->redofun = btree_rv_leafrec_redo_insert_key;
  rv_fun->dump_undofun = btree_rv_keyval_dump;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVBT_KEYVAL_DELETE];
  rv_fun->recv_index = RVBT_KEYVAL_DELETE;
  rv_fun->recv_string = "RVBT_KEYVAL_DELETE";
  rv_fun->undofun = btree_rv_keyval_undo_delete;
  rv_fun->redofun = btree_rv_leafrec_redo_delete;
  rv_fun->dump_undofun = btree_rv_keyval_dump;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVBT_COPYPAGE];
  rv_fun->recv_index = RVBT_COPYPAGE;
  rv_fun->recv_string = "RVBT_COPYPAGE";
  rv_fun->undofun = btree_rv_undoredo_copy_page;
  rv_fun->redofun = NULL;       /* btree_rv_undoredo_copy_page */
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVBT_NOOP];
  rv_fun->recv_index = RVBT_NOOP;
  rv_fun->recv_string = "RVBT_NOOP";
  rv_fun->undofun = NULL;
  rv_fun->redofun = btree_rv_nop;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVBT_INS_PGRECORDS];
  rv_fun->recv_index = RVBT_INS_PGRECORDS;
  rv_fun->recv_string = "RVBT_INS_PGRECORDS";
  rv_fun->undofun = btree_rv_pagerec_delete;
  rv_fun->redofun = btree_rv_pagerec_insert;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVBT_CREATE_INDEX];
  rv_fun->recv_index = RVBT_CREATE_INDEX;
  rv_fun->recv_string = "RVBT_CREATE_INDEX";
  rv_fun->undofun = btree_rv_undo_create_index;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = btree_rv_dump_create_index;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVCT_NEWPAGE];
  rv_fun->recv_index = RVCT_NEWPAGE;
  rv_fun->recv_string = "RVCT_NEWPAGE";
  rv_fun->undofun = NULL;
  rv_fun->redofun = catalog_rv_new_page_redo;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVCT_INSERT];
  rv_fun->recv_index = RVCT_INSERT;
  rv_fun->recv_string = "RVCT_INSERT";
  rv_fun->undofun = catalog_rv_insert_undo;
  rv_fun->redofun = catalog_rv_insert_redo;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVCT_DELETE];
  rv_fun->recv_index = RVCT_DELETE;
  rv_fun->recv_string = "RVCT_DELETE";
  rv_fun->undofun = catalog_rv_delete_undo;
  rv_fun->redofun = catalog_rv_delete_redo;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVCT_UPDATE];
  rv_fun->recv_index = RVCT_UPDATE;
  rv_fun->recv_string = "RVCT_UPDATE";
  rv_fun->undofun = catalog_rv_update;
  rv_fun->redofun = catalog_rv_update;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVCT_NEW_OVFPAGE_LOGICAL_UNDO];
  rv_fun->recv_index = RVCT_NEW_OVFPAGE_LOGICAL_UNDO;
  rv_fun->recv_string = "RVCT_NEW_OVFPAGE_LOGICAL_UNDO";
  rv_fun->undofun = catalog_rv_ovf_page_logical_insert_undo;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVLOG_OUTSIDE_LOGICAL_REDO_NOOP];
  rv_fun->recv_index = RVLOG_OUTSIDE_LOGICAL_REDO_NOOP;
  rv_fun->recv_string = "RVLOG_OUTSIDE_LOGICAL_REDO_NOOP";
  rv_fun->undofun = NULL;
  rv_fun->redofun = log_rv_outside_noop_redo;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = NULL;

  rv_fun = &RV_fun[RVREPL_DATA_INSERT];
  rv_fun->recv_index = RVREPL_DATA_INSERT;
  rv_fun->recv_string = "RVREPL_DATA_INSERT";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = log_repl_data_dump;

  rv_fun = &RV_fun[RVREPL_DATA_UPDATE];
  rv_fun->recv_index = RVREPL_DATA_UPDATE;
  rv_fun->recv_string = "RVREPL_DATA_UPDATE";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = log_repl_data_dump;

  rv_fun = &RV_fun[RVREPL_DATA_DELETE];
  rv_fun->recv_index = RVREPL_DATA_DELETE;
  rv_fun->recv_string = "RVREPL_DATA_DELETE";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = log_repl_data_dump;

  rv_fun = &RV_fun[RVREPL_SCHEMA];
  rv_fun->recv_index = RVREPL_SCHEMA;
  rv_fun->recv_string = "RVREPL_SCHEMA";
  rv_fun->undofun = NULL;
  rv_fun->redofun = NULL;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = log_repl_schema_dump;

  rv_fun = &RV_fun[RVDK_IDDEALLOC_BITMAP_ONLY];
  rv_fun->recv_index = RVDK_IDDEALLOC_BITMAP_ONLY;
  rv_fun->recv_string = "RVDK_IDDEALLOC_BITMAP_ONLY";
  rv_fun->undofun = disk_rv_set_alloctable_bitmap_only;
  rv_fun->redofun = disk_rv_clear_alloctable_bitmap_only;
  rv_fun->dump_undofun = disk_rv_dump_alloctable_with_vhdr;
  rv_fun->dump_redofun = disk_rv_dump_alloctable_with_vhdr;

  rv_fun = &RV_fun[RVDK_IDDEALLOC_VHDR_ONLY];
  rv_fun->recv_index = RVDK_IDDEALLOC_VHDR_ONLY;
  rv_fun->recv_string = "RVDK_IDDEALLOC_VHDR_ONLY";
  rv_fun->undofun = disk_rv_set_alloctable_vhdr_only;
  rv_fun->redofun = disk_rv_clear_alloctable_vhdr_only;
  rv_fun->dump_undofun = disk_rv_dump_alloctable_with_vhdr;
  rv_fun->dump_redofun = disk_rv_dump_alloctable_with_vhdr;

  rv_fun = &RV_fun[RVDK_INIT_PAGES];
  rv_fun->recv_index = RVDK_INIT_PAGES;
  rv_fun->recv_string = "RVDK_INIT_PAGES";
  rv_fun->undofun = NULL;
  rv_fun->redofun = disk_rv_redo_dboutside_init_pages;
  rv_fun->dump_undofun = NULL;
  rv_fun->dump_redofun = disk_rv_dump_init_pages;

  return NO_ERROR;
}

/*
 * rv_rcvindex_string - RETURN STRING ASSOCIATED WITH GIVEN LOG_RCVINDEX
 *
 * return:
 *
 *   rcvindex(in): Numeric recovery index
 *
 * NOTE: Return a string corresponding to the associated recovery
 *              index identifier.
 */
const char *
rv_rcvindex_string (LOG_RCVINDEX rcvindex)
{
  assert (RV_fun[rcvindex].recv_index == rcvindex);

  if (rcvindex >= RCV_INDEX_END)
    {
      return "Unknown index";
    }

  return RV_fun[rcvindex].recv_string;
}

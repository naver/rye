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
 * boot_sr.h - Boot managment in the server (interface)
 *
 * Note: See .c file for overview and description of the interface functions.
 *
 */

#ifndef _BOOT_SR_H_
#define _BOOT_SR_H_

#ident "$Id$"

#include "config.h"

#include <time.h>

#include "boot.h"
#include "error_manager.h"
#include "storage_common.h"
#include "oid.h"
#include "disk_manager.h"
#include "log_comm.h"
#include "file_io.h"

#if defined (SERVER_MODE)
#define AUTO_ADD_VOL_EXPAND_NPAGES        (20)

typedef struct auto_addvol_job AUTO_ADDVOL_JOB;
struct auto_addvol_job
{
  pthread_mutex_t lock;
  pthread_cond_t cond;
  DBDEF_VOL_EXT_INFO ext_info;
  VOLID ret_volid;
};

#define BOOT_AUTO_ADDVOL_JOB_INITIALIZER                          \
  { PTHREAD_MUTEX_INITIALIZER,                                    \
    PTHREAD_COND_INITIALIZER,                                     \
    {NULL, 0, 0, 0, DISK_PERMVOL_DATA_PURPOSE, false},          \
    NULL_VOLID                                                    \
  };

#endif

typedef enum boot_server_status BOOT_SERVER_STATUS;
enum boot_server_status
{ BOOT_SERVER_UP, BOOT_SERVER_DOWN };
extern BOOT_SERVER_STATUS boot_Server_status;

#if defined (SERVER_MODE)
extern AUTO_ADDVOL_JOB boot_Auto_addvol_job;
#endif

#define BO_IS_SERVER_RESTARTED() \
        (boot_Server_status == BOOT_SERVER_UP)

extern void boot_server_status (BOOT_SERVER_STATUS status);

#if defined(SERVER_MODE)
/* in xserver_interface.h */
extern void boot_donot_shutdown_server_at_exit (void);
extern void xboot_notify_unregister_client (THREAD_ENTRY * thread_p,
					    int tran_index);
#endif /* SERVER_MODE */

extern const char *boot_db_name (void);
extern HFID *boot_find_root_heap (void);

extern VOLID boot_find_next_permanent_volid (THREAD_ENTRY * thread_p);
extern int boot_reset_db_parm (THREAD_ENTRY * thread_p);
extern DKNPAGES boot_max_pages_new_volume (void);
extern DKNPAGES boot_max_pages_for_new_auto_volume_extension (void);
extern DKNPAGES boot_max_pages_for_new_temp_volume (void);
extern VOLID boot_add_auto_volume_extension (THREAD_ENTRY * thread_p,
					     DKNPAGES min_npages,
					     DISK_SETPAGE_TYPE setpage_type,
					     DISK_VOLPURPOSE vol_purpose,
					     bool wait);
extern VOLID boot_add_temp_volume (THREAD_ENTRY * thread_p, DKNPAGES npages);
extern DKNPAGES boot_get_temp_temp_vol_max_npages (void);
extern int boot_add_temp_volume_and_file (VFID * vfid, DKNPAGES npages);
extern int boot_restart_server (THREAD_ENTRY * thread_p, bool print_restart,
				const char *db_name, bool from_backup,
				BO_RESTART_ARG * r_args);
#if defined (SA_MODE)
extern int xboot_restart_from_backup (THREAD_ENTRY * thread_p,
				      int print_restart, const char *db_name,
				      BO_RESTART_ARG * r_args);
#endif /* SA_MODE */

extern bool xboot_shutdown_server (THREAD_ENTRY * thread_p);
extern int xboot_delete (THREAD_ENTRY * thread_p, const char *db_name,
			 bool force_delete);
extern void boot_server_all_finalize (THREAD_ENTRY * thread_p);

#endif /* _BOOT_SR_H_ */

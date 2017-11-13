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
 * release_string.h - release related information (at client and server)
 */

#ifndef _RELEASE_STRING_H_
#define _RELEASE_STRING_H_

#ident "$Id$"

#include "config.h"

#define REL_MAX_RELEASE_LENGTH 15
#define REL_MAX_VERSION_LENGTH 256

typedef struct
{
  short major;
  short minor;
  short patch;
  short build;
} RYE_VERSION;

#define RYE_CUR_VERSION		\
	{ MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION, BUILD_SEQ }

/*
 * REL_COMPATIBILITY - Describes the various types of compatibility we can have.
 *                     Returned by the rel_get_disk_compatible function.
 */
typedef enum
{
  REL_NOT_COMPATIBLE,
  REL_COMPATIBLE,
} REL_COMPATIBILITY;

extern const char *rel_name (void);
extern const char *rel_release_string (void);
extern const char *rel_major_release_string (void);
extern const char *rel_build_number (void);
extern const char *rel_build_os (void);
#if defined(ENABLE_UNUSED_FUNCTION)
extern const char *rel_copyright_header (void);
extern const char *rel_copyright_body (void);
#endif
extern float rel_disk_compatible (void);
extern void rel_set_disk_compatible (float level);
extern int rel_bit_platform (void);

extern REL_COMPATIBILITY rel_get_disk_compatible (float db_level);
extern bool rel_is_log_compatible (const char *writer_rel_str,
				   const char *reader_rel_str);
extern REL_COMPATIBILITY rel_check_net_compatible (const RYE_VERSION * ver1,
						   const RYE_VERSION * ver2);
extern void rel_copy_version_string (char *buf, size_t len);
extern char *rel_version_to_string (RYE_VERSION * version, char *buffer,
				    int buffer_len);

#endif /* _RELEASE_STRING_H_ */

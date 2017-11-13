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
 * release_string.c - release related information (at client and server)
 *
 * Note: This file contains some very simple functions related to version and
 *       releases of Rye. Among these functions are copyright information
 *       of Rye products, name of Rye engine, and version.
 */

#ident "$Id$"

#include <stdlib.h>
#include <string.h>

#include "config.h"
#include "porting.h"
#include "release_string.h"
#include "message_catalog.h"
#include "chartype.h"
#include "language_support.h"
#include "environment_variable.h"
#include "log_comm.h"
#include "log_manager.h"

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * Copyright Information
 */
static const char *copyright_header = "\
Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.\n\
";

static const char *copyright_body = "\
Copyright Information\n\
";
#endif

/*
 * CURRENT VERSIONS
 */
#define makestring1(x) #x
#define makestring(x) makestring1(x)

static const char *release_string = makestring (RELEASE_STRING);
static const char *major_release_string = makestring (MAJOR_RELEASE_STRING);
static const char *build_number = makestring (BUILD_NUMBER);
static const char *package_string = PACKAGE_STRING;
static const char *build_os = makestring (BUILD_OS);
static int bit_platform = __WORDSIZE;

static REL_COMPATIBILITY
rel_get_compatible_internal (const char *base_rel_str,
			     const char *apply_rel_str);

/*
 * Disk (database image) Version Compatibility
 */
static float disk_compatibility_level = 1.0f;

/*
 * rel_copy_version_string - version string of the product
 *   return: void
 */
void
rel_copy_version_string (char *buf, size_t len)
{
  snprintf (buf, len, "%s (%s) (%dbit "
#if defined (NDEBUG)
	    "release"
#else /* NDEBUG */
	    "debug"
#endif /* !NDEBUG */
	    " build for %s) (%s %s)",
	    rel_name (), rel_build_number (), rel_bit_platform (),
	    rel_build_os (), __DATE__, __TIME__);
}

/*
 * rel_name - Name of the product from the message catalog
 *   return: static character string
 */
const char *
rel_name (void)
{
  return package_string;
}

/*
 * rel_release_string - Release number of the product
 *   return: static char string
 */
const char *
rel_release_string (void)
{
  return release_string;
}

/*
 * rel_major_release_string - Major release portion of the release string
 *   return: static char string
 */
const char *
rel_major_release_string (void)
{
  return major_release_string;
}

/*
 * rel_build_number - Build bumber portion of the release string
 *   return: static char string
 */
const char *
rel_build_number (void)
{
  return build_number;
}

/*
 * rel_build_os - Build OS portion of the release string
 *   return: static char string
 */
const char *
rel_build_os (void)
{
  return build_os;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * rel_copyright_header - Copyright header from the message catalog
 *   return: static char string
 */
const char *
rel_copyright_header (void)
{
  const char *name;

  lang_init ();
  name = msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_GENERAL,
			 MSGCAT_GENERAL_COPYRIGHT_HEADER);
  return (name) ? name : copyright_header;
}

/*
 * rel_copyright_body - Copyright body fromt he message catalog
 *   return: static char string
 */
const char *
rel_copyright_body (void)
{
  const char *name;

  lang_init ();
  name = msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_GENERAL,
			 MSGCAT_GENERAL_COPYRIGHT_BODY);
  return (name) ? name : copyright_body;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * rel_disk_compatible - Disk compatibility level
 *   return:
 */
float
rel_disk_compatible (void)
{
  return disk_compatibility_level;
}


/*
 * rel_set_disk_compatible - Change disk compatibility level
 *   return: none
 *   level(in):
 */
void
rel_set_disk_compatible (float level)
{
  disk_compatibility_level = level;
}

/*
 * rel_platform - built platform
 *   return: none
 *   level(in):
 */
int
rel_bit_platform (void)
{
  return bit_platform;
}

/*
 * rel_get_disk_compatible - Test compatibility of disk (database image)
 *                          Check a disk compatibility number stored in
 *                          a database with the disk compatibility number
 *                          for the system being run
 *   return: REL_COMPATIBLE or REL_NOT_COMPATIBLE 
 *   db_level(in):
 */
REL_COMPATIBILITY
rel_get_disk_compatible (float db_level)
{
  if (disk_compatibility_level == db_level)
    {
      return REL_COMPATIBLE;
    }
  else
    {
      return REL_NOT_COMPATIBLE;
    }
}

/*
 * rel_is_log_compatible - Test compatiblility of log file format
 *   return: true if compatible
 *   writer_rel_str(in): release string of the log writer (log file)
 *   reader_rel_str(in): release string of the log reader (system being run)
 */
bool
rel_is_log_compatible (const char *writer_rel_str, const char *reader_rel_str)
{
  REL_COMPATIBILITY compat;

  compat = rel_get_compatible_internal (writer_rel_str, reader_rel_str);
  if (compat == REL_NOT_COMPATIBLE)
    {
      return false;
    }

  return true;
}

/*
 * rel_check_net_compatible - 
 */
REL_COMPATIBILITY
rel_check_net_compatible (const RYE_VERSION * ver1, const RYE_VERSION * ver2)
{
  if (ver1->major == ver2->major)
    {
      return REL_COMPATIBLE;
    }
  else
    {
      return REL_NOT_COMPATIBLE;
    }
}

char *
rel_version_to_string (RYE_VERSION * version, char *buffer, int buffer_len)
{
  snprintf (buffer, buffer_len, "%d.%d.%d.%04d",
	    version->major, version->minor, version->patch, version->build);
  return buffer;
}

/*
 * rel_get_compatible_internal - Compare the release to determine compatibility.
 *   return: REL_COMPATIBILITY
 *
 *   base_rel_str(in): base release string (of database)
 *   apply_rel_str(in): applier's release string (of system)
 *   rules(in): rules to determine forward/backward compatibility
 */
static REL_COMPATIBILITY
rel_get_compatible_internal (const char *base_rel_str,
			     const char *apply_rel_str)
{
  char *base, *apply, *str_a, *str_b;
  int val;

  unsigned char base_major, base_minor, apply_major, apply_minor;
  unsigned short base_patch, apply_patch;

  if (apply_rel_str == NULL || base_rel_str == NULL)
    {
      return REL_NOT_COMPATIBLE;
    }

  /* release string should be in the form of <major>.<minor>[.<patch>] */

  /* check major number */
  str_to_int32 (&val, &str_a, apply_rel_str, 10);
  apply_major = (unsigned char) val;
  str_to_int32 (&val, &str_b, base_rel_str, 10);
  base_major = (unsigned char) val;
  if (apply_major == 0 || base_major == 0)
    {
      return REL_NOT_COMPATIBLE;
    }

  /* skip '.' */
  while (*str_a && *str_a == '.')
    {
      str_a++;
    }
  while (*str_b && *str_b == '.')
    {
      str_b++;
    }

  /* check minor number */
  apply = str_a;
  base = str_b;
  str_to_int32 (&val, &str_a, apply, 10);
  apply_minor = (unsigned char) val;
  str_to_int32 (&val, &str_b, base, 10);
  base_minor = (unsigned char) val;

  /* skip '.' */
  while (*str_a && *str_a == '.')
    {
      str_a++;
    }
  while (*str_b && *str_b == '.')
    {
      str_b++;
    }

  if (apply_major == base_major)
    {
      return REL_COMPATIBLE;
    }
  else
    {
      return REL_NOT_COMPATIBLE;
    }
}

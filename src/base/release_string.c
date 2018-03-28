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

static const char *version_string = makestring (BUILD_NUMBER);
static const char *package_string = PACKAGE_STRING;
static const char *build_os = makestring (BUILD_OS);
static int bit_platform = __WORDSIZE;

static RYE_VERSION rye_Cur_version = RYE_CUR_VERSION;
static RYE_VERSION rye_Null_version = RYE_NULL_VERSION;

RYE_VERSION
rel_cur_version (void)
{
  return rye_Cur_version;
}

RYE_VERSION
rel_null_version (void)
{
  return rye_Null_version;
}

/*
 * rel_copy_release_string - 
 */
void
rel_copy_release_string (char *buf, size_t len)
{
  const char *mode;
#if defined (NDEBUG)
  mode = "release";
#else /* NDEBUG */
  mode = "debug";
#endif /* !NDEBUG */

  snprintf (buf, len, "%s (%dbit %s build for %s) (%s %s)",
            rel_package_string (), rel_bit_platform (), mode, rel_build_os (), __DATE__, __TIME__);
}

/*
 * rel_package_string - 
 */
const char *
rel_package_string (void)
{
  return package_string;
}

/*
 * rel_version_string - Release version of the product
 *   return: static char string
 */
const char *
rel_version_string (void)
{
  return version_string;
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
  name = msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_GENERAL, MSGCAT_GENERAL_COPYRIGHT_HEADER);
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
  name = msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_GENERAL, MSGCAT_GENERAL_COPYRIGHT_BODY);
  return (name) ? name : copyright_body;
}
#endif /* ENABLE_UNUSED_FUNCTION */

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
 * rel_check_disk_compatible - Test compatibility of disk (database image)
 */
REL_COMPATIBILITY
rel_check_disk_compatible (const RYE_VERSION * version)
{
  if (rye_Cur_version.major == version->major)
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
 */
bool
rel_is_log_compatible (const RYE_VERSION * version)
{
  if (rye_Cur_version.major == version->major)
    {
      return true;
    }
  else
    {
      return false;
    }
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

void
rel_version_to_string (RYE_VERSION * version, char *buffer, int buffer_len)
{
  snprintf (buffer, buffer_len, "%d.%d.%d.%04d", version->major, version->minor, version->patch, version->build);
}

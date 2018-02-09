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
 * authenticate.h - Authorization manager
 *
 */

#ifndef _AUTHENTICATE_H_
#define _AUTHENTICATE_H_

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>

#include "error_manager.h"
#include "dbdef.h"
#include "class_object.h"

/*
 * Authorization Class Names
 */

extern const char *AU_ROOT_CLASS_NAME;
extern const char *AU_OLD_ROOT_CLASS_NAME;
// extern const char *AU_USER_CLASS_NAME;
extern const char *AU_AUTH_CLASS_NAME;
extern const char *AU_GRANT_CLASS_NAME;
//extern const char *AU_PUBLIC_USER_NAME;
extern const char *AU_DBA_USER_NAME;

/*
 * Authorization Types
 */
/* obsolete, should be using the definition from dbdef.h */

#define AU_TYPE         DB_AUTH
#define AU_NONE         DB_AUTH_NONE
#define AU_SELECT       DB_AUTH_SELECT
#define AU_INSERT       DB_AUTH_INSERT
#define AU_UPDATE       DB_AUTH_UPDATE
#define AU_DELETE       DB_AUTH_DELETE
#define AU_ALTER        DB_AUTH_ALTER

/*
 * Mask to extract only the authorization bits from a cache.  This can also
 * be used as an absolute value to see if all possible authorizations have
 * been given
 * TODO : LP64
 */

#define AU_TYPE_MASK            0x7F
#define AU_GRANT_MASK           0x7F00
#define AU_FULL_AUTHORIZATION   0x7F7F
#define AU_NO_AUTHORIZATION     0

/*
 * the grant option for any particular authorization type is cached in the
 * same integer, shifted up eight bits.
 */

#define AU_GRANT_SHIFT          8

/* Invalid cache is identified when the high bit is on. */

#define AU_CACHE_INVALID        0x80000000


int au_disable (void);
void au_enable (int save);

MOP au_get_root (void);
MOP au_get_public_user (void);
MOP au_get_dba_user (void);
MOP au_get_user (void);

#define AU_DISABLE(save) \
  do \
    { \
      save = Au_disable; \
      Au_disable = 1; \
    } \
  while (0)
#define AU_ENABLE(save) \
  do \
    { \
      Au_disable = save; \
    } \
  while (0)
#define AU_SAVE_AND_ENABLE(save) \
  do \
    { \
      save = Au_disable; \
      Au_disable = 0; \
    } \
  while (0)
#define AU_SAVE_AND_DISABLE(save) \
  do \
    { \
      save = Au_disable; \
      Au_disable = 1; \
    } \
  while (0)
#define AU_RESTORE(save) \
  do \
    { \
      Au_disable = save; \
    } \
  while (0)

#define AU_DISABLE_PASSWORDS    au_disable_passwords
#define AU_ENABLE_PASSWORDS     au_enable_passwords
#define AU_SET_USER     au_set_user

#define AU_MAX_PASSWORD_CHARS   31
#define AU_MAX_PASSWORD_BUF     2048

/*
 * Global Variables
 */
extern int Au_disable;


extern void au_init (void);
extern void au_final (void);

extern int au_install (void);
extern int au_start (void);
extern int au_login (const char *name, const char *password,
		     bool ignore_dba_privilege);
extern int au_perform_login (const char *name, const char *password,
			     bool ignore_dba_privilege);

extern void au_disable_passwords (void);
extern void au_enable_passwords (void);
extern int au_set_user (MOP newuser);

/* user/group hierarchy maintenance */
extern MOP au_find_user (const char *user_name);
extern MOP au_add_user (const char *name, int *exists);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int au_drop_member (MOP group, MOP member);
#endif
extern int au_drop_user (MOP user);
extern int au_set_password (MOP user, const char *password, bool encode);
extern int au_drop_table (const char *table_name);
extern int au_rename_table (const char *old_name, const char *new_name);

extern char *au_user_name (void);
extern char *au_user_name_dup (void);
/* grant/revoke */
extern int au_grant (MOP user, MOP class_mop, DB_AUTH type,
		     bool grant_option);
extern int au_revoke (MOP user, MOP class_mop, DB_AUTH type);

/* class & instance accessors */
extern int au_fetch_class (MOP op, SM_CLASS ** class_ptr,
			   LOCK lock, DB_AUTH type);
extern int au_fetch_class_force (MOP op, SM_CLASS ** class_, LOCK lock);

extern int au_fetch_instance (MOP op, MOBJ * obj_ptr, LOCK lock,
			      DB_AUTH type);
extern int au_fetch_instance_force (MOP op, MOBJ * obj_ptr, LOCK lock);

/* class cache support */
extern void au_free_authorization_cache (void *cache);
extern void au_reset_authorization_caches (void);

/* misc utilities */
extern int au_change_owner (MOP classmop, MOP owner);
extern MOP au_get_class_owner (MOP classmop);
extern int au_check_user (void);
extern char *au_get_user_name (MOP obj);
extern bool au_is_dba_group_member (MOP user);

/* debugging functions */

#if defined(ENABLE_UNUSED_FUNCTION)
/* used by test code, should be changed to au_dump . . . */
extern void au_print_class_auth (MOP class_mop);
#endif

/* migration utilities */

#if defined (ENABLE_UNUSED_FUNCTION)
extern int au_get_class_privilege (DB_OBJECT * mop, unsigned int *auth);
#endif

/*
 * Etc
 */
extern int au_change_owner_helper (MOP obj, DB_VALUE * returnval,
				   DB_VALUE * class_, DB_VALUE * owner);
extern const char *au_get_public_user_name (void);
extern const char *au_get_user_class_name (void);
#if defined(ENABLE_UNUSED_FUNCTION)
extern char *toupper_string (const char *name1, char *name2);
#endif
#endif /* _AUTHENTICATE_H_ */

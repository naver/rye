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
 * chartype.h : character type checking functions
 *
 *  Note : Functions defined in "ctypes.h" may work incorrectly
 *         on multi-byte characters.
 */

#ifndef _CHARTYPE_H_
#define _CHARTYPE_H_

#ident "$Id$"

#ifdef __cplusplus
extern "C"
{
#endif

  extern int char_islower (int c);
  extern int char_isupper (int c);
  extern int char_isalpha (int c);
  extern int char_isdigit (int c);
  extern int char_isalnum (int c);
  extern int char_isspace (int c);
  extern int char_iseol (int c);
  extern int char_isxdigit (int c);
#if defined (ENABLE_UNUSED_FUNCTION)
  extern int char_isascii (int c);
#endif

  extern int char_tolower (int c);
  extern int char_toupper (int c);

#if defined (ENABLE_UNUSED_FUNCTION)
  extern int char_isupper_iso8859 (int c);
  extern int char_islower_iso8859 (int c);
  extern int char_tolower_iso8859 (int c);
  extern int char_toupper_iso8859 (int c);
#endif

#ifdef __cplusplus
}
#endif

#endif                          /* _CHARTYPE_H_ */

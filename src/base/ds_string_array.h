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
 * ds_string_array.h
 */

#ifndef DS_STRING_ARRAY_H_
#define DS_STRING_ARRAY_H_

#include "porting.h"

typedef char **RSTR_ARRAY;

extern RSTR_ARRAY Rye_split_string (const char *str, const char *delim);
extern void Rye_str_array_free (RSTR_ARRAY array);
extern int Rye_str_array_get_length (RSTR_ARRAY array);
extern int Rye_str_array_find (RSTR_ARRAY array, const char *value);
extern void Rye_str_array_shuffle (RSTR_ARRAY array);

#endif /* DS_STRING_ARRAY_H_ */

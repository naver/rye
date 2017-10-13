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
 * lock_table.c - lock managment module. (client + server)
 *          Definition of lock matrix tables
 */

#ident "$Id$"

#include "config.h"

#include "storage_common.h"

#ifndef DB_NA
#define DB_NA           2
#endif

/*
 *
 *                       LOCK COMPATIBILITY TABLE
 *
 * column : current lock mode (granted lock mode)
 * row    : request lock mode
 * ----------------------------------------------
 *         |   N/A  NULL      S       U       X
 * ----------------------------------------------
 *    N/A  |   N/A   N/A    N/A     N/A     N/A
 *
 *   NULL  |   N/A  True   True    True    True
 *
 *      S  |   N/A  True   True   True    False
 *
 *      U  |   N/A  True   True   False   False
 *
 *      X  |   N/A  True  False   False   False
 * ----------------------------------------------
 * N/A : not applicable
 */

int lock_Comp[5][5] = {
  {DB_NA, DB_NA, DB_NA, DB_NA, DB_NA}		/* N/A */
  ,
  {DB_NA, true, true, true, true}		/* NULL */
  ,
  {DB_NA, true, true, true, false}		/* S */
  ,
  {DB_NA, true, true, false, false}		/* U */
  ,
  {DB_NA, true, false, false, false}		/* X */
};

/*
 *
 *                         LOCK CONVERSION TABLE
 *
 * column : current lock mode (granted lock mode)
 * row    : request lock mode
 * ----------------------------------------
 *         | N/A  NULL    S      U      X
 * ----------------------------------------
 *     N/A | N/A   N/A  N/A    N/A    N/A
 *
 *    NULL | N/A  NULL    S      U      X
 *
 *       S | N/A     S    S      U      X
 *
 *       U | N/A     U    U      U      X
 *
 *       X | N/A     X    X      X      X
 * ----------------------------------------
 * N/A : not applicable
 */

LOCK lock_Conv[5][5] = {
  {NA_LOCK, NA_LOCK, NA_LOCK, NA_LOCK, NA_LOCK}	/* N/A */
  ,
  {NA_LOCK, NULL_LOCK, S_LOCK,  U_LOCK, X_LOCK}	/* NULL */
  ,
  {NA_LOCK, S_LOCK, S_LOCK, U_LOCK, X_LOCK}	/* S */
  ,
  {NA_LOCK, U_LOCK, U_LOCK, U_LOCK, X_LOCK}	/* U */
  ,
  {NA_LOCK, X_LOCK, X_LOCK, X_LOCK, X_LOCK}	/* X */
};

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
 * rbl_error_log.h -
 */

#ifndef RBL_ERROR_LOG_H_
#define RBL_ERROR_LOG_H_

#ident "$Id$"

#include <assert.h>

#define RBL_CCI_ERROR                -1
#define RBL_NODE_NOT_FOUND           -2
#define RBL_OUT_OF_MEMORY            -3
#define RBL_LOG_PAGE_ERROR           -4
#define RBL_LOG_DECOMPRESS_FAIL      -5

#define RBL_DEBUG_SEVERITY       0
#define RBL_NOTICE_SEVERITY      1
#define RBL_ERROR_SEVERITY       2

#if !defined(NDEBUG)
#define RBL_DEBUG(...) rbl_error_log(RBL_DEBUG_SEVERITY, __VA_ARGS__)
#else
#define RBL_DEBUG(...)
#endif

#define RBL_NOTICE(...) rbl_error_log(RBL_NOTICE_SEVERITY, __VA_ARGS__)

#define RBL_ERROR(fileline, err_code, ...) \
  rbl_error_log(RBL_ERROR_SEVERITY, fileline, rbl_Err_msg[-(err_code)], __VA_ARGS__)
#define RBL_ERROR_MSG(...) rbl_error_log(RBL_ERROR_SEVERITY, __VA_ARGS__)

#define RBL_ASSERT(expr) \
  do { \
    if (!(expr)) rbl_error_log (RBL_ERROR_SEVERITY, __FILE__, __LINE__, "Assert Fail: %s\n", __STRING(expr)); \
    assert (expr); \
  } while (0)

extern const char *rbl_Err_msg[];

extern void rbl_error_log_init (const char *prefix, char *dbname, int id);
extern void rbl_error_log_final (bool remove_log_file);
extern void rbl_error_log (int severity, const char *file_name, const int line_no, const char *fmt, ...);
#endif /* RBL_ERROR_LOG_H_ */

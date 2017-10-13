/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors
 *   may be used to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */


/*
 * cci_util.h -
 */

#ifndef	_CCI_UTIL_H_
#define	_CCI_UTIL_H_

#ident "$Id$"

#if defined(CAS) || defined(CAS_BROKER)
#error include error
#endif

/************************************************************************
 * IMPORTED SYSTEM HEADER FILES						*
 ************************************************************************/

/************************************************************************
 * IMPORTED OTHER HEADER FILES						*
 ************************************************************************/

#include "cci_handle_mng.h"

/************************************************************************
 * EXPORTED DEFINITIONS							*
 ************************************************************************/

#define RYE_URL_HEADER		"cci:rye://"
#define RYE_URL_PATTERN		RYE_URL_HEADER  \
	"([a-zA-Z_0-9\\.-]+:[0-9]+(,[a-zA-Z_0-9\\.-]+:[0-9]+)*)/([^:/]+)(:[^:/]*)?(:[^:/]*)?/([^:/?]+)(\\?([a-zA-Z_0-9]+=[^&=?]+(&[a-zA-Z_0-9]+=[^&=?]+)*)?)?"

#define MAX_URL_MATCH_COUNT	6	/* sizeof (match_idx) / sizeof (int) - 1 */

#define URL_DEFAULT_DBUSER	("PUBLIC")

#define MAKE_STRI(x) #x
#define MAKE_STR(x) MAKE_STRI(x)

/************************************************************************
 * EXPORTED TYPE DEFINITIONS						*
 ************************************************************************/


/************************************************************************
 * EXPORTED FUNCTION PROTOTYPES						*
 ************************************************************************/

extern char *ut_make_url (char *buf, int bufsize,
			  const char *server_list, const char *dbname,
			  const char *dbuser, const char *dbpasswd,
			  const char *port_name, const char *property);

extern int ut_str_to_bigint (const char *str, INT64 * value);
extern int ut_str_to_int (const char *str, int *value);
extern int ut_str_to_double (const char *str, double *value);
extern void ut_int_to_str (INT64 value, char *str, int size);
extern void ut_double_to_str (double value, char *str, int size);
extern void ut_datetime_to_str (T_CCI_DATETIME * value, T_CCI_TYPE type,
				char *str, int size);
extern void ut_bit_to_str (char *bit_str, int bit_size, char *str,
			   int str_size);

extern int cci_url_match (const char *src, char *token[]);
extern long ut_timeval_diff_msec (struct timeval *start, struct timeval *end);

extern int ut_host_str_to_addr (const char *host_str, unsigned char *ip_addr);
extern int ut_set_host_info (T_HOST_INFO * host_info,
			     const char *hostname, int port);

extern char *ut_host_info_to_str (char *buf, const T_HOST_INFO * host_info);

extern char *ut_cur_host_info_to_str (char *buf,
				      const T_CON_HANDLE * con_handle);

extern int cci_url_get_properties (T_CON_PROPERTY * handle,
				   const char *properties);
extern int cci_url_get_althosts (T_ALTER_HOST ** ret_alter_host,
				 const char *server_list,
				 char is_load_balance_mode);
extern void con_property_free (T_CON_PROPERTY * con_property);

extern void ut_tolower (char *str);

/************************************************************************
 * EXPORTED VARIABLES							*
 ************************************************************************/

#endif /* _CCI_UTIL_H_ */

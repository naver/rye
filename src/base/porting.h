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
 * porting.h - Functions supporting platform porting
 */

#ifndef _PORTING_H_
#define _PORTING_H_

#ident "$Id$"

#include <netinet/in.h>

#include "config.h"
#ifdef __cplusplus
extern "C"
{
#endif


#if defined(__GNUC__)
#define UNUSED_ARG	__attribute__((__unused__))
#define UNUSED_VAR	__attribute__((__unused__))
#else
#define __attribute__(X)
#endif

#define IMPORT_VAR 	extern
#define EXPORT_VAR

#ifndef L_cuserid
#define L_cuserid 9
#endif				/* !L_cuserid */

#define ONE_K		1024
#define ONE_M		1048576
#define ONE_G		1073741824
#define ONE_T		1099511627776LL
#define ONE_P		1125899906842624LL

#define ONE_SEC		1000
#define ONE_MIN		60000
#define ONE_HOUR	3600000

#define CTIME_MAX 64

#ifndef LLONG_MAX
#define LLONG_MAX	9223372036854775807LL
#endif
#ifndef LLONG_MIN
#define LLONG_MIN	(-LLONG_MAX - 1LL)
#endif
#ifndef ULLONG_MAX
#define ULLONG_MAX	18446744073709551615ULL
#endif


#if !defined(HAVE_CTIME_R)
#  error "HAVE_CTIME_R"
#endif

#if !defined(HAVE_LOCALTIME_R)
#  error "HAVE_LOCALTIME_R"
#endif

#if !defined(HAVE_DRAND48_R)
#  error "HAVE_DRAND48_R"
#endif


#define PATH_SEPARATOR  '/'
#define PATH_CURRENT    '.'

#define IS_PATH_SEPARATOR(c) ((c) == PATH_SEPARATOR)

#define IS_ABS_PATH(p) IS_PATH_SEPARATOR((p)[0])

/*
 * Some platforms (e.g., Solaris) evidently don't define _longjmp.  If
 * it's not available, just use regular old longjmp.
 */
#define LONGJMP _longjmp
#define SETJMP _setjmp

#define GETHOSTNAME(p, l) gethostname(p, l)

#define FINITE(x) finite(x)

#define difftime64(time1, time2) difftime(time1, time2)


#if !defined(HAVE_STRDUP)
  extern char *strdup (const char *str);
#endif				/* HAVE_STRDUP */

#if !defined(HAVE_VASPRINTF)
  extern int vasprintf (char **ptr, const char *format, va_list ap);
#endif				/* HAVE_VASPRINTF */
#if !defined(HAVE_ASPRINTF)
  extern int asprintf (char **ptr, const char *format, ...);
#endif				/* HAVE_ASPRINTF */
#if defined(HAVE_ERR_H)
#include <err.h>
#else
#define err(fd, ...) do { fprintf(stderr, __VA_ARGS__); exit(1); } while (0)
#define errx(fd, ...) do { fprintf(stderr, __VA_ARGS__); exit(1); } while (0)
#endif
  extern int rye_dirname_r (const char *path, char *pathbuf, size_t buflen);
  extern bool rye_mkdir (const char *path, mode_t mode);
  extern bool rye_remove_files (const char *remove_files);

#if !defined(HAVE_DIRNAME)
  char *dirname (const char *path);
#endif				/* HAVE_DIRNAME */
  extern int basename_r (const char *path, char *pathbuf, size_t buflen);
#if !defined(HAVE_BASENAME)
  extern char *basename (const char *path);
#endif				/* HAVE_BASENAME */

#if defined(ENABLE_UNUSED_FUNCTION)
  extern int utona (unsigned int u, char *s, size_t n);
  extern int itona (int i, char *s, size_t n);
#endif

  extern const char *stristr (const char *s, const char *find);

#define strlen(s1)  ((int) strlen(s1))
#define CAST_STRLEN (int)
#define CAST_BUFLEN (int)
#if _FILE_OFFSET_BITS == 32
#define OFF_T_MAX  INT_MAX
#else
#define OFF_T_MAX  LLONG_MAX
#endif

  typedef int SOCKET;
#define INVALID_SOCKET (-1)
#define IS_INVALID_SOCKET(socket) ((socket) < 0)

  typedef enum
  {
    MEM_VSIZE = 1,
    MEM_RSS
  } PROC_MEMORY_TYPE;


/*
 * wrapper for cuserid()
 */
  extern char *getuserid (char *string, int size);
/*
 * wrapper for OS dependent operations
 */
  extern INT64 os_get_mem_size (int pid, PROC_MEMORY_TYPE mem_type);
  extern int os_rename_file (const char *src_path, const char *dest_path);

/* os_send_kill() - send the KILL signal to ourselves */
#define os_send_kill() os_send_signal(SIGKILL)
  typedef void (*SIGNAL_HANDLER_FUNCTION) (int sig_no);
  extern SIGNAL_HANDLER_FUNCTION os_set_signal_handler (const int sig_no,
							SIGNAL_HANDLER_FUNCTION
							sig_handler);
  extern void os_send_signal (const int sig_no);


#define THREAD_RET_T void*
#define THREAD_CALLING_CONVENTION


#if defined(X86)
#define COPYMEM(type,dst,src)   do {		\
  *((type *) (dst)) = *((type *) (src));  	\
}while(0)
#else				/* !X86 */
#define COPYMEM(type,dst,src)   do {		\
  memcpy((dst), (src), sizeof(type)); 		\
}while(0)
#endif				/* !X86 */

/*
 * Interfaces for atomic operations
 *
 * Developers should check HAVE_ATOMIC_BUILTINS before using atomic builtins
 * as follows.
 *  #if defined(HAVE_ATOMIC_BUILTINS)
 *   ... write codes with atomic builtins ...
 *  #else
 *   ... leave legacy codes or write codes without atomic builtins ...
 *  #endif
 *
 * ATOMIC_TAS_xx (atomic test-and-set) writes new_val into *ptr, and returns
 * the previous contents of *ptr. ATOMIC_CAS_xx (atomic compare-and-swap) returns
 * true if the swap is done. It is only done if *ptr equals to cmp_val.
 * ATOMIC_INC_xx (atomic increment) returns the result of *ptr + amount.
 *
 * Regarding Windows, there are two types of APIs to provide atomic operations.
 * While InterlockedXXX functions handles 32bit values, InterlockedXXX64 handles
 * 64bit values. That is why we define two types of macros.
 */
#if defined(HAVE_GCC_ATOMIC_BUILTINS)

#define HAVE_ATOMIC_BUILTINS

#define ATOMIC_TAS_32(ptr, new_val) \
	__sync_lock_test_and_set(ptr, new_val)
#define ATOMIC_CAS_32(ptr, cmp_val, swap_val) \
	__sync_bool_compare_and_swap(ptr, cmp_val, swap_val)
#define ATOMIC_INC_32(ptr, amount) \
	__sync_add_and_fetch(ptr, amount)

#define ATOMIC_TAS_64(ptr, new_val) \
	__sync_lock_test_and_set((ptr), (new_val))
#define ATOMIC_CAS_64(ptr, cmp_val, swap_val) \
	__sync_bool_compare_and_swap((ptr), (cmp_val), (swap_val))
#define ATOMIC_INC_64(ptr, amount) \
	__sync_add_and_fetch((ptr), (amount))

#else				/* HAVE_GCC_ATOMIC_BUILTINS */
/*
 * Currently we do not provide interfaces for atomic operations
 * on other OS or compilers.
 */
#endif				/* HAVE_GCC_ATOMIC_BUILTINS */

#ifdef __cplusplus
}
#endif

typedef struct rye_string RYE_STRING;
struct rye_string
{
  char *buffer;

  int max_length;
  int length;
};

#define string_to_double(str, end_ptr) strtod((str), (end_ptr))

/* for time */
extern INT64 timeval_diff_in_msec (const struct timeval *end_time,
				   const struct timeval *start_time);
extern int timeval_add_msec (struct timeval *added_time,
			     const struct timeval *start_time, int msec);
extern int timeval_to_timespec (struct timespec *to,
				const struct timeval *from);
extern INT64 timeval_to_msec (const struct timeval *val);

/* for stream file */
extern FILE *port_open_memstream (char **ptr, size_t * sizeloc);

extern void port_close_memstream (FILE * fp, char **ptr, size_t * sizeloc);

/* for HA */
extern bool ha_make_log_path (char *path, int size, const char *base,
			      const char *db, const char *node);
extern bool ha_concat_db_and_host (char *db_host, int size, const char *db,
				   const char *host);

/* for string */
extern char *trim (char *str);

extern int parse_int (int *ret_p, const char *str_p, int base);
extern int parse_bigint (INT64 * ret_p, const char *str_p, int base);

extern int str_to_int32 (int *ret_p, char **end_p, const char *str_p,
			 int base);
extern int str_to_uint32 (unsigned int *ret_p, char **end_p,
			  const char *str_p, int base);
extern int str_to_int64 (INT64 * ret_p, char **end_p, const char *str_p,
			 int base);
extern int str_to_uint64 (UINT64 * ret_p, char **end_p, const char *str_p,
			  int base);
extern int rye_init_string (RYE_STRING * buffer, int max_size);
extern void rye_free_string (RYE_STRING * buffer);
extern int rye_append_string (RYE_STRING * buffer, const char *src);
extern char **rye_split_string (RYE_STRING * buffer, const char *delim);
extern int rye_append_format_string (RYE_STRING * buffer,
				     const char *format, ...);
extern void rye_free_string_array (char **array);
extern size_t str_append (char *dst, size_t dst_length, const char *src,
			  size_t max_src_length);
extern char *get_random_string (char *rand_string, int len);
extern in_addr_t hostname_to_ip (const char *host);

#ifndef HAVE_STRLCPY
extern size_t strlcpy (char *, const char *, size_t);
#endif

/* for etc */
#define ABS(i) ((i) >= 0 ? (i) : -(i))

#define THREAD_SLEEP(milliseconds)                              \
  do {                                                          \
    struct timeval to;                                          \
    to.tv_sec = (int) ((milliseconds) / 1000);                  \
    to.tv_usec = ((int) ((milliseconds) * 1000)) % 1000000;     \
    select (0, NULL, NULL, NULL, &to);                          \
  } while(0)

/* for debugging */
#define THREAD_WAIT(condition, ...)     \
  do {                                  \
      while (condition)                 \
        {                               \
          fprintf (stdout, __VA_ARGS__);\
          fflush (stdout);              \
	  THREAD_SLEEP (1000);          \
        }                               \
  } while (0)

#endif /* _PORTING_H_ */

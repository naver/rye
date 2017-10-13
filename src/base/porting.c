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
 * porting.c - Functions supporting platform porting
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <ctype.h>
#include <time.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>

#include <unistd.h>
#include <curses.h>
#include "mprec.h"

#include "porting.h"

#if !defined(HAVE_ASPRINTF)
#include <stdarg.h>
#endif

#ifndef HAVE_STRLCPY
#include <sys/types.h>
#include <string.h>
#endif




#if !defined(HAVE_STRDUP)
/*
 * strdup() - duplicate a string
 *   return: returns a pointer to the duplicated string
 *   str(in): string
 */
char *
strdup (const char *str)
{
  char *sdup;

  assert (str != NULL);

  size_t len = strlen (str) + 1;
  sdup = (char *) malloc (len);
  if (sdup != NULL)
    {
      memcpy (sdup, str, len);
    }

  return sdup;
}
#endif /* !HAVE_STRDUP */

#if !defined(HAVE_VASPRINTF)
int
vasprintf (char **ptr, const char *format, va_list ap)
{
  va_list ap_copy;
  char *buffer = NULL;
  int count;

  va_copy (ap_copy, ap);
  count = vsnprintf (NULL, 0, format, ap);
  if (count >= 0)
    {
      buffer = (char *) malloc (count + 1);
      if (buffer != NULL)
	{
	  count = vsnprintf (buffer, count + 1, format, ap_copy);
	  if (count < 0)
	    {
	      free (buffer);
	    }
	  else
	    {
	      *ptr = buffer;
	    }
	}
      else
	{
	  count = -1;
	}
    }
  va_end (ap_copy);

  return count;
}
#endif /* !HAVE_VASPRINTF */

#if !defined(HAVE_ASPRINTF)
int
asprintf (char **ptr, const char *format, ...)
{
  va_list ap;
  int ret;

  *ptr = NULL;

  va_start (ap, format);
  ret = vasprintf (ptr, format, ap);
  va_end (ap);

  return ret;
}
#endif /* !HAVE_ASPRINTF */

int
rye_dirname_r (const char *path, char *pathbuf, size_t buflen)
{
  const char *endp;
  ptrdiff_t len;

  if (buflen < 2)
    {
      return (errno = ERANGE);
    }

  /* Empty or NULL string gets treated as "." */
  if (path == NULL || *path == '\0')
    {
      pathbuf[0] = PATH_CURRENT;
      pathbuf[1] = '\0';
      return 1;
    }

  /* Strip trailing slashes */
  endp = path + strlen (path) - 1;
  while (endp > path && *endp == PATH_SEPARATOR)
    {
      endp--;
    }

  /* Find the start of the dir */
  while (endp > path && *endp != PATH_SEPARATOR)
    {
      endp--;
    }

  /* Either the dir is "/" or there are no slashes */
  if (endp == path)
    {
      if (*endp == PATH_SEPARATOR)
	{
	  pathbuf[0] = PATH_SEPARATOR;
	}
      else
	{
	  pathbuf[0] = PATH_CURRENT;
	}
      pathbuf[1] = '\0';
      return 1;
    }
  else
    {
      do
	{
	  endp--;
	}
      while (endp > path && *endp == PATH_SEPARATOR);
    }

  len = (ptrdiff_t) (endp - path) + 1;
  if (len + 1 > PATH_MAX)
    {
      return (errno = ENAMETOOLONG);
    }
  if (len + 1 > (int) buflen)
    {
      return (errno = ERANGE);
    }
  (void) strncpy (pathbuf, path, len);
  pathbuf[len] = '\0';
  return (int) len;
}

bool
rye_remove_files (const char *remove_files)
{
  remove (remove_files);

  return true;
}

bool
rye_mkdir (const char *path, mode_t mode)
{
  char dir[PATH_MAX];
  struct stat statbuf;

  if (stat (path, &statbuf) == 0 && S_ISDIR (statbuf.st_mode))
    {
      return true;
    }

  rye_dirname_r (path, dir, PATH_MAX);
  if (stat (dir, &statbuf) == -1)
    {
      if (errno == ENOENT && rye_mkdir (dir, mode))
	{
	  return mkdir (path, mode) == 0;
	}
      else
	{
	  return false;
	}
    }
  else if (S_ISDIR (statbuf.st_mode))
    {
      return mkdir (path, mode) == 0;
    }

  return false;
}

#if !defined(HAVE_DIRNAME)
char *
dirname (const char *path)
{
  static char *bname = NULL;

  if (bname == NULL)
    {
      bname = (char *) malloc (PATH_MAX);
      if (bname == NULL)
	return (NULL);
    }

  return (rye_dirname_r (path, bname, PATH_MAX) < 0) ? NULL : bname;
}
#endif /* !HAVE_DIRNAME */

int
basename_r (const char *path, char *pathbuf, size_t buflen)
{
  const char *endp, *startp;
  ptrdiff_t len;

  if (buflen < 2)
    {
      return (errno = ERANGE);
    }

  /* Empty or NULL string gets treated as "." */
  if (path == NULL || *path == '\0')
    {
      pathbuf[0] = PATH_CURRENT;
      pathbuf[1] = '\0';
      return 1;
    }

  /* Strip trailing slashes */
  endp = path + strlen (path) - 1;
  while (endp > path && *endp == PATH_SEPARATOR)
    {
      endp--;
    }

  /* All slashes becomes "/" */
  if (endp == path && *endp == PATH_SEPARATOR)
    {
      pathbuf[0] = PATH_SEPARATOR;
      pathbuf[1] = '\0';
      return 1;
    }

  /* Find the start of the base */
  startp = endp;
  while (startp > path && *(startp - 1) != PATH_SEPARATOR)
    {
      startp--;
    }

  len = (ptrdiff_t) (endp - startp) + 1;
  if (len + 1 > PATH_MAX)
    {
      return (errno = ENAMETOOLONG);
    }
  if (len + 1 > (int) buflen)
    {
      return (errno = ERANGE);
    }
  (void) strncpy (pathbuf, startp, len);
  pathbuf[len] = '\0';
  return (int) len;
}

#if !defined(HAVE_BASENAME)
char *
basename (const char *path)
{
  static char *bname = NULL;

  if (bname == NULL)
    {
      bname = (char *) malloc (PATH_MAX);
      if (bname == NULL)
	return (NULL);
    }

  return (basename_r (path, bname, PATH_MAX) < 0) ? NULL : bname;
}
#endif /* !HAVE_BASENAME */


#if defined (ENABLE_UNUSED_FUNCTION)
int
utona (unsigned int u, char *s, size_t n)
{
  char nbuf[10], *p, *t;

  if (s == NULL || n == 0)
    {
      return 0;
    }
  if (n == 1)
    {
      *s = '\0';
      return 1;
    }

  p = nbuf;
  do
    {
      *p++ = u % 10 + '0';
    }
  while ((u /= 10) > 0);
  p--;

  t = s;
  do
    {
      *t++ = *p--;
    }
  while (p >= nbuf && --n > 1);
  *t++ = '\0';

  return (t - s);
}

int
itona (int i, char *s, size_t n)
{
  if (s == NULL || n == 0)
    {
      return 0;
    }
  if (n == 1)
    {
      *s = '\0';
      return 1;
    }

  if (i < 0)
    {
      *s++ = '-';
      n--;
      return utona (-i, s, n) + 1;
    }
  else
    {
      return utona (i, s, n);
    }
}
#endif /* ENABLE_UNUSED_FUNCTION */

const char *
stristr (const char *s, const char *find)
{
  char c, sc;
  size_t len;

  if ((c = *find++) != '0')
    {
      len = strlen (find);
      do
	{
	  do
	    {
	      if ((sc = *s++) == '\0')
		{
		  return NULL;
		}
	    }
	  while (toupper (sc) != toupper (c));
	}
      while (strncasecmp (s, find, len) != 0);
      s--;
    }
  return s;
}

/*
 * wrapper for cuserid() function
 */
char *
getuserid (char *string, int size)
{
  if (cuserid (string) == NULL)
    {
      return NULL;
    }
  else
    {
      string[size - 1] = '\0';
      return string;
    }
}

/*
 * wrapper for OS dependent operations
 */

static char *
skip_token (char *p)
{
  while (isspace (*p))
    p++;
  while (*p && !isspace (*p))
    p++;
  return p;
}

INT64
os_get_mem_size (int pid)
{
  char buf[4096];
  char *p;
  int fd;
  int read_len, i;
  INT64 psize;

  if (pid <= 0)
    {
      return -1;
    }

  sprintf (buf, "/proc/%d/stat", pid);
  fd = open (buf, O_RDONLY);
  if (fd < 0)
    {
      return -1;
    }

  read_len = read (fd, buf, sizeof (buf) - 1);
  close (fd);

  if (read_len < 0 || read_len >= (int) sizeof (buf))
    {
      return 1;
    }
  buf[read_len] = '\0';

  p = strchr (buf, ')');
  p++;
  for (i = 0; i < 20; i++)
    {
      p = skip_token (p);
    }

  psize = atoll (p);
  return psize;
}

/*
 * os_rename_file() - rename a file
 *   return: 0 on success, otherwise -1
 *   src_path(in): source path
 *   dest_path(in): destination path
 */
int
os_rename_file (const char *src_path, const char *dest_path)
{
  return rename (src_path, dest_path);
}

#include <signal.h>
/*
 * os_set_signal_handler() - sets the signal handler
 *   return: Old signal handler which can be used to restore
 *           If it fails, it returns SIG_ERR
 *   signo(in): specifies the signal except SIGKILL and/or SIGSTOP
 *   sig_handler(in): Function to handle the above signal or SIG_DFL, SIG_IGN
 *
 * Note: We would like the signals to work as follow:
 *   - Multiple signals should not get lost; the system should queue them
 *   - Signals must be reliable. The signal handler should not need to
 *     reestablish itself like in the old days of Unix
 *   - The signal hander remains installed after a signal has been delivered
 *   - If a caught signal occurs during certain system calls terminating
 *     the call prematurely, the call is automatically restarted
 *   - If SIG_DFL is given, the default action is reinstaled
 *   - If SIG_IGN is given as sig_handler, the signal is subsequently ignored
 *     and pending instances of the signal are discarded
 */
SIGNAL_HANDLER_FUNCTION
os_set_signal_handler (const int sig_no, SIGNAL_HANDLER_FUNCTION sig_handler)
{
  struct sigaction act;
  struct sigaction oact;

  act.sa_handler = sig_handler;
  act.sa_flags = 0;

  if (sigemptyset (&act.sa_mask) < 0)
    {
      return (SIG_ERR);
    }

  switch (sig_no)
    {
    case SIGALRM:
#if defined(SA_INTERRUPT)
      act.sa_flags |= SA_INTERRUPT;	/* disable other interrupts */
#endif /* SA_INTERRUPT */
      break;
    default:
#if defined(SA_RESTART)
      act.sa_flags |= SA_RESTART;	/* making certain system calls
					   restartable across signals */
#endif /* SA_RESTART */
      break;
    }

  if (sigaction (sig_no, &act, &oact) < 0)
    {
      return (SIG_ERR);
    }

  return (oact.sa_handler);
}

/*
 * os_send_signal() - send the signal to ourselves
 *   return: none
 *   signo(in): signal number to send
 */
void
os_send_signal (const int sig_no)
{
  kill (getpid (), sig_no);
}

/*
 * timeval_diff_in_msec -
 *
 *   return: msec
 *
 */
INT64
timeval_diff_in_msec (const struct timeval *end_time,
		      const struct timeval *start_time)
{
  INT64 msec;

  msec = (end_time->tv_sec - start_time->tv_sec) * 1000LL;
  msec += (end_time->tv_usec - start_time->tv_usec) / 1000LL;

  return msec;
}

/*
 * timeval_add_msec -
 *   return: 0
 *
 *   addted_time(out):
 *   start_time(in):
 *   msec(in):
 */
int
timeval_add_msec (struct timeval *added_time,
		  const struct timeval *start_time, int msec)
{
  int usec;

  if (added_time == start_time)
    {
      assert (false);
      return -1;
    }

  added_time->tv_sec = start_time->tv_sec + msec / 1000LL;
  usec = (msec % 1000LL) * 1000LL;

  added_time->tv_sec += (start_time->tv_usec + usec) / 1000000LL;
  added_time->tv_usec = (start_time->tv_usec + usec) % 1000000LL;

  return 0;
}

/*
 * timeval_to_timespec -
 *   return: 0
 *
 *   to(out):
 *   from(in):
 */
int
timeval_to_timespec (struct timespec *to, const struct timeval *from)
{
  assert (to != NULL);
  assert (from != NULL);

  to->tv_sec = from->tv_sec;
  to->tv_nsec = from->tv_usec * 1000LL;

  return 0;
}

/*
 * timeval_to_msec -
 *
 *   return: msec
 *
 */
INT64
timeval_to_msec (const struct timeval * val)
{
  INT64 time_in_msecs;

  time_in_msecs = val->tv_sec * 1000LL;
  time_in_msecs += val->tv_usec / 1000LL;

  return time_in_msecs;
}


/*
 * port_open_memstream - make memory stream file handle if possible.
 *			 if not, make temporiry file handle.
 *   return: file handle
 *
 *   ptr (out): memory stream (or temp file name)
 *   sizeloc (out): stream size
 *
 *   NOTE: this function use memory allocation in it.
 *         so you should ensure that stream size is not too huge
 *         before you use this.
 */
FILE *
port_open_memstream (char **ptr, size_t * sizeloc)
{
#ifdef HAVE_OPEN_MEMSTREAM
  return open_memstream (ptr, sizeloc);
#else
  *ptr = tempnam (NULL, "rye_");
  return fopen (*ptr, "w+");
#endif
}


/*
 * port_close_memstream - flush file handle and close
 *
 *   fp (in): file handle to close
 *   ptr (in/out): memory stream (out) or temp file name (in)
 *   sizeloc (out): stream size
 *
 *   NOTE: you should call this function before refer ptr
 *         this function flush contents to ptr before close handle
 */
void
port_close_memstream (FILE * fp, UNUSED_ARG char **ptr,
		      UNUSED_ARG size_t * sizeloc)
{
#if !defined(HAVE_OPEN_MEMSTREAM)
  char *buff = NULL;
  struct stat stat_buf;
  size_t n;
#endif

  fflush (fp);

  if (fp)
    {
#ifdef HAVE_OPEN_MEMSTREAM
      fclose (fp);
#else
      if (fstat (fileno (fp), &stat_buf) == 0)
	{
	  *sizeloc = stat_buf.st_size;

	  buff = malloc (*sizeloc + 1);
	  if (buff)
	    {
	      fseek (fp, 0, SEEK_SET);
	      n = fread (buff, 1, *sizeloc, fp);
	      buff[n] = '\0';
	      *sizeloc = n;
	    }
	}

      fclose (fp);
      /* tempname from port_open_memstream */
      unlink (*ptr);
      free (*ptr);

      /* set output */
      *ptr = buff;
#endif
    }
}

/* HA */
bool
ha_make_log_path (char *path, int size, const char *base,
		  const char *db, const char *node)
{
  return snprintf (path, size, "%s/%s_%s", base, db, node) >= 0;
}

bool
ha_concat_db_and_host (char *db_host, int size, const char *db,
		       const char *host)
{
  return snprintf (db_host, size, "%s@%s", db, host) >= 0;
}


char *
trim (char *str)
{
  char *p;
  char *s;

  if (str == NULL)
    return (str);

  for (s = str;
       *s != '\0' && (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r');
       s++)
    ;
  if (*s == '\0')
    {
      *str = '\0';
      return (str);
    }

  /* *s must be a non-white char */
  for (p = s; *p != '\0'; p++)
    ;
  for (p--; *p == ' ' || *p == '\t' || *p == '\n' || *p == '\r'; p--)
    ;
  *++p = '\0';

  if (s != str)
    memmove (str, s, strlen (s) + 1);

  return (str);
}

int
parse_int (int *ret_p, const char *str_p, int base)
{
  int error = 0;
  int val;
  char *end_p;

  assert (ret_p != NULL);
  assert (str_p != NULL);

  *ret_p = 0;

  error = str_to_int32 (&val, &end_p, str_p, base);
  if (error < 0)
    {
      return -1;
    }

  if (*end_p != '\0')
    {
      return -1;
    }

  *ret_p = val;

  return 0;
}

int
parse_bigint (INT64 * ret_p, const char *str_p, int base)
{
  int error = 0;
  INT64 val;
  char *end_p;

  assert (ret_p != NULL);
  assert (str_p != NULL);

  *ret_p = 0;

  error = str_to_int64 (&val, &end_p, str_p, base);
  if (error < 0)
    {
      return -1;
    }

  if (*end_p != '\0')
    {
      return -1;
    }

  *ret_p = val;

  return 0;
}

int
str_to_int32 (int *ret_p, char **end_p, const char *str_p, int base)
{
  long val = 0;

  assert (ret_p != NULL);
  assert (end_p != NULL);
  assert (str_p != NULL);

  *ret_p = 0;
  *end_p = NULL;

  errno = 0;
  val = strtol (str_p, end_p, base);

  if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN))
      || (errno != 0 && val == 0))
    {
      return -1;
    }

  if (*end_p == str_p)
    {
      return -1;
    }

  /* Long is 8 bytes and int is 4 bytes in Linux 64bit, so the
   * additional check of integer range is necessary.
   */
  if (val < INT_MIN || val > INT_MAX)
    {
      return -1;
    }

  *ret_p = (int) val;

  return 0;
}

int
str_to_uint32 (unsigned int *ret_p, char **end_p, const char *str_p, int base)
{
  unsigned long val = 0;

  assert (ret_p != NULL);
  assert (end_p != NULL);
  assert (str_p != NULL);

  *ret_p = 0;
  *end_p = NULL;

  errno = 0;
  val = strtoul (str_p, end_p, base);

  if ((errno == ERANGE && val == ULONG_MAX) || (errno != 0 && val == 0))
    {
      return -1;
    }

  if (*end_p == str_p)
    {
      return -1;
    }

  /* Long is 8 bytes and int is 4 bytes in Linux 64bit, so the
   * additional check of integer range is necessary.
   */
  if (val > UINT_MAX)
    {
      return -1;
    }

  *ret_p = (unsigned int) val;

  return 0;
}


int
str_to_int64 (INT64 * ret_p, char **end_p, const char *str_p, int base)
{
  INT64 val;

  assert (ret_p != NULL);
  assert (end_p != NULL);
  assert (str_p != NULL);

  *ret_p = 0;
  *end_p = NULL;

  errno = 0;
  val = strtoll (str_p, end_p, base);

  if ((errno == ERANGE && (val == LLONG_MAX || val == LLONG_MIN))
      || (errno != 0 && val == 0))
    {
      return -1;
    }

  if (*end_p == str_p)
    {
      return -1;
    }

  *ret_p = val;

  return 0;
}

int
str_to_uint64 (UINT64 * ret_p, char **end_p, const char *str_p, int base)
{
  UINT64 val;

  assert (ret_p != NULL);
  assert (end_p != NULL);
  assert (str_p != NULL);

  *ret_p = 0;
  *end_p = NULL;

  errno = 0;
  val = strtoull (str_p, end_p, base);

  if ((errno == ERANGE && val == ULLONG_MAX) || (errno != 0 && val == 0))
    {
      return -1;
    }

  if (*end_p == str_p)
    {
      return -1;
    }

  *ret_p = val;

  return 0;
}

/*
 * rye_init_string () -
 *    return: 0: SUCCESS, -1: ERROR
 *
 *    buffer(out):
 *    max_size(in):
 */
int
rye_init_string (RYE_STRING * buffer, int max_size)
{
  char *mem;

  if (buffer == NULL || max_size < 1)
    {
      return -1;
    }

  mem = (char *) malloc (max_size);
  if (mem == NULL)
    {
      return -1;
    }
  mem[0] = '\0';

  buffer->buffer = mem;
  buffer->max_length = max_size;
  buffer->length = 0;

  return 0;
}

/*
 * rye_free_string () -
 *
 */
void
rye_free_string (RYE_STRING * buffer)
{
  if (buffer == NULL)
    {
      return;
    }
  if (buffer->buffer != NULL)
    {
      free (buffer->buffer);
    }

  buffer->buffer = NULL;
  buffer->max_length = 0;
  buffer->length = 0;
}

/*
 * rye_append_string () -
 *    return: 0: SUCCESS, -1: ERROR
 *
 *    buffer(in/out):
 *    src(in):
 */
int
rye_append_string (RYE_STRING * buffer, const char *src)
{
  int len;

  if (buffer == NULL)
    {
      return -1;
    }

  len = (int) strlcpy (buffer->buffer + buffer->length, src,
		       buffer->max_length);
  if (buffer->length + len >= buffer->max_length)
    {
      return -1;
    }
  buffer->length += len;

  return 0;
}

/*
 * rye_append_string () -
 *    return: 0: SUCCESS, -1: ERROR
 *
 *    buffer(in/out):
 *    src(in):
 */
int
rye_append_format_string (RYE_STRING * buffer, const char *format, ...)
{
  va_list ap;
  int len;

  if (buffer == NULL)
    {
      return -1;
    }

  va_start (ap, format);
  len = (int) vsnprintf (buffer->buffer + buffer->length,
			 buffer->max_length - buffer->length, format, ap);
  va_end (ap);

  if (buffer->length + len >= buffer->max_length)
    {
      return -1;
    }
  buffer->length += len;

  return 0;
}

/*
 * rye_split_string ()-
 *   return: char** is null terminated char* array;
 *           ex: "a,b" --> { "a", "b", NULL }
 *   str(in):
 *   delim(in):
 */
char **
rye_split_string (RYE_STRING * buffer, const char *delim)
{
  char *o;
  char *save = NULL, *v;
  char **r = NULL;
  int count = 1;

  if (buffer == NULL)
    {
      return NULL;
    }

  o = strdup (buffer->buffer);
  if (o == NULL)
    {
      return NULL;
    }

  for (v = strtok_r (o, delim, &save);
       v != NULL; v = strtok_r (NULL, delim, &save))
    {
      r = (char **) realloc (r, sizeof (char *) * (count + 1));
      if (r == NULL)
	{
	  free (o);
	  return NULL;
	}
      r[count - 1] = strdup (v);
      r[count] = NULL;
      count++;
    }

  free (o);
  return r;
}

/*
 * rye_free_string_array ()-
 *   return:
 *
 *   array(in/out):
 */
void
rye_free_string_array (char **array)
{
  int i;

  for (i = 0; array[i] != NULL; i++)
    {
      free (array[i]);
    }
  free (array);
}

/*
 * str_append () -
 *    return: dst_length + src_length
 *
 *    dst(in/out):
 *    dst_length(in):
 *    src(in):
 *    max_src_length(in):
 */
size_t
str_append (char *dst, size_t dst_length, const char *src,
	    size_t max_src_length)
{
  size_t len;

  len = strlcpy (dst + dst_length, src, max_src_length);

  return dst_length + len;
}

/*
 * get_random_string () -
 *    return: random string
 *
 *    rand_string(out):
 *    len(in): generated string len
 */
char *
get_random_string (char *rand_string, int len)
{
  static bool init = false;
  int max_index;
  int i;

  char alphanum[] = "0123456789abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  if (init == false)
    {
      srand (time (NULL));
      init = true;
    }

  max_index = sizeof (alphanum) - 1;
  for (i = 0; i < len; i++)
    {
      rand_string[i] = alphanum[rand () % max_index];
    }
  rand_string[i] = '\0';

  return rand_string;
}

in_addr_t
hostname_to_ip (const char *host)
{
  in_addr_t in_addr;

  /*
   * First try to convert to the host name as a dotted-decimal number.
   * Only if that fails do we call gethostbyname.
   */
  in_addr = inet_addr (host);
  if (in_addr == INADDR_NONE)
    {
# if defined (HAVE_GETHOSTBYNAME_R)
# if defined (HAVE_GETHOSTBYNAME_R_GLIBC)
      struct hostent *hp, hent;
      int herr;
      char buf[1024];

      if (gethostbyname_r (host, &hent, buf, sizeof (buf), &hp, &herr) != 0
	  || hp == NULL)
	{
	  return INADDR_NONE;
	}
      if (hent.h_length != sizeof (in_addr_t))
	{
	  assert (0);
	  return INADDR_NONE;
	}
      memcpy ((void *) &in_addr, (void *) hent.h_addr, hent.h_length);
# else
# error "HAVE_GETHOSTBYNAME_R_GLIBC"
#endif /* !HAVE_GETHOSTBYNAME_R_GLIBC */
# else
# error "HAVE_GETHOSTBYNAME_R"
#endif /* !HAVE_GETHOSTBYNAME_R */
    }

  return in_addr;
}

#ifndef HAVE_STRLCPY
/*
 * Copy src to string dst of size siz.  At most siz-1 characters
 * will be copied.  Always NUL terminates (unless siz == 0).
 * Returns strlen(src); if retval >= siz, truncation occurred.
 */
size_t
strlcpy (char *dst, const char *src, size_t siz)
{
  char *d = dst;
  const char *s = src;
  size_t n = siz;

  assert (dst != NULL);
  assert (src != NULL);

  /* Copy as many bytes as will fit */
  if (n != 0 && --n != 0)
    {
      do
	{
	  if ((*d++ = *s++) == 0)
	    break;
	}
      while (--n != 0);
    }

  /* Not enough room in dst, add NUL and traverse rest of src */
  if (n == 0)
    {
      if (siz != 0)
	*d = '\0';		/* NUL-terminate dst */
      while (*s++)
	;
    }

  return (s - src - 1);		/* count does not include NUL */
}
#endif /* !HAVE_STRLCPY */

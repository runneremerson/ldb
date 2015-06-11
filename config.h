#ifndef LDB_CONFIG_H
#define LDB_CONFIG_H

#ifdef __APPLE__
#include <AvailabilityMacros.h>
#endif

/* Use tcmalloc's malloc_size() when available.
 * When tcmalloc is used, native OSX malloc_size() may never be used because
 * this expects a different allocation scheme. Therefore, *exclusively* use
 * either tcmalloc or OSX's malloc_size()! */

#if defined(USE_TCMALLOC)
#include <gperftools/tcmalloc.h>
#define USE_TCMALLOC 1
#define ldb_malloc_size(p) tc_malloc_size(p)
#endif




/* test for backtrace() */
#if defined(__APPLE__) || defined(__linux__)
#define HAVE_BACKTRACE 1
#endif


#endif

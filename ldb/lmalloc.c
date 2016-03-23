#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include "config.h"


#if defined(USE_TCMALLOC)
#define malloc(size) tc_malloc(size)
#define calloc(count,size) tc_calloc(count,size)
#define realloc(ptr,size) tc_realloc(ptr,size)
#define free(ptr) tc_free(ptr)
#endif

#if defined(USE_JEMALLOC)
#define malloc(size) je_malloc(size)
#define calloc(count,size) je_calloc(count,size)
#define realloc(ptr,size) je_realloc(ptr,size)
#define free(ptr) je_free(ptr)
#endif



static void lmalloc_oom(size_t size) {
    fprintf(stderr, "lmalloc: Out of memory trying to allocate %zu bytes\n",
        size);
    fflush(stderr);
    abort();
}


void *lmalloc(size_t size) {
    void *ptr = malloc(size);

    if (!ptr) {
        // try again
        ptr = malloc(size);
        if (!ptr)
            lmalloc_oom(size);
    }
    return ptr;
}


void *lrealloc(void *ptr, size_t size) {
    void *newptr;

    if (ptr == NULL) return lmalloc(size);

    newptr = realloc(ptr,size);
    if (!newptr) lmalloc_oom(size);

    return newptr;
}

void lfree(void *ptr) {
    if (ptr == NULL) return;
    free(ptr);
}

char *lstrdup(const char *s) {
    size_t l = strlen(s)+1;
    char *p = lmalloc(l);

    memcpy(p,s,l);
    return p;
}

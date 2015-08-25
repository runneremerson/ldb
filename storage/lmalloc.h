#ifndef LDB_LMALLOC_H
#define LDB_LMALLOC_H


void *lmalloc(size_t size);
void *lrealloc(void *ptr, size_t size);
void lfree(void *ptr);
char *lstrdup(const char *s);

#endif //LDB_LMALLOC_H

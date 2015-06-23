#ifndef LDB_META_H
#define LDB_META_H

#include "ldb_slice.h"

#include <stdint.h>



typedef struct ldb_meta_t  ldb_meta_t;

ldb_meta_t* ldb_meta_create(uint32_t vercare, uint64_t lastver, uint64_t nextver);

void ldb_meta_destroy(ldb_meta_t* meta);

uint32_t ldb_meta_vercare(const ldb_meta_t* meta);

uint64_t ldb_meta_lastver(const ldb_meta_t* meta);

uint64_t ldb_meta_nextver(const ldb_meta_t* meta);

void ldb_meta_encode(char* buf, uint32_t vercare, uint64_t lastver, uint64_t nextver);


ldb_slice_t* ldb_meta_slice_create(const ldb_meta_t* meta);

#endif //LDB_META_H


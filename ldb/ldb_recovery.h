#ifndef LDB_RECOVERY_H
#define LDB_RECOVERY_H

#include "ldb_context.h"

typedef struct ldb_recovery_t     ldb_recovery_t;

ldb_recovery_t* ldb_recovery_create( ldb_context_t* context );

void ldb_recovery_destroy(ldb_recovery_t* recovery);

void ldb_recovery_rec(ldb_recovery_t* recovery);

void ldb_recovery_rec_batch(ldb_recovery_t* recovery, size_t limit);


#endif //LDB_RECOVERY_H

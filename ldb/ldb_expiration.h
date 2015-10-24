#ifndef LDB_EXPIRATION_H
#define LDB_EXPIRATION_H

#include "ldb_context.h"


typedef struct ldb_expiration_t     ldb_expiration_t;

ldb_expiration_t* ldb_expiration_create( ldb_context_t* context );
void ldb_expiration_destroy( ldb_expiration_t* expiration );

int ldb_expiration_next( ldb_expiration_t* expiration );


#endif //LDB_EXPIRATION_H

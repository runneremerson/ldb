CC=gcc
MAKE=make

LDB_OBJS = ldb_session.o ldb_bytes.o ldb_context.o ldb_list.o ldb_meta.o ldb_slice.o \
	   ldb_iterator.o lmalloc.o util.o t_string.o t_zset.o t_hash.o t_set.o ldb_expiration.o \
	   ldb_recovery.o



CFLAGS= -std=gnu99 -O0 -g -I../deps/leveldb-1.18/include -I../  -I../../ -DUSE_TCMALLOC=1 -DUSE_INT=1 

all: ${LDB_OBJS}
	ar -cru ../libldb.a ${LDB_OBJS}


ldb_session.o: ldb_session.h ldb_session.c
	${CC} ${CFLAGS} -c ldb_session.c
ldb_bytes.o: ldb_bytes.h ldb_bytes.c
	${CC} ${CFLAGS} -c ldb_bytes.c
ldb_context.o: ldb_context.h ldb_context.c
	${CC} ${CFLAGS} -c ldb_context.c
ldb_list.o: ldb_list.h ldb_list.c
	${CC} ${CFLAGS} -c ldb_list.c
ldb_meta.o: ldb_meta.h ldb_meta.c
	${CC} ${CFLAGS} -c ldb_meta.c
ldb_slice.o: ldb_slice.h ldb_slice.c
	${CC} ${CFLAGS} -c ldb_slice.c
ldb_iterator.o: ldb_iterator.h ldb_iterator.c
	${CC} ${CFLAGS} -c ldb_iterator.c
ldb_expiration.o: ldb_expiration.h ldb_expiration.c
	${CC} ${CFLAGS} -c ldb_expiration.c
ldb_recovery.o: ldb_recovery.h ldb_recovery.c
	${CC} ${CFLAGS} -c ldb_recovery.c
lmalloc.o: lmalloc.h lmalloc.c
	${CC} ${CFLAGS} -c lmalloc.c
util.o: util.h util.c
	${CC} ${CFLAGS} -c util.c
t_string.o: t_string.h t_string.c
	${CC} ${CFLAGS} -c t_string.c
t_zset.o: t_zset.h t_zset.c
	${CC} ${CFLAGS} -c t_zset.c
t_hash.o: t_hash.h t_hash.c
	${CC} ${CFLAGS} -c t_hash.c
t_set.o: t_set.h t_set.c
	${CC} ${CFLAGS} -c t_set.c



clean:
	rm -f  *.o  *.a
	rm -f ldb.test



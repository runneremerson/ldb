CC=gcc
MAKE=make

LDB_OBJS = ldb_session.o ldb_bytes.o ldb_context.o ldb_list.o ldb_meta.o ldb_slice.o \
	   ldb_iterator.o lmalloc.o t_kv.o t_zset.o 

ORG_OBJS = ldb_bytes.o ldb_list.o ldb_slice.o ldb_iterator.o lmalloc.o


#CFLAGS= -std=gnu99 -O2 -I../ -I../../ -DUSE_TCMALLOC=1 -DUSE_INT=1 
CFLAGS= -std=gnu99 -O0 -g -I../ -I../../ -DUSE_TCMALLOC=1 -DUSE_INT=1 

all: ${LDB_OBJS} ${ORG_OBJS}
	ar -cru ../libldb.a ${LDB_OBJS}
	ar -cru ../liborg.a ${ORG_OBJS}


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
lmalloc.o: lmalloc.h lmalloc.c
	${CC} ${CFLAGS} -c lmalloc.c
t_kv.o: t_kv.h t_kv.c
	${CC} ${CFLAGS} -c t_kv.c
t_zset.o: t_zset.h t_zset.c
	${CC} ${CFLAGS} -c t_zset.c



clean:
	rm -f  *.o  *.a



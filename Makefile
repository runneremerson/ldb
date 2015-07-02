CC=gcc
OBJS = ldb_session.o ldb_bytes.o ldb_context.o ldb_list.o ldb_meta.o ldb_slice.o \
	   lmalloc.o t_kv.o 


#CFLAGS= -std=gnu99 -I../ -I../../ -DUSE_TCMALLOC=1 -DUSE_INT=1 
CFLAGS= -std=gnu99 -O0 -g -I../ -I../../  -DUSE_INT=1 
CLIBS= -lpthread #-ltcmalloc  

all: ${OBJS}
	ar -cru libldb.a ${OBJS}

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
lmalloc.o: lmalloc.h lmalloc.c
	${CC} ${CFLAGS} -c lmalloc.c
t_kv.o: t_kv.h t_kv.c
	${CC} ${CFLAGS} -c t_kv.c



clean:
	rm -f  *.o  *.a



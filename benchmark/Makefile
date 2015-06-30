CC=gcc

LIBS = ./libldb.a 

CFLAGS= -std=gnu99 -I../ -I../../ -DUSE_TCMALLOC=1 -DUSE_INT=1 
CLIBS= -lleveldb-ldb -lpthread -ltcmalloc  

all: ldb_bench.o
	${CC} -o ldb_bench ldb_bench.o ${LIBS} ${CLIBS}

ldb_bench.o: ldb_bench.c
	${CC} ${CFLAGS} -c ldb_bench.c

clean:
	rm -f *.o  *.a
	rm -f ldb_bench
	

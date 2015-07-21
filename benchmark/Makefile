CC=gcc
CXX=g++

LDB_LIBS = ../libldb.a /usr/local/lib/libleveldb-ldb.a /usr/local/lib/libtcmalloc_minimal.a 
ORG_LIBS = ../liborg.a /usr/local/lib/libleveldb.a /usr/local/lib/libtcmalloc_minimal.a

LDB_CFLAGS= -std=gnu99 -O2 -I../src/ -I../../ -DUSE_TCMALLOC=1 -DUSE_INT=1 
#LDB_CFLAGS= -std=gnu99 -O0 -g  -I../src/ -I../../ -DUSE_TCMALLOC=1  -DUSE_INT=1 
ORG_CFLAGS= -std=gnu99 -O2 -I../src/ -I../../ -DUSE_TCMALLOC=1  -DUSE_INT=1 
#ORG_CFLAGS= -std=gnu99 -O0 -g  -I../src/ -I../../ -DUSE_TCMALLOC=1  -DUSE_INT=1 

CLIBS=  -lpthread 

all: ldb_bench org_bench

ldb_bench: ldb_bench.o
	${CXX} -o ../ldb_bench ldb_bench.o  ${LDB_LIBS} ${CLIBS}

org_bench: org_bench.o org_context.o
	${CXX} -o ../org_bench org_bench.o org_context.o  ${ORG_LIBS} ${CLIBS}

ldb_bench.o: ldb_bench.c
	${CC} ${LDB_CFLAGS} -c ldb_bench.c 

org_bench.o: org_bench.c
	${CC} ${ORG_CFLAGS} -c org_bench.c

org_context.o: org_context.c
	${CC} ${ORG_CFLAGS} -c org_context.c


clean:
	rm -f *.o
	

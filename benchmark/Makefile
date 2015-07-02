CC=gcc
CXX=g++

LIBS = ../libldb.a /usr/local/lib/libleveldb-ldb.a 

#CFLAGS= -std=gnu99 -I../ -I../../ -DUSE_TCMALLOC=1 -DUSE_INT=1 
CFLAGS= -std=gnu99 -O0 -g -I../ -I../../  -DUSE_INT=1 
CLIBS=  -lpthread #-ltcmalloc  

all: ldb_bench.o
	${CXX}  -o ldb_bench ldb_bench.o  ${LIBS} ${CLIBS}

ldb_bench.o: ldb_bench.c
	${CC} ${CFLAGS} -c ldb_bench.c 

clean:
	rm -f *.o
	rm -f ldb_bench
	

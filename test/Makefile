CC=gcc
CXX=g++

LIBS = ../libldb.a ../deps/leveldb-1.18/libleveldb-ldb.a /usr/local/lib/libtcmalloc_minimal.a 

#CFLAGS= -std=gnu99 -O2 -I../  -DUSE_TCMALLOC=1 -DUSE_INT=1 
CFLAGS= -std=gnu99 -O0 -g  -I../  -DUSE_TCMALLOC=1  -DUSE_INT=1 
CLIBS=  -lpthread 

all: test_kv.o
	${CXX}  -o test_kv test_kv.o  ${LIBS} ${CLIBS}

test_kv.o: test_kv.c
	${CC} ${CFLAGS} -c test_kv.c 

clean:
	rm -f *.o
	rm -f test_kv

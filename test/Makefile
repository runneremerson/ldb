CC=gcc
CXX=g++

LIBS = ../libldb.a ../deps/leveldb-1.18/libleveldb-ldb.a /usr/local/lib/libtcmalloc_minimal.a 

CFLAGS= -std=gnu99 -O0 -g  -I../  -DUSE_TCMALLOC=1  -DUSE_INT=1 
CLIBS=  -lpthread 

all: test_string.o test_hash.o test_zset.o
	${CXX}  -o test_string test_string.o  ${LIBS} ${CLIBS}
	${CXX}  -o test_hash   test_hash.o    ${LIBS} ${CLIBS}
	${CXX}  -o test_zset   test_zset.o    ${LIBS} ${CLIBS}


test_string.o: test_string.c
	${CC} ${CFLAGS} -c test_string.c 

test_hash.o: test_hash.c
	${CC} ${CFLAGS} -c test_hash.c 

test_zset.o: test_zset.c
	${CC} ${CFLAGS} -c test_zset.c 

clean:
	rm -rf *.o
	rm -rf test_string
	rm -rf test_hash
	rm -rf test_zset

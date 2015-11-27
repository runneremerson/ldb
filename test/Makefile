include ../build_config.mk

LIBS = ../libldb.a 

CFLAGS+= -std=gnu99 -I../  -DUSE_JEMALLOC=1  -DUSE_INT=1 

all: test_string.o test_hash.o test_zset.o test_set.o
	${CXX}  -o test_string test_string.o  ${LIBS} ${CLIBS}
	${CXX}  -o test_hash   test_hash.o    ${LIBS} ${CLIBS}
	${CXX}  -o test_zset   test_zset.o    ${LIBS} ${CLIBS}
	${CXX}  -o test_set    test_set.o     ${LIBS} ${CLIBS}


test_string.o: test_string.c
	${CC} ${CFLAGS} -c test_string.c 

test_hash.o: test_hash.c
	${CC} ${CFLAGS} -c test_hash.c 

test_zset.o: test_zset.c
	${CC} ${CFLAGS} -c test_zset.c 

test_set.o: test_set.c
	${CC} ${CFLAGS} -c test_set.c

clean:
	rm -rf *.o
	rm -rf test_string
	rm -rf test_hash
	rm -rf test_zset
	rm -rf test_set

MAKE = make

all:
	cd src/; ${MAKE}
	cd benchmark/; ${MAKE}
	cd test/; ${MAKE}

clean:
	rm -f *.a  ldb_bench
	cd src/; ${MAKE} clean
	cd benchmark/; ${MAKE} clean
	cd test/; ${MAKE} clean


MAKE = make

all:
	cd src/; ${MAKE}
	cd benchmark/; ${MAKE}

clean:
	rm -f *.a  ldb_bench
	cd src/; ${MAKE} clean
	cd benchmark/; ${MAKE} clean

$(shell sh build.sh 1>&2)
include build_config.mk



all:
	chmod u+x "${LEVELDB_PATH}/build_detect_platform"
	cd "${LEVELDB_PATH}"; ${MAKE}
	cd storage/; ${MAKE}
	cd test/; ${MAKE}

clean:
	rm -f *.a  ldb_bench org_bench
	cd storage/; ${MAKE} clean
	cd benchmark/; ${MAKE} clean
	cd test/; ${MAKE} clean

clean_all: clean
	cd "${LEVELDB_PATH}"; ${MAKE} clean
	rm -f ${JEMALLOC_PATH}/Makefile
	cd "${SNAPPY_PATH}"; ${MAKE} clean
	rm -f ${SNAPPY_PATH}/Makefile

#!/bin/sh
BASE_DIR=`pwd`
JEMALLOC_PATH="$BASE_DIR/deps/jemalloc-3.3.1"
LEVELDB_PATH="$BASE_DIR/deps/leveldb-1.18"
SNAPPY_PATH="$BASE_DIR/deps/snappy-1.1.1"

if test -z "$TARGET_OS"; then
	TARGET_OS=`uname -s`
fi
if test -z "$MAKE"; then
	MAKE=make
fi
if test -z "$CC"; then
	CC=gcc
fi
if test -z "$CXX"; then
	CXX=g++
fi

case "$TARGET_OS" in
    Darwin)
        #PLATFORM_CLIBS="-pthread"
		#PLATFORM_CFLAGS=""
        ;;
    Linux)
        PLATFORM_CLIBS="-pthread"
        ;;
    OS_ANDROID_CROSSCOMPILE)
        PLATFORM_CLIBS="-pthread"
        SNAPPY_HOST="--host=i386-linux"
        ;;
    CYGWIN_*)
        PLATFORM_CLIBS="-lpthread"
        ;;
    SunOS)
        PLATFORM_CLIBS="-lpthread -lrt"
        ;;
    FreeBSD)
        PLATFORM_CLIBS="-lpthread"
		MAKE=gmake
        ;;
    NetBSD)
        PLATFORM_CLIBS="-lpthread -lgcc_s"
        ;;
    OpenBSD)
        PLATFORM_CLIBS="-pthread"
        ;;
    DragonFly)
        PLATFORM_CLIBS="-lpthread"
        ;;
    HP-UX)
        PLATFORM_CLIBS="-pthread"
        ;;
    *)
        echo "Unknown platform!" >&2
        exit 1
esac


DIR=`pwd`
cd $SNAPPY_PATH
if [ ! -f Makefile ]; then
	echo ""
	echo "##### building snappy... #####"
	./configure $SNAPPY_HOST
	# FUCK! snappy compilation doesn't work on some linux!
	find . | xargs touch
	make
	echo "##### building snappy finished #####"
	echo ""
fi
cd "$DIR"


case "$TARGET_OS" in
	CYGWIN*|FreeBSD|OS_ANDROID_CROSSCOMPILE)
		echo "not using jemalloc on $TARGET_OS"
	;;
	*)
		DIR=`pwd`
		cd $JEMALLOC_PATH
		if [ ! -f Makefile ]; then
			echo ""
			echo "##### building jemalloc... #####"
			./configure --with-jemalloc-prefix=je_
			make
			echo "##### building jemalloc finished #####"
			echo ""
		fi
		cd "$DIR"
	;;
esac


rm -f ldb/version.h
echo "#ifndef LDB_DEPS_H" >> ldb/version.h
echo "#define LDB_DEPS_H" >> ldb/version.h
echo "#ifndef LDB_VERSION" >> ldb/version.h
echo "#define LDB_VERSION \"`cat version`\"" >> ldb/version.h
echo "#endif" >> ldb/version.h
echo "#endif" >> ldb/version.h

rm -f build_config.mk
echo CC=$CC >> build_config.mk
echo CXX=$CXX >> build_config.mk
echo "MAKE=$MAKE" >> build_config.mk
echo "LEVELDB_PATH=$LEVELDB_PATH" >> build_config.mk
echo "JEMALLOC_PATH=$JEMALLOC_PATH" >> build_config.mk
echo "SNAPPY_PATH=$SNAPPY_PATH" >> build_config.mk

echo "CFLAGS=" >> build_config.mk
#echo "CFLAGS = -DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare" >> build_config.mk
echo "CFLAGS = -Wall -O0 -g -Wno-sign-compare" >> build_config.mk
echo "CFLAGS += ${PLATFORM_CFLAGS}" >> build_config.mk
echo "CFLAGS += -I $LEVELDB_PATH/include" >> build_config.mk

echo "CLIBS=" >> build_config.mk
echo "CLIBS += ${PLATFORM_CLIBS}" >> build_config.mk
echo "CLIBS += $LEVELDB_PATH/libleveldb.a" >> build_config.mk
echo "CLIBS += $SNAPPY_PATH/.libs/libsnappy.a" >> build_config.mk


case "$TARGET_OS" in
	CYGWIN*|FreeBSD|OS_ANDROID_CROSSCOMPILE)
	;;
	*)
		echo "CLIBS += \"$JEMALLOC_PATH/lib/libjemalloc.a\"" >> build_config.mk
		echo "CFLAGS += -I \"$JEMALLOC_PATH/include\"" >> build_config.mk
	;;
esac


if test -z "$TMPDIR"; then
    TMPDIR=/tmp
fi

g++ -x c++ - -o $TMPDIR/ssdb_build_test.$$ 2>/dev/null <<EOF
	#include <unordered_map>
	int main() {}
EOF
if [ "$?" = 0 ]; then
	echo "CFLAGS += -DNEW_MAC" >> build_config.mk
fi


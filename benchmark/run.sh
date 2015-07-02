#!/bin/sh

make clean

cd  ../

make clean && make


cd benchmark/

make

./ldb_bench 10  100000

make clean

cd ../

make clean

#!/bin/sh

make clean

cd  ../

make clean && make

make clean

cd benchmark/

make

./ldb_bench

make clean

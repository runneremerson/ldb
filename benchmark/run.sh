#!/bin/sh


cd  ../

make clean && make


../ldb_bench 10  100000


make clean

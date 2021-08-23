#/bin/bash

workload="./workloads/$1.spec"
dbpath="../test_metadb"

rm -rf $dbpath/*

echo "load"
./ycsbc -db metadb -dbpath $dbpath -threads 1 -P $workload -load true -dboption 3

echo "run"
cgexec -g memory:mutant ./ycsbc -db metadb -dbpath $dbpath -threads 1 -P $workload -run true -dboption 3 -dbstatistics true


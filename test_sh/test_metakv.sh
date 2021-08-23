#/bin/bash

workload="./workloads/workloada.spec"
dbpath="/mnt/AEP1/metakv/TEST"


if [ -n "$dbpath" ];then
    rm -f $dbpath/*
fi
rm -rf /mnt/AEP1/metakv/TEST
./ycsbc -db metadb -dbpath $dbpath -threads 1 -P $workload -load true -dboption 0
echo "load"

# ./ycsbc -db metadb -dbpath $dbpath -threads 1 -P $workload -run true -dboption 0
# echo "run"

rm ../test-metadb 
./ycsbc -db metadb -dbpath ../test-metadb -threads 1 -P workloads/workloadb.spec -load true -run true -dbstatistics true
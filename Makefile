
# Rocksdb的头文件
#ROCKSDB_INCLUDE=/home/ubuntu/zyh/Cloud/rocksCloud/include
# RocksDB 的静态链接库
#ROCKSDB_LIBRARY=/home/ubuntu/zyh/Cloud/rocksCloud/build/librocksdb.a 
#ROCKSDB_LIB=/home/ubuntu/zyh/Cloud/rocksCloud/build/

METADB_INCLUDE=/home/minxinhao/c++/metakv_0522/include
METADB_LIBRARY=/home/minxinhao/c++/metakv_0522/libmetadb.a
# METADB_LIB=/home/minxinhao/c++/metakv_0522/build/

CC=g++
CFLAGS=-std=c++11 -g -mcx16 -Wall -I./ -I$(METADB_INCLUDE)
LDFLAGS= -lpthread -lz -lbz2 -llz4 -ldl -lsnappy -lpmem -lpmemobj -lnuma -lzstd -lhdr_histogram -lboost_regex -lboost_iostreams -latomic
SUBDIRS= core db 
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	#$(MAKE) -C $@
	$(MAKE) -C $@ METADB_INCLUDE=${METADB_INCLUDE} METADB_LIBRARY=${METADB_LIBRARY}

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^  $(METADB_LIBRARY) -o $@ $(LDFLAGS)

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)


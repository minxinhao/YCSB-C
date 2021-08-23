//
// 
//

#ifndef YCSB_C_MetaDB_DB_H
#define YCSB_C_MetaDB_DB_H

#include "core/db.h"
#include <iostream>
#include <string>
#include "core/properties.h"
#include <MetaDb.h>
#include <sys/time.h>
#include <hdr/hdr_histogram.h>

using std::cout;
using std::endl;

namespace ycsbc {
    class MetaDB : public DB{
    public :
        struct hdr_histogram* hdr_ = NULL;
        struct hdr_histogram* hdr_last_1s_ = NULL;
        struct hdr_histogram* hdr_get_= NULL;
        struct hdr_histogram* hdr_put_= NULL;
        struct hdr_histogram* hdr_update_ = NULL;
        struct hdr_histogram* hdr_scan_ = NULL;
        struct hdr_histogram* hdr_delete_ = NULL;
        struct hdr_histogram* hdr_rmw_ = NULL;

        void latency_hiccup(uint64_t iops);
        std::FILE* f_hdr_output_;
        std::FILE* f_hdr_hiccup_output_;

        MetaDB(const char *dbfilename, utils::Properties &props);

        ~MetaDB();

        // Read for GetFileInode
        int Read(uint64_t pinode,const std::string& fname,uint64_t* inode);

        // Scan for ReadDir
        int Scan(uint64_t pinode, std::vector<std::string>&fnames, std::vector<uint64_t> inodes);

        // Insert for file-inode
        int Insert(uint64_t pinode, const std::string &fname,uint64_t inode);

        int Update(uint64_t pinode, const std::string &fname,uint64_t inode);

        int Delete(uint64_t pinode , const std::string& fname);

        void RecordTime(int op,uint64_t tx_xtime);

        void PrintStats();

        // bool HaveBalancedDistribution();

        uint64_t get_now_micros(){
            struct timeval tv;
            gettimeofday(&tv, NULL);
            return (tv.tv_sec) * 1000000 + tv.tv_usec;
        }


    private:
        struct MetaDb db_;
        unsigned noResult;

        void SetOptions(DBOption *options, utils::Properties &props);
        void SerializeValues(std::vector<KVPair> &kvs, std::string &value);
        void DeSerializeValues(std::string &value, std::vector<KVPair> &kvs);

    };
}


#endif //YCSB_C_MetaDB_DB_H

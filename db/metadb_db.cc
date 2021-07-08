//
// Created by wujy on 1/23/19.
//
#include <iostream>


#include "metadb_db.h"
#include "lib/coding.h"


//#define MUTANT
#define PCACHE

using namespace std;

namespace ycsbc {

    void MetaDB::latency_hiccup(uint64_t iops) {
        fprintf(f_hdr_hiccup_output_, "%-11.2lf %-8ld %-8ld %-8ld %-8ld\n",
              hdr_mean((const hdr_histogram*)hdr_last_1s_),
              hdr_value_at_percentile(hdr_last_1s_, 95),
              hdr_value_at_percentile(hdr_last_1s_, 99),
              hdr_value_at_percentile(hdr_last_1s_, 99.99),
			  iops);
        hdr_reset(hdr_last_1s_);
        fflush(f_hdr_hiccup_output_);
    }

    MetaDB::MetaDB(const char *dbfilename, utils::Properties &props) :noResult(0){
        int r = hdr_init(1,INT64_C(3600000000),3,&hdr_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_last_1s_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_get_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_put_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_update_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_scan_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_rmw_);

        if((0 != r) || (NULL == hdr_) || (NULL == hdr_last_1s_) 
		    || (NULL == hdr_get_) || (NULL == hdr_put_)
		    || (NULL == hdr_scan_) || (NULL == hdr_rmw_) 
		    || (NULL == hdr_update_) || (23552 < hdr_->counts_len)) {
            cout << "DEBUG- init hdrhistogram failed." << endl;
            cout << "DEBUG- r=" << r << endl;
            cout << "DEBUG- histogram=" << &hdr_ << endl;
            cout << "DEBUG- counts_len=" << hdr_->counts_len << endl;
            cout << "DEBUG- counts:" << hdr_->counts << ", total_c:" << hdr_->total_count << endl;
            cout << "DEBUG- lowest:" << hdr_->lowest_discernible_value << ", max:" <<hdr_->highest_trackable_value << endl;
            free(hdr_);
            exit(0);
        }
        
        f_hdr_output_= std::fopen("../metadb-lat.hgrm", "w+");
        if(!f_hdr_output_) {
            std::perror("hdr output file opening failed");
            exit(0);
        }
	
        f_hdr_hiccup_output_ = std::fopen("../metadb-lat.hiccup", "w+");	
        if(!f_hdr_hiccup_output_) {
            std::perror("hdr hiccup output file opening failed");
            exit(0);
        }   
    	fprintf(f_hdr_hiccup_output_, "#mean       95th    99th    99.99th    IOPS\n");

        struct DBOption db_option;
        SetDefaultDBop(&db_option);
        printf("%s:%s\n",__func__,dbfilename);
        Status sts = DBOpen(&db_option,dbfilename,&db_);
        if(sts != OK){
            cout<<"Can't open rocksdb "<<dbfilename<<" with returned status "<<sts<<endl;
            exit(0);
        }
    }

    MetaDB::~MetaDB() {
        DBExit(&db_);
        printf("~MetaDB\n");
    }
    
    void SetOptions(DBOption *options, utils::Properties &props){
        
    }

    void MetaDB::PrintStats() {
        cout<<"read not found:"<<noResult<<endl;
 
        cout << "-----------------------------------------------------" << endl;
        cout << "SUMMARY latency (us) of this run with HDR measurement" << endl;
        cout << "         ALL      GET      PUT      UPD      SCAN    RMW" << endl;
        fprintf(stdout, "mean     %-8.3lf %-8.3lf %-8.3lf %-8.3lf %8.3lf %8.3lf\n",
            hdr_mean(hdr_),
            hdr_mean(hdr_get_),
            hdr_mean(hdr_put_),
            hdr_mean(hdr_update_),
            hdr_mean(hdr_scan_),
            hdr_mean(hdr_rmw_));
            
        fprintf(stdout, "95th     %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
            hdr_value_at_percentile(hdr_, 95),
                hdr_value_at_percentile(hdr_get_, 95),
            hdr_value_at_percentile(hdr_put_, 95),
            hdr_value_at_percentile(hdr_update_, 95),
            hdr_value_at_percentile(hdr_scan_, 95),
            hdr_value_at_percentile(hdr_rmw_, 95));
            fprintf(stdout, "99th     %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
                    hdr_value_at_percentile(hdr_, 99),
                    hdr_value_at_percentile(hdr_get_, 99),
                    hdr_value_at_percentile(hdr_put_, 99),
                    hdr_value_at_percentile(hdr_update_, 99),
            hdr_value_at_percentile(hdr_scan_, 99),
            hdr_value_at_percentile(hdr_rmw_, 99));
            fprintf(stdout, "99.99th  %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
                    hdr_value_at_percentile(hdr_, 99.99),
                    hdr_value_at_percentile(hdr_get_, 99.99),
                    hdr_value_at_percentile(hdr_put_, 99.99),
                    hdr_value_at_percentile(hdr_update_, 99.99),
                    hdr_value_at_percentile(hdr_scan_, 99.99),
                    hdr_value_at_percentile(hdr_rmw_, 99.99));

        
        int ret = hdr_percentiles_print(hdr_,f_hdr_output_,5,1.0,CLASSIC);
        if( 0 != ret ){
            cout << "hdr percentile output print file error!" <<endl;
        }
        cout << "-------------------------------" << endl;
    }


    int MetaDB::Read(uint64_t pinode,std::string& fname,uint64_t* inode){
        Status sts = GetFileInode(&db_,pinode , (char*)fname.c_str(), inode);

        if(sts == OK) {
            return DB::kOK;
        }
        if(sts == KEY_NOT_EXIST){
            noResult++;
            return DB::kOK;
        }else{
            cout <<"MetaDB GET() ERROR! error  "<< sts << endl;
            exit(0);
        }
    }


    int MetaDB::Scan(uint64_t pinode, std::vector<std::string>& inodes){
        struct hashmap* res = hashmap_new(sizeof(struct file_entry), 0, 0, 0, 
                                     file_entry_hash, file_entry_compare, NULL);
        Status sts = ReadDir(&db_,pinode,res);
        if(sts != OK) {
            cout <<"MetaDB GET() ERROR! error "<< sts << endl;
            exit(0);
        }
        return DB::kOK;
    }

    int MetaDB::Insert(uint64_t pinode, const std::string &fname,uint64_t inode){
        Status sts = InsertFileInode(&db_,pinode,(char*)fname.c_str(),inode);
        
        if(sts != OK){
            cout <<"MetaDB PUT() ERROR! error "<< sts << endl;
            exit(0);
        }
        return DB::kOK;
    }

    int MetaDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
    	// return Insert(table,key,values);
        return DB::kOK;
    }

    int MetaDB::Delete(const std::string &table, const std::string &key) {
        // MetaDB::Status s;
        // s = db_->Delete(MetaDB::WriteOptions(),key);
        // if(!s.ok()){
        //     cout <<"MetaDB DEL() ERROR! error string: "<< s.ToString() << endl;
	    // exit(0);
        // }
        return DB::kOK;
    }


    void MetaDB::RecordTime(int op,uint64_t tx_xtime){
        if(tx_xtime > 3600000000) {
            cout << "too large tx_xtime" << endl;
        }

        hdr_record_value(hdr_, tx_xtime);
        hdr_record_value(hdr_last_1s_, tx_xtime);

        if(op == 1){
            hdr_record_value(hdr_put_, tx_xtime);
        } else if(op == 2) {
            hdr_record_value(hdr_get_, tx_xtime);
        } else if(op == 3) {
            hdr_record_value(hdr_update_, tx_xtime);
        } else if(op == 4) {
            hdr_record_value(hdr_scan_, tx_xtime);
        } else if(op == 5) {
            hdr_record_value(hdr_rmw_, tx_xtime);
        } else {
            cout << "record time err with op error" << endl;
        }
    }

    void MetaDB::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void MetaDB::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
        uint64_t offset = 0;
        uint64_t kv_num = 0;
        uint64_t key_size = 0;
        uint64_t value_size = 0;

        kv_num = DecodeFixed64(value.c_str());
        offset += 8;
        for( unsigned int i = 0; i < kv_num; i++){
            ycsbc::DB::KVPair pair;
            key_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.first.assign(value.c_str() + offset, key_size);
            offset += key_size;

            value_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.second.assign(value.c_str() + offset, value_size);
            offset += value_size;
            kvs.push_back(pair);
        }
    }
}

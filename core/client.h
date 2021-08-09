//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"
#include "utils.h"
#include <string>

extern uint64_t ops_cnt[ycsbc::Operation::READMODIFYWRITE + 1] ;    //操作个数
extern uint64_t ops_time[ycsbc::Operation::READMODIFYWRITE + 1] ;   //微秒

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) { }
  
  virtual bool DoInsert();
  virtual bool DoTransaction();
  
  virtual ~Client() { }
  
 protected:
  
  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();
  
  DB &db_;
  CoreWorkload &workload_;
};

inline bool Client::DoInsert() {
  uint64_t pinode = workload_.NextSequencePinode();
  printf("%s pinode:%lu\n",__func__,pinode);
  // uint64_t inode = workload_.NextSequenceInode();
  // std::string fname = workload_.NextSequenceKey();

  uint64_t inode;
  std::string fname;
  // define YSCB_BATCH 100
  for(uint64_t i = 0 ; i < 100; i++){
    inode = pinode + i ;
    fname = std::string("string") + std::to_string(inode);
    db_.Insert(pinode,fname,inode);
  }
  return true;
}

inline bool Client::DoTransaction() {
  int status = -1;
  uint64_t start_time = get_now_micros();
  uint64_t op_time;
  switch (workload_.NextOperation()) {
    case READ:
      status = TransactionRead();
      op_time = (get_now_micros() - start_time );
      ops_time[READ] += op_time;
      ops_cnt[READ]++;
      db_.RecordTime(2,op_time);
      break;
    case UPDATE:
      status = TransactionUpdate();
      op_time = (get_now_micros() - start_time );
      ops_time[UPDATE] += op_time;
      ops_cnt[UPDATE]++;
      db_.RecordTime(3,op_time);
      break;
    case INSERT:
      status = TransactionInsert();
      op_time = (get_now_micros() - start_time );
      ops_time[INSERT] += op_time;
      ops_cnt[INSERT]++;
      db_.RecordTime(1,op_time);
      break;
    case SCAN:
      status = TransactionScan();
      op_time = (get_now_micros() - start_time );
      ops_time[SCAN] += op_time;
      ops_cnt[SCAN]++;
      db_.RecordTime(4,op_time);
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      op_time = (get_now_micros() - start_time );
      ops_time[READMODIFYWRITE] += op_time;
      ops_cnt[READMODIFYWRITE]++;
      db_.RecordTime(5,op_time);
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  uint64_t pinode = workload_.NextTransactionPinode();
  std::string fname = workload_.NextTransactionKey();
  uint64_t inode;
  // printf("%s pinode:%lu\n",__func__,pinode);
  for(uint64_t i = 0 ; i < 100; i++){
    inode = pinode + i ;
    fname = std::string("string") + std::to_string(inode);
    db_.Insert(pinode,fname,inode);
  }
  return db_.Read(pinode,fname,&inode);
}

inline int Client::TransactionReadModifyWrite() {
  // const std::string &table = workload_.NextTable();
  // const std::string &key = workload_.NextTransactionKey();
  // std::vector<DB::KVPair> result;

  // if (!workload_.read_all_fields()) {
  //   std::vector<std::string> fields;
  //   fields.push_back("field" + workload_.NextFieldName());
  //   db_.Read(table, key, &fields, result);
  // } else {
  //   db_.Read(table, key, NULL, result);00
  // }

  // std::vector<DB::KVPair> values;
  // if (workload_.write_all_fields()) {
  //   workload_.BuildValues(values);
  // } else {
  //   workload_.BuildUpdate(values);
  // }
  // return db_.Update(table, key, values);
  return DB::kOK;
}

inline int Client::TransactionScan() {
  uint64_t pinode = workload_.NextSequencePinode();
  std::vector<std::string> res;
  return db_.Scan(pinode,res);
  
}

inline int Client::TransactionUpdate() {
  int64_t pinode = workload_.NextTransactionPinode();
  std::string fname;
  uint64_t inode;
  for(uint64_t i = 0 ; i < 100; i++){
    inode = pinode + i ;
    fname = std::string("string") + std::to_string(inode);
    db_.Insert(pinode,fname,inode);
  }
  return DB::kOK;
}

inline int Client::TransactionInsert() {
  uint64_t pinode = workload_.NextSequencePinode();
  std::string fname;
  uint64_t inode;
  for(uint64_t i = 0 ; i < 100; i++){
    inode = pinode + i ;
    fname = std::string("string") + std::to_string(inode);
    db_.Insert(pinode,fname,inode);
  }
  return DB::kOK;
} 

} // ycsbc

#endif // YCSB_C_CLIENT_H_

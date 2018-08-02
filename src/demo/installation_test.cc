//
// Created by Zeyuan_Hu on 4/16/18.
//
#include <assert.h>
#include <string.h>
#include <pebblesdb/db.h>
#include <iostream>

int main(){
  leveldb::DB* db;
  leveldb::Options options;
  // If true, the database will be created if it is missing.
  // Default: false
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options,"/tmp/testdb", &db);
  assert(status.ok());

  //write key1,value1
  std::string key="key";
  std::string value = "value";

  status = db->Put(leveldb::WriteOptions(), key,value);
  assert(status.ok());

  status = db->Get(leveldb::ReadOptions(), key, &value);
  assert(status.ok());
  std::cout<<value<<std::endl;
  std::string key2 = "key2";

  //move the value under key to key2
  status = db->Put(leveldb::WriteOptions(),key2,value);
  assert(status.ok());
  status = db->Delete(leveldb::WriteOptions(), key);

  assert(status.ok());

  status = db->Get(leveldb::ReadOptions(),key2, &value);

  assert(status.ok());
  std::cout<<key2<<"=="<<value<<std::endl;

  status = db->Get(leveldb::ReadOptions(),key, &value);

  if(!status.ok())
    std::cerr<<key<<"  "<<status.ToString()<<std::endl;
  else
    std::cout<<key<<"="<<value<<std::endl;

  std::string property;
  db->GetProperty("leveldb.stats", &property);
  std::cout << property <<std::endl;

  delete db;
  return 0;
}

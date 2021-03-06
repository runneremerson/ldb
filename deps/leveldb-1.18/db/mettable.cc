// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/mettable.h"
#include "util/mutexlock.h"
#include "util/crc32c.h"
#include "leveldb/slice.h"
#include <utility>
#include <assert.h>



namespace leveldb {

const int kNumKeyBuckets = 1024;


MetTable::MetTable()
    : refs_(0) {
    for(int i=0; i<kNumKeyBuckets; ++i){
        buckets_.push_back(new KeyBucket(i));
    }
}

MetTable::~MetTable() {
    assert(refs_ == 0);
    for( int i=0; i<kNumKeyBuckets; ++i){
        delete buckets_[i];
    }
}


bool MetTable::Insert(uint32_t value, const Slice& key, uint64_t version){
    buckets_[value%kNumKeyBuckets]->Insert(std::string(key.data(), key.size()), version);
}

bool MetTable::Remove(uint32_t value, const Slice& key, uint64_t version){
    return buckets_[value%kNumKeyBuckets]->Remove(std::string(key.data(), key.size()), version);
}

bool MetTable::Query(uint32_t value, const Slice& key, uint64_t* version){
    return buckets_[value%kNumKeyBuckets]->Query(std::string(key.data(), key.size()), version);
}


size_t MetTable::ApproximateMemoryUsage(){
    size_t ret=0;
    for(int i=0; i<kNumKeyBuckets; ++i){
        ret += buckets_[i]->ApproximateMemoryUsage();
    }
    return ret; 
}

MetTable::KeyBucket::KeyBucket(const int id)
    :id_(id),memory_usage_(0) {
}



bool MetTable::KeyBucket::Insert(const std::string& key, uint64_t version){
    MutexLock l(&mutex_);
    std::tr1::unordered_map<std::string, uint64_t>::iterator key_iter=keys_.find(key);
    if(key_iter!=keys_.end() && key_iter->second <version){
        key_iter->second=version;
        return true;
    }else if(key_iter==keys_.end()){
        keys_.insert(std::make_pair(key, version));
        memory_usage_ += (key.size() + sizeof(uint64_t));
        return true;
    }
    return false;
}

bool MetTable::KeyBucket::Remove(const std::string& key, uint64_t version){
    MutexLock l(&mutex_);
    std::tr1::unordered_map<std::string, uint64_t>::iterator key_iter=keys_.find(key);
    if(key_iter!=keys_.end()){
        //printf("%s old=%lu, new=%lu\n", __func__, key_iter->second, version);
        if(key_iter->second < version){
            keys_.erase(key_iter);
            memory_usage_ -= (key.size() + sizeof(uint64_t));
            return true;
        }
    }else if(key_iter==keys_.end()){
        return true;
    }
    return false;
}

bool MetTable::KeyBucket::Query(const std::string& key, uint64_t* version){
  MutexLock l(&mutex_);
  std::tr1::unordered_map<std::string, uint64_t>::iterator iter=keys_.find(key);
  if(iter!=keys_.end()){
    (*version) = iter->second;
    return true;
  }
  return false;
}

size_t MetTable::KeyBucket::ApproximateMemoryUsage(){
  MutexLock l(&mutex_);
  return memory_usage_;
}


}  // namespace leveldb

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_METTABLE_H_
#define STORAGE_LEVELDB_DB_METTABLE_H_

#include <assert.h>
#include <stdint.h>
#include <string>
#include <vector>
#include <tr1/unordered_map>
#include "port/port.h"

namespace leveldb {

class Slice;


class MetTable {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  MetTable();

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  void Insert(uint32_t value, const Slice& key, uint64_t version);
  bool Remove(uint32_t value, const Slice& key, uint64_t version);
  bool Query(uint32_t value, const Slice& key, uint64_t* version);

  // Returns an estimate of the number of bytes of data in use by this
  // data structure.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  size_t ApproximateMemoryUsage();


 private:
  ~MetTable();  // Private since only Unref() should be used to delete it

  struct KeyBucket{
    KeyBucket();
    void Insert(const std::string& key, uint64_t version);
    bool Remove(const std::string& key, uint64_t version);
    bool Query(const std::string& key, uint64_t* version);
    size_t ApproximateMemoryUsage();
    std::tr1::unordered_map<std::string, uint64_t> keys_;
    std::tr1::unordered_map<std::string, uint64_t> dels_;
    port::Mutex mutex_;
    size_t memory_usage_;
  };
  typedef std::vector<KeyBucket*>   Buckets;
  
  Buckets buckets_;
  
  int refs_;

  // No copying allowed
  MetTable(const MetTable&);
  void operator=(const MetTable&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_METTABLE_H_

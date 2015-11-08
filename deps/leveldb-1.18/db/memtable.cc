// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/mettable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "port/port.h"
#include <time.h>

namespace leveldb {

const int kNumKeyMutexs = 64;

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(MetTable* met, const InternalKeyComparator& cmp)
    : met_(met),
      comparator_(cmp),
      refs_(0),
      table_(comparator_, &arena_) {
  if(met_!=NULL){
    met_->Ref();
  }
  for(int i=0; i<kNumKeyMutexs; ++i){
      mutexs_.push_back(new port::Mutex);
  }
}

MemTable::~MemTable() {
  assert(refs_ == 0);
  if(met_!=NULL){
      met_->Unref();
  }
  for(int i=0; i<kNumKeyMutexs; ++i){
      delete mutexs_[i];
  }
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator: public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()- sizeof(uint32_t) - 3*sizeof(uint64_t)
  //  key bytes    : char[internal_key.size() - sizeof(uint32_t) - 3*sizeof(uint64_t)]
  //  value_size   : varint32 of value.size() + sizeof(uint8_t) + sizeof(uint64_t) (maybe + sizeof(uint64_t) depends on type)
  //  value bytes  : char[value.size()+sizeof(uint8_t)+sizeof(uint64_t) maybe + sizeof(uint64_t)], include fields-- type and currversion
  ValueType val_type = type;
  if(type == kTypeValue ){

      size_t key_size = key.size();
      size_t val_size = value.size() + 1 + 8;
      size_t mat_size = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
      assert(key_size > mat_size);

      //decode meta data
      uint32_t versioncare = DecodeFixed32(key.data());
      uint64_t lastversion = DecodeFixed64(key.data() + sizeof(uint32_t));
      uint64_t nextversion = DecodeFixed64(key.data() + sizeof(uint32_t) + sizeof(uint64_t));
      uint64_t expiration = DecodeFixed64(key.data() + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t));
      if(expiration > 0){
        val_type = ValueType(val_type | kTypeExpiration);
        val_size += 8;
      }

      //encode key
      size_t internal_key_size = key_size - mat_size + 8;
      const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
      char* buf = arena_.Allocate(encoded_len);
      char* p = EncodeVarint32(buf, internal_key_size);
      memcpy(p, (key.data()+mat_size), (key_size-mat_size));
      p += (key_size-mat_size);
      EncodeFixed64(p, (s << 8) | type);
      p += 8;

      //encode value
      p = EncodeVarint32(p, val_size);
      EncodeFixed8(p, val_type);
      p += 1;
      EncodeFixed64(p, nextversion);
      p += 8;
      if(val_type & kTypeExpiration){
        EncodeFixed64(p, expiration);
        p += 8;
      }
      memcpy(p, value.data(), value.size());
      assert((p + value.size()) - buf == encoded_len);

      uint64_t currversion = 0;
      if(met_!=NULL && nextversion>0){
          Slice mat_key(key.data()+mat_size, key.size()-mat_size);
          uint32_t crc32value = crc32c::Value(mat_key.data(), mat_key.size());
          mutexs_[crc32value%kNumKeyMutexs]->Lock();
          if(versioncare & 0x00000001){
              if(met_->Query(crc32value, mat_key, &currversion)){
                  if(currversion == lastversion){
                      if(met_->Insert(crc32value, mat_key, nextversion)){
                        table_.Insert(buf);
                      }
                  }
              }
          }else{
              if(!met_->Insert(crc32value, mat_key, nextversion)){
                  table_.Insert(buf);
              }
          }
          mutexs_[crc32value%kNumKeyMutexs]->Unlock();
      }else{
          table_.Insert(buf);
      }
  }else if(type == kTypeDeletion){
      size_t key_size = key.size();
      size_t val_size = value.size() + 1 + 8; //+ type + nextversion
      size_t mat_size = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
      assert(key_size > mat_size);

      //decode meta data
      uint32_t versioncare = DecodeFixed32(key.data());
      uint64_t lastversion = DecodeFixed64(key.data() + sizeof(uint32_t));
      uint64_t nextversion = DecodeFixed64(key.data() + sizeof(uint32_t) + sizeof(uint64_t));
      if(nextversion >0 && (0x00000002 & versioncare)){
        type = kTypeValue;
        val_type = ValueType(type | kTypeLater);
      }

      //encode key
      size_t internal_key_size = key_size - mat_size + 8;
      const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
      char* buf = arena_.Allocate(encoded_len);
      char* p = EncodeVarint32(buf, internal_key_size);
      memcpy(p, (key.data()+mat_size), (key_size-mat_size));
      p += (key_size - mat_size); 
      EncodeFixed64(p, (s << 8) | type);
      p += 8;
      //encode value
      p = EncodeVarint32(p, val_size);
      EncodeFixed8(p , val_type);
      p += 1;
      EncodeFixed64(p, nextversion);
      p += 8;
      memcpy(p, value.data(), value.size());
      assert((p + value.size()) - buf == encoded_len);
      if(met_ !=NULL && type == kTypeValue){
          Slice mat_key(key.data()+mat_size, key.size()-mat_size);
          uint32_t crc32value = crc32c::Value(mat_key.data(), mat_key.size());
          mutexs_[crc32value%kNumKeyMutexs]->Lock();
          if(type == kTypeValue){
            if(met_->Insert(crc32value, mat_key, nextversion)){
              table_.Insert(buf);
            }
          }else if(type == kTypeDeletion ){
            if(met_->Remove(crc32value, mat_key, nextversion)){ 
              table_.Insert(buf);
            }
          }
          mutexs_[crc32value%kNumKeyMutexs]->Unlock();
      }else {
          table_.Insert(buf);
      }
  }
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb

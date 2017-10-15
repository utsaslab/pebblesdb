// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
//    kTypeGuard varstring varint32 
// varstring :=
//    len: varint32
//    data: uint8[len]

#define __STDC_LIMIT_MACROS

#include "pebblesdb/write_batch.h"

#include "pebblesdb/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"

#include "db/murmurhash3.h"
#include "db/version_edit.h"
#include "db/version_set.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch()
  : rep_() {
  Clear();
}

WriteBatch::~WriteBatch() { }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

int WriteBatch::Count() const {
  return WriteBatchInternal::Count(this);
}

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  uint32_t level;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
    case kTypeGuard:
	if (GetLengthPrefixedSlice(&input, &key) &&
	    GetVarint32(&input, &level)) {
	  handler->HandleGuard(key, level);
	} else {
	return Status::Corruption("bad WriteBatch Guard");
	}
	break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::PutGuard(const Slice& key, int level) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeGuard));
  PutLengthPrefixedSlice(&rep_, key);
  PutVarint32(&rep_, level);
}
  
void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

/* 
vijayc: Changing memtable inserter so that it inserts guards into a
version in addition to adding keys to the memtable.
*/
namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  MemTableInserter()
    : sequence_(),
      mem_() {
    version_ = NULL;
  }
  SequenceNumber sequence_;
  MemTable* mem_;
  Version* version_;

  virtual void Put(const Slice& key, const Slice& value) {
//	ParsedInternalKey parsed_key;
//	ParseInternalKey(key, &parsed_key);
//	printf("Adding key %s to memtable of size: %d ", parsed_key.user_key.data(), mem_->num_entries);
    mem_->Add(sequence_, kTypeValue, key, value);
//    printf("size after adding: %d\n", mem_->num_entries);
    sequence_++;
  }
  virtual void Delete(const Slice& key) {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
  virtual void HandleGuard(const Slice& key, unsigned level) {
    /* Return harmlessly if no version to insert into. */
    if (!version_) return;
    assert(level < config::kNumLevels);
    GuardMetaData* g = new GuardMetaData();
    InternalKey ikey(key, sequence_, kTypeValue);
    g->guard_key = ikey;
    g->level = level;
    g->number_segments = 0;
    g->refs = 1;
    version_->AddToCompleteGuards(g, level);
//    version_->AddToNewGuards(g, level);
    sequence_++;
  }
 private:
  MemTableInserter(const MemTableInserter&);
  MemTableInserter& operator = (const MemTableInserter&);
};

/* 
vijayc: class to iterate over WriteBatch and check if a key should be
a guard. If so, do two things: 
- add to the string representation (this will later be appended to the
WriteBatch contents.
- insert it into the Versions structure. 
*/
 class GuardInserter : public WriteBatch::Handler {
 public:
   GuardInserter() : sequence_(), bit_mask(0) {
     new_batch = NULL;
     for (int i = 0; i < config::kNumLevels; i++)
       num_guards[i] = 0;
   }
   WriteBatch* new_batch;
   SequenceNumber sequence_;

  virtual void Put(const Slice& key, const Slice& value) {
    // Need to hash and check the last few bits. 
    void* input = (void*) key.data();
    size_t size = key.size();
    const unsigned int murmur_seed = 42;
    unsigned int hash_result;
    MurmurHash3_x86_32(input, size, murmur_seed, &hash_result);

    // Go through each level, starting from the top and checking if it
    // is a guard on that level. 
    unsigned num_bits = top_level_bits;
    
    for (unsigned i = 0; i < config::kNumLevels; i++) {
      set_mask(num_bits);
      if ((hash_result & bit_mask) == bit_mask) {
		// found a guard
		// Insert the guard to this level and all the lower levels
		for (unsigned j = i; j < config::kNumLevels; j++) {
			new_batch->PutGuard(key, j);
			num_guards[j]++;
		}
		break;
      }
      // Check next level
      num_bits -= bit_decrement;
    }
    sequence_++;
  }

  virtual void Delete(const Slice& key) {
    sequence_++;
  }

  virtual void HandleGuard(const Slice& key, unsigned level) {
    /* vijayc: emptyHandleGuard. */
    assert(0);
  }
   
  void print_all_levels() {
    for (int i = 0; i < config::kNumLevels; i++)
      if (num_guards[i] > 0)
	printf("%d %d\n", i, num_guards[i]);
  }
   
 private:
   // Number of random bits to match in hash value for key to become
   // top level guard. Note that this has the least probability, will
   // increase as the levels become deeper.
   const static unsigned top_level_bits = 27;
   // Top level guard = 17 bits should match in guard
   // Next level guard = 15 bits
   // Next level guard = 13 bits and so on..
   const static int bit_decrement = 2;

   int num_guards[config::kNumLevels];
   
   unsigned bit_mask;
   
   void set_mask(unsigned num_bits) {
     assert(num_bits > 0 && num_bits < 32);
     bit_mask = (1 << num_bits) - 1;
   }
   
  GuardInserter(const GuardInserter&);
  GuardInserter& operator = (const GuardInserter&);
};
  
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b,
					MemTable* memtable) {
  /* The old MemTableInserter code. This doesn't take a version to
     insert guards into. Otherwise, it works exactly the same as the
     old MemTableInserter. */
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}
  
Status WriteBatchInternal::InsertIntoVersion(const WriteBatch* b,
                                      MemTable* memtable,
				      Version* version) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.version_ = version;
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

Status WriteBatchInternal::SetGuards(const WriteBatch* b,
				       WriteBatch* new_b) {
  // Determine the guards.
  GuardInserter g_inserter;
  g_inserter.sequence_ = WriteBatchInternal::Sequence(b);
  g_inserter.new_batch = new_b;
  Status s = b->Iterate(&g_inserter);
  return s;
}
  
void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb

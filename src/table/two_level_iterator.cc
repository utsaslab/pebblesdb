// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"
#include "pebblesdb/table.h"
#include "db/dbformat.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

typedef Iterator* (*BlockFunctionGuards)(void*, const void*, void*, unsigned, const ReadOptions&, const Slice&);

class TwoLevelIterator: public Iterator {
 public:
  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options);

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  virtual bool Valid() const {
    return data_iter_.Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }
  virtual const Status& status() const {
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != NULL && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  TwoLevelIterator(const TwoLevelIterator&);
  TwoLevelIterator& operator = (const TwoLevelIterator&);
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_; // May be NULL
  // If data_iter_ is non-NULL, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      status_(),
      index_iter_(index_iter),
      data_iter_(NULL),
      data_block_handle_() {
}

TwoLevelIterator::~TwoLevelIterator() {
}

void TwoLevelIterator::Seek(const Slice& target) {
//  uint64_t a, b, c, d, e;
//  pthread_t tid = Env::Default()->GetThreadId();
//  a = Env::Default()->NowMicros();
  index_iter_.Seek(target);
//  b = Env::Default()->NowMicros();
  InitDataBlock();
//  c = Env::Default()->NowMicros();
  if (data_iter_.iter() != NULL) data_iter_.Seek(target);
//  d = Env::Default()->NowMicros();
  SkipEmptyDataBlocksForward();
//  e = Env::Default()->NowMicros();
//  printf("TwoLevelIterator::Seek: Thread %lu: start: %llu, after index seek: %llu, after init data block: %llu, after data_iter seek: %llu, after skip empty blocks: %llu\n", tid, a, b, c, d, e);
//  printf("TwoLevelIterator::Seek: Thread %lu: index_iter_seek: %llu, init data block: %llu, data_iter seek: %llu, skip empty blocks: %llu\n", tid, b - a, c -b, d - c, e - d);
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}


void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != NULL) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(NULL);
  } else {
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != NULL && handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
//      uint64_t a, b;
//      pthread_t tid = Env::Default()->GetThreadId();
//      a = Env::Default()->NowMicros();
      Iterator* iter = (*block_function_)(arg_, options_, handle);
//      b = Env::Default()->NowMicros();
//      printf("InitDataBlock:: Thread %lu: before BlockReader: %llu after BlockReader: %llu diff: %llu\n", tid, a, b, b - a);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

// Two Level Iterator For Guards

class TwoLevelIteratorGuards: public Iterator {
 public:
  TwoLevelIteratorGuards(
    Iterator* index_iter,
    BlockFunctionGuards block_function,
    void* arg1,
	const void* arg2,
	void* arg3,
	unsigned level,
    const ReadOptions& options);

  virtual ~TwoLevelIteratorGuards();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  virtual bool Valid() const {
    return data_iter_.Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }
  virtual const Status& status() const {
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != NULL && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  TwoLevelIteratorGuards(const TwoLevelIteratorGuards&);
  TwoLevelIteratorGuards& operator = (const TwoLevelIteratorGuards&);
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunctionGuards block_function_;
  void* arg1_;
  const void* arg2_;
  void* arg3_;
  unsigned level;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_; // May be NULL
  // If data_iter_ is non-NULL, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
};

TwoLevelIteratorGuards::TwoLevelIteratorGuards(
    Iterator* index_iter,
    BlockFunctionGuards block_function,
    void* arg1,
	const void* arg2,
	void* arg3,
	unsigned l,
    const ReadOptions& options)
    : block_function_(block_function),
      arg1_(arg1),
	  arg2_(arg2),
	  arg3_(arg3),
	  level(l),
      options_(options),
      status_(),
      index_iter_(index_iter),
      data_iter_(NULL),
      data_block_handle_() {
}

TwoLevelIteratorGuards::~TwoLevelIteratorGuards() {
}

void TwoLevelIteratorGuards::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIteratorGuards::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != NULL) {
	  data_iter_.SeekToFirst();
  }
  SkipEmptyDataBlocksForward();
}

void TwoLevelIteratorGuards::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIteratorGuards::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIteratorGuards::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}


void TwoLevelIteratorGuards::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  }
}

void TwoLevelIteratorGuards::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  }
}

void TwoLevelIteratorGuards::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != NULL) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIteratorGuards::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(NULL);
  } else {
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != NULL && handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*block_function_)(arg1_, arg2_, arg3_, level, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}


}  // namespace

Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

/*Iterator* NewTwoLevelIteratorGuards(
    Iterator* index_iter,
    BlockFunctionGuards block_function,
    void* arg1,
	const void* arg2,
    const ReadOptions& options) {
  return new TwoLevelIteratorGuards(index_iter, block_function, arg1, arg2, options);
}*/

Iterator* NewTwoLevelIteratorGuards(
    Iterator* index_iter,
    BlockFunctionGuards block_function,
    void* arg1,
	const void* arg2,
	void* arg3,
	unsigned level,
    const ReadOptions& options) {
  return new TwoLevelIteratorGuards(index_iter, block_function, arg1, arg2, arg3, level, options);
}

}  // namespace leveldb

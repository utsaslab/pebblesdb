// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include "util/atomic.h"
#include "util/mutexlock.h"

namespace leveldb {

class Arena {
 public:
  Arena();
  ~Arena() throw ();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena (including space allocated but not yet used for user
  // allocations).
  uint64_t MemoryUsage() { return atomic::load_64_nobarrier(&memory_usage_); }

 private:
  struct Block;

  Block* NewBlock(size_t bytes);
  char* AllocateLarge(size_t bytes);
  char* AllocateFinalize(Block* b, size_t bytes);

  const size_t align_;
  const size_t page_size_;
  uint64_t memory_usage_;
  Block* blocks_;
  Block* large_;

  // No copying allowed
  Arena(const Arena&);
  void operator=(const Arena&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_

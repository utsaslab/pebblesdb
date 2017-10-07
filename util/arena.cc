// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"
#include <assert.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

#define BLOCK_SIZE 65536

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

namespace leveldb {

struct Arena::Block {
 public:
  Block()
    : next(NULL),
      rem(0),
      next_lower(NULL),
      next_upper(NULL),
      base(NULL),
      size(0) {
  }
  ~Block() throw () {
    uint32_t r = atomic::load_32_acquire(&rem);
    (void) r;
    if (base) {
      munmap(base, size);
    }
  }

  Block* next;
  uint32_t rem;
  char* next_lower;
  char* next_upper;
  char* base;
  uint32_t size;
 private:
  Block(const Block&);
  Block& operator = (const Block&);
};

Arena::Arena()
  : align_((sizeof(void*) > 8) ? sizeof(void*) : 8),
    page_size_(getpagesize()),
    memory_usage_(),
    blocks_(),
    large_() {
  using namespace atomic;
  assert((align_ & (align_-1)) == 0); // Pointer size should be a power of 2
  store_64_release(&memory_usage_, sizeof(Block));
  Block* nb = NewBlock(1U << 16);
  store_ptr_release(&blocks_, nb);
  store_ptr_nobarrier(&large_, static_cast<Block*>(NULL));
  increment_64_nobarrier(&memory_usage_, sizeof(Block));
}

Arena::~Arena() throw () {
  using namespace atomic;
  memory_barrier();
  Block* list = load_ptr_acquire(&blocks_);
  while (list) {
    Block* tmp = load_ptr_acquire(&list->next);
    delete list;
    list = tmp;
  }
  list = load_ptr_acquire(&large_);
  while (list) {
    Block* tmp = load_ptr_acquire(&list->next);
    delete list;
    list = tmp;
  }
}

char* Arena::Allocate(size_t bytes) {
  using namespace atomic;
  assert(bytes > 0);
  if (bytes > BLOCK_SIZE >> 2) {
    return AllocateLarge(bytes);
  }
  while (true) {
    Block* b = load_ptr_acquire(&blocks_);
    uint32_t rem = load_32_acquire(&b->rem);
    uint32_t witness;
    while (rem >= bytes) {
      witness = compare_and_swap_32_acquire(&b->rem, rem, rem - bytes);
      if (witness == rem) {
        return AllocateFinalize(b, bytes);
      }
      rem = witness;
    }
    Block* nb = NewBlock(BLOCK_SIZE);
    store_ptr_release(&nb->next, b);
    if (compare_and_swap_ptr_fullbarrier(&blocks_, b, nb) != b) {
      delete nb;
    } else {
      increment_64_nobarrier(&memory_usage_, sizeof(Block));
    }
  }
}

char* Arena::AllocateAligned(size_t bytes) {
  uint64_t new_bytes = (bytes + (align_ - 1)) & ~(align_ - 1);
  char* result = Allocate(new_bytes);
  assert((reinterpret_cast<uintptr_t>(result) & (align_ - 1)) == 0);
  return result;
}

Arena::Block* Arena::NewBlock(size_t bytes) {
  using namespace atomic;
  const size_t sz = (bytes + page_size_ - 1) & ~(page_size_ - 1);
  assert(sz / page_size_ * page_size_ == sz);
  Block* nb = new Block();
  void* ptr = mmap(NULL, sz, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED) {
    throw std::bad_alloc();
  }
  store_ptr_nobarrier(&nb->base, reinterpret_cast<char*>(ptr));
  store_ptr_nobarrier(&nb->next_lower, nb->base);
  store_ptr_nobarrier(&nb->next_upper, nb->base + sz);
  store_32_nobarrier(&nb->size, sz);
  store_32_release(&nb->rem, sz);
  return nb;
}

char* Arena::AllocateLarge(size_t bytes) {
  using namespace atomic;
  assert(bytes > 0);
  Block* nb = NewBlock(bytes);
  Block* b = load_ptr_acquire(&large_);
  Block* witness = NULL;
  store_ptr_release(&nb->next, b);
  while ((witness = compare_and_swap_ptr_fullbarrier(&large_, b, nb)) != b) {
    b = witness;
    store_ptr_release(&nb->next, b);
  }
  increment_64_nobarrier(&memory_usage_, sizeof(Block));
  return AllocateFinalize(nb, bytes);
}

char* Arena::AllocateFinalize(Block* b, size_t bytes) {
  using namespace atomic;
  char** p = NULL;
  ssize_t cas_diff = 0;
  ssize_t ret_diff = 0;
  uint64_t aligned = (bytes + (align_ - 1)) & ~(align_ - 1);
  if (bytes == aligned) {
    p = &b->next_upper;
    cas_diff = 0 - bytes;
    ret_diff = 0;
  } else {
    p = &b->next_lower;
    cas_diff = 0 + bytes;
    ret_diff = 0 - bytes;
  }
  char* expected = load_ptr_nobarrier(p);
  char* witness = NULL;
  while ((witness = compare_and_swap_ptr_nobarrier(p, expected, expected + cas_diff)) != expected) {
    expected = witness;
  }
  char* nl = load_ptr_nobarrier(&b->next_lower);
  char* nu = load_ptr_nobarrier(&b->next_upper);
  uint32_t x = load_32_nobarrier(&b->rem);
  (void) x;
  assert(nl <= nu);
  assert(load_ptr_nobarrier(&b->next_lower) <=
         load_ptr_nobarrier(&b->next_upper));
  char* ret = expected + cas_diff + ret_diff;
  increment_64_nobarrier(&memory_usage_, bytes);
  return ret;
}

}  // namespace leveldb

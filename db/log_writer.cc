// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "hyperleveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"

namespace leveldb {
namespace log {

Writer::Writer(ConcurrentWritableFile* dest)
    : dest_(dest),
      offset_(0) {
  for (unsigned i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() {
}

Status Writer::AddRecord(const Slice& slice) {
  // computation of block_offset requires a pow2
  assert(kBlockSize == 32768);
  uint64_t start_offset = 0;
  uint64_t end_offset = 0;

  while (true) {
    start_offset = __sync_add_and_fetch(&offset_, 0);
    uint64_t roundup_start = start_offset;
    if (kBlockSize - (start_offset & (kBlockSize - 1)) < kHeaderSize) {
      roundup_start += kHeaderSize;
      roundup_start = roundup_start & ~(kBlockSize - 1);
    }
    const uint64_t left = kBlockSize - (roundup_start & (kBlockSize - 1));
    assert(left >= kHeaderSize);
    if (kHeaderSize + slice.size() <= left) {
      end_offset = roundup_start + kHeaderSize + slice.size();
    } else {
      end_offset = ComputeRecordSize(roundup_start + left,
                                     slice.size() + kHeaderSize - left);
    }
    if (__sync_bool_compare_and_swap(&offset_, start_offset, end_offset)) {
      break;
    }
  }

  const char* ptr = slice.data();
  size_t left = slice.size();
  uint64_t offset = start_offset;

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    uint64_t block_offset = offset & (kBlockSize - 1);
    const uint64_t leftover = kBlockSize - block_offset;
    assert(leftover > 0);
    if (leftover < kHeaderSize) {
      // Switch to a new block
      // Fill the trailer (literal below relies on kHeaderSize being 7)
      assert(kHeaderSize == 7);
      dest_->WriteAt(offset, Slice("\x00\x00\x00\x00\x00\x00", leftover));
      block_offset = 0;
      offset += leftover;
    }
    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize >= block_offset);
    assert(kBlockSize - block_offset >= kHeaderSize);

    const size_t avail = kBlockSize - block_offset - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecordAt(type, ptr, offset, fragment_length);
    offset += kHeaderSize + fragment_length;
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

uint64_t Writer::ComputeRecordSize(uint64_t start, uint64_t remain) {
  assert((start & ~(kBlockSize- 1)) == start);
  const uint64_t per_block = kBlockSize - kHeaderSize;
  const uint64_t whole_blocks = remain / per_block;
  const uint64_t leftover = remain % per_block;
  return start + whole_blocks * kBlockSize + kHeaderSize + leftover;
}

Status Writer::EmitPhysicalRecordAt(RecordType t, const char* ptr, uint64_t offset, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes

  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  Status s = dest_->WriteAt(offset, Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->WriteAt(offset + kHeaderSize, Slice(ptr, n));
  }
  return s;
}

}  // namespace log
}  // namespace leveldb

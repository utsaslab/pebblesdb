// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "pebblesdb/options.h"

#include "pebblesdb/comparator.h"
#include "pebblesdb/env.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      write_buffer_size(4<<20),
      max_open_files(1000),
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
      compression(kNoCompression),
      filter_policy(NULL),
      manual_garbage_collection(false) {
}


}  // namespace leveldb

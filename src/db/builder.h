// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_BUILDER_H_
#define STORAGE_LEVELDB_DB_BUILDER_H_

#include "pebblesdb/status.h"
#include "db/version_set.h"

namespace leveldb {

struct Options;
struct FileMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;

extern Status BuildLevel0Tables(const std::string& dbname,
		Env* env,
        const Options& options,
        TableCache* table_cache,
        Iterator* iter,
        std::vector<FileMetaData> *meta_list,
		std::vector<std::string*> *filter_list,
		VersionSet* versions_,
		std::set<uint64_t>* pending_outputs_,
		std::vector<GuardMetaData*> complete_guards_,
		port::Mutex* mutex_,
		uint64_t* reserved_file_numbers,
		FileLevelFilterBuilder* file_level_filter_builder);

// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
extern Status BuildTable(const std::string& dbname,
                         Env* env,
                         const Options& options,
                         TableCache* table_cache,
                         Iterator* iter,
                         FileMetaData* meta);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_BUILDER_H_

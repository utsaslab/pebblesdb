// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "pebblesdb/env.h"
#include "pebblesdb/table.h"
#include "util/coding.h"

#ifdef TIMER_LOG
	#define start_timer(s) if (timer != NULL) timer->StartTimer(s)
	#define record_timer(s) if (timer != NULL) timer->Record(s)
#else
	#define start_timer(s1)
	#define record_timer(s1)
#endif

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& /*key*/, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
	for (int i = 0; i < NUM_SEEK_THREADS; i++) {
		static_timers_[i] = new Timer();
	}
}

TableCache::~TableCache() {
  for (std::map<uint64_t, FileMetaData*>::iterator it = file_metadata_map.begin(); it != file_metadata_map.end(); ++it) {
	  FileMetaData* f = (*it).second;
	  if (f != NULL) {
		  delete f;
	  }
  }
  for (int i = 0; i < NUM_SEEK_THREADS; i++) {
	  if (static_timers_[i] != NULL) {
		  delete static_timers_[i];
	  }
  }
  delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle, Timer* timer) {
	Status s;
	char buf[sizeof(file_number)];
	EncodeFixed64(buf, file_number);
	Slice key(buf, sizeof(buf));
	*handle = cache_->Lookup(key);
	if (*handle == NULL) {
		start_timer(GET_TABLE_CACHE_GET_FROM_DISK);
		std::string fname = TableFileName(dbname_, file_number);
		RandomAccessFile* file = NULL;
		Table* table = NULL;
		start_timer(GET_TABLE_CACHE_GET_NEW_RANDOM_ACCESS_FILE);
		s = env_->NewRandomAccessFile(fname, &file);
		record_timer(GET_TABLE_CACHE_GET_NEW_RANDOM_ACCESS_FILE);
		if (!s.ok()) {
			std::string old_fname = LDBTableFileName(dbname_, file_number);
			if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
				s = Status::OK();
			}
		}
		if (s.ok()) {
			start_timer(GET_TABLE_CACHE_GET_TABLE_OPEN);
			s = Table::Open(*options_, file, file_size, &table, timer);
			record_timer(GET_TABLE_CACHE_GET_TABLE_OPEN);
		}

		if (!s.ok()) {
			assert(table == NULL);
			delete file;
			// We do not cache error results so that if the error is transient,
			// or somebody repairs the file, we recover automatically.
		} else {
			TableAndFile* tf = new TableAndFile;
			table->SetStaticTimers(static_timers_);
			tf->file = file;
			tf->table = table;
			start_timer(GET_TABLE_CACHE_GET_INSERT_INTO_CACHE);
			*handle = cache_->Insert(key, tf, 1, &DeleteEntry);
			record_timer(GET_TABLE_CACHE_GET_INSERT_INTO_CACHE);
		}
		record_timer(GET_TABLE_CACHE_GET_FROM_DISK);
	}
	return s;
}

void TableCache::SetFileMetaDataMap(uint64_t file_number, uint64_t file_size, InternalKey smallest, InternalKey largest) {
//	printf("Setting file meta data map for file %llu\n", file_number);
	// Disabling this because accessing map with multiple bg compaction threads might lead to race conditions.
	// Anyways this map is not being used anywhere.
	return;
//	  if (file_metadata_map[file_number] == NULL) {
////		  printf("DEBUG :: Creating new FileMetaData for file %llu to set file metadata map in table_cache\n", file_number);
//		  FileMetaData* file_meta = new FileMetaData();
//		  file_meta->number = file_number;
//		  file_meta->file_size = file_size;
//		  file_meta->smallest = smallest;
//		  file_meta->largest = largest;
//		  file_metadata_map[file_number] = file_meta;
//	  }
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle, NULL);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&),
					   Timer* timer) {
//  printf("Tablecache Get().\n");
  Cache::Handle* handle = NULL;
  Status s;
  start_timer(GET_TABLE_CACHE_FIND_TABLE);
  s = FindTable(file_number, file_size, &handle, timer);
//  printf("After finding table. \n");
  record_timer(GET_TABLE_CACHE_FIND_TABLE);
  if (s.ok()) {
    start_timer(GET_TABLE_CACHE_INTERNAL_GET);
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
//    printf("Doing an InternalGet for key from Table.\n");
//    printf("Calling table->InternalGet. \n");
    s = t->InternalGet(options, k, arg, saver, timer);
//    printf("Releasing cache->handle.\n");
    cache_->Release(handle);
    record_timer(GET_TABLE_CACHE_INTERNAL_GET);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "pebblesdb/table.h"

#include "db/version_set.h"
#include "pebblesdb/cache.h"
#include "pebblesdb/comparator.h"
#include "pebblesdb/env.h"
#include "pebblesdb/filter_policy.h"
#include "pebblesdb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/timer.h"

#ifdef TIMER_LOG
	#define start_timer(s) if (timer != NULL) timer->StartTimer(s)
	#define record_timer(s) if (timer != NULL) timer->Record(s)
#else
	#define start_timer(s1)
	#define record_timer(s1)
#endif

#ifdef TIMER_LOG_SEEK
	#define sstart_timer(s) timer->StartTimer(s)
	#define srecord_timer(s) timer->Record(s)
#else
	#define sstart_timer(s)
	#define srecord_timer(s)
#endif

namespace leveldb {

struct Table::Rep {
  Rep()
    : options(),
      status(),
      file(NULL),
      cache_id(),
      filter(),
      filter_data(),
      metaindex_handle(),
      index_block() {
  }
  ~Rep() {
    delete filter;
    delete [] filter_data;
    delete index_block;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;

  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;

 private:
  Rep(const Rep&);
  Rep& operator = (const Rep&);
};

Status Table::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t size,
                   Table** table,
				   Timer* timer) {
  *table = NULL;
  if (size < Footer::kEncodedLength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  start_timer(GET_TABLE_CACHE_GET_TABLE_OPEN_FOOTER_READ);
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  record_timer(GET_TABLE_CACHE_GET_TABLE_OPEN_FOOTER_READ);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents contents;
  Block* index_block = NULL;
  if (s.ok()) {
	start_timer(GET_TABLE_CACHE_GET_TABLE_OPEN_INDEX_BLOCK_READ);
    s = ReadBlock(file, ReadOptions(), footer.index_handle(), &contents);
	record_timer(GET_TABLE_CACHE_GET_TABLE_OPEN_INDEX_BLOCK_READ);
    if (s.ok()) {
      index_block = new Block(contents);
    }
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = NULL;
    rep->filter = NULL;
    *table = new Table(rep);
	start_timer(GET_TABLE_CACHE_GET_TABLE_OPEN_READ_META);
    (*table)->ReadMeta(footer);
	record_timer(GET_TABLE_CACHE_GET_TABLE_OPEN_READ_META);
  } else {
    if (index_block) delete index_block;
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == NULL) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();     // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
  delete rep_;
}

static void DeleteBlock(void* arg, void* /*ignored*/) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& /*key*/, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) {
  int index = rand() % NUM_SEEK_THREADS;

  Table* table = reinterpret_cast<Table*>(arg);
  Timer* timer = table->static_timers_[index];
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = NULL;
  Cache::Handle* cache_handle = NULL;

  sstart_timer(SEEK_BLOCK_READER_TOTAL);

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  uint64_t a, b;
  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
    	sstart_timer(SEEK_BLOCK_READER_READ_BLOCK);
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        srecord_timer(SEEK_BLOCK_READER_READ_BLOCK);

        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);
          }
        }
      }
    } else {
      sstart_timer(SEEK_BLOCK_READER_READ_BLOCK);
      s = ReadBlock(table->rep_->file, options, handle, &contents);
   	  srecord_timer(SEEK_BLOCK_READER_READ_BLOCK);

   	  if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  srecord_timer(SEEK_BLOCK_READER_TOTAL);

  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg,
                          void (*saver)(void*, const Slice&, const Slice&),
						  Timer* timer) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  start_timer(GET_TABLE_CACHE_INDEX_ITER_SEEK);
  iiter->Seek(k);
  record_timer(GET_TABLE_CACHE_INDEX_ITER_SEEK);

  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    start_timer(GET_TABLE_CACHE_FILTER_CHECK);
    bool file_level_filter_enabled = false;

#ifdef FILE_LEVEL_FILTER
    file_level_filter_enabled = true;
#endif
    if (file_level_filter_enabled == false && filter != NULL &&
        handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
    	record_timer(GET_TABLE_CACHE_FILTER_CHECK);
      // Not found
    } else {
      record_timer(GET_TABLE_CACHE_FILTER_CHECK);

      start_timer(GET_TABLE_CACHE_READ_DATA_BLOCK);
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*saver)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
      record_timer(GET_TABLE_CACHE_READ_DATA_BLOCK);
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}


uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb

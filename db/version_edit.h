// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct GuardMetaData; 
 
struct FileMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table
  GuardMetaData* guard;       // The guard that the file belongs to.
  
FileMetaData() : refs(0), allowed_seeks(1 << 30), number(0), file_size(0), smallest(), largest(), guard() { }
};

/* 
guard_key is the smallest key served by the guard file. In each level,
there can be only one guard starting with a given key, so (level, key)
uniquely identifies a guard.
*/
struct GuardMetaData {
  int refs;
  int level;
  uint64_t number_segments;
  InternalKey guard_key; // guard key is selected before any keys are inserted
  /* Need not be same as guard_key. Ex: g: 100, smallest: 102 */
  InternalKey smallest; 
  InternalKey largest;   // Largest internal key served by table
  // The list of file numbers that form a part of this guard.
  std::vector<uint64_t> files;
  std::vector<FileMetaData*> file_metas;
  
GuardMetaData() : refs(0), level(-1), guard_key(), smallest(), largest(), number_segments(0) { files.clear();}
};
 
class VersionEdit {
 public:
  VersionEdit()
    : comparator_(),
      log_number_(),
      prev_log_number_(),
      next_file_number_(),
      last_sequence_(),
      has_comparator_(),
      has_log_number_(),
      has_prev_log_number_(),
      has_next_file_number_(),
      has_last_sequence_(),
      compact_pointers_(),
      deleted_files_(),
      new_files_() {
    Clear();
  }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  /* Add the file to the appropriate sentinel. */
  void AddFileToSentinel(FileMetaData* f, int level) {
    assert(level >= 0 && level < config::kNumLevels);
    FileMetaData ff = *f;
    sentinel_files_[level].push_back(ff);
  }

  void AddSentinelFile(int level, int allowed_seeks, uint64_t file_size, GuardMetaData* g, InternalKey largest, InternalKey smallest, uint64_t number, int refs) {
	  FileMetaData meta;
	  meta.allowed_seeks = allowed_seeks;
	  meta.file_size = file_size;
	  meta.guard = g;
	  meta.largest = largest;
	  meta.smallest = smallest;
	  meta.number = number;
	  meta.refs = refs;
	  sentinel_files_[level].push_back(meta);
  }

  void AddSentinelFileNo(int level, uint64_t number) {
	  sentinel_file_nos_[level].push_back(number);
  }

  void AddGuard(int level, const InternalKey& guard_key) {
    assert(level >= 0 && level < config::kNumLevels);
    GuardMetaData g;
    g.guard_key = guard_key;
    g.level = level;
    g.number_segments = 0;
    new_guards_[level].push_back(g);
  }

  void AddCompleteGuard(int level, const InternalKey& guard_key) {
    assert(level >= 0 && level < config::kNumLevels);
    GuardMetaData g;
    g.guard_key = guard_key;
    g.level = level;
    g.number_segments = 0;
    new_complete_guards_[level].push_back(g);
  }

  void AddGuardFromExisting(int level, GuardMetaData* g) {
    assert(level >= 0 && level < config::kNumLevels);
    GuardMetaData new_g(*g);
    new_guards_[level].push_back(new_g);
  }

  void AddCompleteGuardFromExisting(int level, GuardMetaData* g) {
    assert(level >= 0 && level < config::kNumLevels);
    GuardMetaData new_g(*g);
    new_complete_guards_[level].push_back(new_g);
  }

  /* A version of AddGuard that contains files. */
  void AddGuardWithFiles(int level, uint64_t number_segments,
			 const InternalKey& guard_key,
			 const InternalKey& smallest,
			 const InternalKey& largest,
			 const std::vector<uint64_t> files) {
    assert(level >= 0 && level < config::kNumLevels);
    GuardMetaData g;
    g.guard_key = guard_key;
    g.level = level;
    g.smallest = smallest;
    g.largest = largest;
    g.number_segments = number_segments;
    g.files.insert(g.files.end(), files.begin(), files.end());
    new_guards_[level].push_back(g);
  }
  
  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  void DeleteGuard(int level, InternalKey guard) {
    deleted_guards_.insert(std::make_pair(level, guard));
  }

  void DeleteSentinelFile(int level, uint64_t file) {
    deleted_sentinel_files_.insert(std::make_pair(level, file));
  }
  
  void UpdateGuards(uint64_t* guard_array) {
    for (int i = 0; i < config::kNumLevels; i++) {
      guard_array[i] += new_guards_[i].size();
    }
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::pair<int, InternalKey> GuardPair;
  
  // Create struct to allow comparing two pairs of level and internal keys.
  struct BySmallestPair {
    const InternalKeyComparator* internal_comparator;

    bool operator()(GuardPair p1, GuardPair p2) const {
      if (p1.first !=  p2.first) {
        return (p1.first < p2.first);
      } else {
	int r = internal_comparator->Compare(p1.second, p2.second);	
        return (r < 0);
      }
    }
  };
  
  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;
  typedef std::set< std::pair<int, InternalKey>, BySmallestPair> DeletedGuardSet;
  
  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector< std::pair<int, FileMetaData> > new_files_;

  /* Structures to contain files for the sentinel guards. */
  std::vector<FileMetaData> sentinel_files_[config::kNumLevels];
  std::vector<uint64_t> sentinel_file_nos_[config::kNumLevels];
  DeletedFileSet deleted_sentinel_files_;
  
  std::vector<GuardMetaData> new_guards_[config::kNumLevels];
  std::vector<GuardMetaData> new_complete_guards_[config::kNumLevels];
  DeletedGuardSet deleted_guards_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_

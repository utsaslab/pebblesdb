// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"

namespace leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator           = 1,
  kLogNumber            = 2,
  kNextFileNumber       = 3,
  kLastSequence         = 4,
  kCompactPointer       = 5,
  kDeletedFile          = 6,
  kNewFile              = 7,
  // 8 was used for large value refs
  kPrevLogNumber        = 9,
  kDeletedGuard         = 10,
  kNewGuard             = 11,
  kFileInsideGuard      = 12,
  kNewSentinelFile      = 13,
  kDeletedSentinelFile  = 14,
  kNewCompleteGuard     = 15,
  kNewSentinelFileNo	= 16
};

void VersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  deleted_files_.clear();
  new_files_.clear();
  deleted_guards_.clear();
  for (size_t i = 0; i < config::kNumLevels; i++) {
    new_guards_[i].clear();
    sentinel_files_[i].clear();
  }
  deleted_sentinel_files_.clear();
}

void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
       iter != deleted_files_.end();
       ++iter) {
    PutVarint32(dst, kDeletedFile);
    PutVarint32(dst, iter->first);   // level
    PutVarint64(dst, iter->second);  // file number
  }

  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, new_files_[i].first);  // level
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
  }

  // Encode deleted guards
  for (DeletedGuardSet::const_iterator iter = deleted_guards_.begin();
       iter != deleted_guards_.end();
       ++iter) {
    PutVarint32(dst, kDeletedGuard);
    PutVarint32(dst, iter->first);   // level
    PutLengthPrefixedSlice(dst, iter->second.Encode());  // guard key
  }

  // Encode added guards
  for (size_t k = 0; k < config::kNumLevels; k++) {
    for (size_t i = 0; i < new_guards_[k].size(); i++) {
      const GuardMetaData& g = new_guards_[k][i];
      PutVarint32(dst, kNewGuard);
      PutVarint32(dst, g.level);  // level
      PutVarint64(dst, 0 /* g.number_segments */); // We don't write the file information to disk because it will be automatically retrieved from files_ in LogAndApply
      PutLengthPrefixedSlice(dst, g.guard_key.Encode());
    }
  }

  // Encode complete guards
  for (size_t k = 0; k < config::kNumLevels; k++) {
    for (size_t i = 0; i < new_complete_guards_[k].size(); i++) {
      const GuardMetaData& g = new_complete_guards_[k][i];
      PutVarint32(dst, kNewCompleteGuard);
      PutVarint32(dst, g.level);  // level
      PutVarint64(dst, 0 /* g.number_segments */);
      PutLengthPrefixedSlice(dst, g.guard_key.Encode());
    }
  }

  for (DeletedFileSet::const_iterator iter = deleted_sentinel_files_.begin();
       iter != deleted_sentinel_files_.end();
       ++iter) {
    PutVarint32(dst, kDeletedSentinelFile);
    PutVarint32(dst, iter->first);   // level
    PutVarint64(dst, iter->second);  // file number
  }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return true;
  } else {
    return false;
  }
}

static bool GetLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) &&
      v < config::kNumLevels) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  const char* msg = NULL;
  uint32_t tag, tag2;

  // Temporary storage for parsing
  int level;
  uint64_t number, fnumber;
  FileMetaData f;
  Slice str;
  InternalKey key;
  GuardMetaData g;
  
  while (msg == NULL && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level) &&
            GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;

      case kDeletedFile:
        if (GetLevel(&input, &level) &&
            GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          msg = "deleted file";
        }
        break;

    case kDeletedGuard:
        if (GetLevel(&input, &level) &&
	    GetInternalKey(&input, &key)) {
          deleted_guards_.insert(std::make_pair(level, key));
        } else {
          msg = "deleted guard";
        }
        break;

    case kDeletedSentinelFile:
      if (GetLevel(&input, &level) &&
	  GetVarint64(&input, &number)) {
    	  deleted_sentinel_files_.insert(std::make_pair(level, number));
      } else {
    	  msg = "deleted sentinel file";
      }
      break;

    case kNewFile:
      if (GetLevel(&input, &level) &&
	  GetVarint64(&input, &f.number) &&
	  GetVarint64(&input, &f.file_size) &&
	  GetInternalKey(&input, &f.smallest) &&
	  GetInternalKey(&input, &f.largest)) {
    	  new_files_.push_back(std::make_pair(level, f));
      } else {
	msg = "new-file entry";
      }
	    break;
	    
    case kNewSentinelFile:
      if (GetLevel(&input, &level) &&
	  GetVarint64(&input, &fnumber)) {
    	  sentinel_file_nos_[level].push_back(fnumber);
      } else {
	msg = "new-sentinel-file entry";
      }
      break;
      
    case kNewSentinelFileNo:
      if (GetLevel(&input, &level) &&
	  GetVarint64(&input, &f.number) &&
	  GetVarint64(&input, &f.file_size) &&
	  GetInternalKey(&input, &f.smallest) &&
	  GetInternalKey(&input, &f.largest)) {
	new_files_.push_back(std::make_pair(level, f));
      } else {
	msg = "new-sentinel-file entry";
      }
      break;

   case kNewGuard:
        if (GetLevel(&input, &g.level)
        		&& GetVarint64(&input, &g.number_segments)
				&& GetInternalKey(&input, &g.guard_key)) {
          /* Gather all the files inside the guard. */
		  g.files.clear();
		  if (g.number_segments > 0) {
			assert(GetInternalKey(&input, &g.smallest) &&
			   GetInternalKey(&input, &g.largest));
			for (size_t j = 0; j < g.number_segments; j++) {
			  GetVarint32(&input, &tag2);
			  assert(tag2 == kFileInsideGuard);
			  GetVarint64(&input, &fnumber);
			  g.files.push_back(fnumber);
			}
		  }
		  new_guards_[g.level].push_back(g);
        } else {
          msg = "new-guard entry";
        }
        break;
	    
   case kNewCompleteGuard:
        if (GetLevel(&input, &g.level)
        		&& GetVarint64(&input, &g.number_segments)
				&& GetInternalKey(&input, &g.guard_key)) {
          /* Gather all the files inside the guard. */
		  g.files.clear();
		  // For complete guards, we do not decode the individual file details
		  if (g.number_segments > 0) {
			assert(GetInternalKey(&input, &g.smallest)
					&& GetInternalKey(&input, &g.largest));
		  }
		  new_complete_guards_[g.level].push_back(g);
        } else {
          msg = "new-complete-guard entry";
        }
        break;

   default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == NULL && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != NULL) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    r.append("\n  CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }
  for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
       iter != deleted_files_.end();
       ++iter) {
    r.append("\n  DeleteFile: ");
    AppendNumberTo(&r, iter->first);
    r.append(" ");
    AppendNumberTo(&r, iter->second);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.number);
    r.append(" ");
    AppendNumberTo(&r, f.file_size);
    r.append(" ");
    r.append(f.smallest.DebugString());
    r.append(" .. ");
    r.append(f.largest.DebugString());
  }
  // Add guards to the debug string
  for (DeletedGuardSet::const_iterator iter = deleted_guards_.begin();
       iter != deleted_guards_.end();
       ++iter) {
    r.append("\n  DeleteGuard: ");
    AppendNumberTo(&r, iter->first);
    r.append(" ");
    r.append(iter->second.DebugString());
  }

  for (size_t k = 0; k < config::kNumLevels; k++)
    for (size_t i = 0; i < new_guards_[k].size(); i++) {
      const GuardMetaData& g = new_guards_[k][i];
      r.append("\n  AddGuard: ");
      AppendNumberTo(&r, g.level);
      r.append(" ");
      AppendNumberTo(&r, g.number_segments);
      r.append(" ");
      r.append(g.guard_key.DebugString());
      r.append(" ");
      r.append(g.smallest.DebugString());
      r.append(" .. ");
      r.append(g.largest.DebugString());
      r.append(" Files: ");
      if (g.number_segments > 0) {
	for (size_t j = 0; j < g.files.size(); j++) {
	  AppendNumberTo(&r, g.files[j]);
	  r.append(" ");
	}
      }
    }
  r.append("\n");

  for (size_t k = 0; k < config::kNumLevels; k++)
    for (size_t i = 0; i < new_complete_guards_[k].size(); i++) {
      const GuardMetaData& g = new_complete_guards_[k][i];
      r.append("\n  AddCompleteGuard: ");
      AppendNumberTo(&r, g.level);
      r.append(" ");
      AppendNumberTo(&r, g.number_segments);
      r.append(" ");
      r.append(g.guard_key.DebugString());
      r.append(" ");
      r.append(g.smallest.DebugString());
      r.append(" .. ");
      r.append(g.largest.DebugString());
      r.append(" Files: ");
      if (g.number_segments > 0) {
    	  for (size_t j = 0; j < g.files.size(); j++) {
    		  AppendNumberTo(&r, g.files[j]);
    		  r.append(" ");
    	  }
      }
    }
  r.append("\n}\n");
  return r;
}

}  // namespace leveldb

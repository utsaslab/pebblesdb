// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "pebblesdb/db.h"
#include "pebblesdb/env.h"
#include "pebblesdb/iterator.h"

namespace leveldb {

// Finish and check for file errors
void FinishFileCompletion(Status s,
		FileMetaData meta,
		WritableFile* file,
		TableCache* table_cache,
		Env* env,
		std::string fname) {
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta.number, meta.file_size);
      s = it->status();
      delete it;
    }
    if (s.ok() && meta.file_size > 0) {
      // Keep the file
    } else {
      env->DeleteFile(fname);
    }
}

void AddFilterString(FileLevelFilterBuilder* file_level_filter_builder, int n,
					 std::vector<std::string*>* filter_list,
					 const FilterPolicy* filter_policy, uint64_t file_number) {
#ifdef FILE_LEVEL_FILTER
	if (filter_policy != NULL) {
		std::string* filter_string = file_level_filter_builder->GenerateFilter();
		filter_list->push_back(filter_string);
		file_level_filter_builder->Clear();
	}
#endif
}

Status BuildLevel0Tables(const std::string& dbname,
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
				  FileLevelFilterBuilder* file_level_filter_builder) {
	  Status s;
	  int meta_index = 0, num_guards = complete_guards_.size();
	  int num_reserved_file_numbers = num_guards + 1;
	  int file_number_index = 0;
	  int count = 0;
	  int tot_parsed = 0;

	  FileMetaData meta;
	  WritableFile* file;
	  TableBuilder* builder;
	  const FilterPolicy* filter_policy = options.filter_policy;
	  int index = 0;

	  iter->SeekToFirst();
	  if (iter->Valid()) {
		  for (int i = 0; i < num_guards; i++) {
			  for (; iter->Valid(); iter->Next()) {
				  Slice key = iter->key();

				  ParsedInternalKey parsed_key;
				  ParseInternalKey(key, &parsed_key);

				  GuardMetaData* current_guard = complete_guards_[i];
				  const Comparator* user_comparator = versions_->GetInternalComparator().user_comparator();
				  if (user_comparator->Compare(parsed_key.user_key, current_guard->guard_key.user_key()) < 0) {
					  if (count == 0) {
						  	if (file_number_index < num_reserved_file_numbers) {
						  		meta.number = reserved_file_numbers[file_number_index];
							  	file_number_index++;
						  	} else {
							  	mutex_->Lock();
								meta.number = versions_->NewFileNumber();
								pending_outputs_->insert(meta.number);
								mutex_->Unlock();
						  	}
							const std::string fname = TableFileName(dbname, meta.number);
							s = env->NewWritableFile(fname, &file);
							if (!s.ok()) {
								return s;
							}
							builder = new TableBuilder(options, file);
							meta.smallest.DecodeFrom(iter->key());
					  }
					  builder->Add(iter->key(), iter->value());
#ifdef FILE_LEVEL_FILTER
					  file_level_filter_builder->AddKey(key);
#endif
					  meta.largest.DecodeFrom(iter->key());
					  count++;
					  tot_parsed++;
				  } else {
					  if (count > 0) {
						  s = builder->Finish();
						  if (s.ok()) {
							  meta.file_size = builder->FileSize();
							  assert(meta.file_size > 0);
							  meta_list->push_back(meta);

							  // Calculate the filter string for this file
							  AddFilterString(file_level_filter_builder, index, filter_list, filter_policy, meta.number);
							  table_cache->SetFileMetaDataMap(meta.number, meta.file_size, meta.smallest, meta.largest);
						  }
						  delete builder;
						  count = 0;
						  index = 0;
						  const std::string fname = TableFileName(dbname, meta.number);
						  FinishFileCompletion(s, meta, file, table_cache, env, fname);
					  }
					  break;
				  }
			  }
		  }
		  if (count > 0) {
			  s = builder->Finish();
			  if (s.ok()) {
				  meta.file_size = builder->FileSize();
				  assert(meta.file_size > 0);
				  meta_list->push_back(meta);

				  // Calculate the filter string for this file
				  AddFilterString(file_level_filter_builder, index, filter_list, filter_policy, meta.number);
				  table_cache->SetFileMetaDataMap(meta.number, meta.file_size, meta.smallest, meta.largest);
			  }
			  delete builder;
			  count = 0;
			  index = 0;
			  const std::string fname = TableFileName(dbname, meta.number);
			  FinishFileCompletion(s, meta, file, table_cache, env, fname);
		  }
		  // Creating file for the entries belonging to last guard (or) the sentinel (in case there are no guards)
		  for (; iter->Valid(); iter->Next()) {
			  if (count == 0) {
				  	if (file_number_index < num_reserved_file_numbers) {
				  		meta.number = reserved_file_numbers[file_number_index];
					  	file_number_index++;
				  	} else {
					  	mutex_->Lock();
						meta.number = versions_->NewFileNumber();
						pending_outputs_->insert(meta.number);
						mutex_->Unlock();
				  	}

				  	const std::string fname = TableFileName(dbname, meta.number);
					s = env->NewWritableFile(fname, &file);
					if (!s.ok()) {
						return s;
					}
					builder = new TableBuilder(options, file);
					meta.smallest.DecodeFrom(iter->key());
			  }
			  builder->Add(iter->key(), iter->value());

#ifdef FILE_LEVEL_FILTER
			  file_level_filter_builder->AddKey(iter->key());
#endif
			  meta.largest.DecodeFrom(iter->key());
			  count++;
			  tot_parsed++;
		  }
		  if (count > 0) {
			  s = builder->Finish();
			  if (s.ok()) {
				  meta.file_size = builder->FileSize();
				  assert(meta.file_size > 0);
				  meta_list->push_back(meta);

				  // Calculate the filter string for this file
				  AddFilterString(file_level_filter_builder, index, filter_list, filter_policy, meta.number);
				  table_cache->SetFileMetaDataMap(meta.number, meta.file_size, meta.smallest, meta.largest);
			  }
			  delete builder;
			  count = 0;
			  index = 0;
			  const std::string fname = TableFileName(dbname, meta.number);
			  FinishFileCompletion(s, meta, file, table_cache, env, fname);
		  }
	  }
	  // Check for input iterator errors
	  if (!iter->status().ok()) {
	    s = iter->status();
	  }
	  return s;
}

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      builder->Add(key, iter->value());
    }

    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();
      if (s.ok()) {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
      }
    } else {
      builder->Abandon();
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = NULL;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb

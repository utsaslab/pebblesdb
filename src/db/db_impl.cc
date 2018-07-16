// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define __STDC_LIMIT_MACROS

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/replay_iterator.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "pebblesdb/db.h"
#include "pebblesdb/env.h"
#include "pebblesdb/replay_iterator.h"
#include "pebblesdb/status.h"
#include "pebblesdb/table.h"
#include "pebblesdb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/timer.h"

#include <iostream>

#ifdef TIMER_LOG
	#define start_timer(s) timer->StartTimer(s)
	#define record_timer(s) timer->Record(s)
#else
	#define start_timer(s)
	#define record_timer(s)
#endif

#ifdef TIMER_LOG_SIMPLE
	#define start_timer_simple(s) timer->StartTimer(s)
	#define record_timer_simple(s) timer->Record(s)
#else
	#define start_timer_simple(s)
	#define record_timer_simple(s)
#endif

namespace leveldb {

const unsigned kStraightReads = 10;

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  port::CondVar cv_;
  bool linked_;
  bool has_imm_;
  bool wake_me_when_head_;
  bool block_if_backup_in_progress_;
  Writer* prev_;
  Writer* next_;
  uint64_t micros_;
  uint64_t start_sequence_;
  uint64_t end_sequence_;
  MemTable* mem_;
  SHARED_PTR<WritableFile> logfile_;
  SHARED_PTR<log::Writer> log_;

  explicit Writer(port::Mutex* mtx)
    : cv_(mtx),
      linked_(false),
      has_imm_(false),
      wake_me_when_head_(false),
      block_if_backup_in_progress_(true),
      prev_(NULL),
      next_(NULL),
      micros_(0),
      start_sequence_(0),
      end_sequence_(0),
      mem_(NULL),
      logfile_(),
      log_() {
  }
  ~Writer() throw () {
    // must do in order: log, logfile
    if (log_) {
      assert(logfile_);
      log_.reset();
      logfile_.reset();
    }

    // safe because Unref is synchronized internally
    if (mem_) {
      mem_->Unref();
    }
  }
 private:
  Writer(const Writer&);
  Writer& operator = (const Writer&);
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    Output() : number(), file_size(), smallest(), largest() {}
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest, largest;
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(),
        outputs(),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
 private:
  CompactionState(const CompactionState&);
  CompactionState& operator = (const CompactionState&);
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(),
      db_lock_(NULL),
      mutex_(),
      shutting_down_(NULL),
      mem_(new MemTable(internal_comparator_)),
      imm_(NULL),
      has_imm_(),
      logfile_(),
      logfile_number_(0),
      log_(),
      seed_(0),
      writers_mutex_(),
      writers_upper_(0),
      writers_tail_(NULL),
      snapshots_(),
      pending_outputs_(),
      allow_background_activity_(false),
      num_bg_threads_(0),
      bg_fg_cv_(&mutex_),
      bg_compaction_cv_(&mutex_),
      bg_memtable_cv_(&mutex_),
      bg_log_cv_(&mutex_),
      bg_log_occupied_(false),
      manual_compaction_(NULL),
      manual_garbage_cutoff_(raw_options.manual_garbage_collection ?
                             SequenceNumber(0) : kMaxSequenceNumber),
      replay_iters_(),
      straight_reads_(0),
      versions_(),
      backup_cv_(&writers_mutex_),
      backup_in_progress_(),
      backup_waiters_(0),
      backup_waiter_has_it_(false),
      backup_deferred_delete_(),
      bg_error_(),
      num_bg_compaction_threads_(1) {
  mutex_.Lock();
  mem_->Ref();
  has_imm_.Release_Store(NULL);
  backup_in_progress_.Release_Store(NULL);
  env_->StartThread(&DBImpl::CompactMemTableWrapper, this);
  for (int i = 1; i <= num_bg_compaction_threads_; i++) {
	  env_->StartThread(&DBImpl::CompactLevelWrapper, this);
  }
  num_bg_threads_ = num_bg_compaction_threads_ + 1;

  // Reserve ten files or so for other uses and give the rest to TableCache.
  int max_open_files = options_.max_open_files;
  const int table_cache_size = max_open_files - kNumNonTableCacheFiles;

  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);
  timer = new Timer();
  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &internal_comparator_, timer);

  for (unsigned i = 0; i < leveldb::config::kNumLevels; ++i) {
    guard_array_[i] = 0;
    levels_locked_[i] = false;
  }
  mutex_.Unlock();
  writers_mutex_.Lock();
  writers_mutex_.Unlock();
}

DBImpl::~DBImpl() {

  // Wait for background work to finish
#ifdef TIMER_LOG
  PrintTimerAudit();
#endif

#ifdef TIMER_LOG_SIMPLE
	PrintTimerAudit();
#endif

  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  bg_compaction_cv_.SignalAll();
  bg_memtable_cv_.SignalAll();
  while (num_bg_threads_ > 0) {
    bg_fg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  log_.reset();
  logfile_.reset();
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
  delete timer;
}

void DBImpl::ClearTimer() {
	timer->clear();
}

void DBImpl::PrintTimerAudit() {
	  printf("-------------------------- Timer information ----------------------\n");
	  printf("%s\n", timer->DebugString().c_str());
	  printf("-------------------------------------------------------------------\n");

	  versions_->PrintSeekThreadsTimerAuditIndividual();
	  versions_->PrintSeekThreadsTimerAuditCumulative();

	  versions_->PrintSeekThreadsStaticTimerAuditIndividual();
	  versions_->PrintSeekThreadsStaticTimerAuditCumulative();
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  ConcurrentWritableFile* file;
  Status s = env_->NewConcurrentWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  // Defer if there's background activity
  mutex_.AssertHeld();
  if (backup_in_progress_.Acquire_Load() != NULL) {
    backup_deferred_delete_ = true;
    return;
  }

  // If you ever release mutex_ in this function, you'll need to do more work in
  // LiveBackup

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
        default:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
          // Remove all the in memory maps used.
          versions_->RemoveFileLevelBloomFilterInfo(number);
          versions_->RemoveFileMetaDataFromTableCache(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit* edit) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(); // This will recover from current manifest file.
  if (s.ok()) {
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t min_log = versions_->LogNumber(); // This will be populated by versions_->Recover() from manifest file
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) {
      return s;
    }
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        expected.erase(number);
        if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
          logs.push_back(number);
      }
    }
    if (!expected.empty()) {
      char buf[50];
      snprintf(buf, sizeof(buf), "%d missing files; e.g.",
               static_cast<int>(expected.size()));
      return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
      s = RecoverLogFile(logs[i], edit, &max_sequence);

      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]);
    }

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
      }
    }
  }

  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number,
                              VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    LogReporter()
      : env(),
        info_log(),
        fname(),
        status() {
    }
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
   private:
    LogReporter(const LogReporter&);
    LogReporter& operator = (const LogReporter&);
  };

  mutex_.AssertHeld();

  FileLevelFilterBuilder file_level_filter_builder(options_.filter_policy);

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentially make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertIntoVersion(&batch, mem,
						   versions_->current());

    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      std::vector<uint64_t> numbers;
      std::vector<std::string*> file_level_filters;
      status = WriteLevel0TableGuards(mem, edit, versions_->current(), numbers, &file_level_filter_builder, &file_level_filters);
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
      // Numbers can possibly contain more values than filters because the reserved file numbers are appended
      // at the end to be cleared from pending outputs
      for (int i = 0; i < numbers.size() && i < file_level_filters.size(); i++) {
      	versions_->AddFileLevelBloomFilterInfo(numbers[i], file_level_filters[i]);
      }
      file_level_filters.clear();
      mem->Unref();
      mem = NULL;
    }
  }

  // Add all the guards to edit for this level
  std::set<int> level_to_load_complete_guards;
  versions_->current()->AddGuardsToEdit(edit, level_to_load_complete_guards);

  // Add all the complete guards to edit
  versions_->current()->AddCompleteGuardsToEdit(edit);

  if (status.ok() && mem != NULL) {
	std::vector<uint64_t> numbers;
	std::vector<std::string*> file_level_filters;
    status = WriteLevel0TableGuards(mem, edit, versions_->current(), numbers, &file_level_filter_builder, &file_level_filters);
    // Reflect errors immediately so that conditions like full
    // file-systems cause the DB::Open() to fail.

#ifdef FILE_LEVEL_FILTER
    if (options_.filter_policy != NULL) {
		for (int i = 0; i < numbers.size(); i++) {
			versions_->AddFileLevelBloomFilterInfo(numbers[i], file_level_filters[i]);
		}
    }
#endif

    file_level_filters.clear();
    for (size_t fno = 0; fno < numbers.size(); fno++) {
    	pending_outputs_.erase(numbers[fno]);
    }
  }

  if (mem != NULL) {
	  mem->Unref();
	  mem = NULL;
  }
  file_level_filter_builder.Destroy();
  delete file;
  return status;
}

Status DBImpl::WriteLevel0TableGuards(MemTable* mem, VersionEdit* edit,
                                Version* base, std::vector<uint64_t> &numbers,
								FileLevelFilterBuilder* file_level_filter_builder,
								std::vector<std::string*>* file_level_filters) {
  mutex_.AssertHeld();
  int m;
  const uint64_t start_micros = env_->NowMicros();
  std::vector<FileMetaData> meta_list;

  Iterator* iter = mem->NewIterator();
  std::vector<GuardMetaData*> guards_;
  if (base != NULL) {
  	guards_ = base->GetGuardsAtLevel(0);
  }
  int num_level0_guards = guards_.size();
  uint64_t reserved_file_numbers[num_level0_guards+1];
  for (int i = 0; i <= num_level0_guards; i++) {
	  reserved_file_numbers[i] = versions_->NewFileNumber();
	  pending_outputs_.insert(reserved_file_numbers[i]);
  }
  Status s;
  {
    mutex_.Unlock();
    start_timer(BUILD_LEVEL0_TABLES);
    s = BuildLevel0Tables(dbname_, env_, options_, table_cache_, iter, &meta_list,
    					  file_level_filters, versions_, &pending_outputs_, guards_, &mutex_,
						  reserved_file_numbers, file_level_filter_builder);
    record_timer(BUILD_LEVEL0_TABLES);

    start_timer(GET_LOCK_AFTER_BUILD_LEVEL0_TABLES);
    mutex_.Lock();
    record_timer(GET_LOCK_AFTER_BUILD_LEVEL0_TABLES);
  }
  delete iter;

  start_timer(ADD_LEVEL0_FILES_TO_EDIT);
  uint64_t total_file_size = 0;
  for (unsigned i = 0; i < meta_list.size(); i++) {
		FileMetaData meta = meta_list[i];
		Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
				(unsigned long long) meta.number,
				(unsigned long long) meta.file_size,
				s.ToString().c_str());
		// Note that if file_size is zero, the file has been deleted and
		// should not be added to the manifest.
		int level = 0;
		if (s.ok() && meta.file_size > 0) {
			const Slice min_user_key = meta.smallest.user_key();
			const Slice max_user_key = meta.largest.user_key();
			// Note: We are always putting the new files to level 0
			edit->AddFile(level, meta.number, meta.file_size,
						  meta.smallest, meta.largest);
			numbers.push_back(meta.number);
			total_file_size += meta.file_size;
		}
  }

  // Adding the remaining reserved but unused file numbers so that they can be removed from the pending_outputs set
  for (unsigned i = meta_list.size(); i <= num_level0_guards; i++) {
	  numbers.push_back(reserved_file_numbers[i]);
  }
  record_timer(ADD_LEVEL0_FILES_TO_EDIT);

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = total_file_size;
  stats_[0].Add(stats);
  return s;
}

void DBImpl::CompactMemTableThread() {
  MutexLock l(&mutex_);

  FileLevelFilterBuilder file_level_filter_builder(options_.filter_policy);

  int cnt = 0;
  bool first_memtable_compaction = true;
  while (!shutting_down_.Acquire_Load() && !allow_background_activity_) {
    bg_memtable_cv_.Wait();
  }
  while (!shutting_down_.Acquire_Load()) {
    while (!shutting_down_.Acquire_Load() && imm_ == NULL) {
      bg_memtable_cv_.Wait();
    }
    if (shutting_down_.Acquire_Load()) {
      break;
    }

    start_timer_simple(TOTAL_MEMTABLE_COMPACTION);
    start_timer(TOTAL_MEMTABLE_COMPACTION);
    cnt++;
    // Save the contents of the memtable as a new Table
    VersionEdit edit;
    Version* base = versions_->current();

    base->Ref();
    std::vector<uint64_t> numbers;
    uint64_t number = 0;
    start_timer(WRITE_LEVEL0_TABLE_GUARDS);
    std::vector<std::string*> file_level_filters;

    Status s = WriteLevel0TableGuards(imm_, &edit, base, numbers, &file_level_filter_builder, &file_level_filters);
    record_timer(WRITE_LEVEL0_TABLE_GUARDS);
    
    // Add all the complete guards to edit
    start_timer(CMT_ADD_COMPLETE_GUARDS_TO_EDIT);
    if (first_memtable_compaction) {
    	base->AddCompleteGuardsToEdit(&edit);
    }
    record_timer(CMT_ADD_COMPLETE_GUARDS_TO_EDIT);

    base->Unref(); base = NULL;

    if (s.ok() && shutting_down_.Acquire_Load()) {
      s = Status::IOError("Deleting DB during memtable compaction");
    }

    // Replace immutable memtable with the generated Table
    if (s.ok()) {
      edit.SetPrevLogNumber(0);
      edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed

      start_timer(CMT_LOG_AND_APPLY);
      s = versions_->LogAndApply(&edit, &mutex_, &bg_log_cv_, &bg_log_occupied_, numbers, file_level_filters, 1);
      record_timer(CMT_LOG_AND_APPLY);
    }

    start_timer(CMT_ERASE_PENDING_OUTPUTS);
    for (int v = 0; v < numbers.size(); v++) {
    	pending_outputs_.erase(numbers[v]);
    }
    record_timer(CMT_ERASE_PENDING_OUTPUTS);

    if (s.ok()) {
      // Commit to the new state
      start_timer(CMT_DELETE_OBSOLETE_FILES);
      imm_->Unref();
      imm_ = NULL;
      has_imm_.Release_Store(NULL);
      bg_fg_cv_.SignalAll();

      bg_compaction_cv_.SignalAll();
      DeleteObsoleteFiles();
      record_timer(CMT_DELETE_OBSOLETE_FILES);
    } else {
      RecordBackgroundError(s);
    }

    if (!shutting_down_.Acquire_Load() && !s.ok()) {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      bg_fg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      Log(options_.info_log, "Waiting after memtable compaction error: %s",
          s.ToString().c_str());
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }
    assert(config::kL0_SlowdownWritesTrigger > 0);
    first_memtable_compaction = false;
    record_timer(TOTAL_MEMTABLE_COMPACTION);
    record_timer_simple(TOTAL_MEMTABLE_COMPACTION);
  }

  Log(options_.info_log, "cleaning up CompactMemTableThread");
  num_bg_threads_ -= 1;
  bg_fg_cv_.SignalAll();
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (unsigned level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactOnce() {
	printf("Signalling background compaction from CompactOnce . . \n");
	bg_compaction_cv_.Signal();
	Env::Default()->SleepForMicroseconds(10000000);
}

void DBImpl::TEST_CompactAllLevels() {
	while (!versions_->IsAllLevelsCompacted()) {
		bool force_compact;
		int level = versions_->PickCompactionLevel(levels_locked_, false, &force_compact);
		printf("Level picked for compaction - %d\n", level);
		printf("Signalling background compaction compactAllLevels. . \n");
		bg_compaction_cv_.Signal();
		printf("Sleeping for 5 seconds . . .");
		env_->SleepForMicroseconds(5000000);
	}
}

void DBImpl::TEST_CompactRange(unsigned level, const Slice* begin,const Slice* end) {
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      bg_compaction_cv_.Signal();
      bg_memtable_cv_.Signal();
    } else {  // Running either my compaction or another compaction.
      bg_fg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

void DBImpl::TEST_ComapactFilesToSingleLevel() {
	while (true) {
		int min_level, max_level;
		double max_level_compaction_score;
		printf("Current version state -- %s\n", versions_->current()->DebugString().c_str());
		int num_active_levels = versions_->current()->GetNumLevelsWithFiles(&min_level, &max_level, &max_level_compaction_score);
		printf("Num active levels: %d min_level: %d max_level: %d\n", num_active_levels, min_level, max_level);
		if (num_active_levels <= 1 && max_level_compaction_score <= 1.0) {
			break;
		}

		versions_->HACK_IncreaseCompactionScoresForLevel(min_level);
		printf("Signalling background compaction compact files to single level . . \n");

		bg_compaction_cv_.Signal();
		printf("Sleeping for 10 seconds . . .\n");
		env_->SleepForMicroseconds(10000000);
	}
}

void DBImpl::TEST_ReduceNumActiveLevelsByOne() {
	int min_level, max_level;
	double max_level_compaction_score;
	int num_active_levels = versions_->current()->GetNumLevelsWithFiles(&min_level, &max_level, &max_level_compaction_score);
	if (num_active_levels <= 1) {
		return;
	}
	while (true) {
		versions_->HACK_IncreaseCompactionScoresForLevel(min_level);
		printf("Signalling background compaction - reduce num levels by 1 . . \n");
		bg_compaction_cv_.Signal();
		printf("Waiting for 20 seconds . . .\n");
		env_->SleepForMicroseconds(20000000);
		int new_active_levels = versions_->current()->GetNumLevelsWithFiles(&min_level, &max_level, &max_level_compaction_score);
		printf("Num active levels: %d new_active_levels: %d min_level: %d max_level: %d\n", num_active_levels, new_active_levels, min_level, max_level);
		if (num_active_levels <= 1 || new_active_levels <= num_active_levels-1) {
			break;
		}
	}
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_fg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::CompactLevelThread() {
  MutexLock l(&mutex_);
  FileLevelFilterBuilder file_level_filter_builder(options_.filter_policy);

  while (!shutting_down_.Acquire_Load() && !allow_background_activity_) {
    bg_compaction_cv_.Wait();
  }
  while (!shutting_down_.Acquire_Load()) {
    while (!shutting_down_.Acquire_Load() &&
           manual_compaction_ == NULL &&
           !versions_->NeedsCompaction(levels_locked_, straight_reads_ > kStraightReads)) {
      bg_compaction_cv_.Wait();
    }
    if (shutting_down_.Acquire_Load()) {
      break;
    }

    assert(manual_compaction_ == NULL || num_bg_threads_ == 2);

    start_timer_simple(TOTAL_BACKGROUND_COMPACTION);
    start_timer(TOTAL_BACKGROUND_COMPACTION);
    Status s = BackgroundCompactionGuards(&file_level_filter_builder);

    record_timer(TOTAL_BACKGROUND_COMPACTION);
    record_timer_simple(TOTAL_BACKGROUND_COMPACTION);

    bg_fg_cv_.SignalAll(); // before the backoff In case a waiter
                           // can proceed despite the error

    if (s.ok()) {
      // Success
    } else if (shutting_down_.Acquire_Load()) {
      // Error most likely due to shutdown; do not wait
    } else {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      Log(options_.info_log, "Waiting after background compaction error: %s",
          s.ToString().c_str());
      mutex_.Unlock();
      int seconds_to_sleep = 1;
      env_->SleepForMicroseconds(seconds_to_sleep * 1000000);
      mutex_.Lock();
    }
  }
  Log(options_.info_log, "cleaning up CompactLevelThread");
  num_bg_threads_ -= 1;
  bg_fg_cv_.SignalAll();
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_fg_cv_.SignalAll();
  }
}

Status DBImpl::BackgroundCompactionGuards(FileLevelFilterBuilder* file_level_filter_builder) {
  int x, y, z;
  mutex_.AssertHeld();
  bool force_compact;
  Compaction* c = NULL;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  std::vector<GuardMetaData*> complete_guards_used_in_bg_compaction;
  if (is_manual) {
	// TODO Handle CompactRange method for guards
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      // TODO not true in case of guard design
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {

	start_timer(BGC_PICK_COMPACTION_LEVEL);
    unsigned level = versions_->PickCompactionLevel(levels_locked_, straight_reads_ > kStraightReads, &force_compact);
    record_timer(BGC_PICK_COMPACTION_LEVEL);

    start_timer(BGC_PICK_COMPACTION);
    if (level != config::kNumLevels) {
      c = versions_->PickCompactionForGuards(versions_->current(), level, &complete_guards_used_in_bg_compaction, force_compact);
    }
    record_timer(BGC_PICK_COMPACTION);

    if (c) {
      assert(!levels_locked_[c->level() + 0]);
      if (c->level() + 1 < config::kNumLevels) {
    	  assert(!levels_locked_[c->level() + 1]);
    	  levels_locked_[c->level() + 1] = true;
      }
      levels_locked_[c->level() + 0] = true;
    }
  }

  Status status;

  if (c == NULL) {
//	status = Status::OK();
    // Nothing to do
  } else {
    CompactionState* compact = new CompactionState(c);

    // Add the guard information in current version to edit (primarily to add the new guards in complete_guards_)
    std::set<int> level_to_load_from_complete_guards;
    // NOTE: The complete guards are copied to the guards only during background compaction and not during manual
    // compaction. It is debatable and needs code change while picking compaction if this needs to be enabled.
    if (!is_manual) {
		level_to_load_from_complete_guards.insert(c->level());
		if (!c->is_horizontal_compaction) {
			level_to_load_from_complete_guards.insert(c->level() + 1);
		}
    }
    start_timer(BGC_ADD_GUARDS_TO_EDIT);
    versions_->current()->AddGuardsToEdit(compact->compaction->edit(), level_to_load_from_complete_guards);
    record_timer(BGC_ADD_GUARDS_TO_EDIT);

    start_timer(BGC_DO_COMPACTION_WORK_GUARDS);
    status = DoCompactionWorkGuards(compact, complete_guards_used_in_bg_compaction, file_level_filter_builder);
    record_timer(BGC_DO_COMPACTION_WORK_GUARDS);

    if (!status.ok()) {
      RecordBackgroundError(status);
    }

    start_timer(BGC_CLEANUP_COMPACTION);
    CleanupCompaction(compact);
    c->ReleaseInputs();
    DeleteObsoleteFiles();
    record_timer(BGC_CLEANUP_COMPACTION);
  }

  if (c) {
    levels_locked_[c->level() + 0] = false;
    levels_locked_[c->level() + 1] = false;
    delete c;
  }

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  // TODO see what is to be done here
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
  return status;
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input, FileLevelFilterBuilder* file_level_filter_builder,
										  std::vector<uint64_t>* file_numbers,
										  std::vector<std::string*>* file_level_filters) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }

  const uint64_t current_bytes = compact->builder->FileSize();
  InternalKey smallest = compact->current_output()->smallest;
  InternalKey largest = compact->current_output()->largest;
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;

#ifdef FILE_LEVEL_FILTER
  // Populate file level filter information to in-memory map
  if (options_.filter_policy != NULL) {
	  std::string* filter_string = file_level_filter_builder->GenerateFilter();
	  assert(filter_string != NULL);
	  file_level_filters->push_back(filter_string);
	  file_level_filter_builder->Clear();
  }
#endif

  file_numbers->push_back(output_number);
  table_cache_->SetFileMetaDataMap(output_number, current_bytes, smallest, largest);

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact, const int level_to_add_new_files,
		std::vector<uint64_t> file_numbers, std::vector<std::string*> file_level_filters) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %lu@%d + %lu@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // Add compaction outputs
  compact->compaction->AddInputDeletions(compact->compaction->edit());
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(
        level_to_add_new_files,
        out.number, out.file_size, out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_, &bg_log_cv_, &bg_log_occupied_, file_numbers, file_level_filters, 0);
}

Status DBImpl::DoCompactionWorkGuards(CompactionState* compact,
		std::vector<GuardMetaData*> complete_guards_used_in_bg_compaction,
		FileLevelFilterBuilder* file_level_filter_builder) {

  if (compact->compaction->num_input_files(0) == 0 && compact->compaction->num_input_files(1) == 0) {
	  return Status::OK();
  }

  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions
  std::vector<uint64_t> file_numbers;
  std::vector<std::string*> file_level_filters;
  int total_input_files = 0, current_level_input_files = 0, next_level_input_files = 0, total_output_files = 0;
  uint64_t total_input_data_size = 0, current_level_input_data_size = 0, next_level_input_data_size = 0, total_output_data_size = 0;

  Log(options_.info_log,  "Compacting %lu@%d + %lu@%d files for guards in a level",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }
  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();
  // In case some other level needs to be compacted and some thread is waiting.
  bg_compaction_cv_.Signal();

  start_timer(BGC_MAKE_INPUT_ITERATOR);
  Iterator* input = versions_->MakeInputIteratorForGuardsInALevel(compact->compaction);
  record_timer(BGC_MAKE_INPUT_ITERATOR);

  int index = 0;

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  ParsedInternalKey current_key;
  std::string current_key_backing;
  bool has_current_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  size_t boundary_hint = 0;
  std::vector<GuardMetaData*> guards;
  int compaction_level = compact->compaction->level();

  // If the compaction level is the last level, get the guards of the same level
  // Else, get the guards of the next level since the new files will be populated to next level
  guards = complete_guards_used_in_bg_compaction;
  unsigned num_guards = guards.size(), current_guard = 0;

  start_timer(BGC_ITERATE_KEYS_AND_SPLIT);
  InternalKey prev;
  bool first_entry = true;
  int cnt = 0;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
	Slice key = input->key();
	cnt++;
    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_key_backing.clear();
      has_current_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_key ||
          user_comparator()->Compare(ikey.user_key,
                                     current_key.user_key) != 0) {
        if (has_current_key && compact->builder &&
            compact->builder->FileSize() >=
            compact->compaction->MinOutputFileSize() &&
            compact->compaction->CrossesBoundary(current_key, ikey, &boundary_hint)) {
          start_timer(BGC_FINISH_COMPACTION_OUTPUT_FILE);
          status = FinishCompactionOutputFile(compact, input, file_level_filter_builder, &file_numbers, &file_level_filters);
          index = 0;
          record_timer(BGC_FINISH_COMPACTION_OUTPUT_FILE);
          if (!status.ok()) {
            break;
          }
        }
        // First occurrence of this user key
        current_key_backing.assign(key.data(), key.size());
        bool x = ParseInternalKey(Slice(current_key_backing), &current_key);
        assert(x);
        has_current_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      // Just remember that last_sequence_for_key is decreasing over time, and
      // all of this makes sense.

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      // If we're going to drop this key, and there was no previous version of
      // this key, and it was written at or after the garbage cutoff, we keep
      // it.
      if (drop &&
          last_sequence_for_key == kMaxSequenceNumber  &&
          ikey.sequence >= manual_garbage_cutoff_) {
        drop = false;
      }

      last_sequence_for_key = ikey.sequence;
    }

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
    	start_timer(BGC_OPEN_COMPACTION_OUTPUT_FILE);
        status = OpenCompactionOutputFile(compact);
        record_timer(BGC_OPEN_COMPACTION_OUTPUT_FILE);
        if (!status.ok()) {
          break;
        }
      }

      // To split the files based on the guard keys
      InternalKey current_ikey;
      current_ikey.DecodeFrom(key);
      if (current_guard < guards.size()
    		  && user_comparator()->Compare(current_ikey.user_key(), guards[current_guard]->guard_key.user_key()) >= 0) {
    	  unsigned temp = current_guard;
    	  while (temp < guards.size()
    		  && user_comparator()->Compare(current_ikey.user_key(), guards[temp]->guard_key.user_key()) >= 0) {
    		  temp++;
    	  }
          if (compact->builder->NumEntries() > 0) {
              start_timer(BGC_FINISH_COMPACTION_OUTPUT_FILE);
        	  status = FinishCompactionOutputFile(compact, input, file_level_filter_builder, &file_numbers, &file_level_filters);
        	  index = 0;
              record_timer(BGC_FINISH_COMPACTION_OUTPUT_FILE);
          }
          current_guard = temp;
          if (!status.ok()) {
            break;
          }
      }

      // Open output file again in case the file was closed after reaching the guard  limit
      if (compact->builder == NULL) {
      	start_timer(BGC_OPEN_COMPACTION_OUTPUT_FILE);
        status = OpenCompactionOutputFile(compact);
        record_timer(BGC_OPEN_COMPACTION_OUTPUT_FILE);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

#ifdef FILE_LEVEL_FILTER
      file_level_filter_builder->AddKey(key);
#endif

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        start_timer(BGC_FINISH_COMPACTION_OUTPUT_FILE);
        status = FinishCompactionOutputFile(compact, input, file_level_filter_builder, &file_numbers, &file_level_filters);
        index = 0;
        record_timer(BGC_FINISH_COMPACTION_OUTPUT_FILE);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    start_timer(BGC_FINISH_COMPACTION_OUTPUT_FILE);
    status = FinishCompactionOutputFile(compact, input, file_level_filter_builder, &file_numbers, &file_level_filters);
    index = 0;
    record_timer(BGC_FINISH_COMPACTION_OUTPUT_FILE);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  record_timer(BGC_ITERATE_KEYS_AND_SPLIT);

  current_level_input_files = compact->compaction->num_input_files(0);
  next_level_input_files = compact->compaction->num_input_files(1);
  total_input_files = current_level_input_files + next_level_input_files;

  start_timer(BGC_COLLECT_STATS);
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
      if (which == 0) {
    	  current_level_input_data_size += compact->compaction->input(which, i)->file_size;
      } else {
    	  next_level_input_data_size += compact->compaction->input(which, i)->file_size;
      }
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
    total_output_data_size += compact->outputs[i].file_size;
  }
  total_output_files = compact->outputs.size();

  record_timer(BGC_COLLECT_STATS);

  start_timer(BGC_GET_LOCK_BEFORE_INSTALL);
  mutex_.Lock();
  record_timer(BGC_GET_LOCK_BEFORE_INSTALL);

  int level_written_to = compaction_level;

  if (!compact->compaction->is_horizontal_compaction) {
	  level_written_to = compaction_level + 1;
  }
  stats_[level_written_to].Add(stats);

  start_timer(BGC_INSTALL_COMPACTION_RESULTS);
  if (status.ok()) {
	  status = InstallCompactionResults(compact, level_written_to, file_numbers, file_level_filters);
  }
  record_timer(BGC_INSTALL_COMPACTION_RESULTS);

  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* /*arg2*/) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options, uint64_t number,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed, bool external_sync) {
  IterState* cleanup = new IterState;
  if (!external_sync) {
    mutex_.Lock();
  }
  ++straight_reads_;
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddSomeIteratorsGuards(options, number, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size(), versions_);
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  if (!external_sync) {
    mutex_.Unlock();
  }
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), 0, &ignored, &ignored_seed, false);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  start_timer_simple(GET_OVERALL_TIME);
  start_timer(GET_OVERALL_TIME);
  start_timer(GET_TIME_TO_GET_MUTEX);
  MutexLock l(&mutex_);
  record_timer(GET_TIME_TO_GET_MUTEX);

  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) {
	  imm->Ref();
  }
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    start_timer(GET_TIME_TO_CHECK_MEM_IMM);
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      record_timer(GET_TIME_TO_CHECK_MEM_IMM);

      start_timer(GET_TIME_TO_CHECK_VERSION);
      s = current->Get(options, lkey, value, &stats);
      total_files_read += current->num_files_read;
      record_timer(GET_TIME_TO_CHECK_VERSION);

      have_stat_update = true;
    }

    start_timer(GET_TIME_TO_LOCK_MUTEX);
    mutex_.Lock();
    record_timer(GET_TIME_TO_LOCK_MUTEX);
  }

  start_timer(GET_TIME_TO_FINISH_UNREF);
  if (have_stat_update && current->UpdateStats(stats)) {
    bg_compaction_cv_.Signal();
  }
  //Disable compaction on continous read(Get) requests. COmpaction is triggered
  //only for contiguous seeks. 
  //++straight_reads_;
  mem->Unref();
  if (imm != NULL) {
	 imm->Unref();
  }
  current->Unref();
  record_timer(GET_TIME_TO_FINISH_UNREF);
  record_timer(GET_OVERALL_TIME);
  record_timer_simple(GET_OVERALL_TIME);
  return s;
}

Status DBImpl::GetCurrentVersionState(std::string* value) {
	if (versions_ == NULL) {
		printf("versions_ is NULL !!\n");
		return Status::OK();
	}
	std::string version_debug_string = versions_->GetCurrentVersionState();
	*value = version_debug_string;
	return Status::OK();
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, 0, &latest_snapshot, &seed, false);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

void DBImpl::GetReplayTimestamp(std::string* timestamp) {
  uint64_t file = 0;
  uint64_t seqno = 0;

  {
    MutexLock l(&mutex_);
    file = versions_->NewFileNumber();
    versions_->ReuseFileNumber(file);
    seqno = versions_->LastSequence();
  }

  WaitOutWriters();
  timestamp->clear();
  PutVarint64(timestamp, file);
  PutVarint64(timestamp, seqno);
}

void DBImpl::AllowGarbageCollectBeforeTimestamp(const std::string& timestamp) {
  Slice ts_slice(timestamp);
  uint64_t file = 0;
  uint64_t seqno = 0;

  if (timestamp == "all") {
    // keep zeroes
  } else if (timestamp == "now") {
    MutexLock l(&mutex_);
    seqno = versions_->LastSequence();
    if (manual_garbage_cutoff_ < seqno) {
      manual_garbage_cutoff_ = seqno;
    }
  } else if (GetVarint64(&ts_slice, &file) &&
             GetVarint64(&ts_slice, &seqno)) {
    MutexLock l(&mutex_);
    if (manual_garbage_cutoff_ < seqno) {
      manual_garbage_cutoff_ = seqno;
    }
  }
}

bool DBImpl::ValidateTimestamp(const std::string& ts) {
  uint64_t file = 0;
  uint64_t seqno = 0;
  Slice ts_slice(ts);
  return ts == "all" || ts == "now" ||
         (GetVarint64(&ts_slice, &file) &&
          GetVarint64(&ts_slice, &seqno));
}

int DBImpl::CompareTimestamps(const std::string& lhs, const std::string& rhs) {
  uint64_t now = 0;
  uint64_t lhs_seqno = 0;
  uint64_t rhs_seqno = 0;
  uint64_t tmp;
  if (lhs == "now" || rhs == "now") {
    MutexLock l(&mutex_);
    now = versions_->LastSequence();
  }
  if (lhs == "all") {
    lhs_seqno = 0;
  } else if (lhs == "now") {
    lhs_seqno = now;
  } else {
    Slice lhs_slice(lhs);
    GetVarint64(&lhs_slice, &tmp);
    GetVarint64(&lhs_slice, &lhs_seqno);
  }
  if (rhs == "all") {
    rhs_seqno = 0;
  } else if (rhs == "now") {
    rhs_seqno = now;
  } else {
    Slice rhs_slice(rhs);
    GetVarint64(&rhs_slice, &tmp);
    GetVarint64(&rhs_slice, &rhs_seqno);
  }

  if (lhs_seqno < rhs_seqno) {
    return -1;
  } else if (lhs_seqno > rhs_seqno) {
    return 1;
  } else {
    return 0;
  }
}

Status DBImpl::GetReplayIterator(const std::string& timestamp,
                                 ReplayIterator** iter) {
  *iter = NULL;
  Slice ts_slice(timestamp);
  uint64_t file = 0;
  uint64_t seqno = 0;

  if (timestamp == "all") {
    seqno = 0;
  } else if (timestamp == "now") {
    {
      MutexLock l(&mutex_);
      file = versions_->NewFileNumber();
      versions_->ReuseFileNumber(file);
      seqno = versions_->LastSequence();
    }
    WaitOutWriters();
  } else if (!GetVarint64(&ts_slice, &file) ||
             !GetVarint64(&ts_slice, &seqno)) {
    return Status::InvalidArgument("Timestamp is not valid");
  }

  ReadOptions options;
  options.fill_cache = false;
  SequenceNumber latest_snapshot;
  uint32_t seed;
  MutexLock l(&mutex_);
  Iterator* internal_iter = NewInternalIterator(options, file, &latest_snapshot, &seed, true);
  internal_iter->SeekToFirst();
  ReplayIteratorImpl* iterimpl;
  iterimpl = new ReplayIteratorImpl(
      this, &mutex_, user_comparator(), internal_iter, mem_, SequenceNumber(seqno));
  *iter = iterimpl;
  replay_iters_.push_back(iterimpl);
  return Status::OK();
}

void DBImpl::ReleaseReplayIterator(ReplayIterator* _iter) {
  MutexLock l(&mutex_);
  ReplayIteratorImpl* iter = reinterpret_cast<ReplayIteratorImpl*>(_iter);
  for (std::list<ReplayIteratorImpl*>::iterator it = replay_iters_.begin();
      it != replay_iters_.end(); ++it) {
    if (*it == iter) {
      iter->cleanup(); // calls delete
      replay_iters_.erase(it);
      return;
    }
  }
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  ++straight_reads_;
  if (versions_->current()->RecordReadSample(key)) {
    bg_compaction_cv_.Signal();
  }
}

SequenceNumber DBImpl::LastSequence() {
  SequenceNumber ret;

  {
    MutexLock l(&mutex_);
    ret = versions_->LastSequence();
  }

  WaitOutWriters();
  return ret;
}

const Snapshot* DBImpl::GetSnapshot() {
  const Snapshot* ret;

  {
    MutexLock l(&mutex_);
    ret = snapshots_.New(versions_->LastSequence());
  }

  WaitOutWriters();
  return ret;
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&writers_mutex_);
  Status s;

  start_timer_simple(WRITE_OVERALL_TIME);
  start_timer(WRITE_OVERALL_TIME);
  start_timer(WRITE_SEQUENCE_WRITE_BEGIN_TOTAL);
  s = SequenceWriteBegin(&w, updates);
  record_timer(WRITE_SEQUENCE_WRITE_BEGIN_TOTAL);

  WriteBatch* updates_with_guards = NULL;
  if (s.ok() && updates != NULL) { // NULL batch is for compactions

    start_timer(WRITE_SET_SEQUENCE_CREATE_NEW_BATCH);
    WriteBatchInternal::SetSequence(updates, w.start_sequence_);

    /* Create a new WriteBatch which includes the guards. */
    updates_with_guards = new WriteBatch(*updates);
    record_timer(WRITE_SET_SEQUENCE_CREATE_NEW_BATCH);

    /* This step adds guards to the new WriteBatch, which can then be
       used to generate new serialized contents. */
    start_timer(WRITE_SET_GUARDS);
    s = WriteBatchInternal::SetGuards(updates, updates_with_guards);
    record_timer(WRITE_SET_GUARDS);

    if (!s.ok()) {
      printf("Something went wrong with set guards\n");
      assert(0);
    }

    // Add to log and apply to memtable.  We do this without holding the lock
    // because both the log and the memtable are safe for concurrent access.
    // The synchronization with readers occurs with SequenceWriteEnd.
    start_timer(WRITE_LOG_ADDRECORD);
    s = w.log_->AddRecord(WriteBatchInternal::Contents(updates_with_guards));
    record_timer(WRITE_LOG_ADDRECORD);

    if (!s.ok()) {
      printf("Problem writing log!");
      assert(0);
    }
    
    if (s.ok() && options.sync) {
      start_timer(WRITE_LOG_FILE_SYNC);
      s = w.logfile_->Sync();
      record_timer(WRITE_LOG_FILE_SYNC);
    }
  }

  start_timer(WRITE_SEQUENCE_WRITE_END_TOTAL);
  SequenceWriteEnd(&w, updates_with_guards, s);
  record_timer(WRITE_SEQUENCE_WRITE_END_TOTAL);
  record_timer(WRITE_OVERALL_TIME);
  record_timer_simple(WRITE_OVERALL_TIME);

  delete updates_with_guards;

  return s;
}

Status DBImpl::SequenceWriteBegin(Writer* w, WriteBatch* updates) {
  Status s;

  {
	start_timer(SWB_INIT_MUTEX);
    MutexLock l(&mutex_);
    record_timer(SWB_INIT_MUTEX);

    straight_reads_ = 0;
    bool force = updates == NULL;
    bool enqueue_mem = false;
    w->micros_ = versions_->NumLevelFiles(0);

    start_timer(SWB_INIT_MEMTABLES);
    while (true) {
      if (!bg_error_.ok()) {
        // Yield previous error
        s = bg_error_;
        break;
      } else if (!force &&
                 (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
        // There is room in current memtable
        // Note that this is a sloppy check.  We can overfill a memtable by the
        // amount of concurrently written data.
        break;
      } else if (imm_ != NULL) {
        // We have filled up the current memtable, but the previous
        // one is still being compacted, so we wait.

        bg_memtable_cv_.Signal();
        bg_fg_cv_.Wait();
      } else {
        // Attempt to switch to a new memtable and trigger compaction of old
        assert(versions_->PrevLogNumber() == 0);
        uint64_t new_log_number = versions_->NewFileNumber();
        ConcurrentWritableFile* lfile = NULL;
        s = env_->NewConcurrentWritableFile(LogFileName(dbname_, new_log_number), &lfile);
        if (!s.ok()) {
          // Avoid chewing through file number space in a tight loop.
          versions_->ReuseFileNumber(new_log_number);
          break;
        }
        logfile_.reset(lfile);
        logfile_number_ = new_log_number;
        log_.reset(new log::Writer(lfile));
        imm_ = mem_;
        w->has_imm_ = true;
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
        force = false;   // Do not force another compaction if have room
        enqueue_mem = true;
        break;
      }
    }
    record_timer(SWB_INIT_MEMTABLES);

    start_timer(SWB_SET_LOG_DETAILS);
    if (s.ok()) {
      w->log_ = log_;
      w->logfile_ = logfile_;
      // HACK! To solve memtable concurrency issue
//      w->mem_ = mem_;
//      mem_->Ref();
    }
    record_timer(SWB_SET_LOG_DETAILS);

    start_timer(SWB_ENQUEUE_MEM);
    if (enqueue_mem) {
      for (std::list<ReplayIteratorImpl*>::iterator it = replay_iters_.begin();
          it != replay_iters_.end(); ++it) {
        (*it)->enqueue(mem_, w->start_sequence_);
      }
    }
    record_timer(SWB_ENQUEUE_MEM);
  }

  if (s.ok()) {
	start_timer(SWB_SET_TAIL);
    uint64_t diff = updates ? WriteBatchInternal::Count(updates) : 0;
    uint64_t ticket = 0;
    w->linked_ = true;
    w->next_ = NULL;

    writers_mutex_.Lock();
    if (writers_tail_) {
      writers_tail_->next_ = w;
      w->prev_ = writers_tail_;
    }
    record_timer(SWB_SET_TAIL);

    start_timer(SWB_SYNC_AND_FETCH);
    writers_tail_ = w;
    ticket = __sync_add_and_fetch(&writers_upper_, 1 + diff);
    while (w->block_if_backup_in_progress_ &&
           backup_in_progress_.Acquire_Load()) {
      w->wake_me_when_head_ = true;
      w->cv_.Wait();
      w->wake_me_when_head_ = false;
    }
    record_timer(SWB_SYNC_AND_FETCH);

    writers_mutex_.Unlock();
    w->start_sequence_ = ticket - diff;
    w->end_sequence_ = ticket;
  }

  return s;
}

void DBImpl::SequenceWriteEnd(Writer* w, WriteBatch* updates_with_guards, Status s) {
  int a;
  if (!w->linked_) {
    return;
  }

  start_timer(SWE_LOCK_MUTEX);
  mutex_.Lock();
  record_timer(SWE_LOCK_MUTEX);

  // HACK! Using current mem_ instead of w->mem_
  mem_->Ref();
  if (s.ok() && updates_with_guards != NULL) {
	start_timer(WRITE_INSERT_INTO_VERSION);
	s = WriteBatchInternal::InsertIntoVersion(updates_with_guards,
					mem_, versions_->current());
	record_timer(WRITE_INSERT_INTO_VERSION);
  }
  if (!s.ok()) {
	RecordBackgroundError(s);
  }

  versions_->SetLastSequence(w->end_sequence_);
  mem_->Unref();
  mutex_.Unlock();

  start_timer(SWE_LOCK_WRITERS_MUTEX);
  writers_mutex_.Lock();
  if (w->prev_) {
    w->prev_->next_ = w->next_;
    if (w->has_imm_) {
      w->prev_->has_imm_ = true;
      w->has_imm_ = false;
    }
  }
  record_timer(SWE_LOCK_WRITERS_MUTEX);

  start_timer(SWE_SET_NEXT);
  if (w->next_) {
    w->next_->prev_ = w->prev_;
    // if we're the head and we're setting someone else to be the head who wants
    // to be notified when they become the head, signal them.
    if (w->next_->wake_me_when_head_ && !w->prev_) {
      w->next_->cv_.Signal();
    }
  }
  record_timer(SWE_SET_NEXT);

  start_timer(SWE_UNLOCK_WRITERS_MUTEX);
  if (writers_tail_ == w) {
    assert(!w->next_);
    writers_tail_ = NULL;
  }
  writers_mutex_.Unlock();
  record_timer(SWE_UNLOCK_WRITERS_MUTEX);

  start_timer(SWE_SET_IMM);
  if (w->has_imm_ && !w->prev_) {
    mutex_.Lock();
    has_imm_.Release_Store(imm_);
    w->has_imm_ = false;
    bg_memtable_cv_.Signal();
    mutex_.Unlock();
  }
  record_timer(SWE_SET_IMM);

  if (w->micros_ > config::kL0_SlowdownWritesTrigger) {
    start_timer(SWE_SLEEP);
	env_->SleepForMicroseconds(w->micros_ - config::kL0_SlowdownWritesTrigger);
    record_timer(SWE_SLEEP);
  }
}

void DBImpl::WaitOutWriters() {
  Writer w(&writers_mutex_);
  writers_mutex_.Lock();
  if (writers_tail_) {
    writers_tail_->next_ = &w;
    w.prev_ = writers_tail_;
  }
  writers_tail_ = &w;
  w.linked_ = true;
  w.next_ = NULL;
  while (w.prev_) {
    w.wake_me_when_head_ = true;
    w.cv_.Wait();
  }
  assert(!w.prev_);
  if (w.next_) {
    w.next_->prev_ = NULL;
    // if we're the head and we're setting someone else to be the head who wants
    // to be notified when they become the head, signal them.
    if (w.next_->wake_me_when_head_) {
      w.next_->cv_.Signal();
    }
  }
  if (writers_tail_ == &w) {
    assert(!w.next_);
    writers_tail_ = NULL;
  }
  writers_mutex_.Unlock();

  if (w.has_imm_) {
    mutex_.Lock();
    has_imm_.Release_Store(imm_);
    w.has_imm_ = false;
    bg_memtable_cv_.Signal();
    mutex_.Unlock();
  }
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in.starts_with("num-guards-at-level")) {
    in.remove_prefix(strlen("num-guards-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      /* Get from versions */
      int num_guards = versions_->NumGuards(level);
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
	       num_guards);
      *value = buf;
      return true;
    }
  } else if (in.starts_with("num-guard-files-at-level")) {
	  in.remove_prefix(strlen("num-guard-files-at-level"));
	  uint64_t level;
	  bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
	  if (!ok || level >= config::kNumLevels) {
		  return false;
	  }

	  // Get the number of guard files from version
	  int num_guard_files = versions_->NumGuardFiles(level);
	  char buf[100];
	  snprintf(buf, sizeof(buf), "%d", num_guard_files);
	  *value = buf;
	  return true;
  } else if (in.starts_with("num-sentinel-files-at-level")) {
	  in.remove_prefix(strlen("num-sentinel-files-at-level"));
	  uint64_t level;
	  bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
	  if (!ok || level >= config::kNumLevels) {
		return false;
	  }

	  /* Get from versions */
	  int num_sentinel_files = versions_->NumSentinelFiles(level);
	  char buf[100];
	  snprintf(buf, sizeof(buf), "%d", num_sentinel_files);
	  *value = buf;
	  return true;
  } else if (in.starts_with("guard-details-at-level")) {
	  in.remove_prefix(strlen("guard-details-at-level"));
	  uint64_t level;
	  bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
	  if (!ok || level >= config::kNumLevels) {
		  return false;
	  }

	  // Get guards and the files belonging to each guard from versions
	  std::string guard_details = versions_->GuardDetailsAtLevel(level);
	  *value = guard_details.c_str();
	  return true;
  } else if (in.starts_with("sentinel-details-at-level")) {
	  in.remove_prefix(strlen("sentinel-details-at-level"));
	  uint64_t level;
	  bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
	  if (!ok || level >= config::kNumLevels) {
		  return false;
	  }

	  // Get sentinel deatils from the versions
	  std::string sentinel_details = versions_->SentinelDetailsAtLevel(level);
	  *value = sentinel_details.c_str();
	  return true;
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (unsigned level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

Status DBImpl::LiveBackup(const Slice& _name) {
  Slice name = _name;
  size_t name_sz = 0;

  for (; name_sz < name.size() && name.data()[name_sz] != '\0'; ++name_sz)
      ;

  name = Slice(name.data(), name_sz);
  std::set<uint64_t> live;

  {
    MutexLock l(&writers_mutex_);
    backup_in_progress_.Release_Store(this);
    while (backup_waiter_has_it_) {
      ++backup_waiters_;
      backup_cv_.Wait();
      --backup_waiters_;
    }
    backup_waiter_has_it_ = true;
  }

  Writer w(&writers_mutex_);
  w.block_if_backup_in_progress_ = false;
  SequenceWriteBegin(&w, NULL);

  {
    MutexLock l(&writers_mutex_);
    Writer* p = &w;
    while (p->prev_) {
      p = p->prev_;
    }
    while (p != &w) {
      assert(p);
      p->block_if_backup_in_progress_ = false;
      p->cv_.Signal();
      p = p->next_;
    }
    while (w.prev_) {
      w.wake_me_when_head_ = true;
      w.cv_.Wait();
    }
  }

  {
    MutexLock l(&mutex_);
    versions_->SetLastSequence(w.end_sequence_);
    while (bg_log_occupied_) {
      bg_log_cv_.Wait();
    }
    bg_log_occupied_ = true;
    // note that this logic assumes that DeleteObsoleteFiles never releases
    // mutex_, so that once we release at this brace, we'll guarantee that it
    // will see backup_in_progress_.  If you change DeleteObsoleteFiles to
    // release mutex_, you'll need to add some sort of synchronization in place
    // of this text block.
    versions_->AddLiveFiles(&live);
  }

  Status s;
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  std::string backup_dir = dbname_ + "/backup-" + name.ToString() + "/";

  if (s.ok()) {
    s = env_->CreateDir(backup_dir);
  }

  uint64_t number;
  FileType type;

  for (size_t i = 0; i < filenames.size(); i++) {
    if (!s.ok()) {
      continue;
    }
    if (ParseFileName(filenames[i], &number, &type)) {
      std::string src = dbname_ + "/" + filenames[i];
      std::string target = backup_dir + "/" + filenames[i];
      switch (type) {
        case kLogFile:
        case kDescriptorFile:
        case kCurrentFile:
        case kInfoLogFile:
          s = env_->CopyFile(src, target);
          break;
        case kTableFile:
          // If it's a file referenced by a version, we have logged that version
          // and applied it.  Our MANIFEST will reflect that, and the file
          // number assigned to new files will be greater or equal, ensuring
          // that they aren't overwritten.  Any file not in "live" either exists
          // past the current manifest (output of ongoing compaction) or so far
          // in the past we don't care (we're going to delete it at the end of
          // this backup).  I'd rather play safe than sorry.
          //
          // Under no circumstances should you collapse this to a single
          // LinkFile without the conditional as it has implications for backups
          // that share hardlinks.  Opening an older backup that has files
          // hardlinked with newer backups will overwrite "immutable" files in
          // the newer backups because they aren't in our manifest, and we do an
          // open/write rather than a creat/rename.  We avoid linking these
          // files.
          if (live.find(number) != live.end()) {
            s = env_->LinkFile(src, target);
          }
          break;
        case kTempFile:
        case kDBLockFile:
          break;
        default:
          break;
      }
    }
  }

  {
    MutexLock l(&mutex_);
    if (s.ok() && backup_deferred_delete_) {
      DeleteObsoleteFiles();
    }
    backup_deferred_delete_ = false;
    bg_log_occupied_ = false;
    bg_log_cv_.Signal();
  }

  {
    MutexLock l(&writers_mutex_);
    backup_waiter_has_it_ = false;
    if (backup_waiters_ > 0) {
      backup_in_progress_.Release_Store(this);
      backup_cv_.Signal();
    } else {
      backup_in_progress_.Release_Store(NULL);
    }
  }

  SequenceWriteEnd(&w, NULL, Status::OK());
  return s;
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;
  DBImpl* impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();

  VersionEdit edit;
  Status s = impl->Recover(&edit); // Handles create_if_missing, error_if_exists

  if (s.ok()) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    ConcurrentWritableFile* lfile;
    s = options.env->NewConcurrentWritableFile(LogFileName(dbname, new_log_number),
                                               &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_.reset(lfile);
      impl->logfile_number_ = new_log_number;
      impl->log_.reset(new log::Writer(lfile));
      s = impl->versions_->LogAndApply(&edit, &impl->mutex_, &impl->bg_log_cv_, &impl->bg_log_occupied_, std::vector<uint64_t>(), std::vector<std::string*>(), 1);
    }
    if (s.ok()) {
      impl->DeleteObsoleteFiles();
      impl->bg_compaction_cv_.Signal();
      impl->bg_memtable_cv_.Signal();
    }
  }

  // Populate the file level bloom filter at the start of the database
  // TODO: Optimize this by storing the filter values in file during shutdown and just reading them during open or
  // store file level bloom filter for every file along with the index block
  uint64_t before = Env::Default()->NowMicros();
  impl->versions_->InitializeFileLevelBloomFilter();
  uint64_t after = Env::Default()->NowMicros();

  impl->versions_->InitializeTableCacheFileMetaData();
  before = Env::Default()->NowMicros();

  impl->pending_outputs_.clear();
  impl->allow_background_activity_ = true;
  impl->bg_compaction_cv_.SignalAll();
  impl->bg_memtable_cv_.SignalAll();
  impl->mutex_.Unlock();

  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
    impl = NULL;
  }
  if (impl) {
    impl->writers_mutex_.Lock();
    impl->writers_upper_ = impl->versions_->LastSequence();
    impl->writers_mutex_.Unlock();
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb


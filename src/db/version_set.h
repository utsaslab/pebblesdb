// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <unordered_map>
#include <set>
#include <vector>
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/timer.h"
#include "db/table_cache.h"
#include "table/iterator_wrapper.h"
#include "table/filter_block.h"

//#define READ_PARALLEL
//#define SEEK_PARALLEL
#define FILE_LEVEL_FILTER
//#define DISABLE_SEEK_BASED_COMPACTION


#ifdef TIMER_LOG_SEEK
	#define hstart_timer(s) timer->StartTimer(s)
	#define hrecord_timer(s) timer->Record(s)
#else
	#define hstart_timer(s)
	#define hrecord_timer(s)
#endif

#ifdef READ_PARALLEL
#define NUM_READ_THREADS 16
#endif

//#define SEEK_TWO_WAY_SIGNAL
namespace leveldb {

enum ThreadStatus {
	IDLE,
	ASSIGNED,
	BUSY,
	COMPLETED
};
enum  SeekReturnStatus {
	UNINITIALIZED,
	INITIALIZED,
	ACKNOWLEDGED
};
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt
};
struct Saver {
  Saver() : state(), ucmp(), user_key(), value() {}
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string *value;
 private:
  Saver(const Saver&);
  Saver& operator = (const Saver&);
};
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

namespace log { class Writer; }

class Compaction;
class CompactionBoundary;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class FileLevelFilterBuilder;
class Version;
class VersionSet;
class ConcurrentWritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
extern int FindFile(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    const Slice& key);

// Return the smallest guard file i such that
// guard[i+1]->guard_key >= key; Return guards.size() if there is no
// such guard. REQUIRES: "guards" contains a sorted list of guards.
extern int FindGuard(const InternalKeyComparator& icmp,
		      const std::vector<GuardMetaData*>& guards,
		      const Slice& key);
 
// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==NULL represents a key smaller than all keys in the DB.
// largest==NULL represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
extern bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const std::vector<FileMetaData*>& files,
    const Slice* smallest_user_key,
    const Slice* largest_user_key);

class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // Append to *iters a sequence of iterators that will
  // yield a subset of the contents of this Version when merged together.
  // Yields only files with number greater or equal to num
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddSomeIterators(const ReadOptions&, uint64_t num, std::vector<Iterator*>* iters);

  // Append to *iters a sequence of iterators that will
  // yield a subset of the contents of this Version when merged together.
  // Yields only files with number greater or equal to num
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  // This function is with taking guards into account
  void AddSomeIteratorsGuards(const ReadOptions&, uint64_t num, std::vector<Iterator*>* iters);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  void GetOverlappingInputs(
      unsigned level,
      const InternalKey* begin,         // NULL means before all keys
      const InternalKey* end,           // NULL means after all keys
      std::vector<FileMetaData*>* inputs);

  void GetOverlappingInputsGuards(
      unsigned level,
      const InternalKey* begin,         // NULL means before all keys
      const InternalKey* end,           // NULL means after all keys
      std::vector<FileMetaData*>* inputs,
	  std::vector<GuardMetaData*>* guard_inputs,
	  std::vector<FileMetaData*>* sentinel_inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==NULL represents a key smaller than all keys in the DB.
  // largest_user_key==NULL represents a key largest than all keys in the DB.
  bool OverlapInLevel(unsigned level,
                      const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  bool OverlapInLevelGuards(unsigned level,
                      const Slice* smallest_user_key,
                      const Slice* largest_user_key);
  
  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  int PickLevelForMemTableOutputGuards(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  size_t NumFiles(unsigned level) const { return files_[level].size(); }

  size_t NumGuards(unsigned level) const { return guards_[level].size(); }

  size_t NumGuardFiles(unsigned level) const {
    assert(level < config::kNumLevels);
    int num_guard_files = 0;
    std::vector<GuardMetaData*> guards = guards_[level];
    for (unsigned i = 0; i < guards.size(); i++) {
      if (guards[i] != NULL) {
    	  num_guard_files += guards[i]->number_segments;
      }
    }
    return num_guard_files;
  }

  size_t NumCompleteGuardFiles(unsigned level) const {
    assert(level < config::kNumLevels);
    int num_guard_files = 0;
    std::vector<GuardMetaData*> guards = complete_guards_[level];
    for (unsigned i = 0; i < guards.size(); i++) {
  	  num_guard_files += guards[i]->number_segments;
    }
    return num_guard_files;
  }

  size_t NumSentinelFiles(unsigned level) const { return sentinel_files_[level].size(); }

  std::string GuardDetailsAtLevel(unsigned level) const {
		assert(level < config::kNumLevels);
		std::vector<GuardMetaData*> guard_meta_data_list = guards_[level];
		std::string result = "{\"level\":";
		result.append(NumberToString(level))
				.append(",");
		result.append("\"num_guards\":")
				.append(NumberToString(NumGuards(level)))
				.append(",");
		result.append("\"guards\":[");
		for (unsigned i = 0; i < guard_meta_data_list.size(); i++) {
			if (i > 0) {
				result.append("},");
			}
			result.append("{\"guard_key\":\"")
					.append(guard_meta_data_list[i]->guard_key.user_key().ToString())
					.append("\",\"num_segments\":")
					.append(NumberToString(guard_meta_data_list[i]->number_segments))
					.append(",\"files\":[");
			for (unsigned j = 0; j < guard_meta_data_list[i]->files.size(); j++) {
				if (j > 0) {
					result.append(",");
				}
				result.append(NumberToString(guard_meta_data_list[i]->files[j]));
			}
			result.append("]");
		}
		result.append("}]}\n");
		return result;
  }

  std::string SentinelDetailsAtLevel(unsigned level) const {
  	assert(level < config::kNumLevels);
  	std::vector<FileMetaData*> file_meta_data_list = sentinel_files_[level];
  	std::string result = "{\"level\":";
  	result.append(NumberToString(level))
  			.append(",");
  	result.append("\"num_sentinel_files\":")
  			.append(NumberToString(file_meta_data_list.size()))
  			.append(",");
  	result.append("\"files\":[");
  	for (unsigned i = 0; i < file_meta_data_list.size(); i++) {
  		if (i > 0) {
  			result.append(",");
  		}
  		result.append(NumberToString(file_meta_data_list[i]->number));
  	}
  	result.append("]}\n");
  	return result;
  }

  std::string GetCurrentVersionState() {
  	return DebugString();
  }

  int TotalGuards() const {
    int total = 0;
    for (int i = 0; i < config::kNumLevels; i++)
      total += guards_[i].size();
    return total;
  }
  
  void AddGuard(GuardMetaData* g, int level) {
    assert(level >=0 && level < config::kNumLevels);
    guards_[level].push_back(g);
  }

  void AddToCompleteGuards(GuardMetaData* g, int level) {
    assert(level >=0 && level < config::kNumLevels);
    complete_guards_[level].push_back(g);
  }

  void AddToNewGuards(GuardMetaData* g, int level) {
    assert(level >=0 && level < config::kNumLevels);
  }

  void AddCompleteGuardsToEdit(VersionEdit* edit) {
	assert(edit != NULL);
    for (int i = 0; i < config::kNumLevels; i++) {
    	for (int j = 0; j < complete_guards_[i].size(); j++) {
    		edit->AddCompleteGuardFromExisting(i, complete_guards_[i][j]);
    	}
    }
  }

  // For the levels present in level_to_load_complete_guards, update the edit with complete_guards_,
  // Else, update the edit with guards_
  void AddGuardsToEdit(VersionEdit* edit, std::set<int> level_to_load_complete_guards) {
    assert(edit != NULL);
    for (int i = 0; i < config::kNumLevels; i++) {
    	if (level_to_load_complete_guards.count(i) > 0) {
    		for (int j = 0; j < complete_guards_[i].size(); j++) {
    			edit->AddGuardFromExisting(i, complete_guards_[i][j]);
    		}
    	}
    }
  }

  // For the levels present in level_to_load_complete_guards, add only the delta between guards and
  // complete guards to the edit
  void AddDeltaGuardsToEdit(VersionEdit* edit, std::set<int> level_to_load_complete_guards);

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

  std::vector<GuardMetaData*> GetCompleteGuardsAtLevel(unsigned level) {
	  assert(level < config::kNumLevels);
	  return complete_guards_[level];
  }

  std::vector<GuardMetaData*> GetGuardsAtLevel(unsigned level) {
	  assert(level < config::kNumLevels);
	  return guards_[level];
  }

  int GetNumLevelsWithFiles(int *min_level, int *max_level, double* max_level_compaction_score) {
	  *min_level = -1; *max_level = -1;
	  int cnt = 0;
	  for (int level = 0; level < config::kNumLevels; level++) {
		  if (files_[level].size() > 0) {
			  if (*min_level == -1) {
				  *min_level = level;
			  }
			  *max_level = level;
			  *max_level_compaction_score = compaction_scores_[level];
			  cnt++;
		  }
	  }
	  return cnt;
  }

  void HACK_IncreaseCompactionScoresForLevel(int level) {
	  if (files_[level].size() == 0) {
		  return;
	  }
	  compaction_scores_[level] = 1.5;
	  if (sentinel_files_[level].size() > 0) {
		  sentinel_compaction_scores_[level] = 1.5;
	  }
	  for (int i = 0; i < guards_[level].size(); i++) {
		  if (guards_[level][i]->number_segments > 0) {
			  guard_compaction_scores_[level][i] = 1.5;
		  }
	  }
  }

    int num_files_read;
    
 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;
  class LevelGuardNumIterator;

  Iterator* NewConcatenatingIterator(const ReadOptions&, unsigned level, uint64_t num) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void ForEachOverlapping(Slice user_key, Slice internal_key,
                          void* arg,
                          bool (*func)(void*, unsigned, FileMetaData*));

  VersionSet* vset_;            // VersionSet to which this Version belongs
  Version* next_;               // Next version in linked list
  Version* prev_;               // Previous version in linked list
  int refs_;                    // Number of live refs to this version

  // List of files per level
  std::vector<FileMetaData*> files_[config::kNumLevels];
  // List of guards per level which are persisted to disk and already committed to a MANIFEST
  std::vector<GuardMetaData*> guards_[config::kNumLevels];
  // List of guards per level including the guards which are present in memory (not yet checkpointed)
  std::vector<GuardMetaData*> complete_guards_[config::kNumLevels];
  // List of files in the sentinel guard per level - Not persisted to disk
  std::vector<FileMetaData*> sentinel_files_[config::kNumLevels];
  // List of sentinel file numbers alone - persisted to disk
  std::vector<uint64_t> sentinel_file_nos_[config::kNumLevels];
  // Refers to the number of complete guards persisted in any version
  int num_complete_guards_[config::kNumLevels];
  
  // Next file to compact based on seek stats.
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  double compaction_scores_[config::kNumLevels];

  std::vector<double> guard_compaction_scores_[config::kNumLevels];

  // To hold the compaction score of sentinel files in each level
  double sentinel_compaction_scores_[config::kNumLevels];

  explicit Version(VersionSet* vset)
      : vset_(vset), next_(this), prev_(this), refs_(0),
        file_to_compact_(NULL),
        file_to_compact_level_(-1) {
    for (unsigned i = 0; i < config::kNumLevels; ++i) {
      compaction_scores_[i] = -1;
      num_complete_guards_[i] = 0;
    }
  }

  ~Version();

  // No copying allowed
  Version(const Version&);
  void operator=(const Version&);
};

class VersionSet {
 public:
  VersionSet(const std::string& dbname,
             const Options* options,
             TableCache* table_cache,
             const InternalKeyComparator*,
			 Timer* timer);
  ~VersionSet();

#ifdef SEEK_PARALLEL
  static void ConcurrentSeekWrapper(void* vset) {
	  VersionSet* vset_ = reinterpret_cast<VersionSet*>(vset);
	  pthread_t thread_id = vset_->env_->GetThreadId(); // Thread id of this thread
	  int index = vset_->seek_pthread_index_map_[thread_id]; // Get the static mapping of thread id to integer index

	  Timer* timer = vset_->seek_thread_timers_[index];

	  while (vset_->stop_seek_threads_ == 0) {
		  while (vset_->seek_thread_status_[index] != ASSIGNED && vset_->stop_seek_threads_ == 0) {
			  vset_->seek_threads_cv_[index].Wait();
		  }
		  if (vset_->stop_seek_threads_ == 1) {
			  break;
		  }
		  hstart_timer(SEEK_THREAD_TOTAL_ACTIVE);
		  vset_->seek_thread_status_[index] = BUSY;
		  // Get the data to work upon from shared variables at the VersionSet level
		  IteratorWrapper* iterator_wrapper = vset_->iterators_to_seek_[index];
		  Slice target = vset_->internal_keys_to_seek_[index];
		  int group_index = vset_->seek_assigned_group_id_[index]; // This is used to coordinate between a set of threads which belong to the same parallel call
		  hstart_timer(SEEK_THREAD_ITER_SEEK);

		  iterator_wrapper->Seek(target);

		  hrecord_timer(SEEK_THREAD_ITER_SEEK);

#ifdef SEEK_TWO_WAY_SIGNAL
		  // Decrement the number of pending threads in this group to complete.
		  bool last_thread_to_complete = false;
		  vset_->seek_group_threads_mutex_[group_index].Lock();
		  vset_->seek_group_num_pending_[group_index]--;
		  if (vset_->seek_group_num_pending_[group_index] == 0) {
			  last_thread_to_complete = true;
		  }
		  vset_->seek_group_threads_mutex_[group_index].Unlock();

		  // The last thread to complete in this group signals the parent thread until it acknowledges.
		  if (last_thread_to_complete) {
			  while (vset_->seek_group_return_status_[group_index] != ACKNOWLEDGED) { // Kind of busy waiting here.
				  vset_->seek_group_threads_cv_[group_index].Signal(); // Parent thread will be waiting on this cv. s
			  }
		  }
#endif
		  vset_->iterators_to_seek_[index] = NULL; // Not required -- just to avoid silent failures.
		  vset_->seek_thread_status_[index] = IDLE;
		  vset_->seek_threads_current_workload[index] = 0; // Meaning this thread is now free for processing
		  hrecord_timer(SEEK_THREAD_TOTAL_ACTIVE);
	  }
  }

  // Returns the index of the thread which is idle. Returns -1 if no thread is idle.
  int GetIdleThreadIndex(pthread_t current_thread) {
	  int index = -1;
	  seek_mutex_.Lock();
	  for (int i = 0; i < NUM_SEEK_THREADS; i++) {
		  if (seek_threads_current_workload[i] == 0) {
			  seek_threads_current_workload[i] = current_thread;
			  index = i;
			  break;
		  }
	  }
	  seek_mutex_.Unlock();
	  return index;
  }

  // Returns the index of the thread which is idle. Returns -1 if no thread is idle.
  int GetNextGroupIndex() {
	  int index = -1;
	  seek_mutex_.Lock();
	  for (int i = 0; i < NUM_SEEK_THREADS; i++) {
		  if (seek_group_occupied_[i] == 0) {
			  seek_group_occupied_[i] = 1;
			  index = i;
			  break;
		  }
	  }
	  seek_mutex_.Unlock();
	  return index;
  }

  // Returns the index of the thread which is idle. Returns -1 if no thread is idle.
  int GetIdleTableIteratorThreadIndex(pthread_t current_thread) {
	  int index = -1;
	  seek_titerator_mutex_.Lock();
	  for (int i = 0; i < NUM_SEEK_THREADS; i++) {
		  if (seek_titerator_threads_current_workload[i] == 0) {
			  seek_titerator_threads_current_workload[i] = current_thread;
			  index = i;
			  break;
		  }
	  }
	  seek_titerator_mutex_.Unlock();
	  return index;
  }

  int GetNextTableIteratorGroupIndex() {
	  int index = -1;
	  seek_titerator_mutex_.Lock();
	  for (int i = 0; i < NUM_SEEK_THREADS; i++) {
		  if (seek_titerator_group_occupied_[i] == 0) {
			  seek_titerator_group_occupied_[i] = 1;
			  index = i;
			  break;
		  }
	  }
	  seek_titerator_mutex_.Unlock();
	  return index;
  }

#endif

  /*
#ifdef READ_PARALLEL
  static void ConcurrentReadWrapper(void* vset) {
	  VersionSet* vset_ = reinterpret_cast<VersionSet*>(vset);
	  pthread_t thread_id = vset_->env_->GetThreadId();
	  int index = vset_->pthread_index_map_[thread_id];

	  while (vset_->stop_read_threads_ == 0) {
		  while (vset_->read_threads_current_workload[index] == 0 && vset_->stop_read_threads_ == 0) {
			  vset_->read_threads_cv_[index].Wait();
		  }
		  if (vset_->stop_read_threads_ == 1) {
			  break;
		  }
		  vset_->thread_status_[index] = BUSY;
		  vset_->table_cache_->Get(vset_->read_options_[index], vset_->file_numbers_to_read_[index],
				  vset_->file_sizes_to_read_[index], vset_->internal_keys_to_read_[index],
				  vset_->saver_values_from_read_[index], SaveValue, vset_->timer);
		  vset_->read_threads_current_workload[index] = 0;
		  vset_->thread_status_[index] = IDLE;
	  }
	  int n = vset_->num_read_threads_;
	  vset_->num_read_threads_--;
  }

  // Returns the index of the thread which is idle. Returns -1 if no thread is idle.
  int GetIdleThreadIndex(pthread_t current_thread) {
	  int index = -1;
	  get_mutex_.Lock();
	  for (int i = 0; i < NUM_READ_THREADS; i++) {
		  if (read_threads_current_workload[i] == 0) {
			  read_threads_current_workload[i] = current_thread;
			  index = i;
			  break;
		  }
	  }
	  get_mutex_.Unlock();
	  return index;
  }
#endif
  */

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu, port::CondVar* cv, bool* wt, std::vector<uint64_t> file_numbers, std::vector<std::string*> file_level_fitlers, int mtc)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover();

  void PrintSeekThreadsTimerAuditIndividual() {
#ifdef SEEK_PARALLEL
	  for (int i = 0; i < NUM_SEEK_THREADS; i++) {
		  if (seek_thread_timers_[i] == NULL) {
			  continue;
		  }
	  	  printf("-------------------------- Individual timer information %d --------------------\n", i);
	  	  printf("%s\n", seek_thread_timers_[i]->DebugString().c_str());
	  	  printf("-------------------------------------------------------------------\n");
	  }
#endif
  }

  void PrintSeekThreadsTimerAuditCumulative() {
#ifdef SEEK_PARALLEL
	  Timer* cum_timer = new Timer();
	  for (int i = 0; i < NUM_SEEK_THREADS; i++) {
		  cum_timer->AppendTimerInfo(seek_thread_timers_[i]);
	  }
	  printf("-------------------------- Cumulative timer information --------------------\n");
	  printf("%s\n", cum_timer->DebugString().c_str());
	  printf("-------------------------------------------------------------------\n");
	  delete cum_timer;
#endif
  }

  void PrintSeekThreadsStaticTimerAuditIndividual();

  void PrintSeekThreadsStaticTimerAuditCumulative();

  pthread_t GetCurrentThreadId() const {
	  return env_->GetThreadId();
  }

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(unsigned level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(unsigned level) const;

  // Returns the number of guards at a given level
  int NumGuards(unsigned level) const;
  
  // Return the list of Guards in a given level
  std::vector<GuardMetaData*> GetGuardsAtLevel(unsigned level);

  // Return the list of Guards in a given level
  std::vector<GuardMetaData*> GetCompleteGuardsAtLevel(unsigned level);

  // Return the number of files assigned to some guard in a level
  int NumGuardFiles(unsigned level) const;

  // Return the number of sentinel files in a given level
  int NumSentinelFiles(unsigned level) const;

  // Returns the guard and the files in each guard for a given level
  std::string GuardDetailsAtLevel(unsigned level) const;

  // Returns the sentinels and the files present for a given level
  std::string SentinelDetailsAtLevel(unsigned level) const;

  // Get the current version state
  std::string GetCurrentVersionState();

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s, if it's not already larger
  void SetLastSequence(uint64_t s) {
    if (last_sequence_ <= s) {
      last_sequence_ = s;
    }
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  bool IsAllLevelsCompacted();
  // Pick level for a new compaction.
  // Returns kNumLevels if there is no compaction to be done.
  // Otherwise returns the lowest unlocked level that may compact upwards.
  unsigned PickCompactionLevel(bool* locked, bool seek_driven, bool* force_compact) const;

  unsigned NumUncompactedLevels();
  // Pick inputs for a new compaction at the specified level.
  // Returns NULL if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction(Version* v, unsigned level);

  // To pick the required compaction object to compact the guard files
  // in a level and assign them to proper guards
  Compaction* PickCompactionForGuardsInLevel(Version* v, unsigned level);

  // To pick the required compaction object to compact the guard files
  // in a level and assign them to proper guards
  Compaction* PickCompactionForGuards(Version* v, unsigned level, std::vector<GuardMetaData*>* complete_guards_used_in_bg_compaction, bool force_compact);

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns NULL if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(
      unsigned level,
      const InternalKey* begin,
      const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  Iterator* MakeInputIteratorForGuardsInALevel(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction(bool* levels, bool seek_driven) const {
	bool force_compact;
    return PickCompactionLevel(levels, seek_driven, &force_compact) != config::kNumLevels;
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);
  
  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

  InternalKeyComparator GetInternalComparator() { return icmp_; }

  Timer* timer;

  void AddFileLevelBloomFilterInfo(uint64_t file_number, std::string* filter_string);
  void RemoveFileLevelBloomFilterInfo(uint64_t file_number);
  void InitializeFileLevelBloomFilter();
  void InitializeTableCacheFileMetaData();

  void RemoveFileMetaDataFromTableCache(uint64_t file_number);

  void HACK_IncreaseCompactionScoresForLevel(int level) {
	  current_->HACK_IncreaseCompactionScoresForLevel(level);
  }

  TableCache* const GetTableCache() {
	  return table_cache_;
  }

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  uint64_t GetOverlappingRangeBetweenFiles(FileMetaData* f1, FileMetaData* f2);
  unsigned int RangeDifference(Slice a, Slice b);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs,
                InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest,
                 InternalKey* largest);

  void GetCompactionBoundaries(Version* version,
                               unsigned level,
                               std::vector<FileMetaData*>* LA,
                               std::vector<FileMetaData*>* LB,
                               std::vector<uint64_t>* LA_sizes,
                               std::vector<uint64_t>* LB_sizes,
                               std::vector<class CompactionBoundary>* boundaries);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);
  void PopulateFileLevelBloomFilter();
  void PopulateBloomFilterForFile(FileMetaData* file, FileLevelFilterBuilder* file_level_filter_builder);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

/*
#ifdef READ_PARALLEL
  // For synchronizing concurrent reads
  std::map<pthread_t, int> pthread_index_map_;
  ThreadStatus thread_status_[NUM_READ_THREADS];
  // Contains the value of the calling thread id for which the read threads are serving
  pthread_t read_threads_current_workload[NUM_READ_THREADS];
//  bool thread_occupied_[NUM_READ_THREADS];
  uint64_t file_numbers_to_read_[NUM_READ_THREADS];
  uint64_t file_sizes_to_read_[NUM_READ_THREADS];
  Slice internal_keys_to_read_[NUM_READ_THREADS];
  Saver* saver_values_from_read_[NUM_READ_THREADS];
  ReadOptions read_options_[NUM_READ_THREADS];
//  int* num_concurrent_reads_[NUM_READ_THREADS];
  port::Mutex read_threads_mutex_[NUM_READ_THREADS];
  port::CondVar read_threads_cv_[NUM_READ_THREADS];
//  port::Mutex read_threads_mutex_;
  port::Mutex get_mutex_;

  int stop_read_threads_;
  int num_read_threads_;
#endif
*/


#ifdef SEEK_PARALLEL
 public: // Because these have to be modified in MergingIterator class
  // For synchronizing concurrent reads
  std::map<pthread_t, int> seek_pthread_index_map_;
  ThreadStatus seek_thread_status_[NUM_SEEK_THREADS];
  // Contains the value of the calling thread id for which the read threads are serving
  pthread_t seek_threads_current_workload[NUM_SEEK_THREADS];
  IteratorWrapper* iterators_to_seek_[NUM_SEEK_THREADS];
  Slice internal_keys_to_seek_[NUM_SEEK_THREADS];

  port::Mutex seek_threads_mutex_[NUM_SEEK_THREADS];
  port::CondVar seek_threads_cv_[NUM_SEEK_THREADS];
  port::Mutex seek_group_threads_mutex_[NUM_SEEK_THREADS];
  port::CondVar seek_group_threads_cv_[NUM_SEEK_THREADS];

  int seek_group_occupied_[NUM_SEEK_THREADS];
  int seek_assigned_group_id_[NUM_SEEK_THREADS];
  int seek_group_num_pending_[NUM_SEEK_THREADS];
  SeekReturnStatus seek_group_return_status_[NUM_SEEK_THREADS];
  port::Mutex seek_mutex_;

  std::map<pthread_t, int> seek_titerator_pthread_index_map_;
  ThreadStatus seek_titerator_thread_status_[NUM_SEEK_THREADS];
  // Contains the value of the calling thread id for which the read threads are serving
  pthread_t seek_titerator_threads_current_workload[NUM_SEEK_THREADS];
  Iterator** seek_titerator_result_iterators_[NUM_SEEK_THREADS];

  uint64_t seek_titerator_file_numbers_[NUM_SEEK_THREADS];
  uint64_t seek_titerator_file_sizes_[NUM_SEEK_THREADS];
  ReadOptions seek_titerator_read_options_[NUM_SEEK_THREADS];

  port::Mutex seek_titerator_threads_mutex_[NUM_SEEK_THREADS];
  port::CondVar seek_titerator_threads_cv_[NUM_SEEK_THREADS];
  port::Mutex seek_titerator_group_threads_mutex_[NUM_SEEK_THREADS];
  port::CondVar seek_titerator_group_threads_cv_[NUM_SEEK_THREADS];

  int seek_titerator_group_occupied_[NUM_SEEK_THREADS];
  int seek_titerator_assigned_group_id_[NUM_SEEK_THREADS];
  int seek_titerator_group_num_pending_[NUM_SEEK_THREADS];
  SeekReturnStatus seek_titerator_group_return_status_[NUM_SEEK_THREADS];
  port::Mutex seek_titerator_mutex_;

  pthread_t current_thread_;

  Timer* seek_thread_timers_[NUM_SEEK_THREADS];
  int stop_seek_threads_;

  Env* getEnv() {
	  return env_;
  }

 private:
#endif

  int num_seek_threads_;

  std::map<uint64_t, std::string*> file_level_bloom_filter;

  // Opened lazily
  ConcurrentWritableFile* descriptor_file_;
  log::Writer* descriptor_log_;
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  // vijayc: this could be the start of a guard.
  std::string compact_pointer_[config::kNumLevels];
        
  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  unsigned level() const { return level_; }
  
  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  size_t num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }
  
  // Minimum size of files to build during this compaction.
  uint64_t MinOutputFileSize() const { return min_output_file_size_; }
                
  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Does the transition from old_key to new_key cross any boundaries at a
  // higher level?
  bool CrossesBoundary(const ParsedInternalKey& old_key,
                       const ParsedInternalKey& new_key,
                       size_t* hint) const;
  
  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);
  
  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

  std::string DebugString();

  bool is_horizontal_compaction;

 private:
  friend class Version;
  friend class VersionSet;
  Compaction(const Compaction&);
  Compaction& operator = (const Compaction&);

  explicit Compaction(unsigned level);

  unsigned level_;
  uint64_t min_output_file_size_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1", and avoids
  // writing generating overlap in "level_+2".
  std::vector<FileMetaData*> inputs_[2]; // The three sets of inputs
  std::vector<GuardMetaData*> guard_inputs_[2];
  std::vector<FileMetaData*> sentinel_inputs_[2]; // inputs_ = guard_inputs_ + sentinel_inputs_
  std::vector<std::pair<uint64_t, leveldb::Slice> > boundaries_;

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_

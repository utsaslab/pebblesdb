// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include <algorithm>

#include "pebblesdb/comparator.h"
#include "pebblesdb/iterator.h"
#include "table/iterator_wrapper.h"
#include "util/timer.h"

#ifdef TIMER_LOG_SEEK
	#define start_timer(s) timer->StartTimer(s)
	#define vstart_timer(s) vset_->timer->StartTimer(s)

	#define record_timer(s) timer->Record(s)
	#define vrecord_timer(s) vset_->timer->Record(s)
	#define vrecord_timer2(s, count) vset_->timer->Record(s, count)
#else
	#define start_timer(s)
	#define vstart_timer(s)

	#define record_timer(s)
	#define vrecord_timer(s)
	#define vrecord_timer2(s, count)
#endif

namespace leveldb {

namespace {
class MergingIterator;

struct HeapComparator {
  HeapComparator(MergingIterator* mi) : mi_(mi) {}
  HeapComparator(const HeapComparator& other) : mi_(other.mi_) {}

  MergingIterator* mi_;

  bool operator () (unsigned lhs, unsigned rhs) const;

  private:
    HeapComparator& operator = (const HeapComparator&);
};

class MergingIterator : public Iterator {
 private:
  void ReinitializeComparisons();
  void PopCurrentComparison();
  void PushCurrentComparison();
 public:
  MergingIterator(const Comparator* comparator, Iterator** children,
		  FileMetaData** file_meta_list, int n,
		  const InternalKeyComparator* icmp,
		  bool is_merging_iterator_for_files,
		  VersionSet* vset, unsigned l)
      : comparator_(comparator),
		icmp_(icmp),
        children_(new IteratorWrapper[n]),
		file_meta_list(file_meta_list),
        comparisons_(new uint64_t[n]),
        heap_(new unsigned[n]),
        heap_sz_(0),
        n_(n),
        comparisons_intialized_(false),
        current_(NULL),
        status_(),
        direction_(kForward),
		is_merging_iterator_for_files_(is_merging_iterator_for_files),
		vset_(vset),
		level(l){
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    ReinitializeComparisons();
//    for (int i = 0; i < n; i++) {
//    	thread_status_[i] = IDLE;
//    	iterators_to_seek_[i] = NULL;
//    }
    if (vset_ != NULL) {
    	current_thread = vset_->GetCurrentThreadId();
    }
  }

  virtual ~MergingIterator() {
    delete[] children_;
    delete[] comparisons_;
    delete[] heap_;
    if (file_meta_list != NULL) {
    	delete[] file_meta_list;
    }
  }

  virtual bool Valid() const {
    return (current_ != NULL);
  }

  virtual void SeekToFirst() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    direction_ = kForward;
    ReinitializeComparisons();
    FindSmallest();
  }

  virtual void SeekToLast() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    direction_ = kReverse;
    ReinitializeComparisons();
    FindLargest();
  }

#ifdef SEEK_PARALLEL
  void SeekInParallel(const Slice& target) {
    std::vector<int> seek_thread_indices;
    int group_index;

    vstart_timer(SEEK_PARALLEL_TOTAL);
    vstart_timer(SEEK_PARALLEL_ASSIGN_THREADS);
#ifdef SEEK_TWO_WAY_SIGNAL
    // Get a group_index which is used to coordinate among the parallel threads being triggered
    // to maintain the count of pending threds, to signal back using cv etc.
    while (true) {
    	group_index = vset_->GetNextGroupIndex();
    	if (group_index >= 0 && group_index < NUM_SEEK_THREADS) {
    		break;
    	}
    }
#endif

    for (uint32_t i = 0; i < n_; ++i) {
	  int index;
	  // BUSY WAITING !! To get the index of the idle thread.
	  // Assuming that the number of threads will be sufficient enough to process any parallel request.
	  while (true) {
		  index = vset_->GetIdleThreadIndex(current_thread);
		  if (index >= 0 && index < NUM_SEEK_THREADS) {
			  break;
		  }
	  }
	  seek_thread_indices.push_back(index);
	  vset_->internal_keys_to_seek_[index] = target;
	  vset_->iterators_to_seek_[index] = &children_[i];
	  vset_->seek_thread_status_[index] = ASSIGNED;

	  vset_->seek_assigned_group_id_[index] = group_index;

	  vset_->seek_threads_cv_[index].Signal();
    }
    vrecord_timer2(SEEK_PARALLEL_ASSIGN_THREADS, n_);

#ifdef SEEK_TWO_WAY_SIGNAL
	vset_->seek_group_return_status_[group_index] = INITIALIZED;
    vset_->seek_group_num_pending_[group_index] = n_;
#endif

    // Signalling the seek threads to start.
    vstart_timer(SEEK_PARALLEL_SIGNAL_THREADS);
    while (true) {
    	bool all_threads_started = true;
		for (int i = 0; i < seek_thread_indices.size(); i++) {
			int index = seek_thread_indices[i];
			if (vset_->seek_thread_status_[index] == ASSIGNED && vset_->seek_threads_current_workload[index] == current_thread) { // This child thread has not yet received the signal from the parent
				vset_->seek_threads_cv_[index].Signal();
				all_threads_started = false; // because we don't know if the child thread actually received the signal.
			}
		}
		if (all_threads_started) {
			break;
		}
    }
    vrecord_timer2(SEEK_PARALLEL_SIGNAL_THREADS, n_);

#ifdef SEEK_TWO_WAY_SIGNAL
    vstart_timer(SEEK_PARALLEL_WAIT_FOR_THREADS);
    while (vset_->seek_group_num_pending_[group_index] > 0) {
    	vset_->seek_group_threads_cv_[group_index].Wait();
    }
    vset_->seek_group_return_status_[group_index] = ACKNOWLEDGED; // After this, the last thread will stop signalling.
    vset_->seek_group_occupied_[group_index] = 0; // Free up this slot so that this can be used by other thread groups
    vrecord_timer2(SEEK_PARALLEL_WAIT_FOR_THREADS, n_);
#else
    // BUSY WAITING here !
    vstart_timer(SEEK_PARALLEL_WAIT_FOR_THREADS);
    while (true) {
		bool completed = true;
		for (int i = 0; i < seek_thread_indices.size(); i++) {
			int index = seek_thread_indices[i];
			if (vset_->seek_threads_current_workload[index] == current_thread) {
				completed = false;
				break;
			}
		}
		if (completed) {
			break;
		}
	}
    vrecord_timer2(SEEK_PARALLEL_WAIT_FOR_THREADS, n_);
#endif
    vrecord_timer2(SEEK_PARALLEL_TOTAL, n_);
  }
#endif

  void SeekInSequence(const Slice& target) {
#ifdef TIMER_LOG_SEEK
	if (is_merging_iterator_for_files_) {
		vstart_timer(SEEK_SEQUENTIAL_FILES);
	} else {
		vstart_timer(SEEK_SEQUENTIAL_TOTAL);
	}
#endif

	uint64_t start, end;
	for (int i = 0; i < n_; i++) {
//		start = Env::Default()->NowMicros();
		children_[i].Seek(target);
//		end = Env::Default()->NowMicros();
//		if (is_merging_iterator_for_files_) {
//			printf("SeekInSequence::%d start: %llu end: %llu diff: %llu \n", i, start, end, end - start);
//		}
	}

#ifdef TIMER_LOG_SEEK
	if (is_merging_iterator_for_files_) {
		vrecord_timer2(SEEK_SEQUENTIAL_FILES, n_);
	} else {
		vrecord_timer2(SEEK_SEQUENTIAL_TOTAL, n_);
	}
#endif
  }

  virtual void Seek(const Slice& target) {
#ifdef SEEK_PARALLEL
	if (is_merging_iterator_for_files_ && level == config::kNumLevels-1) {
		SeekInParallel(target);
//		SeekInSequence(target);
	} else {
//		SeekInParallel(target);
		SeekInSequence(target);
	}
#else
	SeekInSequence(target);
#endif
	direction_ = kForward;
#ifdef TIMER_LOG_SEEK
	if (is_merging_iterator_for_files_) {
		vstart_timer(SEEK_REINIT_FILE_LEVEL);
	} else {
		vstart_timer(SEEK_REINIT);
	}
#endif
    ReinitializeComparisons();
    FindSmallest();
#ifdef TIMER_LOG_SEEK
	if (is_merging_iterator_for_files_) {
		vrecord_timer(SEEK_REINIT_FILE_LEVEL);
	} else {
		vrecord_timer(SEEK_REINIT);
	}
#endif
  }

  /*  virtual void Seek(const Slice& target) {
/*		for (int i = 0; i < n_; i++) {
		  children_[i].Seek(target);
		}
/	if (n_ == 1 || file_meta_list == NULL) {
		for (int i = 0; i < n_; i++) {
		  children_[i].Seek(target);
		}
	} else {
		for (int i = 0; i < n_; i++) {
			if (file_meta_list[i] == NULL) {
				children_[i].Seek(target);
				continue;
			}
			InternalKey target_ikey;
			target_ikey.DecodeFrom(target);
			if (icmp_->user_comparator()->Compare(file_meta_list[i]->largest.user_key(), target_ikey.user_key()) < 0) {
			  children_[i].MakeInvalid();
		  } else if (icmp_->user_comparator()->Compare(file_meta_list[i]->smallest.user_key(), target_ikey.user_key()) >= 0) {
			  children_[i].SeekToFirst();
		  } else {
			children_[i].Seek(target);
		  }
		}
	}
    direction_ = kForward;
    ReinitializeComparisons();
    FindSmallest();
  }
*/

  virtual void Next() {
#ifdef TIMER_LOG_SEEK
	if (is_merging_iterator_for_files_) {
		vstart_timer(SEEK_NEXT_FILE_LEVEL);
	} else {
		vstart_timer(SEEK_NEXT);
	}
#endif
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
      ReinitializeComparisons();
    }

#ifdef TIMER_LOG_SEEK
    if (is_merging_iterator_for_files_) {
    	vstart_timer(SEEK_NEXT_POP_FILE_LEVEL);
    	PopCurrentComparison();
    	vrecord_timer(SEEK_NEXT_POP_FILE_LEVEL);

    	vstart_timer(SEEK_NEXT_CURRENT_NEXT_FILE_LEVEL);
        current_->Next();
        vrecord_timer(SEEK_NEXT_CURRENT_NEXT_FILE_LEVEL);

        vstart_timer(SEEK_NEXT_PUSH_FILE_LEVEL);
        PushCurrentComparison();
        vrecord_timer(SEEK_NEXT_PUSH_FILE_LEVEL);

        FindSmallest();

    } else {
    	vstart_timer(SEEK_NEXT_POP);
    	PopCurrentComparison();
    	vrecord_timer(SEEK_NEXT_POP);

    	vstart_timer(SEEK_NEXT_CURRENT_NEXT);
        current_->Next();
        vrecord_timer(SEEK_NEXT_CURRENT_NEXT);

        vstart_timer(SEEK_NEXT_PUSH);
        PushCurrentComparison();
        vrecord_timer(SEEK_NEXT_PUSH);

        FindSmallest();
    }
#else
    PopCurrentComparison();
    current_->Next();
    PushCurrentComparison();
    FindSmallest();
#endif

#ifdef TIMER_LOG_SEEK
	if (is_merging_iterator_for_files_) {
		vrecord_timer(SEEK_NEXT_FILE_LEVEL);
	} else {
		vrecord_timer(SEEK_NEXT);
	}
#endif
  }

  virtual void Prev() {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
      ReinitializeComparisons();
    }

    PopCurrentComparison();
    current_->Prev();
    PushCurrentComparison();
    FindLargest();
  }

  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  virtual const Status& status() const {
    // XXX this value can easily be cached
    for (int i = 0; i < n_; i++) {
      if (!children_[i].status().ok()) {
        return children_[i].status();
      }
    }
    return status_;
  }

 private:
  MergingIterator(const MergingIterator&);
  MergingIterator& operator = (const MergingIterator&);
  friend class HeapComparator;
  void FindSmallest();
  void FindLargest();

  const Comparator* comparator_;
  IteratorWrapper* children_;
  FileMetaData** file_meta_list;
  uint64_t* comparisons_;
  unsigned* heap_;
  size_t heap_sz_;
  int n_;
  bool comparisons_intialized_;
  IteratorWrapper* current_;
  Status status_;
  const InternalKeyComparator* icmp_;
  bool is_merging_iterator_for_files_;
  VersionSet* vset_;
  unsigned level;
  pthread_t current_thread;

  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
};

bool HeapComparator::operator ()(unsigned lhs, unsigned rhs) const {
  if (mi_->direction_ == MergingIterator::kForward) {
    std::swap(lhs, rhs);
  }
  return mi_->comparisons_[lhs] < mi_->comparisons_[rhs] ||
         (mi_->comparisons_[lhs] == mi_->comparisons_[rhs] &&
          mi_->comparator_->Compare(mi_->children_[lhs].key(), mi_->children_[rhs].key()) < 0);
}

void MergingIterator::ReinitializeComparisons() {
  heap_sz_ = 0;
  for (int i = 0; i < n_; ++i) {
    if (children_[i].Valid()) {
      comparisons_[i] = comparator_->KeyNum(children_[i].key());
      heap_[heap_sz_] = i;
      ++heap_sz_;
    }
  }
  HeapComparator hc(this);
  std::make_heap(heap_, heap_ + heap_sz_, hc);
}

void MergingIterator::PopCurrentComparison() {
  unsigned idx = current_ - children_;
  assert(heap_[0] == idx);
  HeapComparator hc(this);
  std::pop_heap(heap_, heap_ + heap_sz_, hc);
  --heap_sz_;
}

void MergingIterator::PushCurrentComparison() {
  unsigned idx = current_ - children_;
  assert(&children_[idx] == current_);
  if (current_->Valid()) {
    comparisons_[idx] = comparator_->KeyNum(current_->key());
    heap_[heap_sz_] = idx;
    ++heap_sz_;
    HeapComparator hc(this);
    std::push_heap(heap_, heap_ + heap_sz_, hc);
  }
}

void MergingIterator::FindSmallest() {
  assert(direction_ == kForward);
  if (heap_sz_ > 0) {
    current_ = &children_[heap_[0]];
  } else {
    current_ = NULL;
  }
}

void MergingIterator::FindLargest() {
  assert(direction_ == kReverse);
  if (heap_sz_ > 0) {
    current_ = &children_[heap_[0]];
  } else {
    current_ = NULL;
  }
}
}  // namespace

/*Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, NULL, n, NULL, false, NULL);
  }
}*/

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n, VersionSet* vset) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, NULL, n, NULL, false, vset, -1);
  }
}

Iterator* NewMergingIteratorForFiles(const Comparator* cmp, Iterator** list,
		FileMetaData** file_meta_list, int n,
		const InternalKeyComparator* icmp,
		VersionSet* vset, unsigned level) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, file_meta_list, n, icmp, true, vset, level);
  }
}

}  // namespace leveldb

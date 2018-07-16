// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#define __STDC_LIMIT_MACROS

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/atomic.h"
#include "util/random.h"

namespace leveldb {

class Arena;

template<typename Key, class Comparator, class Extractor>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Extractor ext, Arena* arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 17 };

  // Immutable after construction
  Comparator const compare_;
  Extractor const extractor_;
  Arena* const arena_;    // Arena used for allocations of nodes

  Node* const head_;

  // Read/written only by Insert().
  port::Mutex rnd_mutex_;
  Random rnd_;

  Node* NewNode(const Key& key, unsigned height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev, Node** obs) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // No copying allowed
  SkipList(const SkipList&);
  void operator=(const SkipList&);
};

// Implementation details follow
template<typename Key, class Comparator, class Extractor>
struct SkipList<Key,Comparator,Extractor>::Node {
  explicit Node(uint64_t c, const Key& k) : cmp(c), key(k) {}

  uint64_t const cmp;
  Key const key;

  void SetNext(unsigned n, uint64_t c, Node* x) {
    assert(n < kMaxHeight);
    atomic::store_ptr_release(&ptrs_[n].next, x);
    atomic::store_64_release(&ptrs_[n].cmp, c);
  }
  bool CasNext(unsigned n, Node* old_node, Node* new_node) {
    using namespace atomic;
    if (compare_and_swap_ptr_release(&ptrs_[n].next, old_node, new_node) == old_node) {
      uint64_t new_cmp = new_node->cmp;
      uint64_t expected = UINT64_MAX;
      while (expected > new_cmp) {
        expected = compare_and_swap_64_release(&ptrs_[n].cmp, expected, new_cmp);
      }
      return true;
    } else {
      return false;
    }
  }

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  uint64_t NoBarrier_Cmp(unsigned n) {
    return atomic::load_64_nobarrier(&ptrs_[n].cmp);
  }
  Node* NoBarrier_Next(unsigned n) {
    return atomic::load_ptr_nobarrier(&ptrs_[n].next);
  }
  Node* Next(unsigned n) {
    return atomic::load_ptr_acquire(&ptrs_[n].next);
  }
  void GetNext(unsigned n, uint64_t* c, Node** x) {
    *c = atomic::load_64_acquire(&ptrs_[n].cmp);
    *x = atomic::load_ptr_acquire(&ptrs_[n].next);
  }
  void NoBarrier_SetNext(unsigned n, uint64_t c, Node* x) {
    atomic::store_ptr_nobarrier(&ptrs_[n].next, x);
    atomic::store_64_nobarrier(&ptrs_[n].cmp, c);
  }

  struct PointerHintPair {
    uint64_t cmp;
    Node* next;
  };

 private:
  PointerHintPair ptrs_[1];
};

template<typename Key, class Comparator, class Extractor>
typename SkipList<Key,Comparator,Extractor>::Node*
SkipList<Key,Comparator,Extractor>::NewNode(const Key& key, unsigned height) {
  char* mem = arena_->AllocateAligned(
      sizeof(Node) + sizeof(typename Node::PointerHintPair) * (height - 1));
  uint64_t cmp = extractor_(key);
  Node* n = new (mem) Node(cmp, key);
  for (unsigned i = 0; i < height; ++i) {
    n->NoBarrier_SetNext(i, UINT64_MAX, NULL);
  }
  return n;
}

template<typename Key, class Comparator, class Extractor>
inline SkipList<Key,Comparator,Extractor>::Iterator::Iterator(const SkipList* list)
  : list_(list),
    node_(NULL) {
}

template<typename Key, class Comparator, class Extractor>
inline bool SkipList<Key,Comparator,Extractor>::Iterator::Valid() const {
  return node_ != NULL;
}

template<typename Key, class Comparator, class Extractor>
inline const Key& SkipList<Key,Comparator,Extractor>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template<typename Key, class Comparator, class Extractor>
inline void SkipList<Key,Comparator,Extractor>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template<typename Key, class Comparator, class Extractor>
inline void SkipList<Key,Comparator,Extractor>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

template<typename Key, class Comparator, class Extractor>
inline void SkipList<Key,Comparator,Extractor>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, NULL, NULL);
}

template<typename Key, class Comparator, class Extractor>
inline void SkipList<Key,Comparator,Extractor>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template<typename Key, class Comparator, class Extractor>
inline void SkipList<Key,Comparator,Extractor>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

template<typename Key, class Comparator, class Extractor>
int SkipList<Key,Comparator,Extractor>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  rnd_mutex_.Lock();
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  rnd_mutex_.Unlock();
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template<typename Key, class Comparator, class Extractor>
bool SkipList<Key,Comparator,Extractor>::KeyIsAfterNode(const Key& key, Node* n) const {
  // NULL n is considered infinite
  return (n != NULL) && (compare_(n->key, key) < 0);
}

template<typename Key, class Comparator, class Extractor>
typename SkipList<Key,Comparator,Extractor>::Node* SkipList<Key,Comparator,Extractor>::FindGreaterOrEqual(const Key& key, Node** prev, Node** obs)
    const {
  const uint64_t cmp = extractor_(key);
  Node* x = head_;
  int level = kMaxHeight - 1;
  while (true) {
    while (level > 0 && x->NoBarrier_Cmp(level) > cmp) {
      if (prev != NULL) prev[level] = x;
      if (obs != NULL) obs[level] = NULL;
      --level;
    }
    uint64_t c = 0;
    Node* next = NULL;
    x->GetNext(level, &c, &next);
    if (c < cmp || KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) prev[level] = x;
      if (obs != NULL) obs[level] = next;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        --level;
      }
    }
  }
}

#if 0
template<typename Key, class Comparator, class Extractor>
typename SkipList<Key,Comparator,Extractor>::Node* SkipList<Key,Comparator,Extractor>::FindGreaterOrEqual(const Key& key, Node** prev, Node** obs)
    const {
  const uint64_t cmp = extractor_(key);
  Node* x = &nodes_[cmp >> 56];
  int level = kMaxHeight - 1;
  const uint64_t mask = cmp >> 32;
  while (true) {
    uint64_t c = 0;
    Node* next = NULL;
    x->GetNext(level, &c, &next);

    if (!IsSpacerNode(next) && (c < mask || KeyIsAfterNode(key, next))) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) prev[level] = x;
      if (obs != NULL) obs[level] = next;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        --level;
      }
    }
  }
}
#endif

template<typename Key, class Comparator, class Extractor>
typename SkipList<Key,Comparator,Extractor>::Node*
SkipList<Key,Comparator,Extractor>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = kMaxHeight - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == NULL || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template<typename Key, class Comparator, class Extractor>
typename SkipList<Key,Comparator,Extractor>::Node* SkipList<Key,Comparator,Extractor>::FindLast()
    const {
  Node* x = head_;
  int level = kMaxHeight - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == NULL) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template<typename Key, class Comparator, class Extractor>
SkipList<Key,Comparator,Extractor>::SkipList(Comparator cmp, Extractor ext, Arena* arena)
    : compare_(cmp),
      extractor_(ext),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      rnd_mutex_(),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, UINT64_MAX, NULL);
  }
}

template<typename Key, class Comparator, class Extractor>
void SkipList<Key,Comparator,Extractor>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* obs[kMaxHeight];
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev, obs);

  // Our data structure does not allow duplicate insertion
  assert(x == NULL || !Equal(key, x->key));

  int height = RandomHeight();

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    while (true) {
      Node* n = obs[i];
      uint64_t c = n ? n->cmp : UINT64_MAX;
      x->NoBarrier_SetNext(i, c, n);
      if (c >= x->cmp && prev[i]->CasNext(i, n, x)) {
        break;
      }

      // advance and retry the CAS
      while (true) {
        c = 0;
        obs[i] = NULL;
        prev[i]->GetNext(i, &c, &obs[i]);
        if (c < x->cmp || KeyIsAfterNode(x->key, obs[i])) {
          prev[i] = obs[i];
        } else {
          break;
        }
      }
    }
  }
}

template<typename Key, class Comparator, class Extractor>
bool SkipList<Key,Comparator,Extractor>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, NULL, NULL);
  if (x != NULL && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

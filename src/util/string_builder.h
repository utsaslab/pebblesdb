// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "pebblesdb/slice.h"
#include <stdlib.h>

#ifndef STORAGE_LEVELDB_UTIL_STRING_BUILDER_H_
#define STORAGE_LEVELDB_UTIL_STRING_BUILDER_H_

namespace leveldb {

struct StringBuilder {
  StringBuilder()
      : m_buf(static_cast<char*>(malloc(8))),
        m_cap(8),
        m_sz(0) {
    m_cap = m_buf ? 8 : 0;
  }
  ~StringBuilder() throw () { if (m_buf) free(m_buf); }

  void destroy_memory() {
	  if (m_buf) {
		  free(m_buf);
		  m_buf = NULL;
		  m_sz = 0;
		  m_cap = 0;
	  }
  }

  void init_buffer(int size) {
	  if (m_buf) {
		  free(m_buf);
	  }
	  m_buf = static_cast<char*> (malloc(size));
	  m_cap = m_buf ? size: 0;
	  m_sz = 0;
  }

  void clear() {
	  m_sz = 0;
//	  m_buf = static_cast<char*>(realloc(m_buf, 8));
  }
  size_t size() const { return m_sz; }
  bool empty() const { return size() == 0; }
  Slice slice() const { return Slice(m_buf, m_sz); }
  const char* data() const { return m_buf; }
  char* data() { return m_buf; }
  void append(const leveldb::Slice& s) {
    append(s.data(), s.size());
  }
  void shrink(size_t sz) {
    assert(sz <= m_sz);
    m_sz = sz;
  }
  void append(const char* buf, size_t sz, int print = 0) {
    if (m_sz + sz > m_cap) {
      grow_at_least(sz, print);
    }

    char* ptr = m_buf + m_sz;
    memmove(ptr, buf, sz);
    m_sz += sz;
  }

 private:
  StringBuilder(const StringBuilder&);
  StringBuilder& operator = (const StringBuilder&);
  void grow_at_least(size_t sz, int print) {
    size_t new_cap = m_cap + (m_cap >> 1) + sz;
    new_cap = (new_cap + 31) & ~31ULL;
//    if (print == 1) {
//    	printf("Growing m_buf from %d to %d to accomodate key of size %d\n", m_cap, new_cap, sz);
//    }
    char* new_buf = static_cast<char*>(realloc(m_buf, new_cap));

    if (!new_buf) {
      throw std::bad_alloc();
    }

    m_buf = new_buf;
    m_cap = new_cap;
  }

  char* m_buf;
  size_t m_cap;
  size_t m_sz;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_STRING_BUILDER_H_

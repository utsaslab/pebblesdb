// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// See port_example.h for documentation for the following types/functions.

#ifndef STORAGE_LEVELDB_PORT_PORT_POSIX_H_
#define STORAGE_LEVELDB_PORT_PORT_POSIX_H_

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_ENDIAN_H
#include <endian.h>
#endif

#ifdef HAVE_MACHINE_ENDIAN_H
#include <machine/endian.h>
#endif

#ifdef HAVE_SYS_ENDIAN_H
#include <sys/endian.h>
#endif

#ifdef HAVE_SYS_ISA_DEFS_H
#include <sys/isa_defs.h>
#endif

#ifdef HAVE_SYS_ENDIAN_H
#include <sys/types.h>
#endif

#undef PLATFORM_IS_LITTLE_ENDIAN

#if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
# define PLATFORM_IS_LITTLE_ENDIAN (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
#elif defined(_BYTE_ORDER) && defined(_LITTLE_ENDIAN)
  // Due to a bug in the NDK x86 <sys/endian.h> definition,
  // _BYTE_ORDER must be used instead of __BYTE_ORDER on Android.
  // See http://code.google.com/p/android/issues/detail?id=39824
# define PLATFORM_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
#elif defined(_LITTLE_ENDIAN)
# define PLATFORM_IS_LITTLE_ENDIAN true
#endif

#include <pthread.h>
#ifdef SNAPPY
#include <snappy.h>
#endif
#include <stdint.h>
#include <string>
#include "port/atomic_pointer.h"

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

#ifdef HAVE_FFLUSH_UNLOCKED
// do nothing
#elif HAVE_FFLUSH
#define fflush_unlocked fflush
#else
#error "no fflush found"
#endif

#ifdef HAVE_FREAD_UNLOCKED
// do nothing
#elif HAVE_FREAD
#define fread_unlocked fread
#else
#error "no fread found"
#endif

#ifdef HAVE_FWRITE_UNLOCKED
// do nothing
#elif HAVE_FWRITE
#define fwrite_unlocked fwrite
#else
#error "no fwrite found"
#endif

#if HAVE_DECL_FDATASYNC
// do nothing
#elif HAVE_FSYNC
#define fdatasync fsync
#else
#error "no fdatasync/fsync found"
#endif

namespace leveldb {
namespace port {

static const bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

class CondVar;

class Mutex {
 public:
  Mutex();
  ~Mutex();

  void Lock();
  void Unlock();
  void AssertHeld() { }

 private:
  friend class CondVar;
  pthread_mutex_t mu_;

  // No copying
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  CondVar():mu_() { }
  ~CondVar();

  void InitMutex(Mutex* mu);
  void Wait();
  void Signal();
  void SignalAll();

 private:
  pthread_cond_t cv_;
  Mutex* mu_;

  // No copying
  CondVar(const CondVar&);
  void operator=(const CondVar&);
};

typedef pthread_once_t OnceType;
#define LEVELDB_ONCE_INIT PTHREAD_ONCE_INIT
extern void InitOnce(OnceType* once, void (*initializer)());

inline bool Snappy_Compress(const char* input, size_t length,
                            ::std::string* output) {
#ifdef SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#else
  (void)input;
  (void)length;
  (void)output;
#endif

  return false;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#ifdef SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  (void)input;
  (void)length;
  (void)result;
  return false;
#endif
}

inline bool Snappy_Uncompress(const char* input, size_t length,
                              char* output) {
#ifdef SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  (void)input;
  (void)length;
  (void)output;
  return false;
#endif
}

inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) {
  (void)func;
  (void)arg;
  return false;
}

} // namespace port
} // namespace leveldb

#endif  // STORAGE_LEVELDB_PORT_PORT_POSIX_H_

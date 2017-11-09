// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <deque>
#include <set>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "pebblesdb/env.h"
#include "pebblesdb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"

namespace leveldb {

namespace {

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

class PosixSequentialFile: public SequentialFile {
 private:
  PosixSequentialFile(const PosixSequentialFile&);
  PosixSequentialFile& operator = (const PosixSequentialFile&);
  std::string filename_;
  FILE* file_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~PosixSequentialFile() { fclose(file_); }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    size_t r = fread_unlocked(scratch, 1, n, file_);
    *result = Slice(scratch, r);
    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};

// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  virtual ~PosixRandomAccessFile() { close(fd_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }
};

// Helper class to limit mmap file usage so that we do not end up
// running out virtual memory or running into kernel performance
// problems for very large databases.
class MmapLimiter {
 public:
  // Up to 1000 mmaps for 64-bit binaries; none for smaller pointer sizes.
  MmapLimiter()
    : mu_(),
      allowed_() {
    SetAllowed(sizeof(void*) >= 8 ? 1000 : 0);
  }

  // If another mmap slot is available, acquire it and return true.
  // Else return false.
  bool Acquire() {
    if (GetAllowed() <= 0) {
      return false;
    }
    MutexLock l(&mu_);
    intptr_t x = GetAllowed();
    if (x <= 0) {
      return false;
    } else {
      SetAllowed(x - 1);
      return true;
    }
  }

  // Release a slot acquired by a previous call to Acquire() that returned true.
  void Release() {
    MutexLock l(&mu_);
    SetAllowed(GetAllowed() + 1);
  }

 private:
  port::Mutex mu_;
  port::AtomicPointer allowed_;

  intptr_t GetAllowed() const {
    return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
  }

  // REQUIRES: mu_ must be held
  void SetAllowed(intptr_t v) {
    allowed_.Release_Store(reinterpret_cast<void*>(v));
  }

  MmapLimiter(const MmapLimiter&);
  void operator=(const MmapLimiter&);
};

// mmap() based random-access
class PosixMmapReadableFile: public RandomAccessFile {
 private:
  PosixMmapReadableFile(const PosixMmapReadableFile&);
  PosixMmapReadableFile& operator = (const PosixMmapReadableFile&);
  std::string filename_;
  void* mmapped_region_;
  size_t length_;
  MmapLimiter* limiter_;

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  PosixMmapReadableFile(const std::string& fname, void* base, size_t length,
                        MmapLimiter* limiter)
      : filename_(fname), mmapped_region_(base), length_(length),
        limiter_(limiter) {
  }

  virtual ~PosixMmapReadableFile() {
    munmap(mmapped_region_, length_);
    limiter_->Release();
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* /*scratch*/) const {
    Status s;
    if (offset + n > length_) {
      *result = Slice();
      s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
    }
    return s;
  }
};

class PosixWritableFile : public WritableFile {
 private:
  PosixWritableFile(const PosixWritableFile&);
  PosixWritableFile& operator = (const PosixWritableFile&);
  std::string filename_;
  FILE* file_;

 public:
  PosixWritableFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }

  ~PosixWritableFile() {
    if (file_ != NULL) {
      // Ignoring any potential errors
      fclose(file_);
    }
  }

  virtual Status Append(const Slice& data) {
    size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);
    if (r != data.size()) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  virtual Status Close() {
    Status result;
    if (fclose(file_) != 0) {
      result = IOError(filename_, errno);
    }
    file_ = NULL;
    return result;
  }

  virtual Status Flush() {
    if (fflush_unlocked(file_) != 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    const char* f = filename_.c_str();
    const char* sep = strrchr(f, '/');
    Slice basename;
    std::string dir;
    if (sep == NULL) {
      dir = ".";
      basename = f;
    } else {
      dir = std::string(f, sep - f);
      basename = sep + 1;
    }
    Status s;
    if (basename.starts_with("MANIFEST")) {
      int fd = open(dir.c_str(), O_RDONLY);
      if (fd < 0) {
        s = IOError(dir, errno);
      } else {
        if (fsync(fd) < 0) {
          s = IOError(dir, errno);
        }
        close(fd);
      }
    }
    return s;
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();
    if (!s.ok()) {
      return s;
    }
    if (fflush_unlocked(file_) != 0 ||
        fdatasync(fileno(file_)) != 0) {
      s = Status::IOError(filename_, strerror(errno));
    }
    return s;
  }
};

// We preallocate up to an extra megabyte and use memcpy to append new
// data to the file.  This is safe since we either properly close the
// file before reading from it, or for log files, the reading code
// knows enough to skip zero suffixes.
class PosixMmapFile : public ConcurrentWritableFile {
 private:
  PosixMmapFile(const PosixMmapFile&);
  PosixMmapFile& operator = (const PosixMmapFile&);
  struct MmapSegment{
    char* base_;
  };

  std::string filename_;    // Path to the file
  int fd_;                  // The open file
  const size_t block_size_; // System page size
  uint64_t end_offset_;     // Where does the file end?
  MmapSegment* segments_;   // mmap'ed regions of memory
  size_t segments_sz_;      // number of segments that are truncated
  bool trunc_in_progress_;  // is there an ongoing truncate operation?
  uint64_t trunc_waiters_;  // number of threads waiting for truncate
  port::Mutex mtx_;         // Protection for state
  port::CondVar cnd_;       // Wait for truncate

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) {
    return ((x + y - 1) / y) * y;
  }

  bool GrowViaTruncate(uint64_t block) {
    mtx_.Lock();
    while (trunc_in_progress_ && segments_sz_ <= block) {
      ++trunc_waiters_;
      cnd_.Wait();
      --trunc_waiters_;
    }
    uint64_t cur_sz = segments_sz_;
    trunc_in_progress_ = cur_sz <= block;
    mtx_.Unlock();

    bool error = false;
    if (cur_sz <= block) {
      uint64_t new_sz = ((block + 7) & ~7ULL) + 1;
      if (ftruncate(fd_, new_sz * block_size_) < 0) {
        error = true;
      }
      MmapSegment* new_segs = new MmapSegment[new_sz];
      MmapSegment* old_segs = NULL;
      mtx_.Lock();
      old_segs = segments_;
      for (size_t i = 0; i < segments_sz_; ++i) {
        new_segs[i].base_ = old_segs[i].base_;
      }
      for (size_t i = segments_sz_; i < new_sz; ++i) {
        new_segs[i].base_ = NULL;
      }
      segments_ = new_segs;
      segments_sz_ = new_sz;
      trunc_in_progress_ = false;
      cnd_.SignalAll();
      mtx_.Unlock();
      delete[] old_segs;
    }
    return !error;
  }

  bool UnmapSegment(char* base) {
    return munmap(base, block_size_) >= 0;
  }

  // Call holding mtx_
  char* GetSegment(uint64_t block) {
    char* base = NULL;
    mtx_.Lock();
    size_t cur_sz = segments_sz_;
    if (block < segments_sz_) {
      base = segments_[block].base_;
    }
    mtx_.Unlock();
    if (base) {
      return base;
    }
    if (cur_sz <= block) {
      if (!GrowViaTruncate(block)) {
        return NULL;
      }
    }
    void* ptr = mmap(NULL, block_size_, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd_, block * block_size_);
    if (ptr == MAP_FAILED) {
      abort();
      return NULL;
    }
    bool unmap = false;
    mtx_.Lock();
    assert(block < segments_sz_);
    if (segments_[block].base_) {
      base = segments_[block].base_;
      unmap = true;
    } else {
      base = reinterpret_cast<char*>(ptr);
      segments_[block].base_ = base;
      unmap = false;
    }
    mtx_.Unlock();
    if (unmap) {
      if (!UnmapSegment(reinterpret_cast<char*>(ptr))) {
        return NULL;
      }
    }
    return base;
  }

 public:
  PosixMmapFile(const std::string& fname, int fd, size_t page_size)
      : filename_(fname),
        fd_(fd),
        block_size_(Roundup(page_size, 262144)),
        end_offset_(0),
        segments_(NULL),
        segments_sz_(0),
        trunc_in_progress_(false),
        trunc_waiters_(0),
        mtx_(),
        cnd_(&mtx_) {
    assert((page_size & (page_size - 1)) == 0);
  }

  ~PosixMmapFile() {
    PosixMmapFile::Close();
  }

  virtual Status WriteAt(uint64_t offset, const Slice& data) {
    const uint64_t end = offset + data.size();
    const char* src = data.data();
    uint64_t rem = data.size();
    mtx_.Lock();
    end_offset_ = end_offset_ < end ? end : end_offset_;
    mtx_.Unlock();
    while (rem > 0) {
      const uint64_t block = offset / block_size_;
      char* base = GetSegment(block);
      if (!base) {
        return IOError(filename_, errno);
      }
      const uint64_t loff = offset - block * block_size_;
      uint64_t n = block_size_ - loff;
      n = n < rem ? n : rem;
      memmove(base + loff, src, n);
      rem -= n;
      src += n;
      offset += n;
    }
    return Status::OK();
  }

  virtual Status Append(const Slice& data) {
    mtx_.Lock();
    uint64_t offset = end_offset_;
    mtx_.Unlock();
    return WriteAt(offset, data);
  }

  virtual Status Close() {
    Status s;
    int fd;
    MmapSegment* segments;
    size_t end_offset;
    size_t segments_sz;
    mtx_.Lock();
    fd = fd_;
    fd_ = -1;
    end_offset = end_offset_;
    end_offset_ = 0;
    segments = segments_;
    segments_ = NULL;
    segments_sz = segments_sz_;
    segments_sz_ = 0;
    mtx_.Unlock();
    if (fd < 0) {
      return s;
    }
    for (size_t i = 0; i < segments_sz; ++i) {
      if (segments[i].base_ != NULL &&
          munmap(segments[i].base_, block_size_) < 0) {
        s = IOError(filename_, errno);
      }
    }
    delete[] segments;
    if (ftruncate(fd, end_offset) < 0) {
      s = IOError(filename_, errno);
    }
    if (close(fd) < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  Status Flush() {
    return Status::OK();
  }

  Status SyncDirIfManifest() {
    const char* f = filename_.c_str();
    const char* sep = strrchr(f, '/');
    Slice basename;
    std::string dir;
    if (sep == NULL) {
      dir = ".";
      basename = f;
    } else {
      dir = std::string(f, sep - f);
      basename = sep + 1;
    }
    Status s;
    if (basename.starts_with("MANIFEST")) {
      int fd = open(dir.c_str(), O_RDONLY);
      if (fd < 0) {
        s = IOError(dir, errno);
      } else {
        if (fsync(fd) < 0) {
          s = IOError(dir, errno);
        }
        close(fd);
      }
    }
    return s;
  }

  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();

    if (!s.ok()) {
      return s;
    }

    size_t block = 0;
    while (true) {
      char* base = NULL;
      mtx_.Lock();
      if (block < segments_sz_) {
        base = segments_[block].base_;
      }
      mtx_.Unlock();
      if (!base) {
        break;
      }
      if (msync(base, block_size_, MS_SYNC) < 0) {
        s = IOError(filename_, errno);
      }
      ++block;
    }
    return s;
  }
};

static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
}

class PosixFileLock : public FileLock {
 public:
  PosixFileLock() : fd_(-1), name_() { }
  int fd_;
  std::string name_;
};

// Set of locked files.  We keep a separate set instead of just
// relying on fcntrl(F_SETLK) since fcntl(F_SETLK) does not provide
// any protection against multiple uses from the same process.
class PosixLockTable {
 private:
  port::Mutex mu_;
  std::set<std::string> locked_files_;
 public:
  PosixLockTable() : mu_(), locked_files_() { }
  bool Insert(const std::string& fname) {
    MutexLock l(&mu_);
    return locked_files_.insert(fname).second;
  }
  void Remove(const std::string& fname) {
    MutexLock l(&mu_);
    locked_files_.erase(fname);
  }
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  virtual ~PosixEnv() {
    fprintf(stderr, "Destroying Env::Default()\n");
    abort();
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    FILE* f = fopen(fname.c_str(), "r");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixSequentialFile(fname, f);
      return Status::OK();
    }
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    Status s;
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
      s = IOError(fname, errno);
    } else if (false && mmap_limit_.Acquire()) {
      uint64_t size;
      s = GetFileSize(fname, &size);
      if (s.ok()) {
        void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
        if (base != MAP_FAILED) {
          *result = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
        } else {
          s = IOError(fname, errno);
        }
      }
      close(fd);
      if (!s.ok()) {
        mmap_limit_.Release();
      }
    } else {
      *result = new PosixRandomAccessFile(fname, fd);
    }
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    Status s;
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      s = IOError(fname, errno);
    } else {
      *result = new PosixWritableFile(fname, f);
    }
    return s;
  }

  virtual Status NewConcurrentWritableFile(const std::string& fname,
                                           ConcurrentWritableFile** result) {
    Status s;
    const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
      *result = NULL;
      s = IOError(fname, errno);
    } else {
      *result = new PosixMmapFile(fname, fd, page_size_);
    }
    return s;
  }

  virtual bool FileExists(const std::string& fname) {
    return access(fname.c_str(), F_OK) == 0;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  }

  virtual Status CreateDir(const std::string& name) {
    Status result;
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status DeleteDir(const std::string& name) {
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  virtual Status RenameFile(const std::string& src, const std::string& target) {
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status CopyFile(const std::string& src, const std::string& target) {
    Status result;
    int fd1 = -1;
    int fd2 = -1;

    if (result.ok() && (fd1 = open(src.c_str(), O_RDONLY)) < 0) {
      result = IOError(src, errno);
    }
    if (result.ok() && (fd2 = open(target.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0) {
      result = IOError(target, errno);
    }

    ssize_t amt = 0;
    char buf[512];

    while (result.ok() && (amt = read(fd1, buf, 512)) > 0) {
      if (write(fd2, buf, amt) != amt) {
        result = IOError(src, errno);
      }
    }

    if (result.ok() && amt < 0) {
      result = IOError(src, errno);
    }

    if (fd1 >= 0 && close(fd1) < 0) {
      if (result.ok()) {
        result = IOError(src, errno);
      }
    }

    if (fd2 >= 0 && close(fd2) < 0) {
      if (result.ok()) {
        result = IOError(target, errno);
      }
    }

    return result;
  }

  virtual Status LinkFile(const std::string& src, const std::string& target) {
    Status result;
    if (link(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (!locks_.Insert(fname)) {
      close(fd);
      result = Status::IOError("lock " + fname, "already held by process");
    } else if (LockOrUnlock(fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      close(fd);
      locks_.Remove(fname);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      my_lock->name_ = fname;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    locks_.Remove(my_lock->name_);
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg);

  virtual void StartThread(void (*function)(void* arg), void* arg);

  virtual pthread_t StartThreadAndReturnThreadId(void (*function)(void* arg), void* arg);

  virtual void WaitForThread(unsigned long int th, void** return_status);

  virtual Status GetTestDirectory(std::string* result) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixLogger(f, &PosixEnv::gettid);
      return Status::OK();
    }
  }

  virtual uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) {
    usleep(micros);
  }

  virtual pthread_t GetThreadId() {
	pthread_t tid = pthread_self();
	return tid;
  }

 private:
  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      abort();
    }
  }

  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return NULL;
  }

  size_t page_size_;
  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  pthread_t bgthread_;
  bool started_bgthread_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;

  PosixLockTable locks_;
  MmapLimiter mmap_limit_;
};

PosixEnv::PosixEnv() : page_size_(getpagesize()),
                       mu_(),
                       bgsignal_(),
                       bgthread_(),
                       started_bgthread_(false),
                       queue_(),
                       locks_(),
                       mmap_limit_() {
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
}

void PosixEnv::Schedule(void (*function)(void*), void* arg) {
  PthreadCall("lock", pthread_mutex_lock(&mu_));

  // Start background thread if necessary
  if (!started_bgthread_) {
    started_bgthread_ = true;
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_, NULL,  &PosixEnv::BGThreadWrapper, this));
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

void PosixEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}
static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
}

pthread_t PosixEnv::StartThreadAndReturnThreadId(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
  return t;
}

void PosixEnv::WaitForThread(unsigned long int th, void** return_status) {
  PthreadCall("wait for thread",
              pthread_join((pthread_t)th, return_status));
}
}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  return default_env;
}

}  // namespace leveldb

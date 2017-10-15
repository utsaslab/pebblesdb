// Copyright (c) 2012-2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "pebblesdb/env.h"
#include "pebblesdb/iterator.h"
#include "pebblesdb/options.h"
#include "pebblesdb/status.h"
#include "pebblesdb/table.h"
#include "pebblesdb/write_batch.h"
#include "util/logging.h"

namespace leveldb {

namespace {

bool GuessType(const std::string& fname, FileType* type) {
  size_t pos = fname.rfind('/');
  std::string basename;
  if (pos == std::string::npos) {
    basename = fname;
  } else {
    basename = std::string(fname.data() + pos + 1, fname.size() - pos - 1);
  }
  uint64_t ignored;
  return ParseFileName(basename, &ignored, type);
}

// Notified when log reader encounters corruption.
class CorruptionReporter : public log::Reader::Reporter {
 public:
  virtual void Corruption(size_t bytes, const Status& status) {
    printf("corruption: %d bytes; %s\n",
            static_cast<int>(bytes),
            status.ToString().c_str());
  }
};

// Print contents of a log file. (*func)() is called on every record.
bool PrintLogContents(Env* env, const std::string& fname,
                      void (*func)(Slice)) {
  SequentialFile* file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    return false;
  }
  CorruptionReporter reporter;
  log::Reader reader(file, &reporter, true, 0);
  Slice record;
  std::string scratch;
  while (reader.ReadRecord(&record, &scratch)) {
    (*func)(record);
  }
  delete file;
  return true;
}

// Called on every item found in a WriteBatch.
class WriteBatchItemPrinter : public WriteBatch::Handler {
 public:
  WriteBatchItemPrinter()
    : offset_(),
      sequence_() {
  }
  uint64_t offset_;
  uint64_t sequence_;

  virtual void Put(const Slice& /*key*/, const Slice& /*value*/) {
  }
  virtual void Delete(const Slice& /*key*/) {
  }
  virtual void HandleGuard(const Slice& key, unsigned level) {
    assert(0);
  }
};

// Called on every log record (each one of which is a WriteBatch)
// found in a kLogFile.
static void WriteBatchPrinter(Slice record) {
  if (record.size() < 12) {
    printf("log record length %d is too small\n",
           static_cast<int>(record.size()));
    return;
  }
  WriteBatch batch;
  WriteBatchInternal::SetContents(&batch, record);
  WriteBatchItemPrinter batch_item_printer;
  Status s = batch.Iterate(&batch_item_printer);
  if (!s.ok()) {
    fprintf(stderr, "error: %s\n", s.ToString().c_str());
  }
}

bool DumpLog(Env* env, const std::string& fname) {
  return PrintLogContents(env, fname, WriteBatchPrinter);
}

// Called on every log record (each one of which is a WriteBatch)
// found in a kDescriptorFile.
static void VersionEditPrinter(Slice record) {
  VersionEdit edit;
  Status s = edit.DecodeFrom(record);
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    return;
  }
}

bool DumpDescriptor(Env* env, const std::string& fname) {
  return PrintLogContents(env, fname, VersionEditPrinter);
}

bool DumpTable(Env* env, const std::string& fname) {
  uint64_t file_size;
  RandomAccessFile* file = NULL;
  Table* table = NULL;
  Status s = env->GetFileSize(fname, &file_size);
  if (s.ok()) {
    s = env->NewRandomAccessFile(fname, &file);
  }
  if (s.ok()) {
    // We use the default comparator, which may or may not match the
    // comparator used in this database. However this should not cause
    // problems since we only use Table operations that do not require
    // any comparisons.  In particular, we do not call Seek or Prev.
    s = Table::Open(Options(), file, file_size, &table, NULL);
  }
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.ToString().c_str());
    delete table;
    delete file;
    return false;
  }

  ReadOptions ro;
  ro.verify_checksums = true;
  Iterator* iter = table->NewIterator(ro);
  Iterator* verify = table->NewIterator(ro);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (!iter->status().ok()) {
      fprintf(stderr, "bad iteration %s\n", iter->status().ToString().c_str());
    }
    ParsedInternalKey key;
    if (!ParseInternalKey(iter->key(), &key)) {
      fprintf(stderr, "badkey '%s' => '%s'\n",
             EscapeString(iter->key()).c_str(),
             EscapeString(iter->value()).c_str());
    }
    verify->SeekToFirst();
    if (!verify->status().ok()) {
      fprintf(stderr, "bad iteration %s\n", verify->status().ToString().c_str());
    }
    verify->Seek(key.user_key);
    if (!verify->status().ok()) {
      fprintf(stderr, "bad iteration %s\n", verify->status().ToString().c_str());
    }
  }
  s = iter->status();
  if (!s.ok()) {
    fprintf(stderr, "iterator error: %s\n", s.ToString().c_str());
  }

  delete iter;
  delete verify;
  delete table;
  delete file;
  return true;
}

bool DumpFile(Env* env, const std::string& fname) {
  FileType ftype;
  if (!GuessType(fname, &ftype)) {
    fprintf(stderr, "%s: unknown file type\n", fname.c_str());
    return false;
  }
  switch (ftype) {
    case kLogFile:         return DumpLog(env, fname);
    case kDescriptorFile:  return DumpDescriptor(env, fname);
    case kTableFile:       return DumpTable(env, fname);

    case kDBLockFile:
    case kCurrentFile:
    case kTempFile:
    case kInfoLogFile:
    default: {
      fprintf(stderr, "%s: not a dump-able file type\n", fname.c_str());
      break;
    }
  }
  return false;
}

bool HandleDumpCommand(Env* env, char** files, int num) {
  bool ok = true;
  for (int i = 0; i < num; i++) {
    ok &= DumpFile(env, files[i]);
  }
  return ok;
}

}
}  // namespace leveldb

int main(int argc, char** argv) {
  leveldb::Env* env = leveldb::Env::Default();
  bool ok = true;
  ok = leveldb::HandleDumpCommand(env, argv+1, argc-1);
  return (ok ? 0 : 1);
}

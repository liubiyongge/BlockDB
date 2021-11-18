// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/compaction_state.h"
#include "db/db_iter.h"
#include "db/db_stats.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/thread_pool.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "leveldb/table_editor.h"
#include "port/port.h"
#include "table/block.h"
#include "table/format.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch *batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex *mu)
      : cv(mu), batch(nullptr), sync(false), done(false) {
    //
  }
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T *ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

Options SanitizeOptions(const std::string &dbname,
                        const InternalKeyComparator *icmp,
                        const InternalFilterPolicy *ipolicy,
                        const Options &src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles,
              50000);                                         // 74 ~ 50k
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);  // 64KB ~ 1GB
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);       // 1MB ~ 1GB
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);          // 1KB ~ 4MB
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
    result.block_cache = NewLRUCache(8 << 20);  // 8 MB
  }
  return result;
}

DBImpl::DBImpl(const Options &raw_options, const std::string &dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),  // internal key
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(NULL),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL),
      flush_cv_(&flush_mtx_) {
  has_imm_.Release_Store(NULL);

  flush_atomic_.Release_Store(nullptr);
  bg_flush_scheduled_ = false;

  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

  versions_ =
      new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_);

  num_workers_ = options_.num_workers;
  thread_pool_ = new ThreadPool(num_workers_);

  for (uint64_t i = 0; i < num_workers_; i++) {
    CacheID_queue_.push(i);
  }

  // pointers targeted to upper-level and lower-level SSTables.
  if (options_.direct_io && options_.compaction != kTableCompaction) {
    upper_tables_ = nullptr;
    lower_tables_ = nullptr;
    // lower_tables_ = new char *[num_workers_];
    // for (uint64_t i = 0; i < num_workers_; i++) {
    //   int ret = posix_memalign((void **)&lower_tables_[i], 4096, 256 << 20);
    //   if (ret != 0) {
    //     printf("lower_tables: failed to posix_memalign: %s\n",
    //     strerror(errno)); exit(0);
    //   }
    //   // 256 MB
    //   memset(lower_tables_[i], 0, 256 << 20);
    // }
  }

  // caches used to buffer new sstables during compaction operations.
  // if (options_.direct_io) {
  write_caches_ = new char *[num_workers_];
  for (uint64_t i = 0; i < num_workers_; i++) {
    int ret = posix_memalign((void **)&write_caches_[i], 4096, 32 << 20);
    if (ret != 0) {
      printf("write_tables: failed to posix_memalign: %s\n", strerror(errno));
      exit(0);
    }
    // 256 MB
    memset(write_caches_[i], 0, 32 << 20);
  }
  // }

  // flush_space
  // if (options_.direct_io) {
  flush_buffer_ = nullptr;
  int ret = posix_memalign((void **)&flush_buffer_, 4096, 32 << 20);
  if (ret != 0) {
    printf("Failed to posix_memalign: %s\n", strerror(errno));
    exit(0);
  }
  // 256 MB
  memset(flush_buffer_, 0, 32 << 20);
  //  }

  hybrid = new HybridCompaction(env_, options_, dbname_, thread_pool_,
                                table_cache_, internal_comparator_,
                                &pending_outputs_, versions_, CacheID_queue_,
                                upper_tables_, write_caches_, &mutex_, stats_);

  switch (options_.compaction) {
    case CompactionType::kBlockCompaction:
      fprintf(stdout, "use Block Compaction!\n");
      break;
    case CompactionType::kTableCompaction:
      fprintf(stdout, "use Table Compaction!\n");
      break;
    default:
      fprintf(stdout, "error compaction!\n");
      exit(0);
  }

  fprintf(stdout, "write buffer: %.2lf\n",
          1.0 * options_.write_buffer_size / (1 << 20));
  fprintf(stdout, "max file size: %.2lf\n",
          1.0 * options_.max_file_size / (1 << 20));
  fprintf(stdout, "direct io: %d\n", options_.direct_io);
  fprintf(stdout, "selective: %d\n", options_.selective_compaction);
  fprintf(stdout, "num workers: %ld\n", options_.num_workers);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }

  // if (options_.direct_io) {
  //
  // if (options_.compaction != kTableCompaction) {
  //   for (uint64_t i = 0; i < num_workers_; i++) {
  //     free(lower_tables_[i]);
  //   }
  //   delete[] lower_tables_;
  //   lower_tables_ = nullptr;
  // }

  for (uint64_t i = 0; i < num_workers_; i++) {
    free(write_caches_[i]);
  }
  delete[] write_caches_;
  write_caches_ = nullptr;

  delete flush_buffer_;
  flush_buffer_ = nullptr;

  // }
  delete hybrid;
  delete thread_pool_;
  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile *file;
  Status s = env_->NewWritableFile(manifest, &file);
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

void DBImpl::MaybeIgnoreError(Status *s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  cnt_delete++;
  if (cnt_delete % lazy_delete) return;

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
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
      }

      if (!keep) {
        Log(options_.info_log, "Delete type=%d #%lld\n", int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
}

Status DBImpl::Recover(VersionEdit *edit, bool *save_manifest) {
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
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
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
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool *save_manifest, VersionEdit *edit,
                              SequenceNumber *max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env *env;
    Logger *info_log;
    const char *fname;
    Status *status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status &s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile *file;
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
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable *mem = NULL;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
      mem->Unref();
      mem = NULL;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == NULL);
    assert(log_ == NULL);
    assert(mem_ == NULL);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != NULL) {
        mem_ = mem;
        mem = NULL;
      } else {
        // mem can be NULL if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != NULL) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable *mem, VersionEdit *edit,
                                Version *base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator *iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  uint64_t flush_time = 0;
  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta,
                   flush_buffer_, flush_time);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size, meta.data_size,
                  meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.read_bytes = 0;
  stats.write_bytes = meta.file_size;
  stats.micros = env_->NowMicros() - start_micros;
  stats_[level].Add(stats);
  return s;
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version *base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  //  if (s.ok() && shutting_down_.Acquire_Load()) {
  //    s = Status::IOError("Deleting DB during memtable compaction");
  //  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    // DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice *begin,
                               const Slice *end) {
  assert(level >= 0);
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
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

void DBImpl::CompactRange(const Slice *begin, const Slice *end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version *base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }

  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap

  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::RecordBackgroundError(const Status &s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == NULL && manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void *db) {
  reinterpret_cast<DBImpl *>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();
  if (imm_ != NULL) {
    CompactMemTable();
    return;
  }

  Compaction *c = nullptr;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction *m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual) {
    CompactionType compaction_type = options_.compaction;
    bool is_trivial = c->IsTrivialMove();
    if (compaction_type == CompactionType::kBlockCompaction &&
        options_.selective_compaction && is_trivial) {
      /**
       * When sstables are moved into the last level, we may rebuild
       * sstables to recycle obsolete spaces. We consider two factors.
       * 1. the last level is set to 5 (100 GB).
       * 2. file size is larger than data size.
       */
      FileMetaData *f = c->input(0, 0);
      if (c->level() >= options_.last_level &&
          f->file_size > 1.20 * f->data_size) {
        is_trivial = false;
      }
    }
    if (is_trivial) {
      // ------------------ Trivial Move ------------------
      FileMetaData *f = c->input(0, 0);
      c->edit()->DeleteFile(c->level(), f->number);
      c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->data_size,
                         f->smallest, f->largest);
      status = versions_->LogAndApply(c->edit(), &mutex_);
      if (!status.ok()) {
        RecordBackgroundError(status);
      }
      VersionSet::LevelSummaryStorage tmp;
      Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
          static_cast<unsigned long long>(f->number), c->level() + 1,
          static_cast<unsigned long long>(f->file_size),
          status.ToString().c_str(), versions_->LevelSummary(&tmp));
    } else {
      CompactionState *compact = new CompactionState(c);
      if (compaction_type == CompactionType::kTableCompaction) {
        status = DoSimpleCompactionWork(compact);
      } else {
        if (c->num_input_files(1) > 0 && c->num_input_files(0) > 0 &&
            c->level() > 0) {
          status = DoHybridCompactionWork(compact);
        } else {
          status = DoSimpleCompactionWork(compact);
        }
      }
      if (!status.ok()) {
        RecordBackgroundError(status);
      }
      stats_[compact->compaction->level() + 1].num_compactions++;
      CleanupCompaction(compact);
      c->ReleaseInputs();
      uint64_t start_micros = env_->NowMicros();
      DeleteObsoleteFiles();
      all_delete += (env_->NowMicros() - start_micros);
    }
  }
  delete c;
  c = nullptr;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction *m = manual_compaction_;
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
}

void DBImpl::CleanupCompaction(CompactionState *compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output &out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState *compact) {
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

Status DBImpl::FinishCompactionOutputFile(CompactionState *compact,
                                          Iterator *input) {
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
  compact->current_output()->file_size = current_bytes;
  compact->current_output()->data_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = nullptr;

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator *iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

Status DBImpl::InstallCompactionResults(CompactionState *compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  const int level = compact->compaction->level();
  VersionEdit *edit = compact->compaction->edit();

  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < compact->compaction->num_input_files(which); i++) {
      FileMetaData *meta = compact->compaction->input(which, i);
      edit->DeleteFile(level + which, meta->number);
      table_cache_->Evict(meta->number, meta->file_size);
    }
  }

  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output &out = compact->outputs[i];
    edit->AddFile(level + 1, out.number, out.file_size, out.data_size,
                  out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState *compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
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

  Iterator *input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load();) {
    // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();

    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
              0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
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

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      // Close output file if it is big enough
      if (compact->builder->DataSize() >=
          compact->compaction->MaxOutputTableSize()) {
        status = FinishCompactionOutputFile(compact, input);
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
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.read_bytes += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.write_bytes += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {
struct IterState {
  port::Mutex *mu;
  Version *version;
  MemTable *mem;
  MemTable *imm;
};

static void CleanupIteratorState(void *arg1, void *arg2) {
  IterState *state = reinterpret_cast<IterState *>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator *DBImpl::NewInternalIterator(const ReadOptions &options,
                                      SequenceNumber *latest_snapshot,
                                      uint32_t *seed) {
  IterState *cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator *> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator *internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator *DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions &options, const Slice &key,
                   std::string *value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot =
        reinterpret_cast<const SnapshotImpl *>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable *mem = mem_;
  MemTable *imm = imm_;
  Version *current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
      gMemTableHit++;
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
      gMemTableHit++;
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  // if (have_stat_update && current->UpdateStats(stats)) {
  //   MaybeScheduleCompaction();
  // }

  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

Iterator *DBImpl::NewIterator(const ReadOptions &options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator *iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
           ? reinterpret_cast<const SnapshotImpl *>(options.snapshot)->number_
           : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot *DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot *s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl *>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions &o, const Slice &key, const Slice &val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions &options, const Slice &key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions &options, WriteBatch *my_batch) {
  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == NULL);
  uint64_t last_sequence = versions_->LastSequence();
  Writer *last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch *updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer *ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch *DBImpl::BuildBatchGroup(Writer **last_writer) {
  assert(!writers_.empty());
  Writer *first = writers_.front();
  WriteBatch *result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer *>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer *w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  options_.level0_slowdown_writes_trigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
    } else if (versions_->NumLevelFiles(0) >=
               options_.level0_stop_writes_trigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile *lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice &property, std::string *value) {
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
  } else if (in == "stats") {
    char buf[1024];
    // ---------------------------------------------------------------------------------------
    memset(buf, 0, 1024);
    snprintf(buf, sizeof(buf),
             "Level  Files  FileSize(MB)  DataSize(MB)  Time(sec)  Read(MB)  "
             "Write(MB)  Compaction\n"
             "---------------------------------------------------------------"
             "---------------------\n");
    value->append(buf);
    // body
    memset(buf, 0, 1024);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      snprintf(buf, sizeof(buf),
               "%3d %8d %13.0lf %13.0lf %10.0lf %9.0lf %10.0lf %11ld\n", level,
               files, versions_->NumLevelFileBytes(level) / 1048576.0,
               versions_->NumLevelDataBytes(level) / 1048576.0,
               1.0 * stats_[level].micros / 1e6,
               1.0 * stats_[level].read_bytes / 1048576.0,
               1.0 * stats_[level].write_bytes / 1048576.0,
               stats_[level].num_compactions);
      value->append(buf);
      memset(buf, 0, 1024);
    }
    // ---------------------------------------------------------------------------------------
    memset(buf, 0, 1024);
    snprintf(buf, sizeof(buf),
             "Level  TabComBytes(MB)  BlkComBytes(MB)  NumTabCom  NumBlkCom\n"
             "-------------------------------------------------------------\n");
    value->append(buf);
    // body
    memset(buf, 0, 1024);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      snprintf(buf, sizeof(buf), "%3d %18.0lf %16.0lf %10ld %10ld\n", level,
               stats_[level].table_write_bytes / 1048576.0,
               stats_[level].block_write_bytes / 1048576.0,
               stats_[level].num_table_compactions,
               stats_[level].num_block_compactions);
      value->append(buf);
      memset(buf, 0, 1024);
    }
    value->append(buf);
    // ---------------------------------------------------------------------------------------
    memset(buf, 0, 1024);
    snprintf(buf, sizeof(buf),
             "Level  DoTable(s)  DoBlock(s)  FinishTable(s)  FinishBlock(s)\n"
             "-------------------------------------------------------------\n");
    value->append(buf);
    // body
    memset(buf, 0, 1024);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      snprintf(buf, sizeof(buf), "%3d %13.0lf %11.0lf %15.0lf %15.0lf\n", level,
               stats_[level].table_micros / 1e6,
               stats_[level].block_micros / 1e6,
               stats_[level].table_finish_micros / 1e6,
               stats_[level].block_finish_micros / 1e6);
      value->append(buf);
      memset(buf, 0, 1024);
    }
    value->append(buf);
    // ---------------------------------------------------------------------------------------
    memset(buf, 0, 1024);
    snprintf(buf, sizeof(buf),
             "Level  TableRead(MB)  BlockRead(MB)\n"
             "-----------------------------------\n");
    value->append(buf);
    // body
    memset(buf, 0, 1024);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      snprintf(buf, sizeof(buf), "%3d %16.0lf %14.0lf\n", level,
               stats_[level].table_read_bytes / 1048576.0,
               stats_[level].block_read_bytes / 1048576.0);
      value->append(buf);
      memset(buf, 0, 1024);
    }
    value->append(buf);
    //
    memset(buf, 0, 1024);
    snprintf(buf, sizeof(buf), "Delete Time(s): %f\n", all_delete / 1e6);
    value->append(buf);
    //
    memset(buf, 0, 1024);
    snprintf(buf, sizeof(buf),
             "Prepare File: %ld Prepare Chosen: %ld Prepare Data: %ld\n",
             gPrepareFile, gPrepareChosen, gPrepareData);
    value->append(buf);
    return true;
  } else if (in == "hit-ratio") {
    char buf[1024];
    memset(buf, 0, 1024);
    snprintf(buf, sizeof(buf),
             "MemTableHit  TableCacheHit  TableCacheMiss  BlockCacheHit  "
             "BlockCacheMiss\n"
             "----------------------------------------------------------"
             "--------------\n");
    value->append(buf);
    //
    memset(buf, 0, 1024);
    snprintf(buf, sizeof(buf), "%11ld  %13ld  %14ld  %13ld  %14ld\n",
             gMemTableHit, gTableCacheHit, gTableCacheMiss, gBlockCacheHit,
             gBlockCacheMiss);
    value->append(buf);
    return true;
  } else if (in == "index") {
    Version *current = versions_->current();
    current->Ref();
    current->Print();
    current->Unref();
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range *range, int n, uint64_t *sizes) {
  // TODO(opt): better implementation
  Version *v;
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

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions &opt, const Slice &key, const Slice &value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions &opt, const Slice &key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() {}

Status DB::Open(const Options &options, const std::string &dbname, DB **dbptr) {
  *dbptr = NULL;

  DBImpl *impl = new DBImpl(options, dbname);
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == NULL) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile *lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != NULL);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {}

Status DestroyDB(const std::string &dbname, const Options &options) {
  Env *env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock *lock;
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

/***********************************************************************
 * Block Compaction Strategy
 * @author: wxl147
 * @time: 2021-05-31
 * @
 ***********************************************************************/

void DBImpl::BGFlush(void *db) {
  reinterpret_cast<DBImpl *>(db)->DoFlush();
  pthread_detach(pthread_self());
}

void DBImpl::DoFlush() {
  bool is_stop = false;
  while (!is_stop) {
    if (has_imm_.NoBarrier_Load() != nullptr) {
      mutex_.Lock();
      if (imm_ != nullptr) {
        // CompactMemTable();
        FlushWork();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
    }
    if (flush_atomic_.NoBarrier_Load() == nullptr) {
      is_stop = true;
    } else {
      usleep(100);
    }
  }
  flush_mtx_.Lock();
  bg_flush_scheduled_ = false;
  flush_cv_.SignalAll();
  flush_mtx_.Unlock();
}

void DBImpl::FlushWork() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version *base = versions_->current();
  base->Ref();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator *iter = imm_->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  uint64_t flush_time = 0;
  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta,
                   flush_buffer_, flush_time);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit.AddFile(level, meta.number, meta.file_size, meta.data_size,
                 meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.read_bytes = 0;
  stats.write_bytes = meta.file_size;
  stats_[level].Add(stats);
  base->Unref();

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    // DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

bool DBImpl::HandleTargetKey(CompactionState *compact, Slice &target_key,
                             bool &has_current_user_key,
                             std::string &current_user_key,
                             SequenceNumber &last_sequence_for_key) {
  ParsedInternalKey ikey;
  // Handle key/value, add to state, etc.
  bool drop = false;
  if (!ParseInternalKey(target_key, &ikey)) {
    // Do not hide error keys
    current_user_key.clear();
    has_current_user_key = false;
    last_sequence_for_key = kMaxSequenceNumber;
  } else {
    if (!has_current_user_key ||
        user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) !=
            0) {
      // First occurrence of this user key
      current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
      has_current_user_key = true;
      last_sequence_for_key = kMaxSequenceNumber;
    }

    if (last_sequence_for_key <= compact->smallest_snapshot) {
      // Hidden by an newer entry for same user key
      drop = true;
    } else if (ikey.type == kTypeDeletion &&
               ikey.sequence <= compact->smallest_snapshot &&
               compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
      // For this user key:
      // (1) there is no data in higher levels
      // (2) data in lower levels will have larger sequence numbers
      // (3) data in layers that are being compacted here and have
      //     smaller sequence numbers will be dropped in the next few
      //     iterations of this loop (by rule (A) above).
      // Therefore this deletion marker is obsolete and can be dropped.
      drop = true;
    }
    last_sequence_for_key = ikey.sequence;
  }
  return drop;
}

TableEditor *DBImpl::NewTableEditorForCreate(int buff_id, bool is_direct) {
  uint64_t file_number;
  uint64_t file_offset;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    file_offset = 0;
    pending_outputs_.insert(file_number);
    mutex_.Unlock();
  }
  return new TableEditor(options_, dbname_, file_number, file_offset,
                         write_caches_[buff_id], is_direct);
}

Status DBImpl::FinishTableCompaction(CompactionState *compact,
                                     TableEditor *&editor) {
  assert(editor != nullptr);
  Table *tab = editor->Finish(true);
  table_cache_->Insert(tab);
  {
    mutex_.Lock();
    CompactionState::Output out;
    out.number = editor->get_file_number();
    out.smallest.DecodeFrom(editor->get_smallest());
    out.largest.DecodeFrom(editor->get_largest());
    out.file_size = editor->FileSize();
    out.data_size = editor->FileSize();
    out.is_update = false;
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }
  //
  const uint64_t output_number = editor->get_file_number();
  const uint64_t output_entries = editor->NumEntries();
  const uint64_t output_write = editor->FileSize();
  const uint64_t output_fsize = editor->FileSize();

  delete editor;
  editor = nullptr;

  Status s;
  if (output_entries > 0) {
    // Verify that the table is usable.
    Iterator *iter =
        table_cache_->NewIterator(ReadOptions(), output_number, output_fsize);
    s = iter->status();
    delete iter;
    Log(options_.info_log, "Create table #%llu@%d: %lld keys, %lld bytes",
        (unsigned long long)output_number, compact->compaction->level(),
        (unsigned long long)output_entries, (unsigned long long)output_write);
  }
  return s;
}

Status DBImpl::DoSimpleCompactionWork(CompactionState *compact) {
  const uint64_t start_micros = env_->NowMicros();
  uint64_t imm_micros = 0;

  int level = compact->compaction->level();
  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), level + 0,
      compact->compaction->num_input_files(1), level + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->outfile == nullptr);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Iterator *input = nullptr;
  if (options_.direct_io) {
    input = versions_->MakeInputIteratorInMemory(compact->compaction);
  } else {
    input = versions_->MakeInputIterator(compact->compaction);
  }
  input->SeekToFirst();

  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  //
  TableEditor *editor = nullptr;
  for (; input->Valid();) {
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();

    // if (compact->compaction->ShouldStopBefore(key) && editor != nullptr) {
    //   status = FinishHybridCompaction_Create(compact, editor, job_stats);
    //   if (!status.ok()) {
    //     break;
    //   }
    // }

    bool drop = HandleTargetKey(compact, key, has_current_user_key,
                                current_user_key, last_sequence_for_key);

    if (!drop) {
      // Open output file if necessary
      if (editor == nullptr) {
        editor = NewTableEditorForCreate(0, options_.direct_io);
      }
      if (editor->NumEntries() == 0) {
        editor->set_smallest(key);
      }

      editor->set_largest(key);
      editor->AddNData(key, input->value());

      // Close output file if it is big enough
      if (editor->FileSize() >= compact->compaction->MaxOutputTableSize()) {
        status = FinishTableCompaction(compact, editor);
      }
    }

    input->Next();
  }

  if (editor != nullptr) {
    status = FinishTableCompaction(compact, editor);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.read_bytes += compact->compaction->input(which, i)->file_size;
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.write_bytes += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

Status DBImpl::DoHybridCompactionWork(CompactionState *compact) {
  mutex_.AssertHeld();
  // snapshot
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  flush_atomic_.NoBarrier_Store(this);
  bg_flush_scheduled_ = true;
  env_->StartThread(&DBImpl::BGFlush, this);

  hybrid->DoWork(compact);

  mutex_.Unlock();

  flush_atomic_.NoBarrier_Store(nullptr);
  flush_mtx_.Lock();
  while (bg_flush_scheduled_) {
    flush_cv_.Wait();
  }
  flush_mtx_.Unlock();

  mutex_.Lock();

  hybrid->Finish();

  return Status::OK();
}

}  // namespace leveldb

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <map>
#include <queue>
#include <set>

#include "db/compaction_state.h"
#include "db/compaction_stats.h"
#include "db/dbformat.h"
#include "db/hybrid_compaction.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class FileMetaData;
class Table;
class Block;

class TableEditor;
class ThreadPool;

class DBImpl : public DB {
 public:
  DBImpl(const Options &options, const std::string &dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions &, const Slice &key,
                     const Slice &value);
  virtual Status Delete(const WriteOptions &, const Slice &key);
  virtual Status Write(const WriteOptions &options, WriteBatch *updates);
  virtual Status Get(const ReadOptions &options, const Slice &key,
                     std::string *value);
  virtual Iterator *NewIterator(const ReadOptions &);
  virtual const Snapshot *GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot *snapshot);
  virtual bool GetProperty(const Slice &property, std::string *value);
  virtual void GetApproximateSizes(const Range *range, int n, uint64_t *sizes);
  virtual void CompactRange(const Slice *begin, const Slice *end);

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice *begin, const Slice *end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator *TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;

  struct Writer;

  Iterator *NewInternalIterator(const ReadOptions &,
                                SequenceNumber *latest_snapshot,
                                uint32_t *seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit *edit, bool *save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status *s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool *save_manifest,
                        VersionEdit *edit, SequenceNumber *max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable *mem, VersionEdit *edit, Version *base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch *BuildBatchGroup(Writer **last_writer);

  void RecordBackgroundError(const Status &s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void *db);
  void BackgroundCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState *compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState *compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Native Compaction
  Status OpenCompactionOutputFile(CompactionState *compact);
  Status FinishCompactionOutputFile(CompactionState *compact, Iterator *input);
  Status InstallCompactionResults(CompactionState *compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  bool HandleTargetKey(CompactionState *compact, Slice &target_key,
                       bool &has_current_user_key,
                       std::string &current_user_key,
                       SequenceNumber &last_sequence_for_key);

  // Simple Compaction
  TableEditor *NewTableEditorForCreate(int buff_id, bool is_direct);
  Status FinishTableCompaction(CompactionState *compact, TableEditor *&editor);

  uint64_t cnt_delete = 0;
  uint64_t lazy_delete = 4;
  uint64_t all_delete = 0;

  char *flush_buffer_;
  bool bg_flush_scheduled_;
  port::AtomicPointer flush_atomic_;
  port::Mutex flush_mtx_;
  port::CondVar flush_cv_;
  void DoFlush();
  static void BGFlush(void *db);

  ThreadPool *thread_pool_;
  uint64_t num_workers_;

  char **lower_tables_;
  char **upper_tables_;
  char **write_caches_;
  std::queue<int> CacheID_queue_;

  HybridCompaction *hybrid;

  void FlushWork();

  // Simple Compaction Work
  Status DoSimpleCompactionWork(CompactionState *compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Hybrid Compaction Work
  Status DoHybridCompactionWork(CompactionState *compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Manual Compaction Work
  Status DoManualCompactionWork(CompactionState *compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /////////////////////////////////////////////////////////////////////////////////////

  // Constant after construction
  Env *const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;
  bool owns_info_log_;
  bool owns_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache *table_cache_;
  std::map<uint64_t, uint64_t> deleted_files_;

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock *db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_;  // Signalled when background work finishes
  MemTable *mem_;
  MemTable *imm_;                // Memtable being compacted
  port::AtomicPointer has_imm_;  // So bg thread can detect non-NULL imm_
  WritableFile *logfile_;
  uint64_t logfile_number_;
  log::Writer *log_;
  uint32_t seed_;  // For sampling.

  // Queue of writers.
  std::deque<Writer *> writers_;
  WriteBatch *tmp_batch_;

  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_;

  // Has a background compaction been scheduled or is running?
  bool bg_compaction_scheduled_;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey *begin;  // NULL means beginning of key range
    const InternalKey *end;    // NULL means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };
  ManualCompaction *manual_compaction_;

  VersionSet *versions_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  CompactionStats stats_[config::kNumLevels];

  // No copying allowed
  DBImpl(const DBImpl &);
  void operator=(const DBImpl &);

  const Comparator *user_comparator() const {
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string &db,
                               const InternalKeyComparator *icmp,
                               const InternalFilterPolicy *ipolicy,
                               const Options &src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_

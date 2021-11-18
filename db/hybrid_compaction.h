
#ifndef HYBRID_COMPACTION_H
#define HYBRID_COMPACTION_H

#include "db/compaction_state.h"
#include "db/compaction_stats.h"
#include "db/db_stats.h"
#include "db/job_stats.h"
#include "db/table_cache.h"
#include "db/thread_pool.h"
#include "db/version_set.h"
#include "port/port.h"

namespace leveldb {

struct TableContext {
  int input;  // valid if input >= 0
  FileMetaData* fmd;
  Cache::Handle* handle;
  Table* table;

  Block* idx_block;
  Iterator* idx_iter;
  Block* kvs_block;
  Iterator* kvs_iter;

  TableContext()
      : input(-1),
        fmd(nullptr),
        handle(nullptr),
        table(nullptr),
        idx_block(nullptr),
        idx_iter(nullptr),
        kvs_block(nullptr),
        kvs_iter(nullptr) {}

  void Reset() {
    input = -1;
    fmd = nullptr;
    handle = nullptr;
    table = nullptr;
    idx_block = nullptr;
    idx_iter = nullptr;
    kvs_block = nullptr;
    kvs_iter = nullptr;
  }
};

class HybridCompaction {
 public:
  explicit HybridCompaction(Env* env, const Options ops,
                            const std::string dbname, ThreadPool* thread_pool,
                            TableCache* table_cache,
                            const InternalKeyComparator internal_comparator,
                            std::set<uint64_t>* pending_outputs,
                            VersionSet* version_set, std::queue<int>&,
                            char** upper_tables, char** write_caches,
                            port::Mutex* mtx, CompactionStats* stat);
  void DoWork(CompactionState* compact);
  void Finish();
  static JobStats SubJob(void* hc, int input, int buff);
  JobStats DoSubJob(int input, int buff);

 private:
  void ReadUpperTables(Compaction* c, JobStats& job_stats);
  void FreeUpperTables(Compaction* c);

  void SkipTable(TableContext* ctx, FileMetaData* fmd);

  typedef std::pair<uint64_t, uint64_t> BlockIndex;
  bool PrepareWork(FileMetaData* fmd, std::vector<BlockIndex>& result);

  bool AccessNextDataBlock_T0(TableContext* ctx_0);
  bool AccessNextDataBlock_T1(Table* table, char* file, Iterator*& t1_idx_iter,
                              Block*& t1_kvs_block, Iterator*& t1_kvs_iter);

  void AppendLefts_T0_Block(TableEditor* editor, TableContext* ctx_0);
  void AppendLefts_T1_Block(TableEditor* editor, Iterator*& t1_idx_iter);

  void SeekTable(Slice& start_key, TableContext*& ctx_0);

  bool HandleTargetKey(Slice& target_key, bool& has_current_user_key,
                       std::string& current_user_key,
                       SequenceNumber& last_sequence_for_key);
  Status InstallCompactionResults();

  // ++++++++++++++ block compaction ++++++++++++++
  void UpdateOneDataBlock(TableEditor* editor, TableContext* ctx_0,
                          Iterator*& t1_kvs_iter);
  TableEditor* NewTableEditorForUpdate(FileMetaData* fmd_1, int cache_id);
  void DoBlockCompaction(int input_id, int cache_id, TableContext* ctx_0,
                         FileMetaData* fmd_1, std::vector<BlockIndex>& result,
                         JobStats& job_stats);
  Status FinishBlockCompaction(uint64_t valid_data_size, FileMetaData* fmd,
                               TableEditor*& editor, JobStats& job_stats);

  // ++++++++++++++ table compaction ++++++++++++++
  TableEditor* NewTableEditorForCreate(int cache_id);
  void DoTableCompaction(int input_id, int cache_id, TableContext* ctx_0,
                         FileMetaData* fmd_1, JobStats& job_stats);
  Status FinishTableCompaction(TableEditor*& editor, JobStats& job_stats);

 private:
  Env* env_;
  const InternalKeyComparator internal_comparator_;
  const Options ops_;
  CompactionState* compact_;
  VersionSet* version_set_;
  TableCache* table_cache_;
  std::set<uint64_t>* pending_outputs_;
  char** upper_tables_;
  char** write_caches_;
  const std::string dbname_;
  ThreadPool* thread_pool_;
  uint64_t max_sstable_size;

  //
  port::Mutex* mutex_;
  port::Mutex compact_mtx_;

  //
  CompactionStats* stats_;

  //
  std::queue<int> CacheID_queue_;
  port::Mutex CacheID_mtx_;
  port::CondVar CacheID_cv_;

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

 private:
  // No copying allowed
  HybridCompaction(const HybridCompaction&);
  void operator=(const HybridCompaction&);
};

}  // namespace leveldb

#endif
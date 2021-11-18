
#include "hybrid_compaction.h"

#include <assert.h>

#include <future>

#include "leveldb/cache.h"

namespace leveldb {

HybridCompaction::HybridCompaction(
    Env *env, const Options ops, const std::string dbname,
    ThreadPool *thread_pool, TableCache *table_cache,
    const InternalKeyComparator internal_comparator,
    std::set<uint64_t> *pending_outputs, VersionSet *version_set,
    std::queue<int> &CacheID_queue, char **upper_tables, char **write_caches,
    port::Mutex *mtx, CompactionStats *stats)
    : env_(env),
      ops_(ops),
      dbname_(dbname),
      thread_pool_(thread_pool),
      table_cache_(table_cache),
      internal_comparator_(internal_comparator),
      version_set_(version_set),
      pending_outputs_(pending_outputs),
      upper_tables_(upper_tables),
      write_caches_(write_caches),
      CacheID_queue_(CacheID_queue),
      CacheID_cv_(&CacheID_mtx_),
      mutex_(mtx),
      stats_(stats) {
  compact_ = nullptr;
  max_sstable_size = ops.max_file_size;
}

void HybridCompaction::DoWork(CompactionState *compact) {
  mutex_->AssertHeld();
  uint64_t start_micros = env_->NowMicros();
  compact_ = compact;
  int level = compact_->compaction->level();
  assert(level > 0);
  Log(ops_.info_log, "Compacting %d@%d + %d@%d files",
      compact_->compaction->num_input_files(0), level + 0,
      compact_->compaction->num_input_files(1), level + 1);

  mutex_->Unlock();

  Compaction *c = compact_->compaction;
  Status status;
  int input_0_size = c->num_input_files(0);
  int input_1_size = c->num_input_files(1);

  JobStats job_stats;
  if (ops_.direct_io) {
    ReadUpperTables(c, job_stats);
  } else {
    for (int input_0 = 0; input_0 < input_0_size; input_0++) {
      FileMetaData *fmd = c->input(0, input_0);
      job_stats.read_bytes += fmd->data_size;
    }
  }

  std::vector<std::future<JobStats>> actual_jobs;

  for (int input_id = 0; input_id < input_1_size; input_id++) {
    CacheID_mtx_.Lock();
    while (CacheID_queue_.empty()) {
      CacheID_cv_.Wait();
    }
    int cache_id = CacheID_queue_.front();
    CacheID_queue_.pop();
    CacheID_mtx_.Unlock();
    actual_jobs.emplace_back(
        thread_pool_->enqueue(SubJob, (void *)this, input_id, cache_id));
  }

  for (auto &n : actual_jobs) {
    job_stats.add(n.get());
  }

  if (ops_.direct_io) {
    FreeUpperTables(c);
  }

  mutex_->Lock();

  CompactionStats stats;

  stats.read_bytes = job_stats.read_bytes;
  stats.write_bytes = job_stats.write_bytes;
  stats.micros = (env_->NowMicros() - start_micros);
  // table compaction
  stats.num_table_compactions = job_stats.num_table_compactions;
  stats.table_read_bytes = job_stats.table_read_bytes;
  stats.table_write_bytes = job_stats.table_write_bytes;
  stats.table_micros = job_stats.table_micros;
  stats.table_finish_micros = job_stats.table_finish_micros;
  // block compaciton
  stats.num_block_compactions = job_stats.num_block_compactions;
  stats.block_read_bytes = job_stats.block_read_bytes;
  stats.block_write_bytes = job_stats.block_write_bytes;
  stats.block_micros = job_stats.block_micros;
  stats.block_finish_micros = job_stats.block_finish_micros;
  //
  stats_[compact_->compaction->level() + 1].Add(stats);
}

void HybridCompaction::Finish() {
  mutex_->AssertHeld();
  //
  InstallCompactionResults();
  //
  VersionSet::LevelSummaryStorage tmp;
  Log(ops_.info_log, "compacted to: %s", version_set_->LevelSummary(&tmp));
}

JobStats HybridCompaction::SubJob(void *hc, int input_id, int cache_id) {
  return reinterpret_cast<HybridCompaction *>(hc)->DoSubJob(input_id, cache_id);
}

JobStats HybridCompaction::DoSubJob(int input_id, int cache_id) {
  JobStats job_stats;
  Compaction *c = compact_->compaction;
  const Comparator *comparator = ops_.comparator;

  int input_0_size = c->num_input_files(0);
  int input_1_size = c->num_input_files(1);

  FileMetaData *fmd_1 = c->input(1, input_id);

  Slice start_key;
  if (input_id > 0) {
    start_key = c->input(1, input_id - 1)->largest.Encode();
  }

  TableContext *ctx_0 = new TableContext();

  // We initialize Table Context of SSTables in top level. We Find the
  // position that the key is not smaller than start_key. There are two cases:
  // start_key is empty or not.
  SeekTable(start_key, ctx_0);

  // There is a speical case. If there are no any key value pairs in top
  // levels are covered by the target SSTable (input_id). We reuse this
  // SSTable file by skipping. Note, the target SSTable must not be the last
  // one.
  Slice t1_largest = fmd_1->largest.Encode();
  if (input_id != (input_1_size - 1) &&
      comparator->Compare(t1_largest, ctx_0->kvs_iter->key()) < 0) {
    SkipTable(ctx_0, fmd_1);
    CacheID_mtx_.Lock();
    CacheID_queue_.push(cache_id);
    CacheID_mtx_.Unlock();
    CacheID_cv_.SignalAll();
    return job_stats;
  }

  std::vector<BlockIndex> target_blocks;
  // This function can determine LSM-tree to perform block compaction or table
  // compatction. If we perform block compaction, we use a vector to store the
  // target blocks.
  bool do_block_compaction = PrepareWork(fmd_1, target_blocks);

  if (do_block_compaction) {
    job_stats.num_block_compactions++;
    uint64_t start_micros = env_->NowMicros();
    DoBlockCompaction(input_id, cache_id, ctx_0, fmd_1, target_blocks,
                      job_stats);
    job_stats.block_micros += (env_->NowMicros() - start_micros);
  } else {
    job_stats.num_table_compactions++;
    uint64_t start_micros = env_->NowMicros();
    DoTableCompaction(input_id, cache_id, ctx_0, fmd_1, job_stats);
    job_stats.table_micros += (env_->NowMicros() - start_micros);
  }

  if (ctx_0->table) {
    table_cache_->ReleaseTAF(ctx_0->handle);
    ctx_0->handle = nullptr;
    ctx_0->table = nullptr;
    // index iterator
    delete ctx_0->idx_iter;
    ctx_0->idx_iter = nullptr;
  }

  // data block and iterator
  if (ctx_0->kvs_iter) {
    delete ctx_0->kvs_iter;
    ctx_0->kvs_iter = nullptr;
    delete ctx_0->kvs_block;
    ctx_0->kvs_block = nullptr;
  }
  delete ctx_0;

  CacheID_mtx_.Lock();
  CacheID_queue_.push(cache_id);
  CacheID_mtx_.Unlock();
  CacheID_cv_.SignalAll();

  return job_stats;
}

Block *ReadDataBlock(const char *table_file, const Slice &index_value) {
  Block *block = nullptr;
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  if (s.ok()) {
    size_t size = static_cast<size_t>(handle.size());
    BlockContents contents;
    contents.data = Slice(table_file + handle.offset(), size);
    contents.cachable = false;
    contents.heap_allocated = false;
    block = new Block(contents);
  }
  assert(s.ok());
  return block;
}

void HybridCompaction::ReadUpperTables(Compaction *c, JobStats &job_stats) {
  int input_0_size = c->num_input_files(0);
  upper_tables_ = new char *[input_0_size];
  FileMetaData *fmd = nullptr;
  Cache::Handle *handle = nullptr;
  Table *table = nullptr;
  for (int input_0 = 0; input_0 < input_0_size; input_0++) {
    fmd = c->input(0, input_0);
    handle = table_cache_->LookupTAF(fmd->number, fmd->file_size);
    table = table_cache_->ValueTAF(handle);
    upper_tables_[input_0] = table->LoadDataFile();
    table_cache_->ReleaseTAF(handle);
    job_stats.read_bytes += fmd->file_size;
  }
}

void HybridCompaction::FreeUpperTables(Compaction *c) {
  int input_0_size = c->num_input_files(0);
  for (int input_0 = 0; input_0 < input_0_size; input_0++) {
    free(upper_tables_[input_0]);
  }
  delete[] upper_tables_;
}

bool HybridCompaction::AccessNextDataBlock_T0(TableContext *ctx_0) {
  if (ctx_0->kvs_iter->Valid() == false) {
    // data block and iterator
    delete ctx_0->kvs_iter;
    ctx_0->kvs_iter = nullptr;
    delete ctx_0->kvs_block;
    ctx_0->kvs_block = nullptr;

    ctx_0->idx_iter->Next();
    if (ctx_0->idx_iter->Valid() == false) {
      // table cache
      table_cache_->ReleaseTAF(ctx_0->handle);
      ctx_0->handle = nullptr;
      ctx_0->table = nullptr;

      // index iterator
      delete ctx_0->idx_iter;
      ctx_0->idx_iter = nullptr;

      // try next t0 file
      ctx_0->input++;
      Compaction *c = compact_->compaction;
      if (ctx_0->input < c->num_input_files(0)) {
        // table cache
        ctx_0->fmd = c->input(0, ctx_0->input);
        ctx_0->handle =
            table_cache_->LookupTAF(ctx_0->fmd->number, ctx_0->fmd->file_size);
        ctx_0->table = table_cache_->ValueTAF(ctx_0->handle);

        // index iterator
        ctx_0->idx_block = ctx_0->table->GetIndexBlock();
        ctx_0->idx_iter = ctx_0->idx_block->NewIterator(ops_.comparator);
        ctx_0->idx_iter->SeekToFirst();

        // data block and iterator
        Slice index_value = ctx_0->idx_iter->value();
        if (ops_.direct_io) {
          char *file_0 = upper_tables_[ctx_0->input];
          ctx_0->kvs_block = ReadDataBlock(file_0, index_value);
        } else {
          ctx_0->kvs_block = ctx_0->table->GetDataBlock(index_value);
        }
        ctx_0->kvs_iter = ctx_0->kvs_block->NewIterator(ops_.comparator);
        ctx_0->kvs_iter->SeekToFirst();
      } else {
        return false;
      }
    } else {
      // data block and iterator
      Slice index_value = ctx_0->idx_iter->value();
      if (ops_.direct_io) {
        char *file_0 = upper_tables_[ctx_0->input];
        ctx_0->kvs_block = ReadDataBlock(file_0, index_value);
      } else {
        ctx_0->kvs_block = ctx_0->table->GetDataBlock(index_value);
      }
      ctx_0->kvs_iter = ctx_0->kvs_block->NewIterator(ops_.comparator);
      ctx_0->kvs_iter->SeekToFirst();
    }
  }
  return true;
}

bool HybridCompaction::AccessNextDataBlock_T1(Table *table_1, char *file_1,
                                              Iterator *&t1_idx_iter,
                                              Block *&t1_kvs_block,
                                              Iterator *&t1_kvs_iter) {
  if (t1_kvs_iter->Valid() == false) {
    delete t1_kvs_iter;
    t1_kvs_iter = nullptr;
    delete t1_kvs_block;
    t1_kvs_block = nullptr;

    t1_idx_iter->Next();
    if (t1_idx_iter->Valid() == false) {
      return false;
    } else {
      // read t1 block
      if (ops_.direct_io) {
        t1_kvs_block = ReadDataBlock(file_1, t1_idx_iter->value());
      } else {
        t1_kvs_block = table_1->GetDataBlock(t1_idx_iter->value());
      }
      t1_kvs_iter = t1_kvs_block->NewIterator(ops_.comparator);
      t1_kvs_iter->SeekToFirst();
    }
  }
  return true;
}

bool HybridCompaction::PrepareWork(
    FileMetaData *fmd_1, std::vector<HybridCompaction::BlockIndex> &result) {
  Compaction *c = compact_->compaction;
  const Comparator *cmp = ops_.comparator;
  int level = c->level();

  // if the data size of the SSTable is too large, we use table compaction to
  // reorganize it into multiple SSTables for avoiding large-scale compaction
  // tasks. Otherwise, LSM-Tree will have the hidden trouble of write pause.
  if (fmd_1->file_size > 8.0 * max_sstable_size) {
    gPrepareFile++;
    return false;
  }

  // if the ratio of the data size to the file size is too small (which means
  // there are too many obsolete blocks), we use table compaction to recycle
  // these blocks. Otherwise, LSM-Tree will consume much space consumption.

  if (ops_.selective_compaction) {
    if (level < ops_.last_level) {
      if (fmd_1->data_size < 0.4 * fmd_1->file_size) return false;
    } else {
      if (fmd_1->data_size < 0.8 * fmd_1->file_size) return false;
    }
  } else {
    if (fmd_1->data_size < 0.4 * fmd_1->file_size) {
      gPrepareData++;
      return false;
    }
  }

  // We find target data blocks of the target SSTable in the lower level.
  // The method is to use the index block of the target SSTable to require the
  // boundary of data blocks. Then, for each data block whether its range
  // cover key value pairs of upper-level or not determines it needs to be
  // updated or not. By this way, we can find target blocks (named dirty
  // blocks).
  int input_0_size = c->num_input_files(0);

  FileMetaData *fmd_0 = nullptr;
  Cache::Handle *handle_0 = nullptr;
  Table *table_0 = nullptr;

  Block *t0_idx_block = nullptr;
  Iterator *t0_idx_iter = nullptr;
  Block *t0_kvs_block = nullptr;
  Iterator *t0_kvs_iter = nullptr;

  Slice t1_smallest = fmd_1->smallest.Encode();
  Slice t1_largest = fmd_1->largest.Encode();
  int input_id = 0;
  for (; input_id < input_0_size; input_id++) {
    fmd_0 = c->input(0, input_id);
    if (cmp->Compare(fmd_0->smallest.Encode(), t1_largest) < 0 &&
        cmp->Compare(fmd_0->largest.Encode(), t1_smallest) > 0) {
      break;
    }
  }
  assert(input_id < input_0_size);

  // table cache
  handle_0 = table_cache_->LookupTAF(fmd_0->number, fmd_0->file_size);
  table_0 = table_cache_->ValueTAF(handle_0);

  // index block
  t0_idx_block = table_0->GetIndexBlock();
  t0_idx_iter = t0_idx_block->NewIterator(ops_.comparator);
  t0_idx_iter->Seek(t1_smallest);
  assert(t0_idx_iter->Valid());
  // t0_idx_iter->SeekToFirst();
  // while (t0_idx_iter->Valid()) {
  //   if (ops_.comparator->Compare(t0_idx_iter->key(), t1_smallest) > 0) {
  //     break;
  //   }
  //   t0_idx_iter->Next();
  // }

  // data block
  if (ops_.direct_io) {
    char *file_0 = upper_tables_[input_id];
    t0_kvs_block = ReadDataBlock(file_0, t0_idx_iter->value());
  } else {
    t0_kvs_block = table_0->GetDataBlock(t0_idx_iter->value());
  }
  t0_kvs_iter = t0_kvs_block->NewIterator(ops_.comparator);
  t0_kvs_iter->Seek(t1_smallest);
  assert(t0_kvs_iter->Valid());
  // t0_kvs_iter->SeekToFirst();
  // while (t0_kvs_iter->Valid()) {
  //   if (ops_.comparator->Compare(t0_kvs_iter->key(), t1_smallest) > 0) {
  //     break;
  //   }
  //   t0_kvs_iter->Next();
  // }

  TableContext *ctx_0 = new TableContext();
  ctx_0->input = input_id;
  ctx_0->fmd = fmd_0;
  ctx_0->handle = handle_0;
  ctx_0->table = table_0;
  ctx_0->idx_block = t0_idx_block;
  ctx_0->idx_iter = t0_idx_iter;
  ctx_0->kvs_block = t0_kvs_block;
  ctx_0->kvs_iter = t0_kvs_iter;

  Cache::Handle *handle_1 = nullptr;
  Table *table_1 = nullptr;

  Block *t1_idx_block = nullptr;
  Iterator *t1_idx_iter = nullptr;

  // table cache
  handle_1 = table_cache_->LookupTAF(fmd_1->number, fmd_1->file_size);
  table_1 = table_cache_->ValueTAF(handle_1);

  // index block
  t1_idx_block = table_1->GetIndexBlock();
  t1_idx_iter = t1_idx_block->NewIterator(ops_.comparator);
  t1_idx_iter->SeekToFirst();

  // The sum of the sizes of all dirty blocks.
  uint64_t chosen = 0;
  while (t1_idx_iter->Valid()) {
    BlockHandle handle;
    Slice ivalue = t1_idx_iter->value();
    Status s = handle.DecodeFrom(&ivalue);
    Slice last_key = t1_idx_iter->key();
    std::string first = ComposeFirstKey(last_key, ivalue);
    Slice first_key(first);
    assert(s.ok());

    while (ops_.comparator->Compare(ctx_0->kvs_iter->key(), first_key) < 0) {
      ctx_0->kvs_iter->Next();
      if (AccessNextDataBlock_T0(ctx_0) == false) break;
    }

    if (ctx_0->kvs_iter == nullptr) break;

    if (ops_.comparator->Compare(ctx_0->kvs_iter->key(), last_key) < 0) {
      result.push_back(std::make_pair(handle.offset(), handle.size()));
      chosen += handle.size();
    }

    while (ops_.comparator->Compare(ctx_0->kvs_iter->key(), last_key) < 0) {
      ctx_0->kvs_iter->Next();
      if (AccessNextDataBlock_T0(ctx_0) == false) break;
    }

    if (ctx_0->kvs_iter == nullptr) break;

    t1_idx_iter->Next();
  }

  table_cache_->ReleaseTAF(handle_1);
  table_1 = nullptr;
  delete t1_idx_iter;
  t1_idx_iter = nullptr;

  if (ctx_0->table != nullptr) {
    table_cache_->ReleaseTAF(ctx_0->handle);
    ctx_0->handle = nullptr;
    ctx_0->table = nullptr;
    // index iterator
    delete ctx_0->idx_iter;
    ctx_0->idx_iter = nullptr;
  }

  // data block and iterator
  if (ctx_0->kvs_iter) {
    delete ctx_0->kvs_iter;
    ctx_0->kvs_iter = nullptr;
    delete ctx_0->kvs_block;
    ctx_0->kvs_block = nullptr;
  }

  delete ctx_0;
  ctx_0 = nullptr;

  if (ops_.compaction == CompactionType::kTableCompaction) {
    return false;
  }

  if ((fmd_1->data_size * 0.6) < chosen) {
    gPrepareChosen++;
    return false;
  }

  // In current implementation, we continue to perform table compaction in the
  // level_0. Because sstables of this levels are overlapped with each other.
  // Moreover, if there are two many dirty blocks, block compaction do not
  // work well and even incur large space consumption.
  if (ops_.selective_compaction && level >= ops_.last_level &&
      (fmd_1->data_size * ops_.overlap_ratio) < chosen) {
    return false;
  }

  return true;
}

void HybridCompaction::AppendLefts_T0_Block(TableEditor *editor,
                                            TableContext *ctx_0) {
  while (ctx_0->kvs_iter->Valid()) {
    editor->set_largest(ctx_0->kvs_iter->key());
    editor->AddNData(ctx_0->kvs_iter->key(), ctx_0->kvs_iter->value());
    ctx_0->kvs_iter->Next();
    if (AccessNextDataBlock_T0(ctx_0) == false) break;
  }
}

void HybridCompaction::AppendLefts_T1_Block(TableEditor *editor,
                                            Iterator *&t1_idx_iter) {
  while (t1_idx_iter->Valid()) {
    editor->set_largest(t1_idx_iter->key());
    editor->AddIndex(t1_idx_iter->key(), t1_idx_iter->value());
    t1_idx_iter->Next();
  }
}

void HybridCompaction::SeekTable(Slice &start_key, TableContext *&ctx_0) {
  // Member variables of table context
  FileMetaData *fmd_0 = nullptr;
  Cache::Handle *handle_0 = nullptr;
  Table *table_0 = nullptr;

  Block *t0_idx_block = nullptr;
  Iterator *t0_idx_iter = nullptr;
  Block *t0_kvs_block = nullptr;
  Iterator *t0_kvs_iter = nullptr;

  Compaction *c = compact_->compaction;
  int input_id = -1;
  int input_0_size = c->num_input_files(0);
  if (start_key.empty()) {
    // table context
    input_id = 0;
    fmd_0 = c->input(0, 0);
    handle_0 = table_cache_->LookupTAF(fmd_0->number, fmd_0->file_size);
    table_0 = table_cache_->ValueTAF(handle_0);

    // index block
    t0_idx_block = table_0->GetIndexBlock();
    t0_idx_iter = t0_idx_block->NewIterator(ops_.comparator);
    t0_idx_iter->SeekToFirst();

    // data block
    if (ops_.direct_io) {
      char *file_0 = upper_tables_[input_id];
      t0_kvs_block = ReadDataBlock(file_0, t0_idx_iter->value());
    } else {
      t0_kvs_block = table_0->GetDataBlock(t0_idx_iter->value());
    }
    t0_kvs_iter = t0_kvs_block->NewIterator(ops_.comparator);
    t0_kvs_iter->SeekToFirst();
  } else {
    // table context
    for (input_id = 0; input_id < input_0_size; input_id++) {
      fmd_0 = c->input(0, input_id);
      if (ops_.comparator->Compare(start_key, fmd_0->largest.Encode()) < 0) {
        break;
      }
    }
    assert(input_id < input_0_size);
    handle_0 = table_cache_->LookupTAF(fmd_0->number, fmd_0->file_size);
    table_0 = table_cache_->ValueTAF(handle_0);

    // index block
    t0_idx_block = table_0->GetIndexBlock();
    t0_idx_iter = t0_idx_block->NewIterator(ops_.comparator);
    t0_idx_iter->SeekToFirst();

    if (ops_.comparator->Compare(start_key, fmd_0->smallest.Encode()) < 0) {
      // first
      if (ops_.direct_io) {
        char *file_0 = upper_tables_[input_id];
        t0_kvs_block = ReadDataBlock(file_0, t0_idx_iter->value());
      } else {
        t0_kvs_block = table_0->GetDataBlock(t0_idx_iter->value());
      }
      t0_kvs_iter = t0_kvs_block->NewIterator(ops_.comparator);
      t0_kvs_iter->SeekToFirst();
    } else {
      while (t0_idx_iter->Valid()) {
        if (ops_.comparator->Compare(t0_idx_iter->key(), start_key) > 0) {
          break;
        }
        t0_idx_iter->Next();
      }
      // data block
      if (ops_.direct_io) {
        char *file_0 = upper_tables_[input_id];
        t0_kvs_block = ReadDataBlock(file_0, t0_idx_iter->value());
      } else {
        t0_kvs_block = table_0->GetDataBlock(t0_idx_iter->value());
      }
      t0_kvs_iter = t0_kvs_block->NewIterator(ops_.comparator);
      t0_kvs_iter->SeekToFirst();
      while (t0_kvs_iter->Valid()) {
        if (ops_.comparator->Compare(t0_kvs_iter->key(), start_key) > 0) {
          break;
        }
        t0_kvs_iter->Next();
      }
    }
  }  // end if

  ctx_0->input = input_id;
  ctx_0->fmd = fmd_0;
  ctx_0->handle = handle_0;
  ctx_0->table = table_0;
  ctx_0->idx_block = t0_idx_block;
  ctx_0->idx_iter = t0_idx_iter;
  ctx_0->kvs_block = t0_kvs_block;
  ctx_0->kvs_iter = t0_kvs_iter;
}

void HybridCompaction::SkipTable(TableContext *ctx_0, FileMetaData *fmd_1) {
  compact_mtx_.Lock();
  compact_->reused_tables.insert(fmd_1->number);
  compact_mtx_.Unlock();

  table_cache_->ReleaseTAF(ctx_0->handle);
  ctx_0->table = nullptr;
  // index iterator
  delete ctx_0->idx_iter;
  ctx_0->idx_iter = nullptr;

  // block data and iterator
  delete ctx_0->kvs_iter;
  ctx_0->kvs_iter = nullptr;
  delete ctx_0->kvs_block;
  ctx_0->kvs_block = nullptr;
  delete ctx_0;
}

bool HybridCompaction::HandleTargetKey(Slice &target_key,
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

    if (last_sequence_for_key <= compact_->smallest_snapshot) {
      // Hidden by an newer entry for same user key
      drop = true;
    } else if (ikey.type == kTypeDeletion &&
               ikey.sequence <= compact_->smallest_snapshot &&
               compact_->compaction->IsBaseLevelForKey(ikey.user_key)) {
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

void HybridCompaction::UpdateOneDataBlock(TableEditor *editor,
                                          TableContext *ctx_0,
                                          Iterator *&t1_kvs_iter) {
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  Iterator *input = nullptr;
  while (ctx_0->kvs_iter->Valid() && t1_kvs_iter->Valid()) {
    if (internal_comparator_.Compare(ctx_0->kvs_iter->key(),
                                     t1_kvs_iter->key()) < 0) {
      input = ctx_0->kvs_iter;
    } else {
      input = t1_kvs_iter;
    }
    Slice key = input->key();
    bool drop = HandleTargetKey(key, has_current_user_key, current_user_key,
                                last_sequence_for_key);
    if (!drop) {
      editor->set_largest(key);
      editor->AddNData(key, input->value());
    }
    input->Next();
    if (AccessNextDataBlock_T0(ctx_0) == false) break;
  }
  // the reset key value pairs of the target data block.
  while (t1_kvs_iter->Valid()) {
    editor->set_largest(t1_kvs_iter->key());
    editor->AddNData(t1_kvs_iter->key(), t1_kvs_iter->value());
    t1_kvs_iter->Next();
  }
  editor->NextBlock();
}

TableEditor *HybridCompaction::NewTableEditorForUpdate(FileMetaData *fmd_1,
                                                       int cache_id) {
  uint64_t file_number = 0;
  uint64_t file_offset = 0;
  {
    mutex_->Lock();
    file_number = fmd_1->number;
    file_offset = fmd_1->file_size;
    pending_outputs_->insert(file_number);
    mutex_->Unlock();
  }
  bool is_direct = ops_.direct_io;
  return new TableEditor(ops_, dbname_, file_number, file_offset,
                         write_caches_[cache_id], is_direct);
}

void HybridCompaction::DoBlockCompaction(
    int input_id, int cache_id, TableContext *ctx_0, FileMetaData *fmd_1,
    std::vector<HybridCompaction::BlockIndex> &victim, JobStats &job_stats) {
  /**
   * Block Compaction
   * Mechanism: Merge Blocks + Append new blocks.
   */

  Compaction *c = compact_->compaction;
  const Comparator *comparator = ops_.comparator;

  TableEditor *editor = NewTableEditorForUpdate(fmd_1, cache_id);
  {
    compact_mtx_.Lock();
    compact_->reused_tables.insert(fmd_1->number);
    compact_mtx_.Unlock();
  }

  Cache::Handle *handle_1 = nullptr;
  Table *table_1 = nullptr;

  Block *t1_idx_block = nullptr;
  Iterator *t1_idx_iter = nullptr;

  // table cache
  handle_1 = table_cache_->LookupTAF(fmd_1->number, fmd_1->file_size);
  table_1 = table_cache_->ValueTAF(handle_1);

  // index block
  t1_idx_block = table_1->GetIndexBlock();
  t1_idx_iter = t1_idx_block->NewIterator(ops_.comparator);
  t1_idx_iter->SeekToFirst();

  // Compute the data size of the target SSTable. Note that we have to minus
  // the size of footer (occupies a single aligned block size) and the size
  // of index block.
  uint64_t valid_data_size = fmd_1->data_size;
  valid_data_size -= kSectorSize;
  valid_data_size -= t1_idx_block->size();

  uint64_t start_load = env_->NowMicros();

  // Since we have required the positions of all dirty blocks, we can load
  // these data blocks by parallel I/Os or Async IO.
  char *file_0 = nullptr;
  if (ops_.direct_io) {
    file_0 = table_1->LoadVictim3(victim);
  }

  Slice t0_smallest = ctx_0->kvs_iter->key();
  Slice t1_smallest = fmd_1->smallest.Encode();
  if (comparator->Compare(t0_smallest, t1_smallest) < 0) {
    editor->set_smallest(t0_smallest);
  } else {
    editor->set_smallest(t1_smallest);
  }

  while (ctx_0->idx_iter != nullptr && t1_idx_iter->Valid()) {
    while (true) {
      Slice t0_key = ctx_0->kvs_iter->key();
      while (t1_idx_iter->Valid() &&
             comparator->Compare(t1_idx_iter->key(), t0_key) < 0) {
        editor->set_largest(t1_idx_iter->key());
        editor->AddIndex(t1_idx_iter->key(), t1_idx_iter->value());
        t1_idx_iter->Next();
      }

      if (t1_idx_iter->Valid() != true) break;

      BlockHandle handle;
      Slice ivalue = t1_idx_iter->value();
      Status s = handle.DecodeFrom(&ivalue);
      assert(s.ok());
      Slice last_key = t1_idx_iter->key();
      std::string first_str = ComposeFirstKey(last_key, ivalue);
      Slice first_key(first_str);

      assert(t0_key.compare(first_key) != 0);

      while (ctx_0->kvs_iter->Valid() &&
             comparator->Compare(ctx_0->kvs_iter->key(), first_key) < 0) {
        editor->set_largest(ctx_0->kvs_iter->key());
        editor->AddNData(ctx_0->kvs_iter->key(), ctx_0->kvs_iter->value());
        ctx_0->kvs_iter->Next();
        if (AccessNextDataBlock_T0(ctx_0) == false) break;
      }

      editor->NextBlock();

      if (ctx_0->idx_iter == nullptr) break;

      if ((ops_.comparator->Compare(ctx_0->kvs_iter->key(), first_key) > 0) &&
          (ops_.comparator->Compare(ctx_0->kvs_iter->key(), last_key) < 0)) {
        break;
      }
    }
    // Attention - upper table or lower table finish !!!
    if (t1_idx_iter->Valid() == false || ctx_0->idx_iter == nullptr) {
      break;
    }

    // Read the target data block of the target SSTable. Note, this data block
    // has been already loaded into memory.
    Block *t1_kvs_block = nullptr;
    Iterator *t1_kvs_iter = nullptr;
    if (ops_.direct_io) {
      t1_kvs_block = ReadDataBlock(file_0, t1_idx_iter->value());
    } else {
      t1_kvs_block = table_1->GetDataBlock(t1_idx_iter->value());
    }
    t1_kvs_iter = t1_kvs_block->NewIterator(ops_.comparator);
    t1_kvs_iter->SeekToFirst();

    //
    job_stats.read_bytes += t1_kvs_block->size();
    job_stats.block_read_bytes += t1_kvs_block->size();

    UpdateOneDataBlock(editor, ctx_0, t1_kvs_iter);

    // Minus the obsolete data block.
    valid_data_size -= t1_kvs_block->size();

    // data block and iterator
    delete t1_kvs_iter;
    t1_kvs_iter = nullptr;
    delete t1_kvs_block;
    t1_kvs_block = nullptr;

    //
    t1_idx_iter->Next();
  }
  // the rest of table_1
  if (t1_idx_iter->Valid()) {
    AppendLefts_T1_Block(editor, t1_idx_iter);
  }
  // the rest of table_0
  if (input_id == c->num_input_files(1) - 1 && ctx_0->kvs_iter != nullptr) {
    AppendLefts_T0_Block(editor, ctx_0);
  }
  //
  if (editor != nullptr) {
    Status s = FinishBlockCompaction(valid_data_size, fmd_1, editor, job_stats);
    assert(s.ok());
  }
  //
  if (ops_.direct_io) {
    free(file_0);
  }
  table_cache_->ReleaseTAF(handle_1);
  // index iterator
  delete t1_idx_iter;
  t1_idx_iter = nullptr;
}

Status HybridCompaction::FinishBlockCompaction(uint64_t valid_data_size,
                                               FileMetaData *fmd_1,
                                               TableEditor *&editor,
                                               JobStats &job_stats) {
  assert(editor != nullptr);
  uint64_t start_micros = env_->NowMicros();
  Table *tab = editor->Finish(true);
  table_cache_->Insert(tab);
  {
    mutex_->Lock();
    CompactionState::Output out;
    out.number = editor->get_file_number();
    out.smallest.DecodeFrom(editor->get_smallest());
    out.largest.DecodeFrom(editor->get_largest());
    out.is_update = true;
    out.init_size = fmd_1->file_size;
    out.file_size = editor->FileSize();
    out.data_size = valid_data_size + (editor->FileSize() - fmd_1->file_size);
    compact_->outputs.push_back(out);
    mutex_->Unlock();
  }
  //
  const uint64_t output_number = editor->get_file_number();
  const uint64_t output_entries = editor->NumEntries();
  const uint64_t output_write = (editor->FileSize() - fmd_1->file_size);
  const uint64_t output_filesz = editor->FileSize();
  const uint64_t output_datasz = valid_data_size + output_write;

  job_stats.write_bytes += output_write;
  job_stats.block_write_bytes += output_write;
  job_stats.block_finish_micros += (env_->NowMicros() - start_micros);

  delete editor;
  editor = nullptr;

  Status s;
  if (output_entries > 0) {
    // Verify that the table is usable.
    Iterator *iter =
        table_cache_->NewIterator(ReadOptions(), output_number, output_filesz);
    s = iter->status();
    delete iter;
    Log(ops_.info_log,
        "Update table #%llu@%d: %lld keys, %lld bytes, %lld bytes",
        (unsigned long long)output_number, compact_->compaction->level(),
        (unsigned long long)output_entries, (unsigned long long)output_filesz,
        (unsigned long long)output_datasz);
  }
  assert(s.ok());
  return s;
}

TableEditor *HybridCompaction::NewTableEditorForCreate(int cache_id) {
  uint64_t file_number = 0;
  uint64_t file_offset = 0;
  {
    mutex_->Lock();
    file_number = version_set_->NewFileNumber();
    file_offset = 0;
    pending_outputs_->insert(file_number);
    mutex_->Unlock();
  }
  bool is_direct = ops_.direct_io;
  return new TableEditor(ops_, dbname_, file_number, file_offset,
                         write_caches_[cache_id], is_direct);
}

void HybridCompaction::DoTableCompaction(int input_id, int cache_id,
                                         TableContext *ctx_0,
                                         FileMetaData *fmd_1,
                                         JobStats &job_stats) {
  /**
   * Table Compaction
   * Mechansim: Merge tables + Create new tables
   */
  if (ops_.direct_io) {
    job_stats.read_bytes += fmd_1->file_size;
    job_stats.table_read_bytes += fmd_1->file_size;
  } else {
    job_stats.read_bytes += fmd_1->data_size;
    job_stats.table_read_bytes += fmd_1->data_size;
  }

  Compaction *c = compact_->compaction;
  const Comparator *comparator = ops_.comparator;

  // the member variable of Table Context
  Cache::Handle *handle_1 = nullptr;
  Table *table_1 = nullptr;
  Block *t1_idx_block = nullptr;
  Iterator *t1_idx_iter = nullptr;
  Block *t1_kvs_block = nullptr;
  Iterator *t1_kvs_iter = nullptr;

  // table cache
  handle_1 = table_cache_->LookupTAF(fmd_1->number, fmd_1->file_size);
  table_1 = table_cache_->ValueTAF(handle_1);

  // index block
  t1_idx_block = table_1->GetIndexBlock();
  t1_idx_iter = t1_idx_block->NewIterator(ops_.comparator);
  t1_idx_iter->SeekToFirst();

  char *file_1 = nullptr;
  if (ops_.direct_io) {
    file_1 = table_1->LoadDataFile();
  }

  // Load the whole SSTable file by the system call to avoids random I/Os.
  if (ops_.direct_io) {
    t1_kvs_block = ReadDataBlock(file_1, t1_idx_iter->value());
  } else {
    t1_kvs_block = table_1->GetDataBlock(t1_idx_iter->value());
  }
  t1_kvs_iter = t1_kvs_block->NewIterator(ops_.comparator);
  t1_kvs_iter->SeekToFirst();
  // -----------------------------

  TableEditor *editor = NewTableEditorForCreate(cache_id);

  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  Status s;
  Iterator *input = nullptr;
  while (s.ok() && ctx_0->kvs_iter->Valid() && t1_kvs_iter->Valid()) {
    if (comparator->Compare(ctx_0->kvs_iter->key(), t1_kvs_iter->key()) < 0) {
      input = ctx_0->kvs_iter;
    } else {
      input = t1_kvs_iter;
    }

    Slice key = input->key();
    bool drop = HandleTargetKey(key, has_current_user_key, current_user_key,
                                last_sequence_for_key);
    if (!drop) {
      if (editor == nullptr) {
        editor = NewTableEditorForCreate(cache_id);
      }
      if (editor->NumEntries() == 0) {
        editor->set_smallest(key);
      }
      editor->set_largest(key);
      editor->AddNData(key, input->value());
      // Close output file if it is big enough
      if (editor->FileSize() >= c->MaxOutputTableSize()) {
        Status s = FinishTableCompaction(editor, job_stats);
        assert(s.ok());
      }
    }

    input->Next();

    if (AccessNextDataBlock_T0(ctx_0) == false) {
      break;
    }
    if (AccessNextDataBlock_T1(table_1, file_1, t1_idx_iter, t1_kvs_block,
                               t1_kvs_iter) == false) {
      break;
    }
  }
  // the rest of upper SSTable file
  if (input_id == c->num_input_files(1) - 1 && ctx_0->kvs_iter != nullptr) {
    while (ctx_0->kvs_iter->Valid()) {
      if (editor == nullptr) {
        editor = NewTableEditorForCreate(cache_id);
      }
      if (editor->NumEntries() == 0) {
        editor->set_smallest(ctx_0->kvs_iter->key());
      }
      editor->set_largest(ctx_0->kvs_iter->key());
      editor->AddNData(ctx_0->kvs_iter->key(), ctx_0->kvs_iter->value());
      // Close output file if it is big enough
      if (editor->FileSize() >= c->MaxOutputTableSize()) {
        Status s = FinishTableCompaction(editor, job_stats);
        assert(s.ok());
      }
      ctx_0->kvs_iter->Next();
      if (AccessNextDataBlock_T0(ctx_0) == false) {
        break;
      }
    }  // while
  }
  // the rest of lower SSTable file
  if (t1_idx_iter->Valid()) {
    while (t1_kvs_iter->Valid()) {
      if (editor == nullptr) {
        editor = NewTableEditorForCreate(cache_id);
      }
      if (editor->NumEntries() == 0) {
        editor->set_smallest(t1_kvs_iter->key());
      }
      editor->set_largest(t1_kvs_iter->key());
      editor->AddNData(t1_kvs_iter->key(), t1_kvs_iter->value());
      // Close output file if it is big enough
      if (editor->FileSize() >= c->MaxOutputTableSize()) {
        Status s = FinishTableCompaction(editor, job_stats);
        assert(s.ok());
      }
      t1_kvs_iter->Next();
      if (AccessNextDataBlock_T1(table_1, file_1, t1_idx_iter, t1_kvs_block,
                                 t1_kvs_iter) == false) {
        break;
      }
    }  // while
  }
  //
  if (editor != nullptr) {
    Status s = FinishTableCompaction(editor, job_stats);
    assert(s.ok());
  }
  //
  if (ops_.direct_io) {
    free(file_1);
  }
  table_cache_->ReleaseTAF(handle_1);
  // index iterator
  delete t1_idx_iter;
  t1_idx_iter = nullptr;
}

Status HybridCompaction::FinishTableCompaction(TableEditor *&editor,
                                               JobStats &job_stats) {
  assert(editor != nullptr);
  uint64_t start_micros = env_->NowMicros();
  Table *tab = editor->Finish(true);
  table_cache_->Insert(tab);
  {
    mutex_->Lock();
    CompactionState::Output out;
    out.number = editor->get_file_number();
    out.smallest.DecodeFrom(editor->get_smallest());
    out.largest.DecodeFrom(editor->get_largest());
    out.is_update = false;
    out.init_size = 0;
    out.file_size = editor->FileSize();
    out.data_size = editor->FileSize();
    compact_->outputs.push_back(out);
    mutex_->Unlock();
  }
  //
  const uint64_t output_number = editor->get_file_number();
  const uint64_t output_entries = editor->NumEntries();
  const uint64_t output_write = editor->FileSize();
  const uint64_t output_fsize = editor->FileSize();

  job_stats.write_bytes += output_write;
  job_stats.table_write_bytes += output_write;
  job_stats.table_finish_micros += (env_->NowMicros() - start_micros);

  delete editor;
  editor = nullptr;

  Status s;
  if (output_entries > 0) {
    // Verify that the table is usable.
    Iterator *iter =
        table_cache_->NewIterator(ReadOptions(), output_number, output_fsize);
    s = iter->status();
    delete iter;
    Log(ops_.info_log, "Create table #%llu@%d: %lld keys, %lld bytes",
        (unsigned long long)output_number, compact_->compaction->level(),
        (unsigned long long)output_entries, (unsigned long long)output_write);
  }
  assert(s.ok());
  return s;
}

Status HybridCompaction::InstallCompactionResults() {
  mutex_->AssertHeld();
  Log(ops_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact_->compaction->num_input_files(0), compact_->compaction->level(),
      compact_->compaction->num_input_files(1),
      compact_->compaction->level() + 1,
      static_cast<long long>(compact_->total_bytes));

  const int level = compact_->compaction->level();
  VersionEdit *edit = compact_->compaction->edit();

  // Deleted Files
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < compact_->compaction->num_input_files(which); i++) {
      FileMetaData *meta = compact_->compaction->input(which, i);
      if (compact_->reused_tables.count(meta->number) < 1) {
        edit->DeleteFile(level + which, meta->number);
        table_cache_->Evict(meta->number, meta->file_size);
      }
    }
  }

  // Created Files & Updated Files
  for (int i = 0; i < compact_->outputs.size(); i++) {
    const CompactionState::Output &out = compact_->outputs[i];
    if (out.is_update) {
      edit->UpdateFile(level + 1, out.number, out.file_size, out.data_size,
                       out.smallest, out.largest);
      table_cache_->Evict(out.number, out.init_size);
    } else {
      edit->AddNewFile(level + 1, out.number, out.file_size, out.data_size,
                       out.smallest, out.largest);
    }
  }

  return version_set_->LogAndApply(compact_->compaction->edit(), mutex_);
}

}  // namespace leveldb

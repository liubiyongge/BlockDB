// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include <fcntl.h>
#include <libaio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <future>
#include <iostream>

#include "db/db_stats.h"
#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/filter_table.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

Status Table::Open(const Options &options, RandomAccessFile *file,
                   uint64_t number, uint64_t size, Table **table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char *footer_space = nullptr;
  Slice input;
  uint64_t off = 0;
  uint64_t len = 0;
  if (options.direct_io) {
    int ret = posix_memalign((void **)&footer_space, kSectorSize, kSectorSize);
    if (ret != 0) {
      printf("Failed to posix_memalign: %s", strerror(errno));
      exit(0);
    }
    len = kSectorSize;
    off = size - len;
  } else {
    footer_space = new char[Footer::kEncodedLength];
    len = Footer::kEncodedLength;
    off = size - len;
  }
  Status s = file->Read(off, len, &input, footer_space);
  if (!s.ok()) {
    fprintf(stdout, "%s\n", s.ToString().c_str());
    exit(0);
  }

  Slice footer_input(footer_space, Footer::kEncodedLength);
  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // Read the index block
  BlockContents contents;
  Block *index_block = NULL;
  if (s.ok()) {
    ReadOptions opt;
    if (options.paranoid_checks) {
      opt.verify_checksums = true;
    }
    bool direct_io = options.direct_io;
    s = ReadBlock(direct_io, file, opt, footer.index_handle(), &contents);
    if (s.ok()) {
      index_block = new Block(contents);
    }
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Rep *rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter_size = 0;
    rep->filter = nullptr;
    rep->file_number = number;
    rep->file_size = size;
    rep->in_memory = false;
    rep->table_file = nullptr;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  } else {
    delete index_block;
  }

  free(footer_space);
  return s;
}

void Table::ReadMeta(const Footer &footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->options.direct_io, rep_->file, opt,
                 footer.metaindex_handle(), &contents)
           .ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block *meta = new Block(contents);

  Iterator *iter = meta->NewIterator(BytewiseComparator());
  std::string key = "tablefilter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice &filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  bool direct_io = rep_->options.direct_io;
  if (!ReadBlock(direct_io, rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
    rep_->filter_size = block.data.size();
  }

  rep_->filter = new FilterTableReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void *arg, void *ignored) {
  delete reinterpret_cast<Block *>(arg);
}

static void DeleteCachedBlock(const Slice &key, void *value) {
  Block *block = reinterpret_cast<Block *>(value);
  delete block;
}

static void ReleaseBlock(void *arg, void *h) {
  Cache *cache = reinterpret_cast<Cache *>(arg);
  Cache::Handle *handle = reinterpret_cast<Cache::Handle *>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator *Table::BlockReaderForQuery(void *arg, const ReadOptions &options,
                                     const Slice &index_value) {
  Table *table = reinterpret_cast<Table *>(arg);
  Cache *block_cache = table->rep_->options.block_cache;
  Block *block = NULL;
  Cache::Handle *cache_handle = NULL;
  bool direct_io = table->rep_->options.direct_io;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->file_number);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        // block cache hit
        g_block_cache_hit++;

        block = reinterpret_cast<Block *>(block_cache->Value(cache_handle));
      } else {
        // block cache miss
        g_block_cache_miss++;

        s = ReadBlock(direct_io, table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            // block cache add bytes
            g_block_cache_add_bytes += block->size();

            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(direct_io, table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator *iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator *Table::BlockReader(void *arg, const ReadOptions &options,
                             const Slice &index_value) {
  Table *table = reinterpret_cast<Table *>(arg);
  Cache *block_cache = table->rep_->options.block_cache;
  Block *block = NULL;
  Cache::Handle *cache_handle = NULL;
  bool direct_io = table->rep_->options.direct_io;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->file_number);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        block = reinterpret_cast<Block *>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(direct_io, table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(direct_io, table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator *iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator *Table::NewIterator(const ReadOptions &options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReaderForQuery, const_cast<Table *>(this), options);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator *Table::BlockReaderInMemory(void *arg, const ReadOptions &options,
                                     const Slice &index_value) {
  Table *table = reinterpret_cast<Table *>(arg);
  assert(table->rep_->table_file != nullptr);

  Cache *block_cache = table->rep_->options.block_cache;
  Block *block = NULL;
  Cache::Handle *cache_handle = NULL;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->file_number);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        block = reinterpret_cast<Block *>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlockInMemory(table->rep_->table_file, options, handle,
                              &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlockInMemory(table->rep_->table_file, options, handle,
                            &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator *iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator *Table::NewIteratorInMemory(const ReadOptions &options) {
  rep_->table_file = LoadDataFile();
  assert(rep_->table_file != nullptr);
  rep_->in_memory = true;
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReaderInMemory, const_cast<Table *>(this), options);
}

Status Table::InternalGet(const ReadOptions &options, const Slice &k, void *arg,
                          void (*saver)(void *, const Slice &, const Slice &)) {
  Status s;
  if (rep_->filter) {
    // Check filter first.
    FilterTableReader *filter = rep_->filter;
    if (!filter->KeyMayMatch(k)) {
      // Not found
      return s;
    }
  }

  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    BlockHandle handle;
    s = handle.DecodeFrom(&handle_value);

    if (rep_->options.compaction == CompactionType::kBlockCompaction) {
      // first key
      Slice last_key = iiter->key();
      std::string first = ComposeFirstKey(last_key, handle_value);
      Slice f(first);

      Slice user_key = ExtractUserKey(k);
      Slice first_user_key = ExtractUserKey(f);

      if (user_key.compare(first_user_key) < 0) {
        delete iiter;
        return s;
      }
    }

    // For query
    Iterator *block_iter = BlockReaderForQuery(this, options, iiter->value());
    block_iter->Seek(k);
    if (block_iter->Valid()) {
      (*saver)(arg, block_iter->key(), block_iter->value());
    }
    s = block_iter->status();
    delete block_iter;
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice &key) const {
  Iterator *index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

Block *Table::GetIndexBlock() {
  Block *block = rep_->index_block;
  return block;
}

Block *Table::GetDataBlock(const Slice &index_value) {
  ReadOptions options;
  options.verify_checksums = false;
  options.fill_cache = false;
  bool direct_io = rep_->options.direct_io;

  Block *block = nullptr;
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  BlockContents contents;
  if (s.ok()) {
    s = ReadBlock(direct_io, rep_->file, options, handle, &contents);
  }
  if (s.ok()) {
    block = new Block(contents);
  }
  return block;
}

char *Table::LoadDataFile() {
  char *data_file = nullptr;
  int ret = posix_memalign((void **)&data_file, kSectorSize, rep_->file_size);
  if (ret != 0) {
    fprintf(stdout, "Failed to posix_memalign!\n");
    exit(0);
  }
  Slice result;
  Status s = rep_->file->Read(0, rep_->file_size, &result, data_file);
  if (!s.ok()) {
    printf("Failed to load table file!\n");
  }
  return data_file;
}

void Table::FreeDataFile() {
  if (rep_ && rep_->in_memory) {
    free(rep_->table_file);
    rep_->table_file = nullptr;
  }
}

char *Table::LoadVictim1(std::vector<std::pair<uint64_t, uint64_t>> vtm) {
  char *data_file = nullptr;
  int ret = posix_memalign((void **)&data_file, kSectorSize, rep_->file_size);
  if (ret != 0) {
    fprintf(stdout, "Failed to posix_memalign!\n");
    exit(0);
  }
  for (int i = 0; i < vtm.size(); i++) {
    uint64_t off1 = vtm[i].first;
    uint64_t len1 = vtm[i].second;
    uint64_t off2 = 0;
    uint64_t len2 = 0;
    //
    AlignPM(off1, len1, off2, len2);
    Slice result;
    Status s = rep_->file->Read(off2, len2, &result, data_file + off2);
    if (!s.ok()) {
      printf("Failed to read table file!\n");
      exit(0);
    }
  }
  return data_file;
}

char *Table::LoadVictim2(std::vector<std::pair<uint64_t, uint64_t>> idx) {
  char *data_file = nullptr;
  int ret = posix_memalign((void **)&data_file, kSectorSize, rep_->file_size);
  if (ret != 0) {
    fprintf(stdout, "Failed to posix_memalign!\n");
    exit(0);
  }

  io_context_t ctx;
  memset(&ctx, 0, sizeof(ctx));

  const int max_events = 100;
  int errcode = io_setup(max_events, &ctx);
  if (errcode != 0) {
    printf("io_setup: %s\n", strerror(-errcode));
    exit(0);
  }

  struct iocb *iocbpp[max_events];
  for (int j = 0; j < max_events; j++) {
    iocbpp[j] = (struct iocb *)malloc(sizeof(struct iocb));
  }

  int fd = rep_->file->FileDesc();

  int loops = idx.size() / max_events + 1;
  for (int i = 0; i < loops; i++) {
    int left = idx.size() - i * max_events;
    int num_events = std::min(max_events, left);
    // for
    for (int j = 0; j < num_events; j++) {
      int k = i * max_events + j;
      uint64_t offset = idx[k].first;
      uint64_t length = idx[k].second;

      uint64_t pading = (kSectorSize - (length % kSectorSize)) % kSectorSize;
      char *des = data_file + offset;
      io_prep_pread(iocbpp[j], fd, des, length + pading, offset);
    }
    int n = io_submit(ctx, num_events, iocbpp);
    if (n < 0) {
      printf("failed to io_submit!\n");
      exit(0);
    }
    struct io_event events[num_events];
    n = io_getevents(ctx, num_events, num_events, events, nullptr);
    if (n != num_events) {
      printf("failed to io_getevents!\n");
      exit(0);
    }
  }

  io_destroy(ctx);
  return data_file;
}

char *Table::LoadVictim3(std::vector<std::pair<uint64_t, uint64_t>> vtm) {
  char *data_file = nullptr;
  int ret = posix_memalign((void **)&data_file, kSectorSize, rep_->file_size);
  if (ret != 0) {
    fprintf(stdout, "Failed to posix_memalign!\n");
    exit(0);
  }
  std::vector<std::future<int>> parts;

  int num_workers = 8;
  uint64_t tasks = std::ceil(1.0 * vtm.size() / num_workers);

  for (size_t i = 0; i < num_workers; i++) {
    std::future<int> res = std::async(
        [](RandomAccessFile *file, int worker, int tasks, char *des,
           std::vector<std::pair<uint64_t, uint64_t>> idx) {
          int start = worker * tasks;
          int remain = idx.size() - start;
          int limit = std::min(remain, tasks);
          // for
          for (int j = 0; j < limit; j++) {
            uint64_t off1 = idx[start + j].first;
            uint64_t len1 = idx[start + j].second;
            uint64_t off2 = 0;
            uint64_t len2 = 0;
            AlignPM(off1, len1, off2, len2);
            Slice result;
            Status s = file->Read(off2, len2, &result, des + off2);
            if (!s.ok()) {
              printf("Failed to read table file!\n");
            }
          }
          return 1;
        },
        rep_->file, i, tasks, data_file, vtm);
    // future
    parts.emplace_back(std::move(res));
  }
  int size = 0;
  for (auto &n : parts) {
    size += n.get();
  }
  assert(size == num_workers);
  return data_file;
}

}  // namespace leveldb

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/db_stats.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

static void DeleteEntry(const Slice &key, void *value) {
  TableAndFile *tf = reinterpret_cast<TableAndFile *>(value);

  // Remove table index size.
  g_memory_table_index_size -= tf->table->rep_->index_block->size();

  // Remove filter size.
  g_memory_filter_size -= tf->table->rep_->filter_size;

  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void *arg1, void *arg2) {
  Cache *cache = reinterpret_cast<Cache *>(arg1);
  Cache::Handle *h = reinterpret_cast<Cache::Handle *>(arg2);
  cache->Release(h);
}

static void UnrefEntryInMemory(void *arg1, void *arg2) {
  Cache *cache = reinterpret_cast<Cache *>(arg1);
  Cache::Handle *h = reinterpret_cast<Cache::Handle *>(arg2);
  Table *table = reinterpret_cast<TableAndFile *>(cache->Value(h))->table;
  table->FreeDataFile();
  cache->Release(h);
}

TableCache::TableCache(const std::string &dbname, const Options *options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

Status TableCache::FindTableForQuery(uint64_t file_number, uint64_t file_size,
                                     Cache::Handle **handle) {
  Status s;
  char buf1[sizeof(file_number)];
  EncodeFixed64(buf1, file_number);
  char buf2[sizeof(file_size)];
  EncodeFixed64(buf2, file_size);
  std::string t;
  t.append(buf1, sizeof(file_number));
  t.append(buf2, sizeof(file_size));
  Slice key(t);
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    // table cache miss
    g_table_cache_miss++;
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile *file = nullptr;
    Table *table = nullptr;
    if (options_->direct_io) {
      s = env_->NewRandomAccessFileWithDirect(fname, &file);
    } else {
      s = env_->NewRandomAccessFile(fname, &file);
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_number, file_size, &table);
    }

    if (!s.ok()) {
      printf("%s: Failed to open table!\n", __FUNCTION__);
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile *tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      tf->file_number = file_number;
      tf->file_size = file_size;

      // Record table index size.
      g_memory_table_index_size += table->rep_->index_block->size();

      // Record filter size.
      g_memory_filter_size += table->rep_->filter_size;

      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  } else {
    // table cache hit
    g_table_cache_hit++;
  }
  return s;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle **handle) {
  Status s;
  char buf1[sizeof(file_number)];
  EncodeFixed64(buf1, file_number);
  char buf2[sizeof(file_size)];
  EncodeFixed64(buf2, file_size);
  std::string t;
  t.append(buf1, sizeof(file_number));
  t.append(buf2, sizeof(file_size));
  Slice key(t);
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile *file = nullptr;
    Table *table = nullptr;
    if (options_->direct_io) {
      s = env_->NewRandomAccessFileWithDirect(fname, &file);
    } else {
      s = env_->NewRandomAccessFile(fname, &file);
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_number, file_size, &table);
    }

    if (!s.ok()) {
      printf("%s: Failed to open table!\n", __FUNCTION__);
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile *tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      tf->file_number = file_number;
      tf->file_size = file_size;

      // Record table index size.
      g_memory_table_index_size += table->rep_->index_block->size();

      // Record filter size.
      g_memory_filter_size += table->rep_->filter_size;

      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator *TableCache::NewIterator(const ReadOptions &options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table **tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }
  Cache::Handle *handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }
  Table *table = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
  Iterator *result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Iterator *TableCache::NewIteratorInMemory(const ReadOptions &options,
                                          uint64_t file_number,
                                          uint64_t file_size,
                                          Table **tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }
  Cache::Handle *handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }
  Table *table = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
  Iterator *result = table->NewIteratorInMemory(options);
  result->RegisterCleanup(&UnrefEntryInMemory, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(int level, const ReadOptions &options,
                       uint64_t file_number, uint64_t file_size, const Slice &k,
                       void *arg,
                       void (*saver)(void *, const Slice &, const Slice &)) {
  Cache::Handle *handle = NULL;
  Status s = FindTableForQuery(file_number, file_size, &handle);
  if (s.ok()) {
    Table *t = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Insert(Table *table) {
  //
  uint64_t file_size = table->rep_->file_size;
  uint64_t file_number = table->rep_->file_number;

  char buf1[sizeof(file_number)];
  EncodeFixed64(buf1, file_number);
  char buf2[sizeof(file_size)];
  EncodeFixed64(buf2, file_size);
  std::string t;
  t.append(buf1, sizeof(file_number));
  t.append(buf2, sizeof(file_size));
  Slice key(t);

  Cache::Handle *handle = nullptr;
  RandomAccessFile *file = table->rep_->file;
  TableAndFile *tf = new TableAndFile;
  tf->file = file;
  tf->table = table;
  tf->file_number = file_number;
  tf->file_size = file_size;

  // Record table index size.
  g_memory_table_index_size += table->rep_->index_block->size();

  // Record filter size.
  g_memory_filter_size += table->rep_->filter_size;

  handle = cache_->Insert(key, tf, 1, &DeleteEntry);
  cache_->Release(handle);
}

void TableCache::Evict(uint64_t file_number, uint64_t file_size) {
  char buf1[sizeof(file_number)];
  EncodeFixed64(buf1, file_number);
  char buf2[sizeof(file_size)];
  EncodeFixed64(buf2, file_size);
  std::string t;
  t.append(buf1, sizeof(file_number));
  t.append(buf2, sizeof(file_size));
  Slice key(t);

  cache_->Erase(key);
}

Cache::Handle *TableCache::LookupTAF(uint64_t file_number, uint64_t file_size) {
  Cache::Handle *handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  assert(s.ok());
  return handle;
}

Table *TableCache::ValueTAF(Cache::Handle *handle) {
  assert(handle != nullptr);
  return reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
}

void TableCache::ReleaseTAF(Cache::Handle *handle) {
  assert(handle != nullptr);
  cache_->Release(handle);
}

}  // namespace leveldb

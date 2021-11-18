// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <stdint.h>

#include <vector>

#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class Table {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // If there was an error while initializing the table, sets "*table"
  // to NULL and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options &options, RandomAccessFile *file,
                     uint64_t file_number, uint64_t file_size, Table **table);

  ~Table();

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator *NewIterator(const ReadOptions &) const;
  Iterator *NewIteratorInMemory(const ReadOptions &);

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice &key) const;

 public:
  struct Rep {
    ~Rep() {
      delete filter;
      delete[] filter_data;
      delete index_block;
      if (table_file != nullptr) {
        free(table_file);
      }
    }

    Options options;
    Status status;
    RandomAccessFile *file;
    uint64_t cache_id;
    FilterBlockReader *filter;
    const char *filter_data;

    BlockHandle
        metaindex_handle;  // Handle to metaindex_block: saved from footer
    Block *index_block;

    uint64_t file_number;
    uint64_t file_size;

    bool in_memory = false;
    char *table_file;
  };

  Rep *rep_ = nullptr;

  explicit Table(Rep *rep) { rep_ = rep; }

 private:
  static Iterator *BlockReader(void *, const ReadOptions &, const Slice &);

  static Iterator *BlockReaderForQuery(void *, const ReadOptions &,
                                       const Slice &);

  static Iterator *BlockReaderInMemory(void *, const ReadOptions &,
                                       const Slice &);

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  friend class TableCache;
  Status InternalGet(const ReadOptions &, const Slice &key, void *arg,
                     void (*handle_result)(void *arg, const Slice &k,
                                           const Slice &v));

  void ReadMeta(const Footer &footer);
  void ReadFilter(const Slice &filter_handle_value);

  // No copying allowed
  Table(const Table &);
  void operator=(const Table &);

 public:
  Block *GetIndexBlock();
  Block *GetDataBlock(const Slice &);

  char *LoadDataFile();
  void FreeDataFile();

  char *LoadVictim1(std::vector<std::pair<uint64_t, uint64_t>> idx);
  char *LoadVictim2(std::vector<std::pair<uint64_t, uint64_t>> idx);
  char *LoadVictim3(std::vector<std::pair<uint64_t, uint64_t>> idx);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_

/*
 * table_add.h
 *
 *  Created on: Jul 2, 2019
 *      Author: wxl147
 */

#ifndef INCLUDE_LEVELDB_TABLE_REBUILDER_H_
#define INCLUDE_LEVELDB_TABLE_REBUILDER_H_

#include <stdint.h>

#include <string>
#include <vector>

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"

namespace leveldb {

class BlockBuilder;
class BlockHandle;
class Table;
class Block;

class TableEditor {
 public:
  // Created a editor that store new updated blocks.
  // 'backing_store' caches new blocks.
  TableEditor(const Options &options, const std::string dbname,
              uint64_t file_number, uint64_t file_offset, char *buffer,
              bool is_direct);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~TableEditor();

  void AddNData(const Slice &key, const Slice &value);
  void AddIndex(const Slice &key, const Slice &value);
  void NextBlock();

  Status WriteTable(WritableFile *file);

  Table *Finish(bool is_algined);

  uint64_t FileSize() const;

  uint64_t NumEntries() const;

  void Abandon();

 private:
  void Flush();
  Slice WriteIndexBlock(BlockBuilder *block, BlockHandle *handle);
  void WriteBlock(BlockBuilder *block, BlockHandle *handle);
  void WriteRawBlock(const Slice &block_contents, CompressionType type,
                     BlockHandle *handle);
  void Write2Align();
  void WriteBuffer(Slice data);

 private:
  bool is_ffresh_;
  bool is_direct_;
  const std::string dbname_;
  WritableFile *outfile_ = nullptr;

  uint64_t file_number_;
  uint64_t file_offset_;

  uint64_t kBufferSize = 16777216;  // 16 MB
  uint64_t kSectorSize = 512;
  char *buffer_;
  uint64_t offset_;

  uint64_t num_entries_;

  Options options_;
  Options index_options_;

  FilterBlockBuilder *filter_block_;

  std::string compressed_output_;

  std::string first_key_;
  std::string last_key_;
  BlockBuilder data_block_;

  bool pending_index_entry_;
  BlockHandle pending_handle_;

  BlockBuilder index_block_;

  bool closed_;

 public:
  Status status_;
  bool ok() const { return status_.ok(); }

  uint64_t get_file_number() const { return file_number_; }

 public:
  std::string smallest, largest;
  std::string get_smallest() { return smallest; }
  void set_smallest(const Slice &k) { smallest.assign(k.data(), k.size()); }
  std::string get_largest() { return largest; }
  void set_largest(const Slice &k) { largest.assign(k.data(), k.size()); }

 private:
  // No copying allowed
  TableEditor(const TableEditor &);
  void operator=(const TableEditor &);
};

}  // namespace leveldb

#endif /* INCLUDE_LEVELDB_TABLE_REBUILDER_H_ */

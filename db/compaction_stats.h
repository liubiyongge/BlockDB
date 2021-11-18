
#ifndef COMPACTION_STATS_H
#define COMPACTION_STATS_H

#include <stdint.h>

namespace leveldb {

// Per level compaction stats.  stats_[level] stores the stats for
// compactions that produced data for the specified "level".
struct CompactionStats {
  uint64_t read_bytes;
  uint64_t write_bytes;
  uint64_t micros;
  uint64_t num_compactions;

  // table compaction
  uint64_t num_table_compactions;
  uint64_t table_read_bytes;
  uint64_t table_write_bytes;
  uint64_t table_micros;
  uint64_t table_finish_micros;

  // block compaction
  uint64_t num_block_compactions;
  uint64_t block_read_bytes;
  uint64_t block_write_bytes;
  uint64_t block_micros;
  uint64_t block_finish_micros;

  CompactionStats()
      : read_bytes(0),
        write_bytes(0),
        micros(0),
        num_compactions(0),
        num_table_compactions(0),
        table_read_bytes(0),
        table_write_bytes(0),
        table_micros(0),
        table_finish_micros(0),
        num_block_compactions(0),
        block_read_bytes(0),
        block_write_bytes(0),
        block_micros(0),
        block_finish_micros(0) {}

  void Add(const CompactionStats &c) {
    this->read_bytes += c.read_bytes;
    this->write_bytes += c.write_bytes;
    this->micros += c.micros;
    this->num_compactions += c.num_compactions;
    //
    this->num_table_compactions += c.num_table_compactions;
    this->table_read_bytes += c.table_read_bytes;
    this->table_write_bytes += c.table_write_bytes;
    this->table_micros += c.table_micros;
    this->table_finish_micros += c.table_finish_micros;
    //
    this->num_block_compactions += c.num_block_compactions;
    this->block_read_bytes += c.block_read_bytes;
    this->block_write_bytes += c.block_write_bytes;
    this->block_micros += c.block_micros;
    this->block_finish_micros += c.block_finish_micros;
  }
};

}  // namespace leveldb

#endif
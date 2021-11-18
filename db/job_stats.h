
#ifndef JOB_STATS_H
#define JOB_STATS_H

#include <stdint.h>

namespace leveldb {

struct JobStats {
  uint64_t read_bytes;
  uint64_t write_bytes;

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

  JobStats()
      : read_bytes(0),
        write_bytes(0),
        num_table_compactions(0),
        table_read_bytes(0),
        table_write_bytes(0),
        table_micros(0),
        table_finish_micros(0),
        num_block_compactions(0),
        block_read_bytes(0),
        block_write_bytes(0),
        block_micros(0),
        block_finish_micros(0) {
    ;
  }

  void add(JobStats s) {
    this->read_bytes += s.read_bytes;
    this->write_bytes += s.write_bytes;
    //
    this->num_table_compactions += s.num_table_compactions;
    this->table_read_bytes += s.table_read_bytes;
    this->table_write_bytes += s.table_write_bytes;
    this->table_micros += s.table_micros;
    this->table_finish_micros += s.table_finish_micros;
    //
    this->num_block_compactions += s.num_block_compactions;
    this->block_read_bytes += s.block_read_bytes;
    this->block_write_bytes += s.block_write_bytes;
    this->block_micros += s.block_micros;
    this->block_finish_micros += s.block_finish_micros;
  }
};

}  // namespace leveldb

#endif
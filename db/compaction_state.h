
#ifndef COMPACTION_STATE_H
#define COMPACTION_STATE_H

#include <stdint.h>

#include "db/dbformat.h"
#include "db/version_set.h"
#include "leveldb/table_editor.h"

namespace leveldb {

struct CompactionState {
  Compaction *const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t init_size;
    uint64_t file_size;
    uint64_t data_size;
    InternalKey smallest, largest;
    bool is_update;
  };
  std::vector<Output> outputs;

  //
  TableBuilder *builder;

  WritableFile *outfile;

  // State kept for output being generated
  uint64_t total_bytes;
  uint64_t write_bytes;

  Output *current_output() { return &outputs[outputs.size() - 1]; }

  // Tables which are not deleted during compaction
  std::set<uint64_t> reused_tables;

  explicit CompactionState(Compaction *c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0),
        write_bytes(0),
        smallest_snapshot(0) {
    reused_tables.clear();
  }
};

}  // namespace leveldb

#endif
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      debug_log(NULL),
      write_buffer_size(4 << 20),
      max_open_files(1000),
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
      max_file_size(2 << 20),
      compression(kSnappyCompression),
      reuse_logs(false),
      filter_policy(NULL),
      overlap_ratio(0.2),
      direct_io(false),
      num_workers(1),
      compaction(kBlockCompaction),
      level0_slowdown_writes_trigger(8),
      level0_stop_writes_trigger(12),
      selective_compaction(true),
      last_level(4) {}

}  // namespace leveldb

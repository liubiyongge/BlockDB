
#ifndef BLOCK_DB_STATS_H
#define BLOCK_DB_STATS_H

#include <stdint.h>

#include <atomic>

namespace leveldb {

// memory filter size
extern std::atomic<uint64_t> g_memory_filter_size;

// memory table index size
extern std::atomic<uint64_t> g_memory_table_index_size;

// table cache
extern std::atomic<uint64_t> g_table_cache_hit;
extern std::atomic<uint64_t> g_table_cache_miss;

// block cache
extern std::atomic<uint64_t> g_block_cache_hit;
extern std::atomic<uint64_t> g_block_cache_miss;

// memtable hit
extern std::atomic<uint64_t> g_memtable_hit;

//  add bytes
extern std::atomic<uint64_t> g_block_cache_add_bytes;

// prepare work
extern std::atomic<uint64_t> g_large_sstable_file;
extern std::atomic<uint64_t> g_small_sstable_data;
extern std::atomic<uint64_t> g_large_choosen_data;

}  // namespace leveldb

#endif
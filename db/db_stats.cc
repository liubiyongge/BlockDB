
#include "db_stats.h"

namespace leveldb {

// memory filter size
std::atomic<uint64_t> g_memory_filter_size = {0};

// memory table index size
std::atomic<uint64_t> g_memory_table_index_size = {0};

// table cache
std::atomic<uint64_t> g_table_cache_hit = {0};
std::atomic<uint64_t> g_table_cache_miss = {0};

// block cache
std::atomic<uint64_t> g_block_cache_hit = {0};
std::atomic<uint64_t> g_block_cache_miss = {0};

//  add bytes
std::atomic<uint64_t> g_block_cache_add_bytes = {0};

// memtable & immutable memtable
std::atomic<uint64_t> g_memtable_hit = {0};

// prepare work
std::atomic<uint64_t> g_large_sstable_file = {0};
std::atomic<uint64_t> g_small_sstable_data = {0};
std::atomic<uint64_t> g_large_choosen_data = {0};

}  // namespace leveldb

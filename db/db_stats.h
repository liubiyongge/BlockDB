
#ifndef BLOCK_DB_STATS_H
#define BLOCK_DB_STATS_H

#include <stdint.h>

#include <atomic>

namespace leveldb {

extern uint64_t gMemTableHit;

extern uint64_t gBlockCacheMiss;

extern uint64_t gBlockCacheHit;

extern uint64_t gTableCacheMiss;

extern uint64_t gTableCacheHit;

extern uint64_t gPrepareFile;

extern uint64_t gPrepareChosen;

extern uint64_t gPrepareData;

}  // namespace leveldb

#endif
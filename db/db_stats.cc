
#include "db_stats.h"

namespace leveldb {

uint64_t gMemTableHit = 0;

uint64_t gBlockCacheMiss = 0;

uint64_t gBlockCacheHit = 0;

uint64_t gTableCacheMiss = 0;

uint64_t gTableCacheHit = 0;

uint64_t gPrepareFile = 0;

uint64_t gPrepareChosen = 0;

uint64_t gPrepareData = 0;

}  // namespace leveldb

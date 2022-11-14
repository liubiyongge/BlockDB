
#ifndef TEST_BLOCKDB_H
#define TEST_BLOCKDB_H

#include <stdint.h>

#include <iostream>
#include <string>

#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/options.h"

void TestBlockDB_RandomPut(std::vector<uint64_t> keys, uint64_t value_size);

void TestBlockDB_RandomGet(std::vector<uint64_t> keys);

#endif
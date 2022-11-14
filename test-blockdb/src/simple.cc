/*
 * example.cc
 *
 *  Created on: Jun 30, 2020
 *      Author: wxl147
 */

#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "test_blockdb.h"

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

int main(int argc, char *argv[]) {
  uint64_t database = 500ul << 20;
  uint64_t value_size = 1ul << 10;
  uint64_t num_keys = database / value_size;

  std::vector<uint64_t> keys;
  for (uint64_t i = 1; i < num_keys; i++) {
    keys.push_back(i);
  }
  std::random_shuffle(keys.begin(), keys.end());

  uint64_t start_time = NowMicros();
  TestBlockDB_RandomPut(keys, value_size);
  TestBlockDB_RandomGet(keys);
  std::cout << "Running Time (s): " << 1.0 * (NowMicros() - start_time) / 1e6
            << std::endl;

  return 0;
}

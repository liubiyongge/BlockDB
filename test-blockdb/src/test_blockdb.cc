

#include "test_blockdb.h"

#include "leveldb/filter_policy.h"

void TestBlockDB_RandomPut(std::vector<uint64_t> keys, uint64_t xxx) {
  leveldb::Options options;
  options.create_if_missing = true;
  options.compression = leveldb::kNoCompression;
  options.compaction = leveldb::kBlockCompaction;
  options.write_buffer_size = 16 << 20;
  options.max_file_size = 4 << 20;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  options.max_open_files = 10000;
  options.direct_io = true;
  options.num_workers = 1;

  leveldb::ReadOptions read_ops;
  leveldb::WriteOptions write_ops;

  leveldb::DB *db = nullptr;
  leveldb::Status s = leveldb::DB::Open(
      options, "/home/wxl/Block_Compaction/db_load/blockdb", &db);
  if (!s.ok()) {
    fprintf(stdout, "Failed to open leveldb!");
    exit(0);
  }

  char key[64];
  memset(key, 0, sizeof(key));
  int key_size = 64;

  char value[1024];
  memset(value, 0, sizeof(value));
  int value_size = 1024;

  std::cout << "Write ..." << std::endl;
  for (uint64_t i = 0; i < keys.size(); i++) {
    snprintf(key, sizeof(key), "%ld", keys[i]);
    snprintf(value, sizeof(value), "%ld", keys[i]);
    s = db->Put(write_ops, std::string(key, key_size),
                std::string(value, value_size));
    if (s.ok() != true) {
      printf("%s\n", s.ToString().c_str());
      exit(0);
    }
    if ((i + 1) % 100000 == 0) {
      std::cout << "#" << std::flush;
    }
    if ((i + 1) % 1000000 == 0) {
      std::cout << std::endl;
    }
  }

  std::string stats;
  db->GetProperty("leveldb.stats", &stats);
  std::cout << stats << std::endl;
  delete db;
}

void TestBlockDB_RandomGet(std::vector<uint64_t> keys) {
  leveldb::Options options;
  options.create_if_missing = true;
  options.compression = leveldb::kNoCompression;
  options.compaction = leveldb::kBlockCompaction;
  options.write_buffer_size = 16 << 20;
  options.max_file_size = 4 << 20;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  options.direct_io = true;

  leveldb::ReadOptions read_ops;
  leveldb::WriteOptions write_ops;

  leveldb::DB *db = nullptr;
  leveldb::Status s = leveldb::DB::Open(
      options, "/home/wxl/Block_Compaction/db_load/blockdb", &db);
  if (!s.ok()) {
    fprintf(stdout, "Failed to open leveldb!");
    exit(0);
  }

  char key[64];
  memset(key, 0, sizeof(key));
  int key_size = 64;

  std::cout << "Read ..." << std::endl;
  for (uint64_t i = 0; i < keys.size(); i++) {
    std::string value;
    snprintf(key, sizeof(key), "%ld", keys[i]);
    // std::cout << "i: " << i << std::endl;
    s = db->Get(read_ops, std::string(key, key_size), &value);
    if (s.ok() != true) {
      printf("%s\n", s.ToString().c_str());
      exit(0);
    }
    uint64_t res = 0;
    sscanf(value.c_str(), "%ld", &res);
    if (res != keys[i]) {
      printf("key: %ld: Error Value!\n", keys[i]);
      exit(0);
    }
    if ((i + 1) % 100000 == 0) {
      std::cout << "#" << std::flush;
    }
    if ((i + 1) % 1000000 == 0) {
      std::cout << std::endl;
    }
  }
  delete db;
}
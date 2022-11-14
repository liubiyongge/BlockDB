
#ifndef STORAGE_LEVELDB_TABLE_FILTER_TABLE_H_
#define STORAGE_LEVELDB_TABLE_FILTER_TABLE_H_

#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

class FilterTableBuilder {
 public:
  FilterTableBuilder(FilterPolicy* policy, int level, std::string* old_filter);

  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  FilterPolicy* policy_;
  std::string keys_;
  std::vector<uint32_t> start_;
  std::string result_;
  std::vector<Slice> tmp_keys_;

  const int level_;
  std::string* old_filter_;

  // No copying allowed
  FilterTableBuilder(const FilterTableBuilder&);
  void operator=(const FilterTableBuilder&);
};

class FilterTableReader {
 public:
  // REQUIRES: "contents" and "*policy" must  stay live.
  FilterTableReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(const Slice& key);

 private:
  const FilterPolicy* policy_;
  Slice data_;

 private:
};

}  // namespace leveldb

#endif
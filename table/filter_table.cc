

#include "table/filter_table.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

uint64_t reserved_keys[7] = {0 /* level 0 */,
                             0 /* level 1 */,
                             8000 /* level 2 */,
                             4000 /* level 3 */,
                             1600 /* level 4 */,
                             0 /* level 5 */,
                             0};

// Filter Table Builder
FilterTableBuilder::FilterTableBuilder(FilterPolicy* policy, int level,
                                       std::string* old_filter)
    : policy_(policy), level_(level), old_filter_(old_filter) {}

void FilterTableBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

Slice FilterTableBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }
  return Slice(result_);
}

void FilterTableBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();

  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());
  tmp_keys_.resize(num_keys);

  for (uint32_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter
  if (level_ == 0) {
    // Nomral
    policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);
    return;
  }

  if (old_filter_) {
    result_.assign(old_filter_->data(), old_filter_->size());
    policy_->AddKeys(&tmp_keys_[0], static_cast<int>(num_keys), &result_);
    return;
  }

  policy_->CreateFilterWithReserved(&tmp_keys_[0], static_cast<int>(num_keys),
                                    &result_, reserved_keys[level_]);
}

// Filter Table Reader
FilterTableReader::FilterTableReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(contents) {}

bool FilterTableReader::KeyMayMatch(const Slice& key) {
  return policy_->KeyMayMatch(key, data_);
}

}  // namespace leveldb
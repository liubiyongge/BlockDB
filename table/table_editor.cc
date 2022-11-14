/*
 * table_adder.cc
 *
 *  Created on: Jul 2, 2019
 *      Author: wxl147
 */
#include "leveldb/table_editor.h"

#include <stdint.h>

#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

TableEditor::TableEditor(const Options &opt, const std::string dbname,
                         uint64_t file_number, uint64_t file_offset,
                         uint64_t level, std::string *old_filter, char *buffer)
    : options_(opt),
      dbname_(dbname),
      index_options_(opt),
      pending_index_entry_(
          false),  // pending_index_entry is true only if data_block_ is empty.
      file_number_(file_number),
      file_offset_(file_offset),
      level_(level),
      old_filter_(old_filter),
      is_direct_(opt.direct_io),
      buffer_(buffer),
      offset_(0),
      num_entries_(0),
      closed_(false),
      data_block_(&options_),
      index_block_(&index_options_) {
  // bloom filter
  if (opt.filter_policy == nullptr) {
    filter_table_ = nullptr;
  } else {
    if (file_offset > 0 && old_filter_ != nullptr) {
      filter_table_ = new FilterTableBuilder(
          const_cast<FilterPolicy *>(opt.filter_policy), level_, old_filter_);
    } else {
      filter_table_ = new FilterTableBuilder(
          const_cast<FilterPolicy *>(opt.filter_policy), level_, nullptr);
    }
  }
  index_options_.block_restart_interval = 1;

  // There are two cases:
  // 1) file_offset == 0; we create new file.
  // 2) file_offset != 0; we update old file.
  if (file_offset == 0) {
    is_ffresh_ = true;
  } else {
    is_ffresh_ = false;
  }

  Status s;
  std::string fname = TableFileName(dbname_, file_number_);
  if (is_ffresh_) {
    if (is_direct_)
      s = options_.env->NewWritableFileWithDirect(fname, &outfile_);
    else
      s = options_.env->NewWritableFile(fname, &outfile_);
    assert(s.ok());
  } else {
    if (is_direct_)
      s = options_.env->NewAppendableFileWithDirect(fname, &outfile_);
    else
      s = options_.env->NewAppendableFile(fname, &outfile_);
    assert(s.ok());
  }

  // if (is_direct_) {
  //   int ret = posix_memalign((void **)&buffer_, kSectorSize, kBufferSize);
  //   if (ret != 0) {
  //     fprintf(stdout, "TableEditor: failed to posix_memalign!\n");
  //     exit(0);
  //   }
  //   memset(buffer_, 0, kBufferSize);
  //   offset_ = 0;
  // }
}

TableEditor::~TableEditor() {
  assert(closed_);  // Catch errors where caller forgot to call Finish()
  delete filter_table_;
  if (old_filter_) {
    delete old_filter_;
  }
  // if (buffer_) free(buffer_);
}

void TableEditor::AddPair(const Slice &key, const Slice &value) {
  assert(!closed_);
  if (!ok()) return;
  if (num_entries_ > 0) {
    assert(options_.comparator->Compare(key, Slice(last_key_)) > 0);
  }

  // index block
  if (pending_index_entry_) {
    assert(data_block_.empty());
    std::string handle_encoding;
    pending_handle_.EncodeTo(&handle_encoding);
    {
      // index handle format:
      // last_key + offset + first_key (diff)
      const uint32_t min_length = std::min(first_key_.size(), last_key_.size());
      uint32_t shard = 0;
      while ((shard < min_length) && (first_key_[shard] == last_key_[shard])) {
        shard++;
      }
      uint32_t non_shard = first_key_.size() - shard;
      handle_encoding.append((char *)&shard, sizeof(uint32_t));
      handle_encoding.append(first_key_.data() + shard, non_shard);
    }
    // handle_encoding.append(first_key_);
    index_block_.Add(last_key_, handle_encoding);
    pending_index_entry_ = false;
  }

  // filter block
  if (filter_table_ != nullptr) {
    filter_table_->AddKey(key);
  }

  // data block
  if (data_block_.empty()) {
    assert(pending_index_entry_ != true);
    first_key_.assign(key.data(), key.size());
  }
  last_key_.assign(key.data(), key.size());
  num_entries_++;
  data_block_.Add(key, value);

  //
  const size_t estimated_block_size = data_block_.CurrentSizeEstimate();
  if (estimated_block_size >= options_.block_size) {
    Flush();
  }
}

void TableEditor::AddIndex(const Slice &key, const Slice &value) {
  assert(!closed_);
  if (!ok()) return;
  index_block_.Add(key, value);
}

void TableEditor::NextBlock() {
  assert(!closed_);
  if (!ok()) return;
  Flush();
  if (pending_index_entry_) {
    assert(data_block_.empty());
    std::string handle_encoding;
    // options_.comparator->FindShortestSeparator(&last_key_, key);
    pending_handle_.EncodeTo(&handle_encoding);
    {
      const uint32_t min_length = std::min(first_key_.size(), last_key_.size());
      uint32_t shard = 0;
      while ((shard < min_length) && (first_key_[shard] == last_key_[shard])) {
        shard++;
      }
      uint32_t non_shard = first_key_.size() - shard;
      handle_encoding.append((char *)&shard, sizeof(uint32_t));
      handle_encoding.append(first_key_.data() + shard, non_shard);
    }
    // handle_encoding.append(first_key_);
    index_block_.Add(last_key_, Slice(handle_encoding));
    pending_index_entry_ = false;
  }
}

void TableEditor::Flush() {
  assert(!closed_);
  if (!ok()) return;
  if (data_block_.empty()) return;
  assert(!pending_index_entry_);
  //
  WriteBlock(&data_block_, &pending_handle_);
  if (ok()) {
    pending_index_entry_ = true;
  }
}

Slice TableEditor::WriteIndexBlock(BlockBuilder *index, BlockHandle *handle) {
  // File format contains a sequence of blocks where each block has:
  //   block_data: uint8_t[n]
  //   type: uint8_t
  //   crc: uint32
  assert(ok());
  Slice raw = index->Finish();

  Slice block_contents;
  CompressionType type = options_.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string *compressed = &compressed_output_;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  compressed_output_.clear();
  return block_contents;
}

void TableEditor::WriteBlock(BlockBuilder *block, BlockHandle *handle) {
  // File format contains a sequence of blocks where each block has:
  //   block_data: uint8_t[n]
  //   type: uint8_t
  //   crc: uint32
  assert(ok());
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = options_.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string *compressed = &compressed_output_;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  compressed_output_.clear();
  block->Reset();
}

void TableEditor::WriteRawBlock(const Slice &block_contents,
                                CompressionType type, BlockHandle *handle) {
  handle->set_offset(file_offset_);
  handle->set_size(block_contents.size());
  Status s;
  if (is_direct_) {
    // memcpy(buffer_ + offset_, block_contents.data(), block_contents.size());
    WriteBuffer(block_contents);
  } else {
    s = outfile_->Append(block_contents);
    assert(s.ok());
  }
  file_offset_ += block_contents.size();
  if (status_.ok()) {
    // type + crc
    char trailer[kBlockTrailerSize];  // 5 bytes
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    if (is_direct_) {
      // memcpy(buffer_ + offset_, trailer, kBlockTrailerSize);
      WriteBuffer(Slice(trailer, kBlockTrailerSize));
    } else {
      s = outfile_->Append(Slice(trailer, kBlockTrailerSize));
      assert(s.ok());
    }
    file_offset_ += kBlockTrailerSize;
  }
}

void TableEditor::Write2Align() {
  uint64_t padding = (kSectorSize - offset_ % kSectorSize) % kSectorSize;
  offset_ += padding;
  Status s = outfile_->Append(Slice(buffer_, offset_));
  assert(s.ok());

  file_offset_ += padding;
  offset_ = 0;
  assert(file_offset_ % kSectorSize == 0);
}

void TableEditor::WriteBuffer(Slice data) {
  assert(!data.empty());
  uint64_t curr = 0;
  uint64_t left = 0;
  uint64_t size = 0;
  Status s;
  while (s.ok() && curr < data.size()) {
    left = data.size() - curr;
    size = left < (kBufferSize - offset_) ? left : (kBufferSize - offset_);
    memcpy(buffer_ + offset_, data.data() + curr, size);
    curr += size;
    offset_ = offset_ + size;
    if (offset_ == kBufferSize) {
      s = outfile_->Append(Slice(buffer_, kBufferSize));
      if (s.ok() != true) {
        fprintf(stdout, "%s\n", s.ToString().c_str());
        exit(0);
      }
      offset_ = 0;
      // memset(buffer_, 0, kBufferSize);
    }
  }
}

Table *TableEditor::Finish(bool is_algined) {
  assert(!closed_);
  Flush();

  BlockHandle filter_block_handle, meta_index_block_handle, index_block_handle;

  Slice filter_contents;
  // Write bloom filter block
  if (ok() && filter_table_ != nullptr) {
    filter_contents = filter_table_->Finish();
    WriteRawBlock(filter_contents, kNoCompression, &filter_block_handle);
  }

  // Write meta index block
  if (ok()) {
    BlockBuilder meta_index_block_(&options_);
    if (filter_table_ != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "tablefilter.";
      key.append(options_.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block_.Add(key, handle_encoding);
    }
    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block_, &meta_index_block_handle);
  }

  Slice index_contents;
  // Write index block
  if (ok()) {
    if (pending_index_entry_) {
      // options_.comparator->FindShortSuccessor(&last_key_);
      std::string handle_encoding;
      pending_handle_.EncodeTo(&handle_encoding);
      {
        const uint32_t min_length =
            std::min(first_key_.size(), last_key_.size());
        uint32_t shard = 0;
        while ((shard < min_length) &&
               (first_key_[shard] == last_key_[shard])) {
          shard++;
        }
        uint32_t non_shard = first_key_.size() - shard;
        handle_encoding.append((char *)&shard, sizeof(uint32_t));
        handle_encoding.append(first_key_.data() + shard, non_shard);
      }
      // handle_encoding.append(first_key_);
      index_block_.Add(last_key_, Slice(handle_encoding));
      pending_index_entry_ = false;
    }
    index_contents = WriteIndexBlock(&index_block_, &index_block_handle);
  }

  if (is_direct_) {
    Write2Align();
  }

  // Write footer
  std::string footer_encoding;
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(meta_index_block_handle);
    footer.set_index_handle(index_block_handle);
    footer.EncodeTo(&footer_encoding);
    file_offset_ += footer_encoding.size();
  }

  Status s;
  if (is_direct_) {
    WriteBuffer(footer_encoding);
    Write2Align();
  } else {
    outfile_->Append(footer_encoding);
    outfile_->Sync();
  }
  outfile_->Close();
  delete outfile_;
  outfile_ = nullptr;

  // Open SSTable file
  std::string fname = TableFileName(dbname_, file_number_);
  RandomAccessFile *infile = nullptr;
  if (is_direct_) {
    s = options_.env->NewRandomAccessFileWithDirect(fname, &infile);
  } else {
    s = options_.env->NewRandomAccessFile(fname, &infile);
  }

  assert(s.ok());

  // index block
  uint64_t index_size = index_block_handle.size();
  char *index_buf = new char[index_size];
  memcpy(index_buf, index_contents.data(), index_contents.size());

  BlockContents contents;
  contents.data = Slice(index_buf, index_size);
  contents.heap_allocated = true;
  contents.cachable = true;

  Table::Rep *rep = new Table::Rep;
  rep->options = options_;
  rep->file = infile;
  rep->metaindex_handle = meta_index_block_handle;
  rep->index_block = new Block(contents);
  rep->cache_id = 0;

  // filter table
  if (filter_table_ != nullptr) {
    uint64_t filter_size = filter_block_handle.size();
    char *filter_data = new char[filter_size];
    memcpy(filter_data, filter_contents.data(), filter_contents.size());
    rep->filter_data = filter_data;
    rep->filter_size = filter_size;
    rep->filter = new FilterTableReader(options_.filter_policy,
                                        Slice(filter_data, filter_size));
  } else {
    rep->filter_data = nullptr;
    rep->filter_size = 0;
    rep->filter = nullptr;
  }

  rep->file_number = file_number_;
  rep->file_size = file_offset_;
  rep->table_file = nullptr;

  closed_ = true;
  return new Table(rep);
}

Status TableEditor::WriteTable(WritableFile *file) {
  ;
  return status_;
}

void TableEditor::Abandon() {
  assert(!closed_);
  assert(ok());
  closed_ = true;
}

uint64_t TableEditor::FileSize() const { return file_offset_; }

uint64_t TableEditor::NumEntries() const { return num_entries_; }

}  // namespace leveldb

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include <malloc.h>

#include "leveldb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

void BlockHandle::EncodeTo(std::string *dst) const {
  // Sanity check that all fields have been set
  assert(offset_ != ~static_cast<uint64_t>(0));
  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice *input) {
  if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

void Footer::EncodeTo(std::string *dst) const {
  const size_t original_size = dst->size();
  metaindex_handle_.EncodeTo(dst);
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
  assert(dst->size() == original_size + kEncodedLength);
  (void)original_size;  // Disable unused variable warning.
}

Status Footer::DecodeFrom(Slice *input) {
  const char *magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::Corruption("not an sstable (bad magic number)");
  }

  Status result = metaindex_handle_.DecodeFrom(input);
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input);
  }
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char *end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

Status ReadBlock(bool direct_io, RandomAccessFile *file,
                 const ReadOptions &options, const BlockHandle &handle,
                 BlockContents *result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());

  uint64_t block_len = 0;
  uint64_t block_off = 0;
  char *block_buf = nullptr;
  char *value_buf = nullptr;

  uint64_t l_padding = 0;
  uint64_t r_padding = 0;
  if (direct_io) {
    uint64_t l_off = handle.offset();
    l_padding = l_off % kSectorSize;
    uint64_t r_off = (handle.offset() + n + kBlockTrailerSize) % kSectorSize;
    r_padding = (kSectorSize - r_off) % kSectorSize;

    block_off = handle.offset() - l_padding;
    block_len = l_padding + n + kBlockTrailerSize + r_padding;
    block_buf = reinterpret_cast<char *>(memalign(kSectorSize, block_len));
    assert(block_buf != nullptr);
  } else {
    block_off = handle.offset();
    block_len = n + kBlockTrailerSize;
    block_buf = new char[block_len];
  }

  Slice input;
  Status s = file->Read(block_off, block_len, &input, block_buf);

  if (direct_io) {
    value_buf = new char[n + kBlockTrailerSize];
    memcpy(value_buf, block_buf + l_padding, n + kBlockTrailerSize);
    free(block_buf);
  } else {
    value_buf = block_buf;
  }

  Slice contents(value_buf, n + kBlockTrailerSize);

  if (!s.ok()) {
    delete[] value_buf;
    return s;
  }

  // Check the crc of the type and the block contents
  const char *data = contents.data();  // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] block_buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      if (data != value_buf) {
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] value_buf;
        result->data = Slice(data, n);
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache
      } else {
        result->data = Slice(value_buf, n);
        result->heap_allocated = true;
        result->cachable = true;
      }
      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] value_buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char *ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] value_buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] value_buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      delete[] value_buf;
      return Status::Corruption("bad block type");
  }
  return Status::OK();
}

Status ReadBlockInMemory(char *buff, const ReadOptions &options,
                         const BlockHandle &handle, BlockContents *result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());

  Status s;
  Slice contents(buff + handle.offset(), n + kBlockTrailerSize);

  // Check the crc of the type and the block contents
  const char *data = contents.data();  // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }

  switch (data[n]) {
    case kNoCompression:
      result->data = Slice(data, n);
      result->heap_allocated = false;
      result->cachable = false;
      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        return Status::Corruption("corrupted compressed block contents");
      }
      char *ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;
      result->cachable = true;
      break;
    }
    default:
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

std::string ComposeFirstKey(Slice key, Slice value) {
  uint32_t shard = 0;
  memcpy(&shard, value.data(), sizeof(uint32_t));
  uint32_t non_shard = value.size() - sizeof(uint32_t);
  std::string first;
  first.append(key.data(), shard);
  first.append(value.data() + sizeof(uint32_t), non_shard);
  return first;
}

void AlignPM(uint64_t off1, uint64_t len1, uint64_t &off2, uint64_t &len2) {
  uint64_t l_padding = 0;
  uint64_t r_padding = 0;
  l_padding = off1 % kSectorSize;
  off2 = off1 - l_padding;
  r_padding = (kSectorSize - (off1 + len1) % kSectorSize) % kSectorSize;
  len2 = l_padding + len1 + r_padding;
}

}  // namespace leveldb

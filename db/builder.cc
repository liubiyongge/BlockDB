// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/table_editor.h"

namespace leveldb {

Status BuildTable(const std::string &dbname, Env *env, const Options &options,
                  TableCache *table_cache, Iterator *iter, FileMetaData *meta,
                  char *buffer, uint64_t &build_micros) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  TableEditor *editor = nullptr;
  if (iter->Valid()) {
    editor = new TableEditor(options, dbname, meta->number, meta->file_size,
                             0 /*target level*/, nullptr /* filter*/, buffer);
    uint64_t start_micros = env->NowMicros();

    ParsedInternalKey ikey;
    std::string current_user_key;
    bool has_current_user_key = false;

    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      bool drop = false;
      {
        // Remove obsolete key-value pairs
        ParseInternalKey(key, &ikey);
        if (!has_current_user_key ||
            options.comparator->Compare(ikey.user_key,
                                        Slice(current_user_key)) != 0) {
          // First occurrence of this user key
          current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
          has_current_user_key = true;
        } else {
          drop = true;
        }
      }
      if (!drop) {
        meta->largest.DecodeFrom(key);
        editor->AddPair(key, iter->value());
      }
    }
    // Finish and check for builder errors
    if (s.ok()) {
      Table *tab = editor->Finish(true);
      build_micros += (env->NowMicros() - start_micros);
      table_cache->Insert(tab);
      //
      meta->file_size = editor->FileSize();
      meta->data_size = editor->FileSize();
      assert(meta->file_size > 0);
      assert(meta->data_size > 0);

    } else {
      editor->Abandon();
    }
    delete editor;
    editor = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator *it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }
  assert(s.ok());

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0 && meta->data_size > 0) {
    // Keep it
  } else {
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb

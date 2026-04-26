// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_DB_H_
#define STORAGE_LEVELDB_INCLUDE_DB_H_

#include <cstdint>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

#include "leveldb/export.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"

namespace leveldb {

static const int kMajorVersion = 1;
static const int kMinorVersion = 23;

struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch;

class LEVELDB_EXPORT Snapshot {
 protected:
  virtual ~Snapshot();
};

struct LEVELDB_EXPORT Range {
  Range() = default;
  Range(const Slice& s, const Slice& l) : start(s), limit(l) {}
  Slice start;
  Slice limit;
};

class LEVELDB_EXPORT DB {
 public:
  static Status Open(const Options& options, const std::string& name,
                     DB** dbptr);

  DB() = default;
  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;
  virtual ~DB();

  // --------------------------------------------------------
  // CompactionReport MUST be declared before ForceFullCompaction
  // so the compiler knows the type when it parses the signature.
  // --------------------------------------------------------
  struct CompactionReport {
    int     num_compactions    = 0;
    int     total_input_files  = 0;
    int     total_output_files = 0;
    int64_t bytes_read         = 0;
    int64_t bytes_written      = 0;

    void Print() const {
      printf(
        "\n========================================\n"
        "   ForceFullCompaction Statistics\n"
        "========================================\n"
        "  Compaction rounds executed : %d\n"
        "  Total input  files         : %d\n"
        "  Total output files         : %d\n"
        "  Total bytes read           : %s\n"
        "  Total bytes written        : %s\n"
        "========================================\n\n",
        num_compactions,
        total_input_files,
        total_output_files,
        FormatBytes(bytes_read).c_str(),
        FormatBytes(bytes_written).c_str()
      );
    }

    static std::string FormatBytes(int64_t bytes) {
      char buf[64];
      if      (bytes >= (1LL << 30))
        snprintf(buf, sizeof(buf), "%.2f GB", bytes / double(1LL << 30));
      else if (bytes >= (1LL << 20))
        snprintf(buf, sizeof(buf), "%.2f MB", bytes / double(1LL << 20));
      else if (bytes >= (1LL << 10))
        snprintf(buf, sizeof(buf), "%.2f KB", bytes / double(1LL << 10));
      else
        snprintf(buf, sizeof(buf), "%lld B", (long long)bytes);
      return std::string(buf);
    }
  };

  // Put: insert or update a key-value pair
  virtual Status Put(const WriteOptions& options, const Slice& key,
                     const Slice& value) = 0;

  // Delete: remove a single key
  virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

  // DeleteRange: remove all keys in [start_key, end_key)
  virtual Status DeleteRange(const WriteOptions& options,
                             const Slice& start_key,
                             const Slice& end_key) = 0;

  // Write: apply a WriteBatch atomically
  virtual Status Write(const WriteOptions& options, WriteBatch* updates) = 0;

  // Scan: return all key-value pairs in [start_key, end_key)
  virtual Status Scan(const ReadOptions& options,
                      const Slice& start_key,
                      const Slice& end_key,
                      std::vector<std::pair<std::string,
                                            std::string>>* result) = 0;

  // Get: retrieve value for a single key
  virtual Status Get(const ReadOptions& options, const Slice& key,
                     std::string* value) = 0;

  // NewIterator: return an iterator over the database
  virtual Iterator* NewIterator(const ReadOptions& options) = 0;

  // Snapshot management
  virtual const Snapshot* GetSnapshot() = 0;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;

  // Property queries
  virtual bool GetProperty(const Slice& property, std::string* value) = 0;

  // Approximate sizes
  virtual void GetApproximateSizes(const Range* range, int n,
                                   uint64_t* sizes) = 0;

  // CompactRange: compact keys in [*begin, *end]
  // Pass nullptr for begin/end to compact entire database
  virtual void CompactRange(const Slice* begin, const Slice* end) = 0;

  // ForceFullCompaction: synchronous full compaction across all levels.
  // Blocks reads and writes until complete.
  // Stats are printed on completion.
  // CompactionReport is declared above so the type is visible here.
 virtual Status ForceFullCompaction() = 0;
};

LEVELDB_EXPORT Status DestroyDB(const std::string& name,
                                const Options& options);

LEVELDB_EXPORT Status RepairDB(const std::string& dbname,
                               const Options& options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_DB_H_
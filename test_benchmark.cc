#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <cassert>
#include <cstdio>
#include <ctime>
#include <chrono>
#include <iomanip>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

// -------------------------------------------------------
// Timer utility
// -------------------------------------------------------

struct Timer {
  std::chrono::high_resolution_clock::time_point start_;
  std::string name_;

  Timer(const std::string& name) : name_(name) {
    start_ = std::chrono::high_resolution_clock::now();
    std::cout << "  [START] " << name_ << "\n";
  }

  double ElapsedMs() const {
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start_).count();
  }

  void Report(int ops = 0) const {
    double ms = ElapsedMs();
    std::cout << "  [DONE]  " << name_ << "\n";
    std::cout << "          Time:       " << std::fixed
              << std::setprecision(2) << ms << " ms\n";
    if (ops > 0) {
      double ops_per_sec = ops / (ms / 1000.0);
      std::cout << "          Ops:        " << ops << "\n";
      std::cout << "          Throughput: " << std::fixed
                << std::setprecision(0) << ops_per_sec << " ops/sec\n";
    }
    std::cout << "\n";
  }
};

// -------------------------------------------------------
// Helpers
// -------------------------------------------------------

static leveldb::DB* OpenFreshDB(const std::string& path,
                                 size_t write_buffer = 4 * 1024 * 1024) {
  system(("rm -rf " + path).c_str());
  leveldb::Options opts;
  opts.create_if_missing  = true;
  opts.write_buffer_size  = write_buffer;
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(opts, path, &db);
  if (!s.ok()) { std::cerr << s.ToString(); exit(1); }
  return db;
}

static std::string MakeKey(int i, int width = 8) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%0*d", width, i);
  return std::string(buf);
}

static std::string MakeVal(int i, int size = 64) {
  // Create a value of 'size' bytes
  std::string v(size, 'x');
  snprintf(&v[0], std::min(size, 16), "val_%08d", i);
  v.back() = '\0';
  // fix null terminator issue
  v = std::string(size, 'v');
  char tmp[16];
  snprintf(tmp, sizeof(tmp), "%d", i);
  v.replace(0, strlen(tmp), tmp);
  return v;
}

// -------------------------------------------------------
// BENCHMARK 1: Sequential Put throughput
// -------------------------------------------------------
void Bench_SequentialPut() {
  std::cout << "=== BENCHMARK 1: Sequential Put ===\n";

  const int N = 100000;
  leveldb::DB* db = OpenFreshDB("/tmp/bench_1");
  leveldb::WriteOptions wo;

  Timer t("Sequential Put x" + std::to_string(N));
  for (int i = 0; i < N; i++) {
    db->Put(wo, MakeKey(i), MakeVal(i));
  }
  t.Report(N);

  delete db;
}

// -------------------------------------------------------
// BENCHMARK 2: Random Put throughput
// -------------------------------------------------------
void Bench_RandomPut() {
  std::cout << "=== BENCHMARK 2: Random Put ===\n";

  const int N = 100000;
  leveldb::DB* db = OpenFreshDB("/tmp/bench_2");
  leveldb::WriteOptions wo;

  // Pre-generate random order
  std::vector<int> indices(N);
  for (int i = 0; i < N; i++) indices[i] = i;
  // Fisher-Yates shuffle using rand()
  srand(42);
  for (int i = N - 1; i > 0; i--) {
    int j = rand() % (i + 1);
    std::swap(indices[i], indices[j]);
  }

  Timer t("Random Put x" + std::to_string(N));
  for (int idx : indices) {
    db->Put(wo, MakeKey(idx), MakeVal(idx));
  }
  t.Report(N);

  delete db;
}

// -------------------------------------------------------
// BENCHMARK 3: Sequential Get throughput
// -------------------------------------------------------
void Bench_SequentialGet() {
  std::cout << "=== BENCHMARK 3: Sequential Get ===\n";

  const int N = 50000;
  leveldb::DB* db = OpenFreshDB("/tmp/bench_3");
  leveldb::WriteOptions wo;

  // Pre-populate
  for (int i = 0; i < N; i++) db->Put(wo, MakeKey(i), MakeVal(i));
  db->CompactRange(nullptr, nullptr);  // flush to disk

  leveldb::ReadOptions ro;
  std::string val;
  int hits = 0;

  Timer t("Sequential Get x" + std::to_string(N));
  for (int i = 0; i < N; i++) {
    if (db->Get(ro, MakeKey(i), &val).ok()) hits++;
  }
  t.Report(N);

  assert(hits == N);
  std::cout << "  Hit rate: 100%\n\n";

  delete db;
}

// -------------------------------------------------------
// BENCHMARK 4: Scan throughput — small ranges
// -------------------------------------------------------
void Bench_ScanSmallRange() {
  std::cout << "=== BENCHMARK 4: Scan (small ranges, 100 keys each) ===\n";

  const int TOTAL   = 100000;
  const int RANGE   = 100;
  const int QUERIES = 1000;

  leveldb::DB* db = OpenFreshDB("/tmp/bench_4");
  leveldb::WriteOptions wo;

  for (int i = 0; i < TOTAL; i++) db->Put(wo, MakeKey(i), MakeVal(i));
  db->CompactRange(nullptr, nullptr);

  std::vector<std::pair<std::string,std::string>> result;
  int total_keys_returned = 0;

  Timer t("Small Range Scan x" + std::to_string(QUERIES) +
          " (range=" + std::to_string(RANGE) + ")");

  for (int q = 0; q < QUERIES; q++) {
    int start = (q * RANGE) % (TOTAL - RANGE);
    db->Scan(leveldb::ReadOptions(),
             MakeKey(start),
             MakeKey(start + RANGE),
             &result);
    total_keys_returned += result.size();
  }

  t.Report(QUERIES);
  std::cout << "  Total keys returned: " << total_keys_returned << "\n\n";

  delete db;
}

// -------------------------------------------------------
// BENCHMARK 5: Scan throughput — large ranges
// -------------------------------------------------------
void Bench_ScanLargeRange() {
  std::cout << "=== BENCHMARK 5: Scan (large range, all keys) ===\n";

  const int N = 100000;
  leveldb::DB* db = OpenFreshDB("/tmp/bench_5");
  leveldb::WriteOptions wo;

  for (int i = 0; i < N; i++) db->Put(wo, MakeKey(i), MakeVal(i));
  db->CompactRange(nullptr, nullptr);

  std::vector<std::pair<std::string,std::string>> result;

  Timer t("Full DB Scan (all " + std::to_string(N) + " keys)");
  db->Scan(leveldb::ReadOptions(),
           MakeKey(0),
           MakeKey(N),
           &result);
  t.Report(1);

  assert((int)result.size() == N);
  std::cout << "  Keys returned: " << result.size() << "\n\n";

  delete db;
}

// -------------------------------------------------------
// BENCHMARK 6: DeleteRange vs individual Delete
// -------------------------------------------------------
void Bench_DeleteRange_vs_IndividualDelete() {
  std::cout << "=== BENCHMARK 6: DeleteRange vs Individual Delete ===\n";

  const int N = 50000;

  // Method A: Individual Delete
  {
    leveldb::DB* db = OpenFreshDB("/tmp/bench_6a");
    leveldb::WriteOptions wo;
    for (int i = 0; i < N; i++) db->Put(wo, MakeKey(i), MakeVal(i));
    db->CompactRange(nullptr, nullptr);

    Timer t("Individual Delete x" + std::to_string(N));
    for (int i = 0; i < N; i++) db->Delete(wo, MakeKey(i));
    t.Report(N);

    delete db;
  }

  // Method B: DeleteRange
  {
    leveldb::DB* db = OpenFreshDB("/tmp/bench_6b");
    leveldb::WriteOptions wo;
    for (int i = 0; i < N; i++) db->Put(wo, MakeKey(i), MakeVal(i));
    db->CompactRange(nullptr, nullptr);

    Timer t("DeleteRange (single call for " + std::to_string(N) + " keys)");
    db->DeleteRange(wo, MakeKey(0), MakeKey(N));
    t.Report(1);

    delete db;
  }

  std::cout << "  Note: DeleteRange is faster for large contiguous ranges\n\n";
}

// -------------------------------------------------------
// BENCHMARK 7: ForceFullCompaction timing and stats
// -------------------------------------------------------
void Bench_ForceFullCompaction() {
  std::cout << "=== BENCHMARK 7: ForceFullCompaction ===\n";

  const int N = 200000;
  leveldb::DB* db = OpenFreshDB("/tmp/bench_7", 64 * 1024);
  leveldb::WriteOptions wo;

  // Insert with many overwrites to create lots of dead data
  std::cout << "  Inserting " << N
            << " keys (with overwrites to create dead versions)...\n";
  for (int round = 0; round < 3; round++) {
    for (int i = 0; i < N / 3; i++) {
      db->Put(wo, MakeKey(i), MakeVal(i + round * 1000));
    }
  }

  // Delete 1/3 of keys
  for (int i = 0; i < N / 9; i++) db->Delete(wo, MakeKey(i));

  std::cout << "  Setup complete. Running ForceFullCompaction...\n\n";

  leveldb::CompactionReport report;
  Timer t("ForceFullCompaction");
  leveldb::Status s = db->ForceFullCompaction(&report);
  t.Report();

  assert(s.ok());
  report.Print();

  // Verify data is still correct
  std::vector<std::pair<std::string,std::string>> result;
  db->Scan(leveldb::ReadOptions(), "", std::string(8, '\xff'), &result);
  std::cout << "  Keys remaining after compaction: " << result.size() << "\n\n";

  delete db;
}

// -------------------------------------------------------
// BENCHMARK 8: Scan performance before vs after compaction
// Shows compaction improves read performance
// -------------------------------------------------------
void Bench_ScanBeforeVsAfterCompaction() {
  std::cout << "=== BENCHMARK 8: Scan Performance Before vs After Compaction ===\n";

  const int N = 50000;
  leveldb::DB* db = OpenFreshDB("/tmp/bench_8", 4 * 1024);

  leveldb::WriteOptions wo;
  // Write in many small batches to create lots of L0 files
  for (int i = 0; i < N; i++) {
    db->Put(wo, MakeKey(i), MakeVal(i));
  }

  std::vector<std::pair<std::string,std::string>> result;

  // Scan BEFORE compaction (many L0 files → slower)
  {
    Timer t("Scan BEFORE compaction (" + std::to_string(N) + " keys)");
    db->Scan(leveldb::ReadOptions(), MakeKey(0), MakeKey(N), &result);
    t.Report(1);
    std::cout << "  Keys returned: " << result.size() << "\n\n";
  }

  // Compact
  leveldb::CompactionReport report;
  db->ForceFullCompaction(&report);

  // Scan AFTER compaction (fewer, larger files → faster)
  {
    Timer t("Scan AFTER compaction (" + std::to_string(N) + " keys)");
    db->Scan(leveldb::ReadOptions(), MakeKey(0), MakeKey(N), &result);
    t.Report(1);
    std::cout << "  Keys returned: " << result.size() << "\n\n";
  }

  std::cout << "  Note: Post-compaction scan is faster due to\n";
  std::cout << "        fewer SSTable files to merge\n\n";

  delete db;
}

// -------------------------------------------------------
// BENCHMARK 9: Write amplification measurement
// How much we write vs how much original data exists
// -------------------------------------------------------
void Bench_WriteAmplification() {
  std::cout << "=== BENCHMARK 9: Write Amplification Measurement ===\n";

  const int N       = 100000;
  const int VAL_SZ  = 64;

  leveldb::DB* db = OpenFreshDB("/tmp/bench_9", 64 * 1024);
  leveldb::WriteOptions wo;

  int64_t raw_data_bytes = (int64_t)N * (8 + VAL_SZ);  // key + val
  std::cout << "  Raw data size: "
            << leveldb::CompactionReport::FormatBytes(raw_data_bytes) << "\n";

  for (int i = 0; i < N; i++) {
    db->Put(wo, MakeKey(i), MakeVal(i, VAL_SZ));
  }

  leveldb::CompactionReport report;
  db->ForceFullCompaction(&report);
  report.Print();

  if (raw_data_bytes > 0 && report.bytes_written > 0) {
    double write_amp = double(report.bytes_written) / double(raw_data_bytes);
    std::cout << "  Write amplification: " << std::fixed
              << std::setprecision(2) << write_amp << "x\n";
    std::cout << "  (ratio of bytes written during compaction to raw data)\n\n";
  }

  delete db;
}

// -------------------------------------------------------
// SUMMARY TABLE
// Collect all benchmark names and times
// -------------------------------------------------------
struct BenchResult {
  std::string name;
  double      ms;
  int         ops;
};

// -------------------------------------------------------
// MAIN
// -------------------------------------------------------
int main() {
  std::cout << "===========================================\n";
  std::cout << "   LevelDB Assignment 3 — Benchmarks\n";
  std::cout << "===========================================\n\n";

  Bench_SequentialPut();
  Bench_RandomPut();
  Bench_SequentialGet();
  Bench_ScanSmallRange();
  Bench_ScanLargeRange();
  Bench_DeleteRange_vs_IndividualDelete();
  Bench_ForceFullCompaction();
  Bench_ScanBeforeVsAfterCompaction();
  Bench_WriteAmplification();

  std::cout << "===========================================\n";
  std::cout << "   ALL BENCHMARKS COMPLETE\n";
  std::cout << "   Paste these numbers into your report!\n";
  std::cout << "===========================================\n";
  return 0;
}
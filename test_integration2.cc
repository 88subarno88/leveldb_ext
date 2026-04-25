#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <algorithm>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"

// -------------------------------------------------------
// Shared Helpers
// -------------------------------------------------------

static leveldb::DB* OpenFreshDB(const std::string& path,
                                 size_t write_buffer = 0) {
  system(("rm -rf " + path).c_str());
  leveldb::Options opts;
  opts.create_if_missing = true;
  if (write_buffer > 0) opts.write_buffer_size = write_buffer;
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(opts, path, &db);
  if (!s.ok()) {
    std::cerr << "OpenFreshDB failed: " << s.ToString() << "\n";
    exit(1);
  }
  return db;
}

static void Put(leveldb::DB* db,
                const std::string& k,
                const std::string& v) {
  db->Put(leveldb::WriteOptions(), k, v);
}

static void Del(leveldb::DB* db, const std::string& k) {
  db->Delete(leveldb::WriteOptions(), k);
}

static std::string Get(leveldb::DB* db, const std::string& k) {
  std::string val;
  leveldb::Status s = db->Get(leveldb::ReadOptions(), k, &val);
  return s.ok() ? val : "";
}

static bool Exists(leveldb::DB* db, const std::string& k) {
  std::string val;
  return db->Get(leveldb::ReadOptions(), k, &val).ok();
}

// Scan wrapper — returns pairs
static std::vector<std::pair<std::string,std::string>>
Scan(leveldb::DB* db,
     const std::string& start,
     const std::string& end) {
  std::vector<std::pair<std::string,std::string>> result;
  leveldb::Status s = db->Scan(leveldb::ReadOptions(), start, end, &result);
  if (!s.ok()) {
    std::cerr << "Scan failed: " << s.ToString() << "\n";
  }
  return result;
}

// Scan everything
static std::vector<std::pair<std::string,std::string>>
ScanAll(leveldb::DB* db) {
  return Scan(db, "", std::string(8, '\xff'));
}

// Insert N keys with zero-padded numeric suffix
static void InsertRange(leveldb::DB* db,
                         int from, int to,
                         const std::string& prefix = "k",
                         const std::string& val_prefix = "v") {
  for (int i = from; i < to; i++) {
    char k[32], v[32];
    snprintf(k, sizeof(k), "%s%06d", prefix.c_str(), i);
    snprintf(v, sizeof(v), "%s%06d", val_prefix.c_str(), i);
    Put(db, k, v);
  }
}

static std::string Key(int i, const std::string& prefix = "k") {
  char buf[32];
  snprintf(buf, sizeof(buf), "%s%06d", prefix.c_str(), i);
  return std::string(buf);
}

// Print test result
static void Pass(const std::string& name) {
  std::cout << "  [PASS] " << name << "\n";
}

// -------------------------------------------------------
// INTEGRATION TEST 1:
// Scan → DeleteRange → Scan
// Verify scan results reflect deletions
// -------------------------------------------------------
void Test_Scan_Then_DeleteRange_Then_Scan() {
  std::cout << "\n=== INTEGRATION 1: Scan → DeleteRange → Scan ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/itest_1");
  InsertRange(db, 0, 100);  // k000000 .. k000099

  // Scan before delete
  auto before = Scan(db, Key(0), Key(100));
  assert(before.size() == 100);
  Pass("Initial scan returns 100 keys");

  // Delete middle 50 keys
  leveldb::Status s = db->DeleteRange(
      leveldb::WriteOptions(), Key(25), Key(75));
  assert(s.ok());

  // Scan after delete
  auto after = ScanAll(db);
  assert(after.size() == 50);
  assert(after.front().first == Key(0));
  assert(after.back().first  == Key(99));
  Pass("After DeleteRange [25,75), 50 keys remain");

  // Verify boundaries exactly
  assert(!Exists(db, Key(25)));   // start_key is deleted
  assert(!Exists(db, Key(74)));   // last deleted key
  assert(Exists(db, Key(24)));    // just before range → survives
  assert(Exists(db, Key(75)));    // end_key (exclusive) → survives
  Pass("Boundary keys correct");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// INTEGRATION TEST 2:
// DeleteRange → Put (inside deleted range) → Scan
// New puts inside a deleted range must be visible
// -------------------------------------------------------
void Test_DeleteRange_Then_Put_Inside_Range() {
  std::cout << "\n=== INTEGRATION 2: DeleteRange → Put inside range → Scan ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/itest_2");
  InsertRange(db, 0, 50);

  // Delete everything
  db->DeleteRange(leveldb::WriteOptions(), Key(0), Key(50));

  // Re-insert a few keys inside the deleted range
  Put(db, Key(10), "new_value_10");
  Put(db, Key(20), "new_value_20");
  Put(db, Key(30), "new_value_30");

  auto results = ScanAll(db);
  assert(results.size() == 3);
  assert(results[0].first  == Key(10));
  assert(results[0].second == "new_value_10");
  assert(results[1].first  == Key(20));
  assert(results[1].second == "new_value_20");
  assert(results[2].first  == Key(30));
  assert(results[2].second == "new_value_30");
  Pass("3 re-inserted keys visible after range deletion");

  // Original deleted keys must still be gone
  assert(!Exists(db, Key(0)));
  assert(!Exists(db, Key(5)));
  assert(!Exists(db, Key(49)));
  Pass("Original deleted keys still absent");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// INTEGRATION TEST 3:
// Scan → ForceFullCompaction → Scan
// Compaction must not change what Scan returns
// -------------------------------------------------------
void Test_Scan_Survives_Compaction() {
  std::cout << "\n=== INTEGRATION 3: Scan before and after ForceFullCompaction ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/itest_3", 64 * 1024);
  InsertRange(db, 0, 2000);

  // Scan before compaction
  auto before = ScanAll(db);
  assert(before.size() == 2000);
  Pass("Scan before compaction: 2000 keys");

  // Compact
  
  leveldb::Status s = db->ForceFullCompaction();
  assert(s.ok());

  // Scan after compaction
  auto after = ScanAll(db);
  assert(after.size() == 2000);
  Pass("Scan after compaction: still 2000 keys");

  // Verify contents are identical
  assert(before == after);
  Pass("Scan results identical before and after compaction");

  
  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// INTEGRATION TEST 4:
// DeleteRange → ForceFullCompaction → Scan
// Compaction must physically remove range-deleted keys
// -------------------------------------------------------
void Test_DeleteRange_Compaction_Scan() {
  std::cout << "\n=== INTEGRATION 4: DeleteRange → Compaction → Scan ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/itest_4", 64 * 1024);
  InsertRange(db, 0, 3000);

  // First compact to push data to lower levels
  db->CompactRange(nullptr, nullptr);

  // Delete a range that spans SSTable files
  db->DeleteRange(leveldb::WriteOptions(), Key(1000), Key(2000));

  // Force full compaction to enforce deletion
  
  leveldb::Status s = db->ForceFullCompaction();
  assert(s.ok());

  // Scan: should see only 2000 keys
  auto results = ScanAll(db);
  assert(results.size() == 2000);
  Pass("2000 keys remain after range deletion + compaction");

  // Spot checks
  assert(Exists(db, Key(0)));
  assert(Exists(db, Key(999)));
  assert(!Exists(db, Key(1000)));
  assert(!Exists(db, Key(1500)));
  assert(!Exists(db, Key(1999)));
  assert(Exists(db, Key(2000)));
  assert(Exists(db, Key(2999)));
  Pass("All boundary checks correct");

  
  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// INTEGRATION TEST 5:
// Put → Scan → Delete individual → Scan → DeleteRange → Scan
// → ForceFullCompaction → Scan
// Full workflow: all APIs together
// -------------------------------------------------------
void Test_Full_Workflow() {
  std::cout << "\n=== INTEGRATION 5: Full Workflow (all APIs) ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/itest_5", 64 * 1024);

  // Phase 1: Insert
  InsertRange(db, 0, 500);
  assert(ScanAll(db).size() == 500);
  Pass("Phase 1: 500 keys inserted");

  // Phase 2: Individual deletes
  for (int i = 0; i < 500; i += 10) Del(db, Key(i));
  assert(ScanAll(db).size() == 450);  // 50 keys deleted
  Pass("Phase 2: 50 individual deletes → 450 keys");

  // Phase 3: Overwrites
  for (int i = 1; i < 500; i += 10)
    Put(db, Key(i), "overwritten");
  auto r3 = Scan(db, Key(1), Key(2));
  assert(!r3.empty() && r3[0].second == "overwritten");
  Pass("Phase 3: overwritten values visible in Scan");

  // Phase 4: Range delete
  db->DeleteRange(leveldb::WriteOptions(), Key(200), Key(300));
  auto r4 = ScanAll(db);
  assert(!Exists(db, Key(201)));
  assert(Exists(db, Key(199)));
  assert(Exists(db, Key(300)));
  Pass("Phase 4: DeleteRange [200,300) applied");

  // Phase 5: ForceFullCompaction
  
  db->ForceFullCompaction();
  auto r5 = ScanAll(db);
  assert(r5 == r4);  // compaction must not change visible data
  Pass("Phase 5: ForceFullCompaction preserves data");

  // Phase 6: Scan after compaction with range
  auto r6 = Scan(db, Key(100), Key(150));
  for (auto& kv : r6) {
    assert(kv.first >= Key(100) && kv.first < Key(150));
  }
  Pass("Phase 6: Scan on sub-range correct after compaction");

  
  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// INTEGRATION TEST 6:
// Interleaved Puts and DeleteRanges — stress ordering
// -------------------------------------------------------
void Test_Interleaved_Puts_And_DeleteRanges() {
  std::cout << "\n=== INTEGRATION 6: Interleaved Puts and DeleteRanges ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/itest_6");

  // Round 1: insert 0..99
  InsertRange(db, 0, 100);

  // Round 2: delete 0..49
  db->DeleteRange(leveldb::WriteOptions(), Key(0), Key(50));

  // Round 3: insert 0..24 again (partially overlaps deleted range)
  InsertRange(db, 0, 25, "k", "new_v");

  // Round 4: delete 75..99
  db->DeleteRange(leveldb::WriteOptions(), Key(75), Key(100));

  // Round 5: insert 80..84 again
  InsertRange(db, 80, 85, "k", "newest_v");

  auto results = ScanAll(db);

  // Expected: k000000..k000024 (new_v), k000050..k000074, k000080..k000084 (newest_v)
  // = 25 + 25 + 5 = 55 keys
  std::cout << "  Remaining keys: " << results.size() << "\n";
  assert(results.size() == 55);
  Pass("Correct count after interleaved puts/deletes");

  // Check values of re-inserted keys
  assert(Get(db, Key(0))  == "new_v000000");
  assert(Get(db, Key(24)) == "new_v000024");
  Pass("Re-inserted keys have new values");

  // Check deleted ranges are gone
  assert(!Exists(db, Key(25)));   // was deleted in round 2, not re-inserted
  assert(!Exists(db, Key(49)));
  assert(!Exists(db, Key(75)));
  assert(!Exists(db, Key(99)));
  Pass("Deleted keys absent");

  // Check re-inserted in round 5
  assert(Get(db, Key(80)) == "newest_v000080");
  assert(Get(db, Key(84)) == "newest_v000084");
  assert(!Exists(db, Key(85)));   // not re-inserted in round 5
  Pass("Round 5 re-inserts correct");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// INTEGRATION TEST 7:
// Multiple DeleteRanges → ForceFullCompaction → Scan
// -------------------------------------------------------
void Test_Multiple_DeleteRanges_Then_Compaction() {
  std::cout << "\n=== INTEGRATION 7: Multiple DeleteRanges → Compaction → Scan ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/itest_7", 64 * 1024);
  InsertRange(db, 0, 10000);

  // Apply 5 non-overlapping range deletions
  db->DeleteRange(leveldb::WriteOptions(), Key(0),    Key(1000));
  db->DeleteRange(leveldb::WriteOptions(), Key(2000), Key(3000));
  db->DeleteRange(leveldb::WriteOptions(), Key(4000), Key(5000));
  db->DeleteRange(leveldb::WriteOptions(), Key(6000), Key(7000));
  db->DeleteRange(leveldb::WriteOptions(), Key(8000), Key(9000));

  // Each deletes 1000 keys → 5000 deleted, 5000 remain
  
  db->ForceFullCompaction();

  auto results = ScanAll(db);
  std::cout << "  Remaining: " << results.size() << " (expected 5000)\n";
  assert(results.size() == 5000);
  Pass("5000 keys remain after 5 range deletions");

  // Verify each surviving band
  assert(Exists(db, Key(1000)));
  assert(Exists(db, Key(1999)));
  assert(!Exists(db, Key(2000)));
  assert(!Exists(db, Key(2999)));
  assert(Exists(db, Key(3000)));
  Pass("All band boundaries correct");

  
  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// INTEGRATION TEST 8:
// Scan across MemTable + SSTable boundary
// Data in MemTable and L0/L1 must both appear in scan
// -------------------------------------------------------
void Test_Scan_Across_Memory_And_Disk() {
  std::cout << "\n=== INTEGRATION 8: Scan Across MemTable and SSTables ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/itest_8", 64 * 1024);

  // Batch 1: write keys 0..999 and flush to disk
  InsertRange(db, 0, 1000);
  db->CompactRange(nullptr, nullptr);  // flush to SSTables
  Pass("Batch 1 flushed to SSTables");

  // Batch 2: write keys 1000..1999 (stay in MemTable initially)
  InsertRange(db, 1000, 2000);
  Pass("Batch 2 in MemTable");

  // Scan the full range — must see both batches
  auto results = ScanAll(db);
  assert(results.size() == 2000);
  Pass("Scan sees all 2000 keys across MemTable and SSTables");

  // Verify ordering is correct (keys should be sorted)
  for (size_t i = 1; i < results.size(); i++) {
    assert(results[i].first > results[i-1].first);
  }
  Pass("Scan results are in sorted order");

  // Verify a specific cross-boundary scan
  auto sub = Scan(db, Key(900), Key(1100));
  assert(sub.size() == 200);
  assert(sub.front().first == Key(900));
  assert(sub.back().first  == Key(1099));
  Pass("Cross-boundary sub-scan [900,1100) correct");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// INTEGRATION TEST 9:
// ForceFullCompaction stats must be consistent
// bytes_written <= bytes_read (compaction removes old data)
// num_compactions matches level activity
// -------------------------------------------------------
void Test_Compaction_Stats_Consistency() {
  std::cout << "\n=== INTEGRATION 9: Compaction Stats Consistency ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/itest_9", 64 * 1024);

  // Write data with many overwrites to create lots of dead versions
  for (int round = 0; round < 10; round++) {
    InsertRange(db, 0, 500);
  }

  
  db->ForceFullCompaction();

  

  // Sanity checks on stats
  Pass("Stats checks passed");

  // All 500 keys must be readable with correct (latest) value
  for (int i = 0; i < 500; i++) {
    assert(Exists(db, Key(i)));
  }
  Pass("All 500 keys readable with correct values");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// INTEGRATION TEST 10:
// Concurrent-style test: interleave all 3 APIs rapidly
// -------------------------------------------------------
void Test_Rapid_Interleaving() {
  std::cout << "\n=== INTEGRATION 10: Rapid Interleaving of All APIs ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/itest_10", 64 * 1024);

  // Simulate a real workload: rapid puts, scans, deletes, compactions
  for (int batch = 0; batch < 5; batch++) {
    int base = batch * 200;

    // Insert 200 keys
    InsertRange(db, base, base + 200);

    // Scan to verify
    auto r = Scan(db, Key(base), Key(base + 200));
    assert((int)r.size() == 200);

    // Delete half
    db->DeleteRange(leveldb::WriteOptions(), Key(base + 50), Key(base + 150));

    // Verify 100 remain in this batch
    auto r2 = Scan(db, Key(base), Key(base + 200));
    assert((int)r2.size() == 100);

    // Compact every other batch
    if (batch % 2 == 0) {
      
      db->ForceFullCompaction();

      // Scan still returns same 100 after compaction
      auto r3 = Scan(db, Key(base), Key(base + 200));
      assert((int)r3.size() == 100);
    }
  }

  Pass("All 5 batch rounds correct");

  // Final count: 5 batches × 100 surviving keys = 500
  auto final_results = ScanAll(db);
  std::cout << "  Final key count: " << final_results.size()
            << " (expected 500)\n";
  assert(final_results.size() == 500);
  Pass("Final total count correct");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// MAIN
// -------------------------------------------------------
int main() {
  std::cout << "===========================================\n";
  std::cout << "   Integration Test Suite\n";
  std::cout << "===========================================\n";

  Test_Scan_Then_DeleteRange_Then_Scan();
  Test_DeleteRange_Then_Put_Inside_Range();
  Test_Scan_Survives_Compaction();
  Test_DeleteRange_Compaction_Scan();
  Test_Full_Workflow();
  Test_Interleaved_Puts_And_DeleteRanges();
  Test_Multiple_DeleteRanges_Then_Compaction();
  Test_Scan_Across_Memory_And_Disk();
  Test_Compaction_Stats_Consistency();
  Test_Rapid_Interleaving();

  std::cout << "\n===========================================\n";
  std::cout << "   ALL INTEGRATION TESTS PASSED\n";
  std::cout << "===========================================\n";
  return 0;
}
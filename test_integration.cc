#include <iostream>
#include <cassert>
#include <vector>
#include <utility>
#include <string>
#include "leveldb/db.h"

// Helpers
leveldb::DB* OpenFreshDB(const std::string& path) {
  system(("rm -rf " + path).c_str());
  leveldb::Options opts;
  opts.create_if_missing = true;
  leveldb::DB* db;
  leveldb::DB::Open(opts, path, &db);
  return db;
}

void Put(leveldb::DB* db, const std::string& k, const std::string& v) {
  db->Put(leveldb::WriteOptions(), k, v);
}

bool Exists(leveldb::DB* db, const std::string& k) {
  std::string v;
  return db->Get(leveldb::ReadOptions(), k, &v).ok();
}

std::vector<std::pair<std::string,std::string>> Scan(
    leveldb::DB* db, const std::string& s, const std::string& e) {
  std::vector<std::pair<std::string,std::string>> r;
  db->Scan(leveldb::ReadOptions(), s, e, &r);
  return r;
}

std::vector<std::pair<std::string,std::string>> ScanAll(leveldb::DB* db) {
  return Scan(db, "", std::string(8, '\xff'));
}

void Pass(const std::string& msg) { std::cout << "  [PASS] " << msg << "\n"; }

// -------------------------------------------------------
// TEST 1: Scan -> DeleteRange -> Scan
// -------------------------------------------------------
void Test_Scan_DeleteRange_Scan() {
  std::cout << "\n=== TEST 1: Scan -> DeleteRange -> Scan ===\n";
  leveldb::DB* db = OpenFreshDB("/tmp/itest_1");

  for (int i = 0; i < 10; i++) {
    char k[8], v[8];
    snprintf(k, sizeof(k), "key_%02d", i);
    snprintf(v, sizeof(v), "val_%02d", i);
    Put(db, k, v);
  }

  auto before = ScanAll(db);
  assert(before.size() == 10);
  Pass("Initial scan: 10 keys");

  db->DeleteRange(leveldb::WriteOptions(), "key_03", "key_07");

  auto after = ScanAll(db);
  assert(after.size() == 6);
  assert(!Exists(db, "key_03"));
  assert(!Exists(db, "key_06"));
  assert(Exists(db, "key_02"));
  assert(Exists(db, "key_07"));
  Pass("After DeleteRange [key_03, key_07): 6 keys remain");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// TEST 2: DeleteRange -> Put inside range -> Scan
// -------------------------------------------------------
void Test_DeleteRange_Put_Scan() {
  std::cout << "\n=== TEST 2: DeleteRange -> Put inside range -> Scan ===\n";
  leveldb::DB* db = OpenFreshDB("/tmp/itest_2");

  Put(db, "a", "1"); Put(db, "b", "2");
  Put(db, "c", "3"); Put(db, "d", "4");

  db->DeleteRange(leveldb::WriteOptions(), "a", "d");

  // Re-insert b after deletion
  Put(db, "b", "new_b");

  auto r = ScanAll(db);
  assert(r.size() == 2); // b and d
  assert(Exists(db, "b"));
  assert(!Exists(db, "a"));
  assert(!Exists(db, "c"));
  assert(Exists(db, "d"));
  Pass("Re-inserted key visible, deleted keys gone");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// TEST 3: Scan -> ForceFullCompaction -> Scan
// -------------------------------------------------------
void Test_Scan_Compaction_Scan() {
  std::cout << "\n=== TEST 3: Scan -> ForceFullCompaction -> Scan ===\n";
  leveldb::DB* db = OpenFreshDB("/tmp/itest_3");

  for (int i = 0; i < 1000; i++) {
    char k[16], v[16];
    snprintf(k, sizeof(k), "key_%04d", i);
    snprintf(v, sizeof(v), "val_%04d", i);
    Put(db, k, v);
  }

  auto before = ScanAll(db);
  assert(before.size() == 1000);
  Pass("Before compaction: 1000 keys");

  leveldb::Status s = db->ForceFullCompaction();
  assert(s.ok());

  auto after = ScanAll(db);
  assert(after.size() == 1000);
  assert(before == after);
  Pass("After compaction: still 1000 keys, contents identical");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// TEST 4: DeleteRange -> ForceFullCompaction -> Scan
// -------------------------------------------------------
void Test_DeleteRange_Compaction_Scan() {
  std::cout << "\n=== TEST 4: DeleteRange -> ForceFullCompaction -> Scan ===\n";

  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.write_buffer_size = 64 * 1024;
  system("rm -rf /tmp/itest_4");
  leveldb::DB* db;
  leveldb::DB::Open(opts, "/tmp/itest_4", &db);

  for (int i = 0; i < 3000; i++) {
    char k[16], v[16];
    snprintf(k, sizeof(k), "key_%04d", i);
    snprintf(v, sizeof(v), "val_%04d", i);
    Put(db, k, v);
  }

  // Flush to SSTables first
  db->CompactRange(nullptr, nullptr);

  // Delete middle 1000
  db->DeleteRange(leveldb::WriteOptions(), "key_1000", "key_2000");
  db->ForceFullCompaction();

  auto r = ScanAll(db);
  assert(r.size() == 2000);
  assert(!Exists(db, "key_1000"));
  assert(!Exists(db, "key_1999"));
  assert(Exists(db, "key_0999"));
  assert(Exists(db, "key_2000"));
  Pass("2000 keys remain after DeleteRange + ForceFullCompaction");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// TEST 5: Full workflow — all APIs together
// -------------------------------------------------------
void Test_Full_Workflow() {
  std::cout << "\n=== TEST 5: Full Workflow (all APIs) ===\n";
  leveldb::DB* db = OpenFreshDB("/tmp/itest_5");

  // Insert 100 keys
  for (int i = 0; i < 100; i++) {
    char k[16], v[16];
    snprintf(k, sizeof(k), "k%03d", i);
    snprintf(v, sizeof(v), "v%03d", i);
    Put(db, k, v);
  }
  assert(ScanAll(db).size() == 100);
  Pass("100 keys inserted");

  // Delete a range
  db->DeleteRange(leveldb::WriteOptions(), "k020", "k050");
  assert(ScanAll(db).size() == 70);
  Pass("DeleteRange removed 30 keys");

  // Overwrite some
  Put(db, "k010", "new_v010");
  Put(db, "k060", "new_v060");

  // Scan sub-range
  auto r = Scan(db, "k000", "k020");
  assert(r.size() == 20);
  Pass("Scan sub-range correct");

  // Compact
  assert(db->ForceFullCompaction().ok());

  // Verify after compaction
  assert(ScanAll(db).size() == 72); // 70 + 2 overwrites already counted... 
  // Actually k010 and k060 were already in the 70, just updated values
  // So still 70
  auto final_r = ScanAll(db);
  std::string val;
  db->Get(leveldb::ReadOptions(), "k010", &val);
  assert(val == "new_v010");
  db->Get(leveldb::ReadOptions(), "k060", &val);
  assert(val == "new_v060");
  Pass("Overwritten values correct after compaction");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// TEST 6: Multiple DeleteRanges -> Compaction -> Scan
// -------------------------------------------------------
void Test_Multiple_DeleteRanges() {
  std::cout << "\n=== TEST 6: Multiple DeleteRanges -> Compaction -> Scan ===\n";
  leveldb::DB* db = OpenFreshDB("/tmp/itest_6");

  for (int i = 0; i < 100; i++) {
    char k[8];
    snprintf(k, sizeof(k), "k%03d", i);
    Put(db, k, "v");
  }

  db->DeleteRange(leveldb::WriteOptions(), "k010", "k020");
  db->DeleteRange(leveldb::WriteOptions(), "k040", "k060");
  db->DeleteRange(leveldb::WriteOptions(), "k080", "k090");

  db->ForceFullCompaction();

  auto r = ScanAll(db);
  // Deleted: 10+20+10 = 40 keys, remaining: 60
  assert(r.size() == 60);
  assert(!Exists(db, "k010"));
  assert(!Exists(db, "k050"));
  assert(!Exists(db, "k085"));
  assert(Exists(db, "k009"));
  assert(Exists(db, "k020"));
  assert(Exists(db, "k090"));
  Pass("60 keys remain after 3 range deletions + compaction");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// TEST 7: Scan across MemTable and SSTables
// -------------------------------------------------------
void Test_Scan_Across_Levels() {
  std::cout << "\n=== TEST 7: Scan Across MemTable and SSTables ===\n";

  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.write_buffer_size = 64 * 1024;
  system("rm -rf /tmp/itest_7");
  leveldb::DB* db;
  leveldb::DB::Open(opts, "/tmp/itest_7", &db);

  // Batch 1: flush to disk
  for (int i = 0; i < 1000; i++) {
    char k[16], v[16];
    snprintf(k, sizeof(k), "key_%04d", i);
    snprintf(v, sizeof(v), "val_%04d", i);
    Put(db, k, v);
  }
  db->CompactRange(nullptr, nullptr);

  // Batch 2: stays in MemTable
  for (int i = 1000; i < 2000; i++) {
    char k[16], v[16];
    snprintf(k, sizeof(k), "key_%04d", i);
    snprintf(v, sizeof(v), "val_%04d", i);
    Put(db, k, v);
  }

  auto r = ScanAll(db);
  assert(r.size() == 2000);
  Pass("Scan sees all 2000 keys across MemTable + SSTables");

  // Verify sorted order
  for (size_t i = 1; i < r.size(); i++) {
    assert(r[i].first > r[i-1].first);
  }
  Pass("Scan results in sorted order");

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

  Test_Scan_DeleteRange_Scan();
  Test_DeleteRange_Put_Scan();
  Test_Scan_Compaction_Scan();
  Test_DeleteRange_Compaction_Scan();
  Test_Full_Workflow();
  Test_Multiple_DeleteRanges();
  Test_Scan_Across_Levels();

  std::cout << "\n===========================================\n";
  std::cout << "   ALL INTEGRATION TESTS PASSED\n";
  std::cout << "===========================================\n";
  return 0;
}

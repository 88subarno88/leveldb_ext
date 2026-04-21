#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <cassert>
#include <cstdio>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"

// -------------------------------------------------------
// Helpers
// -------------------------------------------------------

leveldb::DB* OpenDB(const std::string& path) {
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(options, path, &db);
  if (!s.ok()) {
    std::cerr << "Failed to open DB: " << s.ToString() << "\n";
    exit(1);
  }
  return db;
}

// Scan helper — reuses our Scan() API
std::vector<std::pair<std::string,std::string>> ScanAll(leveldb::DB* db) {
  std::vector<std::pair<std::string,std::string>> results;
  std::string big_key(8, '\xff');
  db->Scan(leveldb::ReadOptions(), "", big_key, &results);
  return results;
}

std::vector<std::pair<std::string,std::string>> ScanRange(
    leveldb::DB* db,
    const std::string& start,
    const std::string& end) {
  std::vector<std::pair<std::string,std::string>> results;
  db->Scan(leveldb::ReadOptions(), start, end, &results);
  return results;
}

void PrintKV(const std::vector<std::pair<std::string,std::string>>& kvs) {
  if (kvs.empty()) { std::cout << "  (empty)\n"; return; }
  for (auto& kv : kvs)
    std::cout << "  [" << kv.first << "] => " << kv.second << "\n";
}

bool KeyExists(leveldb::DB* db, const std::string& key) {
  std::string val;
  return db->Get(leveldb::ReadOptions(), key, &val).ok();
}

// -------------------------------------------------------
// TEST 1: Basic DeleteRange
// Delete middle keys, check boundaries survive
// -------------------------------------------------------
void TestBasicDeleteRange() {
  std::cout << "\n=== TEST 1: Basic DeleteRange ===\n";

  system("rm -rf /tmp/testdb_dr1");
  leveldb::DB* db = OpenDB("/tmp/testdb_dr1");
  leveldb::WriteOptions wo;

  db->Put(wo, "a", "val_a");
  db->Put(wo, "b", "val_b");
  db->Put(wo, "c", "val_c");
  db->Put(wo, "d", "val_d");
  db->Put(wo, "e", "val_e");

  std::cout << "Before DeleteRange [b, d):\n";
  PrintKV(ScanAll(db));

  leveldb::Status s = db->DeleteRange(wo, "b", "d");
  assert(s.ok());

  std::cout << "\nAfter DeleteRange [b, d):\n";
  auto results = ScanAll(db);
  PrintKV(results);

  // "a" and "e" survive, "d" survives (exclusive), "b" and "c" are deleted
  assert(!KeyExists(db, "b"));
  assert(!KeyExists(db, "c"));
  assert(KeyExists(db, "a"));
  assert(KeyExists(db, "d"));
  assert(KeyExists(db, "e"));

  std::cout << "PASSED\n";
  delete db;
}

// -------------------------------------------------------
// TEST 2: DeleteRange then Scan — deleted keys must not appear
// -------------------------------------------------------
void TestDeleteRangeThenScan() {
  std::cout << "\n=== TEST 2: DeleteRange then Scan ===\n";

  system("rm -rf /tmp/testdb_dr2");
  leveldb::DB* db = OpenDB("/tmp/testdb_dr2");
  leveldb::WriteOptions wo;

  // Insert keys: key_00 to key_19
  for (int i = 0; i < 20; i++) {
    char key[16], val[16];
    snprintf(key, sizeof(key), "key_%02d", i);
    snprintf(val, sizeof(val), "val_%02d", i);
    db->Put(wo, key, val);
  }

  // Delete key_05 to key_14 (10 keys)
  leveldb::Status s = db->DeleteRange(wo, "key_05", "key_15");
  assert(s.ok());

  // Scan everything
  auto results = ScanAll(db);
  std::cout << "Remaining keys after DeleteRange [key_05, key_15):\n";
  PrintKV(results);

  // Should have key_00..key_04 and key_15..key_19 = 10 keys
  assert(results.size() == 10);
  assert(results.front().first == "key_00");
  assert(results.back().first  == "key_19");

  // Verify deleted keys are gone
  for (int i = 5; i < 15; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%02d", i);
    assert(!KeyExists(db, key));
  }

  // Verify surviving keys are still there
  for (int i = 0; i < 5; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%02d", i);
    assert(KeyExists(db, key));
  }
  for (int i = 15; i < 20; i++) {
    char key[16];
    snprintf(key, sizeof(key), "key_%02d", i);
    assert(KeyExists(db, key));
  }

  std::cout << "PASSED\n";
  delete db;
}

// -------------------------------------------------------
// TEST 3: start_key == end_key → nothing deleted
// -------------------------------------------------------
void TestEmptyRange() {
  std::cout << "\n=== TEST 3: Empty Range (start == end) ===\n";

  system("rm -rf /tmp/testdb_dr3");
  leveldb::DB* db = OpenDB("/tmp/testdb_dr3");
  leveldb::WriteOptions wo;

  db->Put(wo, "x", "val_x");
  db->Put(wo, "y", "val_y");
  db->Put(wo, "z", "val_z");

  leveldb::Status s = db->DeleteRange(wo, "y", "y");
  assert(s.ok());

  // Nothing should be deleted
  assert(KeyExists(db, "x"));
  assert(KeyExists(db, "y"));
  assert(KeyExists(db, "z"));

  std::cout << "DeleteRange [y, y) deleted nothing — correct\n";
  std::cout << "PASSED\n";
  delete db;
}

// -------------------------------------------------------
// TEST 4: DeleteRange on non-existent key range
// -------------------------------------------------------
void TestNonExistentRange() {
  std::cout << "\n=== TEST 4: DeleteRange on Non-Existent Range ===\n";

  system("rm -rf /tmp/testdb_dr4");
  leveldb::DB* db = OpenDB("/tmp/testdb_dr4");
  leveldb::WriteOptions wo;

  db->Put(wo, "apple",  "v1");
  db->Put(wo, "banana", "v2");

  // Delete a range where no keys exist
  leveldb::Status s = db->DeleteRange(wo, "mango", "zebra");
  assert(s.ok());

  // Existing keys must survive
  assert(KeyExists(db, "apple"));
  assert(KeyExists(db, "banana"));

  std::cout << "DeleteRange on empty range — existing keys unaffected\n";
  std::cout << "PASSED\n";
  delete db;
}

// -------------------------------------------------------
// TEST 5: Put after DeleteRange — new keys must be visible
// -------------------------------------------------------
void TestPutAfterDeleteRange() {
  std::cout << "\n=== TEST 5: Put After DeleteRange ===\n";

  system("rm -rf /tmp/testdb_dr5");
  leveldb::DB* db = OpenDB("/tmp/testdb_dr5");
  leveldb::WriteOptions wo;

  db->Put(wo, "k1", "old1");
  db->Put(wo, "k2", "old2");
  db->Put(wo, "k3", "old3");

  // Delete all three
  db->DeleteRange(wo, "k1", "k4");

  // Now re-insert k2 with a new value
  db->Put(wo, "k2", "new2");

  // k1 and k3 should be gone, k2 should have new value
  assert(!KeyExists(db, "k1"));
  assert(!KeyExists(db, "k3"));

  std::string val;
  leveldb::Status s = db->Get(leveldb::ReadOptions(), "k2", &val);
  assert(s.ok());
  assert(val == "new2");

  std::cout << "k1 deleted: " << (!KeyExists(db, "k1") ? "yes" : "no") << "\n";
  std::cout << "k2 value after re-insert: " << val << "\n";
  std::cout << "k3 deleted: " << (!KeyExists(db, "k3") ? "yes" : "no") << "\n";
  std::cout << "PASSED\n";
  delete db;
}

// -------------------------------------------------------
// TEST 6: DeleteRange across MemTable and SSTables
// Insert → flush → insert more → delete range spanning both
// -------------------------------------------------------
void TestDeleteRangeAcrossLevels() {
  std::cout << "\n=== TEST 6: DeleteRange Across MemTable and SSTables ===\n";

  system("rm -rf /tmp/testdb_dr6");

  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.write_buffer_size = 64 * 1024;  // 64KB — small buffer to force flush

  leveldb::DB* db;
  leveldb::DB::Open(opts, "/tmp/testdb_dr6", &db);
  leveldb::WriteOptions wo;

  // Batch 1: write enough to flush to SSTable (L0)
  for (int i = 0; i < 1000; i++) {
    char key[20], val[20];
    snprintf(key, sizeof(key), "row_%04d", i);
    snprintf(val, sizeof(val), "val_%04d", i);
    db->Put(wo, key, val);
  }

  // Force flush to disk
  db->CompactRange(nullptr, nullptr);

  // Batch 2: write more keys (these stay in MemTable initially)
  for (int i = 1000; i < 2000; i++) {
    char key[20], val[20];
    snprintf(key, sizeof(key), "row_%04d", i);
    snprintf(val, sizeof(val), "val_%04d", i);
    db->Put(wo, key, val);
  }

  std::cout << "Inserted 2000 keys, first 1000 flushed to SSTables\n";

  // Delete row_0500 to row_1500 — spans both SSTable and MemTable
  leveldb::Status s = db->DeleteRange(wo, "row_0500", "row_1500");
  assert(s.ok());

  // Force compaction to enforce the range deletion on SSTables
  db->CompactRange(nullptr, nullptr);

  // Scan to verify
  auto results = ScanAll(db);
  std::cout << "Keys remaining after DeleteRange [row_0500, row_1500):\n";
  std::cout << "  Count: " << results.size() << " (expected 1000)\n";
  if (!results.empty()) {
    std::cout << "  First: " << results.front().first << "\n";
    std::cout << "  Last:  " << results.back().first  << "\n";
  }

  // Should have row_0000..row_0499 and row_1500..row_1999 = 1000 keys
  assert(results.size() == 1000);
  assert(results.front().first == "row_0000");
  assert(results.back().first  == "row_1999");

  // Spot check: deleted key
  assert(!KeyExists(db, "row_0500"));
  assert(!KeyExists(db, "row_1000"));
  assert(!KeyExists(db, "row_1499"));

  // Spot check: surviving key
  assert(KeyExists(db, "row_0499"));
  assert(KeyExists(db, "row_1500"));

  std::cout << "PASSED\n";
  delete db;
}

// -------------------------------------------------------
// TEST 7: Multiple overlapping DeleteRange calls
// -------------------------------------------------------
void TestMultipleDeleteRanges() {
  std::cout << "\n=== TEST 7: Multiple DeleteRange Calls ===\n";

  system("rm -rf /tmp/testdb_dr7");
  leveldb::DB* db = OpenDB("/tmp/testdb_dr7");
  leveldb::WriteOptions wo;

  // Insert keys a through z (26 keys)
  for (char c = 'a'; c <= 'z'; c++) {
    std::string key(1, c);
    db->Put(wo, key, "val_" + key);
  }

  // Delete [b, f) → deletes b,c,d,e
  db->DeleteRange(wo, "b", "f");
  // Delete [m, p) → deletes m,n,o
  db->DeleteRange(wo, "m", "p");
  // Delete [x, z) → deletes x,y
  db->DeleteRange(wo, "x", "z");

  auto results = ScanAll(db);
  std::cout << "Keys remaining after 3 DeleteRange calls:\n";
  PrintKV(results);

  // Deleted: b,c,d,e,m,n,o,x,y = 9 keys
  // Remaining: 26 - 9 = 17 keys
  assert(results.size() == 17);

  // Spot checks
  assert(!KeyExists(db, "b"));
  assert(!KeyExists(db, "e"));
  assert(!KeyExists(db, "m"));
  assert(!KeyExists(db, "o"));
  assert(!KeyExists(db, "x"));
  assert(!KeyExists(db, "y"));

  assert(KeyExists(db, "a"));
  assert(KeyExists(db, "f"));  // f is start of gap, survives
  assert(KeyExists(db, "p"));  // p is end_key, survives
  assert(KeyExists(db, "z"));  // z is end_key, survives

  std::cout << "PASSED\n";
  delete db;
}

// -------------------------------------------------------
// TEST 8: DeleteRange entire database
// -------------------------------------------------------
void TestDeleteEntireDB() {
  std::cout << "\n=== TEST 8: DeleteRange Entire DB ===\n";

  system("rm -rf /tmp/testdb_dr8");
  leveldb::DB* db = OpenDB("/tmp/testdb_dr8");
  leveldb::WriteOptions wo;

  for (int i = 0; i < 100; i++) {
    char key[16], val[16];
    snprintf(key, sizeof(key), "key_%03d", i);
    snprintf(val, sizeof(val), "val_%03d", i);
    db->Put(wo, key, val);
  }

  // Delete everything
  std::string big_key(8, '\xff');
  leveldb::Status s = db->DeleteRange(wo, "", big_key);
  assert(s.ok());

  auto results = ScanAll(db);
  std::cout << "Keys after deleting entire DB: " << results.size() << "\n";
  assert(results.empty());

  std::cout << "PASSED\n";
  delete db;
}

// -------------------------------------------------------
// MAIN
// -------------------------------------------------------
int main() {
  std::cout << "===================================\n";
  std::cout << " Range Delete Test Suite\n";
  std::cout << "===================================\n";

  TestBasicDeleteRange();
  TestDeleteRangeThenScan();
  TestEmptyRange();
  TestNonExistentRange();
  TestPutAfterDeleteRange();
  TestDeleteRangeAcrossLevels();
  TestMultipleDeleteRanges();
  TestDeleteEntireDB();

  std::cout << "\n===================================\n";
  std::cout << " ALL DELETE RANGE TESTS PASSED\n";
  std::cout << "===================================\n";
  return 0;
}
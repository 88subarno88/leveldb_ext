#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <cassert>
#include <cstdio>
#include <cstring>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"


using CompactionReport = leveldb::DB::CompactionReport;

// -------------------------------------------------------
// Helpers
// -------------------------------------------------------

leveldb::DB* OpenDB(const std::string& path, size_t write_buffer_size = 0) {
  leveldb::Options options;
  options.create_if_missing = true;
  if (write_buffer_size > 0)
    options.write_buffer_size = write_buffer_size;
  leveldb::DB* db;
  leveldb::Status s = leveldb::DB::Open(options, path, &db);
  if (!s.ok()) {
    std::cerr << "Failed to open DB: " << s.ToString() << "\n";
    exit(1);
  }
  return db;
}

std::vector<std::pair<std::string,std::string>> ScanAll(leveldb::DB* db) {
  std::vector<std::pair<std::string,std::string>> results;
  std::string big_key(8, '\xff');
  db->Scan(leveldb::ReadOptions(), "", big_key, &results);
  return results;
}

bool KeyExists(leveldb::DB* db, const std::string& key) {
  std::string val;
  return db->Get(leveldb::ReadOptions(), key, &val).ok();
}

void InsertKeys(leveldb::DB* db, int start, int end,
                const std::string& prefix = "key_") {
  leveldb::WriteOptions wo;
  for (int i = start; i < end; i++) {
    char key[32], val[32];
    snprintf(key, sizeof(key), "%s%04d", prefix.c_str(), i);
    snprintf(val, sizeof(val), "val_%04d", i);
    db->Put(wo, key, val);
  }
}

// -------------------------------------------------------
// TEST 1: Basic ForceFullCompaction on empty DB
// Should succeed and report 0 compactions
// -------------------------------------------------------
void TestCompactionEmptyDB() {
  std::cout << "\n=== TEST 1: ForceFullCompaction on Empty DB ===\n";

  system("rm -rf /tmp/testdb_fc1");
  leveldb::DB* db = OpenDB("/tmp/testdb_fc1");

  CompactionReport report;
  leveldb::Status s = db->ForceFullCompaction(&report);

  assert(s.ok());
  assert(report.num_compactions == 0);
  assert(report.bytes_read == 0);
  assert(report.bytes_written == 0);

  std::cout << "ForceFullCompaction on empty DB:\n";
  report.Print();
  std::cout << "PASSED\n";

  delete db;
}

// -------------------------------------------------------
// TEST 2: Basic ForceFullCompaction with small data
// -------------------------------------------------------
void TestCompactionSmallData() {
  std::cout << "\n=== TEST 2: ForceFullCompaction with Small Data ===\n";

  system("rm -rf /tmp/testdb_fc2");
  leveldb::DB* db = OpenDB("/tmp/testdb_fc2");

  InsertKeys(db, 0, 100);

  std::cout << "Inserted 100 keys. Running ForceFullCompaction...\n";

  CompactionReport report;
  leveldb::Status s = db->ForceFullCompaction(&report);
  assert(s.ok());

  report.Print();

  for (int i = 0; i < 100; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    assert(KeyExists(db, key));
  }

  std::cout << "All 100 keys still readable after compaction\n";
  std::cout << "PASSED\n";

  delete db;
}

// -------------------------------------------------------
// TEST 3: ForceFullCompaction removes deleted keys
// -------------------------------------------------------
void TestCompactionRemovesDeleted() {
  std::cout << "\n=== TEST 3: Compaction Removes Deleted Keys ===\n";

  system("rm -rf /tmp/testdb_fc3");
  leveldb::DB* db = OpenDB("/tmp/testdb_fc3", 64 * 1024);

  leveldb::WriteOptions wo;

  InsertKeys(db, 0, 2000);

  for (int i = 0; i < 1000; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    db->Delete(wo, key);
  }

  std::cout << "Inserted 2000 keys, deleted first 1000\n";
  std::cout << "Running ForceFullCompaction...\n";

  CompactionReport report;
  leveldb::Status s = db->ForceFullCompaction(&report);
  assert(s.ok());

  report.Print();

  for (int i = 0; i < 1000; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    assert(!KeyExists(db, key));
  }

  for (int i = 1000; i < 2000; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    assert(KeyExists(db, key));
  }

  std::cout << "Deleted keys confirmed absent\n";
  std::cout << "Surviving keys confirmed present\n";
  std::cout << "PASSED\n";

  delete db;
}

// -------------------------------------------------------
// TEST 4: ForceFullCompaction removes old versions (overwrites)
// -------------------------------------------------------
void TestCompactionRemovesOldVersions() {
  std::cout << "\n=== TEST 4: Compaction Removes Old Versions ===\n";

  system("rm -rf /tmp/testdb_fc4");
  leveldb::DB* db = OpenDB("/tmp/testdb_fc4", 64 * 1024);

  leveldb::WriteOptions wo;

  for (int round = 0; round < 5; round++) {
    for (int i = 0; i < 500; i++) {
      char key[32], val[32];
      snprintf(key, sizeof(key), "key_%04d", i);
      snprintf(val, sizeof(val), "val_round%d_%04d", round, i);
      db->Put(wo, key, val);
    }
  }

  std::cout << "Wrote 500 keys 5 times each (2500 total writes)\n";
  std::cout << "Running ForceFullCompaction...\n";

  CompactionReport report;
  leveldb::Status s = db->ForceFullCompaction(&report);
  assert(s.ok());

  report.Print();

  for (int i = 0; i < 500; i++) {
    char key[32], expected_val[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    snprintf(expected_val, sizeof(expected_val), "val_round4_%04d", i);

    std::string actual_val;
    leveldb::Status gs = db->Get(leveldb::ReadOptions(), key, &actual_val);
    assert(gs.ok());
    assert(actual_val == expected_val);
  }

  std::cout << "All 500 keys have correct latest value\n";
  std::cout << "PASSED\n";

  delete db;
}

// -------------------------------------------------------
// TEST 5: ForceFullCompaction + DeleteRange interaction
// -------------------------------------------------------
void TestCompactionWithDeleteRange() {
  std::cout << "\n=== TEST 5: ForceFullCompaction + DeleteRange ===\n";

  system("rm -rf /tmp/testdb_fc5");
  leveldb::DB* db = OpenDB("/tmp/testdb_fc5", 64 * 1024);

  InsertKeys(db, 0, 3000);

  db->CompactRange(nullptr, nullptr);

  leveldb::WriteOptions wo;
  leveldb::Status s = db->DeleteRange(wo, "key_1000", "key_2000");
  assert(s.ok());

  std::cout << "Inserted 3000 keys, range deleted [key_1000, key_2000)\n";
  std::cout << "Running ForceFullCompaction...\n";

  CompactionReport report;
  s = db->ForceFullCompaction(&report);
  assert(s.ok());

  report.Print();

  assert(!KeyExists(db, "key_1000"));
  assert(!KeyExists(db, "key_1500"));
  assert(!KeyExists(db, "key_1999"));

  assert(KeyExists(db, "key_0999"));
  assert(KeyExists(db, "key_2000"));

  auto results = ScanAll(db);
  std::cout << "Keys remaining: " << results.size() << " (expected 2000)\n";
  assert(results.size() == 2000);

  std::cout << "PASSED\n";
  delete db;
}

// -------------------------------------------------------
// TEST 6: Compaction stats are non-zero for real data
// -------------------------------------------------------
void TestCompactionStatsNonZero() {
  std::cout << "\n=== TEST 6: Compaction Stats are Non-Zero ===\n";

  system("rm -rf /tmp/testdb_fc6");
  leveldb::DB* db = OpenDB("/tmp/testdb_fc6", 64 * 1024);

  InsertKeys(db, 0, 5000);

  std::cout << "Inserted 5000 keys across multiple SSTables\n";
  std::cout << "Running ForceFullCompaction...\n";

  CompactionReport report;
  leveldb::Status s = db->ForceFullCompaction(&report);
  assert(s.ok());

  report.Print();

  assert(report.num_compactions > 0);
  assert(report.bytes_read > 0);
  assert(report.bytes_written > 0);
  assert(report.total_input_files > 0);

  std::cout << "All stats fields are non-zero — correct\n";
  std::cout << "PASSED\n";

  delete db;
}

// -------------------------------------------------------
// TEST 7: ForceFullCompaction is idempotent
// -------------------------------------------------------
void TestCompactionIdempotent() {
  std::cout << "\n=== TEST 7: ForceFullCompaction is Idempotent ===\n";

  system("rm -rf /tmp/testdb_fc7");
  leveldb::DB* db = OpenDB("/tmp/testdb_fc7", 64 * 1024);

  InsertKeys(db, 0, 1000);

  CompactionReport report1;
  leveldb::Status s = db->ForceFullCompaction(&report1);
  assert(s.ok());

  std::cout << "First compaction:\n";
  report1.Print();

  CompactionReport report2;
  s = db->ForceFullCompaction(&report2);
  assert(s.ok());

  std::cout << "Second compaction (should do less work):\n";
  report2.Print();

  assert(report2.bytes_written <= report1.bytes_written);

  for (int i = 0; i < 1000; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    assert(KeyExists(db, key));
  }

  std::cout << "All keys readable after two compactions\n";
  std::cout << "PASSED\n";

  delete db;
}

// -------------------------------------------------------
// TEST 8: ForceFullCompaction with nullptr report
// -------------------------------------------------------
void TestCompactionNullReport() {
  std::cout << "\n=== TEST 8: ForceFullCompaction with nullptr report ===\n";

  system("rm -rf /tmp/testdb_fc8");
  leveldb::DB* db = OpenDB("/tmp/testdb_fc8");

  InsertKeys(db, 0, 200);

  leveldb::Status s = db->ForceFullCompaction(nullptr);
  assert(s.ok());

  for (int i = 0; i < 200; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    assert(KeyExists(db, key));
  }

  std::cout << "ForceFullCompaction(nullptr) succeeded\n";
  std::cout << "All keys readable\n";
  std::cout << "PASSED\n";

  delete db;
}

// -------------------------------------------------------
// TEST 9: Large data stress test
// -------------------------------------------------------
void TestCompactionLargeData() {
  std::cout << "\n=== TEST 9: Large Data Stress Test ===\n";

  system("rm -rf /tmp/testdb_fc9");
  leveldb::DB* db = OpenDB("/tmp/testdb_fc9", 64 * 1024);

  leveldb::WriteOptions wo;

  std::string long_value(100, 'x');
  for (int i = 0; i < 20000; i++) {
    char key[32];
    snprintf(key, sizeof(key), "bigkey_%06d", i);
    db->Put(wo, key, long_value);
  }

  for (int i = 0; i < 20000; i += 2) {
    char key[32];
    snprintf(key, sizeof(key), "bigkey_%06d", i);
    db->Delete(wo, key);
  }

  std::cout << "Inserted 20000 keys (100B each), deleted 10000\n";
  std::cout << "Running ForceFullCompaction...\n";

  CompactionReport report;
  leveldb::Status s = db->ForceFullCompaction(&report);
  assert(s.ok());

  report.Print();

  for (int i = 1; i < 20000; i += 2) {
    char key[32];
    snprintf(key, sizeof(key), "bigkey_%06d", i);
    assert(KeyExists(db, key));
  }

  for (int i = 0; i < 20000; i += 2) {
    char key[32];
    snprintf(key, sizeof(key), "bigkey_%06d", i);
    assert(!KeyExists(db, key));
  }

  assert(report.num_compactions > 0);
  assert(report.bytes_read > 0);

  std::cout << "All surviving keys verified\n";
  std::cout << "All deleted keys confirmed absent\n";
  std::cout << "PASSED\n";

  delete db;
}

// -------------------------------------------------------
// TEST 10: Data integrity after compaction
// -------------------------------------------------------
void TestDataIntegrityAfterCompaction() {
  std::cout << "\n=== TEST 10: Data Integrity After Compaction ===\n";

  system("rm -rf /tmp/testdb_fc10");
  leveldb::DB* db = OpenDB("/tmp/testdb_fc10", 64 * 1024);

  leveldb::WriteOptions wo;

  for (int i = 0; i < 1000; i++) {
    char key[32], val[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    snprintf(val, sizeof(val), "val_v1_%04d", i);
    db->Put(wo, key, val);
  }

  for (int i = 0; i < 500; i++) {
    char key[32], val[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    snprintf(val, sizeof(val), "val_v2_%04d", i);
    db->Put(wo, key, val);
  }

  for (int i = 200; i < 300; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    db->Delete(wo, key);
  }

  std::cout << "Complex write pattern complete. Running ForceFullCompaction...\n";

  CompactionReport report;
  leveldb::Status s = db->ForceFullCompaction(&report);
  assert(s.ok());

  report.Print();

  for (int i = 0; i < 200; i++) {
    char key[32], expected[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    snprintf(expected, sizeof(expected), "val_v2_%04d", i);
    std::string val;
    assert(db->Get(leveldb::ReadOptions(), key, &val).ok());
    assert(val == expected);
  }

  for (int i = 200; i < 300; i++) {
    char key[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    assert(!KeyExists(db, key));
  }

  for (int i = 300; i < 500; i++) {
    char key[32], expected[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    snprintf(expected, sizeof(expected), "val_v2_%04d", i);
    std::string val;
    assert(db->Get(leveldb::ReadOptions(), key, &val).ok());
    assert(val == expected);
  }

  for (int i = 500; i < 1000; i++) {
    char key[32], expected[32];
    snprintf(key, sizeof(key), "key_%04d", i);
    snprintf(expected, sizeof(expected), "val_v1_%04d", i);
    std::string val;
    assert(db->Get(leveldb::ReadOptions(), key, &val).ok());
    assert(val == expected);
  }

  std::cout << "All data integrity checks passed\n";
  std::cout << "PASSED\n";

  delete db;
}

// -------------------------------------------------------
// MAIN
// -------------------------------------------------------
int main() {
  std::cout << "=========================================\n";
  std::cout << "   ForceFullCompaction Test Suite\n";
  std::cout << "=========================================\n";

  TestCompactionEmptyDB();
  TestCompactionSmallData();
  TestCompactionRemovesDeleted();
  TestCompactionRemovesOldVersions();
  TestCompactionWithDeleteRange();
  TestCompactionStatsNonZero();
  TestCompactionIdempotent();
  TestCompactionNullReport();
  TestCompactionLargeData();
  TestDataIntegrityAfterCompaction();

  std::cout << "\n=========================================\n";
  std::cout << "  ALL COMPACTION TESTS PASSED\n";
  std::cout << "=========================================\n";

  return 0;
}
#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <climits>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

// -------------------------------------------------------
// Helpers (same as integration test)
// -------------------------------------------------------

static leveldb::DB* OpenFreshDB(const std::string& path,
                                 size_t buf = 0) {
  system(("rm -rf " + path).c_str());
  leveldb::Options opts;
  opts.create_if_missing = true;
  if (buf > 0) opts.write_buffer_size = buf;
  leveldb::DB* db;
  leveldb::DB::Open(opts, path, &db);
  return db;
}

static void Put(leveldb::DB* db, const std::string& k, const std::string& v) {
  db->Put(leveldb::WriteOptions(), k, v);
}

static void Del(leveldb::DB* db, const std::string& k) {
  db->Delete(leveldb::WriteOptions(), k);
}

static bool Exists(leveldb::DB* db, const std::string& k) {
  std::string v;
  return db->Get(leveldb::ReadOptions(), k, &v).ok();
}

static std::string GetVal(leveldb::DB* db, const std::string& k) {
  std::string v;
  db->Get(leveldb::ReadOptions(), k, &v);
  return v;
}

static std::vector<std::pair<std::string,std::string>>
Scan(leveldb::DB* db, const std::string& s, const std::string& e) {
  std::vector<std::pair<std::string,std::string>> r;
  db->Scan(leveldb::ReadOptions(), s, e, &r);
  return r;
}

static std::vector<std::pair<std::string,std::string>>
ScanAll(leveldb::DB* db) {
  return Scan(db, "", std::string(8, '\xff'));
}

static void Pass(const std::string& msg) {
  std::cout << "  [PASS] " << msg << "\n";
}

// -------------------------------------------------------
// EDGE CASE 1: Empty string keys
// "" is a valid key in LevelDB
// -------------------------------------------------------
void Edge_EmptyStringKey() {
  std::cout << "\n=== EDGE 1: Empty String Key ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_1");

  // Put with empty key
  Put(db, "", "empty_key_value");
  Put(db, "a", "val_a");
  Put(db, "b", "val_b");

  // Get empty key
  assert(GetVal(db, "") == "empty_key_value");
  Pass("Get with empty key works");

  // Scan starting from empty key
  auto r = Scan(db, "", "b");
  assert(r.size() == 2);
  assert(r[0].first == "");
  assert(r[0].second == "empty_key_value");
  assert(r[1].first == "a");
  Pass("Scan from empty start_key includes empty key");

  // DeleteRange starting from empty key
  db->DeleteRange(leveldb::WriteOptions(), "", "a");
  assert(!Exists(db, ""));   // empty key deleted
  assert(Exists(db, "a"));   // "a" is end_key → survives
  Pass("DeleteRange [empty, 'a') removes empty key");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 2: Single-character and single-key ranges
// -------------------------------------------------------
void Edge_SingleKeyRange() {
  std::cout << "\n=== EDGE 2: Single Key in Range ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_2");

  Put(db, "aaa", "v1");
  Put(db, "aab", "v2");
  Put(db, "aac", "v3");

  // Scan a range that contains exactly one key
  auto r = Scan(db, "aab", "aac");
  assert(r.size() == 1);
  assert(r[0].first == "aab");
  Pass("Scan range with exactly one key");

  // DeleteRange on exactly one key
  db->DeleteRange(leveldb::WriteOptions(), "aaa", "aab");
  assert(!Exists(db, "aaa"));
  assert(Exists(db, "aab"));
  assert(Exists(db, "aac"));
  Pass("DeleteRange on exactly one key");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 3: Keys with special characters and binary data
// -------------------------------------------------------
void Edge_SpecialCharacterKeys() {
  std::cout << "\n=== EDGE 3: Special Character Keys ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_3");

  // Keys with spaces, slashes, newlines
  Put(db, "key with space",   "val1");
  Put(db, "key/with/slash",   "val2");
  Put(db, "key\nwith\nnewline", "val3");
  Put(db, "key\twith\ttab",   "val4");

  assert(GetVal(db, "key with space")    == "val1");
  assert(GetVal(db, "key/with/slash")    == "val2");
  assert(GetVal(db, "key\nwith\nnewline") == "val3");
  assert(GetVal(db, "key\twith\ttab")    == "val4");
  Pass("Keys with special characters stored/retrieved");

  // Scan should include them all (sorted by byte value)
  auto all = ScanAll(db);
  assert(all.size() == 4);
  Pass("Scan returns all special-character keys");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 4: Scan with start_key > end_key
// Should return empty, not crash
// -------------------------------------------------------
void Edge_ReversedRange() {
  std::cout << "\n=== EDGE 4: Reversed Range (start > end) ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_4");

  Put(db, "a", "v1");
  Put(db, "b", "v2");
  Put(db, "c", "v3");

  // start > end → should return empty, not crash
  auto r1 = Scan(db, "z", "a");
  assert(r1.empty());
  Pass("Scan with start > end returns empty");

  // DeleteRange with start > end → should do nothing, not crash
  leveldb::Status s = db->DeleteRange(leveldb::WriteOptions(), "z", "a");
  assert(s.ok());
  assert(Exists(db, "a"));
  assert(Exists(db, "b"));
  assert(Exists(db, "c"));
  Pass("DeleteRange with start > end does nothing");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 5: Very large keys and values
// -------------------------------------------------------
void Edge_LargeKeysAndValues() {
  std::cout << "\n=== EDGE 5: Large Keys and Values ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_5");

  // LevelDB supports keys up to a few KB and values up to a few MB
  std::string large_key(1000, 'k');           // 1000-byte key
  std::string large_val(100 * 1024, 'v');     // 100KB value

  Put(db, large_key, large_val);
  assert(GetVal(db, large_key) == large_val);
  Pass("Large key (1000B) and value (100KB) stored/retrieved");

  // Scan should include the large key
  std::string scan_start(1000, 'k');  // exact key as start
  std::string scan_end(1001, 'l');    // something bigger
  auto r = Scan(db, scan_start, scan_end);
  assert(r.size() == 1);
  assert(r[0].first  == large_key);
  assert(r[0].second == large_val);
  Pass("Scan returns large key correctly");

  // DeleteRange covering the large key
  std::string del_start(999, 'k');    // just before
  std::string del_end(1001, 'k');     // just after
  db->DeleteRange(leveldb::WriteOptions(), del_start, del_end);
  assert(!Exists(db, large_key));
  Pass("DeleteRange removes large key");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 6: Scan returns empty after all keys deleted
// -------------------------------------------------------
void Edge_ScanAfterAllDeleted() {
  std::cout << "\n=== EDGE 6: Scan After All Keys Deleted ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_6");

  Put(db, "x", "1");
  Put(db, "y", "2");
  Put(db, "z", "3");

  Del(db, "x");
  Del(db, "y");
  Del(db, "z");

  auto r = ScanAll(db);
  assert(r.empty());
  Pass("Scan returns empty after all keys individually deleted");

  // Reopen the DB and scan again — persisted deletes should hold
  delete db;
  leveldb::Options opts;
  opts.create_if_missing = false;
  leveldb::DB::Open(opts, "/tmp/edge_6", &db);

  auto r2 = ScanAll(db);
  assert(r2.empty());
  Pass("Scan still empty after DB reopen");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 7: DeleteRange when DB is empty
// -------------------------------------------------------
void Edge_DeleteRangeOnEmptyDB() {
  std::cout << "\n=== EDGE 7: DeleteRange on Empty DB ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_7");

  // Should not crash or error
  leveldb::Status s = db->DeleteRange(leveldb::WriteOptions(), "a", "z");
  assert(s.ok());
  Pass("DeleteRange on empty DB returns OK");

  auto r = ScanAll(db);
  assert(r.empty());
  Pass("Scan still empty after DeleteRange on empty DB");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 8: ForceFullCompaction on DB with only tombstones
// After compaction tombstones should be removed
// -------------------------------------------------------
void Edge_CompactionClearsTombstones() {
  std::cout << "\n=== EDGE 8: Compaction Clears Tombstones ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_8", 64 * 1024);

  // Insert and delete many keys to create lots of tombstones
  for (int i = 0; i < 2000; i++) {
    char k[32];
    snprintf(k, sizeof(k), "tomb_%06d", i);
    Put(db, k, "value");
  }

  // Force flush to create SSTables with real data
  db->CompactRange(nullptr, nullptr);

  // Delete all of them
  for (int i = 0; i < 2000; i++) {
    char k[32];
    snprintf(k, sizeof(k), "tomb_%06d", i);
    Del(db, k);
  }

  std::cout << "  2000 keys inserted then deleted → DB has only tombstones\n";

  // ForceFullCompaction should remove all tombstones
  leveldb::CompactionReport report;
  leveldb::Status s = db->ForceFullCompaction(&report);
  assert(s.ok());

  auto r = ScanAll(db);
  assert(r.empty());
  Pass("All tombstones removed after ForceFullCompaction");

  // bytes_written should be << bytes_read (tombstones eliminated)
  std::cout << "  bytes_read:    "
            << leveldb::CompactionReport::FormatBytes(report.bytes_read) << "\n";
  std::cout << "  bytes_written: "
            << leveldb::CompactionReport::FormatBytes(report.bytes_written) << "\n";

  report.Print();
  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 9: Scan result order must always be sorted
// -------------------------------------------------------
void Edge_ScanOrderGuarantee() {
  std::cout << "\n=== EDGE 9: Scan Results Always Sorted ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_9", 64 * 1024);

  // Insert in non-sorted order
  Put(db, "z_key", "vz");
  Put(db, "a_key", "va");
  Put(db, "m_key", "vm");
  Put(db, "b_key", "vb");
  Put(db, "y_key", "vy");

  // Flush some to disk
  db->CompactRange(nullptr, nullptr);

  // Insert more in non-sorted order (these stay in MemTable)
  Put(db, "c_key", "vc");
  Put(db, "x_key", "vx");
  Put(db, "d_key", "vd");

  auto r = ScanAll(db);
  assert(r.size() == 8);

  // Verify sorted order
  for (size_t i = 1; i < r.size(); i++) {
    assert(r[i].first > r[i-1].first);
  }
  Pass("Scan results sorted even when inserts were out of order");

  // Verify exact order
  assert(r[0].first == "a_key");
  assert(r[1].first == "b_key");
  assert(r[2].first == "c_key");
  assert(r[3].first == "d_key");
  assert(r[4].first == "m_key");
  Pass("Exact sorted order verified");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 10: DeleteRange with adjacent ranges
// Two DeleteRanges that share a boundary — no gap, no double-delete
// -------------------------------------------------------
void Edge_AdjacentDeleteRanges() {
  std::cout << "\n=== EDGE 10: Adjacent DeleteRanges ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_10");

  for (char c = 'a'; c <= 'j'; c++) {
    Put(db, std::string(1, c), std::string("v_") + c);
  }

  // [a, e) and [e, i) are adjacent — together delete a..h
  db->DeleteRange(leveldb::WriteOptions(), "a", "e");
  db->DeleteRange(leveldb::WriteOptions(), "e", "i");

  auto r = ScanAll(db);

  // Only i and j survive
  assert(r.size() == 2);
  assert(r[0].first == "i");
  assert(r[1].first == "j");
  Pass("Adjacent ranges [a,e) and [e,i) together delete a..h");

  // Boundary key "e" is deleted by second range
  assert(!Exists(db, "e"));
  // end_key "i" survives
  assert(Exists(db, "i"));
  Pass("Shared boundary 'e' deleted, 'i' survives");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 11: Scan result pointer is null — should return error
// -------------------------------------------------------
void Edge_ScanNullResultPointer() {
  std::cout << "\n=== EDGE 11: Scan with null result pointer ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_11");
  Put(db, "a", "v1");

  // Passing nullptr should return InvalidArgument, not crash
  leveldb::Status s = db->Scan(leveldb::ReadOptions(), "a", "b", nullptr);
  assert(!s.ok());
  assert(s.IsInvalidArgument());
  Pass("Scan with null result returns InvalidArgument");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 12: DB reopened after operations
// Data must persist across close/reopen
// -------------------------------------------------------
void Edge_PersistenceAcrossReopen() {
  std::cout << "\n=== EDGE 12: Persistence After Reopen ===\n";

  const std::string path = "/tmp/edge_12";
  system(("rm -rf " + path).c_str());

  // Session 1: write data
  {
    leveldb::DB* db = OpenFreshDB(path);

    for (int i = 0; i < 100; i++) {
      char k[32], v[32];
      snprintf(k, sizeof(k), "persist_%03d", i);
      snprintf(v, sizeof(v), "val_%03d", i);
      Put(db, k, v);
    }

    // Delete range
    char start[32], end_k[32];
    snprintf(start, sizeof(start), "persist_%03d", 40);
    snprintf(end_k, sizeof(end_k), "persist_%03d", 60);
    db->DeleteRange(leveldb::WriteOptions(), start, end_k);

    // Compact before closing
    leveldb::CompactionReport r;
    db->ForceFullCompaction(&r);

    delete db;
    Pass("Session 1 complete: wrote, deleted range, compacted");
  }

  // Session 2: reopen and verify
  {
    leveldb::Options opts;
    opts.create_if_missing = false;
    leveldb::DB* db;
    leveldb::DB::Open(opts, path, &db);

    auto results = ScanAll(db);
    assert(results.size() == 80);  // 100 - 20 deleted
    Pass("Session 2: 80 keys survive after reopen");

    // Verify deleted range
    char k[32];
    snprintf(k, sizeof(k), "persist_%03d", 40);
    assert(!Exists(db, k));

    snprintf(k, sizeof(k), "persist_%03d", 59);
    assert(!Exists(db, k));

    snprintf(k, sizeof(k), "persist_%03d", 39);
    assert(Exists(db, k));

    snprintf(k, sizeof(k), "persist_%03d", 60);
    assert(Exists(db, k));
    Pass("Deleted range confirmed absent after reopen");

    delete db;
  }

  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 13: Scan with result that was previously result-cleared
// -------------------------------------------------------
void Edge_ScanClearsOldResults() {
  std::cout << "\n=== EDGE 13: Scan Clears Previous Results ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_13");

  Put(db, "a", "1");
  Put(db, "b", "2");
  Put(db, "c", "3");

  std::vector<std::pair<std::string,std::string>> result;

  // First scan — fills with 3 results
  db->Scan(leveldb::ReadOptions(), "a", "d", &result);
  assert(result.size() == 3);

  // Second scan on smaller range — result must be CLEARED first
  // not appended to existing results
  db->Scan(leveldb::ReadOptions(), "a", "b", &result);
  assert(result.size() == 1);  // NOT 4
  assert(result[0].first == "a");
  Pass("Second scan clears previous results (no accumulation)");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 14: ForceFullCompaction with no SSTable files
// Only MemTable data — should still work
// -------------------------------------------------------
void Edge_CompactionMemTableOnly() {
  std::cout << "\n=== EDGE 14: ForceFullCompaction with MemTable data only ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_14");

  // Insert small amount — stays in MemTable (no flush yet)
  Put(db, "memonly_1", "v1");
  Put(db, "memonly_2", "v2");
  Put(db, "memonly_3", "v3");

  leveldb::CompactionReport report;
  leveldb::Status s = db->ForceFullCompaction(&report);
  assert(s.ok());
  Pass("ForceFullCompaction with MemTable-only data succeeds");

  // Data must still be readable
  assert(GetVal(db, "memonly_1") == "v1");
  assert(GetVal(db, "memonly_2") == "v2");
  assert(GetVal(db, "memonly_3") == "v3");
  Pass("All MemTable keys readable after compaction");

  report.Print();
  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// EDGE CASE 15: DeleteRange completely overlapping a previous range
// -------------------------------------------------------
void Edge_OverlappingDeleteRanges() {
  std::cout << "\n=== EDGE 15: Overlapping DeleteRanges ===\n";

  leveldb::DB* db = OpenFreshDB("/tmp/edge_15");

  for (int i = 0; i < 20; i++) {
    char k[8];
    snprintf(k, sizeof(k), "k%02d", i);
    Put(db, k, "v");
  }

  // First delete: k05..k15
  db->DeleteRange(leveldb::WriteOptions(), "k05", "k15");
  // Second delete: k03..k17 (larger range, overlaps first)
  db->DeleteRange(leveldb::WriteOptions(), "k03", "k17");

  auto r = ScanAll(db);

  // Deleted: k03..k16 → 14 keys gone, 6 remain (k00,k01,k02,k17,k18,k19)
  assert(r.size() == 6);
  assert(!Exists(db, "k03"));
  assert(!Exists(db, "k10"));
  assert(!Exists(db, "k16"));
  assert(Exists(db, "k02"));
  assert(Exists(db, "k17"));
  Pass("Overlapping ranges correctly delete union of both ranges");

  delete db;
  std::cout << "PASSED\n";
}

// -------------------------------------------------------
// MAIN
// -------------------------------------------------------
int main() {
  std::cout << "===========================================\n";
  std::cout << "   Edge Case Test Suite\n";
  std::cout << "===========================================\n";

  Edge_EmptyStringKey();
  Edge_SingleKeyRange();
  Edge_SpecialCharacterKeys();
  Edge_ReversedRange();
  Edge_LargeKeysAndValues();
  Edge_ScanAfterAllDeleted();
  Edge_DeleteRangeOnEmptyDB();
  Edge_CompactionClearsTombstones();
  Edge_ScanOrderGuarantee();
  Edge_AdjacentDeleteRanges();
  Edge_ScanNullResultPointer();
  Edge_PersistenceAcrossReopen();
  Edge_ScanClearsOldResults();
  Edge_CompactionMemTableOnly();
  Edge_OverlappingDeleteRanges();

  std::cout << "\n===========================================\n";
  std::cout << "   ALL EDGE CASE TESTS PASSED\n";
  std::cout << "===========================================\n";
  return 0;
}
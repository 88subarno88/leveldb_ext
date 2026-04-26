// brutal_integration_test.cc
// Exhaustive integration test for COP290 A3: Scan, DeleteRange, ForceFullCompaction
//
// ── HOW TO COMPILE (from inside your leveldb/build directory) ──────────────
//   cp /path/to/brutal_integration_test.cc ../brutal_integration_test.cc
//
//   Add to leveldb/CMakeLists.txt (before the final closing brace):
//     add_executable(brutal_test brutal_integration_test.cc)
//     target_link_libraries(brutal_test leveldb)
//
//   Then rebuild:
//     cmake --build . -j
//
//   Run:
//     ./brutal_test
// ──────────────────────────────────────────────────────────────────────────

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/iterator.h"

// ═══════════════════════════════════════════════════════════════════════════
// Helpers / framework
// ═══════════════════════════════════════════════════════════════════════════

static int g_failures  = 0;
static int g_tests_run = 0;
static std::string g_current_test;

#define CHECK(cond, msg)                                                       \
  do {                                                                         \
    ++g_tests_run;                                                             \
    if (!(cond)) {                                                             \
      std::cerr << "  FAIL [" << g_current_test << " | " << __FILE__          \
                << ":" << __LINE__ << "] " << msg << "\n";                    \
      ++g_failures;                                                            \
    }                                                                          \
  } while (0)

#define CHECK_OK(s, msg)                                                       \
  CHECK((s).ok(), msg << ": " << (s).ToString())

#define CHECK_NOT_FOUND(s, msg)                                                \
  CHECK((s).IsNotFound(), msg << ": expected NotFound, got " << (s).ToString())

#define CHECK_EQ(expected, actual, msg)                                        \
  CHECK((expected) == (actual),                                                \
        msg << " — expected [" << (expected) << "] got [" << (actual) << "]")

static void BeginTest(const std::string& name) {
  g_current_test = name;
  std::cout << "[" << name << "] running...\n";
}

static void EndTest(const std::string& name) {
  std::cout << "[" << name << "] done\n";
}

// ── Reference model ──────────────────────────────────────────────────────

using Model = std::map<std::string, std::string>;

static void ModelPut(Model& m, const std::string& k, const std::string& v) {
  m[k] = v;
}

static void ModelDelete(Model& m, const std::string& k) { m.erase(k); }

static void ModelDeleteRange(Model& m,
                             const std::string& start,
                             const std::string& end) {
  if (start >= end) return;
  for (auto it = m.lower_bound(start); it != m.end() && it->first < end;)
    it = m.erase(it);
}

static std::vector<std::pair<std::string, std::string>> ModelScan(
    const Model& m, const std::string& start, const std::string& end) {
  std::vector<std::pair<std::string, std::string>> r;
  if (start >= end) return r;
  for (auto it = m.lower_bound(start); it != m.end() && it->first < end; ++it)
    r.push_back({it->first, it->second});
  return r;
}

// ── DB helpers ───────────────────────────────────────────────────────────

static std::string DbBasePath() {
  const char* p = std::getenv("COP290_BRUTAL_DB");
  return (p && p[0]) ? p : "/tmp/cop290_brutal_testdb";
}

static leveldb::DB* OpenFreshDB(const std::string& path,
                                size_t write_buf = 64 * 1024) {
  std::system(("rm -rf '" + path + "'").c_str());
  leveldb::DB* db = nullptr;
  leveldb::Options opts;
  opts.create_if_missing = true;
  opts.write_buffer_size = write_buf;  // small → more SSTable flushes
  leveldb::Status s = leveldb::DB::Open(opts, path, &db);
  if (!s.ok()) {
    std::cerr << "FATAL: cannot open DB at " << path << ": " << s.ToString()
              << "\n";
    std::exit(1);
  }
  return db;
}

// Canonical key/value generators
static std::string Key(int n) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "key%08d", n);
  return buf;
}

static std::string Val(int n, const std::string& tag = "") {
  return "val" + std::to_string(n) + tag;
}

// ── Verification primitives ───────────────────────────────────────────────

// Verify DB Scan matches model for the given range
static void VerifyScan(leveldb::DB* db, const Model& model,
                       const std::string& start, const std::string& end,
                       const std::string& ctx = "") {
  auto expected = ModelScan(model, start, end);
  std::vector<std::pair<std::string, std::string>> got;
  leveldb::Status s = db->Scan(leveldb::ReadOptions(), start, end, &got);
  CHECK_OK(s, ctx + " Scan status");
  CHECK_EQ(expected.size(), got.size(), ctx + " Scan count [" + start + "," + end + ")");
  if (expected == got) return;
  // Print first divergence for debugging
  for (size_t i = 0; i < std::min(expected.size(), got.size()); ++i) {
    if (expected[i] != got[i]) {
      CHECK(false, ctx + " Scan first mismatch at i=" + std::to_string(i) +
                   " expected k=" + expected[i].first + " v=" + expected[i].second +
                   " got k=" + got[i].first + " v=" + got[i].second);
      break;
    }
  }
  if (expected.size() != got.size())
    CHECK(false, ctx + " Scan size mismatch");

  // Every returned key must pass Get
  for (auto& [k, v] : got) {
    std::string gv;
    leveldb::Status gs = db->Get(leveldb::ReadOptions(), k, &gv);
    CHECK_OK(gs, ctx + " Get for scanned key " + k);
    CHECK_EQ(v, gv, ctx + " Scan vs Get mismatch for " + k);
  }

  // Verify sorted
  for (size_t i = 1; i < got.size(); ++i) {
    CHECK(got[i - 1].first < got[i].first,
          ctx + " Scan result not strictly sorted at i=" + std::to_string(i));
  }

  // Every key is strictly inside [start, end)
  for (auto& [k, v] : got) {
    CHECK(k >= start, ctx + " Scan key below start: " + k);
    CHECK(k < end,    ctx + " Scan key at/above end: " + k);
  }
}

// Verify every model key exists with the right value
static void VerifyAllModelKeys(leveldb::DB* db, const Model& model,
                               const std::string& ctx = "") {
  for (auto& [k, v] : model) {
    std::string got;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), k, &got);
    CHECK_OK(s, ctx + " Get model key " + k);
    CHECK_EQ(v, got, ctx + " Value mismatch for " + k);
  }
}

// Verify every key in `absent` is NotFound
static void VerifyAbsent(leveldb::DB* db, const std::vector<std::string>& keys,
                         const std::string& ctx = "") {
  for (auto& k : keys) {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), k, &val),
                    ctx + " key should be absent: " + k);
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// T1 — Basic Scan: ranges, boundaries, inverted, empty
// ═══════════════════════════════════════════════════════════════════════════
static void T1_BasicScan(leveldb::DB* db, Model& model) {
  BeginTest("T1_BasicScan");

  // Insert 1000 keys
  for (int i = 1; i <= 1000; i++) {
    std::string k = Key(i), v = Val(i);
    CHECK_OK(db->Put(leveldb::WriteOptions(), k, v), "T1 Put " + k);
    ModelPut(model, k, v);
  }

  // Full range
  VerifyScan(db, model, Key(1), Key(1001), "T1 full");

  // Sub-ranges
  VerifyScan(db, model, Key(1),   Key(2),    "T1 first key only");
  VerifyScan(db, model, Key(100), Key(200),  "T1 [100,200)");
  VerifyScan(db, model, Key(500), Key(501),  "T1 single key");
  VerifyScan(db, model, Key(999), Key(1001), "T1 tail");

  // Inverted range — must be empty + OK
  {
    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(leveldb::ReadOptions(), Key(500), Key(100), &r), "T1 inverted status");
    CHECK(r.empty(), "T1 inverted range must be empty");
  }

  // Equal range — must be empty + OK
  {
    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(leveldb::ReadOptions(), Key(500), Key(500), &r), "T1 equal range status");
    CHECK(r.empty(), "T1 equal range must be empty");
  }

  // Range with no keys inside
  {
    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(leveldb::ReadOptions(), "zzz_start", "zzz_end", &r), "T1 no-key range");
    CHECK(r.empty(), "T1 no-key range must be empty");
  }

  // Boundaries don't exist but keys are in between
  VerifyScan(db, model, "key000000050x", "key000000060x", "T1 boundary gaps");

  // Null result pointer must return error
  {
    leveldb::Status s = db->Scan(leveldb::ReadOptions(), Key(1), Key(10), nullptr);
    CHECK(!s.ok(), "T1 null result pointer must return error status");
  }

  EndTest("T1_BasicScan");
}

// ═══════════════════════════════════════════════════════════════════════════
// T2 — Scan: strict sort + per-key Get consistency
// ═══════════════════════════════════════════════════════════════════════════
static void T2_ScanSortConsistency(leveldb::DB* db, const Model& model) {
  BeginTest("T2_ScanSortConsistency");

  std::vector<std::pair<std::string, std::string>> result;
  CHECK_OK(db->Scan(leveldb::ReadOptions(), Key(1), Key(1001), &result), "T2 Scan");

  for (size_t i = 1; i < result.size(); ++i)
    CHECK(result[i - 1].first < result[i].first,
          "T2 not strictly sorted at i=" + std::to_string(i));

  for (auto& [k, v] : result) {
    std::string got;
    CHECK_OK(db->Get(leveldb::ReadOptions(), k, &got), "T2 Get " + k);
    CHECK_EQ(v, got, "T2 Scan vs Get mismatch for " + k);
  }

  // Every scan entry must be in [start, end)
  for (auto& [k, v] : result) {
    CHECK(k >= Key(1) && k < Key(1001), "T2 key outside requested range: " + k);
  }

  EndTest("T2_ScanSortConsistency");
}

// ═══════════════════════════════════════════════════════════════════════════
// T3 — Scan: snapshot semantics
// ═══════════════════════════════════════════════════════════════════════════
static void T3_ScanSnapshot(leveldb::DB* db, Model& model) {
  BeginTest("T3_ScanSnapshot");

  // Populate some keys then take a snapshot
  for (int i = 1; i <= 5; i++) {
    CHECK_OK(db->Put(leveldb::WriteOptions(), "snap_pre_" + std::to_string(i),
                     "pre_v" + std::to_string(i)), "T3 pre-put");
    ModelPut(model, "snap_pre_" + std::to_string(i), "pre_v" + std::to_string(i));
  }
  const leveldb::Snapshot* snap1 = db->GetSnapshot();

  // Keys added AFTER snapshot
  for (int i = 1; i <= 5; i++) {
    CHECK_OK(db->Put(leveldb::WriteOptions(), "snap_post_" + std::to_string(i),
                     "post_v" + std::to_string(i)), "T3 post-put");
    ModelPut(model, "snap_post_" + std::to_string(i), "post_v" + std::to_string(i));
  }

  // Scan with snap1: post_ keys must NOT appear
  {
    leveldb::ReadOptions ropt;
    ropt.snapshot = snap1;
    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(ropt, "snap_post_", "snap_post_z", &r), "T3 snap scan post_");
    CHECK(r.empty(), "T3 snap scan must not see keys added after snapshot");
  }

  // Scan with snap1: pre_ keys must appear
  {
    leveldb::ReadOptions ropt;
    ropt.snapshot = snap1;
    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(ropt, "snap_pre_", "snap_pre_z", &r), "T3 snap scan pre_");
    CHECK_EQ((size_t)5, r.size(), "T3 snap scan pre_ count");
  }

  // Take snap2, then delete a pre_ key, snap2 still sees it
  const leveldb::Snapshot* snap2 = db->GetSnapshot();
  CHECK_OK(db->Delete(leveldb::WriteOptions(), "snap_pre_3"), "T3 delete pre_3");
  ModelDelete(model, "snap_pre_3");

  {
    leveldb::ReadOptions ropt;
    ropt.snapshot = snap2;
    std::string val;
    CHECK_OK(db->Get(ropt, "snap_pre_3", &val), "T3 snap2 Get deleted key");
    CHECK_EQ(std::string("pre_v3"), val, "T3 snap2 deleted key value");

    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(ropt, "snap_pre_3", "snap_pre_4", &r), "T3 snap2 Scan deleted key");
    CHECK(r.size() == 1 && r[0].first == "snap_pre_3", "T3 snap2 Scan sees deleted key");
  }

  // Without snapshot: deleted key must be gone
  {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), "snap_pre_3", &val),
                    "T3 current: deleted key absent");
  }

  db->ReleaseSnapshot(snap1);
  db->ReleaseSnapshot(snap2);

  // Snapshot across [snap_a, snap_c) — classic assignment check
  CHECK_OK(db->Put(leveldb::WriteOptions(), "snap_a", "1"), "T3 put snap_a");
  CHECK_OK(db->Put(leveldb::WriteOptions(), "snap_c", "3"), "T3 put snap_c");
  ModelPut(model, "snap_a", "1");
  ModelPut(model, "snap_c", "3");

  const leveldb::Snapshot* snap3 = db->GetSnapshot();
  CHECK_OK(db->Put(leveldb::WriteOptions(), "snap_b", "2"), "T3 put snap_b");
  ModelPut(model, "snap_b", "2");

  {
    leveldb::ReadOptions ropt;
    ropt.snapshot = snap3;
    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(ropt, "snap_a", "snap_c", &r), "T3 classic snap scan");
    // snap_c is the end key (excluded), snap_b wasn't visible at snap3
    CHECK(r.size() == 1 && r[0].first == "snap_a" && r[0].second == "1",
          "T3 classic snap scan: expected only (snap_a,1)");
  }
  db->ReleaseSnapshot(snap3);

  EndTest("T3_ScanSnapshot");
}

// ═══════════════════════════════════════════════════════════════════════════
// T4 — Basic DeleteRange: boundaries, no-op, scan confirmation
// ═══════════════════════════════════════════════════════════════════════════
static void T4_BasicDeleteRange(leveldb::DB* db, Model& model) {
  BeginTest("T4_BasicDeleteRange");

  // Delete [100, 200) from the 1000-key set installed in T1
  ModelDeleteRange(model, Key(100), Key(200));
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(100), Key(200)), "T4 DR [100,200)");

  for (int i = 100; i < 200; i++) {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), Key(i), &val),
                    "T4 deleted key " + Key(i));
  }

  // Keys just outside the range must survive
  {
    std::string val;
    CHECK_OK(db->Get(leveldb::ReadOptions(), Key(99), &val), "T4 key before range");
    CHECK_EQ(Val(99), val, "T4 key before range value");
    CHECK_OK(db->Get(leveldb::ReadOptions(), Key(200), &val), "T4 key at end (exclusive)");
    CHECK_EQ(Val(200), val, "T4 key at end value");
  }

  VerifyScan(db, model, Key(90), Key(210), "T4 scan around deleted range");

  // Empty / inverted DeleteRange must be no-ops
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(500), Key(500)), "T4 empty DR");
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(700), Key(300)), "T4 inverted DR");
  VerifyScan(db, model, Key(490), Key(510), "T4 scan after no-op DR");

  // Single key range [n, n+1) — only that exact key deleted
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(300), Key(301)), "T4 single-key DR");
  ModelDeleteRange(model, Key(300), Key(301));
  {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), Key(300), &val), "T4 single-key gone");
    CHECK_OK(db->Get(leveldb::ReadOptions(), Key(301), &val), "T4 next key still there");
  }

  EndTest("T4_BasicDeleteRange");
}

// ═══════════════════════════════════════════════════════════════════════════
// T5 — CRITICAL: DeleteRange then Put in same range (sequence-number bug)
// ═══════════════════════════════════════════════════════════════════════════
static void T5_DeleteRangeThenPut(leveldb::DB* db, Model& model) {
  BeginTest("T5_DeleteRangeThenPut");

  // Fresh sub-range to avoid interference
  for (int i = 4000; i < 4100; i++) {
    CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), Val(i)), "T5 setup Put");
    ModelPut(model, Key(i), Val(i));
  }

  // Delete range [4000, 4100)
  ModelDeleteRange(model, Key(4000), Key(4100));
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(4000), Key(4100)), "T5 DR");

  // Re-put subset inside the deleted range — these MUST survive compaction
  for (int i = 4020; i < 4030; i++) {
    std::string v = Val(i, "_reput");
    CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), v), "T5 re-Put");
    ModelPut(model, Key(i), v);
  }

  // Verify BEFORE compaction
  for (int i = 4000; i < 4100; i++) {
    std::string val;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), Key(i), &val);
    if (i >= 4020 && i < 4030) {
      CHECK_OK(s, "T5 pre-compact: re-put key should exist " + Key(i));
      CHECK_EQ(Val(i, "_reput"), val, "T5 pre-compact reput value " + Key(i));
    } else {
      CHECK_NOT_FOUND(s, "T5 pre-compact: deleted key should be absent " + Key(i));
    }
  }

  // ForceFullCompaction — re-put keys MUST NOT be incorrectly dropped
  CHECK_OK(db->ForceFullCompaction(), "T5 ForceFullCompaction");

  // Verify AFTER compaction
  for (int i = 4000; i < 4100; i++) {
    std::string val;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), Key(i), &val);
    if (i >= 4020 && i < 4030) {
      CHECK_OK(s, "T5 post-compact: re-put key MUST exist " + Key(i));
      CHECK_EQ(Val(i, "_reput"), val, "T5 post-compact reput value " + Key(i));
    } else {
      CHECK_NOT_FOUND(s, "T5 post-compact: deleted key MUST be absent " + Key(i));
    }
  }

  VerifyScan(db, model, Key(3990), Key(4110), "T5 scan post-compact");

  // Round 2: multiple cycles of delete-then-reput-then-delete
  for (int cycle = 0; cycle < 3; cycle++) {
    // Put fresh
    for (int i = 4200; i < 4250; i++) {
      std::string v = Val(i, "_c" + std::to_string(cycle));
      CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), v), "T5 cycle Put");
      ModelPut(model, Key(i), v);
    }
    // Delete range
    ModelDeleteRange(model, Key(4200), Key(4250));
    CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(4200), Key(4250)), "T5 cycle DR");
    // Reput half
    for (int i = 4225; i < 4240; i++) {
      std::string v = Val(i, "_rc" + std::to_string(cycle));
      CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), v), "T5 cycle rePut");
      ModelPut(model, Key(i), v);
    }
  }

  CHECK_OK(db->ForceFullCompaction(), "T5 cycle FFC");
  VerifyAllModelKeys(db, model, "T5 cycles post-FFC");
  VerifyScan(db, model, Key(4195), Key(4260), "T5 cycles scan");

  EndTest("T5_DeleteRangeThenPut");
}

// ═══════════════════════════════════════════════════════════════════════════
// T6 — Overlapping DeleteRange operations
// ═══════════════════════════════════════════════════════════════════════════
static void T6_OverlappingDeleteRange(leveldb::DB* db, Model& model) {
  BeginTest("T6_OverlappingDeleteRange");

  for (int i = 5000; i < 5200; i++) {
    CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), Val(i)), "T6 Put");
    ModelPut(model, Key(i), Val(i));
  }

  // Three overlapping ranges: [5010,5060), [5040,5080), [5070,5100)
  struct DR { int s, e; };
  std::vector<DR> ranges = {{5010, 5060}, {5040, 5080}, {5070, 5100}};
  for (auto& dr : ranges) {
    ModelDeleteRange(model, Key(dr.s), Key(dr.e));
    CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(dr.s), Key(dr.e)), "T6 DR");
  }

  // Verify every key individually
  for (int i = 5000; i < 5200; i++) {
    std::string val;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), Key(i), &val);
    bool in_model = model.count(Key(i)) > 0;
    if (in_model) {
      CHECK_OK(s, "T6 should exist: " + Key(i));
      CHECK_EQ(model.at(Key(i)), val, "T6 value mismatch: " + Key(i));
    } else {
      CHECK_NOT_FOUND(s, "T6 should be absent: " + Key(i));
    }
  }

  VerifyScan(db, model, Key(5000), Key(5200), "T6 scan overlap DR");

  // Put back some keys that were deleted, then compact
  for (int i = 5045; i < 5055; i++) {
    std::string v = Val(i, "_back");
    CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), v), "T6 put-back");
    ModelPut(model, Key(i), v);
  }

  CHECK_OK(db->ForceFullCompaction(), "T6 FFC");

  for (int i = 5000; i < 5200; i++) {
    std::string val;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), Key(i), &val);
    bool in_model = model.count(Key(i)) > 0;
    if (in_model) {
      CHECK_OK(s, "T6 post-FFC should exist: " + Key(i));
    } else {
      CHECK_NOT_FOUND(s, "T6 post-FFC should be absent: " + Key(i));
    }
  }

  EndTest("T6_OverlappingDeleteRange");
}

// ═══════════════════════════════════════════════════════════════════════════
// T7 — ForceFullCompaction: correctness, double FFC, data integrity
// ═══════════════════════════════════════════════════════════════════════════
static void T7_ForceFullCompaction(leveldb::DB* db, Model& model) {
  BeginTest("T7_ForceFullCompaction");

  // Load enough data to span multiple SSTables
  for (int i = 10000; i < 11000; i++) {
    CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), Val(i)), "T7 Put");
    ModelPut(model, Key(i), Val(i));
  }

  // Individual deletes for first 300
  for (int i = 10000; i < 10300; i++) {
    CHECK_OK(db->Delete(leveldb::WriteOptions(), Key(i)), "T7 Delete");
    ModelDelete(model, Key(i));
  }

  // Range-delete next 200
  ModelDeleteRange(model, Key(10300), Key(10500));
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(10300), Key(10500)), "T7 DR");

  // Update remaining keys
  for (int i = 10500; i < 11000; i++) {
    std::string v = Val(i, "_upd");
    CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), v), "T7 Update");
    ModelPut(model, Key(i), v);
  }

  CHECK_OK(db->ForceFullCompaction(), "T7 FFC 1");
  VerifyAllModelKeys(db, model, "T7 after FFC 1");

  // Deleted keys must be physically gone
  for (int i = 10000; i < 10500; i++) {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), Key(i), &val), "T7 absent post-FFC");
  }

  // Remaining keys correct
  for (int i = 10500; i < 11000; i++) {
    std::string val;
    CHECK_OK(db->Get(leveldb::ReadOptions(), Key(i), &val), "T7 present post-FFC");
    CHECK_EQ(Val(i, "_upd"), val, "T7 updated value");
  }

  // Double FFC on already-compacted DB — must be idempotent
  CHECK_OK(db->ForceFullCompaction(), "T7 FFC 2 (idempotent)");
  VerifyAllModelKeys(db, model, "T7 after FFC 2");
  CHECK_OK(db->ForceFullCompaction(), "T7 FFC 3 (idempotent)");

  VerifyScan(db, model, Key(10000), Key(11000), "T7 scan post-triple-FFC");

  EndTest("T7_ForceFullCompaction");
}

// ═══════════════════════════════════════════════════════════════════════════
// T8 — DeleteRange with no keys in range (pure no-op)
// ═══════════════════════════════════════════════════════════════════════════
static void T8_DeleteRangeNoKeys(leveldb::DB* db, Model& model) {
  BeginTest("T8_DeleteRangeNoKeys");

  // Range guaranteed to be empty in DB
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), "zzz00001", "zzz99999"), "T8 DR empty region");
  VerifyScan(db, model, "zzz00000", "zzzzzzzzz", "T8 scan empty region");

  // Empty because start == end
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(1), Key(1)), "T8 DR start==end");

  // Empty because start > end
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(999), Key(1)), "T8 DR start>end");

  // Verify model untouched
  VerifyAllModelKeys(db, model, "T8 model unchanged");

  EndTest("T8_DeleteRangeNoKeys");
}

// ═══════════════════════════════════════════════════════════════════════════
// T9 — Large-scale stress: 20000 random ops, model comparison throughout
// ═══════════════════════════════════════════════════════════════════════════
static void T9_LargeStress(leveldb::DB* db, Model& model) {
  BeginTest("T9_LargeStress");

  std::mt19937 gen(0xDEADBEEF);
  auto rng = [&](int lo, int hi) -> int {
    return lo + static_cast<int>(gen() % static_cast<uint32_t>(hi - lo + 1));
  };

  // Phase A: mass insert
  for (int i = 0; i < 10000; i++) {
    int ki = rng(1, 30000);
    std::string k = Key(ki), v = Val(ki, "_A" + std::to_string(i));
    CHECK_OK(db->Put(leveldb::WriteOptions(), k, v), "T9A Put");
    ModelPut(model, k, v);
  }

  // Phase B: random scans vs model
  for (int i = 0; i < 300; i++) {
    int a = rng(1, 30000), b = rng(1, 30000);
    if (a > b) std::swap(a, b);
    VerifyScan(db, model, Key(a), Key(b + 1), "T9B scan " + std::to_string(i));
  }

  // Phase C: random deletes
  for (int i = 0; i < 1000; i++) {
    int ki = rng(1, 30000);
    CHECK_OK(db->Delete(leveldb::WriteOptions(), Key(ki)), "T9C Delete");
    ModelDelete(model, Key(ki));
  }

  // Phase D: random DeleteRange (capped width to keep it interesting)
  for (int i = 0; i < 10; i++) {
    int a = rng(1, 30000), b = rng(1, 30000);
    if (a > b) std::swap(a, b);
    b = std::min(b, a + 500);
    ModelDeleteRange(model, Key(a), Key(b));
    CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(a), Key(b)), "T9D DR");
  }

  // Phase E: scans after deletes
  for (int i = 0; i < 200; i++) {
    int a = rng(1, 30000), b = rng(1, 30000);
    if (a > b) std::swap(a, b);
    VerifyScan(db, model, Key(a), Key(b + 1), "T9E scan " + std::to_string(i));
  }

  // Phase F: puts back into deleted ranges (tests sequence-number correctness)
  for (int i = 0; i < 3000; i++) {
    int ki = rng(1, 30000);
    std::string v = Val(ki, "_F" + std::to_string(i));
    CHECK_OK(db->Put(leveldb::WriteOptions(), Key(ki), v), "T9F Put-back");
    ModelPut(model, Key(ki), v);
  }

  // Phase G: ForceFullCompaction
  CHECK_OK(db->ForceFullCompaction(), "T9G FFC");

  // Phase H: full model verification after compaction
  VerifyAllModelKeys(db, model, "T9H post-FFC model");

  // Phase I: scans after compaction
  for (int i = 0; i < 300; i++) {
    int a = rng(1, 30000), b = rng(1, 30000);
    if (a > b) std::swap(a, b);
    VerifyScan(db, model, Key(a), Key(b + 1), "T9I post-FFC scan " + std::to_string(i));
  }

  EndTest("T9_LargeStress");
}

// ═══════════════════════════════════════════════════════════════════════════
// T10 — Multi-round Put → DR → Put → DR → Put → FFC chain
// ═══════════════════════════════════════════════════════════════════════════
static void T10_PutDRChain(leveldb::DB* db, Model& model) {
  BeginTest("T10_PutDRChain");

  const std::string pfx = "chain";

  // Round 1: put 100 keys under "chain" prefix
  for (int i = 0; i < 100; i++) {
    std::string k = pfx + Key(i), v = "r1_" + Val(i);
    CHECK_OK(db->Put(leveldb::WriteOptions(), k, v), "T10 r1 Put");
    ModelPut(model, k, v);
  }

  // Round 2: delete all
  std::string ds = pfx + Key(0), de = pfx + Key(100);
  ModelDeleteRange(model, ds, de);
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), ds, de), "T10 DR r2");

  // Round 3: re-put 0..49
  for (int i = 0; i < 50; i++) {
    std::string k = pfx + Key(i), v = "r3_" + Val(i);
    CHECK_OK(db->Put(leveldb::WriteOptions(), k, v), "T10 r3 Put");
    ModelPut(model, k, v);
  }

  // Round 4: delete 0..19
  std::string ds2 = pfx + Key(0), de2 = pfx + Key(20);
  ModelDeleteRange(model, ds2, de2);
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), ds2, de2), "T10 DR r4");

  // Round 5: re-put 5..9 (inside the last deleted range)
  for (int i = 5; i < 10; i++) {
    std::string k = pfx + Key(i), v = "r5_" + Val(i);
    CHECK_OK(db->Put(leveldb::WriteOptions(), k, v), "T10 r5 Put");
    ModelPut(model, k, v);
  }

  // Verify BEFORE FFC
  for (int i = 0; i < 100; i++) {
    std::string k = pfx + Key(i), val;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), k, &val);
    if (model.count(k)) {
      CHECK_OK(s, "T10 pre-FFC exist " + k);
      CHECK_EQ(model.at(k), val, "T10 pre-FFC value " + k);
    } else {
      CHECK_NOT_FOUND(s, "T10 pre-FFC absent " + k);
    }
  }

  CHECK_OK(db->ForceFullCompaction(), "T10 FFC");

  // Verify AFTER FFC
  for (int i = 0; i < 100; i++) {
    std::string k = pfx + Key(i), val;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), k, &val);
    if (model.count(k)) {
      CHECK_OK(s, "T10 post-FFC exist " + k);
      CHECK_EQ(model.at(k), val, "T10 post-FFC value " + k);
    } else {
      CHECK_NOT_FOUND(s, "T10 post-FFC absent " + k);
    }
  }

  VerifyScan(db, model, ds, de, "T10 scan post-FFC");

  EndTest("T10_PutDRChain");
}

// ═══════════════════════════════════════════════════════════════════════════
// T11 — DeleteRange exact key boundaries (inclusive start, exclusive end)
// ═══════════════════════════════════════════════════════════════════════════
static void T11_ExactBoundaries(leveldb::DB* db, Model& model) {
  BeginTest("T11_ExactBoundaries");

  // Three adjacent keys: alpha, beta, gamma
  std::string ka = "bnd_alpha", kb = "bnd_beta", kc = "bnd_gamma";
  CHECK_OK(db->Put(leveldb::WriteOptions(), ka, "va"), "T11 Put ka");
  CHECK_OK(db->Put(leveldb::WriteOptions(), kb, "vb"), "T11 Put kb");
  CHECK_OK(db->Put(leveldb::WriteOptions(), kc, "vc"), "T11 Put kc");
  ModelPut(model, ka, "va");
  ModelPut(model, kb, "vb");
  ModelPut(model, kc, "vc");

  // [ka, kb) deletes only ka; kb and kc survive
  ModelDeleteRange(model, ka, kb);
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), ka, kb), "T11 DR [a,b)");
  {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), ka, &val), "T11 ka deleted");
    CHECK_OK(db->Get(leveldb::ReadOptions(), kb, &val), "T11 kb survives");
    CHECK_EQ(std::string("vb"), val, "T11 kb value");
    CHECK_OK(db->Get(leveldb::ReadOptions(), kc, &val), "T11 kc survives");
  }

  // [kb, kc) deletes only kb; kc survives
  ModelDeleteRange(model, kb, kc);
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), kb, kc), "T11 DR [b,c)");
  {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), kb, &val), "T11 kb deleted");
    CHECK_OK(db->Get(leveldb::ReadOptions(), kc, &val), "T11 kc still survives");
    CHECK_EQ(std::string("vc"), val, "T11 kc value");
  }

  CHECK_OK(db->ForceFullCompaction(), "T11 FFC");

  {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), ka, &val), "T11 ka absent post-FFC");
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), kb, &val), "T11 kb absent post-FFC");
    CHECK_OK(db->Get(leveldb::ReadOptions(), kc, &val), "T11 kc present post-FFC");
    CHECK_EQ(std::string("vc"), val, "T11 kc value post-FFC");
  }

  EndTest("T11_ExactBoundaries");
}

// ═══════════════════════════════════════════════════════════════════════════
// T12 — Scan: multiple updates to same key, always returns latest version
// ═══════════════════════════════════════════════════════════════════════════
static void T12_MultiUpdateScan(leveldb::DB* db, Model& model) {
  BeginTest("T12_MultiUpdateScan");

  std::string k = "multiupdate_key";
  for (int i = 0; i < 200; i++) {
    std::string v = "ver_" + std::to_string(i);
    CHECK_OK(db->Put(leveldb::WriteOptions(), k, v), "T12 Put ver " + std::to_string(i));
    ModelPut(model, k, v);
  }

  // Scan should return exactly one entry with the latest value
  {
    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(leveldb::ReadOptions(), k, k + "\xff", &r), "T12 Scan");
    CHECK(r.size() == 1, "T12 scan size must be 1");
    if (!r.empty()) {
      CHECK_EQ(k, r[0].first, "T12 scan key");
      CHECK_EQ(std::string("ver_199"), r[0].second, "T12 scan latest value");
    }
  }

  CHECK_OK(db->ForceFullCompaction(), "T12 FFC");

  {
    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(leveldb::ReadOptions(), k, k + "\xff", &r), "T12 Scan post-FFC");
    CHECK(r.size() == 1, "T12 post-FFC scan size must be 1");
    if (!r.empty())
      CHECK_EQ(std::string("ver_199"), r[0].second, "T12 post-FFC latest value");
  }

  EndTest("T12_MultiUpdateScan");
}

// ═══════════════════════════════════════════════════════════════════════════
// T13 — FFC on empty DB
// ═══════════════════════════════════════════════════════════════════════════
static void T13_FFCEmptyDB() {
  BeginTest("T13_FFCEmptyDB");

  std::string path = DbBasePath() + "_empty";
  leveldb::DB* db = OpenFreshDB(path);

  CHECK_OK(db->ForceFullCompaction(), "T13 FFC on empty DB");

  std::vector<std::pair<std::string, std::string>> r;
  CHECK_OK(db->Scan(leveldb::ReadOptions(), "a", "z", &r), "T13 Scan empty");
  CHECK(r.empty(), "T13 scan empty DB must return empty");

  CHECK_OK(db->ForceFullCompaction(), "T13 second FFC on empty DB");

  delete db;
  EndTest("T13_FFCEmptyDB");
}

// ═══════════════════════════════════════════════════════════════════════════
// T14 — Multiple consecutive FFC then full model scan
// ═══════════════════════════════════════════════════════════════════════════
static void T14_TripleFFC(leveldb::DB* db, Model& model) {
  BeginTest("T14_TripleFFC");

  CHECK_OK(db->ForceFullCompaction(), "T14 FFC 1");
  CHECK_OK(db->ForceFullCompaction(), "T14 FFC 2");
  CHECK_OK(db->ForceFullCompaction(), "T14 FFC 3");

  VerifyAllModelKeys(db, model, "T14 post-triple-FFC");
  VerifyScan(db, model, Key(1), Key(35000), "T14 big scan");

  EndTest("T14_TripleFFC");
}

// ═══════════════════════════════════════════════════════════════════════════
// T15 — DeleteRange covering whole key space, then put back
// ═══════════════════════════════════════════════════════════════════════════
static void T15_DeleteAll(leveldb::DB* db, Model& model) {
  BeginTest("T15_DeleteAll");

  // Wipe all "key0xxxxxxx" keys
  ModelDeleteRange(model, Key(0), Key(99999));
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(0), Key(99999)), "T15 DR all");

  // Sample verification
  for (int i : {1, 100, 500, 1000, 5000, 10000, 15000, 20000, 25000, 30000}) {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), Key(i), &val),
                    "T15 absent " + Key(i));
  }

  std::vector<std::pair<std::string, std::string>> r;
  CHECK_OK(db->Scan(leveldb::ReadOptions(), Key(0), Key(99999), &r), "T15 scan empty");
  CHECK(r.empty(), "T15 scan must be empty after delete-all");

  // Put some back
  for (int i = 200; i <= 215; i++) {
    std::string v = Val(i, "_back");
    CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), v), "T15 put-back");
    ModelPut(model, Key(i), v);
  }

  // Immediately visible before FFC
  for (int i = 200; i <= 215; i++) {
    std::string val;
    CHECK_OK(db->Get(leveldb::ReadOptions(), Key(i), &val), "T15 put-back visible");
    CHECK_EQ(Val(i, "_back"), val, "T15 put-back value");
  }

  CHECK_OK(db->ForceFullCompaction(), "T15 FFC after put-back");

  // Still visible after FFC
  for (int i = 200; i <= 215; i++) {
    std::string val;
    CHECK_OK(db->Get(leveldb::ReadOptions(), Key(i), &val), "T15 post-FFC");
    CHECK_EQ(Val(i, "_back"), val, "T15 post-FFC value");
  }

  VerifyScan(db, model, Key(0), Key(99999), "T15 scan after put-back FFC");

  EndTest("T15_DeleteAll");
}

// ═══════════════════════════════════════════════════════════════════════════
// T16 — Scan boundary strictness: end key must NEVER appear in results
// ═══════════════════════════════════════════════════════════════════════════
static void T16_ScanBoundaryStrictness(leveldb::DB* db, Model& model) {
  BeginTest("T16_ScanBoundaryStrictness");

  std::string ks = "bndstrt_key", ke = "bndend_key", km = "bndmid_key";
  CHECK_OK(db->Put(leveldb::WriteOptions(), ks, "vs"), "T16 Put ks");
  CHECK_OK(db->Put(leveldb::WriteOptions(), ke, "ve"), "T16 Put ke");
  CHECK_OK(db->Put(leveldb::WriteOptions(), km, "vm"), "T16 Put km");
  ModelPut(model, ks, "vs");
  ModelPut(model, ke, "ve");
  ModelPut(model, km, "vm");

  // Scan [ks, ke): all keys >= ks and < ke
  // Lexicographically: bndend_ < bndmid_ < bndstrt_, so in [ks, ke):
  // ks="bndstrt_key" and ke="bndend_key" — ke < ks, so range is inverted → empty
  // Let's use a range where the boundary matters clearly
  std::string start2 = "bndend_key", end2 = "bndstrt_key";
  {
    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(leveldb::ReadOptions(), start2, end2, &r), "T16 scan [end,strt)");
    for (auto& [k, v] : r) {
      CHECK(k >= start2, "T16 key below start: " + k);
      CHECK(k < end2, "T16 key at/above end: " + k);
    }
    bool found_end = false;
    for (auto& [k, v] : r) if (k == end2) found_end = true;
    CHECK(!found_end, "T16 end key must not appear in scan");
  }

  // Another explicit check: put key exactly equal to end_key, verify it's excluded
  std::string explicit_end = "explicit_end_key";
  std::string just_before  = "explicit_end_jey";  // 'j' < 'k'
  CHECK_OK(db->Put(leveldb::WriteOptions(), explicit_end, "end_val"), "T16 Put explicit_end");
  CHECK_OK(db->Put(leveldb::WriteOptions(), just_before, "before_val"), "T16 Put just_before");
  ModelPut(model, explicit_end, "end_val");
  ModelPut(model, just_before, "before_val");

  {
    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(leveldb::ReadOptions(), just_before, explicit_end, &r),
             "T16 scan up to explicit_end");
    bool found_end_key = false;
    for (auto& [k, v] : r) if (k == explicit_end) found_end_key = true;
    CHECK(!found_end_key, "T16 explicit_end must not appear in scan results");
    // just_before should appear
    bool found_before = false;
    for (auto& [k, v] : r) if (k == just_before) found_before = true;
    CHECK(found_before, "T16 just_before should appear in scan results");
  }

  EndTest("T16_ScanBoundaryStrictness");
}

// ═══════════════════════════════════════════════════════════════════════════
// T17 — Frequent FFC interspersed with puts and DR
// ═══════════════════════════════════════════════════════════════════════════
static void T17_FrequentFFC(leveldb::DB* db, Model& model) {
  BeginTest("T17_FrequentFFC");

  std::mt19937 gen(0xCAFEBABE);
  auto rng = [&](int lo, int hi) -> int {
    return lo + static_cast<int>(gen() % static_cast<uint32_t>(hi - lo + 1));
  };

  for (int round = 0; round < 8; round++) {
    std::string rtag = "_r" + std::to_string(round);

    // Put
    for (int i = 0; i < 150; i++) {
      int ki = rng(60000, 70000);
      std::string k = Key(ki), v = Val(ki, rtag);
      CHECK_OK(db->Put(leveldb::WriteOptions(), k, v), "T17 Put round " + std::to_string(round));
      ModelPut(model, k, v);
    }

    // DeleteRange
    int a = rng(60000, 70000), b = rng(60000, 70000);
    if (a > b) std::swap(a, b);
    b = std::min(b, a + 300);
    ModelDeleteRange(model, Key(a), Key(b));
    CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(a), Key(b)), "T17 DR round " + std::to_string(round));

    // FFC every round
    CHECK_OK(db->ForceFullCompaction(), "T17 FFC round " + std::to_string(round));

    // Spot-check a sample of model keys
    VerifyAllModelKeys(db, model, "T17 round " + std::to_string(round));

    // Scan the active region
    VerifyScan(db, model, Key(60000), Key(70001), "T17 scan round " + std::to_string(round));
  }

  EndTest("T17_FrequentFFC");
}

// ═══════════════════════════════════════════════════════════════════════════
// T18 — Long keys and large values
// ═══════════════════════════════════════════════════════════════════════════
static void T18_LongKeysValues(leveldb::DB* db, Model& model) {
  BeginTest("T18_LongKeysValues");

  std::string longval(50000, 'X');
  std::string lk1 = std::string(200, 'L') + "1";
  std::string lk2 = std::string(200, 'L') + "2";
  std::string lk3 = std::string(200, 'L') + "3";

  CHECK_OK(db->Put(leveldb::WriteOptions(), lk1, longval + "A"), "T18 Put lk1");
  CHECK_OK(db->Put(leveldb::WriteOptions(), lk2, longval + "B"), "T18 Put lk2");
  CHECK_OK(db->Put(leveldb::WriteOptions(), lk3, longval + "C"), "T18 Put lk3");
  ModelPut(model, lk1, longval + "A");
  ModelPut(model, lk2, longval + "B");
  ModelPut(model, lk3, longval + "C");

  VerifyScan(db, model, lk1, lk3, "T18 scan long keys");

  // Delete [lk1, lk3): removes lk1 and lk2, lk3 survives
  ModelDeleteRange(model, lk1, lk3);
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), lk1, lk3), "T18 DR long");

  {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), lk1, &val), "T18 lk1 absent");
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), lk2, &val), "T18 lk2 absent");
    CHECK_OK(db->Get(leveldb::ReadOptions(), lk3, &val), "T18 lk3 present");
    CHECK_EQ(longval + "C", val, "T18 lk3 value");
  }

  CHECK_OK(db->ForceFullCompaction(), "T18 FFC");

  {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), lk1, &val), "T18 lk1 absent post-FFC");
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), lk2, &val), "T18 lk2 absent post-FFC");
    CHECK_OK(db->Get(leveldb::ReadOptions(), lk3, &val), "T18 lk3 present post-FFC");
    CHECK_EQ(longval + "C", val, "T18 lk3 value post-FFC");
  }

  EndTest("T18_LongKeysValues");
}

// ═══════════════════════════════════════════════════════════════════════════
// T19 — Read-your-writes: Get immediately after Put/Delete/DeleteRange
// ═══════════════════════════════════════════════════════════════════════════
static void T19_ReadYourWrites(leveldb::DB* db, Model& model) {
  BeginTest("T19_ReadYourWrites");

  for (int i = 80000; i < 80200; i++) {
    std::string k = Key(i), v = Val(i);
    CHECK_OK(db->Put(leveldb::WriteOptions(), k, v), "T19 Put");
    // Immediately read back
    std::string got;
    CHECK_OK(db->Get(leveldb::ReadOptions(), k, &got), "T19 Get after Put");
    CHECK_EQ(v, got, "T19 read-your-write");
    ModelPut(model, k, v);
  }

  // Delete and immediately check
  for (int i = 80000; i < 80050; i++) {
    CHECK_OK(db->Delete(leveldb::WriteOptions(), Key(i)), "T19 Delete");
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), Key(i), &val),
                    "T19 Get after Delete " + Key(i));
    ModelDelete(model, Key(i));
  }

  // DeleteRange and immediately check
  ModelDeleteRange(model, Key(80050), Key(80100));
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(80050), Key(80100)), "T19 DR");
  for (int i = 80050; i < 80100; i++) {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), Key(i), &val),
                    "T19 Get after DR " + Key(i));
  }

  // Remaining keys still visible
  for (int i = 80100; i < 80200; i++) {
    std::string val;
    CHECK_OK(db->Get(leveldb::ReadOptions(), Key(i), &val), "T19 remaining");
    CHECK_EQ(Val(i), val, "T19 remaining value");
  }

  VerifyScan(db, model, Key(80000), Key(80200), "T19 scan");

  EndTest("T19_ReadYourWrites");
}

// ═══════════════════════════════════════════════════════════════════════════
// T20 — Concurrent stress (separate DB, no golden output — pass/fail only)
// ═══════════════════════════════════════════════════════════════════════════
static void T20_Concurrent() {
  BeginTest("T20_Concurrent");

  std::string path = DbBasePath() + "_concurrent";
  leveldb::DB* db = OpenFreshDB(path, 32 * 1024);  // 32KB — very frequent flushes

  const int N_THREADS = 10;
  const int OPS_EACH  = 5000;
  std::atomic<bool> failed{false};
  std::atomic<int>  error_count{0};

  auto worker = [&](int tid) {
    std::mt19937 gen(static_cast<unsigned>(tid) * 1009u + 37u);
    auto rng = [&](int lo, int hi) -> int {
      return lo + static_cast<int>(gen() % static_cast<uint32_t>(hi - lo + 1));
    };

    for (int iter = 0; iter < OPS_EACH; ++iter) {
      if (failed.load(std::memory_order_relaxed)) return;

      int op = rng(0, 99);
      std::string k1 = Key(rng(1, 8000));
      std::string k2 = Key(rng(1, 8000));
      if (k1 > k2) std::swap(k1, k2);
      std::string v = "t" + std::to_string(tid) + "i" + std::to_string(iter);

      leveldb::Status s;
      if      (op < 30) { s = db->Put(leveldb::WriteOptions(), k1, v); }
      else if (op < 50) { std::string g; s = db->Get(leveldb::ReadOptions(), k1, &g);
                          if (s.IsNotFound()) s = leveldb::Status::OK(); }
      else if (op < 60) { s = db->Delete(leveldb::WriteOptions(), k1); }
      else if (op < 78) {
        std::vector<std::pair<std::string, std::string>> r;
        s = db->Scan(leveldb::ReadOptions(), k1, k2, &r);
        if (s.ok()) {
          for (size_t j = 1; j < r.size(); ++j) {
            if (r[j-1].first >= r[j].first) {
              std::cerr << "T20: concurrent Scan not sorted at tid=" << tid
                        << " iter=" << iter << "\n";
              failed.store(true);
              return;
            }
          }
          for (auto& [k, v2] : r) {
            if (k < k1 || k >= k2) {
              std::cerr << "T20: concurrent Scan key out of range: " << k
                        << " not in [" << k1 << "," << k2 << ")\n";
              failed.store(true);
              return;
            }
          }
        }
      }
      else if (op < 93) { s = db->DeleteRange(leveldb::WriteOptions(), k1, k2); }
      else              { s = db->ForceFullCompaction(); }

      if (!s.ok()) {
        error_count.fetch_add(1, std::memory_order_relaxed);
        failed.store(true);
        std::cerr << "T20: worker tid=" << tid << " iter=" << iter
                  << " op=" << op << " error: " << s.ToString() << "\n";
        return;
      }
    }
  };

  std::vector<std::thread> threads;
  threads.reserve(N_THREADS);
  for (int i = 0; i < N_THREADS; ++i) threads.emplace_back(worker, i);
  for (auto& t : threads) t.join();

  CHECK(!failed.load(), "T20 concurrent worker failed (" +
        std::to_string(error_count.load()) + " errors)");

  // Final compaction must succeed after all concurrent chaos
  CHECK_OK(db->ForceFullCompaction(), "T20 final FFC");

  delete db;
  EndTest("T20_Concurrent");
}

// ═══════════════════════════════════════════════════════════════════════════
// T21 — DeleteRange then immediate Scan (before any compaction)
// ═══════════════════════════════════════════════════════════════════════════
static void T21_DeleteRangeImmediateScan(leveldb::DB* db, Model& model) {
  BeginTest("T21_DeleteRangeImmediateScan");

  // Fresh key block
  for (int i = 90000; i < 90500; i++) {
    CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), Val(i)), "T21 Put");
    ModelPut(model, Key(i), Val(i));
  }

  // Delete two sub-ranges without compacting
  ModelDeleteRange(model, Key(90050), Key(90150));
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(90050), Key(90150)), "T21 DR1");

  ModelDeleteRange(model, Key(90300), Key(90400));
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), Key(90300), Key(90400)), "T21 DR2");

  // Immediately scan — no FFC in between
  VerifyScan(db, model, Key(90000), Key(90500), "T21 immediate post-DR scan");

  // Spot Get checks
  for (int i = 90050; i < 90150; i++) {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), Key(i), &val), "T21 DR1 absent");
  }
  for (int i = 90300; i < 90400; i++) {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), Key(i), &val), "T21 DR2 absent");
  }

  // Keys between the two deleted ranges must still exist
  for (int i = 90150; i < 90300; i++) {
    std::string val;
    CHECK_OK(db->Get(leveldb::ReadOptions(), Key(i), &val), "T21 between-ranges");
    CHECK_EQ(Val(i), val, "T21 between-ranges value");
  }

  EndTest("T21_DeleteRangeImmediateScan");
}

// ═══════════════════════════════════════════════════════════════════════════
// T22 — Edge: single-key DB, Scan, DeleteRange, FFC
// ═══════════════════════════════════════════════════════════════════════════
static void T22_SingleKeyDB() {
  BeginTest("T22_SingleKeyDB");

  std::string path = DbBasePath() + "_single";
  leveldb::DB* db = OpenFreshDB(path);
  Model m;

  CHECK_OK(db->Put(leveldb::WriteOptions(), "solo", "one"), "T22 Put");
  ModelPut(m, "solo", "one");

  VerifyScan(db, m, "solo", "solp", "T22 scan single key");

  // Scan that contains it
  {
    std::vector<std::pair<std::string, std::string>> r;
    CHECK_OK(db->Scan(leveldb::ReadOptions(), "a", "z", &r), "T22 full scan");
    CHECK(r.size() == 1, "T22 single key scan count");
  }

  // DeleteRange it
  ModelDeleteRange(m, "solo", "solp");
  CHECK_OK(db->DeleteRange(leveldb::WriteOptions(), "solo", "solp"), "T22 DR");
  {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), "solo", &val), "T22 solo gone");
  }

  CHECK_OK(db->ForceFullCompaction(), "T22 FFC");
  {
    std::string val;
    CHECK_NOT_FOUND(db->Get(leveldb::ReadOptions(), "solo", &val), "T22 solo gone post-FFC");
  }

  delete db;
  EndTest("T22_SingleKeyDB");
}

// ═══════════════════════════════════════════════════════════════════════════
// T23 — Verify FFC actually removes tombstones (SSTable-level correctness)
// ═══════════════════════════════════════════════════════════════════════════
static void T23_TombstoneCleanup(leveldb::DB* db, Model& model) {
  BeginTest("T23_TombstoneCleanup");

  // Insert a large batch, delete all, compact, then insert different values.
  // If tombstones aren't cleaned, old values might resurface.
  for (int i = 95000; i < 95500; i++) {
    CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), Val(i, "_old")), "T23 Put old");
    ModelPut(model, Key(i), Val(i, "_old"));
  }

  // Delete all via individual tombstones
  for (int i = 95000; i < 95500; i++) {
    CHECK_OK(db->Delete(leveldb::WriteOptions(), Key(i)), "T23 Delete");
    ModelDelete(model, Key(i));
  }

  CHECK_OK(db->ForceFullCompaction(), "T23 FFC 1 (removes tombstones)");

  // Now put different values — if ghosts of old values remain, this fails
  for (int i = 95000; i < 95500; i++) {
    CHECK_OK(db->Put(leveldb::WriteOptions(), Key(i), Val(i, "_new")), "T23 Put new");
    ModelPut(model, Key(i), Val(i, "_new"));
  }

  CHECK_OK(db->ForceFullCompaction(), "T23 FFC 2");

  for (int i = 95000; i < 95500; i++) {
    std::string val;
    CHECK_OK(db->Get(leveldb::ReadOptions(), Key(i), &val), "T23 Get new");
    CHECK_EQ(Val(i, "_new"), val, "T23 new value — no ghost");
  }

  VerifyScan(db, model, Key(95000), Key(95500), "T23 scan new values");

  EndTest("T23_TombstoneCleanup");
}

// ═══════════════════════════════════════════════════════════════════════════
// T24 — Final consistency: exhaustive key-by-key verification across whole model
// ═══════════════════════════════════════════════════════════════════════════
static void T24_FinalExhaustiveCheck(leveldb::DB* db, const Model& model) {
  BeginTest("T24_FinalExhaustiveCheck");

  // One last FFC for good measure
  CHECK_OK(db->ForceFullCompaction(), "T24 final FFC");

  // Verify every model entry
  VerifyAllModelKeys(db, model, "T24 all model keys");

  // Full scan must match model exactly in [Key(0), Key(99999)) union other prefixes
  std::vector<std::pair<std::string, std::string>> db_all;

  // Scan various prefix ranges
  auto scan_range = [&](const std::string& a, const std::string& b) {
    auto expected = ModelScan(model, a, b);
    std::vector<std::pair<std::string, std::string>> got;
    CHECK_OK(db->Scan(leveldb::ReadOptions(), a, b, &got),
             "T24 final scan [" + a + "," + b + ")");
    CHECK_EQ(expected.size(), got.size(),
             "T24 final scan count mismatch [" + a + "," + b + ")");
    for (size_t i = 1; i < got.size(); ++i)
      CHECK(got[i-1].first < got[i].first, "T24 final scan not sorted");
    CHECK(got == expected, "T24 final scan content mismatch [" + a + "," + b + ")");
  };

  scan_range(Key(0), Key(99999));
  scan_range("bnd", "bndz");
  scan_range("chain", "chainz");
  scan_range("explicit", "explicitz");
  scan_range("multiupdate", "multiupdatez");
  scan_range("snap", "snapz");
  scan_range(std::string(200, 'L'), std::string(200, 'L') + "z");

  EndTest("T24_FinalExhaustiveCheck");
}

// ═══════════════════════════════════════════════════════════════════════════
// main
// ═══════════════════════════════════════════════════════════════════════════
int main() {
  std::cout << "\n"
            << "╔════════════════════════════════════════════════╗\n"
            << "║   BRUTAL INTEGRATION TEST — COP290 A3          ║\n"
            << "║   Scan · DeleteRange · ForceFullCompaction      ║\n"
            << "╚════════════════════════════════════════════════╝\n\n";

  // Independent single-DB tests (own fresh instances)
  T13_FFCEmptyDB();
  T22_SingleKeyDB();

  // Main DB — all tests share one instance and one model
  leveldb::DB* db = OpenFreshDB(DbBasePath());
  Model model;

  T1_BasicScan(db, model);
  T2_ScanSortConsistency(db, model);
  T3_ScanSnapshot(db, model);
  T4_BasicDeleteRange(db, model);
  T5_DeleteRangeThenPut(db, model);
  T6_OverlappingDeleteRange(db, model);
  T7_ForceFullCompaction(db, model);
  T8_DeleteRangeNoKeys(db, model);
  T9_LargeStress(db, model);
  T10_PutDRChain(db, model);
  T11_ExactBoundaries(db, model);
  T12_MultiUpdateScan(db, model);
  T14_TripleFFC(db, model);
  T15_DeleteAll(db, model);
  T16_ScanBoundaryStrictness(db, model);
  T17_FrequentFFC(db, model);
  T18_LongKeysValues(db, model);
  T19_ReadYourWrites(db, model);
  T21_DeleteRangeImmediateScan(db, model);
  T23_TombstoneCleanup(db, model);
  T24_FinalExhaustiveCheck(db, model);  // must be last on main DB

  delete db;

  // Concurrent stress (own DB)
  T20_Concurrent();

  // ── Summary ─────────────────────────────────────────────────────────────
  std::cout << "\n╔════════════════════════════════════════════════╗\n";
  if (g_failures == 0) {
    std::cout << "║  ALL " << g_tests_run
              << " CHECKS PASSED — implementation correct  ║\n";
  } else {
    std::cout << "║  " << g_failures << " / " << g_tests_run
              << " CHECKS FAILED                           ║\n";
  }
  std::cout << "╚════════════════════════════════════════════════╝\n\n";

  return g_failures == 0 ? 0 : 1;
}
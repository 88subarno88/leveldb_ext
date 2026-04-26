// COP290 A3 self-check: exercises Scan, DeleteRange, and ForceFullCompaction
// with a deterministic prefix (assignment + clarifications) and a seeded random
// workload. Aligns with assignment 3.4: fresh DB (rm -rf) before Open.

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#include <cstdint>
#include <random>

#include "leveldb/db.h"
#include "leveldb/iterator.h"

namespace {

// Inclusive [lo, hi] from one std::mt19937 draw. std::uniform_int_distribution
// is not required to map engine output the same way on every stdlib, so
// bit-identical goldens (ans.txt) would otherwise differ across machines.
int UniformInRange(std::mt19937& gen, int lo, int hi) {
  if (lo > hi) {
    return lo;
  }
  const int64_t span64 =
      static_cast<int64_t>(hi) - static_cast<int64_t>(lo) + 1;
  if (span64 <= 0) {
    return lo;
  }
  const uint32_t span = static_cast<uint32_t>(span64);
  const uint32_t x = gen();
  return lo + static_cast<int>(x % span);
}

const char* DbPath() {
  const char* p = std::getenv("COP290_DB");
  return (p && p[0]) ? p : "/tmp/testdb";
}

int Fail(const char* what, const leveldb::Status& s) {
  std::cerr << "FAIL: " << what << ": " << s.ToString() << std::endl;
  return 1;
}

int Fail(const char* what) {
  std::cerr << "FAIL: " << what << std::endl;
  return 1;
}

// Expected [start_key, end_key) per LevelDB bytewise order — matches Iterator /
// Get semantics for string keys in this harness.
void ModelDeleteRangeInPlace(std::map<std::string, std::string>* m,
                            const std::string& start_key,
                            const std::string& end_key,
                            std::vector<std::string>* removed_out) {
  if (removed_out) {
    removed_out->clear();
  }
  if (start_key >= end_key) {
    return;
  }
  for (auto it = m->lower_bound(start_key);
       it != m->end() && it->first < end_key;) {
    if (removed_out) {
      removed_out->push_back(it->first);
    }
    it = m->erase(it);
  }
}

std::vector<std::pair<std::string, std::string>> ModelRangeScan(
    const std::map<std::string, std::string>& m,
    const std::string& start_key,
    const std::string& end_key) {
  std::vector<std::pair<std::string, std::string>> r;
  if (start_key >= end_key) {
    return r;
  }
  for (auto it = m.lower_bound(start_key);
       it != m.end() && it->first < end_key; ++it) {
    r.push_back({it->first, it->second});
  }
  return r;
}

bool IsStrictlySorted(const std::vector<std::pair<std::string, std::string>>& a) {
  for (size_t i = 1; i < a.size(); ++i) {
    if (a[i - 1].first >= a[i].first) {
      return false;
    }
  }
  return true;
}

// Deterministic checks matching clarifications: snapshot semantics, inverted
// range (empty), empty DeleteRange. Lexicographic order: "snap_a" < "snap_b" <
// "snap_c" for the snapshot exercise.
int RunDeterministic(leveldb::DB* db, std::ofstream& out) {
  leveldb::WriteOptions wopt;
  leveldb::ReadOptions ropt;

  const std::string k = "det_inv_z";
  const std::string l = "det_inv_a";
  {
    // start_key > end_key => empty result (Piazza: treat as empty range, not
    // error)
    if (k <= l) {
      std::cerr << "Internal checker error: inverted test keys not ordered\n";
      return 1;
    }
    std::vector<std::pair<std::string, std::string>> r;
    leveldb::Status s = db->Scan(ropt, k, l, &r);
    if (!s.ok()) {
      return Fail("Scan (inverted range)", s);
    }
    if (!r.empty()) {
      std::cerr << "FAIL: Scan inverted range expected 0 results\n";
      return 1;
    }
    out << "DET scan_inverted 0\n";
  }

  {
    leveldb::Status s = db->Put(wopt, "snap_a", "1");
    if (!s.ok()) {
      return Fail("Put snap_a", s);
    }
    s = db->Put(wopt, "snap_c", "3");
    if (!s.ok()) {
      return Fail("Put snap_c", s);
    }
    const leveldb::Snapshot* snap = db->GetSnapshot();
    s = db->Put(wopt, "snap_b", "2");
    if (!s.ok()) {
      return Fail("Put snap_b", s);
    }
    leveldb::ReadOptions sro;
    sro.snapshot = snap;
    std::vector<std::pair<std::string, std::string>> r;
    s = db->Scan(sro, "snap_a", "snap_c", &r);
    db->ReleaseSnapshot(snap);
    if (!s.ok()) {
      return Fail("Scan (snapshot range)", s);
    }
    // [snap_a, snap_c) visible at snapshot: a and c existed; b does not; end
    // is exclusive for c, so c is not returned.
    if (r.size() != 1 || r[0].first != "snap_a" || r[0].second != "1") {
      std::cerr
          << "FAIL: snapshot scan expected one pair (snap_a,1), got count="
          << r.size() << std::endl;
      return 1;
    }
    out << "DET scan_snapshot 1\n";
  }

  {
    leveldb::Status s = db->DeleteRange(wopt, "det_same", "det_same");
    if (!s.ok()) {
      return Fail("DeleteRange empty", s);
    }
    out << "DET delrange_empty ok\n";
  }
  return 0;
}

const char* RaceDbPath() {
  const char* p = std::getenv("COP290_RACE_DB");
  return (p && p[0]) ? p : "/tmp/testdb_race";
}

// Multi-threaded stress: concurrent Put / Get / Delete / Scan / DeleteRange /
// ForceFullCompaction on one DB, per clarifications (integrate with LevelDB
// thread safety, no golden output — pass/fail only). Uses a separate path from
// the single-threaded trace so `out.txt` is unchanged for `make run`.
// If `emit_race_line` is false, success is silent (main prints `ST: OK` /
// `RACE: OK` for `sample --concurrent` / `make test`).
int RunConcurrentRaceTest(bool emit_race_line) {
  {
    const std::string rmcmd =
        std::string("rm -rf '") + std::string(RaceDbPath()) + "'";
    std::system(rmcmd.c_str());
  }
  leveldb::DB* db = nullptr;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status ostatus = leveldb::DB::Open(options, RaceDbPath(), &db);
  if (!ostatus.ok()) {
    std::cerr << "RACE: open: " << ostatus.ToString() << std::endl;
    return 1;
  }

  // Tunable via env: COP290_RACE_THREADS (default 6), COP290_RACE_OPS (default
  // 2500 per thread).
  int n_threads = 6;
  int ops_each = 2500;
  if (const char* t = std::getenv("COP290_RACE_THREADS")) {
    n_threads = std::max(2, std::atoi(t));
  }
  if (const char* o = std::getenv("COP290_RACE_OPS")) {
    ops_each = std::max(200, std::atoi(o));
  }

  std::vector<std::thread> threads;
  std::atomic<bool> failed{false};

  for (int tid = 0; tid < n_threads; ++tid) {
    threads.emplace_back([db, n_threads, ops_each, tid, &failed]() {
      std::mt19937 gen(67u + 1009u * static_cast<unsigned>(tid) +
                       17u * static_cast<unsigned>(n_threads));
      const std::string pfx = "m" + std::to_string(tid);
      for (int i = 0; i < ops_each; ++i) {
        if (failed.load()) {
          return;
        }
        // Occasional cross-thread key to stress DeleteRange vs Put.
        int foreign = (tid + 1 + (i & 3)) % n_threads;
        std::string k1 =
            pfx + "k" + std::to_string(UniformInRange(gen, 0, 9999));
        std::string k2 = "m" + std::to_string(foreign) + "k" +
                         std::to_string(UniformInRange(gen, 0, 9999) ^ 1);
        std::string a = k1, b = k2;
        if (a > b) {
          std::swap(a, b);
        }
        std::string value_buf =
            pfx + "v" + std::to_string(UniformInRange(gen, 1, 100000));
        int op = UniformInRange(gen, 0, 99);
        leveldb::WriteOptions wo;
        leveldb::ReadOptions ro;
        leveldb::Status s;
        if (op < 46) {
          s = db->Put(wo, k1, value_buf);
        } else if (op < 66) {
          std::string g;
          s = db->Get(ro, k1, &g);
        } else if (op < 80) {
          s = db->Delete(wo, k1);
        } else if (op < 89) {
          std::vector<std::pair<std::string, std::string>> r;
          s = db->Scan(ro, a, b, &r);
        } else if (op < 96) {
          s = db->DeleteRange(wo, a, b);
        } else if (op < 99) {
          s = db->Get(ro, k1, &value_buf);
        } else {
          s = db->ForceFullCompaction();
        }
        if (!s.ok() && !s.IsNotFound()) {
          failed.store(true);
          return;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  if (failed.load()) {
    std::cerr << "RACE: a worker reported a non-OK non-NotFound status\n";
    delete db;
    return 1;
  }

  leveldb::Status f = db->ForceFullCompaction();
  if (!f.ok()) {
    std::cerr << "RACE: final ForceFullCompaction: " << f.ToString()
              << std::endl;
    delete db;
    return 1;
  }

  delete db;
  if (emit_race_line) {
    std::cout << "RACE: ok threads=" << n_threads
              << " ops_per_thread=" << ops_each << std::endl;
  }
  return 0;
}

}  // namespace

namespace {
bool ArgMatch(int argc, char** argv, const char* flag) {
  for (int i = 1; i < argc; ++i) {
    if (std::string_view(argv[i]) == flag) {
      return true;
    }
  }
  return false;
}
}  // namespace

int main(int argc, char** argv) {
  if (ArgMatch(argc, argv, "--race-only")) {
    std::cout << "ST: skip" << std::endl;
    return RunConcurrentRaceTest(true);
  }
  if (ArgMatch(argc, argv, "--help") || ArgMatch(argc, argv, "-h")) {
    std::cout
        << "usage: sample [--write] [--concurrent] [--race-only] [--help]\n"
        << "  (default)     single-threaded workload -> out.txt\n"
        << "  --write       write ans.txt (for update_golden)\n"
        << "  --concurrent  after ST run, also run multi-threaded RACE on "
           "COP290_RACE_DB (default /tmp/testdb_race)\n"
        << "  --race-only   only the concurrent stress (no out.txt; for "
           "make race)\n";
    return 0;
  }

  const bool do_concurrent = ArgMatch(argc, argv, "--concurrent");
  bool write_mode = false;
  for (int i = 1; i < argc; ++i) {
    if (std::string_view(argv[i]) == "--write") {
      write_mode = true;
    }
  }

  const std::string output_path = write_mode ? "ans.txt" : "out.txt";
  std::ofstream out(output_path, std::ios::out | std::ios::trunc);
  if (!out.is_open()) {
    std::cerr << "Failed to open output file: " << output_path << std::endl;
    return 1;
  }

  const char* const dbname = DbPath();
  {
    const std::string rmcmd = std::string("rm -rf '") + dbname + "'";
    std::system(rmcmd.c_str());
  }

  leveldb::DB* db = nullptr;
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(options, dbname, &db);
  if (!status.ok()) {
    std::cerr << "Failed to open database: " << status.ToString() << std::endl;
    return 1;
  }
  if (!db) {
    return 1;
  }

  leveldb::WriteOptions write_options;
  leveldb::ReadOptions read_options;

  constexpr unsigned kSeed = 67;
  constexpr int kNumOps = 10000;
  // protocol=4: portable UniformInRange (one mt19937 draw, uint32_t %);
  // protocol=3 used std::uniform_int_distribution (not cross-stdlib stable).
  out << "protocol=4 seed=" << kSeed << " ops=" << kNumOps << "\n";

  int drc = RunDeterministic(db, out);
  if (drc != 0) {
    delete db;
    return drc;
  }

  std::map<std::string, std::string> model; 
  std::mt19937 gen(kSeed);

  for (int i = 0; i < kNumOps; ++i) {
    int op = UniformInRange(gen, 0, 99);
    std::string k1 = "key" + std::to_string(UniformInRange(gen, 1, 10000));
    std::string k2 = "key" + std::to_string(UniformInRange(gen, 1, 10000));
    std::string v = "value" + std::to_string(UniformInRange(gen, 1, 100000));

    if (op < 50) {
      const leveldb::Status ps = db->Put(write_options, k1, v);
      if (!ps.ok()) {
        return Fail("Put (random op)", ps);
      }
      std::string got;
      const leveldb::Status gr = db->Get(read_options, k1, &got);
      if (!gr.ok()) {
        return Fail("Get after Put (read-your-writes)", gr);
      }
      if (got != v) {
        return Fail("Get after Put: value mismatch");
      }
      model[k1] = v;
    } else if (op < 70) {
      std::string value;
      status = db->Get(read_options, k1, &value);
      if (status.ok()) {
        const auto it = model.find(k1);
        if (it == model.end() || it->second != value) {
          return Fail("Get: value disagrees with reference model");
        }
        out << "GET " << k1 << " => " << value << "\n";
      } else if (status.IsNotFound()) {
        if (model.find(k1) != model.end()) {
          return Fail("Get: NotFound but key exists in reference model");
        }
      } else {
        return Fail("Get", status);
      }
    } else if (op < 85) {
      status = db->Delete(write_options, k1);
      if (!status.ok()) {
        return Fail("Delete", status);
      }
      {
        std::string dummy;
        const leveldb::Status g = db->Get(read_options, k1, &dummy);
        if (!g.IsNotFound()) {
          return Fail("Delete: key still visible to Get (expected NotFound)", g);
        }
      }
      if (const auto it = model.find(k1); it != model.end()) {
        model.erase(it);
      }
    } else if (op < 95) {
      const std::string& start_key = std::min(k1, k2);
      const std::string& end_key = std::max(k1, k2);
      const auto expected = ModelRangeScan(model, start_key, end_key);
      std::vector<std::pair<std::string, std::string>> scan_result;
      status = db->Scan(read_options, start_key, end_key, &scan_result);
      if (!status.ok()) {
        return Fail("Scan", status);
      }
      if (scan_result != expected) {
        return Fail("Scan: result does not match reference [start,end) slice");
      }
      if (!IsStrictlySorted(scan_result)) {
        return Fail("Scan: keys not strictly increasing");
      }
      for (const auto& p : scan_result) {
        if (p.first < start_key || p.first >= end_key) {
          return Fail("Scan: key outside [start, end) half-open range");
        }
        std::string g;
        const leveldb::Status gs = db->Get(read_options, p.first, &g);
        if (!gs.ok() || g != p.second) {
          return Fail("Scan vs Get: pair must match Get(key)", gs);
        }
      }
      out << "SCAN [" << start_key << ", " << end_key << ")";
      for (const auto& p : scan_result) {
        out << " " << p.first << "=>" << p.second;
      }
      out << "\n";
    } else if (op < 99) {
      const std::string& start_key = std::min(k1, k2);
      const std::string& end_key = std::max(k1, k2);
      std::vector<std::string> removed;
      ModelDeleteRangeInPlace(&model, start_key, end_key, &removed);
      status = db->DeleteRange(write_options, start_key, end_key);
      if (!status.ok()) {
        return Fail("DeleteRange", status);
      }
      for (const std::string& k : removed) {
        std::string d;
        const leveldb::Status dgs = db->Get(read_options, k, &d);
        if (!dgs.IsNotFound()) {
          return Fail("DeleteRange: in-range key still visible to Get (expected NotFound)", dgs);
        }
      }
    } else {
      status = db->ForceFullCompaction();
      if (!status.ok()) {
        return Fail("ForceFullCompaction", status);
      }
      for (int ki = 1; ki <= 10000; ++ki) {
        const std::string k = "key" + std::to_string(ki);
        std::string gv;
        const leveldb::Status gs = db->Get(read_options, k, &gv);
        const auto mit = model.find(k);
        if (mit == model.end()) {
          if (!gs.IsNotFound()) {
            return Fail("After ForceFullCompaction: key should be absent (model)", gs);
          }
        } else {
          if (!gs.ok() || gv != mit->second) {
            return Fail("After ForceFullCompaction: Get disagrees with model", gs);
          }
        }
      }
    }
  }

  delete db;
  out.close();

  if (write_mode) {
    std::cout << "Wrote " << output_path << "\n";
  }
  // stdout only — not written to out.txt. `make test` runs `sample
  // --concurrent` and expects the last two lines to be `ST: OK` then
  // `RACE: OK` after both stages succeed.
  if (do_concurrent && !write_mode) {
    const int rrc = RunConcurrentRaceTest(false);
    if (rrc != 0) {
      return rrc;
    }
    std::cout << "ST: OK" << std::endl;
    std::cout << "RACE: OK" << std::endl;
    return 0;
  }
  std::cout << "ST: OK" << std::endl;
  std::cout << "RACE: skip" << std::endl;
  return 0;
}
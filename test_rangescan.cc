#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <cassert>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"


// opens the DB and crashes safely  if something goes wrong.
leveldb::DB* OpenDB(const std::string& path) {
  leveldb::Options options;
  options.create_if_missing = true; 
  leveldb::DB* db;
  leveldb::Status status = leveldb::DB::Open(options, path, &db);
  if (!status.ok()) {
    std::cerr << "Couldn't open the database: " << status.ToString() << std::endl;
    exit(1);
  }
  return db;
}

// print out scan results to see what's going on.
void PrintRes(const std::vector<std::pair<std::string, std::string>>& results) {
  if (results.empty()) {
    std::cout << "  results empty in PrintRes fx \n";
    return;
  }
  for (const auto& kv : results) {
    std::cout << "  [" << kv.first << "] => " << kv.second << "\n";
  }
}


void TestBasicScan(leveldb::DB* db) {
  std::cout << "\n TEST 1: The Basics \n";
  std::cout << "Checking if a  scan works correctly  \n";

  leveldb::WriteOptions write_opts;
  db->Put(write_opts, "apple",  "fruit_1");
  db->Put(write_opts, "banana", "fruit_2");
  db->Put(write_opts, "cherry", "fruit_3");
  db->Put(write_opts, "date",   "fruit_4");
  db->Put(write_opts, "elderberry", "fruit_5");

  // want  scan from "banana" up to "date". 
  std::vector<std::pair<std::string, std::string>> results;
  leveldb::Status status = db->Scan(leveldb::ReadOptions(), "banana", "date", &results);
  //verify
  assert(status.ok());
  assert(results.size() == 2); 
  assert(results[0].first == "banana" && results[0].second == "fruit_2");
  assert(results[1].first == "cherry" && results[1].second == "fruit_3");

  PrintRes(results);
  std::cout << "PASSED\n";
}


void TestEmptyRange(leveldb::DB* db) {
  std::cout << "\n  TEST 2: Searching for ghosts\n";
  std::cout << "What happens if  scan a range that doesn't exist? \n";

  std::vector<std::pair<std::string, std::string>> results;

  // There are no items starting with 'm' through 'p' in our database now.
  leveldb::Status status = db->Scan(leveldb::ReadOptions(), "mango", "peach", &results);
  //verify
  assert(status.ok());
  assert(results.empty()); 
  PrintRes(results);
  std::cout << "PASSED\n";
}


void TestStartEndSame(leveldb::DB* db) {
  std::cout << "\n  TEST 3: Zero-length scan \n";
  std::cout << "Testing when the start and end keys are exactly the same \n";

  std::vector<std::pair<std::string, std::string>> results;
  leveldb::Status status = db->Scan(leveldb::ReadOptions(), "banana", "banana", &results);
  //verify
  assert(status.ok());
  assert(results.empty()); 
  PrintRes(results);
  std::cout << "PASSED\n";
}


void TestEndKeyExclu(leveldb::DB* db) {
  std::cout << "\n TEST 4: Checking the boundary \n";
  std::cout << "Making sure the 'end' key isnot included in the results  \n";

  std::vector<std::pair<std::string, std::string>> results;
  
  // cherry is the end key, so the scan should stop before it includes cherry.
  leveldb::Status status = db->Scan(leveldb::ReadOptions(), "banana", "cherry", &results);
  //verify
  assert(status.ok());
  assert(results.size() == 1);
  assert(results[0].first == "banana");

  PrintRes(results);
  std::cout << "PASSED\n";
}


void TestOverwrite(leveldb::DB* db) {
  std::cout << "\n  TEST 5: Overwriting data \n";
  std::cout << "If we update a key, does the scan give us the old or new value?\n";

  leveldb::WriteOptions write_opts;
  db->Put(write_opts, "key_x", "old_value");
  db->Put(write_opts, "key_x", "new_value");
  std::vector<std::pair<std::string, std::string>> results;
  leveldb::Status status = db->Scan(leveldb::ReadOptions(), "key_x", "key_y", &results);
  //verify
  assert(status.ok());
  assert(results.size() == 1);
  assert(results[0].first == "key_x");
  assert(results[0].second == "new_value"); 
  PrintRes(results);
  std::cout << "PASSED\n";
}


void TestDeletedKeysImVisible(leveldb::DB* db) {
  std::cout << "\n TEST 6: Ignoring deleted items\n";
  std::cout << "If we delete a key, the scan should skip right over it\n";

  leveldb::WriteOptions write_opts;
  db->Put(write_opts, "z_key1", "value1");
  db->Put(write_opts, "z_key2", "value2");
  db->Put(write_opts, "z_key3", "value3");
  
  // delete the middle one
  db->Delete(write_opts, "z_key2"); 

  std::vector<std::pair<std::string, std::string>> results;
  leveldb::Status status = db->Scan(leveldb::ReadOptions(), "z_key1", "z_key4", &results);
  //verify
  assert(status.ok());
  assert(results.size() == 2);              
  assert(results[0].first == "z_key1");
  assert(results[1].first == "z_key3");
  PrintRes(results);
  std::cout << "PASSED\n";
}


void TestLargeScan(leveldb::DB* db) {
  std::cout << "\n TEST 7: A bigger test (1000 keys) \n";
  
  leveldb::WriteOptions write_opts;

  // putting 1000 keys into the database. 
  //  snprintf just to make the numbers pad with zeros (like "num_0042").
  for (int i = 0; i < 1000; i++) {
    char key[16], val[16];
    snprintf(key, sizeof(key), "num_%04d", i);
    snprintf(val, sizeof(val), "val_%04d", i);
    db->Put(write_opts, key, val);
  }

  // trying to get  100 of them.
  std::vector<std::pair<std::string, std::string>> results;
  leveldb::Status status = db->Scan(leveldb::ReadOptions(), "num_0100", "num_0200", &results);
  //verify
  assert(status.ok());
  assert(results.size() == 100);
  assert(results.front().first == "num_0100");
  assert(results.back().first  == "num_0199"); 
  std::cout << "  Got exactly " << results.size() << " results.\n";
  std::cout << "  First item: " << results.front().first << "\n";
  std::cout << "  Last item:  " << results.back().first  << "\n";
  std::cout << "PASSED!\n";
}


void TestScanAll(leveldb::DB* db) {
  std::cout << "\n TEST 8: Scanning everything \n";

  std::vector<std::pair<std::string, std::string>> results;
  
  //scan from an empty string up to the highest possible character value.
  std::string huge_key(8, '\xff'); 
  leveldb::Status status = db->Scan(leveldb::ReadOptions(), "", huge_key, &results);

  assert(status.ok());
  std::cout << "  Total keys currently sitting in the Database: " << results.size() << "\n";
  std::cout << "PASSED\n";
}


void TestScanAfterCompaction(leveldb::DB* db) {
  std::cout << "\n TEST 9: Scanning from the hard drive \n";
  std::cout << "Making sure Scan works after LevelDB flushes memory to disk ...\n";

  leveldb::WriteOptions write_opts;

  // nserting a lot of data to force the database to write to disk.
  for (int i = 0; i < 5000; i++) {
    char key[20], val[20];
    snprintf(key, sizeof(key), "compact_key_%04d", i);
    snprintf(val, sizeof(val), "compact_val_%04d", i);
    db->Put(write_opts, key, val);
  }

  // This command forces LevelDB to clean up and flush everything to disk.
  db->CompactRange(nullptr, nullptr);

  std::vector<std::pair<std::string, std::string>> results;
  leveldb::Status status = db->Scan(leveldb::ReadOptions(),
                                    "compact_key_1000",
                                    "compact_key_2000",
                                    &results);
  //verify
  assert(status.ok());
  assert(results.size() == 1000);
  std::cout << "  Got " << results.size() << " results back from disk.\n";
  std::cout << "PASSED\n";
}


int main() {
  const std::string db_path = "/tmp/testdb_scan";

  // clear the test database folder before  start, 
  // so that old data from previous runs doesn't mess up  tests.
  system(("rm -rf " + db_path).c_str());

  leveldb::DB* db = OpenDB(db_path);

  TestBasicScan(db);
  TestEmptyRange(db);
  TestStartEndSame(db);
  TestEndKeyExclu(db);
  TestOverwrite(db);
  TestDeletedKeysImVisible(db);
  TestLargeScan(db);
  TestScanAll(db);
  TestScanAfterCompaction(db);

  delete db;

  std::cout << " <ALL SCAN TESTS PASSED SUCCESSFULLY!> \n";
  
  return 0;
}
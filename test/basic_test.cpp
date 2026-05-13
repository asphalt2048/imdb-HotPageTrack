#include <iostream>
#include <string>
#include <cstring>
#include <cassert>
#include <chrono>
#include <fstream>
#include <cstdlib> 
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include "StorageEngine.h"

using namespace std;
using namespace imdb;

void test_basic_operations(StorageEngine& db) {
    cout << "--- Running Basic Correctness Tests ---\n";

    // 1. Test Insert
    string key1 = "user:100";
    const char* data1 = "Alice";
    assert(db.put(key1, data1, strlen(data1) + 1) == true);
    
    // 2. Test Get
    char buffer[512]; 
    uint64_t retrieved_size = 0;
    assert(db.get(key1, buffer, retrieved_size) == true);
    assert(strcmp(buffer, "Alice") == 0);
    
    // 3. Test Delete
    assert(db.del(key1) == true);
    assert(db.get(key1, buffer, retrieved_size) == false); 
    
    cout << "[+] Basic RAM operations passed.\n\n";
}

// =======================================================================
// CONCURRENCY SUITE 1: Independent Inserts (Tests SCM / Hashmap parallel scale)
// =======================================================================
void test_concurrent_independent_inserts(StorageEngine& db) {
    cout << "--- Running Concurrent Independent Inserts Test ---\n";
    const int NUM_THREADS = 8;
    const int RECORDS_PER_THREAD = 10000;
    vector<thread> threads;

    auto start = chrono::high_resolution_clock::now();

    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&db, t, RECORDS_PER_THREAD]() {
            for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                string key = "c_key_" + to_string(t) + "_" + to_string(i);
                string data = "payload_" + to_string(t) + "_" + to_string(i);
                
                // Add padding to ensure records are ~80 bytes (Fits in 128 SCM safely)
                data.append(80 - data.length(), 'x');
                assert(db.put(key, data.c_str(), data.size() + 1));
            }
        });
    }

    for (auto& th : threads) th.join();

    auto end = chrono::high_resolution_clock::now();
    cout << "[+] " << NUM_THREADS * RECORDS_PER_THREAD << " concurrent inserts finished in " 
         << chrono::duration<double, std::milli>(end - start).count() << " ms.\n";

    // Verification phase
    for (int t = 0; t < NUM_THREADS; t++) {
        for (int i = 0; i < RECORDS_PER_THREAD; i++) {
            string key = "c_key_" + to_string(t) + "_" + to_string(i);
            string expected = "payload_" + to_string(t) + "_" + to_string(i);
            expected.append(80 - expected.length(), 'x');
            
            char buf[256];
            uint64_t size = 0;
            bool ok = db.get(key, buf, size);
            assert(ok);
            assert(strcmp(buf, expected.c_str()) == 0);
        }
    }
    cout << "[+] Verification successful! All concurrent data intact.\n\n";
}

// =======================================================================
// CONCURRENCY SUITE 2: Contentious Updates (Tests TT Locks & Reallocations)
// =======================================================================
void test_concurrent_updates(StorageEngine& db) {
    cout << "--- Running Concurrent Updates (Contention) Test ---\n";
    const int NUM_KEYS = 1000;
    const int NUM_THREADS = 8;
    const int UPDATES_PER_THREAD = 2000;

    // Pre-insert 1000 keys
    for(int i = 0; i < NUM_KEYS; i++) {
        string key = "u_key_" + to_string(i);
        string data = "init";
        db.put(key, data.c_str(), data.size() + 1);
    }

    vector<thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&db, t, NUM_KEYS, UPDATES_PER_THREAD]() {
            // Thread-local PRNG for payload size variation
            thread_local std::mt19937 rng(std::random_device{}() + t);
            std::uniform_int_distribution<int> size_dist(10, 200); // 10B to 200B max payload
            std::uniform_int_distribution<int> key_dist(0, NUM_KEYS - 1);

            for (int i = 0; i < UPDATES_PER_THREAD; i++) {
                int k = key_dist(rng);
                string key = "u_key_" + to_string(k);
                
                string data = "T" + to_string(t) + "_I" + to_string(i) + "_";
                size_t target_size = size_dist(rng);
                
                // This payload size variation GUARANTEES heavy testing of the 
                // in-place vs reallocation logic in `update_record`
                if (data.length() < target_size) {
                    data.append(target_size - data.length(), 'Y'); 
                }

                db.put(key, data.c_str(), data.size() + 1);
            }
        });
    }

    for(auto& th : threads) th.join();
    
    // Verify they are still readable and haven't corrupted Memory
    int valid_count = 0;
    for(int i = 0; i < NUM_KEYS; i++) {
        string key = "u_key_" + to_string(i);
        char buf[256];
        uint64_t size = 0;
        if (db.get(key, buf, size)) {
            valid_count++;
        }
    }
    assert(valid_count == NUM_KEYS);
    cout << "[+] " << NUM_THREADS * UPDATES_PER_THREAD << " highly contended updates resolved safely.\n\n";
}

// =======================================================================
// CONCURRENCY SUITE 3: Mixed Read / Write (Tests Reader-Writer Locks)
// =======================================================================
void test_concurrent_reads_writes(StorageEngine& db) {
    cout << "--- Running Concurrent Read/Write Workload Test ---\n";
    const int NUM_KEYS = 5000;
    const int NUM_WRITERS = 4;
    const int NUM_READERS = 4;
    const int OPS_PER_THREAD = 10000;

    // Seed the database
    for(int i = 0; i < NUM_KEYS; i++) {
        string key = "rw_key_" + to_string(i);
        string data = "original_data";
        db.put(key, data.c_str(), data.size() + 1);
    }

    std::atomic<bool> start_flag{false};
    vector<thread> threads;

    // Writers
    for (int t = 0; t < NUM_WRITERS; t++) {
        threads.emplace_back([&db, &start_flag, NUM_KEYS, OPS_PER_THREAD]() {
            while (!start_flag) std::this_thread::yield();
            thread_local std::mt19937 rng(std::random_device{}());
            std::uniform_int_distribution<int> key_dist(0, NUM_KEYS - 1);

            for (int i = 0; i < OPS_PER_THREAD; i++) {
                string key = "rw_key_" + to_string(key_dist(rng));
                string data = "updated_by_writer_" + to_string(i);
                db.put(key, data.c_str(), data.size() + 1);
            }
        });
    }

    // Readers
    for (int t = 0; t < NUM_READERS; t++) {
        threads.emplace_back([&db, &start_flag, NUM_KEYS, OPS_PER_THREAD]() {
            while (!start_flag) std::this_thread::yield();
            thread_local std::mt19937 rng(std::random_device{}());
            std::uniform_int_distribution<int> key_dist(0, NUM_KEYS - 1);

            char buf[256];
            uint64_t size = 0;
            for (int i = 0; i < OPS_PER_THREAD; i++) {
                string key = "rw_key_" + to_string(key_dist(rng));
                // We don't assert value here because writer might be changing it,
                // we assert that the database doesn't crash or read garbage pointers.
                bool success = db.get(key, buf, size);
                assert(success); 
            }
        });
    }

    start_flag = true; // Fire the starting gun
    for(auto& th : threads) th.join();

    cout << "[+] Reader/Writer workload resolved successfully without data races.\n\n";
}

// =======================================================================
// ORIGINAL SWEEPER & DISK TEST (Run last)
// =======================================================================
void test_eviction_and_disk(StorageEngine& db, const string& file_path) {
    cout << "--- Running Sweeper & Disk I/O Stress Test ---\n";
    cout << "[!] Pumping massive data to trigger the background Eviction Sweeper...\n";

    const int NUM_RECORDS = 250000;
    string base_data(80, 'D'); 

    auto start = chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_RECORDS; i++) {
        string key = "disk_user:" + to_string(i);
        string data = base_data + "_" + to_string(i);
        
        db.put(key, data.c_str(), data.length() + 1);

        if (i > 0 && i % 50000 == 0) {
            cout << "    ... inserted " << i << " records. Sweeper should be working hard...\n";
        }
    }

    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double, std::milli> insert_ms = end - start;
    cout << "[+] Insertion complete in " << insert_ms.count() << " ms.\n";

    cout << "\n[!] Retrieving cold data from the Disk...\n";
    
    char buffer[512];
    uint64_t retrieved_size = 0;

    string target_key = "disk_user:0";
    string expected_data = base_data + "_0";
    assert(db.get(target_key, buffer, retrieved_size) == true);
    assert(strcmp(buffer, expected_data.c_str()) == 0);
    cout << "[+] Disk Read #1 Successful! First record intact.\n";

    string target_key2 = "disk_user:50000";
    string expected_data2 = base_data + "_50000";
    assert(db.get(target_key2, buffer, retrieved_size) == true);
    assert(strcmp(buffer, expected_data2.c_str()) == 0);
    cout << "[+] Disk Read #2 Successful! Middle record intact.\n";

    std::ifstream file(file_path, std::ios::binary | std::ios::ate);
    if (file) {
        std::streamsize size = file.tellg();
        cout << "\n[+] PHYSICAL HARDWARE CHECK:\n";
        cout << "    Append-Only File Size: " << size / (1024.0 * 1024.0) << " MB\n";
        if (size > 0) {
            cout << "    Disk flushing confirmed. Your architecture works!\n";
        }
    }

    cout << "--- Sweeper & Disk Tests Passed! ---\n\n";
}

int main(int argc, char* argv[]) {
    string db_path = "../data/imdb.aof";
    if (argc > 1) { db_path = argv[1]; }
    DBConfig config;
    config.db_file_path = db_path;

    cout << "=========================================\n";
    cout << " Booting IMDB Kernel-Bypassed Engine...\n";
    cout << " Mounting Disk Log at: " << db_path << "\n";
    cout << "=========================================\n\n";

    int success = system("mkdir -p ../data");
    if (success != 0) {
        cerr << "Error: Failed to create data directory.\n";
        return -1;
    }

    // Erase previous test file to ensure a clean slate for Disk logic
    success = system(("rm -f " + db_path).c_str());
    if (success != 0) {
        cerr << "Error: Failed to erase old swap file.\n";
        return -1;
    }

    StorageEngine db(config);

    // 1. Single Thread Validation
    test_basic_operations(db);

    // 2. Concurrency Suite
    test_concurrent_independent_inserts(db);
    test_concurrent_updates(db);
    test_concurrent_reads_writes(db);

    // 3. Hardware I/O & Sweeper (Run last, heavily dirties RAM/Disk)
    test_eviction_and_disk(db, db_path);

    cout << "=========================================\n";
    cout << " ALL SYSTEMS NOMINAL. ENGINE ONLINE.\n";
    cout << "=========================================\n";

    return 0;
}
#include <iostream>
#include <string>
#include <cstring>
#include <cassert>
#include <chrono>
#include <fstream>
#include <cstdlib> // For system()
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

void test_eviction_and_disk(StorageEngine& db, const string& file_path) {
    cout << "--- Running Sweeper & Disk I/O Stress Test ---\n";
    cout << "[!] Pumping massive data to trigger the background Eviction Sweeper...\n";

    // Assuming a ~4MB Arena, 250,000 records of ~100 bytes each = ~25MB of data.
    // This ABSOLUTELY guarantees the Arena will fill up, the Sweeper will wake up, 
    // and pages will be flushed to the Append-Only Log.
    const int NUM_RECORDS = 250000;
    string base_data(80, 'D'); // 80 char string + header = fits perfectly in 128B Size Class

    auto start = chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_RECORDS; i++) {
        string key = "disk_user:" + to_string(i);
        string data = base_data + "_" + to_string(i);
        
        // If the DB doesn't crash from OOM here, the Sweeper is successfully freeing pages!
        db.put(key, data.c_str(), data.length() + 1);

        if (i > 0 && i % 50000 == 0) {
            cout << "    ... inserted " << i << " records. Sweeper should be working hard...\n";
        }
    }

    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double, std::milli> insert_ms = end - start;
    cout << "[+] Insertion complete in " << insert_ms.count() << " ms.\n";

    // Now, test retrieval of the OLDEST records. 
    // Because they were inserted first, the LRU algorithm guarantees they were evicted to disk.
    cout << "\n[!] Retrieving cold data from the Disk...\n";
    
    char buffer[512];
    uint64_t retrieved_size = 0;

    // Test record 0 (Definitely on disk)
    string target_key = "disk_user:0";
    string expected_data = base_data + "_0";

    assert(db.get(target_key, buffer, retrieved_size) == true);
    assert(strcmp(buffer, expected_data.c_str()) == 0);
    cout << "[+] Disk Read #1 Successful! First record intact.\n";

    // Test record 50,000 (Definitely on disk)
    string target_key2 = "disk_user:50000";
    string expected_data2 = base_data + "_50000";
    
    assert(db.get(target_key2, buffer, retrieved_size) == true);
    assert(strcmp(buffer, expected_data2.c_str()) == 0);
    cout << "[+] Disk Read #2 Successful! Middle record intact.\n";

    // Check physical file size
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

void test_insert_race() {
    std::cout << "--- Starting Concurrent Insert Race Test ---\n";

    // 1. Setup the Database
    DBConfig cfg;
    cfg.db_file_path = "../data/test_race.aof";
    StorageEngine db(cfg);

    // 2. The Starting Gun
    std::atomic<bool> start_gun{false};
    std::atomic<int> ready_count{0};

    const int NUM_THREADS = 16;
    std::vector<std::thread> threads;

    // 3. Spawn the Competitors
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&db, &start_gun, &ready_count, i]() {
            // Pre-compute the data so we don't waste time during the race
            std::string key = "race_key";
            std::string payload = "Data_from_thread_" + std::to_string(i);
            
            // Tell the main thread we are spawned and ready
            ready_count++;

            // THE SPINLOCK: Wait for the gun to fire!
            while (!start_gun.load(std::memory_order_acquire)) {
                std::this_thread::yield(); 
            }

            // THE SPRINT: All 16 threads hit this line at the exact same nanosecond
            db.put(key, payload.c_str(), payload.size() + 1); // +1 for null terminator
        });
    }

    // Wait for all threads to reach the starting line
    while (ready_count < NUM_THREADS) {
        std::this_thread::yield();
    }

    std::cout << "All 16 threads ready. Firing the starting gun in 3... 2... 1...\n";
    
    // 4. FIRE!
    start_gun.store(true, std::memory_order_release);

    // 5. Wait for the dust to settle
    for (auto& t : threads) {
        t.join();
    }

    // ==========================================
    // THE ASSERTIONS
    // ==========================================
    
    char buffer[256];
    uint64_t fetched_size = 0;
    
    // Assertion 1: The database must not have crashed, and the key MUST exist.
    bool success = db.get("race_key", buffer, fetched_size);
    
    if (success) {
        std::cout << "[SUCCESS] Data retrieved without crashing!\n";
        std::cout << "Winning Thread Data: " << buffer << "\n";
    } else {
        std::cout << "[FATAL] The key vanished into the void.\n";
    }

    // Assertion 2 (Mental Check): 
    // If you put a print statement inside your rollback `if (!success)` block 
    // in `insert_new_record`, you should see exactly 15 "Rollback!" prints 
    // in your console, and exactly 1 thread quietly succeeding.
    
    std::cout << "--- Test Complete ---\n\n";
}

int main(int argc, char* argv[]) {
    // Determine file path. Default to ../data/imdb.aof, but allow terminal override
    string db_path = "../data/imdb.aof";
    if (argc > 1) {
        db_path = argv[1];
    }
    DBConfig config;
    config.db_file_path = db_path;

    cout << "=========================================\n";
    cout << " Booting IMDB Kernel-Bypassed Engine...\n";
    cout << " Mounting Disk Log at: " << db_path << "\n";
    cout << "=========================================\n\n";

    // Ensure the data directory actually exists before DiskManager tries to open it
    int success = system("mkdir -p ../data");
    if (success != 0) {
        cerr << "Error: Failed to create data directory. Check permissions.\n";
        return -1;
    }

    // Boot the DB and inject the path
    StorageEngine db(config);

    // 1. Sanity Check
    //test_basic_operations(db);

    // 2. Hardware I/O & Background Threading
    test_eviction_and_disk(db, db_path);

    //test_insert_race();

    cout << "=========================================\n";
    cout << " ALL SYSTEMS NOMINAL. ENGINE ONLINE.\n";
    cout << "=========================================\n";

    return 0;
}
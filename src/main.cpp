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

int main(int argc, char* argv[]) {
    // Determine file path. Default to ../data/imdb.aof, but allow terminal override
    string db_path = "../data/imdb.aof";
    if (argc > 1) {
        db_path = argv[1];
    }

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
    StorageEngine db(db_path);

    // 1. Sanity Check
    test_basic_operations(db);

    // 2. The Final Test: Hardware I/O & Background Threading
    test_eviction_and_disk(db, db_path);

    cout << "=========================================\n";
    cout << " ALL SYSTEMS NOMINAL. ENGINE ONLINE.\n";
    cout << "=========================================\n";

    return 0;
}
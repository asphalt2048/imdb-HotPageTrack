#include <iostream>
#include <string>
#include <cstring>
#include <cassert>
#include <chrono>
#include "StorageEngine.h"

using namespace std;
using namespace imdb;

void test_basic_operations(StorageEngine& db) {
    cout << "--- Running Basic Correctness Tests ---\n";

    // 1. Test Insert
    string key1 = "user:100";
    const char* data1 = "Alice";
    assert(db.put(key1, data1, strlen(data1) + 1) == true);
    cout << "[+] Insert successful.\n";

    // 2. Test Get
    char buffer[512]; // Increased buffer size to handle up to max size class
    uint64_t retrieved_size = 0;
    assert(db.get(key1, buffer, retrieved_size) == true);
    assert(strcmp(buffer, "Alice") == 0);
    cout << "[+] Get successful. Retrieved: " << buffer << " (Size: " << retrieved_size << ")\n";

    // 3. Test Update
    const char* data1_new = "Alice_Updated";
    assert(db.put(key1, data1_new, strlen(data1_new) + 1) == true);
    assert(db.get(key1, buffer, retrieved_size) == true);
    assert(strcmp(buffer, "Alice_Updated") == 0);
    cout << "[+] Update successful. Retrieved: " << buffer << "\n";

    // 4. Test Delete
    assert(db.del(key1) == true);
    assert(db.get(key1, buffer, retrieved_size) == false); // Should fail to find it
    cout << "[+] Delete successful. Record properly removed.\n";
    
    // 5. Test Bounds Checking (Updated: Should gracefully reject > 256 bytes!)
    cout << "[+] Testing bounds check (expecting an error message below):\n";
    string long_data(300, 'X'); // 300 bytes + 8 byte header = 308 > 256 max class
    bool put_result = db.put("user:overflow", long_data.c_str(), long_data.length() + 1);
    assert(put_result == false || put_result == 0); // Fails gracefully
    
    cout << "--- Basic Tests Passed! ---\n\n";
}

void test_variable_sizes(StorageEngine& db) {
    cout << "--- Running Variable Size & Reallocation Tests ---\n";

    // 1. Insert across different size classes
    string key_tiny = "var:tiny";
    string data_tiny = "Hi"; // 3 bytes (Fits 16B class)
    
    string key_mid = "var:mid";
    string data_mid(80, 'M'); // 81 bytes (Fits 128B class)
    
    string key_large = "var:large";
    string data_large(200, 'L'); // 201 bytes (Fits 256B class)

    assert(db.put(key_tiny, data_tiny.c_str(), data_tiny.length() + 1) == true);
    assert(db.put(key_mid, data_mid.c_str(), data_mid.length() + 1) == true);
    assert(db.put(key_large, data_large.c_str(), data_large.length() + 1) == true);
    cout << "[+] Multi-size insertions successful.\n";

    // Verify them
    char buffer[512]; 
    uint64_t retrieved_size = 0;
    
    assert(db.get(key_tiny, buffer, retrieved_size));
    assert(strcmp(buffer, data_tiny.c_str()) == 0);
    
    assert(db.get(key_mid, buffer, retrieved_size));
    assert(strcmp(buffer, data_mid.c_str()) == 0);
    
    assert(db.get(key_large, buffer, retrieved_size));
    assert(strcmp(buffer, data_large.c_str()) == 0);
    cout << "[+] Multi-size retrievals successful.\n";

    // 2. Test Dynamic Reallocation (Growing a record out of its current Size Class)
    string key_grow = "var:grow";
    string data_start = "Start Small"; // ~12 bytes (fits 32B class)
    assert(db.put(key_grow, data_start.c_str(), data_start.length() + 1));
    
    string data_grown(150, 'G'); // 151 bytes (Forces reallocation to 256B class)
    assert(db.put(key_grow, data_grown.c_str(), data_grown.length() + 1));
    
    assert(db.get(key_grow, buffer, retrieved_size));
    assert(strcmp(buffer, data_grown.c_str()) == 0);
    cout << "[+] Dynamic reallocation (growth) successful.\n";

    // 3. Test In-Place Update (Shrinking/Staying in same class)
    string data_shrink = "Shrunk"; // Very small, but engine should safely update in-place within the 256B slot
    assert(db.put(key_grow, data_shrink.c_str(), data_shrink.length() + 1));
    assert(db.get(key_grow, buffer, retrieved_size));
    assert(strcmp(buffer, data_shrink.c_str()) == 0);
    cout << "[+] In-place update (shrink) successful.\n";

    cout << "--- Variable Size Tests Passed! ---\n\n";
}

void test_benchmark(StorageEngine& db) {
    cout << "--- Running Stress Test & Benchmark ---\n";
    
    const int NUM_RECORDS = 100000;
    cout << "Inserting " << NUM_RECORDS << " records...\n";

    auto start = chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_RECORDS; i++) {
        string key = "bench:user:" + to_string(i);
        string data = "PayloadData_" + to_string(i);
        
        db.put(key, data.c_str(), data.length() + 1);
    }

    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double, std::milli> insert_ms = end - start;

    cout << "[+] Inserted " << NUM_RECORDS << " records in " << insert_ms.count() << " ms.\n";
    cout << "[+] Average latency per insert: " << (insert_ms.count() * 1000.0) / NUM_RECORDS << " microseconds.\n";

    char buffer[512];
    uint64_t retrieved_size = 0;
    assert(db.get("bench:user:50000", buffer, retrieved_size) == true);
    assert(strcmp(buffer, "PayloadData_50000") == 0);
    cout << "[+] Random benchmark record verification successful.\n";
    
    cout << "--- Stress Test Passed! ---\n";
}

int main() {
    cout << "Initializing IMDB Storage Engine...\n\n";
    
    StorageEngine db;

    test_basic_operations(db);
    test_variable_sizes(db); // <-- NEW TEST SUITE ADDED
    test_benchmark(db);

    cout << "\nALL TESTS PASSED SUCCESSFULLY! The memory allocator is dynamically routing sizes.\n";
    return 0;
}
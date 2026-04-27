#include <iostream>
#include <string>
#include <cstring>
#include <cassert>
#include <chrono>
#include "StorageEngine.h" // Or whatever you named your header

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
    char buffer[64];
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
    
    // 5. Test Bounds Checking (Should gracefully reject > 56 bytes)
    cout << "[+] Testing bounds check (expecting an error message below):\n";
    string long_data(100, 'X'); 
    bool put_result = db.put("user:overflow", long_data.c_str(), 100);
    assert(put_result == false || put_result == 0); // Fails gracefully or exits
    
    cout << "--- Basic Tests Passed! ---\n\n";
}

void test_benchmark(StorageEngine& db) {
    cout << "--- Running Stress Test & Benchmark ---\n";
    
    const int NUM_RECORDS = 100000;
    cout << "Inserting " << NUM_RECORDS << " records...\n";

    // Start Timer
    auto start = chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_RECORDS; i++) {
        string key = "bench:user:" + to_string(i);
        string data = "PayloadData_" + to_string(i);
        
        // This will force Arena to allocate many pages and translation_table to grow!
        db.put(key, data.c_str(), data.length() + 1);
    }

    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double, std::milli> insert_ms = end - start;

    cout << "[+] Inserted " << NUM_RECORDS << " records in " << insert_ms.count() << " ms.\n";
    cout << "[+] Average latency per insert: " << (insert_ms.count() * 1000.0) / NUM_RECORDS << " microseconds.\n";

    // Verify a random record from the benchmark
    char buffer[64];
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
    test_benchmark(db);

    cout << "\nALL TESTS PASSED SUCCESSFULLY! The V0.1 Memory Engine is rock solid.\n";
    return 0;
}
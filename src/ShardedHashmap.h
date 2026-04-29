#pragma once

#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <string>
#include <vector>
#include <functional>

namespace imdb {
// The number of shards, better be a prime number
#define SHARDS_COUNT 17

class ShardedHashMap{
private:
    struct Shard {
        std::shared_mutex rw_lock;
        std::unordered_map<std::string, uint64_t> map;
    };

    // Array of shards. 
    std::vector<Shard> shards;
    size_t num_shards;

    size_t get_shard_idx(const std::string& key) const {
        return std::hash<std::string>{}(key) % num_shards;
    }

public:
    ShardedHashMap(size_t shards_count = SHARDS_COUNT) : num_shards(shards_count){
        // Initialize the vector with default Shards
        shards = std::vector<Shard>(num_shards);
    }

    // Returns true if inserted. Returns false and populates out_existing_id if data all ready exists
    bool insert(const std::string& key, uint64_t logical_id, uint64_t &out_existing_id){
        size_t idx = get_shard_idx(key);

        std::unique_lock<std::shared_mutex> write_lock(shards[idx].rw_lock);

        auto res = shards[idx].map.insert({key, logical_id});
        if(!res.second){
            out_existing_id = res.first->second;
            return false;
        }
        return res.second;
    }

    // Updates existing or inserts new
    void put(const std::string& key, uint64_t logical_id) {
        size_t idx = get_shard_idx(key);
        std::unique_lock<std::shared_mutex> write_lock(shards[idx].rw_lock);
        shards[idx].map[key] = logical_id;
    }

    // Returns true if found, sets logical_id out-parameter
    bool get(const std::string& key, uint64_t& out_logical_id) {
        size_t idx = get_shard_idx(key);
        
        std::shared_lock<std::shared_mutex> read_lock(shards[idx].rw_lock);
        
        auto it = shards[idx].map.find(key);
        if (it != shards[idx].map.end()) {
            out_logical_id = it->second;
            return true;
        }
        return false;
    }

    bool del(const std::string& key) {
        size_t idx = get_shard_idx(key);
        std::unique_lock<std::shared_mutex> write_lock(shards[idx].rw_lock);
        return shards[idx].map.erase(key) > 0;
    }
};

} // namespace imdb
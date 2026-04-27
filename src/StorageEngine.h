/* StorageEngine.h */
#pragma once

#include <vector>
#include <unordered_map>
#include <string>
#include <cstring>
#include <shared_mutex>
#include "SizeClass.h"
#include "DiskManager.h"
#include "Sweeper.h"

namespace imdb{
#define TABLE_END  0XFFFFFFFFFFFFFFFF
/* strcut RecordLoc(record location). Content in the translation table. 
 *
 * Record lookup is like: 
 * 1. hashmap[key] --> translation table idx.
 * 2. translation_table[idx] --> loc in ram / loc in disk
 * 
 * When unused(caused by record delete), stores next free idx
 */
struct RecordLoc{
    union{
        struct{
            union{
                void* ram_addr;
                size_t disk_offset;
            }; // total 16 bytes **try to keep it small!**
            uint32_t size; // 4 bytes
            bool is_in_ram; // 1 byte
            // 3 bytes compiler padding
        }in_use;

        uint64_t next_free_idx;
    };
}; 

/* record header. logical id must be stored here to complete reverse map in evcition.
 * TODO: might consider putting 'record size' here, too! For DB persistence
 */
#pragma pack(push, 1)
struct RecordHeader{
    uint64_t logical_id;

    char* get_payload() {
        return reinterpret_cast<char*>(this + 1);
    }
};
#pragma pack(pop)

/* Provides put, get and delete.
 */
class StorageEngine{
    private:
        Arena arena;
        EvictionSweeper sweeper;
        /* Recordheader is 8 bytes. Record >= 512B is managed by LargeRecordManager.
         * SCMs ranging from 16 bytes to 256 bytes.
         * 16, 32, 64, 128, 256
         */
        SizeClassManager SCMs[5];

        /* logical id generator, also serves as first_free_idx in translation table */
        uint64_t next_logical_id;
        /* translation table, bridge hashmap and record location. See comment at struct RecordLoc */
        std::vector<RecordLoc> translation_table;
        /* How much the table grow when it bocomes full: new_size = old_size*table_grow_speed. default 2. */
        uint8_t table_grow_speed;
        
        /* tracks key -> translation_table index mappings */
        std::unordered_map<std::string, uint64_t> hashmap;

        std::shared_mutex rw_lock;
        void evict_cold_page();

        /* get the idx of corresponding SCM by size */
        uint8_t get_scm_index(size_t total_size);

        uint64_t add_to_table(const RecordLoc& new_loc); // on success, return the inserted data's logical id
        void remove_from_table(uint64_t logical_id);
        /* grow the table by factor table_grow_speed */
        void grow_table();

    public:
        StorageEngine();

        ~StorageEngine() = default;

        /* Insert a new record or update a existing one. Return true on success */
        bool put(const std::string& key, const char* record, uint64_t record_size);
        /* Read a record from DB using key. Return true on success */
        bool get(const std::string& key, char* buf, uint64_t& record_size);
        /* Delete a record from DB using key. Return true on success */
        bool del(const std::string& key);
};

}// namespace imdb
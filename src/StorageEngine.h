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
 
#define PAGE_HOT_SCALE 4  
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

/* config object*/
struct DBConfig{
    std::string db_file_path = "../data/imdb.aof";
    uint8_t table_grow_speed = 2;

    bool enable_hot_rescue = 1;
};

/* StorageEngine, the highest level interface, provides put, get and delete. 
 * TODO: it owns sweeper thread. Might wish to move it else where.
 */
class StorageEngine{
    private:
        DBConfig config;
        Arena arena;
        DiskManager disk_manager;
        /* Recordheader is 8 bytes. Record >= 512B is managed by LargeRecordManager.
         * SCMs ranging from 16 bytes to 256 bytes.
         * 16, 32, 64, 128, 256
         */
        SizeClassManager SCMs[5];
        EvictionSweeper sweeper;

        /* logical id generator, also serves as first_free_idx in translation table */
        uint64_t next_logical_id;
        /* translation table, bridge hashmap and record location. See comment at struct RecordLoc */
        std::vector<RecordLoc> translation_table;
        
        /* tracks key -> translation_table index mappings */
        std::unordered_map<std::string, uint64_t> hashmap;

        std::shared_mutex rw_lock;
        void evict_cold_page();
        /* rescue hot record, and write other records to disk.*/
        void page_hot_rescue(Page* victim_page);

        /* --------------- helper functions -------------------------- */

        /* get the idx of corresponding SCM by size */
        uint8_t get_scm_index(size_t total_size);
        /* given an allocated slot, initialize it with the given logical id and payload 
         * don't check whether the size fits, nor whether the slot is allocated.     */
        void init_slot_nocheck(void* slot_addr, uint64_t logical_id, const char* record, uint64_t record_size);


        /* -------- translation table operations -------- */

        uint64_t add_to_table(const RecordLoc& new_loc); // on success, return the inserted data's logical id
        void remove_from_table(uint64_t logical_id);
        /* grow the table by factor config.table_grow_speed */
        void grow_table();

    public:
        StorageEngine(const DBConfig &cfg = DBConfig());

        ~StorageEngine() = default;

        /* Insert a new record or update a existing one. Return true on success */
        bool put(const std::string& key, const char* record, uint64_t record_size);
        /* Read a record from DB using key. Return true on success */
        bool get(const std::string& key, char* buf, uint64_t& record_size);
        /* Delete a record from DB using key. Return true on success */
        bool del(const std::string& key);
};

} // namespace imdb
#include "StorageEngine.h"

/*------------------------ctor/dtor-----------------------------*/

namespace imdb{
StorageEngine::StorageEngine(const std::string& db_file_path, unsigned short table_grow_speed):
    disk_manager(db_file_path),
    SCMs{{16, arena}, {32, arena}, {64, arena}, {128, arena}, {256, arena}},
    sweeper(arena, [this](){this->evict_cold_page();}),
    next_logical_id(0), table_grow_speed(table_grow_speed)
{
    translation_table.resize(10000);
    for(int i = 0; i<10000-1; i++){ translation_table[i].next_free_idx = i + 1; }
    translation_table[9999].next_free_idx = TABLE_END;
}

/*------------------------------------Helper Functions---------------------------------------*/

uint8_t StorageEngine::get_scm_index(uint64_t total_size) {
    if (total_size <= 16) return 0;
    if (total_size <= 32) return 1;
    if (total_size <= 64) return 2;
    if (total_size <= 128) return 3;
    if (total_size <= 256) return 4;
    
    return 255; // Error/Too large
}

void StorageEngine::init_slot_nocheck(void* slot_addr, uint64_t logical_id, const char* record, uint64_t record_size){
    RecordHeader* header = reinterpret_cast<RecordHeader*>(slot_addr);
    header->logical_id = logical_id;
    char* payload = header->get_payload();
    std::memcpy(payload, record, record_size);
}

/*---------------------------------Translationn table related----------------------------------*/

uint64_t StorageEngine::add_to_table(const RecordLoc& new_loc){
    if(next_logical_id == TABLE_END) grow_table();

    uint64_t logical_id = next_logical_id;
    next_logical_id = translation_table[next_logical_id].next_free_idx;
    translation_table[logical_id] = new_loc;

    return logical_id;
}

void StorageEngine::remove_from_table(uint64_t logical_id){
    translation_table[logical_id].next_free_idx = next_logical_id;
    next_logical_id = logical_id;
}

void StorageEngine::grow_table(){
    uint64_t old_size = translation_table.size();
    uint64_t new_size = old_size*table_grow_speed;
    translation_table.resize(new_size);

    for(uint64_t i=old_size; i<new_size-1; i++){ 
        translation_table[i].next_free_idx = i+1;
    }
    translation_table[new_size-1].next_free_idx = TABLE_END;

    next_logical_id = old_size;
}

/*-----------------------------------------Eviction logic----------------------------------------*/
void StorageEngine::evict_cold_page(){
    std::unique_lock<std::shared_mutex> write_lock(rw_lock); // TODO: remove this

    // ==================================================================== TODO: lock 1 for phrase 1
    // Get the coldest page from the Arena
    Page* victim_page = reinterpret_cast<Page*>(arena.get_lru_tail());
    if (!victim_page) return;
    // Scan every slot on the page
    for (uint16_t i = 0; i < victim_page->header.max_slots; i++) {
        char* slot_addr = victim_page->get_slot_addr(i);
        RecordHeader* header = reinterpret_cast<RecordHeader*>(slot_addr);
        
        uint64_t logical_id = header->logical_id;

        if (logical_id < translation_table.size()) {
            RecordLoc& loc = translation_table[logical_id];
            
            // If the translation table agrees that this record lives at this exact memory address:
            if (loc.in_use.is_in_ram && loc.in_use.ram_addr == slot_addr){
                
                // write the header AND the payload to disk
                size_t write_size = sizeof(RecordHeader) + loc.in_use.size;
                size_t disk_offset = disk_manager.write_record(slot_addr, write_size);
                
                // update the translation table
                loc.in_use.is_in_ram = false;
                loc.in_use.disk_offset = disk_offset;
            }
        }
    }
    // ===================================================================== TODO: lock 2 for phrase 2
    uint8_t scm_idx = get_scm_index(victim_page->header.slot_size);
    SCMs[scm_idx].reclaim_evicted_page(victim_page);
}

/*-----------------------------------------Interface---------------------------------------------*/

/* TODO: for V1.0, global lock is used. */

bool StorageEngine::put(const std::string& key, const char* record, uint64_t record_size){
    // TODO: remove this line. Currently, without this line, there will be deadlock
    // cause by put() hold rw_lock and wait on sweeper
    while(arena.is_critical()){
        std::this_thread::yield(); // yield to sweeper
    }

    std::unique_lock<std::shared_mutex> write_lock(rw_lock);
    
    size_t real_size = record_size + sizeof(RecordHeader);
    if(real_size > 256){
        // TODO: large record manager
        std::cerr<<RED<<"Out of bound\n"<<RESET;
        return false;
    }

    auto it = hashmap.find(key);
    /* case 1: record exists, update the record */
    if(it != hashmap.end()){
        uint64_t logical_id = it->second;
        RecordLoc& loc = translation_table[logical_id];

        /* case 1.1: record is in ram */
        if(loc.in_use.is_in_ram){
            void* slot_addr = loc.in_use.ram_addr;
            size_t old_size = get_struct_page(slot_addr)->header.slot_size;

            /* case 1.1.1: New data fits in the existing slot. Update in-place. */
            if( real_size <= old_size){
                init_slot_nocheck(slot_addr, logical_id, record, record_size);

                loc.in_use.size = record_size;
                mark_slot_hot(slot_addr);
                return true;
            }
            /* case 1.1.2: New data outgrows the current slot. Reallocate. */
            else{
                SizeClassManager &scm = SCMs[get_scm_index(real_size)];
                void* new_slot = scm.alloc();

                // Set up new header and payload
                init_slot_nocheck(new_slot, logical_id, record, record_size);

                // Free old slot
                SizeClassManager &old_scm = SCMs[get_scm_index(old_size)];
                old_scm.free(slot_addr);
                mark_slot_cold(slot_addr);

                // Update translation table
                loc.in_use.ram_addr = new_slot;
                loc.in_use.size = record_size;
                
                mark_slot_hot(new_slot);
                return true;
            }
        }
        /* case 1.2: record is in disk */
        else{
            // allocate a new slot in RAM for the updated record
            SizeClassManager &scm = SCMs[get_scm_index(real_size)];
            void* new_slot = scm.alloc();

            // Set up new header and payload
            init_slot_nocheck(new_slot, logical_id, record, record_size);

            // Update translation table to bring the record back to RAM
            loc.in_use.is_in_ram = true;
            loc.in_use.ram_addr = new_slot;
            loc.in_use.size = record_size;
            
            mark_slot_hot(new_slot);
            return true;
        }
    }
    /* case 2: record doesn't exist insert a new one */
    else{
        /* Don't foeget to take header into account */
        SizeClassManager &scm = SCMs[get_scm_index(record_size+sizeof(RecordHeader))];
        void* new_slot_addr = scm.alloc();

        /* update the translation table */
        RecordLoc new_loc;
        new_loc.in_use.is_in_ram = true;
        new_loc.in_use.size = record_size;
        new_loc.in_use.ram_addr = new_slot_addr;

        uint64_t new_logical_id = add_to_table(new_loc);
        hashmap[key] = new_logical_id;

        /* set up the record's header */
        init_slot_nocheck(new_slot_addr, new_logical_id, record, record_size);

        mark_slot_hot(new_slot_addr);

        return true;
    }
}

bool StorageEngine::get(const std::string& key, char* buf, uint64_t& record_size){
    std::shared_lock<std::shared_mutex> read_lock(rw_lock);

    auto it = hashmap.find(key);
    if(it == hashmap.end()) {std::cerr<<RED<<"Get record with key: "<<key<<" error: record not exists\n"<<RESET; return false;}

    uint64_t logical_id = it->second;
    RecordLoc& loc = translation_table[logical_id];

    if(loc.in_use.is_in_ram){
        RecordHeader* header = reinterpret_cast<RecordHeader*>(loc.in_use.ram_addr);
        char* payload = header->get_payload();

        std::memcpy(buf, payload, loc.in_use.size);
        record_size = loc.in_use.size;

        mark_slot_hot(loc.in_use.ram_addr);

        return true;
    }
    else{
        // Data is on disk. 
        size_t payload_offset = loc.in_use.disk_offset + sizeof(RecordHeader);
        
        bool success = disk_manager.read_record(payload_offset, buf, loc.in_use.size);
        if(!success) return false;

        record_size = loc.in_use.size;
        return true;
    }
}

bool StorageEngine::del(const std::string& key){
    std::unique_lock<std::shared_mutex> write_lock(rw_lock);

    auto it = hashmap.find(key);
    if(it == hashmap.end()) {std::cerr<<RED<<"Delete record with key: "<<key<<" error: record not exists\n"<<RESET; return false;}

    uint64_t logical_id = it->second;
    RecordLoc &loc = translation_table[logical_id];

    /* record is in ram, delete directly */
    if (loc.in_use.is_in_ram) {
        void* slot_addr = loc.in_use.ram_addr;
        Page* page = get_struct_page(slot_addr);
        size_t slot_size = page->header.slot_size;

        SCMs[get_scm_index(slot_size)].free(slot_addr);
    } 
    /* record is in disk, logical delete */
    else{
        // TODO: Logical deletion for on-disk records (tombstoning)
    }

    remove_from_table(logical_id);
    hashmap.erase(it);

    return true;
}

}//namespace imdb
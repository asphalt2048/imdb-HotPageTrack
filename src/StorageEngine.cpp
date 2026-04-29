#include "StorageEngine.h"

/*------------------------ctor/dtor-----------------------------*/

namespace imdb{
StorageEngine::StorageEngine(const DBConfig &cfg):
    config(cfg),
    disk_manager(config.db_file_path),
    SCMs{{16, arena}, {32, arena}, {64, arena}, {128, arena}, {256, arena}},
    sweeper(arena, [this](){this->evict_cold_page();}),
    next_logical_id(0)
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

/* given an allocated slot, initialize it with the given logical id and payload 
 * don't check whether the size fits, nor whether the slot is allocated.     */
void StorageEngine::init_slot_nocheck(void* slot_addr, uint64_t logical_id, const char* record, uint64_t record_size){
    RecordHeader* header = reinterpret_cast<RecordHeader*>(slot_addr);
    header->logical_id = logical_id;
    char* payload = header->get_payload();
    std::memcpy(payload, record, record_size);
}

bool StorageEngine::update_record(uint64_t logical_id, const char* record, uint64_t record_size) {
    size_t real_size = record_size + sizeof(RecordHeader);
    // TODO: We still hold the global lock here for now. 
    // In the next step, we will move this down to enable Optimistic Concurrency!
    std::unique_lock<std::shared_mutex> write_lock(tt_scm_lock);

    RecordLoc& loc = translation_table[logical_id];

    /* case 1: record is in ram */
    if(loc.in_use.is_in_ram){
        void* slot_addr = loc.in_use.ram_addr;
        size_t old_size = get_struct_page(slot_addr)->header.slot_size;

        /* case 1.1: New data fits in the existing slot. Update in-place. */
        if( real_size <= old_size){
            init_slot_nocheck(slot_addr, logical_id, record, record_size);
            mark_slot_hot(slot_addr);
            loc.in_use.size = record_size;
            return true;
        }
        /* case 1.2: New data outgrows the current slot. Reallocate. */
        else{
            SizeClassManager &scm = SCMs[get_scm_index(real_size)];
            void* new_slot = scm.alloc();

            init_slot_nocheck(new_slot, logical_id, record, record_size);
            mark_slot_hot(new_slot);

            SizeClassManager &old_scm = SCMs[get_scm_index(old_size)];
            old_scm.free(slot_addr);
            mark_slot_cold(slot_addr);

            loc.in_use.ram_addr = new_slot;
            loc.in_use.size = record_size;
            return true;
        }
    }
    /* case 2: record is in disk */
    else{
        SizeClassManager &scm = SCMs[get_scm_index(real_size)];
        void* new_slot = scm.alloc();

        init_slot_nocheck(new_slot, logical_id, record, record_size);
        mark_slot_hot(new_slot);

        loc.in_use.is_in_ram = true;
        loc.in_use.ram_addr = new_slot;
        loc.in_use.size = record_size;
        return true;
    }
}

bool StorageEngine::insert_record(const std::string& key, const char* record, uint64_t record_size, uint64_t &collided_id) {
    // TODO: We still hold the global lock here for now. 
    // In the next step, we will move this down to enable Optimistic Concurrency!
    std::unique_lock<std::shared_mutex> write_lock(tt_scm_lock);

    SizeClassManager &scm = SCMs[get_scm_index(record_size + sizeof(RecordHeader))];
    void* new_slot_addr = scm.alloc();

    RecordLoc new_loc;
    new_loc.in_use.is_in_ram = true;
    new_loc.in_use.size = record_size;
    new_loc.in_use.ram_addr = new_slot_addr;

    uint64_t new_logical_id = add_to_table(new_loc);
    
    /* Beware of insert race:
     * Two insert with same key fire at the same time. The loser in the race must roll back.
     */
    bool success = hashmap.insert(key, new_logical_id, collided_id);
    if(!success){ // roll back
        scm.free(new_slot_addr);
        remove_from_table(new_logical_id);
        return false;
    }

    init_slot_nocheck(new_slot_addr, new_logical_id, record, record_size);
    mark_slot_hot(new_slot_addr);

    return true;
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
    uint64_t new_size = old_size*config.table_grow_speed;
    translation_table.resize(new_size);

    for(uint64_t i=old_size; i<new_size-1; i++){ 
        translation_table[i].next_free_idx = i+1;
    }
    translation_table[new_size-1].next_free_idx = TABLE_END;

    next_logical_id = old_size;
}

/*-----------------------------------------Interface---------------------------------------------*/

/* TODO: for V1.0, global lock is used. */

bool StorageEngine::put(const std::string& key, const char* record, uint64_t record_size){
    // TODO: remove this line. Currently, without this line, there will be deadlock
    // cause by put() hold rw_lock and wait on sweeper
    while(arena.is_critical()){
        std::this_thread::yield(); // yield to sweeper
    }

    size_t real_size = record_size + sizeof(RecordHeader);
    if(real_size > 256){
        std::cerr<<RED<<"Out of bound\n"<<RESET;
        return false;
    }

    uint64_t logical_id;
    bool exist = hashmap.get(key, logical_id);

    if (exist) {
        return update_record(logical_id, record, record_size);
    } else {
        uint64_t collided_id;
        bool succ = insert_record(key, record, record_size, collided_id);
        if(!succ){
            return update_record(collided_id, record, record_size);
        }
        return true;
    }
}

bool StorageEngine::get(const std::string& key, char* buf, uint64_t& record_size){
    uint64_t logical_id;
    bool exist = hashmap.get(key, logical_id);
    if(!exist) {std::cerr<<RED<<"Get record with key: "<<key<<" error: record not exists\n"<<RESET; return false;}

    std::shared_lock<std::shared_mutex> read_lock(tt_scm_lock);

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
    uint64_t logical_id;
    bool exist = hashmap.get(key, logical_id);
    if(!exist) {std::cerr<<RED<<"Delete record with key: "<<key<<" error: record not exists\n"<<RESET; return false;}

    std::unique_lock<std::shared_mutex> write_lock(tt_scm_lock);

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
        // Logical deletion for on-disk records. We just delete metadata.
        // that way from the user's view, the record is deleted as well.
    }

    remove_from_table(logical_id);
    hashmap.del(key);

    return true;
}

/* ============================================================================================= */
/*-----------------------------------------Eviction logic----------------------------------------*/
/* ============================================================================================= */

void StorageEngine::page_hot_rescue(Page* victim_page){
    uint16_t max_slots = victim_page->header.max_slots;
    SizeClassManager &scm = SCMs[get_scm_index(victim_page->header.slot_size)];

    for (uint16_t i = 0; i < max_slots; i++) {
        char* slot_addr = victim_page->get_slot_addr(i);
        RecordHeader* header = reinterpret_cast<RecordHeader*>(slot_addr);
        uint64_t logical_id = header->logical_id;

        if (logical_id < translation_table.size()) {
            RecordLoc& loc = translation_table[logical_id];
            
            if (loc.in_use.is_in_ram && loc.in_use.ram_addr == slot_addr) {
                
                // --- BEST EFFORT RESCUE ---
                if (config.enable_hot_rescue && is_slot_hot(slot_addr)) {
                    size_t total_size = loc.in_use.size + sizeof(RecordHeader);
                    
                    /* TRY to ask free space in SCM. Might fail. 
                     * If fail, let the record die(write to disk).
                     *
                     * WARNING: allocation in this section must not sleep!
                     */
                    void* new_slot = scm.alloc_notrigger();
                    
                    if (new_slot != nullptr) {
                        std::memcpy(new_slot, slot_addr, total_size);
                        loc.in_use.ram_addr = new_slot;
                        mark_slot_cold(new_slot); 
                        continue;
                    }
                    // TODO: do we lift the 'new home' in lru?
                }

                // --- COLD EVICTION (OR RESCUE FAILED) ---
                size_t total_size = loc.in_use.size + sizeof(RecordHeader);
                size_t disk_offset = disk_manager.write_record(slot_addr, total_size);
                
                loc.in_use.is_in_ram = false;
                loc.in_use.disk_offset = disk_offset;
            }
        }
    }
}
void StorageEngine::evict_cold_page() {
    std::unique_lock<std::shared_mutex> write_lock(tt_scm_lock);

    while (true) {
        Page* victim_page = reinterpret_cast<Page*>(arena.get_lru_tail());
        if (!victim_page) return; 

        uint16_t max_slots = victim_page->header.max_slots;
        

        /* =======================================================================
         * Clock algorithm
         * If a page have (hot records > max_slot/PAGE_HOT_SCALE), give it a second chance.
         * the page is lifted to the head of lru. And all hot bits are cleared
          ======================================================================= */
        uint16_t hot_count = get_page_hot_count(victim_page);
        if (hot_count > (max_slots / PAGE_HOT_SCALE)) {
            clear_page_hot_bits(victim_page);
            arena.lift_in_lru(victim_page);
            continue; 
        }
        
        /* page selected as evcition target */
        page_hot_rescue(victim_page);

        /* reclaim the page. */
        uint8_t scm_idx = get_scm_index(victim_page->header.slot_size);
        SCMs[scm_idx].reclaim_evicted_page(victim_page);
        break; 
    }
}

}//namespace imdb
#include "StorageEngine.h"

namespace imdb{
StorageEngine::StorageEngine():next_logical_id(0), scm_64(64, arena), table_grow_speed(2){
    translation_table.resize(10000);
    for(int i = 0; i<10000-1; i++){ translation_table[i].next_free_idx = i + 1; }
    translation_table[9999].next_free_idx = TABLE_END;
}


void StorageEngine::mark_slot_hot_dynamic(void* slot_addr, uint8_t size_class){
    // TODO: overflow in 32 bit system?
    size_t slot_size = (1ULL << size_class);
    mark_slot_hot(slot_addr);
}

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

bool StorageEngine::put(const std::string& key, const char* record, uint64_t record_size){
    /* V0.1 test: only support 64 byte size class */
    if(record_size > 64 - sizeof(RecordHeader)){
        std::cerr<<RED<<"V0.1 test: only 64 byte size class supported\n"<<RESET;
        return false;
    }

    auto it = hashmap.find(key);
    /* case 1: record exists, update the record */
    if(it != hashmap.end()){
        uint64_t logical_id = it->second;
        RecordLoc& loc = translation_table[logical_id]; // take a reference
        /* case 1.1: record is in ram */
        if(loc.in_use.is_in_ram){
            // TODO: support update that changes size class

            RecordHeader* header = reinterpret_cast<RecordHeader*>(loc.in_use.ram_addr);
            char* payload = header->get_payload();
            std::memcpy(payload, record, record_size);

            loc.in_use.size = record_size;
            mark_slot_hot_dynamic(loc.in_use.ram_addr, loc.in_use.size_class);

            return true;
        }
        /* case 1.2: record is in disk */
        else{
            // TODO: 
            return false;
        }
    }
    /* case 2: record doesn't exist insert a new one */
    else{
        // TODO: support other sized record
        void* slot_addr = scm_64.alloc();

        /* update the translation table */
        RecordLoc new_loc;
        new_loc.in_use.is_in_ram = true;
        new_loc.in_use.size = record_size;
        new_loc.in_use.ram_addr = slot_addr;
        // TODO: change this
        new_loc.in_use.size_class = 6;

        uint64_t logical_id = add_to_table(new_loc);
        hashmap[key] = logical_id;

        /* set up the record's header */
        RecordHeader* header = reinterpret_cast<RecordHeader*>(slot_addr);
        header->logical_id = logical_id;
        /* store the record */
        char* payload = header->get_payload();
        std::memcpy(payload, record, record_size);

        mark_slot_hot_dynamic(slot_addr, new_loc.in_use.size_class);

        return true;
    }
}

bool StorageEngine::get(const std::string& key, char* buf, uint64_t& record_size){
    auto it = hashmap.find(key);
    if(it == hashmap.end()) {std::cerr<<RED<<"Get record with key: "<<key<<" error: record not exists\n"<<RESET; return false;}

    uint64_t logical_id = it->second;
    RecordLoc& loc = translation_table[logical_id];

    if(loc.in_use.is_in_ram){
        RecordHeader* header = reinterpret_cast<RecordHeader*>(loc.in_use.ram_addr);
        char* payload = header->get_payload();

        std::memcpy(buf, payload, loc.in_use.size);
        record_size = loc.in_use.size;

        mark_slot_hot_dynamic(loc.in_use.ram_addr, loc.in_use.size_class);

        return true;
    }
    else{
        // TODO: data in disk
        return false;
    }
}

bool StorageEngine::del(const std::string& key){
    auto it = hashmap.find(key);
    if(it == hashmap.end()) {std::cerr<<RED<<"Delete record with key: "<<key<<" error: record not exists\n"<<RESET; return false;}

    uint64_t logical_id = it->second;
    scm_64.free(translation_table[logical_id].in_use.ram_addr);

    remove_from_table(logical_id);
    hashmap.erase(it);

    return true;
}

}//namespace imdb
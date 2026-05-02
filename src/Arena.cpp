#include "Arena.h"

namespace imdb{
Arena::Arena(){
    arena_base = mmap(nullptr, ARENA_SIZE, PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);
    if(arena_base == MAP_FAILED){
        std::cerr << RED << "mmap failed, arena setup existing\n"<<RESET;
        exit(-1);
    }
    /* Pin down arena in memory. We don't want OS to swap our page, we do it ourselves */
    if(mlock(arena_base, ARENA_SIZE) != 0){
        std::cerr<<RED<<"Failed to mlock arena\n"<<RESET;
        exit(-1);
    }

    /* init bitmap */
    for(size_t i = 0; i<BITMAP_SIZE; i++){ bitmap[i] = 0; }
    last_searched_idx = 0;

    high_watermark = TOTAL_PAGES*0.80;
    low_watermark = TOTAL_PAGES*0.90;
    min_watermark = TOTAL_PAGES*0.98;
}

Arena::~Arena(){ munmap(arena_base, ARENA_SIZE); }

void* Arena::alloc_a_page_nocheck(){
    for(size_t i = 0; i<BITMAP_SIZE; i++){
        size_t idx = (last_searched_idx + i) % BITMAP_SIZE;

        /* bitmap[idx] is not full, i.e. not 0xFFFFFFFFFFFFFFFF */
        bitmap_mutex.lock();
        if(bitmap[idx] != ~0ULL){
            unsigned int free_bit = __builtin_ffsll(~bitmap[idx]) - 1;
            bitmap[idx] |= (1ULL << free_bit);

            // unlock after setting the bit to minimize the critical section
            bitmap_mutex.unlock();

            last_searched_idx = idx;

            size_t page_id = 64*idx + free_bit;
            uintptr_t page_base_addr  = reinterpret_cast<uintptr_t>(arena_base) + page_id*PAGE_SIZE;

            used_pages.fetch_add(1);
            
            return reinterpret_cast<void *>(page_base_addr);
        }
        else{
            // check next one, don't forget to unlock before continue
            bitmap_mutex.unlock();
        }
    }
    return nullptr;
}

void* Arena::alloc_a_page(){
    /* direct reclamation. Stop the world to evict pages when min_watermark is hit */
    while (is_critical()){
        sweeper_cv.notify_one();
        std::this_thread::yield();
    }
    
    /* allocation should be OK with direct reclamation ahead */
    void* page = alloc_a_page_nocheck();
    if(page == nullptr){
        std::cerr<<RED<<"OOM: Page alloc failed\n"<<RESET;
        exit(-1);
    }
    /* Don't hold any mutex here. 
     * Might suffer a lost wakeup(needs_sweeping is true, but no sweeper awaken),
     * but following alloc_a_page will wake up one eventually.
     */
    if(needs_sweeping()){
        sweeper_cv.notify_one();
    }

    return page;
}

void Arena::free_a_page(void *raw_page_base){
    uintptr_t arena_base_ = reinterpret_cast<uintptr_t>(arena_base);
    uintptr_t page_base = reinterpret_cast<uintptr_t>(raw_page_base);

    if(page_base<arena_base_ || page_base>=arena_base_ + ARENA_SIZE){
        std::cerr<<RED<<"err: Arena::free_a_page(), page not belongs to arena\n"<<RESET;
        exit(-1);
    }

    size_t page_id = (page_base - arena_base_) / PAGE_SIZE;

    unsigned int idx = page_id / 64;
    unsigned int free_bit = page_id % 64;

    bitmap_mutex.lock();
    bitmap[idx] &= ~(1ULL << free_bit);
    bitmap_mutex.unlock();

    used_pages.fetch_sub(1);
    // remove_from_lru(raw_page_base);

    if(idx < last_searched_idx){ last_searched_idx = idx; } 
    return;
}

uint32_t Arena::get_page_id(void* raw_addr){
    uintptr_t addr = reinterpret_cast<uintptr_t>(raw_addr);
    uintptr_t base = reinterpret_cast<uintptr_t>(arena_base);
    return (addr - base) / PAGE_SIZE;
}

/*---------------------------------LRU Management-----------------------------------*/

void Arena::add_to_lru(void* page_base){
    Page* page = reinterpret_cast<Page*>(page_base);
    std::lock_guard<std::mutex> lock(lru_mutex);

    if (page->header.lru_next != nullptr || 
        page->header.lru_prev != nullptr || lru_head == page) {
        return; // Already in list
    }

    page->header.lru_next = lru_head;
    page->header.lru_prev = nullptr;

    if (lru_head) lru_head->header.lru_prev = page;
    lru_head = page;

    if (!lru_tail) lru_tail = page;
}

void Arena::remove_from_lru(void* page_base){
    Page* page = reinterpret_cast<Page*>(page_base);
    std::lock_guard<std::mutex> lock(lru_mutex);

    if (page->header.lru_prev) {
        page->header.lru_prev->header.lru_next = page->header.lru_next;
    } else {
        lru_head = page->header.lru_next;
    }

    if (page->header.lru_next) {
        page->header.lru_next->header.lru_prev = page->header.lru_prev;
    } else {
        lru_tail = page->header.lru_prev;
    }

    page->header.lru_next = nullptr;
    page->header.lru_prev = nullptr;
}

void Arena::lift_in_lru(void* page_base){
    // simply remove it and immediately add it back.
    // don't lock here because remove/add handle their own locking.
    remove_from_lru(page_base);
    add_to_lru(page_base);
}

void* Arena::get_lru_tail(){
    std::lock_guard<std::mutex> lock(lru_mutex);
    return reinterpret_cast<void*>(lru_tail); 
}


/*-------------------------Helper functions---------------------------------------*/
/* They are not bind to a size class(not a member funtion) for flexibilty reasons */

// TODO: lock free or not? Does user need to hold lock when calling this function?
// (this is a write to page, a write need a lock)
// false positive acceptive?
void mark_slot_hot(void* slot_addr){
    Page* page = get_struct_page(slot_addr);
    uint16_t slot_id = page->get_slot_id_nocheck(slot_addr);

    size_t arr_idx = slot_id / 64;
    size_t bit_idx = slot_id % 64;

    page->is_hot[arr_idx].fetch_or(1ULL << bit_idx, std::memory_order_relaxed);
};

// TODO: lock free or not? Does user need to hold lock when calling this function?
// (this is a write to page, a write need a lock)
void mark_slot_cold(void* slot_addr){
    Page* page = get_struct_page(slot_addr);
    uint16_t slot_id = page->get_slot_id_nocheck(slot_addr);

    size_t arr_idx = slot_id / 64;
    size_t bit_idx = slot_id % 64;

    page->is_hot[arr_idx].fetch_and(~(1ULL << bit_idx), std::memory_order_relaxed);
};

bool is_slot_hot(void* slot_addr){
    Page* page = get_struct_page(slot_addr);
    uint16_t slot_id = page->get_slot_id_nocheck(slot_addr);

    size_t arr_idx = slot_id / 64;
    size_t bit_idx = slot_id % 64;

    return page->is_hot[arr_idx].load(std::memory_order_relaxed) & (1ULL << bit_idx);
}

uint16_t get_page_hot_count(Page* page) {
    uint16_t total_hot = 0;
    for (int i = 0; i < IS_HOT_ARR_LENGTH; i++) {
        // no lock. 'slightly false' value is acceptable
        uint64_t chunk = page->is_hot[i].load(std::memory_order_relaxed);
        total_hot += __builtin_popcountll(chunk);
    }
    return total_hot;
}

void clear_page_hot_bits(Page* page) {
    for (int i = 0; i < 4; i++) {
        page->is_hot[i].store(0ULL, std::memory_order_relaxed);
    }
}

Page* get_struct_page(void* slot_addr){
    uintptr_t slot_addr_ = reinterpret_cast<uintptr_t>(slot_addr);
    uintptr_t page_base = PAGE_ALIGN(slot_addr_);

    return reinterpret_cast<Page*>(page_base);
}
}//namesapce imdb
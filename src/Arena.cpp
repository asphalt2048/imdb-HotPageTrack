#include "Arena.h"

namespace imdb{
Arena::Arena(){
    arena_base = mmap(nullptr, ARENA_SIZE, PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);
    if(arena_base == MAP_FAILED){
        std::cerr << RED << "mmap failed, arena setup existing\n"<<RESET;
        exit(-1);
    }

    /* init bitmap */
    for(int i = 0; i<BITMAP_SIZE; i++){ bitmap[i] = 0; }
    last_searched_idx = 0;

    high_watermark = TOTAL_PAGES*0.80;
    low_watermark = TOTAL_PAGES*0.90;
    min_watermark = TOTAL_PAGES*0.98;
}

Arena::~Arena(){ munmap(arena_base, ARENA_SIZE); }

void* Arena::alloc_a_page_nocheck(){
    for(int i = 0; i<BITMAP_SIZE; i++){
        size_t idx = (last_searched_idx + i) % BITMAP_SIZE;

        /* bitmap[idx] is not full, i.e. not 0xFFFFFFFFFFFFFFFF */
        bitmap_mutex.lock();
        if(bitmap[idx] != ~0ULL){
            unsigned int free_bit = __builtin_ffsll(~bitmap[idx]) - 1;

            bitmap[idx] |= (1ULL << free_bit);
            bitmap_mutex.unlock();
            last_searched_idx = idx;

            size_t page_id = 64*idx + free_bit;
            uintptr_t page_base_addr  = reinterpret_cast<uintptr_t>(arena_base) + page_id*PAGE_SIZE;

            return reinterpret_cast<void *>(page_base_addr);
        }else{ bitmap_mutex.unlock(); }
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

    used_pages.fetch_add(1);
    // TODO
    add_to_lru(page);

    /* Don't hold any mutex here. 
     * Might suffer a lost wakeup(needs_sweeping is true, but no sweeper awaken),
     * but following alloc_a_page will wake up one eventually.
     */
    if(needs_sweeping()){
        sweeper_cv.notify_one();
    }

    return page;
}

// TODO: change used_pages
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

    if (page->header.lru_pointers.lru_next != nullptr || 
        page->header.lru_pointers.lru_prev != nullptr || lru_head == page) {
        return; // Already in list
    }

    page->header.lru_pointers.lru_next = lru_head;
    page->header.lru_pointers.lru_prev = nullptr;

    if (lru_head) lru_head->header.lru_pointers.lru_prev = page;
    lru_head = page;

    if (!lru_tail) lru_tail = page;
}

void Arena::remove_from_lru(void* page_base){
    Page* page = reinterpret_cast<Page*>(page_base);
    std::lock_guard<std::mutex> lock(lru_mutex);

    if (page->header.lru_pointers.lru_prev) {
        page->header.lru_pointers.lru_prev->header.lru_pointers.lru_next = page->header.lru_pointers.lru_next;
    } else {
        lru_head = page->header.lru_pointers.lru_next;
    }

    if (page->header.lru_pointers.lru_next) {
        page->header.lru_pointers.lru_next->header.lru_pointers.lru_prev = page->header.lru_pointers.lru_prev;
    } else {
        lru_tail = page->header.lru_pointers.lru_prev;
    }

    page->header.lru_pointers.lru_next = nullptr;
    page->header.lru_pointers.lru_prev = nullptr;
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

void mark_slot_hot(void* slot_addr){
    uintptr_t slot_addr_ = reinterpret_cast<uintptr_t>(slot_addr);
    uintptr_t page_base = PAGE_ALIGN(slot_addr_);

    Page* page = reinterpret_cast<Page*>(page_base);

    uint16_t slot_id = ((slot_addr_-page_base) - page->header.header_reserved) / page->header.slot_size;

    size_t arr_idx = slot_id / 64;
    size_t bit_idx = slot_id % 64;

    page->is_hot[arr_idx] |= (1ULL << bit_idx);
};

void mark_slot_cold(void* slot_addr){
    uintptr_t slot_addr_ = reinterpret_cast<uintptr_t>(slot_addr);
    uintptr_t page_base = PAGE_ALIGN(slot_addr_);

    Page* page = reinterpret_cast<Page*>(page_base);

    uint16_t slot_id = ((slot_addr_-page_base) - page->header.header_reserved) / page->header.slot_size;

    size_t arr_idx = slot_id / 64;
    size_t bit_idx = slot_id % 64;

    page->is_hot[arr_idx] &= ~(1ULL << bit_idx);
};

Page* get_struct_page(void* slot_addr){
    uintptr_t slot_addr_ = reinterpret_cast<uintptr_t>(slot_addr);
    uintptr_t page_base = PAGE_ALIGN(slot_addr_);

    return reinterpret_cast<Page*>(page_base);
}
}//namesapce imdb
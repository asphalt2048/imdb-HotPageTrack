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
    size_t local_start = last_searched_idx.load(std::memory_order_relaxed);

    for (size_t i = 0; i < BITMAP_SIZE; i++) {
        size_t idx = (local_start + i) % BITMAP_SIZE;
        
        uint64_t chunk = bitmap[idx].load(std::memory_order_acquire);
        
        // Spin lock-free on this specific chunk until we claim a bit or it fills up
        while (chunk != ~0ULL) {
            int first_free_bit = __builtin_ctzll(~chunk);
            uint64_t new_chunk = chunk | (1ULL << first_free_bit);
            
            // Try to atomically swap our modified chunk into the array
            if (bitmap[idx].compare_exchange_weak(chunk, new_chunk, std::memory_order_release, std::memory_order_relaxed)){
                
                last_searched_idx.store(idx, std::memory_order_relaxed);
                used_pages.fetch_add(1, std::memory_order_relaxed);
                
                uintptr_t offset = ((idx * 64) + first_free_bit) * PAGE_SIZE;
                return reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(arena_base) + offset);
            }
            // If compare_exchange_weak failed, another thread stole some bits! 
            // loop again
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

    bitmap[idx].fetch_and(~(1ULL << free_bit), std::memory_order_release);
    used_pages.fetch_sub(1, std::memory_order_release);

    size_t current_hint = last_searched_idx.load(std::memory_order_relaxed);
    // CAS loop to safely pull the hint downwards
    while (idx < current_hint){
        if (last_searched_idx.compare_exchange_weak(current_hint, idx, std::memory_order_relaxed)) {
            break;
        }
    }
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

    // Sanity check: page belonga to LRU won't be remove. Keep LRU metadata safe
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

    // Sanity check: page don't belong to LRU won't be remove. Keep LRU metadata safe
    if(page->header.lru_next == nullptr && page->header.lru_prev == nullptr && lru_head != page){
        return;
    }

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
}//namesapce imdb
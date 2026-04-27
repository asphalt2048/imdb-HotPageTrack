/* Arene is the 'global' pool of pages this DB uses. SizeClasses will allocate/free
 * pages inside arena. Pages that are swapped out should also be returned here.
 */
#pragma once

#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <sys/mman.h>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <thread>

#define RED     "\033[31m"  /* text color: red */
#define RESET   "\033[0m"   /* reset color */ 
#define PAGE_ALIGN(x) ((x) & ~(4096ULL - 1))

/* The manager of arena.
 * V1.0: uses bitmap to track free pages
 */

namespace imdb{ 
struct Page{
    static constexpr size_t PAGE_SIZE = 4096;

    struct{
        struct{
            Page* prev;
            Page* next;
        };
        struct{
            Page* lru_prev;
            Page* lru_next;
        }lru_pointers;
        uint32_t page_id;

        uint16_t first_free_idx;

        uint16_t slot_size;
        uint16_t max_slots; // dynamically set by SizeClassManager::init_page()
        uint16_t used;    // counter of in-use slots

        /* The size header takes. Calculated dynamically at init_page() */
        uint16_t header_reserved;
    }header;

    /* array that tracks records usage. Recently accessed record should have is_hot bit == 1.
     * Record header is 8 bytes, so SC begins with 16 bytes. Making a page containing at most 254(4096/16) records.
     */
    uint64_t is_hot[4];

    /* ------------------Helper functions--------------------- */

    inline char* get_slot_addr(uint16_t idx){
        return reinterpret_cast<char*>(this) + header.header_reserved + (idx * header.slot_size);
    }
    /* slots in managed by internal free list. This function returns the reference to the "next_free" pointer in a slot */
    inline uint16_t& next_free(uint16_t idx){
        return *reinterpret_cast<uint16_t*>(get_slot_addr(idx));
    }
};

class Arena{
    private:
        static constexpr size_t ARENA_SIZE = 1024*1024*8;
        static constexpr size_t PAGE_SIZE = 4096;
        static constexpr size_t TOTAL_PAGES = ARENA_SIZE / PAGE_SIZE;
        /* devided by 64 as bitmap is defined as uint64_t 
         * ARENA_SIZE must be multiple of 64 pages or this will fail
         */
        static constexpr size_t BITMAP_SIZE = TOTAL_PAGES / 64;
        static_assert(TOTAL_PAGES % 64 == 0, "Arena size must be a multiple of 64 pages");

        uint64_t bitmap[BITMAP_SIZE];
        std::mutex bitmap_mutex;

        /* The beginning address of arena */
        void *arena_base;
        std::atomic<size_t> used_pages{0};
        
        /* record the idx where we found last free page
         * next search won't start from idx 0, beneficial for continuous alloc 
         */
        size_t last_searched_idx;

        size_t high_watermark;
        size_t min_watermark;
        size_t low_watermark;

        std::mutex lru_mutex;
        Page* lru_head{nullptr};
        Page* lru_tail{nullptr};

        /*----------------------helper function--------------------------*/
        void* alloc_a_page_nocheck();
        
    public:
        Arena();
        ~Arena();

        std::mutex sweeper_mutex;
        std::atomic<bool> is_sweeper_running{false};
        std::condition_variable sweeper_cv;

        /* Allocate one page from arena, returns page-aligned address */
        void* alloc_a_page();
        /* simply set the bitmap to mark page free */
        void free_a_page(void *raw_page_base);
        /* translate a raw address to page id */
        uint32_t get_page_id(void* raw_addr);

        void add_to_lru(void* page_base);
        void remove_from_lru(void* page_base);
        void lift_in_lru(void* page_base);
        void* get_lru_tail();

        bool needs_sweeping() const { return used_pages.load() >= low_watermark; }
        bool is_critical() const { return used_pages.load() >= min_watermark; }
        bool is_safe() const { return used_pages.load() <= high_watermark; }
};

/* helper function. The functions are not a member of SCM for flexibilty reasons */
void mark_slot_hot(void* slot_addr);
void mark_slot_cold(void* slot_addr);
/* helper function. Input: addr of a slot. Output: struct page the slot belonging to */
Page* get_struct_page(void* slot_addr);
}// namespace imdb
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
#define IS_HOT_ARR_LENGTH 4
#define IS_ALLOCATED_ARR_LENGHT 4

/* The manager of arena.
 * V1.0: uses bitmap to track free pages
 */

namespace imdb{ 
struct Page{
    static constexpr size_t PAGE_SIZE = 4096;

    struct{
        struct{
            Page* prev{nullptr};
            Page* next{nullptr};
        };
        struct{
            Page* lru_prev{nullptr};
            Page* lru_next{nullptr};
        };
        // uint32_t page_id; //TODO: delete this?

        uint16_t slot_size;
        uint16_t max_slots; // dynamically set by SizeClassManager::init_page()
        std::atomic<uint16_t> used;    // counter of in-use slots

        /* The size header takes. Calculated dynamically at init_page() */
        uint16_t header_reserved;
    }header;

    /* array that tracks records usage. Recently accessed record should have is_hot bit == 1.
     * Record header is 8 bytes, so SC begins with 16 bytes. Making a page containing at most 254 records.
     */
    std::atomic<uint64_t> is_hot[IS_HOT_ARR_LENGTH];
    /* TODO: added this just to prevent a deadlock bug. See StorageEngine.cpp: page_hot_rescue. Free slot bug */
    std::atomic<uint64_t> is_allocated[IS_ALLOCATED_ARR_LENGHT];

    /* ------------------Helper functions--------------------- */
    /* return the idx of slot. Perform zero check on the slot addr passed in */
    inline uint16_t get_slot_idx_nocheck(void* slot_addr){
        uintptr_t slot_addr_ = reinterpret_cast<uintptr_t>(slot_addr);
        return ((slot_addr_ - PAGE_ALIGN(slot_addr_)) - header.header_reserved) / header.slot_size;
    }
    inline char* get_slot_addr(uint16_t idx){
        return reinterpret_cast<char*>(this) + header.header_reserved + (idx * header.slot_size);
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

        std::atomic<uint64_t> bitmap[BITMAP_SIZE];

        /* The beginning address of arena */
        void *arena_base;
        std::atomic<size_t> used_pages{0};
        
        /* record the idx where we found last free page
         * next search won't start from idx 0, beneficial for continuous alloc 
         */
        std::atomic<size_t> last_searched_idx;

        size_t high_watermark;
        size_t min_watermark;
        size_t low_watermark;

        std::mutex lru_mutex;
        Page* lru_head{nullptr};
        Page* lru_tail{nullptr};
        
    public:
        Arena();
        ~Arena();

        std::mutex sweeper_mutex;
        std::atomic<bool> is_sweeper_running{false};
        std::condition_variable sweeper_cv;

        /* Allocate one page from arena, returns page-aligned address. 
         * Will checks for watermarks, might sleep */
        void* alloc_a_page();
        /* simply set the bitmap to mark page free */
        void free_a_page(void *raw_page_base);
        /* don't check matermarks, never sleep. Might return nullptr. */
        void* alloc_a_page_nocheck();

        void add_to_lru(void* page_base);
        void remove_from_lru(void* page_base);
        void lift_in_lru(void* page_base);
        void* get_lru_tail();

        /*----------------------helper function--------------------------*/
        bool needs_sweeping() const { return used_pages.load() >= low_watermark; }
        bool is_critical() const { return used_pages.load() >= min_watermark; }
        bool is_safe() const { return used_pages.load() <= high_watermark; }

        /* translate a raw address to page id */
        uint32_t get_page_id(void* raw_addr);
};
}// namespace imdb
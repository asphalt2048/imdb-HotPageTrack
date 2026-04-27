/* Arene is the 'global' pool of pages this DB uses. SizeClasses will allocate/free
 * pages inside arena. Pages that are swapped out should also be returned here.
 */
#pragma once

#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <sys/mman.h>

#define RED     "\033[31m"  /* text color: red */
#define RESET   "\033[0m"   /* reset color */ 
#define PAGE_ALIGN(x) ((x) & ~(4096ULL - 1))

/* The manager of arena.
 * V1.0: uses bitmap to track free pages
 */

namespace imdb{ 

class Arena{
    private:
        /* V1.0: statically define the arena size */
        static constexpr size_t ARENA_SIZE = 1024*1024*8;
        static constexpr size_t PAGE_SIZE = 4096;
        static constexpr size_t TOTAL_PAGES = ARENA_SIZE / PAGE_SIZE;
        /* devided by 64 as bitmap is defined as uint64_t 
         * ARENA_SIZE must be multiple of 64 pages or this will fail
         */
        static constexpr size_t BITMAP_SIZE = TOTAL_PAGES / 64;
        static_assert(TOTAL_PAGES % 64 == 0, "Arena size must be a multiple of 64 pages");

        uint64_t bitmap[BITMAP_SIZE];

        /* The beginning address of arena */
        void *arena_base;
        
        /* record the idx where we found last free page
         * next search won't start from idx 0, beneficial for continuous alloc 
         */
        size_t last_searched_idx;
    public:
        Arena();
        ~Arena();

        /* Allocate one page from arena, returns page-aligned address */
        void* alloc_a_page();
        void free_a_page(void *raw_addr);
        /* translate a raw address to page id */
        uint32_t get_page_id(void* raw_addr);

};
}// namespace imdb
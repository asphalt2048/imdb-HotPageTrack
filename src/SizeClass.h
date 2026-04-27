/* SizeClass.h 
 * A size class is a group of pages storing records of same power-of-2 size
 * for example, size class 64 byte will store and only store record of (32 byte, 64 byte]
 * 
 * Size class is used for smaller sized records(<512 byte, a quater of PAGE_SIZE)
 * for larger sized record, use LargeObjectManager, which need to use off-page metadata, 
 * and different allocation strategy.
 * 
 * TODO: implement LargeObjectManager
 */
#pragma once

#include <stdint.h>
#include <stddef.h>
#include "Arena.h"

#define SLOT_END 0XFFFF

namespace imdb{

struct Page{
    static constexpr size_t PAGE_SIZE = 4096;

    struct{
        Page* prev;
        Page* next;
        uint32_t page_id;

        uint16_t first_free_idx;

        uint16_t slot_size;
        uint16_t max_slots; // dynamically set by init_page()
        uint16_t in_use;    // counter of in-use slots

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
    /* slots in managed by internal free list. This function returns the pointer to the "next_free" pointer in a slot */
    inline uint16_t& next_free(uint16_t idx){
        return *reinterpret_cast<uint16_t*>(get_slot_addr(idx));
    }
};

/* Manager of one kind of size class, allocates and frees slots 
 */
class SizeClassManager{
    private:
        uint16_t slot_size;
        /* list head of partially full pages
         * A SizeClassManager only needs to track partially full pages
         * a free page should be returned to arena
         * a full page can not longer be allocated from thus not tracked
         */
        Page *partial_list_head;

        Arena& arena;

        void push_to_partial_list(Page *page);
        void remove_from_partial_list(Page *page);

        Page* get_a_page_from_arena();
        void return_a_page_to_arena(Page* page);

        /* init a page with the correct format, setup interal free slots list */
        Page* init_page(void* raw_page_base);

    public:
        SizeClassManager(uint16_t slot_size, Arena& arena): slot_size(slot_size), arena(arena) {partial_list_head = nullptr;}
        ~SizeClassManager() = default;
        
        /* alloate a slot, if page becomes full, stop tracking the page */
        void* alloc();
        /* free a slot, if page become free, return it to the arena*/
        void free(void* raw_addr);
};

/* helper function. It's not bind to a size class(not a member funtion) for flexibilty reasons */
void mark_slot_hot(void* slot_addr);
}// namespace imdb
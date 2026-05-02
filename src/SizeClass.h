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
#include <shared_mutex>
#include "Arena.h"

#define SLOT_END 0XFFFF

namespace imdb{
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

        std::shared_mutex rw_lock;

        void push_to_partial_list(Page *page);
        void remove_from_partial_list(Page *page);

        Page* get_a_page();
        void return_a_page(Page* page);

        /* init a page with the correct format, setup interal free slots list */
        Page* init_page(void* raw_page_base);

    public:
        SizeClassManager(uint16_t slot_size, Arena& arena): slot_size(slot_size), arena(arena) {partial_list_head = nullptr;}
        ~SizeClassManager() = default;
        
        /* alloate a slot, if page becomes full, stop tracking the page
         * will ask arena for new page if needed, might sleep. Not supposed to fail
         */
        void* alloc();
        /* free a slot, if page become free, return it to the arena*/
        void free(void* raw_addr);
        /* allocate a slot if SCM have free space, never ask new page. Might fail */
        void* alloc_notrigger();
        /* same as remove_from_partial_list, used by sweeper thread to isolate page */
        void quarantine_page(Page* page);
        /* same as push_to_partial_list, used by sweeper to return a not fully cleared page */
        void unquarantine_page(Page* page);

        /* Called ONLY by the Sweeper when a page is being forcefully evicted to disk.
         * This is not a confortable solution. As sweeper have to touch a SCM when evicting a page, 
         * even though it touch it only to call this function.
         */
        void reclaim_evicted_page(Page* page) {
            if (page->header.used > 0 && page->header.used < page->header.max_slots) {
                remove_from_partial_list(page);
            }
            arena.remove_from_lru(reinterpret_cast<void*>(page));
            arena.free_a_page(reinterpret_cast<void*>(page));
        }
};
}// namespace imdb
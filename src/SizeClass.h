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

namespace imdb {
template <size_t size>
struct Page;

/* templete is used here so that one definition of slot can be used for all size class*/
template<size_t size>
union Slot{
    // use internal free list to track free slots. when slot is free, the first 16bits should be idx to next free slot
    uint16_t next_free;
    char data[size];
};

template<size_t size>
struct PageHeader{
    Page<size> *prev;
    Page<size> *next;

    uint32_t page_id;
    // idx to first free slot, 0XFFFF means no free slot is out there
    uint16_t first_free_idx;
    // counter of used slots
    uint16_t used;
    uint16_t record_size;
};

template<size_t size>
struct Page{
    static constexpr size_t PAGE_SIZE = 4096;
    /* is_hot array is of type uint64_t, thus devided by 64 */
    static constexpr size_t ARRAY_SIZE = (PAGE_SIZE/size + 63) / 64;
    static constexpr size_t HEADER_SLOTS = (sizeof(PageHeader<size>) +  ARRAY_SIZE*sizeof(uint64_t)+ size - 1) / size;
    static constexpr size_t HEADER_RESERVED = size * HEADER_SLOTS;
    static constexpr size_t MAX_SLOTS = (PAGE_SIZE - HEADER_RESERVED) / size;

    union{
        struct{
            PageHeader<size> header;
            uint64_t is_hot[ARRAY_SIZE];
        };
        char padding[HEADER_RESERVED];
    };

    Slot<size> slots[MAX_SLOTS];
};

/* Manager of one kind of size class, allocates and frees slots 
 */
template<size_t size>
class SizeClassManager{
    private:
        /* list head of partially full pages
         * A SizeClassManager only needs to track partially full pages
         * a free page should be returned to arena
         * a full page can not longer be allocated from thus not tracked
         */
        Page<size> *partial_list_head;

        Arena& arena;

        void push_to_partial_list(Page<size> *page){
            page->header.prev = nullptr;
            page->header.next = partial_list_head;

            if(partial_list_head){ partial_list_head->header.prev = page; }
            partial_list_head = page;
        }

        void remove_from_partial_list(Page<size> *page){
            if (page->header.prev) {
                page->header.prev->header.next = page->header.next;
            } else {
                partial_list_head = page->header.next; 
            }
            if (page->header.next) {
                page->header.next->header.prev = page->header.prev;
            }
            page->header.prev = nullptr;
            page->header.next = nullptr;
        }

        Page<size>* get_a_page_from_arena(){
            /* TODO: batch alloc? */
            void* raw_addr = arena.alloc_a_page();
            /* V0.1: This should never be nullptr, alloc_a_page must be successful */
            if(raw_addr == nullptr){
                exit(-1);
            }

            auto page = init_page(raw_addr);
            push_to_partial_list(page);

            return page;
        }

        void return_a_page_to_arena(Page<size>* page){
            remove_from_partial_list(page);
            arena.free_a_page(reinterpret_cast<void *>(page));
        }

        /* init a page with the correct format, setup free slots list
         * TODO: what about SizeClass that stores more than PAGE_SIZE?
         */
        Page<size>* init_page(void* raw_page_base){
            Page<size>* page = reinterpret_cast<Page<size>*>(raw_page_base);
            page->header.first_free_idx = 0;
            page->header.used = 0;
            page->header.next = nullptr;
            page->header.prev = nullptr;
            page->header.page_id = arena.get_page_id(raw_page_base);

            /* setup is_hot array */
            for(int i=0; i < Page<size>::ARRAY_SIZE; i++){
                page->is_hot[i] = 0ULL;
            }

            /* setup slots' internal free list */
            for(int i=0; i<Page<size>::MAX_SLOTS - 1; i++){
                page->slots[i].next_free = i+1;
            }
            page->slots[Page<size>::MAX_SLOTS - 1].next_free = SLOT_END;

            return page;
        }

    public:
        SizeClassManager(Arena& arena): arena(arena) {partial_list_head = nullptr;}
        ~SizeClassManager() = default;
        
        /* alloate a slot, if page becomes full, stop tracking the page */
        void* alloc(){
            Page<size> *page = partial_list_head;

            /* SizeClass out of usable pages, ask page from the arena */
            if(!page){
                page = get_a_page_from_arena();
            }

            uint16_t slot_idx = page->header.first_free_idx;
            page->header.first_free_idx = page->slots[slot_idx].next_free;
            page->header.used++;

            if(page->header.used == Page<size>::MAX_SLOTS){
                remove_from_partial_list(page);
            }

            return static_cast<void*>(page->slots[slot_idx].data);
        }

        /* free a slot, if page become free, return it to the arena*/
        void free(void* raw_addr){
            uintptr_t addr = reinterpret_cast<uintptr_t>(raw_addr);

            uintptr_t page_addr = PAGE_ALIGN(addr);
            Page<size> *page = reinterpret_cast<Page<size>*>(page_addr);

            size_t offset = addr - page_addr;
            uint16_t slot_idx = (offset - Page<size>::HEADER_RESERVED) / size;

            /* link the slot back to free list */
            page->slots[slot_idx].next_free = page->header.first_free_idx;
            page->header.first_free_idx = slot_idx;

            if(page->header.used == Page<size>::MAX_SLOTS){
                push_to_partial_list(page);
            }

            page->header.used--;

            if(page->header.used == 0){
                return_a_page_to_arena(page);
            }

            return;
        }
};

/* helper function. It's not bind to a size class(not a member funtion) for flexibilty reasons */
template<size_t slot_size>
void mark_slot_hot(void* slot_addr){
    uintptr_t slot_addr_ = reinterpret_cast<uintptr_t>(slot_addr);
    uintptr_t page_base = PAGE_ALIGN(slot_addr_);

    Page<slot_size>* page = reinterpret_cast<Page<slot_size>*>(page_base);

    uint16_t slot_id = ((slot_addr_-page_base) - Page<slot_size>::HEADER_RESERVED) / slot_size;

    size_t arr_idx = slot_id / 64;
    size_t bit_idx = slot_id % 64;

    page->is_hot[arr_idx] |= (1ULL << bit_idx);
};
}// namespace imdb
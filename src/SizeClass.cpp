#include "SizeClass.h"

namespace imdb{
void SizeClassManager::push_to_partial_list(Page *page){
    page->header.prev = nullptr;
    page->header.next = partial_list_head;

    if(partial_list_head){ partial_list_head->header.prev = page; }
    partial_list_head = page;
}

void SizeClassManager::remove_from_partial_list(Page *page){
    if (page->header.prev){
        page->header.prev->header.next = page->header.next;
    }else{
        partial_list_head = page->header.next; 
    }
    if (page->header.next){
        page->header.next->header.prev = page->header.prev;
    }
    page->header.prev = nullptr;
    page->header.next = nullptr;
}

Page* SizeClassManager::get_a_page(){
    /* TODO: batch alloc? */
    void* raw_addr = arena.alloc_a_page();
    /* V0.1: This should never be nullptr, alloc_a_page must be successful */
    if(raw_addr == nullptr){
        std::cerr<<RED<<"Arena out of memory!\n"<<RESET;
        exit(-1);
    }

    Page* page = init_page(raw_addr);
    // TODO: adding and removing here? Or elsewhere?
    // should partial pages be in lru?
    arena.add_to_lru(page);
    push_to_partial_list(page);

    return page;
}

void SizeClassManager::return_a_page(Page* page){
    remove_from_partial_list(page);
    arena.remove_from_lru(page);
    arena.free_a_page(reinterpret_cast<void *>(page));
}

/* init a page with the correct format, setup interal free slots list */
Page* SizeClassManager::init_page(void* raw_page_base){
    Page* page = reinterpret_cast<Page*>(raw_page_base);
    page->header.first_free_idx = 0;
    page->header.used = 0;
    page->header.next = nullptr;
    page->header.prev = nullptr;
    page->header.lru_pointers.lru_prev = nullptr;
    page->header.lru_pointers.lru_next = nullptr;
    page->header.page_id = arena.get_page_id(raw_page_base);
    page->header.slot_size = this->slot_size;

    /* dynamically sets up header_reserved and max_slots */
    size_t header_size = sizeof(Page);
    size_t header_slots = (header_size + slot_size - 1) / slot_size;
    page->header.header_reserved = header_slots * slot_size;
    page->header.max_slots = (Page::PAGE_SIZE - page->header.header_reserved) / slot_size;

    /* setup is_hot array */
    for(int i=0; i < 4; i++){
        page->is_hot[i] = 0ULL;
    }

    /* setup slots' internal free list */
    for(int i=0; i<page->header.max_slots - 1; i++){
        page->next_free(i) = i+1;
    }
    page->next_free(page->header.max_slots-1) = SLOT_END;

    return page;
}

/* alloate a slot, if page becomes full, stop tracking the page */
void* SizeClassManager::alloc(){
    Page *page = partial_list_head;

    /* SizeClass out of usable pages, ask page from the arena */
    if(!page){
        page = get_a_page();
    }

    uint16_t slot_idx = page->header.first_free_idx;
    page->header.first_free_idx = page->next_free(slot_idx);
    page->header.used++;

    if(page->header.used == page->header.max_slots){
        remove_from_partial_list(page);
    }

    return static_cast<void*>(page->get_slot_addr(slot_idx));
}

/* free a slot, if page become free, return it to the arena*/
void SizeClassManager::free(void* raw_addr){
    uintptr_t addr = reinterpret_cast<uintptr_t>(raw_addr);

    uintptr_t page_addr = PAGE_ALIGN(addr);
    Page *page = reinterpret_cast<Page*>(page_addr);

    size_t offset = addr - page_addr;
    uint16_t slot_idx = (offset - page->header.header_reserved) / page->header.slot_size;

    /* link the slot back to free list */
    page->next_free(slot_idx) = page->header.first_free_idx;
    page->header.first_free_idx = slot_idx;

    /* if page becomes partial, put to partial list */
    if(page->header.used == page->header.max_slots){
        push_to_partial_list(page);
    }

    page->header.used--;

    /* if page becomes empty, return it to the arena */
    if(page->header.used == 0){
        return_a_page(page);
    }

    return;
}
}// namespace imdb
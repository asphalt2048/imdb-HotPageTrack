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
}

Arena::~Arena(){ munmap(arena_base, ARENA_SIZE); }

void* Arena::alloc_a_page(){
    for(int i = 0; i<BITMAP_SIZE; i++){
        size_t idx = (last_searched_idx + i) % BITMAP_SIZE;

        /* bitmap[idx] is not full, i.e. not 0xFFFFFFFFFFFFFFFF */
        if(bitmap[idx] != ~0ULL){
            unsigned int free_bit = __builtin_ffsll(~bitmap[idx]) - 1;

            bitmap[idx] |= (1ULL << free_bit);
            last_searched_idx = idx;

            size_t page_id = 64*idx + free_bit;
            uintptr_t page_base_addr  = reinterpret_cast<uintptr_t>(arena_base) + page_id*PAGE_SIZE;

            return reinterpret_cast<void *>(page_base_addr);
        }
    }
    // TODO: out-of-mem management: might call for swapd to swap pages out
    std::cerr<<RED<<"alloc_a_page() failed, arena out of memory\n";
    exit(-1);
}

void Arena::free_a_page(void *raw_addr){
    uintptr_t arena_base_ = reinterpret_cast<uintptr_t>(arena_base);
    uintptr_t addr = reinterpret_cast<uintptr_t>(raw_addr);

    if(addr<arena_base_ || addr>=arena_base_ + ARENA_SIZE){
        std::cerr<<RED<<"err: Arena::free_a_page(), page not belongs to arena\n"<<RESET;
        exit(-1);
    }
    
    /* TODO: usually raw_addr should already be page-aligned, might remove this line */
    uintptr_t page_base = PAGE_ALIGN(addr);

    size_t page_id = (page_base - arena_base_) / PAGE_SIZE;

    unsigned int idx = page_id / 64;
    unsigned int free_bit = page_id % 64;

    bitmap[idx] &= ~(1ULL << free_bit);

    if(idx < last_searched_idx){ last_searched_idx = idx; } 
    return;
}

uint32_t Arena::get_page_id(void* raw_addr){
    uintptr_t addr = reinterpret_cast<uintptr_t>(raw_addr);
    uintptr_t base = reinterpret_cast<uintptr_t>(arena_base);
    return (addr - base) / PAGE_SIZE;
}

}//namesapce imdb
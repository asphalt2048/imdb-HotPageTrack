/* EvictionSweeper.h */
#pragma once
#include <thread>
#include <atomic>
#include <functional>
#include "Arena.h"

namespace imdb {

class EvictionSweeper{
private:
    Arena& arena;
    std::thread sweeper_thread;
    std::atomic<bool> shutdown_flag{false};
    
    /* actual eviction logic must be provided by other component,
     * The sweeper is ignorant of eviciton algorithm.
     */
    std::function<void()> evict_callback;

    void evict_loop();

public:
    // Constructor requires the arena and the eviction function
    EvictionSweeper(Arena& a, std::function<void()> evict_func);
    
    ~EvictionSweeper();
};

} // namespace imdb
/* Disk manager, handles basic disk IO
 * TODO: might consider moving to io_uring
 */

#pragma once
#include <string>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>

namespace imdb {

    class DiskManager {
    private:
        int fd;
        static constexpr size_t PAGE_SIZE = 4096;

        /* A simple counter to track the end of the file.
           TODO:  bitmap/free-list to reuse deleted disk slots. */
        size_t next_disk_page_id; 

    public:
        DiskManager(const std::string& file_path);
        ~DiskManager();

        // The I/O Workhorses
        void write_page(size_t disk_page_id, const void* page_data);
        void read_page(size_t disk_page_id, void* page_data);

        /* Ask the disk for a new place to put a page */
        size_t allocate_disk_page();
    };

} // namespace imdb
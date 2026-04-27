#include "DiskManager.h"
#include <cstring>

#define RED "\033[31m"
#define RESET "\033[0m"

namespace imdb {

    DiskManager::DiskManager(const std::string& file_path) : next_disk_page_id(0) {
        /* 0666 sets read/write permissions for the OS. */
        fd = ::open(file_path.c_str(), O_RDWR | O_CREAT, 0666);
        if (fd < 0) {
            std::cerr<<RED<<"DiskManager: Failed to open swap file: "<<file_path<<RESET<<"\n";
            exit(-1);
        }
    }

    DiskManager::~DiskManager() {
        if (fd >= 0) {
            ::close(fd);
        }
    }

    size_t DiskManager::allocate_disk_page() {
        /* TODO: use freelist */
        return next_disk_page_id++;
    }

    void DiskManager::write_page(size_t disk_page_id, const void* page_data) {
        size_t offset = disk_page_id * PAGE_SIZE;
        
        ssize_t bytes_written = ::pwrite(fd, page_data, PAGE_SIZE, offset);
        
        if (bytes_written != PAGE_SIZE) {
            std::cerr<<RED<<"DiskManager: Critical I/O Error on write!\n"<<RESET;
            exit(-1);
        }
    }

    void DiskManager::read_page(size_t disk_page_id, void* page_data) {
        size_t offset = disk_page_id * PAGE_SIZE;
        
        ssize_t bytes_read = ::pread(fd, page_data, PAGE_SIZE, offset);
        
        if (bytes_read != PAGE_SIZE) {
            std::cerr<<RED<<"DiskManager: Critical I/O Error on read!\n"<<RESET;
            exit(-1);
        }
    }

} // namespace imdb
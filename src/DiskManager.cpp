#include "DiskManager.h"
#include <iostream>
#include <fcntl.h>
#include <unistd.h>  
#include <sys/stat.h>

#define RED     "\033[31m"  /* text color: red */
#define RESET   "\033[0m"   /* reset color */ 

namespace imdb {

DiskManager::DiskManager(const std::string& db_file_path) {
    // 0666 sets standard file permissions (rw-rw-rw-)
    db_fd = open(db_file_path.c_str(), O_RDWR | O_CREAT, 0666);
    
    if (db_fd < 0) {
        std::cerr<<RED<<"FATAL: DiskManager: failed to open file: "<<db_file_path<<"\n"<<RESET;
        exit(-1);
    }

    // If booting up using an existing file, get the file's end
    off_t size = lseek(db_fd, 0, SEEK_END);
    if (size < 0) {
        std::cerr<<RED<< "FATAL: DiskManager: failed to seek end of file.\n"<<RESET;
        exit(-1);
    }
    
    file_end.store(static_cast<size_t>(size));
}

DiskManager::~DiskManager() {
    if (db_fd >= 0) {
        sync();        // Force any remaining OS buffers to the SSD
        close(db_fd); 
    }
}

size_t DiskManager::write_record(const char* data, size_t size){
    // TODO: use buffer or optimize this lock
    std::lock_guard<std::mutex> lock(write_lock);

    size_t write_offset = file_end.load();

    ssize_t bytes_written = pwrite(db_fd, data, size, write_offset);

    if (bytes_written != static_cast<ssize_t>(size)) {
        std::cerr<<RED<<"Disk write failed or partial write!\n"<<RESET;
        // TODO: fault handling
        return -1; 
    }

    file_end.fetch_add(size);

    return write_offset;
}

bool DiskManager::read_record(size_t offset, char* buffer, size_t size){
    ssize_t bytes_read = pread(db_fd, buffer, size, offset);

    if (bytes_read != static_cast<ssize_t>(size)) {
        std::cerr << "Failed to read record from disk at offset: " << offset << "\n";
        return false;
    }

    return true;
}

void DiskManager::sync() {
    fsync(db_fd);
}

} // namespace imdb
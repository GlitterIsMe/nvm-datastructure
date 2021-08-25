//
// Created by zzyyyww on 2021/8/24.
//

#ifndef CCEH_LOG_H
#define CCEH_LOG_H


#include <libpmem.h>
#include <cstdint>
#include <string>
#include <atomic>

namespace pm {


    using PmAddr = uint64_t;
    class LogStore {
    public:
        explicit LogStore(const std::string& pm_path, uint64_t pm_size){
            raw_ = (char *) pmem_map_file(pm_path.c_str(), pm_size, PMEM_FILE_CREATE, 0666, &mapped_len_, &is_pmem_);
            if (raw_ == nullptr) {
                fprintf(stderr, "map file failed [%s]\n", strerror(errno));
                exit(-1);
            }
            tail_ = 0;
        }
        ~LogStore(){
            pmem_unmap(raw_, mapped_len_);
        }

        PmAddr Alloc(size_t size){
            return tail_.fetch_add(size, std::memory_order_relaxed);
        }
        void Append(PmAddr offset, const std::string& payload){
            pmem_memcpy_persist(raw_ + offset, payload.c_str(), payload.size());
        }

        char* raw() const {return raw_;}

    private:
        char* raw_;
        std::atomic<uint64_t> tail_;
        size_t mapped_len_;
        int is_pmem_;
    };
}

#endif //CCEH_LOG_H


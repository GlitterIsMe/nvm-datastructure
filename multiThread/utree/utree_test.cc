//
// Created by zzyyyww on 2021/8/24.
#include <string>

#include "utree.h"
#include "log.h"

size_t standard(const void* _ptr, size_t _len,
                       size_t _seed=static_cast<size_t>(0xc70f6907UL)){
    return std::_Hash_bytes(_ptr, _len, _seed);
}

std::string GenerateRandomKey(uint64_t sequence){
    uint64_t key_num = standard(&sequence, sizeof(uint64_t), 0xc70697UL);
    std::string key_num_str = std::to_string(key_num);
    int zeros = 16 - key_num_str.length();
    zeros = std::max(0, zeros);

    std::string raw = std::string("user").append(zeros, '0').append(key_num_str);
    uint64_t size = raw.size();
    std::string raw_size((char*)(&size), sizeof(uint64_t));
    //return std::string("user").append(zeros, '0').append(key_num_str);
    return raw_size + raw;
}

int main() {
    btree* tree = new btree();
    pm::LogStore* log = new pm::LogStore("./pmem", 1024UL * 1024UL * 1024UL);
    int num_entries = 1000;

    // insert
    for (int i = 1; i < num_entries; i++) {
        std::string raw_key = GenerateRandomKey(i);
        pm::PmAddr addr = log->Alloc(raw_key.size());
        log->Append(addr, raw_key);
        tree->insert((uint64_t)(log->raw() + addr), (char*)i);
    }
    printf("insert [%d]\n", num_entries - 1);
    //get
    int search_found = 0;
    int search_miss = 0;
    for (int i = 1; i < num_entries; i++) {
        std::string raw_key = GenerateRandomKey(i);
        char *lookup_key = new char[raw_key.size()];
        memcpy(lookup_key, raw_key.c_str(), raw_key.size());
        char* res = tree->search((entry_key_t)lookup_key);
        if ((uint64_t)res == i) {
            search_found++;
        } else {
            search_miss++;
        }
        delete[] lookup_key;
        printf("found [%d], not found [%d]\n", search_found, search_miss);
    }

    //scan
    std::string raw_key = GenerateRandomKey(1);
    char *lookup_key = new char[raw_key.size()];
    memcpy(lookup_key, raw_key.c_str(), raw_key.size());
    uint64_t *scan_res = new uint64_t [num_entries];
    tree->scan((entry_key_t)lookup_key, num_entries-1, scan_res);
    delete[] lookup_key;
    delete[] scan_res;

}


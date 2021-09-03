//
// Created by zzyyyww on 2021/8/24.
#include <string>
#include <iostream>

#include "udb.h"

using namespace utree;

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
    uDB* db = new uDB("./pmem", 1024UL * 1024UL * 1024UL);
    int num_entries = 1000;

    // insert
    for (int i = 1; i < num_entries; i++) {
        std::string raw_key = GenerateRandomKey(i);
        std::string prefix(std::to_string(i % 20) + "-");
        db->Put(prefix + raw_key, std::to_string(i));
        //printf("insert [%s, %d]\n", (prefix + raw_key).c_str(), i);
    }
    printf("insert [%d]\n", num_entries - 1);
    //get
    int search_found = 0;
    int search_miss = 0;
    for (int i = 1; i < num_entries; i++) {
        std::string raw_key = GenerateRandomKey(i);
        std::string prefix(std::to_string(i % 20) + "-");
        std::string value;
        bool res = db->Get(prefix + raw_key, &value);
        if (res) {
            search_found++;
        } else {
            search_miss++;
        }
    }
    printf("found [%d], not found [%d]\n", search_found, search_miss);

    //scan
    std::string raw_key = GenerateRandomKey(10);
    std::string prefix(std::to_string(10 % 20) + "-");
    std::cout << prefix << std::endl;
    std::vector<std::string> res;
    db->Scan(prefix + raw_key, res);
    int count = 0;
    for (auto item : res) {
        printf("%d.found [%s]\n",count++, item.c_str());
    }
}


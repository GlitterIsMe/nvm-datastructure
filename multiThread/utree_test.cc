//
// Created by zzyyyww on 2021/8/24.
#include <string>
#include <iostream>

#include "tree_db.h"

#ifdef UTREE
#include "utree/utree.h"
using namespace utree;
#endif

#ifdef FASTFAIR
#include "fast_fair/btree.h"
using namespace fastfair;
#endif

using namespace treedb;

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

    return raw;
}

int main() {
    TreeDB* db = new TreeDB("/mnt/pmem/log_pool", 1024UL * 1024UL * 1024UL);
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

    //preix scan
    {
        std::string raw_key = GenerateRandomKey(10);
        std::string prefix(std::to_string(10 % 20) + "-");
        std::cout << prefix << std::endl;
        std::vector<KVPair> res;
        db->Scan(prefix + raw_key, res);
        int count = 0;
        for (auto item : res) {
            printf("%d.found [%s-%s]\n",count++, item.first.c_str(), item.second.c_str());
        }
    }

    // normal scan
    {
        std::string raw_key = GenerateRandomKey(10);
        std::string prefix(std::to_string(10) + "-");
        std::vector<KVPair> res;
        db->Scan(prefix + raw_key, 10, res);
        int count = 0;
        for (auto item : res) {
            printf("%d.found [%s-%s]\n",count++, item.first.c_str(), item.second.c_str());
        }
    }

}


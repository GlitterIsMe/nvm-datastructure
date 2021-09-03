//
// Created by zzyyyww on 2021/9/2.
//

#include "udb.h"
#include "utree.h"
#include "log.h"
#include "global_log.h"

namespace utree {



    const int LEN_SIZE = 8;

    bool ShouldStop(const std::string& first_str, const std::string& str){
        //printf("prefix[%s], str[%s]\n", first_str.c_str(), str.substr(0, str.find('-')).c_str());
        return first_str == str.substr(0, str.find('-'));
    }

    uint64_t DecodeSize(const char* raw) {
        if (raw == nullptr) {
            return 0;
        }
        uint64_t* size = (uint64_t*)(raw);
        return *size;
    }

    uDB::uDB(std::string log_path, uint64_t log_size) {
        utree_ = new btree;
        global_log_ = new pm::LogStore(log_path, log_size);
    }

    uDB::~uDB() {
        delete utree_;
        delete global_log_;
    }

    bool uDB::Put(const std::string &key, const std::string &value) {
        uint64_t key_size = key.size();
        uint64_t value_size = value.size();
        uint64_t total_size = key.size() + value.size() + LEN_SIZE * 2;
        pm::PmAddr addr = global_log_->Alloc(total_size);
        char* raw = global_log_->raw() + addr;
        memcpy(raw, (char*)(&key_size), LEN_SIZE);
        memcpy(raw + LEN_SIZE, key.data(), key.size());
        memcpy(raw + LEN_SIZE + key_size, (char*)(&value_size), LEN_SIZE);
        memcpy(raw + LEN_SIZE * 2 + key_size, value.data(), value.size());
        pmem_persist((void*)raw, total_size);

        utree_->insert((entry_key_t)(raw), raw + LEN_SIZE + key_size);

        return true;
    }

    bool uDB::Get(const std::string &key, std::string *value) {
        bool found = false;
        uint64_t key_size = key.size();
        char* lookup = new char[key.size() + LEN_SIZE];
        memcpy(lookup, (char*)(&key_size), LEN_SIZE);
        memcpy(lookup + LEN_SIZE, key.data(), key.size());
        entry_key_t lookup_key = (entry_key_t)(lookup);
        auto ret = utree_->search(lookup_key);
        if (ret != nullptr) {
            uint64_t size = *(uint64_t*)(ret);
            *value = std::move(std::string((char*)ret + LEN_SIZE, size));
            found = true;
        }
        delete[] lookup;
        return found;
    }

    bool uDB::Delete(const std::string &key) {
        uint64_t key_size = key.size();
        char* lookup = new char[key.size() + LEN_SIZE];
        memcpy(lookup, (char*)(&key_size), LEN_SIZE);
        memcpy(lookup + LEN_SIZE, key.data(), key.size());
        entry_key_t lookup_key = (entry_key_t)(lookup);
        utree_->remove(lookup_key);
        return true;
    }

    bool uDB::Scan(const std::string &key, std::vector<KVPair> &values) {
        std::string value;
        std::string lookup_prefix = key.substr(0, key.find('-'));
        uint64_t prefix_size = lookup_prefix.size();
        char* lookup = new char[prefix_size + LEN_SIZE];
        memcpy(lookup, (char*)(&prefix_size), LEN_SIZE);
        memcpy(lookup + LEN_SIZE, lookup_prefix.data(), prefix_size);
        entry_key_t lookup_key = (entry_key_t)(lookup);

        list_node_t* node = utree_->scan(lookup_key);
        list_node_t* n = node;
        std::string tkey, tvalue;
        uint64_t key_size = DecodeSize((char*)node->key);
        tkey = std::string((char*)node->key + LEN_SIZE, key_size);
        uint64_t value_size = DecodeSize((char*)node->ptr);
        tvalue = std::string((char*)node->ptr + LEN_SIZE, value_size);
        //printf("scan start at [%s]\n", tkey.c_str());

        std::string prefix = tkey.substr(0, tkey.find('-'));
        if (node!= nullptr) {
            for (; ShouldStop(prefix, tkey) && n != nullptr; n = n->next) {
                values.push_back(std::move(KVPair(tkey,tvalue)));
                key_size = DecodeSize((char*)n->key);
                value_size = DecodeSize((char*)n->ptr);
                tkey = std::move(std::string((char*)n->key + LEN_SIZE, key_size));
                tvalue = std::move(std::string((char*)n->ptr + LEN_SIZE, value_size));
                //printf("scan get key [%s]\n", tkey.c_str());
            }
            return true;
        }
        return false;
    }

    bool uDB::Scan(const std::string &key, int range, std::vector<KVPair> &values) {
        uint64_t lookup_key_size = key.size();
        char* lookup = new char[lookup_key_size + LEN_SIZE];
        memcpy(lookup, (char*)(&lookup_key_size), LEN_SIZE);
        memcpy(lookup + LEN_SIZE, key.data(), lookup_key_size);
        entry_key_t lookup_key = (entry_key_t)(lookup);

        list_node_t* node = utree_->scan(lookup_key);
        list_node_t* n = node;
        std::string tkey, tvalue;
        uint64_t key_size = DecodeSize((char*)node->key);
        tkey = std::string((char*)node->key + LEN_SIZE, key_size);
        uint64_t value_size = DecodeSize((char*)node->ptr);
        tvalue = std::string((char*)node->ptr + LEN_SIZE, value_size);
        //printf("scan start at [%s]\n", tkey.c_str());

        if (node!= nullptr) {
            for (int i = 0; i < range && n != nullptr; n = n->next, i++) {
                values.push_back(std::move(KVPair(tkey,tvalue)));
                key_size = DecodeSize((char*)n->key);
                value_size = DecodeSize((char*)n->ptr);
                tkey = std::move(std::string((char*)n->key + LEN_SIZE, key_size));
                tvalue = std::move(std::string((char*)n->ptr + LEN_SIZE, value_size));
                //printf("scan get key [%s]\n", tkey.c_str());
            }
            return true;
        }
        return false;
    }
}

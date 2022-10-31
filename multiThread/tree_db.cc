//
// Created by zzyyyww on 2021/9/2.
//

#include "tree_db.h"
#include "log.h"
#include "global_log.h"

#ifdef UTREE
#include "utree/utree.h"
#endif

#ifdef FASTFAIR
#include "fast_fair/btree.h"
#endif

#include "../../latency_counter.h"

namespace treedb {

    const int LEN_SIZE = 8;

    bool ShouldStop(const std::string& first_str, const std::string& str){
        //printf("prefix[%s], str[%s]\n", first_str.c_str(), str.substr(0, str.find('-')).c_str());
        //return first_str == str.substr(0, 8);
        return str.find(first_str) != std::string::npos;
    }

    uint64_t DecodeSize(const char* raw) {
        if (raw == nullptr) {
            return 0;
        }
        uint64_t size;
        memcpy(&size, raw, 8);
        return size;
    }

#ifdef FASTFAIR
    fastfair::btree* tree_;
#endif

#ifdef UTREE
    utree::btree* tree_;
#endif

    TreeDB::TreeDB(std::string log_path, uint64_t log_size) {
#ifdef FASTFAIR
        tree_ = new fastfair::btree;
#endif
#ifdef UTREE
        tree_ = new utree::btree;
#endif
        global_log_ = new LogStore(log_path, log_size);
    }

    TreeDB::~TreeDB() {
        delete tree_;
        delete global_log_;
        printf("total allocated %lu bytes, %lf MB\n", utree::page::total_allocated, utree::page::total_allocated / 1024.0 / 1024.0);
        utree::print_pm_allocation();
    }

    bool TreeDB::Put(const std::string &key, const std::string &value) {
        uint64_t key_size = key.size();
        uint64_t value_size = value.size();
        uint64_t total_size = key.size() + value.size() + LEN_SIZE * 2;
        START
        PmAddr addr = global_log_->Alloc(total_size);
        char* raw = global_log_->raw() + addr;
        memcpy(raw, (char*)(&key_size), LEN_SIZE);
        memcpy(raw + LEN_SIZE, key.data(), key.size());
        memcpy(raw + LEN_SIZE + key_size, (char*)(&value_size), LEN_SIZE);
        memcpy(raw + LEN_SIZE * 2 + key_size, value.data(), value.size());
        pmem_persist((void*)raw, total_size);
        LOG_END
#ifdef FASTFAIR
        tree_->btree_insert((fastfair::entry_key_t)(raw), raw + LEN_SIZE + key_size);
#endif
#ifdef UTREE
        START
        tree_->insert((utree::entry_key_t)(raw), raw + LEN_SIZE + key_size);
        INDEX_END
#endif
        return true;
    }

    bool TreeDB::Get(const std::string &key, std::string *value) {
        START
        bool found = false;
        uint64_t key_size = key.size();
        char* lookup = new char[key.size() + LEN_SIZE];
        memcpy(lookup, (char*)(&key_size), LEN_SIZE);
        memcpy(lookup + LEN_SIZE, key.data(), key.size());
#ifdef FASTFAIR
        fastfair::entry_key_t lookup_key = (fastfair::entry_key_t)(lookup);
        auto ret = tree_->btree_search(lookup_key);
#endif
#ifdef UTREE
        utree::entry_key_t lookup_key = (utree::entry_key_t)(lookup);
        auto ret = tree_->search(lookup_key);
#endif
        INDEX_END
        if (ret != nullptr) {
            START
            uint64_t size = *(uint64_t*)(ret);
            *value = std::move(std::string((char*)ret + LEN_SIZE, size));
            found = true;
            LOG_END
        }
        delete[] lookup;
        return found;
    }

    bool TreeDB::Delete(const std::string &key) {
        START
        uint64_t key_size = key.size();
        char* lookup = new char[key.size() + LEN_SIZE];
        memcpy(lookup, (char*)(&key_size), LEN_SIZE);
        memcpy(lookup + LEN_SIZE, key.data(), key.size());
#ifdef FASTFAIR
        fastfair::entry_key_t lookup_key = (fastfair::entry_key_t)(lookup);
        tree_->btree_delete(lookup_key);
#endif
#ifdef UTREE
        utree::entry_key_t lookup_key = (utree::entry_key_t)(lookup);
        tree_->remove(lookup_key);
#endif
        INDEX_END
        return true;
    }

    bool TreeDB::Scan(const std::string &key, std::vector<KVPair> &values) {
        START
        std::string value;
        std::string lookup_prefix = key.substr(0, 8);
        uint64_t prefix_size = lookup_prefix.size();
        char* lookup = new char[prefix_size + LEN_SIZE];
        memcpy(lookup, (char*)(&prefix_size), LEN_SIZE);
        memcpy(lookup + LEN_SIZE, lookup_prefix.data(), prefix_size);

#ifdef UTREE
        utree::entry_key_t lookup_key = (utree::entry_key_t)(lookup);
        utree::list_node_t* node = tree_->scan(lookup_key);
        //printf("scan start at [%s]\n", tkey.c_str());
        //std::string prefix = tkey.substr(0, 8);
        INDEX_END
        START
        if (node!= nullptr) {
            utree::list_node_t* n = node;
            std::string tkey, tvalue;
            uint64_t key_size, value_size;
            key_size = DecodeSize((char*)node->key);
            tkey = std::string((char*)node->key + LEN_SIZE, key_size);
            value_size = DecodeSize((char*)node->ptr);
            tvalue = std::string((char*)node->ptr + LEN_SIZE, value_size);

            for (; ShouldStop(lookup_prefix, tkey) && n != nullptr; n = n->next) {
                key_size = DecodeSize((char*)n->key);
                value_size = DecodeSize((char*)n->ptr);
                tkey = std::move(std::string((char*)n->key + LEN_SIZE, key_size));
                tvalue = std::move(std::string((char*)n->ptr + LEN_SIZE, value_size));
                values.emplace_back(tkey,tvalue);

                //printf("scan get key [%s]\n", tkey.c_str());
            }
            delete[] lookup;
            LOG_END
            return true;
        }
        delete[] lookup;
        LOG_END
        return false;
#endif
#ifdef FASTFAIR
        fastfair::entry_key_t lookup_key = (fastfair::entry_key_t)(lookup);
        unsigned long* res;
        int res_num = 0;
        tree_-> scan(lookup_key, lookup_prefix.data(), lookup_prefix.size(), &res, &res_num);
        if (res != nullptr) {
            uint64_t key_size, value_size;
            std::string tkey, tvalue;
            for (int i = 0; i < res_num; ++i) {
                key_size = DecodeSize((char*)res[i]);
                value_size = DecodeSize((char*)res[i] + key_size + LEN_SIZE);
                tkey = std::move(std::string((char*)res[i] + LEN_SIZE, key_size));
                tvalue = std::move(std::string((char*)res[i] + key_size + LEN_SIZE * 2, value_size));
                values.emplace_back(KVPair(tkey, tvalue));
            }
        } else {
            delete[] res;
            return false;
        }

        delete[] res;
        return true;
#endif
    }

    bool TreeDB::Scan(const std::string &key, int range, std::vector<KVPair> &values) {
#ifdef UTREE
        uint64_t lookup_key_size = key.size();
        char* lookup = new char[lookup_key_size + LEN_SIZE];
        memcpy(lookup, (char*)(&lookup_key_size), LEN_SIZE);
        memcpy(lookup + LEN_SIZE, key.data(), lookup_key_size);
        utree::entry_key_t lookup_key = (utree::entry_key_t)(lookup);

        utree::list_node_t* node = tree_->scan(lookup_key);
        utree::list_node_t* n = node;
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
            delete[] lookup;
            return true;
        }
        delete[] lookup;
        return false;
#endif

#ifdef FASTFAIR
        uint64_t lookup_key_size = key.size();
        char* lookup = new char[lookup_key_size + LEN_SIZE];
        memcpy(lookup, (char*)(&lookup_key_size), LEN_SIZE);
        memcpy(lookup + LEN_SIZE, key.data(), lookup_key_size);
        fastfair::entry_key_t lookup_key = (fastfair::entry_key_t)(lookup);

        unsigned long *res = new unsigned long[range];
        tree_->scan(lookup_key, range, res);
        uint64_t key_size, value_size;
        std::string tkey, tvalue;
        for (int i = 0; i < range && res[i] != 0; i++) {
            key_size = DecodeSize((char*)res[i]);
            value_size = DecodeSize((char*)res[i] + key_size + LEN_SIZE);
            tkey = std::move(std::string((char*)res[i] + LEN_SIZE, key_size));
            tvalue = std::move(std::string((char*)res[i] + key_size + LEN_SIZE * 2, value_size));
            values.push_back(std::move(KVPair(tkey,tvalue)));
            //printf("scan get key [%s]\n", tkey.c_str());
        }
        delete[] lookup;
#endif
    }
}

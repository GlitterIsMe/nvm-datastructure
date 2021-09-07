//
// Created by zzyyyww on 2021/9/2.
//

#ifndef TREE_UDB_H
#define TREE_UDB_H

#include <string>
#include <vector>

namespace treedb {

    using KVPair = std::pair<std::string, std::string>;

    class btree;
    class log;

    class TreeDB {
    public:
        TreeDB(std::string log_path, uint64_t log_size);
        ~TreeDB();

        bool Put(const std::string& key, const std::string& value);
        bool Get(const std::string& key, std::string* value);
        bool Delete(const std::string& key);
        // prefix scan
        bool Scan(const std::string& key, std::vector<KVPair>& values);
        // normal scan
        bool Scan(const std::string& key, int range, std::vector<KVPair>& values);

    private:
    };
}

#endif //UTREE_UDB_H

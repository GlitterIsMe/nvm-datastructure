//
// Created by zzyyyww on 2021/9/2.
//

#ifndef UTREE_UDB_H
#define UTREE_UDB_H

#include <string>
#include <vector>

namespace utree {

    class btree;
    class log;

    class uDB {
    public:
        uDB(std::string log_path, uint64_t log_size);
        ~uDB();

        bool Put(const std::string& key, const std::string& value);
        bool Get(const std::string& key, std::string* value);
        bool Delete(const std::string& key);
        bool Scan(const std::string& key, std::vector<std::string>& values);

    private:
        btree* utree_;
    };
}

#endif //UTREE_UDB_H

//
// Created by zzyyyww on 2021/8/24.
//

#ifndef CCEH_RAW_KEY_H
#define CCEH_RAW_KEY_H

#include <cstdint>

/*Key value item format:
     *  |--key_size--|----key----|
     *  |-----8B-----|--variable-|
     * */
namespace treedb {

    inline int compare(char* a, char* b, uint64_t size_a, uint64_t size_b) {
        const size_t min_len = (size_a < size_b) ? size_a : size_b;
        int r = memcmp(a, b, min_len);
        if (r == 0) {
            if (size_a < size_b)
                r = -1;
            else if (size_a > size_b)
                r = +1;
        }
        return r;
    }
    /* INVALID key is the largest key
     * */
    class PtrKey {
    public:
        PtrKey() =default;
        explicit PtrKey(char* src) {
            if (src == nullptr) {
                valid_ = true;
                ptr_ = 0;
                size_ = 0;
            } else {
                ptr_ = (uint64_t)(src);
                init_size();
                valid_ = true;
            }
        }

        explicit PtrKey(uint64_t src) {
            if (src == LONG_MAX) {
                valid_ = false;
            } else if (src == 0) {
                valid_ = true;
                ptr_ = 0;
                size_ = 0;
            } else {
                ptr_ = src;
                init_size();
                valid_ = true;
            }
        }

        uint64_t size() const {
            return size_;
        }

        char* ptr() const {
            return (char*)ptr_ + sizeof(uint64_t);
        }

        bool valid() const {
            return valid_;
        };

        PtrKey& operator=(const PtrKey& key) =default;

        bool operator==(const PtrKey& key) const {
            if (!valid_ || !key.valid()) {
                return !valid_ && !key.valid();
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) == 0;
            }
        }

        bool operator!=(const PtrKey& key) const {
            if (!valid_ || !key.valid()) {
                return (!valid_ && key.valid()) || (valid_ && !key.valid());
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) != 0;
            }
        }

        bool operator>(const PtrKey& key) const {
            if (!valid_ || !key.valid()) {
                return !valid_;
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) > 0;
            }
        }

        bool operator>=(const PtrKey& key) const {
            if (!valid_ || !key.valid()) {
                return (!valid_ && !key.valid()) || !valid_;
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) >= 0;
            }
        }

        bool operator<(const PtrKey& key) const {
            if (!valid_ || !key.valid()) {
                return valid_;
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) < 0;
            }
        }

        bool operator<=(const PtrKey& key) const {
            if (!valid_ || !key.valid()) {
                return (!valid_ && !key.valid()) || valid_;
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) <= 0;
            }
        }

    private:
        void init_size() {
            uint64_t *raw_size = (uint64_t*)ptr_;
            size_ = *raw_size;
        }
        uint64_t ptr_;
        uint64_t size_;
        bool valid_;
    };
}
#endif //CCEH_RAW_KEY_H


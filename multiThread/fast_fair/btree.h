#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#include <libpmemobj.h>
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <vector>
using namespace std;

/******************************************* The PCM emulate configuration ******************************************/
#define EXTRA_SCM_LATENCY  500
#define M_PCM_CPUFREQ      3900
#define NS2CYCLE(__ns)     ((__ns) * M_PCM_CPUFREQ / 1000)
#define CYCLE2NS(__cycles) ((__cycles) * 1000 / M_PCM_CPUFREQ)
#define FLUSH_ALIGN ((uintptr_t)64)

#define USE_PM
#ifndef USE_PM
#define USE_DRAM
#endif

namespace fastfair{

    static inline unsigned long long asm_rdtsc(void)
    {
        unsigned hi, lo;
        __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
        return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
    }
    static inline void emulate_latency_ns(uint64_t ns)
    {
        /*
          uint64_t cycles, start, stop;
          start = asm_rdtsc();
          cycles = NS2CYCLE(ns);

          do {
              stop = asm_rdtsc();
          } while (stop - start < cycles);
        */
        return ;
    }


#define CPU_FREQ_MHZ (3900)
#define CACHE_LINE_SIZE 64 
#define IS_FORWARD(c) (c % 2 == 0)
#define PAGESIZE 1056                         
#define entry_key_t EntryKey

    struct EntryKey {
        uint64_t ptr_;

        EntryKey() =default;

        EntryKey(uint64_t ptr) {
            ptr_ = ptr;
        }

        EntryKey(long ptr) {
            ptr_ = ptr;
        }

        EntryKey(int ptr) {
            ptr_ = ptr;
        }

        EntryKey(char* ptr) {
            ptr_ = (uint64_t)ptr;
        }

        static int compare(char* a, char* b, uint64_t size_a, uint64_t size_b) {
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

        uint64_t size() const {
            uint64_t *raw_size = (uint64_t*)ptr_;
            return *raw_size;
        }

        char* ptr() const {
            return (char*)ptr_ + sizeof(uint64_t);
        }

        char* raw() const {
            return (char*)ptr_;
        }

        bool valid() const{
            return ptr_ != LONG_MAX;
        }

        bool operator==(const EntryKey& key) const {
            if (!valid() || !key.valid()) {
                return !valid() && !key.valid();
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) == 0;
            }
        }

        bool operator!=(const EntryKey& key) const {
            if (!valid() || !key.valid()) {
                return (!valid() && key.valid()) || (valid() && !key.valid());
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) != 0;
            }
        }

        bool operator>(const EntryKey& key) const {
            if (!valid() || !key.valid()) {
                return !valid();
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) > 0;
            }
        }

        bool operator>=(const EntryKey& key) const {
            if (!valid() || !key.valid()) {
                return (!valid() && !key.valid()) || !valid();
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) >= 0;
            }
        }

        bool operator<(const EntryKey& key) const {
            if (!valid() || !key.valid()) {
                return valid();
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) < 0;
            }
        }

        bool operator<=(const EntryKey& key) const {
            if (!valid() || !key.valid()) {
                return (!valid() && !key.valid()) || valid();
            } else {
                uint64_t size_a = size(), size_b = key.size();
                char* a = ptr();
                char* b = key.ptr();
                return compare(a, b, size_a, size_b) <= 0;
            }
        }

    };

    const uint64_t SPACE_PER_THREAD = 35ULL * 1024ULL * 1024ULL * 1024ULL;
    const uint64_t SPACE_OF_MAIN_THREAD = 35ULL * 1024ULL * 1024ULL * 1024ULL;
    extern __thread char *start_addr;
    extern __thread char *curr_addr;

    static inline void cpu_pause()
    {
        __asm__ volatile ("pause" ::: "memory");
    }

    static inline unsigned long read_tsc(void)
    {
        unsigned long var;
        unsigned int hi, lo;

        asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
        var = ((unsigned long long int) hi << 32) | lo;

        return var;
    }

    inline void mfence()
    {
        asm volatile("mfence":::"memory");
    }

    // HQD ADD:
    static inline void asm_clflush(volatile uint64_t *addr)
    {
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr)));
    }

    inline void clflush(char *addr, int len)
    {
#ifdef USE_PM
        mfence();
        for (uintptr_t uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1);
        uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN) {
            asm_clflush((uint64_t *)uptr);
            emulate_latency_ns(EXTRA_SCM_LATENCY);
        }
        mfence();
#endif
    }

    class page;
#ifdef USE_PMDK
    POBJ_LAYOUT_BEGIN(btree);
    POBJ_LAYOUT_TOID(btree, page);
    POBJ_LAYOUT_END(btree);
    extern __thread PMEMobjpool *pop;
#endif

    class btree{
    private:
        int height;
        char* root;

    public:

        btree();
        ~btree();
        void setNewRoot(char *);
        void getNumberOfNodes();
        void btree_insert(entry_key_t, char*);
        void btree_insert_internal(char *, entry_key_t, char *, uint32_t);
        void btree_delete(entry_key_t);
        void btree_delete_internal
        (entry_key_t, char *, uint32_t, entry_key_t *, bool *, page **);
        char *btree_search(entry_key_t);
        void btree_search_range(entry_key_t, entry_key_t, unsigned long *);
        void scan(EntryKey key, int range, unsigned long* buf);
        void scan(EntryKey key, const char* prefix, int len, unsigned long** buf, int* res_num);
        void printAll();

        friend class page;
    };

    class header{
    private:
        page* leftmost_ptr;         // 8 bytes
        page* sibling_ptr;          // 8 bytes
        uint32_t level;             // 4 bytes
        uint8_t switch_counter;     // 1 bytes
        uint8_t is_deleted;         // 1 bytes
        int16_t last_index;         // 2 bytes
        std::mutex *mtx;            // 8 bytes

        friend class page;
        friend class btree;

    public:
        header() {
            mtx = new std::mutex();

            leftmost_ptr = NULL;
            sibling_ptr = NULL;
            switch_counter = 0;
            last_index = -1;
            is_deleted = false;
        }

        ~header() {
            delete mtx;
        }
    };

    class entry{
    private:
        entry_key_t key; // 8 bytes
        char* ptr; // 8 bytes

    public :
        entry(){
        key = LONG_MAX;
        ptr = NULL;
    }

    friend class page;
    friend class btree;
    };

    const int cardinality = (PAGESIZE-sizeof(header))/sizeof(entry);
    const int count_in_line = CACHE_LINE_SIZE / sizeof(entry);

    class page{
    private:
        header hdr;  // header in persistent memory, 16 bytes
        entry records[cardinality]; // slots in persistent memory, 16 bytes * n

    public:
        friend class btree;

        page(uint32_t level = 0) {
            hdr.level = level;
            records[0].ptr = NULL;
        }

        // this is called when tree grows
        page(page* left, entry_key_t key, page* right, uint32_t level = 0) {
            hdr.leftmost_ptr = left;
            hdr.level = level;
            records[0].key = key;
            records[0].ptr = (char*) right;
            records[1].ptr = NULL;

            hdr.last_index = 0;

            clflush((char*)this, sizeof(page));

        }

        void *operator new(size_t size) {
#ifdef USE_PM
#ifdef USE_PMDK
            TOID(page) p;
            POBJ_ZALLOC(pop, &p, page, size);
            return (void *)pmemobj_direct(p.oid);
#else
            void *ret;
            ret = curr_addr;
            curr_addr += size;
            if (curr_addr >= start_addr + SPACE_PER_THREAD) {
                printf("start_addr is %p, curr_addr is %p, SPACE_PER_THREAD is %lu, no free space to alloc\n", start_addr, curr_addr, SPACE_PER_THREAD); exit(0);
            }
            return ret;
#endif
#endif
#ifdef USE_DRAM
            void *ret;
            posix_memalign(&ret, 64, size);
            return ret;
#endif
        }

        inline int count() {
            uint8_t previous_switch_counter;
            int count = 0;
            do {
                previous_switch_counter = hdr.switch_counter;
                count = hdr.last_index + 1;

                while(count >= 0 && records[count].ptr != NULL) {
                    if(IS_FORWARD(previous_switch_counter))
                        ++count;
                    else
                        --count;
                }

                if(count < 0) {
                    count = 0;
                    while(records[count].ptr != NULL) {
                        ++count;
                    }
                }

            } while(previous_switch_counter != hdr.switch_counter);

            return count;
        }

        inline bool remove_key(entry_key_t key) {
            // Set the switch_counter
            if(IS_FORWARD(hdr.switch_counter))
                ++hdr.switch_counter;

            bool shift = false;
            int i;
            for(i = 0; records[i].ptr != NULL; ++i) {
                if(!shift && records[i].key == key) {
                    records[i].ptr = (i == 0) ?
                            (char *)hdr.leftmost_ptr : records[i - 1].ptr;
                    shift = true;
                }

                if(shift) {
                    records[i].key = records[i + 1].key;
                    records[i].ptr = records[i + 1].ptr;

                    // flush
                    uint64_t records_ptr = (uint64_t)(&records[i]);
                    int remainder = records_ptr % CACHE_LINE_SIZE;
                    bool do_flush = (remainder == 0) ||
                            ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1) &&
                            ((remainder + sizeof(entry)) % CACHE_LINE_SIZE) != 0);
                    if(do_flush) {

                        clflush((char *)records_ptr, CACHE_LINE_SIZE);

                    }
                }
            }

            if(shift) {
                --hdr.last_index;
            }
            return shift;
        }

        bool remove(btree* bt, entry_key_t key, bool only_rebalance = false, bool with_lock = true) {
            hdr.mtx->lock();

            bool ret = remove_key(key);

            hdr.mtx->unlock();

            return ret;
        }

        /*
         * Although we implemented the rebalancing of B+-Tree, it is currently blocked for the performance.
         * Please refer to the follow.
         * Chi, P., Lee, W. C., & Xie, Y. (2014, August).
         * Making B+-tree efficient in PCM-based main memory. In Proceedings of the 2014
         * international symposium on Low power electronics and design (pp. 69-74). ACM.
         */
        bool remove_rebalancing(btree* bt, entry_key_t key, bool only_rebalance = false, bool with_lock = true) {
            if(with_lock) {
                hdr.mtx->lock();
            }
            if(hdr.is_deleted) {
                if(with_lock) {
                    hdr.mtx->unlock();
                }
                return false;
            }

            if(!only_rebalance) {
                register int num_entries_before = count();

                // This node is root
                if(this == (page *)bt->root) {
                    if(hdr.level > 0) {
                        if(num_entries_before == 1 && !hdr.sibling_ptr) {
                            bt->root = (char *)hdr.leftmost_ptr;

                            clflush((char *)&(bt->root), sizeof(char *));

              
                            hdr.is_deleted = 1;
                        }
                    }

                    // Remove the key from this node
                    bool ret = remove_key(key);

                    if(with_lock) {
                        hdr.mtx->unlock();
                    }
                    return true;
                }

                bool should_rebalance = true;
                // check the node utilization
                if(num_entries_before - 1 >= (int)((cardinality - 1) * 0.5)) {
                    should_rebalance = false;
                }

                // Remove the key from this node
                bool ret = remove_key(key);

                if(!should_rebalance) {
                    if(with_lock) {
                        hdr.mtx->unlock();
                    }
                    return (hdr.leftmost_ptr == NULL) ? ret : true;
                }
            }

            //Remove a key from the parent node
            entry_key_t deleted_key_from_parent = 0;
            bool is_leftmost_node = false;
            page *left_sibling;
            bt->btree_delete_internal(key, (char *)this, hdr.level + 1,
                                      &deleted_key_from_parent, &is_leftmost_node, &left_sibling);

            if(is_leftmost_node) {
                if(with_lock) {
                    hdr.mtx->unlock();
                }

                if(!with_lock) {
                    hdr.sibling_ptr->hdr.mtx->lock();
                }
                hdr.sibling_ptr->remove(bt, hdr.sibling_ptr->records[0].key, true, with_lock);
                if(!with_lock) {
                    hdr.sibling_ptr->hdr.mtx->unlock();
                }
                return true;
            }

            if(with_lock) {
                left_sibling->hdr.mtx->lock();
            }

            while(left_sibling->hdr.sibling_ptr != this) {
                if(with_lock) {
                    page *t = left_sibling->hdr.sibling_ptr;
                    left_sibling->hdr.mtx->unlock();
                    left_sibling = t;
                    left_sibling->hdr.mtx->lock();
                }
                else
                    left_sibling = left_sibling->hdr.sibling_ptr;
            }

            register int num_entries = count();
            register int left_num_entries = left_sibling->count();

            // Merge or Redistribution
            int total_num_entries = num_entries + left_num_entries;
            if(hdr.leftmost_ptr)
                ++total_num_entries;

            entry_key_t parent_key;

            if(total_num_entries > cardinality - 1) { // Redistribution
                register int m = (int) ceil(total_num_entries / 2);

                if(num_entries < left_num_entries) { // left -> right
                    if(hdr.leftmost_ptr == nullptr){
                        for(int i=left_num_entries - 1; i>=m; i--){
                            insert_key
                            (left_sibling->records[i].key, left_sibling->records[i].ptr, &num_entries);
                        }

                        left_sibling->records[m].ptr = nullptr;

                        clflush((char *)&(left_sibling->records[m].ptr), sizeof(char *));

                        left_sibling->hdr.last_index = m - 1;

                        clflush((char *)&(left_sibling->hdr.last_index), sizeof(int16_t));

                        parent_key = records[0].key;
                    }
                    else{
                        insert_key(deleted_key_from_parent, (char*)hdr.leftmost_ptr,
                                   &num_entries);

                        for(int i=left_num_entries - 1; i>m; i--){
                            insert_key
                            (left_sibling->records[i].key, left_sibling->records[i].ptr, &num_entries);
                        }

                        parent_key = left_sibling->records[m].key;

                        hdr.leftmost_ptr = (page*)left_sibling->records[m].ptr;

                        clflush((char *)&(hdr.leftmost_ptr), sizeof(page *));

                        left_sibling->records[m].ptr = nullptr;

                        clflush((char *)&(left_sibling->records[m].ptr), sizeof(char *));

                        left_sibling->hdr.last_index = m - 1;

                        clflush((char *)&(left_sibling->hdr.last_index), sizeof(int16_t));

                    }

                    if(left_sibling == ((page *)bt->root)) {
                        page* new_root = new page(left_sibling, parent_key, this, hdr.level + 1);
                        bt->setNewRoot((char *)new_root);
                    }
                    else {
                        bt->btree_insert_internal
                        ((char *)left_sibling, parent_key, (char *)this, hdr.level + 1);
                    }
                }
                else{ // from leftmost case
                    hdr.is_deleted = 1;

                    clflush((char *)&(hdr.is_deleted), sizeof(uint8_t));

                    page* new_sibling = new page(hdr.level);
                    new_sibling->hdr.mtx->lock();
                    new_sibling->hdr.sibling_ptr = hdr.sibling_ptr;

                    int num_dist_entries = num_entries - m;
                    int new_sibling_cnt = 0;

                    if(hdr.leftmost_ptr == nullptr){
                        for(int i=0; i<num_dist_entries; i++){
                            left_sibling->insert_key(records[i].key, records[i].ptr,
                                                     &left_num_entries);
                        }

                        for(int i=num_dist_entries; records[i].ptr != NULL; i++){
                            new_sibling->insert_key(records[i].key, records[i].ptr,
                                                    &new_sibling_cnt, false);
                        }

                        clflush((char *)(new_sibling), sizeof(page));

                        left_sibling->hdr.sibling_ptr = new_sibling;

                        clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(page *));

                        parent_key = new_sibling->records[0].key;
                    }
                    else{
                        left_sibling->insert_key(deleted_key_from_parent,
                                                 (char*)hdr.leftmost_ptr, &left_num_entries);

                        for(int i=0; i<num_dist_entries - 1; i++){
                            left_sibling->insert_key(records[i].key, records[i].ptr,
                                                     &left_num_entries);
                        }

                        parent_key = records[num_dist_entries - 1].key;

                        new_sibling->hdr.leftmost_ptr = (page*)records[num_dist_entries - 1].ptr;
                        for(int i=num_dist_entries; records[i].ptr != NULL; i++){
                            new_sibling->insert_key(records[i].key, records[i].ptr,
                                                    &new_sibling_cnt, false);
                        }

                        clflush((char *)(new_sibling), sizeof(page));

                        left_sibling->hdr.sibling_ptr = new_sibling;
                        clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(page *));
                    }

                    if(left_sibling == ((page *)bt->root)) {
                        page* new_root = new page(left_sibling, parent_key, new_sibling, hdr.level + 1);
                        bt->setNewRoot((char *)new_root);
                    }
                    else {
                        bt->btree_insert_internal
                        ((char *)left_sibling, parent_key, (char *)new_sibling, hdr.level + 1);
                    }

                    new_sibling->hdr.mtx->unlock();
                }
            }
            else {
                hdr.is_deleted = 1;
                clflush((char *)&(hdr.is_deleted), sizeof(uint8_t));

                if(hdr.leftmost_ptr)
                    left_sibling->insert_key(deleted_key_from_parent,
                                             (char *)hdr.leftmost_ptr, &left_num_entries);

                for(int i = 0; records[i].ptr != NULL; ++i) {
                    left_sibling->insert_key(records[i].key, records[i].ptr, &left_num_entries);
                }

                left_sibling->hdr.sibling_ptr = hdr.sibling_ptr;
                clflush((char *)&(left_sibling->hdr.sibling_ptr), sizeof(page *));
            }

            if(with_lock) {
                left_sibling->hdr.mtx->unlock();
                hdr.mtx->unlock();
            }

            return true;
        }

        inline void
        insert_key(entry_key_t key, char* ptr, int *num_entries, bool flush = true,
                   bool update_last_index = true) {
            // update switch_counter
            if(!IS_FORWARD(hdr.switch_counter))
                ++hdr.switch_counter;

            // FAST
            if(*num_entries == 0) {  // this page is empty
                entry* new_entry = (entry*) &records[0];
                entry* array_end = (entry*) &records[1];
                new_entry->key = (entry_key_t) key;
                new_entry->ptr = (char*) ptr;

                array_end->ptr = (char*)NULL;

                if(flush) {
                    clflush((char*) this, CACHE_LINE_SIZE);
                }
            }
            else {
                int i = *num_entries - 1, inserted = 0, to_flush_cnt = 0;
                records[*num_entries+1].ptr = records[*num_entries].ptr;
                if(flush) {
                    if((uint64_t)&(records[*num_entries+1].ptr) % CACHE_LINE_SIZE == 0)
                        clflush((char*)&(records[*num_entries+1].ptr), sizeof(char*));
                }

                // FAST
                for(i = *num_entries - 1; i >= 0; i--) {
                    if(key < records[i].key ) {
                        records[i+1].ptr = records[i].ptr;
                        records[i+1].key = records[i].key;

                        if(flush) {
                            uint64_t records_ptr = (uint64_t)(&records[i+1]);

                            int remainder = records_ptr % CACHE_LINE_SIZE;
                            bool do_flush = (remainder == 0) ||
                                    ((((int)(remainder + sizeof(entry)) / CACHE_LINE_SIZE) == 1)
                                    && ((remainder+sizeof(entry))%CACHE_LINE_SIZE)!=0);
                            if(do_flush) {
                                clflush((char*)records_ptr,CACHE_LINE_SIZE);
                                to_flush_cnt = 0;
                            }
                            else
                                ++to_flush_cnt;
                        }
                    }
                    else{
                        records[i+1].ptr = records[i].ptr;
                        records[i+1].key = key;
                        records[i+1].ptr = ptr;

                        if(flush)
                            clflush((char*)&records[i+1],sizeof(entry));
                        inserted = 1;
                        break;
                    }
                }
                if(inserted==0){
                    records[0].ptr =(char*) hdr.leftmost_ptr;
                    records[0].key = key;
                    records[0].ptr = ptr;
                    if(flush)
                        clflush((char*) &records[0], sizeof(entry));
                }
            }

            if(update_last_index) {
                hdr.last_index = *num_entries;
            }
            ++(*num_entries);
        }

        // Insert a new key - FAST and FAIR
        page *store
        (btree* bt, char* left, entry_key_t key, char* right,
         bool flush, bool with_lock, page *invalid_sibling = NULL) {
            if(with_lock) {
                hdr.mtx->lock(); // Lock the write lock
            }
            if(hdr.is_deleted) {
                if(with_lock) {
                    hdr.mtx->unlock();
                }

                return NULL;
            }

            // If this node has a sibling node,
            if(hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
                // Compare this key with the first key of the sibling
                if(key > hdr.sibling_ptr->records[0].key) {
                    if(with_lock) {
                        hdr.mtx->unlock(); // Unlock the write lock
                    }
                    return hdr.sibling_ptr->store(bt, NULL, key, right,
                                                  true, with_lock, invalid_sibling);
                }
            }

            register int num_entries = count();

            //simulate the 64B-value-persist latency.
            emulate_latency_ns(EXTRA_SCM_LATENCY);

            // FAST
            if(num_entries < cardinality - 1) {
                insert_key(key, right, &num_entries, flush);

                if(with_lock) {
                    hdr.mtx->unlock(); // Unlock the write lock
                }

                return this;
            }
            else {// FAIR
                // overflow
                // create a new node
                page* sibling = new page(hdr.level);
                register int m = (int) ceil(num_entries/2);
                entry_key_t split_key = records[m].key;

                // migrate half of keys into the sibling
                int sibling_cnt = 0;
                if(hdr.leftmost_ptr == NULL){ // leaf node
                    for(int i=m; i<num_entries; ++i){
                        sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
                    }
                }
                else{ // internal node
                    for(int i=m+1;i<num_entries;++i){
                        sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt, false);
                    }
                    sibling->hdr.leftmost_ptr = (page*) records[m].ptr;
                }

                sibling->hdr.sibling_ptr = hdr.sibling_ptr;
                clflush((char *)sibling, sizeof(page));

                hdr.sibling_ptr = sibling;
                clflush((char*) &hdr, sizeof(hdr));

                // set to NULL
                if(IS_FORWARD(hdr.switch_counter))
                    hdr.switch_counter += 2;
                else
                    ++hdr.switch_counter;
                records[m].ptr = NULL;
                clflush((char*) &records[m], sizeof(entry));

                hdr.last_index = m - 1;
                clflush((char *)&(hdr.last_index), sizeof(int16_t));

                num_entries = hdr.last_index + 1;

                page *ret;

                // insert the key
                if(key < split_key) {
                    insert_key(key, right, &num_entries);
                    ret = this;
                }
                else {
                    sibling->insert_key(key, right, &sibling_cnt);
                    ret = sibling;
                }

                // Set a new root or insert the split key to the parent
                if(bt->root == (char *)this) { // only one node can update the root ptr
                    page* new_root = new page((page*)this, split_key, sibling,
                                              hdr.level + 1);
                    bt->setNewRoot((char *)new_root);

                    if(with_lock) {
                        hdr.mtx->unlock(); // Unlock the write lock
                    }
                }
                else {
                    if(with_lock) {
                        hdr.mtx->unlock(); // Unlock the write lock
                    }
                    bt->btree_insert_internal(NULL, split_key, (char *)sibling,
                                              hdr.level + 1);
                }

                return ret;
            }

        }

        // Search keys with linear search
        void linear_search_range
        (entry_key_t min, entry_key_t max, unsigned long *buf) {
            int i, off = 0;
            uint8_t previous_switch_counter;
            page *current = this;

            while(current) {
                int old_off = off;
                do {
                    previous_switch_counter = current->hdr.switch_counter;
                    off = old_off;

                    entry_key_t tmp_key;
                    char *tmp_ptr;

                    if(IS_FORWARD(previous_switch_counter)) {
                        if((tmp_key = current->records[0].key) > min) {
                            if(tmp_key < max) {
                                if((tmp_ptr = current->records[0].ptr) != NULL) {
                                    if(tmp_key == current->records[0].key) {
                                        if(tmp_ptr) {
                                            buf[off++] = (unsigned long)tmp_ptr;
                                        }
                                    }
                                }
                            }
                            else
                                return;
                        }

                        for(i=1; current->records[i].ptr != NULL; ++i) {
                            if((tmp_key = current->records[i].key) > min) {
                                if(tmp_key < max) {
                                    if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                        if(tmp_key == current->records[i].key) {
                                            if(tmp_ptr)
                                                buf[off++] = (unsigned long)tmp_ptr;
                                        }
                                    }
                                }
                                else
                                    return;
                            }
                        }
                    }
                    else {
                        for(i=count() - 1; i > 0; --i) {
                            if((tmp_key = current->records[i].key) > min) {
                                if(tmp_key < max) {
                                    if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                        if(tmp_key == current->records[i].key) {
                                            if(tmp_ptr)
                                                buf[off++] = (unsigned long)tmp_ptr;
                                        }
                                    }
                                }
                                else
                                    return;
                            }
                        }

                        if((tmp_key = current->records[0].key) > min) {
                            if(tmp_key < max) {
                                if((tmp_ptr = current->records[0].ptr) != NULL) {
                                    if(tmp_key == current->records[0].key) {
                                        if(tmp_ptr) {
                                            buf[off++] = (unsigned long)tmp_ptr;
                                        }
                                    }
                                }
                            }
                            else
                                return;
                        }
                    }
                } while(previous_switch_counter != current->hdr.switch_counter);

                current = current->hdr.sibling_ptr;
            }
        }

        // Search keys with linear search
        void linear_search_range
        (entry_key_t min, int range, unsigned long *buf) {
            int i, off = 0;
            uint8_t previous_switch_counter;
            page *current = this;

            while(current) {
                int old_off = off;
                do {
                    previous_switch_counter = current->hdr.switch_counter;
                    off = old_off;

                    entry_key_t tmp_key;
                    char *tmp_ptr;

                    if(IS_FORWARD(previous_switch_counter)) {
                        tmp_key = current->records[0].key;
                        if((tmp_ptr = current->records[0].ptr) != NULL) {
                            if(tmp_key == current->records[0].key) {
                                if(tmp_ptr) {
                                    buf[off++] = (unsigned long)tmp_key.raw();
                                    if (off > range) return;
                                }
                            }
                        }

                        for(i=1; current->records[i].ptr != NULL; ++i) {
                            tmp_key = current->records[i].key;
                            if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                if(tmp_key == current->records[i].key) {
                                    if(tmp_ptr){
                                        buf[off++] = (unsigned long)tmp_key.raw();
                                        if (off > range) return;
                                    }
                                }
                            }
                        }
                    }
                    else {
                        for(i=count() - 1; i > 0; --i) {
                            tmp_key = current->records[i].key;
                            if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                if(tmp_key == current->records[i].key) {
                                    if(tmp_ptr) {
                                        buf[off++] = (unsigned long)tmp_key.raw();
                                        if (off > range) return ;
                                    }

                                }
                            }
                        }
                        tmp_key = current->records[0].key;
                            if(tmp_key == current->records[0].key) {
                                if(tmp_ptr) {
                                    buf[off++] = (unsigned long)tmp_key.raw();
                                    if (off > range) return;
                                }
                            }

                    }
                } while(previous_switch_counter != current->hdr.switch_counter);

                current = current->hdr.sibling_ptr;
            }
        }

        // Search keys with linear search by prefix
        void linear_search_range_prefix
        (entry_key_t min, const char* prefix, int len, unsigned long **buf, int *res_num) {
            int i, off = 0;
            uint8_t previous_switch_counter;
            page *current = this;
            std::vector<unsigned long> found;

            while(current) {
                int old_off = off;
                do {
                    previous_switch_counter = current->hdr.switch_counter;
                    off = old_off;

                    entry_key_t tmp_key;
                    char *tmp_ptr;

                    if(IS_FORWARD(previous_switch_counter)) {
                        tmp_key = current->records[0].key;
                        if((tmp_ptr = current->records[0].ptr) != NULL) {
                            if(tmp_key == current->records[0].key) {
                                if(strncmp(tmp_key.ptr(), prefix, len) == 0 && tmp_ptr) {
                                    found.push_back((unsigned long)tmp_key.raw());
                                }
                            }
                        }

                        for(i=1; current->records[i].ptr != NULL; ++i) {
                            tmp_key = current->records[i].key;
                            if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                if(tmp_key == current->records[i].key) {
                                    if(strncmp(tmp_key.ptr(), prefix, len) == 0 && tmp_ptr){
                                        //buf[off++] = (unsigned long)tmp_ptr;
                                        found.push_back((unsigned long)tmp_key.raw());
                                    }
                                }
                            }
                        }
                    }
                    else {
                        for(i=count() - 1; i > 0; --i) {
                            tmp_key = current->records[i].key;
                            if((tmp_ptr = current->records[i].ptr) != current->records[i - 1].ptr) {
                                if(tmp_key == current->records[i].key) {
                                    if(strncmp(tmp_key.ptr(), prefix, len) == 0 && tmp_ptr) {
                                        //buf[off++] = (unsigned long)tmp_ptr;
                                        found.push_back((unsigned long)tmp_key.raw());
                                    }

                                }
                            }
                        }
                        tmp_key = current->records[0].key;
                        if(tmp_key == current->records[0].key) {
                            if(strncmp(tmp_key.ptr(), prefix, len) == 0 && tmp_ptr) {
                                //buf[off++] = (unsigned long)tmp_ptr;
                                found.push_back((unsigned long)tmp_key.raw());
                            }
                        }

                    }
                } while(previous_switch_counter != current->hdr.switch_counter);

                current = current->hdr.sibling_ptr;
            }
            *buf = new unsigned long[found.size()];
            int pos = 0;
            for (auto item : found) {
                (*buf)[pos++] = item;
            }
            *res_num = found.size();
        }

        char *linear_search(entry_key_t key) {
            int i = 1;
            uint8_t previous_switch_counter;
            char *ret = NULL;
            char *t;
            entry_key_t k;

            if(hdr.leftmost_ptr == NULL) { // Search a leaf node
                do {
                    previous_switch_counter = hdr.switch_counter;
                    ret = NULL;

                    // search from left ro right
                    if(IS_FORWARD(previous_switch_counter)) {
                        if((k = records[0].key) == key) {
                            if((t = records[0].ptr) != NULL) {
                                if(k == records[0].key) {
                                    ret = t;
                                    continue;
                                }
                            }
                        }

                        for(i=1; records[i].ptr != NULL; ++i) {
                            if((k = records[i].key) == key) {
                                if(records[i-1].ptr != (t = records[i].ptr)) {
                                    if(k == records[i].key) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    else { // search from right to left
                        for(i = count() - 1; i > 0; --i) {
                            if((k = records[i].key) == key) {
                                if(records[i - 1].ptr != (t = records[i].ptr) && t) {
                                    if(k == records[i].key) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }

                        if(!ret) {
                            if((k = records[0].key) == key) {
                                if(NULL != (t = records[0].ptr) && t) {
                                    if(k == records[0].key) {
                                        ret = t;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                } while(hdr.switch_counter != previous_switch_counter);

                if(ret) {
                    return ret;
                }

                if((t = (char *)hdr.sibling_ptr) && key >= ((page *)t)->records[0].key)
                    return t;

                return NULL;
            }
            else { // internal node
                do {
                    previous_switch_counter = hdr.switch_counter;
                    ret = NULL;

                    if(IS_FORWARD(previous_switch_counter)) {
                        if(key < (k = records[0].key)) {
                            if((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
                                ret = t;
                                continue;
                            }
                        }

                        for(i = 1; records[i].ptr != NULL; ++i) {
                            if(key < (k = records[i].key)) {
                                if((t = records[i-1].ptr) != records[i].ptr) {
                                    ret = t;
                                    break;
                                }
                            }
                        }

                        if(!ret) {
                            ret = records[i - 1].ptr;
                            continue;
                        }
                    }
                    else { // search from right to left
                        for(i = count() - 1; i >= 0; --i) {
                            if(key >= (k = records[i].key)) {
                                if(i == 0) {
                                    if((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                                        ret = t;
                                        break;
                                    }
                                }
                                else {
                                    if(records[i - 1].ptr != (t = records[i].ptr)) {
                                        ret = t;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } while(hdr.switch_counter != previous_switch_counter);

                if((t = (char *)hdr.sibling_ptr) != NULL) {
                    if(key >= ((page *)t)->records[0].key)
                        return t;
                }

                if(ret) {
                    return ret;
                }
                else
                    return (char *)hdr.leftmost_ptr;
            }

            return NULL;
        }

        // print a node
        void print() {
            if(hdr.leftmost_ptr == NULL)
                printf("[%d] leaf %x \n", this->hdr.level, this);
            else
                printf("[%d] internal %x \n", this->hdr.level, this);
            printf("last_index: %d\n", hdr.last_index);
            printf("switch_counter: %d\n", hdr.switch_counter);
            printf("search direction: ");
            if(IS_FORWARD(hdr.switch_counter))
                printf("->\n");
            else
                printf("<-\n");

            if(hdr.leftmost_ptr!=NULL)
                printf("%x ",hdr.leftmost_ptr);

            for(int i=0;records[i].ptr != NULL;++i)
                printf("%ld,%x ",records[i].key,records[i].ptr);

            printf("%x ", hdr.sibling_ptr);

            printf("\n");
        }

        void printAll() {
            if(hdr.leftmost_ptr==NULL) {
                printf("printing leaf node: ");
                print();
            }
            else {
                printf("printing internal node: ");
                print();
                ((page*) hdr.leftmost_ptr)->printAll();
                for(int i=0;records[i].ptr != NULL;++i){
                    ((page*) records[i].ptr)->printAll();
                }
            }
        }
    };

    inline int file_exists(const char *filename) {
        struct stat buffer;
        return stat(filename, &buffer);
    }

    void openPmemobjPool(char* pathname);
}

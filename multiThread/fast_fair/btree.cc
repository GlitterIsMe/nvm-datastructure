//
// Created by zzyyyww on 2021/9/6.
//
#include "btree.h"

namespace fastfair{
    unsigned long long search_time_in_insert=0;
    unsigned int gettime_cnt= 0;
    unsigned long long clflush_time_in_insert=0;
    unsigned long long update_time_in_insert=0;
    int clflush_cnt = 0;
    int node_cnt=0;
    pthread_mutex_t print_mtx;
    __thread PMEMobjpool *pop;

    inline void openPmemobjPool(char* pathname) {
#ifdef USE_PMDK
        int sds_write_value = 0;
        pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
        if (file_exists(pathname) != 0) {
            printf("create new one.\n");
            if ((pop = pmemobj_create(pathname, POBJ_LAYOUT_NAME(btree),
                                      (uint64_t)35ULL * 1024ULL * 1024ULL * 1024ULL, 0666)) == NULL) {
                perror("failed to create pool.\n");
                return;
            }
        } else {
            printf("open existing one.\n");
            if ((pop = pmemobj_open(pathname, POBJ_LAYOUT_NAME(btree))) == NULL) {
                perror("failed to open pool.\n");
                return;
            }
        }
#endif
    }

    /*
     * class btree
     */
    btree::btree(){
#ifdef USE_PMDK
        openPmemobjPool("/mnt/pmem/fstfair.pool");
#else
        printf("use DRAM!\n");
#endif
        root = (char*)new page();
        height = 1;
    }

    btree::~btree() {
#ifdef USE_PMDK
        pmemobj_close(pop);
#endif
    }

    void btree::setNewRoot(char *new_root) {
        this->root = (char*)new_root;
        clflush((char*)&(this->root),sizeof(char*));
        ++height;
    }

    char *btree::btree_search(entry_key_t key){
        page* p = (page*)root;

        while(p->hdr.leftmost_ptr != NULL) {
            p = (page *)p->linear_search(key);
        }

        page *t;
        while((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
            p = t;
            if(!p) {
                break;
            }
        }

        //if(!t || (char *)t != (char *)key) {
        if(!t) {
            return NULL;
        }

        return (char *)t;
    }

    // insert the key in the leaf node
    void btree::btree_insert(entry_key_t key, char* right){ //need to be string
        page* p = (page*)root;

        while(p->hdr.leftmost_ptr != NULL) {
            p = (page*)p->linear_search(key);
        }

        if(!p->store(this, NULL, key, right, true, true)) { // store
            btree_insert(key, right);
        }
    }

    // store the key into the node at the given level
    void btree::btree_insert_internal
    (char *left, entry_key_t key, char *right, uint32_t level) {
        if(level > ((page *)root)->hdr.level)
            return;

        page *p = (page *)this->root;

        while(p->hdr.level > level)
            p = (page *)p->linear_search(key);

        if(!p->store(this, NULL, key, right, true, true)) {
            btree_insert_internal(left, key, right, level);
        }
    }

    void btree::btree_delete(entry_key_t key) {
        page* p = (page*)root;

        while(p->hdr.leftmost_ptr != NULL){
            p = (page*) p->linear_search(key);
        }

        page *t;
        while((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
            p = t;
            if(!p)
                break;
        }

        if(p) {
            if(!p->remove(this, key)) {
                btree_delete(key);
            }
        }
        else {
            printf("not found the key to delete %lu\n", key);
        }
    }

    void btree::btree_delete_internal
    (entry_key_t key, char *ptr, uint32_t level, entry_key_t *deleted_key,
     bool *is_leftmost_node, page **left_sibling) {
        if(level > ((page *)this->root)->hdr.level)
            return;

        page *p = (page *)this->root;

        while(p->hdr.level > level) {
            p = (page *)p->linear_search(key);
        }

        p->hdr.mtx->lock();

        if((char *)p->hdr.leftmost_ptr == ptr) {
            *is_leftmost_node = true;
            p->hdr.mtx->unlock();
            return;
        }

        *is_leftmost_node = false;

        for(int i=0; p->records[i].ptr != NULL; ++i) {
            if(p->records[i].ptr == ptr) {
                if(i == 0) {
                    if((char *)p->hdr.leftmost_ptr != p->records[i].ptr) {
                        *deleted_key = p->records[i].key;
                        *left_sibling = p->hdr.leftmost_ptr;
                        p->remove(this, *deleted_key, false, false);
                        break;
                    }
                }
                else {
                    if(p->records[i - 1].ptr != p->records[i].ptr) {
                        *deleted_key = p->records[i].key;
                        *left_sibling = (page *)p->records[i - 1].ptr;
                        p->remove(this, *deleted_key, false, false);
                        break;
                    }
                }
            }
        }

        p->hdr.mtx->unlock();
    }

    // Function to search keys from "min" to "max"
    void btree::btree_search_range
    (entry_key_t min, entry_key_t max, unsigned long *buf) {
        page *p = (page *)root;

        while(p) {
            if(p->hdr.leftmost_ptr != NULL) {
                // The current page is internal
                p = (page *)p->linear_search(min);
            }
            else {
                // Found a leaf
                p->linear_search_range(min, max, buf);

                break;
            }
        }
    }

    void btree::scan(EntryKey key, int range, unsigned long* buf) {
        page *p = (page *)root;

        while(p) {
            if(p->hdr.leftmost_ptr != NULL) {
                // The current page is internal
                p = (page *)p->linear_search(key);
            }
            else {
                // Found a leaf
                p->linear_search_range(key, range, buf);
                break;
            }
        }
    }

    void btree::scan(EntryKey key, const char* prefix, int len, unsigned long** buf, int* res_num) {
        page *p = (page *)root;

        while(p) {
            if(p->hdr.leftmost_ptr != NULL) {
                // The current page is internal
                p = (page *)p->linear_search(key);
            }
            else {
                // Found a leaf
                p->linear_search_range_prefix(key, prefix, len, buf, res_num);
                break;
            }
        }
    }

    void btree::printAll(){
        pthread_mutex_lock(&print_mtx);
        int total_keys = 0;
        page *leftmost = (page *)root;
        printf("root: %x\n", root);
        do {
            page *sibling = leftmost;
            while(sibling) {
                if(sibling->hdr.level == 0) {
                    total_keys += sibling->hdr.last_index + 1;
                }
                sibling->print();
                sibling = sibling->hdr.sibling_ptr;
            }
            printf("-----------------------------------------\n");
            leftmost = leftmost->hdr.leftmost_ptr;
        } while(leftmost);

        printf("total number of keys: %d\n", total_keys);
        pthread_mutex_unlock(&print_mtx);
    }
}


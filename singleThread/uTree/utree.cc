//
// Created by zyw on 2021/8/17.
//
#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#ifdef USE_PMDK
#include <libpmemobj.h>
#endif
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
//#include <gperftools/profiler.h>

#include "utree.h"

using namespace std;

pthread_mutex_t print_mtx;

char *start_addr;
char *curr_addr;

void list_node_t::printAll(void) {
    printf("addr=%p, key=%d, ptr=%u, isUpdate=%d, isDelete=%d, next=%p\n",
           this, this->key, this->ptr, this->isUpdate, this->isDelete, this->next);
}

void *alloc(size_t size) {
#ifdef USE_PMDK
    TOID(list_node_t) p;
    POBJ_ZALLOC(pop, &p, list_node_t, size);
    return pmemobj_direct(p.oid);
#else
    void *ret = curr_addr;
    memset(ret, 0, sizeof(list_node_t));
    curr_addr += size;
    if (curr_addr >= start_addr + SPACE_PER_THREAD) {
        printf("start_addr is %p, curr_addr is %p, SPACE_PER_THREAD is %lu, no "
               "free space to alloc\n",
               start_addr, curr_addr, SPACE_PER_THREAD);
        exit(0);
    }
    return ret;
#endif
}

#ifdef USE_PMDK
int file_exists(const char *filename) {
    struct stat buffer;
    return stat(filename, &buffer);
}

void openPmemobjPool() {
    printf("use pmdk!\n");
    char pathname[100] = "/home/fkd/CPTree-202006/mount/pool";
    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
    if (file_exists(pathname) != 0) {
        printf("create new one.\n");
        if ((pop = pmemobj_create(pathname, POBJ_LAYOUT_NAME(btree),
                                  (uint64_t)600ULL * 1024ULL * 1024ULL * 1024ULL, 0666)) == NULL) {
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
}
#endif
/*
 * class btree
 */
btree::btree(){
#ifdef USE_PMDK
    openPmemobjPool();
#else
    printf("without pmdk!\n");
#endif
    root = (char*)new page();
    list_head = (list_node_t *)alloc(sizeof(list_node_t));
    printf("list_head=%p\n", list_head);
    list_head->next = NULL;
    height = 1;
}

btree::~btree() {
#ifdef USE_PMDK
    pmemobj_close(pop);
#endif
}

void btree::setNewRoot(char *new_root) {
    this->root = (char*)new_root;
    ++height;
}

char *btree::btree_search_pred(entry_key_t key, bool *f, char **prev, bool debug=false){
    page* p = (page*)root;

    while(p->hdr.leftmost_ptr != NULL) {
        p = (page *)p->linear_search(key);
    }

    page *t;
    while((t = (page *)p->linear_search_pred(key, prev, debug)) == p->hdr.sibling_ptr) {
        p = t;
        if(!p) {
            break;
        }
    }

    if(!t) {
        DEBUG_KEY(key, "btree_search_pred");
        //printf("NOT FOUND %lu, t = %p\n", raw_to_string(key).c_str(), t);
        *f = false;
        return NULL;
    }

    *f = true;
    return (char *)t;
}


char *btree::search(entry_key_t key) {
    DEBUG_KEY(key, "search");
    bool f = false;
    char *prev;
    char *ptr = btree_search_pred(key, &f, &prev);
    if (f) {
        list_node_t *n = (list_node_t *)ptr;
        if (n->ptr != 0){
            //printf("search success: n=%llu\n", n->ptr);
            return (char *)(n->ptr);
        }

    } else {
        ;//printf("not found.\n");
    }
    return NULL;
}

void btree::scan(entry_key_t key, int num, uint64_t buf[]){
    bool f = false;
    char *prev;
    char *ptr = btree_search_pred(key, &f, &prev);
    register int i;
    assert(f);
    list_node_t *n = (list_node_t *)ptr;
    for (i = 0; i < num; i++) {
        buf[i] = n->ptr;
        // printf("access %d-th element in scan from %lu\n", i, key);
        n = n->next;
    }
}
// insert the key in the leaf node
void btree::btree_insert_pred(entry_key_t key, char* right, char **pred, bool *update){ //need to be string
    DEBUG_KEY(key, "btree_insert_pred");
    page* p = (page*)root;

    while(p->hdr.leftmost_ptr != NULL) {
        p = (page*)p->linear_search(key);
    }
    *pred = NULL;
    if(!p->store(this, NULL, key, right, true, true, pred)) { // store
        // The key already exist.
        *update = true;
    } else {
        // Insert a new key.
        *update = false;
    }
}

void btree::insert(entry_key_t key, char *right) {
    DEBUG_KEY(key, "insert");
    list_node_t *n = (list_node_t *)alloc(sizeof(list_node_t));
    //printf("n=%p\n", n);
    n->next = NULL;
    n->key = key;
    n->ptr = (uint64_t)right;
    n->isUpdate = false;
    n->isDelete = false;
    list_node_t *prev = NULL;
    bool update;
    bool rt = false;
    btree_insert_pred(key, (char *)n, (char **)&prev, &update);
    if (update && prev != NULL) {
        // Overwrite.
        prev->ptr = (uint64_t)right;
        //flush.
        clflush((char *)prev, sizeof(list_node_t));
    }
    else {
        int retry_number = 0, w=0;
        retry:
        retry_number += 1;
        if (retry_number > 10 && w == 3) {
            return;
        }
        if (rt) {
            // we need to re-search the key!
            bool f;
            btree_search_pred(key, &f, (char **)&prev);
            if (!f) {
                printf("error!!!!\n");
                exit(-1);
            }
        }
        rt = true;
        // Insert a new key.
        if (list_head->next != NULL) {

            if (prev == NULL) {
                // Insert a smallest one.
                prev = list_head;
            }
            if (prev->isUpdate){
                w = 1;
                goto retry;
            }

            // check the order and CAS.
            list_node_t *next = prev->next;
            n->next = next;
            clflush((char *)n, sizeof(list_node_t));
            //if (prev->key < key && (next == NULL || next->key > key)) {
            if (is_key_equal(prev->key, key) < 0 && (next == NULL || is_key_equal(next->key, key) > 0)) {
                if (!__sync_bool_compare_and_swap(&(prev->next), next, n)){
                    w = 2;
                    goto retry;
                }

                clflush((char *)prev, sizeof(list_node_t));
            } else {
                // View changed, retry.
                w = 3;
                goto retry;
            }
        } else {
            // This is the first insert!
            if (!__sync_bool_compare_and_swap(&(list_head->next), NULL, n))
                goto retry;
        }
    }
    printf("exit insert\n");
}


void btree::remove(entry_key_t key) {
    bool f, debug=false;
    list_node_t *cur = NULL, *prev = NULL;
    retry:
    cur = (list_node_t *)btree_search_pred(key, &f, (char **)&prev, debug);
    if (!f) {
        printf("not found.\n");
        return;
    }
    if (prev == NULL) {
        prev = list_head;
    }
    if (prev->next != cur) {
        if (debug){
            printf("prev list node:\n");
            prev->printAll();
            printf("current list node:\n");
            cur->printAll();
        }
        goto retry;
    } else {
        // Delete it.
        if (!__sync_bool_compare_and_swap(&(prev->next), cur, cur->next))
            goto retry;
        clflush((char *)prev, sizeof(list_node_t));
        btree_delete(key);
    }

}

// store the key into the node at the given level
void btree::btree_insert_internal(char *left, entry_key_t key, char *right, uint32_t level) {
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



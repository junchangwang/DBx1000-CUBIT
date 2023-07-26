//
// Created by chien on 6/1/23.
//

#ifndef DBX1000_INDEX_BWTREE_H
#define DBX1000_INDEX_BWTREE_H

#include "bwtree/bwtree.h"
#include "global.h"
#include "helper.h"
#include "index_base.h"

class index_bwtree : public index_base {
public:
    using BwTreeType = wangziqi2013::bwtree::BwTree<idx_key_t,
                        itemid_t *,
                        std::less<idx_key_t>,
                        std::equal_to<idx_key_t>,
                        std::hash<idx_key_t>>;

    ~index_bwtree() {
        for (uint32_t i = 0; i < part_cnt; ++i) {
            delete roots[i];
        }
        return;
    }

    RC init(uint64_t part_cnt);

    RC init(uint64_t part_cnt, table_t * table);

    BwTreeType * find_root(uint64_t part_id);

    bool index_exist(idx_key_t key);

    bool index_exist(idx_key_t key, int part_id=-1);

    RC index_insert(idx_key_t key, itemid_t * item, int part_id=-1);

    RC index_read(idx_key_t key, std::vector<itemid_t *>&items, int part_id = -1);

    RC index_read(idx_key_t key, std::vector<itemid_t *>&items, int part_id = -1, int thd_id = 0);

    RC index_remove(idx_key_t key);

    int index_size();

    void UpdateThreadLocal(size_t thread_num) {
        roots[0]->UpdateThreadLocal(thread_num);
    }

    void AssignGCID(int thread_id) {
        roots[0]->AssignGCID((size_t)thread_id);
    }

    void UnregisterThread(int thread_id) {
        roots[0]->UnregisterThread((size_t)thread_id);
    }

private:
    BwTreeType **roots;
    uint64_t part_cnt;
};

#endif //DBX1000_INDEX_BWTREE_H

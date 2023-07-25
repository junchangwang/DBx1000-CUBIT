//
// Created by chien on 6/1/23.
//

#ifndef DBX1000_INDEX_ART_H
#define DBX1000_INDEX_ART_H

#include "ARTOLC/Tree.h"
#include "global.h"
#include "helper.h"
#include "index_base.h"
#include <shared_mutex>

class index_art : public index_base {
public:
    ~index_art() {
        for (uint32_t i = 0; i < part_cnt; ++i) {
            delete roots[i];
        }
        return;
    }

    RC init(uint64_t part_cnt);

    RC init(uint64_t part_cnt, table_t * table);

    ART_OLC::Tree * find_root(uint64_t part_id);

    bool index_exist(idx_key_t key);

    bool index_exist(idx_key_t key, int part_id=-1);

    RC index_insert(idx_key_t key, itemid_t * item, int part_id=-1);

	RC index_read(idx_key_t key, std::vector<itemid_t *> &items, int part_id=-1);
	
	RC index_read(idx_key_t key, std::vector<itemid_t *> &items, int part_id=-1, int thd_id=0);

    RC index_remove(idx_key_t key);

    int index_size();

	std::shared_mutex rw_lock;

private:
    ART_OLC::Tree **roots;
    uint64_t part_cnt;
};

#endif //DBX1000_INDEX_ART_H

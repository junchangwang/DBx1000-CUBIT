//
// Created by chien on 6/2/23.
//

#include <cstdio>

#include "row.h"
#include "index_bwtree.h"
#include "mem_alloc.h"

RC index_bwtree::init(uint64_t part_cnt) {
    this->part_cnt = part_cnt;
    roots = (BwTreeType**) malloc(part_cnt * sizeof(BwTreeType));
    for (uint32_t i = 0; i < part_cnt; ++i) {
        roots[i] = new BwTreeType{};
    }
    return RCOK;
}

RC index_bwtree::init(uint64_t part_cnt, table_t * table) {
    this->table = table;
    init(part_cnt);
    return RCOK;
}

int index_bwtree::index_size() {
    int size = 0;

    if (roots == NULL) {
        // cout << "NULL" << endl;
        return -1;
    }

    for (uint32_t i = 0; i < part_cnt; ++i) {
        auto iter = roots[i]->Begin();
        while (!iter.IsEnd()) {
            itemid_t *item = iter->second;
            while (item != nullptr) {
                size += sizeof(*item);
                item = item->next;
            }
            iter++;
        }
        uint64_t max_node_id = roots[i]->next_unused_node_id.load();
        std::vector<uint64_t> free_node_list{};
        auto free_node_pop_result = roots[i]->free_node_id_list.Pop();
        while (free_node_pop_result.first == true) {
            free_node_list.push_back(free_node_pop_result.second);
            free_node_pop_result = roots[i]->free_node_id_list.Pop();
        }
        for (unsigned long id = 1; id < max_node_id; id++) {
            if (std::find(free_node_list.begin(), free_node_list.end(), id) == free_node_list.end()) {
                const auto node = roots[i]->GetNode(id);
                switch (node->GetType()) {
                case BwTreeType::NodeType::InnerType: {
                    auto inner_node = static_cast<const BwTreeType::InnerNode *>(node);
                    auto elem_num = inner_node->GetSize();
                    size += elem_num * sizeof(BwTreeType::KeyNodeIDPair);

                    // auto first_key = *inner_node->KeyBegin();
                    // auto first_node_id = *inner_node->NodeIDBegin();
                    // size_t *alloc_size = new size_t;
                    // size_t *used_size = new size_t;
                    // *alloc_size = 0;
                    // *used_size = 0;
                    // auto pair = std::make_pair(first_key, first_node_id);
                    // inner_node->GetAllocationStatistics(&pair, alloc_size, used_size);
                    // size += *used_size;

                    break;
                }
                case BwTreeType::NodeType::LeafType: {
                    auto leaf_node = static_cast<const BwTreeType::LeafNode *>(node);
                    auto elem_num = leaf_node->GetSize();
                    size += elem_num * sizeof(BwTreeType::KeyValuePair);
                    break;
                }
                default:
                    break;
                }
            }
        }
    }

    return size;
}

index_bwtree::BwTreeType * index_bwtree::find_root(uint64_t part_id) {
    assert(part_id < part_cnt);
    return roots[part_id];
}


bool index_bwtree::index_exist(idx_key_t key) {
    BwTreeType * root = find_root(0);
    assert(root != NULL);

    std::vector<itemid_t *> value_list;
    root->GetValue(key, value_list);

    return value_list.size() > 0;
}

bool index_bwtree::index_exist(idx_key_t key, int part_id) {
    BwTreeType * root = find_root(part_id);
    assert(root != NULL);

    std::vector<itemid_t *> value_list;
    root->GetValue(key, value_list);

    return value_list.size() > 0;
}

RC index_bwtree::index_insert(idx_key_t key, itemid_t * item, int part_id) {
    BwTreeType * root = find_root(part_id);
    assert(root != NULL);

    root->Insert(key, item);
    return RCOK;
}

RC index_bwtree::index_read(idx_key_t key, std::vector<itemid_t *>&items, int part_id) {
    BwTreeType * root = find_root(part_id);
    std::vector<itemid_t *> item_list;
    root->GetValue(key, item_list);
    items = item_list;
    return RCOK;
}

RC index_bwtree::index_read(idx_key_t key, std::vector<itemid_t *>&items, int part_id, int thd_id) {
    BwTreeType * root = find_root(part_id);
    std::vector<itemid_t *> item_list;
    root->GetValue(key, item_list);
    items = item_list;
    return RCOK;
}

RC index_bwtree::index_remove(idx_key_t key) {
    RC rc = RCOK;

    BwTreeType * root = find_root(0);
    assert(root != NULL);

    std::vector<itemid_t *> value_list;
    root->GetValue(key, value_list);

    for (auto value : value_list) {
        root->Delete(key, value);
    }
    return rc;
}

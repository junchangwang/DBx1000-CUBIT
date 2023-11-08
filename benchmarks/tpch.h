#ifndef _TPCH_H_
#define _TPCH_H_

#include "wl.h"
#include "txn.h"

#include "fastbit/bitvector.h"
#include "nicolas/base_table.h"
#include "nicolas/util.h"
#include "ub/table.h"
#include "ucb/table.h"
#include "naive/table.h"
#include "nbub/table_lf.h"
#include "nbub/table_lk.h"

#include <algorithm>
#include <thread>

#define TPCH_Q6_SCAN_THREADS 4

class table_t;
class INDEX;
class tpch_query;

class tpch_wl : public workload {
public:
	RC init();
	RC init_table();
	RC init_bitmap();
	RC init_schema(const char * schema_file);
	RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
	RC build();
	RC printMemory();
	
	table_t * 		t_lineitem;
	table_t * 		t_orders;

	INDEX * 	i_orders;
	INDEX * 	i_lineitem;
	IndexHash *		i_Q6_hashtable;
	index_btree *	i_Q6_btree;
    index_bwtree *  i_Q6_bwtree;
	index_art *     i_Q6_art;

	BaseTable *bitmap_shipdate;
	BaseTable *bitmap_discount;
	BaseTable *bitmap_quantity;
	
	bool ** delivering;
	uint32_t next_tid;
private:
	uint64_t num_wh;
	void init_tab_orderAndLineitem();
	void init_test();
	
	void init_permutation(uint64_t * perm_c_id, uint64_t wid);
};

class tpch_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
	RC run_txn(int tid, base_query * query);
private:
	tpch_wl * _wl;
	
	RC run_Q6_scan(int tid, tpch_query * m_query);
	RC run_Q6_hash(int tid, tpch_query * query, IndexHash *index);
	RC run_Q6_btree(int tid, tpch_query * query, index_btree *index);
    RC run_Q6_bwtree(int tid, tpch_query * query, index_bwtree *index);
    RC run_Q6_art(int tid, tpch_query * query, index_art *index);
    RC run_Q6_bitmap(int tid, tpch_query *query);
    RC run_RF1(int tid);
    RC run_RF2(int tid);
};

static inline uint64_t tpch_lineitemKey(uint64_t i, uint64_t lcnt) {
  return (uint64_t)(i * 8 + lcnt);
}

static inline uint64_t tpch_lineitemKey_index(uint64_t shipdate, uint64_t discount, uint64_t quantity) {
	return (uint64_t)((shipdate * 12 + discount) * 52 + quantity);
}

static inline int bitmap_quantity_bin(uint64_t quantity) {
	// [0,23], 24, [25,49]
	if (quantity <= 23)
		return 0;
	else if (quantity == 24)
		return 1;
	else return 2;
}

#endif

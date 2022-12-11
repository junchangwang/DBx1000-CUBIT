#include "query.h"
#include "tpch_query.h"
#include "tpch.h"
#include "tpcc_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"

void tpch_query::init(uint64_t thd_id, workload * h_wl) {
	// double x = (rand() % 100);
	part_to_access = (uint64_t *) 
		mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);
	
	gen_Q6(thd_id);
	// gen_Q6_index(thd_id);
	// gen_RF1(thd_id);
	// gen_RF2(thd_id);

}

void tpch_query::gen_Q6(uint64_t thd_id) {
	type = TPCH_Q6;
	if (FIRST_PART_LOCAL)
		id = thd_id % g_num_wh + 1;
	else
		id = URand(1, g_num_wh, thd_id % g_num_wh);	
	uint64_t part_id = wh_to_part(id);
	part_to_access[0] = part_id;
	part_num = 1;


	//Q6 related vars
	uint64_t year = URand(93, 97, 0);
	// uint64_t day = URand(1, 365, 0);
	date = (uint64_t)(year * 1000 + 1);
	discount = ((double)URand(2, 9, 0)) / 100;
	quantity = (double)URand(24, 25, 0);

	// // test
	// date = (uint64_t)95222;
	// discount = (double)0.06;
	// quantity = double(24);
}

void tpch_query::gen_Q6_index(uint64_t thd_id) {
	type = TPCH_Q6_index;
	if (FIRST_PART_LOCAL)
		id = thd_id % g_num_wh + 1;
	else
		id = URand(1, g_num_wh, thd_id % g_num_wh);	
	uint64_t part_id = wh_to_part(id);
	part_to_access[0] = part_id;
	part_num = 1;


	//Q6 related vars
	uint64_t year = URand(93, 97, 0);
	// uint64_t day = URand(1, 365, 0);
	date = (uint64_t)(year * 1000 + 1);
	discount = ((double)URand(2, 9, 0)) / 100;
	quantity = (double)URand(24, 25, 0);
}

void tpch_query::gen_RF1(uint64_t thd_id) {
	type = TPCH_RF1;
	if (FIRST_PART_LOCAL)
		id = thd_id % g_num_wh + 1;
	else
		id = URand(1, g_num_wh, thd_id % g_num_wh);	

	
}

void tpch_query::gen_RF2(uint64_t thd_id) {
	type = TPCH_RF2;
	if (FIRST_PART_LOCAL)
		id = thd_id % g_num_wh + 1;
	else
		id = URand(1, g_num_wh, thd_id % g_num_wh);	
}
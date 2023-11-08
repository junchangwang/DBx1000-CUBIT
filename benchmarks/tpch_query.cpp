#include "query.h"
#include "tpch_query.h"
#include "tpch.h"
#include "tpc_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"

void tpch_query::init(uint64_t thd_id, workload * h_wl) 
{
	return;
}

void tpch_query::gen_Q6(uint64_t thd_id, uint64_t itr) 
{
	return;
}

void tpch_query::gen_RF1(uint64_t thd_id) {
	type = TPCH_RF1;
	return;
}

void tpch_query::gen_RF2(uint64_t thd_id) {
	type = TPCH_RF2;
	return;
}
#include "query.h"
#include "chbench_query.h"
#include "chbench.h"
#include "tpc_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "Date.h"
#include <ctime>

int chbench_query::q6_id = 0;
int chbench_query::q1_id = 0;

extern CHBenchQuery query_number;

void chbench_query::init(uint64_t thd_id, workload * h_wl) {
	// this thread is for running q6
	if(thd_id < CHBENCH_OLAP_NUMBER) {
		if(query_number == CHBenchQuery::CHBenchQ6)
			gen_q6(thd_id);
		else if(query_number == CHBenchQuery::CHBenchQ1)
			gen_q1(thd_id);
		else
			test_table(thd_id);
		return;
	}
	double x = (double)(rand() % 100) / 100.0;
	part_to_access = (uint64_t *) 
		mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);
	if (x < g_perc_payment)
		gen_payment(thd_id);
	else 
		gen_new_order(thd_id);
}

void chbench_query::gen_payment(uint64_t thd_id) {
	type = CHBENCH_PAYMENT;
	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
	else
		w_id = URand(1, g_num_wh, thd_id % g_num_wh);
	d_w_id = w_id;
	uint64_t part_id = wh_to_part(w_id);
	part_to_access[0] = part_id;
	part_num = 1;

	d_id = URand(1, DIST_PER_WARE, w_id-1);
	h_amount = URand(1, 5000, w_id-1);
	int x = URand(1, 100, w_id-1);
	int y = URand(1, 100, w_id-1);


	if(x <= 85) { 
		// home warehouse
		c_d_id = d_id;
		c_w_id = w_id;
	} else {	
		// remote warehouse
		c_d_id = URand(1, DIST_PER_WARE, w_id-1);
		if(g_num_wh > 1) {
			while((c_w_id = URand(1, g_num_wh, w_id-1)) == w_id) {}
			if (wh_to_part(w_id) != wh_to_part(c_w_id)) {
				part_to_access[1] = wh_to_part(c_w_id);
				part_num = 2;
			}
		} else 
			c_w_id = w_id;
	}
	if(y <= 60) {
		// by last name
		by_last_name = true;
		Lastname(NURand(255,0,999,w_id-1),c_last);
	} else {
		// by cust id
		by_last_name = false;
		c_id = NURand(1023, 1, g_cust_per_dist,w_id-1);
	}
}

void chbench_query::gen_new_order(uint64_t thd_id) {
	type = CHBENCH_NEW_ORDER;
	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
	else
		w_id = URand(1, g_num_wh, thd_id % g_num_wh);
	d_id = URand(1, DIST_PER_WARE, w_id-1);
	c_id = NURand(1023, 1, g_cust_per_dist, w_id-1);
	rbk = URand(1, 100, w_id-1);
	ol_cnt = URand(5, 15, w_id-1);

	// get time
	time_t curtime;
	time(&curtime);
	tm *nowtime = localtime(&curtime);
	Date o_entry_date(nowtime->tm_year + 1900, nowtime->tm_mon + 1, nowtime->tm_mday);
	o_entry_d = o_entry_date.DateToUint64();
	
	items = (Item_no_chbench *) _mm_malloc(sizeof(Item_no_chbench) * ol_cnt, 64);
	remote = false;
	part_to_access[0] = wh_to_part(w_id);
	part_num = 1;

	for (UInt32 oid = 0; oid < ol_cnt; oid ++) {
		items[oid].ol_i_id = NURand(8191, 1, g_max_items, w_id-1);
		UInt32 x = URand(1, 100, w_id-1);
		if (x > 1 || g_num_wh == 1)
			items[oid].ol_supply_w_id = w_id;
		else  {
			while((items[oid].ol_supply_w_id = URand(1, g_num_wh, w_id-1)) == w_id) {}
			remote = true;
		}
		items[oid].ol_quantity = URand(1, 10, w_id-1);
	}
	// Remove duplicate items
	for (UInt32 i = 0; i < ol_cnt; i ++) {
		for (UInt32 j = 0; j < i; j++) {
			if (items[i].ol_i_id == items[j].ol_i_id) {
				for (UInt32 k = i; k < ol_cnt - 1; k++)
					items[k] = items[k + 1];
				ol_cnt --;
				i--;
			}
		}
	}
	for (UInt32 i = 0; i < ol_cnt; i ++) 
		for (UInt32 j = 0; j < i; j++) 
			assert(items[i].ol_i_id != items[j].ol_i_id);
	// update part_to_access
	for (UInt32 i = 0; i < ol_cnt; i ++) {
		UInt32 j;
		for (j = 0; j < part_num; j++ ) 
			if (part_to_access[j] == wh_to_part(items[i].ol_supply_w_id))
				break;
		if (j == part_num) // not found! add to it.
		part_to_access[part_num ++] = wh_to_part( items[i].ol_supply_w_id );
	}
}

void 
chbench_query::gen_order_status(uint64_t thd_id) {
	type = CHBENCH_ORDER_STATUS;
	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
	else
		w_id = URand(1, g_num_wh, thd_id % g_num_wh);
	d_id = URand(1, DIST_PER_WARE, w_id-1);
	c_w_id = w_id;
	c_d_id = d_id;
	int y = URand(1, 100, w_id-1);
	if(y <= 60) {
		// by last name
		by_last_name = true;
		Lastname(NURand(255,0,999,w_id-1),c_last);
	} else {
		// by cust id
		by_last_name = false;
		c_id = NURand(1023, 1, g_cust_per_dist, w_id-1);
	}
}


void chbench_query::gen_q6(uint64_t thd_id) {
	min_delivery_d = 19990101;
	max_delivery_d = 20200101;
	min_quantity = 1;
	max_quantity = 1000;
	

	switch (CHBENCH_QUERY_METHOD)
	{
	case CHBenchQueryMethod::SCAN_METHOD :
		type = CHBENCH_Q6_SCAN;
		break;
	case CHBenchQueryMethod::BTREE_METHOD :
		type = CHBENCH_Q6_BTREE;
		break;
	case CHBenchQueryMethod::BITMAP_METHOD :
		type = CHBENCH_Q6_BITMAP;
		break;
	case CHBenchQueryMethod::BITMAP_PARA_METHOD :
		type = CHBENCH_Q6_BITMAP_PARALLEL;
		break;
	case CHBenchQueryMethod::BWTREE_METHOD :
		type = CHBENCH_Q6_BWTREE;
		break;
	case CHBenchQueryMethod::ART_METHOD :
		type = CHBENCH_Q6_ART;
		break;
	case CHBenchQueryMethod::ALL_METHOD :
		switch (q6_id%5)
		{
		case 0 :
			type = CHBENCH_Q6_SCAN;
			break;
		case 1 :
			type = CHBENCH_Q6_BTREE;
			break;
		case 2 :
			type = CHBENCH_Q6_BWTREE;
			break;
		case 3:
			type = CHBENCH_Q6_ART;
			break;
		case 4:
			type = CHBENCH_Q6_BITMAP;
			break;
		case 5:
			type = CHBENCH_Q6_BITMAP_PARALLEL;
			break;
		default:
			assert(false);
			break;
		}
		break;
	default:
		assert(false);
		break;
	}
	q6_id++;
	return;
}

void chbench_query::gen_q1(uint64_t thd_id) {
	min_delivery_d = CHBENCH_Q1_MIN_DELIVERY_DATE;
	switch (q1_id%4)
	{
	case 0 :
		type = CHBENCH_Q1_SCAN;
		break;
	case 1 :
		type = CHBENCH_Q1_BTREE;
		break;
	case 2 :
		type = CHBENCH_Q1_BITMAP;
		break;
	case 3 :
		type = CHBENCH_Q1_BITMAP_PARALLEL;
		break;
	default:
		assert(false);
		break;
	}

	switch (CHBENCH_QUERY_METHOD)
	{
	case CHBenchQueryMethod::SCAN_METHOD :
		type = CHBENCH_Q1_SCAN;
		break;
	case CHBenchQueryMethod::BTREE_METHOD :
		type = CHBENCH_Q1_BTREE;
		break;
	case CHBenchQueryMethod::BITMAP_METHOD :
		type = CHBENCH_Q1_BITMAP;
		break;
	case CHBenchQueryMethod::BITMAP_PARA_METHOD :
		type = CHBENCH_Q1_BITMAP_PARALLEL;
		break;
	case CHBenchQueryMethod::BWTREE_METHOD :
		type = CHBENCH_Q1_BWTREE;
		break;
	case CHBenchQueryMethod::ART_METHOD :
		type = CHBENCH_Q1_ART;
		break;
	case CHBenchQueryMethod::ALL_METHOD :
		break;
	default:
		assert(false);
		break;
	}
	q1_id++;
	return;
}

void chbench_query::test_table(uint64_t thd_id){
	type = CHBENCH_VERIFY_TABLE;
}
#include <cstdint>
#include <cstdio>
#include <shared_mutex>
#include <signal.h>
#include <tuple>
#include "global.h"
#include "tpch.h"
#include "tpch_query.h"
#include "tpc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_bwtree.h"
#include "index_art.h"
#include "tpch_const.h"

#include "perf.h"

extern bool perf_enabled;
#define WAIT_FOR_PERF_U (1000 * 50)

void tpch_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (tpch_wl *) h_wl;

	cout.precision(32);
}

RC tpch_txn_man::run_txn(int tid, base_query * query) 
{
	RC rc = RCOK;
	tpch_query * m_query = (tpch_query *) query;

	switch (m_query->type) {
		ts_t _starttime;
		ts_t _endtime;
		uint64_t _timespan;
		case TPCH_Q6_SCAN :
			_starttime = get_sys_clock();
			rc = run_Q6_scan(tid, m_query);
			_endtime = get_sys_clock();
			_timespan = _endtime - _starttime;
			INC_STATS(get_thd_id(), scan_run_time, _timespan);
			INC_STATS(get_thd_id(), Q6_scan_txn_cnt, 1);
			break;

		case TPCH_Q6_HASH :
			_starttime = get_sys_clock();
			rc = run_Q6_hash(tid, m_query, _wl->i_Q6_hashtable);
			_endtime = get_sys_clock();
			_timespan = _endtime - _starttime;
			INC_STATS(get_thd_id(), hash_run_time, _timespan);
			INC_STATS(get_thd_id(), Q6_hash_txn_cnt, 1);
			break;
		
		case TPCH_Q6_BTREE :
			_starttime = get_sys_clock();
			rc = run_Q6_btree(tid, m_query, _wl->i_Q6_btree);
			_endtime = get_sys_clock();
			_timespan = _endtime - _starttime;
			INC_STATS(get_thd_id(), btree_run_time, _timespan);
			INC_STATS(get_thd_id(), Q6_btree_txn_cnt, 1);
			break;

        case TPCH_Q6_BWTREE :
            _starttime = get_sys_clock();
            rc = run_Q6_bwtree(tid, m_query, _wl->i_Q6_bwtree);
            _endtime = get_sys_clock();
            _timespan = _endtime - _starttime;
            INC_STATS(get_thd_id(), bwtree_run_time, _timespan);
            INC_STATS(get_thd_id(), Q6_bwtree_txn_cnt, 1);
            break;

		case TPCH_Q6_ART :
            _starttime = get_sys_clock();
            rc = run_Q6_art(tid, m_query, _wl->i_Q6_art);
            _endtime = get_sys_clock();
            _timespan = _endtime - _starttime;
            INC_STATS(get_thd_id(), art_run_time, _timespan);
            INC_STATS(get_thd_id(), Q6_art_txn_cnt, 1);
            break;

		case TPCH_Q6_CUBIT :
			_starttime = get_sys_clock();
			rc = run_Q6_bitmap(tid, m_query);
			_endtime = get_sys_clock();
			_timespan = _endtime - _starttime;
			INC_STATS(get_thd_id(), cubit_run_time, _timespan);
			INC_STATS(get_thd_id(), Q6_cubit_txn_cnt, 1);
			break;

		case TPCH_RF1 :
			rc = run_RF1(tid); 
			break;

		case TPCH_RF2 :
			rc = run_RF2(tid);
			break;			

		default:
			assert(false);
	}

	return rc;
}

RC tpch_txn_man::run_Q6_scan(int tid, tpch_query * query) {
	RC rc = RCOK;
	itemid_t * item;
	int cnt = 0;
	
//	int perf_pid;
//	if (perf_enabled == true && tid == 0) {
//		perf_pid = gen_perf_process((char *)"SCAN");
//		usleep(WAIT_FOR_PERF_U);
//	}

    struct scan_block {
	    void operator()(tpch_txn_man *obj, tpch_query *query, table_t *table, uint64_t row_start, uint64_t row_end, std::tuple<double, int> &result) {
            for (uint64_t row_id = row_start; row_id < row_end; row_id++) {
				row_t * r_lt = (row_t *) &table->row_buffer[row_id];
				assert(r_lt != NULL);
				row_t * r_lt_local = obj->get_row(r_lt, SCAN);
				if (r_lt_local == NULL) {
					// Skip the deleted item.
					// return finish(Abort);
					continue;
				}

				uint64_t l_shipdate;
				r_lt_local->get_value(L_SHIPDATE, l_shipdate);
				double l_discount;
				r_lt_local->get_value(L_DISCOUNT, l_discount);
				double l_quantity;
				r_lt_local->get_value(L_QUANTITY, l_quantity);

				if ((l_shipdate / 1000) == (query->date / 1000)
					&& (uint64_t)(l_discount*100) >= (uint64_t)((uint64_t)(query->discount*100) - 1) 
					&& (uint64_t)(l_discount*100) <= (uint64_t)((uint64_t)(query->discount*100) + 1) 
					&& (uint64_t)l_quantity < (uint64_t)query->quantity)
				{
					double l_extendedprice;
					r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
					std::get<0>(result) += l_extendedprice * l_discount;
					std::get<1>(result) ++;
				}
			}
		}
	};
	
	auto start = std::chrono::high_resolution_clock::now();

	double revenue = 0;
	uint64_t max_items = (uint64_t) _wl->t_lineitem->cur_tab_size;

    std::vector<std::tuple<double, int>> results(TPCH_Q6_SCAN_THREADS);
    uint64_t block_size = max_items / TPCH_Q6_SCAN_THREADS;
	uint64_t block_start = 0;
	std::vector<std::thread> threads(TPCH_Q6_SCAN_THREADS - 1);
    for (int i = 0; i < TPCH_Q6_SCAN_THREADS - 1; i++) {
    	uint64_t block_end = block_start + block_size - 1;
        threads[i] = std::thread(scan_block(), this, query, _wl->t_lineitem, block_start, block_end, std::ref(results[i]));
		block_start = block_end + 1;
	}
    scan_block()(this, query, _wl->t_lineitem, block_start, max_items - 1, results[TPCH_Q6_SCAN_THREADS - 1]);
    for (auto &thread : threads) {
		thread.join();
    }

    for (auto &result : results) {
		revenue += std::get<0>(result);
		cnt += std::get<1>(result);
	}


	// for (uint64_t row_id = 0; row_id < max_items; row_id ++) {
    //             // We rely on hardware cache prefeching and adjacent prefeching, which is smart enough to handle this.
    //             // The only concern is that when a large number of workers run Q6_scan concurrently,
    //             // they compete for the cache, especially the LLC.
	// 	row_t * r_lt = (row_t *) &_wl->t_lineitem->row_buffer[row_id];
	// 	assert(r_lt != NULL);
	// 	row_t * r_lt_local = get_row(r_lt, SCAN);
	// 	if (r_lt_local == NULL) {
	// 		// Skip the deleted item.
	// 		// return finish(Abort);
	// 		continue;
	// 	}

	// 	uint64_t l_shipdate;
	// 	r_lt_local->get_value(L_SHIPDATE, l_shipdate);
	// 	double l_discount;
	// 	r_lt_local->get_value(L_DISCOUNT, l_discount);
	// 	double l_quantity;
	// 	r_lt_local->get_value(L_QUANTITY, l_quantity);

	// 	if ((l_shipdate / 1000) == (query->date / 1000)
	// 		&& (uint64_t)(l_discount*100) >= (uint64_t)((uint64_t)(query->discount*100) - 1) 
	// 		&& (uint64_t)(l_discount*100) <= (uint64_t)((uint64_t)(query->discount*100) + 1) 
	// 		&& (uint64_t)l_quantity < (uint64_t)query->quantity)
	// 	{
	// 			double l_extendedprice;
	// 			r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
	// 			revenue += l_extendedprice * l_discount;

	// 			cnt ++;
	// 	}
	// }

	auto end = std::chrono::high_resolution_clock::now();
	long  long time_elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();

//	if (perf_enabled == true && tid == 0) {
//		kill_perf_process(perf_pid);
//		usleep(WAIT_FOR_PERF_U);
//	}

	cout << "********Q6 with SCAN  revenue is : " << revenue << "  . Number of items: " << cnt << endl;
	string tmp = "SCAN " + to_string(cnt) + " " + to_string(time_elapsed_us) + "\n";
	output_info[tid].push_back(tmp);

	assert(rc == RCOK);
	return finish(rc);
}

RC tpch_txn_man::run_Q6_hash(int tid, tpch_query * query, IndexHash *index) 
{
	RC rc = RCOK;
	int cnt = 0;
	double revenue = 0;
	uint64_t date = query->date;	// e.g., 1st Jan. 97
	uint64_t discount = (uint64_t)(query->discount * 100); // Unit is 1
	double quantity = query->quantity;
	long  long index_us = (long  long)0;
	long long index_read_us = (long long)0;
	long long leaf_read_us = (long long)0;
	long long total_us = (long long)0;
	vector<itemid_t *> item_list{};

	auto start = std::chrono::high_resolution_clock::now();

	shared_lock<shared_mutex> r_lock(index->rw_lock);

	int perf_pid;
	if (perf_enabled == true && tid == 0) {
		perf_pid = gen_perf_process((char *)"HASH");
		usleep(WAIT_FOR_PERF_U);
	}

	for (uint64_t i = date; i <= (uint64_t)(date + 364); i++) {
		for (uint64_t j = (uint64_t)(discount - 1); j <= (uint64_t)(discount + 1); j++) {
			for (uint64_t k = (uint64_t)((uint64_t)quantity - 1); k > (uint64_t)0; k--)
			{
				uint64_t key = tpch_lineitemKey_index(i, j, k);

				if ( !index->index_exist(key, 0) ){
					continue;
				}

				auto start1 = std::chrono::high_resolution_clock::now();
				itemid_t *item = index_read((INDEX *)index, key, 0);
				auto end1 = std::chrono::high_resolution_clock::now();
				index_read_us += std::chrono::duration_cast<std::chrono::microseconds>(end1 - start1).count();
				auto start2 = std::chrono::high_resolution_clock::now();
				for (itemid_t *local_item = item; local_item != NULL; local_item = local_item->next)
				{
					item_list.push_back(local_item);
					cnt ++;
				}
				auto end2 = std::chrono::high_resolution_clock::now();
				leaf_read_us += std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2).count();
			}
		}
	}

	if (perf_enabled == true && tid == 0) {
		kill_perf_process(perf_pid);
	}

	auto end_f = std::chrono::high_resolution_clock::now();
	index_us = std::chrono::duration_cast<std::chrono::microseconds>(end_f-start).count();

	// int perf_pid;
	// if (perf_enabled == true && tid == 0) {
	// 	perf_pid = gen_perf_process((char *)"HASH");
	// 	usleep(WAIT_FOR_PERF_U);
	// }

	auto tmp_5 = std::chrono::high_resolution_clock::now();

	for (auto const &local_item : item_list) 
	{
		row_t * r_lt = ((row_t *)local_item->location);
		row_t * r_lt_local = get_row(r_lt, SCAN);
		if (r_lt_local == NULL) {
			// Skip the deleted item.
			// return finish(Abort);
			continue;
		}
		// cout << "address = " << &r_lt_local->data << endl;
		double l_extendedprice;
		r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
		double l_discount;
		r_lt_local->get_value(L_DISCOUNT, l_discount);
		revenue += l_extendedprice * l_discount;
	}

	auto end = std::chrono::high_resolution_clock::now();
	long  long tuple_us = std::chrono::duration_cast<std::chrono::microseconds>(end-tmp_5).count();
	total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

	// if (perf_enabled == true && tid == 0) {
	// 	kill_perf_process(perf_pid);
	// 	usleep(WAIT_FOR_PERF_U);
	// }

	cout << "********Q6 with Hash  revenue is : " << revenue << "  . Number of items: " << cnt << endl;
	string tmp = "Hash " + to_string(item_list.size()) + ":" + to_string(cnt) + " " + to_string(index_us+tuple_us) + "  " + to_string(index_us) + "  " + to_string(tuple_us) + "\n";
	string tmp2 = "Hash(new) " + to_string(item_list.size()) + ":" + to_string(total_us) + " " + to_string(index_read_us) + " " + to_string(leaf_read_us) + " " + to_string(tuple_us) + "\n";
	output_info[tid].push_back(tmp);
	output_info[tid].push_back(tmp2);

	assert(rc == RCOK);
	return finish(rc);
}

RC tpch_txn_man::run_Q6_btree(int tid, tpch_query * query, index_btree *index) 
{
	RC rc = RCOK;
	int cnt = 0;
	double revenue = 0;
	uint64_t date = query->date;	// e.g., 1st Jan. 97
	uint64_t discount = (uint64_t)(query->discount * 100); // Unit is 1
	double quantity = query->quantity;
	long  long index_us = (long  long)0;
	long long index_read_us = (long long)0;
	long long leaf_read_us = (long long)0;
	long long total_us = (long long)0;
	vector<itemid_t *> item_list{};

	auto start = std::chrono::high_resolution_clock::now();

	shared_lock<shared_mutex> r_lock(index->rw_lock);

	int perf_pid;
	if (perf_enabled == true && tid == 0) {
		perf_pid = gen_perf_process((char *)"BTREE");
		usleep(WAIT_FOR_PERF_U);
	}

	for (uint64_t i = date; i <= (uint64_t)(date + 364); i++) {
		for (uint64_t j = (uint64_t)(discount - 1); j <= (uint64_t)(discount + 1); j++) {
			for (uint64_t k = (uint64_t)((uint64_t)quantity - 1); k > (uint64_t)0; k--)
			{
				uint64_t key = tpch_lineitemKey_index(i, j, k);

				if ( !index->index_exist(key, 0) ){
					continue;
				}

				auto start1 = std::chrono::high_resolution_clock::now();
				itemid_t * item = index_read(index, key, 0);
				auto end1 = std::chrono::high_resolution_clock::now();
				index_read_us += std::chrono::duration_cast<std::chrono::microseconds>(end1 - start1).count();
				auto start2 = std::chrono::high_resolution_clock::now();
				for (itemid_t * local_item = item; local_item != NULL; local_item = local_item->next) {
					item_list.push_back(local_item);
					cnt ++;
				}
				auto end2 = std::chrono::high_resolution_clock::now();
				leaf_read_us += std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2).count();
			}
		}
	}

	if (perf_enabled == true && tid == 0) {
		kill_perf_process(perf_pid);
	}

	auto end_f = std::chrono::high_resolution_clock::now();
	index_us = std::chrono::duration_cast<std::chrono::microseconds>(end_f-start).count();

	// int perf_pid;
	// if (perf_enabled == true && tid == 0) {
	// 	perf_pid = gen_perf_process((char *)"BTREE");
	// 	usleep(WAIT_FOR_PERF_U);
	// }

	auto tmp_5 = std::chrono::high_resolution_clock::now();

	for (auto const &local_item : item_list)
	{
		row_t * r_lt = ((row_t *)local_item->location);
		row_t * r_lt_local = get_row(r_lt, SCAN);
		if (r_lt_local == NULL) {
			// Skip the deleted item
			// return finish(Abort);
			continue;
		}
		// cout << "address = " << &r_lt_local->data << endl;
		double l_extendedprice;
		r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
		double l_discount;
		r_lt_local->get_value(L_DISCOUNT, l_discount);
		revenue += l_extendedprice * l_discount;
	}

	auto end = std::chrono::high_resolution_clock::now();
	long long tuple_us = std::chrono::duration_cast<std::chrono::microseconds>(end-tmp_5).count();
	total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

	// if (perf_enabled == true && tid == 0) {
	// 	kill_perf_process(perf_pid);
	// 	usleep(WAIT_FOR_PERF_U);
	// }

	cout << "********Q6 with BTree revenue is : " << revenue << "  . Number of items: " << cnt << endl;
	string tmp = "BTree " + to_string(item_list.size()) + ":" + to_string(cnt) + " " + to_string(index_us+tuple_us) + "  " + to_string(index_us) + "  " + to_string(tuple_us) + "\n";
	string tmp2 = "BTree(new) " + to_string(item_list.size()) + ":" + to_string(total_us) + " " + to_string(index_read_us) + " " + to_string(leaf_read_us) + " " + to_string(tuple_us) + "\n";
	output_info[tid].push_back(tmp);
	output_info[tid].push_back(tmp2);

	assert(rc == RCOK);
	return finish(rc);
}

RC tpch_txn_man::run_Q6_bwtree(int tid, tpch_query *query, index_bwtree *index) {
    RC rc = RCOK;
    int cnt = 0;
    double revenue = 0;
    uint64_t date = query->date;
    uint64_t discount = (uint64_t)(query->discount * 100);
    double quantity = query->quantity;
    long long index_us = (long long) 0;
	long long index_read_us = (long long)0;
	long long leaf_read_us = (long long)0;
	long long total_us = (long long)0;
    vector<itemid_t *> item_list{};

    auto start = std::chrono::high_resolution_clock::now();

    int perf_pid;
    if (perf_enabled == true && tid == 0) {
        perf_pid = gen_perf_process((char *)"BWTREE");
        usleep(WAIT_FOR_PERF_U);
    }

    index->AssignGCID(tid);
    for (uint64_t i = date; i <= (uint64_t)(date + 364); i++) {
        for (uint64_t j = (uint64_t)(discount - 1); j <= (uint64_t)(discount + 1); j++) {
            for (uint64_t k = (uint64_t)((uint64_t)quantity - 1); k > (uint64_t)0; k--) {
                uint key = tpch_lineitemKey_index(i, j, k);

                if ( !index->index_exist(key, 0) ) {
                    continue;
                }

				auto start1 = std::chrono::high_resolution_clock::now();
                vector<itemid_t *> items = index_read(index, key, 0);
				auto end1 = std::chrono::high_resolution_clock::now();
				index_read_us += std::chrono::duration_cast<std::chrono::microseconds>(end1 - start1).count();
				auto start2 = std::chrono::high_resolution_clock::now();
				for (auto item : items) {
					item_list.push_back(item);
                	cnt ++;
				}
				auto end2 = std::chrono::high_resolution_clock::now();
				leaf_read_us += std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2).count();
            }
        }
    }
    index->UnregisterThread(tid);

    if (perf_enabled == true && tid == 0) {
        kill_perf_process(perf_pid);
    }

    auto end_f = std::chrono::high_resolution_clock::now();
    index_us = std::chrono::duration_cast<std::chrono::microseconds>(end_f-start).count();

    // int perf_pid;
    // if (perf_enabled == true && tid == 0) {
    //     perf_pid = gen_perf_process((char *)"BWTREE");
    //     usleep(WAIT_FOR_PERF_U);
    // }

    auto tmp_5 = std::chrono::high_resolution_clock::now();

    for (auto const &local_item : item_list) {
        row_t * r_lt = ((row_t *)local_item->location);
        row_t * r_lt_local = get_row(r_lt, SCAN);
        if (r_lt_local == NULL) {
            // Skip the deleted item
            // return finish(Abort);
            continue;
        }
        // cout << "address = " << &r_lt_local->data << endl;
        double l_extendedprice;
        r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
		double l_discount;
		r_lt_local->get_value(L_DISCOUNT, l_discount);
		revenue += l_extendedprice * l_discount;
    }

    auto end = std::chrono::high_resolution_clock::now();
    long long tuple_us = std::chrono::duration_cast<std::chrono::microseconds>(end-tmp_5).count();
	total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    // if (perf_enabled == true && tid == 0) {
    //     kill_perf_process(perf_pid);
    //     usleep(WAIT_FOR_PERF_U);
    // }

	cout << "********Q6 with BWTree revenue is : " << revenue << "  . Number of items: " << cnt << endl;;
    string tmp = "BWTree " + to_string(item_list.size()) + ":" + to_string(cnt) + " " + to_string(index_us+tuple_us) + "  " + to_string(index_us) + "  " + to_string(tuple_us) + "\n";
	string tmp2 = "BWTree(new) " + to_string(item_list.size()) + ":" + to_string(total_us) + " " + to_string(index_read_us) + " " + to_string(leaf_read_us) + " " + to_string(tuple_us) + "\n";
    output_info[tid].push_back(tmp);
    output_info[tid].push_back(tmp2);

    assert(rc == RCOK);
    return finish(rc);
}


RC tpch_txn_man::run_Q6_art(int tid, tpch_query * query, index_art *index) 
{
	RC rc = RCOK;
	int cnt = 0;
	double revenue = 0;
	uint64_t date = query->date;	// e.g., 1st Jan. 97
	uint64_t discount = (uint64_t)(query->discount * 100); // Unit is 1
	double quantity = query->quantity;
	long  long index_us = (long  long)0;
	long long index_read_us = (long long)0;
	long long leaf_read_us = (long long)0;
	long long total_us = (long long)0;
	vector<itemid_t *> item_list{};

	auto start = std::chrono::high_resolution_clock::now();

	shared_lock<shared_mutex> r_lock(index->rw_lock);

	int perf_pid;
	if (perf_enabled == true && tid == 0) {
		perf_pid = gen_perf_process((char *)"ART");
		usleep(WAIT_FOR_PERF_U);
	}

	for (uint64_t i = date; i <= (uint64_t)(date + 364); i++) {
		for (uint64_t j = (uint64_t)(discount - 1); j <= (uint64_t)(discount + 1); j++) {
			for (uint64_t k = (uint64_t)((uint64_t)quantity - 1); k > (uint64_t)0; k--)
			{
				uint64_t key = tpch_lineitemKey_index(i, j, k);

				if ( !index->index_exist(key, 0) ){
					continue;
				}

				auto start1 = std::chrono::high_resolution_clock::now();
				itemid_t * item = index_read((INDEX *)index, key, 0);
				auto end1 = std::chrono::high_resolution_clock::now();
				index_read_us += std::chrono::duration_cast<std::chrono::microseconds>(end1 - start1).count();
				auto start2 = std::chrono::high_resolution_clock::now();
				for (itemid_t * local_item = item; local_item != NULL; local_item = local_item->next) {
					item_list.push_back(local_item);
					cnt ++;
				}
				auto end2 = std::chrono::high_resolution_clock::now();
				leaf_read_us += std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2).count();
			}
		}
	}

	if (perf_enabled == true && tid == 0) {
		kill_perf_process(perf_pid);
	}

	auto end_f = std::chrono::high_resolution_clock::now();
	index_us = std::chrono::duration_cast<std::chrono::microseconds>(end_f-start).count();

	// int perf_pid;
	// if (perf_enabled == true && tid == 0) {
	// 	perf_pid = gen_perf_process((char *)"ART");
	// 	usleep(WAIT_FOR_PERF_U);
	// }

	auto tmp_5 = std::chrono::high_resolution_clock::now();

	for (auto const &local_item : item_list)
	{
		row_t * r_lt = ((row_t *)local_item->location);
		row_t * r_lt_local = get_row(r_lt, SCAN);
		if (r_lt_local == NULL) {
			// Skip the deleted item
			// return finish(Abort);
			continue;
		}
		// cout << "address = " << &r_lt_local->data << endl;
		double l_extendedprice;
		r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
		double l_discount;
		r_lt_local->get_value(L_DISCOUNT, l_discount);
		revenue += l_extendedprice * l_discount;
	}

	auto end = std::chrono::high_resolution_clock::now();
	long long tuple_us = std::chrono::duration_cast<std::chrono::microseconds>(end-tmp_5).count();
	total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

	// if (perf_enabled == true && tid == 0) {
	// 	kill_perf_process(perf_pid);
	// 	usleep(WAIT_FOR_PERF_U);
	// }

	cout << "********Q6 with ART revenue is : " << revenue << "  . Number of items: " << cnt << endl;
	string tmp = "ART " + to_string(item_list.size()) + ":" + to_string(cnt) + " " + to_string(index_us+tuple_us) + "  " + to_string(index_us) + "  " + to_string(tuple_us) + "\n";
	string tmp2 = "ART(new) " + to_string(item_list.size()) + ":" + to_string(total_us) + " " + to_string(index_read_us) + " " + to_string(leaf_read_us) + " " + to_string(tuple_us) + "\n";
	output_info[tid].push_back(tmp);
	output_info[tid].push_back(tmp2);

	assert(rc == RCOK);
	return finish(rc);
}

RC tpch_txn_man::run_Q6_bitmap(int tid, tpch_query *query)
{
	RC rc = RCOK;
	
	int year_val = (query->date / 1000) - 92;  // [0, 7]
	assert(year_val >= 1);
	assert(year_val <= 7);
	int discount_val = query->discount * 100; // [0, 10]
	assert(discount_val >= 2);
	assert(discount_val <= 9);
	int quantity_val = query->quantity; // [24, 25]
	assert(quantity_val == 24 || quantity_val == 25);
	long  long index_us = (long  long)0;

	int perf_pid;
	if (perf_enabled == true && tid == 0) {
		perf_pid = gen_perf_process((char *)"CUBIT");
		usleep(WAIT_FOR_PERF_U);
	}

	auto start = std::chrono::high_resolution_clock::now();
	
	// TODO: 
	// To get the accurate revenue, we should invoke bitmap->evaluate(),
	// which makes a private copy of the underlying bitvector if there are related RUBs.
	// Optimizations can be applied to remove the cost of making private copies.
	// However, it makes our memory reclamation mechanism more complex. Therefore, currently, 
	// we simply retrive a pointer to the latest bitvector, which is always safe to access.
	nbub::Nbub *bitmap_sd, *bitmap_dc, *bitmap_qt;
	bitmap_sd = dynamic_cast<nbub::Nbub *>(_wl->bitmap_shipdate);
	bitmap_sd->trans_begin(tid);
	ibis::bitvector *btv_shipdate = bitmap_sd->bitmaps[year_val]->btv;

	bitmap_dc = dynamic_cast<nbub::Nbub *>(_wl->bitmap_discount);
	bitmap_dc->trans_begin(tid);
	ibis::bitvector btv_discount;
	btv_discount.copy(*bitmap_dc->bitmaps[discount_val-1]->btv);
	btv_discount |= *bitmap_dc->bitmaps[discount_val]->btv;
	btv_discount |= *bitmap_dc->bitmaps[discount_val+1]->btv;

	auto tmp_1 = std::chrono::high_resolution_clock::now();

	bitmap_qt = dynamic_cast<nbub::Nbub *>(_wl->bitmap_quantity);
	bitmap_qt->trans_begin(tid);
	ibis::bitvector result;
	result.copy(*bitmap_qt->bitmaps[0]->btv);
	for (int i = 1; i < bitmap_quantity_bin(quantity_val); i++) {
		result |= *bitmap_qt->bitmaps[i]->btv;
	}

	auto tmp_2 = std::chrono::high_resolution_clock::now();

	result &= btv_discount;
	result &= *btv_shipdate;

	auto tmp_3 = std::chrono::high_resolution_clock::now();

	int cnt = 0;
	double revenue = 0;
	row_t *row_buffer = _wl->t_lineitem->row_buffer;

	// NOTE: we choose the factor 0.03 because in TPCH Q6,
	//			the selectivity is about 2%.
	uint64_t n_ids_max = _wl->t_lineitem->cur_tab_size * 0.03;
	int *ids = new int[n_ids_max];

	// Convert bitvector to ID list
	for (ibis::bitvector::indexSet is = result.firstIndexSet(); is.nIndices() > 0; ++ is) 
	{
		const ibis::bitvector::word_t *ii = is.indices();
		if (is.isRange()) {
			for (ibis::bitvector::word_t j = *ii;
					j < ii[1];
					++ j) {
				ids[cnt] = j;
				++ cnt;
			}
		}
		else {
			for (unsigned j = 0; j < is.nIndices(); ++ j) {
				ids[cnt] = ii[j];
				++ cnt;
			}
		}
	}

	auto tmp_4 = std::chrono::high_resolution_clock::now();
	index_us = std::chrono::duration_cast<std::chrono::microseconds>(tmp_4-start).count();

	if (perf_enabled == true && tid == 0) {
		kill_perf_process(perf_pid);
	}

	// int perf_pid;
	// if (perf_enabled == true && tid == 0) {
	// 	perf_pid = gen_perf_process((char *)"CUBIT");
	// 	usleep(WAIT_FOR_PERF_U);
	// }

	auto tmp_5 = std::chrono::high_resolution_clock::now();

	// Fetch tuples in ID list
	for(int k = 0; k < cnt; k++) 
	{
		row_t *row_tmp = (row_t *) &row_buffer[ids[k]];
		row_t *row_local = get_row(row_tmp, SCAN);
		if (row_local == NULL) {
			// return finish(Abort);
			continue;
		}
		double l_extendedprice;
		row_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
		double l_discount;
		row_local->get_value(L_DISCOUNT, l_discount);
		revenue += l_extendedprice * l_discount; 
	}

	auto end = std::chrono::high_resolution_clock::now();
	long long tuple_us = std::chrono::duration_cast<std::chrono::microseconds>(end-tmp_5).count();

	// if (perf_enabled == true && tid == 0) {
	// 	kill_perf_process(perf_pid);
	// 	usleep(WAIT_FOR_PERF_U);
	// }

	cout << "********Q6 with CUBIT revenue is : " << revenue << "  . Number of items: " << cnt << endl;
	string tmp = "CUBIT " + to_string(cnt) + " " + to_string(index_us+tuple_us) + "  " + to_string(index_us) + "  " + to_string(tuple_us) + "\n";
	output_info[tid].push_back(tmp);

	// Detailed performance analysis
        // if (tid == 0) {
	//         cout << "[CUBIT Q6]: tmp_1: " << std::chrono::duration_cast<std::chrono::microseconds>(tmp_1-start).count()
	// 		<< "  tmp_2: " << std::chrono::duration_cast<std::chrono::microseconds>(tmp_2-tmp_1).count()
	// 		<< "  tmp_3: " << std::chrono::duration_cast<std::chrono::microseconds>(tmp_3-tmp_2).count()
	// 		<< "  tmp_4: " << std::chrono::duration_cast<std::chrono::microseconds>(tmp_4-tmp_3).count()
	// 		<< "  end: " << std::chrono::duration_cast<std::chrono::microseconds>(end-tmp_4).count() << endl;
        // }

	delete [] ids;
	assert(rc == RCOK);
	bitmap_qt->trans_commit(tid);
	bitmap_dc->trans_commit(tid);
	bitmap_sd->trans_commit(tid);
	return finish(rc);
}

RC tpch_txn_man::run_RF1(int tid) 
{
	// for (uint64_t i = (uint64_t)(g_num_orders + 1); i < (uint64_t)(SF * 1500 + g_num_orders + 1); ++i) {
	// RF1 is generated one hundredth.

	RC rc = RCOK;
	row_t * row;
	double ins_revenue = 0;
	uint64_t row_id1;
	_wl->t_orders->get_new_row(row, 0, row_id1);
	row_id1 ++;
	//Primary key
	row->set_primary_key(row_id1);
	row->set_value(O_ORDERKEY, row_id1);

	//Related data
	uint64_t year = URand(92, 98, 0);
	uint64_t day = URand(1, 365, 0);
	if (year == 98) {
		day = day % 214;
	} 
	uint64_t orderdate = (uint64_t)(year*1000 + day);
	row->set_value(O_ORDERDATE, orderdate); 

	//Unrelated data
	row->set_value(O_CUSTKEY, (uint64_t)123456);
	row->set_value(O_ORDERSTATUS, 'A');
	row->set_value(O_TOTALPRICE, (double)12345.56); // May be used
	char temp[20];
	MakeAlphaString(10, 19, temp, 0);
	row->set_value(O_ORDERPRIORITY, temp);
	row->set_value(O_CLERK, temp);
	row->set_value(O_SHIPPRIORITY, (uint64_t)654321);
	row->set_value(O_COMMENT, temp);

	unique_lock<shared_mutex> w_lock(_wl->i_orders->rw_lock);
	_wl->index_insert(_wl->i_orders, row_id1, row, 0);

	// **********************Lineitems*****************************************

	unique_lock<shared_mutex> w_lock1(_wl->i_lineitem->rw_lock);
	unique_lock<shared_mutex> w_lock2(_wl->i_Q6_hashtable->rw_lock);
	unique_lock<shared_mutex> w_lock3(_wl->i_Q6_btree->rw_lock);

	dynamic_cast<nbub::Nbub *>(_wl->bitmap_shipdate)->trans_begin(tid);
	dynamic_cast<nbub::Nbub *>(_wl->bitmap_discount)->trans_begin(tid);
	dynamic_cast<nbub::Nbub *>(_wl->bitmap_quantity)->trans_begin(tid);

	uint64_t lines = URand(1, 7, 0);
	g_nor_in_lineitems += lines;
	for (uint64_t lcnt = 1; lcnt <= lines; lcnt++) {
		row_t * row2;
		uint64_t row_id2;
		_wl->t_lineitem->get_new_row_seq(row2, 0, row_id2);

		// Populate data
		// Primary key
		row2->set_primary_key(tpch_lineitemKey(row_id1, lcnt));
		row2->set_value(L_ORDERKEY, row_id1);
		row2->set_value(L_LINENUMBER, lcnt);

		// Related data
		double quantity = (double)URand(1, 50, 0);
		row2->set_value(L_QUANTITY, quantity); 
		uint64_t partkey = URand(1, SF * 200000, 0);	// Related to SF
		row2->set_value(L_PARTKEY, partkey); 
		uint64_t p_retailprice = (uint64_t)((90000 + ((partkey / 10) % 20001) + 100 *(partkey % 1000)) /100); // Defined in table PART
		row2->set_value(L_EXTENDEDPRICE, (double)(quantity * p_retailprice)); // 
		uint64_t discount = URand(0, 10, 0); // discount is defined as int for Q6
		row2->set_value(L_DISCOUNT, ((double)discount) / 100); 
		uint64_t shipdate;
		uint64_t dayAdd = URand(1, 121, 0);
		if (day + dayAdd > (uint64_t)365) {
			shipdate = (uint64_t)((year+1) * 1000 + day + dayAdd - 365);
		} else {
			shipdate = (uint64_t) (year *1000 + day + dayAdd);
		}
		row2->set_value(L_SHIPDATE, shipdate);	 //  O_ORDERDATE + random value[1.. 121]
		
		//Unrelated data
		row2->set_value(L_SUPPKEY, (uint64_t)123456);
		row2->set_value(L_TAX, (double)URand(0, 8, 0));
		row2->set_value(L_RETURNFLAG, 'a');
		row2->set_value(L_LINESTATUS, 'b');
		row2->set_value(L_COMMITDATE, (uint64_t)2022);
		row2->set_value(L_RECEIPTDATE, (uint64_t)2022);
		char temp[20];
		MakeAlphaString(10, 19, temp, 0);
		row2->set_value(L_SHIPINSTRUCT, temp);
		row2->set_value(L_SHIPMODE, temp);
		row2->set_value(L_COMMENT, temp);

		ins_revenue += (double)(quantity * p_retailprice) * ((double)discount / 100);

		// Index (scan)
		uint64_t key = tpch_lineitemKey(row_id1, lcnt);
		_wl->index_insert(_wl->i_lineitem, key, row2, 0);

		// Q6 index (hash)
		uint64_t Q6_key = tpch_lineitemKey_index(shipdate, discount, (uint64_t)quantity);
		_wl->index_insert((INDEX *)_wl->i_Q6_hashtable, Q6_key, row2, 0);

		// Q6 index (btree)
		_wl->index_insert((INDEX *)_wl->i_Q6_btree, Q6_key, row2, 0);

		// CUBIT
		_wl->bitmap_shipdate->append(tid, (shipdate/1000-92));
		_wl->bitmap_discount->append(tid, discount);
		_wl->bitmap_quantity->append(tid, bitmap_quantity_bin(quantity));
	}

	cout << "******** RF1 completes successfully ********" << endl
		<< lines << " tuples with revenue being " << ins_revenue << " have been inserted." << endl << endl;

	assert(rc == RCOK);
	dynamic_cast<nbub::Nbub *>(_wl->bitmap_shipdate)->trans_commit(tid);
	dynamic_cast<nbub::Nbub *>(_wl->bitmap_discount)->trans_commit(tid);
	dynamic_cast<nbub::Nbub *>(_wl->bitmap_quantity)->trans_commit(tid);
	return finish(rc);
}

RC tpch_txn_man::run_RF2(int tid) 
{
	// for (uint64_t i = 1; i < (uint64_t)(SF * 1500); ++i)
	// RF2 is generated one hundredth.

	RC rc = RCOK;
	int del_cnt = 0;
	double del_revenue = 0;

	/****** Process Table ORDER ******/

	INDEX *index1 = _wl->i_orders;
	uint64_t key1 = URand(1, g_num_orders, 0);
	if ( !index1->index_exist(key1, 0)) {
		return finish(Abort);
	}
	itemid_t * item1 = index_read(index1, key1, 0);

	row_t * row1 = ((row_t *)item1->location);
	// Delete the row
	row_t * row1_local = get_row(row1, DEL);
	if (row1_local == NULL) {
		return finish(Abort);
	}
	// Delete from the index
	unique_lock<shared_mutex> w_lock(_wl->i_orders->rw_lock);
	_wl->i_orders->index_remove(key1);

	/****** Process Table LINEITEM ******/

	INDEX *index2 = _wl->i_lineitem;
	unique_lock<shared_mutex> w_lock1(_wl->i_lineitem->rw_lock);
	unique_lock<shared_mutex> w_lock2(_wl->i_Q6_hashtable->rw_lock);
	unique_lock<shared_mutex> w_lock3(_wl->i_Q6_btree->rw_lock);

	dynamic_cast<nbub::Nbub *>(_wl->bitmap_shipdate)->trans_begin(tid);
	dynamic_cast<nbub::Nbub *>(_wl->bitmap_discount)->trans_begin(tid);
	dynamic_cast<nbub::Nbub *>(_wl->bitmap_quantity)->trans_begin(tid);

	for (uint64_t lcnt = (uint64_t)1; lcnt <= (uint64_t)7; ++lcnt)
	{
		uint64_t key2 = tpch_lineitemKey(key1, lcnt);
		if (!index2->index_exist(key2, 0)) {
			continue;
		}
		itemid_t * item2 = index_read(index2, key2, 0);
		if (!item2)
			continue;
		row_t * row2 = ((row_t *)item2->location);
		// Delete the row
		row_t * row2_local = get_row(row2, DEL);
		if (row2_local == NULL) {
			return finish(Abort);
		}	
		// Delete from the index
		_wl->i_lineitem->index_remove(key2);
		del_cnt ++;	

		// Delete from Q6_hashtable
		uint64_t l_shipdate;
		row2_local->get_value(L_SHIPDATE, l_shipdate);
		double l_discount;
		row2_local->get_value(L_DISCOUNT, l_discount);
		double l_quantity;
		row2_local->get_value(L_QUANTITY, l_quantity);

		uint64_t Q6_key = tpch_lineitemKey_index(l_shipdate, (int)(l_discount*100), l_quantity);
		rc = _wl->i_Q6_hashtable->index_remove(Q6_key);

		// Delete from BTree
		rc = _wl->i_Q6_btree->index_remove(Q6_key);

		// Bitmap
		uint64_t row_id = row2 - _wl->t_lineitem->row_buffer;
		_wl->bitmap_discount->remove(tid, row_id);
		_wl->bitmap_quantity->remove(tid, row_id);
		_wl->bitmap_shipdate->remove(tid, row_id);

		double l_extendedprice;
		row2_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
		del_revenue += l_extendedprice * l_discount;
	}

	cout << "******** RF2 completes successfully ********" << endl
			<< del_cnt << " tuples with revenue being " << del_revenue << " have been removed." << endl << endl;

	assert(rc == RCOK);
	dynamic_cast<nbub::Nbub *>(_wl->bitmap_shipdate)->trans_commit(tid);
	dynamic_cast<nbub::Nbub *>(_wl->bitmap_discount)->trans_commit(tid);
	dynamic_cast<nbub::Nbub *>(_wl->bitmap_quantity)->trans_commit(tid);
	return finish(rc);
}

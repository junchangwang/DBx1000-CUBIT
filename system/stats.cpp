#include "global.h"
#include "helper.h"
#include "stats.h"
#include "mem_alloc.h"

#define BILLION 1000000000UL

void Stats_thd::init(uint64_t thd_id) {
	clear();
	all_debug1 = (uint64_t *)
		_mm_malloc(sizeof(uint64_t) * MAX_TXN_PER_PART, 64);
	all_debug2 = (uint64_t *)
		_mm_malloc(sizeof(uint64_t) * MAX_TXN_PER_PART, 64);
}

void Stats_thd::clear() {
	txn_cnt = 0;
	abort_cnt = 0;
	run_time = 0;
	time_man = 0;
	debug1 = 0;
	debug2 = 0;
	debug3 = 0;
	debug4 = 0;
	debug5 = 0;
	time_index = 0;
	time_abort = 0;
	time_cleanup = 0;
	time_wait = 0;
	time_ts_alloc = 0;
	latency = 0;
	time_query = 0;
	Q6_btree_txn_cnt = 0;
	Q6_bwtree_txn_cnt = 0;
	Q6_art_txn_cnt = 0;
	Q6_cubit_txn_cnt = 0;
	Q6_hash_txn_cnt = 0;
	Q6_scan_txn_cnt = 0;
	scan_run_time = 0;
	hash_run_time = 0;
	btree_run_time = 0;
	bwtree_run_time = 0;
	art_run_time = 0;
	cubit_run_time = 0;
}

void Stats_tmp::init() {
	clear();
}

void Stats_tmp::clear() {	
	time_man = 0;
	time_index = 0;
	time_wait = 0;
}

void Stats::init() {
	if (!STATS_ENABLE) 
		return;
	_stats = (Stats_thd**) 
			_mm_malloc(sizeof(Stats_thd*) * g_thread_cnt, 64);
	tmp_stats = (Stats_tmp**) 
			_mm_malloc(sizeof(Stats_tmp*) * g_thread_cnt, 64);
	dl_detect_time = 0;
	dl_wait_time = 0;
	deadlock = 0;
	cycle_detect = 0;
}

void Stats::init(uint64_t thread_id) {
	if (!STATS_ENABLE) 
		return;
	_stats[thread_id] = (Stats_thd *) 
		_mm_malloc(sizeof(Stats_thd), 64);
	tmp_stats[thread_id] = (Stats_tmp *)
		_mm_malloc(sizeof(Stats_tmp), 64);

	_stats[thread_id]->init(thread_id);
	tmp_stats[thread_id]->init();
}

void Stats::clear(uint64_t tid) {
	if (STATS_ENABLE) {
		_stats[tid]->clear();
		tmp_stats[tid]->clear();

		dl_detect_time = 0;
		dl_wait_time = 0;
		cycle_detect = 0;
		deadlock = 0;
	}
}

void Stats::add_debug(uint64_t thd_id, uint64_t value, uint32_t select) {
	if (g_prt_lat_distr && warmup_finish) {
		uint64_t tnum = _stats[thd_id]->txn_cnt;
		if (select == 1)
			_stats[thd_id]->all_debug1[tnum] = value;
		else if (select == 2)
			_stats[thd_id]->all_debug2[tnum] = value;
	}
}

void Stats::commit(uint64_t thd_id) {
	if (STATS_ENABLE) {
		_stats[thd_id]->time_man += tmp_stats[thd_id]->time_man;
		_stats[thd_id]->time_index += tmp_stats[thd_id]->time_index;
		_stats[thd_id]->time_wait += tmp_stats[thd_id]->time_wait;
		tmp_stats[thd_id]->init();
	}
}

void Stats::abort(uint64_t thd_id) {	
	if (STATS_ENABLE) 
		tmp_stats[thd_id]->init();
}

void Stats::print() {
	
	uint64_t total_txn_cnt = 0;
	uint64_t total_abort_cnt = 0;
	double total_run_time = 0;
	double total_scan_run_time = 0;
	double total_hash_run_time = 0;
	double total_btree_run_time = 0;
	double total_bwtree_run_time = 0;
	double total_art_run_time = 0;
	double total_cubit_run_time = 0;
	// uint64_t total_Q6_txn_cnt = 0;
	uint64_t total_scan_Q6_txn_cnt = 0;
	uint64_t total_hash_Q6_txn_cnt = 0;
	uint64_t total_btree_Q6_txn_cnt = 0;
	uint64_t total_bwtree_Q6_txn_cnt = 0;
	uint64_t total_art_Q6_txn_cnt = 0;
	uint64_t total_cubit_Q6_txn_cnt = 0;
	double total_time_man = 0;
	double total_debug1 = 0;
	double total_debug2 = 0;
	double total_debug3 = 0;
	double total_debug4 = 0;
	double total_debug5 = 0;
	double total_time_index = 0;
	double total_time_abort = 0;
	double total_time_cleanup = 0;
	double total_time_wait = 0;
	double total_time_ts_alloc = 0;
	double total_latency = 0;
	double total_time_query = 0;
	for (uint64_t tid = 0; tid < g_thread_cnt; tid ++) {
		total_txn_cnt += _stats[tid]->txn_cnt;
		total_abort_cnt += _stats[tid]->abort_cnt;
		total_run_time += _stats[tid]->run_time;
		total_scan_run_time += _stats[tid]->scan_run_time;
		total_hash_run_time += _stats[tid]->hash_run_time;
		total_btree_run_time += _stats[tid]->btree_run_time;
		total_bwtree_run_time += _stats[tid]->bwtree_run_time;
		total_art_run_time += _stats[tid]->art_run_time;
		total_cubit_run_time += _stats[tid]->cubit_run_time;
		// total_Q6_txn_cnt += _stats[tid]->Q6_tnx_cnt;
		total_scan_Q6_txn_cnt += _stats[tid]->Q6_scan_txn_cnt;
		total_hash_Q6_txn_cnt += _stats[tid]->Q6_hash_txn_cnt;
		total_btree_Q6_txn_cnt += _stats[tid]->Q6_btree_txn_cnt;
		total_bwtree_Q6_txn_cnt += _stats[tid]->Q6_bwtree_txn_cnt;
		total_art_Q6_txn_cnt += _stats[tid]->Q6_art_txn_cnt;
		total_cubit_Q6_txn_cnt += _stats[tid]->Q6_cubit_txn_cnt;
		total_time_man += _stats[tid]->time_man;
		total_debug1 += _stats[tid]->debug1;
		total_debug2 += _stats[tid]->debug2;
		total_debug3 += _stats[tid]->debug3;
		total_debug4 += _stats[tid]->debug4;
		total_debug5 += _stats[tid]->debug5;
		total_time_index += _stats[tid]->time_index;
		total_time_abort += _stats[tid]->time_abort;
		total_time_cleanup += _stats[tid]->time_cleanup;
		total_time_wait += _stats[tid]->time_wait;
		total_time_ts_alloc += _stats[tid]->time_ts_alloc;
		total_latency += _stats[tid]->latency;
		total_time_query += _stats[tid]->time_query;
		
		printf("[tid=%ld] txn_cnt=%ld,abort_cnt=%ld\n", 
			tid,
			_stats[tid]->txn_cnt,
			_stats[tid]->abort_cnt
		);
	}
	// print Scan BTree Hash CUBIT
	for (auto it : output_info) {
		for (auto it2 : it) {
			cout << it2;
		}
	}

	FILE * outf;
	if (output_file != NULL) {
		outf = fopen(output_file, "w");
		fprintf(outf, "[summary] txn_cnt=%ld, abort_cnt=%ld"
			", run_time=%f, time_wait=%f, time_ts_alloc=%f"
			", time_man=%f, time_index=%f, time_abort=%f, time_cleanup=%f, latency=%f"
			", deadlock_cnt=%ld, cycle_detect=%ld, dl_detect_time=%f, dl_wait_time=%f"
			", time_query=%f, debug1=%f, debug2=%f, debug3=%f, debug4=%f, debug5=%f\n",
			total_txn_cnt, 
			total_abort_cnt,
			total_run_time / BILLION,
			total_time_wait / BILLION,
			total_time_ts_alloc / BILLION,
			(total_time_man - total_time_wait) / BILLION,
			total_time_index / BILLION,
			total_time_abort / BILLION,
			total_time_cleanup / BILLION,
			total_latency / BILLION / total_txn_cnt,
			deadlock,
			cycle_detect,
			dl_detect_time / BILLION,
			dl_wait_time / BILLION,
			total_time_query / BILLION,
			total_debug1, // / BILLION,
			total_debug2, // / BILLION,
			total_debug3, // / BILLION,
			total_debug4, // / BILLION,
			total_debug5 / BILLION
		);
		fclose(outf);
	}
	printf("[summary] txn_cnt=%ld, abort_cnt=%ld"
		", run_time=%f, time_wait=%f, time_ts_alloc=%f"
		", time_man=%f, time_index=%f, time_abort=%f, time_cleanup=%f, latency=%f"
		", deadlock_cnt=%ld, cycle_detect=%ld, dl_detect_time=%f, dl_wait_time=%f"
		", time_query=%f, debug1=%f, debug2=%f, debug3=%f, debug4=%f, debug5=%f\n", 
		total_txn_cnt, 
		total_abort_cnt,
		total_run_time / BILLION,
		total_time_wait / BILLION,
		total_time_ts_alloc / BILLION,
		(total_time_man - total_time_wait) / BILLION,
		total_time_index / BILLION,
		total_time_abort / BILLION,
		total_time_cleanup / BILLION,
		total_latency / BILLION / total_txn_cnt,
		deadlock,
		cycle_detect,
		dl_detect_time / BILLION,
		dl_wait_time / BILLION,
		total_time_query / BILLION,
		total_debug1 / BILLION,
		total_debug2, // / BILLION,
		total_debug3, // / BILLION,
		total_debug4, // / BILLION,
		total_debug5  // / BILLION 
	);
	printf("\n*************** Throughput output****************\n");
	printf("ThroughputScan %f\n", total_scan_Q6_txn_cnt / (total_scan_run_time / BILLION) * g_thread_cnt);
	printf("ThroughputHash %f\n", total_hash_Q6_txn_cnt / (total_hash_run_time / BILLION) * g_thread_cnt);
	printf("ThroughputBTree %f\n", total_btree_Q6_txn_cnt / (total_btree_run_time / BILLION) * g_thread_cnt);
	printf("ThroughputBWTree %f\n", total_bwtree_Q6_txn_cnt / (total_bwtree_run_time / BILLION) * g_thread_cnt);
	printf("ThroughputART %f\n", total_art_Q6_txn_cnt / (total_art_run_time / BILLION) * g_thread_cnt);
	printf("ThroughputCUBIT %f\n", total_cubit_Q6_txn_cnt / (total_cubit_run_time / BILLION) * g_thread_cnt);
	cout << "THREAD_CNT " << g_thread_cnt << endl;
	printf("run time (s): total %f, scan %f, hash %f, btree %f, bwtree %f, art %f, cubit %f\n",  (total_scan_run_time + total_hash_run_time + total_btree_run_time + total_cubit_run_time) / BILLION,total_scan_run_time / BILLION, total_hash_run_time / BILLION, total_btree_run_time / BILLION, total_bwtree_run_time / BILLION, total_art_run_time / BILLION, total_cubit_run_time / BILLION);
	printf("txn count: total %ld, scan %ld, hash %ld, btree %ld, bwtree %ld, art %ld, cubit %ld\n", total_scan_Q6_txn_cnt + total_hash_Q6_txn_cnt + total_btree_Q6_txn_cnt + total_cubit_Q6_txn_cnt,total_scan_Q6_txn_cnt, total_hash_Q6_txn_cnt, total_btree_Q6_txn_cnt, total_bwtree_Q6_txn_cnt, total_art_Q6_txn_cnt, total_cubit_Q6_txn_cnt);
	// printf("total_Q6 time %f\n", (total_scan_run_time + total_hash_run_time + total_btree_run_time + total_cubit_run_time) / BILLION);
	printf("SF = %.4f\n", (double)g_num_orders/1500000);
	if (g_prt_lat_distr)
		print_lat_distr();
}

void Stats::print_lat_distr() {
	FILE * outf;
	if (output_file != NULL) {
		outf = fopen(output_file, "a");
		for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) {
			fprintf(outf, "[all_debug1 thd=%d] ", tid);
			for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
				fprintf(outf, "%ld,", _stats[tid]->all_debug1[tnum]);
			fprintf(outf, "\n[all_debug2 thd=%d] ", tid);
			for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
				fprintf(outf, "%ld,", _stats[tid]->all_debug2[tnum]);
			fprintf(outf, "\n");
		}
		fclose(outf);
	} 
}

#include <mutex>

#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "tpch.h"
#include "test.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"

#include "nicolas/util.h"

void * f(void *);

thread_t ** m_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);
int verify_bitmap(workload * m_wl);

int main(int argc, char* argv[])
{
	parser(argc, argv);
	
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
	stats.init();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
	glob_manager->init();
	if (g_cc_alg == DL_DETECT) 
		dl_detector.init();
	printf("mem_allocator initialized!\n");

	workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl; break;
		case TPCC :
			m_wl = new tpcc_wl; break;
		case TEST :
			m_wl = new TestWorkload; 
			((TestWorkload *)m_wl)->tick();
			break;
		case TPCH :
			m_wl = new tpch_wl; break;
		default:
			assert(false);	
	}
	
	if (Mode == NULL || (Mode && strcmp(Mode, "cache") == 0)) {
		m_wl->init();
		printf("workload initialized!\n");
	}

	if (Mode && strcmp(Mode, "build") == 0) {
		// ((tpch_wl *)m_wl)->build();
		dynamic_cast<tpch_wl *>(m_wl)->build();
		return 0;
	}
	
	uint64_t thd_cnt = g_thread_cnt;
	pthread_t p_thds[thd_cnt - 1];
	m_thds = new thread_t * [thd_cnt];
	for (uint32_t i = 0; i < thd_cnt; i++)
		m_thds[i] = (thread_t *) _mm_malloc(sizeof(thread_t), 64);
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	query_queue = (Query_queue *) _mm_malloc(sizeof(Query_queue), 64);
	if (WORKLOAD != TEST)
		query_queue->init(m_wl);
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
	printf("query_queue initialized!\n");
#if CC_ALG == HSTORE
	part_lock_man.init();
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif

	for (uint32_t i = 0; i < thd_cnt; i++) 
		m_thds[i]->init(i, m_wl);

	if (WARMUP > 0){
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		f((void *)(thd_cnt - 1));
		for (uint32_t i = 0; i < thd_cnt - 1; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, g_thread_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );

	// Intialize the background merge threads for NBUB.
	#define WORKERS_PER_MERGE_TH (4)
    int n_merge_ths;
    std::thread *merge_ths;
	BaseTable *bitmap = NULL;
	Table_config *config = NULL;
	
#if (WORKLOAD == TPCC && TPCC_EVA_CUBIT == true)
	bitmap = dynamic_cast<tpcc_wl *>(m_wl)->bitmap_c_w_id;
	config = bitmap->config;

	if ((config->approach == "nbub-lf") || (config->approach == "nbub-lk")) 
	{
		merge_ths = new thread[config->n_workers / WORKERS_PER_MERGE_TH + 1];

		__atomic_store_n(&run_merge_func, true, MM_CST);

		n_merge_ths = config->n_workers / WORKERS_PER_MERGE_TH;
		for (int i = 0; i < n_merge_ths; i++) {
			int begin = i * WORKERS_PER_MERGE_TH;
			int range = WORKERS_PER_MERGE_TH;
			cout << "[CUBIT]: Range for merge thread " << i << " : [" << begin << " - " << begin+range << ")" << endl;
			merge_ths[i] = std::thread(merge_func, bitmap, begin, range, config);
		}
		if ( config->n_workers % WORKERS_PER_MERGE_TH != 0) {
			int begin = n_merge_ths * WORKERS_PER_MERGE_TH;
			int range = (config->n_workers % WORKERS_PER_MERGE_TH);
			cout << "[CUBIT]: Range for merge thread " << n_merge_ths << " : [" << begin << " - " << begin+range << ")" << endl;
			merge_ths[n_merge_ths] = std::thread(merge_func, bitmap, begin, range, config);
			n_merge_ths ++;
		}
		cout << "[CUBIT]: Creating " << n_merge_ths << " merging threads" << endl;
	}
#endif

	// spawn and run txns again.
	int64_t starttime = get_server_clock();
	for (uint32_t i = 0; i < thd_cnt - 1; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], NULL, f, (void *)vid);
	}
	f((void *)(thd_cnt - 1));

	if (WORKLOAD == TPCC && TPCC_EVA_CUBIT == true)
		assert(!verify_bitmap(m_wl));

	for (uint32_t i = 0; i < thd_cnt - 1; i++) 
		pthread_join(p_thds[i], NULL);
	int64_t endtime = get_server_clock();

#if (WORKLOAD == TPCC && TPCC_EVA_CUBIT == true)
	if ((config->approach == "nbub-lf") || (config->approach == "nbub-lk")) 
	{
		__atomic_store_n(&run_merge_func, false, MM_CST);

		for (int i = 0; i < n_merge_ths; i++) {
			merge_ths[i].join();
		}
		delete[] merge_ths;
	}
#endif
	
	if (WORKLOAD != TEST) {
		printf("PASS! SimTime = %ld\n", endtime - starttime);
		if (STATS_ENABLE)
			stats.print();
	} else {
		((TestWorkload *)m_wl)->summarize();
	}
	return 0;
}

void * f(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid]->run();
	return NULL;
}

int verify_bitmap(workload * m_wl) 
{
	rcu_register_thread();

	tpcc_wl *wl = dynamic_cast<tpcc_wl *>(m_wl);

	if (wl->bitmap_c_w_id->config->approach == "naive") {
		int ROW_ID = g_cust_per_dist;
		naive::Table *bitmap = dynamic_cast<naive::Table*>(wl->bitmap_c_w_id);
		assert(bitmap->get_value(ROW_ID-1) == bitmap->get_value(0));
		int old_val = bitmap->get_value(ROW_ID);
		int to_val = old_val + 1;

		assert(bitmap->bitmaps[old_val]->getBit(ROW_ID, bitmap->config) == 1);
		bitmap->update(0, ROW_ID, to_val);
		assert(bitmap->bitmaps[old_val]->getBit(ROW_ID, bitmap->config) == 0);
		assert(bitmap->bitmaps[to_val]->getBit(ROW_ID, bitmap->config) == 1);
	} 
	else if (wl->bitmap_c_w_id->config->approach == "nbub-lk") {
		RUB last_rub = RUB{0, TYPE_INV, {}};
		int ROW_ID = 6;
		nbub::Nbub *bitmap = dynamic_cast<nbub::Nbub*>(wl->bitmap_c_w_id);
		assert(bitmap->get_value_rcu(ROW_ID-1, bitmap->g_timestamp, last_rub) == 
					bitmap->get_value_rcu(0, bitmap->g_timestamp, last_rub));
		int old_val = bitmap->get_value_rcu(ROW_ID, bitmap->g_timestamp, last_rub);
		int to_val = old_val + 1;

		assert(bitmap->bitmaps[old_val]->btv->getBit(ROW_ID, bitmap->config) == 1);
		bitmap->update(0, ROW_ID, to_val);
		assert(bitmap->bitmaps[old_val]->btv->getBit(ROW_ID, bitmap->config) == 1);

		assert(bitmap->get_value_rcu(/*rowid*/ ROW_ID, bitmap->g_timestamp-1, last_rub) == old_val);
		assert(bitmap->get_value_rcu(/*rowid*/ ROW_ID, bitmap->g_timestamp, last_rub) == to_val);

		// FIXME: after enable merge()
		// MERGE_THRESH = 1;  // Make sure MERGE_THRESH = 1;
		bitmap->evaluate(0, old_val);
		bitmap->evaluate(0, to_val);
		this_thread::sleep_for(1ms);
		assert(bitmap->bitmaps[old_val]->btv->getBit(ROW_ID, bitmap->config) == 0);
		assert(bitmap->bitmaps[to_val]->btv->getBit(ROW_ID, bitmap->config) == 0);
		this_thread::sleep_for(1ms);
		bitmap->evaluate(0, old_val);
		bitmap->evaluate(0, to_val); 
		this_thread::sleep_for(1ms);
		assert(bitmap->bitmaps[old_val]->btv->getBit(ROW_ID, bitmap->config) == 0);
		assert(bitmap->bitmaps[to_val]->btv->getBit(ROW_ID, bitmap->config) == 1);

		cout << "[CUBIT]: verify_bitmap by moving row " << ROW_ID
		<< " from " << old_val << " to " << to_val << endl;
	}

	rcu_unregister_thread();

	return 0;		
}
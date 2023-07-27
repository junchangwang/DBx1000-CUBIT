#include "global.h"
#include "mem_alloc.h"
#include "stats.h"
#include "dl_detect.h"
#include "manager.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"

mem_alloc mem_allocator;
Stats stats;
DL_detect dl_detector;
Manager * glob_manager;
Query_queue * query_queue;
Plock part_lock_man;
OptCC occ_man;
#if CC_ALG == VLL
VLLMan vll_man;
#endif 

bool volatile warmup_finish = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;
#ifndef NOGRAPHITE
carbon_barrier_t enable_barrier;
#endif

ts_t g_abort_penalty = ABORT_PENALTY;
bool g_central_man = CENTRAL_MAN;
UInt32 g_ts_alloc = TS_ALLOC;
bool g_key_order = KEY_ORDER;
bool g_no_dl = NO_DL;
ts_t g_timeout = TIMEOUT;
ts_t g_dl_loop_detect = DL_LOOP_DETECT;
bool g_ts_batch_alloc = TS_BATCH_ALLOC;
UInt32 g_ts_batch_num = TS_BATCH_NUM;

bool g_part_alloc = PART_ALLOC;
bool g_mem_pad = MEM_PAD;
UInt32 g_cc_alg = CC_ALG;
ts_t g_query_intvl = QUERY_INTVL;
UInt32 g_part_per_txn = PART_PER_TXN;
double g_perc_multi_part = PERC_MULTI_PART;
double g_read_perc = READ_PERC;
double g_write_perc = WRITE_PERC;
double g_zipf_theta = ZIPF_THETA;
bool g_prt_lat_distr = PRT_LAT_DISTR;
UInt32 g_part_cnt = PART_CNT;
UInt32 g_virtual_part_cnt = VIRTUAL_PART_CNT;
UInt32 g_thread_cnt = THREAD_CNT;
UInt64 g_synth_table_size = SYNTH_TABLE_SIZE;
UInt32 g_req_per_query = REQ_PER_QUERY;
UInt32 g_field_per_tuple = FIELD_PER_TUPLE;
UInt32 g_init_parallelism = INIT_PARALLELISM;

UInt32 g_num_wh = NUM_WH;
double g_perc_payment = PERC_PAYMENT;
bool g_wh_update = WH_UPDATE;
char * output_file = NULL;
char * Mode = NULL;
bool perf_enabled = false;

// TPCH
uint64_t SF = (uint64_t)(1);
uint64_t g_num_orders = (uint64_t)1500000 * SF;  // FIXME! 1,500,000 * SF
uint64_t g_max_lineitems = g_num_orders * 6;
uint64_t g_nor_in_lineitems = 0UL;
uint64_t STARTDATE = (uint64_t)92001; // 1992.01.01
uint64_t ENDDATE = (uint64_t)98365; // 1998.12.31
uint64_t CURRENTDATE = (uint64_t)95168; //1995.06.17
uint64_t curr_SF = g_num_orders;
map<string, string> g_params;
// vector<vector<string>> output(g_thread_cnt, vector<string>()); // g_thread_cnt is not initialized yet
vector<vector<string>> output_info(128, vector<string>());

#if TPCC_SMALL
UInt32 g_max_items = 15000;
UInt32 g_cust_per_dist = 3000;
#else 
UInt32 g_max_items = 100000;
UInt32 g_cust_per_dist = 3000;
#endif

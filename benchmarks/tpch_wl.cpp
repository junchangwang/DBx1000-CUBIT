#include "global.h"
#include "helper.h"
#include "tpc_helper.h"
#include "tpch.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_bwtree.h"
#include "index_art.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "tpch_const.h"

RC tpch_wl::init() 
{
#if TPCH_EVA_CUBIT
	init_bitmap();
#endif
	workload::init();
	string path = "./benchmarks/";
	path += "TPCH_schema.txt";
	cout << "reading TPCH schema file: " << path << endl;
	init_schema( path.c_str() );
	cout << "TPCH schema initialized" << endl;
	t_lineitem->init_row_buffer(g_max_lineitems);
	init_table();
	next_tid = 0;
	return RCOK; 
}

RC tpch_wl::build()
{	
	// Check whether the index has been built before.
    ifstream doneFlag;
	string path = "bm_" + to_string(curr_SF) + "_done";
    doneFlag.open(path);
    if (doneFlag.good()) { 
        cout << "WARNING: The index for " + path + " has been built before. Skip building." << endl;
        doneFlag.close();
        return RCOK;
    }

	init();

        int ret;
	// bitmap_shipdate
	nbub::Nbub *bitmap = dynamic_cast<nbub::Nbub *>(bitmap_shipdate);
	for (uint64_t i = 0; i < bitmap_shipdate->config->g_cardinality; ++i) {
		string temp = "bm_";
		temp.append(to_string(curr_SF));
		temp.append("_shipdate/");
		string cmd = "mkdir -p ";
		cmd.append(temp);
		ret = system(cmd.c_str());
                sleep(1);
		temp.append(to_string(i));
		temp.append(".bm");
		bitmap->bitmaps[i]->btv->write(temp.c_str());

		ibis::bitvector * test_btv = new ibis::bitvector();
		test_btv->read(temp.c_str());
		test_btv->adjustSize(0, bitmap->g_number_of_rows);
		assert(*(bitmap->bitmaps[i]->btv) == (*test_btv));
	}


	bitmap = dynamic_cast<nbub::Nbub *>(bitmap_discount);
	for (uint64_t i = 0; i < bitmap_discount->config->g_cardinality; ++i) {
		string temp = "bm_";
		temp.append(to_string(curr_SF));
		temp.append("_discount/");
		string cmd = "mkdir -p ";
		cmd.append(temp);
		ret = system(cmd.c_str());
                sleep(1);
		temp.append(to_string(i));
		temp.append(".bm");
		bitmap->bitmaps[i]->btv->write(temp.c_str());

		ibis::bitvector * test_btv = new ibis::bitvector();
		test_btv->read(temp.c_str());
		test_btv->adjustSize(0, bitmap->g_number_of_rows);
		assert(*(bitmap->bitmaps[i]->btv) == (*test_btv));
	}
	
	bitmap = dynamic_cast<nbub::Nbub *>(bitmap_quantity);
	for (uint64_t i = 0; i < bitmap_quantity->config->g_cardinality; ++i) {
		string temp = "bm_";
		temp.append(to_string(curr_SF));
		temp.append("_quantity/");
		string cmd = "mkdir -p ";
		cmd.append(temp);
		ret = system(cmd.c_str());
                sleep(1);
		temp.append(to_string(i));
		temp.append(".bm");
		bitmap->bitmaps[i]->btv->write(temp.c_str());

		ibis::bitvector * test_btv = new ibis::bitvector();
		test_btv->read(temp.c_str());
		test_btv->adjustSize(0, bitmap->g_number_of_rows);
		assert(*(bitmap->bitmaps[i]->btv) == (*test_btv));
	}
	// create done file
    fstream done;   
    done.open(path, ios::out);
    if (done.is_open()) {
        done << bitmap->g_number_of_rows;
        cout << "Succeeded in building bitmap files and " << path << endl;
        done.close(); 
    }
    else {
        cout << "Failed in building bitmap files and " << path << endl;
    } 
	return RCOK; 	
}

RC tpch_wl::init_schema(const char * schema_file) {
	workload::init_schema(schema_file);
	t_lineitem = tables["LINEITEM"];
    t_orders = tables["ORDERS"];

	i_lineitem = indexes["LINEITEM_IDX"];
	i_orders = indexes["ORDERS_IDX"];

	// Explicitly build i_Q6_hashtable and i_Q6_btree and i_Q6_bwtree
	string tname("LINEITEM");
	int part_cnt = (CENTRAL_INDEX)? 1 : g_part_cnt;

	i_Q6_hashtable = (IndexHash *) _mm_malloc(sizeof(IndexHash), 64);
	new(i_Q6_hashtable) IndexHash();
	#if WORKLOAD == YCSB
			i_Q6_hashtable->init(part_cnt, tables[tname], g_synth_table_size * 2);
	#elif WORKLOAD == TPCC
			assert(tables[tname] != NULL);
			i_Q6_hashtable->init(part_cnt, tables[tname], 1024* 64 * part_cnt);
	#elif WORKLOAD == TPCH
			assert(tables[tname] != NULL);
			i_Q6_hashtable->init(part_cnt, tables[tname], 1024* 64 * part_cnt);
	#endif

	i_Q6_btree = (index_btree *) _mm_malloc(sizeof(index_btree), 64);
	new(i_Q6_btree) index_btree();
	i_Q6_btree->init(part_cnt, tables[tname]);

    i_Q6_bwtree = (index_bwtree *) _mm_malloc(sizeof(index_bwtree), 64);
    new(i_Q6_bwtree) index_bwtree();
    i_Q6_bwtree->init(part_cnt, tables[tname]);

    i_Q6_art = (index_art *) _mm_malloc(sizeof(index_art), 64);
    new(i_Q6_art) index_art();
    i_Q6_art->init(part_cnt, tables[tname]);

	return RCOK;
}

RC tpch_wl::init_table() {
	tpc_buffer = new drand48_data * [1];
	tpc_buffer[0] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
	int tid = ATOM_FETCH_ADD(next_tid, 1);
	assert(tid == 0);
	srand48_r(1, tpc_buffer[tid]);

	init_tab_orderAndLineitem();

	printf("TPCH Data Initialization Complete!\n");

	// print Memory concumption
	printMemory();
	return RCOK;
}
RC tpch_wl::printMemory() {
	// each bitmap Menory
    uint64_t bitmap = 0, updateable_bitmap = 0, fence_pointers = 0;

	auto Bitmap = dynamic_cast<nbub::Nbub *>(bitmap_discount);
    for (int i = 0; i < Bitmap->config->g_cardinality; ++i) {
		Bitmap->bitmaps[i]->btv->compress();
        bitmap += Bitmap->bitmaps[i]->btv->getSerialSize();
        fence_pointers += Bitmap->bitmaps[i]->btv->index.size() * sizeof(int) * 2;
    }

	Bitmap = dynamic_cast<nbub::Nbub *>(bitmap_quantity);
    for (int i = 0; i < Bitmap->config->g_cardinality; ++i) {
        Bitmap->bitmaps[i]->btv->compress();
        bitmap += Bitmap->bitmaps[i]->btv->getSerialSize();
        fence_pointers += Bitmap->bitmaps[i]->btv->index.size() * sizeof(int) * 2;
    }

	Bitmap = dynamic_cast<nbub::Nbub *>(bitmap_shipdate);
    for (int i = 0; i < Bitmap->config->g_cardinality; ++i) {
        Bitmap->bitmaps[i]->btv->compress();
        bitmap += Bitmap->bitmaps[i]->btv->getSerialSize();
        fence_pointers += Bitmap->bitmaps[i]->btv->index.size() * sizeof(int) * 2;
    }

	cout << "*************************Print Memory concumption: *******" << endl;	

	cout << "HashMemory (MB): " <<  i_Q6_hashtable->index_size()/1000000 << endl;
	cout << "BtreeMemory (MB): " << i_Q6_btree->index_size()/1000000 << endl;
    cout << "BwtreeMemory (MB): " << i_Q6_bwtree->index_size()/1000000 << endl;
    cout << "ARTMemory (MB): " << i_Q6_art->index_size()/1000000 << endl;
	cout << "CUBITMemory (MB): " << bitmap/1000000 << endl;

	cout << "*************************Print Memory concumption end *******" << endl;	
	return RCOK;
}

RC tpch_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
	txn_manager = (tpch_txn_man *) _mm_malloc( sizeof(tpch_txn_man), 64);
	new(txn_manager) tpch_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}

void tpch_wl::init_tab_orderAndLineitem() {
	cout << "initializing ORDER and LINEITEM table" << endl;

	auto start_g = std::chrono::high_resolution_clock::now();
	long  long  i_order_time = (long  long)0;
	long  long  i_lineitem_time = (long  long)0;
	long  long  i_Q6_hashtable_time = (long  long)0;
	long  long  i_Q6_btree_time = (long  long)0;
    long  long  i_Q6_bwtree_time = (long  long)0;
    long  long  i_Q6_art_time = (long  long)0;
	long  long  i_bitmap_time = (long  long)0;
	long  long  orders_allocate = (long  long)0;
	long  long  orders_set_value = (long  long)0;
	long  long  lineitem_allocate = (long  long)0;
	long  long  lineitem_set_value = (long  long)0;
	for (uint64_t i = 1; i <= g_num_orders; ++i) {
		row_t * row;
		uint64_t row_id;

		auto ts_1 = std::chrono::high_resolution_clock::now();
		t_orders->get_new_row(row, 0, row_id);
		auto ts_2 = std::chrono::high_resolution_clock::now();
		orders_allocate += std::chrono::duration_cast<std::chrono::microseconds>(ts_2 - ts_1).count();

		//Primary key
		row->set_primary_key(i);
		row->set_value(O_ORDERKEY, i);

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
		row->set_value(O_TOTALPRICE, (double)12345.56); 
		char temp[20];
		MakeAlphaString(10, 19, temp, 0);
		row->set_value(O_ORDERPRIORITY, temp);
		row->set_value(O_CLERK, temp);
		row->set_value(O_SHIPPRIORITY, (uint64_t)654321);
		row->set_value(O_COMMENT, temp);

		auto ts_3 = std::chrono::high_resolution_clock::now();
		orders_set_value += std::chrono::duration_cast<std::chrono::microseconds>(ts_3 - ts_2).count();

		auto start = std::chrono::high_resolution_clock::now();
		index_insert(i_orders, i, row, 0);
		auto end = std::chrono::high_resolution_clock::now();
		i_order_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
		// **********************Lineitems*****************************************

		uint64_t lines = URand(1, 7, 0);
		// uint64_t lines = 1;
		g_nor_in_lineitems += lines;

        i_Q6_bwtree->UpdateThreadLocal(g_thread_cnt);
        i_Q6_bwtree->AssignGCID(0);
		for (uint64_t lcnt = 1; lcnt <= lines; lcnt++) {
			row_t * row2;
			uint64_t row_id2 = 0;

			auto ts_4 = std::chrono::high_resolution_clock::now();
			// t_lineitem->get_new_row(row2, 0, row_id2);
			t_lineitem->get_new_row_seq(row2, 0, row_id2);
			auto ts_5 = std::chrono::high_resolution_clock::now();
			lineitem_allocate += std::chrono::duration_cast<std::chrono::microseconds>(ts_5 - ts_4).count();

			// Populate data
			// Primary key
			row2->set_primary_key(tpch_lineitemKey(i, lcnt));
			row2->set_value(L_ORDERKEY, i);
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
			
			// debug
			// cout << endl << "-----------pupulating data debug " << endl;
			// cout << "O_ORDERKEY " << i << endl;	
			// cout << "O_ORDERDATE " << orderdate << endl ;	
			// cout << "-----------lineitem--- " << endl;
			// cout << "L_QUANTITY " << quantity<< endl;	
			// cout << "L_PARTKEY " << partkey<< endl;	
			// cout << "L_EXTENDEDPRICE " << (double)(quantity * p_retailprice) << endl;	
			// cout << "L_DISCOUNT " << ((double)discount) / 100 << endl;	
			// cout << "L_SHIPDATE " << shipdate << endl;	

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

			auto ts_6 = std::chrono::high_resolution_clock::now();
			lineitem_set_value += std::chrono::duration_cast<std::chrono::microseconds>(ts_6 - ts_5).count();
			

			//Index 
			uint64_t key = tpch_lineitemKey(i, lcnt);
			start = std::chrono::high_resolution_clock::now();
			index_insert(i_lineitem, key, row2, 0);
			end = std::chrono::high_resolution_clock::now();
			i_lineitem_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

			// Q6 index
			uint64_t Q6_key = tpch_lineitemKey_index(shipdate, discount, (uint64_t)quantity);
			// cout << "Q6_insert_key = " << Q6_key << endl; 
			start = std::chrono::high_resolution_clock::now();
			index_insert_with_primary_key((INDEX *)i_Q6_hashtable, Q6_key, key, row2, 0);
			end = std::chrono::high_resolution_clock::now();
			i_Q6_hashtable_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

			start = std::chrono::high_resolution_clock::now();
			index_insert((INDEX *)i_Q6_btree, Q6_key, row2, 0);
			end = std::chrono::high_resolution_clock::now();
			i_Q6_btree_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            start = std::chrono::high_resolution_clock::now();
            index_insert((INDEX *)i_Q6_bwtree, Q6_key, row2, 0);
            end = std::chrono::high_resolution_clock::now();
            i_Q6_bwtree_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            start = std::chrono::high_resolution_clock::now();
            index_insert_with_primary_key((INDEX *)i_Q6_art, Q6_key, key, row2, 0);
            end = std::chrono::high_resolution_clock::now();
            i_Q6_art_time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

			auto ts_7 = std::chrono::high_resolution_clock::now();

			// if (Mode != "cache")
			if (Mode == NULL || (Mode && strcmp(Mode, "build") == 0))
			{
				if (bitmap_shipdate->config->approach == "naive" ) {
					bitmap_shipdate->append(0, row_id2);
				}
				else if (bitmap_shipdate->config->approach == "nbub-lk") {
					nbub::Nbub *bitmap = dynamic_cast<nbub::Nbub *>(bitmap_shipdate);
					bitmap->__init_append(0, row_id2, (shipdate/1000-92));

					bitmap = dynamic_cast<nbub::Nbub *>(bitmap_discount);
					bitmap->__init_append(0, row_id2, discount);

					bitmap = dynamic_cast<nbub::Nbub *>(bitmap_quantity);
					// bitmap->__init_append(0, row_id2, quantity-1);
					bitmap->__init_append(0, row_id2, bitmap_quantity_bin(quantity));
				}
			}
			auto ts_8 = std::chrono::high_resolution_clock::now();
			i_bitmap_time += std::chrono::duration_cast<std::chrono::microseconds>(ts_8 - ts_7).count();
		}
	}
    i_Q6_bwtree->UnregisterThread(0);

	auto end_g = std::chrono::high_resolution_clock::now();
	cout << "*************************INDEX build time: *******" << endl
		<< "Overall time (microseconds):" << std::chrono::duration_cast<std::chrono::microseconds>(end_g - start_g).count() << endl
		<< "i_order_time:" << i_order_time << ", " 
		<< "i_lineitem_time:" << i_lineitem_time << ", "
		<< "i_Q6_hashtable_time:" << i_Q6_hashtable_time << ", "
		<< "i_Q6_btree_time:" << i_Q6_btree_time << ", "
		<< "i_Q6_bwtree_time:" << i_Q6_bwtree_time << ", "
		<< "i_Q6_art_time:" << i_Q6_art_time <<", "
		<< "i_bitmap_time:" << i_bitmap_time << endl
	    << "orders_allocate:" << orders_allocate << ", "
		<< "orders_set_value:" << orders_set_value << ", "
		<< "lineitem_allocate:" << lineitem_allocate << ", "
		<< "lineitem_set_value:" << lineitem_set_value << endl
		<< "sum = " << i_order_time + i_Q6_hashtable_time + i_lineitem_time + i_Q6_btree_time + i_Q6_bwtree_time + i_Q6_art_time + i_bitmap_time + orders_allocate 
						+ orders_set_value + lineitem_allocate + lineitem_set_value << endl
		<<"*************************INDEX build time end*******" << endl;


}

void tpch_wl::init_test() {
	cout << "initializing TEST table" << endl;	

	// // ###################1
	// row_t * row;
	// uint64_t row_id;
	// t_orders->get_new_row(row, 0, row_id);		
	// //Primary key
	// row->set_primary_key(1);
	// row->set_value(O_ORDERKEY, 1);
	// index_insert(i_orders, i, row, 0);
	// // ####################2
	// row_t * row;
	// uint64_t row_id;
	// t_orders->get_new_row(row, 0, row_id);		
	// //Primary key
	// row->set_primary_key(2);
	// row->set_value(O_ORDERKEY, 2);
	// index_insert(i_orders, i, row, 0);



		// **********************Lineitems*****************************************
	// ###################################11111111111111
	for (uint64_t i = 1; i <= 10; ++i) {
		row_t * row2;
		uint64_t row_id2;
		// t_lineitem->get_new_row(row2, 0, row_id2);
		t_lineitem->get_new_row_seq(row2, 0, row_id2);
		// Primary key
		// uint64_t i = 1;
		uint64_t lcnt = 1;
		// uint64_t key1 = (uint64_t)(i * 8 + lcnt);
		row2->set_primary_key(i);
		row2->set_value(L_ORDERKEY, i);
		row2->set_value(L_LINENUMBER, lcnt);

		// Related data
		double quantity = (double)23;
		double exprice = (double)100;
		double discount = (double)0.06;
		uint64_t shipdate = (uint64_t) 96221;
		row2->set_value(L_QUANTITY, quantity); 	
		row2->set_value(L_EXTENDEDPRICE, exprice); 
		row2->set_value(L_DISCOUNT, discount);
		row2->set_value(L_SHIPDATE,shipdate );	

		//Index 
		// index_insert(i_lineitem, tpch_lineitemKey(i,lcnt), row2, 0);
		index_insert(i_lineitem, i, row2, 0);
		
		// Q6 index
		// uint64_t Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
		// index_insert(i_Q6_hashtable, Q6_key, row2, 0);
	}
	uint64_t toDeleteKey = (uint64_t)1;
	i_lineitem->index_remove(toDeleteKey);
	row_t * row2;
	uint64_t row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);		
	t_lineitem->get_new_row_seq(row2, 0, row_id2);
	index_insert(i_lineitem, toDeleteKey, row2, 0);
	index_insert(i_lineitem, toDeleteKey, row2, 0);
	index_insert(i_lineitem, toDeleteKey, row2, 0);
	index_insert(i_lineitem, toDeleteKey, row2, 0);
	index_insert(i_lineitem, toDeleteKey, row2, 0);
	index_insert(i_lineitem, toDeleteKey, row2, 0);


	// ###################################222222222222222
	// row_t * row2;
	// uint64_t row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);
	// Primary key
	// i = 1000;
	// lcnt = 1;
	// key1 = (uint64_t)(i * 8 + lcnt);
	// row2->set_primary_key(key1);
	// row2->set_value(L_ORDERKEY, i);
	// row2->set_value(L_LINENUMBER, lcnt);

	// Related data
	// quantity = (double)23;
	// exprice = (double)100;
	// discount = (double)0.06;
	// shipdate = (uint64_t) 96222;
	// row2->set_value(L_QUANTITY, quantity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	//Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
	// index_insert(i_Q6_hashtable, Q6_key, row2, 0);
		
	// // ###################################333333333333333333
	// // row_t * row2;
	// // row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);
	// // Primary key
	// i = 1000;
	// lcnt = 2;
	// key1 = (uint64_t)(i * 8 + lcnt);
	// row2->set_primary_key(key1);
	// row2->set_value(L_ORDERKEY, i);
	// row2->set_value(L_LINENUMBER, lcnt);

	// // Related data
	// quantity = (double)23;
	// exprice = (double)100;
	// discount = (double)0.06;
	// shipdate = (uint64_t) 96223;
	// row2->set_value(L_QUANTITY, quantity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
	// index_insert(i_Q6_hashtable, Q6_key, row2, 0);


	// 	// ###################################4444444444444
	// // row_t * row2;
	// // row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);
	// // Primary key
	// i = 1000;
	// lcnt = 3;
	// key1 = (uint64_t)(i * 8 + lcnt);
	// row2->set_primary_key(key1);
	// row2->set_value(L_ORDERKEY, i);
	// row2->set_value(L_LINENUMBER, lcnt);

	// // Related data
	// quantity = (double)27;
	// exprice = (double)100;
	// discount = (double)0.03;
	// shipdate = (uint64_t) 92132;
	// row2->set_value(L_QUANTITY, quantity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
	// index_insert(i_Q6_hashtable, Q6_key, row2, 0);

	// 	// ###################################55555555555555
	// // row_t * row2;
	// // row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);
	// // Primary key
	// i = 2002;
	// lcnt = 1;
	// key1 = (uint64_t)(i * 8 + lcnt);
	// row2->set_primary_key(key1);
	// row2->set_value(L_ORDERKEY, i);
	// row2->set_value(L_LINENUMBER, lcnt);

	// // Related data
	// quantity = (double)20;
	// exprice = (double)100;
	// discount = (double)0.04;
	// shipdate = (uint64_t) 95222;
	// row2->set_value(L_QUANTITY, quantity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
	// index_insert(i_Q6_hashtable, Q6_key, row2, 0);

	// 	// #################################666666666666666
	// // row_t * row2;
	// // row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);
	// // Primary key
	// i = 3121;
	// lcnt = 1;
	// key1 = (uint64_t)(i * 8 + lcnt);
	// row2->set_primary_key(key1);
	// row2->set_value(L_ORDERKEY, i);
	// row2->set_value(L_LINENUMBER, lcnt);

	// // Related data
	// quantity = (double)10;
	// exprice = (double)100;
	// discount = (double)0.05;
	// shipdate = (uint64_t) 95365;
	// row2->set_value(L_QUANTITY, quantity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
	// index_insert(i_Q6_hashtable, Q6_key, row2, 0);
}


RC tpch_wl::init_bitmap() 
{
	// auto start = std::chrono::high_resolution_clock::now();
	// auto end = std::chrono::high_resolution_clock::now();
	// long  long  time = (long  long)0;	

	uint64_t n_rows = 0UL;
	if (Mode && strcmp(Mode, "cache") == 0) {
		fstream done;
		string path = "bm_" + to_string(curr_SF) + "_done";
		done.open(path, ios::in);
		if (done.is_open()) {
			done >> n_rows;
			cout << "[cache] Number of rows: " << n_rows << " . Fetched from bitmap cached file: " << path << endl;
			done.close();
		}
		else {
			cout << "[cache] Failed to open the specified bitmap cached file: " << path << endl;
			exit(-1);
		}

	}
	db_number_of_rows = n_rows;

/********************* bitmap_shipdate ******************************/
	{
	Table_config *config_shipdate = new Table_config{};
	config_shipdate->n_workers = g_thread_cnt;
	config_shipdate->DATA_PATH = "";
	if (Mode && strcmp(Mode, "cache") == 0) {
		string temp = "bm_";
		temp.append(to_string(curr_SF));
		temp.append("_shipdate");
		config_shipdate->INDEX_PATH = temp;
	} else
		config_shipdate->INDEX_PATH = "";
	config_shipdate->n_rows = n_rows; 
	config_shipdate->g_cardinality = 7; // [92, 98]
	enable_fence_pointer = config_shipdate->enable_fence_pointer = true;
	INDEX_WORDS = 10000;  // Fence length 
	config_shipdate->approach = {"nbub-lk"};
	config_shipdate->nThreads_for_getval = 4;
	config_shipdate->show_memory = true;
	config_shipdate->on_disk = false;
	config_shipdate->showEB = false;
    config_shipdate->decode = false;

	// DBx1000 doesn't use the following parameters;
	// they are used by nicolas.
	config_shipdate->n_queries = MAX_TXN_PER_PART;
	config_shipdate->n_udis = MAX_TXN_PER_PART * 0.1;
	config_shipdate->verbose = false;
	config_shipdate->time_out = 100;
	config_shipdate->autoCommit = false;
	config_shipdate->n_merge_threshold = 16;
	config_shipdate->db_control = true;

	config_shipdate->segmented_btv = false;
	config_shipdate->encoded_word_len = 31;
	config_shipdate->rows_per_seg = 100000;
	config_shipdate->enable_parallel_cnt = false;
	
	// start = std::chrono::high_resolution_clock::now();
	if (config_shipdate->approach == "ub") {
        bitmap_shipdate = new ub::Table(config_shipdate);
    } else if (config_shipdate->approach == "nbub-lk") {
        bitmap_shipdate = new nbub_lk::NbubLK(config_shipdate);
    } else if (config_shipdate->approach == "nbub-lf" || config_shipdate->approach =="nbub") {
        bitmap_shipdate = new nbub_lf::NbubLF(config_shipdate);
    } else if (config_shipdate->approach == "ucb") {
        bitmap_shipdate = new ucb::Table(config_shipdate);
    } else if (config_shipdate->approach == "naive") {
        bitmap_shipdate = new naive::Table(config_shipdate);
    }
    else {
        cerr << "Unknown approach." << endl;
        exit(-1);
    }
	// end = std::chrono::high_resolution_clock::now();
	// time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

	cout << "[CUBIT]: Bitmap bitmap_shipdate initialized successfully. "
			<< "[Cardinality:" << config_shipdate->g_cardinality
			<< "] [Method:" << config_shipdate->approach << "]" << endl;
	}

/********************* bitmap_discount ******************************/
	{
	Table_config *config_discount = new Table_config{};
	config_discount->n_workers = g_thread_cnt;
	config_discount->DATA_PATH = "";
	if (Mode && strcmp(Mode, "cache") == 0) {
		string temp = "bm_";
		temp.append(to_string(curr_SF));
		temp.append("_discount");
		config_discount->INDEX_PATH = temp;
	} else
		config_discount->INDEX_PATH = "";
	config_discount->n_rows = n_rows; 
	config_discount->g_cardinality = 11; // [0, 10]
	enable_fence_pointer = config_discount->enable_fence_pointer = true;
	INDEX_WORDS = 10000;  // Fence length 
	config_discount->approach = {"nbub-lk"};
	config_discount->nThreads_for_getval = 4;
	config_discount->show_memory = true;
	config_discount->on_disk = false;
	config_discount->showEB = false;
    config_discount->decode = false;

	// DBx1000 doesn't use the following parameters;
	// they are used by nicolas.
	config_discount->n_queries = MAX_TXN_PER_PART;
	config_discount->n_udis = MAX_TXN_PER_PART * 0.1;
	config_discount->verbose = false;
	config_discount->time_out = 100;
	config_discount->autoCommit = false;
	config_discount->n_merge_threshold = 16;
	config_discount->db_control = true;

	config_discount->segmented_btv = false;
	config_discount->encoded_word_len = 31;
	config_discount->rows_per_seg = 100000;
	config_discount->enable_parallel_cnt = false;
	
	// start = std::chrono::high_resolution_clock::now();
	if (config_discount->approach == "ub") {
        bitmap_discount = new ub::Table(config_discount);
    } else if (config_discount->approach == "nbub-lk") {
        bitmap_discount = new nbub_lk::NbubLK(config_discount);
    } else if (config_discount->approach == "nbub-lf" || config_discount->approach =="nbub") {
        bitmap_discount = new nbub_lf::NbubLF(config_discount);
    } else if (config_discount->approach == "ucb") {
        bitmap_discount = new ucb::Table(config_discount);
    } else if (config_discount->approach == "naive") {
        bitmap_discount = new naive::Table(config_discount);
    }
    else {
        cerr << "Unknown approach." << endl;
        exit(-1);
    }
	// end = std::chrono::high_resolution_clock::now();
	// time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

	cout << "[CUBIT]: Bitmap bitmap_discount initialized successfully. "
			<< "[Cardinality:" << config_discount->g_cardinality
			<< "] [Method:" << config_discount->approach << "]" << endl;
	}

/********************* bitmap_quantity ******************************/
	{
	Table_config *config_quantity = new Table_config{};
	config_quantity->n_workers = g_thread_cnt;
	config_quantity->DATA_PATH = "";
	if (Mode && strcmp(Mode, "cache") == 0) {
		string temp = "bm_";
		temp.append(to_string(curr_SF));
		temp.append("_quantity");
		config_quantity->INDEX_PATH = temp;
	} else
		config_quantity->INDEX_PATH = "";
	config_quantity->n_rows = n_rows;  
	// config_quantity->g_cardinality = 50; // [0, 49]
	config_quantity->g_cardinality = 3; // [0,23], 24, [25,49]
	enable_fence_pointer = config_quantity->enable_fence_pointer = true;
	INDEX_WORDS = 10000;  // Fence length 
	config_quantity->approach = {"nbub-lk"};
	config_quantity->nThreads_for_getval = 4;
	config_quantity->show_memory = true;
	config_quantity->on_disk = false;
	config_quantity->showEB = false;
    config_quantity->decode = false;

	// DBx1000 doesn't use the following parameters;
	// they are used by nicolas.
	config_quantity->n_queries = MAX_TXN_PER_PART;
	config_quantity->n_udis = MAX_TXN_PER_PART * 0.1;
	config_quantity->verbose = false;
	config_quantity->time_out = 100;
	config_quantity->autoCommit = false;
	config_quantity->n_merge_threshold = 16;
	config_quantity->db_control = true;

	config_quantity->segmented_btv = false;
	config_quantity->encoded_word_len = 31;
	config_quantity->rows_per_seg = 100000;
	config_quantity->enable_parallel_cnt = false;
	
	// start = std::chrono::high_resolution_clock::now();
	if (config_quantity->approach == "ub") {
        bitmap_quantity = new ub::Table(config_quantity);
    } else if (config_quantity->approach == "nbub-lk") {
        bitmap_quantity = new nbub_lk::NbubLK(config_quantity);
    } else if (config_quantity->approach == "nbub-lf" || config_quantity->approach =="nbub") {
        bitmap_quantity = new nbub_lf::NbubLF(config_quantity);
    } else if (config_quantity->approach == "ucb") {
        bitmap_quantity = new ucb::Table(config_quantity);
    } else if (config_quantity->approach == "naive") {
        bitmap_quantity = new naive::Table(config_quantity);
    }
    else {
        cerr << "Unknown approach." << endl;
        exit(-1);
    }
	// end = std::chrono::high_resolution_clock::now();
	// time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();	

	cout << "[CUBIT]: Bitmap bitmap_quantity initialized successfully. "
			<< "[Cardinality:" << config_quantity->g_cardinality
			<< "] [Method:" << config_quantity->approach << "]" << endl;
	}

	// cout << "INDEX bitmap build time:" << time << endl;

	return RCOK;
}
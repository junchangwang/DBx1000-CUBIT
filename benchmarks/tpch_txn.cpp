#include "tpch.h"
#include "tpch_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpch_const.h"

void tpch_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (tpch_wl *) h_wl;
}

RC tpch_txn_man::run_txn(base_query * query) {
	tpch_query * m_query = (tpch_query *) query;
	switch (m_query->type) {
		case TPCH_Q6 :
			return run_Q6(m_query); break;
		case TPCH_Q6_index :
			return run_Q6_index(m_query); break;
		case TPCH_RF1 :
			return run_RF1(); break;
		case TPCH_RF2 :
			return run_RF2(); break;			
		default:
			assert(false);
	}
}

RC tpch_txn_man::run_Q6(tpch_query * query) {
	RC rc = RCOK;
	itemid_t * item;
	INDEX * index = _wl->i_lineitem;
	
	// txn
	double revenue = 0;
	uint64_t max_number = (uint64_t) (tpch_lineitemKey(g_max_lineitem, (uint64_t)9));
	for (uint64_t i = 1; i <= max_number; ++i) {
		// cout << "i = " << i << endl;
		if ( !index->index_exist(i, 0) ){
			// cout << i << " NOT EXIST!" << endl;
			continue;
		}
		item = index_read(index, i, 0);
		assert(item != NULL);
		row_t * r_lt = ((row_t *)item->location);
		row_t * r_lt_local = get_row(r_lt, RD);
		if (r_lt_local == NULL) {
			return finish(Abort);
		}
		// begin
		uint64_t l_shipdate;
		r_lt_local->get_value(L_SHIPDATE, l_shipdate);
		double l_discount;
		r_lt_local->get_value(L_DISCOUNT, l_discount);
		double l_quantity;
		r_lt_local->get_value(L_QUANTITY, l_quantity);
		
		// debug
		// cout << "A query_date " << query->date << " " << l_shipdate << endl;
		// cout << "A query_discount " << query->discount << " " << l_discount << endl;
		// cout << "A query_quantity " << query->quantity << " " << l_quantity << endl << endl;		

		if (l_shipdate >= query->date 
			&& l_shipdate < (uint64_t)(query->date + 1000) 
			&& (uint64_t)(l_discount*100) >= (uint64_t)((uint64_t)(query->discount*100) - 1) 
			&& (uint64_t)(l_discount*100) <= (uint64_t)((uint64_t)(query->discount*100) + 1) 
			&& (uint64_t)l_quantity < (uint64_t)query->quantity){
				
				// debug
				// cout << "B query_date " << query->date << " " << l_shipdate << endl;
				// cout << "B query_discount " << query->discount << " " << l_discount << endl;
				// cout << "B query_quantity " << query->quantity << " " << l_quantity << endl << endl;
				
				// cout << "address = " << &r_lt_local->data << endl;

				double l_extendedprice;
				r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
				revenue += l_extendedprice * l_discount;
			}
	}
	cout << "********Q6            revenue is *********" << revenue << endl; 

	assert( rc == RCOK );
	return finish(rc);
}

RC tpch_txn_man::run_Q6_index(tpch_query * query) {
	RC rc = RCOK;
	itemid_t * item;
	uint64_t key;
	double revenue = 0;
	INDEX * index = _wl->i_Q6_index;
	uint64_t date = query->date;	// + 1 year
	uint64_t discount = (uint64_t)(query->discount * 100); // +1 -1
	double quantity = query->quantity;

	for (uint64_t i = date; i <= (uint64_t)(date + 364); i++) {
		for (uint64_t j = (uint64_t)(discount - 1); j <= (uint64_t)(discount + 1); j++) {
			for (uint64_t k = (uint64_t)((uint64_t)quantity - 1); k > (uint64_t)0; k--){
	
				// key = (uint64_t)((i * 12 + j) * 26 + k);
				key = tpch_lineitemKey_index(i, j, k);
				// cout << "Q6_txn_key = " << key << endl; 
				if ( !index->index_exist(key, 0) ){
					// cout << i << " NOT EXIST!" << endl;
					continue;
				}	

				// debug
				// cout << "Q6_txn_key = " << key << endl;
				// cout << "index query_date " << query->date << " " << i << endl;
				// cout << "index query_discount " << query->discount << " " << j << endl;
				// cout << "index query_quantity " << query->quantity << " " << k << endl << endl;
				
				item = index_read(index, key, 0);
				itemid_t * local_item = item;
				assert(item != NULL);
				while (local_item != NULL) {
					row_t * r_lt = ((row_t *)local_item->location);
					row_t * r_lt_local = get_row(r_lt, RD);
					if (r_lt_local == NULL) {
						return finish(Abort);
					}
					// cout << "address = " << &r_lt_local->data << endl;
					double l_extendedprice;
					r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
					revenue += l_extendedprice * ((double)j / 100);

					local_item = local_item->next;
				}

			}
		}
	}

	cout << "********Q6 with index revenue is *********" << revenue << endl << endl;
	assert( rc == RCOK );
	return finish(rc);
}

RC tpch_txn_man::run_RF1() {
	for (uint64_t i = (uint64_t)(g_max_lineitem + 1); i < (uint64_t)(SF * 1500 + g_max_lineitem + 1); ++i) {
		row_t * row;
		uint64_t row_id;
		_wl->t_orders->get_new_row(row, 0, row_id);		
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
		row->set_value(O_TOTALPRICE, (double)12345.56); // May be used
		char temp[20];
		MakeAlphaString(10, 19, temp, 0);
		row->set_value(O_ORDERPRIORITY, temp);
		row->set_value(O_CLERK, temp);
		row->set_value(O_SHIPPRIORITY, (uint64_t)654321);
		row->set_value(O_COMMENT, temp);

		// index_insert(_wl->i_orders, i, row, 0);
		_wl->index_insert(_wl->i_orders, i, row, 0);




		// **********************Lineitems*****************************************

		uint64_t lines = URand(1, 7, 0);
		// uint64_t lines = 1;
		g_total_line_in_lineitems += lines;
		for (uint64_t lcnt = 1; lcnt <= lines; lcnt++) {
			row_t * row2;
			uint64_t row_id2;
			_wl->t_lineitem->get_new_row(row2, 0, row_id2);

			// Populate data
			// Primary key
			row2->set_primary_key(tpch_lineitemKey(i, lcnt));
			row2->set_value(L_ORDERKEY, i);
			row2->set_value(L_LINENUMBER, lcnt);

			// Related data
			double quntity = (double)URand(1, 50, 0);
			row2->set_value(L_QUANTITY, quntity); 
			uint64_t partkey = URand(1, SF * 200000, 0);	// Related to SF
			row2->set_value(L_PARTKEY, partkey); 
			uint64_t p_retailprice = (uint64_t)((90000 + ((partkey / 10) % 20001) + 100 *(partkey % 1000)) /100); // Defined in table PART
			row2->set_value(L_EXTENDEDPRICE, (double)(quntity * p_retailprice)); // 
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
			// cout << "L_QUANTITY " << quntity<< endl;	
			// cout << "L_PARTKEY " << partkey<< endl;	
			// cout << "L_EXTENDEDPRICE " << (double)(quntity * p_retailprice) << endl;	
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

			//Index 
			uint64_t key = tpch_lineitemKey(i, lcnt);
			_wl->index_insert(_wl->i_lineitem, key, row2, 0);


			// Q6 index
			uint64_t Q6_key = tpch_lineitemKey_index(shipdate, discount, (uint64_t)quntity);
			// cout << "Q6_insert_key = " << Q6_key << endl; 
			_wl->index_insert(_wl->i_Q6_index, Q6_key, row2, 0);
		}
	}
	return RCOK;
}

RC tpch_txn_man::run_RF2() {
	for (uint64_t i = 1; i < (uint64_t)(SF * 1500); ++i) {
		uint64_t key = URand(1, g_max_lineitem, 0);
		_wl->i_orders->index_remove(key);
		for (uint64_t lcnt = (uint64_t)1; lcnt <= (uint64_t)7; ++lcnt){
			_wl->i_lineitem->index_remove(tpch_lineitemKey(key, lcnt));
		}
	}

	return RCOK;

}
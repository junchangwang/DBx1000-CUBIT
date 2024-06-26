#include <cstdio>

#include "global.h"
#include "helper.h"
#include "wl.h"
#include "row.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_bwtree.h"
#include "index_art.h"
#include "catalog.h"
#include "mem_alloc.h"


#include "fastbit/bitvector.h"
#include "nicolas/base_table.h"
#include "nicolas/util.h"
#include "ub/table.h"
#include "ucb/table.h"
#include "naive/table.h"
#include "cubit/table_lf.h"
#include "cubit/table_lk.h"

RC workload::init() {
	sim_done = false;
	return RCOK;
}

RC workload::init_schema(string schema_file) {
    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);	
	string line;
	ifstream fin(schema_file);
    Catalog * schema;
    while (getline(fin, line)) {
		if (line.compare(0, 6, "TABLE=") == 0) {
			string tname;
			tname = &line[6];
			schema = (Catalog *) _mm_malloc(sizeof(Catalog), CL_SIZE);
			getline(fin, line);
			int col_count = 0;
			// Read all fields for this table.
			vector<string> lines;
			while (line.length() > 1) {
				lines.push_back(line);
				getline(fin, line);
			}
			schema->init( (char *)tname.c_str(), lines.size() );
			for (UInt32 i = 0; i < lines.size(); i++) {
				string line = lines[i];
			    size_t pos = 0;
				string token;
				int elem_num = 0;
				int size = 0;
				string type;
				string name;
				while (line.length() != 0) {
					pos = line.find(",");
					if (pos == string::npos)
						pos = line.length();
	    			token = line.substr(0, pos);
			    	line.erase(0, pos + 1);
					switch (elem_num) {
					case 0: size = atoi(token.c_str()); break;
					case 1: type = token; break;
					case 2: name = token; break;
					default: assert(false);
					}
					elem_num ++;
				}
				assert(elem_num == 3);
                schema->add_col((char *)name.c_str(), size, (char *)type.c_str());
				col_count ++;
			}
			table_t * cur_tab = (table_t *) _mm_malloc(sizeof(table_t), CL_SIZE);
			cur_tab->init(schema);
			tables[tname] = cur_tab;
		} else if (!line.compare(0, 6, "INDEX=")) {
			string iname;
			iname = &line[6];
			getline(fin, line);

			vector<string> items;
			string token;
			size_t pos;
			while (line.length() != 0) {
				pos = line.find(",");
				if (pos == string::npos)
					pos = line.length();
				token = line.substr(0, pos);
				items.push_back(token);
				line.erase(0, pos + 1);
			}
			
			string tname(items[0]);
			INDEX * index = (INDEX *) _mm_malloc(sizeof(INDEX), 64);
			new(index) INDEX();
			int part_cnt = (CENTRAL_INDEX)? 1 : g_part_cnt;
			if (tname == "ITEM")
				part_cnt = 1;
#if INDEX_STRUCT == IDX_HASH
	#if WORKLOAD == YCSB
			index->init(part_cnt, tables[tname], g_synth_table_size * 2);
	#elif WORKLOAD == TPCC
			assert(tables[tname] != NULL);
			index->init(part_cnt, tables[tname], stoi( items[1] ) * part_cnt);
	#elif WORKLOAD == CHBench
			assert(tables[tname] != NULL);
			index->init(part_cnt, tables[tname], stoi( items[1] ) * part_cnt);
	#elif WORKLOAD == TPCH
			assert(tables[tname] != NULL);
			index->init(part_cnt, tables[tname], stoi( items[1] ) * part_cnt);
		
	#endif
#else
			index->init(part_cnt, tables[tname]);
#endif
			indexes[iname] = index;
		}
    }
	fin.close();
	return RCOK;
}

void workload::index_insert(string index_name, uint64_t key, row_t * row) {
	assert(false);
	INDEX * index = (INDEX *) indexes[index_name];
	index_insert(index, key, row);
}

void workload::index_insert(INDEX * index, uint64_t key, row_t * row, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), pid );
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;
#if WORKLOAD == CHBench
	unique_lock<shared_mutex> w_lock(index->rw_lock);
#endif
	assert( index->index_insert(key, m_item, pid) == RCOK );
}

void workload::index_insert_orderline(INDEX * index, uint64_t key,  row_t * row, uint64_t item_id, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemid_t * m_item = (itemid_t *) mem_allocator.alloc(sizeof(itemid_t), pid);
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;
	m_item->item_id = item_id;

    assert( index->index_insert(key, m_item, pid) == RCOK );
}

void workload::index_insert_with_primary_key(string index_name, uint64_t key, uint64_t primary_key, row_t * row) {
	assert(false);
	INDEX * index = (INDEX *) indexes[index_name];
	index_insert_with_primary_key(index, key, primary_key, row);
}

void workload::index_insert_with_primary_key(INDEX * index, uint64_t key, uint64_t primary_key, row_t * row, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), pid );
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;
	m_item->primary_key = primary_key;

    assert( index->index_insert(key, m_item, pid) == RCOK );
}

void workload::bitmap_multiply(string read_dir_name, string write_dir_name, int cardinality, int multiple) {
	for(int i = 0; i < cardinality; i++) {
		ibis::bitvector result;
		ibis::bitvector read_btv;
		string s = read_dir_name + "/" + to_string(i) + ".bm";
		read_btv.read(s.c_str());
		for(int j = 0; j < multiple; j++) {
			result += read_btv;
		}
		s = write_dir_name;
		string cmd = "mkdir -p ";
		cmd.append(s);
		int ret = system(cmd.c_str());
        sleep(1);
		s += "/" + to_string(i) + ".bm";
		result.write(s.c_str());
	}
	std::cout << "multiply done" << std::endl;
}

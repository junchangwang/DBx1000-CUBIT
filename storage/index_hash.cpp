#include "global.h"	
#include "index_hash.h"
#include "mem_alloc.h"
#include "table.h"

RC IndexHash::init(uint64_t bucket_cnt, int part_cnt) {
	_bucket_cnt = bucket_cnt;
	_bucket_cnt_per_part = bucket_cnt / part_cnt;
	_buckets = new BucketHeader * [part_cnt];
	for (int i = 0; i < part_cnt; i++) {
		_buckets[i] = (BucketHeader *) _mm_malloc(sizeof(BucketHeader) * _bucket_cnt_per_part, 64);
		for (uint32_t n = 0; n < _bucket_cnt_per_part; n ++)
			_buckets[i][n].init();
	}
	return RCOK;
}

int IndexHash::index_size() 
{
	int size = 0;
	int item_cnt = 0;
	BucketHeader * bucket;
	BucketNode * cur_node;
	itemid_t * item;
	int part_cnt = _bucket_cnt / _bucket_cnt_per_part;
	int bucket_cnt_per_part = _bucket_cnt_per_part;

	for (int i = 0; i < part_cnt; i++) {
		for (int j = 0; j < bucket_cnt_per_part; j++) {
				bucket = &_buckets[i][j];
				cur_node = bucket->first_node;
				size += sizeof(*bucket);
				size += sizeof(*cur_node);
				while (cur_node != NULL) {
					item = cur_node->items;
					while (item != NULL) {
						item_cnt ++;
						size += sizeof(*item);
						item = item->next;
					}
					cur_node = cur_node->next;
					size += sizeof(*cur_node);
				}
		}
	}

	// cout << "Number of items in the Hashtable: " << item_cnt <<
	// 		". Size in bytes: " << size << endl;
	return size;
}

RC 
IndexHash::init(int part_cnt, table_t * table, uint64_t bucket_cnt) {
	init(bucket_cnt, part_cnt);
	this->table = table;
	return RCOK;
}

bool IndexHash::index_exist(idx_key_t key, int part_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	return cur_bkt->exist_item(key);
}

bool IndexHash::index_exist(idx_key_t key) {
	assert(false);
}

void 
IndexHash::get_latch(BucketHeader * bucket) {
	while (!ATOM_CAS(bucket->locked, false, true)) {}
}

void 
IndexHash::release_latch(BucketHeader * bucket) {
	bool ok = ATOM_CAS(bucket->locked, true, false);
	assert(ok);
}

	
RC IndexHash::index_insert(idx_key_t key, itemid_t * item, int part_id) {
	RC rc = RCOK;
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	// 1. get the ex latch
	// get_latch(cur_bkt);
	
	// 2. update the latch list
	cur_bkt->insert_item(key, item, part_id);
	
	// 3. release the latch
	// release_latch(cur_bkt);
	return rc;
}

RC IndexHash::index_read(idx_key_t key, itemid_t * &item, int part_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
//	get_latch(cur_bkt);
	cur_bkt->read_item(key, item, table->get_table_name());
	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;

}

RC IndexHash::index_read(idx_key_t key, itemid_t * &item, 
						int part_id, int thd_id) {
	uint64_t bkt_idx = hash(key);
	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[part_id][bkt_idx];
	RC rc = RCOK;
	// 1. get the sh latch
//	get_latch(cur_bkt);
	cur_bkt->read_item(key, item, table->get_table_name());
	// 3. release the latch
//	release_latch(cur_bkt);
	return rc;
}

RC IndexHash::index_remove(idx_key_t key) {
	uint64_t bkt_idx = hash(key);

	assert(bkt_idx < _bucket_cnt_per_part);
	BucketHeader * cur_bkt = &_buckets[0][bkt_idx];
	bool ret = cur_bkt->exist_item(key);
	if ( !ret ) {
		// cout << "index_remove! Key " << key << " NOT EXIST!" << endl;
		return ERROR;
	} else {
		BucketNode * cur_node = cur_bkt->first_node;
		BucketNode * prev_node = NULL;
		while (cur_node != NULL) {
			if (cur_node->key == key)
				break;
			prev_node = cur_node;
			cur_node = cur_node->next;
		}	
		if (prev_node == NULL) { // head
			cur_bkt->first_node = cur_node->next; 
		} else {
			prev_node->next = cur_node->next;
		}
		delete cur_node; // FIXME
	} 
	return RCOK;
}

/************** BucketHeader Operations ******************/

void BucketHeader::init() {
	node_cnt = 0;
	first_node = NULL;
	locked = false;
}

void BucketHeader::insert_item(idx_key_t key, 
		itemid_t * item, 
		int part_id) 
{
	BucketNode * cur_node = first_node;
	BucketNode * prev_node = NULL;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		prev_node = cur_node;
		cur_node = cur_node->next;
	}
	if (cur_node == NULL) {		
		BucketNode * new_node = (BucketNode *) 
			mem_allocator.alloc(sizeof(BucketNode), part_id );
		new_node->init(key);
		new_node->items = item;
		if (prev_node != NULL) {
			new_node->next = prev_node->next;
			prev_node->next = new_node;
		} else {
			new_node->next = first_node;
			first_node = new_node;
		}
	} else {
#ifdef ORDERED_LEAF_LIST
		if (cur_node->items == NULL) {
			cur_node->items = item;
		} else {
			if (item->primary_key <= cur_node->items->primary_key) {
				item->next = cur_node->items->next;
				cur_node->items->next = item;
			} else {
				itemid_t* current = cur_node->items;
				itemid_t* prior = current;
				current = current->next;
				while (current != NULL && item->primary_key >= current->primary_key) {
					current = current->next;
					prior = prior->next;
				}
				prior->next = item;
				item->next = current;
			}
		}
#else
		item->next = cur_node->items;
		cur_node->items = item;
#endif
	}
}

void BucketHeader::read_item(idx_key_t key, itemid_t * &item, const char * tname) 
{
	BucketNode * cur_node = first_node;
	while (cur_node != NULL) {
		if (cur_node->key == key)
			break;
		cur_node = cur_node->next;
	}
	// M_ASSERT(cur_node->key == key, "Key does not exist!");

	if (cur_node)
		item = cur_node->items;
	else
		item = NULL;
}

bool BucketHeader::exist_item(idx_key_t key) 
{
	BucketNode * cur_node = first_node;
	while (cur_node != NULL) {
		if (cur_node->key == key){
			return 1;
			break;
		}
		cur_node = cur_node->next;
	}
	//M_ASSERT(cur_node->key == key, "Key does not exist!");
	return 0;
}
#include "global.h"
#include "helper.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "mem_alloc.h"

void table_t::init(Catalog * schema) {
	this->table_name = schema->table_name;
	this->schema = schema;
	cur_tab_size = 0UL;
	row_buffer = NULL;
}

RC table_t::get_new_row(row_t *& row) {
	// this function is obsolete. 
	assert(false);
	return RCOK;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id) {
	RC rc = RCOK;

	row_id = cur_tab_size;
	cur_tab_size ++;
	
	row = (row_t *) _mm_malloc(sizeof(row_t), 64);
	rc = row->init(this, part_id, row_id);
	row->init_manager(row);

	return rc;
}

RC table_t::init_row_buffer(int n_size) {
	RC rc = RCOK;

	//row = (row_t *) _mm_malloc(sizeof(row_t), 64);
	assert(!row_buffer);
	row_buffer = (row_t *) calloc(n_size, sizeof(row_t));
	max_rows = n_size;
	assert(row_buffer);
	return rc;
}

// the row is stored sequentially. 
RC table_t::get_new_row_seq(row_t *& row, uint64_t part_id, uint64_t &row_id) {
	RC rc = RCOK;

	assert(row_buffer);
	assert(cur_tab_size < max_rows);

	row_id = cur_tab_size;
	cur_tab_size ++;
	
	row = (row_t *) &row_buffer[row_id];
	rc = row->init(this, part_id, row_id);
	row->init_manager(row);

	return rc;
}

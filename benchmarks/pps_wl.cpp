/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "global.h"
#include "helper.h"
#include "pps.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "pps_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"

RC PPSWorkload::init() {
	Workload::init();
	char * cpath = getenv("SCHEMA_PATH");
	string path;	
	if (cpath == NULL) 
		path = "./benchmarks/";
	else { 
		path = string(cpath);
	}
	path += "PPS_schema.txt";
	cout << "reading schema file: " << path << endl;
	
  printf("Initializing schema... ");
  fflush(stdout);
	init_schema( path.c_str() );
  printf("Done\n");
  printf("Initializing table... ");
  fflush(stdout);
	init_table();
  printf("Done\n");
  fflush(stdout);
	return RCOK;
}

RC PPSWorkload::init_schema(const char * schema_file) {
	Workload::init_schema(schema_file);
	t_suppliers = tables["SUPPLIERS"];
	t_products = tables["PRODUCTS"];
	t_parts = tables["PARTS"];
	t_supplies = tables["SUPPLIES"];
	t_uses = tables["USES"];

	i_parts = indexes["PARTS_IDX"];
	i_suppliers = indexes["SUPPLIERS_IDX"];
	i_products = indexes["PRODUCTS_IDX"];
	i_uses = indexes["USES_IDX"];
	i_supplies = indexes["SUPPLIES_IDX"];
	return RCOK;
}

RC PPSWorkload::init_table() {

/******** fill in data ************/
// data filling process:
// supplies
// --parts 
// --suppliers
// uses 
// --products 
/**********************************/

  pps_thr_args * tt = new pps_thr_args[g_init_parallelism];
	for (UInt32 i = 0; i < g_init_parallelism ; i++) {
    tt[i].wl = this;
    tt[i].id = i;
  }

  threadInitParts(&tt[0]);
  printf("PARTS Done\n");
  threadInitProducts(&tt[0]);
  printf("PRODUCTS Done\n");
  threadInitSuppliers(&tt[0]);
  printf("SUPPLIERS Done\n");
  threadInitUses(&tt[0]);
  printf("USES Done\n");
  threadInitSupplies(&tt[0]);
  printf("SUPPLIES Done\n");
  fflush(stdout);
	printf("\nData Initialization Complete!\n\n");
	return RCOK;
}

RC PPSWorkload::get_txn_man(TxnManager *& txn_manager) {
  DEBUG_M("PPSWorkload::get_txn_man PPSTxnManager alloc\n");
	txn_manager = (PPSTxnManager *)
		mem_allocator.align_alloc( sizeof(PPSTxnManager));
	new(txn_manager) PPSTxnManager();
	//txn_manager->init( this);
	return RCOK;
}

void PPSWorkload::init_tab_parts() {
  char * padding = new char[100];
  for (int i = 0; i < 100; i++) {
    padding[i] = 'z';
  }
  for (UInt32 id = 1; id <= g_max_part_key; id++) {
    if (GET_NODE_ID(parts_to_partition(id)) != g_node_id) 
      continue;
		row_t * row;
		uint64_t row_id;
#if CC_ALG == LIFE
    t_parts->get_new_row(row, parts_to_partition(id), row_id);
#else
    t_parts->get_new_row(row, 0, row_id);
#endif
    row->set_primary_key(id);
    row->set_value(0,id); // part id
    row->set_value(PART_AMOUNT,1000); // # of parts
    row->set_value(FIELD1,padding);
    row->set_value(FIELD2,padding);
    row->set_value(FIELD3,padding);
    row->set_value(FIELD4,padding);
    row->set_value(FIELD5,padding);
    row->set_value(FIELD6,padding);
    row->set_value(FIELD7,padding);
    row->set_value(FIELD8,padding);
    row->set_value(FIELD9,padding);
    row->set_value(FIELD10,padding);
    
		index_insert(i_parts, id, row, parts_to_partition(id));
    DEBUG("PARTS added (%d, ...)\n",id);

  }
}

void PPSWorkload::init_tab_suppliers() {
  char * padding = new char[100];
  for (int i = 0; i < 100; i++) {
    padding[i] = 'z';
  }
  for (UInt32 id = 1; id <= g_max_supplier_key; id++) {
    if (GET_NODE_ID(suppliers_to_partition(id)) != g_node_id) 
      continue;
		row_t * row;
		uint64_t row_id;
#if CC_ALG == LIFE
    t_suppliers->get_new_row(row, suppliers_to_partition(id), row_id);
#else
    t_suppliers->get_new_row(row, 0, row_id);
#endif
    row->set_primary_key(id);
    row->set_value(0,id);
    row->set_value(FIELD1,padding);
    row->set_value(FIELD2,padding);
    row->set_value(FIELD3,padding);
    row->set_value(FIELD4,padding);
    row->set_value(FIELD5,padding);
    row->set_value(FIELD6,padding);
    row->set_value(FIELD7,padding);
    row->set_value(FIELD8,padding);
    row->set_value(FIELD9,padding);
    row->set_value(FIELD10,padding);
    
		index_insert(i_suppliers, id, row, suppliers_to_partition(id));

  }
}

void PPSWorkload::init_tab_products() {
  char * padding = new char[100];
  for (int i = 0; i < 100; i++) {
    padding[i] = 'z';
  }
  for (UInt32 id = 1; id <= g_max_product_key; id++) {
    if (GET_NODE_ID(products_to_partition(id)) != g_node_id) 
      continue;
		row_t * row;
		uint64_t row_id;
#if CC_ALG == LIFE
    t_products->get_new_row(row, products_to_partition(id), row_id);
#else
    t_products->get_new_row(row, 0, row_id);
#endif
    row->set_primary_key(id);
    row->set_value(0,id);
    row->set_value(FIELD1,padding);
    row->set_value(FIELD2,padding);
    row->set_value(FIELD3,padding);
    row->set_value(FIELD4,padding);
    row->set_value(FIELD5,padding);
    row->set_value(FIELD6,padding);
    row->set_value(FIELD7,padding);
    row->set_value(FIELD8,padding);
    row->set_value(FIELD9,padding);
    row->set_value(FIELD10,padding);
    
		index_insert(i_products, id, row, products_to_partition(id));
    DEBUG("PRODUCTS added (%d, ...)\n",id);

  }
}

void PPSWorkload::init_tab_supplies() {
  for (UInt32 id = 1; id <= g_max_supplier_key; id++) {
    std::set<uint64_t> parts_set;
    for (UInt32 i = 0; i < g_max_parts_per; i++) {
      parts_set.insert(URand(1,g_max_part_key));
    }
#if CC_ALG == LIFE
    uint64_t slot = 0;
#endif
    for(auto it = parts_set.begin(); it != parts_set.end();it++
#if CC_ALG == LIFE
        , slot++
#endif
        ) {
      row_t * row;
      uint64_t row_id;
      uint64_t part_id = *it;
#if CC_ALG == LIFE
      t_supplies->get_new_row(row, suppliers_to_partition(id), row_id);
#else
      t_supplies->get_new_row(row, 0, row_id);
#endif
      // The non-unique hash index prepends every entry, so count zero reads
      // the last inserted row. Number physical rows in that same traversal
      // order; LIFE can then use (parent, count) as a stable object identity.
#if CC_ALG == LIFE
      row->set_primary_key((static_cast<uint64_t>(id) << 32) |
                           (parts_set.size() - slot));
#else
      row->set_primary_key(id);
#endif
      //row->set_value(SUPPLIER_KEY,id);
      //row->set_value(PART_KEY,part_id);
      row->set_value(0,id);
      row->set_value(1,part_id);
      index_insert_nonunique(i_supplies, id, row, suppliers_to_partition(id));
    }
  }

}

void PPSWorkload::init_tab_uses() {
  for (UInt32 id = 1; id <= g_max_product_key; id++) {
    std::set<uint64_t> parts_set;
    for (UInt32 i = 0; i < g_max_parts_per; i++) {
      parts_set.insert(URand(1,g_max_part_key));
    }
#if CC_ALG == LIFE
    uint64_t slot = 0;
#endif
    for(auto it = parts_set.begin(); it != parts_set.end();it++
#if CC_ALG == LIFE
        , slot++
#endif
        ) {
      row_t * row;
      uint64_t row_id;
      uint64_t part_id = *it;
#if CC_ALG == LIFE
      t_uses->get_new_row(row, products_to_partition(id), row_id);
#else
      t_uses->get_new_row(row, 0, row_id);
#endif
      // Keep the physical identity aligned with index_read(..., count); see
      // the SUPPLIES initialization above.
#if CC_ALG == LIFE
      row->set_primary_key((static_cast<uint64_t>(id) << 32) |
                           (parts_set.size() - slot));
#else
      row->set_primary_key(id);
#endif
      //row->set_value(PRODUCT_KEY,id);
      //row->set_value(PART_KEY,part_id);
      row->set_value(0,id);
      row->set_value(1,part_id);
      index_insert_nonunique(i_uses, id, row, products_to_partition(id));
      DEBUG("USES added (%d, %ld) -- %lx\n",id,part_id,(uint64_t)row);
    }
  }

}


/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

void * PPSWorkload::threadInitSuppliers(void * This) {
  PPSWorkload * wl = ((pps_thr_args*) This)->wl;
  //int id = ((pps_thr_args*) This)->id;
	wl->init_tab_suppliers();
	return NULL;
}

void * PPSWorkload::threadInitProducts(void * This) {
  PPSWorkload * wl = ((pps_thr_args*) This)->wl;
  //int id = ((pps_thr_args*) This)->id;
	wl->init_tab_products();
	return NULL;
}

void * PPSWorkload::threadInitParts(void * This) {
  PPSWorkload * wl = ((pps_thr_args*) This)->wl;
  //int id = ((pps_thr_args*) This)->id;
	wl->init_tab_parts();
	return NULL;
}

void * PPSWorkload::threadInitSupplies(void * This) {
  PPSWorkload * wl = ((pps_thr_args*) This)->wl;
  //int id = ((pps_thr_args*) This)->id;
	wl->init_tab_supplies();
	return NULL;
}

void * PPSWorkload::threadInitUses(void * This) {
  PPSWorkload * wl = ((pps_thr_args*) This)->wl;
  //int id = ((pps_thr_args*) This)->id;
	wl->init_tab_uses();
	return NULL;
}

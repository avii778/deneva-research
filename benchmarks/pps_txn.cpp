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

#include "pps.h"
#include "pps_query.h"
#include "pps_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "transport.h"
#include "msg_queue.h"
#include "message.h"
#include <algorithm>
#include <cstring>

PPSTxnManager::PPSTxnManager()
    : _wl(NULL), _rc(RCOK), row(NULL), parts_processed_count(0),
      life_part_index(0) {}

void PPSTxnManager::init(uint64_t thd_id, Workload * h_wl) {
#if CC_ALG == LIFE
    YCSBTxnManager::init(thd_id, h_wl);
#else
    TxnManager::init(thd_id, h_wl);
#endif
    _wl = (PPSWorkload *) h_wl;
    reset();
}

void PPSTxnManager::reset() {
    PPSQuery* pps_query = (PPSQuery*) query;
    state = PPS_GETPART0;

    int txn_type = pps_query->txn_type;

    if (txn_type == PPS_GETPART) {
      state = PPS_GETPART0;
    }
    if (txn_type == PPS_GETPRODUCT) {
      state = PPS_GETPRODUCT0;
    }
    if (txn_type == PPS_GETSUPPLIER) {
      state = PPS_GETSUPPLIER0;
    }
    if (txn_type == PPS_GETPARTBYPRODUCT) {
      state = PPS_GETPARTBYPRODUCT0;
    }
    if (txn_type == PPS_GETPARTBYSUPPLIER) {
      state = PPS_GETPARTBYSUPPLIER0;
    }
    if (txn_type == PPS_ORDERPRODUCT) {
      state = PPS_ORDERPRODUCT0;
    }
    if (txn_type == PPS_UPDATEPRODUCTPART) {
      state = PPS_UPDATEPRODUCTPART0;
    }
    if (txn_type == PPS_UPDATEPART) {
      state = PPS_UPDATEPART0;
    }

    parts_processed_count = 0;
    life_part_index = 0;
    pps_query->part_keys.clear();
#if CC_ALG == LIFE
    YCSBTxnManager::reset();
#else
    TxnManager::reset();
#endif
}

RC PPSTxnManager::run_txn_post_wait() {
    get_row_post_wait(row);
    next_pps_state();
    return RCOK;
}


RC PPSTxnManager::run_txn() {
#if MODE == SETUP_MODE
  return RCOK;
#endif
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();

#if CC_ALG == CALVIN
  rc = run_calvin_txn();
  return rc;
#endif
#if CC_ALG == LIFE
  return run_life_txn();
#endif

  if(IS_LOCAL(txn->txn_id) && 
      (state == PPS_GETPART0 || 
       state == PPS_GETPRODUCT0 ||
       state == PPS_GETSUPPLIER0 ||
       state == PPS_GETPARTBYSUPPLIER0 ||
       state == PPS_GETPARTBYPRODUCT0 ||
       state == PPS_ORDERPRODUCT0 ||
       state == PPS_UPDATEPRODUCTPART0 ||
       state == PPS_UPDATEPART0 
       )) {
    DEBUG("Running txn %ld, type %d\n",txn->txn_id,((PPSQuery*)query)->txn_type);
#if DISTR_DEBUG
    query->print();
#endif
    query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
  }


  while(rc == RCOK && !is_done()) {
    rc = run_txn_state();
  }

  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;

  if(IS_LOCAL(get_txn_id())) {
    if(is_done() && rc == RCOK) 
      rc = start_commit();
    else if(rc == Abort)
      rc = start_abort();
  }

  return rc;

}

bool PPSTxnManager::is_done() {
  bool done = false;
  done = state == PPS_FIN;
  return done;
}

RC PPSTxnManager::acquire_locks() {
  uint64_t starttime = get_sys_clock();
  assert(CC_ALG == CALVIN);
  locking_done = false;
  RC rc = RCOK;
  RC rc2 = RCOK;
  INDEX * index;
  itemid_t * item;
  incr_lr();
  PPSQuery* pps_query = (PPSQuery*) query;
  uint64_t part_key = pps_query->part_key;
  uint64_t product_key = pps_query->product_key;
  uint64_t supplier_key = pps_query->supplier_key;
  uint64_t partition_id_part = parts_to_partition(pps_query->part_key);
  uint64_t partition_id_product = products_to_partition(pps_query->product_key);
  uint64_t partition_id_supplier = suppliers_to_partition(pps_query->supplier_key);

  switch(pps_query->txn_type) {
    case PPS_GETPART:
      if(GET_NODE_ID(partition_id_part) == g_node_id) {
        index = _wl->i_parts;
        item = index_read(index, part_key, partition_id_part);
        row_t * row = ((row_t *)item->location);
        rc2 = get_lock(row,RD);
        if(rc2 != RCOK)
          rc = rc2;
      }
      break;
    case PPS_GETPRODUCT:
      if(GET_NODE_ID(partition_id_product) == g_node_id) {
        index = _wl->i_products;
        item = index_read(index, product_key, partition_id_product);
        row_t * row = ((row_t *)item->location);
        rc2 = get_lock(row,RD);
        if(rc2 != RCOK)
          rc = rc2;
      }
      break;
    case PPS_GETSUPPLIER:
      if(GET_NODE_ID(partition_id_supplier) == g_node_id) {
        index = _wl->i_suppliers;
        item = index_read(index, supplier_key, partition_id_supplier);
        row_t * row = ((row_t *)item->location);
        rc2 = get_lock(row,RD);
        if(rc2 != RCOK)
          rc = rc2;
      }
      break;
    case PPS_GETPARTBYSUPPLIER:
      if(GET_NODE_ID(partition_id_supplier) == g_node_id) {
        index = _wl->i_suppliers;
        item = index_read(index, supplier_key, partition_id_supplier);
        row_t * row = ((row_t *)item->location);
        rc2 = get_lock(row,RD);
        if(rc2 != RCOK)
          rc = rc2;

        index = _wl->i_supplies;
        int count = 0;
        item = index_read(index, supplier_key, partition_id_product,count);
        while (item != NULL) {
            count++;
            row_t * row = ((row_t *)item->location);
            rc2 = get_lock(row,RD);
            if(rc2 != RCOK)
              rc = rc2;
            item = index_read(index, supplier_key, partition_id_product,count);
        }
      }
      for (uint64_t i = 0; i < pps_query->part_keys.size(); i++) {
          uint64_t key = pps_query->part_keys[i];
          uint64_t pid = parts_to_partition(key);
          if(GET_NODE_ID(pid) == g_node_id) {
            index = _wl->i_parts;
            item = index_read(index, key, pid);
            row_t * row = ((row_t *)item->location);
            rc2 = get_lock(row,RD);
            if(rc2 != RCOK)
              rc = rc2;
          }
      }

      break;
    case PPS_GETPARTBYPRODUCT:
        if(GET_NODE_ID(partition_id_product) == g_node_id) {
          index = _wl->i_products;
          item = index_read(index, product_key, partition_id_product);
          row_t * row = ((row_t *)item->location);
          rc2 = get_lock(row,RD);
          if(rc2 != RCOK)
            rc = rc2;

          index = _wl->i_uses;
          int count = 0;
          item = index_read(index, product_key, partition_id_product,count);
          while (item != NULL) {
              count++;
              row_t * row = ((row_t *)item->location);
              rc2 = get_lock(row,RD);
              if(rc2 != RCOK)
                rc = rc2;
              item = index_read(index, product_key, partition_id_product,count);
          }
        }
        for (uint64_t i = 0; i < pps_query->part_keys.size(); i++) {
            uint64_t key = pps_query->part_keys[i];
            uint64_t pid = parts_to_partition(key);
            if(GET_NODE_ID(pid) == g_node_id) {
              index = _wl->i_parts;
              item = index_read(index, key, pid);
              row_t * row = ((row_t *)item->location);
              rc2 = get_lock(row,RD);
              if(rc2 != RCOK)
                rc = rc2;
            }
        }

      break;
    case PPS_ORDERPRODUCT:
      if(GET_NODE_ID(partition_id_product) == g_node_id) {
        index = _wl->i_products;
        item = index_read(index, product_key, partition_id_product);
        row_t * row = ((row_t *)item->location);
        rc2 = get_lock(row,RD);
        if(rc2 != RCOK)
          rc = rc2;

        index = _wl->i_uses;
        int count = 0;
        item = index_read(index, product_key, partition_id_product,count);
        while (item != NULL) {
            count++;
            row_t * row = ((row_t *)item->location);
            rc2 = get_lock(row,RD);
            if(rc2 != RCOK)
              rc = rc2;
            item = index_read(index, product_key, partition_id_product,count);
        }
      }
      for (uint64_t i = 0; i < pps_query->part_keys.size(); i++) {
          uint64_t key = pps_query->part_keys[i];
          uint64_t pid = parts_to_partition(key);
          if(GET_NODE_ID(pid) == g_node_id) {
            index = _wl->i_parts;
            item = index_read(index, key, pid);
            row_t * row = ((row_t *)item->location);
            rc2 = get_lock(row,WR);
            if(rc2 != RCOK)
              rc = rc2;
          }
      }

      break;
    case PPS_UPDATEPRODUCTPART:
      if(GET_NODE_ID(partition_id_product) == g_node_id) {
        index = _wl->i_products;
        item = index_read(index, product_key, partition_id_product);
        row_t * row = ((row_t *)item->location);
        rc2 = get_lock(row,WR);
        if(rc2 != RCOK)
          rc = rc2;
      }
      break;
    case PPS_UPDATEPART:
      if(GET_NODE_ID(partition_id_part) == g_node_id) {
        index = _wl->i_parts;
        item = index_read(index, part_key, partition_id_part);
        row_t * row = ((row_t *)item->location);
        rc2 = get_lock(row,WR);
        if(rc2 != RCOK)
          rc = rc2;
      }
      break;

    default: assert(false);
  }
  if(decr_lr() == 0) {
    if(ATOM_CAS(lock_ready,0,1))
      rc = RCOK;
  }
  txn_stats.wait_starttime = get_sys_clock();
  locking_done = true;
  INC_STATS(get_thd_id(),calvin_sched_time,get_sys_clock() - starttime);
  return rc;
}


void PPSTxnManager::next_pps_state() {
  switch(state) {
    case PPS_GETPART_S:
      state = PPS_GETPART0;
      break;
    case PPS_GETPART0:
      state = PPS_GETPART1;
      break;
    case PPS_GETPART1:
      state = PPS_FIN;
      break;
    case PPS_GETPRODUCT_S:
      state = PPS_GETPRODUCT0;
      break;
    case PPS_GETPRODUCT0:
      state = PPS_GETPRODUCT1;
      break;
    case PPS_GETPRODUCT1:
      state = PPS_FIN;
      break;
    case PPS_GETSUPPLIER_S:
      state = PPS_GETSUPPLIER0;
      break;
    case PPS_GETSUPPLIER0:
      state = PPS_GETSUPPLIER1;
      break;
    case PPS_GETSUPPLIER1:
      state = PPS_FIN;
      break;
    case PPS_GETPARTBYSUPPLIER_S:
      state = PPS_GETPARTBYSUPPLIER0;
      break;
    case PPS_GETPARTBYSUPPLIER0:
      state = PPS_GETPARTBYSUPPLIER1;
      break;
    case PPS_GETPARTBYSUPPLIER1:
      state = PPS_GETPARTBYSUPPLIER2;
      break;
    case PPS_GETPARTBYSUPPLIER2:
      if (row == NULL) {
        state = PPS_FIN;
      }
      else {
        ++parts_processed_count;
        state = PPS_GETPARTBYSUPPLIER3;
      }
      break;
    case PPS_GETPARTBYSUPPLIER3:
      state = PPS_GETPARTBYSUPPLIER4;
      break;
    case PPS_GETPARTBYSUPPLIER4:
      state = PPS_GETPARTBYSUPPLIER5;
      break;
    case PPS_GETPARTBYSUPPLIER5:
      state = PPS_GETPARTBYSUPPLIER2;
      if (!IS_LOCAL(txn->txn_id)) {
          state = PPS_FIN;
      }
      break;
    case PPS_GETPARTBYPRODUCT_S:
      state = PPS_GETPARTBYPRODUCT0;
      break;
    case PPS_GETPARTBYPRODUCT0:
      state = PPS_GETPARTBYPRODUCT1;
      break;
    case PPS_GETPARTBYPRODUCT1:
      state = PPS_GETPARTBYPRODUCT2;
      break;
    case PPS_GETPARTBYPRODUCT2:
      if (row == NULL) {
        state = PPS_FIN;
      }
      else {
        ++parts_processed_count;
        state = PPS_GETPARTBYPRODUCT3;
      }
      break;
    case PPS_GETPARTBYPRODUCT3:
      state = PPS_GETPARTBYPRODUCT4;
      break;
    case PPS_GETPARTBYPRODUCT4:
      state = PPS_GETPARTBYPRODUCT5;
      break;
    case PPS_GETPARTBYPRODUCT5:
      state = PPS_GETPARTBYPRODUCT2;
      if (!IS_LOCAL(txn->txn_id)) {
          state = PPS_FIN;
      }
      //state = PPS_FIN;
      break;
    case PPS_ORDERPRODUCT_S:
      state = PPS_ORDERPRODUCT0;
      break;
    case PPS_ORDERPRODUCT0:
      state = PPS_ORDERPRODUCT1;
      break;
    case PPS_ORDERPRODUCT1:
      state = PPS_ORDERPRODUCT2;
      break;
    case PPS_ORDERPRODUCT2:
        if(!IS_LOCAL(txn->txn_id) && row != NULL) {
            ++parts_processed_count;
            state = PPS_ORDERPRODUCT3;
        }
        else {
            state = PPS_FIN;
        }
        break;
    case PPS_ORDERPRODUCT3:
      state = PPS_ORDERPRODUCT4;
      break;
    case PPS_ORDERPRODUCT4:
      state = PPS_ORDERPRODUCT5;
      break;
    case PPS_ORDERPRODUCT5:
      state = PPS_ORDERPRODUCT2;
      if (!IS_LOCAL(txn->txn_id)) {
          state = PPS_FIN;
      }
      break;
    case PPS_UPDATEPRODUCTPART_S:
      state = PPS_UPDATEPRODUCTPART0;
      break;
    case PPS_UPDATEPRODUCTPART0:
      state = PPS_UPDATEPRODUCTPART1;
      break;
    case PPS_UPDATEPRODUCTPART1:
      state = PPS_FIN;
      break;
    case PPS_UPDATEPART_S:
      state = PPS_UPDATEPART0;
      break;
    case PPS_UPDATEPART0:
      state = PPS_UPDATEPART1;
      break;
    case PPS_UPDATEPART1:
      state = PPS_FIN;
      break;
    case PPS_FIN:
      break;
    default:
      assert(false);
  }

}

// remote request should always be on a parts key, given our workload
RC PPSTxnManager::send_remote_request() {
  assert(IS_LOCAL(get_txn_id()));
  PPSQuery* pps_query = (PPSQuery*) query;
  PPSRemTxnType next_state = PPS_FIN;
	uint64_t part_key = pps_query->part_key;
  uint64_t dest_node_id = UINT64_MAX;
  dest_node_id = GET_NODE_ID(parts_to_partition(part_key));
  PPSQueryMessage * msg = (PPSQueryMessage*)Message::create_message(this,RQRY);
  msg->state = state;
  query->partitions_touched.add_unique(GET_PART_ID(0,dest_node_id));
  msg_queue.enqueue(get_thd_id(),msg,dest_node_id);
  state = next_state;
  return WAIT_REM;
}

RC PPSTxnManager::run_txn_state() {
    PPSQuery* pps_query = (PPSQuery*) query;
	RC rc = RCOK;
    uint64_t part_key = pps_query->part_key;
    uint64_t product_key = pps_query->product_key;
    uint64_t supplier_key = pps_query->supplier_key;
    bool part_loc = GET_NODE_ID(parts_to_partition(part_key)) == g_node_id;
    bool product_loc = GET_NODE_ID(products_to_partition(product_key)) == g_node_id;
    bool supplier_loc = GET_NODE_ID(suppliers_to_partition(supplier_key)) == g_node_id;
  switch(state) {
    case PPS_GETPART0:
      if (part_loc)
        rc = run_getpart_0(part_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_GETPART1:
        rc = run_getpart_1(row);
      break;
    case PPS_GETPRODUCT0:
      if (product_loc)
        rc = run_getproduct_0(product_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_GETPRODUCT1:
        rc = run_getproduct_1(row);
      break;
    case PPS_GETSUPPLIER0:
      if (supplier_loc)
        rc = run_getsupplier_0(supplier_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_GETSUPPLIER1:
        rc = run_getsupplier_1(row);
      break;
    case PPS_GETPARTBYSUPPLIER0:
      if (supplier_loc)
        rc = run_getpartsbysupplier_0(supplier_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_GETPARTBYSUPPLIER1:
        rc = run_getpartsbysupplier_1(row);
      break;
    case PPS_GETPARTBYSUPPLIER2:
      if (supplier_loc)
        rc = run_getpartsbysupplier_2(supplier_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_GETPARTBYSUPPLIER3:
        rc = run_getpartsbysupplier_3(pps_query->part_key, row);
      break;
    case PPS_GETPARTBYSUPPLIER4:
      if (part_loc)
        rc = run_getpartsbysupplier_4(pps_query->part_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_GETPARTBYSUPPLIER5:
        rc = run_getpartsbysupplier_5(row);
      break;
    case PPS_GETPARTBYPRODUCT0:
      if (product_loc)
        rc = run_getpartsbyproduct_0(product_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_GETPARTBYPRODUCT1:
        rc = run_getpartsbyproduct_1(row);
      break;
    case PPS_GETPARTBYPRODUCT2:
      if (product_loc)
        rc = run_getpartsbyproduct_2(product_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_GETPARTBYPRODUCT3:
        rc = run_getpartsbyproduct_3(pps_query->part_key,row);
      break;
    case PPS_GETPARTBYPRODUCT4:
      if (part_loc)
        rc = run_getpartsbyproduct_4(pps_query->part_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_GETPARTBYPRODUCT5:
        rc = run_getpartsbyproduct_5(row);
      break;
    case PPS_ORDERPRODUCT0:
      if (product_loc)
        rc = run_orderproduct_0(product_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_ORDERPRODUCT1:
        rc = run_orderproduct_1(row);
      break;
    case PPS_ORDERPRODUCT2:
      if (product_loc)
        rc = run_orderproduct_2(product_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_ORDERPRODUCT3:
        rc = run_orderproduct_3(pps_query->part_key,row);
      break;
    case PPS_ORDERPRODUCT4:
      if (part_loc)
        rc = run_orderproduct_4(pps_query->part_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_ORDERPRODUCT5:
        rc = run_orderproduct_5(row);
      break;
    case PPS_UPDATEPRODUCTPART0:
      if (product_loc)
        rc = run_updateproductpart_0(product_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_UPDATEPRODUCTPART1:
        rc = run_updateproductpart_1(part_key, row);
      break;
    case PPS_UPDATEPART0:
      if (part_loc)
        rc = run_updatepart_0(part_key, row);
      else
        rc = send_remote_request();
      break;
    case PPS_UPDATEPART1:
        rc = run_updatepart_1(row);
      break;
    case PPS_FIN:
      state = PPS_FIN;
      break;
    default:
      assert(false);
  }
  if(rc == RCOK)
    next_pps_state();
  return rc;
}

inline RC PPSTxnManager::run_getpart_0(uint64_t part_key, row_t *& r_local) {
  /*
    SELECT * FROM PARTS WHERE PART_KEY = :part_key; 
   */
    RC rc;
    itemid_t * item;
		INDEX * index = _wl->i_parts;
		item = index_read(index, part_key, parts_to_partition(part_key));
		assert(item != NULL);
		row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
    return rc;
}
inline RC PPSTxnManager::run_getpart_1(row_t *& r_local) {
  /*
    SELECT * FROM PARTS WHERE PART_KEY = :part_key; 
   */
  assert(r_local);
  getAllFields(r_local);
  return RCOK;
}


inline RC PPSTxnManager::run_getproduct_0(uint64_t product_key, row_t *& r_local) {
  /*
    SELECT * FROM PRODUCTS WHERE PRODUCT_KEY = :product_key; 
   */
    RC rc;
    itemid_t * item;
		INDEX * index = _wl->i_products;
		item = index_read(index, product_key, products_to_partition(product_key));
		assert(item != NULL);
		row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
    return rc;
}
inline RC PPSTxnManager::run_getproduct_1(row_t *& r_local) {
  /*
    SELECT * FROM PRODUCTS WHERE PRODUCT_KEY = :product_key; 
   */
  assert(r_local);
  getAllFields(r_local);
  return RCOK;
}

inline void PPSTxnManager::getThreeFields(row_t *& r_local) {
  //char * fields = new char[100];
  char * data __attribute__((unused));
  data = r_local->get_data();
  //r_local->get_value(FIELD1,fields);
  //r_local->get_value(FIELD2,fields);
  //r_local->get_value(FIELD3,fields);
}

inline void PPSTxnManager::getAllFields(row_t *& r_local) {
  char * data __attribute__((unused));
  data = r_local->get_data();
  /*
  char * fields = new char[100];
  r_local->get_value(FIELD1,fields);
  r_local->get_value(FIELD2,fields);
  r_local->get_value(FIELD3,fields);
  r_local->get_value(FIELD4,fields);
  r_local->get_value(FIELD5,fields);
  r_local->get_value(FIELD6,fields);
  r_local->get_value(FIELD7,fields);
  r_local->get_value(FIELD8,fields);
  r_local->get_value(FIELD9,fields);
  r_local->get_value(FIELD10,fields);
  */
}

inline RC PPSTxnManager::run_getsupplier_0(uint64_t supplier_key, row_t *& r_local) {
  /*
    SELECT * FROM SUPPLIERS WHERE SUPPLIER_KEY = :supplier_key; 
   */
    RC rc;
    itemid_t * item;
		INDEX * index = _wl->i_suppliers;
		item = index_read(index, supplier_key, suppliers_to_partition(supplier_key));
		assert(item != NULL);
		row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
    return rc;

}

inline RC PPSTxnManager::run_getsupplier_1(row_t *& r_local) {
  /*
    SELECT * FROM SUPPLIERS WHERE SUPPLIER_KEY = :supplier_key; 
   */
  assert(r_local);
  getAllFields(r_local);
  return RCOK;

}

inline RC PPSTxnManager::run_getpartsbyproduct_0(uint64_t product_key, row_t *& r_local) {
  /*
    SELECT FIELD1, FIELD2, FIELD3 FROM PRODUCTS WHERE PRODUCT_KEY = ? 
   */
    RC rc;
    itemid_t * item;
		INDEX * index = _wl->i_products;
		item = index_read(index, product_key, products_to_partition(product_key));
		assert(item != NULL);
		row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
    return rc;
}
inline RC PPSTxnManager::run_getpartsbyproduct_1(row_t *& r_local) {
  /*
    SELECT FIELD1, FIELD2, FIELD3 FROM PRODUCTS WHERE PRODUCT_KEY = ? 
   */
  assert(r_local);
  getThreeFields(r_local);
  return RCOK;
}

inline RC PPSTxnManager::run_getpartsbyproduct_2(uint64_t product_key, row_t *& r_local) {
  /*
    SELECT PART_KEY FROM USES WHERE PRODUCT_KEY = ? 
   */
  DEBUG("Getting product_key %ld count %ld\n",product_key,parts_processed_count);
    RC rc;
    itemid_t * item;
		INDEX * index = _wl->i_uses;
		item = index_read(index, product_key, products_to_partition(product_key),parts_processed_count);
    if (item == NULL) {
      r_local = NULL;
      return RCOK;
    }
		assert(item != NULL);
		row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
  DEBUG("From product_key %ld -- %lx\n",product_key,(uint64_t)r_local);
    return rc;
}

inline RC PPSTxnManager::run_getpartsbyproduct_3(uint64_t &part_key, row_t *& r_local) {
  /*
    SELECT PART_KEY FROM USES WHERE PRODUCT_KEY = ? 
   */
  //r_local->get_value(PART_KEY,part_key);
  //char * data __attribute__((unused));
  //data = r_local->get_data();
  // PART_KEY is located at column 1
  r_local->get_value(1,part_key);
  DEBUG("Read part_key %ld\n",part_key);
  return RCOK;
}

inline RC PPSTxnManager::run_getpartsbyproduct_4(uint64_t part_key, row_t *& r_local) {
  /*
   SELECT FIELD1, FIELD2, FIELD3 FROM PARTS WHERE PART_KEY = ? 
   */
  DEBUG("Access part_key %ld\n",part_key);
    RC rc;
    itemid_t * item;
		INDEX * index = _wl->i_parts;
		item = index_read(index, part_key, parts_to_partition(part_key));
		assert(item != NULL);
		row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
    return rc;
}
inline RC PPSTxnManager::run_getpartsbyproduct_5(row_t *& r_local) {
  /*
   SELECT FIELD1, FIELD2, FIELD3 FROM PARTS WHERE PART_KEY = ? 
   */
  DEBUG("run_getpartsbyproduct_5\n");
  assert(r_local);
  getThreeFields(r_local);
  return RCOK;
}

inline RC PPSTxnManager::run_orderproduct_0(uint64_t product_key, row_t *& r_local) {
  /*
    SELECT FIELD1, FIELD2, FIELD3 FROM PRODUCTS WHERE PRODUCT_KEY = ? 
   */
    RC rc;
    itemid_t * item;
		INDEX * index = _wl->i_products;
		item = index_read(index, product_key, products_to_partition(product_key));
		assert(item != NULL);
		row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
    return rc;
}
inline RC PPSTxnManager::run_orderproduct_1(row_t *& r_local) {
  /*
    SELECT FIELD1, FIELD2, FIELD3 FROM PRODUCTS WHERE PRODUCT_KEY = ? 
   */
  assert(r_local);
  getThreeFields(r_local);
  return RCOK;
}

inline RC PPSTxnManager::run_orderproduct_2(uint64_t product_key, row_t *& r_local) {
    /*
      SELECT PART_KEY FROM USES WHERE PRODUCT_KEY = ?
     */
    DEBUG("Getting product_key %ld count %ld\n",product_key,parts_processed_count);
    RC rc;
    itemid_t * item;
    INDEX * index = _wl->i_uses;
    item = index_read(index, product_key, products_to_partition(product_key),parts_processed_count);
    if (item == NULL) {
        r_local = NULL;
        return RCOK;
    }
    assert(item != NULL);
    row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
    DEBUG("From product_key %ld -- %lx\n",product_key,(uint64_t)r_local);
    return rc;
}

inline RC PPSTxnManager::run_orderproduct_3(uint64_t &part_key, row_t *& r_local) {
  /*
    SELECT PART_KEY FROM USES WHERE PRODUCT_KEY = ? 
   */
  //r_local->get_value(PART_KEY,part_key);
  //char * data __attribute__((unused));
  //data = r_local->get_data();
  // PART_KEY is located at column 1
  r_local->get_value(1,part_key);
  DEBUG("Read part_key %ld\n",part_key);
  return RCOK;
}

inline RC PPSTxnManager::run_orderproduct_4(uint64_t part_key, row_t *& r_local) {
    /*
     SELECT FIELD1, FIELD2, FIELD3 FROM PARTS WHERE PART_KEY = ?
     */
    DEBUG("Access part_key %ld\n",part_key);
    RC rc;
    itemid_t * item;
    INDEX * index = _wl->i_parts;
    item = index_read(index, part_key, parts_to_partition(part_key));
    assert(item != NULL);
    row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, WR, r_local);
    return rc;
}
inline RC PPSTxnManager::run_orderproduct_5(row_t *& r_local) {
  /*
   SELECT FIELD1, FIELD2, FIELD3 FROM PARTS WHERE PART_KEY = ? 
   */
  DEBUG("run_orderproduct_5\n");
  assert(r_local);
  // update
  // If part_amount is 0, should abort
  uint64_t part_amount;
  r_local->get_value(PART_AMOUNT,part_amount);
  r_local->set_value(PART_AMOUNT,part_amount-1);
  return RCOK;
}


inline RC PPSTxnManager::run_getpartsbysupplier_0(uint64_t supplier_key, row_t *& r_local) {
  /*
    SELECT FIELD1, FIELD2, FIELD3 FROM suppliers WHERE supplier_KEY = ? 
   */
    RC rc;
    itemid_t * item;
		INDEX * index = _wl->i_suppliers;
		item = index_read(index, supplier_key, suppliers_to_partition(supplier_key));
		assert(item != NULL);
		row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
    return rc;
}
inline RC PPSTxnManager::run_getpartsbysupplier_1(row_t *& r_local) {
  /*
    SELECT FIELD1, FIELD2, FIELD3 FROM suppliers WHERE supplier_KEY = ? 
   */
  assert(r_local);
  getThreeFields(r_local);
  return RCOK;
}

inline RC PPSTxnManager::run_getpartsbysupplier_2(uint64_t supplier_key, row_t *& r_local) {
  /*
    SELECT PART_KEY FROM USES WHERE supplier_KEY = ? 
   */
    RC rc;
    itemid_t * item;
    INDEX * index = _wl->i_supplies;
    item = index_read(index, supplier_key, suppliers_to_partition(supplier_key),parts_processed_count);
    if (item == NULL) {
        r_local = NULL;
        return RCOK;
    }
    assert(item != NULL);
    row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
    return rc;
}
inline RC PPSTxnManager::run_getpartsbysupplier_3(uint64_t &part_key, row_t *& r_local) {
  /*
    SELECT PART_KEY FROM USES WHERE supplier_KEY = ? 
   */
    r_local->get_value(1,part_key);
    DEBUG("Read part_key %ld\n",part_key);
    return RCOK;
}
inline RC PPSTxnManager::run_getpartsbysupplier_4(uint64_t part_key, row_t *& r_local) {
  /*
   SELECT FIELD1, FIELD2, FIELD3 FROM PARTS WHERE PART_KEY = ? 
   */
    RC rc;
    itemid_t * item;
    INDEX * index = _wl->i_parts;
    item = index_read(index, part_key, parts_to_partition(part_key));
    assert(item != NULL);
    row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
    return rc;
}
inline RC PPSTxnManager::run_getpartsbysupplier_5(row_t *& r_local) {
  /*
   SELECT FIELD1, FIELD2, FIELD3 FROM PARTS WHERE PART_KEY = ? 
   */
  assert(r_local);
  getThreeFields(r_local);
  return RCOK;
}

inline RC PPSTxnManager::run_updateproductpart_0(uint64_t product_key, row_t *& r_local) {
  /*
    SELECT FIELD1, FIELD2, FIELD3 FROM PRODUCTS WHERE PRODUCT_KEY = ? 
   */
    RC rc;
    itemid_t * item;
    INDEX * index = _wl->i_uses;
    // XXX this will always return the first part for this product
    item = index_read(index, product_key, products_to_partition(product_key));
    assert(item != NULL);
    row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, WR, r_local);
    return rc;
}
inline RC PPSTxnManager::run_updateproductpart_1(uint64_t part_key, row_t *& r_local) {
  /*
    UPDATE PART_KEY FROM PRODUCTS WHERE PRODUCT_KEY = ? 
   */
  assert(r_local);
  r_local->set_value(1,part_key);
  return RCOK;
}

inline RC PPSTxnManager::run_updatepart_0(uint64_t part_key, row_t *& r_local) {
  /*
    SELECT * FROM PARTS WHERE PART_KEY = :part_key; 
   */
    RC rc;
    itemid_t * item;
		INDEX * index = _wl->i_parts;
		item = index_read(index, part_key, parts_to_partition(part_key));
		assert(item != NULL);
		row_t* r_loc = (row_t *) item->location;
    rc = get_row(r_loc, WR, r_local);
    return rc;
}
inline RC PPSTxnManager::run_updatepart_1(row_t *& r_local) {
  /*
    SELECT * FROM PARTS WHERE PART_KEY = :part_key; 
   */
  assert(r_local);
  uint64_t amount;
  r_local->get_value(1,amount);
  r_local->set_value(1,amount+100);
  return RCOK;
}

#if CC_ALG == LIFE
namespace {

uint32_t pps_initial_life_state(uint32_t txn_type) {
  switch (txn_type) {
  case PPS_GETPART: return PPS_GETPART0;
  case PPS_GETSUPPLIER: return PPS_GETSUPPLIER0;
  case PPS_GETPRODUCT: return PPS_GETPRODUCT0;
  case PPS_GETPARTBYSUPPLIER: return PPS_GETPARTBYSUPPLIER0;
  case PPS_GETPARTBYPRODUCT: return PPS_GETPARTBYPRODUCT0;
  case PPS_ORDERPRODUCT: return PPS_ORDERPRODUCT0;
  case PPS_UPDATEPRODUCTPART: return PPS_UPDATEPRODUCTPART0;
  case PPS_UPDATEPART: return PPS_UPDATEPART0;
  default: assert(false); return PPS_FIN;
  }
}

uint64_t pps_relation_row_id(uint64_t parent_key, uint64_t slot) {
  assert(parent_key <= UINT32_MAX && slot < UINT32_MAX);
  return (parent_key << 32) | (slot + 1);
}

uint64_t pps_relation_parent(uint64_t row_id) { return row_id >> 32; }
uint64_t pps_relation_slot(uint64_t row_id) {
  assert((row_id & UINT32_MAX) != 0);
  return (row_id & UINT32_MAX) - 1;
}

void pps_consume_life_response(LifePpsSnapshot &pps,
                               const LifeResponse &response) {
  switch (pps.state) {
  case PPS_GETPART0:
  case PPS_GETPRODUCT0:
  case PPS_GETSUPPLIER0:
  case PPS_UPDATEPRODUCTPART0:
  case PPS_UPDATEPART0:
    pps.state = PPS_FIN;
    break;
  case PPS_GETPARTBYSUPPLIER0:
    pps.state = PPS_GETPARTBYSUPPLIER2;
    break;
  case PPS_GETPARTBYPRODUCT0:
    pps.state = PPS_GETPARTBYPRODUCT2;
    break;
  case PPS_ORDERPRODUCT0:
    pps.state = PPS_ORDERPRODUCT2;
    break;
  case PPS_GETPARTBYSUPPLIER2:
  case PPS_GETPARTBYPRODUCT2:
  case PPS_ORDERPRODUCT2: {
    assert(response.value.size() == sizeof(uint64_t));
    uint64_t key;
    std::memcpy(&key, response.value.data(), sizeof(key));
    pps.part_keys.push_back(key);
    ++pps.scan_index;
    break;
  }
  case PPS_GETPARTBYSUPPLIER4:
  case PPS_GETPARTBYPRODUCT4:
  case PPS_ORDERPRODUCT4:
    ++pps.part_index;
    break;
  default:
    assert(false);
  }
}

} // namespace

LifeTxnDescriptor PPSTxnManager::life_descriptor() const {
  LifeTxnDescriptor descriptor = TxnManager::life_descriptor();
  const PPSQuery *pps_query = static_cast<const PPSQuery *>(query);
  descriptor.workload = LifeWorkloadKind::Pps;
  descriptor.pps.txn_type = static_cast<uint32_t>(pps_query->txn_type);
  descriptor.pps.state = static_cast<uint32_t>(state);
  descriptor.pps.part_key = pps_query->part_key;
  descriptor.pps.supplier_key = pps_query->supplier_key;
  descriptor.pps.product_key = pps_query->product_key;
  descriptor.pps.scan_index = parts_processed_count;
  descriptor.pps.part_index = life_part_index;
  descriptor.pps.program_index = descriptor.history.size();
  descriptor.pps.part_keys.reserve(pps_query->part_keys.size());
  for (uint64_t i = 0; i < pps_query->part_keys.size(); ++i)
    descriptor.pps.part_keys.push_back(pps_query->part_keys[i]);
  descriptor.history.reserve(MAX_PPS_PARTS_PER * 2 + 2);
  descriptor.touched_objects.reserve(MAX_PPS_PARTS_PER * 2 + 2);
  return descriptor;
}

void PPSTxnManager::life_reset_workload() {
  PPSQuery *pps_query = static_cast<PPSQuery *>(query);
  state = static_cast<PPSRemTxnType>(
      pps_initial_life_state(static_cast<uint32_t>(pps_query->txn_type)));
  parts_processed_count = 0;
  life_part_index = 0;
  pps_query->part_keys.clear();
  reset_pending_life_finalize();
  reset_life_piggyback_prepare();
}

RC PPSTxnManager::complete_life_home_transaction() {
  return YCSBTxnManager::complete_life_home_transaction();
}

LifeOperation PPSTxnManager::make_life_pps_operation(
    uint64_t table_id, uint64_t partition_id, uint64_t logical_row_id,
    LifeOperationKind kind, uint32_t field_id, int64_t argument) const {
  LifeOperation operation;
  operation.object.table_id = table_id;
  operation.object.partition_id = partition_id;
  operation.object.primary_key = logical_row_id;
  operation.object.row_id = logical_row_id;
  operation.kind = kind;
  operation.field_id = field_id;
  operation.value_size = sizeof(uint64_t);
  if (kind != LifeOperationKind::ReadField) {
    const uint8_t *bytes = reinterpret_cast<const uint8_t *>(&argument);
    operation.argument.assign(bytes, bytes + sizeof(argument));
  }
  return operation;
}

row_t *PPSTxnManager::lookup_life_row(const LifeObjectId &object) const {
  INDEX *index = NULL;
  uint64_t lookup_key = object.primary_key;
  uint64_t count = 0;

  if (object.table_id == _wl->t_parts->get_table_id()) {
    index = _wl->i_parts;
  } else if (object.table_id == _wl->t_products->get_table_id()) {
    index = _wl->i_products;
  } else if (object.table_id == _wl->t_suppliers->get_table_id()) {
    index = _wl->i_suppliers;
  } else if (object.table_id == _wl->t_uses->get_table_id()) {
    index = _wl->i_uses;
    lookup_key = pps_relation_parent(object.primary_key);
    count = pps_relation_slot(object.primary_key);
  } else if (object.table_id == _wl->t_supplies->get_table_id()) {
    index = _wl->i_supplies;
    lookup_key = pps_relation_parent(object.primary_key);
    count = pps_relation_slot(object.primary_key);
  } else {
    assert(false);
  }

  itemid_t *item = const_cast<PPSTxnManager *>(this)->index_read(
      index, lookup_key, object.partition_id, count);
  assert(item != NULL);
  row_t *result = static_cast<row_t *>(item->location);
  assert(result->get_primary_key() == object.primary_key);
  return result;
}

void PPSTxnManager::life_reconcile_descriptor(
    LifeTxnDescriptor &descriptor) {
  LifePpsSnapshot &pps = descriptor.pps;
  assert(descriptor.workload == LifeWorkloadKind::Pps);
  assert(pps.program_index <= descriptor.history.size());

  // Algorithm 2 advances run(state, response) after Execute has appended the
  // operation. Row_life can expose that durable descriptor to a helper first,
  // so consume every response not yet reflected in the PPS continuation.
  while (pps.program_index < descriptor.history.size()) {
    pps_consume_life_response(
        pps, descriptor.history[pps.program_index].response);
    ++pps.program_index;
  }

  INDEX *scan_index = NULL;
  uint64_t scan_key = 0;
  uint64_t scan_partition = 0;
  uint32_t part_state = PPS_FIN;

  if (pps.state == PPS_GETPARTBYSUPPLIER2) {
    scan_index = _wl->i_supplies;
    scan_key = pps.supplier_key;
    scan_partition = suppliers_to_partition(scan_key);
    part_state = PPS_GETPARTBYSUPPLIER4;
  } else if (pps.state == PPS_GETPARTBYPRODUCT2) {
    scan_index = _wl->i_uses;
    scan_key = pps.product_key;
    scan_partition = products_to_partition(scan_key);
    part_state = PPS_GETPARTBYPRODUCT4;
  } else if (pps.state == PPS_ORDERPRODUCT2) {
    scan_index = _wl->i_uses;
    scan_key = pps.product_key;
    scan_partition = products_to_partition(scan_key);
    part_state = PPS_ORDERPRODUCT4;
  }

  if (scan_index != NULL && GET_NODE_ID(scan_partition) == g_node_id) {
    itemid_t *item = index_read(scan_index, scan_key, scan_partition,
                                static_cast<int>(pps.scan_index));
    if (item == NULL) {
      pps.state = part_state;
      pps.part_index = 0;
    }
  }

  if ((pps.state == PPS_GETPARTBYSUPPLIER4 ||
       pps.state == PPS_GETPARTBYPRODUCT4 ||
       pps.state == PPS_ORDERPRODUCT4) &&
      pps.part_index >= pps.part_keys.size())
    pps.state = PPS_FIN;
}

bool PPSTxnManager::life_program_done(
    const LifeTxnDescriptor &descriptor) const {
  assert(descriptor.workload == LifeWorkloadKind::Pps);
  assert(descriptor.pps.program_index <= descriptor.history.size());
  if (descriptor.pps.state == PPS_FIN)
    assert(descriptor.pps.program_index == descriptor.history.size());
  return descriptor.pps.state == PPS_FIN;
}

LifeOperation PPSTxnManager::life_current_operation(
    LifeTxnDescriptor &descriptor) const {
  const LifePpsSnapshot &pps = descriptor.pps;
  assert(descriptor.workload == LifeWorkloadKind::Pps);
  assert(pps.program_index == descriptor.history.size());
  const uint32_t state = pps.state;

  if (state == PPS_GETPART0)
    return make_life_pps_operation(_wl->t_parts->get_table_id(),
        parts_to_partition(pps.part_key), pps.part_key,
        LifeOperationKind::ReadField, 0);
  if (state == PPS_GETPRODUCT0)
    return make_life_pps_operation(_wl->t_products->get_table_id(),
        products_to_partition(pps.product_key), pps.product_key,
        LifeOperationKind::ReadField, 0);
  if (state == PPS_GETSUPPLIER0)
    return make_life_pps_operation(_wl->t_suppliers->get_table_id(),
        suppliers_to_partition(pps.supplier_key), pps.supplier_key,
        LifeOperationKind::ReadField, 0);

  if (state == PPS_GETPARTBYSUPPLIER0)
    return make_life_pps_operation(_wl->t_suppliers->get_table_id(),
        suppliers_to_partition(pps.supplier_key), pps.supplier_key,
        LifeOperationKind::ReadField, 0);
  if (state == PPS_GETPARTBYPRODUCT0 || state == PPS_ORDERPRODUCT0)
    return make_life_pps_operation(_wl->t_products->get_table_id(),
        products_to_partition(pps.product_key), pps.product_key,
        LifeOperationKind::ReadField, 0);

  if (state == PPS_GETPARTBYSUPPLIER2)
    return make_life_pps_operation(_wl->t_supplies->get_table_id(),
        suppliers_to_partition(pps.supplier_key),
        pps_relation_row_id(pps.supplier_key, pps.scan_index),
        LifeOperationKind::ReadField, 1);
  if (state == PPS_GETPARTBYPRODUCT2 || state == PPS_ORDERPRODUCT2)
    return make_life_pps_operation(_wl->t_uses->get_table_id(),
        products_to_partition(pps.product_key),
        pps_relation_row_id(pps.product_key, pps.scan_index),
        LifeOperationKind::ReadField, 1);

  if (state == PPS_GETPARTBYSUPPLIER4 ||
      state == PPS_GETPARTBYPRODUCT4) {
    const uint64_t key = pps.part_keys[pps.part_index];
    return make_life_pps_operation(_wl->t_parts->get_table_id(),
        parts_to_partition(key), key, LifeOperationKind::ReadField, 0);
  }
  if (state == PPS_ORDERPRODUCT4) {
    const uint64_t key = pps.part_keys[pps.part_index];
    return make_life_pps_operation(_wl->t_parts->get_table_id(),
        parts_to_partition(key), key, LifeOperationKind::AddInt64,
        PART_AMOUNT, -1);
  }
  if (state == PPS_UPDATEPRODUCTPART0)
    return make_life_pps_operation(_wl->t_uses->get_table_id(),
        products_to_partition(pps.product_key),
        pps_relation_row_id(pps.product_key, 0),
        LifeOperationKind::WriteField, 1,
        static_cast<int64_t>(pps.part_key));
  if (state == PPS_UPDATEPART0)
    return make_life_pps_operation(_wl->t_parts->get_table_id(),
        parts_to_partition(pps.part_key), pps.part_key,
        LifeOperationKind::AddInt64, 1, 100);

  assert(false);
  return LifeOperation();
}

LifeExecuteResult PPSTxnManager::execute_life_operation(
    LifeTxnDescriptor &descriptor, LifeOperation &operation) {
  operation = life_current_operation(descriptor);
  row_t *life_row = lookup_life_row(operation.object);
  operation.manager = life_row->manager;
  return life_row->execute_life(descriptor, operation);
}

void PPSTxnManager::life_advance_program(LifeTxnDescriptor &descriptor,
                                         const LifeOperation &operation,
                                         const LifeResponse &response) {
  LifeHistoryEntry entry;
  entry.operation = operation;
  entry.response = response;
  life_append_history(descriptor, entry);
  life_reconcile_descriptor(descriptor);
}

uint64_t PPSTxnManager::life_program_position(
    const LifeTxnDescriptor &descriptor) const {
  return descriptor.pps.program_index;
}

bool PPSTxnManager::life_program_present(
    const LifeTxnDescriptor &descriptor) const {
  return descriptor.workload == LifeWorkloadKind::Pps;
}

void PPSTxnManager::life_reset_program(LifeTxnDescriptor &descriptor) {
  descriptor.pps.state = pps_initial_life_state(descriptor.pps.txn_type);
  descriptor.pps.scan_index = 0;
  descriptor.pps.part_index = 0;
  descriptor.pps.program_index = 0;
  descriptor.pps.part_keys.clear();
}

void PPSTxnManager::life_copy_program_to_workload(
    const LifeTxnDescriptor &descriptor) {
  const LifePpsSnapshot &pps = descriptor.pps;
  PPSQuery *pps_query = static_cast<PPSQuery *>(query);
  state = static_cast<PPSRemTxnType>(pps.state);
  parts_processed_count = pps.scan_index;
  life_part_index = pps.part_index;
  pps_query->part_keys.clear();
  for (size_t i = 0; i < pps.part_keys.size(); ++i)
    pps_query->part_keys.add(pps.part_keys[i]);
}

void PPSTxnManager::life_copy_remote_program(
    LifeTxnDescriptor &destination, const LifeTxnDescriptor &source) {
  assert(destination.workload == LifeWorkloadKind::Pps);
  assert(source.workload == LifeWorkloadKind::Pps);
  assert(source.pps.program_index == destination.history.size());
  destination.workload = source.workload;
  destination.pps = source.pps;
}

uint64_t PPSTxnManager::life_remote_batch_stop(
    const LifeTxnDescriptor &, uint64_t) const {
  return UINT64_MAX;
}
#endif



RC PPSTxnManager::run_calvin_txn() {
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  PPSQuery* pps_query = (PPSQuery*) query;
  DEBUG("(%ld,%ld) Run calvin txn %d (recon: %d)\n",txn->txn_id,txn->batch_id,pps_query->txn_type,isRecon());
  while(!calvin_exec_phase_done() && rc == RCOK) {
    DEBUG("(%ld,%ld) phase %d\n",txn->txn_id,txn->batch_id,this->phase);
    switch(this->phase) {
      case CALVIN_RW_ANALYSIS:
        // Phase 1: Read/write set analysis
        calvin_expected_rsp_cnt = pps_query->get_participants(_wl);
        if(query->participant_nodes[g_node_id] == 1) {
          calvin_expected_rsp_cnt--;
        }

        DEBUG("(%ld,%ld) expects %d responses; %ld participants, %ld active\n",txn->txn_id,txn->batch_id,calvin_expected_rsp_cnt,query->participant_nodes.size(),query->active_nodes.size());

        this->phase = CALVIN_LOC_RD;
        break;
      case CALVIN_LOC_RD:
        // Phase 2: Perform local reads
        DEBUG("(%ld,%ld) local reads\n",txn->txn_id,txn->batch_id);
        rc = run_pps_phase2();
        //release_read_locks(pps_query);
        if (isRecon()) {
            this->phase = CALVIN_DONE;
        }
        else {
            this->phase = CALVIN_SERVE_RD;
        }

        break;
      case CALVIN_SERVE_RD:
        // Phase 3: Serve remote reads
        if(query->participant_nodes[g_node_id] == 1) {
          rc = send_remote_reads();
        }
        if(query->active_nodes[g_node_id] == 1) {
          this->phase = CALVIN_COLLECT_RD;
          if(calvin_collect_phase_done()) {
            rc = RCOK;
          } else {
            assert(calvin_expected_rsp_cnt > 0);
            DEBUG("(%ld,%ld) wait in collect phase; %d / %d rfwds received\n",txn->txn_id,txn->batch_id,rsp_cnt,calvin_expected_rsp_cnt);
            rc = WAIT;
          }
        } else { // Done
          rc = RCOK;
          this->phase = CALVIN_DONE;
        }
        break;
      case CALVIN_COLLECT_RD:
        // Phase 4: Collect remote reads
        this->phase = CALVIN_EXEC_WR;
        break;
      case CALVIN_EXEC_WR:
        // Phase 5: Execute transaction / perform local writes
        DEBUG("(%ld,%ld) execute writes\n",txn->txn_id,txn->batch_id);
        if (txn->rc == RCOK) {
            rc = run_pps_phase5();
        }
        this->phase = CALVIN_DONE;
        break;
      default:
        assert(false);
    }
  }
  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  txn_stats.wait_starttime = get_sys_clock();
  return rc;
  
}


RC PPSTxnManager::run_pps_phase2() {
    PPSQuery* pps_query = (PPSQuery*) query;
    RC rc = RCOK;
    uint64_t part_key = pps_query->part_key;
    uint64_t product_key = pps_query->product_key;
    uint64_t supplier_key = pps_query->supplier_key;
    bool part_loc = GET_NODE_ID(parts_to_partition(part_key)) == g_node_id;
    bool product_loc = GET_NODE_ID(products_to_partition(product_key)) == g_node_id;
    bool supplier_loc = GET_NODE_ID(suppliers_to_partition(supplier_key)) == g_node_id;
    assert(CC_ALG == CALVIN);

	switch (pps_query->txn_type) {
    case PPS_GETPART:
        if (part_loc) {
            rc = run_getpart_0(part_key, row);
            rc = run_getpart_1(row);
        }
        break;
    case PPS_GETSUPPLIER:
        if (supplier_loc) {
            rc = run_getsupplier_0(supplier_key, row);
            rc = run_getsupplier_1(row);
        }
        break;
    case PPS_GETPRODUCT:
        if (product_loc) {
            rc = run_getproduct_0(product_key, row);
            rc = run_getproduct_1(row);
        }
        break;
    case PPS_GETPARTBYSUPPLIER:
        if (supplier_loc) {
            rc = run_getpartsbysupplier_0(supplier_key, row);
            rc = run_getpartsbysupplier_1(row);
            rc = run_getpartsbysupplier_2(supplier_key, row);
            while (row != NULL) {
                ++parts_processed_count;
                rc = run_getpartsbysupplier_3(part_key, row);
                if (isRecon()) {
                    pps_query->part_keys.add(part_key);
                }
                else {
                    break;
                    // check if the parts have changed since we did recon
                    if (!pps_query->part_keys.contains(part_key)) {
                        txn->rc = Abort;
                        break;
                    }
                }
                rc = run_getpartsbysupplier_2(supplier_key, row);
            }

        }
        if (txn->rc == Abort) {
            break;
        }
        if (!isRecon()) {
            for (uint64_t key = 0; key < pps_query->part_keys.size();key++) {
                part_key = pps_query->part_keys[key];
                part_loc = GET_NODE_ID(parts_to_partition(part_key)) == g_node_id;
                if (part_loc) {
                    rc = run_getpartsbysupplier_4(part_key, row);
                    rc = run_getpartsbysupplier_5(row);
                }
            }
        }
        break;
    case PPS_GETPARTBYPRODUCT:
        if (product_loc) {
            rc = run_getpartsbyproduct_0(product_key, row);
            rc = run_getpartsbyproduct_1(row);
            rc = run_getpartsbyproduct_2(product_key, row);
            while (row != NULL) {
                ++parts_processed_count;
                rc = run_getpartsbyproduct_3(part_key, row);
                if (isRecon()) {
                    pps_query->part_keys.add(part_key);
                }
                else {
                    // check if the parts have changed since we did recon
                    if (!pps_query->part_keys.contains(part_key)) {
                        txn->rc = Abort;
                        break;
                    }
                }
                rc = run_getpartsbyproduct_2(product_key, row);
            }

        }
        if (txn->rc == Abort) {
            break;
        }
        if (!isRecon()) {
            for (uint64_t key = 0; key < pps_query->part_keys.size();key++) {
                part_key = pps_query->part_keys[key];
                part_loc = GET_NODE_ID(parts_to_partition(part_key)) == g_node_id;
                if (part_loc) {
                    rc = run_getpartsbyproduct_4(part_key, row);
                    rc = run_getpartsbyproduct_5(row);
                }
            }
        }
        break;
    case PPS_ORDERPRODUCT:
        if (product_loc) {
            rc = run_orderproduct_0(product_key, row);
            rc = run_orderproduct_1(row);
            rc = run_orderproduct_2(product_key, row);
            while (row != NULL) {
                ++parts_processed_count;
                rc = run_orderproduct_3(part_key, row);
                if (isRecon()) {
                    pps_query->part_keys.add(part_key);
                }
                else {
                    // check if the parts have changed since we did recon
                    if (!pps_query->part_keys.contains(part_key)) {
                        txn->rc = Abort;
                        break;
                    }
                }
                rc = run_orderproduct_2(product_key, row);
            }

        }
        break;
    case PPS_UPDATEPRODUCTPART:
        break;
    case PPS_UPDATEPART:
        break;
    default: assert(false);
    }
    return rc;
}

RC PPSTxnManager::run_pps_phase5() {
    PPSQuery* pps_query = (PPSQuery*) query;
    RC rc = RCOK;
    uint64_t part_key = pps_query->part_key;
    uint64_t product_key = pps_query->product_key;
    //uint64_t supplier_key = pps_query->supplier_key;
    bool part_loc = GET_NODE_ID(parts_to_partition(part_key)) == g_node_id;
    bool product_loc = GET_NODE_ID(products_to_partition(product_key)) == g_node_id;
    //bool supplier_loc = GET_NODE_ID(suppliers_to_partition(supplier_key)) == g_node_id;
    assert(CC_ALG == CALVIN);

  switch (pps_query->txn_type) {
  case PPS_GETPART:
      break;
  case PPS_GETSUPPLIER:
      break;
  case PPS_GETPRODUCT:
      break;
  case PPS_GETPARTBYSUPPLIER:
      break;
  case PPS_GETPARTBYPRODUCT:
      break;
  case PPS_ORDERPRODUCT:
      for (uint64_t key = 0; key < pps_query->part_keys.size();key++) {
          part_key = pps_query->part_keys[key];
          part_loc = GET_NODE_ID(parts_to_partition(part_key)) == g_node_id;
          if (part_loc) {
              rc = run_orderproduct_4(part_key, row);
              rc = run_orderproduct_5(row);
          }
      }
      break;
  case PPS_UPDATEPRODUCTPART:
      if (product_loc) {

          DEBUG("UPDATE Product %ld Part %ld\n",product_key,part_key);
          rc = run_updateproductpart_0(product_key, row);
          rc = run_updateproductpart_1(part_key, row);
      }
      break;
  case PPS_UPDATEPART:
      if (part_loc) {
          rc = run_updatepart_0(part_key, row);
          rc = run_updatepart_1(row);
      }
      break;
  default: assert(false);
  }
  return rc;

}

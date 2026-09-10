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

#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_const.h"
#include "transport.h"
#include "msg_queue.h"
#include "message.h"
#if CC_ALG == LIFE
#include "mem_alloc.h"
#include "row_life.h"
#endif
#include <algorithm>
#include <cstring>

#if CC_ALG == LIFE
TPCCTxnManager::TPCCTxnManager()
    : _wl(NULL), _rc(RCOK), row(NULL), next_item_id(0) {}
#endif

void TPCCTxnManager::init(uint64_t thd_id, Workload * h_wl) {
#if CC_ALG == LIFE
	YCSBTxnManager::init(thd_id, h_wl);
#else
	TxnManager::init(thd_id, h_wl);
#endif
	_wl = (TPCCWorkload *) h_wl;
  reset();
#if CC_ALG != LIFE
	TxnManager::reset();
#endif
}

void TPCCTxnManager::reset() {
#if CC_ALG == LIFE
  discard_all_life_staged_inserts();
#endif
  TPCCQuery* tpcc_query = (TPCCQuery*) query;
  state = TPCC_PAYMENT0;
  if(tpcc_query->txn_type == TPCC_PAYMENT) {
    state = TPCC_PAYMENT0;
  } else if (tpcc_query->txn_type == TPCC_NEW_ORDER) {
    state = TPCC_NEWORDER0;
  }
  next_item_id = 0;
#if CC_ALG == LIFE
  YCSBTxnManager::reset();
#else
	TxnManager::reset();
#endif
}

RC TPCCTxnManager::run_txn_post_wait() {
    get_row_post_wait(row);
    next_tpcc_state();
    return RCOK;
}


RC TPCCTxnManager::run_txn() {
#if MODE == SETUP_MODE
  return RCOK;
#endif
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();

#if CC_ALG == CALVIN
  rc = run_calvin_txn();
  return rc;
#endif

  if(IS_LOCAL(txn->txn_id) && (state == TPCC_PAYMENT0 || state == TPCC_NEWORDER0)) {
    DEBUG("Running txn %ld\n",txn->txn_id);
#if DISTR_DEBUG
    query->print();
#endif
    query->partitions_touched.add_unique(
        wh_to_part(static_cast<TPCCQuery *>(query)->w_id));
  }

#if CC_ALG == LIFE
  return run_life_txn();
#endif

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

bool TPCCTxnManager::is_done() {
  bool done = false;
  TPCCQuery* tpcc_query = (TPCCQuery*) query;
  switch(tpcc_query->txn_type) {
    case TPCC_PAYMENT:
      //done = state == TPCC_PAYMENT5 || state == TPCC_FIN;
      done = state == TPCC_FIN;
      break;
    case TPCC_NEW_ORDER:
      //done = next_item_id == tpcc_query->ol_cnt || state == TPCC_FIN;
      done = next_item_id == tpcc_query->items.size() || state == TPCC_FIN;
      break;
    default: assert(false);
  }

  
  return done;
}

RC TPCCTxnManager::acquire_locks() {
  uint64_t starttime = get_sys_clock();
  assert(CC_ALG == CALVIN);
  locking_done = false;
  RC rc = RCOK;
  RC rc2;
  INDEX * index;
  itemid_t * item;
  row_t* row;
  uint64_t key;
  incr_lr();
  TPCCQuery* tpcc_query = (TPCCQuery*) query;

	uint64_t w_id = tpcc_query->w_id;
  uint64_t d_id = tpcc_query->d_id;
  uint64_t c_id = tpcc_query->c_id;
  uint64_t d_w_id = tpcc_query->d_w_id;
  uint64_t c_w_id = tpcc_query->c_w_id;
  uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
  uint64_t part_id_w = wh_to_part(w_id);
  uint64_t part_id_c_w = wh_to_part(c_w_id);
  switch(tpcc_query->txn_type) {
    case TPCC_PAYMENT:
      if(GET_NODE_ID(part_id_w) == g_node_id) {
      // WH
        index = _wl->i_warehouse;
        item = index_read(index, w_id, part_id_w);
        row_t * row = ((row_t *)item->location);
        rc2 = get_lock(row,g_wh_update? WR:RD);
        if(rc2 != RCOK)
          rc = rc2;

      // Dist
        key = distKey(d_id, d_w_id);
        item = index_read(_wl->i_district, key, part_id_w);
        row = ((row_t *)item->location);
        rc2 = get_lock(row, WR);
        if(rc2 != RCOK)
          rc = rc2;
      }
      if(GET_NODE_ID(part_id_c_w) == g_node_id) {
      // Cust
        if (tpcc_query->by_last_name) { 

          key = custNPKey(c_last, c_d_id, c_w_id);
          index = _wl->i_customer_last;
          item = index_read(index, key, part_id_c_w);
          int cnt = 0;
          itemid_t * it = item;
          itemid_t * mid = item;
          while (it != NULL) {
            cnt ++;
            it = it->next;
            if (cnt % 2 == 0)
              mid = mid->next;
          }
          row = ((row_t *)mid->location);
          
        }
        else { 
          key = custKey(c_id, c_d_id, c_w_id);
          index = _wl->i_customer_id;
          item = index_read(index, key, part_id_c_w);
          row = (row_t *) item->location;
        }
        rc2  = get_lock(row, WR);
        if(rc2 != RCOK)
          rc = rc2;
 
      }
      break;
    case TPCC_NEW_ORDER:
      if(GET_NODE_ID(part_id_w) == g_node_id) {
      // WH
        index = _wl->i_warehouse;
        item = index_read(index, w_id, part_id_w);
        row_t * row = ((row_t *)item->location);
        rc2 = get_lock(row,RD);
        if(rc2 != RCOK)
          rc = rc2;
      // Cust
        index = _wl->i_customer_id;
        key = custKey(c_id, d_id, w_id);
        item = index_read(index, key, wh_to_part(w_id));
        row = (row_t *) item->location;
        rc2 = get_lock(row, RD);
        if(rc2 != RCOK)
          rc = rc2;
      // Dist
        key = distKey(d_id, w_id);
        item = index_read(_wl->i_district, key, wh_to_part(w_id));
        row = ((row_t *)item->location);
        rc2 = get_lock(row, WR);
        if(rc2 != RCOK)
          rc = rc2;
      }
      // Items
        for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {
          if(GET_NODE_ID(wh_to_part(tpcc_query->items[i]->ol_supply_w_id)) != g_node_id) 
            continue;

          key = tpcc_query->items[i]->ol_i_id;
          item = index_read(_wl->i_item, key, 0);
          row = ((row_t *)item->location);
          rc2 = get_lock(row, RD);
          if(rc2 != RCOK)
            rc = rc2;
          key = stockKey(tpcc_query->items[i]->ol_i_id, tpcc_query->items[i]->ol_supply_w_id);
          index = _wl->i_stock;
          item = index_read(index, key, wh_to_part(tpcc_query->items[i]->ol_supply_w_id));
          row = ((row_t *)item->location);
          rc2 = get_lock(row, WR);
          if(rc2 != RCOK)
            rc = rc2;
        }
      break;
    default: assert(false);
  }
  if(decr_lr() == 0) {
    if(ATOM_CAS(lock_ready,false,true))
      rc = RCOK;
  }
  txn_stats.wait_starttime = get_sys_clock();
  locking_done = true;
  INC_STATS(get_thd_id(),calvin_sched_time,get_sys_clock() - starttime);
  return rc;
}


void TPCCTxnManager::next_tpcc_state() {
  //TPCCQuery* tpcc_query = (TPCCQuery*) query;

  switch(state) {
    case TPCC_PAYMENT_S:
      state = TPCC_PAYMENT0;
      break;
    case TPCC_PAYMENT0:
      state = TPCC_PAYMENT1;
      break;
    case TPCC_PAYMENT1:
      state = TPCC_PAYMENT2;
      break;
    case TPCC_PAYMENT2:
      state = TPCC_PAYMENT3;
      break;
    case TPCC_PAYMENT3:
      state = TPCC_PAYMENT4;
      break;
    case TPCC_PAYMENT4:
      state = TPCC_PAYMENT5;
      break;
    case TPCC_PAYMENT5:
      state = TPCC_FIN;
      break;
    case TPCC_NEWORDER_S:
      state = TPCC_NEWORDER0;
      break;
    case TPCC_NEWORDER0:
      state = TPCC_NEWORDER1;
      break;
    case TPCC_NEWORDER1:
      state = TPCC_NEWORDER2;
      break;
    case TPCC_NEWORDER2:
      state = TPCC_NEWORDER3;
      break;
    case TPCC_NEWORDER3:
      state = TPCC_NEWORDER4;
      break;
    case TPCC_NEWORDER4:
      state = TPCC_NEWORDER5;
      break;
    case TPCC_NEWORDER5:
      if(!IS_LOCAL(txn->txn_id) || !is_done()) {
        state = TPCC_NEWORDER6;
      }
      else {
        state = TPCC_FIN;
      }
      break;
    case TPCC_NEWORDER6: // loop pt 1
      state = TPCC_NEWORDER7;
      break;
    case TPCC_NEWORDER7:
      state = TPCC_NEWORDER8;
      break;
    case TPCC_NEWORDER8: // loop pt 2
      state = TPCC_NEWORDER9;
      break;
    case TPCC_NEWORDER9:
      ++next_item_id;
      if(!IS_LOCAL(txn->txn_id) || !is_done()) {
        state = TPCC_NEWORDER6;
      }
      else {
        state = TPCC_FIN;
      }
      break;
    case TPCC_FIN:
      break;
    default:
      assert(false);
  }

}

bool TPCCTxnManager::is_local_item(uint64_t idx) {
  TPCCQuery* tpcc_query = (TPCCQuery*) query;
	uint64_t ol_supply_w_id = tpcc_query->items[idx]->ol_supply_w_id;
  uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
  return GET_NODE_ID(part_id_ol_supply_w) == g_node_id;
}


RC TPCCTxnManager::send_remote_request() {
  assert(IS_LOCAL(get_txn_id()));
  TPCCQuery* tpcc_query = (TPCCQuery*) query;
  TPCCRemTxnType next_state = TPCC_FIN;
	uint64_t w_id = tpcc_query->w_id;
  uint64_t c_w_id = tpcc_query->c_w_id;
  uint64_t dest_node_id = UINT64_MAX;
  if(state == TPCC_PAYMENT0) {
    dest_node_id = GET_NODE_ID(wh_to_part(w_id));
    next_state = TPCC_PAYMENT2;
  } else if(state == TPCC_PAYMENT4) {
    dest_node_id = GET_NODE_ID(wh_to_part(c_w_id));
    next_state = TPCC_FIN;
  } else if(state == TPCC_NEWORDER0) {
    dest_node_id = GET_NODE_ID(wh_to_part(w_id));
    next_state = TPCC_NEWORDER6;
  } else if(state == TPCC_NEWORDER8) {
    dest_node_id = GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
    /*
    while(GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id)) != dest_node_id) {
      msg->items.add(tpcc_query->items[next_item_id++]);
    }
    */
    if(is_done())
      next_state = TPCC_FIN;
    else 
      next_state = TPCC_NEWORDER6;
  } else {
    assert(false);
  }
  TPCCQueryMessage * msg = (TPCCQueryMessage*)Message::create_message(this,RQRY);
  msg->state = state;
  query->partitions_touched.add_unique(GET_PART_ID(0,dest_node_id));
  msg_queue.enqueue(get_thd_id(),msg,dest_node_id);
  state = next_state;
  return WAIT_REM;
}

void TPCCTxnManager::copy_remote_items(TPCCQueryMessage * msg) {
  TPCCQuery* tpcc_query = (TPCCQuery*) query;
  msg->items.init(tpcc_query->items.size());
  if(tpcc_query->txn_type == TPCC_PAYMENT)
    return;
  uint64_t dest_node_id = GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id));
  while(next_item_id < tpcc_query->items.size() && !is_local_item(next_item_id) && GET_NODE_ID(wh_to_part(tpcc_query->items[next_item_id]->ol_supply_w_id)) == dest_node_id) {
    Item_no * req = (Item_no*) mem_allocator.alloc(sizeof(Item_no));
    req->copy(tpcc_query->items[next_item_id++]);
    msg->items.add(req);
  }
}


RC TPCCTxnManager::run_txn_state() {
    TPCCQuery* tpcc_query = (TPCCQuery*) query;
    uint64_t w_id = tpcc_query->w_id;
    uint64_t d_id = tpcc_query->d_id;
    uint64_t c_id = tpcc_query->c_id;
    uint64_t d_w_id = tpcc_query->d_w_id;
    uint64_t c_w_id = tpcc_query->c_w_id;
    uint64_t c_d_id = tpcc_query->c_d_id;
    char * c_last = tpcc_query->c_last;
    double h_amount = tpcc_query->h_amount;
    bool by_last_name = tpcc_query->by_last_name;
    bool remote = tpcc_query->remote;
    uint64_t ol_cnt = tpcc_query->ol_cnt;
    uint64_t o_entry_d = tpcc_query->o_entry_d;
    uint64_t ol_i_id = 0;
    uint64_t ol_supply_w_id = 0;
    uint64_t ol_quantity = 0;
    if(tpcc_query->txn_type == TPCC_NEW_ORDER) {
        ol_i_id = tpcc_query->items[next_item_id]->ol_i_id;
        ol_supply_w_id = tpcc_query->items[next_item_id]->ol_supply_w_id;
        ol_quantity = tpcc_query->items[next_item_id]->ol_quantity;
    }
    uint64_t ol_number = next_item_id;
    uint64_t ol_amount = tpcc_query->ol_amount;
    uint64_t o_id = tpcc_query->o_id;

    uint64_t part_id_w = wh_to_part(w_id);
    uint64_t part_id_c_w = wh_to_part(c_w_id);
    uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
    bool w_loc = GET_NODE_ID(part_id_w) == g_node_id;
    bool c_w_loc = GET_NODE_ID(part_id_c_w) == g_node_id;
    bool ol_supply_w_loc = GET_NODE_ID(part_id_ol_supply_w) == g_node_id;

	RC rc = RCOK;

	switch (state) {
		case TPCC_PAYMENT0 :
            if(w_loc)
                    rc = run_payment_0(w_id, d_id, d_w_id, h_amount, row);
            else {
              rc = send_remote_request();
            }
            break;
		case TPCC_PAYMENT1 :
            rc = run_payment_1(w_id, d_id, d_w_id, h_amount, row);
            break;
		case TPCC_PAYMENT2 :
            rc = run_payment_2(w_id, d_id, d_w_id, h_amount, row);
            break;
		case TPCC_PAYMENT3 :
            rc = run_payment_3(w_id, d_id, d_w_id, h_amount, row);
            break;
		case TPCC_PAYMENT4 :
            if(c_w_loc)
                rc = run_payment_4( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row);
            else {
                rc = send_remote_request();
            }
            break;
		case TPCC_PAYMENT5 :
            rc = run_payment_5( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row);
            break;
		case TPCC_NEWORDER0 :
            if(w_loc)
                rc = new_order_0( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
            else {
                rc = send_remote_request();
            }
			break;
		case TPCC_NEWORDER1 :
            rc = new_order_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
            break;
		case TPCC_NEWORDER2 :
            rc = new_order_2( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
            break;
		case TPCC_NEWORDER3 :
            rc = new_order_3( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
            break;
		case TPCC_NEWORDER4 :
            rc = new_order_4( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
            break;
		case TPCC_NEWORDER5 :
            rc = new_order_5( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
            break;
		case TPCC_NEWORDER6 :
			rc = new_order_6(ol_i_id, row);
			break;
		case TPCC_NEWORDER7 :
			rc = new_order_7(ol_i_id, row);
			break;
		case TPCC_NEWORDER8 :
		      if(ol_supply_w_loc) {
                  rc = new_order_8( w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity,  ol_number, o_id, row);
		      }
		      else {
                  rc = send_remote_request();
		      }
              break;
		case TPCC_NEWORDER9 :
            rc = new_order_9( w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity,  ol_number, ol_amount, o_id, row);
            break;
    case TPCC_FIN :
        state = TPCC_FIN;
        if(tpcc_query->rbk)
            return Abort;
            //return finish(tpcc_query,false);
        break;
    default:
        assert(false);
	}

  if(rc == RCOK)
    next_tpcc_state();
  return rc;
}

inline RC TPCCTxnManager::run_payment_0(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t *& r_wh_local) {

	uint64_t key;
	itemid_t * item;
/*====================================================+
    	EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
		WHERE w_id=:w_id;
	+====================================================*/
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/


  RC rc;
	key = w_id;
	INDEX * index = _wl->i_warehouse; 
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	if (g_wh_update)
		rc = get_row(r_wh, WR, r_wh_local);
	else 
		rc = get_row(r_wh, RD, r_wh_local);

  return rc;
}

inline RC TPCCTxnManager::run_payment_1(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t * r_wh_local) {

  assert(r_wh_local != NULL);
/*====================================================+
    	EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
		WHERE w_id=:w_id;
	+====================================================*/
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/


	double w_ytd;
	r_wh_local->get_value(W_YTD, w_ytd);
	if (g_wh_update) {
		r_wh_local->set_value(W_YTD, w_ytd + h_amount);
	}
  return RCOK;
}

inline RC TPCCTxnManager::run_payment_2(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t *& r_dist_local) {
	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	uint64_t key;
	itemid_t * item;
	key = distKey(d_id, d_w_id);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	RC rc = get_row(r_dist, WR, r_dist_local);
  return rc;

}

inline RC TPCCTxnManager::run_payment_3(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t * r_dist_local) {
  assert(r_dist_local != NULL);

	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	double d_ytd;
	r_dist_local->get_value(D_YTD, d_ytd);
	r_dist_local->set_value(D_YTD, d_ytd + h_amount);

	return RCOK;
}

inline RC TPCCTxnManager::run_payment_4(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, double h_amount, bool by_last_name, row_t *& r_cust_local) { 
	/*====================================================================+
		EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
		INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
		FROM district
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+====================================================================*/

	itemid_t * item;
	uint64_t key;
	row_t * r_cust;
	if (by_last_name) { 
		/*==========================================================+
			EXEC SQL SELECT count(c_id) INTO :namecnt
			FROM customer
			WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
		+==========================================================*/
		/*==========================================================================+
			EXEC SQL DECLARE c_byname CURSOR FOR
			SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
			c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
			ORDER BY c_first;
			EXEC SQL OPEN c_byname;
		+===========================================================================*/

		key = custNPKey(c_last, c_d_id, c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted... 
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		item = index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);
		
		/*============================================================================+
			for (n=0; n<namecnt/2; n++) {
				EXEC SQL FETCH c_byname
				INTO :c_first, :c_middle, :c_id,
					 :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
					 :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
			}
			EXEC SQL CLOSE c_byname;
		+=============================================================================*/
		// XXX: we don't retrieve all the info, just the tuple we are interested in
	}

	else { // search customers by cust_id
		/*=====================================================================+
			EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
			c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
			c_discount, c_balance, c_since
			INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
			:c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
			:c_discount, :c_balance, :c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+======================================================================*/
		key = custKey(c_id, c_d_id, c_w_id);
		INDEX * index = _wl->i_customer_id;
		item = index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		r_cust = (row_t *) item->location;
	}

  	/*======================================================================+
	   	EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
   		WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
   	+======================================================================*/
	RC rc  = get_row(r_cust, WR, r_cust_local);

  return rc;
}


inline RC TPCCTxnManager::run_payment_5(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, double h_amount, bool by_last_name, row_t * r_cust_local) { 
  assert(r_cust_local != NULL);
	double c_balance;
	double c_ytd_payment;
	uint64_t c_payment_cnt;

	r_cust_local->get_value(C_BALANCE, c_balance);
	r_cust_local->set_value(C_BALANCE, c_balance - h_amount);
	r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
	r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + h_amount);
	r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
	r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

	//char * c_credit = r_cust_local->get_value(C_CREDIT);

	/*=============================================================================+
	  EXEC SQL INSERT INTO
	  history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
	  VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
	  +=============================================================================*/
	row_t * r_hist;
	uint64_t row_id;
	// Which partition should we be inserting into?
	_wl->t_history->get_new_row(r_hist, wh_to_part(c_w_id), row_id);
	r_hist->set_value(H_C_ID, c_id);
	r_hist->set_value(H_C_D_ID, c_d_id);
	r_hist->set_value(H_C_W_ID, c_w_id);
	r_hist->set_value(H_D_ID, d_id);
	r_hist->set_value(H_W_ID, w_id);
	int64_t date = 2013;		
	r_hist->set_value(H_DATE, date);
	r_hist->set_value(H_AMOUNT, h_amount);
	insert_row(r_hist, _wl->t_history);

	return RCOK;
}



// new_order 0
inline RC TPCCTxnManager::new_order_0(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_wh_local) {
	uint64_t key;
	itemid_t * item;
	/*=======================================================================+
	EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
		FROM customer, warehouse
		WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
	+========================================================================*/
	key = w_id;
	INDEX * index = _wl->i_warehouse; 
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
  RC rc = get_row(r_wh, RD, r_wh_local);
  return rc;
}

inline RC TPCCTxnManager::new_order_1(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_wh_local) {
  assert(r_wh_local != NULL);
	double w_tax;
	r_wh_local->get_value(W_TAX, w_tax); 
  return RCOK;
}

inline RC TPCCTxnManager::new_order_2(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_cust_local) {
	uint64_t key;
	itemid_t * item;
	key = custKey(c_id, d_id, w_id);
	INDEX * index = _wl->i_customer_id;
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_cust = (row_t *) item->location;
  RC rc = get_row(r_cust, RD, r_cust_local);
  return rc;
}

inline RC TPCCTxnManager::new_order_3(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_cust_local) {
  assert(r_cust_local != NULL);
	uint64_t c_discount;
	//char * c_last;
	//char * c_credit;
	r_cust_local->get_value(C_DISCOUNT, c_discount);
	//c_last = r_cust_local->get_value(C_LAST);
	//c_credit = r_cust_local->get_value(C_CREDIT);
  return RCOK;
}
 	
inline RC TPCCTxnManager::new_order_4(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_dist_local) {
	uint64_t key;
	itemid_t * item;
	/*==================================================+
	EXEC SQL SELECT d_next_o_id, d_tax
		INTO :d_next_o_id, :d_tax
		FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
	EXEC SQL UPDATE d istrict SET d _next_o_id = :d _next_o_id + 1
		WH ERE d _id = :d_id AN D d _w _id = :w _id ;
	+===================================================*/
	key = distKey(d_id, w_id);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
  RC rc = get_row(r_dist, WR, r_dist_local);
  return rc;
}

inline RC TPCCTxnManager::new_order_5(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_dist_local) {
  assert(r_dist_local != NULL);
	//double d_tax;
	//int64_t o_id;
	//d_tax = *(double *) r_dist_local->get_value(D_TAX);
	*o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);
	(*o_id) ++;
	r_dist_local->set_value(D_NEXT_O_ID, *o_id);

	// return o_id
	/*========================================================================================+
	EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
	+========================================================================================*/
	row_t * r_order;
	uint64_t row_id;
	_wl->t_order->get_new_row(r_order, wh_to_part(w_id), row_id);
	r_order->set_value(O_ID, *o_id);
	r_order->set_value(O_C_ID, c_id);
	r_order->set_value(O_D_ID, d_id);
	r_order->set_value(O_W_ID, w_id);
	r_order->set_value(O_ENTRY_D, o_entry_d);
	r_order->set_value(O_OL_CNT, ol_cnt);
	int64_t all_local = (remote? 0 : 1);
	r_order->set_value(O_ALL_LOCAL, all_local);
	insert_row(r_order, _wl->t_order);
	/*=======================================================+
    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
        VALUES (:o_id, :d_id, :w_id);
    +=======================================================*/
	row_t * r_no;
	_wl->t_neworder->get_new_row(r_no, wh_to_part(w_id), row_id);
	r_no->set_value(NO_O_ID, *o_id);
	r_no->set_value(NO_D_ID, d_id);
	r_no->set_value(NO_W_ID, w_id);
	insert_row(r_no, _wl->t_neworder);

	return RCOK;
}



// new_order 1
// Read from replicated read-only item table
inline RC TPCCTxnManager::new_order_6(uint64_t ol_i_id, row_t *& r_item_local) {
		uint64_t key;
		itemid_t * item;
		/*===========================================+
		EXEC SQL SELECT i_price, i_name , i_data
			INTO :i_price, :i_name, :i_data
			FROM item
			WHERE i_id = :ol_i_id;
		+===========================================*/
		key = ol_i_id;
		item = index_read(_wl->i_item, key, 0);
		assert(item != NULL);
		row_t * r_item = ((row_t *)item->location);

    RC rc = get_row(r_item, RD, r_item_local);
    return rc;
}

inline RC TPCCTxnManager::new_order_7(uint64_t ol_i_id, row_t * r_item_local) {
  assert(r_item_local != NULL);
		int64_t i_price;
		//char * i_name;
		//char * i_data;
		
		r_item_local->get_value(I_PRICE, i_price);
		//i_name = r_item_local->get_value(I_NAME);
		//i_data = r_item_local->get_value(I_DATA);


	return RCOK;
}

// new_order 2
inline RC TPCCTxnManager::new_order_8(uint64_t w_id,uint64_t  d_id,bool remote, uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number,uint64_t  o_id, row_t *& r_stock_local) {
		uint64_t key;
		itemid_t * item;

		/*===================================================================+
		EXEC SQL SELECT s_quantity, s_data,
				s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
				s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
			INTO :s_quantity, :s_data,
				:s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
				:s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
			FROM stock
			WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
		EXEC SQL UPDATE stock SET s_quantity = :s_quantity
			WHERE s_i_id = :ol_i_id
			AND s_w_id = :ol_supply_w_id;
		+===============================================*/

		key = stockKey(ol_i_id, ol_supply_w_id);
		INDEX * index = _wl->i_stock;
		item = index_read(index, key, wh_to_part(ol_supply_w_id));
		assert(item != NULL);
		row_t * r_stock = ((row_t *)item->location);
    RC rc = get_row(r_stock, WR, r_stock_local);
    return rc;
}
		
inline RC TPCCTxnManager::new_order_9(uint64_t w_id,uint64_t  d_id,bool remote, uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number, uint64_t ol_amount, uint64_t  o_id, row_t * r_stock_local) {
  assert(r_stock_local != NULL);
		// XXX s_dist_xx are not retrieved.
		UInt64 s_quantity;
		int64_t s_remote_cnt;
		s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
#if !TPCC_SMALL
		int64_t s_ytd;
		int64_t s_order_cnt;
		char * s_data __attribute__ ((unused));
		r_stock_local->get_value(S_YTD, s_ytd);
		r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
    // In Coordination Avoidance, this record must be protected!
		r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
		r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
		s_data = r_stock_local->get_value(S_DATA);
#endif
			if (ol_supply_w_id != w_id) {
			s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
			s_remote_cnt ++;
			r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
		}
		uint64_t quantity;
		if (s_quantity > ol_quantity + 10) {
			quantity = s_quantity - ol_quantity;
		} else {
			quantity = s_quantity - ol_quantity + 91;
		}
		r_stock_local->set_value(S_QUANTITY, &quantity);

		/*====================================================+
		EXEC SQL INSERT
			INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
				ol_i_id, ol_supply_w_id,
				ol_quantity, ol_amount, ol_dist_info)
			VALUES(:o_id, :d_id, :w_id, :ol_number,
				:ol_i_id, :ol_supply_w_id,
				:ol_quantity, :ol_amount, :ol_dist_info);
		+====================================================*/
		row_t * r_ol;
		uint64_t row_id;
		_wl->t_orderline->get_new_row(r_ol, wh_to_part(ol_supply_w_id), row_id);
		r_ol->set_value(OL_O_ID, &o_id);
		r_ol->set_value(OL_D_ID, &d_id);
		r_ol->set_value(OL_W_ID, &w_id);
		r_ol->set_value(OL_NUMBER, &ol_number);
		r_ol->set_value(OL_I_ID, &ol_i_id);
#if !TPCC_SMALL
		r_ol->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
		r_ol->set_value(OL_QUANTITY, &ol_quantity);
		r_ol->set_value(OL_AMOUNT, &ol_amount);
#endif		
		insert_row(r_ol, _wl->t_orderline);

	return RCOK;
}

#if CC_ALG == LIFE
namespace {

uint64_t tpcc_double_bits(double value) {
  uint64_t bits;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

double tpcc_bits_double(uint64_t bits) {
  double value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

LifeOperation tpcc_operation(uint64_t table_id, uint64_t partition_id,
                             uint64_t key, LifeOperationKind kind,
                             uint32_t field, uint64_t argument = 0,
                             uint64_t routing_partition_id = UINT64_MAX) {
  LifeOperation op;
  op.object.table_id = table_id;
  op.object.partition_id = partition_id;
  op.object.routing_partition_id = routing_partition_id;
  op.object.primary_key = key;
  op.object.row_id = key;
  op.kind = kind;
  op.field_id = field;
  op.value_size = sizeof(uint64_t);
  if (kind != LifeOperationKind::ReadField) {
    const uint8_t *bytes = reinterpret_cast<const uint8_t *>(&argument);
    op.argument.assign(bytes, bytes + sizeof(argument));
  }
  return op;
}

uint64_t tpcc_neworder_item_steps() { return 2; }

uint64_t tpcc_neworder_steps(const LifeTpccSnapshot &tpcc) {
  // Warehouse read, customer read, district RMW, then item read + stock RMW.
  return 3 + tpcc.items.size() * tpcc_neworder_item_steps();
}

} // namespace

LifeTxnDescriptor TPCCTxnManager::life_descriptor() const {
  LifeTxnDescriptor descriptor = TxnManager::life_descriptor();
  const TPCCQuery *q = static_cast<const TPCCQuery *>(query);
  descriptor.workload = LifeWorkloadKind::Tpcc;
  descriptor.tpcc.txn_type = static_cast<uint32_t>(q->txn_type);
  descriptor.tpcc.state = static_cast<uint32_t>(state);
  descriptor.tpcc.program_index = descriptor.history.size();
  descriptor.tpcc.w_id = q->w_id;
  descriptor.tpcc.d_id = q->d_id;
  descriptor.tpcc.c_id = q->c_id;
  descriptor.tpcc.d_w_id = q->d_w_id;
  descriptor.tpcc.c_w_id = q->c_w_id;
  descriptor.tpcc.c_d_id = q->c_d_id;
  descriptor.tpcc.h_amount_bits = tpcc_double_bits(q->h_amount);
  descriptor.tpcc.by_last_name = q->by_last_name;
  std::memcpy(descriptor.tpcc.c_last, q->c_last,
              std::min(sizeof(descriptor.tpcc.c_last), sizeof(q->c_last)));
  descriptor.tpcc.remote = q->remote;
  descriptor.tpcc.ol_cnt = q->ol_cnt;
  descriptor.tpcc.o_entry_d = q->o_entry_d;
  descriptor.tpcc.o_id = q->o_id;
  descriptor.tpcc.item_index = next_item_id;
  descriptor.tpcc.materialized = false;
  descriptor.tpcc.items.reserve(q->items.size());
  for (uint64_t i = 0; i < q->items.size(); ++i) {
    LifeTpccItem item;
    item.item_id = q->items[i]->ol_i_id;
    item.supply_w_id = q->items[i]->ol_supply_w_id;
    item.quantity = q->items[i]->ol_quantity;
    descriptor.tpcc.items.push_back(item);
  }
  descriptor.history.reserve(3 + q->items.size() * 2);
  descriptor.touched_objects.reserve(3 + q->items.size() * 2);
  return descriptor;
}

void TPCCTxnManager::life_reset_workload() {
  state = static_cast<TPCCQuery *>(query)->txn_type == TPCC_PAYMENT
              ? TPCC_PAYMENT0 : TPCC_NEWORDER0;
  next_item_id = 0;
  reset_pending_life_finalize();
  reset_life_piggyback_prepare();
}

void TPCCTxnManager::life_reconcile_descriptor(LifeTxnDescriptor &descriptor) {
  LifeTpccSnapshot &tpcc = descriptor.tpcc;
  assert(descriptor.workload == LifeWorkloadKind::Tpcc);
  while (tpcc.program_index < descriptor.history.size()) {
    const LifeResponse &response = descriptor.history[tpcc.program_index].response;
    if (response.value.size() == sizeof(uint64_t))
      std::memcpy(&tpcc.scratch_bits, response.value.data(), sizeof(uint64_t));

    const uint64_t step = tpcc.program_index;
    if (tpcc.txn_type == TPCC_NEW_ORDER) {
      if (step == 2) {
        tpcc.o_id = tpcc.scratch_bits;
      } else if (step >= 3) {
        tpcc.item_index = (step - 3) / tpcc_neworder_item_steps();
      }
    }
    ++tpcc.program_index;
  }
  const uint64_t total = tpcc.txn_type == TPCC_PAYMENT
                             ? 3
                             : tpcc_neworder_steps(tpcc);
  tpcc.state = tpcc.program_index >= total ? TPCC_FIN :
      (tpcc.txn_type == TPCC_PAYMENT ? TPCC_PAYMENT0 : TPCC_NEWORDER0);
}

bool TPCCTxnManager::life_program_done(const LifeTxnDescriptor &d) const {
  return d.tpcc.program_index >=
      (d.tpcc.txn_type == TPCC_PAYMENT ? 3
                                       : tpcc_neworder_steps(d.tpcc));
}

LifeOperation TPCCTxnManager::life_current_operation(LifeTxnDescriptor &d) const {
  const LifeTpccSnapshot &t = d.tpcc;
  uint64_t step = t.program_index;
  const uint64_t amount = t.h_amount_bits;
  if (t.txn_type == TPCC_PAYMENT) {
    if (step == 0)
      return tpcc_operation(
          _wl->t_warehouse->get_table_id(), wh_to_part(t.w_id), t.w_id,
          g_wh_update ? LifeOperationKind::TpccPaymentYtd
                      : LifeOperationKind::ReadField,
          W_YTD, amount);
    if (step == 1)
      return tpcc_operation(
          _wl->t_district->get_table_id(), wh_to_part(t.d_w_id),
          distKey(t.d_id, t.d_w_id), LifeOperationKind::TpccPaymentYtd,
          D_YTD, amount);
    assert(step == 2);
    const uint64_t customer_key = t.by_last_name
        ? custNPKey(const_cast<char *>(t.c_last), t.c_d_id, t.c_w_id)
        : custKey(t.c_id, t.c_d_id, t.c_w_id);
    LifeOperation op = tpcc_operation(
        _wl->t_customer->get_table_id(), wh_to_part(t.c_w_id), customer_key,
        LifeOperationKind::TpccPaymentCustomer, C_BALANCE, amount);
    if (t.by_last_name) op.object.row_id = UINT64_MAX;
    return op;
  }

  if (step == 0)
    return tpcc_operation(_wl->t_warehouse->get_table_id(), wh_to_part(t.w_id),
                          t.w_id, LifeOperationKind::ReadField, W_TAX);
  if (step == 1)
    return tpcc_operation(_wl->t_customer->get_table_id(), wh_to_part(t.w_id),
        custKey(t.c_id, t.d_id, t.w_id), LifeOperationKind::ReadField,
        C_DISCOUNT);
  if (step == 2)
    return tpcc_operation(_wl->t_district->get_table_id(), wh_to_part(t.w_id),
        distKey(t.d_id, t.w_id), LifeOperationKind::TpccNextOrderId,
        D_NEXT_O_ID);

  const uint64_t offset = step - 3;
  const size_t item_index = offset / tpcc_neworder_item_steps();
  assert(item_index < t.items.size());
  const LifeTpccItem &item = t.items[item_index];
  const uint64_t stock_key = stockKey(item.item_id, item.supply_w_id);
  const uint64_t stock_part = wh_to_part(item.supply_w_id);
  if (offset % tpcc_neworder_item_steps() == 0)
    // ITEM is replicated. Execute its read at the stock participant, as
    // Calvin does, while lookup_life_row uses physical index partition 0.
    return tpcc_operation(_wl->t_item->get_table_id(), 0, item.item_id,
                          LifeOperationKind::ReadField, I_PRICE, 0,
                          stock_part);
  const uint64_t packed = item.quantity |
      (item.supply_w_id != t.w_id ? (uint64_t(1) << 63) : 0);
  return tpcc_operation(_wl->t_stock->get_table_id(), stock_part, stock_key,
                        LifeOperationKind::TpccUpdateStock, S_QUANTITY,
                        packed);
}

row_t *TPCCTxnManager::lookup_life_row(const LifeObjectId &object) const {
  INDEX *index = NULL;
  if (object.table_id == _wl->t_warehouse->get_table_id()) index = _wl->i_warehouse;
  else if (object.table_id == _wl->t_district->get_table_id()) index = _wl->i_district;
  else if (object.table_id == _wl->t_customer->get_table_id()) index = _wl->i_customer_id;
  else if (object.table_id == _wl->t_item->get_table_id()) index = _wl->i_item;
  else if (object.table_id == _wl->t_stock->get_table_id()) index = _wl->i_stock;
  else assert(false);
  const uint64_t index_partition =
      object.table_id == _wl->t_item->get_table_id() ? 0 : object.partition_id;
  itemid_t *item = const_cast<TPCCTxnManager *>(this)->index_read(
      index, object.primary_key, index_partition);
  assert(item != NULL);
  return static_cast<row_t *>(item->location);
}

LifeExecuteResult TPCCTxnManager::execute_life_operation(
    LifeTxnDescriptor &descriptor, LifeOperation &operation) {
  operation = life_current_operation(descriptor);
  row_t *target = NULL;
  if (descriptor.tpcc.by_last_name && operation.object.row_id == UINT64_MAX) {
    itemid_t *item = index_read(_wl->i_customer_last, operation.object.primary_key,
                                operation.object.partition_id);
    assert(item != NULL);
    itemid_t *mid = item;
    uint64_t count = 0;
    for (itemid_t *it = item; it != NULL; it = it->next) {
      ++count;
      if (count % 2 == 0) mid = mid->next;
    }
    target = static_cast<row_t *>(mid->location);
    operation.object.primary_key = target->get_primary_key();
    operation.object.row_id = target->get_primary_key();
  } else {
    target = lookup_life_row(operation.object);
  }
  operation.manager = target->manager;
  return target->execute_life(descriptor, operation);
}

void TPCCTxnManager::life_advance_program(LifeTxnDescriptor &d,
                                           const LifeOperation &op,
                                           const LifeResponse &response) {
  LifeHistoryEntry entry;
  entry.operation = op;
  entry.response = response;
  life_append_history(d, entry);
  life_reconcile_descriptor(d);
}

uint64_t TPCCTxnManager::life_program_position(const LifeTxnDescriptor &d) const {
  return d.tpcc.program_index;
}
bool TPCCTxnManager::life_program_present(const LifeTxnDescriptor &d) const {
  return d.workload == LifeWorkloadKind::Tpcc;
}
void TPCCTxnManager::life_reset_program(LifeTxnDescriptor &d) {
  d.tpcc.program_index = 0;
  d.tpcc.item_index = 0;
  d.tpcc.o_id = 0;
  d.tpcc.stock_quantity = 0;
  d.tpcc.scratch_bits = 0;
  d.tpcc.state = d.tpcc.txn_type == TPCC_PAYMENT ? TPCC_PAYMENT0 : TPCC_NEWORDER0;
}
void TPCCTxnManager::life_copy_program_to_workload(const LifeTxnDescriptor &d) {
  state = static_cast<TPCCRemTxnType>(d.tpcc.state);
  next_item_id = d.tpcc.item_index;
  static_cast<TPCCQuery *>(query)->o_id = d.tpcc.o_id;
}
void TPCCTxnManager::life_copy_remote_program(LifeTxnDescriptor &dst,
                                               const LifeTxnDescriptor &src) {
  assert(src.workload == LifeWorkloadKind::Tpcc);
  dst.workload = src.workload;
  dst.tpcc = src.tpcc;
}
uint64_t TPCCTxnManager::life_remote_batch_stop(const LifeTxnDescriptor &d,
                                                 uint64_t node) const {
  LifeTxnDescriptor probe = d;
  uint64_t stop = probe.tpcc.program_index;
  while (!life_program_done(probe)) {
    LifeOperation op = life_current_operation(probe);
    if (GET_NODE_ID(life_routing_partition(op.object)) != node) break;
    ++probe.tpcc.program_index;
    ++stop;
  }
  return stop == d.tpcc.program_index ? stop + 1 : stop;
}

void TPCCTxnManager::stage_life_insert(const LifeTxnDescriptor &descriptor,
                                       row_t *staged_row,
                                       table_t *table) {
  LifeStagedInsert insert;
  insert.pid = descriptor.pid;
  insert.tid = descriptor.tid;
  insert.row = staged_row;
  insert.table = table;
  life_staged_inserts.push_back(insert);
}

void TPCCTxnManager::stage_life_inserts(
    const LifeTxnDescriptor &descriptor) {
  const LifeTpccSnapshot &t = descriptor.tpcc;
  if (t.txn_type == TPCC_PAYMENT) {
    if (GET_NODE_ID(wh_to_part(t.c_w_id)) != g_node_id) return;
    row_t *hist;
    uint64_t row_id = 0;
    _wl->t_history->get_new_row(hist, wh_to_part(t.c_w_id), row_id);
    hist->set_value(H_C_ID, t.c_id);
    hist->set_value(H_C_D_ID, t.c_d_id);
    hist->set_value(H_C_W_ID, t.c_w_id);
    hist->set_value(H_D_ID, t.d_id);
    hist->set_value(H_W_ID, t.w_id);
    int64_t date = 2013;
    hist->set_value(H_DATE, date);
    const double amount = tpcc_bits_double(t.h_amount_bits);
    hist->set_value(H_AMOUNT, amount);
    stage_life_insert(descriptor, hist, _wl->t_history);
    return;
  }

  if (GET_NODE_ID(wh_to_part(t.w_id)) == g_node_id) {
    uint64_t row_id = 0;
    row_t *order;
    _wl->t_order->get_new_row(order, wh_to_part(t.w_id), row_id);
    order->set_value(O_ID, t.o_id);
    order->set_value(O_C_ID, t.c_id);
    order->set_value(O_D_ID, t.d_id);
    order->set_value(O_W_ID, t.w_id);
    order->set_value(O_ENTRY_D, t.o_entry_d);
    order->set_value(O_OL_CNT, t.ol_cnt);
    const int64_t all_local = t.remote ? 0 : 1;
    order->set_value(O_ALL_LOCAL, all_local);
    stage_life_insert(descriptor, order, _wl->t_order);

    row_t *new_order;
    _wl->t_neworder->get_new_row(new_order, wh_to_part(t.w_id), row_id);
    new_order->set_value(NO_O_ID, t.o_id);
    new_order->set_value(NO_D_ID, t.d_id);
    new_order->set_value(NO_W_ID, t.w_id);
    stage_life_insert(descriptor, new_order, _wl->t_neworder);
  }

  for (size_t i = 0; i < t.items.size(); ++i) {
    const LifeTpccItem &item = t.items[i];
    if (GET_NODE_ID(wh_to_part(item.supply_w_id)) != g_node_id) continue;
    uint64_t row_id = 0;
    row_t *line;
    _wl->t_orderline->get_new_row(line, wh_to_part(item.supply_w_id), row_id);
    line->set_value(OL_O_ID, t.o_id);
    line->set_value(OL_D_ID, t.d_id);
    line->set_value(OL_W_ID, t.w_id);
    const uint64_t number = i;
    line->set_value(OL_NUMBER, number);
    line->set_value(OL_I_ID, item.item_id);
#if !TPCC_SMALL
    line->set_value(OL_SUPPLY_W_ID, item.supply_w_id);
    line->set_value(OL_QUANTITY, item.quantity);
    const uint64_t amount = 0;
    line->set_value(OL_AMOUNT, amount);
#endif
    stage_life_insert(descriptor, line, _wl->t_orderline);
  }
}

void TPCCTxnManager::life_stage_inserts(
    const LifeTxnDescriptor &descriptor) {
  if (descriptor.workload != LifeWorkloadKind::Tpcc)
    return;
  for (std::vector<LifeStagedInsert>::const_iterator it =
           life_staged_inserts.begin();
       it != life_staged_inserts.end(); ++it) {
    if (it->pid == descriptor.pid && it->tid == descriptor.tid)
      return;
  }
  stage_life_inserts(descriptor);
}

void TPCCTxnManager::life_publish_staged_inserts(
    const LifeTxnDescriptor &descriptor) {
  for (std::vector<LifeStagedInsert>::iterator it =
           life_staged_inserts.begin();
       it != life_staged_inserts.end();) {
    if (it->pid != descriptor.pid || it->tid != descriptor.tid) {
      ++it;
      continue;
    }
    // Runtime TPC-C inserts have no active index publication in this codebase;
    // committed rows are retained when transaction-owned insert tracking is
    // cleared. Dropping the staging record transfers that same ownership
    // without tying a helped transaction's committed rows to the helper's
    // later abort cleanup.
    it = life_staged_inserts.erase(it);
  }
}

void TPCCTxnManager::life_discard_staged_inserts(
    const LifeTxnDescriptor &descriptor) {
  for (std::vector<LifeStagedInsert>::iterator it =
           life_staged_inserts.begin();
       it != life_staged_inserts.end();) {
    if (it->pid != descriptor.pid || it->tid != descriptor.tid) {
      ++it;
      continue;
    }
    it->row->manager->~Row_life();
    mem_allocator.free(it->row->manager, 0);
    it->row->free_row();
    mem_allocator.free(it->row, sizeof(row_t));
    it = life_staged_inserts.erase(it);
  }
}

void TPCCTxnManager::discard_all_life_staged_inserts() {
  while (!life_staged_inserts.empty()) {
    LifeStagedInsert &insert = life_staged_inserts.back();
    insert.row->manager->~Row_life();
    mem_allocator.free(insert.row->manager, 0);
    insert.row->free_row();
    mem_allocator.free(insert.row, sizeof(row_t));
    life_staged_inserts.pop_back();
  }
}
#endif


RC TPCCTxnManager::run_calvin_txn() {
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  TPCCQuery* tpcc_query = (TPCCQuery*) query;
  DEBUG("(%ld,%ld) Run calvin txn\n",txn->txn_id,txn->batch_id);
  while(!calvin_exec_phase_done() && rc == RCOK) {
    DEBUG("(%ld,%ld) phase %d\n",txn->txn_id,txn->batch_id,this->phase);
    switch(this->phase) {
      case CALVIN_RW_ANALYSIS:
        // Phase 1: Read/write set analysis
        calvin_expected_rsp_cnt = tpcc_query->get_participants(_wl);
        if(query->participant_nodes[g_node_id] == 1) {
          calvin_expected_rsp_cnt--;
        }

        DEBUG("(%ld,%ld) expects %d responses; %ld participants, %ld active\n",txn->txn_id,txn->batch_id,calvin_expected_rsp_cnt,query->participant_nodes.size(),query->active_nodes.size());

        this->phase = CALVIN_LOC_RD;
        break;
      case CALVIN_LOC_RD:
        // Phase 2: Perform local reads
        DEBUG("(%ld,%ld) local reads\n",txn->txn_id,txn->batch_id);
        rc = run_tpcc_phase2();
        //release_read_locks(tpcc_query);

        this->phase = CALVIN_SERVE_RD;
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
        rc = run_tpcc_phase5();
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


RC TPCCTxnManager::run_tpcc_phase2() {
  TPCCQuery* tpcc_query = (TPCCQuery*) query;
  RC rc = RCOK;
  assert(CC_ALG == CALVIN);

	uint64_t w_id = tpcc_query->w_id;
  uint64_t d_id = tpcc_query->d_id;
  uint64_t c_id = tpcc_query->c_id;
  //uint64_t d_w_id = tpcc_query->d_w_id;
  //uint64_t c_w_id = tpcc_query->c_w_id;
  //uint64_t c_d_id = tpcc_query->c_d_id;
	//char * c_last = tpcc_query->c_last;
  //double h_amount = tpcc_query->h_amount;
	//bool by_last_name = tpcc_query->by_last_name;
	bool remote = tpcc_query->remote;
	uint64_t ol_cnt = tpcc_query->ol_cnt;
	uint64_t o_entry_d = tpcc_query->o_entry_d;
  //uint64_t o_id = tpcc_query->o_id;

	uint64_t part_id_w = wh_to_part(w_id);
	//uint64_t part_id_c_w = wh_to_part(c_w_id);
  bool w_loc = GET_NODE_ID(part_id_w) == g_node_id;
  //bool c_w_loc = GET_NODE_ID(part_id_c_w) == g_node_id;


	switch (tpcc_query->txn_type) {
		case TPCC_PAYMENT :
      break;
		case TPCC_NEW_ORDER :
      if(w_loc) {
			  rc = new_order_0( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row); 
        rc = new_order_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row); 
        rc = new_order_2( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row); 
        rc = new_order_3( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row); 
        rc = new_order_4( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row); 
        tpcc_query->o_id = *(int64_t *) row->get_value(D_NEXT_O_ID);
        //rc = new_order_5( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row); 
      }
        for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {

          uint64_t ol_number = i;
          uint64_t ol_i_id = tpcc_query->items[ol_number]->ol_i_id;
          uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
          //uint64_t ol_quantity = tpcc_query->items[ol_number].ol_quantity;
          //uint64_t ol_amount = tpcc_query->ol_amount;
          uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
          bool ol_supply_w_loc = GET_NODE_ID(part_id_ol_supply_w) == g_node_id;
          if(ol_supply_w_loc) {
            rc = new_order_6(ol_i_id, row);
            rc = new_order_7(ol_i_id, row);
          }
        }
        break;
    default: assert(false);
  }
  return rc;
}

RC TPCCTxnManager::run_tpcc_phase5() {
  TPCCQuery* tpcc_query = (TPCCQuery*) query;
  RC rc = RCOK;
  assert(CC_ALG == CALVIN);

	uint64_t w_id = tpcc_query->w_id;
  uint64_t d_id = tpcc_query->d_id;
  uint64_t c_id = tpcc_query->c_id;
  uint64_t d_w_id = tpcc_query->d_w_id;
  uint64_t c_w_id = tpcc_query->c_w_id;
  uint64_t c_d_id = tpcc_query->c_d_id;
	char * c_last = tpcc_query->c_last;
  double h_amount = tpcc_query->h_amount;
	bool by_last_name = tpcc_query->by_last_name;
	bool remote = tpcc_query->remote;
	uint64_t ol_cnt = tpcc_query->ol_cnt;
	uint64_t o_entry_d = tpcc_query->o_entry_d;
  uint64_t o_id = tpcc_query->o_id;

	uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
  bool w_loc = GET_NODE_ID(part_id_w) == g_node_id;
  bool c_w_loc = GET_NODE_ID(part_id_c_w) == g_node_id;


	switch (tpcc_query->txn_type) {
		case TPCC_PAYMENT :
      if(w_loc) {
        rc = run_payment_0(w_id, d_id, d_w_id, h_amount, row);
        rc = run_payment_1(w_id, d_id, d_w_id, h_amount, row);
        rc = run_payment_2(w_id, d_id, d_w_id, h_amount, row);
        rc = run_payment_3(w_id, d_id, d_w_id, h_amount, row);
      }
      if(c_w_loc) {
        rc = run_payment_4( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row); 
        rc = run_payment_5( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row); 
      }
      break;
		case TPCC_NEW_ORDER :
	      if(w_loc) {
	        rc = new_order_4( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
	        rc = new_order_5( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &tpcc_query->o_id, row);
	      }
        for(uint64_t i = 0; i < tpcc_query->ol_cnt; i++) {

          uint64_t ol_number = i;
          uint64_t ol_i_id = tpcc_query->items[ol_number]->ol_i_id;
          uint64_t ol_supply_w_id = tpcc_query->items[ol_number]->ol_supply_w_id;
          uint64_t ol_quantity = tpcc_query->items[ol_number]->ol_quantity;
          uint64_t ol_amount = tpcc_query->ol_amount;
          uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
          bool ol_supply_w_loc = GET_NODE_ID(part_id_ol_supply_w) == g_node_id;
          if(ol_supply_w_loc) {
            rc = new_order_8( w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity,  ol_number, o_id, row); 
            rc = new_order_9( w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity,  ol_number, ol_amount, o_id, row); 
          }
        }
        break;
    default: assert(false);
  }
  return rc;

}

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
#include "manager.h"
#include "thread.h"
#include "worker_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "math.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "work_queue.h"
#include "logger.h"
#include "message.h"
#include "abort_queue.h"
#include "maat.h"

#if CC_ALG == LIFE
volatile uint64_t life_dbg_execute_sent = 0;
volatile uint64_t life_dbg_execute_recv = 0;
volatile uint64_t life_dbg_execute_rsp_sent = 0;
volatile uint64_t life_dbg_execute_rsp_recv = 0;
volatile uint64_t life_dbg_execute_rsp_applied = 0;
volatile uint64_t life_dbg_prepare_sent = 0;
volatile uint64_t life_dbg_prepare_recv = 0;
volatile uint64_t life_dbg_prepare_rsp_sent = 0;
volatile uint64_t life_dbg_prepare_rsp_recv = 0;
volatile uint64_t life_dbg_finish_sent = 0;
volatile uint64_t life_dbg_finish_recv = 0;
volatile uint64_t life_dbg_finalize_sent = 0;
volatile uint64_t life_dbg_finalize_recv = 0;
volatile uint64_t life_dbg_finalize_rsp_sent = 0;
volatile uint64_t life_dbg_finalize_rsp_recv = 0;
volatile uint64_t life_dbg_execute_rsp_stale = 0;
volatile uint64_t life_dbg_execute_duplicate_wait = 0;
volatile uint64_t life_dbg_prepare_rsp_duplicate = 0;
volatile uint64_t life_dbg_inactive_msg_dropped = 0;
volatile uint64_t life_dbg_execute_rsp_recv_code[6] = {0, 0, 0, 0, 0, 0};
volatile uint64_t life_dbg_msg_count = 0;
volatile uint64_t life_dbg_msg_bytes = 0;

#ifndef LIFE_DEBUG_COUNTERS
#define LIFE_DEBUG_COUNTERS false
#endif
#if LIFE_DEBUG_COUNTERS
#define LIFE_DBG_INC(counter) __sync_fetch_and_add(&(counter), 1)
#else
#define LIFE_DBG_INC(counter) ((void)0)
#endif

void life_dbg_count_execute_rsp_code(LifeResultCode code) {
#if LIFE_DEBUG_COUNTERS
  const uint32_t index = static_cast<uint32_t>(code);
  if (index < 6)
    LIFE_DBG_INC(life_dbg_execute_rsp_recv_code[index]);
#else
  (void)code;
#endif
}

static RC life_note_wait_start(TxnManager *txn_man, RC rc) {
  if (rc == WAIT_REM && txn_man != NULL)
    txn_man->txn_stats.wait_starttime = get_sys_clock();
  return rc;
}
#endif

void WorkerThread::setup() {

	if( get_thd_id() == 0) {
    send_init_done_to_all_nodes();
  }
  _thd_txn_id = 0;

}

void WorkerThread::process(Message * msg) {
  RC rc __attribute__ ((unused));

  DEBUG("%ld Processing %ld %d\n",get_thd_id(),msg->get_txn_id(),msg->get_rtype());
  assert(msg->get_rtype() == CL_QRY || msg->get_txn_id() != UINT64_MAX);
  uint64_t starttime = get_sys_clock();
		switch(msg->get_rtype()) {
			case RPASS:
        //rc = process_rpass(msg);
				break;
			case RPREPARE: 
        rc = process_rprepare(msg);
				break;
			case RFWD:
        rc = process_rfwd(msg);
				break;
			case RQRY:
        rc = process_rqry(msg);
				break;
			case RQRY_CONT:
        rc = process_rqry_cont(msg);
				break;
			case RQRY_RSP:
        rc = process_rqry_rsp(msg);
				break;
#if CC_ALG == LIFE
      case RLIFE_EXECUTE:
        rc = process_life_execute(msg);
        break;
      case RLIFE_EXECUTE_RSP:
        rc = process_life_execute_rsp(msg);
        break;
      case RLIFE_PREPARE:
        rc = process_life_prepare(msg);
        break;
      case RLIFE_PREPARE_RSP:
        rc = process_life_prepare_rsp(msg);
        break;
      case RLIFE_FINISH:
        rc = process_life_finish(msg);
        break;
      case RLIFE_HELP:
        rc = process_life_help(msg);
        break;
      case RLIFE_HELP_APPLY:
        rc = process_life_help_apply(msg);
        break;
      case RLIFE_FINALIZE:
        rc = process_life_finalize(msg);
        break;
      case RLIFE_FINALIZE_RSP:
        rc = process_life_finalize_rsp(msg);
        break;
#endif
			case RFIN: 
        rc = process_rfin(msg);
				break;
			case RACK_PREP:
        rc = process_rack_prep(msg);
				break;
			case RACK_FIN:
        rc = process_rack_rfin(msg);
				break;
			case RTXN_CONT:
        rc = process_rtxn_cont(msg);
				break;
      case CL_QRY:
			case RTXN:
#if CC_ALG == CALVIN
        rc = process_calvin_rtxn(msg);
#else
        rc = process_rtxn(msg);
#endif
				break;
			case LOG_FLUSHED:
        rc = process_log_flushed(msg);
				break;
			case LOG_MSG:
        rc = process_log_msg(msg);
				break;
			case LOG_MSG_RSP:
        rc = process_log_msg_rsp(msg);
				break;
			default:
        printf("Msg: %d\n",msg->get_rtype());
        fflush(stdout);
				assert(false);
				break;
		}
  uint64_t timespan = get_sys_clock() - starttime;
  INC_STATS(get_thd_id(),worker_process_cnt,1);
  INC_STATS(get_thd_id(),worker_process_time,timespan);
  INC_STATS(get_thd_id(),worker_process_cnt_by_type[msg->rtype],1);
  INC_STATS(get_thd_id(),worker_process_time_by_type[msg->rtype],timespan);
  DEBUG("%ld EndProcessing %d %ld\n",get_thd_id(),msg->get_rtype(),msg->get_txn_id());
}

void WorkerThread::check_if_done(RC rc) {
  if(txn_man->waiting_for_response())
    return;
  if(rc == Commit)
    commit();
  if(rc == Abort)
    abort();
}

void WorkerThread::release_txn_man() {
#if CC_ALG == LIFE
  // LifeTxnManager is the shared coordinator base for every LIFE workload.
  ((LifeTxnManager *)txn_man)->clear_life_active();
#endif
  txn_table.release_transaction_manager(get_thd_id(),txn_man->get_txn_id(),txn_man->get_batch_id());
  txn_man = NULL;
}

void WorkerThread::calvin_wrapup() {
  const RC outcome = txn_man->get_rc();
  txn_man->release_locks(outcome);
  // Reconnaissance and failed validation attempts are protocol attempts, not
  // committed transactions. The sequencer records the logical commit after
  // it has collected every ACK.
  if (!txn_man->isRecon() && outcome == RCOK)
    txn_man->commit_stats();
  DEBUG("(%ld,%ld) calvin ack to %ld\n",txn_man->get_txn_id(),txn_man->get_batch_id(),txn_man->return_id);
  if(txn_man->return_id == g_node_id) {
    work_queue.sequencer_enqueue(_thd_id,Message::create_message(txn_man,CALVIN_ACK));
  } else {
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,CALVIN_ACK),txn_man->return_id);
  }
  release_txn_man();
}

// Can't use txn_man after this function
void WorkerThread::commit() {
  //TxnManager * txn_man = txn_table.get_transaction_manager(txn_id,0);
  //txn_man->release_locks(RCOK);
  //        txn_man->commit_stats();
  assert(txn_man);
  assert(IS_LOCAL(txn_man->get_txn_id()));

  uint64_t timespan = get_sys_clock() - txn_man->txn_stats.starttime;
  DEBUG("COMMIT %ld %f -- %f\n",txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),(double)timespan/ BILLION);

  // Send result back to client
#if !SERVER_GENERATE_QUERIES
  if (ISCLIENTN(txn_man->client_id)) {
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,CL_RSP),txn_man->client_id);
  } else {
    DEBUG("Skip CL_RSP for %ld with invalid client_id %ld\n",
          txn_man->get_txn_id(), txn_man->client_id);
  }
#endif
  // remove txn from pool
  release_txn_man();
  // Do not use txn_man after this

}

void WorkerThread::abort() {

  DEBUG("ABORT %ld -- %f\n",txn_man->get_txn_id(),(double)get_sys_clock() - run_starttime/ BILLION);
  // TODO: TPCC Rollback here

  ++txn_man->abort_cnt;
  txn_man->reset();

  uint64_t penalty = abort_queue.enqueue(get_thd_id(), txn_man->get_txn_id(),txn_man->get_abort_cnt());

  txn_man->txn_stats.total_abort_time += penalty;

}

TxnManager * WorkerThread::get_transaction_manager(Message * msg) {
#if CC_ALG == CALVIN
  TxnManager * local_txn_man = txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),msg->get_batch_id());
#else
  TxnManager * local_txn_man = txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),0);
#endif
  return local_txn_man;
}

RC WorkerThread::run() {
  tsetup();
  printf("Running WorkerThread %ld\n",_thd_id);

  uint64_t ready_starttime;
  uint64_t idle_starttime = 0;

	while(!simulation->is_done()) {
    txn_man = NULL;
    heartbeat();

    progress_stats();

    Message * msg = work_queue.dequeue(get_thd_id());

    if(!msg) {
      if(idle_starttime ==0)
        idle_starttime = get_sys_clock();
      continue;
    }
    if(idle_starttime > 0) {
      INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
      idle_starttime = 0;
    }
    //uint64_t starttime = get_sys_clock();

    if(msg->rtype != CL_QRY || CC_ALG == CALVIN) {
      txn_man = get_transaction_manager(msg);
#if CC_ALG == LIFE
      if (txn_man == NULL) {
        work_queue.enqueue(get_thd_id(),msg,true);
        continue;
      }
#endif

      if (CC_ALG != CALVIN && IS_LOCAL(txn_man->get_txn_id())) {
        if (msg->rtype != RTXN_CONT && ((msg->rtype != RACK_PREP) || (txn_man->get_rsp_cnt() == 1))) {
          txn_man->txn_stats.work_queue_time_short += msg->lat_work_queue_time;
          txn_man->txn_stats.cc_block_time_short += msg->lat_cc_block_time;
          txn_man->txn_stats.cc_time_short += msg->lat_cc_time;
          txn_man->txn_stats.msg_queue_time_short += msg->lat_msg_queue_time;
          txn_man->txn_stats.process_time_short += msg->lat_process_time;
          /*
          if (msg->lat_network_time/BILLION > 1.0) {
            printf("%ld %d %ld -> %ld: %f %f\n",msg->txn_id, msg->rtype, msg->return_node_id,get_node_id() ,msg->lat_network_time/BILLION, msg->lat_other_time/BILLION);
          } 
          */
          txn_man->txn_stats.network_time_short += msg->lat_network_time;
        }

      } else {
          txn_man->txn_stats.clear_short();
      }
      if (CC_ALG != CALVIN) {
        txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
        txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;
      }
      txn_man->txn_stats.msg_queue_time += msg->mq_time;
      txn_man->txn_stats.msg_queue_time_short += msg->mq_time;
      msg->mq_time = 0;
      txn_man->txn_stats.work_queue_time += msg->wq_time;
      txn_man->txn_stats.work_queue_time_short += msg->wq_time;
      //txn_man->txn_stats.network_time += msg->ntwk_time;
      msg->wq_time = 0;
      txn_man->txn_stats.work_queue_cnt += 1;


      ready_starttime = get_sys_clock();
#if CC_ALG == LIFE
      bool ready = true;
#else
      bool ready = txn_man->unset_ready();
#endif
      INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
      if(!ready) {
        // Return to work queue, end processing
        work_queue.enqueue(get_thd_id(),msg,true);
        continue;
      }
      txn_man->register_thread(this);
    }

    process(msg);

    ready_starttime = get_sys_clock();
    if(txn_man) {
      bool ready = txn_man->set_ready();
      assert(ready);
    }
    INC_STATS(get_thd_id(),worker_deactivate_txn_time,get_sys_clock() - ready_starttime);

    // delete message
    ready_starttime = get_sys_clock();
#if CC_ALG != CALVIN
    Message::release_message(msg);
#endif
    INC_STATS(get_thd_id(),worker_release_msg_time,get_sys_clock() - ready_starttime);

	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

RC WorkerThread::process_rfin(Message * msg) {
  DEBUG("RFIN %ld\n",msg->get_txn_id());
  assert(CC_ALG != CALVIN);

  M_ASSERT_V(!IS_LOCAL(msg->get_txn_id()),"RFIN local: %ld %ld/%d\n",msg->get_txn_id(),msg->get_txn_id()%g_node_cnt,g_node_id);
#if CC_ALG == MAAT
  txn_man->set_commit_timestamp(((FinishMessage*)msg)->commit_timestamp);
#endif

  if(((FinishMessage*)msg)->rc == Abort) {
    txn_man->abort();
    txn_man->reset();
    txn_man->reset_query();
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_FIN),GET_NODE_ID(msg->get_txn_id()));
    return Abort;
  } 
  txn_man->commit();
  //if(!txn_man->query->readonly() || CC_ALG == OCC)
  if(!((FinishMessage*)msg)->readonly || CC_ALG == MAAT || CC_ALG == OCC)
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_FIN),GET_NODE_ID(msg->get_txn_id()));
  release_txn_man();

  return RCOK;
}

RC WorkerThread::process_rack_prep(Message * msg) {
  DEBUG("RPREP_ACK %ld\n",msg->get_txn_id());

  RC rc = RCOK;

  int responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  assert(responses_left >=0);
#if CC_ALG == MAAT
  // Integrate bounds
  uint64_t lower = ((AckMessage*)msg)->lower;
  uint64_t upper = ((AckMessage*)msg)->upper;
  if(lower > time_table.get_lower(get_thd_id(),msg->get_txn_id())) {
    time_table.set_lower(get_thd_id(),msg->get_txn_id(),lower);
  }
  if(upper < time_table.get_upper(get_thd_id(),msg->get_txn_id())) {
    time_table.set_upper(get_thd_id(),msg->get_txn_id(),upper);
  }
  DEBUG("%ld bound set: [%ld,%ld] -> [%ld,%ld]\n",msg->get_txn_id(),lower,upper,time_table.get_lower(get_thd_id(),msg->get_txn_id()),time_table.get_upper(get_thd_id(),msg->get_txn_id()));
  if(((AckMessage*)msg)->rc != RCOK) {
    time_table.set_state(get_thd_id(),msg->get_txn_id(),MAAT_ABORTED);
  }
#endif
  if(responses_left > 0) 
    return WAIT;

  // Done waiting 
  if(txn_man->get_rc() == RCOK) {
    rc  = txn_man->validate();
  }
  if(rc == Abort || txn_man->get_rc() == Abort) {
    txn_man->txn->rc = Abort;
    rc = Abort;
  }
  txn_man->send_finish_messages();
  if(rc == Abort) {
    txn_man->abort();
  } else {
    txn_man->commit();
  }

  return rc;
}

RC WorkerThread::process_rack_rfin(Message * msg) {
  DEBUG("RFIN_ACK %ld\n",msg->get_txn_id());

  RC rc = RCOK;

  int responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  assert(responses_left >=0);
  if(responses_left > 0) 
    return WAIT;

  // Done waiting 
  txn_man->txn_stats.twopc_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

  if(txn_man->get_rc() == RCOK) {
    //txn_man->commit();
    commit();
  } else {
    //txn_man->abort();
    abort();
  }
  return rc;
}

RC WorkerThread::process_rqry_rsp(Message * msg) {
  DEBUG("RQRY_RSP %ld\n",msg->get_txn_id());
  assert(IS_LOCAL(msg->get_txn_id()));

  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

  if(((QueryResponseMessage*)msg)->rc == Abort) {
    txn_man->start_abort();
    return Abort;
  }

  RC rc = txn_man->run_txn();
  check_if_done(rc);
  return rc;

}

RC WorkerThread::process_rqry(Message * msg) {
  DEBUG("RQRY %ld\n",msg->get_txn_id());
  M_ASSERT_V(!IS_LOCAL(msg->get_txn_id()),"RQRY local: %ld %ld/%d\n",msg->get_txn_id(),msg->get_txn_id()%g_node_cnt,g_node_id);
  assert(!IS_LOCAL(msg->get_txn_id()));
  RC rc = RCOK;

  msg->copy_to_txn(txn_man);

#if CC_ALG == MVCC
  txn_table.update_min_ts(get_thd_id(),txn_man->get_txn_id(),0,txn_man->get_timestamp());
#endif
#if CC_ALG == MAAT
          time_table.init(get_thd_id(),txn_man->get_txn_id());
#endif

  rc = txn_man->run_txn();

  // Send response
  if(rc != WAIT) {
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RQRY_RSP),txn_man->return_id);
  }
  return rc;
}

RC WorkerThread::process_rqry_cont(Message * msg) {
  DEBUG("RQRY_CONT %ld\n",msg->get_txn_id());
  assert(!IS_LOCAL(msg->get_txn_id()));
  RC rc = RCOK;

  txn_man->run_txn_post_wait();
  rc = txn_man->run_txn();

  // Send response
  if(rc != WAIT) {
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RQRY_RSP),txn_man->return_id);
  }
  return rc;
}

#if CC_ALG == LIFE
RC WorkerThread::process_life_execute(Message *msg) {
  DEBUG("RLIFE_EXECUTE %ld\n", msg->get_txn_id());
  assert(CC_ALG == LIFE);
  LIFE_DBG_INC(life_dbg_execute_recv);

  ((LifeTxnManager *)txn_man)->mark_life_active();
  LifeExecuteMessage *life_msg = (LifeExecuteMessage *)msg;
  LifeExecuteResponseMessage *response =
      (LifeExecuteResponseMessage *)Message::create_message(RLIFE_EXECUTE_RSP);
  response->txn_id = msg->get_txn_id();
  response->wait_id = life_msg->wait_id;
  response->prepared = false;
  LifeTxnManager *life_txn = (LifeTxnManager *)txn_man;
  life_txn->serve_life_execute(life_msg->descriptor,
                               life_msg->operation,
                               life_msg->stop_record_id,
                               life_msg->prepare_after_execute,
                               response->result, response->prepared);
  response->history_base_size = life_msg->descriptor.history.size();
  const LifeResultCode result_code = response->result.code;
  if (response->result.code == LifeResultCode::Success) {
    std::vector<LifeHistoryEntry> &history =
        response->result.transaction.history;
    assert(history.size() >= response->history_base_size);
    history.erase(history.begin(),
                  history.begin() + response->history_base_size);
    response->result.transaction.ycsb.requests.clear();
    response->result.transaction.touched_objects.clear();
  }

  msg_queue.enqueue(get_thd_id(), response, msg->return_node_id);
  LIFE_DBG_INC(life_dbg_execute_rsp_sent);

  // Ordinary remote LIFE execution is part of a longer envelope transaction.
  // Keep its manager in the transaction table so later execute/prepare/finish
  // messages reuse the same initialized manager instead of cycling it through
  // TxnManPool for every round trip. The txn id pins all of those messages to
  // one worker, and the ready flag serializes table acquisition.
  //
  // Committed and InvalidOperation are terminal execute results: the requester
  // will not run the normal prepare/finish path for this remote manager. Release
  // immediately so stale or invalid messages cannot leave orphaned entries.
  if (!IS_LOCAL(msg->get_txn_id()) &&
      (result_code == LifeResultCode::Committed ||
       result_code == LifeResultCode::InvalidOperation))
    release_txn_man();
  return RCOK;
}

RC WorkerThread::process_life_execute_rsp(Message *msg) {
  DEBUG("RLIFE_EXECUTE_RSP %ld\n", msg->get_txn_id());
  assert(CC_ALG == LIFE);
  LIFE_DBG_INC(life_dbg_execute_rsp_recv);

  LifeTxnManager *life_txn = (LifeTxnManager *)txn_man;
  if (!life_txn->is_life_active()) {
    LIFE_DBG_INC(life_dbg_inactive_msg_dropped);
    release_txn_man();
    return RCOK;
  }

  txn_man->txn_stats.remote_wait_time +=
      get_sys_clock() - txn_man->txn_stats.wait_starttime;

  LifeExecuteResponseMessage *life_msg = (LifeExecuteResponseMessage *)msg;
  life_dbg_count_execute_rsp_code(life_msg->result.code);
  RC rc = life_txn->apply_life_execute_response(life_msg->result,
                                                life_msg->wait_id,
                                                life_msg->history_base_size,
                                                life_msg->prepared,
                                                msg->return_node_id);
  LIFE_DBG_INC(life_dbg_execute_rsp_applied);
  check_if_done(rc);
  life_note_wait_start(txn_man, rc);
  if (!IS_LOCAL(msg->get_txn_id()) && txn_man != NULL && rc != WAIT_REM)
    release_txn_man();
  return rc;
}

RC WorkerThread::process_life_prepare(Message *msg) {
  DEBUG("RLIFE_PREPARE %ld\n", msg->get_txn_id());
  assert(CC_ALG == LIFE);
  LIFE_DBG_INC(life_dbg_prepare_recv);

  ((LifeTxnManager *)txn_man)->mark_life_active();
  LifePrepareMessage *life_msg = (LifePrepareMessage *)msg;
  LifePrepareResponseMessage *response =
      (LifePrepareResponseMessage *)Message::create_message(RLIFE_PREPARE_RSP);
  response->txn_id = msg->get_txn_id();
  response->pid = life_msg->descriptor.pid;
  response->tid = life_msg->descriptor.tid;
  response->result =
      ((LifeTxnManager *)txn_man)->prepare_life_remote(life_msg->descriptor);
  const LifeResultCode result_code = response->result.code;
  msg_queue.enqueue(get_thd_id(), response, msg->return_node_id);
  LIFE_DBG_INC(life_dbg_prepare_rsp_sent);

  // Success means at least one row is prepared and RLIFE_FINISH is expected;
  // retain the manager until that terminal message. Other results need no
  // manager-local continuity. Releasing them also handles duplicate prepare
  // requests that observe rows already committed after the original finish.
  if (!IS_LOCAL(msg->get_txn_id()) &&
      result_code != LifeResultCode::Success)
    release_txn_man();
  return RCOK;
}

RC WorkerThread::process_life_prepare_rsp(Message *msg) {
  DEBUG("RLIFE_PREPARE_RSP %ld\n", msg->get_txn_id());
  assert(CC_ALG == LIFE);
  LIFE_DBG_INC(life_dbg_prepare_rsp_recv);

  LifeTxnManager *life_txn = (LifeTxnManager *)txn_man;
  if (!life_txn->is_life_active()) {
    LIFE_DBG_INC(life_dbg_inactive_msg_dropped);
    release_txn_man();
    return RCOK;
  }

  txn_man->txn_stats.remote_wait_time +=
      get_sys_clock() - txn_man->txn_stats.wait_starttime;

  LifePrepareResponseMessage *life_msg = (LifePrepareResponseMessage *)msg;
  RC rc = life_txn->apply_life_prepare_response(life_msg->result,
                                                life_msg->pid,
                                                life_msg->tid,
                                                msg->return_node_id);
  check_if_done(rc);
  life_note_wait_start(txn_man, rc);
  if (!IS_LOCAL(msg->get_txn_id()) && txn_man != NULL && rc != WAIT_REM)
    release_txn_man();
  return rc;
}

RC WorkerThread::process_life_finish(Message *msg) {
  DEBUG("RLIFE_FINISH %ld\n", msg->get_txn_id());
  assert(CC_ALG == LIFE);
  LIFE_DBG_INC(life_dbg_finish_recv);

  ((LifeTxnManager *)txn_man)->mark_life_active();
  LifeFinishMessage *life_msg = (LifeFinishMessage *)msg;
  ((LifeTxnManager *)txn_man)
      ->finish_life_remote(life_msg->descriptor, life_msg->decision);
  // RLIFE_FINISH is the terminal owner of a durable remote LIFE manager. It
  // commits/rolls back row-owned state above, then removes the manager from the
  // transaction table and returns it to TxnManPool. Duplicate finishes safely
  // create and immediately release a fresh manager.
  if (!IS_LOCAL(msg->get_txn_id()))
    release_txn_man();
  return RCOK;
}

RC WorkerThread::process_life_help(Message *msg) {
  DEBUG("RLIFE_HELP %ld\n", msg->get_txn_id());
  assert(CC_ALG == LIFE);
  assert(IS_LOCAL(msg->get_txn_id()));

  LifeTxnManager *life_txn = (LifeTxnManager *)txn_man;
  if (!life_txn->is_life_active()) {
    LIFE_DBG_INC(life_dbg_inactive_msg_dropped);
    release_txn_man();
    return RCOK;
  }

  LifeHelpMessage *life_msg = (LifeHelpMessage *)msg;
  RC rc = life_txn->apply_life_help_request(life_msg->descriptor,
                                            msg->return_node_id);
  return life_note_wait_start(txn_man, rc);
}

RC WorkerThread::process_life_help_apply(Message *msg) {
  DEBUG("RLIFE_HELP_APPLY %ld\n", msg->get_txn_id());
  assert(CC_ALG == LIFE);

  ((LifeTxnManager *)txn_man)->mark_life_active();
  LifeHelpApplyMessage *life_msg = (LifeHelpApplyMessage *)msg;
  RC rc = ((LifeTxnManager *)txn_man)->help_life_remote(life_msg->descriptor);
  life_note_wait_start(txn_man, rc);
  if (!IS_LOCAL(msg->get_txn_id()) && rc != WAIT_REM)
    release_txn_man();
  return rc;
}

RC WorkerThread::process_life_finalize(Message *msg) {
  DEBUG("RLIFE_FINALIZE %ld\n", msg->get_txn_id());
  assert(CC_ALG == LIFE);
  assert(IS_LOCAL(msg->get_txn_id()));
  LIFE_DBG_INC(life_dbg_finalize_recv);

  LifeTxnManager *life_txn = (LifeTxnManager *)txn_man;
  life_txn->mark_life_active();
  LifeFinalizeMessage *life_msg = (LifeFinalizeMessage *)msg;
  RC rc = life_txn->apply_life_finalize_request(life_msg->descriptor,
                                                msg->return_node_id,
                                                life_msg->requester_txn_id);
  check_if_done(rc);
  life_note_wait_start(txn_man, rc);
  return rc;
}

RC WorkerThread::process_life_finalize_rsp(Message *msg) {
  DEBUG("RLIFE_FINALIZE_RSP %ld\n", msg->get_txn_id());
  assert(CC_ALG == LIFE);
  LIFE_DBG_INC(life_dbg_finalize_rsp_recv);

  LifeTxnManager *life_txn = (LifeTxnManager *)txn_man;
  if (!life_txn->is_life_active()) {
    LIFE_DBG_INC(life_dbg_inactive_msg_dropped);
    release_txn_man();
    return RCOK;
  }

  LifeFinalizeResponseMessage *life_msg = (LifeFinalizeResponseMessage *)msg;
  RC rc = life_txn->apply_life_finalize_response(life_msg->result);
  check_if_done(rc);
  life_note_wait_start(txn_man, rc);
  if (!IS_LOCAL(msg->get_txn_id()) && txn_man != NULL && rc != WAIT_REM)
    release_txn_man();
  return rc;
}
#endif


RC WorkerThread::process_rtxn_cont(Message * msg) {
  DEBUG("RTXN_CONT %ld\n",msg->get_txn_id());
  assert(IS_LOCAL(msg->get_txn_id()));

  txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;

  txn_man->run_txn_post_wait();
  RC rc = txn_man->run_txn();
  check_if_done(rc);
  return RCOK;
}

RC WorkerThread::process_rprepare(Message * msg) {
  DEBUG("RPREP %ld\n",msg->get_txn_id());
    RC rc = RCOK;

    // Validate transaction
    rc  = txn_man->validate();
    txn_man->set_rc(rc);
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_PREP),msg->return_node_id);
    // Clean up as soon as abort is possible
    if(rc == Abort) {
      txn_man->abort();
    }

    return rc;
}

uint64_t WorkerThread::get_next_txn_id() {
  uint64_t txn_id = ( get_node_id() + get_thd_id() * g_node_cnt) 
							+ (g_thread_cnt * g_node_cnt * _thd_txn_id);
  ++_thd_txn_id;
  return txn_id;
}

RC WorkerThread::process_rtxn(Message * msg) {
        RC rc = RCOK;
        uint64_t txn_id = UINT64_MAX;

        if(msg->get_rtype() == CL_QRY) {
          // This is a new transaction

					// Only set new txn_id when txn first starts
          txn_id = get_next_txn_id();
          msg->txn_id = txn_id;

					// Put txn in txn_table
          txn_man = txn_table.get_transaction_manager(get_thd_id(),txn_id,0);
          assert(txn_man);
          txn_man->register_thread(this);
          uint64_t ready_starttime = get_sys_clock();
#if CC_ALG == LIFE
          bool ready = true;
#else
          bool ready = txn_man->unset_ready();
#endif
          INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
          assert(ready);
					if (CC_ALG == WAIT_DIE) {
            txn_man->set_timestamp(get_next_ts());
          }
          txn_man->txn_stats.starttime = get_sys_clock();
          txn_man->txn_stats.restart_starttime = txn_man->txn_stats.starttime;
          msg->copy_to_txn(txn_man);
#if CC_ALG == LIFE
          // Response handlers accept continuations only for active home owners.
          ((LifeTxnManager *)txn_man)->mark_life_active();
#endif
          DEBUG("START %ld %f %lu\n",txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),txn_man->txn_stats.starttime);
          INC_STATS(get_thd_id(),local_txn_start_cnt,1);

        } else {
            txn_man->txn_stats.restart_starttime = get_sys_clock();
          DEBUG("RESTART %ld %f %lu\n",txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),txn_man->txn_stats.starttime);
        }

          // Get new timestamps
          if(is_cc_new_timestamp()) {
            txn_man->set_timestamp(get_next_ts());
					}
#if CC_ALG == MVCC
          txn_table.update_min_ts(get_thd_id(),txn_id,0,txn_man->get_timestamp());
#endif

#if CC_ALG == OCC
          txn_man->set_start_timestamp(get_next_ts());
#endif
#if CC_ALG == MAAT
          time_table.init(get_thd_id(),txn_man->get_txn_id());
          assert(time_table.get_lower(get_thd_id(),txn_man->get_txn_id()) == 0);
          assert(time_table.get_upper(get_thd_id(),txn_man->get_txn_id()) == UINT64_MAX);
          assert(time_table.get_state(get_thd_id(),txn_man->get_txn_id()) == MAAT_RUNNING);
#endif

    rc = init_phase();
    if(rc != RCOK)
      return rc;

    // Execute transaction
    rc = txn_man->run_txn();
  check_if_done(rc);
    return rc;
}

RC WorkerThread::init_phase() {
  RC rc = RCOK;
  //m_query->part_touched[m_query->part_touched_cnt++] = m_query->part_to_access[0];
  return rc;
}


RC WorkerThread::process_log_msg(Message * msg) {
  assert(ISREPLICA);
  DEBUG("REPLICA PROCESS %ld\n",msg->get_txn_id());
  LogRecord * record = logger.createRecord(&((LogMessage*)msg)->record);
  logger.enqueueRecord(record);
  return RCOK;
}

RC WorkerThread::process_log_msg_rsp(Message * msg) {
  DEBUG("REPLICA RSP %ld\n",msg->get_txn_id());
  txn_man->repl_finished = true;
  if(txn_man->log_flushed)
    commit();
  return RCOK;
}

RC WorkerThread::process_log_flushed(Message * msg) {
  DEBUG("LOG FLUSHED %ld\n",msg->get_txn_id());
  if(ISREPLICA) {
    msg_queue.enqueue(get_thd_id(),Message::create_message(msg->txn_id,LOG_MSG_RSP),GET_NODE_ID(msg->txn_id)); 
    return RCOK;
  }

  txn_man->log_flushed = true;
  if(g_repl_cnt == 0 || txn_man->repl_finished)
    commit();
  return RCOK; 
}

RC WorkerThread::process_rfwd(Message * msg) {
  DEBUG("RFWD (%ld,%ld)\n",msg->get_txn_id(),msg->get_batch_id());
  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  assert(CC_ALG == CALVIN);
  int responses_left = txn_man->received_response(((ForwardMessage*)msg)->rc);
  assert(responses_left >=0);
  if(txn_man->calvin_collect_phase_done()) {
    assert(ISSERVERN(txn_man->return_id));
    RC rc = txn_man->run_calvin_txn();
    if(rc == RCOK && txn_man->calvin_exec_phase_done()) {
      calvin_wrapup();
      return RCOK;
    }   
  }
  return WAIT;

}

RC WorkerThread::process_calvin_rtxn(Message * msg) {

  DEBUG("START %ld %f %lu\n",txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),txn_man->txn_stats.starttime);
  assert(ISSERVERN(txn_man->return_id));
  txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  // Execute
  RC rc = txn_man->run_calvin_txn();
  //if((txn_man->phase==6 && rc == RCOK) || txn_man->active_cnt == 0 || txn_man->participant_cnt == 1) {
  if(rc == RCOK && txn_man->calvin_exec_phase_done()) {
    calvin_wrapup();
  }
  return RCOK;

}


bool WorkerThread::is_cc_new_timestamp() {
  return (CC_ALG == MVCC || CC_ALG == TIMESTAMP);
}

ts_t WorkerThread::get_next_ts() {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager.get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager.get_ts(get_thd_id());
		return _curr_ts;
	}
}

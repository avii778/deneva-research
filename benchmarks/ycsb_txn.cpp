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

#include "catalog.h"
#include "global.h"
#include "helper.h"
#include "index_btree.h"
#include "index_hash.h"
#include "manager.h"
#include "mem_alloc.h"
#include "message.h"
#include "msg_queue.h"
#include "query.h"
#include "row.h"
#include "row_life.h"
#include "row_lock.h"
#include "row_mvcc.h"
#include "row_ts.h"
#include "table.h"
#include "thread.h"
#include "wl.h"
#include "work_queue.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>

#if CC_ALG == LIFE
namespace {

void add_unique_life_node(std::vector<uint64_t> &nodes, uint64_t node_id) {
  if (std::find(nodes.begin(), nodes.end(), node_id) == nodes.end())
    nodes.push_back(node_id);
}

LifeTxnDescriptor
make_life_finish_message_descriptor(const LifeTxnDescriptor &descriptor,
                                    uint64_t node_id) {
  LifeTxnDescriptor message_descriptor;
  message_descriptor.pid = descriptor.pid;
  message_descriptor.tid = descriptor.tid;
  message_descriptor.workload = descriptor.workload;
  message_descriptor.ycsb.state = descriptor.ycsb.state;
  message_descriptor.ycsb.next_record_id = descriptor.ycsb.next_record_id;
  message_descriptor.pps = descriptor.pps;
  message_descriptor.history.reserve(descriptor.history.size());
  message_descriptor.touched_objects.reserve(descriptor.touched_objects.size());
  for (std::vector<LifeHistoryEntry>::const_iterator it =
           descriptor.history.begin();
       it != descriptor.history.end(); ++it) {
    if (GET_NODE_ID(it->operation.object.partition_id) == node_id)
      life_append_history(message_descriptor, *it);
  }
  return message_descriptor;
}

#if LOG_LIFE
pthread_mutex_t life_log_mutex = PTHREAD_MUTEX_INITIALIZER;

int life_log_fd() {
  static const int fd =
      open(LIFE_LOG_FILE, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0) {
    fprintf(stderr, "Failed to open LIFE log %s: %s\n", LIFE_LOG_FILE,
            strerror(errno));
    abort();
  }
  return fd;
}

void persist_life_timing(uint64_t txn_id, uint64_t node_id, uint64_t worker_id,
                         uint64_t attempts, uint64_t own_time,
                         uint64_t help_time, uint64_t finalize_time) {
  char record[256];
  const int record_size = snprintf(
      record, sizeof(record),
      "LIFE_TIMING txn_id=%lu node_id=%lu worker_id=%lu attempts=%lu "
      "own_ns=%lu helping_ns=%lu finalization_ns=%lu\n",
      txn_id, node_id, worker_id, attempts, own_time, help_time, finalize_time);
  assert(record_size > 0 && static_cast<size_t>(record_size) < sizeof(record));

  pthread_mutex_lock(&life_log_mutex);
  const int fd = life_log_fd();
  size_t written = 0;
  while (written < static_cast<size_t>(record_size)) {
    const ssize_t result =
        write(fd, record + written, static_cast<size_t>(record_size) - written);
    if (result < 0 && errno == EINTR)
      continue;
    if (result <= 0) {
      fprintf(stderr, "Failed to write LIFE log %s: %s\n", LIFE_LOG_FILE,
              strerror(errno));
      abort();
    }
    written += static_cast<size_t>(result);
  }
  if (fdatasync(fd) != 0) {
    fprintf(stderr, "Failed to sync LIFE log %s: %s\n", LIFE_LOG_FILE,
            strerror(errno));
    abort();
  }
  pthread_mutex_unlock(&life_log_mutex);
}
#endif

} // namespace
#endif

#if CC_ALG == LIFE
extern volatile uint64_t life_dbg_execute_sent;
extern volatile uint64_t life_dbg_execute_rsp_sent;
extern volatile uint64_t life_dbg_prepare_sent;
extern volatile uint64_t life_dbg_finish_sent;
extern volatile uint64_t life_dbg_finalize_sent;
extern volatile uint64_t life_dbg_finalize_rsp_sent;
extern volatile uint64_t life_dbg_execute_rsp_stale;
extern volatile uint64_t life_dbg_execute_duplicate_wait;
extern volatile uint64_t life_dbg_prepare_rsp_duplicate;
#ifndef LIFE_DEBUG_COUNTERS
#define LIFE_DEBUG_COUNTERS false
#endif
#if LIFE_DEBUG_COUNTERS
#define LIFE_DBG_INC(counter) __sync_fetch_and_add(&(counter), 1)
#else
#define LIFE_DBG_INC(counter) ((void)0)
#endif
#endif

YCSBTxnManager::YCSBTxnManager()
    : life_piggyback_prepared_tid(),
      life_piggyback_prepared_tid_valid(false),
      life_finalize_requester_nodes(NULL),
      life_finalize_requester_txn_ids(NULL), life_wait_stacks(NULL),
      life_active(false), life_next_wait_id(1) {
  h_thd = NULL;
  h_wl = NULL;
  txn = NULL;
  query = NULL;
  return_id = UINT64_MAX;
  client_startts = 0;
  client_id = 0;
  abort_cnt = 0;
  txn_ready = 1;
}

YCSBTxnManager::~YCSBTxnManager() {
  delete life_finalize_requester_nodes;
  delete life_finalize_requester_txn_ids;
  delete life_wait_stacks;
}

void YCSBTxnManager::init(uint64_t thd_id, Workload *h_wl) {
  if (life_finalize_requester_nodes == NULL)
    life_finalize_requester_nodes = new std::vector<uint64_t>();
  if (life_finalize_requester_txn_ids == NULL)
    life_finalize_requester_txn_ids = new std::vector<uint64_t>();
  if (life_wait_stacks == NULL)
    life_wait_stacks = new std::vector<LifeWaitContext>();
  life_finalize_requester_nodes->reserve(g_node_cnt);
  life_finalize_requester_txn_ids->reserve(g_node_cnt);
  life_wait_stacks->reserve(g_req_per_query);
  life_prepare_response_nodes.reserve(g_node_cnt);
  life_pending_prepare_nodes.reserve(g_node_cnt);
  life_piggyback_prepared_nodes.reserve(g_node_cnt);
  life_txn_stack.reserve(g_req_per_query);
  life_response_stack.reserve(g_req_per_query);
  life_object_scratch.reserve(g_req_per_query);
  TxnManager::init(thd_id, h_wl);
  _wl = (YCSBWorkload *)h_wl;
  reset();
}

void YCSBTxnManager::reset() {
  state = YCSB_0;
  next_record_id = 0;
#if CC_ALG == LIFE
  life_active = false;
  reset_pending_life_finalize();
  life_wait_stacks->clear();
  reset_life_piggyback_prepare();
  life_next_wait_id = 1;
#endif
#if LOG_LIFE
  life_help_time = 0;
  life_own_time = 0;
  life_finalize_time = 0;
#endif
  TxnManager::reset();
}

#if CC_ALG == LIFE
void YCSBTxnManager::life_reset_workload() {
  state = YCSB_0;
  next_record_id = 0;
  reset_pending_life_finalize();
  reset_life_piggyback_prepare();
}

bool YCSBTxnManager::is_life_active() const { return life_active; }

void YCSBTxnManager::mark_life_active() { life_active = true; }

void YCSBTxnManager::clear_life_active() {
  life_active = false;
  state = YCSB_0;
  next_record_id = 0;
  reset_pending_life_finalize();
  life_wait_stacks->clear();
  reset_life_piggyback_prepare();
  life_next_wait_id = 1;
}
#endif

RC YCSBTxnManager::acquire_locks() {
  uint64_t starttime = get_sys_clock();
  assert(CC_ALG == CALVIN);
  YCSBQuery *ycsb_query = (YCSBQuery *)query;
  locking_done = false;
  RC rc = RCOK;
  incr_lr();
  assert(ycsb_query->requests.size() == g_req_per_query);
  assert(phase == CALVIN_RW_ANALYSIS);
  for (uint32_t rid = 0; rid < ycsb_query->requests.size(); rid++) {
    ycsb_request *req = ycsb_query->requests[rid];
    uint64_t part_id = _wl->key_to_part(req->key);
    DEBUG("LK Acquire (%ld,%ld) %d,%ld -> %ld\n", get_txn_id(), get_batch_id(),
          req->acctype, req->key, GET_NODE_ID(part_id));
    if (GET_NODE_ID(part_id) != g_node_id)
      continue;
    INDEX *index = _wl->the_index;
    itemid_t *item;
    item = index_read(index, req->key, part_id);
    row_t *row = ((row_t *)item->location);
    RC rc2 = get_lock(row, req->acctype);
    if (rc2 != RCOK) {
      rc = rc2;
    }
  }
  if (decr_lr() == 0) {
    if (ATOM_CAS(lock_ready, false, true))
      rc = RCOK;
  }
  txn_stats.wait_starttime = get_sys_clock();
  /*
  if(rc == WAIT && lock_ready_cnt == 0) {
    if(ATOM_CAS(lock_ready,false,true))
    //lock_ready = true;
      rc = RCOK;
  }
  */
  INC_STATS(get_thd_id(), calvin_sched_time, get_sys_clock() - starttime);
  locking_done = true;
  return rc;
}

RC YCSBTxnManager::run_txn() {
  RC rc = RCOK;
  assert(CC_ALG != CALVIN);

  if (IS_LOCAL(txn->txn_id) && state == YCSB_0 && next_record_id == 0) {
    DEBUG("Running txn %ld\n", txn->txn_id);
    // query->print();
    query->partitions_touched.add_unique(GET_PART_ID(0, g_node_id));
  }

#if CC_ALG == LIFE
  return run_life_txn();
#endif

  uint64_t starttime = get_sys_clock();

  while (rc == RCOK && !is_done()) {
    rc = run_txn_state();
  }

  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  txn_stats.wait_starttime = get_sys_clock();

  if (IS_LOCAL(get_txn_id())) {
    if (is_done() && rc == RCOK)
      rc = start_commit();
    else if (rc == Abort)
      rc = start_abort();
  } else if (rc == Abort) {
    rc = abort();
  }

  return rc;
}

#if CC_ALG == LIFE
RC YCSBTxnManager::run_life_txn() {
  uint64_t starttime = get_sys_clock();

  life_txn_stack.clear();
  life_txn_stack.push_back(life_descriptor());

  const bool complete = try_life_transactions(life_txn_stack);

  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  txn_stats.wait_starttime = get_sys_clock();

  if (!complete)
    return WAIT_REM;

  return complete_life_home_transaction();
}

RC YCSBTxnManager::complete_life_home_transaction() {
  if (query == NULL || query->partitions_touched.size() == 0)
    return RCOK;

  txn->life_status = LifeTxnStatus::Committed;
#if LOG_LIFE
  persist_life_timing(get_txn_id(), g_node_id, get_thd_id(),
                      txn->life_tid.attempt, life_own_time, life_help_time,
                      life_finalize_time);
#endif
  return commit();
}

bool YCSBTxnManager::try_life_transactions(
    std::vector<LifeTxnDescriptor> &txns) {
  while (!txns.empty()) {
    LifeTxnDescriptor &ctx = txns.back();
    life_reconcile_descriptor(ctx);

    if (life_program_done(ctx)) {
#if LOG_LIFE
      const uint64_t finalize_start = get_sys_clock();
#endif
      const bool finalized = finalize_life_descriptor(ctx);
#if LOG_LIFE
      life_finalize_time += get_sys_clock() - finalize_start;
#endif
      if (life_finalize_waiting) {
        const uint64_t wait_id = allocate_life_wait_id();
        save_life_wait_stack(txns, wait_id, 2,
                             GET_NODE_ID(life_pending_finalize.tid.time),
                             life_pending_finalize.tid.time);
        return false;
      }

      if (finalized) {
        const bool owns_descriptor =
            ctx.pid == txn->life_pid &&
            ctx.tid.time == txn->life_tid.time;
        if (owns_descriptor) {
          // The owned descriptor is the bottom of the coordinator's stack.
          // If it is ready to pop, every helped descriptor above it has
          // already completed. Record protocol completion before the stack is
          // emptied so response handlers run only the framework epilogue
          // instead of reconstructing and finalizing this descriptor again.
          assert(txns.size() == 1);
          txn->life_tid = ctx.tid;
          txn->life_history = ctx.history;
          txn->life_objects.clear();
          txn->life_status = LifeTxnStatus::Committed;
          life_copy_program_to_workload(ctx);
        }
        txns.pop_back();
      }
      continue;
    }

    LifeOperation pending_operation = life_current_operation(ctx);
    const uint64_t part_id = pending_operation.object.partition_id;
    if (GET_NODE_ID(part_id) != g_node_id) {
#if LIFE_DEBUG_COUNTERS
      if (has_life_wait_stack(ctx, 1, GET_NODE_ID(part_id),
                              pending_operation.object.primary_key))
        LIFE_DBG_INC(life_dbg_execute_duplicate_wait);
#endif
      const uint64_t wait_id = allocate_life_wait_id();
      send_life_execute(ctx, pending_operation, wait_id);
      save_life_wait_stack(txns, wait_id, 1, GET_NODE_ID(part_id),
                           pending_operation.object.primary_key);
      return false;
    }

    LifeOperation operation;
#if LOG_LIFE
    const uint64_t operation_start = get_sys_clock();
#endif
    LifeExecuteResult result = execute_life_operation(ctx, operation);
#if LOG_LIFE
    const uint64_t operation_time = get_sys_clock() - operation_start;
    if (ctx.pid == txn->life_pid && ctx.tid.time == txn->life_tid.time)
      life_own_time += operation_time;
    else
      life_help_time += operation_time;
#endif

    switch (result.code) {

    case LifeResultCode::Success:
      life_advance_program(ctx, operation, result.response);
      break;

    case LifeResultCode::Finalize: {
      LifeTxnDescriptor response = std::move(result.transaction);
      const uint64_t response_time = response.tid.time;
      const uint64_t ctx_time = ctx.tid.time;
#if LOG_LIFE
      const uint64_t finalize_start = get_sys_clock();
#endif
      finalize_life_descriptor(response);
#if LOG_LIFE
      life_finalize_time += get_sys_clock() - finalize_start;
#endif
      if (life_finalize_waiting) {
        const uint64_t wait_id = allocate_life_wait_id();
        save_life_wait_stack(txns, wait_id, 2,
                             GET_NODE_ID(life_pending_finalize.tid.time),
                             life_pending_finalize.tid.time);
        return false;
      }
      // Algorithm 2/3 continues helping an older descriptor even when
      // finalization reset it to a newer attempt.
      if (response_time < ctx_time)
        txns.push_back(response);
      break;
    }

    case LifeResultCode::Help: {
      push_life_help_descriptor(txns, result.transaction);
      break;
    }

    case LifeResultCode::Committed:
      txns.pop_back();
      break;

    case LifeResultCode::Retry:
      reset_life_descriptor(ctx, result.observed_attempt);
      break;

    default:
      assert(false);
      return false;
    }
  }

  return true;
}

LifeExecuteResult
YCSBTxnManager::execute_life_operation(LifeTxnDescriptor &descriptor,
                                       LifeOperation &operation) {

  const LifeYcsbRequest &request =
      descriptor.ycsb.requests[descriptor.ycsb.next_record_id];

  const bool owns_descriptor = descriptor.pid == txn->life_pid &&
                               descriptor.tid.time == txn->life_tid.time;
  row_t *life_row = owns_descriptor ? request.row : NULL;
  if (life_row == NULL)
    life_row = lookup_life_row(request.key);
  operation = make_life_operation(life_row, request);

  return life_row->execute_life(descriptor, operation);
}

RC YCSBTxnManager::send_life_execute(const LifeTxnDescriptor &descriptor,
                                     const LifeOperation &operation,
                                     uint64_t wait_id) {
  uint64_t dest_node_id = GET_NODE_ID(operation.object.partition_id);
  assert(dest_node_id != g_node_id);
  const uint64_t stop_record_id =
      life_remote_batch_stop(descriptor, dest_node_id);
  assert(stop_record_id > life_program_position(descriptor));
  if (query != NULL)
    query->partitions_touched.add_unique(operation.object.partition_id);

  LifeExecuteMessage *msg =
      (LifeExecuteMessage *)Message::create_message(RLIFE_EXECUTE);
  msg->txn_id = get_txn_id();
  msg->descriptor = descriptor;
  msg->operation = operation;
  msg->wait_id = wait_id;
  msg->stop_record_id = stop_record_id;
  // attempt changes on retry while txn->life_tid retains the transaction's
  // stable identity, so ownership is pid + logical timestamp, not attempt.
  const bool owns_descriptor =
      descriptor.pid == txn->life_pid &&
      descriptor.tid.time == txn->life_tid.time;
  // YCSB knows statically that its final destination batch reaches program
  // end. PPS is data-dependent, so ask the receiver to prepare only if this
  // execute happens to finish the program. The existing response bookkeeping
  // records preparation against the exact attempt and responder node.
  msg->prepare_after_execute =
      owns_descriptor &&
      ((descriptor.workload == LifeWorkloadKind::Ycsb &&
        stop_record_id == descriptor.ycsb.requests.size()) ||
       descriptor.workload == LifeWorkloadKind::Pps);
  LIFE_DBG_INC(life_dbg_execute_sent);
  msg_queue.enqueue(get_thd_id(), msg, dest_node_id);
  return WAIT_REM;
}

LifeExecuteResult
YCSBTxnManager::execute_life_remote(const LifeTxnDescriptor &descriptor,
                                    const LifeOperation &operation) {
  Row_life *manager = operation.manager;
  if (manager == NULL)
    manager = lookup_life_row(operation.object)->manager;
  assert(manager != NULL);

  LifeTxnDescriptor local_descriptor = descriptor;
  LifeOperation local_operation = operation;
  local_operation.manager = manager;
  return manager->execute(local_descriptor, local_operation);
}

RC YCSBTxnManager::serve_life_execute(const LifeTxnDescriptor &descriptor,
                                      const LifeOperation &operation,
                                      uint64_t stop_record_id,
                                      bool prepare_after_execute,
                                      LifeExecuteResult &immediate_result,
                                      bool &prepared) {
  prepared = false;
  LifeTxnDescriptor response_descriptor = descriptor;
  if (stop_record_id <= life_program_position(response_descriptor))
    stop_record_id = life_program_position(response_descriptor) + 1;

  // Validate the routing operation against the continuation exactly as the
  // sender saw it. PPS reconciliation can legitimately replace this operation
  // without advancing the history position when the owner of a relation index
  // discovers end-of-scan. Comparing after reconciliation would mistake that
  // phase transition for a corrupted request.
  assert(!life_program_done(response_descriptor));
  const LifeOperation requested_operation =
      life_current_operation(response_descriptor);
  if (requested_operation.object != operation.object) {
    std::fprintf(stderr,
                 "LIFE execute object mismatch: expected=(%lu,%lu,%lu,%lu) "
                 "received=(%lu,%lu,%lu,%lu) position=%lu history=%lu\n",
                 requested_operation.object.table_id,
                 requested_operation.object.partition_id,
                 requested_operation.object.primary_key,
                 requested_operation.object.row_id, operation.object.table_id,
                 operation.object.partition_id, operation.object.primary_key,
                 operation.object.row_id, life_program_position(descriptor),
                 static_cast<unsigned long>(descriptor.history.size()));
  }
  assert(requested_operation.object == operation.object);
  assert(GET_NODE_ID(operation.object.partition_id) == g_node_id);

  immediate_result = LifeExecuteResult();
  immediate_result.code = LifeResultCode::InvalidOperation;
  while (life_program_position(response_descriptor) < stop_record_id) {
    life_reconcile_descriptor(response_descriptor);
    if (life_program_done(response_descriptor)) {
      // Reconciliation itself may complete an empty PPS scan. Returning the
      // updated continuation with no history delta lets the coordinator
      // finalize without another execute round trip.
      immediate_result.code = LifeResultCode::Success;
      break;
    }
    LifeOperation local_operation =
        life_current_operation(response_descriptor);
    const uint64_t part_id = local_operation.object.partition_id;
    if (GET_NODE_ID(part_id) != g_node_id) {
      // A PPS scan can end on this node and expose a part whose owner is a
      // different node. Return the reconciled program state immediately so
      // the coordinator can route there. If the part remains local, continue
      // below and retain destination batching throughput.
      immediate_result.code = LifeResultCode::Success;
      break;
    }

    immediate_result =
        execute_life_remote(response_descriptor, local_operation);

    if (immediate_result.code != LifeResultCode::Success)
      break;

    life_advance_program(response_descriptor, local_operation,
                         immediate_result.response);
  }

  if (immediate_result.code == LifeResultCode::Success) {
    immediate_result.transaction = response_descriptor;
    immediate_result.observed_attempt = response_descriptor.tid.attempt;

    // PPS cannot know at send time which remote execute will finish its
    // data-dependent scan/program. Treat the request bit as "prepare if done"
    // and never prepare a prefix that stopped at a destination change.
    if (prepare_after_execute && life_program_done(response_descriptor)) {
      const LifeExecuteResult prepare_result =
          prepare_life_remote(response_descriptor);
      if (prepare_result.code == LifeResultCode::Success) {
        prepared = true;
      } else {
        // prepare_life_remote() can prepare an earlier row before a later row
        // rejects the attempt. Roll back the complete local descriptor so a
        // retry cannot leave an unrecorded prepared suffix behind.
        if (prepare_result.code != LifeResultCode::Committed)
          finish_life_remote(response_descriptor, Abort);
        immediate_result = prepare_result;
      }
    }
  }

  if (immediate_result.code == LifeResultCode::Retry)
    rollback_life_descriptor(response_descriptor);

  if (!life_program_present(immediate_result.transaction))
    immediate_result.transaction = response_descriptor;

  // Match Algorithm 1: Execute returns Help/Finalize to its caller. The
  // requester owns the transaction stack and will decide which descriptor to
  // help or finalize next.
  return RCOK;
}

LifeExecuteResult
YCSBTxnManager::prepare_life_remote(const LifeTxnDescriptor &descriptor) {
  life_object_scratch.clear();
  const std::vector<LifeFinalizeObject> *objects = &descriptor.touched_objects;
  size_t cached_indices = 0;
  for (std::vector<LifeFinalizeObject>::const_iterator it = objects->begin();
       it != objects->end(); ++it) {
    cached_indices += it->history_indices.size();
  }
  if (cached_indices != descriptor.history.size()) {
    collect_life_objects(descriptor, life_object_scratch);
    objects = &life_object_scratch;
  }

  LifeTxnDescriptorPtr frozen = std::make_shared<LifeTxnDescriptor>(descriptor);
  LifeExecuteResult response;
  response.code = LifeResultCode::Success;
  response.transaction = descriptor;
  response.observed_attempt = descriptor.tid.attempt;
  bool prepared_object = false;
  bool found_local_object = false;

  for (std::vector<LifeFinalizeObject>::const_iterator it = objects->begin();
       it != objects->end(); ++it) {
    if (GET_NODE_ID(it->object.partition_id) != g_node_id)
      continue;
    found_local_object = true;

    Row_life *manager = it->manager;
    if (manager == NULL)
      manager = lookup_life_row(it->object)->manager;
    assert(manager != NULL);

    const LifeExecuteResult result = manager->prepare(frozen);
    if (result.code == LifeResultCode::Success) {
      prepared_object = true;
      continue;
    }
    if (result.code == LifeResultCode::Committed) {
      continue;
    }

    if (result.code == LifeResultCode::Retry)
      response.observed_attempt = result.observed_attempt;
    response.code = result.code;
    return response;
  }

  // A duplicate prepare can arrive after the original finish released its
  // remote transaction manager. Report an all-committed result distinctly so
  // the worker does not retain a newly created manager that will never receive
  // another finish. If any row was newly/repeatedly prepared, Success remains
  // the correct response and the manager stays durable until RLIFE_FINISH.
  if (found_local_object && !prepared_object)
    response.code = LifeResultCode::Committed;

  return response;
}

LifeExecuteResult
YCSBTxnManager::finish_life_remote(const LifeTxnDescriptor &descriptor,
                                   RC decision) {
  life_object_scratch.clear();
  const std::vector<LifeFinalizeObject> *objects = &descriptor.touched_objects;
  size_t cached_indices = 0;
  for (std::vector<LifeFinalizeObject>::const_iterator it = objects->begin();
       it != objects->end(); ++it) {
    cached_indices += it->history_indices.size();
  }
  if (cached_indices != descriptor.history.size()) {
    collect_life_objects(descriptor, life_object_scratch);
    objects = &life_object_scratch;
  }

  LifeTxnDescriptorPtr frozen = std::make_shared<LifeTxnDescriptor>(descriptor);
  for (std::vector<LifeFinalizeObject>::const_iterator it = objects->begin();
       it != objects->end(); ++it) {
    if (GET_NODE_ID(it->object.partition_id) != g_node_id)
      continue;

    Row_life *manager = it->manager;
    if (manager == NULL)
      manager = lookup_life_row(it->object)->manager;
    assert(manager != NULL);

    if (decision == Commit)
      manager->commit(frozen, it->history_indices);
    else
      manager->rollback(descriptor);
  }

  LifeExecuteResult response;
  response.code = LifeResultCode::Success;
  response.transaction = descriptor;
  return response;
}

RC YCSBTxnManager::help_life_remote(const LifeTxnDescriptor &descriptor) {
  life_response_stack.clear();
  life_response_stack.push_back(descriptor);
  return try_life_transactions(life_response_stack) ? RCOK : WAIT_REM;
}

RC YCSBTxnManager::apply_life_help_request(const LifeTxnDescriptor &descriptor,
                                           uint64_t requester_node_id) {
  (void)requester_node_id;
  return help_life_remote(descriptor);
}

RC YCSBTxnManager::apply_life_finalize_request(
    const LifeTxnDescriptor &descriptor, uint64_t requester_node_id,
    uint64_t requester_txn_id) {
  add_life_finalize_requester(requester_node_id, requester_txn_id);
  if (life_finalize_waiting) {
    return WAIT_REM;
  }

  LifeTxnDescriptor finalize = descriptor;
  const bool finalized = finalize_life_descriptor(finalize);
  if (life_finalize_waiting) {
    return WAIT_REM;
  }

  const bool owns_descriptor =
      finalize.pid == txn->life_pid && finalize.tid.time == txn->life_tid.time;
  LifeExecuteResult response = LifeExecuteResult();
  response.code = finalized ? LifeResultCode::Success : LifeResultCode::Retry;
  response.transaction = finalize;
  response.observed_attempt = finalize.tid.attempt;
  send_life_finalize_response(response, requester_node_id, requester_txn_id);

  if (finalized) {
    if (!owns_descriptor || query == NULL ||
        query->partitions_touched.size() == 0)
      return RCOK;

    copy_life_descriptor_to_workload(finalize);
    txn->life_status = LifeTxnStatus::Committed;
    return commit();
  }

  if (!owns_descriptor || query == NULL)
    return RCOK;

  copy_life_descriptor_to_workload(finalize);
  return run_life_txn();
}

RC YCSBTxnManager::apply_life_finalize_response(
    const LifeExecuteResult &result) {
  life_response_stack.clear();
  std::vector<LifeTxnDescriptor> &txns = life_response_stack;
  const bool has_saved_stack =
      take_life_wait_stack_by_descriptor(2, result.transaction, txns);
  reset_pending_life_finalize();

  if (result.code == LifeResultCode::Retry) {
    const bool owns_response =
        result.transaction.pid == txn->life_pid &&
        result.transaction.tid.time == txn->life_tid.time;
    if (has_saved_stack && !txns.empty()) {
      // Finalization messages omit the workload program (YCSB requests and
      // PPS-discovered keys). Use the returned history to undo row state, but
      // reset the complete program retained on the coordinator stack as
      // required by Algorithm 2's ResetTransaction.
      LifeTxnDescriptor rollback = result.transaction;
      rollback_life_descriptor(rollback);
      LifeTxnDescriptor retry = txns.back();
      retry.tid.attempt =
          std::max(std::max(retry.tid.attempt,
                            result.transaction.tid.attempt),
                   result.observed_attempt) + 1;
      retry.history.clear();
      retry.touched_objects.clear();
      life_reset_program(retry);
      txns.back() = retry;
      return try_life_transactions(txns) ? continue_life_after_stack()
                                         : WAIT_REM;
    }
    if (owns_response)
      copy_life_descriptor_to_workload(result.transaction);
    return continue_life_after_stack();
  }

  if (result.code == LifeResultCode::Success ||
      result.code == LifeResultCode::Committed) {
    if (has_saved_stack && !txns.empty()) {
      const LifeTxnDescriptor &finished = result.transaction;
      if (txns.back().pid == finished.pid &&
          txns.back().tid.time == finished.tid.time)
        txns.pop_back();
      if (txns.empty())
        return continue_life_after_stack();
      return try_life_transactions(txns) ? continue_life_after_stack()
                                         : WAIT_REM;
    }
    return continue_life_after_stack();
  }

  if (result.code == LifeResultCode::Help ||
      result.code == LifeResultCode::Finalize) {
    LifeTxnDescriptor descriptor = LifeTxnDescriptor();
    const bool has_current_descriptor = query != NULL;
    if (has_current_descriptor)
      descriptor = life_descriptor();

    if (!has_saved_stack && has_current_descriptor)
      txns.push_back(descriptor);
    txns.push_back(result.transaction);
    return try_life_transactions(txns) ? continue_life_after_stack() : WAIT_REM;
  }

  return Abort;
}

void YCSBTxnManager::copy_life_descriptor_to_workload(
    const LifeTxnDescriptor &descriptor) {
  txn->life_tid = descriptor.tid;
  txn->life_history = descriptor.history;
  txn->life_objects.clear();
  life_copy_program_to_workload(descriptor);
}

uint64_t YCSBTxnManager::allocate_life_wait_id() {
  uint64_t wait_id = life_next_wait_id++;
  if (wait_id == UINT64_MAX)
    wait_id = life_next_wait_id++;
  return wait_id;
}

void YCSBTxnManager::save_life_wait_stack(
    std::vector<LifeTxnDescriptor> &txns, uint64_t wait_id, uint32_t reason,
    uint64_t remote_node_id, uint64_t remote_key) {
  assert(!txns.empty());
  LifeWaitContext context;
  context.wait_id = wait_id;
  context.reason = reason;
  context.remote_node_id = remote_node_id;
  context.remote_key = remote_key;
  context.stack.swap(txns);
  life_wait_stacks->push_back(std::move(context));
}

bool YCSBTxnManager::has_life_wait_stack(const LifeTxnDescriptor &descriptor,
                                         uint32_t reason,
                                         uint64_t remote_node_id,
                                         uint64_t remote_key) const {
  for (std::vector<LifeWaitContext>::const_iterator it =
           life_wait_stacks->begin();
       it != life_wait_stacks->end(); ++it) {
    if (it->stack.empty())
      continue;
    const LifeTxnDescriptor &waiting = it->stack.back();
    if (it->reason == reason && it->remote_node_id == remote_node_id &&
        it->remote_key == remote_key && waiting.pid == descriptor.pid &&
        waiting.tid == descriptor.tid &&
        life_program_position(waiting) == life_program_position(descriptor))
      return true;
  }
  return false;
}

bool YCSBTxnManager::take_life_wait_stack(
    uint64_t wait_id, std::vector<LifeTxnDescriptor> &txns) {
  for (std::vector<LifeWaitContext>::iterator it = life_wait_stacks->begin();
       it != life_wait_stacks->end(); ++it) {
    if (it->wait_id != wait_id)
      continue;
    txns.swap(it->stack);
    life_wait_stacks->erase(it);
    return true;
  }
  return false;
}

bool YCSBTxnManager::take_life_wait_stack_by_reason(
    uint32_t reason, std::vector<LifeTxnDescriptor> &txns) {
  for (std::vector<LifeWaitContext>::iterator it = life_wait_stacks->begin();
       it != life_wait_stacks->end(); ++it) {
    if (it->reason != reason)
      continue;
    txns.swap(it->stack);
    life_wait_stacks->erase(it);
    return true;
  }
  return false;
}

bool YCSBTxnManager::take_life_wait_stack_by_descriptor(
    uint32_t reason, const LifeTxnDescriptor &descriptor,
    std::vector<LifeTxnDescriptor> &txns) {
  for (std::vector<LifeWaitContext>::iterator it = life_wait_stacks->begin();
       it != life_wait_stacks->end(); ++it) {
    if (it->reason != reason)
      continue;
    if (it->remote_key != descriptor.tid.time)
      continue;

    bool found = false;
    for (std::vector<LifeTxnDescriptor>::const_iterator ctx = it->stack.begin();
         ctx != it->stack.end(); ++ctx) {
      if (ctx->pid == descriptor.pid && ctx->tid.time == descriptor.tid.time) {
        found = true;
        break;
      }
    }
    if (!found)
      continue;

    txns.swap(it->stack);
    life_wait_stacks->erase(it);
    return true;
  }
  return false;
}

bool YCSBTxnManager::pending_life_finalize_matches(const LifeProcessId &pid,
                                                   const LifeTxnId &tid) const {
  return life_finalize_waiting && life_pending_finalize.pid == pid &&
         life_pending_finalize.tid.time == tid.time &&
         life_pending_finalize.tid.attempt == tid.attempt;
}

bool YCSBTxnManager::note_life_prepare_response(uint64_t responder_node_id) {
  for (std::vector<uint64_t>::const_iterator it =
           life_prepare_response_nodes.begin();
       it != life_prepare_response_nodes.end(); ++it) {
    if (*it == responder_node_id)
      return false;
  }
  life_prepare_response_nodes.push_back(responder_node_id);
  return true;
}

void YCSBTxnManager::reset_life_piggyback_prepare() {
  life_piggyback_prepared_nodes.clear();
  life_piggyback_prepared_tid = LifeTxnId();
  life_piggyback_prepared_tid_valid = false;
}

void YCSBTxnManager::note_life_piggyback_prepare(
    const LifeTxnDescriptor &descriptor, uint64_t node_id) {
  // Helped descriptors deliberately keep the existing distributed finalize
  // path because their envelope transaction can have a different owner.
  if (descriptor.pid != txn->life_pid ||
      descriptor.tid.time != txn->life_tid.time)
    return;

  if (!life_piggyback_prepared_tid_valid ||
      life_piggyback_prepared_tid != descriptor.tid) {
    life_piggyback_prepared_nodes.clear();
    life_piggyback_prepared_tid = descriptor.tid;
    life_piggyback_prepared_tid_valid = true;
  }
  add_unique_life_node(life_piggyback_prepared_nodes, node_id);
}

bool YCSBTxnManager::has_life_piggyback_prepare(
    const LifeTxnDescriptor &descriptor, uint64_t node_id) const {
  if (!life_piggyback_prepared_tid_valid ||
      life_piggyback_prepared_tid != descriptor.tid ||
      descriptor.pid != txn->life_pid ||
      descriptor.tid.time != txn->life_tid.time)
    return false;
  return std::find(life_piggyback_prepared_nodes.begin(),
                   life_piggyback_prepared_nodes.end(),
                   node_id) != life_piggyback_prepared_nodes.end();
}

void YCSBTxnManager::debug_life_state() const {
  uint32_t wait_reason = 0;
  uint64_t wait_node = UINT64_MAX;
  uint64_t wait_key = UINT64_MAX;
  size_t wait_depth = 0;
  if (!life_wait_stacks->empty()) {
    const LifeWaitContext &context = life_wait_stacks->front();
    wait_reason = context.reason;
    wait_node = context.remote_node_id;
    wait_key = context.remote_key;
    wait_depth = context.stack.size();
  }
  printf(" life_waiting=%d prep=%lu wait_stack=%d wait_depth=%lu "
         "wait_reason=%u wait_node=%lu wait_key=%lu wait_slots=%lu "
         "pending_tid=%lu/%lu "
         "owner_tid=%lu/%lu state=%d next=%lu",
         life_finalize_waiting ? 1 : 0, life_prepare_pending,
         life_wait_stacks->empty() ? 0 : 1, wait_depth,
         wait_reason, wait_node, wait_key, life_wait_stacks->size(),
         life_pending_finalize.tid.time, life_pending_finalize.tid.attempt,
         txn == NULL ? 0UL : txn->life_tid.time,
         txn == NULL ? 0UL : txn->life_tid.attempt, (int)state, next_record_id);
}

void YCSBTxnManager::reset_pending_life_finalize() {
  life_finalize_waiting = false;
  life_prepare_pending = 0;
  life_prepare_failed = false;
  life_prepare_observed_attempt = 0;
  life_pending_finalize = LifeTxnDescriptor();
  life_pending_objects.clear();
  life_pending_remote_nodes.clear();
  life_pending_prepare_nodes.clear();
  life_prepare_response_nodes.clear();
  life_finalize_requester_nodes->clear();
  life_finalize_requester_txn_ids->clear();
}

void YCSBTxnManager::send_life_message_to_node(Message *msg, uint64_t node_id) {
  if (node_id == g_node_id) {
    work_queue.enqueue(get_thd_id(), msg, false);
    return;
  }
  msg_queue.enqueue(get_thd_id(), msg, node_id);
}

void YCSBTxnManager::send_life_finalize_request(
    const LifeTxnDescriptor &descriptor) {
  LifeFinalizeMessage *msg =
      (LifeFinalizeMessage *)Message::create_message(RLIFE_FINALIZE);
  msg->txn_id = descriptor.tid.time;
  msg->return_node_id = g_node_id;
  msg->requester_txn_id = get_txn_id();
  msg->descriptor = descriptor;
  LIFE_DBG_INC(life_dbg_finalize_sent);
  send_life_message_to_node(msg, GET_NODE_ID(descriptor.tid.time));
}

void YCSBTxnManager::add_life_finalize_requester(uint64_t requester_node_id,
                                                 uint64_t requester_txn_id) {
  if (requester_node_id == UINT64_MAX)
    return;
  if (requester_txn_id == UINT64_MAX)
    return;

  for (size_t index = 0; index < life_finalize_requester_nodes->size();
       ++index) {
    if ((*life_finalize_requester_nodes)[index] == requester_node_id &&
        (*life_finalize_requester_txn_ids)[index] == requester_txn_id)
      return;
  }

  life_finalize_requester_nodes->push_back(requester_node_id);
  life_finalize_requester_txn_ids->push_back(requester_txn_id);
}

void YCSBTxnManager::send_life_finalize_response(
    const LifeExecuteResult &result, uint64_t requester_node_id,
    uint64_t requester_txn_id) {
  if (requester_node_id == UINT64_MAX)
    return;
  if (requester_txn_id == UINT64_MAX)
    return;

  LifeFinalizeResponseMessage *response =
      (LifeFinalizeResponseMessage *)Message::create_message(
          RLIFE_FINALIZE_RSP);
  response->txn_id = requester_txn_id;
  response->result = result;
  LIFE_DBG_INC(life_dbg_finalize_rsp_sent);
  send_life_message_to_node(response, requester_node_id);
}

void YCSBTxnManager::send_life_prepare_messages(
    const LifeTxnDescriptor &descriptor, const std::vector<uint64_t> &nodes) {
  for (std::vector<uint64_t>::const_iterator it = nodes.begin();
       it != nodes.end(); ++it) {
    LifePrepareMessage *msg =
        (LifePrepareMessage *)Message::create_message(RLIFE_PREPARE);
    msg->txn_id = get_txn_id();
    // Prepare installs this descriptor in each row's process record. A later
    // contender can receive it as Help/Finalize and must be able to resume or
    // reset the workload, so the YCSB/PPS program is part of the durable
    // descriptor rather than an expendable prepare-only payload.
    msg->descriptor = descriptor;
    LIFE_DBG_INC(life_dbg_prepare_sent);
    msg_queue.enqueue(get_thd_id(), msg, *it);
  }
}

void YCSBTxnManager::send_life_finish_messages(
    const LifeTxnDescriptor &descriptor, const std::vector<uint64_t> &nodes,
    RC decision) {
  for (std::vector<uint64_t>::const_iterator it = nodes.begin();
       it != nodes.end(); ++it) {
    LifeFinishMessage *msg =
        (LifeFinishMessage *)Message::create_message(RLIFE_FINISH);
    msg->txn_id = get_txn_id();
    msg->descriptor = make_life_finish_message_descriptor(descriptor, *it);
    msg->decision = decision;
    LIFE_DBG_INC(life_dbg_finish_sent);
    msg_queue.enqueue(get_thd_id(), msg, *it);
  }
}

RC YCSBTxnManager::apply_life_execute_response(const LifeExecuteResult &result,
                                               uint64_t wait_id,
                                               uint64_t history_base_size,
                                               bool prepared,
                                               uint64_t responder_node_id) {
  life_response_stack.clear();
  std::vector<LifeTxnDescriptor> &txns = life_response_stack;
  const bool has_saved_stack = take_life_wait_stack(wait_id, txns);
  if (!has_saved_stack) {
    LIFE_DBG_INC(life_dbg_execute_rsp_stale);
    return WAIT_REM;
  }

  switch (result.code) {

  case LifeResultCode::Success: {
    if (txns.empty() || txns.back().history.size() != history_base_size) {
      LIFE_DBG_INC(life_dbg_execute_rsp_stale);
      return Abort;
    }

    LifeTxnDescriptor &response = txns.back();
    for (std::vector<LifeHistoryEntry>::const_iterator it =
             result.transaction.history.begin();
         it != result.transaction.history.end(); ++it)
      life_append_history(response, *it);
    life_copy_remote_program(response, result.transaction);
    if (prepared) {
      if (!life_program_done(response)) {
        LIFE_DBG_INC(life_dbg_execute_rsp_stale);
        return Abort;
      }
      note_life_piggyback_prepare(response, responder_node_id);
    }
    return try_life_transactions(txns) ? continue_life_after_stack()
                                       : WAIT_REM;
  }

  case LifeResultCode::Committed: {
    if (has_saved_stack && !txns.empty()) {
      const LifeTxnDescriptor &committed =
          !life_program_present(result.transaction) ? txns.back()
                                                    : result.transaction;
      if (committed.pid == txn->life_pid &&
          committed.tid.time == txn->life_tid.time) {
        copy_life_descriptor_to_workload(committed);
        txn->life_status = LifeTxnStatus::Committed;
        txns.clear();
        return query == NULL || query->partitions_touched.size() == 0
                   ? RCOK
                   : commit();
      }
      txns.pop_back();
      if (!txns.empty())
        return try_life_transactions(txns) ? continue_life_after_stack()
                                           : WAIT_REM;
      return continue_life_after_stack();
    }

    return continue_life_after_stack();
  }

  case LifeResultCode::Help: {
    LifeTxnDescriptor helped = result.transaction;
    push_life_help_descriptor(txns, helped);
    return try_life_transactions(txns) ? continue_life_after_stack() : WAIT_REM;
  }

  case LifeResultCode::Finalize: {
    LifeTxnDescriptor finalize = result.transaction;
    finalize_life_descriptor(finalize);
    if (life_finalize_waiting) {
      const uint64_t finalize_wait_id = allocate_life_wait_id();
      save_life_wait_stack(txns, finalize_wait_id, 2,
                           GET_NODE_ID(life_pending_finalize.tid.time),
                           life_pending_finalize.tid.time);
      return WAIT_REM;
    }
    const uint64_t ctx_time =
        txns.empty() ? finalize.tid.time : txns.back().tid.time;
    if (finalize.tid.time >= ctx_time)
      return try_life_transactions(txns) ? continue_life_after_stack()
                                         : WAIT_REM;

    txns.push_back(finalize);
    return try_life_transactions(txns) ? continue_life_after_stack() : WAIT_REM;
  }

  case LifeResultCode::Retry: {
    assert(!txns.empty());
    LifeTxnDescriptor retry = !life_program_present(result.transaction) &&
                                      result.transaction.history.empty()
                                  ? txns.back()
                                  : result.transaction;
    if (retry.pid == txn->life_pid &&
        retry.tid.time == txn->life_tid.time)
      reset_life_piggyback_prepare();
    rollback_life_descriptor(retry);
    retry.tid.attempt =
        std::max(retry.tid.attempt, result.observed_attempt) + 1;
    retry.history.clear();
    retry.touched_objects.clear();
    life_reset_program(retry);
    txns.back() = retry;
    return try_life_transactions(txns) ? continue_life_after_stack() : WAIT_REM;
  } break;

  default:
    assert(false);
    return Abort;
  }

  return continue_life_after_stack();
}

RC YCSBTxnManager::apply_life_prepare_response(const LifeExecuteResult &result,
                                               const LifeProcessId &pid,
                                               const LifeTxnId &tid,
                                               uint64_t responder_node_id) {
  if (!life_finalize_waiting || life_prepare_pending == 0)
    return continue_life_after_stack();
  if (!pending_life_finalize_matches(pid, tid))
    return WAIT_REM;
  if (!note_life_prepare_response(responder_node_id)) {
    LIFE_DBG_INC(life_dbg_prepare_rsp_duplicate);
    return WAIT_REM;
  }

  if (result.code != LifeResultCode::Success &&
      result.code != LifeResultCode::Committed) {
    life_prepare_failed = true;
    if (result.code == LifeResultCode::Retry)
      life_prepare_observed_attempt = result.observed_attempt;
  }

  --life_prepare_pending;
  if (life_prepare_pending > 0)
    return WAIT_REM;

  if (life_prepare_failed) {
    rollback_life_descriptor(life_pending_finalize);
    send_life_finish_messages(life_pending_finalize, life_pending_remote_nodes,
                              Abort);
    return complete_life_finish();
  }

  LifeTxnDescriptorPtr frozen =
      std::make_shared<LifeTxnDescriptor>(life_pending_finalize);
  for (std::vector<LifeFinalizeObject>::const_iterator it =
           life_pending_objects.begin();
       it != life_pending_objects.end(); ++it) {
    if (GET_NODE_ID(it->object.partition_id) != g_node_id)
      continue;

    Row_life *manager = it->manager;
    if (manager == NULL)
      manager = lookup_life_row(it->object)->manager;
    assert(manager != NULL);
    manager->commit(frozen, it->history_indices);
  }

  send_life_finish_messages(life_pending_finalize, life_pending_remote_nodes,
                            Commit);
  return complete_life_finish();
}

RC YCSBTxnManager::complete_life_finish() {
  if (!life_finalize_waiting)
    return continue_life_after_stack();

  const bool committed = !life_prepare_failed;
  LifeTxnDescriptor finished = life_pending_finalize;
  if (!committed)
    reset_life_descriptor(finished, life_prepare_observed_attempt);
  for (size_t index = 0; index < life_finalize_requester_nodes->size();
       ++index) {
    LifeExecuteResult response = LifeExecuteResult();
    response.code = committed ? LifeResultCode::Success : LifeResultCode::Retry;
    response.transaction = finished;
    response.observed_attempt = finished.tid.attempt;
    send_life_finalize_response(response,
                                (*life_finalize_requester_nodes)[index],
                                (*life_finalize_requester_txn_ids)[index]);
  }

  const bool owns_pending =
      finished.pid == txn->life_pid && finished.tid.time == txn->life_tid.time;
  if (!owns_pending) {
    life_response_stack.clear();
    std::vector<LifeTxnDescriptor> &txns = life_response_stack;
    const bool has_saved_stack =
        take_life_wait_stack_by_descriptor(2, finished, txns);
    reset_pending_life_finalize();

    if (has_saved_stack && !txns.empty()) {
      if (committed) {
        txns.pop_back();
      } else {
        txns.back() = finished;
      }
      if (!txns.empty())
        return try_life_transactions(txns) ? continue_life_after_stack()
                                           : WAIT_REM;
    }
    return continue_life_after_stack();
  }

  life_response_stack.clear();
  take_life_wait_stack_by_descriptor(2, finished, life_response_stack);

  if (committed) {
    copy_life_descriptor_to_workload(finished);
    txn->life_status = LifeTxnStatus::Committed;
    if (query == NULL || query->partitions_touched.size() == 0) {
      reset_pending_life_finalize();
      return RCOK;
    }
    reset_pending_life_finalize();
    return commit();
  }

  reset_pending_life_finalize();
  copy_life_descriptor_to_workload(finished);
  return run_life_txn();
}

void YCSBTxnManager::collect_life_objects(
    const LifeTxnDescriptor &descriptor,
    std::vector<LifeFinalizeObject> &objects) {
  if (objects.capacity() < descriptor.history.size())
    objects.reserve(descriptor.history.size());

  for (size_t index = 0; index < descriptor.history.size(); ++index) {
    const LifeObjectId &object_id = descriptor.history[index].operation.object;

    std::vector<LifeFinalizeObject>::iterator object = objects.begin();
    for (; object != objects.end(); ++object) {
      if (object->object == object_id)
        break;
    }
    if (object == objects.end()) {
      LifeFinalizeObject added;
      added.object = object_id;
      added.manager = descriptor.history[index].operation.manager;
      objects.push_back(added);
      object = objects.end() - 1;
    } else if (object->manager == NULL) {
      object->manager = descriptor.history[index].operation.manager;
    }
    object->history_indices.push_back(index);
  }
}

void YCSBTxnManager::rollback_life_descriptor(
    const LifeTxnDescriptor &descriptor) {
  life_object_scratch.clear();
  const std::vector<LifeFinalizeObject> *objects = &descriptor.touched_objects;
  size_t cached_indices = 0;
  for (std::vector<LifeFinalizeObject>::const_iterator it = objects->begin();
       it != objects->end(); ++it) {
    cached_indices += it->history_indices.size();
  }
  if (cached_indices != descriptor.history.size()) {
    collect_life_objects(descriptor, life_object_scratch);
    objects = &life_object_scratch;
  }

  for (std::vector<LifeFinalizeObject>::const_iterator it = objects->begin();
       it != objects->end(); ++it) {
    if (GET_NODE_ID(it->object.partition_id) != g_node_id)
      continue;

    Row_life *manager = it->manager;
    if (manager == NULL)
      manager = lookup_life_row(it->object)->manager;
    assert(manager != NULL);
    manager->rollback(descriptor);
  }
}

bool YCSBTxnManager::finalize_life_descriptor(LifeTxnDescriptor &descriptor) {
  reset_pending_life_finalize();

  size_t cached_indices = 0;
  for (std::vector<LifeFinalizeObject>::const_iterator it =
           descriptor.touched_objects.begin();
       it != descriptor.touched_objects.end(); ++it) {
    cached_indices += it->history_indices.size();
  }
  if (cached_indices != descriptor.history.size()) {
    descriptor.touched_objects.clear();
    descriptor.touched_objects.reserve(descriptor.history.size());
    collect_life_objects(descriptor, descriptor.touched_objects);
  }

  LifeTxnDescriptorPtr frozen = std::make_shared<LifeTxnDescriptor>(descriptor);
  const std::vector<LifeFinalizeObject> &objects = frozen->touched_objects;

  std::vector<uint64_t> &remote_nodes = life_pending_remote_nodes;
  std::vector<uint64_t> &prepare_nodes = life_pending_prepare_nodes;
  remote_nodes.clear();
  prepare_nodes.clear();
  remote_nodes.reserve(g_node_cnt);
  prepare_nodes.reserve(g_node_cnt);
  for (std::vector<LifeFinalizeObject>::const_iterator it = objects.begin();
       it != objects.end(); ++it) {
    const uint64_t node_id = GET_NODE_ID(it->object.partition_id);
    if (node_id != g_node_id) {
      add_unique_life_node(remote_nodes, node_id);
      if (!has_life_piggyback_prepare(descriptor, node_id))
        add_unique_life_node(prepare_nodes, node_id);
    }
  }

  uint64_t observed_attempt = descriptor.tid.attempt;
  for (std::vector<LifeFinalizeObject>::const_iterator it = objects.begin();
       it != objects.end(); ++it) {
    const uint64_t node_id = GET_NODE_ID(it->object.partition_id);
    if (node_id != g_node_id)
      continue;

    Row_life *manager = it->manager;
    if (manager == NULL)
      manager = lookup_life_row(it->object)->manager;
    assert(manager != NULL);
    const LifeExecuteResult result = manager->prepare(frozen);
    if (result.code == LifeResultCode::Success ||
        result.code == LifeResultCode::Committed) {
      continue;
    }

    if (result.code == LifeResultCode::Retry)
      observed_attempt = result.observed_attempt;

    if (!remote_nodes.empty()) {
      LifeTxnDescriptor abort_descriptor = descriptor;
      abort_descriptor.touched_objects = objects;
      rollback_life_descriptor(abort_descriptor);
      send_life_finish_messages(abort_descriptor, life_pending_remote_nodes,
                                Abort);
    }

    reset_life_descriptor(descriptor, observed_attempt);
    return false;
  }

  if (!prepare_nodes.empty()) {
    life_finalize_waiting = true;
    life_prepare_pending = prepare_nodes.size();
    life_prepare_failed = false;
    life_prepare_observed_attempt = descriptor.tid.attempt;
    life_pending_finalize = descriptor;
    life_pending_finalize.touched_objects = objects;
    life_pending_objects = objects;
    send_life_prepare_messages(life_pending_finalize,
                               life_pending_prepare_nodes);
    return false;
  }

  for (std::vector<LifeFinalizeObject>::const_iterator it = objects.begin();
       it != objects.end(); ++it) {
    if (GET_NODE_ID(it->object.partition_id) != g_node_id)
      continue;

    Row_life *manager = it->manager;
    if (manager == NULL)
      manager = lookup_life_row(it->object)->manager;
    assert(manager != NULL);
    manager->commit(frozen, it->history_indices);
  }

  // Piggyback preparation suppresses only the prepare round trip. Finish is
  // still required to publish commit and release each durable remote manager.
  if (!remote_nodes.empty())
    send_life_finish_messages(descriptor, remote_nodes, Commit);
  return true;
}

void YCSBTxnManager::reset_life_descriptor(LifeTxnDescriptor &descriptor,
                                           uint64_t observed_attempt) {
  if (descriptor.pid == txn->life_pid &&
      descriptor.tid.time == txn->life_tid.time)
    reset_life_piggyback_prepare();
  rollback_life_descriptor(descriptor);
  descriptor.tid.attempt =
      std::max(descriptor.tid.attempt, observed_attempt) + 1;
  descriptor.history.clear();
  descriptor.touched_objects.clear();
  life_reset_program(descriptor);
}

void YCSBTxnManager::append_life_success(LifeTxnDescriptor &descriptor,
                                         const LifeOperation &operation,
                                         const LifeResponse &response) {
  LifeHistoryEntry entry;
  entry.operation = operation;
  entry.response = response;
  life_append_history(descriptor, entry);
  descriptor.ycsb.next_record_id++;
  descriptor.ycsb.state =
      descriptor.ycsb.next_record_id == descriptor.ycsb.requests.size()
          ? YCSB_FIN
          : YCSB_0;
}

void YCSBTxnManager::life_reconcile_descriptor(
    LifeTxnDescriptor &descriptor) {
  // Row records append a successful operation before a helper advances the
  // workload cursor. Reconcile both pieces of program state from that durable
  // history. Keeping only the cursor in sync can leave a completed descriptor
  // in YCSB_0, violating Algorithm 2's state=End completion invariant.
  reconcile_life_ycsb_program(descriptor);
}

bool YCSBTxnManager::life_program_done(
    const LifeTxnDescriptor &descriptor) const {
  return descriptor.ycsb.next_record_id >= descriptor.ycsb.requests.size();
}

LifeOperation YCSBTxnManager::life_current_operation(
    LifeTxnDescriptor &descriptor) const {
  assert(!life_program_done(descriptor));
  return const_cast<YCSBTxnManager *>(this)->make_life_operation(
      descriptor.ycsb.requests[descriptor.ycsb.next_record_id]);
}

void YCSBTxnManager::life_advance_program(LifeTxnDescriptor &descriptor,
                                          const LifeOperation &operation,
                                          const LifeResponse &response) {
  append_life_success(descriptor, operation, response);
}

uint64_t YCSBTxnManager::life_program_position(
    const LifeTxnDescriptor &descriptor) const {
  return descriptor.ycsb.next_record_id;
}

bool YCSBTxnManager::life_program_present(
    const LifeTxnDescriptor &descriptor) const {
  return !descriptor.ycsb.requests.empty();
}

void YCSBTxnManager::life_reset_program(LifeTxnDescriptor &descriptor) {
  descriptor.ycsb.next_record_id = 0;
  descriptor.ycsb.state = YCSB_0;
}

void YCSBTxnManager::life_copy_program_to_workload(
    const LifeTxnDescriptor &descriptor) {
  state = static_cast<YCSBRemTxnType>(descriptor.ycsb.state);
  next_record_id = descriptor.ycsb.next_record_id;
}

void YCSBTxnManager::life_copy_remote_program(
    LifeTxnDescriptor &destination, const LifeTxnDescriptor &source) {
  assert(destination.workload == LifeWorkloadKind::Ycsb);
  assert(source.workload == LifeWorkloadKind::Ycsb);
  assert(source.ycsb.next_record_id <= destination.ycsb.requests.size());
  destination.workload = source.workload;
  destination.ycsb.state = source.ycsb.state;
  destination.ycsb.next_record_id = source.ycsb.next_record_id;
}

RC YCSBTxnManager::continue_life_after_stack() {
  if (query == NULL)
    return RCOK;

  // A successfully finalized owned descriptor has already committed its LIFE
  // rows and sent the one terminal finish per remote node. Only transaction
  // manager bookkeeping remains; running the workload again would rebuild the
  // completed descriptor and emit duplicate finish messages.
  if (txn->life_status == LifeTxnStatus::Committed) {
#if WORKLOAD == YCSB
    assert(state == YCSB_FIN);
    assert(next_record_id == ((YCSBQuery *)query)->requests.size());
#endif
    return complete_life_home_transaction();
  }

  return run_life_txn();
}

void YCSBTxnManager::push_life_help_descriptor(
    std::vector<LifeTxnDescriptor> &txns, const LifeTxnDescriptor &descriptor) {
  for (std::vector<LifeTxnDescriptor>::iterator it = txns.begin();
       it != txns.end();) {
    if (it->tid.time == descriptor.tid.time)
      it = txns.erase(it);
    else
      ++it;
  }
  txns.push_back(descriptor);
}

row_t *YCSBTxnManager::lookup_life_row(const LifeObjectId &object) const {
  itemid_t *item = const_cast<YCSBTxnManager *>(this)->index_read(
      _wl->the_index, object.primary_key, object.partition_id);
  assert(item != NULL);
  return (row_t *)item->location;
}

row_t *YCSBTxnManager::lookup_life_row(uint64_t key) const {
  const int part_id = _wl->key_to_part(key);
  assert(GET_NODE_ID(part_id) == g_node_id);
  itemid_t *item = const_cast<YCSBTxnManager *>(this)->index_read(
      _wl->the_index, key, part_id);
  assert(item != NULL);
  return (row_t *)item->location;
}

uint64_t
YCSBTxnManager::life_remote_batch_stop(const LifeTxnDescriptor &descriptor,
                                       uint64_t dest_node_id) const {
  uint64_t index = descriptor.ycsb.next_record_id;
  while (index < descriptor.ycsb.requests.size()) {
    const LifeYcsbRequest &request = descriptor.ycsb.requests[index];
    const uint64_t node_id = GET_NODE_ID(_wl->key_to_part(request.key));
    if (node_id != dest_node_id)
      break;
    ++index;
  }
  return index;
}

LifeOperation
YCSBTxnManager::make_life_operation(const LifeYcsbRequest &request) {
  LifeOperation operation = LifeOperation();
  operation.object.table_id = _wl->the_table->get_table_id();
  operation.object.partition_id = _wl->key_to_part(request.key);
  operation.object.primary_key = request.key;
  operation.object.row_id = request.key;
  operation.field_id = 0;
  operation.value_size = sizeof(uint64_t);

  if (request.kind == LifeYcsbRequestKind::Read ||
      request.kind == LifeYcsbRequestKind::Scan) {
    operation.kind = LifeOperationKind::ReadField;
    return operation;
  }

  assert(request.kind == LifeYcsbRequestKind::Write);
  operation.kind = LifeOperationKind::WriteField;
  const uint64_t value = request.value;
  const uint8_t *bytes = reinterpret_cast<const uint8_t *>(&value);
  operation.argument.assign(bytes, bytes + sizeof(value));
  return operation;
}

LifeOperation YCSBTxnManager::make_life_operation(row_t *life_row,
                                                  ycsb_request *req) {
  return make_life_operation(life_row, make_life_ycsb_request(*req));
}

LifeOperation
YCSBTxnManager::make_life_operation(row_t *life_row,
                                    const LifeYcsbRequest &request) {
  LifeOperation operation = LifeOperation();
  operation.manager = life_row->manager;
  operation.object.table_id = life_row->get_table()->get_table_id();
  operation.object.partition_id = life_row->get_part_id();
  operation.object.primary_key = life_row->get_primary_key();
  // Unique-index YCSB objects use the primary key as their stable identity.
  // The allocator row id is local construction state and is not available to
  // a remote descriptor.
  operation.object.row_id = life_row->get_primary_key();
  operation.field_id = 0;
  operation.value_size = sizeof(uint64_t);

  if (request.kind == LifeYcsbRequestKind::Read ||
      request.kind == LifeYcsbRequestKind::Scan) {
    operation.kind = LifeOperationKind::ReadField;
    return operation;
  }

  assert(request.kind == LifeYcsbRequestKind::Write);
  operation.kind = LifeOperationKind::WriteField;
  const uint64_t value = request.value;
  const uint8_t *bytes = reinterpret_cast<const uint8_t *>(&value);
  operation.argument.assign(bytes, bytes + sizeof(value));
  return operation;
}

#endif

RC YCSBTxnManager::run_txn_post_wait() {
  get_row_post_wait(row);
  next_ycsb_state();
  return RCOK;
}

bool YCSBTxnManager::is_done() {
  return next_record_id == ((YCSBQuery *)query)->requests.size();
}

void YCSBTxnManager::next_ycsb_state() {
  switch (state) {
  case YCSB_0:
    state = YCSB_1;
    break;
  case YCSB_1:
    next_record_id++;
    if (!IS_LOCAL(txn->txn_id) || !is_done()) {
      state = YCSB_0;
    } else {
      state = YCSB_FIN;
    }
    break;
  case YCSB_FIN:
    break;
  default:
    assert(false);
  }
}

bool YCSBTxnManager::is_local_request(uint64_t idx) {
  return GET_NODE_ID(_wl->key_to_part(
             ((YCSBQuery *)query)->requests[idx]->key)) == g_node_id;
}

RC YCSBTxnManager::send_remote_request() {
  YCSBQuery *ycsb_query = (YCSBQuery *)query;
  uint64_t dest_node_id =
      GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
  ycsb_query->partitions_touched.add_unique(GET_PART_ID(0, dest_node_id));
  msg_queue.enqueue(get_thd_id(), Message::create_message(this, RQRY),
                    dest_node_id);
  return WAIT_REM;
}

void YCSBTxnManager::copy_remote_requests(YCSBQueryMessage *msg) {
  YCSBQuery *ycsb_query = (YCSBQuery *)query;
  // msg->requests.init(ycsb_query->requests.size());
  uint64_t dest_node_id =
      GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
  while (next_record_id < ycsb_query->requests.size() &&
         !is_local_request(next_record_id) &&
         GET_NODE_ID(ycsb_query->requests[next_record_id]->key) ==
             dest_node_id) {
    YCSBQuery::copy_request_to_msg(ycsb_query, msg, next_record_id++);
  }
}

RC YCSBTxnManager::run_txn_state() {
  YCSBQuery *ycsb_query = (YCSBQuery *)query;
  ycsb_request *req = ycsb_query->requests[next_record_id];
  uint64_t part_id = _wl->key_to_part(req->key);
  bool loc = GET_NODE_ID(part_id) == g_node_id;

  RC rc = RCOK;

  switch (state) {
  case YCSB_0:
    if (loc) {
      rc = run_ycsb_0(req, row, req->acctype);
    } else {
      rc = send_remote_request();
    }

    break;
  case YCSB_1:
    rc = run_ycsb_1(req->acctype, static_cast<uint8_t>(req->value), row);
    break;
  case YCSB_FIN:
    state = YCSB_FIN;
    break;
  default:
    assert(false);
  }

  if (rc == RCOK)
    next_ycsb_state();

  return rc;
}

RC YCSBTxnManager::run_ycsb_0(ycsb_request *req, row_t *&row_local,
                              access_t type) {
  RC rc = RCOK;
  int part_id = _wl->key_to_part(req->key);
  itemid_t *m_item;

  m_item = index_read(_wl->the_index, req->key, part_id);

  row_t *row = ((row_t *)m_item->location);

  rc = get_row(row, type, row_local);

  return rc;
}

RC YCSBTxnManager::run_ycsb_1(access_t acctype, uint8_t value,
                              row_t *row_local) {
  if (acctype == RD || acctype == SCAN) {
    int fid = 0;
    char *data = row_local->get_data();
    uint64_t fval __attribute__((unused));
    fval = *(uint64_t *)(&data[fid * 100]);
#if ISOLATION_LEVEL == READ_COMMITTED || ISOLATION_LEVEL == READ_UNCOMMITTED
    // Release lock after read
    release_last_row_lock();
#endif

  } else {
    assert(acctype == WR);
    int fid = 0;
    char *data = row_local->get_data();
    // YCSB generates one byte of update data per request. Store it in the
    // benchmark's existing eight-byte field representation so every CC path
    // applies the requested value and Calvin can observe intervening writes.
    *(uint64_t *)(&data[fid * 100]) = value;
#if YCSB_ABORT_MODE
    if (data[0] == 'a')
      return RCOK;
#endif

#if ISOLATION_LEVEL == READ_UNCOMMITTED
    // Release lock after write
    release_last_row_lock();
#endif
  }
  return RCOK;
}
RC YCSBTxnManager::run_calvin_txn() {
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  YCSBQuery *ycsb_query = (YCSBQuery *)query;
  DEBUG("(%ld,%ld) Run calvin txn\n", txn->txn_id, txn->batch_id);
  while (!calvin_exec_phase_done() && rc == RCOK) {
    DEBUG("(%ld,%ld) phase %d\n", txn->txn_id, txn->batch_id, this->phase);
    switch (this->phase) {
    case CALVIN_RW_ANALYSIS:

      // Phase 1: Read/write set analysis
      calvin_expected_rsp_cnt = ycsb_query->get_participants(_wl);
      if (query->participant_nodes[g_node_id] == 1) {
        calvin_expected_rsp_cnt--;
      }
      DEBUG("(%ld,%ld) expects %d responses;\n", txn->txn_id, txn->batch_id,
            calvin_expected_rsp_cnt);

      this->phase = CALVIN_LOC_RD;
      break;
    case CALVIN_LOC_RD:
      // Phase 2: Perform local reads
      DEBUG("(%ld,%ld) local reads\n", txn->txn_id, txn->batch_id);
      rc = run_ycsb();
      // release_read_locks(query);
      this->phase = isRecon() ? CALVIN_DONE : CALVIN_VALIDATE;
      break;
    case CALVIN_VALIDATE:
      // Phase 3: Recheck the reconnaissance reads while Calvin locks are held.
      DEBUG("(%ld,%ld) validate reconnaissance\n", txn->txn_id,
            txn->batch_id);
      validate_ycsb_recon();
      this->phase = CALVIN_SERVE_RD;
      break;
    case CALVIN_SERVE_RD:
      // Phase 4: Forward local validation results to every writer.
      // If there is any abort logic, relevant reads need to be sent to all
      // active nodes...
      if (query->participant_nodes[g_node_id] == 1) {
        rc = send_remote_reads();
      }
      if (query->active_nodes[g_node_id] == 1) {
        this->phase = CALVIN_COLLECT_RD;
        if (calvin_collect_phase_done()) {
          rc = RCOK;
        } else {
          DEBUG("(%ld,%ld) wait in collect phase; %d / %d rfwds received\n",
                txn->txn_id, txn->batch_id, rsp_cnt, calvin_expected_rsp_cnt);
          rc = WAIT;
        }
      } else { // Done
        rc = RCOK;
        this->phase = CALVIN_DONE;
      }

      break;
    case CALVIN_COLLECT_RD:
      // Phase 5: Collect remote validation results.
      this->phase = CALVIN_EXEC_WR;
      break;
    case CALVIN_EXEC_WR:
      // Phase 6: Execute transaction / perform local writes.
      DEBUG("(%ld,%ld) execute writes\n", txn->txn_id, txn->batch_id);
      if (txn->rc == RCOK)
        rc = run_ycsb();
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

RC YCSBTxnManager::run_ycsb() {
  RC rc = RCOK;
  assert(CC_ALG == CALVIN);
  YCSBQuery *ycsb_query = (YCSBQuery *)query;

  for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
    ycsb_request *req = ycsb_query->requests[i];
    if (this->phase == CALVIN_LOC_RD && req->acctype == WR && !isRecon())
      continue;
    if (this->phase == CALVIN_EXEC_WR &&
        (req->acctype == RD || req->acctype == SCAN))
      continue;

    uint64_t part_id = _wl->key_to_part(req->key);
    bool loc = GET_NODE_ID(part_id) == g_node_id;

    if (!loc)
      continue;

    // Reconnaissance observes every touched row, including write-only rows,
    // but must never acquire a write access or mutate the tuple.
    const access_t access_type = isRecon() ? RD : req->acctype;
    rc = run_ycsb_0(req, row, access_type);
    assert(rc == RCOK);

    if (isRecon()) {
      capture_ycsb_recon(req->key, row);
    }

    rc = run_ycsb_1(access_type, static_cast<uint8_t>(req->value), row);
    assert(rc == RCOK);
  }
  return rc;
}

void YCSBTxnManager::capture_ycsb_recon(uint64_t key, row_t *row_local) {
  YCSBQuery *ycsb_query = (YCSBQuery *)query;
  YCSBReconRecord record;
  record.key = key;
  const uint64_t tuple_size = row_local->get_tuple_size();
  record.tuple.assign(row_local->get_data(),
                      row_local->get_data() + tuple_size);
  ycsb_query->recon_records.push_back(record);
}

void YCSBTxnManager::validate_ycsb_recon() {
  YCSBQuery *ycsb_query = (YCSBQuery *)query;

  for (uint64_t i = 0; i < ycsb_query->requests.size(); ++i) {
    ycsb_request *req = ycsb_query->requests[i];

    const uint64_t part_id = _wl->key_to_part(req->key);
    if (GET_NODE_ID(part_id) != g_node_id)
      continue;

    const YCSBReconRecord *record =
        ycsb_query->find_recon_record(req->key);
    itemid_t *item = index_read(_wl->the_index, req->key, part_id);
    row_t *current = (row_t *)item->location;
    const uint64_t tuple_size = current->get_tuple_size();
    if (record == NULL || record->tuple.size() != tuple_size ||
        memcmp(record->tuple.data(), current->get_data(), tuple_size) != 0) {
      DEBUG("(%ld,%ld) YCSB OLLP validation failed for key %ld\n",
            txn->txn_id, txn->batch_id, req->key);
      txn->rc = Abort;
      return;
    }
  }
}

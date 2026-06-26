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
#include <set>

#if CC_ALG == LIFE
namespace {

void wait_before_life_help() {
  if (LIFE_HELP_WAIT_US > 0)
    usleep(LIFE_HELP_WAIT_US);
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
extern volatile uint64_t life_dbg_prepare_sent;
extern volatile uint64_t life_dbg_finish_sent;
extern volatile uint64_t life_dbg_finalize_sent;
extern volatile uint64_t life_dbg_finalize_rsp_sent;
#endif

YCSBTxnManager::YCSBTxnManager()
    : life_finalize_requester_nodes(NULL),
      life_finalize_requester_txn_ids(NULL), life_wait_stack_active(false),
      life_wait_stack(NULL), life_wait_stack_reason(0),
      life_wait_stack_remote_node(UINT64_MAX),
      life_wait_stack_remote_key(UINT64_MAX) {
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
  delete life_wait_stack;
}

void YCSBTxnManager::init(uint64_t thd_id, Workload *h_wl) {
  if (life_finalize_requester_nodes == NULL)
    life_finalize_requester_nodes = new std::vector<uint64_t>();
  if (life_finalize_requester_txn_ids == NULL)
    life_finalize_requester_txn_ids = new std::vector<uint64_t>();
  if (life_wait_stack == NULL)
    life_wait_stack = new std::vector<LifeTxnDescriptor>();
  TxnManager::init(thd_id, h_wl);
  _wl = (YCSBWorkload *)h_wl;
  reset();
}

void YCSBTxnManager::reset() {
  state = YCSB_0;
  next_record_id = 0;
#if CC_ALG == LIFE
  reset_pending_life_finalize();
  life_wait_stack_active = false;
  life_wait_stack->clear();
  life_wait_stack_reason = 0;
  life_wait_stack_remote_node = UINT64_MAX;
  life_wait_stack_remote_key = UINT64_MAX;
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

  std::vector<LifeTxnDescriptor> txns;
  txns.reserve(g_req_per_query);
  txns.push_back(life_descriptor());

  const bool complete = try_life_transactions(txns);

  uint64_t curr_time = get_sys_clock();
  txn_stats.process_time += curr_time - starttime;
  txn_stats.process_time_short += curr_time - starttime;
  txn_stats.wait_starttime = get_sys_clock();

  if (!complete)
    return WAIT_REM;

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

    // Row records append successful operations to history before the workload
    // cursor is advanced. A helper must reconcile that cursor before deciding
    // whether another request exists.
    if (ctx.ycsb.next_record_id < ctx.history.size())
      ctx.ycsb.next_record_id = ctx.history.size();

    if (ctx.ycsb.next_record_id >= ctx.ycsb.requests.size()) {
#if LOG_LIFE
      const uint64_t finalize_start = get_sys_clock();
#endif
      const bool finalized = finalize_life_descriptor(ctx);
#if LOG_LIFE
      life_finalize_time += get_sys_clock() - finalize_start;
#endif
      if (life_finalize_waiting)
      {
        save_life_wait_stack(txns, 2, GET_NODE_ID(life_pending_finalize.tid.time),
                             life_pending_finalize.tid.time);
        return false;
      }

      if (finalized) {
        if (ctx.pid == txn->life_pid && ctx.tid.time == txn->life_tid.time) {
          txn->life_tid = ctx.tid;
          txn->life_history = ctx.history;
          txn->life_objects.clear();
          state = YCSB_FIN;
          next_record_id = ctx.ycsb.next_record_id;
        }
        txns.pop_back();
      }
      continue;
    }

    const LifeYcsbRequest &request = ctx.ycsb.requests[ctx.ycsb.next_record_id];
    const int part_id = _wl->key_to_part(request.key);
    if (GET_NODE_ID(part_id) != g_node_id) {
      LifeOperation operation = LifeOperation();
      operation.object.table_id = _wl->the_table->get_table_id();
      operation.object.partition_id = part_id;
      operation.object.primary_key = request.key;
      operation.field_id = 0;
      operation.value_size = sizeof(uint64_t);
      if (request.kind == LifeYcsbRequestKind::Read ||
          request.kind == LifeYcsbRequestKind::Scan) {
        operation.kind = LifeOperationKind::ReadField;
      } else {
        assert(request.kind == LifeYcsbRequestKind::Write);
        operation.kind = LifeOperationKind::WriteField;
        const uint64_t value = 0;
        const uint8_t *bytes = reinterpret_cast<const uint8_t *>(&value);
        operation.argument.assign(bytes, bytes + sizeof(value));
      }
      save_life_wait_stack(txns, 1, GET_NODE_ID(part_id), request.key);
      send_life_execute(ctx, operation);
      return false;
    }

    LifeOperation operation;
#if LOG_LIFE
    const uint64_t operation_start = get_sys_clock();
#endif
    LifeExecuteResult result = execute_life_operation(ctx, operation);
    if (result.code == LifeResultCode::Help ||
        result.code == LifeResultCode::Finalize) {
      wait_before_life_help();
      result = execute_life_operation(ctx, operation);
    }
#if LOG_LIFE
    const uint64_t operation_time = get_sys_clock() - operation_start;
    if (ctx.pid == txn->life_pid && ctx.tid.time == txn->life_tid.time)
      life_own_time += operation_time;
    else
      life_help_time += operation_time;
#endif

    switch (result.code) {

    case LifeResultCode::Success:
      append_life_success(ctx, operation, result.response);
      break;

    case LifeResultCode::Finalize: {
      LifeTxnDescriptor response = std::move(result.transaction);
      const uint64_t response_time = response.tid.time;
      const uint64_t ctx_time = ctx.tid.time;
#if LOG_LIFE
      const uint64_t finalize_start = get_sys_clock();
#endif
      const bool finalized = finalize_life_descriptor(response);
#if LOG_LIFE
      life_finalize_time += get_sys_clock() - finalize_start;
#endif
      if (life_finalize_waiting)
      {
        save_life_wait_stack(txns, 2, GET_NODE_ID(life_pending_finalize.tid.time),
                             life_pending_finalize.tid.time);
        return false;
      }
      if (finalized && response_time < ctx_time)
        txns.push_back(response);
      break;
    }

    case LifeResultCode::Help: {
      LifeTxnDescriptor response = std::move(result.transaction);
      for (std::vector<LifeTxnDescriptor>::iterator it = txns.begin();
           it != txns.end();) {
        if (it->tid.time == response.tid.time)
          it = txns.erase(it);
        else
          ++it;
      }
      txns.push_back(response);
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
                                     const LifeOperation &operation) {
  uint64_t dest_node_id = GET_NODE_ID(operation.object.partition_id);
  assert(dest_node_id != g_node_id);
  query->partitions_touched.add_unique(operation.object.partition_id);

  LifeExecuteMessage *msg =
      (LifeExecuteMessage *)Message::create_message(RLIFE_EXECUTE);
  msg->txn_id = get_txn_id();
  msg->descriptor = descriptor;
  msg->operation = operation;
  __sync_fetch_and_add(&life_dbg_execute_sent, 1);
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

LifeExecuteResult
YCSBTxnManager::prepare_life_remote(const LifeTxnDescriptor &descriptor) {
  LifeTxnDescriptorPtr frozen = std::make_shared<LifeTxnDescriptor>(descriptor);
  LifeExecuteResult response;
  response.code = LifeResultCode::Success;
  response.transaction = descriptor;
  response.observed_attempt = descriptor.tid.attempt;

  for (std::vector<LifeFinalizeObject>::const_iterator it =
           descriptor.touched_objects.begin();
       it != descriptor.touched_objects.end(); ++it) {
    if (GET_NODE_ID(it->object.partition_id) != g_node_id)
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
      response.observed_attempt = result.observed_attempt;
    response.code = result.code;
    return response;
  }

  return response;
}

LifeExecuteResult
YCSBTxnManager::finish_life_remote(const LifeTxnDescriptor &descriptor,
                                   RC decision) {
  LifeTxnDescriptorPtr frozen = std::make_shared<LifeTxnDescriptor>(descriptor);
  for (std::vector<LifeFinalizeObject>::const_iterator it =
           descriptor.touched_objects.begin();
       it != descriptor.touched_objects.end(); ++it) {
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
  std::vector<LifeFinalizeObject> objects = descriptor.touched_objects;
  if (objects.empty())
    collect_life_objects(descriptor, objects);

  for (std::vector<LifeFinalizeObject>::const_iterator it = objects.begin();
       it != objects.end(); ++it) {
    if (GET_NODE_ID(it->object.partition_id) != g_node_id)
      continue;

    Row_life *manager = it->manager;
    if (manager == NULL)
      manager = lookup_life_row(it->object)->manager;
    assert(manager != NULL);
    manager->help(descriptor);
  }

  return RCOK;
}

RC YCSBTxnManager::apply_life_help_request(const LifeTxnDescriptor &descriptor,
                                           uint64_t requester_node_id) {
  std::vector<LifeFinalizeObject> objects = descriptor.touched_objects;
  if (objects.empty())
    collect_life_objects(descriptor, objects);

  std::set<uint64_t> remote_nodes;
  for (std::vector<LifeFinalizeObject>::const_iterator it = objects.begin();
       it != objects.end(); ++it) {
    const uint64_t node_id = GET_NODE_ID(it->object.partition_id);
    if (node_id == g_node_id)
      continue;
    if (node_id == requester_node_id)
      continue;
    remote_nodes.insert(node_id);
  }

  if (requester_node_id != g_node_id)
    help_life_remote(descriptor);
  if (!remote_nodes.empty()) {
    std::vector<uint64_t> nodes(remote_nodes.begin(), remote_nodes.end());
    send_life_help_apply_messages(descriptor, nodes, requester_node_id);
  }

  return RCOK;
}

RC YCSBTxnManager::apply_life_finalize_request(
    const LifeTxnDescriptor &descriptor, uint64_t requester_node_id,
    uint64_t requester_txn_id) {
  if (life_finalize_waiting) {
    add_life_finalize_requester(requester_node_id, requester_txn_id);
    return WAIT_REM;
  }

  LifeTxnDescriptor finalize = descriptor;
  const bool finalized = finalize_life_descriptor(finalize);
  if (life_finalize_waiting) {
    add_life_finalize_requester(requester_node_id, requester_txn_id);
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
  std::vector<LifeTxnDescriptor> txns;
  const bool has_saved_stack = take_life_wait_stack(txns);
  reset_pending_life_finalize();

  if (result.code == LifeResultCode::Retry) {
    const bool owns_response = result.transaction.pid == txn->life_pid &&
                               result.transaction.tid.time == txn->life_tid.time;
    if (has_saved_stack && !txns.empty()) {
      LifeTxnDescriptor retry = result.transaction;
      rollback_life_descriptor(retry);
      retry.tid.attempt =
          std::max(retry.tid.attempt, result.observed_attempt) + 1;
      retry.history.clear();
      retry.touched_objects.clear();
      retry.ycsb.next_record_id = 0;
      retry.ycsb.state = YCSB_0;
      txns.back() = retry;
      return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;
    }
    if (owns_response)
      copy_life_descriptor_to_workload(result.transaction);
    return run_life_txn();
  }

  if (result.code == LifeResultCode::Success ||
      result.code == LifeResultCode::Committed) {
    if (has_saved_stack && !txns.empty()) {
      const LifeTxnDescriptor &finished = result.transaction;
      if (txns.back().pid == finished.pid &&
          txns.back().tid.time == finished.tid.time)
        txns.pop_back();
      if (txns.empty())
        return run_life_txn();
      return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;
    }
    return run_life_txn();
  }

  if (result.code == LifeResultCode::Help ||
      result.code == LifeResultCode::Finalize) {
    LifeTxnDescriptor descriptor = life_descriptor();
    descriptor.ycsb =
        make_life_ycsb_snapshot(*(YCSBQuery *)query, state, next_record_id);

    if (!has_saved_stack)
      txns.push_back(descriptor);
    txns.push_back(result.transaction);
    return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;
  }

  return Abort;
}

void YCSBTxnManager::copy_life_descriptor_to_workload(
    const LifeTxnDescriptor &descriptor) {
  txn->life_tid = descriptor.tid;
  txn->life_history = descriptor.history;
  txn->life_objects.clear();
  state = static_cast<YCSBRemTxnType>(descriptor.ycsb.state);
  next_record_id = descriptor.ycsb.next_record_id;
}

void YCSBTxnManager::save_life_wait_stack(
    const std::vector<LifeTxnDescriptor> &txns, uint32_t reason,
    uint64_t remote_node_id, uint64_t remote_key) {
  *life_wait_stack = txns;
  life_wait_stack_active = true;
  life_wait_stack_reason = reason;
  life_wait_stack_remote_node = remote_node_id;
  life_wait_stack_remote_key = remote_key;
}

bool YCSBTxnManager::take_life_wait_stack(
    std::vector<LifeTxnDescriptor> &txns) {
  if (!life_wait_stack_active)
    return false;

  txns = *life_wait_stack;
  life_wait_stack->clear();
  life_wait_stack_active = false;
  life_wait_stack_reason = 0;
  life_wait_stack_remote_node = UINT64_MAX;
  life_wait_stack_remote_key = UINT64_MAX;
  return true;
}

void YCSBTxnManager::debug_life_state() const {
  printf(" life_waiting=%d prep=%lu finish=%lu wait_stack=%d wait_depth=%lu "
         "wait_reason=%u wait_node=%lu wait_key=%lu pending_tid=%lu/%lu "
         "owner_tid=%lu/%lu state=%d next=%lu",
         life_finalize_waiting ? 1 : 0, life_prepare_pending,
         life_finish_pending, life_wait_stack_active ? 1 : 0,
         life_wait_stack == NULL ? 0UL : life_wait_stack->size(),
         life_wait_stack_reason, life_wait_stack_remote_node,
         life_wait_stack_remote_key,
         life_pending_finalize.tid.time, life_pending_finalize.tid.attempt,
         txn == NULL ? 0UL : txn->life_tid.time,
         txn == NULL ? 0UL : txn->life_tid.attempt, (int)state,
         next_record_id);
}

void YCSBTxnManager::reset_pending_life_finalize() {
  life_finalize_waiting = false;
  life_prepare_pending = 0;
  life_finish_pending = 0;
  life_prepare_failed = false;
  life_prepare_observed_attempt = 0;
  life_pending_finalize = LifeTxnDescriptor();
  life_pending_objects.clear();
  life_pending_remote_nodes.clear();
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

void YCSBTxnManager::send_life_help_request(
    const LifeTxnDescriptor &descriptor) {
  if (GET_NODE_ID(descriptor.tid.time) == g_node_id) {
    apply_life_help_request(descriptor, UINT64_MAX);
    return;
  }

  LifeHelpMessage *msg = (LifeHelpMessage *)Message::create_message(RLIFE_HELP);
  msg->txn_id = descriptor.tid.time;
  msg->return_node_id = g_node_id;
  msg->descriptor = descriptor;
  send_life_message_to_node(msg, GET_NODE_ID(descriptor.tid.time));
}

void YCSBTxnManager::send_life_help_apply_messages(
    const LifeTxnDescriptor &descriptor, const std::vector<uint64_t> &nodes,
    uint64_t requester_node_id) {
  for (std::vector<uint64_t>::const_iterator it = nodes.begin();
       it != nodes.end(); ++it) {
    if (*it == requester_node_id)
      continue;
    LifeHelpApplyMessage *msg =
        (LifeHelpApplyMessage *)Message::create_message(RLIFE_HELP_APPLY);
    msg->txn_id = descriptor.tid.time;
    msg->descriptor = descriptor;
    send_life_message_to_node(msg, *it);
  }
}

void YCSBTxnManager::send_life_finalize_request(
    const LifeTxnDescriptor &descriptor) {
    LifeFinalizeMessage *msg =
      (LifeFinalizeMessage *)Message::create_message(RLIFE_FINALIZE);
  msg->txn_id = descriptor.tid.time;
  msg->return_node_id = g_node_id;
  msg->requester_txn_id = get_txn_id();
  msg->descriptor = descriptor;
  __sync_fetch_and_add(&life_dbg_finalize_sent, 1);
  send_life_message_to_node(msg, GET_NODE_ID(descriptor.tid.time));
}

void YCSBTxnManager::add_life_finalize_requester(
    uint64_t requester_node_id, uint64_t requester_txn_id) {
  if (requester_node_id == UINT64_MAX || requester_node_id == g_node_id)
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
  if (requester_node_id == UINT64_MAX || requester_node_id == g_node_id)
    return;
  if (requester_txn_id == UINT64_MAX)
    return;

  LifeFinalizeResponseMessage *response =
      (LifeFinalizeResponseMessage *)Message::create_message(
          RLIFE_FINALIZE_RSP);
  response->txn_id = requester_txn_id;
  response->result = result;
  __sync_fetch_and_add(&life_dbg_finalize_rsp_sent, 1);
  msg_queue.enqueue(get_thd_id(), response, requester_node_id);
}

void YCSBTxnManager::send_life_prepare_messages(
    const LifeTxnDescriptor &descriptor, const std::vector<uint64_t> &nodes) {
  for (std::vector<uint64_t>::const_iterator it = nodes.begin();
       it != nodes.end(); ++it) {
    LifePrepareMessage *msg =
        (LifePrepareMessage *)Message::create_message(RLIFE_PREPARE);
    msg->txn_id = get_txn_id();
    msg->descriptor = descriptor;
    __sync_fetch_and_add(&life_dbg_prepare_sent, 1);
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
    msg->descriptor = descriptor;
    msg->decision = decision;
    __sync_fetch_and_add(&life_dbg_finish_sent, 1);
    msg_queue.enqueue(get_thd_id(), msg, *it);
  }
}

RC YCSBTxnManager::apply_life_execute_response(
    const LifeExecuteResult &result) {
  LifeTxnDescriptor descriptor = life_descriptor();
  descriptor.ycsb =
      make_life_ycsb_snapshot(*(YCSBQuery *)query, state, next_record_id);
  std::vector<LifeTxnDescriptor> txns;
  const bool has_saved_stack = take_life_wait_stack(txns);
  if (!has_saved_stack)
    txns.push_back(descriptor);

  switch (result.code) {

  case LifeResultCode::Success: {
    const LifeTxnDescriptor &response = result.transaction;
    const bool owns_response =
        response.pid == txn->life_pid && response.tid == txn->life_tid;
    if (!owns_response) {
      assert(!txns.empty());
      txns.back() = response;
      return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;
    }
    copy_life_descriptor_to_workload(result.transaction);
    break;
  }

  case LifeResultCode::Committed: {
    const LifeTxnDescriptor &response = result.transaction;
    const bool owns_response =
        response.pid == txn->life_pid && response.tid.time == txn->life_tid.time;
    rollback_life_descriptor(owns_response ? descriptor : response);
    if (!owns_response) {
      assert(!txns.empty());
      txns.pop_back();
      if (!txns.empty())
        return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;
      return run_life_txn();
    }
    return run_life_txn();
  }

  case LifeResultCode::Help: {
    LifeTxnDescriptor helped = result.transaction;
    const bool owns_helped =
        helped.pid == txn->life_pid && helped.tid.time == txn->life_tid.time;
    if (owns_helped) {
      if (has_saved_stack && !txns.empty()) {
        txns.resize(1);
        txns.back() = descriptor;
        return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;
      }
      return run_life_txn();
    }

    for (std::vector<LifeTxnDescriptor>::iterator it = txns.begin();
         it != txns.end();) {
      if (it->tid.time == helped.tid.time)
        it = txns.erase(it);
      else
        ++it;
    }
    txns.push_back(helped);
    return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;
  }

  case LifeResultCode::Finalize: {
    LifeTxnDescriptor finalize = result.transaction;
    const bool finalized = finalize_life_descriptor(finalize);
    if (life_finalize_waiting) {
      save_life_wait_stack(txns, 2, GET_NODE_ID(life_pending_finalize.tid.time),
                           life_pending_finalize.tid.time);
      return WAIT_REM;
    }
    if (!finalized)
      return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;
    const uint64_t ctx_time = txns.empty() ? descriptor.tid.time
                                           : txns.back().tid.time;
    if (finalize.tid.time >= ctx_time)
      return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;

    txns.push_back(finalize);
    return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;
  }

  case LifeResultCode::Retry: {
    LifeTxnDescriptor retry = result.transaction;
    const bool owns_retry =
        retry.pid == txn->life_pid && retry.tid.time == txn->life_tid.time;
    if (owns_retry)
      retry = descriptor;
    rollback_life_descriptor(retry);
    retry.tid.attempt = std::max(retry.tid.attempt, result.observed_attempt) + 1;
    retry.history.clear();
    retry.touched_objects.clear();
    retry.ycsb.next_record_id = 0;
    retry.ycsb.state = YCSB_0;
    if (owns_retry) {
      copy_life_descriptor_to_workload(retry);
      break;
    }

    assert(!txns.empty());
    txns.back() = retry;
    return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;
  }
    break;

  default:
    assert(false);
    return Abort;
  }

  return run_life_txn();
}

RC YCSBTxnManager::apply_life_prepare_response(
    const LifeExecuteResult &result) {
  if (!life_finalize_waiting || life_prepare_pending == 0)
    return run_life_txn();

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
    send_life_finish_messages(life_pending_finalize, life_pending_remote_nodes,
                              Abort);
    reset_life_descriptor(life_pending_finalize, life_prepare_observed_attempt);
    life_finish_pending = life_pending_remote_nodes.size();
    return life_finish_pending > 0 ? WAIT_REM
                                   : apply_life_finish_response(result);
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
  life_finish_pending = life_pending_remote_nodes.size();
  return life_finish_pending > 0 ? WAIT_REM
                                 : apply_life_finish_response(result);
}

RC YCSBTxnManager::apply_life_finish_response(const LifeExecuteResult &result) {
  (void)result;
  if (!life_finalize_waiting || life_finish_pending == 0)
    return run_life_txn();

  --life_finish_pending;
  if (life_finish_pending > 0)
    return WAIT_REM;

  const bool committed = !life_prepare_failed;
  LifeTxnDescriptor finished = life_pending_finalize;
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

  const bool owns_pending = finished.pid == txn->life_pid &&
                            finished.tid.time == txn->life_tid.time;
  if (!owns_pending) {
    std::vector<LifeTxnDescriptor> txns;
    const bool has_saved_stack = take_life_wait_stack(txns);
    reset_pending_life_finalize();

    if (has_saved_stack && !txns.empty()) {
      if (committed) {
        txns.pop_back();
      } else {
        txns.back() = finished;
      }
      if (!txns.empty())
        return try_life_transactions(txns) ? run_life_txn() : WAIT_REM;
    }
    return run_life_txn();
  }

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
  std::vector<LifeFinalizeObject> fallback_objects;
  const std::vector<LifeFinalizeObject> *objects = &descriptor.touched_objects;
  size_t cached_indices = 0;
  for (std::vector<LifeFinalizeObject>::const_iterator it = objects->begin();
       it != objects->end(); ++it) {
    cached_indices += it->history_indices.size();
  }
  if (cached_indices != descriptor.history.size()) {
    fallback_objects.reserve(descriptor.history.size());
    collect_life_objects(descriptor, fallback_objects);
    objects = &fallback_objects;
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

  uint64_t observed_attempt = descriptor.tid.attempt;
  std::set<uint64_t> remote_nodes;
  for (std::vector<LifeFinalizeObject>::const_iterator it = objects.begin();
       it != objects.end(); ++it) {
    const uint64_t node_id = GET_NODE_ID(it->object.partition_id);
    if (node_id != g_node_id) {
      remote_nodes.insert(node_id);
      continue;
    }

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
    reset_life_descriptor(descriptor, observed_attempt);
    return false;
  }

  if (!remote_nodes.empty()) {
    const bool owns_descriptor = descriptor.pid == txn->life_pid &&
                                 descriptor.tid.time == txn->life_tid.time;
    const uint64_t home_node = GET_NODE_ID(descriptor.tid.time);
    if (!owns_descriptor && g_node_id != home_node) {
      rollback_life_descriptor(descriptor);
      life_finalize_waiting = true;
      life_pending_finalize = descriptor;
      send_life_finalize_request(descriptor);
      return false;
    }

    life_finalize_waiting = true;
    life_prepare_pending = remote_nodes.size();
    life_finish_pending = 0;
    life_prepare_failed = false;
    life_prepare_observed_attempt = descriptor.tid.attempt;
    life_pending_finalize = descriptor;
    life_pending_finalize.touched_objects = objects;
    life_pending_objects = objects;
    life_pending_remote_nodes.assign(remote_nodes.begin(), remote_nodes.end());
    send_life_prepare_messages(life_pending_finalize,
                               life_pending_remote_nodes);
    return false;
  }

  for (std::vector<LifeFinalizeObject>::const_iterator it = objects.begin();
       it != objects.end(); ++it) {
    Row_life *manager = it->manager;
    if (manager == NULL)
      manager = lookup_life_row(it->object)->manager;
    assert(manager != NULL);
    manager->commit(frozen, it->history_indices);
  }
  return true;
}

void YCSBTxnManager::reset_life_descriptor(LifeTxnDescriptor &descriptor,
                                           uint64_t observed_attempt) {
  rollback_life_descriptor(descriptor);
  descriptor.tid.attempt =
      std::max(descriptor.tid.attempt, observed_attempt) + 1;
  descriptor.history.clear();
  descriptor.touched_objects.clear();
  descriptor.ycsb.next_record_id = 0;
  descriptor.ycsb.state = YCSB_0;
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
  operation.field_id = 0;
  operation.value_size = sizeof(uint64_t);

  if (request.kind == LifeYcsbRequestKind::Read ||
      request.kind == LifeYcsbRequestKind::Scan) {
    operation.kind = LifeOperationKind::ReadField;
    return operation;
  }

  assert(request.kind == LifeYcsbRequestKind::Write);
  operation.kind = LifeOperationKind::WriteField;
  const uint64_t value = 0;
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
      rc = run_ycsb_0(req, row);
    } else {
      rc = send_remote_request();
    }

    break;
  case YCSB_1:
    rc = run_ycsb_1(req->acctype, row);
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

RC YCSBTxnManager::run_ycsb_0(ycsb_request *req, row_t *&row_local) {
  RC rc = RCOK;
  int part_id = _wl->key_to_part(req->key);
  access_t type = req->acctype;
  itemid_t *m_item;

  m_item = index_read(_wl->the_index, req->key, part_id);

  row_t *row = ((row_t *)m_item->location);

  rc = get_row(row, type, row_local);

  return rc;
}

RC YCSBTxnManager::run_ycsb_1(access_t acctype, row_t *row_local) {
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
    *(uint64_t *)(&data[fid * 100]) = 0;
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
#if YCSB_ABORT_MODE
      if (query->participant_nodes[g_node_id] == 1) {
        calvin_expected_rsp_cnt--;
      }
#else
      calvin_expected_rsp_cnt = 0;
#endif
      DEBUG("(%ld,%ld) expects %d responses;\n", txn->txn_id, txn->batch_id,
            calvin_expected_rsp_cnt);

      this->phase = CALVIN_LOC_RD;
      break;
    case CALVIN_LOC_RD:
      // Phase 2: Perform local reads
      DEBUG("(%ld,%ld) local reads\n", txn->txn_id, txn->batch_id);
      rc = run_ycsb();
      // release_read_locks(query);

      this->phase = CALVIN_SERVE_RD;
      break;
    case CALVIN_SERVE_RD:
      // Phase 3: Serve remote reads
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
      // Phase 4: Collect remote reads
      this->phase = CALVIN_EXEC_WR;
      break;
    case CALVIN_EXEC_WR:
      // Phase 5: Execute transaction / perform local writes
      DEBUG("(%ld,%ld) execute writes\n", txn->txn_id, txn->batch_id);
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
    if (this->phase == CALVIN_LOC_RD && req->acctype == WR)
      continue;
    if (this->phase == CALVIN_EXEC_WR && req->acctype == RD)
      continue;

    uint64_t part_id = _wl->key_to_part(req->key);
    bool loc = GET_NODE_ID(part_id) == g_node_id;

    if (!loc)
      continue;

    rc = run_ycsb_0(req, row);
    assert(rc == RCOK);

    rc = run_ycsb_1(req->acctype, row);
    assert(rc == RCOK);
  }
  return rc;
}

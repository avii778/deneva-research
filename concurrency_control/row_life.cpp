#include "row_life.h"
#include "../storage/catalog.h"
#include "../storage/row.h"
#include "../storage/table.h"
#include "life_types.h"
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>

namespace {

class LifeLatchGuard {
public:
  explicit LifeLatchGuard(pthread_mutex_t *latch) : latch_(latch) {
    pthread_mutex_lock(latch_);
  }

  ~LifeLatchGuard() { pthread_mutex_unlock(latch_); }

private:
  pthread_mutex_t *latch_;
};

} // namespace

void Row_life::init(row_t *row) {
  assert(row != NULL);

  _row = row;

  committed_state.assign(reinterpret_cast<const uint8_t *>(row->get_data()),
                         reinterpret_cast<const uint8_t *>(row->get_data()) +
                             row->get_tuple_size());

  active_process.reset();

  processes.clear();
  inline_operation.reset();

  pthread_mutex_init(&latch, NULL);
}

bool Row_life::higher_priority(const LifeTxnId &lhs, const LifeTxnId &rhs) {
  return lhs < rhs;
}

bool Row_life::priority_less_equal(const LifeTxnId &lhs, const LifeTxnId &rhs) {
  return lhs == rhs || higher_priority(lhs, rhs);
}

std::vector<LifeHistoryEntry>
Row_life::object_history(const LifeTxnDescriptor &tx,
                         const LifeObjectId &object) {

  std::vector<LifeHistoryEntry> history;

  for (std::vector<LifeHistoryEntry>::const_iterator it = tx.history.begin();
       it != tx.history.end(); ++it) {
    if (it->operation.object == object) {
      history.push_back(*it);
    }
  }

  return history;
}

bool Row_life::pid_equals(const LifeProcessId &pid1,
                          const LifeProcessId &pid2) {
  return pid1.node_id == pid2.node_id && pid1.worker_id == pid2.worker_id;
}
// Returns the record of the last tx this process was running, with a special
// value for none
LifeProcessRecord Row_life::process_record(const LifeProcessId &pid) const {

  ProcessMap::const_iterator it = processes.find(pid);

  if (it != processes.end())
    return it->second;

  LifeProcessRecord record = LifeProcessRecord();
  record.transaction.pid = pid;
  record.transaction.tid.time = 0;
  record.transaction.tid.attempt = 0;
  record.status = LifeTxnStatus::Aborted;
  return record;
}

// Return the record of the current proposed action for the transaction
LifeProcessRecord Row_life::context_record() const {

  if (active_process.has_value)
    return process_record(active_process.value);

  LifeProcessRecord record = LifeProcessRecord();
  record.transaction.tid.time = std::numeric_limits<uint64_t>::max();
  record.transaction.tid.attempt = std::numeric_limits<uint64_t>::max();
  record.status = LifeTxnStatus::Aborted;
  return record;
}

// Make a LifeExecuteResult object
LifeExecuteResult Row_life::make_result(LifeResultCode code) const {

  LifeExecuteResult result = LifeExecuteResult();
  result.code = code;
  return result;
}

// Get the id of this object
LifeObjectId Row_life::object_id() const {

  LifeObjectId id;
  id.table_id = _row->get_table()->get_table_id();
  id.partition_id = _row->get_part_id();
  id.primary_key = _row->get_primary_key();
  return id;
}

// Updates state if this is a write, updates response if this is a read
// according to the operation
bool Row_life::apply_operation(const LifeOperation &operation,
                               std::vector<uint8_t> &state,
                               LifeResponse &response) const {

  response.value.clear();

  if (operation.object != object_id() ||
      operation.field_id >= _row->get_field_cnt() ||
      state.size() != _row->get_tuple_size()) {
    return false;
  }

  const uint64_t field_offset =
      _row->get_schema()->get_field_index(operation.field_id);
  const uint64_t field_size =
      _row->get_schema()->get_field_size(operation.field_id);

  if (field_offset > state.size() || field_size > state.size() - field_offset)
    return false;

  if (operation.kind == LifeOperationKind::ReadField) {

    if (!operation.argument.empty())
      return false;

    response.value.assign(state.begin() + field_offset,
                          state.begin() + field_offset + field_size);
    return true;
  }

  if (operation.kind == LifeOperationKind::WriteField) {
    if (operation.argument.size() != field_size)
      return false;

    std::copy(operation.argument.begin(), operation.argument.end(),
              state.begin() + field_offset);

    return true;
  }

  return false;
}

bool Row_life::replay_history(const std::vector<LifeHistoryEntry> &history,
                              std::vector<uint8_t> &state) const {

  for (std::vector<LifeHistoryEntry>::const_iterator it = history.begin();
       it != history.end(); ++it) {
    LifeResponse replayed_response;

    if (!apply_operation(it->operation, state, replayed_response) ||
        replayed_response != it->response) {
      return false;
    }
  }
  return true;
}

LifeExecuteResult Row_life::execute(const LifeTxnDescriptor &tx,
                                    const LifeOperation &operation) {

  LifeLatchGuard guard(&latch);

  if (operation.object != object_id())
    return make_result(LifeResultCode::InvalidOperation);

  const LifeProcessRecord context = context_record();
  const LifeProcessRecord local = process_record(tx.pid);

  const std::vector<LifeHistoryEntry> tx_object_history =
      object_history(tx, operation.object);

  const bool same_process_txn_time =
      tx.tid.time == local.transaction.tid.time;
  const bool local_has_newer_attempt =
      same_process_txn_time && tx.tid.attempt < local.transaction.tid.attempt;
  const bool same_process_txn_attempt = tx.tid == local.transaction.tid;

  const bool must_defer =
      context.status == LifeTxnStatus::Prepared ||
      context.transaction.tid < tx.tid || local_has_newer_attempt ||
      (same_process_txn_attempt &&
       (local.status == LifeTxnStatus::Aborted ||
        tx.history.size() < local.transaction.history.size()));

  if (must_defer) {

    if (context.status == LifeTxnStatus::Prepared) {

      if (!inline_operation.has_value ||
          priority_less_equal(tx.tid, inline_operation.value.transaction.tid)) {
        LifeInlineOperation pending;
        pending.transaction = tx;
        pending.operation = operation;
        inline_operation.set(pending);
      }

      LifeExecuteResult result = make_result(LifeResultCode::Finalize);
      result.transaction = context.transaction;
      return result;
    }

    if (higher_priority(context.transaction.tid, tx.tid)) {
      LifeExecuteResult result = make_result(LifeResultCode::Help);
      result.transaction = context.transaction;
      return result;
    }

    if (tx.tid.time < local.transaction.tid.time)
      return make_result(LifeResultCode::Committed);

    if (same_process_txn_time && local.status == LifeTxnStatus::Committed)
      return make_result(LifeResultCode::Committed);

    if (local_has_newer_attempt ||
        (same_process_txn_attempt && local.status == LifeTxnStatus::Aborted)) {
      LifeExecuteResult result = make_result(LifeResultCode::Retry);
      result.observed_attempt = local.transaction.tid.attempt;
      return result;
    }

    if (tx.history.size() < local.transaction.history.size()) {
      const uint64_t response_index = tx_object_history.size();
      if (response_index >=
              object_history(local.transaction, object_id()).size() ||
          object_history(local.transaction, object_id())[response_index]
                  .operation != operation) {
        return make_result(LifeResultCode::InvalidOperation);
      }

      LifeExecuteResult result = make_result(LifeResultCode::Success);
      result.response =
          object_history(local.transaction, object_id())[response_index]
              .response;
      return result;
    }
  }

  if (active_process.has_value) {
    ProcessMap::iterator active = processes.find(active_process.value);
    if (active != processes.end())
      active->second.status = LifeTxnStatus::Aborted;
  }

  std::vector<uint8_t> speculative_state = committed_state;

  if (!replay_history(tx_object_history, speculative_state))
    return make_result(LifeResultCode::InvalidOperation);

  LifeResponse response;
  if (!apply_operation(operation, speculative_state, response))
    return make_result(LifeResultCode::InvalidOperation);

  LifeHistoryEntry entry;
  entry.operation = operation;
  entry.response = response;

  LifeProcessRecord updated;
  updated.transaction = tx;
  updated.transaction.history.push_back(entry);
  updated.status = LifeTxnStatus::Executing;

  active_process.set(tx.pid);
  processes[tx.pid] = updated;

  LifeExecuteResult result = make_result(LifeResultCode::Success);
  result.response = response;
  return result;
}

// the prepare protocol as described in the psuedocode
LifeExecuteResult Row_life::prepare(const LifeTxnDescriptor &tx) {

  LifeLatchGuard guard(&latch);
  LifeProcessRecord local = process_record(tx.pid);

  bool already_committed = (tx.tid.time < local.transaction.tid.time) ||
                           (local.status == LifeTxnStatus::Committed);

  if (already_committed) {
    return make_result(LifeResultCode::Committed);
  }

  bool prepare_must_fail = (tx.tid.attempt < local.transaction.tid.attempt) ||
                           (local.status == LifeTxnStatus::Aborted);

  if (prepare_must_fail) {
    LifeExecuteResult result = make_result(LifeResultCode::Retry);
    result.observed_attempt = local.transaction.tid.attempt;
    return result;
  }

  // we may prepare now

  LifeProcessRecord updated;
  updated.status = LifeTxnStatus::Prepared;
  updated.transaction = tx;
  processes[tx.pid] = updated;

  return make_result(LifeResultCode::Success);
}

void Row_life::commit(const LifeTxnDescriptor &tx) {

  {
    LifeLatchGuard guard(&latch);
    LifeProcessRecord local = process_record(tx.pid);

    bool already_commited = (tx.tid.time < local.transaction.tid.time) ||
                            (local.status == LifeTxnStatus::Committed);

    if (already_commited) {
      return;
    }

    // need to make sure that this is right
    std::vector<uint8_t> final = committed_state;
    if (!replay_history(object_history(tx, object_id()), final))
      return;
    committed_state = final;
    std::memcpy(_row->get_data(), committed_state.data(),
                committed_state.size());

    LifeProcessRecord updated;
    updated.transaction = tx;
    updated.status = LifeTxnStatus::Committed;
    processes[tx.pid] = updated;
  }

  help(tx);
}

void Row_life::rollback(const LifeTxnDescriptor &tx) {

  {
    LifeLatchGuard guard(&latch);

    LifeProcessRecord local = process_record(tx.pid);

    if (local.transaction.tid == tx.tid &&
        local.status != LifeTxnStatus::Aborted &&
        local.status != LifeTxnStatus::Committed) {
      processes[tx.pid].status = LifeTxnStatus::Aborted;
    }
  }

  help(tx);
}

void Row_life::help(const LifeTxnDescriptor &tx) {

  LifeInlineOperation pending;
  bool has_pending = false;

  {
    LifeLatchGuard guard(&latch);

    if (!active_process.has_value || !pid_equals(active_process.value, tx.pid))
      return;

    active_process.reset();

    if (inline_operation.has_value) {
      pending = inline_operation.value;
      has_pending = true;
      inline_operation.reset();
    }
  }

  if (has_pending)
    execute(pending.transaction, pending.operation);
}

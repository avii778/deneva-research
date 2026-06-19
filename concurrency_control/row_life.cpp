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

  active_process.reset();

  processes.reset();
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
const LifeProcessRecord *
Row_life::process_record(const LifeProcessId &pid) const {

  if (!processes)
    return NULL;

  ProcessMap::const_iterator it = processes->find(pid);

  if (it != processes->end())
    return &it->second;

  return NULL;
}

LifeProcessRecord &
Row_life::mutable_process_record(const LifeProcessId &pid) {
  if (!processes)
    processes.reset(new ProcessMap());
  return (*processes)[pid];
}

// Return the record of the current proposed action for the transaction
const LifeProcessRecord *Row_life::context_record() const {

  if (active_process.has_value)
    return process_record(active_process.value);

  return NULL;
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

bool Row_life::validate_committed_operation(
    const LifeOperation &operation) const {
  if (operation.object != object_id() ||
      operation.field_id >= _row->get_field_cnt()) {
    return false;
  }

  const uint64_t field_offset =
      _row->get_schema()->get_field_index(operation.field_id);
  const uint64_t field_size =
      _row->get_schema()->get_field_size(operation.field_id);
  const uint64_t tuple_size = _row->get_tuple_size();
  if (field_offset > tuple_size || field_size > tuple_size - field_offset)
    return false;

  if (operation.kind == LifeOperationKind::ReadField)
    return operation.argument.empty();

  return operation.kind == LifeOperationKind::WriteField &&
         operation.argument.size() == field_size;
}

bool Row_life::apply_committed_operation(const LifeOperation &operation) {
  if (!validate_committed_operation(operation))
    return false;

  if (operation.kind == LifeOperationKind::ReadField)
    return true;

  const uint64_t field_offset =
      _row->get_schema()->get_field_index(operation.field_id);
  const uint64_t field_size =
      _row->get_schema()->get_field_size(operation.field_id);
  std::memcpy(_row->get_data() + field_offset, operation.argument.data(),
              field_size);
  return true;
}

LifeExecuteResult Row_life::execute(const LifeTxnDescriptor &tx,
                                    const LifeOperation &operation) {

  LifeLatchGuard guard(&latch);

  if (operation.object != object_id())
    return make_result(LifeResultCode::InvalidOperation);

  const LifeProcessRecord *context = context_record();
  const LifeProcessRecord *local = process_record(tx.pid);
  const LifeTxnId no_local_tid = {0, 0};
  const LifeTxnId no_context_tid = {std::numeric_limits<uint64_t>::max(),
                                    std::numeric_limits<uint64_t>::max()};
  const LifeTxnId &local_tid =
      local != NULL ? local->transaction->tid : no_local_tid;
  const LifeTxnId &context_tid =
      context != NULL ? context->transaction->tid : no_context_tid;
  const LifeTxnStatus local_status =
      local != NULL ? local->status : LifeTxnStatus::Aborted;
  const LifeTxnStatus context_status =
      context != NULL ? context->status : LifeTxnStatus::Aborted;

  const std::vector<LifeHistoryEntry> tx_object_history =
      object_history(tx, operation.object);

  const bool same_process_txn_time =
      tx.tid.time == local_tid.time;
  const bool local_has_newer_attempt =
      same_process_txn_time && tx.tid.attempt < local_tid.attempt;
  const bool same_process_txn_attempt = tx.tid == local_tid;
  const size_t local_history_size =
      local != NULL ? local->transaction->history.size() : 0;

  const bool must_defer =
      context_status == LifeTxnStatus::Prepared || context_tid < tx.tid ||
      local_has_newer_attempt ||
      (same_process_txn_attempt &&
       (local_status == LifeTxnStatus::Aborted ||
        tx.history.size() < local_history_size));

  if (must_defer) {

    if (context_status == LifeTxnStatus::Prepared) {

      if (!inline_operation ||
          priority_less_equal(
              tx.tid, inline_operation->transaction->tid)) {
        LifeInlineOperation pending;
        pending.transaction = std::make_shared<LifeTxnDescriptor>(tx);
        pending.operation = operation;
        inline_operation.reset(new LifeInlineOperation(pending));
      }

      LifeExecuteResult result = make_result(LifeResultCode::Finalize);
      assert(context != NULL && context->transaction);
      result.transaction = *context->transaction;
      return result;
    }

    if (higher_priority(context_tid, tx.tid)) {
      LifeExecuteResult result = make_result(LifeResultCode::Help);
      assert(context != NULL && context->transaction);
      result.transaction = *context->transaction;
      return result;
    }

    if (tx.tid.time < local_tid.time)
      return make_result(LifeResultCode::Committed);

    if (same_process_txn_time && local_status == LifeTxnStatus::Committed)
      return make_result(LifeResultCode::Committed);

    if (local_has_newer_attempt ||
        (same_process_txn_attempt && local_status == LifeTxnStatus::Aborted)) {
      LifeExecuteResult result = make_result(LifeResultCode::Retry);
      result.observed_attempt = local_tid.attempt;
      return result;
    }

    if (tx.history.size() < local_history_size) {
      assert(local != NULL && local->transaction);
      const uint64_t response_index = tx_object_history.size();
      if (response_index >=
              object_history(*local->transaction, object_id()).size() ||
          object_history(*local->transaction, object_id())[response_index]
                  .operation != operation) {
        return make_result(LifeResultCode::InvalidOperation);
      }

      LifeExecuteResult result = make_result(LifeResultCode::Success);
      result.response =
          object_history(*local->transaction, object_id())[response_index]
              .response;
      return result;
    }
  }

  if (active_process.has_value) {
    assert(processes);
    ProcessMap::iterator active = processes->find(active_process.value);
    if (active != processes->end())
      active->second.status = LifeTxnStatus::Aborted;
  }

  const uint8_t *committed =
      reinterpret_cast<const uint8_t *>(_row->get_data());
  std::vector<uint8_t> speculative_state(
      committed, committed + _row->get_tuple_size());

  if (!replay_history(tx_object_history, speculative_state))
    return make_result(LifeResultCode::InvalidOperation);

  LifeResponse response;
  if (!apply_operation(operation, speculative_state, response))
    return make_result(LifeResultCode::InvalidOperation);

  LifeHistoryEntry entry;
  entry.operation = operation;
  entry.response = response;

  LifeProcessRecord updated;
  LifeTxnDescriptor updated_transaction = tx;
  updated_transaction.history.push_back(entry);
  updated.transaction =
      std::make_shared<LifeTxnDescriptor>(std::move(updated_transaction));
  updated.status = LifeTxnStatus::Executing;

  active_process.set(tx.pid);
  mutable_process_record(tx.pid) = updated;

  LifeExecuteResult result = make_result(LifeResultCode::Success);
  result.response = response;
  return result;
}

// the prepare protocol as described in the psuedocode
LifeExecuteResult Row_life::prepare(const LifeTxnDescriptor &tx) {
  return prepare(std::make_shared<LifeTxnDescriptor>(tx));
}

LifeExecuteResult Row_life::prepare(const LifeTxnDescriptorPtr &tx) {
  assert(tx);

  LifeLatchGuard guard(&latch);
  const LifeProcessRecord *local = process_record(tx->pid);
  const LifeTxnId no_local_tid = {0, 0};
  const LifeTxnId &local_tid =
      local != NULL ? local->transaction->tid : no_local_tid;
  const LifeTxnStatus local_status =
      local != NULL ? local->status : LifeTxnStatus::Aborted;

  bool already_committed = (tx->tid.time < local_tid.time) ||
                           (local_status == LifeTxnStatus::Committed);

  if (already_committed) {
    return make_result(LifeResultCode::Committed);
  }

  bool prepare_must_fail = (tx->tid.attempt < local_tid.attempt) ||
                           (local_status == LifeTxnStatus::Aborted);

  if (prepare_must_fail) {
    LifeExecuteResult result = make_result(LifeResultCode::Retry);
    result.observed_attempt = local_tid.attempt;
    return result;
  }

  // we may prepare now

  LifeProcessRecord updated;
  updated.status = LifeTxnStatus::Prepared;
  updated.transaction = tx;
  mutable_process_record(tx->pid) = updated;

  return make_result(LifeResultCode::Success);
}

void Row_life::commit(const LifeTxnDescriptor &tx) {
  LifeTxnDescriptorPtr shared = std::make_shared<LifeTxnDescriptor>(tx);
  std::vector<size_t> history_indices;
  for (size_t i = 0; i < tx.history.size(); ++i) {
    if (tx.history[i].operation.object == object_id())
      history_indices.push_back(i);
  }
  commit(shared, history_indices);
}

void Row_life::commit(const LifeTxnDescriptorPtr &tx,
                      const std::vector<size_t> &history_indices) {
  assert(tx);

  {
    LifeLatchGuard guard(&latch);
    const LifeProcessRecord *local = process_record(tx->pid);
    const LifeTxnId no_local_tid = {0, 0};
    const LifeTxnId &local_tid =
        local != NULL ? local->transaction->tid : no_local_tid;
    const LifeTxnStatus local_status =
        local != NULL ? local->status : LifeTxnStatus::Aborted;

    bool already_commited = (tx->tid.time < local_tid.time) ||
                            (local_status == LifeTxnStatus::Committed);

    if (already_commited) {
      return;
    }

    for (std::vector<size_t>::const_iterator it = history_indices.begin();
         it != history_indices.end(); ++it) {
      if (*it >= tx->history.size() || !validate_committed_operation(
                                            tx->history[*it].operation)) {
        assert(false);
        return;
      }
    }
    for (std::vector<size_t>::const_iterator it = history_indices.begin();
         it != history_indices.end(); ++it) {
      const bool applied =
          apply_committed_operation(tx->history[*it].operation);
      assert(applied);
      (void)applied;
    }

    LifeProcessRecord &updated = mutable_process_record(tx->pid);
    updated.transaction = tx;
    updated.status = LifeTxnStatus::Committed;
  }

  help(*tx);
}

void Row_life::rollback(const LifeTxnDescriptor &tx) {

  {
    LifeLatchGuard guard(&latch);

    const LifeProcessRecord *local = process_record(tx.pid);

    if (local != NULL && local->transaction->tid == tx.tid &&
        local->status != LifeTxnStatus::Aborted &&
        local->status != LifeTxnStatus::Committed) {
      mutable_process_record(tx.pid).status = LifeTxnStatus::Aborted;
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

    if (inline_operation) {
      pending = *inline_operation;
      has_pending = true;
      inline_operation.reset();
    }
  }

  if (has_pending)
    execute(*pending.transaction, pending.operation);
}

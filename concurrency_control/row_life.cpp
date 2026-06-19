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
  explicit LifeLatchGuard(pthread_mutex_t *latch)
      : latch_(latch), locked_(true) {
    pthread_mutex_lock(latch_);
  }

  ~LifeLatchGuard() {
    if (locked_)
      pthread_mutex_unlock(latch_);
  }

  void unlock() {
    assert(locked_);
    pthread_mutex_unlock(latch_);
    locked_ = false;
  }

private:
  pthread_mutex_t *latch_;
  bool locked_;
};

} // namespace

void Row_life::init(row_t *row) {
  assert(row != NULL);

  _row = row;
  _schema = row->get_schema();
  _object_id.table_id = row->get_table()->get_table_id();
  _object_id.partition_id = row->get_part_id();
  _object_id.primary_key = 0;
  _primary_key_cached = false;
  _tuple_size = row->get_tuple_size();
  _field_count = row->get_field_cnt();

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

const LifeTxnDescriptor::TouchedObject *
Row_life::touched_object(const LifeTxnDescriptor &tx) const {
  for (std::vector<LifeTxnDescriptor::TouchedObject>::const_iterator it =
           tx.touched_objects.begin();
       it != tx.touched_objects.end(); ++it) {
    if ((it->manager != NULL && it->manager == this) ||
        it->object == object_id()) {
      return &*it;
    }
  }
  return NULL;
}

size_t Row_life::object_history_size(const LifeTxnDescriptor &tx) const {
  const LifeTxnDescriptor::TouchedObject *touched = touched_object(tx);
  if (touched != NULL)
    return touched->history_indices.size();

  size_t count = 0;
  for (std::vector<LifeHistoryEntry>::const_iterator it = tx.history.begin();
       it != tx.history.end(); ++it) {
    if (it->operation.object == object_id())
      ++count;
  }
  return count;
}

const LifeHistoryEntry *
Row_life::object_history_entry(const LifeTxnDescriptor &tx,
                               size_t object_index) const {
  const LifeTxnDescriptor::TouchedObject *touched = touched_object(tx);
  if (touched != NULL) {
    if (object_index >= touched->history_indices.size())
      return NULL;
    const size_t history_index = touched->history_indices[object_index];
    return history_index < tx.history.size() ? &tx.history[history_index]
                                             : NULL;
  }

  for (std::vector<LifeHistoryEntry>::const_iterator it = tx.history.begin();
       it != tx.history.end(); ++it) {
    if (it->operation.object != object_id())
      continue;
    if (object_index == 0)
      return &*it;
    --object_index;
  }
  return NULL;
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

  assert(pid.node_id < g_node_cnt);
  const size_t index =
      static_cast<size_t>(pid.worker_id) * g_node_cnt + pid.node_id;
  if (index >= processes->size() || !(*processes)[index].has_value)
    return NULL;
  return &(*processes)[index];
}

LifeProcessRecord &Row_life::mutable_process_record(const LifeProcessId &pid) {
  if (!processes)
    processes.reset(new ProcessSlots());
  assert(pid.node_id < g_node_cnt);
  const size_t index =
      static_cast<size_t>(pid.worker_id) * g_node_cnt + pid.node_id;
  if (index >= processes->size())
    processes->resize(index + 1);
  return (*processes)[index];
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
const LifeObjectId &Row_life::object_id() const {
  if (!_primary_key_cached) {
    _object_id.primary_key = _row->get_primary_key();
    _primary_key_cached = true;
  }
  return _object_id;
}

// Updates state if this is a write, updates response if this is a read
// according to the operation
bool Row_life::apply_operation(const LifeOperation &operation,
                               std::vector<uint8_t> &state,
                               LifeResponse &response) const {

  response.value.clear();

  if (operation.object != object_id() || operation.field_id >= _field_count ||
      state.size() != _tuple_size) {
    return false;
  }

  const uint64_t field_offset = _schema->get_field_index(operation.field_id);
  const uint64_t field_size = _schema->get_field_size(operation.field_id);

  if (operation.value_size == 0 ||
      operation.value_size > LIFE_INLINE_VALUE_CAPACITY ||
      operation.value_size > field_size || field_offset > state.size() ||
      operation.value_size > state.size() - field_offset)
    return false;

  if (operation.kind == LifeOperationKind::ReadField) {

    if (!operation.argument.empty())
      return false;

    response.value.assign(state.begin() + field_offset,
                          state.begin() + field_offset + operation.value_size);
    return true;
  }

  if (operation.kind == LifeOperationKind::WriteField) {
    if (operation.argument.size() != operation.value_size)
      return false;

    std::copy(operation.argument.begin(), operation.argument.end(),
              state.begin() + field_offset);

    return true;
  }

  return false;
}

bool Row_life::replay_history(const LifeTxnDescriptor &tx,
                              std::vector<uint8_t> &state) const {
  const LifeTxnDescriptor::TouchedObject *touched = touched_object(tx);
  if (touched != NULL) {
    for (std::vector<size_t>::const_iterator it =
             touched->history_indices.begin();
         it != touched->history_indices.end(); ++it) {
      if (*it >= tx.history.size())
        return false;
      const LifeHistoryEntry &entry = tx.history[*it];
      LifeResponse replayed_response;
      if (!apply_operation(entry.operation, state, replayed_response) ||
          replayed_response != entry.response) {
        return false;
      }
    }
    return true;
  }

  for (std::vector<LifeHistoryEntry>::const_iterator it = tx.history.begin();
       it != tx.history.end(); ++it) {
    if (it->operation.object != object_id())
      continue;
    LifeResponse replayed_response;
    if (!apply_operation(it->operation, state, replayed_response) ||
        replayed_response != it->response)
      return false;
  }
  return true;
}

bool Row_life::validate_committed_operation(
    const LifeOperation &operation) const {
  if (operation.object != object_id() || operation.field_id >= _field_count) {
    return false;
  }

  const uint64_t field_offset = _schema->get_field_index(operation.field_id);
  const uint64_t field_size = _schema->get_field_size(operation.field_id);
  if (operation.value_size == 0 ||
      operation.value_size > LIFE_INLINE_VALUE_CAPACITY ||
      operation.value_size > field_size || field_offset > _tuple_size ||
      operation.value_size > _tuple_size - field_offset)
    return false;

  if (operation.kind == LifeOperationKind::ReadField)
    return operation.argument.empty();

  return operation.kind == LifeOperationKind::WriteField &&
         operation.argument.size() == operation.value_size;
}

bool Row_life::apply_committed_operation(const LifeOperation &operation) {
  if (!validate_committed_operation(operation))
    return false;

  if (operation.kind == LifeOperationKind::ReadField)
    return true;

  const uint64_t field_offset = _schema->get_field_index(operation.field_id);
  std::memcpy(_row->get_data() + field_offset, operation.argument.data(),
              operation.value_size);
  return true;
}

LifeExecuteResult Row_life::execute(const LifeTxnDescriptor &tx,
                                    const LifeOperation &operation) {

  // These allocations and the descriptor copy depend only on caller-owned
  // state. Do them before taking the row latch so allocator and deep-copy
  // latency do not serialize otherwise independent contenders on this row.
  std::vector<uint8_t> speculative_state(_tuple_size);
  std::shared_ptr<LifeTxnDescriptor> updated_transaction =
      std::make_shared<LifeTxnDescriptor>(tx);

  LifeLatchGuard guard(&latch);

  if (operation.object != object_id())
    return make_result(LifeResultCode::InvalidOperation);

  const LifeProcessRecord *context = context_record();
  const LifeProcessRecord *local = process_record(tx.pid);
  const LifeTxnId no_local_tid = {0, 0};
  const LifeTxnId no_context_tid = {std::numeric_limits<uint64_t>::max(),
                                    std::numeric_limits<uint64_t>::max()};
  const LifeTxnId &local_tid = local != NULL ? local->tid : no_local_tid;
  const LifeTxnId &context_tid =
      context != NULL ? context->tid : no_context_tid;
  const LifeTxnStatus local_status =
      local != NULL ? local->status : LifeTxnStatus::Aborted;
  const LifeTxnStatus context_status =
      context != NULL ? context->status : LifeTxnStatus::Aborted;

  const size_t tx_object_history_size = object_history_size(tx);

  const bool same_process_txn_time = tx.tid.time == local_tid.time;
  const bool local_has_newer_attempt =
      same_process_txn_time && tx.tid.attempt < local_tid.attempt;
  const bool same_process_txn_attempt = tx.tid == local_tid;
  const size_t local_history_size = local != NULL && local->transaction
                                        ? local->transaction->history.size()
                                        : 0;

  const bool must_defer =
      context_status == LifeTxnStatus::Prepared || context_tid < tx.tid ||
      tx.tid.time < local_tid.time || local_has_newer_attempt ||
      (same_process_txn_attempt && (local_status == LifeTxnStatus::Aborted ||
                                    local_status == LifeTxnStatus::Committed ||
                                    tx.history.size() < local_history_size));

  if (must_defer) {

    if (context_status == LifeTxnStatus::Prepared) {

      if (!inline_operation ||
          priority_less_equal(tx.tid, inline_operation->transaction->tid)) {
        LifeInlineOperation pending;
        pending.transaction = updated_transaction;
        pending.operation = operation;
        inline_operation.reset(new LifeInlineOperation(pending));
      }

      LifeExecuteResult result = make_result(LifeResultCode::Finalize);
      assert(context != NULL && context->transaction);
      const LifeTxnDescriptorPtr context_transaction = context->transaction;
      guard.unlock();
      result.transaction = *context_transaction;
      return result;
    }

    if (higher_priority(context_tid, tx.tid)) {
      LifeExecuteResult result = make_result(LifeResultCode::Help);
      assert(context != NULL && context->transaction);
      const LifeTxnDescriptorPtr context_transaction = context->transaction;
      guard.unlock();
      result.transaction = *context_transaction;
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
      const LifeHistoryEntry *stored_entry =
          object_history_entry(*local->transaction, tx_object_history_size);
      if (stored_entry == NULL || stored_entry->operation != operation) {
        return make_result(LifeResultCode::InvalidOperation);
      }

      LifeExecuteResult result = make_result(LifeResultCode::Success);
      result.response = stored_entry->response;
      return result;
    }
  }

  // Keep committed descriptors helpable/idempotent until a later transaction
  // is admitted on this row. Once that happens, their terminal ID/status is
  // sufficient to reject stale execute/commit calls without retaining the
  // full workload and history payload.
  if (processes) {
    for (ProcessSlots::iterator it = processes->begin(); it != processes->end();
         ++it) {
      if (it->has_value && it->status == LifeTxnStatus::Committed &&
          it->tid < tx.tid) {
        it->transaction.reset();
      }
    }
  }

  if (active_process.has_value) {
    assert(processes);
    LifeProcessRecord *active =
        const_cast<LifeProcessRecord *>(process_record(active_process.value));
    if (active != NULL)
      active->status = LifeTxnStatus::Aborted;
  }

  const uint8_t *committed =
      reinterpret_cast<const uint8_t *>(_row->get_data());
  std::copy(committed, committed + _tuple_size, speculative_state.begin());

  if (!replay_history(tx, speculative_state))
    return make_result(LifeResultCode::InvalidOperation);

  LifeResponse response;
  if (!apply_operation(operation, speculative_state, response))
    return make_result(LifeResultCode::InvalidOperation);

  LifeHistoryEntry entry;
  entry.operation = operation;
  entry.response = response;

  LifeProcessRecord updated;
  life_append_history(*updated_transaction, entry);
  updated.transaction = updated_transaction;
  updated.tid = tx.tid;
  updated.status = LifeTxnStatus::Executing;
  updated.has_value = true;

  active_process.set(tx.pid);
  mutable_process_record(tx.pid) = updated;

  LifeExecuteResult result = make_result(LifeResultCode::Success);
  guard.unlock();
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
  const LifeTxnId &local_tid = local != NULL ? local->tid : no_local_tid;
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
  updated.tid = tx->tid;
  updated.has_value = true;
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

  std::unique_ptr<LifeInlineOperation> pending;

  // Bounds checking only examines the immutable frozen descriptor and does
  // not need to extend the row's critical section.
  for (std::vector<size_t>::const_iterator it = history_indices.begin();
       it != history_indices.end(); ++it) {
    if (*it >= tx->history.size()) {
      assert(false);
      return;
    }
  }

  {
    LifeLatchGuard guard(&latch);
    const LifeProcessRecord *local = process_record(tx->pid);
    const LifeTxnId no_local_tid = {0, 0};
    const LifeTxnId &local_tid = local != NULL ? local->tid : no_local_tid;
    const LifeTxnStatus local_status =
        local != NULL ? local->status : LifeTxnStatus::Aborted;

    bool already_commited = (tx->tid.time < local_tid.time) ||
                            (local_status == LifeTxnStatus::Committed);

    if (already_commited) {
      return;
    }

    for (std::vector<size_t>::const_iterator it = history_indices.begin();
         it != history_indices.end(); ++it) {
      if (!validate_committed_operation(tx->history[*it].operation)) {
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
    // Retain the committed descriptor until a later transaction is admitted
    // on this row. The later execute path compacts it to an ID/status
    // tombstone.
    updated.transaction = tx;
    updated.tid = tx->tid;
    updated.status = LifeTxnStatus::Committed;
    updated.has_value = true;

    // Publish the terminal record and detach the active context atomically.
    // A contender must never observe an active committed record whose full
    // descriptor has already been released.
    if (active_process.has_value && pid_equals(active_process.value, tx->pid)) {
      active_process.reset();
      pending = std::move(inline_operation);
    }
  }

  if (pending)
    execute(*pending->transaction, pending->operation);
}

void Row_life::rollback(const LifeTxnDescriptor &tx) {

  {
    LifeLatchGuard guard(&latch);

    const LifeProcessRecord *local = process_record(tx.pid);

    if (local != NULL && local->tid == tx.tid &&
        local->status != LifeTxnStatus::Aborted &&
        local->status != LifeTxnStatus::Committed) {
      mutable_process_record(tx.pid).status = LifeTxnStatus::Aborted;
    }
  }

  help(tx);
}

void Row_life::help(const LifeTxnDescriptor &tx) {

  std::unique_ptr<LifeInlineOperation> pending;

  {
    LifeLatchGuard guard(&latch);

    if (!active_process.has_value || !pid_equals(active_process.value, tx.pid))
      return;

    active_process.reset();

    pending = std::move(inline_operation);
  }

  if (pending)
    execute(*pending->transaction, pending->operation);
}

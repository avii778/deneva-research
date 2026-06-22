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

LifeTxnId min_txn_id() { return LifeTxnId{0, 0}; }

LifeTxnId max_txn_id() {
  return LifeTxnId{std::numeric_limits<uint64_t>::max(),
                   std::numeric_limits<uint64_t>::max()};
}

const LifeTxnId &record_tid_or(const LifeProcessRecord *record,
                               const LifeTxnId &fallback) {
  return record != NULL ? record->tid : fallback;
}

LifeTxnStatus record_status_or_aborted(const LifeProcessRecord *record) {
  return record != NULL ? record->status : LifeTxnStatus::Aborted;
}

size_t record_history_size(const LifeProcessRecord *record) {
  return record != NULL && record->transaction ? record->transaction->history.size()
                                               : 0;
}

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

const LifeHistoryIndices *
Row_life::object_history_indices(const LifeTxnDescriptor &tx) const {
  const LifeTxnDescriptor::TouchedObject *touched = touched_object(tx);
  return touched != NULL ? &touched->history_indices : NULL;
}

const LifeHistoryEntry *
Row_life::object_history_entry(const LifeTxnDescriptor &tx,
                               size_t object_index) const {
  const LifeHistoryIndices *history_indices = object_history_indices(tx);
  if (history_indices != NULL) {
    if (object_index >= history_indices->size())
      return NULL;
    const size_t history_index = (*history_indices)[object_index];
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
                               uint8_t *state, size_t state_size,
                               LifeResponse &response) const {

  response.value.clear();

  if (operation.object != object_id() || operation.field_id >= _field_count ||
      state == NULL || state_size != _tuple_size) {
    return false;
  }

  const uint64_t field_offset = _schema->get_field_index(operation.field_id);
  const uint64_t field_size = _schema->get_field_size(operation.field_id);

  if (operation.value_size == 0 ||
      operation.value_size > LIFE_INLINE_VALUE_CAPACITY ||
      operation.value_size > field_size || field_offset > state_size ||
      operation.value_size > state_size - field_offset)
    return false;

  if (operation.kind == LifeOperationKind::ReadField) {

    if (!operation.argument.empty())
      return false;

    response.value.assign(state + field_offset,
                          state + field_offset + operation.value_size);
    return true;
  }

  if (operation.kind == LifeOperationKind::WriteField) {
    if (operation.argument.size() != operation.value_size)
      return false;

    std::copy(operation.argument.begin(), operation.argument.end(),
              state + field_offset);

    return true;
  }

  return false;
}

bool Row_life::replay_history(const LifeTxnDescriptor &tx,
                              const LifeHistoryIndices *history_indices,
                              uint8_t *state, size_t state_size) const {
  if (history_indices != NULL) {
    for (size_t i = 0; i < history_indices->size(); ++i) {
      const size_t history_index = (*history_indices)[i];
      if (history_index >= tx.history.size())
        return false;
      const LifeHistoryEntry &entry = tx.history[history_index];
      LifeResponse replayed_response;
      if (!apply_operation(entry.operation, state, state_size,
                           replayed_response) ||
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
    if (!apply_operation(it->operation, state, state_size,
                         replayed_response) ||
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

bool Row_life::evaluate_committed_operation(
    const LifeOperation &operation, LifeResponse &response) const {
  response.value.clear();
  if (!validate_committed_operation(operation))
    return false;

  if (operation.kind == LifeOperationKind::WriteField)
    return true;

  const uint64_t field_offset = _schema->get_field_index(operation.field_id);
  const uint8_t *field =
      reinterpret_cast<const uint8_t *>(_row->get_data()) + field_offset;
  response.value.assign(field, field + operation.value_size);
  return true;
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
  assert(_tuple_size <= MAX_TUPLE_SIZE);
  if (_tuple_size > MAX_TUPLE_SIZE)
    return make_result(LifeResultCode::InvalidOperation);
  uint8_t speculative_state[MAX_TUPLE_SIZE];
  std::shared_ptr<LifeTxnDescriptor> updated_transaction =
      std::make_shared<LifeTxnDescriptor>(tx);

  LifeLatchGuard guard(&latch);

  if (operation.object != object_id())
    return make_result(LifeResultCode::InvalidOperation);

  const LifeProcessRecord *context = context_record();
  const LifeProcessRecord *local = process_record(tx.pid);
  const LifeTxnId no_local_tid = min_txn_id();
  const LifeTxnId no_context_tid = max_txn_id();
  const LifeTxnId &local_tid = record_tid_or(local, no_local_tid);
  const LifeTxnId &context_tid = record_tid_or(context, no_context_tid);
  const LifeTxnStatus local_status = record_status_or_aborted(local);
  const LifeTxnStatus context_status = record_status_or_aborted(context);

  const LifeHistoryIndices *tx_object_history_indices =
      object_history_indices(tx);
  size_t tx_object_history_size = 0;
  if (tx_object_history_indices != NULL) {
    tx_object_history_size = tx_object_history_indices->size();
  } else {
    for (std::vector<LifeHistoryEntry>::const_iterator it = tx.history.begin();
         it != tx.history.end(); ++it) {
      if (it->operation.object == object_id())
        ++tx_object_history_size;
    }
  }

  const bool same_process_txn_time = tx.tid.time == local_tid.time;
  const bool local_has_newer_attempt =
      same_process_txn_time && tx.tid.attempt < local_tid.attempt;
  const bool same_process_txn_attempt = tx.tid == local_tid;
  const size_t local_history_size = record_history_size(local);

  const bool must_defer =
      context_status == LifeTxnStatus::Prepared || context_tid < tx.tid ||
      tx.tid.time < local_tid.time || local_has_newer_attempt ||
      (same_process_txn_attempt && (local_status == LifeTxnStatus::Aborted ||
                                    local_status == LifeTxnStatus::Committed ||
                                    tx.history.size() < local_history_size));

  if (must_defer) {

    if (context_status == LifeTxnStatus::Prepared) {

      if (!inline_operation || tx.tid <= inline_operation->transaction->tid) {
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

    if (context_tid < tx.tid) {
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

  LifeResponse response;
  if (tx_object_history_size == 0) {
    if (!evaluate_committed_operation(operation, response))
      return make_result(LifeResultCode::InvalidOperation);
  } else {
    const uint8_t *committed =
        reinterpret_cast<const uint8_t *>(_row->get_data());
    std::memcpy(speculative_state, committed, _tuple_size);

    if (!replay_history(tx, tx_object_history_indices, speculative_state,
                        _tuple_size) ||
        !apply_operation(operation, speculative_state, _tuple_size,
                         response)) {
      return make_result(LifeResultCode::InvalidOperation);
    }
  }

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
  const LifeTxnId no_local_tid = min_txn_id();
  const LifeTxnId &local_tid = record_tid_or(local, no_local_tid);
  const LifeTxnStatus local_status = record_status_or_aborted(local);

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
  const LifeHistoryIndices *cached_history_indices = object_history_indices(tx);
  if (cached_history_indices != NULL) {
    commit(shared, *cached_history_indices);
    return;
  }

  LifeHistoryIndices history_indices;
  for (size_t i = 0; i < tx.history.size(); ++i) {
    if (tx.history[i].operation.object == object_id())
      history_indices.push_back(i);
  }
  commit(shared, history_indices);
}

void Row_life::commit(const LifeTxnDescriptorPtr &tx,
                      const std::vector<size_t> &history_indices) {
  LifeHistoryIndices compact_indices;
  for (std::vector<size_t>::const_iterator it = history_indices.begin();
       it != history_indices.end(); ++it) {
    compact_indices.push_back(*it);
  }
  commit(tx, compact_indices);
}

void Row_life::commit(const LifeTxnDescriptorPtr &tx,
                      const LifeHistoryIndices &history_indices) {
  assert(tx);

  std::unique_ptr<LifeInlineOperation> pending;

  // Bounds checking only examines the immutable frozen descriptor and does
  // not need to extend the row's critical section.
  for (size_t i = 0; i < history_indices.size(); ++i) {
    if (history_indices[i] >= tx->history.size()) {
      assert(false);
      return;
    }
  }

  {
    LifeLatchGuard guard(&latch);
    const LifeProcessRecord *local = process_record(tx->pid);
    const LifeTxnId no_local_tid = min_txn_id();
    const LifeTxnId &local_tid = record_tid_or(local, no_local_tid);
    const LifeTxnStatus local_status = record_status_or_aborted(local);

    bool already_commited = (tx->tid.time < local_tid.time) ||
                            (local_status == LifeTxnStatus::Committed);

    if (already_commited) {
      return;
    }

    for (size_t i = 0; i < history_indices.size(); ++i) {
      if (!validate_committed_operation(
              tx->history[history_indices[i]].operation)) {
        assert(false);
        return;
      }
    }
    for (size_t i = 0; i < history_indices.size(); ++i) {
      const bool applied =
          apply_committed_operation(tx->history[history_indices[i]].operation);
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
    if (active_process.has_value && active_process.value == tx->pid) {
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

    if (!active_process.has_value || active_process.value != tx.pid)
      return;

    active_process.reset();

    pending = std::move(inline_operation);
  }

  if (pending)
    execute(*pending->transaction, pending->operation);
}

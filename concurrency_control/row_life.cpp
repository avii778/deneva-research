#include "row_life.h"
#include "../storage/catalog.h"
#include "../storage/row.h"
#include "../storage/table.h"
#include "life_types.h"
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <unistd.h>

namespace {

LifeTxnId min_txn_id() { return LifeTxnId{0, 0}; }

LifeTxnId max_txn_id() {
  return LifeTxnId{std::numeric_limits<uint64_t>::max(),
                   std::numeric_limits<uint64_t>::max()};
}

const size_t NO_HEAP_INDEX = std::numeric_limits<size_t>::max();

const LifeTxnId &record_tid_or(const LifeProcessRecord *record,
                               const LifeTxnId &fallback) {
  return record != NULL ? record->tid : fallback;
}

LifeTxnStatus record_status_or_aborted(const LifeProcessRecord *record) {
  return record != NULL ? record->status : LifeTxnStatus::Aborted;
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
  active_process.reset();

  processes.reset();
  priority_heap.clear();
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

  ProcessSlots::const_iterator it = processes->find(pid);
  if (it == processes->end() || !it->second.record.has_value)
    return NULL;
  return &it->second.record;
}

LifeProcessRecord &Row_life::mutable_process_record(const LifeProcessId &pid) {
  return mutable_process_slot(pid)->record;
}

Row_life::ProcessSlot *
Row_life::mutable_process_slot(const LifeProcessId &pid) {
  if (!processes) {
    processes.reset(new ProcessSlots());
    processes->reserve(g_node_cnt * g_thread_cnt);
  }
  ProcessSlot &slot = (*processes)[pid];
  slot.pid = pid;
  return &slot;
}

const Row_life::ProcessSlot *Row_life::priority_top() const {
  return priority_heap.empty() ? NULL : priority_heap.front();
}

void Row_life::priority_swap(size_t lhs, size_t rhs) {
  std::swap(priority_heap[lhs], priority_heap[rhs]);
  priority_heap[lhs]->heap_index = lhs;
  priority_heap[rhs]->heap_index = rhs;
}

void Row_life::priority_sift_up(size_t index) {
  while (index != 0) {
    const size_t parent = (index - 1) / 2;
    if (!(priority_heap[index]->record.tid <
          priority_heap[parent]->record.tid))
      break;
    priority_swap(index, parent);
    index = parent;
  }
}

void Row_life::priority_sift_down(size_t index) {
  for (;;) {
    const size_t left = index * 2 + 1;
    if (left >= priority_heap.size())
      return;
    const size_t right = left + 1;
    size_t best = left;
    if (right < priority_heap.size() &&
        priority_heap[right]->record.tid < priority_heap[left]->record.tid)
      best = right;
    if (!(priority_heap[best]->record.tid <
          priority_heap[index]->record.tid))
      return;
    priority_swap(index, best);
    index = best;
  }
}

void Row_life::priority_insert_or_update(ProcessSlot *slot) {
  assert(slot != NULL && slot->record.has_value);
  if (slot->heap_index == NO_HEAP_INDEX) {
    slot->heap_index = priority_heap.size();
    priority_heap.push_back(slot);
    priority_sift_up(slot->heap_index);
    return;
  }
  const size_t index = slot->heap_index;
  priority_sift_up(index);
  priority_sift_down(slot->heap_index);
}

void Row_life::priority_remove(ProcessSlot *slot) {
  if (slot == NULL || slot->heap_index == NO_HEAP_INDEX)
    return;
  const size_t index = slot->heap_index;
  const size_t last = priority_heap.size() - 1;
  if (index != last)
    priority_swap(index, last);
  priority_heap.pop_back();
  slot->heap_index = NO_HEAP_INDEX;
  if (index < priority_heap.size()) {
    priority_sift_up(index);
    priority_sift_down(priority_heap[index]->heap_index);
  }
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
  id.row_id = _row->get_primary_key();
  return id;
}

// Updates state if this is a write, updates response if this is a read
// according to the operation
bool Row_life::apply_operation(const LifeOperation &operation, uint8_t *state,
                               size_t state_size,
                               LifeResponse &response) const {

  response.value.clear();

  Catalog *schema = _row->get_schema();
  const uint64_t field_count = _row->get_field_cnt();

  if (operation.object != object_id() || operation.field_id >= field_count ||
      state == NULL || state_size != _row->get_tuple_size()) {
    std::fprintf(stderr,
                 "LIFE invalid operation: table=%lu key=%lu row=%lu "
                 "field=%u fields=%lu object_match=%d state_size=%lu "
                 "tuple_size=%lu\n",
                 operation.object.table_id, operation.object.primary_key,
                 operation.object.row_id, operation.field_id, field_count,
                 operation.object == object_id(),
                 static_cast<unsigned long>(state_size),
                 static_cast<unsigned long>(_row->get_tuple_size()));
    return false;
  }

  const uint64_t field_offset = schema->get_field_index(operation.field_id);
  const uint64_t field_size = schema->get_field_size(operation.field_id);

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

  if (operation.kind == LifeOperationKind::AddInt64) {
    if (operation.value_size != sizeof(int64_t) ||
        operation.argument.size() != sizeof(int64_t) ||
        field_size < sizeof(int64_t))
      return false;

    uint64_t current;
    int64_t delta;
    std::memcpy(&current, state + field_offset, sizeof(current));
    std::memcpy(&delta, operation.argument.data(), sizeof(delta));
    current += static_cast<uint64_t>(delta);
    std::memcpy(state + field_offset, &current, sizeof(current));
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
    if (!apply_operation(it->operation, state, state_size, replayed_response) ||
        replayed_response != it->response)
      return false;
  }
  return true;
}

bool Row_life::validate_committed_operation(
    const LifeOperation &operation) const {
  Catalog *schema = _row->get_schema();
  const uint64_t tuple_size = _row->get_tuple_size();
  const uint64_t field_count = _row->get_field_cnt();

  if (operation.object != object_id() || operation.field_id >= field_count) {
    return false;
  }

  const uint64_t field_offset = schema->get_field_index(operation.field_id);
  const uint64_t field_size = schema->get_field_size(operation.field_id);
  if (operation.value_size == 0 ||
      operation.value_size > LIFE_INLINE_VALUE_CAPACITY ||
      operation.value_size > field_size || field_offset > tuple_size ||
      operation.value_size > tuple_size - field_offset)
    return false;

  if (operation.kind == LifeOperationKind::ReadField)
    return operation.argument.empty();

  if (operation.kind == LifeOperationKind::WriteField)
    return operation.argument.size() == operation.value_size;
  return operation.kind == LifeOperationKind::AddInt64 &&
         operation.value_size == sizeof(int64_t) &&
         operation.argument.size() == sizeof(int64_t);
}

bool Row_life::evaluate_committed_operation(const LifeOperation &operation,
                                            LifeResponse &response) const {
  response.value.clear();
  if (!validate_committed_operation(operation))
    return false;

  if (operation.kind == LifeOperationKind::WriteField ||
      operation.kind == LifeOperationKind::AddInt64)
    return true;

  const uint64_t field_offset =
      _row->get_schema()->get_field_index(operation.field_id);
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

  const uint64_t field_offset =
      _row->get_schema()->get_field_index(operation.field_id);
  if (operation.kind == LifeOperationKind::WriteField) {
    std::memcpy(_row->get_data() + field_offset, operation.argument.data(),
                operation.value_size);
    return true;
  }

  uint64_t current;
  int64_t delta;
  std::memcpy(&current, _row->get_data() + field_offset, sizeof(current));
  std::memcpy(&delta, operation.argument.data(), sizeof(delta));
  current += static_cast<uint64_t>(delta);
  std::memcpy(_row->get_data() + field_offset, &current, sizeof(current));
  return true;
}

LifeExecuteResult Row_life::execute(const LifeTxnDescriptor &tx,
                                    const LifeOperation &operation) {
  return execute(tx, operation, true);
}

LifeExecuteResult Row_life::execute(const LifeTxnDescriptor &tx,
                                    const LifeOperation &operation,
                                    bool allow_help_wait) {
  const uint64_t tuple_size = _row->get_tuple_size();
  assert(tuple_size <= MAX_TUPLE_SIZE);
  if (tuple_size > MAX_TUPLE_SIZE)
    return make_result(LifeResultCode::InvalidOperation);
  uint8_t speculative_state[MAX_TUPLE_SIZE];

  LifeLatchGuard guard(&latch);

  if (operation.object != object_id())
    return make_result(LifeResultCode::InvalidOperation);

  const LifeProcessRecord *context = context_record();
  const LifeProcessRecord *local = process_record(tx.pid);
#if life_fairness
  const ProcessSlot *heap_top = priority_top();
#endif
  const LifeTxnId no_local_tid = min_txn_id();
  const LifeTxnId no_blocking_tid = max_txn_id();
  const LifeTxnId &local_tid = record_tid_or(local, no_local_tid);
#if life_fairness
  const LifeProcessRecord *blocking =
      heap_top != NULL ? &heap_top->record : NULL;
#else
  const LifeProcessRecord *blocking = context;
#endif
  const LifeTxnId &blocking_tid =
      record_tid_or(blocking, no_blocking_tid);
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
  size_t local_object_history_size = 0;
  if (local != NULL && local->transaction) {
    const LifeHistoryIndices *local_object_history_indices =
        object_history_indices(*local->transaction);
    if (local_object_history_indices != NULL) {
      local_object_history_size = local_object_history_indices->size();
    } else {
      for (std::vector<LifeHistoryEntry>::const_iterator it =
               local->transaction->history.begin();
           it != local->transaction->history.end(); ++it) {
        if (it->operation.object == object_id())
          ++local_object_history_size;
      }
    }
  }

  if (tx.tid.time < local_tid.time) {
    return make_result(LifeResultCode::Committed);
  }

  if (same_process_txn_time && local_status == LifeTxnStatus::Committed) {
    LifeExecuteResult result = make_result(LifeResultCode::Committed);
    if (local != NULL && local->transaction) {
      const LifeTxnDescriptorPtr committed = local->transaction;
      guard.unlock();
      result.transaction = *committed;
    }
    return result;
  }

  const bool must_defer =
      context_status == LifeTxnStatus::Prepared || blocking_tid < tx.tid ||
      tx.tid.time < local_tid.time || local_has_newer_attempt ||
      (same_process_txn_attempt && (local_status == LifeTxnStatus::Aborted ||
                                    local_status == LifeTxnStatus::Committed ||
                                    tx_object_history_size <
                                        local_object_history_size));

  if (must_defer) {

    if (context_status == LifeTxnStatus::Prepared) {

#if LIFE_HELP_WAIT_US > 0
      if (allow_help_wait) {
        guard.unlock();
        usleep(LIFE_HELP_WAIT_US);
        return execute(tx, operation, false);
      }
#else
      (void)allow_help_wait;
#endif

      if (!inline_operation ||
#if life_fairness
          tx.tid < inline_operation->transaction->tid
#else
          tx.tid <= inline_operation->transaction->tid
#endif
      ) {
        LifeInlineOperation pending;
        pending.transaction = std::make_shared<LifeTxnDescriptor>(tx);
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

    if (blocking_tid < tx.tid) {
#if LIFE_HELP_WAIT_US > 0
      if (allow_help_wait) {
        guard.unlock();
        usleep(LIFE_HELP_WAIT_US);
        return execute(tx, operation, false);
      }
#else
      (void)allow_help_wait;
#endif

      LifeExecuteResult result = make_result(LifeResultCode::Help);
      assert(blocking != NULL && blocking->transaction);
      const LifeTxnDescriptorPtr context_transaction = blocking->transaction;
      guard.unlock();
      result.transaction = *context_transaction;
      return result;
    }

    if (local_has_newer_attempt ||
        (same_process_txn_attempt && local_status == LifeTxnStatus::Aborted)) {
      LifeExecuteResult result = make_result(LifeResultCode::Retry);
      result.observed_attempt = local_tid.attempt;
      return result;
    }

    if (tx_object_history_size < local_object_history_size) {
      assert(local != NULL && local->transaction);
      const LifeHistoryEntry *stored_entry =
          object_history_entry(*local->transaction, tx_object_history_size);
      if (stored_entry == NULL || stored_entry->operation != operation) {
        std::fprintf(stderr,
                     "LIFE history mismatch: table=%lu key=%lu row=%lu "
                     "object_history=%lu local_history=%lu\n",
                     operation.object.table_id, operation.object.primary_key,
                     operation.object.row_id,
                     static_cast<unsigned long>(tx_object_history_size),
                     static_cast<unsigned long>(local_object_history_size));
        return make_result(LifeResultCode::InvalidOperation);
      }

      LifeExecuteResult result = make_result(LifeResultCode::Success);
      result.response = stored_entry->response;
      return result;
    }
  }

#if !life_fairness
  // Original policy retains committed descriptors until any later transaction
  // is admitted, then keeps only their terminal tombstones.
  if (processes) {
    for (ProcessSlots::iterator it = processes->begin(); it != processes->end();
         ++it) {
      LifeProcessRecord &record = it->second.record;
      if (record.has_value && record.status == LifeTxnStatus::Committed &&
          record.tid < tx.tid)
        record.transaction.reset();
    }
  }
#endif

#if !life_fairness
  if (active_process.has_value) {
    assert(processes);
    LifeProcessRecord *active =
        const_cast<LifeProcessRecord *>(process_record(active_process.value));
    if (active != NULL)
      active->status = LifeTxnStatus::Aborted;
  }
#endif

  // Validate and evaluate before changing A, P, or PHeap. Invalid input must
  // not leave a phantom heap entry in fairness mode.
  LifeResponse response;
  if (tx_object_history_size == 0) {
    if (!evaluate_committed_operation(operation, response))
      return make_result(LifeResultCode::InvalidOperation);
  } else {
    const uint8_t *committed =
        reinterpret_cast<const uint8_t *>(_row->get_data());
    std::memcpy(speculative_state, committed, tuple_size);

    if (!replay_history(tx, tx_object_history_indices, speculative_state,
                        tuple_size) ||
        !apply_operation(operation, speculative_state, tuple_size, response)) {
      return make_result(LifeResultCode::InvalidOperation);
    }
  }

  LifeHistoryEntry entry;
  entry.operation = operation;
  entry.response = response;

  std::shared_ptr<LifeTxnDescriptor> updated_transaction =
      std::make_shared<LifeTxnDescriptor>(tx);
  LifeProcessRecord updated;
  life_append_history(*updated_transaction, entry);
  updated.transaction = updated_transaction;
  updated.tid = tx.tid;
  updated.status = LifeTxnStatus::Executing;
  updated.has_value = true;

#if life_fairness
  if (active_process.has_value) {
    assert(processes);
    LifeProcessRecord *active =
        const_cast<LifeProcessRecord *>(process_record(active_process.value));
    if (active != NULL)
      active->status = LifeTxnStatus::Aborted;
  }
#endif

  ProcessSlot *slot = mutable_process_slot(tx.pid);
  slot->record = updated;
#if life_fairness
  priority_insert_or_update(slot);
#endif
  active_process.set(tx.pid);

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

#if life_fairness
    // A delayed commit for an older attempt must not publish data or remove
    // the indexed node belonging to the current attempt.
    if (local == NULL || local->tid != tx->tid ||
        local_status == LifeTxnStatus::Aborted) {
      return;
    }
#endif

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

    ProcessSlot *slot = mutable_process_slot(tx->pid);
    LifeProcessRecord &updated = slot->record;
#if life_fairness
    // The terminal tid/status tombstone rejects stale traffic. The descriptor
    // is no longer needed after removal from PHeap and may be shared by other
    // prepared rows, so release this row's ownership immediately.
    updated.transaction.reset();
    updated.tid = tx->tid;
    updated.status = LifeTxnStatus::Committed;
    updated.has_value = true;
    priority_remove(slot);
#else
    LifeTxnDescriptorPtr retained_transaction = tx;
    if (local != NULL && local->tid == tx->tid &&
        local->status == LifeTxnStatus::Prepared && local->transaction)
      retained_transaction = local->transaction;
    updated.transaction = retained_transaction;
    updated.tid = tx->tid;
    updated.status = LifeTxnStatus::Committed;
    updated.has_value = true;
#endif

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

  std::unique_ptr<LifeInlineOperation> pending;

  {
    LifeLatchGuard guard(&latch);

    const LifeProcessRecord *local = process_record(tx.pid);

    if (local != NULL && local->tid == tx.tid &&
        local->status != LifeTxnStatus::Aborted &&
        local->status != LifeTxnStatus::Committed) {
      LifeProcessRecord &record = mutable_process_record(tx.pid);
      record.status = LifeTxnStatus::Aborted;
#if !life_fairness
      record.transaction.reset();
#endif
    }

    if (active_process.has_value && active_process.value == tx.pid) {
      active_process.reset();
      pending = std::move(inline_operation);
    }
  }

  if (pending)
    execute(*pending->transaction, pending->operation);
}

void Row_life::help(const LifeTxnDescriptor &tx) {

  std::unique_ptr<LifeInlineOperation> pending;

  {
    LifeLatchGuard guard(&latch);

    if (!active_process.has_value || active_process.value != tx.pid)
      return;

    active_process.reset();

    pending = std::move(inline_operation);
    inline_operation.reset();
  }

  if (pending)
    execute(*pending->transaction, pending->operation);
}

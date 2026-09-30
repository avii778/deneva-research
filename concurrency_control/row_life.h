#ifndef ROW_LIFE_H
#define ROW_LIFE_H

#include "config.h"
#include "life_types.h"
#include <limits>
#include <pthread.h>
#include <unordered_map>
#include <vector>

class Catalog;
class row_t;
class Message;

class Row_life {
public:
  void init(row_t *row);

  LifeExecuteResult execute(const LifeTxnDescriptor &tx,
                            const LifeOperation &operation);

  // A released waiter bypasses the delay for exactly this execution.
  LifeExecuteResult execute(const LifeTxnDescriptor &tx,
                            const LifeOperation &operation,
                            bool allow_help_wait);

  LifeExecuteResult prepare(const LifeTxnDescriptor &tx);
  LifeExecuteResult prepare(const LifeTxnDescriptorPtr &tx);

  void commit(const LifeTxnDescriptor &tx);
  void commit(const LifeTxnDescriptorPtr &tx,
              const LifeHistoryIndices &history_indices);
  void commit(const LifeTxnDescriptorPtr &tx,
              const std::vector<size_t> &history_indices);

  void rollback(const LifeTxnDescriptor &tx);

  void help(const LifeTxnDescriptor &tx);
#if CC_ALG == LIFE && LIFE_WAIT_QUEUE
  void enqueue_wait(uint64_t generation, uint64_t token, Message *msg);
#endif

private:
  struct ProcessSlot {
    ProcessSlot()
        : pid(), record(), heap_index(std::numeric_limits<size_t>::max()),
          holder_index(std::numeric_limits<size_t>::max()), exclusive(false) {}

    LifeProcessId pid;
    LifeProcessRecord record;
    size_t heap_index;
    size_t holder_index;
    bool exclusive;
#if !life_fairness
    size_t retained_index = std::numeric_limits<size_t>::max();
#endif
#if CC_ALG == LIFE && LIFE_WAIT_QUEUE
    uint64_t prepared_at = 0;
#endif
  };

  typedef std::unordered_map<LifeProcessId, ProcessSlot, LifeProcessIdHash>
      ProcessSlots;

  const LifeTxnDescriptor::TouchedObject *
  touched_object(const LifeTxnDescriptor &tx) const;
  const LifeHistoryIndices *object_history_indices(
      const LifeTxnDescriptor &tx) const;
  const LifeHistoryEntry *object_history_entry(const LifeTxnDescriptor &tx,
                                               size_t object_index) const;

  const LifeProcessRecord *process_record(const LifeProcessId &pid) const;
  const LifeProcessRecord *context_record() const;
  LifeProcessRecord &mutable_process_record(const LifeProcessId &pid);
  ProcessSlot *mutable_process_slot(const LifeProcessId &pid);
  const ProcessSlot *priority_top() const;
  void priority_insert_or_update(ProcessSlot *slot);
  void priority_remove(ProcessSlot *slot);
  void priority_sift_up(size_t index);
  void priority_sift_down(size_t index);
  void priority_swap(size_t lhs, size_t rhs);
  bool holder_conflicts(const ProcessSlot &slot,
                        const LifeTxnDescriptor &tx, bool exclusive) const;
  bool release_holder(const LifeTxnDescriptor &tx);
  void remove_holder(ProcessSlot *slot);
#if !life_fairness
  void forget_retained_descriptor(ProcessSlot *slot);
  void retire_committed_descriptors(const LifeTxnId &before);
#endif
  LifeExecuteResult make_result(LifeResultCode code) const;
  LifeObjectId object_id() const;
  bool apply_operation(const LifeOperation &operation,
                       uint8_t *state, size_t state_size,
                       LifeResponse &response) const;
  bool replay_history(const LifeTxnDescriptor &tx,
                      const LifeHistoryIndices *history_indices,
                      uint8_t *state, size_t state_size) const;
  bool validate_committed_operation(const LifeOperation &operation) const;
  bool evaluate_committed_operation(const LifeOperation &operation,
                                    LifeResponse &response) const;
  bool apply_committed_operation(const LifeOperation &operation);

  pthread_mutex_t latch;
#if CC_ALG == LIFE && LIFE_WAIT_QUEUE
  uint64_t release_generation;
  void notify_release(const LifeProcessId &pid);
#endif
  row_t *_row;
  LifeOptional<LifeProcessId> active_process;
  std::unique_ptr<ProcessSlots> processes;
  std::vector<ProcessSlot *> priority_heap;
  std::vector<ProcessSlot *> holders;
#if !life_fairness
  // Only committed records still owning a descriptor. Map entries themselves
  // remain as terminal tombstones; unordered_map rehash preserves slot pointers.
  std::vector<ProcessSlot *> retained_descriptors;
#endif
  std::unique_ptr<LifeInlineOperation> inline_operation;
};

#endif

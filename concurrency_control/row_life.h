#ifndef ROW_LIFE_H
#define ROW_LIFE_H

#include "life_types.h"
#include <pthread.h>
#include <vector>

class Catalog;
class row_t;

class Row_life {
public:
  void init(row_t *row);

  LifeExecuteResult execute(const LifeTxnDescriptor &tx,
                            const LifeOperation &operation);

  LifeExecuteResult prepare(const LifeTxnDescriptor &tx);
  LifeExecuteResult prepare(const LifeTxnDescriptorPtr &tx);

  void commit(const LifeTxnDescriptor &tx);
  void commit(const LifeTxnDescriptorPtr &tx,
              const LifeHistoryIndices &history_indices);
  void commit(const LifeTxnDescriptorPtr &tx,
              const std::vector<size_t> &history_indices);

  void rollback(const LifeTxnDescriptor &tx);

  void help(const LifeTxnDescriptor &tx);

private:
  typedef std::vector<LifeProcessRecord> ProcessSlots;

  const LifeTxnDescriptor::TouchedObject *
  touched_object(const LifeTxnDescriptor &tx) const;
  const LifeHistoryIndices *object_history_indices(
      const LifeTxnDescriptor &tx) const;
  const LifeHistoryEntry *object_history_entry(const LifeTxnDescriptor &tx,
                                               size_t object_index) const;

  const LifeProcessRecord *process_record(const LifeProcessId &pid) const;
  const LifeProcessRecord *context_record() const;
  LifeProcessRecord &mutable_process_record(const LifeProcessId &pid);
  LifeExecuteResult make_result(LifeResultCode code) const;
  const LifeObjectId &object_id() const;
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
  row_t *_row;
  Catalog *_schema;
  mutable LifeObjectId _object_id;
  mutable bool _primary_key_cached;
  uint64_t _tuple_size;
  uint64_t _field_count;
  LifeOptional<LifeProcessId> active_process;
  std::unique_ptr<ProcessSlots> processes;
  std::unique_ptr<LifeInlineOperation> inline_operation;
};

#endif

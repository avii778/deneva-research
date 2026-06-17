#ifndef ROW_LIFE_H
#define ROW_LIFE_H

#include "life_types.h"
#include <pthread.h>
#include <unordered_map>
#include <vector>

class row_t;

class Row_life {
public:
  void init(row_t *row);

  LifeExecuteResult execute(const LifeTxnDescriptor &tx,
                            const LifeOperation &operation);

  LifeExecuteResult prepare(const LifeTxnDescriptor &tx);

  void commit(const LifeTxnDescriptor &tx);

  void rollback(const LifeTxnDescriptor &tx);

  void help(const LifeTxnDescriptor &tx);

private:
  typedef std::unordered_map<LifeProcessId, LifeProcessRecord,
                             LifeProcessIdHash>
      ProcessMap;

  static bool higher_priority(const LifeTxnId &lhs, const LifeTxnId &rhs);
  static bool priority_less_equal(const LifeTxnId &lhs, const LifeTxnId &rhs);
  static std::vector<LifeHistoryEntry>
  object_history(const LifeTxnDescriptor &tx, const LifeObjectId &object);

  LifeProcessRecord process_record(const LifeProcessId &pid) const;
  LifeProcessRecord context_record() const;
  LifeExecuteResult make_result(LifeResultCode code) const;
  LifeObjectId object_id() const;
  bool apply_operation(const LifeOperation &operation,
                       std::vector<uint8_t> &state,
                       LifeResponse &response) const;
  bool replay_history(const std::vector<LifeHistoryEntry> &history,
                      std::vector<uint8_t> &state) const;

  bool pid_equals(const LifeProcessId &pid1, const LifeProcessId &pid2);
  pthread_mutex_t latch;
  row_t *_row;
  std::vector<uint8_t> committed_state;
  LifeOptional<LifeProcessId> active_process;
  ProcessMap processes;
  LifeOptional<LifeInlineOperation> inline_operation;
};

#endif

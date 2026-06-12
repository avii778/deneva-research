#ifndef ROW_LIFE_H
#define ROW_LIFE_H

#include "../storage/row.h"
#include "../system/txn.h"
#include "life_types.h"
#include <vector>

enum States { EXECUTING, ABORTED };
enum RESTR { HELP, COMMITTED, RETRY, SUCCESS };

struct RE {

  TxnManager *txn;
  RESTR stat;

} typedef RE;

class Row_life {

public:
  void init(row_t *row);
  LifeExecuteResult execute(TxnManager *txn);
  LifeResponse prepare(TxnManager *txn);
  void commit(TxnManager *txn);
  void rollback(const LifeTxnDescriptor &tx);

private:
  pthread_mutex_t *latch;
  // placeholder for now
  row_t *_row;
  Optional<LifeProcessId> active_process;
  std::unordered_map<LifeProcessId, LifeProcssRecord> processes;
  uint64_t proposed_action;
};

#endif

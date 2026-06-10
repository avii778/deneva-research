#ifndef ROW_LIFE_H
#define ROW_LIFE_H

#include "../storage/row.h"
#include "../system/txn.h"
#include <vector>

enum States { EXECUTING, ABORTED };
enum RESTR { HELP, COMMITTED, RETRY, SUCCESS };

struct RE {

  TxnManager *txn;
  RESTR stat;

} typedef RE;

class Row_state {

public:
  void init(row_t *row);
  RE execute(TxnManager *txn);
  RE prepare(TxnManager *txn);
  void commit(TxnManager *txn);

private:
  pthread_mutex_t *latch;
  // placeholder for now
  row_t *_row;
  uint64_t proposed_action;
  TxnManager *inLine;

  // the whole pid to latest transaction thing
  vector<pair<TxnManager *, States>> processes;
};

#endif

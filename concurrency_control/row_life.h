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
  RC execute(TxnManager *txn);
  RE prepare(TxnManager *txn);
  void commit(TxnManager *txn);

private:
  // placeholder for now
  row_t *_row;
  int proposed_action;
  TxnManager *inLine;
  vector<pair<TxnManager *, States>> processes;
};

#endif

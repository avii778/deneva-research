#include "row_life.h"
#include "../storage/row.h"
#include "../system/txn.h"
#include "helper.h"
#include "manager.h"
#include "mem_alloc.h"
#include <cstdint>

void Row_state::init(row_t *row) {

  _row = row;
  proposed_action = 0;
  inLine = nullptr;
  latch = new pthread_mutex_t;
  pthread_mutex_init(latch, NULL);
  // figure out how to get num processes later
  processes = vector<pair<TxnManager *, States>>();
  processes.reserve(50);
}

RE Row_state::execute(TxnManager *txn) {

  pthread_mutex_lock(latch);

  if (!proposed_action) {

    uint64_t pid = txn->txn->get_thd_id();

    if (pid > process.size()) {
      processes.resize(pid);
      processes[pid] = {txn, EXECUTING};
      return (SUCCESS, nullptr);
    } else {
    }
  }
}

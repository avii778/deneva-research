// Adapted from dbiir/HDCC, mixed branch, commit 82514f9 (Apache-2.0).
// See docs/hdcc-integration.md for provenance and integration changes.
#include "txn.h"
#include "row.h"
#include "row_aria.h"

#if CC_ALG == ARIA

RC TxnManager::reserve() {
    const uint64_t id = get_txn_id();
    for (uint64_t i = 0; i < txn->row_cnt; ++i) {
        Access *access = txn->accesses[i];
        auto &reservation = access->type == WR
            ? access->orig_row->manager->write_reseration
            : access->orig_row->manager->read_reseration;
        uint64_t previous = reservation.load();
        while (previous > id && !reservation.compare_exchange_weak(previous, id)) {}
    }
    return RCOK;
}

RC TxnManager::check() {
    RC rc = RCOK;
    uint64_t txn_id = get_txn_id();
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t * row = txn->accesses[i]->orig_row;
        if (txn->accesses[i]->type == WR) {
            if (row->manager->write_reseration < txn_id) {
                txn->rc = Abort;
                rc = Abort;
                break;
            }
            if (row->manager->read_reseration < txn_id) {
                war = true;
            }
        } else {
            if (row->manager->write_reseration < txn_id) {
                raw = true;
            }
        }
        if (war && raw) {
            txn->rc = Abort;
            rc = Abort;
            break;
        }
    }
    return rc;
}

RC TxnManager::finish(RC rc) {
    uint64_t txn_id = get_txn_id();
    // If the txn is commited, then write the value into the original row
    if (rc == RCOK || rc == Commit) {
        for (uint64_t i = 0; i < txn->row_cnt; i++) {
            if (txn->accesses[i]->type == WR) {
                row_t * row = txn->accesses[i]->orig_row;
                row->copy(txn->accesses[i]->data);
            }
        }
    }
    //If the row is reserved by this txn, then reset it to UINT64_MAX.
    for (uint64_t i = 0; i < txn->row_cnt; i++) {
        row_t * row = txn->accesses[i]->orig_row;
        if (txn->accesses[i]->type == WR) {
            uint64_t expected = txn_id;
            row->manager->write_reseration.compare_exchange_strong(expected, UINT64_MAX);
        } else {
            uint64_t expected = txn_id;
            row->manager->read_reseration.compare_exchange_strong(expected, UINT64_MAX);
        }
    }
    return rc;
}

#endif

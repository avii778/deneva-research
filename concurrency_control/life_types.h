#ifndef LIFE_TYPES_H
#define LIFE_TYPES_H
#include "row_life.h"
#include <cstdint>
#include <vector>

struct LifeProcessId {

  uint32_t node_id;
  uint32_t worker_id;

} typedef LifeProcessId;

struct LifeTxnId {

  uint64_t time;
  uint64_t attempt;
} typedef LifeTxnId;

struct LifeObjectId {

  uint64_t table_id;
  uint64_t partition_id;
  uint64_t pkey;
} typedef LifeObjectId;

enum class LifeOperationKind { ReadField, WriteField };
enum class Results { Finalize, Committed, Help, SUCCESS } struct LifeOperation {
  LifeObjectId object;
  LifeOperationKind kind;
  std::vector<uint8_t> arguement;
} typedef LifeOperation;

// for network i/o life response should be some series of bytes
struct LifeResponse {
  std::vector<uint8_t> value;
} typedef LifeResponse;

struct LifeHistoryEntry {
  LifeOperation operation;
  LifeResponse response;
} typedef LifeHistoryEntry;

struct LifeExecuteResult {
  LifeResponse response;
  Results result;
}

struct LifeTxnDescriptor {
  LifeProcessId pid;
  LifeTxnId txn;
} typedef LifeTxnDescriptor;

#endif

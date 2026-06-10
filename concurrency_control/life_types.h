#ifndef LIFE_TYPES_H
#define LIFE_TYPES_H
#include <cstdint>
#include <vector>

struct LifeProcessId {

  uint32_t node_id;
  uint32_t worker_id;

} typedef LifeProcessId;

struct LifeTxnId {

  uint64_t tme;
  uint64_t attempt;
} typedef LifeTxnId;

struct LifeObjectId {

  uint64_t table_id;
  uint64_t partition_id;
  uint64_t pkey;
} typedef LifeObjectId;

enum class LifeOperationKind { ReadField, WriteField };

struct LifeOperation {
  LifeObjectId object;
  LifeOperationKind kind;
  std::vector<uint8_t> arguement;
} typedef LifeOperation;

struct LifeResponse {
  std::vector<uint8_t> value;
} typedef LifeResponse;

struct LifeHistoryEntry {
  LifeOperation operation;
  LifeResponse response;
} typedef LifeHistoryEntry;

#endif

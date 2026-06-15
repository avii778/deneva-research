#ifndef LIFE_TYPES_H
#define LIFE_TYPES_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

struct LifeProcessId {
  uint32_t node_id;
  uint32_t worker_id;
};

inline bool operator==(const LifeProcessId &lhs, const LifeProcessId &rhs) {
  return lhs.node_id == rhs.node_id && lhs.worker_id == rhs.worker_id;
}

inline bool operator!=(const LifeProcessId &lhs, const LifeProcessId &rhs) {
  return !(lhs == rhs);
}

struct LifeProcessIdHash {

  std::size_t operator()(const LifeProcessId &pid) const {
    const std::size_t node_hash = std::hash<uint32_t>()(pid.node_id);
    const std::size_t worker_hash = std::hash<uint32_t>()(pid.worker_id);
    return node_hash ^
           (worker_hash + 0x9e3779b9U + (node_hash << 6) + (node_hash >> 2));
  }
};

struct LifeTxnId {
  uint64_t time;
  uint64_t attempt;
};

inline bool operator==(const LifeTxnId &lhs, const LifeTxnId &rhs) {
  return lhs.time == rhs.time && lhs.attempt == rhs.attempt;
}

inline bool operator!=(const LifeTxnId &lhs, const LifeTxnId &rhs) {
  return !(lhs == rhs);
}

inline bool operator<(const LifeTxnId &lhs, const LifeTxnId &rhs) {
  return lhs.time < rhs.time ||
         (lhs.time == rhs.time && lhs.attempt < rhs.attempt);
}

inline bool operator<=(const LifeTxnId &lhs, const LifeTxnId &rhs) {
  return lhs < rhs || lhs == rhs;
}

struct LifeObjectId {
  uint64_t table_id;
  uint64_t partition_id;
  uint64_t primary_key;
};

inline bool operator==(const LifeObjectId &lhs, const LifeObjectId &rhs) {
  return lhs.table_id == rhs.table_id && lhs.partition_id == rhs.partition_id &&
         lhs.primary_key == rhs.primary_key;
}

inline bool operator!=(const LifeObjectId &lhs, const LifeObjectId &rhs) {
  return !(lhs == rhs);
}

enum class LifeOperationKind { ReadField, WriteField };

struct LifeOperation {
  LifeObjectId object;
  LifeOperationKind kind;
  uint32_t field_id;
  std::vector<uint8_t> argument;
};

inline bool operator==(const LifeOperation &lhs, const LifeOperation &rhs) {
  return lhs.object == rhs.object && lhs.kind == rhs.kind &&
         lhs.field_id == rhs.field_id && lhs.argument == rhs.argument;
}

inline bool operator!=(const LifeOperation &lhs, const LifeOperation &rhs) {
  return !(lhs == rhs);
}

struct LifeResponse {
  std::vector<uint8_t> value;
};

inline bool operator==(const LifeResponse &lhs, const LifeResponse &rhs) {
  return lhs.value == rhs.value;
}

inline bool operator!=(const LifeResponse &lhs, const LifeResponse &rhs) {
  return !(lhs == rhs);
}

struct LifeHistoryEntry {
  LifeOperation operation;
  LifeResponse response;
};

inline bool operator==(const LifeHistoryEntry &lhs,
                       const LifeHistoryEntry &rhs) {
  return lhs.operation == rhs.operation && lhs.response == rhs.response;
}

inline bool operator!=(const LifeHistoryEntry &lhs,
                       const LifeHistoryEntry &rhs) {
  return !(lhs == rhs);
}

enum class LifeTxnStatus { Executing, Prepared, Committed, Aborted };

enum class LifeYcsbRequestKind { Read, Write, Scan };

struct LifeYcsbRequest {
  LifeYcsbRequestKind kind;
  uint64_t key;
  uint8_t value;
};

inline bool operator==(const LifeYcsbRequest &lhs, const LifeYcsbRequest &rhs) {
  return lhs.kind == rhs.kind && lhs.key == rhs.key && lhs.value == rhs.value;
}

inline bool operator!=(const LifeYcsbRequest &lhs, const LifeYcsbRequest &rhs) {
  return !(lhs == rhs);
}

struct LifeYcsbSnapshot {
  std::vector<LifeYcsbRequest> requests;
  uint32_t state;
  uint64_t next_record_id;
};

inline bool operator==(const LifeYcsbSnapshot &lhs,
                       const LifeYcsbSnapshot &rhs) {
  return lhs.requests == rhs.requests && lhs.state == rhs.state &&
         lhs.next_record_id == rhs.next_record_id;
}

inline bool operator!=(const LifeYcsbSnapshot &lhs,
                       const LifeYcsbSnapshot &rhs) {
  return !(lhs == rhs);
}

struct LifeTxnDescriptor {
  LifeProcessId pid;
  LifeTxnId tid;
  std::vector<LifeHistoryEntry> history;
  LifeYcsbSnapshot ycsb;
};

inline bool operator==(const LifeTxnDescriptor &lhs,
                       const LifeTxnDescriptor &rhs) {
  return lhs.pid == rhs.pid && lhs.tid == rhs.tid &&
         lhs.history == rhs.history && lhs.ycsb == rhs.ycsb;
}

inline bool operator!=(const LifeTxnDescriptor &lhs,
                       const LifeTxnDescriptor &rhs) {
  return !(lhs == rhs);
}

struct LifeProcessRecord {
  LifeTxnDescriptor transaction;
  LifeTxnStatus status;
};

struct LifeInlineOperation {
  LifeTxnDescriptor transaction;
  LifeOperation operation;
};

enum class LifeResultCode {
  Success,
  Finalize,
  Committed,
  Help,
  Retry,
  InvalidOperation
};

struct LifeExecuteResult {
  LifeResultCode code;
  LifeResponse response;
  LifeTxnDescriptor transaction;
  uint64_t observed_attempt;
};

template <typename T> struct LifeOptional {
  LifeOptional() : has_value(false), value() {}

  void reset() {
    has_value = false;
    value = T();
  }

  void set(const T &new_value) {
    value = new_value;
    has_value = true;
  }

  bool has_value;
  T value;
};

#endif

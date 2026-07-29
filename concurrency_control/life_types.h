#ifndef LIFE_TYPES_H
#define LIFE_TYPES_H

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

class row_t;
class Row_life;

struct LifeProcessId {
  LifeProcessId() : node_id(0), worker_id(0) {}
  uint32_t node_id;
  // Stable process/transaction identity component assigned at admission.
  // It is not the worker currently executing a migrated continuation.
  uint64_t worker_id;
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
    const std::size_t worker_hash = std::hash<uint64_t>()(pid.worker_id);
    return node_hash ^
           (worker_hash + 0x9e3779b9U + (node_hash << 6) + (node_hash >> 2));
  }
};

struct LifeTxnId {
  LifeTxnId() : time(0), attempt(0) {}
  LifeTxnId(uint64_t txn_time, uint64_t txn_attempt)
      : time(txn_time), attempt(txn_attempt) {}
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
         (lhs.time == rhs.time && lhs.attempt > rhs.attempt);
}

inline bool operator<=(const LifeTxnId &lhs, const LifeTxnId &rhs) {
  return lhs < rhs || lhs == rhs;
}

struct LifeObjectId {
  LifeObjectId() : table_id(0), partition_id(0), primary_key(0), row_id(0) {}

  uint64_t table_id;
  uint64_t partition_id;
  uint64_t primary_key;
  // A table-local physical identity. This disambiguates rows behind a
  // non-unique index (PPS USES/SUPPLIES) without changing their lookup key.
  uint64_t row_id;
};

inline bool operator==(const LifeObjectId &lhs, const LifeObjectId &rhs) {
  return lhs.table_id == rhs.table_id && lhs.partition_id == rhs.partition_id &&
         lhs.primary_key == rhs.primary_key && lhs.row_id == rhs.row_id;
}

inline bool operator!=(const LifeObjectId &lhs, const LifeObjectId &rhs) {
  return !(lhs == rhs);
}

enum class LifeOperationKind { ReadField, WriteField, AddInt64 };

static const size_t LIFE_INLINE_VALUE_CAPACITY = sizeof(uint64_t);

class LifeBytes {
public:
  LifeBytes() : size_(0) {}

  bool empty() const { return size_ == 0; }
  size_t size() const { return size_; }
  void clear() { size_ = 0; }

  uint8_t *begin() { return bytes_; }
  const uint8_t *begin() const { return bytes_; }
  uint8_t *end() { return bytes_ + size_; }
  const uint8_t *end() const { return bytes_ + size_; }
  uint8_t *data() { return bytes_; }
  const uint8_t *data() const { return bytes_; }

  template <typename Iterator> void assign(Iterator first, Iterator last) {
    size_ = 0;
    while (first != last) {
      if (size_ == LIFE_INLINE_VALUE_CAPACITY) {
        assert(false);
        size_ = 0;
        return;
      }
      bytes_[size_++] = static_cast<uint8_t>(*first++);
    }
  }

  void assign(size_t count, uint8_t value) {
    if (count > LIFE_INLINE_VALUE_CAPACITY) {
      assert(false);
      size_ = 0;
      return;
    }
    size_ = count;
    for (size_t i = 0; i < count; ++i)
      bytes_[i] = value;
  }

  friend bool operator==(const LifeBytes &lhs, const LifeBytes &rhs) {
    if (lhs.size_ != rhs.size_)
      return false;
    for (size_t i = 0; i < lhs.size_; ++i) {
      if (lhs.bytes_[i] != rhs.bytes_[i])
        return false;
    }
    return true;
  }

  friend bool operator!=(const LifeBytes &lhs, const LifeBytes &rhs) {
    return !(lhs == rhs);
  }

private:
  uint8_t bytes_[LIFE_INLINE_VALUE_CAPACITY];
  uint8_t size_;
};

struct LifeOperation {
  LifeOperation()
      : object(), manager(NULL), kind(LifeOperationKind::ReadField),
        field_id(0), value_size(0), argument() {}

  LifeObjectId object;
  Row_life *manager;
  LifeOperationKind kind;
  uint32_t field_id;
  uint8_t value_size;
  LifeBytes argument;
};

inline bool operator==(const LifeOperation &lhs, const LifeOperation &rhs) {
  return lhs.object == rhs.object && lhs.kind == rhs.kind &&
         lhs.field_id == rhs.field_id && lhs.value_size == rhs.value_size &&
         lhs.argument == rhs.argument;
}

inline bool operator!=(const LifeOperation &lhs, const LifeOperation &rhs) {
  return !(lhs == rhs);
}

struct LifeResponse {
  LifeBytes value;
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
  row_t *row;
};

inline bool operator==(const LifeYcsbRequest &lhs, const LifeYcsbRequest &rhs) {
  return lhs.kind == rhs.kind && lhs.key == rhs.key && lhs.value == rhs.value;
}

inline bool operator!=(const LifeYcsbRequest &lhs, const LifeYcsbRequest &rhs) {
  return !(lhs == rhs);
}

struct LifeYcsbSnapshot {
  LifeYcsbSnapshot() : state(0), next_record_id(0) {}
  std::vector<LifeYcsbRequest> requests;
  uint32_t state;
  uint64_t next_record_id;
};

enum class LifeWorkloadKind { Ycsb, Pps };

// Serialized continuation state for a PPS transaction. Values are stored as
// integers here so life_types.h remains independent of the PPS headers.
struct LifePpsSnapshot {
  LifePpsSnapshot()
      : txn_type(0), state(0), part_key(0), supplier_key(0), product_key(0),
        scan_index(0), part_index(0), program_index(0) {}

  uint32_t txn_type;
  uint32_t state;
  uint64_t part_key;
  uint64_t supplier_key;
  uint64_t product_key;
  uint64_t scan_index;
  uint64_t part_index;
  // Number of history entries already reflected in state/cursors/part_keys.
  // Row_life may return a descriptor with one additional durable history
  // entry before the workload program has consumed its response.
  uint64_t program_index;
  std::vector<uint64_t> part_keys;
};

inline bool operator==(const LifePpsSnapshot &lhs,
                       const LifePpsSnapshot &rhs) {
  return lhs.txn_type == rhs.txn_type && lhs.state == rhs.state &&
         lhs.part_key == rhs.part_key &&
         lhs.supplier_key == rhs.supplier_key &&
         lhs.product_key == rhs.product_key &&
         lhs.scan_index == rhs.scan_index && lhs.part_index == rhs.part_index &&
         lhs.program_index == rhs.program_index &&
         lhs.part_keys == rhs.part_keys;
}

inline bool operator!=(const LifePpsSnapshot &lhs,
                       const LifePpsSnapshot &rhs) {
  return !(lhs == rhs);
}

inline bool operator==(const LifeYcsbSnapshot &lhs,
                       const LifeYcsbSnapshot &rhs) {
  return lhs.requests == rhs.requests && lhs.state == rhs.state &&
         lhs.next_record_id == rhs.next_record_id;
}

inline bool operator!=(const LifeYcsbSnapshot &lhs,
                       const LifeYcsbSnapshot &rhs) {
  return !(lhs == rhs);
}

class LifeHistoryIndices {
public:
  LifeHistoryIndices() : first_(0), has_first_(false), overflow_() {}

  size_t size() const { return has_first_ ? overflow_.size() + 1 : 0; }

  size_t operator[](size_t index) const {
    assert(index < size());
    return index == 0 ? first_ : overflow_[index - 1];
  }

  void push_back(size_t index) {
    if (!has_first_) {
      first_ = index;
      has_first_ = true;
      return;
    }
    overflow_.push_back(index);
  }

private:
  size_t first_;
  bool has_first_;
  std::vector<size_t> overflow_;
};

struct LifeTxnDescriptor {
  LifeTxnDescriptor() : workload(LifeWorkloadKind::Ycsb) {}

  struct TouchedObject {
    LifeObjectId object;
    Row_life *manager;
    LifeHistoryIndices history_indices;
  };

  LifeProcessId pid;
  LifeTxnId tid;
  LifeWorkloadKind workload;
  std::vector<LifeHistoryEntry> history;
  LifeYcsbSnapshot ycsb;
  LifePpsSnapshot pps;
  std::vector<TouchedObject> touched_objects;
};

inline void life_append_history(LifeTxnDescriptor &tx,
                                const LifeHistoryEntry &entry) {
  const size_t history_index = tx.history.size();
  std::vector<LifeTxnDescriptor::TouchedObject>::iterator touched =
      tx.touched_objects.begin();
  for (; touched != tx.touched_objects.end(); ++touched) {
    if ((entry.operation.manager != NULL &&
         touched->manager == entry.operation.manager) ||
        touched->object == entry.operation.object) {
      break;
    }
  }

  if (touched == tx.touched_objects.end()) {
    LifeTxnDescriptor::TouchedObject added;
    added.object = entry.operation.object;
    added.manager = entry.operation.manager;
    tx.touched_objects.push_back(added);
    touched = tx.touched_objects.end() - 1;
  } else if (touched->manager == NULL) {
    touched->manager = entry.operation.manager;
  }

  touched->history_indices.push_back(history_index);
  tx.history.push_back(entry);
}

// Local protocol records may share an immutable descriptor instead of making
// another deep copy. This is safe only because the shared_ptr owns the
// descriptor independently of any pooled TxnManager that created it.
//
// This pointer is never a distributed reference. A remote LIFE message must
// serialize the descriptor's value state and reconstruct a locally owned
// descriptor before installing it in a row record. The embedded row/manager
// pointers are local caches and must likewise be rebuilt from object IDs.
typedef std::shared_ptr<const LifeTxnDescriptor> LifeTxnDescriptorPtr;

inline bool operator==(const LifeTxnDescriptor &lhs,
                       const LifeTxnDescriptor &rhs) {
  return lhs.pid == rhs.pid && lhs.tid == rhs.tid &&
         lhs.workload == rhs.workload && lhs.history == rhs.history &&
         lhs.ycsb == rhs.ycsb && lhs.pps == rhs.pps;
}

inline bool operator!=(const LifeTxnDescriptor &lhs,
                       const LifeTxnDescriptor &rhs) {
  return !(lhs == rhs);
}

struct LifeProcessRecord {
  LifeProcessRecord()
      : transaction(), tid(), status(LifeTxnStatus::Aborted), has_value(false) {}

  LifeTxnDescriptorPtr transaction;
  LifeTxnId tid;
  LifeTxnStatus status;
  bool has_value;
};

struct LifeInlineOperation {
  LifeTxnDescriptorPtr transaction;
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
  LifeExecuteResult()
      : code(LifeResultCode::InvalidOperation), response(), transaction(),
        observed_attempt(0) {}
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

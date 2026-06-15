#include "ycsb.h"
#include "ycsb_query.h"

LifeYcsbSnapshot make_life_ycsb_snapshot(const YCSBQuery &query,
                                         uint32_t state,
                                         uint64_t next_record_id) {
  LifeYcsbSnapshot snapshot = LifeYcsbSnapshot();
  snapshot.state = state;
  snapshot.next_record_id = next_record_id;
  snapshot.requests.reserve(query.requests.size());

  for (uint64_t i = 0; i < query.requests.size(); ++i) {
    const ycsb_request *request = query.requests[i];
    LifeYcsbRequest request_snapshot = LifeYcsbRequest();

    switch (request->acctype) {
    case RD:
      request_snapshot.kind = LifeYcsbRequestKind::Read;
      break;
    case WR:
      request_snapshot.kind = LifeYcsbRequestKind::Write;
      break;
    case SCAN:
      request_snapshot.kind = LifeYcsbRequestKind::Scan;
      break;
    default:
      assert(false);
    }

    request_snapshot.key = request->key;
    request_snapshot.value = static_cast<uint8_t>(request->value);
    snapshot.requests.push_back(request_snapshot);
  }

  return snapshot;
}

LifeTxnDescriptor YCSBTxnManager::life_descriptor() const {
  LifeTxnDescriptor descriptor = TxnManager::life_descriptor();
  descriptor.ycsb =
      make_life_ycsb_snapshot(*static_cast<const YCSBQuery *>(query),
                              static_cast<uint32_t>(state), next_record_id);
  return descriptor;
}

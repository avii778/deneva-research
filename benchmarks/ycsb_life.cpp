#include "ycsb.h"
#include "ycsb_query.h"
#include <algorithm>

#if CC_ALG == LIFE
namespace {

uint64_t life_destination_rank(const LifeYcsbRequest &request,
                               uint64_t home_node_id) {
  // YCSBWorkload::key_to_part() is key % g_part_cnt. Keep the home group first
  // so a multi-node transaction naturally ends at a remote node and can use
  // prepare-after-execute. Remote groups retain deterministic node order.
  const uint64_t partition_id = request.key % g_part_cnt;
  const uint64_t node_id = GET_NODE_ID(partition_id);
  return node_id == home_node_id ? 0 : node_id + 1;
}

void stable_group_life_requests_by_destination(LifeYcsbSnapshot &snapshot,
                                               uint64_t home_node_id) {
  std::stable_sort(
      snapshot.requests.begin(), snapshot.requests.end(),
      [home_node_id](const LifeYcsbRequest &lhs,
                     const LifeYcsbRequest &rhs) {
        return life_destination_rank(lhs, home_node_id) <
               life_destination_rank(rhs, home_node_id);
      });
}

} // namespace
#endif

LifeYcsbRequest make_life_ycsb_request(const ycsb_request &request) {
  LifeYcsbRequest request_snapshot = LifeYcsbRequest();

  switch (request.acctype) {
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

  request_snapshot.key = request.key;
  request_snapshot.value = static_cast<uint8_t>(request.value);
  return request_snapshot;
}

LifeYcsbSnapshot make_life_ycsb_snapshot(const YCSBQuery &query,
                                         uint32_t state,
                                         uint64_t next_record_id) {
  LifeYcsbSnapshot snapshot = LifeYcsbSnapshot();
  snapshot.state = state;
  snapshot.next_record_id = next_record_id;
  snapshot.requests.reserve(query.requests.size());

  for (uint64_t i = 0; i < query.requests.size(); ++i) {
    snapshot.requests.push_back(make_life_ycsb_request(*query.requests[i]));
  }

#if CC_ALG == LIFE
  // Sort the LIFE-owned snapshot, never the shared YCSB query. The stable sort
  // preserves program order within a destination while collapsing alternating
  // destinations into one execute batch apiece.
  stable_group_life_requests_by_destination(snapshot, g_node_id);
#endif

  return snapshot;
}

LifeTxnDescriptor YCSBTxnManager::life_descriptor() const {
  LifeTxnDescriptor descriptor = TxnManager::life_descriptor();
  descriptor.ycsb =
      make_life_ycsb_snapshot(*static_cast<const YCSBQuery *>(query),
                              static_cast<uint32_t>(state), next_record_id);
  descriptor.history.reserve(descriptor.ycsb.requests.size());
  descriptor.touched_objects.reserve(descriptor.ycsb.requests.size());
#if CC_ALG == LIFE
  for (std::vector<LifeYcsbRequest>::iterator it =
           descriptor.ycsb.requests.begin();
       it != descriptor.ycsb.requests.end(); ++it) {
    const int part_id = _wl->key_to_part(it->key);
    if (GET_NODE_ID(part_id) == g_node_id)
      it->row = lookup_life_row(it->key);
  }
#endif
  return descriptor;
}

#include "../../benchmarks/ycsb.h"
#include "../../benchmarks/ycsb_query.h"
#include "../row_life.h"
#include "../../storage/catalog.h"
#include "../../storage/row.h"
#include "../../storage/table.h"
#include "../../system/mem_alloc.h"

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <memory>

mem_alloc mem_allocator;
bool volatile warmup_done = false;
UInt32 g_node_cnt = 1;
UInt32 g_part_cnt = 1;
UInt32 g_thread_cnt = 1;
UInt32 g_req_per_query = 3;
#if CC_ALG == LIFE
UInt32 g_node_id = 0;
#endif

void *mem_alloc::alloc(uint64_t size) { return std::malloc(size); }
void *mem_alloc::align_alloc(uint64_t size) { return std::malloc(size); }
void *mem_alloc::realloc(void *ptr, uint64_t size) {
  return std::realloc(ptr, size);
}
void mem_alloc::free(void *ptr, uint64_t) { std::free(ptr); }

void BaseQuery::init() {}
void YCSBQuery::init() {}
void YCSBQuery::print() {}
bool YCSBQuery::readonly() { return true; }
LifeTxnDescriptor TxnManager::life_descriptor() const {
  return LifeTxnDescriptor();
}

void table_t::init(Catalog *host_schema) {
  schema = host_schema;
  table_name = host_schema->table_name;
  table_id = host_schema->table_id;
  cur_tab_size = new uint64_t(0);
}

RC row_t::init(table_t *host_table, uint64_t part_id, uint64_t row_id) {
  table = host_table;
  _part_id = part_id;
  _row_id = row_id;
  tuple_size = host_table->get_schema()->get_tuple_size();
  data = static_cast<char *>(std::malloc(tuple_size));
  std::memset(data, 0, tuple_size);
  return RCOK;
}

table_t *row_t::get_table() { return table; }
Catalog *row_t::get_schema() { return table->get_schema(); }
uint64_t row_t::get_field_cnt() { return get_schema()->get_field_cnt(); }
uint64_t row_t::get_tuple_size() { return get_schema()->get_tuple_size(); }
char *row_t::get_data() { return data; }

namespace {

LifeTxnDescriptor descriptor(uint32_t worker_id, uint64_t time,
                             uint64_t attempt, uint32_t state,
                             uint64_t next_record_id) {
  LifeTxnDescriptor tx = LifeTxnDescriptor();
  tx.pid.node_id = 0;
  tx.pid.worker_id = worker_id;
  tx.tid.time = time;
  tx.tid.attempt = attempt;
  tx.ycsb.state = state;
  tx.ycsb.next_record_id = next_record_id;

  LifeYcsbRequest request = LifeYcsbRequest();
  request.kind = LifeYcsbRequestKind::Read;
  request.key = 7;
  request.value = 11;
  tx.ycsb.requests.push_back(request);
  return tx;
}

LifeOperation operation(row_t &row, LifeOperationKind kind, uint64_t value) {
  LifeOperation op = LifeOperation();
  op.object.table_id = row.table->get_table_id();
  op.object.partition_id = row.get_part_id();
  op.object.primary_key = row.get_primary_key();
  op.kind = kind;
  op.field_id = 0;
  op.value_size = sizeof(uint64_t);

  if (kind == LifeOperationKind::WriteField) {
    const uint8_t *bytes = reinterpret_cast<const uint8_t *>(&value);
    op.argument.assign(bytes, bytes + sizeof(value));
  }
  return op;
}

void test_snapshot_is_owned() {
  YCSBQuery query;
  query.requests.init(3);

  ycsb_request read;
  read.acctype = RD;
  read.key = 10;
  read.value = 1;
  query.requests.add(&read);

  ycsb_request write;
  write.acctype = WR;
  write.key = 20;
  write.value = 2;
  query.requests.add(&write);

  ycsb_request scan;
  scan.acctype = SCAN;
  scan.key = 30;
  scan.value = 3;
  query.requests.add(&scan);

  const LifeYcsbSnapshot snapshot = make_life_ycsb_snapshot(query, YCSB_1, 2);

  assert(snapshot.state == YCSB_1);
  assert(snapshot.next_record_id == 2);
  assert(snapshot.requests.size() == 3);
  assert(snapshot.requests[0].kind == LifeYcsbRequestKind::Read);
  assert(snapshot.requests[1].kind == LifeYcsbRequestKind::Write);
  assert(snapshot.requests[2].kind == LifeYcsbRequestKind::Scan);
  assert(snapshot.requests[0].row == NULL);

  read.key = 999;
  write.value = 99;
  query.requests.clear();

  assert(snapshot.requests[0].key == 10);
  assert(snapshot.requests[1].value == 2);
  assert(snapshot.requests.size() == 3);

  query.requests.release();
}

#if CC_ALG == LIFE
void test_snapshot_stably_groups_destinations_home_first() {
  g_node_cnt = 3;
  g_part_cnt = 3;
  g_node_id = 1;

  YCSBQuery query;
  query.requests.init(6);
  ycsb_request requests[6];
  const uint64_t keys[6] = {0, 1, 5, 4, 3, 2};
  for (uint64_t i = 0; i < 6; ++i) {
    requests[i].acctype = RD;
    requests[i].key = keys[i];
    requests[i].value = static_cast<char>(i);
    query.requests.add(&requests[i]);
  }

  const LifeYcsbSnapshot snapshot = make_life_ycsb_snapshot(query, YCSB_0, 0);
  const uint64_t expected[6] = {1, 4, 0, 3, 5, 2};
  const uint8_t expected_values[6] = {1, 3, 0, 4, 2, 5};
  for (uint64_t i = 0; i < 6; ++i) {
    assert(snapshot.requests[i].key == expected[i]);
    assert(snapshot.requests[i].value == expected_values[i]);
  }

  query.requests.release();
  g_node_id = 0;
  g_node_cnt = 1;
  g_part_cnt = 1;
}
#endif

void test_execute_stale_history_refresh_and_help() {
  Catalog schema;
  schema.table_name = "MAIN_TABLE";
  schema.table_id = 3;
  schema.field_cnt = 1;
  schema.tuple_size = sizeof(uint64_t);
  schema._columns = new Column[1];
  schema._columns[0].id = 0;
  schema._columns[0].size = sizeof(uint64_t);
  schema._columns[0].index = 0;

  table_t table;
  table.init(&schema);

  row_t row;
  row.init(&table, 4, 0);
  row.set_primary_key(7);
  const uint64_t initial_value = 42;
  std::memcpy(row.data, &initial_value, sizeof(initial_value));

  Row_life life_row;
  life_row.init(&row);

  LifeTxnDescriptor owner = descriptor(1, 10, 1, YCSB_0, 0);
  const LifeOperation read = operation(row, LifeOperationKind::ReadField, 0);
  const LifeExecuteResult first = life_row.execute(owner, read);
  assert(first.code == LifeResultCode::Success);

  LifeHistoryEntry read_entry;
  read_entry.operation = read;
  read_entry.response = first.response;
  LifeTxnDescriptor stored = owner;
  stored.history.push_back(read_entry);

  owner.ycsb.requests[0].key = 999;
  const LifeTxnDescriptor contender = descriptor(2, 20, 1, YCSB_0, 0);
  const LifeExecuteResult help = life_row.execute(contender, read);
  assert(help.code == LifeResultCode::Help);
  assert(help.transaction == stored);

  const LifeTxnDescriptor stale = descriptor(1, 10, 1, YCSB_0, 0);
  const LifeExecuteResult replay = life_row.execute(stale, read);
  assert(replay.code == LifeResultCode::Success);
  assert(replay.response == first.response);

  LifeTxnDescriptor continued = stored;
  continued.ycsb.state = YCSB_1;
  continued.ycsb.next_record_id = 1;
  const LifeOperation write = operation(row, LifeOperationKind::WriteField, 84);
  const LifeExecuteResult second = life_row.execute(continued, write);
  assert(second.code == LifeResultCode::Success);

  LifeHistoryEntry write_entry;
  write_entry.operation = write;
  write_entry.response = second.response;
  LifeTxnDescriptor refreshed = continued;
  refreshed.history.push_back(write_entry);

  const LifeTxnDescriptor later = descriptor(3, 30, 1, YCSB_0, 0);
  const LifeExecuteResult refreshed_help = life_row.execute(later, read);
  assert(refreshed_help.code == LifeResultCode::Help);
  assert(refreshed_help.transaction == refreshed);

  std::free(row.data);
}

void test_rollback_runs_inline_help() {
  Catalog schema;
  schema.table_name = "MAIN_TABLE";
  schema.table_id = 3;
  schema.field_cnt = 1;
  schema.tuple_size = sizeof(uint64_t);
  schema._columns = new Column[1];
  schema._columns[0].id = 0;
  schema._columns[0].size = sizeof(uint64_t);
  schema._columns[0].index = 0;

  table_t table;
  table.init(&schema);

  row_t row;
  row.init(&table, 4, 0);
  row.set_primary_key(7);
  const uint64_t initial_value = 42;
  std::memcpy(row.data, &initial_value, sizeof(initial_value));

  Row_life life_row;
  life_row.init(&row);

  LifeTxnDescriptor owner = descriptor(1, 10, 1, YCSB_0, 0);
  const LifeOperation owner_read =
      operation(row, LifeOperationKind::ReadField, 0);
  const LifeExecuteResult first = life_row.execute(owner, owner_read);
  assert(first.code == LifeResultCode::Success);

  LifeHistoryEntry owner_entry;
  owner_entry.operation = owner_read;
  owner_entry.response = first.response;
  LifeTxnDescriptor prepared_owner = owner;
  prepared_owner.history.push_back(owner_entry);

  const LifeExecuteResult prepared = life_row.prepare(prepared_owner);
  assert(prepared.code == LifeResultCode::Success);

  LifeTxnDescriptor contender = descriptor(2, 20, 1, YCSB_0, 0);
  const LifeOperation contender_read =
      operation(row, LifeOperationKind::ReadField, 0);
  const LifeExecuteResult finalize =
      life_row.execute(contender, contender_read);
  assert(finalize.code == LifeResultCode::Finalize);
  assert(finalize.transaction == prepared_owner);

  life_row.rollback(prepared_owner);

  LifeHistoryEntry contender_entry;
  contender_entry.operation = contender_read;
  contender_entry.response = first.response;
  LifeTxnDescriptor helped = contender;
  helped.history.push_back(contender_entry);

  const LifeTxnDescriptor later = descriptor(3, 30, 1, YCSB_0, 0);
  const LifeExecuteResult help = life_row.execute(later, contender_read);
  assert(help.code == LifeResultCode::Help);
  assert(help.transaction == helped);

  std::free(row.data);
}

void test_prepare_retry_and_commit_publish() {
  Catalog schema;
  schema.table_name = "MAIN_TABLE";
  schema.table_id = 3;
  schema.field_cnt = 1;
  schema.tuple_size = sizeof(uint64_t);
  schema._columns = new Column[1];
  schema._columns[0].id = 0;
  schema._columns[0].size = sizeof(uint64_t);
  schema._columns[0].index = 0;

  table_t table;
  table.init(&schema);

  row_t row;
  row.init(&table, 4, 0);
  row.set_primary_key(7);
  const uint64_t initial_value = 42;
  std::memcpy(row.data, &initial_value, sizeof(initial_value));

  Row_life life_row;
  life_row.init(&row);

  LifeTxnDescriptor tx = descriptor(1, 10, 1, YCSB_0, 0);
  const LifeOperation write = operation(row, LifeOperationKind::WriteField, 84);
  const LifeExecuteResult written = life_row.execute(tx, write);
  assert(written.code == LifeResultCode::Success);

  LifeHistoryEntry write_entry;
  write_entry.operation = write;
  write_entry.response = written.response;
  LifeTxnDescriptor final_tx = tx;
  final_tx.history.push_back(write_entry);

  const LifeExecuteResult prepared = life_row.prepare(final_tx);
  assert(prepared.code == LifeResultCode::Success);

  life_row.commit(final_tx);

  uint64_t committed_value = 0;
  std::memcpy(&committed_value, row.data, sizeof(committed_value));
  assert(committed_value == 84);

  // A stale helper must not publish the transaction's write a second time.
  const uint64_t later_value = 99;
  std::memcpy(row.data, &later_value, sizeof(later_value));
  life_row.commit(final_tx);
  std::memcpy(&committed_value, row.data, sizeof(committed_value));
  assert(committed_value == later_value);

  // Admitting a higher transaction frees the committed descriptor but keeps
  // its tombstone. The tombstone must still reject a stale helper commit.
  const LifeTxnDescriptor higher = descriptor(2, 20, 1, YCSB_0, 0);
  const LifeOperation higher_read =
      operation(row, LifeOperationKind::ReadField, 0);
  assert(life_row.execute(higher, higher_read).code == LifeResultCode::Success);
  life_row.commit(final_tx);
  std::memcpy(&committed_value, row.data, sizeof(committed_value));
  assert(committed_value == later_value);

  life_row.rollback(final_tx);
  const LifeExecuteResult retry = life_row.prepare(final_tx);
  assert(retry.code == LifeResultCode::Committed);

  std::free(row.data);
}

void test_eight_byte_operation_preserves_rest_of_ycsb_field() {
  Catalog schema;
  schema.table_name = "MAIN_TABLE";
  schema.table_id = 3;
  schema.field_cnt = 1;
  schema.tuple_size = 100;
  schema._columns = new Column[1];
  schema._columns[0].id = 0;
  schema._columns[0].size = 100;
  schema._columns[0].index = 0;

  table_t table;
  table.init(&schema);

  row_t row;
  row.init(&table, 4, 0);
  row.set_primary_key(7);
  std::memset(row.data, 0x5a, row.get_tuple_size());

  Row_life life_row;
  life_row.init(&row);

  LifeTxnDescriptor tx = descriptor(1, 10, 1, YCSB_0, 0);
  const LifeOperation write = operation(row, LifeOperationKind::WriteField, 84);
  const LifeExecuteResult executed = life_row.execute(tx, write);
  assert(executed.code == LifeResultCode::Success);

  LifeHistoryEntry entry;
  entry.operation = write;
  entry.response = executed.response;
  tx.history.push_back(entry);
  assert(life_row.prepare(tx).code == LifeResultCode::Success);
  life_row.commit(tx);

  uint64_t committed_value = 0;
  std::memcpy(&committed_value, row.data, sizeof(committed_value));
  assert(committed_value == 84);
  for (size_t i = sizeof(committed_value); i < row.get_tuple_size(); ++i)
    assert(static_cast<uint8_t>(row.data[i]) == 0x5a);

  const LifeTxnDescriptor read_tx = descriptor(2, 20, 1, YCSB_0, 0);
  const LifeOperation read = operation(row, LifeOperationKind::ReadField, 0);
  const LifeExecuteResult read_result = life_row.execute(read_tx, read);
  assert(read_result.code == LifeResultCode::Success);
  assert(read_result.response.value.size() == sizeof(uint64_t));

  std::free(row.data);
}

void test_shared_prepare_owns_descriptor_and_read_commit_is_stable() {
  Catalog schema;
  schema.table_name = "MAIN_TABLE";
  schema.table_id = 3;
  schema.field_cnt = 1;
  schema.tuple_size = sizeof(uint64_t);
  schema._columns = new Column[1];
  schema._columns[0].id = 0;
  schema._columns[0].size = sizeof(uint64_t);
  schema._columns[0].index = 0;

  table_t table;
  table.init(&schema);

  row_t row;
  row.init(&table, 4, 0);
  row.set_primary_key(7);
  const uint64_t initial_value = 42;
  std::memcpy(row.data, &initial_value, sizeof(initial_value));

  Row_life life_row;
  life_row.init(&row);

  LifeTxnDescriptor tx = descriptor(1, 10, 1, YCSB_0, 0);
  const LifeOperation read = operation(row, LifeOperationKind::ReadField, 0);
  const LifeExecuteResult executed = life_row.execute(tx, read);
  assert(executed.code == LifeResultCode::Success);

  LifeHistoryEntry entry;
  entry.operation = read;
  entry.response = executed.response;
  tx.history.push_back(entry);

  LifeTxnDescriptorPtr frozen = std::make_shared<LifeTxnDescriptor>(tx);
  std::weak_ptr<const LifeTxnDescriptor> retained = frozen;
  assert(life_row.prepare(frozen).code == LifeResultCode::Success);
  assert(frozen.use_count() == 2);
  std::vector<size_t> indices(1, 0);
  life_row.commit(frozen, indices);
  assert(frozen.use_count() == 2);
  frozen.reset();
  assert(!retained.expired());

  uint64_t value_after_read_commit = 0;
  std::memcpy(&value_after_read_commit, row.data,
              sizeof(value_after_read_commit));
  assert(value_after_read_commit == initial_value);

  const LifeTxnDescriptor same_tx = descriptor(1, 10, 1, YCSB_0, 0);
  const LifeExecuteResult committed = life_row.execute(same_tx, read);
  assert(committed.code == LifeResultCode::Committed);

  const LifeTxnDescriptor replacement = descriptor(2, 11, 1, YCSB_0, 0);
  assert(life_row.execute(replacement, read).code == LifeResultCode::Success);
  assert(retained.expired());

  std::free(row.data);
}

void test_prepared_rows_share_one_frozen_descriptor() {
  Catalog schema;
  schema.table_name = "MAIN_TABLE";
  schema.table_id = 3;
  schema.field_cnt = 1;
  schema.tuple_size = sizeof(uint64_t);
  schema._columns = new Column[1];
  schema._columns[0].id = 0;
  schema._columns[0].size = sizeof(uint64_t);
  schema._columns[0].index = 0;

  table_t table;
  table.init(&schema);

  row_t first_row;
  first_row.init(&table, 4, 0);
  first_row.set_primary_key(7);
  row_t second_row;
  second_row.init(&table, 4, 1);
  second_row.set_primary_key(8);
  const uint64_t initial_value = 42;
  std::memcpy(first_row.data, &initial_value, sizeof(initial_value));
  std::memcpy(second_row.data, &initial_value, sizeof(initial_value));

  Row_life first_life_row;
  first_life_row.init(&first_row);
  Row_life second_life_row;
  second_life_row.init(&second_row);

  LifeTxnDescriptor tx = descriptor(1, 10, 1, YCSB_0, 0);
  const LifeOperation first_read =
      operation(first_row, LifeOperationKind::ReadField, 0);
  const LifeExecuteResult first = first_life_row.execute(tx, first_read);
  assert(first.code == LifeResultCode::Success);
  LifeHistoryEntry first_entry;
  first_entry.operation = first_read;
  first_entry.response = first.response;
  tx.history.push_back(first_entry);

  const LifeOperation second_read =
      operation(second_row, LifeOperationKind::ReadField, 0);
  const LifeExecuteResult second = second_life_row.execute(tx, second_read);
  assert(second.code == LifeResultCode::Success);
  LifeHistoryEntry second_entry;
  second_entry.operation = second_read;
  second_entry.response = second.response;
  tx.history.push_back(second_entry);

  LifeTxnDescriptorPtr frozen = std::make_shared<LifeTxnDescriptor>(tx);
  std::weak_ptr<const LifeTxnDescriptor> retained = frozen;
  assert(first_life_row.prepare(frozen).code == LifeResultCode::Success);
  assert(second_life_row.prepare(frozen).code == LifeResultCode::Success);
  assert(frozen.use_count() == 3);

  std::shared_ptr<LifeTxnDescriptor> filtered_first =
      std::make_shared<LifeTxnDescriptor>();
  filtered_first->pid = tx.pid;
  filtered_first->tid = tx.tid;
  filtered_first->history.push_back(tx.history[0]);
  first_life_row.commit(filtered_first, std::vector<size_t>(1, 0));
  const LifeExecuteResult committed_first =
      first_life_row.execute(tx, first_read);
  assert(committed_first.code == LifeResultCode::Committed);
  assert(committed_first.transaction.history.size() == tx.history.size());
  second_life_row.commit(frozen, std::vector<size_t>(1, 1));
  assert(frozen.use_count() == 3);
  assert(!retained.expired());

  std::free(first_row.data);
  std::free(second_row.data);
}

void test_local_row_cache_and_incremental_grouping() {
  Catalog schema;
  schema.table_name = "MAIN_TABLE";
  schema.table_id = 3;
  schema.field_cnt = 1;
  schema.tuple_size = sizeof(uint64_t);
  schema._columns = new Column[1];
  schema._columns[0].id = 0;
  schema._columns[0].size = sizeof(uint64_t);
  schema._columns[0].index = 0;

  table_t table;
  table.init(&schema);

  row_t row;
  row.init(&table, 4, 0);
  Row_life life_row;
  life_row.init(&row);

  // Production rows install their manager before the workload sets the key.
  row.set_primary_key(7);
  const uint64_t initial_value = 42;
  std::memcpy(row.data, &initial_value, sizeof(initial_value));

  LifeTxnDescriptor tx = descriptor(1, 10, 1, YCSB_0, 0);
  LifeOperation read = operation(row, LifeOperationKind::ReadField, 0);
  read.manager = &life_row;
  const LifeExecuteResult executed = life_row.execute(tx, read);
  assert(executed.code == LifeResultCode::Success);

  LifeHistoryEntry entry;
  entry.operation = read;
  entry.response = executed.response;
  life_append_history(tx, entry);

  assert(tx.history.size() == 1);
  assert(tx.touched_objects.size() == 1);
  assert(tx.touched_objects[0].manager == &life_row);
  assert(tx.touched_objects[0].object == read.object);
  assert(tx.touched_objects[0].history_indices.size() == 1);
  assert(tx.touched_objects[0].history_indices[0] == 0);

  LifeOperation second_read = read;
  LifeHistoryEntry second_entry;
  second_entry.operation = second_read;
  second_entry.response = executed.response;
  life_append_history(tx, second_entry);
  assert(tx.touched_objects.size() == 1);
  assert(tx.touched_objects[0].history_indices.size() == 2);
  assert(tx.touched_objects[0].history_indices[1] == 1);

  std::free(row.data);
}

} // namespace

int main() {
  static_assert(sizeof(LifeBytes) <= 16,
                "LIFE values must remain inline and compact");
  test_snapshot_is_owned();
#if CC_ALG == LIFE
  test_snapshot_stably_groups_destinations_home_first();
#endif
  test_execute_stale_history_refresh_and_help();
  test_rollback_runs_inline_help();
  test_prepare_retry_and_commit_publish();
  test_eight_byte_operation_preserves_rest_of_ycsb_field();
  test_shared_prepare_owns_descriptor_and_read_commit_is_stable();
  test_prepared_rows_share_one_frozen_descriptor();
  test_local_row_cache_and_incremental_grouping();
  return 0;
}

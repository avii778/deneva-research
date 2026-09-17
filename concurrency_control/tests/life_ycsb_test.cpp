#include "../../benchmarks/ycsb.h"
#include "../../benchmarks/ycsb_query.h"
#include "../row_life.h"
#include "../../storage/catalog.h"
#include "../../storage/row.h"
#include "../../storage/table.h"
#include "../../system/mem_alloc.h"
#if WORKLOAD == TPCC
#include "../../benchmarks/tpcc_const.h"
#endif

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <thread>

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
int YCSBWorkload::key_to_part(uint64_t key) { return key % g_part_cnt; }
row_t *YCSBTxnManager::lookup_life_row(uint64_t) const { return NULL; }

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
  op.object.row_id = row.get_primary_key();
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

void test_ycsb_reconcile_consumes_durable_response() {
  LifeTxnDescriptor tx = descriptor(1, 12, 1, YCSB_0, 0);
  LifeHistoryEntry entry;
  entry.operation.object.table_id = 1;
  entry.operation.object.partition_id = 0;
  entry.operation.object.primary_key = 7;
  entry.operation.object.row_id = 7;
  tx.history.push_back(entry);

  reconcile_life_ycsb_program(tx);

  assert(tx.ycsb.next_record_id == tx.history.size());
  assert(tx.ycsb.state == YCSB_FIN);
}

void test_add_int64_replays_and_commits() {
  Catalog schema;
  schema.table_id = 18;
  schema.table_name = "PPS_TEST";
  schema.field_cnt = 1;
  schema.tuple_size = sizeof(uint64_t);
  schema._columns = new Column[1];
  schema._columns[0].id = 0;
  schema._columns[0].size = sizeof(uint64_t);
  schema._columns[0].index = 0;
  table_t table;
  table.init(&schema);
  row_t row;
  row.init(&table, 0, 9);
  row.set_primary_key(9);
  uint64_t value = 10;
  std::memcpy(row.data, &value, sizeof(value));
  Row_life life_row;
  life_row.init(&row);

  LifeTxnDescriptor tx = descriptor(1, 44, 1, YCSB_0, 0);
  LifeOperation add = operation(row, LifeOperationKind::AddInt64, 0);
  // A replicated row keeps its physical identity while protocol routing can
  // target a different logical partition.
  add.object.routing_partition_id = 7;
  const int64_t delta = -3;
  add.argument.assign(reinterpret_cast<const uint8_t *>(&delta),
                      reinterpret_cast<const uint8_t *>(&delta) +
                          sizeof(delta));
  add.manager = &life_row;
  const LifeExecuteResult result = life_row.execute(tx, add);
  assert(result.code == LifeResultCode::Success);
  LifeHistoryEntry entry;
  entry.operation = add;
  entry.response = result.response;
  life_append_history(tx, entry);
  assert(life_row.prepare(tx).code == LifeResultCode::Success);
  life_row.commit(tx);
  std::memcpy(&value, row.data, sizeof(value));
  assert(value == 7);
  std::free(row.data);
  delete[] schema._columns;
}

#if CC_ALG == LIFE
void test_query_stably_groups_destinations_home_first() {
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

  query.stable_group_requests_by_destination(g_node_id);
  const uint64_t expected[6] = {1, 4, 0, 3, 5, 2};
  const uint8_t expected_values[6] = {1, 3, 0, 4, 2, 5};
  for (uint64_t i = 0; i < 6; ++i) {
    assert(query.requests[i]->key == expected[i]);
    assert(static_cast<uint8_t>(query.requests[i]->value) == expected_values[i]);
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
  const LifeExecuteResult help = life_row.execute(
      contender, operation(row, LifeOperationKind::WriteField, 84));
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

void test_execute_rejects_globally_stale_descriptor() {
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

  const LifeOperation read = operation(row, LifeOperationKind::ReadField, 0);
  LifeTxnDescriptor newest = descriptor(1, 10, 1, YCSB_0, 0);

  // Model progress on two other objects. The row record must retain this
  // complete descriptor even if a delayed continuation has the same amount of
  // history for this row.
  for (uint64_t key = 100; key < 102; ++key) {
    LifeHistoryEntry foreign;
    foreign.operation = read;
    foreign.operation.object.table_id = 99;
    foreign.operation.object.primary_key = key;
    foreign.operation.object.row_id = key;
    newest.history.push_back(foreign);
  }

  const LifeExecuteResult first = life_row.execute(newest, read);
  assert(first.code == LifeResultCode::Success);
  LifeHistoryEntry read_entry;
  read_entry.operation = read;
  read_entry.response = first.response;
  newest.history.push_back(read_entry);

  LifeTxnDescriptor stale = descriptor(1, 10, 1, YCSB_0, 0);
  stale.history.push_back(read_entry);
  const LifeOperation write = operation(row, LifeOperationKind::WriteField, 84);
  const LifeExecuteResult rejected = life_row.execute(stale, write);
  assert(rejected.code == LifeResultCode::InvalidOperation);

  const LifeTxnDescriptor contender = descriptor(2, 20, 1, YCSB_0, 0);
  const LifeExecuteResult help = life_row.execute(contender, write);
  assert(help.code == LifeResultCode::Help);
  assert(help.transaction == newest);

  std::free(row.data);
  delete[] schema._columns;
}

void test_prepared_holder_precedes_committed_duplicate() {
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
  const LifeOperation read = operation(row, LifeOperationKind::ReadField, 0);

  LifeTxnDescriptor committed = descriptor(1, 10, 1, YCSB_0, 0);
  const LifeExecuteResult committed_execute = life_row.execute(committed, read);
  assert(committed_execute.code == LifeResultCode::Success);
  LifeHistoryEntry committed_entry;
  committed_entry.operation = read;
  committed_entry.response = committed_execute.response;
  committed.history.push_back(committed_entry);
  assert(life_row.prepare(committed).code == LifeResultCode::Success);
  life_row.commit(committed);

  LifeTxnDescriptor holder = descriptor(2, 20, 1, YCSB_0, 0);
  const LifeExecuteResult holder_execute = life_row.execute(holder, read);
  assert(holder_execute.code == LifeResultCode::Success);
  LifeHistoryEntry holder_entry;
  holder_entry.operation = read;
  holder_entry.response = holder_execute.response;
  holder.history.push_back(holder_entry);
  assert(life_row.prepare(holder).code == LifeResultCode::Success);

  const LifeExecuteResult duplicate = life_row.execute(committed, read);
#if life_fairness
  assert(duplicate.code == LifeResultCode::Finalize);
  assert(duplicate.transaction == holder);
#else
  assert(duplicate.code == LifeResultCode::Committed);
#endif

  std::free(row.data);
  delete[] schema._columns;
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
      operation(row, LifeOperationKind::WriteField, 84);
  const LifeExecuteResult finalize =
      life_row.execute(contender, contender_read);
  assert(finalize.code == LifeResultCode::Finalize);
  assert(finalize.transaction == prepared_owner);

  life_row.rollback(prepared_owner);

  // PHeap retains the aborted owner. The inline contender cannot pass it;
  // the next caller is directed to retry/help that higher-priority attempt.
  const LifeTxnDescriptor later = descriptor(3, 30, 1, YCSB_0, 0);
  const LifeExecuteResult help = life_row.execute(later, contender_read);
  assert(help.code == LifeResultCode::Help);
#if life_fairness
  assert(help.transaction == prepared_owner);
#else
  LifeHistoryEntry contender_entry;
  contender_entry.operation = contender_read;
  contender_entry.response = LifeResponse();
  LifeTxnDescriptor helped = contender;
  helped.history.push_back(contender_entry);
  assert(help.transaction == helped);
#endif

  std::free(row.data);
}

void test_priority_heap_orders_all_uncommitted_transactions() {
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
  const LifeOperation read = operation(row, LifeOperationKind::ReadField, 0);

  LifeTxnDescriptor lower = descriptor(2, 20, 1, YCSB_0, 0);
  LifeExecuteResult lower_result = life_row.execute(lower, read);
  assert(lower_result.code == LifeResultCode::Success);
  LifeHistoryEntry lower_entry;
  lower_entry.operation = read;
  lower_entry.response = lower_result.response;
  lower.history.push_back(lower_entry);

  // A higher-priority transaction may replace A, but the aborted former
  // holder remains in PHeap and must be helped after the new top is removed.
  LifeTxnDescriptor higher = descriptor(1, 10, 1, YCSB_0, 0);
  LifeExecuteResult higher_result = life_row.execute(higher, read);
  assert(higher_result.code == LifeResultCode::Success);
  LifeHistoryEntry higher_entry;
  higher_entry.operation = read;
  higher_entry.response = higher_result.response;
  higher.history.push_back(higher_entry);
  assert(life_row.prepare(higher).code == LifeResultCode::Success);

  const LifeTxnDescriptor contender = descriptor(3, 30, 1, YCSB_0, 0);
  LifeExecuteResult first_help = life_row.execute(contender, read);
  assert(first_help.code == LifeResultCode::Finalize);
  assert(first_help.transaction == higher);

  life_row.commit(higher);
  LifeExecuteResult second_help = life_row.execute(contender, read);
  assert(second_help.code == LifeResultCode::Help);
  assert(second_help.transaction == lower);

  std::free(row.data);
  delete[] schema._columns;
}

void test_prepared_holder_blocks_higher_priority_contender() {
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
  const LifeOperation read = operation(row, LifeOperationKind::ReadField, 0);

  LifeTxnDescriptor prepared = descriptor(2, 20, 1, YCSB_0, 0);
  LifeExecuteResult executed = life_row.execute(prepared, read);
  LifeHistoryEntry entry;
  entry.operation = read;
  entry.response = executed.response;
  prepared.history.push_back(entry);
  assert(life_row.prepare(prepared).code == LifeResultCode::Success);

  const LifeTxnDescriptor higher = descriptor(1, 10, 1, YCSB_0, 0);
  const LifeExecuteResult result = life_row.execute(higher, read);
  assert(result.code == LifeResultCode::Finalize);
  assert(result.transaction == prepared);

  std::free(row.data);
  delete[] schema._columns;
}

void test_higher_attempt_reuses_heap_node_and_rejects_stale_commit() {
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
  const LifeOperation read = operation(row, LifeOperationKind::ReadField, 0);

  LifeTxnDescriptor first = descriptor(1, 10, 1, YCSB_0, 0);
  LifeExecuteResult first_result = life_row.execute(first, read);
  assert(first_result.code == LifeResultCode::Success);
  LifeHistoryEntry first_entry;
  first_entry.operation = read;
  first_entry.response = first_result.response;
  first.history.push_back(first_entry);
  life_row.rollback(first);

  LifeTxnDescriptor retry = descriptor(1, 10, 2, YCSB_0, 0);
  LifeExecuteResult retry_result = life_row.execute(retry, read);
  assert(retry_result.code == LifeResultCode::Success);
  LifeHistoryEntry retry_entry;
  retry_entry.operation = read;
  retry_entry.response = retry_result.response;
  retry.history.push_back(retry_entry);

  const LifeTxnDescriptor contender = descriptor(2, 20, 1, YCSB_0, 0);
  LifeExecuteResult before_stale_commit = life_row.execute(contender, read);
  assert(before_stale_commit.code == LifeResultCode::Help);
  assert(before_stale_commit.transaction == retry);

  life_row.commit(first);
  LifeExecuteResult after_stale_commit = life_row.execute(contender, read);
  assert(after_stale_commit.code == LifeResultCode::Help);
  assert(after_stale_commit.transaction == retry);

  assert(life_row.prepare(retry).code == LifeResultCode::Success);
  life_row.commit(retry);
  assert(life_row.execute(contender, read).code == LifeResultCode::Success);

  std::free(row.data);
  delete[] schema._columns;
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

void test_recycled_process_name_rejects_stale_transaction() {
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
  LifeOperation write = operation(row, LifeOperationKind::WriteField, 84);

  LifeTxnDescriptor old_tx = descriptor(1, 10, 1, YCSB_0, 0);
  LifeExecuteResult old_result = life_row.execute(old_tx, write);
  assert(old_result.code == LifeResultCode::Success);
  LifeHistoryEntry old_entry;
  old_entry.operation = write;
  old_entry.response = old_result.response;
  old_tx.history.push_back(old_entry);
  assert(life_row.prepare(old_tx).code == LifeResultCode::Success);
  life_row.commit(old_tx);

  // The newer transaction deliberately has the same pid: its virtual name
  // has been recycled after the old transaction terminated.
  LifeTxnDescriptor new_tx = descriptor(1, 20, 1, YCSB_0, 0);
  LifeOperation new_write = operation(row, LifeOperationKind::WriteField, 99);
  LifeExecuteResult new_result = life_row.execute(new_tx, new_write);
  assert(new_result.code == LifeResultCode::Success);

  // A delayed old commit must not publish its write over the new owner.
  life_row.commit(old_tx);
  uint64_t value = 0;
  std::memcpy(&value, row.data, sizeof(value));
  assert(value == 84);

  LifeHistoryEntry new_entry;
  new_entry.operation = new_write;
  new_entry.response = new_result.response;
  new_tx.history.push_back(new_entry);
  assert(life_row.prepare(new_tx).code == LifeResultCode::Success);
  life_row.commit(new_tx);
  std::memcpy(&value, row.data, sizeof(value));
  assert(value == 99);

  assert(life_row.execute(old_tx, write).code == LifeResultCode::Committed);
  life_row.rollback(old_tx);
  std::memcpy(&value, row.data, sizeof(value));
  assert(value == 99);

  std::free(row.data);
  delete[] schema._columns;
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
#if life_fairness
  assert(frozen.use_count() == 1);
  frozen.reset();
  assert(retained.expired());
#else
  assert(frozen.use_count() == 2);
  frozen.reset();
  assert(!retained.expired());
#endif

  uint64_t value_after_read_commit = 0;
  std::memcpy(&value_after_read_commit, row.data,
              sizeof(value_after_read_commit));
  assert(value_after_read_commit == initial_value);

  const LifeTxnDescriptor same_tx = descriptor(1, 10, 1, YCSB_0, 0);
  const LifeExecuteResult committed = life_row.execute(same_tx, read);
  assert(committed.code == LifeResultCode::Committed);
#if life_fairness
  assert(committed.transaction.history.empty());
#endif

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
#if life_fairness
  assert(committed_first.transaction.history.empty());
  assert(frozen.use_count() == 2);
  second_life_row.commit(frozen, std::vector<size_t>(1, 1));
  assert(frozen.use_count() == 1);
  frozen.reset();
  assert(retained.expired());
#else
  assert(committed_first.transaction.history.size() == tx.history.size());
  second_life_row.commit(frozen, std::vector<size_t>(1, 1));
  assert(frozen.use_count() == 3);
  assert(!retained.expired());
#endif

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

#if WORKLOAD == TPCC
void init_uniform_schema(Catalog &schema, uint64_t table_id,
                         uint32_t field_count) {
  schema.table_id = table_id;
  schema.table_name = "TPCC_RMW_TEST";
  schema.field_cnt = field_count;
  schema.tuple_size = field_count * sizeof(uint64_t);
  schema._columns = new Column[field_count];
  for (uint32_t field = 0; field < field_count; ++field) {
    schema._columns[field].id = field;
    schema._columns[field].size = sizeof(uint64_t);
    schema._columns[field].index = field * sizeof(uint64_t);
  }
}

LifeOperation tpcc_operation(row_t &row, LifeOperationKind kind,
                             uint32_t field, uint64_t argument) {
  LifeOperation op;
  op.object.table_id = row.table->get_table_id();
  op.object.partition_id = row.get_part_id();
  op.object.primary_key = row.get_primary_key();
  op.object.row_id = row.get_primary_key();
  op.kind = kind;
  op.field_id = field;
  op.value_size = sizeof(argument);
  const uint8_t *bytes = reinterpret_cast<const uint8_t *>(&argument);
  op.argument.assign(bytes, bytes + sizeof(argument));
  return op;
}

LifeExecuteResult execute_and_commit(Row_life &manager, LifeTxnDescriptor &tx,
                                     LifeOperation &operation) {
  operation.manager = &manager;
  LifeExecuteResult result = manager.execute(tx, operation);
  assert(result.code == LifeResultCode::Success);
  LifeHistoryEntry entry;
  entry.operation = operation;
  entry.response = result.response;
  life_append_history(tx, entry);
  assert(tx.history.size() == 1);
  assert(manager.prepare(tx).code == LifeResultCode::Success);
  manager.commit(tx);
  return result;
}

void test_tpcc_payment_ytd_rmw() {
  Catalog schema;
  init_uniform_schema(schema, 30, W_YTD + 1);
  table_t table;
  table.init(&schema);
  row_t row;
  row.init(&table, 0, 0);
  row.set_primary_key(1);
  double ytd = 30000.0;
  std::memcpy(row.data + schema.get_field_index(W_YTD), &ytd, sizeof(ytd));
  Row_life manager;
  manager.init(&row);

  const double amount = 125.5;
  uint64_t amount_bits;
  std::memcpy(&amount_bits, &amount, sizeof(amount_bits));
  LifeOperation operation = tpcc_operation(
      row, LifeOperationKind::TpccPaymentYtd, W_YTD, amount_bits);
  LifeTxnDescriptor tx = descriptor(1, 100, 1, YCSB_0, 0);
  execute_and_commit(manager, tx, operation);
  std::memcpy(&ytd, row.data + schema.get_field_index(W_YTD), sizeof(ytd));
  assert(ytd == 30125.5);

  std::free(row.data);
  delete[] schema._columns;
}

#if !life_fairness
void test_tpcc_shared_reads_then_payment() {
  Catalog schema;
  init_uniform_schema(schema, 30, W_YTD + 1);
  table_t table;
  table.init(&schema);
  row_t row;
  row.init(&table, 0, 0);
  row.set_primary_key(1);
  double ytd = 30000.0;
  std::memcpy(row.data + schema.get_field_index(W_YTD), &ytd, sizeof(ytd));
  Row_life manager;
  manager.init(&row);
  LifeOperation read = tpcc_operation(row, LifeOperationKind::ReadField, W_YTD, 0);
  read.argument.clear();
  LifeTxnDescriptor readers[] = {
      descriptor(1, 100, 1, YCSB_0, 0),
      descriptor(2, 200, 1, YCSB_0, 0)};
  for (size_t i = 0; i < 2; ++i) {
    LifeExecuteResult result = manager.execute(readers[i], read);
    assert(result.code == LifeResultCode::Success);
    double observed;
    std::memcpy(&observed, result.response.value.data(), sizeof(observed));
    assert(observed == 30000.0);
    LifeHistoryEntry entry;
    entry.operation = read;
    entry.response = result.response;
    life_append_history(readers[i], entry);
    assert(manager.prepare(readers[i]).code == LifeResultCode::Success);
  }
  const double amount = 125.5;
  uint64_t bits;
  std::memcpy(&bits, &amount, sizeof(bits));
  LifeOperation payment = tpcc_operation(
      row, LifeOperationKind::TpccPaymentYtd, W_YTD, bits);
  LifeTxnDescriptor writer = descriptor(3, 50, 1, YCSB_0, 0);
  assert(manager.execute(writer, payment).code == LifeResultCode::Finalize);
  manager.commit(readers[0]);
  assert(manager.execute(writer, payment).code == LifeResultCode::Finalize);
  manager.commit(readers[1]);
  // Inline execution is speculative: committed data is still unchanged.
  std::memcpy(&ytd, row.data + schema.get_field_index(W_YTD), sizeof(ytd));
  assert(ytd == 30000.0);
  execute_and_commit(manager, writer, payment);
  manager.commit(writer); // Duplicate finish must not apply payment twice.
  std::memcpy(&ytd, row.data + schema.get_field_index(W_YTD), sizeof(ytd));
  assert(ytd == 30125.5);
  std::free(row.data);
  delete[] schema._columns;
}
#endif

void test_tpcc_customer_payment_rmw() {
  Catalog schema;
  init_uniform_schema(schema, 31, C_PAYMENT_CNT + 1);
  table_t table;
  table.init(&schema);
  row_t row;
  row.init(&table, 0, 0);
  row.set_primary_key(2);
  double balance = 1000.0;
  double ytd = 50.0;
  uint64_t count = 7;
  const uint64_t untouched = 99;
  std::memcpy(row.data + schema.get_field_index(C_DISCOUNT), &untouched,
              sizeof(untouched));
  std::memcpy(row.data + schema.get_field_index(C_BALANCE), &balance,
              sizeof(balance));
  std::memcpy(row.data + schema.get_field_index(C_YTD_PAYMENT), &ytd,
              sizeof(ytd));
  std::memcpy(row.data + schema.get_field_index(C_PAYMENT_CNT), &count,
              sizeof(count));
  Row_life manager;
  manager.init(&row);

  const double amount = 25.0;
  uint64_t amount_bits;
  std::memcpy(&amount_bits, &amount, sizeof(amount_bits));
  LifeOperation operation = tpcc_operation(
      row, LifeOperationKind::TpccPaymentCustomer, C_BALANCE, amount_bits);
  LifeTxnDescriptor tx = descriptor(2, 101, 1, YCSB_0, 0);
  execute_and_commit(manager, tx, operation);
  uint64_t unchanged;
  std::memcpy(&balance, row.data + schema.get_field_index(C_BALANCE),
              sizeof(balance));
  std::memcpy(&ytd, row.data + schema.get_field_index(C_YTD_PAYMENT),
              sizeof(ytd));
  std::memcpy(&count, row.data + schema.get_field_index(C_PAYMENT_CNT),
              sizeof(count));
  std::memcpy(&unchanged, row.data + schema.get_field_index(C_DISCOUNT),
              sizeof(unchanged));
  assert(balance == 975.0);
  assert(ytd == 75.0);
  assert(count == 8);
  assert(unchanged == untouched);

  std::free(row.data);
  delete[] schema._columns;
}

void test_tpcc_next_order_id_rmw_returns_new_value() {
  Catalog schema;
  init_uniform_schema(schema, 32, D_NEXT_O_ID + 1);
  table_t table;
  table.init(&schema);
  row_t row;
  row.init(&table, 0, 0);
  row.set_primary_key(3);
  uint64_t order_id = 3001;
  std::memcpy(row.data + schema.get_field_index(D_NEXT_O_ID), &order_id,
              sizeof(order_id));
  Row_life manager;
  manager.init(&row);

  LifeOperation operation = tpcc_operation(
      row, LifeOperationKind::TpccNextOrderId, D_NEXT_O_ID, 0);
  LifeTxnDescriptor tx = descriptor(3, 102, 1, YCSB_0, 0);
  const LifeExecuteResult result = execute_and_commit(manager, tx, operation);
  assert(result.response.value.size() == sizeof(order_id));
  uint64_t returned_id;
  std::memcpy(&returned_id, result.response.value.data(), sizeof(returned_id));
  std::memcpy(&order_id, row.data + schema.get_field_index(D_NEXT_O_ID),
              sizeof(order_id));
  assert(returned_id == 3002);
  assert(order_id == 3002);

  std::free(row.data);
  delete[] schema._columns;
}

void test_tpcc_stock_rmw_branches_and_remote_count() {
  Catalog schema;
  init_uniform_schema(schema, 33, S_REMOTE_CNT + 1);
  table_t table;
  table.init(&schema);

  for (uint32_t branch = 0; branch < 2; ++branch) {
    row_t row;
    row.init(&table, 0, 0);
    row.set_primary_key(10 + branch);
    uint64_t quantity = branch == 0 ? 100 : 15;
    uint64_t remote_count = 4;
    std::memcpy(row.data + schema.get_field_index(S_QUANTITY), &quantity,
                sizeof(quantity));
    std::memcpy(row.data + schema.get_field_index(S_REMOTE_CNT), &remote_count,
                sizeof(remote_count));
    Row_life manager;
    manager.init(&row);

    const bool remote = branch != 0;
    const uint64_t packed = 5 | (remote ? (uint64_t(1) << 63) : 0);
    LifeOperation operation = tpcc_operation(
        row, LifeOperationKind::TpccUpdateStock, S_QUANTITY, packed);
    LifeTxnDescriptor tx = descriptor(4 + branch, 103 + branch, 1, YCSB_0, 0);
    execute_and_commit(manager, tx, operation);
    std::memcpy(&quantity, row.data + schema.get_field_index(S_QUANTITY),
                sizeof(quantity));
    std::memcpy(&remote_count,
                row.data + schema.get_field_index(S_REMOTE_CNT),
                sizeof(remote_count));
    assert(quantity == (branch == 0 ? 95 : 101));
    assert(remote_count == (remote ? 5 : 4));
#if !TPCC_SMALL
    uint64_t ytd;
    uint64_t order_count;
    std::memcpy(&ytd, row.data + schema.get_field_index(S_YTD), sizeof(ytd));
    std::memcpy(&order_count,
                row.data + schema.get_field_index(S_ORDER_CNT),
                sizeof(order_count));
    assert(ytd == 5);
    assert(order_count == 1);
#endif
    std::free(row.data);
  }
  delete[] schema._columns;
}
#endif

} // namespace

#if !life_fairness
namespace {
struct SharedRowFixture {
  Catalog schema;
  table_t table;
  row_t row;
  Row_life manager;
  SharedRowFixture() {
    schema.table_name = "MAIN_TABLE";
    schema.table_id = 3;
    schema.field_cnt = 1;
    schema.tuple_size = sizeof(uint64_t);
    schema._columns = new Column[1];
    schema._columns[0].id = 0;
    schema._columns[0].size = sizeof(uint64_t);
    schema._columns[0].index = 0;
    table.init(&schema);
    row.init(&table, 4, 0);
    row.set_primary_key(7);
    manager.init(&row);
  }
  ~SharedRowFixture() {
    std::free(row.data);
    delete[] schema._columns;
  }
  LifeOperation read() { return operation(row, LifeOperationKind::ReadField, 0); }
  LifeOperation write() { return operation(row, LifeOperationKind::WriteField, 84); }
  LifeExecuteResult execute(LifeTxnDescriptor &tx, const LifeOperation &op) {
    LifeExecuteResult result = manager.execute(tx, op);
    if (result.code == LifeResultCode::Success) {
      LifeHistoryEntry entry;
      entry.operation = op;
      entry.response = result.response;
      life_append_history(tx, entry);
    }
    return result;
  }
};

void test_shared_reader_admission_and_upgrade() {
  SharedRowFixture f;
  LifeTxnDescriptor young = descriptor(1, 30, 1, YCSB_0, 0);
  LifeTxnDescriptor old = descriptor(2, 10, 1, YCSB_0, 0);
  LifeTxnDescriptor writer = descriptor(3, 20, 1, YCSB_0, 0);
  assert(f.execute(young, f.read()).code == LifeResultCode::Success);
  assert(f.execute(old, f.read()).code == LifeResultCode::Success);
  assert(f.manager.execute(young, f.write()).code == LifeResultCode::Help);
  // The first holder is a potential victim, but the second blocks admission.
  assert(f.execute(writer, f.write()).code == LifeResultCode::Help);
  assert(f.manager.prepare(young).code == LifeResultCode::Success);
  f.manager.rollback(young);
  LifeOperation invalid = f.write();
  invalid.field_id = 99;
  LifeTxnDescriptor oldest = descriptor(4, 5, 1, YCSB_0, 0);
  assert(f.execute(oldest, invalid).code == LifeResultCode::InvalidOperation);
  assert(f.execute(old, f.write()).code == LifeResultCode::Success);
  assert(f.execute(old, f.read()).code == LifeResultCode::Success);
  uint64_t value = 0;
  std::memcpy(&value, old.history.back().response.value.data(), sizeof(value));
  assert(value == 84);
  assert(f.execute(writer, f.read()).code == LifeResultCode::Help);
  assert(f.manager.prepare(old).code == LifeResultCode::Success);
  f.manager.commit(old);
  assert(f.execute(writer, f.read()).code == LifeResultCode::Success);
}

void test_shared_selective_abort_and_stale_cleanup() {
  SharedRowFixture f;
  LifeTxnDescriptor a = descriptor(1, 20, 1, YCSB_0, 0);
  LifeTxnDescriptor b = descriptor(2, 30, 1, YCSB_0, 0);
  LifeTxnDescriptor writer = descriptor(3, 10, 1, YCSB_0, 0);
  assert(f.execute(a, f.read()).code == LifeResultCode::Success);
  assert(f.execute(b, f.read()).code == LifeResultCode::Success);
  assert(f.execute(writer, f.write()).code == LifeResultCode::Success);
  assert(f.manager.prepare(a).code == LifeResultCode::Retry);
  assert(f.manager.prepare(b).code == LifeResultCode::Retry);
  LifeTxnDescriptor stale = writer;
  f.manager.rollback(writer);
  writer = descriptor(3, 10, 2, YCSB_0, 0);
  assert(f.execute(writer, f.write()).code == LifeResultCode::Success);
  f.manager.rollback(stale);
  f.manager.help(stale);
  f.manager.commit(stale);
  assert(f.execute(a, f.read()).code == LifeResultCode::Retry);
  LifeTxnDescriptor other = descriptor(4, 40, 1, YCSB_0, 0);
  assert(f.execute(other, f.read()).code == LifeResultCode::Help);
  assert(f.manager.prepare(writer).code == LifeResultCode::Success);
  f.manager.commit(writer);
  assert(f.manager.execute(stale, f.write()).code == LifeResultCode::Committed);
}

void test_concurrent_prepared_readers_and_inline_writer() {
  SharedRowFixture f;
  LifeTxnDescriptor a = descriptor(1, 20, 1, YCSB_0, 0);
  LifeTxnDescriptor b = descriptor(2, 30, 1, YCSB_0, 0);
  auto reader = [&f](LifeTxnDescriptor &tx) {
    assert(f.execute(tx, f.read()).code == LifeResultCode::Success);
    assert(f.manager.prepare(tx).code == LifeResultCode::Success);
  };
  std::thread first(reader, std::ref(a));
  std::thread second(reader, std::ref(b));
  first.join();
  second.join();
  LifeTxnDescriptor c = descriptor(4, 40, 1, YCSB_0, 0);
  assert(f.execute(c, f.read()).code == LifeResultCode::Success);
  assert(f.manager.prepare(c).code == LifeResultCode::Success);
  f.manager.commit(c);
  LifeTxnDescriptor writer = descriptor(3, 10, 1, YCSB_0, 0);
  assert(f.manager.execute(writer, f.write()).code == LifeResultCode::Finalize);
  f.manager.commit(a); // Inline replay must encounter the remaining reader.
  assert(f.manager.execute(writer, f.write()).code == LifeResultCode::Finalize);
  f.manager.commit(b); // Inline replay can now admit the writer.
  assert(f.execute(writer, f.write()).code == LifeResultCode::Success);
  assert(writer.history.size() == 1);
  assert(f.manager.prepare(writer).code == LifeResultCode::Success);
  f.manager.commit(writer);
  uint64_t value = 0;
  std::memcpy(&value, f.row.data, sizeof(value));
  assert(value == 84);
}

void test_shared_stale_checks_precede_holder_conflicts() {
  SharedRowFixture f;
  LifeTxnDescriptor reader = descriptor(1, 20, 1, YCSB_0, 0);
  const LifeTxnDescriptor before_read = reader;
  assert(f.execute(reader, f.read()).code == LifeResultCode::Success);
  const LifeExecuteResult replay = f.manager.execute(before_read, f.read());
  assert(replay.code == LifeResultCode::Success);
  assert(replay.response == reader.history.back().response);

  LifeTxnDescriptor writer = descriptor(2, 10, 1, YCSB_0, 0);
  assert(f.execute(writer, f.write()).code == LifeResultCode::Success);
  assert(f.manager.prepare(writer).code == LifeResultCode::Success);
  // The reader was wounded. A stale request must retry before it can help
  // another holder, even when that holder is prepared.
  assert(f.manager.execute(reader, f.read()).code == LifeResultCode::Retry);
  ++reader.tid.attempt;
  reader.history.clear();
  reader.touched_objects.clear();
  assert(f.manager.execute(reader, f.read()).code == LifeResultCode::Finalize);
  f.manager.commit(writer);
  assert(f.execute(reader, f.read()).code == LifeResultCode::Success);
  assert(f.manager.prepare(reader).code == LifeResultCode::Success);
  f.manager.commit(reader);

  LifeTxnDescriptor next = descriptor(1, 30, 1, YCSB_0, 0);
  assert(f.execute(next, f.write()).code == LifeResultCode::Success);
  f.manager.rollback(reader);
  f.manager.help(reader);
  f.manager.commit(reader);
  assert(f.manager.execute(reader, f.read()).code == LifeResultCode::Committed);
  LifeTxnDescriptor contender = descriptor(3, 40, 1, YCSB_0, 0);
  assert(f.manager.execute(contender, f.read()).code == LifeResultCode::Help);
  f.manager.rollback(next);
  assert(f.execute(contender, f.read()).code == LifeResultCode::Success);
}
} // namespace
#endif

int main() {
#if !life_fairness
  test_shared_reader_admission_and_upgrade();
  test_shared_selective_abort_and_stale_cleanup();
  test_concurrent_prepared_readers_and_inline_writer();
  test_shared_stale_checks_precede_holder_conflicts();
#endif
  static_assert(sizeof(LifeBytes) <= 16,
                "LIFE values must remain inline and compact");
  test_snapshot_is_owned();
  test_ycsb_reconcile_consumes_durable_response();
  test_add_int64_replays_and_commits();
#if CC_ALG == LIFE
  test_query_stably_groups_destinations_home_first();
#endif
  test_execute_stale_history_refresh_and_help();
  test_execute_rejects_globally_stale_descriptor();
  test_prepared_holder_precedes_committed_duplicate();
  test_rollback_runs_inline_help();
#if life_fairness
  test_priority_heap_orders_all_uncommitted_transactions();
  test_prepared_holder_blocks_higher_priority_contender();
  test_higher_attempt_reuses_heap_node_and_rejects_stale_commit();
#endif
  test_prepare_retry_and_commit_publish();
  test_recycled_process_name_rejects_stale_transaction();
  test_eight_byte_operation_preserves_rest_of_ycsb_field();
  test_shared_prepare_owns_descriptor_and_read_commit_is_stable();
  test_prepared_rows_share_one_frozen_descriptor();
  test_local_row_cache_and_incremental_grouping();
#if WORKLOAD == TPCC
#if !life_fairness
  test_tpcc_shared_reads_then_payment();
#endif
  test_tpcc_payment_ytd_rmw();
  test_tpcc_customer_payment_rmw();
  test_tpcc_next_order_id_rmw_returns_new_value();
  test_tpcc_stock_rmw_branches_and_remote_count();
#endif
  return 0;
}

/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "global.h"
#include "helper.h"
#include "txn.h"
#include "wl.h"

class YCSBQuery;
class YCSBQueryMessage;
class Message;
class ycsb_request;

LifeYcsbRequest make_life_ycsb_request(const ycsb_request &request);
LifeYcsbSnapshot make_life_ycsb_snapshot(const YCSBQuery &query, uint32_t state,
                                         uint64_t next_record_id);
void reconcile_life_ycsb_program(LifeTxnDescriptor &descriptor);

enum YCSBRemTxnType { YCSB_0, YCSB_1, YCSB_FIN, YCSB_RDONE };

class YCSBWorkload : public Workload {
public:
  RC init();
  RC init_table();
  RC init_schema(const char *schema_file);
  RC get_txn_man(TxnManager *&txn_manager);
  int key_to_part(uint64_t key);
  INDEX *the_index;
  table_t *the_table;

private:
  void init_table_parallel();
  void *init_table_slice();
  static void *threadInitTable(void *This) {
    ((YCSBWorkload *)This)->init_table_slice();
    return NULL;
  }
  pthread_mutex_t insert_lock;
  //  For parallel initialization
  static int next_tid;
};

class YCSBTxnManager : public TxnManager {
public:
  YCSBTxnManager();
  virtual ~YCSBTxnManager();
  void init(uint64_t thd_id, Workload *h_wl);
  void reset();
  void partial_reset();
  RC acquire_locks();
  RC run_txn();
  RC run_txn_post_wait();
  RC run_calvin_txn();
  virtual LifeTxnDescriptor life_descriptor() const;
#if CC_ALG == LIFE
  void life_reset_workload();
  RC send_life_execute(const LifeTxnDescriptor &descriptor,
                       const LifeOperation &operation, uint64_t wait_id);
  LifeExecuteResult execute_life_remote(const LifeTxnDescriptor &descriptor,
                                        const LifeOperation &operation);
  RC serve_life_execute(const LifeTxnDescriptor &descriptor,
                        const LifeOperation &operation,
                        uint64_t stop_record_id,
                        bool prepare_after_execute,
                        LifeExecuteResult &immediate_result,
                        bool &prepared);
  RC apply_life_execute_response(const LifeExecuteResult &result,
                                 uint64_t wait_id,
                                 uint64_t history_base_size, bool prepared,
                                 uint64_t responder_node_id);
  LifeExecuteResult prepare_life_remote(const LifeTxnDescriptor &descriptor);
  LifeExecuteResult finish_life_remote(const LifeTxnDescriptor &descriptor,
                                       RC decision);
  RC apply_life_help_request(const LifeTxnDescriptor &descriptor,
                             uint64_t requester_node_id);
  RC help_life_remote(const LifeTxnDescriptor &descriptor);
  RC apply_life_finalize_request(const LifeTxnDescriptor &descriptor,
                                 uint64_t requester_node_id,
                                 uint64_t requester_txn_id);
  RC apply_life_finalize_response(const LifeExecuteResult &result);
  RC apply_life_prepare_response(const LifeExecuteResult &result,
                                  const LifeProcessId &pid,
                                  const LifeTxnId &tid,
                                  uint64_t responder_node_id);
  RC complete_life_finish();
  void debug_life_state() const;
  bool is_life_active() const;
  void mark_life_active();
  void clear_life_active();
#endif
  void copy_remote_requests(YCSBQueryMessage *msg);

protected:
  void next_ycsb_state();
  struct LifeWaitContext {
    uint64_t wait_id;
    uint32_t reason;
    uint64_t remote_node_id;
    uint64_t remote_key;
    std::vector<LifeTxnDescriptor> stack;
  };
#if CC_ALG == LIFE
  RC run_life_txn();
  virtual RC complete_life_home_transaction();
  bool try_life_transactions(std::vector<LifeTxnDescriptor> &txns);
  virtual LifeExecuteResult
  execute_life_operation(LifeTxnDescriptor &descriptor,
                         LifeOperation &operation);
  virtual void life_reconcile_descriptor(LifeTxnDescriptor &descriptor);
  virtual bool life_program_done(const LifeTxnDescriptor &descriptor) const;
  virtual LifeOperation
  life_current_operation(LifeTxnDescriptor &descriptor) const;
  virtual void life_advance_program(LifeTxnDescriptor &descriptor,
                                    const LifeOperation &operation,
                                    const LifeResponse &response);
  virtual uint64_t life_program_position(
      const LifeTxnDescriptor &descriptor) const;
  virtual bool life_program_present(const LifeTxnDescriptor &descriptor) const;
  virtual void life_reset_program(LifeTxnDescriptor &descriptor);
  virtual void life_copy_program_to_workload(
      const LifeTxnDescriptor &descriptor);
  virtual void life_copy_remote_program(LifeTxnDescriptor &destination,
                                        const LifeTxnDescriptor &source);
  bool finalize_life_descriptor(LifeTxnDescriptor &descriptor);
  void reset_life_descriptor(LifeTxnDescriptor &descriptor,
                             uint64_t observed_attempt);
  void append_life_success(LifeTxnDescriptor &descriptor,
                           const LifeOperation &operation,
                           const LifeResponse &response);
  RC continue_life_after_stack();
  void push_life_help_descriptor(std::vector<LifeTxnDescriptor> &txns,
                                 const LifeTxnDescriptor &descriptor);
  void rollback_life_descriptor(const LifeTxnDescriptor &descriptor);
  void collect_life_objects(const LifeTxnDescriptor &descriptor,
                            std::vector<LifeFinalizeObject> &objects);
  void copy_life_descriptor_to_workload(const LifeTxnDescriptor &descriptor);
  uint64_t allocate_life_wait_id();
  void save_life_wait_stack(std::vector<LifeTxnDescriptor> &txns,
                            uint64_t wait_id, uint32_t reason,
                            uint64_t remote_node_id, uint64_t remote_key);
  bool take_life_wait_stack(uint64_t wait_id,
                            std::vector<LifeTxnDescriptor> &txns);
  bool take_life_wait_stack_by_reason(uint32_t reason,
                                      std::vector<LifeTxnDescriptor> &txns);
  bool take_life_wait_stack_by_descriptor(
      uint32_t reason, const LifeTxnDescriptor &descriptor,
      std::vector<LifeTxnDescriptor> &txns);
  bool has_life_wait_stack(const LifeTxnDescriptor &descriptor,
                           uint32_t reason, uint64_t remote_node_id,
                           uint64_t remote_key) const;
  bool pending_life_finalize_matches(const LifeProcessId &pid,
                                     const LifeTxnId &tid) const;
  bool note_life_prepare_response(uint64_t responder_node_id);
  void note_life_piggyback_prepare(const LifeTxnDescriptor &descriptor,
                                   uint64_t node_id);
  bool has_life_piggyback_prepare(const LifeTxnDescriptor &descriptor,
                                  uint64_t node_id) const;
  void reset_life_piggyback_prepare();
  virtual row_t *lookup_life_row(const LifeObjectId &object) const;
  row_t *lookup_life_row(uint64_t key) const;
  virtual uint64_t life_remote_batch_stop(const LifeTxnDescriptor &descriptor,
                                          uint64_t dest_node_id) const;
  LifeOperation make_life_operation(const LifeYcsbRequest &req);
  LifeOperation make_life_operation(row_t *row, ycsb_request *req);
  LifeOperation make_life_operation(row_t *row, const LifeYcsbRequest &req);
  void reset_pending_life_finalize();
  void send_life_message_to_node(Message *msg, uint64_t node_id);
  void send_life_finalize_request(const LifeTxnDescriptor &descriptor);
  void add_life_finalize_requester(uint64_t requester_node_id,
                                   uint64_t requester_txn_id);
  void send_life_finalize_response(const LifeExecuteResult &result,
                                   uint64_t requester_node_id,
                                   uint64_t requester_txn_id);
  void send_life_prepare_messages(const LifeTxnDescriptor &descriptor,
                                  const std::vector<uint64_t> &nodes);
  void send_life_finish_messages(const LifeTxnDescriptor &descriptor,
                                 const std::vector<uint64_t> &nodes,
                                 RC decision);
#endif
  RC run_txn_state();
  RC run_ycsb_0(ycsb_request *req, row_t *&row_local);
  RC run_ycsb_1(access_t acctype, row_t *row_local);
  RC run_ycsb();
  bool is_done();
  bool is_local_request(uint64_t idx);
  RC send_remote_request();

  row_t *row;
  YCSBWorkload *_wl;
  YCSBRemTxnType state;
  uint64_t next_record_id;
  bool life_finalize_waiting;
  uint64_t life_prepare_pending;
  bool life_prepare_failed;
  uint64_t life_prepare_observed_attempt;
  LifeTxnDescriptor life_pending_finalize;
  std::vector<LifeFinalizeObject> life_pending_objects;
  std::vector<uint64_t> life_pending_remote_nodes;
  std::vector<uint64_t> life_pending_prepare_nodes;
  std::vector<uint64_t> life_prepare_response_nodes;
  std::vector<uint64_t> life_piggyback_prepared_nodes;
  LifeTxnId life_piggyback_prepared_tid;
  bool life_piggyback_prepared_tid_valid;
  std::vector<LifeTxnDescriptor> life_txn_stack;
  std::vector<LifeTxnDescriptor> life_response_stack;
  std::vector<LifeFinalizeObject> life_object_scratch;
  std::vector<uint64_t> *life_finalize_requester_nodes;
  std::vector<uint64_t> *life_finalize_requester_txn_ids;
  std::vector<LifeWaitContext> *life_wait_stacks;
  bool life_active;
  uint64_t life_next_wait_id;
#if LOG_LIFE
  uint64_t life_help_time;
  uint64_t life_own_time;
  uint64_t life_finalize_time;
#endif
};

// Workload-neutral name used by LIFE dispatch. YCSBTxnManager retains its
// historical name to avoid a disruptive ABI/source rename, while PPS derives
// from the same protocol coordinator through this alias.
typedef YCSBTxnManager LifeTxnManager;

#endif

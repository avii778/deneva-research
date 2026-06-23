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
class ycsb_request;

LifeYcsbRequest make_life_ycsb_request(const ycsb_request &request);
LifeYcsbSnapshot make_life_ycsb_snapshot(const YCSBQuery &query, uint32_t state,
                                         uint64_t next_record_id);

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
  void init(uint64_t thd_id, Workload *h_wl);
  void reset();
  void partial_reset();
  RC acquire_locks();
  RC run_txn();
  RC run_txn_post_wait();
  RC run_calvin_txn();
  LifeTxnDescriptor life_descriptor() const;
  void life_reset_workload();
  RC send_life_execute(const LifeTxnDescriptor &descriptor,
                       const LifeOperation &operation);
  LifeExecuteResult execute_life_remote(const LifeTxnDescriptor &descriptor,
                                        const LifeOperation &operation);
  RC apply_life_execute_response(const LifeExecuteResult &result);
  void copy_remote_requests(YCSBQueryMessage *msg);

private:
  void next_ycsb_state();
  RC run_life_txn();
  bool try_life_transactions(std::vector<LifeTxnDescriptor> &txns);
  LifeExecuteResult execute_life_operation(LifeTxnDescriptor &descriptor,
                                           LifeOperation &operation);
  bool finalize_life_descriptor(LifeTxnDescriptor &descriptor);
  void reset_life_descriptor(LifeTxnDescriptor &descriptor,
                             uint64_t observed_attempt);
  void append_life_success(LifeTxnDescriptor &descriptor,
                           const LifeOperation &operation,
                           const LifeResponse &response);
  void rollback_life_descriptor(const LifeTxnDescriptor &descriptor);
  void collect_life_objects(const LifeTxnDescriptor &descriptor,
                            std::vector<LifeFinalizeObject> &objects);
  void copy_life_descriptor_to_workload(const LifeTxnDescriptor &descriptor);
  row_t *lookup_life_row(const LifeObjectId &object) const;
  row_t *lookup_life_row(uint64_t key) const;
  LifeOperation make_life_operation(row_t *row, ycsb_request *req);
  LifeOperation make_life_operation(row_t *row, const LifeYcsbRequest &req);
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
#if LOG_LIFE
  uint64_t life_help_time;
  uint64_t life_own_time;
  uint64_t life_finalize_time;
#endif
};

#endif

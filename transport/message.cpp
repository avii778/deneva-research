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

#include "mem_alloc.h"
#include "query.h"
#include "ycsb_query.h"
#include "ycsb.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "pps_query.h"
#include "pps.h"
#include "global.h"
#include "message.h"
#include "maat.h"

namespace {

uint64_t life_bytes_size(const LifeBytes &bytes) {
  return sizeof(uint8_t) + bytes.size();
}

void life_write_bytes(char *buf, uint64_t &ptr, const LifeBytes &bytes) {
  const uint8_t size = static_cast<uint8_t>(bytes.size());
  COPY_BUF(buf, size, ptr);
  if (size > 0) {
    memcpy(&buf[ptr], bytes.data(), size);
    ptr += size;
  }
}

void life_read_bytes(char *buf, uint64_t &ptr, LifeBytes &bytes) {
  uint8_t size;
  COPY_VAL(size, buf, ptr);
  assert(size <= LIFE_INLINE_VALUE_CAPACITY);

  uint8_t data[LIFE_INLINE_VALUE_CAPACITY];
  if (size > 0) {
    memcpy(data, &buf[ptr], size);
    ptr += size;
  }
  bytes.assign(data, data + size);
}

uint64_t life_object_id_size() {
  return sizeof(uint64_t) * 3;
}

void life_write_object_id(char *buf, uint64_t &ptr,
                          const LifeObjectId &object) {
  COPY_BUF(buf, object.table_id, ptr);
  COPY_BUF(buf, object.partition_id, ptr);
  COPY_BUF(buf, object.primary_key, ptr);
}

void life_read_object_id(char *buf, uint64_t &ptr, LifeObjectId &object) {
  COPY_VAL(object.table_id, buf, ptr);
  COPY_VAL(object.partition_id, buf, ptr);
  COPY_VAL(object.primary_key, buf, ptr);
}

uint64_t life_operation_size(const LifeOperation &operation) {
  return life_object_id_size() + sizeof(uint32_t) + sizeof(uint32_t) +
         sizeof(uint8_t) + life_bytes_size(operation.argument);
}

void life_write_operation(char *buf, uint64_t &ptr,
                          const LifeOperation &operation) {
  life_write_object_id(buf, ptr, operation.object);
  const uint32_t kind = static_cast<uint32_t>(operation.kind);
  COPY_BUF(buf, kind, ptr);
  COPY_BUF(buf, operation.field_id, ptr);
  COPY_BUF(buf, operation.value_size, ptr);
  life_write_bytes(buf, ptr, operation.argument);
}

void life_read_operation(char *buf, uint64_t &ptr, LifeOperation &operation) {
  life_read_object_id(buf, ptr, operation.object);
  uint32_t kind;
  COPY_VAL(kind, buf, ptr);
  operation.kind = static_cast<LifeOperationKind>(kind);
  COPY_VAL(operation.field_id, buf, ptr);
  COPY_VAL(operation.value_size, buf, ptr);
  life_read_bytes(buf, ptr, operation.argument);
  operation.manager = NULL;
}

uint64_t life_response_size(const LifeResponse &response) {
  return life_bytes_size(response.value);
}

void life_write_response(char *buf, uint64_t &ptr,
                         const LifeResponse &response) {
  life_write_bytes(buf, ptr, response.value);
}

void life_read_response(char *buf, uint64_t &ptr, LifeResponse &response) {
  life_read_bytes(buf, ptr, response.value);
}

uint64_t life_history_entry_size(const LifeHistoryEntry &entry) {
  return life_operation_size(entry.operation) + life_response_size(entry.response);
}

void life_write_history_entry(char *buf, uint64_t &ptr,
                              const LifeHistoryEntry &entry) {
  life_write_operation(buf, ptr, entry.operation);
  life_write_response(buf, ptr, entry.response);
}

void life_read_history_entry(char *buf, uint64_t &ptr,
                             LifeHistoryEntry &entry) {
  life_read_operation(buf, ptr, entry.operation);
  life_read_response(buf, ptr, entry.response);
}

uint64_t life_ycsb_request_size() {
  return sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint8_t);
}

void life_write_ycsb_request(char *buf, uint64_t &ptr,
                             const LifeYcsbRequest &request) {
  const uint32_t kind = static_cast<uint32_t>(request.kind);
  COPY_BUF(buf, kind, ptr);
  COPY_BUF(buf, request.key, ptr);
  COPY_BUF(buf, request.value, ptr);
}

void life_read_ycsb_request(char *buf, uint64_t &ptr,
                            LifeYcsbRequest &request) {
  uint32_t kind;
  COPY_VAL(kind, buf, ptr);
  request.kind = static_cast<LifeYcsbRequestKind>(kind);
  COPY_VAL(request.key, buf, ptr);
  COPY_VAL(request.value, buf, ptr);
  request.row = NULL;
}

uint64_t life_descriptor_size(const LifeTxnDescriptor &descriptor) {
  uint64_t size = sizeof(descriptor.pid.node_id) +
                  sizeof(descriptor.pid.worker_id) + sizeof(uint64_t) * 2;
  size += sizeof(uint32_t) + sizeof(uint64_t);

  size += sizeof(size_t);
  for (size_t i = 0; i < descriptor.history.size(); ++i)
    size += life_history_entry_size(descriptor.history[i]);

  size += sizeof(size_t);
  for (size_t i = 0; i < descriptor.ycsb.requests.size(); ++i)
    size += life_ycsb_request_size();

  return size;
}

void life_write_descriptor(char *buf, uint64_t &ptr,
                           const LifeTxnDescriptor &descriptor) {
  COPY_BUF(buf, descriptor.pid.node_id, ptr);
  COPY_BUF(buf, descriptor.pid.worker_id, ptr);
  COPY_BUF(buf, descriptor.tid.time, ptr);
  COPY_BUF(buf, descriptor.tid.attempt, ptr);
  COPY_BUF(buf, descriptor.ycsb.state, ptr);
  COPY_BUF(buf, descriptor.ycsb.next_record_id, ptr);

  size_t size = descriptor.history.size();
  COPY_BUF(buf, size, ptr);
  for (size_t i = 0; i < descriptor.history.size(); ++i)
    life_write_history_entry(buf, ptr, descriptor.history[i]);

  size = descriptor.ycsb.requests.size();
  COPY_BUF(buf, size, ptr);
  for (size_t i = 0; i < descriptor.ycsb.requests.size(); ++i)
    life_write_ycsb_request(buf, ptr, descriptor.ycsb.requests[i]);
}

void life_read_descriptor(char *buf, uint64_t &ptr,
                          LifeTxnDescriptor &descriptor) {
  descriptor = LifeTxnDescriptor();
  COPY_VAL(descriptor.pid.node_id, buf, ptr);
  COPY_VAL(descriptor.pid.worker_id, buf, ptr);
  COPY_VAL(descriptor.tid.time, buf, ptr);
  COPY_VAL(descriptor.tid.attempt, buf, ptr);
  COPY_VAL(descriptor.ycsb.state, buf, ptr);
  COPY_VAL(descriptor.ycsb.next_record_id, buf, ptr);

  size_t size;
  COPY_VAL(size, buf, ptr);
  descriptor.history.resize(size);
  for (size_t i = 0; i < size; ++i)
    life_read_history_entry(buf, ptr, descriptor.history[i]);

  COPY_VAL(size, buf, ptr);
  descriptor.ycsb.requests.resize(size);
  for (size_t i = 0; i < size; ++i)
    life_read_ycsb_request(buf, ptr, descriptor.ycsb.requests[i]);
  descriptor.history.reserve(descriptor.ycsb.requests.size());
  descriptor.touched_objects.reserve(descriptor.ycsb.requests.size());
}

uint64_t life_result_size(const LifeExecuteResult &result) {
  return sizeof(uint32_t) + life_response_size(result.response) +
         life_descriptor_size(result.transaction) + sizeof(uint64_t);
}

void life_write_result(char *buf, uint64_t &ptr,
                       const LifeExecuteResult &result) {
  const uint32_t code = static_cast<uint32_t>(result.code);
  COPY_BUF(buf, code, ptr);
  life_write_response(buf, ptr, result.response);
  life_write_descriptor(buf, ptr, result.transaction);
  COPY_BUF(buf, result.observed_attempt, ptr);
}

void life_read_result(char *buf, uint64_t &ptr, LifeExecuteResult &result) {
  uint32_t code;
  COPY_VAL(code, buf, ptr);
  result.code = static_cast<LifeResultCode>(code);
  life_read_response(buf, ptr, result.response);
  life_read_descriptor(buf, ptr, result.transaction);
  COPY_VAL(result.observed_attempt, buf, ptr);
}

} // namespace

std::vector<Message*> * Message::create_messages(char * buf) {
  std::vector<Message*> * all_msgs = new std::vector<Message*>;
  char * data = buf;
	uint64_t ptr = 0;
  uint32_t dest_id;
  uint32_t return_id;
  uint32_t txn_cnt;
  COPY_VAL(dest_id,data,ptr);
  COPY_VAL(return_id,data,ptr);
  COPY_VAL(txn_cnt,data,ptr);
  assert(dest_id == g_node_id);
  assert(return_id != g_node_id);
  assert(ISCLIENTN(return_id) || ISSERVERN(return_id) || ISREPLICAN(return_id));
  while(txn_cnt > 0) {
    Message * msg = create_message(&data[ptr]);
    msg->return_node_id = return_id;
    ptr += msg->get_size();
    all_msgs->push_back(msg);
    --txn_cnt;
  }
  return all_msgs;
}

Message * Message::create_message(char * buf) {
 RemReqType rtype = NO_MSG;
 uint64_t ptr = 0;
 COPY_VAL(rtype,buf,ptr);
 Message * msg = create_message(rtype);
 msg->copy_from_buf(buf);
 return msg;
}

Message * Message::create_message(TxnManager * txn, RemReqType rtype) {
 Message * msg = create_message(rtype);
 msg->mcopy_from_txn(txn);
 msg->copy_from_txn(txn);

 // copy latency here
 msg->lat_work_queue_time = txn->txn_stats.work_queue_time_short;
 msg->lat_msg_queue_time = txn->txn_stats.msg_queue_time_short;
 msg->lat_cc_block_time = txn->txn_stats.cc_block_time_short;
 msg->lat_cc_time = txn->txn_stats.cc_time_short;
 msg->lat_process_time = txn->txn_stats.process_time_short;
 msg->lat_network_time = txn->txn_stats.lat_network_time_start;
 msg->lat_other_time = txn->txn_stats.lat_other_time_start;

 return msg;
}

Message * Message::create_message(LogRecord * record, RemReqType rtype) {
 Message * msg = create_message(rtype);
 ((LogMessage*)msg)->copy_from_record(record);
 msg->txn_id = record->rcd.txn_id;
 return msg;
}


Message * Message::create_message(BaseQuery * query, RemReqType rtype) {
 assert(rtype == RQRY || rtype == CL_QRY);
 Message * msg = create_message(rtype);
#if WORKLOAD == YCSB
 ((YCSBClientQueryMessage*)msg)->copy_from_query(query);
#elif WORKLOAD == TPCC 
 ((TPCCClientQueryMessage*)msg)->copy_from_query(query);
#elif WORKLOAD == PPS 
 ((PPSClientQueryMessage*)msg)->copy_from_query(query);
#endif
 return msg;
}

Message * Message::create_message(uint64_t txn_id, RemReqType rtype) {
 Message * msg = create_message(rtype);
 msg->txn_id = txn_id;
 return msg;
}

Message * Message::create_message(uint64_t txn_id, uint64_t batch_id, RemReqType rtype) {
 Message * msg = create_message(rtype);
 msg->txn_id = txn_id;
 msg->batch_id = batch_id;
 return msg;
}

Message * Message::create_message(RemReqType rtype) {
  Message * msg;
  switch(rtype) {
    case INIT_DONE:
      msg = new InitDoneMessage;
      break;
    case RQRY:
    case RQRY_CONT:
#if WORKLOAD == YCSB
      msg = new YCSBQueryMessage;
#elif WORKLOAD == TPCC 
      msg = new TPCCQueryMessage;
#elif WORKLOAD == PPS 
      msg = new PPSQueryMessage;
#endif
      msg->init();
      break;
    case RFIN:
      msg = new FinishMessage;
      break;
    case RQRY_RSP:
      msg = new QueryResponseMessage;
      break;
    case LOG_MSG:
      msg = new LogMessage;
      break;
    case LOG_MSG_RSP:
      msg = new LogRspMessage;
      break;
    case LOG_FLUSHED:
      msg = new LogFlushedMessage;
      break;
    case CALVIN_ACK:
    case RACK_PREP:
    case RACK_FIN:
      msg = new AckMessage;
      break;
    case CL_QRY:
    case RTXN:
    case RTXN_CONT:
#if WORKLOAD == YCSB
      msg = new YCSBClientQueryMessage;
#elif WORKLOAD == TPCC 
      msg = new TPCCClientQueryMessage;
#elif WORKLOAD == PPS 
      msg = new PPSClientQueryMessage;
#endif
      msg->init();
      break;
    case RPREPARE:
      msg = new PrepareMessage;
      break;
    case RLIFE_EXECUTE:
      msg = new LifeExecuteMessage;
      break;
    case RLIFE_EXECUTE_RSP:
      msg = new LifeExecuteResponseMessage;
      break;
    case RLIFE_PREPARE:
      msg = new LifePrepareMessage;
      break;
    case RLIFE_PREPARE_RSP:
      msg = new LifePrepareResponseMessage;
      break;
    case RLIFE_FINISH:
      msg = new LifeFinishMessage;
      break;
    case RLIFE_FINISH_RSP:
      msg = new LifeFinishResponseMessage;
      break;
    case RLIFE_HELP:
      msg = new LifeHelpMessage;
      break;
    case RLIFE_HELP_APPLY:
      msg = new LifeHelpApplyMessage;
      break;
    case RLIFE_FINALIZE:
      msg = new LifeFinalizeMessage;
      break;
    case RLIFE_FINALIZE_RSP:
      msg = new LifeFinalizeResponseMessage;
      break;
    case RFWD:
      msg = new ForwardMessage;
      break;
    case RDONE:
      msg = new DoneMessage;
      break;
    case CL_RSP:
      msg = new ClientResponseMessage;
      break;
    default: assert(false);
  }
  assert(msg);
  msg->rtype = rtype;
  msg->txn_id = UINT64_MAX;
  msg->batch_id = UINT64_MAX;
  msg->return_node_id = g_node_id;
  msg->wq_time = 0;
  msg->mq_time = 0;
  msg->ntwk_time = 0;

  msg->lat_work_queue_time = 0;
  msg->lat_msg_queue_time = 0;
  msg->lat_cc_block_time = 0;
  msg->lat_cc_time = 0;
  msg->lat_process_time = 0;
  msg->lat_network_time = 0;
  msg->lat_other_time = 0;


  return msg;
}

uint64_t Message::mget_size() {
  uint64_t size = 0;
  size += sizeof(RemReqType);
  size += sizeof(uint64_t);
#if CC_ALG == CALVIN
  size += sizeof(uint64_t);
#endif
  // for stats, send message queue time
  size += sizeof(uint64_t);

  // for stats, latency
  size += sizeof(uint64_t) * 7;
  return size;
}

void Message::mcopy_from_txn(TxnManager * txn) {
  //rtype = query->rtype;
  txn_id = txn->get_txn_id();
#if CC_ALG == CALVIN
  batch_id = txn->get_batch_id();
#endif
}

void Message::mcopy_to_txn(TxnManager * txn) {
  txn->return_id = return_node_id;
}


void Message::mcopy_from_buf(char * buf) {
  uint64_t ptr = 0;
  COPY_VAL(rtype,buf,ptr);
  COPY_VAL(txn_id,buf,ptr);
#if CC_ALG == CALVIN
  COPY_VAL(batch_id,buf,ptr);
#endif
  COPY_VAL(mq_time,buf,ptr);

  COPY_VAL(lat_work_queue_time,buf,ptr);
  COPY_VAL(lat_msg_queue_time,buf,ptr);
  COPY_VAL(lat_cc_block_time,buf,ptr);
  COPY_VAL(lat_cc_time,buf,ptr);
  COPY_VAL(lat_process_time,buf,ptr);
  COPY_VAL(lat_network_time,buf,ptr);
  COPY_VAL(lat_other_time,buf,ptr);
  if ((CC_ALG == CALVIN && rtype == CALVIN_ACK && txn_id % g_node_cnt == g_node_id) || (CC_ALG != CALVIN && IS_LOCAL(txn_id))) {
    lat_network_time = (get_sys_clock() - lat_network_time) - lat_other_time;
  } else {
    lat_other_time = get_sys_clock();
  }
  //printf("buftot %ld: %f, %f\n",txn_id,lat_network_time,lat_other_time);
}

void Message::mcopy_to_buf(char * buf) {
  uint64_t ptr = 0;
  COPY_BUF(buf,rtype,ptr);
  COPY_BUF(buf,txn_id,ptr);
#if CC_ALG == CALVIN
  COPY_BUF(buf,batch_id,ptr);
#endif
  COPY_BUF(buf,mq_time,ptr);

  COPY_BUF(buf,lat_work_queue_time,ptr);
  COPY_BUF(buf,lat_msg_queue_time,ptr);
  COPY_BUF(buf,lat_cc_block_time,ptr);
  COPY_BUF(buf,lat_cc_time,ptr);
  COPY_BUF(buf,lat_process_time,ptr);
  if ((CC_ALG == CALVIN && rtype == CL_QRY && txn_id % g_node_cnt == g_node_id) || (CC_ALG != CALVIN && IS_LOCAL(txn_id))) {
    lat_network_time = get_sys_clock();
  } else {
    lat_other_time = get_sys_clock() - lat_other_time;
  }
  //printf("mtobuf %ld: %f, %f\n",txn_id,lat_network_time,lat_other_time);
  COPY_BUF(buf,lat_network_time,ptr);
  COPY_BUF(buf,lat_other_time,ptr);
}

void Message::release_message(Message * msg) {
  switch(msg->rtype) {
    case INIT_DONE: {
      InitDoneMessage * m_msg = (InitDoneMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                    }
    case RQRY:
    case RQRY_CONT: {
#if WORKLOAD == YCSB
      YCSBQueryMessage * m_msg = (YCSBQueryMessage*)msg;
#elif WORKLOAD == TPCC 
      TPCCQueryMessage * m_msg = (TPCCQueryMessage*)msg;
#elif WORKLOAD == PPS 
      PPSQueryMessage * m_msg = (PPSQueryMessage*)msg;
#endif
      m_msg->release();
      delete m_msg;
      break;
                    }
    case RFIN: {
      FinishMessage * m_msg = (FinishMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
               }
    case RQRY_RSP: {
      QueryResponseMessage * m_msg = (QueryResponseMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                   }
    case LOG_MSG: {
      LogMessage * m_msg = (LogMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                  }
    case LOG_MSG_RSP: {
      LogRspMessage * m_msg = (LogRspMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                      }
    case LOG_FLUSHED: {
      LogFlushedMessage * m_msg = (LogFlushedMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                      }
    case CALVIN_ACK:
    case RACK_PREP:
    case RACK_FIN: {
      AckMessage * m_msg = (AckMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                   }
    case CL_QRY:
    case RTXN:
    case RTXN_CONT: {
#if WORKLOAD == YCSB
      YCSBClientQueryMessage * m_msg = (YCSBClientQueryMessage*)msg;
#elif WORKLOAD == TPCC 
      TPCCClientQueryMessage * m_msg = (TPCCClientQueryMessage*)msg;
#elif WORKLOAD == PPS 
      PPSClientQueryMessage * m_msg = (PPSClientQueryMessage*)msg;
#endif
      m_msg->release();
      delete m_msg;
      break;
                    }
    case RPREPARE: {
      PrepareMessage * m_msg = (PrepareMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                   }
    case RLIFE_EXECUTE: {
      LifeExecuteMessage *m_msg = (LifeExecuteMessage *)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case RLIFE_EXECUTE_RSP: {
      LifeExecuteResponseMessage *m_msg = (LifeExecuteResponseMessage *)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case RLIFE_PREPARE: {
      LifePrepareMessage *m_msg = (LifePrepareMessage *)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case RLIFE_PREPARE_RSP: {
      LifePrepareResponseMessage *m_msg = (LifePrepareResponseMessage *)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case RLIFE_FINISH: {
      LifeFinishMessage *m_msg = (LifeFinishMessage *)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case RLIFE_FINISH_RSP: {
      LifeFinishResponseMessage *m_msg = (LifeFinishResponseMessage *)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case RLIFE_FINALIZE_RSP: {
      LifeFinalizeResponseMessage *m_msg =
          (LifeFinalizeResponseMessage *)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case RLIFE_HELP: {
      LifeHelpMessage *m_msg = (LifeHelpMessage *)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case RLIFE_HELP_APPLY: {
      LifeHelpApplyMessage *m_msg = (LifeHelpApplyMessage *)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case RLIFE_FINALIZE: {
      LifeFinalizeMessage *m_msg = (LifeFinalizeMessage *)msg;
      m_msg->release();
      delete m_msg;
      break;
    }
    case RFWD: {
      ForwardMessage * m_msg = (ForwardMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
               }
    case RDONE: {
      DoneMessage * m_msg = (DoneMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                }
    case CL_RSP: {
      ClientResponseMessage * m_msg = (ClientResponseMessage*)msg;
      m_msg->release();
      delete m_msg;
      break;
                 }
    default: { assert(false); }
  }
}
/************************/

uint64_t QueryMessage::get_size() {
  uint64_t size = Message::mget_size();
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC
  size += sizeof(ts);
#endif
#if CC_ALG == OCC 
  size += sizeof(start_ts);
#endif  
  return size;
}

void QueryMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC
  ts = txn->get_timestamp();
  assert(ts != 0);
#endif
#if CC_ALG == OCC 
  start_ts = txn->get_start_timestamp();
#endif
}

void QueryMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC
  assert(ts != 0);
  txn->set_timestamp(ts);
#endif
#if CC_ALG == OCC 
  txn->set_start_timestamp(start_ts);
#endif

}

void QueryMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr __attribute__ ((unused));
  ptr = Message::mget_size();
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC
 COPY_VAL(ts,buf,ptr);
  assert(ts != 0);
#endif
#if CC_ALG == OCC 
 COPY_VAL(start_ts,buf,ptr);
#endif
}

void QueryMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr __attribute__ ((unused));
  ptr = Message::mget_size();
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC
 COPY_BUF(buf,ts,ptr);
  assert(ts != 0);
#endif
#if CC_ALG == OCC 
 COPY_BUF(buf,start_ts,ptr);
#endif
}

/************************/

void YCSBClientQueryMessage::init() {
}

void YCSBClientQueryMessage::release() {
  ClientQueryMessage::release();
  // Freeing requests is the responsibility of txn at commit time
/*
  for(uint64_t i = 0; i < requests.size(); i++) {
    DEBUG_M("YCSBClientQueryMessage::release ycsb_request free\n");
    mem_allocator.free(requests[i],sizeof(ycsb_request));
  }
*/
  requests.release();
}

uint64_t YCSBClientQueryMessage::get_size() {
  uint64_t size = ClientQueryMessage::get_size();
  size += sizeof(size_t);
  size += sizeof(ycsb_request) * requests.size();
  return size;
}

void YCSBClientQueryMessage::copy_from_query(BaseQuery * query) {
  ClientQueryMessage::copy_from_query(query);
/*
  requests.init(g_req_per_query);
  for(uint64_t i = 0; i < ((YCSBQuery*)(query))->requests.size(); i++) {
      YCSBQuery::copy_request_to_msg(((YCSBQuery*)(query)),this,i);
  }
*/
  requests.copy(((YCSBQuery*)(query))->requests);
}


void YCSBClientQueryMessage::copy_from_txn(TxnManager * txn) {
  ClientQueryMessage::mcopy_from_txn(txn);
/*
  requests.init(g_req_per_query);
  for(uint64_t i = 0; i < ((YCSBQuery*)(txn->query))->requests.size(); i++) {
      YCSBQuery::copy_request_to_msg(((YCSBQuery*)(txn->query)),this,i);
  }
*/
  requests.copy(((YCSBQuery*)(txn->query))->requests);
}

void YCSBClientQueryMessage::copy_to_txn(TxnManager * txn) {
  // this only copies over the pointers, so if requests are freed, we'll lose the request data
  ClientQueryMessage::copy_to_txn(txn);
  // Copies pointers to txn
  ((YCSBQuery*)(txn->query))->requests.append(requests);
/*
  for(uint64_t i = 0; i < requests.size(); i++) {
      YCSBQuery::copy_request_to_qry(((YCSBQuery*)(txn->query)),this,i);
  }
*/
}

void YCSBClientQueryMessage::copy_from_buf(char * buf) {
  ClientQueryMessage::copy_from_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();
  size_t size;
  //DEBUG("1YCSBClientQuery %ld\n",ptr);
  COPY_VAL(size,buf,ptr);
  requests.init(size);
  //DEBUG("2YCSBClientQuery %ld\n",ptr);
  for(uint64_t i = 0 ; i < size;i++) {
    DEBUG_M("YCSBClientQueryMessage::copy ycsb_request alloc\n");
    ycsb_request * req = (ycsb_request*)mem_allocator.alloc(sizeof(ycsb_request));
    COPY_VAL(*req,buf,ptr);
    //DEBUG("3YCSBClientQuery %ld\n",ptr);
    assert(req->key < g_synth_table_size);
    requests.add(req);
  }
 assert(ptr == get_size());
}

void YCSBClientQueryMessage::copy_to_buf(char * buf) {
  ClientQueryMessage::copy_to_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();
  //DEBUG("1YCSBClientQuery %ld\n",ptr);
  size_t size = requests.size();
  COPY_BUF(buf,size,ptr);
  //DEBUG("2YCSBClientQuery %ld\n",ptr);
  for(uint64_t i = 0; i < requests.size(); i++) {
    ycsb_request * req = requests[i];
    assert(req->key < g_synth_table_size);
    COPY_BUF(buf,*req,ptr);
    //DEBUG("3YCSBClientQuery %ld\n",ptr);
  }
 assert(ptr == get_size());
}
/************************/

void TPCCClientQueryMessage::init() {
}

void TPCCClientQueryMessage::release() {
  ClientQueryMessage::release();
  // Freeing requests is the responsibility of txn
  /*
  for(uint64_t i = 0; i < items.size(); i++) {
    DEBUG_M("TPCCClientQueryMessage::release item free\n");
    mem_allocator.free(items[i],sizeof(Item_no));
  }
  */
  items.release();
}

uint64_t TPCCClientQueryMessage::get_size() {
  uint64_t size = ClientQueryMessage::get_size();
  size += sizeof(uint64_t) * 10; 
  size += sizeof(char) * LASTNAME_LEN; 
  size += sizeof(bool) * 3;
  size += sizeof(size_t);
  size += sizeof(Item_no) * items.size();
  return size;
}

void TPCCClientQueryMessage::copy_from_query(BaseQuery * query) {
  ClientQueryMessage::copy_from_query(query);
  TPCCQuery* tpcc_query = (TPCCQuery*)(query);
  
  txn_type = tpcc_query->txn_type;
	// common txn input for both payment & new-order
  w_id = tpcc_query->w_id;
  d_id = tpcc_query->d_id;
  c_id = tpcc_query->c_id;

  // payment
  d_w_id = tpcc_query->d_w_id;
  c_w_id = tpcc_query->c_w_id;
  c_d_id = tpcc_query->c_d_id;
  strcpy(c_last,tpcc_query->c_last);
  h_amount = tpcc_query->h_amount;
  by_last_name = tpcc_query->by_last_name;

  // new order
  items.copy(tpcc_query->items);
  rbk = tpcc_query->rbk;
  remote = tpcc_query->remote;
  ol_cnt = tpcc_query->ol_cnt;
  o_entry_d = tpcc_query->o_entry_d;
}


void TPCCClientQueryMessage::copy_from_txn(TxnManager * txn) {
  ClientQueryMessage::mcopy_from_txn(txn);
  copy_from_query(txn->query);
}

void TPCCClientQueryMessage::copy_to_txn(TxnManager * txn) {
  ClientQueryMessage::copy_to_txn(txn);
  TPCCQuery* tpcc_query = (TPCCQuery*)(txn->query);

  txn->client_id = return_node_id;


  tpcc_query->txn_type = (TPCCTxnType)txn_type;
  if(tpcc_query->txn_type == TPCC_PAYMENT)
    ((TPCCTxnManager*)txn)->state = TPCC_PAYMENT0;
  else if (tpcc_query->txn_type == TPCC_NEW_ORDER) 
    ((TPCCTxnManager*)txn)->state = TPCC_NEWORDER0;
	// common txn input for both payment & new-order
  tpcc_query->w_id = w_id;
  tpcc_query->d_id = d_id;
  tpcc_query->c_id = c_id;

  // payment
  tpcc_query->d_w_id = d_w_id;
  tpcc_query->c_w_id = c_w_id;
  tpcc_query->c_d_id = c_d_id;
  strcpy(tpcc_query->c_last,c_last);
  tpcc_query->h_amount = h_amount;
  tpcc_query->by_last_name = by_last_name;

  // new order
  tpcc_query->items.append(items);
  tpcc_query->rbk = rbk;
  tpcc_query->remote = remote;
  tpcc_query->ol_cnt = ol_cnt;
  tpcc_query->o_entry_d = o_entry_d;

}

void TPCCClientQueryMessage::copy_from_buf(char * buf) {
  ClientQueryMessage::copy_from_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();

  COPY_VAL(txn_type,buf,ptr); 
	// common txn input for both payment & new-order
  COPY_VAL(w_id,buf,ptr);
  COPY_VAL(d_id,buf,ptr);
  COPY_VAL(c_id,buf,ptr);

  // payment
  COPY_VAL(d_w_id,buf,ptr);
  COPY_VAL(c_w_id,buf,ptr);
  COPY_VAL(c_d_id,buf,ptr);
	COPY_VAL(c_last,buf,ptr);
  COPY_VAL(h_amount,buf,ptr);
  COPY_VAL(by_last_name,buf,ptr);

  // new order
  size_t size;
  COPY_VAL(size,buf,ptr);
  items.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    DEBUG_M("TPCCClientQueryMessage::copy_from_buf item alloc\n");
    Item_no * item = (Item_no*)mem_allocator.alloc(sizeof(Item_no));
    COPY_VAL(*item,buf,ptr);
    items.add(item);
  }

  COPY_VAL(rbk,buf,ptr);
  COPY_VAL(remote,buf,ptr);
  COPY_VAL(ol_cnt,buf,ptr);
  COPY_VAL(o_entry_d,buf,ptr);

 assert(ptr == get_size());
}

void TPCCClientQueryMessage::copy_to_buf(char * buf) {
  ClientQueryMessage::copy_to_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();

  COPY_BUF(buf,txn_type,ptr); 
	// common txn input for both payment & new-order
  COPY_BUF(buf,w_id,ptr);
  COPY_BUF(buf,d_id,ptr);
  COPY_BUF(buf,c_id,ptr);

  // payment
  COPY_BUF(buf,d_w_id,ptr);
  COPY_BUF(buf,c_w_id,ptr);
  COPY_BUF(buf,c_d_id,ptr);
	COPY_BUF(buf,c_last,ptr);
  COPY_BUF(buf,h_amount,ptr);
  COPY_BUF(buf,by_last_name,ptr);

  size_t size = items.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < items.size(); i++) {
    Item_no * item = items[i];
    COPY_BUF(buf,*item,ptr);
  }

  COPY_BUF(buf,rbk,ptr);
  COPY_BUF(buf,remote,ptr);
  COPY_BUF(buf,ol_cnt,ptr);
  COPY_BUF(buf,o_entry_d,ptr);
 assert(ptr == get_size());
}


/************************/


/************************/

void PPSClientQueryMessage::init() {
}

void PPSClientQueryMessage::release() {
  ClientQueryMessage::release();
}

uint64_t PPSClientQueryMessage::get_size() {
  uint64_t size = ClientQueryMessage::get_size();
  size += sizeof(uint64_t);
  size += sizeof(uint64_t)*3; 
  size += sizeof(size_t);
  size += sizeof(uint64_t) * part_keys.size();
#if CC_ALG == CALVIN
  size += sizeof(bool);
#endif
  return size;
}

void PPSClientQueryMessage::copy_from_query(BaseQuery * query) {
  ClientQueryMessage::copy_from_query(query);
  PPSQuery* pps_query = (PPSQuery*)(query);
  
  txn_type = pps_query->txn_type;

  part_key = pps_query->part_key;
  product_key = pps_query->product_key;
  supplier_key = pps_query->supplier_key;

  part_keys.copy(pps_query->part_keys);

}


void PPSClientQueryMessage::copy_from_txn(TxnManager * txn) {
  ClientQueryMessage::mcopy_from_txn(txn);
  copy_from_query(txn->query);
#if CC_ALG == CALVIN
  recon = txn->isRecon();
#endif
}

void PPSClientQueryMessage::copy_to_txn(TxnManager * txn) {
  ClientQueryMessage::copy_to_txn(txn);
  PPSQuery* pps_query = (PPSQuery*)(txn->query);

  txn->client_id = return_node_id;
  pps_query->txn_type = (PPSTxnType)txn_type;
  if(pps_query->txn_type == PPS_GETPART)
    ((PPSTxnManager*)txn)->state = PPS_GETPART0;
  else if(pps_query->txn_type == PPS_GETPRODUCT)
    ((PPSTxnManager*)txn)->state = PPS_GETPRODUCT0;
  else if(pps_query->txn_type == PPS_GETSUPPLIER)
    ((PPSTxnManager*)txn)->state = PPS_GETSUPPLIER0;
  else if(pps_query->txn_type == PPS_GETPARTBYPRODUCT)
    ((PPSTxnManager*)txn)->state = PPS_GETPARTBYPRODUCT0;
  else if(pps_query->txn_type == PPS_GETPARTBYSUPPLIER)
    ((PPSTxnManager*)txn)->state = PPS_GETPARTBYSUPPLIER0;
  else if(pps_query->txn_type == PPS_ORDERPRODUCT)
    ((PPSTxnManager*)txn)->state = PPS_ORDERPRODUCT0;
  else if(pps_query->txn_type == PPS_UPDATEPRODUCTPART)
    ((PPSTxnManager*)txn)->state = PPS_UPDATEPRODUCTPART0;
  else if(pps_query->txn_type == PPS_UPDATEPART)
    ((PPSTxnManager*)txn)->state = PPS_UPDATEPART0;
  pps_query->part_key = part_key;
  pps_query->product_key = product_key;
  pps_query->supplier_key = supplier_key;
  pps_query->part_keys.append(part_keys);

#if CC_ALG == CALVIN
  txn->recon = recon;
#endif
#if DEBUG_DISTR
  std::cout << "PPSClient::copy_to_txn "
    << "type " << (PPSTxnType)txn_type
    << " part_key " << part_key
    << " product_key " << product_key
    << " supplier_key " << supplier_key
    << std::endl;
#endif

}

void PPSClientQueryMessage::copy_from_buf(char * buf) {
  ClientQueryMessage::copy_from_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();

  COPY_VAL(txn_type,buf,ptr); 
	// common txn input for both payment & new-order
  COPY_VAL(part_key,buf,ptr);
  COPY_VAL(product_key,buf,ptr);
  COPY_VAL(supplier_key,buf,ptr);

  size_t size;
  COPY_VAL(size,buf,ptr);
  part_keys.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    uint64_t item;
    COPY_VAL(item,buf,ptr);
    part_keys.add(item);
  }

#if CC_ALG == CALVIN
  COPY_VAL(recon,buf,ptr);
#endif

 assert(ptr == get_size());
#if DEBUG_DISTR
  std::cout << "PPSClient::copy_from_buf "
    << "type " << (PPSTxnType)txn_type
    << " part_key " << part_key
    << " product_key " << product_key
    << " supplier_key " << supplier_key
    << std::endl;
#endif
}

void PPSClientQueryMessage::copy_to_buf(char * buf) {
  ClientQueryMessage::copy_to_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();

  COPY_BUF(buf,txn_type,ptr); 
	// common txn input for both payment & new-order
  COPY_BUF(buf,part_key,ptr);
  COPY_BUF(buf,product_key,ptr);
  COPY_BUF(buf,supplier_key,ptr);

  size_t size = part_keys.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < part_keys.size(); i++) {
    uint64_t item = part_keys[i];
    COPY_BUF(buf,item,ptr);
  }

#if CC_ALG == CALVIN
  COPY_BUF(buf,recon,ptr);
#endif

 assert(ptr == get_size());
#if DEBUG_DISTR
  std::cout << "PPSClient::copy_to_buf "
    << "type " << (PPSTxnType)txn_type
    << " part_key " << part_key
    << " product_key " << product_key
    << " supplier_key " << supplier_key
    << std::endl;
#endif
}


/************************/

void ClientQueryMessage::init() {
    first_startts = 0;
}

void ClientQueryMessage::release() {
  partitions.release();
  first_startts = 0;
}

uint64_t ClientQueryMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(client_startts);
  /*
  uint64_t size = sizeof(ClientQueryMessage);
  */
  size += sizeof(size_t);
  size += sizeof(uint64_t) * partitions.size();
  return size;
}

void ClientQueryMessage::copy_from_query(BaseQuery * query) {
  partitions.clear();
  partitions.copy(query->partitions);
}

void ClientQueryMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  //ts = txn->txn->timestamp;
  partitions.clear();
  partitions.copy(txn->query->partitions);
  client_startts = txn->client_startts;
}

void ClientQueryMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  //txn->txn->timestamp = ts;
  txn->query->partitions.clear();
  txn->query->partitions.append(partitions);
  txn->client_startts = client_startts;
  txn->client_id = return_node_id;
}

void ClientQueryMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  //COPY_VAL(ts,buf,ptr);
  COPY_VAL(client_startts,buf,ptr);
  size_t size;
  COPY_VAL(size,buf,ptr);
  partitions.init(size);
  for(uint64_t i = 0; i < size; i++) {
    //COPY_VAL(partitions[i],buf,ptr);
    uint64_t part;
    COPY_VAL(part,buf,ptr);
    partitions.add(part);
  }
}

void ClientQueryMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  //COPY_BUF(buf,ts,ptr);
  COPY_BUF(buf,client_startts,ptr);
  size_t size = partitions.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < size; i++) {
    uint64_t part = partitions[i];
    COPY_BUF(buf,part,ptr);
  }
}

/************************/


uint64_t ClientResponseMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(uint64_t);
  return size;
}

void ClientResponseMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  client_startts = txn->client_startts;
}

void ClientResponseMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  txn->client_startts = client_startts;
}

void ClientResponseMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(client_startts,buf,ptr);
 assert(ptr == get_size());
}

void ClientResponseMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,client_startts,ptr);
 assert(ptr == get_size());
}

/************************/


uint64_t DoneMessage::get_size() {
  uint64_t size = Message::mget_size();
  return size;
}

void DoneMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
}

void DoneMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
}

void DoneMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
 assert(ptr == get_size());
}

void DoneMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
 assert(ptr == get_size());
}

/************************/


uint64_t ForwardMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(RC);
#if WORKLOAD == TPCC
	size += sizeof(uint64_t);
#endif
  return size;
}

void ForwardMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  rc = txn->get_rc();
#if WORKLOAD == TPCC
  o_id = ((TPCCQuery*)txn->query)->o_id;
#endif
}

void ForwardMessage::copy_to_txn(TxnManager * txn) {
  // Don't copy return ID
  //Message::mcopy_to_txn(txn);
#if WORKLOAD == TPCC
  ((TPCCQuery*)txn->query)->o_id = o_id;
#endif
}

void ForwardMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(rc,buf,ptr);
#if WORKLOAD == TPCC
  COPY_VAL(o_id,buf,ptr);
#endif
 assert(ptr == get_size());
}

void ForwardMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,rc,ptr);
#if WORKLOAD == TPCC
  COPY_BUF(buf,o_id,ptr);
#endif
 assert(ptr == get_size());
}

/************************/

uint64_t PrepareMessage::get_size() {
  uint64_t size = Message::mget_size();
  //size += sizeof(uint64_t);
  return size;
}

void PrepareMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
}

void PrepareMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
}

void PrepareMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
 assert(ptr == get_size());
}

void PrepareMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
 assert(ptr == get_size());
}

/************************/

uint64_t LifeExecuteMessage::get_size() {
  return Message::mget_size() + life_descriptor_size(descriptor) +
         life_operation_size(operation) + sizeof(wait_id);
}

void LifeExecuteMessage::copy_from_txn(TxnManager *txn) {
  (void)txn;
}

void LifeExecuteMessage::copy_to_txn(TxnManager *txn) {
  Message::mcopy_to_txn(txn);
}

void LifeExecuteMessage::copy_from_buf(char *buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_read_descriptor(buf, ptr, descriptor);
  life_read_operation(buf, ptr, operation);
  COPY_VAL(wait_id, buf, ptr);
  assert(ptr == get_size());
}

void LifeExecuteMessage::copy_to_buf(char *buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_write_descriptor(buf, ptr, descriptor);
  life_write_operation(buf, ptr, operation);
  COPY_BUF(buf, wait_id, ptr);
  assert(ptr == get_size());
}

uint64_t LifeExecuteResponseMessage::get_size() {
  return Message::mget_size() + sizeof(wait_id) + life_result_size(result);
}

void LifeExecuteResponseMessage::copy_from_txn(TxnManager *txn) {
  (void)txn;
}

void LifeExecuteResponseMessage::copy_to_txn(TxnManager *txn) {
  Message::mcopy_to_txn(txn);
}

void LifeExecuteResponseMessage::copy_from_buf(char *buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(wait_id, buf, ptr);
  life_read_result(buf, ptr, result);
  assert(ptr == get_size());
}

void LifeExecuteResponseMessage::copy_to_buf(char *buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf, wait_id, ptr);
  life_write_result(buf, ptr, result);
  assert(ptr == get_size());
}

uint64_t LifePrepareMessage::get_size() {
  return Message::mget_size() + life_descriptor_size(descriptor);
}

void LifePrepareMessage::copy_from_txn(TxnManager *txn) {
  (void)txn;
}

void LifePrepareMessage::copy_to_txn(TxnManager *txn) {
  Message::mcopy_to_txn(txn);
}

void LifePrepareMessage::copy_from_buf(char *buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_read_descriptor(buf, ptr, descriptor);
  assert(ptr == get_size());
}

void LifePrepareMessage::copy_to_buf(char *buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_write_descriptor(buf, ptr, descriptor);
  assert(ptr == get_size());
}

uint64_t LifePrepareResponseMessage::get_size() {
  return Message::mget_size() + sizeof(uint32_t) + sizeof(uint64_t);
}

void LifePrepareResponseMessage::copy_from_txn(TxnManager *txn) {
  (void)txn;
}

void LifePrepareResponseMessage::copy_to_txn(TxnManager *txn) {
  Message::mcopy_to_txn(txn);
}

void LifePrepareResponseMessage::copy_from_buf(char *buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  uint32_t code;
  COPY_VAL(code, buf, ptr);
  result = LifeExecuteResult();
  result.code = static_cast<LifeResultCode>(code);
  COPY_VAL(result.observed_attempt, buf, ptr);
  assert(ptr == get_size());
}

void LifePrepareResponseMessage::copy_to_buf(char *buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  const uint32_t code = static_cast<uint32_t>(result.code);
  COPY_BUF(buf, code, ptr);
  COPY_BUF(buf, result.observed_attempt, ptr);
  assert(ptr == get_size());
}

uint64_t LifeHelpMessage::get_size() {
  return Message::mget_size() + life_descriptor_size(descriptor);
}

void LifeHelpMessage::copy_from_txn(TxnManager *txn) {
  (void)txn;
}

void LifeHelpMessage::copy_to_txn(TxnManager *txn) {
  Message::mcopy_to_txn(txn);
}

void LifeHelpMessage::copy_from_buf(char *buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_read_descriptor(buf, ptr, descriptor);
  assert(ptr == get_size());
}

void LifeHelpMessage::copy_to_buf(char *buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_write_descriptor(buf, ptr, descriptor);
  assert(ptr == get_size());
}

uint64_t LifeHelpApplyMessage::get_size() {
  return Message::mget_size() + life_descriptor_size(descriptor);
}

void LifeHelpApplyMessage::copy_from_txn(TxnManager *txn) {
  (void)txn;
}

void LifeHelpApplyMessage::copy_to_txn(TxnManager *txn) {
  Message::mcopy_to_txn(txn);
}

void LifeHelpApplyMessage::copy_from_buf(char *buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_read_descriptor(buf, ptr, descriptor);
  assert(ptr == get_size());
}

void LifeHelpApplyMessage::copy_to_buf(char *buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_write_descriptor(buf, ptr, descriptor);
  assert(ptr == get_size());
}

uint64_t LifeFinalizeMessage::get_size() {
  return Message::mget_size() + sizeof(requester_txn_id) +
         life_descriptor_size(descriptor);
}

void LifeFinalizeMessage::copy_from_txn(TxnManager *txn) {
  (void)txn;
}

void LifeFinalizeMessage::copy_to_txn(TxnManager *txn) {
  Message::mcopy_to_txn(txn);
}

void LifeFinalizeMessage::copy_from_buf(char *buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(requester_txn_id, buf, ptr);
  life_read_descriptor(buf, ptr, descriptor);
  assert(ptr == get_size());
}

void LifeFinalizeMessage::copy_to_buf(char *buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf, requester_txn_id, ptr);
  life_write_descriptor(buf, ptr, descriptor);
  assert(ptr == get_size());
}

uint64_t LifeFinalizeResponseMessage::get_size() {
  return Message::mget_size() + life_result_size(result);
}

void LifeFinalizeResponseMessage::copy_from_txn(TxnManager *txn) {
  (void)txn;
}

void LifeFinalizeResponseMessage::copy_to_txn(TxnManager *txn) {
  Message::mcopy_to_txn(txn);
}

void LifeFinalizeResponseMessage::copy_from_buf(char *buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_read_result(buf, ptr, result);
  assert(ptr == get_size());
}

void LifeFinalizeResponseMessage::copy_to_buf(char *buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_write_result(buf, ptr, result);
  assert(ptr == get_size());
}

uint64_t LifeFinishMessage::get_size() {
  return Message::mget_size() + life_descriptor_size(descriptor) + sizeof(RC);
}

void LifeFinishMessage::copy_from_txn(TxnManager *txn) {
  (void)txn;
}

void LifeFinishMessage::copy_to_txn(TxnManager *txn) {
  Message::mcopy_to_txn(txn);
}

void LifeFinishMessage::copy_from_buf(char *buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_read_descriptor(buf, ptr, descriptor);
  COPY_VAL(decision, buf, ptr);
  assert(ptr == get_size());
}

void LifeFinishMessage::copy_to_buf(char *buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  life_write_descriptor(buf, ptr, descriptor);
  COPY_BUF(buf, decision, ptr);
  assert(ptr == get_size());
}

uint64_t LifeFinishResponseMessage::get_size() {
  return Message::mget_size() + sizeof(uint32_t);
}

void LifeFinishResponseMessage::copy_from_txn(TxnManager *txn) {
  (void)txn;
}

void LifeFinishResponseMessage::copy_to_txn(TxnManager *txn) {
  Message::mcopy_to_txn(txn);
}

void LifeFinishResponseMessage::copy_from_buf(char *buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  uint32_t code;
  COPY_VAL(code, buf, ptr);
  result = LifeExecuteResult();
  result.code = static_cast<LifeResultCode>(code);
  assert(ptr == get_size());
}

void LifeFinishResponseMessage::copy_to_buf(char *buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  const uint32_t code = static_cast<uint32_t>(result.code);
  COPY_BUF(buf, code, ptr);
  assert(ptr == get_size());
}

/************************/

uint64_t AckMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(RC);
#if CC_ALG == MAAT
  size += sizeof(uint64_t) * 2;
#endif
#if WORKLOAD == PPS && CC_ALG == CALVIN
  size += sizeof(size_t);
  size += sizeof(uint64_t) * part_keys.size();
#endif
  return size;
}

void AckMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  //rc = query->rc;
  rc = txn->get_rc();
#if CC_ALG == MAAT
  lower = time_table.get_lower(txn->get_thd_id(),txn->get_txn_id());
  upper = time_table.get_upper(txn->get_thd_id(),txn->get_txn_id());
#endif
#if WORKLOAD == PPS && CC_ALG == CALVIN
  PPSQuery* pps_query = (PPSQuery*)(txn->query);
  part_keys.copy(pps_query->part_keys);
#endif
}

void AckMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  //query->rc = rc;
#if WORKLOAD == PPS && CC_ALG == CALVIN

  PPSQuery* pps_query = (PPSQuery*)(txn->query);
  pps_query->part_keys.append(part_keys);
#endif
}

void AckMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(rc,buf,ptr);
#if CC_ALG == MAAT
  COPY_VAL(lower,buf,ptr);
  COPY_VAL(upper,buf,ptr);
#endif
#if WORKLOAD == PPS && CC_ALG == CALVIN

  size_t size;
  COPY_VAL(size,buf,ptr);
  part_keys.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    uint64_t item;
    COPY_VAL(item,buf,ptr);
    part_keys.add(item);
  }
#endif
 assert(ptr == get_size());
}

void AckMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,rc,ptr);
#if CC_ALG == MAAT
  COPY_BUF(buf,lower,ptr);
  COPY_BUF(buf,upper,ptr);
#endif
#if WORKLOAD == PPS && CC_ALG == CALVIN

  size_t size = part_keys.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < part_keys.size(); i++) {
    uint64_t item = part_keys[i];
    COPY_BUF(buf,item,ptr);
  }
#endif
 assert(ptr == get_size());
}

/************************/

uint64_t QueryResponseMessage::get_size() {
  uint64_t size = Message::mget_size(); 
  size += sizeof(RC);
  //size += sizeof(uint64_t);
  return size;
}

void QueryResponseMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  rc = txn->get_rc();

}

void QueryResponseMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  //query->rc = rc;

}

void QueryResponseMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(rc,buf,ptr);

 assert(ptr == get_size());
}

void QueryResponseMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,rc,ptr);
 assert(ptr == get_size());
}

/************************/



uint64_t FinishMessage::get_size() {
  uint64_t size = Message::mget_size();
  size += sizeof(uint64_t); 
  size += sizeof(RC); 
  size += sizeof(bool); 
#if CC_ALG == MAAT
  size += sizeof(uint64_t); 
#endif
  return size;
}

void FinishMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  rc = txn->get_rc();
  readonly = txn->query->readonly();
#if CC_ALG == MAAT
  commit_timestamp = txn->get_commit_timestamp();
#endif
}

void FinishMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
#if CC_ALG == MAAT
  txn->commit_timestamp = commit_timestamp;
#endif
}

void FinishMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(pid,buf,ptr);
  COPY_VAL(rc,buf,ptr);
  COPY_VAL(readonly,buf,ptr);
#if CC_ALG == MAAT
  COPY_VAL(commit_timestamp,buf,ptr);
#endif
 assert(ptr == get_size());
}

void FinishMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,pid,ptr);
  COPY_BUF(buf,rc,ptr);
  COPY_BUF(buf,readonly,ptr);
#if CC_ALG == MAAT
  COPY_BUF(buf,commit_timestamp,ptr);
#endif
 assert(ptr == get_size());
}

/************************/

void LogMessage::release() {
  //log_records.release();
}

uint64_t LogMessage::get_size() {
  uint64_t size = Message::mget_size();
  //size += sizeof(size_t);
  //size += sizeof(LogRecord) * log_records.size();
  return size;
}

void LogMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
}

void LogMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
}

void LogMessage::copy_from_record(LogRecord * record) {
  this->record.copyRecord(record);
  
}


void LogMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(record,buf,ptr);
 assert(ptr == get_size());
}

void LogMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,record,ptr);
 assert(ptr == get_size());
}

/************************/

uint64_t LogRspMessage::get_size() {
  uint64_t size = Message::mget_size();
  return size;
}

void LogRspMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
}

void LogRspMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
}

void LogRspMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  //uint64_t ptr = Message::mget_size();
}

void LogRspMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  //uint64_t ptr = Message::mget_size();
}



/************************/

uint64_t InitDoneMessage::get_size() {
  uint64_t size = Message::mget_size();
  return size;
}

void InitDoneMessage::copy_from_txn(TxnManager * txn) {
}

void InitDoneMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
}

void InitDoneMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
}

void InitDoneMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
}

/************************/

void YCSBQueryMessage::init() {
}

void YCSBQueryMessage::release() {
  QueryMessage::release();
  // Freeing requests is the responsibility of txn
/*
  for(uint64_t i = 0; i < requests.size(); i++) {
    DEBUG_M("YCSBQueryMessage::release ycsb_request free\n");
    mem_allocator.free(requests[i],sizeof(ycsb_request));
  }
*/
  requests.release();
}

uint64_t YCSBQueryMessage::get_size() {
  uint64_t size = QueryMessage::get_size();
  size += sizeof(size_t);
  size += sizeof(ycsb_request) * requests.size();
  return size;
}

void YCSBQueryMessage::copy_from_txn(TxnManager * txn) {
  QueryMessage::copy_from_txn(txn);
  requests.init(g_req_per_query);
  ((YCSBTxnManager*)txn)->copy_remote_requests(this);
  //requests.copy(((YCSBQuery*)(txn->query))->requests);
}

void YCSBQueryMessage::copy_to_txn(TxnManager * txn) {
  QueryMessage::copy_to_txn(txn);
  //((YCSBQuery*)(txn->query))->requests.copy(requests);
  ((YCSBQuery*)(txn->query))->requests.append(requests);
}


void YCSBQueryMessage::copy_from_buf(char * buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();
  size_t size;
  COPY_VAL(size,buf,ptr);
  assert(size<=g_req_per_query);
  requests.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    DEBUG_M("YCSBQueryMessage::copy ycsb_request alloc\n");
    ycsb_request * req = (ycsb_request*)mem_allocator.alloc(sizeof(ycsb_request));
    COPY_VAL(*req,buf,ptr);
    ASSERT(req->key < g_synth_table_size);
    requests.add(req);
  }
 assert(ptr == get_size());
}

void YCSBQueryMessage::copy_to_buf(char * buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = QueryMessage::get_size();
  size_t size = requests.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < requests.size(); i++) {
    ycsb_request * req = requests[i];
    COPY_BUF(buf,*req,ptr);
  }
 assert(ptr == get_size());
}
/************************/

void TPCCQueryMessage::init() {
}

void TPCCQueryMessage::release() {
  QueryMessage::release();
  // Freeing items is the responsibility of txn
  /*
  for(uint64_t i = 0; i < items.size(); i++) {
    DEBUG_M("TPCCQueryMessage::release item free\n");
    mem_allocator.free(items[i],sizeof(Item_no));
  }
  */
  items.release();
}

uint64_t TPCCQueryMessage::get_size() {
  uint64_t size = QueryMessage::get_size();

  size += sizeof(uint64_t); //txn_type
  size += sizeof(uint64_t); //state
  size += sizeof(uint64_t) * 3; // w_id, d_id, c_id

  // Payment
  if(txn_type == TPCC_PAYMENT) {
  
    size += sizeof(uint64_t) * 4; // d_w_id, c_w_id, c_d_id;, h_amount
    size += sizeof(char) * LASTNAME_LEN; // c_last[LASTNAME_LEN]
    size += sizeof(bool); // by_last_name

  }

  // New Order
  if(txn_type == TPCC_NEW_ORDER) {
    size += sizeof(uint64_t) * 2; // ol_cnt, o_entry_d,
    size += sizeof(bool) * 2; // rbk, remote
    size += sizeof(Item_no) * items.size();
    size += sizeof(uint64_t); // items size
  }

  return size;
}

void TPCCQueryMessage::copy_from_txn(TxnManager * txn) {
  QueryMessage::copy_from_txn(txn);
  TPCCQuery* tpcc_query = (TPCCQuery*)(txn->query);
  
  txn_type = tpcc_query->txn_type;
  state = (uint64_t)((TPCCTxnManager*)txn)->state;
	// common txn input for both payment & new-order
  w_id = tpcc_query->w_id;
  d_id = tpcc_query->d_id;
  c_id = tpcc_query->c_id;

  // payment
  if(txn_type == TPCC_PAYMENT) {
    d_w_id = tpcc_query->d_w_id;
    c_w_id = tpcc_query->c_w_id;
    c_d_id = tpcc_query->c_d_id;
    strcpy(c_last,tpcc_query->c_last);
    h_amount = tpcc_query->h_amount;
    by_last_name = tpcc_query->by_last_name;
  }

  // new order
  //items.copy(tpcc_query->items);
  if(txn_type == TPCC_NEW_ORDER) {
    ((TPCCTxnManager*)txn)->copy_remote_items(this);
    rbk = tpcc_query->rbk;
    remote = tpcc_query->remote;
    ol_cnt = tpcc_query->ol_cnt;
    o_entry_d = tpcc_query->o_entry_d;
  }

}

void TPCCQueryMessage::copy_to_txn(TxnManager * txn) {
  QueryMessage::copy_to_txn(txn);

  TPCCQuery* tpcc_query = (TPCCQuery*)(txn->query);

  tpcc_query->txn_type = (TPCCTxnType)txn_type;
  ((TPCCTxnManager*)txn)->state = (TPCCRemTxnType)state;
	// common txn input for both payment & new-order
  tpcc_query->w_id = w_id;
  tpcc_query->d_id = d_id;
  tpcc_query->c_id = c_id;

  // payment
  if(txn_type == TPCC_PAYMENT) {
    tpcc_query->d_w_id = d_w_id;
    tpcc_query->c_w_id = c_w_id;
    tpcc_query->c_d_id = c_d_id;
    strcpy(tpcc_query->c_last,c_last);
    tpcc_query->h_amount = h_amount;
    tpcc_query->by_last_name = by_last_name;
  }

  // new order
  if(txn_type == TPCC_NEW_ORDER) {
    tpcc_query->items.append(items);
    tpcc_query->rbk = rbk;
    tpcc_query->remote = remote;
    tpcc_query->ol_cnt = ol_cnt;
    tpcc_query->o_entry_d = o_entry_d;
  }


}


void TPCCQueryMessage::copy_from_buf(char * buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_VAL(txn_type,buf,ptr); 
  assert(txn_type == TPCC_PAYMENT || txn_type == TPCC_NEW_ORDER);
  COPY_VAL(state,buf,ptr); 
	// common txn input for both payment & new-order
  COPY_VAL(w_id,buf,ptr);
  COPY_VAL(d_id,buf,ptr);
  COPY_VAL(c_id,buf,ptr);

  // payment
  if(txn_type == TPCC_PAYMENT) {
    COPY_VAL(d_w_id,buf,ptr);
    COPY_VAL(c_w_id,buf,ptr);
    COPY_VAL(c_d_id,buf,ptr);
    COPY_VAL(c_last,buf,ptr);
    COPY_VAL(h_amount,buf,ptr);
    COPY_VAL(by_last_name,buf,ptr);
  }

  // new order
  if(txn_type == TPCC_NEW_ORDER) {
    size_t size;
    COPY_VAL(size,buf,ptr);
    items.init(size);
    for(uint64_t i = 0 ; i < size;i++) {
      DEBUG_M("TPCCQueryMessage::copy item alloc\n");
      Item_no * item = (Item_no*)mem_allocator.alloc(sizeof(Item_no));
      COPY_VAL(*item,buf,ptr);
      items.add(item);
    }

    COPY_VAL(rbk,buf,ptr);
    COPY_VAL(remote,buf,ptr);
    COPY_VAL(ol_cnt,buf,ptr);
    COPY_VAL(o_entry_d,buf,ptr);
  }

 assert(ptr == get_size());

}

void TPCCQueryMessage::copy_to_buf(char * buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_BUF(buf,txn_type,ptr); 
  COPY_BUF(buf,state,ptr); 
	// common txn input for both payment & new-order
  COPY_BUF(buf,w_id,ptr);
  COPY_BUF(buf,d_id,ptr);
  COPY_BUF(buf,c_id,ptr);

  // payment
  if(txn_type == TPCC_PAYMENT) {
    COPY_BUF(buf,d_w_id,ptr);
    COPY_BUF(buf,c_w_id,ptr);
    COPY_BUF(buf,c_d_id,ptr);
    COPY_BUF(buf,c_last,ptr);
    COPY_BUF(buf,h_amount,ptr);
    COPY_BUF(buf,by_last_name,ptr);
  }

  if(txn_type == TPCC_NEW_ORDER) {
    size_t size = items.size();
    COPY_BUF(buf,size,ptr);
    for(uint64_t i = 0; i < items.size(); i++) {
      Item_no * item = items[i];
      COPY_BUF(buf,*item,ptr);
    }

    COPY_BUF(buf,rbk,ptr);
    COPY_BUF(buf,remote,ptr);
    COPY_BUF(buf,ol_cnt,ptr);
    COPY_BUF(buf,o_entry_d,ptr);
  }
 assert(ptr == get_size());

}
/************************/

void PPSQueryMessage::init() {
}

void PPSQueryMessage::release() {
  QueryMessage::release();
}

uint64_t PPSQueryMessage::get_size() {
  uint64_t size = QueryMessage::get_size();

  size += sizeof(uint64_t); // txn_type
  size += sizeof(uint64_t); // state
  size += sizeof(uint64_t); // part/product/supply key 
  size += sizeof(size_t);
  size += sizeof(uint64_t) * part_keys.size();
  return size;
}

void PPSQueryMessage::copy_from_txn(TxnManager * txn) {
  QueryMessage::copy_from_txn(txn);
  PPSQuery* pps_query = (PPSQuery*)(txn->query);
  
  txn_type = pps_query->txn_type;
  state = (uint64_t)((PPSTxnManager*)txn)->state;

  if (txn_type == PPS_GETPART) {
    part_key = pps_query->part_key;
  }
  if (txn_type == PPS_GETPRODUCT) {
    product_key = pps_query->product_key;
  }
  if (txn_type == PPS_GETSUPPLIER) {
    supplier_key = pps_query->supplier_key;
  }
  if (txn_type == PPS_GETPARTBYPRODUCT) {
    //product_key = pps_query->product_key;
    part_key = pps_query->part_key;
  }
  if (txn_type == PPS_GETPARTBYSUPPLIER) {
    //supplier_key = pps_query->supplier_key;
    part_key = pps_query->part_key;
  }
  if (txn_type == PPS_ORDERPRODUCT) {
      part_key = pps_query->part_key;
  }
  if (txn_type == PPS_UPDATEPRODUCTPART) {
      product_key = pps_query->product_key;
  }
  if (txn_type == PPS_UPDATEPART) {
      part_key = pps_query->part_key;
  }

  part_keys.copy(pps_query->part_keys);

}

void PPSQueryMessage::copy_to_txn(TxnManager * txn) {
  QueryMessage::copy_to_txn(txn);

  PPSQuery* pps_query = (PPSQuery*)(txn->query);

  pps_query->txn_type = (PPSTxnType)txn_type;
  ((PPSTxnManager*)txn)->state = (PPSRemTxnType)state;

  if (txn_type == PPS_GETPART) {
    pps_query->part_key = part_key;
  }
  if (txn_type == PPS_GETPRODUCT) {
    pps_query->product_key = product_key;
  }
  if (txn_type == PPS_GETSUPPLIER) {
    pps_query->supplier_key = supplier_key;
  }
  if (txn_type == PPS_GETPARTBYPRODUCT) {
    //pps_query->product_key = product_key;
    pps_query->part_key = part_key;
  }
  if (txn_type == PPS_GETPARTBYSUPPLIER) {
    //pps_query->supplier_key = supplier_key;
    pps_query->part_key = part_key;
  }
  if (txn_type == PPS_ORDERPRODUCT) {
    //pps_query->product_key = product_key;
    pps_query->part_key = part_key;
  }
  if (txn_type == PPS_UPDATEPRODUCTPART) {
      pps_query->product_key = product_key;
  }
  if (txn_type == PPS_UPDATEPART) {
      pps_query->part_key = part_key;
  }
  pps_query->part_keys.append(part_keys);

}


void PPSQueryMessage::copy_from_buf(char * buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_VAL(txn_type,buf,ptr); 
  COPY_VAL(state,buf,ptr); 
  if (txn_type == PPS_GETPART) {
    COPY_VAL(part_key,buf,ptr); 
  }
  if (txn_type == PPS_GETPRODUCT) {
    COPY_VAL(product_key,buf,ptr); 
  }
  if (txn_type == PPS_GETSUPPLIER) {
    COPY_VAL(supplier_key,buf,ptr); 
  }
  if (txn_type == PPS_GETPARTBYPRODUCT) {
    //COPY_VAL(product_key,buf,ptr); 
    COPY_VAL(part_key,buf,ptr); 
  }
  if (txn_type == PPS_GETPARTBYSUPPLIER) {
    //COPY_VAL(supplier_key,buf,ptr); 
    COPY_VAL(part_key,buf,ptr); 
  }
  if (txn_type == PPS_ORDERPRODUCT) {
    //COPY_VAL(product_key,buf,ptr); 
    COPY_VAL(part_key,buf,ptr); 
  }
  if (txn_type == PPS_UPDATEPRODUCTPART) {
      COPY_VAL(product_key,buf,ptr);
  }
  if (txn_type == PPS_UPDATEPART) {
      COPY_VAL(part_key,buf,ptr);
  }

  size_t size;
  COPY_VAL(size,buf,ptr);
  part_keys.init(size);
  for(uint64_t i = 0 ; i < size;i++) {
    uint64_t item;
    COPY_VAL(item,buf,ptr);
    part_keys.add(item);
  }

 assert(ptr == get_size());

}

void PPSQueryMessage::copy_to_buf(char * buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_BUF(buf,txn_type,ptr); 
  COPY_BUF(buf,state,ptr); 

  if (txn_type == PPS_GETPART) {
    COPY_BUF(buf,part_key,ptr); 
  }
  if (txn_type == PPS_GETPRODUCT) {
    COPY_BUF(buf,product_key,ptr); 
  }
  if (txn_type == PPS_GETSUPPLIER) {
    COPY_BUF(buf,supplier_key,ptr); 
  }
  if (txn_type == PPS_GETPARTBYPRODUCT) {
    //COPY_BUF(buf,product_key,ptr); 
    COPY_BUF(buf,part_key,ptr); 
  }
  if (txn_type == PPS_GETPARTBYSUPPLIER) {
    //COPY_BUF(buf,supplier_key,ptr); 
    COPY_BUF(buf,part_key,ptr); 
  }
  if (txn_type == PPS_ORDERPRODUCT) {
    //COPY_BUF(buf,product_key,ptr); 
    COPY_BUF(buf,part_key,ptr); 
  }
  if (txn_type == PPS_UPDATEPRODUCTPART) {
    //COPY_BUF(buf,product_key,ptr);
    COPY_BUF(buf,product_key,ptr);
  }
  if (txn_type == PPS_UPDATEPART) {
    //COPY_BUF(buf,product_key,ptr);
    COPY_BUF(buf,part_key,ptr);
  }

  size_t size = part_keys.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < part_keys.size(); i++) {
    uint64_t item = part_keys[i];
    COPY_BUF(buf,item,ptr);
  }

 assert(ptr == get_size());

}

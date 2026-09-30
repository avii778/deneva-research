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

#include "work_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "message.h"
#include "client_query.h"
#include <boost/lockfree/queue.hpp>

#if CC_ALG == LIFE
#include "life_queue_policy.h"
#endif

void QWorkQueue::init() {

  last_sched_dq = NULL;
  sched_ptr = 0;
  seq_queue = new boost::lockfree::queue<work_queue_entry* > (0);
#if CC_ALG == LIFE
  life_old_dequeue_streak = new uint32_t[g_thread_cnt]();
#if LIFE_WAIT_QUEUE
  life_wait_prefer_protocol.reset(new uint8_t[g_thread_cnt]());
#endif
#endif
  // LIFE continuations deliberately share this queue. The transaction-table
  // ready claim, rather than queue affinity, serializes each durable manager.
  work_queue = new boost::lockfree::queue<work_queue_entry* > (0);
  new_txn_queue = new boost::lockfree::queue<work_queue_entry* >(0);
  sched_queue = new boost::lockfree::queue<work_queue_entry* > * [g_node_cnt];
  for ( uint64_t i = 0; i < g_node_cnt; i++) {
    sched_queue[i] = new boost::lockfree::queue<work_queue_entry* > (0);
  }

}

void QWorkQueue::sequencer_enqueue(uint64_t thd_id, Message * msg) {
  uint64_t starttime = get_sys_clock();
  assert(msg);
  DEBUG_M("SeqQueue::enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();
  assert(ISSERVER);

  DEBUG("Seq Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
  while(!seq_queue->push(entry) && !simulation->is_done()) {}

  INC_STATS(thd_id,seq_queue_enqueue_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,seq_queue_enq_cnt,1);

}

Message * QWorkQueue::sequencer_dequeue(uint64_t thd_id) {
  uint64_t starttime = get_sys_clock();
  assert(ISSERVER);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;
  bool valid = seq_queue->pop(entry);
  
  if(valid) {
    msg = entry->msg;
    assert(msg);
    DEBUG("Seq Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(thd_id,seq_queue_wait_time,queue_time);
    INC_STATS(thd_id,seq_queue_cnt,1);
    //DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
  DEBUG_M("SeqQueue::dequeue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));
    INC_STATS(thd_id,seq_queue_dequeue_time,get_sys_clock() - starttime);
  }

  return msg;

}

void QWorkQueue::sched_enqueue(uint64_t thd_id, Message * msg) {
  assert(CC_ALG == CALVIN || CC_ALG == HDCC);
  assert(msg);
  assert(ISSERVERN(msg->return_node_id));
  uint64_t starttime = get_sys_clock();

  DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();

  DEBUG("Sched Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
  uint64_t mtx_time_start = get_sys_clock();
  while(!sched_queue[msg->get_return_id()]->push(entry) && !simulation->is_done()) {}
  INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_time_start);

  INC_STATS(thd_id,sched_queue_enqueue_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,sched_queue_enq_cnt,1);
}

Message * QWorkQueue::sched_dequeue(uint64_t thd_id) {
  uint64_t starttime = get_sys_clock();

  assert(CC_ALG == CALVIN || CC_ALG == HDCC);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;

  bool valid = sched_queue[sched_ptr]->pop(entry);

  if(valid) {

    msg = entry->msg;
    DEBUG("Sched Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);

    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(thd_id,sched_queue_wait_time,queue_time);
    INC_STATS(thd_id,sched_queue_cnt,1);

    DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));

    if(msg->rtype == RDONE) {
      // Advance to next queue or next epoch
      DEBUG("Sched RDONE %ld %ld\n",sched_ptr,simulation->get_worker_epoch());
      assert(msg->get_batch_id() == simulation->get_worker_epoch());
      if(sched_ptr == g_node_cnt - 1) {
        INC_STATS(thd_id,sched_epoch_cnt,1);
        INC_STATS(thd_id,sched_epoch_diff,get_sys_clock()-simulation->last_worker_epoch_time);
        simulation->next_worker_epoch();
      }
      sched_ptr = (sched_ptr + 1) % g_node_cnt;
      msg->release();
      msg = NULL;

    } else {
      simulation->inc_epoch_txn_cnt();
      DEBUG("Sched msg dequeue %ld (%ld,%ld) %ld\n",sched_ptr,msg->txn_id,msg->batch_id,simulation->get_worker_epoch());
      assert(msg->batch_id == simulation->get_worker_epoch());
    }

    INC_STATS(thd_id,sched_queue_dequeue_time,get_sys_clock() - starttime);
  }


  return msg;

}


void QWorkQueue::enqueue(uint64_t thd_id, Message * msg,bool busy) {
  uint64_t starttime = get_sys_clock();
  assert(msg);
  DEBUG_M("QWorkQueue::enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();
  assert(ISSERVER || ISREPLICA);
  DEBUG("Work Enqueue (%ld,%ld) %d\n",entry->txn_id,entry->batch_id,entry->rtype);

  uint64_t mtx_wait_starttime = get_sys_clock();
  if(msg->rtype == CL_QRY) {
    while(!new_txn_queue->push(entry) && !simulation->is_done()) {}
  } else {
#if CC_ALG == LIFE
    assert(msg->txn_id != UINT64_MAX);
#endif
    while(!work_queue->push(entry) && !simulation->is_done()) {}
  }
  INC_STATS(thd_id,mtx[13],get_sys_clock() - mtx_wait_starttime);

  if(busy) {
    INC_STATS(thd_id,work_queue_conflict_cnt,1);
  }
  INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,work_queue_enq_cnt,1);
}

#if CC_ALG == LIFE && LIFE_WAIT_QUEUE
void QWorkQueue::life_wait_enqueue(uintptr_t row, uint64_t token, Message *msg,
                                   bool completion_raced) {
  life_wait_admitted.fetch_add(1, std::memory_order_relaxed);
  life_wait_queue.push(row, token, std::unique_ptr<Message>(msg), life_wait_now_ns(),
                       completion_raced);
}

void QWorkQueue::life_wait_wake(uintptr_t row) {
  life_wait_queue.wake(row, life_wait_now_ns());
}

void QWorkQueue::life_wait_cancel(uintptr_t row, uint64_t token) {
  life_wait_queue.cancel(row, token);
}

void QWorkQueue::life_wait_clear() { life_wait_queue.clear(); }

void QWorkQueue::life_wait_result(bool finalize) {
  (finalize ? life_wait_finalize : life_wait_help).fetch_add(
      1, std::memory_order_relaxed);
}

namespace {
void life_counter_max(std::atomic<uint64_t> &counter, uint64_t value) {
  uint64_t previous = counter.load(std::memory_order_relaxed);
  while (previous < value && !counter.compare_exchange_weak(
      previous, value, std::memory_order_relaxed)) {}
}
}

void QWorkQueue::life_wait_resumed(LifeResultCode result, bool completion,
                                  uint64_t dispatch_ns, uint64_t ready_ns) {
  ResumeCounts &counts = life_resume_counts[completion ? 1 : 0];
  counts.outcomes[static_cast<unsigned>(result)].fetch_add(1, std::memory_order_relaxed);
  counts.dispatch_ns.fetch_add(dispatch_ns, std::memory_order_relaxed);
  counts.ready_ns.fetch_add(ready_ns, std::memory_order_relaxed);
}

void QWorkQueue::life_prepared_release(uint64_t ns) {
  life_prepared_releases.fetch_add(1, std::memory_order_relaxed);
  life_prepared_ns.fetch_add(ns, std::memory_order_relaxed);
  life_counter_max(life_prepared_max_ns, ns);
}

void QWorkQueue::life_cleanup_scan(uint64_t scanned, uint64_t reset) {
  life_cleanup_calls.fetch_add(1, std::memory_order_relaxed);
  life_cleanup_scanned.fetch_add(scanned, std::memory_order_relaxed);
  life_cleanup_reset.fetch_add(reset, std::memory_order_relaxed);
  life_counter_max(life_cleanup_max_scan, scanned);
}

void QWorkQueue::life_wait_print() {
  printf("LIFE_WAIT_QUEUE admitted=%lu released=%lu wait_ns=%lu help=%lu finalize=%lu\n",
         life_wait_admitted.load(), life_wait_released.load(),
         life_wait_ns.load(), life_wait_help.load(), life_wait_finalize.load());
  const auto s = life_wait_queue.snapshot();
  printf("LIFE_WAIT_DIAG admitted=%lu duplicates=%lu canceled=%lu pending=%lu "
         "peak_pending=%lu peak_row_depth=%lu depth_sum=%lu wake_calls=%lu "
         "wake_empty=%lu wake_coalesced=%lu registration_races=%lu "
         "cancel_preserved_wake=%lu timer_releases=%lu wake_releases=%lu "
         "ready_delay_ns=%lu max_ready_delay_ns=%lu\n",
         s.admitted, s.duplicates, s.canceled, s.pending, s.peak_pending,
         s.peak_row_depth, s.depth_sum, s.wake_calls, s.wake_empty,
         s.wake_coalesced, s.registration_races, s.cancel_preserved_wake,
         s.timer_releases, s.wake_releases, s.ready_delay_ns, s.max_ready_delay_ns);
  for (unsigned i = 0; i < 2; ++i) {
    const auto &c = life_resume_counts[i];
    printf("LIFE_WAIT_RESUME source=%s success=%lu finalize=%lu committed=%lu "
           "help=%lu retry=%lu invalid=%lu deferred=%lu dispatch_ns=%lu ready_ns=%lu\n",
           i ? "completion" : "timer", c.outcomes[0].load(), c.outcomes[1].load(),
           c.outcomes[2].load(), c.outcomes[3].load(), c.outcomes[4].load(),
           c.outcomes[5].load(), c.outcomes[6].load(), c.dispatch_ns.load(), c.ready_ns.load());
  }
  printf("LIFE_ROW_DIAG prepared_releases=%lu prepared_ns=%lu prepared_max_ns=%lu "
         "cleanup_calls=%lu cleanup_scanned=%lu cleanup_reset=%lu cleanup_max_scan=%lu\n",
         life_prepared_releases.load(), life_prepared_ns.load(), life_prepared_max_ns.load(),
         life_cleanup_calls.load(), life_cleanup_scanned.load(), life_cleanup_reset.load(),
         life_cleanup_max_scan.load());
}
#endif

Message * QWorkQueue::dequeue(uint64_t thd_id) {
  uint64_t starttime = get_sys_clock();
  assert(ISSERVER || ISREPLICA);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;
  uint64_t mtx_wait_starttime = get_sys_clock();
#if CC_ALG == LIFE
  assert(thd_id < g_thread_cnt);
  // Periodically offer this worker a new transaction, then fall back
  // immediately to the shared continuation queue if admission is empty. Only
  // this worker mutates its streak counter.
  bool valid = false;
#if !SERVER_GENERATE_QUERIES
  const bool prefer_new =
      life_should_try_admission(life_old_dequeue_streak[thd_id]);
  if (prefer_new)
    valid = (CC_ALG != ARIA && new_txn_queue->pop(entry));
#endif
#if LIFE_WAIT_QUEUE
  // Alternate ready waiters with protocol traffic so a busy timer queue
  // cannot starve the finish messages that clear its conflicts.
  if (!valid && life_wait_prefer_protocol[thd_id])
    valid = work_queue->pop(entry);
  if (!valid) {
    LifeWaitQueue<std::unique_ptr<Message> >::Release release;
    const uint64_t now = life_wait_now_ns();
    std::unique_ptr<Message> resumed = life_wait_queue.pop(now, &release);
    if (resumed) {
      auto &wait = static_cast<LifeResumeMessage *>(resumed.get())->wait;
      wait->released_at = now;
      wait->ready_at = release.ready_at;
      wait->completion_wake = release.completion;
      life_wait_released.fetch_add(1, std::memory_order_relaxed);
      life_wait_ns.fetch_add(life_wait_now_ns() -
          static_cast<LifeResumeMessage *>(resumed.get())->wait->enqueued_at,
          std::memory_order_relaxed);
      life_record_dequeue(life_old_dequeue_streak[thd_id], false);
      life_wait_prefer_protocol[thd_id] = 1;
      return resumed.release();
    }
  }
#endif
  if (!valid)
    valid = work_queue->pop(entry);
#else
  bool valid = work_queue->pop(entry);
#endif
  if(!valid) {
#if SERVER_GENERATE_QUERIES
    if(ISSERVER) {
      BaseQuery * m_query = client_query_queue.get_next_query(thd_id,thd_id);
      if(m_query) {
        assert(m_query);
        msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
      }
    }
#else
    valid = (CC_ALG != ARIA && new_txn_queue->pop(entry));
#endif
  }
  INC_STATS(thd_id,mtx[14],get_sys_clock() - mtx_wait_starttime);
  
  if(valid) {
    msg = entry->msg;
    assert(msg);
#if CC_ALG == LIFE
    life_record_dequeue(life_old_dequeue_streak[thd_id],
                        msg->rtype == CL_QRY);
#if LIFE_WAIT_QUEUE
    if (msg->rtype != CL_QRY)
      life_wait_prefer_protocol[thd_id] = msg->rtype == RLIFE_RESUME;
#endif
#endif
    //printf("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(thd_id,work_queue_wait_time,queue_time);
    INC_STATS(thd_id,work_queue_cnt,1);
    if(msg->rtype == CL_QRY) {
      INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
      INC_STATS(thd_id,work_queue_new_cnt,1);
    } else {
      INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
      INC_STATS(thd_id,work_queue_old_cnt,1);
#if CC_ALG == LIFE
      INC_STATS(thd_id,life_cont_dequeue_wait_time_by_type[msg->rtype],
                queue_time);
      INC_STATS(thd_id,life_cont_dequeue_cnt_by_type[msg->rtype],1);
#endif
    }
#if CC_ALG == LIFE
    // A failed manager claim can re-enqueue the same logical message.
    // Keep all queue segments instead of replacing the previous segment.
    msg->wq_time += queue_time;
#else
    msg->wq_time = queue_time;
#endif
    //DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
    DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
    DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));
    INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
  }

#if SERVER_GENERATE_QUERIES
  if(msg && msg->rtype == CL_QRY) {
    INC_STATS(thd_id,work_queue_new_wait_time,get_sys_clock() - starttime);
    INC_STATS(thd_id,work_queue_new_cnt,1);
  }
#endif
  return msg;
}

#if CC_ALG == ARIA
Message *QWorkQueue::aria_dequeue_client() {
  work_queue_entry *entry = NULL;
  if (!new_txn_queue->pop(entry)) return NULL;
  Message *msg = entry->msg;
  mem_allocator.free(entry, sizeof(*entry));
  return msg;
}
#endif

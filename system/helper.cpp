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

#include "global.h"
#include "helper.h"
#include "mem_alloc.h"
#include "time.h"

bool itemid_t::operator==(const itemid_t &other) const {
	return (type == other.type && location == other.location);
}

bool itemid_t::operator!=(const itemid_t &other) const {
	return !(*this == other);
}

void itemid_t::operator=(const itemid_t &other){
	this->valid = other.valid;
	this->type = other.type;
	this->location = other.location;
	assert(*this == other);
	assert(this->valid);
}

void itemid_t::init() {
	valid = false;
	location = 0;
	next = NULL;
}

int get_thdid_from_txnid(uint64_t txnid) {
	return txnid % g_thread_cnt;
}

uint64_t get_part_id(void * addr) {
	return ((uint64_t)addr / PAGE_SIZE) % g_part_cnt; 
}

uint64_t key_to_part(uint64_t key) {
	if (g_part_alloc)
		return key % g_part_cnt;
	else 
		return 0;
}

uint64_t merge_idx_key(UInt64 key_cnt, UInt64 * keys) {
	UInt64 len = 64 / key_cnt;
	UInt64 key = 0;
	for (UInt32 i = 0; i < len; i++) {
		assert(keys[i] < (1UL << len));
		key = (key << len) | keys[i];
	}
	return key;
}

uint64_t merge_idx_key(uint64_t key1, uint64_t key2) {
	assert(key1 < (1UL << 32) && key2 < (1UL << 32));
	return key1 << 32 | key2;
}

uint64_t merge_idx_key(uint64_t key1, uint64_t key2, uint64_t key3) {
	assert(key1 < (1 << 21) && key2 < (1 << 21) && key3 < (1 << 21));
	return key1 << 42 | key2 << 21 | key3;
}

void init_globals() {
  g_max_read_req = g_node_cnt * g_inflight_max;
  g_max_pre_req = g_node_cnt * g_inflight_max;
}

void init_client_globals() {
  if(g_node_cnt > g_client_node_cnt) {
    g_servers_per_client = g_node_cnt / g_client_node_cnt;
    g_clients_per_server = 1;
  }
  else {
    g_servers_per_client = 1;
    g_clients_per_server = g_client_node_cnt / g_node_cnt;
  }
  uint32_t client_node_id = g_node_id - g_node_cnt;
  g_server_start_node = (client_node_id * g_servers_per_client) % g_node_cnt; 
  if (g_node_cnt >= g_client_node_cnt && g_node_cnt % g_client_node_cnt != 0 && g_node_id == (g_node_cnt + g_client_node_cnt -1)) {
      // Have last client pick up any leftover servers if the number of
      // servers cannot be evenly divided between client nodes
      // fix the remainder to be equally distributed among clients
      g_servers_per_client += g_node_cnt % g_client_node_cnt;
  }
    printf("Node %u: servicing %u total nodes starting with node %u\n", g_node_id, g_servers_per_client, g_server_start_node);
}

/****************************************************/
// Global Clock!
/****************************************************/

uint64_t get_wall_clock() {
	timespec tp;
  clock_gettime(CLOCK_REALTIME, &tp);
  return static_cast<uint64_t>(tp.tv_sec) * 1000000000UL + tp.tv_nsec;
}

uint64_t get_server_clock() {
	// RDTSC only measures nanoseconds after division by the machine's actual
	// invariant-TSC frequency.  The old hard-coded CPU_FREQ silently scaled all
	// latency values on other processors.  REALTIME also retains the shared
	// clock domain required by timestamps carried between experiment hosts.
	timespec tp;
	clock_gettime(CLOCK_REALTIME, &tp);
	return static_cast<uint64_t>(tp.tv_sec) * 1000000000UL + tp.tv_nsec;
}

uint64_t get_sys_clock() {
	if (TIME_ENABLE) 
		return get_server_clock();
	return 0;
}

void myrand::init(uint64_t seed) {
	this->seed = seed;
}

uint64_t myrand::next() {
	seed = (seed * 1103515247UL + 12345UL) % (1UL<<63);
	return (seed / 65537) % RAND_MAX;
}

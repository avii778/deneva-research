#ifndef _YCSB_OLLP_H_
#define _YCSB_OLLP_H_

#include <stdint.h>
#include <vector>

struct YCSBReconRecord {
  uint64_t key;
  std::vector<char> tuple;

  YCSBReconRecord() : key(0) {}
};

#endif

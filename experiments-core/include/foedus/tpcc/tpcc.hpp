/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_TPCC_TPCC_HPP_
#define FOEDUS_TPCC_TPCC_HPP_

/**
 * @file tpcc.hpp
 * @brief Common methods for TPC-C benchmark.
 */
#include <stdint.h>
#include <time.h>

#include <cstring>
#include <string>

#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/tpcc/tpcc_scale.hpp"
#include "foedus/tpcc/tpcc_schema.hpp"

namespace foedus {
namespace tpcc {

template <typename T>
inline void zero_clear(T* data) {
  std::memset(data, 0, sizeof(T));
}

/**
 * TPC-C Lastname Function.
 * @param[in] num  non-uniform random number
 * @param[out] name last name string
 */
inline void generate_lastname(int32_t num, char *name) {
  const char *n[] = {
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
  };
  const uint8_t l[] = {3, 5, 4, 3, 4, 3, 4, 5, 5, 4};

  uint8_t len = 0;
  for (int i = 0; i < 3; ++i) {
    int32_t choice = i == 0 ? num % 10 : (i == 1 ? (num / 10) % 10 : (num / 100) % 10);
    std::memcpy(name + len, n[choice], l[choice]);
    len += l[choice];
  }

  // to make sure, fill out _all_ remaining part with NULL character.
  std::memset(name + len, 0, 17 - len);
}

inline storage::masstree::KeySlice to_wdoid_slice(Wid wid, Did did, Oid oid) {
  Wdid wdid = combine_wdid(wid, did);
  Wdoid wdoid = combine_wdoid(wdid, oid);
  return storage::masstree::normalize_primitive<Wdoid>(wdoid);
}

inline storage::masstree::KeySlice to_wdol_slice(Wid wid, Did did, Oid oid, Ol ol) {
  Wdid wdid = combine_wdid(wid, did);
  Wdoid wdoid = combine_wdoid(wdid, oid);
  Wdol wdol = combine_wdol(wdoid, ol);
  return storage::masstree::normalize_primitive<Wdol>(wdol);
}

inline storage::masstree::KeySlice to_wdcid_slice(Wid wid, Did did, Cid cid) {
  Wdid wdid = combine_wdid(wid, did);
  Wdcid wdcid = combine_wdcid(wdid, cid);
  return storage::masstree::normalize_primitive<Wdcid>(wdcid);
}

inline std::string get_current_time_string() {
  time_t t_clock;
  ::time(&t_clock);
  const char* str = ::ctime(&t_clock);  // NOLINT(runtime/threadsafe_fn) no race here
  return std::string(str, ::strlen(str));
}

}  // namespace tpcc
}  // namespace foedus

#endif  // FOEDUS_TPCC_TPCC_HPP_

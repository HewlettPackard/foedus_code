/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
inline void generate_lastname(uint32_t num, char *name) {
  const char *n[] = {
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
  };
  const uint8_t l[] = {3, 5, 4, 3, 4, 3, 4, 5, 5, 4};

  uint8_t len = 0;
  for (int i = 0; i < 3; ++i) {
    uint16_t choice = i == 0 ? num % 10 : (i == 1 ? (num / 10) % 10 : (num / 100) % 10);
    ASSERT_ND(choice < 10U);
    ASSERT_ND(len + l[choice] <= 17U);
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

inline std::string get_current_time_string(char* ctime_buffer) {
  time_t t_clock;
  ::time(&t_clock);
  const char* str = ::ctime_r(&t_clock, ctime_buffer);
  return std::string(str, ::strlen(str));
}

}  // namespace tpcc
}  // namespace foedus

#endif  // FOEDUS_TPCC_TPCC_HPP_

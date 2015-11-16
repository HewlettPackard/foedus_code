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
#ifndef FOEDUS_TPCE_TPCE_HPP_
#define FOEDUS_TPCE_TPCE_HPP_

/**
 * @file tpce.hpp
 * @brief Common methods for TPC-E benchmark.
 */
#include <stdint.h>

#include <cstring>
#include <ctime>
#include <string>

#include "foedus/tpce/tpce_schema.hpp"

namespace foedus {
namespace tpce {

template <typename T>
inline void zero_clear(T* data) {
  std::memset(data, 0, sizeof(T));
}

// seconds from 1800/1/1 to unix epoch time.
// thx to http://www.epochconverter.com/
const uint64_t kUnixAdjustSeconds = 5364662400UL;
// granularity of our datetime/time in seconds
const uint32_t kTimeUnitInSeconds = 3;
const uint64_t kUnixAdjustUnits = kUnixAdjustSeconds / kTimeUnitInSeconds;

inline Datetime get_current_datetime() {
  std::time_t now = std::time(nullptr);
  uint32_t seconds = static_cast<uint32_t>(now);
  Datetime units = seconds / kTimeUnitInSeconds;
  units += kUnixAdjustUnits;
  return units;
}

inline std::string to_datetime_string(Datetime value) {
  std::time_t converted = (value  - kUnixAdjustUnits) * kTimeUnitInSeconds;
  // Yes, this function must not be called in a racy place due to this.
  std::tm * ptm = std::gmtime(&converted);  // NOLINT(runtime/threadsafe_fn)
  // No clean solution in std, though ("_r" thingy are not standard yet).
  // Did you drink enough coffee, C++ committees?
  // http://stackoverflow.com/questions/25618702/why-is-there-no-c11-threadsafe-alternative-to-stdlocaltime-and-stdgmtime
  char buffer[64];
  int written = std::strftime(buffer, 64, "%Y/%m/%d %H:%M:%S GMT", ptm);
  return std::string(buffer, written);
}

}  // namespace tpce
}  // namespace foedus

#endif  // FOEDUS_TPCE_TPCE_HPP_

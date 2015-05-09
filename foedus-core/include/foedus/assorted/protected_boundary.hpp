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
#ifndef FOEDUS_ASSORTED_PROTECTED_BOUNDARY_HPP_
#define FOEDUS_ASSORTED_PROTECTED_BOUNDARY_HPP_

#include <stdint.h>

#include <algorithm>
#include <cstring>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/error_code.hpp"

namespace foedus {
namespace assorted {

const uint64_t kProtectedBoundaryMagicWord = 0x42a6292680d7ce36ULL;

/**
 * @brief A 4kb dummy data placed between separate memory regions so that we can check
 * if/where a bogus memory access happens.
 * @ingroup ASSORTED
 * @details
 * As a middleware, we often allocate a large chunk of memory and internally split it into
 * smaller data regions. While it reduces overheads, that means a lack of memory boundary check.
 * Instead, we place this dummy block to achieve it.
 * When we shut down the system, we check if there happened any unexpected overrun between
 * memory regions.
 * In addition, we also use mprotect() to prohibit accesses to these dummy blocks, which
 * would immediately fire up SIGSEGV for bogus access, making debugging easier.
 */
struct ProtectedBoundary {
  enum Constants {
    kByteSize = 1 << 12,
    kMaxBoundaryNameLength = 128,
    kWordCount = (kByteSize - kMaxBoundaryNameLength) / 8,
  };

  char      boundary_name_[kMaxBoundaryNameLength];
  uint64_t  data_[kWordCount];

  /**
   * Puts a strict access prohibition via mprotect().
   * @attention This method might cause severe performance. Use in appropriate places.
   * We use hugepages in most places, but mprotect() most likely splits the TLB entries to
   * achieve the protection, throwing away the performance benefits of hugepages.
   * @note this method is idempotent. safe to call multiple times
   * @returns The only possible error is kErrorCodeOsMProtectFailed
   */
  ErrorCode acquire_protect();

  /**
   * Removes all access restrictions via mprotect().
   * @attention This method might cause severe performance hit. Use in appropriate places.
   * @note this method is idempotent. safe to call multiple times
   * @returns The only possible error is kErrorCodeOsMProtectFailed
   */
  ErrorCode release_protect();

  /**
   * Fills the block with magic words.
   * @attention This must be invoked BEFORE acquire_protect()
   */
  void      reset(const std::string& boundary_name) {
    uint16_t copy_len = std::min<uint64_t>(boundary_name.length(), kMaxBoundaryNameLength);
    boundary_name.copy(boundary_name_, copy_len, 0);
    if (copy_len < kMaxBoundaryNameLength) {
      std::memset(boundary_name_ + copy_len, 0, kMaxBoundaryNameLength- copy_len);
    }
    for (uint32_t i = 0; i < kWordCount; ++i) {
      data_[i] = kProtectedBoundaryMagicWord;
    }
  }

  /**
   * Called at shutdown to check whether these boundaries were not accessed.
   * @attention This must be invoked AFTER release_protect()
   */
  void      assert_boundary() const {
    for (uint32_t i = 0; i < kWordCount; ++i) {
      ASSERT_ND(data_[i] == kProtectedBoundaryMagicWord);
    }
  }

  std::string get_boundary_name() const {
    uint16_t len;
    for (len = 0; len < kMaxBoundaryNameLength; ++len) {
      if (boundary_name_[len] == 0) {
        break;
      }
    }
    ASSERT_ND(len < kMaxBoundaryNameLength);
    return std::string(boundary_name_, len);
  }
};

}  // namespace assorted
}  // namespace foedus

#endif  // FOEDUS_ASSORTED_PROTECTED_BOUNDARY_HPP_

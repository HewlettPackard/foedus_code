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
#ifndef FOEDUS_XCT_XCT_OPTIONS_HPP_
#define FOEDUS_XCT_XCT_OPTIONS_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/cxx11.hpp"
#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace xct {
/**
 * @brief Set of options for xct manager.
 * @ingroup XCT
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct XctOptions CXX11_FINAL : public virtual externalize::Externalizable {
  /** Constant values. */
  enum Constants {
    /** Default value for max_read_set_size_. */
    kDefaultMaxReadSetSize = 32 << 10,
    /** Default value for max_write_set_size_. */
    kDefaultMaxWriteSetSize = 8 << 10,
    /** Default value for max_lock_free_write_set_size_. */
    kDefaultMaxLockFreeWriteSetSize = 4 << 10,
    /** Default value for local_work_memory_size_mb_. */
    kDefaultLocalWorkMemorySizeMb = 2,
    /** Default value for epoch_advance_interval_ms_. */
    kDefaultEpochAdvanceIntervalMs = 20,
  };

  /**
   * Constructs option values with default values.
   */
  XctOptions();

  EXTERNALIZABLE(XctOptions);

  /**
   * @brief The maximum number of read-set one transaction can have.
   * @details
   * Default is 64K records.
   * We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.
   */
  uint32_t    max_read_set_size_;

  /**
   * @brief The maximum number of write-set one transaction can have.
   * @details
   * Default is 16K records.
   * We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.
   */
  uint32_t    max_write_set_size_;

  /**
   * @brief The maximum number of lock-free write-set one transaction can have.
   * @details
   * Default is 8K records.
   * We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.
   */
  uint32_t    max_lock_free_write_set_size_;

  /**
   * @brief Size of local and temporary work memory one transaction can use during transaction.
   * @details
   * Local work memory is used for various purposes during a transaction.
   * We avoid allocating such temporary memory for each transaction and pre-allocate this
   * size at start up.
   */
  uint32_t    local_work_memory_size_mb_;

  /**
   * @brief Intervals in milliseconds between epoch advancements.
   * @details
   * Default is 20 ms.
   * Too frequent epoch advancement might become bottleneck because we \b synchronously write
   * out savepoint file for each non-empty epoch. However, too infrequent epoch advancement
   * would increase the latency of queries because transactions are not deemed as commit
   * until the epoch advances.
   */
  uint32_t    epoch_advance_interval_ms_;
};
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_OPTIONS_HPP_

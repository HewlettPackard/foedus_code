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
#include "foedus/externalize/externalizable.hpp"
#include "foedus/xct/xct_options.hpp"
namespace foedus {
namespace xct {
XctOptions::XctOptions() {
  max_read_set_size_ = kDefaultMaxReadSetSize;
  max_write_set_size_ = kDefaultMaxWriteSetSize;
  max_lock_free_write_set_size_ = kDefaultMaxLockFreeWriteSetSize;
  local_work_memory_size_mb_ = kDefaultLocalWorkMemorySizeMb;
  epoch_advance_interval_ms_ = kDefaultEpochAdvanceIntervalMs;
}

ErrorStack XctOptions::load(tinyxml2::XMLElement* element) {
  EXTERNALIZE_LOAD_ELEMENT(element, max_read_set_size_);
  EXTERNALIZE_LOAD_ELEMENT(element, max_write_set_size_);
  EXTERNALIZE_LOAD_ELEMENT(element, max_lock_free_write_set_size_);
  EXTERNALIZE_LOAD_ELEMENT(element, local_work_memory_size_mb_);
  EXTERNALIZE_LOAD_ELEMENT(element, epoch_advance_interval_ms_);
  return kRetOk;
}

ErrorStack XctOptions::save(tinyxml2::XMLElement* element) const {
  CHECK_ERROR(insert_comment(element, "Set of options for xct manager"));

  EXTERNALIZE_SAVE_ELEMENT(element, max_read_set_size_,
    "The maximum number of read-set one transaction can have. Default is 64K records.\n"
    " We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.");
  EXTERNALIZE_SAVE_ELEMENT(element, max_write_set_size_,
    "The maximum number of write-set one transaction can have. Default is 16K records.\n"
    " We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.");
  EXTERNALIZE_SAVE_ELEMENT(element, max_lock_free_write_set_size_,
    "The maximum number of lock-free write-set one transaction can have. Default is 8K records.\n"
    " We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.");
  EXTERNALIZE_SAVE_ELEMENT(element, local_work_memory_size_mb_,
    "Local work memory is used for various purposes during a transaction."
    " We avoid allocating such temporary memory for each transaction and pre-allocate this"
    " size at start up.");
  EXTERNALIZE_SAVE_ELEMENT(element, epoch_advance_interval_ms_,
    "Intervals in milliseconds between epoch advancements. Default is 20 ms\n"
    " Too frequent epoch advancement might become bottleneck because we synchronously write.\n"
    " out savepoint file for each non-empty epoch. However, too infrequent epoch advancement\n"
    " would increase the latency of queries because transactions are not deemed as commit"
    " until the epoch advances.");
  return kRetOk;
}


}  // namespace xct
}  // namespace foedus

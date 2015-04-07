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
#include "foedus/storage/page.hpp"

#include <glog/logging.h>

#include <ostream>

#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {

std::ostream& operator<<(std::ostream& o, const PageVersionStatus& v) {
  o << "<PageVersionStatus><flags>"
    << (v.is_moved() ? "M" : " ")
    << (v.is_retired() ? "R" : " ")
    << "</flags><ver>" << v.get_version_counter() << "</ver>"
    << "</PageVersionStatus>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const PageVersion& v) {
  o << "<PageVersion><locked>"
    << (v.is_locked() ? "L" : " ")
    << (v.is_moved() ? "M" : " ")
    << (v.is_retired() ? "R" : " ")
    << "</locked>" << v.status_
    << "</PageVersion>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const PageHeader& v) {
  o << "<PageHeader>";
  o << std::endl << "<page_id>"
    << "<raw>" << assorted::Hex(v.page_id_, 16) << "</raw>";
  // Also write out interpreted version
  if (v.snapshot_) {
    describe_snapshot_pointer(&o, v.page_id_);
  } else {
    VolatilePagePointer pointer;
    pointer.word = v.page_id_;
    describe_volatile_pointer(&o, pointer);
  }
  o << "</page_id>";
  o << std::endl << "<storage_id_>" << v.storage_id_ << "</storage_id_>";
  o << "<checksum_>" << v.checksum_ << "</checksum_>";
  o << "<page_type_>" << static_cast<int>(v.page_type_) << "</page_type_>";
  o << std::endl << "<snapshot_>" << v.snapshot_ << "</snapshot_>";
  o << "<key_count_>" << v.key_count_ << "</key_count_>";
  o << "<masstree_layer_>" << static_cast<int>(v.masstree_layer_) << "</masstree_layer_>";
  o << "<masstree_in_layer_level_>" << static_cast<int>(v.masstree_in_layer_level_)
    << "</masstree_in_layer_level_>";
  o << std::endl << "<stat_last_updater_node_>" << static_cast<int>(v.stat_last_updater_node_)
    << "</stat_last_updater_node_>";
  o << v.page_version_;
  o << "</PageHeader>";
  return o;
}

PageVersionLockScope::PageVersionLockScope(
  thread::Thread* context,
  PageVersion* version,
  bool initial_lock) {
  context_ = context;
  version_ = version;
  changed_ = false;
  released_ = false;
  if (initial_lock) {
    block_ = context->mcs_initial_lock(&version->lock_);
  } else {
    block_ = context->mcs_acquire_lock(&version->lock_);
  }
}

void PageVersionLockScope::release() {
  if (!released_) {
    if (changed_) {
      version_->increment_version_counter();
    }
    context_->mcs_release_lock(&version_->lock_, block_);
    released_ = true;
  }
}


}  // namespace storage
}  // namespace foedus

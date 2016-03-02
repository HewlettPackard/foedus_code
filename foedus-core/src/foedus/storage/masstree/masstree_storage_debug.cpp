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
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"

#include <glog/logging.h>

#include "foedus/engine.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace masstree {

ErrorStack MasstreeStoragePimpl::debugout_single_thread(
  Engine* engine,
  bool volatile_only,
  uint32_t max_pages) {
  LOG(INFO) << "**"
    << std::endl << "***************************************************************"
    << std::endl << "***   Dumping " << MasstreeStorage(engine_, control_block_) << " in details. "
    << std::endl << "*** volatile_only=" << volatile_only << ", max_pages=" << max_pages
    << std::endl << "***************************************************************";

  cache::SnapshotFileSet files(engine);
  CHECK_ERROR(files.initialize());
  UninitializeGuard files_guard(&files, UninitializeGuard::kWarnIfUninitializeError);

  LOG(INFO) << "First, dumping volatile pages...";
  DualPagePointer pointer = control_block_->root_page_pointer_;
  if (pointer.volatile_pointer_.is_null()) {
    LOG(INFO) << "No volatile pages.";
  } else {
    uint32_t remaining = max_pages;
    CHECK_ERROR(debugout_single_thread_follow(engine, &files, pointer, true, &remaining));
  }
  LOG(INFO) << "Dumped volatile pages.";
  if (!volatile_only) {
    LOG(INFO) << "Now dumping snapshot pages...";
    if (pointer.snapshot_pointer_ == 0) {
      LOG(INFO) << "No snapshot pages.";
    } else {
      uint32_t remaining = max_pages;
      CHECK_ERROR(debugout_single_thread_follow(engine, &files, pointer, false, &remaining));
    }
    LOG(INFO) << "Dumped snapshot pages.";
  }

  CHECK_ERROR(files.uninitialize());
  return kRetOk;
}


ErrorStack MasstreeStoragePimpl::debugout_single_thread_recurse(
  Engine* engine,
  cache::SnapshotFileSet* files,
  MasstreePage* parent,
  bool follow_volatile,
  uint32_t* remaining_pages) {
  if (((*remaining_pages) == 0) || --(*remaining_pages) == 0) {
    LOG(INFO) << "Reached write-out max. skip the following";
    return kRetOk;
  }

  LOG(INFO) << *parent;
  uint16_t key_count = parent->get_key_count();
  if (parent->is_border()) {
    MasstreeBorderPage* casted = reinterpret_cast<MasstreeBorderPage*>(parent);
    for (uint16_t i = 0; i < key_count; ++i) {
      if (casted->does_point_to_layer(i)) {
        CHECK_ERROR(debugout_single_thread_follow(
          engine,
          files,
          *casted->get_next_layer(i),
          follow_volatile,
          remaining_pages));
      }
    }
  } else {
    MasstreeIntermediatePage* casted = reinterpret_cast<MasstreeIntermediatePage*>(parent);
    for (MasstreeIntermediatePointerIterator it(casted); it.is_valid(); it.next()) {
      CHECK_ERROR(debugout_single_thread_follow(
        engine,
        files,
        it.get_pointer(),
        follow_volatile,
        remaining_pages));
    }
  }

  return kRetOk;
}

ErrorStack MasstreeStoragePimpl::debugout_single_thread_follow(
  Engine* engine,
  cache::SnapshotFileSet* files,
  const DualPagePointer& pointer,
  bool follow_volatile,
  uint32_t* remaining_pages) {
  if ((*remaining_pages) == 0) {
    return kRetOk;
  }
  if (follow_volatile) {
    if (!pointer.volatile_pointer_.is_null()) {
     MasstreePage* page = reinterpret_cast<MasstreePage*>(
       engine->get_memory_manager()->get_global_volatile_page_resolver().resolve_offset(
        pointer.volatile_pointer_));
      CHECK_ERROR(debugout_single_thread_recurse(engine, files, page, true, remaining_pages));
    }
  } else {
    if (pointer.snapshot_pointer_ != 0) {
      memory::AlignedMemory buf;
      buf.alloc(1 << 12U, 1 << 12U, memory::AlignedMemory::kNumaAllocOnnode, 0);
      MasstreePage* page = reinterpret_cast<MasstreePage*>(buf.get_block());
      WRAP_ERROR_CODE(files->read_page(pointer.snapshot_pointer_, page));
      CHECK_ERROR(debugout_single_thread_recurse(engine, files, page, false, remaining_pages));
    }
  }
  return kRetOk;
}


ErrorStack MasstreeStoragePimpl::hcc_reset_all_temperature_stat(Engine* engine) {
  LOG(INFO) << "**"
    << std::endl << "***************************************************************"
    << std::endl << "***   Reseting " << MasstreeStorage(engine_, control_block_) << "'s "
    << std::endl << "*** temperature stat for HCC"
    << std::endl << "***************************************************************";

  DualPagePointer pointer = control_block_->root_page_pointer_;
  if (pointer.volatile_pointer_.is_null()) {
    LOG(INFO) << "No volatile pages.";
  } else {
    CHECK_ERROR(hcc_reset_all_temperature_stat_follow(engine, pointer.volatile_pointer_));
  }

  LOG(INFO) << "Done resettting";
  return kRetOk;
}


ErrorStack MasstreeStoragePimpl::hcc_reset_all_temperature_stat_recurse(
  Engine* engine,
  MasstreePage* parent) {
  uint16_t key_count = parent->get_key_count();
  if (parent->is_border()) {
    MasstreeBorderPage* casted = reinterpret_cast<MasstreeBorderPage*>(parent);
    casted->header().hotness_.reset();
    for (uint16_t i = 0; i < key_count; ++i) {
      if (casted->does_point_to_layer(i)) {
        CHECK_ERROR(hcc_reset_all_temperature_stat_follow(
          engine,
          casted->get_next_layer(i)->volatile_pointer_));
      }
    }
  } else {
    MasstreeIntermediatePage* casted = reinterpret_cast<MasstreeIntermediatePage*>(parent);
    for (MasstreeIntermediatePointerIterator it(casted); it.is_valid(); it.next()) {
      CHECK_ERROR(hcc_reset_all_temperature_stat_follow(
        engine,
        it.get_pointer().volatile_pointer_));
    }
  }

  return kRetOk;
}

ErrorStack MasstreeStoragePimpl::hcc_reset_all_temperature_stat_follow(
  Engine* engine,
  VolatilePagePointer page_id) {
  if (page_id.is_null()) {
    MasstreePage* page = reinterpret_cast<MasstreePage*>(
      engine->get_memory_manager()->get_global_volatile_page_resolver().resolve_offset(page_id));
    CHECK_ERROR(hcc_reset_all_temperature_stat_recurse(engine, page));
  }
  return kRetOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

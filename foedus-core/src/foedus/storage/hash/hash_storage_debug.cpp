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
#include "foedus/storage/hash/hash_storage_pimpl.hpp"

#include <glog/logging.h>

#include "foedus/engine.hpp"
#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/memory/aligned_memory.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace hash {

struct TmpSnashotPage {
  ErrorStack init(cache::SnapshotFileSet* files, SnapshotPagePointer pointer) {
    if (buf_.is_null() || buf_.get_size() < 1 << 12U) {
      buf_.alloc(1 << 12U, 1 << 12U, memory::AlignedMemory::kNumaAllocOnnode, 0);
    }
    page_ = reinterpret_cast<Page*>(buf_.get_block());
    WRAP_ERROR_CODE(files->read_page(pointer, page_));
    return kRetOk;
  }

  HashIntermediatePage* as_intermediate() { return reinterpret_cast<HashIntermediatePage*>(page_); }
  HashDataPage*         as_data() { return reinterpret_cast<HashDataPage*>(page_); }

  memory::AlignedMemory buf_;
  Page*                 page_;
};

ErrorStack HashStoragePimpl::debugout_single_thread(
  Engine* engine,
  bool volatile_only,
  bool intermediate_only,
  uint32_t max_pages) {
  LOG(INFO) << "**"
    << std::endl << "***************************************************************"
    << std::endl << "***   Dumping " << HashStorage(engine_, control_block_) << " in details. "
    << std::endl << "*** volatile_only=" << volatile_only
      << ", intermediate_only=" << intermediate_only << ", max_pages=" << max_pages
    << std::endl << "***************************************************************";

  if (max_pages == 0) {
    return kRetOk;
  }

  cache::SnapshotFileSet files(engine);
  CHECK_ERROR(files.initialize());
  UninitializeGuard files_guard(&files, UninitializeGuard::kWarnIfUninitializeError);

  LOG(INFO) << "First, dumping volatile pages...";
  DualPagePointer pointer = control_block_->root_page_pointer_;
  if (pointer.volatile_pointer_.is_null()) {
    LOG(INFO) << "No volatile pages.";
  } else {
    HashIntermediatePage* root = reinterpret_cast<HashIntermediatePage*>(
      engine->get_memory_manager()->get_global_volatile_page_resolver().resolve_offset(
        pointer.volatile_pointer_));
    uint32_t remaining = max_pages - 1U;
    CHECK_ERROR(debugout_single_thread_intermediate(
      engine,
      &files,
      root,
      true,
      intermediate_only,
      &remaining));
  }
  LOG(INFO) << "Dumped volatile pages.";
  if (!volatile_only) {
    LOG(INFO) << "Now dumping snapshot pages...";
    if (pointer.snapshot_pointer_ == 0) {
      LOG(INFO) << "No snapshot pages.";
    } else {
      TmpSnashotPage tmp;
      CHECK_ERROR(tmp.init(&files, pointer.snapshot_pointer_));
      HashIntermediatePage* root = tmp.as_intermediate();
      uint32_t remaining = max_pages - 1U;
      CHECK_ERROR(debugout_single_thread_intermediate(
        engine,
        &files,
        root,
        false,
        intermediate_only,
        &remaining));
    }
    LOG(INFO) << "Dumped snapshot pages.";
  }

  CHECK_ERROR(files.uninitialize());
  return kRetOk;
}


ErrorStack HashStoragePimpl::debugout_single_thread_intermediate(
  Engine* engine,
  cache::SnapshotFileSet* files,
  HashIntermediatePage* parent,
  bool follow_volatile,
  bool intermediate_only,
  uint32_t* remaining) {
  if (((*remaining) == 0) || --(*remaining) == 0) {
    LOG(INFO) << "Reached write-out max. skip the following";
    return kRetOk;
  }

  LOG(INFO) << *parent;
  bool bottom = parent->get_level() == 0;
  if (bottom && intermediate_only) {
    return kRetOk;
  }

  const auto& resolver = engine->get_memory_manager()->get_global_volatile_page_resolver();
  TmpSnashotPage tmp;
  for (uint8_t i = 0; i < kHashIntermediatePageFanout && ((*remaining) > 0); ++i) {
    DualPagePointer pointer = parent->get_pointer(i);
    if (follow_volatile) {
      if (pointer.volatile_pointer_.is_null()) {
        continue;
      }
      if (bottom) {
        HashDataPage* page = reinterpret_cast<HashDataPage*>(
          resolver.resolve_offset(pointer.volatile_pointer_));
        CHECK_ERROR(debugout_single_thread_data(engine, files, page, true, remaining));
      } else {
        HashIntermediatePage* page = reinterpret_cast<HashIntermediatePage*>(
          resolver.resolve_offset(pointer.volatile_pointer_));
        CHECK_ERROR(debugout_single_thread_intermediate(
          engine,
          files,
          page,
          true,
          intermediate_only,
          remaining));
      }
    } else {
      if (pointer.snapshot_pointer_ == 0) {
        continue;
      }
      CHECK_ERROR(tmp.init(files, pointer.snapshot_pointer_));
      if (bottom) {
        HashDataPage* page = tmp.as_data();
        CHECK_ERROR(debugout_single_thread_data(engine, files, page, false, remaining));
      } else {
        HashIntermediatePage* page = tmp.as_intermediate();
        CHECK_ERROR(debugout_single_thread_intermediate(
          engine,
          files,
          page,
          false,
          intermediate_only,
          remaining));
      }
    }
  }

  return kRetOk;
}

ErrorStack HashStoragePimpl::debugout_single_thread_data(
  Engine* engine,
  cache::SnapshotFileSet* files,
  HashDataPage* head,
  bool follow_volatile,
  uint32_t* remaining) {
  TmpSnashotPage tmp;
  const auto& resolver = engine->get_memory_manager()->get_global_volatile_page_resolver();
  for (HashDataPage* cur = head; cur;) {
    LOG(INFO) << *cur;
    if ((*remaining) == 0 || --(*remaining) == 0) {
      LOG(INFO) << "Reached write-out max. skip the following";
      return kRetOk;
    }

    // here, we take a copy of pointer so that "cur" might be okay to become invalid after here.
    DualPagePointer pointer = cur->next_page();
    cur = nullptr;
    if (follow_volatile) {
      if (!pointer.volatile_pointer_.is_null()) {
        cur = reinterpret_cast<HashDataPage*>(resolver.resolve_offset(pointer.volatile_pointer_));
      }
    } else {
      if (pointer.snapshot_pointer_) {
        CHECK_ERROR(tmp.init(files, pointer.snapshot_pointer_));  // note, this overwrites tmp
        cur = tmp.as_data();
      }
    }
  }
  return kRetOk;
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus

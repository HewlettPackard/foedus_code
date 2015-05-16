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
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/hash_page_impl.hpp"
#include "foedus/storage/hash/hash_storage.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace hash {

#define CHECK_AND_ASSERT(x) do { ASSERT_ND(x); if (!(x)) \
  return ERROR_STACK(kErrorCodeStrHashFailedVerification); } while (0)


ErrorStack HashStoragePimpl::verify_single_thread(thread::Thread* context) {
  return verify_single_thread(context->get_engine());
}
ErrorStack HashStoragePimpl::verify_single_thread(Engine* engine) {
  VolatilePagePointer pointer = control_block_->root_page_pointer_.volatile_pointer_;
  if (!pointer.is_null()) {
    // TASK(Hideaki) probably two versions: always follow volatile vs snapshot
    // so far check volatile only
    HashIntermediatePage* root = reinterpret_cast<HashIntermediatePage*>(
      engine->get_memory_manager()->get_global_volatile_page_resolver().resolve_offset(pointer));
    CHECK_AND_ASSERT(root->get_level() + 1U == control_block_->levels_);
    CHECK_ERROR(verify_single_thread_intermediate(engine, root));
  }
  return kRetOk;
}

ErrorStack HashStoragePimpl::verify_single_thread_intermediate(
  Engine* engine,
  HashIntermediatePage* page) {
  CHECK_AND_ASSERT(page->header().get_page_type() == kHashIntermediatePageType);
  CHECK_AND_ASSERT(!page->header().page_version_.is_locked());
  CHECK_AND_ASSERT(!page->header().page_version_.is_moved());
  CHECK_AND_ASSERT(!page->header().page_version_.is_retired());
  uint8_t level = page->get_level();
  HashBin begin = page->get_bin_range().begin_;
  HashBin interval = kHashMaxBins[level];
  CHECK_AND_ASSERT(page->get_bin_range().end_ == begin + interval * kHashIntermediatePageFanout);
  const auto& resolver = engine->get_memory_manager()->get_global_volatile_page_resolver();
  for (uint8_t i = 0; i < kHashIntermediatePageFanout; ++i) {
    DualPagePointer pointer = page->get_pointer(i);
    if (!pointer.volatile_pointer_.is_null()) {
      if (level == 0) {
        HashDataPage* child = reinterpret_cast<HashDataPage*>(
          resolver.resolve_offset(pointer.volatile_pointer_));
        CHECK_AND_ASSERT(child->get_bin() == begin + i);
        CHECK_ERROR(verify_single_thread_data(engine, child));
      } else {
        HashIntermediatePage* child = reinterpret_cast<HashIntermediatePage*>(
          resolver.resolve_offset(pointer.volatile_pointer_));
        CHECK_AND_ASSERT(child->get_level() + 1U == level);
        CHECK_AND_ASSERT(child->get_bin_range().begin_ == begin + i * interval);
        CHECK_ERROR(verify_single_thread_intermediate(engine, child));
      }
    }
  }
  return kRetOk;
}

ErrorStack HashStoragePimpl::verify_single_thread_data(
  Engine* engine,
  HashDataPage* head) {
  const auto& resolver = engine->get_memory_manager()->get_global_volatile_page_resolver();
  HashBin bin = head->get_bin();
  for (HashDataPage* page = head; page;) {
    CHECK_AND_ASSERT(page->header().get_page_type() == kHashDataPageType);
    CHECK_AND_ASSERT(!page->header().page_version_.is_locked());
    CHECK_AND_ASSERT(!page->header().page_version_.is_moved());
    CHECK_AND_ASSERT(!page->header().page_version_.is_retired());
    CHECK_AND_ASSERT(page->header().masstree_in_layer_level_ == 0);
    CHECK_AND_ASSERT(page->get_bin() == bin);

    page->assert_entries_impl();

    uint16_t records = page->get_record_count();
    for (DataPageSlotIndex i = 0; i < records; ++i) {
      HashDataPage::Slot* slot = page->get_slot_address(i);
      CHECK_AND_ASSERT(!slot->tid_.is_keylocked());
      CHECK_AND_ASSERT(!slot->tid_.xct_id_.is_being_written());
      CHECK_AND_ASSERT(slot->tid_.xct_id_.get_epoch().is_valid());
    }

    VolatilePagePointer next = page->next_page().volatile_pointer_;
    page = nullptr;
    if (!next.is_null()) {
      page = reinterpret_cast<HashDataPage*>(resolver.resolve_offset(next));
    }
  }

  return kRetOk;
}


}  // namespace hash
}  // namespace storage
}  // namespace foedus

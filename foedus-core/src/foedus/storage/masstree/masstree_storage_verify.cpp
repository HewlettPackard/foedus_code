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
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace masstree {

#define CHECK_AND_ASSERT(x) do { ASSERT_ND(x); if (!(x)) \
  return ERROR_STACK(kErrorCodeStrMasstreeFailedVerification); } while (0)


ErrorStack MasstreeStoragePimpl::verify_single_thread(thread::Thread* context) {
  MasstreeIntermediatePage* layer_root;
  WRAP_ERROR_CODE(get_first_root(context, false, &layer_root));
  CHECK_AND_ASSERT(!layer_root->is_border());  // root of first layer is always intermediate page
  CHECK_ERROR(verify_single_thread_layer(context, 0, layer_root));
  return kRetOk;
}

ErrorStack MasstreeStoragePimpl::verify_single_thread_layer(
  thread::Thread* context,
  uint8_t layer,
  MasstreePage* layer_root) {
  CHECK_AND_ASSERT(layer_root->get_layer() == layer);
  HighFence high_fence(kSupremumSlice, true);
  if (layer_root->is_border()) {
    CHECK_ERROR(verify_single_thread_border(
      context,
      kInfimumSlice,
      high_fence,
      reinterpret_cast<MasstreeBorderPage*>(layer_root)));
  } else {
    CHECK_ERROR(verify_single_thread_intermediate(
      context,
      kInfimumSlice,
      high_fence,
      reinterpret_cast<MasstreeIntermediatePage*>(layer_root)));
  }
  return kRetOk;
}

ErrorStack verify_page_basic(
  thread::Thread* context,
  MasstreePage* page,
  PageType page_type,
  KeySlice low_fence,
  HighFence high_fence) {
  CHECK_AND_ASSERT(!page->is_locked());
  CHECK_AND_ASSERT(!page->is_retired());
  CHECK_AND_ASSERT(page->header().get_page_type() == page_type);
  CHECK_AND_ASSERT(page->is_border() || page->get_btree_level() > 0);
  CHECK_AND_ASSERT(!page->is_border() || page->get_btree_level() == 0);
  CHECK_AND_ASSERT(page->get_low_fence() == low_fence);
  CHECK_AND_ASSERT(page->get_high_fence() == high_fence.slice_);
  CHECK_AND_ASSERT(page->is_high_fence_supremum() == high_fence.supremum_);
  CHECK_AND_ASSERT(
    (page->is_moved() && page->has_foster_child()
      && !page->get_foster_major().is_null() && !page->get_foster_minor().is_null()) ||
    (!page->is_moved() && !page->has_foster_child()
      && page->get_foster_major().is_null() && page->get_foster_minor().is_null()));

  if (!page->get_foster_major().is_null()) {
    CHECK_AND_ASSERT(!page->header().snapshot_);
    CHECK_AND_ASSERT(!context->resolve(page->get_foster_major())->get_header().snapshot_);
    CHECK_AND_ASSERT(context->resolve(page->get_foster_major())->get_header().get_page_type()
      == page_type);
  }
  if (!page->get_foster_minor().is_null()) {
    CHECK_AND_ASSERT(!page->header().snapshot_);
    CHECK_AND_ASSERT(!context->resolve(page->get_foster_minor())->get_header().snapshot_);
    CHECK_AND_ASSERT(context->resolve(page->get_foster_minor())->get_header().get_page_type()
      == page_type);
  }
  return kRetOk;
}

ErrorStack MasstreeStoragePimpl::verify_single_thread_intermediate(
  thread::Thread* context,
  KeySlice low_fence,
  HighFence high_fence,
  MasstreeIntermediatePage* page) {
  CHECK_ERROR(
    verify_page_basic(context, page, kMasstreeIntermediatePageType, low_fence, high_fence));

  if (page->is_moved()) {
    CHECK_ERROR(verify_single_thread_intermediate(
      context,
      low_fence,
      HighFence(page->get_foster_fence(), false),
      context->resolve_cast<MasstreeIntermediatePage>(page->get_foster_minor())));
    CHECK_ERROR(verify_single_thread_intermediate(
      context,
      page->get_foster_fence(),
      high_fence,
      context->resolve_cast<MasstreeIntermediatePage>(page->get_foster_major())));
    return kRetOk;
  }

  uint8_t key_count = page->get_key_count();
  CHECK_AND_ASSERT(key_count <= kMaxIntermediateSeparators);
  KeySlice previous_low = low_fence;
  for (uint8_t i = 0; i <= key_count; ++i) {
    HighFence mini_high(0, false);
    if (i < key_count) {
      mini_high.slice_ = page->get_separator(i);
      mini_high.supremum_ = false;
      CHECK_AND_ASSERT(high_fence.supremum_ || mini_high.slice_ < high_fence.slice_);
      if (i == 0) {
        CHECK_AND_ASSERT(mini_high.slice_ > low_fence);
      } else {
        CHECK_AND_ASSERT(mini_high.slice_ > page->get_separator(i - 1));
      }
    } else {
      mini_high = high_fence;
    }

    MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(i);
    uint8_t mini_count = minipage.key_count_;
    CHECK_AND_ASSERT(mini_count <= kMaxIntermediateMiniSeparators);
    KeySlice page_low = previous_low;
    for (uint8_t j = 0; j <= mini_count; ++j) {
      HighFence page_high(0, false);
      if (j < mini_count) {
        page_high.slice_ = minipage.separators_[j];
        page_high.supremum_ = false;
        CHECK_AND_ASSERT(page_high.slice_ < mini_high.slice_ || mini_high.supremum_);
        if (j == 0) {
          CHECK_AND_ASSERT(page_high.slice_ > previous_low);
        } else {
          CHECK_AND_ASSERT(page_high.slice_ > minipage.separators_[j - 1]);
        }
      } else {
        page_high = mini_high;
      }
      CHECK_AND_ASSERT(!minipage.pointers_[j].is_both_null());
      MasstreePage* next;
      // TASK(Hideaki) probably two versions: always follow volatile vs snapshot
      // so far check volatile only
      WRAP_ERROR_CODE(follow_page(context, true, &minipage.pointers_[j], &next));
      CHECK_AND_ASSERT(next->get_layer() == page->get_layer());
      CHECK_AND_ASSERT(next->get_btree_level() + 1U == page->get_btree_level());
      if (next->is_border()) {
        CHECK_ERROR(verify_single_thread_border(
          context,
          page_low,
          page_high,
          reinterpret_cast<MasstreeBorderPage*>(next)));
      } else {
        CHECK_ERROR(verify_single_thread_intermediate(
          context,
          page_low,
          page_high,
          reinterpret_cast<MasstreeIntermediatePage*>(next)));
      }

      page_low = page_high.slice_;
    }

    previous_low = mini_high.slice_;
  }

  return kRetOk;
}

ErrorStack MasstreeStoragePimpl::verify_single_thread_border(
  thread::Thread* context,
  KeySlice low_fence,
  HighFence high_fence,
  MasstreeBorderPage* page) {
  CHECK_ERROR(verify_page_basic(context, page, kMasstreeBorderPageType, low_fence, high_fence));
  // check consecutive_inserts_. this should be consistent whether it's moved or not.
  bool sorted = true;
  for (uint8_t i = 1; i < page->get_key_count(); ++i) {
    KeySlice prev = page->get_slice(i - 1);
    KeySlice slice = page->get_slice(i);
    uint8_t prev_len = page->get_remaining_key_length(i - 1);
    uint8_t len = page->get_remaining_key_length(i);
    if (prev > slice || (prev == slice && prev_len > len)) {
      sorted = false;
      break;
    }
  }
  CHECK_AND_ASSERT(page->is_consecutive_inserts() == sorted);

  if (page->is_moved()) {
    CHECK_ERROR(verify_single_thread_border(
      context,
      low_fence,
      HighFence(page->get_foster_fence(), false),
      context->resolve_cast<MasstreeBorderPage>(page->get_foster_minor())));
    CHECK_ERROR(verify_single_thread_border(
      context,
      page->get_foster_fence(),
      high_fence,
      context->resolve_cast<MasstreeBorderPage>(page->get_foster_major())));
    return kRetOk;
  }

  CHECK_AND_ASSERT(!page->is_moved());
  CHECK_AND_ASSERT(page->get_key_count() <= MasstreeBorderPage::kMaxKeys);
  for (uint8_t i = 0; i < page->get_key_count(); ++i) {
    CHECK_AND_ASSERT(!page->get_owner_id(i)->lock_.is_keylocked());
    CHECK_AND_ASSERT(!page->get_owner_id(i)->lock_.is_rangelocked());
    CHECK_AND_ASSERT(!page->get_owner_id(i)->xct_id_.is_being_written());
    CHECK_AND_ASSERT(page->get_owner_id(i)->xct_id_.get_epoch().is_valid());
    if (i == 0) {
      CHECK_AND_ASSERT(page->get_offset_in_bytes(i) < MasstreeBorderPage::kDataSize);
      CHECK_AND_ASSERT(page->get_offset_in_bytes(i) + page->get_physical_record_size_in_bytes(i)
        == MasstreeBorderPage::kDataSize);
    } else {
      CHECK_AND_ASSERT(page->get_offset_in_bytes(i) < page->get_offset_in_bytes(i - 1));
      CHECK_AND_ASSERT(page->get_offset_in_bytes(i) + page->get_physical_record_size_in_bytes(i)
        == page->get_offset_in_bytes(i - 1));
    }
    KeySlice slice = page->get_slice(i);
    CHECK_AND_ASSERT(slice >= low_fence);
    CHECK_AND_ASSERT(slice < high_fence.slice_ || page->is_high_fence_supremum());
    if (page->does_point_to_layer(i)) {
      CHECK_AND_ASSERT(page->get_physical_record_size_in_bytes(i)
        >= sizeof(DualPagePointer));
      CHECK_AND_ASSERT(page->get_owner_id(i)->xct_id_.is_next_layer());
      CHECK_AND_ASSERT(!page->get_next_layer(i)->is_both_null());
      MasstreePage* next;
      // TASK(Hideaki) probably two versions: always follow volatile vs snapshot
      // so far check volatile only
      WRAP_ERROR_CODE(follow_page(context, true, page->get_next_layer(i), &next));
      CHECK_ERROR(verify_single_thread_layer(context, page->get_layer() + 1, next));
    } else {
      CHECK_AND_ASSERT(!page->get_owner_id(i)->xct_id_.is_next_layer());
      CHECK_AND_ASSERT(page->get_physical_record_size_in_bytes(i)
        >= page->get_suffix_length_aligned(i) + page->get_payload_length(i));
      CHECK_AND_ASSERT(page->get_max_payload_length(i) >= page->get_payload_length(i));
    }
  }

  return kRetOk;
}


}  // namespace masstree
}  // namespace storage
}  // namespace foedus

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
#include "foedus/storage/masstree/masstree_cursor.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <string>

#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/memory/engine_memory.hpp"
#include "foedus/memory/numa_core_memory.hpp"
#include "foedus/memory/page_pool.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_retry_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"
#include "foedus/xct/xct.hpp"


namespace foedus {
namespace storage {
namespace masstree {

MasstreeCursor::MasstreeCursor(MasstreeStorage storage, thread::Thread* context)
  : engine_(storage.get_engine()),
    storage_(storage.get_engine(), storage.get_control_block()),
    context_(context),
    current_xct_(&context->get_current_xct()) {
  for_writes_ = false;
  forward_cursor_ = true;
  reached_end_ = false;

  route_count_ = 0;
  routes_ = nullptr;
  should_skip_cur_route_ = false;

  end_inclusive_ = false;
  end_key_length_ = 0;
  end_key_ = nullptr;
  end_key_slices_ = nullptr;

  cur_route_prefix_slices_ = nullptr;
  cur_route_prefix_be_ = nullptr;

  cur_key_length_ = 0;
  cur_key_owner_id_address = nullptr;
  cur_key_suffix_ = nullptr;
  cur_key_in_layer_slice_ = 0;
  cur_key_in_layer_remainder_ = 0;
  cur_key_next_layer_ = false;
  cur_key_observed_owner_id_.data_ = 0;

  cur_payload_length_ = 0;

  search_key_length_ = 0;
  search_key_ = nullptr;
  search_key_slices_ = nullptr;
}

void MasstreeCursor::copy_combined_key(char* buffer) const {
  ASSERT_ND(is_valid_record());
  const Route* route = cur_route();
  const Layer layer = route->layer_;
  ASSERT_ND(layer == route->page_->get_layer());
  ASSERT_ND(cur_key_in_layer_remainder_ != kInitiallyNextLayer);
  ASSERT_ND(cur_key_length_ == cur_key_in_layer_remainder_ + layer * sizeof(KeySlice));

  std::memcpy(buffer, ASSUME_ALIGNED(cur_route_prefix_be_, 256), layer * sizeof(KeySlice));
  assorted::write_bigendian<KeySlice>(cur_key_in_layer_slice_, buffer + layer * sizeof(KeySlice));
  KeyLength suffix_length = calculate_suffix_length(cur_key_in_layer_remainder_);
  if (suffix_length > 0) {
    std::memcpy(
      buffer + (layer + 1U) * sizeof(KeySlice),
      ASSUME_ALIGNED(cur_key_suffix_, 8),
      suffix_length);
  }
}
void MasstreeCursor::copy_combined_key_part(KeyLength offset, KeyLength len, char* buffer) const {
  ASSERT_ND(is_valid_record());
  ASSERT_ND(offset + len <= cur_key_length_);
  const Route* route = cur_route();
  const Layer layer = route->layer_;
  ASSERT_ND(layer == route->page_->get_layer());
  ASSERT_ND(cur_key_in_layer_remainder_ != kInitiallyNextLayer);
  ASSERT_ND(cur_key_length_ == cur_key_in_layer_remainder_ + layer * sizeof(KeySlice));

  const KeyLength prefix_len = layer * sizeof(KeySlice);
  KeyLength buffer_pos = 0;
  KeyLength len_remaining = len;
  if (offset < prefix_len) {
    KeyLength prefix_copy_len = std::min<KeyLength>(prefix_len - offset, len_remaining);
    std::memcpy(buffer, cur_route_prefix_be_ + offset, prefix_copy_len);
    buffer_pos += prefix_copy_len;
    len_remaining -= prefix_copy_len;
  }

  if (len_remaining > 0) {
    if (offset < prefix_len + sizeof(KeySlice)) {
      char cur_slice_be[sizeof(KeySlice)];
      assorted::write_bigendian<KeySlice>(cur_key_in_layer_slice_, cur_slice_be);

      KeyLength cur_slice_offset = offset - prefix_len;
      KeyLength cur_slice_copy_len
        = std::min<KeyLength>(sizeof(KeySlice) - cur_slice_offset, len_remaining);
      std::memcpy(buffer + buffer_pos, cur_slice_be + cur_slice_offset, cur_slice_copy_len);
      buffer_pos += cur_slice_copy_len;
      len_remaining -= cur_slice_copy_len;
    }

    if (len_remaining > 0) {
      KeyLength suffix_offset = offset - prefix_len - sizeof(KeySlice);
      KeyLength suffix_length = calculate_suffix_length(cur_key_in_layer_remainder_);
      KeyLength suffix_copy_len = std::min<KeyLength>(suffix_length - suffix_offset, len_remaining);
      std::memcpy(
        buffer + buffer_pos,
        cur_key_suffix_ + suffix_offset,
        suffix_copy_len);
      buffer_pos += suffix_copy_len;
      len_remaining -= suffix_copy_len;
    }
  }

  ASSERT_ND(buffer_pos == len);
  ASSERT_ND(len_remaining == 0);
}

template <typename T>
inline ErrorCode MasstreeCursor::allocate_if_not_exist(T** pointer) {
  if (*pointer) {
    return kErrorCodeOk;
  }

  ASSERT_ND(*pointer == nullptr);
  void* out;
  CHECK_ERROR_CODE(current_xct_->acquire_local_work_memory(
    1U << 12,
    &out,
    1U << 12));
  *pointer = reinterpret_cast<T*>(out);
  return kErrorCodeOk;
}

MasstreePage* MasstreeCursor::resolve_volatile(VolatilePagePointer ptr) const {
  ASSERT_ND(!ptr.is_null());
  return reinterpret_cast<MasstreePage*>(
    engine_->get_memory_manager()->get_global_volatile_page_resolver().resolve_offset(ptr));
}

/////////////////////////////////////////////////////////////////////////////////////////
//
//      next() and proceed_xxx methods
//
/////////////////////////////////////////////////////////////////////////////////////////

ErrorCode MasstreeCursor::next() {
  ASSERT_ND(!should_skip_cur_route_);
  if (!is_valid_record()) {
    return kErrorCodeOk;
  }

  assert_route();
  while (true) {
    CHECK_ERROR_CODE(proceed_route());
    if (UNLIKELY(should_skip_cur_route_)) {
      LOG(INFO) << "Rare. Skipping empty page";
      CHECK_ERROR_CODE(proceed_pop());
      continue;
    }
    if (is_valid_record() && cur_key_observed_owner_id_.is_deleted()) {
      // when we follow to next layer, it is possible to locate a deleted record and stopped there.
      // in that case, we repeat it. Don't worry, in most cases we are skipping bunch of deleted
      // records at once.
      continue;
    } else {
      break;
    }
  }
  ASSERT_ND(!should_skip_cur_route_);
  check_end_key();
  if (is_valid_record()) {
    assert_route();
  }
  return kErrorCodeOk;
}

inline ErrorCode MasstreeCursor::proceed_route() {
  if (cur_route()->page_->is_border()) {
    return proceed_route_border();
  } else {
    return proceed_route_intermediate();
  }
}

ErrorCode MasstreeCursor::proceed_route_border() {
  Route* route = cur_route();
  ASSERT_ND(!route->stable_.is_moved());
  ASSERT_ND(route->page_->is_border());
  if (UNLIKELY(route->page_->get_version().status_ != route->stable_)) {
    // something has changed in this page.
    // TASK(Hideaki) until we implement range lock, we have to roll back in this case.
    return kErrorCodeXctRaceAbort;
  }

  PageVersionStatus stable = route->stable_;
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(route->page_);
  while (true) {
    if (forward_cursor_) {
      ++route->index_;
    } else {
      --route->index_;
    }
    if (route->index_ < route->key_count_) {
      fetch_cur_record(page, route->get_cur_original_index());
      if (!route->snapshot_ && !is_cur_key_next_layer()) {
        CHECK_ERROR_CODE(current_xct_->add_to_read_set(
          context_,
          storage_.get_id(),
          cur_key_observed_owner_id_,
          cur_key_owner_id_address,
          false));  // figure this out
      }
      // If it points to next-layer, we ignore deleted bit. It has no meaning for next-layer rec.
      if (is_cur_key_next_layer()) {
        CHECK_ERROR_CODE(proceed_next_layer());
      } else if (cur_key_observed_owner_id_.is_deleted()) {
        continue;
      }
      break;
    } else {
      CHECK_ERROR_CODE(proceed_pop());
      break;
    }
  }
  assorted::memory_fence_consume();
  if (UNLIKELY(page->get_version().status_ != stable)) {
    return kErrorCodeXctRaceAbort;  // same above
  }
  return kErrorCodeOk;
}

void MasstreeCursor::proceed_route_intermediate_rebase_separator() {
  Route* route = cur_route();
  ASSERT_ND(!route->page_->is_border());
  MasstreeIntermediatePage* page = reinterpret_cast<MasstreeIntermediatePage*>(route->page_);
  // We do NOT update stable_ here in case it's now moved.
  // even if it's moved now, we can keep using this node because of the master-tree invariant.
  // rather, if we update the stable_, proceed_pop will be confused by that
  route->key_count_ = route->page_->get_key_count();

  const KeySlice last = route->latest_separator_;
  ASSERT_ND(last <= page->get_high_fence() && last >= page->get_low_fence());
  for (route->index_ = 0; route->index_ < route->key_count_; ++route->index_) {
    if (last <= page->get_separator(route->index_)) {
      break;
    }
  }
  ASSERT_ND(route->index_ == route->key_count_ || last <= page->get_separator(route->index_));

  DVLOG(0) << "rebase. new index=" << route->index_ << "/" << route->key_count_;
  if (route->index_ < route->key_count_ && last == page->get_separator(route->index_)) {
    DVLOG(0) << "oh, matched with first level separator";
    // we are "done" up to the last separator. which pointer to follow next?
    if (forward_cursor_) {
      ++route->index_;
      const MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(route->index_);
      route->key_count_mini_ = minipage.key_count_;
      route->index_mini_ =  0;
    } else {
      const MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(route->index_);
      route->key_count_mini_ = minipage.key_count_;
      route->index_mini_ = minipage.key_count_;
    }
    return;
  }

  const MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(route->index_);
  do {
    route->key_count_mini_ = minipage.key_count_;
    DVLOG(0) << "checking second level... count=" << route->key_count_mini_;
    for (route->index_mini_ = 0;
         route->index_mini_ < route->key_count_mini_;
         ++route->index_mini_) {
      ASSERT_ND(last >= minipage.separators_[route->index_mini_]);
      if (last == minipage.separators_[route->index_mini_]) {
        break;
      }
    }
    ASSERT_ND(route->index_mini_ <= route->key_count_mini_);
  } while (route->key_count_mini_ != minipage.key_count_ ||
           route->index_mini_ == route->key_count_mini_);
  DVLOG(0) << "checked second level... index_mini=" << route->index_mini_;
  ASSERT_ND(route->index_mini_ < route->key_count_mini_);
  if (forward_cursor_) {
    // last==separators[n], so we are following pointers[n+1] next
    ++route->index_mini_;
  } else {
    // last==separators[n], so we are following pointers[n] next
  }
}

ErrorCode MasstreeCursor::proceed_route_intermediate() {
  while (true) {
    Route* route = cur_route();
    ASSERT_ND(!route->page_->is_border());
    ASSERT_ND(route->index_ <= route->key_count_);
    MasstreeIntermediatePage* page = reinterpret_cast<MasstreeIntermediatePage*>(route->page_);

    if (forward_cursor_) {
      ++route->index_mini_;
    } else {
      --route->index_mini_;
    }
    if (route->index_mini_ > route->key_count_mini_) {  // this is also a 'negative' check
      if (forward_cursor_) {
        ++route->index_;
      } else {
        --route->index_;
      }
      if (route->index_ > route->key_count_) {  // also a 'negative' check
        // seems like we reached end of this page... did we?
        return proceed_pop();
      } else {
        route->key_count_mini_ = page->get_minipage(route->index_).key_count_;
        if (forward_cursor_) {
          route->index_mini_ = 0;
        } else {
          route->index_mini_ = route->key_count_mini_;
        }
      }
    }

    // Master-tree invariant
    // verify that the next separator/page starts from the separator we followed previously.
    // as far as we check it, we don't need the hand-over-hand version verification.
    KeySlice new_separator;
    MasstreePage* next;
    while (true) {
      ASSERT_ND(route->latest_separator_ >= page->get_low_fence());
      ASSERT_ND(route->latest_separator_ <= page->get_high_fence());
      MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(route->index_);
      DualPagePointer& pointer = minipage.pointers_[route->index_mini_];
      ASSERT_ND(!pointer.is_both_null());
      CHECK_ERROR_CODE(
        MasstreeStoragePimpl(&storage_).follow_page(context_, for_writes_, &pointer, &next));

      if (forward_cursor_) {
        if (UNLIKELY(next->get_low_fence() != route->latest_separator_)) {
          VLOG(0) << "Interesting3A. separator doesn't match. concurrent adoption. local retry.";
          proceed_route_intermediate_rebase_separator();
          continue;
        }
        new_separator = next->get_high_fence();
      } else {
        if (UNLIKELY(next->get_high_fence() != route->latest_separator_)) {
          VLOG(0) << "Interesting3B. separator doesn't match. concurrent adoption. local retry.";
          proceed_route_intermediate_rebase_separator();
          continue;
        }
        new_separator = next->get_low_fence();
      }
      break;
    }

    route->latest_separator_ = new_separator;
    CHECK_ERROR_CODE(push_route(next));
    return proceed_deeper();
  }
  return kErrorCodeOk;
}

inline ErrorCode MasstreeCursor::proceed_pop() {
  while (true) {
    --route_count_;
    should_skip_cur_route_ = false;
    if (route_count_ == 0) {
      reached_end_ = true;
      return kErrorCodeOk;
    }
    Route& route = routes_[route_count_ - 1];
    if (route.stable_.is_moved()) {
      // in case we were at a moved page, we either followed foster minor or foster major.
      ASSERT_ND(route.moved_page_search_status_ == Route::kMovedPageSearchedOne ||
        route.moved_page_search_status_ == Route::kMovedPageSearchedBoth);
      if (route.moved_page_search_status_ == Route::kMovedPageSearchedBoth) {
        // we checked both foster children. we are done with this. so, pop again
        continue;
      }
      route.moved_page_search_status_ = Route::kMovedPageSearchedBoth;
      MasstreePage* left = resolve_volatile(route.page_->get_foster_minor());
      MasstreePage* right = resolve_volatile(route.page_->get_foster_major());
      // check another foster child
      CHECK_ERROR_CODE(push_route(forward_cursor_ ? right : left));
      return proceed_deeper();
    } else {
      // otherwise, next record in this page
      return proceed_route();
    }
  }
}
inline ErrorCode MasstreeCursor::proceed_next_layer() {
  Route* route = cur_route();
  ASSERT_ND(route->page_->is_border());
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(route->page_);
  Layer layer = page->get_layer();
  KeySlice record_slice = page->get_slice(route->get_cur_original_index());
  cur_route_prefix_slices_[layer] = record_slice;
  assorted::write_bigendian<KeySlice>(
    record_slice,
    cur_route_prefix_be_ + (layer * sizeof(KeySlice)));
  DualPagePointer* pointer = page->get_next_layer(route->get_cur_original_index());
  MasstreePage* next;
  CHECK_ERROR_CODE(
    MasstreeStoragePimpl(&storage_).follow_page(context_, for_writes_, pointer, &next));
  CHECK_ERROR_CODE(push_route(next));
  return proceed_deeper();
}

inline ErrorCode MasstreeCursor::proceed_deeper() {
  // if we are hitting a moved page, go to left or right, depending on forward cur or not
  while (UNLIKELY(cur_route()->stable_.is_moved())) {
    MasstreePage* next_page = resolve_volatile(
      forward_cursor_
      ? cur_route()->page_->get_foster_minor()
      : cur_route()->page_->get_foster_major());
    ASSERT_ND(cur_route()->moved_page_search_status_ == Route::kMovedPageSearchedNeither);
    cur_route()->moved_page_search_status_ = Route::kMovedPageSearchedOne;
    CHECK_ERROR_CODE(push_route(next_page));
  }
  ASSERT_ND(!cur_route()->stable_.is_moved());

  if (cur_route()->page_->is_border()) {
    return proceed_deeper_border();
  } else {
    return proceed_deeper_intermediate();
  }
}

inline ErrorCode MasstreeCursor::proceed_deeper_border() {
  Route* route = cur_route();
  ASSERT_ND(route->page_->is_border());
  ASSERT_ND(!route->stable_.is_moved());
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(cur_route()->page_);

  // We might have an empty border page in the route. We just skip over such a page.
  if (route->key_count_ == 0) {
    LOG(INFO) << "Huh, rare but possible. A cursor hit an empty border page. Just skips";
    route->index_ = 0;
    should_skip_cur_route_ = true;
    ASSERT_ND(!is_valid_record());
    return kErrorCodeOk;  // next proceed_route will take care of it.
  }

  ASSERT_ND(route->key_count_ > 0);
  route->index_ = forward_cursor_ ? 0 : route->key_count_ - 1;
  SlotIndex record = route->get_original_index(route->index_);
  fetch_cur_record(page, record);

  if (!route->snapshot_ && !is_cur_key_next_layer()) {
    CHECK_ERROR_CODE(current_xct_->add_to_read_set(
      context_,
      storage_.get_id(),
      cur_key_observed_owner_id_,
      cur_key_owner_id_address,
      false));  // TODO(tzwang): figure this out
  }

  if (is_cur_key_next_layer()) {
    return proceed_next_layer();
  }
  return kErrorCodeOk;
}

inline ErrorCode MasstreeCursor::proceed_deeper_intermediate() {
  Route* route = cur_route();
  ASSERT_ND(!route->page_->is_border());
  ASSERT_ND(!route->stable_.is_moved());
  MasstreeIntermediatePage* page = reinterpret_cast<MasstreeIntermediatePage*>(cur_route()->page_);
  route->index_ = forward_cursor_ ? 0 : route->key_count_;
  MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(route->index_);

  MasstreePage* next;
  KeySlice separator_low, separator_high;
  while (true) {
    route->key_count_mini_ = minipage.key_count_;
    assorted::memory_fence_consume();
    route->index_mini_ = forward_cursor_ ? 0 : route->key_count_mini_;

    extract_separators(&separator_low, &separator_high);
    DualPagePointer& pointer = minipage.pointers_[route->index_mini_];
    ASSERT_ND(!pointer.is_both_null());
    CHECK_ERROR_CODE(
      MasstreeStoragePimpl(&storage_).follow_page(context_, for_writes_, &pointer, &next));
    if (UNLIKELY(next->get_low_fence() != separator_low ||
        next->get_high_fence() != separator_high)) {
      VLOG(0) << "Interesting4. first sep doesn't match. concurrent adoption. local retry.";
      continue;
    } else {
      break;
    }
  }

  route->latest_separator_ = forward_cursor_ ? separator_high : separator_low;
  CHECK_ERROR_CODE(push_route(next));
  return proceed_deeper();
}

/////////////////////////////////////////////////////////////////////////////////////////
//
//      common methods
//
/////////////////////////////////////////////////////////////////////////////////////////

void MasstreeCursor::fetch_cur_record(MasstreeBorderPage* page, SlotIndex record) {
  // fetch everything
  ASSERT_ND(record < page->get_key_count());
  cur_key_owner_id_address = page->get_owner_id(record);
  cur_key_observed_owner_id_ = cur_key_owner_id_address->xct_id_.spin_while_being_written();
  KeyLength remainder = page->get_remainder_length(record);
  cur_key_in_layer_remainder_ = remainder;
  cur_key_in_layer_slice_ = page->get_slice(record);
  Layer layer = page->get_layer();
  cur_key_length_ = layer * sizeof(KeySlice) + remainder;
  if (!is_cur_key_next_layer()) {
    cur_key_suffix_ = page->get_record(record);
    cur_payload_length_ = page->get_payload_length(record);
    cur_payload_ = page->get_record_payload(record);
  } else {
    cur_key_suffix_ = nullptr;
    cur_payload_length_ = 0;
    cur_payload_ = nullptr;
  }
}

inline void MasstreeCursor::Route::setup_order() {
  ASSERT_ND(page_->is_border());
  // sort entries in this page
  // we have already called prefetch_general(), which prefetches 4 cachelines (256 bytes).
  // as we are reading up to slices_[key_count - 1], we might want to prefetch more.
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(page_);
  for (SlotIndex i = 0; i < key_count_; ++i) {
    order_[i] = i;
  }

  if (page->is_consecutive_inserts()) {
    DVLOG(2) << "lucky, an already sorted border page.";
    return;
  }

  page->prefetch_additional_if_needed(key_count_);
  struct Sorter {
    explicit Sorter(const MasstreeBorderPage* target) : target_(target) {}
    bool operator() (SlotIndex left, SlotIndex right) {
      KeySlice left_slice = target_->get_slice(left);
      KeySlice right_slice = target_->get_slice(right);
      if (left_slice < right_slice) {
        return true;
      } else if (left_slice == right_slice) {
        return target_->get_remainder_length(left) < target_->get_remainder_length(right);
      } else {
        return false;
      }
    }
    const MasstreeBorderPage* target_;
  };

  // this sort order in page is correct even without evaluating the suffix.
  // however, to compare with our searching key, we need to consider suffix
  std::sort(order_, order_ + key_count_, Sorter(page));
}

inline ErrorCode MasstreeCursor::push_route(MasstreePage* page) {
  assert_aligned_page(page);
  if (route_count_ == kMaxRoutes) {
    return kErrorCodeStrMasstreeCursorTooDeep;
  }
  page->prefetch_general();
  Route& route = routes_[route_count_];
  while (true) {
    route.key_count_ = page->get_key_count();
    assorted::memory_fence_consume();
    route.stable_ = page->get_version().status_;
    route.page_ = page;
    if (route.stable_.is_moved()) {
      route.moved_page_search_status_ = Route::kMovedPageSearchedNeither;
    } else {
      route.moved_page_search_status_ = Route::kNotMovedPage;
    }
    route.latest_separator_ = kInfimumSlice;  // must be set shortly after this method
    route.index_ = kMaxRecords;  // must be set shortly after this method
    route.index_mini_ = kMaxRecords;  // must be set shortly after this method
    should_skip_cur_route_ = false;
    route.snapshot_ = page->header().snapshot_;
    route.layer_ = page->get_layer();
    if (page->is_border() && !route.stable_.is_moved()) {
      route.setup_order();
      assorted::memory_fence_consume();
      // the setup_order must not be confused by concurrent updates.
      // because we check version after consume fence, this also catches the case where
      // we have a new key, is_consecutive_inserts()==true no longer holds, etc.
      if (UNLIKELY(route.stable_ != page->get_version().status_)) {
        continue;
      }
    }
    break;
  }

  ++route_count_;
  // We don't need to take a page into the page version set unless we need to lock a range in it.
  // We thus need it only for border pages. Even if an interior page changes, splits, whatever,
  // the pre-existing border pages are already responsible for the searched key regions.
  // this is an outstanding difference from original masstree/silo protocol.
  // we also don't have to consider moved pages. they are stable.
  if (!page->is_border() || page->header().snapshot_ || route.stable_.is_moved()) {
    return kErrorCodeOk;
  }
  return current_xct_->add_to_page_version_set(&page->header().page_version_, route.stable_);
}

inline ErrorCode MasstreeCursor::follow_foster(KeySlice slice) {
  // a bit more complicated than point queries because of exclusive cases.
  while (true) {
    Route* route = cur_route();
    ASSERT_ND(search_type_ == kBackwardExclusive || route->page_->within_fences(slice));
    ASSERT_ND(search_type_ != kBackwardExclusive
      || route->page_->within_fences(slice)
      || route->page_->get_high_fence() == slice);
    if (LIKELY(!route->stable_.is_moved())) {
      break;
    }

    ASSERT_ND(route->stable_.is_moved());
    ASSERT_ND(route->moved_page_search_status_ == Route::kMovedPageSearchedNeither
      || route->moved_page_search_status_ == Route::kMovedPageSearchedOne);  // see locate_border
    KeySlice foster_fence = route->page_->get_foster_fence();
    MasstreePage* left = resolve_volatile(route->page_->get_foster_minor());
    MasstreePage* right = resolve_volatile(route->page_->get_foster_major());
    MasstreePage* page;
    if (slice < foster_fence) {
      page = left;
    } else if (slice > foster_fence) {
      page = right;
    } else {
      ASSERT_ND(slice == foster_fence);
      if (search_type_ == kBackwardExclusive) {
        page = left;
      } else {
        page = right;
      }
    }
    // if we are following the right foster, 1) if forward-cursor, we already skipeed left,
    // so we are "both" done. 2) otherwise, just one of them done. vice versa.
    if ((forward_cursor_ && page == left) || (!forward_cursor_ && page == right)) {
      route->moved_page_search_status_ = Route::kMovedPageSearchedOne;
    } else {
      route->moved_page_search_status_ = Route::kMovedPageSearchedBoth;
    }
    CHECK_ERROR_CODE(push_route(page));
  }
  return kErrorCodeOk;
}

inline void MasstreeCursor::extract_separators(
  KeySlice* separator_low,
  KeySlice* separator_high) const {
  // so far this method does not call MasstreeIntermediatePage::extract_separators() to avoid
  // relying on key_count in the page (this method uses Route's counts which are taken with fences).
  // I suspect it's okay to call it (and to reduce code), lets revisit later.
  const Route* route = cur_route();
  ASSERT_ND(!route->page_->is_border());
  ASSERT_ND(route->index_ <= route->key_count_);
  ASSERT_ND(route->index_mini_ <= route->key_count_mini_);
  MasstreeIntermediatePage* cur = reinterpret_cast<MasstreeIntermediatePage*>(route->page_);
  const MasstreeIntermediatePage::MiniPage& minipage = cur->get_minipage(route->index_);
  if (route->index_mini_ == 0) {
    if (route->index_ == 0) {
      *separator_low = cur->get_low_fence();
    } else {
      *separator_low = cur->get_separator(route->index_ - 1U);
    }
  } else {
    *separator_low = minipage.separators_[route->index_mini_ - 1U];
  }
  if (route->index_mini_ == route->key_count_mini_) {
    if (route->index_ == route->key_count_) {
      *separator_high = cur->get_high_fence();
    } else {
      *separator_high = cur->get_separator(route->index_);
    }
  } else {
    *separator_high = minipage.separators_[route->index_mini_];
  }
}

inline void MasstreeCursor::check_end_key() {
  if (is_valid_record()) {
    KeyCompareResult result = compare_cur_key_aginst_end_key();
    ASSERT_ND(result != kCurKeyContains && result != kCurKeyBeingsWith);
    bool ended = false;
    if (result == kCurKeySmaller) {
      if (!forward_cursor_) {
        reached_end_ = true;
      }
    } else if (result == kCurKeyLarger) {
      if (forward_cursor_) {
        reached_end_ = true;
      }
    } else {
      ASSERT_ND(result == kCurKeyEquals);
      if (!end_inclusive_) {
        reached_end_ = true;
      }
    }
    if (ended) {
      reached_end_ = true;
    }
  }
}

inline MasstreeCursor::KeyCompareResult MasstreeCursor::compare_cur_key_aginst_search_key(
  KeySlice slice,
  uint8_t layer) const {
  if (is_search_key_extremum() || search_key_length_ <= layer * sizeof(KeySlice)) {
    if (forward_cursor_) {
      return kCurKeyLarger;
    } else {
      return kCurKeySmaller;
    }
  }

#ifndef NDEBUG
  // The fact that we are in this page means the page or its descendants can contain search key.
  // Let's check.
  for (Layer i = 0; i < layer && sizeof(KeySlice) * i < search_key_length_; ++i) {
    if (is_forward_cursor()) {
      ASSERT_ND(search_key_slices_[i] <= cur_route_prefix_slices_[i]);
    } else {
      ASSERT_ND(search_key_slices_[i] >= cur_route_prefix_slices_[i]);
    }
  }
#endif  // NDEBUG
  return compare_cur_key(slice, layer, search_key_, search_key_length_);
}

inline MasstreeCursor::KeyCompareResult MasstreeCursor::compare_cur_key_aginst_end_key() const {
  if (is_end_key_supremum()) {
    return forward_cursor_ ? kCurKeySmaller : kCurKeyLarger;
  }

  ASSERT_ND(!is_cur_key_next_layer());
  const Layer layer = cur_route()->layer_;

  // TASK(Hideaki): We don't have to compare prefixes each time.
  // We can remember whether the end_key is trivially satisfied in route.
  // So far we compare each time... let's see what CPU profile says.
  for (Layer i = 0; i < layer && sizeof(KeySlice) * i < end_key_length_; ++i) {
    if (end_key_slices_[i] > cur_route_prefix_slices_[i]) {
      return kCurKeySmaller;
    } else if (end_key_slices_[i] < cur_route_prefix_slices_[i]) {
      return kCurKeyLarger;
    }
  }
  // Was the last slice a complete one?
  // For example, end-key="123456", layer=1. end_key_slices_[0] was incomplete.
  // We know cur_route_prefix_slices_[0] was "123456  " (space as \0). So it's actually different.
  if (sizeof(KeySlice) * layer > end_key_length_) {
    ASSERT_ND(cur_key_length_ > end_key_length_);
    if (is_forward_cursor()) {
      return kCurKeyLarger;
    } else {
      return kCurKeySmaller;
    }
  }

  // okay, all prefix slices were exactly the same.
  // We have to compare in-layer slice and suffix

  // 1. in-layer slice.
  KeySlice end_slice = end_key_slices_[layer];
  if (cur_key_in_layer_slice_ < end_slice) {
    return kCurKeySmaller;
  } else if (cur_key_in_layer_slice_ > end_slice) {
    return kCurKeyLarger;
  }

  KeyLength end_remainder = end_key_length_ - sizeof(KeySlice) * layer;
  if (cur_key_in_layer_remainder_ <= sizeof(KeySlice) || end_remainder <= sizeof(KeySlice)) {
    if (cur_key_in_layer_remainder_ == end_remainder) {
      return kCurKeyEquals;
    } else if (cur_key_in_layer_remainder_ >= end_remainder) {
      return kCurKeyLarger;
    } else {
      return kCurKeySmaller;
    }
  }

  // 2. suffix
  ASSERT_ND(cur_key_in_layer_remainder_ > sizeof(KeySlice));
  ASSERT_ND(end_remainder > sizeof(KeySlice));
  KeyLength cur_suffix_len = cur_key_in_layer_remainder_ - sizeof(KeySlice);
  KeyLength end_suffix_len = end_remainder - sizeof(KeySlice);
  KeyLength min_suffix_len = std::min(cur_suffix_len, end_suffix_len);

  int cmp = std::memcmp(
    cur_key_suffix_,
    end_key_ + (layer + 1U) * sizeof(KeySlice),
    min_suffix_len);
  if (cmp < 0) {
    return kCurKeySmaller;
  } else if (cmp > 0) {
    return kCurKeyLarger;
  } else {
    if (cur_key_length_ > end_key_length_) {
      return kCurKeySmaller;
    } else if (cur_key_length_ < end_key_length_) {
      return kCurKeyLarger;
    } else {
      return kCurKeyEquals;
    }
  }
}

inline MasstreeCursor::KeyCompareResult MasstreeCursor::compare_cur_key(
  KeySlice slice,
  uint8_t layer,
  const char* full_key,
  KeyLength full_length) const {
  ASSERT_ND(full_length >= layer * sizeof(KeySlice));
  KeyLength remainder = full_length - layer * sizeof(KeySlice);
  if (is_cur_key_next_layer()) {
    // treat this case separately
    if (cur_key_in_layer_slice_ < slice) {
      return kCurKeySmaller;
    } else if (cur_key_in_layer_slice_ > slice) {
      return kCurKeyLarger;
    } else if (remainder < sizeof(KeySlice)) {
      return kCurKeyLarger;
    } else if (remainder == sizeof(KeySlice)) {
      return kCurKeyBeingsWith;
    } else {
      return kCurKeyContains;
    }
  } else if (cur_key_in_layer_slice_ < slice) {
    return kCurKeySmaller;
  } else if (cur_key_in_layer_slice_ > slice) {
    return kCurKeyLarger;
  } else {
    if (cur_key_in_layer_remainder_ <= sizeof(KeySlice)) {
      // then simply length determines the <,>,== relation
      if (cur_key_in_layer_remainder_ < remainder) {
        return kCurKeySmaller;
      } else if (cur_key_in_layer_remainder_ == remainder) {
        // exactly matches.
        return kCurKeyEquals;
      } else {
        return kCurKeyLarger;
      }
    } else if (remainder <= sizeof(KeySlice)) {
      return kCurKeyLarger;
    } else {
      // we have to compare suffix. which suffix is longer?
      KeyLength min_length = std::min<KeyLength>(remainder, cur_key_in_layer_remainder_);
      int cmp = std::memcmp(
        cur_key_suffix_,
        full_key + (layer + 1U) * sizeof(KeySlice),
        min_length - sizeof(KeySlice));
      if (cmp < 0) {
        return kCurKeySmaller;
      } else if (cmp > 0) {
        return kCurKeyLarger;
      } else {
        if (cur_key_in_layer_remainder_ < remainder) {
          return kCurKeySmaller;
        } else if (cur_key_in_layer_remainder_ == remainder) {
          return kCurKeyEquals;
        } else {
          return kCurKeyLarger;
        }
      }
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////
//
//      Initial open() and locate() methods
//
/////////////////////////////////////////////////////////////////////////////////////////

void copy_input_key(const char* input_key, KeyLength length, char* buffer, KeySlice* slice_buffer) {
  std::memcpy(ASSUME_ALIGNED(buffer, 256), input_key, length);
  for (Layer i = 0; i * sizeof(KeySlice) < length; ++i) {
    slice_buffer[i] = slice_key(input_key + i * sizeof(KeySlice), length - i * sizeof(KeySlice));
  }
}

ErrorCode MasstreeCursor::open(
  const char* begin_key,
  KeyLength begin_key_length,
  const char* end_key,
  KeyLength end_key_length,
  bool forward_cursor,
  bool for_writes,
  bool begin_inclusive,
  bool end_inclusive) {
  CHECK_ERROR_CODE(allocate_if_not_exist(&routes_));
  CHECK_ERROR_CODE(allocate_if_not_exist(&search_key_));
  CHECK_ERROR_CODE(allocate_if_not_exist(&search_key_slices_));
  CHECK_ERROR_CODE(allocate_if_not_exist(&cur_route_prefix_slices_));
  CHECK_ERROR_CODE(allocate_if_not_exist(&cur_route_prefix_be_));
  CHECK_ERROR_CODE(allocate_if_not_exist(&end_key_));
  CHECK_ERROR_CODE(allocate_if_not_exist(&end_key_slices_));

  if (!current_xct_->is_active()) {
    return kErrorCodeXctNoXct;
  }

  forward_cursor_ = forward_cursor;
  reached_end_ = false;
  for_writes_ = for_writes;
  end_inclusive_ = end_inclusive;
  end_key_length_ = end_key_length;
  route_count_ = 0;
  if (!is_end_key_supremum()) {
    copy_input_key(end_key, end_key_length, end_key_, end_key_slices_);
  }

  search_key_length_ = begin_key_length;
  search_type_ = forward_cursor ? (begin_inclusive ? kForwardInclusive : kForwardExclusive)
                  : (begin_inclusive ? kBackwardInclusive : kBackwardExclusive);
  if (!is_search_key_extremum()) {
    copy_input_key(begin_key, begin_key_length, search_key_, search_key_slices_);
  }

  MasstreeIntermediatePage* root;
  MasstreeStoragePimpl pimpl(&storage_);
  CHECK_ERROR_CODE(pimpl.get_first_root(context_, for_writes, &root));
  CHECK_ERROR_CODE(push_route(root));
  CHECK_ERROR_CODE(locate_layer(0));
  while (UNLIKELY(should_skip_cur_route_)) {
    // unluckily we hit an empty page or the page boundary in initial locate().
    // Let's skip the page by proceed_pop() and find a next page.
    // Note, it's a while, not if. It's very unlikely but possible that proceed_pop again
    // results in should_skip_cur_route_.
    ASSERT_ND(cur_route()->page_->is_border());
    ASSERT_ND(!is_valid_record());
    LOG(INFO) << "Rage. Skipping empty page";
    CHECK_ERROR_CODE(proceed_pop());
  }
  ASSERT_ND(!should_skip_cur_route_);
  check_end_key();
  if (is_valid_record()) {
    // locate_xxx doesn't take care of deleted record as it can't proceed to another page.
    // so, if the initially located record is a deleted record, use next() now.
    if (cur_key_observed_owner_id_.is_deleted()) {
      CHECK_ERROR_CODE(next());
    }
  }
  ASSERT_ND(!should_skip_cur_route_);
  return kErrorCodeOk;
}

inline ErrorCode MasstreeCursor::locate_layer(uint8_t layer) {
  MasstreePage* layer_root = cur_route()->page_;
  ASSERT_ND(layer_root->get_layer() == layer);
  // set up the search in this layer. What's the slice we will look for in this layer?
  KeySlice slice;
  search_key_in_layer_extremum_ = false;
  if (is_search_key_extremum() || search_key_length_ <= layer * sizeof(KeySlice)) {
    slice = forward_cursor_ ? kInfimumSlice : kSupremumSlice;
    search_key_in_layer_extremum_ = true;
  } else if (search_key_length_ >= (layer + 1U) * sizeof(KeySlice)) {
    slice = search_key_slices_[layer];
  } else {
    // if we don't have a full slice for this layer, cursor has to do a bit special thing.
    // remember that we might be doing backward search.
    if (forward_cursor_) {
      // fill unsed bytes with 0
      slice = kInfimumSlice;
    } else {
      // fill unsed bytes with FF
      slice = kSupremumSlice;
    }
    std::memcpy(
      &slice,
      search_key_ + layer * sizeof(KeySlice),
      search_key_length_ - layer * sizeof(KeySlice));
    slice = assorted::read_bigendian<KeySlice>(&slice);
  }

  ASSERT_ND(layer != 0 || !layer_root->is_border());  // layer0-root is always intermediate
  if (!layer_root->is_border()) {
    CHECK_ERROR_CODE(locate_descend(slice));
  }
  ASSERT_ND(cur_route()->page_->is_border());
  CHECK_ERROR_CODE(locate_border(slice));

#ifndef NDEBUG
  if (is_valid_record()) {
    KeySlice cur_key_slice_this_layer;
    if (cur_route()->layer_ == layer) {
      cur_key_slice_this_layer = cur_key_in_layer_slice_;
    } else {
      ASSERT_ND(cur_route()->layer_ > layer);
      cur_key_slice_this_layer = cur_route_prefix_slices_[layer];
    }
    if (forward_cursor_) {
      ASSERT_ND(cur_key_slice_this_layer >= slice);
    } else {
      ASSERT_ND(cur_key_slice_this_layer <= slice);
    }
  }
#endif  // NDEBUG

  return kErrorCodeOk;
}

ErrorCode MasstreeCursor::locate_border(KeySlice slice) {
  while (true) {
    CHECK_ERROR_CODE(follow_foster(slice));
    // Master-Tree invariant: we are in a page that contains this slice.
    // the only exception is that it's a backward-exclusive search and slice==high fence
    Route* route = cur_route();
    MasstreeBorderPage* border = reinterpret_cast<MasstreeBorderPage*>(route->page_);
    ASSERT_ND(border->get_low_fence() <= slice);
    ASSERT_ND(border->get_high_fence() >= slice);
    ASSERT_ND(search_type_ == kBackwardExclusive || border->within_fences(slice));
    ASSERT_ND(search_type_ != kBackwardExclusive
      || border->within_fences(slice)
      || border->get_high_fence() == slice);

    ASSERT_ND(!route->stable_.is_moved());

    if (route->key_count_ == 0) {
      LOG(INFO) << "Huh, rare but possible. Cursor's Initial locate() hits an empty border page.";
      route->index_ = 0;
      should_skip_cur_route_ = true;
      ASSERT_ND(!is_valid_record());
      break;
    }

    uint8_t layer = border->get_layer();
    // find right record. be aware of backward-exclusive case!
    SlotIndex index;

    // no need for fast-path for supremum.
    // almost always supremum-search is for backward search, so anyway it finds it first.
    // if we have supremum-search for forward search, we miss opportunity, but who does it...
    // same for infimum-search for backward.
    if (search_type_ == kForwardExclusive || search_type_ == kForwardInclusive) {
      for (index = 0; index < route->key_count_; ++index) {
        SlotIndex record = route->get_original_index(index);
        if (border->get_slice(record) < slice) {
          // if slice is strictly smaller, we are sure it's not the record we want. skip without
          // reading. Because of key-immutability and purely-increasing key count, this doesn't
          // violate serializability
          continue;
        }
        fetch_cur_record(border, record);
        ASSERT_ND(cur_key_in_layer_slice_ >= slice);
        KeyCompareResult result = compare_cur_key_aginst_search_key(slice, layer);
        if (result == kCurKeySmaller ||
          (result == kCurKeyEquals && search_type_ == kForwardExclusive)) {
          continue;
        }

        // okay, this seems to satisfy our search.
        // we need to add this to read set. even if it's deleted.
        // but if that's next layer pointer, don't bother. the pointer is always valid
        if (!route->snapshot_ && !is_cur_key_next_layer()) {
          CHECK_ERROR_CODE(current_xct_->add_to_read_set(
            context_,
            storage_.get_id(),
            cur_key_observed_owner_id_,
            cur_key_owner_id_address,
            false));  // TODO(tzwang): figure this out
        }
        break;
      }
    } else {
      for (index = route->key_count_ - 1; index < route->key_count_; --index) {
        SlotIndex record = route->get_original_index(index);
        if (border->get_slice(record) > slice) {
          continue;
        }
        fetch_cur_record(border, record);
        ASSERT_ND(cur_key_in_layer_slice_ <= slice);
        KeyCompareResult result = compare_cur_key_aginst_search_key(slice, layer);
        if (result == kCurKeyLarger ||
          (result == kCurKeyEquals && search_type_ == kBackwardExclusive)) {
          continue;
        }

        if (!route->snapshot_ && !is_cur_key_next_layer()) {
          CHECK_ERROR_CODE(current_xct_->add_to_read_set(
            context_,
            storage_.get_id(),
            cur_key_observed_owner_id_,
            cur_key_owner_id_address,
            false));  // TODO(tzwang): figure this out
        }
        break;
      }
    }
    route->index_ = index;
    assorted::memory_fence_consume();

    if (UNLIKELY(route->stable_ != border->get_version().status_)) {
      PageVersionStatus new_stable = border->get_version().status_;
      if (new_stable.is_moved()) {
        ASSERT_ND(!route->stable_.is_moved());
        ASSERT_ND(route->moved_page_search_status_ == Route::kNotMovedPage);
        // this page has split. it IS fine thanks to Master-Tree invariant.
        // go deeper to one of foster child
        // the previous follow_foster didn't know about this split (route not pushed)
        route->stable_ = new_stable;
        route->moved_page_search_status_ = Route::kMovedPageSearchedNeither;
        continue;
      } else {
        // this means something has been inserted to this page.
        // so far we don't have range-lock (one of many todos), so we have to
        // rollback in this case.
        return kErrorCodeXctRaceAbort;
      }
    } else {
      // done about this page
      if (route->index_ >= route->key_count_) {
        DVLOG(2) << "Initial locate() hits page boundary.";
        should_skip_cur_route_ = true;
        ASSERT_ND(!is_valid_record());
      }
      break;
    }
  }

  if (is_valid_record() && is_cur_key_next_layer()) {
    return locate_next_layer();
  }

  return kErrorCodeOk;
}


ErrorCode MasstreeCursor::locate_next_layer() {
  Route* route = cur_route();
  MasstreeBorderPage* border = reinterpret_cast<MasstreeBorderPage*>(route->page_);
  Layer layer = border->get_layer();
  KeySlice record_slice = border->get_slice(route->get_cur_original_index());
  cur_route_prefix_slices_[layer] = record_slice;
  assorted::write_bigendian<KeySlice>(
    record_slice,
    cur_route_prefix_be_ + (layer * sizeof(KeySlice)));
  DualPagePointer* pointer = border->get_next_layer(route->get_cur_original_index());
  MasstreePage* next;
  CHECK_ERROR_CODE(
    MasstreeStoragePimpl(&storage_).follow_page(context_, for_writes_, pointer, &next));
  CHECK_ERROR_CODE(push_route(next));
  return locate_layer(layer + 1U);
}

ErrorCode MasstreeCursor::locate_descend(KeySlice slice) {
  while (true) {
    CHECK_ERROR_CODE(follow_foster(slice));
    Route* route = cur_route();
    MasstreeIntermediatePage* cur = reinterpret_cast<MasstreeIntermediatePage*>(route->page_);
    ASSERT_ND(search_type_ == kBackwardExclusive || cur->within_fences(slice));
    ASSERT_ND(search_type_ != kBackwardExclusive
      || cur->within_fences(slice)
      || cur->get_high_fence() == slice);

    ASSERT_ND(!route->stable_.is_moved());
    // find right minipage. be aware of backward-exclusive case!
    SlotIndex index = 0;
    // fast path for supremum-search.
    if (search_key_in_layer_extremum_) {
      if (forward_cursor_) {
        index = 0;
      } else {
        index = route->key_count_;
      }
    } else if (search_type_ != kBackwardExclusive) {
      for (; index < route->key_count_; ++index) {
        if (slice < cur->get_separator(index)) {
          break;
        }
      }
    } else {
      for (; index < route->key_count_; ++index) {
        if (slice <= cur->get_separator(index)) {
          break;
        }
      }
    }
    route->index_ = index;
    MasstreeIntermediatePage::MiniPage& minipage = cur->get_minipage(route->index_);

    minipage.prefetch();
    route->key_count_mini_ = minipage.key_count_;

    SlotIndex index_mini = 0;
    if (search_key_in_layer_extremum_) {
      // fast path for supremum-search.
      if (forward_cursor_) {
        index_mini = 0;
      } else {
        index_mini = route->key_count_mini_;
      }
    } else if (search_type_ != kBackwardExclusive) {
        for (; index_mini < route->key_count_mini_; ++index_mini) {
          if (slice < minipage.separators_[index_mini]) {
            break;
          }
        }
    } else {
      for (; index_mini < route->key_count_mini_; ++index_mini) {
        if (slice <= minipage.separators_[index_mini]) {
          break;
        }
      }
    }
    route->index_mini_ = index_mini;

    KeySlice separator_low, separator_high;
    extract_separators(&separator_low, &separator_high);
    if (UNLIKELY(slice < separator_low || slice > separator_high)) {
      VLOG(0) << "Interesting5. separator doesn't match. concurrent adopt. local retry.";
      assorted::memory_fence_acquire();
      route->stable_ = cur->get_version().status_;
      route->key_count_ = cur->get_key_count();
      continue;
    }
    ASSERT_ND(separator_low <= slice && slice <= separator_high);

    DualPagePointer& pointer = minipage.pointers_[route->index_mini_];
    ASSERT_ND(!pointer.is_both_null());
    MasstreePage* next;
    CHECK_ERROR_CODE(
      MasstreeStoragePimpl(&storage_).follow_page(context_, for_writes_, &pointer, &next));

    // Master-tree invariant
    // verify that the followed page covers the key range we want.
    // as far as we check it, we don't need the hand-over-hand version verification.
    // what this page once covered will be forever reachable from this page.
    if (UNLIKELY(next->get_low_fence() != separator_low ||
        next->get_high_fence() != separator_high)) {
      VLOG(0) << "Interesting. separator doesn't match. concurrent adopt. local retry.";
      assorted::memory_fence_acquire();
      route->stable_ = cur->get_version().status_;
      route->key_count_ = cur->get_key_count();
      continue;
    }

    route->latest_separator_ = forward_cursor_ ? separator_high : separator_low;
    ASSERT_ND(next->get_low_fence() <= slice);
    ASSERT_ND(next->get_high_fence() >= slice);
    CHECK_ERROR_CODE(push_route(next));
    if (next->is_border()) {
      return kErrorCodeOk;
    } else {
      continue;
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////
//
//      APIs to modify current record
//
/////////////////////////////////////////////////////////////////////////////////////////
ErrorCode MasstreeCursor::overwrite_record(
  const void* payload,
  PayloadLength payload_offset,
  PayloadLength payload_count) {
  assert_modify();
  char key[kMaxKeyLength];
  copy_combined_key(key);
  return MasstreeStoragePimpl(&storage_).overwrite_general(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_observed_owner_id_,
    key,
    cur_key_length_,
    payload,
    payload_offset,
    payload_count);
}

template <typename PAYLOAD>
ErrorCode MasstreeCursor::overwrite_record_primitive(
  PAYLOAD payload,
  PayloadLength payload_offset) {
  assert_modify();
  char key[kMaxKeyLength];
  copy_combined_key(key);
  return MasstreeStoragePimpl(&storage_).overwrite_general(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_observed_owner_id_,
    key,
    cur_key_length_,
    &payload,
    payload_offset,
    sizeof(PAYLOAD));
}

ErrorCode MasstreeCursor::delete_record() {
  assert_modify();
  char key[kMaxKeyLength];
  copy_combined_key(key);
  return MasstreeStoragePimpl(&storage_).delete_general(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_observed_owner_id_,
    key,
    cur_key_length_);
}

template <typename PAYLOAD>
ErrorCode MasstreeCursor::increment_record(PAYLOAD* value, PayloadLength payload_offset) {
  assert_modify();
  char key[kMaxKeyLength];
  copy_combined_key(key);
  return MasstreeStoragePimpl(&storage_).increment_general<PAYLOAD>(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_observed_owner_id_,
    key,
    cur_key_length_,
    value,
    payload_offset);
}

void MasstreeCursor::assert_route_impl() const {
  for (uint16_t i = 0; i + 1U < route_count_; ++i) {
    const Route* route = routes_ + i;
    ASSERT_ND(route->page_);
    ASSERT_ND(route->layer_ == route->page_->get_layer());
    if (route->stable_.is_moved()) {
      // then we don't use any information in this path
    } else if (reinterpret_cast<Page*>(route->page_)->get_header().get_page_type()
      == kMasstreeBorderPageType) {
      ASSERT_ND(route->index_ < kMaxRecords);
      ASSERT_ND(route->index_ < route->key_count_);
    } else {
      ASSERT_ND(route->index_ <= route->key_count_);
      ASSERT_ND(route->index_ <= kMaxIntermediateSeparators);
      ASSERT_ND(route->index_mini_ <= route->key_count_mini_);
      ASSERT_ND(route->index_mini_ <= kMaxIntermediateMiniSeparators);
    }
  }
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

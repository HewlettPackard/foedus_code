/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
  routes_memory_offset_ = 0;

  end_inclusive_ = false;
  end_key_length_ = 0;
  end_key_ = nullptr;
  end_key_memory_offset_ = 0;

  cur_key_length_ = 0;
  cur_key_ = nullptr;
  cur_key_memory_offset_ = 0;
  cur_key_owner_id_address = nullptr;
  cur_key_in_layer_slice_ = 0;
  cur_key_in_layer_remaining_ = 0;

  cur_payload_length_ = 0;

  search_key_length_ = 0;
  search_key_ = nullptr;
  search_key_memory_offset_ = 0;
}

template <typename T>
void MasstreeCursor::release_if_exist(memory::PagePoolOffset* offset, T** pointer) {
  if (*offset) {
    ASSERT_ND(*pointer);
    context_->get_thread_memory()->release_free_snapshot_page(*offset);
    *offset = 0;
    *pointer = nullptr;
  }
}

MasstreeCursor::~MasstreeCursor() {
  release_if_exist(&routes_memory_offset_, &routes_);
  release_if_exist(&search_key_memory_offset_, &search_key_);
  release_if_exist(&cur_key_memory_offset_, &cur_key_);
  release_if_exist(&end_key_memory_offset_, &end_key_);
}

template <typename T>
inline ErrorCode MasstreeCursor::allocate_if_not_exist(
  memory::PagePoolOffset* offset,
  T** pointer) {
  if (*offset) {
    ASSERT_ND(*pointer);
    return kErrorCodeOk;
  }

  ASSERT_ND(*pointer == nullptr);
  *offset = context_->get_thread_memory()->grab_free_snapshot_page();
  if (*offset == 0) {
    return kErrorCodeMemoryNoFreePages;
  }

  *pointer = reinterpret_cast<T*>(
    context_->get_thread_memory()->get_snapshot_pool()->get_resolver().resolve_offset_newpage(
      *offset));
  return kErrorCodeOk;
}

MasstreePage* MasstreeCursor::resolve(VolatilePagePointer ptr) const {
  return reinterpret_cast<MasstreePage*>(
    engine_->get_memory_manager().get_global_volatile_page_resolver().resolve_offset(ptr));
}

/////////////////////////////////////////////////////////////////////////////////////////
//
//      next() and proceed_xxx methods
//
/////////////////////////////////////////////////////////////////////////////////////////

ErrorCode MasstreeCursor::next() {
  if (!is_valid_record()) {
    return kErrorCodeOk;
  }

  assert_route();
  while (true) {
    CHECK_ERROR_CODE(proceed_route());
    if (is_valid_record() && cur_key_observed_owner_id_.is_deleted()) {
      // when we follow to next layer, it is possible to locate a deleted record and stopped there.
      // in that case, we repeat it. Don't worry, in most cases we are skipping bunch of deleted
      // records at once.
      continue;
    } else {
      break;
    }
  }
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
    // TODO(Hideaki) until we implement range lock, we have to roll back in this case.
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
      if (!route->snapshot_ &&
        cur_key_in_layer_remaining_ != MasstreeBorderPage::kKeyLengthNextLayer) {
        CHECK_ERROR_CODE(current_xct_->add_to_read_set(
          storage_.get_id(),
          cur_key_observed_owner_id_,
          cur_key_owner_id_address));
      }
      if (cur_key_observed_owner_id_.is_deleted()) {
        continue;
      }
      if (cur_key_in_layer_remaining_ == MasstreeBorderPage::kKeyLengthNextLayer) {
        CHECK_ERROR_CODE(proceed_next_layer());
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
  route->key_count_mini_ = minipage.key_count_;
  DVLOG(0) << "checking second level... count=" << route->key_count_mini_;
  for (route->index_mini_ = 0; route->index_mini_ < route->key_count_mini_; ++route->index_mini_) {
    ASSERT_ND(last >= minipage.separators_[route->index_mini_]);
    if (last == minipage.separators_[route->index_mini_]) {
      break;
    }
  }
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
      MasstreePage* left = resolve(route.page_->get_foster_minor());
      MasstreePage* right = resolve(route.page_->get_foster_major());
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
  KeySlice record_slice = page->get_slice(route->get_cur_original_index());
  assorted::write_bigendian<KeySlice>(
    record_slice,
    cur_key_ + (page->get_layer() * sizeof(KeySlice)));
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
    MasstreePage* next_page = forward_cursor_
      ? resolve(cur_route()->page_->get_foster_minor())
      : resolve(cur_route()->page_->get_foster_major());
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
  route->index_ = forward_cursor_ ? 0 : route->key_count_ - 1;
  uint8_t record = route->get_original_index(route->index_);
  fetch_cur_record(page, record);

  if (!route->snapshot_ &&
    cur_key_in_layer_remaining_ != MasstreeBorderPage::kKeyLengthNextLayer) {
    CHECK_ERROR_CODE(current_xct_->add_to_read_set(
      storage_.get_id(),
      cur_key_observed_owner_id_,
      cur_key_owner_id_address));
  }

  if (cur_key_in_layer_remaining_ == MasstreeBorderPage::kKeyLengthNextLayer) {
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

void MasstreeCursor::fetch_cur_record(MasstreeBorderPage* page, uint8_t record) {
  // fetch everything
  ASSERT_ND(record < page->get_key_count());
  cur_key_owner_id_address = page->get_owner_id(record);
  if (!page->header().snapshot_) {
    cur_key_observed_owner_id_ = cur_key_owner_id_address->xct_id_;
  }
  uint8_t remaining = page->get_remaining_key_length(record);
  uint8_t suffix_length = 0;
  if (remaining > sizeof(KeySlice)) {
    suffix_length = remaining - sizeof(KeySlice);
  }
  cur_key_in_layer_remaining_ = remaining;
  cur_key_in_layer_slice_ = page->get_slice(record);
  uint8_t layer = page->get_layer();
  cur_key_length_ = layer * sizeof(KeySlice) + remaining;
  char* layer_key = cur_key_ + layer * sizeof(KeySlice);
  assorted::write_bigendian<KeySlice>(cur_key_in_layer_slice_, layer_key);
  ASSERT_ND(assorted::read_bigendian<KeySlice>(layer_key) == cur_key_in_layer_slice_);
  if (suffix_length > 0) {
    std::memcpy(cur_key_ + (layer + 1) * sizeof(KeySlice), page->get_record(record), suffix_length);
  }
  cur_payload_length_ = page->get_payload_length(record);
  cur_payload_ = page->get_record(record) + suffix_length;
}

inline void MasstreeCursor::Route::setup_order() {
  ASSERT_ND(page_->is_border());
  // sort entries in this page
  // we have already called prefetch_general(), which prefetches 4 cachelines (256 bytes).
  // as we are reading up to slices_[key_count - 1], we might want to prefetch more.
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(page_);
  for (uint8_t i = 0; i < key_count_; ++i) {
    order_[i] = i;
  }

  if (page->is_consecutive_inserts()) {
    DVLOG(2) << "lucky, an already sorted border page.";
    return;
  }

  const uint16_t prefetched = assorted::kCachelineSize * 4;
  const uint16_t end_of_remaining_key_length = 136U;
  uint16_t prefetch_upto = key_count_ * sizeof(KeySlice) + end_of_remaining_key_length;
  if (prefetch_upto > prefetched) {
    uint16_t additional_prefetches = (prefetch_upto - prefetched) % assorted::kCachelineSize + 1;
    char* base = reinterpret_cast<char*>(page) + prefetched;
    assorted::prefetch_cachelines(base, additional_prefetches);
  }
  struct Sorter {
    explicit Sorter(const MasstreeBorderPage* target) : target_(target) {}
    bool operator() (uint8_t left, uint8_t right) {
      KeySlice left_slice = target_->get_slice(left);
      KeySlice right_slice = target_->get_slice(right);
      if (left_slice < right_slice) {
        return true;
      } else if (left_slice == right_slice) {
        return target_->get_remaining_key_length(left) < target_->get_remaining_key_length(right);
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
    route.locate_miss_in_page_ = false;
    route.snapshot_ = page->header().snapshot_;
    if (page->is_border() && !route.stable_.is_moved()) {
      route.setup_order();
      assorted::memory_fence_consume();
      // the setup_order must not be confused by concurrent updates
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
    ASSERT_ND(route->moved_page_search_status_ == Route::kMovedPageSearchedNeither);
    KeySlice foster_fence = route->page_->get_foster_fence();
    MasstreePage* left = resolve(route->page_->get_foster_minor());
    MasstreePage* right = resolve(route->page_->get_foster_major());
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
  return compare_cur_key(slice, layer, search_key_, search_key_length_);
}

inline MasstreeCursor::KeyCompareResult MasstreeCursor::compare_cur_key_aginst_end_key() const {
  if (is_end_key_supremum()) {
    return forward_cursor_ ? kCurKeySmaller : kCurKeyLarger;
  }
  uint16_t min_length = std::min(cur_key_length_, end_key_length_);
  int cmp = std::memcmp(cur_key_, end_key_, min_length);
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
  uint16_t full_length) const {
  ASSERT_ND(full_length >= layer * sizeof(KeySlice));
  uint16_t remaining = full_length - layer * sizeof(KeySlice);
  if (cur_key_in_layer_remaining_ == MasstreeBorderPage::kKeyLengthNextLayer) {
    // treat this case separately
    if (cur_key_in_layer_slice_ < slice) {
      return kCurKeySmaller;
    } else if (cur_key_in_layer_slice_ > slice) {
      return kCurKeyLarger;
    } else if (remaining < sizeof(KeySlice)) {
      return kCurKeyLarger;
    } else if (remaining == sizeof(KeySlice)) {
      return kCurKeyBeingsWith;
    } else {
      return kCurKeyContains;
    }
  } else if (cur_key_in_layer_slice_ < slice) {
    return kCurKeySmaller;
  } else if (cur_key_in_layer_slice_ > slice) {
    return kCurKeyLarger;
  } else {
    if (cur_key_in_layer_remaining_ <= sizeof(KeySlice)) {
      // then simply length determines the <,>,== relation
      if (cur_key_in_layer_remaining_ < remaining) {
        return kCurKeySmaller;
      } else if (cur_key_in_layer_remaining_ == remaining) {
        // exactly matches.
        return kCurKeyEquals;
      } else {
        return kCurKeyLarger;
      }
    } else if (remaining <= sizeof(KeySlice)) {
      return kCurKeyLarger;
    } else {
      // we have to compare suffix. which suffix is longer?
      uint16_t min_length = std::min<uint16_t>(remaining, cur_key_in_layer_remaining_);
      int cmp = std::memcmp(
        cur_key_ + layer * sizeof(KeySlice),
        full_key + layer * sizeof(KeySlice),
        min_length - sizeof(KeySlice));
      if (cmp < 0) {
        return kCurKeySmaller;
      } else if (cmp > 0) {
        return kCurKeyLarger;
      } else {
        if (cur_key_in_layer_remaining_ < remaining) {
          return kCurKeySmaller;
        } else if (cur_key_in_layer_remaining_ == remaining) {
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

ErrorCode MasstreeCursor::open(
  const char* begin_key,
  uint16_t begin_key_length,
  const char* end_key,
  uint16_t end_key_length,
  bool forward_cursor,
  bool for_writes,
  bool begin_inclusive,
  bool end_inclusive) {
  CHECK_ERROR_CODE(allocate_if_not_exist(&routes_memory_offset_, &routes_));
  CHECK_ERROR_CODE(allocate_if_not_exist(&search_key_memory_offset_, &search_key_));
  CHECK_ERROR_CODE(allocate_if_not_exist(&cur_key_memory_offset_, &cur_key_));
  CHECK_ERROR_CODE(allocate_if_not_exist(&end_key_memory_offset_, &end_key_));

  forward_cursor_ = forward_cursor;
  reached_end_ = false;
  for_writes_ = for_writes;
  end_inclusive_ = end_inclusive;
  end_key_length_ = end_key_length;
  route_count_ = 0;
  if (!is_end_key_supremum()) {
    std::memcpy(end_key_, end_key, end_key_length);
  }

  search_key_length_ = begin_key_length;
  search_type_ = forward_cursor ? (begin_inclusive ? kForwardInclusive : kForwardExclusive)
                  : (begin_inclusive ? kBackwardInclusive : kBackwardExclusive);
  if (!is_search_key_extremum()) {
    std::memcpy(search_key_, begin_key, begin_key_length);
  }

  ASSERT_ND(storage_.get_control_block()->root_page_pointer_.volatile_pointer_.components.offset);
  VolatilePagePointer pointer = storage_.get_control_block()->root_page_pointer_.volatile_pointer_;
  MasstreePage* root = reinterpret_cast<MasstreePage*>(
    context_->get_global_volatile_page_resolver().resolve_offset(pointer));
  CHECK_ERROR_CODE(push_route(root));
  CHECK_ERROR_CODE(locate_layer(0));
  if (!is_valid_record() && cur_route()->locate_miss_in_page_) {
    // unluckily we hit the page boundary in initial locate(). let next() take care.
    ASSERT_ND(cur_route()->page_->is_border());
    cur_route()->index_ = forward_cursor_ ? cur_route()->key_count_ - 1 : 0;
    CHECK_ERROR_CODE(next());
  }
  check_end_key();
  if (is_valid_record()) {
    // locate_xxx doesn't take care of deleted record as it can't proceed to another page.
    // so, if the initially located record is a deleted record, use next() now.
    if (cur_key_observed_owner_id_.is_deleted()) {
      CHECK_ERROR_CODE(next());
    }
  }
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
    slice = assorted::read_bigendian<KeySlice>(search_key_ + layer * sizeof(KeySlice));
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

  if (!layer_root->is_border()) {
    CHECK_ERROR_CODE(locate_descend(slice));
  }
  CHECK_ERROR_CODE(locate_border(slice));
  ASSERT_ND(!is_valid_record() ||
    forward_cursor_ ||
    assorted::read_bigendian<KeySlice>(cur_key_ + layer * sizeof(KeySlice)) <= slice);
  ASSERT_ND(!is_valid_record() ||
    !forward_cursor_ ||
    assorted::read_bigendian<KeySlice>(cur_key_ + layer * sizeof(KeySlice)) >= slice);
  ASSERT_ND(cur_route()->page_->get_layer() != layer ||
    !is_valid_record() ||
    !forward_cursor_ ||
    cur_key_in_layer_slice_ >= slice);
  ASSERT_ND(cur_route()->page_->get_layer() != layer ||
    !is_valid_record() ||
    forward_cursor_ ||
    cur_key_in_layer_slice_ <= slice);
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
    uint8_t layer = border->get_layer();
    // find right record. be aware of backward-exclusive case!
    uint8_t index;

    // no need for fast-path for supremum.
    // almost always supremum-search is for backward search, so anyway it finds it first.
    // if we have supremum-search for forward search, we miss opportunity, but who does it...
    // same for infimum-search for backward.
    if (search_type_ == kForwardExclusive || search_type_ == kForwardInclusive) {
      for (index = 0; index < route->key_count_; ++index) {
        uint8_t record = route->get_original_index(index);
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
        if (!route->snapshot_ &&
          cur_key_in_layer_remaining_ != MasstreeBorderPage::kKeyLengthNextLayer) {
          CHECK_ERROR_CODE(current_xct_->add_to_read_set(
            storage_.get_id(),
            cur_key_observed_owner_id_,
            cur_key_owner_id_address));
        }
        break;
      }
    } else {
      for (index = route->key_count_ - 1; index < route->key_count_; --index) {
        uint8_t record = route->get_original_index(index);
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

        if (!route->snapshot_ &&
          cur_key_in_layer_remaining_ != MasstreeBorderPage::kKeyLengthNextLayer) {
          CHECK_ERROR_CODE(current_xct_->add_to_read_set(
            storage_.get_id(),
            cur_key_observed_owner_id_,
            cur_key_owner_id_address));
        }
        break;
      }
    }
    route->index_ = index;
    assorted::memory_fence_consume();

    if (UNLIKELY(route->stable_ != border->get_version().status_)) {
      PageVersionStatus new_stable = border->get_version().status_;
      if (new_stable.is_moved()) {
        // this page has split. it IS fine thanks to Master-Tree invariant.
        // go deeper to one of foster child
        route->stable_ = new_stable;
        route->moved_page_search_status_ = Route::kMovedPageSearchedOne;
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
        route->locate_miss_in_page_ = true;
      }
      break;
    }
  }

  if (is_valid_record() &&
    cur_key_in_layer_remaining_ == MasstreeBorderPage::kKeyLengthNextLayer) {
    return locate_next_layer();
  }

  return kErrorCodeOk;
}


ErrorCode MasstreeCursor::locate_next_layer() {
  Route* route = cur_route();
  MasstreeBorderPage* border = reinterpret_cast<MasstreeBorderPage*>(route->page_);
  KeySlice record_slice = border->get_slice(route->get_cur_original_index());
  assorted::write_bigendian<KeySlice>(
    record_slice,
    cur_key_ + (border->get_layer() * sizeof(KeySlice)));
  DualPagePointer* pointer = border->get_next_layer(route->get_cur_original_index());
  MasstreePage* next;
  CHECK_ERROR_CODE(
    MasstreeStoragePimpl(&storage_).follow_page(context_, for_writes_, pointer, &next));
  CHECK_ERROR_CODE(push_route(next));
  return locate_layer(border->get_layer() + 1U);
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
    uint8_t index = 0;
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

    uint8_t index_mini = 0;
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
  uint16_t payload_offset,
  uint16_t payload_count) {
  assert_modify();
  return MasstreeStoragePimpl(&storage_).overwrite_general(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_observed_owner_id_,
    cur_key_,
    cur_key_length_,
    payload,
    payload_offset,
    payload_count);
}

template <typename PAYLOAD>
ErrorCode MasstreeCursor::overwrite_record_primitive(PAYLOAD payload, uint16_t payload_offset) {
  assert_modify();
  return MasstreeStoragePimpl(&storage_).overwrite_general(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_observed_owner_id_,
    cur_key_,
    cur_key_length_,
    &payload,
    payload_offset,
    sizeof(PAYLOAD));
}

ErrorCode MasstreeCursor::delete_record() {
  assert_modify();
  return MasstreeStoragePimpl(&storage_).delete_general(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_observed_owner_id_,
    cur_key_,
    cur_key_length_);
}

template <typename PAYLOAD>
ErrorCode MasstreeCursor::increment_record(PAYLOAD* value, uint16_t payload_offset) {
  assert_modify();
  return MasstreeStoragePimpl(&storage_).increment_general<PAYLOAD>(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_observed_owner_id_,
    cur_key_,
    cur_key_length_,
    value,
    payload_offset);
}
}  // namespace masstree
}  // namespace storage
}  // namespace foedus

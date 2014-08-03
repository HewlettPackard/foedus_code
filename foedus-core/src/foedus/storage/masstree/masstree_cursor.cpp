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

MasstreeCursor::MasstreeCursor(Engine* engine, MasstreeStorage* storage, thread::Thread* context)
  : engine_(engine),
    storage_(storage),
    storage_pimpl_(storage->get_pimpl()),
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
  cur_payload_ = nullptr;
  cur_payload_memory_offset_ = 0;

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
  release_if_exist(&cur_payload_memory_offset_, &cur_payload_);
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
    context_->get_thread_memory()->get_snapshot_pool()->get_resolver().resolve_offset(*offset));
  return kErrorCodeOk;
}

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
  CHECK_ERROR_CODE(allocate_if_not_exist(&cur_payload_memory_offset_, &cur_payload_));
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

  ASSERT_ND(storage_pimpl_->first_root_pointer_.volatile_pointer_.components.offset);
  VolatilePagePointer pointer = storage_pimpl_->first_root_pointer_.volatile_pointer_;
  MasstreePage* root = reinterpret_cast<MasstreePage*>(
    context_->get_global_volatile_page_resolver().resolve_offset(pointer));
  PageVersion root_version;
  CHECK_ERROR_CODE(push_route(root, &root_version));
  CHECK_ERROR_CODE(locate_layer(0));
  check_end_key();
  if (is_valid_record()) {
    // locate_xxx doesn't take care of deleted record as it can't proceed to another page.
    // so, if the initially located record is deleted, use next() now.
    if (cur_key_observed_owner_id_.is_deleted()) {
      CHECK_ERROR_CODE(next());  // this also calls fetch_cur_payload()
    } else {
      fetch_cur_payload();
    }
  }
  return kErrorCodeOk;
}

void MasstreeCursor::fetch_cur_record(MasstreeBorderPage* page, uint8_t record) {
  // fetch everything except payload itself
  ASSERT_ND(record < page->get_version().get_key_count());
  cur_key_owner_id_address = page->get_owner_id(record);
  if (!page->header().snapshot_) {
    cur_key_observed_owner_id_ = cur_key_owner_id_address->spin_while_keylocked();
  }
  uint8_t remaining = page->get_remaining_key_length(record);
  cur_key_in_layer_remaining_ = remaining;
  cur_key_in_layer_slice_ = page->get_slice(record);
  uint8_t layer = page->get_layer();
  cur_key_length_ = layer * sizeof(KeySlice) + remaining;
  uint64_t be_slice = assorted::htobe<uint64_t>(page->get_slice(record));
  std::memcpy(
    cur_key_ + layer * sizeof(KeySlice),
    &be_slice,
    std::min<uint64_t>(remaining, sizeof(KeySlice)));
  if (remaining > sizeof(KeySlice)) {
    std::memcpy(
      cur_key_ + (layer + 1) * sizeof(KeySlice),
      page->get_record(record),
      remaining - sizeof(KeySlice));
  }
  cur_payload_length_ = page->get_payload_length(record);
}
void MasstreeCursor::fetch_cur_payload() {
  ASSERT_ND(is_valid_record());
  ASSERT_ND(cur_route()->page_->is_border());
  ASSERT_ND(cur_route()->index_ < cur_route()->key_count_);
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(cur_route()->page_);
  uint8_t record = cur_route()->get_cur_original_index();
  ASSERT_ND(record < cur_route()->key_count_);
  uint8_t suffix_length = 0;
  if (cur_key_in_layer_remaining_ > sizeof(KeySlice)) {
    suffix_length = cur_key_in_layer_remaining_ - sizeof(KeySlice);
  }
  std::memcpy(
    cur_payload_,
    page->get_record(record) + suffix_length,
    cur_payload_length_);
}

ErrorCode MasstreeCursor::next() {
  if (!is_valid_record()) {
    return kErrorCodeOk;
  }

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
    fetch_cur_payload();
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
  ASSERT_ND(route->page_->is_border());
  if (route->page_->get_version().data_ != route->stable_.data_) {
    // TODO(Hideaki) do the retry with done_upto
    return kErrorCodeXctRaceAbort;
  }
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(cur_route()->page_);
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
          storage_,
          cur_key_observed_owner_id_,
          cur_key_owner_id_address));
      }
      if (cur_key_observed_owner_id_.is_deleted()) {
        continue;
      }
      route->done_upto_ = cur_key_in_layer_slice_;
      route->done_upto_length_ = cur_key_in_layer_remaining_;
      if (cur_key_in_layer_remaining_ == MasstreeBorderPage::kKeyLengthNextLayer) {
        return proceed_next_layer();
      } else {
        return kErrorCodeOk;
      }
    } else {
      return proceed_pop();
    }
  }
  return kErrorCodeOk;
}

ErrorCode MasstreeCursor::proceed_route_intermediate() {
  Route* route = cur_route();
  ASSERT_ND(!route->page_->is_border());
  if (route->page_->get_version().data_ != route->stable_.data_) {
    // TODO(Hideaki) do the retry with done_upto
    return kErrorCodeXctRaceAbort;
  }
  MasstreeIntermediatePage* page = reinterpret_cast<MasstreeIntermediatePage*>(cur_route()->page_);
  while (true) {
    if (forward_cursor_) {
      ++route->index_mini_;
    } else {
      --route->index_mini_;
    }
    if (page->get_minipage(route->index_).mini_version_.data_ != route->stable_mini_.data_) {
      // TODO(Hideaki) do the retry with done_upto
      return kErrorCodeXctRaceAbort;
    }

    if (route->index_mini_ <= route->key_count_mini_) {
      DualPagePointer& pointer = page->get_minipage(route->index_).pointers_[route->index_mini_];
      ASSERT_ND(!pointer.is_both_null());
      MasstreePage* next;
      CHECK_ERROR_CODE(storage_pimpl_->follow_page(context_, for_writes_, &pointer, &next));
      PageVersion next_stable;
      CHECK_ERROR_CODE(push_route(next, &next_stable));
      return proceed_deeper();
    } else {
      if (forward_cursor_) {
        ++route->index_;
      } else {
        --route->index_;
      }
      if (route->index_ <= route->key_count_) {
        MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(route->index_);
        route->stable_mini_ = minipage.get_stable_version();
        route->key_count_mini_ = route->stable_mini_.get_key_count();
        assorted::memory_fence_consume();
        route->index_mini_ = forward_cursor_ ? 0 : route->key_count_mini_;

        DualPagePointer& pointer = minipage.pointers_[route->index_mini_];
        ASSERT_ND(!pointer.is_both_null());
        MasstreePage* next;
        CHECK_ERROR_CODE(storage_pimpl_->follow_page(context_, for_writes_, &pointer, &next));
        PageVersion next_stable;
        CHECK_ERROR_CODE(push_route(next, &next_stable));
        return proceed_deeper();
      } else {
        return proceed_pop();
      }
    }
  }
  return kErrorCodeOk;
}

inline ErrorCode MasstreeCursor::proceed_pop() {
  --route_count_;
  if (route_count_ == 0) {
    reached_end_ = true;
    return kErrorCodeOk;
  }
  return proceed_route();
}
inline ErrorCode MasstreeCursor::proceed_next_layer() {
  Route* route = cur_route();
  ASSERT_ND(route->page_->is_border());
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(cur_route()->page_);
  DualPagePointer* pointer = page->get_next_layer(route->get_cur_original_index());
  MasstreePage* next;
  CHECK_ERROR_CODE(storage_pimpl_->follow_page(context_, for_writes_, pointer, &next));
  PageVersion next_stable;
  CHECK_ERROR_CODE(push_route(next, &next_stable));
  return proceed_deeper();
}

inline ErrorCode MasstreeCursor::proceed_deeper() {
  if (cur_route()->page_->is_border()) {
    return proceed_deeper_border();
  } else {
    return proceed_deeper_intermediate();
  }
}
inline ErrorCode MasstreeCursor::proceed_deeper_border() {
  Route* route = cur_route();
  ASSERT_ND(route->page_->is_border());
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(cur_route()->page_);
  route->index_ = forward_cursor_ ? 0 : route->key_count_ - 1;
  uint8_t record = route->get_original_index(route->index_);
  fetch_cur_record(page, record);
  route->done_upto_ = cur_key_in_layer_slice_;
  route->done_upto_length_ = cur_key_in_layer_remaining_;

  if (!route->snapshot_ &&
    cur_key_in_layer_remaining_ != MasstreeBorderPage::kKeyLengthNextLayer) {
    CHECK_ERROR_CODE(current_xct_->add_to_read_set(
      storage_,
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
  MasstreeIntermediatePage* page = reinterpret_cast<MasstreeIntermediatePage*>(cur_route()->page_);
  uint8_t index = forward_cursor_ ? 0 : route->key_count_;
  MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(index);

  route->stable_mini_ = minipage.get_stable_version();
  route->key_count_mini_ = route->stable_mini_.get_key_count();
  assorted::memory_fence_consume();
  route->index_mini_ = forward_cursor_ ? 0 : route->key_count_mini_;

  DualPagePointer& pointer = minipage.pointers_[route->index_mini_];
  ASSERT_ND(!pointer.is_both_null());
  MasstreePage* next;
  CHECK_ERROR_CODE(storage_pimpl_->follow_page(context_, for_writes_, &pointer, &next));
  PageVersion next_stable;
  CHECK_ERROR_CODE(push_route(next, &next_stable));
  return proceed_deeper();
}

inline void MasstreeCursor::Route::setup_order() {
  ASSERT_ND(page_->is_border());
  // sort entries in this page
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
  for (uint8_t i = 0; i < key_count_; ++i) {
    order_[i] = i;
  }

  // this sort order in page is correct even without evaluating the suffix.
  // however, to compare with our searching key, we need to consider suffix
  MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(page_);
  std::sort(order_, order_ + key_count_, Sorter(page));
}

inline ErrorCode MasstreeCursor::push_route(MasstreePage* page, PageVersion* page_version) {
  if (route_count_ == kMaxRoutes) {
    return kErrorCodeStrMasstreeCursorTooDeep;
  }
  page->prefetch_general();
  Route& route = routes_[route_count_];
  while (true) {
    *page_version = page->get_stable_version();
    ASSERT_ND(!page_version->is_locked());
    route.page_ = page;
    route.index_ = kMaxRecords;
    route.index_mini_ = kMaxRecords;
    route.stable_ = *page_version;
    route.key_count_ = page_version->get_key_count();
    route.snapshot_ = page->header().snapshot_;
    if (page->is_border() && !page_version->is_moved()) {
      route.setup_order();
    }
    assorted::memory_fence_consume();
    if (page_version->data_ == page->get_stable_version().data_) {
      break;
    }
  }
  ++route_count_;
  // We don't need to take a page into the page version set unless we need to lock a range in it.
  // We thus need it only for border pages. Even if an interior page changes, splits, whatever,
  // the pre-existing border pages are already responsible for the searched key regions.
  // this is an outstanding difference from original masstree/silo protocol.
  if (!page->is_border() || page->header().snapshot_) {
    return kErrorCodeOk;
  }
  return current_xct_->add_to_page_version_set(&page->header().page_version_, *page_version);
}

template<typename PAGE_TYPE>
inline ErrorCode MasstreeCursor::follow_foster(
  KeySlice slice,
  PAGE_TYPE** cur,
  PageVersion* version) {
  ASSERT_ND(search_type_ == kBackwardExclusive || (*cur)->within_fences(slice));
  ASSERT_ND(search_type_ != kBackwardExclusive
    || (*cur)->within_fences(slice)
    || (*cur)->get_high_fence() == slice);
  // a bit more complicated than point queries because of exclusive cases.
  while (UNLIKELY(version->has_foster_child())) {
    ASSERT_ND(version->is_moved());
    KeySlice foster_fence = (*cur)->get_foster_fence();
    if (slice < foster_fence) {
      *cur = reinterpret_cast<PAGE_TYPE*>((*cur)->get_foster_minor());
    } else if (slice > foster_fence) {
      *cur = reinterpret_cast<PAGE_TYPE*>((*cur)->get_foster_major());
    } else {
      ASSERT_ND(slice == foster_fence);
      if (search_type_ == kBackwardExclusive) {
        *cur = reinterpret_cast<PAGE_TYPE*>((*cur)->get_foster_minor());
      } else {
        *cur = reinterpret_cast<PAGE_TYPE*>((*cur)->get_foster_major());
      }
    }
    CHECK_ERROR_CODE(push_route(*cur, version));
  }
  return kErrorCodeOk;
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
  if (is_search_key_extremum()) {
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
    return kCurKeySmaller;
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

inline ErrorCode MasstreeCursor::locate_layer(uint8_t layer) {
  MasstreePage* layer_root = cur_route()->page_;
  ASSERT_ND(layer_root->get_layer() == layer);
  KeySlice slice;
  if (is_search_key_extremum()) {
    slice = forward_cursor_ ? kInfimumSlice : kSupremumSlice;
  } else {
    slice = slice_layer(search_key_, search_key_length_, layer);
  }
  if (!layer_root->is_border()) {
    CHECK_ERROR_CODE(locate_descend(slice));
  }
  return locate_border(slice);
}

ErrorCode MasstreeCursor::locate_border(KeySlice slice) {
  while (true) {
    MasstreeBorderPage* border = reinterpret_cast<MasstreeBorderPage*>(cur_route()->page_);
    PageVersion border_version = cur_route()->stable_;
    CHECK_ERROR_CODE(follow_foster(slice, &border, &border_version));
    ASSERT_ND(search_type_ == kBackwardExclusive || border->within_fences(slice));
    ASSERT_ND(search_type_ != kBackwardExclusive
      || border->within_fences(slice)
      || border->get_high_fence() == slice);

    Route* route = cur_route();
    ASSERT_ND(reinterpret_cast<MasstreeBorderPage*>(route->page_) == border);

    ASSERT_ND(!route->stable_.has_foster_child());
    uint8_t layer = border->get_layer();
    // find right record. be aware of backward-exclusive case!
    uint8_t index;

    // no need for fast-path for supremum.
    // almost always supremum-search is for backward search, so anyway it finds it first.
    // if we have supremum-search for forward search, we miss opportunity, but who does it...
    if (search_type_ == kForwardExclusive || search_type_ == kForwardInclusive) {
      for (index = 0; index < route->key_count_; ++index) {
        uint8_t record = route->get_original_index(index);
        fetch_cur_record(border, record);
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
            storage_,
            cur_key_observed_owner_id_,
            cur_key_owner_id_address));
        }
        break;
      }
    } else {
      for (index = route->key_count_ - 1; index < route->key_count_; --index) {
        uint8_t record = route->get_original_index(index);
        fetch_cur_record(border, record);
        KeyCompareResult result = compare_cur_key_aginst_search_key(slice, layer);
        if (result == kCurKeyLarger ||
          (result == kCurKeyEquals && search_type_ == kBackwardExclusive)) {
          continue;
        }

        if (!route->snapshot_ &&
          cur_key_in_layer_remaining_ != MasstreeBorderPage::kKeyLengthNextLayer) {
          CHECK_ERROR_CODE(current_xct_->add_to_read_set(
            storage_,
            cur_key_observed_owner_id_,
            cur_key_owner_id_address));
        }
        break;
      }
    }
    route->index_ = index;
    route->done_upto_ = cur_key_in_layer_slice_;
    route->done_upto_length_ = cur_key_in_layer_remaining_;

    assorted::memory_fence_consume();

    PageVersion version = border->get_stable_version();
    if (route->stable_.data_ != version.data_) {
      route->stable_ = version;
      continue;
    } else {
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
  MasstreeBorderPage* border = reinterpret_cast<MasstreeBorderPage*>(cur_route()->page_);
  DualPagePointer* pointer = border->get_next_layer(route->get_cur_original_index());
  MasstreePage* next;
  CHECK_ERROR_CODE(storage_pimpl_->follow_page(context_, for_writes_, pointer, &next));
  PageVersion next_stable;
  CHECK_ERROR_CODE(push_route(next, &next_stable));
  return locate_layer(border->get_layer() + 1U);
}


ErrorCode MasstreeCursor::locate_descend(KeySlice slice) {
  MasstreeIntermediatePage* cur = reinterpret_cast<MasstreeIntermediatePage*>(cur_route()->page_);
  PageVersion cur_stable = cur_route()->stable_;
  while (true) {
    CHECK_ERROR_CODE(follow_foster(slice, &cur, &cur_stable));

    Route* route = cur_route();
    ASSERT_ND(reinterpret_cast<MasstreeIntermediatePage*>(route->page_) == cur);

    ASSERT_ND(!cur_stable.has_foster_child());
    // find right minipage. be aware of backward-exclusive case!
    uint8_t index = 0;
    // fast path for supremum-search.
    if (is_search_key_extremum()) {
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
    route->stable_mini_ = minipage.get_stable_version();
    assorted::memory_fence_consume();
    while (true) {
      route->key_count_mini_ = route->stable_mini_.get_key_count();

      uint8_t index_mini = 0;
      if (is_search_key_extremum()) {
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
      /*  TODO(Hideaki) do this later
      if (search_type_ == kForwardExclusive || search_type_ == kForwardInclusive) {
        if (index_mini == 0) {
          done_upto_ =
        } else {
          done_upto_ =
        }
      } else {
      }
      */

      assorted::memory_fence_consume();
      PageVersion version_mini = minipage.get_stable_version();
      if (route->stable_mini_.data_ != version_mini.data_) {
        route->stable_mini_ = version_mini;
        continue;
      } else {
        break;
      }
    }

    PageVersion version = cur->get_stable_version();
    if (route->stable_.data_ != version.data_) {
      route->stable_ = version;
      continue;
    }

    DualPagePointer& pointer = minipage.pointers_[route->index_mini_];
    ASSERT_ND(!pointer.is_both_null());
    MasstreePage* next;
    CHECK_ERROR_CODE(storage_pimpl_->follow_page(context_, for_writes_, &pointer, &next));
    PageVersion next_stable;
    CHECK_ERROR_CODE(push_route(next, &next_stable));
    if (next->is_border()) {
      return kErrorCodeOk;
    } else {
      cur = reinterpret_cast<MasstreeIntermediatePage*>(next);
      cur_stable = next_stable;
    }
  }
}


ErrorCode MasstreeCursor::overwrite_record(
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  assert_modify();
  return storage_pimpl_->overwrite_general(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_,
    cur_key_length_,
    payload,
    payload_offset,
    payload_count);
}

template <typename PAYLOAD>
ErrorCode MasstreeCursor::overwrite_record_primitive(PAYLOAD payload, uint16_t payload_offset) {
  assert_modify();
  return storage_pimpl_->overwrite_general(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_,
    cur_key_length_,
    &payload,
    payload_offset,
    sizeof(PAYLOAD));
}

ErrorCode MasstreeCursor::delete_record() {
  assert_modify();
  return storage_pimpl_->delete_general(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_,
    cur_key_length_);
}

template <typename PAYLOAD>
ErrorCode MasstreeCursor::increment_record(PAYLOAD* value, uint16_t payload_offset) {
  assert_modify();
  return storage_pimpl_->increment_general<PAYLOAD>(
    context_,
    reinterpret_cast<MasstreeBorderPage*>(get_cur_page()),
    get_cur_index(),
    cur_key_,
    cur_key_length_,
    value,
    payload_offset);
}
}  // namespace masstree
}  // namespace storage
}  // namespace foedus

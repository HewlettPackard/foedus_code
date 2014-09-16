/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"

#include <glog/logging.h>

#include "foedus/assert_nd.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/storage/page_prefetch.hpp"

namespace foedus {
namespace storage {
namespace masstree {

ErrorCode MasstreeStoragePimpl::prefetch_pages_normalized(
  thread::Thread* context,
  KeySlice from,
  KeySlice to) {
  debugging::StopWatch watch;
  VLOG(0) << "Thread-" << context->get_thread_id()
    << " prefetching " << get_name() << " from=" << from << ", to=" << to;

  ASSERT_ND(first_root_pointer_.volatile_pointer_.components.offset);
  VolatilePagePointer pointer = control_block_->root_page_pointer_.volatile_pointer_;
  MasstreePage* root_page = reinterpret_cast<MasstreePage*>(
    context->get_global_volatile_page_resolver().resolve_offset(pointer));
  prefetch_page_l2(root_page);
  CHECK_ERROR_CODE(prefetch_pages_normalized_recurse(context, from, to, root_page));

  watch.stop();
  VLOG(0) << "Thread-" << context->get_thread_id()
    << " prefetched " << get_name() << " in " << watch.elapsed_us() << "us";
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::prefetch_pages_normalized_recurse(
  thread::Thread* context,
  KeySlice from,
  KeySlice to,
  MasstreePage* p) {
  if (p->has_foster_child()) {
    CHECK_ERROR_CODE(prefetch_pages_normalized_recurse(context, from, to, p->get_foster_minor()));
    CHECK_ERROR_CODE(prefetch_pages_normalized_recurse(context, from, to, p->get_foster_major()));
    return kErrorCodeOk;
  }

  const memory::GlobalVolatilePageResolver& resolver = context->get_global_volatile_page_resolver();
  uint8_t count = p->get_key_count();
  if (p->is_border()) {
    MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(p);
    for (uint8_t i = 0; i < count; ++i) {
      if (page->does_point_to_layer(i) && page->get_slice(i) >= from && page->get_slice(i) <= to) {
        VolatilePagePointer pointer = page->get_next_layer(i)->volatile_pointer_;
        MasstreePage* next = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(pointer));
        prefetch_page_l2(next);
        CHECK_ERROR_CODE(prefetch_pages_exhaustive(context, next));
      }
    }
  } else {
    MasstreeIntermediatePage* page = reinterpret_cast<MasstreeIntermediatePage*>(p);
    for (uint8_t i = 0; i <= count; ++i) {
      if (i < count && page->get_separator(i) >= from) {
        continue;
      }
      if (i > 0 && page->get_separator(i - 1) > to) {
        break;
      }
      MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(i);
      for (uint8_t j = 0; j <= minipage.key_count_; ++j) {
        if (j < minipage.key_count_ && minipage.separators_[j] >= from) {
          continue;
        }
        if (j > 0 && minipage.separators_[j - 1] > to) {
          break;
        }
        VolatilePagePointer pointer = minipage.pointers_[j].volatile_pointer_;
        MasstreePage* next = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(pointer));
        prefetch_page_l2(next);
        CHECK_ERROR_CODE(prefetch_pages_normalized_recurse(context, from, to, next));
      }
    }
  }
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::prefetch_pages_exhaustive(
  thread::Thread* context,
  MasstreePage* p) {
  if (p->has_foster_child()) {
    CHECK_ERROR_CODE(prefetch_pages_exhaustive(context, p->get_foster_minor()));
    CHECK_ERROR_CODE(prefetch_pages_exhaustive(context, p->get_foster_major()));
    return kErrorCodeOk;
  }

  const memory::GlobalVolatilePageResolver& resolver = context->get_global_volatile_page_resolver();
  uint8_t count = p->get_key_count();
  if (p->is_border()) {
    MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(p);
    for (uint8_t i = 0; i < count; ++i) {
      if (page->does_point_to_layer(i)) {
        VolatilePagePointer pointer = page->get_next_layer(i)->volatile_pointer_;
        MasstreePage* next = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(pointer));
        prefetch_page_l2(next);
        CHECK_ERROR_CODE(prefetch_pages_exhaustive(context, next));
      }
    }
  } else {
    MasstreeIntermediatePage* page = reinterpret_cast<MasstreeIntermediatePage*>(p);
    for (uint8_t i = 0; i <= count; ++i) {
      MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(i);
      for (uint8_t j = 0; j <= minipage.key_count_; ++j) {
        VolatilePagePointer pointer = minipage.pointers_[j].volatile_pointer_;
        MasstreePage* next = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(pointer));
        prefetch_page_l2(next);
        CHECK_ERROR_CODE(prefetch_pages_exhaustive(context, next));
      }
    }
  }
  return kErrorCodeOk;
}
}  // namespace masstree
}  // namespace storage
}  // namespace foedus

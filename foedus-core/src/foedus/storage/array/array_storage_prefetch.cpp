/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_storage_pimpl.hpp"

#include <glog/logging.h>

#include "foedus/assert_nd.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/storage/page_prefetch.hpp"
#include "foedus/storage/array/array_page_impl.hpp"
#include "foedus/thread/thread.hpp"

namespace foedus {
namespace storage {
namespace array {
ErrorCode ArrayStoragePimpl::prefetch_pages(
  thread::Thread* context,
  bool vol_on,
  bool snp_on,
  ArrayOffset from,
  ArrayOffset to) {
  debugging::StopWatch watch;
  VLOG(0) << "Thread-" << context->get_thread_id()
    << " prefetching " << get_meta().name_ << " from=" << from << ", to=" << to;
  ArrayPage* root_page = get_root_page();
  prefetch_page_l2(root_page);
  if (!root_page->is_leaf()) {
    CHECK_ERROR_CODE(prefetch_pages_recurse(context, vol_on, snp_on, from, to, root_page));
  }

  watch.stop();
  VLOG(0) << "Thread-" << context->get_thread_id()
    << " prefetched " << get_meta().name_ << " in " << watch.elapsed_us() << "us";
  return kErrorCodeOk;
}

ErrorCode ArrayStoragePimpl::prefetch_pages_recurse(
  thread::Thread* context,
  bool vol_on,
  bool snp_on,
  ArrayOffset from,
  ArrayOffset to,
  ArrayPage* page) {
  uint8_t level = page->get_level();
  ASSERT_ND(level > 0);
  ArrayOffset interval = control_block_->route_finder_.get_records_in_leaf();
  for (uint8_t i = 1; i < level; ++i) {
    interval *= kInteriorFanout;
  }
  ArrayRange page_range = page->get_array_range();
  ArrayRange range(from, to);
  ASSERT_ND(page_range.overlaps(range));  // otherwise why we came here...
  ASSERT_ND(page_range.begin_ + (interval * kInteriorFanout) >= page_range.end_);  // probably==
  for (uint16_t i = 0; i < kInteriorFanout; ++i) {
    ArrayRange child_range(
      page_range.begin_ + i * interval,
      page_range.begin_ + (i + 1U) * interval);
    if (!range.overlaps(child_range)) {
      continue;
    }

    DualPagePointer& pointer = page->get_interior_record(i);

    // first, do we have to cache snapshot page?
    if (pointer.snapshot_pointer_ != 0) {
      if (snp_on) {
        ArrayPage* child;
        CHECK_ERROR_CODE(context->find_or_read_a_snapshot_page(
          pointer.snapshot_pointer_,
          reinterpret_cast<Page**>(&child)));
        ASSERT_ND(child->get_array_range().begin_ == page_range.begin_ + i * interval);
        ASSERT_ND(range.overlaps(child->get_array_range()));
        prefetch_page_l2(child);
        if (level > 1U) {
          CHECK_ERROR_CODE(prefetch_pages_recurse(context, false, snp_on, from, to, child));
        }
      }
      // do we have to install volatile page based on it?
      if (pointer.volatile_pointer_.is_null() && vol_on) {
        ASSERT_ND(!page->header().snapshot_);
        ArrayPage* child;
        CHECK_ERROR_CODE(context->install_a_volatile_page(
          &pointer,
          reinterpret_cast<Page**>(&child)));
        ASSERT_ND(child->get_array_range().begin_ == page_range.begin_ + i * interval);
        ASSERT_ND(range.overlaps(child->get_array_range()));
      }
    }

    // then go down
    if (vol_on) {
      if (pointer.volatile_pointer_.is_null() && pointer.snapshot_pointer_ == 0) {
        // the page is not populated yet. we create an empty page in this case.
        ArrayPage* dummy;
        CHECK_ERROR_CODE(follow_pointer(context, false, true, &pointer, &dummy, page, i));
      }

      if (!pointer.volatile_pointer_.is_null()) {
        ASSERT_ND(!page->header().snapshot_);
        ArrayPage* child = context->resolve_cast<ArrayPage>(pointer.volatile_pointer_);
          ASSERT_ND(child->get_array_range().begin_ == page_range.begin_ + i * interval);
          ASSERT_ND(range.overlaps(child->get_array_range()));
        prefetch_page_l2(child);
        if (level > 1U) {
          CHECK_ERROR_CODE(prefetch_pages_recurse(context, vol_on, snp_on, from, to, child));
        }
      }
    }
  }
  return kErrorCodeOk;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus

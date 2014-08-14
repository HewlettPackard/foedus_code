/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/storage/array/array_storage_pimpl.hpp"

#include <glog/logging.h>

#include <vector>

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
  ArrayOffset from,
  ArrayOffset to) {
  debugging::StopWatch watch;
  VLOG(0) << "Thread-" << context->get_thread_id()
    << " prefetching " << metadata_.name_ << " from=" << from << ", to=" << to;
  prefetch_page_l2(root_page_);
  if (!root_page_->is_leaf()) {
    CHECK_ERROR_CODE(prefetch_pages_recurse(context, from, to, root_page_));
  }

  watch.stop();
  VLOG(0) << "Thread-" << context->get_thread_id()
    << " prefetched " << metadata_.name_ << " in " << watch.elapsed_us() << "us";
  return kErrorCodeOk;
}

ErrorCode ArrayStoragePimpl::prefetch_pages_recurse(
  thread::Thread* context,
  ArrayOffset from,
  ArrayOffset to,
  ArrayPage* page) {
  uint8_t level = page->get_level();
  ASSERT_ND(level > 0);
  const memory::GlobalVolatilePageResolver& resolver = context->get_global_volatile_page_resolver();
  for (uint16_t i = 0; i < kInteriorFanout; ++i) {
    VolatilePagePointer pointer = page->get_interior_record(i).volatile_pointer_;
    if (pointer.components.offset != 0) {
      ArrayPage* child = reinterpret_cast<ArrayPage*>(resolver.resolve_offset(pointer));
      if (child->get_array_range().end_ <= from) {
        continue;
      }
      if (child->get_array_range().begin_ >= to) {
        break;
      }
      prefetch_page_l2(child);
      if (level > 1U) {
        CHECK_ERROR_CODE(prefetch_pages_recurse(context, from, to, child));
      }
    }
  }
  return kErrorCodeOk;
}

}  // namespace array
}  // namespace storage
}  // namespace foedus

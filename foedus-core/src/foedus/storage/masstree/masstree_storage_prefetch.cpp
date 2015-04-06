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

#include "foedus/assert_nd.hpp"
#include "foedus/debugging/stop_watch.hpp"
#include "foedus/storage/page_prefetch.hpp"

namespace foedus {
namespace storage {
namespace masstree {

ErrorCode MasstreeStoragePimpl::prefetch_pages_normalized(
  thread::Thread* context,
  bool vol_on,
  bool snp_on,
  KeySlice from,
  KeySlice to) {
  debugging::StopWatch watch;
  VLOG(0) << "Thread-" << context->get_thread_id()
    << " prefetching " << get_name() << " from=" << from << ", to=" << to;

  MasstreeIntermediatePage* root_page;
  CHECK_ERROR_CODE(get_first_root(context, false, &root_page));
  prefetch_page_l2(root_page);
  CHECK_ERROR_CODE(prefetch_pages_normalized_recurse(context, vol_on, snp_on, from, to, root_page));

  watch.stop();
  VLOG(0) << "Thread-" << context->get_thread_id()
    << " prefetched " << get_name() << " in " << watch.elapsed_us() << "us";
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::prefetch_pages_normalized_recurse(
  thread::Thread* context,
  bool vol_on,
  bool snp_on,
  KeySlice from,
  KeySlice to,
  MasstreePage* p) {
  if (p->has_foster_child()) {
    MasstreePage* minor = context->resolve_cast<MasstreePage>(p->get_foster_minor());
    MasstreePage* major = context->resolve_cast<MasstreePage>(p->get_foster_major());
    CHECK_ERROR_CODE(prefetch_pages_normalized_recurse(context, vol_on, snp_on, from, to, minor));
    CHECK_ERROR_CODE(prefetch_pages_normalized_recurse(context, vol_on, snp_on, from, to, major));
    return kErrorCodeOk;
  }

  uint8_t count = p->get_key_count();
  if (p->is_border()) {
    MasstreeBorderPage* page = reinterpret_cast<MasstreeBorderPage*>(p);
    for (uint8_t i = 0; i < count; ++i) {
      if (page->does_point_to_layer(i) && page->get_slice(i) >= from && page->get_slice(i) <= to) {
        DualPagePointer* pointer = page->get_next_layer(i);
        // next layer. exhaustively read
        CHECK_ERROR_CODE(prefetch_pages_follow(
          context,
          pointer,
          vol_on,
          snp_on,
          kInfimumSlice,
          kSupremumSlice));
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
        DualPagePointer* pointer = &minipage.pointers_[j];
        // next layer. exhaustively read
        CHECK_ERROR_CODE(prefetch_pages_follow(context, pointer, vol_on, snp_on, from, to));
      }
    }
  }
  return kErrorCodeOk;
}

ErrorCode MasstreeStoragePimpl::prefetch_pages_follow(
  thread::Thread* context,
  DualPagePointer* pointer,
  bool vol_on,
  bool snp_on,
  KeySlice from,
  KeySlice to) {
  // first, do we have to cache snapshot page?
  if (pointer->snapshot_pointer_ != 0) {
    if (snp_on) {
      MasstreePage* child;
      CHECK_ERROR_CODE(context->find_or_read_a_snapshot_page(
        pointer->snapshot_pointer_,
        reinterpret_cast<Page**>(&child)));
      prefetch_page_l2(child);
      CHECK_ERROR_CODE(prefetch_pages_normalized_recurse(
        context,
        false,
        snp_on,
        from,
        to,
        child));
    }
    // do we have to install volatile page based on it?
    if (pointer->volatile_pointer_.is_null() && vol_on) {
      ASSERT_ND(!to_page(pointer)->get_header().snapshot_);
      Page* child;
      CHECK_ERROR_CODE(context->install_a_volatile_page(pointer, &child));
    }
  }

  // then go down
  if (!pointer->volatile_pointer_.is_null() && vol_on) {
    ASSERT_ND(!to_page(pointer)->get_header().snapshot_);
    MasstreePage* child = context->resolve_cast<MasstreePage>(pointer->volatile_pointer_);
    prefetch_page_l2(child);
    CHECK_ERROR_CODE(prefetch_pages_normalized_recurse(
      context,
      vol_on,
      snp_on,
      from,
      to,
      child));
  }
  return kErrorCodeOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

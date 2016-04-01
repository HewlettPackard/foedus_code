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

#include "foedus/memory/engine_memory.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"

namespace foedus {
namespace storage {
namespace masstree {

ErrorCode MasstreeStorage::peek_volatile_page_boundaries(
  Engine* engine, const MasstreeStorage::PeekBoundariesArguments& args) {
  MasstreeStoragePimpl pimpl(this);
  return pimpl.peek_volatile_page_boundaries(engine, args);
}

ErrorCode MasstreeStoragePimpl::peek_volatile_page_boundaries(
  Engine* engine,
  const MasstreeStorage::PeekBoundariesArguments& args) {
  *args.found_boundary_count_ = 0;

  VolatilePagePointer root_pointer = get_first_root_pointer().volatile_pointer_;
  if (root_pointer.is_null()) {
    VLOG(0) << "This masstree has no volatile pages. Maybe the composer is executed as part of"
      << " restart?";
    return kErrorCodeOk;
  }

  const memory::GlobalVolatilePageResolver& resolver
    = engine->get_memory_manager()->get_global_volatile_page_resolver();
  const MasstreePage* root
    = reinterpret_cast<const MasstreePage*>(resolver.resolve_offset(root_pointer));

  if (args.prefix_slice_count_ == 0) {
    return peek_volatile_page_boundaries_this_layer(root, resolver, args);
  } else {
    return peek_volatile_page_boundaries_next_layer(root, resolver, args);
  }
}

ErrorCode MasstreeStoragePimpl::peek_volatile_page_boundaries_next_layer(
  const MasstreePage* layer_root,
  const memory::GlobalVolatilePageResolver& resolver,
  const MasstreeStorage::PeekBoundariesArguments& args) {
  ASSERT_ND(!layer_root->header().snapshot_);
  uint8_t this_layer = layer_root->get_layer();
  ASSERT_ND(this_layer < args.prefix_slice_count_);
  ASSERT_ND(layer_root->get_low_fence() == kInfimumSlice && layer_root->is_high_fence_supremum());
  KeySlice slice = args.prefix_slices_[layer_root->get_layer()];

  // look for this slice in this layer. If we can't find it (which is unlikely), no results.
  // compared to tree-traversal code in the transactional methods, we can give up whenever
  // something rare happens.
  const MasstreePage* cur = layer_root;
  cur->prefetch_general();
  while (!cur->is_border()) {
    ASSERT_ND(cur->within_fences(slice));
    // We do NOT follow foster-twins here.
    // Foster-twins are sooner or later adopted, and Master-Tree invariant for intermediate page
    // guarantees that we can just read the old page. no need to follow foster-twins.
    const MasstreeIntermediatePage* page = reinterpret_cast<const MasstreeIntermediatePage*>(cur);
    uint8_t minipage_index = page->find_minipage(slice);
    const MasstreeIntermediatePage::MiniPage& minipage = page->get_minipage(minipage_index);

    minipage.prefetch();
    uint8_t pointer_index = minipage.find_pointer(slice);
    VolatilePagePointer pointer = minipage.pointers_[pointer_index].volatile_pointer_;
    const MasstreePage* next
      = reinterpret_cast<const MasstreePage*>(resolver.resolve_offset(pointer));
    next->prefetch_general();
    if (LIKELY(next->within_fences(slice))) {
      cur = next;
    } else {
      // even in this case, local retry suffices thanks to foster-twin
      VLOG(0) << "Interesting. concurrent thread affected the search. local retry";
      assorted::memory_fence_acquire();
    }
  }
  const MasstreeBorderPage* border = reinterpret_cast<const MasstreeBorderPage*>(cur);

  // following code are not transactional at all, but it's fine because these are just hints.
  // however, make sure we don't hit something like segfault. check nulls etc.
  ASSERT_ND(border->within_fences(slice));
  uint8_t key_count = border->get_key_count();
  SlotIndex rec = kBorderPageMaxSlots;
  border->prefetch_additional_if_needed(key_count);
  for (SlotIndex i = 0; i < key_count; ++i) {
    KeySlice cur_slice = border->get_slice(i);
    if (LIKELY(cur_slice < slice)) {
      continue;
    }
    if (cur_slice > slice) {
      break;
    }
    // one slice might be used for up to 10 keys, length 0 to 8 and pointer to next layer.
    if (border->does_point_to_layer(i)) {
      rec = i;
      break;
    }
  }

  if (UNLIKELY(rec >= key_count || !border->does_point_to_layer(rec))) {
    LOG(INFO) << "Slice not found during peeking. Gives up finding a page boundary hint";
    return kErrorCodeOk;
  }

  VolatilePagePointer pointer = border->get_next_layer(rec)->volatile_pointer_;
  if (UNLIKELY(pointer.is_null()
    || pointer.get_numa_node() >= resolver.numa_node_count_
    || pointer.get_offset() < resolver.begin_
    || pointer.get_offset() >= resolver.end_)) {
    LOG(INFO) << "Encountered invalid page during peeking. Gives up finding a page boundary hint";
    return kErrorCodeOk;
  }
  const MasstreePage* next_root
    = reinterpret_cast<const MasstreePage*>(resolver.resolve_offset(pointer));
  if (this_layer + 1U == args.prefix_slice_count_) {
    return peek_volatile_page_boundaries_this_layer(next_root, resolver, args);
  } else {
    return peek_volatile_page_boundaries_next_layer(next_root, resolver, args);
  }
}

ErrorCode MasstreeStoragePimpl::peek_volatile_page_boundaries_this_layer(
  const MasstreePage* layer_root,
  const memory::GlobalVolatilePageResolver& resolver,
  const MasstreeStorage::PeekBoundariesArguments& args) {
  ASSERT_ND(!layer_root->header().snapshot_);
  ASSERT_ND(layer_root->get_layer() == args.prefix_slice_count_);
  ASSERT_ND(layer_root->get_low_fence() == kInfimumSlice && layer_root->is_high_fence_supremum());
  if (layer_root->is_border()) {
    ASSERT_ND(layer_root->get_layer() > 0);  // first layer's root page is always intermediate
    // the root page of the layer of interest is a border page, hence no boundaries.
    return kErrorCodeOk;
  }
  const MasstreeIntermediatePage* cur
    = reinterpret_cast<const MasstreeIntermediatePage*>(layer_root);
  return peek_volatile_page_boundaries_this_layer_recurse(cur, resolver, args);
}

ErrorCode MasstreeStoragePimpl::peek_volatile_page_boundaries_this_layer_recurse(
  const MasstreeIntermediatePage* cur,
  const memory::GlobalVolatilePageResolver& resolver,
  const MasstreeStorage::PeekBoundariesArguments& args) {
  ASSERT_ND(!cur->header().snapshot_);
  ASSERT_ND(!cur->is_border());
  ASSERT_ND(cur->get_layer() == args.prefix_slice_count_);
  // because of concurrent modifications, this is possible. we just give up finding hints.
  if (UNLIKELY(cur->get_low_fence() >= args.to_ || cur->get_high_fence() <= args.from_)) {
    return kErrorCodeOk;
  }

  uint8_t begin_index = 0;
  uint8_t begin_mini_index = 0;
  if (cur->within_fences(args.from_)) {
    begin_index = cur->find_minipage(args.from_);
    const MasstreeIntermediatePage::MiniPage& minipage = cur->get_minipage(begin_index);
    minipage.prefetch();
    begin_mini_index = minipage.find_pointer(args.from_);
  }

  // if children of this page are intermediate pages (this level>=2), we further recurse.
  // if children of this page are border pages, just append the page boundaries stored in this page.
  const bool needs_recurse = (cur->get_btree_level() >= 2U);
  // we don't care wherther the child has foster twins. it's rare, and most keys are already
  // pushed up to this page. again, this method is opportunistic.
  // this guarantees that the cost of peeking is cheap. we just read intermediate pages.
  // again, this code is not protected from concurrent transactions at all.
  // we just make sure it doesn't hit segfault etc.
  MasstreeIntermediatePointerIterator it(cur);
  it.index_ = begin_index;
  it.index_mini_ = begin_mini_index;
  for (; it.is_valid(); it.next()) {
    VolatilePagePointer pointer = it.get_pointer().volatile_pointer_;
    KeySlice boundary = it.get_low_key();
    if (boundary >= args.to_) {  // all pages under the pointer are >= boundary.
      break;
    }
    if (boundary > args.from_) {
      // at least guarantee that the resulting found_boundaries_ is ordered.
      if ((*args.found_boundary_count_) == 0
        || args.found_boundaries_[(*args.found_boundary_count_) - 1U] < boundary) {
        ASSERT_ND((*args.found_boundary_count_) < args.found_boundary_capacity_);
        args.found_boundaries_[*args.found_boundary_count_] = boundary;
        ++(*args.found_boundary_count_);
        if ((*args.found_boundary_count_) >= args.found_boundary_capacity_) {
          VLOG(0) << "Found too many boundaries while peeking. stops here.";
          return kErrorCodeOk;
        }
      }
    }
    // recurse to find more. note that it might contain pages of interest
    // even if boundary < args.from_ because it's a *low*-fence.
    if (needs_recurse && !pointer.is_null()) {
      if (LIKELY(pointer.get_numa_node() < resolver.numa_node_count_
        && pointer.get_offset() >= resolver.begin_
        && pointer.get_offset() < resolver.end_)) {
        const MasstreeIntermediatePage* next
          = reinterpret_cast<const MasstreeIntermediatePage*>(resolver.resolve_offset(pointer));
        if (next->is_border()) {
          // this is possible because our masstree might be unbalanced.
          ASSERT_ND(cur->get_layer() == 0);  // but it can happen only at first layer's root
          ASSERT_ND(cur->get_low_fence() == kInfimumSlice && cur->is_high_fence_supremum());
          continue;
        }
        ASSERT_ND(next->header().get_page_type() == kMasstreeIntermediatePageType);
        CHECK_ERROR_CODE(peek_volatile_page_boundaries_this_layer_recurse(next, resolver, args));
        ASSERT_ND((*args.found_boundary_count_) <= args.found_boundary_capacity_);
        if ((*args.found_boundary_count_) >= args.found_boundary_capacity_) {
          return kErrorCodeOk;
        }
      }
    }
  }

  return kErrorCodeOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

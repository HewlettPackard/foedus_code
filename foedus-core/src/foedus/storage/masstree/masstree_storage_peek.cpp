/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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
    if (UNLIKELY(cur->has_foster_child())) {
      // follow one of foster-twin.
      if (cur->within_foster_minor(slice)) {
        cur = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(cur->get_foster_minor()));
      } else {
        cur = reinterpret_cast<MasstreePage*>(resolver.resolve_offset(cur->get_foster_major()));
      }
      ASSERT_ND(cur->within_fences(slice));
      continue;
    }

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
  uint8_t rec = MasstreeBorderPage::kMaxKeys;
  border->prefetch_additional_if_needed(key_count);
  for (uint8_t i = 0; i < key_count; ++i) {
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
    || pointer.components.numa_node >= resolver.numa_node_count_
    || pointer.components.offset < resolver.begin_
    || pointer.components.offset >= resolver.end_)) {
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

  // if children of this page are intermediate pages, we further recurse.
  // if children of this page are border pages, just append the page boundaries stored in this page.
  // we don't care wherther the child has foster twins. it's rare, and most keys are already
  // pushed up to this page. again, this method is opportunistic.
  // this guarantees that the cost of peeking is cheap. we just read intermediate pages.
  bool needs_recurse = true;  // determined when we check some child.
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
        args.found_boundaries_[*args.found_boundary_count_] = boundary;
        ++(*args.found_boundary_count_);
        if ((*args.found_boundary_count_) >= args.found_boundary_capacity_) {
          VLOG(0) << "Found too many boundaries while peeking. stops here.";
          break;
        }
      }
    }
    // recurse to find more. note that it might contain pages of interest
    // even if boundary < args.from_ because it's a *low*-fence.
    if (needs_recurse && !pointer.is_null()) {
      if (LIKELY(pointer.components.numa_node < resolver.numa_node_count_
        && pointer.components.offset >= resolver.begin_
        && pointer.components.offset < resolver.end_)) {
        const MasstreePage* next
          = reinterpret_cast<const MasstreePage*>(resolver.resolve_offset(pointer));
        if (next->header().get_page_type() != kMasstreeIntermediatePageType) {
          //
          needs_recurse = false;
        } else {
          CHECK_ERROR_CODE(peek_volatile_page_boundaries_this_layer_recurse(
            reinterpret_cast<const MasstreeIntermediatePage*>(next), resolver, args));
        }
      }
    }
  }

  return kErrorCodeOk;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus

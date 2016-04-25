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
#include "foedus/storage/masstree/masstree_adopt_impl.hpp"
#include "foedus/storage/masstree/masstree_cursor.hpp"
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_split_impl.hpp"

namespace foedus {
namespace storage {
namespace masstree {

uint32_t count_children_approximate(const MasstreeIntermediatePage* page) {
  // this method doesn't need page-lock, but instead it might be inaccurate
  const uint16_t key_count = page->get_key_count();
  uint32_t current_count = 0;
  for (uint16_t minipage_index = 0; minipage_index <= key_count; ++minipage_index) {
    const auto& minipage = page->get_minipage(minipage_index);
    current_count += minipage.key_count_ + 1;
  }
  return current_count;
}
ErrorCode MasstreeStoragePimpl::approximate_count_root_children(
  thread::Thread* context,
  uint32_t* out) {
  *out = 0;
  MasstreeIntermediatePage* root;
  CHECK_ERROR_CODE(get_first_root(context, true, &root));
  *out = count_children_approximate(root);
  return kErrorCodeOk;
}

constexpr uint32_t kIntermediateAlmostFull = kMaxIntermediatePointers * 9U / 10U;

ErrorStack MasstreeStoragePimpl::fatify_first_root(
  thread::Thread* context,
  uint32_t desired_count) {
  LOG(INFO) << "Masstree-" << get_name() << " being fatified for " << desired_count;

  if (desired_count > kIntermediateAlmostFull) {
    LOG(INFO) << "desired_count too large. adjusted to the max";
    desired_count = kIntermediateAlmostFull;
  }
  uint32_t initial_children;
  WRAP_ERROR_CODE(approximate_count_root_children(context, &initial_children));
  LOG(INFO) << "initial_children=" << initial_children;

  // We keep doubling the direct root-children
  debugging::StopWatch watch;
  watch.start();
  uint16_t iterations = 0;
  for (uint32_t count = initial_children; count < desired_count && iterations < 10U; ++iterations) {
    CHECK_ERROR(fatify_first_root_double(context));
    uint32_t new_count;
    WRAP_ERROR_CODE(approximate_count_root_children(context, &new_count));
    if (count == new_count) {
      LOG(WARNING) << "Not enough descendants for further fatification. Stopped here";
      break;
    }
    count = new_count;
  }

  uint32_t after_children;
  WRAP_ERROR_CODE(approximate_count_root_children(context, &after_children));
  watch.stop();
  LOG(INFO) << "fatify done: Iterations=" << iterations
    << ", took " << watch.elapsed_us() << "us in total."
    << " child count: " << initial_children << "->" << after_children;

  return kRetOk;
}
// TODO
uint32_t aaaa2_recurse_general(thread::Thread* context, Page* page);
uint32_t aaaa2_recurse(thread::Thread* context, MasstreeBorderPage* page) {
  if (page->is_empty_range()) {
    return 0;
  }
  auto resolver = context->get_global_volatile_page_resolver();
  uint32_t ret = 0;
  if (page->is_moved()) {
    ret += aaaa2_recurse(context, reinterpret_cast<MasstreeBorderPage*>(resolver.resolve_offset(page->get_foster_minor())));
    ret += aaaa2_recurse(context, reinterpret_cast<MasstreeBorderPage*>(resolver.resolve_offset(page->get_foster_major())));
    return ret;
  }

  for (uint32_t i = 0; i < page->get_key_count(); ++i) {
    if (page->does_point_to_layer(i)) {
      auto* child = resolver.resolve_offset(page->get_next_layer(i)->volatile_pointer_);
      ret += aaaa2_recurse_general(context, child);
    } else {
      if (!page->get_slot(i)->tid_.is_deleted()) {
        ++ret;
      }
    }
  }
  return ret;
}
uint32_t aaaa2_recurse(thread::Thread* context, MasstreeIntermediatePage* page) {
  auto resolver = context->get_global_volatile_page_resolver();
  uint32_t ret = 0;
  for (MasstreeIntermediatePointerIterator it(page); it.is_valid(); it.next()) {
    auto* child = resolver.resolve_offset(it.get_pointer().volatile_pointer_);
    ret += aaaa2_recurse_general(context, child);
  }
  return ret;
}
uint32_t aaaa2_recurse_general(thread::Thread* context, Page* page) {
  if (reinterpret_cast<MasstreePage*>(page)->is_empty_range()) {
    return 0;
  }
  if (page->get_header().page_type_ == kMasstreeBorderPageType) {
    return aaaa2_recurse(context, reinterpret_cast<MasstreeBorderPage*>(page));
  } else {
    return aaaa2_recurse(context, reinterpret_cast<MasstreeIntermediatePage*>(page));
  }
}
uint32_t aaaa2(thread::Thread* context, MasstreeStoragePimpl* t) {
  MasstreeIntermediatePage* root;
  t->get_first_root(context, true, &root);
  return aaaa2_recurse(context, root);
}
void aaaa(thread::Thread* context, MasstreeStoragePimpl* t) {
  MasstreeStorage s(context->get_engine(), t->get_control_block());
  MasstreeCursor c(s, context);
  c.open();
  int a = 0;
  while (c.is_valid_record()) {
    ++a;
    c.next();
  }
  LOG(INFO) << "============================================================ rec =" << a;
  LOG(INFO) << "============================================================ rec_correct ="
    << aaaa2(context, t);
  ASSERT_ND(a == 12000U);
}

ErrorStack MasstreeStoragePimpl::fatify_first_root_double(thread::Thread* context) {
  // We invoke split sysxct and adopt sysxct many times.
  debugging::StopWatch watch;
  watch.start();
  uint16_t root_retries = 0;
  KeySlice cur_slice = kInfimumSlice;
  uint16_t skipped_children = 0;
  uint16_t adopted_children = 0;
  uint32_t initial_children;
  WRAP_ERROR_CODE(approximate_count_root_children(context, &initial_children));
  while (cur_slice != kSupremumSlice) {
    if (initial_children + adopted_children >= kIntermediateAlmostFull) {
      LOG(INFO) << "Root page nearing full. Stopped fatification";
      break;
    }

    // Get a non-moved root. This might trigger grow-root.
    // grow-root is a non-mandatory operation, so we might keep seeing a moved root.
    MasstreeIntermediatePage* root;
    WRAP_ERROR_CODE(get_first_root(context, true, &root));
    ASSERT_ND(root->get_low_fence() == kInfimumSlice);
    ASSERT_ND(root->get_high_fence() == kSupremumSlice);
    if (root->is_moved()) {
      ++root_retries;
      if (root_retries > 50U) {
        LOG(WARNING) << "Hm? there might be some contention to prevent grow-root. Gave up";
        break;
      }
      continue;
    }
// TODO
// aaaa(context, this);
    root_retries = 0;
    const auto minipage_index = root->find_minipage(cur_slice);
    auto& minipage = root->get_minipage(minipage_index);
    auto pointer_index = minipage.find_pointer(cur_slice);

    MasstreePage* child;
    WRAP_ERROR_CODE(follow_page(
      context,
      true,
      minipage.pointers_ + pointer_index,
      &child));
    ASSERT_ND(!child->header().snapshot_);
    cur_slice = child->get_high_fence();  // go on to next
/* TODO
if (cur_slice == 1379796796737ULL) {
LOG(INFO) << "asdasd"  ;
}
*/
    if (!child->is_moved()) {
      // Split the child so that we can adopt it to the root
      if (child->is_border()) {
        if (child->get_key_count() >= 2U) {
          MasstreeBorderPage* casted = reinterpret_cast<MasstreeBorderPage*>(child);
/* TODO
if (cur_slice == 1379796796737ULL) {
LOG(INFO) << "before " << *casted;
}
*/
          auto low_slice = child->get_low_fence();
          auto high_slice = child->get_high_fence();
          auto mid_slice = low_slice + (high_slice - low_slice) / 2U;
          // TODO if I don't specify disable-NRC, test_masstree_tpcc's Customer-Secondary
          // get 11998 records, not 12000. Something wrong, but looks like the split code
          // is fine. I guess cursor code is wrong? Needs to investigate
          SplitBorder split(context, casted, mid_slice, false);
          WRAP_ERROR_CODE(context->run_nested_sysxct(&split, 2U));
/* TODO
if (cur_slice == 1379796796737ULL) {
LOG(INFO) << "after " << *casted;
MasstreeBorderPage* minor = (MasstreeBorderPage*) context->get_global_volatile_page_resolver().resolve_offset(child->get_foster_minor());
MasstreeBorderPage* major = (MasstreeBorderPage*) context->get_global_volatile_page_resolver().resolve_offset(child->get_foster_major());
LOG(INFO) << "minor " << *minor;
LOG(INFO) << "major " << *major;
}
*/
        } else {
          ++skipped_children;
          continue;  // not worth splitting
        }
      } else {
        MasstreeIntermediatePage* casted = reinterpret_cast<MasstreeIntermediatePage*>(child);
        uint32_t grandchild_count = count_children_approximate(casted);
        if (grandchild_count >= 2U) {
          SplitIntermediate split(context, casted);
          WRAP_ERROR_CODE(context->run_nested_sysxct(&split, 2U));
        } else {
          ++skipped_children;
          continue;  // not worth splitting
        }
      }
    }
// TODO
// aaaa(context, this);
    Adopt adopt(context, root, child);
    WRAP_ERROR_CODE(context->run_nested_sysxct(&adopt, 2U));
    ++adopted_children;
// TODO
// aaaa(context, this);
  }

  uint32_t after_children;
  WRAP_ERROR_CODE(approximate_count_root_children(context, &after_children));
  watch.stop();
  LOG(INFO) << "fatify_double: adopted " << adopted_children << " root-children and skipped "
    << skipped_children << " that are already too sparse in " << watch.elapsed_us() << "us."
    << " child count: " << initial_children << "->" << after_children;

  return kRetOk;
}
}  // namespace masstree
}  // namespace storage
}  // namespace foedus

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
#include "foedus/storage/hash/hash_composed_bins_impl.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>

#include "foedus/cache/snapshot_file_set.hpp"
#include "foedus/snapshot/snapshot_writer_impl.hpp"

namespace foedus {
namespace storage {
namespace hash {


void ComposedBinsBuffer::init(
  cache::SnapshotFileSet* fileset,
  SnapshotPagePointer head_page_id,
  uint32_t total_pages,
  uint32_t buffer_size,
  HashComposedBinsPage* buffer) {
  fileset_ = fileset;
  head_page_id_ = head_page_id;
  total_pages_ = total_pages;
  buffer_size_ = buffer_size;
  buffer_pos_ = 0;
  buffer_count_ = 0;
  cursor_buffer_ = 0;
  cursor_bin_ = 0;
  cursor_bin_count_ = 0;
  buffer_ = buffer;
}

ErrorCode ComposedBinsBuffer::next_pages() {
  ASSERT_ND(buffer_pos_ + buffer_count_ <= total_pages_);
  if (buffer_pos_ + buffer_count_ < total_pages_) {
    uint32_t previous_end = buffer_pos_ + buffer_count_;
    uint32_t pages_to_read = std::min<uint32_t>(buffer_size_, total_pages_ - previous_end);
    SnapshotPagePointer read_from = head_page_id_ + previous_end;
    CHECK_ERROR_CODE(fileset_->read_pages(read_from, pages_to_read, buffer_));
    buffer_pos_ = previous_end;
    buffer_count_ = pages_to_read;
    cursor_buffer_ = 0;
    cursor_bin_ = 0;
    cursor_bin_count_ = buffer_[0].bin_count_;
    ASSERT_ND(cursor_bin_count_ > 0);
  } else {
    buffer_pos_ = total_pages_;
    buffer_count_ = 0;
    cursor_buffer_ = 0;
    cursor_bin_ = 0;
    cursor_bin_count_ = 0;
  }
  return kErrorCodeOk;
}

void ComposedBinsBuffer::assure_read_buffer_size(
  memory::AlignedMemory* read_buffer,
  uint32_t inputs) {
  // split read_buffer_ to each input
  read_buffer->assure_capacity(
    kPageSize * kMinBufferSize * inputs,
    2.0,
    false);
  uint32_t total_size = read_buffer->get_size() / kPageSize;
  uint32_t buffer_size = total_size / inputs;
  ASSERT_ND(buffer_size >= kMinBufferSize);
}

ErrorStack ComposedBinsMergedStream::init(
  const HashRootInfoPage* const* inputs,
  uint32_t                  input_count,
  PagePtr                   root_page,
  uint16_t                  root_child_index,
  memory::AlignedMemory*    read_buffer,
  cache::SnapshotFileSet*   fileset,
  snapshot::SnapshotWriter* writer,
  uint32_t*                 writer_buffer_pos,
  uint32_t*                 writer_higher_buffer_pos,
  bool*                     had_any_change) {
  ASSERT_ND(root_page->get_level() >= 1U);
  snapshot_id_ = writer->get_snapshot_id();
  std::memset(cur_path_, 0, sizeof(cur_path_));

  inputs_memory_.reset(new ComposedBinsBuffer[input_count]);
  inputs_ = inputs_memory_.get();
  input_count_ = input_count;

  ComposedBinsBuffer::assure_read_buffer_size(read_buffer, input_count);
  uint64_t buffer_piece_size = read_buffer->get_size() / kPageSize / input_count;
  for (uint32_t i = 0; i < input_count_; ++i) {
    const DualPagePointer& root_child = inputs[i]->get_pointer(root_child_index);
    SnapshotPagePointer head_page_id = root_child.snapshot_pointer_;
    uint32_t total_pages = root_child.volatile_pointer_.word;
    ASSERT_ND(total_pages > 0);
    HashComposedBinsPage* read_buffer_casted
      = reinterpret_cast<HashComposedBinsPage*>(read_buffer->get_block());
    HashComposedBinsPage* buffer_piece = read_buffer_casted + buffer_piece_size * i;
    inputs_[i].init(fileset, head_page_id, total_pages, buffer_piece_size, buffer_piece);
    if (inputs_[i].has_more()) {
      WRAP_ERROR_CODE(inputs_[i].next_pages());
    }
  }

  levels_ = root_page->get_level() + 1U;
  cur_path_[root_page->get_level()] = root_page;

  // what's the bin we initially seek to?
  HashBin initial_bin = kInvalidHashBin;
  for (uint32_t i = 0; i < input_count_; ++i) {
    ComposedBinsBuffer* input = inputs_ + i;
    if (input->has_more()) {
      initial_bin = std::min<HashBin>(initial_bin, input->get_cur_bin().bin_);
    }
  }

  if (initial_bin == kInvalidHashBin) {
    *had_any_change = false;
  } else {
    *had_any_change = true;
    WRAP_ERROR_CODE(open_path(
      initial_bin,
      root_page->get_level(),
      fileset,
      writer,
      writer_buffer_pos,
      writer_higher_buffer_pos));
  }

  return kRetOk;
}

ErrorCode ComposedBinsMergedStream::process_a_bin(
  uint32_t* installed_count,
  HashBin* next_lowest_bin) {
  HashIntermediatePage* const page = cur_path_[0];
  ASSERT_ND(page);
  const HashBinRange& cur_range = page->get_bin_range();
  ASSERT_ND(page->get_level() == 0);
  ASSERT_ND(page->header().snapshot_);
  *next_lowest_bin = kInvalidHashBin;
  uint32_t installed_diff = 0;

  for (uint32_t i = 0; i < input_count_; ++i) {
    ComposedBinsBuffer* input = inputs_ + i;
    while (input->has_more()) {
      const ComposedBin& entry = input->get_cur_bin();
      ASSERT_ND(cur_range.begin_ <= entry.bin_);
      ASSERT_ND(entry.page_id_ != 0);
      ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(entry.page_id_) == snapshot_id_);
      if (entry.bin_ >= cur_range.end_) {
        // no more bins for this page from this input
        *next_lowest_bin = std::min<HashBin>(*next_lowest_bin, entry.bin_);
        break;
      }

      uint16_t index = entry.bin_ - cur_range.begin_;
      ASSERT_ND(index < kHashIntermediatePageFanout);
      DualPagePointer* target = page->get_pointer_address(index);
      ASSERT_ND(target->volatile_pointer_.is_null());
      ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(target->snapshot_pointer_)
        != snapshot_id_);
      target->snapshot_pointer_ = entry.page_id_;
      ++installed_diff;

      CHECK_ERROR_CODE(input->next_bin());
    }
  }

  *installed_count += installed_diff;
  return kErrorCodeOk;
}

ErrorCode ComposedBinsMergedStream::switch_path(
  HashBin lowest_bin,
  cache::SnapshotFileSet* fileset,
  snapshot::SnapshotWriter* writer,
  uint32_t* writer_buffer_pos,
  uint32_t* writer_higher_buffer_pos) {
  ASSERT_ND(lowest_bin != kInvalidHashBin);
  // because a path in this sub-tree must be switched, this is at least 3-levels.
  // levels_==1 : single-level hash, separately processed in construct_root
  // levels_==2 : then each sub-tree has only one level-0 page. switch_path is never called.
  ASSERT_ND(levels_ >= 3U);
  ASSERT_ND(!cur_path_[0]->get_bin_range().contains(lowest_bin));
  ASSERT_ND(!cur_path_[levels_ - 1U]->get_bin_range().contains(lowest_bin));  // root page
  ASSERT_ND(!cur_path_[levels_ - 2U]->get_bin_range().contains(lowest_bin));  // root_child page

  // where do we have to switch?
  uint8_t valid_upto;
  for (valid_upto = 1U; valid_upto + 2U < levels_; ++valid_upto) {
    if (cur_path_[valid_upto]->get_bin_range().contains(lowest_bin)) {
      break;
    }
  }
  ASSERT_ND(cur_path_[valid_upto]->get_bin_range().contains(lowest_bin));
  ASSERT_ND(!cur_path_[valid_upto - 1U]->get_bin_range().contains(lowest_bin));

  return open_path(
    lowest_bin,
    valid_upto,
    fileset,
    writer,
    writer_buffer_pos,
    writer_higher_buffer_pos);
}

ErrorCode ComposedBinsMergedStream::open_path(
  HashBin bin,
  uint8_t fixed_upto_level,
  cache::SnapshotFileSet* fileset,
  snapshot::SnapshotWriter* writer,
  uint32_t* writer_buffer_pos,
  uint32_t* writer_higher_buffer_pos) {
  ASSERT_ND(bin != kInvalidHashBin);
  ASSERT_ND(cur_path_[fixed_upto_level]->get_bin_range().contains(bin));
  ASSERT_ND(fixed_upto_level < levels_);

  // we need a room for one-page in main buffer, and levels pages in higher-level buffer.
  CHECK_ERROR_CODE(assure_writer_buffer(writer, writer_buffer_pos, *writer_higher_buffer_pos));
  PagePtr higher_base = reinterpret_cast<PagePtr>(writer->get_intermediate_base());
  PagePtr main_base = reinterpret_cast<PagePtr>(writer->get_page_base());

  IntermediateRoute route = IntermediateRoute::construct(bin);
  ASSERT_ND(route.route[levels_] == 0);
  const StorageId storage_id = cur_path_[levels_ - 1U]->header().storage_id_;

  // let's open the page that contains the bin. higher levels first.
  for (uint8_t parent_level = fixed_upto_level; parent_level > 0; --parent_level) {
    uint8_t level = parent_level - 1U;
    PagePtr parent = cur_path_[parent_level];
    ASSERT_ND(parent->get_level() == parent_level);
    ASSERT_ND(parent->get_bin_range().contains(bin));

    const uint16_t index = route.route[parent_level];
    HashBin range_begin = parent->get_bin_range().begin_ + index * kHashMaxBins[parent_level];
    ASSERT_ND(parent->get_bin_range().length() == kHashMaxBins[parent_level + 1U]);

    SnapshotPagePointer old_page_id = parent->get_pointer(index).snapshot_pointer_;
    SnapshotPagePointer new_page_id;
    PagePtr new_page;
    if (level > 0) {
      new_page_id = *writer_higher_buffer_pos;
      new_page = higher_base + new_page_id;
      ++(*writer_higher_buffer_pos);
      ASSERT_ND((*writer_higher_buffer_pos) <= writer->get_intermediate_size());
    } else {
      // Unlike higher-levels, we can finalize the page ID for level-0 pages.
      new_page_id = writer->get_next_page_id() + (*writer_buffer_pos);
      new_page = main_base + *writer_buffer_pos;
      ++(*writer_buffer_pos);
      ASSERT_ND((*writer_buffer_pos) <= writer->get_page_size());
    }
    cur_path_[level] = new_page;

    if (old_page_id == 0) {
      // the page didn't exist before, create a new one.
      new_page->initialize_snapshot_page(storage_id, new_page_id, level, range_begin);
    } else {
      // otherwise, start from the previous page image.
      ASSERT_ND(extract_snapshot_id_from_snapshot_pointer(old_page_id) != snapshot_id_);
      CHECK_ERROR_CODE(fileset->read_page(old_page_id, new_page));
      ASSERT_ND(new_page->get_bin_range().begin_ == range_begin);
      ASSERT_ND(new_page->get_level() == level);
      new_page->header().page_id_ = new_page_id;
    }
    parent->get_pointer(index).snapshot_pointer_ = new_page_id;
  }

  return kErrorCodeOk;
}

ErrorCode ComposedBinsMergedStream::assure_writer_buffer(
  snapshot::SnapshotWriter* writer,
  uint32_t* writer_buffer_pos,
  uint32_t writer_higher_buffer_pos) {
  ASSERT_ND(*writer_buffer_pos <= writer->get_page_size());
  ASSERT_ND(writer_higher_buffer_pos <= writer->get_intermediate_size());
  if (UNLIKELY(*writer_buffer_pos == writer->get_page_size())) {
    CHECK_ERROR_CODE(writer->dump_pages(0, *writer_buffer_pos));
    *writer_buffer_pos = 0;
  }
  if (UNLIKELY(writer_higher_buffer_pos + levels_ >= writer->get_intermediate_size())) {
    CHECK_ERROR_CODE(writer->expand_intermediate_memory(writer_higher_buffer_pos + levels_, true));
    ASSERT_ND(writer_higher_buffer_pos + levels_ < writer->get_intermediate_size());
  }
  return kErrorCodeOk;
}

}  // namespace hash
}  // namespace storage
}  // namespace foedus

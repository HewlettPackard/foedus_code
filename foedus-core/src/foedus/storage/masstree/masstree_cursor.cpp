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
#include "foedus/storage/masstree/masstree_page_impl.hpp"
#include "foedus/storage/masstree/masstree_retry_impl.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/storage/masstree/masstree_storage_pimpl.hpp"
#include "foedus/xct/xct.hpp"


namespace foedus {
namespace storage {
namespace masstree {

MasstreeCursor::MasstreeCursor(Engine* engine, MasstreeStorage* storage, thread::Thread* context)
  : engine_(engine), storage_(storage), storage_pimpl_(storage->get_pimpl()), context_(context) {
  for_writes_ = false;
  forward_cursor_ = true;
  cur_page_address_ = nullptr;
  cur_record_ = kMaxRecords;
  // route_count_ = 0;
  end_inclusive_ = false;
  end_key_length_ = 0;
  end_key_ = nullptr;
  cur_key_length_ = 0;
}

ErrorCode MasstreeCursor::seek(
  const char* begin_key,
  uint16_t begin_key_length,
  const char* end_key,
  uint16_t end_key_length,
  bool forward_cursor,
  bool for_writes,
  bool begin_inclusive,
  bool end_inclusive) {
  forward_cursor_ = forward_cursor;
  for_writes_ = for_writes;
  end_inclusive_ = end_inclusive;
  end_key_ = end_key;
  end_key_length_ = end_key_length;

  // find the border page
  CHECK_ERROR_CODE(masstree_retry([this, begin_key, begin_key_length]() {
    // return seek_main(begin_key, begin_key_length, begin_inclusive);
    // currently we just use locate_record each time we swtich page or even find a record
    // that points to next layer.
    uint8_t index;
    CHECK_ERROR_CODE(storage_pimpl_->locate_record(
      context_,
      begin_key,
      begin_key_length,
      true,  // TODO(Hideaki) so far always find a volatile page. we need to change locate_record()
      &cur_page_address_,
      &index));
    // read the entire page with the optimistic read protocol
    cur_page_stable_ = cur_page_address_->get_stable_version();
    assorted::memory_fence_consume();
    std::memcpy(cur_page_, cur_page_address_, kPageSize);
    assorted::memory_fence_consume();
    PageVersion cur_page_stable_again = cur_page_address_->get_stable_version();
    if (cur_page_stable_.data_ != cur_page_stable_again.data_) {
      return kErrorCodeStrMasstreeRetry;
    }
    return kErrorCodeOk;
  }));

  // setup other variables
  MasstreeBorderPage* cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page_);
  cur_layer_ = cur_page->get_layer();
  cur_page_records_ = cur_page_stable_.get_key_count();
  std::memcpy(cur_key_, begin_key, cur_layer_ * sizeof(KeySlice));

  // sort entries in this page
  struct Sorter {
    Sorter(MasstreeBorderPage* page) : page_(page) {}
    bool operator() (uint8_t left, uint8_t right) {
      KeySlice left_slice = page_->get_slice(left);
      KeySlice right_slice = page_->get_slice(right);
      if (left_slice < right_slice) {
        return true;
      } else if (left_slice == right_slice) {
        return page_->get_remaining_key_length(left) < page_->get_remaining_key_length(right);
      } else {
        return false;
      }
    }
    MasstreeBorderPage* page_;
  };
  for (uint8_t i = 0; i < cur_page_records_; ++i) {
    cur_page_order_[i] = i;
  }

  // this sort order in page is correct even without evaluating the suffix.
  // however, to compare with our searching key, we need to consider suffix
  std::sort(cur_page_order_, cur_page_order_ + cur_page_records_, Sorter(cur_page));

  // TODO(Hideaki) probably we should have dedicated locate_record for cursor.
  // otherwise we can't fully address the messes below
  KeySlice slice = slice_layer(begin_key, begin_key_length, cur_layer_);
  uint8_t remaining = begin_key_length - cur_layer_ * sizeof(KeySlice);
  const char* begin_suffix = begin_key + (cur_layer_ + 1) * sizeof(KeySlice);
  int16_t found_record;  // signed. -1 to moved to previous page
  if (forward_cursor) {
    found_record = kMaxRecords;
    for (uint8_t i = 0; i < cur_page_records_; ++i) {
      uint8_t index = cur_page_order_[i];
      uint8_t len = cur_page->get_remaining_key_length(index);
      ASSERT_ND(len != 255U);  // TODO(Hideaki) currently we don't handle this case
      if (cur_page->get_owner_id(index)->is_deleted()) {
        continue;
      } else if (cur_page->get_slice(index) < slice) {
        continue;
      } else if (cur_page->get_slice(index) == slice) {
        if (len < remaining) {
          continue;
        } else if (len == remaining) {
          if (remaining > sizeof(KeySlice)) {
            // has suffix
            int cmp = std::memcmp(
              cur_page->get_record(index),
              begin_suffix,
              remaining - sizeof(KeySlice));
            if (cmp < 0) {
              // the record in page was smaller, but this can be the only record that could be
              // same as the searching key because there is only one record that has length >8
              // of the slice (except next layer)
              found_record = i + 1;
              break;
            } else if (cmp == 0) {
              if (begin_inclusive) {
                found_record = i;
                break;
              } else {
                found_record = i + 1;
                break;
              }
            } else {
              found_record = i;
              break;
            }
          } else {
            // no suffix. so it's surely matched
            if (begin_inclusive) {
              found_record = i;
              break;
            } else {
              found_record = i + 1;
              break;
            }
          }
        }
      } else {
        found_record = i;
        break;
      }
    }
  } else {
    found_record = -1;
    for (int16_t i = cur_page_records_ - 1; i >= 0; --i) {
      uint8_t index = cur_page_order_[i];
      uint8_t len = cur_page->get_remaining_key_length(index);
      ASSERT_ND(len != 255U);  // TODO(Hideaki) currently we don't handle this case
      if (cur_page->get_owner_id(index)->is_deleted()) {
        continue;
      } else if (cur_page->get_slice(index) > slice) {
        continue;
      } else if (cur_page->get_slice(index) == slice) {
        if (len > remaining) {
          continue;
        } else if (len == remaining) {
          if (remaining > sizeof(KeySlice)) {
            // has suffix
            int cmp = std::memcmp(
              cur_page->get_record(index),
              begin_suffix,
              remaining - sizeof(KeySlice));
            if (cmp > 0) {
              found_record = i - 1;
              break;
            } else if (cmp == 0) {
              if (begin_inclusive) {
                found_record = i;
                break;
              } else {
                found_record = i - 1;
                break;
              }
            } else {
              found_record = i;
              break;
            }
          } else {
            if (begin_inclusive) {
              found_record = i;
              break;
            } else {
              found_record = i - 1;
              break;
            }
          }
        }
      } else {
        found_record = i;
        break;
      }
    }
  }
  // TODO(Hideaki) no, this doesn't work in case it points to next layer.
  /*
  if (found_record < 0 || found_record >= cur_page_records_) {
    DVLOG(0) << "mm, going over page boundary for seek. possible only for exclusive search."
      << ", begin_inclusive=" << begin_inclusive;
    ASSERT_ND(!begin_inclusive);
    // redo search to make sure we hit
    if (found_record < 0) {
      ASSERT_ND(!forward_cursor);
      std::string modified_key(cumutative_prefix_, cur_layer_ * sizeof(KeySlice));
      uint64_t be_key = assorted::htobe<uint64_t>(cur_page->get_low_fence());
      modified_key.append(&be_key, sizeof(KeySlice));
      CHECK_ERROR_CODE(seek(
        modified_key.data(),
        (cur_layer_ + 1U) * sizeof(KeySlice),
        end_key,
        end_key_length,
        forward_cursor,
        for_writes,
        false,
        end_inclusive));
    } else {
      ASSERT_ND(forward_cursor);
    }
  }
  */
  ASSERT_ND(found_record >= 0);
  ASSERT_ND(found_record < cur_page_records_);
  if (found_record < 0) {
    found_record = 0;
  } else if (found_record >= cur_page_records_) {
    found_record = cur_page_records_ - 1;
  }
  cur_record_ = found_record;

  context_->get_current_xct().add_to_page_version_set(
    &cur_page_address_->header().page_version_,
    cur_page_stable_);
  CHECK_ERROR_CODE(read_cur_key());
  return kErrorCodeOk;
}

ErrorCode MasstreeCursor::read_cur_key() {
  ASSERT_ND(is_valid_record());
  ASSERT_ND(cur_record_ < cur_page_records_);
  MasstreeBorderPage* cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page_);
  uint8_t index = cur_page_order_[cur_record_];
  context_->get_current_xct().add_to_read_set(
    storage_,
    // TODO(Hideaki) what if it's locked as of copying
    *cur_page->get_owner_id(index),
    cur_page_address_->get_owner_id(index));
  uint8_t remaining = cur_page->get_remaining_key_length(index);
  cur_key_length_ = cur_layer_ * sizeof(KeySlice) + remaining;
  uint64_t be_slice = assorted::htobe<uint64_t>(cur_page->get_slice(index));
  std::memcpy(
    cur_key_ + cur_layer_ * sizeof(KeySlice),
    &be_slice,
    std::min<uint64_t>(remaining, sizeof(KeySlice)));
  if (remaining > sizeof(KeySlice)) {
    std::memcpy(
      cur_key_ + (cur_layer_ + 1) * sizeof(KeySlice),
      cur_page->get_record(index),
      remaining - sizeof(KeySlice));
  }
}


/* TODO(Hideaki) cursor with route information.
ErrorCode MasstreeCursor::seek_main(
  const char* key,
  uint16_t key_length,
  bool key_inclusive,
  MasstreeBorderPage** out_page,
  uint8_t* record_index) {
  MasstreePage* layer_root;
  PageVersion root_version;
  CHECK_ERROR_CODE(storage_pimpl_->get_first_root(context_, &layer_root, &root_version));
  for (uint16_t current_layer = 0;; ++current_layer) {
    uint8_t remaining_length = key_length - current_layer * 8;
    KeySlice slice = slice_layer(key, key_length, current_layer);
    const void* suffix = reinterpret_cast<const char*>(key) + (current_layer + 1) * 8;
    MasstreeBorderPage* border;
    PageVersion border_version;
    CHECK_ERROR_CODE(find_border(
      context_,
      layer_root,
      current_layer,
      for_writes,
      slice,
      &border,
      &border_version));
    uint8_t stable_key_count = border_version.get_key_count();
    uint8_t index = border->find_key(stable_key_count, slice, suffix, remaining_length);

    if (index == MasstreeBorderPage::kMaxKeys) {
      // this means not found
      // TODO(Hideaki) range lock
      return kErrorCodeStrKeyNotFound;
    }
    if (border->does_point_to_layer(index)) {
      CHECK_ERROR_CODE(follow_layer(context, for_writes, border, index, &layer_root));
      continue;
    } else {
      *out_page = border;
      *record_index = index;
      return kErrorCodeOk;
    }
  }

  CHECK_ERROR_CODE(storage_pimpl_->locate_record(
    context_,
    begin_key,
    begin_key_length,
    true,  // TODO(Hideaki) so far always find a volatile page. we need to change locate_record()
    &border,
    &index));
  return kErrorCodeOk;
}

ErrorCode MasstreeCursor::seek_recursive(
  MasstreePage** root,
  PageVersion* version) {
  while (true) {
    ASSERT_ND(first_root_pointer_.volatile_pointer_.components.offset);
    VolatilePagePointer pointer = first_root_pointer_.volatile_pointer_;
    MasstreePage* page = reinterpret_cast<MasstreePage*>(
      context->get_global_volatile_page_resolver().resolve_offset(pointer));
    *version = page->get_stable_version();
    *root = page;

    if (!version->is_root()) {
      // someone else has just changed the root!
      LOG(INFO) << "Interesting. Someone has just changed the root. retry.";
      continue;
    } else if (UNLIKELY(version->has_foster_child())) {
      // root page has a foster child... time for tree growth!
      CHECK_ERROR_CODE(grow_root(context, &first_root_pointer_, page));
      continue;
    }

    // thanks to the is_root check above, we don't need to add this to the pointer set.
    return kErrorCodeOk;
  }
}
*/

ErrorCode MasstreeCursor::next() {
  MasstreeBorderPage* cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page_);
  if (forward_cursor_) {
    ++cur_record_;
    if (cur_record_ >= cur_page_records_) {
      if (cur_page->is_high_fence_supremum()) {
        cur_record_ = kMaxRecords;
        return kErrorCodeOk;
      } else {
        // TODO(Hideaki) cursor's own traverse needed
        cur_record_ = kMaxRecords;
        return kErrorCodeOk;
      }
    }
  } else {
    if (cur_record_ == 0) {
      if (cur_page->get_low_fence() == kInfimumSlice) {
        cur_record_ = kMaxRecords;
        return kErrorCodeOk;
      } else {
        // TODO(Hideaki) cursor's own traverse needed
        cur_record_ = kMaxRecords;
        return kErrorCodeOk;
      }
    } else {
      --cur_record_;
    }
  }
  return read_cur_key();
}


KeySlice MasstreeCursor::get_key_current_slice() {
  ASSERT_ND(is_valid_record());
  MasstreeBorderPage* cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page_);
  return cur_page->get_slice(get_cur_index());
}

const char* MasstreeCursor::get_key_suffix() {
  ASSERT_ND(is_valid_record());
  MasstreeBorderPage* cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page_);
  return cur_page->get_record(get_cur_index());
}

uint8_t MasstreeCursor::get_key_suffix_length() {
  ASSERT_ND(is_valid_record());
  MasstreeBorderPage* cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page_);
  return cur_page->get_suffix_length(get_cur_index());
}

uint16_t MasstreeCursor::get_key_length() {
  ASSERT_ND(is_valid_record());
  return cur_key_length_;
}

void MasstreeCursor::get_key(char* key) {
  ASSERT_ND(is_valid_record());
  std::memcpy(key, cur_key_, cur_key_length_);
}
void MasstreeCursor::get_key_part(char* key, uint16_t key_offset, uint16_t key_count) {
  ASSERT_ND(is_valid_record());
  ASSERT_ND(key_offset + key_count <= cur_key_length_);
  std::memcpy(key, cur_key_ + key_offset, key_count);
}

void MasstreeCursor::get_payload(void* payload) {
  ASSERT_ND(is_valid_record());
  MasstreeBorderPage* cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page_);
  std::memcpy(
    payload,
    cur_page->get_record(get_cur_index()) + get_key_suffix_length(),
    get_payload_length());
}

uint16_t MasstreeCursor::get_payload_length() {
  ASSERT_ND(is_valid_record());
  MasstreeBorderPage* cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page_);
  return cur_page->get_payload_length(get_cur_index());
}


void MasstreeCursor::get_payload_part(
  void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  ASSERT_ND(is_valid_record());
  ASSERT_ND(payload_offset + payload_count <= get_payload_length());
  MasstreeBorderPage* cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page_);
  std::memcpy(
    payload,
    cur_page->get_record(get_cur_index()) + get_key_suffix_length() + payload_offset,
    payload_count);
}

template <typename PAYLOAD>
void MasstreeCursor::get_payload_primitive(PAYLOAD* payload, uint16_t payload_offset) {
  ASSERT_ND(is_valid_record());
  ASSERT_ND(payload_offset + sizeof(PAYLOAD) <= get_payload_length());
  MasstreeBorderPage* cur_page = reinterpret_cast<MasstreeBorderPage*>(cur_page_);
  const PAYLOAD* address = reinterpret_cast<const PAYLOAD*>(
    cur_page->get_record(get_cur_index()) + get_key_suffix_length() + payload_offset);
  *payload = *address;
}

ErrorCode MasstreeCursor::overwrite_record(
  const void* payload,
  uint16_t payload_offset,
  uint16_t payload_count) {
  return masstree_retry([this, payload, payload_offset, payload_count]() {
    return storage_pimpl_->overwrite_general(
      context_,
      cur_page_address_,
      get_cur_index(),
      cur_key_,
      cur_key_length_,
      payload,
      payload_offset,
      payload_count);
  });
}

template <typename PAYLOAD>
ErrorCode MasstreeCursor::overwrite_record_primitive(PAYLOAD payload, uint16_t payload_offset) {
  return masstree_retry([this, payload, payload_offset]() {
    return storage_pimpl_->overwrite_general(
      context_,
      cur_page_address_,
      get_cur_index(),
      cur_key_,
      cur_key_length_,
      &payload,
      payload_offset,
      sizeof(PAYLOAD));
  });
}

ErrorCode MasstreeCursor::delete_record() {
  return masstree_retry([this]() {
    return storage_pimpl_->delete_general(
      context_,
      cur_page_address_,
      get_cur_index(),
      cur_key_,
      cur_key_length_);
  });
}

template <typename PAYLOAD>
ErrorCode MasstreeCursor::increment_record(PAYLOAD* value, uint16_t payload_offset) {
  return masstree_retry([this, value, payload_offset]() {
    return storage_pimpl_->increment_general<PAYLOAD>(
      context_,
      cur_page_address_,
      get_cur_index(),
      cur_key_,
      cur_key_length_,
      value,
      payload_offset);
  });
}



}  // namespace masstree
}  // namespace storage
}  // namespace foedus

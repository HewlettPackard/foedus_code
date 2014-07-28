/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_CURSOR_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_CURSOR_HPP_

#include <stdint.h>

#include <cstring>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/engine.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/**
 * @brief Represents a cursor object for Masstree storage.
 * @ingroup MASSTREE
 */
class MasstreeCursor CXX11_FINAL {
 public:
  MasstreeCursor(Engine* engine, MasstreeStorage* storage, thread::Thread* context);

  thread::Thread*   get_context() { return context_; }
  MasstreeStorage*  get_storage() { return storage_; }
  bool              is_for_writes() { return for_writes_; }
  bool              is_forward_cursor() { return forward_cursor_; }

  ErrorCode   seek(
    const char* begin_key,
    uint16_t begin_key_length,
    const char* end_key,
    uint16_t end_key_length,
    bool forward_cursor = true,
    bool for_writes = true,
    bool begin_inclusive = true,
    bool end_inclusive = false);

  uint8_t     get_cur_layer() { return cur_layer_; }
  KeySlice    get_key_current_slice();
  const char* get_key_suffix();
  uint8_t     get_key_suffix_length();

  void      get_key(char* key);
  uint16_t  get_key_length();
  void      get_key_part(char* key, uint16_t key_offset, uint16_t key_count);

  ErrorCode next();
  bool      is_valid_record();

  void      get_payload(void* payload);
  uint16_t  get_payload_length();
  void      get_payload_part(void* payload, uint16_t payload_offset, uint16_t payload_count);
  template <typename PAYLOAD>
  void      get_payload_primitive(PAYLOAD* payload, uint16_t payload_offset);

  ErrorCode delete_record();

  ErrorCode overwrite_record(const void* payload, uint16_t payload_offset, uint16_t payload_count);
  template <typename PAYLOAD>
  ErrorCode overwrite_record_primitive(PAYLOAD payload, uint16_t payload_offset);

  template <typename PAYLOAD>
  ErrorCode increment_record(PAYLOAD* value, uint16_t payload_offset);

 private:
  enum Constants {
    kMaxRoutes = 64,
    kMaxRecords = 64,
  };
  struct Route {
    MasstreePage* page_;
    /** stable version of the page when we visited it */
    PageVersion   stable_;
    /** only for intermediate pages */
    PageVersion   stable_mini_;
    /** which index in the page are we now following. */
    uint16_t      index_;
    /** only for intermediate pages */
    uint16_t      index_mini_;
  };

  Engine* const engine_;
  MasstreeStorage* const storage_;
  MasstreeStoragePimpl* const storage_pimpl_;
  thread::Thread* const context_;
  bool        for_writes_;
  bool        forward_cursor_;

  // if forward_cursor_, store high_xxx, if not, low_xxx.
  bool        end_inclusive_;
  uint16_t    end_key_length_;
  const char* end_key_;

  /** the current border page. may or maynot be snapshot. */
  MasstreeBorderPage* cur_page_address_;

  /** stable version of teh current border page as of copying cur_page_. */
  PageVersion cur_page_stable_;
  /** same as cur_page_stable_.get_key_count() */
  uint8_t     cur_page_records_;
  /** cur_page_order_[0] is the index of smallest record in cur_page, [1] second smallest... */
  uint8_t     cur_page_order_[kMaxRecords];

  /**
   * 0 <= cur_record_ < cur_page_records_. not the index in page but the index in order.
   * kMaxRecord means not a valid record (end or not seeked yet).
   */
  uint8_t     cur_record_;
  /** always equal to cur_page->get_layer(). for convenience. */
  uint16_t    cur_layer_;

  uint16_t    cur_key_length_;

  /** number of higher layer pages. the current border page is not included. so, might be 0. */
  /*
  uint16_t    route_count_;
  Route       routes_[kMaxRoutes];
  */

  /** copied image of the current border page. */
  char        cur_page_[kPageSize];

  /** big-endian key */
  char        cur_key_[kMaxKeyLength];

  ErrorCode   read_cur_key();
  uint8_t     get_cur_index() {
    ASSERT_ND(is_valid_record());
    return cur_page_order_[cur_record_];
  }
};

bool MasstreeCursor::is_valid_record() {
  return cur_record_ < kMaxRecords;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_CURSOR_HPP_

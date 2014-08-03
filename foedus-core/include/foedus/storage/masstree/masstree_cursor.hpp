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
#include "foedus/assorted/endianness.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {
/**
 * @brief Represents a cursor object for Masstree storage.
 * @ingroup MASSTREE
 * @details
 * @par Dynamic Memory
 * This cursor objects uses a few dymanically allocated memory for keys, route information,
 * etc. However, we of course can't tolerate new/delete during transaction.
 * We thus grab memory from the memory pool for snapshot pages (could be volatile page's, but
 * probably volatile memory pool is more limiting, so let's grab it from snapshot memory pool).
 * These memory are allocated via libnuma and using THP, so it's also faster.
 * The only drawback is that we consume them in a little bit generous way, but shouldn't be a
 * big issue.
 * All of them are returned to the pool in destructor.
 */
class MasstreeCursor CXX11_FINAL {
 public:
  struct Route {
    MasstreePage* page_;
    /** version as of getting calculating order_. */
    PageVersion stable_;
    /** only for interior. */
    PageVersion stable_mini_;
    /** index in ordered keys. in interior, same. */
    uint8_t index_;
    /** only for interior. */
    uint8_t index_mini_;
    /** same as stable_.get_key_count() */
    uint8_t key_count_;
    /** same as stable_mini_.get_key_count() */
    uint8_t key_count_mini_;

    KeySlice done_upto_;

    uint8_t done_upto_length_;

    /** whether page_ is a snapshot page */
    bool    snapshot_;

    /** only for border. order_[0] is the index of smallest record, [1] second smallest... */
    uint8_t order_[64];

    uint8_t get_cur_original_index() const ALWAYS_INLINE {
      return order_[index_];
    }
    uint8_t get_original_index(uint8_t index) const ALWAYS_INLINE {
      return order_[index];
    }
    bool    is_valid_record() const ALWAYS_INLINE {
      return index_ < key_count_;
    }
    void setup_order();
  };
  enum SearchType {
    kForwardInclusive = 0,
    kForwardExclusive,
    kBackwardInclusive,
    kBackwardExclusive,
  };
  enum KeyCompareResult {
    kCurKeySmaller,
    kCurKeyEquals,
    kCurKeyLarger,
    // the following two are only when cur key points to next layer. instead, no equals for layer
    kCurKeyBeingsWith,
    kCurKeyContains,
  };
  enum Constants {
    kMaxRecords = 64,
    kMaxRoutes = kPageSize / sizeof(Route),
    kKeyLengthExtremum = 0,
  };

  MasstreeCursor(Engine* engine, MasstreeStorage* storage, thread::Thread* context);
  ~MasstreeCursor();

  thread::Thread*   get_context() { return context_; }
  MasstreeStorage*  get_storage() { return storage_; }
  bool              is_for_writes() const { return for_writes_; }
  bool              is_forward_cursor() const { return forward_cursor_; }

  ErrorCode   open(
    const char* begin_key = CXX11_NULLPTR,
    uint16_t begin_key_length = kKeyLengthExtremum,
    const char* end_key = CXX11_NULLPTR,
    uint16_t end_key_length = kKeyLengthExtremum,
    bool forward_cursor = true,
    bool for_writes = false,
    bool begin_inclusive = true,
    bool end_inclusive = false);

  ErrorCode   open_normalized(
    KeySlice begin_key,
    KeySlice end_key,
    bool forward_cursor = true,
    bool for_writes = false,
    bool begin_inclusive = true,
    bool end_inclusive = false) {
    KeySlice begin_key_be = assorted::htobe<KeySlice>(begin_key);
    KeySlice end_key_be = assorted::htobe<KeySlice>(end_key);
    return open(
      reinterpret_cast<const char*>(&begin_key_be),
      sizeof(KeySlice),
      reinterpret_cast<const char*>(&end_key_be),
      sizeof(KeySlice),
      forward_cursor,
      for_writes,
      begin_inclusive,
      end_inclusive);
  }

  bool      is_valid_record() const ALWAYS_INLINE {
    return !reached_end_ && cur_route()->is_valid_record();
  }
  const char* get_key() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    return cur_key_;
  }
  uint16_t  get_key_length() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    return cur_key_length_;
  }
  const char* get_payload() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    return cur_payload_;
  }
  uint16_t  get_payload_length() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    return cur_payload_length_;
  }

  ErrorCode next();


  ErrorCode delete_record();

  ErrorCode overwrite_record(const void* payload, uint16_t payload_offset, uint16_t payload_count);
  template <typename PAYLOAD>
  ErrorCode overwrite_record_primitive(PAYLOAD payload, uint16_t payload_offset);

  template <typename PAYLOAD>
  ErrorCode increment_record(PAYLOAD* value, uint16_t payload_offset);

 private:
  Engine* const engine_;
  MasstreeStorage* const storage_;
  MasstreeStoragePimpl* const storage_pimpl_;
  thread::Thread* const context_;
  xct::Xct* const current_xct_;

  bool        for_writes_;
  bool        forward_cursor_;
  bool        end_inclusive_;
  bool        reached_end_;

  /** If this value is zero, it means supremum. */
  uint16_t    end_key_length_;
  /** If this value is zero, it means supremum. */
  uint16_t    cur_key_length_;
  uint16_t    cur_payload_length_;
  /** If this value is zero, it means supremum. */
  uint16_t    search_key_length_;
  SearchType  search_type_;


  /** number of higher layer pages. the current border page is not included. so, might be 0. */
  uint16_t    route_count_;

  uint8_t     cur_key_in_layer_remaining_;
  KeySlice    cur_key_in_layer_slice_;
  xct::XctId  cur_key_observed_owner_id_;
  xct::XctId* cur_key_owner_id_address;

  /** full big-endian key to terminate search. backed by end_key_memory_offset_ */
  char*       end_key_;

  /** full big-endian key of current record. backed by cur_key_memory_offset_  */
  char*       cur_key_;

  /** full payload of current record. backed by cur_payload_memory_offset_  */
  char*       cur_payload_;

  /** full big-endian key of current search. backed by search_key_memory_offset_  */
  char*       search_key_;

  /** stable version of teh current border page as of copying cur_page_. */
  PageVersion cur_page_stable_;

  /** backed by routes_memory_offset_. */
  Route*      routes_;

  memory::PagePoolOffset routes_memory_offset_;
  memory::PagePoolOffset end_key_memory_offset_;
  memory::PagePoolOffset cur_key_memory_offset_;
  memory::PagePoolOffset cur_payload_memory_offset_;
  memory::PagePoolOffset search_key_memory_offset_;

  ErrorCode push_route(MasstreePage* page, PageVersion* page_version);
  void      fetch_cur_record(MasstreeBorderPage* page, uint8_t record);
  void      fetch_cur_payload();
  void      check_end_key();
  KeyCompareResult compare_cur_key_aginst_search_key(KeySlice slice, uint8_t layer) const;
  KeyCompareResult compare_cur_key_aginst_end_key() const;
  KeyCompareResult compare_cur_key(
    KeySlice slice,
    uint8_t layer,
    const char* full_key,
    uint16_t full_length) const;

  uint8_t       get_cur_index() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    return cur_route()->get_cur_original_index();
  }
  MasstreePage* get_cur_page() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    return cur_route()->page_;
  }

  Route*        cur_route() ALWAYS_INLINE {
    ASSERT_ND(route_count_ > 0);
    return routes_ + route_count_ - 1;
  }
  const Route*  cur_route() const ALWAYS_INLINE {
    ASSERT_ND(route_count_ > 0);
    return routes_ + route_count_ - 1;
  }
  ErrorCode     search_layer();

  template <typename T>
  void release_if_exist(memory::PagePoolOffset* offset, T** pointer);

  template <typename T>
  ErrorCode allocate_if_not_exist(memory::PagePoolOffset* offset, T** pointer);

  bool is_search_key_extremum() const ALWAYS_INLINE {
    return search_key_length_ == kKeyLengthExtremum;
  }
  bool is_end_key_supremum() const ALWAYS_INLINE {
    return end_key_length_ == kKeyLengthExtremum;
  }

  template<typename PAGE_TYPE>
  ErrorCode follow_foster(KeySlice slice, PAGE_TYPE** cur, PageVersion* version);

  // locate_xxx is for initial search
  ErrorCode locate_layer(uint8_t layer);
  ErrorCode locate_border(KeySlice slice);
  ErrorCode locate_next_layer();
  ErrorCode locate_descend(KeySlice slice);

  // proceed_xxx is for next
  ErrorCode proceed_route();
  ErrorCode proceed_route_border();
  ErrorCode proceed_route_intermediate();
  ErrorCode proceed_pop();
  ErrorCode proceed_next_layer();
  ErrorCode proceed_deeper();
  ErrorCode proceed_deeper_border();
  ErrorCode proceed_deeper_intermediate();

  void assert_modify() const ALWAYS_INLINE {
    ASSERT_ND(for_writes_);
    ASSERT_ND(is_valid_record());
    ASSERT_ND(!cur_route()->snapshot_);
    ASSERT_ND(reinterpret_cast<Page*>(get_cur_page())->get_header().get_page_type()
      == kMasstreeBorderPageType);
  }
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_CURSOR_HPP_

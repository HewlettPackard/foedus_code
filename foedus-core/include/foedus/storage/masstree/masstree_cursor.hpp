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
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_CURSOR_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_CURSOR_HPP_

#include <stdint.h>

#include <cstring>
#include <string>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/engine.hpp"
#include "foedus/assorted/endianness.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_record_location.hpp"
#include "foedus/storage/masstree/masstree_storage.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace storage {
namespace masstree {
/**
 * @brief Represents a cursor object for Masstree storage.
 * @ingroup MASSTREE
 * @details
 * @par Cursor Example
 * Below is a typical usecase (from TPC-C) of this cursor class.
 * @code{.cpp}
 * ... (begin xct, etc)
 * MasstreeCursor cursor(orderlines, context);
 * CHECK_ERROR_CODE(cursor.open_normalized(low, high, true, true));
 * while (cursor.is_valid_record()) {
 *  const char* key_be = cursor.get_key();
 *  const OrderlineData* payload = reinterpret_cast<const OrderlineData*>(cursor.get_payload());
 *  *ol_amount_total += payload->amount_;
 *  ++(*ol_count);
 *  CHECK_ERROR_CODE(cursor.overwrite_record(delivery_date, offset, delivery_date_len));
 *  CHECK_ERROR_CODE(cursor.next());
 * }
 * ... (commit xct, etc)
 * @endcode
 *
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
  /**
   * @brief Represents one page in the current search path from layer0-root.
   * @details
   * Either it's interior or border, this object stores some kind of a marker
   * to note up to where we read the page so far.
   */
  struct Route {
    enum MovedPageSearchStatus {
      kNotMovedPage = 0,
      kMovedPageSearchedNeither = 1,
      kMovedPageSearchedOne = 2,
      kMovedPageSearchedBoth = 3,
    };
    MasstreePage* page_;
    /** version as of getting calculating order_. */
    PageVersionStatus stable_;
    /** index in ordered keys. in interior, same. */
    SlotIndex index_;
    /** only for interior. */
    SlotIndex index_mini_;
    /**
     * same as stable_.get_key_count()
     * @note Even in a border page, \b key_count_ \b might \b be \b zero.
     * Such a case might happen right after no-record-split, split for a super-long key/value etc.
     * We tolerate such a page in routes_, and just skip over it in next() etc.
     */
    SlotIndex key_count_;
    /** only for interior. */
    SlotIndex key_count_mini_;

    /** whether page_ is a snapshot page */
    bool      snapshot_;
    /** Shorthand for page_->get_layer() */
    Layer     layer_;

    /** only when stable_ indicates that this page is a moved page */
    MovedPageSearchStatus moved_page_search_status_;

    /**
     * Upto which separator we are done. only for interior.
     * If forward search, we followed a pointer before this separator.
     * If backward search, we followed a pointer after this separator.
     * This is updated whenever we follow a pointer from this interior page,
     * and used when we have to re-find the separator. In Master-tree, a separator never
     * disappears from the page, so we can surely find this.
     */
    KeySlice latest_separator_;

    /** only for border. order_[0] is the index of smallest record, [1] second smallest... */
    SlotIndex order_[kBorderPageMaxSlots];

    SlotIndex get_cur_original_index() const ALWAYS_INLINE {
      return order_[index_];
    }
    SlotIndex get_original_index(SlotIndex index) const ALWAYS_INLINE {
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
    kMaxRecords = kBorderPageMaxSlots,
    kMaxRoutes = kPageSize / sizeof(Route),
    kKeyLengthExtremum = 0,
  };

  MasstreeCursor(MasstreeStorage storage, thread::Thread* context);

  thread::Thread*   get_context() { return context_; }
  MasstreeStorage&  get_storage() { return storage_; }
  bool              is_for_writes() const { return for_writes_; }
  bool              is_forward_cursor() const { return forward_cursor_; }

  ErrorCode   open(
    const char* begin_key = CXX11_NULLPTR,
    KeyLength begin_key_length = kKeyLengthExtremum,
    const char* end_key = CXX11_NULLPTR,
    KeyLength end_key_length = kKeyLengthExtremum,
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
    ASSERT_ND((forward_cursor && end_key >= begin_key) ||
      (!forward_cursor && end_key <= begin_key));
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
    return route_count_ > 0 && !reached_end_ && cur_route()->is_valid_record();
  }
  /** Returns the length of the whole key we are currently pointing to */
  KeyLength   get_key_length() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    return cur_key_length_;
  }
  /**
   * Returns the prefix slices upto the current page.
   * When the current page is in layer-n, first n slices are set.
   */
  const KeySlice* get_cur_route_prefix_slices() const ALWAYS_INLINE {
    return cur_route_prefix_slices_;
  }
  /** Big-endian version */
  const char* get_cur_route_prefix_be() const ALWAYS_INLINE {
    return cur_route_prefix_be_;
  }
  /** Returns only the slice of the key in the layer of the current page. Mostly internal use */
  KeySlice    get_key_in_layer_slice() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    return cur_key_in_layer_slice_;
  }
  /** Returns the suffix part of the key in the page. Mostly internal use */
  const char* get_key_suffix() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    return cur_key_suffix_;
  }
  /** This method assumes the key length is at most 8 bytes. Instead, it's fast and handy. */
  KeySlice    get_normalized_key() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    ASSERT_ND(cur_key_length_ <= sizeof(KeySlice));
    return cur_key_in_layer_slice_;
  };
  /**
   * @brief Copies the entire big-endian key of the current record to the given buffer.
   * @param[out] buffer to receive the combined big-endian key. must be get_key_length() or longer.
   * @details
   * In other words, this combines get_cur_route_prefixes(),
   * get_key_in_layer_slice() and get_key_suffix().
   * This method is handy to get a whole key as one char array, but it must copy something,
   * so remember that it's a bit costly.
   *
   * @par Why we can't just return const char*
   * We initially provided such a method "get_key()". However, to provide such a method, we
   * have to keep memcpy-ing for each key because there is no single contiguous key string!
   * A key in masstree is stored in at least 3 components; prefix slices, current slice, suffix.
   * If we could just point to somewhere in the data page, it's a no-cost operation.
   * What we can do is only get_key_suffix() in that regard.
   * We removed the get_key() method and instead added this explicit copy-method so that the
   * user can choose when to re-construct the entire key.
   */
  void        copy_combined_key(char* buffer) const;
  /** Another version to get a part of the key, from the offset for len bytes. */
  void        copy_combined_key_part(KeyLength offset, KeyLength len, char* buffer) const;
  /** For even handier use, it returns std::string. It's SLOOOW. So use it as such. */
  std::string get_combined_key() const ALWAYS_INLINE {
    char buf[kMaxKeyLength];
    copy_combined_key(buf);
    return std::string(buf, cur_key_length_);
  }

  const char* get_payload() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    return cur_payload_;
  }
  PayloadLength  get_payload_length() const ALWAYS_INLINE {
    ASSERT_ND(is_valid_record());
    return cur_payload_length_;
  }

  /**
   * @brief Moves the cursor to next record.
   * @details
   * When the cursor already reached the end, it does nothing.
   * @par Implementation note
   * This method changes a complete state of the cursor to a next complete status.
   * In other words, this must NOT be called from other internal methods when
   * this object is in interim state (such as should_skip_cur_route_==true).
   * When this method returns, it is guaranteed that should_skip_cur_route_==false for this reason.
   */
  ErrorCode next();


  ErrorCode delete_record();

  ErrorCode overwrite_record(
    const void* payload,
    PayloadLength payload_offset,
    PayloadLength payload_count);
  template <typename PAYLOAD>
  ErrorCode overwrite_record_primitive(
    PAYLOAD payload,
    PayloadLength payload_offset);

  template <typename PAYLOAD>
  ErrorCode increment_record(PAYLOAD* value, PayloadLength payload_offset);

 private:
  Engine* const         engine_;
  MasstreeStorage       storage_;
  thread::Thread* const context_;
  xct::Xct* const current_xct_;

  bool        for_writes_;
  bool        forward_cursor_;
  bool        end_inclusive_;
  bool        reached_end_;

  /** If this value is zero, it means supremum. */
  KeyLength   end_key_length_;
  /** If this value is zero, it means supremum. */
  KeyLength   cur_key_length_;
  PayloadLength cur_payload_length_;
  /** If this value is zero, it means supremum. */
  KeyLength   search_key_length_;
  SearchType  search_type_;
  bool        search_key_in_layer_extremum_;

  /** number of higher layer pages. the current border page is not included. so, might be 0. */
  uint16_t    route_count_;

  /**
   * This is set to true when the cur_route() should be skipped over to find a next valid
   * record. Whenever this is true, is_valid_record() must be false.
   * This becomes true in a few cases.
   * \li the initial locate() didn't find a matching record because it unluckily hit the page
   * boundary.
   * \li locate() or proceed_deep() ran into an empty border page.
   *
   * Receiving this flag, locate() and next() are responsible to invoke next() to resolve the
   * state. next() then sees this flag and moves on to next page/record.
   * When the control is returned to the user code, this flag must be always false.
   */
  bool        should_skip_cur_route_;

  bool        cur_key_next_layer_;
  KeyLength   cur_key_in_layer_remainder_;
  KeySlice    cur_key_in_layer_slice_;
  /**
   * Like in per-record operation, now we mix logical operation into cursors.
   * The cursor logically observes XctId, potentially taking locks or read-sets.
   */
  RecordLocation cur_key_location_;
  // xct::XctId  cur_key_observed_owner_id_;
  xct::RwLockableXctId* cur_key_owner_id_address;

  /** full big-endian key to terminate search. allocated in transaction's local work memory */
  char*       end_key_;
  /** full native-endian key to terminate search. allocated in transaction's local work memory */
  KeySlice*   end_key_slices_;

  /**
   * native-endian slices of the B-trie path up to the current page.
   * When the current page is in layer-n, first n-elements are set.
   * allocated in transaction's local work memory.
   */
  KeySlice*   cur_route_prefix_slices_;
  /** big-endian version. allocated in transaction's local work memory. */
  char*       cur_route_prefix_be_;
  /**
   * big-endian suffix part of the current record's key. this points to somewhere in the current
   * page. Unlike original masstree, the suffix part is immutable. So, it's safe to just
   * point to it rather than copying.
   */
  const char* cur_key_suffix_;

  /** full payload of current record. Directly points to address in current page */
  const char* cur_payload_;

  /** full big-endian key of current search. allocated in transaction's local work memory */
  char*       search_key_;
  /** full native-endian key of current search. allocated in transaction's local work memory */
  KeySlice*   search_key_slices_;

  /** stable version of teh current border page as of copying cur_page_. */
  PageVersionStatus cur_page_stable_;

  /** allocated in transaction's local work memory. */
  Route*      routes_;

  /**
   * This method is the only place we \e increment route_count_ to push an entry to routes_.
   * It takes a stable version of the page and
   */
  ErrorCode push_route(MasstreePage* page);
  /**
   * This is now a logical operation that might add lock/readset.
   * You can't use this method to "peek" cur record. Be careful!
   */
  ErrorCode fetch_cur_record_logical(MasstreeBorderPage* page, SlotIndex record);
  void      check_end_key();
  bool      is_cur_key_next_layer() const { return cur_key_location_.observed_.is_next_layer(); }
  KeyCompareResult compare_cur_key_aginst_search_key(KeySlice slice, uint8_t layer) const;
  KeyCompareResult compare_cur_key_aginst_end_key() const;
  KeyCompareResult compare_cur_key(
    KeySlice slice,
    uint8_t layer,
    const char* full_key,
    KeyLength full_length) const;

  SlotIndex     get_cur_index() const ALWAYS_INLINE {
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

  template <typename T>
  ErrorCode allocate_if_not_exist(T** pointer);

  bool is_search_key_extremum() const ALWAYS_INLINE {
    return search_key_length_ == kKeyLengthExtremum;
  }
  bool is_end_key_supremum() const ALWAYS_INLINE {
    return end_key_length_ == kKeyLengthExtremum;
  }

  ErrorCode follow_foster(KeySlice slice);
  void extract_separators(KeySlice* separator_low, KeySlice* separator_high) const ALWAYS_INLINE;

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
  void      proceed_route_intermediate_rebase_separator();

  MasstreePage* resolve_volatile(VolatilePagePointer ptr) const;

  void assert_modify() const ALWAYS_INLINE {
#ifndef NDEBUG
    ASSERT_ND(!should_skip_cur_route_);
    ASSERT_ND(for_writes_);
    ASSERT_ND(is_valid_record());
    ASSERT_ND(!cur_route()->snapshot_);
    ASSERT_ND(reinterpret_cast<Page*>(get_cur_page())->get_header().get_page_type()
      == kMasstreeBorderPageType);
#endif  // NDEBUG
  }
  void assert_route() const ALWAYS_INLINE {
#ifndef NDEBUG
    assert_route_impl();
#endif  // NDEBUG
  }
  void assert_route_impl() const;
};

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_CURSOR_HPP_

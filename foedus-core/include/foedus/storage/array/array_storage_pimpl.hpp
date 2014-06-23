/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
#include <stdint.h>

#include <string>
#include <vector>

#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/assorted/const_div.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_id.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/fwd.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace array {

/**
 * @brief Pimpl object of ArrayStorage.
 * @ingroup ARRAY
 * @details
 * A private pimpl object for ArrayStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class ArrayStoragePimpl final : public DefaultInitializable {
 public:
  /**
   * Compactly represents the route to reach the given offset.
   * Fanout cannot exceed 256 (as empty-payload is not allowed, minimal entry size is 16 bytes
   * in both leaf and interior, 4096/16=256), uint8_t is enough to represent the route.
   * Also, interior page always has a big fanout close to 256, so 8 levels are more than enough.
   */
  union LookupRoute {
    /** This is a 64bit data. */
    uint64_t word;
    /** [0] means record ordinal in leaf, [1] in its parent page, [2]...*/
    uint8_t route[8];
  };

  ArrayStoragePimpl() = delete;
  ArrayStoragePimpl(Engine* engine, ArrayStorage* holder, const ArrayMetadata &metadata,
            bool create);

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ErrorStack  create(thread::Thread* context);

  // all per-record APIs are called so frequently, so returns ErrorCode rather than ErrorStack
  ErrorCode   locate_record(thread::Thread* context, ArrayOffset offset,
                Record **out) ALWAYS_INLINE;
  ErrorCode   get_record(thread::Thread* context, ArrayOffset offset,
          void *payload, uint16_t payload_offset, uint16_t payload_count) ALWAYS_INLINE;
  template <typename T>
  ErrorCode   get_record_primitive(thread::Thread* context, ArrayOffset offset,
            T *payload, uint16_t payload_offset);
  ErrorCode   overwrite_record(thread::Thread* context, ArrayOffset offset,
      const void *payload, uint16_t payload_offset, uint16_t payload_count) ALWAYS_INLINE;
  template <typename T>
  ErrorCode   overwrite_record_primitive(thread::Thread* context, ArrayOffset offset,
            T payload, uint16_t payload_offset);
  template <typename T>
  ErrorCode   increment_record(thread::Thread* context, ArrayOffset offset,
            T* value, uint16_t payload_offset);

  LookupRoute find_route(ArrayOffset offset) const ALWAYS_INLINE;

  ErrorCode   lookup(thread::Thread* context, ArrayOffset offset,
            ArrayPage** out, uint16_t *index) ALWAYS_INLINE;

  /** Used only from uninitialize() */
  void        release_pages_recursive(
    memory::PageReleaseBatch* batch, ArrayPage* page, VolatilePagePointer volatile_page_id);

  /**
  * Calculate leaf/interior pages we need.
  * @return index=level.
  */
  static std::vector<uint64_t> calculate_required_pages(uint64_t array_size, uint16_t payload);

  Engine* const           engine_;
  ArrayStorage* const     holder_;
  ArrayMetadata           metadata_;

  /**
   * Points to the root page.
   */
  DualPagePointer         root_page_pointer_;

  /**
   * Root page is assured to be never evicted.
   * So, we can access the root_page_ without going through caching module.
   */
  ArrayPage*              root_page_;

  /** Number of levels. */
  uint8_t                 levels_;

  bool                    exist_;

  /** Number of records in leaf page. */
  const uint16_t                records_in_leaf_;
  /** ConstDiv(records_in_leaf_) to speed up integer division in lookup(). */
  const assorted::ConstDiv      leaf_fanout_div_;
  /** ConstDiv(kInteriorFanout) to speed up integer division in lookup(). */
  const assorted::ConstDiv      interior_fanout_div_;
};
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_

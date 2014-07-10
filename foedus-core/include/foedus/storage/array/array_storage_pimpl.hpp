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
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/array/array_id.hpp"
#include "foedus/storage/array/array_metadata.hpp"
#include "foedus/storage/array/array_route.hpp"
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

  bool                    exist_;

  /** Number of levels. */
  const uint8_t           levels_;

  LookupRouteFinder       route_finder_;
};
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_ARRAY_STORAGE_PIMPL_HPP_
                                                                                                                                                              

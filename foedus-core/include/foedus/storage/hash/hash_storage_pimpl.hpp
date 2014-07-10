/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_HASH_HASH_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_HASH_HASH_STORAGE_PIMPL_HPP_
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
#include "foedus/storage/hash/hash_id.hpp"
#include "foedus/storage/hash/hash_metadata.hpp"
#include "foedus/storage/hash/fwd.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace hash {

/**
* @brief Pimpl object of HashStorage.
 * @ingroup HASH
 * @details
 * A private pimpl object for HashStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class HashStoragePimpl final : public DefaultInitializable {
 public:
  HashStoragePimpl() = delete;
  HashStoragePimpl(Engine* engine, HashStorage* holder, const HashMetadata &metadata,
            bool create);

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ErrorStack  create(thread::Thread* context);

  // all per-record APIs are called so frequently, so returns ErrorCode rather than ErrorStack
  ErrorCode   get_record(thread::Thread* context, const void *key, uint16_t key_length,
          void *payload, uint16_t payload_offset, uint16_t payload_count) ALWAYS_INLINE;

  Engine* const           engine_;
  HashStorage* const     holder_;
  HashMetadata           metadata_;

  /**
   * Points to the root page.
   */
  DualPagePointer         root_page_pointer_;

  /**
   * Root page is assured to be never evicted.
   * So, we can access the root_page_ without going through caching module.
   */
  HashPage*              root_page_;

  bool                    exist_;

};
}  // namespace hash
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_HASH_HASH_STORAGE_PIMPL_HPP_

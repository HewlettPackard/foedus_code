/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_PIMPL_HPP_
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
#include "foedus/storage/sequential/fwd.hpp"
#include "foedus/storage/sequential/sequential_id.hpp"
#include "foedus/storage/sequential/sequential_metadata.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace sequential {

/**
 * @brief Pimpl object of SequentialStorage.
 * @ingroup SEQUENTIAL
 * @details
 * A private pimpl object for SequentialStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class SequentialStoragePimpl final : public DefaultInitializable {
 public:
  SequentialStoragePimpl() = delete;
  SequentialStoragePimpl(Engine* engine,
                          SequentialStorage* holder,
                          const SequentialMetadata &metadata,
                          bool create);

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ErrorStack  create(thread::Thread* context);

  ErrorCode   append_record(thread::Thread* context, const void *payload, uint16_t payload_count);

  /** Used only from uninitialize() */
  void        release_pages_recursive(
    memory::PageReleaseBatch* batch, SequentialPage* page, VolatilePagePointer volatile_page_id);

  Engine* const             engine_;
  SequentialStorage* const  holder_;
  SequentialMetadata        metadata_;

  /**
   * Points to snapshot \e head pages.
   * A sequential storage has zero or more head pages that point to the beginning of
   * singly-linked list of snapshot pages; one for each node and each snapshot.
   * In addition to these stable pages, a sequential storage has one volatile purely in-memory
   * append-only list.
   */
  std::vector<DualPagePointer> head_pages_;

  bool                        exist_;
};
}  // namespace sequential
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_SEQUENTIAL_SEQUENTIAL_STORAGE_PIMPL_HPP_

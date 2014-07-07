/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_PIMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_PIMPL_HPP_
#include <stdint.h>

#include <string>
#include <vector>

#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/memory/fwd.hpp"
#include "foedus/storage/fwd.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/storage/masstree/fwd.hpp"
#include "foedus/storage/masstree/masstree_id.hpp"
#include "foedus/storage/masstree/masstree_metadata.hpp"
#include "foedus/thread/fwd.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/**
 * @brief Pimpl object of MasstreeStorage.
 * @ingroup MASSTREE
 * @details
 * A private pimpl object for MasstreeStorage.
 * Do not include this header from a client program unless you know what you are doing.
 */
class MasstreeStoragePimpl final : public DefaultInitializable {
 public:
  MasstreeStoragePimpl() = delete;
  MasstreeStoragePimpl(Engine* engine,
                      MasstreeStorage* holder,
                      const MasstreeMetadata &metadata,
                      bool create);

  ErrorStack  initialize_once() override;
  ErrorStack  uninitialize_once() override;

  ErrorStack  create(thread::Thread* context);

  Engine* const           engine_;
  MasstreeStorage* const  holder_;
  MasstreeMetadata        metadata_;

  /** If this is true, initialize() reads it back from previous snapshot and logs. */
  bool                      exist_;
};
}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_STORAGE_PIMPL_HPP_

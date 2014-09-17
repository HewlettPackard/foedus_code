/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_STORAGE_LOG_TYPES_HPP_
#define FOEDUS_STORAGE_STORAGE_LOG_TYPES_HPP_
#include <stdint.h>

#include <iosfwd>

#include "foedus/log/common_log_types.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/fwd.hpp"

/**
 * @file foedus/storage/storage_log_types.hpp
 * @brief Declares common log types for all (or at least multiple) storage types.
 * @ingroup STORAGE
 */
namespace foedus {
namespace storage {
/**
 * @brief Log type of DROP STORAGE operation.
 * @ingroup STORAGE LOGTYPE
 * @details
 * This log corresponds to StorageManager::drop_storage() opereation.
 * DROP STORAGE has no in-epoch transaction order.
 * It is always processed in a separate epoch from operations for the storage.
 * Thus, we advance epoch before and after dropping a storage.
 */
struct DropLogType : public log::StorageLogType {
  LOG_TYPE_NO_CONSTRUCT(DropLogType)

  void populate(StorageId storage_id);
  void apply_storage(thread::Thread* context, StorageId storage_id);
  void assert_valid();
  friend std::ostream& operator<<(std::ostream& o, const DropLogType& v);
};

}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_STORAGE_LOG_TYPES_HPP_

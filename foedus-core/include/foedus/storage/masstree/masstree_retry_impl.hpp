/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_MASSTREE_RETRY_IMPL_HPP_
#define FOEDUS_STORAGE_MASSTREE_MASSTREE_RETRY_IMPL_HPP_

#include <stdint.h>

#include "foedus/error_code.hpp"

namespace foedus {
namespace storage {
namespace masstree {

/**
 * @brief Retry logic used in masstree.
 * @ingroup MASSTREE
 * @details
 * @attention handler must be idempotent because this method retries.
 */
template <typename HANDLER>
inline ErrorCode masstree_retry(HANDLER handler) {
  const uint32_t kMaxRetries = 1000;
  for (uint32_t tries = 0; tries < kMaxRetries; ++tries) {
    ErrorCode code = handler();
    if (code == kErrorCodeStrMasstreeRetry) {
      continue;
    }
    return code;
  }
  return kErrorCodeStrMasstreeTooManyRetries;
}

}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_RETRY_IMPL_HPP_

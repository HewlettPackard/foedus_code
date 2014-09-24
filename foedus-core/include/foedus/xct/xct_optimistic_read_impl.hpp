/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_OPTIMISTIC_READ_IMPL_HPP_
#define FOEDUS_XCT_XCT_OPTIMISTIC_READ_IMPL_HPP_

#include "foedus/error_stack.hpp"
#include "foedus/assorted/atomic_fences.hpp"
#include "foedus/storage/storage_id.hpp"
#include "foedus/xct/xct.hpp"
#include "foedus/xct/xct_access.hpp"

namespace foedus {
namespace xct {

/**
 * @brief implements the optimistic read protocol with version checking and appropriate fences.
 * @ingroup XCT
 * @details
 * This is a generic optimistic read logic that receives the actual reading as lambda parameter.
 * @attention handler must be idempotent because this method retries.
 * See MasstreeStoragePimpl::increment_general() for notes on common mistake.
 */
template <typename HANDLER>
inline ErrorCode optimistic_read_protocol(
  Xct* xct,
  storage::StorageId storage_id,
  LockableXctId* owner_id_address,
  bool in_snapshot,
  HANDLER handler) {
  if (in_snapshot ||
    xct->get_isolation_level() == kDirtyReadPreferSnapshot ||
    xct->get_isolation_level() == kDirtyReadPreferVolatile) {
    CHECK_ERROR_CODE(handler(owner_id_address->xct_id_));
    return kErrorCodeOk;
  }

  XctId observed(owner_id_address->xct_id_);
  assorted::memory_fence_consume();
  CHECK_ERROR_CODE(handler(observed));

  // The Masstree paper additionally has another fence and version-check and then a retry if the
  // version differs. However, in our protocol such thing is anyway caught in commit phase.
  // Thus, we have only one fence here.
  return xct->add_to_read_set(storage_id, observed, owner_id_address);
}

}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_OPTIMISTIC_READ_IMPL_HPP_

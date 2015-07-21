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
/*
template <typename HANDLER>
inline ErrorCode masstree_retry(HANDLER handler, uint32_t max_retries = 1000) {
  for (uint32_t tries = 0; tries < max_retries; ++tries) {
    ErrorCode code = handler();
    if (code == kErrorCodeStrMasstreeRetry) {
      continue;
    }
    return code;
  }
  return kErrorCodeStrMasstreeTooManyRetries;
}
Not used any more. No global retry required for Master-tree unlike Mass-tree
*/
}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_MASSTREE_RETRY_IMPL_HPP_

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
#ifndef FOEDUS_LOG_LOG_TYPE_INVOKE_HPP_
#define FOEDUS_LOG_LOG_TYPE_INVOKE_HPP_
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/thread/fwd.hpp"
#include "foedus/xct/xct_id.hpp"

// include all header files that declare log types defined in the xmacro.
// unlike log_type.hpp, we need full declarations here. so this file would be big.
#include "foedus/log/common_log_types.hpp"  // NOLINT(build/include_alpha)
#include "foedus/storage/storage_log_types.hpp"
#include "foedus/storage/array/array_log_types.hpp"
#include "foedus/storage/hash/hash_log_types.hpp"
#include "foedus/storage/masstree/masstree_log_types.hpp"
#include "foedus/storage/sequential/sequential_log_types.hpp"

namespace foedus {
namespace log {

/**
 * @brief Invokes the apply logic for an engine-wide log type.
 * @ingroup LOGTYPE
 * @details
 * This is not inlined because this log kind appears much more infrequently.
 */
void invoke_apply_engine(void *log_buffer, thread::Thread* context);

/**
 * @brief Invokes the apply logic for a storage-wide log type.
 * @ingroup LOGTYPE
 * @details
 * This is not inlined because this log kind appears much more infrequently.
 */
void invoke_apply_storage(void *log_buffer, Engine* engine, storage::StorageId id);

/**
 * @brief Invokes the apply logic for a record-wise log type.
 * @ingroup LOGTYPE
 * @details
 * This is inlined because this is invoked for every single record type log.
 */
void invoke_apply_record(
  void *log_buffer,
  thread::Thread* context,
  storage::StorageId storage_id,
  xct::RwLockableXctId* owner_id_address,
  char* payload_address);

/**
 * @brief Invokes the assertion logic of each log type.
 * @ingroup LOGTYPE
 * @details
 * This is inlined because this is invoked for every single record type log.
 * In non-debug mode, this is anyway empty.
 */
void invoke_assert_valid(void *log_buffer);

/**
 * @brief Invokes the ostream operator for the given log type defined in log_type.xmacro.
 * @ingroup LOGTYPE
 * @details
 * This is only for debugging and analysis use, so does not have to be optimized.
 * Thus not inlined here.
 * This writes out an XML representation of the log entry.
 */
void invoke_ostream(const void *buffer, std::ostream *ptr);

#define X(a, b, c) case a: \
  reinterpret_cast< c* >(buffer)->apply_record(context, storage_id, owner_id, payload); return;
inline void invoke_apply_record(
  void *buffer,
  thread::Thread* context,
  storage::StorageId storage_id,
  xct::RwLockableXctId* owner_id,
  char* payload) {
  invoke_assert_valid(buffer);
  LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
  LogCode code = header->get_type();
  switch (code) {
#include "foedus/log/log_type.xmacro" // NOLINT
    default:
      ASSERT_ND(false);
      return;
  }
}
#undef X

#ifdef NDEBUG
inline void invoke_assert_valid(void* /*buffer*/) {}
#else  // NDEBUG
#define X(a, b, c) case a: reinterpret_cast< c* >(buffer)->assert_valid(); return;
inline void invoke_assert_valid(void *buffer) {
  LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
  LogCode code = header->get_type();
  switch (code) {
#include "foedus/log/log_type.xmacro" // NOLINT
    default:
      ASSERT_ND(false);
      return;
  }
}
#undef X
#endif  // NDEBUG

}  // namespace log
}  // namespace foedus

#endif  // FOEDUS_LOG_LOG_TYPE_INVOKE_HPP_

/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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

namespace foedus {
namespace log {

/**
 * @brief Invokes the apply logic for an engine-wide log type.
 * @ingroup LOGTYPE
 * @details
 * This is not inlined because this log kind appears much more infrequently.
 */
void invoke_apply_engine(const xct::XctId &xct_id, void *log_buffer, thread::Thread* context);

/**
 * @brief Invokes the apply logic for a storage-wide log type.
 * @ingroup LOGTYPE
 * @details
 * This is not inlined because this log kind appears much more infrequently.
 */
void invoke_apply_storage(const xct::XctId &xct_id, void *log_buffer,
              thread::Thread* context, storage::Storage* storage);

/**
 * @brief Invokes the apply logic for a record-wise log type.
 * @ingroup LOGTYPE
 * @details
 * This is inlined because this is invoked for every single record type log.
 * This also unlocks the record by overwriting the record's owner_id.
 * @attention When the individual implementation of apply_record does it, it must be careful on
 * the memory order of unlock and data write. It must write data first, then unlock.
 * Otherwise the correctness is not guaranteed.
 * For that, apply_record() method must put memory_fence_release() between data and owner_id writes.
 */
void invoke_apply_record(const xct::XctId &xct_id, void *log_buffer,
          thread::Thread* context, storage::Storage* storage, storage::Record* record);

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
void invoke_ostream(void *buffer, std::ostream *ptr);

#define X(a, b, c) case a: \
  reinterpret_cast< c* >(buffer)->apply_record(xct_id, context, storage, record); return;
inline void invoke_apply_record(const xct::XctId &xct_id, void *buffer,
          thread::Thread* context, storage::Storage* storage, storage::Record* record) {
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

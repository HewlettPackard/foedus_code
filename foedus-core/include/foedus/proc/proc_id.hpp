/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_PROC_PROC_ID_HPP_
#define FOEDUS_PROC_PROC_ID_HPP_

#include <stdint.h>

#include "foedus/error_stack.hpp"
#include "foedus/assorted/fixed_string.hpp"
#include "foedus/thread/fwd.hpp"

/**
 * @file foedus/proc/proc_id.hpp
 * @brief Typedefs of ID types used in procedure package.
 * @ingroup PROC
 */
namespace foedus {
namespace proc {

/**
 * Represents a unique name of a procedure. Upto 60 characters so far.
 * It should be a globally unique name, but we do not bother checking it to avoid scalability issue.
 * Usually, all SOCs have the same set of procedures, so it's globally unique, too.
 */
typedef assorted::FixedString<60> ProcName;

/**
 * @brief Represents a locally-unique ID of a procedure in one SOC.
 * @ingroup PROC
 * @details
 * The same procedure might have different ID in another SOC.
 * If the procedure is a system procedure, it always has the same ID in all SOCs because
 * system procedures are registered at the beginning. User procedures have larger IDs than all
 * system procedures.
 * Zero means an invalid procedure.
 */
typedef uint32_t LocalProcId;

/**
 * A globally unique ID of a procedure.
 * The high 32 bit is the SOC ID, low 32 bit is LocalProcId.
 */
typedef uint64_t GlobalProcId;

inline GlobalProcId combined_global_proc_id(uint16_t node, LocalProcId local_id) {
  return static_cast<GlobalProcId>(node) << 32 | local_id;
}
inline uint16_t extract_numa_node_from_global_proc_id(GlobalProcId id) {
  return static_cast<uint16_t>(id >> 32);
}
inline LocalProcId extract_local_id_from_global_proc_id(GlobalProcId id) {
  return static_cast<LocalProcId>(id);
}

/**
 * @brief A function pointer of a user/system stored procedure.
 * @ingroup PROC
 * @details
 * For example, define your procedure as follows:
 * @code{.cpp}
 * ErrorStack my_proc(
 *   thread::Thread* context,
 *   const void *input_buffer,
 *   uint32_t input_len,
 *   void* output_buffer,
 *   uint32_t output_len) {
 *   ...
 *   return kRetOk;
 * }
 *
 * Proc the_proc = &my_proc;
 * register the_proc and execute it...
 * @endcode
 */
typedef ErrorStack (*Proc)(thread::Thread*, const void*, uint32_t, void*, uint32_t);

}  // namespace proc
}  // namespace foedus
#endif  // FOEDUS_PROC_PROC_ID_HPP_

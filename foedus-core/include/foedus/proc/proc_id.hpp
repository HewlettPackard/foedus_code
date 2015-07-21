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
#ifndef FOEDUS_PROC_PROC_ID_HPP_
#define FOEDUS_PROC_PROC_ID_HPP_

#include <stdint.h>

#include <utility>

#include "foedus/error_stack.hpp"
#include "foedus/fwd.hpp"
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
 * @ingroup PROC
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
 * 2^32-1 means an invalid procedure.
 */
typedef uint32_t LocalProcId;

const LocalProcId kLocalProcInvalid = -1;

/**
 * A globally unique ID of a procedure.
 * The high 32 bit is the SOC ID, low 32 bit is LocalProcId.
 * @ingroup PROC
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
 * @brief Set of arguments, both inputs and outputs, given to each procedure.
 * @ingroup PROC
 */
struct ProcArguments {
  /** [IN] Database Engine */
  Engine*         engine_;
  /** [IN] Thread on which the procedure is running */
  thread::Thread* context_;
  /** [IN] Arbitrary user input given to the procedure */
  const void*     input_buffer_;
  /** [IN] Byte length of input_buffer_ */
  uint32_t        input_len_;
  /** [OUT] Arbitrary user output buffer given to the procedure */
  void*           output_buffer_;
  /** [IN] Byte length of output_buffer_ capacity */
  uint32_t        output_buffer_size_;
  /** [OUT] Byte length of output_buffer_ actually written */
  uint32_t*       output_used_;
};

/**
 * @brief A function pointer of a user/system stored procedure.
 * @ingroup PROC
 * @details
 * For example, define your procedure as follows:
 * @code{.cpp}
 * ErrorStack my_proc(const ProcArguments& args) {
 *   ...
 *   return kRetOk;
 * }
 *
 * Proc the_proc = &my_proc;
 * register the_proc and execute it...
 * @endcode
 */
typedef ErrorStack (*Proc)(const ProcArguments& args);

/**
 * Just a std::pair<ProcName, Proc>.
 * @ingroup PROC
 */
typedef std::pair<ProcName, Proc> ProcAndName;

}  // namespace proc
}  // namespace foedus
#endif  // FOEDUS_PROC_PROC_ID_HPP_

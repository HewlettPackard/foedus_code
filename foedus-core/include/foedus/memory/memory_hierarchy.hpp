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
#ifndef FOEDUS_MEMORY_MEMORY_HIERARCHY_HPP_
#define FOEDUS_MEMORY_MEMORY_HIERARCHY_HPP_

/**
 * @defgroup MEMHIERARCHY Memory Hierarchy
 * @brief NUMA-Aware \b Memory \b Hierarchy in libfoedus-core
 * @ingroup MEMORY
 * @details
 * @par Overview
 * We have a memory hierarchy of three levels in libfoedus, resembling the memory hiearchy in
 * NUMA architecture:
 *   \li Memories shared engine-wide (EngineMemory)
 *   \li Memories shared NUMA-Node-wide (NumaNodeMemory)
 *   \li Private Memories in each core (NumaCoreMemory)
 *
 * The resemblance is intentinal to achieve the best performance of memory
 * allocation/deallocation/sharing in NUMA setting.
 *
 * @par Absolutely no global nor truly TLS variables
 * You might notice that the top level of the hierarchy is \e engine-wide, not \e global.
 * libfoedus uses absolutely no global nor static variables except const primitive types.
 * This simplifies the design of memory management in the library and allows
 * running multiple instances (engines) of our library even in one process.
 * What we have in Engine object is everything. When Engine's uninitialize() is invoked,
 * everything the Engine acquired is released, separately from other Engine's.
 * This also means that memory-leak checkers like valgrind can easily check for potential errors.
 *
 * @note Unfortunately, there is one exception to this rule; google-logging.
 * google::InitGoogleLogging() and ShutdownGoogleLogging() must be called exactly once in the
 * process. So, we do them in Engine's initialize()/uninitialize() with a process-global atomic
 * counter to make sure we call them only once.
 *
 * @par Memories shared engine-wide (EngineMemory)
 * @copydetails foedus::memory::EngineMemory
 *
 * @par Memories shared NUMA-Node-wide (NumaNodeMemory)
 * @copydetails foedus::memory::NumaNodeMemory
 *
 * @par Private Memories in each core (NumaCoreMemory)
 * @copydetails foedus::memory::NumaCoreMemory
 *
 */

#endif  // FOEDUS_MEMORY_MEMORY_HIERARCHY_HPP_

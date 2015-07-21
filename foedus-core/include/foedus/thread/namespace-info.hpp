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
#ifndef FOEDUS_THREAD_NAMESPACE_INFO_HPP_
#define FOEDUS_THREAD_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::thread
 * @brief \b Thread and \b Thread-Group, which abstracts NUMA-core/node and provides API to
 * attach/detach tasks to pooled threads.
 * @details
 * This package is analogous to std::thread or boost::thread with a few additional features
 * specific to our database engine. Among all, the main difference is that this package is
 * fully aware of NUMA (Non Uniform Memory Access) architecture in hardware.
 *
 * @par Worker Thread vs NUMA-Core
 * In our library, each worker thread (a thread that runs user/system transactions) runs on
 * exactly one NUMA core (using ::numa_run_on_node()), and each NUMA core runs at most one
 * worker thread (except when the user runs multiple engines).
 * Hence, we use the words "thread" and "NUMA-core" interchangablly.
 * The one-to-one mapping is to save thread-context switches and processor cache misses.
 *
 * @par Thread and ThreadGroup: Thread Memory Hierarchy
 * NUMA cores have a hierarchy in hardware, which is explained in \ref MEMHIERARCHY.
 * Our thread representations reflect the hierarchy; Thread=NUMA Core, and ThreadGroup=NUMA Node.
 * Unfortunately, the terminology "thread group" has a different meaning in some other context,
 * but I couldn't come with a better name.
 *
 * @par Thread Pool and Thread Impersonation
 * We pre-allocate worker threads in the engine and user-programs \e impersonate one of the
 * threads to run transactions. For more details, see \ref THREADPOOL.
 *
 * @par C++11 and public headers
 * We do \b NOT allow C++11 features in public headers; see \ref CXX11.
 * However, C++11's std::thread and related classes are the only cross-platform library
 * that has no dependencies (boost::thread is NOT header-only).
 * Hence, we use C++11's std::thread but encapsulates the details in implementations (cpp)
 * and non-public header files (-impl.hpp).
 */

/**
 * @defgroup THREAD Thread and Thread-Group
 * @ingroup COMPONENTS
 * @copydoc foedus::thread
 */

#endif  // FOEDUS_THREAD_NAMESPACE_INFO_HPP_

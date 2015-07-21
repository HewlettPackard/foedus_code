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
#ifndef FOEDUS_LOG_NAMESPACE_INFO_HPP_
#define FOEDUS_LOG_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::log
 * @brief \b Log \b Manager, which writes out transactional logs.
 * @details
 * This package contains classes that control transactional logging.
 *
 * @par Decentralized Logging
 * Unlike traditional log manager in DBMS, this log manager is \e decentralized, meaning
 * each log writer writes to its own file concurrently. This eliminates the bottleneck in
 * log manager when there are a large number of cores.
 * The basic idea to guarantee serializability is the epoch-based commit protocol, which does the
 * check on all loggers before returning the results to client in a way similar to group-commit.
 *
 * @par Thread-private log buffer
 * Each ThreadLoadBugger instance maintains a thread-local log buffer that is filled by
 * the thread without any synchronization or blocking. The logger collects them and writes
 * them out to log files. A single log writer handles one or more transactional threads (cores),
 * and a single NUMA node hosts one or more log writers.
 * @see foedus::log::ThreadLogBuffer
 *
 * @par Log Writer
 * Each Logger instance writes out files suffixed with ordinal (eg ".0", ".1"...).
 * The older logs files are deactivated and deleted after log gleaner consumes them.
 * @see foedus::log::Logger
 *
 * @par Global Durable Epoch
 * An important job of Log Manager is to maintain the \e global durable epoch.
 * Log manager is the module to determine when it's safe to advance the global durable epoch.
 * It makes sure all loggers flushed their logs up to the epoch, invoked required fsync(),
 * and also takes a savepoint before it announces the new global durable epoch.
 * @see foedus::log::LogManager
 */

/**
 * @defgroup LOG Log Manager
 * @ingroup COMPONENTS
 * @copydoc foedus::log
 */

#endif  // FOEDUS_LOG_NAMESPACE_INFO_HPP_

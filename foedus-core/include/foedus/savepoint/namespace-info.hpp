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
#ifndef FOEDUS_SAVEPOINT_NAMESPACE_INFO_HPP_
#define FOEDUS_SAVEPOINT_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::savepoint
 * @brief \b Savepoint \b Manager, which durably and atomically remembers what happened in the
 * engine occasionally (eg. every group commit).
 * @details
 * @par Overview
 * In nutshell, this package durably and atomically (*) writes out a tiny file (\b savepoint file)
 * whenever we advance the global epoch, replacing the savepoint file durably and atomically.
 * The savepoint file is an XML file that describes the progress of individual loggers,
 * log gleaners, and whatever we have to take note for durability and serializablity.
 * These are very compact data, essentially only one or two integers per modules/threads.
 * Thus, the dominating cost is fsync(), which is not cheap but we are okay to pay assuming
 * epoch-based commit and reasonably fast media (SSD/NVRAM).
 *
 * @par Why XML
 * As writing savepoint files happens only occasionally, file format doesn't matter for performance.
 * Thus, we use XML for better debuggability and flexibility. We already use tinyxml for config
 * files, so why not.
 *
 * @par Terminology
 * We named this package \e savepoint rather than \e checkpoint.
 * Checkpoint has a \e slightly different meaning in DBMS, so we avoid that.
 * Savepoint also has a different meaning in DBMS, but it's \e very different, so no confusion.
 *
 * @see foedus::fs::durable_atomic_rename()
 */

/**
 * @defgroup SAVEPOINT Savepoint Manager
 * @ingroup COMPONENTS
 * @copydoc foedus::savepoint
 */

#endif  // FOEDUS_SAVEPOINT_NAMESPACE_INFO_HPP_

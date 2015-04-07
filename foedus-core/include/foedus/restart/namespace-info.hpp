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
#ifndef FOEDUS_RESTART_NAMESPACE_INFO_HPP_
#define FOEDUS_RESTART_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::restart
 * @brief \b Restart \b Manager, which recovers the state of database by re-playing transaction
 * logs at start-up.
 * @details
 * When the engine starts up, it invokes the restart manager at the end of initialization
 * (after all other modules are initialized). Restart manager then replays all transaction logs
 * since the previous complete snapshotting to recover the statue of database.
 * Restart manager works only at restart, and does nothing after initialization nor during
 * uninitialization.
 */

/**
 * @defgroup RESTART Restart Manager
 * @ingroup COMPONENTS
 * @copydoc foedus::restart
 */

#endif  // FOEDUS_RESTART_NAMESPACE_INFO_HPP_

/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
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

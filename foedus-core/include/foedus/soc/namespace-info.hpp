/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SOC_NAMESPACE_INFO_HPP_
#define FOEDUS_SOC_NAMESPACE_INFO_HPP_

/**
 * @namespace foedus::soc
 * @brief System On Chip (\b SOC) and interprocess communication (\b IPC).
 * @details
 * This module is special in many ways.
 * SOC might be spawned as a local or remote process, so we first instantiate SOCs
 * before everything else with a special manner (partially because linux's tricky process
 * handling semantics).
 * @see foedus::EngineType
 */

/**
 * @defgroup SOC SOC and IPC
 * @ingroup COMPONENTS
 * @copydoc foedus::soc
 */

#endif  // FOEDUS_SOC_NAMESPACE_INFO_HPP_

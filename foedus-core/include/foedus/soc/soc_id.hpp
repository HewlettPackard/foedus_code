/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SOC_SOC_ID_HPP_
#define FOEDUS_SOC_SOC_ID_HPP_

#include <stdint.h>

/**
 * @file foedus/soc/soc_id.hpp
 * @brief Typedefs of ID types used in SOC package.
 * @ingroup SOC
 */
namespace foedus {
namespace soc {

/**
 * Maximum number of SOCs
 * @ingroup SOC
 */
const uint16_t kMaxSocs = 256U;

/**
 * Represents an ID of an SOC, or NUMA node
 * @ingroup SOC
 */
typedef uint16_t SocId;

/**
 * Universal (or Unique) ID of a process.
 * This is so far what getpid() returns, but might be something else when we support remote nodes.
 * @ingroup SOC
 */
typedef uint64_t Upid;

}  // namespace soc
}  // namespace foedus
#endif  // FOEDUS_SOC_SOC_ID_HPP_

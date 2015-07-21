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

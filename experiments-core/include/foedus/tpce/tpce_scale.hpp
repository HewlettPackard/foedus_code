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
#ifndef FOEDUS_TPCE_TPCE_SCALE_HPP_
#define FOEDUS_TPCE_TPCE_SCALE_HPP_

#include <stdint.h>

/**
 * @file tpce_scale.hpp
 * @brief Scale factors for TPC-C benchmark.
 * @details
 * These should be a program parameter, but I haven't seen anyone varying any of them
 * except kWarehouses and kNewOrderRemotePercent.
 */
namespace foedus {
namespace tpce {

/** Number of warehouses. Does not grow dynamically */
// const uint16_t kWarehouses = 16U;
// kWarehouses is now a program parameter. See tpce_driver
/** Maximum number of warehouses. */
const uint16_t kMaxWarehouses = (1U << 10);

/** Number of items per warehouse. Does not grow dynamically  */
const uint32_t kItems = 100000U;

/** Number of districts per warehouse. Does not grow dynamically  */
const uint8_t kDistricts = 10U;

/** Number of customers per district. Does not grow dynamically  */
const uint32_t kCustomers = 3000U;

/** Number of orders per district. Does grow dynamically. */
const uint32_t kOrders = 3000U;

#ifndef OLAP_MODE  // see cmake script for tpce_olap
/** Max number of orders per district */
const uint32_t kMaxOrders = 1U << 31;
const uint16_t kMaxOlCount = 15U;
#else  // OLAP_MODE
// in OLAP mode, many more orderlines per order.
// tested with at least 1500U. But, h-store doesn't handle that many due to OOPS 32GB limit,
// so we might use smaller setting for comparison.
const uint32_t kMaxOrders = 1U << 20;
const uint16_t kMaxOlCount = 500U;
#endif  // OLAP_MODE

/** Number of variations of last names. Does not grow dynamically. */
const uint32_t kLnames = 1000U;

const uint8_t kMinOlCount = 5U;
const uint16_t kOlMax = kMaxOlCount + 1U;

#ifndef OLAP_MODE  // see cmake script for tpce_olap
// See Sec 5.2.2 of the TPCE spec
const uint8_t kXctNewOrderPercent = 45U;
const uint8_t kXctPaymentPercent = 43U + kXctNewOrderPercent;
const uint8_t kXctOrderStatusPercent = 4U + kXctPaymentPercent;
const uint8_t kXctDelieveryPercent = 4U + kXctOrderStatusPercent;
#else  // OLAP_MODE
const uint8_t kXctNewOrderPercent = 0U;
const uint8_t kXctPaymentPercent = 0U + kXctNewOrderPercent;
// const uint8_t kXctOrderStatusPercent = 50U + kXctPaymentPercent;
const uint8_t kXctOrderStatusPercent = 100U + kXctPaymentPercent;
const uint8_t kXctDelieveryPercent = 0U + kXctOrderStatusPercent;
#endif  // OLAP_MODE
// remainings are stock-level xct.

/**
 * How much of supplier in neworder transaction uses remote warehouse?
 * Note that one neworder xct touches on average 10 suppliers, so in total
 * a bit less than 10% of neworder accesses remote warehouse(s).
 */
// const uint8_t kNewOrderRemotePercent = 1U;  this is now a program parameter

/** How much of payment transaction uses remote warehouse/district? */
// const uint8_t kPaymentRemotePercent = 15U;  this is now a program parameter

}  // namespace tpce
}  // namespace foedus

#endif  // FOEDUS_TPCE_TPCE_SCALE_HPP_

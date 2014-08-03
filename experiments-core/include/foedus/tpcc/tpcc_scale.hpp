/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_TPCC_TPCC_SCALE_HPP_
#define FOEDUS_TPCC_TPCC_SCALE_HPP_

#include <stdint.h>

/**
 * @file tpcc_scale.hpp
 * @brief Scale factors for TPC-C benchmark.
 * @details
 * These should be a program parameter, but I haven't seen anyone varying any of them
 * except kWarehouses and kNewOrderRemotePercent.
 */
namespace foedus {
namespace tpcc {

/** Number of warehouses. Does not grow dynamically */
const uint16_t kWarehouses = 2U;

/** Number of items per warehouse. Does not grow dynamically  */
const uint32_t kItems = 100000U;

/** Number of districts per warehouse. Does not grow dynamically  */
const uint8_t kDistricts = 10U;

/** Number of customers per district. Does not grow dynamically  */
const uint32_t kCustomers = 3000U;

/** Number of orders per district. Does grow dynamically. */
const uint32_t kOrders = 3000U;
/** Max number of orders per district */
const uint32_t kMaxOrders = 1U << 31;

/** Number of variations of last names. Does not grow dynamically. */
const uint32_t kLnames = 1000U;

const uint8_t kMinOlCount = 5U;
const uint8_t kMaxOlCount = 15U;
const uint8_t kOlMax = kMaxOlCount + 1U;

// See Sec 5.2.2 of the TPCC spec
const uint8_t kXctNewOrderPercent = 45U;
const uint8_t kXctPaymentPercent = 43U + kXctNewOrderPercent;
const uint8_t kXctOrderStatusPercent = 4U + kXctPaymentPercent;
const uint8_t kXctDelieveryPercent = 4U + kXctOrderStatusPercent;
// remainings are stock-level xct.

/**
 * How much of supplier in neworder transaction uses remote warehouse?
 * Note that one neworder xct touches on average 10 suppliers, so in total
 * a bit less than 10% of neworder accesses remote warehouse(s).
 */
const uint8_t kNewOrderRemotePercent = 1U;

/** How much of payment transaction uses remote warehouse/district? */
const uint8_t kPaymentRemotePercent = 15U;

}  // namespace tpcc
}  // namespace foedus

#endif  // FOEDUS_TPCC_TPCC_SCALE_HPP_

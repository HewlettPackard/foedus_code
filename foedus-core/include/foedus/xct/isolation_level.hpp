/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_ISOLATION_LEVEL_HPP_
#define FOEDUS_XCT_ISOLATION_LEVEL_HPP_
/**
 * @defgroup ISOLATION Isolation Levels
 * @brief \b Isolation \b Levels for Transactions
 * @ingroup XCT
 * @details
 * bluh
 */

namespace foedus {
namespace xct {
/**
 * @brief Specifies the level of isolation during transaction processing.
 * @ingroup ISOLATION
 */
enum IsolationLevel {
    /**
     * Snapshot isolation, meaning the transaction might see or be based on stale snapshot.
     * Optionally, the client can specify which snapshot we should be based on.
     */
    SNAPSHOT,

    /**
     * Protects against all anomalies in all situations.
     * This is the most expensive level, but everything good has a price.
     */
    SERIALIZABLE,
};
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_ISOLATION_LEVEL_HPP_

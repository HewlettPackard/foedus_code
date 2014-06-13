/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_OPTIONS_HPP_
#define FOEDUS_XCT_XCT_OPTIONS_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/externalize/externalizable.hpp>
#include <stdint.h>
#include <iosfwd>
namespace foedus {
namespace xct {
/**
 * @brief Set of options for xct manager.
 * @ingroup XCT
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct XctOptions CXX11_FINAL : public virtual externalize::Externalizable {
    /** Constant values. */
    enum Constants {
        /** Default value for max_read_set_size_. */
        kDefaultMaxReadSetSize = 1 << 16,
        /** Default value for max_write_set_size_. */
        kDefaultMaxWriteSetSize = 1 << 14,
        /** Default value for epoch_advance_interval_ms_. */
        kDefaultEpochAdvanceIntervalMs = 20,
    };

    /**
     * Constructs option values with default values.
     */
    XctOptions();

    EXTERNALIZABLE(XctOptions);

    /**
     * @brief The maximum number of read-set one transaction can have.
     * @details
     * Default is 64K records.
     * We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.
     */
    uint32_t    max_read_set_size_;

    /**
     * @brief The maximum number of read-set one transaction can have.
     * @details
     * Default is 16K records.
     * We pre-allocate this much memory for each NumaCoreMemory. So, don't make it too large.
     */
    uint32_t    max_write_set_size_;

    /**
     * @brief Intervals in milliseconds between epoch advancements.
     * @details
     * Default is 20 ms.
     * Too frequent epoch advancement might become bottleneck because we \b synchronously write
     * out savepoint file for each non-empty epoch. However, too infrequent epoch advancement
     * would increase the latency of queries because transactions are not deemed as commit
     * until the epoch advances.
     */
    uint32_t    epoch_advance_interval_ms_;
};
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_OPTIONS_HPP_

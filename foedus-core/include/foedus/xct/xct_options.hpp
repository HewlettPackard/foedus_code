/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_XCT_OPTIONS_HPP_
#define FOEDUS_XCT_XCT_OPTIONS_HPP_
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
struct XctOptions {
    /** Constant values. */
    enum Constants {
        /** Default value for max_read_set_size_. */
        DEFAULT_MAX_READ_SET_SIZE = 1 << 16,
        /** Default value for max_write_set_size_. */
        DEFAULT_MAX_WRITE_SET_SIZE = 1 << 14,
    };

    /**
     * Constructs option values with default values.
     */
    XctOptions();

    friend std::ostream& operator<<(std::ostream& o, const XctOptions& v);

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
};
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_XCT_OPTIONS_HPP_

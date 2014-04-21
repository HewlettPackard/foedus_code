/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SAVEPOINT_SAVEPOINT_OPTIONS_HPP_
#define FOEDUS_SAVEPOINT_SAVEPOINT_OPTIONS_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/externalize/externalizable.hpp>
#include <iosfwd>
#include <string>
namespace foedus {
namespace savepoint {
/**
 * @brief Set of options for savepoint manager.
 * @ingroup SAVEPOINT
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct SavepointOptions CXX11_FINAL : public virtual externalize::Externalizable {
    /**
     * Constructs option values with default values.
     */
    SavepointOptions();

    /**
     * @brief Full path of the savepoint file.
     * @details
     * This file is atomically and durably updated for each epoch-based commit.
     * Default is "savepoint.xml".
     */
    std::string savepoint_path_;

    ErrorStack load(tinyxml2::XMLElement* element) CXX11_OVERRIDE;
    ErrorStack save(tinyxml2::XMLElement* element) const CXX11_OVERRIDE;
    friend std::ostream& operator<<(std::ostream& o, const SavepointOptions& v) {
        v.save_to_stream(&o);
        return o;
    }
};
}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_OPTIONS_HPP_

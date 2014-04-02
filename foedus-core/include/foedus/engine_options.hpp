/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_OPTIONS_HPP_
#define FOEDUS_ENGINE_OPTIONS_HPP_

#include <ostream>
namespace foedus {
/**
 * @brief Set of option values given to the engine at start-up.
 * @ingroup ENGINE
 * @details
 * This object is a collection of engine-wide settings and individual module-wide settings.
 */
struct EngineOptions {
    /**
     * Constructs option values with default values.
     */
    EngineOptions();
    EngineOptions(const EngineOptions& other);
    EngineOptions* operator=(const EngineOptions& other);
    ~EngineOptions();
};
}  // namespace foedus
std::ostream& operator<<(std::ostream& o, const foedus::EngineOptions& v);
#endif  // FOEDUS_ENGINE_OPTIONS_HPP_

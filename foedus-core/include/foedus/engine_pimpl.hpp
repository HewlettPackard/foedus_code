/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ENGINE_PIMPL_HPP_
#define FOEDUS_ENGINE_PIMPL_HPP_

#include <foedus/engine_options.hpp>
namespace foedus {
/**
 * @brief Pimpl object of Engine.
 * @ingroup ENGINE
 * @details
 * Detailed description of this class.
 */
class EnginePimpl {
 public:
    /**
     * Description of constructor.
     */
    explicit EnginePimpl(const EngineOptions &options);
    /**
     * Description of destructor.
     */
    ~EnginePimpl();

    /** Options given at boot time. */
    EngineOptions options_;
};
}  // namespace foedus
#endif  // FOEDUS_ENGINE_PIMPL_HPP_

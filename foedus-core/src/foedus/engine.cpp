/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_options.hpp>
#include <foedus/engine_pimpl.hpp>
namespace foedus {
Engine::Engine(const EngineOptions& options) {
    pimpl_ = new EnginePimpl(options);
}
Engine::~Engine() {
    delete pimpl_;
}
}  // namespace foedus

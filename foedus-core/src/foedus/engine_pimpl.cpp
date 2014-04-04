/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine_pimpl.hpp>
namespace foedus {
EnginePimpl::EnginePimpl(const EngineOptions &options)
    : options_(options), memory_(nullptr), filesystem_(nullptr), running_(false) {
}
EnginePimpl::~EnginePimpl() {
}

ErrorStack EnginePimpl::initialize() {
    return RET_OK;
}

ErrorStack EnginePimpl::uninitialize() {
    return RET_OK;
}

}  // namespace foedus

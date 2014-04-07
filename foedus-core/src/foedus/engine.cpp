/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine.hpp>
#include <foedus/engine_pimpl.hpp>
namespace foedus {
Engine::Engine(const EngineOptions& options) {
    pimpl_ = new EnginePimpl(options);
}
Engine::~Engine() {
    delete pimpl_;
}

// simply forward to pimpl object
const EngineOptions& Engine::get_options() const    { return pimpl_->options_; }

fs::Filesystem*     Engine::get_filesystem() const  { return pimpl_->filesystem_; }
memory::EngineMemory*   Engine::get_memory() const  { return pimpl_->memory_; }
void                Engine::set_initialized(bool value) { pimpl_->set_initialized(value); }
bool                Engine::is_initialized() const  { return pimpl_->is_initialized(); }
ErrorStack          Engine::initialize()            { return pimpl_->initialize(); }
ErrorStack          Engine::uninitialize()          { return pimpl_->uninitialize(); }


}  // namespace foedus

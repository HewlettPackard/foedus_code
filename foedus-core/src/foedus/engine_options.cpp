/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine_options.hpp>
#include <ostream>
namespace foedus {
EngineOptions::EngineOptions() {
}
EngineOptions::EngineOptions(const EngineOptions& other) {
    operator=(other);
}

EngineOptions& EngineOptions::operator=(const EngineOptions& other) {
    cache_ = other.cache_;
    fs_ = other.fs_;
    log_ = other.log_;
    memory_ = other.memory_;
    snapshot_ = other.snapshot_;
    storage_ = other.storage_;
    thread_ = other.thread_;
    return *this;
}

std::ostream& operator<<(std::ostream& o, const EngineOptions& v) {
    o << "[EngineOptions]" << std::endl;
    o << v.cache_ << std::endl;
    o << v.fs_ << std::endl;
    o << v.log_ << std::endl;
    o << v.memory_ << std::endl;
    o << v.snapshot_ << std::endl;
    o << v.storage_ << std::endl;
    o << v.thread_ << std::endl;
    return o;
}

}  // namespace foedus

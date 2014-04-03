/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/engine_options.hpp>
#include <foedus/log/log_options.hpp>
#include <foedus/memory/memory_options.hpp>
#include <foedus/snapshot/snapshot_options.hpp>
#include <foedus/storage/storage_options.hpp>
#include <ostream>
namespace foedus {
EngineOptions::EngineOptions() {
    log_ = new log::LogOptions;
    memory_ = new memory::MemoryOptions;
    snapshot_ = new snapshot::SnapshotOptions;
    storage_ = new storage::StorageOptions;
}
EngineOptions::EngineOptions(const EngineOptions& other) {
    log_ = new log::LogOptions(*other.log_);
    memory_ = new memory::MemoryOptions(*other.memory_);
    snapshot_ = new snapshot::SnapshotOptions(*other.snapshot_);
    storage_ = new storage::StorageOptions(*other.storage_);
}

EngineOptions& EngineOptions::operator=(const EngineOptions& other) {
    (*log_) = (*other.log_);
    (*memory_) = (*other.memory_);
    (*snapshot_) = (*other.snapshot_);
    (*storage_) = (*other.storage_);
    return *this;
}

EngineOptions::~EngineOptions() {
    delete storage_;
    delete snapshot_;
    delete memory_;
    delete log_;
}
}  // namespace foedus

std::ostream& operator<<(std::ostream& o, const foedus::EngineOptions& v) {
    o << "[EngineOptions]" << std::endl;
    o << *v.log_ << std::endl;
    o << *v.memory_ << std::endl;
    o << *v.snapshot_ << std::endl;
    o << *v.storage_ << std::endl;
    return o;
}

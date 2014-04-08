/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/initializable.hpp>
#include <glog/logging.h>
#include <cassert>
namespace foedus {
DefaultInitializable::~DefaultInitializable() {
    if (is_initialized()) {
        LOG(ERROR) << "Destructor has found that uninitialize() was not called. This is a BUG!"
            << " We must call uninitialize() before destructors!";
        assert(false);
    }
}
}  // namespace foedus


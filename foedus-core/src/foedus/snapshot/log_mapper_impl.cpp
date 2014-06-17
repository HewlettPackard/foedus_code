/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
4 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <foedus/engine.hpp>
#include <foedus/epoch.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/snapshot/log_gleaner_impl.hpp>
#include <foedus/snapshot/log_mapper_impl.hpp>
#include <glog/logging.h>
#include <ostream>
#include <string>
namespace foedus {
namespace snapshot {

ErrorStack LogMapper::handle_initialize() {
    return kRetOk;
}

ErrorStack LogMapper::handle_uninitialize() {
    ErrorStackBatch batch;
    return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack LogMapper::handle_epoch() {
    // Epoch epoch = parent_->get_processing_epoch();
    return kRetOk;
}

std::ostream& operator<<(std::ostream& o, const LogMapper& v) {
    o << "<LogMapper>"
        << "<id_>" << v.id_ << "</id_>"
        << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
        << "<thread_>" << v.thread_ << "</thread_>"
        << "</LogMapper>";
    return o;
}


}  // namespace snapshot
}  // namespace foedus

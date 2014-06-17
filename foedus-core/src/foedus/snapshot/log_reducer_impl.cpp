/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
4 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/assert_nd.hpp>
#include <foedus/engine.hpp>
#include <foedus/epoch.hpp>
#include <foedus/error_stack_batch.hpp>
#include <foedus/snapshot/log_gleaner_impl.hpp>
#include <foedus/snapshot/log_reducer_impl.hpp>
#include <glog/logging.h>
#include <ostream>
#include <string>
namespace foedus {
namespace snapshot {

ErrorStack LogReducer::handle_initialize() {
    return kRetOk;
}

ErrorStack LogReducer::handle_uninitialize() {
    ErrorStackBatch batch;
    return SUMMARIZE_ERROR_BATCH(batch);
}

ErrorStack LogReducer::handle_epoch() {
    // Epoch epoch = parent_->get_processing_epoch();
    SPINLOCK_WHILE(!parent_->is_all_mappers_completed()) {
    }
    return kRetOk;
}

std::ostream& operator<<(std::ostream& o, const LogReducer& v) {
    o << "<LogReducer>"
        << "<id_>" << v.id_ << "</id_>"
        << "<numa_node_>" << static_cast<int>(v.numa_node_) << "</numa_node_>"
        << "<thread_>" << v.thread_ << "</thread_>"
        << "</LogReducer>";
    return o;
}


}  // namespace snapshot
}  // namespace foedus

/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/error_stack_batch.hpp>
#include <iostream>
#include <sstream>
namespace foedus {
ErrorStack ErrorStackBatch::summarize() const {
    if (!is_error()) {
        return RET_OK;
    } else if (error_batch_.size() == 1) {
        return error_batch_[0];
    } else {
        // there were multiple errors. we must batch them.
        std::stringstream message;
        for (size_t i = 0; i < error_batch_.size(); ++i) {
            if (i > 0) {
                message << std::endl;
            }
            message << "Error[" << i << "]:" << error_batch_[i];
        }
        return ERROR_STACK_MSG(ERROR_CODE_BATCHED_ERROR, message.str().c_str());
    }
}
}  // namespace foedus

std::ostream& operator<<(std::ostream& o, const foedus::ErrorStackBatch& obj) {
    o << obj.summarize();
    return o;
}

/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/log/log_type_invoke.hpp>
#include <iostream>
namespace foedus {
namespace log {

#define X(a, b, c) case a: return reinterpret_cast< c* >(buffer)->apply_engine(xct_id, context);
void invoke_apply_engine(const xct::XctId &xct_id, void *buffer, thread::Thread* context) {
    invoke_assert_valid(buffer);
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
    LogCode code = header->get_type();
    switch (code) {
#include <foedus/log/log_type.xmacro> // NOLINT
        default:
            ASSERT_ND(false);
            return;
    }
}
#undef X

#define X(a, b, c) case a: \
    reinterpret_cast< c* >(buffer)->apply_storage(xct_id, context, storage); return;
void invoke_apply_storage(const xct::XctId &xct_id, void *buffer,
                                 thread::Thread* context, storage::Storage* storage) {
    invoke_assert_valid(buffer);
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
    LogCode code = header->get_type();
    switch (code) {
#include <foedus/log/log_type.xmacro> // NOLINT
        default:
            ASSERT_ND(false);
            return;
    }
}
#undef X


#define X(a, b, c) case a: o << *reinterpret_cast< c* >(buffer); break;
void invoke_ostream(void *buffer, std::ostream *ptr) {
    LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
    LogCode code = header->get_type();
    std::ostream &o = *ptr;
    switch (code) {
        case LOG_TYPE_INVALID: break;
#include <foedus/log/log_type.xmacro> // NOLINT
    }
}
#undef X

}  // namespace log
}  // namespace foedus

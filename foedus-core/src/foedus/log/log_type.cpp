/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/log/log_type.hpp"

#include <iostream>

namespace foedus {
namespace log {
// A bit tricky to get "a" from a in C macro.
#define X_QUOTE(str) #str
#define X_EXPAND_AND_QUOTE(str) X_QUOTE(str)
#define X(a, b, c) case a: return X_EXPAND_AND_QUOTE(a);
const char* get_log_type_name(LogCode code) {
  switch (code) {
    case kLogCodeInvalid: return "kLogCodeInvalid";
#include "foedus/log/log_type.xmacro" // NOLINT
    default: return "UNKNOWN";
  }
}
#undef X
#undef X_EXPAND_AND_QUOTE
#undef X_QUOTE

}  // namespace log
}  // namespace foedus

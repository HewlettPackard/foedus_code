/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ASSORTED_RICH_BACKTRACE_HPP_
#define FOEDUS_ASSORTED_RICH_BACKTRACE_HPP_

#include <string>
#include <vector>

namespace foedus {
namespace assorted {

/**
 * Returns the backtrace information of the current stack.
 * If rich flag is given, the backtrace information is converted to human-readable format
 * as much as possible via addr2line (which is linux-only).
 */
std::vector<std::string> get_backtrace(bool rich = true);

}  // namespace assorted
}  // namespace foedus



#endif  // FOEDUS_ASSORTED_RICH_BACKTRACE_HPP_

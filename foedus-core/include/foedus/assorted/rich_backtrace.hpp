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
 * @brief Returns the backtrace information of the current stack.
 * @details
 * If rich flag is given, the backtrace information is converted to human-readable format
 * as much as possible via addr2line (which is linux-only).
 * Also, this method does not care about out-of-memory situation.
 * When you are really concerned with it, just use ::backtrace(), ::backtrace_symbols_fd() etc.
 */
std::vector<std::string> get_backtrace(bool rich = true);

}  // namespace assorted
}  // namespace foedus



#endif  // FOEDUS_ASSORTED_RICH_BACKTRACE_HPP_

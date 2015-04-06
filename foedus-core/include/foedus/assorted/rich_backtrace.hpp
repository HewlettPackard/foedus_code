/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
 * When you are really concerned with it, just use backtrace, backtrace_symbols_fd etc.
 */
std::vector<std::string> get_backtrace(bool rich = true);

}  // namespace assorted
}  // namespace foedus



#endif  // FOEDUS_ASSORTED_RICH_BACKTRACE_HPP_

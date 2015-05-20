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
#include "foedus/error_stack_batch.hpp"

#include <iostream>
#include <sstream>

namespace foedus {
ErrorStack ErrorStackBatch::summarize(
  const char* filename, const char* func, uint32_t linenum) const {
  if (!is_error()) {
    return kRetOk;
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
    return ErrorStack(filename, func, linenum, kErrorCodeBatchedError, message.str().c_str());
  }
}

std::ostream& operator<<(std::ostream& o, const ErrorStackBatch& obj) {
  o << SUMMARIZE_ERROR_BATCH(obj);
  return o;
}

}  // namespace foedus

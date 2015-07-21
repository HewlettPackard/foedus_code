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
#include "foedus/log/log_type_invoke.hpp"

#include <iostream>

namespace foedus {
namespace log {

#define X(a, b, c) case a: return reinterpret_cast< c* >(buffer)->apply_engine(context);
void invoke_apply_engine(void *buffer, thread::Thread* context) {
  invoke_assert_valid(buffer);
  LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
  LogCode code = header->get_type();
  switch (code) {
#include "foedus/log/log_type.xmacro" // NOLINT
    default:
      ASSERT_ND(false);
      return;
  }
}
#undef X

#define X(a, b, c) case a: \
  reinterpret_cast< c* >(buffer)->apply_storage(engine, storage_id); return;
void invoke_apply_storage(void *buffer, Engine* engine, storage::StorageId storage_id) {
  invoke_assert_valid(buffer);
  LogHeader* header = reinterpret_cast<LogHeader*>(buffer);
  LogCode code = header->get_type();
  switch (code) {
#include "foedus/log/log_type.xmacro" // NOLINT
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
    case kLogCodeInvalid: break;
#include "foedus/log/log_type.xmacro" // NOLINT
  }
}
#undef X

}  // namespace log
}  // namespace foedus

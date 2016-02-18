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
#include "foedus/xct/xct_access.hpp"

#include <ostream>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/log_type_invoke.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage.hpp"
#include "foedus/xct/xct_id.hpp"

namespace foedus {
namespace xct {
std::ostream& operator<<(std::ostream& o, const PointerAccess& v) {
  o << "<PointerAccess><address>" << v.address_ << "</address>"
    << "<observed>" << assorted::Hex(v.observed_.word) << "</observed></PointerAccess>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const PageVersionAccess& v) {
  o << "<PageVersionAccess><address>" << v.address_ << "</address>"
    << "<observed>" << v.observed_ << "</observed></PageVersionAccess>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const ReadXctAccess& v) {
  o << "<ReadXctAccess><storage>" << v.storage_id_ << "</storage>"
//    << "<current_lock_position_>" << v.current_lock_position_ << "</current_lock_position_>"
    << "<observed_owner_id>" << v.observed_owner_id_ << "</observed_owner_id>"
    << "<record_address>" << v.owner_id_address_ << "</record_address>"
    << "<current_owner_id>" << *v.owner_id_address_ << "</current_owner_id>";
  if (v.related_write_) {
    o << "<HasRelatedWrite />";  // does not output its content to avoid circle
  }
  o << "</ReadXctAccess>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const WriteXctAccess& v) {
  o << "<WriteAccess><storage>" << v.storage_id_ << "</storage>"
    << "<record_address>" << v.owner_id_address_ << "</record_address>"
//    << "<current_lock_position_>" << v.current_lock_position_ << "</current_lock_position_>"
    << "<write_set_ordinal_>" << v.write_set_ordinal_ << "</write_set_ordinal_>"
    << "<current_owner_id>" << *(v.owner_id_address_) << "</current_owner_id><log>"
    << "<owner_lock_id>" << v.owner_lock_id_ << "</owner_lock_id><log>";
  log::invoke_ostream(v.log_entry_, &o);
  o << "</log>";
  if (v.related_read_) {
    o << "<HasRelatedRead />";  // does not output its content to avoid circle
  }
  o << "</WriteAccess>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const LockFreeWriteXctAccess& v) {
  o << "<LockFreeWriteXctAccess>"
    << "<storage>" << v.storage_id_ << "</storage>";
  log::invoke_ostream(v.log_entry_, &o);
  o << "</LockFreeWriteXctAccess>";
  return o;
}

}  // namespace xct
}  // namespace foedus

/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include "foedus/xct/xct_access.hpp"

#include <ostream>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/log/log_type.hpp"
#include "foedus/log/log_type_invoke.hpp"
#include "foedus/storage/page.hpp"
#include "foedus/storage/record.hpp"
#include "foedus/storage/storage.hpp"

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

std::ostream& operator<<(std::ostream& o, const XctAccess& v) {
  o << "<XctAccess><storage>" << v.storage_->get_name() << "</storage>"
    << "<observed_owner_id>" << v.observed_owner_id_ << "</observed_owner_id>"
    << "<record_address>" << v.owner_id_address_ << "</record_address>"
    << "<current_owner_id>" << *(v.owner_id_address_) << "</current_owner_id></XctAccess>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const WriteXctAccess& v) {
  o << "<WriteAccess><storage>" << v.storage_->get_name() << "</storage>"
    << "<record_address>" << v.owner_id_address_ << "</record_address>"
    << "<current_owner_id>" << *(v.owner_id_address_) << "</current_owner_id><log>";
  log::invoke_ostream(v.log_entry_, &o);
  o << "</log></WriteAccess>";
  return o;
}

std::ostream& operator<<(std::ostream& o, const LockFreeWriteXctAccess& v) {
  o << "<LockFreeWriteXctAccess>"
    << "<storage>" << v.storage_->get_id() << "</storage>";
  log::invoke_ostream(v.log_entry_, &o);
  o << "</LockFreeWriteXctAccess>";
  return o;
}

}  // namespace xct
}  // namespace foedus

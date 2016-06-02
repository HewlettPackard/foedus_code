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
#include "foedus/externalize/externalizable.hpp"

#include <tinyxml2.h>

#include <cstring>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "foedus/assorted/assorted_func.hpp"
#include "foedus/externalize/tinyxml_wrapper.hpp"
#include "foedus/fs/direct_io_file.hpp"
#include "foedus/fs/filesystem.hpp"
#include "foedus/fs/path.hpp"
#include "foedus/memory/aligned_memory.hpp"

namespace foedus {
namespace externalize {

ErrorStack Externalizable::load_from_string(const std::string& xml) {
  tinyxml2::XMLDocument document;
  tinyxml2::XMLError load_error = document.Parse(xml.data(), xml.size());
  if (load_error != tinyxml2::XML_SUCCESS) {
    std::stringstream custom_message;
    custom_message << "xml=" << xml << ", tinyxml2 error=" << load_error
       << ", GetErrorStr1()=" << document.GetErrorStr1()
       << ", GetErrorStr2()=" << document.GetErrorStr2();
    return ERROR_STACK_MSG(kErrorCodeConfParseFailed, custom_message.str().c_str());
  } else if (!document.RootElement()) {
    return ERROR_STACK_MSG(kErrorCodeConfEmptyXml, xml.c_str());
  } else {
    CHECK_ERROR(load(document.RootElement()));
  }
  return kRetOk;
}

void Externalizable::save_to_stream(std::ostream* ptr) const {
  std::ostream &o = *ptr;
  tinyxml2::XMLDocument doc;
  // root element name is class name.
  tinyxml2::XMLElement* element = doc.NewElement(get_tag_name());
  if (!element) {
    o << "Out-of-memory during Externalizable::save_to_stream()";
    return;
  }
  doc.InsertFirstChild(element);
  ErrorStack error_stack = save(element);
  if (error_stack.is_error()) {
    o << "Failed during Externalizable::save_to_stream(): " << error_stack;
    return;
  }
  tinyxml2::XMLPrinter printer;
  doc.Print(&printer);
  o << printer.CStr();
}

ErrorStack Externalizable::load_from_file(const fs::Path& path) {
  tinyxml2::XMLDocument document;
  if (!fs::exists(path)) {
    return ERROR_STACK_MSG(kErrorCodeConfFileNotFount, path.c_str());
  }

  tinyxml2::XMLError load_error = document.LoadFile(path.c_str());
  if (load_error != tinyxml2::XML_SUCCESS) {
    std::stringstream custom_message;
    custom_message << "problemtic file=" << path << ", tinyxml2 error=" << load_error
       << ", GetErrorStr1()=" << document.GetErrorStr1()
       << ", GetErrorStr2()=" << document.GetErrorStr2();
    return ERROR_STACK_MSG(kErrorCodeConfParseFailed, custom_message.str().c_str());
  } else if (!document.RootElement()) {
    return ERROR_STACK_MSG(kErrorCodeConfEmptyXml, path.c_str());
  } else {
    CHECK_ERROR(load(document.RootElement()));
  }
  return kRetOk;
}

ErrorStack Externalizable::save_to_file(const fs::Path& path) const {
  // construct the XML in memory
  tinyxml2::XMLDocument document;
  tinyxml2::XMLElement* root = document.NewElement(get_tag_name());
  CHECK_OUTOFMEMORY(root);
  document.InsertFirstChild(root);
  CHECK_ERROR(save(root));

  fs::Path folder = path.parent_path();
  // create the folder if not exists
  if (!fs::exists(folder)) {
    if (!fs::create_directories(folder, true)) {
      std::stringstream custom_message;
      custom_message << "file=" << path << ", folder=" << folder
        << ", err=" << assorted::os_error();
      return ERROR_STACK_MSG(kErrorCodeConfMkdirsFailed, custom_message.str().c_str());
    }
  }

  // Retrieve an XML representation. We initially used document.SaveFile(), but
  // it is not ideal as it uses non-aligned non-O_DIRECT APIs.
  // This caused a data loss in our NVDIMM environment.
  tinyxml2::XMLPrinter xml_stream;
  document.Print(&xml_stream);
  const uint32_t non_aligned_size = xml_stream.CStrSize();
  const uint32_t aligned_size = assorted::align<uint32_t, 1U << 12>(non_aligned_size);
  memory::AlignedMemory xml_memory;
  xml_memory.alloc(aligned_size, 1U << 12, memory::AlignedMemory::kPosixMemalign, 0);
  if (xml_memory.is_null()) {
    return ERROR_STACK_MSG(kErrorCodeConfCouldNotWrite, "Out of memory in posix_memalign");
  }
  char* buffer = reinterpret_cast<char*>(xml_memory.get_block());
  std::memcpy(buffer, xml_stream.CStr(), non_aligned_size);
  std::memset(buffer + non_aligned_size, '\n', aligned_size - non_aligned_size);  // LF padding

  // To atomically save a file, we write to a temporary file and call sync, then use POSIX rename.
  fs::Path tmp_path(path);
  tmp_path += ".tmp_";
  tmp_path += fs::unique_name("%%%%%%%%");

  {
    // Use Direct I/O to save this file. For all critical files, we always use
    // open/write with O_DIRECT.
    fs::DirectIoFile tmp_file(tmp_path);
    WRAP_ERROR_CODE(tmp_file.open(false, true, true, true));
    WRAP_ERROR_CODE(tmp_file.write(aligned_size, xml_memory));
    tmp_file.close();
    // TASK(Hideaki) We should not need the following fsync. Let's test this later.
    // For now, do a safer thing.
    fs::fsync(tmp_path, true);
  }

  if (!fs::durable_atomic_rename(tmp_path, path)) {
    std::stringstream custom_message;
    custom_message << "dest file=" << path << ", src file=" << tmp_path
      << ", err=" << assorted::os_error();
    return ERROR_STACK_MSG(kErrorCodeConfCouldNotRename, custom_message.str().c_str());
  }

  return kRetOk;
}


ErrorStack insert_comment_impl(tinyxml2::XMLElement* element, const std::string& comment) {
  if (comment.size() > 0) {
    tinyxml2::XMLComment* cm = element->GetDocument()->NewComment(comment.c_str());
    CHECK_OUTOFMEMORY(cm);
    tinyxml2::XMLNode* parent = element->Parent();
    if (!parent) {
      element->GetDocument()->InsertFirstChild(cm);
    } else {
      tinyxml2::XMLNode* previous = element->PreviousSibling();
      if (previous) {
        parent->InsertAfterChild(previous, cm);
      } else {
        parent->InsertFirstChild(cm);
      }
    }
  }
  return kRetOk;
}
ErrorStack Externalizable::insert_comment(tinyxml2::XMLElement* element,
                      const std::string& comment) {
  return insert_comment_impl(element, comment);
}
ErrorStack Externalizable::append_comment(tinyxml2::XMLElement* parent,
                      const std::string& comment) {
  if (comment.size() > 0) {
    tinyxml2::XMLComment* cm = parent->GetDocument()->NewComment(comment.c_str());
    CHECK_OUTOFMEMORY(cm);
    parent->InsertEndChild(cm);
  }
  return kRetOk;
}

ErrorStack Externalizable::create_element(tinyxml2::XMLElement* parent, const std::string& name,
        tinyxml2::XMLElement** out) {
  *out = parent->GetDocument()->NewElement(name.c_str());
  CHECK_OUTOFMEMORY(*out);
  parent->InsertEndChild(*out);
  return kRetOk;
}

template <typename T>
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent,
                const std::string& tag, const std::string& comment, T value) {
  tinyxml2::XMLElement* element = parent->GetDocument()->NewElement(tag.c_str());
  CHECK_OUTOFMEMORY(element);
  TinyxmlSetter<T> tinyxml_setter;
  tinyxml_setter(element, value);
  parent->InsertEndChild(element);
  if (comment.size() > 0) {
    CHECK_ERROR(insert_comment_impl(element,
            tag + " (type=" + assorted::get_pretty_type_name<T>() + "): " + comment));
  }
  return kRetOk;
}

// Explicit instantiations for each type
// @cond DOXYGEN_IGNORE
#define EXPLICIT_INSTANTIATION_ADD(x) template ErrorStack Externalizable::add_element< x > \
  (tinyxml2::XMLElement* parent, const std::string& tag, const std::string& comment, x value)
INSTANTIATE_ALL_TYPES(EXPLICIT_INSTANTIATION_ADD);
// @endcond

ErrorStack Externalizable::add_child_element(tinyxml2::XMLElement* parent, const std::string& tag,
                     const std::string& comment, const Externalizable& child) {
  tinyxml2::XMLElement* element = parent->GetDocument()->NewElement(tag.c_str());
  CHECK_OUTOFMEMORY(element);
  parent->InsertEndChild(element);
  CHECK_ERROR(insert_comment_impl(element, comment));
  CHECK_ERROR(child.save(element));
  return kRetOk;
}

template <typename T>
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                      T* out, bool optional, T default_value) {
  TinyxmlGetter<T> tinyxml_getter;
  tinyxml2::XMLElement* element = parent->FirstChildElement(tag.c_str());
  if (element) {
    tinyxml2::XMLError xml_error = tinyxml_getter(element, out);
    if (xml_error == tinyxml2::XML_SUCCESS) {
      return kRetOk;
    } else {
      return ERROR_STACK_MSG(kErrorCodeConfInvalidElement, tag.c_str());
    }
  } else {
    if (optional) {
      *out = default_value;
      return kRetOk;
    } else {
      return ERROR_STACK_MSG(kErrorCodeConfMissingElement, tag.c_str());
    }
  }
}

// Explicit instantiations for each type
// @cond DOXYGEN_IGNORE
#define EXPLICIT_INSTANTIATION_GET(x) template ErrorStack Externalizable::get_element< x > \
  (tinyxml2::XMLElement* parent, const std::string& tag, x * out, bool optional, x default_value)
INSTANTIATE_ALL_TYPES(EXPLICIT_INSTANTIATION_GET);
// @endcond

ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                  std::string* out, bool optional, const char* default_value) {
  return get_element<std::string>(parent, tag, out, optional, std::string(default_value));
}

template <typename T>
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                      std::vector<T> * out, bool optional) {
  out->clear();
  TinyxmlGetter<T> tinyxml_getter;
  for (tinyxml2::XMLElement* element = parent->FirstChildElement(tag.c_str());
     element; element = element->NextSiblingElement(tag.c_str())) {
    T tmp;
    tinyxml2::XMLError xml_error = tinyxml_getter(element, &tmp);
    if (xml_error == tinyxml2::XML_SUCCESS) {
      out->push_back(tmp);  // vector<bool> doesn't support emplace_back!
    } else {
      return ERROR_STACK_MSG(kErrorCodeConfInvalidElement, tag.c_str());
    }
  }
  if (out->size() == 0 && !optional) {
    return ERROR_STACK_MSG(kErrorCodeConfMissingElement, tag.c_str());
  }
  return kRetOk;
}

// Explicit instantiations for each type
// @cond DOXYGEN_IGNORE
#define EXPLICIT_INSTANTIATION_GET_VECTOR(x) template ErrorStack Externalizable::get_element< x > \
  (tinyxml2::XMLElement* parent, const std::string& tag, std::vector< x > * out, bool optional)
INSTANTIATE_ALL_TYPES(EXPLICIT_INSTANTIATION_GET_VECTOR);
// @endcond

ErrorStack Externalizable::get_child_element(tinyxml2::XMLElement* parent, const std::string& tag,
                     Externalizable* child, bool optional) {
  tinyxml2::XMLElement* element = parent->FirstChildElement(tag.c_str());
  if (element) {
    return child->load(element);
  } else {
    if (optional) {
      return kRetOk;
    } else {
      return ERROR_STACK_MSG(kErrorCodeConfMissingElement, tag.c_str());
    }
  }
}

}  // namespace externalize
}  // namespace foedus

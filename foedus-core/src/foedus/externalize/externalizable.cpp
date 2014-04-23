/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <foedus/fs/filesystem.hpp>
#include <foedus/fs/path.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <tinyxml2.h>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>
namespace foedus {
namespace externalize {
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
        return ERROR_STACK_MSG(ERROR_CODE_CONF_FILE_NOT_FOUNT, path.c_str());
    }

    tinyxml2::XMLError load_error = document.LoadFile(path.c_str());
    if (load_error != tinyxml2::XML_SUCCESS) {
        std::stringstream custom_message;
        custom_message << "problemtic file=" << path << ", tinyxml2 error=" << load_error
             << ", GetErrorStr1()=" << document.GetErrorStr1()
             << ", GetErrorStr2()=" << document.GetErrorStr2();
        return ERROR_STACK_MSG(ERROR_CODE_CONF_PARSE_FAILED, custom_message.str().c_str());
    } else if (!document.RootElement()) {
        return ERROR_STACK_MSG(ERROR_CODE_CONF_EMPTY_XML, path.c_str());
    } else {
        CHECK_ERROR(load(document.RootElement()));
    }
    return RET_OK;
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
            custom_message << "file=" << path << ", folder=" << folder << ", errno=" << errno;
            return ERROR_STACK_MSG(ERROR_CODE_CONF_MKDIRS_FAILED, custom_message.str().c_str());
        }
    }

    // To atomically save a file, we write to a temporary file and call sync, then use POSIX rename.
    fs::Path tmp_path(path);
    tmp_path += ".tmp_";
    tmp_path += fs::unique_path("%%%%%%%%");

    tinyxml2::XMLError save_error = document.SaveFile(tmp_path.c_str());
    if (save_error != tinyxml2::XML_SUCCESS) {
        std::stringstream custom_message;
        custom_message << "problemtic file=" << path << ", tinyxml2 error=" << save_error
             << ", GetErrorStr1()=" << document.GetErrorStr1()
             << ", GetErrorStr2()=" << document.GetErrorStr2();
        return ERROR_STACK_MSG(ERROR_CODE_CONF_COULD_NOT_WRITE, custom_message.str().c_str());
    }

    if (!fs::durable_atomic_rename(tmp_path, path)) {
        std::stringstream custom_message;
        custom_message << "dest file=" << path << ", src file=" << tmp_path
            << ", errno=" << errno;
        return ERROR_STACK_MSG(ERROR_CODE_CONF_COULD_NOT_RENAME, custom_message.str().c_str());
    }

    return RET_OK;
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
    return RET_OK;
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
    return RET_OK;
}

ErrorStack Externalizable::create_element(tinyxml2::XMLElement* parent, const std::string& name,
                tinyxml2::XMLElement** out) {
    *out = parent->GetDocument()->NewElement(name.c_str());
    CHECK_OUTOFMEMORY(*out);
    parent->InsertEndChild(*out);
    return RET_OK;
}


template <typename T>
ErrorStack add_element_impl(tinyxml2::XMLElement* parent,
                            const std::string& tag, const std::string& comment, const T& value) {
    tinyxml2::XMLElement* element = parent->GetDocument()->NewElement(tag.c_str());
    CHECK_OUTOFMEMORY(element);
    element->SetText(value);
    parent->InsertEndChild(element);
    if (comment.size() > 0) {
        CHECK_ERROR(insert_comment_impl(element,
                        tag + " (type=" + assorted::get_pretty_type_name<T>() + "): " + comment));
    }
    return RET_OK;
}

ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent,
                                const std::string& tag, const std::string& comment, bool value) {
    return add_element_impl<bool>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                              const std::string& comment, float value) {
    return add_element_impl<float>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                              const std::string& comment, double value) {
    return add_element_impl<double>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                           const std::string& comment, int64_t value) {
    return add_element_impl<int64_t>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                            const std::string& comment, uint64_t value) {
    return add_element_impl<uint64_t>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                           const std::string& comment, int32_t value) {
    return add_element_impl<int32_t>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                            const std::string& comment, uint32_t value) {
    return add_element_impl<uint32_t>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                           const std::string& comment, int16_t value) {
    return add_element_impl<int16_t>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                            const std::string& comment, uint16_t value) {
    return add_element_impl<uint16_t>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                           const std::string& comment, int8_t value) {
    return add_element_impl<int8_t>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                            const std::string& comment, uint8_t value) {
    return add_element_impl<uint8_t>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                            const std::string& comment, const std::string& value) {
    return add_element_impl<const char*>(parent, tag, comment, value.c_str());
}

ErrorStack Externalizable::add_child_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                       const std::string& comment, const Externalizable& child) {
    tinyxml2::XMLElement* element = parent->GetDocument()->NewElement(tag.c_str());
    CHECK_OUTOFMEMORY(element);
    parent->InsertEndChild(element);
    CHECK_ERROR(insert_comment_impl(element, comment));
    CHECK_ERROR(child.save(element));
    return RET_OK;
}


template <typename T> struct TinyxmlGetter {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, T* out);
};
template<> struct TinyxmlGetter<bool> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, bool *out) {
        return element->QueryBoolText(out);
    }
};
template<> struct TinyxmlGetter<int64_t> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, int64_t *out) {
        return element->QueryLongLongText(out);
    }
};
template<> struct TinyxmlGetter<uint64_t> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, uint64_t *out) {
        return element->QueryUnsignedLongLongText(out);
    }
};
template<> struct TinyxmlGetter<std::string> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, std::string *out) {
        *out = element->GetText();
        return tinyxml2::XML_SUCCESS;
    }
};
template<> struct TinyxmlGetter<double> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, double *out) {
        return element->QueryDoubleText(out);
    }
};
template<> struct TinyxmlGetter<float> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, float *out) {
        return element->QueryFloatText(out);
    }
};

template <typename T>
ErrorStack get_element_impl(tinyxml2::XMLElement* parent, const std::string& tag,
    T* out, bool optional, T default_value, TinyxmlGetter<T> tinyxml_getter) {
    tinyxml2::XMLElement* element = parent->FirstChildElement(tag.c_str());
    if (element) {
        tinyxml2::XMLError xml_error = tinyxml_getter(element, out);
        if (xml_error == tinyxml2::XML_SUCCESS) {
            return RET_OK;
        } else {
            return ERROR_STACK_MSG(ERROR_CODE_CONF_INVALID_ELEMENT, tag.c_str());
        }
    } else {
        if (optional) {
            *out = default_value;
            return RET_OK;
        } else {
            return ERROR_STACK_MSG(ERROR_CODE_CONF_MISSING_ELEMENT, tag.c_str());
        }
    }
}

template <typename T>
ErrorStack get_element_vector_impl(tinyxml2::XMLElement* parent, const std::string& tag,
    std::vector<T>* out, bool optional, TinyxmlGetter<T> tinyxml_getter) {
    out->clear();
    for (tinyxml2::XMLElement* element = parent->FirstChildElement(tag.c_str());
         element; element = element->NextSiblingElement(tag.c_str())) {
        T tmp;
        tinyxml2::XMLError xml_error = tinyxml_getter(element, &tmp);
        if (xml_error == tinyxml2::XML_SUCCESS) {
            out->emplace_back(tmp);
        } else {
            return ERROR_STACK_MSG(ERROR_CODE_CONF_INVALID_ELEMENT, tag.c_str());
        }
    }
    if (out->size() == 0 && !optional) {
        return ERROR_STACK_MSG(ERROR_CODE_CONF_MISSING_ELEMENT, tag.c_str());
    }
    return RET_OK;
}

template <typename T, typename LARGEST_TYPE>
ErrorStack get_smaller_element_vector(tinyxml2::XMLElement* parent, const std::string& tag,
                            std::vector<T> * out, bool optional) {
    std::vector< LARGEST_TYPE > tmp;
    CHECK_ERROR(get_element_vector_impl< LARGEST_TYPE >(
        parent, tag, &tmp, optional, TinyxmlGetter< LARGEST_TYPE >()));
    out->clear();
    for (LARGEST_TYPE value : tmp) {
        if (static_cast<LARGEST_TYPE>(static_cast<T>(value)) != value) {
            return ERROR_STACK_MSG(ERROR_CODE_CONF_VALUE_OUTOFRANGE, tag.c_str());
        }
        out->push_back(static_cast<T>(value));
    }
    return RET_OK;
}


ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                            bool* out, bool optional, bool default_value) {
    return get_element_impl(parent, tag, out, optional, default_value, TinyxmlGetter<bool>());
}

ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                             int64_t* out, bool optional, int64_t default_value) {
    return get_element_impl(parent, tag, out, optional, default_value, TinyxmlGetter<int64_t>());
}

ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                    uint64_t* out, bool optional, uint64_t default_value) {
    return get_element_impl(parent, tag, out, optional, default_value, TinyxmlGetter<uint64_t>());
}


ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                            int32_t* out, bool optional, int32_t default_value) {
    return get_smaller_element<int32_t, int64_t>(parent, tag, out, optional, default_value);
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                            int16_t* out, bool optional, int16_t default_value) {
    return get_smaller_element<int16_t, int64_t>(parent, tag, out, optional, default_value);
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                            int8_t* out, bool optional, int8_t default_value) {
    return get_smaller_element<int8_t, int64_t>(parent, tag, out, optional, default_value);
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                            uint32_t* out, bool optional, uint32_t default_value) {
    return get_smaller_element<uint32_t, uint64_t>(parent, tag, out, optional, default_value);
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                            uint16_t* out, bool optional, uint16_t default_value) {
    return get_smaller_element<uint16_t, uint64_t>(parent, tag, out, optional, default_value);
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                            uint8_t* out, bool optional, uint8_t default_value) {
    return get_smaller_element<uint8_t, uint64_t>(parent, tag, out, optional, default_value);
}

ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                              float* out, bool optional, float default_value) {
    return get_element_impl(parent, tag, out, optional, default_value, TinyxmlGetter<float>());
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                              double* out, bool optional, double default_value) {
    return get_element_impl(parent, tag, out, optional, default_value, TinyxmlGetter<double>());
}

ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                    std::string* out, bool optional, const char* default_value) {
    return get_element_impl(parent, tag, out, optional,
                       std::string(default_value), TinyxmlGetter<std::string>());
}

ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                    std::vector<int64_t>* out, bool optional) {
    return get_element_vector_impl(parent, tag, out, optional, TinyxmlGetter<int64_t>());
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                    std::vector<uint64_t>* out, bool optional) {
    return get_element_vector_impl(parent, tag, out, optional, TinyxmlGetter<uint64_t>());
}

ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                    std::vector<int32_t>* out, bool optional) {
    return get_smaller_element_vector<int32_t, int64_t>(parent, tag, out, optional);
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                    std::vector<uint32_t>* out, bool optional) {
    return get_smaller_element_vector<uint32_t, uint64_t>(parent, tag, out, optional);
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                    std::vector<int16_t>* out, bool optional) {
    return get_smaller_element_vector<int16_t, int64_t>(parent, tag, out, optional);
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                    std::vector<uint16_t>* out, bool optional) {
    return get_smaller_element_vector<uint16_t, uint64_t>(parent, tag, out, optional);
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                    std::vector<int8_t>* out, bool optional) {
    return get_smaller_element_vector<int8_t, int64_t>(parent, tag, out, optional);
}
ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                    std::vector<uint8_t>* out, bool optional) {
    return get_smaller_element_vector<uint8_t, uint64_t>(parent, tag, out, optional);
}

ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                       std::vector< std::string >* out, bool optional) {
    return get_element_vector_impl(parent, tag, out, optional, TinyxmlGetter<std::string>());
}

ErrorStack Externalizable::get_child_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                       Externalizable* child, bool optional) {
    tinyxml2::XMLElement* element = parent->FirstChildElement(tag.c_str());
    if (element) {
        return child->load(element);
    } else {
        if (optional) {
            return RET_OK;
        } else {
            return ERROR_STACK_MSG(ERROR_CODE_CONF_MISSING_ELEMENT, tag.c_str());
        }
    }
}

}  // namespace externalize
}  // namespace foedus

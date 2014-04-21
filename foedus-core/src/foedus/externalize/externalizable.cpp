/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#include <foedus/externalize/externalizable.hpp>
#include <tinyxml2.h>
#include <ostream>
#include <string>
#include <typeinfo>
#include <vector>
namespace foedus {
namespace externalize {
void Externalizable::save_to_stream(std::ostream* ptr) const {
    std::ostream &o = *ptr;
    tinyxml2::XMLDocument doc;
    // root element name is class name.
    tinyxml2::XMLElement* element = doc.NewElement("Root");
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
    // type_info.name() is a bit hard to read. maybe http://ideone.com/GZEGN6 would help.
    // but, I don't wanna introduce compiler-dependent demangling just for comment...
    insert_comment_impl(element, tag + " (type=" + typeid(value).name() + "): " + comment);
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
                                           const std::string& comment, int32_t value) {
    return add_element_impl<int>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                            const std::string& comment, uint32_t value) {
    return add_element_impl<uint>(parent, tag, comment, value);
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                            const std::string& comment, const std::string& value) {
    return add_element_impl<const char*>(parent, tag, comment, value.c_str());
}
ErrorStack Externalizable::add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                    const std::string& comment, const std::vector< std::string >& value) {
    for (std::size_t i = 0; i < value.size(); ++i) {
        CHECK_ERROR(add_element_impl<const char*>(parent, tag, comment, value[i].c_str()));
    }
    return RET_OK;
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
template<> struct TinyxmlGetter<int32_t> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, int32_t *out) {
        return element->QueryIntText(out);
    }
};
template<> struct TinyxmlGetter<uint32_t> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, uint32_t *out) {
        return element->QueryUnsignedText(out);
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


ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                            bool* out, bool optional, bool default_value) {
    return get_element_impl(parent, tag, out, optional, default_value, TinyxmlGetter<bool>());
}

ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                             int32_t* out, bool optional, int32_t default_value) {
    return get_element_impl(parent, tag, out, optional, default_value, TinyxmlGetter<int32_t>());
}

ErrorStack Externalizable::get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                    uint32_t* out, bool optional, uint32_t default_value) {
    return get_element_impl(parent, tag, out, optional, default_value, TinyxmlGetter<uint32_t>());
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

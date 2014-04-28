/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_EXTERNALIZE_TINYXML_WRAPPER_HPP_
#define FOEDUS_EXTERNALIZE_TINYXML_WRAPPER_HPP_
#include <tinyxml2.h>
#include <stdint.h>
#include <string>
#include <vector>
namespace foedus {
namespace externalize {
/**
 * @brief Functor to help use tinyxml2's Element QueryXxxText().
 * @ingroup EXTERNALIZE
 * @details
 * tinyxml2::XMLElement has a different method name for each type. Handle it by these getters.
 */
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

template <typename T, typename LARGEST_TYPE, typename LARGEST_GETTER>
tinyxml2::XMLError get_smaller_int(const tinyxml2::XMLElement *element, T* out) {
    LARGEST_TYPE tmp;
    LARGEST_GETTER largest_getter;
    tinyxml2::XMLError ret = largest_getter(element, &tmp);
    if (ret != tinyxml2::XML_SUCCESS) {
        return ret;
    }
    *out = static_cast<T>(tmp);
    if (static_cast<LARGEST_TYPE>(*out) != tmp) {
        return tinyxml2::XML_CAN_NOT_CONVERT_TEXT;
    } else {
        return tinyxml2::XML_SUCCESS;
    }
}

template<> struct TinyxmlGetter<int32_t> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, int32_t *out) {
        return get_smaller_int<int32_t, int64_t, TinyxmlGetter<int64_t> >(element, out);
    }
};
template<> struct TinyxmlGetter<uint32_t> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, uint32_t *out) {
        return get_smaller_int<uint32_t, uint64_t, TinyxmlGetter<uint64_t> >(element, out);
    }
};
template<> struct TinyxmlGetter<int16_t> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, int16_t *out) {
        return get_smaller_int<int16_t, int64_t, TinyxmlGetter<int64_t> >(element, out);
    }
};
template<> struct TinyxmlGetter<uint16_t> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, uint16_t *out) {
        return get_smaller_int<uint16_t, uint64_t, TinyxmlGetter<uint64_t> >(element, out);
    }
};
template<> struct TinyxmlGetter<int8_t> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, int8_t *out) {
        return get_smaller_int<int8_t, int64_t, TinyxmlGetter<int64_t> >(element, out);
    }
};
template<> struct TinyxmlGetter<uint8_t> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, uint8_t *out) {
        return get_smaller_int<uint8_t, uint64_t, TinyxmlGetter<uint64_t> >(element, out);
    }
};

template<> struct TinyxmlGetter<std::string> {
    tinyxml2::XMLError operator()(const tinyxml2::XMLElement *element, std::string *out) {
        const char* text = element->GetText();
        if (text) {
            *out = text;
        } else {
            out->clear();
        }
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

/**
 * @brief Functor to help use tinyxml2's Element SetText().
 * @ingroup EXTERNALIZE
 * @details
 * tinyxml2::XMLElement::SetText() is already generic, but it doesn't know std::string.
 */
template <typename T> struct TinyxmlSetter {
    void operator()(tinyxml2::XMLElement *element, T value) {
        element->SetText(value);
    }
};
template <> struct TinyxmlSetter<std::string> {
    void operator()(tinyxml2::XMLElement *element, std::string value) {
        element->SetText(value.c_str());
    }
};

}  // namespace externalize
}  // namespace foedus

#endif  // FOEDUS_EXTERNALIZE_TINYXML_WRAPPER_HPP_

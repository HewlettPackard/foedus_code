/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_EXTERNALIZE_EXTERNALIZABLE_HPP_
#define FOEDUS_EXTERNALIZE_EXTERNALIZABLE_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <foedus/assorted/assorted_func.hpp>
#include <foedus/fs/fwd.hpp>
#include <stdint.h>
#include <iosfwd>
#include <string>
#include <vector>
// forward declarations for tinyxml2. They should provide a header file for this...
namespace tinyxml2 {
    class XMLDocument;
    class XMLElement;
    class XMLAttribute;
    class XMLComment;
    class XMLNode;
    class XMLText;
    class XMLDeclaration;
    class XMLUnknown;
    class XMLPrinter;
}  // namespace tinyxml2

namespace foedus {
namespace externalize {
/**
 * @brief Represents an object that can be written to and read from files/bytes in XML format.
 * @ingroup EXTERNALIZE
 * @details
 * Derived classes must implement load() and save().
 */
struct Externalizable {
    virtual ~Externalizable() {}

    /**
     * @brief Reads the content of this object from the given XML element.
     * @param[in] element the XML element that represents this object
     * @details
     * Expect errors due to missing-elements, out-of-range values, etc.
     */
    virtual ErrorStack load(tinyxml2::XMLElement* element) = 0;

    /**
     * @brief Writes the content of this object to the given XML element.
     * @param[in] element the XML element that represents this object
     * @details
     * Expect only out-of-memory error.
     * We receive the XML element this object will represent, so this method does not determine
     * the XML element name of itself. The parent object determines children's tag names
     * because one parent object might have multiple child objects of the same type with different
     * XML element name.
     */
    virtual ErrorStack save(tinyxml2::XMLElement* element) const = 0;

    /**
     * @brief Returns an XML tag name for this object as a root element.
     * @details
     * We might want to give a different name for same externalizable objects,
     * so this is used only when it is the root element of xml.
     */
    virtual const char* get_tag_name() const = 0;

    /**
     * @brief Polymorphic assign operator. This should invoke operator= of the derived class.
     * @param[in] other assigned value. It must be dynamic-castable to the assignee class.
     */
    virtual void assign(const foedus::externalize::Externalizable *other) = 0;

    /**
     * @brief Invokes save() and directs the resulting XML text to the given stream.
     * @param[in] ptr ostream to write to.
     */
    void        save_to_stream(std::ostream* ptr) const;

    /**
     * @brief Load the content of this object from the specified XML file.
     * @param[in] path path of the XML file.
     * @details
     * Expect errors due to missing-elements, out-of-range values, etc.
     */
    ErrorStack  load_from_file(const fs::Path &path);

    /**
     * @brief Atomically and durably writes out this object to the specified XML file.
     * @param[in] path path of the XML file.
     * @details
     * If the file exists, this method atomically overwrites it via POSIX's atomic rename semantics.
     * If the parent folder doesn't exist, this method automatically creates the folder.
     * Expect errors due to file-permission (and other file I/O issue), out-of-memory, etc.
     */
    ErrorStack  save_to_file(const fs::Path &path) const;

    // convenience methods
    static ErrorStack insert_comment(tinyxml2::XMLElement* element, const std::string& comment);
    static ErrorStack append_comment(tinyxml2::XMLElement* parent, const std::string& comment);
    static ErrorStack create_element(tinyxml2::XMLElement* parent, const std::string& name,
                                    tinyxml2::XMLElement** out);

    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, bool value);
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, int64_t value);
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, uint64_t value);
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, int32_t value);
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, uint32_t value);
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, int16_t value);
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, uint16_t value);
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, int8_t value);
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, uint8_t value);
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, const std::string& value);
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, float value);
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, double value);

    template <typename T>
    static ErrorStack add_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        const std::string& comment, const std::vector< T >& value) {
        if (comment.size() > 0) {
            CHECK_ERROR(append_comment(parent,
                tag + " (type=" + assorted::get_pretty_type_name< std::vector< T > >()
                    + "): " + comment));
        }
        for (std::size_t i = 0; i < value.size(); ++i) {
            CHECK_ERROR(add_element(parent, tag, "", value[i]));
        }
        return RET_OK;
    }

    template <typename ENUM>
    static ErrorStack add_enum_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                const std::string& comment, ENUM value) {
        return add_element(parent, tag, comment, static_cast<int64_t>(value));
    }

    static ErrorStack add_child_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                  const std::string& comment, const Externalizable& child);

    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                                bool* out, bool optional = false, bool default_value = false);

    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                               int64_t* out, bool optional = false, int64_t default_value = 0);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                               uint64_t* out, bool optional = false, uint64_t default_value = 0);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                               int32_t* out, bool optional = false, int32_t default_value = 0);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                               int16_t* out, bool optional = false, int16_t default_value = 0);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                               int8_t* out, bool optional = false, int8_t default_value = 0);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                               uint32_t* out, bool optional = false, uint32_t default_value = 0);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                               uint16_t* out, bool optional = false, uint16_t default_value = 0);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                               uint8_t* out, bool optional = false, uint8_t default_value = 0);

    template <typename T, typename LARGEST_TYPE>
    static ErrorStack get_smaller_element(tinyxml2::XMLElement* parent, const std::string& tag,
                               T* out, bool optional, T default_value) {
        LARGEST_TYPE tmp;
        CHECK_ERROR(get_element(parent, tag, &tmp, optional, default_value));
        if (static_cast<LARGEST_TYPE>(static_cast<T>(tmp)) != tmp) {
            return ERROR_STACK_MSG(ERROR_CODE_CONF_VALUE_OUTOFRANGE, tag.c_str());
        }
        *out = static_cast<T>(tmp);
        return RET_OK;
    }

    template <typename ENUM>
    static ErrorStack get_enum_element(tinyxml2::XMLElement* parent, const std::string& tag,
                    ENUM* out, bool optional = false, ENUM default_value = static_cast<ENUM>(0)) {
        // enum might be signged or unsigned, 1 byte, 2 byte, or 4 byte.
        // But surely it won't exceed int64_t range.
        return get_smaller_element<ENUM, int64_t>(parent, tag, out, optional, default_value);
    }

    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                               float* out, bool optional = false, float default_value = 0);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                               double* out, bool optional = false, double default_value = 0);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        std::string* out, bool optional = false, const char* default_value = "");

    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        std::vector<int64_t>* out, bool optional = false);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        std::vector<uint64_t>* out, bool optional = false);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        std::vector<int32_t>* out, bool optional = false);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        std::vector<uint32_t>* out, bool optional = false);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        std::vector<int16_t>* out, bool optional = false);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        std::vector<uint16_t>* out, bool optional = false);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        std::vector<int8_t>* out, bool optional = false);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        std::vector<uint8_t>* out, bool optional = false);
    static ErrorStack get_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        std::vector< std::string >* out, bool optional = false);

    static ErrorStack get_child_element(tinyxml2::XMLElement* parent, const std::string& tag,
                        Externalizable* child, bool optional = false);
};

}  // namespace externalize
}  // namespace foedus

// A bit tricky to get "a" from a in C macro.
#define EX_QUOTE(str) #str
#define EX_EXPAND(str) EX_QUOTE(str)

/**
 * @def EXTERNALIZE_SAVE_ELEMENT(element, attribute, comment)
 * @ingroup EXTERNALIZE
 * @brief Adds an xml element to represent a member variable of \e this object.
 * @param[in] element the current XML element that represents \e this, in other words parent
 * of the new element.
 * @param[in] attribute the member variable of \e this to save. This is also used as tag name.
 * @param[in] comment this is output as an XML comment.
 */
#define EXTERNALIZE_SAVE_ELEMENT(element, attribute, comment) \
    CHECK_ERROR(add_element(element, EX_EXPAND(attribute), comment, attribute))
/**
 * @def EXTERNALIZE_SAVE_ENUM_ELEMENT(element, attribute, comment)
 * @ingroup EXTERNALIZE
 * @copydoc EXTERNALIZE_SAVE_ELEMENT(element, attribute, comment)
 * @details
 * For enums, use this one.
 */
#define EXTERNALIZE_SAVE_ENUM_ELEMENT(element, attribute, comment) \
    CHECK_ERROR(add_enum_element(element, EX_EXPAND(attribute), comment, attribute))

/**
 * @def EXTERNALIZE_LOAD_ELEMENT(element, attribute)
 * @ingroup EXTERNALIZE
 * @brief Reads a child xml element to load a member variable of \e this object.
 * @param[in] element the current XML element that represents \e this, in other words parent
 * of the element to read.
 * @param[in] attribute the member variable of \e this to save. This is also used as tag name.
 */
#define EXTERNALIZE_LOAD_ELEMENT(element, attribute) \
    CHECK_ERROR(get_element(element, EX_EXPAND(attribute), & attribute))
/**
 * @def EXTERNALIZE_LOAD_ELEMENT_OPTIONAL(element, attribute, default_value)
 * @ingroup EXTERNALIZE
 * @copydoc EXTERNALIZE_LOAD_ELEMENT(element, attribute)
 * @param[in] default_value If the element doesn't exist, this value is set to the variable.
 * @details
 * For optional elements, use this.
 */
#define EXTERNALIZE_LOAD_ELEMENT_OPTIONAL(element, attribute, default_value) \
    CHECK_ERROR(get_element(element, EX_EXPAND(attribute), & attribute, true, default_value))

/**
 * @def EXTERNALIZE_LOAD_ENUM_ELEMENT(element, attribute)
 * @ingroup EXTERNALIZE
 * @copydoc EXTERNALIZE_LOAD_ELEMENT(element, attribute)
 * @details
 * For enum, use this one.
 */
#define EXTERNALIZE_LOAD_ENUM_ELEMENT(element, attribute) \
    CHECK_ERROR(get_enum_element(element, EX_EXPAND(attribute), & attribute))
/**
 * @def EXTERNALIZE_LOAD_ENUM_ELEMENT_OPTIONAL(element, attribute, default_value)
 * @ingroup EXTERNALIZE
 * @copydoc EXTERNALIZE_LOAD_ELEMENT_OPTIONAL(element, attribute, default_value)
 * @details
 * For optional enum, use this one.
 */
#define EXTERNALIZE_LOAD_ENUM_ELEMENT_OPTIONAL(element, attribute, default_value) \
    CHECK_ERROR(get_enum_element(element, EX_EXPAND(attribute), & attribute, true, default_value))

/**
 * @def EXTERNALIZABLE(clazz)
 * @ingroup EXTERNALIZE
 * @brief Macro to declare/define essential methods for an externalizable class.
 * @details
 * Each externalizable class should invoke this macro in public scope of class definition.
 * Then, it should define load() and save() in cpp.
 */
#define EXTERNALIZABLE(clazz) \
    ErrorStack load(tinyxml2::XMLElement* element) CXX11_OVERRIDE;\
    ErrorStack save(tinyxml2::XMLElement* element) const CXX11_OVERRIDE;\
    const char* get_tag_name() const CXX11_OVERRIDE { return EX_EXPAND(clazz); }\
    void assign(const foedus::externalize::Externalizable *other) CXX11_OVERRIDE {\
        *this = *dynamic_cast< const clazz * >(other);\
    }\
    friend std::ostream& operator<<(std::ostream& o, const clazz & v) {\
        v.save_to_stream(&o);\
        return o;\
    }

#endif  // FOEDUS_EXTERNALIZE_EXTERNALIZABLE_HPP_

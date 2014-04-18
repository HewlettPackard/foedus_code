/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_EXTERNALIZE_EXTERNALIZABLE_HPP_
#define FOEDUS_EXTERNALIZE_EXTERNALIZABLE_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <stdint.h>
#include <iostream>
#include <string>
#include <vector>
/**
 * @defgroup EXTERNALIZE Externalization
 * @brief \b Isolation \b Levels for Transactions
 * @ingroup IDIOMS
 * @details
 * @par Overview
 * Analogous to java.io.Externalizable.
 * We do externalization for metadata and configuration objects.
 * We never use them for main data files, so the code here does not have to be seriously optimized.
 *
 * @par Why not boost::serialize
 * Because boost::serialize is not header-only (booo!).
 * Same applies to other XML-based libraries.
 * We don't need a complex serialization, so just DYI.
 *
 * @par Externalization vs Serialization
 * I just picked the word \e Externalize because \e serialization, or \e serializable, has totally
 * different meanings in DBMS and lock-free algorithm.
 */

namespace foedus {
namespace externalize {
/**
 * @brief An interface to denote that the class can be written to and read from files/bytes.
 * @ingroup EXTERNALIZE
 */
/*
struct Externalizable {
    virtual ~Externalizable() {}
    virtual ErrorStack read_externalizable(const std::string &path)     = 0;
    virtual ErrorStack read_externalizable(std::istream *istr)          = 0;
    virtual ErrorStack write_externalizable(const std::string &path)    = 0;
    virtual ErrorStack write_externalizable(std::ostream *ostr)         = 0;
};
*/

/**
 * @brief Write up a value to the given stream as an XML element.
 * @ingroup EXTERNALIZE
 * @details
 * Use EXTERNALIZE_WRITE(x) for convenience.
 *
 */
template <typename T>
inline void write(std::ostream* ostr, const std::string &tag, const T& value) {
    std::ostream& o = *ostr;
    o << "    <" << tag << ">" << value << "</" << tag << ">" << std::endl;
}

// bool (0/1) to true/false just for aesthetic reason
template <>
inline void write<bool>(std::ostream* ostr, const std::string &tag, const bool& value) {
    std::ostream& o = *ostr;
    o << "    <" << tag << ">" << (value ? "true" : "false") << "</" << tag << ">" << std::endl;
}

// char/uchar: just cast it to int16_t to make it safe
template <>
inline void write<char>(std::ostream* ostr, const std::string &tag, const char& value) {
    write<int16_t>(ostr, tag, value);
}
template <>
inline void write<unsigned char>(std::ostream* ostr, const std::string &tag,
                                 unsigned char const & value) {
    write<uint16_t>(ostr, tag, value);
}

// string has to be escaped
inline std::string xml_escape(const std::string &value) {
    std::string result;
    result.reserve(value.size());
    for (std::size_t i = 0; i < value.size(); ++i) {
        switch (value[i]) {
            case '&':
                result.append("&amp;");
                break;
            case '>':
                result.append("&gt;");
                break;
            case '<':
                result.append("&lt;");
                break;
            case '\'':
                result.append("&apos;");
                break;
            case '\"':
                result.append("&quot;");
                break;
            default:
                result.push_back(value[i]);
        }
    }
    return result;
}

template <> inline void write< std::string >(
    std::ostream* ostr, const std::string &tag, const std::string& value) {
    std::ostream& o = *ostr;
    o << "    <" << tag << ">" << xml_escape(value) << "</" << tag << ">" << std::endl;
}

// Currently, std::vector<string> is the only vector type we care.
// C++ doesn't allow partial specialization of functions, but this suffices.
template <> inline void write< std::vector< std::string > >(
    std::ostream* ostr, const std::string &tag, const std::vector< std::string >& value) {
    for (std::size_t i = 0; i < value.size(); ++i) {
        write(ostr, tag, value[i]);
    }
}


}  // namespace externalize
}  // namespace foedus

// A bit tricky to get "a" from a in C macro.
#define EX_QUOTE(str) #str
#define EX_EXPAND(str) EX_QUOTE(str)

#define EXTERNALIZE_WRITE(x) foedus::externalize::write(&o, EX_EXPAND(x), v. x)

#endif  // FOEDUS_EXTERNALIZE_EXTERNALIZABLE_HPP_

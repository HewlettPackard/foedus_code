/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ERROR_STACK_HPP_
#define FOEDUS_ERROR_STACK_HPP_

#include <foedus/error_code.hpp>
#include <foedus/cxx11.hpp>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iosfwd>
namespace foedus {

/**
 * @brief Brings error stacktrace information as return value of functions.
 * @ingroup ERRORCODES
 * @details
 * This is returned by many API functions.
 * As it brings stacktrace information, it's more informative than just returning ErrorCode.
 * However, note that instantiating and augmenting this stack object has some overhead.
 *
 * @par Why not exception
 * A couple of reasons:
 *   \li Performance
 *   \li Portability
 *   \li Our Google C++ Coding Style overlord said so
 *
 * We are not even sure what exceptions would look like in future environments.
 * So, we don't throw or catch any exceptions in our program.
 *
 * @par Macros to help use ErrorStack
 * In most places, you should use RET_OK, CHECK(x), or ERROR_STACK(e) to handle this class.
 * See the doucments of those macros.
 *
 * @par Forced return code checking
 * An error code must be checked by some code, else it will abort with an
 * "error-not-checked" error in stderr. We might later make this warning instead of aborting,
 * but we should keep the current setting for a while to check for undesired coding.
 * Once you get used to, making it sure is quite effortless.
 *
 * @par Maximum stack trace depth
 * When the return code is an error code, we propagate back the stack trace
 * for easier debugging. We could have a linked-list for this
 * and, to ameriolate allocate/delete cost for it, a TLS object pool.
 * Unfortunately, it causes issues in some environments and is not so readable/maintainable.
 * Instead, we limit the depth of stacktraces stored in this object to a reasonable number
 * enough for debugging; \ref MAX_STACK_DEPTH.
 * We then store just line numbers and const pointers to file names. No heap allocation.
 * The only thing that has to be allocated on heap is a custom error message.
 * However, there are not many places that use custom messages, so the cost usually doesn't happen.
 *
 * @par Moveable
 * This object is \e moveable only when C++11 is enabled. The moveable semantics is beneficial
 * only when there is a custom message. Otherwise, this object doesn't own any object anyways.
 *
 * This class is header-only \b except output(), dump_and_abort(), and std::ostream redirect.
 */
class ErrorStack {
 public:
    /** Constant values. */
    enum Constants {
       /** Maximum stack trace depth. */
       MAX_STACK_DEPTH = 8,
    };

    /** Empty constructor. This is same as duplicating RET_OK. */
    ErrorStack();

    /**
     * @brief Instantiate a return code without a custom error message nor stacktrace.
     * @param[in] code Error code, either ERROR_CODE_OK or real errors.
     * @details
     * This is the most (next to RET_OK) light-weight way to create/propagate a return code.
     * Use this one if you do not need a detail information to debug the error (eg, error whose
     * cause is obvious, an expected error that is immediately caught, etc).
     */
    explicit ErrorStack(ErrorCode code);

    /**
     * @brief Instantiate a return code with stacktrace and optionally a custom error message.
     * @param[in] filename file name of the current place.
     *   It must be a const and permanent string, such as what "__FILE__" returns. Note that we do
     * NOT do deep-copy of the strings.
     * @param[in] linenum line number of the current place. Usually "__LINE__".
     * @param[in] code Error code, must be real errors.
     * @param[in] custom_message Optional custom error message in addition to the default one
     * inferred from error code. If you pass a non-NULL string to this argument,
     * we do deep-copy for each hand-over, so it's EXPENSIVE!
     */
    ErrorStack(const char* filename, uint32_t linenum, ErrorCode code,
                const char* custom_message = NULL);

    /** Copy constructor. */
    ErrorStack(const ErrorStack &other);

    /** Copy constructor to augment the stacktrace. */
    ErrorStack(const ErrorStack &other, const char* filename, uint32_t linenum,
                const char* more_custom_message = NULL);

    /** Copy constructor. */
    ErrorStack& operator=(ErrorStack const &other);

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
    /**
     * Move constructor that steals the custom message without copying.
     * This is more efficient than non-move copy constructor above if it has a custom message,
     * but this is available only with C++11.
     */
    ErrorStack(ErrorStack &&other);
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

    /** Will warn in stderr if the error code is not checked yet. */
    ~ErrorStack();

    /** Returns if this return code is not ERROR_CODE_OK. */
    bool                is_error() const;

    /** Return the integer error code. */
    ErrorCode           get_error_code() const;

    /** Returns the error message inferred by the error code. */
    const char*         get_message() const;

    /** Returns the custom error message. */
    const char*         get_custom_message() const;

    /**
     * Copy the given custom message into this object.
     * This method does NOT delete the current custom_message_. The caller is responsible for that.
     */
    void                copy_custom_message(const char* message);

    /** Appends more custom error message at the end. */
    void                append_custom_message(const char* more_custom_message);

    /** Returns the depth of stack this error code has collected. */
    uint16_t            get_stack_depth() const;

    /** Returns the line number of the given stack position. */
    uint16_t            get_linenum(uint16_t stack_index) const;

    /** Returns the file name of the given stack position. */
    const char*         get_filename(uint16_t stack_index) const;

    /** Output a warning to stderr if the error is not checked yet. */
    void                verify() const;

    /** Describe this object to the given stream. */
    void                output(std::ostream* ptr) const;

    /** Describe this object to std::cerr and then abort. */
    void                dump_and_abort(const char *abort_message) const;

 private:
    /**
     * @brief Filenames of stacktraces.
     * @details
     * This is deep-first, so _filenames[0] is where the ErrorStack was initially instantiated.
     * When we reach MAX_STACK_DEPTH, we don't store any more stacktraces and
     * just say ".. more" in the output.
     * We do NOT deep-copy the strings, assuming the file name string is const and
     * permanent. We only copy the pointers when passing around.
     * As far as we use "__FILE__" macro to get file name, this is the always case.
     */
    const char*     filenames_[MAX_STACK_DEPTH];

    /** @brief Line numbers of stacktraces. */
    uint16_t        linenums_[MAX_STACK_DEPTH];

    /**
     * @brief Optional custom error message.
     * We deep-copy this string if it's non-NULL.
     * The reason why we don't use auto_ptr etc for this is that they are also expensive and will
     * screw things up if someone misuse our class. Custom error message should be rare, anyways.
     */
    const char*     custom_message_;

    /**
     * @brief Integer error code.
     * @section Invariant Important invariants
     * If this value is ERROR_CODE_OK, all other members have no meanings and we might not even
     * bother clearing them for better performance because that's by far the common case.
     * So, all functions in this class should first check if this value is ERROR_CODE_OK or not
     * to avoid further processing.
     */
    ErrorCode      error_code_;

    /**
     * @brief Current stack depth.
     * Value 0 implies that we don't pass around stacktrace for this return code,
     * bypassing stacktrace collection.
     */
    uint16_t        stack_depth_;

    /** @brief Whether someone already checked the error code of this object. */
    mutable bool    checked_;
};

/**
 * @ingroup ERRORCODES
 * @brief Normal return value for no-error case.
 * @details
 * Const return code that indicates no error.
 * This is the normal way to return from a method or function.
 */
const ErrorStack RET_OK;

inline ErrorStack::ErrorStack()
    : custom_message_(NULL), error_code_(ERROR_CODE_OK), stack_depth_(0), checked_(true) {
}

inline ErrorStack::ErrorStack(ErrorCode code)
    : custom_message_(NULL), error_code_(code), stack_depth_(0), checked_(false) {
}

inline ErrorStack::ErrorStack(const char* filename, uint32_t linenum, ErrorCode code,
                              const char* custom_message)
    : error_code_(code), stack_depth_(1), checked_(false) {
    assert(code != ERROR_CODE_OK);
    filenames_[0] = filename;
    linenums_[0] = linenum;
    copy_custom_message(custom_message);
}

inline ErrorStack::ErrorStack(const ErrorStack &other) {
    operator=(other);
}

inline void ErrorStack::copy_custom_message(const char* message) {
    // Invariant: if ERROR_CODE_OK, no more processing
    if (error_code_ == ERROR_CODE_OK) {
        return;
    }

    if (message != NULL) {
        // do NOT use strdup to make sure new/delete everywhere.
        size_t len = std::strlen(message);
        char *copied = new char[len + 1];  // +1 for null terminator
        custom_message_ = copied;
        std::memcpy(copied, message, len + 1);
    } else {
        custom_message_ = message;
    }
}
inline ErrorStack& ErrorStack::operator=(ErrorStack const &other) {
    // Invariant: if ERROR_CODE_OK, no more processing
    if (other.error_code_ == ERROR_CODE_OK) {
        this->error_code_ = ERROR_CODE_OK;
        return *this;
    }

    // As we don't have any linked-list etc, mostly this is enough. Quick.
    std::memcpy(this, &other, sizeof(ErrorStack));  // we take copy BEFORE mark other checked
    other.checked_ = true;
    copy_custom_message(other.custom_message_);  // except custom error message
    return *this;
}

inline ErrorStack::ErrorStack(const ErrorStack &other, const char* filename, uint32_t linenum,
                              const char* more_custom_message) {
    // Invariant: if ERROR_CODE_OK, no more processing
    if (other.error_code_ == ERROR_CODE_OK) {
        this->error_code_ = ERROR_CODE_OK;
        return;
    }

    operator=(other);
    // augment stacktrace
    if (stack_depth_ != 0 && stack_depth_ < MAX_STACK_DEPTH) {
        filenames_[stack_depth_] = filename;
        linenums_[stack_depth_] = linenum;
        ++stack_depth_;
    }
    // augment custom error message
    if (more_custom_message != NULL) {
        append_custom_message(more_custom_message);
    }
}

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
inline ErrorStack::ErrorStack(ErrorStack &&other) {
    // Invariant: if ERROR_CODE_OK, no more processing
    if (other.error_code_ == ERROR_CODE_OK) {
        this->error_code_ = ERROR_CODE_OK;
        return;
    }

    std::memcpy(this, &other, sizeof(ErrorStack));
    other.checked_ = true;
    other.custom_message_ = NULL;  // simply stolen. much more efficient.
}
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

inline ErrorStack::~ErrorStack() {
    // Invariant: if ERROR_CODE_OK, no more processing
    if (error_code_ == ERROR_CODE_OK) {
        return;
    }
#ifdef DEBUG
    // We output warning if some error code is not checked, but we don't do so in release mode.
    verify();
#endif  // DEBUG
    if (custom_message_ != NULL) {
        delete[] custom_message_;
        custom_message_ = NULL;
    }
}

inline void ErrorStack::append_custom_message(const char* more_custom_message) {
    // Invariant: if ERROR_CODE_OK, no more processing
    if (error_code_ == ERROR_CODE_OK) {
        return;
    }
    // augment custom error message
    if (custom_message_ != NULL) {
        // concat
        size_t cur_len = std::strlen(custom_message_);
        size_t more_len = std::strlen(more_custom_message);
        char *copied = new char[cur_len + more_len + 1];
        custom_message_ = copied;
        std::memcpy(copied, custom_message_, cur_len);
        std::memcpy(copied + cur_len, more_custom_message, more_len + 1);
    } else {
        copy_custom_message(more_custom_message);  // just put the new message
    }
}

inline bool ErrorStack::is_error() const {
    checked_ = true;
    return error_code_ != ERROR_CODE_OK;
}

inline ErrorCode ErrorStack::get_error_code() const {
    checked_ = true;
    return error_code_;
}

inline const char* ErrorStack::get_message() const {
    return get_error_message(error_code_);
}

inline const char* ErrorStack::get_custom_message() const {
    // Invariant: if ERROR_CODE_OK, no more processing
    if (error_code_ == ERROR_CODE_OK) {
        return NULL;
    }
    return custom_message_;
}

inline uint16_t ErrorStack::get_stack_depth() const {
    // Invariant: if ERROR_CODE_OK, no more processing
    if (error_code_ == ERROR_CODE_OK) {
        return 0;
    }
    return stack_depth_;
}

inline uint16_t ErrorStack::get_linenum(uint16_t stack_index) const {
    // Invariant: if ERROR_CODE_OK, no more processing
    if (error_code_ == ERROR_CODE_OK) {
        return 0;
    }
    assert(stack_index < stack_depth_);
    return linenums_[stack_index];
}

inline const char* ErrorStack::get_filename(uint16_t stack_index) const {
    // Invariant: if ERROR_CODE_OK, no more processing
    if (error_code_ == ERROR_CODE_OK) {
        return NULL;
    }
    assert(stack_index < stack_depth_);
    return filenames_[stack_index];
}

inline void ErrorStack::verify() const {
    // Invariant: if ERROR_CODE_OK, no more processing
    if (error_code_ == ERROR_CODE_OK) {
        return;
    }
    if (!checked_) {
        dump_and_abort("Return value is not checked. ErrorStack must be checked");
    }
}

}  // namespace foedus

std::ostream& operator<<(std::ostream& o, const foedus::ErrorStack& obj);

// The followings are macros. So, they belong to no namespaces.

/**
 * @def ERROR_STACK(e)
 * @ingroup ERRORCODES
 * @brief Instantiates ErrorStack with the given foedus::error_code,
 * creating an error stack with the current file, line, and error code.
 * @details
 * For example, use it as follows:
 * @code{.cpp}
 * ErrorStack your_func() {
 *   if (out-of-memory-observed) {
 *      return ERROR_STACK(ERROR_CODE_OUTOFMEMORY);
 *   }
 *   return RET_OK;
 * }
 * @endcode
 */
#define ERROR_STACK(e)      foedus::ErrorStack(__FILE__, __LINE__, e)

/**
 * @def ERROR_STACK_MSG(e, m)
 * @ingroup ERRORCODES
 * @brief Overload of ERROR_STACK(e) to receive a custom error message.
 * @details
 * For example, use it as follows:
 * @code{.cpp}
 * ErrorStack your_func() {
 *   if (out-of-memory-observed) {
 *      std::string additional_message = ...;
 *      return ERROR_STACK_MSG(ERROR_CODE_OUTOFMEMORY, additional_message.c_str());
 *   }
 *   return RET_OK;
 * }
 * @endcode
 */
#define ERROR_STACK_MSG(e, m)   foedus::ErrorStack(__FILE__, __LINE__, e, m)

/**
 * @def CHECK(x)
 * @ingroup ERRORCODES
 * @brief
 * This macro calls \b x and checks its returned value.  If an error is encountered, it
 * immediately returns from the current function or method, augmenting
 * the stack trace held by the return code.
 * For example, use it as follows:
 * @code{.cpp}
 * ErrorStack your_func() {
 *   CHECK(another_func());
 *   CHECK(yet_another_func());
 *   return RET_OK;
 * }
 * @endcode
 */
#define CHECK(x)\
{\
    foedus::ErrorStack __e(x);\
    if (__e.is_error()) {return foedus::ErrorStack(__e, __FILE__, __LINE__);}\
}

/**
 * @def CHECK_MSG(x, m)
 * @ingroup ERRORCODES
 * @brief Overload of CHECK(x) to receive a custom error message.
 * For example, use it as follows:
 * @code{.cpp}
 * ErrorStack your_func() {
 *   CHECK_MSG(another_func(), "I was doing xxx");
 *   CHECK_MSG(yet_another_func(), "I was doing yyy");
 *   return RET_OK;
 * }
 * @endcode
 */
#define CHECK_MSG(x, m)\
{\
    foedus::ErrorStack __e(x);\
    if (__e.is_error()) {return foedus::ErrorStack(__e, __FILE__, __LINE__, m);}\
}

/**
 * @def COERCE(x)
 * @ingroup ERRORCODES
 * @brief
 * This macro calls \b x and aborts if encounters an error.
 * This should be used only in places that expects no error.
 * For example, use it as follows:
 * @code{.cpp}
 * void YourThread::run() {
 *   // the signature of thread::run() is defined elsewhere, so you can't return ErrorStack.
 *   // and you are sure an error won't happen here, or an error would be anyway catastrophic.
 *   COERCE(another_func());
 * }
 * @endcode
 */
#define COERCE(x)\
{\
    foedus::ErrorStack __e(x);\
    if (__e.is_error()) {__e.output_and_abort(&std::cerr, "Unexpected error happened");}\
}

#endif  // FOEDUS_ERROR_STACK_HPP_

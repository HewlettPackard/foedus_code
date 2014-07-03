/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ERROR_STACK_HPP_
#define FOEDUS_ERROR_STACK_HPP_

#include <errno.h>
#include <stdint.h>

#include <cstring>
#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/compiler.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/error_code.hpp"

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
 * In most places, you should use kRetOk, CHECK_ERROR(x), or ERROR_STACK(e) to handle this class.
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
 * enough for debugging; \ref kMaxStackDepth.
 * We then store just line numbers and const pointers to file names. No heap allocation.
 * The only thing that has to be allocated on heap is a custom error message.
 * However, there are not many places that use custom messages, so the cost usually doesn't happen.
 *
 * @par Moveable/Copiable
 * This object is \e copiable. Further, the copy constructor and copy assignment operator
 * are equivalent to \e move. Although they take a const reference, we \e steal its
 * checked_ and custom_message_. This might be confusing, but much more efficient without C++11.
 * As this object is copied so many times, we take this approach.
 *
 * This class is header-only \b except output(), dump_and_abort(), and std::ostream redirect.
 */
class ErrorStack {
 public:
  /** Constant values. */
  enum Constants {
     /** Maximum stack trace depth. */
     kMaxStackDepth = 8,
  };

  /** Empty constructor. This is same as duplicating kRetOk. */
  ErrorStack();

  /**
   * @brief Instantiate a return code without a custom error message nor stacktrace.
   * @param[in] code Error code, either kErrorCodeOk or real errors.
   * @details
   * This is the most (next to kRetOk) light-weight way to create/propagate a return code.
   * Use this one if you do not need a detail information to debug the error (eg, error whose
   * cause is obvious, an expected error that is immediately caught, etc).
   */
  explicit ErrorStack(ErrorCode code);

  /**
   * @brief Instantiate a return code with stacktrace and optionally a custom error message.
   * @param[in] filename file name of the current place.
   *   It must be a const and permanent string, such as what "__FILE__" returns. Note that we do
   * NOT do deep-copy of the strings.
   * @param[in] func functiona name of the current place.
   *   Must be a const-permanent as well, such as "__FUNCTION__" of gcc/MSVC or C++11's __func__.
   * @param[in] linenum line number of the current place. Usually "__LINE__".
   * @param[in] code Error code, must be real errors.
   * @param[in] custom_message Optional custom error message in addition to the default one
   * inferred from error code. If you pass a non-NULL string to this argument,
   * we do deep-copy, so it's EXPENSIVE!
   */
  ErrorStack(const char* filename, const char* func, uint32_t linenum, ErrorCode code,
        const char* custom_message = CXX11_NULLPTR);

  /** Copy constructor. */
  ErrorStack(const ErrorStack &other);

  /** Copy constructor to augment the stacktrace. */
  ErrorStack(const ErrorStack &other, const char* filename, const char* func, uint32_t linenum,
        const char* more_custom_message = CXX11_NULLPTR);

  /** Assignment operator. */
  ErrorStack& operator=(const ErrorStack &other);

  /** Will warn in stderr if the error code is not checked yet. */
  ~ErrorStack();

  /** Returns if this return code is not kErrorCodeOk. */
  bool                is_error() const;

  /** Return the integer error code. */
  ErrorCode           get_error_code() const;

  /** Returns the error message inferred by the error code. */
  const char*         get_message() const;

  /** Returns the custom error message. */
  const char*         get_custom_message() const;

  /**
   * Copy the given custom message into this object.
   */
  void                copy_custom_message(const char* message);

  /** Deletes custom message from this object. */
  void                clear_custom_message();

  /** Appends more custom error message at the end. */
  void                append_custom_message(const char* more_custom_message);

  /** Returns the depth of stack this error code has collected. */
  uint16_t            get_stack_depth() const;

  /** Returns the line number of the given stack position. */
  uint32_t            get_linenum(uint16_t stack_index) const;

  /** Returns the file name of the given stack position. */
  const char*         get_filename(uint16_t stack_index) const;

  /** Returns the function name of the given stack position. */
  const char*         get_func(uint16_t stack_index) const;

  /** Output a warning to stderr if the error is not checked yet. */
  void                verify() const;

  /** Describe this object to the given stream. */
  void                output(std::ostream* ptr) const;

  /** Describe this object to std::cerr and then abort. */
  void                dump_and_abort(const char *abort_message) const;

  friend std::ostream& operator<<(std::ostream& o, const ErrorStack& obj);

 private:
  /**
   * @brief Filenames of stacktraces.
   * @details
   * This is deep-first, so _filenames[0] is where the ErrorStack was initially instantiated.
   * When we reach kMaxStackDepth, we don't store any more stacktraces and
   * just say ".. more" in the output.
   * We do NOT deep-copy the strings, assuming the file name string is const and
   * permanent. We only copy the pointers when passing around.
   * As far as we use "__FILE__" macro to get file name, this is the always case.
   */
  const char*     filenames_[kMaxStackDepth];

  /** @brief Functions of stacktraces (no deep-copy as well). */
  const char*     funcs_[kMaxStackDepth];

  /** @brief Line numbers of stacktraces. */
  uint32_t        linenums_[kMaxStackDepth];

  /**
   * @brief Optional custom error message.
   * We deep-copy this string if it's non-NULL.
   * The reason why we don't use auto_ptr etc for this is that they are also expensive and will
   * screw things up if someone misuse our class. Custom error message should be rare, anyways.
   */
  mutable const char*     custom_message_;

  /**
   * @brief Global errno set by a failed system call.
   * @details
   * This value is retrieved from the \e global errno when this stack is instantiated.
   * As it's a global variable, it might be not related to the actual error of this stack
   * (previous error somewhere else).
   */
  int             os_errno_;

  /**
   * @brief Integer error code.
   * @invariant
   * If this value is kErrorCodeOk, all other members have no meanings and we might not even
   * bother clearing them for better performance because that's by far the common case.
   * So, all functions in this class should first check if this value is kErrorCodeOk or not
   * to avoid further processing.
   */
  ErrorCode       error_code_;

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
 * @var kRetOk
 * @ingroup ERRORCODES
 * @brief Normal return value for no-error case.
 * @details
 * Const return code that indicates no error.
 * This is the normal way to return from a method or function.
 */
const ErrorStack kRetOk;

inline ErrorStack::ErrorStack()
  : custom_message_(CXX11_NULLPTR), os_errno_(0), error_code_(kErrorCodeOk),
    stack_depth_(0), checked_(true) {
}

inline ErrorStack::ErrorStack(ErrorCode code)
  : custom_message_(CXX11_NULLPTR), os_errno_(errno), error_code_(code),
    stack_depth_(0), checked_(false) {
}

inline ErrorStack::ErrorStack(const char* filename, const char* func, uint32_t linenum,
                ErrorCode code, const char* custom_message)
  : custom_message_(CXX11_NULLPTR), os_errno_(errno), error_code_(code), stack_depth_(1),
    checked_(false) {
  ASSERT_ND(code != kErrorCodeOk);
  filenames_[0] = filename;
  funcs_[0] = func;
  linenums_[0] = linenum;
  copy_custom_message(custom_message);
}

inline ErrorStack::ErrorStack(const ErrorStack &other)
  : custom_message_(CXX11_NULLPTR) {
  operator=(other);
}

inline ErrorStack::ErrorStack(const ErrorStack &other, const char* filename,
              const char* func, uint32_t linenum, const char* more_custom_message)
  : custom_message_(CXX11_NULLPTR) {
  // Invariant: if kErrorCodeOk, no more processing
  if (LIKELY(other.error_code_ == kErrorCodeOk)) {
    this->error_code_ = kErrorCodeOk;
    return;
  }

  operator=(other);
  // augment stacktrace
  if (stack_depth_ != 0 && stack_depth_ < kMaxStackDepth) {
    filenames_[stack_depth_] = filename;
    funcs_[stack_depth_] = func;
    linenums_[stack_depth_] = linenum;
    ++stack_depth_;
  }
  // augment custom error message
  if (more_custom_message) {
    append_custom_message(more_custom_message);
  }
}

inline ErrorStack& ErrorStack::operator=(const ErrorStack &other) {
  // Invariant: if kErrorCodeOk, no more processing
  if (LIKELY(other.error_code_ == kErrorCodeOk)) {
    this->error_code_ = kErrorCodeOk;
    return *this;
  }

  // this copy assignment is actually a move assignment.
  // checked_/custom_message_ are mutable for that.
  custom_message_ = other.custom_message_;  // steal.
  other.custom_message_ = CXX11_NULLPTR;  // simply stolen. much more efficient.
  stack_depth_ = other.stack_depth_;
  for (int i = 0; i < other.stack_depth_; ++i) {
    filenames_[i] = other.filenames_[i];
    funcs_[i] = other.funcs_[i];
    linenums_[i] = other.linenums_[i];
  }
  os_errno_ = other.os_errno_;
  error_code_ = other.error_code_;
  checked_ = false;
  other.checked_ = true;
  return *this;
}

inline ErrorStack::~ErrorStack() {
  // Invariant: if kErrorCodeOk, no more processing
  if (LIKELY(error_code_ == kErrorCodeOk)) {
    return;
  }
#ifdef DEBUG
  // We output warning if some error code is not checked, but we don't do so in release mode.
  verify();
#endif  // DEBUG
  clear_custom_message();
}


inline void ErrorStack::clear_custom_message() {
  if (UNLIKELY(custom_message_)) {
    delete[] custom_message_;
    custom_message_ = CXX11_NULLPTR;
  }
}

inline void ErrorStack::copy_custom_message(const char* message) {
  // Invariant: if kErrorCodeOk, no more processing
  if (LIKELY(error_code_ == kErrorCodeOk)) {
    return;
  }

  clear_custom_message();
  if (message) {
    // do NOT use strdup to make sure new/delete everywhere.
    size_t len = std::strlen(message);
    char *copied = new char[len + 1];  // +1 for null terminator
    if (copied) {
      custom_message_ = copied;
      std::memcpy(copied, message, len + 1);
    }
  }
}

inline void ErrorStack::append_custom_message(const char* more_custom_message) {
  // Invariant: if kErrorCodeOk, no more processing
  if (LIKELY(error_code_ == kErrorCodeOk)) {
    return;
  }
  // augment custom error message
  if (custom_message_) {
    // concat
    size_t cur_len = std::strlen(custom_message_);
    size_t more_len = std::strlen(more_custom_message);
    char *copied = new char[cur_len + more_len + 1];
    if (copied) {
      custom_message_ = copied;
      std::memcpy(copied, custom_message_, cur_len);
      std::memcpy(copied + cur_len, more_custom_message, more_len + 1);
    }
  } else {
    copy_custom_message(more_custom_message);  // just put the new message
  }
}

inline bool ErrorStack::is_error() const {
  checked_ = true;
  return error_code_ != kErrorCodeOk;
}

inline ErrorCode ErrorStack::get_error_code() const {
  checked_ = true;
  return error_code_;
}

inline const char* ErrorStack::get_message() const {
  return get_error_message(error_code_);
}

inline const char* ErrorStack::get_custom_message() const {
  // Invariant: if kErrorCodeOk, no more processing
  if (error_code_ == kErrorCodeOk) {
    return CXX11_NULLPTR;
  }
  return custom_message_;
}

inline uint16_t ErrorStack::get_stack_depth() const {
  // Invariant: if kErrorCodeOk, no more processing
  if (error_code_ == kErrorCodeOk) {
    return 0;
  }
  return stack_depth_;
}

inline uint32_t ErrorStack::get_linenum(uint16_t stack_index) const {
  // Invariant: if kErrorCodeOk, no more processing
  if (error_code_ == kErrorCodeOk) {
    return 0;
  }
  ASSERT_ND(stack_index < stack_depth_);
  return linenums_[stack_index];
}

inline const char* ErrorStack::get_filename(uint16_t stack_index) const {
  // Invariant: if kErrorCodeOk, no more processing
  if (error_code_ == kErrorCodeOk) {
    return CXX11_NULLPTR;
  }
  ASSERT_ND(stack_index < stack_depth_);
  return filenames_[stack_index];
}

inline const char* ErrorStack::get_func(uint16_t stack_index) const {
  // Invariant: if kErrorCodeOk, no more processing
  if (error_code_ == kErrorCodeOk) {
    return CXX11_NULLPTR;
  }
  ASSERT_ND(stack_index < stack_depth_);
  return funcs_[stack_index];
}

inline void ErrorStack::verify() const {
  // Invariant: if kErrorCodeOk, no more processing
  if (LIKELY(error_code_ == kErrorCodeOk)) {
    return;
  }
  if (!checked_) {
    dump_and_abort("Return value is not checked. ErrorStack must be checked");
  }
}

}  // namespace foedus

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
 *      return ERROR_STACK(kErrorCodeOutofmemory);
 *   }
 *   return kRetOk;
 * }
 * @endcode
 */
#define ERROR_STACK(e)      foedus::ErrorStack(__FILE__, __FUNCTION__, __LINE__, e)

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
 *      return ERROR_STACK_MSG(kErrorCodeOutofmemory, additional_message.c_str());
 *   }
 *   return kRetOk;
 * }
 * @endcode
 */
#define ERROR_STACK_MSG(e, m)   foedus::ErrorStack(__FILE__, __FUNCTION__, __LINE__, e, m)

/**
 * @def CHECK_ERROR(x)
 * @ingroup ERRORCODES
 * @brief
 * This macro calls \b x and checks its returned value.  If an error is encountered, it
 * immediately returns from the current function or method, augmenting
 * the stack trace held by the return code.
 * For example, use it as follows:
 * @code{.cpp}
 * ErrorStack your_func() {
 *   CHECK_ERROR(another_func());
 *   CHECK_ERROR(yet_another_func());
 *   return kRetOk;
 * }
 * @endcode
 * @note The name is CHECK_ERROR, not CHECK, because Google-logging defines CHECK.
 */
#define CHECK_ERROR(x)\
{\
  foedus::ErrorStack __e(x);\
  if (UNLIKELY(__e.is_error())) {\
    return foedus::ErrorStack(__e, __FILE__, __FUNCTION__, __LINE__);\
  }\
}

/**
 * @def WRAP_ERROR_CODE(x)
 * @ingroup ERRORCODES
 * @brief
 * Same as CHECK_ERROR(x) except it receives only an error code, thus more efficient.
 * @note Unlike CHECK_ERROR_CODE(x), this returns ErrorStack.
 * @see CHECK_ERROR_CODE(x)
 */
#define WRAP_ERROR_CODE(x)\
{\
  foedus::ErrorCode __e = x;\
  if (UNLIKELY(__e != kErrorCodeOk)) {return ERROR_STACK(__e);}\
}

/**
 * @def UNWRAP_ERROR_STACK(x)
 * @ingroup ERRORCODES
 * @brief
 * Similar to WRAP_ERROR_CODE(x), but this one converts ErrorStack to ErrorCode.
 * This reduces information, so use it carefully.
 * @see WRAP_ERROR_CODE(x)
 */
#define UNWRAP_ERROR_STACK(x)\
{\
  foedus::ErrorStack __e = x;\
  if (UNLIKELY(__e.is_error())) { return __e.get_error_code(); }\
}

/**
 * @def CHECK_ERROR_MSG(x, m)
 * @ingroup ERRORCODES
 * @brief Overload of ERROR_CHECK(x) to receive a custom error message.
 * For example, use it as follows:
 * @code{.cpp}
 * ErrorStack your_func() {
 *   CHECK_ERROR_MSG(another_func(), "I was doing xxx");
 *   CHECK_ERROR_MSG(yet_another_func(), "I was doing yyy");
 *   return kRetOk;
 * }
 * @endcode
 */
#define CHECK_ERROR_MSG(x, m)\
{\
  foedus::ErrorStack __e(x);\
  if (UNLIKELY(__e.is_error())) {\
    return foedus::ErrorStack(__e, __FILE__, __FUNCTION__, __LINE__, m);\
  }\
}

/**
 * @def CHECK_OUTOFMEMORY(ptr)
 * @ingroup ERRORCODES
 * @brief
 * This macro checks if \b ptr is nullptr, and if so exists with kErrorCodeOutofmemory error stack.
 * This is useful as a null check after new/malloc. For example:
 * @code{.cpp}
 * ErrorStack your_func() {
 *   int* ptr = new int[123];
 *   CHECK_OUTOFMEMORY(ptr);
 *   ...
 *   delete[] ptr;
 *   return kRetOk;
 * }
 * @endcode
 */
#define CHECK_OUTOFMEMORY(ptr)\
if (UNLIKELY(!ptr)) {\
  return foedus::ErrorStack(__FILE__, __FUNCTION__, __LINE__, kErrorCodeOutofmemory);\
}

/**
 * @def COERCE_ERROR(x)
 * @ingroup ERRORCODES
 * @brief
 * This macro calls \b x and aborts if encounters an error.
 * This should be used only in places that expects no error.
 * For example, use it as follows:
 * @code{.cpp}
 * void YourThread::run() {
 *   // the signature of thread::run() is defined elsewhere, so you can't return ErrorStack.
 *   // and you are sure an error won't happen here, or an error would be anyway catastrophic.
 *   COERCE_ERROR(another_func());
 * }
 * @endcode
 */
#define COERCE_ERROR(x)\
{\
  foedus::ErrorStack __e(x);\
  if (UNLIKELY(__e.is_error())) {\
    __e.dump_and_abort("Unexpected error happened");\
  }\
}

#endif  // FOEDUS_ERROR_STACK_HPP_

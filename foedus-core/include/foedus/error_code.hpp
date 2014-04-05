/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ERROR_CODE_HPP_
#define FOEDUS_ERROR_CODE_HPP_

namespace foedus {

/**
 * @defgroup ERRORCODES Error codes, messages, and stacktraces
 * @brief Error codes (ErrorCode), their error messages defined in error_code.xmacro, and
 * stacktrace information (ErrorStack) returned by our API functions.
 * @details
 * @par What it is
 * We define all error codes and their error messages here.
 * Whenever you want a new error message, add a new line in error_code.xmacro like existing lines.
 * This file is completely independent and header-only. Just include this file to use.
 *
 * @par X-Macros
 * To concisely define error codes, error names, and error messages,
 * we use the so-called "X Macro" style, which doesn't require any code generation.
 * @see http://en.wikipedia.org/wiki/X_Macro
 * @see http://www.drdobbs.com/the-new-c-x-macros/184401387
 *
 * @par ErrorCode vs ErrorStack
 * ErrorCode is merely an integer to identify the type of error.
 * You can get a correponding error message and name of the error via
 * get_error_name() and get_error_message(), but you can't get stacktrace information.
 * For lightweight functions used internally, it might be enough.
 * However, public API methods might need stacktrace information for ease of use.
 * In that case, you should return ErrorStack, which additionally contains stacktrace and
 * custom error message.
 * ErrorStack is much more costly if it returns an error (if it's ERROR_CODE_OK, very efficinet)
 * and especially when it contains a custom error message (better when C++11 is enabled. See
 * ErrorStack for more details).
 *
 * @par How to use ErrorStack
 * To use ErrorStack, you should be familiar with how to use the following macros:
 * RET_OK, CHECK_ERROR(x), ERROR_STACK(e), COERCE_ERROR(x), and a few others.
 * For example, use it as follows:
 * @code{.cpp}
 * ErrorStack your_func() {
 *   if (out-of-memory-observed) {
 *      return ERROR_STACK(ERROR_CODE_OUTOFMEMORY);
 *   }
 *   CHECK_ERROR(another_func());
 *   CHECK_ERROR(yet_another_func());
 *   return RET_OK;
 * }
 * @endcode
 */
/**
 * @file error_code.xmacro
 * @ingroup ERRORCODES
 * @brief Error code/message definition in X-Macro style.
 */

#define X(a, b, c) /** c. */ a = b,
/**
 * @enum ErrorCode
 * @ingroup ERRORCODES
 * @brief Enum of error codes defined in error_code.xmacro.
 * @details
 * This is often used as a return value of lightweight functions.
 * If you need more informative information, such as error stack, use ErrorStack.
 * But, note that returning this value is MUCH more efficient.
 */
enum ErrorCode {
    /** 0 means no-error. */
    ERROR_CODE_OK = 0,
#include <foedus/error_code.xmacro> // NOLINT
};
#undef X

/**
 * @brief Returns the names of ErrorCode enum defined error_code.xmacro.
 * @ingroup ERRORCODES
 */
const char* get_error_name(ErrorCode code);

/**
 * @brief Returns the error messages corresponding to ErrorCode enum defined in error_code.xmacro.
 * @ingroup ERRORCODES
 */
const char* get_error_message(ErrorCode code);

// A bit tricky to get "a" from a in C macro.
#define X_QUOTE(str) #str
#define X_EXPAND_AND_QUOTE(str) X_QUOTE(str)
#define X(a, b, c) case a: return X_EXPAND_AND_QUOTE(a);
inline const char* get_error_name(ErrorCode code) {
    switch (code) {
        case ERROR_CODE_OK: return "ERROR_CODE_OK";
#include <foedus/error_code.xmacro> // NOLINT
    }
    return "Unexpected error code";
}
#undef X
#undef X_EXPAND_AND_QUOTE
#undef X_QUOTE

#define X(a, b, c) case a: return c;
inline const char* get_error_message(ErrorCode code) {
    switch (code) {
        case ERROR_CODE_OK: return "no_error";
#include <foedus/error_code.xmacro> // NOLINT
    }
    return "Unexpected error code";
}
#undef X
}  // namespace foedus

#endif  // FOEDUS_ERROR_CODE_HPP_

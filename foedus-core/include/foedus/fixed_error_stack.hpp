/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_FIXED_ERROR_STACK_HPP_
#define FOEDUS_FIXED_ERROR_STACK_HPP_

#include <stdint.h>

#include <iosfwd>

#include "foedus/assert_nd.hpp"
#include "foedus/cxx11.hpp"
#include "foedus/error_stack.hpp"
#include "foedus/assorted/fixed_string.hpp"

namespace foedus {

/**
 * @brief Representation of ErrorStack that can be copied to other processes and even serialized
 * to files.
 * @ingroup ERRORCODES
 * @details
 * ErrorStack contains char*, so can't be trivially passed to other processes or serialized.
 * This object is used in such a situation. It does string copying and (if too long) truncation,
 * so do not use this unless required.
 */
class FixedErrorStack {
 public:
  typedef assorted::FixedString<124> FixedFileName;
  typedef assorted::FixedString<124> FixedFuncName;
  typedef assorted::FixedString<508> FixedErrorMessage;

  /** Empty constructor. */
  FixedErrorStack() : os_errno_(0), error_code_(kErrorCodeOk), stack_depth_(0) {}
  /** Copy the content of the given ErrorStack. */
  explicit FixedErrorStack(const ErrorStack& src) { operator=(src); }

  /** Assignment operator. */
  FixedErrorStack& operator=(const ErrorStack &src);

  /** Returns if this return code is not kErrorCodeOk. */
  bool                is_error() const { return error_code_ != kErrorCodeOk; }

  /** Return the integer error code. */
  ErrorCode           get_error_code() const { return error_code_; }

  /** Returns the error message inferred by the error code. */
  const char*         get_message() const { return get_error_message(error_code_); }

  /** Returns the custom error message. */
  const FixedErrorMessage& get_custom_message() const { return custom_message_; }

  /** Returns the depth of stack this error code has collected. */
  uint16_t            get_stack_depth() const { return stack_depth_; }

  /** Returns the line number of the given stack position. */
  uint32_t            get_linenum(uint16_t stack_index) const {
    ASSERT_ND(stack_index < stack_depth_);
    return linenums_[stack_index];
  }

  /** Returns the file name of the given stack position. */
  const FixedFileName& get_filename(uint16_t stack_index) const {
    ASSERT_ND(stack_index < stack_depth_);
    return filenames_[stack_index];
  }

  /** Returns the function name of the given stack position. */
  const FixedFuncName& get_func(uint16_t stack_index) const {
    ASSERT_ND(stack_index < stack_depth_);
    return funcs_[stack_index];
  }

  /** Describe this object to the given stream. */
  void                output(std::ostream* ptr) const;

  void                clear() { error_code_ = kErrorCodeOk; }

  /**
   * Instantiates an ErrorStack object based on this object.
   * We can't generate a completely same object (ErrorStack needs const char* pointers),
   * so we put much of information as custom error message.
   */
  ErrorStack          to_error_stack() const;

  friend std::ostream& operator<<(std::ostream& o, const FixedErrorStack& obj);

 private:
  /** @copydoc foedus::ErrorStack::filenames_ */
  FixedFileName   filenames_[ErrorStack::kMaxStackDepth];

  /** @copydoc foedus::ErrorStack::funcs_ */
  FixedFuncName   funcs_[ErrorStack::kMaxStackDepth];

  /** @copydoc foedus::ErrorStack::linenums_ */
  uint32_t        linenums_[ErrorStack::kMaxStackDepth];

  /** @copydoc foedus::ErrorStack::custom_message_ */
  FixedErrorMessage custom_message_;

  /** @copydoc foedus::ErrorStack::os_errno_ */
  int             os_errno_;

  /** @copydoc foedus::ErrorStack::error_code_ */
  ErrorCode       error_code_;

  /** @copydoc foedus::ErrorStack::stack_depth_ */
  uint16_t        stack_depth_;
};

}  // namespace foedus
#endif  // FOEDUS_FIXED_ERROR_STACK_HPP_

/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_ERROR_STACK_BATCH_HPP_
#define FOEDUS_ERROR_STACK_BATCH_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_stack.hpp>
#include <iosfwd>
#include <vector>
namespace foedus {
/**
 * @brief Batches zero or more ErrorStack objects to represent in one ErrorStack.
 * @ingroup ERRORCODES
 * @details
 * This batching is useful in a context that might observe multiple errors while it can return
 * only one ErrorStack object; e.g., Initializable#uninitialize().
 */
class ErrorStackBatch {
 public:
  ErrorStackBatch() {}
  /** Non-move copy constructor. */
  ErrorStackBatch(const ErrorStackBatch &other) : error_batch_(other.error_batch_) {}

  /** Non-move assignment. */
  ErrorStackBatch& operator=(const ErrorStackBatch &other) {
    error_batch_ = other.error_batch_;
    return *this;
  }

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  /**
   * Move constructor that steals the internal std::vector without copying.
   * This is more efficient than non-move copy constructor,
   * but provided only when C++11 is supported.
   */
  ErrorStackBatch(ErrorStackBatch &&other) {
    error_batch_ = std::move(other.error_batch_);
  }

  /**
   * Move assignment that steals the internal std::vector without copying.
   * This is more efficient, but provided only when C++11 is supported.
   */
  ErrorStackBatch& operator=(ErrorStackBatch &&other) {
    error_batch_ = std::move(other.error_batch_);
    return *this;
  }
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

  void clear() { error_batch_.clear(); }

  /**
   * If the given ErrorStack is an error, this method adds it to the end of this batch.
   */
  void push_back(const ErrorStack &error_stack) {
    if (!error_stack.is_error()) {
      return;
    }
    error_batch_.push_back(error_stack);
  }

#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
  /**
   * If the given ErrorStack is an error, this method adds it to the end of this batch.
   * This method is more efficient than push_back() but provided only when C++11 is supported.
   */
  void emprace_back(ErrorStack &&error_stack) {
    if (!error_stack.is_error()) {
      return;
    }
    error_batch_.emplace_back(error_stack);
  }
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS

  /** Returns whether there was any error. */
  bool        is_error() const { return !error_batch_.empty(); }

  /**
   * A convenience method to uninitialize and delete all Initializable objects in a vector,
   * storing all errors in this batch.
   */
  template<class T>
  void        uninitialize_and_delete_all(std::vector< T* > *vec) {
    while (!vec->empty()) {
#ifndef DISABLE_CXX11_IN_PUBLIC_HEADERS
      emprace_back(vec->back()->uninitialize());
#else   // DISABLE_CXX11_IN_PUBLIC_HEADERS
      push_back(vec->back()->uninitialize());
#endif  // DISABLE_CXX11_IN_PUBLIC_HEADERS
      delete vec->back();
      vec->pop_back();
    }
  }

  /**
   * Instantiate an ErrorStack object that summarizes all errors in this batch.
   * Consider using SUMMARIZE_ERROR_BATCH(batch).
   */
  ErrorStack  summarize(const char* filename, const char* func, uint32_t linenum) const;

  friend std::ostream& operator<<(std::ostream& o, const ErrorStackBatch& obj);

 private:
  /**
   * Error results this batch has collected.
   */
  std::vector<ErrorStack> error_batch_;
};
}  // namespace foedus

/**
 * @def SUMMARIZE_ERROR_BATCH(batch)
 * @ingroup ERRORCODES
 * @brief
 * This macro calls ErrorStackBatch#summarize() with automatically provided parameters.
 * See \ref INITIALIZABLE for an example usage.
 */
#define SUMMARIZE_ERROR_BATCH(x) x.summarize(__FILE__, __FUNCTION__, __LINE__)

#endif  // FOEDUS_ERROR_STACK_BATCH_HPP_

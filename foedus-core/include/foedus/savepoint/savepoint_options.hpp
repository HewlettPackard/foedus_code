/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SAVEPOINT_SAVEPOINT_OPTIONS_HPP_
#define FOEDUS_SAVEPOINT_SAVEPOINT_OPTIONS_HPP_
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/externalize/externalizable.hpp"

namespace foedus {
namespace savepoint {
/**
 * @brief Set of options for savepoint manager.
 * @ingroup SAVEPOINT
 * @details
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct SavepointOptions CXX11_FINAL : public virtual externalize::Externalizable {
  /**
   * Constructs option values with default values.
   */
  SavepointOptions();

  /**
   * @brief Full path of the savepoint file.
   * @details
   * This file is atomically and durably updated for each epoch-based commit.
   * Default is "savepoint.xml".
   */
  std::string savepoint_path_;

  EXTERNALIZABLE(SavepointOptions);
};
}  // namespace savepoint
}  // namespace foedus
#endif  // FOEDUS_SAVEPOINT_SAVEPOINT_OPTIONS_HPP_

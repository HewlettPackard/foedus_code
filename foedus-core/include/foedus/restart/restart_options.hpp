/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_RESTART_RESTART_OPTIONS_HPP_
#define FOEDUS_RESTART_RESTART_OPTIONS_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/externalize/externalizable.hpp>
namespace foedus {
namespace restart {
/**
 * @brief Set of options for restart manager.
 * @ingroup RESTART
 * This is a POD struct. Default destructor/copy-constructor/assignment operator work fine.
 */
struct RestartOptions CXX11_FINAL : public virtual externalize::Externalizable {
  /**
   * Constructs option values with default values.
   */
  RestartOptions();

  EXTERNALIZABLE(RestartOptions);
};
}  // namespace restart
}  // namespace foedus
#endif  // FOEDUS_RESTART_RESTART_OPTIONS_HPP_

/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MODULE_TYPE_HPP_
#define FOEDUS_MODULE_TYPE_HPP_

namespace foedus {
/**
 * Enumerates modules in FOEDUS engine. In initialization order.
 * @ingroup ENGINE
 */
enum ModuleType {
  kInvalid = 0,
  kSoc,
  kDebug,
  kProc,
  kMemory,
  kSavepoint,
  kThread,
  kLog,
  kSnapshot,
  kStorage,
  kXct,
  kRestart,
  kDummyTail,
};
}  // namespace foedus
#endif  // FOEDUS_MODULE_TYPE_HPP_

/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_XCT_FWD_HPP_
#define FOEDUS_XCT_FWD_HPP_
/**
 * @file foedus/xct/fwd.hpp
 * @brief Forward declarations of classes in transaction package.
 * @ingroup XCT
 */
namespace foedus {
namespace xct {
class   CombinedLock;
struct  InCommitEpochGuard;
struct  LockableXctId;
struct  LockFreeWriteXctAccess;
struct  McsBlock;
struct  McsLock;
struct  McsLockScope;
struct  PointerAccess;
struct  ReadXctAccess;
struct  WriteXctAccess;
class   Xct;
struct  XctId;
class   XctManager;
struct  XctManagerControlBlock;
class   XctManagerPimpl;
}  // namespace xct
}  // namespace foedus
#endif  // FOEDUS_XCT_FWD_HPP_

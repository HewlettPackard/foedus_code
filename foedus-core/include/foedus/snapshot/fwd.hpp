/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_SNAPSHOT_FWD_HPP_
#define FOEDUS_SNAPSHOT_FWD_HPP_
/**
 * @file foedus/snapshot/fwd.hpp
 * @brief Forward declarations of classes in snapshot manager package.
 * @ingroup SNAPSHOT
 */
namespace foedus {
namespace snapshot {
class   InMemorySortedBuffer;
class   DumpFileSortedBuffer;
struct  LogBuffer;
class   LogGleaner;
struct  LogGleanerControlBlock;
class   LogGleanerRef;
class   LogMapper;
class   LogReducer;
struct  LogReducerControlBlock;
class   LogReducerRef;
class   MapReduceBase;
class   MergeSort;
struct  NumaThreadScope;
struct  Snapshot;
class   SnapshotManager;
struct  SnapshotManagerControlBlock;
class   SnapshotManagerPimpl;
struct  SnapshotMetadata;
struct  SnapshotOptions;
class   SnapshotWriter;
class   SortedBuffer;
}  // namespace snapshot
}  // namespace foedus
#endif  // FOEDUS_SNAPSHOT_FWD_HPP_

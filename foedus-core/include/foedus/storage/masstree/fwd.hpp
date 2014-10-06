/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_MASSTREE_FWD_HPP_
#define FOEDUS_STORAGE_MASSTREE_FWD_HPP_
/**
 * @file foedus/storage/masstree/fwd.hpp
 * @brief Forward declarations of classes in masstree storage package.
 * @ingroup MASSTREE
 */
namespace foedus {
namespace storage {
namespace masstree {
class   MasstreeBorderPage;
struct  MasstreeCreateLogType;
class   MasstreeCursor;
struct  MasstreeDeleteLogType;
struct  MasstreeInsertLogType;
class   MasstreeIntermediatePage;
struct  MasstreeMetadata;
struct  MasstreeOverwriteLogType;
class   MasstreePage;
class   MasstreePartitioner;
struct  MasstreePartitionerData;
struct  MasstreePartitionerInDesignData;
class   MasstreeStorage;
struct  MasstreeStorageControlBlock;
class   MasstreeStorageFactory;
class   MasstreeStoragePimpl;
}  // namespace masstree
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_MASSTREE_FWD_HPP_

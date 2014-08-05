/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_FWD_HPP_
#define FOEDUS_STORAGE_FWD_HPP_
/**
 * @file foedus/storage/fwd.hpp
 * @brief Forward declarations of classes in storage package.
 * @ingroup STORAGE
 */
namespace foedus {
namespace storage {
class   Composer;
struct  DropLogType;
struct  DualPagePointer;
struct  DummyVolatilePageInitializer;
struct  Metadata;
struct  Page;
struct  PageVersion;
class   Partitioner;
struct  Record;
class   Storage;
class   StorageFactory;
class   StorageManager;
class   StorageManagerPimpl;
struct  VolatilePageInitializer;
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_FWD_HPP_

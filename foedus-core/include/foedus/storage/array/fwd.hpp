/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_STORAGE_ARRAY_FWD_HPP_
#define FOEDUS_STORAGE_ARRAY_FWD_HPP_
/**
 * @file foedus/storage/array/fwd.hpp
 * @brief Forward declarations of classes in array storage package.
 * @ingroup ARRAY
 */
namespace foedus {
namespace storage {
namespace array {
struct  ArrayCreateLogType;
struct  ArrayIncrementLogType;
struct  ArrayMetadata;
struct  ArrayOverwriteLogType;
class   ArrayPage;
class   ArrayPartitioner;
struct  ArrayRange;
class   ArrayStorage;
struct  ArrayStorageCache;
struct  ArrayStorageControlBlock;
class   ArrayStorageFactory;
class   ArrayStoragePimpl;
class   LookupRouteFinder;
}  // namespace array
}  // namespace storage
}  // namespace foedus
#endif  // FOEDUS_STORAGE_ARRAY_FWD_HPP_

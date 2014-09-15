/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_MEMORY_FWD_HPP_
#define FOEDUS_MEMORY_FWD_HPP_
/**
 * @file foedus/memory/fwd.hpp
 * @brief Forward declarations of classes in memory package.
 * @ingroup MEMORY
 */
namespace foedus {
namespace memory {
class   AlignedMemory;
struct  AlignedMemorySlice;
class   EngineMemory;
struct  GlobalVolatilePageResolver;
struct  LocalPageResolver;
struct  MemoryOptions;
class   NumaCoreMemory;
class   NumaNodeMemory;
class   NumaNodeMemoryRef;
class   PagePoolOffsetChunk;
class   PagePool;
struct  PagePoolControlBlock;
class   PagePoolPimpl;
class   PageReleaseBatch;
class   PageResolver;
class   RoundRobinPageGrabBatch;
class   SharedMemory;
}  // namespace memory
}  // namespace foedus
#endif  // FOEDUS_MEMORY_FWD_HPP_

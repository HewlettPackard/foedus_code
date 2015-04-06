/*
 * Copyright (c) 2014-2015, Hewlett-Packard Development Company, LP.
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details. You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * HP designates this particular file as subject to the "Classpath" exception
 * as provided by HP in the LICENSE.txt file that accompanied this code.
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
class   PagePoolOffsetAndEpochChunk;
class   PagePoolOffsetChunk;
class   PagePoolOffsetDynamicChunk;
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

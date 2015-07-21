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
#ifndef FOEDUS_LOG_FWD_HPP_
#define FOEDUS_LOG_FWD_HPP_
/**
 * @file foedus/log/fwd.hpp
 * @brief Forward declarations of classes in log manager package.
 * @ingroup LOG
 */
namespace foedus {
namespace log {
struct  BaseLogType;
struct  EngineLogType;
struct  EpochHistory;
struct  EpochMarkerLogType;
struct  FillerLogType;
struct  LogHeader;
class   LogManager;
struct  LogManagerControlBlock;
class   LogManagerPimpl;
struct  LogOptions;
class   Logger;
class   LoggerRef;
struct  LoggerControlBlock;
class   MetaLogBuffer;
struct  MetaLogControlBlock;
class   MetaLogger;
struct  RecordLogType;
struct  StorageLogType;
class   ThreadEpockMark;
class   ThreadLogBuffer;
class   ThreadLogBufferMeta;
}  // namespace log
}  // namespace foedus
#endif  // FOEDUS_LOG_FWD_HPP_

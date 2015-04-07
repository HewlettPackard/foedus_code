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
#ifndef FOEDUS_FS_FILESYSTEM_HPP_
#define FOEDUS_FS_FILESYSTEM_HPP_
#include <iosfwd>
#include <string>

#include "foedus/cxx11.hpp"
#include "foedus/error_code.hpp"
#include "foedus/fwd.hpp"
#include "foedus/initializable.hpp"
#include "foedus/assorted/fixed_string.hpp"
#include "foedus/fs/path.hpp"

namespace foedus {
namespace fs {

/**
 * @enum FileType
 * @brief Analogue of boost::filesystem::file_type.
 * @ingroup FILESYSTEM
 */
enum FileType {
  kStatusError = 0,
  kFileNotFound,
  kRegularFile,
  kDirectoryFile,
  kTypeUnknown,
};

/**
 * @enum FilePermission
 * @brief Analogue of boost::filesystem::perm.
 * @ingroup FILESYSTEM
 */
enum FilePermission {
  kNoPerms = 0,       // kFileNotFound is kNoPerms rather than kPermsNotKnown

  // POSIX equivalent macros given in comments.
  // Values are from POSIX and are given in octal per the POSIX standard.

  // permission bits

  kOwnerRead = 0400,      // S_IRUSR, Read permission, owner
  kOwnerWrite = 0200,     // S_IWUSR, Write permission, owner
  kOwnerExe = 0100,       // S_IXUSR, Execute/search permission, owner
  kOwnerAll = 0700,       // S_IRWXU, Read, write, execute/search by owner

  kGroupRead = 040,       // S_IRGRP, Read permission, group
  kGroupWrite = 020,      // S_IWGRP, Write permission, group
  kGroupExe = 010,        // S_IXGRP, Execute/search permission, group
  kGroupAll = 070,        // S_IRWXG, Read, write, execute/search by group

  kOthersRead = 04,       // S_IROTH, Read permission, others
  kOthersWrite = 02,      // S_IWOTH, Write permission, others
  kOthersExe = 01,        // S_IXOTH, Execute/search permission, others
  kOthersAll = 07,        // S_IRWXO, Read, write, execute/search by others

  kAllAll = kOwnerAll|kGroupAll|kOthersAll,  // 0777

  kPermsNotKnown = 0xFFFF,   // present when directory_entry cache not loaded
};

/**
 * @brief Represents a fixed (thus can be placed in shared memory) path string.
 * @ingroup FILESYSTEM
 */
typedef assorted::FixedString<508> FixedPath;

/**
 * @brief Analogue of boost::filesystem::file_status.
 * @ingroup FILESYSTEM
 */
struct FileStatus {
  FileStatus()            : type_(kStatusError), permissions_(kPermsNotKnown) {}
  explicit FileStatus(FileType type, FilePermission permissions = kPermsNotKnown)
                    : type_(type), permissions_(permissions) {}

  bool type_present() const       { return type_ != kStatusError; }
  bool permissions_present() const    { return permissions_ != kPermsNotKnown;}
  bool status_known() const       { return type_present() && permissions_present(); }
  bool exists() const             { return type_ != kStatusError && type_ != kFileNotFound; }
  bool is_regular_file() const    { return type_ == kRegularFile; }
  bool is_directory() const       { return type_ == kDirectoryFile; }

  FileType        type_;
  FilePermission  permissions_;
};

/**
 * @brief Analogue of boost::filesystem::space_info.
 * @ingroup FILESYSTEM
 * all values are byte counts.
 */
struct SpaceInfo  {
  uint64_t capacity_;
  /** Less than capacity_. */
  uint64_t free_;
  /** Less than free_. */
  uint64_t available_;
  friend std::ostream& operator<<(std::ostream& o, const SpaceInfo& v);
};

/**
 * Returns the status of the file.
 * @ingroup FILESYSTEM
 */
FileStatus status(const Path& p);
/**
 * Returns if the file exists.
 * @ingroup FILESYSTEM
 */
inline bool exists(const Path& p) {return status(p).exists(); }
/**
 * Returns if the file is a directory.
 * @ingroup FILESYSTEM
 */
inline bool is_directory(const Path& p) {return status(p).is_directory(); }
/**
 * Returns if the file is a regular file.
 * @ingroup FILESYSTEM
 */
inline bool is_regular_file(const Path& p) {return status(p).is_regular_file(); }

/**
 * Returns the current working directory.
 * @ingroup FILESYSTEM
 */
Path        current_path();
/**
 * Returns the absolute path of the home directory of the user running this process.
 * @ingroup FILESYSTEM
 * @details
 * So far this checks only HOME environment variable, which might not be set in some environment.
 * In that case, this returns an empty path. A truly crossplatform home_path is not in standard
 * C++, unfortunately.
 * @see http://stackoverflow.com/questions/2552416/how-can-i-find-the-users-home-dir-in-a-cross-platform-manner-using-c
 */
Path        home_path();
/**
 * Returns the absolue path of the specified path.
 * @ingroup FILESYSTEM
 */
Path        absolute(const std::string& p);

// so far not needed
// ErrorCode copy(const Path& from, const Path& to);
// ErrorCode copy_directory(const Path& from, const Path& to);
// ErrorCode copy_file(const Path& from, const Path& to);

/**
 * Recursive mkdir (mkdirs).
 * @ingroup FILESYSTEM
 * @param[in] p path of the directory to create
 * @param[in] sync (optional, default false) wheter to call fsync() on the created directories
 * and their parents. This is required to make sure the new directory entries become durable.
 * @return whether the directory already exists or creation succeeded
 */
bool        create_directories(const Path& p, bool sync = false);
/**
 * mkdir.
 * @ingroup FILESYSTEM
 * @param[in] p path of the directory to create
 * @param[in] sync (optional, default false) wheter to call fsync() on the created directory
 * and its parent. This is required to make sure the new directory entry becomes durable.
 * @return whether the directory already exists or creation whether succeeded
 */
bool        create_directory(const Path& p, bool sync = false);
/**
 * Returns size of the file.
 * @ingroup FILESYSTEM
 */
uint64_t    file_size(const Path& p);
/**
 * Deletes a regular file or an empty directory.
 * @ingroup FILESYSTEM
 * @return whether succeeded
 */
bool        remove(const Path& p);
/**
 * Recursively deletes a directory.
 * @ingroup FILESYSTEM
 * @return number of files/directories deleted.
 */
uint64_t    remove_all(const Path& p);
/**
 * Returns free space information for the device the file is on.
 * @ingroup FILESYSTEM
 */
SpaceInfo   space(const Path& p);
/**
 * Equivalent to unique_path("%%%%-%%%%-%%%%-%%%%").
 * @ingroup FILESYSTEM
 */
std::string unique_name(uint64_t differentiator = 0);
/**
 * Returns a randomly generated file name with the given template.
 * @ingroup FILESYSTEM
 * @param[in] model file name template where % will be replaced with random hex numbers.
 * @param[in] differentiator optional parameter to further randomize this method.
 * @details
 * We use std::chrono::high_resolution_clock::now() to get a random seed.
 * \b However, even high_resolution_clock sometimes has low precision depending on environment.
 * When you are concerned with a conflict (eg running many concurrent testcases), also give
 * a differentiator.
 */
std::string unique_name(const std::string& model, uint64_t differentiator = 0);

/**
 * @brief Makes the content and metadata of the file durable all the way up to devices.
 * @ingroup FILESYSTEM
 * @param[in] path path of the file to make durable
 * @param[in] sync_parent_directory (optional, default false) whether to also call fsync on
 * the parent directory to make sure the directory entry is written to device. This is required
 * when you create a new file, rename, etc.
 * @return whether the sync succeeded or not. If failed, check the errno global variable
 * (set by lower-level library).
 * @details
 * Surprisingly, there is no analogus method in boost::filesystem.
 * This method provides the fundamental building block of fault-tolerant systems; fsync.
 * We so far don't provide fdatasync (no metadata sync), but this should suffice.
 */
bool        fsync(const Path& path, bool sync_parent_directory = false);

/**
 * @brief Renames the old file to the new file with the POSIX atomic-rename semantics.
 * @ingroup FILESYSTEM
 * @param[in] old_path path of the file to rename
 * @param[in] new_path path after rename
 * @return whether the rename succeeded
 * @pre exists(old_path)
 * @pre !exists(new_path) is \b NOT a pre-condition. See below.
 * @details
 * This is analogus to boost::filesystem::rename(), but we named this atomic_rename to clarify
 * that this implementation guarantees the POSIX atomic-rename semantics.
 * When new_path already exists, this method atomically swaps the file on filesystem with
 * the old_path file, appropriately deleting the old file.
 * This is an essential semantics to achieve safe and fault-tolerant file writes.
 * And, for that usecase, do NOT forget to also call fsync before/after rename, too.
 * Use durable_atomic_rename() to make sure.
 *
 * @see http://pubs.opengroup.org/onlinepubs/009695399/functions/rename.html
 * @see Eat My Data: How Everybody Gets File IO Wrong:
 * https://www.flamingspork.com/talks/2007/06/eat_my_data.odp
 */
bool        atomic_rename(const Path& old_path, const Path& new_path);

/**
 * @brief fsync() on source file before rename, then fsync() on the parent folder after rename.
 * @ingroup FILESYSTEM
 * @details
 * This method makes 2 fsync calls, one on old file \b before rename
 * and another on the parent directory \b after rename.
 *
 * Note that we don't need fsync on parent directory before rename assuming old_path and new_path
 * is in the same folder (if not, you have to call fsync yourself before calling this method).
 * Even if a crash happens right after rename, we still see the old content of new_path.
 *
 * Also, we don't need fsync on new_path after rename because POSIX rename doesn't change
 * the inode of renamed file. It's already there as soon as parent folder's fsync is done.
 *
 * Quite complex and expensive, but this is required to make it durable regardless of filesystems.
 * Fortunately, we have to call this method only once per epoch-advance.
 */
bool        durable_atomic_rename(const Path& old_path, const Path& new_path);

/**
 * Just a synonym of atomic_rename() to avoid confusion.
 * @ingroup FILESYSTEM
 */
inline bool rename(const Path& old_path, const Path& new_path) {
  return atomic_rename(old_path, new_path);
}

}  // namespace fs
}  // namespace foedus
#endif  // FOEDUS_FS_FILESYSTEM_HPP_

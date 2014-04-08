/*
 * Copyright (c) 2014, Hewlett-Packard Development Company, LP.
 * The license and distribution terms for this file are placed in LICENSE.txt.
 */
#ifndef FOEDUS_FS_FILESYSTEM_HPP_
#define FOEDUS_FS_FILESYSTEM_HPP_
#include <foedus/cxx11.hpp>
#include <foedus/error_code.hpp>
#include <foedus/initializable.hpp>
#include <foedus/fs/filesystem_options.hpp>
#include <foedus/fs/path.hpp>
#include <iosfwd>
namespace foedus {
namespace fs {

/**
 * @enum FileType
 * @brief Analogue of boost::filesystem::file_type.
 * @ingroup FILESYSTEM
 */
enum FileType {
    status_error = 0,
    file_not_found,
    regular_file,
    directory_file,
    type_unknown,
};

/**
 * @enum FilePermission
 * @brief Analogue of boost::filesystem::perm.
 * @ingroup FILESYSTEM
 */
enum FilePermission {
    no_perms = 0,       // file_not_found is no_perms rather than perms_not_known

    // POSIX equivalent macros given in comments.
    // Values are from POSIX and are given in octal per the POSIX standard.

    // permission bits

    owner_read = 0400,      // S_IRUSR, Read permission, owner
    owner_write = 0200,     // S_IWUSR, Write permission, owner
    owner_exe = 0100,       // S_IXUSR, Execute/search permission, owner
    owner_all = 0700,       // S_IRWXU, Read, write, execute/search by owner

    group_read = 040,       // S_IRGRP, Read permission, group
    group_write = 020,      // S_IWGRP, Write permission, group
    group_exe = 010,        // S_IXGRP, Execute/search permission, group
    group_all = 070,        // S_IRWXG, Read, write, execute/search by group

    others_read = 04,       // S_IROTH, Read permission, others
    others_write = 02,      // S_IWOTH, Write permission, others
    others_exe = 01,        // S_IXOTH, Execute/search permission, others
    others_all = 07,        // S_IRWXO, Read, write, execute/search by others

    all_all = owner_all|group_all|others_all,  // 0777

    perms_not_known = 0xFFFF,   // present when directory_entry cache not loaded
};

/**
 * @brief Analogue of boost::filesystem::file_status.
 * @ingroup FILESYSTEM
 */
struct FileStatus {
    FileStatus()            : type_(status_error), permissions_(perms_not_known) {}
    explicit FileStatus(FileType type, FilePermission permissions = perms_not_known)
                                      : type_(type), permissions_(permissions) {}

    bool type_present() const       { return type_ != status_error; }
    bool permissions_present() const    { return permissions_ != perms_not_known;}
    bool status_known() const       { return type_present() && permissions_present(); }
    bool exists() const             { return type_ != status_error && type_ != file_not_found; }
    bool is_regular_file() const    { return type_ == regular_file; }
    bool is_directory() const       { return type_ == directory_file; }

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
 * @brief Analogue of boost::filesystem.
 * @ingroup FILESYSTEM
 * @details
 * This class resembles boost::filesystem namespace.
 * However, this is a class, not a namespace, to receive FilesystemOptions.
 */
class Filesystem : public virtual Initializable {
 public:
    explicit Filesystem(const FilesystemOptions& options);

    // Disable default constructors
    Filesystem() CXX11_FUNC_DELETE;
    Filesystem(const Filesystem &) CXX11_FUNC_DELETE;
    Filesystem& operator=(const Filesystem &) CXX11_FUNC_DELETE;

    // so far nothing...
    ErrorStack  initialize() CXX11_OVERRIDE             { return RET_OK; }
    bool        is_initialized() const CXX11_OVERRIDE   { return true; }
    ErrorStack  uninitialize() CXX11_OVERRIDE           { return RET_OK; }

    FileStatus status(const Path& p) const;
    bool exists(const Path& p) const {return status(p).exists(); }
    bool is_directory(const Path& p) const {return status(p).is_directory(); }
    bool is_regular_file(const Path& p) const {return status(p).is_regular_file(); }

    Path current_path() const;
    Path absolute(const Path& p) const;

    // so far not needed
    // ErrorCode copy(const Path& from, const Path& to) const;
    // ErrorCode copy_directory(const Path& from, const Path& to) const;
    // ErrorCode copy_file(const Path& from, const Path& to) const;

    bool        create_directories(const Path& p) const;
    bool        create_directory(const Path& p) const;
    uint64_t    file_size(const Path& p) const;
    bool        remove(const Path& p) const;
    uint64_t    remove_all(const Path& p) const;
    SpaceInfo   space(const Path& p) const;
    Path        unique_path()  const;
    Path        unique_path(const Path& model)  const;

 private:
    /** The only variable of this object. It's immutable. Thus, all methods are const. */
    FilesystemOptions options_;
};
}  // namespace fs
}  // namespace foedus
#endif  // FOEDUS_FS_FILESYSTEM_HPP_

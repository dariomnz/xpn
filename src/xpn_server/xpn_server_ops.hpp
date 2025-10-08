
/*
 *  Copyright 2020-2024 Felix Garcia Carballeira, Diego Camarmas Alonso, Alejandro Calderon Mateos, Dario Muñoz Muñoz
 *
 *  This file is part of Expand.
 *
 *  Expand is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Expand is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with Expand.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#pragma once

#include <dirent.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include "base_cpp/filesystem.hpp"
#include "xpn/xpn_metadata.hpp"
#include "xpn_filesystem/xpn_server_db.hpp"

/* Operations */

namespace XPN {

enum class xpn_server_ops {
    // File operations
    OPEN_FILE,
    CREAT_FILE,
    READ_FILE,
    WRITE_FILE,
    CLOSE_FILE,
    RM_FILE,
    RM_FILE_ASYNC,
    RENAME_FILE,
    GETATTR_FILE,
    SETATTR_FILE,
    REQUEST_BLOCK,

    // Directory operations
    MKDIR_DIR,
    RMDIR_DIR,
    RMDIR_DIR_ASYNC,
    OPENDIR_DIR,
    READDIR_DIR,
    CLOSEDIR_DIR,

    // FS Operations
    STATVFS_DIR,

    // Metadata
    READ_MDATA,
    WRITE_MDATA,
    WRITE_MDATA_FILE_SIZE,

    // Connection operatons
    FINALIZE,
    DISCONNECT,
    END,

    // Flush preload
    FLUSH,
    PRELOAD,

    // For enum count
    size,
};

static const std::array<std::string, static_cast<uint64_t>(xpn_server_ops::size) + 1> xpn_server_ops_names = {
    // File operations
    "OPEN_FILE",
    "CREAT_FILE",
    "READ_FILE",
    "WRITE_FILE",
    "CLOSE_FILE",
    "RM_FILE",
    "RM_FILE_ASYNC",
    "RENAME_FILE",
    "GETATTR_FILE",
    "SETATTR_FILE",
    "REQUEST_BLOCK",

    // Directory operations
    "MKDIR_DIR",
    "RMDIR_DIR",
    "RMDIR_DIR_ASYNC",
    "OPENDIR_DIR",
    "READDIR_DIR",
    "CLOSEDIR_DIR",

    // FS Operations
    "STATVFS_DIR",

    // Metadata
    "READ_MDATA",
    "WRITE_MDATA",
    "WRITE_MDATA_FILE_SIZE",

    // Connection operatons
    "FINALIZE",
    "DISCONNECT",
    "END",

    // Flush preload
    "FLUSH",
    "PRELOAD",

    // For enum count
    "size",
};

static inline const std::string &xpn_server_ops_name(xpn_server_ops op) {
    return xpn_server_ops_names[static_cast<uint64_t>(op)];
}

/* Message struct */
struct xpn_server_path {
    uint32_t size;
    char path[PATH_MAX];

    uint64_t get_size() { return offsetof(std::remove_pointer<decltype(this)>::type, path) + size; }
};

struct xpn_server_double_path {
    uint32_t size1;
    uint32_t size2;
    char path[PATH_MAX * 2];

    uint64_t get_size() { return offsetof(std::remove_pointer<decltype(this)>::type, path) + size1 + size2; }
    char *path1() { return path; }
    char *path2() { return path + size1; }
    const char *path1() const { return path; }
    const char *path2() const { return path + size1; }
};

struct st_xpn_server_status {
    int32_t ret;
    int32_t server_errno;

    uint64_t get_size() { return sizeof(*this); }
};

struct st_xpn_server_path_flags {
    int32_t flags;
    mode_t mode;
    char xpn_session;
    xpn_server_path path;

    uint64_t get_size() { return offsetof(std::remove_pointer<decltype(this)>::type, path) + path.get_size(); }
};
struct st_xpn_server_path {
    xpn_server_path path;

    uint64_t get_size() { return path.get_size(); }
};

struct st_xpn_server_close {
    int fd;
    uint64_t dir;
    char xpn_session;
    xpn_server_path path;

    uint64_t get_size() { return offsetof(std::remove_pointer<decltype(this)>::type, path) + path.get_size(); }
};

struct st_xpn_server_rw {
    int64_t offset;
    uint64_t size;
    int fd;
    char xpn_session;
    // uint64_t new_file_size;
    xpn_server_path path;

    uint64_t get_size() { return offsetof(std::remove_pointer<decltype(this)>::type, path) + path.get_size(); }
};

struct st_xpn_server_rw_req {
    int64_t size;
    struct st_xpn_server_status status;

    uint64_t get_size() { return sizeof(*this); }
};

struct st_xpn_server_rename {
    xpn_server_double_path paths;

    uint64_t get_size() { return paths.get_size(); }
};

struct st_xpn_server_request_block {
    xpn_server_db::xpn_server_block block;
    xpn_server_path path;

    uint64_t get_size() { return offsetof(std::remove_pointer<decltype(this)>::type, path) + path.get_size(); }
};

struct st_xpn_server_request_block_req {
    xpn_server_db::xpn_server_block block;
    struct st_xpn_server_status status;

    uint64_t get_size() { return sizeof(*this); }
};

#define eq_cast(val1, val2) val1 = static_cast<std::remove_reference_t<decltype(val1)>>(val2)

struct st_xpn_server_stat {
    uint64_t st_dev;
    uint64_t st_ino;
    uint64_t st_nlink;
    uint32_t st_mode;
    uint32_t st_uid;
    uint32_t st_gid;
    int32_t __pad0;
    uint64_t st_rdev;
    int64_t st_size;
    int64_t st_blksize;
    int64_t st_blocks;
    int64_t st_atim[2];
    int64_t st_mtim[2];
    int64_t st_ctim[2];

    st_xpn_server_stat() = default;
    st_xpn_server_stat(struct ::stat *entry) {
        if (!entry) return;
        eq_cast(st_dev, entry->st_dev);
        eq_cast(st_dev, entry->st_dev);
        eq_cast(st_ino, entry->st_ino);
        eq_cast(st_nlink, entry->st_nlink);
        eq_cast(st_mode, entry->st_mode);
        eq_cast(st_uid, entry->st_uid);
        eq_cast(st_gid, entry->st_gid);
        eq_cast(st_rdev, entry->st_rdev);
        eq_cast(st_size, entry->st_size);
        eq_cast(st_blksize, entry->st_blksize);
        eq_cast(st_blocks, entry->st_blocks);
        eq_cast(st_atim[0], entry->st_atim.tv_sec);
        eq_cast(st_atim[1], entry->st_atim.tv_nsec);
        eq_cast(st_mtim[0], entry->st_mtim.tv_sec);
        eq_cast(st_mtim[1], entry->st_mtim.tv_nsec);
        eq_cast(st_ctim[0], entry->st_ctim.tv_sec);
        eq_cast(st_ctim[1], entry->st_ctim.tv_nsec);
    }

    struct ::stat to_stat() {
        struct ::stat ret = {};
        eq_cast(ret.st_dev, st_dev);
        eq_cast(ret.st_ino, st_ino);
        eq_cast(ret.st_nlink, st_nlink);
        eq_cast(ret.st_mode, st_mode);
        eq_cast(ret.st_uid, st_uid);
        eq_cast(ret.st_gid, st_gid);
        eq_cast(ret.st_rdev, st_rdev);
        eq_cast(ret.st_size, st_size);
        eq_cast(ret.st_blksize, st_blksize);
        eq_cast(ret.st_blocks, st_blocks);
        eq_cast(ret.st_atim.tv_sec, st_atim[0]);
        eq_cast(ret.st_atim.tv_nsec, st_atim[1]);
        eq_cast(ret.st_mtim.tv_sec, st_mtim[0]);
        eq_cast(ret.st_mtim.tv_nsec, st_mtim[1]);
        eq_cast(ret.st_ctim.tv_sec, st_ctim[0]);
        eq_cast(ret.st_ctim.tv_nsec, st_ctim[1]);
        return ret;
    }
};

struct st_xpn_server_setattr {
    st_xpn_server_stat attr;
    xpn_server_path path;

    uint64_t get_size() { return offsetof(std::remove_pointer<decltype(this)>::type, path) + path.get_size(); }
};

struct st_xpn_server_attr_req {
    char status;
    st_xpn_server_stat attr;
    struct st_xpn_server_status status_req;

    uint64_t get_size() { return sizeof(*this); }
};

struct st_xpn_server_readdir {
    int64_t telldir;
    int64_t dir;
    char xpn_session;
    xpn_server_path path;

    uint64_t get_size() { return offsetof(std::remove_pointer<decltype(this)>::type, path) + path.get_size(); }
};

struct st_xpn_server_opendir_req {
    int64_t dir;
    struct st_xpn_server_status status;

    uint64_t get_size() { return sizeof(*this); }
};

struct st_xpn_server_dirent {
    int64_t d_ino;
    int64_t d_off;
    uint16_t d_reclen;
    unsigned char d_type;
    char d_name[256];

    st_xpn_server_dirent() = default;
    st_xpn_server_dirent(::dirent *entry) {
        if (!entry) return;
        eq_cast(d_ino, entry->d_ino);
        eq_cast(d_off, entry->d_off);
        eq_cast(d_reclen, entry->d_reclen);
        eq_cast(d_type, entry->d_type);
        std::memcpy(d_name, entry->d_name, std::min(sizeof(d_name), sizeof(entry->d_name)));
    }

    ::dirent to_dirent() {
        ::dirent ret = {};
        eq_cast(ret.d_ino, d_ino);
        eq_cast(ret.d_off, d_off);
        eq_cast(ret.d_reclen, d_reclen);
        eq_cast(ret.d_type, d_type);
        std::memcpy(ret.d_name, d_name, std::min(sizeof(ret.d_name), sizeof(d_name)));
        return ret;
    }
};

struct st_xpn_server_readdir_req {
    char end;  // If end = 1 exist entry; 0 not exist
    st_xpn_server_dirent ret;
    int64_t telldir;
    struct st_xpn_server_status status;

    uint64_t get_size() { return sizeof(*this); }
};

struct st_xpn_server_read_mdata_req {
    xpn_metadata::data mdata;
    struct st_xpn_server_status status;

    uint64_t get_size() { return sizeof(*this); }
};

struct st_xpn_server_write_mdata {
    xpn_metadata::data mdata;
    xpn_server_path path;

    uint64_t get_size() { return offsetof(std::remove_pointer<decltype(this)>::type, path) + path.get_size(); }
};

struct st_xpn_server_write_mdata_file_size {
    uint64_t size;
    xpn_server_path path;

    uint64_t get_size() { return offsetof(std::remove_pointer<decltype(this)>::type, path) + path.get_size(); }
};

struct st_xpn_server_statvfs {
    uint64_t f_bsize;
    uint64_t f_frsize;
    uint64_t f_blocks;
    uint64_t f_bfree;
    uint64_t f_bavail;
    uint64_t f_files;
    uint64_t f_ffree;
    uint64_t f_favail;
    uint64_t f_fsid;
    uint64_t f_flag;
    uint64_t f_namemax;
    uint32_t f_type;
    int32_t __f_spare[5];

    st_xpn_server_statvfs() = default;
    st_xpn_server_statvfs(struct ::statvfs *entry) {
        if (!entry) return;
        eq_cast(f_bsize, entry->f_bsize);
        eq_cast(f_frsize, entry->f_frsize);
        eq_cast(f_blocks, entry->f_blocks);
        eq_cast(f_bfree, entry->f_bfree);
        eq_cast(f_bavail, entry->f_bavail);
        eq_cast(f_files, entry->f_files);
        eq_cast(f_ffree, entry->f_ffree);
        eq_cast(f_favail, entry->f_favail);
        eq_cast(f_fsid, entry->f_fsid);
        eq_cast(f_flag, entry->f_flag);
        eq_cast(f_namemax, entry->f_namemax);
    }

    struct ::statvfs to_statvfs() {
        struct ::statvfs ret = {};
        eq_cast(ret.f_bsize, f_bsize);
        eq_cast(ret.f_frsize, f_frsize);
        eq_cast(ret.f_blocks, f_blocks);
        eq_cast(ret.f_bfree, f_bfree);
        eq_cast(ret.f_bavail, f_bavail);
        eq_cast(ret.f_files, f_files);
        eq_cast(ret.f_ffree, f_ffree);
        eq_cast(ret.f_favail, f_favail);
        eq_cast(ret.f_fsid, f_fsid);
        eq_cast(ret.f_flag, f_flag);
        eq_cast(ret.f_namemax, f_namemax);
        return ret;
    }
};

struct st_xpn_server_statvfs_req {
    st_xpn_server_statvfs attr;
    struct st_xpn_server_status status_req;

    uint64_t get_size() { return sizeof(*this); }
};

struct st_xpn_server_flush_preload {
    xpn_server_double_path paths;

    size_t get_size() { return paths.get_size(); }
};

constexpr uint64_t get_xpn_server_max_msg_size() {
    uint64_t size = 0;
    if (size < sizeof(st_xpn_server_status)) size = sizeof(st_xpn_server_status);
    if (size < sizeof(st_xpn_server_path_flags)) size = sizeof(st_xpn_server_path_flags);
    if (size < sizeof(st_xpn_server_path)) size = sizeof(st_xpn_server_path);
    if (size < sizeof(st_xpn_server_close)) size = sizeof(st_xpn_server_close);
    if (size < sizeof(st_xpn_server_rw)) size = sizeof(st_xpn_server_rw);
    if (size < sizeof(st_xpn_server_rw_req)) size = sizeof(st_xpn_server_rw_req);
    if (size < sizeof(st_xpn_server_rename)) size = sizeof(st_xpn_server_rename);
    if (size < sizeof(st_xpn_server_setattr)) size = sizeof(st_xpn_server_setattr);
    if (size < sizeof(st_xpn_server_attr_req)) size = sizeof(st_xpn_server_attr_req);
    if (size < sizeof(st_xpn_server_readdir)) size = sizeof(st_xpn_server_readdir);
    if (size < sizeof(st_xpn_server_opendir_req)) size = sizeof(st_xpn_server_opendir_req);
    if (size < sizeof(st_xpn_server_readdir_req)) size = sizeof(st_xpn_server_readdir_req);
    if (size < sizeof(st_xpn_server_read_mdata_req)) size = sizeof(st_xpn_server_read_mdata_req);
    if (size < sizeof(st_xpn_server_write_mdata)) size = sizeof(st_xpn_server_write_mdata);
    if (size < sizeof(st_xpn_server_write_mdata_file_size)) size = sizeof(st_xpn_server_write_mdata_file_size);
    if (size < sizeof(st_xpn_server_statvfs_req)) size = sizeof(st_xpn_server_statvfs_req);
    if (size < sizeof(st_xpn_server_flush_preload)) size = sizeof(st_xpn_server_flush_preload);
    if (size < sizeof(st_xpn_server_request_block)) size = sizeof(st_xpn_server_request_block);
    if (size < sizeof(st_xpn_server_request_block_req)) size = sizeof(st_xpn_server_request_block_req);
    return size;
}

struct xpn_server_msg {
    int32_t op = 0;
    int32_t tag = 0;
    uint32_t msg_size = 0;
    char msg_buffer[get_xpn_server_max_msg_size()] = {};

    uint32_t get_size() { return offsetof(std::remove_pointer<decltype(this)>::type, msg_buffer) + msg_size; }
    uint32_t get_header_size() { return offsetof(std::remove_pointer<decltype(this)>::type, msg_buffer); }
};

}  // namespace XPN
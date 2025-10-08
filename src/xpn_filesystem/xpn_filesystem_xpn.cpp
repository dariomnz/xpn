
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

#include "xpn_filesystem_xpn.hpp"

#include <fcntl.h>

#include "base_cpp/debug.hpp"
#include "base_cpp/filesystem.hpp"
#include "base_cpp/proxy.hpp"

#include "xpn.h"

namespace XPN {

int xpn_filesystem_xpn::creat(const char *path, uint32_t mode) {
    debug_info(" >> BEGIN");
    auto ret = xpn_creat(path, mode);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_xpn::open(const char *path, int flags) {
    debug_info(" >> BEGIN");
    auto ret = xpn_open(path, flags);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_xpn::open(const char *path, int flags, uint32_t mode) {
    debug_info(" >> BEGIN");
    auto ret = xpn_open(path, flags, mode);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_xpn::close(int fd) {
    debug_info(" >> BEGIN");
    auto ret = xpn_close(fd);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_xpn::fsync([[maybe_unused]] int fd) {
    debug_info(" >> BEGIN");
    // auto ret = xpn_fsync(fd);
    debug_info(" << END");
    return 0;
}

int xpn_filesystem_xpn::unlink(const char *path) {
    debug_info(" >> BEGIN");
    auto ret = xpn_unlink(path);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_xpn::rename(const char *oldPath, const char *newPath) {
    debug_info(" >> BEGIN");
    auto ret = xpn_rename(oldPath, newPath);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_xpn::stat(const char *path, struct ::stat *st) {
    debug_info(" >> BEGIN");
    auto ret = xpn_stat(path, st);
    debug_info(" << END");
    return ret;
}

int64_t xpn_filesystem_xpn::write(int fd, const void *data, uint64_t len) {
    debug_info(" >> BEGIN");
    auto ret = xpn_write(fd, data, len);
    debug_info(" << END");
    return ret;
}

int64_t xpn_filesystem_xpn::pwrite(int fd, const void *data, uint64_t len, int64_t offset) {
    debug_info(" >> BEGIN");
    auto ret = xpn_pwrite(fd, data, len, offset);
    debug_info(" << END");
    return ret;
}

int64_t xpn_filesystem_xpn::read(int fd, void *data, uint64_t len) {
    debug_info(" >> BEGIN");
    auto ret = xpn_read(fd, data, len);
    debug_info(" << END");
    return ret;
}

int64_t xpn_filesystem_xpn::pread(int fd, void *data, uint64_t len, int64_t offset) {
    debug_info(" >> BEGIN");
    auto ret = xpn_pread(fd, data, len, offset);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_xpn::mkdir(const char *path, uint32_t mode) {
    debug_info(" >> BEGIN");
    auto ret = xpn_mkdir(path, mode);
    debug_info(" << END");
    return ret;
}

::DIR *xpn_filesystem_xpn::opendir(const char *path) {
    debug_info(" >> BEGIN");
    auto ret = xpn_opendir(path);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_xpn::closedir(::DIR *dir) {
    debug_info(" >> BEGIN");
    auto ret = xpn_closedir(dir);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_xpn::rmdir(const char *path) {
    debug_info(" >> BEGIN");
    auto ret = xpn_rmdir(path);
    debug_info(" << END");
    return ret;
}

struct ::dirent *xpn_filesystem_xpn::readdir(::DIR *dir) {
    debug_info(" >> BEGIN");
    auto ret = xpn_readdir(dir);
    debug_info(" << END");
    return ret;
}

int64_t xpn_filesystem_xpn::telldir([[maybe_unused]] ::DIR *dir) {
    debug_info(" >> BEGIN");
    // auto ret = xpn_telldir(dir);
    auto ret = 0;
    unreachable("TODO");
    debug_info(" << END");
    return ret;
}

void xpn_filesystem_xpn::seekdir([[maybe_unused]] ::DIR *dir, [[maybe_unused]] int64_t pos) {
    debug_info(" >> BEGIN");
    // xpn_seekdir(dir, pos);
    unreachable("TODO");
    debug_info(" << END");
}

int xpn_filesystem_xpn::statvfs(const char *path, struct ::statvfs *buff) {
    debug_info(" >> BEGIN");
    auto ret = xpn_statvfs(path, buff);
    debug_info(" << END");
    return ret;
}

}  // namespace XPN
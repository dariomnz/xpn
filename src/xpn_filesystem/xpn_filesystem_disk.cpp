
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

#include "xpn_filesystem_disk.hpp"

#include <fcntl.h>

#include "base_cpp/debug.hpp"
#include "base_cpp/filesystem.hpp"
#include "base_cpp/proxy.hpp"

namespace XPN {

int xpn_filesystem_disk::creat(const char *path, uint32_t mode) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(creat)(path, mode);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_disk::open(const char *path, int flags) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(open)(path, flags);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_disk::open(const char *path, int flags, uint32_t mode) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(open)(path, flags, mode);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_disk::close(int fd) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(close)(fd);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_disk::fsync(int fd) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(fsync)(fd);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_disk::unlink(const char *path) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(unlink)(path);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_disk::rename(const char *oldPath, const char *newPath) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(rename)(oldPath, newPath);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_disk::stat(const char *path, struct ::stat *st) {
    debug_info(" >> BEGIN");
    #ifdef _STAT_VER
    auto ret = PROXY(__xstat)(_STAT_VER, path, st);
    #else
    auto ret = PROXY(stat)(path, st);
    #endif
    debug_info(" << END");
    return ret;
}

int64_t xpn_filesystem_disk::write(int fd, const void *data, uint64_t len) {
    debug_info(" >> BEGIN");
    auto ret = filesystem::write(fd, data, len);
    debug_info(" << END");
    return ret;
}

int64_t xpn_filesystem_disk::pwrite(int fd, const void *data, uint64_t len, int64_t offset) {
    debug_info(" >> BEGIN");
    auto ret = filesystem::pwrite(fd, data, len, offset);
    debug_info(" << END");
    return ret;
}

int64_t xpn_filesystem_disk::read(int fd, void *data, uint64_t len) {
    debug_info(" >> BEGIN");
    auto ret = filesystem::read(fd, data, len);
    debug_info(" << END");
    return ret;
}

int64_t xpn_filesystem_disk::pread(int fd, void *data, uint64_t len, int64_t offset) {
    debug_info(" >> BEGIN");
    auto ret = filesystem::pread(fd, data, len, offset);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_disk::mkdir(const char *path, uint32_t mode) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(mkdir)(path, mode);
    debug_info(" << END");
    return ret;
}

::DIR *xpn_filesystem_disk::opendir(const char *path) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(opendir)(path);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_disk::closedir(::DIR *dir) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(closedir)(dir);
    debug_info(" << END");
    return ret;
}

int xpn_filesystem_disk::rmdir(const char *path) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(rmdir)(path);
    debug_info(" << END");
    return ret;
}

struct ::dirent *xpn_filesystem_disk::readdir(::DIR *dir) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(readdir)(dir);
    debug_info(" << END");
    return ret;
}

int64_t xpn_filesystem_disk::telldir(::DIR *dir) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(telldir)(dir);
    debug_info(" << END");
    return ret;
}

void xpn_filesystem_disk::seekdir(::DIR *dir, int64_t pos) {
    debug_info(" >> BEGIN");
    PROXY(seekdir)(dir, pos);
    debug_info(" << END");
}

int xpn_filesystem_disk::statvfs(const char *path, struct ::statvfs *buff) {
    debug_info(" >> BEGIN");
    auto ret = PROXY(statvfs)(path, buff);
    debug_info(" << END");
    return ret;
}

}  // namespace XPN
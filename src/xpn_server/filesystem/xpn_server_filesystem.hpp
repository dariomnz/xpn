
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

#include <cstdint>
#include <memory>

namespace XPN {
enum class filesystem_mode {
    disk = 0,
    xpn = 1,
};

class xpn_server_filesystem {
   public:
    static std::unique_ptr<xpn_server_filesystem> Create(filesystem_mode mode);
    filesystem_mode m_mode;

   public:
    xpn_server_filesystem() = default;
    virtual ~xpn_server_filesystem() = default;

    virtual int creat(const char *path, uint32_t mode) = 0;
    virtual int open(const char *path, int flags) = 0;
    virtual int open(const char *path, int flags, uint32_t mode) = 0;
    virtual int close(int fd) = 0;
    virtual int fsync(int fd) = 0;
    virtual int unlink(const char *path) = 0;
    virtual int rename(const char *oldPath, const char *newPath) = 0;
    virtual int stat(const char *path, struct ::stat *st) = 0;

    virtual int64_t write(int fd, const void *data, uint64_t len) = 0;
    virtual int64_t pwrite(int fd, const void *data, uint64_t len, int64_t offset) = 0;
    virtual int64_t read(int fd, void *data, uint64_t len) = 0;
    virtual int64_t pread(int fd, void *data, uint64_t len, int64_t offset) = 0;

    virtual int mkdir(const char *path, uint32_t mode) = 0;
    virtual ::DIR *opendir(const char *path) = 0;
    virtual int closedir(::DIR *dir) = 0;
    virtual int rmdir(const char *path) = 0;
    virtual struct ::dirent *readdir(::DIR *dir) = 0;
    virtual int64_t telldir(::DIR *dir) = 0;
    virtual void seekdir(::DIR *dir, int64_t pos) = 0;

    virtual int statvfs(const char *path, struct ::statvfs *buff) = 0;
};
}  // namespace XPN
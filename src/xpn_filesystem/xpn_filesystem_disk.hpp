
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

#include "xpn_filesystem.hpp"

namespace XPN {

class xpn_filesystem_disk : public xpn_filesystem {
   public:
    int creat(const char *path, uint32_t mode) override;
    int open(const char *path, int flags) override;
    int open(const char *path, int flags, uint32_t mode) override;
    int close(int fd) override;
    int fsync(int fd) override;
    int unlink(const char *path) override;
    int rename(const char *oldPath, const char *newPath) override;
    int stat(const char *path, struct ::stat *st) override;

    int64_t write(int fd, const void *data, uint64_t len) override;
    int64_t pwrite(int fd, const void *data, uint64_t len, int64_t offset) override;
    int64_t read(int fd, void *data, uint64_t len) override;
    int64_t pread(int fd, void *data, uint64_t len, int64_t offset) override;

    int mkdir(const char *path, uint32_t mode) override;
    ::DIR *opendir(const char *path) override;
    int closedir(::DIR *dir) override;
    int rmdir(const char *path) override;
    struct ::dirent *readdir(::DIR *dir) override;
    int64_t telldir(::DIR *dir) override;
    void seekdir(::DIR *dir, int64_t pos) override;

    int statvfs(const char *path, struct ::statvfs *buff) override;
};
}  // namespace XPN
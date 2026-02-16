
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

/* ... Include / Inclusion ........................................... */
// #define DEBUG
// #define BUILD_WITH_DMTCP
#include <bits/types/FILE.h>

#include <variant>

#include "base_cpp/debug.hpp"
#include "base_cpp/proxy.hpp"
#include "xpn.h"

#ifdef ENABLE_MPI_SERVER
#include "mpi.h"
#endif
// #include <signal.h>
#include <dirent.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <time.h>

#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string_view>
#include <unordered_set>

/* ... Const / Const ................................................. */

#ifdef BUILD_WITH_DMTCP
#include "xpn_dmtcp.hpp"
#define scope_xpn                  \
    scope_disable_ckpt scope_ckpt; \
    check_xpn_init();

#undef PROXY
#define PROXY(func) NEXT_FNC(func)
// #warning "Compiling with dmtcp"
#else
#define scope_xpn check_xpn_init();
// #warning "Compiling without dmtcp"
#endif

/* ... Global variables / Variables globales ........................ */

static const std::string_view getEnvOrDefault(const char *envVarName, std::string_view defaultValue) {
    const char *envValue = std::getenv(envVarName);
    if (envValue != nullptr) {
        return envValue;
    } else {
        return defaultValue;
    }
}

static std::string_view xpn_part_prefix = "/tmp/expand/";

/* ... Auxiliar functions / Funciones auxiliares ......................................... */

/**
 * Initialize xpn
 */
static bool check_xpn_init_initialized = false;
inline void check_xpn_init() {
    if (!check_xpn_init_initialized) {
        check_xpn_init_initialized = true;
        xpn_init();
    }
}
/**
 * Initialize xpn_part_prefix
 */
bool init_xpn_part_prefix_initialized = false;
inline void init_xpn_part_prefix() {
    if (!init_xpn_part_prefix_initialized) {
        xpn_part_prefix = getEnvOrDefault("XPN_MOUNT_POINT", "/tmp/expand/");
        init_xpn_part_prefix_initialized = true;
    }
}

/**
 * Check that the path contains the prefix of XPN
 */
inline int is_xpn_prefix(const char *path) {
    init_xpn_part_prefix();
    // debug_info_fmt(path << " " << xpn_part_prefix);
    return (std::strlen(path) > xpn_part_prefix.size() &&
            !std::memcmp(xpn_part_prefix.data(), path, xpn_part_prefix.size()));
}

/**
 * Skip the XPN prefix
 */
inline const char *skip_xpn_prefix(const char *path) {
    init_xpn_part_prefix();
    return (const char *)(path + xpn_part_prefix.size());
}

/**
 * File descriptors table management
 */
std::recursive_mutex fdstable_mutex;
using fdtable_item = std::variant<int, FILE *, DIR *>;
template <class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

std::unordered_set<fdtable_item> fdstable;
std::unordered_set<int> fdstable_ckpt;

bool fdstable_get(int fd) {
    if (fd < 0) return false;
    // debug_info_fmt("[BYPASS] Begin fdstable_get(%d)", fd);
    bool ret = fdstable.find(fd) != fdstable.end();
    // debug_info_fmt("[BYPASS] End fdstable_get(%d) = %d", fd, ret);
    return ret;
}

bool fdstable_get(FILE *file) {
    if (file == nullptr) return false;
    // debug_info_fmt("[BYPASS] Begin fdstable_get(%d)", fd);
    bool ret = fdstable.find(file) != fdstable.end();
    // debug_info_fmt("[BYPASS] End fdstable_get(%d) = %d", fd, ret);
    return ret;
}

bool fdstable_get(DIR *dir) {
    if (dir == nullptr) return false;
    // debug_info_fmt("[BYPASS] Begin fdstable_get(%d)", fd);
    bool ret = fdstable.find(dir) != fdstable.end();
    // debug_info_fmt("[BYPASS] End fdstable_get(%d) = %d", fd, ret);
    return ret;
}

extern "C" int open(const char *path, int flags, ...);
int fdstable_put(int fd) {
    if (fd < 0) return fd;
    std::unique_lock lock(fdstable_mutex);
    // debug_info_fmt("[BYPASS] >> Begin fdstable_put %d", fd);
    int new_fd = PROXY(open)("/dev/null", O_RDONLY);
    // debug_info_fmt("[BYPASS] fd %d", fd);
    if (new_fd < 0) return -1;
    if (new_fd != fd) {
        int fd2 = xpn_dup2(fd, new_fd);
        // debug_info_fmt("[BYPASS] xpn_dup2 (%d, %d) = %d", fd, fd, fd2);
        if (fd2 < 0) return -1;
        xpn_close(fd);
    }
    int ret = fdstable.emplace(new_fd).second ? new_fd : -1;
#ifdef BUILD_WITH_DMTCP
    if (xpn_dmtcp::instance().m_inCkpt) {
        fdstable_ckpt.emplace(ret);
        debug_info_fmt("[BYPASS] fdstable_ckpt emplace %d", fd);
    }
#endif
    // debug_info_fmt("[BYPASS] End = %d", ret);
    return ret;
}

FILE *fdstable_put(FILE *file) {
    if (file == nullptr) return file;
    std::unique_lock lock(fdstable_mutex);
    // debug_info_fmt("[BYPASS] >> Begin fdstable_put %d", fd);
    int new_fd = PROXY(open)("/dev/null", O_RDONLY);
    // debug_info_fmt("[BYPASS] fd %d", fd);
    if (new_fd < 0) return nullptr;
    if (new_fd != file->_fileno) {
        int fd2 = xpn_dup2(file->_fileno, new_fd);
        // debug_info_fmt("[BYPASS] xpn_dup2 (%d, %d) = %d", fd, fd, fd2);
        if (fd2 < 0) return nullptr;
        xpn_close(file->_fileno);
        file->_fileno = new_fd;
    }
    FILE *ret = fdstable.emplace(file).second ? file : nullptr;
    if (ret) fdstable.emplace(file->_fileno);
    // debug_info_fmt("[BYPASS] End = %d", ret);
    return ret;
}

DIR *fdstable_put(DIR *dir) {
    if (dir == nullptr) return dir;
    return fdstable.emplace(dir).second ? dir : nullptr;
}

extern "C" int close(int fd);
bool fdstable_remove(int fd) {
    if (fd < 0) return false;
    std::unique_lock lock(fdstable_mutex);
    PROXY(close)(fd);
    return fdstable.erase(fd) == 1;
}

bool fdstable_remove(FILE *file) {
    if (file == nullptr) return false;
    std::unique_lock lock(fdstable_mutex);
    fdstable.erase(file->_fileno);
    PROXY(close)(file->_fileno);
    return fdstable.erase(file) == 1;
}

bool fdstable_remove(DIR *dir) {
    if (dir == nullptr) return false;
    std::unique_lock lock(fdstable_mutex);
    return fdstable.erase(dir) == 1;
}

/**
 * stat management
 */
int stat_to_stat64(struct stat64 *buf, struct stat *st) {
    buf->st_dev = (__dev_t)st->st_dev;
    buf->st_ino = (__ino64_t)st->st_ino;
    buf->st_mode = (__mode_t)st->st_mode;
    buf->st_nlink = (__nlink_t)st->st_nlink;
    buf->st_uid = (__uid_t)st->st_uid;
    buf->st_gid = (__gid_t)st->st_gid;
    buf->st_rdev = (__dev_t)st->st_rdev;
    buf->st_size = (__off64_t)st->st_size;
    buf->st_blksize = (__blksize_t)st->st_blksize;
    buf->st_blocks = (__blkcnt64_t)st->st_blocks;
    buf->st_atime = (__time_t)st->st_atime;
    buf->st_mtime = (__time_t)st->st_mtime;
    buf->st_ctime = (__time_t)st->st_ctime;

    return 0;
}

int stat64_to_stat(struct stat *buf, struct stat64 *st) {
    buf->st_dev = (__dev_t)st->st_dev;
    buf->st_ino = (__ino_t)st->st_ino;
    buf->st_mode = (__mode_t)st->st_mode;
    buf->st_nlink = (__nlink_t)st->st_nlink;
    buf->st_uid = (__uid_t)st->st_uid;
    buf->st_gid = (__gid_t)st->st_gid;
    buf->st_rdev = (__dev_t)st->st_rdev;
    buf->st_size = (__off_t)st->st_size;
    buf->st_blksize = (__blksize_t)st->st_blksize;
    buf->st_blocks = (__blkcnt_t)st->st_blocks;
    buf->st_atime = (__time_t)st->st_atime;
    buf->st_mtime = (__time_t)st->st_mtime;
    buf->st_ctime = (__time_t)st->st_ctime;

    return 0;
}

/* ... Functions / Funciones ......................................... */

// Memory
#ifdef BUILF_WITH_DMTCP
extern "C" void *malloc(size_t size) {
    void *ret;
    debug_info_fmt("[BYPASS] >> Begin malloc(%ld)", size);
    auto &instance = xpn_dmtcp::instance();
    if (instance.m_disableAlloc) {
        debug_info_fmt("[BYPASS] << Error: Malloc is disabled");
        throw std::runtime_error("malloc is disabled");
    } else {
        ret = PROXY(malloc)(size);
        debug_info_fmt("[BYPASS] << PROXY(malloc)(%ld) -> %p", size, ret);
    }
    return ret;
}

void *realloc(void *old_ptr, size_t size) {
    void *ret;
    debug_info_fmt("[BYPASS] >> Begin realloc(%p, %ld)", old_ptr, size);
    auto &instance = xpn_dmtcp::instance();
    if (instance.m_disableAlloc) {
        debug_info_fmt("[BYPASS] << Error: Realloc is disabled");
        throw std::runtime_error("realloc is disabled");
    } else {
        ret = PROXY(realloc)(old_ptr, size);
        debug_info_fmt("[BYPASS] << PROXY(realloc)(%p, %ld) -> %p", old_ptr, size, ret);
    }
    return ret;
}

extern "C" void free(void *ptr) {
    debug_info_fmt("[BYPASS] >> Begin free(%p)", ptr);
    auto &instance = xpn_dmtcp::instance();
    if (instance.m_disableAlloc) {
        debug_info_fmt("[BYPASS] << Error: free is disabled");
        throw std::runtime_error("free is disabled");
    } else {
        PROXY(free)(ptr);
        debug_info_fmt("[BYPASS] << PROXY(free)(%p)", ptr);
    }
}
#endif

// File API
extern "C" int open(const char *path, int flags, ...) {
    int ret, fd;
    va_list ap;
    mode_t mode = 0;
    va_start(ap, flags);
    mode = va_arg(ap, mode_t);
    va_end(ap);
    debug_info_fmt("[BYPASS] >> Begin open(%s, %d, %d)", path, flags, mode);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        if (mode != 0) {
            fd = xpn_open(skip_xpn_prefix(path), flags, mode);
        } else {
            fd = xpn_open(skip_xpn_prefix(path), flags);
        }
        ret = fdstable_put(fd);
        debug_info_fmt("[BYPASS] << xpn_open(%s, %d, %d) -> %d", skip_xpn_prefix(path), flags, mode, ret);
    } else {
        ret = PROXY(open)(path, flags, mode);
        debug_info_fmt("[BYPASS] << PROXY(open)(%s, %d, %d) -> %d", path, flags, mode, ret);
    }
    return ret;
}

extern "C" int open64(const char *path, int flags, ...) {
    int fd, ret;
    va_list ap;
    mode_t mode = 0;
    va_start(ap, flags);
    mode = va_arg(ap, mode_t);
    va_end(ap);
    debug_info_fmt("[BYPASS] >> Begin open64(%s, %d, %d)", path, flags, mode);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        if (mode != 0) {
            fd = xpn_open(skip_xpn_prefix(path), flags, mode);
        } else {
            fd = xpn_open(skip_xpn_prefix(path), flags);
        }
        ret = fdstable_put(fd);
        debug_info_fmt("[BYPASS] << xpn_open(%s, %d, %d) -> %d", skip_xpn_prefix(path), flags, mode, ret);
    } else {
        ret = PROXY(open64)((char *)path, flags, mode);
        debug_info_fmt("[BYPASS] << PROXY(open64)(%s, %d, %d) -> %d", path, flags, mode, ret);
    }
    return ret;
}

#ifndef HAVE_ICC

extern "C" int __open_2(const char *path, int flags, ...) {
    int fd, ret;
    va_list ap;
    mode_t mode = 0;
    va_start(ap, flags);
    mode = va_arg(ap, mode_t);
    va_end(ap);
    debug_info_fmt("[BYPASS] >> Begin __open_2(%s, %d, %d)", path, flags, mode);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        if (mode != 0) {
            fd = xpn_open(skip_xpn_prefix(path), flags, mode);
        } else {
            fd = xpn_open(skip_xpn_prefix(path), flags);
        }
        ret = fdstable_put(fd);
        debug_info_fmt("[BYPASS] << xpn_open(%s, %d, %d) -> %d", skip_xpn_prefix(path), flags, mode, ret);
    } else {
        ret = PROXY(__open_2)((char *)path, flags);
        debug_info_fmt("[BYPASS] << PROXY(__open_2)(%s, %d, %d) -> %d", path, flags, mode, ret);
    }
    return ret;
}

#endif

extern "C" int creat(const char *path, mode_t mode) {
    int fd, ret;
    debug_info_fmt("[BYPASS] >> Begin creat(%s, %d)", path, mode);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        fd = xpn_creat((const char *)skip_xpn_prefix(path), mode);
        ret = fdstable_put(fd);
        debug_info_fmt("[BYPASS] << xpn_creat(%s, %d) -> %d", skip_xpn_prefix(path), mode, ret);
    } else {
        ret = PROXY(creat)(path, mode);
        debug_info_fmt("[BYPASS] << PROXY(creat)(%s, %d) -> %d", path, mode, ret);
    }
    debug_info_fmt("[BYPASS] << After creat....");
    return ret;
}

extern "C" int mkstemp(char *templ) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin mkstemp(%s)", templ);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(templ)) {
        scope_xpn;
        srand(time(NULL));
        int n = rand() % 100000;
        char *str_init = strstr(templ, "XXXXXX");
        sprintf(str_init, "%06d", n);
        int fd = xpn_creat((const char *)skip_xpn_prefix(templ), S_IRUSR | S_IWUSR);
        ret = fdstable_put(fd);
        debug_info_fmt("[BYPASS] << xpn_creat(%s, %d) -> %d", skip_xpn_prefix(templ), S_IRUSR | S_IWUSR, ret);
    } else {
        ret = PROXY(mkstemp)(templ);
        debug_info_fmt("[BYPASS] << PROXY(mkstemp)(%s) -> %d", templ, ret);
    }
    return ret;
}

extern "C" int ftruncate(int fd, off_t length) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin ftruncate(%d, %ld)", fd, length);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_ftruncate(fd, length);
        debug_info_fmt("[BYPASS] << xpn_ftruncate(%d, %ld) -> %d", fd, length, ret);
    } else {
        ret = PROXY(ftruncate)(fd, length);
        debug_info_fmt("[BYPASS] << PROXY(ftruncate)(%d, %ld) -> %d", fd, length, ret);
    }
    return ret;
}

extern "C" ssize_t read(int fd, void *buf, size_t nbyte) {
    ssize_t ret = -1;
    debug_info_fmt("[BYPASS] >> Begin read(%d, %p, %ld)", fd, buf, nbyte);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_read(fd, buf, nbyte);
        debug_info_fmt("[BYPASS] << xpn_read(%d, %p, %ld) -> %ld", fd, buf, nbyte, ret);
    } else {
        ret = PROXY(read)(fd, buf, nbyte);
        debug_info_fmt("[BYPASS] << PROXY(read)(%d, %p, %ld) -> %ld", fd, buf, nbyte, ret);
    }
    return ret;
}

extern "C" ssize_t write(int fd, const void *buf, size_t nbyte) {
    ssize_t ret = -1;
    // debug_info_fmt("[BYPASS] >> Begin write(%d, %p, %ld)", fd, buf, nbyte);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
#ifdef BUILD_WITH_DMTCP
        auto &instance = xpn_dmtcp::instance();
        if (instance.m_inCkpt) {
            instance.m_disableAlloc = true;
        }
#endif
        ret = xpn_write(fd, (void *)buf, nbyte);
#ifdef BUILD_WITH_DMTCP
        if (instance.m_inCkpt) {
            instance.m_disableAlloc = false;
        }
#endif
        debug_info_fmt("[BYPASS] << xpn_write(%d, %p, %ld) -> %ld", fd, buf, nbyte, ret);
    } else {
        ret = PROXY(write)(fd, (void *)buf, nbyte);
        // debug_info_fmt("[BYPASS] << PROXY(write)(%d, %p, %ld) -> %ld", fd, buf, nbyte, ret);
    }
    return ret;
}

extern "C" ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
    ssize_t ret = -1;
    debug_info_fmt("[BYPASS] >> Begin pread(%d, %p, %ld, %ld)", fd, buf, count, offset);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_pread(fd, buf, count, offset);
        debug_info_fmt("[BYPASS] << xpn_read(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    } else {
        ret = PROXY(pread)(fd, buf, count, offset);
        debug_info_fmt("[BYPASS] << PROXY(pread)(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    }
    return ret;
}

extern "C" ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
    ssize_t ret = -1;
    debug_info_fmt("[BYPASS] >> Begin pwrite(%d, %p, %ld, %ld)", fd, buf, count, offset);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_pwrite(fd, buf, count, offset);
        debug_info_fmt("[BYPASS] << xpn_pwrite(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    } else {
        ret = PROXY(pwrite)(fd, buf, count, offset);
        debug_info_fmt("[BYPASS] << PROXY(pwrite)(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    }
    return ret;
}

extern "C" ssize_t pread64(int fd, void *buf, size_t count, off_t offset) {
    ssize_t ret = -1;
    debug_info_fmt("[BYPASS] >> Begin pread64(%d, %p, %ld, %ld)", fd, buf, count, offset);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_pread(fd, buf, count, offset);
        debug_info_fmt("[BYPASS] << xpn_pread(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    } else {
        ret = PROXY(pread64)(fd, buf, count, offset);
        debug_info_fmt("[BYPASS] << PROXY(pread64)(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    }
    return ret;
}

extern "C" ssize_t pwrite64(int fd, const void *buf, size_t count, off_t offset) {
    ssize_t ret = -1;
    debug_info_fmt("[BYPASS] >> Begin pwrite64(%d, %p, %ld, %ld)", fd, buf, count, offset);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_pwrite(fd, buf, count, offset);
        debug_info_fmt("[BYPASS] << xpn_pwrite(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    } else {
        ret = PROXY(pwrite64)(fd, buf, count, offset);
        debug_info_fmt("[BYPASS] << PROXY(pwrite64)(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    }
    return ret;
}

extern "C" off_t lseek(int fd, off_t offset, int whence) {
    off_t ret = (off_t)-1;
    debug_info_fmt("[BYPASS] >> Begin lseek(%d, %ld, %d)", fd, offset, whence);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_lseek(fd, offset, whence);
        debug_info_fmt("[BYPASS] << xpn_lseek(%d, %ld, %d) -> %ld", fd, offset, whence, ret);
    } else {
        ret = PROXY(lseek)(fd, offset, whence);
        debug_info_fmt("[BYPASS] << PROXY(lseek)(%d, %ld, %d) -> %ld", fd, offset, whence, ret);
    }
    return ret;
}

extern "C" off64_t lseek64(int fd, off64_t offset, int whence) {
    off64_t ret = (off64_t)-1;
    debug_info_fmt("[BYPASS] >> Begin lseek64(%d, %ld, %d)", fd, offset, whence);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_lseek(fd, offset, whence);
        debug_info_fmt("[BYPASS] << xpn_lseek(%d, %ld, %d) -> %ld", fd, offset, whence, ret);
    } else {
        ret = PROXY(lseek64)(fd, offset, whence);
        debug_info_fmt("[BYPASS] << PROXY(lseek64)(%d, %ld, %d) -> %ld", fd, offset, whence, ret);
    }
    return ret;
}

extern "C" int stat(const char *path, struct stat *buf) {
    int ret;
    debug_info_fmt("[BYPASS] >> Begin stat(%s, %p)", path, buf);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_stat(skip_xpn_prefix(path), buf);
        debug_info_fmt("[BYPASS] << xpn_stat(%s, %p) -> %d", skip_xpn_prefix(path), buf, ret);
    } else {
#ifdef _STAT_VER
        ret = PROXY(__xstat)(_STAT_VER, (const char *)path, buf);
#else
        ret = PROXY(stat)((const char *)path, buf);
#endif
        debug_info_fmt("[BYPASS] << PROXY(__xstat)(%s, %p) -> %d", path, buf, ret);
    }
    return ret;
}

extern "C" int __lxstat64(int ver, const char *path, struct stat64 *buf) {
    int ret;
    struct stat st;
    debug_info_fmt("[BYPASS] >> Begin __lxstat64(%d, %s, %p)", ver, path, buf);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_stat(skip_xpn_prefix(path), &st);
        if (ret >= 0) {
            stat_to_stat64(buf, &st);
        }
        debug_info_fmt("[BYPASS] << xpn_stat(%s, %p) -> %d", skip_xpn_prefix(path), buf, ret);
    } else {
        ret = PROXY(__lxstat64)(ver, (const char *)path, buf);
        debug_info_fmt("[BYPASS] << PROXY(__lxstat64)(%d, %s, %p) -> %d", ver, path, buf, ret);
    }
    return ret;
}

extern "C" int __xstat64(int ver, const char *path, struct stat64 *buf) {
    int ret;
    struct stat st;
    debug_info_fmt("[BYPASS] >> Begin __xstat64(%d, %s, %p)", ver, path, buf);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_stat(skip_xpn_prefix(path), &st);
        if (ret >= 0) {
            stat_to_stat64(buf, &st);
        }
        debug_info_fmt("[BYPASS] << xpn_stat(%s, %p) -> %d", skip_xpn_prefix(path), buf, ret);
    } else {
        ret = PROXY(__xstat64)(ver, (const char *)path, buf);
        debug_info_fmt("[BYPASS] << PROXY(__xstat64)(%d, %s, %p) -> %d", ver, path, buf, ret);
    }
    return ret;
}

extern "C" int __fxstat64(int ver, int fd, struct stat64 *buf) {
    int ret;
    struct stat st;
    debug_info_fmt("[BYPASS] >> Begin __fxstat64(%d, %d, %p)", ver, fd, buf);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_fstat(fd, &st);
        if (ret >= 0) {
            stat_to_stat64(buf, &st);
        }
        debug_info_fmt("[BYPASS] << xpn_fstat(%d, %p) -> %d", fd, buf, ret);
    } else {
        ret = PROXY(__fxstat64)(ver, fd, buf);
        debug_info_fmt("[BYPASS] << PROXY(__fxstat64)(%d, %d, %p) -> %d", ver, fd, buf, ret);
    }
    return ret;
}

extern "C" int __lxstat(int ver, const char *path, struct stat *buf) {
    int ret;
    debug_info_fmt("[BYPASS] >> Begin __lxstat(%d, %s, %p)", ver, path, buf);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_stat(skip_xpn_prefix(path), buf);
        debug_info_fmt("[BYPASS] << xpn_stat(%s, %p) -> %d", skip_xpn_prefix(path), buf, ret);
    } else {
        ret = PROXY(__lxstat)(ver, (const char *)path, buf);
        debug_info_fmt("[BYPASS] << PROXY(__lxstat)(%d, %s, %p) -> %d", ver, path, buf, ret);
    }
    return ret;
}

extern "C" int __xstat(int ver, const char *path, struct stat *buf) {
    int ret;
    debug_info_fmt("[BYPASS] >> Begin __xstat(%d, %s, %p)", ver, path, buf);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_stat(skip_xpn_prefix(path), buf);
        debug_info_fmt("[BYPASS] << xpn_stat(%s, %p) -> %d", skip_xpn_prefix(path), buf, ret);
    } else {
        ret = PROXY(__xstat)(ver, (const char *)path, buf);
        debug_info_fmt("[BYPASS] << PROXY(__xstat)(%d, %s, %p) -> %d", ver, path, buf, ret);
    }
    return ret;
}

extern "C" int fstat(int fd, struct stat *buf) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin fstat(%d, %p)", fd, buf);
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_fstat(fd, buf);
        debug_info_fmt("[BYPASS] << xpn_fstat(%d, %p) -> %d", fd, buf, ret);
    } else {
        ret = PROXY(fstat)(fd, buf);
        debug_info_fmt("[BYPASS] << PROXY(fstat)(%d, %p) -> %d", fd, buf, ret);
    }
    return ret;
}

extern "C" int __fxstat(int ver, int fd, struct stat *buf) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin __fxstat(%d, %d, %p)", ver, fd, buf);
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_fstat(fd, buf);
        debug_info_fmt("[BYPASS] << xpn_fstat(%d, %p) -> %d", fd, buf, ret);
    } else {
        ret = PROXY(__fxstat)(ver, fd, buf);
        debug_info_fmt("[BYPASS] << PROXY(__fxstat)(%d, %d, %p) -> %d", ver, fd, buf, ret);
    }
    return ret;
}

extern "C" int __fxstatat64(int ver, int dirfd, const char *path, struct stat64 *buf, int flags) {
    int ret = -1;
    struct stat st;
    debug_info_fmt("[BYPASS] >> Begin __fxstatat64(%d, %d, %s, %p, %d)", ver, dirfd, path, buf, flags);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_stat(skip_xpn_prefix(path), &st);
        if (ret >= 0) {
            stat_to_stat64(buf, &st);
        }
        debug_info_fmt("[BYPASS] << xpn_stat(%s, %p) -> %d", skip_xpn_prefix(path), buf, ret);
    } else {
        ret = PROXY(__fxstatat64)(ver, dirfd, path, buf, flags);
        debug_info_fmt("[BYPASS] << PROXY(__fxstatat64)(%d, %d, %s, %p, %d) -> %d", ver, dirfd, path, buf, flags, ret);
    }
    return ret;
}

extern "C" int __fxstatat(int ver, int dirfd, const char *path, struct stat *buf, int flags) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin __fxstatat(%d, %d, %s, %p, %d)", ver, dirfd, path, buf, flags);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_stat(skip_xpn_prefix(path), buf);
        debug_info_fmt("[BYPASS] << xpn_stat(%s, %p) -> %d", skip_xpn_prefix(path), buf, ret);
    } else {
        ret = PROXY(__fxstatat)(ver, dirfd, path, buf, flags);
        debug_info_fmt("[BYPASS] << PROXY(__fxstatat)(%d, %d, %s, %p, %d) -> %d", ver, dirfd, path, buf, flags, ret);
    }
    return ret;
}

extern "C" int close(int fd) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin close(%d)", fd);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_close(fd);
        fdstable_remove(fd);
        debug_info_fmt("[BYPASS] << xpn_close(%d) -> %d", fd, ret);
    } else {
        ret = PROXY(close)(fd);
        debug_info_fmt("[BYPASS] << PROXY(close)(%d) -> %d", fd, ret);
    }
    return ret;
}

extern "C" int rename(const char *old_path, const char *new_path) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin rename(%s, %s)", old_path, new_path);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(old_path) && is_xpn_prefix(new_path)) {
        scope_xpn;
        ret = xpn_rename(skip_xpn_prefix(old_path), skip_xpn_prefix(new_path));
        debug_info_fmt("[BYPASS] << xpn_rename(%s, %s) -> %d", old_path, new_path, ret);
    } else {
        ret = PROXY(rename)(old_path, new_path);
        debug_info_fmt("[BYPASS] << PROXY(rename)(%s, %s) -> %d", old_path, new_path, ret);
    }
    return ret;
}

extern "C" int unlink(const char *path) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin unlink(%s)", path);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_unlink(skip_xpn_prefix(path));
        debug_info_fmt("[BYPASS] << xpn_unlink(%s) -> %d", skip_xpn_prefix(path), ret);
    } else {
        ret = PROXY(unlink)((char *)path);
        debug_info_fmt("[BYPASS] << PROXY(unlink)(%s) -> %d", path, ret);
    }
    return ret;
}

extern "C" int remove(const char *path) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin remove(%s)", path);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        struct stat buf;
        scope_xpn;
        ret = xpn_stat(skip_xpn_prefix(path), &buf);
        if ((buf.st_mode & S_IFMT) == S_IFREG) {
            ret = xpn_unlink(skip_xpn_prefix(path));
            debug_info_fmt("[BYPASS] << xpn_unlink(%s) -> %d", skip_xpn_prefix(path), ret);
        } else if ((buf.st_mode & S_IFMT) == S_IFDIR) {
            ret = xpn_rmdir(skip_xpn_prefix(path));
            debug_info_fmt("[BYPASS] << xpn_rmdir(%s) -> %d", skip_xpn_prefix(path), ret);
        }
    } else {
        ret = PROXY(remove)((char *)path);
        debug_info_fmt("[BYPASS] << remove(%s) -> %d", path, ret);
    }
    return ret;
}

// File API (stdio)
extern "C" int fileno(FILE *stream) {
    int ret;
    debug_info_fmt("[BYPASS] >> Begin fileno(%p)", stream);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (fdstable_get(stream)) {
        ret = xpn_fileno(stream);
        debug_info_fmt("[BYPASS] << xpn_fileno(%p) -> %d", stream, ret);
    } else {
        ret = PROXY(fileno)(stream);
        debug_info_fmt("[BYPASS] << PROXY(fileno) (%p) -> %d", stream, ret);
    }
    return ret;
}

extern "C" FILE *fopen(const char *path, const char *mode) {
    FILE *ret;
    debug_info_fmt("[BYPASS] >> Begin fopen(%s, %s)", path, mode);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_fopen(skip_xpn_prefix(path), mode);
        if (ret != nullptr) {
            debug_info_fmt("[BYPASS] << xpn_fopen(%s, %s) fileno %d", path, mode, fileno(ret));
            ret = fdstable_put(ret);
            debug_info_fmt("[BYPASS] << xpn_fopen(%s, %s) fileno %d", path, mode, fileno(ret));
        }
        debug_info_fmt("[BYPASS] << xpn_fopen(%s, %s) -> %p fd %d", skip_xpn_prefix(path), mode, ret, fileno(ret));
    } else {
        ret = PROXY(fopen)((const char *)path, mode);
        debug_info_fmt("[BYPASS] << PROXY(fopen) (%s, %s) -> %p", path, mode, ret);
    }
    return ret;
}

extern "C" FILE *fdopen(int fd, const char *mode) {
    FILE *fp;
    debug_info_fmt("[BYPASS] >> Begin fdopen(%d, %s)", fd, mode);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        // TODO: fix with fdstable_put
        fp = PROXY(fopen)("/dev/null", mode);
        fp->_fileno = fd;
    } else {
        fp = PROXY(fdopen)(fd, mode);
    }
    debug_info_fmt("[BYPASS] << PROXY(fdopen)(%d, %s) -> %p fd %d", fd, mode, fp, fileno(fp));
    return fp;
}

extern "C" int fclose(FILE *stream) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin fclose(%p)", stream);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(stream)) {
        scope_xpn;
        ret = xpn_fclose(stream);
        fdstable_remove(stream);
        debug_info_fmt("[BYPASS] << xpn_close(%p) -> %d", stream, ret);
    } else {
        ret = PROXY(fclose)(stream);
        debug_info_fmt("[BYPASS] << PROXY(fclose)(%p) -> %d", stream, ret);
    }
    return ret;
}

extern "C" size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    size_t ret = (size_t)-1;
    debug_info_fmt("[BYPASS] >> Begin fread(%p, %ld, %ld, %p)", ptr, size, nmemb, stream);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(stream)) {
        scope_xpn;
        ret = xpn_fread(ptr, size, nmemb, stream);
        debug_info_fmt("[BYPASS] << xpn_fread(%p, %ld, %ld, %p) -> %ld", ptr, size, nmemb, stream, ret);
    } else {
        ret = PROXY(fread)(ptr, size, nmemb, stream);
        debug_info_fmt("[BYPASS] << PROXY(fread)(%p, %ld, %ld, %p) -> %ld", ptr, size, nmemb, stream, ret);
    }
    return ret;
}

extern "C" size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
    size_t ret = (size_t)-1;
    debug_info_fmt("[BYPASS] >> Begin fwrite(%p, %ld, %ld, %p)", ptr, size, nmemb, stream);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(stream)) {
        scope_xpn;
        ret = xpn_fwrite(ptr, size, nmemb, stream);
        debug_info_fmt("[BYPASS] << xpn_fwrite(%p, %ld, %ld, %p) -> %ld", ptr, size, nmemb, stream, ret);
    } else {
        ret = PROXY(fwrite)(ptr, size, nmemb, stream);
        debug_info_fmt("[BYPASS] << PROXY(fwrite)(%p, %ld, %ld, %p) -> %ld", ptr, size, nmemb, stream, ret);
    }
    return ret;
}

extern "C" int fseek(FILE *stream, long int offset, int whence) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin fseek(%p, %ld, %d)", stream, offset, whence);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(stream)) {
        scope_xpn;
        ret = xpn_fseek(stream, offset, whence);
        debug_info_fmt("[BYPASS] << xpn_fseek(%p, %ld, %d) -> %d", stream, offset, whence, ret);
    } else {
        ret = PROXY(fseek)(stream, offset, whence);
        debug_info_fmt("[BYPASS] << PROXY(fseek)(%p, %ld, %d) -> %d", stream, offset, whence, ret);
    }
    return ret;
}

extern "C" long ftell(FILE *stream) {
    long ret = -1;
    debug_info_fmt("[BYPASS] >> Begin ftell(%p)", stream);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(stream)) {
        scope_xpn;
        ret = xpn_ftell(stream);
        debug_info_fmt("[BYPASS] << xpn_ftell(%p) -> %ld", stream, ret);
    } else {
        ret = PROXY(ftell)(stream);
        debug_info_fmt("[BYPASS] << PROXY(ftell)(%p) -> %ld", stream, ret);
    }
    return ret;
}

extern "C" void rewind(FILE *stream) {
    debug_info_fmt("[BYPASS] >> Begin rewind(%p)", stream);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(stream)) {
        scope_xpn;
        xpn_rewind(stream);
        debug_info_fmt("[BYPASS] << xpn_rewind(%p)", stream);
    } else {
        PROXY(rewind)(stream);
        debug_info_fmt("[BYPASS] << PROXY(rewind)(%p)", stream);
    }
}

extern "C" int feof(FILE *stream) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin feof(%p)", stream);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(stream)) {
        scope_xpn;
        int ret1, ret2;
        int fd = xpn_fileno(stream);
        ret1 = xpn_lseek(fd, 0, SEEK_CUR);
        if (ret1 == -1) {
            debug_info_fmt("[BYPASS] << xpn_lseek(%d, %d, %d) -> %d", fd, 0, SEEK_CUR, ret);
            return ret1;
        }
        ret2 = xpn_lseek(fd, 0, SEEK_END);
        if (ret2 != -1) {
            debug_info_fmt("[BYPASS] << xpn_lseek(%d, %d, %d) -> %d", fd, 0, SEEK_END, ret);
            return ret2;
        }
        if (ret1 != ret2) {
            ret = 0;
        } else {
            ret = 1;
        }
        debug_info_fmt("[BYPASS] << xpn_lseek(%d) -> %d", fd, ret);
    } else {
        ret = PROXY(feof)(stream);
        debug_info_fmt("[BYPASS] << PROXY(feof)(%p) -> %d", stream, ret);
    }
    return ret;
}

void clearerr(FILE *stream) {
    debug_info_fmt("[BYPASS] >> Begin clearerr(%p)", stream);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(stream)) {
        scope_xpn;
        // TODO
        debug_info_fmt("[BYPASS] << xpn_clearerr(%p)", stream);
    } else {
        PROXY(clearerr)(stream);
        debug_info_fmt("[BYPASS] << PROXY(clearerr)(%p)", stream);
    }
}

// Directory API

extern "C" int mkdir(const char *path, mode_t mode) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin mkdir(%s, %d)", path, mode);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_mkdir(skip_xpn_prefix(path), mode);
        debug_info_fmt("[BYPASS] << xpn_mkdir(%s, %d) -> %d", skip_xpn_prefix(path), mode, ret);
    } else {
        ret = PROXY(mkdir)((char *)path, mode);
        debug_info_fmt("[BYPASS] << PROXY(mkdir)(%s, %d) -> %d", path, mode, ret);
    }
    return ret;
}

extern "C" DIR *opendir(const char *dirname) {
    DIR *ret;
    debug_info_fmt("[BYPASS] >> Begin opendir(%p)", dirname);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(dirname)) {
        scope_xpn;
        ret = xpn_opendir(skip_xpn_prefix(dirname));
        if (ret != NULL) {
            fdstable_put(ret);
        }
        debug_info_fmt("[BYPASS] << xpn_opendir(%s) -> %p", skip_xpn_prefix(dirname), ret);
    } else {
        ret = PROXY(opendir)((char *)dirname);
        debug_info_fmt("[BYPASS] << PROXY(mkdir)(%s) -> %p", dirname, ret);
    }
    return ret;
}

extern "C" struct dirent *readdir(DIR *dirp) {
    struct dirent *ret;
    debug_info_fmt("[BYPASS] >> Begin readdir(%p)", dirp);
    if (fdstable_get(dirp)) {
        scope_xpn;
        ret = xpn_readdir(dirp);
        debug_info_fmt("[BYPASS] << xpn_readdir(%p) -> %p", dirp, ret);
    } else {
        ret = PROXY(readdir)(dirp);
        debug_info_fmt("[BYPASS] << PROXY(readdir)(%p) -> %p", dirp, ret);
    }
    return ret;
}

extern "C" struct dirent64 *readdir64(DIR *dirp) {
    struct dirent *aux;
    struct dirent64 *ret = NULL;
    debug_info_fmt("[BYPASS] >> Begin readdir64(%p)", dirp);
    if (fdstable_get(dirp)) {
        scope_xpn;
        aux = xpn_readdir(dirp);
        if (aux != NULL) {
            // TODO: change to static memory per dir... or where memory is free?
            ret = (struct dirent64 *)malloc(sizeof(struct dirent64));
            ret->d_ino = (__ino64_t)aux->d_ino;
            ret->d_off = (__off64_t)aux->d_off;
            ret->d_reclen = aux->d_reclen;
            ret->d_type = aux->d_type;
            strcpy(ret->d_name, aux->d_name);
        }
        debug_info_fmt("[BYPASS] << xpn_readdir(%p) -> %p", dirp, ret);
    } else {
        ret = PROXY(readdir64)(dirp);
        debug_info_fmt("[BYPASS] << PROXY(readdir64)(%p) -> %p", dirp, ret);
    }
    return ret;
}

extern "C" int closedir(DIR *dirp) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin closedir(%p)", dirp);
    if (fdstable_get(dirp)) {
        scope_xpn;
        fdstable_remove(dirp);
        ret = xpn_closedir(dirp);
        debug_info_fmt("[BYPASS] << xpn_closedir(%p) -> %d", dirp, ret);
    } else {
        ret = PROXY(closedir)(dirp);
        debug_info_fmt("[BYPASS] << PROXY(closedir)(%p) -> %d", dirp, ret);
    }
    return ret;
}

extern "C" int rmdir(const char *path) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin rmdir(%s)", path);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_rmdir(skip_xpn_prefix(path));
        debug_info_fmt("[BYPASS] << xpn_closedir(%s) -> %d", skip_xpn_prefix(path), ret);
    } else {
        ret = PROXY(rmdir)((char *)path);
        debug_info_fmt("[BYPASS] << PROXY(rmdir)(%s) -> %d", path, ret);
    }
    return ret;
}

// Proccess API

extern "C" pid_t fork(void) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin fork()");
    ret = PROXY(fork)();
    if (0 == ret) {
        // We want the children to be initialized
        check_xpn_init_initialized = false;
    }
    debug_info_fmt("[BYPASS] << fork() -> %d", ret);
    return ret;
}

// extern "C" int pipe(int pipefd[2]) {
//     debug_info_fmt("[BYPASS] >> Begin pipe()");
//     // debug_info_fmt("[BYPASS]    1) fd1 " << pipefd[0]);
//     // debug_info_fmt("[BYPASS]    2) fd2 " << pipefd[1]);
//     debug_info_fmt("[BYPASS]\t try to PROXY(pipe)");

//     int ret = PROXY(pipe)(pipefd);

//     // debug_info_fmt("[BYPASS]\t PROXY(pipe) -> " << ret);
//     debug_info_fmt("[BYPASS] << After pipe()");

//     return ret;
// }

extern "C" int dup(int fd) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin dup(%d)", fd);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_dup(fd);
        debug_info_fmt("[BYPASS] << xpn_dup(%d) -> %d", fd, ret);
    } else {
        ret = PROXY(dup)(fd);
        debug_info_fmt("[BYPASS] << PROXY(dup)(%d) -> %d", fd, ret);
    }
    return ret;
}

extern "C" int dup2(int fd, int fd2) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin dup2(%d, %d)", fd, fd2);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_dup2(fd, fd2);
        // TODO: fix to insert in table
        fdstable_put(fd2);
        debug_info_fmt("[BYPASS] << xpn_dup2(%d, %d) -> %d", fd, fd2, ret);
    } else {
        ret = PROXY(dup2)(fd, fd2);
        debug_info_fmt("[BYPASS] << PROXY(dup2)(%d, %d) -> %d", fd, fd2, ret);
    }
    return ret;
}

// void exit(int status) {
//     debug_info_fmt("[BYPASS] >> Begin exit...");
//     debug_info_fmt("[BYPASS]    1) status " << status);

//     if (xpn_adaptor_initCalled == 1) {
//         debug_info_fmt("[BYPASS] xpn_destroy");

//         xpn_destroy();
//     }

//     debug_info_fmt("[BYPASS] PROXY(exit)");

//     PROXY(exit)(status);
//     __builtin_unreachable();

//     debug_info_fmt("[BYPASS] << After exit()");
// }

// Manager API

extern "C" int chdir(const char *path) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin chdir(%s)", path);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_chdir((char *)skip_xpn_prefix(path));
        debug_info_fmt("[BYPASS] << xpn_chdir(%s) -> %d", skip_xpn_prefix(path), ret);
    } else {
        ret = PROXY(chdir)((char *)path);
        debug_info_fmt("[BYPASS] << PROXY(chdir)(%s) -> %d", path, ret);
    }
    return ret;
}

extern "C" int chmod(const char *path, mode_t mode) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin chmod(%s, %d)", path, mode);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_chmod(skip_xpn_prefix(path), mode);
        debug_info_fmt("[BYPASS] << xpn_chmod(%s, %d) -> %d", skip_xpn_prefix(path), mode, ret);
    } else {
        ret = PROXY(chmod)((char *)path, mode);
        debug_info_fmt("[BYPASS] << PROXY(chmod)(%s, %d) -> %d", path, mode, ret);
    }
    return ret;
}

extern "C" int fchmod(int fd, mode_t mode) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin chmod(%d, %d)", fd, mode);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_fchmod(fd, mode);
        debug_info_fmt("[BYPASS] << xpn_chmod(%d, %d) -> %d", fd, mode, ret);
    } else {
        ret = PROXY(fchmod)(fd, mode);
        debug_info_fmt("[BYPASS] << PROXY(fchmod)(%d, %d) -> %d", fd, mode, ret);
    }
    return ret;
}

extern "C" int chown(const char *path, uid_t owner, gid_t group) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin chown(%s, %d, %d)", path, owner, group);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_chown(skip_xpn_prefix(path), owner, group);
        debug_info_fmt("[BYPASS] << xpn_chown(%s, %d, %d) -> %d", skip_xpn_prefix(path), owner, group, ret);
    } else {
        ret = PROXY(chown)((char *)path, owner, group);
        debug_info_fmt("[BYPASS] << PROXY(chown)(%s, %d, %d) -> %d", path, owner, group, ret);
    }
    return ret;
}

extern "C" int fcntl(int fd, int cmd, ...)  // TODO
{
    long arg = 0;
    va_list ap;
    va_start(ap, cmd);
    arg = va_arg(ap, long);
    va_end(ap);
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin fcntl(%d, %d, %ld)", fd, cmd, arg);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        // TODO
        ret = 0;
        debug_info_fmt("[BYPASS] << todo xpn_fcntl(%d, %d, %ld) -> %d", fd, cmd, arg, ret);
    } else {
        ret = PROXY(fcntl)(fd, cmd, arg);
        debug_info_fmt("[BYPASS] << PROXY(fcntl)(%d, %d, %ld) -> %d", fd, cmd, arg, ret);
    }
    return ret;
}

extern "C" int access(const char *path, int mode) {
    int ret = -1;
    struct stat stats;
    debug_info_fmt("[BYPASS] >> Begin access(%s, %d)", path, mode);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        if (stat(path, &stats)) {
            debug_info_fmt("[BYPASS] << stat access(%s, %d) -> -1", path, mode);
            return -1;
        }
        if (mode == F_OK) {
            debug_info_fmt("[BYPASS] << F_OK access(%s, %d) -> 0", path, mode);
            return 0;
        }
        if ((mode & X_OK) == 0 || (stats.st_mode & (S_IXUSR | S_IXGRP | S_IXOTH))) {
            debug_info_fmt("[BYPASS] << stat access(%s, %d) -> 0", path, mode);
            return 0;
        }
    } else {
        ret = PROXY(access)(path, mode);
        debug_info_fmt("[BYPASS] << PROXY(access)(%s, %d) -> %d", path, mode, ret);
    }
    return ret;
}

extern "C" char *realpath(const char *__restrict__ path, char *__restrict__ resolved_path) {
    debug_info_fmt("[BYPASS] >> Begin realpath(%s, %s)", path, resolved_path);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        strcpy(resolved_path, path);
        debug_info_fmt("[BYPASS] << xpn_realpath(%s, %s) -> %s", path, resolved_path, resolved_path);
        return resolved_path;
    } else {
        char *ret = PROXY(realpath)(path, resolved_path);
        debug_info_fmt("[BYPASS] << PROXY(realpath)(%s, %s) -> %s", path, resolved_path, ret);
        return ret;
    }
}

extern "C" char *__realpath_chk(const char *path, char *resolved_path,
                                __attribute__((__unused__)) size_t resolved_len) {
    debug_info_fmt("[BYPASS] >> Begin __realpath_chk(%s, %s, %ld)", path, resolved_path, resolved_len);

    // TODO: taken from
    // https://refspecs.linuxbase.org/LSB_4.1.0/LSB-Core-generic/LSB-Core-generic/libc---realpath-chk-1.html
    // -> ... If resolved_len is less than PATH_MAX, then the function shall abort, and the program calling it shall
    // exit.
    //
    // if (resolved_len < PATH_MAX) {
    //    return -1;
    //}

    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        strcpy(resolved_path, path);
        debug_info_fmt("[BYPASS] << xpn_realpath(%s, %s) -> %s", path, resolved_path, resolved_path);
        return resolved_path;
    } else {
        char *ret = PROXY(realpath)(path, resolved_path);
        debug_info_fmt("[BYPASS] << PROXY(realpath)(%s, %s) -> %s", path, resolved_path, ret);
        return ret;
    }
}

extern "C" int fsync(int fd)  // TODO
{
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin fsync(%d)", fd);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        // TODO
        ret = 0;
        debug_info_fmt("[BYPASS] << xpn_fsync(%d) -> %d", fd, ret);
    } else {
        ret = PROXY(fsync)(fd);
        debug_info_fmt("[BYPASS] << PROXY(fsync)(%d) -> %d", fd, ret);
    }
    return ret;
}

extern "C" int flock(int fd, int operation) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin flock(%d, %d)", fd, operation);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        // TODO
        ret = 0;
        debug_info_fmt("[BYPASS] << xpn_flock(%d, %d) -> %d", fd, operation, ret);
    } else {
        ret = PROXY(flock)(fd, operation);
        debug_info_fmt("[BYPASS] << PROXY(flock)(%d, %d) -> %d", fd, operation, ret);
    }
    return ret;
}

extern "C" int statvfs(const char *path, struct statvfs *buf) {
    int ret;
    debug_info_fmt("[BYPASS] >> Begin statvfs(%s, %p)", path, buf);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_statvfs(skip_xpn_prefix(path), buf);
        debug_info_fmt("[BYPASS] << xpn_statvfs(%s, %p) -> %d", skip_xpn_prefix(path), buf, ret);
    } else {
        ret = PROXY(statvfs)(path, buf);
        debug_info_fmt("[BYPASS] << PROXY(statvfs)(%s, %p) -> %d", path, buf, ret);
    }
    return ret;
}

extern "C" int fstatvfs(int fd, struct statvfs *buf) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin fstatvfs(%d, %p)", fd, buf);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_fstatvfs(fd, buf);
        debug_info_fmt("[BYPASS] << xpn_fstatvfs(%d, %p) -> %d", fd, buf, ret);
    } else {
        ret = PROXY(fstatvfs)(fd, buf);
        debug_info_fmt("[BYPASS] << PROXY(fstatvfs)(%d, %p) -> %d", fd, buf, ret);
    }
    return ret;
}

extern "C" int statfs(const char *path, struct statfs *buf) {
    int ret;
    debug_info_fmt("[BYPASS] >> Begin statfs(%s, %p)", path, buf);
    // This if checks if variable path passed as argument starts with the expand prefix.
    if (is_xpn_prefix(path)) {
        scope_xpn;
        ret = xpn_statfs(skip_xpn_prefix(path), buf);
        debug_info_fmt("[BYPASS] << xpn_statfs(%s, %p) -> %d", skip_xpn_prefix(path), buf, ret);
    } else {
        ret = PROXY(statfs)(path, buf);
        debug_info_fmt("[BYPASS] << PROXY(statfs)(%s, %p) -> %d", path, buf, ret);
    }
    return ret;
}

extern "C" int fstatfs(int fd, struct statfs *buf) {
    int ret = -1;
    debug_info_fmt("[BYPASS] >> Begin fstatfs(%d, %p)", fd, buf);
    // This if checks if variable fd passed as argument is a expand fd.
    if (fdstable_get(fd)) {
        scope_xpn;
        ret = xpn_fstatfs(fd, buf);
        debug_info_fmt("[BYPASS] << xpn_fstatfs(%d, %p) -> %d", fd, buf, ret);
    } else {
        ret = PROXY(fstatfs)(fd, buf);
        debug_info_fmt("[BYPASS] << PROXY(fstatfs)(%d, %p) -> %d", fd, buf, ret);
    }
    return ret;
}

// MPI API
#if defined ENABLE_MPI_SERVER && not BUILD_WITH_DMTCP
extern "C" int MPI_Init(int *argc, char ***argv) {
    char *value;
    debug_info_fmt("[BYPASS] >> Begin MPI_Init");
    // We must initialize expand if it has not been initialized yet.
    check_xpn_init();
    // It is an XPN partition, so we redirect the syscall to expand syscall
    value = getenv("XPN_IS_MPI_SERVER");
    if (NULL == value) {
        debug_info_fmt("[BYPASS] << After MPI_Init");
        return PMPI_Init(argc, argv);
    }
    debug_info_fmt("[BYPASS] << After MPI_Init");
    return MPI_SUCCESS;
}

extern "C" int MPI_Init_thread(int *argc, char ***argv, int required, int *provided) {
    char *value;
    debug_info_fmt("[BYPASS] >> Begin MPI_Init_thread");
    // We must initialize expand if it has not been initialized yet.
    check_xpn_init();
    // It is an XPN partition, so we redirect the syscall to expand syscall
    value = getenv("XPN_IS_MPI_SERVER");
    if (NULL == value) {
        debug_info_fmt("[BYPASS] << After MPI_Init_thread");
        return PMPI_Init_thread(argc, argv, required, provided);
    }
    debug_info_fmt("[BYPASS] << After MPI_Init_thread");
    return MPI_SUCCESS;
}

extern "C" int MPI_Finalize(void) {
    char *value;
    debug_info_fmt("[BYPASS] >> Begin MPI_Finalize");
    value = getenv("XPN_IS_MPI_SERVER");
    if (NULL != value && check_xpn_init_initialized == 1) {
        debug_info_fmt("[BYPASS] xpn_destroy");
        xpn_destroy();
    }
    debug_info_fmt("[BYPASS] << After MPI_Finalize");
    return PMPI_Finalize();
}
#endif


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
#define DEBUG
#include "dmtcp.h"
extern "C" {
// #include <signal.h>
#include <dirent.h>
#include <dlfcn.h>
#include <pthread.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <time.h>
}

#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string_view>
#include <unordered_set>

/* ... Const / Const ................................................. */

#ifdef DEBUG
#define debug_info(...)                             \
    {                                               \
        NEXT_FNC(printf)("[%ld] ", pthread_self()); \
        NEXT_FNC(printf)(__VA_ARGS__);              \
        NEXT_FNC(printf)("\n");                     \
        NEXT_FNC(fflush)(stdout);                   \
    }
#else
#define debug_info(...)
#endif

#undef __USE_FILE_OFFSET64
#undef __USE_LARGEFILE64

// Types
#ifndef O_ACCMODE
#define O_ACCMODE 00000003
#endif
#ifndef O_RDONLY
#define O_RDONLY 00000000
#endif
#ifndef O_WRONLY
#define O_WRONLY 00000001
#endif
#ifndef O_RDWR
#define O_RDWR 00000002
#endif
#ifndef O_CREAT
#define O_CREAT 00000100   // not fcntl
#endif
#ifndef O_EXCL
#define O_EXCL 00000200    // not fcntl
#endif
#ifndef O_NOCTTY
#define O_NOCTTY 00000400  // not fcntl
#endif
#ifndef O_TRUNC
#define O_TRUNC 00001000   // not fcntl
#endif
#ifndef O_APPEND
#define O_APPEND 00002000
#endif
#ifndef O_NONBLOCK
#define O_NONBLOCK 00004000
#endif
#ifndef O_DSYNC
#define O_DSYNC 00010000   // used to be O_SYNC, see below
#endif
#ifndef FASYNC
#define FASYNC 00020000    // fcntl, for BSD compatibility
#endif
#ifndef O_DIRECT
#define O_DIRECT 00040000  // direct disk access hint
#endif
#ifndef O_LARGEFILE
#define O_LARGEFILE 00100000
#endif
#ifndef O_DIRECTORY
#define O_DIRECTORY 00200000  // must be a directory
#endif
#ifndef O_NOFOLLOW
#define O_NOFOLLOW 00400000   // don't follow links
#endif
#ifndef O_NOATIME
#define O_NOATIME 01000000
#endif
#ifndef O_CLOEXEC
#define O_CLOEXEC 02000000  // set close_on_exec */
#endif

// for access
#ifndef R_OK
#define R_OK 4
#endif
#ifndef W_OK
#define W_OK 2
#endif
#ifndef X_OK
#define X_OK 1
#endif
#ifndef F_OK
#define F_OK 0
#endif

/* ... Functions / Funciones ......................................... */

// File API
extern "C" int open(const char *path, int flags, ...) {
    int ret;
    va_list ap;
    mode_t mode = 0;
    va_start(ap, flags);
    mode = va_arg(ap, mode_t);
    va_end(ap);
    debug_info("[BYPASS] >> Begin open(%s, %d, %d)", path, flags, mode);
    ret = NEXT_FNC(open)((char *)path, flags, mode);
    debug_info("[BYPASS] << NEXT_FNC(open)(%s, %d, %d) -> %d", path, flags, mode, ret);
    return ret;
}

extern "C" int open64(const char *path, int flags, ...) {
    int ret;
    va_list ap;
    mode_t mode = 0;
    va_start(ap, flags);
    mode = va_arg(ap, mode_t);
    va_end(ap);
    debug_info("[BYPASS] >> Begin open64(%s, %d, %d)", path, flags, mode);
    ret = NEXT_FNC(open64)((char *)path, flags, mode);
    debug_info("[BYPASS] << NEXT_FNC(open64)(%s, %d, %d) -> %d", path, flags, mode, ret);
    return ret;
}

#ifndef HAVE_ICC

extern "C" int __open_2(const char *path, int flags, ...) {
    int ret;
    va_list ap;
    mode_t mode = 0;
    va_start(ap, flags);
    mode = va_arg(ap, mode_t);
    va_end(ap);
    debug_info("[BYPASS] >> Begin __open_2(%s, %d, %d)", path, flags, mode);
    ret = NEXT_FNC(__open_2)((char *)path, flags, mode);
    debug_info("[BYPASS] << NEXT_FNC(__open_2)(%s, %d, %d) -> %d", path, flags, mode, ret);
    return ret;
}

#endif

extern "C" int creat(const char *path, mode_t mode) {
    int ret;
    debug_info("[BYPASS] >> Begin creat(%s, %d)", path, mode);
    ret = NEXT_FNC(creat)(path, mode);
    debug_info("[BYPASS] << NEXT_FNC(creat)(%s, %d) -> %d", path, mode, ret);
    debug_info("[BYPASS] << After creat....");
    return ret;
}

extern "C" int mkstemp(char *templ) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin mkstemp(%s)", templ);
    ret = NEXT_FNC(mkstemp)(templ);
    debug_info("[BYPASS] << NEXT_FNC(mkstemp)(%s) -> %d", templ, ret);
    return ret;
}

extern "C" int ftruncate(int fd, off_t length) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin ftruncate(%d, %ld)", fd, length);
    ret = NEXT_FNC(ftruncate)(fd, length);
    debug_info("[BYPASS] << NEXT_FNC(ftruncate)(%d, %ld) -> %d", fd, length, ret);
    return ret;
}

extern "C" ssize_t read(int fd, void *buf, size_t nbyte) {
    ssize_t ret = -1;
    debug_info("[BYPASS] >> Begin read(%d, %p, %ld)", fd, buf, nbyte);
    ret = NEXT_FNC(read)(fd, buf, nbyte);
    debug_info("[BYPASS] << NEXT_FNC(read)(%d, %p, %ld) -> %ld", fd, buf, nbyte, ret);
    return ret;
}

extern "C" ssize_t write(int fd, const void *buf, size_t nbyte) {
    ssize_t ret = -1;
    debug_info("[BYPASS] >> Begin write(%d, %p, %ld)", fd, buf, nbyte);
    ret = NEXT_FNC(write)(fd, (void *)buf, nbyte);
    debug_info("[BYPASS] << NEXT_FNC(write)(%d, %p, %ld) -> %ld", fd, buf, nbyte, ret);
    return ret;
}

extern "C" ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
    ssize_t ret = -1;
    debug_info("[BYPASS] >> Begin pread(%d, %p, %ld, %ld)", fd, buf, count, offset);
    ret = NEXT_FNC(pread)(fd, buf, count, offset);
    debug_info("[BYPASS] << NEXT_FNC(pread)(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    return ret;
}

extern "C" ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
    ssize_t ret = -1;
    debug_info("[BYPASS] >> Begin pwrite(%d, %p, %ld, %ld)", fd, buf, count, offset);
    ret = NEXT_FNC(pwrite)(fd, buf, count, offset);
    debug_info("[BYPASS] << NEXT_FNC(pwrite)(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    return ret;
}

extern "C" ssize_t pread64(int fd, void *buf, size_t count, off_t offset) {
    ssize_t ret = -1;
    debug_info("[BYPASS] >> Begin pread64(%d, %p, %ld, %ld)", fd, buf, count, offset);
    ret = NEXT_FNC(pread64)(fd, buf, count, offset);
    debug_info("[BYPASS] << NEXT_FNC(pread64)(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    return ret;
}

extern "C" ssize_t pwrite64(int fd, const void *buf, size_t count, off_t offset) {
    ssize_t ret = -1;
    debug_info("[BYPASS] >> Begin pwrite64(%d, %p, %ld, %ld)", fd, buf, count, offset);
    ret = NEXT_FNC(pwrite64)(fd, buf, count, offset);
    debug_info("[BYPASS] << NEXT_FNC(pwrite64)(%d, %p, %ld, %ld) -> %ld", fd, buf, count, offset, ret);
    return ret;
}

extern "C" off_t lseek(int fd, off_t offset, int whence) {
    off_t ret = (off_t)-1;
    debug_info("[BYPASS] >> Begin lseek(%d, %ld, %d)", fd, offset, whence);
    ret = NEXT_FNC(lseek)(fd, offset, whence);
    debug_info("[BYPASS] << NEXT_FNC(lseek)(%d, %ld, %d) -> %ld", fd, offset, whence, ret);
    return ret;
}

extern "C" off64_t lseek64(int fd, off64_t offset, int whence) {
    off64_t ret = (off64_t)-1;
    debug_info("[BYPASS] >> Begin lseek64(%d, %ld, %d)", fd, offset, whence);
    ret = NEXT_FNC(lseek64)(fd, offset, whence);
    debug_info("[BYPASS] << NEXT_FNC(lseek64)(%d, %ld, %d) -> %ld", fd, offset, whence, ret);
    return ret;
}

#ifndef _STAT_VER
extern "C" int stat(const char *path, struct stat *buf) {
    int ret;
    debug_info("[BYPASS] >> Begin stat(%s, %p)", path, buf);
    ret = NEXT_FNC(stat)((const char *)path, buf);
    debug_info("[BYPASS] << NEXT_FNC(stat)(%s, %p) -> %d", path, buf, ret);
    return ret;
}
#endif

extern "C" int __lxstat64(int ver, const char *path, struct stat64 *buf) {
    int ret;
    debug_info("[BYPASS] >> Begin __lxstat64(%d, %s, %p)", ver, path, buf);
    ret = NEXT_FNC(__lxstat64)(ver, (const char *)path, buf);
    debug_info("[BYPASS] << NEXT_FNC(__lxstat64)(%d, %s, %p) -> %d", ver, path, buf, ret);
    return ret;
}

extern "C" int __xstat64(int ver, const char *path, struct stat64 *buf) {
    int ret;
    debug_info("[BYPASS] >> Begin __xstat64(%d, %s, %p)", ver, path, buf);
    ret = NEXT_FNC(__xstat64)(ver, (const char *)path, buf);
    debug_info("[BYPASS] << NEXT_FNC(__xstat64)(%d, %s, %p) -> %d", ver, path, buf, ret);
    return ret;
}

extern "C" int __fxstat64(int ver, int fd, struct stat64 *buf) {
    int ret;
    debug_info("[BYPASS] >> Begin __fxstat64(%d, %d, %p)", ver, fd, buf);
    ret = NEXT_FNC(__fxstat64)(ver, fd, buf);
    debug_info("[BYPASS] << NEXT_FNC(__fxstat64)(%d, %d, %p) -> %d", ver, fd, buf, ret);
    return ret;
}

extern "C" int __lxstat(int ver, const char *path, struct stat *buf) {
    int ret;
    debug_info("[BYPASS] >> Begin __lxstat(%d, %s, %p)", ver, path, buf);
    ret = NEXT_FNC(__lxstat)(ver, (const char *)path, buf);
    debug_info("[BYPASS] << NEXT_FNC(__lxstat)(%d, %s, %p) -> %d", ver, path, buf, ret);
    return ret;
}

extern "C" int __xstat(int ver, const char *path, struct stat *buf) {
    int ret;
    debug_info("[BYPASS] >> Begin __xstat(%d, %s, %p)", ver, path, buf);
    ret = NEXT_FNC(__xstat)(ver, (const char *)path, buf);
    debug_info("[BYPASS] << NEXT_FNC(__xstat)(%d, %s, %p) -> %d", ver, path, buf, ret);
    return ret;
}

#ifndef _STAT_VER
extern "C" int fstat(int fd, struct stat *buf) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin fstat(%d, %p)", fd, buf);
    ret = NEXT_FNC(fstat)(fd, buf);
    debug_info("[BYPASS] << NEXT_FNC(fstat)(%d, %p) -> %d", fd, buf, ret);
    return ret;
}
#endif

extern "C" int __fxstat(int ver, int fd, struct stat *buf) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin __fxstat(%d, %d, %p)", ver, fd, buf);
    ret = NEXT_FNC(__fxstat)(ver, fd, buf);
    debug_info("[BYPASS] << NEXT_FNC(__fxstat)(%d, %d, %p) -> %d", ver, fd, buf, ret);
    return ret;
}

extern "C" int __fxstatat64(int ver, int dirfd, const char *path, struct stat64 *buf, int flags) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin __fxstatat64(%d, %d, %s, %p, %d)", ver, dirfd, path, buf, flags);
    ret = NEXT_FNC(__fxstatat64)(ver, dirfd, path, buf, flags);
    debug_info("[BYPASS] << NEXT_FNC(__fxstatat64)(%d, %d, %s, %p, %d) -> %d", ver, dirfd, path, buf, flags, ret);
    return ret;
}

extern "C" int __fxstatat(int ver, int dirfd, const char *path, struct stat *buf, int flags) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin __fxstatat(%d, %d, %s, %p, %d)", ver, dirfd, path, buf, flags);
    ret = NEXT_FNC(__fxstatat)(ver, dirfd, path, buf, flags);
    debug_info("[BYPASS] << NEXT_FNC(__fxstatat)(%d, %d, %s, %p, %d) -> %d", ver, dirfd, path, buf, flags, ret);
    return ret;
}

extern "C" int close(int fd) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin close(%d)", fd);
    ret = NEXT_FNC(close)(fd);
    debug_info("[BYPASS] << NEXT_FNC(close)(%d) -> %d", fd, ret);
    return ret;
}

extern "C" int rename(const char *old_path, const char *new_path) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin rename(%s, %s)", old_path, new_path);
    ret = NEXT_FNC(rename)(old_path, new_path);
    debug_info("[BYPASS] << NEXT_FNC(rename)(%s, %s) -> %d", old_path, new_path, ret);
    return ret;
}

extern "C" int unlink(const char *path) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin unlink(%s)", path);
    ret = NEXT_FNC(unlink)((char *)path);
    debug_info("[BYPASS] << NEXT_FNC(unlink)(%s) -> %d", path, ret);
    return ret;
}

extern "C" int remove(const char *path) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin remove(%s)", path);
    ret = NEXT_FNC(remove)((char *)path);
    debug_info("[BYPASS] << remove(%s) -> %d", path, ret);
    return ret;
}

// File API (stdio)
extern "C" FILE *fopen(const char *path, const char *mode) {
    FILE *ret;
    debug_info("[BYPASS] >> Begin fopen(%s, %s)", path, mode);
    ret = NEXT_FNC(fopen)((const char *)path, mode);
    debug_info("[BYPASS] << NEXT_FNC(fopen) (%s, %s) -> %p", path, mode, ret);
    return ret;
}

extern "C" FILE *fdopen(int fd, const char *mode) {
    FILE *fp;
    debug_info("[BYPASS] >> Begin fdopen(%d, %s)", fd, mode);
    fp = NEXT_FNC(fdopen)(fd, mode);
    debug_info("[BYPASS] << NEXT_FNC(fdopen)(%d, %s) -> %p", fd, mode, fp);
    return fp;
}

extern "C" int fclose(FILE *stream) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin fclose(%p)", stream);
    ret = NEXT_FNC(fclose)(stream);
    debug_info("[BYPASS] << NEXT_FNC(fclose)(%p) -> %d", stream, ret);
    return ret;
}

extern "C" size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    size_t ret = (size_t)-1;
    debug_info("[BYPASS] >> Begin fread(%p, %ld, %ld, %p)", ptr, size, nmemb, stream);
    ret = NEXT_FNC(fread)(ptr, size, nmemb, stream);
    debug_info("[BYPASS] << NEXT_FNC(fread)(%p, %ld, %ld, %p) -> %ld", ptr, size, nmemb, stream, ret);
    return ret;
}

extern "C" size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
    size_t ret = (size_t)-1;
    debug_info("[BYPASS] >> Begin fwrite(%p, %ld, %ld, %p)", ptr, size, nmemb, stream);
    ret = NEXT_FNC(fwrite)(ptr, size, nmemb, stream);
    debug_info("[BYPASS] << NEXT_FNC(fwrite)(%p, %ld, %ld, %p) -> %ld", ptr, size, nmemb, stream, ret);
    return ret;
}

extern "C" int fseek(FILE *stream, long int offset, int whence) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin fseek(%p, %ld, %d)", stream, offset, whence);
    ret = NEXT_FNC(fseek)(stream, offset, whence);
    debug_info("[BYPASS] << NEXT_FNC(fseek)(%p, %ld, %d) -> %d", stream, offset, whence, ret);
    return ret;
}

extern "C" long ftell(FILE *stream) {
    long ret = -1;
    debug_info("[BYPASS] >> Begin ftell(%p)", stream);
    ret = NEXT_FNC(ftell)(stream);
    debug_info("[BYPASS] << NEXT_FNC(ftell)(%p) -> %ld", stream, ret);
    return ret;
}

extern "C" void rewind(FILE *stream) {
    debug_info("[BYPASS] >> Begin rewind(%p)", stream);
    NEXT_FNC(rewind)(stream);
    debug_info("[BYPASS] << NEXT_FNC(rewind)(%p)", stream);
}

extern "C" int feof(FILE *stream) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin feof(%p)", stream);
    ret = NEXT_FNC(feof)(stream);
    debug_info("[BYPASS] << NEXT_FNC(feof)(%p) -> %d", stream, ret);
    return ret;
}

// Directory API

extern "C" int mkdir(const char *path, mode_t mode) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin mkdir(%s, %d)", path, mode);
    ret = NEXT_FNC(mkdir)((char *)path, mode);
    debug_info("[BYPASS] << NEXT_FNC(mkdir)(%s, %d) -> %d", path, mode, ret);
    return ret;
}

extern "C" DIR *opendir(const char *dirname) {
    DIR *ret;
    debug_info("[BYPASS] >> Begin opendir(%p)", dirname);
    ret = NEXT_FNC(opendir)((char *)dirname);
    debug_info("[BYPASS] << NEXT_FNC(mkdir)(%s) -> %p", dirname, ret);
    return ret;
}

extern "C" struct dirent *readdir(DIR *dirp) {
    struct dirent *ret;
    debug_info("[BYPASS] >> Begin readdir(%p)", dirp);
    ret = NEXT_FNC(readdir)(dirp);
    debug_info("[BYPASS] << NEXT_FNC(readdir)(%p) -> %p", dirp, ret);
    return ret;
}

extern "C" struct dirent64 *readdir64(DIR *dirp) {
    struct dirent64 *ret = NULL;
    debug_info("[BYPASS] >> Begin readdir64(%p)", dirp);
    ret = NEXT_FNC(readdir64)(dirp);
    debug_info("[BYPASS] << NEXT_FNC(readdir64)(%p) -> %p", dirp, ret);
    return ret;
}

extern "C" int closedir(DIR *dirp) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin closedir(%p)", dirp);
    ret = NEXT_FNC(closedir)(dirp);
    debug_info("[BYPASS] << NEXT_FNC(closedir)(%p) -> %d", dirp, ret);
    return ret;
}

extern "C" int rmdir(const char *path) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin rmdir(%s)", path);
    ret = NEXT_FNC(rmdir)((char *)path);
    debug_info("[BYPASS] << NEXT_FNC(rmdir)(%s) -> %d", path, ret);
    return ret;
}

// Proccess API

// extern "C" pid_t fork(void) {
//     int ret = -1;
//     debug_info("[BYPASS] >> Begin fork()");
//     ret = NEXT_FNC(fork)();
//     debug_info("[BYPASS] << fork() -> %d", ret);
//     return ret;
// }

extern "C" int pipe(int pipefd[2]) {
    debug_info("[BYPASS] >> Begin pipe()");
    // debug_info("[BYPASS]    1) fd1 " << pipefd[0]);
    // debug_info("[BYPASS]    2) fd2 " << pipefd[1]);
    debug_info("[BYPASS]\t try to NEXT_FNC(pipe)");

    int ret = NEXT_FNC(pipe)(pipefd);

    // debug_info("[BYPASS]\t NEXT_FNC(pipe) -> " << ret);
    debug_info("[BYPASS] << After pipe()");

    return ret;
}

extern "C" int dup(int fd) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin dup(%d)", fd);
    ret = NEXT_FNC(dup)(fd);
    debug_info("[BYPASS] << NEXT_FNC(dup)(%d) -> %d", fd, ret);
    return ret;
}

extern "C" int dup2(int fd, int fd2) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin dup2(%d, %d)", fd, fd2);
    ret = NEXT_FNC(dup2)(fd, fd2);
    debug_info("[BYPASS] << NEXT_FNC(dup2)(%d, %d) -> %d", fd, fd2, ret);
    return ret;
}

// Manager API

extern "C" int chdir(const char *path) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin chdir(%s)", path);
    ret = NEXT_FNC(chdir)((char *)path);
    debug_info("[BYPASS] << NEXT_FNC(chdir)(%s) -> %d", path, ret);
    return ret;
}

extern "C" int chmod(const char *path, mode_t mode) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin chmod(%s, %d)", path, mode);
    ret = NEXT_FNC(chmod)((char *)path, mode);
    debug_info("[BYPASS] << NEXT_FNC(chmod)(%s, %d) -> %d", path, mode, ret);
    return ret;
}

extern "C" int fchmod(int fd, mode_t mode) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin chmod(%d, %d)", fd, mode);
    ret = NEXT_FNC(fchmod)(fd, mode);
    debug_info("[BYPASS] << NEXT_FNC(fchmod)(%d, %d) -> %d", fd, mode, ret);
    return ret;
}

extern "C" int chown(const char *path, uid_t owner, gid_t group) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin chown(%s, %d, %d)", path, owner, group);
    ret = NEXT_FNC(chown)((char *)path, owner, group);
    debug_info("[BYPASS] << NEXT_FNC(chown)(%s, %d, %d) -> %d", path, owner, group, ret);
    return ret;
}

// extern "C" int fcntl(int fd, int cmd, ...)  // TODO
// {
//     int ret = -1;
//     debug_info("[BYPASS] >> Begin fcntl(%d, %d, %ld)", fd, cmd, arg);
//     ret = NEXT_FNC(fcntl)(fd, cmd, arg);
//     debug_info("[BYPASS] << NEXT_FNC(fcntl)(%d, %d, %ld) -> %d", fd, cmd, arg, ret);
//     return ret;
// }

extern "C" int access(const char *path, int mode) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin access(%s, %d)", path, mode);
    ret = NEXT_FNC(access)(path, mode);
    debug_info("[BYPASS] << NEXT_FNC(access)(%s, %d) -> %d", path, mode, ret);
    return ret;
}

extern "C" char *realpath(const char *__restrict__ path, char *__restrict__ resolved_path) {
    debug_info("[BYPASS] >> Begin realpath(%s, %s)", path, resolved_path);
    char *ret = NEXT_FNC(realpath)(path, resolved_path);
    debug_info("[BYPASS] << NEXT_FNC(realpath)(%s, %s) -> %s", path, resolved_path, ret);
    return ret;
}

extern "C" char *__realpath_chk(const char *path, char *resolved_path,
                                __attribute__((__unused__)) size_t resolved_len) {
    debug_info("[BYPASS] >> Begin __realpath_chk(%s, %s, %ld)", path, resolved_path, resolved_len);
    char *ret = NEXT_FNC(realpath)(path, resolved_path);
    debug_info("[BYPASS] << NEXT_FNC(realpath)(%s, %s) -> %s", path, resolved_path, ret);
    return ret;
}

extern "C" int fsync(int fd)  // TODO
{
    int ret = -1;
    debug_info("[BYPASS] >> Begin fsync(%d)", fd);
    ret = NEXT_FNC(fsync)(fd);
    debug_info("[BYPASS] << NEXT_FNC(fsync)(%d) -> %d", fd, ret);
    return ret;
}

extern "C" int flock(int fd, int operation) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin flock(%d, %d)", fd, operation);
    ret = NEXT_FNC(flock)(fd, operation);
    debug_info("[BYPASS] << NEXT_FNC(flock)(%d, %d) -> %d", fd, operation, ret);
    return ret;
}

extern "C" int statvfs(const char *path, struct statvfs *buf) {
    int ret;
    debug_info("[BYPASS] >> Begin statvfs(%s, %p)", path, buf);
    ret = NEXT_FNC(statvfs)(path, buf);
    debug_info("[BYPASS] << NEXT_FNC(statvfs)(%s, %p) -> %d", path, buf, ret);
    return ret;
}

extern "C" int fstatvfs(int fd, struct statvfs *buf) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin fstatvfs(%d, %p)", fd, buf);
    ret = NEXT_FNC(fstatvfs)(fd, buf);
    debug_info("[BYPASS] << NEXT_FNC(fstatvfs)(%d, %p) -> %d", fd, buf, ret);
    return ret;
}

extern "C" int statfs(const char *path, struct statfs *buf) {
    int ret;
    debug_info("[BYPASS] >> Begin statfs(%s, %p)", path, buf);
    ret = NEXT_FNC(statfs)(path, buf);
    debug_info("[BYPASS] << NEXT_FNC(statfs)(%s, %p) -> %d", path, buf, ret);
    return ret;
}

extern "C" int fstatfs(int fd, struct statfs *buf) {
    int ret = -1;
    debug_info("[BYPASS] >> Begin fstatfs(%d, %p)", fd, buf);
    ret = NEXT_FNC(fstatfs)(fd, buf);
    debug_info("[BYPASS] << NEXT_FNC(fstatfs)(%d, %p) -> %d", fd, buf, ret);
    return ret;
}

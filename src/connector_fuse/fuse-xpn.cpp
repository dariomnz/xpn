/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  Copyright (C) 2011       Sebastian Pipping <sebastian@pipping.org>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall fusexmp_fh.c `pkg-config fuse --cflags --libs` -lulockmgr -o fusexmp_fh
*/

#define FUSE_USE_VERSION 314
// #define DEBUG

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <string>

#include "base_cpp/debug.hpp"
using namespace XPN;
#include "xpn.h"
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include <sys/file.h> /* flock(2) */

static void *xmp_init([[maybe_unused]] struct fuse_conn_info *conn, [[maybe_unused]] struct fuse_config *cfg) {
    int res;
    debug_info("[FUSE-XPN] Init");
    debug_info("[FUSE-XPN] proto_major " << conn->proto_major);
    debug_info("[FUSE-XPN] proto_minor " << conn->proto_minor);
    debug_info("[FUSE-XPN] max_write " << conn->max_write);
    debug_info("[FUSE-XPN] max_read " << conn->max_read);
    debug_info("[FUSE-XPN] max_readahead " << conn->max_readahead);
    debug_info("[FUSE-XPN] capable " << conn->capable);
    debug_info("[FUSE-XPN] want " << conn->want);
    debug_info("[FUSE-XPN] max_background " << conn->max_background);
    debug_info("[FUSE-XPN] congestion_threshold " << conn->congestion_threshold);
    debug_info("[FUSE-XPN] time_gran " << conn->time_gran);

    res = xpn_init();
    debug_info("[FUSE-XPN] End");
    if (res == -1) return (void *)static_cast<size_t>(-errno);

    return 0;
}

static void xmp_destroy(void *) {
    debug_info("[FUSE-XPN] Init");
    xpn_destroy();
    debug_info("[FUSE-XPN] End");
    return;
}

static int xmp_getattr(const char *path, struct stat *stbuf, [[maybe_unused]] fuse_file_info *fi) {
    int res;
    debug_info("[FUSE-XPN] Init getattr(" << path << ", " << stbuf << ")");

    std::string p(path);
    p.insert(0, "xpn");
    res = xpn_stat(p.c_str(), stbuf);

    debug_info("[FUSE-XPN] End getattr(" << path << ", " << stbuf << ")");
    if (res == -1) return -errno;

    return 0;
}

// TODO
// static int xmp_access(const char *path, int mask) {
//     int res;

//     res = xpn_access(path, mask);
//     if (res == -1) return -errno;

//     return 0;
// }

// TODO
// static int xmp_readlink(const char *path, char *buf, size_t size) {
//     int res;

//     res = xpn_readlink(path, buf, size - 1);
//     if (res == -1) return -errno;

//     buf[res] = '\0';
//     return 0;
// }

struct xmp_dirp {
    DIR *dp;
    struct dirent *entry;
    off_t offset;
};

static int xmp_opendir(const char *path, struct fuse_file_info *fi) {
    int res;
    debug_info("[FUSE-XPN] Init opendir(" << path << ", " << fi->fh << ")");
    struct xmp_dirp *d = (xmp_dirp *)malloc(sizeof(struct xmp_dirp));
    if (d == NULL) return -ENOMEM;

    std::string p(path);
    p.insert(0, "xpn");
    d->dp = xpn_opendir(p.c_str());
    if (d->dp == NULL) {
        res = -errno;
        free(d);
        debug_info("[FUSE-XPN] End opendir(" << path << ", " << fi->fh << ")");
        return res;
    }
    d->offset = 0;
    d->entry = NULL;

    fi->fh = (unsigned long)d;
    debug_info("[FUSE-XPN] End opendir(" << path << ", " << fi->fh << ")");
    return 0;
}

static inline struct xmp_dirp *get_dirp(struct fuse_file_info *fi) { return (struct xmp_dirp *)(uintptr_t)fi->fh; }

static int xmp_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi,
                       [[maybe_unused]] fuse_readdir_flags flags) {
    debug_info("[FUSE-XPN] Init readdir(" << path << ", " << buf << ", " << offset << ", " << fi->fh << ")");
    struct xmp_dirp *d = get_dirp(fi);

    (void)path;
    (void)offset;
    // if (offset != d->offset) {
    //     xpn_seekdir(d->dp, offset);
    //     d->entry = NULL;
    //     d->offset = offset;
    // }
    while (1) {
        struct stat st;
        off_t nextoff = 0;

        if (!d->entry) {
            d->entry = xpn_readdir(d->dp);
            if (!d->entry) break;
        }

        memset(&st, 0, sizeof(st));
        st.st_ino = d->entry->d_ino;
        st.st_mode = d->entry->d_type << 12;
        // nextoff = telldir(d->dp);
        if (filler(buf, d->entry->d_name, &st, nextoff, static_cast<enum fuse_fill_dir_flags>(0))) break;

        d->entry = NULL;
        // d->offset = nextoff;
    }

    debug_info("[FUSE-XPN] End readdir(" << path << ", " << buf << ", " << offset << ", " << fi->fh << ")");
    return 0;
}

static int xmp_releasedir(const char *path, struct fuse_file_info *fi) {
    debug_info("[FUSE-XPN] Init releasedir(" << path << ", " << fi->fh << ")");
    struct xmp_dirp *d = get_dirp(fi);
    (void)path;
    xpn_closedir(d->dp);
    free(d);
    debug_info("[FUSE-XPN] End releasedir(" << path << ", " << fi->fh << ")");
    return 0;
}

// TODO
// static int xmp_mknod(const char *path, mode_t mode, dev_t rdev) {
//     int res;

//     if (S_ISFIFO(mode))
//         res = mkfifo(path, mode);
//     else
//         res = mknod(path, mode, rdev);
//     if (res == -1) return -errno;

//     return 0;
// }

static int xmp_mkdir(const char *path, mode_t mode) {
    int res;
    debug_info("[FUSE-XPN] Init mkdir(" << path << ", " << mode << ")");

    std::string p(path);
    p.insert(0, "xpn");
    res = xpn_mkdir(p.c_str(), mode);
    debug_info("[FUSE-XPN] End mkdir(" << path << ", " << mode << ")");
    if (res == -1) return -errno;

    return 0;
}

static int xmp_unlink(const char *path) {
    int res;
    debug_info("[FUSE-XPN] Init unlink(" << path << ")");

    std::string p(path);
    p.insert(0, "xpn");
    res = xpn_unlink(p.c_str());
    debug_info("[FUSE-XPN] End unlink(" << path << ")");
    if (res == -1) return -errno;

    return 0;
}

static int xmp_rmdir(const char *path) {
    int res;
    debug_info("[FUSE-XPN] Init unlink(" << path << ")");

    std::string p(path);
    p.insert(0, "xpn");
    res = xpn_rmdir(p.c_str());
    debug_info("[FUSE-XPN] End unlink(" << path << ")");
    if (res == -1) return -errno;

    return 0;
}

// TODO
// static int xmp_symlink(const char *from, const char *to) {
//     int res;

//     res = xpn_symlink(from, to);
//     if (res == -1) return -errno;

//     return 0;
// }

static int xmp_rename(const char *from, const char *to, [[maybe_unused]] unsigned int flags) {
    int res;
    debug_info("[FUSE-XPN] Init rename(" << from << ", " << to << ")");
    // TODO: fix the correct use of flags
    std::string f(from);
    f.insert(0, "xpn");
    std::string t(to);
    t.insert(0, "xpn");
    res = xpn_rename(f.c_str(), t.c_str());
    debug_info("[FUSE-XPN] End rename(" << from << ", " << to << ")");
    if (res == -1) return -errno;

    return 0;
}

// TODO
// static int xmp_link(const char *from, const char *to) {
//     int res;

//     res = xpn_link(from, to);
//     if (res == -1) return -errno;

//     return 0;
// }

static int xmp_chmod(const char *path, mode_t mode, [[maybe_unused]] fuse_file_info *fi) {
    int res;
    debug_info("[FUSE-XPN] Init chmod(" << path << ", " << mode << ")");

    std::string p(path);
    p.insert(0, "xpn");
    res = xpn_chmod(p.c_str(), mode);
    debug_info("[FUSE-XPN] End chmod(" << path << ", " << mode << ")");
    if (res == -1) return -errno;

    return 0;
}

// TODO
// static int xmp_chown(const char *path, uid_t uid, gid_t gid) {
//     int res;

//     res = xpn_lchown(path, uid, gid);
//     if (res == -1) return -errno;

//     return 0;
// }

static int xmp_truncate(const char *path, off_t size, [[maybe_unused]] fuse_file_info *fi) {
    int res;
    debug_info("[FUSE-XPN] Init truncate(" << path << ", " << size << ")");

    std::string p(path);
    p.insert(0, "xpn");
    res = xpn_truncate(p.c_str(), size);
    debug_info("[FUSE-XPN] End truncate(" << path << ", " << size << ")");
    if (res == -1) return -errno;

    return 0;
}

#ifdef HAVE_UTIMENSAT
static int xmp_utimens(const char *path, const struct timespec ts[2]) {
    int res;

    /* don't use utime/utimes since they follow symlinks */
    res = utimensat(0, path, ts, AT_SYMLINK_NOFOLLOW);
    if (res == -1) return -errno;

    return 0;
}
#endif

static int xmp_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    int fd;
    debug_info("[FUSE-XPN] Init create(" << fi->fh << ", " << path << ", " << mode << ")");

    std::string p(path);
    p.insert(0, "xpn");
    fd = xpn_open(p.c_str(), fi->flags, mode);
    debug_info("[FUSE-XPN] End create(" << fd << ", " << path << ", " << mode << ")");
    if (fd == -1) return -errno;

    fi->fh = fd;
    return 0;
}

static int xmp_open(const char *path, struct fuse_file_info *fi) {
    int fd;
    debug_info("[FUSE-XPN] Init open(" << fi->fh << ", " << path << ")");

    std::string p(path);
    p.insert(0, "xpn");
    fd = xpn_open(p.c_str(), fi->flags);
    debug_info("[FUSE-XPN] End open(" << fd << ", " << path << ")");
    if (fd == -1) return -errno;

    fi->fh = fd;
    return 0;
}

static int xmp_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    int res;
    debug_info("[FUSE-XPN] Init read(" << fi->fh << ", " << path << ", " << size << ", " << offset << ")");

    (void)path;
    res = xpn_pread(fi->fh, buf, size, offset);
    debug_info("[FUSE-XPN] End read(" << fi->fh << ", " << path << ", " << size << ", " << offset << ")");
    if (res == -1) res = -errno;

    return res;
}

// static int xmp_read_buf(const char *path, struct fuse_bufvec **bufp, size_t size, off_t offset,
//                         struct fuse_file_info *fi) {
//     struct fuse_bufvec *src;
//     debug_info("[FUSE-XPN] Init read_buf(" << fi->fh << ", " << path << ", " << *bufp << ", " << offset << ")");

//     (void)path;

//     src = (fuse_bufvec *)malloc(sizeof(struct fuse_bufvec));
//     if (src == NULL) return -ENOMEM;

//     *src = FUSE_BUFVEC_INIT(size);

//     src->buf[0].flags = (fuse_buf_flags)(FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK);
//     src->buf[0].fd = fi->fh;
//     src->buf[0].pos = offset;

//     *bufp = src;

//     debug_info("[FUSE-XPN] End read_buf(" << fi->fh << ", " << path << ", " << *bufp << ", " << offset << ")");
//     return 0;
// }

static int xmp_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    int res;
    debug_info("[FUSE-XPN] Init write(" << fi->fh << ", " << path << ", " << size << ", " << offset << ")");

    (void)path;
    res = xpn_pwrite(fi->fh, buf, size, offset);
    if (res == -1) res = -errno;

    debug_info("[FUSE-XPN] End write(" << fi->fh << ", " << path << ", " << size << ", " << offset << ")");
    return res;
}

// static int xmp_write_buf(const char *path, struct fuse_bufvec *buf, off_t offset, struct fuse_file_info *fi) {
//     debug_info("[FUSE-XPN] Init write_buf(" << fi->fh << ", " << path << ", " << buf << ", " << offset << ")");
//     struct fuse_bufvec dst = FUSE_BUFVEC_INIT(fuse_buf_size(buf));

//     (void)path;

//     dst.buf[0].flags = (fuse_buf_flags)(FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK);
//     dst.buf[0].fd = fi->fh;
//     dst.buf[0].pos = offset;

//     debug_info("[FUSE-XPN] End write_buf(" << fi->fh << ", " << path << ", " << buf << ", " << offset << ")");
//     return fuse_buf_copy(&dst, buf, FUSE_BUF_SPLICE_NONBLOCK);
// }

static int xmp_statfs(const char *path, struct statvfs *stbuf) {
    int res;
    debug_info("[FUSE-XPN] Init statfs(" << path << ", " << stbuf << ")");

    std::string p(path);
    p.insert(0, "xpn");

    res = xpn_statvfs(p.c_str(), stbuf);
    debug_info("[FUSE-XPN] End statfs(" << path << ", " << stbuf << ")");
    if (res == -1) return -errno;

    return 0;
}

static int xmp_flush(const char *path, struct fuse_file_info *fi) {
    debug_info("[FUSE-XPN] Init flush(" << fi->fh << ", " << path << ")");
    (void)path;
    (void)fi;
    /* This is called from every close on an open file, so call the
       close on the underlying filesystem.	But since flush may be
       called multiple times for an open file, this must not really
       close the file.  This is important if used on a network
       filesystem like NFS which flush the data/metadata on close() */
    // res = xpn_fflush(fi->fh);
    // res = xpn_close(xpn_fldup(fi->fh));
    // if (res == -1) return -errno;
    debug_info("[FUSE-XPN] End flush(" << fi->fh << ", " << path << ")");

    return 0;
}

static int xmp_release(const char *path, struct fuse_file_info *fi) {
    debug_info("[FUSE-XPN] Init release(" << fi->fh << ", " << path << ")");
    (void)path;
    xpn_close(fi->fh);
    debug_info("[FUSE-XPN] End release(" << fi->fh << ", " << path << ")");

    return 0;
}

static int xmp_fsync(const char *path, int isdatasync, struct fuse_file_info *fi) {
    debug_info("[FUSE-XPN] Init fsync(" << fi->fh << ", " << path << ", " << isdatasync << ")");
    (void)path;
    (void)fi;

#ifndef HAVE_FDATASYNC
    (void)isdatasync;
#else
    if (isdatasync)
        res = fdatasync(fi->fh);
    else
#endif
    // res = xpn_fsync(fi->fh);
    // if (res == -1) return -errno;

    debug_info("[FUSE-XPN] End fsync(" << fi->fh << ", " << path << ", " << isdatasync << ")");
    return 0;
}

// static off_t xmp_lseek(const char *path, off_t off, int whence, struct fuse_file_info *fi) {
//     debug_info("[FUSE-XPN] Init lseek(" << fi->fh << ", " << path << ", " << off << ", " << whence << ")");
//     (void)path;
//     xpn_lseek(fi->fh, off, whence);
//     debug_info("[FUSE-XPN] End lseek(" << fi->fh << ", " << path << ", " << off << ", " << whence << ")");

//     return 0;
// }

#ifdef HAVE_POSIX_FALLOCATE
static int xmp_fallocate(const char *path, int mode, off_t offset, off_t length, struct fuse_file_info *fi) {
    (void)path;

    if (mode) return -EOPNOTSUPP;

    return -posix_fallocate(fi->fh, offset, length);
}
#endif

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int xmp_setxattr(const char *path, const char *name, const char *value, size_t size, int flags) {
    int res = lsetxattr(path, name, value, size, flags);
    if (res == -1) return -errno;
    return 0;
}

static int xmp_getxattr(const char *path, const char *name, char *value, size_t size) {
    int res = lgetxattr(path, name, value, size);
    if (res == -1) return -errno;
    return res;
}

static int xmp_listxattr(const char *path, char *list, size_t size) {
    int res = llistxattr(path, list, size);
    if (res == -1) return -errno;
    return res;
}

static int xmp_removexattr(const char *path, const char *name) {
    int res = lremovexattr(path, name);
    if (res == -1) return -errno;
    return 0;
}
#endif /* HAVE_SETXATTR */

// static int xmp_lock(const char *path, struct fuse_file_info *fi, int cmd, struct flock *lock) {
//     (void)path;

//     return ulockmgr_op(fi->fh, cmd, lock, &fi->lock_owner, sizeof(fi->lock_owner));
// }

// static int xmp_flock(const char *path, struct fuse_file_info *fi, int op) {
//     int res;
//     (void)path;

//     res = flock(fi->fh, op);
//     if (res == -1) return -errno;

//     return 0;
// }

static struct fuse_operations xmp_oper = {
    .getattr = xmp_getattr,
    .readlink = nullptr,
    // .getdir = nullptr,
    .mknod = nullptr,
    .mkdir = xmp_mkdir,
    .unlink = xmp_unlink,
    .rmdir = xmp_rmdir,
    .symlink = nullptr,
    .rename = xmp_rename,
    .link = nullptr,
    .chmod = xmp_chmod,
    .chown = nullptr,
    .truncate = xmp_truncate,
    // .utime = nullptr,
    .open = xmp_open,
    .read = xmp_read,
    .write = xmp_write,
    .statfs = xmp_statfs,
    .flush = xmp_flush,
    .release = xmp_release,
    .fsync = xmp_fsync,
    .setxattr = nullptr,
    .getxattr = nullptr,
    .listxattr = nullptr,
    .removexattr = nullptr,
    .opendir = xmp_opendir,
    .readdir = xmp_readdir,
    .releasedir = xmp_releasedir,
    .fsyncdir = nullptr,
    .init = xmp_init,
    .destroy = xmp_destroy,
    .access = nullptr,
    .create = xmp_create,
    .lock = nullptr,
    .utimens = nullptr,
    .bmap = nullptr,
    .ioctl = nullptr,
    .poll = nullptr,
    .write_buf = nullptr,
    .read_buf = nullptr,
    .flock = nullptr,
    .fallocate = nullptr,
    // .copy_file_range = nullptr,
    // .lseek = xmp_lseek,
};

int main(int argc, char *argv[]) {
    umask(0);
    return fuse_main(argc, argv, &xmp_oper, NULL);
}

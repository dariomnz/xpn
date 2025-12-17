
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

#include "base_cpp/debug.hpp"
#include "xpn/xpn_api.hpp"

#ifndef O_CREAT
  #define O_CREAT 0100
#endif
#include <cstdarg>
#include <sys/statfs.h>

extern "C" {

int xpn_initialized ( void )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_init] >> Begin");

  ret = XPN::xpn_api::get_instance().initialized();

  debug_info("[XPN_UNISTD] [xpn_init] >> End");

  return ret;
}

int xpn_init ( void )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_init] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().init();
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_init] >> End");

  return ret;
}

int xpn_destroy ( void )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_destroy] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().destroy();
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_destroy] >> End");

  return ret;
}

int xpn_print_partitions ( void )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_print_partitions] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().print_partitions();
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_print_partitions] >> End");

  return ret;
}

int xpn_clean_connections ( void )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_clean_connections] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().clean_connections();
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_clean_connections] >> End");

  return ret;
}

int xpn_flush ( const char * path )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_flush] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().flush_preload(path, true);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_flush] >> End");

  return ret;
}

int xpn_preload ( const char * path )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_preload] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().flush_preload(path, false);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_preload] >> End");

  return ret;
}

int xpn_checkpoint ( const char * path )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_checkpoint] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().checkpoint(path);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_checkpoint] >> End");

  return ret;
}

int xpn_mark_error_server ( int index )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_mark_error_server] >> End");

  ret = XPN::xpn_api::get_instance().mark_error_server(index);

  debug_info("[XPN_UNISTD] [xpn_mark_error_server] >> End");

  return ret;
}

int xpn_get_block_locality ( char *path, off_t offset, int *url_c, char **url_v[] )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_get_block_locality] >> End");

  ret = XPN::xpn_api::get_instance().get_block_locality(path, offset, url_c, url_v);

  debug_info("[XPN_UNISTD] [xpn_get_block_locality] >> End");

  return ret;
}

int xpn_free_block_locality ( int *url_c, char **url_v[] )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_free_block_locality] >> End");

  ret = XPN::xpn_api::get_instance().free_block_locality(url_c, url_v);

  debug_info("[XPN_UNISTD] [xpn_free_block_locality] >> End");

  return ret;
}

int xpn_creat ( const char *path, mode_t perm )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_creat] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().creat(path, perm);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_creat] >> End");

  return ret;
}

int xpn_open ( const char *path, int flags, ... )
{
  va_list ap;
  int     ret  = -1;
  mode_t  mode = 0;

  debug_info("[XPN_UNISTD] [xpn_open] >> Begin");

  // get mode
  if ((flags & O_CREAT) > 0)
  {
    va_start(ap, flags);
    mode = va_arg(ap, mode_t);
    va_end(ap);
  }

  // call simple_open
  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().open(path, flags, mode);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_open] >> End");

  // return ret
  return ret;
}

int xpn_close ( int fd )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_close] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().close(fd);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_close] >> End");

  return ret;
}

ssize_t xpn_read  ( int fd, void *buffer, size_t size )
{
  ssize_t ret = -1;

  debug_info("[XPN_UNISTD] [xpn_read] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().read(fd, buffer, size);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_read] >> End");

  return ret;
}

ssize_t xpn_pread ( int fd, void *buffer, size_t size, off_t offset )
{
  ssize_t ret = -1;

  debug_info("[XPN_UNISTD] [xpn_pread] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().pread(fd, buffer, size, offset);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_pread] >> End");

  return ret;
}

ssize_t xpn_write ( int fd, const void *buffer, size_t size )
{
  ssize_t ret = -1;

  debug_info("[XPN_UNISTD] [xpn_write] >> Begin ("<<fd<<", "<<buffer<<", "<<size<<")");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().write(fd, buffer, size);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_write] >> End ("<<fd<<", "<<buffer<<", "<<size<<") = "<<ret);

  return ret;
}

ssize_t xpn_pwrite ( int fd, const void *buffer, size_t size, off_t offset )
{
  ssize_t ret = -1;

  debug_info("[XPN_UNISTD] [xpn_pwrite] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().pwrite(fd, buffer, size, offset);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_pwrite] >> End");

  return ret;
}

off_t   xpn_lseek ( int fd, off_t offset, int flag )
{
  off_t ret = (off_t) -1;

  debug_info("[XPN_UNISTD] [xpn_lseek] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().lseek(fd, offset, flag);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_lseek] >> End");

  return ret;
}

char* xpn_getcwd ( char *path, size_t size )
{
  char * ret = NULL;

  debug_info("[XPN_UNISTD] [xpn_getcwd] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().getcwd(path, size);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_getcwd] >> End");

  return ret;
}

int xpn_chdir(char *path)
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_chdir] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().chdir(path);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_chdir] >> End");

  return ret;
}

int xpn_mkdir ( const char *path, mode_t perm )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_mkdir] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().mkdir(path, perm);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_mkdir] >> End");

  return ret;
}

int xpn_rmdir(const char *path)
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_rmdir] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().rmdir(path);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_rmdir] >> End");

  return ret;
}

DIR *xpn_opendir ( const char *path )
{
  DIR *ret = NULL;

  debug_info("[XPN_UNISTD] [xpn_opendir] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().opendir(path);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_opendir] >> End");

  return ret;
}

struct dirent* xpn_readdir ( DIR *dirp )
{
  struct dirent* ret = NULL;

  debug_info("[XPN_UNISTD] [xpn_readdir] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().readdir(dirp);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_readdir] >> End");

  return ret;
}

int xpn_closedir ( DIR *dirp )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_closedir] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().closedir(dirp);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_closedir] >> End");

  return ret;
}

void xpn_rewinddir ( DIR *dirp )
{
  debug_info("[XPN_UNISTD] [xpn_rewinddir] >> Begin");

  XPN_API_LOCK();
  XPN::xpn_api::get_instance().rewinddir(dirp);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_rewinddir] >> End");
}

int xpn_unlink ( const char *path )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_unlink] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().unlink(path);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_unlink] >> End");

  return ret;
}

int xpn_rename ( const char *path, const char *newpath )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_rename] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().rename(path, newpath);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_rename] >> End");

  return ret;
}

int xpn_transfer_xpn2fs ( const char *path, const char *newpath )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_transfer_xpn2fs] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().transfer_xpn2fs(path, newpath);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_transfer_xpn2fs] >> End");

  return ret;
}

int xpn_transfer_fs2xpn ( const char *path, const char *newpath )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_transfer_fs2xpn] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().transfer_fs2xpn(path, newpath);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_transfer_fs2xpn] >> End");

  return ret;
}

int xpn_truncate ( const char *path,  off_t length )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_truncate] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().truncate(path, length);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_truncate] >> End");

  return ret;
}

int xpn_ftruncate ( int fd,  off_t length )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_ftruncate] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().ftruncate(fd, length);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_ftruncate] >> End");

  return ret;
}

int xpn_stat (const char *path, struct stat *sb )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_stat] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().stat(path, sb);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_stat] >> End");

  return ret;
}

int xpn_fstat ( int fd, struct stat *sb )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_fstat] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().fstat(fd, sb);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_fstat] >> End");

  return ret;
}

int xpn_statvfs ( const char *path, struct statvfs *buf )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_fstat] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().statvfs(path, buf);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_fstat] >> End");

  return ret;
}

int xpn_fstatvfs ( int fd, struct statvfs *buf )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_fstat] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().fstatvfs(fd, buf);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_fstat] >> End");

  return ret;
}


int xpn_statfs ( const char *path, struct statfs *buf )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_fstat] >> Begin");

  XPN_API_LOCK();
  // TODO: do a real statfs
  struct statvfs aux_buf;
  ret = XPN::xpn_api::get_instance().statvfs(path, &aux_buf);

  // TODO: f_type
  // buf->f_type = aux_buf.f_type;
  buf->f_bsize = aux_buf.f_bsize;
  buf->f_blocks = aux_buf.f_blocks;
  buf->f_bfree = aux_buf.f_bfree;
  buf->f_bavail = aux_buf.f_bavail;
  buf->f_files = aux_buf.f_files;
  buf->f_ffree = aux_buf.f_ffree;
  memcpy(&buf->f_fsid, &aux_buf.f_fsid, sizeof(aux_buf.f_fsid));
  buf->f_namelen = aux_buf.f_namemax;
  buf->f_frsize = aux_buf.f_frsize;
  buf->f_flags = aux_buf.f_flag;
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_fstat] >> End");

  return ret;
}

int xpn_fstatfs ( int fd, struct statfs *buf )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_fstat] >> Begin");

  XPN_API_LOCK();
  // TODO: do a real statfs
  struct statvfs aux_buf;
  ret = XPN::xpn_api::get_instance().fstatvfs(fd, &aux_buf);

  // TODO: f_type
  // buf->f_type = aux_buf.f_type;
  buf->f_bsize = aux_buf.f_bsize;
  buf->f_blocks = aux_buf.f_blocks;
  buf->f_bfree = aux_buf.f_bfree;
  buf->f_bavail = aux_buf.f_bavail;
  buf->f_files = aux_buf.f_files;
  buf->f_ffree = aux_buf.f_ffree;
  memcpy(&buf->f_fsid, &aux_buf.f_fsid, sizeof(aux_buf.f_fsid));
  buf->f_namelen = aux_buf.f_namemax;
  buf->f_frsize = aux_buf.f_frsize;
  buf->f_flags = aux_buf.f_flag;
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_fstat] >> End");

  return ret;
}

int xpn_chown ( const char *path,  uid_t owner,  gid_t group )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_chown] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().chown(path, owner, group);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_chown] >> End");

  return ret;
}

int xpn_fchown (int  fd,  uid_t owner,  gid_t group )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_fchown] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().fchown(fd, owner, group);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_fchown] >> End");

  return ret;
}

int xpn_chmod ( const char *path,  mode_t mode )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_chmod] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().chmod(path, mode);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_chmod] >> End");

  return ret;
}

int xpn_fchmod ( int fd,  mode_t mode )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_fchmod] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().fchmod(fd, mode);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_fchmod] >> End");

  return ret;
}

int xpn_dup ( int fd )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_dup] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().dup(fd);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_dup] >> End");

  return ret;
}

int xpn_dup2 ( int fd, int fd2 )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_dup2] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().dup2(fd, fd2);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_dup2] >> End");

  return ret;
}

int xpn_start_expand ( const char *host_list, int rank )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_start_expand] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().start_malleability(host_list, rank, XPN::xpn_api::malleability_type::EXPAND);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_start_expand] >> End");

  return ret;
}

int xpn_end_expand ( const char *host_list, int rank )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_end_expand] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().end_malleability(host_list, rank, XPN::xpn_api::malleability_type::EXPAND);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_end_expand] >> End");

  return ret;
}

int xpn_start_shrink ( const char *host_list, int rank )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_start_shrink] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().start_malleability(host_list, rank, XPN::xpn_api::malleability_type::SHRINK);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_start_shrink] >> End");

  return ret;
}

int xpn_end_shrink ( const char *host_list, int rank )
{
  int ret = -1;

  debug_info("[XPN_UNISTD] [xpn_end_shrink] >> Begin");

  XPN_API_LOCK();
  ret = XPN::xpn_api::get_instance().end_malleability(host_list, rank, XPN::xpn_api::malleability_type::SHRINK);
  XPN_API_UNLOCK();

  debug_info("[XPN_UNISTD] [xpn_end_shrink] >> End");

  return ret;
}

} // extern "C"

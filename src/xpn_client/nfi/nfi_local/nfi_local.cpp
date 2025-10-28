
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

#include "nfi_local.hpp"
#include "base_cpp/xpn_env.hpp"
#include "xpn/xpn_file.hpp"
#include "xpn_server/xpn_server_ops.hpp"
#include <fcntl.h>
#include <sys/stat.h>

namespace XPN
{

// File API
int nfi_local::nfi_open (const std::string &path, int flags, mode_t mode, xpn_fh &fho)
{
  int ret;
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_open] >> Begin");

  fho.path = m_path + "/" + path;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_open] nfi_local_open("<<fho.path<<", "<<format_open_flags(flags)<<", "<<format_open_mode(mode)<<")");

  ret = PROXY(open)(fho.path.c_str(), flags, mode);
  if (ret < 0)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_open] ERROR: real_posix_open2 fails to open '"<<fho.path.c_str()<<"'");
    return -1;
  }

  if (xpn_env::get_instance().xpn_session_file == 0){
    PROXY(close)(ret);
  }

  fho.fd = ret;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_open] nfi_local_open("<<fho.path.c_str()<<")="<<ret);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_open] << End");

  return 0;
}

int nfi_local::nfi_create (const std::string &path, mode_t mode, xpn_fh &fho)
{
  //NOTE: actualy creat is not in use, it use like POSIX open(path, O_WRONLY|O_CREAT|O_TRUNC, mode);
  return nfi_local::nfi_open(path, O_WRONLY|O_CREAT|O_TRUNC, mode, fho);
}

int nfi_local::nfi_close (const xpn_fh &fh)
{
  if (xpn_env::get_instance().xpn_session_file == 1){
    int ret;

    debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_close] >> Begin");
    
    debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_close] nfi_local_close("<<fh.fd<<")");

    ret = PROXY(close)(fh.fd);

    debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_close] nfi_local_close("<<fh.fd<<")="<<ret);
    debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_close] >> End");

    return ret;
  }else{
    // Without sesion close do nothing
    return 0;
  }
}

int64_t nfi_local::nfi_read (const xpn_fh &fh, char *buffer, int64_t offset, uint64_t size)
{
  int64_t ret;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read] >> Begin");

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read] nfi_local_read("<<fh.path<<", "<<offset<<", "<<size<<")");
  
  // Check arguments...
  if (size == 0){
    return 0;
  }
  
  int fd;
  if (xpn_env::get_instance().xpn_session_file == 1){
    fd = fh.fd;
  }else{
    fd = PROXY(open)(fh.path.c_str(), O_RDONLY);
  }

  if (fd < 0)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read] ERROR: real_posix_read open fail '"<<fh.path<<"'");
    return -1;
  }
  ret = PROXY(lseek)(fd, offset, SEEK_SET);
  if (ret < 0)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read] ERROR: real_posix_read lseek fail from '"<<fh.path<<"'");
    ret = -1;
    goto cleanup_nfi_local_read;
  }
  ret = filesystem::read(fd, buffer, size);
  if (ret < 0)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read] ERROR: real_posix_read reads fail from '"<<fh.path<<"'");
    ret = -1;
    goto cleanup_nfi_local_read;
  }
cleanup_nfi_local_read:
  if (xpn_env::get_instance().xpn_session_file == 0){
    PROXY(close)(fd);
  }
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read] nfi_local_read("<<fh.path<<", "<<offset<<", "<<size<<")="<<ret);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read] >> End");

  return ret;
}

int64_t nfi_local::nfi_write (const xpn_fh &fh, const char *buffer, int64_t offset, uint64_t size)
{
  int64_t ret;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_write] >> Begin");

  // Check arguments...
  if (size == 0){
    return 0;
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_write] nfi_local_write("<<fh.path<<", "<<offset<<", "<<size<<")");

  int fd;
  if (xpn_env::get_instance().xpn_session_file == 1){
    fd = fh.fd;
  }else{
    fd = PROXY(open)(fh.path.c_str(), O_WRONLY);
  }

  if (fd < 0)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_write] ERROR: real_posix_write open fail '"<<fh.path<<"'");
    return -1;
  }
  ret = PROXY(lseek)(fd, offset, SEEK_SET);
  if (ret < 0)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_write] ERROR: real_posix_write lseek fail from '"<<fh.path<<"'");
    ret = -1;
    goto cleanup_nfi_local_write;
  }
  ret = filesystem::write(fd, buffer, size);
  if (ret < 0)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_write] ERROR: real_posix_write write fail from '"<<fh.path<<"'");
    ret = -1;
    goto cleanup_nfi_local_write;
  }

cleanup_nfi_local_write:
  if (xpn_env::get_instance().xpn_session_file == 1){
    PROXY(fsync)(fd);
  }else{
    PROXY(close)(fd);
  }
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_write] nfi_local_write("<<fh.path<<", "<<offset<<", "<<size<<")="<<ret);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_write] >> End");

  return ret;
}

int nfi_local::nfi_remove (const std::string &path, [[maybe_unused]] bool is_async)
{
  int ret;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_remove] >> Begin");

  std::string srv_path = m_path + "/" + path;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_remove] nfi_local_remove("<<srv_path<<")");
  ret = PROXY(unlink)(srv_path.c_str());
  if (ret < 0)
  {
    debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_remove] ERROR: real_posix_unlink fails to unlink '"<<srv_path<<"'");
    return -1;
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_remove] nfi_local_remove("<<srv_path<<")="<<ret);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_remove] >> End");

  return ret;
}

int nfi_local::nfi_rename (const std::string &path, const std::string &new_path)
{
  int  ret;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_rename] >> Begin");

  std::string srv_path = m_path + "/" + path;
  std::string new_srv_path = m_path + "/" + new_path;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_rename] nfi_local_rename("<<srv_path<<", "<<new_srv_path<<")");

  ret = PROXY(rename)(srv_path.c_str(), new_srv_path.c_str());
  if (ret < 0)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_rename] ERROR: real_posix_rename fails to rename '"<<srv_path<<"'");
    return -1;
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_rename] nfi_local_rename("<<srv_path<<", "<<new_srv_path<<")="<<ret);
  debug_info("[NFI_LOCAL] [nfi_local_rename] >> End\n");

  return ret;
}

int nfi_local::nfi_getattr (const std::string &path, struct ::stat &st)
{
  int  ret;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_getattr] >> Begin");

  std::string srv_path = m_path + "/" + path;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_getattr] nfi_local_getattr("<<srv_path<<")");
  #ifdef _STAT_VER
  ret = PROXY(__xstat)(_STAT_VER, srv_path.c_str(), &st);
  #else
  ret = PROXY(stat)(srv_path.c_str(), &st);
  #endif
  if (ret < 0)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_getattr] ERROR: real_posix_stat fails to stat '"<<srv_path<<"'");
    return ret;
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_getattr] nfi_local_getattr("<<srv_path<<")="<<ret);

  debug_info("[NFI_LOCAL] [nfi_local_getattr] >> End\n");

  return ret;
}

int nfi_local::nfi_setattr ([[maybe_unused]] const std::string &path, [[maybe_unused]] struct ::stat &st)
{
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_setattr] >> Begin");

  // TODO: setattr

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_setattr] >> End");

  return 0;
}

// Directories API
int nfi_local::nfi_mkdir(const std::string &path, mode_t mode)
{
  int    ret;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_mkdir] >> Begin");

  std::string srv_path = m_path + "/" + path;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_mkdir] nfi_local_mkdir("<<srv_path<<")");
  ret = PROXY(mkdir)(srv_path.c_str(), mode);
  if ((ret < 0) && (errno != EEXIST))
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_mkdir] ERROR: real_posix_mkdir fails to mkdir '"<<srv_path<<"'");
    return -1;
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_mkdir] nfi_local_mkdir("<<srv_path<<")="<<ret);

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_mkdir] >> End");

  return ret;
}

int nfi_local::nfi_opendir(const std::string &path, xpn_fh &fho)
{
  DIR* s;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_opendir] >> Begin");

  fho.path = m_path + "/" + path;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_opendir] nfi_local_opendir("<<fho.path<<")");

  s = PROXY(opendir)(fho.path.c_str());
  if (s == NULL)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_opendir] ERROR: real_posix_opendir fails to opendir '"<<fho.path<<"'");
    return -1;
  }

  if (xpn_env::get_instance().xpn_session_dir == 0){
    fho.telldir = PROXY(telldir)(s);
    PROXY(closedir)(s);
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_opendir] nfi_local_opendir("<<fho.path<<")="<<s);

  fho.dir = reinterpret_cast<int64_t>(s);

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_opendir] >> End");

  return 0;
}

int nfi_local::nfi_readdir(xpn_fh &fhd, struct ::dirent &entry)
{
  DIR* s;
  ::dirent *ent;
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_readdir] >> Begin");

  // cleaning entry values...
  memset(&entry, 0, sizeof(dirent));

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_readdir] nfi_local_readdir("<<fhd.path<<")");
  
  if (xpn_env::get_instance().xpn_session_dir == 0){
    s = PROXY(opendir)(fhd.path.c_str());  
    if (s == NULL) {
      debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_readdir] ERROR: real_posix_opendir fails to opendir '"<<fhd.path<<"'");
      return -1;
    }
    
    PROXY(seekdir)(s, fhd.telldir);
  }else{
    s = reinterpret_cast<::DIR*>(fhd.dir);
  }
  // Reset errno
  errno = 0;
  ent = PROXY(readdir)(s);
  if (ent == NULL)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_readdir] ERROR: real_posix_readdir fails to readdir '"<<fhd.path<<"'");
    return -1;
  }
  if (xpn_env::get_instance().xpn_session_dir == 0){
    fhd.telldir = PROXY(telldir)(s);
    PROXY(closedir)(s);
  }

  memcpy(&entry, ent, sizeof(::dirent));

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_readdir] nfi_local_readdir("<<fhd.path<<")="<<(void*)&entry);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_readdir] >> End");

  return 0;
}

int nfi_local::nfi_closedir (const xpn_fh &fhd)
{
  if (xpn_env::get_instance().xpn_session_dir == 1){
    int ret;

    debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_closedir] >> Begin");

    debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_closedir] nfi_local_closedir("<<fhd.dir<<")");

    ret = PROXY(closedir)(reinterpret_cast<::DIR*>(fhd.dir));

    debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_closedir] nfi_local_closedir("<<fhd.dir<<")="<<ret);
    debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_closedir] >> End");

    return ret;
  }else{
    // Without sesion close do nothing
    return 0;
  }
}

int nfi_local::nfi_rmdir(const std::string &path, [[maybe_unused]] bool is_async)
{
  int ret;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_rmdir] >> Begin");

  std::string srv_path = m_path + "/" + path;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_rmdir] nfi_local_rmdir("<<srv_path<<")");

  ret = PROXY(rmdir)(srv_path.c_str());
  if (ret < 0)
  {
    debug_error("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_rmdir] ERROR: real_posix_rmdir fails to rm '"<<srv_path<<"'");
    return -1;
  }

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_rmdir] nfi_local_rmdir("<<srv_path<<")="<<ret);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_rmdir] >> End");

  return 0;
}

int nfi_local::nfi_statvfs(const std::string &path, struct ::statvfs &inf)
{
  int ret;
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_statfs] >> Begin");

  std::string srv_path = m_path + "/" + path;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_statfs] nfi_local_statvfs("<<srv_path<<")");

  ret = PROXY(statvfs)(srv_path.c_str(), &inf);

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_statfs] nfi_local_statfs("<<srv_path<<")="<<ret);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_statfs] >> End");

  return ret;
}

int nfi_local::nfi_read_mdata (const std::string &path, xpn_metadata &mdata)
{
  int ret, fd;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read_mdata] >> Begin");

  std::string srv_path = m_path + "/" + path;

  mdata.m_data = {};

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read_mdata] nfi_local_read_mdata("<<srv_path<<")");

  fd = PROXY(open)(srv_path.c_str(), O_RDWR);
  if (fd < 0){
    if (errno == EISDIR){
      // if is directory there are no metadata to read so return 0
      debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read_mdata] << End");
      return 0;
    }
    debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read_mdata] << End");
    return -1;
  }

  ret = filesystem::read(fd, &mdata.m_data, sizeof(mdata.m_data));

  if (!mdata.m_data.is_valid()){
    mdata.m_data = {};
  }

  PROXY(close)(fd); //TODO: think if necesary check error in close

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read_mdata] nfi_local_read_mdata("<<srv_path<<")="<<ret);
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_read_mdata] << End");
  return ret;
}

int nfi_local::nfi_write_mdata (const std::string &path, const xpn_metadata::data &mdata, bool only_file_size)
{
  int ret, fd;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_write_mdata] >> Begin");

  std::string srv_path = m_path + "/" + path;

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_local_write_mdata] nfi_local_write_mdata("<<srv_path<<")");

  // is necessary to do it in xpn_server in order to ensure atomic operation
  if(only_file_size){
    if (m_protocol == "file") {
      static std::mutex m;
      uint64_t actual_file_size = 0;
      std::unique_lock lock(m);
      fd = PROXY(open)(srv_path.c_str(), O_RDWR);
      if (fd < 0){
        if (errno == EISDIR){
          // if is directory there are no metadata to write so return 0
          ret = 0;
          debug_info("[SERV_ID="<<m_server<<"] [XPN_SERVER_OPS] [nfi_local_write_mdata] nfi_local_write_mdata("<<srv_path<<")="<<ret);
          return ret;
        }
        ret = fd;
        debug_info("[SERV_ID="<<m_server<<"] [XPN_SERVER_OPS] [nfi_local_write_mdata] nfi_local_write_mdata("<<srv_path<<")="<<ret);
        return ret;
      }
      ret = filesystem::pread(fd, &actual_file_size, sizeof(actual_file_size), offsetof(xpn_metadata::data, file_size));

      if (ret > 0 && actual_file_size < mdata.file_size){
        ret = filesystem::pwrite(fd, &mdata.file_size, sizeof(mdata.file_size), offsetof(xpn_metadata::data, file_size));
      }
      
      PROXY(close)(fd); //TODO: think if necesary check error in close
    }else{
      struct st_xpn_server_write_mdata_file_size msg;
      uint64_t length = srv_path.copy(msg.path.path, srv_path.size());
      msg.path.path[length] = '\0';
      msg.path.size = length + 1;
      msg.size = mdata.file_size;
      ret = nfi_write_operation(xpn_server_ops::WRITE_MDATA_FILE_SIZE, msg, false);
    }
  }else{
    fd = PROXY(open)(srv_path.c_str(), O_WRONLY | O_CREAT, S_IRWXU);
    if (fd < 0){
      if (errno == EISDIR){
        // if is directory there are no metadata to write so return 0
        ret = 0;
        debug_info("[SERV_ID="<<m_server<<"] [XPN_SERVER_OPS] [nfi_local_write_mdata] nfi_local_write_mdata("<<srv_path<<")="<<ret);
        return ret;
      }
      ret = fd;
      debug_info("[SERV_ID="<<m_server<<"] [XPN_SERVER_OPS] [nfi_local_write_mdata] nfi_local_write_mdata("<<srv_path<<")="<<ret<<" "<<strerror(errno));
      return ret;
    }

    ret = filesystem::write(fd, &mdata, sizeof(mdata));

    PROXY(close)(fd); //TODO: think if necesary check error in close
  }

  debug_info("[Server="<<m_server<<"] [NFI_LOCAL] [nfi_local_write_mdata] nfi_local_write_mdata("<<srv_path<<")="<<ret);
  debug_info("[Server="<<m_server<<"] [NFI_LOCAL] [nfi_local_write_mdata] << End");
  return ret;
}

// TODO: duplicated from nfi_xpn_server because it needs to connect to the server to do the operation
int nfi_local::nfi_flush (const char *path)
{
  int ret;
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_flush] >> Begin");
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_flush] nfi_flush("<<m_path<<", "<<path<<")");

  st_xpn_server_flush_preload_ckpt msg;
    
  std::size_t length = m_path.copy(msg.paths.path1(), m_path.size());
  msg.paths.path1()[length] = '\0';
  msg.paths.size1 = length + 1;
  
  length = strlen(path);
  strcpy(msg.paths.path2(), path);
  msg.paths.path2()[length] = '\0';
  msg.paths.size2 = length + 1;

  ret = nfi_write_operation(xpn_server_ops::FLUSH, msg);

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_flush] nfi_flush("<<m_path<<", "<<path<<")="<<ret);
  debug_info("[NFI_LOCAL] [nfi_flush] >> End");
  return ret;
}

int nfi_local::nfi_preload (const char *path)
{
  int ret;
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_preload] >> Begin");
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_preload] nfi_preload("<<m_path<<", "<<path<<")");

  st_xpn_server_flush_preload_ckpt msg;
  
  size_t length = strlen(path);
  strcpy(msg.paths.path1(), path);
  msg.paths.path1()[length] = '\0';
  msg.paths.size1 = length + 1;
  
  length = m_path.copy(msg.paths.path2(), m_path.size());
  msg.paths.path2()[length] = '\0';
  msg.paths.size2 = length + 1;

  ret = nfi_write_operation(xpn_server_ops::PRELOAD, msg);

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_preload] nfi_preload("<<m_path<<", "<<path<<")="<<ret);
  debug_info("[NFI_LOCAL] [nfi_preload] >> End");
  return ret;
}

int nfi_local::nfi_checkpoint (const char *path)
{
  int ret;
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_checkpoint] >> Begin");
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_checkpoint] nfi_checkpoint("<<m_path<<", "<<path<<")");

  st_xpn_server_flush_preload_ckpt msg;
    
  std::size_t length = m_path.copy(msg.paths.path1(), m_path.size());
  msg.paths.path1()[length] = '\0';
  msg.paths.size1 = length + 1;
  
  length = strlen(path);
  strcpy(msg.paths.path2(), path);
  msg.paths.path2()[length] = '\0';
  msg.paths.size2 = length + 1;

  ret = nfi_write_operation(xpn_server_ops::CHECKPOINT, msg);

  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_checkpoint] nfi_checkpoint("<<m_path<<", "<<path<<")="<<ret);
  debug_info("[NFI_LOCAL] [nfi_checkpoint] >> End");
  return ret;
}

int nfi_local::nfi_response() {
  
  int ret;
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_response] >> Begin");
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_response] nfi_response()");

  st_xpn_server_status req;
    
  ret = nfi_read_response(req);

  if (ret > 0) {
    if (req.ret < 0) {
      errno = req.server_errno;
      ret = req.ret;
    }
  }
  debug_info("[SERV_ID="<<m_server<<"] [NFI_LOCAL] [nfi_response] nfi_response()="<<ret);
  debug_info("[NFI_LOCAL] [nfi_response] >> End");
  return ret;
}

} // namespace XPN

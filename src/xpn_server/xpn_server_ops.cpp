
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
#include "xpn_server.hpp"
#include "base_cpp/timer.hpp"
#include "lz4.h"
#include <stddef.h>
#include <fcntl.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <cstdlib>
#include <thread>

#ifdef ENABLE_MQTT_SERVER
#include "mqtt_server/mqtt_server_ops.hpp"
#endif
#include "sck_server/sck_server_comm.hpp"

namespace XPN
{

#define HANDLE_OPERATION(op_struct, op_function) \
  const op_struct* msg_struct = reinterpret_cast<const op_struct*>(msg.msg_buffer); \
  (op_function)(comm, (*msg_struct), (rank), (tag));

//Read the operation to realize
void xpn_server::do_operation ( xpn_server_comm &comm, const xpn_server_msg& msg, int rank, int tag, timer timer )
{
  debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER_OPS] [xpn_server_do_operation] >> Begin");
  xpn_server_ops type_op = static_cast<xpn_server_ops>(msg.op);
  debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER_OPS] [xpn_server_do_operation] OP '"<<xpn_server_ops_name(type_op)<<"'; OP_ID "<< static_cast<int>(type_op));
  switch (type_op)
  {
    //File API
    case xpn_server_ops::OPEN_FILE:              {HANDLE_OPERATION(st_xpn_server_path_flags,             op_open);                  break;}
    case xpn_server_ops::CREAT_FILE:             {HANDLE_OPERATION(st_xpn_server_path_flags,             op_creat);                 break;}
    case xpn_server_ops::READ_FILE:              {HANDLE_OPERATION(st_xpn_server_rw,                     op_read);
                                                  std::optional<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat = std::nullopt;
                                                  if (xpn_env::get_instance().xpn_stats) { io_stat.emplace(xpn_stats::scope_stat<xpn_stats::io_stats>(m_stats.m_read_total, msg_struct->size, timer)); } 
                                                  break;}
    case xpn_server_ops::WRITE_FILE:             {HANDLE_OPERATION(st_xpn_server_rw,                     op_write);
                                                  std::optional<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
                                                  if (xpn_env::get_instance().xpn_stats) { io_stat.emplace(xpn_stats::scope_stat<xpn_stats::io_stats>(m_stats.m_write_total, msg_struct->size, timer)); } 
                                                  break;}
    case xpn_server_ops::CLOSE_FILE:             {HANDLE_OPERATION(st_xpn_server_close,                  op_close);                 break;}
    case xpn_server_ops::RM_FILE:                {HANDLE_OPERATION(st_xpn_server_path,                   op_rm);                    break;}
    case xpn_server_ops::RM_FILE_ASYNC:          {HANDLE_OPERATION(st_xpn_server_path,                   op_rm_async);              break;}
    case xpn_server_ops::RENAME_FILE:            {HANDLE_OPERATION(st_xpn_server_rename,                 op_rename);                break;}
    case xpn_server_ops::GETATTR_FILE:           {HANDLE_OPERATION(st_xpn_server_path,                   op_getattr);               break;}
    case xpn_server_ops::SETATTR_FILE:           {HANDLE_OPERATION(st_xpn_server_setattr,                op_setattr);               break;}

    //Directory API
    case xpn_server_ops::MKDIR_DIR:              {HANDLE_OPERATION(st_xpn_server_path_flags,             op_mkdir);                 break;}
    case xpn_server_ops::OPENDIR_DIR:            {HANDLE_OPERATION(st_xpn_server_path_flags,             op_opendir);               break;}
    case xpn_server_ops::READDIR_DIR:            {HANDLE_OPERATION(st_xpn_server_readdir,                op_readdir);               break;}
    case xpn_server_ops::CLOSEDIR_DIR:           {HANDLE_OPERATION(st_xpn_server_close,                  op_closedir);              break;}
    case xpn_server_ops::RMDIR_DIR:              {HANDLE_OPERATION(st_xpn_server_path,                   op_rmdir);                 break;}
    case xpn_server_ops::RMDIR_DIR_ASYNC:        {HANDLE_OPERATION(st_xpn_server_path,                   op_rmdir_async);           break;}

    //Metadata API
    case xpn_server_ops::READ_MDATA:             {HANDLE_OPERATION(st_xpn_server_path,                   op_read_mdata);            break;}
    case xpn_server_ops::WRITE_MDATA:            {HANDLE_OPERATION(st_xpn_server_write_mdata,            op_write_mdata);           break;}
    case xpn_server_ops::WRITE_MDATA_FILE_SIZE:  {HANDLE_OPERATION(st_xpn_server_write_mdata_file_size,  op_write_mdata_file_size); break;}

    case xpn_server_ops::STATVFS_DIR:            {HANDLE_OPERATION(st_xpn_server_path,                   op_statvfs);               break;}

    // Flush preload
    case xpn_server_ops::FLUSH:                  {HANDLE_OPERATION(st_xpn_server_flush_preload_ckpt,     op_flush);                 break;}
    case xpn_server_ops::PRELOAD:                {HANDLE_OPERATION(st_xpn_server_flush_preload_ckpt,     op_preload);               break;}
    case xpn_server_ops::CHECKPOINT:             {HANDLE_OPERATION(st_xpn_server_flush_preload_ckpt,     op_checkpoint);            break;}
    //Connection API
    case xpn_server_ops::DISCONNECT: break;
    //Rest operation are unknown
    default: std::cerr << "Server " << serv_name << " has received an unknown operation." << std::endl;
  }

  debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER_OPS] [xpn_server_do_operation] << End");
}


// File API
void xpn_server::op_open ( xpn_server_comm &comm, const st_xpn_server_path_flags &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_open] >> Begin");

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_open] open("<<head.path.path<<", "<<format_open_flags(head.flags)<<", "<<format_open_mode(head.mode)<<")");

  // do open
  status.ret = m_filesystem->open(head.path.path, head.flags, head.mode);
  status.server_errno = errno;
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_open] open("<<head.path.path<<")="<< status.ret);
  if (status.ret < 0){
    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
  }else{
    if (head.xpn_session == 0){
      status.ret = m_filesystem->close(status.ret);
    }
    status.server_errno = errno;

    if (m_params.srv_type == server_type::MQTT){
      if (m_control_comm->m_type == server_type::SCK){
        #if defined(ENABLE_MQTT_SERVER)
        auto sck_comm = static_cast<sck_server_control_comm*>(m_control_comm.get());
        mqtt_server_ops::subscribe(static_cast<mosquitto*>(sck_comm->m_mqtt), m_params.mqtt_qos, head.path.path);
        #endif
      }
    }

    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
  }

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_open] << End");
}

void xpn_server::op_creat ( xpn_server_comm &comm, const st_xpn_server_path_flags &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_creat] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_creat] creat("<<head.path.path<<")");

  // do creat
  status.ret = m_filesystem->creat(head.path.path, head.mode);
  status.server_errno = errno;
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_creat] creat("<<head.path.path<<")="<<status.ret);
  if (status.ret < 0){
    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
  }else{
    status.ret = m_filesystem->close(status.ret);
    status.server_errno = errno;

    if (m_params.srv_type == server_type::MQTT){
      if (m_control_comm->m_type == server_type::SCK){
        #if defined(ENABLE_MQTT_SERVER)
        auto sck_comm = static_cast<sck_server_control_comm*>(m_control_comm.get());
        mqtt_server_ops::subscribe(static_cast<mosquitto*>(sck_comm->m_mqtt), m_params.mqtt_qos, head.path.path);
        #endif
      }
    }

    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
  }

  // show debug info
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_creat] << End");
}

void xpn_server::op_read ( xpn_server_comm &comm, const st_xpn_server_rw &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_rw_req req;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] read("<<head.path.path<<", "<<head.offset<<", "<<head.size<<")");

  auto start_time = std::chrono::high_resolution_clock::now();

  std::vector<char> buffer(head.size);

  //Open file
  int fd;
  if (head.xpn_session == 1){
    fd = head.fd;
  }else{
    fd = m_filesystem->open(head.path.path, O_RDONLY);
  }
  if (fd < 0)
  {
    req.size = -1;
    req.status.ret = fd;
    req.status.server_errno = errno;
    debug_error("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write] Error open "<<head.path.path<<" "<<strerror(errno));
    comm.write_data((char *)&req,sizeof(struct st_xpn_server_rw_req), rank_client_id, tag_client_id);
    goto cleanup_xpn_server_op_read;
  }

  // read data...
  {
    std::optional<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
    if (xpn_env::get_instance().xpn_stats) { io_stat.emplace(xpn_stats::scope_stat<xpn_stats::io_stats>(m_stats.m_read_disk, buffer.size())); } 
    req.size = m_filesystem->pread(fd, buffer.data(), buffer.size(), head.offset);
  }
  // if error then send as "how many bytes" -1
  if (req.size < 0 || req.status.ret == -1)
  {
    req.size = -1;
    req.status.ret = -1;
    req.status.server_errno = errno;
    comm.write_data((char *)&req,sizeof(struct st_xpn_server_rw_req), rank_client_id, tag_client_id);
    goto cleanup_xpn_server_op_read;
  }

  if (head.compressed_size == 1) {
    std::vector<char> compressed_data(LZ4_COMPRESSBOUND(req.size));
    req.compressed_size = LZ4_compress_fast(buffer.data(), compressed_data.data(), buffer.size(), compressed_data.size(), 10);
    debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] compress read "<<format_bytes(req.compressed_size)<<"/"<< format_bytes(buffer.size()));
    req.uncompressed_size = req.size;
    req.status.ret = 0;
    req.status.server_errno = errno;
    req.ellapsed_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
    comm.write_data((char *)&req, sizeof(struct st_xpn_server_rw_req), rank_client_id, tag_client_id);
    debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] op_read: send size "<< req.size);
    {
      std::optional<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
      if (xpn_env::get_instance().xpn_stats) { io_stat.emplace(xpn_stats::scope_stat<xpn_stats::io_stats>(m_stats.m_write_net, req.compressed_size)); } 
      comm.write_data(compressed_data.data(), req.compressed_size, rank_client_id, tag_client_id);
    }
  } else {
    req.compressed_size = 0;
    req.uncompressed_size = req.size;
    req.status.ret = 0;
    req.status.server_errno = errno;
    req.ellapsed_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
    comm.write_data((char *)&req, sizeof(struct st_xpn_server_rw_req), rank_client_id, tag_client_id);
    debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] op_read: send size "<< req.size);
    {
      std::optional<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
      if (xpn_env::get_instance().xpn_stats) { io_stat.emplace(xpn_stats::scope_stat<xpn_stats::io_stats>(m_stats.m_write_net, req.size)); } 
      comm.write_data(buffer.data(), req.size, rank_client_id, tag_client_id);
    }
  }
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] op_read: send data");

cleanup_xpn_server_op_read:
  if (head.xpn_session == 0){
    m_filesystem->close(fd);
  }

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] read("<<head.path.path<<", "<<head.offset<<", "<<head.size<<")="<< req.size);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] << End");
}

void xpn_server::op_write ( xpn_server_comm &comm, const st_xpn_server_rw &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_rw_req req;
  int decompressed_size;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write] write("<<head.path.path<<", "<<head.offset<<", "<<head.size<<")");

  std::vector<char> compressed_buffer(head.compressed_size);
  std::vector<char> uncompressed_buffer(head.uncompressed_size);
  
  decltype(std::chrono::high_resolution_clock::now()) start_time;

  // read data from MPI and write into the file
  {
    std::optional<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
    if (xpn_env::get_instance().xpn_stats) { io_stat.emplace(xpn_stats::scope_stat<xpn_stats::io_stats>(m_stats.m_read_net, head.compressed_size)); } 
    if (head.compressed_size > 0) {
      comm.read_data(compressed_buffer.data(), head.compressed_size, rank_client_id, tag_client_id);
    } else {
      comm.read_data(uncompressed_buffer.data(), head.uncompressed_size, rank_client_id, tag_client_id);
    }
  }

  //Open file
  int fd;
  if (head.xpn_session == 1) {
    fd = head.fd;
  }else{
    fd = m_filesystem->open(head.path.path, O_WRONLY);
  }

  if (fd < 0) {
    req.size = -1;
    req.status.ret = -1;
    debug_error("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write] Error open "<<head.path.path<<" "<<strerror(errno));
    goto cleanup_xpn_server_op_write;
  }

  start_time = std::chrono::high_resolution_clock::now();
  
  if (head.compressed_size > 0) {
    decompressed_size = LZ4_decompress_safe(compressed_buffer.data(), uncompressed_buffer.data(), compressed_buffer.size(), uncompressed_buffer.size());
    if (decompressed_size >= 0) {
      std::optional<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
      if (xpn_env::get_instance().xpn_stats) { io_stat.emplace(xpn_stats::scope_stat<xpn_stats::io_stats>(m_stats.m_write_disk, uncompressed_buffer.size())); } 
      req.size = m_filesystem->pwrite(fd, uncompressed_buffer.data(), uncompressed_buffer.size(), head.offset);
    } else {
      req.size = decompressed_size;
    }
  } else {
    std::optional<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
    if (xpn_env::get_instance().xpn_stats) { io_stat.emplace(xpn_stats::scope_stat<xpn_stats::io_stats>(m_stats.m_write_disk, uncompressed_buffer.size())); } 
    req.size = m_filesystem->pwrite(fd, uncompressed_buffer.data(), uncompressed_buffer.size(), head.offset);
  }
  // print("write "<<(head.compressed_size > 0 ? head.compressed_size:head.uncompressed_size)<<"/"<<req.size<<" ellapsed recv "<<ellapsed_recv<<" decompress "<<ellapsed_decompress<<" write "<<ellapsed_write);
  if (req.size < 0) {
    req.status.ret = -1;
    goto cleanup_xpn_server_op_write;
  }

  req.status.ret = 0;
cleanup_xpn_server_op_write:
  // write to the client the status of the write operation
  req.status.server_errno = errno;
  req.ellapsed_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_time).count();
  comm.write_data((char *)&req,sizeof(struct st_xpn_server_rw_req), rank_client_id, tag_client_id);

  if (head.xpn_session == 1){
    m_filesystem->fsync(fd);
  }else{
    m_filesystem->close(fd);
  }

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write] write("<<head.path.path<<", "<<head.offset<<", "<<head.size<<")="<< req.size);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write] << End");
}

void xpn_server::op_close ( xpn_server_comm &comm, const st_xpn_server_close &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_status status = {};

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_close] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_close] close("<<head.fd<<")");

  if (head.xpn_session) {
    status.ret = m_filesystem->close(head.fd);
    status.server_errno = errno;
  }

  if (m_params.srv_type == server_type::MQTT) {
    if (m_control_comm->m_type == server_type::SCK) {
      #if defined(ENABLE_MQTT_SERVER)
      auto sck_comm = static_cast<sck_server_control_comm*>(m_control_comm.get());
      mqtt_server_ops::unsubscribe(static_cast<mosquitto*>(sck_comm->m_mqtt), head.path.path);
      #endif
    }
  }

  comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_close] close("<<head.fd<<")="<< status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_close] << End");

}

void xpn_server::op_rm ( xpn_server_comm &comm, const st_xpn_server_path &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm] unlink("<<head.path.path<<")");

  // do rm
  status.ret = m_filesystem->unlink(head.path.path);
  status.server_errno = errno;
  comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm] unlink("<<head.path.path<<")="<< status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm] << End");
}

void xpn_server::op_rm_async ( [[maybe_unused]] xpn_server_comm &comm, const st_xpn_server_path &head, [[maybe_unused]] int rank_client_id, [[maybe_unused]] int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm_async] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm_async] unlink("<<head.path.path<<")");

  // do rm
  m_filesystem->unlink(head.path.path);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm_async] unlink("<<head.path.path<<")="<< 0);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm_async] << End");
}

void xpn_server::op_rename ( xpn_server_comm &comm, const st_xpn_server_rename &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rename] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rename] rename("<<head.paths.path1()<<", "<<head.paths.path2()<<")");

  // do rename
  status.ret = m_filesystem->rename(head.paths.path1(), head.paths.path2());
  status.server_errno = errno;
  comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rename] rename("<<head.paths.path1()<<", "<<head.paths.path2()<<")="<<status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rename] << End");
}

void xpn_server::op_getattr ( xpn_server_comm &comm, const st_xpn_server_path &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_attr_req req;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] stat("<<head.path.path<<")");

  // do getattr
  struct ::stat st = {};
  req.status = m_filesystem->stat(head.path.path, &st);
  req.attr = st_xpn_server_stat{&st};
  req.status_req.ret = req.status;
  req.status_req.server_errno = errno;

  comm.write_data((char *)&req,sizeof(struct st_xpn_server_attr_req), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] stat("<<head.path.path<<")="<< req.status);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] << End");
}

void xpn_server::op_setattr ( [[maybe_unused]] xpn_server_comm &comm, [[maybe_unused]] const st_xpn_server_setattr &head, [[maybe_unused]] int rank_client_id, [[maybe_unused]] int tag_client_id)
{
  XPN_PROFILE_FUNCTION();
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_setattr] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_setattr] SETATTR(...)");

  // do setattr
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_setattr] SETATTR operation to be implemented !!");

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_setattr] SETATTR(...)=(...)");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_setattr] << End");
}

//Directory API
void xpn_server::op_mkdir ( xpn_server_comm &comm, const st_xpn_server_path_flags &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_mkdir] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_mkdir] mkdir("<<head.path.path<<")");

  // do mkdir
  status.ret = m_filesystem->mkdir(head.path.path, head.mode);
  status.server_errno = errno;
  comm.write_data((char *)&status,sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_mkdir] mkdir("<<head.path.path<<")="<<status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_mkdir] << End");
}

void xpn_server::op_opendir ( xpn_server_comm &comm, const st_xpn_server_path_flags &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  DIR* ret;
  struct st_xpn_server_opendir_req req;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_opendir] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_opendir] opendir("<<head.path.path<<")");

  ret = m_filesystem->opendir(head.path.path);
  req.status.ret = ret == NULL ? -1 : 0;
  req.status.server_errno = errno;

  if (req.status.ret == 0){
    if (head.xpn_session == 1){
      req.dir = reinterpret_cast<int64_t>(ret);
    }else{
      req.status.ret = m_filesystem->telldir(ret);
    }
    req.status.server_errno = errno;
  }

  if (head.xpn_session == 0){
    m_filesystem->closedir(ret);
  }

  comm.write_data((char *)&req, sizeof(struct st_xpn_server_opendir_req), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_opendir] opendir("<<head.path.path<<")=%p"<<ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_opendir] << End");
}

void xpn_server::op_readdir ( xpn_server_comm &comm, const st_xpn_server_readdir &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct dirent * ret = NULL;
  struct st_xpn_server_readdir_req ret_entry;
  DIR* s = NULL;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_readdir] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_readdir] readdir("<<head.path.path<<")");

  if (head.xpn_session == 1){
    // Reset errno
    errno = 0;
    ret = m_filesystem->readdir(reinterpret_cast<::DIR*>(head.dir));
  }else{
    s = m_filesystem->opendir(head.path.path);
    ret_entry.status.ret = s == NULL ? -1 : 0;
    ret_entry.status.server_errno = errno;
    if (s == NULL) {
      ret = NULL;
    }else{
      m_filesystem->seekdir(s, head.telldir);
      
      // Reset errno
      errno = 0;
      ret = m_filesystem->readdir(s);
    }
  }
  if (ret != NULL) {
    ret_entry.end = 1;
    ret_entry.ret = st_xpn_server_dirent{ret};
  } else {
    ret_entry.end = 0;
  }

  ret_entry.status.ret = ret == NULL ? -1 : 0;

  if (head.xpn_session == 0){
    ret_entry.telldir = m_filesystem->telldir(s);

    ret_entry.status.ret = m_filesystem->closedir(s);
  }
  ret_entry.status.server_errno = errno;

  comm.write_data((char *)&ret_entry, sizeof(struct st_xpn_server_readdir_req), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_readdir] readdir("<<(void*)s<<")="<< (void*)ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_readdir] << End");
}

void xpn_server::op_closedir ( xpn_server_comm &comm, const st_xpn_server_close &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_closedir] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_closedir] closedir("<<(void*)head.dir<<")");

  // do rm
  status.ret = m_filesystem->closedir(reinterpret_cast<::DIR*>(head.dir));
  status.server_errno = errno;
  comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_closedir] closedir("<<(void*)head.dir<<")="<<status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_closedir] << End");

}

void xpn_server::op_rmdir ( xpn_server_comm &comm, const st_xpn_server_path &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir] rmdir("<<head.path.path<<")");

  // do rmdir
  status.ret = m_filesystem->rmdir(head.path.path);
  status.server_errno = errno;
  comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir] rmdir("<<head.path.path<<")="<< status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir] << End");
}

void xpn_server::op_rmdir_async ( [[maybe_unused]] xpn_server_comm &comm, const st_xpn_server_path &head, [[maybe_unused]] int rank_client_id, [[maybe_unused]] int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir_async] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir_async] rmdir("<<head.path.path<<")");

  // do rmdir
  m_filesystem->rmdir(head.path.path);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir_async] rmdir("<<head.path.path<<")="<<0);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir_async] << End");
}

void xpn_server::op_read_mdata   ( xpn_server_comm &comm, const st_xpn_server_path &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  int ret, fd;
  struct st_xpn_server_read_mdata_req req = {};

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read_mdata] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read_mdata] read_mdata("<<head.path.path<<")");

  file_map_md_fq_item& item = get_mdata_queue(head.path.path);
  std::unique_lock lock(item.m_writing_mutex);

  fd = m_filesystem->open(head.path.path, O_RDWR);
  if (fd < 0){
    if (errno == EISDIR){
      // if is directory there are no metadata to read so return 0
      ret = 0;
      req.mdata = {};
      goto cleanup_xpn_server_op_read_mdata;
    }
    ret = fd;
    debug_error("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read_mdata] Error open "<<head.path.path<<" "<<strerror(errno));
    goto cleanup_xpn_server_op_read_mdata;
  }

  ret = m_filesystem->read(fd, &req.mdata, sizeof(req.mdata));

  if (!req.mdata.is_valid()){
    req.mdata = {};
  }

  m_filesystem->close(fd); //TODO: think if necesary check error in close

  cleanup_xpn_server_op_read_mdata:
  lock.unlock();
  release_mdata_queue(head.path.path, item);
  req.status.ret = ret;
  req.status.server_errno = errno;

  comm.write_data((char *)&req,sizeof(struct st_xpn_server_read_mdata_req), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read_mdata] read_mdata("<<head.path.path<<")="<< req.status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read_mdata] << End");
}

void xpn_server::op_write_mdata ( xpn_server_comm &comm, const st_xpn_server_write_mdata &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  int ret, fd;
  struct st_xpn_server_status req;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata] write_mdata("<<head.path.path<<")");

  file_map_md_fq_item& item = get_mdata_queue(head.path.path);
  std::unique_lock lock(item.m_writing_mutex);
  // Mode like a fopen call
  fd = m_filesystem->open(head.path.path, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
  if (fd < 0){
    if (errno == EISDIR){
      // if is directory there are no metadata to write so return 0
      ret = 0;
      goto cleanup_xpn_server_op_write_mdata;
    }
    ret = fd;
    debug_error("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata] Error open "<<head.path.path<<" "<<strerror(errno));
    goto cleanup_xpn_server_op_write_mdata;
  }
  ret = m_filesystem->write(fd, &head.mdata, sizeof(head.mdata));

  m_filesystem->close(fd); //TODO: think if necesary check error in close
cleanup_xpn_server_op_write_mdata:
  lock.unlock();
  release_mdata_queue(head.path.path, item);
  req.ret = ret;
  req.server_errno = errno;
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata] ret "<<req.ret<<" server_errno "<<req.server_errno);

  comm.write_data((char *)&req,sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata] write_mdata("<<head.path.path<<")="<<req.ret<<" "<<strerror(req.server_errno));
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata] << End");

}

// pthread_mutex_t op_write_mdata_file_size_mutex = PTHREAD_MUTEX_INITIALIZER;

// void xpn_server::op_write_mdata_file_size ( xpn_server_comm &comm, const st_xpn_server_write_mdata_file_size &head, int rank_client_id, int tag_client_id )
// {
//   XPN_PROFILE_FUNCTION_ARGS("with mutex");
//   int ret, fd;
//   uint64_t actual_file_size = 0;
//   struct st_xpn_server_status req;

//   debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] >> Begin");
//   debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] write_mdata_file_size("<<head.path<<", "<<head.size<<")");
  
//   debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] mutex lock");
//   pthread_mutex_lock(&op_write_mdata_file_size_mutex);

//   fd = m_filesystem->open(head.path.path, O_RDWR);
//   if (fd < 0){
//     if (errno == EISDIR){
//       // if is directory there are no metadata to write so return 0
//       ret = 0;
//       goto cleanup_xpn_server_op_write_mdata_file_size;
//     }
//     ret = fd;
//     goto cleanup_xpn_server_op_write_mdata_file_size;
//   }

//   ret = m_filesystem->pread(fd, &actual_file_size, sizeof(actual_file_size), offsetof(struct xpn_metadata::data, file_size));
//   if (ret > 0 && actual_file_size < head.size){
//     ret = m_filesystem->pwrite(fd, &head.size, sizeof(int64_t), offsetof(struct xpn_metadata::data, file_size));
//   }

//   m_filesystem->close(fd); //TODO: think if necesary check error in close

// cleanup_xpn_server_op_write_mdata_file_size:

//   pthread_mutex_unlock(&op_write_mdata_file_size_mutex);
//   debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] mutex unlock");

//   req.ret = ret;
//   req.server_errno = errno;

//   comm.write_data((char *)&req,sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

//   debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] write_mdata_file_size("<<head.path<<", "<<head.size<<")="<< req.ret);
//   debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] << End");
// }

void xpn_server::op_write_mdata_file_size ( [[maybe_unused]] xpn_server_comm &comm, const st_xpn_server_write_mdata_file_size &head, [[maybe_unused]] int rank_client_id, [[maybe_unused]] int tag_client_id )
{
  XPN_PROFILE_FUNCTION_ARGS("without mutex");
  int ret = 0, fd;
  uint64_t actual_file_size = 0;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] write_mdata_file_size("<<head.path.path<<", "<<head.size<<")");
  
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] xpn_session "<<(int)head.xpn_session<<" fd "<<head.fd);

  file_map_md_fq_item& item = get_mdata_queue(head.path.path);

  {
    std::unique_lock lock(item.m_queue_mutex);
    if (item.m_in_queue < head.size){
      item.m_in_queue = head.size;
    }
  }

  // comm.write_data((char *)&req,sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  {
    std::unique_lock lock(item.m_queue_mutex);
    while(item.m_in_queue != 0){
      if (!item.m_writing.exchange(true)) {
        uint64_t best_file_size = item.m_in_queue;
        item.m_in_queue = 0;
        lock.unlock();
        {
          std::unique_lock wlock(item.m_writing_mutex);
          if (head.xpn_session == 1 && head.fd >= 0){
            fd = head.fd;
          } else {
            fd = m_filesystem->open(head.path.path, O_RDWR);
          }
          if (fd < 0){
            if (errno == EISDIR){
              // if is directory there are no metadata to write so return 0
              ret = 0;
              break;
            }
            ret = fd;
            break;
          }
          ret = m_filesystem->pread(fd, &actual_file_size, sizeof(actual_file_size), offsetof(xpn_metadata::data, file_size));

          if (ret > 0 && actual_file_size < best_file_size){
            ret = m_filesystem->pwrite(fd, &best_file_size, sizeof(int64_t), offsetof(xpn_metadata::data, file_size));
          }
          
          if (head.xpn_session == 1 && head.fd >= 0){
            m_filesystem->fsync(fd);
          }else{
            m_filesystem->close(fd);
          }
        }
        lock.lock();
        item.m_writing.store(false);
      }else{
        break;
      }
    }
  }

  release_mdata_queue(head.path.path, item);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] write_mdata_file_size("<<head.path.path<<", "<<head.size<<", "<<head.fd<<")="<< ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] << End");
}

xpn_server::file_map_md_fq_item &xpn_server::get_mdata_queue(const char *path) {
  std::unique_lock lock(m_file_map_md_fq_mutex);
  auto it = m_file_map_md_fq.find(path);
  if (it == m_file_map_md_fq.end()) {
    auto [new_it, _] =
      m_file_map_md_fq.emplace(std::piecewise_construct, std::forward_as_tuple(path), std::forward_as_tuple());
    new_it->second.m_count++;
    return new_it->second;
  } else {
    it->second.m_count++;
    return it->second;
  }
}

void xpn_server::release_mdata_queue(const char *path, file_map_md_fq_item &item) {
  std::unique_lock lock(m_file_map_md_fq_mutex);
  item.m_count--;
  if (item.m_count == 0) {
    m_file_map_md_fq.erase(path);
  }
}

void xpn_server::op_statvfs ( xpn_server_comm &comm, const st_xpn_server_path &head, int rank_client_id, int tag_client_id )
{
  XPN_PROFILE_FUNCTION();
  struct st_xpn_server_statvfs_req req;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] statvfs("<<head.path.path<<")");

  // do statvfs
  struct ::statvfs attr = {};
  req.status_req.ret = m_filesystem->statvfs(head.path.path, &attr);
  req.attr = st_xpn_server_statvfs{&attr};
  req.status_req.server_errno = errno;

  comm.write_data(&req, sizeof(req), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] statvfs("<<head.path.path<<")="<< req.status_req.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] << End");
}
/* ................................................................... */

} // namespace XPN

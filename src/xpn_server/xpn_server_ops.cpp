
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

#include "xpn_server.hpp"
#include "base_cpp/timer.hpp"
#include <stddef.h>
#include <fcntl.h>

#include <filesystem>
#include <iostream>
#include <string>
#include <cstdlib>
#include <thread>
namespace XPN
{

#define HANDLE_OPERATION(op_struct, op_function) \
  const op_struct* msg_struct = reinterpret_cast<const op_struct*>(msg.msg_buffer); \
  (op_function)(*(comm), (*msg_struct), (rank), (tag));

//Read the operation to realize
void xpn_server::do_operation ( xpn_server_comm *comm, const xpn_server_msg& msg, int rank, int tag, timer timer )
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
                                                  std::unique_ptr<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
                                                  if (xpn_env::get_instance().xpn_stats) { io_stat = std::make_unique<xpn_stats::scope_stat<xpn_stats::io_stats>>(m_stats.m_read_total, msg_struct->size, timer); } 
                                                  break;}
    case xpn_server_ops::WRITE_FILE:             {HANDLE_OPERATION(st_xpn_server_rw,                     op_write);
                                                  std::unique_ptr<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
                                                  if (xpn_env::get_instance().xpn_stats) { io_stat = std::make_unique<xpn_stats::scope_stat<xpn_stats::io_stats>>(m_stats.m_write_total, msg_struct->size, timer); } 
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
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_open] >> Begin");

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_open] open("<<head.path.path<<", "<<head.flags<<", "<<head.mode<<")");

  // do open
  status.ret = PROXY(open)(head.path.path, head.flags, head.mode);
  status.server_errno = errno;
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_open] open("<<head.path.path<<")="<< status.ret);
  if (status.ret < 0){
    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
  }else{
    if (head.xpn_session == 0){
      status.ret = PROXY(close)(status.ret);
    }
    status.server_errno = errno;

    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
  }

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_open] << End");
}

void xpn_server::op_creat ( xpn_server_comm &comm, const st_xpn_server_path_flags &head, int rank_client_id, int tag_client_id )
{
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_creat] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_creat] creat("<<head.path.path<<")");

  // do creat
  status.ret = PROXY(creat)(head.path.path, head.mode);
  status.server_errno = errno;
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_creat] creat("<<head.path.path<<")="<<status.ret);
  if (status.ret < 0){
    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
    return;
  }else{
    status.ret = PROXY(close)(status.ret);
    status.server_errno = errno;

    comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);
  }

  // show debug info
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_creat] << End");
}

void xpn_server::op_read ( xpn_server_comm &comm, const st_xpn_server_rw &head, int rank_client_id, int tag_client_id )
{
  struct st_xpn_server_rw_req req;
  long   size, diff, to_read, cont;
  off_t ret_lseek;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] read("<<head.path.path<<", "<<head.offset<<", "<<head.size<<")");

  // initialize counters
  cont = 0;
  size = head.size;
  if (size > MAX_BUFFER_SIZE) {
    size = MAX_BUFFER_SIZE;
  }
  diff = head.size - cont;

  std::vector<char> buffer(size);

  //Open file
  int fd;
  if (head.xpn_session == 1){
    fd = head.fd;
  }else{
    fd = PROXY(open)(head.path.path, O_RDONLY);
  }
  if (fd < 0)
  {
    req.size = -1;
    req.status.ret = fd;
    req.status.server_errno = errno;
    comm.write_data((char *)&req,sizeof(struct st_xpn_server_rw_req), rank_client_id, tag_client_id);
    goto cleanup_xpn_server_op_read;
  }

  // loop...
  do
  {
    if (diff > size) {
      to_read = size;
    }
    else {
      to_read = diff;
    }

    // lseek and read data...
    ret_lseek = PROXY(lseek)(fd, head.offset + cont, SEEK_SET);
    if (ret_lseek == -1)
    {
      req.size = -1;
      req.status.ret = -1;
      req.status.server_errno = errno;
      comm.write_data((char *)&req,sizeof(struct st_xpn_server_rw_req), rank_client_id, tag_client_id);
      goto cleanup_xpn_server_op_read;
    }
    {
      std::unique_ptr<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
      if (xpn_env::get_instance().xpn_stats) { io_stat = std::make_unique<xpn_stats::scope_stat<xpn_stats::io_stats>>(m_stats.m_read_disk, to_read); } 
      req.size = PROXY(read)(fd, buffer.data(), to_read);
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
    // send (how many + data) to client...
    req.status.ret = 0;
    req.status.server_errno = errno;
    comm.write_data((char *)&req, sizeof(struct st_xpn_server_rw_req), rank_client_id, tag_client_id);
    debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] op_read: send size "<< req.size);

    // send data to client...
    if (req.size > 0)
    {
      {
        std::unique_ptr<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
        if (xpn_env::get_instance().xpn_stats) { io_stat = std::make_unique<xpn_stats::scope_stat<xpn_stats::io_stats>>(m_stats.m_write_net, to_read); } 
        comm.write_data(buffer.data(), req.size, rank_client_id, tag_client_id);
      }
      debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] op_read: send data");
    }
    cont = cont + req.size; //Send bytes
    diff = head.size - cont;

  } while ((diff > 0) && (req.size != 0));
cleanup_xpn_server_op_read:
  if (head.xpn_session == 0){
    PROXY(close)(fd);
  }

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] read("<<head.path.path<<", "<<head.offset<<", "<<head.size<<")="<< cont);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read] << End");
}

void xpn_server::op_write ( xpn_server_comm &comm, const st_xpn_server_rw &head, int rank_client_id, int tag_client_id )
{
  struct st_xpn_server_rw_req req;
  int    size, diff, cont, to_write;
  off_t ret_lseek;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write] write("<<head.path.path<<", "<<head.offset<<", "<<head.size<<")");

  // initialize counters
  cont = 0;
  size = (head.size);
  if (size > MAX_BUFFER_SIZE) {
    size = MAX_BUFFER_SIZE;
  }
  diff = head.size - cont;

  std::vector<char> buffer(size);

  //Open file
  int fd;
  if (head.xpn_session == 1){
    fd = head.fd;
  }else{
    fd = PROXY(open)(head.path.path, O_WRONLY);
  }
  if (fd < 0)
  {
    req.size = -1;
    req.status.ret = -1;
    goto cleanup_xpn_server_op_write;
  }

  // loop...
  do
  {
    if (diff > size){
      to_write = size;
    }
    else{
      to_write = diff;
    }

    // read data from MPI and write into the file
    {
      std::unique_ptr<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
      if (xpn_env::get_instance().xpn_stats) { io_stat = std::make_unique<xpn_stats::scope_stat<xpn_stats::io_stats>>(m_stats.m_read_net, to_write); } 
      comm.read_data(buffer.data(), to_write, rank_client_id, tag_client_id);
    }
    ret_lseek = PROXY(lseek)(fd, head.offset + cont, SEEK_SET);
    if (ret_lseek < 0)
    {
      req.status.ret = -1;
      goto cleanup_xpn_server_op_write;
    }
    {
      std::unique_ptr<xpn_stats::scope_stat<xpn_stats::io_stats>> io_stat;
      if (xpn_env::get_instance().xpn_stats) { io_stat = std::make_unique<xpn_stats::scope_stat<xpn_stats::io_stats>>(m_stats.m_write_disk, to_write); } 
      req.size = PROXY(write)(fd, buffer.data(), to_write);
    }
    if (req.size < 0)
    {
      req.status.ret = -1;
      goto cleanup_xpn_server_op_write;
    }

    // update counters
    cont = cont + req.size; // Received bytes
    diff = head.size - cont;

  } while ((diff > 0) && (req.size != 0));

  req.size = cont;
  req.status.ret = 0;
cleanup_xpn_server_op_write:
  // write to the client the status of the write operation
  req.status.server_errno = errno;
  comm.write_data((char *)&req,sizeof(struct st_xpn_server_rw_req), rank_client_id, tag_client_id);

  if (head.xpn_session == 1){
    PROXY(fsync)(fd);
  }else{
    PROXY(close)(fd);
  }

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write] write("<<head.path.path<<", "<<head.offset<<", "<<head.size<<")="<< cont);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write] << End");
}

void xpn_server::op_close ( xpn_server_comm &comm, const st_xpn_server_close &head, int rank_client_id, int tag_client_id )
{
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_close] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_close] close("<<head.fd<<")");

  status.ret = PROXY(close)(head.fd);
  status.server_errno = errno;
  comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_close] close("<<head.fd<<")="<< status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_close] << End");

}

void xpn_server::op_rm ( xpn_server_comm &comm, const st_xpn_server_path &head, int rank_client_id, int tag_client_id )
{
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm] unlink("<<head.path.path<<")");

  // do rm
  status.ret = PROXY(unlink)(head.path.path);
  status.server_errno = errno;
  comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm] unlink("<<head.path.path<<")="<< status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm] << End");
}

void xpn_server::op_rm_async ( [[maybe_unused]] xpn_server_comm &comm, const st_xpn_server_path &head, [[maybe_unused]] int rank_client_id, [[maybe_unused]] int tag_client_id )
{
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm_async] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm_async] unlink("<<head.path.path<<")");

  // do rm
  PROXY(unlink)(head.path.path);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm_async] unlink("<<head.path.path<<")="<< 0);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rm_async] << End");
}

void xpn_server::op_rename ( xpn_server_comm &comm, const st_xpn_server_rename &head, int rank_client_id, int tag_client_id )
{
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rename] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rename] rename("<<head.paths.path1()<<", "<<head.paths.path2()<<")");

  // do rename
  status.ret = PROXY(rename)(head.paths.path1(), head.paths.path2());
  status.server_errno = errno;
  comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rename] rename("<<head.paths.path1()<<", "<<head.paths.path2()<<")="<<status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rename] << End");
}

void xpn_server::op_getattr ( xpn_server_comm &comm, const st_xpn_server_path &head, int rank_client_id, int tag_client_id )
{
  struct st_xpn_server_attr_req req;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] stat("<<head.path.path<<")");

  // do getattr
  req.status = PROXY(__xstat)(_STAT_VER, head.path.path, &req.attr);
  req.status_req.ret = req.status;
  req.status_req.server_errno = errno;

  comm.write_data((char *)&req,sizeof(struct st_xpn_server_attr_req), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] stat("<<head.path.path<<")="<< req.status);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] << End");
}

void xpn_server::op_setattr ( [[maybe_unused]] xpn_server_comm &comm, [[maybe_unused]] const st_xpn_server_setattr &head, [[maybe_unused]] int rank_client_id, [[maybe_unused]] int tag_client_id)
{
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
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_mkdir] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_mkdir] mkdir("<<head.path.path<<")");

  // do mkdir
  status.ret = PROXY(mkdir)(head.path.path, head.mode);
  status.server_errno = errno;
  comm.write_data((char *)&status,sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_mkdir] mkdir("<<head.path.path<<")="<<status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_mkdir] << End");
}

void xpn_server::op_opendir ( xpn_server_comm &comm, const st_xpn_server_path_flags &head, int rank_client_id, int tag_client_id )
{
  DIR* ret;
  struct st_xpn_server_opendir_req req;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_opendir] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_opendir] opendir("<<head.path.path<<")");

  ret = PROXY(opendir)(head.path.path);
  req.status.ret = ret == NULL ? -1 : 0;
  req.status.server_errno = errno;

  if (req.status.ret == 0){
    if (head.xpn_session == 1){
      req.dir = ret;
    }else{
      req.status.ret = PROXY(telldir)(ret);
    }
    req.status.server_errno = errno;
  }

  if (head.xpn_session == 0){
    PROXY(closedir)(ret);
  }

  comm.write_data((char *)&req, sizeof(struct st_xpn_server_opendir_req), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_opendir] opendir("<<head.path.path<<")=%p"<<ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_opendir] << End");
}

void xpn_server::op_readdir ( xpn_server_comm &comm, const st_xpn_server_readdir &head, int rank_client_id, int tag_client_id )
{
  struct dirent * ret;
  struct st_xpn_server_readdir_req ret_entry;
  DIR* s = NULL;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_readdir] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_readdir] readdir("<<head.path.path<<")");

  if (head.xpn_session == 1){
    // Reset errno
    errno = 0;
    ret = PROXY(readdir)(head.dir);
  }else{

    s = PROXY(opendir)(head.path.path);
    ret_entry.status.ret = s == NULL ? -1 : 0;
    ret_entry.status.server_errno = errno;

    PROXY(seekdir)(s, head.telldir);

    // Reset errno
    errno = 0;
    ret = PROXY(readdir)(s);
  }
  if (ret != NULL)
  {
    ret_entry.end = 1;
    ret_entry.ret = *ret;
  }
  else{
    ret_entry.end = 0;
  }

  ret_entry.status.ret = ret == NULL ? -1 : 0;

  if (head.xpn_session == 0){
    ret_entry.telldir = PROXY(telldir)(s);

    ret_entry.status.ret = PROXY(closedir)(s);
  }
  ret_entry.status.server_errno = errno;

  comm.write_data((char *)&ret_entry, sizeof(struct st_xpn_server_readdir_req), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_readdir] readdir("<<(void*)s<<")="<< (void*)ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_readdir] << End");
}

void xpn_server::op_closedir ( xpn_server_comm &comm, const st_xpn_server_close &head, int rank_client_id, int tag_client_id )
{
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_closedir] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_closedir] closedir("<<(void*)head.dir<<")");

  // do rm
  status.ret = PROXY(closedir)(head.dir);
  status.server_errno = errno;
  comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_closedir] closedir("<<(void*)head.dir<<")="<<status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_closedir] << End");

}

void xpn_server::op_rmdir ( xpn_server_comm &comm, const st_xpn_server_path &head, int rank_client_id, int tag_client_id )
{
  struct st_xpn_server_status status;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir] rmdir("<<head.path.path<<")");

  // do rmdir
  status.ret = PROXY(rmdir)(head.path.path);
  status.server_errno = errno;
  comm.write_data((char *)&status, sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir] rmdir("<<head.path.path<<")="<< status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir] << End");
}

void xpn_server::op_rmdir_async ( [[maybe_unused]] xpn_server_comm &comm, const st_xpn_server_path &head, [[maybe_unused]] int rank_client_id, [[maybe_unused]] int tag_client_id )
{
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir_async] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir_async] rmdir("<<head.path.path<<")");

  // do rmdir
  PROXY(rmdir)(head.path.path);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir_async] rmdir("<<head.path.path<<")="<<0);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_rmdir_async] << End");
}

void xpn_server::op_read_mdata   ( xpn_server_comm &comm, const st_xpn_server_path &head, int rank_client_id, int tag_client_id )
{
  int ret, fd;
  struct st_xpn_server_read_mdata_req req;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read_mdata] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read_mdata] read_mdata("<<head.path.path<<")");

  fd = PROXY(open)(head.path.path, O_RDWR);
  if (fd < 0){
    if (errno == EISDIR){
      // if is directory there are no metadata to read so return 0
      ret = 0;
      req.mdata = {};
      goto cleanup_xpn_server_op_read_mdata;
    }
    ret = fd;
    goto cleanup_xpn_server_op_read_mdata;
  }

  ret = PROXY(read)(fd, &req.mdata, sizeof(req.mdata));


  if (!req.mdata.is_valid()){
    req.mdata = {};
  }

  PROXY(close)(fd); //TODO: think if necesary check error in close

cleanup_xpn_server_op_read_mdata:
  req.status.ret = ret;
  req.status.server_errno = errno;

  comm.write_data((char *)&req,sizeof(struct st_xpn_server_read_mdata_req), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read_mdata] read_mdata("<<head.path.path<<")="<< req.status.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_read_mdata] << End");
}

void xpn_server::op_write_mdata ( xpn_server_comm &comm, const st_xpn_server_write_mdata &head, int rank_client_id, int tag_client_id )
{
  int ret, fd;
  struct st_xpn_server_status req;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata] write_mdata("<<head.path.path<<")");

  fd = PROXY(open)(head.path.path, O_WRONLY | O_CREAT, S_IRWXU);
  if (fd < 0){
    if (errno == EISDIR){
      // if is directory there are no metadata to write so return 0
      ret = 0;
      goto cleanup_xpn_server_op_write_mdata;
    }
    ret = fd;
    goto cleanup_xpn_server_op_write_mdata;
  }
  ret = PROXY(write)(fd, &head.mdata, sizeof(head.mdata));

  PROXY(close)(fd); //TODO: think if necesary check error in close

cleanup_xpn_server_op_write_mdata:
  req.ret = ret;
  req.server_errno = errno;

  comm.write_data((char *)&req,sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata] write_mdata("<<head.path.path<<")=%d"<< req.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata] << End");

}

pthread_mutex_t op_write_mdata_file_size_mutex = PTHREAD_MUTEX_INITIALIZER;

void xpn_server::op_write_mdata_file_size ( xpn_server_comm &comm, const st_xpn_server_write_mdata_file_size &head, int rank_client_id, int tag_client_id )
{
  int ret, fd;
  uint64_t actual_file_size = 0;
  struct st_xpn_server_status req;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] write_mdata_file_size("<<head.path.path<<", "<<head.size<<")");
  
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] mutex lock");
  pthread_mutex_lock(&op_write_mdata_file_size_mutex);

  fd = PROXY(open)(head.path.path, O_RDWR);
  if (fd < 0){
    if (errno == EISDIR){
      // if is directory there are no metadata to write so return 0
      ret = 0;
      goto cleanup_xpn_server_op_write_mdata_file_size;
    }
    ret = fd;
    goto cleanup_xpn_server_op_write_mdata_file_size;
  }

  PROXY(lseek)(fd, offsetof(struct xpn_metadata::data, file_size), SEEK_SET);
  ret = PROXY(read)(fd, &actual_file_size, sizeof(actual_file_size));
  if (ret > 0 && actual_file_size < head.size){
    PROXY(lseek)(fd, offsetof(struct xpn_metadata::data, file_size), SEEK_SET);
    ret = PROXY(write)(fd, &head.size, sizeof(ssize_t));
  }

  PROXY(close)(fd); //TODO: think if necesary check error in close

cleanup_xpn_server_op_write_mdata_file_size:

  pthread_mutex_unlock(&op_write_mdata_file_size_mutex);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] mutex unlock");

  req.ret = ret;
  req.server_errno = errno;

  comm.write_data((char *)&req,sizeof(struct st_xpn_server_status), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] write_mdata_file_size("<<head.path.path<<", "<<head.size<<")="<< req.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_write_mdata_file_size] << End");

}

void xpn_server::op_statvfs ( xpn_server_comm &comm, const st_xpn_server_path &head, int rank_client_id, int tag_client_id )
{
  struct st_xpn_server_statvfs_req req;

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] >> Begin");
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] statvfs("<<head.path.path<<")");

  // do statvfs
  req.status_req.ret = PROXY(statvfs)(head.path.path, &req.attr);
  req.status_req.server_errno = errno;

  comm.write_data(&req, sizeof(req), rank_client_id, tag_client_id);

  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] statvfs("<<head.path.path<<")="<< req.status_req.ret);
  debug_info("[Server="<<serv_name<<"] [XPN_SERVER_OPS] [xpn_server_op_getattr] << End");
}
/* ................................................................... */

} // namespace XPN


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

#include "base_cpp/queue_pool.hpp"
#include "base_cpp/workers.hpp"
#include "coroutine/xpn_coroutine.hpp"
#include "xpn/xpn_stats.hpp"
#include "xpn_server_co_comm.hpp"
#include "xpn_server_ops.hpp"
#include "xpn_server_params.hpp"

namespace XPN {
class xpn_server_co {
   public:
    xpn_server_co(int argc, char *argv[]);
    ~xpn_server_co();
    task<int> run();
    task<int> stop();

    task<int> accept(int socket);
    task<int> dispatcher(int comm_id);
    task<int> do_operation(const xpn_server_msg &msg, int comm_id, int tag_client_id, timer timer);
    task<int> finish();

   public:
    char serv_name[HOST_NAME_MAX];
    xpn_server_params m_params;
    std::unique_ptr<xpn_server_co_control_comm> m_control_comm;
    // std::unique_ptr<workers> m_worker1, m_worker2;

    xpn_stats m_stats;
    std::unique_ptr<xpn_window_stats> m_window_stats;

    std::atomic_bool m_disconnect = {false};
    std::atomic_int64_t m_clients = {0};

    queue_pool<xpn_server_msg> msg_pool;

    co_mutex op_write_mdata_file_size_mutex;

   public:
    // File operations
    task<int> op_open(const st_xpn_server_path_flags &head, int comm_id, int tag_client_id);
    task<int> op_creat(const st_xpn_server_path_flags &head, int comm_id, int tag_client_id);
    task<int> op_read(const st_xpn_server_rw &head, int comm_id, int tag_client_id);
    task<int> op_write(const st_xpn_server_rw &head, int comm_id, int tag_client_id);
    task<int> op_close(const st_xpn_server_close &head, int comm_id, int tag_client_id);
    task<int> op_rm(const st_xpn_server_path &head, int comm_id, int tag_client_id);
    task<int> op_rm_async(const st_xpn_server_path &head, int comm_id, int tag_client_id);
    task<int> op_rename(const st_xpn_server_rename &head, int comm_id, int tag_client_id);
    task<int> op_setattr(const st_xpn_server_setattr &head, int comm_id, int tag_client_id);
    task<int> op_getattr(const st_xpn_server_path &head, int comm_id, int tag_client_id);

    // Directory operations
    task<int> op_mkdir(const st_xpn_server_path_flags &head, int comm_id, int tag_client_id);
    task<int> op_opendir(const st_xpn_server_path_flags &head, int comm_id, int tag_client_id);
    task<int> op_readdir(const st_xpn_server_readdir &head, int comm_id, int tag_client_id);
    task<int> op_closedir(const st_xpn_server_close &head, int comm_id, int tag_client_id);
    task<int> op_rmdir(const st_xpn_server_path &head, int comm_id, int tag_client_id);
    task<int> op_rmdir_async(const st_xpn_server_path &head, int comm_id, int tag_client_id);

    // FS Operations
    task<int> op_statvfs(const st_xpn_server_path &head, int comm_id, int tag_client_id);

    // Metadata
    task<int> op_read_mdata(const st_xpn_server_path &head, int comm_id, int tag_client_id);
    task<int> op_write_mdata(const st_xpn_server_write_mdata &head, int comm_id, int tag_client_id);
    task<int> op_write_mdata_file_size(const st_xpn_server_write_mdata_file_size &head, int comm_id, int tag_client_id);
};
}  // namespace XPN
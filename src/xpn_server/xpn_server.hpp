
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

#include "filesystem/xpn_server_filesystem.hpp"
#include "xpn_server_params.hpp"
#include "xpn_server_comm.hpp"
#include "xpn_server_ops.hpp"
#include "base_cpp/workers.hpp"
#include "base_cpp/queue_pool.hpp"
#include "xpn/xpn_stats.hpp"


namespace XPN
{
    class xpn_server
    {
    public:
        xpn_server(int argc, char *argv[]);
        ~xpn_server();
        int run();
        int stop();
        int print_stats();

        void accept(int socket);
        void dispatcher(std::shared_ptr<xpn_server_comm> comm);
        void one_dispatcher();
        void connectionless_dispatcher();
        void do_operation(xpn_server_comm &comm, const xpn_server_msg& msg, int rank_client_id, int tag_client_id, timer timer);
        void finish();

    public:
        char serv_name[HOST_NAME_MAX];
        xpn_server_params m_params;
        std::unique_ptr<xpn_server_control_comm> m_control_comm;
        std::unique_ptr<xpn_server_control_comm> m_control_comm_connectionless;
        std::unique_ptr<workers> m_worker1, m_worker2, m_workerConnectionLess;

        xpn_stats m_stats;
        std::unique_ptr<xpn_window_stats> m_window_stats;

        bool m_disconnect = false;
        std::unordered_map<int64_t, std::shared_ptr<xpn_server_comm>> m_clients = {};
        std::mutex m_clients_mutex = {};
        std::condition_variable m_clients_cv = {};

        queue_pool<xpn_server_msg> msg_pool;

        std::unique_ptr<xpn_server_filesystem> m_filesystem;

        // op_write_mdata_file_size
        struct file_map_md_fq_item {
            uint64_t m_count = 0;
            
            std::mutex m_writing_mutex = {};
            std::atomic_bool m_writing = {false};
            std::mutex m_queue_mutex = {};
            uint64_t m_in_queue = 0;
        };
        std::mutex m_file_map_md_fq_mutex;
        std::unordered_map<std::string, file_map_md_fq_item> m_file_map_md_fq;

        std::chrono::time_point<std::chrono::high_resolution_clock> m_start_time;
        bool m_some_client_had_error = false;

    public:
        // File operations
        void op_open        ( xpn_server_comm &comm, const st_xpn_server_path_flags   &head, int rank_client_id, int tag_client_id );
        void op_creat       ( xpn_server_comm &comm, const st_xpn_server_path_flags   &head, int rank_client_id, int tag_client_id );
        void op_read        ( xpn_server_comm &comm, const st_xpn_server_rw           &head, int rank_client_id, int tag_client_id );
        void op_write       ( xpn_server_comm &comm, const st_xpn_server_rw           &head, int rank_client_id, int tag_client_id );
        void op_close       ( xpn_server_comm &comm, const st_xpn_server_close        &head, int rank_client_id, int tag_client_id );
        void op_rm          ( xpn_server_comm &comm, const st_xpn_server_path         &head, int rank_client_id, int tag_client_id );
        void op_rm_async    ( xpn_server_comm &comm, const st_xpn_server_path         &head, int rank_client_id, int tag_client_id );
        void op_rename      ( xpn_server_comm &comm, const st_xpn_server_rename       &head, int rank_client_id, int tag_client_id );
        void op_setattr     ( xpn_server_comm &comm, const st_xpn_server_setattr      &head, int rank_client_id, int tag_client_id );
        void op_getattr     ( xpn_server_comm &comm, const st_xpn_server_path         &head, int rank_client_id, int tag_client_id );

        // Directory operations
        void op_mkdir       ( xpn_server_comm &comm, const st_xpn_server_path_flags   &head, int rank_client_id, int tag_client_id );
        void op_opendir     ( xpn_server_comm &comm, const st_xpn_server_path_flags   &head, int rank_client_id, int tag_client_id );
        void op_readdir     ( xpn_server_comm &comm, const st_xpn_server_readdir      &head, int rank_client_id, int tag_client_id );
        void op_closedir    ( xpn_server_comm &comm, const st_xpn_server_close        &head, int rank_client_id, int tag_client_id );
        void op_rmdir       ( xpn_server_comm &comm, const st_xpn_server_path         &head, int rank_client_id, int tag_client_id );
        void op_rmdir_async ( xpn_server_comm &comm, const st_xpn_server_path         &head, int rank_client_id, int tag_client_id );

        // FS Operations
        void op_statvfs      ( xpn_server_comm &comm, const st_xpn_server_path        &head, int rank_client_id, int tag_client_id ); //TODO: implement

        // Metadata
        void op_read_mdata   ( xpn_server_comm &comm, const st_xpn_server_path        &head, int rank_client_id, int tag_client_id );
        void op_write_mdata  ( xpn_server_comm &comm, const st_xpn_server_write_mdata &head, int rank_client_id, int tag_client_id );
        void op_write_mdata_file_size  ( xpn_server_comm &comm, const st_xpn_server_write_mdata_file_size &head, int rank_client_id, int tag_client_id );
        file_map_md_fq_item &get_mdata_queue(const char *path);
        void release_mdata_queue(const char *path, file_map_md_fq_item &item);
        
        // Flush preload
        void op_flush        ( xpn_server_comm &comm, const st_xpn_server_flush_preload_ckpt &head, int rank_client_id, int tag_client_id );
        void op_preload      ( xpn_server_comm &comm, const st_xpn_server_flush_preload_ckpt &head, int rank_client_id, int tag_client_id );
        void op_checkpoint   ( xpn_server_comm &comm, const st_xpn_server_flush_preload_ckpt &head, int rank_client_id, int tag_client_id );
    };    
}
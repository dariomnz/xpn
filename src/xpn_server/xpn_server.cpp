
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

#include <unistd.h>
#include <memory>
#include <vector>
#include <string>
#include <thread>
#include "base_cpp/socket.hpp"
#include "base_cpp/timer.hpp"
#include "base_cpp/xpn_env.hpp"
#include "xpn_server/xpn_server_ops.hpp"
#include "xpn_server_comm.hpp"

#include "xpn_server.hpp"
#include <csignal>

namespace XPN
{

void xpn_server::dispatcher ( std::shared_ptr<xpn_server_comm> comm )
{
    int ret;

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] >> Begin");
    xpn_server_msg* msg;
    xpn_server_ops type_op = xpn_server_ops::size;
    int rank_client_id = 0, tag_client_id = 0;
    int disconnect = 0;
    while (!disconnect)
    {
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] Waiting for operation");
        msg = msg_pool.acquire();
        if (msg == nullptr) {
            debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] ERROR: new msg allocation");
            return;
        }
        
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] read operation");
        ret = comm->read_operation(*msg, rank_client_id, tag_client_id);
        if (ret < 0) {
            debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] ERROR: read operation fail");
            return;
        }
        
        type_op = static_cast<xpn_server_ops>(msg->op);
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] OP '"<<xpn_server_ops_name(type_op)<<"'; OP_ID "<< static_cast<int>(type_op)<<" client_rank "<<rank_client_id<<" tag_client "<<tag_client_id);

        if (type_op == xpn_server_ops::DISCONNECT) {
            debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] DISCONNECT received");

            disconnect = 1;
            std::unique_lock l(m_clients_mutex);
            m_clients.erase(comm->get_rank());
            msg_pool.release(msg);
            continue;
        }
        
        if (type_op == xpn_server_ops::FINALIZE) {
            debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] FINALIZE received");
            
            disconnect = 1;
            std::unique_lock l(m_clients_mutex);
            m_clients.erase(comm->get_rank());
            msg_pool.release(msg);
            continue;
        }
        timer timer;
        m_worker2->launch_no_future([this, timer, comm, msg, rank_client_id, tag_client_id] () {
            std::unique_ptr<xpn_stats::scope_stat<xpn_stats::op_stats>> op_stat;
            if (xpn_env::get_instance().xpn_stats) { op_stat = std::make_unique<xpn_stats::scope_stat<xpn_stats::op_stats>>(m_stats.m_ops_stats[msg->op], timer); } 
            do_operation(*comm, *msg, rank_client_id, tag_client_id, timer);
            msg_pool.release(msg);
        });

        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] Worker launched");
    }

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] Client "<<rank_client_id<<" close");
    {
        std::unique_lock l(m_clients_mutex);
        m_clients.erase(comm->get_rank());
    }
    m_control_comm->disconnect(comm);

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] End");
}

void xpn_server::one_dispatcher () {
    
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_one_dispatcher] >> Begin");
    int ret;
    xpn_server_msg* msg;
    xpn_server_ops type_op = xpn_server_ops::size;
    int rank_client_id = 0, tag_client_id = 0;

    while (!m_disconnect)
    {
        {
            std::unique_lock l(m_clients_mutex);
            m_clients_cv.wait(l, [this](){return m_clients.size() != 0 || m_disconnect;});
            if (m_disconnect){
                break;
            }
            debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_one_dispatcher] Waiting for operation, clients: "<<m_clients.size());
        }
        msg = msg_pool.acquire();
        if (msg == nullptr) {
            debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_one_dispatcher] ERROR: new msg allocation");
            return;
        }

        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_one_dispatcher] read operation");
        ret = m_control_comm->read_operation(*msg, rank_client_id, tag_client_id);
        if (ret < 0) {
            debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_one_dispatcher] ERROR: read operation fail mark as disconnected");
            msg->op = static_cast<int32_t>(xpn_server_ops::DISCONNECT);
            m_some_client_had_error = true;
        }

        type_op = static_cast<xpn_server_ops>(msg->op);
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_one_dispatcher] OP '"<<xpn_server_ops_name(type_op)<<"'; OP_ID "<< static_cast<int>(type_op)<<" client_rank "<<rank_client_id<<" tag_client "<<tag_client_id);

        if (type_op == xpn_server_ops::DISCONNECT || type_op == xpn_server_ops::FINALIZE) {
            debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_one_dispatcher] DISCONNECT received");

            m_control_comm->disconnect(rank_client_id);
            
            {
                std::unique_lock l(m_clients_mutex);
                m_clients.erase(rank_client_id);
                m_clients_cv.notify_all();
                debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_one_dispatcher] Currently "<<m_clients.size()<<" clients");
            }
            
            msg_pool.release(msg);
            continue;
        }

        timer timer;
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_one_dispatcher] Worker launch");
        m_worker2->launch_no_future([this, timer, msg, rank_client_id, tag_client_id] () {
            std::unique_ptr<xpn_stats::scope_stat<xpn_stats::op_stats>> op_stat;
            if (xpn_env::get_instance().xpn_stats) { op_stat = std::make_unique<xpn_stats::scope_stat<xpn_stats::op_stats>>(m_stats.m_ops_stats[msg->op], timer); }
            xpn_server_comm *comm_ptr = nullptr;
            {
                std::unique_lock l(m_clients_mutex);
                comm_ptr = m_clients[rank_client_id].get();
            }
            do_operation(*comm_ptr, *msg, rank_client_id, tag_client_id, timer);
            msg_pool.release(msg);
            m_control_comm->rearm(rank_client_id);
        });
        
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_one_dispatcher] Worker launched");
    }

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_one_dispatcher] End");
}

// This is only used in the sck_server
void xpn_server::connectionless_dispatcher () {
    
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] >> Begin");
    int ret;
    xpn_server_msg* msg;
    xpn_server_ops type_op = xpn_server_ops::size;
    int rank_client_id = 0, tag_client_id = 0;

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] accept in port "<<m_control_comm_connectionless->m_port_name);

    while (!m_disconnect)
    {
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] Waiting for operation");
        msg = msg_pool.acquire();
        if (msg == nullptr) {
            debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] ERROR: new msg allocation");
            return;
        }

        std::shared_ptr<xpn_server_comm> comm = m_control_comm_connectionless->accept(-1, false);
        if (m_disconnect) {
            debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] disconnect");
            return;
        }
        if (!comm) {
            debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] ERROR: server_accept");
            continue;
        }


        ret = comm->read_operation(*msg, rank_client_id, tag_client_id);
        if (ret < 0) {
            debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] ERROR: read operation fail");
            continue;
        }

        type_op = static_cast<xpn_server_ops>(msg->op);
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] OP '"<<xpn_server_ops_name(type_op)<<"'; OP_ID "<< static_cast<int>(type_op)<<" client_rank "<<rank_client_id<<" tag_client "<<tag_client_id);

        if (type_op == xpn_server_ops::DISCONNECT || type_op == xpn_server_ops::FINALIZE) {
            debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] DISCONNECT received");
            continue;
        }

        timer timer;
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] Worker launch");
        m_worker2->launch_no_future([this, comm, timer, msg, rank_client_id, tag_client_id] () {
            std::unique_ptr<xpn_stats::scope_stat<xpn_stats::op_stats>> op_stat;
            if (xpn_env::get_instance().xpn_stats) { op_stat = std::make_unique<xpn_stats::scope_stat<xpn_stats::op_stats>>(m_stats.m_ops_stats[msg->op], timer); }
            do_operation(*comm, *msg, rank_client_id, tag_client_id, timer);
            m_control_comm_connectionless->disconnect(comm);
            msg_pool.release(msg);
        });
        
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] Worker launched");
    }

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_connectionless_dispatcher] End");
}

void xpn_server::accept ( int connection_socket )
{
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Start accepting");
    
    std::shared_ptr<xpn_server_comm> comm = m_control_comm->accept(connection_socket);
    {
        std::unique_lock l(m_clients_mutex);
        m_clients.emplace(comm->get_rank(), comm);
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] notify new client "<<m_clients.size());
        m_clients_cv.notify_all();
    }

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Accept received");

    if (m_params.srv_type == server_type::FABRIC || m_params.srv_type == server_type::SCK){
        static bool only_one = true;
        if (only_one){
            only_one = false;
            const int THREADS = 1;
            for(size_t i = 0; i < THREADS; i++){
                m_worker1->launch_no_future([this]{
                    this->one_dispatcher();
                    return 0;
                });
            }
        }
    }else{
        m_worker1->launch_no_future([this, comm]{
            this->dispatcher(comm);
            return 0;
        });
    }
}

void xpn_server::finish ( void )
{
    // Wait and finalize for all current workers
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_finish] Workers destroy");
    
    {
        std::unique_lock l(m_clients_mutex);
        m_disconnect = true;
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_finish] notify disconnect");
        m_clients_cv.notify_all();
    }

    m_control_comm_connectionless.reset();
    m_control_comm.reset();
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_finish] comms destroy");
    
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_finish] Before worker1 reset");
    m_worker1.reset();
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_finish] Before worker2 reset");
    m_worker2.reset();
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_finish] Before workerConnectionLess reset");
    m_workerConnectionLess.reset();
    
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_finish] workers destroy");
}

/* ... Functions / Funciones ......................................... */

xpn_server::xpn_server(int argc, char *argv[]) : m_params(argc, argv)
{
    if (xpn_env::get_instance().xpn_stats){
        m_window_stats = std::make_unique<xpn_window_stats>(m_stats);
    }
    m_filesystem = xpn_server_filesystem::Create(m_params.fs_mode);
    if (!m_filesystem){
        std::cerr << "Error: unexpected error cannot create filesystem interface" << std::endl;
        std::raise(SIGTERM);
    }

    m_start_time = std::chrono::high_resolution_clock::now();
}

xpn_server::~xpn_server()
{
}

// Start servers
int xpn_server::run()
{
    int ret;
    int server_socket;
    int connection_socket;
    int recv_code = 0;
    timer timer;

    XPN_PROFILE_BEGIN_SESSION("xpn_server");
    XPN_PROFILE_FUNCTION();

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] >> Begin");

    // Initialize server
    // * mpi_comm initialization
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Comm initialization");

    m_control_comm = xpn_server_control_comm::Create(m_params);

    // * Workers initialization
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Workers initialization");

    m_worker1 = workers::Create(workers_mode::thread_on_demand, false);
    if (m_worker1 == nullptr) {
        debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] ERROR: Workers initialization fails");
        return -1;
    }

    m_worker2 = workers::Create(m_params.thread_mode);
    if (m_worker2 == nullptr) {
        debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] ERROR: Workers initialization fails");
        return -1;
    }

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Comm connectionless initialization");
    xpn_server_params connectionless_params{m_params.argc, m_params.argv};
    connectionless_params.srv_type = server_type::SCK;
    connectionless_params.srv_comm_port = m_params.srv_connectionless_port;
    m_control_comm_connectionless = xpn_server_control_comm::Create(connectionless_params);

    m_workerConnectionLess = workers::Create(workers_mode::thread_on_demand);
    if (m_workerConnectionLess == nullptr) {
        debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] ERROR: Workers initialization fails");
        return -1;
    }
    
    m_workerConnectionLess->launch_no_future([this](){
        connectionless_dispatcher();
    });

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Control socket initialization");
    ret = socket::server_create(m_params.srv_control_port, server_socket);
    if (ret < 0) {
        debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] ERROR: Socket initialization fails");
        return -1;
    }
    
    std::cout << " | * Time to initialize XPN server: " << timer.elapsed() << std::endl;

    int the_end = 0;
    uint32_t ack = 0;
    while (!the_end)
    {
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Listening to conections");
        ret = socket::server_accept(server_socket, connection_socket);
        if (ret < 0) continue;
        XPN_PROFILE_SCOPE("Op control socket");
        ret = socket::recv(connection_socket, &recv_code, sizeof(recv_code));
        if (ret < 0) continue;

        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] socket recv: "<<recv_code);
        switch (recv_code)
        {
            case socket::xpn_server::ACCEPT_CODE:
                accept(connection_socket);
                break;

            case socket::xpn_server::CONNECTIONLESS_PORT_CODE: {
                std::string port_name(MAX_PORT_NAME, '\0');
                if (m_control_comm_connectionless) {
                    port_name = m_control_comm_connectionless->m_port_name;
                }
                ret = socket::send(connection_socket, port_name.data(), MAX_PORT_NAME);
                if (ret < 0){
                    print("[Server="<<ns::get_host_name()<<"] [MPI_SERVER_CONTROL_COMM] [mpi_server_control_comm_accept] ERROR: socket send port fails");
                }
                } break;

            case socket::xpn_server::STATS_wINDOW_CODE:
                if (m_window_stats){
                    socket::send(connection_socket, &m_window_stats->get_current_stats(), sizeof(m_stats));
                }else{
                    socket::send(connection_socket, &m_stats, sizeof(m_stats));
                }
                break;
            case socket::xpn_server::STATS_CODE:
                socket::send(connection_socket, &m_stats, sizeof(m_stats));
                break;

            case socket::xpn_server::FINISH_CODE:
            case socket::xpn_server::FINISH_CODE_AWAIT:
                finish();
                the_end = 1;
                if (recv_code == socket::xpn_server::FINISH_CODE_AWAIT){
                    socket::send(connection_socket, &recv_code, sizeof(recv_code));
                }
                break;

            case socket::xpn_server::PING_CODE:
                socket::send(connection_socket, &ack, sizeof(ack));
                break;

            default:
                debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] >> Socket recv unknown code "<< recv_code);
                break;
        }

        socket::close(connection_socket);
    }

    socket::close(server_socket);

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] >> End");
    return 0;
}

// Stop servers
int xpn_server::stop()
{
    int res = 0;
    char srv_name[1024];
    FILE *file;

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] >> Begin");

    std::vector<std::string> srv_names;
    if (!m_params.shutdown_file.empty()) {

        // Open host file
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] Open host file "<< m_params.shutdown_file);

        file = fopen(m_params.shutdown_file.c_str(), "r");
        if (file == NULL) {
            debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] ERROR: invalid file "<< m_params.shutdown_file);
            return -1;
        }

        while (fscanf(file, "%[^\n] ", srv_name) != EOF) {
            srv_names.push_back(srv_name);
        }
        
        // Close host file
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] Close host file");
        
        fclose(file);
    } else if (!m_params.shutdown_hostlist.empty()) {
        std::stringstream ss(m_params.shutdown_hostlist);
        std::string token;

        while (std::getline(ss, token, ',')) {
            srv_names.push_back(token);
        }
    } else {
        std::cerr << "It is necesary to provide --shutdown_hosts or --shutdown_file" << std::endl;
        return -1;
    }
    
    std::unique_ptr<workers> worker = workers::Create(workers_mode::thread_on_demand);
    std::vector<std::future<int>> v_res(srv_names.size());
    int index = 0;
    for (auto &name : srv_names)
    {
        v_res[index++] = worker->launch([this, &name] (){

            printf(" * Stopping server (%s)\n", name.c_str());
            int socket;
            int ret;
            int buffer = socket::xpn_server::FINISH_CODE;
            if (m_params.await_stop == 1){
                buffer = socket::xpn_server::FINISH_CODE_AWAIT;
            }
            std::string srv_name;
            int srv_port;
            auto port_pos = name.find_last_of(":");
            if (port_pos != std::string::npos){
                srv_name = name.substr(0, port_pos);
                std::string aux_port = name.substr(port_pos + 1, name.size() - (port_pos + 1));
                srv_port = stoi(aux_port);
            }else{
                srv_name = name;
                srv_port = DEFAULT_XPN_SERVER_CONTROL_PORT;
            }
            ret = socket::client_connect(srv_name, srv_port, socket);
            if (ret < 0) {
                print("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] ERROR: socket connection " << name);
                return ret;
            }

            ret = socket::send(socket, &buffer, sizeof(buffer));
            if (ret < 0) {
                print("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] ERROR: socket send " << name);
            }
            
            if (m_params.await_stop == 0){
                socket::close(socket);
            }

            if (m_params.await_stop == 1){
                ret = socket::recv(socket, &buffer, sizeof(buffer));
                if (ret < 0) {
                    print("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] ERROR: socket recv " << name);
                }
                socket::close(socket);
            }
            return ret;
        });
    }

    int aux_res;
    for (auto &fut : v_res)
    {
        aux_res = fut.get();
        if (aux_res < 0){
            res = aux_res;
        }
    }
    
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] >> End");

    return res;
}
} // namespace XPN
// Main
int main ( int argc, char *argv[] )
{
    int ret = -1;
    char *exec_name = NULL;

    // Initializing...
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    // Get arguments..
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [main] Get server params");

    XPN::xpn_server server(argc, argv);

    exec_name = basename(argv[0]);
    gethostname(server.serv_name, HOST_NAME_MAX);

    // Welcome...
    printf("\n");
    printf(" + xpn_server\n");
    printf(" | ----------\n");

    // Show configuration...
    printf(" | * action=%s\n", exec_name);
    printf(" | * host=%s\n", server.serv_name);
    server.m_params.show();

    // Do associate action...
    if (strcasecmp(exec_name, "xpn_stop_server") == 0) {
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [main] Down servers");
        ret = server.stop();
    } else {
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [main] Up servers");
        ret = server.run();
    }

    return ret;
}

/* ................................................................... */


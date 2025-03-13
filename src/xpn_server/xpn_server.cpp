
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
#include <vector>
#include <string>
#include <thread>
#include "base_cpp/socket.hpp"
#include "base_cpp/timer.hpp"
#include "base_cpp/xpn_env.hpp"
#include "xpn_server_comm.hpp"
#include "fabric_server/fabric_server_comm.hpp"

#include "xpn_server.hpp"

namespace XPN
{

void xpn_server::dispatcher ( xpn_server_comm* comm )
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
            m_clients--;
            continue;
        }

        if (type_op == xpn_server_ops::FINALIZE) {
            debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] FINALIZE received");

            disconnect = 1;
            m_clients--;
            continue;
        }
        timer timer;
        m_worker2->launch_no_future([this, timer, comm, msg, rank_client_id, tag_client_id] {
            std::unique_ptr<xpn_stats::scope_stat<xpn_stats::op_stats>> op_stat;
            if (xpn_env::get_instance().xpn_stats) { op_stat = std::make_unique<xpn_stats::scope_stat<xpn_stats::op_stats>>(m_stats.m_ops_stats[msg->op], timer); } 
            do_operation(comm, *msg, rank_client_id, tag_client_id, timer);
            msg_pool.release(msg);
        });

        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] Worker launched");
    }

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] Client "<<rank_client_id<<" close");

    m_control_comm->disconnect(comm);

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] End");
}

void xpn_server::fabric_dispatcher ( xpn_server_comm* comm )
{
    int ret;

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_fabric_dispatcher] >> Begin");
    xpn_server_msg* msg;
    xpn_server_ops type_op = xpn_server_ops::size;
    int rank_client_id = 0, tag_client_id = 0;

    while (!m_disconnect)
    {
        while(m_clients == 0 && !m_disconnect){
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (m_disconnect){
            break;
        }
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_fabric_dispatcher] Waiting for operation");
        msg = msg_pool.acquire();
        if (msg == nullptr) {
            debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_dispatcher] ERROR: new msg allocation");
            return;
        }

        ret = comm->read_operation(*msg, rank_client_id, tag_client_id);
        if (ret < 0) {
            debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_fabric_dispatcher] ERROR: read operation fail");
            return;
        }

        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_fabric_dispatcher] OP '"<<xpn_server_ops_name(type_op)<<"'; OP_ID "<< static_cast<int>(type_op)<<" client_rank "<<rank_client_id<<" tag_client "<<tag_client_id);

        type_op = static_cast<xpn_server_ops>(msg->op);
        if (type_op == xpn_server_ops::DISCONNECT || type_op == xpn_server_ops::FINALIZE) {
            debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_fabric_dispatcher] DISCONNECT received");

            fabric_server_control_comm::disconnect(rank_client_id);
            m_clients--;

            debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_fabric_dispatcher] Currently "<<m_clients.load()<<" clients");
            continue;
        }

        timer timer;
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_fabric_dispatcher] Worker launch");
        m_worker2->launch_no_future([this, timer, comm, msg, rank_client_id, tag_client_id]{
            std::unique_ptr<xpn_stats::scope_stat<xpn_stats::op_stats>> op_stat;
            if (xpn_env::get_instance().xpn_stats) { op_stat = std::make_unique<xpn_stats::scope_stat<xpn_stats::op_stats>>(m_stats.m_ops_stats[msg->op], timer); } 
            do_operation(comm, *msg, rank_client_id, tag_client_id, timer);
            msg_pool.release(msg);
        });

        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_fabric_dispatcher] Worker launched");
    }

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_fabric_dispatcher] Client "<<rank_client_id<<" close");

    m_control_comm->disconnect(comm);

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_fabric_dispatcher] End");
}

void xpn_server::accept ( int connection_socket )
{
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Start accepting");
    
    xpn_server_comm* comm = m_control_comm->accept(connection_socket);

    m_clients++;
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Accept received");

    if (m_params.server_type == XPN_SERVER_TYPE_FABRIC){
        delete comm;
        xpn_server_comm* general_comm = new fabric_server_comm(-1);
        static bool only_one = true;
        if (only_one){
            only_one = false;
            m_worker1->launch_no_future([this, general_comm]{
                this->fabric_dispatcher(general_comm);
                return 0;
            });
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
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Workers destroy");
    
    m_disconnect = true;
    // base_workers_destroy(&m_worker1);
    m_worker1.reset();
    m_worker2.reset();
    // base_workers_destroy(&m_worker2);

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] mpi_comm destroy");

    m_control_comm.reset();
}

/* ... Functions / Funciones ......................................... */

xpn_server::xpn_server(int argc, char *argv[]) : m_params(argc, argv)
{
    if (xpn_env::get_instance().xpn_stats){
        m_window_stats = std::make_unique<xpn_window_stats>(m_stats);
    }
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

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] >> Begin");

    // Initialize server
    // * mpi_comm initialization
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Comm initialization");

    m_control_comm = xpn_server_control_comm::Create(m_params);

    // * Workers initialization
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Workers initialization");

    m_worker1 = workers::Create(m_params.thread_mode_connections, false);
    if (m_worker1 == nullptr) {
        debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] ERROR: Workers initialization fails");
        return -1;
    }

    m_worker2 = workers::Create(m_params.thread_mode_operations, false);
    if (m_worker2 == nullptr) {
        debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] ERROR: Workers initialization fails");
        return -1;
    }

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] Control socket initialization");
    ret = socket::server_create(xpn_env::get_instance().xpn_sck_port, server_socket);
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

        ret = socket::recv(connection_socket, &recv_code, sizeof(recv_code));
        if (ret < 0) continue;

        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] socket recv: "<<recv_code);
        switch (recv_code)
        {
            case socket::xpn_server::ACCEPT_CODE:
                accept(connection_socket);
                break;

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

// Start servers spawn
// int xpn_is_server_spawned ( void )
// {
//     int ret;

//     debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_is_server_spawned] >> Begin");

//     #ifdef ENABLE_MPI_SERVER
//     // Initialize server
//     // mpi_comm initialization
//     debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_is_server_spawned] mpi_comm initialization");
//     ret = PMPI_Init(&params.argc, &params.argv);

//     // TODO: check if necesary bypass the bypass with dlysm RTLD_NEXT
//     filesystem_low_set(RTLD_NEXT);

//     // Workers initialization
//     debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_is_server_spawned] Workers initialization");

//     // in spawn there are no connections so server is secuential
//     ret = base_workers_init(&worker1, TH_NOT);
//     if (ret < 0) {
//         printf("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_is_server_spawned] ERROR: Workers initialization fails");
//         return -1;
//     }

//     ret = base_workers_init(&worker2, params.thread_mode_operations);
//     if (ret < 0) {
//         printf("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_is_server_spawned] ERROR: Workers initialization fails");
//         return -1;
//     }

//     debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_is_server_spawned] Get parent");
//     struct st_th th_arg;
//     MPI_Comm *parent;

//     parent = (MPI_Comm *)malloc(sizeof(MPI_Comm));
//     if (NULL == parent) {
//         printf("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_is_server_spawned] ERROR: Memory allocation");
//         return -1;
//     }

//     ret = MPI_Comm_get_parent(parent);
//     if ( (ret < 0) || (MPI_COMM_NULL == *parent) ) {
//         printf("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_is_server_spawned] ERROR: parent not found");
//         return -1;
//     }

//     // Launch dispatcher per aplication
//     th_arg.params = &params;
//     th_arg.comm = parent;
//     th_arg.function = xpn_server_dispatcher;
//     th_arg.type_op = 0;
//     th_arg.rank_client_id = 0;
//     th_arg.tag_client_id = 0;
//     th_arg.wait4me = FALSE;

//     base_workers_launch(&worker1, &th_arg, xpn_server_dispatcher);

//     base_workers_destroy(&worker1);
//     base_workers_destroy(&worker2);
//     PMPI_Finalize();

//     #else
//     printf("WARNING: if you have not compiled XPN with the MPI server then you cannot use spawn server.\n");
//     #endif

//     debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_is_server_spawned] >> End");
//     return 0;
// }

// Stop servers
int xpn_server::stop()
{
    int res = 0;
    char srv_name[1024];
    FILE *file;

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] >> Begin");

    // Open host file
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] Open host file "<< m_params.shutdown_file);

    file = fopen(m_params.shutdown_file.c_str(), "r");
    if (file == NULL) {
        debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] ERROR: invalid file "<< m_params.shutdown_file);
        return -1;
    }

    std::vector<std::string> srv_names;
    while (fscanf(file, "%[^\n] ", srv_name) != EOF)
    {
        srv_names.push_back(srv_name);
    }

    // Close host file
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] Close host file");

    fclose(file);

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
            ret = socket::client_connect(name, xpn_env::get_instance().xpn_sck_port, socket);
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

// Stats servers
int xpn_server::print_stats()
{
    char srv_name[1024];
    FILE *file;

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_print_stats] >> Begin");

    // Open host file
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_print_stats] Open host file "<< m_params.shutdown_file);

    file = fopen(m_params.shutdown_file.c_str(), "r");
    if (file == NULL) {
        debug_error("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_print_stats] ERROR: invalid file "<< m_params.shutdown_file);
        return -1;
    }

    std::vector<std::string> srv_names;
    while (fscanf(file, "%[^\n] ", srv_name) != EOF)
    {
        srv_names.push_back(srv_name);
    }

    // Close host file
    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_print_stats] Close host file");

    fclose(file);

    std::cout << std::endl;
    for (auto &name : srv_names)
    {
        int socket;
        int ret;
        int buffer = socket::xpn_server::STATS_CODE;
        xpn_stats stat_buff;
        ret = socket::client_connect(name.data(), xpn_env::get_instance().xpn_sck_port, socket);
        if (ret < 0) {
            print("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_print_stats] ERROR: socket connection " << name);
            continue;
        }

        ret = socket::send(socket, &buffer, sizeof(buffer));
        if (ret < 0) {
            print("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_print_stats] ERROR: socket send " << name);
            continue;
        }
        
        ret = socket::recv(socket, &stat_buff, sizeof(stat_buff));
        if (ret < 0) {
            print("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_print_stats] ERROR: socket recv " << name);
            continue;
        }
        socket::close(socket);

        std::cout << "Server " << name << ":" << std::endl;
        std::cout << "Bandwidth :" << std::endl;
        std::cout << stat_buff.to_string_bandwidth() << std::endl;
        std::cout << "Ops :" << std::endl;
        std::cout << stat_buff.to_string_ops() << std::endl;
        std::cout << std::endl;
    }

    debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_up] >> End");

    return 0;
}
// int xpn_server_terminate ( void )
// {
//     int ret;
//     int buffer = SOCKET_FINISH_CODE;
//     int connection_socket;

//     printf(" * Stopping server (%s)\n", params.srv_name);
//     /*
//     printf("\n");
//     printf(" ----------------\n");
//     printf(" Stopping server (%s)\n", params.srv_name);
//     printf(" ----------------\n");
//     printf("\n");
//     */

//     ret = socket_client_connect(params.srv_name, &connection_socket);
//     if (ret < 0) {
//         printf("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] ERROR: socket connection %s", params.srv_name);
//         return -1 ;
//     }

//     ret = socket_send(connection_socket, &buffer, sizeof(buffer));
//     if (ret < 0) {
//         printf("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [xpn_server_down] ERROR: socket send %s", params.srv_name);
//         return -1 ;
//     }

//     close(connection_socket);
//     return 0;
// }

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

    // ret = xpn_server_params_get(&params, argc, argv);
    // if (ret < 0) {
        // xpn_server_params_show_usage();
        // return -1;
    // }
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
    // xpn_server_params_show(&params);


    // Do associate action...
    if (strcasecmp(exec_name, "xpn_server_spawn") == 0)
    {
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [main] Spawn server");
        // ret = xpn_is_server_spawned(); //TODO: check if si posible with mpi ofi
    }
    else if (strcasecmp(exec_name, "xpn_stop_server") == 0)
    {
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [main] Down servers");
        ret = server.stop();
        // ret = xpn_server_down();
    }
    else if (strcasecmp(exec_name, "xpn_terminate_server") == 0)
    {
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [main] Terminate server");
        // ret = xpn_server_terminate();
    }
    else if (strcasecmp(exec_name, "xpn_server_stats") == 0)
    {
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [main] Terminate server");
        ret = server.print_stats();
    }
    else
    {
        debug_info("[TH_ID="<<std::this_thread::get_id()<<"] [XPN_SERVER] [main] Up servers");
        ret = server.run();
    }

    return ret;
}

/* ................................................................... */



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

#include "xpn_server_co.hpp"

#include <unistd.h>

#include <string>
#include <thread>
#include <vector>

#include "base_cpp/socket.hpp"
#include "base_cpp/timer.hpp"
#include "base_cpp/xpn_env.hpp"
#include "coroutine/xpn_coroutine.hpp"
#include "coroutine/xpn_socket_co.hpp"
#include "xpn_server_co_comm.hpp"

namespace XPN {

task<int> xpn_server_co::dispatcher(int comm_id) {
    int ret;

    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_dispatcher] >> Begin");
    xpn_server_msg *msg;
    xpn_server_ops type_op = xpn_server_ops::size;
    int tag_client_id = 0;
    int disconnect = 0;
    while (!disconnect) {
        debug_info("[TH_ID=" << std::this_thread::get_id()
                             << "] [XPN_SERVER] [xpn_server_dispatcher] Waiting for operation");
        msg = msg_pool.acquire();
        if (msg == nullptr) {
            debug_error("[TH_ID=" << std::this_thread::get_id()
                                  << "] [XPN_SERVER] [xpn_server_dispatcher] ERROR: new msg allocation");
            co_return -1;
        }
        ret = co_await xpn_server_co_comm::read_operation(*msg, comm_id, tag_client_id);
        if (ret < 0) {
            debug_error("[TH_ID=" << std::this_thread::get_id()
                                  << "] [XPN_SERVER] [xpn_server_dispatcher] ERROR: read operation fail");
            co_return -1;
        }

        type_op = static_cast<xpn_server_ops>(msg->op);
        debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_dispatcher] OP '"
                             << xpn_server_ops_name(type_op) << "'; OP_ID " << static_cast<int>(type_op)
                             << " client_rank " << comm_id << " tag_client " << tag_client_id);

        if (type_op == xpn_server_ops::DISCONNECT) {
            debug_info("[TH_ID=" << std::this_thread::get_id()
                                 << "] [XPN_SERVER] [xpn_server_dispatcher] DISCONNECT received");

            disconnect = 1;
            m_clients--;
            continue;
        }

        if (type_op == xpn_server_ops::FINALIZE) {
            debug_info("[TH_ID=" << std::this_thread::get_id()
                                 << "] [XPN_SERVER] [xpn_server_dispatcher] FINALIZE received");

            disconnect = 1;
            m_clients--;
            continue;
        }
        timer timer;
        {
            std::unique_ptr<xpn_stats::scope_stat<xpn_stats::op_stats>> op_stat;
            if (xpn_env::get_instance().xpn_stats) {
                op_stat =
                    std::make_unique<xpn_stats::scope_stat<xpn_stats::op_stats>>(m_stats.m_ops_stats[msg->op], timer);
            }
            co_await do_operation(*msg, comm_id, tag_client_id, timer);
            msg_pool.release(msg);
        }
        debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_dispatcher] Worker launched");
    }

    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_dispatcher] Client " << comm_id
                         << " close");

    m_control_comm->disconnect(comm_id);

    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_dispatcher] End");

    co_return 0;
}

task<int> xpn_server_co::accept(int connection_socket) {
    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_up] Start accepting");

    int comm_id = co_await m_control_comm->accept(connection_socket);

    m_clients++;
    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_up] Accept received");

    dispatcher(comm_id);
    co_return 0;
}

task<int> xpn_server_co::finish(void) {
    // Wait and finalize for all current workers
    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_up] Workers destroy");

    m_disconnect = true;
    // base_workers_destroy(&m_worker1);
    // m_worker1.reset();
    // m_worker2.reset();
    // base_workers_destroy(&m_worker2);

    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_up] mpi_comm destroy");

    m_control_comm.reset();
    co_return 0;
}

/* ... Functions / Funciones ......................................... */

xpn_server_co::xpn_server_co(int argc, char *argv[]) : m_params(argc, argv) {
    if (xpn_env::get_instance().xpn_stats) {
        m_window_stats = std::make_unique<xpn_window_stats>(m_stats);
    }
}

xpn_server_co::~xpn_server_co() {}

// Start servers
task<int> xpn_server_co::run() {
    int ret;
    int server_socket;
    int connection_socket;
    int recv_code = 0;
    timer timer;

    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_up] >> Begin");

    // Initialize server
    // * mpi_comm initialization
    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_up] Comm initialization");

    m_control_comm = std::make_unique<xpn_server_co_control_comm>();

    // * Workers initialization
    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_up] Workers initialization");

    // m_worker1 = workers::Create(m_params.thread_mode_connections, false);
    // if (m_worker1 == nullptr) {
    //     debug_error("[TH_ID=" << std::this_thread::get_id()
    //                           << "] [XPN_SERVER] [xpn_server_up] ERROR: Workers initialization fails");
    //     co_return -1;
    // }

    // m_worker2 = workers::Create(m_params.thread_mode_operations);
    // if (m_worker2 == nullptr) {
    //     debug_error("[TH_ID=" << std::this_thread::get_id()
    //                           << "] [XPN_SERVER] [xpn_server_up] ERROR: Workers initialization fails");
    //     co_return -1;
    // }

    debug_info("[TH_ID=" << std::this_thread::get_id()
                         << "] [XPN_SERVER] [xpn_server_up] Control socket initialization");
    ret = socket::server_create(xpn_env::get_instance().xpn_sck_port, server_socket);
    if (ret < 0) {
        debug_error("[TH_ID=" << std::this_thread::get_id()
                              << "] [XPN_SERVER] [xpn_server_up] ERROR: Socket initialization fails");
        co_return -1;
    }

    std::cout << " | * Time to initialize XPN server: " << timer.elapsed() << std::endl;

    int the_end = 0;
    uint32_t ack = 0;
    while (!the_end) {
        debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_up] Listening to conections");
        connection_socket = co_await SocketAcceptAwaitable(server_socket);
        if (connection_socket < 0) continue;

        ret = co_await SocketRecvAwaitable(connection_socket, &recv_code, sizeof(recv_code));
        if (ret < 0) continue;

        debug_info("[TH_ID=" << std::this_thread::get_id()
                             << "] [XPN_SERVER] [xpn_server_up] socket recv: " << recv_code);
        switch (recv_code) {
            case socket::xpn_server::ACCEPT_CODE:
                co_await accept(connection_socket);
                break;

            case socket::xpn_server::STATS_wINDOW_CODE:
                if (m_window_stats) {
                    co_await SocketSendAwaitable(connection_socket, &m_window_stats->get_current_stats(),
                                                 sizeof(m_stats));
                } else {
                    co_await SocketSendAwaitable(connection_socket, &m_stats, sizeof(m_stats));
                }
                break;
            case socket::xpn_server::STATS_CODE:
                co_await SocketSendAwaitable(connection_socket, &m_stats, sizeof(m_stats));
                break;

            case socket::xpn_server::FINISH_CODE:
            case socket::xpn_server::FINISH_CODE_AWAIT:
                co_await finish();
                the_end = 1;
                if (recv_code == socket::xpn_server::FINISH_CODE_AWAIT) {
                    co_await SocketSendAwaitable(connection_socket, &recv_code, sizeof(recv_code));
                }
                break;

            case socket::xpn_server::PING_CODE:
                co_await SocketSendAwaitable(connection_socket, &ack, sizeof(ack));
                break;

            default:
                debug_info("[TH_ID=" << std::this_thread::get_id()
                                     << "] [XPN_SERVER] [xpn_server_up] >> Socket recv unknown code " << recv_code);
                break;
        }

        socket::close(connection_socket);
    }

    socket::close(server_socket);

    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_up] >> End");
    co_return 0;
}

// Stop servers
task<int> xpn_server_co::stop() {
    int res = 0;
    char srv_name[1024];
    FILE *file;

    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_down] >> Begin");

    // Open host file
    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_down] Open host file "
                         << m_params.shutdown_file);

    file = fopen(m_params.shutdown_file.c_str(), "r");
    if (file == NULL) {
        debug_error("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_down] ERROR: invalid file "
                              << m_params.shutdown_file);
        co_return -1;
    }

    std::vector<std::string> srv_names;
    while (fscanf(file, "%[^\n] ", srv_name) != EOF) {
        srv_names.push_back(srv_name);
    }

    // Close host file
    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_down] Close host file");

    fclose(file);

    std::unique_ptr<workers> worker = workers::Create(workers_mode::thread_on_demand);
    std::vector<std::future<int>> v_res(srv_names.size());
    int index = 0;
    for (auto &name : srv_names) {
        v_res[index++] = worker->launch([this, &name]() {
            printf(" * Stopping server (%s)\n", name.c_str());
            int socket;
            int ret;
            int buffer = socket::xpn_server::FINISH_CODE;
            if (m_params.await_stop == 1) {
                buffer = socket::xpn_server::FINISH_CODE_AWAIT;
            }
            ret = socket::client_connect(name, xpn_env::get_instance().xpn_sck_port, socket);
            if (ret < 0) {
                print("[TH_ID=" << std::this_thread::get_id()
                                << "] [XPN_SERVER] [xpn_server_down] ERROR: socket connection " << name);
                return ret;
            }

            ret = socket::send(socket, &buffer, sizeof(buffer));
            if (ret < 0) {
                print("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_down] ERROR: socket send "
                                << name);
            }

            if (m_params.await_stop == 0) {
                socket::close(socket);
            }

            if (m_params.await_stop == 1) {
                ret = socket::recv(socket, &buffer, sizeof(buffer));
                if (ret < 0) {
                    print("[TH_ID=" << std::this_thread::get_id()
                                    << "] [XPN_SERVER] [xpn_server_down] ERROR: socket recv " << name);
                }
                socket::close(socket);
            }
            return ret;
        });
    }

    int aux_res;
    for (auto &fut : v_res) {
        aux_res = fut.get();
        if (aux_res < 0) {
            res = aux_res;
        }
    }

    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [xpn_server_up] >> End");

    co_return res;
}

}  // namespace XPN
// Main
using namespace XPN;
int main(int argc, char *argv[]) {
    int ret = -1;
    char *exec_name = NULL;

    // Initializing...
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    // Get arguments..
    debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [main] Get server params");

    xpn_server_co server(argc, argv);

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

    progress_loop &loop = progress_loop::get_instance();

    auto run_task = server.run();

    while (!run_task.done()) {
        loop.run_one_step();
    }
    // xpn_server_params_show(&params);

    // Do associate action...
    // if (strcasecmp(exec_name, "xpn_stop_server") == 0) {
    //     debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [main] Down servers");
    //     ret = co_await server.stop();
    // } else {
    //     debug_info("[TH_ID=" << std::this_thread::get_id() << "] [XPN_SERVER] [main] Up servers");
    //     ret = co_await server.run();
    // }

    return ret;
}

/* ................................................................... */

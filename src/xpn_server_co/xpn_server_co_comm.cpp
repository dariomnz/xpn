
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

#include "xpn_server_co_comm.hpp"

#include <cassert>
#include <csignal>

#include "base_cpp/debug.hpp"
#include "base_cpp/ns.hpp"
#include "base_cpp/socket.hpp"
#include "base_cpp/timer.hpp"
#include "coroutine/xpn_coroutine.hpp"
#include "coroutine/xpn_socket_co.hpp"
#include "lfi.h"
#include "lfi_async.h"

namespace XPN {
xpn_server_co_control_comm::xpn_server_co_control_comm() {
    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [xpn_server_co_control_comm] >> Begin");

    int port = 0;
    m_server_comm = lfi_server_create(NULL, &port);

    m_port_name = std::to_string(port);

    if (m_server_comm < 0) {
        print("[Server=" << ns::get_host_name()
                         << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_init] ERROR: bind fails");
        std::raise(SIGTERM);
    }

    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [xpn_server_co_control_comm] >> End");
}

xpn_server_co_control_comm::~xpn_server_co_control_comm() {
    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [~xpn_server_co_control_comm] >> Begin");

    lfi_server_close(m_server_comm);

    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [~xpn_server_co_control_comm] >> End");
}

task<int> xpn_server_co_control_comm::accept(int socket) {
    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_accept] >> Begin");

    int ret = 0;
    ret = co_await socket_co::send(socket, m_port_name.data(), MAX_PORT_NAME);
    if (ret < 0) {
        print("[Server=" << ns::get_host_name()
                         << "] [XPN_SERVER_co_CONTROL_COMM] [xpn_server_co_control_comm_accept] ERROR: socket "
                            "send port fails");
        co_return -1;
    }

    int new_comm = co_await LFIAcceptAwaitable(m_server_comm);
    if (new_comm < 0) {
        print("[Server=" << ns::get_host_name()
                         << "] [XPN_SERVER_co_CONTROL_COMM] [xpn_server_co_control_comm_accept] ERROR: accept fails");
        co_return -1;
    }

    debug_info("[Server=" << ns::get_host_name()
                          << "] [XPN_SERVER_co_CONTROL_COMM] [xpn_server_co_control_comm_accept] << End");

    co_return new_comm;
}

void xpn_server_co_control_comm::disconnect(int comm_id) {
    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_disconnect] >> Begin");

    lfi_client_close(comm_id);

    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_disconnect] << End");
}

task<int64_t> xpn_server_co_comm::read_operation(xpn_server_msg &msg, int comm_id, int &tag_client_id) {
    int ret = 0;
    debug_info("[Server=" << ns::get_host_name()
                          << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_read_operation] >> Begin");

    ret = co_await LFIRecvAwaitable(comm_id, &msg, sizeof(msg), 0);
    tag_client_id = msg.tag;

    debug_info("[Server=" << ns::get_host_name()
                          << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_read_operation] read (RANK " << comm_id
                          << ", TAG " << tag_client_id << ") = " << ret);
    debug_info("[Server=" << ns::get_host_name()
                          << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_read_operation] << End");

    // Return OK
    co_return ret;
}

task<int64_t> xpn_server_co_comm::read_data(void *data, int64_t size, int comm_id, int tag_client_id) {
    int64_t ret = 0;

    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_read_data] >> Begin");

    if (size == 0) {
        co_return 0;
    }
    if (size < 0) {
        print("[Server=" << ns::get_host_name()
                         << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_read_data] ERROR: size < 0");
        co_return -1;
    }

    // Get message
    debug_info("[Server=" << ns::get_host_name()
                          << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_read_data] Read data tag " << tag_client_id);
    ret = co_await LFIRecvAwaitable(comm_id, data, size, tag_client_id);
    if (ret < 0) {
        debug_warning("[Server=" << ns::get_host_name()
                                 << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_read_data] ERROR: read fails");
    }

    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_read_data] read (RANK "
                          << comm_id << ", TAG " << tag_client_id << ") = " << ret);
    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_read_data] << End");

    // Return bytes read
    co_return ret;
}

task<int64_t> xpn_server_co_comm::write_data(const void *data, int64_t size, int comm_id, int tag_client_id) {
    int64_t ret = 0;

    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_write_data] >> Begin");

    if (size == 0) {
        co_return 0;
    }
    if (size < 0) {
        print("[Server=" << ns::get_host_name()
                         << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_write_data] ERROR: size < 0");
        co_return -1;
    }

    // Send message
    debug_info("[Server=" << ns::get_host_name()
                          << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_write_data] Write data tag " << tag_client_id);

    ret = co_await LFISendAwaitable(comm_id, data, size, tag_client_id);
    if (ret < 0) {
        debug_warning("[Server=" << ns::get_host_name()
                                 << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_write_data] ERROR: MPI_Send fails");
    }

    debug_info("[Server=" << ns::get_host_name() << "] [XPN_SERVER_co_COMM] [xpn_server_co_comm_write_data] " << ret
                          << " << End");

    // Return bytes written
    co_return ret;
}

}  // namespace XPN
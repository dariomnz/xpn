
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

#include <limits.h>
#include <unistd.h>

#include <fstream>
#include <iostream>

#include "base_cpp/subprocess.hpp"
#include "base_cpp/workers.hpp"
#include "base_cpp/xpn_conf.hpp"
#include "nfi/nfi_server.hpp"
#include "xpn_controller.hpp"

namespace XPN {

int xpn_controller::send_action(action act) {
    debug_info("[XPN_CONTROLLER] >> Start");
    xpn_conf conf;
    // TODO: do for more than the first partition
    auto url = conf.partitions[0].controler_url;
    int ret;
    int socket;
    ret = socket::client_connect(url, socket::xpn_controller::DEFAULT_XPN_CONTROLLER_SCK_PORT, 5000, socket);
    if (ret < 0) {
        print_error("connecting to the xpn_controller");
        return -1;
    }
    int buf = socket::xpn_controller::ACTION_CODE;
    ret = socket::send(socket, &buf, sizeof(buf));
    if (ret != sizeof(buf)) {
        print_error("sending code to the xpn_controller");
        socket::close(socket);
        return -1;
    }
    int code = static_cast<int>(act);
    ret = socket::send(socket, &code, sizeof(code));
    if (ret != sizeof(code)) {
        print_error("sending code to the xpn_controller");
        socket::close(socket);
        return -1;
    }

    switch (act) {
        case action::STOP:
            ret = send_stop(socket);
            break;
        case action::START_SERVERS:
            ret = send_start_servers(socket);
            break;
        case action::STOP_SERVERS:
            ret = send_stop_servers(socket);
            break;
        case action::PING_SERVERS:
            ret = send_ping_servers(socket);
            break;
        case action::EXPAND_NEW:
        case action::EXPAND_CHANGE:
            ret = send_expand(socket);
            break;
        case action::SHRINK_NEW:
        case action::SHRINK_CHANGE:
            ret = send_shrink(socket);
            break;
        default:
            ret = -1;
            std::cerr << "Unknown action" << std::endl;
            break;
    }

    if (ret < 0) {
        print_error("send action");
    }

    ret = socket::recv(socket, &code, sizeof(code));
    if (ret != sizeof(code)) {
        print_error("sending code to the xpn_controller");
        socket::close(socket);
        return -1;
    }
    socket::close(socket);
    debug_info("[XPN_CONTROLLER] >> End");
    return code;
}

int xpn_controller::send_stop(int socket) {
    debug_info("[XPN_CONTROLLER] >> Start");
    int ret = send_stop_servers(socket);
    debug_info("[XPN_CONTROLLER] >> End");
    return ret;
}

int xpn_controller::send_start_servers(int socket) {
    int ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    bool await = m_args.has_option(option_await);
    ret = socket::send(socket, &await, sizeof(await));
    if (ret != sizeof(await)) {
        print_error("send await");
        return -1;
    }
    int server_cores = ::atoi(std::string(m_args.get_option(option_server_cores)).c_str());
    ret = socket::send(socket, &server_cores, sizeof(server_cores));
    if (ret != sizeof(server_cores)) {
        print_error("send server_cores");
        return -1;
    }
    bool debug = m_args.has_option(option_debug);
    ret = socket::send(socket, &debug, sizeof(debug));
    if (ret != sizeof(debug)) {
        print_error("send debug");
        return -1;
    }
    debug_info("[XPN_CONTROLLER] >> End");
    return 0;
}

int xpn_controller::send_stop_servers(int socket) {
    int ret;
    debug_info("[XPN_CONTROLLER] >> Start");
    bool await = m_args.has_option(option_await);
    ret = socket::send(socket, &await, sizeof(await));
    if (ret != sizeof(await)) {
        print_error("send await");
        return -1;
    }
    debug_info("[XPN_CONTROLLER] >> End");
    return 0;
}

int xpn_controller::send_ping_servers([[maybe_unused]] int socket) {
    int ret = 0;
    debug_info("[XPN_CONTROLLER] >> Start");
    debug_info("[XPN_CONTROLLER] >> End");
    return ret;
}

int xpn_controller::send_expand(int socket) {
    debug_info("[XPN_CONTROLLER] >> Start");
    // Necesary options
    auto host_list = m_args.get_option(option_hostlist);
    if (host_list.empty()) {
        std::cerr << "To expand_new and expand_change is necesary the option host_list" << std::endl;
        exit(EXIT_FAILURE);
    }
    int64_t ret;
    // Send the required strings
    ret = socket::send_str(socket, host_list);
    if (ret < 0) {
        print_error("send_str host_list");
        return ret;
    }
    debug_info("[XPN_CONTROLLER] >> End");
    return ret;
}

int xpn_controller::send_shrink(int socket) {
    debug_info("[XPN_CONTROLLER] >> Start");
    // Necesary options
    auto host_list = m_args.get_option(option_hostlist);
    if (host_list.empty()) {
        std::cerr << "To shrink_new and shrink_change is necesary the option host_list" << std::endl;
        exit(EXIT_FAILURE);
    }
    int64_t ret;
    // Send the required strings
    ret = socket::send_str(socket, host_list);
    if (ret < 0) {
        print_error("send_str host_list");
        return ret;
    }
    debug_info("[XPN_CONTROLLER] >> End");
    return ret;
}
}  // namespace XPN